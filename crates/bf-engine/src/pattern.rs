//! Conservative trigram candidates followed by the reference filter evaluator.
use bf_types::{ApiError, Document, Filter, FilterOp, Id, Schema};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Default, Serialize, Deserialize)]
pub(crate) struct Trigrams {
    sensitive: BTreeMap<String, RoaringBitmap>,
    folded: BTreeMap<String, RoaringBitmap>,
}
fn grams(text: &str) -> Vec<String> {
    text.chars()
        .collect::<Vec<_>>()
        .windows(3)
        .map(|w| w.iter().collect())
        .collect()
}
impl Trigrams {
    pub fn build(docs: &BTreeMap<Id, Document>, name: &str) -> Self {
        let mut out = Self::default();
        for (row, doc) in docs.values().enumerate() {
            let Some(value) = doc.get(name) else { continue };
            let strings: Vec<_> = if let Some(s) = value.as_str() {
                vec![s]
            } else {
                value
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|v| v.as_str())
                    .collect()
            };
            for s in strings {
                for gram in grams(s) {
                    out.sensitive.entry(gram).or_default().insert(row as u32);
                }
                for gram in grams(&s.to_lowercase()) {
                    out.folded.entry(gram).or_default().insert(row as u32);
                }
            }
        }
        out
    }
    pub fn candidates(&self, f: &Filter, live: &RoaringBitmap) -> Result<RoaringBitmap, ApiError> {
        let Filter::Compare {
            op, value, params, ..
        } = f
        else {
            return Ok(live.clone());
        };
        if *op != FilterOp::Fuzzy {
            let text = value.as_str().expect("validated pattern");
            let pattern = if *op == FilterOp::Regex {
                text.to_owned()
            } else {
                globset::GlobBuilder::new(text)
                    .case_insensitive(matches!(op, FilterOp::IGlob | FilterOp::NotIGlob))
                    .build()
                    .map_err(|_| ApiError::bad("invalid glob pattern"))?
                    .regex()
                    .to_owned()
            };
            let hir = regex_syntax::ParserBuilder::new()
                .utf8(false)
                .build()
                .parse(&pattern)
                .map_err(|_| ApiError::bad("invalid regular expression"))?;
            return Ok(regex_candidates(&hir, &self.sensitive, live));
        }
        let config = crate::filter::fuzzy_params(params.as_ref())?;
        let query = value.as_str().expect("validated fuzzy string");
        let Some(distance) = config.distance(query) else {
            return Ok(RoaringBitmap::new());
        };
        let folded;
        let (query, postings) = if config.case_sensitive {
            (query, &self.sensitive)
        } else {
            folded = query.to_lowercase();
            (folded.as_str(), &self.folded)
        };
        // Each edit destroys at most three query trigrams. Count occurrences,
        // including repeated grams, so repetition cannot introduce false negatives.
        let grams = grams(query);
        let required = grams.len().saturating_sub(3 * distance);
        if required == 0 {
            return Ok(live.clone());
        }
        let mut counts = BTreeMap::<u32, usize>::new();
        for gram in grams {
            if let Some(rows) = postings.get(&gram) {
                for row in rows & live {
                    *counts.entry(row).or_default() += 1;
                }
            }
        }
        Ok(counts
            .into_iter()
            .filter_map(|(row, count)| (count >= required).then_some(row))
            .collect())
    }
}
/// New index flags must be rebuilt before a query can rely on them.
pub(crate) fn ready(
    f: &Filter,
    schema: &Schema,
    segments: &[bf_wal::Segment],
) -> Result<(), ApiError> {
    match f {
        Filter::And(fs) | Filter::Or(fs) => {
            for f in fs {
                ready(f, schema, segments)?
            }
        }
        Filter::Not(f) => ready(f, schema, segments)?,
        Filter::Compare { attribute, op, .. } => {
            let setting = |cfg: &bf_types::AttributeSchema| match op {
                FilterOp::Fuzzy => cfg.fuzzy,
                FilterOp::Regex => cfg.regex,
                FilterOp::Glob | FilterOp::NotGlob | FilterOp::IGlob | FilterOp::NotIGlob => {
                    cfg.glob
                }
                _ => None,
            };
            if schema
                .get(attribute)
                .is_some_and(|cfg| setting(cfg) == Some(true))
                && segments.iter().any(|seg| {
                    seg.schema
                        .get(attribute)
                        .is_some_and(|cfg| setting(cfg) != Some(true))
                })
            {
                return Err(ApiError::new(
                    409,
                    format!("{op:?} index for '{attribute}' is still being built"),
                ));
            }
        }
    }
    Ok(())
}
// Extract only mandatory literals. Optional branches contribute the full live
// set; alternations union candidates, and concatenations intersect them. Every
// candidate is verified with the original regex/glob matcher before returning.
fn regex_candidates(
    hir: &regex_syntax::hir::Hir,
    postings: &BTreeMap<String, RoaringBitmap>,
    live: &RoaringBitmap,
) -> RoaringBitmap {
    use regex_syntax::hir::HirKind;
    if let Some(strings) = exact_strings(hir) {
        let mut out = RoaringBitmap::new();
        for text in strings {
            let mut rows = live.clone();
            for gram in grams(&text) {
                rows &= postings.get(&gram).cloned().unwrap_or_default();
            }
            out |= rows;
        }
        return out;
    }
    match hir.kind() {
        HirKind::Concat(xs) => xs
            .iter()
            .fold(live.clone(), |rows, x| regex_candidates(x, postings, &rows)),
        HirKind::Alternation(xs) => xs.iter().fold(RoaringBitmap::new(), |rows, x| {
            rows | regex_candidates(x, postings, live)
        }),
        HirKind::Capture(c) => regex_candidates(&c.sub, postings, live),
        HirKind::Repetition(r) if r.min > 0 => regex_candidates(&r.sub, postings, live),
        _ => live.clone(),
    }
}
fn exact_strings(hir: &regex_syntax::hir::Hir) -> Option<Vec<String>> {
    use regex_syntax::hir::{Class, HirKind};
    match hir.kind() {
        HirKind::Empty | HirKind::Look(_) => Some(vec![String::new()]),
        HirKind::Literal(lit) => Some(vec![std::str::from_utf8(&lit.0).ok()?.to_owned()]),
        HirKind::Class(Class::Unicode(class)) => {
            let mut values = vec![];
            for range in class.iter() {
                if range.end() as u32 - range.start() as u32 > 16 {
                    return None;
                }
                for ch in range.start()..=range.end() {
                    values.push(ch.to_string());
                }
                if values.len() > 16 {
                    return None;
                }
            }
            Some(values)
        }
        HirKind::Capture(c) => exact_strings(&c.sub),
        HirKind::Concat(xs) => {
            let mut values = vec![String::new()];
            for x in xs {
                let part = exact_strings(x)?;
                if values.len().checked_mul(part.len())? > 4096 {
                    return None;
                }
                values = values
                    .iter()
                    .flat_map(|a| part.iter().map(move |b| format!("{a}{b}")))
                    .collect();
            }
            Some(values)
        }
        HirKind::Alternation(xs) => {
            let mut values = vec![];
            for x in xs {
                values.extend(exact_strings(x)?);
                if values.len() > 4096 {
                    return None;
                }
            }
            Some(values)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn trigram_narrowing_never_discards_edit_matches() {
        let query = "abc🐡abcéabc";
        let chars: Vec<_> = query.chars().collect();
        let mut strings = vec![query.to_owned(), query.to_uppercase()];
        for i in 0..chars.len() {
            let mut deleted = chars.clone();
            deleted.remove(i);
            strings.push(deleted.into_iter().collect());
            let mut replaced = chars.clone();
            replaced[i] = 'x';
            strings.push(replaced.into_iter().collect());
            let mut inserted = chars.clone();
            inserted.insert(i, 'z');
            strings.push(inserted.into_iter().collect());
        }
        let first = strings.clone();
        for s in first {
            for i in 0..s.chars().count() {
                let mut second: Vec<_> = s.chars().collect();
                second[i] = 'y';
                strings.push(second.into_iter().collect());
            }
        }
        let docs: BTreeMap<_, _> = strings
            .iter()
            .enumerate()
            .map(|(i, s)| {
                (
                    Id::Uint(i as u64),
                    Document::from_iter([("s".into(), json!(format!("prefix {s} suffix")))]),
                )
            })
            .collect();
        let index = Trigrams::build(&docs, "s");
        let live: RoaringBitmap = (0..docs.len() as u32).collect();
        for sensitive in [false, true] {
            let f = Filter::parse(json!(["s","Fuzzy",query,{"max_edit_distance":[{"min_query_chars":9,"distance":2}],"case_sensitive":sensitive}])).unwrap();
            let candidates = index.candidates(&f, &live).unwrap();
            let Filter::Compare { params, .. } = &f else {
                unreachable!()
            };
            let config = crate::filter::fuzzy_params(params.as_ref()).unwrap();
            for (i, doc) in docs.values().enumerate() {
                if config.matches(doc["s"].as_str().unwrap(), query) {
                    assert!(candidates.contains(i as u32), "lost {}", doc["s"]);
                }
            }
        }
    }
}
