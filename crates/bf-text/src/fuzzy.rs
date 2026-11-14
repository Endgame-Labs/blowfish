//! Unicode substring edit distance, independent of tokenization and BM25.
use serde::{Deserialize, Serialize};

/// The allowed Levenshtein distance at a query length threshold.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EditDistance {
    /// Minimum number of Unicode characters in the query.
    pub min_query_chars: usize,
    /// Maximum edits (zero, one, or two).
    pub distance: usize,
}
/// Fuzzy substring matching parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FuzzyParams {
    /// Increasing query length thresholds.
    pub max_edit_distance: Vec<EditDistance>,
    /// Count case differences as edits (defaults to true).
    #[serde(default = "yes")]
    pub case_sensitive: bool,
}
fn yes() -> bool {
    true
}
impl FuzzyParams {
    /// Validate the thresholds needed for lossless trigram candidate narrowing.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_edit_distance.is_empty() {
            return Err("max_edit_distance must contain at least one threshold".into());
        }
        for (i, threshold) in self.max_edit_distance.iter().enumerate() {
            if threshold.distance > 2 || threshold.min_query_chars < 3 * (threshold.distance + 1) {
                return Err("min_query_chars must be at least 3 * (distance + 1), and distance must be between 0 and 2".into());
            }
            if i > 0 && threshold.min_query_chars <= self.max_edit_distance[i - 1].min_query_chars {
                return Err("min_query_chars thresholds must be strictly increasing".into());
            }
        }
        Ok(())
    }
    /// Select the last applicable threshold, counting characters before case folding.
    pub fn distance(&self, query: &str) -> Option<usize> {
        let length = query.chars().count();
        self.max_edit_distance
            .iter()
            .rev()
            .find(|t| length >= t.min_query_chars)
            .map(|t| t.distance)
    }
    /// Match any substring, counting each diacritic or case difference as one edit.
    pub fn matches(&self, text: &str, query: &str) -> bool {
        let Some(limit) = self.distance(query) else {
            return false;
        };
        if self.case_sensitive {
            substring_distance(text, query) <= limit
        } else {
            substring_distance(&text.to_lowercase(), &query.to_lowercase()) <= limit
        }
    }
}
/// Minimum Levenshtein distance from `query` to any substring of `text`.
///
/// Distances count Unicode scalar values, and do not normalize diacritics. The
/// empty prefix in every text position makes the alignment free at both ends.
pub fn substring_distance(text: &str, query: &str) -> usize {
    let query: Vec<char> = query.chars().collect();
    let mut previous: Vec<usize> = (0..=query.len()).collect();
    let mut current = vec![0; query.len() + 1];
    let mut best = query.len();
    for ch in text.chars() {
        current[0] = 0;
        for (j, q) in query.iter().enumerate() {
            current[j + 1] = (previous[j] + usize::from(ch != *q))
                .min(previous[j + 1] + 1)
                .min(current[j] + 1);
        }
        best = best.min(current[query.len()]);
        std::mem::swap(&mut previous, &mut current);
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn edits_substrings_and_unicode() {
        for (text, query, distance) in [
            ("prefix turbopuffer suffix", "turbopufer", 1),
            ("prefix turbopuffer suffix", "turboppuffer", 1),
            ("túrbopuffer", "turbopuffer", 1),
            ("Túrbopuffer", "turbopuffer", 2),
            ("abc", "", 0),
            ("", "abc", 3),
            ("ab", "ba", 1),
            ("🦊🐡🦊", "🐡", 0),
        ] {
            assert_eq!(substring_distance(text, query), distance, "{text}: {query}");
        }
        let p = FuzzyParams {
            max_edit_distance: vec![EditDistance {
                min_query_chars: 6,
                distance: 1,
            }],
            case_sensitive: false,
        };
        assert!(p.matches("xx TURBOPUFFER xx", "turbopufer"));
        assert!(!p.matches("turbo", "turbo"));
        assert!(p.validate().is_ok());
    }
}
