//! Small, exhaustive BM25 reference scorer, replaceable by bf-text without changing query semantics.
//! Corpus statistics always include every document with this text attribute, before filtering.
use bf_types::{Document, FullTextConfig};
use serde_json::Value;
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;

/// Tokenize one value; array elements remain separate for phrase matching.
pub fn token_groups(value: &Value, cfg: &FullTextConfig) -> Vec<Vec<String>> {
    if cfg.tokenizer == "pre_tokenized_array" {
        return vec![
            value
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default(),
        ];
    }
    let strings: Vec<_> = if let Some(s) = value.as_str() {
        vec![s]
    } else {
        value
            .as_array()
            .map(|a| a.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default()
    };
    strings
        .into_iter()
        .map(|s| {
            let words: Vec<&str> = if cfg.tokenizer == "whitespace" {
                s.split_whitespace().collect()
            } else {
                s.unicode_words().collect()
            };
            words
                .into_iter()
                .filter(|s| s.len() <= cfg.max_token_length)
                .map(|s| {
                    if cfg.case_sensitive {
                        s.to_owned()
                    } else {
                        s.to_lowercase()
                    }
                })
                .filter(|s| {
                    !cfg.remove_stopwords
                        || !matches!(
                            s.as_str(),
                            "a" | "an"
                                | "and"
                                | "are"
                                | "as"
                                | "at"
                                | "be"
                                | "by"
                                | "for"
                                | "from"
                                | "in"
                                | "is"
                                | "it"
                                | "of"
                                | "on"
                                | "or"
                                | "that"
                                | "the"
                                | "this"
                                | "to"
                                | "was"
                                | "with"
                        )
                })
                .map(|s| {
                    let s = if cfg.stemming {
                        let algorithm = algorithm(&cfg.language);
                        rust_stemmers::Stemmer::create(algorithm)
                            .stem(&s)
                            .into_owned()
                    } else {
                        s
                    };
                    if cfg.ascii_folding {
                        s.nfkd()
                            .filter(|c| !unicode_normalization::char::is_combining_mark(*c))
                            .collect()
                    } else {
                        s
                    }
                })
                .collect()
        })
        .collect()
}
/// Flatten token groups for term-frequency counting.
pub fn tokens(value: &Value, cfg: &FullTextConfig) -> Vec<String> {
    token_groups(value, cfg).into_iter().flatten().collect()
}
/// Score a document against namespace-wide corpus statistics using BM25 defaults.
pub fn score<'a>(
    doc: &Document,
    attribute: &str,
    query: &Value,
    prefix: bool,
    corpus: impl Iterator<Item = &'a Document>,
    cfg: &FullTextConfig,
) -> f64 {
    let terms = tokens(query, cfg);
    if terms.is_empty() {
        return 0.0;
    }
    let corpus: Vec<_> = corpus
        .filter_map(|d| d.get(attribute))
        .filter(|v| !v.is_null())
        .map(|v| tokens(v, cfg))
        .collect();
    if corpus.is_empty() {
        return 0.0;
    }
    let tokens = tokens(doc.get(attribute).unwrap_or(&Value::Null), cfg);
    if tokens.is_empty() {
        return 0.0;
    }
    let count = corpus.len() as f64;
    let avg = corpus.iter().map(Vec::len).sum::<usize>() as f64 / count;
    if avg == 0.0 {
        return 0.0;
    }
    let mut score = 0.0;
    for (i, term) in terms.iter().enumerate() {
        if terms[..i].contains(term) {
            continue;
        }
        if prefix && i + 1 == terms.len() {
            if tokens.iter().any(|t| t.starts_with(term)) {
                score += 1.0;
            }
            continue;
        }
        let tf = tokens.iter().filter(|t| *t == term).count() as f64;
        if tf == 0.0 {
            continue;
        }
        let df = corpus.iter().filter(|ts| ts.contains(term)).count() as f64;
        let idf = (1.0 + (count - df + 0.5) / (df + 0.5)).ln();
        let qf = terms.iter().filter(|t| *t == term).count() as f64;
        score += idf * tf * (cfg.k1 + 1.0)
            / (tf + cfg.k1 * (1.0 - cfg.b + cfg.b * tokens.len() as f64 / avg))
            * (cfg.k3 + 1.0)
            * qf
            / (cfg.k3 + qf);
    }
    score
}

fn algorithm(language: &str) -> rust_stemmers::Algorithm {
    use rust_stemmers::Algorithm::*;
    match language {
        "arabic" => Arabic,
        "danish" => Danish,
        "dutch" => Dutch,
        "finnish" => Finnish,
        "french" => French,
        "german" => German,
        "greek" => Greek,
        "hungarian" => Hungarian,
        "italian" => Italian,
        "norwegian" => Norwegian,
        "portuguese" => Portuguese,
        "romanian" => Romanian,
        "russian" => Russian,
        "spanish" => Spanish,
        "swedish" => Swedish,
        "tamil" => Tamil,
        "turkish" => Turkish,
        _ => English,
    }
}

/// Reject configurations that need a tokenizer absent from this temporary fallback.
pub fn validate_config(config: &FullTextConfig) -> Result<(), bf_types::ApiError> {
    if !matches!(
        config.tokenizer.as_str(),
        "word_v0"
            | "word_v1"
            | "word_v2"
            | "word_v3"
            | "word_v4"
            | "whitespace"
            | "pre_tokenized_array"
    ) {
        return Err(bf_types::ApiError::bad(
            "this full-text tokenizer is not supported by the M1 fallback yet",
        ));
    }
    if config.tokenizer == "whitespace" && (config.stemming || config.remove_stopwords) {
        return Err(bf_types::ApiError::bad(
            "whitespace tokenization does not support stemming or stopwords",
        ));
    }
    Ok(())
}
