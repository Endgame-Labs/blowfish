use alyze::analyze::{
    AnalysisOptions, Analyzer, LanguageWithStopwords, ReusableBuffer, StemmingLanguage,
    StopwordRemoval, TokenizerOptions,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeMap};
use unicode_segmentation::UnicodeSegmentation;

/// A documented turbopuffer tokenizer. Historic versions approximate old Unicode tables.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tokenizer {
    /// Unicode 17 UAX #29, using turbopuffer's production analyzer.
    #[default]
    WordV4,
    /// Uses the current UAX #29 implementation (historic edge cases may differ).
    WordV3,
    /// Alphanumeric runs, individual ideographs, and emoji graphemes.
    WordV2,
    /// Alphanumeric runs including ideographs, and emoji graphemes.
    WordV1,
    /// Alphanumeric runs including ideographs; emoji are discarded.
    WordV0,
    /// Unicode whitespace splitting, preserving punctuation.
    Whitespace,
    /// Dictionary-based Chinese segmentation.
    Smartcn,
    /// Dictionary-based Japanese segmentation.
    Kuromoji,
    /// Dictionary-based Korean segmentation.
    Nori,
    /// Each supplied array element is one case-sensitive token.
    PreTokenizedArray,
}

macro_rules! languages {
    ($($variant:ident),+ $(,)?) => {
        /// Supported stemming and stopword languages.
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(rename_all = "lowercase")]
        pub enum Language {
            /// Default language.
            #[default]
            English,
            $(#[doc = concat!(stringify!($variant), " language.")] $variant,)+
        }
        impl Language {
            fn stemmer(self) -> StemmingLanguage {
                match self {
                    Self::English => StemmingLanguage::English,
                    $(Self::$variant => StemmingLanguage::$variant,)+
                }
            }
        }
    }
}
languages!(
    Arabic, Danish, Dutch, Finnish, French, German, Greek, Hungarian, Italian, Norwegian,
    Portuguese, Romanian, Russian, Spanish, Swedish, Tamil, Turkish
);

impl Language {
    fn stopwords(self) -> Option<LanguageWithStopwords> {
        Some(match self {
            Self::Arabic | Self::Greek | Self::Romanian | Self::Tamil | Self::Turkish => {
                return None;
            }
            Self::Danish => LanguageWithStopwords::Danish,
            Self::Dutch => LanguageWithStopwords::Dutch,
            Self::English => LanguageWithStopwords::English,
            Self::Finnish => LanguageWithStopwords::Finnish,
            Self::French => LanguageWithStopwords::French,
            Self::German => LanguageWithStopwords::German,
            Self::Hungarian => LanguageWithStopwords::Hungarian,
            Self::Italian => LanguageWithStopwords::Italian,
            Self::Norwegian => LanguageWithStopwords::Norwegian,
            Self::Portuguese => LanguageWithStopwords::Portuguese,
            Self::Russian => LanguageWithStopwords::Russian,
            Self::Spanish => LanguageWithStopwords::Spanish,
            Self::Swedish => LanguageWithStopwords::Swedish,
        })
    }
}

/// Expanded full-text schema parameters. Deserializes both `true` and an options object.
///
/// `false` belongs to the engine's disabled-FTS schema variant and is rejected here.
/// Call [`Self::validate`] after constructing or modifying parameters in Rust.
#[derive(Clone, Debug, PartialEq)]
pub struct FtsParams {
    /// Document-term saturation, positive and finite.
    pub k1: f32,
    /// Length normalization in `[0, 1]`.
    pub b: f32,
    /// Query-term saturation, positive and finite.
    pub k3: f32,
    /// Fold supported accented characters after stemming and stopword removal.
    pub ascii_folding: bool,
    /// Preserve case.
    pub case_sensitive: bool,
    /// Language for stemming and stopword removal. Ignored and serialized as null
    /// for pre-tokenized and CJK dictionary fields.
    pub language: Language,
    /// Enable Snowball stemming.
    pub stemming: bool,
    /// Remove language-specific stopwords, retaining position gaps.
    pub remove_stopwords: bool,
    /// Persist this explicitly to preserve namespace behavior.
    pub tokenizer: Tokenizer,
    /// Maximum token length in Unicode scalar values, as measured by the live oracle.
    /// Ignored and serialized as null for pre-tokenized fields.
    pub max_token_length: u8,
}

impl Default for FtsParams {
    fn default() -> Self {
        Self {
            k1: 1.2,
            b: 0.75,
            k3: 8.0,
            ascii_folding: false,
            case_sensitive: false,
            language: Language::English,
            stemming: false,
            remove_stopwords: false,
            tokenizer: Tokenizer::WordV4,
            max_token_length: 39,
        }
    }
}

impl Serialize for FtsParams {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(11))?;
        map.serialize_entry("k1", &self.k1)?;
        map.serialize_entry("b", &self.b)?;
        map.serialize_entry("k3", &self.k3)?;
        map.serialize_entry("ascii_folding", &self.ascii_folding)?;
        map.serialize_entry("case_sensitive", &self.case_sensitive)?;
        let language = (!matches!(
            self.tokenizer,
            Tokenizer::PreTokenizedArray
                | Tokenizer::Smartcn
                | Tokenizer::Kuromoji
                | Tokenizer::Nori
        ))
        .then_some(self.language);
        map.serialize_entry("language", &language)?;
        map.serialize_entry("stemming", &self.stemming)?;
        map.serialize_entry("remove_stopwords", &self.remove_stopwords)?;
        map.serialize_entry("tokenizer", &self.tokenizer)?;
        map.serialize_entry(
            "max_token_length",
            &(self.tokenizer != Tokenizer::PreTokenizedArray).then_some(self.max_token_length),
        )?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for FtsParams {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value == true {
            return Ok(Self::default());
        }
        let object = value.as_object().ok_or_else(|| {
            serde::de::Error::custom("full_text_search must be true or an object")
        })?;
        let mut p = Self::default();
        macro_rules! field {
            ($($name:ident),+) => { $(if let Some(v) = object.get(stringify!($name)) {
                if !(v.is_null() && matches!(stringify!($name),"language" | "max_token_length")) {
                    p.$name = serde_json::from_value(v.clone()).map_err(serde::de::Error::custom)?;
                }
            })+ }
        }
        field!(
            k1,
            b,
            k3,
            ascii_folding,
            case_sensitive,
            language,
            stemming,
            remove_stopwords,
            tokenizer,
            max_token_length
        );
        if p.tokenizer == Tokenizer::PreTokenizedArray {
            if object.get("language").is_some_and(|v| !v.is_null())
                || object.get("case_sensitive") == Some(&false.into())
            {
                return Err(serde::de::Error::custom(
                    "pre_tokenized_array requires case sensitivity and no language",
                ));
            }
            p.case_sensitive = true;
        }
        if matches!(
            p.tokenizer,
            Tokenizer::Smartcn | Tokenizer::Kuromoji | Tokenizer::Nori
        ) && (object.get("language").is_some_and(|v| !v.is_null())
            || p.case_sensitive
            || p.stemming
            || p.remove_stopwords
            || p.ascii_folding)
        {
            return Err(serde::de::Error::custom(
                "CJK dictionary tokenizers do not accept normalization options",
            ));
        }
        p.validate().map_err(serde::de::Error::custom)?;
        Ok(p)
    }
}

impl FtsParams {
    /// Validate numeric ranges and incompatible analysis options.
    pub fn validate(&self) -> Result<(), String> {
        if !self.k1.is_finite() || self.k1 <= 0.0 || !self.k3.is_finite() || self.k3 <= 0.0 {
            return Err("k1 and k3 must be positive and finite".into());
        }
        if !self.b.is_finite()
            || !(0.0..=1.0).contains(&self.b)
            || self.max_token_length == 0
            || self.max_token_length == 255
        {
            return Err("b must be in [0, 1], and max_token_length must be in [1, 254]".into());
        }
        if (self.case_sensitive
            || matches!(
                self.tokenizer,
                Tokenizer::Whitespace | Tokenizer::PreTokenizedArray
            ))
            && (self.stemming || self.remove_stopwords)
        {
            return Err("stemming and stopwords require a case-insensitive word tokenizer".into());
        }
        if self.remove_stopwords && self.language.stopwords().is_none() {
            return Err("stopword removal is unavailable for this language".into());
        }
        if matches!(
            self.tokenizer,
            Tokenizer::Smartcn | Tokenizer::Kuromoji | Tokenizer::Nori
        ) && (self.case_sensitive
            || self.stemming
            || self.remove_stopwords
            || self.ascii_folding)
        {
            return Err("CJK dictionary tokenizers do not accept normalization options".into());
        }
        if self.tokenizer == Tokenizer::PreTokenizedArray && !self.case_sensitive {
            return Err("pre_tokenized_array requires case sensitivity".into());
        }
        Ok(())
    }

    fn analyzer(&self, legacy: bool) -> Analyzer<'static> {
        match self.tokenizer {
            Tokenizer::Smartcn => return Analyzer::smartcn(Default::default()),
            Tokenizer::Kuromoji => return Analyzer::kuromoji(Default::default()),
            Tokenizer::Nori => return Analyzer::nori(Default::default()),
            _ => (),
        }
        Analyzer::new(AnalysisOptions {
            tokenizer: if legacy || self.tokenizer == Tokenizer::Whitespace {
                TokenizerOptions::Whitespace
            } else {
                TokenizerOptions::UAX29Word(Default::default())
            },
            maximum_token_length: Some(self.max_token_length as usize),
            case_sensitive: self.case_sensitive,
            // Legacy words are normalized individually with a word analyzer below.
            stopword_removal: (!legacy && self.remove_stopwords).then(|| {
                StopwordRemoval::ForLanguage(
                    self.language.stopwords().expect("validated parameters"),
                )
            }),
            stemming: (!legacy && self.stemming).then(|| self.language.stemmer()),
            ascii_folding: self.ascii_folding,
        })
    }
}

/// An analyzed token. Position gaps survive stopword and length filtering.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Token {
    /// Normalized term text.
    pub text: String,
    /// Zero-based position within the input element.
    pub position: u32,
    /// Input array element. Phrases never cross an element boundary.
    #[serde(default)]
    pub element: u32,
}

impl Token {
    /// Construct a token for a scalar string.
    pub fn new(text: impl Into<String>, position: u32) -> Self {
        Self {
            text: text.into(),
            position,
            element: 0,
        }
    }
}

/// Analyze a scalar string. For pre-tokenized fields, the entire string is one token.
/// Parameters must pass [`FtsParams::validate`].
pub fn tokenize(text: &str, params: &FtsParams) -> Vec<Token> {
    tokenize_array(&[text], params)
}

/// Analyze an array; normal string arrays retain phrase boundaries, whereas
/// pre-tokenized arrays form one contiguous token sequence with no normalization.
pub fn tokenize_array<T: AsRef<str>>(texts: &[T], params: &FtsParams) -> Vec<Token> {
    if params.tokenizer == Tokenizer::PreTokenizedArray {
        return texts
            .iter()
            .enumerate()
            .map(|(i, s)| Token::new(s.as_ref(), i as u32))
            .collect();
    }
    let legacy = matches!(
        params.tokenizer,
        Tokenizer::WordV0 | Tokenizer::WordV1 | Tokenizer::WordV2
    );
    let analyzer = params.analyzer(legacy);
    let mut buffer = ReusableBuffer::new();
    let mut tokens = Vec::new();
    for (element, text) in texts.iter().enumerate() {
        if legacy {
            let words = legacy_words(text.as_ref(), params.tokenizer);
            // Whitespace analyzer preserves the old splitter's token boundaries, and
            // a separate modern analyzer applies stemming and stopwords to each word.
            let mut norm = params.clone();
            norm.tokenizer = Tokenizer::WordV4;
            let normalizer = norm.analyzer(false);
            for (position, word) in words.iter().enumerate() {
                if params.stemming || params.remove_stopwords {
                    normalizer.analyze(word, &mut buffer, |token| {
                        tokens.push(Token {
                            text: token.text.into(),
                            position: position as u32,
                            element: element as u32,
                        });
                        true
                    });
                } else {
                    analyzer.analyze(word, &mut buffer, |token| {
                        tokens.push(Token {
                            text: token.text.into(),
                            position: position as u32,
                            element: element as u32,
                        });
                        true
                    });
                }
            }
        } else {
            analyzer.analyze(text.as_ref(), &mut buffer, |token| {
                let cjk = matches!(
                    params.tokenizer,
                    Tokenizer::Smartcn | Tokenizer::Kuromoji | Tokenizer::Nori
                );
                // Standard analysis checks original length before case/ASCII expansion.
                if !cjk || token.text.chars().count() <= params.max_token_length as usize {
                    tokens.push(Token {
                        text: token.text.into(),
                        position: token.position as u32,
                        element: element as u32,
                    });
                }
                true
            });
        }
    }
    tokens
}

fn ideograph(c: char) -> bool {
    matches!(c as u32, 0x3400..=0x4dbf | 0x4e00..=0x9fff | 0xf900..=0xfaff | 0x20000..=0x323af)
}

fn legacy_words(text: &str, tokenizer: Tokenizer) -> Vec<String> {
    let mut words = Vec::new();
    let mut run = String::new();
    for g in text.graphemes(true) {
        let c = g.chars().next().unwrap();
        if c.is_alphanumeric() && !(tokenizer == Tokenizer::WordV2 && ideograph(c)) {
            run.push_str(g);
        } else {
            if !run.is_empty() {
                words.push(std::mem::take(&mut run));
            }
            if (tokenizer == Tokenizer::WordV2 && ideograph(c))
                || (tokenizer != Tokenizer::WordV0
                    && g.chars()
                        .any(|c| matches!(c as u32, 0x1f000..=0x1faff | 0x2600..=0x27bf)))
            {
                words.push(g.into());
            }
        }
    }
    if !run.is_empty() {
        words.push(run);
    }
    words
}

/// Boolean token matching also recognizes punctuation-delimited subwords inside
/// UAX #29 terms. Underscores remain part of a word. BM25 and phrase matching
/// always use the original analyzed tokens instead.
pub fn matches_all_tokens(document: &[Token], query: &crate::Query, tokenizer: Tokenizer) -> bool {
    query.tokens.iter().enumerate().all(|(i, q)| {
        document.iter().any(|d| {
            boolean_term_matches(
                &d.text,
                &q.text,
                query.last_as_prefix && i + 1 == query.tokens.len(),
                tokenizer,
            )
        })
    })
}

/// Match at least one boolean query token, honoring final-token prefix matching.
pub fn matches_any_token(document: &[Token], query: &crate::Query, tokenizer: Tokenizer) -> bool {
    query.tokens.iter().enumerate().any(|(i, q)| {
        document.iter().any(|d| {
            boolean_term_matches(
                &d.text,
                &q.text,
                query.last_as_prefix && i + 1 == query.tokens.len(),
                tokenizer,
            )
        })
    })
}

/// Match query tokens at their relative positions within a single input element.
/// Prefix matching is unsupported and returns false.
pub fn matches_token_sequence(document: &[Token], query: &crate::Query) -> bool {
    if query.last_as_prefix {
        return false;
    }
    let Some(first) = query.tokens.first() else {
        return true;
    };
    document.iter().any(|start| {
        query.tokens.iter().all(|q| {
            document.iter().any(|d| {
                d.element == start.element
                    && d.text == q.text
                    && d.position as i64 - start.position as i64
                        == q.position as i64 - first.position as i64
            })
        })
    })
}

/// The punctuation-delimited subwords of a stored term that boolean token filters also match.
pub(crate) fn subwords(term: &str) -> impl Iterator<Item = &str> {
    term.split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|s| !s.is_empty())
}

pub(crate) fn boolean_term_matches(
    term: &str,
    query: &str,
    prefix: bool,
    tokenizer: Tokenizer,
) -> bool {
    let matches = |s: &str| {
        if prefix {
            s.starts_with(query)
        } else {
            s == query
        }
    };
    matches(term)
        || (matches!(tokenizer, Tokenizer::WordV3 | Tokenizer::WordV4)
            && subwords(term).any(matches))
}

/// Analyze a scalar string while retaining original, half-open UTF-8 byte ranges.
/// Normalization, stemming, and stopword removal never change these ranges.
pub fn tokenize_spans(text: &str, params: &FtsParams) -> Vec<(String, std::ops::Range<usize>)> {
    if matches!(
        params.tokenizer,
        Tokenizer::WordV0 | Tokenizer::WordV1 | Tokenizer::WordV2
    ) {
        let mut offset = 0;
        let mut out = vec![];
        for word in legacy_words(text, params.tokenizer) {
            let Some(found) = text[offset..].find(&word) else {
                continue;
            };
            let start = offset + found;
            let end = start + word.len();
            offset = end;
            for token in tokenize(&word, params) {
                out.push((token.text, start..end));
            }
        }
        return out;
    }
    let analyzer = params.analyzer(false);
    let mut buffer = ReusableBuffer::new();
    let mut out = vec![];
    analyzer.analyze(text, &mut buffer, |token| {
        let cjk = matches!(
            params.tokenizer,
            Tokenizer::Smartcn | Tokenizer::Kuromoji | Tokenizer::Nori
        );
        if !cjk || token.text.chars().count() <= params.max_token_length as usize {
            out.push((token.text.to_owned(), token.byte_range));
        }
        true
    });
    out
}
