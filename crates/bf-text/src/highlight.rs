//! Fragment boundaries and offset conversions for computed highlights.
use serde::{Deserialize, Serialize};
use std::ops::Range;
use unicode_segmentation::UnicodeSegmentation;

/// Fragmentation granularity for highlighting. Boundaries never split UTF-8.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FragmentBy {
    /// Treat the complete input as one fragment.
    None,
    /// Unicode sentence boundaries, with surrounding whitespace removed.
    #[default]
    Sentence,
    /// Paragraphs separated by line boundaries.
    Paragraph,
    /// Unicode word boundaries.
    Word,
}
/// Half-open byte ranges of nonempty fragments, excluding surrounding whitespace.
pub fn fragment_ranges(text: &str, by: FragmentBy) -> Vec<Range<usize>> {
    let parts: Vec<(usize, &str)> = match by {
        FragmentBy::None => vec![(0, text)],
        FragmentBy::Sentence => text.split_sentence_bound_indices().collect(),
        FragmentBy::Word => text
            .split_word_bound_indices()
            .filter(|(_, s)| !s.trim().is_empty())
            .collect(),
        FragmentBy::Paragraph => {
            let mut offset = 0;
            text.split_inclusive('\n')
                .map(|part| {
                    let start = offset;
                    offset += part.len();
                    (start, part)
                })
                .collect()
        }
    };
    parts
        .into_iter()
        .filter_map(|(offset, s)| {
            let leading = s.len() - s.trim_start().len();
            let end = s.trim_end().len();
            (leading < end).then_some(offset + leading..offset + end)
        })
        .collect()
}
/// Convert a UTF-8 byte boundary into a requested offset unit.
/// Callers validate the unit and the character boundary before calling.
pub fn offset(text: &str, byte: usize, unit: &str) -> usize {
    match unit {
        "utf-16" => text[..byte].encode_utf16().count(),
        "codepoints" => text[..byte].chars().count(),
        _ => byte,
    }
}
