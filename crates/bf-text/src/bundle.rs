//! Dictionary bundles for text formats 5, 6, and 7. Long postings and phrase positions precede
//! bundles; short postings share their term's directory and dictionary read.
use super::*;
use fst::Streamer;

pub(super) const VERSION_BUNDLED: u32 = 5;
const BUNDLE_TERMS: usize = 128;
const BUNDLE_BYTES: usize = 32 << 10;

struct Term {
    text: Vec<u8>,
    df: u32,
    blocks: Vec<BlockMeta>,
    payloads: Vec<Vec<u8>>,
    compact: bool,
}
impl Term {
    fn record_size(&self) -> usize {
        if self.compact {
            compact::record_size(&self.blocks)
        } else {
            RECORD_HEADER + BLOCK_META * self.blocks.len() + 4
        }
    }
    fn stored_size(&self) -> usize {
        self.record_size()
            + if self.blocks.len() <= SMALL_BLOCKS {
                self.blocks.iter().map(|b| b.size as usize).sum()
            } else {
                0
            }
    }
}

/// Reserve a dictionary region before assigning absolute directory addresses.
/// Its padding breaks the dependency between FST size and its encoded offsets.
fn dictionary(terms: &[Term], base: u64) -> io::Result<(Vec<u8>, usize)> {
    let mut reserve = terms.iter().map(|t| t.text.len() + 16).sum::<usize>();
    for _ in 0..16 {
        let mut offset = base + 8 + reserve as u64;
        let mut builder = fst::MapBuilder::memory();
        for term in terms {
            if offset >> (64 - DF_BITS) != 0 {
                return Err(input("text segment too large"));
            }
            builder
                .insert(&term.text, (offset << DF_BITS) | u64::from(term.df))
                .map_err(|e| invalid(&e.to_string()))?;
            offset += term.stored_size() as u64;
        }
        let bytes = builder.into_inner().map_err(|e| invalid(&e.to_string()))?;
        if bytes.len() <= reserve {
            return Ok((bytes, reserve));
        }
        reserve = bytes.len() + bytes.len() / 8 + 32;
    }
    Err(input("dictionary reservation did not converge"))
}

/// Repack the builder's intermediate colocated bytes. Every posting and directory
/// has exactly one final address; no old-format records or duplicate postings remain.
pub(super) fn bundle(bytes: Vec<u8>, compressed_positions: bool) -> io::Result<Vec<u8>> {
    bundle_as(bytes, compressed_positions, true, true)
}

#[cfg(any(test, feature = "legacy-fixtures"))]
pub(super) fn legacy_bundle(bytes: Vec<u8>) -> io::Result<Vec<u8>> {
    bundle_as(bytes, false, false, false)
}

fn bundle_as(
    bytes: Vec<u8>,
    compressed_positions: bool,
    output_compressed: bool,
    output_compact: bool,
) -> io::Result<Vec<u8>> {
    let footer = checked_slice(&bytes, bytes.len().saturating_sub(FOOTER)..bytes.len())?;
    let header = text_header(&bytes)?.ok_or_else(|| invalid("text header missing"))?;
    let version = u32::from_le_bytes(footer[8..12].try_into().unwrap());
    if !matches!(
        version,
        VERSION_COLOCATED | VERSION_BUNDLED | VERSION_COMPRESSED
    ) {
        return Err(invalid("expected legacy text builder output"));
    }
    let lengths = u64::from_le_bytes(footer[32..40].try_into().unwrap()) as usize;
    let positions = u64::from_le_bytes(footer[40..48].try_into().unwrap()) as usize;
    let old_dictionary = u64::from_le_bytes(footer[48..56].try_into().unwrap()) as usize;
    let chunks_start = header.chunks.first().map_or(lengths, |c| c.1 as usize);
    let docs = u32::from_le_bytes(footer[16..20].try_into().unwrap());
    let doc_lengths = decode_lengths(
        checked_slice(&bytes, lengths..old_dictionary)?,
        docs as usize,
    )?;
    if doc_lengths.iter().map(|&n| u64::from(n)).sum::<u64>()
        != u64::from_le_bytes(footer[24..32].try_into().unwrap())
    {
        return Err(invalid("invalid transcode document lengths"));
    }
    let has_positions = u32::from_le_bytes(footer[12..16].try_into().unwrap()) != 0;
    let mut terms = Vec::new();
    let mut position_data = Vec::new();
    for (_, offset, len, crc) in &header.chunks {
        let blob = checked_slice(&bytes, *offset as usize..*offset as usize + *len as usize)?;
        verify(blob, *crc)?;
        let blob = Bytes::copy_from_slice(blob);
        let map = if version >= VERSION_BUNDLED {
            SegmentReader::decode_bundle(&blob)?
        } else {
            fst::Map::new(blob).map_err(|e| invalid(&e.to_string()))?
        };
        let mut stream = map.stream();
        while let Some((text, handle)) = stream.next() {
            let df = (handle & DF_MASK) as u32;
            let count = (df as usize).div_ceil(BLOCK);
            let size = RECORD_HEADER + BLOCK_META * count + 4;
            let offset = (handle >> DF_BITS) as usize;
            let record = checked_slice(&bytes, offset..offset + size)?;
            verify(
                &record[..size - 4],
                u32::from_le_bytes(record[size - 4..].try_into().unwrap()),
            )?;
            let mut c = Cursor::new(&record[..size - 4]);
            if df == 0 || c.u32()? != df || c.u32()? as usize != count {
                return Err(invalid("invalid bundled term record"));
            }
            let mut blocks = (0..count)
                .map(|_| BlockMeta::decode(&mut c))
                .collect::<io::Result<Vec<_>>>()?;
            let mut previous = None;
            for (i, b) in blocks.iter().enumerate() {
                check_block(b, docs, previous, lengths as u64, has_positions)?;
                if b.count as usize != (df as usize - i * BLOCK).min(BLOCK) {
                    return Err(invalid("invalid transcode block count"));
                }
                previous = Some(b.last);
            }
            let mut payloads = Vec::with_capacity(blocks.len());
            for b in &mut blocks {
                let posting = checked_slice(
                    &bytes,
                    b.offset as usize..b.offset as usize + b.size as usize,
                )?;
                verify(posting, b.crc)?;
                let decoded = packed_postings(posting, b)?;
                if decoded.iter().any(|&(row, tf)| {
                    tf > doc_lengths[row as usize] || doc_lengths[row as usize] < b.min_length
                }) {
                    return Err(invalid("invalid transcode posting length"));
                }
                let payload = if output_compact && b.count < BLOCK as u32 {
                    compact::encode_tail(&decoded)
                } else {
                    posting.to_vec()
                };
                b.size = payload.len() as u32;
                b.crc = crc32fast::hash(&payload);
                payloads.push(payload);
                let start = usize::try_from(b.positions)
                    .map_err(|_| invalid("position offset overflow"))?;
                let end = start
                    .checked_add(b.positions_size as usize)
                    .ok_or_else(|| invalid("position size overflow"))?;
                if start < positions || end > chunks_start {
                    return Err(invalid("phrase positions outside their region"));
                }
                let raw = checked_slice(&bytes, start..end)?;
                verify(raw, b.positions_crc)?;
                let new_start = position_data.len();
                if compressed_positions || !output_compressed {
                    if !raw.is_empty() {
                        let mut decoder = positions::Decoder::new(raw, compressed_positions)?;
                        for &(_, tf) in &decoded {
                            decoder.document(tf)?;
                        }
                        decoder.finish()?;
                    }
                    position_data.extend_from_slice(raw);
                } else if !raw.is_empty() {
                    let frequencies = decoded.iter().map(|&(_, tf)| tf);
                    let mut decoder = positions::Decoder::new(raw, false)?;
                    let documents = frequencies
                        .map(|tf| decoder.document(tf))
                        .collect::<io::Result<Vec<_>>>()?;
                    decoder.finish()?;
                    positions::encode(documents.iter().map(Vec::as_slice), &mut position_data);
                }
                b.positions = new_start as u64;
                b.positions_size = u32::try_from(position_data.len() - new_start)
                    .map_err(|_| input("position block too large"))?;
                b.positions_crc = crc32fast::hash(&position_data[new_start..]);
            }
            terms.push(Term {
                text: text.to_vec(),
                df,
                blocks,
                payloads,
                compact: output_compact,
            });
        }
    }
    let mut out = Vec::new();
    for term in &mut terms {
        if term.blocks.len() > SMALL_BLOCKS {
            for (b, raw) in term.blocks.iter_mut().zip(&term.payloads) {
                b.offset = out.len() as u64;
                out.extend_from_slice(raw);
            }
        }
    }
    let positions_start = out.len() as u64;
    out.extend(position_data);
    for term in &mut terms {
        for b in &mut term.blocks {
            b.positions += positions_start;
        }
    }
    let mut groups = Vec::new();
    let (mut first, mut size) = (0, 0);
    for (i, term) in terms.iter().enumerate() {
        let next = term.stored_size() + term.text.len() + 16;
        if i > first && (i - first >= BUNDLE_TERMS || size + next > BUNDLE_BYTES) {
            groups.push(first..i);
            first = i;
            size = 0;
        }
        size += next;
    }
    if first < terms.len() {
        groups.push(first..terms.len());
    }
    let mut index = Vec::new();
    put32(&mut index, groups.len() as u32);
    for group in groups {
        let chunk = &mut terms[group];
        let base = out.len() as u64;
        let (dict, reserve) = dictionary(chunk, base)?;
        let mut blob = vec![0; 8 + reserve];
        blob[..4].copy_from_slice(&(dict.len() as u32).to_le_bytes());
        blob[4..8].copy_from_slice(&(reserve as u32).to_le_bytes());
        blob[8..8 + dict.len()].copy_from_slice(&dict);
        for term in chunk.iter_mut() {
            let record_size = term.record_size();
            let mut payload = Vec::new();
            if term.blocks.len() <= SMALL_BLOCKS {
                for (b, raw) in term.blocks.iter_mut().zip(&term.payloads) {
                    b.offset = base + blob.len() as u64 + record_size as u64 + payload.len() as u64;
                    payload.extend_from_slice(raw);
                }
            }
            if output_compact {
                blob.extend(compact::encode_record(&term.blocks));
            } else {
                let start = blob.len();
                put32(&mut blob, term.df);
                put32(&mut blob, term.blocks.len() as u32);
                for b in &term.blocks {
                    b.encode(&mut blob);
                }
                let crc = crc32fast::hash(&blob[start..]);
                put32(&mut blob, crc);
            }
            blob.extend(payload);
        }
        put32(&mut index, chunk[0].text.len() as u32);
        index.extend_from_slice(&chunk[0].text);
        put64(&mut index, base);
        put32(
            &mut index,
            u32::try_from(blob.len()).map_err(|_| input("dictionary bundle too large"))?,
        );
        put32(&mut index, crc32fast::hash(&blob));
        out.extend(blob);
    }
    let lengths_offset = out.len() as u64;
    out.extend_from_slice(&bytes[lengths..old_dictionary]);
    let dictionary_offset = out.len() as u64;
    let index_crc = crc32fast::hash(&index);
    out.extend(index);
    let mut footer = footer.to_vec();
    footer[8..12].copy_from_slice(
        &if output_compact {
            VERSION_COMPACT
        } else if output_compressed {
            VERSION_COMPRESSED
        } else {
            VERSION_BUNDLED
        }
        .to_le_bytes(),
    );
    footer[32..40].copy_from_slice(&lengths_offset.to_le_bytes());
    footer[40..48].copy_from_slice(&positions_start.to_le_bytes());
    footer[48..56].copy_from_slice(&dictionary_offset.to_le_bytes());
    footer[64..68].copy_from_slice(&index_crc.to_le_bytes());
    let crc = crc32fast::hash(&footer[..68]);
    footer[68..].copy_from_slice(&crc.to_le_bytes());
    out.extend(footer);
    Ok(out)
}

fn packed_postings(bytes: &[u8], b: &BlockMeta) -> io::Result<Vec<(u32, u32)>> {
    let frequencies = frequencies(bytes, b.count)?;
    let mut deltas = [0; BLOCK];
    BitPacker4x::new().decompress(&bytes[2..], &mut deltas, bytes[0]);
    let mut row = b.first;
    let mut out = Vec::with_capacity(b.count as usize);
    for (i, tf) in frequencies.into_iter().enumerate() {
        if (i == 0 && deltas[i] != 0) || (i > 0 && deltas[i] == 0) || tf > b.max_tf {
            return Err(invalid("invalid transcode posting"));
        }
        row = row
            .checked_add(deltas[i])
            .ok_or_else(|| invalid("posting overflow"))?;
        if row > b.last {
            return Err(invalid("posting row out of bounds"));
        }
        out.push((row, tf));
    }
    if row != b.last {
        return Err(invalid("invalid last posting row"));
    }
    Ok(out)
}

#[cfg(any(test, feature = "legacy-fixtures"))]
pub(super) fn format_six(bytes: Vec<u8>) -> io::Result<Vec<u8>> {
    bundle_as(bytes, true, true, false)
}

fn frequencies(bytes: &[u8], count: u32) -> io::Result<Vec<u32>> {
    if count == 0
        || count as usize > BLOCK
        || bytes.len() < 2
        || bytes[0] > 32
        || bytes[1] > 32
        || bytes.len() != 2 + 16 * (bytes[0] as usize + bytes[1] as usize)
    {
        return Err(invalid("invalid packed posting size"));
    }
    let mut frequencies = [0; BLOCK];
    BitPacker4x::new().decompress(
        &bytes[2 + 16 * bytes[0] as usize..],
        &mut frequencies,
        bytes[1],
    );
    if frequencies[..count as usize].contains(&0) {
        return Err(invalid("zero posting frequency"));
    }
    Ok(frequencies[..count as usize].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    #[test]
    fn colocated_upgrade_preserves_scores_positions_missing_rows_and_lengths() {
        let p = FtsParams::default();
        let mut builder = SegmentBuilder::new(true);
        builder.add_missing_document().unwrap();
        builder.add_document(&[]).unwrap();
        for i in 0..2000 {
            builder
                .add_document(&crate::tokenize(
                    &format!("common word{} rare rare", i % 17),
                    &p,
                ))
                .unwrap();
        }
        let old = builder.finish_colocated().unwrap();
        assert!(!text_header(&old).unwrap().unwrap().is_current());
        let new = upgrade_segment(old.clone()).unwrap().unwrap();
        assert!(text_header(&new).unwrap().unwrap().is_current());
        let a = block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(old)))).unwrap();
        let b = block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(new)))).unwrap();
        assert_eq!(a.doc_lengths(), b.doc_lengths());
        let stats = block_on(a.stats()).unwrap();
        assert_eq!(stats, block_on(b.stats()).unwrap());
        for text in ["rare", "common word3", "word1 rare rare", "absent"] {
            let query = Query::new(crate::tokenize(text, &p), false);
            let left = block_on(a.search_prefetched(&query, &stats, &p, 10, None)).unwrap();
            let right = block_on(b.search_prefetched(&query, &stats, &p, 10, None)).unwrap();
            assert_eq!(left, right);
            assert_eq!(
                block_on(a.contains_token_sequence(&query)).unwrap(),
                block_on(b.contains_token_sequence(&query)).unwrap()
            );
        }
    }

    #[test]
    fn upgrade_is_byte_identical_to_a_fresh_build_with_long_and_short_postings() {
        for positions in [false, true] {
            let build = || {
                let mut b = SegmentBuilder::new(positions);
                b.add_missing_document().unwrap();
                b.add_document(&[]).unwrap();
                for row in 0..9000 {
                    b.add_document(&crate::tokenize(
                        &format!("common café rare{} common", row % 113),
                        &FtsParams::default(),
                    ))
                    .unwrap();
                }
                b
            };
            let old = build().finish_colocated().unwrap();
            let expected = build().finish().unwrap();
            let upgraded = upgrade_segment(old).unwrap().unwrap();
            assert_eq!(upgraded, expected);
            assert_eq!(
                upgrade_segment(upgraded.clone()).unwrap().unwrap(),
                upgraded
            );
        }
    }

    #[test]
    fn upgrade_rejects_corrupt_postings_positions_lengths_dictionary_and_footer() {
        let mut b = SegmentBuilder::new(true);
        b.add_document(&crate::tokenize("fish fish water", &FtsParams::default()))
            .unwrap();
        let bytes = b.finish_colocated().unwrap();
        let header = text_header(&bytes).unwrap().unwrap();
        let footer = &header.footer;
        let lengths = u64::from_le_bytes(footer[32..40].try_into().unwrap()) as usize;
        let positions = u64::from_le_bytes(footer[40..48].try_into().unwrap()) as usize;
        let dictionary = u64::from_le_bytes(footer[48..56].try_into().unwrap()) as usize;
        for at in [
            0,
            positions,
            lengths,
            header.chunks[0].1 as usize,
            dictionary,
            bytes.len() - 1,
        ] {
            let mut bad = bytes.clone();
            bad[at] ^= 1;
            assert!(upgrade_segment(bad).is_err(), "corruption at {at}");
        }
        for end in [0, 1, 71, bytes.len() - 1] {
            assert!(upgrade_segment(bytes[..end].to_vec()).is_err());
        }
    }
}

#[cfg(test)]
mod compressed_tests {
    use super::*;
    use futures::executor::block_on;

    #[test]
    fn format_five_upgrade_is_identical_to_new_build_for_scalar_and_array_fields() {
        for positions in [false, true] {
            for arrays in [false, true] {
                let build = || {
                    let mut b = SegmentBuilder::new(positions);
                    b.add_missing_document().unwrap();
                    b.add_document(&[]).unwrap();
                    for row in 0..5000 {
                        let p = FtsParams::default();
                        let tokens = if arrays {
                            crate::tokenize_array(
                                &["fish fish", "fish and water", &format!("term{}", row % 17)],
                                &p,
                            )
                        } else {
                            crate::tokenize(&format!("fish fish and water term{}", row % 17), &p)
                        };
                        b.add_document(&tokens).unwrap();
                    }
                    b
                };
                let old = legacy_bundle(build().finish_colocated().unwrap()).unwrap();
                assert_eq!(
                    u32::from_le_bytes(
                        text_header(&old).unwrap().unwrap().footer[8..12]
                            .try_into()
                            .unwrap()
                    ),
                    5
                );
                let expected = build().finish().unwrap();
                assert_eq!(upgrade_segment(old.clone()).unwrap().unwrap(), expected);
                let a =
                    block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(old)))).unwrap();
                let h = text_header(&expected).unwrap().unwrap();
                assert!(h.is_current());
                let b =
                    SegmentReader::open_with_header(Arc::new(MemoryRangeRead::from(expected)), &h)
                        .unwrap();
                let stats = block_on(a.stats()).unwrap();
                for text in [
                    "fish",
                    "fish fish",
                    "water term3",
                    "fish water",
                    "fish and water",
                    "absent",
                ] {
                    let p = FtsParams::default();
                    let q = Query::new(crate::tokenize(text, &p), false);
                    assert_eq!(
                        block_on(a.search_prefetched(&q, &stats, &p, 10, None)).unwrap(),
                        block_on(b.search_prefetched(&q, &stats, &p, 10, None)).unwrap()
                    );
                    if positions {
                        assert_eq!(
                            block_on(a.contains_token_sequence(&q)).unwrap(),
                            block_on(b.contains_token_sequence(&q)).unwrap()
                        );
                    }
                }
            }
        }
    }
}
