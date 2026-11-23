//! Dictionary bundles for text format 5. Long postings and phrase positions precede
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
}
impl Term {
    fn record_size(&self) -> usize {
        RECORD_HEADER + BLOCK_META * self.blocks.len() + 4
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
pub(super) fn bundle(bytes: Vec<u8>) -> io::Result<Vec<u8>> {
    let footer = checked_slice(&bytes, bytes.len().saturating_sub(FOOTER)..bytes.len())?;
    let header = text_header(&bytes)?.ok_or_else(|| invalid("text header missing"))?;
    if footer[8..12] != VERSION_COLOCATED.to_le_bytes() {
        return Err(invalid("expected colocated text builder output"));
    }
    let lengths = u64::from_le_bytes(footer[32..40].try_into().unwrap()) as usize;
    let positions = u64::from_le_bytes(footer[40..48].try_into().unwrap()) as usize;
    let old_dictionary = u64::from_le_bytes(footer[48..56].try_into().unwrap()) as usize;
    let chunks_start = header.chunks.first().map_or(lengths, |c| c.1 as usize);
    let mut terms = Vec::new();
    for (_, offset, len, crc) in &header.chunks {
        let blob = checked_slice(&bytes, *offset as usize..*offset as usize + *len as usize)?;
        verify(blob, *crc)?;
        let map = fst::Map::new(blob).map_err(|e| invalid(&e.to_string()))?;
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
            let blocks = (0..count)
                .map(|_| BlockMeta::decode(&mut c))
                .collect::<io::Result<Vec<_>>>()?;
            terms.push(Term {
                text: text.to_vec(),
                df,
                blocks,
            });
        }
    }
    let mut out = Vec::new();
    for term in &mut terms {
        if term.blocks.len() > SMALL_BLOCKS {
            for b in &mut term.blocks {
                let raw = checked_slice(
                    &bytes,
                    b.offset as usize..b.offset as usize + b.size as usize,
                )?;
                verify(raw, b.crc)?;
                b.offset = out.len() as u64;
                out.extend_from_slice(raw);
            }
        }
    }
    let positions_start = out.len() as u64;
    out.extend_from_slice(checked_slice(&bytes, positions..chunks_start)?);
    for term in &mut terms {
        for b in &mut term.blocks {
            b.positions = positions_start
                + b.positions
                    .checked_sub(positions as u64)
                    .ok_or_else(|| invalid("invalid phrase position offset"))?;
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
                for b in &mut term.blocks {
                    let raw = checked_slice(
                        &bytes,
                        b.offset as usize..b.offset as usize + b.size as usize,
                    )?;
                    verify(raw, b.crc)?;
                    b.offset = base + blob.len() as u64 + record_size as u64 + payload.len() as u64;
                    payload.extend_from_slice(raw);
                }
            }
            let start = blob.len();
            put32(&mut blob, term.df);
            put32(&mut blob, term.blocks.len() as u32);
            for b in &term.blocks {
                b.encode(&mut blob);
            }
            let crc = crc32fast::hash(&blob[start..]);
            put32(&mut blob, crc);
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
    footer[8..12].copy_from_slice(&VERSION_BUNDLED.to_le_bytes());
    footer[32..40].copy_from_slice(&lengths_offset.to_le_bytes());
    footer[40..48].copy_from_slice(&positions_start.to_le_bytes());
    footer[48..56].copy_from_slice(&dictionary_offset.to_le_bytes());
    footer[64..68].copy_from_slice(&index_crc.to_le_bytes());
    let crc = crc32fast::hash(&footer[..68]);
    footer[68..].copy_from_slice(&crc.to_le_bytes());
    out.extend(footer);
    Ok(out)
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
        let new = bundle(old.clone()).unwrap();
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
}
