//! Format-7 posting tails and variable-width term records. Offsets within each
//! term follow from frame sizes; independently readable frames retain CRCs.
use super::*;

fn put_varint(out: &mut Vec<u8>, mut n: u32) {
    while n >= 128 {
        out.push(n as u8 | 128);
        n >>= 7;
    }
    out.push(n as u8);
}
fn varint(c: &mut Cursor<'_>) -> io::Result<u32> {
    let mut value = 0;
    for shift in (0..35).step_by(7) {
        let byte = c.take(1)?[0];
        if shift == 28 && byte > 15 {
            return Err(invalid("overflowing posting varint"));
        }
        value |= u32::from(byte & 127) << shift;
        if byte < 128 {
            if shift > 0 && byte == 0 {
                return Err(invalid("noncanonical posting varint"));
            }
            return Ok(value);
        }
    }
    Err(invalid("unterminated posting varint"))
}

/// Encode only real postings in a partial block. The first row is in the record.
pub(super) fn encode_tail(postings: &[(u32, u32)]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut previous = postings[0].0;
    for (i, &(row, tf)) in postings.iter().enumerate() {
        if i > 0 {
            put_varint(&mut out, row - previous);
        }
        put_varint(&mut out, tf);
        previous = row;
    }
    out
}

pub(super) fn decode_tail(bytes: &[u8], b: &BlockMeta) -> io::Result<Vec<(u32, u32)>> {
    if b.count == 0 || b.count as usize >= BLOCK {
        return Err(invalid("invalid partial posting count"));
    }
    let mut c = Cursor::new(bytes);
    let mut row = b.first;
    let mut out = Vec::with_capacity(b.count as usize);
    for i in 0..b.count {
        if i > 0 {
            let delta = varint(&mut c)?;
            if delta == 0 {
                return Err(invalid("duplicate posting row"));
            }
            row = row
                .checked_add(delta)
                .ok_or_else(|| invalid("posting row overflow"))?;
        }
        let tf = varint(&mut c)?;
        if row > b.last || tf == 0 || tf > b.max_tf {
            return Err(invalid("invalid partial posting"));
        }
        out.push((row, tf));
    }
    if c.at != bytes.len() || row != b.last {
        return Err(invalid("invalid partial posting frame"));
    }
    Ok(out)
}

pub(super) fn record_size(blocks: &[BlockMeta]) -> usize {
    24 + blocks
        .iter()
        .map(|b| {
            8 + [
                b.first,
                b.last - b.first,
                b.max_tf,
                b.min_length,
                b.size,
                b.positions_size,
            ]
            .into_iter()
            .map(|n| (32 - n.leading_zeros()).max(1).div_ceil(7) as usize)
            .sum::<usize>()
        })
        .sum::<usize>()
}

pub(super) fn encode_record(blocks: &[BlockMeta]) -> Vec<u8> {
    let mut out = Vec::new();
    put32(&mut out, 0); // total record bytes, including this word and the CRC
    put64(&mut out, blocks[0].offset);
    put64(&mut out, blocks[0].positions);
    for b in blocks {
        for n in [
            b.first,
            b.last - b.first,
            b.max_tf,
            b.min_length,
            b.size,
            b.positions_size,
        ] {
            put_varint(&mut out, n);
        }
        put32(&mut out, b.crc);
        put32(&mut out, b.positions_crc);
    }
    let size = out.len() as u32 + 4;
    out[..4].copy_from_slice(&size.to_le_bytes());
    let crc = crc32fast::hash(&out);
    put32(&mut out, crc);
    out
}

pub(super) fn decode_record(
    bytes: &[u8],
    df: u32,
    docs: u32,
    end: u64,
    positions: bool,
) -> io::Result<TermMeta> {
    let mut c = Cursor::new(bytes);
    let size = c.u32()? as usize;
    let count = (df as usize).div_ceil(BLOCK);
    // Each block consumes at least six varints and two CRCs. Check before allocating.
    if df == 0 || size < 24 || size > bytes.len() || count > (size - 24) / 14 {
        return Err(invalid("invalid compact term record size"));
    }
    verify(
        &bytes[..size - 4],
        u32::from_le_bytes(bytes[size - 4..size].try_into().unwrap()),
    )?;
    let mut c = Cursor::new(&bytes[4..size - 4]);
    let (mut offset, mut position) = (c.u64()?, c.u64()?);
    let mut blocks = Vec::with_capacity(count);
    let mut previous = None;
    for i in 0..count {
        let first = varint(&mut c)?;
        let last = first
            .checked_add(varint(&mut c)?)
            .ok_or_else(|| invalid("posting row overflow"))?;
        let b = BlockMeta {
            first,
            last,
            count: (df as usize - i * BLOCK).min(BLOCK) as u32,
            max_tf: varint(&mut c)?,
            min_length: varint(&mut c)?,
            offset,
            size: varint(&mut c)?,
            positions: position,
            positions_size: varint(&mut c)?,
            crc: c.u32()?,
            positions_crc: c.u32()?,
        };
        check_block(&b, docs, previous, end, positions)?;
        previous = Some(last);
        offset = offset
            .checked_add(u64::from(b.size))
            .ok_or_else(|| invalid("posting offset overflow"))?;
        position = position
            .checked_add(u64::from(b.positions_size))
            .ok_or_else(|| invalid("position offset overflow"))?;
        blocks.push(b);
    }
    if c.at != c.data.len() {
        return Err(invalid("trailing compact term metadata"));
    }
    Ok(TermMeta { df, blocks })
}

impl SegmentReader {
    /// Records are already in the checked dictionary bundle. Reading a size word
    /// from the backend first would introduce an unnecessary dependent read wave.
    pub(super) async fn compact_records(&self, handles: &[u64]) -> io::Result<Vec<Arc<TermMeta>>> {
        let Dictionary::Chunked { entries, .. } = &self.dictionary else {
            return Err(invalid("compact dictionary missing"));
        };
        let wanted = handles
            .iter()
            .map(|h| {
                entries
                    .partition_point(|e| e.offset <= h >> DF_BITS)
                    .checked_sub(1)
                    .ok_or_else(|| invalid("term outside dictionary"))
            })
            .collect::<io::Result<Vec<_>>>()?;
        // Keep owned bundle handles, so concurrent memo eviction cannot remove a
        // record while this query decodes it. Bound temporary retained payloads.
        let mut out = Vec::with_capacity(handles.len());
        for (handles, wanted) in handles.chunks(128).zip(wanted.chunks(128)) {
            let chunks = self.chunks(wanted).await?;
            for ((&h, &chunk), bundle) in handles.iter().zip(wanted).zip(chunks) {
                let bytes = &bundle.bytes;
                let offset = (h >> DF_BITS)
                    .checked_sub(entries[chunk].offset)
                    .ok_or_else(|| invalid("invalid term offset"))?;
                let record = bytes
                    .get(offset as usize..)
                    .ok_or_else(|| invalid("term outside bundle"))?;
                out.push(Arc::new(decode_record(
                    record,
                    (h & DF_MASK) as u32,
                    self.docs,
                    self.postings_end,
                    self.positions,
                )?));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn partial_frames_preserve_actual_rows_and_frequencies(
            gaps in prop::collection::vec(1u32..10000,0..126),
            frequencies in prop::collection::vec(1u32..100000,127),
        ) {
            let mut row=7;
            let mut postings=vec![(row,frequencies[0])];
            for (i,gap) in gaps.into_iter().enumerate() {row+=gap;postings.push((row,frequencies[i+1]));}
            let bytes=encode_tail(&postings);
            let b=BlockMeta {first:7,last:row,count:postings.len() as u32,max_tf:100000,min_length:1,offset:0,size:bytes.len() as u32,crc:crc32fast::hash(&bytes),positions:0,positions_size:0,positions_crc:0};
            prop_assert_eq!(decode_tail(&bytes,&b).unwrap(),postings);
            let record=encode_record(std::slice::from_ref(&b));
            prop_assert_eq!(record.len(),record_size(std::slice::from_ref(&b)));
            let decoded=decode_record(&record,b.count,row+1,10000000,false).unwrap();
            prop_assert_eq!(decoded.blocks[0].size,b.size);
            for end in 0..record.len() {prop_assert!(decode_record(&record[..end],b.count,row+1,10000000,false).is_err());}
        }
    }

    #[test]
    fn format_six_upgrade_preserves_all_positions_and_score_bits() {
        for arrays in [false, true] {
            let mut builder = SegmentBuilder::new(true);
            builder.add_missing_document().unwrap();
            builder.add_document(&[]).unwrap();
            let p = FtsParams::default();
            for row in 0..9001 {
                let text = format!("common fish fish rare{}", row % 131);
                let tokens = if arrays {
                    crate::tokenize_array(&[text.as_str(), "fish water"], &p)
                } else {
                    crate::tokenize(&text, &p)
                };
                builder.add_document(&tokens).unwrap();
            }
            let old = bundle::format_six(builder.finish_colocated_impl(true).unwrap()).unwrap();
            assert_eq!(
                u32::from_le_bytes(
                    text_header(&old).unwrap().unwrap().footer[8..12]
                        .try_into()
                        .unwrap()
                ),
                6
            );
            let new = upgrade_segment(old.clone()).unwrap().unwrap();
            assert!(new.len() < old.len());
            assert!(text_header(&new).unwrap().unwrap().is_current());
            let a = block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(old)))).unwrap();
            let b = block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(new)))).unwrap();
            assert_eq!(a.doc_lengths(), b.doc_lengths());
            let stats = block_on(a.stats()).unwrap();
            assert_eq!(stats, block_on(b.stats()).unwrap());
            for ((_, ah), (_, bh)) in block_on(a.all_terms())
                .unwrap()
                .into_iter()
                .zip(block_on(b.all_terms()).unwrap())
            {
                let aa = block_on(a.load([ah])).unwrap().remove(&ah).unwrap();
                let bb = block_on(b.load([bh])).unwrap().remove(&bh).unwrap();
                for (x, y) in aa.blocks.iter().zip(&bb.blocks) {
                    assert_eq!(
                        block_on(a.postings(x, aa.df)).unwrap(),
                        block_on(b.postings(y, bb.df)).unwrap()
                    );
                    assert_eq!(
                        block_on(
                            a.source
                                .read(x.positions..x.positions + u64::from(x.positions_size))
                        )
                        .unwrap(),
                        block_on(
                            b.source
                                .read(y.positions..y.positions + u64::from(y.positions_size))
                        )
                        .unwrap()
                    );
                }
            }
            for text in ["common fish fish", "rare1 fish", "fish water", "absent"] {
                let q = Query::new(crate::tokenize(text, &p), false);
                assert_eq!(
                    block_on(a.search_prefetched(&q, &stats, &p, 1000, None)).unwrap(),
                    block_on(b.search_prefetched(&q, &stats, &p, 1000, None)).unwrap()
                );
                assert_eq!(
                    block_on(a.contains_token_sequence(&q)).unwrap(),
                    block_on(b.contains_token_sequence(&q)).unwrap()
                );
            }
        }
    }

    #[test]
    fn tail_decoder_rejects_overflow_truncation_duplicate_rows_and_trailing_bytes() {
        let b = BlockMeta {
            first: 0,
            last: 1,
            count: 2,
            max_tf: 10,
            min_length: 1,
            offset: 0,
            size: 3,
            crc: 0,
            positions: 0,
            positions_size: 0,
            positions_crc: 0,
        };
        assert_eq!(decode_tail(&[1, 1, 1], &b).unwrap(), vec![(0, 1), (1, 1)]);
        for bytes in [
            vec![],
            vec![1],
            vec![1, 0, 1],
            vec![1, 1, 0],
            vec![1, 1, 1, 0],
            vec![1, 128, 0, 1],
            vec![1, 255, 255, 255, 255, 16, 1],
        ] {
            assert!(decode_tail(&bytes, &b).is_err());
        }
        let mut record = encode_record(&[b]);
        record[5] ^= 1;
        assert!(decode_record(&record, 2, 2, 100, false).is_err());
    }
}
