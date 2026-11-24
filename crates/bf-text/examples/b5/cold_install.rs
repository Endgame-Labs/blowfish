// Compiled only by the temporary engine patch used for the B5 cold kill test.
use super::*;
struct PackedSource {
    original: Arc<dyn RangeRead>,
    frames: BTreeMap<u64, Bytes>,
}
impl PackedSource {
    fn cached(&self, r: &Range<u64>) -> Option<Bytes> {
        let (&base, bytes) = self.frames.range(..=r.start).next_back()?;
        (r.start <= r.end && r.end <= base + bytes.len() as u64)
            .then(|| bytes.slice((r.start - base) as usize..(r.end - base) as usize))
    }
}
#[async_trait]
impl RangeRead for PackedSource {
    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        if let Some(b) = self.cached(&range) {
            Ok(b)
        } else {
            self.original.read(range).await
        }
    }
    async fn read_batch(&self, ranges: Vec<Range<u64>>) -> io::Result<Vec<Bytes>> {
        let mut out = vec![Bytes::new(); ranges.len()];
        let mut missing = Vec::new();
        let mut slots = Vec::new();
        for (i, r) in ranges.into_iter().enumerate() {
            if let Some(b) = self.cached(&r) {
                out[i] = b;
            } else {
                slots.push(i);
                missing.push(r);
            }
        }
        for (i, b) in slots
            .into_iter()
            .zip(self.original.read_batch(missing).await?)
        {
            out[i] = b;
        }
        Ok(out)
    }
    fn len(&self) -> u64 {
        self.original.len()
    }
}
impl SegmentReader {
    /// Install validated term-pack frames in this disposable query reader.
    pub fn b5_install(&mut self, frames: &[Vec<u8>], file: &str) -> io::Result<()> {
        let mut map = fst::MapBuilder::memory();
        let mut metas = Vec::new();
        let mut payloads = BTreeMap::new();
        for frame in frames {
            let mut c = Cursor::new(frame);
            let len = c.u32()? as usize;
            let word = c.take(len)?;
            let _df = c.u64()?;
            let parts = c.u32()?;
            for _ in 0..parts {
                let key = c.u32()?;
                let df = c.u32()?;
                let n = c.u32()? as usize;
                let blocks = (0..n)
                    .map(|_| BlockMeta::decode(&mut c))
                    .collect::<io::Result<Vec<_>>>()?;
                let len = c.u32()? as usize;
                let payload = c.take(len)?;
                if key == crc32fast::hash(file.as_bytes()) {
                    map.insert(word, metas.len() as u64)
                        .map_err(|e| invalid(&e.to_string()))?;
                    let mut at = 0;
                    if n <= SMALL_BLOCKS {
                        for b in &blocks {
                            let bytes = checked_slice(payload, at..at + b.size as usize)?;
                            verify(bytes, b.crc)?;
                            payloads.insert(b.offset, Bytes::copy_from_slice(bytes));
                            at += b.size as usize;
                        }
                    }
                    if at != payload.len() {
                        return Err(invalid("invalid packed posting bytes"));
                    }
                    metas.push(Arc::new(TermMeta { df, blocks }));
                }
            }
            if c.at != frame.len() {
                return Err(invalid("trailing packed term"));
            }
        }
        self.dictionary = Dictionary::Whole(
            fst::Map::new(Bytes::from(
                map.into_inner().map_err(|e| invalid(&e.to_string()))?,
            ))
            .map_err(|e| invalid(&e.to_string()))?,
        );
        self.terms = Terms::Eager(metas);
        self.source = Arc::new(PackedSource {
            original: self.source.clone(),
            frames: payloads,
        });
        Ok(())
    }
}
