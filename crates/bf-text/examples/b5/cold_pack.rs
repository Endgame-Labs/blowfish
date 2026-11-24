// Offline checkpoint packer used only by the disposable cold facade experiment.
use super::*;

pub fn pack(root: &Path) {
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest-decoded.local")).unwrap())
            .unwrap();
    let mut names = Vec::new();
    let mut readers = Vec::new();
    for segment in manifest["segments"].as_array().unwrap() {
        let file = &segment["files"]["fts:text"];
        let key = file["key"].as_str().unwrap();
        let raw = std::fs::read(root.join("checkpoint").join(key)).unwrap();
        assert_eq!(&raw[..8], b"BFPUT001");
        readers.push(
            block_on(SegmentReader::open(Arc::new(MemoryRangeRead::from(
                raw[24..].to_vec(),
            ))))
            .unwrap(),
        );
        names.push(key.to_owned());
    }
    assert_eq!(
        names
            .iter()
            .map(|n| crc32fast::hash(n.as_bytes()))
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        names.len(),
        "fixture file fingerprints must be unique"
    );
    let mut terms = BTreeMap::<Vec<u8>, Vec<(usize, u64)>>::new();
    for (part, r) in readers.iter().enumerate() {
        for (term, h) in block_on(r.all_terms()).unwrap() {
            terms.entry(term).or_default().push((part, h));
        }
    }
    let mut bytes = Vec::new();
    let mut groups = Vec::new();
    let mut frames = Vec::new();
    for (word, handles) in terms {
        let mut frame = Vec::new();
        put32(&mut frame, word.len() as u32);
        frame.extend_from_slice(&word);
        put64(&mut frame, handles.iter().map(|(_, h)| h & DF_MASK).sum());
        put32(&mut frame, handles.len() as u32);
        for (part, h) in handles {
            let r = &readers[part];
            let meta = block_on(r.load([h])).unwrap().remove(&h).unwrap();
            put32(&mut frame, crc32fast::hash(names[part].as_bytes()));
            put32(&mut frame, meta.df);
            put32(&mut frame, meta.blocks.len() as u32);
            for b in &meta.blocks {
                b.encode(&mut frame);
            }
            let mut payload = Vec::new();
            if meta.blocks.len() <= SMALL_BLOCKS {
                for b in &meta.blocks {
                    payload.extend_from_slice(
                        &block_on(r.source.read(b.offset..b.offset + u64::from(b.size))).unwrap(),
                    );
                }
            }
            put32(&mut frame, payload.len() as u32);
            frame.extend(payload);
        }
        frames.push((word, frame));
        if frames.len()
            == std::env::var("B5_PACK_TERMS")
                .ok()
                .map_or(128, |s| s.parse::<usize>().unwrap())
        {
            flush(&mut frames, &mut bytes, &mut groups);
        }
    }
    if !frames.is_empty() {
        flush(&mut frames, &mut bytes, &mut groups);
    }
    let key = "ns/b5-msmarco/segments/b5-term-pack.bfp";
    let mut object = b"BFPUT001".to_vec();
    object.extend([0u8; 16]);
    object.extend_from_slice(&bytes);
    std::fs::write(root.join("checkpoint").join(key), object).unwrap();
    let index = serde_json::json!({"key":key,"size":bytes.len(),"files":names,"groups":groups});
    std::fs::write(
        root.join("pack-index.local"),
        serde_json::to_vec(&index).unwrap(),
    )
    .unwrap();
    eprintln!(
        "{}",
        serde_json::json!({"packed_bytes":bytes.len(),"groups":groups.len(),"segments":readers.len()})
    );
}
fn flush(
    frames: &mut Vec<(Vec<u8>, Vec<u8>)>,
    bytes: &mut Vec<u8>,
    groups: &mut Vec<serde_json::Value>,
) {
    let offset = bytes.len();
    put32(bytes, frames.len() as u32);
    let first = String::from_utf8(frames[0].0.clone()).unwrap();
    for (_, frame) in frames.drain(..) {
        put32(bytes, frame.len() as u32);
        bytes.extend(frame);
    }
    groups.push(serde_json::json!([
        first,
        offset,
        bytes.len() - offset,
        crc32fast::hash(&bytes[offset..])
    ]));
}
