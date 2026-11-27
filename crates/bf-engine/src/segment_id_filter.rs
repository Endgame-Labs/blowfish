//! Seek ID predicates in the sorted document column instead of decoding a bitmap per ID.
use super::*;
use bf_types::FilterOp;

/// ID rows are in ascending order. A scalar comparison therefore selects one row interval
/// (or its complement), which can be intersected with the caller's live rows directly.
pub(super) async fn evaluate(
    store: &Storage,
    segment: &Segment,
    op: FilterOp,
    value: &Value,
    schema: &Schema,
    live: &RoaringBitmap,
) -> Result<Option<RoaringBitmap>, ApiError> {
    use FilterOp::*;
    if !matches!(op, Eq | NotEq | Lt | Lte | Gt | Gte) {
        return Ok(None);
    }
    let operand = if let Some(t) = schema.get("id").and_then(|s| s.data_type.as_ref()) {
        crate::write::normalize_value(value, t)?
    } else {
        value.clone()
    };
    if live.is_empty() {
        return Ok(Some(RoaringBitmap::new()));
    }
    let bounds = bounds(store, segment).await?;
    let seeker = tokio::sync::OnceCell::new();
    // Only the boundary that the operator needs is sought. Min/max checks eliminate whole
    // segments before their docs directory, ID column, or ID dictionary is opened. `Eq` and
    // `NotEq` seek both ends at once.
    let (low, high) = match op {
        Eq | NotEq => {
            futures::future::try_join(
                boundary(store, segment, &seeker, &operand, false, bounds.as_ref()),
                boundary(store, segment, &seeker, &operand, true, bounds.as_ref()),
            )
            .await?
        }
        Lt | Gte => (
            boundary(store, segment, &seeker, &operand, false, bounds.as_ref()).await?,
            0,
        ),
        _ => (
            0,
            boundary(store, segment, &seeker, &operand, true, bounds.as_ref()).await?,
        ),
    };
    let range = match op {
        Eq | NotEq => low..high,
        Lt => 0..low,
        Lte => 0..high,
        Gt => high..segment.row_count,
        Gte => low..segment.row_count,
        _ => unreachable!(),
    };
    let mut rows = RoaringBitmap::new();
    rows.insert_range(range);
    Ok(Some(if op == NotEq {
        live - rows
    } else {
        rows & live
    }))
}

/// Read just the existing Bloom object's range header; the Bloom bits are irrelevant to ranges.
async fn bounds(store: &Storage, segment: &Segment) -> Result<Option<(Value, Value)>, ApiError> {
    let Some(file) = segment.files.get("idfilter") else {
        return Ok(None);
    };
    let key = format!("id-bounds:{}", file.key);
    if let Some(bounds) = read_cache::get::<(Value, Value)>(&key, 0) {
        return Ok(Some((*bounds).clone()));
    }
    let reader = ObjectReader::new(store, file);
    let header = reader
        .read_batch_sparse(std::iter::once(0..file.size.min(13)).collect())
        .await
        .map_err(index_error)?;
    let header = &header[0];
    if header.len() != 13 || &header[..8] != ID_FILTER_MAGIC {
        return Err(corrupt());
    }
    let len = u32::from_le_bytes(header[9..13].try_into().map_err(index_error)?) as u64;
    let end = 13u64
        .checked_add(len)
        .filter(|n| *n <= file.size)
        .ok_or_else(corrupt)?;
    let bytes = reader
        .read_batch_sparse(std::iter::once(13..end).collect())
        .await
        .map_err(index_error)?;
    let (min, max): (Id, Id) = serde_json::from_slice(&bytes[0]).map_err(index_error)?;
    if min > max {
        return Err(corrupt());
    }
    let bounds = (min.value(), max.value());
    read_cache::insert(&key, len + 128, bounds.clone());
    Ok(Some(bounds))
}

/// Probes in the first wave are at most this many: each is one small request.
const MAX_PROBES: usize = 8;
/// A probe reads one ID block through the sparse cache's blocks of this size.
const PROBE_BYTES: u64 = 16 << 10;

/// A segment's sorted ID column, read through the document directory.
struct Seeker {
    reader: ObjectReader,
    dir: Arc<Directory>,
    docs_key: String,
    rows: u32,
    /// Row IDs from a staged local ANN map: seeks then need no object-store reads at all.
    staged: bool,
}

impl Seeker {
    async fn open(store: &Storage, segment: &Segment) -> Result<Self, ApiError> {
        let staged = segment.files.get("ids").is_some_and(|file| {
            crate::local_ann::staged_row_ids(&file.key, &RoaringBitmap::from_iter([0])).is_some()
        });
        let docs = segment.files.get("docs").ok_or_else(corrupt)?;
        let reader = ObjectReader::new(store, docs);
        let dir = directory(&reader).await?;
        let blocks = dir.columns.get("id").ok_or_else(corrupt)?.blocks.len();
        if blocks != (segment.row_count as usize).div_ceil(BLOCK_ROWS) {
            return Err(corrupt());
        }
        Ok(Self {
            reader,
            dir,
            docs_key: docs.key.clone(),
            rows: segment.row_count,
            staged,
        })
    }

    fn blocks(&self) -> usize {
        self.dir.columns["id"].blocks.len()
    }

    fn block_rows(&self, i: usize) -> usize {
        (self.rows as usize - i * BLOCK_ROWS).min(BLOCK_ROWS)
    }

    /// The decoded ID blocks at `indices`, from the decoded cache or one batch of sparse
    /// reads. Every CRC is checked, and decoded blocks enter the cache that document
    /// hydration shares.
    async fn fetch(&self, indices: &[usize]) -> Result<Vec<Arc<Vec<Option<Value>>>>, ApiError> {
        let col = &self.dir.columns["id"];
        let context = read_cache::context();
        let key = |i: usize| format!("{}#id#{i}", self.docs_key);
        let mut out: Vec<Option<Arc<Vec<Option<Value>>>>> = indices
            .iter()
            .map(|&i| {
                let hit = context.as_ref()?.cache.column(&key(i))?;
                context
                    .as_ref()?
                    .stats
                    .record_memory_hit(col.blocks[i].length);
                Some(hit)
            })
            .collect();
        let missing: Vec<usize> = (0..indices.len()).filter(|n| out[*n].is_none()).collect();
        if !missing.is_empty() {
            let blocks: Vec<&Block> = missing.iter().map(|n| &col.blocks[indices[*n]]).collect();
            let ranges = blocks
                .iter()
                .map(|b| b.offset..b.offset + b.length)
                .collect();
            let data = self
                .reader
                .read_batch_sparse(ranges)
                .await
                .map_err(index_error)?;
            for ((&n, b), data) in missing.iter().zip(blocks).zip(data) {
                if crc32fast::hash(&data) != b.crc {
                    return Err(corrupt());
                }
                let values = decode_column(&data, col, b)?;
                if values.len() != self.block_rows(indices[n]) {
                    return Err(corrupt());
                }
                let values = Arc::new(values);
                if let Some(c) = &context {
                    c.cache.insert_column(&key(indices[n]), values.clone());
                }
                out[n] = Some(values);
            }
        }
        Ok(out.into_iter().map(|v| v.expect("filled")).collect())
    }
}

/// The ID at position `i` of a decoded block; IDs are never null.
fn id_at(values: &[Option<Value>], i: usize) -> Result<&Value, ApiError> {
    values.get(i).and_then(Option::as_ref).ok_or_else(corrupt)
}

/// First row >= operand (or > operand for an inclusive upper boundary), in at most two waves
/// of reads after the directory. The first wave reads the first ID of up to [`MAX_PROBES`]
/// evenly spaced blocks, all at once, which brackets the boundary in one region of the
/// column. The second wave reads that region's blocks as one coalesced range and searches it
/// in memory. A block already in the decoded cache costs no read in either wave.
async fn boundary(
    store: &Storage,
    segment: &Segment,
    seeker: &tokio::sync::OnceCell<Seeker>,
    operand: &Value,
    inclusive: bool,
    bounds: Option<&(Value, Value)>,
) -> Result<u32, ApiError> {
    let before = |v: &Value| {
        let cmp = filter::compare(v, operand);
        cmp.is_lt() || (inclusive && cmp.is_eq())
    };
    if let Some((min, max)) = bounds {
        if !before(min) {
            return Ok(0);
        }
        if before(max) {
            return Ok(segment.row_count);
        }
    }
    let seeker = seeker
        .get_or_try_init(|| Seeker::open(store, segment))
        .await?;
    if seeker.staged {
        return staged_boundary(store, segment, &before).await;
    }
    let trace = crate::ann::trace_stages().then(std::time::Instant::now);
    let nb = seeker.blocks();
    let column_bytes: u64 = seeker.dir.columns["id"]
        .blocks
        .iter()
        .map(|b| b.length)
        .sum();
    // Probes cost PROBE_BYTES each and the region costs the column over the probe count.
    let k = (((column_bytes / PROBE_BYTES) as f64).sqrt().ceil() as usize)
        .clamp(2, MAX_PROBES)
        .min(nb);
    let starts: Vec<usize> = (0..k).map(|t| t * nb / k).collect();
    // Wave one: the first ID of every probe block. Probe 0 is included, since without the
    // ID-bounds header nothing says that the column's first ID precedes the boundary.
    let probes = seeker.fetch(&starts).await?;
    if let Some(t) = trace {
        eprintln!(
            "ID_SEEK {} wave1 {k} {}",
            seeker.docs_key,
            t.elapsed().as_micros()
        );
    }
    let mut firsts = Vec::with_capacity(k);
    for values in &probes {
        firsts.push(before(id_at(values, 0)?));
    }
    let Some(s) = firsts.iter().rposition(|b| *b) else {
        return Ok(0);
    };
    let end = starts.get(s + 1).copied().unwrap_or(nb);
    // Wave two: every block of the bracketing region, in one batch.
    let region: Vec<usize> = (starts[s]..end).collect();
    let blocks = seeker.fetch(&region).await?;
    if let Some(t) = trace {
        eprintln!(
            "ID_SEEK {} wave2 {} {}",
            seeker.docs_key,
            region.len(),
            t.elapsed().as_micros()
        );
    }
    // The first block whose last ID is not before the operand holds the boundary, if any.
    for (&i, values) in region.iter().zip(&blocks) {
        if before(id_at(values, values.len() - 1)?) {
            continue;
        }
        let mut within = Vec::with_capacity(values.len());
        for n in 0..values.len() {
            within.push(before(id_at(values, n)?));
        }
        return Ok((i * BLOCK_ROWS + within.partition_point(|b| *b)) as u32);
    }
    Ok((end * BLOCK_ROWS).min(segment.row_count as usize) as u32)
}

/// The row-by-row binary search, for segments whose row IDs are staged locally (no reads).
async fn staged_boundary(
    store: &Storage,
    segment: &Segment,
    before: &impl Fn(&Value) -> bool,
) -> Result<u32, ApiError> {
    let columns = std::collections::BTreeSet::from(["id".to_string()]);
    let (mut low, mut high) = (0, segment.row_count);
    while low < high {
        let mid = low + (high - low) / 2;
        let docs = projected_rows(
            store,
            segment,
            &RoaringBitmap::from_iter([mid]),
            Some(&columns),
        )
        .await?;
        let value = docs
            .get(&mid)
            .and_then(|d| d.get("id"))
            .ok_or_else(corrupt)?;
        if before(value) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    Ok(low)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn seeks_match_document_evaluation_for_every_id_type_and_live_holes() {
        for kind in ["uint", "string", "uuid"] {
            let store = Storage::open(&bf_storage::StorageConfig {
                url: "memory://".into(),
                ..Default::default()
            })
            .await
            .unwrap();
            let schema: Schema = serde_json::from_value(json!({"id": {"type":kind}})).unwrap();
            let values: Vec<Value> = (0..1100u64)
                .map(|i| match kind {
                    "uint" => json!(u64::MAX - 4000 + i * 3),
                    "string" => json!(format!("doc-{:04}-é", i * 3)),
                    _ => json!(uuid::Uuid::from_u128((i * 3) as u128).to_string()),
                })
                .collect();
            let docs: BTreeMap<Id, Document> = values
                .iter()
                .map(|v| {
                    (
                        serde_json::from_value(v.clone()).unwrap(),
                        Document::from_iter([("id".into(), v.clone())]),
                    )
                })
                .collect();
            let mut segment = build_with(
                &store,
                "ids",
                &schema,
                bf_types::DistanceMetric::CosineDistance,
                &docs,
                BTreeMap::new(),
                1,
                1,
            )
            .await
            .unwrap();
            // The implementation must not need either full dictionary.
            segment.files.remove("ids");
            segment.files.remove("filters");
            let live: RoaringBitmap = (0..1100).filter(|i| i % 7 != 0).collect();
            for legacy in [false, true] {
                if legacy {
                    segment.files.remove("idfilter");
                }
                for operand in [
                    Value::Null,
                    values[0].clone(),
                    values[255].clone(),
                    values[256].clone(),
                    values[1099].clone(),
                ] {
                    for op in ["Eq", "NotEq", "Lt", "Lte", "Gt", "Gte"] {
                        let f: Filter = serde_json::from_value(json!(["id", op, operand])).unwrap();
                        let got = filter_rows(&store, &segment, &f, &schema, &live)
                            .await
                            .unwrap();
                        let expected: RoaringBitmap = docs
                            .values()
                            .enumerate()
                            .filter_map(|(i, doc)| {
                                (live.contains(i as u32)
                                    && filter::matches(&f, Some(doc), None, &schema).unwrap())
                                .then_some(i as u32)
                            })
                            .collect();
                        assert_eq!(got, expected, "{kind} {op} {operand} legacy={legacy}");
                    }
                }
            }
        }
    }

    /// Fat string IDs make a 16-block column that is large enough for the full probe count.
    #[tokio::test]
    async fn kary_seek_matches_document_evaluation_at_every_block_edge() {
        let store = Storage::open(&bf_storage::StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let schema: Schema = serde_json::from_value(json!({"id": {"type":"string"}})).unwrap();
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut noise = || {
            (0..180)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    char::from(b'a' + (seed % 26) as u8)
                })
                .collect::<String>()
        };
        let values: Vec<Value> = (0..4000u64)
            .map(|i| json!(format!("{:05}-{}", i * 2, noise())))
            .collect();
        let docs: BTreeMap<Id, Document> = values
            .iter()
            .map(|v| {
                (
                    serde_json::from_value(v.clone()).unwrap(),
                    Document::from_iter([("id".into(), v.clone())]),
                )
            })
            .collect();
        let mut segment = build_with(
            &store,
            "fat",
            &schema,
            bf_types::DistanceMetric::CosineDistance,
            &docs,
            BTreeMap::new(),
            1,
            1,
        )
        .await
        .unwrap();
        let whole = segment.clone();
        segment.files.remove("ids");
        segment.files.remove("filters");
        let docs_dir = directory(&ObjectReader::new(&store, &segment.files["docs"]))
            .await
            .unwrap();
        assert_eq!(docs_dir.columns["id"].blocks.len(), 16);
        let live: RoaringBitmap = (0..4000).filter(|i| i % 11 != 0).collect();
        let mut operands = vec![Value::Null, json!("00000"), json!("99999"), json!("")];
        for row in [
            0usize, 1, 255, 256, 257, 511, 512, 1999, 2000, 2047, 2048, 3743, 3744, 3999,
        ] {
            let id = values[row].as_str().unwrap();
            operands.push(values[row].clone());
            // Absent IDs between rows: just before and just after this one.
            operands.push(json!(format!("{}", &id[..5])));
            operands.push(json!(format!("{}~", &id[..5])));
        }
        for legacy in [false, true] {
            if legacy {
                segment.files.remove("idfilter");
            }
            for operand in &operands {
                for op in ["Eq", "NotEq", "Lt", "Lte", "Gt", "Gte"] {
                    let f: Filter = serde_json::from_value(json!(["id", op, operand])).unwrap();
                    let got = filter_rows(&store, &segment, &f, &schema, &live)
                        .await
                        .unwrap();
                    let expected: RoaringBitmap = docs
                        .values()
                        .enumerate()
                        .filter_map(|(i, doc)| {
                            (live.contains(i as u32)
                                && filter::matches(&f, Some(doc), None, &schema).unwrap())
                            .then_some(i as u32)
                        })
                        .collect();
                    assert_eq!(got, expected, "{op} {operand} legacy={legacy}");
                }
            }
        }
        let list: Vec<&Value> = vec![&values[0], &values[777], &values[3999], &operands[3]];
        let f: Filter = serde_json::from_value(json!(["id", "In", list])).unwrap();
        let got = filter_rows(&store, &whole, &f, &schema, &live)
            .await
            .unwrap();
        let expected: RoaringBitmap = docs
            .values()
            .enumerate()
            .filter_map(|(i, doc)| {
                (live.contains(i as u32) && filter::matches(&f, Some(doc), None, &schema).unwrap())
                    .then_some(i as u32)
            })
            .collect();
        assert_eq!(got, expected);
    }
}
