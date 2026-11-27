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
    if op == FilterOp::In {
        return Ok(lookup_in(store, segment, value, schema, live.is_empty())
            .await?
            .map(|rows| rows & live));
    }
    let Some(range) = row_range(store, segment, op, value, schema, live.is_empty()).await? else {
        return Ok(None);
    };
    let mut rows = RoaringBitmap::new();
    rows.insert_range(range);
    Ok(Some(if op == FilterOp::NotEq {
        live - rows
    } else {
        rows & live
    }))
}

/// Start seeking an ID predicate's rows while the caller loads the segment's live rows, which
/// the seek does not need: the directory and ID blocks it reads stay in the caches, so the
/// evaluation after the live load costs no reads. Failures are left for the evaluation to report.
pub(super) async fn prefetch(store: &Storage, segment: &Segment, f: &Filter, schema: &Schema) {
    if let Filter::Compare {
        attribute,
        op,
        value,
        ..
    } = f
        && attribute == "id"
    {
        if *op == FilterOp::In {
            let _ = Box::pin(lookup_in(store, segment, value, schema, false)).await;
        } else {
            let _ = Box::pin(row_range(store, segment, *op, value, schema, false)).await;
        }
    }
}

/// The interval of rows an ID comparison selects (for `NotEq`, the rows it excludes), or `None`
/// when the operator is not a scalar comparison. With no live rows, nothing is read.
async fn row_range(
    store: &Storage,
    segment: &Segment,
    op: FilterOp,
    value: &Value,
    schema: &Schema,
    no_live_rows: bool,
) -> Result<Option<std::ops::Range<u32>>, ApiError> {
    use FilterOp::*;
    if !matches!(op, Eq | NotEq | Lt | Lte | Gt | Gte) {
        return Ok(None);
    }
    let operand = if let Some(t) = schema.get("id").and_then(|s| s.data_type.as_ref()) {
        crate::write::normalize_value(value, t)?
    } else {
        value.clone()
    };
    if no_live_rows {
        return Ok(Some(0..0));
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
    Ok(Some(match op {
        Eq | NotEq => low..high,
        Lt => 0..low,
        Lte => 0..high,
        Gt => high..segment.row_count,
        Gte => low..segment.row_count,
        _ => unreachable!(),
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
const MAX_PROBES: usize = 16;
/// The second wave reads one region of about this many bytes of the ID column, which the
/// cache coalesces into a single GET (32 sparse blocks of 16 KiB).
const REGION_BYTES: u64 = 512 << 10;

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

    /// The first block of each probe region. The probes divide the column into regions of about
    /// REGION_BYTES. At least two probes keep a region from being the whole column; a column of
    /// one block has one probe.
    fn starts(&self) -> Vec<usize> {
        let nb = self.blocks();
        let column_bytes: u64 = self.dir.columns["id"].blocks.iter().map(|b| b.length).sum();
        let k = usize::try_from(column_bytes.div_ceil(REGION_BYTES))
            .unwrap_or(MAX_PROBES)
            .clamp(2, MAX_PROBES)
            .min(nb);
        (0..k).map(|t| t * nb / k).collect()
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
/// evenly spaced blocks, plus the last block, all at once, which brackets the boundary in one region of the
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
    let starts = seeker.starts();
    let k = starts.len();
    // Wave one: the first ID of every probe block, and the last ID of the column's last
    // block. Probe 0 is included, since without the ID-bounds header nothing says that the
    // column's first ID precedes the boundary; the last block settles a segment whose every
    // ID precedes it without a second wave.
    let mut wanted = starts.clone();
    if starts.last() != Some(&(nb - 1)) {
        wanted.push(nb - 1);
    }
    let probes = seeker.fetch(&wanted).await?;
    if let Some(t) = trace {
        eprintln!(
            "ID_SEEK {} wave1 {} {}",
            seeker.docs_key,
            wanted.len(),
            t.elapsed().as_micros()
        );
    }
    let last = probes.last().ok_or_else(corrupt)?;
    if before(id_at(last, last.len() - 1)?) {
        return Ok(segment.row_count);
    }
    let mut firsts = Vec::with_capacity(k);
    for values in &probes[..k] {
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

/// The rows holding the IDs of an `In` list, or `None` when the dictionary path is cheaper (or the
/// operand is not a plain list). All IDs share one probe wave and one fetch of the regions they
/// land in, so a short list costs about what one `Eq` does. The planner compares the bytes the
/// second wave would read with the bytes of the segment's ID dictionary, which is what the
/// general path would load, and keeps the dictionary path when the seek would read more or when
/// the dictionary is already decoded in the cache. Live rows are not consulted.
async fn lookup_in(
    store: &Storage,
    segment: &Segment,
    value: &Value,
    schema: &Schema,
    no_live_rows: bool,
) -> Result<Option<RoaringBitmap>, ApiError> {
    let Some(items) = value.as_array() else {
        return Ok(None);
    };
    let t = schema.get("id").and_then(|s| s.data_type.as_ref());
    let mut operands = Vec::with_capacity(items.len());
    for item in items {
        operands.push(match t {
            Some(t) => crate::write::normalize_value(item, t)?,
            None => item.clone(),
        });
    }
    if no_live_rows {
        return Ok(Some(RoaringBitmap::new()));
    }
    operands.sort_by(filter::compare);
    operands.dedup_by(|a, b| filter::compare(a, b).is_eq());
    // Segments whose ID range excludes every listed ID read nothing beyond the range header.
    if let Some((min, max)) = bounds(store, segment).await? {
        operands.retain(|v| filter::compare(v, &min).is_ge() && filter::compare(v, &max).is_le());
    }
    let mut rows = RoaringBitmap::new();
    if operands.is_empty() {
        return Ok(Some(rows));
    }
    let filters = segment.files.get("filters");
    if let Some(file) = filters
        && read_cache::get::<Vec<(Value, RoaringBitmap)>>(&format!("{}:id", file.key), 0).is_some()
    {
        return Ok(None);
    }
    let seeker = Seeker::open(store, segment).await?;
    if seeker.staged {
        for operand in &operands {
            let lt = |v: &Value| filter::compare(v, operand).is_lt();
            let le = |v: &Value| filter::compare(v, operand).is_le();
            let low = staged_boundary(store, segment, &lt).await?;
            if staged_boundary(store, segment, &le).await? > low {
                rows.insert(low);
            }
        }
        return Ok(Some(rows));
    }
    let nb = seeker.blocks();
    let starts = seeker.starts();
    let k = starts.len();
    let mut wanted = starts.clone();
    if starts.last() != Some(&(nb - 1)) {
        wanted.push(nb - 1);
    }
    // Wave one, with the dictionary's size, which the cost model needs: the first ID of every
    // probe block and the last ID of the column.
    let dictionary = async {
        let Some(file) = filters else {
            return u64::MAX;
        };
        match directory(&ObjectReader::new(store, file)).await {
            Ok(dir) => dir
                .columns
                .get("id")
                .and_then(|c| c.blocks.first())
                .map_or(u64::MAX, |b| b.length),
            Err(_) => u64::MAX,
        }
    };
    let (probes, dictionary_bytes) = futures::future::join(seeker.fetch(&wanted), dictionary).await;
    let probes = probes?;
    let last = probes.last().ok_or_else(corrupt)?;
    let last_id = id_at(last, last.len() - 1)?;
    let firsts: Vec<&Value> = probes[..k]
        .iter()
        .map(|values| id_at(values, 0))
        .collect::<Result<_, _>>()?;
    let mut regions: BTreeMap<usize, Vec<&Value>> = BTreeMap::new();
    for operand in &operands {
        if filter::compare(last_id, operand).is_lt() {
            continue;
        }
        // The last probe whose first ID is not after the operand brackets it.
        let after = firsts.partition_point(|f| filter::compare(f, operand).is_le());
        if after > 0 {
            regions.entry(after - 1).or_default().push(operand);
        }
    }
    let span = |s: usize| starts[s]..starts.get(s + 1).copied().unwrap_or(nb);
    let col = &seeker.dir.columns["id"];
    let seek_bytes: u64 = regions
        .keys()
        .map(|&s| col.blocks[span(s)].iter().map(|b| b.length).sum::<u64>())
        .sum();
    if seek_bytes > dictionary_bytes {
        return Ok(None);
    }
    // Wave two: every needed region in one batch, searched in memory.
    let indices: Vec<usize> = regions.keys().flat_map(|&s| span(s)).collect();
    let blocks = seeker.fetch(&indices).await?;
    let mut at = 0;
    for (&s, operands) in &regions {
        let range = span(s);
        let region = &blocks[at..at + range.len()];
        at += range.len();
        for operand in operands {
            let n = region.partition_point(|values| {
                values
                    .last()
                    .and_then(Option::as_ref)
                    .is_some_and(|v| filter::compare(v, operand).is_lt())
            });
            let Some(values) = region.get(n) else {
                continue;
            };
            let p = values.partition_point(|v| {
                v.as_ref()
                    .is_some_and(|v| filter::compare(v, operand).is_lt())
            });
            if values
                .get(p)
                .and_then(Option::as_ref)
                .is_some_and(|v| filter::compare(v, operand).is_eq())
            {
                rows.insert(((range.start + n) * BLOCK_ROWS + p) as u32);
            }
        }
    }
    Ok(Some(rows))
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
                // `In` lists: duplicates, absent IDs between and beyond the range, a null, and
                // IDs on live holes and block edges, all through the seek (no dictionary here).
                let absent = match kind {
                    "uint" => json!(u64::MAX - 4000 + 1),
                    "string" => json!("doc-0001-é"),
                    _ => json!(uuid::Uuid::from_u128(1).to_string()),
                };
                let lists = [
                    vec![],
                    vec![values[0].clone()],
                    vec![
                        values[1099].clone(),
                        values[1099].clone(),
                        values[7].clone(),
                    ],
                    vec![absent.clone()],
                    vec![
                        values[255].clone(),
                        values[256].clone(),
                        values[512].clone(),
                        absent.clone(),
                        values[0].clone(),
                        values[3].clone(),
                        values[600].clone(),
                    ],
                    values.clone(),
                ];
                for list in lists {
                    let f: Filter = serde_json::from_value(json!(["id", "In", list])).unwrap();
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
                    assert_eq!(got, expected, "{kind} In {} legacy={legacy}", list.len());
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
            operands.push(json!(id[..5].to_string()));
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
        for legacy in [false, true] {
            let target = if legacy { &whole } else { &segment };
            let edges = [0usize, 255, 256, 511, 512, 2047, 2048, 3743, 3744, 3999];
            let lists: Vec<Vec<Value>> = vec![
                vec![values[3743].clone(), values[3743].clone()],
                edges.iter().map(|&r| values[r].clone()).collect(),
                edges
                    .iter()
                    .map(|&r| json!(format!("{}~", &values[r].as_str().unwrap()[..5])))
                    .collect(),
                values.iter().step_by(3).cloned().collect(),
                vec![json!("zzzzz"), json!(""), values[1].clone()],
            ];
            for list in lists {
                let f: Filter = serde_json::from_value(json!(["id", "In", list])).unwrap();
                let got = filter_rows(&store, target, &f, &schema, &live)
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
                assert_eq!(got, expected, "In {} whole={legacy}", list.len());
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

    /// Through the whole engine: an indexed segment plus WAL deletes, overwrites, and new rows.
    /// `id In` (alone, duplicated, absent, deleted, overwritten, and under `And`/`Or`/`Not` with
    /// another filter) must return the reference evaluator's ordered IDs and distances.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn id_in_matches_the_reference_with_deletes_and_overwrites() {
        use crate::*;
        let store = Storage::open(&bf_storage::StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let config = EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..Default::default()
        };
        let engine = Engine::with_config(store.clone(), config.clone());
        let row = |id: u64, step: u64| {
            let vector: Vec<f32> = (0..16)
                .map(|d| ((id * 31 + d * 7 + step * 13) % 97) as f32 / 97.0 - 0.4)
                .collect();
            json!({"id":id,"group":(id + step) % 5,"vector":vector})
        };
        engine
            .write(
                "idin",
                serde_json::from_value(json!({"distance_metric":"cosine_distance","schema":{"vector":{"type":"[16]f32"}},
                    "upsert_rows":(0..2300).map(|id| row(id, 0)).collect::<Vec<_>>()}))
                .unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("idin", true).await.unwrap();
        for step in 0..2u64 {
            if step == 1 {
                // WAL changes on top of the segment: overwrite 10 and 11 with new groups,
                // delete 12 and 2299, and add 5000.
                engine
                    .write(
                        "idin",
                        serde_json::from_value(json!({
                            "upsert_rows":[row(10, 7), row(11, 8), row(5000, 1)],
                            "deletes":[12, 2299]}))
                        .unwrap(),
                    )
                    .await
                    .unwrap();
            }
            let (state, _) = engine.snapshot("idin", &Consistency::Strong).await.unwrap();
            let full = snapshot::materialize(&store, &state).await.unwrap();
            let context = read_cache::ReadContext::new(engine.0.read_cache.clone());
            read_cache::scope(context, async {
                let lists = [
                    json!([5]),
                    json!([5, 5, 5]),
                    json!([9, 10, 11, 12, 13]),
                    json!([2298, 2299, 2300, 5000, 123456]),
                    json!([100000, 100001]),
                    json!((0..300).map(|i| i * 7).collect::<Vec<_>>()),
                    json!([]),
                ];
                for list in lists {
                    for filters in [
                        json!(["id", "In", list]),
                        json!(["And", [["id", "In", list], ["group", "In", [0, 1]]]]),
                        json!(["Or", [["id", "In", list], ["group", "Eq", 4]]]),
                        json!(["Not", ["id", "In", list]]),
                        json!(["And", [["id", "In", list], ["id", "Lt", 1000]]]),
                    ] {
                        let request: QueryRequest = serde_json::from_value(json!({
                            "rank_by":["vector","ANN",[0.3,0.1,0.2,0.0,0.4,0.1,0.2,0.3,0.1,0.0,0.2,0.2,0.1,0.3,0.0,0.1]],
                            "limit":{"total":40},"include_attributes":["group"],"filters":filters}))
                        .unwrap();
                        let expected = query::execute(&full, &request).unwrap().result.rows;
                        for _ in 0..2 {
                            let got = indexed::execute(&store, &state, &request, &config, false)
                                .await
                                .unwrap()
                                .result
                                .rows;
                            assert_eq!(got, expected, "step {step} {request:?}");
                        }
                    }
                }
            })
            .await;
        }
    }
}
