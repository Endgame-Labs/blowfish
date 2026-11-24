use super::{
    HotGraph, check, quant,
    source::Rows,
    vamana,
    vectors::{Matrix, Vectors},
};
use crate::{Error, Result};
use half::f16;
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
const DEGREE: usize = 32;
const RECORD: usize = (DEGREE + 1) * 4;
struct Spill(PathBuf);
impl Drop for Spill {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Partition {
    ids: Vec<u32>,
    center: Vec<f16>,
}
/// Streaming source collector for a disposable graph. Dropping it removes all owned spill files.
/// Construction is synchronous; call `finish` inside a dedicated, bounded CPU pool.
pub struct HotGraphBuilder {
    spill: Spill,
    source: File,
    dims: usize,
    count: usize,
    rows: Vec<u32>,
    groups: BTreeMap<usize, Vec<u32>>,
}
impl HotGraphBuilder {
    /// Create uniquely owned spill files under `root`. No existing directory is modified.
    /// At most `count` rows may be appended, in clusters from one or more spatial partitions.
    pub fn new(root: &Path, dims: usize, count: usize) -> Result<Self> {
        if HotGraph::reservation(count, dims, 1).is_none() {
            return Err(Error::InvalidInput("unsupported graph source shape".into()));
        }
        std::fs::create_dir_all(root)?;
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = root.join(format!(
            "hot-{}-{stamp}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path)?;
        let spill = Spill(path);
        let source = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(spill.0.join("source"))?;
        uncached(&source)?;
        Ok(Self {
            spill,
            source,
            dims,
            count,
            rows: Vec::with_capacity(count),
            groups: BTreeMap::new(),
        })
    }
    /// Append validated little-endian f16 rows from a spatial source partition (normally a slab).
    /// `rows` are immutable tier coordinates; graph node IDs are assigned densely in arrival order.
    pub fn append(&mut self, partition: usize, rows: &[u32], packed: &[u8]) -> Result<()> {
        if self.rows.len().saturating_add(rows.len()) > self.count
            || rows.len().checked_mul(self.dims * 2) != Some(packed.len())
        {
            return Err(Error::InvalidInput("invalid graph source rows".into()));
        }
        for row in packed.chunks_exact(self.dims * 2) {
            let mut norm = 0f64;
            for b in row.as_chunks::<2>().0 {
                let x = f16::from_le_bytes(*b).to_f64();
                norm += x * x;
            }
            if !norm.is_finite() || norm == 0. {
                return Err(Error::InvalidInput(
                    "graph requires finite, nonzero rows".into(),
                ));
            }
        }
        let start = self.rows.len();
        self.source.write_all(packed)?;
        self.groups
            .entry(partition)
            .or_default()
            .extend((start..start + rows.len()).map(|i| i as u32));
        self.rows.extend_from_slice(rows);
        if self.rows.len() / 8192 > start / 8192 {
            discard(&self.source)?;
        }
        Ok(())
    }
    /// Build two-pass R32/L64 Vamana partitions, stitch boundary rows, stream SQ8 codes,
    /// and load compact immutable links. Every CPU loop checks `cancelled` between rows.
    /// Oversized and skewed partitions are recursively median-split before construction.
    pub fn finish(mut self, cancelled: &(impl Fn() -> bool + Sync)) -> Result<HotGraph> {
        check(cancelled)?;
        if self.rows.len() != self.count {
            return Err(Error::InvalidInput("incomplete graph source".into()));
        }
        let mut sorted = self.rows.clone();
        sorted.sort_unstable();
        if sorted.windows(2).any(|p| p[0] == p[1]) {
            return Err(Error::InvalidInput("duplicate tier row".into()));
        }
        drop(sorted);
        let cap = (384 * 1024 * 1024 / (self.dims * 5 + 1024)).clamp(256, 65536);
        discard(&self.source)?;
        let source = Rows::new(&self.source, self.dims, self.count)?;
        let mut parts = Vec::new();
        for (_, ids) in std::mem::take(&mut self.groups) {
            // Compute source means without loading their vectors. Boundary overlap must
            // connect coarse slabs before size-capped subdivision.
            split(&source, self.dims, ids, usize::MAX, &mut parts, cancelled)?;
        }
        // Boundary overlap is geometric, rather than a row-count quota. A second subdivision
        // below enforces the memory cap even when every row belongs to the same boundary.
        let mut primary = vec![0usize; self.count];
        for (p, part) in parts.iter().enumerate() {
            for &id in &part.ids {
                primary[id as usize] = p;
            }
        }
        let mut overlaps = Vec::new();
        for (id, &p) in primary.iter().enumerate() {
            check(cancelled)?;
            if id.is_multiple_of(8192) {
                discard(&self.source)?;
            }
            let row = read_row(&source, self.dims, id as u32)?;
            let mut best = (usize::MAX, f32::INFINITY);
            let mut second = best;
            for (i, part) in parts.iter().enumerate() {
                let d = cosine(&row, &part.center);
                if d < best.1 {
                    second = best;
                    best = (i, d);
                } else if d < second.1 {
                    second = (i, d);
                }
            }
            if second.0 != usize::MAX && second.1 <= best.1 * 1.07 {
                let secondary = if best.0 == p { second.0 } else { best.0 };
                if secondary != p {
                    overlaps.push((secondary, id as u32));
                }
            }
        }
        for (p, id) in overlaps {
            parts[p].ids.push(id);
        }
        drop(primary);
        let mut bounded = Vec::new();
        for part in parts {
            split(&source, self.dims, part.ids, cap, &mut bounded, cancelled)?;
        }
        let adjacency = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(self.spill.0.join("links"))?;
        adjacency.set_len((self.count * RECORD) as u64)?;
        let codefile = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(self.spill.0.join("codes"))?;
        uncached(&adjacency)?;
        uncached(&codefile)?;
        discard(&self.source)?;
        let padded = self.dims.next_power_of_two();
        codefile.set_len((self.count * (padded + 4)) as u64)?;
        let mut present = vec![false; self.count];
        let mut entries = Vec::new();
        for part in bounded {
            check(cancelled)?;
            let mut packed = vec![0; part.ids.len() * self.dims * 2];
            for (&id, out) in part.ids.iter().zip(packed.chunks_exact_mut(self.dims * 2)) {
                check(cancelled)?;
                source.copy(id, out)?;
            }
            let vectors = Vectors::from_matrix(Matrix {
                rows: part.ids.len(),
                cols: self.dims,
                bytes: packed,
            });
            let entry = (0..part.ids.len() as u32)
                .min_by(|a, b| {
                    vectors
                        .query(&part.center, *a)
                        .total_cmp(&vectors.query(&part.center, *b))
                })
                .unwrap();
            entries.push(part.ids[entry as usize]);
            let local: BTreeMap<_, _> = part
                .ids
                .iter()
                .enumerate()
                .map(|(i, &id)| (id, i as u32))
                .collect();
            let codes = quant::Codes::build(&vectors);
            let links = vamana::build(&vectors, entry, DEGREE, 64, cancelled)?;
            for (i, &id) in part.ids.iter().enumerate() {
                check(cancelled)?;
                let mut neighbors = links[i]
                    .iter()
                    .map(|p| part.ids[*p as usize])
                    .collect::<Vec<_>>();
                if present[id as usize] {
                    let mut record = [0; RECORD];
                    adjacency.read_exact_at(&mut record, id as u64 * RECORD as u64)?;
                    let n = u32::from_le_bytes(record[..4].try_into().unwrap()) as usize;
                    neighbors.extend(
                        record[4..4 + n * 4]
                            .as_chunks::<4>()
                            .0
                            .iter()
                            .map(|b| u32::from_le_bytes(*b)),
                    );
                    neighbors.sort_unstable();
                    neighbors.dedup();
                    let mut rows = BTreeMap::new();
                    rows.insert(id, vectors.row(i as u32).to_vec());
                    for &p in &neighbors {
                        let row = match local.get(&p) {
                            Some(&i) => vectors.row(i).to_vec(),
                            None => read_row(&source, self.dims, p)?,
                        };
                        rows.insert(p, row);
                    }
                    neighbors = vamana::prune(id, neighbors, DEGREE, 1.2, |a, b| {
                        cosine(&rows[&a], &rows[&b])
                    });
                } else {
                    present[id as usize] = true;
                    codefile.write_all_at(
                        &codes.scales[i].to_le_bytes(),
                        id as u64 * (padded + 4) as u64,
                    )?;
                    let bytes = codes.values[i * padded..(i + 1) * padded]
                        .iter()
                        .map(|x| *x as u8)
                        .collect::<Vec<_>>();
                    codefile.write_all_at(&bytes, id as u64 * (padded + 4) as u64 + 4)?;
                }
                let mut record = [0; RECORD];
                record[..4].copy_from_slice(&(neighbors.len() as u32).to_le_bytes());
                for (&p, out) in neighbors.iter().zip(record[4..].as_chunks_mut::<4>().0) {
                    out.copy_from_slice(&p.to_le_bytes());
                }
                adjacency.write_all_at(&record, id as u64 * RECORD as u64)?;
                if i % 4096 == 0 {
                    discard(&self.source)?;
                }
            }
            discard(&self.source)?;
            discard(&adjacency)?;
            discard(&codefile)?;
        }
        if present.iter().any(|p| !*p) {
            return Err(Error::InvalidInput("missing primary graph row".into()));
        }
        drop(present);
        drop(source);
        entries.sort_unstable();
        entries.dedup();
        let mut links = Vec::with_capacity(self.count);
        let mut degrees = Vec::with_capacity(self.count);
        for id in 0..self.count {
            check(cancelled)?;
            let mut record = [0; RECORD];
            adjacency.read_exact_at(&mut record, (id * RECORD) as u64)?;
            let n = u32::from_le_bytes(record[..4].try_into().unwrap()) as usize;
            let mut out = [0; 34];
            for (p, b) in out.iter_mut().zip(record[4..4 + n * 4].as_chunks::<4>().0) {
                *p = u32::from_le_bytes(*b);
            }
            let mut n = n;
            // A ring provides a bounded connectivity guarantee even for degenerate partitions.
            for p in [(id + 1) % self.count, (id + self.count - 1) % self.count] {
                if p != id && !out[..n].contains(&(p as u32)) {
                    out[n] = p as u32;
                    n += 1;
                }
            }
            links.push(out);
            degrees.push(n as u8);
            if id % 8192 == 0 {
                discard(&adjacency)?;
            }
        }
        let mut reader = std::io::BufReader::new(codefile);
        let mut values = Vec::with_capacity(self.count * padded);
        let mut scales = Vec::with_capacity(self.count);
        let mut row = vec![0; padded];
        for id in 0..self.count {
            check(cancelled)?;
            let mut b = [0; 4];
            reader.read_exact(&mut b)?;
            scales.push(f32::from_le_bytes(b));
            reader.read_exact(&mut row)?;
            values.extend(row.iter().map(|x| *x as i8));
            if id % 8192 == 0 {
                discard(reader.get_ref())?;
            }
        }
        Ok(HotGraph {
            dims: self.dims,
            rows: std::mem::take(&mut self.rows),
            links,
            degrees,
            entries,
            codes: quant::Codes {
                dims: padded,
                values,
                scales,
            },
        })
    }
}
fn read_row(file: &Rows<'_>, _dims: usize, id: u32) -> Result<Vec<f16>> {
    file.row(id)
}
fn cosine(a: &[f16], b: &[f16]) -> f32 {
    use numkong::Dot;
    // SAFETY: both transparent wrappers store IEEE binary16 with identical alignment.
    let cast = |v: &[f16]| unsafe {
        std::slice::from_raw_parts(v.as_ptr().cast::<numkong::f16>(), v.len())
    };
    let (a, b) = (cast(a), cast(b));
    (1. - numkong::f16::dot(a, b).unwrap()
        / (numkong::f16::dot(a, a).unwrap() * numkong::f16::dot(b, b).unwrap()).sqrt())
    .max(0.)
}
fn split(
    file: &Rows<'_>,
    dims: usize,
    ids: Vec<u32>,
    cap: usize,
    out: &mut Vec<Partition>,
    cancelled: &(impl Fn() -> bool + Sync),
) -> Result<()> {
    check(cancelled)?;
    if ids.is_empty() {
        return Ok(());
    }
    if ids.len() > cap {
        // Median projection guarantees progress, including duplicate and collinear vectors.
        let a = read_row(file, dims, ids[0])?;
        let b = read_row(file, dims, ids[ids.len() / 2])?;
        let direction = a
            .iter()
            .zip(&b)
            .map(|(a, b)| a.to_f32() - b.to_f32())
            .collect::<Vec<_>>();
        let mut projected = Vec::with_capacity(ids.len());
        for id in ids {
            check(cancelled)?;
            let row = read_row(file, dims, id)?;
            let value = row
                .iter()
                .zip(&direction)
                .map(|(v, d)| v.to_f32() * d)
                .sum::<f32>();
            projected.push((value, id));
        }
        projected.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let right = projected.split_off(projected.len() / 2);
        split(
            file,
            dims,
            projected.into_iter().map(|p| p.1).collect(),
            cap,
            out,
            cancelled,
        )?;
        split(
            file,
            dims,
            right.into_iter().map(|p| p.1).collect(),
            cap,
            out,
            cancelled,
        )?;
    } else {
        let mut sum = vec![0f64; dims];
        for &id in &ids {
            check(cancelled)?;
            let row = read_row(file, dims, id)?;
            let norm = row.iter().map(|x| x.to_f64().powi(2)).sum::<f64>().sqrt();
            for (s, x) in sum.iter_mut().zip(row) {
                *s += x.to_f64() / norm;
            }
        }
        let norm = sum.iter().map(|x| x * x).sum::<f64>().sqrt();
        let center = if norm > 0. {
            sum.into_iter().map(|x| f16::from_f64(x / norm)).collect()
        } else {
            read_row(file, dims, ids[0])?
        };
        out.push(Partition { ids, center });
    }
    Ok(())
}

fn uncached(file: &File) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: F_NOCACHE changes caching for this valid, privately owned spill descriptor.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) } < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    #[cfg(not(target_os = "macos"))]
    let _ = file;
    Ok(())
}
fn discard(file: &File) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        file.sync_data()?;
        // SAFETY: the valid descriptor belongs to this disposable build. Flushed pages are
        // discardable; future reads continue to use the same immutable file contents.
        let result =
            unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        if result != 0 {
            return Err(std::io::Error::from_raw_os_error(result).into());
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = file;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn duplicate_vectors_and_boundary_skew_obey_partition_cap() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../.scratch")
            .join(format!("g4-split-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let cleanup = Spill(root.clone());
        let mut f = File::create(root.join("rows")).unwrap();
        for _ in 0..101 {
            f.write_all(&f16::ONE.to_le_bytes()).unwrap();
            f.write_all(&f16::ZERO.to_le_bytes()).unwrap();
        }
        drop(f);
        let f = File::open(root.join("rows")).unwrap();
        let source = Rows::new(&f, 2, 101).unwrap();
        let mut parts = Vec::new();
        split(&source, 2, (0..101).collect(), 8, &mut parts, &|| false).unwrap();
        assert!(parts.iter().all(|p| !p.ids.is_empty() && p.ids.len() <= 8));
        let mut ids = parts.into_iter().flat_map(|p| p.ids).collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, (0..101).collect::<Vec<_>>());
        drop(cleanup);
    }
}
