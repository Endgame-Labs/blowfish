use super::{HotGraph, check, quant, vamana};
use crate::{Error, Result, rabitq::RaBitQ};
use half::f16;
use numkong::Dot;
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
struct Spill(PathBuf);
impl Drop for Spill {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
/// Streaming SQ8 collector for a disposable, whole-tier row graph.
/// Dropping it removes its owned spill. Run collection and `finish` on a bounded CPU pool.
/// Original f16 rows are never retained or reread during construction.
pub struct HotGraphBuilder {
    spill: Spill,
    codes: BufWriter<File>,
    transform: RaBitQ,
    dims: usize,
    count: usize,
    rows: Vec<u32>,
    sum: Vec<f64>,
    first: Vec<f32>,
    norms: Vec<crate::CosineNorm>,
}
impl HotGraphBuilder {
    /// Create a uniquely owned spill under `root`. No existing directory is modified.
    /// At most `count` rows may be appended, in immutable source-leaf order.
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
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(spill.0.join("codes"))?;
        uncached(&file)?;
        Ok(Self {
            spill,
            codes: BufWriter::with_capacity(1 << 20, file),
            transform: RaBitQ::new(dims, 42)?,
            dims,
            count,
            rows: Vec::with_capacity(count),
            sum: vec![0.; dims],
            first: Vec::new(),
            norms: Vec::with_capacity(count),
        })
    }
    /// Append little-endian f16 rows. Immutable tier coordinates are preserved explicitly;
    /// graph node IDs follow arrival order. Nonfinite and zero vectors are rejected.
    /// Cancellation is checked for each row. Drop the builder after an I/O or cancellation error.
    pub fn append(
        &mut self,
        rows: &[u32],
        packed: &[u8],
        cancelled: &(impl Fn() -> bool + Sync),
    ) -> Result<()> {
        if self.rows.len().saturating_add(rows.len()) > self.count
            || rows.len().checked_mul(self.dims * 2) != Some(packed.len())
        {
            return Err(Error::InvalidInput("invalid graph source rows".into()));
        }
        // Validate the entire append before changing its logical state.
        for row in packed.chunks_exact(self.dims * 2) {
            check(cancelled)?;
            let norm = row
                .as_chunks::<2>()
                .0
                .iter()
                .map(|b| f16::from_le_bytes(*b).to_f64().powi(2))
                .sum::<f64>();
            if !norm.is_finite() || norm == 0. {
                return Err(Error::InvalidInput(
                    "graph requires finite, nonzero rows".into(),
                ));
            }
        }
        for row in packed.chunks_exact(self.dims * 2) {
            check(cancelled)?;
            let values = row
                .as_chunks::<2>()
                .0
                .iter()
                .map(|b| f16::from_le_bytes(*b))
                .collect::<Vec<_>>();
            // SAFETY: both transparent half wrappers have identical layout and alignment.
            let half = unsafe {
                std::slice::from_raw_parts(values.as_ptr().cast::<numkong::f16>(), values.len())
            };
            let original = values.iter().map(|x| x.to_f32()).collect::<Vec<_>>();
            self.norms.push(crate::CosineNorm::new(&original)?);
            let inv = 1. / numkong::f16::dot(half, half).unwrap().sqrt();
            let normalized = if inv.is_finite() && inv > 0. {
                values.iter().map(|x| x.to_f32() * inv).collect::<Vec<_>>()
            } else {
                let norm = values
                    .iter()
                    .map(|x| x.to_f64().powi(2))
                    .sum::<f64>()
                    .sqrt();
                values
                    .iter()
                    .map(|x| (x.to_f64() / norm) as f32)
                    .collect::<Vec<_>>()
            };
            if self.first.is_empty() {
                self.first.clone_from(&normalized);
            }
            for (sum, value) in self.sum.iter_mut().zip(&normalized) {
                *sum += f64::from(*value);
            }
            let (values, scale) = quant::encode(&self.transform.rotate(&normalized));
            self.codes.write_all(&scale.to_le_bytes())?;
            // SAFETY: i8 and u8 have identical layout, and admit every bit pattern.
            let bytes =
                unsafe { std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), values.len()) };
            self.codes.write_all(bytes)?;
        }
        self.rows.extend_from_slice(rows);
        Ok(())
    }
    /// Build one monolithic R32/L64 SQ8 Vamana graph, with two passes and bounded adjacency.
    /// Construction checks cancellation between source rows, graph points, and bounded phases.
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
        self.codes.flush()?;
        self.codes.get_ref().sync_all()?;
        let file = File::open(self.spill.0.join("codes"))?;
        uncached(&file)?;
        let padded = self.dims.next_power_of_two();
        if file.metadata()?.len() != (self.count * (padded + 4)) as u64 {
            return Err(Error::InvalidInput("incomplete graph codes".into()));
        }
        let mut reader = BufReader::with_capacity(1 << 20, file);
        let mut values = Vec::with_capacity(self.count * padded);
        let mut scales = Vec::with_capacity(self.count);
        let mut row = vec![0; padded];
        for id in 0..self.count {
            check(cancelled)?;
            let mut scale = [0; 4];
            reader.read_exact(&mut scale)?;
            scales.push(f32::from_le_bytes(scale));
            reader.read_exact(&mut row)?;
            values.extend(row.iter().map(|x| *x as i8));
            if id.is_multiple_of(8192) {
                discard(reader.get_ref())?;
            }
        }
        discard(reader.get_ref())?;
        drop(reader);
        let codes = quant::Codes {
            dims: padded,
            values,
            scales,
        };
        let norm = self.sum.iter().map(|x| x * x).sum::<f64>().sqrt();
        let center = if norm > 0. {
            self.sum.iter().map(|x| (x / norm) as f32).collect()
        } else {
            self.first.clone()
        };
        let (query, scale) = codes.prepare(&center);
        let mut best = (0, f32::INFINITY);
        for id in 0..self.count as u32 {
            check(cancelled)?;
            let distance = codes.score(&query, scale, id);
            if distance < best.1 {
                best = (id, distance);
            }
        }
        let nodes = vamana::build(&codes, best.0, cancelled)?;
        let mut links = Vec::with_capacity(self.count);
        let mut degrees = Vec::with_capacity(self.count);
        for (id, neighbors) in nodes.into_iter().enumerate() {
            check(cancelled)?;
            let neighbors = neighbors.into_inner();
            let mut row = [0; 34];
            let mut count = neighbors.as_slice().len();
            row[..count].copy_from_slice(neighbors.as_slice());
            for other in [(id + 1) % self.count, (id + self.count - 1) % self.count] {
                if other != id && !row[..count].contains(&(other as u32)) {
                    row[count] = other as u32;
                    count += 1;
                }
            }
            links.push(row);
            degrees.push(count as u8);
        }
        Ok(HotGraph {
            dims: self.dims,
            rows: std::mem::take(&mut self.rows),
            links,
            degrees,
            entries: vec![best.0],
            codes,
            norms: std::mem::take(&mut self.norms),
        })
    }
}
fn uncached(file: &File) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: F_NOCACHE changes only this valid, privately owned spill descriptor.
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
        // SAFETY: the code spool was flushed before reading, and is immutable here.
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
