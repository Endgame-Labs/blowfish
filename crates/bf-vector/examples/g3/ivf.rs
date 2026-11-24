//! Resident production IVF ranking and scan, with the same global bound and exact rerank cap.
use super::{
    data::{Result, Vectors},
    graph::Hit,
};
use bf_vector::{Bytes, Directory, Fragment, Index, RangeRead};
use futures::executor::block_on;
use std::{fs::File, io, ops::Range, os::unix::fs::FileExt, path::Path};
pub struct Reader {
    file: File,
    len: u64,
}
impl RangeRead for Reader {
    fn len(&self) -> u64 {
        self.len
    }
    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        let mut b = vec![0; (range.end - range.start) as usize];
        self.file.read_exact_at(&mut b, range.start)?;
        Ok(Bytes::from(b))
    }
}
pub struct Ivf {
    dir: Directory,
    indexes: Vec<Index<Reader>>,
    codes: Vec<Vec<Bytes>>,
    rowmap: Vec<u32>,
    rows: usize,
}
impl Ivf {
    pub fn load(path: &Path) -> Result<Self> {
        let mut files: Vec<_> = std::fs::read_dir(path)?
            .filter_map(|p| p.ok())
            .map(|p| p.path())
            .filter(|p| p.extension().is_some_and(|s| s == "bfa"))
            .collect();
        files.sort();
        let mut sources = Vec::new();
        let mut indexes = Vec::new();
        let mut codes = Vec::new();
        let mut rows = 0;
        for (i, path) in files.into_iter().enumerate() {
            let bytes = std::fs::read(&path)?;
            let meta = bf_vector::metadata_len(&bytes)?;
            sources.push((
                i.to_string(),
                i.to_string(),
                i.to_string(),
                Fragment::decode(&bytes[..meta as usize])?,
            ));
            let len = bytes.len() as u64;
            drop(bytes);
            let reader = Reader {
                file: File::open(path)?,
                len,
            };
            let index = block_on(Index::open_with_metadata_len(reader, meta))?;
            rows += index.len();
            codes.push(block_on(
                index.read_codes(&(0..index.cluster_count()).collect::<Vec<_>>()),
            )?);
            indexes.push(index);
        }
        let dir = Directory::decode(&Directory::build(&sources)?.ok_or("no IVF fragments")?)?;
        let bytes = std::fs::read(path.join("rowmap.u32"))?;
        let rowmap = bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|b| u32::from_le_bytes(*b))
            .collect();
        eprintln!(
            "ivf rows={rows} leaves={} slabs={}",
            dir.entries().len(),
            indexes.len()
        );
        Ok(Self {
            dir,
            indexes,
            codes,
            rowmap,
            rows,
        })
    }
    pub fn search(&self, v: &Vectors, q: &[f32], probes: usize) -> (Vec<Hit>, usize, usize) {
        let rotated = self.dir.rotate_query(q).unwrap();
        let distances = self.dir.center_distances_rotated(&rotated, false).unwrap();
        let mut order: Vec<_> = distances.iter().enumerate().map(|(i, d)| (*d, i)).collect();
        let probes = ((probes as f64 * self.dir.entries().len() as f64 / (self.rows as f64).sqrt())
            .ceil() as usize)
            .min(order.len());
        if probes < order.len() {
            order.select_nth_unstable_by(probes, |a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            order.truncate(probes);
        }
        order.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let mut clusters = vec![Vec::new(); self.indexes.len()];
        let mut rows = 0;
        for (_, i) in order {
            let e = &self.dir.entries()[i];
            clusters[e.segment].push(e.cluster);
            rows += e.rows as usize;
        }
        let mut pool = Vec::new();
        for (i, cs) in clusters.iter().enumerate().filter(|(_, cs)| !cs.is_empty()) {
            let blocks = cs.iter().map(|c| self.codes[i][*c].clone()).collect();
            pool.extend(
                self.indexes[i]
                    .scan_blocks_bounded_rotated(&rotated, cs, blocks, None, 10)
                    .unwrap(),
            );
        }
        if pool.len() > 10 {
            let mut upper: Vec<_> = pool.iter().map(|c| c.distance + c.error).collect();
            upper.select_nth_unstable_by(9, f32::total_cmp);
            let cut = upper[9];
            pool.retain(|c| c.distance - c.error <= cut);
        }
        if pool.len() > 320 {
            pool.select_nth_unstable_by(320, |a, b| {
                a.distance.total_cmp(&b.distance).then(a.row.cmp(&b.row))
            });
            pool.truncate(320);
        }
        let count = pool.len();
        let mut hits: Vec<_> = pool
            .into_iter()
            .map(|c| {
                let id = self.rowmap[c.row as usize];
                Hit {
                    id,
                    distance: v.rerank(q, id),
                }
            })
            .collect();
        hits.sort_unstable();
        hits.truncate(10);
        (hits, rows, count)
    }
}
impl Ivf {
    pub fn leaves(&self) -> Vec<Vec<u32>> {
        self.indexes
            .iter()
            .flat_map(|index| {
                (0..index.cluster_count()).map(|c| {
                    let (ids, _) = block_on(index.read_cluster_rows(c)).unwrap();
                    ids.into_iter().map(|id| self.rowmap[id as usize]).collect()
                })
            })
            .collect()
    }
}
