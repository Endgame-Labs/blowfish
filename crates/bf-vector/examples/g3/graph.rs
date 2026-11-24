//! HNSW construction with hnsw_rs (MIT OR Apache-2.0), followed by compact immutable links.
use super::data::{Result, Vectors};
use hnsw_rs::prelude::*;
use rayon::prelude::*;
use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
    sync::Arc,
    time::Instant,
};

struct Exact(Arc<Vectors>);
impl Distance<u32> for Exact {
    fn eval(&self, a: &[u32], b: &[u32]) -> f32 {
        self.0.between(a[0], b[0])
    }
}

pub struct Graph {
    pub levels: Vec<Vec<Vec<u32>>>,
    pub entry: u32,
}
impl Graph {
    pub fn build(v: Arc<Vectors>, m: usize, ef: usize) -> Self {
        let start = Instant::now();
        let mut h = Hnsw::<u32, Exact>::new(m, v.len(), 16, ef, Exact(v.clone()));
        h.set_extend_candidates(false);
        h.set_keeping_pruned(false);
        let mut ids: Vec<_> = (0..v.len() as u32).collect();
        // Fixed insertion permutation; parallel scheduling and HNSW layer sampling remain random.
        let mut rng = 7u64;
        for i in (1..ids.len()).rev() {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            ids.swap(i, rng as usize % (i + 1));
        }
        for (chunk, batch) in ids.chunks(10000).enumerate() {
            batch
                .par_iter()
                .for_each(|id| h.insert_slice((&[*id], *id as usize)));
            if chunk % 10 == 0 {
                eprintln!(
                    "build rows={} seconds={:.1} rss_kib={}",
                    ((chunk + 1) * 10000).min(ids.len()),
                    start.elapsed().as_secs_f64(),
                    super::data::rss_kib()
                );
            }
        }
        let mut levels = vec![Vec::new(); v.len()];
        let mut entry = 0;
        for point in h.get_point_indexation().into_iter() {
            let id = point.get_origin_id();
            let mut neighbors = point.get_neighborhood_id();
            while neighbors.last().is_some_and(Vec::is_empty) {
                neighbors.pop();
            }
            if neighbors.len() > levels[entry].len() {
                entry = id;
            }
            levels[id] = neighbors
                .into_iter()
                .map(|ns| ns.into_iter().map(|n| n.get_origin_id() as u32).collect())
                .collect();
        }
        // Singletons need an empty base layer.
        for row in &mut levels {
            if row.is_empty() {
                row.push(Vec::new());
            }
        }
        eprintln!(
            "build_done seconds={:.3} rss_kib={}",
            start.elapsed().as_secs_f64(),
            super::data::rss_kib()
        );
        Self {
            levels,
            entry: entry as u32,
        }
    }
    pub fn write(&self, path: &Path) -> Result<()> {
        let mut f = BufWriter::new(File::create(path)?);
        f.write_all(b"G3SCR001")?;
        for x in [self.levels.len() as u32, self.entry] {
            f.write_all(&x.to_le_bytes())?;
        }
        for row in &self.levels {
            f.write_all(&(row.len() as u32).to_le_bytes())?;
            for level in row {
                f.write_all(&(level.len() as u32).to_le_bytes())?;
                for id in level {
                    f.write_all(&id.to_le_bytes())?;
                }
            }
        }
        f.flush()?;
        Ok(())
    }
    pub fn read(path: &Path) -> Result<Self> {
        let mut f = BufReader::new(File::open(path)?);
        let mut magic = [0; 8];
        f.read_exact(&mut magic)?;
        if &magic != b"G3SCR001" {
            return Err("not an experiment graph".into());
        }
        fn word(f: &mut impl Read) -> Result<u32> {
            let mut b = [0; 4];
            f.read_exact(&mut b)?;
            Ok(u32::from_le_bytes(b))
        }
        let n = word(&mut f)? as usize;
        let entry = word(&mut f)?;
        if n == 0 || entry as usize >= n {
            return Err("invalid graph size or entry".into());
        }
        let mut levels = Vec::with_capacity(n);
        for _ in 0..n {
            let count = word(&mut f)? as usize;
            if !(1..=16).contains(&count) {
                return Err("invalid levels".into());
            }
            let mut row = Vec::with_capacity(count);
            for _ in 0..count {
                let count = word(&mut f)? as usize;
                if count > 512 {
                    return Err("invalid degree".into());
                }
                let mut level = Vec::with_capacity(count);
                for _ in 0..count {
                    let id = word(&mut f)?;
                    if id as usize >= n {
                        return Err("invalid link".into());
                    }
                    level.push(id);
                }
                row.push(level);
            }
            levels.push(row);
        }
        let mut end = [0];
        if f.read(&mut end)? != 0 {
            return Err("trailing graph bytes".into());
        }
        Ok(Self { levels, entry })
    }
    /// Start one beam at several already-scored entry points (the stitched-graph probe).
    pub fn search_seeded(
        &self,
        ef: usize,
        scratch: &mut Search,
        seeds: &[Hit],
        mut score: impl FnMut(u32) -> f32,
    ) -> (Vec<Hit>, usize) {
        assert!(ef >= seeds.len() && !seeds.is_empty());
        scratch.reset();
        let mut count = 0;
        for &seed in seeds {
            if scratch.visit(seed.id) {
                scratch.frontier.push(Reverse(seed));
                scratch.nearest.push(seed);
                count += 1;
            }
        }
        while let Some(Reverse(candidate)) = scratch.frontier.pop() {
            if scratch.nearest.len() >= ef && candidate > *scratch.nearest.peek().unwrap() {
                break;
            }
            for &id in &self.levels[candidate.id as usize][0] {
                if !scratch.visit(id) {
                    continue;
                }
                let h = Hit {
                    id,
                    distance: score(id),
                };
                count += 1;
                if scratch.nearest.len() < ef || h < *scratch.nearest.peek().unwrap() {
                    scratch.frontier.push(Reverse(h));
                    scratch.nearest.push(h);
                    if scratch.nearest.len() > ef {
                        scratch.nearest.pop();
                    }
                }
            }
        }
        (scratch.nearest.clone().into_sorted_vec(), count)
    }
    pub fn search(
        &self,
        ef: usize,
        scratch: &mut Search,
        mut score: impl FnMut(u32) -> f32,
    ) -> (Vec<Hit>, usize) {
        assert!(ef > 0);
        scratch.reset();
        let mut count = 1;
        let mut best = Hit {
            id: self.entry,
            distance: score(self.entry),
        };
        for level in (1..self.levels[self.entry as usize].len()).rev() {
            loop {
                let mut next = best;
                for &id in &self.levels[best.id as usize][level] {
                    let h = Hit {
                        id,
                        distance: score(id),
                    };
                    count += 1;
                    if h < next {
                        next = h;
                    }
                }
                if next.id == best.id {
                    break;
                }
                best = next;
            }
        }
        scratch.visit(best.id);
        scratch.frontier.push(Reverse(best));
        scratch.nearest.push(best);
        while let Some(Reverse(candidate)) = scratch.frontier.pop() {
            if scratch.nearest.len() >= ef && candidate > *scratch.nearest.peek().unwrap() {
                break;
            }
            for &id in &self.levels[candidate.id as usize][0] {
                if !scratch.visit(id) {
                    continue;
                }
                let h = Hit {
                    id,
                    distance: score(id),
                };
                count += 1;
                if scratch.nearest.len() < ef || h < *scratch.nearest.peek().unwrap() {
                    scratch.frontier.push(Reverse(h));
                    scratch.nearest.push(h);
                    if scratch.nearest.len() > ef {
                        scratch.nearest.pop();
                    }
                }
            }
        }
        (scratch.nearest.clone().into_sorted_vec(), count)
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Hit {
    pub id: u32,
    pub distance: f32,
}
impl PartialEq for Hit {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Hit {}
impl PartialOrd for Hit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Hit {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then(self.id.cmp(&other.id))
    }
}
pub struct Search {
    marks: Vec<u8>,
    generation: u8,
    frontier: BinaryHeap<Reverse<Hit>>,
    nearest: BinaryHeap<Hit>,
}
impl Search {
    pub fn new(n: usize) -> Self {
        Self {
            marks: vec![0; n],
            generation: 0,
            frontier: BinaryHeap::new(),
            nearest: BinaryHeap::new(),
        }
    }
    fn reset(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.marks.fill(0);
            self.generation = 1;
        }
        self.frontier.clear();
        self.nearest.clear();
    }
    fn visit(&mut self, id: u32) -> bool {
        let m = &mut self.marks[id as usize];
        if *m == self.generation {
            false
        } else {
            *m = self.generation;
            true
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn full_beam_matches_exhaustive_and_generation_wraps() {
        let g = Graph {
            entry: 0,
            levels: (0..20)
                .map(|i| vec![(0..20).filter(|j| *j != i).collect()])
                .collect(),
        };
        let mut scratch = Search::new(20);
        for _ in 0..520 {
            let (h, n) = g.search(20, &mut scratch, |id| ((id as i32 - 7).abs()) as f32);
            assert_eq!(h[0].id, 7);
            assert_eq!(n, 20);
            assert_eq!(h.len(), 20);
        }
    }
}
#[cfg(test)]
mod retrieval_tests {
    use super::*;
    use half::f16;
    #[test]
    fn exported_hierarchy_retrieves_all_small_self_queries() {
        let rows = 160;
        let dims = 16;
        let values = (0..rows * dims)
            .map(|i| f16::from_f32((i as f32 * 1.618 + ((i / dims) as f32).cos()).sin()))
            .collect();
        let inv_norms = vec![1.; rows];
        let v = Arc::new(Vectors {
            dims,
            values,
            inv_norms,
        });
        // Recompute true norms to make this a cosine retrieval test.
        let bytes = v
            .values
            .iter()
            .flat_map(|x| x.to_bits().to_le_bytes())
            .collect();
        let v = Arc::new(Vectors::from_matrix(super::super::data::Matrix {
            rows,
            cols: dims,
            bytes,
        }));
        let g = Graph::build(v.clone(), 16, 100);
        let mut s = Search::new(rows);
        for id in 0..rows as u32 {
            let (hits, _) = g.search(rows, &mut s, |j| v.between(id, j));
            assert!(hits.iter().take(2).any(|h| h.id == id), "missed self {id}");
            let mut exact: Vec<_> = (0..rows as u32)
                .map(|j| Hit {
                    id: j,
                    distance: v.between(id, j),
                })
                .collect();
            exact.sort_unstable();
            assert_eq!(
                hits.iter().take(10).map(|h| h.id).collect::<Vec<_>>(),
                exact.iter().take(10).map(|h| h.id).collect::<Vec<_>>()
            );
        }
    }
}

#[cfg(test)]
mod seeded_tests {
    use super::*;
    #[test]
    fn multiple_entries_search_disconnected_partitions_in_one_beam() {
        let g = Graph {
            levels: vec![vec![vec![1]], vec![vec![0]], vec![vec![3]], vec![vec![2]]],
            entry: 0,
        };
        let d = [0.1, 0.3, 0.2, 0.4];
        let (hits, count) = g.search_seeded(
            4,
            &mut Search::new(4),
            &[
                Hit {
                    id: 0,
                    distance: d[0],
                },
                Hit {
                    id: 2,
                    distance: d[2],
                },
            ],
            |id| d[id as usize],
        );
        assert_eq!(
            hits.iter().map(|h| h.id).collect::<Vec<_>>(),
            vec![0, 2, 1, 3]
        );
        assert_eq!(count, 4);
    }
}
