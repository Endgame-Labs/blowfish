use std::cmp::Ordering;
#[derive(Clone, Copy, Debug)]
pub struct Hit {
    pub id: u32,
    pub distance: f32,
}
impl PartialEq for Hit {
    fn eq(&self, b: &Self) -> bool {
        self.cmp(b) == Ordering::Equal
    }
}
impl Eq for Hit {}
impl PartialOrd for Hit {
    fn partial_cmp(&self, b: &Self) -> Option<Ordering> {
        Some(self.cmp(b))
    }
}
impl Ord for Hit {
    fn cmp(&self, b: &Self) -> Ordering {
        self.distance
            .total_cmp(&b.distance)
            .then(self.id.cmp(&b.id))
    }
}
// Each word packs a query generation above 32 exact visitation bits. This cuts the
// byte-per-node allocation by four, with direct indexing and no hash probes or per-query clear.
struct Visited {
    words: Vec<u64>,
    stamp: u64,
}
impl Visited {
    const GENERATION: u64 = 1 << 32;
    fn new(nodes: usize) -> Self {
        Self {
            words: vec![0; nodes.div_ceil(32)],
            stamp: 0,
        }
    }
    fn begin(&mut self) {
        self.stamp = self.stamp.wrapping_add(Self::GENERATION);
        if self.stamp == 0 {
            self.words.fill(0);
            self.stamp = Self::GENERATION;
        }
    }
    fn insert(&mut self, id: u32) -> bool {
        let word = &mut self.words[id as usize / 32];
        let bit = 1u64 << (id % 32);
        if *word & !(u32::MAX as u64) != self.stamp {
            *word = self.stamp | bit;
            true
        } else {
            let previous = *word;
            *word |= bit;
            previous & bit == 0
        }
    }
}
/// Largest adjacency list a search can read: construction keeps up to twice the final degree.
pub const MAX_LINKS: usize = 64;
#[derive(Clone, Copy)]
struct BeamHit {
    hit: Hit,
    expanded: bool,
}
/// Reusable, exact visitation and one sorted beam. Its owner reserves retained capacity.
pub struct Scratch {
    visited: Visited,
    beam: Vec<BeamHit>,
}
impl Scratch {
    pub fn new(n: usize) -> Self {
        Self {
            visited: Visited::new(n),
            beam: Vec::new(),
        }
    }
    #[cfg(test)]
    pub fn search(
        &mut self,
        links: &[[u32; 34]],
        degrees: &[u8],
        entry: u32,
        ef: usize,
        score: impl FnMut(u32) -> f32,
    ) -> (Vec<Hit>, usize) {
        self.search_prefetched(links, degrees, entry, ef, score, |_| {})
    }
    pub fn search_prefetched(
        &mut self,
        links: &[[u32; 34]],
        degrees: &[u8],
        entry: u32,
        ef: usize,
        score: impl FnMut(u32) -> f32,
        prefetch: impl FnMut(u32),
    ) -> (Vec<Hit>, usize) {
        let evaluations = self.search_with(
            |id, out| {
                let degree = degrees[id as usize] as usize;
                out[..degree].copy_from_slice(&links[id as usize][..degree]);
                degree
            },
            entry,
            ef,
            score,
            prefetch,
            |_| {},
        );
        (self.beam.iter().map(|b| b.hit).collect(), evaluations)
    }
    /// The shared beam search, for serving and for graph construction. `adjacent` copies one
    /// node's neighbors into the buffer and returns their count (at most [`MAX_LINKS`]).
    /// `expanded` sees every node in the order the search expands it, with its score. Returns
    /// the number of scores. The sorted beam remains in the scratch until the next search.
    pub fn search_with(
        &mut self,
        mut adjacent: impl FnMut(u32, &mut [u32; MAX_LINKS]) -> usize,
        entry: u32,
        ef: usize,
        mut score: impl FnMut(u32) -> f32,
        mut prefetch: impl FnMut(u32),
        mut expanded: impl FnMut(Hit),
    ) -> usize {
        self.visited.begin();
        self.beam.clear();
        self.visited.insert(entry);
        self.beam.push(BeamHit {
            hit: Hit {
                id: entry,
                distance: score(entry),
            },
            expanded: false,
        });
        let mut evaluations = 1;
        let mut next = 0;
        let mut pending = [0u32; MAX_LINKS];
        loop {
            while next < self.beam.len() && self.beam[next].expanded {
                next += 1;
            }
            if next == self.beam.len() {
                break;
            }
            let current = self.beam[next].hit;
            self.beam[next].expanded = true;
            next += 1;
            expanded(current);
            // Every unseen neighbor is scored before selecting another expansion. Collecting
            // that same ordered batch lets future code rows arrive while scoring this one.
            let degree = adjacent(current.id, &mut pending);
            let mut count = 0;
            for position in 0..degree {
                let id = pending[position];
                if self.visited.insert(id) {
                    pending[count] = id;
                    count += 1;
                }
            }
            let pending = &pending[..count];
            for &id in pending.iter().take(2) {
                prefetch(id);
            }
            for (position, &id) in pending.iter().enumerate() {
                if let Some(&next) = pending.get(position + 2) {
                    prefetch(next);
                }
                let hit = Hit {
                    id,
                    distance: score(id),
                };
                evaluations += 1;
                if self.beam.len() < ef || hit < self.beam.last().unwrap().hit {
                    let pos = self.beam.partition_point(|b| b.hit < hit);
                    self.beam.insert(
                        pos,
                        BeamHit {
                            hit,
                            expanded: false,
                        },
                    );
                    if self.beam.len() > ef {
                        self.beam.pop();
                    }
                    next = next.min(pos);
                }
            }
        }
        evaluations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cmp::Reverse, collections::BinaryHeap};
    /// Reusable, byte-per-row visitation state. Its owner also owns the memory reservation.
    pub struct Reference {
        marks: Vec<u8>,
        generation: u8,
        frontier: BinaryHeap<Reverse<Hit>>,
        nearest: BinaryHeap<Hit>,
    }
    impl Reference {
        pub fn new(n: usize) -> Self {
            Self {
                marks: vec![0; n],
                generation: 0,
                frontier: BinaryHeap::new(),
                nearest: BinaryHeap::new(),
            }
        }
        fn visit(&mut self, id: u32) -> bool {
            let p = &mut self.marks[id as usize];
            if *p == self.generation {
                false
            } else {
                *p = self.generation;
                true
            }
        }
        pub fn search(
            &mut self,
            links: &[[u32; 34]],
            degrees: &[u8],
            entry: u32,
            ef: usize,
            mut score: impl FnMut(u32) -> f32,
        ) -> (Vec<Hit>, usize) {
            self.generation = self.generation.wrapping_add(1);
            if self.generation == 0 {
                self.marks.fill(0);
                self.generation = 1;
            }
            self.frontier.clear();
            self.nearest.clear();
            self.visit(entry);
            let start = Hit {
                id: entry,
                distance: score(entry),
            };
            self.frontier.push(Reverse(start));
            self.nearest.push(start);
            let mut evaluations = 1;
            while let Some(Reverse(current)) = self.frontier.pop() {
                if self.nearest.len() >= ef && current > *self.nearest.peek().unwrap() {
                    break;
                }
                for &id in &links[current.id as usize][..degrees[current.id as usize] as usize] {
                    if !self.visit(id) {
                        continue;
                    }
                    let h = Hit {
                        id,
                        distance: score(id),
                    };
                    evaluations += 1;
                    if self.nearest.len() < ef || h < *self.nearest.peek().unwrap() {
                        self.frontier.push(Reverse(h));
                        self.nearest.push(h);
                        if self.nearest.len() > ef {
                            self.nearest.pop();
                        }
                    }
                }
                // The threshold can only improve. Pending nodes already outside the beam can
                // never be expanded, so release them before stale heap entries grow with N.
                if self.frontier.len() > ef * 2 {
                    let worst = *self.nearest.peek().unwrap();
                    self.frontier.retain(|hit| hit.0 <= worst);
                }
            }
            (self.nearest.clone().into_sorted_vec(), evaluations)
        }
    }

    #[test]
    fn beam_preserves_every_score_visit_and_tie_break() {
        let n = 4096;
        let mut seed = 71u32;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed
        };
        let links = (0..n)
            .map(|_| std::array::from_fn(|_| next() % n as u32))
            .collect::<Vec<_>>();
        let degrees = vec![34; n];
        let distances = (0..n).map(|_| (next() % 41) as f32).collect::<Vec<_>>();
        for ef in [1, 7, 32, 80, 100, 256, n] {
            let mut actual = Scratch::new(n);
            let mut reference = Reference::new(n);
            for pass in 0..260 {
                let entry = (pass * 97 % n) as u32;
                let (mut va, mut vb, mut prefetched) = (Vec::new(), Vec::new(), Vec::new());
                let a = actual.search_prefetched(
                    &links,
                    &degrees,
                    entry,
                    ef,
                    |id| {
                        va.push(id);
                        distances[id as usize]
                    },
                    |id| prefetched.push(id),
                );
                let b = reference.search(&links, &degrees, entry, ef, |id| {
                    vb.push(id);
                    distances[id as usize]
                });
                assert_eq!(va, vb, "ef={ef}, pass={pass}");
                assert_eq!(prefetched, va[1..], "no unused code row is prefetched");
                assert_eq!(a.1, b.1);
                assert_eq!(a.0, b.0);
            }
        }
    }
    #[test]
    fn visitation_words_are_exact_across_queries_and_generation_wrap() {
        let n = 100000;
        let mut visited = Visited::new(n);
        for _ in 0..3 {
            visited.begin();
            for id in 0..n as u32 {
                assert!(visited.insert(id));
                assert!(!visited.insert(id));
            }
            assert!(visited.words.capacity() * 8 <= n.div_ceil(32) * 8);
        }
        visited.stamp = !(u32::MAX as u64);
        visited.begin();
        for id in 0..n as u32 {
            assert!(visited.insert(id));
        }
    }
    #[test]
    fn long_improving_walk_keeps_a_bounded_beam() {
        let n = 4096;
        let ef = 32;
        let links = (0..n)
            .map(|i| std::array::from_fn(|j| (i + j + 1).min(n - 1) as u32))
            .collect::<Vec<_>>();
        let mut scratch = Scratch::new(n);
        let (hits, _) = scratch.search(&links, &vec![34; n], 0, ef, |id| (n - id as usize) as f32);
        assert_eq!(
            hits.iter().map(|h| h.id).collect::<Vec<_>>(),
            (n - ef..n).rev().map(|id| id as u32).collect::<Vec<_>>()
        );
        assert!(scratch.beam.capacity() <= ef * 2);
    }
}
