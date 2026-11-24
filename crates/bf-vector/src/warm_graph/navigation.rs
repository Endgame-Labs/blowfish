use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};
#[derive(Clone, Copy)]
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
/// Reusable, byte-per-row visitation state. Its owner also owns the memory reservation.
pub struct Scratch {
    marks: Vec<u8>,
    generation: u8,
    frontier: BinaryHeap<Reverse<Hit>>,
    nearest: BinaryHeap<Hit>,
}
impl Scratch {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn long_improving_walk_bounds_stale_frontier_and_survives_generation_wrap() {
        let n = 4096;
        let ef = 32;
        let links = (0..n)
            .map(|i| std::array::from_fn(|j| (i + j + 1).min(n - 1) as u32))
            .collect::<Vec<_>>();
        let degrees = vec![34; n];
        let mut scratch = Scratch::new(n);
        for _ in 0..260 {
            let (hits, _) = scratch.search(&links, &degrees, 0, ef, |id| (n - id as usize) as f32);
            assert_eq!(
                hits.iter().map(|h| h.id).collect::<Vec<_>>(),
                (n - ef..n).rev().map(|id| id as u32).collect::<Vec<_>>()
            );
            assert!(scratch.frontier.capacity() <= ef * 4);
        }
    }
}
