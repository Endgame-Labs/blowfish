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
        }
        (self.nearest.clone().into_sorted_vec(), evaluations)
    }
}
