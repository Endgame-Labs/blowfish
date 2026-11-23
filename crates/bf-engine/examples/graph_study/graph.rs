//! Bounded-degree, single-layer HNSW-style insertion with diversified neighbor pruning.
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

#[derive(Clone, Copy, Debug)]
pub(super) struct Hit(pub f32, pub usize);
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
        self.0.total_cmp(&other.0).then(self.1.cmp(&other.1))
    }
}

pub(super) struct Search {
    seen: Vec<u32>,
    epoch: u32,
    frontier: BinaryHeap<Reverse<Hit>>,
    best: BinaryHeap<Hit>,
}
impl Search {
    pub fn new(n: usize) -> Self {
        Self {
            seen: vec![0; n],
            epoch: 0,
            frontier: BinaryHeap::new(),
            best: BinaryHeap::new(),
        }
    }
    /// Return the nearest `beam` reached nodes, and the number of distance evaluations.
    pub fn run(
        &mut self,
        graph: &[Vec<usize>],
        seeds: &[usize],
        beam: usize,
        allowed: impl Fn(usize) -> bool,
        distance: impl Fn(usize) -> f32,
    ) -> (Vec<Hit>, usize) {
        assert!(beam > 0);
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.seen.fill(0);
            self.epoch = 1;
        }
        self.frontier.clear();
        self.best.clear();
        let mut visited = 0;
        for &s in seeds {
            if !allowed(s) || self.seen[s] == self.epoch {
                continue;
            }
            self.seen[s] = self.epoch;
            let h = Hit(distance(s), s);
            visited += 1;
            self.frontier.push(Reverse(h));
            self.best.push(h);
            if self.best.len() > beam {
                self.best.pop();
            }
        }
        while let Some(Reverse(h)) = self.frontier.pop() {
            if self.best.len() == beam && h > *self.best.peek().unwrap() {
                break;
            }
            for &v in &graph[h.1] {
                if self.seen[v] == self.epoch || !allowed(v) {
                    continue;
                }
                self.seen[v] = self.epoch;
                let next = Hit(distance(v), v);
                visited += 1;
                if self.best.len() < beam || next < *self.best.peek().unwrap() {
                    self.frontier.push(Reverse(next));
                    self.best.push(next);
                    if self.best.len() > beam {
                        self.best.pop();
                    }
                }
            }
        }
        let mut result = self.best.drain().collect::<Vec<_>>();
        result.sort_unstable();
        (result, visited)
    }
}

pub(super) struct Graph {
    pub edges: Vec<Vec<usize>>,
    pub entry: usize,
}
impl Graph {
    pub fn build(points: &[Vec<f32>]) -> Self {
        let mut order: Vec<usize> = (0..points.len()).collect();
        let mut rng = 7u64;
        for i in (1..order.len()).rev() {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            order.swap(i, rng as usize % (i + 1));
        }
        let mut graph = Self {
            edges: vec![Vec::new(); points.len()],
            entry: order[0],
        };
        let mut search = Search::new(points.len());
        for &v in &order[1..] {
            let (hits, _) = search.run(
                &graph.edges,
                &[graph.entry],
                64,
                |_| true,
                |i| bf_vector::squared_distance(&points[v], &points[i]),
            );
            let candidates: Vec<_> = hits.iter().map(|h| h.1).collect();
            graph.edges[v] = prune(v, &candidates, points);
            for n in graph.edges[v].clone() {
                graph.edges[n].push(v);
                if graph.edges[n].len() > 16 {
                    graph.edges[n] = prune(n, &graph.edges[n], points);
                }
            }
        }
        graph
    }
}
fn prune(v: usize, candidates: &[usize], points: &[Vec<f32>]) -> Vec<usize> {
    let mut hits: Vec<_> = candidates
        .iter()
        .filter(|&&i| i != v)
        .map(|&i| Hit(bf_vector::squared_distance(&points[v], &points[i]), i))
        .collect();
    hits.sort_unstable();
    hits.dedup_by_key(|h| h.1);
    let mut selected: Vec<usize> = Vec::new();
    for h in &hits {
        if selected
            .iter()
            .all(|&s| bf_vector::squared_distance(&points[h.1], &points[s]) >= h.0)
        {
            selected.push(h.1);
            if selected.len() == 16 {
                return selected;
            }
        }
    }
    // HNSW's keep-pruned-connections heuristic fills unused neighbor slots.
    for h in hits {
        if !selected.contains(&h.1) {
            selected.push(h.1);
        }
        if selected.len() == 16 {
            break;
        }
    }
    selected
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn search_and_page_restriction() {
        let graph = vec![vec![1], vec![0, 2], vec![1, 3], vec![2]];
        let mut search = Search::new(4);
        let (found, visited) = search.run(&graph, &[0], 4, |_| true, |i| (3 - i) as f32);
        assert_eq!(found[0].1, 3);
        assert_eq!(visited, 4);
        let (found, visited) = search.run(&graph, &[0], 4, |i| i < 2, |i| (3 - i) as f32);
        assert_eq!(found[0].1, 1);
        assert_eq!(visited, 2);
        assert_eq!(found.len(), 2);
    }
    #[test]
    fn graph_has_valid_bounded_edges_and_finds_self() {
        let points: Vec<_> = (0..200).map(|i| vec![i as f32, (i % 13) as f32]).collect();
        let graph = Graph::build(&points);
        for (i, edges) in graph.edges.iter().enumerate() {
            assert!(edges.len() <= 16);
            assert!(edges.iter().all(|&j| j < points.len() && i != j));
            let mut unique = edges.clone();
            unique.sort_unstable();
            unique.dedup();
            assert_eq!(unique.len(), edges.len());
        }
        let mut search = Search::new(points.len());
        for i in 0..points.len() {
            let (found, _) = search.run(
                &graph.edges,
                &[graph.entry],
                64,
                |_| true,
                |j| bf_vector::squared_distance(&points[i], &points[j]),
            );
            assert_eq!(found[0].1, i);
        }
    }
}
