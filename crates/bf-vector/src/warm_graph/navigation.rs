use super::binary::Bits;
use std::cmp::Ordering;
/// G29-TEMP
pub static DEGREE_CAP: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
/// G29-TEMP
pub static DEGREE_SPREAD: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
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
    /// Hamming distance of this node's one-bit code to the query's signs (zero unless screening).
    h: u16,
    expanded: bool,
    /// Whether the row passes the walk's filter (always true without one).
    allowed: bool,
}
/// A filtered walk: the beam is sized by the rows that pass `allow` (the survivors), not by its
/// length, so a walk stops once `survivors` of them are in hand and every better node is expanded.
/// Rows that fail the filter are still stepping stones.
pub struct Filtered<'a> {
    /// Whether a node (a graph id) passes the filter.
    pub allow: &'a dyn Fn(u32) -> bool,
    /// How many survivors the beam keeps.
    pub survivors: usize,
    /// Score nodes that fail the filter from their one-bit code alone.
    pub gate: bool,
    /// How many residual deviations below the one-bit estimate a gated node enters the beam.
    pub slack: f32,
    /// With `Some`, every node scored with SQ8 is logged with the expansions so far.
    pub log: Option<Vec<(u32, u32)>>,
    /// Stop once this many expansions pass with no survivor entering the nearest `top` (0: never).
    pub patience: u32,
    /// How many of the nearest survivors the patience counts.
    pub top: usize,
    /// Distances of the `top` nearest survivors scored so far, ascending.
    best: Vec<f32>,
    /// Expansions at the last change of `best`.
    last_change: u32,
    /// Expansions performed.
    pub expansions: u32,
    /// Nodes that failed the filter and were placed from their one-bit code.
    pub transit: u32,
    fit: Fit,
}
impl<'a> Filtered<'a> {
    /// Note a scored survivor: it resets the patience when it is among the nearest `top` so far.
    fn note(&mut self, distance: f32) {
        if self.patience == 0 {
            return;
        }
        if self.best.len() < self.top || distance < *self.best.last().unwrap() {
            let at = self.best.partition_point(|&b| b <= distance);
            self.best.insert(at, distance);
            self.best.truncate(self.top);
            self.last_change = self.expansions;
        }
    }
    fn patient(&self) -> bool {
        self.patience > 0
            && self.best.len() >= self.top
            && self.expansions - self.last_change > self.patience
    }
    pub fn new(allow: &'a dyn Fn(u32) -> bool, survivors: usize) -> Self {
        Self {
            allow,
            survivors,
            gate: false,
            slack: 1.,
            log: None,
            patience: 0,
            top: 10,
            best: Vec::new(),
            last_change: 0,
            expansions: 0,
            transit: 0,
            fit: Fit::default(),
        }
    }
}
/// Running least squares of the SQ8 distance on the one-bit Hamming distance, per query.
#[derive(Default)]
struct Fit {
    n: f64,
    h: f64,
    d: f64,
    hh: f64,
    hd: f64,
    dd: f64,
}
impl Fit {
    /// Samples scored with SQ8 before any node may be placed from its one-bit code alone.
    const WARM: f64 = 48.;
    fn add(&mut self, h: u16, d: f32) {
        let (h, d) = (f64::from(h), f64::from(d));
        self.n += 1.;
        self.h += h;
        self.d += d;
        self.hh += h * h;
        self.hd += h * d;
        self.dd += d * d;
    }
    fn ready(&self) -> bool {
        self.n >= Self::WARM
    }
    /// The estimated distance of a node with Hamming distance `h`, lowered by `slack` residual
    /// deviations.
    fn estimate(&self, h: u16, slack: f32) -> f32 {
        let n = self.n;
        let (mh, md) = (self.h / n, self.d / n);
        let vh = (self.hh / n - mh * mh).max(1e-9);
        let cov = self.hd / n - mh * md;
        let vd = (self.dd / n - md * md).max(0.);
        let slope = cov / vh;
        let resid = (vd - slope * cov).max(0.).sqrt();
        (md + slope * (f64::from(h) - mh) - f64::from(slack) * resid) as f32
    }
}
/// Members at the tail of a full beam whose mean Hamming distance sets the screen's reference.
const TAIL: usize = 8;
/// One-bit screening of unseen neighbors against a full beam.
///
/// Once the beam holds `ef` nodes, a neighbor is scored with its SQ8 code only if its one-bit
/// code's Hamming distance to the query's signs is at most the mean Hamming distance of the
/// beam's [`TAIL`] worst members plus a margin. The margin is `sqrt(n) / 2` bits for codes of
/// `n` bits: the standard deviation of the Hamming distance between two unrelated codes, so a
/// neighbor must look about one deviation worse than the beam's tail to be dropped. The rule
/// reads only the codes and the beam, so it has no per-dataset parameter.
#[derive(Clone, Copy)]
pub struct Screen<'a> {
    bits: &'a Bits,
    query: &'a [u64],
    margin: u16,
}
impl<'a> Screen<'a> {
    /// `query` holds the signs of the rotated query, in the layout of a row of `bits`.
    pub(super) fn new(bits: &'a Bits, query: &'a [u64]) -> Self {
        let deviation = ((query.len() * 64) as f32).sqrt() / 2.;
        Self::with_margin(bits, query, deviation.ceil() as u16)
    }
    /// A screen with an explicit margin, in bits.
    pub(super) fn with_margin(bits: &'a Bits, query: &'a [u64], margin: u16) -> Self {
        Self {
            bits,
            query,
            margin,
        }
    }
    fn skips(&self, tail: u16, h: u16) -> bool {
        h > tail.saturating_add(self.margin)
    }
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
    /// The beam's surviving rows, nearest first, at most `count`.
    pub fn survivors(&self, count: usize) -> Vec<u32> {
        self.beam
            .iter()
            .filter(|b| b.allowed)
            .take(count)
            .map(|b| b.hit.id)
            .collect()
    }
    /// Mean Hamming distance (rounded up) of the beam's [`TAIL`] worst members.
    fn tail_hamming(&self) -> u16 {
        let n = TAIL.min(self.beam.len()).max(1);
        let sum: u32 = self.beam[self.beam.len() - n..]
            .iter()
            .map(|b| u32::from(b.h))
            .sum();
        sum.div_ceil(n as u32) as u16
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
        self.search_prefetched(links, degrees, entry, ef, score, |_| {}, None)
    }
    #[allow(clippy::too_many_arguments)]
    pub fn search_prefetched(
        &mut self,
        links: &[[u32; 34]],
        degrees: &[u8],
        entry: u32,
        ef: usize,
        score: impl FnMut(u32) -> f32,
        prefetch: impl FnMut(u32),
        screen: Option<Screen>,
    ) -> (Vec<Hit>, usize) {
        let evaluations = self.search_with(
            |id, out| {
                let degree = degrees[id as usize] as usize;
                // G29-TEMP: degree replay
                let cap = DEGREE_CAP.load(std::sync::atomic::Ordering::Relaxed);
                let spread = DEGREE_SPREAD.load(std::sync::atomic::Ordering::Relaxed);
                if cap > 0 && degree > cap {
                    let row = &links[id as usize][..degree];
                    if spread == 0 {
                        out[..cap].copy_from_slice(&row[..cap]);
                    } else {
                        let short = spread.min(cap);
                        out[..short].copy_from_slice(&row[..short]);
                        let longs = cap - short;
                        let rest = &row[short..];
                        for j in 0..longs {
                            out[short + j] = rest[j * rest.len() / longs.max(1)];
                        }
                    }
                    return cap;
                }
                out[..degree].copy_from_slice(&links[id as usize][..degree]);
                degree
            },
            entry,
            ef,
            score,
            prefetch,
            |_| {},
            screen,
            None,
        );
        (self.beam.iter().map(|b| b.hit).collect(), evaluations)
    }
    /// The shared beam search, for serving and for graph construction. `adjacent` copies one
    /// node's neighbors into the buffer and returns their count (at most [`MAX_LINKS`]).
    /// `expanded` sees every node in the order the search expands it, with its score. Returns
    /// the number of scores. The sorted beam remains in the scratch until the next search. With
    /// a `screen`, neighbors it rejects are visited but never scored.
    #[allow(clippy::too_many_arguments)]
    pub fn search_with(
        &mut self,
        mut adjacent: impl FnMut(u32, &mut [u32; MAX_LINKS]) -> usize,
        entry: u32,
        ef: usize,
        mut score: impl FnMut(u32) -> f32,
        mut prefetch: impl FnMut(u32),
        mut expanded: impl FnMut(Hit),
        screen: Option<Screen>,
        mut filter: Option<&mut Filtered>,
    ) -> usize {
        self.visited.begin();
        self.beam.clear();
        self.visited.insert(entry);
        let entry_hit = Hit {
            id: entry,
            distance: score(entry),
        };
        let entry_h = screen.map_or(0, |sc| sc.bits.hamming(sc.query, entry));
        let entry_allowed = filter.as_ref().is_none_or(|f| (f.allow)(entry));
        if let Some(f) = filter.as_deref_mut() {
            if entry_allowed {
                f.note(entry_hit.distance);
            }
            f.fit.add(entry_h, entry_hit.distance);
            if let Some(log) = f.log.as_mut() {
                log.push((entry, 0));
            }
        }
        // Survivors in the beam; with a filter the beam is full at `survivors` of them.
        let mut kept = usize::from(entry_allowed);
        self.beam.push(BeamHit {
            hit: entry_hit,
            h: entry_h,
            expanded: false,
            allowed: entry_allowed,
        });
        let mut evaluations = 1;
        let mut next = 0;
        let mut pending = [0u32; MAX_LINKS];
        loop {
            while next < self.beam.len() && self.beam[next].expanded {
                next += 1;
            }
            if next >= self.beam.len() {
                break;
            }
            if filter.as_ref().is_some_and(|f| f.patient()) {
                break;
            }
            let current = self.beam[next].hit;
            self.beam[next].expanded = true;
            next += 1;
            expanded(current);
            if let Some(f) = filter.as_deref_mut() {
                f.expansions += 1;
            }
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
            // The one-bit codes of the whole batch are read first, then the full code rows of
            // the survivors arrive while earlier ones are scored.
            let mut hamming = [0u16; MAX_LINKS];
            if let Some(screen) = &screen {
                for &id in pending {
                    screen.bits.prefetch(id);
                }
                for (h, &id) in hamming.iter_mut().zip(pending) {
                    *h = screen.bits.hamming(screen.query, id);
                }
            }
            let mut tail = self.tail_hamming();
            let survivors = filter.as_ref().map(|f| f.survivors);
            let full = |beam: &Vec<BeamHit>, kept: usize| match survivors {
                Some(s) => kept >= s || beam.len() >= ef,
                None => beam.len() >= ef,
            };
            let rejected = |beam_full: bool, tail: u16, h: u16| {
                beam_full && screen.is_some_and(|screen| screen.skips(tail, h))
            };
            let (mut ahead, mut issued) = (0, 0);
            for position in 0..pending.len() {
                while issued < position + 3 && ahead < pending.len() {
                    if !rejected(full(&self.beam, kept), tail, hamming[ahead]) {
                        prefetch(pending[ahead]);
                        issued += 1;
                    }
                    ahead += 1;
                }
                let (id, h) = (pending[position], hamming[position]);
                if rejected(full(&self.beam, kept), tail, h) {
                    continue;
                }
                let mut allowed = true;
                let mut estimated = None;
                if let Some(f) = filter.as_deref_mut() {
                    allowed = (f.allow)(id);
                    if !allowed && f.gate && f.fit.ready() {
                        estimated = Some(f.fit.estimate(h, f.slack));
                        f.transit += 1;
                    }
                }
                let distance = match estimated {
                    Some(d) => d,
                    None => {
                        let d = score(id);
                        evaluations += 1;
                        if let Some(f) = filter.as_deref_mut() {
                            if allowed {
                                f.note(d);
                            }
                            f.fit.add(h, d);
                            let at = f.expansions;
                            if let Some(log) = f.log.as_mut() {
                                log.push((id, at));
                            }
                        }
                        d
                    }
                };
                let hit = Hit { id, distance };
                if !full(&self.beam, kept) || hit < self.beam.last().unwrap().hit {
                    let pos = self.beam.partition_point(|b| b.hit < hit);
                    self.beam.insert(
                        pos,
                        BeamHit {
                            hit,
                            h,
                            expanded: false,
                            allowed,
                        },
                    );
                    kept += usize::from(allowed);
                    // Trim to the width: `ef` nodes, or with a filter the nodes up to its
                    // `survivors`-th survivor.
                    while let Some(last) = self.beam.last() {
                        let over = self.beam.len() > ef
                            || survivors.is_some_and(|s| kept > s || (kept == s && !last.allowed));
                        if !over {
                            break;
                        }
                        kept -= usize::from(last.allowed);
                        self.beam.pop();
                    }
                    next = next.min(pos);
                    if screen.is_some() {
                        tail = self.tail_hamming();
                    }
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
                    None,
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
    /// Random one-bit codes over a random graph, scored by their exact Hamming distance, so the
    /// screen's estimate is the score itself: the screened walk must find the same nearest rows.
    #[test]
    fn screened_beam_returns_the_unscreened_top_k_when_the_screen_is_exact() {
        let n = 3000;
        let per = 4;
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let words = (0..n * per).map(|_| next()).collect::<Vec<_>>();
        let bits = Bits::from_words(words, per);
        let links = (0..n)
            .map(|_| std::array::from_fn(|_| (next() % n as u64) as u32))
            .collect::<Vec<_>>();
        let degrees = vec![34; n];
        let (mut screened, mut plain) = (Scratch::new(n), Scratch::new(n));
        let (mut kept, mut full) = (0, 0);
        for pass in 0..200 {
            let query = (0..per).map(|_| next()).collect::<Vec<_>>();
            let entry = (pass * 31 % n) as u32;
            let score = |id: u32| bits.hamming(&query, id) as f32;
            let screen = Screen::with_margin(&bits, &query, u16::MAX);
            let a = screened.search_prefetched(
                &links,
                &degrees,
                entry,
                32,
                score,
                |_| {},
                Some(screen),
            );
            let b = plain.search_prefetched(&links, &degrees, entry, 32, score, |_| {}, None);
            // A screen that never rejects is the plain walk.
            assert_eq!(a, b, "pass {pass}");
            let screen = Screen::new(&bits, &query);
            assert_eq!(screen.margin, 8);
            let c = screened.search_prefetched(
                &links,
                &degrees,
                entry,
                32,
                score,
                |_| {},
                Some(screen),
            );
            assert!(c.1 <= b.1);
            kept += c.1;
            full += b.1;
            assert_eq!(c.0[..10], b.0[..10], "pass {pass}");
        }
        assert!(
            kept < full,
            "the screen rejects some neighbors: {kept} of {full}"
        );
    }
    /// Allowing every row with `survivors` equal to the width is the plain walk, bit for bit.
    #[test]
    fn a_filter_that_allows_everything_is_the_plain_walk() {
        let n = 3000;
        let mut seed = 5u32;
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
        let distances = (0..n).map(|_| (next() % 977) as f32).collect::<Vec<_>>();
        let (mut a, mut b) = (Scratch::new(n), Scratch::new(n));
        for ef in [8, 40, 200] {
            for pass in 0..50 {
                let entry = (pass * 61 % n) as u32;
                let plain = a.search(&links, &degrees, entry, ef, |id| distances[id as usize]);
                let allow = |_: u32| true;
                let mut f = Filtered::new(&allow, ef);
                let adj = |id: u32, out: &mut [u32; MAX_LINKS]| {
                    out[..34].copy_from_slice(&links[id as usize]);
                    34
                };
                let e = b.search_with(
                    adj,
                    entry,
                    ef,
                    |id| distances[id as usize],
                    |_| {},
                    |_| {},
                    None,
                    Some(&mut f),
                );
                assert_eq!(e, plain.1);
                assert_eq!(
                    b.survivors(ef),
                    plain.0.iter().map(|h| h.id).collect::<Vec<_>>()
                );
            }
        }
    }
    /// With a filter the walk returns only allowed rows, and a wider survivor count finds at
    /// least the nearest allowed rows a narrower one found (on a graph where distance is smooth).
    #[test]
    fn a_filtered_walk_returns_allowed_rows_nearest_first() {
        let n = 4000;
        let links: Vec<[u32; 34]> = (0..n)
            .map(|i| std::array::from_fn(|j| ((i + (j + 1) * (j % 5 + 1)) % n) as u32))
            .collect::<Vec<_>>();
        let degrees = vec![34; n];
        let distances = (0..n)
            .map(|i| ((i as i64 - 1700).abs()) as f32)
            .collect::<Vec<_>>();
        let allow = |id: u32| id % 5 == 0;
        let mut scratch = Scratch::new(n);
        for survivors in [10, 40, 200] {
            let mut f = Filtered::new(&allow, survivors);
            let adj = |id: u32, out: &mut [u32; MAX_LINKS]| {
                out[..34].copy_from_slice(&links[id as usize]);
                34
            };
            scratch.search_with(
                adj,
                0,
                n,
                |id| distances[id as usize],
                |_| {},
                |_| {},
                None,
                Some(&mut f),
            );
            let found = scratch.survivors(survivors);
            assert!(found.len() >= survivors.min(10));
            assert!(found.iter().all(|id| id % 5 == 0));
            assert!(
                found
                    .windows(2)
                    .all(|w| distances[w[0] as usize] <= distances[w[1] as usize])
            );
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
