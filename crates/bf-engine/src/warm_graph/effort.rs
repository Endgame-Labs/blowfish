//! Paired query recall controls effort; common query difficulty cancels in the difference.
use std::collections::VecDeque;

const MIN: usize = 64; // Twice the graph's maximum construction degree.
const MAX: usize = 4096;
const FIRST: usize = 32;
const MATCHED: usize = 128;
const WINDOW: usize = 512;

pub(super) struct Effort {
    pub trial: usize,
    pub serving: Option<usize>,
    bad: usize,
    samples: VecDeque<(f64, f64)>,
}
pub(super) struct Update {
    pub accepted: bool,
    pub changed: bool,
    pub exhausted: bool,
}
impl Effort {
    pub fn new() -> Self {
        Self {
            trial: MIN,
            serving: None,
            bad: 0,
            samples: VecDeque::new(),
        }
    }
    pub fn len(&self) -> usize {
        self.samples.len()
    }
    /// Means and an approximate 95% interval for paired query recall differences.
    /// Query units preserve correlation among a query's neighbors. A variance floor prevents
    /// an all-equal early window from claiming zero uncertainty.
    pub fn statistics(&self) -> (f64, f64, f64, f64) {
        let n = self.len();
        if n == 0 {
            return (0., 0., -1., 1.);
        }
        let graph = self.samples.iter().map(|x| x.0).sum::<f64>() / n as f64;
        let ivf = self.samples.iter().map(|x| x.1).sum::<f64>() / n as f64;
        let gap = graph - ivf;
        let variance = self
            .samples
            .iter()
            .map(|(g, i)| (g - i - gap).powi(2))
            .sum::<f64>()
            / n.saturating_sub(1).max(1) as f64;
        let radius = 1.96 * ((variance + 0.25 / n as f64) / n as f64).sqrt();
        (graph, ivf, gap - radius, gap + radius)
    }
    pub fn settled(&self) -> bool {
        self.serving == Some(self.trial)
            && (self.trial == MIN
                || (self.bad >= MIN && self.trial - self.bad <= self.trial.div_ceil(8)))
    }
    fn lower(&self) -> usize {
        if self.bad < MIN {
            (self.trial * 4 / 5).max(MIN)
        } else if self.trial - self.bad > self.trial.div_ceil(8) {
            self.bad + (self.trial - self.bad) / 2
        } else {
            self.trial
        }
    }
    pub fn observe(&mut self, graph: f64, ivf: f64) -> Update {
        let previous = self.serving;
        let before = self.trial;
        let mut exhausted = false;
        if graph.is_finite() && ivf.is_finite() {
            self.samples
                .push_back((graph.clamp(0., 1.), ivf.clamp(0., 1.)));
            if self.len() > WINDOW {
                self.samples.pop_front();
            }
        }
        if self.len() >= FIRST {
            let (g, i, lower, upper) = self.statistics();
            // Increase only with evidence of inferiority, or a persistent deficit at the
            // bounded observation limit. A noisy eight-query window cannot ratchet effort.
            if upper < 0. || (self.len() == WINDOW && g < i) {
                self.bad = self.bad.max(self.trial);
                if self.serving == Some(self.trial) {
                    self.serving = None;
                }
                if self.trial == MAX {
                    exhausted = self.serving.is_none();
                } else if let Some(good) = self.serving.filter(|good| *good > self.bad) {
                    self.trial = if good - self.bad <= good.div_ceil(8) {
                        good
                    } else {
                        self.bad + (good - self.bad) / 2
                    };
                } else {
                    self.trial = (self.trial * 5).div_ceil(4).min(MAX);
                }
            } else if lower > 0. || (self.len() >= MATCHED && g >= i) {
                // Preserve a validated serving effort while cheaper trials explore below it.
                self.serving = Some(self.trial);
                if self.len() == WINDOW && lower > 0. {
                    self.bad = 0; // Revisit an old lower bound after sustained excess quality.
                }
                self.trial = self.lower();
            }
        }
        let changed = before != self.trial;
        if changed {
            self.samples.clear();
        }
        Update {
            accepted: self.serving != previous && self.serving.is_some(),
            changed,
            exhausted,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn uniform(seed: &mut u64) -> f64 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        (*seed >> 11) as f64 / (1u64 << 53) as f64
    }
    fn stream(initial: usize) -> (Effort, usize) {
        let mut e = Effort::new();
        e.trial = initial;
        let mut seed = 17;
        let mut changes = 0;
        for _ in 0..12_000 {
            let probability = (0.91 + e.trial as f64 * 0.0005).min(0.999);
            // Correlated hard queries and shared per-neighbor noise are present in both
            // plans. The graph matches IVF's true recall at effort 100.
            let hard = uniform(&mut seed) < 0.04;
            let mut graph = 0.;
            let mut ivf = 0.;
            for _ in 0..10 {
                let u = uniform(&mut seed);
                graph += f64::from(u8::from(!hard && u < probability));
                ivf += f64::from(u8::from(!hard && u < 0.96));
            }
            let update = e.observe(graph / 10., ivf / 10.);
            changes += usize::from(update.changed);
            assert!(!update.exhausted);
            assert!(e.len() <= WINDOW);
        }
        (e, changes)
    }
    #[test]
    fn noisy_paired_recall_settles_near_the_true_effort_from_both_directions() {
        for initial in [MIN, 512] {
            let (e, changes) = stream(initial);
            let serving = e
                .serving
                .expect("paired controller must find a serving effort");
            assert!((90..=125).contains(&serving), "{initial}: {serving}");
            assert!(
                changes < 160,
                "unbounded noise-driven oscillation: {changes}"
            );
        }
    }
    #[test]
    fn an_early_difficult_window_cannot_ratchet_and_failed_lower_trials_keep_serving() {
        let mut e = Effort::new();
        for _ in 0..8 {
            e.observe(0., 1.);
        }
        assert_eq!(e.trial, MIN);
        assert!(e.serving.is_none());
        e.trial = 128;
        e.samples.clear();
        for _ in 0..MATCHED {
            e.observe(1., 0.9);
            if e.trial != 128 {
                break;
            }
        }
        assert_eq!(e.serving, Some(128));
        assert!(e.trial < 128);
        while e.trial != 128 {
            e.observe(0.8, 0.9);
            assert_eq!(e.serving, Some(128));
        }
        assert!(e.settled());
    }
    #[test]
    fn invalid_samples_are_ignored_and_an_unattainable_target_exhausts_the_bound() {
        let mut e = Effort::new();
        e.observe(f64::NAN, 1.);
        assert_eq!(e.len(), 0);
        for _ in 0..20_000 {
            if e.observe(0.8, 0.95).exhausted {
                assert_eq!(e.trial, MAX);
                assert!(e.serving.is_none());
                return;
            }
        }
        panic!("unattainable quality did not terminate");
    }
}
