//! TEMPORARY (Round G29 measurement only; not committed): a switch file the server re-reads, and
//! counters it writes beside it.
use std::sync::{
    OnceLock, RwLock,
    atomic::{AtomicU64, Ordering::Relaxed},
};

#[derive(Clone, Copy, Debug)]
pub struct Arm {
    pub new: bool,
    pub survivors: usize,
    pub cap: f64,
    pub gate: bool,
    pub slack: f32,
    pub stats: bool,
    pub ef: usize,
    pub patience: u32,
    pub top: usize,
}
impl Default for Arm {
    fn default() -> Self {
        Self {
            new: false,
            survivors: 200,
            cap: 1.,
            gate: false,
            slack: 1.,
            stats: false,
            ef: 0,
            patience: 0,
            top: 10,
        }
    }
}
static ARM: RwLock<Option<Arm>> = RwLock::new(None);
pub static QUERIES: AtomicU64 = AtomicU64::new(0);
pub static EXPANSIONS: AtomicU64 = AtomicU64::new(0);
pub static EVALUATIONS: AtomicU64 = AtomicU64::new(0);
pub static TRANSIT: AtomicU64 = AtomicU64::new(0);
pub static LAST_SEEN: AtomicU64 = AtomicU64::new(0);
pub static DECLINED: AtomicU64 = AtomicU64::new(0);
static STARTED: OnceLock<()> = OnceLock::new();

fn parse(text: &str) -> Arm {
    let mut arm = Arm::default();
    let (mut deg, mut spread) = (0usize, 0usize);
    for word in text.split_whitespace() {
        let (key, value) = word.split_once('=').unwrap_or((word, "1"));
        match key {
            "new" => arm.new = value != "0",
            "s" => arm.survivors = value.parse().unwrap_or(arm.survivors),
            "cap" => arm.cap = value.parse().unwrap_or(1.),
            "gate" => arm.gate = value != "0",
            "slack" => arm.slack = value.parse().unwrap_or(1.),
            "stats" => arm.stats = value != "0",
            "top" => arm.top = value.parse().unwrap_or(10),
            "p" => arm.patience = value.parse().unwrap_or(0),
            "ef" => arm.ef = value.parse().unwrap_or(0),
            "deg" => deg = value.parse().unwrap_or(0),
            "spread" => spread = value.parse().unwrap_or(0),
            _ => {}
        }
    }
    bf_vector::warm_graph::DEGREE_CAP.store(deg, Relaxed);
    bf_vector::warm_graph::DEGREE_SPREAD.store(spread, Relaxed);
    arm
}
pub fn arm() -> Arm {
    if let Some(path) = std::env::var_os("BF_G29_SWITCH") {
        STARTED.get_or_init(|| {
            let path = std::path::PathBuf::from(path);
            std::thread::spawn(move || {
                loop {
                    if let Ok(text) = std::fs::read_to_string(&path) {
                        *ARM.write().unwrap() = Some(parse(&text));
                    }
                    let line = format!(
                        "{} {} {} {} {} {}\n",
                        QUERIES.load(Relaxed),
                        EXPANSIONS.load(Relaxed),
                        EVALUATIONS.load(Relaxed),
                        TRANSIT.load(Relaxed),
                        LAST_SEEN.load(Relaxed),
                        DECLINED.load(Relaxed)
                    );
                    let mut stats = path.clone().into_os_string();
                    stats.push(".stats");
                    let _ = std::fs::write(stats, line);
                    std::thread::sleep(std::time::Duration::from_millis(250));
                }
            });
        });
    }
    ARM.read().unwrap().unwrap_or_default()
}
