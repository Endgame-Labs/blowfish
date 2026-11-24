//! Optional perf-stat FIFO control: count only the resident query region, excluding load/build.
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    time::Instant,
};
pub struct Region {
    start: Instant,
    perf: Option<(File, BufReader<File>)>,
}
impl Region {
    pub fn begin() -> Self {
        let perf = std::env::var_os("G3_PERF_CTL").map(|ctl| {
            let control = OpenOptions::new()
                .write(true)
                .open(ctl)
                .expect("perf control FIFO");
            let ack = File::open(std::env::var_os("G3_PERF_ACK").expect("perf ack path"))
                .expect("perf ack FIFO");
            (control, BufReader::new(ack))
        });
        let mut region = Self {
            start: Instant::now(),
            perf,
        };
        region.command("enable\n");
        region.start = Instant::now();
        region
    }
    fn command(&mut self, s: &str) {
        if let Some((control, ack)) = &mut self.perf {
            control.write_all(s.as_bytes()).unwrap();
            control.flush().unwrap();
            let mut line = String::new();
            ack.read_line(&mut line).unwrap();
            assert!(line.starts_with("ack"));
        }
    }
    pub fn end(mut self) -> f64 {
        let seconds = self.start.elapsed().as_secs_f64();
        self.command("disable\n");
        seconds
    }
}
