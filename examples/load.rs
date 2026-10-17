//! Open-loop load benchmark: CPU cost vs. latency of each `CoreMode` at several loads.
//!
//! A generator on core 1 (always `Spin`) sends requests to a worker on core 2 at random times
//! (Poisson arrivals) at a fixed rate; the worker, in the mode under test, answers each one.
//! Latency is measured from the *scheduled* send time to the reception of the answer, so a
//! late generator does not hide queueing (no coordinated omission). The worker core's CPU time
//! comes from its counters.
//!
//! usage: cargo run --release --example load -- [secs_per_run=2] [worker_core=2] [generator_core=1]

use std::sync::mpsc;
use std::time::{Duration, Instant};

use cannes::{Actor, ActorId, Context, CoreMode, Engine, Handler, StartSequence};

struct Request {
    scheduled: Instant,
}

struct Response {
    scheduled: Instant,
}

struct Worker;

impl Actor for Worker {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Request, Self>();
    }
}

impl Handler<Request> for Worker {
    fn on_event(&mut self, ctx: &mut Context, src: ActorId, r: &Request) {
        ctx.send(src, Response { scheduled: r.scheduled });
    }
}

struct Generator {
    worker: ActorId,
    rate: f64,
    total: usize,
    sent: usize,
    next: Instant,
    rng: u64,
    latencies: Vec<u64>,
    report: mpsc::Sender<Vec<u64>>,
}

impl Generator {
    /// Exponentially distributed gap between arrivals, for a Poisson process at `rate`.
    fn gap(&mut self) -> Duration {
        // xorshift64*
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        let u = ((self.rng.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 11) as f64 + 1.0) / (1u64 << 53) as f64;
        Duration::from_secs_f64(-u.ln() / self.rate)
    }
}

impl Actor for Generator {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Response, Self>();
        self.next = Instant::now() + Duration::from_millis(10);
        ctx.register_callback();
    }

    fn on_callback(&mut self, ctx: &mut Context) {
        let now = Instant::now();
        while self.sent < self.total && self.next <= now {
            ctx.send(self.worker, Request { scheduled: self.next });
            self.sent += 1;
            let gap = self.gap();
            self.next += gap;
        }
        if self.sent < self.total {
            ctx.register_callback();
        }
    }
}

impl Handler<Response> for Generator {
    fn on_event(&mut self, _ctx: &mut Context, _src: ActorId, r: &Response) {
        self.latencies.push(r.scheduled.elapsed().as_nanos() as u64);
        if self.latencies.len() == self.total {
            let _ = self.report.send(std::mem::take(&mut self.latencies));
        }
    }
}

struct Outcome {
    p50: f64,
    p99: f64,
    p999: f64,
    cpu_percent: f64,
    sleeps: u64,
}

fn run(mode: CoreMode, rate: f64, secs: f64, worker_core: usize, generator_core: usize) -> Outcome {
    let total = (rate * secs) as usize;
    let (tx, rx) = mpsc::channel();
    let mut seq = StartSequence::new();
    seq.core_mode(worker_core, mode);
    let worker = seq.add_actor(worker_core, || Worker);
    seq.add_actor(generator_core, move || Generator {
        worker,
        rate,
        total,
        sent: 0,
        next: Instant::now(),
        rng: 0x9e37_79b9_7f4a_7c15,
        latencies: Vec::with_capacity(total),
        report: tx,
    });
    let start = Instant::now();
    let engine = Engine::start(seq);
    let mut latencies = rx.recv().expect("generator did not report");
    let counters = engine.shutdown();
    let elapsed = start.elapsed();
    let worker = counters.iter().find(|c| c.core == worker_core).expect("worker counters");

    latencies.sort_unstable();
    let pct = |p: f64| latencies[(p / 100.0 * (latencies.len() - 1) as f64) as usize] as f64 / 1000.0;
    Outcome {
        p50: pct(50.0),
        p99: pct(99.0),
        p999: pct(99.9),
        cpu_percent: worker.cpu_ns as f64 / elapsed.as_nanos() as f64 * 100.0,
        sleeps: worker.sleeps,
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let secs: f64 = args.get(1).map_or(2.0, |s| s.parse().expect("secs_per_run"));
    let worker_core: usize = args.get(2).map_or(2, |s| s.parse().expect("worker_core"));
    let generator_core: usize = args.get(3).map_or(1, |s| s.parse().expect("generator_core"));

    let us = Duration::from_micros;
    let modes = [
        ("spin", CoreMode::Spin),
        ("sleep 10us", CoreMode::Sleep(us(10))),
        ("sleep 100us", CoreMode::Sleep(us(100))),
        ("adaptive 1k loops / 20us", CoreMode::Adaptive { idle_loops: 1_000, sleep: us(20) }),
        ("adaptive 50k loops / 100us", CoreMode::Adaptive { idle_loops: 50_000, sleep: us(100) }),
    ];
    let rates = [1_000.0, 10_000.0, 100_000.0, 300_000.0];

    println!("| mode | load (msg/s) | worker CPU | p50 (µs) | p99 (µs) | p99.9 (µs) | sleeps |");
    println!("|---|---:|---:|---:|---:|---:|---:|");
    for &rate in &rates {
        for (name, mode) in modes {
            let o = run(mode, rate, secs, worker_core, generator_core);
            println!(
                "| {name} | {rate:.0} | {:.1} % | {:.1} | {:.1} | {:.1} | {} |",
                o.cpu_percent, o.p50, o.p99, o.p999, o.sleeps
            );
        }
    }
}
