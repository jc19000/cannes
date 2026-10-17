//! Inter-core round-trip latency benchmark, mirroring bench/simplx-cpp/pingpong_bench.cpp.
//!
//! - `engine`: Ping actor (core A) and Pong service (core B) on the engine.
//! - `raw`: two bare pinned threads and two SPSC rings, no engine: the floor for this hardware.
//!
//! One event in flight at a time; each round trip is timed with rdtsc, calibrated against
//! `Instant` over the run. Off x86_64 the tick source falls back to `Instant` (nanoseconds),
//! which is coarser and slower to read than rdtsc.
//!
//! usage: cargo run --release --example pingpong -- [iterations=1000000] [warmup=100000] [ping_core=1] [pong_core=2] [engine|raw]

use std::sync::mpsc;
#[cfg(not(target_arch = "x86_64"))]
use std::sync::OnceLock;
use std::thread;
use std::time::Instant;

use cannes::{affinity, spsc, Actor, ActorId, Context, Engine, Handler, StartSequence};

#[derive(Clone, Copy)]
struct PingEvent {
    #[allow(dead_code)] // carried for parity with the C++ event
    seq: u64,
    t0: u64,
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn rdtsc() -> u64 {
    unsafe { std::arch::x86_64::_rdtsc() }
}

/// Fallback tick source: nanoseconds since the first call.
#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn rdtsc() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

struct Results {
    samples: Vec<u64>,
    total_ticks: u64,
}

struct PongActor;
struct PongTag;

impl Actor for PongActor {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<PingEvent, Self>();
    }
}

impl Handler<PingEvent> for PongActor {
    fn on_event(&mut self, ctx: &mut Context, src: ActorId, event: &PingEvent) {
        ctx.send(src, *event);
    }
}

struct PingActor {
    iterations: u64,
    warmup: u64,
    count: u64,
    start: u64,
    samples: Vec<u64>,
    report: mpsc::Sender<Results>,
}

impl Actor for PingActor {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<PingEvent, Self>();
        let pong = ctx.service::<PongTag>().expect("pong service");
        ctx.send(pong, PingEvent { seq: 0, t0: rdtsc() });
    }
}

impl Handler<PingEvent> for PingActor {
    fn on_event(&mut self, ctx: &mut Context, src: ActorId, event: &PingEvent) {
        let now = rdtsc();
        if self.count >= self.warmup {
            self.samples.push(now - event.t0);
        } else if self.count + 1 == self.warmup {
            self.start = now;
        }
        self.count += 1;
        if self.count == self.warmup + self.iterations {
            let samples = std::mem::take(&mut self.samples);
            let _ = self.report.send(Results { samples, total_ticks: now - self.start });
            return;
        }
        ctx.send(src, PingEvent { seq: self.count, t0: rdtsc() });
    }
}

fn run_engine(iterations: u64, warmup: u64, ping_core: usize, pong_core: usize) -> Results {
    let (tx, rx) = mpsc::channel();
    let mut sequence = StartSequence::new();
    sequence.add_service::<PongTag, _, _>(pong_core, || PongActor);
    sequence.add_actor(ping_core, move || PingActor {
        iterations,
        warmup,
        count: 0,
        start: 0,
        samples: Vec::with_capacity(iterations as usize),
        report: tx,
    });
    let engine = Engine::start(sequence);
    let results = rx.recv().expect("ping actor did not report");
    // After the run, as in the C++ bench.
    for c in engine.shutdown() {
        eprintln!(
            "counters core={} loop_total={} loop_usage={} events={} callbacks={} publications={} bytes_written_to={:?}",
            c.core, c.loop_total, c.loop_usage, c.events, c.callbacks, c.publications, c.bytes_written_to
        );
    }
    results
}

fn run_raw(iterations: u64, warmup: u64, ping_core: usize, pong_core: usize) -> Results {
    let (mut to_pong, mut from_ping) = spsc::channel::<PingEvent>(4096);
    let (mut to_ping, mut from_pong) = spsc::channel::<PingEvent>(4096);
    let total = warmup + iterations;

    let pong = thread::spawn(move || {
        affinity::pin_current_thread(pong_core).expect("pin pong");
        let mut n = 0;
        while n < total {
            if let Some(e) = from_ping.pop() {
                while to_ping.push(e).is_err() {}
                n += 1;
            }
        }
    });

    let mut results = Results { samples: Vec::with_capacity(iterations as usize), total_ticks: 0 };
    affinity::pin_current_thread(ping_core).expect("pin ping");
    let mut start = 0;
    for count in 0..total {
        let _ = to_pong.push(PingEvent { seq: count, t0: rdtsc() });
        let e = loop {
            if let Some(e) = from_pong.pop() {
                break e;
            }
        };
        let now = rdtsc();
        if count >= warmup {
            results.samples.push(now - e.t0);
        } else if count + 1 == warmup {
            start = now;
        }
        if count + 1 == total {
            results.total_ticks = now - start;
        }
    }
    pong.join().unwrap();
    results
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |i: usize, default: &str| args.get(i).cloned().unwrap_or_else(|| default.to_string());
    let iterations: u64 = arg(1, "1000000").parse().expect("iterations");
    let warmup: u64 = arg(2, "100000").parse().expect("warmup");
    let ping_core: usize = arg(3, "1").parse().expect("ping_core");
    let pong_core: usize = arg(4, "2").parse().expect("pong_core");
    let mode = arg(5, "engine");

    let wall0 = Instant::now();
    let tsc0 = rdtsc();
    let mut results = match mode.as_str() {
        "engine" => run_engine(iterations, warmup, ping_core, pong_core),
        "raw" => run_raw(iterations, warmup, ping_core, pong_core),
        other => panic!("unknown mode {other}, expected engine|raw"),
    };
    let tsc1 = rdtsc();
    let ns_per_tick = wall0.elapsed().as_nanos() as f64 / (tsc1 - tsc0) as f64;

    let samples = &mut results.samples;
    samples.sort_unstable();
    let pct = |p: f64| samples[(p / 100.0 * (samples.len() - 1) as f64) as usize] as f64 * ns_per_tick;
    let mean = samples.iter().map(|&s| s as f64).sum::<f64>() / samples.len() as f64 * ns_per_tick;
    let total_ns = results.total_ticks as f64 * ns_per_tick;

    println!("impl=cannes-{mode} iterations={} cores={ping_core},{pong_core}", samples.len());
    println!(
        "rtt_ns min={:.0} p50={:.0} p90={:.0} p99={:.0} p99.9={:.0} p99.99={:.0} max={:.0} mean={:.1}",
        pct(0.0),
        pct(50.0),
        pct(90.0),
        pct(99.0),
        pct(99.9),
        pct(99.99),
        pct(100.0),
        mean
    );
    println!("throughput round_trips_per_s={:.0}", samples.len() as f64 / (total_ns * 1e-9));
}
