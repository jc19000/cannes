//! Per-core idle modes: spin, fixed sleep, adaptive. Each test runs real pinned event loops, so
//! they are serialized.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use cannes::{Actor, ActorId, Context, CoreCounters, CoreMode, Engine, Handler, StartSequence};

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

struct Ball;

/// Sends the ball back.
struct Wall;

impl Actor for Wall {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Ball, Self>();
    }
}

impl Handler<Ball> for Wall {
    fn on_event(&mut self, ctx: &mut Context, src: ActorId, _: &Ball) {
        ctx.send(src, Ball);
    }
}

/// Plays `rounds` round trips against the wall, then stops.
struct Player {
    wall: ActorId,
    rounds: u64,
    done: Arc<AtomicU64>,
}

impl Actor for Player {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Ball, Self>();
        ctx.send(self.wall, Ball);
    }
}

impl Handler<Ball> for Player {
    fn on_event(&mut self, ctx: &mut Context, src: ActorId, _: &Ball) {
        let done = self.done.fetch_add(1, Ordering::AcqRel) + 1;
        if done < self.rounds {
            ctx.send(src, Ball);
        }
    }
}

/// Plays `rounds` round trips with the wall on core 2 in `mode`, then leaves the engine idle for
/// `idle`. Returns the counters of core 2 and the elapsed time.
fn run(mode: CoreMode, rounds: u64, idle: Duration) -> (CoreCounters, Duration) {
    let done = Arc::new(AtomicU64::new(0));
    let mut seq = StartSequence::new();
    seq.core_mode(2, mode);
    let wall = seq.add_actor(2, || Wall);
    let d = done.clone();
    seq.add_actor(1, move || Player { wall, rounds, done: d });
    let start = Instant::now();
    let engine = Engine::start(seq);
    let deadline = Instant::now() + Duration::from_secs(20);
    while done.load(Ordering::Acquire) < rounds {
        assert!(Instant::now() < deadline, "only {} round trips", done.load(Ordering::Acquire));
        std::thread::sleep(Duration::from_millis(1));
    }
    let busy = engine.counters().into_iter().find(|c| c.core == 2).unwrap_or_default();
    std::thread::sleep(idle);
    let counters = engine.shutdown();
    let elapsed = start.elapsed();
    let core2 = counters.into_iter().find(|c| c.core == 2).unwrap();
    assert!(core2.sleeps >= busy.sleeps);
    (core2, elapsed)
}

#[test]
fn spin_never_sleeps() {
    let _g = serial();
    let (c, _) = run(CoreMode::Spin, 1000, Duration::from_millis(50));
    assert_eq!(c.events, 1000);
    assert_eq!((c.sleeps, c.slept_ns), (0, 0));
}

#[test]
fn sleep_mode_delivers_everything_and_frees_the_core_when_idle() {
    let _g = serial();
    let (c, elapsed) = run(CoreMode::Sleep(Duration::from_micros(500)), 200, Duration::from_millis(300));
    assert_eq!(c.events, 200);
    assert!(c.sleeps > 100, "{c:?}");
    // Mostly asleep over the idle period: well under half of the elapsed time on CPU.
    assert!(c.cpu_ns < elapsed.as_nanos() as u64 / 2, "cpu {} ns over {:?}: {c:?}", c.cpu_ns, elapsed);
    assert!(c.slept_ns > elapsed.as_nanos() as u64 / 4, "{c:?}");
}

#[test]
fn adaptive_spins_under_traffic_and_sleeps_when_idle() {
    let _g = serial();
    // Continuous traffic: an event every round trip, far below 100 000 idle iterations apart.
    let (busy, _) =
        run(CoreMode::Adaptive { idle_loops: 100_000, sleep: Duration::from_micros(200) }, 50_000, Duration::ZERO);
    assert_eq!(busy.events, 50_000);
    let (idle, elapsed) = run(
        CoreMode::Adaptive { idle_loops: 1000, sleep: Duration::from_micros(200) },
        1000,
        Duration::from_millis(300),
    );
    assert!(idle.sleeps > 200, "{idle:?}");
    assert!(idle.cpu_ns < elapsed.as_nanos() as u64 / 2, "cpu {} ns over {:?}", idle.cpu_ns, elapsed);
    // Under traffic, sleeps only happen around start-up and shutdown, not between round trips.
    assert!(busy.sleeps < busy.events / 100, "{busy:?}");
}
