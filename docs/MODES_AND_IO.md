# Idle modes and io_uring

## Idle modes

By default every event loop busy-polls (`CoreMode::Spin`): lowest latency, 100 % of the core at
all times. `StartSequence::core_mode` chooses per core:

```rust
use std::time::Duration;
use cannes::{CoreMode, StartSequence};

let mut seq = StartSequence::new();
seq.core_mode(1, CoreMode::Spin);                                   // critical path
seq.core_mode(2, CoreMode::Sleep(Duration::from_micros(100)));      // sleep after every idle loop
seq.core_mode(3, CoreMode::Adaptive { idle_loops: 1_000, sleep: Duration::from_micros(20) });
```

- `Sleep(d)`: after an iteration that handled no event, sleep `d`.
- `Adaptive { idle_loops, sleep }`: spin while events keep coming; after `idle_loops` consecutive
  idle iterations, sleep `sleep` after each further idle iteration until an event shows up.

A sleeping loop is not woken up by incoming events from other cores: it finds them when the sleep
ends, so the sleep duration adds to the latency of events reaching an idle core (and to the
precision of timers served from that core). Senders are not affected: publishing is unchanged,
there is no wake-up protocol on the hot path. With the `io-uring` feature, a core that has a ring
sleeps in `io_uring_enter` instead, and an I/O completion wakes it at once.

Engine threads that may sleep set their timer slack to 1 ns (`PR_SET_TIMERSLACK`): by default
Linux may delay a timed sleep by up to 50 µs.

`CoreCounters` reports `sleeps`, `slept_ns` and `cpu_ns` (thread CPU time), so the saving can be
measured in production.

### Cost vs. latency

`cargo run --release --example load`: a generator on core 1 sends requests at random times
(Poisson arrivals) to a worker on core 2, in the mode under test, which answers each one.
Latency is measured from the scheduled send time (no coordinated omission); worker CPU is the
worker thread's CPU time over the run. 2 s per run, KVM cloud VM (4 vCPU Sapphire Rapids), not
tuned: tails beyond p99 mostly show the hypervisor.

| mode | load (msg/s) | worker CPU | p50 (µs) | p99 (µs) | p99.9 (µs) | sleeps |
|---|---:|---:|---:|---:|---:|---:|
| spin | 1000 | 99.1 % | 1.4 | 635.8 | 2257.8 | 0 |
| sleep 10us | 1000 | 60.4 % | 14.9 | 76.2 | 936.6 | 76001 |
| sleep 100us | 1000 | 13.8 % | 59.1 | 406.7 | 4187.0 | 17575 |
| adaptive 1k loops / 20us | 1000 | 48.1 % | 18.6 | 215.8 | 2113.9 | 59804 |
| adaptive 50k loops / 100us | 1000 | 42.8 % | 28.5 | 247.1 | 2131.8 | 11652 |
| spin | 10000 | 98.9 % | 0.8 | 235.9 | 2249.0 | 0 |
| sleep 10us | 10000 | 63.2 % | 14.3 | 185.6 | 3569.4 | 75566 |
| sleep 100us | 10000 | 14.1 % | 60.1 | 12032.4 | 30161.9 | 17100 |
| adaptive 1k loops / 20us | 10000 | 50.9 % | 16.7 | 581.2 | 3135.0 | 56713 |
| adaptive 50k loops / 100us | 10000 | 95.6 % | 0.9 | 124.2 | 1565.0 | 748 |
| spin | 100000 | 99.7 % | 0.8 | 19.1 | 275.7 | 0 |
| sleep 10us | 100000 | 60.8 % | 13.8 | 109.6 | 1308.0 | 78025 |
| sleep 100us | 100000 | 14.6 % | 59.4 | 254.7 | 10084.0 | 17373 |
| adaptive 1k loops / 20us | 100000 | 62.1 % | 11.4 | 196.0 | 1758.8 | 43055 |
| adaptive 50k loops / 100us | 100000 | 99.2 % | 0.8 | 21.6 | 689.7 | 86 |
| spin | 300000 | 99.6 % | 0.8 | 3101.7 | 13437.9 | 0 |
| sleep 10us | 300000 | 61.8 % | 13.7 | 421.5 | 2343.9 | 79806 |
| sleep 100us | 300000 | 14.7 % | 59.7 | 134.5 | 1182.8 | 17524 |
| adaptive 1k loops / 20us | 300000 | 74.4 % | 2.4 | 194.3 | 2245.2 | 29264 |
| adaptive 50k loops / 100us | 300000 | 97.5 % | 0.8 | 883.2 | 3136.4 | 107 |

What it shows:

- **Short sleeps are not free.** A sleep/wake cycle costs on the order of 10 µs of kernel time on
  this VM, so `Sleep(10 µs)` still burns ~60 % of the core. From 100 µs, the idle core drops to
  ~14 %, at the price of ~60 µs median latency.
- **Adaptive follows the traffic rhythm.** With 50 000 idle loops tolerated (~1 ms here), it keeps
  spinning from 10 000 msg/s (median under 1 µs, ~96 % CPU) and only saves CPU when gaps exceed
  that; with 1 000 loops it sleeps between messages at low rates.
- Rule of thumb: `Spin` for the critical path, `Sleep(≥ 100 µs)` for cores that are mostly idle,
  `Adaptive` with `idle_loops` sized to the gap you are willing to spin through.

## io_uring (feature `io-uring`, Linux)

```rust,ignore
seq.io_uring(1, 256); // core 1 gets a ring with 256 submission slots

impl Actor for Server {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<IoCompletion, Self>();
        ctx.io().unwrap().accept(self.listener_fd, ACCEPT);
    }
}
impl Handler<IoCompletion> for Server {
    fn on_event(&mut self, ctx: &mut Context, _: ActorId, c: &IoCompletion) {
        // c.token, c.result (bytes, new fd, or -errno), c.take_buf() (zero-copy)
    }
}
```

- One ring per core, created by the core's own thread (`SINGLE_ISSUER`): no locks.
- Operations queued during an iteration are submitted with one `io_uring_enter` at the end of
  the iteration; completions are read from shared memory at the start of the next one and
  dispatched as `IoCompletion` events to the actor that submitted them.
- The ring is set up with `COOP_TASKRUN` and `TASKRUN_FLAG`: completions that need kernel task
  work only raise a flag, which the loop checks; a spinning loop is never interrupted by IPIs and
  makes no system call when it has nothing to submit.
- Buffers are moved into the engine for the duration of the operation and handed back with
  `IoCompletion::take_buf`, capacity intact, ready for the next operation. At shutdown,
  operations still in flight are cancelled; buffers the kernel has not released after 200 ms are
  leaked rather than freed under its feet.
- Supported operations: `accept`, `recv`, `send`, `read`, `write`, `close`.

`tests/io_uring.rs` has a complete TCP echo server (concurrent clients, partial sends), a file
write/read, and a check that a core sleeping 500 ms at a time answers TCP in well under 100 ms.
On the test VM, an echo round trip over loopback TCP takes about 20 µs at p50.
