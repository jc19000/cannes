# Changelog

## 0.2.0

- Per-core idle modes in the start sequence: `CoreMode::Spin` (default), `Sleep(d)`,
  `Adaptive { idle_loops, sleep }`; timer slack set to 1 ns on threads that may sleep.
- Feature `io-uring` (Linux): one io_uring per core, `Context::io()` (`accept`, `recv`, `send`,
  `read`, `write`, `close`), completions delivered as `IoCompletion` events with zero-copy buffer
  hand-back; sleeping cores wait in io_uring and are woken by I/O.
- `CoreCounters`: `sleeps`, `slept_ns`, `cpu_ns`.
- `examples/load.rs`: open-loop load benchmark (CPU vs. latency per idle mode).
- No change on the `Spin` hot path (ping-pong p50 unchanged).

## 0.1.0

- First release: thread-per-core engine, SPSC byte rings, heterogeneous events, undelivered
  events, lifecycle and destroy protocol, services, references and singletons, timers, counters,
  manual flush.
