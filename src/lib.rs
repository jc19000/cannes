//! # cannes: a thread-per-core actor engine for low-latency systems
//!
//! `cannes` runs one busy-polling event loop per CPU core, each pinned to its core. Actors live
//! on one core for their whole life, so their state needs no locks, no atomics and no `Send`.
//! Cores talk through a full mesh of lock-free single-producer / single-consumer rings, one per
//! ordered pair of cores. Events sent during a loop iteration are published with one Release
//! store per destination core at the end of the iteration (or earlier with
//! [`Context::flush`]), and handlers read them in place in the ring, without copying.
//!
//! On a 2-core ping-pong (one event in flight), an inter-core round trip takes about 600 ns on a
//! cloud VM, within 10 to 20 ns of two bare threads exchanging through the same rings, and about
//! twice as fast as the C++ framework it is modelled on, Tredzone
//! [simplx](https://github.com/Tredzone/simplx). See the README for the numbers and method.
//!
//! ## Example
//!
//! ```no_run
//! use cannes::{Actor, ActorId, Context, Engine, Handler, StartSequence};
//!
//! struct Ping(u64);
//!
//! struct Pong;
//! impl Actor for Pong {
//!     fn on_start(&mut self, ctx: &mut Context) {
//!         ctx.register_handler::<Ping, Self>();
//!     }
//! }
//! impl Handler<Ping> for Pong {
//!     fn on_event(&mut self, ctx: &mut Context, src: ActorId, ping: &Ping) {
//!         ctx.send(src, Ping(ping.0 + 1)); // back to the sender, on its own core
//!     }
//! }
//!
//! struct Starter {
//!     pong: ActorId,
//! }
//! impl Actor for Starter {
//!     fn on_start(&mut self, ctx: &mut Context) {
//!         ctx.register_handler::<Ping, Self>();
//!         ctx.send(self.pong, Ping(0));
//!     }
//! }
//! impl Handler<Ping> for Starter {
//!     fn on_event(&mut self, ctx: &mut Context, src: ActorId, ping: &Ping) {
//!         if ping.0 < 1_000_000 {
//!             ctx.send(src, Ping(ping.0 + 1));
//!         }
//!     }
//! }
//!
//! let mut sequence = StartSequence::new();
//! let pong = sequence.add_actor(2, || Pong); // event loop pinned to core 2
//! sequence.add_actor(1, move || Starter { pong }); // event loop pinned to core 1
//! let engine = Engine::start(sequence);
//! std::thread::sleep(std::time::Duration::from_secs(1));
//! let counters = engine.shutdown(); // per-core loop, event and publication counts
//! println!("{counters:?}");
//! ```
//!
//! ## What is in the box
//!
//! - Typed, heterogeneous events of any size ([`Handler`]); undelivered events come back to their
//!   sender ([`UndeliveredHandler`], [`Context::return_to_sender`]).
//! - Actor lifecycle: dynamic creation ([`Context::new_actor`]), a destroy protocol that an actor
//!   can postpone ([`Actor::on_destroy_request`]), callbacks at the end of the loop iteration.
//! - Services reachable by tag from any core ([`StartSequence::add_service`]).
//! - Direct, synchronous calls between actors of the same core ([`ActorRef`]), with simplx's
//!   reference semantics, and per-core singletons ([`Context::singleton`]).
//! - Timers built like simplx's: one service, one hub per core, one cross-core event per tick
//!   ([`Timer`]).
//! - Per-core idle modes ([`CoreMode`]): busy-poll, fixed sleep, or adaptive, to trade latency
//!   for CPU.
//! - Per-core io_uring (feature `io-uring`, Linux): network and file I/O from actors, completions
//!   delivered as events (`Context::io`, `IoCompletion`).
//! - Per-core performance counters ([`CoreCounters`]): loop saturation, events, batching factor,
//!   bytes per destination core, sleeps, CPU time.
//!
//! ## What the compiler checks for you
//!
//! - An [`ActorRef`] is not `Send`: a direct reference cannot escape to another core.
//! - Ring endpoints are split into a non-clonable producer and consumer: one writer and one
//!   reader per ring, by construction.
//! - Events must be `Send + 'static`: nothing sent across cores can point into another core's
//!   memory.
//! - A re-entrant call into a running actor panics instead of aliasing it.
//!
//! ## Platform
//!
//! Thread pinning is implemented on Linux; elsewhere the loops run unpinned. The engine
//! busy-polls: each core of the start sequence runs at 100 % by design.

#![warn(missing_docs)]

pub mod affinity;
pub mod bytering;
pub mod engine;
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub mod io;
pub mod spsc;
mod sys;
pub mod timer;

pub use engine::{
    Actor, ActorId, ActorRef, Context, CoreCounters, CoreId, CoreMode, Engine, Event, Handler, ReferenceError,
    StartSequence, UndeliveredHandler,
};
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub use io::{Io, IoCompletion};
pub use timer::{NoTimerService, Timeout, Timer, TimerService, TimerServiceTag};
