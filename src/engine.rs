//! The engine: one pinned, busy-spinning event loop per core, actors owned by their core,
//! and a full mesh of byte rings (one per ordered pair of cores) carrying heterogeneous events.
//!
//! Mapping to simplx:
//!
//! | simplx                                   | here                                         |
//! |------------------------------------------|----------------------------------------------|
//! | `Actor::Event` subclass                  | any `Send + 'static` type                    |
//! | `registerEventHandler<E>(*this)`         | `ctx.register_handler::<E, Self>()`          |
//! | `onEvent(const E&)`                      | `Handler<E>::on_event(&mut self, ctx, src, &E)` |
//! | `Event::Pipe(*this, dst).push<E>(...)`   | `ctx.send(dst, event)`                       |
//! | `registerUndeliveredEventHandler<E>`     | `ctx.register_undelivered_handler::<E, Self>()` |
//! | `throw ReturnToSenderException()`        | `ctx.return_to_sender()`                     |
//! | `newUnreferencedActor<A>()`              | `ctx.new_actor(actor)`                       |
//! | `newReferencedActor<A>()` / `ActorReference<A>` | `ctx.new_referenced_actor(actor)` / `ActorRef<A>` |
//! | `referenceLocalActor<A>(id)`             | `ctx.reference_local_actor::<A>(id)`         |
//! | `newReferencedSingletonActor<A>()`       | `ctx.singleton(\|\| actor)`                    |
//! | `requestDestroy()` / `onDestroyRequest()` / `acceptDestroy()` | `ctx.request_destroy()` / `on_destroy_request() -> bool` |
//! | `registerCallback(*this)` / `onCallback()` | `ctx.register_callback()` / `on_callback()` |
//! | `addServiceActor<Tag, A>(core)` / `getServiceActorId<Tag>()` | `add_service::<Tag, _>(core, f)` / `ctx.service::<Tag>()` |
//! | `Engine` destructor                      | `Engine::shutdown()` / `Drop`                |
//! | `getCorePerformanceCounters()`           | `ctx.counters()` / `Engine::counters()`      |
//!
//! Events sent during a loop iteration are staged and published once per destination core at the
//! end of the iteration, or earlier with `ctx.flush()`. Handlers receive a reference into the ring itself: no copy on delivery.
//! On shutdown, regular actors are asked to destroy themselves first, services once no regular
//! actor is left anywhere; the loops exit when no actor is left.
//!
//! Each actor lives in an `Rc<RefCell<A>>`, so that an `ActorRef<A>` (same core only: it is not
//! `Send`) can call it directly. As in simplx, a referenced actor is not destroyed while
//! referenced, an actor created by `new_referenced_actor` or `singleton` is destroyed once its
//! last reference is dropped, and creating a circular reference is refused. Calling back into an
//! actor that is already running (through a reference) panics instead of aliasing it.

use std::any::{type_name, Any, TypeId};
use std::cell::{Cell, Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::fmt;
use std::mem::{align_of, size_of};
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::affinity;
use crate::bytering::{self, Consumer, Producer, RECORD_ALIGN};
#[cfg(all(feature = "io-uring", target_os = "linux"))]
use crate::io::{Io, IoRing};
use crate::sys;

/// CPU core number, as used by the OS for pinning.
pub type CoreId = usize;

/// Address of an actor: valid on any core, compared by value. A destroyed actor's slot is reused
/// with another `generation`, so stale ids never reach the new occupant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct ActorId {
    /// Core the actor lives on.
    pub core: u16,
    /// Incremented each time the slot is reused.
    pub generation: u16,
    /// Slot of the actor on its core.
    pub index: u32,
}

impl ActorId {
    /// An id no actor ever has; events sent to it come back as undelivered.
    pub const NULL: ActorId = ActorId { core: u16::MAX, generation: 0, index: u32::MAX };
}

/// Anything sent between actors: any `Send + 'static` type (implemented for all of them).
/// Moved into the destination ring, handed to the handler by reference, dropped after it.
pub trait Event: Send + 'static {}
impl<T: Send + 'static> Event for T {}

/// An actor lives on one core for its whole life; it does not need to be `Send`.
pub trait Actor: 'static {
    /// Called once on the actor's core, before any event; register handlers here.
    fn on_start(&mut self, _ctx: &mut Context) {}
    /// Called at the end of the loop iteration in which `register_callback` was called.
    fn on_callback(&mut self, _ctx: &mut Context) {}
    /// Return `false` to postpone destruction. During shutdown the request is repeated on every
    /// loop iteration; otherwise call `request_destroy` again later.
    fn on_destroy_request(&mut self, _ctx: &mut Context) -> bool {
        true
    }
}

/// Handling of events of type `E`; enable with `Context::register_handler::<E, Self>()`.
pub trait Handler<E: Event>: Actor {
    /// Called on the actor's core for each `E` sent to it; `src` is the sender.
    fn on_event(&mut self, ctx: &mut Context, src: ActorId, event: &E);
}

/// Handling of this actor's events of type `E` that could not be delivered (no such actor, no
/// handler, or `return_to_sender`); enable with `Context::register_undelivered_handler`.
pub trait UndeliveredHandler<E: Event>: Actor {
    /// Called with the event and the destination it did not reach.
    fn on_undelivered_event(&mut self, ctx: &mut Context, dst: ActorId, event: &E);
}

// ---- Wire format ---------------------------------------------------------------------------

const KIND_EVENT: u32 = 0;
const KIND_UNDELIVERED: u32 = 1;

/// Record header; the event payload follows at offset `HEADER`. `len` must stay the first field:
/// the byte ring reads it.
#[repr(C)]
struct Header {
    len: u32,
    kind: u32,
    src: ActorId,
    dst: ActorId,
    type_id: TypeId,
    drop_fn: unsafe fn(*mut u8),
}

const HEADER: usize = round_up(size_of::<Header>());

const fn round_up(n: usize) -> usize {
    (n + RECORD_ALIGN - 1) & !(RECORD_ALIGN - 1)
}

const fn record_len<E>() -> usize {
    round_up(HEADER + size_of::<E>())
}

unsafe fn drop_payload<E>(payload: *mut u8) {
    ptr::drop_in_place(payload as *mut E);
}

#[inline]
unsafe fn write_record<E: Event>(record: *mut u8, kind: u32, src: ActorId, dst: ActorId, event: E) {
    const { assert!(align_of::<E>() <= RECORD_ALIGN, "event alignment above 16 is not supported") };
    let header = Header {
        len: record_len::<E>() as u32,
        kind,
        src,
        dst,
        type_id: TypeId::of::<E>(),
        drop_fn: drop_payload::<E>,
    };
    ptr::write(record as *mut Header, header);
    ptr::write(record.add(HEADER) as *mut E, event);
}

/// Growable buffer of records, 16-byte aligned (local events and overflow of full rings).
#[derive(Default)]
struct RecordBuf {
    words: Vec<u128>,
    len: usize,
}

impl RecordBuf {
    #[inline]
    fn alloc(&mut self, n: usize) -> *mut u8 {
        let end = self.len + n;
        let words = end.div_ceil(RECORD_ALIGN);
        if words > self.words.len() {
            self.words.resize(words.max(2 * self.words.len()).max(64), 0);
        }
        let p = unsafe { (self.words.as_mut_ptr() as *mut u8).add(self.len) };
        self.len = end;
        p
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn base(&mut self) -> *mut u8 {
        self.words.as_mut_ptr() as *mut u8
    }

    /// Removes the first `n` bytes.
    fn consume_front(&mut self, n: usize) {
        if n == self.len {
            self.len = 0;
            return;
        }
        let base = self.base();
        unsafe { ptr::copy(base.add(n), base, self.len - n) };
        self.len -= n;
    }
}

// ---- Context ---------------------------------------------------------------------------------

type Thunk = unsafe fn(*mut u8, &mut Context, ActorId, *const u8);

/// Exclusive access to a running actor; a second access means a re-entrant call through an
/// `ActorRef`, which would alias `&mut A`.
#[inline]
fn enter<A>(cell: &RefCell<A>) -> RefMut<'_, A> {
    match cell.try_borrow_mut() {
        Ok(actor) => actor,
        Err(_) => panic!("actor {} re-entered while already running (call through an ActorRef)", type_name::<A>()),
    }
}

unsafe fn event_thunk<E: Event, A: Handler<E>>(actor: *mut u8, ctx: &mut Context, src: ActorId, event: *const u8) {
    enter(&*(actor as *const RefCell<A>)).on_event(ctx, src, &*(event as *const E));
}

unsafe fn undelivered_thunk<E: Event, A: UndeliveredHandler<E>>(
    actor: *mut u8,
    ctx: &mut Context,
    dst: ActorId,
    event: *const u8,
) {
    enter(&*(actor as *const RefCell<A>)).on_undelivered_event(ctx, dst, &*(event as *const E));
}

/// Lifecycle entry points of a type-erased actor.
trait ActorCell {
    fn on_start(&self, ctx: &mut Context);
    fn on_callback(&self, ctx: &mut Context);
    fn on_destroy_request(&self, ctx: &mut Context) -> bool;
}

impl<A: Actor> ActorCell for RefCell<A> {
    fn on_start(&self, ctx: &mut Context) {
        enter(self).on_start(ctx)
    }
    fn on_callback(&self, ctx: &mut Context) {
        enter(self).on_callback(ctx)
    }
    fn on_destroy_request(&self, ctx: &mut Context) -> bool {
        enter(self).on_destroy_request(ctx)
    }
}

/// An actor built on its core, ready to be installed in a slot.
struct NewActor {
    cell: Rc<dyn ActorCell>,
    any: Rc<dyn Any>,
    ptr: *mut u8,
    actor_type: TypeId,
}

fn new_cell<A: Actor>(actor: A) -> (Rc<RefCell<A>>, NewActor) {
    let cell = Rc::new(RefCell::new(actor));
    let new = NewActor {
        cell: cell.clone(),
        any: cell.clone(),
        ptr: Rc::as_ptr(&cell) as *mut u8,
        actor_type: TypeId::of::<A>(),
    };
    (cell, new)
}

#[derive(Clone, Copy, Default)]
struct Spawn {
    service: bool,
    auto_destroy: bool,
    singleton: Option<TypeId>,
}

/// Who references whom on one core (slot indexes), to block destruction of referenced actors
/// and refuse circular references.
#[derive(Default)]
struct RefTable {
    out: HashMap<u32, Vec<u32>>,
    incoming: HashMap<u32, u32>,
    unreferenced: Vec<u32>,
}

/// The table plus a flag the event loop can test each iteration without borrowing it.
#[derive(Default)]
struct Refs {
    pending: Cell<bool>, // `table.unreferenced` is not empty
    table: RefCell<RefTable>,
}

impl Refs {
    fn add(&self, holder: u32, target: u32) {
        self.table.borrow_mut().add(holder, target);
    }

    fn remove(&self, holder: u32, target: u32) {
        let mut table = self.table.borrow_mut();
        table.remove(holder, target);
        if !table.unreferenced.is_empty() {
            self.pending.set(true);
        }
    }
}

impl RefTable {
    fn add(&mut self, holder: u32, target: u32) {
        self.out.entry(holder).or_default().push(target);
        *self.incoming.entry(target).or_default() += 1;
    }

    fn remove(&mut self, holder: u32, target: u32) {
        if let Some(targets) = self.out.get_mut(&holder) {
            if let Some(i) = targets.iter().position(|&t| t == target) {
                targets.swap_remove(i);
            }
            if targets.is_empty() {
                self.out.remove(&holder);
            }
        }
        if let Some(count) = self.incoming.get_mut(&target) {
            *count -= 1;
            if *count == 0 {
                self.incoming.remove(&target);
                self.unreferenced.push(target);
            }
        }
    }

    fn is_referenced(&self, target: u32) -> bool {
        self.incoming.contains_key(&target)
    }

    /// Whether `from` already reaches `to` through references.
    fn reaches(&self, from: u32, to: u32) -> bool {
        let mut stack = vec![from];
        let mut seen = Vec::new();
        while let Some(a) = stack.pop() {
            if a == to {
                return true;
            }
            if !seen.contains(&a) {
                seen.push(a);
                stack.extend(self.out.get(&a).into_iter().flatten().copied());
            }
        }
        false
    }
}

/// Direct, synchronous access to an actor on the same core (simplx `ActorReference`). The actor
/// is not destroyed while a reference to it exists. Not `Send`: it cannot leave its core.
pub struct ActorRef<A: Actor> {
    cell: Rc<RefCell<A>>,
    id: ActorId,
    holder: u32,
    refs: Rc<Refs>,
}

impl<A: Actor> ActorRef<A> {
    /// Id of the referenced actor, to send it events.
    pub fn id(&self) -> ActorId {
        self.id
    }

    /// Shared access to the actor; panics if it is running.
    pub fn borrow(&self) -> Ref<'_, A> {
        match self.cell.try_borrow() {
            Ok(actor) => actor,
            Err(_) => panic!("actor {} is running and cannot be borrowed", type_name::<A>()),
        }
    }

    /// Exclusive access to the actor, to call its methods directly; panics if it is running
    /// (re-entrant call).
    pub fn borrow_mut(&self) -> RefMut<'_, A> {
        enter(&self.cell)
    }
}

impl<A: Actor> Clone for ActorRef<A> {
    /// The clone is held by the same actor as the original, as in simplx.
    fn clone(&self) -> Self {
        self.refs.add(self.holder, self.id.index);
        ActorRef { cell: self.cell.clone(), id: self.id, holder: self.holder, refs: self.refs.clone() }
    }
}

impl<A: Actor> Drop for ActorRef<A> {
    fn drop(&mut self) {
        self.refs.remove(self.holder, self.id.index);
    }
}

/// Why `Context::reference_local_actor` failed.
#[derive(Debug, PartialEq, Eq)]
pub enum ReferenceError {
    /// The actor lives on another core.
    NotLocal,
    /// No live actor has this id.
    NotFound,
    /// The actor is not an `A`.
    WrongType,
    /// The actor already references the caller, directly or not (or is the caller).
    Circular,
}

impl fmt::Display for ReferenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let what = match self {
            ReferenceError::NotLocal => "actor is on another core",
            ReferenceError::NotFound => "no such actor",
            ReferenceError::WrongType => "actor has another type",
            ReferenceError::Circular => "reference would be circular",
        };
        f.write_str(what)
    }
}

impl std::error::Error for ReferenceError {}

struct HandlerRegistration {
    index: u32,
    actor_type: TypeId,
    undelivered: bool,
    type_id: TypeId,
    thunk: Thunk,
}

/// Per-core performance counters (simplx `CorePerformanceCounters`). All cumulative.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CoreCounters {
    /// Core these counters belong to.
    pub core: CoreId,
    /// Event-loop iterations.
    pub loop_total: u64,
    /// Iterations that ran at least one actor callback (event, callback, destroy request):
    /// `loop_usage / loop_total` is the core's saturation.
    pub loop_usage: u64,
    /// Event handler calls, undelivered handlers included.
    pub events: u64,
    /// `on_callback` calls.
    pub callbacks: u64,
    /// Ring publications (one Release store of `tail` each): events sent / publications is the
    /// batching factor.
    pub publications: u64,
    /// Bytes of event records written towards each core (headers included), by core id; the
    /// entry of this core counts local events.
    pub bytes_written_to: Vec<u64>,
    /// Times the loop went to sleep (`CoreMode::Sleep` / `CoreMode::Adaptive`).
    pub sleeps: u64,
    /// Wall-clock time spent sleeping, in nanoseconds.
    pub slept_ns: u64,
    /// CPU time consumed by the core's thread, in nanoseconds (Linux; 0 elsewhere). Updated when
    /// the counters are published, exact at shutdown. `cpu_ns` over elapsed time is the share of
    /// the core actually burnt.
    pub cpu_ns: u64,
}

/// How an event loop waits when it has nothing to do. Set per core with
/// `StartSequence::core_mode`.
///
/// A sleeping loop is not woken up by incoming events: it finds them when the sleep ends. The
/// sleep duration is therefore added to the latency of events reaching an idle core, and to the
/// precision of timers served from that core. Senders are not affected: publishing is unchanged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CoreMode {
    /// Busy-poll forever: lowest latency, 100 % of the core at all times. The default.
    #[default]
    Spin,
    /// Sleep for this long after every iteration that handled no event.
    Sleep(Duration),
    /// Busy-poll while events keep coming; once `idle_loops` consecutive iterations handled no
    /// event, sleep for `sleep` after each further idle iteration, until an event shows up.
    Adaptive {
        /// Consecutive idle iterations before sleeping.
        idle_loops: u32,
        /// Sleep duration once idle.
        sleep: Duration,
    },
}

#[repr(align(128))]
struct Padded<T>(T);

/// Iterations between two publications of the counters for `Engine::counters`.
const COUNTERS_PUBLISH_PERIOD: u64 = 4096;

struct Shared {
    shutdown: AtomicBool,
    live_total: AtomicUsize,
    live_regular: AtomicUsize,
    services: HashMap<TypeId, ActorId>,
    cores: usize,
    cores_done: AtomicUsize,
    counters: Vec<Padded<Mutex<CoreCounters>>>, // by core id, published periodically by each loop
}

#[repr(align(128))]
struct PaddedShared(Shared);

/// Per-core handle given to actors.
pub struct Context {
    core: u16,
    current: ActorId,
    local: RecordBuf,
    outbound: Vec<Option<Producer>>, // indexed by destination core
    overflow: Vec<RecordBuf>,        // records that did not fit in a full ring
    dirty: Vec<CoreId>,
    shared: Arc<PaddedShared>,

    // Effects of the running callback, applied by the event loop once it returns.
    effects: bool,
    return_to_sender: bool,
    registrations: Vec<HandlerRegistration>,
    new_actors: Vec<(ActorId, NewActor, Spawn)>,
    destroy_requests: Vec<ActorId>,
    callbacks: Vec<ActorId>,

    free_slots: Vec<(u32, u16)>,
    next_index: u32,

    cells: Vec<Option<(u16, Rc<dyn Any>)>>, // by slot index, to hand out references
    refs: Rc<Refs>,
    singletons: HashMap<TypeId, ActorId>,

    counters: CoreCounters,
    active: bool, // an actor callback ran in this iteration

    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    io: Option<IoRing>,
}

impl Context {
    /// Id of the actor being called.
    #[inline]
    pub fn id(&self) -> ActorId {
        self.current
    }

    /// Core of this event loop.
    #[inline]
    pub fn core(&self) -> CoreId {
        self.core as CoreId
    }

    /// Sends `event` to `dst`, on this core or another. Remote events are published at the end of
    /// the loop iteration or on `flush`; events from one actor to another arrive in order.
    #[inline]
    pub fn send<E: Event>(&mut self, dst: ActorId, event: E) {
        self.send_from(self.current, dst, event);
    }

    /// Sends on behalf of `src` (an actor of this core), e.g. from a method called through an
    /// `ActorRef` while another actor is running.
    #[inline]
    pub(crate) fn send_from<E: Event>(&mut self, src: ActorId, dst: ActorId, event: E) {
        let record = self.alloc_record(dst.core, record_len::<E>());
        unsafe { write_record(record, KIND_EVENT, src, dst, event) };
    }

    /// Event handlers of this actor's type for `E`; call from `on_start` (or later).
    pub fn register_handler<E: Event, A: Handler<E>>(&mut self) {
        self.registrations.push(HandlerRegistration {
            index: self.current.index,
            actor_type: TypeId::of::<A>(),
            undelivered: false,
            type_id: TypeId::of::<E>(),
            thunk: event_thunk::<E, A>,
        });
        self.effects = true;
    }

    /// Undelivered handler of this actor's type for its own events of type `E`.
    pub fn register_undelivered_handler<E: Event, A: UndeliveredHandler<E>>(&mut self) {
        self.registrations.push(HandlerRegistration {
            index: self.current.index,
            actor_type: TypeId::of::<A>(),
            undelivered: true,
            type_id: TypeId::of::<E>(),
            thunk: undelivered_thunk::<E, A>,
        });
        self.effects = true;
    }

    /// From an event handler: send the event back to its sender as undelivered.
    pub fn return_to_sender(&mut self) {
        self.return_to_sender = true;
    }

    /// Creates an actor on this core; it is started once the current callback returns.
    pub fn new_actor<A: Actor>(&mut self, actor: A) -> ActorId {
        self.spawn(new_cell(actor).1, Spawn::default())
    }

    /// Creates an actor on this core and returns a reference to it; the actor is destroyed once
    /// the last reference is dropped. Its methods can be called right away; `on_start` runs once
    /// the current callback returns.
    pub fn new_referenced_actor<A: Actor>(&mut self, actor: A) -> ActorRef<A> {
        let (cell, new) = new_cell(actor);
        let id = self.spawn(new, Spawn { auto_destroy: true, ..Spawn::default() });
        self.make_ref(cell, id)
    }

    /// References an existing actor of this core.
    pub fn reference_local_actor<A: Actor>(&mut self, id: ActorId) -> Result<ActorRef<A>, ReferenceError> {
        if id.core != self.core {
            return Err(ReferenceError::NotLocal);
        }
        let any = match self.cells.get(id.index as usize) {
            Some(Some((generation, any))) if *generation == id.generation => any.clone(),
            _ => return Err(ReferenceError::NotFound),
        };
        let cell = any.downcast::<RefCell<A>>().map_err(|_| ReferenceError::WrongType)?;
        let holder = self.current.index;
        if holder == id.index || self.refs.table.borrow().reaches(id.index, holder) {
            return Err(ReferenceError::Circular);
        }
        Ok(self.make_ref(cell, id))
    }

    /// The instance of `A` on this core, created with `make` on first use and destroyed once no
    /// longer referenced (simplx `newReferencedSingletonActor`).
    pub fn singleton<A: Actor>(&mut self, make: impl FnOnce() -> A) -> ActorRef<A> {
        if let Some(&id) = self.singletons.get(&TypeId::of::<A>()) {
            match self.reference_local_actor(id) {
                Ok(r) => return r,
                Err(e) => panic!("cannot reference singleton {}: {e}", type_name::<A>()),
            }
        }
        let (cell, new) = new_cell(make());
        let id = self.spawn(new, Spawn { auto_destroy: true, singleton: Some(TypeId::of::<A>()), ..Spawn::default() });
        self.singletons.insert(TypeId::of::<A>(), id);
        self.make_ref(cell, id)
    }

    fn spawn(&mut self, new: NewActor, spawn: Spawn) -> ActorId {
        let (index, generation) = self.free_slots.pop().unwrap_or_else(|| {
            self.next_index += 1;
            (self.next_index - 1, 0)
        });
        let id = ActorId { core: self.core, generation, index };
        let shared = &self.shared.0;
        shared.live_total.fetch_add(1, Ordering::AcqRel);
        shared.live_regular.fetch_add(1, Ordering::AcqRel);
        self.set_cell(id, new.any.clone());
        self.new_actors.push((id, new, spawn));
        self.effects = true;
        id
    }

    fn set_cell(&mut self, id: ActorId, any: Rc<dyn Any>) {
        let index = id.index as usize;
        if self.cells.len() <= index {
            self.cells.resize_with(index + 1, || None);
        }
        self.cells[index] = Some((id.generation, any));
    }

    fn make_ref<A: Actor>(&mut self, cell: Rc<RefCell<A>>, id: ActorId) -> ActorRef<A> {
        let holder = self.current.index;
        self.refs.add(holder, id.index);
        ActorRef { cell, id, holder, refs: self.refs.clone() }
    }

    /// `on_destroy_request` of the current actor will be called at the end of this loop iteration,
    /// or once it is no longer referenced.
    pub fn request_destroy(&mut self) {
        self.destroy_requests.push(self.current);
    }

    /// `on_callback` of the current actor will be called at the end of this loop iteration.
    pub fn register_callback(&mut self) {
        self.callbacks.push(self.current);
    }

    /// Id of the service registered under `Tag` in the start sequence.
    pub fn service<Tag: 'static>(&self) -> Option<ActorId> {
        self.shared.0.services.get(&TypeId::of::<Tag>()).copied()
    }

    /// This core's counters, up to date.
    pub fn counters(&self) -> &CoreCounters {
        &self.counters
    }

    /// Submits I/O on this core's io_uring on behalf of the current actor; `None` if the core
    /// has no ring (see `StartSequence::io_uring`). Feature `io-uring`, Linux only.
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    pub fn io(&mut self) -> Option<Io<'_>> {
        let owner = self.current;
        self.io.as_mut().map(|ring| Io { ring, owner })
    }

    /// Idle wait: in io_uring if the core has one (an I/O completion ends it early), else a plain
    /// sleep.
    fn sleep(&mut self, d: Duration) {
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        if let Some(ring) = self.io.as_mut() {
            ring.wait(d);
            return;
        }
        thread::sleep(d);
    }

    #[inline]
    fn alloc_record(&mut self, core: u16, len: usize) -> *mut u8 {
        if core == self.core {
            self.counters.bytes_written_to[self.core as usize] += len as u64;
            return self.local.alloc(len);
        }
        let c = core as CoreId;
        if let Some(bytes) = self.counters.bytes_written_to.get_mut(c) {
            *bytes += len as u64;
        }
        match self.outbound.get_mut(c).and_then(Option::as_mut) {
            Some(producer) => {
                assert!(len <= producer.max_record(), "event too large for the ring");
                if !self.dirty.contains(&c) {
                    self.dirty.push(c);
                }
                if self.overflow[c].is_empty() {
                    if let Some(record) = producer.reserve(len) {
                        return record;
                    }
                }
                self.overflow[c].alloc(len)
            }
            // Unknown core (e.g. ActorId::NULL): deliver locally, it will bounce as undelivered.
            None => self.local.alloc(len),
        }
    }

    /// Moves an undelivered record back to its sender, without running the event's destructor.
    unsafe fn bounce(&mut self, record: *const u8) {
        let header = &*(record as *const Header);
        if header.kind != KIND_EVENT || header.src == ActorId::NULL {
            (header.drop_fn)(record.add(HEADER) as *mut u8);
            return;
        }
        let len = header.len as usize;
        let src_core = header.src.core;
        // `record` is never in `self.local` (local records are dispatched from a swapped-out
        // buffer), so growing it here cannot move the source.
        let copy = self.alloc_record(src_core, len);
        ptr::copy_nonoverlapping(record, copy, len);
        // Routed to `src`; the receiving loop reads `kind` to know which id is the target.
        (*(copy as *mut Header)).kind = KIND_UNDELIVERED;
    }

    /// Publishes the events sent so far to other cores now, instead of at the end of the loop
    /// iteration: one Release store per destination core with pending events. Useful before a
    /// long computation in a handler. Events that did not fit in a full ring stay queued and are
    /// retried at the end of the iteration. Local events are unaffected (dispatched in this
    /// iteration anyway).
    #[inline]
    pub fn flush(&mut self) {
        let mut i = 0;
        while i < self.dirty.len() {
            let c = self.dirty[i];
            let producer = self.outbound[c].as_mut().unwrap();
            let overflow = &mut self.overflow[c];
            if !overflow.is_empty() {
                let base = overflow.base();
                let mut offset = 0;
                while offset < overflow.len {
                    let len = unsafe { (base.add(offset) as *const u32).read() } as usize;
                    match producer.reserve(len) {
                        Some(record) => unsafe { ptr::copy_nonoverlapping(base.add(offset), record, len) },
                        None => break,
                    }
                    offset += len;
                }
                overflow.consume_front(offset);
            }
            producer.commit();
            self.counters.publications += 1;
            if overflow.is_empty() {
                self.dirty.swap_remove(i);
            } else {
                i += 1;
            }
        }
    }
}

// ---- Start sequence and engine ---------------------------------------------------------------

struct StartEntry {
    core: CoreId,
    service: bool,
    factory: Box<dyn FnOnce() -> NewActor + Send>,
}

/// The cores and actors to start the engine with (simplx `Engine::StartSequence`).
pub struct StartSequence {
    entries: Vec<StartEntry>,
    per_core: Vec<u32>,
    used: Vec<bool>,
    services: HashMap<TypeId, ActorId>,
    ring_capacity: usize,
    modes: Vec<CoreMode>,
    io_entries: Vec<u32>,
}

impl Default for StartSequence {
    fn default() -> Self {
        Self::new()
    }
}

impl StartSequence {
    /// An empty sequence with 64 KiB rings.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            per_core: Vec::new(),
            used: Vec::new(),
            services: HashMap::new(),
            ring_capacity: 64 * 1024,
            modes: Vec::new(),
            io_entries: Vec::new(),
        }
    }

    /// Size in bytes of each inter-core ring.
    pub fn ring_capacity(mut self, bytes: usize) -> Self {
        self.ring_capacity = bytes;
        self
    }

    /// Runs an event loop on `core` even if no start actor is placed there.
    pub fn add_core(&mut self, core: CoreId) {
        if self.per_core.len() <= core {
            self.per_core.resize(core + 1, 0);
            self.used.resize(core + 1, false);
            self.modes.resize(core + 1, CoreMode::Spin);
            self.io_entries.resize(core + 1, 0);
        }
        self.used[core] = true;
    }

    /// How the event loop of `core` waits when idle (`CoreMode::Spin` by default). Also makes
    /// `core` part of the engine.
    pub fn core_mode(&mut self, core: CoreId, mode: CoreMode) {
        self.add_core(core);
        self.modes[core] = mode;
    }

    /// Gives `core` an io_uring with `entries` submission slots, used by its actors through
    /// `Context::io()`. Feature `io-uring`, Linux only.
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    pub fn io_uring(&mut self, core: CoreId, entries: u32) {
        self.add_core(core);
        self.io_entries[core] = entries.max(1);
    }

    /// Registers an actor built on `core` when the engine starts; returns its id.
    pub fn add_actor<A: Actor, F: FnOnce() -> A + Send + 'static>(&mut self, core: CoreId, factory: F) -> ActorId {
        self.push(core, false, factory)
    }

    /// Same as `add_actor`, and makes the actor reachable through `Context::service::<Tag>()`.
    pub fn add_service<Tag: 'static, A: Actor, F: FnOnce() -> A + Send + 'static>(
        &mut self,
        core: CoreId,
        factory: F,
    ) -> ActorId {
        let id = self.push(core, true, factory);
        assert!(self.services.insert(TypeId::of::<Tag>(), id).is_none(), "service registered twice");
        id
    }

    fn push<A: Actor, F: FnOnce() -> A + Send + 'static>(
        &mut self,
        core: CoreId,
        service: bool,
        factory: F,
    ) -> ActorId {
        self.add_core(core);
        let id = ActorId { core: core as u16, generation: 0, index: self.per_core[core] };
        self.per_core[core] += 1;
        self.entries.push(StartEntry { core, service, factory: Box::new(move || new_cell(factory()).1) });
        id
    }
}

/// A running engine: one pinned event-loop thread per core of the start sequence.
pub struct Engine {
    shared: Arc<PaddedShared>,
    threads: Vec<JoinHandle<()>>,
}

impl Engine {
    /// Starts one event loop per core of the start sequence; returns once every start actor has
    /// been built and started.
    pub fn start(sequence: StartSequence) -> Engine {
        let StartSequence { entries, per_core, used, services, ring_capacity, modes, io_entries } = sequence;
        let n = per_core.len();
        let cores: Vec<CoreId> = (0..n).filter(|&c| used[c]).collect();

        let mut outbound: Vec<Vec<Option<Producer>>> = (0..n).map(|_| (0..n).map(|_| None).collect()).collect();
        let mut inbound: Vec<Vec<Consumer>> = (0..n).map(|_| Vec::new()).collect();
        for &src in &cores {
            for &dst in &cores {
                if src != dst {
                    let (p, c) = bytering::channel(ring_capacity);
                    outbound[src][dst] = Some(p);
                    inbound[dst].push(c);
                }
            }
        }

        let regular = entries.iter().filter(|e| !e.service).count();
        let shared = Arc::new(PaddedShared(Shared {
            shutdown: AtomicBool::new(false),
            live_total: AtomicUsize::new(entries.len()),
            live_regular: AtomicUsize::new(regular),
            services,
            cores: cores.len(),
            cores_done: AtomicUsize::new(0),
            counters: (0..n).map(|_| Padded(Mutex::new(CoreCounters::default()))).collect(),
        }));

        let mut per_core_entries: Vec<Vec<StartEntry>> = (0..n).map(|_| Vec::new()).collect();
        for entry in entries {
            per_core_entries[entry.core].push(entry);
        }

        let started = Arc::new(Barrier::new(cores.len() + 1));
        let mut outbound: Vec<_> = outbound.into_iter().map(Some).collect();
        let mut inbound: Vec<_> = inbound.into_iter().map(Some).collect();
        let mut per_core_entries: Vec<_> = per_core_entries.into_iter().map(Some).collect();
        let threads = cores
            .iter()
            .map(|&core| {
                let loop_state = CoreLoop {
                    core,
                    mode: modes[core],
                    io_entries: io_entries[core],
                    entries: per_core_entries[core].take().unwrap(),
                    inbound: inbound[core].take().unwrap(),
                    outbound: outbound[core].take().unwrap(),
                    shared: shared.clone(),
                    started: started.clone(),
                };
                thread::Builder::new()
                    .name(format!("engine-core-{core}"))
                    .spawn(move || loop_state.run())
                    .expect("failed to spawn engine thread")
            })
            .collect();
        started.wait();
        Engine { shared, threads }
    }

    /// Latest counters of each core, as published by the loops every few thousand iterations
    /// (exact once the engine is shut down).
    pub fn counters(&self) -> Vec<CoreCounters> {
        published_counters(&self.shared.0)
    }

    /// Requests destruction of every actor (regular actors first, then services), waits until
    /// all of them accepted and every event loop exited, and returns the final counters.
    pub fn shutdown(mut self) -> Vec<CoreCounters> {
        self.stop();
        published_counters(&self.shared.0)
    }

    fn stop(&mut self) {
        self.shared.0.shutdown.store(true, Ordering::Release);
        for handle in self.threads.drain(..) {
            handle.join().expect("engine thread panicked");
        }
    }
}

fn published_counters(shared: &Shared) -> Vec<CoreCounters> {
    shared.counters.iter().map(|c| c.0.lock().unwrap().clone()).filter(|c| c.loop_total > 0).collect()
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.stop();
    }
}

// ---- Event loop ------------------------------------------------------------------------------

struct Slot {
    cell: Option<Rc<dyn ActorCell>>,
    actor_ptr: *mut u8, // the RefCell<A> inside `cell`
    actor_type: TypeId,
    generation: u16,
    service: bool,
    auto_destroy: bool,     // destroy once unreferenced
    destroy_deferred: bool, // destroy requested while referenced
    singleton: Option<TypeId>,
    destroy_pending: bool,
    callback_iteration: u64,
    handlers: Vec<(TypeId, Thunk)>,
    undelivered: Vec<(TypeId, Thunk)>,
}

impl Slot {
    fn empty() -> Slot {
        Slot {
            cell: None,
            actor_ptr: ptr::null_mut(),
            actor_type: TypeId::of::<()>(),
            generation: 0,
            service: false,
            auto_destroy: false,
            destroy_deferred: false,
            singleton: None,
            destroy_pending: false,
            callback_iteration: u64::MAX,
            handlers: Vec::new(),
            undelivered: Vec::new(),
        }
    }
}

#[inline]
fn find(table: &[(TypeId, Thunk)], type_id: TypeId) -> Option<Thunk> {
    table.iter().find(|(t, _)| *t == type_id).map(|(_, thunk)| *thunk)
}

struct CoreLoop {
    core: CoreId,
    mode: CoreMode,
    io_entries: u32,
    entries: Vec<StartEntry>,
    inbound: Vec<Consumer>,
    outbound: Vec<Option<Producer>>,
    shared: Arc<PaddedShared>,
    started: Arc<Barrier>,
}

struct Actors {
    slots: Vec<Slot>,
}

impl Actors {
    #[inline]
    fn live(&self, core: u16, id: ActorId) -> Option<&Slot> {
        if id.core != core {
            return None;
        }
        let slot = self.slots.get(id.index as usize)?;
        (slot.cell.is_some() && slot.generation == id.generation).then_some(slot)
    }

    fn install(&mut self, ctx: &mut Context, id: ActorId, new: NewActor, spawn: Spawn) {
        let index = id.index as usize;
        if self.slots.len() <= index {
            self.slots.resize_with(index + 1, Slot::empty);
        }
        ctx.set_cell(id, new.any);
        let cell = new.cell.clone();
        self.slots[index] = Slot {
            cell: Some(new.cell),
            actor_ptr: new.ptr,
            actor_type: new.actor_type,
            generation: id.generation,
            service: spawn.service,
            auto_destroy: spawn.auto_destroy,
            singleton: spawn.singleton,
            ..Slot::empty()
        };
        ctx.current = id;
        cell.on_start(ctx);
        self.apply_effects(ctx);
    }

    /// Applies handler registrations and actor creations requested by the callback that just ran.
    #[inline]
    fn apply_effects(&mut self, ctx: &mut Context) {
        if !ctx.effects {
            return;
        }
        ctx.effects = false;
        let current = ctx.current;
        for r in ctx.registrations.drain(..) {
            let slot = &mut self.slots[r.index as usize];
            assert_eq!(slot.actor_type, r.actor_type, "handler registered for another actor type");
            let table = if r.undelivered { &mut slot.undelivered } else { &mut slot.handlers };
            match table.iter_mut().find(|(t, _)| *t == r.type_id) {
                Some(entry) => entry.1 = r.thunk,
                None => table.push((r.type_id, r.thunk)),
            }
        }
        let new_actors = std::mem::take(&mut ctx.new_actors);
        for (id, new, spawn) in new_actors {
            self.install(ctx, id, new, spawn);
        }
        ctx.current = current;
    }

    #[inline]
    unsafe fn dispatch(&mut self, ctx: &mut Context, record: *mut u8) {
        let header = &*(record as *const Header);
        let payload = record.add(HEADER);
        if header.kind == KIND_EVENT {
            let target = self.live(ctx.core, header.dst).map(|s| (s.actor_ptr, find(&s.handlers, header.type_id)));
            if let Some((actor, Some(thunk))) = target {
                ctx.current = header.dst;
                ctx.counters.events += 1;
                ctx.active = true;
                thunk(actor, ctx, header.src, payload);
                let returned = std::mem::take(&mut ctx.return_to_sender);
                self.apply_effects(ctx);
                if !returned {
                    (header.drop_fn)(payload);
                    return;
                }
            }
            ctx.bounce(record);
        } else {
            let target = self.live(ctx.core, header.src).map(|s| (s.actor_ptr, find(&s.undelivered, header.type_id)));
            if let Some((actor, Some(thunk))) = target {
                ctx.current = header.src;
                ctx.counters.events += 1;
                ctx.active = true;
                thunk(actor, ctx, header.dst, payload);
                self.apply_effects(ctx);
            }
            (header.drop_fn)(payload);
        }
    }

    fn run_callbacks(&mut self, ctx: &mut Context, iteration: u64) {
        let callbacks = std::mem::take(&mut ctx.callbacks);
        for id in &callbacks {
            match self.live(ctx.core, *id) {
                Some(slot) if slot.callback_iteration != iteration => {}
                _ => continue, // dead, or already called back in this iteration
            }
            let slot = &mut self.slots[id.index as usize];
            slot.callback_iteration = iteration;
            let cell = slot.cell.clone().unwrap();
            ctx.current = *id;
            ctx.counters.callbacks += 1;
            ctx.active = true;
            cell.on_callback(ctx);
            self.apply_effects(ctx);
        }
        // Keep the allocation for the next iteration.
        let mut callbacks = callbacks;
        callbacks.clear();
        if ctx.callbacks.is_empty() {
            ctx.callbacks = callbacks;
        }
    }

    #[inline]
    fn run_destroy_requests(&mut self, ctx: &mut Context, shared: &Shared) {
        if shared.shutdown.load(Ordering::Relaxed) {
            let services_too = shared.live_regular.load(Ordering::Acquire) == 0;
            for (index, slot) in self.slots.iter_mut().enumerate() {
                if slot.cell.is_some() && !slot.destroy_pending && (!slot.service || services_too) {
                    slot.destroy_pending = true;
                    ctx.destroy_requests.push(ActorId {
                        core: ctx.core,
                        generation: slot.generation,
                        index: index as u32,
                    });
                }
            }
        }
        // Actors whose last reference was dropped: destroy them if they asked for it. Tested with
        // a plain flag: this runs on every loop iteration.
        if ctx.refs.pending.get() {
            ctx.refs.pending.set(false);
            let unreferenced = std::mem::take(&mut ctx.refs.table.borrow_mut().unreferenced);
            for index in unreferenced {
                if let Some(slot) = self.slots.get(index as usize) {
                    if slot.cell.is_some() && (slot.auto_destroy || slot.destroy_deferred) {
                        ctx.destroy_requests.push(ActorId { core: ctx.core, generation: slot.generation, index });
                    }
                }
            }
        }
        if ctx.destroy_requests.is_empty() {
            return;
        }
        let requests = std::mem::take(&mut ctx.destroy_requests);
        for id in requests {
            if self.live(ctx.core, id).is_none() {
                continue;
            }
            let index = id.index as usize;
            self.slots[index].destroy_pending = false;
            if ctx.refs.table.borrow().is_referenced(id.index) {
                self.slots[index].destroy_deferred = true; // retried once unreferenced
                continue;
            }
            ctx.current = id;
            let cell = self.slots[index].cell.clone().unwrap();
            ctx.active = true;
            let accepted = cell.on_destroy_request(ctx);
            self.apply_effects(ctx);
            if accepted {
                let slot = std::mem::replace(&mut self.slots[index], Slot::empty());
                ctx.cells[index] = None;
                if let Some(t) = slot.singleton {
                    ctx.singletons.remove(&t);
                }
                ctx.free_slots.push((id.index, slot.generation.wrapping_add(1)));
                if !slot.service {
                    shared.live_regular.fetch_sub(1, Ordering::AcqRel);
                }
                shared.live_total.fetch_sub(1, Ordering::AcqRel);
                // Dropping the actor last: its own references and timers release here, which
                // may unreference other actors (handled on the next iteration).
                drop(cell);
                drop(slot);
            }
        }
    }
}

impl CoreLoop {
    fn run(self) {
        let CoreLoop { core, mode, io_entries, entries, mut inbound, outbound, shared, started } = self;
        #[cfg(not(all(feature = "io-uring", target_os = "linux")))]
        let _ = io_entries;
        if let Err(e) = affinity::pin_current_thread(core) {
            eprintln!("warning: could not pin engine thread to core {core}: {e}");
        }
        if mode != CoreMode::Spin {
            sys::minimize_timer_slack();
        }
        let n = outbound.len();
        let mut ctx = Context {
            core: core as u16,
            current: ActorId::NULL,
            local: RecordBuf::default(),
            outbound,
            overflow: (0..n).map(|_| RecordBuf::default()).collect(),
            dirty: Vec::with_capacity(n),
            shared,
            effects: false,
            return_to_sender: false,
            registrations: Vec::new(),
            new_actors: Vec::new(),
            destroy_requests: Vec::new(),
            callbacks: Vec::new(),
            free_slots: Vec::new(),
            next_index: entries.len() as u32,
            cells: Vec::new(),
            refs: Rc::new(Refs::default()),
            singletons: HashMap::new(),
            counters: CoreCounters { core, bytes_written_to: vec![0; n], ..CoreCounters::default() },
            active: false,
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            io: (io_entries > 0).then(|| {
                IoRing::new(io_entries).unwrap_or_else(|e| panic!("io_uring setup failed on core {core}: {e}"))
            }),
        };
        let mut actors = Actors { slots: Vec::new() };

        // Start actors are built and started on their own core, as in simplx.
        for (index, entry) in entries.into_iter().enumerate() {
            let id = ActorId { core: core as u16, generation: 0, index: index as u32 };
            let new = (entry.factory)();
            actors.install(&mut ctx, id, new, Spawn { service: entry.service, ..Spawn::default() });
        }
        ctx.flush();
        started.wait();

        let shared = ctx.shared.clone(); // read-only from here on; cloning per iteration would bounce the refcount line
        let mut local = RecordBuf::default();
        let mut iteration: u64 = 0;
        let mut idle_iterations: u32 = 0;
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        let mut io_done: Vec<(ActorId, crate::io::IoCompletion)> = Vec::new();
        loop {
            let events_before = ctx.counters.events;
            // I/O completions become local events, dispatched in this iteration.
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            if let Some(ring) = ctx.io.as_mut() {
                ring.submit();
                ring.drain(|owner, completion| io_done.push((owner, completion)));
                for (owner, completion) in io_done.drain(..) {
                    ctx.send_from(ActorId::NULL, owner, completion);
                }
            }
            for consumer in inbound.iter_mut() {
                let mut any = false;
                while let Some((record, len)) = consumer.peek() {
                    unsafe { actors.dispatch(&mut ctx, record) };
                    consumer.advance(len);
                    any = true;
                }
                if any {
                    consumer.release();
                }
            }
            while !ctx.local.is_empty() {
                std::mem::swap(&mut local, &mut ctx.local);
                let base = local.base();
                let mut offset = 0;
                while offset < local.len {
                    let record = unsafe { base.add(offset) };
                    let len = unsafe { (record as *const u32).read() } as usize;
                    unsafe { actors.dispatch(&mut ctx, record) };
                    offset += len;
                }
                local.len = 0;
            }
            if !ctx.callbacks.is_empty() {
                actors.run_callbacks(&mut ctx, iteration);
            }
            actors.run_destroy_requests(&mut ctx, &shared.0);
            ctx.flush();
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            if let Some(ring) = ctx.io.as_mut() {
                ring.submit();
            }
            iteration += 1;
            ctx.counters.loop_total += 1;
            ctx.counters.loop_usage += ctx.active as u64;
            ctx.active = false;
            if ctx.counters.loop_total % COUNTERS_PUBLISH_PERIOD == 0 {
                ctx.counters.cpu_ns = sys::thread_cpu_ns();
                // try_lock: never wait for a reader on the hot path, publish next time instead.
                if let Ok(mut published) = shared.0.counters[core].0.try_lock() {
                    published.clone_from(&ctx.counters);
                }
            }
            // Idle-time policy. Everything sent in this iteration is already published above.
            if mode != CoreMode::Spin {
                idle_iterations =
                    if ctx.counters.events == events_before { idle_iterations.saturating_add(1) } else { 0 };
                let sleep = match mode {
                    CoreMode::Sleep(d) if idle_iterations > 0 => Some(d),
                    CoreMode::Adaptive { idle_loops, sleep } if idle_iterations > idle_loops => Some(sleep),
                    _ => None,
                };
                if let Some(d) = sleep {
                    if ctx.local.is_empty() {
                        let start = Instant::now();
                        ctx.sleep(d);
                        ctx.counters.sleeps += 1;
                        ctx.counters.slept_ns += start.elapsed().as_nanos() as u64;
                    }
                }
            }
            if shared.0.live_total.load(Ordering::Acquire) == 0 && ctx.local.is_empty() {
                break;
            }
        }
        ctx.counters.cpu_ns = sys::thread_cpu_ns();
        shared.0.counters[core].0.lock().unwrap().clone_from(&ctx.counters);

        // No actor left anywhere, so nothing can handle an event anymore: drop whatever arrives,
        // push out our remaining overflow, and keep draining until every core did the same.
        let drop_inbound = |inbound: &mut Vec<Consumer>| {
            for consumer in inbound.iter_mut() {
                while let Some((record, len)) = consumer.peek() {
                    let header = unsafe { &*(record as *const Header) };
                    unsafe { (header.drop_fn)(record.add(HEADER)) };
                    consumer.advance(len);
                }
                consumer.release();
            }
        };
        while !ctx.dirty.is_empty() {
            drop_inbound(&mut inbound);
            ctx.flush();
        }
        shared.0.cores_done.fetch_add(1, Ordering::AcqRel);
        while shared.0.cores_done.load(Ordering::Acquire) < shared.0.cores {
            drop_inbound(&mut inbound);
            std::hint::spin_loop();
        }
        drop_inbound(&mut inbound);
    }
}
