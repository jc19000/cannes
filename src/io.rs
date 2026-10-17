//! Per-core io_uring (feature `io-uring`, Linux): actors submit network and file operations
//! through `Context::io()`, and each completion comes back to the submitting actor as an
//! [`IoCompletion`] event, dispatched by the event loop like any other event.
//!
//! - One ring per core, created by the core's own thread (`SINGLE_ISSUER`): no locking.
//! - Operations queued during a loop iteration are submitted with one `io_uring_enter` at the end
//!   of the iteration; completions are read from shared memory at the start of the next one,
//!   without a system call unless the kernel flags pending task work (`COOP_TASKRUN` +
//!   `TASKRUN_FLAG`), so a spinning loop is never interrupted.
//! - A sleeping loop (`CoreMode::Sleep` / `Adaptive`) waits in `io_uring_enter` instead of
//!   `nanosleep`: an I/O completion wakes it up at once.
//! - Buffers are moved into the engine for the duration of the operation and handed back in the
//!   completion, so the kernel never writes into memory the actor could touch or free.

use std::cell::Cell;
use std::fmt;
use std::io;
use std::os::fd::RawFd;
use std::time::{Duration, Instant};

use io_uring::{opcode, squeue, types, IoUring};

use crate::engine::ActorId;

/// Completion of an operation submitted through [`Io`], sent to the actor that submitted it.
/// Register a handler with `ctx.register_handler::<IoCompletion, Self>()`.
pub struct IoCompletion {
    /// The token given when submitting.
    pub token: u64,
    /// The operation's return value: bytes transferred, a new file descriptor for `accept`, or a
    /// negated `errno` (for instance `-104` for `ECONNRESET`).
    pub result: i32,
    buf: Cell<Option<Vec<u8>>>,
}

impl IoCompletion {
    /// Takes back the buffer given to `recv`, `read`, `send` or `write`, without copying (handlers
    /// receive events by reference). For `recv` and `read`, its length has grown by the number of
    /// bytes received; its capacity is unchanged, so it can be reused for the next operation.
    /// Returns `None` for other operations, or if already taken.
    pub fn take_buf(&self) -> Option<Vec<u8>> {
        self.buf.take()
    }
}

impl fmt::Debug for IoCompletion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoCompletion").field("token", &self.token).field("result", &self.result).finish_non_exhaustive()
    }
}

/// A buffer whose received bytes are appended after `base_len` (recv, read).
const FILL: u8 = 1;
/// user_data of cancellation requests issued at shutdown.
const CANCEL: u64 = u64::MAX;

struct Pending {
    owner: ActorId,
    token: u64,
    buf: Option<Vec<u8>>,
    kind: u8,
    base_len: usize,
}

pub(crate) struct IoRing {
    ring: IoUring,
    pending: Vec<Option<Pending>>,
    free: Vec<u32>,
    unsubmitted: usize,
    in_flight: usize,
}

impl IoRing {
    pub(crate) fn new(entries: u32) -> io::Result<IoRing> {
        // Cooperative task work: completions needing task work only raise a flag, checked by the
        // loop, instead of interrupting the busy-polling thread. Fall back on older kernels.
        let ring = IoUring::builder()
            .setup_coop_taskrun()
            .setup_taskrun_flag()
            .setup_single_issuer()
            .build(entries)
            .or_else(|_| IoUring::new(entries))?;
        Ok(IoRing { ring, pending: Vec::new(), free: Vec::new(), unsubmitted: 0, in_flight: 0 })
    }

    fn track(&mut self, pending: Pending) -> u64 {
        self.in_flight += 1;
        match self.free.pop() {
            Some(i) => {
                self.pending[i as usize] = Some(pending);
                i as u64
            }
            None => {
                self.pending.push(Some(pending));
                self.pending.len() as u64 - 1
            }
        }
    }

    fn push(&mut self, entry: squeue::Entry) {
        // SAFETY: every buffer referenced by `entry` is owned by `self.pending` until its
        // completion is read (or leaked at shutdown), so it outlives the kernel's use of it.
        while unsafe { self.ring.submission().push(&entry) }.is_err() {
            // Submission queue full: hand it to the kernel now and retry.
            self.ring.submit().expect("io_uring submit failed");
            self.unsubmitted = 0;
        }
        self.unsubmitted += 1;
    }

    /// Submits queued operations, and runs pending task work if the kernel asked for it.
    #[inline]
    pub(crate) fn submit(&mut self) {
        if self.unsubmitted > 0 || self.ring.submission().taskrun() {
            match self.ring.submit() {
                Ok(_) => self.unsubmitted = 0,
                Err(e) if e.raw_os_error() == Some(16) /* EBUSY: CQ full, drain first */ => {}
                Err(e) => panic!("io_uring submit failed: {e}"),
            }
        }
    }

    /// Submits queued operations and waits until one completes or `timeout` elapses.
    pub(crate) fn wait(&mut self, timeout: Duration) {
        let ts = types::Timespec::new().sec(timeout.as_secs()).nsec(timeout.subsec_nanos());
        let args = types::SubmitArgs::new().timespec(&ts);
        match self.ring.submitter().submit_with_args(1, &args) {
            Ok(_) => self.unsubmitted = 0,
            // ETIME (timeout), EINTR, EBUSY: nothing to do, the loop goes on.
            Err(e) if matches!(e.raw_os_error(), Some(62) | Some(4) | Some(16)) => self.unsubmitted = 0,
            Err(e) => panic!("io_uring wait failed: {e}"),
        }
    }

    /// Hands every available completion to `deliver`, with the actor that submitted it.
    #[inline]
    pub(crate) fn drain(&mut self, mut deliver: impl FnMut(ActorId, IoCompletion)) {
        let IoRing { ring, pending, free, in_flight, .. } = self;
        for cqe in ring.completion() {
            let index = cqe.user_data();
            if index == CANCEL {
                continue;
            }
            let Some(p) = pending[index as usize].take() else { continue };
            free.push(index as u32);
            *in_flight -= 1;
            let result = cqe.result();
            let mut buf = p.buf;
            if let Some(b) = buf.as_mut() {
                if p.kind == FILL && result > 0 {
                    // SAFETY: the kernel wrote `result` bytes into the spare capacity after
                    // `base_len`, which we passed as the operation's buffer.
                    unsafe { b.set_len(p.base_len + result as usize) };
                }
            }
            deliver(p.owner, IoCompletion { token: p.token, result, buf: Cell::new(buf) });
        }
    }

    /// Cancels what is still in flight and waits a little for the kernel to let go of the
    /// buffers; buffers of operations still not completed are leaked rather than freed under the
    /// kernel's feet.
    fn shutdown(&mut self) {
        if self.in_flight == 0 {
            return;
        }
        let in_flight: Vec<u64> =
            (0..self.pending.len() as u64).filter(|&i| self.pending[i as usize].is_some()).collect();
        for user_data in in_flight {
            self.push(opcode::AsyncCancel::new(user_data).build().user_data(CANCEL));
        }
        let deadline = Instant::now() + Duration::from_millis(200);
        while self.in_flight > 0 && Instant::now() < deadline {
            self.wait(Duration::from_millis(10));
            self.drain(|_, _| {});
        }
        for p in self.pending.iter_mut().filter_map(Option::take) {
            std::mem::forget(p.buf);
        }
    }
}

impl Drop for IoRing {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Submits I/O on the current core's ring on behalf of the current actor; obtained with
/// `Context::io()`. Operations are submitted at the end of the loop iteration; each one ends with
/// an [`IoCompletion`] carrying `token`.
pub struct Io<'a> {
    pub(crate) ring: &'a mut IoRing,
    pub(crate) owner: ActorId,
}

impl Io<'_> {
    fn op(&mut self, entry: squeue::Entry, token: u64, buf: Option<Vec<u8>>, kind: u8, base_len: usize) {
        let index = self.ring.track(Pending { owner: self.owner, token, buf, kind, base_len });
        self.ring.push(entry.user_data(index));
    }

    /// Accepts a connection on listening socket `fd`; `result` is the new socket.
    pub fn accept(&mut self, fd: RawFd, token: u64) {
        let entry = opcode::Accept::new(types::Fd(fd), std::ptr::null_mut(), std::ptr::null_mut()).build();
        self.op(entry, token, None, 0, 0);
    }

    /// Receives from socket `fd` into the spare capacity of `buf` (`capacity - len`, which must
    /// not be zero); the received bytes are appended to `buf`. `result == 0`: peer closed.
    pub fn recv(&mut self, fd: RawFd, mut buf: Vec<u8>, token: u64) {
        let base_len = buf.len();
        let spare = buf.capacity() - base_len;
        assert!(spare > 0, "recv needs a buffer with spare capacity");
        // SAFETY: within the allocation; the buffer stays owned by the ring until completion.
        let ptr = unsafe { buf.as_mut_ptr().add(base_len) };
        let entry = opcode::Recv::new(types::Fd(fd), ptr, spare.min(u32::MAX as usize) as u32).build();
        self.op(entry, token, Some(buf), FILL, base_len);
    }

    /// Sends the content of `buf` on socket `fd`; `result` is the number of bytes sent, which can
    /// be less than `buf.len()`.
    pub fn send(&mut self, fd: RawFd, buf: Vec<u8>, token: u64) {
        let entry = opcode::Send::new(types::Fd(fd), buf.as_ptr(), buf.len().min(u32::MAX as usize) as u32).build();
        self.op(entry, token, Some(buf), 0, 0);
    }

    /// Reads from `fd` at `offset` into the spare capacity of `buf`, like `recv`.
    pub fn read(&mut self, fd: RawFd, mut buf: Vec<u8>, offset: u64, token: u64) {
        let base_len = buf.len();
        let spare = buf.capacity() - base_len;
        assert!(spare > 0, "read needs a buffer with spare capacity");
        // SAFETY: as in `recv`.
        let ptr = unsafe { buf.as_mut_ptr().add(base_len) };
        let entry = opcode::Read::new(types::Fd(fd), ptr, spare.min(u32::MAX as usize) as u32).offset(offset).build();
        self.op(entry, token, Some(buf), FILL, base_len);
    }

    /// Writes the content of `buf` to `fd` at `offset`.
    pub fn write(&mut self, fd: RawFd, buf: Vec<u8>, offset: u64, token: u64) {
        let entry = opcode::Write::new(types::Fd(fd), buf.as_ptr(), buf.len().min(u32::MAX as usize) as u32)
            .offset(offset)
            .build();
        self.op(entry, token, Some(buf), 0, 0);
    }

    /// Closes `fd`.
    pub fn close(&mut self, fd: RawFd, token: u64) {
        self.op(opcode::Close::new(types::Fd(fd)).build(), token, None, 0, 0);
    }
}
