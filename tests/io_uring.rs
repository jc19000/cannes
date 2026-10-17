//! io_uring integration (feature `io-uring`, Linux): a TCP echo server driven by completions,
//! wake-up of a sleeping core by I/O, and file reads and writes.
#![cfg(all(feature = "io-uring", target_os = "linux"))]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::{IntoRawFd, RawFd};
use std::sync::{mpsc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use cannes::{Actor, ActorId, Context, CoreMode, Engine, Handler, IoCompletion, StartSequence};

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

const ACCEPT: u64 = 0;
const RECV: u64 = 1;
const SEND: u64 = 2;
const CLOSE: u64 = 3;

fn token(kind: u64, fd: i32) -> u64 {
    kind << 32 | fd as u32 as u64
}

/// Echoes every byte received on every accepted connection, entirely through io_uring.
struct EchoServer {
    listener: RawFd,
}

impl Actor for EchoServer {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<IoCompletion, Self>();
        ctx.io().expect("io_uring on this core").accept(self.listener, token(ACCEPT, self.listener));
    }
}

impl Handler<IoCompletion> for EchoServer {
    fn on_event(&mut self, ctx: &mut Context, _src: ActorId, c: &IoCompletion) {
        let (kind, fd) = (c.token >> 32, c.token as u32 as i32);
        let buf = c.take_buf(); // no copy; keeps its capacity for the next recv
        let mut io = ctx.io().unwrap();
        match kind {
            ACCEPT => {
                if c.result >= 0 {
                    io.recv(c.result, Vec::with_capacity(4096), token(RECV, c.result));
                }
                io.accept(fd, token(ACCEPT, fd));
            }
            RECV if c.result > 0 => io.send(fd, buf.unwrap(), token(SEND, fd)),
            RECV => io.close(fd, token(CLOSE, fd)), // peer closed, or error
            SEND if c.result < 0 => io.close(fd, token(CLOSE, fd)),
            SEND => {
                let mut buf = buf.unwrap();
                buf.drain(..c.result as usize);
                if buf.is_empty() {
                    io.recv(fd, buf, token(RECV, fd)); // buf keeps its 4 KiB capacity
                } else {
                    io.send(fd, buf, token(SEND, fd)); // partial send: the rest
                }
            }
            _ => {}
        }
    }
}

extern "C" {
    fn setsockopt(fd: i32, level: i32, name: i32, value: *const i32, len: u32) -> i32;
}

/// TCP_NODELAY on the listener, inherited by accepted sockets on Linux: without it, Nagle plus
/// delayed ACKs add up to 40 ms whenever an echo is sent in two parts.
fn set_nodelay(fd: RawFd) {
    const IPPROTO_TCP: i32 = 6;
    const TCP_NODELAY: i32 = 1;
    let one = 1i32;
    assert_eq!(unsafe { setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, 4) }, 0);
}

fn start_echo(mode: CoreMode) -> (Engine, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let fd = listener.into_raw_fd();
    set_nodelay(fd);
    let mut seq = StartSequence::new();
    seq.core_mode(1, mode);
    seq.io_uring(1, 256);
    seq.add_actor(1, move || EchoServer { listener: fd });
    (Engine::start(seq), addr)
}

#[test]
fn tcp_echo_server_with_concurrent_clients() {
    let _g = serial();
    let (engine, addr) = start_echo(CoreMode::Spin);
    let clients: Vec<_> = (0..4)
        .map(|c| {
            std::thread::spawn(move || {
                let mut s = TcpStream::connect(addr).unwrap();
                s.set_nodelay(true).unwrap();
                s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                for i in 0..200usize {
                    let msg: Vec<u8> = (0..(1 + (i * 37 + c) % 3000)).map(|b| (b + i + c) as u8).collect();
                    s.write_all(&msg).unwrap();
                    let mut back = vec![0; msg.len()];
                    s.read_exact(&mut back).unwrap();
                    assert_eq!(back, msg, "client {c} message {i}");
                }
            })
        })
        .collect();
    for c in clients {
        c.join().unwrap();
    }
    let counters = engine.shutdown();
    assert!(counters[0].events >= 4 * 200 * 2, "{:?}", counters[0]);
}

#[test]
fn io_completion_wakes_a_sleeping_core() {
    let _g = serial();
    // The core sleeps 500 ms at a time when idle; I/O must cut the sleep short.
    let (engine, addr) = start_echo(CoreMode::Sleep(Duration::from_millis(500)));
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_nodelay(true).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    s.write_all(b"warm-up").unwrap();
    let mut back = [0u8; 7];
    s.read_exact(&mut back).unwrap();
    std::thread::sleep(Duration::from_millis(100)); // let the core fall asleep again
    let mut worst = Duration::ZERO;
    for _ in 0..10 {
        let t = Instant::now();
        s.write_all(b"ping").unwrap();
        let mut back = [0u8; 4];
        s.read_exact(&mut back).unwrap();
        assert_eq!(&back, b"ping");
        worst = worst.max(t.elapsed());
        std::thread::sleep(Duration::from_millis(20));
    }
    let counters = engine.shutdown();
    assert!(counters[0].sleeps > 0);
    assert!(worst < Duration::from_millis(100), "round trip {worst:?} despite a 500 ms sleep: not woken by I/O");
}

/// Writes a file through io_uring, reads it back, reports the content.
struct FileActor {
    fd: RawFd,
    report: mpsc::Sender<Vec<u8>>,
}

impl Actor for FileActor {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<IoCompletion, Self>();
        ctx.io().unwrap().write(self.fd, b"hello from io_uring".to_vec(), 0, SEND);
    }
}

impl Handler<IoCompletion> for FileActor {
    fn on_event(&mut self, ctx: &mut Context, _src: ActorId, c: &IoCompletion) {
        assert!(c.result >= 0, "io error {}", c.result);
        match c.token {
            SEND => ctx.io().unwrap().read(self.fd, Vec::with_capacity(64), 6, RECV),
            RECV => {
                let _ = self.report.send(c.take_buf().unwrap());
                ctx.io().unwrap().close(self.fd, CLOSE);
            }
            _ => {}
        }
    }
}

#[test]
fn file_write_then_read_at_offset() {
    let _g = serial();
    let path = std::env::temp_dir().join(format!("cannes-io-{}", std::process::id()));
    let fd = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap()
        .into_raw_fd();
    let (tx, rx) = mpsc::channel();
    let mut seq = StartSequence::new();
    seq.io_uring(1, 8);
    seq.add_actor(1, move || FileActor { fd, report: tx });
    let engine = Engine::start(seq);
    let read = rx.recv_timeout(Duration::from_secs(10)).expect("no read completion");
    engine.shutdown();
    let _ = std::fs::remove_file(&path);
    assert_eq!(read, b"from io_uring");
}
