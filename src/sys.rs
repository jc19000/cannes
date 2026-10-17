//! Small Linux system calls used by the event loops, without pulling in the libc crate.

#[cfg(target_os = "linux")]
mod imp {
    #[repr(C)]
    struct Timespec {
        tv_sec: i64,
        tv_nsec: i64,
    }

    extern "C" {
        fn prctl(option: i32, arg2: u64, arg3: u64, arg4: u64, arg5: u64) -> i32;
        fn clock_gettime(clock: i32, ts: *mut Timespec) -> i32;
    }

    const PR_SET_TIMERSLACK: i32 = 29;
    const CLOCK_THREAD_CPUTIME_ID: i32 = 3;

    /// By default the kernel may delay a timed sleep by up to 50 µs to batch wake-ups; one
    /// nanosecond makes short sleeps mean what they say.
    pub fn minimize_timer_slack() {
        unsafe { prctl(PR_SET_TIMERSLACK, 1, 0, 0, 0) };
    }

    /// CPU time consumed by the calling thread, in nanoseconds.
    pub fn thread_cpu_ns() -> u64 {
        let mut ts = Timespec { tv_sec: 0, tv_nsec: 0 };
        if unsafe { clock_gettime(CLOCK_THREAD_CPUTIME_ID, &mut ts) } != 0 {
            return 0;
        }
        ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    pub fn minimize_timer_slack() {}

    pub fn thread_cpu_ns() -> u64 {
        0
    }
}

pub use imp::{minimize_timer_slack, thread_cpu_ns};
