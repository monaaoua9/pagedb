//! Crash injection for tests. Every durable I/O call asks `hit()` first; once the
//! armed countdown reaches zero that call and every later one on this thread fails,
//! which is what the process dying at that exact point looks like from the files.

use std::cell::Cell;
use std::io;

use crate::error::{Error, Result};

thread_local! {
    static LEFT: Cell<Option<u64>> = const { Cell::new(None) };
    static DEAD: Cell<bool> = const { Cell::new(false) };
    static TORN: Cell<bool> = const { Cell::new(false) };
    static EVENTS: Cell<u64> = const { Cell::new(0) };
}

// fail the (after+1)th I/O event. with `torn`, writes that hit the failure land half-written
pub fn arm(after: u64, torn: bool) {
    LEFT.with(|c| c.set(Some(after)));
    DEAD.with(|c| c.set(false));
    TORN.with(|c| c.set(torn));
    EVENTS.with(|c| c.set(0));
}

// never fails, just counts, so a test can learn how many events a workload has
pub fn count_only() {
    arm(u64::MAX, false);
}

pub fn disarm() {
    LEFT.with(|c| c.set(None));
    DEAD.with(|c| c.set(false));
    TORN.with(|c| c.set(false));
}

pub fn events() -> u64 {
    EVENTS.with(|c| c.get())
}

pub fn torn() -> bool {
    TORN.with(|c| c.get())
}

pub fn hit() -> Result<()> {
    if DEAD.with(|c| c.get()) {
        return Err(Error::Io(io::Error::other("injected crash")));
    }
    EVENTS.with(|c| c.set(c.get() + 1));
    if let Some(n) = LEFT.with(|c| c.get()) {
        if n == 0 {
            DEAD.with(|c| c.set(true));
            return Err(Error::Io(io::Error::other("injected crash")));
        }
        LEFT.with(|c| c.set(Some(n - 1)));
    }
    Ok(())
}
