use std::collections::{HashMap, HashSet};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::sync::lock;
use crate::types::{PageId, TxnId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    IS,
    IX,
    S,
    SIX,
    X,
}

use LockMode::*;

impl LockMode {
    pub fn compatible(self, other: LockMode) -> bool {
        matches!(
            (self, other),
            (IS, IS) | (IS, IX) | (IS, S) | (IS, SIX) | (IX, IS) | (IX, IX) | (S, IS) | (S, S) | (SIX, IS)
        )
    }

    // weakest mode that grants everything both modes do
    pub fn sup(self, other: LockMode) -> LockMode {
        match (self, other) {
            (a, b) if a == b => a,
            (X, _) | (_, X) => X,
            (SIX, _) | (_, SIX) => SIX,
            (IS, m) | (m, IS) => m,
            // the only pair left is S + IX
            _ => SIX,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Resource {
    Table(PageId),
    Row(PageId, Vec<u8>),
}

struct Request {
    txn: TxnId,
    held: Option<LockMode>,
    want: LockMode,
}

impl Request {
    fn waiting(&self) -> bool {
        self.held != Some(self.want)
    }
}

#[derive(Default)]
struct Queue {
    reqs: Vec<Request>,
}

#[derive(Default)]
struct Table {
    queues: HashMap<Resource, Queue>,
    held: HashMap<TxnId, Vec<Resource>>,
    waiting_on: HashMap<TxnId, Resource>,
}

// Strict 2PL with intention locks. FIFO queues so writers aren't starved by a
// stream of readers; upgrades jump the line. Deadlocks are found by walking
// the waits-for graph every time a request would block, and the transaction
// whose request closes the cycle is the victim.
pub struct LockManager {
    t: Mutex<Table>,
    cv: Condvar,
    timeout: Duration,
}

impl Table {
    fn grantable(&self, res: &Resource, txn: TxnId) -> bool {
        let q = &self.queues[res];
        let pos = q.reqs.iter().position(|r| r.txn == txn).unwrap();
        let me = &q.reqs[pos];
        let upgrading = me.held.is_some();
        q.reqs.iter().enumerate().all(|(i, r)| {
            if r.txn == txn {
                return true;
            }
            if r.held.is_some_and(|h| !me.want.compatible(h)) {
                return false;
            }
            // don't jump a waiter that's ahead of us and wouldn't coexist with us
            upgrading || i > pos || !r.waiting() || me.want.compatible(r.want)
        })
    }

    fn blockers(&self, txn: TxnId) -> Vec<TxnId> {
        let Some(res) = self.waiting_on.get(&txn) else { return vec![] };
        let q = &self.queues[res];
        let pos = q.reqs.iter().position(|r| r.txn == txn).unwrap();
        let me = &q.reqs[pos];
        let mut out = Vec::new();
        for (i, r) in q.reqs.iter().enumerate() {
            if r.txn == txn {
                continue;
            }
            let by_holder = r.held.is_some_and(|h| !me.want.compatible(h));
            let by_queue = me.held.is_none() && i < pos && r.waiting() && !me.want.compatible(r.want);
            if by_holder || by_queue {
                out.push(r.txn);
            }
        }
        out
    }

    fn deadlocked(&self, start: TxnId) -> bool {
        let mut seen = HashSet::new();
        let mut stack = self.blockers(start);
        while let Some(t) = stack.pop() {
            if t == start {
                return true;
            }
            if seen.insert(t) {
                stack.extend(self.blockers(t));
            }
        }
        false
    }

    fn give_up(&mut self, res: &Resource, txn: TxnId) {
        self.waiting_on.remove(&txn);
        let q = self.queues.get_mut(res).unwrap();
        let pos = q.reqs.iter().position(|r| r.txn == txn).unwrap();
        if q.reqs[pos].held.is_some() {
            // failed upgrade: keep what we had
            let h = q.reqs[pos].held.unwrap();
            q.reqs[pos].want = h;
        } else {
            q.reqs.remove(pos);
            if q.reqs.is_empty() {
                self.queues.remove(res);
            }
        }
    }
}

impl LockManager {
    pub fn new(timeout: Duration) -> Self {
        LockManager { t: Mutex::new(Table::default()), cv: Condvar::new(), timeout }
    }

    pub fn lock(&self, txn: TxnId, res: Resource, mode: LockMode) -> Result<()> {
        let mut t = lock(&self.t);
        let q = t.queues.entry(res.clone()).or_default();
        match q.reqs.iter_mut().find(|r| r.txn == txn) {
            Some(r) => {
                let target = r.held.map_or(mode, |h| h.sup(mode));
                if r.held == Some(target) {
                    return Ok(());
                }
                r.want = target;
            }
            None => q.reqs.push(Request { txn, held: None, want: mode }),
        }

        let deadline = Instant::now() + self.timeout;
        loop {
            if t.grantable(&res, txn) {
                let q = t.queues.get_mut(&res).unwrap();
                let r = q.reqs.iter_mut().find(|r| r.txn == txn).unwrap();
                let first = r.held.is_none();
                r.held = Some(r.want);
                t.waiting_on.remove(&txn);
                if first {
                    t.held.entry(txn).or_default().push(res);
                }
                return Ok(());
            }
            t.waiting_on.insert(txn, res.clone());
            if t.deadlocked(txn) {
                t.give_up(&res, txn);
                self.cv.notify_all();
                return Err(Error::Deadlock);
            }
            let now = Instant::now();
            if now >= deadline {
                t.give_up(&res, txn);
                self.cv.notify_all();
                return Err(Error::LockTimeout);
            }
            // short slices: a wakeup can race with the graph changing under us
            let slice = (deadline - now).min(Duration::from_millis(50));
            t = self.cv.wait_timeout(t, slice).unwrap_or_else(|e| e.into_inner()).0;
        }
    }

    pub fn release_all(&self, txn: TxnId) {
        let mut t = lock(&self.t);
        t.waiting_on.remove(&txn);
        for res in t.held.remove(&txn).unwrap_or_default() {
            if let Some(q) = t.queues.get_mut(&res) {
                q.reqs.retain(|r| r.txn != txn);
                if q.reqs.is_empty() {
                    t.queues.remove(&res);
                }
            }
        }
        self.cv.notify_all();
    }

    pub fn held_by(&self, txn: TxnId) -> usize {
        lock(&self.t).held.get(&txn).map_or(0, |v| v.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn row(k: &str) -> Resource {
        Resource::Row(1, k.as_bytes().to_vec())
    }

    fn lm() -> Arc<LockManager> {
        Arc::new(LockManager::new(Duration::from_secs(5)))
    }

    #[test]
    fn matrix_is_symmetric() {
        let all = [IS, IX, S, SIX, X];
        for a in all {
            for b in all {
                assert_eq!(a.compatible(b), b.compatible(a), "{a:?} {b:?}");
            }
        }
        assert!(!X.compatible(IS));
        assert!(S.compatible(S));
        assert!(!S.compatible(IX));
    }

    #[test]
    fn sup_is_an_upper_bound() {
        let all = [IS, IX, S, SIX, X];
        for a in all {
            for b in all {
                let s = a.sup(b);
                for c in all {
                    // anything incompatible with a or b must be incompatible with the sup
                    if !a.compatible(c) || !b.compatible(c) {
                        assert!(!s.compatible(c), "{a:?}+{b:?}={s:?} vs {c:?}");
                    }
                }
            }
        }
        assert_eq!(S.sup(IX), SIX);
        assert_eq!(IS.sup(X), X);
    }

    #[test]
    fn shared_locks_coexist_exclusive_waits() {
        let m = lm();
        m.lock(1, row("a"), S).unwrap();
        m.lock(2, row("a"), S).unwrap();

        let m2 = m.clone();
        let h = thread::spawn(move || {
            m2.lock(3, row("a"), X).unwrap();
            m2.release_all(3);
        });
        thread::sleep(Duration::from_millis(80));
        assert_eq!(m.held_by(3), 0);
        m.release_all(1);
        m.release_all(2);
        h.join().unwrap();
    }

    #[test]
    fn relock_and_upgrade_without_contention() {
        let m = lm();
        m.lock(1, row("a"), S).unwrap();
        m.lock(1, row("a"), S).unwrap();
        m.lock(1, row("a"), X).unwrap();
        m.lock(1, row("a"), S).unwrap();
        assert_eq!(m.held_by(1), 1);
        m.release_all(1);
        m.lock(2, row("a"), X).unwrap();
    }

    #[test]
    fn two_way_deadlock_picks_the_closer() {
        let m = lm();
        m.lock(1, row("a"), X).unwrap();
        m.lock(2, row("b"), X).unwrap();

        let m1 = m.clone();
        let h = thread::spawn(move || m1.lock(1, row("b"), X));
        thread::sleep(Duration::from_millis(100));
        // 1 is waiting on 2, so 2 asking for a closes the loop
        assert!(matches!(m.lock(2, row("a"), X), Err(Error::Deadlock)));
        m.release_all(2);
        h.join().unwrap().unwrap();
        m.release_all(1);
    }

    #[test]
    fn upgrade_deadlock_is_detected() {
        let m = lm();
        m.lock(1, row("a"), S).unwrap();
        m.lock(2, row("a"), S).unwrap();
        let m1 = m.clone();
        let h = thread::spawn(move || m1.lock(1, row("a"), X));
        thread::sleep(Duration::from_millis(100));
        assert!(matches!(m.lock(2, row("a"), X), Err(Error::Deadlock)));
        m.release_all(2);
        h.join().unwrap().unwrap();
    }

    #[test]
    fn intention_locks_vs_table_scan() {
        let m = lm();
        let t = Resource::Table(1);
        m.lock(1, t.clone(), IX).unwrap();
        m.lock(2, t.clone(), IX).unwrap();
        m.lock(3, t.clone(), IS).unwrap();

        let m2 = m.clone();
        let t2 = t.clone();
        let h = thread::spawn(move || {
            m2.lock(4, t2, S).unwrap();
        });
        thread::sleep(Duration::from_millis(80));
        assert_eq!(m.held_by(4), 0);
        m.release_all(1);
        m.release_all(2);
        h.join().unwrap();
    }

    #[test]
    fn timeout_cleans_up_after_itself() {
        let m = Arc::new(LockManager::new(Duration::from_millis(150)));
        m.lock(1, row("a"), X).unwrap();
        assert!(matches!(m.lock(2, row("a"), S), Err(Error::LockTimeout)));
        assert_eq!(m.held_by(2), 0);
        m.release_all(1);
        m.lock(3, row("a"), X).unwrap();
    }
}
