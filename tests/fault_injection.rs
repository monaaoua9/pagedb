mod common;

use std::collections::BTreeMap;
use std::ops::Bound::Unbounded;

use common::{Rng, TempDir};
use pagedb::{failpoint, Database, Error, Options};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn opts() -> Options {
    Options { pool_pages: 16, checkpoint_bytes: 64 << 10, ..Options::default() }
}

struct Outcome {
    // everything whose commit returned Ok
    committed: Model,
    // the transaction whose commit was running when the failure hit: it may or may not have made it
    in_doubt: Option<Model>,
}

// A fixed workload; stops at the first I/O error, which is where the "process" died.
fn workload(dir: &TempDir, seed: u64) -> Outcome {
    let mut out = Outcome { committed: Model::new(), in_doubt: None };
    let mut r = Rng(seed | 1);
    let Ok(db) = Database::open(dir.path(), opts()) else { return out };
    let Ok(t) = db.create_table("t") else { return out };
    for step in 0..40 {
        let mut tx = db.begin();
        let mut local = out.committed.clone();
        for _ in 0..r.below(25) {
            let key = format!("k{:03}", r.below(150)).into_bytes();
            let res = if r.below(4) == 0 {
                tx.delete(&t, &key).map(|_| {
                    local.remove(&key);
                })
            } else {
                let n = r.below(500) as usize;
                let v = r.bytes(n);
                tx.put(&t, &key, &v).map(|_| {
                    local.insert(key, v);
                })
            };
            if res.is_err() {
                std::mem::forget(tx);
                return out;
            }
        }
        match r.below(8) {
            0 => {
                if tx.rollback().is_err() {
                    return out;
                }
            }
            _ => {
                out.in_doubt = Some(local.clone());
                if tx.commit().is_err() {
                    return out;
                }
                out.committed = local;
                out.in_doubt = None;
            }
        }
        if step % 9 == 8 && db.checkpoint().is_err() {
            return out;
        }
    }
    // clean exit path: dropping the db checkpoints, which can fail too
    out
}

fn state(db: &Database) -> Option<Model> {
    let t = match db.table("t") {
        Ok(t) => t,
        Err(Error::NoSuchTable(_)) => return None,
        Err(e) => panic!("{e}"),
    };
    let mut tx = db.begin();
    let rows = tx.scan(&t, Unbounded, Unbounded).unwrap();
    tx.commit().unwrap();
    Some(rows.into_iter().collect())
}

fn check(dir: &TempDir, o: &Outcome, ctx: &str) {
    let db = Database::open(dir.path(), opts()).unwrap_or_else(|e| panic!("{ctx}: reopen failed: {e}"));
    db.verify().unwrap_or_else(|e| panic!("{ctx}: structure broken: {e}"));
    let got = state(&db).unwrap_or_default();
    let ok = got == o.committed || o.in_doubt.as_ref().is_some_and(|m| *m == got);
    assert!(ok, "{ctx}: recovered state matches neither committed nor in-doubt model");
}

fn total_events(seed: u64) -> u64 {
    let dir = TempDir::new("fi-count");
    failpoint::count_only();
    workload(&dir, seed);
    let n = failpoint::events();
    failpoint::disarm();
    n
}

fn sweep(seed: u64, torn: bool, stride: u64) {
    let total = total_events(seed);
    assert!(total > 50, "workload does too little I/O to be meaningful ({total})");
    let mut n = 0;
    while n < total {
        let dir = TempDir::new("fi");
        failpoint::arm(n, torn);
        let out = workload(&dir, seed);
        failpoint::disarm();
        check(&dir, &out, &format!("seed {seed} torn {torn} failing event {n}/{total}"));
        n += stride;
    }
}

#[test]
fn crash_at_every_io_event() {
    sweep(1, false, 1);
}

#[test]
fn crash_at_every_io_event_with_torn_writes() {
    sweep(2, true, 1);
}

#[test]
fn another_workload_sampled() {
    sweep(3, true, 3);
    sweep(4, false, 3);
}

// Recovery itself dies at every I/O point of its own run; the next recovery must still converge.
#[test]
fn crash_during_recovery() {
    let seed = 5;
    let total = total_events(seed);
    for first in [total / 4, total / 2, total * 3 / 4] {
        for torn in [false, true] {
            // build the crashed state once, copy it for each recovery-failure point
            let base = TempDir::new("fi-rec-base");
            failpoint::arm(first, torn);
            let out = workload(&base, seed);
            failpoint::disarm();

            failpoint::count_only();
            let probe = TempDir::new("fi-rec-probe");
            copy_dir(base.path(), probe.path());
            let _ = Database::open(probe.path(), opts());
            let rec_events = failpoint::events();
            failpoint::disarm();

            for m in 0..rec_events {
                let dir = TempDir::new("fi-rec");
                copy_dir(base.path(), dir.path());
                failpoint::arm(m, torn);
                let _ = Database::open(dir.path(), opts());
                failpoint::disarm();
                check(
                    &dir,
                    &out,
                    &format!("first {first} torn {torn}, recovery died at event {m}/{rec_events}"),
                );
            }
        }
    }
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
    }
}
