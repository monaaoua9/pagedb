mod common;

use std::ops::Bound::Unbounded;
use std::sync::atomic::{AtomicBool, Ordering::*};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use common::{Rng, TempDir};
use pagedb::{Database, Error, Options};

fn open(dir: &TempDir) -> Database {
    Database::open(dir.path(), Options::default()).unwrap()
}

fn num(b: &[u8]) -> i64 {
    i64::from_le_bytes(b.try_into().unwrap())
}

#[test]
fn crud_roundtrip() {
    let dir = TempDir::new("crud");
    let db = open(&dir);
    let t = db.create_table("users").unwrap();
    assert!(matches!(db.create_table("users"), Err(Error::TableExists(_))));
    assert!(matches!(db.table("nope"), Err(Error::NoSuchTable(_))));

    let mut tx = db.begin();
    tx.insert(&t, b"ann", b"1").unwrap();
    tx.insert(&t, b"bob", b"2").unwrap();
    assert!(matches!(tx.insert(&t, b"ann", b"x"), Err(Error::DuplicateKey)));
    assert_eq!(tx.update(&t, b"bob", b"3").unwrap(), b"2");
    assert!(matches!(tx.update(&t, b"zed", b"3"), Err(Error::KeyNotFound)));
    assert_eq!(tx.get(&t, b"bob").unwrap().unwrap(), b"3");
    assert!(tx.delete(&t, b"ann").unwrap());
    assert!(!tx.delete(&t, b"ann").unwrap());
    tx.commit().unwrap();

    let mut tx = db.begin();
    assert_eq!(tx.get(&t, b"ann").unwrap(), None);
    assert_eq!(tx.scan(&t, Unbounded, Unbounded).unwrap(), vec![(b"bob".to_vec(), b"3".to_vec())]);
    tx.commit().unwrap();
}

#[test]
fn rollback_undoes_inserts_updates_and_deletes() {
    let dir = TempDir::new("rollback");
    let db = open(&dir);
    let t = db.create_table("t").unwrap();

    let mut setup = db.begin();
    for i in 0..500u32 {
        setup.insert(&t, &i.to_be_bytes(), b"original").unwrap();
    }
    setup.commit().unwrap();
    let before = db.begin().scan(&t, Unbounded, Unbounded).unwrap();

    let mut tx = db.begin();
    for i in 0..500u32 {
        match i % 3 {
            0 => {
                tx.delete(&t, &i.to_be_bytes()).unwrap();
            }
            1 => {
                tx.update(&t, &i.to_be_bytes(), &[9u8; 700]).unwrap();
            }
            _ => {}
        }
    }
    for i in 1000..2000u32 {
        tx.insert(&t, &i.to_be_bytes(), &[1u8; 300]).unwrap();
    }
    tx.rollback().unwrap();

    let after = db.begin().scan(&t, Unbounded, Unbounded).unwrap();
    assert_eq!(before, after);
    db.verify().unwrap();
}

#[test]
fn dropping_a_transaction_rolls_it_back() {
    let dir = TempDir::new("drop");
    let db = open(&dir);
    let t = db.create_table("t").unwrap();
    {
        let mut tx = db.begin();
        tx.insert(&t, b"k", b"v").unwrap();
    }
    let mut tx = db.begin();
    assert_eq!(tx.get(&t, b"k").unwrap(), None);
    // and the lock is gone
    tx.insert(&t, b"k", b"w").unwrap();
    tx.commit().unwrap();
}

#[test]
fn no_dirty_reads() {
    let dir = TempDir::new("dirty");
    let db = open(&dir);
    let t = db.create_table("t").unwrap();
    let mut s = db.begin();
    s.insert(&t, b"k", b"old").unwrap();
    s.commit().unwrap();

    let mut w = db.begin();
    w.update(&t, b"k", b"new").unwrap();

    let (tx, rx) = mpsc::channel();
    let (db2, t2) = (db.clone(), t.clone());
    let h = std::thread::spawn(move || {
        let mut r = db2.begin();
        let v = r.get(&t2, b"k").unwrap();
        tx.send(v).unwrap();
    });
    // reader must be parked on the row lock
    assert!(rx.recv_timeout(Duration::from_millis(250)).is_err());
    w.rollback().unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap().unwrap(), b"old");
    h.join().unwrap();
}

#[test]
fn scan_blocks_phantoms() {
    let dir = TempDir::new("phantom");
    let db = open(&dir);
    let t = db.create_table("t").unwrap();
    let mut s = db.begin();
    s.insert(&t, b"a", b"1").unwrap();
    s.commit().unwrap();

    let mut reader = db.begin();
    let first = reader.scan(&t, Unbounded, Unbounded).unwrap();

    let (tx, rx) = mpsc::channel();
    let (db2, t2) = (db.clone(), t.clone());
    let h = std::thread::spawn(move || {
        let mut w = db2.begin();
        w.insert(&t2, b"b", b"2").unwrap();
        w.commit().unwrap();
        tx.send(()).unwrap();
    });
    assert!(rx.recv_timeout(Duration::from_millis(250)).is_err());
    assert_eq!(reader.scan(&t, Unbounded, Unbounded).unwrap(), first);
    reader.commit().unwrap();
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
    h.join().unwrap();
}

#[test]
fn deadlock_victim_gets_an_error_and_the_other_proceeds() {
    let dir = TempDir::new("deadlock");
    let db = open(&dir);
    let t = db.create_table("t").unwrap();
    let mut s = db.begin();
    s.insert(&t, b"a", b"0").unwrap();
    s.insert(&t, b"b", b"0").unwrap();
    s.commit().unwrap();

    let mut t1 = db.begin();
    t1.update(&t, b"a", b"1").unwrap();

    let (db2, t2) = (db.clone(), t.clone());
    let (ready_tx, ready_rx) = mpsc::channel();
    let h = std::thread::spawn(move || {
        let mut x = db2.begin();
        x.update(&t2, b"b", b"2").unwrap();
        ready_tx.send(()).unwrap();
        // blocks on t1's row a
        let r = x.update(&t2, b"a", b"2");
        match r {
            Ok(_) => {
                x.commit().unwrap();
                "committed"
            }
            Err(Error::Deadlock) => "victim",
            Err(e) => panic!("{e}"),
        }
    });
    ready_rx.recv().unwrap();
    std::thread::sleep(Duration::from_millis(150));
    // closing the cycle: this request is the victim, the thread's one proceeds
    let r = t1.update(&t, b"b", b"1");
    assert!(matches!(r, Err(Error::Deadlock)), "{r:?}");
    t1.rollback().unwrap();
    assert_eq!(h.join().unwrap(), "committed");

    let mut v = db.begin();
    assert_eq!(v.get(&t, b"a").unwrap().unwrap(), b"2");
    assert_eq!(v.get(&t, b"b").unwrap().unwrap(), b"2");
}

#[test]
fn bank_transfers_keep_the_total_and_auditors_never_see_a_torn_one() {
    let dir = TempDir::new("bank");
    let db = Database::open(dir.path(), Options { pool_pages: 256, ..Options::default() }).unwrap();
    let t = db.create_table("acct").unwrap();
    let n_acct = 40u32;
    let start = 1_000i64;
    let mut s = db.begin();
    for i in 0..n_acct {
        s.insert(&t, &i.to_be_bytes(), &start.to_le_bytes()).unwrap();
    }
    s.commit().unwrap();
    let total = start * n_acct as i64;

    let stop = Arc::new(AtomicBool::new(false));
    let mut workers = vec![];
    for w in 0..6u64 {
        let (db, t) = (db.clone(), t.clone());
        workers.push(std::thread::spawn(move || {
            let mut r = Rng(w * 77 + 5);
            let (mut done, mut retried) = (0, 0);
            while done < 150 {
                let a = r.below(n_acct as u64) as u32;
                let mut b = r.below(n_acct as u64) as u32;
                if a == b {
                    b = (b + 1) % n_acct;
                }
                let amt = r.below(50) as i64;
                let mut tx = db.begin();
                let res = (|| -> pagedb::Result<()> {
                    let x = num(&tx.get(&t, &a.to_be_bytes())?.unwrap());
                    let y = num(&tx.get(&t, &b.to_be_bytes())?.unwrap());
                    tx.put(&t, &a.to_be_bytes(), &(x - amt).to_le_bytes())?;
                    tx.put(&t, &b.to_be_bytes(), &(y + amt).to_le_bytes())?;
                    Ok(())
                })();
                match res {
                    Ok(()) => {
                        tx.commit().unwrap();
                        done += 1;
                    }
                    // read-then-write on the same row upgrades S to X, so this does happen
                    Err(Error::Deadlock) | Err(Error::LockTimeout) => {
                        tx.rollback().unwrap();
                        retried += 1;
                    }
                    Err(e) => panic!("{e}"),
                }
            }
            retried
        }));
    }
    let auditor = {
        let (db, t, stop) = (db.clone(), t.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut audits = 0;
            while !stop.load(Acquire) {
                let mut tx = db.begin();
                match tx.scan(&t, Unbounded, Unbounded) {
                    Ok(rows) => {
                        let sum: i64 = rows.iter().map(|(_, v)| num(v)).sum();
                        assert_eq!(sum, total, "auditor saw a half-applied transfer");
                        audits += 1;
                        tx.commit().unwrap();
                    }
                    Err(Error::Deadlock) | Err(Error::LockTimeout) => tx.rollback().unwrap(),
                    Err(e) => panic!("{e}"),
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            audits
        })
    };
    let retries: i32 = workers.into_iter().map(|h| h.join().unwrap()).sum();
    stop.store(true, Release);
    let audits = auditor.join().unwrap();
    eprintln!("bank: {retries} retries, {audits} audits");

    let mut tx = db.begin();
    let rows = tx.scan(&t, Unbounded, Unbounded).unwrap();
    assert_eq!(rows.len() as u32, n_acct);
    assert_eq!(rows.iter().map(|(_, v)| num(v)).sum::<i64>(), total);
    tx.commit().unwrap();
    db.verify().unwrap();
}

#[test]
fn many_tables_and_reopen() {
    let dir = TempDir::new("reopen");
    {
        let db = open(&dir);
        for n in ["alpha", "beta", "gamma"] {
            let t = db.create_table(n).unwrap();
            let mut tx = db.begin();
            for i in 0..300u32 {
                tx.insert(&t, &i.to_be_bytes(), n.as_bytes()).unwrap();
            }
            tx.commit().unwrap();
        }
    }
    let db = open(&dir);
    assert_eq!(db.table_names(), vec!["alpha", "beta", "gamma"]);
    let mut tx = db.begin();
    for n in ["alpha", "beta", "gamma"] {
        let t = db.table(n).unwrap();
        let rows = tx.scan(&t, Unbounded, Unbounded).unwrap();
        assert_eq!(rows.len(), 300);
        assert!(rows.iter().all(|(_, v)| v == n.as_bytes()));
    }
    // clean shutdown checkpoints, so there's nothing to redo
    assert_eq!(db.recovery_report().redone, 0);
}

#[test]
fn freed_pages_are_reused_and_survive_a_crash() {
    let dir = TempDir::new("freelist");
    let size = |d: &TempDir| std::fs::metadata(d.path().join("data.db")).unwrap().len();
    {
        let db = open(&dir);
        let t = db.create_table("t").unwrap();
        for round in 0..3 {
            let mut tx = db.begin();
            for i in 0..3000u32 {
                tx.insert(&t, &i.to_be_bytes(), &[round as u8; 400]).unwrap();
            }
            tx.commit().unwrap();
            let mut tx = db.begin();
            for i in 0..3000u32 {
                tx.delete(&t, &i.to_be_bytes()).unwrap();
            }
            tx.commit().unwrap();
            if round == 0 {
                db.checkpoint().unwrap();
            }
        }
        db.simulate_crash();
    }
    let db = open(&dir);
    let t = db.table("t").unwrap();
    let grown_before = size(&dir);
    let mut tx = db.begin();
    for i in 0..3000u32 {
        tx.insert(&t, &i.to_be_bytes(), &[9u8; 400]).unwrap();
    }
    tx.commit().unwrap();
    db.checkpoint().unwrap();
    // the refill came out of the persisted free list, not new file space
    assert!(size(&dir) <= grown_before + 8192 * 8, "{} -> {}", grown_before, size(&dir));
    db.verify().unwrap();
}
