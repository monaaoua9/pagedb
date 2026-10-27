mod common;

use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::ops::Bound::Unbounded;

use common::TempDir;
use pagedb::{Database, Options};

const PAGE: u64 = 8192;

fn open_with(dir: &TempDir, pool: usize) -> Database {
    Database::open(dir.path(), Options { pool_pages: pool, ..Options::default() }).unwrap()
}

fn open(dir: &TempDir) -> Database {
    open_with(dir, 256)
}

fn rows(db: &Database, table: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let t = db.table(table).unwrap();
    let mut tx = db.begin();
    let r = tx.scan(&t, Unbounded, Unbounded).unwrap();
    tx.commit().unwrap();
    r
}

fn k(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

#[test]
fn committed_work_survives_a_crash_with_no_page_flushed() {
    let dir = TempDir::new("rec-commit");
    {
        let db = open(&dir);
        let t = db.create_table("t").unwrap();
        for batch in 0..10u32 {
            let mut tx = db.begin();
            for i in 0..100 {
                tx.insert(&t, &k(batch * 100 + i), format!("v{i}").as_bytes()).unwrap();
            }
            tx.commit().unwrap();
        }
        db.simulate_crash();
    }
    let db = open(&dir);
    assert_eq!(rows(&db, "t").len(), 1000);
    assert!(db.recovery_report().redone > 0, "{:?}", db.recovery_report());
    db.verify().unwrap();
}

#[test]
fn uncommitted_work_is_rolled_back() {
    let dir = TempDir::new("rec-loser");
    let loser;
    {
        let db = open(&dir);
        let t = db.create_table("t").unwrap();
        let mut ok = db.begin();
        for i in 0..200 {
            ok.insert(&t, &k(i), b"committed").unwrap();
        }
        ok.commit().unwrap();

        let mut bad = db.begin();
        for i in 0..200 {
            bad.update(&t, &k(i), b"dirty").unwrap_or_default();
        }
        for i in 1000..1400 {
            bad.insert(&t, &k(i), &[7u8; 200]).unwrap();
        }
        // a later commit flushes the log, dragging the loser's records to disk with it
        let mut other = db.begin();
        other.insert(&t, &k(5000), b"other").unwrap();
        other.commit().unwrap();

        db.simulate_crash();
        loser = bad;
    }
    drop(loser);
    let db = open(&dir);
    let r = rows(&db, "t");
    assert_eq!(r.len(), 201);
    assert!(r.iter().take(200).all(|(_, v)| v == b"committed"));
    assert_eq!(db.recovery_report().rolled_back, 1);
    db.verify().unwrap();
}

#[test]
fn stolen_pages_of_a_loser_are_undone() {
    // tiny pool: the uncommitted transaction's dirty pages are evicted to disk
    // long before anything commits, so undo has real work to do on disk state
    let dir = TempDir::new("rec-steal");
    let loser;
    {
        let db = open_with(&dir, 24);
        let t = db.create_table("t").unwrap();
        let mut ok = db.begin();
        for i in 0..300 {
            ok.insert(&t, &k(i), &[1u8; 100]).unwrap();
        }
        ok.commit().unwrap();

        let mut bad = db.begin();
        for i in 300..3000 {
            bad.insert(&t, &k(i), &[2u8; 300]).unwrap();
        }
        for i in 0..300 {
            bad.delete(&t, &k(i)).unwrap();
        }
        assert!(db.pool_stats().evictions > 50);
        let mut other = db.begin();
        other.insert(&t, &k(9999), b"x").unwrap();
        other.commit().unwrap();
        db.simulate_crash();
        loser = bad;
    }
    drop(loser);
    let db = open_with(&dir, 24);
    let r = rows(&db, "t");
    assert_eq!(r.len(), 301);
    assert!(r[..300].iter().all(|(_, v)| v == &[1u8; 100]));
    db.verify().unwrap();
}

#[test]
fn rolled_back_work_stays_rolled_back_after_a_crash() {
    let dir = TempDir::new("rec-rb");
    {
        let db = open(&dir);
        let t = db.create_table("t").unwrap();
        let mut a = db.begin();
        for i in 0..400 {
            a.insert(&t, &k(i), &[3u8; 80]).unwrap();
        }
        a.rollback().unwrap();
        let mut b = db.begin();
        b.insert(&t, &k(1), b"kept").unwrap();
        b.commit().unwrap();
        db.simulate_crash();
    }
    let db = open(&dir);
    assert_eq!(rows(&db, "t"), vec![(k(1).to_vec(), b"kept".to_vec())]);
    db.verify().unwrap();
}

#[test]
fn recovery_from_a_checkpoint_keeps_pre_and_post_checkpoint_work() {
    let dir = TempDir::new("rec-ckpt");
    {
        let db = open(&dir);
        let t = db.create_table("t").unwrap();
        let mut tx = db.begin();
        for i in 0..500 {
            tx.insert(&t, &k(i), b"before").unwrap();
        }
        tx.commit().unwrap();

        // active across the checkpoint: its record is in the checkpoint's table
        let mut long = db.begin();
        long.insert(&t, &k(7000), b"long").unwrap();

        db.checkpoint().unwrap();

        let mut tx = db.begin();
        for i in 500..800 {
            tx.insert(&t, &k(i), b"after").unwrap();
        }
        tx.commit().unwrap();
        long.insert(&t, &k(7001), b"long").unwrap();
        // not committed; commit of `tx` above flushed its records
        db.simulate_crash();
        std::mem::forget(long);
    }
    let db = open(&dir);
    let r = rows(&db, "t");
    assert_eq!(r.len(), 800);
    assert!(r.iter().all(|(key, _)| key.as_slice() < &k(7000)[..]));
    db.verify().unwrap();
}

#[test]
fn recovering_twice_is_the_same_as_recovering_once() {
    let dir = TempDir::new("rec-twice");
    {
        let db = open(&dir);
        let t = db.create_table("t").unwrap();
        let mut tx = db.begin();
        for i in 0..600 {
            tx.insert(&t, &k(i), &[5u8; 50]).unwrap();
        }
        tx.commit().unwrap();
        let mut bad = db.begin();
        for i in 0..600 {
            bad.delete(&t, &k(i)).unwrap();
        }
        let mut o = db.begin();
        o.insert(&t, &k(9000), b"o").unwrap();
        o.commit().unwrap();
        db.simulate_crash();
        std::mem::forget(bad);
    }
    let first = {
        let db = open(&dir);
        let r = rows(&db, "t");
        db.simulate_crash();
        r
    };
    let db = open(&dir);
    assert_eq!(rows(&db, "t"), first);
    assert_eq!(first.len(), 601);
    db.verify().unwrap();
}

#[test]
fn a_torn_page_is_rebuilt_from_its_full_page_image() {
    let dir = TempDir::new("rec-torn");
    let root;
    {
        let db = open(&dir);
        let t = db.create_table("t").unwrap();
        root = t.root();
        let mut tx = db.begin();
        for i in 0..100 {
            tx.insert(&t, &k(i), b"old").unwrap();
        }
        tx.commit().unwrap();
        // everything is on disk and clean now
        db.checkpoint().unwrap();

        let mut tx = db.begin();
        for i in 0..100 {
            tx.update(&t, &k(i), b"new").unwrap();
        }
        tx.commit().unwrap();
        db.simulate_crash();
    }
    // power cut halfway through writing the page: garbage in the middle
    {
        let mut f = OpenOptions::new().write(true).open(dir.path().join("data.db")).unwrap();
        f.seek(SeekFrom::Start(root as u64 * PAGE + 4000)).unwrap();
        f.write_all(&[0xEE; 128]).unwrap();
    }
    let db = open(&dir);
    let r = rows(&db, "t");
    assert_eq!(r.len(), 100);
    assert!(r.iter().all(|(_, v)| v == b"new"));
    db.verify().unwrap();
}

#[test]
fn a_cut_commit_record_means_the_transaction_never_happened() {
    let dir = TempDir::new("rec-cut");
    {
        let db = open(&dir);
        let t = db.create_table("t").unwrap();
        let mut a = db.begin();
        a.insert(&t, b"a", b"1").unwrap();
        a.commit().unwrap();
        let mut b = db.begin();
        b.insert(&t, b"b", b"2").unwrap();
        b.commit().unwrap();
        db.simulate_crash();
    }
    // last durable frame is b's Commit; shave its tail so the crc can't match
    let wal = dir.path().join("wal.log");
    let len = std::fs::metadata(&wal).unwrap().len();
    OpenOptions::new().write(true).open(&wal).unwrap().set_len(len - 2).unwrap();

    let db = open(&dir);
    assert_eq!(rows(&db, "t"), vec![(b"a".to_vec(), b"1".to_vec())]);
    db.verify().unwrap();
}

#[test]
fn ids_keep_moving_forward_across_restarts() {
    let dir = TempDir::new("rec-ids");
    let first;
    {
        let db = open(&dir);
        let t = db.create_table("t").unwrap();
        let mut tx = db.begin();
        tx.insert(&t, b"k", b"v").unwrap();
        first = tx.id();
        tx.commit().unwrap();
        db.simulate_crash();
    }
    let db = open(&dir);
    assert!(db.begin().id() > first);
}
