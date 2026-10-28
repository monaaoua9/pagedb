mod common;

use std::collections::BTreeMap;
use std::ops::Bound::Unbounded;
use std::sync::Arc;

use common::{Rng, TempDir};
use pagedb::btree::{BTree, LogMode};
use pagedb::buffer::BufferPool;
use pagedb::storage::DiskManager;
use pagedb::wal::LogManager;
use pagedb::{Database, Options};

const SYS: LogMode<'static> = LogMode::System;

fn tree(tag: &str, frames: usize) -> (TempDir, Arc<BufferPool>, BTree) {
    let dir = TempDir::new(tag);
    let disk = Arc::new(DiskManager::open(&dir.path().join("t.db")).unwrap());
    let pool = Arc::new(BufferPool::new(disk, frames));
    let log = Arc::new(LogManager::open(&dir.path().join("t.log")).unwrap());
    pool.set_flusher(log.clone());
    let t = BTree::create(pool.clone(), log).unwrap();
    (dir, pool, t)
}

fn blob(r: &mut Rng, n: usize) -> Vec<u8> {
    r.bytes(n)
}

#[test]
fn sizes_around_every_boundary_roundtrip() {
    let (_d, pool, t) = tree("ovf-sizes", 128);
    let mut r = Rng(7);
    let mut want = BTreeMap::new();
    // inline limit, one page of payload, a page boundary +-1, multi-page, the cap
    for (i, n) in
        [0, 1, 1500, 1501, 8159, 8160, 8161, 16320, 16321, 100_000, 256 * 1024].into_iter().enumerate()
    {
        let v = blob(&mut r, n);
        t.put(format!("k{i:02}").as_bytes(), &v, &SYS).unwrap();
        want.insert(format!("k{i:02}").into_bytes(), v);
    }
    for (k, v) in &want {
        assert_eq!(&t.get(k).unwrap().unwrap(), v, "key {}", String::from_utf8_lossy(k));
    }
    let scanned: BTreeMap<_, _> = t.scan(Unbounded, Unbounded).unwrap().into_iter().collect();
    assert_eq!(scanned, want);
    let st = t.check().unwrap();
    assert!(st.overflow_pages > 40, "{st:?}");
    assert!(t.put(b"x", &vec![0u8; 256 * 1024 + 1], &SYS).is_err());
    assert_eq!(pool.pinned_count(), 0);
}

#[test]
fn replacing_and_deleting_frees_the_chain() {
    let (_d, pool, t) = tree("ovf-free", 128);
    let mut r = Rng(9);
    let big = blob(&mut r, 60_000);
    t.put(b"a", &big, &SYS).unwrap();
    let before = t.check().unwrap().overflow_pages;
    assert!(before >= 7);

    // big -> small: chain gone
    let old = t.put(b"a", b"tiny", &SYS).unwrap().unwrap();
    assert_eq!(old, big);
    assert_eq!(t.check().unwrap().overflow_pages, 0);
    // small -> big -> bigger
    t.put(b"a", &blob(&mut r, 30_000), &SYS).unwrap();
    let bigger = blob(&mut r, 90_000);
    t.put(b"a", &bigger, &SYS).unwrap();
    assert_eq!(t.get(b"a").unwrap().unwrap(), bigger);
    assert_eq!(t.check().unwrap().overflow_pages, 90_000usize.div_ceil(8160));
    // insert-if-absent must not touch an existing big value
    assert!(!t.insert(b"a", b"nope", &SYS).unwrap());
    assert_eq!(t.get(b"a").unwrap().unwrap(), bigger);

    assert_eq!(t.delete(b"a", &SYS).unwrap().unwrap(), bigger);
    assert_eq!(t.check().unwrap().overflow_pages, 0);
    assert_eq!(pool.pinned_count(), 0);
}

#[test]
fn random_mix_of_small_and_huge_values_matches_a_model() {
    let (_d, pool, t) = tree("ovf-fuzz", 200);
    let mut r = Rng(0xABCDEF);
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for step in 0..3000 {
        let k = format!("key{:03}", r.below(120)).into_bytes();
        match r.below(8) {
            0..=3 => {
                let n = match r.below(4) {
                    0 => r.below(100),
                    1 => 1400 + r.below(300),
                    2 => 2000 + r.below(20_000),
                    _ => r.below(60_000),
                } as usize;
                let v = blob(&mut r, n);
                assert_eq!(t.put(&k, &v, &SYS).unwrap(), model.insert(k, v));
            }
            4..=5 => assert_eq!(t.delete(&k, &SYS).unwrap(), model.remove(&k)),
            _ => assert_eq!(t.get(&k).unwrap().as_ref(), model.get(&k)),
        }
        if step % 500 == 499 {
            let st = t.check().unwrap();
            assert_eq!(st.entries, model.len());
        }
    }
    let all: Vec<_> = t.scan(Unbounded, Unbounded).unwrap();
    assert_eq!(all, model.into_iter().collect::<Vec<_>>());
    assert_eq!(pool.pinned_count(), 0);
}

#[test]
fn rollback_and_crash_recovery_with_huge_values() {
    let dir = TempDir::new("ovf-db");
    let opts = Options { pool_pages: 256, ..Options::default() };
    let mut r = Rng(31);
    let kept = blob(&mut r, 120_000);
    {
        let db = Database::open(dir.path(), opts.clone()).unwrap();
        let t = db.create_table("t").unwrap();
        let mut tx = db.begin();
        tx.insert(&t, b"keep", &kept).unwrap();
        tx.insert(&t, b"small", b"s").unwrap();
        tx.commit().unwrap();

        // rolled back: replace big with big, delete big, add big
        let mut tx = db.begin();
        tx.put(&t, b"keep", &blob(&mut r, 50_000)).unwrap();
        tx.insert(&t, b"new", &blob(&mut r, 70_000)).unwrap();
        tx.delete(&t, b"small").unwrap();
        tx.rollback().unwrap();

        let mut tx = db.begin();
        assert_eq!(tx.get(&t, b"keep").unwrap().unwrap(), kept);
        assert_eq!(tx.get(&t, b"small").unwrap().unwrap(), b"s");
        assert_eq!(tx.get(&t, b"new").unwrap(), None);
        tx.commit().unwrap();

        // in flight at the crash, then a commit that drags its records to disk
        let mut loser = db.begin();
        loser.put(&t, b"keep", &blob(&mut r, 200_000)).unwrap();
        let mut other = db.begin();
        other.insert(&t, b"other", &blob(&mut r, 20_000)).unwrap();
        other.commit().unwrap();
        db.simulate_crash();
        std::mem::forget(loser);
    }
    let db = Database::open(dir.path(), opts).unwrap();
    let t = db.table("t").unwrap();
    let mut tx = db.begin();
    assert_eq!(tx.get(&t, b"keep").unwrap().unwrap(), kept);
    assert_eq!(tx.get(&t, b"other").unwrap().unwrap().len(), 20_000);
    tx.commit().unwrap();
    db.verify().unwrap();
}

#[test]
fn freed_overflow_pages_are_reused() {
    let dir = TempDir::new("ovf-reuse");
    let db = Database::open(dir.path(), Options { pool_pages: 256, ..Options::default() }).unwrap();
    let t = db.create_table("t").unwrap();
    let mut r = Rng(5);
    let v = blob(&mut r, 200_000);
    let size = || std::fs::metadata(dir.path().join("data.db")).unwrap().len();
    let mut after_first = 0;
    for round in 0..6 {
        let mut tx = db.begin();
        tx.put(&t, b"big", &v).unwrap();
        tx.commit().unwrap();
        let mut tx = db.begin();
        tx.delete(&t, b"big").unwrap();
        tx.commit().unwrap();
        db.checkpoint().unwrap();
        if round == 0 {
            after_first = size();
        }
    }
    assert!(size() <= after_first + 8192 * 4, "file kept growing: {after_first} -> {}", size());
}
