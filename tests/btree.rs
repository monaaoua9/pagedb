mod common;

use std::collections::BTreeMap;
use std::ops::Bound::*;
use std::sync::Arc;

use common::{Rng, TempDir};
use pagedb::btree::{BTree, LogMode};
use pagedb::buffer::BufferPool;
use pagedb::storage::DiskManager;
use pagedb::wal::LogManager;

struct Env {
    _dir: TempDir,
    pool: Arc<BufferPool>,
    tree: BTree,
}

fn env(tag: &str, frames: usize) -> Env {
    let dir = TempDir::new(tag);
    let disk = Arc::new(DiskManager::open(&dir.path().join("t.db")).unwrap());
    let pool = Arc::new(BufferPool::new(disk, frames));
    let log = Arc::new(LogManager::open(&dir.path().join("t.log")).unwrap());
    pool.set_flusher(log.clone());
    let tree = BTree::create(pool.clone(), log).unwrap();
    Env { _dir: dir, pool, tree }
}

const SYS: LogMode<'static> = LogMode::System;

fn key(i: u64) -> Vec<u8> {
    format!("key-{i:08}").into_bytes()
}

#[test]
fn ascending_inserts() {
    let e = env("asc", 64);
    for i in 0..20_000 {
        assert!(e.tree.insert(&key(i), &i.to_le_bytes(), &SYS).unwrap());
    }
    let st = e.tree.check().unwrap();
    assert_eq!(st.entries, 20_000);
    assert_eq!(st.height, 2, "{st:?}");
    for i in (0..20_000).step_by(37) {
        assert_eq!(e.tree.get(&key(i)).unwrap().unwrap(), i.to_le_bytes());
    }
    assert_eq!(e.tree.get(b"nope").unwrap(), None);
    assert_eq!(e.pool.pinned_count(), 0);
}

#[test]
fn descending_inserts_and_full_scan_is_sorted() {
    let e = env("desc", 64);
    for i in (0..8_000).rev() {
        e.tree.put(&key(i), b"v", &SYS).unwrap();
    }
    e.tree.check().unwrap();
    let all = e.tree.scan(Unbounded, Unbounded).unwrap();
    assert_eq!(all.len(), 8_000);
    assert!(all.windows(2).all(|w| w[0].0 < w[1].0));
}

#[test]
fn duplicate_insert_is_rejected_and_put_replaces() {
    let e = env("dup", 32);
    assert!(e.tree.insert(b"a", b"1", &SYS).unwrap());
    assert!(!e.tree.insert(b"a", b"2", &SYS).unwrap());
    assert_eq!(e.tree.get(b"a").unwrap().unwrap(), b"1");
    assert_eq!(e.tree.put(b"a", b"3", &SYS).unwrap().unwrap(), b"1");
    assert_eq!(e.tree.get(b"a").unwrap().unwrap(), b"3");
}

#[test]
fn limits_are_enforced() {
    let e = env("limits", 32);
    assert!(e.tree.put(&vec![1u8; 513], b"", &SYS).is_err());
    assert!(e.tree.put(b"k", &vec![1u8; 256 * 1024 + 1], &SYS).is_err());
    // the largest allowed cells still split correctly
    for i in 0..200u32 {
        let mut k = vec![b'k'; 500];
        k.extend_from_slice(&i.to_be_bytes());
        e.tree.put(&k[..512.min(k.len())], &vec![i as u8; 1500], &SYS).unwrap();
    }
    e.tree.check().unwrap();
}

#[test]
fn range_scans_respect_bounds() {
    let e = env("range", 64);
    for i in 0..3_000 {
        e.tree.put(&key(i), b"x", &SYS).unwrap();
    }
    let a = key(100);
    let b = key(200);
    let r = e.tree.scan(Included(&a[..]), Excluded(&b[..])).unwrap();
    assert_eq!(r.len(), 100);
    assert_eq!(r[0].0, a);
    let r = e.tree.scan(Excluded(&a[..]), Included(&b[..])).unwrap();
    assert_eq!(r.len(), 100);
    assert_eq!(r[0].0, key(101));
    assert_eq!(r.last().unwrap().0, b);
    let r = e.tree.scan(Included(b"zzz"), Unbounded).unwrap();
    assert!(r.is_empty());

    let mut seen = 0;
    e.tree
        .scan_with(Unbounded, Unbounded, |_, _| {
            seen += 1;
            seen < 10
        })
        .unwrap();
    assert_eq!(seen, 10);
}

#[test]
fn delete_everything_collapses_the_tree() {
    let e = env("shrink", 64);
    let n = 4_000;
    for i in 0..n {
        e.tree.put(&key(i), &[0u8; 1000], &SYS).unwrap();
    }
    let grown = e.tree.check().unwrap();
    assert!(grown.height >= 3);

    // delete in a scrambled order to hit merges and redistributions on both sides
    let mut order: Vec<u64> = (0..n).collect();
    let mut r = Rng(0x9E3779B97F4A7C15);
    for i in (1..order.len()).rev() {
        order.swap(i, r.below(i as u64 + 1) as usize);
    }
    for (c, &i) in order.iter().enumerate() {
        assert!(e.tree.delete(&key(i), &SYS).unwrap().is_some());
        if c % 1500 == 0 {
            e.tree.check().unwrap();
        }
    }
    let st = e.tree.check().unwrap();
    assert_eq!(st.entries, 0);
    assert_eq!(st.height, 1, "{st:?}");
    assert_eq!(e.tree.delete(&key(5), &SYS).unwrap(), None);
    // and it's still usable
    e.tree.put(b"again", b"1", &SYS).unwrap();
    assert_eq!(e.tree.get(b"again").unwrap().unwrap(), b"1");
}

#[test]
fn matches_a_btreemap_under_random_load() {
    for seed in [1u64, 7, 99] {
        let e = env("fuzz", 160);
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut r = Rng(seed.wrapping_mul(0x2545F4914F6CDD1D) | 1);
        for step in 0..12_000 {
            let k = {
                let len = 1 + r.below(120) as usize;
                // small alphabet so keys collide and share long prefixes
                (0..len).map(|_| b'a' + r.below(4) as u8).collect::<Vec<u8>>()
            };
            match r.below(10) {
                0..=4 => {
                    let n = r.below(400) as usize;
                    let v = r.bytes(n);
                    let old = e.tree.put(&k, &v, &SYS).unwrap();
                    assert_eq!(old, model.insert(k, v));
                }
                5..=7 => {
                    let old = e.tree.delete(&k, &SYS).unwrap();
                    assert_eq!(old, model.remove(&k));
                }
                _ => {
                    assert_eq!(e.tree.get(&k).unwrap().as_ref(), model.get(&k));
                }
            }
            if step % 4_000 == 3_999 {
                let st = e.tree.check().unwrap();
                assert_eq!(st.entries, model.len());
            }
        }
        let all = e.tree.scan(Unbounded, Unbounded).unwrap();
        let expect: Vec<_> = model.into_iter().collect();
        assert_eq!(all, expect);
        assert_eq!(e.pool.pinned_count(), 0);
    }
}

#[test]
fn works_with_a_tiny_pool() {
    // height + a sibling or two is all a single thread should ever need pinned
    let e = env("tiny", 12);
    for i in 0..5_000 {
        e.tree.put(&key(i), &[1u8; 100], &SYS).unwrap();
    }
    for i in 0..5_000 {
        if i % 3 != 0 {
            e.tree.delete(&key(i), &SYS).unwrap();
        }
    }
    let st = e.tree.check().unwrap();
    assert_eq!(st.entries, 1_667);
    assert!(e.pool.stats().evictions > 100);
}

#[test]
fn concurrent_writers_and_scanners() {
    let e = env("conc", 512);
    let tree = e.tree.clone();
    let threads = 8u64;
    let per = 3_000u64;

    let mut hs = vec![];
    for t in 0..threads {
        let tree = tree.clone();
        hs.push(std::thread::spawn(move || {
            let mut r = Rng(t + 1);
            // interleaved key space so every thread fights over the same leaves
            for i in 0..per {
                tree.put(&key(i * threads + t), &t.to_le_bytes(), &SYS).unwrap();
                if i % 4 == 3 {
                    let j = r.below(i + 1);
                    tree.delete(&key(j * threads + t), &SYS).unwrap();
                    tree.put(&key(j * threads + t), &t.to_le_bytes(), &SYS).unwrap();
                }
            }
        }));
    }
    for _ in 0..2 {
        let tree = tree.clone();
        hs.push(std::thread::spawn(move || {
            for _ in 0..40 {
                let rows = tree.scan(Unbounded, Unbounded).unwrap();
                // whatever snapshot-less interleaving we hit, order must hold
                assert!(rows.windows(2).all(|w| w[0].0 < w[1].0));
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    let st = tree.check().unwrap();
    assert_eq!(st.entries as u64, threads * per);
    for t in 0..threads {
        for i in (0..per).step_by(101) {
            assert_eq!(tree.get(&key(i * threads + t)).unwrap().unwrap(), t.to_le_bytes());
        }
    }
    assert_eq!(e.pool.pinned_count(), 0);
}

#[test]
fn concurrent_deletes_with_merges() {
    let e = env("conc-del", 512);
    let tree = e.tree.clone();
    let n = 24_000u64;
    for i in 0..n {
        tree.put(&key(i), &[0u8; 40], &SYS).unwrap();
    }
    let threads = 6u64;
    let hs: Vec<_> = (0..threads)
        .map(|t| {
            let tree = tree.clone();
            std::thread::spawn(move || {
                for i in (t..n).step_by(threads as usize) {
                    // leave every 10th alive
                    if i % 10 != 0 {
                        assert!(tree.delete(&key(i), &SYS).unwrap().is_some());
                    }
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    let st = tree.check().unwrap();
    assert_eq!(st.entries as u64, n / 10);
    assert_eq!(e.pool.pinned_count(), 0);
}
