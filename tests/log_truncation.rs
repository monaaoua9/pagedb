mod common;

use std::fs;
use std::ops::Bound::Unbounded;
use std::path::Path;

use common::TempDir;
use pagedb::{Database, Options};

fn wal_bytes(dir: &Path) -> u64 {
    fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("wal.log"))
        .map(|e| e.metadata().unwrap().len())
        .sum()
}

fn fill(db: &Database, from: u32, to: u32) {
    let t = db.table("t").unwrap();
    for chunk in (from..to).collect::<Vec<_>>().chunks(100) {
        let mut tx = db.begin();
        for i in chunk {
            tx.insert(&t, format!("k{i:08}").as_bytes(), &[7u8; 200]).unwrap();
        }
        tx.commit().unwrap();
    }
}

fn count(db: &Database) -> usize {
    let t = db.table("t").unwrap();
    db.begin().scan(&t, Unbounded, Unbounded).unwrap().len()
}

fn no_auto() -> Options {
    Options { checkpoint_bytes: u64::MAX, ..Options::default() }
}

#[test]
fn checkpoint_shrinks_the_log() {
    let dir = TempDir::new("trunc-shrink");
    let db = Database::open(dir.path(), no_auto()).unwrap();
    db.create_table("t").unwrap();
    fill(&db, 0, 20_000);
    let before = wal_bytes(dir.path());
    assert!(before > 4 << 20, "{before}");
    db.checkpoint().unwrap();
    let after = wal_bytes(dir.path());
    assert!(after < 64 << 10, "{after}");

    // keeps working across the cut, and across a restart
    fill(&db, 20_000, 21_000);
    drop(db);
    let db = Database::open(dir.path(), no_auto()).unwrap();
    assert_eq!(count(&db), 21_000);
    db.verify().unwrap();
}

#[test]
fn crash_after_truncation_recovers() {
    let dir = TempDir::new("trunc-crash");
    let db = Database::open(dir.path(), no_auto()).unwrap();
    db.create_table("t").unwrap();
    fill(&db, 0, 15_000);
    db.checkpoint().unwrap();
    fill(&db, 15_000, 16_000);
    db.simulate_crash();

    let db = Database::open(dir.path(), no_auto()).unwrap();
    assert_eq!(count(&db), 16_000);
    db.verify().unwrap();
    // a second round on the already-truncated log
    fill(&db, 16_000, 30_000);
    db.checkpoint().unwrap();
    fill(&db, 30_000, 30_500);
    db.simulate_crash();
    let db = Database::open(dir.path(), no_auto()).unwrap();
    assert_eq!(count(&db), 30_500);
    db.verify().unwrap();
}

#[test]
fn running_transaction_pins_the_log() {
    let dir = TempDir::new("trunc-pin");
    let db = Database::open(dir.path(), no_auto()).unwrap();
    let t = db.create_table("t").unwrap();
    let mut slow = db.begin();
    slow.insert(&t, b"zz-uncommitted", b"x").unwrap();
    fill(&db, 0, 15_000);
    db.checkpoint().unwrap();
    // the open transaction's first record is at the very front, nothing can go
    assert!(wal_bytes(dir.path()) > 4 << 20);
    std::mem::forget(slow);
    db.simulate_crash();

    let db = Database::open(dir.path(), no_auto()).unwrap();
    assert_eq!(db.recovery_report().rolled_back, 1);
    assert_eq!(count(&db), 15_000);
    let t = db.table("t").unwrap();
    assert!(db.begin().get(&t, b"zz-uncommitted").unwrap().is_none());
}

#[test]
fn transaction_spanning_a_checkpoint_rolls_back_after_truncation() {
    let dir = TempDir::new("trunc-span");
    let db = Database::open(dir.path(), no_auto()).unwrap();
    let t = db.create_table("t").unwrap();
    fill(&db, 0, 15_000);
    db.checkpoint().unwrap();
    let mut slow = db.begin();
    slow.insert(&t, b"zz-late", b"x").unwrap();
    fill(&db, 15_000, 30_000);
    db.checkpoint().unwrap();
    std::mem::forget(slow);
    db.simulate_crash();

    let db = Database::open(dir.path(), no_auto()).unwrap();
    assert_eq!(count(&db), 30_000);
    db.verify().unwrap();
}
