use std::ops::Bound::Unbounded;
use std::time::Instant;

use pagedb::{Database, Options};

fn main() -> pagedb::Result<()> {
    let dir = std::env::temp_dir().join("pagedb-bench");
    let _ = std::fs::remove_dir_all(&dir);
    let db = Database::open(&dir, Options { pool_pages: 4096, ..Options::default() })?;
    let t = db.create_table("kv")?;

    let n = 200_000u32;
    let batch: u32 = std::env::var("BATCH").ok().and_then(|v| v.parse().ok()).unwrap_or(200);
    let start = Instant::now();
    for b in 0..n / batch {
        let mut tx = db.begin();
        for i in b * batch..(b + 1) * batch {
            tx.insert(&t, &i.to_be_bytes(), &[7u8; 100])?;
        }
        tx.commit()?;
    }
    let el = start.elapsed();
    println!(
        "insert  {n} rows in {el:.2?}  ({:.0} rows/s, {} commits)",
        n as f64 / el.as_secs_f64(),
        n / batch
    );

    let start = Instant::now();
    let mut tx = db.begin();
    let mut x = 12345u32;
    for _ in 0..n {
        x = x.wrapping_mul(1664525).wrapping_add(1013904223);
        assert!(tx.get(&t, &(x % n).to_be_bytes())?.is_some());
    }
    tx.commit()?;
    let el = start.elapsed();
    println!("get     {n} random in {el:.2?}  ({:.0} ops/s)", n as f64 / el.as_secs_f64());

    let start = Instant::now();
    let rows = db.begin().scan(&t, Unbounded, Unbounded)?;
    let el = start.elapsed();
    println!("scan    {} rows in {el:.2?}", rows.len());

    let start = Instant::now();
    for i in 0..500u32 {
        let mut tx = db.begin();
        tx.put(&t, &i.to_be_bytes(), b"x")?;
        tx.commit()?;
    }
    let el = start.elapsed();
    println!("commit  500 single-row txns (fsync each) in {el:.2?}  ({:.0} tps)", 500.0 / el.as_secs_f64());

    let s = db.pool_stats();
    println!("pool    hits {} misses {} evictions {}", s.hits, s.misses, s.evictions);
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
