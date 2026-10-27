// pagedb vs SQLite on the same four workloads. Both fsync on every commit
// (SQLite: WAL journal, synchronous=FULL), same keys and 100-byte values.
use std::ops::Bound::Unbounded;
use std::time::{Duration, Instant};

use rusqlite::{params, Connection};

const N: u32 = 200_000;
const BATCH: u32 = 200;
const SINGLES: u32 = 500;
const VAL: [u8; 100] = [7u8; 100];

struct Row {
    insert: Duration,
    get: Duration,
    scan: Duration,
    singles: Duration,
}

fn lcg(x: &mut u32) -> u32 {
    *x = x.wrapping_mul(1664525).wrapping_add(1013904223);
    *x >> 8
}

fn pagedb_run() -> Row {
    let dir = std::env::temp_dir().join("cmp-pagedb");
    let _ = std::fs::remove_dir_all(&dir);
    let db = pagedb::Database::open(&dir, pagedb::Options { pool_pages: 4096, ..Default::default() }).unwrap();
    let t = db.create_table("kv").unwrap();

    let s = Instant::now();
    for b in 0..N / BATCH {
        let mut tx = db.begin();
        for i in b * BATCH..(b + 1) * BATCH {
            tx.insert(&t, &i.to_be_bytes(), &VAL).unwrap();
        }
        tx.commit().unwrap();
    }
    let insert = s.elapsed();

    // reads in many short transactions so lock bookkeeping doesn't pile up in one
    let s = Instant::now();
    let mut x = 12345u32;
    for _ in 0..N / 1000 {
        let mut tx = db.begin();
        for _ in 0..1000 {
            let k = lcg(&mut x) % N;
            assert!(tx.get(&t, &k.to_be_bytes()).unwrap().is_some());
        }
        tx.commit().unwrap();
    }
    let get = s.elapsed();

    let s = Instant::now();
    let mut tx = db.begin();
    let rows = tx.scan(&t, Unbounded, Unbounded).unwrap();
    assert_eq!(rows.len() as u32, N);
    tx.commit().unwrap();
    let scan = s.elapsed();

    let s = Instant::now();
    for i in 0..SINGLES {
        let mut tx = db.begin();
        tx.put(&t, &i.to_be_bytes(), b"x").unwrap();
        tx.commit().unwrap();
    }
    let singles = s.elapsed();
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
    Row { insert, get, scan, singles }
}

fn sqlite_run() -> Row {
    let dir = std::env::temp_dir().join("cmp-sqlite");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let c = Connection::open(dir.join("db.sqlite")).unwrap();
    c.pragma_update(None, "journal_mode", "WAL").unwrap();
    c.pragma_update(None, "synchronous", "FULL").unwrap();
    // pagedb gets 4096 x 8KiB = 32MiB of buffer pool; give SQLite the same
    c.pragma_update(None, "cache_size", -32768).unwrap();
    c.execute("CREATE TABLE kv (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID", []).unwrap();

    let s = Instant::now();
    for b in 0..N / BATCH {
        let tx = c.unchecked_transaction().unwrap();
        {
            let mut st = tx.prepare_cached("INSERT INTO kv VALUES (?1, ?2)").unwrap();
            for i in b * BATCH..(b + 1) * BATCH {
                st.execute(params![&i.to_be_bytes()[..], &VAL[..]]).unwrap();
            }
        }
        tx.commit().unwrap();
    }
    let insert = s.elapsed();

    let s = Instant::now();
    let mut x = 12345u32;
    {
        let mut st = c.prepare("SELECT v FROM kv WHERE k = ?1").unwrap();
        for _ in 0..N {
            let k = lcg(&mut x) % N;
            let v: Vec<u8> = st.query_row(params![&k.to_be_bytes()[..]], |r| r.get(0)).unwrap();
            assert_eq!(v.len(), 100);
        }
    }
    let get = s.elapsed();

    let s = Instant::now();
    let mut st = c.prepare("SELECT k, v FROM kv ORDER BY k").unwrap();
    let rows: Vec<(Vec<u8>, Vec<u8>)> = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(rows.len() as u32, N);
    let scan = s.elapsed();
    drop(st);

    let s = Instant::now();
    for i in 0..SINGLES {
        c.execute("INSERT OR REPLACE INTO kv VALUES (?1, ?2)", params![&i.to_be_bytes()[..], &b"x"[..]]).unwrap();
    }
    let singles = s.elapsed();
    drop(c);
    let _ = std::fs::remove_dir_all(&dir);
    Row { insert, get, scan, singles }
}

fn main() {
    let rounds = 3;
    let mut best: Vec<Row> = vec![];
    for name in ["pagedb", "sqlite"] {
        let mut rows: Vec<Row> = (0..rounds).map(|_| if name == "pagedb" { pagedb_run() } else { sqlite_run() }).collect();
        let pick = |f: fn(&Row) -> Duration, rows: &Vec<Row>| rows.iter().map(f).min().unwrap();
        best.push(Row {
            insert: pick(|r| r.insert, &rows),
            get: pick(|r| r.get, &rows),
            scan: pick(|r| r.scan, &rows),
            singles: pick(|r| r.singles, &rows),
        });
        rows.clear();
    }
    let rate = |n: u32, d: Duration| n as f64 / d.as_secs_f64();
    println!("best of {rounds}, {N} rows, 4-byte key, 100-byte value, fsync per commit\n");
    println!("{:<34}{:>14}{:>14}", "", "pagedb", "sqlite");
    println!("{:<34}{:>12.0}/s{:>12.0}/s", "bulk insert (200-row txns)", rate(N, best[0].insert), rate(N, best[1].insert));
    println!("{:<34}{:>12.0}/s{:>12.0}/s", "random point read", rate(N, best[0].get), rate(N, best[1].get));
    println!("{:<34}{:>12.0}ms{:>12.0}ms", "full scan, 200k rows", best[0].scan.as_secs_f64() * 1e3, best[1].scan.as_secs_f64() * 1e3);
    println!("{:<34}{:>12.0}/s{:>12.0}/s", "single-row commit (fsync each)", rate(SINGLES, best[0].singles), rate(SINGLES, best[1].singles));
}
