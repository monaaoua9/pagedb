# pagedb

[![ci](https://github.com/monaaoua9/pagedb/actions/workflows/ci.yml/badge.svg)](https://github.com/monaaoua9/pagedb/actions/workflows/ci.yml) [![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Page-based storage engine in Rust: buffer pool, disk-resident B+ tree, write-ahead log with
ARIES-style recovery, and two-phase-locking transactions. No dependencies.

```rust
use pagedb::{Database, Options};

let db = Database::open("./data", Options::default())?;
let users = db.create_table("users")?;

let mut tx = db.begin();
tx.insert(&users, b"u:1", b"ada")?;
tx.put(&users, b"u:2", b"grace")?;
tx.commit()?;            // fsynced WAL record; survives a crash from here on

let mut tx = db.begin();
let rows = tx.scan(&users, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)?;
```

| requirement | where |
|---|---|
| fixed-size pages, page manager | `storage/` |
| buffer pool, LRU-K, pin/unpin | `buffer/` |
| B+ tree on pages, split / merge | `btree/` |
| WAL, undo/redo, crash recovery | `wal/` |
| shared / exclusive row and table locks | `txn/lock.rs` |

Design notes, invariants and the recovery protocol: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Run it

```
cargo test                           # unit + integration + crash fuzzer
cargo run --release --example demo   # crash with a transaction in flight, recover
cargo run --release --example bench   # quick numbers
(cd bench && cargo run --release)    # pagedb vs SQLite
```

## Compared with SQLite

`bench/` is a separate crate (so the library keeps zero dependencies) that runs the same four workloads
against pagedb and SQLite via `rusqlite`: 200k rows, 4-byte key, 100-byte value, fsync on every commit
(SQLite in WAL mode with `synchronous=FULL`), both with a 32 MiB cache. Best of 3, Windows 11 laptop:

| | pagedb | SQLite |
|---|---|---|
| bulk insert (200-row transactions) | 71k rows/s | 92k rows/s |
| random point read | 517k ops/s | 161k ops/s |
| full scan, 200k rows | 46 ms | 82 ms |
| single-row commit (fsync each) | 1056 tps | 825 tps |

Read this fairly: SQLite is parsing and executing SQL and pagedb is a bare key/value API, so the read
numbers say more about that overhead than about B-tree quality. The one workload where pagedb loses is
bulk insert, where SQLite's tighter write path wins by about 30%. Run it with
`cd bench && cargo run --release`; numbers depend heavily on the disk.

## Limits

Keys ≤ 512 bytes, values ≤ 1500 bytes (no overflow pages).
Log is not truncated. Locking reads, no MVCC. See the architecture doc for the full list.
