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
cargo run --release --example bench
```

## Measured on a Windows 11 laptop (release build, NVMe, fsync on every commit)

| | |
|---|---|
| bulk insert, 200-row transactions | ~25k rows/s (commit fsync ≈ 2–3 ms of that) |
| B+ tree insert alone (no locks, no fsync) | ~8.7 µs/row |
| single-row commits | ~850 tps |
| full scan, 200k rows | ~0.25 s |

Numbers vary a lot with the disk; they are here to show the order of magnitude, not as a benchmark claim.

## Limits

Keys ≤ 512 bytes, values ≤ 1500 bytes (no overflow pages).
Log is not truncated. Locking reads, no MVCC. See the architecture doc for the full list.
