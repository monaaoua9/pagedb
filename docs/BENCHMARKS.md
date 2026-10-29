# pagedb: benchmarks

Measured 2026-10-29.

| | |
|---|---|
| CPU | Intel i5-8365U, 4 cores / 8 threads, 1.6 GHz base |
| RAM / disk | 8 GB, SSD |
| OS | Windows 11 |
| Toolchain | Rust 1.99.1, `--release` |

## Quick numbers

```
cargo run --release --example bench
```

200,000 rows, one run.

| workload | result |
|---|---:|
| insert, 1000 commits | 65,225 rows/s |
| random get | 357,482 ops/s |
| full scan | 54 ms |
| single-row txn, fsync each | 1,073 tps |

Buffer pool: 1,164,806 hits, 0 misses, 0 evictions. The whole data set fits in the pool, so this measures the B+ tree and logging, not the disk read path.

## Against SQLite

```
cd bench && cargo run --release
```

Same four workloads on both engines via `rusqlite`: 200,000 rows, 4-byte key, 100-byte value, fsync on every commit (SQLite in WAL mode with `synchronous=FULL`), best of 3.

| workload | pagedb | SQLite |
|---|---:|---:|
| bulk insert (200-row txns) | 57,922 /s | 95,457 /s |
| random point read | 329,763 /s | 119,831 /s |
| full scan, 200k rows | 67 ms | 101 ms |
| single-row commit (fsync each) | 1,009 /s | 960 /s |

## How to read this

- SQLite parses and executes SQL; pagedb here is a bare key/value API. The read rows say more about that overhead than about B+ tree quality, so don't read "pagedb is 2.7× faster" out of them.
- Bulk insert is where pagedb loses: it does about 40% fewer rows/s than SQLite on this run. I haven't profiled why.
- The commit row is fsync-bound for both, so they're close.
- Insert differs between the two programs (65k in the quick bench, 58k here, different harness). It's a laptop, so treat a ±15% gap as noise.
