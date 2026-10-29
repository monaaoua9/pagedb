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

200,000 rows. Three back-to-back runs; median with min–max in brackets.

| workload | result |
|---|---:|
| insert, 1000 commits | 53,352 rows/s (50,756 – 60,145) |
| random get | 280,205 ops/s (279,975 – 330,885) |
| full scan | 72 ms (68 – 75) |
| single-row txn, fsync each | 945 tps (897 – 981) |

Buffer pool: 1,164,806 hits, 0 misses, 0 evictions. The whole data set fits in the pool, so this measures the B+ tree and logging, not the disk read path.

## Against SQLite

```
cd bench && cargo run --release
```

Same four workloads on both engines via `rusqlite`: 200,000 rows, 4-byte key, 100-byte value, fsync on every commit (SQLite in WAL mode with `synchronous=FULL`). The program itself reports the best of 3 runs.

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
- Numbers drift between runs on this laptop. The quick bench's random get ranged from 280k to 331k over three runs, and the SQLite comparison's 330k sits at the top of that range because it reports the best of 3. Compare engines only within one program run.
