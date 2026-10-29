# pagedb: design decisions

Short version of the choices that shape the engine. Details, record formats and latch ordering: [ARCHITECTURE.md](ARCHITECTURE.md).

## Scope

A single-node transactional storage engine: ordered tables on B+ trees, ACID transactions, ARIES-style recovery, std-only (the benchmark crate is separate so the library has no dependencies). A small SQL subset sits on top through the public API only.

## Decisions

**One log record per structural change (mini-transactions).** An insert that splits three pages touches several pages. The `Mtr` latches them, diffs them against a snapshot and writes a single record. A crash sees the whole split or none of it, so recovery never has to repair a half-done split. Cost: the latches are held until the record has an LSN, which lengthens the critical section a bit.

**Physical redo, logical undo.** Redo re-applies byte ranges per page. Undo re-runs the inverse operation (`Remove{key}` / `Restore{key,value}`) through the tree. Physical undo would be wrong, because by rollback time another transaction may have split or merged the page the change originally landed on.

**Full page image on the first change after a clean page.** That's the answer to torn writes: if the page fails its CRC, recovery treats it as zeroes and the image rebuilds it. Cost is a bigger log after each checkpoint.

**Root page id never changes.** A root split copies the halves into two new pages and rewrites the root in place. Catalog entries stay valid and nobody needs a root pointer update logged.

**Optimistic descent, then latch crabbing.** First try S-latching down and X-latching only the leaf. If the leaf can't absorb the change, restart with X-latches and release ancestors as soon as a node is safe. Scans never block on a sibling latch (`try_fetch_read`, re-descend on contention), which is what makes merges deadlock-free.

**LRU-K (K=2) buffer pool.** A scan shouldn't evict the hot set. Known cost: disk reads and evictions happen under the pool's metadata mutex. Simple and obviously correct; sharding the page table is the first thing to do if it shows up in a profile.

**Strict 2PL, not MVCC.** Row S/X locks under table intention locks, deadlock detection on the waits-for graph, scans take a table S lock (which also prevents phantoms). Serializable and easy to reason about, but a long scan blocks writers on that table.

**Log truncation by file swap.** After a checkpoint the dead front of the log is copied into a new generation file; the master record flips atomically. No rename over an open file, since Windows refuses that.

## How it's checked

- Fault injection: every durable I/O call can fail. The test fails the Nth I/O event for every N across a workload (optionally half-written), reopens, and requires the database to equal the acknowledged commits, or those plus the one in flight. A second sweep kills recovery itself. Deliberately breaking the WAL rule makes it fail at the 11th event, so it does catch real violations.
- Crash fuzzer against a model across many rounds.
- B+ tree model check against `BTreeMap` with tiny pools and concurrent writers, scanners and deleters, plus a structure checker.

## What's not here

No MVCC, no secondary indexes or joins in SQL, single node, no replication or encryption. Keys ≤ 512 B, values ≤ 256 KiB.
