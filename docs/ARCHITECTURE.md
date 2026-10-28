# pagedb architecture

A page-oriented storage engine in Rust, std only, no unsafe beyond two `ManuallyDrop::drop`
calls in the buffer-pool guards. Ordered key/value tables on B+ trees, ACID transactions,
ARIES-style crash recovery.

```
                         Database / Transaction            db.rs, txn/
                          │  strict 2PL: row S/X under table IS/IX/S
                          │  LockManager (waits-for graph deadlock detection)
                          ▼
                       BTree (one per table)              btree/
                          │  latch crabbing, root page id never moves
                          │  every change goes through a Mtr
                          ▼
              ┌────────── Mtr (mini-transaction) ──────────┐   wal/mtr.rs
              │ holds page write latches, diffs the pages, │
              │ emits ONE log record, stamps the LSN       │
              └───────┬─────────────────────────┬──────────┘
                      │ pins / latches          │ append
                      ▼                         ▼
              BufferPool (LRU-K)  ──flush_to──▶ LogManager (WAL)      buffer/, wal/log.rs
                      │   WAL rule: log durable up to page LSN
                      │   before the page is written
                      ▼
                 DiskManager  (8 KiB pages, CRC32 per page, pread/pwrite)   storage/
```

Files in a database directory: `data.db` (pages), `wal.log` (log), `wal.master`
(LSN of the last completed checkpoint, replaced atomically via tmp + rename).

## Layers

### storage: `DiskManager`
Fixed 8192-byte pages addressed by `PageId` (u32). Positional I/O so reads and writes never
share a file cursor. Bytes 0..16 of every page are reserved: page LSN (8), CRC32 (4), spare (4).
The checksum is stamped on write and verified on read; an all-zero page counts as valid
(allocated, never written). Page ids come from a counter derived from file length plus an
free list; once the database is open the Mtr maintains a persistent list in page 0 instead.

### buffer: `BufferPool`
Fixed array of frames, one `RwLock` per frame is the **page latch**. Page table, free list and
replacer sit behind one mutex. Pins are atomics so unpinning never takes that mutex.

* Replacement: **LRU-K (K=2)**. Frames with fewer than K accesses have infinite backward
  distance and are evicted first, so a scan can't flush the hot set.
* `ReadGuard` / `WriteGuard` are RAII: latch is released *before* the unpin on drop, so a frame
  can never be recycled under a live guard.
* `try_fetch_read` lets callers avoid blocking on a latch while holding another one.
* WAL rule is enforced in the single place pages leave memory (`write_out`): it asks the
  `LogFlusher` to make the log durable up to the page LSN first.
* Everything returns `PoolExhausted` instead of blocking if every frame is pinned.

Trade-off: disk reads and evictions happen under the metadata mutex. Simple and obviously
correct; sharding the page table is the first thing to do if it ever shows up in a profile.

### btree: slotted-page B+ tree
Nodes live in pages. Slot array grows up, cells grow down, free space is tracked in the header
so "does it fit" is O(1); fragmentation is repaired by compaction on demand.

* Leaf cell `klen|vlen|key|value`, internal cell `child|klen|key`. n keys, n+1 children, the
  rightmost child lives in the header `link`. Child *i* holds keys `< key[i]`, child *i+1* holds
  keys `>= key[i]`.
* Variable-length keys (≤ 512 B). Values ≤ 1500 B live in the leaf cell; bigger ones (up to 256 KiB)
  spill into a chain of overflow pages and the cell keeps `tag | length | first page`. The cell cap is
  what guarantees any two cells fit on a page, which makes splits always possible. A chain is built,
  replaced and freed inside the same mini-transaction as the tree change, so it is atomic and logged
  like everything else; undo re-creates the value logically.
* **The root page id never changes.** A root split copies the halves into two new pages and
  rewrites the root in place, so catalog entries stay valid forever.
* Leaves are chained through `link` for range scans.
* **Insert**: optimistic first (S-latch down, X-latch only the leaf). If the leaf can't absorb
  the change, restart pessimistically with **latch crabbing**: X-latch down and release every
  ancestor as soon as a node is *safe* (has room for the worst separator a split below could
  push up). Sequential inserts split at the right edge so leaves stay ~100% full.
* **Delete**: same shape; "safe" means it can lose a key and stay above the fill floor (¼ page).
  Underflow triggers merge with a sibling when the pair fits in one page, otherwise
  redistribution. Merges cascade upward; a root left with one child pulls it up.
  Redistribution is skipped when the parent has no room for a longer separator, which only
  leaves a node underfull and never breaks an invariant.
* **Scan**: leaf to leaf with `try_fetch_read` on the sibling; on contention drop the leaf and
  re-descend from the last key. No scanner ever blocks while holding a latch, which is what
  makes merges (which latch a left sibling after the node) deadlock-free.
* `check()` verifies ordering, key ranges per subtree, uniform depth, leaf chain, free-space
  accounting. Tests call it after every kind of workload.

### wal: logging and recovery

**Mini-transactions.** One logical tree operation (insert + all splits it causes, or delete +
all merges) touches several pages. `Mtr` latches them, lets the tree edit them, then diffs each
page against a snapshot and writes **a single log record** holding all diffs. A crash therefore
sees either the whole structure change or none of it, and recovery never has to undo a
half-done split. Latches are held until the record has an LSN and the pages are stamped with
it; releasing earlier would let another thread log a newer change to the same page first.

**Record types** (`wal/record.rs`, length + CRC32 framed, LSN = byte offset):

| record | content |
|---|---|
| `Update` | txn, prev LSN, **logical undo** (`Remove{key}` / `Restore{key,value}`), page diffs |
| `Clr` | compensation record: txn, prev, `undo_next`, page diffs |
| `Commit`, `End` | txn, prev |
| `Checkpoint` | active txn table, dirty page table, next txn id |

Redo is physical (byte ranges per page, or a full image), undo is logical (re-run the inverse
operation through the tree). Physical undo would be wrong here: by rollback time another
transaction may have split or merged the page the original change landed on.

**Torn writes.** The first change to a page after it was last clean logs a *full page image*.
If the page on disk fails its checksum, recovery reads it as zeroes and the image rebuilds it.

**Commit** = append `Commit`, `fsync` the log. Concurrent committers share one fsync because
whoever takes the log mutex first flushes everything buffered (group commit).

**Checkpoint** is fuzzy. Pages are flushed first only to shorten redo; the record is a snapshot
taken with the log gate held exclusively (mini-transactions hold it shared while appending and
stamping), so ATT and DPT are consistent with the log. The master record is updated only after
the data file is fsynced.

**Recovery** (`wal/recovery.rs`), the ARIES three passes:

1. *Analysis* from the checkpoint: rebuild active-txn table (committed or not) and dirty page
   table with each page's first-dirtying LSN.
2. *Redo*: from the smallest DPT LSN, re-apply every `Update`/`Clr` diff whose LSN is newer
   than the page's LSN ("repeat history", including losers).
3. *Undo*: roll back every non-committed transaction along its `prev`/`undo_next` chain,
   writing CLRs so a crash during recovery never repeats finished undo. Committed transactions
   without an `End` get one.

A torn log tail (partial last frame) is detected by CRC at open and truncated.

### txn: concurrency control
Strict two-phase locking, so schedules are serializable.

* Modes `IS IX S SIX X` with the standard compatibility matrix and upgrade lattice.
* Row ops: table `IS`/`IX`, row `S`/`X`. Scans take table `S`, which blocks every writer
  (writers hold `IX`) and therefore also prevents phantoms.
* FIFO queues with upgrade priority, no starvation of writers by a stream of readers.
* **Deadlock detection**: whenever a request would wait, walk the waits-for graph; if it reaches
  the requester the request fails with `Error::Deadlock` (the transaction that closes the cycle
  is the victim). Plus a wait timeout as a backstop.
* Commit/rollback release everything. Dropping a `Transaction` rolls it back.

## Latch / lock ordering (why there are no latch deadlocks)

1. Transaction locks are acquired **before** any latch and never while holding one.
2. Latches are taken top-down along a root-to-leaf path. The only exception, a merge latching
   a *left* sibling after the node, is safe because the parent is X-latched (nobody else can be
   in between) and scanners never block on a sibling.
3. The log gate is taken after page latches, never the other way round, and the checkpoint takes
   it while holding no latches.
4. The buffer-pool mutex is never held while waiting for a page latch (flush pins first,
   releases the mutex, then latches).

## What is deliberately not here

* No SQL layer yet, no secondary indexes.
* (done) Freed pages are chained into a persistent free list headed in page 0, updated inside the
  same log record as the merge that freed them, so they survive crashes and get reused.
* The log is never truncated/archived; a checkpoint only shortens recovery.
* Reads are lock-based, not MVCC; long scans block writers on that table.
* Single node, no replication, no encryption.

## Testing

**Fault injection** (`src/failpoint.rs`, `tests/fault_injection.rs`): every durable I/O call (page write,
log flush, fsync, checkpoint master record) can be made to fail. The test fails the Nth I/O event for
*every* N across a workload, optionally leaving the write half-finished (torn page / torn log tail),
reopens, and requires the recovered database to equal either the set of acknowledged commits or that
set plus the one commit that was in flight. A second sweep kills recovery itself at each of its own
I/O points and recovers again. Deliberately breaking the WAL rule makes it fail at the 11th event,
so it does detect real violations.

`cargo test` runs: unit tests per module, B+ tree model-checking against `BTreeMap` (random
ops, tiny pools, concurrent writers/scanners/deleters, structure check), transaction semantics
(dirty read, phantom, deadlock, a concurrent bank-transfer invariant with an auditor), recovery
(committed survives, losers undone with stolen pages, checkpoints, double recovery, torn page,
cut commit record) and a **crash fuzzer** that randomly commits / rolls back / abandons
transactions, kills the process state, recovers, and compares against a model across many
rounds.
