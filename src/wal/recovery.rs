use std::collections::HashMap;
use std::sync::Arc;

use super::log::LOG_START;
use super::record::{Record, Undo};
use super::{LogManager, TxnCtx};
use crate::btree::{BTree, LogMode};
use crate::buffer::BufferPool;
use crate::error::Result;
use crate::storage::page::page_lsn;
use crate::storage::DiskManager;
use crate::types::{Lsn, PageId, TxnId};

#[derive(Debug, Default, Clone)]
pub struct RecoveryReport {
    pub scanned: usize,
    pub redone: usize,
    pub rolled_back: usize,
    pub next_txn: TxnId,
}

struct AttEntry {
    last: Lsn,
    committed: bool,
}

// Analysis -> redo -> undo, the ARIES order.
//  * analysis rebuilds the active-transaction and dirty-page tables from the
//    last checkpoint forward
//  * redo repeats history for every logged page change newer than the page on disk
//  * undo rolls back whatever never committed, logically, writing CLRs so a
//    crash during recovery doesn't redo the undo work
pub fn recover(disk: &DiskManager, pool: &Arc<BufferPool>, log: &Arc<LogManager>) -> Result<RecoveryReport> {
    let mut rep = RecoveryReport { next_txn: 1, ..Default::default() };
    let mut att: HashMap<TxnId, AttEntry> = HashMap::new();
    let mut dpt: HashMap<PageId, Lsn> = HashMap::new();

    let ckpt = log.checkpoint_lsn();
    let from = if ckpt != 0 { ckpt } else { LOG_START };
    for item in log.iter_from(from) {
        let (lsn, rec) = item?;
        rep.scanned += 1;
        match rec {
            Record::Checkpoint { next_txn, att: a, dpt: d } => {
                // only the one the master record points at seeds the tables;
                // a later checkpoint without a master update would just repeat what we've seen
                if lsn == ckpt {
                    for (t, last, committed) in a {
                        att.insert(t, AttEntry { last, committed });
                    }
                    for (p, l) in d {
                        dpt.insert(p, l);
                    }
                }
                rep.next_txn = rep.next_txn.max(next_txn);
            }
            Record::Update { txn, diffs, .. } | Record::Clr { txn, diffs, .. } => {
                if txn != 0 {
                    att.entry(txn).or_insert(AttEntry { last: 0, committed: false }).last = lsn;
                    rep.next_txn = rep.next_txn.max(txn + 1);
                }
                for d in diffs {
                    dpt.entry(d.page).or_insert(lsn);
                }
            }
            Record::Commit { txn, .. } => {
                let e = att.entry(txn).or_insert(AttEntry { last: 0, committed: false });
                e.last = lsn;
                e.committed = true;
                rep.next_txn = rep.next_txn.max(txn + 1);
            }
            Record::End { txn, .. } => {
                att.remove(&txn);
                rep.next_txn = rep.next_txn.max(txn + 1);
            }
        }
    }

    if let Some(&start) = dpt.values().min() {
        // pages that never made it to disk intact come back as zeroes; the
        // full-page image logged at their first change repairs them
        disk.set_lenient(true);
        let r = redo(disk, pool, log, start, &dpt, &mut rep);
        disk.set_lenient(false);
        r?;
        pool.flush_all()?;
    }

    if disk.num_pages() > 0 {
        pool.set_freelist(true);
    }
    let mut losers = Vec::new();
    for (txn, e) in att {
        let ctx = TxnCtx::new(txn, e.last);
        if e.committed {
            log.append_for(&ctx, |prev| Record::End { txn, prev })?;
        } else {
            losers.push(ctx);
        }
    }
    for ctx in &losers {
        undo_txn(pool, log, ctx)?;
        rep.rolled_back += 1;
    }
    log.flush_all()?;
    Ok(rep)
}

fn redo(
    disk: &DiskManager,
    pool: &BufferPool,
    log: &LogManager,
    start: Lsn,
    dpt: &HashMap<PageId, Lsn>,
    rep: &mut RecoveryReport,
) -> Result<()> {
    for item in log.iter_from(start) {
        let (lsn, rec) = item?;
        let diffs = match &rec {
            Record::Update { diffs, .. } | Record::Clr { diffs, .. } => diffs,
            _ => continue,
        };
        for d in diffs {
            // not dirty at the checkpoint, or not dirtied until after this record
            if dpt.get(&d.page).is_none_or(|&r| lsn < r) {
                continue;
            }
            disk.ensure(d.page);
            let mut g = pool.fetch_write(d.page)?;
            if page_lsn(&g) >= lsn {
                continue;
            }
            d.apply(&mut g);
            g.stamp(lsn);
            rep.redone += 1;
        }
    }
    Ok(())
}

// Walks the transaction's chain newest to oldest, undoing each logical change
// and logging a CLR for it. Safe to re-run after a crash: CLRs carry undo_next,
// so work already done is skipped.
pub fn undo_txn(pool: &Arc<BufferPool>, log: &Arc<LogManager>, ctx: &TxnCtx) -> Result<()> {
    use std::sync::atomic::Ordering::Acquire;

    let mut lsn = ctx.last_lsn.load(Acquire);
    while lsn != 0 {
        let (rec, _) = log.read_at(lsn)?;
        lsn = match rec {
            Record::Update { prev, undo, .. } => {
                apply_undo(pool, log, ctx, undo, prev)?;
                prev
            }
            Record::Clr { undo_next, .. } => undo_next,
            Record::Commit { prev, .. } | Record::End { prev, .. } => prev,
            Record::Checkpoint { .. } => break,
        };
    }
    let id = ctx.id;
    log.append_for(ctx, |prev| Record::End { txn: id, prev })?;
    Ok(())
}

fn apply_undo(
    pool: &Arc<BufferPool>,
    log: &Arc<LogManager>,
    ctx: &TxnCtx,
    undo: Undo,
    undo_next: Lsn,
) -> Result<()> {
    let mode = LogMode::Clr(ctx, undo_next);
    match undo {
        Undo::None => {
            // nothing to take back, but the chain still has to advance past it
        }
        Undo::Remove { root, key } => {
            BTree::open(root, pool.clone(), log.clone()).delete(&key, &mode)?;
        }
        Undo::Restore { root, key, value } => {
            BTree::open(root, pool.clone(), log.clone()).put(&key, &value, &mode)?;
        }
    }
    Ok(())
}
