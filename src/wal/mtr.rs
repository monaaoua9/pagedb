use super::record::{diff_ranges, DiffBody, PageDiff, Record, Undo};
use super::{LogManager, TxnCtx};
use crate::buffer::{BufferPool, WriteGuard};
use crate::error::{corrupt, Result};
use crate::types::{Lsn, PageData, PageId, INVALID_PAGE, PAGE_HEADER, PAGE_SIZE};

pub enum Kind<'t> {
    // structural change with no owner (bootstrap, catalog); redo only
    System,
    Txn(&'t TxnCtx, Undo),
    Clr(&'t TxnCtx, Lsn),
}

// Mini-transaction. Holds write latches on every page one logical operation
// touches and emits them as a single log record, so a crash can never leave a
// half-finished split behind: either the whole record is in the log or none of
// it is, and recovery only ever redoes whole records.
//
// Latches are kept until the record has an LSN and the pages are stamped with
// it. Releasing earlier would let another thread modify the page and log first,
// and redo would replay the two in the wrong order.
pub struct Mtr<'a> {
    pool: &'a BufferPool,
    log: &'a LogManager,
    guards: Vec<Option<WriteGuard<'a>>>,
    freed: Vec<PageId>,
    pending: Vec<PageId>,
}

// head of the persistent free list, in the meta page (page 0); freed pages chain through
// the same offset as a btree node's link field
const FREE_HEAD: usize = PAGE_HEADER + 8;

impl<'a> Mtr<'a> {
    pub fn new(pool: &'a BufferPool, log: &'a LogManager) -> Self {
        Mtr { pool, log, guards: Vec::new(), freed: Vec::new(), pending: Vec::new() }
    }

    pub fn latch(&mut self, pid: PageId) -> Result<usize> {
        if self.guards.iter().flatten().any(|g| g.page_id() == pid) {
            return corrupt(format!("page {pid} latched twice in one mini-transaction"));
        }
        let g = self.pool.fetch_write(pid)?;
        self.guards.push(Some(g));
        Ok(self.guards.len() - 1)
    }

    pub fn find(&self, pid: PageId) -> Option<usize> {
        self.guards.iter().position(|g| g.as_ref().is_some_and(|g| g.page_id() == pid))
    }

    fn meta(&mut self) -> Result<usize> {
        match self.find(0) {
            Some(i) => Ok(i),
            None => self.latch(0),
        }
    }

    fn head_of(p: &PageData) -> PageId {
        PageId::from_le_bytes(p[FREE_HEAD..FREE_HEAD + 4].try_into().unwrap())
    }

    // Reuses a freed page when the list has one. Page 0 is latched after the caller's tree
    // latches and nothing but the list's head page is latched after it, which keeps the
    // latch order acyclic.
    pub fn alloc(&mut self) -> Result<usize> {
        if self.pool.freelist_enabled() {
            let m = self.meta()?;
            let head = Self::head_of(self.page(m));
            if head != INVALID_PAGE {
                let h = self.latch(head)?;
                let next = Self::head_of(self.page(h));
                self.page_mut(m)[FREE_HEAD..FREE_HEAD + 4].copy_from_slice(&next.to_le_bytes());
                self.guards[h].as_mut().unwrap().make_fresh();
                return Ok(h);
            }
        }
        self.alloc_new()
    }

    fn alloc_new(&mut self) -> Result<usize> {
        let g = self.pool.new_page()?;
        self.guards.push(Some(g));
        Ok(self.guards.len() - 1)
    }

    pub fn pid(&self, i: usize) -> PageId {
        self.guards[i].as_ref().unwrap().page_id()
    }

    pub fn page(&self, i: usize) -> &PageData {
        self.guards[i].as_ref().expect("page already released")
    }

    pub fn page_mut(&mut self, i: usize) -> &mut PageData {
        self.guards[i].as_mut().expect("page already released")
    }

    pub fn release(&mut self, i: usize) {
        if let Some(mut g) = self.guards[i].take() {
            // only unmodified pages get released early, but be safe if someone slips
            g.revert();
        }
    }

    pub fn free_later(&mut self, pid: PageId) {
        if self.pool.freelist_enabled() {
            self.pending.push(pid);
        } else {
            self.freed.push(pid);
        }
    }

    fn diff_of(g: &WriteGuard<'_>) -> Option<PageDiff> {
        let after: &PageData = g;
        let full = || DiffBody::Full(after[PAGE_HEADER..].to_vec());
        let body = if g.needs_full_image() {
            full()
        } else {
            let rs = diff_ranges(g.before()?, after);
            if rs.is_empty() {
                return None;
            }
            let sz: usize = rs.iter().map(|(_, b)| b.len() + 4).sum();
            if sz > PAGE_SIZE * 3 / 4 {
                full()
            } else {
                DiffBody::Ranges(rs)
            }
        };
        Some(PageDiff { page: g.page_id(), body })
    }

    // pushes pages freed by this operation onto the persistent list, inside the same record
    fn apply_frees(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let m = self.meta()?;
        for pid in std::mem::take(&mut self.pending) {
            let i = self.find(pid).expect("freed page must still be latched");
            let head = Self::head_of(self.page(m));
            self.page_mut(i)[FREE_HEAD..FREE_HEAD + 4].copy_from_slice(&head.to_le_bytes());
            self.page_mut(m)[FREE_HEAD..FREE_HEAD + 4].copy_from_slice(&pid.to_le_bytes());
        }
        Ok(())
    }

    pub fn commit(mut self, kind: Kind<'_>) -> Result<Lsn> {
        self.apply_frees()?;
        let mut diffs = Vec::new();
        for g in self.guards.iter().flatten().filter(|g| g.touched()) {
            diffs.extend(Self::diff_of(g));
        }
        if diffs.is_empty() && matches!(kind, Kind::System) {
            return Ok(0);
        }

        let gate = self.log.gate_read();
        let lsn = match kind {
            Kind::System => self.log.append(&Record::Update { txn: 0, prev: 0, undo: Undo::None, diffs })?,
            Kind::Txn(ctx, undo) => {
                self.log.append_chained(ctx, |prev| Record::Update { txn: ctx.id, prev, undo, diffs })?
            }
            Kind::Clr(ctx, undo_next) => {
                self.log.append_chained(ctx, |prev| Record::Clr { txn: ctx.id, prev, undo_next, diffs })?
            }
        };
        for g in self.guards.iter_mut().flatten() {
            if g.touched() {
                g.stamp(lsn);
            }
        }
        drop(gate);

        self.guards.clear();
        for pid in self.freed.drain(..) {
            self.pool.free_page(pid);
        }
        Ok(lsn)
    }
}

impl Drop for Mtr<'_> {
    // reached without commit: an error bailed out mid-operation, so put the
    // pages back the way they were rather than leaving unlogged changes in memory
    fn drop(&mut self) {
        for g in self.guards.iter_mut().flatten() {
            g.revert();
        }
    }
}
