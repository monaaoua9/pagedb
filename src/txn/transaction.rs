use std::ops::Bound;
use std::sync::atomic::Ordering::*;
use std::sync::Arc;

use super::lock::{LockMode, Resource};
use crate::btree::LogMode;
use crate::db::{Engine, Table};
use crate::error::{Error, Result};
use crate::types::TxnId;
use crate::wal::recovery::undo_txn;
use crate::wal::{Record, TxnCtx};

// Strict two-phase locking: row locks (under table intention locks) are held to
// commit/rollback. Point reads take S on the row; scans take S on the whole
// table, which also keeps phantoms out. Dropping without commit rolls back.
pub struct Transaction {
    eng: Arc<Engine>,
    ctx: Arc<TxnCtx>,
    done: bool,
}

impl Transaction {
    pub(crate) fn new(eng: Arc<Engine>) -> Self {
        let ctx = eng.txns.begin();
        Transaction { eng, ctx, done: false }
    }

    pub fn id(&self) -> TxnId {
        self.ctx.id
    }

    fn live(&self) -> Result<()> {
        if self.done {
            Err(Error::TxnClosed)
        } else {
            Ok(())
        }
    }

    fn table_lock(&self, t: &Table, m: LockMode) -> Result<()> {
        self.eng.locks.lock(self.ctx.id, Resource::Table(t.root()), m)
    }

    fn row_lock(&self, t: &Table, key: &[u8], m: LockMode) -> Result<()> {
        self.eng.locks.lock(self.ctx.id, Resource::Row(t.root(), key.to_vec()), m)
    }

    fn for_write(&self, t: &Table, key: &[u8]) -> Result<()> {
        self.live()?;
        self.table_lock(t, LockMode::IX)?;
        self.row_lock(t, key, LockMode::X)
    }

    pub fn get(&mut self, t: &Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.live()?;
        self.table_lock(t, LockMode::IS)?;
        self.row_lock(t, key, LockMode::S)?;
        t.tree().get(key)
    }

    pub fn insert(&mut self, t: &Table, key: &[u8], val: &[u8]) -> Result<()> {
        self.for_write(t, key)?;
        if t.tree().insert(key, val, &LogMode::Txn(&self.ctx))? {
            Ok(())
        } else {
            Err(Error::DuplicateKey)
        }
    }

    // upsert, returns the previous value
    pub fn put(&mut self, t: &Table, key: &[u8], val: &[u8]) -> Result<Option<Vec<u8>>> {
        self.for_write(t, key)?;
        t.tree().put(key, val, &LogMode::Txn(&self.ctx))
    }

    pub fn update(&mut self, t: &Table, key: &[u8], val: &[u8]) -> Result<Vec<u8>> {
        self.for_write(t, key)?;
        // row is X-locked, so the existence check can't go stale before the write
        if t.tree().get(key)?.is_none() {
            return Err(Error::KeyNotFound);
        }
        Ok(t.tree().put(key, val, &LogMode::Txn(&self.ctx))?.unwrap())
    }

    pub fn delete(&mut self, t: &Table, key: &[u8]) -> Result<bool> {
        self.for_write(t, key)?;
        Ok(t.tree().delete(key, &LogMode::Txn(&self.ctx))?.is_some())
    }

    pub fn scan(
        &mut self,
        t: &Table,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.live()?;
        self.table_lock(t, LockMode::S)?;
        t.tree().scan(lower, upper)
    }

    // locks the row exclusively up front; for read-modify-write so two of them queue up
    // instead of both taking S and deadlocking on the upgrade
    pub fn get_for_update(&mut self, t: &Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.for_write(t, key)?;
        t.tree().get(key)
    }

    // scan under a table-level X lock, for statements that will write what they read
    pub fn scan_for_update(
        &mut self,
        t: &Table,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.live()?;
        self.table_lock(t, LockMode::X)?;
        t.tree().scan(lower, upper)
    }

    pub fn commit(mut self) -> Result<()> {
        self.live()?;
        self.done = true;
        let res = self.finish_commit();
        self.eng.locks.release_all(self.ctx.id);
        self.eng.txns.finish(self.ctx.id);
        res?;
        self.eng.maybe_checkpoint();
        Ok(())
    }

    fn finish_commit(&self) -> Result<()> {
        if self.ctx.last_lsn.load(Acquire) == 0 {
            return Ok(());
        }
        let id = self.ctx.id;
        let lsn = self.eng.log.append_for(&self.ctx, |prev| Record::Commit { txn: id, prev })?;
        self.eng.log.flush_to(lsn)?;
        // End doesn't need to be durable, recovery would just write it again
        self.eng.log.append_for(&self.ctx, |prev| Record::End { txn: id, prev })?;
        Ok(())
    }

    pub fn rollback(mut self) -> Result<()> {
        self.live()?;
        self.abort()
    }

    fn abort(&mut self) -> Result<()> {
        self.done = true;
        if self.ctx.last_lsn.load(Acquire) != 0 && !self.ctx.committed.load(Acquire) {
            undo_txn(&self.eng.pool, &self.eng.log, &self.ctx)?;
        }
        self.eng.locks.release_all(self.ctx.id);
        self.eng.txns.finish(self.ctx.id);
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.done && !self.eng.is_crashed() {
            let _ = self.abort();
        }
    }
}
