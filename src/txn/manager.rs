use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::*};
use std::sync::{Arc, Mutex};

use crate::sync::lock;
use crate::types::{Lsn, TxnId};
use crate::wal::TxnCtx;

pub struct TxnManager {
    next: AtomicU64,
    active: Mutex<HashMap<TxnId, Arc<TxnCtx>>>,
}

impl TxnManager {
    pub fn new(next: TxnId) -> Self {
        TxnManager { next: AtomicU64::new(next.max(1)), active: Mutex::new(HashMap::new()) }
    }

    pub fn begin(&self) -> Arc<TxnCtx> {
        let id = self.next.fetch_add(1, AcqRel);
        let ctx = Arc::new(TxnCtx::new(id, 0));
        lock(&self.active).insert(id, ctx.clone());
        ctx
    }

    pub fn finish(&self, id: TxnId) {
        lock(&self.active).remove(&id);
    }

    pub fn next_id(&self) -> TxnId {
        self.next.load(Acquire)
    }

    pub fn active_count(&self) -> usize {
        lock(&self.active).len()
    }

    // oldest log record any running transaction might still have to undo
    pub fn min_first_lsn(&self) -> Option<Lsn> {
        lock(&self.active).values().map(|c| c.first_lsn.load(Acquire)).filter(|&l| l != 0).min()
    }

    // read-only transactions have never logged anything and don't need to be known to recovery
    pub fn snapshot(&self) -> Vec<(TxnId, Lsn, bool)> {
        lock(&self.active)
            .values()
            .filter_map(|c| {
                let l = c.last_lsn.load(Acquire);
                (l != 0).then(|| (c.id, l, c.committed.load(Acquire)))
            })
            .collect()
    }
}
