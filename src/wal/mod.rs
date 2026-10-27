pub mod log;
pub mod mtr;
pub mod record;
pub mod recovery;

use std::sync::atomic::{AtomicBool, AtomicU64};

use crate::types::{Lsn, TxnId};

pub use log::LogManager;
pub use mtr::{Kind, Mtr};
pub use record::{Record, Undo};

// what the log needs to know about a running transaction
pub struct TxnCtx {
    pub id: TxnId,
    pub last_lsn: AtomicU64,
    pub committed: AtomicBool,
}

impl TxnCtx {
    pub fn new(id: TxnId, last_lsn: Lsn) -> Self {
        TxnCtx { id, last_lsn: AtomicU64::new(last_lsn), committed: AtomicBool::new(false) }
    }
}
