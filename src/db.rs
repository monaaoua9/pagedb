use std::collections::HashMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::*};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use crate::btree::node::{Node, KIND_LEAF};
use crate::btree::{BTree, LogMode, TreeStats};
use crate::buffer::{BufferPool, PoolStats};
use crate::error::{corrupt, Error, Result};
use crate::storage::DiskManager;
use crate::sync::{lock, read, write};
use crate::txn::{LockManager, Transaction, TxnManager};
use crate::types::{PageId, INVALID_PAGE, PAGE_HEADER};
use crate::wal::recovery::{recover, RecoveryReport};
use crate::wal::{Kind, LogManager, Mtr, Record};

const MAGIC: &[u8; 8] = b"PAGEDB01";
const CATALOG_ROOT: PageId = 1;
const MAX_TABLE_NAME: usize = 255;

#[derive(Clone, Debug)]
pub struct Options {
    pub pool_pages: usize,
    pub lock_timeout: Duration,
    // log growth between automatic checkpoints
    pub checkpoint_bytes: u64,
    // a checkpoint cuts the front off the log once at least this much of it is dead
    pub truncate_bytes: u64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            pool_pages: 1024,
            lock_timeout: Duration::from_secs(5),
            checkpoint_bytes: 16 << 20,
            truncate_bytes: 1 << 20,
        }
    }
}

#[derive(Clone)]
pub struct Table {
    name: Arc<str>,
    tree: BTree,
}

impl Table {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn root(&self) -> PageId {
        self.tree.root()
    }
    pub(crate) fn tree(&self) -> &BTree {
        &self.tree
    }
}

pub(crate) struct Engine {
    pub disk: Arc<DiskManager>,
    pub pool: Arc<BufferPool>,
    pub log: Arc<LogManager>,
    pub locks: LockManager,
    pub txns: TxnManager,
    catalog: BTree,
    tables: RwLock<HashMap<String, Table>>,
    ddl: Mutex<()>,
    opts: Options,
    ckpt_running: AtomicBool,
    ckpt_at: AtomicU64,
    crashed: AtomicBool,
    recovery: RecoveryReport,
}

impl Engine {
    pub fn is_crashed(&self) -> bool {
        self.crashed.load(Acquire)
    }

    pub fn checkpoint(&self) -> Result<()> {
        if self.ckpt_running.swap(true, AcqRel) {
            return Ok(());
        }
        let r = self.checkpoint_inner();
        self.ckpt_running.store(false, Release);
        r
    }

    // Fuzzy: dirty pages are flushed first only to keep the dirty page table
    // (and so the redo window) short. The record itself is a snapshot taken
    // with the log gate closed so no mini-transaction is half-logged.
    fn checkpoint_inner(&self) -> Result<()> {
        self.pool.flush_all()?;
        let (lsn, keep) = {
            let _gate = self.log.gate_write();
            let att = self.txns.snapshot();
            let dpt = self.pool.dirty_page_table();
            // recovery from this checkpoint needs redo from the oldest dirty page and
            // undo back to the first record of every transaction still running
            let oldest = dpt.iter().map(|d| d.1).chain(self.txns.min_first_lsn()).min();
            let lsn = self.log.append(&Record::Checkpoint { next_txn: self.txns.next_id(), att, dpt })?;
            (lsn, oldest.map_or(lsn, |o| o.min(lsn)))
        };
        // writes of pages that looked clean in the snapshot must be on disk
        // before the master record starts trusting the snapshot
        self.disk.sync()?;
        self.log.flush_to(lsn)?;
        self.log.set_checkpoint(lsn)?;
        if keep - self.log.base_lsn() >= self.opts.truncate_bytes.max(1) {
            self.log.truncate_before(keep)?;
        }
        self.ckpt_at.store(self.log.end_lsn(), Release);
        Ok(())
    }

    pub fn maybe_checkpoint(&self) {
        if self.log.end_lsn().saturating_sub(self.ckpt_at.load(Acquire)) > self.opts.checkpoint_bytes {
            let _ = self.checkpoint();
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if !self.is_crashed() {
            let _ = self.checkpoint();
        }
    }
}

#[derive(Clone)]
pub struct Database {
    eng: Arc<Engine>,
}

impl Database {
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Database> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let disk = Arc::new(DiskManager::open(&dir.join("data.db"))?);
        let pool = Arc::new(BufferPool::new(disk.clone(), opts.pool_pages));
        let log = Arc::new(LogManager::open(&dir.join("wal.log"))?);
        pool.set_flusher(log.clone());

        let rep = recover(&disk, &pool, &log)?;

        if disk.num_pages() == 0 {
            bootstrap(&pool, &log)?;
        } else {
            let g = pool.fetch_read(0)?;
            if &g[PAGE_HEADER..PAGE_HEADER + 8] != MAGIC {
                return corrupt("data file has no valid meta page");
            }
        }

        pool.set_freelist(true);
        let catalog = BTree::open(CATALOG_ROOT, pool.clone(), log.clone());
        let mut tables = HashMap::new();
        for (k, v) in catalog.scan(Bound::Unbounded, Bound::Unbounded)? {
            let name = String::from_utf8(k).map_err(|_| Error::Corrupt("table name is not utf-8".into()))?;
            let root =
                PageId::from_le_bytes(v.try_into().map_err(|_| Error::Corrupt("bad catalog entry".into()))?);
            let tree = BTree::open(root, pool.clone(), log.clone());
            tables.insert(name.clone(), Table { name: name.into(), tree });
        }

        let eng = Arc::new(Engine {
            locks: LockManager::new(opts.lock_timeout),
            txns: TxnManager::new(rep.next_txn),
            disk,
            pool,
            log,
            catalog,
            tables: RwLock::new(tables),
            ddl: Mutex::new(()),
            opts,
            ckpt_running: AtomicBool::new(false),
            ckpt_at: AtomicU64::new(0),
            crashed: AtomicBool::new(false),
            recovery: rep,
        });
        // start from a clean slate so the next recovery doesn't replay this one
        eng.checkpoint()?;
        Ok(Database { eng })
    }

    pub fn begin(&self) -> Transaction {
        Transaction::new(self.eng.clone())
    }

    pub fn table(&self, name: &str) -> Result<Table> {
        read(&self.eng.tables).get(name).cloned().ok_or_else(|| Error::NoSuchTable(name.to_string()))
    }

    pub fn table_names(&self) -> Vec<String> {
        let mut v: Vec<String> = read(&self.eng.tables).keys().cloned().collect();
        v.sort();
        v
    }

    pub fn create_table(&self, name: &str) -> Result<Table> {
        if name.is_empty() || name.len() > MAX_TABLE_NAME {
            return Err(Error::KeyTooLarge);
        }
        let _ddl = lock(&self.eng.ddl);
        if read(&self.eng.tables).contains_key(name) {
            return Err(Error::TableExists(name.to_string()));
        }
        let tree = BTree::create(self.eng.pool.clone(), self.eng.log.clone())?;
        self.eng.catalog.put(name.as_bytes(), &tree.root().to_le_bytes(), &LogMode::System)?;
        // catalog entry has to be durable before anyone can commit rows into the table
        self.eng.log.flush_all()?;
        let t = Table { name: name.into(), tree };
        write(&self.eng.tables).insert(name.to_string(), t.clone());
        Ok(t)
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.eng.checkpoint()
    }

    pub fn recovery_report(&self) -> &RecoveryReport {
        &self.eng.recovery
    }

    pub fn pool_stats(&self) -> PoolStats {
        self.eng.pool.stats()
    }

    // structural check of every tree; only meaningful with no writers running
    pub fn verify(&self) -> Result<Vec<(String, TreeStats)>> {
        let mut out = vec![("<catalog>".to_string(), self.eng.catalog.check()?)];
        for name in self.table_names() {
            out.push((name.clone(), self.table(&name)?.tree.check()?));
        }
        Ok(out)
    }

    // Drops everything in memory on the floor without flushing a byte, which is
    // what the on-disk state looks like after a power cut. For tests and demos.
    pub fn simulate_crash(self) {
        self.eng.crashed.store(true, Release);
        self.eng.log.simulate_crash();
    }
}

fn bootstrap(pool: &Arc<BufferPool>, log: &Arc<LogManager>) -> Result<()> {
    let mut m = Mtr::new(pool, log);
    let meta = m.alloc()?;
    let cat = m.alloc()?;
    if m.pid(meta) != 0 || m.pid(cat) != CATALOG_ROOT {
        return corrupt("fresh database did not allocate pages 0 and 1");
    }
    let p = m.page_mut(meta);
    p[PAGE_HEADER..PAGE_HEADER + 8].copy_from_slice(MAGIC);
    p[PAGE_HEADER + 8..PAGE_HEADER + 12].copy_from_slice(&INVALID_PAGE.to_le_bytes());
    Node(m.page_mut(cat)).init(KIND_LEAF, 0, INVALID_PAGE);
    m.commit(Kind::System)?;
    log.flush_all()?;
    pool.flush_all()
}
