use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::*};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};

use super::replacer::LruK;
use crate::error::{Error, Result};
use crate::storage::page::{page_lsn, set_page_lsn};
use crate::storage::DiskManager;
use crate::sync::{lock, read, write};
use crate::types::{Lsn, PageData, PageId, PAGE_SIZE};

pub trait LogFlusher: Send + Sync {
    fn flush_to(&self, lsn: Lsn) -> Result<()>;
}

struct Frame {
    data: RwLock<Box<PageData>>,
    pid: AtomicU32,
    pins: AtomicU32,
    dirty: AtomicBool,
    // lsn of the first record that dirtied this frame since it was last clean; 0 = none
    rec_lsn: AtomicU64,
}

struct State {
    table: HashMap<PageId, usize, BuildHasherDefault<PageHash>>,
    free: Vec<usize>,
    lru: LruK,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PoolStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

// Metadata (page table, replacer) sits behind one mutex and disk reads/evictions
// happen under it. Simple and obviously correct; sharding the table is the next
// step if this ever shows up in a profile.
// page ids are small dense integers and the table sits under the pool mutex on every
// fetch, so SipHash is wasted effort here
#[derive(Default)]
struct PageHash(u64);

impl Hasher for PageHash {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, b: &[u8]) {
        for &x in b {
            self.0 = (self.0 ^ x as u64).wrapping_mul(0x100_0000_01b3);
        }
    }
    fn write_u32(&mut self, n: u32) {
        self.0 = (n as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}

pub struct BufferPool {
    disk: Arc<DiskManager>,
    frames: Box<[Frame]>,
    state: Mutex<State>,
    flusher: RwLock<Option<Arc<dyn LogFlusher>>>,
    freelist: AtomicBool,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl BufferPool {
    pub fn new(disk: Arc<DiskManager>, size: usize) -> Self {
        let frames: Vec<Frame> = (0..size)
            .map(|_| Frame {
                data: RwLock::new(Box::new([0u8; PAGE_SIZE])),
                pid: AtomicU32::new(u32::MAX),
                pins: AtomicU32::new(0),
                dirty: AtomicBool::new(false),
                rec_lsn: AtomicU64::new(0),
            })
            .collect();
        BufferPool {
            disk,
            frames: frames.into_boxed_slice(),
            state: Mutex::new(State {
                table: HashMap::default(),
                free: (0..size).rev().collect(),
                lru: LruK::new(size, 2),
            }),
            flusher: RwLock::new(None),
            freelist: AtomicBool::new(false),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    pub fn disk(&self) -> &Arc<DiskManager> {
        &self.disk
    }

    // once on, page 0 holds the head of a persistent free list that Mtr maintains
    pub fn set_freelist(&self, on: bool) {
        self.freelist.store(on, Release);
    }

    pub fn freelist_enabled(&self) -> bool {
        self.freelist.load(Acquire)
    }

    pub fn set_flusher(&self, f: Arc<dyn LogFlusher>) {
        *write(&self.flusher) = Some(f);
    }

    pub fn stats(&self) -> PoolStats {
        PoolStats {
            hits: self.hits.load(Relaxed),
            misses: self.misses.load(Relaxed),
            evictions: self.evictions.load(Relaxed),
        }
    }

    pub fn capacity(&self) -> usize {
        self.frames.len()
    }

    // pins the page's frame, loading it if needed. caller owes one unpin
    fn pin(&self, pid: PageId) -> Result<usize> {
        let mut st = lock(&self.state);
        if let Some(&f) = st.table.get(&pid) {
            self.frames[f].pins.fetch_add(1, AcqRel);
            st.lru.access(f);
            self.hits.fetch_add(1, Relaxed);
            return Ok(f);
        }
        self.misses.fetch_add(1, Relaxed);
        let f = self.claim_frame(&mut st)?;
        let fr = &self.frames[f];
        {
            // frame is unmapped and unpinned, nobody else can be holding this
            let mut d = write(&fr.data);
            if let Err(e) = self.disk.read_page(pid, &mut d) {
                drop(d);
                st.free.push(f);
                return Err(e);
            }
        }
        self.install(&mut st, f, pid);
        Ok(f)
    }

    fn install(&self, st: &mut State, f: usize, pid: PageId) {
        let fr = &self.frames[f];
        fr.pid.store(pid, Release);
        fr.dirty.store(false, Release);
        fr.rec_lsn.store(0, Release);
        fr.pins.store(1, Release);
        st.table.insert(pid, f);
        st.lru.access(f);
    }

    fn claim_frame(&self, st: &mut State) -> Result<usize> {
        if let Some(f) = st.free.pop() {
            return Ok(f);
        }
        let f = st.lru.victim(|f| self.frames[f].pins.load(Acquire) > 0).ok_or(Error::PoolExhausted)?;
        let fr = &self.frames[f];
        if fr.dirty.load(Acquire) {
            let d = read(&fr.data);
            self.write_out(fr, &d)?;
        }
        st.table.remove(&fr.pid.load(Acquire));
        st.lru.remove(f);
        self.evictions.fetch_add(1, Relaxed);
        Ok(f)
    }

    // WAL rule lives here: the log must be durable up to the page's lsn before the page goes out
    fn write_out(&self, fr: &Frame, d: &PageData) -> Result<()> {
        let lsn = page_lsn(d);
        if lsn != 0 {
            if let Some(fl) = read(&self.flusher).as_ref() {
                fl.flush_to(lsn)?;
            }
        }
        self.disk.write_page(fr.pid.load(Acquire), d)?;
        // rec_lsn stays set until the write is done so a concurrent checkpoint
        // never sees the page as clean while it's still in flight
        fr.dirty.store(false, Release);
        fr.rec_lsn.store(0, Release);
        Ok(())
    }

    pub fn fetch_read(&self, pid: PageId) -> Result<ReadGuard<'_>> {
        let f = self.pin(pid)?;
        let g = read(&self.frames[f].data);
        Ok(ReadGuard { pool: self, f, pid, g: ManuallyDrop::new(g) })
    }

    // None when the latch is taken; callers use this to avoid blocking while already holding a latch
    pub fn try_fetch_read(&self, pid: PageId) -> Result<Option<ReadGuard<'_>>> {
        let f = self.pin(pid)?;
        let g = match self.frames[f].data.try_read() {
            Ok(g) => g,
            Err(TryLockError::Poisoned(e)) => e.into_inner(),
            Err(TryLockError::WouldBlock) => {
                self.frames[f].pins.fetch_sub(1, AcqRel);
                return Ok(None);
            }
        };
        Ok(Some(ReadGuard { pool: self, f, pid, g: ManuallyDrop::new(g) }))
    }

    pub fn fetch_write(&self, pid: PageId) -> Result<WriteGuard<'_>> {
        let f = self.pin(pid)?;
        let g = write(&self.frames[f].data);
        let was_dirty = self.frames[f].dirty.load(Acquire);
        Ok(WriteGuard::new(self, f, pid, g, false, was_dirty))
    }

    pub fn new_page(&self) -> Result<WriteGuard<'_>> {
        let pid = self.disk.allocate();
        let mut st = lock(&self.state);
        let f = match self.claim_frame(&mut st) {
            Ok(f) => f,
            Err(e) => {
                self.disk.deallocate(pid);
                return Err(e);
            }
        };
        let mut g = write(&self.frames[f].data);
        g.fill(0);
        self.install(&mut st, f, pid);
        Ok(WriteGuard::new(self, f, pid, g, true, false))
    }

    // drops the page from the pool without writing it and returns the id to the allocator
    pub fn free_page(&self, pid: PageId) {
        let mut st = lock(&self.state);
        if let Some(&f) = st.table.get(&pid) {
            let fr = &self.frames[f];
            if fr.pins.load(Acquire) > 0 {
                // a flusher has it pinned for a moment; leaking the id beats racing it
                return;
            }
            st.table.remove(&pid);
            st.lru.remove(f);
            fr.dirty.store(false, Release);
            fr.rec_lsn.store(0, Release);
            st.free.push(f);
        }
        self.disk.deallocate(pid);
    }

    pub fn flush_page(&self, pid: PageId) -> Result<()> {
        let f = {
            let st = lock(&self.state);
            match st.table.get(&pid) {
                Some(&f) => {
                    self.frames[f].pins.fetch_add(1, AcqRel);
                    f
                }
                None => return Ok(()),
            }
        };
        // latch taken after the metadata lock is gone, otherwise a writer
        // waiting on that lock while holding this latch would deadlock us
        let r = {
            let d = read(&self.frames[f].data);
            if self.frames[f].dirty.load(Acquire) {
                self.write_out(&self.frames[f], &d)
            } else {
                Ok(())
            }
        };
        self.frames[f].pins.fetch_sub(1, AcqRel);
        r
    }

    pub fn flush_all(&self) -> Result<()> {
        let pids: Vec<PageId> = {
            let st = lock(&self.state);
            st.table.iter().filter(|(_, &f)| self.frames[f].dirty.load(Acquire)).map(|(&p, _)| p).collect()
        };
        for p in pids {
            self.flush_page(p)?;
        }
        self.disk.sync()
    }

    pub fn dirty_page_table(&self) -> Vec<(PageId, Lsn)> {
        let st = lock(&self.state);
        st.table
            .iter()
            .filter_map(|(&p, &f)| {
                let r = self.frames[f].rec_lsn.load(Acquire);
                (r != 0).then_some((p, r))
            })
            .collect()
    }

    pub fn pinned_count(&self) -> usize {
        self.frames.iter().filter(|f| f.pins.load(Acquire) > 0).count()
    }
}

pub struct ReadGuard<'a> {
    pool: &'a BufferPool,
    f: usize,
    pid: PageId,
    g: ManuallyDrop<RwLockReadGuard<'a, Box<PageData>>>,
}

impl ReadGuard<'_> {
    pub fn page_id(&self) -> PageId {
        self.pid
    }
}

impl Deref for ReadGuard<'_> {
    type Target = PageData;
    fn deref(&self) -> &PageData {
        &self.g
    }
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: dropped exactly once, here. latch has to go before the unpin,
        // otherwise the frame could be recycled while we still hold its lock
        unsafe { ManuallyDrop::drop(&mut self.g) };
        self.pool.frames[self.f].pins.fetch_sub(1, AcqRel);
    }
}

pub struct WriteGuard<'a> {
    pool: &'a BufferPool,
    f: usize,
    pid: PageId,
    g: ManuallyDrop<RwLockWriteGuard<'a, Box<PageData>>>,
    before: Option<Box<PageData>>,
    touched: bool,
    fresh: bool,
    was_dirty: bool,
}

impl<'a> WriteGuard<'a> {
    fn new(
        pool: &'a BufferPool,
        f: usize,
        pid: PageId,
        g: RwLockWriteGuard<'a, Box<PageData>>,
        fresh: bool,
        was_dirty: bool,
    ) -> Self {
        WriteGuard { pool, f, pid, g: ManuallyDrop::new(g), before: None, touched: fresh, fresh, was_dirty }
    }

    pub fn page_id(&self) -> PageId {
        self.pid
    }

    pub fn touched(&self) -> bool {
        self.touched
    }

    // true when a torn write could have left this page unreadable, so the log
    // has to carry the whole image instead of a delta
    pub fn needs_full_image(&self) -> bool {
        self.fresh || !self.was_dirty
    }

    pub fn before(&self) -> Option<&PageData> {
        self.before.as_deref()
    }

    // called by the mini-transaction once the redo record is in the log
    pub fn stamp(&mut self, lsn: Lsn) {
        set_page_lsn(&mut self.g, lsn);
        let fr = &self.pool.frames[self.f];
        fr.dirty.store(true, Release);
        if fr.rec_lsn.load(Acquire) == 0 {
            fr.rec_lsn.store(lsn, Release);
        }
        self.touched = false;
        if let Some(b) = self.before.take() {
            recycle(b);
        }
        self.fresh = false;
    }

    // undo in-memory changes of an operation that never made it to the log
    // recycled page: contents are dead, treat it like a brand new one for logging
    pub fn make_fresh(&mut self) {
        self.g.fill(0);
        self.before = None;
        self.fresh = true;
        self.touched = true;
    }

    pub fn revert(&mut self) {
        if !self.touched {
            return;
        }
        match self.before.take() {
            Some(b) => ***self.g = *b,
            None => self.g.fill(0),
        }
        self.touched = false;
    }
}

impl Deref for WriteGuard<'_> {
    type Target = PageData;
    fn deref(&self) -> &PageData {
        &self.g
    }
}

impl DerefMut for WriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut PageData {
        if !self.touched {
            let mut b = SPARE.with(|s| s.borrow_mut().pop()).unwrap_or_else(|| Box::new([0u8; PAGE_SIZE]));
            b.copy_from_slice(&***self.g);
            self.before = Some(b);
            self.touched = true;
        }
        &mut self.g
    }
}

thread_local! {
    // before-images are 8 KiB and one is taken per page per mini-transaction
    static SPARE: std::cell::RefCell<Vec<Box<PageData>>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn recycle(b: Box<PageData>) {
    SPARE.with(|s| {
        let mut s = s.borrow_mut();
        if s.len() < 8 {
            s.push(b);
        }
    });
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        if let Some(b) = self.before.take() {
            recycle(b);
        }
        let fr = &self.pool.frames[self.f];
        // raw users of the pool (no mini-transaction) just want dirty tracking
        if self.touched {
            fr.dirty.store(true, Release);
        }
        // SAFETY: see ReadGuard::drop
        unsafe { ManuallyDrop::drop(&mut self.g) };
        fr.pins.fetch_sub(1, AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::tests::tmp_path;

    fn pool(tag: &str, n: usize) -> (BufferPool, std::path::PathBuf) {
        let path = tmp_path(tag);
        let disk = Arc::new(DiskManager::open(&path).unwrap());
        (BufferPool::new(disk, n), path)
    }

    #[test]
    fn dirty_pages_survive_eviction() {
        let (bp, path) = pool("bp-evict", 3);
        let mut ids = vec![];
        for i in 0..10u8 {
            let mut g = bp.new_page().unwrap();
            g[100] = i + 1;
            ids.push(g.page_id());
        }
        for (i, &p) in ids.iter().enumerate() {
            let g = bp.fetch_read(p).unwrap();
            assert_eq!(g[100], i as u8 + 1);
        }
        assert!(bp.stats().evictions > 0);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn all_pinned_is_an_error_not_a_hang() {
        let (bp, path) = pool("bp-full", 2);
        let a = bp.new_page().unwrap();
        let b = bp.new_page().unwrap();
        assert!(matches!(bp.new_page(), Err(Error::PoolExhausted)));
        drop(a);
        let c = bp.new_page().unwrap();
        drop((b, c));
        assert_eq!(bp.pinned_count(), 0);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn guards_unpin_on_drop() {
        let (bp, path) = pool("bp-pins", 4);
        let id = bp.new_page().unwrap().page_id();
        let r1 = bp.fetch_read(id).unwrap();
        let r2 = bp.fetch_read(id).unwrap();
        assert_eq!(bp.pinned_count(), 1);
        drop(r1);
        assert_eq!(bp.pinned_count(), 1);
        drop(r2);
        assert_eq!(bp.pinned_count(), 0);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn try_read_backs_off_when_write_latched() {
        let (bp, path) = pool("bp-try", 4);
        let w = bp.new_page().unwrap();
        let id = w.page_id();
        assert!(bp.try_fetch_read(id).unwrap().is_none());
        assert_eq!(bp.pinned_count(), 1);
        drop(w);
        assert!(bp.try_fetch_read(id).unwrap().is_some());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn revert_restores_the_page() {
        let (bp, path) = pool("bp-revert", 4);
        let id = {
            let mut g = bp.new_page().unwrap();
            g[50] = 9;
            g.stamp(1);
            g.page_id()
        };
        let mut g = bp.fetch_write(id).unwrap();
        g[50] = 77;
        g.revert();
        assert_eq!(g[50], 9);
        drop(g);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn flush_writes_through_and_clears_dirty() {
        let (bp, path) = pool("bp-flush", 4);
        let id = {
            let mut g = bp.new_page().unwrap();
            g[100] = 3;
            g.stamp(5);
            g.page_id()
        };
        assert_eq!(bp.dirty_page_table(), vec![(id, 5)]);
        bp.flush_all().unwrap();
        assert!(bp.dirty_page_table().is_empty());
        let mut raw = Box::new([0u8; PAGE_SIZE]);
        bp.disk().read_page(id, &mut raw).unwrap();
        assert_eq!(raw[100], 3);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn concurrent_fetches_of_few_pages() {
        let (bp, path) = pool("bp-conc", 8);
        let bp = Arc::new(bp);
        let ids: Vec<PageId> = (0..6)
            .map(|i| {
                let mut g = bp.new_page().unwrap();
                g[200] = i;
                g.page_id()
            })
            .collect();
        let hs: Vec<_> = (0..4)
            .map(|t| {
                let bp = bp.clone();
                let ids = ids.clone();
                std::thread::spawn(move || {
                    for r in 0..500usize {
                        let i = (r * 7 + t) % ids.len();
                        let g = bp.fetch_read(ids[i]).unwrap();
                        assert_eq!(g[200], i as u8);
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        let _ = std::fs::remove_file(path);
    }
}
