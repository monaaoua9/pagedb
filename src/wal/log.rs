use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::*};
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::record::Record;
use super::TxnCtx;
use crate::buffer::LogFlusher;
use crate::error::{corrupt, Result};
use crate::storage::disk::{read_full_at, write_all_at};
use crate::storage::page::crc32;
use crate::sync::{lock, read, write};
use crate::types::Lsn;

const MAGIC: &[u8; 8] = b"PGDBWAL1";
pub const LOG_START: Lsn = 16;
const FRAME_HDR: u64 = 8;
const AUTO_FLUSH: usize = 4 << 20;

struct Inner {
    file: File,
    // everything below this offset is on disk and fsynced
    durable: u64,
    buf: Vec<u8>,
}

// An LSN is the byte offset of the record's frame, so it doubles as a seek
// address. Frame = len u32 | crc u32 | payload.
pub struct LogManager {
    inner: Mutex<Inner>,
    // mini-transactions hold this shared while they append and stamp pages;
    // a checkpoint takes it exclusive to get a snapshot with nothing half-logged
    gate: RwLock<()>,
    master: PathBuf,
    ckpt: AtomicU64,
    crashed: AtomicBool,
}

fn read_frame(f: &File, off: u64, limit: u64) -> Option<(Vec<u8>, u64)> {
    if off + FRAME_HDR > limit {
        return None;
    }
    let mut h = [0u8; 8];
    if read_full_at(f, &mut h, off).ok()? != 8 {
        return None;
    }
    let len = u32::from_le_bytes(h[0..4].try_into().unwrap()) as u64;
    let crc = u32::from_le_bytes(h[4..8].try_into().unwrap());
    if len == 0 || off + FRAME_HDR + len > limit {
        return None;
    }
    let mut p = vec![0u8; len as usize];
    if read_full_at(f, &mut p, off + FRAME_HDR).ok()? != p.len() {
        return None;
    }
    (crc32(&[&p]) == crc).then_some((p, off + FRAME_HDR + len))
}

impl LogManager {
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        let mut len = file.metadata()?.len();
        if len < LOG_START {
            let mut hdr = [0u8; 16];
            hdr[..8].copy_from_slice(MAGIC);
            file.set_len(0)?;
            write_all_at(&file, &hdr, 0)?;
            file.sync_data()?;
            len = LOG_START;
        } else {
            let mut hdr = [0u8; 8];
            read_full_at(&file, &mut hdr, 0)?;
            if &hdr != MAGIC {
                return corrupt("bad wal magic");
            }
        }

        let mut master = path.to_path_buf();
        master.set_extension("master");
        let ckpt = read_master(&master).filter(|&l| {
            // a master pointing at garbage (or past the end) is ignored
            read_frame(&file, l, len)
                .map(|(p, _)| matches!(Record::decode(&p), Ok(Record::Checkpoint { .. })))
                .unwrap_or(false)
        });

        // find the end of the valid prefix; a torn tail from the last crash is cut off
        let mut end = ckpt.unwrap_or(LOG_START);
        while let Some((_, next)) = read_frame(&file, end, len) {
            end = next;
        }
        if end < len {
            file.set_len(end)?;
            file.sync_data()?;
        }

        Ok(LogManager {
            inner: Mutex::new(Inner { file, durable: end, buf: Vec::new() }),
            gate: RwLock::new(()),
            master,
            ckpt: AtomicU64::new(ckpt.unwrap_or(0)),
            crashed: AtomicBool::new(false),
        })
    }

    pub fn gate_read(&self) -> RwLockReadGuard<'_, ()> {
        read(&self.gate)
    }

    pub fn gate_write(&self) -> RwLockWriteGuard<'_, ()> {
        write(&self.gate)
    }

    pub fn append(&self, rec: &Record) -> Result<Lsn> {
        let p = rec.encode();
        let mut g = lock(&self.inner);
        let lsn = g.durable + g.buf.len() as u64;
        g.buf.extend_from_slice(&(p.len() as u32).to_le_bytes());
        g.buf.extend_from_slice(&crc32(&[&p]).to_le_bytes());
        g.buf.extend_from_slice(&p);
        if g.buf.len() >= AUTO_FLUSH {
            Self::flush_locked(&mut g)?;
        }
        Ok(lsn)
    }

    // chains the record to the txn's previous one. caller already holds the gate
    pub fn append_chained(&self, ctx: &TxnCtx, make: impl FnOnce(Lsn) -> Record) -> Result<Lsn> {
        let rec = make(ctx.last_lsn.load(Acquire));
        let lsn = self.append(&rec)?;
        ctx.last_lsn.store(lsn, Release);
        if matches!(rec, Record::Commit { .. }) {
            ctx.committed.store(true, Release);
        }
        Ok(lsn)
    }

    pub fn append_for(&self, ctx: &TxnCtx, make: impl FnOnce(Lsn) -> Record) -> Result<Lsn> {
        let _g = self.gate_read();
        self.append_chained(ctx, make)
    }

    fn flush_locked(g: &mut Inner) -> Result<()> {
        if g.buf.is_empty() {
            return Ok(());
        }
        if let Err(e) = crate::failpoint::hit() {
            if crate::failpoint::torn() {
                write_all_at(&g.file, &g.buf[..g.buf.len() / 2], g.durable)?;
            }
            return Err(e);
        }
        write_all_at(&g.file, &g.buf, g.durable)?;
        // data is in the file but we may die before learning the sync finished
        crate::failpoint::hit()?;
        g.file.sync_data()?;
        g.durable += g.buf.len() as u64;
        g.buf.clear();
        Ok(())
    }

    // after this returns the record at `lsn` survives power loss. whoever gets
    // the mutex first flushes for everyone queued behind it (group commit)
    pub fn flush_to(&self, lsn: Lsn) -> Result<()> {
        if self.crashed.load(Acquire) {
            return Ok(());
        }
        let mut g = lock(&self.inner);
        if lsn < g.durable {
            return Ok(());
        }
        Self::flush_locked(&mut g)
    }

    pub fn flush_all(&self) -> Result<()> {
        if self.crashed.load(Acquire) {
            return Ok(());
        }
        Self::flush_locked(&mut lock(&self.inner))
    }

    pub fn end_lsn(&self) -> Lsn {
        let g = lock(&self.inner);
        g.durable + g.buf.len() as u64
    }

    pub fn durable_lsn(&self) -> Lsn {
        lock(&self.inner).durable
    }

    pub fn read_at(&self, lsn: Lsn) -> Result<(Record, Lsn)> {
        let g = lock(&self.inner);
        if lsn >= g.durable {
            let off = (lsn - g.durable) as usize;
            if off + 8 > g.buf.len() {
                return corrupt(format!("lsn {lsn} is past the end of the log"));
            }
            let len = u32::from_le_bytes(g.buf[off..off + 4].try_into().unwrap()) as usize;
            let body = &g.buf[off + 8..off + 8 + len];
            return Ok((Record::decode(body)?, lsn + FRAME_HDR + len as u64));
        }
        match read_frame(&g.file, lsn, g.durable) {
            Some((p, next)) => Ok((Record::decode(&p)?, next)),
            None => corrupt(format!("no valid log record at lsn {lsn}")),
        }
    }

    pub fn iter_from(&self, lsn: Lsn) -> LogIter<'_> {
        LogIter { log: self, next: lsn.max(LOG_START) }
    }

    pub fn checkpoint_lsn(&self) -> Lsn {
        self.ckpt.load(Acquire)
    }

    pub fn set_checkpoint(&self, lsn: Lsn) -> Result<()> {
        let mut b = lsn.to_le_bytes().to_vec();
        b.extend_from_slice(&crc32(&[&b]).to_le_bytes());
        crate::failpoint::hit()?;
        let tmp = self.master.with_extension("master.tmp");
        {
            let f = File::create(&tmp)?;
            write_all_at(&f, &b, 0)?;
            f.sync_all()?;
        }
        crate::failpoint::hit()?;
        fs::rename(&tmp, &self.master)?;
        self.ckpt.store(lsn, Release);
        Ok(())
    }

    // test hook: stop touching the disk, as if the process had died right now
    pub fn simulate_crash(&self) {
        self.crashed.store(true, Release);
    }
}

fn read_master(p: &Path) -> Option<Lsn> {
    let b = fs::read(p).ok()?;
    if b.len() != 12 {
        return None;
    }
    let crc = u32::from_le_bytes(b[8..12].try_into().unwrap());
    (crc32(&[&b[..8]]) == crc).then(|| u64::from_le_bytes(b[..8].try_into().unwrap()))
}

impl LogFlusher for LogManager {
    fn flush_to(&self, lsn: Lsn) -> Result<()> {
        LogManager::flush_to(self, lsn)
    }
}

pub struct LogIter<'a> {
    log: &'a LogManager,
    next: Lsn,
}

impl Iterator for LogIter<'_> {
    type Item = Result<(Lsn, Record)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.log.end_lsn() {
            return None;
        }
        let at = self.next;
        match self.log.read_at(at) {
            Ok((r, nx)) => {
                self.next = nx;
                Some(Ok((at, r)))
            }
            Err(e) => {
                self.next = u64::MAX;
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::tests::tmp_path;

    fn commit(t: u64) -> Record {
        Record::Commit { txn: t, prev: 0 }
    }

    #[test]
    fn append_flush_reopen() {
        let path = tmp_path("log-basic");
        let _ = fs::remove_file(path.with_extension("master"));
        let (a, b);
        {
            let l = LogManager::open(&path).unwrap();
            a = l.append(&commit(1)).unwrap();
            b = l.append(&commit(2)).unwrap();
            assert_eq!(l.read_at(a).unwrap().0, commit(1)); // still in the buffer
            l.flush_to(b).unwrap();
        }
        let l = LogManager::open(&path).unwrap();
        let all: Vec<_> = l.iter_from(0).map(|r| r.unwrap()).collect();
        assert_eq!(all, vec![(a, commit(1)), (b, commit(2))]);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn unflushed_tail_is_lost() {
        let path = tmp_path("log-lost");
        let _ = fs::remove_file(path.with_extension("master"));
        {
            let l = LogManager::open(&path).unwrap();
            let a = l.append(&commit(1)).unwrap();
            l.flush_to(a).unwrap();
            l.append(&commit(2)).unwrap();
            l.simulate_crash();
        }
        let l = LogManager::open(&path).unwrap();
        assert_eq!(l.iter_from(0).count(), 1);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn torn_tail_is_truncated() {
        let path = tmp_path("log-torn");
        let _ = fs::remove_file(path.with_extension("master"));
        {
            let l = LogManager::open(&path).unwrap();
            l.append(&commit(1)).unwrap();
            let b = l.append(&commit(2)).unwrap();
            l.flush_to(b).unwrap();
        }
        let len = fs::metadata(&path).unwrap().len();
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(len - 3).unwrap(); // chop into the last frame
        drop(f);

        let l = LogManager::open(&path).unwrap();
        assert_eq!(l.iter_from(0).count(), 1);
        // and appends continue cleanly after the cut
        let c = l.append(&commit(3)).unwrap();
        l.flush_to(c).unwrap();
        drop(l);
        let l = LogManager::open(&path).unwrap();
        assert_eq!(l.iter_from(0).count(), 2);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn flipped_byte_ends_the_log() {
        let path = tmp_path("log-flip");
        let _ = fs::remove_file(path.with_extension("master"));
        let first;
        {
            let l = LogManager::open(&path).unwrap();
            first = l.append(&commit(1)).unwrap();
            let b = l.append(&commit(2)).unwrap();
            l.flush_to(b).unwrap();
        }
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        write_all_at(&f, &[0xAA], first + 8 + 2).unwrap();
        drop(f);
        let l = LogManager::open(&path).unwrap();
        assert_eq!(l.iter_from(0).count(), 0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn master_record_roundtrip() {
        let path = tmp_path("log-master");
        let master = path.with_extension("master");
        let _ = fs::remove_file(&master);
        let c;
        {
            let l = LogManager::open(&path).unwrap();
            c = l.append(&Record::Checkpoint { next_txn: 5, att: vec![], dpt: vec![] }).unwrap();
            l.flush_to(c).unwrap();
            l.set_checkpoint(c).unwrap();
        }
        assert_eq!(LogManager::open(&path).unwrap().checkpoint_lsn(), c);
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(master);
    }
}
