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
    // lsn of the first record in the file; physical offset = lsn - base + LOG_START
    base: u64,
    // which wal file we're on, bumped each time the front of the log is cut off
    gen: u64,
    // everything below this lsn is on disk and fsynced
    durable: u64,
    buf: Vec<u8>,
}

// An LSN is the byte offset of the record's frame in the logical log, which
// only ever grows. Truncation starts a new file whose header says where it
// begins. Frame = len u32 | crc u32 | payload.
pub struct LogManager {
    inner: Mutex<Inner>,
    path: PathBuf,
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
    (crc32(&[&p]) == crc).then_some((p, FRAME_HDR + len))
}

fn gen_path(path: &Path, gen: u64) -> PathBuf {
    if gen == 0 {
        return path.to_path_buf();
    }
    let mut s = path.as_os_str().to_owned();
    s.push(format!(".{gen}"));
    PathBuf::from(s)
}

fn write_master(path: &Path, lsn: Lsn, gen: u64) -> Result<()> {
    let mut b = lsn.to_le_bytes().to_vec();
    b.extend_from_slice(&gen.to_le_bytes());
    b.extend_from_slice(&crc32(&[&b]).to_le_bytes());
    crate::failpoint::hit()?;
    let tmp = path.with_extension("master.tmp");
    {
        let f = File::create(&tmp)?;
        write_all_at(&f, &b, 0)?;
        f.sync_all()?;
    }
    crate::failpoint::hit()?;
    fs::rename(&tmp, path)?;
    Ok(())
}

impl LogManager {
    pub fn open(path: &Path) -> Result<Self> {
        let mut master = path.to_path_buf();
        master.set_extension("master");
        let (mck, gen) = read_master(&master).unwrap_or((0, 0));

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(gen_path(path, gen))?;
        let mut len = file.metadata()?.len();
        let mut base = LOG_START;
        if len < LOG_START {
            let mut hdr = [0u8; 16];
            hdr[..8].copy_from_slice(MAGIC);
            hdr[8..].copy_from_slice(&LOG_START.to_le_bytes());
            file.set_len(0)?;
            write_all_at(&file, &hdr, 0)?;
            file.sync_data()?;
            len = LOG_START;
        } else {
            let mut hdr = [0u8; 16];
            read_full_at(&file, &mut hdr, 0)?;
            if &hdr[..8] != MAGIC {
                return corrupt("bad wal magic");
            }
            base = u64::from_le_bytes(hdr[8..].try_into().unwrap()).max(LOG_START);
        }
        // leftovers from a truncation that died half way, or an old file we never got to delete
        let _ = fs::remove_file(gen_path(path, gen + 1));
        if gen > 0 {
            let _ = fs::remove_file(gen_path(path, gen - 1));
        }

        let phys = |l: Lsn| l - base + LOG_START;
        let ckpt = (mck != 0).then_some(mck).filter(|&l| {
            // a master pointing at garbage (or past the end) is ignored
            l >= base
                && read_frame(&file, phys(l), len)
                    .map(|(p, _)| matches!(Record::decode(&p), Ok(Record::Checkpoint { .. })))
                    .unwrap_or(false)
        });

        // find the end of the valid prefix; a torn tail from the last crash is cut off
        let mut end = ckpt.map_or(LOG_START, phys);
        while let Some((_, n)) = read_frame(&file, end, len) {
            end += n;
        }
        if end < len {
            file.set_len(end)?;
            file.sync_data()?;
        }

        Ok(LogManager {
            inner: Mutex::new(Inner { file, base, gen, durable: end - LOG_START + base, buf: Vec::new() }),
            path: path.to_path_buf(),
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
        if ctx.first_lsn.load(Acquire) == 0 {
            ctx.first_lsn.store(lsn, Release);
        }
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
                write_all_at(&g.file, &g.buf[..g.buf.len() / 2], g.durable - g.base + LOG_START)?;
            }
            return Err(e);
        }
        write_all_at(&g.file, &g.buf, g.durable - g.base + LOG_START)?;
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
        if lsn < g.base {
            return corrupt(format!("lsn {lsn} was truncated away"));
        }
        let limit = g.durable - g.base + LOG_START;
        match read_frame(&g.file, lsn - g.base + LOG_START, limit) {
            Some((p, n)) => Ok((Record::decode(&p)?, lsn + n)),
            None => corrupt(format!("no valid log record at lsn {lsn}")),
        }
    }

    pub fn iter_from(&self, lsn: Lsn) -> LogIter<'_> {
        LogIter { log: self, next: lsn.max(lock(&self.inner).base) }
    }

    pub fn checkpoint_lsn(&self) -> Lsn {
        self.ckpt.load(Acquire)
    }

    pub fn set_checkpoint(&self, lsn: Lsn) -> Result<()> {
        let gen = lock(&self.inner).gen;
        write_master(&self.master, lsn, gen)?;
        self.ckpt.store(lsn, Release);
        Ok(())
    }

    pub fn base_lsn(&self) -> Lsn {
        lock(&self.inner).base
    }

    // Drops everything before `keep` (a record boundary) by copying the rest into a
    // new file and switching the master record over to it. No rename over an open
    // file, which Windows doesn't like; a crash anywhere leaves either the old or
    // the new file as the one the master names, and open() deletes the other.
    pub fn truncate_before(&self, keep: Lsn) -> Result<()> {
        let mut g = lock(&self.inner);
        if keep <= g.base || self.crashed.load(Acquire) {
            return Ok(());
        }
        Self::flush_locked(&mut g)?;
        let ngen = g.gen + 1;
        let npath = gen_path(&self.path, ngen);
        let nf = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&npath)?;
        let mut hdr = [0u8; 16];
        hdr[..8].copy_from_slice(MAGIC);
        hdr[8..].copy_from_slice(&keep.to_le_bytes());
        write_all_at(&nf, &hdr, 0)?;

        let (mut src, end) = (keep - g.base + LOG_START, g.durable - g.base + LOG_START);
        let mut dst = LOG_START;
        let mut chunk = vec![0u8; 1 << 20];
        while src < end {
            let n = chunk.len().min((end - src) as usize);
            if read_full_at(&g.file, &mut chunk[..n], src)? != n {
                return corrupt("short read while truncating the log");
            }
            write_all_at(&nf, &chunk[..n], dst)?;
            src += n as u64;
            dst += n as u64;
        }
        crate::failpoint::hit()?;
        nf.sync_all()?;
        write_master(&self.master, self.ckpt.load(Acquire), ngen)?;

        let old = gen_path(&self.path, g.gen);
        g.file = nf;
        g.base = keep;
        g.gen = ngen;
        let _ = fs::remove_file(old);
        Ok(())
    }

    // test hook: stop touching the disk, as if the process had died right now
    pub fn simulate_crash(&self) {
        self.crashed.store(true, Release);
    }
}

fn read_master(p: &Path) -> Option<(Lsn, u64)> {
    let b = fs::read(p).ok()?;
    let n = match b.len() {
        12 | 20 => b.len() - 4,
        _ => return None,
    };
    let crc = u32::from_le_bytes(b[n..].try_into().unwrap());
    if crc32(&[&b[..n]]) != crc {
        return None;
    }
    let gen = if n == 16 { u64::from_le_bytes(b[8..16].try_into().unwrap()) } else { 0 };
    Some((u64::from_le_bytes(b[..8].try_into().unwrap()), gen))
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
