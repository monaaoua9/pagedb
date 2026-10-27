use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::*};
use std::sync::Mutex;

use super::page::{stamp_checksum, verify_checksum};
use crate::error::{corrupt, Result};
use crate::sync::lock;
use crate::types::{PageData, PageId, PAGE_SIZE};

#[cfg(unix)]
fn pread(f: &File, buf: &mut [u8], off: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(f, buf, off)
}
#[cfg(unix)]
fn pwrite(f: &File, buf: &[u8], off: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::write_at(f, buf, off)
}
#[cfg(windows)]
fn pread(f: &File, buf: &mut [u8], off: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(f, buf, off)
}
#[cfg(windows)]
fn pwrite(f: &File, buf: &[u8], off: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_write(f, buf, off)
}

// returns bytes actually read, short only at EOF
pub(crate) fn read_full_at(f: &File, buf: &mut [u8], off: u64) -> io::Result<usize> {
    let mut done = 0;
    while done < buf.len() {
        match pread(f, &mut buf[done..], off + done as u64) {
            Ok(0) => break,
            Ok(n) => done += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(done)
}

pub(crate) fn write_all_at(f: &File, buf: &[u8], off: u64) -> io::Result<()> {
    let mut done = 0;
    while done < buf.len() {
        match pwrite(f, &buf[done..], off + done as u64) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => done += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

pub struct DiskManager {
    file: File,
    pages: AtomicU32,
    free: Mutex<Vec<PageId>>,
    lenient: AtomicBool,
    pub reads: AtomicU64,
    pub writes: AtomicU64,
}

impl DiskManager {
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        let len = file.metadata()?.len();
        Ok(DiskManager {
            file,
            pages: AtomicU32::new(len.div_ceil(PAGE_SIZE as u64) as u32),
            // freed pages are only remembered in memory; after a crash they just leak
            free: Mutex::new(Vec::new()),
            lenient: AtomicBool::new(false),
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
        })
    }

    pub fn num_pages(&self) -> u32 {
        self.pages.load(Acquire)
    }

    pub fn allocate(&self) -> PageId {
        if let Some(p) = lock(&self.free).pop() {
            return p;
        }
        self.pages.fetch_add(1, AcqRel)
    }

    pub fn deallocate(&self, pid: PageId) {
        let mut f = lock(&self.free);
        debug_assert!(!f.contains(&pid), "double free of page {pid}");
        f.push(pid);
    }

    // redo can touch pages past the end of the file when the crash lost the extension
    pub fn ensure(&self, pid: PageId) {
        self.pages.fetch_max(pid + 1, AcqRel);
    }

    // while set, a page failing its checksum reads back as zeroes instead of erroring.
    // only recovery flips this; a full-page image in the log is what repairs it
    pub fn set_lenient(&self, on: bool) {
        self.lenient.store(on, Release);
    }

    pub fn read_page(&self, pid: PageId, buf: &mut PageData) -> Result<()> {
        let n = read_full_at(&self.file, buf, pid as u64 * PAGE_SIZE as u64)?;
        buf[n..].fill(0);
        self.reads.fetch_add(1, Relaxed);
        if !verify_checksum(buf) {
            if self.lenient.load(Acquire) {
                buf.fill(0);
            } else {
                return corrupt(format!("page {pid} failed checksum"));
            }
        }
        Ok(())
    }

    pub fn write_page(&self, pid: PageId, buf: &PageData) -> Result<()> {
        // caller only holds a shared latch, so stamp a copy
        let mut tmp = Box::new(*buf);
        stamp_checksum(&mut tmp);
        let off = pid as u64 * PAGE_SIZE as u64;
        if let Err(e) = crate::failpoint::hit() {
            if crate::failpoint::torn() {
                write_all_at(&self.file, &tmp[..PAGE_SIZE / 2], off)?;
            }
            return Err(e);
        }
        write_all_at(&self.file, &tmp[..], off)?;
        self.writes.fetch_add(1, Relaxed);
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        crate::failpoint::hit()?;
        self.file.sync_data()?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::storage::page::{page_lsn, set_page_lsn};

    pub fn tmp_path(tag: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("pagedb-{tag}-{}-{:?}", std::process::id(), std::thread::current().id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn roundtrip_and_zero_fill() {
        let path = tmp_path("disk-rt");
        let d = DiskManager::open(&path).unwrap();
        let a = d.allocate();
        let b = d.allocate();
        assert_eq!((a, b), (0, 1));

        let mut p = Box::new([0u8; PAGE_SIZE]);
        p[500] = 42;
        set_page_lsn(&mut p, 99);
        d.write_page(b, &p).unwrap();

        let mut out = Box::new([1u8; PAGE_SIZE]);
        d.read_page(b, &mut out).unwrap();
        assert_eq!(out[500], 42);
        assert_eq!(page_lsn(&out), 99);

        // page 0 was never written, file has a hole there
        d.read_page(a, &mut out).unwrap();
        assert!(out.iter().all(|&x| x == 0));
        // way past EOF
        d.read_page(77, &mut out).unwrap();
        assert!(out.iter().all(|&x| x == 0));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn corruption_is_caught_unless_lenient() {
        let path = tmp_path("disk-corrupt");
        let d = DiskManager::open(&path).unwrap();
        let mut p = Box::new([0u8; PAGE_SIZE]);
        p[300] = 5;
        d.write_page(0, &p).unwrap();
        write_all_at(&d.file, &[0xFF; 16], 2000).unwrap();

        let mut out = Box::new([0u8; PAGE_SIZE]);
        assert!(d.read_page(0, &mut out).is_err());
        d.set_lenient(true);
        d.read_page(0, &mut out).unwrap();
        assert!(out.iter().all(|&x| x == 0));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn free_list_reuse() {
        let path = tmp_path("disk-free");
        let d = DiskManager::open(&path).unwrap();
        let a = d.allocate();
        let _b = d.allocate();
        d.deallocate(a);
        assert_eq!(d.allocate(), a);
        assert_eq!(d.allocate(), 2);
        let _ = std::fs::remove_file(path);
    }
}
