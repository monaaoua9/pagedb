use crate::error::{corrupt, Result};
use crate::types::{Lsn, PageData, PageId, TxnId, PAGE_HEADER, PAGE_SIZE};

// how to take a change back out. logical, not physical: by the time we roll
// back, other transactions may have reshaped the pages this one touched
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Undo {
    None,
    Remove { root: PageId, key: Vec<u8> },
    Restore { root: PageId, key: Vec<u8>, value: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffBody {
    // bytes PAGE_HEADER..PAGE_SIZE
    Full(Vec<u8>),
    // (absolute offset, new bytes)
    Ranges(Vec<(u16, Vec<u8>)>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageDiff {
    pub page: PageId,
    pub body: DiffBody,
}

impl PageDiff {
    pub fn apply(&self, p: &mut PageData) {
        match &self.body {
            DiffBody::Full(b) => p[PAGE_HEADER..].copy_from_slice(b),
            DiffBody::Ranges(rs) => {
                for (off, b) in rs {
                    let o = *off as usize;
                    p[o..o + b.len()].copy_from_slice(b);
                }
            }
        }
    }
}

const GAP: usize = 8;

// first differing offset at or after `i`, PAGE_SIZE if none. skips 64 bytes at a
// time with a slice compare (memcmp) since most of a page is untouched
fn next_diff(a: &PageData, b: &PageData, mut i: usize) -> usize {
    while i + 512 <= PAGE_SIZE && a[i..i + 512] == b[i..i + 512] {
        i += 512;
    }
    while i + 64 <= PAGE_SIZE && a[i..i + 64] == b[i..i + 64] {
        i += 64;
    }
    while i < PAGE_SIZE && a[i] == b[i] {
        i += 1;
    }
    i
}

pub fn diff_ranges(before: &PageData, after: &PageData) -> Vec<(u16, Vec<u8>)> {
    let mut out = Vec::new();
    let mut i = next_diff(before, after, PAGE_HEADER);
    while i < PAGE_SIZE {
        let start = i;
        let mut end = i + 1;
        loop {
            let nd = next_diff(before, after, end);
            if nd < PAGE_SIZE && nd - end < GAP {
                end = nd + 1;
            } else {
                i = nd;
                break;
            }
        }
        out.push((start as u16, after[start..end].to_vec()));
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    // txn 0 is the system: redo only, nothing to undo
    Update { txn: TxnId, prev: Lsn, undo: Undo, diffs: Vec<PageDiff> },
    Clr { txn: TxnId, prev: Lsn, undo_next: Lsn, diffs: Vec<PageDiff> },
    Commit { txn: TxnId, prev: Lsn },
    End { txn: TxnId, prev: Lsn },
    Checkpoint { next_txn: TxnId, att: Vec<(TxnId, Lsn, bool)>, dpt: Vec<(PageId, Lsn)> },
}

pub struct Enc(pub Vec<u8>);

impl Enc {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u16(&mut self, v: u16) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn bytes(&mut self, b: &[u8]) {
        self.u32(b.len() as u32);
        self.0.extend_from_slice(b);
    }
}

pub struct Dec<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Dec<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.b.len() {
            return corrupt("log record truncated");
        }
        let s = &self.b[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Result<Vec<u8>> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
}

fn enc_undo(e: &mut Enc, u: &Undo) {
    match u {
        Undo::None => e.u8(0),
        Undo::Remove { root, key } => {
            e.u8(1);
            e.u32(*root);
            e.bytes(key);
        }
        Undo::Restore { root, key, value } => {
            e.u8(2);
            e.u32(*root);
            e.bytes(key);
            e.bytes(value);
        }
    }
}

fn dec_undo(d: &mut Dec) -> Result<Undo> {
    Ok(match d.u8()? {
        0 => Undo::None,
        1 => Undo::Remove { root: d.u32()?, key: d.bytes()? },
        2 => Undo::Restore { root: d.u32()?, key: d.bytes()?, value: d.bytes()? },
        t => return corrupt(format!("bad undo tag {t}")),
    })
}

fn enc_diffs(e: &mut Enc, ds: &[PageDiff]) {
    e.u16(ds.len() as u16);
    for d in ds {
        e.u32(d.page);
        match &d.body {
            DiffBody::Full(b) => {
                e.u8(0);
                e.bytes(b);
            }
            DiffBody::Ranges(rs) => {
                e.u8(1);
                e.u16(rs.len() as u16);
                for (off, b) in rs {
                    e.u16(*off);
                    e.bytes(b);
                }
            }
        }
    }
}

fn dec_diffs(d: &mut Dec) -> Result<Vec<PageDiff>> {
    let n = d.u16()?;
    let mut out = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let page = d.u32()?;
        let body = match d.u8()? {
            0 => {
                let b = d.bytes()?;
                if b.len() != PAGE_SIZE - PAGE_HEADER {
                    return corrupt("full image has wrong length");
                }
                DiffBody::Full(b)
            }
            1 => {
                let k = d.u16()?;
                let mut rs = Vec::with_capacity(k as usize);
                for _ in 0..k {
                    let off = d.u16()?;
                    let b = d.bytes()?;
                    if (off as usize) < PAGE_HEADER || off as usize + b.len() > PAGE_SIZE {
                        return corrupt("diff range out of bounds");
                    }
                    rs.push((off, b));
                }
                DiffBody::Ranges(rs)
            }
            t => return corrupt(format!("bad diff tag {t}")),
        };
        out.push(PageDiff { page, body });
    }
    Ok(out)
}

impl Record {
    pub fn encode(&self) -> Vec<u8> {
        let mut e = Enc(Vec::with_capacity(64));
        match self {
            Record::Update { txn, prev, undo, diffs } => {
                e.u8(1);
                e.u64(*txn);
                e.u64(*prev);
                enc_undo(&mut e, undo);
                enc_diffs(&mut e, diffs);
            }
            Record::Clr { txn, prev, undo_next, diffs } => {
                e.u8(2);
                e.u64(*txn);
                e.u64(*prev);
                e.u64(*undo_next);
                enc_diffs(&mut e, diffs);
            }
            Record::Commit { txn, prev } => {
                e.u8(3);
                e.u64(*txn);
                e.u64(*prev);
            }
            Record::End { txn, prev } => {
                e.u8(4);
                e.u64(*txn);
                e.u64(*prev);
            }
            Record::Checkpoint { next_txn, att, dpt } => {
                e.u8(5);
                e.u64(*next_txn);
                e.u32(att.len() as u32);
                for (t, l, c) in att {
                    e.u64(*t);
                    e.u64(*l);
                    e.u8(*c as u8);
                }
                e.u32(dpt.len() as u32);
                for (p, l) in dpt {
                    e.u32(*p);
                    e.u64(*l);
                }
            }
        }
        e.0
    }

    pub fn decode(b: &[u8]) -> Result<Record> {
        let mut d = Dec { b, pos: 0 };
        let rec = match d.u8()? {
            1 => Record::Update {
                txn: d.u64()?,
                prev: d.u64()?,
                undo: dec_undo(&mut d)?,
                diffs: dec_diffs(&mut d)?,
            },
            2 => {
                Record::Clr { txn: d.u64()?, prev: d.u64()?, undo_next: d.u64()?, diffs: dec_diffs(&mut d)? }
            }
            3 => Record::Commit { txn: d.u64()?, prev: d.u64()? },
            4 => Record::End { txn: d.u64()?, prev: d.u64()? },
            5 => {
                let next_txn = d.u64()?;
                let na = d.u32()?;
                let mut att = Vec::new();
                for _ in 0..na {
                    att.push((d.u64()?, d.u64()?, d.u8()? != 0));
                }
                let nd = d.u32()?;
                let mut dpt = Vec::new();
                for _ in 0..nd {
                    dpt.push((d.u32()?, d.u64()?));
                }
                Record::Checkpoint { next_txn, att, dpt }
            }
            t => return corrupt(format!("unknown record tag {t}")),
        };
        if d.pos != b.len() {
            return corrupt("trailing bytes in log record");
        }
        Ok(rec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_roundtrip_through_apply() {
        let a = Box::new([0u8; PAGE_SIZE]);
        let mut b = a.clone();
        b[20] = 1;
        b[21] = 2;
        b[500] = 9;
        b[PAGE_SIZE - 1] = 4;
        let rs = diff_ranges(&a, &b);
        assert_eq!(rs.len(), 3);
        let mut c = a.clone();
        PageDiff { page: 0, body: DiffBody::Ranges(rs) }.apply(&mut c);
        assert_eq!(c[PAGE_HEADER..], b[PAGE_HEADER..]);
    }

    #[test]
    fn nearby_changes_merge() {
        let a = Box::new([0u8; PAGE_SIZE]);
        let mut b = a.clone();
        b[100] = 1;
        b[105] = 1;
        assert_eq!(diff_ranges(&a, &b).len(), 1);
        b[200] = 1;
        assert_eq!(diff_ranges(&a, &b).len(), 2);
    }

    #[test]
    fn header_is_never_diffed() {
        let a = Box::new([0u8; PAGE_SIZE]);
        let mut b = a.clone();
        b[3] = 1;
        assert!(diff_ranges(&a, &b).is_empty());
    }

    #[test]
    fn record_roundtrip() {
        let recs = vec![
            Record::Update {
                txn: 7,
                prev: 12,
                undo: Undo::Restore { root: 3, key: b"k".to_vec(), value: b"vv".to_vec() },
                diffs: vec![
                    PageDiff { page: 4, body: DiffBody::Ranges(vec![(40, vec![1, 2, 3])]) },
                    PageDiff { page: 5, body: DiffBody::Full(vec![7; PAGE_SIZE - PAGE_HEADER]) },
                ],
            },
            Record::Clr { txn: 7, prev: 99, undo_next: 12, diffs: vec![] },
            Record::Commit { txn: 7, prev: 100 },
            Record::End { txn: 7, prev: 101 },
            Record::Checkpoint {
                next_txn: 9,
                att: vec![(7, 100, true), (8, 50, false)],
                dpt: vec![(1, 20), (2, 30)],
            },
        ];
        for r in recs {
            assert_eq!(Record::decode(&r.encode()).unwrap(), r);
        }
    }

    #[test]
    fn garbage_is_rejected() {
        assert!(Record::decode(&[]).is_err());
        assert!(Record::decode(&[42]).is_err());
        let mut ok = Record::Commit { txn: 1, prev: 0 }.encode();
        ok.push(0);
        assert!(Record::decode(&ok).is_err());
    }
}
