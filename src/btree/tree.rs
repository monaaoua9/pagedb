use std::ops::Bound;
use std::sync::Arc;

use super::node::*;
use crate::buffer::{BufferPool, ReadGuard};
use crate::error::{corrupt, Error, Result};
use crate::types::{Lsn, PageData, PageId, INVALID_PAGE};
use crate::wal::{Kind, LogManager, Mtr, TxnCtx, Undo};

pub enum LogMode<'a> {
    System,
    Txn(&'a TxnCtx),
    Clr(&'a TxnCtx, Lsn),
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TreeStats {
    pub height: usize,
    pub leaves: usize,
    pub internals: usize,
    pub entries: usize,
}

// The root page id never changes: when the root splits its contents move into
// two fresh children and the root is rewritten in place. That keeps the catalog
// entry for a table valid forever.
//
// Writers use latch crabbing: X-latch down the path and drop every ancestor as
// soon as a node is "safe" (can absorb the worst case below it). Most inserts
// first try an optimistic pass that only X-latches the leaf.
#[derive(Clone)]
pub struct BTree {
    root: PageId,
    pool: Arc<BufferPool>,
    log: Arc<LogManager>,
}

fn split_by_size(sizes: &[usize]) -> usize {
    let tot: usize = sizes.iter().sum();
    let mut acc = 0;
    for (i, s) in sizes.iter().enumerate() {
        acc += s;
        if acc * 2 >= tot {
            return (i + 1).clamp(1, sizes.len() - 1);
        }
    }
    sizes.len() - 1
}

// replacing a value frees the old cell first, so it only has to fit net of that
fn leaf_fits(n: &Node<&PageData>, found: std::result::Result<usize, usize>, need: usize) -> bool {
    match found {
        Ok(i) => n.free_bytes() + n.cell_len(i) + 2 >= need,
        Err(_) => n.free_bytes() >= need,
    }
}

impl BTree {
    pub fn open(root: PageId, pool: Arc<BufferPool>, log: Arc<LogManager>) -> Self {
        BTree { root, pool, log }
    }

    pub fn create(pool: Arc<BufferPool>, log: Arc<LogManager>) -> Result<Self> {
        let root = {
            let mut m = Mtr::new(&pool, &log);
            let i = m.alloc()?;
            let pid = m.pid(i);
            Node(m.page_mut(i)).init(KIND_LEAF, 0, INVALID_PAGE);
            m.commit(Kind::System)?;
            pid
        };
        Ok(BTree { root, pool, log })
    }

    pub fn root(&self) -> PageId {
        self.root
    }

    // ---------- reads ----------

    fn find_leaf(&self, key: &[u8]) -> Result<ReadGuard<'_>> {
        let mut g = self.pool.fetch_read(self.root)?;
        loop {
            let (leaf, next) = {
                let n = Node(&*g);
                if n.is_leaf() {
                    (true, 0)
                } else {
                    (false, n.child(n.child_index(key)))
                }
            };
            if leaf {
                return Ok(g);
            }
            // hand-over-hand: child is latched before the parent is let go
            g = self.pool.fetch_read(next)?;
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let g = self.find_leaf(key)?;
        let n = Node(&*g);
        Ok(n.search(key).ok().map(|i| n.val(i).to_vec()))
    }

    // f runs with a leaf latch held, so keep it cheap and don't call back into the tree
    pub fn scan_with<F>(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        let mut start: Bound<Vec<u8>> = match lower {
            Bound::Included(k) => Bound::Included(k.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let seek_key: Vec<u8> = match &start {
            Bound::Included(k) | Bound::Excluded(k) => k.clone(),
            Bound::Unbounded => Vec::new(),
        };
        let mut g = self.find_leaf(&seek_key)?;
        loop {
            let (next, last) = {
                let n = Node(&*g);
                let from = match &start {
                    Bound::Unbounded => 0,
                    Bound::Included(k) => n.search(k).unwrap_or_else(|i| i),
                    Bound::Excluded(k) => match n.search(k) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    },
                };
                let mut last = None;
                for i in from..n.len() {
                    let k = n.key(i);
                    let past = match upper {
                        Bound::Included(u) => k > u,
                        Bound::Excluded(u) => k >= u,
                        Bound::Unbounded => false,
                    };
                    if past || !f(k, n.val(i)) {
                        return Ok(());
                    }
                    last = Some(i);
                }
                (n.link(), last.map(|i| n.key(i).to_vec()))
            };
            if let Some(l) = last {
                start = Bound::Excluded(l);
            }
            if next == INVALID_PAGE {
                return Ok(());
            }
            // Never block on the sibling while holding this leaf: a writer that
            // holds the sibling and wants ours (merge) would deadlock with us.
            match self.pool.try_fetch_read(next)? {
                Some(ng) => {
                    g = ng;
                    // fresh leaf, everything in it is past what we've emitted
                    start = Bound::Unbounded;
                }
                None => {
                    drop(g);
                    std::thread::yield_now();
                    let k: Vec<u8> = match &start {
                        Bound::Included(k) | Bound::Excluded(k) => k.clone(),
                        Bound::Unbounded => Vec::new(),
                    };
                    g = self.find_leaf(&k)?;
                }
            }
        }
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        self.scan_with(lower, upper, |k, v| {
            out.push((k.to_vec(), v.to_vec()));
            true
        })?;
        Ok(out)
    }

    // ---------- writes ----------

    pub fn put(&self, key: &[u8], val: &[u8], mode: &LogMode) -> Result<Option<Vec<u8>>> {
        self.write(key, val, mode, false)
    }

    // Ok(true) if inserted, Ok(false) if the key was already there (nothing changed)
    pub fn insert(&self, key: &[u8], val: &[u8], mode: &LogMode) -> Result<bool> {
        Ok(self.write(key, val, mode, true)?.is_none())
    }

    fn commit(&self, mtr: Mtr<'_>, mode: &LogMode, undo: Undo) -> Result<()> {
        let kind = match mode {
            LogMode::System => Kind::System,
            LogMode::Txn(c) => Kind::Txn(c, undo),
            LogMode::Clr(c, next) => Kind::Clr(c, *next),
        };
        mtr.commit(kind)?;
        Ok(())
    }

    fn undo_for(&self, key: &[u8], old: &Option<Vec<u8>>) -> Undo {
        match old {
            None => Undo::Remove { root: self.root, key: key.to_vec() },
            Some(o) => Undo::Restore { root: self.root, key: key.to_vec(), value: o.clone() },
        }
    }

    // Takes S-latches down to the leaf's parent and then X-latches only the leaf.
    // None means the tree shape didn't match our expectation; caller goes pessimistic.
    fn optimistic_leaf<'a>(&'a self, mtr: &mut Mtr<'a>, key: &[u8]) -> Result<Option<usize>> {
        let mut g = self.pool.fetch_read(self.root)?;
        if Node(&*g).level() == 0 {
            drop(g);
            let i = mtr.latch(self.root)?;
            if Node(mtr.page(i)).is_leaf() {
                return Ok(Some(i));
            }
            mtr.release(i);
            return Ok(None);
        }
        loop {
            let (lvl, child) = {
                let n = Node(&*g);
                (n.level(), n.child(n.child_index(key)))
            };
            if lvl == 1 {
                let i = mtr.latch(child)?;
                drop(g);
                if Node(mtr.page(i)).is_leaf() {
                    return Ok(Some(i));
                }
                mtr.release(i);
                return Ok(None);
            }
            g = self.pool.fetch_read(child)?;
        }
    }

    // Pessimistic descent. `safe` decides when ancestors can be released.
    // Returns mtr indexes of the latches still held, root-most first.
    fn descend<'a>(
        &'a self,
        mtr: &mut Mtr<'a>,
        key: &[u8],
        safe: impl Fn(PageId, &Node<&PageData>) -> bool,
    ) -> Result<Vec<usize>> {
        let r = mtr.latch(self.root)?;
        let mut path = vec![r];
        loop {
            let i = *path.last().unwrap();
            let child = {
                let n = Node(mtr.page(i));
                match n.kind() {
                    KIND_LEAF => return Ok(path),
                    KIND_INTERNAL => n.child(n.child_index(key)),
                    k => return corrupt(format!("page {} has kind {k} inside a tree", mtr.pid(i))),
                }
            };
            let c = mtr.latch(child)?;
            if safe(child, &Node(mtr.page(c))) {
                for p in path.drain(..) {
                    mtr.release(p);
                }
            }
            path.push(c);
        }
    }

    fn write(&self, key: &[u8], val: &[u8], mode: &LogMode, absent_only: bool) -> Result<Option<Vec<u8>>> {
        if key.len() > MAX_KEY {
            return Err(Error::KeyTooLarge);
        }
        if val.len() > MAX_VALUE {
            return Err(Error::ValueTooLarge);
        }
        let cell = leaf_cell(key, val);
        let need = cell.len() + 2;
        let mut mtr = Mtr::new(&self.pool, &self.log);

        if let Some(li) = self.optimistic_leaf(&mut mtr, key)? {
            let found = Node(mtr.page(li)).search(key);
            if leaf_fits(&Node(mtr.page(li)), found, need) {
                return self.finish_leaf_write(mtr, li, found, key, &cell, mode, absent_only);
            }
            mtr.release(li);
        }

        let path = self.descend(&mut mtr, key, |_, n| {
            if n.is_leaf() {
                n.free_bytes() >= need
            } else {
                n.free_bytes() >= MAX_INTERNAL_CELL + 2
            }
        })?;
        let li = *path.last().unwrap();
        let found = Node(mtr.page(li)).search(key);
        if leaf_fits(&Node(mtr.page(li)), found, need) {
            return self.finish_leaf_write(mtr, li, found, key, &cell, mode, absent_only);
        }

        // leaf is full, split
        let existing = match found {
            Ok(i) => {
                let old = Node(mtr.page(li)).val(i).to_vec();
                if absent_only {
                    return Ok(Some(old));
                }
                Node(mtr.page_mut(li)).remove_cell(i);
                Some(old)
            }
            Err(_) => None,
        };
        let pos = Node(mtr.page(li)).search(key).unwrap_err();
        let mut ents = Node(mtr.page(li)).leaf_entries();
        ents.insert(pos, (key.to_vec(), val.to_vec()));
        let at_end = pos == ents.len() - 1 && Node(mtr.page(li)).link() == INVALID_PAGE;
        let is_root = mtr.pid(li) == self.root;
        let mut carry = self.split_leaf(&mut mtr, li, ents, at_end, is_root)?;

        let mut lvl = path.len() - 1;
        while let Some((sep, rpid)) = carry.take() {
            lvl -= 1;
            let pi = path[lvl];
            let (idx, fits) = {
                let p = Node(mtr.page(pi));
                (p.child_index(key), p.free_bytes() >= internal_weight(&sep))
            };
            if fits {
                let left = Node(mtr.page(pi)).child(idx);
                let mut p = Node(mtr.page_mut(pi));
                p.set_child(idx, rpid);
                assert!(p.insert_cell(idx, &internal_cell(left, &sep)));
            } else {
                let (mut keys, mut ch, level) = {
                    let p = Node(mtr.page(pi));
                    let (k, c) = p.internal_entries();
                    (k, c, p.level())
                };
                keys.insert(idx, sep);
                ch.insert(idx + 1, rpid);
                let is_root = mtr.pid(pi) == self.root;
                carry = self.split_internal(&mut mtr, pi, level, keys, ch, is_root)?;
            }
        }

        let undo = self.undo_for(key, &existing);
        self.commit(mtr, mode, undo)?;
        Ok(existing)
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_leaf_write(
        &self,
        mut mtr: Mtr<'_>,
        li: usize,
        found: std::result::Result<usize, usize>,
        key: &[u8],
        cell: &[u8],
        mode: &LogMode,
        absent_only: bool,
    ) -> Result<Option<Vec<u8>>> {
        let (pos, old) = match found {
            Ok(i) => {
                let old = Node(mtr.page(li)).val(i).to_vec();
                if absent_only {
                    return Ok(Some(old));
                }
                Node(mtr.page_mut(li)).remove_cell(i);
                (i, Some(old))
            }
            Err(p) => (p, None),
        };
        assert!(Node(mtr.page_mut(li)).insert_cell(pos, cell));
        let undo = self.undo_for(key, &old);
        self.commit(mtr, mode, undo)?;
        Ok(old)
    }

    fn split_leaf(
        &self,
        mtr: &mut Mtr<'_>,
        li: usize,
        ents: Vec<(Vec<u8>, Vec<u8>)>,
        at_end: bool,
        is_root: bool,
    ) -> Result<Option<(Vec<u8>, PageId)>> {
        // sequential inserts land on the rightmost leaf; leaving the old one
        // full and starting a fresh one keeps fill factor near 100% for them
        let cut = if at_end {
            ents.len() - 1
        } else {
            let sizes: Vec<usize> = ents.iter().map(|(k, v)| leaf_weight(k, v)).collect();
            split_by_size(&sizes)
        };
        let (l, r) = ents.split_at(cut);
        let sep = r[0].0.clone();
        let next = Node(mtr.page(li)).link();
        let ri = mtr.alloc()?;
        let rpid = mtr.pid(ri);
        if is_root {
            let ci = mtr.alloc()?;
            let lpid = mtr.pid(ci);
            Node(mtr.page_mut(ci)).fill_leaf(l, rpid);
            Node(mtr.page_mut(ri)).fill_leaf(r, next);
            Node(mtr.page_mut(li)).fill_internal(1, &[sep], &[lpid, rpid]);
            Ok(None)
        } else {
            Node(mtr.page_mut(ri)).fill_leaf(r, next);
            Node(mtr.page_mut(li)).fill_leaf(l, rpid);
            Ok(Some((sep, rpid)))
        }
    }

    fn split_internal(
        &self,
        mtr: &mut Mtr<'_>,
        ni: usize,
        level: u8,
        keys: Vec<Vec<u8>>,
        ch: Vec<PageId>,
        is_root: bool,
    ) -> Result<Option<(Vec<u8>, PageId)>> {
        let n = keys.len();
        let sizes: Vec<usize> = keys.iter().map(|k| internal_weight(k)).collect();
        let m = split_by_size(&sizes).clamp(1, n - 2);
        let up = keys[m].clone();
        let (lk, rk) = (&keys[..m], &keys[m + 1..]);
        let (lc, rc) = (&ch[..=m], &ch[m + 1..]);
        let ri = mtr.alloc()?;
        let rpid = mtr.pid(ri);
        Node(mtr.page_mut(ri)).fill_internal(level, rk, rc);
        if is_root {
            let ci = mtr.alloc()?;
            let lpid = mtr.pid(ci);
            Node(mtr.page_mut(ci)).fill_internal(level, lk, lc);
            Node(mtr.page_mut(ni)).fill_internal(level + 1, &[up], &[lpid, rpid]);
            Ok(None)
        } else {
            Node(mtr.page_mut(ni)).fill_internal(level, lk, lc);
            Ok(Some((up, rpid)))
        }
    }

    pub fn delete(&self, key: &[u8], mode: &LogMode) -> Result<Option<Vec<u8>>> {
        let mut mtr = Mtr::new(&self.pool, &self.log);

        if let Some(li) = self.optimistic_leaf(&mut mtr, key)? {
            let n = Node(mtr.page(li));
            match n.search(key) {
                Err(_) => return Ok(None),
                Ok(i) => {
                    let after = n.used() - (n.cell_len(i) + 2);
                    if mtr.pid(li) == self.root || after >= MIN_FILL {
                        let old = n.val(i).to_vec();
                        Node(mtr.page_mut(li)).remove_cell(i);
                        let undo = self.undo_for(key, &Some(old.clone()));
                        self.commit(mtr, mode, undo)?;
                        return Ok(Some(old));
                    }
                }
            }
            mtr.release(li);
        }

        // internal node is safe if losing a key (child merge) still leaves it
        // above the fill floor, and there's room for a longer separator
        let path = self.descend(&mut mtr, key, |pid, n| {
            if n.is_leaf() {
                return false;
            }
            let roomy = n.free_bytes() >= MAX_INTERNAL_CELL + 2;
            if pid == self.root {
                n.len() >= 2 && roomy
            } else {
                n.used() >= MIN_FILL + MAX_INTERNAL_CELL + 2 && roomy
            }
        })?;
        let li = *path.last().unwrap();
        let i = match Node(mtr.page(li)).search(key) {
            Ok(i) => i,
            Err(_) => return Ok(None),
        };
        let old = Node(mtr.page(li)).val(i).to_vec();
        Node(mtr.page_mut(li)).remove_cell(i);

        let mut lvl = path.len() - 1;
        loop {
            let ni = path[lvl];
            let npid = mtr.pid(ni);
            if npid == self.root {
                self.collapse_root(&mut mtr, ni)?;
                break;
            }
            if Node(mtr.page(ni)).used() >= MIN_FILL || lvl == 0 {
                break;
            }
            if !self.rebalance(&mut mtr, path[lvl - 1], ni, npid)? {
                break;
            }
            lvl -= 1;
        }

        let undo = self.undo_for(key, &Some(old.clone()));
        self.commit(mtr, mode, undo)?;
        Ok(Some(old))
    }

    // Returns true when the two siblings were merged (parent lost a key and may
    // underflow itself). Redistribution and "can't do it right now" return false.
    fn rebalance(&self, mtr: &mut Mtr<'_>, pi: usize, ni: usize, npid: PageId) -> Result<bool> {
        let (left_idx, lp, rp, sep) = {
            let p = Node(mtr.page(pi));
            if p.is_empty() {
                return corrupt("internal node with no keys has a child that needs rebalancing");
            }
            let Some(idx) = (0..=p.len()).find(|&j| p.child(j) == npid) else {
                return corrupt(format!("page {npid} not found in its parent"));
            };
            let li = idx.saturating_sub(1);
            (li, p.child(li), p.child(li + 1), p.key(li).to_vec())
        };
        // pair is (left, right); our node is the right one unless it was child 0
        let (a, b) = if rp == npid { (mtr.latch(lp)?, ni) } else { (ni, mtr.latch(rp)?) };
        let (apid, bpid) = (mtr.pid(a), mtr.pid(b));

        let parent_room = |mtr: &Mtr<'_>, newsep: &[u8]| {
            let p = Node(mtr.page(pi));
            p.free_bytes() + p.cell_len(left_idx) + 2 >= internal_weight(newsep)
        };

        if Node(mtr.page(a)).is_leaf() {
            let mut e = Node(mtr.page(a)).leaf_entries();
            e.extend(Node(mtr.page(b)).leaf_entries());
            let next = Node(mtr.page(b)).link();
            let sizes: Vec<usize> = e.iter().map(|(k, v)| leaf_weight(k, v)).collect();
            if sizes.iter().sum::<usize>() <= CAPACITY {
                Node(mtr.page_mut(a)).fill_leaf(&e, next);
                self.drop_right(mtr, pi, left_idx, apid, b, bpid);
                return Ok(true);
            }
            let cut = split_by_size(&sizes);
            let newsep = e[cut].0.clone();
            if !parent_room(mtr, &newsep) {
                return Ok(false);
            }
            Node(mtr.page_mut(a)).fill_leaf(&e[..cut], bpid);
            Node(mtr.page_mut(b)).fill_leaf(&e[cut..], next);
            self.replace_sep(mtr, pi, left_idx, apid, &newsep);
            Ok(false)
        } else {
            let level = Node(mtr.page(a)).level();
            let (mut k, mut c) = Node(mtr.page(a)).internal_entries();
            let (k2, c2) = Node(mtr.page(b)).internal_entries();
            k.push(sep);
            k.extend(k2);
            c.extend(c2);
            let sizes: Vec<usize> = k.iter().map(|x| internal_weight(x)).collect();
            if sizes.iter().sum::<usize>() <= CAPACITY {
                Node(mtr.page_mut(a)).fill_internal(level, &k, &c);
                self.drop_right(mtr, pi, left_idx, apid, b, bpid);
                return Ok(true);
            }
            let m = split_by_size(&sizes).clamp(1, k.len() - 2);
            let up = k[m].clone();
            if !parent_room(mtr, &up) {
                return Ok(false);
            }
            Node(mtr.page_mut(a)).fill_internal(level, &k[..m], &c[..=m]);
            Node(mtr.page_mut(b)).fill_internal(level, &k[m + 1..], &c[m + 1..]);
            self.replace_sep(mtr, pi, left_idx, apid, &up);
            Ok(false)
        }
    }

    // right sibling is empty now: unlink from parent and hand the page back
    fn drop_right(
        &self,
        mtr: &mut Mtr<'_>,
        pi: usize,
        left_idx: usize,
        apid: PageId,
        b: usize,
        bpid: PageId,
    ) {
        let mut p = Node(mtr.page_mut(pi));
        p.remove_cell(left_idx);
        p.set_child(left_idx, apid);
        Node(mtr.page_mut(b)).init(KIND_FREE, 0, INVALID_PAGE);
        mtr.free_later(bpid);
    }

    fn replace_sep(&self, mtr: &mut Mtr<'_>, pi: usize, left_idx: usize, apid: PageId, newsep: &[u8]) {
        let mut p = Node(mtr.page_mut(pi));
        p.remove_cell(left_idx);
        assert!(p.insert_cell(left_idx, &internal_cell(apid, newsep)));
    }

    // root with a single child pulls that child's contents up so the root id stays put
    fn collapse_root(&self, mtr: &mut Mtr<'_>, ri: usize) -> Result<()> {
        let (is_internal, empty, only) = {
            let n = Node(mtr.page(ri));
            (n.kind() == KIND_INTERNAL, n.is_empty(), n.link())
        };
        if !is_internal || !empty {
            return Ok(());
        }
        // the merge that emptied the root already latched this child
        let ci = match mtr.find(only) {
            Some(i) => i,
            None => mtr.latch(only)?,
        };
        let cpid = mtr.pid(ci);
        if Node(mtr.page(ci)).is_leaf() {
            let e = Node(mtr.page(ci)).leaf_entries();
            Node(mtr.page_mut(ri)).fill_leaf(&e, INVALID_PAGE);
        } else {
            let (k, c) = Node(mtr.page(ci)).internal_entries();
            let lvl = Node(mtr.page(ci)).level();
            Node(mtr.page_mut(ri)).fill_internal(lvl, &k, &c);
        }
        Node(mtr.page_mut(ci)).init(KIND_FREE, 0, INVALID_PAGE);
        mtr.free_later(cpid);
        Ok(())
    }

    // ---------- verification (quiescent trees only) ----------

    pub fn check(&self) -> Result<TreeStats> {
        let mut st = TreeStats::default();
        let mut leaves: Vec<(PageId, PageId)> = Vec::new();
        let root_level = {
            let g = self.pool.fetch_read(self.root)?;
            Node(&*g).level()
        };
        st.height = root_level as usize + 1;
        self.check_node(self.root, None, None, root_level, &mut st, &mut leaves)?;
        for w in leaves.windows(2) {
            if w[0].1 != w[1].0 {
                return corrupt(format!(
                    "leaf {} links to {} but next in order is {}",
                    w[0].0, w[0].1, w[1].0
                ));
            }
        }
        if let Some(last) = leaves.last() {
            if last.1 != INVALID_PAGE {
                return corrupt("last leaf has a right sibling");
            }
        }
        Ok(st)
    }

    fn check_node(
        &self,
        pid: PageId,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        level: u8,
        st: &mut TreeStats,
        leaves: &mut Vec<(PageId, PageId)>,
    ) -> Result<()> {
        let g = self.pool.fetch_read(pid)?;
        let n = Node(&*g);
        if n.level() != level {
            return corrupt(format!("page {pid}: level {} expected {level}", n.level()));
        }
        for i in 1..n.len() {
            if n.key(i - 1) >= n.key(i) {
                return corrupt(format!("page {pid}: keys out of order at {i}"));
            }
        }
        if !n.is_empty() && (lo.is_some_and(|l| n.key(0) < l) || hi.is_some_and(|h| n.key(n.len() - 1) >= h))
        {
            return corrupt(format!("page {pid}: key outside parent's range"));
        }
        let used: usize = (0..n.len()).map(|i| n.cell_len(i) + 2).sum();
        if used != n.used() {
            return corrupt(format!("page {pid}: free-space accounting off ({used} vs {})", n.used()));
        }
        if n.is_leaf() {
            if level != 0 {
                return corrupt(format!("leaf {pid} at level {level}"));
            }
            st.leaves += 1;
            st.entries += n.len();
            leaves.push((pid, n.link()));
            return Ok(());
        }
        if n.kind() != KIND_INTERNAL || n.is_empty() {
            return corrupt(format!("page {pid}: bad internal node"));
        }
        st.internals += 1;
        let (keys, ch) = n.internal_entries();
        drop(g);
        for (i, &c) in ch.iter().enumerate() {
            let clo = if i == 0 { lo } else { Some(keys[i - 1].as_slice()) };
            let chi = if i == keys.len() { hi } else { Some(keys[i].as_slice()) };
            self.check_node(c, clo, chi, level - 1, st, leaves)?;
        }
        Ok(())
    }
}
