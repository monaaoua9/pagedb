use crate::types::{PageId, PAGE_HEADER, PAGE_SIZE};

pub const KIND_FREE: u8 = 0;
pub const KIND_LEAF: u8 = 1;
pub const KIND_INTERNAL: u8 = 2;
pub const KIND_OVERFLOW: u8 = 3;

const OFF_KIND: usize = PAGE_HEADER;
const OFF_LEVEL: usize = PAGE_HEADER + 1;
const OFF_N: usize = PAGE_HEADER + 2;
const OFF_HEAP: usize = PAGE_HEADER + 4;
const OFF_FREE: usize = PAGE_HEADER + 6;
const OFF_LINK: usize = PAGE_HEADER + 8;
pub const NODE_HEADER: usize = 32;

pub const CAPACITY: usize = PAGE_SIZE - NODE_HEADER;
pub const MAX_KEY: usize = 512;
// values up to this stay in the leaf cell; bigger ones spill into a chain of overflow pages.
// the cap keeps a leaf cell small enough that any two always fit on one page
pub const MAX_INLINE: usize = 1500;
// one mini-transaction latches the whole chain, so this is also bounded by pool size
pub const MAX_VALUE: usize = 256 * 1024;
pub const MAX_LEAF_CELL: usize = 4 + MAX_KEY + 1 + MAX_INLINE;
pub const OVF_CAP: usize = PAGE_SIZE - NODE_HEADER;
pub const MAX_INTERNAL_CELL: usize = 6 + MAX_KEY;
pub const MIN_FILL: usize = CAPACITY / 4;

// Slotted page. Slot array grows up from NODE_HEADER, cells grow down from the
// end of the page.
//   leaf cell:     klen u16 | vlen u16 | key | value        (link = right sibling)
//   internal cell: child u32 | klen u16 | key               (link = rightmost child)
// Internal node with n keys has n+1 children: cell i's child holds keys < key i,
// child i+1 holds keys >= key i.
pub struct Node<P>(pub P);

pub fn leaf_cell(k: &[u8], v: &[u8]) -> Vec<u8> {
    let mut c = Vec::with_capacity(4 + k.len() + v.len());
    c.extend_from_slice(&(k.len() as u16).to_le_bytes());
    c.extend_from_slice(&(v.len() as u16).to_le_bytes());
    c.extend_from_slice(k);
    c.extend_from_slice(v);
    c
}

pub fn internal_cell(child: PageId, k: &[u8]) -> Vec<u8> {
    let mut c = Vec::with_capacity(6 + k.len());
    c.extend_from_slice(&child.to_le_bytes());
    c.extend_from_slice(&(k.len() as u16).to_le_bytes());
    c.extend_from_slice(k);
    c
}

pub fn leaf_weight(k: &[u8], v: &[u8]) -> usize {
    4 + k.len() + v.len() + 2
}

pub fn internal_weight(k: &[u8]) -> usize {
    6 + k.len() + 2
}

impl<P: AsRef<[u8]>> Node<P> {
    fn b(&self) -> &[u8] {
        self.0.as_ref()
    }
    fn u16_at(&self, o: usize) -> usize {
        u16::from_le_bytes([self.b()[o], self.b()[o + 1]]) as usize
    }
    fn u32_at(&self, o: usize) -> u32 {
        u32::from_le_bytes(self.b()[o..o + 4].try_into().unwrap())
    }

    pub fn kind(&self) -> u8 {
        self.b()[OFF_KIND]
    }
    pub fn is_leaf(&self) -> bool {
        self.kind() == KIND_LEAF
    }
    pub fn level(&self) -> u8 {
        self.b()[OFF_LEVEL]
    }
    pub fn len(&self) -> usize {
        self.u16_at(OFF_N)
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn link(&self) -> PageId {
        self.u32_at(OFF_LINK)
    }
    pub fn free_bytes(&self) -> usize {
        self.u16_at(OFF_FREE)
    }
    pub fn used(&self) -> usize {
        CAPACITY - self.free_bytes()
    }

    fn slot(&self, i: usize) -> usize {
        self.u16_at(NODE_HEADER + 2 * i)
    }

    pub fn cell_len(&self, i: usize) -> usize {
        let o = self.slot(i);
        if self.is_leaf() {
            4 + self.u16_at(o) + self.u16_at(o + 2)
        } else {
            6 + self.u16_at(o + 4)
        }
    }

    pub fn key(&self, i: usize) -> &[u8] {
        let o = self.slot(i);
        if self.is_leaf() {
            let kl = self.u16_at(o);
            &self.b()[o + 4..o + 4 + kl]
        } else {
            let kl = self.u16_at(o + 4);
            &self.b()[o + 6..o + 6 + kl]
        }
    }

    pub fn val(&self, i: usize) -> &[u8] {
        let o = self.slot(i);
        let kl = self.u16_at(o);
        let vl = self.u16_at(o + 2);
        &self.b()[o + 4 + kl..o + 4 + kl + vl]
    }

    // child i for i < len, rightmost child at i == len
    pub fn child(&self, i: usize) -> PageId {
        if i == self.len() {
            self.link()
        } else {
            self.u32_at(self.slot(i))
        }
    }

    pub fn search(&self, key: &[u8]) -> Result<usize, usize> {
        let (mut lo, mut hi) = (0, self.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.key(mid).cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }

    pub fn child_index(&self, key: &[u8]) -> usize {
        match self.search(key) {
            Ok(i) => i + 1,
            Err(i) => i,
        }
    }

    pub fn overflow_data(&self) -> &[u8] {
        let n = self.len();
        &self.b()[NODE_HEADER..NODE_HEADER + n]
    }

    pub fn leaf_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..self.len()).map(|i| (self.key(i).to_vec(), self.val(i).to_vec())).collect()
    }

    pub fn internal_entries(&self) -> (Vec<Vec<u8>>, Vec<PageId>) {
        let n = self.len();
        ((0..n).map(|i| self.key(i).to_vec()).collect(), (0..=n).map(|i| self.child(i)).collect())
    }
}

impl<P: AsRef<[u8]> + AsMut<[u8]>> Node<P> {
    fn bm(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
    fn put16(&mut self, o: usize, v: usize) {
        self.bm()[o..o + 2].copy_from_slice(&(v as u16).to_le_bytes());
    }
    fn put32(&mut self, o: usize, v: u32) {
        self.bm()[o..o + 4].copy_from_slice(&v.to_le_bytes());
    }

    pub fn init(&mut self, kind: u8, level: u8, link: PageId) {
        self.bm()[OFF_KIND] = kind;
        self.bm()[OFF_LEVEL] = level;
        self.put16(OFF_N, 0);
        self.put16(OFF_HEAP, PAGE_SIZE);
        self.put16(OFF_FREE, CAPACITY);
        self.put32(OFF_LINK, link);
    }

    pub fn set_link(&mut self, link: PageId) {
        self.put32(OFF_LINK, link);
    }

    pub fn set_child(&mut self, i: usize, pid: PageId) {
        if i == self.len() {
            self.set_link(pid);
        } else {
            let o = self.slot(i);
            self.put32(o, pid);
        }
    }

    fn compact(&mut self) {
        let n = self.len();
        let mut tmp = Vec::with_capacity(CAPACITY);
        let mut lens = Vec::with_capacity(n);
        for i in 0..n {
            let (o, l) = (self.slot(i), self.cell_len(i));
            tmp.extend_from_slice(&self.b()[o..o + l]);
            lens.push(l);
        }
        let mut heap = PAGE_SIZE;
        let mut t = 0;
        for (i, &l) in lens.iter().enumerate() {
            heap -= l;
            self.bm()[heap..heap + l].copy_from_slice(&tmp[t..t + l]);
            self.put16(NODE_HEADER + 2 * i, heap);
            t += l;
        }
        self.put16(OFF_HEAP, heap);
    }

    // false = doesn't fit, page untouched
    pub fn insert_cell(&mut self, pos: usize, cell: &[u8]) -> bool {
        let need = cell.len() + 2;
        if self.free_bytes() < need {
            return false;
        }
        let n = self.len();
        let slots_end = NODE_HEADER + 2 * n;
        if self.u16_at(OFF_HEAP) - slots_end < need {
            self.compact();
        }
        let off = self.u16_at(OFF_HEAP) - cell.len();
        self.bm()[off..off + cell.len()].copy_from_slice(cell);
        let sp = NODE_HEADER + 2 * pos;
        self.bm().copy_within(sp..slots_end, sp + 2);
        self.put16(sp, off);
        self.put16(OFF_N, n + 1);
        self.put16(OFF_HEAP, off);
        let f = self.free_bytes();
        self.put16(OFF_FREE, f - need);
        true
    }

    pub fn remove_cell(&mut self, pos: usize) {
        let n = self.len();
        let l = self.cell_len(pos);
        let sp = NODE_HEADER + 2 * pos;
        let slots_end = NODE_HEADER + 2 * n;
        self.bm().copy_within(sp + 2..slots_end, sp);
        self.put16(OFF_N, n - 1);
        let f = self.free_bytes();
        self.put16(OFF_FREE, f + l + 2);
    }

    // overflow page: chunk length lives in the key-count slot, next page in link
    pub fn init_overflow(&mut self, next: PageId, data: &[u8]) {
        self.init(KIND_OVERFLOW, 0, next);
        self.put16(OFF_N, data.len());
        self.bm()[NODE_HEADER..NODE_HEADER + data.len()].copy_from_slice(data);
    }

    pub fn fill_leaf(&mut self, ents: &[(Vec<u8>, Vec<u8>)], next: PageId) {
        self.init(KIND_LEAF, 0, next);
        for (i, (k, v)) in ents.iter().enumerate() {
            assert!(self.insert_cell(i, &leaf_cell(k, v)), "leaf rebuild overflowed");
        }
    }

    pub fn fill_internal(&mut self, level: u8, keys: &[Vec<u8>], children: &[PageId]) {
        debug_assert_eq!(keys.len() + 1, children.len());
        self.init(KIND_INTERNAL, level, *children.last().unwrap());
        for (i, k) in keys.iter().enumerate() {
            assert!(self.insert_cell(i, &internal_cell(children[i], k)), "internal rebuild overflowed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::INVALID_PAGE;

    fn leaf() -> Box<[u8; PAGE_SIZE]> {
        let mut p = Box::new([0u8; PAGE_SIZE]);
        Node(&mut *p).init(KIND_LEAF, 0, INVALID_PAGE);
        p
    }

    fn put(p: &mut [u8; PAGE_SIZE], k: &[u8], v: &[u8]) -> bool {
        let pos = match Node(&*p).search(k) {
            Ok(i) | Err(i) => i,
        };
        Node(p).insert_cell(pos, &leaf_cell(k, v))
    }

    #[test]
    fn sorted_insert_and_search() {
        let mut p = leaf();
        for k in ["m", "c", "x", "a", "q"] {
            assert!(put(&mut p, k.as_bytes(), b"v"));
        }
        let n = Node(&*p);
        let keys: Vec<_> = (0..n.len()).map(|i| n.key(i).to_vec()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec(), b"m".to_vec(), b"q".to_vec(), b"x".to_vec()]);
        assert_eq!(n.search(b"m"), Ok(2));
        assert_eq!(n.search(b"n"), Err(3));
    }

    #[test]
    fn free_space_accounting_and_compaction() {
        let mut p = leaf();
        let mut i = 0u32;
        // fill until full
        while put(&mut p, &i.to_be_bytes(), &[7u8; 100]) {
            i += 1;
        }
        let full = i as usize;
        assert!(Node(&*p).free_bytes() < 4 + 4 + 100 + 2);
        // punch holes in the middle, then the contiguous gap is too small but free_bytes isn't
        for _ in 0..10 {
            Node(&mut *p).remove_cell(5);
        }
        assert_eq!(Node(&*p).len(), full - 10);
        for j in 0..10u32 {
            assert!(put(&mut p, &(1_000_000 + j).to_be_bytes(), &[9u8; 100]));
        }
        let n = Node(&*p);
        for w in 1..n.len() {
            assert!(n.key(w - 1) < n.key(w));
        }
        assert_eq!(n.val(0), &[7u8; 100][..]);
    }

    #[test]
    fn internal_children_follow_cells() {
        let mut p = Box::new([0u8; PAGE_SIZE]);
        Node(&mut *p).fill_internal(1, &[b"f".to_vec(), b"p".to_vec()], &[10, 20, 30]);
        let n = Node(&*p);
        assert_eq!(n.child_index(b"a"), 0);
        assert_eq!(n.child_index(b"f"), 1);
        assert_eq!(n.child_index(b"g"), 1);
        assert_eq!(n.child_index(b"p"), 2);
        assert_eq!(n.child_index(b"z"), 2);
        assert_eq!((n.child(0), n.child(1), n.child(2)), (10, 20, 30));
        let mut p2 = p.clone();
        Node(&mut *p2).set_child(2, 99);
        Node(&mut *p2).set_child(0, 11);
        assert_eq!(Node(&*p2).child(2), 99);
        assert_eq!(Node(&*p2).child(0), 11);
    }

    #[test]
    fn rebuild_roundtrip() {
        let ents: Vec<_> = (0..20u8).map(|i| (vec![i], vec![i; 30])).collect();
        let mut p = Box::new([0u8; PAGE_SIZE]);
        Node(&mut *p).fill_leaf(&ents, 77);
        assert_eq!(Node(&*p).leaf_entries(), ents);
        assert_eq!(Node(&*p).link(), 77);
    }
}
