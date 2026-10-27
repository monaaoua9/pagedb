use std::collections::VecDeque;

// LRU-K. a frame with fewer than k recorded accesses has infinite backward
// k-distance and goes first (oldest first access wins), so a one-off table scan
// can't push out the hot set the way plain LRU lets it.
pub struct LruK {
    k: usize,
    clock: u64,
    hist: Vec<VecDeque<u64>>,
    present: Vec<bool>,
}

impl LruK {
    pub fn new(frames: usize, k: usize) -> Self {
        LruK { k, clock: 0, hist: vec![VecDeque::new(); frames], present: vec![false; frames] }
    }

    pub fn access(&mut self, f: usize) {
        self.clock += 1;
        let h = &mut self.hist[f];
        h.push_back(self.clock);
        if h.len() > self.k {
            h.pop_front();
        }
        self.present[f] = true;
    }

    pub fn remove(&mut self, f: usize) {
        self.hist[f].clear();
        self.present[f] = false;
    }

    // O(frames) scan; fine for pools in the low thousands and keeps the pin
    // check lock-free for the callers
    pub fn victim(&self, pinned: impl Fn(usize) -> bool) -> Option<usize> {
        (0..self.hist.len())
            .filter(|&f| self.present[f] && !pinned(f))
            .min_by_key(|&f| (self.hist[f].len() >= self.k, self.hist[f][0]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cold_frames_go_first() {
        let mut r = LruK::new(3, 2);
        r.access(0);
        r.access(1);
        r.access(0);
        r.access(1);
        r.access(2); // only one access, inf distance
        assert_eq!(r.victim(|_| false), Some(2));
    }

    #[test]
    fn oldest_kth_access_among_hot() {
        let mut r = LruK::new(3, 2);
        for f in [0, 1, 2, 0, 1, 2] {
            r.access(f);
        }
        // kth most recent: f0 -> t1, f1 -> t2, f2 -> t3
        assert_eq!(r.victim(|_| false), Some(0));
        r.access(0);
        assert_eq!(r.victim(|_| false), Some(1));
    }

    #[test]
    fn pinned_are_skipped_and_removed_are_gone() {
        let mut r = LruK::new(3, 2);
        r.access(0);
        r.access(1);
        assert_eq!(r.victim(|f| f == 0), Some(1));
        r.remove(1);
        assert_eq!(r.victim(|f| f == 0), None);
    }
}
