use crate::types::{Lsn, PageData};

// slicing-by-8; the plain byte-at-a-time loop topped out around 200MB/s and
// every page write and every full-page log image goes through here
const fn make_tables() -> [[u32; 256]; 8] {
    let mut t = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
            k += 1;
        }
        t[0][i] = c;
        i += 1;
    }
    let mut k = 1;
    while k < 8 {
        let mut i = 0;
        while i < 256 {
            let p = t[k - 1][i];
            t[k][i] = (p >> 8) ^ t[0][(p & 0xFF) as usize];
            i += 1;
        }
        k += 1;
    }
    t
}

static T: [[u32; 256]; 8] = make_tables();

#[allow(clippy::chunks_exact_to_as_chunks)]
fn crc_update(mut c: u32, data: &[u8]) -> u32 {
    let mut chunks = data.chunks_exact(8);
    for ch in &mut chunks {
        let lo = u32::from_le_bytes([ch[0], ch[1], ch[2], ch[3]]) ^ c;
        let hi = u32::from_le_bytes([ch[4], ch[5], ch[6], ch[7]]);
        c = T[7][(lo & 0xFF) as usize]
            ^ T[6][((lo >> 8) & 0xFF) as usize]
            ^ T[5][((lo >> 16) & 0xFF) as usize]
            ^ T[4][(lo >> 24) as usize]
            ^ T[3][(hi & 0xFF) as usize]
            ^ T[2][((hi >> 8) & 0xFF) as usize]
            ^ T[1][((hi >> 16) & 0xFF) as usize]
            ^ T[0][(hi >> 24) as usize];
    }
    for &b in chunks.remainder() {
        c = T[0][((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8);
    }
    c
}

pub fn crc32(parts: &[&[u8]]) -> u32 {
    !parts.iter().fold(!0u32, |c, p| crc_update(c, p))
}

pub fn page_lsn(p: &PageData) -> Lsn {
    u64::from_le_bytes(p[0..8].try_into().unwrap())
}

pub fn set_page_lsn(p: &mut PageData, lsn: Lsn) {
    p[0..8].copy_from_slice(&lsn.to_le_bytes());
}

fn checksum(p: &PageData) -> u32 {
    crc32(&[&p[0..8], &p[12..]])
}

pub fn stamp_checksum(p: &mut PageData) {
    let c = checksum(p);
    p[8..12].copy_from_slice(&c.to_le_bytes());
}

// an all-zero page is a page that was allocated but never written; that's legal
pub fn verify_checksum(p: &PageData) -> bool {
    if p.iter().all(|&b| b == 0) {
        return true;
    }
    u32::from_le_bytes(p[8..12].try_into().unwrap()) == checksum(p)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PAGE_SIZE;

    #[test]
    fn crc_known_vector() {
        assert_eq!(crc32(&[b"123456789"]), 0xCBF4_3926);
        assert_eq!(crc32(&[b"1234", b"56789"]), 0xCBF4_3926);
    }

    #[test]
    fn checksum_detects_flip() {
        let mut p = [0u8; PAGE_SIZE];
        p[100] = 7;
        stamp_checksum(&mut p);
        assert!(verify_checksum(&p));
        p[4000] ^= 1;
        assert!(!verify_checksum(&p));
    }

    #[test]
    fn lsn_roundtrip() {
        let mut p = [0u8; PAGE_SIZE];
        set_page_lsn(&mut p, 0xDEAD_BEEF_1234);
        assert_eq!(page_lsn(&p), 0xDEAD_BEEF_1234);
    }
}
