pub const PAGE_SIZE: usize = 8192;

// lsn (8) + checksum (4) + 4 spare. everything above this belongs to the page owner
pub const PAGE_HEADER: usize = 16;

pub type PageId = u32;
pub type Lsn = u64;
pub type TxnId = u64;
pub type PageData = [u8; PAGE_SIZE];

pub const INVALID_PAGE: PageId = u32::MAX;
