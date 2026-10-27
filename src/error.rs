use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Corrupt(String),
    PoolExhausted,
    Deadlock,
    LockTimeout,
    DuplicateKey,
    KeyNotFound,
    KeyTooLarge,
    ValueTooLarge,
    NoSuchTable(String),
    TableExists(String),
    TxnClosed,
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io: {e}"),
            Error::Corrupt(m) => write!(f, "corrupt: {m}"),
            Error::PoolExhausted => write!(f, "buffer pool exhausted, every frame is pinned"),
            Error::Deadlock => write!(f, "deadlock detected, transaction chosen as victim"),
            Error::LockTimeout => write!(f, "lock wait timed out"),
            Error::DuplicateKey => write!(f, "duplicate key"),
            Error::KeyNotFound => write!(f, "key not found"),
            Error::KeyTooLarge => write!(f, "key too large"),
            Error::ValueTooLarge => write!(f, "value too large"),
            Error::NoSuchTable(t) => write!(f, "no such table: {t}"),
            Error::TableExists(t) => write!(f, "table exists: {t}"),
            Error::TxnClosed => write!(f, "transaction already finished"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

pub fn corrupt<T>(msg: impl Into<String>) -> Result<T> {
    Err(Error::Corrupt(msg.into()))
}
