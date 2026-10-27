use std::ops::Bound::Unbounded;

use pagedb::{Database, Options};

fn main() -> pagedb::Result<()> {
    let dir = std::env::temp_dir().join("pagedb-demo");
    let _ = std::fs::remove_dir_all(&dir);

    let db = Database::open(&dir, Options::default())?;
    let users = db.create_table("users")?;

    let mut tx = db.begin();
    tx.insert(&users, b"u:1", b"ada")?;
    tx.insert(&users, b"u:2", b"grace")?;
    tx.commit()?;

    // second transaction is still open when the power goes out
    let mut pending = db.begin();
    pending.insert(&users, b"u:3", b"never committed")?;
    pending.update(&users, b"u:1", b"clobbered")?;
    let mut other = db.begin();
    other.insert(&users, b"u:4", b"linus")?;
    other.commit()?; // forces the log, pending's records go to disk with it

    println!("crashing with 1 transaction in flight");
    db.simulate_crash();
    std::mem::forget(pending);

    let db = Database::open(&dir, Options::default())?;
    println!("recovery: {:?}", db.recovery_report());
    let users = db.table("users")?;
    let mut tx = db.begin();
    for (k, v) in tx.scan(&users, Unbounded, Unbounded)? {
        println!("{} = {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
    }
    tx.commit()?;
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
