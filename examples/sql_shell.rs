use std::io::{self, BufRead, Write};

use pagedb::{Database, Options, Session};

fn main() -> pagedb::Result<()> {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "./sqldata".into());
    let db = Database::open(&dir, Options::default())?;
    let mut s = Session::new(&db)?;
    let stdin = io::stdin();
    loop {
        print!("{}", if s.in_transaction() { "pagedb*> " } else { "pagedb> " });
        io::stdout().flush().unwrap();
        let mut line = String::new();
        if stdin.lock().read_line(&mut line).unwrap() == 0 {
            break;
        }
        let q = line.trim();
        if q.is_empty() {
            continue;
        }
        if q == ".quit" {
            break;
        }
        match s.execute(q) {
            Err(e) => println!("error: {e}"),
            Ok(r) if r.columns.is_empty() => println!("ok, {} row(s) affected", r.affected),
            Ok(r) => {
                println!("{}", r.columns.join(" | "));
                for row in &r.rows {
                    let cells: Vec<String> = row.iter().map(|v| v.to_string()).collect();
                    println!("{}", cells.join(" | "));
                }
                println!("({} rows)", r.rows.len());
            }
        }
    }
    Ok(())
}
