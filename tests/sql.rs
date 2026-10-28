mod common;

use common::TempDir;
use pagedb::sql::Value::{Int, Text};
use pagedb::{Database, Error, Options, Session};

fn t(s: &str) -> pagedb::sql::Value {
    Text(s.to_string())
}

#[test]
fn basic_statements() {
    let dir = TempDir::new("sql-basic");
    let db = Database::open(dir.path(), Options::default()).unwrap();
    let mut s = Session::new(&db).unwrap();
    s.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)").unwrap();
    let r = s.execute("insert into users values (3, 'cy', 30), (1, 'ann', 25), (2, 'bob', NULL)").unwrap();
    assert_eq!(r.affected, 3);

    let r = s.execute("SELECT * FROM users").unwrap();
    assert_eq!(r.columns, ["id", "name", "age"]);
    assert_eq!(r.rows[0], [Int(1), t("ann"), Int(25)]);
    assert_eq!(r.rows[2], [Int(3), t("cy"), Int(30)]);

    let r = s.execute("select name from users where id = 2").unwrap();
    assert_eq!(r.rows, [[t("bob")]]);
    let r = s.execute("select name from users where age > 26 or age is null order by name desc").unwrap();
    assert_eq!(r.rows, [[t("cy")], [t("bob")]]);
    let r = s.execute("select id from users where id >= 2 and id < 10 limit 1").unwrap();
    assert_eq!(r.rows, [[Int(2)]]);
    let r = s.execute("select count(*), sum(age) from users").unwrap();
    assert_eq!(r.rows, [[Int(3), Int(55)]]);

    let r = s.execute("update users set age = age + 1 where id = 1").unwrap();
    assert_eq!(r.affected, 1);
    let r = s.execute("select age from users where id = 1").unwrap();
    assert_eq!(r.rows, [[Int(26)]]);
    s.execute("update users set id = 10 where id = 3").unwrap();
    let r = s.execute("select id from users order by id desc").unwrap();
    assert_eq!(r.rows, [[Int(10)], [Int(2)], [Int(1)]]);

    let r = s.execute("delete from users where age is null").unwrap();
    assert_eq!(r.affected, 1);
    assert_eq!(s.execute("select count(*) from users").unwrap().rows, [[Int(2)]]);
}

#[test]
fn negative_keys_sort_first() {
    let dir = TempDir::new("sql-neg");
    let db = Database::open(dir.path(), Options::default()).unwrap();
    let mut s = Session::new(&db).unwrap();
    s.execute("create table n (k int primary key, v text)").unwrap();
    s.execute("insert into n values (5, 'a'), (-5, 'b'), (0, 'c'), (-100, 'd')").unwrap();
    let r = s.execute("select k from n").unwrap();
    assert_eq!(r.rows, [[Int(-100)], [Int(-5)], [Int(0)], [Int(5)]]);
    let r = s.execute("select k from n where k < 0").unwrap();
    assert_eq!(r.rows, [[Int(-100)], [Int(-5)]]);
}

#[test]
fn errors() {
    let dir = TempDir::new("sql-err");
    let db = Database::open(dir.path(), Options::default()).unwrap();
    let mut s = Session::new(&db).unwrap();
    s.execute("create table a (id int primary key, x text)").unwrap();
    s.execute("insert into a values (1, 'x')").unwrap();
    assert!(matches!(s.execute("insert into a values (1, 'y')"), Err(Error::DuplicateKey)));
    assert!(matches!(s.execute("insert into a values (NULL, 'y')"), Err(Error::Sql(_))));
    assert!(matches!(s.execute("insert into a values ('q', 'y')"), Err(Error::Sql(_))));
    assert!(matches!(s.execute("select nope from a"), Err(Error::Sql(_))));
    assert!(matches!(s.execute("select * from missing"), Err(Error::Sql(_))));
    assert!(matches!(s.execute("create table a (id int primary key)"), Err(Error::Sql(_))));
    assert!(matches!(s.execute("create table b (id int)"), Err(Error::Sql(_))));
    assert!(matches!(s.execute("select 1 / 0 from a"), Err(Error::Sql(_))));
    assert!(matches!(s.execute("select * from a where id = 1 / 0"), Err(Error::Sql(_))));
    assert!(matches!(s.execute("selec * from a"), Err(Error::Sql(_))));
    assert!(matches!(s.execute("select * from a garbage"), Err(Error::Sql(_))));
    assert!(matches!(s.execute("commit"), Err(Error::Sql(_))));
    // failed autocommit statements leave nothing behind
    assert!(matches!(s.execute("insert into a values (2, 'ok'), (1, 'dup')"), Err(Error::DuplicateKey)));
    assert_eq!(s.execute("select count(*) from a").unwrap().rows, [[Int(1)]]);
}

#[test]
fn transactions() {
    let dir = TempDir::new("sql-tx");
    let db = Database::open(dir.path(), Options::default()).unwrap();
    let mut s = Session::new(&db).unwrap();
    s.execute("create table acc (id int primary key, bal int)").unwrap();
    s.execute("insert into acc values (1, 100), (2, 100)").unwrap();

    s.execute("begin").unwrap();
    assert!(s.in_transaction());
    s.execute("update acc set bal = bal - 30 where id = 1").unwrap();
    s.execute("update acc set bal = bal + 30 where id = 2").unwrap();
    s.execute("rollback").unwrap();
    assert_eq!(s.execute("select sum(bal) from acc where id = 1").unwrap().rows, [[Int(100)]]);

    s.execute("begin").unwrap();
    s.execute("update acc set bal = bal - 30 where id = 1").unwrap();
    s.execute("commit").unwrap();
    assert_eq!(s.execute("select bal from acc where id = 1").unwrap().rows, [[Int(70)]]);
}

#[test]
fn survives_crash_and_reopen() {
    let dir = TempDir::new("sql-crash");
    {
        let db = Database::open(dir.path(), Options::default()).unwrap();
        let mut s = Session::new(&db).unwrap();
        s.execute("create table k (id int primary key, v text)").unwrap();
        s.execute("insert into k values (1, 'kept')").unwrap();
        s.execute("begin").unwrap();
        s.execute("insert into k values (2, 'lost')").unwrap();
        std::mem::forget(s);
        db.simulate_crash();
    }
    let db = Database::open(dir.path(), Options::default()).unwrap();
    let mut s = Session::new(&db).unwrap();
    let r = s.execute("select v from k").unwrap();
    assert_eq!(r.rows, [[t("kept")]]);
}

#[test]
fn big_text_values() {
    let dir = TempDir::new("sql-big");
    let db = Database::open(dir.path(), Options::default()).unwrap();
    let mut s = Session::new(&db).unwrap();
    s.execute("create table d (id int primary key, body text)").unwrap();
    let body = "x".repeat(50_000);
    s.execute(&format!("insert into d values (1, '{body}')")).unwrap();
    let r = s.execute("select body from d where id = 1").unwrap();
    assert_eq!(r.rows[0][0], Text(body));
}
