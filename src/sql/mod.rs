use std::cmp::Ordering;
use std::fmt;
use std::ops::Bound;

use crate::db::{Database, Table};
use crate::error::{corrupt, Error, Result};
use crate::txn::Transaction;

mod parser;
use parser::{Expr, Op, SelItem, Stmt};

const SCHEMA_TABLE: &str = "sql_schema";

fn err<T>(m: impl Into<String>) -> Result<T> {
    Err(Error::Sql(m.into()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColType {
    Int,
    Text,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Null,
    Int(i64),
    Text(String),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Int(n) => write!(f, "{n}"),
            Value::Text(s) => write!(f, "{s}"),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub affected: usize,
}

struct Schema {
    cols: Vec<(String, ColType)>,
    pk: usize,
}

impl Schema {
    // "<pk index>|name:int,name:text"
    fn encode(&self) -> Vec<u8> {
        let cols: Vec<String> = self
            .cols
            .iter()
            .map(|(n, t)| format!("{n}:{}", if *t == ColType::Int { "int" } else { "text" }))
            .collect();
        format!("{}|{}", self.pk, cols.join(",")).into_bytes()
    }

    fn decode(b: &[u8]) -> Result<Schema> {
        let s = String::from_utf8_lossy(b);
        let Some((pk, rest)) = s.split_once('|') else { return corrupt("bad sql schema") };
        let mut cols = Vec::new();
        for c in rest.split(',') {
            match c.split_once(':') {
                Some((n, "int")) => cols.push((n.to_string(), ColType::Int)),
                Some((n, "text")) => cols.push((n.to_string(), ColType::Text)),
                _ => return corrupt("bad sql schema column"),
            }
        }
        let Ok(pk) = pk.parse::<usize>() else { return corrupt("bad sql schema pk") };
        if pk >= cols.len() {
            return corrupt("sql schema pk out of range");
        }
        Ok(Schema { cols, pk })
    }

    fn col(&self, name: &str) -> Result<usize> {
        match self.cols.iter().position(|(n, _)| n == name) {
            Some(i) => Ok(i),
            None => err(format!("no such column: {name}")),
        }
    }
}

fn enc_key(v: &Value) -> Result<Vec<u8>> {
    match v {
        // sign bit flipped so negative numbers sort first under memcmp
        Value::Int(n) => Ok(((*n as u64) ^ (1 << 63)).to_be_bytes().to_vec()),
        Value::Text(s) if !s.is_empty() => Ok(s.as_bytes().to_vec()),
        Value::Text(_) => err("primary key can't be an empty string"),
        Value::Null => err("primary key can't be NULL"),
    }
}

fn enc_row(row: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
    for v in row {
        match v {
            Value::Null => out.push(0),
            Value::Int(n) => {
                out.push(1);
                out.extend_from_slice(&n.to_le_bytes());
            }
            Value::Text(s) => {
                out.push(2);
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
        }
    }
    out
}

fn dec_row(b: &[u8], n: usize) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(n);
    let mut p = 0;
    for _ in 0..n {
        let Some(&tag) = b.get(p) else { return corrupt("sql row truncated") };
        p += 1;
        match tag {
            0 => out.push(Value::Null),
            1 => {
                let Some(x) = b.get(p..p + 8) else { return corrupt("sql row truncated") };
                out.push(Value::Int(i64::from_le_bytes(x.try_into().unwrap())));
                p += 8;
            }
            2 => {
                let Some(x) = b.get(p..p + 4) else { return corrupt("sql row truncated") };
                let len = u32::from_le_bytes(x.try_into().unwrap()) as usize;
                p += 4;
                let Some(s) = b.get(p..p + len) else { return corrupt("sql row truncated") };
                out.push(Value::Text(String::from_utf8_lossy(s).into_owned()));
                p += len;
            }
            t => return corrupt(format!("bad sql value tag {t}")),
        }
    }
    Ok(out)
}

fn cmp_vals(a: &Value, b: &Value) -> Result<Ordering> {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => Ok(x.cmp(y)),
        (Value::Text(x), Value::Text(y)) => Ok(x.cmp(y)),
        _ => err("can't compare an integer with text"),
    }
}

fn truth(v: &Value) -> Result<Option<bool>> {
    match v {
        Value::Null => Ok(None),
        Value::Int(n) => Ok(Some(*n != 0)),
        Value::Text(_) => err("text used as a condition"),
    }
}

fn from_bool(b: Option<bool>) -> Value {
    match b {
        Some(b) => Value::Int(b as i64),
        None => Value::Null,
    }
}

fn eval(e: &Expr, cx: Option<(&Schema, &[Value])>) -> Result<Value> {
    match e {
        Expr::Lit(v) => Ok(v.clone()),
        Expr::Col(name) => match cx {
            Some((sc, row)) => Ok(row[sc.col(name)?].clone()),
            None => err(format!("column '{name}' isn't allowed here")),
        },
        Expr::Neg(x) => match eval(x, cx)? {
            Value::Int(n) => {
                n.checked_neg().map(Value::Int).ok_or_else(|| Error::Sql("integer overflow".into()))
            }
            Value::Null => Ok(Value::Null),
            Value::Text(_) => err("can't negate text"),
        },
        Expr::Not(x) => Ok(from_bool(truth(&eval(x, cx)?)?.map(|b| !b))),
        Expr::IsNull(x, neg) => {
            let n = eval(x, cx)? == Value::Null;
            Ok(Value::Int((n != *neg) as i64))
        }
        Expr::Bin(op, l, r) => {
            let (a, b) = (eval(l, cx)?, eval(r, cx)?);
            match op {
                Op::And => {
                    let (x, y) = (truth(&a)?, truth(&b)?);
                    Ok(from_bool(match (x, y) {
                        (Some(false), _) | (_, Some(false)) => Some(false),
                        (Some(true), Some(true)) => Some(true),
                        _ => None,
                    }))
                }
                Op::Or => {
                    let (x, y) = (truth(&a)?, truth(&b)?);
                    Ok(from_bool(match (x, y) {
                        (Some(true), _) | (_, Some(true)) => Some(true),
                        (Some(false), Some(false)) => Some(false),
                        _ => None,
                    }))
                }
                Op::Add | Op::Sub | Op::Mul | Op::Div => {
                    let (x, y) = match (&a, &b) {
                        (Value::Null, _) | (_, Value::Null) => return Ok(Value::Null),
                        (Value::Int(x), Value::Int(y)) => (*x, *y),
                        _ => return err("arithmetic needs integers"),
                    };
                    let r = match op {
                        Op::Add => x.checked_add(y),
                        Op::Sub => x.checked_sub(y),
                        Op::Mul => x.checked_mul(y),
                        _ if y == 0 => return err("division by zero"),
                        _ => x.checked_div(y),
                    };
                    r.map(Value::Int).ok_or_else(|| Error::Sql("integer overflow".into()))
                }
                _ => {
                    if a == Value::Null || b == Value::Null {
                        return Ok(Value::Null);
                    }
                    let o = cmp_vals(&a, &b)?;
                    let hit = match op {
                        Op::Eq => o == Ordering::Equal,
                        Op::Ne => o != Ordering::Equal,
                        Op::Lt => o == Ordering::Less,
                        Op::Le => o != Ordering::Greater,
                        Op::Gt => o == Ordering::Greater,
                        _ => o != Ordering::Less,
                    };
                    Ok(Value::Int(hit as i64))
                }
            }
        }
    }
}

fn keep(filter: &Option<Expr>, sc: &Schema, row: &[Value]) -> Result<bool> {
    match filter {
        None => Ok(true),
        Some(f) => Ok(truth(&eval(f, Some((sc, row)))?)? == Some(true)),
    }
}

fn coerce(v: Value, ty: ColType) -> Result<Value> {
    match (&v, ty) {
        (Value::Null, _) | (Value::Int(_), ColType::Int) | (Value::Text(_), ColType::Text) => Ok(v),
        _ => err("value doesn't match the column type"),
    }
}

enum Access {
    Point(Vec<u8>),
    Range(Bound<Vec<u8>>, Bound<Vec<u8>>),
}

fn conjuncts<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::Bin(Op::And, a, b) = e {
        conjuncts(a, out);
        conjuncts(b, out);
    } else {
        out.push(e);
    }
}

fn flip(op: Op) -> Op {
    match op {
        Op::Lt => Op::Gt,
        Op::Le => Op::Ge,
        Op::Gt => Op::Lt,
        Op::Ge => Op::Le,
        o => o,
    }
}

// only narrows the scan; the full filter is applied to whatever comes back
fn plan(sc: &Schema, filter: &Option<Expr>) -> Access {
    let (mut lo, mut hi) = (Bound::Unbounded, Bound::Unbounded);
    let Some(f) = filter else { return Access::Range(lo, hi) };
    let mut cs = Vec::new();
    conjuncts(f, &mut cs);
    let pk = &sc.cols[sc.pk];
    for c in cs {
        let Expr::Bin(op, l, r) = c else { continue };
        let (op, v) = match (&**l, &**r) {
            (Expr::Col(n), k) if *n == pk.0 => (*op, eval(k, None)),
            (k, Expr::Col(n)) if *n == pk.0 => (flip(*op), eval(k, None)),
            _ => continue,
        };
        let Ok(v) = v else { continue };
        if coerce(v.clone(), pk.1).is_err() {
            continue;
        }
        let Ok(k) = enc_key(&v) else { continue };
        match op {
            Op::Eq => return Access::Point(k),
            Op::Gt if lo == Bound::Unbounded => lo = Bound::Excluded(k),
            Op::Ge if lo == Bound::Unbounded => lo = Bound::Included(k),
            Op::Lt if hi == Bound::Unbounded => hi = Bound::Excluded(k),
            Op::Le if hi == Bound::Unbounded => hi = Bound::Included(k),
            _ => {}
        }
    }
    Access::Range(lo, hi)
}

fn as_slice(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(k) => Bound::Included(k),
        Bound::Excluded(k) => Bound::Excluded(k),
        Bound::Unbounded => Bound::Unbounded,
    }
}

type Rows = Vec<(Vec<u8>, Vec<Value>)>;

fn fetch(tx: &mut Transaction, t: &Table, sc: &Schema, acc: &Access, write: bool) -> Result<Rows> {
    let raw = match acc {
        Access::Point(k) => {
            let v = if write { tx.get_for_update(t, k)? } else { tx.get(t, k)? };
            v.map(|v| (k.clone(), v)).into_iter().collect()
        }
        Access::Range(lo, hi) if write => tx.scan_for_update(t, as_slice(lo), as_slice(hi))?,
        Access::Range(lo, hi) => tx.scan(t, as_slice(lo), as_slice(hi))?,
    };
    raw.into_iter().map(|(k, v)| Ok((k, dec_row(&v, sc.cols.len())?))).collect()
}

fn order_vals(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        _ => cmp_vals(a, b).unwrap_or(Ordering::Equal),
    }
}

pub struct Session<'a> {
    db: &'a Database,
    schemas: Table,
    tx: Option<Transaction>,
}

impl<'a> Session<'a> {
    pub fn new(db: &'a Database) -> Result<Session<'a>> {
        let schemas = match db.table(SCHEMA_TABLE) {
            Ok(t) => t,
            Err(_) => match db.create_table(SCHEMA_TABLE) {
                Ok(t) => t,
                Err(Error::TableExists(_)) => db.table(SCHEMA_TABLE)?,
                Err(e) => return Err(e),
            },
        };
        Ok(Session { db, schemas, tx: None })
    }

    pub fn in_transaction(&self) -> bool {
        self.tx.is_some()
    }

    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        match parser::parse(sql)? {
            Stmt::Begin => {
                if self.tx.is_some() {
                    return err("already in a transaction");
                }
                self.tx = Some(self.db.begin());
                Ok(QueryResult::default())
            }
            Stmt::Commit => match self.tx.take() {
                Some(tx) => tx.commit().map(|_| QueryResult::default()),
                None => err("no transaction in progress"),
            },
            Stmt::Rollback => match self.tx.take() {
                Some(tx) => tx.rollback().map(|_| QueryResult::default()),
                None => err("no transaction in progress"),
            },
            st => {
                let auto = self.tx.is_none();
                let mut tx = self.tx.take().unwrap_or_else(|| self.db.begin());
                let res = self.run(&mut tx, st);
                if !auto {
                    // statement errors leave the transaction open, caller decides
                    self.tx = Some(tx);
                } else if res.is_ok() {
                    tx.commit()?;
                }
                res
            }
        }
    }

    fn load(&self, tx: &mut Transaction, name: &str) -> Result<(Table, Schema)> {
        let Some(raw) = tx.get(&self.schemas, name.as_bytes())? else {
            return err(format!("no such table: {name}"));
        };
        Ok((self.db.table(&format!("sql.{name}"))?, Schema::decode(&raw)?))
    }

    fn run(&self, tx: &mut Transaction, st: Stmt) -> Result<QueryResult> {
        match st {
            Stmt::CreateTable { name, cols, pk } => {
                let sc = Schema { cols, pk };
                for (i, (n, _)) in sc.cols.iter().enumerate() {
                    if sc.cols[..i].iter().any(|(m, _)| m == n) {
                        return err(format!("duplicate column: {n}"));
                    }
                }
                match tx.insert(&self.schemas, name.as_bytes(), &sc.encode()) {
                    Err(Error::DuplicateKey) => return err(format!("table already exists: {name}")),
                    r => r?,
                }
                // a rolled-back CREATE leaves the empty tree behind, reuse it
                match self.db.create_table(&format!("sql.{name}")) {
                    Ok(_) | Err(Error::TableExists(_)) => {}
                    Err(e) => return Err(e),
                }
                Ok(QueryResult::default())
            }
            Stmt::Insert { table, cols, rows } => {
                let (t, sc) = self.load(tx, &table)?;
                let order: Vec<usize> = match &cols {
                    Some(cs) => cs.iter().map(|c| sc.col(c)).collect::<Result<_>>()?,
                    None => (0..sc.cols.len()).collect(),
                };
                let mut n = 0;
                for r in rows {
                    if r.len() != order.len() {
                        return err(format!("expected {} values, got {}", order.len(), r.len()));
                    }
                    let mut row = vec![Value::Null; sc.cols.len()];
                    for (e, &i) in r.iter().zip(&order) {
                        row[i] = coerce(eval(e, None)?, sc.cols[i].1)?;
                    }
                    tx.insert(&t, &enc_key(&row[sc.pk])?, &enc_row(&row))?;
                    n += 1;
                }
                Ok(QueryResult { affected: n, ..Default::default() })
            }
            Stmt::Select { items, table, filter, order, limit } => {
                let (t, sc) = self.load(tx, &table)?;
                let acc = plan(&sc, &filter);
                let mut rows: Vec<Vec<Value>> = Vec::new();
                for (_, row) in fetch(tx, &t, &sc, &acc, false)? {
                    if keep(&filter, &sc, &row)? {
                        rows.push(row);
                    }
                }
                if items.iter().any(|i| matches!(i, SelItem::Count | SelItem::Sum(_))) {
                    return aggregate(&items, &sc, &rows);
                }
                if let Some((c, desc)) = order {
                    let i = sc.col(&c)?;
                    rows.sort_by(|a, b| order_vals(&a[i], &b[i]));
                    if desc {
                        rows.reverse();
                    }
                }
                if let Some(l) = limit {
                    rows.truncate(l);
                }
                let mut idx = Vec::new();
                for it in &items {
                    match it {
                        SelItem::Star => idx.extend(0..sc.cols.len()),
                        SelItem::Col(c) => idx.push(sc.col(c)?),
                        _ => unreachable!(),
                    }
                }
                Ok(QueryResult {
                    columns: idx.iter().map(|&i| sc.cols[i].0.clone()).collect(),
                    rows: rows.iter().map(|r| idx.iter().map(|&i| r[i].clone()).collect()).collect(),
                    affected: 0,
                })
            }
            Stmt::Update { table, sets, filter } => {
                let (t, sc) = self.load(tx, &table)?;
                let sets: Vec<(usize, &Expr)> =
                    sets.iter().map(|(c, e)| Ok((sc.col(c)?, e))).collect::<Result<_>>()?;
                let acc = plan(&sc, &filter);
                let mut n = 0;
                for (key, row) in fetch(tx, &t, &sc, &acc, true)? {
                    if !keep(&filter, &sc, &row)? {
                        continue;
                    }
                    let mut new = row.clone();
                    for &(i, e) in &sets {
                        new[i] = coerce(eval(e, Some((&sc, &row)))?, sc.cols[i].1)?;
                    }
                    let nk = enc_key(&new[sc.pk])?;
                    if nk == key {
                        tx.put(&t, &key, &enc_row(&new))?;
                    } else {
                        tx.delete(&t, &key)?;
                        tx.insert(&t, &nk, &enc_row(&new))?;
                    }
                    n += 1;
                }
                Ok(QueryResult { affected: n, ..Default::default() })
            }
            Stmt::Delete { table, filter } => {
                let (t, sc) = self.load(tx, &table)?;
                let acc = plan(&sc, &filter);
                let mut n = 0;
                for (key, row) in fetch(tx, &t, &sc, &acc, true)? {
                    if keep(&filter, &sc, &row)? {
                        tx.delete(&t, &key)?;
                        n += 1;
                    }
                }
                Ok(QueryResult { affected: n, ..Default::default() })
            }
            Stmt::Begin | Stmt::Commit | Stmt::Rollback => unreachable!(),
        }
    }
}

fn aggregate(items: &[SelItem], sc: &Schema, rows: &[Vec<Value>]) -> Result<QueryResult> {
    let (mut names, mut out) = (Vec::new(), Vec::new());
    for it in items {
        match it {
            SelItem::Count => {
                names.push("count".to_string());
                out.push(Value::Int(rows.len() as i64));
            }
            SelItem::Sum(c) => {
                let i = sc.col(c)?;
                if sc.cols[i].1 != ColType::Int {
                    return err("SUM needs an integer column");
                }
                let mut acc: Option<i64> = None;
                for r in rows {
                    if let Value::Int(n) = r[i] {
                        acc = Some(match acc {
                            None => n,
                            Some(a) => {
                                a.checked_add(n).ok_or_else(|| Error::Sql("integer overflow".into()))?
                            }
                        });
                    }
                }
                names.push(format!("sum({c})"));
                out.push(acc.map_or(Value::Null, Value::Int));
            }
            _ => return err("can't mix aggregates with plain columns"),
        }
    }
    Ok(QueryResult { columns: names, rows: vec![out], affected: 0 })
}
