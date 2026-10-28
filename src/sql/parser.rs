use super::{ColType, Value};
use crate::error::{Error, Result};

fn err<T>(m: impl Into<String>) -> Result<T> {
    Err(Error::Sql(m.into()))
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Word(String),
    Int(i64),
    Str(String),
    Sym(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Op {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Lit(Value),
    Col(String),
    Neg(Box<Expr>),
    Not(Box<Expr>),
    IsNull(Box<Expr>, bool),
    Bin(Op, Box<Expr>, Box<Expr>),
}

#[derive(Debug, Clone)]
pub enum SelItem {
    Star,
    Col(String),
    Count,
    Sum(String),
}

#[derive(Debug, Clone)]
pub enum Stmt {
    CreateTable {
        name: String,
        cols: Vec<(String, ColType)>,
        pk: usize,
    },
    Insert {
        table: String,
        cols: Option<Vec<String>>,
        rows: Vec<Vec<Expr>>,
    },
    Select {
        items: Vec<SelItem>,
        table: String,
        filter: Option<Expr>,
        order: Option<(String, bool)>,
        limit: Option<usize>,
    },
    Update {
        table: String,
        sets: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
    },
    Begin,
    Commit,
    Rollback,
}

fn lex(s: &str) -> Result<Vec<Tok>> {
    let b: Vec<char> = s.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c.is_whitespace() {
            i += 1;
        } else if c.is_ascii_alphabetic() || c == '_' {
            let st = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == '_') {
                i += 1;
            }
            out.push(Tok::Word(b[st..i].iter().collect()));
        } else if c.is_ascii_digit() {
            let st = i;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            let txt: String = b[st..i].iter().collect();
            match txt.parse() {
                Ok(n) => out.push(Tok::Int(n)),
                Err(_) => return err(format!("integer out of range: {txt}")),
            }
        } else if c == '\'' {
            i += 1;
            let mut v = String::new();
            loop {
                if i >= b.len() {
                    return err("unterminated string literal");
                }
                if b[i] == '\'' {
                    if i + 1 < b.len() && b[i + 1] == '\'' {
                        v.push('\'');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                v.push(b[i]);
                i += 1;
            }
            out.push(Tok::Str(v));
        } else {
            let two: String = b[i..(i + 2).min(b.len())].iter().collect();
            let sym = match two.as_str() {
                "<=" => Some("<="),
                ">=" => Some(">="),
                "<>" => Some("<>"),
                "!=" => Some("!="),
                _ => None,
            };
            if let Some(s) = sym {
                out.push(Tok::Sym(s));
                i += 2;
                continue;
            }
            let s = match c {
                '(' => "(",
                ')' => ")",
                ',' => ",",
                ';' => ";",
                '*' => "*",
                '=' => "=",
                '<' => "<",
                '>' => ">",
                '+' => "+",
                '-' => "-",
                '/' => "/",
                _ => return err(format!("unexpected character '{c}'")),
            };
            out.push(Tok::Sym(s));
            i += 1;
        }
    }
    Ok(out)
}

struct P {
    t: Vec<Tok>,
    p: usize,
}

impl P {
    fn peek(&self) -> Option<&Tok> {
        self.t.get(self.p)
    }

    fn kw(&self, k: &str) -> bool {
        matches!(self.peek(), Some(Tok::Word(w)) if w.eq_ignore_ascii_case(k))
    }

    fn accept_kw(&mut self, k: &str) -> bool {
        let hit = self.kw(k);
        if hit {
            self.p += 1;
        }
        hit
    }

    fn expect_kw(&mut self, k: &str) -> Result<()> {
        if self.accept_kw(k) {
            Ok(())
        } else {
            err(format!("expected {}", k.to_uppercase()))
        }
    }

    fn accept_sym(&mut self, s: &str) -> bool {
        let hit = matches!(self.peek(), Some(Tok::Sym(x)) if *x == s);
        if hit {
            self.p += 1;
        }
        hit
    }

    fn expect_sym(&mut self, s: &str) -> Result<()> {
        if self.accept_sym(s) {
            Ok(())
        } else {
            err(format!("expected '{s}'"))
        }
    }

    // identifiers are case-insensitive, stored lower-case
    fn ident(&mut self) -> Result<String> {
        match self.t.get(self.p) {
            Some(Tok::Word(w)) => {
                self.p += 1;
                Ok(w.to_ascii_lowercase())
            }
            _ => err("expected a name"),
        }
    }

    fn expr(&mut self) -> Result<Expr> {
        self.or()
    }

    fn or(&mut self) -> Result<Expr> {
        let mut l = self.and()?;
        while self.accept_kw("or") {
            l = Expr::Bin(Op::Or, Box::new(l), Box::new(self.and()?));
        }
        Ok(l)
    }

    fn and(&mut self) -> Result<Expr> {
        let mut l = self.not()?;
        while self.accept_kw("and") {
            l = Expr::Bin(Op::And, Box::new(l), Box::new(self.not()?));
        }
        Ok(l)
    }

    fn not(&mut self) -> Result<Expr> {
        if self.accept_kw("not") {
            return Ok(Expr::Not(Box::new(self.not()?)));
        }
        self.cmp()
    }

    fn cmp(&mut self) -> Result<Expr> {
        let l = self.add()?;
        if self.accept_kw("is") {
            let neg = self.accept_kw("not");
            self.expect_kw("null")?;
            return Ok(Expr::IsNull(Box::new(l), neg));
        }
        let op = [
            ("=", Op::Eq),
            ("<>", Op::Ne),
            ("!=", Op::Ne),
            ("<=", Op::Le),
            (">=", Op::Ge),
            ("<", Op::Lt),
            (">", Op::Gt),
        ]
        .into_iter()
        .find(|(s, _)| matches!(self.peek(), Some(Tok::Sym(x)) if x == s));
        match op {
            Some((_, op)) => {
                self.p += 1;
                Ok(Expr::Bin(op, Box::new(l), Box::new(self.add()?)))
            }
            None => Ok(l),
        }
    }

    fn add(&mut self) -> Result<Expr> {
        let mut l = self.mul()?;
        loop {
            let op = if self.accept_sym("+") {
                Op::Add
            } else if self.accept_sym("-") {
                Op::Sub
            } else {
                return Ok(l);
            };
            l = Expr::Bin(op, Box::new(l), Box::new(self.mul()?));
        }
    }

    fn mul(&mut self) -> Result<Expr> {
        let mut l = self.unary()?;
        loop {
            let op = if self.accept_sym("*") {
                Op::Mul
            } else if self.accept_sym("/") {
                Op::Div
            } else {
                return Ok(l);
            };
            l = Expr::Bin(op, Box::new(l), Box::new(self.unary()?));
        }
    }

    fn unary(&mut self) -> Result<Expr> {
        if self.accept_sym("-") {
            return Ok(Expr::Neg(Box::new(self.unary()?)));
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<Expr> {
        match self.t.get(self.p).cloned() {
            Some(Tok::Int(n)) => {
                self.p += 1;
                Ok(Expr::Lit(Value::Int(n)))
            }
            Some(Tok::Str(s)) => {
                self.p += 1;
                Ok(Expr::Lit(Value::Text(s)))
            }
            Some(Tok::Word(w)) if w.eq_ignore_ascii_case("null") => {
                self.p += 1;
                Ok(Expr::Lit(Value::Null))
            }
            Some(Tok::Word(_)) => Ok(Expr::Col(self.ident()?)),
            Some(Tok::Sym("(")) => {
                self.p += 1;
                let e = self.expr()?;
                self.expect_sym(")")?;
                Ok(e)
            }
            _ => err("expected a value or column"),
        }
    }

    fn filter(&mut self) -> Result<Option<Expr>> {
        if self.accept_kw("where") {
            Ok(Some(self.expr()?))
        } else {
            Ok(None)
        }
    }

    fn name_list(&mut self) -> Result<Vec<String>> {
        self.expect_sym("(")?;
        let mut v = vec![self.ident()?];
        while self.accept_sym(",") {
            v.push(self.ident()?);
        }
        self.expect_sym(")")?;
        Ok(v)
    }

    fn stmt(&mut self) -> Result<Stmt> {
        if self.accept_kw("begin") {
            return Ok(Stmt::Begin);
        }
        if self.accept_kw("commit") {
            return Ok(Stmt::Commit);
        }
        if self.accept_kw("rollback") {
            return Ok(Stmt::Rollback);
        }
        if self.accept_kw("create") {
            self.expect_kw("table")?;
            let name = self.ident()?;
            self.expect_sym("(")?;
            let (mut cols, mut pk) = (Vec::new(), None);
            loop {
                let c = self.ident()?;
                let ty = if self.accept_kw("int") || self.accept_kw("integer") {
                    ColType::Int
                } else if self.accept_kw("text") {
                    ColType::Text
                } else {
                    return err("column type must be INT or TEXT");
                };
                if self.accept_kw("primary") {
                    self.expect_kw("key")?;
                    if pk.replace(cols.len()).is_some() {
                        return err("only one PRIMARY KEY column is supported");
                    }
                }
                cols.push((c, ty));
                if !self.accept_sym(",") {
                    break;
                }
            }
            self.expect_sym(")")?;
            let Some(pk) = pk else { return err("a PRIMARY KEY column is required") };
            return Ok(Stmt::CreateTable { name, cols, pk });
        }
        if self.accept_kw("insert") {
            self.expect_kw("into")?;
            let table = self.ident()?;
            let cols =
                if matches!(self.peek(), Some(Tok::Sym("("))) { Some(self.name_list()?) } else { None };
            self.expect_kw("values")?;
            let mut rows = Vec::new();
            loop {
                self.expect_sym("(")?;
                let mut r = vec![self.expr()?];
                while self.accept_sym(",") {
                    r.push(self.expr()?);
                }
                self.expect_sym(")")?;
                rows.push(r);
                if !self.accept_sym(",") {
                    break;
                }
            }
            return Ok(Stmt::Insert { table, cols, rows });
        }
        if self.accept_kw("select") {
            let mut items = Vec::new();
            loop {
                if self.accept_sym("*") {
                    items.push(SelItem::Star);
                } else if self.kw("count") {
                    self.p += 1;
                    self.expect_sym("(")?;
                    self.expect_sym("*")?;
                    self.expect_sym(")")?;
                    items.push(SelItem::Count);
                } else if self.kw("sum") {
                    self.p += 1;
                    self.expect_sym("(")?;
                    let c = self.ident()?;
                    self.expect_sym(")")?;
                    items.push(SelItem::Sum(c));
                } else {
                    items.push(SelItem::Col(self.ident()?));
                }
                if !self.accept_sym(",") {
                    break;
                }
            }
            self.expect_kw("from")?;
            let table = self.ident()?;
            let filter = self.filter()?;
            let order = if self.accept_kw("order") {
                self.expect_kw("by")?;
                let c = self.ident()?;
                let desc = self.accept_kw("desc");
                if !desc {
                    self.accept_kw("asc");
                }
                Some((c, desc))
            } else {
                None
            };
            let limit = if self.accept_kw("limit") {
                match self.t.get(self.p) {
                    Some(Tok::Int(n)) if *n >= 0 => {
                        self.p += 1;
                        Some(*n as usize)
                    }
                    _ => return err("LIMIT needs a non-negative integer"),
                }
            } else {
                None
            };
            return Ok(Stmt::Select { items, table, filter, order, limit });
        }
        if self.accept_kw("update") {
            let table = self.ident()?;
            self.expect_kw("set")?;
            let mut sets = Vec::new();
            loop {
                let c = self.ident()?;
                self.expect_sym("=")?;
                sets.push((c, self.expr()?));
                if !self.accept_sym(",") {
                    break;
                }
            }
            return Ok(Stmt::Update { table, sets, filter: self.filter()? });
        }
        if self.accept_kw("delete") {
            self.expect_kw("from")?;
            let table = self.ident()?;
            return Ok(Stmt::Delete { table, filter: self.filter()? });
        }
        err("unsupported statement")
    }
}

pub fn parse(sql: &str) -> Result<Stmt> {
    let mut p = P { t: lex(sql)?, p: 0 };
    let s = p.stmt()?;
    p.accept_sym(";");
    if p.peek().is_some() {
        return err("unexpected input after the end of the statement");
    }
    Ok(s)
}
