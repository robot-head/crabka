//! Recursive-descent statement parser with Pratt expression parsing.

use std::{cell::Cell, rc::Rc};

use crate::{
    ast::{BinaryOp, Expr, UnaryOp},
    error::ParseError,
    lexer::lex,
    token::{Keyword, Token},
};

/// Maximum nesting depth the parser will build before returning `54001`
/// (`statement_too_complex`). This bounds BOTH crash modes:
///   * mode 1 — deep parse recursion (nested parens / subqueries / CASE / NOT /
///     unary minus, all of which funnel through `expr`/`query_expr`), and
///   * mode 2 — a flat left-associative chain (`1+1+1+…`) whose Pratt loop is
///     iterative but builds an N-deep left-nested AST that would overflow later
///     in eval AND on recursive `Box` `Drop`; capping the loop iteration count
///     stops the over-deep tree from ever being built.
///
/// Chosen empirically (see the `at_limit_*` crash-safety tests): the server runs
/// on tokio's default ~2 MiB worker stack, and a query nested at `MAX_DEPTH` must
/// parse AND evaluate without overflowing while a deeper one returns a clean
/// error. Measured on that 2 MiB stack (both plain-debug AND llvm-cov-
/// instrumented builds, since CI runs `cargo llvm-cov nextest`), a deeply-nested
/// `(((…)))` paren parse — the heaviest recursion, an `expr`→`prefix`→`expr`
/// round-trip per level — overflows the stack at a nesting depth of ~133. `50`
/// leaves ~2.6x headroom below that ceiling; the executor's eval recursion
/// (ceiling >12 000 on the same stack) and the AST's recursive `Box` `Drop` are
/// nowhere near it. Real queries nest well under ~50 levels. This cap is
/// deliberately MUCH more conservative than `PostgreSQL`'s own (far higher)
/// `max_stack_depth` — both return `54001` for sufficiently deep input, which is
/// what matters for closing the `DoS`.
pub(crate) const MAX_DEPTH: usize = 50;

type QueryTailAndLocking = (
    Vec<crate::ast::OrderItem>,
    Option<i64>,
    Option<i64>,
    Option<crate::ast::RowLockStrength>,
);

type SetTail = (Vec<crate::ast::OrderItem>, Option<i64>, Option<i64>);

pub(crate) struct Parser {
    toks: Vec<(Token, usize)>,
    source: String,
    pos: usize,
    /// Current recursion depth of the recursive productions (`expr`,
    /// `select_core`). Held behind an `Rc<Cell<…>>` so the RAII [`DepthGuard`]
    /// can hold an OWNED clone of the handle rather than a borrow of `self` —
    /// that lets the guarded method keep calling `&mut self` methods freely while
    /// the guard is alive (a `&self.depth` borrow would conflict with `&mut self`
    /// for the guard's whole lifetime). The guard's `Drop` decrements on EVERY
    /// exit path, including a `?` early-return, so the depth is always restored.
    depth: Rc<Cell<usize>>,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedStatement {
    statement: crate::ast::Statement,
    command_identity: crate::command::CommandIdentity,
}

fn emitted(
    command_identity: crate::command::CommandIdentity,
    statement: Result<crate::ast::Statement, ParseError>,
) -> Result<ParsedStatement, ParseError> {
    statement.map(|statement| ParsedStatement {
        statement,
        command_identity,
    })
}

/// RAII depth counter: increments the shared depth `Cell` on construction and
/// decrements it on `Drop` (so a `?` early-return still restores the count).
/// Holds an owned `Rc` clone, so it does not borrow the `Parser` and never fights
/// the borrow checker with the `&mut self` method calls in the guarded body.
struct DepthGuard {
    depth: Rc<Cell<usize>>,
}

impl DepthGuard {
    /// Enter one recursion level, erroring with `54001` if it would exceed
    /// `MAX_DEPTH`. On error the guard is NOT created (the count is not bumped for
    /// a frame that never ran); the caller returns the error immediately.
    fn enter(depth: &Rc<Cell<usize>>, position: usize) -> Result<Self, ParseError> {
        let next = depth.get() + 1;
        if next > MAX_DEPTH {
            return Err(ParseError::too_deep(position));
        }
        depth.set(next);
        Ok(Self {
            depth: Rc::clone(depth),
        })
    }
}

impl Drop for DepthGuard {
    fn drop(&mut self) {
        self.depth.set(self.depth.get() - 1);
    }
}

impl Parser {
    pub(crate) fn new(toks: Vec<(Token, usize)>, source: String) -> Self {
        Self {
            toks,
            source,
            pos: 0,
            depth: Rc::new(Cell::new(0)),
        }
    }

    fn peek(&self) -> &Token {
        &self.toks[self.pos].0
    }

    /// The token *after* the current one (saturates at EOF). Used for the SP28
    /// two-token lookahead that disambiguates infix `NOT IN`/`NOT BETWEEN`/
    /// `NOT LIKE` from the prefix `NOT` operator.
    fn peek2(&self) -> &Token {
        let i = (self.pos + 1).min(self.toks.len() - 1);
        &self.toks[i].0
    }

    /// The token `n` positions ahead of the current one (saturates at EOF).
    fn peek_n(&self, n: usize) -> &Token {
        let i = (self.pos + n).min(self.toks.len() - 1);
        &self.toks[i].0
    }

    /// The token two positions after the current one (saturates at EOF). Used by
    /// the SP37 `AT TIME ZONE` postfix, whose three-token lead-in (`at time zone`)
    /// needs a three-token lookahead so a bare column named `at` is never mistaken
    /// for the operator.
    fn peek3(&self) -> &Token {
        let i = (self.pos + 2).min(self.toks.len() - 1);
        &self.toks[i].0
    }

    fn peek_pos(&self) -> usize {
        self.toks[self.pos].1
    }

    fn bump(&mut self) -> Token {
        let t = self.toks[self.pos].0.clone();
        if self.pos + 1 < self.toks.len() {
            self.pos += 1;
        }
        t
    }

    fn eat_keyword(&mut self, kw: Keyword) -> bool {
        if *self.peek() == Token::Keyword(kw) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, want: &Token) -> Result<(), ParseError> {
        if self.peek() == want {
            self.bump();
            Ok(())
        } else {
            Err(ParseError::new(
                format!("expected {want:?}, found {:?}", self.peek()),
                self.peek_pos(),
            ))
        }
    }

    fn expect_ident(&mut self) -> Result<String, ParseError> {
        match self.bump() {
            Token::Ident(s) => Ok(s),
            other => Err(ParseError::new(
                format!("expected identifier, found {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    /// Consume an identifier that must equal `want` (case-insensitively). Used by
    /// the SP37 keyword-free multi-word type fold (`time`/`zone`) so those words
    /// stay non-reserved (still usable as identifiers elsewhere).
    fn expect_ident_eq(&mut self, want: &str) -> Result<(), ParseError> {
        let pos = self.peek_pos();
        match self.bump() {
            Token::Ident(s) if s.eq_ignore_ascii_case(want) => Ok(()),
            other => Err(ParseError::new(
                format!("expected `{want}`, found {other:?}"),
                pos,
            )),
        }
    }

    fn eat_ident_eq(&mut self, want: &str) -> bool {
        if matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case(want)) {
            self.bump();
            true
        } else {
            false
        }
    }

    /// Parse a SQL type name into a [`ColumnType`] — shared by `CREATE TABLE`
    /// column definitions and the SP31 cast target (`CAST(_ AS ty)` / `_::ty`).
    /// Folds the two-word `double precision` (SP30) into one normalized name; an
    /// unknown type name is a 42601 parse error (`PostgreSQL`: 42704 — a documented
    /// deviation, consistent with the column-type path).
    fn parse_type_name(&mut self) -> Result<crabka_pgtypes::ColumnType, ParseError> {
        let type_pos = self.peek_pos();
        let mut type_word = self.expect_ident()?;
        // PostgreSQL allows qualifying any built-in type with its schema
        // (`$1::pg_catalog.regclass`, `x::pg_catalog.text`); built-ins all
        // live in pg_catalog, so the qualifier resolves to the bare name.
        if type_word == "pg_catalog" && *self.peek() == Token::Dot {
            self.bump();
            type_word = self.expect_ident()?;
        }
        if type_word.eq_ignore_ascii_case("double")
            && matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("precision"))
        {
            self.bump();
            type_word = "double precision".to_string();
        }
        if type_word.eq_ignore_ascii_case("character")
            && matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("varying"))
        {
            self.bump();
            type_word = "character varying".to_string();
        }
        // SP37: fold the multi-word `timestamp`/`time` { with | without } `time zone`
        // spellings into one normalized name (keyword-free — the lexer lowercases
        // idents, so the three trailing words are matched as plain `Token::Ident`s).
        // `timestamp with time zone` / `timestamp without time zone` /
        // `time with time zone` / `time without time zone`.
        if (type_word.eq_ignore_ascii_case("timestamp") || type_word.eq_ignore_ascii_case("time"))
            && (matches!(self.peek(), Token::Keyword(Keyword::With))
                || matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("without")))
            && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("time"))
        {
            // Consume `{with|without}`; require `time` `zone` to follow.
            let with_zone = matches!(self.bump(), Token::Keyword(Keyword::With));
            self.expect_ident_eq("time")?;
            self.expect_ident_eq("zone")?;
            let qualifier = if with_zone { "with" } else { "without" };
            type_word = format!("{} {qualifier} time zone", type_word.to_ascii_lowercase());
        }
        let ty = crabka_pgtypes::ColumnType::from_sql_name(&type_word)
            .ok_or_else(|| ParseError::new(format!("unknown type \"{type_word}\""), type_pos))?;
        // SP32: `numeric`/`decimal` may carry a `(precision[, scale])` modifier.
        if ty.is_numeric() && *self.peek() == Token::LParen {
            return self.parse_numeric_typmod();
        }
        if matches!(
            ty,
            crabka_pgtypes::ColumnType::Varchar(_) | crabka_pgtypes::ColumnType::Char(_)
        ) && *self.peek() == Token::LParen
        {
            return self.parse_string_typmod(ty);
        }
        Ok(ty)
    }

    fn parse_string_typmod(
        &mut self,
        ty: crabka_pgtypes::ColumnType,
    ) -> Result<crabka_pgtypes::ColumnType, ParseError> {
        self.expect(&Token::LParen)?;
        let limit = self.expect_u16("string length")?;
        self.expect(&Token::RParen)?;
        match ty {
            crabka_pgtypes::ColumnType::Varchar(_) => {
                Ok(crabka_pgtypes::ColumnType::Varchar(Some(limit)))
            }
            crabka_pgtypes::ColumnType::Char(_) => {
                Ok(crabka_pgtypes::ColumnType::Char(Some(limit)))
            }
            _ => unreachable!("parse_string_typmod called for non-string typmod type"),
        }
    }

    /// Parse a `numeric(precision[, scale])` modifier, positioned at `(`. `scale`
    /// defaults to 0 (`PostgreSQL` `numeric(p)` ≡ `numeric(p, 0)`).
    fn parse_numeric_typmod(&mut self) -> Result<crabka_pgtypes::ColumnType, ParseError> {
        self.expect(&Token::LParen)?;
        let precision = self.expect_u16("numeric precision")?;
        let scale = if self.eat_comma() {
            self.expect_u16("numeric scale")?
        } else {
            0
        };
        self.expect(&Token::RParen)?;
        Ok(crabka_pgtypes::ColumnType::Numeric(Some(
            crabka_pgtypes::numeric::Typmod { precision, scale },
        )))
    }

    /// Parse a small unsigned integer literal (a `numeric` precision/scale).
    fn expect_u16(&mut self, what: &str) -> Result<u16, ParseError> {
        let pos = self.peek_pos();
        match self.bump() {
            Token::IntLit(s) => s
                .parse::<u16>()
                .map_err(|_| ParseError::new(format!("invalid {what}"), pos)),
            other => Err(ParseError::new(
                format!("expected {what}, found {other:?}"),
                pos,
            )),
        }
    }

    /// Pratt expression parser. `min_bp` is the minimum left binding power.
    pub(crate) fn expr(&mut self, min_bp: u8) -> Result<Expr, ParseError> {
        // Mode-1 guard: every recursive expression production (parens, NOT, unary
        // minus, CASE, CAST, IN-list, BETWEEN, LIKE, function args, subqueries)
        // funnels back through `expr`, so bounding the recursion depth here caps
        // all of them. The RAII guard decrements on every exit path, `?` included.
        let _guard = DepthGuard::enter(&self.depth, self.peek_pos())?;
        let mut lhs = self.prefix()?;
        // Mode-2 guard: the Pratt loop is iterative, but each iteration adds one
        // level of left-nesting to the result tree (`1+1+1+…`). Capping the
        // iteration count caps the built tree's depth, so it can never grow deep
        // enough to overflow eval/fold/router-walk or recursive `Box` `Drop`.
        let mut iterations: usize = 0;
        loop {
            iterations += 1;
            if iterations > MAX_DEPTH {
                return Err(ParseError::too_deep(self.peek_pos()));
            }
            // SP31: `::` is the tightest-binding operator (tighter than unary
            // minus and every arithmetic/comparison operator), so it is consumed
            // unconditionally here — no `min_bp` gate — and left-associatively
            // (`a::int::text` == `(a::int)::text`). `-2::int` still parses as
            // `-(2::int)` because the unary-minus prefix recurses into `expr`,
            // whose innermost frame grabs the `::` before the minus is applied.
            if *self.peek() == Token::TypeCast {
                self.bump();
                let ty = self.parse_type_name()?;
                lhs = Expr::Cast {
                    expr: Box::new(lhs),
                    ty,
                };
                continue;
            }
            // SP37: `x AT TIME ZONE z` — a postfix operator that lowers onto PG's
            // internal `timezone(z, x)` form (note arg ORDER: zone first, value
            // second). It binds TIGHTER than every binary operator (so
            // `ts AT TIME ZONE 'UTC' = y` groups as `(ts AT TIME ZONE 'UTC') = y`),
            // so — like `::` — it is consumed unconditionally (no `min_bp` gate).
            // Keyword-free: `at`/`time`/`zone` are matched as lowercased idents via
            // a three-token lookahead, so a bare column named `at` is never the
            // operator. The zone operand is parsed at bp 11 (a high-precedence
            // operand, like the `*`/`/` level), and recursion terminates because
            // each iteration consumes the `at time zone` lead-in before recursing.
            if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("at"))
                && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("time"))
                && matches!(self.peek3(), Token::Ident(w) if w.eq_ignore_ascii_case("zone"))
            {
                self.bump(); // at
                self.bump(); // time
                self.bump(); // zone
                let zone = self.expr(11)?;
                lhs = Expr::Func(crate::ast::FuncCall {
                    name: "timezone".into(),
                    distinct: false,
                    args: crate::ast::FuncArgs::Exprs(vec![zone, lhs]),
                });
                continue;
            }
            // SP28: postfix predicates (IS [NOT] NULL, [NOT] IN, [NOT] BETWEEN,
            // [NOT] LIKE/ILIKE) bind at the comparison level (l_bp = 5). They are
            // handled before the binary-operator match so `a = 1 AND b IN (1,2)`
            // groups as `(a=1) AND (b IN (1,2))`.
            if 5 >= min_bp {
                match self.peek() {
                    Token::Keyword(Keyword::Is) => {
                        lhs = self.parse_is_null(lhs)?;
                        continue;
                    }
                    Token::Keyword(Keyword::In) => {
                        lhs = self.parse_in(lhs, false)?;
                        continue;
                    }
                    Token::Keyword(Keyword::Between) => {
                        lhs = self.parse_between(lhs, false)?;
                        continue;
                    }
                    Token::Keyword(Keyword::Like) => {
                        lhs = self.parse_like(lhs, false, false)?;
                        continue;
                    }
                    Token::Keyword(Keyword::Ilike) => {
                        lhs = self.parse_like(lhs, false, true)?;
                        continue;
                    }
                    // Infix `NOT` only when it leads a negated predicate
                    // (`x NOT IN/BETWEEN/LIKE/ILIKE …`); otherwise `NOT` is the
                    // prefix operator handled in `prefix`. Two-token lookahead.
                    Token::Keyword(Keyword::Not)
                        if matches!(
                            self.peek2(),
                            Token::Keyword(
                                Keyword::In | Keyword::Between | Keyword::Like | Keyword::Ilike
                            )
                        ) =>
                    {
                        self.bump(); // NOT
                        lhs = match self.peek() {
                            Token::Keyword(Keyword::In) => self.parse_in(lhs, true)?,
                            Token::Keyword(Keyword::Between) => self.parse_between(lhs, true)?,
                            Token::Keyword(Keyword::Like) => self.parse_like(lhs, true, false)?,
                            Token::Keyword(Keyword::Ilike) => self.parse_like(lhs, true, true)?,
                            _ => unreachable!("lookahead guaranteed a negated predicate"),
                        };
                        continue;
                    }
                    _ => {}
                }
            }
            // SP29 inserts `||` (BinaryOp::Concat) between the comparison level
            // (5/6) and the additive level: like PostgreSQL, `||` binds TIGHTER
            // than `< > = <= >= <>`, `BETWEEN/IN/LIKE`, `AND`/`OR` but LOOSER than
            // `+ - * /`. So `+ - * /` and the unary-minus operand power shift up by
            // two to make room (odd l_bp / even r_bp preserved).
            let (op, l_bp, r_bp) = match self.peek() {
                Token::Keyword(Keyword::Or) => (BinaryOp::Or, 1, 2),
                Token::Keyword(Keyword::And) => (BinaryOp::And, 3, 4),
                Token::Eq => (BinaryOp::Eq, 5, 6),
                Token::Ne => (BinaryOp::Ne, 5, 6),
                Token::Lt => (BinaryOp::Lt, 5, 6),
                Token::Le => (BinaryOp::Le, 5, 6),
                Token::Gt => (BinaryOp::Gt, 5, 6),
                Token::Ge => (BinaryOp::Ge, 5, 6),
                Token::Concat => (BinaryOp::Concat, 7, 8),
                Token::Plus => (BinaryOp::Add, 9, 10),
                Token::Minus => (BinaryOp::Sub, 9, 10),
                Token::Star => (BinaryOp::Mul, 11, 12),
                Token::Slash => (BinaryOp::Div, 11, 12),
                _ => break,
            };
            if l_bp < min_bp {
                break;
            }
            self.bump();
            // SP34: `op ANY|SOME|ALL ( SELECT … )` — a quantified comparison. Only
            // the comparison operators take a quantifier (PostgreSQL).
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::Ne
                    | BinaryOp::Lt
                    | BinaryOp::Le
                    | BinaryOp::Gt
                    | BinaryOp::Ge
            ) && matches!(
                self.peek(),
                Token::Keyword(Keyword::Any | Keyword::Some | Keyword::All)
            ) {
                let all = matches!(self.peek(), Token::Keyword(Keyword::All));
                self.bump(); // ANY / SOME / ALL
                self.expect(&Token::LParen)?;
                let subquery = Box::new(self.query_expr_after_open_paren()?);
                lhs = Expr::Quantified {
                    expr: Box::new(lhs),
                    op,
                    all,
                    subquery,
                };
                continue;
            }
            let rhs = self.expr(r_bp)?;
            lhs = Expr::Binary {
                op,
                left: Box::new(lhs),
                right: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn prefix(&mut self) -> Result<Expr, ParseError> {
        match self.peek().clone() {
            Token::Keyword(Keyword::Not) => {
                self.bump();
                Ok(Expr::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(self.expr(4)?),
                })
            }
            Token::Minus => {
                self.bump();
                // Unary minus binds tighter than `* /` (now 11/12), so its operand
                // is parsed above that level (13) — `-a * b` stays `(-a) * b`.
                Ok(Expr::Unary {
                    op: UnaryOp::Neg,
                    expr: Box::new(self.expr(13)?),
                })
            }
            Token::LParen => {
                // SP34: `( SELECT … )` is a scalar subquery; anything else is a
                // parenthesised (grouping) expression.
                if matches!(
                    self.peek2(),
                    Token::Keyword(Keyword::Select | Keyword::Values | Keyword::With)
                ) {
                    self.bump();
                    let sub = self.query_expr_after_open_paren()?;
                    Ok(Expr::ScalarSubquery(Box::new(sub)))
                } else {
                    self.bump();
                    let e = self.expr(0)?;
                    self.expect(&Token::RParen)?;
                    Ok(e)
                }
            }
            Token::Keyword(Keyword::Exists) => {
                self.bump(); // EXISTS
                self.expect(&Token::LParen)?;
                let sub = self.query_expr_after_open_paren()?;
                Ok(Expr::Exists(Box::new(sub)))
            }
            Token::IntLit(s) => {
                self.bump();
                Ok(Expr::IntLiteral(s))
            }
            Token::FloatLit(s) => {
                self.bump();
                Ok(Expr::NumericLiteral(s))
            }
            Token::StringLit(s) => {
                self.bump();
                Ok(Expr::StringLiteral(s))
            }
            Token::Keyword(Keyword::True) => {
                self.bump();
                Ok(Expr::BoolLiteral(true))
            }
            Token::Keyword(Keyword::False) => {
                self.bump();
                Ok(Expr::BoolLiteral(false))
            }
            Token::Keyword(Keyword::Null) => {
                self.bump();
                Ok(Expr::NullLiteral)
            }
            Token::Keyword(Keyword::Case) => self.case_expr(),
            Token::Keyword(Keyword::Cast) => self.cast_expr(),
            Token::Keyword(Keyword::CurrentUser) => {
                self.bump();
                Ok(Expr::Func(crate::ast::FuncCall {
                    name: "current_user".into(),
                    distinct: false,
                    args: crate::ast::FuncArgs::Exprs(vec![]),
                }))
            }
            // `left`/`right` are PostgreSQL scalar functions AND (LEFT/RIGHT) join
            // keywords. In expression position they are valid only as a function
            // call — `left(s, n)` / `right(s, n)` — so route them to `func_call`.
            Token::Keyword(Keyword::Left) => self.keyword_func_call("left"),
            Token::Keyword(Keyword::Right) => self.keyword_func_call("right"),
            Token::Param(n) => {
                self.bump();
                Ok(Expr::Param(n))
            }
            Token::Ident(s) => {
                // SP37: a typed datetime literal — `DATE '…'` / `TIME '…'` /
                // `TIMESTAMP '…'` / `TIMESTAMPTZ '…'` / `INTERVAL '…'` — lowers onto
                // an explicit cast of a string literal. Only the single-word
                // spellings are typed-literal prefixes (multi-word `TIMESTAMP WITH
                // TIME ZONE '…'` is out of scope — use `'…'::timestamptz`). This is
                // checked BEFORE the function-call path so `date('…')` is not
                // shadowed (a typed literal has NO parenthesis — `peek2` is the
                // string literal, not `(`).
                let lower = s.to_ascii_lowercase();
                if matches!(
                    lower.as_str(),
                    "date" | "time" | "timestamp" | "timestamptz" | "interval"
                ) && matches!(self.peek2(), Token::StringLit(_))
                {
                    self.bump(); // the type-name ident
                    let ty = crabka_pgtypes::ColumnType::from_sql_name(&lower)
                        .expect("single-word datetime type name resolves");
                    let Token::StringLit(string) = self.bump() else {
                        unreachable!("peek2 guaranteed a string literal");
                    };
                    return Ok(Expr::Cast {
                        expr: Box::new(Expr::StringLiteral(string)),
                        ty,
                    });
                }
                self.bump();
                // SP37: niladic keyword functions — `current_date`, `current_time`,
                // `localtimestamp`, `localtime`, `current_timestamp` — have NO
                // parentheses. When one of these names is NOT followed by `(`, build
                // a zero-arg `Func` call (the executor resolves it against the session
                // clock/zone). These names are effectively reserved in PostgreSQL, so
                // shadowing a column of the same name is acceptable. The paren forms
                // (`now()`, `current_timestamp(0)`, etc.) fall through to `func_call`.
                if matches!(
                    lower.as_str(),
                    "current_date"
                        | "current_time"
                        | "session_user"
                        | "localtimestamp"
                        | "localtime"
                        | "current_timestamp"
                ) && *self.peek() != Token::LParen
                {
                    return Ok(Expr::Func(crate::ast::FuncCall {
                        name: lower,
                        distinct: false,
                        args: crate::ast::FuncArgs::Exprs(vec![]),
                    }));
                }
                // SP37: `EXTRACT(field FROM source)` — a special call form that
                // lowers onto `extract('<field>', source)` (field lowercased to a
                // string literal). Checked before the generic comma-arg `func_call`
                // so the `FROM` keyword inside the parens is not mis-parsed.
                if lower == "extract" && *self.peek() == Token::LParen {
                    return self.extract_expr();
                }
                // SP27: `ident (` is a function call; a bare ident is a column.
                // SP33/F-2: `ident . ident` is either a schema-qualified function
                // call (`pg_catalog.format_type(...)`) or a table-qualified column.
                if *self.peek() == Token::LParen {
                    self.func_call(s)
                } else if *self.peek() == Token::Dot {
                    self.bump();
                    let name = self.expect_ident()?;
                    if *self.peek() == Token::LParen {
                        self.func_call(name)
                    } else {
                        Ok(Expr::Column {
                            table: Some(s),
                            name,
                        })
                    }
                } else {
                    Ok(Expr::Column {
                        table: None,
                        name: s,
                    })
                }
            }
            other => Err(ParseError::new(
                format!("unexpected token {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    /// Parse a function call after its name `ident`, positioned at `(`.
    /// `f(*)` yields [`FuncArgs::Star`]; `DISTINCT`/`ALL` may lead the argument
    /// list; otherwise a (possibly empty) comma-separated expression list.
    fn func_call(&mut self, name: String) -> Result<Expr, ParseError> {
        use crate::ast::{FuncArgs, FuncCall};
        self.expect(&Token::LParen)?;
        // `f(*)` — the star form (no DISTINCT, no other args).
        if *self.peek() == Token::Star {
            self.bump();
            self.expect(&Token::RParen)?;
            return Ok(Expr::Func(FuncCall {
                name,
                distinct: false,
                args: FuncArgs::Star,
            }));
        }
        let distinct = if self.eat_keyword(Keyword::Distinct) {
            true
        } else {
            // ALL is the default modifier; accept and ignore it.
            self.eat_keyword(Keyword::All);
            false
        };
        let mut args = Vec::new();
        if *self.peek() != Token::RParen {
            loop {
                args.push(self.expr(0)?);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
        }
        self.expect(&Token::RParen)?;
        Ok(Expr::Func(FuncCall {
            name,
            distinct,
            args: FuncArgs::Exprs(args),
        }))
    }

    /// A keyword that doubles as a scalar function name (`left`/`right`, which are
    /// also join keywords) used in expression position: positioned at the keyword,
    /// it is valid only as a function call `kw (`.
    fn keyword_func_call(&mut self, name: &str) -> Result<Expr, ParseError> {
        self.bump();
        if *self.peek() == Token::LParen {
            self.func_call(name.to_string())
        } else {
            Err(ParseError::new(
                format!("`{name}` is reserved here; use it as a function call `{name}(...)`"),
                self.peek_pos(),
            ))
        }
    }

    /// `EXTRACT(field FROM source)` — positioned at `(`, after the `extract` ident.
    /// Lowers onto `PostgreSQL`'s internal `extract('<field>', source)` form: the
    /// field is an identifier (lowercased to a string literal), the source is a
    /// full expression. The executor resolves the field at runtime.
    fn extract_expr(&mut self) -> Result<Expr, ParseError> {
        use crate::ast::{FuncArgs, FuncCall};
        self.expect(&Token::LParen)?;
        let field = self.expect_ident()?.to_ascii_lowercase();
        self.expect(&Token::Keyword(Keyword::From))?;
        let source = self.expr(0)?;
        self.expect(&Token::RParen)?;
        Ok(Expr::Func(FuncCall {
            name: "extract".into(),
            distinct: false,
            args: FuncArgs::Exprs(vec![Expr::StringLiteral(field), source]),
        }))
    }

    /// `expr IS [NOT] NULL`, positioned at `IS`. (`IS TRUE`/`IS DISTINCT FROM`
    /// are out of scope — anything but `NULL` after `IS`/`IS NOT` is a 42601.)
    fn parse_is_null(&mut self, lhs: Expr) -> Result<Expr, ParseError> {
        self.expect(&Token::Keyword(Keyword::Is))?;
        let negated = self.eat_keyword(Keyword::Not);
        self.expect(&Token::Keyword(Keyword::Null))?;
        Ok(Expr::IsNull {
            expr: Box::new(lhs),
            negated,
        })
    }

    /// `expr [NOT] IN (e1, e2, …)` or `expr [NOT] IN (SELECT …)`, positioned at
    /// `IN`. The value-list form has ≥1 element (`IN ()` is a 42601, matching
    /// `PostgreSQL`); the `SELECT` form (SP34) is a single-column subquery.
    fn parse_in(&mut self, lhs: Expr, negated: bool) -> Result<Expr, ParseError> {
        self.expect(&Token::Keyword(Keyword::In))?;
        self.expect(&Token::LParen)?;
        // SP34: `IN ( SELECT … )` is a subquery; otherwise a value list.
        if matches!(
            self.peek(),
            Token::Keyword(Keyword::Select | Keyword::Values | Keyword::With)
        ) {
            let subquery = self.query_expr_after_open_paren()?;
            return Ok(Expr::InSubquery {
                expr: Box::new(lhs),
                subquery: Box::new(subquery),
                negated,
            });
        }
        let mut list = Vec::new();
        loop {
            list.push(self.expr(0)?);
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        Ok(Expr::InList {
            expr: Box::new(lhs),
            list,
            negated,
        })
    }

    /// `expr [NOT] BETWEEN low AND high`, positioned at `BETWEEN`. The bounds are
    /// parsed at `min_bp = 4` so the separating `AND` (left bp 3) is NOT consumed
    /// as a boolean `AND`; thus `a BETWEEN 1 AND 2 AND b` → `(a BETWEEN 1 AND 2) AND b`.
    fn parse_between(&mut self, lhs: Expr, negated: bool) -> Result<Expr, ParseError> {
        self.expect(&Token::Keyword(Keyword::Between))?;
        let low = self.expr(4)?;
        self.expect(&Token::Keyword(Keyword::And))?;
        let high = self.expr(4)?;
        Ok(Expr::Between {
            expr: Box::new(lhs),
            low: Box::new(low),
            high: Box::new(high),
            negated,
        })
    }

    /// `expr [NOT] LIKE pat` / `[NOT] ILIKE pat`, positioned at `LIKE`/`ILIKE`.
    /// The pattern is parsed at `min_bp = 6` (the right bp of the comparison
    /// level) so it stays a single comparand and does not swallow a trailing
    /// `AND`/`OR`.
    fn parse_like(
        &mut self,
        lhs: Expr,
        negated: bool,
        case_insensitive: bool,
    ) -> Result<Expr, ParseError> {
        self.bump(); // LIKE or ILIKE
        let pattern = self.expr(6)?;
        Ok(Expr::Like {
            expr: Box::new(lhs),
            pattern: Box::new(pattern),
            negated,
            case_insensitive,
        })
    }

    /// A `CASE` expression. Simple form (`CASE x WHEN v THEN r …`) carries an
    /// operand; searched form (`CASE WHEN cond THEN r …`) does not. At least one
    /// `WHEN` is required; `ELSE` is optional.
    fn case_expr(&mut self) -> Result<Expr, ParseError> {
        self.expect(&Token::Keyword(Keyword::Case))?;
        let operand = if *self.peek() == Token::Keyword(Keyword::When) {
            None
        } else {
            Some(Box::new(self.expr(0)?))
        };
        let mut whens = Vec::new();
        while self.eat_keyword(Keyword::When) {
            let cond = self.expr(0)?;
            self.expect(&Token::Keyword(Keyword::Then))?;
            let result = self.expr(0)?;
            whens.push((cond, result));
        }
        if whens.is_empty() {
            return Err(ParseError::new(
                "CASE requires at least one WHEN clause",
                self.peek_pos(),
            ));
        }
        let else_result = if self.eat_keyword(Keyword::Else) {
            Some(Box::new(self.expr(0)?))
        } else {
            None
        };
        self.expect(&Token::Keyword(Keyword::End))?;
        Ok(Expr::Case {
            operand,
            whens,
            else_result,
        })
    }

    /// `CAST(expr AS type)` — positioned at `CAST`. The functional spelling of
    /// the `::` operator; the inner expression is parsed at the lowest precedence
    /// (it is delimited by the surrounding parens).
    fn cast_expr(&mut self) -> Result<Expr, ParseError> {
        self.expect(&Token::Keyword(Keyword::Cast))?;
        self.expect(&Token::LParen)?;
        let expr = self.expr(0)?;
        self.expect(&Token::Keyword(Keyword::As))?;
        let ty = self.parse_type_name()?;
        self.expect(&Token::RParen)?;
        Ok(Expr::Cast {
            expr: Box::new(expr),
            ty,
        })
    }

    /// Pairs each statement with the byte range of its source in
    /// the original input — from its first token's offset up to the trailing `;`
    /// (or end of input). Powers [`parse_with_source`].
    fn program_spanned(
        &mut self,
    ) -> Result<Vec<(ParsedStatement, std::ops::Range<usize>)>, ParseError> {
        let mut stmts = Vec::new();
        loop {
            while *self.peek() == Token::Semicolon {
                self.bump();
            }
            if *self.peek() == Token::Eof {
                break;
            }
            let start = self.peek_pos();
            let s = self.statement()?;
            let end = self.peek_pos();
            stmts.push((s, start..end));
            match self.peek() {
                Token::Semicolon => {
                    self.bump();
                }
                Token::Eof => break,
                other => {
                    return Err(ParseError::new(
                        format!("expected ; or end of input, found {other:?}"),
                        self.peek_pos(),
                    ));
                }
            }
        }
        Ok(stmts)
    }

    fn statement(&mut self) -> Result<ParsedStatement, ParseError> {
        use crate::command::CommandIdentity as I;

        if self.starts_query_expr() {
            let identity = self.query_command_identity();
            return emitted(identity, self.query_statement());
        }
        match self.peek() {
            Token::Keyword(Keyword::Create) => {
                // SP40: lookahead dispatch for FDW DDL vs CREATE TABLE
                match self.peek2() {
                    Token::Keyword(
                        Keyword::Index | Keyword::Unique | Keyword::Global | Keyword::Local,
                    ) => emitted(I::CreateIndex, self.create_index()),
                    Token::Keyword(Keyword::View) => emitted(I::CreateView, self.create_view()),
                    Token::Keyword(Keyword::Foreign) => {
                        // CREATE FOREIGN ... — look at the third token
                        match self.peek3() {
                            Token::Keyword(Keyword::Table) => {
                                emitted(I::CreateForeignTable, self.create_foreign_table())
                            }
                            Token::Keyword(Keyword::Data) => {
                                emitted(I::CreateForeignDataWrapper, self.create_fdw())
                            }
                            _ => Err(ParseError::new(
                                format!(
                                    "unexpected token after CREATE FOREIGN: {:?}",
                                    self.peek3()
                                ),
                                self.peek_pos(),
                            )),
                        }
                    }
                    Token::Keyword(Keyword::Server) => {
                        emitted(I::CreateServer, self.create_server())
                    }
                    Token::Keyword(Keyword::User) => {
                        if matches!(self.peek3(), Token::Keyword(Keyword::Mapping)) {
                            emitted(I::CreateUserMapping, self.create_user_mapping())
                        } else {
                            emitted(I::CreateUser, self.create_role(true))
                        }
                    }
                    Token::Ident(s) if s == "role" => {
                        emitted(I::CreateRole, self.create_role(false))
                    }
                    Token::Ident(s) if s == "sequence" => {
                        emitted(I::CreateSequence, self.create_sequence())
                    }
                    Token::Ident(s) if s == "database" => {
                        emitted(I::CreateDatabase, self.create_database_refusal())
                    }
                    _ => emitted(I::CreateTable, self.create_table()),
                }
            }
            Token::Keyword(Keyword::Drop) => {
                // SP40: lookahead dispatch for FDW DDL vs DROP TABLE.
                // Each drop function handles its own `IF EXISTS` via `eat_if_exists`.
                match self.peek2() {
                    Token::Keyword(Keyword::Foreign) => {
                        // DROP FOREIGN ... — look at the third token to distinguish
                        // DROP FOREIGN DATA WRAPPER from DROP FOREIGN TABLE.
                        match self.peek3() {
                            Token::Keyword(Keyword::Table) => {
                                emitted(I::DropForeignTable, self.drop_foreign_table())
                            }
                            Token::Keyword(Keyword::Data) => {
                                emitted(I::DropForeignDataWrapper, self.drop_fdw())
                            }
                            _ => Err(ParseError::new(
                                format!("unexpected token after DROP FOREIGN: {:?}", self.peek3()),
                                self.peek_pos(),
                            )),
                        }
                    }
                    Token::Keyword(Keyword::Server) => emitted(I::DropServer, self.drop_server()),
                    Token::Keyword(Keyword::View) => emitted(I::DropView, self.drop_view()),
                    Token::Keyword(Keyword::Index) => emitted(I::DropIndex, self.drop_index()),
                    Token::Keyword(Keyword::User) => {
                        if matches!(self.peek3(), Token::Keyword(Keyword::Mapping)) {
                            emitted(I::DropUserMapping, self.drop_user_mapping())
                        } else {
                            emitted(I::DropUser, self.drop_role())
                        }
                    }
                    Token::Ident(s) if s == "role" => emitted(I::DropRole, self.drop_role()),
                    Token::Ident(s) if s == "sequence" => {
                        emitted(I::DropSequence, self.drop_sequence())
                    }
                    Token::Ident(s) if s == "database" => {
                        emitted(I::DropDatabase, self.drop_database_refusal())
                    }
                    Token::Ident(s) if s == "extension" => {
                        emitted(I::DropExtension, self.drop_extension_refusal())
                    }
                    _ => emitted(I::DropTable, self.drop_table()),
                }
            }
            Token::Ident(s) if s == "truncate" => emitted(I::Truncate, self.truncate()),
            Token::Ident(s) if s == "vacuum" => emitted(I::Vacuum, self.vacuum()),
            Token::Ident(s) if s == "grant" => emitted(I::Grant, self.grant_table_privileges()),
            Token::Ident(s) if s == "revoke" => emitted(I::Revoke, self.revoke_table_privileges()),
            Token::Keyword(Keyword::Import) => {
                emitted(I::ImportForeignSchema, self.import_foreign_schema())
            }
            Token::Keyword(Keyword::Insert) => emitted(I::Insert, self.insert()),
            Token::Keyword(Keyword::Copy) => emitted(I::Copy, self.copy_stmt()),
            // SP4: transaction control
            Token::Keyword(Keyword::Begin) => emitted(I::Begin, self.begin()),
            Token::Keyword(Keyword::Start) => emitted(I::StartTransaction, self.begin()),
            Token::Keyword(Keyword::Commit) if matches!(self.peek2(), Token::Ident(s) if s == "prepared") => {
                emitted(
                    I::CommitPrepared,
                    self.prepared_transaction_refusal(crate::ast::RefusalCommand::CommitPrepared),
                )
            }
            Token::Keyword(Keyword::Rollback) if matches!(self.peek2(), Token::Ident(s) if s == "prepared") => {
                emitted(
                    I::RollbackPrepared,
                    self.prepared_transaction_refusal(crate::ast::RefusalCommand::RollbackPrepared),
                )
            }
            Token::Keyword(keyword @ (Keyword::Commit | Keyword::End)) => {
                let identity = if *keyword == Keyword::Commit {
                    I::Commit
                } else {
                    I::End
                };
                self.bump();
                emitted(identity, Ok(crate::ast::Statement::Commit))
            }
            Token::Keyword(keyword @ (Keyword::Rollback | Keyword::Abort)) => {
                let identity = if *keyword == Keyword::Rollback {
                    I::Rollback
                } else {
                    I::Abort
                };
                self.bump();
                emitted(identity, Ok(crate::ast::Statement::Rollback))
            }
            // SP4: DML
            Token::Keyword(Keyword::Update) => emitted(I::Update, self.update()),
            Token::Keyword(Keyword::Delete) => emitted(I::Delete, self.delete()),
            // SP37: GUC control. `SET` is a keyword; `SHOW`/`RESET`/`ALTER` are matched as
            // plain (lowercased) idents — keyword-free so they stay usable as names.
            Token::Keyword(Keyword::Set) => {
                if matches!(self.peek2(), Token::Ident(s) if s == "role") {
                    emitted(I::SetRole, self.set_role_stmt())
                } else if matches!(self.peek2(), Token::Keyword(Keyword::Transaction)) {
                    emitted(I::SetTransaction, self.set_stmt())
                } else {
                    emitted(I::Set, self.set_stmt())
                }
            }
            Token::Ident(s) if s == "show" => emitted(I::Show, self.show_stmt()),
            Token::Ident(s) if s == "reset" => emitted(I::Reset, self.reset_stmt()),
            Token::Ident(s) if s == "discard" => emitted(I::Discard, self.discard_stmt()),
            Token::Ident(s)
                if s == "prepare"
                    && matches!(self.peek2(), Token::Keyword(Keyword::Transaction)) =>
            {
                emitted(
                    I::PrepareTransaction,
                    self.prepared_transaction_refusal(
                        crate::ast::RefusalCommand::PrepareTransaction,
                    ),
                )
            }
            // SP40: ALTER SERVER / ALTER USER MAPPING; bounded ALTER TABLE rename.
            Token::Ident(s) if s == "alter" => match self.peek2() {
                Token::Keyword(Keyword::Table) => emitted(I::AlterTable, self.alter_table()),
                Token::Keyword(Keyword::Server) => emitted(I::AlterServer, self.alter_server()),
                Token::Keyword(Keyword::User) => {
                    emitted(I::AlterUserMapping, self.alter_user_mapping())
                }
                Token::Ident(s) if s == "database" => {
                    emitted(I::AlterDatabase, self.alter_database_refusal())
                }
                Token::Ident(s) if s == "extension" => {
                    emitted(I::AlterExtension, self.alter_extension_refusal())
                }
                _ => Err(ParseError::new(
                    format!("unexpected token after ALTER: {:?}", self.peek2()),
                    self.peek_pos(),
                )),
            },
            other => Err(ParseError::new(
                format!("unexpected statement start {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    fn query_command_identity(&self) -> crate::command::CommandIdentity {
        use crate::command::CommandIdentity;

        let mut offset = 0;
        while matches!(self.peek_n(offset), Token::LParen) {
            offset += 1;
        }
        if matches!(self.peek_n(offset), Token::Keyword(Keyword::Values)) {
            CommandIdentity::Values
        } else {
            CommandIdentity::Select
        }
    }

    fn create_database_refusal(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect_ident_eq("database")?;
        self.expect_ident()?;
        Ok(crate::ast::Statement::CompatibilityRefusal(
            crate::ast::RefusalCommand::CreateDatabase,
        ))
    }

    fn drop_database_refusal(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect_ident_eq("database")?;
        self.expect_ident()?;
        Ok(crate::ast::Statement::CompatibilityRefusal(
            crate::ast::RefusalCommand::DropDatabase,
        ))
    }

    fn drop_extension_refusal(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect_ident_eq("extension")?;
        self.expect_ident()?;
        Ok(crate::ast::Statement::CompatibilityRefusal(
            crate::ast::RefusalCommand::DropExtension,
        ))
    }

    fn alter_database_refusal(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("alter")?;
        self.expect_ident_eq("database")?;
        self.expect_ident()?;
        self.expect_ident_eq("rename")?;
        self.expect_keyword_or_ident(Keyword::To, "to")?;
        self.expect_ident()?;
        Ok(crate::ast::Statement::CompatibilityRefusal(
            crate::ast::RefusalCommand::AlterDatabase,
        ))
    }

    fn alter_extension_refusal(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("alter")?;
        self.expect_ident_eq("extension")?;
        self.expect_ident()?;
        self.expect_keyword_or_ident(Keyword::Update, "update")?;
        Ok(crate::ast::Statement::CompatibilityRefusal(
            crate::ast::RefusalCommand::AlterExtension,
        ))
    }

    fn prepared_transaction_refusal(
        &mut self,
        command: crate::ast::RefusalCommand,
    ) -> Result<crate::ast::Statement, ParseError> {
        match command {
            crate::ast::RefusalCommand::PrepareTransaction => {
                self.expect_ident_eq("prepare")?;
                self.expect(&Token::Keyword(Keyword::Transaction))?;
            }
            crate::ast::RefusalCommand::CommitPrepared => {
                self.expect(&Token::Keyword(Keyword::Commit))?;
                self.expect_ident_eq("prepared")?;
            }
            crate::ast::RefusalCommand::RollbackPrepared => {
                self.expect(&Token::Keyword(Keyword::Rollback))?;
                self.expect_ident_eq("prepared")?;
            }
            _ => unreachable!("only SQL-level prepared transaction commands use this parser"),
        }
        match self.bump() {
            Token::StringLit(_) => {}
            other => {
                return Err(ParseError::new(
                    format!("expected transaction identifier string, found {other:?}"),
                    self.peek_pos(),
                ));
            }
        }
        Ok(crate::ast::Statement::CompatibilityRefusal(command))
    }

    fn alter_table(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{AlterTableRename, Statement};

        self.expect_ident_eq("alter")?;
        self.expect(&Token::Keyword(Keyword::Table))?;
        let table = self.expect_ident()?;
        self.expect_ident_eq("rename")?;
        let rename = if self.eat_ident_eq("column") {
            let column = self.expect_ident()?;
            self.expect_keyword_or_ident(Keyword::To, "to")?;
            AlterTableRename::Column {
                column,
                new_name: self.expect_ident()?,
            }
        } else if self.eat_keyword(Keyword::To) || self.eat_ident_eq("to") {
            AlterTableRename::Table {
                new_name: self.expect_ident()?,
            }
        } else {
            let column = self.expect_ident()?;
            self.expect_keyword_or_ident(Keyword::To, "to")?;
            AlterTableRename::Column {
                column,
                new_name: self.expect_ident()?,
            }
        };
        Ok(Statement::AlterTableRename { table, rename })
    }

    fn create_role(&mut self, can_login: bool) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Create))?;
        if can_login {
            self.expect(&Token::Keyword(Keyword::User))?;
        } else {
            self.expect_ident_eq("role")?;
        }
        Ok(crate::ast::Statement::CreateRole {
            name: self.expect_object_name()?,
            can_login,
        })
    }

    fn drop_role(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Drop))?;
        if self.eat_keyword(Keyword::User) {
            // DROP USER name
        } else {
            self.expect_ident_eq("role")?;
        }
        Ok(crate::ast::Statement::DropRole {
            name: self.expect_object_name()?,
        })
    }

    fn grant_table_privileges(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("grant")?;
        let privileges = self.privilege_list_until_on()?;
        self.expect(&Token::Keyword(Keyword::On))?;
        self.expect(&Token::Keyword(Keyword::Table))?;
        let table = self.expect_object_name()?;
        self.expect(&Token::Keyword(Keyword::To))?;
        let grantees = self.object_name_list()?;
        Ok(crate::ast::Statement::GrantTablePrivileges {
            privileges,
            table,
            grantees,
        })
    }

    fn revoke_table_privileges(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("revoke")?;
        let privileges = self.privilege_list_until_on()?;
        self.expect(&Token::Keyword(Keyword::On))?;
        self.expect(&Token::Keyword(Keyword::Table))?;
        let table = self.expect_object_name()?;
        self.expect(&Token::Keyword(Keyword::From))?;
        let grantees = self.object_name_list()?;
        Ok(crate::ast::Statement::RevokeTablePrivileges {
            privileges,
            table,
            grantees,
        })
    }

    fn set_role_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Set))?;
        self.expect_ident_eq("role")?;
        let role = if matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("none")) {
            self.bump();
            None
        } else {
            Some(self.expect_object_name()?)
        };
        Ok(crate::ast::Statement::SetRole { role })
    }

    fn privilege_list_until_on(&mut self) -> Result<Vec<String>, ParseError> {
        let mut privileges = Vec::new();
        loop {
            if matches!(self.peek(), Token::Keyword(Keyword::On)) {
                break;
            }
            privileges.push(self.expect_privilege_name()?);
            if !self.eat_comma() {
                if matches!(self.peek(), Token::Keyword(Keyword::On)) {
                    break;
                }
                return Err(ParseError::new(
                    "expected `,` or `ON` in privilege list",
                    self.peek_pos(),
                ));
            }
        }
        if privileges.is_empty() {
            return Err(ParseError::new(
                "expected at least one privilege",
                self.peek_pos(),
            ));
        }
        Ok(privileges)
    }

    fn object_name_list(&mut self) -> Result<Vec<String>, ParseError> {
        let mut names = vec![self.expect_object_name()?];
        while self.eat_comma() {
            names.push(self.expect_object_name()?);
        }
        Ok(names)
    }

    fn expect_object_name(&mut self) -> Result<String, ParseError> {
        match self.bump() {
            Token::Ident(s) => Ok(s),
            Token::Keyword(Keyword::Public) => Ok("public".into()),
            Token::Keyword(Keyword::CurrentUser) => Ok("current_user".into()),
            Token::Keyword(Keyword::User) => Ok("user".into()),
            other => Err(ParseError::new(
                format!("expected object name, found {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    fn expect_privilege_name(&mut self) -> Result<String, ParseError> {
        match self.bump() {
            Token::Ident(s) => Ok(s.to_ascii_uppercase()),
            Token::Keyword(Keyword::Select) => Ok("SELECT".into()),
            Token::Keyword(Keyword::Insert) => Ok("INSERT".into()),
            Token::Keyword(Keyword::Update) => Ok("UPDATE".into()),
            Token::Keyword(Keyword::Delete) => Ok("DELETE".into()),
            Token::Keyword(Keyword::All) => Ok("ALL".into()),
            other => Err(ParseError::new(
                format!("expected privilege name, found {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    /// SP37: `SET [LOCAL] <name> (= | TO) <value>` / `SET [LOCAL] TIME ZONE <value>`.
    /// Keyword-free for `LOCAL`/`TO`/`TIME ZONE`/`DEFAULT`/`LOCAL` (the value) —
    /// they are matched as lowercased idents, so none becomes a reserved keyword.
    /// The GUC name is normalized to lowercase; `TIME ZONE` normalizes to
    /// `"timezone"`.
    fn set_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Set))?;
        if self.eat_keyword(Keyword::Transaction)
            || (matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("session"))
                && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("characteristics")))
        {
            if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("session")) {
                self.bump(); // session
                self.bump(); // characteristics
                if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("as")) {
                    self.bump();
                }
                self.expect(&Token::Keyword(Keyword::Transaction))?;
            }
            return self.set_transaction_tail();
        }
        // `SESSION` is the explicit spelling of the default SET scope.
        if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("session")) {
            self.bump();
        }
        // `LOCAL` is the flag only when it leads and is followed by a parameter
        // name (an ident or `TIME ZONE`). It is NEVER a flag after `TIME ZONE`
        // (there it is the value `LOCAL`), and the `set_stmt` entry is before any
        // `TIME ZONE` is consumed, so a leading `LOCAL` here is unambiguous.
        let local = (matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("local"))
            || *self.peek() == Token::Keyword(Keyword::Local))
            && !matches!(self.peek2(), Token::Eq);
        if local {
            self.bump(); // LOCAL
        }
        // The `TIME ZONE` special spelling: `SET [LOCAL] TIME ZONE <value>`.
        if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("time"))
            && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("zone"))
        {
            self.bump(); // time
            self.bump(); // zone
            let value = self.set_time_zone_value()?;
            return Ok(Statement::Set {
                local,
                name: "timezone".into(),
                value,
            });
        }
        // `SET [LOCAL] <name> (= | TO) <value>`.
        let name = self.expect_ident()?.to_ascii_lowercase();
        // `=` is a token; `TO` is now a keyword (Keyword::To) — either separates name from value.
        let sep = *self.peek() == Token::Eq
            || *self.peek() == Token::Keyword(Keyword::To)
            || matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("to"));
        if !sep {
            return Err(ParseError::new(
                "expected `=` or `TO` in SET",
                self.peek_pos(),
            ));
        }
        self.bump(); // = or TO
        let value = self.set_value()?;
        Ok(Statement::Set { local, name, value })
    }

    /// The value after `=`/`TO`: a string literal, a `DEFAULT` ident (→ Default),
    /// or any other identifier (→ that ident verbatim).
    fn set_value(&mut self) -> Result<crate::ast::SetValue, ParseError> {
        use crate::ast::SetValue;
        let mut rendered = String::new();
        let mut separator = "";
        loop {
            let part = match self.peek().clone() {
                Token::Plus | Token::Minus => {
                    let sign = if self.bump() == Token::Minus {
                        "-"
                    } else {
                        "+"
                    };
                    let Token::IntLit(number) = self.bump() else {
                        return Err(ParseError::new(
                            "expected a number after SET value sign",
                            self.peek_pos(),
                        ));
                    };
                    format!("{sign}{number}")
                }
                Token::StringLit(s) | Token::IntLit(s) | Token::FloatLit(s) => {
                    self.bump();
                    s
                }
                Token::Ident(w) if w.eq_ignore_ascii_case("default") => {
                    self.bump();
                    if rendered.is_empty() && *self.peek() != Token::Comma {
                        return Ok(SetValue::Default);
                    }
                    "default".into()
                }
                Token::Ident(w) => {
                    self.bump();
                    w
                }
                Token::Keyword(Keyword::True | Keyword::On) => {
                    self.bump();
                    "on".into()
                }
                Token::Keyword(Keyword::False) => {
                    self.bump();
                    "off".into()
                }
                Token::Keyword(Keyword::Local) => {
                    self.bump();
                    "local".into()
                }
                Token::Keyword(Keyword::Public) => {
                    self.bump();
                    "public".into()
                }
                other => Err(ParseError::new(
                    format!("expected a SET value, found {other:?}"),
                    self.peek_pos(),
                ))?,
            };
            rendered.push_str(separator);
            rendered.push_str(&part);
            if self.eat_comma() {
                separator = ", ";
                continue;
            }
            if matches!(
                self.peek(),
                Token::Ident(_)
                    | Token::StringLit(_)
                    | Token::IntLit(_)
                    | Token::FloatLit(_)
                    | Token::Plus
                    | Token::Minus
                    | Token::Keyword(
                        Keyword::True
                            | Keyword::On
                            | Keyword::False
                            | Keyword::Local
                            | Keyword::Public
                    )
            ) {
                separator = " ";
            } else {
                break;
            }
        }
        Ok(SetValue::Value(rendered))
    }

    fn set_transaction_tail(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{IsolationLevel, SetValue, Statement};
        let isolation = if self.eat_keyword(Keyword::Isolation) {
            self.expect(&Token::Keyword(Keyword::Level))?;
            if self.eat_keyword(Keyword::Repeatable) {
                self.expect(&Token::Keyword(Keyword::Read))?;
                Some(IsolationLevel::RepeatableRead)
            } else if self.eat_keyword(Keyword::Read) {
                self.expect(&Token::Keyword(Keyword::Committed))?;
                Some(IsolationLevel::ReadCommitted)
            } else {
                return Err(ParseError::new(
                    "expected REPEATABLE READ or READ COMMITTED",
                    self.peek_pos(),
                ));
            }
        } else {
            None
        };
        let value = match isolation {
            Some(IsolationLevel::RepeatableRead) => SetValue::Value("repeatable read".into()),
            Some(IsolationLevel::ReadCommitted) => SetValue::Value("read committed".into()),
            None => SetValue::Default,
        };
        Ok(Statement::Set {
            local: false,
            name: "__set_transaction".into(),
            value,
        })
    }

    /// The value after `SET [LOCAL] TIME ZONE`: like [`set_value`], but the bare
    /// idents `LOCAL` and `DEFAULT` both mean "reset to default" (`PostgreSQL`).
    fn set_time_zone_value(&mut self) -> Result<crate::ast::SetValue, ParseError> {
        use crate::ast::SetValue;
        match self.peek().clone() {
            Token::StringLit(s) => {
                self.bump();
                Ok(SetValue::Value(s))
            }
            Token::Ident(w)
                if w.eq_ignore_ascii_case("default") || w.eq_ignore_ascii_case("local") =>
            {
                self.bump();
                Ok(SetValue::Default)
            }
            Token::Keyword(Keyword::Local) => {
                self.bump();
                Ok(SetValue::Default)
            }
            Token::Ident(w) => {
                self.bump();
                Ok(SetValue::Value(w))
            }
            other => Err(ParseError::new(
                format!("expected a TIME ZONE value, found {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    /// SP37: `SHOW <name>` / `SHOW TIME ZONE`. Positioned at the `show` ident.
    fn show_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.bump(); // show
        if self.eat_keyword(Keyword::All) {
            return Ok(Statement::Show { name: "all".into() });
        }
        // `SHOW TIME ZONE` → name `"timezone"`.
        if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("time"))
            && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("zone"))
        {
            self.bump(); // time
            self.bump(); // zone
            return Ok(Statement::Show {
                name: "timezone".into(),
            });
        }
        let name = self.expect_ident()?.to_ascii_lowercase();
        Ok(Statement::Show { name })
    }

    /// SP37: `RESET <name>`. Positioned at the `reset` ident.
    fn reset_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.bump(); // reset
        if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("role")) {
            self.bump();
            return Ok(Statement::SetRole { role: None });
        }
        if self.eat_keyword(Keyword::All) {
            return Ok(Statement::Reset {
                target: crate::ast::ResetTarget::All,
            });
        }
        // `RESET TIME ZONE` → name `"timezone"` (symmetry with SHOW; PG accepts it).
        if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("time"))
            && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("zone"))
        {
            self.bump(); // time
            self.bump(); // zone
            return Ok(Statement::Reset {
                target: crate::ast::ResetTarget::Name("timezone".into()),
            });
        }
        let name = self.expect_ident()?.to_ascii_lowercase();
        Ok(Statement::Reset {
            target: crate::ast::ResetTarget::Name(name),
        })
    }

    fn discard_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.bump(); // discard
        self.expect(&Token::Keyword(Keyword::All))?;
        Ok(crate::ast::Statement::Set {
            local: false,
            name: "__discard_all".into(),
            value: crate::ast::SetValue::Default,
        })
    }

    fn begin(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{IsolationLevel, Statement};
        let leading = self.bump(); // BEGIN or START
        if leading == Token::Keyword(Keyword::Start) {
            // START TRANSACTION is valid; bare START is not a statement.
            self.expect(&Token::Keyword(Keyword::Transaction))?;
        } else {
            // TRANSACTION is optional after BEGIN.
            self.eat_keyword(Keyword::Transaction);
        }
        let isolation = if self.eat_keyword(Keyword::Isolation) {
            self.expect(&Token::Keyword(Keyword::Level))?;
            if self.eat_keyword(Keyword::Repeatable) {
                self.expect(&Token::Keyword(Keyword::Read))?;
                Some(IsolationLevel::RepeatableRead)
            } else if self.eat_keyword(Keyword::Read) {
                self.expect(&Token::Keyword(Keyword::Committed))?;
                Some(IsolationLevel::ReadCommitted)
            } else {
                return Err(ParseError::new(
                    "expected REPEATABLE READ or READ COMMITTED",
                    self.peek_pos(),
                ));
            }
        } else {
            None
        };
        Ok(Statement::Begin { isolation })
    }

    fn update(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Update))?;
        let table = self.expect_ident()?;
        self.expect(&Token::Keyword(Keyword::Set))?;
        let mut assignments = Vec::new();
        loop {
            let col = self.expect_ident()?;
            self.expect(&Token::Eq)?;
            let value = self.expr(0)?;
            assignments.push((col, value));
            if self.eat_comma() {
                continue;
            }
            break;
        }
        let filter = if self.eat_keyword(Keyword::Where) {
            Some(self.expr(0)?)
        } else {
            None
        };
        let returning = self.returning_clause()?;
        Ok(Statement::Update {
            table,
            assignments,
            filter,
            returning,
        })
    }

    fn delete(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Delete))?;
        self.expect(&Token::Keyword(Keyword::From))?;
        let table = self.expect_ident()?;
        let filter = if self.eat_keyword(Keyword::Where) {
            Some(self.expr(0)?)
        } else {
            None
        };
        let returning = self.returning_clause()?;
        Ok(Statement::Delete {
            table,
            filter,
            returning,
        })
    }

    fn create_table(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{ColumnDef, HashShardingSpec, ShardingSpec, Statement};
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect(&Token::Keyword(Keyword::Table))?;
        let name = self.expect_ident()?;
        self.expect(&Token::LParen)?;
        let mut columns = Vec::new();
        let mut constraints = Vec::new();
        loop {
            if self.eat_ident_eq("primary") {
                self.expect_ident_eq("key")?;
                constraints.push(crate::ast::TableConstraint::PrimaryKey(
                    self.parse_ident_list()?,
                ));
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            if self.eat_keyword(Keyword::Unique) {
                constraints.push(crate::ast::TableConstraint::Unique(
                    self.parse_ident_list()?,
                ));
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            if self.eat_ident_eq("check") {
                self.expect(&Token::LParen)?;
                let expr = self.expr(0)?;
                self.expect(&Token::RParen)?;
                constraints.push(crate::ast::TableConstraint::Check(expr));
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            let col_name = self.expect_ident()?;
            let (ty, serial) = self.parse_column_type()?;
            let constraints = self.column_constraints()?;
            columns.push(ColumnDef {
                name: col_name,
                ty,
                serial,
                constraints,
            });
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        let saw_sharded = self.eat_ident_eq("sharded");
        let sharding = if saw_sharded && self.eat_keyword(Keyword::By) {
            self.expect_ident_eq("hash")?;
            self.expect(&Token::LParen)?;
            let hash_columns = vec![self.expect_ident()?];
            if self.eat_comma() {
                return Err(ParseError::new(
                    "hash sharding requires exactly one column",
                    self.peek_pos(),
                ));
            }
            self.expect(&Token::RParen)?;
            self.expect_ident_eq("buckets")?;
            let buckets = self.expect_hash_bucket_count()?;
            let co_location_group = if self.eat_ident_eq("colocated") {
                self.expect_keyword_or_ident(Keyword::With, "with")?;
                Some(self.expect_ident()?)
            } else {
                None
            };
            Some(ShardingSpec::Hash(HashShardingSpec {
                columns: hash_columns,
                buckets,
                co_location_group,
            }))
        } else {
            None
        };
        let sharded = saw_sharded;
        // PostgreSQL storage parameters (`WITH (fillfactor=100, ...)`) tune
        // heap/TOAST behavior Crabka has no equivalent of: accept the standard
        // `key [= value] [, ...]` shape and discard it. pgbench -i emits this
        // clause on every CREATE TABLE.
        if self.eat_keyword(Keyword::With) {
            self.expect(&Token::LParen)?;
            loop {
                self.expect_ident()?;
                if *self.peek() == Token::Dot {
                    self.bump();
                    self.expect_ident()?;
                }
                if *self.peek() == Token::Eq {
                    self.bump();
                    self.storage_parameter_value()?;
                }
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
        }
        Ok(Statement::CreateTable {
            name,
            columns,
            constraints,
            sharded,
            sharding,
        })
    }

    fn parse_column_type(
        &mut self,
    ) -> Result<(crabka_pgtypes::ColumnType, Option<crate::ast::SerialKind>), ParseError> {
        let type_pos = self.peek_pos();
        let type_name = self.expect_ident()?;
        match type_name.as_str() {
            "serial" => Ok((
                crabka_pgtypes::ColumnType::Int4,
                Some(crate::ast::SerialKind::Serial),
            )),
            "bigserial" => Ok((
                crabka_pgtypes::ColumnType::Int8,
                Some(crate::ast::SerialKind::BigSerial),
            )),
            _ => {
                self.pos -= 1;
                self.parse_type_name()
                    .map(|ty| (ty, None))
                    .map_err(|mut err| {
                        err.position = type_pos;
                        err
                    })
            }
        }
    }

    fn column_constraints(&mut self) -> Result<Vec<crate::ast::ColumnConstraint>, ParseError> {
        use crate::ast::ColumnConstraint;
        let mut constraints = Vec::new();
        loop {
            if self.eat_keyword(Keyword::Not) {
                self.expect(&Token::Keyword(Keyword::Null))?;
                constraints.push(ColumnConstraint::NotNull);
                continue;
            }
            if self.eat_ident_eq("default") {
                constraints.push(ColumnConstraint::Default(self.expr(0)?));
                continue;
            }
            if self.eat_ident_eq("primary") {
                self.expect_ident_eq("key")?;
                constraints.push(ColumnConstraint::PrimaryKey);
                continue;
            }
            if self.eat_keyword(Keyword::Unique) {
                constraints.push(ColumnConstraint::Unique);
                continue;
            }
            if self.eat_ident_eq("check") {
                self.expect(&Token::LParen)?;
                let expr = self.expr(0)?;
                self.expect(&Token::RParen)?;
                constraints.push(ColumnConstraint::Check(expr));
                continue;
            }
            break;
        }
        Ok(constraints)
    }

    fn create_index(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{IndexPlacement, Statement};

        self.expect(&Token::Keyword(Keyword::Create))?;
        let mut unique = false;
        let mut placement = IndexPlacement::Local;
        loop {
            if !unique && self.eat_keyword(Keyword::Unique) {
                unique = true;
                continue;
            }
            if self.eat_keyword(Keyword::Global) {
                placement = IndexPlacement::Global;
                continue;
            }
            if self.eat_keyword(Keyword::Local) {
                placement = IndexPlacement::Local;
                continue;
            }
            break;
        }
        self.expect(&Token::Keyword(Keyword::Index))?;
        let name = self.expect_ident()?;
        self.expect(&Token::Keyword(Keyword::On))?;
        let table = self.expect_ident()?;
        let columns = self.parse_ident_list()?;
        if columns.is_empty() {
            return Err(ParseError::new(
                "CREATE INDEX requires at least one column",
                self.peek_pos(),
            ));
        }
        Ok(Statement::CreateIndex {
            name,
            table,
            columns,
            unique,
            placement,
        })
    }

    fn create_view(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;

        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect(&Token::Keyword(Keyword::View))?;
        let name = self.expect_object_name()?;
        self.expect(&Token::Keyword(Keyword::As))?;
        let definition_start = self.peek_pos();
        let query = self.query_expr()?;
        let definition_end = self.peek_pos();
        let definition = self.source[definition_start..definition_end]
            .trim()
            .to_string();
        Ok(Statement::CreateView {
            name,
            definition,
            query,
        })
    }

    fn create_sequence(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{SequenceOptions, Statement};
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect_ident_eq("sequence")?;
        let name = self.expect_ident()?;
        let mut options = SequenceOptions::default();
        while !matches!(self.peek(), Token::Semicolon | Token::Eof) {
            if self.eat_ident_eq("start") || self.eat_keyword(Keyword::Start) {
                self.eat_keyword(Keyword::With);
                options.start = Some(self.expect_i64("START value")?);
            } else if self.eat_ident_eq("increment") {
                self.expect_keyword_or_ident(Keyword::By, "by")?;
                options.increment = Some(self.expect_i64("INCREMENT value")?);
            } else if self.eat_ident_eq("minvalue") {
                options.min = Some(self.expect_i64("MINVALUE")?);
            } else if self.eat_ident_eq("maxvalue") {
                options.max = Some(self.expect_i64("MAXVALUE")?);
            } else if self.eat_ident_eq("no") {
                if self.eat_ident_eq("minvalue") {
                    options.min = None;
                } else if self.eat_ident_eq("maxvalue") {
                    options.max = None;
                } else if self.eat_ident_eq("cycle") {
                    options.cycle = Some(false);
                } else {
                    return Err(ParseError::new(
                        "expected MINVALUE, MAXVALUE, or CYCLE after NO",
                        self.peek_pos(),
                    ));
                }
            } else if self.eat_ident_eq("cache") {
                options.cache = Some(self.expect_i64("CACHE")?);
            } else if self.eat_ident_eq("cycle") {
                options.cycle = Some(true);
            } else {
                return Err(ParseError::new(
                    format!("unexpected CREATE SEQUENCE option {:?}", self.peek()),
                    self.peek_pos(),
                ));
            }
        }
        Ok(Statement::CreateIndex {
            name,
            table: "__crabka_sequence__".into(),
            columns: encode_sequence_options(&options),
            unique: false,
            placement: crate::ast::IndexPlacement::Local,
        })
    }

    fn drop_sequence(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect_ident_eq("sequence")?;
        let if_exists = self.eat_if_exists()?;
        let mut names = vec![format!("__crabka_sequence__:{}", self.expect_ident()?)];
        while *self.peek() == Token::Comma {
            self.bump();
            names.push(format!("__crabka_sequence__:{}", self.expect_ident()?));
        }
        Ok(crate::ast::Statement::DropTable { names, if_exists })
    }

    fn expect_i64(&mut self, what: &str) -> Result<i64, ParseError> {
        let pos = self.peek_pos();
        let negative = *self.peek() == Token::Minus;
        if negative {
            self.bump();
        }
        let Token::IntLit(raw) = self.bump() else {
            return Err(ParseError::new(format!("expected {what}"), pos));
        };
        let signed = if negative { format!("-{raw}") } else { raw };
        signed
            .parse::<i64>()
            .map_err(|_| ParseError::new(format!("{what} out of range"), pos))
    }

    fn expect_keyword_or_ident(&mut self, keyword: Keyword, ident: &str) -> Result<(), ParseError> {
        if self.eat_keyword(keyword) || self.eat_ident_eq(ident) {
            return Ok(());
        }

        Err(ParseError::new(
            format!("expected `{ident}`, found {:?}", self.peek()),
            self.peek_pos(),
        ))
    }

    fn expect_hash_bucket_count(&mut self) -> Result<u32, ParseError> {
        let pos = self.peek_pos();
        let Token::IntLit(raw) = self.bump() else {
            return Err(ParseError::new("expected hash bucket count", pos));
        };
        let buckets = raw
            .parse::<u32>()
            .map_err(|_| ParseError::new("hash bucket count out of range", pos))?;
        if buckets == 0 || !buckets.is_power_of_two() {
            return Err(ParseError::new(
                "hash bucket count must be a power of two",
                pos,
            ));
        }
        Ok(buckets)
    }

    /// `VACUUM [ ( option [value] [, ...] ) ] [FULL] [FREEZE] [VERBOSE]
    /// [ANALYZE] [name [, ...]]` — the whole tail is validated for shape and
    /// discarded: reclamation is autonomous, so the command is a hint.
    fn vacuum(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("vacuum")?;
        if *self.peek() == Token::LParen {
            self.bump();
            loop {
                self.expect_ident()?;
                if !matches!(self.peek(), Token::Comma | Token::RParen) {
                    self.storage_parameter_value()?;
                }
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
        } else {
            // `FULL` lexes as a keyword (FULL JOIN); the rest are plain idents.
            loop {
                match self.peek() {
                    Token::Keyword(Keyword::Full) => {
                        self.bump();
                    }
                    Token::Ident(word)
                        if matches!(word.as_str(), "freeze" | "verbose" | "analyze") =>
                    {
                        self.bump();
                    }
                    _ => break,
                }
            }
        }
        if matches!(self.peek(), Token::Ident(_)) {
            loop {
                self.expect_ident()?;
                if self.eat_comma() {
                    continue;
                }
                break;
            }
        }
        Ok(crate::ast::Statement::Vacuum)
    }

    /// `TRUNCATE [TABLE] name [, ...] [RESTART IDENTITY | CONTINUE IDENTITY]
    /// [CASCADE | RESTRICT]`. `CONTINUE IDENTITY` is the `PostgreSQL` default;
    /// `CASCADE`/`RESTRICT` are accepted and equivalent because no foreign-key
    /// enforcement exists to distinguish them.
    fn truncate(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("truncate")?;
        let _ = self.eat_keyword(Keyword::Table);
        let mut names = vec![self.expect_ident()?];
        while *self.peek() == Token::Comma {
            self.bump();
            names.push(self.expect_ident()?);
        }
        let restart_identity = if self.eat_ident_eq("restart") {
            self.expect_ident_eq("identity")?;
            true
        } else {
            if self.eat_ident_eq("continue") {
                self.expect_ident_eq("identity")?;
            }
            false
        };
        if !self.eat_ident_eq("cascade") {
            let _ = self.eat_ident_eq("restrict");
        }
        Ok(crate::ast::Statement::Truncate {
            names,
            restart_identity,
        })
    }

    /// Consume one storage-parameter value (`WITH (key = value)`): a numeric
    /// literal (optionally negative), a string, or a bare word such as `on` /
    /// `off`. The value is validated for shape and discarded.
    fn storage_parameter_value(&mut self) -> Result<(), ParseError> {
        if *self.peek() == Token::Minus {
            self.bump();
            let pos = self.peek_pos();
            return match self.bump() {
                Token::IntLit(_) | Token::FloatLit(_) => Ok(()),
                other => Err(ParseError::new(
                    format!("expected numeric storage parameter value, found {other:?}"),
                    pos,
                )),
            };
        }
        let pos = self.peek_pos();
        match self.bump() {
            Token::IntLit(_)
            | Token::FloatLit(_)
            | Token::StringLit(_)
            | Token::Ident(_)
            | Token::Keyword(_) => Ok(()),
            other => Err(ParseError::new(
                format!("expected storage parameter value, found {other:?}"),
                pos,
            )),
        }
    }

    fn drop_table(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect(&Token::Keyword(Keyword::Table))?;
        let if_exists = self.eat_if_exists()?;
        let mut names = vec![self.expect_ident()?];
        while *self.peek() == Token::Comma {
            self.bump();
            names.push(self.expect_ident()?);
        }
        Ok(Statement::DropTable { names, if_exists })
    }

    fn drop_index(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;

        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect(&Token::Keyword(Keyword::Index))?;
        let if_exists = self.eat_if_exists()?;
        Ok(Statement::DropIndex {
            name: self.expect_ident()?,
            if_exists,
        })
    }

    fn drop_view(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;

        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect(&Token::Keyword(Keyword::View))?;
        let if_exists = self.eat_if_exists()?;
        Ok(Statement::DropView {
            name: self.expect_object_name()?,
            if_exists,
        })
    }

    fn insert(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Insert))?;
        self.expect(&Token::Keyword(Keyword::Into))?;
        let table = self.expect_ident()?;
        let columns = if *self.peek() == Token::LParen {
            self.bump();
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_ident()?);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
            Some(cols)
        } else {
            None
        };
        self.expect(&Token::Keyword(Keyword::Values))?;
        let mut rows = Vec::new();
        loop {
            self.expect(&Token::LParen)?;
            let mut row = Vec::new();
            loop {
                row.push(self.insert_value_expr()?);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
            rows.push(row);
            if self.eat_comma() {
                continue;
            }
            break;
        }
        Ok(Statement::Insert {
            table,
            columns,
            rows,
            returning: self.returning_clause()?,
        })
    }

    fn returning_clause(&mut self) -> Result<Option<Vec<crate::ast::SelectItem>>, ParseError> {
        if !self.eat_keyword(Keyword::Returning) {
            return Ok(None);
        }

        Ok(Some(self.projection_list()?))
    }

    fn projection_list(&mut self) -> Result<Vec<crate::ast::SelectItem>, ParseError> {
        use crate::ast::SelectItem;

        let mut projection = Vec::new();
        loop {
            if *self.peek() == Token::Star {
                self.bump();
                projection.push(SelectItem::Wildcard);
            } else if let Token::Ident(_) = self.peek()
                && *self.peek_n(1) == Token::Dot
                && *self.peek_n(2) == Token::Star
            {
                let qualifier = self.expect_ident()?;
                self.bump();
                self.bump();
                projection.push(SelectItem::QualifiedWildcard(qualifier));
            } else {
                let expr = self.expr(0)?;
                let alias = if self.eat_keyword(Keyword::As) {
                    Some(self.expect_ident()?)
                } else if let Token::Ident(_) = self.peek() {
                    Some(self.expect_ident()?)
                } else {
                    None
                };
                projection.push(SelectItem::Expr { expr, alias });
            }

            if self.eat_comma() {
                continue;
            }
            break;
        }
        Ok(projection)
    }

    fn insert_value_expr(&mut self) -> Result<crate::ast::Expr, ParseError> {
        if self.eat_ident_eq("default") {
            return Ok(crate::ast::Expr::Default);
        }
        self.expr(0)
    }

    fn copy_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{CopyFormat, CopyStmt, Statement};

        self.expect(&Token::Keyword(Keyword::Copy))?;
        let table = self.expect_ident()?;
        let columns = if *self.peek() == Token::LParen {
            Some(self.parse_parenthesized_ident_list()?)
        } else {
            None
        };

        if self.eat_keyword(Keyword::To) {
            return Err(ParseError::new_sqlstate(
                "0A000",
                "COPY TO is not supported",
                self.peek_pos(),
            ));
        }
        self.expect(&Token::Keyword(Keyword::From))?;
        if !self.eat_ident_eq("stdin") {
            return Err(ParseError::new_sqlstate(
                "0A000",
                "only COPY FROM STDIN is supported",
                self.peek_pos(),
            ));
        }

        let mut format = CopyFormat::Text;
        if self.eat_keyword(Keyword::With) {
            self.expect(&Token::LParen)?;
            loop {
                let option = self.expect_ident()?.to_ascii_lowercase();
                match option.as_str() {
                    "format" => {
                        let value = self.expect_ident()?.to_ascii_lowercase();
                        format = match value.as_str() {
                            "text" => CopyFormat::Text,
                            "csv" => CopyFormat::Csv,
                            "binary" => {
                                return Err(ParseError::new_sqlstate(
                                    "0A000",
                                    "COPY BINARY is not supported",
                                    self.peek_pos(),
                                ));
                            }
                            _ => {
                                return Err(ParseError::new(
                                    format!("unsupported COPY format `{value}`"),
                                    self.peek_pos(),
                                ));
                            }
                        };
                    }
                    "binary" => {
                        return Err(ParseError::new_sqlstate(
                            "0A000",
                            "COPY BINARY is not supported",
                            self.peek_pos(),
                        ));
                    }
                    // FREEZE is a performance hint (skip per-row visibility
                    // bookkeeping on a freshly created/truncated table). Rows
                    // load as ordinary MVCC rows regardless, so the hint is
                    // accepted and ignored; pgbench -i sends `freeze on`.
                    "freeze" => {
                        if matches!(
                            self.peek(),
                            Token::Keyword(Keyword::On | Keyword::True | Keyword::False)
                        ) {
                            self.bump();
                        } else if !matches!(self.peek(), Token::Comma | Token::RParen) {
                            let value = self.expect_ident()?.to_ascii_lowercase();
                            if value != "off" {
                                return Err(ParseError::new(
                                    format!("unsupported COPY FREEZE value `{value}`"),
                                    self.peek_pos(),
                                ));
                            }
                        }
                    }
                    _ => {
                        return Err(ParseError::new_sqlstate(
                            "0A000",
                            format!("COPY option `{option}` is not supported"),
                            self.peek_pos(),
                        ));
                    }
                }
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
        } else if self.eat_ident_eq("binary") {
            return Err(ParseError::new_sqlstate(
                "0A000",
                "COPY BINARY is not supported",
                self.peek_pos(),
            ));
        }

        Ok(Statement::Set {
            local: false,
            name: crate::ast::COPY_FROM_STDIN_SENTINEL.into(),
            value: crate::ast::SetValue::Value(Self::encode_copy_stmt(&CopyStmt {
                table,
                columns,
                format,
            })),
        })
    }

    fn parse_parenthesized_ident_list(&mut self) -> Result<Vec<String>, ParseError> {
        self.expect(&Token::LParen)?;
        let mut cols = Vec::new();
        loop {
            cols.push(self.expect_ident()?);
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        Ok(cols)
    }

    fn encode_copy_stmt(copy: &crate::ast::CopyStmt) -> String {
        let format = match copy.format {
            crate::ast::CopyFormat::Text => "text",
            crate::ast::CopyFormat::Csv => "csv",
        };
        let columns = copy
            .columns
            .as_ref()
            .map(|columns| {
                columns
                    .iter()
                    .map(|column| Self::encode_copy_part(column))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        [format, &Self::encode_copy_part(&copy.table), &columns].join("\t")
    }

    fn encode_copy_part(value: &str) -> String {
        let mut out = String::with_capacity(value.len());
        for ch in value.chars() {
            match ch {
                '\\' => out.push_str(r"\\"),
                '\t' => out.push_str(r"\t"),
                ',' => out.push_str(r"\,"),
                other => out.push(other),
            }
        }
        out
    }

    /// Parse projection → HAVING. Leaves `order_by` / `limit` / `offset` / `locking` empty;
    /// the caller (single SELECT or set-op query) owns the tail.
    fn select_core(&mut self) -> Result<crate::ast::SelectStmt, ParseError> {
        use crate::ast::SelectStmt;
        // Mode-1 depth guard: EVERY SELECT body funnels through `select_core` — a
        // top-level set-op branch (`set_primary → select_core`), a derived table,
        // or a scalar/IN/EXISTS subquery (`query_expr → set_primary → select_core`) — so
        // guarding here bounds all nested-SELECT recursion (e.g. a derived-table
        // chain `( SELECT … FROM ( SELECT … ) )`). Subqueries also pass through
        // `expr` first; guarding both is belt-and-braces.
        let _guard = DepthGuard::enter(&self.depth, self.peek_pos())?;
        self.expect(&Token::Keyword(Keyword::Select))?;
        // SP28: SELECT DISTINCT (ALL is the default modifier — accept and ignore).
        let distinct = self.eat_keyword(Keyword::Distinct);
        if !distinct {
            self.eat_keyword(Keyword::All);
        }
        let projection = self.projection_list()?;
        let from = if self.eat_keyword(Keyword::From) {
            self.parse_from()?
        } else {
            Vec::new()
        };
        let filter = if self.eat_keyword(Keyword::Where) {
            Some(self.expr(0)?)
        } else {
            None
        };
        // SP27: GROUP BY <expr-list> then HAVING <expr>, between WHERE and ORDER BY.
        let mut group_by = Vec::new();
        if self.eat_keyword(Keyword::Group) {
            self.expect(&Token::Keyword(Keyword::By))?;
            loop {
                group_by.push(self.expr(0)?);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
        }
        let having = if self.eat_keyword(Keyword::Having) {
            Some(self.expr(0)?)
        } else {
            None
        };
        Ok(SelectStmt {
            projection,
            from,
            filter,
            distinct,
            group_by,
            having,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            locking: None,
        })
    }

    fn values_stmt(&mut self) -> Result<crate::ast::ValuesStmt, ParseError> {
        self.expect(&Token::Keyword(Keyword::Values))?;
        let mut rows = Vec::new();
        loop {
            self.expect(&Token::LParen)?;
            if *self.peek() == Token::RParen {
                return Err(ParseError::new(
                    "VALUES row must have at least one expression",
                    self.peek_pos(),
                ));
            }
            let mut row = vec![self.expr(0)?];
            while self.eat_comma() {
                row.push(self.expr(0)?);
            }
            self.expect(&Token::RParen)?;
            rows.push(row);
            if !self.eat_comma() {
                break;
            }
        }
        Ok(crate::ast::ValuesStmt { rows })
    }

    /// Parse an optional `ORDER BY …`, then `LIMIT`/`OFFSET` in either order.
    /// The tuple is the three result-level tail components (`order_by`, `limit`, `offset`);
    /// a named struct would not read more clearly than the positional triple.
    fn parse_set_tail(&mut self) -> Result<SetTail, ParseError> {
        use crate::ast::OrderItem;
        let mut order_by = Vec::new();
        if self.eat_keyword(Keyword::Order) {
            self.expect(&Token::Keyword(Keyword::By))?;
            loop {
                let expr = self.expr(0)?;
                let asc = if self.eat_keyword(Keyword::Desc) {
                    false
                } else {
                    self.eat_keyword(Keyword::Asc);
                    true
                };
                order_by.push(OrderItem { expr, asc });
                if self.eat_comma() {
                    continue;
                }
                break;
            }
        }
        // SP28: LIMIT and OFFSET in either order (PostgreSQL accepts both).
        let mut limit = None;
        let mut offset = None;
        loop {
            if limit.is_none() && self.eat_keyword(Keyword::Limit) {
                limit = Some(self.expect_int_count("LIMIT")?);
            } else if offset.is_none() && self.eat_keyword(Keyword::Offset) {
                offset = Some(self.expect_int_count("OFFSET")?);
            } else {
                break;
            }
        }
        Ok((order_by, limit, offset))
    }

    /// Parse an optional `FOR UPDATE` / `FOR SHARE` row-locking clause.
    fn parse_locking(&mut self) -> Result<Option<crate::ast::RowLockStrength>, ParseError> {
        if self.eat_keyword(Keyword::For) {
            if self.eat_keyword(Keyword::Update) {
                Ok(Some(crate::ast::RowLockStrength::ForUpdate))
            } else if self.eat_keyword(Keyword::Share) {
                Ok(Some(crate::ast::RowLockStrength::ForShare))
            } else {
                Err(ParseError::new(
                    "expected UPDATE or SHARE after FOR",
                    self.peek_pos(),
                ))
            }
        } else {
            Ok(None)
        }
    }

    fn query_statement(&mut self) -> Result<crate::ast::Statement, ParseError> {
        Ok(crate::ast::Statement::Query(self.query_expr()?))
    }

    fn query_expr(&mut self) -> Result<crate::ast::QueryExpr, ParseError> {
        let with = self.parse_with_clause()?;
        if *self.peek() == Token::LParen {
            let q = self.parenthesized_query_expr()?;
            if self.peek_is_set_op() {
                let left = self.query_expr_as_set_branch(q)?;
                let body = self.set_expr_rest(left, 0)?;
                let (order_by, limit, offset, locking) = self.parse_query_tail_and_locking()?;
                let mut q = self.finish_query_expr(body, order_by, limit, offset, locking)?;
                q.with = with;
                return Ok(q);
            }
            if !self.query_tail_or_locking_starts() {
                let mut q = q;
                q.with = with;
                return Ok(q);
            }
            let body = Self::query_expr_as_outer_primary(q);
            let (order_by, limit, offset, locking) = self.parse_query_tail_and_locking()?;
            let mut q = self.finish_query_expr(body, order_by, limit, offset, locking)?;
            q.with = with;
            return Ok(q);
        }
        let body = self.set_expr(0)?;
        let (order_by, limit, offset, locking) = self.parse_query_tail_and_locking()?;
        let mut q = self.finish_query_expr(body, order_by, limit, offset, locking)?;
        q.with = with;
        Ok(q)
    }

    fn parse_query_tail_and_locking(&mut self) -> Result<QueryTailAndLocking, ParseError> {
        let (order_by, limit, offset) = self.parse_set_tail()?;
        let locking = self.parse_locking()?;
        Ok((order_by, limit, offset, locking))
    }

    fn query_expr_as_set_branch(
        &mut self,
        q: crate::ast::QueryExpr,
    ) -> Result<crate::ast::SetExpr, ParseError> {
        use crate::ast::{QueryBody, SetExpr};
        let has_tail = q.with.is_some()
            || !q.order_by.is_empty()
            || q.limit.is_some()
            || q.offset.is_some()
            || q.locking.is_some();
        if !has_tail {
            return Ok(q.body);
        }
        if q.locking.is_some() {
            return Err(ParseError::new(
                "FOR UPDATE/SHARE is not allowed with UNION/INTERSECT/EXCEPT",
                self.peek_pos(),
            ));
        }
        match q.body {
            SetExpr::Query(QueryBody::Select(mut select)) => {
                select.order_by = q.order_by;
                select.limit = q.limit;
                select.offset = q.offset;
                Ok(SetExpr::Query(QueryBody::Select(select)))
            }
            body => Ok(SetExpr::Query(QueryBody::Nested(Box::new(
                crate::ast::QueryExpr {
                    with: q.with,
                    body,
                    order_by: q.order_by,
                    limit: q.limit,
                    offset: q.offset,
                    locking: q.locking,
                },
            )))),
        }
    }

    fn query_expr_as_outer_primary(q: crate::ast::QueryExpr) -> crate::ast::SetExpr {
        use crate::ast::{QueryBody, SetExpr};
        let has_tail = q.with.is_some()
            || !q.order_by.is_empty()
            || q.limit.is_some()
            || q.offset.is_some()
            || q.locking.is_some();
        if has_tail {
            SetExpr::Query(QueryBody::Nested(Box::new(q)))
        } else {
            q.body
        }
    }

    fn finish_query_expr(
        &mut self,
        body: crate::ast::SetExpr,
        order_by: Vec<crate::ast::OrderItem>,
        limit: Option<i64>,
        offset: Option<i64>,
        locking: Option<crate::ast::RowLockStrength>,
    ) -> Result<crate::ast::QueryExpr, ParseError> {
        use crate::ast::{QueryBody, QueryExpr, SetExpr};
        if locking.is_some() {
            match &body {
                SetExpr::Query(QueryBody::Select(_)) => {}
                SetExpr::Query(QueryBody::Values(_)) => {
                    return Err(ParseError::new(
                        "FOR UPDATE/SHARE is not allowed with VALUES",
                        self.peek_pos(),
                    ));
                }
                SetExpr::Query(QueryBody::Nested(_)) => {
                    return Err(ParseError::new(
                        "FOR UPDATE/SHARE is not allowed with a nested query expression",
                        self.peek_pos(),
                    ));
                }
                SetExpr::SetOp { .. } => {
                    return Err(ParseError::new(
                        "FOR UPDATE/SHARE is not allowed with UNION/INTERSECT/EXCEPT",
                        self.peek_pos(),
                    ));
                }
            }
        }
        Ok(QueryExpr {
            with: None,
            body,
            order_by,
            limit,
            offset,
            locking,
        })
    }

    fn peek_is_set_op(&self) -> bool {
        matches!(
            self.peek(),
            Token::Keyword(Keyword::Union | Keyword::Except | Keyword::Intersect)
        )
    }

    fn query_tail_or_locking_starts(&self) -> bool {
        matches!(
            self.peek(),
            Token::Keyword(Keyword::Order | Keyword::Limit | Keyword::Offset | Keyword::For)
        )
    }

    fn parse_with_clause(&mut self) -> Result<Option<crate::ast::WithClause>, ParseError> {
        use crate::ast::{Cte, WithClause};
        if !self.eat_keyword(Keyword::With) {
            return Ok(None);
        }
        let recursive = self.eat_keyword(Keyword::Recursive);
        let mut ctes = Vec::new();
        loop {
            let name = self.expect_ident()?;
            if ctes.iter().any(|c: &Cte| c.name == name) {
                return Err(ParseError::new_sqlstate(
                    "42712",
                    format!("table name \"{name}\" specified more than once"),
                    self.peek_pos(),
                ));
            }
            let columns = if *self.peek() == Token::LParen {
                self.bump();
                let mut cols = Vec::new();
                loop {
                    cols.push(self.expect_ident()?);
                    if self.eat_comma() {
                        continue;
                    }
                    break;
                }
                self.expect(&Token::RParen)?;
                Some(cols)
            } else {
                None
            };
            self.expect(&Token::Keyword(Keyword::As))?;
            self.expect(&Token::LParen)?;
            let query = self.query_expr()?;
            self.expect(&Token::RParen)?;
            ctes.push(Cte {
                name,
                columns,
                query,
            });
            if !self.eat_comma() {
                break;
            }
        }
        Ok(Some(WithClause { recursive, ctes }))
    }

    /// Precedence-climbing set-op tree. INTERSECT = 2, UNION/EXCEPT = 1; all
    /// left-associative (recurse for the RHS at `prec + 1`).
    fn set_expr(&mut self, min_prec: u8) -> Result<crate::ast::SetExpr, ParseError> {
        // Mode-1 guard: a parenthesized set-op subtree recurses
        // `set_primary → set_expr → set_primary` for `(((… query …)))`, a path that
        // does NOT funnel through `expr`/`select_core`, so it needs its own guard.
        let _guard = DepthGuard::enter(&self.depth, self.peek_pos())?;
        let left = self.set_primary()?;
        self.set_expr_rest(left, min_prec)
    }

    fn set_expr_rest(
        &mut self,
        mut left: crate::ast::SetExpr,
        min_prec: u8,
    ) -> Result<crate::ast::SetExpr, ParseError> {
        use crate::ast::{SetExpr, SetOp};
        // Mode-2 cap: a flat left-assoc chain `A UNION B UNION C …` is parsed by this
        // LOOP (not recursion), building an N-deep left-nested `SetExpr` that would
        // overflow the executor's `fold`/`resolve_set_columns` AND recursive `Drop`.
        // Capping the iterations prevents the over-deep tree (mirrors the Pratt loop).
        let mut iterations: usize = 0;
        loop {
            let (op, prec) = match self.peek() {
                Token::Keyword(Keyword::Union) => (SetOp::Union, 1u8),
                Token::Keyword(Keyword::Except) => (SetOp::Except, 1u8),
                Token::Keyword(Keyword::Intersect) => (SetOp::Intersect, 2u8),
                _ => break,
            };
            if prec < min_prec {
                break;
            }
            iterations += 1;
            if iterations > MAX_DEPTH {
                return Err(ParseError::too_deep(self.peek_pos()));
            }
            self.bump(); // the operator keyword
            let all = self.eat_keyword(Keyword::All);
            if !all {
                self.eat_keyword(Keyword::Distinct); // explicit default modifier
            }
            let right = self.set_expr(prec + 1)?;
            left = SetExpr::SetOp {
                op,
                all,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// A set-op primary: a parenthesized sub-query (precedence grouping, or a
    /// parenthesized single SELECT that keeps its own ORDER BY / LIMIT), or a bare
    /// SELECT branch (`select_core`, no tail — the query owns the tail).
    fn parenthesized_query_expr(&mut self) -> Result<crate::ast::QueryExpr, ParseError> {
        self.expect(&Token::LParen)?;
        self.query_expr_after_open_paren()
    }

    fn query_expr_after_open_paren(&mut self) -> Result<crate::ast::QueryExpr, ParseError> {
        let with = self.parse_with_clause()?;
        let body = self.set_expr(0)?;
        let (order_by, limit, offset, locking) = self.parse_query_tail_and_locking()?;
        self.expect(&Token::RParen)?;
        let mut q = self.finish_query_expr(body, order_by, limit, offset, locking)?;
        q.with = with;
        Ok(q)
    }

    fn set_primary(&mut self) -> Result<crate::ast::SetExpr, ParseError> {
        use crate::ast::{QueryBody, SetExpr};
        if *self.peek() == Token::LParen {
            self.bump(); // (
            if matches!(self.peek(), Token::Keyword(Keyword::With)) {
                let query = self.query_expr_after_open_paren()?;
                return Ok(Self::query_expr_as_outer_primary(query));
            }
            let inner = self.set_expr(0)?;
            let inner = self.attach_paren_tail(inner)?;
            self.expect(&Token::RParen)?;
            Ok(inner)
        } else if *self.peek() == Token::Keyword(Keyword::Values) {
            Ok(SetExpr::Query(QueryBody::Values(self.values_stmt()?)))
        } else {
            Ok(SetExpr::Query(QueryBody::Select(Box::new(
                self.select_core()?,
            ))))
        }
    }

    /// If an ORDER BY / LIMIT / OFFSET follows inside parentheses, attach it to a
    /// lone-SELECT inner; otherwise preserve the tailed query as a nested primary.
    fn attach_paren_tail(
        &mut self,
        inner: crate::ast::SetExpr,
    ) -> Result<crate::ast::SetExpr, ParseError> {
        use crate::ast::{QueryBody, QueryExpr, SetExpr};
        let has_tail = matches!(
            self.peek(),
            Token::Keyword(Keyword::Order | Keyword::Limit | Keyword::Offset)
        );
        if !has_tail {
            return Ok(inner);
        }
        match inner {
            SetExpr::Query(QueryBody::Select(mut s)) => {
                let (order_by, limit, offset) = self.parse_set_tail()?;
                s.order_by = order_by;
                s.limit = limit;
                s.offset = offset;
                Ok(SetExpr::Query(QueryBody::Select(s)))
            }
            body => {
                let (order_by, limit, offset) = self.parse_set_tail()?;
                Ok(SetExpr::Query(QueryBody::Nested(Box::new(QueryExpr {
                    with: None,
                    body,
                    order_by,
                    limit,
                    offset,
                    locking: None,
                }))))
            }
        }
    }

    /// Parse the FROM clause: a comma-separated list of join trees.
    fn parse_from(&mut self) -> Result<Vec<crate::ast::TableExpr>, ParseError> {
        let mut items = vec![self.join_tree()?];
        while self.eat_comma() {
            items.push(self.join_tree()?);
        }
        Ok(items)
    }

    /// A left-associative chain of joins over table factors. `JOIN` binds tighter
    /// than the top-level comma (handled by `parse_from`).
    fn join_tree(&mut self) -> Result<crate::ast::TableExpr, ParseError> {
        use crate::ast::{JoinConstraint, JoinKind, TableExpr};
        let mut left = self.table_factor()?;
        loop {
            let (kind, natural) = if self.eat_keyword(Keyword::Natural) {
                (self.join_kind()?, true)
            } else if self.peek_is_join_start() {
                (self.join_kind()?, false)
            } else {
                break;
            };
            let right = self.table_factor()?;
            let constraint = if natural || kind == JoinKind::Cross {
                if natural {
                    JoinConstraint::Natural
                } else {
                    JoinConstraint::None
                }
            } else if self.eat_keyword(Keyword::On) {
                JoinConstraint::On(self.expr(0)?)
            } else if self.eat_keyword(Keyword::Using) {
                self.expect(&Token::LParen)?;
                let mut cols = vec![self.expect_ident()?];
                while self.eat_comma() {
                    cols.push(self.expect_ident()?);
                }
                self.expect(&Token::RParen)?;
                JoinConstraint::Using(cols)
            } else {
                return Err(ParseError::new(
                    "expected ON or USING after JOIN",
                    self.peek_pos(),
                ));
            };
            left = TableExpr::Join {
                left: Box::new(left),
                right: Box::new(right),
                kind,
                constraint,
            };
        }
        Ok(left)
    }

    /// True if the next token begins a join clause (after an optional NATURAL).
    fn peek_is_join_start(&self) -> bool {
        matches!(
            self.peek(),
            Token::Keyword(
                Keyword::Join
                    | Keyword::Inner
                    | Keyword::Left
                    | Keyword::Right
                    | Keyword::Full
                    | Keyword::Cross,
            )
        )
    }

    /// Consume a join-kind prefix and the `JOIN` keyword. `INNER`/`LEFT`/`RIGHT`/
    /// `FULL` may be followed by `OUTER`; a bare `JOIN` is INNER.
    fn join_kind(&mut self) -> Result<crate::ast::JoinKind, ParseError> {
        use crate::ast::JoinKind;
        let kind = if self.eat_keyword(Keyword::Inner) {
            JoinKind::Inner
        } else if self.eat_keyword(Keyword::Left) {
            self.eat_keyword(Keyword::Outer);
            JoinKind::Left
        } else if self.eat_keyword(Keyword::Right) {
            self.eat_keyword(Keyword::Outer);
            JoinKind::Right
        } else if self.eat_keyword(Keyword::Full) {
            self.eat_keyword(Keyword::Outer);
            JoinKind::Full
        } else if self.eat_keyword(Keyword::Cross) {
            JoinKind::Cross
        } else {
            JoinKind::Inner // a bare JOIN
        };
        self.expect(&Token::Keyword(Keyword::Join))?;
        Ok(kind)
    }

    fn starts_query_expr(&self) -> bool {
        matches!(
            self.peek(),
            Token::Keyword(Keyword::Select | Keyword::Values | Keyword::With) | Token::LParen
        )
    }

    /// A table factor: a base table (`t` / `t alias` / `t AS alias`), a derived
    /// table (`( SELECT … ) alias`), or a parenthesized join (`( … )`).
    fn table_factor(&mut self) -> Result<crate::ast::TableExpr, ParseError> {
        use crate::ast::TableExpr;
        if *self.peek() == Token::LParen {
            self.bump();
            if matches!(
                self.peek(),
                Token::Keyword(Keyword::Select | Keyword::Values | Keyword::With)
            ) {
                let subquery = self.query_expr_after_open_paren()?;
                let alias = self.opt_alias()?.ok_or_else(|| {
                    ParseError::new("subquery in FROM must have an alias", self.peek_pos())
                })?;
                let columns = self.opt_column_aliases()?;
                return Ok(TableExpr::Derived {
                    subquery,
                    alias,
                    columns,
                });
            }
            let inner = self.join_tree()?;
            self.expect(&Token::RParen)?;
            return Ok(inner);
        }
        let mut name = self.expect_ident()?;
        if *self.peek() == Token::Dot {
            self.bump();
            let object = self.expect_ident()?;
            name = format!("{name}.{object}");
        }
        let alias = self.opt_alias()?;
        Ok(TableExpr::Table { name, alias })
    }

    /// An optional table alias: `AS ident`, or a bare `ident` that is not a
    /// keyword (so `FROM t JOIN …` does not read `JOIN` as an alias).
    fn opt_alias(&mut self) -> Result<Option<String>, ParseError> {
        if self.eat_keyword(Keyword::As) {
            return Ok(Some(self.expect_ident()?));
        }
        if let Token::Ident(_) = self.peek() {
            return Ok(Some(self.expect_ident()?));
        }
        Ok(None)
    }

    fn opt_column_aliases(&mut self) -> Result<Option<Vec<String>>, ParseError> {
        if *self.peek() != Token::LParen {
            return Ok(None);
        }
        self.bump();
        let mut cols = vec![self.expect_ident()?];
        while self.eat_comma() {
            cols.push(self.expect_ident()?);
        }
        self.expect(&Token::RParen)?;
        Ok(Some(cols))
    }

    /// Parse the integer count after a `LIMIT`/`OFFSET` keyword (`what` names it
    /// in error messages).
    fn expect_int_count(&mut self, what: &str) -> Result<i64, ParseError> {
        let pos = self.peek_pos();
        match self.bump() {
            Token::IntLit(s) => s
                .parse::<i64>()
                .map_err(|_| ParseError::new(format!("{what} value out of range"), pos)),
            other => Err(ParseError::new(
                format!("expected {what} count, found {other:?}"),
                pos,
            )),
        }
    }

    fn eat_comma(&mut self) -> bool {
        if *self.peek() == Token::Comma {
            self.bump();
            true
        } else {
            false
        }
    }

    /// Consume a string literal or return a 42601 parse error. Used for OPTIONS values.
    fn expect_string_lit(&mut self) -> Result<String, ParseError> {
        match self.bump() {
            Token::StringLit(s) => Ok(s),
            other => Err(ParseError::new(
                format!("expected string literal, found {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    /// Parse `OPTIONS ( ident 'string' [, …] )`. Returns an empty list if OPTIONS is absent.
    fn parse_options(&mut self) -> Result<crate::ast::OptionList, ParseError> {
        if !self.eat_keyword(Keyword::Options) {
            return Ok(vec![]);
        }
        self.expect(&Token::LParen)?;
        let mut opts = Vec::new();
        loop {
            let k = self.expect_ident()?;
            let v = self.expect_string_lit()?;
            opts.push((k, v));
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        Ok(opts)
    }

    /// Parse the `FOR <user>` clause of `CREATE/ALTER/DROP USER MAPPING`.
    /// Returns the user name as a lowercase string. Accepts `PUBLIC`, `CURRENT_USER`,
    /// or a plain identifier.
    fn parse_user_mapping_user(&mut self) -> Result<String, ParseError> {
        self.expect(&Token::Keyword(Keyword::For))?;
        match self.peek().clone() {
            Token::Keyword(Keyword::Public) => {
                self.bump();
                Ok("public".into())
            }
            Token::Keyword(Keyword::CurrentUser) => {
                self.bump();
                Ok("current_user".into())
            }
            Token::Ident(_) => self.expect_ident(),
            other => Err(ParseError::new(
                format!("expected user name after FOR, found {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    // SP40: FDW DDL parse functions

    /// `CREATE FOREIGN DATA WRAPPER <name> OPTIONS (…)`
    fn create_fdw(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect(&Token::Keyword(Keyword::Foreign))?;
        self.expect(&Token::Keyword(Keyword::Data))?;
        self.expect(&Token::Keyword(Keyword::Wrapper))?;
        let name = self.expect_ident()?;
        let options = self.parse_options()?;
        Ok(Statement::CreateFdw { name, options })
    }

    /// `DROP FOREIGN DATA WRAPPER [IF EXISTS] <name>`
    fn drop_fdw(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect(&Token::Keyword(Keyword::Foreign))?;
        self.expect(&Token::Keyword(Keyword::Data))?;
        self.expect(&Token::Keyword(Keyword::Wrapper))?;
        let if_exists = self.eat_if_exists()?;
        let name = self.expect_ident()?;
        Ok(Statement::DropFdw { name, if_exists })
    }

    /// `CREATE SERVER <name> FOREIGN DATA WRAPPER <wrapper> OPTIONS (…)`
    fn create_server(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect(&Token::Keyword(Keyword::Server))?;
        let name = self.expect_ident()?;
        self.expect(&Token::Keyword(Keyword::Foreign))?;
        self.expect(&Token::Keyword(Keyword::Data))?;
        self.expect(&Token::Keyword(Keyword::Wrapper))?;
        let wrapper = self.expect_ident()?;
        let options = self.parse_options()?;
        Ok(Statement::CreateServer {
            name,
            wrapper,
            options,
        })
    }

    /// `ALTER SERVER <name> OPTIONS (…)`
    fn alter_server(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        // ALTER is not a keyword yet; matched as ident
        self.bump(); // ALTER
        self.expect(&Token::Keyword(Keyword::Server))?;
        let name = self.expect_ident()?;
        let options = self.parse_options()?;
        Ok(Statement::AlterServer { name, options })
    }

    /// `DROP SERVER [IF EXISTS] <name>`
    fn drop_server(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect(&Token::Keyword(Keyword::Server))?;
        let if_exists = self.eat_if_exists()?;
        let name = self.expect_ident()?;
        Ok(Statement::DropServer { name, if_exists })
    }

    /// `CREATE USER MAPPING FOR <user> SERVER <server> OPTIONS (…)`
    fn create_user_mapping(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect(&Token::Keyword(Keyword::User))?;
        self.expect(&Token::Keyword(Keyword::Mapping))?;
        let user = self.parse_user_mapping_user()?;
        self.expect(&Token::Keyword(Keyword::Server))?;
        let server = self.expect_ident()?;
        let options = self.parse_options()?;
        Ok(Statement::CreateUserMapping {
            user,
            server,
            options,
        })
    }

    /// `ALTER USER MAPPING FOR <user> SERVER <server> OPTIONS (…)`
    fn alter_user_mapping(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        // ALTER is not a keyword yet; matched as ident
        self.bump(); // ALTER
        self.expect(&Token::Keyword(Keyword::User))?;
        self.expect(&Token::Keyword(Keyword::Mapping))?;
        let user = self.parse_user_mapping_user()?;
        self.expect(&Token::Keyword(Keyword::Server))?;
        let server = self.expect_ident()?;
        let options = self.parse_options()?;
        Ok(Statement::AlterUserMapping {
            user,
            server,
            options,
        })
    }

    /// `DROP USER MAPPING [IF EXISTS] FOR <user> SERVER <server>`
    fn drop_user_mapping(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect(&Token::Keyword(Keyword::User))?;
        self.expect(&Token::Keyword(Keyword::Mapping))?;
        let if_exists = self.eat_if_exists()?;
        let user = self.parse_user_mapping_user()?;
        self.expect(&Token::Keyword(Keyword::Server))?;
        let server = self.expect_ident()?;
        Ok(Statement::DropUserMapping {
            user,
            server,
            if_exists,
        })
    }

    /// `CREATE FOREIGN TABLE <name> (<col> <type>, …) SERVER <server> OPTIONS (…)`
    fn create_foreign_table(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{ColumnDef, Statement};
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect(&Token::Keyword(Keyword::Foreign))?;
        self.expect(&Token::Keyword(Keyword::Table))?;
        let name = self.expect_ident()?;
        self.expect(&Token::LParen)?;
        let mut columns = Vec::new();
        loop {
            let col_name = self.expect_ident()?;
            let ty = self.parse_type_name()?;
            columns.push(ColumnDef {
                name: col_name,
                ty,
                serial: None,
                constraints: Vec::new(),
            });
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        self.expect(&Token::Keyword(Keyword::Server))?;
        let server = self.expect_ident()?;
        let options = self.parse_options()?;
        Ok(Statement::CreateForeignTable {
            name,
            columns,
            server,
            options,
        })
    }

    /// `DROP FOREIGN TABLE [IF EXISTS] <name>`
    fn drop_foreign_table(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect(&Token::Keyword(Keyword::Foreign))?;
        self.expect(&Token::Keyword(Keyword::Table))?;
        let if_exists = self.eat_if_exists()?;
        let name = self.expect_ident()?;
        Ok(Statement::DropForeignTable { name, if_exists })
    }

    /// `IMPORT FOREIGN SCHEMA <remote_schema> [LIMIT TO | EXCEPT (<tables>)] FROM SERVER <server> [INTO <schema>]`
    fn import_foreign_schema(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{ImportSelector, Statement};
        self.expect(&Token::Keyword(Keyword::Import))?;
        self.expect(&Token::Keyword(Keyword::Foreign))?;
        self.expect(&Token::Keyword(Keyword::Schema))?;
        let remote_schema = self.expect_ident()?;
        let selector = if self.eat_keyword(Keyword::Limit) {
            self.expect(&Token::Keyword(Keyword::To))?;
            ImportSelector::LimitTo(self.parse_ident_list()?)
        } else if self.eat_keyword(Keyword::Except) {
            ImportSelector::Except(self.parse_ident_list()?)
        } else {
            ImportSelector::All
        };
        self.expect(&Token::Keyword(Keyword::From))?;
        self.expect(&Token::Keyword(Keyword::Server))?;
        let server = self.expect_ident()?;
        let into_schema = if self.eat_keyword(Keyword::Into) {
            // INTO public — `public` is a keyword here
            match self.peek().clone() {
                Token::Keyword(Keyword::Public) => {
                    self.bump();
                    "public".into()
                }
                Token::Ident(_) => self.expect_ident()?,
                other => {
                    return Err(ParseError::new(
                        format!("expected schema name after INTO, found {other:?}"),
                        self.peek_pos(),
                    ));
                }
            }
        } else {
            "public".into()
        };
        Ok(Statement::ImportForeignSchema {
            remote_schema,
            selector,
            server,
            into_schema,
        })
    }

    /// Parse `( ident, ident, … )` — used by `IMPORT FOREIGN SCHEMA`.
    fn parse_ident_list(&mut self) -> Result<Vec<String>, ParseError> {
        self.expect(&Token::LParen)?;
        let mut names = vec![self.expect_ident()?];
        while self.eat_comma() {
            names.push(self.expect_ident()?);
        }
        self.expect(&Token::RParen)?;
        Ok(names)
    }

    /// Consume `IF EXISTS` if present, returning whether it was seen.
    ///
    /// Returns `Ok(true)` when `IF EXISTS` is consumed, `Ok(false)` when `IF`
    /// is absent, and `Err` (SQLSTATE 42601) when `IF` is present but `EXISTS`
    /// does not follow — a malformed clause like `DROP SERVER IF NOTEXIST s`.
    fn eat_if_exists(&mut self) -> Result<bool, ParseError> {
        if self.eat_keyword(Keyword::If) {
            // `EXISTS` is always a keyword (Keyword::Exists) in the lexer.
            if *self.peek() == Token::Keyword(Keyword::Exists) {
                self.bump();
                return Ok(true);
            }
            // `IF` was consumed but `EXISTS` did not follow — reject with a
            // clear syntax error instead of silently mis-parsing the statement.
            return Err(ParseError::new(
                format!("expected EXISTS after IF, found {:?}", self.peek()),
                self.peek_pos(),
            ));
        }
        Ok(false)
    }
}

/// Test-support entry: parse a bare expression. `pub` (not cfg(test)) so the
/// executor crate's tests can reuse it; `doc(hidden)` keeps it out of the API.
///
/// # Errors
///
/// Returns a parse error when `sql` is not exactly one valid expression.
#[doc(hidden)]
pub fn parse_expr_for_test(sql: &str) -> Result<Expr, ParseError> {
    let mut p = Parser::new(lex(sql)?, sql.to_string());
    let e = p.expr(0)?;
    if *p.peek() != Token::Eof {
        return Err(ParseError::new(
            "trailing tokens after expression",
            p.peek_pos(),
        ));
    }
    Ok(e)
}

/// Public statement entry — implemented in Task 12.
///
/// # Errors
///
/// Returns a parse error when the SQL text cannot be tokenized or parsed.
pub fn parse(sql: &str) -> Result<Vec<crate::ast::Statement>, ParseError> {
    if let Some((statement, _identity)) = bounded_non_goal_refusal(sql) {
        return Ok(vec![statement]);
    }
    let mut p = Parser::new(lex(sql)?, sql.to_string());
    Ok(p.program_spanned()?
        .into_iter()
        .map(|(parsed, _)| parsed.statement)
        .collect())
}

/// Parse statements and return the parser-owned accepted command identity for
/// each one. Identity classification is the same mandatory gate used by [`parse`].
///
/// # Errors
///
/// Returns a parse error when the SQL text cannot be tokenized, parsed, or
/// classified.
pub fn parse_with_command_identities(
    sql: &str,
) -> Result<Vec<(crate::ast::Statement, crate::command::CommandIdentity)>, ParseError> {
    if let Some((statement, identity)) = bounded_non_goal_refusal(sql) {
        return Ok(vec![(statement, identity)]);
    }
    let mut parser = Parser::new(lex(sql)?, sql.to_string());
    parser
        .program_spanned()?
        .into_iter()
        .map(|(parsed, _range)| Ok((parsed.statement, parsed.command_identity)))
        .collect()
}

/// Parse `sql` into statements, each paired with its EXACT source text — the byte
/// slice of `sql` spanning that statement, trimmed of surrounding whitespace. The
/// multi-range gateway uses this to forward an INDIVIDUAL statement (not the whole
/// `;`-separated simple-query frame) to a remote range's leader, so a frame mixing a
/// local and a remote range never re-runs the local statement on the remote node.
///
/// # Errors
///
/// Returns a parse error when the SQL text cannot be tokenized or parsed.
pub fn parse_with_source(sql: &str) -> Result<Vec<(crate::ast::Statement, String)>, ParseError> {
    if let Some((statement, _identity)) = bounded_non_goal_refusal(sql) {
        return Ok(vec![(
            statement,
            sql.trim().trim_end_matches(';').trim().to_string(),
        )]);
    }
    let mut p = Parser::new(lex(sql)?, sql.to_string());
    p.program_spanned()?
        .into_iter()
        .map(|(parsed, range)| Ok((parsed.statement, sql[range].trim().to_string())))
        .collect()
}

fn bounded_non_goal_refusal(
    sql: &str,
) -> Option<(crate::ast::Statement, crate::command::CommandIdentity)> {
    let trimmed = sql.trim();
    let statement = trimmed.strip_suffix(';').unwrap_or(trimmed).trim();
    let candidate = lex(statement).ok()?;
    crate::ast::NON_GOAL_REFUSALS
        .iter()
        .find(|spec| refusal_tokens_match(&candidate, spec.representative_sql))
        .map(|spec| {
            (
                crate::ast::Statement::CompatibilityRefusal(spec.command),
                spec.identity,
            )
        })
}

fn refusal_tokens_match(candidate: &[(Token, usize)], representative: &str) -> bool {
    const IDENTIFIER_SLOTS: &[&str] = &[
        "conv",
        "conv2",
        "lang",
        "lang2",
        "postgres",
        "opc",
        "opc2",
        "opf",
        "opf2",
        "pub",
        "r",
        "r2",
        "sub",
        "ts",
        "ts2",
        "p",
        "p2",
        "t",
        "t2",
        "am",
        "handler_fn",
        "func",
        "int4eq",
        "f",
    ];
    let Ok(pattern) = lex(representative) else {
        return false;
    };
    candidate.len() == pattern.len()
        && candidate
            .iter()
            .zip(pattern)
            .all(|((actual, _), (expected, _))| match (&expected, actual) {
                (Token::Ident(slot), Token::Ident(_))
                    if IDENTIFIER_SLOTS.contains(&slot.as_str()) =>
                {
                    true
                }
                (Token::StringLit(_), Token::StringLit(_))
                | (Token::IntLit(_), Token::IntLit(_)) => true,
                _ => actual == &expected,
            })
}

fn encode_sequence_options(options: &crate::ast::SequenceOptions) -> Vec<String> {
    let mut encoded = Vec::new();
    if let Some(value) = options.start {
        encoded.push(format!("start={value}"));
    }
    if let Some(value) = options.increment {
        encoded.push(format!("increment={value}"));
    }
    if let Some(value) = options.min {
        encoded.push(format!("min={value}"));
    }
    if let Some(value) = options.max {
        encoded.push(format!("max={value}"));
    }
    if let Some(value) = options.cache {
        encoded.push(format!("cache={value}"));
    }
    if let Some(value) = options.cycle {
        encoded.push(format!("cycle={value}"));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use crabka_pgtypes::ColumnType;

    use super::*;
    use crate::ast::{
        BinaryOp, ColumnConstraint, ColumnDef, Expr, HashShardingSpec, IndexPlacement,
        IsolationLevel, SelectItem, ShardingSpec, Statement, TableConstraint, UnaryOp,
    };

    fn one(sql: &str) -> Statement {
        let mut v = parse(sql).expect("parse");
        assert_eq!(v.len(), 1);
        v.pop().expect("one statement")
    }

    fn only_query(sql: &str) -> crate::ast::QueryExpr {
        let statements = crate::parse(sql).expect("parse ok");
        assert_eq!(statements.len(), 1);
        match statements.into_iter().next().expect("one statement") {
            Statement::Query(q) => q,
            other => panic!("expected Statement::Query, got {other:?}"),
        }
    }

    fn only_select(sql: &str) -> crate::ast::SelectStmt {
        use crate::ast::{QueryBody, SetExpr};
        let q = only_query(sql);
        let SetExpr::Query(QueryBody::Select(select)) = q.body else {
            panic!("expected SELECT query body");
        };
        let mut select = *select;
        select.order_by = q.order_by;
        select.limit = q.limit;
        select.offset = q.offset;
        select.locking = q.locking;
        select
    }

    #[test]
    fn row_producing_statements_share_query_expr_shape() {
        use crate::ast::{QueryBody, SetExpr};

        let q = only_query("SELECT 1 ORDER BY 1 LIMIT 1");
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.limit, Some(1));
        assert!(matches!(q.body, SetExpr::Query(QueryBody::Select(_))));

        let q = only_query("VALUES (1), (2) ORDER BY 1 OFFSET 1");
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.offset, Some(1));
        assert!(matches!(q.body, SetExpr::Query(QueryBody::Values(_))));

        let q = only_query("SELECT 1 UNION ALL VALUES (2) ORDER BY 1");
        assert_eq!(q.order_by.len(), 1);
        assert!(matches!(q.body, SetExpr::SetOp { .. }));
    }

    #[test]
    fn parses_view_ddl_and_retains_definition() {
        let Statement::CreateView {
            name,
            definition,
            query,
        } = one("CREATE VIEW \"Sales View\" AS SELECT id FROM orders WHERE id > 1")
        else {
            panic!("expected CREATE VIEW");
        };
        assert_eq!(name, "Sales View");
        assert_eq!(definition, "SELECT id FROM orders WHERE id > 1");
        assert!(matches!(
            query.body,
            crate::ast::SetExpr::Query(crate::ast::QueryBody::Select(_))
        ));
        assert_eq!(
            one("DROP VIEW IF EXISTS \"Sales View\""),
            Statement::DropView {
                name: "Sales View".into(),
                if_exists: true,
            }
        );
    }

    #[test]
    fn legacy_query_forms_still_parse_after_query_unification() {
        for sql in [
            "SELECT a, b FROM t WHERE a > 1 ORDER BY b LIMIT 5",
            "VALUES (1), (2) ORDER BY 1",
            "(SELECT 1 ORDER BY 1 LIMIT 1) UNION SELECT 2 ORDER BY 1",
            "SELECT * FROM (SELECT 1 AS x) AS d",
            "SELECT * FROM (VALUES (1, 'a')) AS v(id, name)",
        ] {
            let q = only_query(sql);
            assert!(q.locking.is_none());
        }
    }

    #[test]
    fn derived_and_expression_subqueries_accept_query_exprs() {
        use crate::ast::{Expr, QueryBody, SelectItem, SetExpr, TableExpr};

        let outer = only_query("SELECT t.x FROM (SELECT 1 AS x UNION SELECT 2) AS t ORDER BY t.x");
        let SetExpr::Query(QueryBody::Select(select)) = outer.body else {
            panic!("expected outer SELECT query body");
        };
        let [
            TableExpr::Derived {
                subquery, alias, ..
            },
        ] = select.from.as_slice()
        else {
            panic!("expected one derived table");
        };
        assert_eq!(alias, "t");
        assert!(matches!(subquery.body, SetExpr::SetOp { .. }));

        let scalar = only_query("SELECT (VALUES (1) UNION SELECT 2 ORDER BY 1 LIMIT 1)");
        let SetExpr::Query(QueryBody::Select(select)) = scalar.body else {
            panic!("expected SELECT");
        };
        let SelectItem::Expr { expr, .. } = &select.projection[0] else {
            panic!("expected expression projection");
        };
        let Expr::ScalarSubquery(q) = expr else {
            panic!("expected scalar query expression");
        };
        assert!(matches!(q.body, SetExpr::SetOp { .. }));
        assert_eq!(q.limit, Some(1));
    }

    #[test]
    fn parenthesized_query_expr_tail_is_preserved_for_values_and_setops() {
        use crate::ast::{QueryBody, SetExpr, TableExpr};

        let q = only_query("SELECT v.x FROM (VALUES (2), (1) ORDER BY 1 LIMIT 1) AS v(x)");
        let SetExpr::Query(QueryBody::Select(select)) = q.body else {
            panic!("expected SELECT");
        };
        let [TableExpr::Derived { subquery, .. }] = select.from.as_slice() else {
            panic!("expected one derived table");
        };
        assert!(matches!(
            subquery.body,
            SetExpr::Query(QueryBody::Values(_))
        ));
        assert_eq!(subquery.order_by.len(), 1);
        assert_eq!(subquery.limit, Some(1));

        let q =
            only_query("SELECT s.x FROM (SELECT 2 AS x UNION SELECT 1 ORDER BY 1 LIMIT 1) AS s");
        let SetExpr::Query(QueryBody::Select(select)) = q.body else {
            panic!("expected SELECT");
        };
        let [TableExpr::Derived { subquery, .. }] = select.from.as_slice() else {
            panic!("expected one derived table");
        };
        assert!(matches!(subquery.body, SetExpr::SetOp { .. }));
        assert_eq!(subquery.order_by.len(), 1);
        assert_eq!(subquery.limit, Some(1));
    }

    #[test]
    fn quantified_query_expr_preserves_tail() {
        let Expr::Quantified { subquery, .. } =
            expr("1 = ANY (SELECT 1 ORDER BY 1 LIMIT 1 OFFSET 0)")
        else {
            panic!("expected quantified query expression");
        };
        assert_eq!(subquery.order_by.len(), 1);
        assert_eq!(subquery.limit, Some(1));
        assert_eq!(subquery.offset, Some(0));
    }

    #[test]
    fn nested_query_expr_locking_is_preserved_and_validated() {
        use crate::ast::{QueryBody, RowLockStrength, SelectItem, SetExpr};

        let q = only_query("SELECT (SELECT 1 FOR UPDATE)");
        let SetExpr::Query(QueryBody::Select(select)) = q.body else {
            panic!("expected outer SELECT");
        };
        let SelectItem::Expr { expr, .. } = &select.projection[0] else {
            panic!("expected expression projection");
        };
        let Expr::ScalarSubquery(subquery) = expr else {
            panic!("expected scalar subquery");
        };
        assert_eq!(subquery.locking, Some(RowLockStrength::ForUpdate));

        assert!(crate::parse("SELECT (VALUES (1) FOR UPDATE)").is_err());
        assert!(crate::parse("SELECT (SELECT 1 UNION SELECT 2 FOR UPDATE)").is_err());
    }

    #[test]
    fn top_level_parenthesized_query_expr_tail_is_preserved() {
        use crate::ast::{QueryBody, SetExpr};

        let q = only_query("(VALUES (2), (1) ORDER BY 1 LIMIT 1)");
        assert!(matches!(q.body, SetExpr::Query(QueryBody::Values(_))));
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.limit, Some(1));

        let q = only_query("(SELECT 2 UNION SELECT 1 ORDER BY 1 LIMIT 1)");
        assert!(matches!(q.body, SetExpr::SetOp { .. }));
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.limit, Some(1));
    }

    #[test]
    fn parenthesized_query_expr_outer_tail_preserves_inner_values_and_setop_tails() {
        use crate::ast::{QueryBody, SetExpr};

        let q = only_query("(VALUES (2), (1) ORDER BY 1) LIMIT 1");
        assert_eq!(q.limit, Some(1));
        assert!(q.order_by.is_empty());
        let SetExpr::Query(QueryBody::Nested(inner)) = q.body else {
            panic!("expected nested VALUES query body");
        };
        assert_eq!(inner.order_by.len(), 1);
        assert_eq!(inner.limit, None);
        assert!(matches!(inner.body, SetExpr::Query(QueryBody::Values(_))));

        let q = only_query("(SELECT 2 UNION SELECT 1 ORDER BY 1) LIMIT 1");
        assert_eq!(q.limit, Some(1));
        assert!(q.order_by.is_empty());
        let SetExpr::Query(QueryBody::Nested(inner)) = q.body else {
            panic!("expected nested set-op query body");
        };
        assert_eq!(inner.order_by.len(), 1);
        assert_eq!(inner.limit, None);
        assert!(matches!(inner.body, SetExpr::SetOp { .. }));
    }

    #[test]
    fn redundant_parenthesized_query_expr_preserves_inner_values_and_setop_tails() {
        use crate::ast::{QueryBody, SetExpr};

        let q = only_query("((VALUES (2), (1) ORDER BY 1))");
        assert!(q.order_by.is_empty());
        let SetExpr::Query(QueryBody::Nested(inner)) = q.body else {
            panic!("expected nested VALUES query body");
        };
        assert_eq!(inner.order_by.len(), 1);
        assert_eq!(inner.limit, None);
        assert!(matches!(inner.body, SetExpr::Query(QueryBody::Values(_))));

        let q = only_query("((VALUES (2), (1) ORDER BY 1) LIMIT 1)");
        assert!(q.order_by.is_empty());
        assert_eq!(q.limit, Some(1));
        let SetExpr::Query(QueryBody::Nested(inner)) = q.body else {
            panic!("expected nested VALUES query body");
        };
        assert_eq!(inner.order_by.len(), 1);
        assert_eq!(inner.limit, None);
        assert!(matches!(inner.body, SetExpr::Query(QueryBody::Values(_))));

        let q = only_query("((SELECT 2 UNION SELECT 1 ORDER BY 1))");
        assert!(q.order_by.is_empty());
        let SetExpr::Query(QueryBody::Nested(inner)) = q.body else {
            panic!("expected nested set-op query body");
        };
        assert_eq!(inner.order_by.len(), 1);
        assert_eq!(inner.limit, None);
        assert!(matches!(inner.body, SetExpr::SetOp { .. }));

        let q = only_query("((SELECT 2 UNION SELECT 1 ORDER BY 1) LIMIT 1)");
        assert!(q.order_by.is_empty());
        assert_eq!(q.limit, Some(1));
        let SetExpr::Query(QueryBody::Nested(inner)) = q.body else {
            panic!("expected nested set-op query body");
        };
        assert_eq!(inner.order_by.len(), 1);
        assert_eq!(inner.limit, None);
        assert!(matches!(inner.body, SetExpr::SetOp { .. }));
    }

    #[test]
    fn raw_query_expr_tail_placement_is_visible() {
        use crate::ast::{QueryBody, SetExpr};

        let q = only_query("SELECT 1 ORDER BY 1 LIMIT 1");
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.limit, Some(1));
        let SetExpr::Query(QueryBody::Select(select)) = q.body else {
            panic!("expected SELECT body");
        };
        assert!(select.order_by.is_empty());
        assert_eq!(select.limit, None);

        let q = only_query("((SELECT 1 ORDER BY 1))");
        assert!(q.order_by.is_empty());
        let SetExpr::Query(QueryBody::Select(select)) = q.body else {
            panic!("expected SELECT body");
        };
        assert_eq!(select.order_by.len(), 1);
    }

    #[test]
    fn left_and_right_keywords_parse_as_functions_in_expression_position() {
        use crate::ast::{FuncArgs, FuncCall};
        // `LEFT`/`RIGHT` are join keywords, but in expression position they are
        // the scalar functions `left(s, n)` / `right(s, n)` (PostgreSQL allows it).
        for (sql, name) in [("left('abc', 2)", "left"), ("right('abc', 2)", "right")] {
            match parse_expr_for_test(sql).expect("parse fn") {
                Expr::Func(FuncCall {
                    name: n,
                    args: FuncArgs::Exprs(a),
                    ..
                }) => {
                    assert_eq!(n, name);
                    assert_eq!(a.len(), 2);
                }
                other => panic!("expected a function call, got {other:?}"),
            }
        }
        // A bare `left`/`right` not followed by `(` is rejected (still reserved).
        assert!(parse_expr_for_test("left + 1").is_err());
        // And `LEFT JOIN` still parses as a join (keyword role preserved).
        assert!(parse("SELECT * FROM a LEFT JOIN b ON a.id = b.id").is_ok());
    }

    #[test]
    fn parse_with_source_pairs_each_statement_with_its_exact_text() {
        let v =
            parse_with_source("INSERT INTO a VALUES (1); INSERT INTO b VALUES (2)").expect("parse");
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].1, "INSERT INTO a VALUES (1)");
        assert_eq!(v[1].1, "INSERT INTO b VALUES (2)");
        // Surrounding whitespace (and the trailing `;`) is trimmed; a single
        // statement yields its own exact text.
        let solo = parse_with_source("  SELECT 1 ;  ").expect("parse one");
        assert_eq!(solo.len(), 1);
        assert_eq!(solo[0].1, "SELECT 1");
    }

    #[test]
    fn parses_create_table() {
        assert_eq!(
            one("CREATE TABLE t (id int4, name text)"),
            Statement::CreateTable {
                name: "t".into(),
                sharded: false,
                sharding: None,
                constraints: Vec::new(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        ty: ColumnType::Int4,
                        serial: None,
                        constraints: Vec::new(),
                    },
                    ColumnDef {
                        name: "name".into(),
                        ty: ColumnType::Text,
                        serial: None,
                        constraints: Vec::new(),
                    },
                ],
            }
        );
    }

    #[test]
    fn parses_column_constraints_and_default_insert_marker() {
        let Statement::CreateTable {
            columns,
            constraints,
            ..
        } = one(
            "CREATE TABLE t (id int4 PRIMARY KEY, name text NOT NULL DEFAULT 'anon', CHECK (id > 0), UNIQUE (name))",
        )
        else {
            panic!("expected create table");
        };
        assert!(matches!(
            columns[0].constraints[0],
            ColumnConstraint::PrimaryKey
        ));
        assert!(matches!(
            columns[1].constraints[0],
            ColumnConstraint::NotNull
        ));
        assert!(matches!(
            columns[1].constraints[1],
            ColumnConstraint::Default(_)
        ));
        assert!(matches!(constraints[0], TableConstraint::Check(_)));
        assert!(matches!(constraints[1], TableConstraint::Unique(_)));

        let Statement::Insert { rows, .. } = one("INSERT INTO t (id, name) VALUES (1, DEFAULT)")
        else {
            panic!("expected insert");
        };
        assert!(matches!(rows[0][1], Expr::Default));
    }

    #[test]
    fn parses_create_table_sharded_suffix() {
        assert_eq!(
            one("CREATE TABLE t (id int4) SHARDED"),
            Statement::CreateTable {
                name: "t".into(),
                columns: vec![ColumnDef {
                    name: "id".into(),
                    ty: ColumnType::Int4,
                    serial: None,
                    constraints: Vec::new(),
                }],
                constraints: Vec::new(),
                sharded: true,
                sharding: None,
            }
        );
    }

    #[test]
    fn parses_create_table_hash_sharded_suffix() {
        assert_eq!(
            one("CREATE TABLE t (id int4) SHARDED BY HASH (id) BUCKETS 16"),
            Statement::CreateTable {
                name: "t".into(),
                columns: vec![ColumnDef {
                    name: "id".into(),
                    ty: ColumnType::Int4,
                    serial: None,
                    constraints: Vec::new(),
                }],
                constraints: Vec::new(),
                sharded: true,
                sharding: Some(ShardingSpec::Hash(HashShardingSpec {
                    columns: vec!["id".into()],
                    buckets: 16,
                    co_location_group: None,
                })),
            }
        );
    }

    #[test]
    fn parses_create_index_metadata() {
        assert_eq!(
            one("CREATE UNIQUE GLOBAL INDEX users_email_idx ON users (email, id)"),
            Statement::CreateIndex {
                name: "users_email_idx".into(),
                table: "users".into(),
                columns: vec!["email".into(), "id".into()],
                unique: true,
                placement: IndexPlacement::Global,
            }
        );
        assert_eq!(
            one("CREATE INDEX users_id_idx ON users (id)"),
            Statement::CreateIndex {
                name: "users_id_idx".into(),
                table: "users".into(),
                columns: vec!["id".into()],
                unique: false,
                placement: IndexPlacement::Local,
            }
        );
    }

    #[test]
    fn parses_drop_index_if_exists() {
        assert_eq!(
            one("DROP INDEX IF EXISTS \"Users Name Idx\""),
            Statement::DropIndex {
                name: "Users Name Idx".into(),
                if_exists: true,
            }
        );
    }

    #[test]
    fn rejects_non_power_of_two_hash_bucket_count() {
        let err = parse("CREATE TABLE t (id int4) SHARDED BY HASH (id) BUCKETS 3")
            .expect_err("non-power-of-two buckets");
        assert_eq!(err.sqlstate(), "42601");
        assert!(err.message.contains("power of two"));
    }

    #[test]
    fn rejects_multiple_hash_sharding_columns() {
        let err = parse("CREATE TABLE t (a int4, b int4) SHARDED BY HASH (a, b) BUCKETS 16")
            .expect_err("the G9 hash grammar has exactly one column");
        assert_eq!(err.sqlstate(), "42601");
        assert!(err.message.contains("exactly one column"));
    }

    #[test]
    fn rejects_sharded_in_invalid_create_table_positions() {
        let leading = parse("CREATE SHARDED TABLE t (id int4)").expect_err("leading SHARDED");
        assert_eq!(leading.sqlstate(), "42601");
        assert!(
            leading.message.contains("expected Keyword(Table)")
                || leading.message.contains("unexpected token")
        );

        let embedded = parse("CREATE TABLE t SHARDED (id int4)").expect_err("embedded SHARDED");
        assert_eq!(embedded.sqlstate(), "42601");
        assert!(embedded.message.contains("expected LParen"));
    }

    #[test]
    fn unknown_column_type_is_error() {
        let e = parse("CREATE TABLE t (x widget)").expect_err("bad type");
        assert_eq!(e.sqlstate(), "42601");
    }

    #[test]
    fn parses_float8_column_types() {
        // SP30: `float8`, `float`, and the two-word `double precision` all map to Float8.
        for sql in [
            "CREATE TABLE t (x float8)",
            "CREATE TABLE t (x float)",
            "CREATE TABLE t (x double precision)",
        ] {
            match one(sql) {
                Statement::CreateTable { columns, .. } => {
                    assert_eq!(columns[0].ty, ColumnType::Float8, "for `{sql}`");
                }
                other => panic!("expected CreateTable, got {other:?}"),
            }
        }
        // Bare `double` (without `precision`) is not a type — PG rejects it too.
        assert!(parse("CREATE TABLE t (x double)").is_err());
    }

    #[test]
    fn parses_numeric_column_types_with_optional_typmod() {
        use crabka_pgtypes::numeric::Typmod;
        let ty = |sql: &str| match one(sql) {
            Statement::CreateTable { columns, .. } => columns[0].ty,
            other => panic!("expected CreateTable, got {other:?}"),
        };
        // Unconstrained `numeric`/`decimal`.
        assert_eq!(ty("CREATE TABLE t (x numeric)"), ColumnType::Numeric(None));
        assert_eq!(ty("CREATE TABLE t (x decimal)"), ColumnType::Numeric(None));
        // `numeric(p)` ≡ scale 0; `numeric(p, s)`.
        assert_eq!(
            ty("CREATE TABLE t (x numeric(10))"),
            ColumnType::Numeric(Some(Typmod {
                precision: 10,
                scale: 0
            }))
        );
        assert_eq!(
            ty("CREATE TABLE t (x numeric(10, 2))"),
            ColumnType::Numeric(Some(Typmod {
                precision: 10,
                scale: 2
            }))
        );
        // The cast target accepts the same modifier.
        assert!(matches!(
            expr("x::numeric(5,1)"),
            Expr::Cast {
                ty: ColumnType::Numeric(Some(Typmod {
                    precision: 5,
                    scale: 1
                })),
                ..
            }
        ));
    }

    #[test]
    fn parses_varchar_and_char_type_modifiers() {
        let ty = |sql: &str| match one(sql) {
            Statement::CreateTable { columns, .. } => columns[0].ty,
            other => panic!("expected CreateTable, got {other:?}"),
        };

        assert_eq!(ty("CREATE TABLE t (x varchar)"), ColumnType::Varchar(None));
        assert_eq!(
            ty("CREATE TABLE t (x varchar(12))"),
            ColumnType::Varchar(Some(12))
        );
        assert_eq!(
            ty("CREATE TABLE t (x character varying(7))"),
            ColumnType::Varchar(Some(7))
        );
        assert_eq!(ty("CREATE TABLE t (x char)"), ColumnType::Char(Some(1)));
        assert_eq!(
            ty("CREATE TABLE t (x character(3))"),
            ColumnType::Char(Some(3))
        );
        assert!(matches!(
            expr("'abc'::varchar(2)"),
            Expr::Cast {
                ty: ColumnType::Varchar(Some(2)),
                ..
            }
        ));
    }

    #[test]
    fn parses_niladic_keyword_functions_without_parens() {
        use crate::ast::{FuncArgs, FuncCall};
        // `current_date` etc. parse as zero-arg func calls (no parens).
        for name in [
            "current_date",
            "current_time",
            "localtimestamp",
            "localtime",
            "current_timestamp",
        ] {
            assert_eq!(
                expr(name),
                Expr::Func(FuncCall {
                    name: name.into(),
                    distinct: false,
                    args: FuncArgs::Exprs(vec![]),
                }),
                "niladic `{name}`"
            );
        }
        // The paren forms still parse via the normal func-call path.
        assert_eq!(
            expr("now()"),
            Expr::Func(FuncCall {
                name: "now".into(),
                distinct: false,
                args: FuncArgs::Exprs(vec![]),
            })
        );
        match expr("current_timestamp(0)") {
            Expr::Func(FuncCall { name, args, .. }) => {
                assert_eq!(name, "current_timestamp");
                assert!(matches!(args, FuncArgs::Exprs(ref v) if v.len() == 1));
            }
            other => panic!("expected a Func call, got {other:?}"),
        }
    }

    #[test]
    fn parses_numeric_literals() {
        // SP32: bare decimal/exponent literals are `numeric` (was `float8` in SP30).
        assert_eq!(expr("1.5"), Expr::NumericLiteral("1.5".into()));
        assert_eq!(expr(".25"), Expr::NumericLiteral(".25".into()));
        assert_eq!(expr("1e3"), Expr::NumericLiteral("1e3".into()));
        assert_eq!(expr("42"), Expr::IntLiteral("42".into()));
        // float participates in arithmetic with the usual precedence.
        match expr("1 + 2.5 * 2") {
            Expr::Binary {
                op: BinaryOp::Add,
                right,
                ..
            } => assert!(matches!(
                *right,
                Expr::Binary {
                    op: BinaryOp::Mul,
                    ..
                }
            )),
            other => panic!("expected Add(_, Mul), got {other:?}"),
        }
    }

    #[test]
    fn parses_drop_table() {
        use assert2::assert;
        assert!(
            one("DROP TABLE t")
                == Statement::DropTable {
                    names: vec!["t".into()],
                    if_exists: false,
                }
        );
    }

    #[test]
    fn create_table_accepts_and_ignores_storage_parameters() {
        use assert2::assert;
        // The pgbench -i statement verbatim, plus shape variants: value-less
        // params, namespaced params, string / float / negative / keyword-like
        // values. All parse to the same CreateTable as without the clause.
        let cases = [
            "create table pgbench_tellers(tid int not null,bid int,tbalance int,filler char(84)) with (fillfactor=100)",
            "CREATE TABLE pgbench_tellers (tid INT NOT NULL, bid INT, tbalance INT, filler CHAR(84)) WITH (autovacuum_enabled)",
            "CREATE TABLE pgbench_tellers (tid INT NOT NULL, bid INT, tbalance INT, filler CHAR(84)) WITH (toast.autovacuum_enabled = off, fillfactor = 70)",
            "CREATE TABLE pgbench_tellers (tid INT NOT NULL, bid INT, tbalance INT, filler CHAR(84)) WITH (autovacuum_vacuum_scale_factor = 0.5, parallel_workers = -1, vacuum_index_cleanup = 'auto')",
        ];
        let bare = one(
            "CREATE TABLE pgbench_tellers (tid INT NOT NULL, bid INT, tbalance INT, filler CHAR(84))",
        );
        for sql in cases {
            assert!(one(sql) == bare, "case: {sql}");
        }
    }

    #[test]
    fn create_table_storage_parameters_reject_malformed_clauses() {
        use assert2::assert;
        for sql in [
            "CREATE TABLE t (id INT) WITH ()",
            "CREATE TABLE t (id INT) WITH (fillfactor=)",
            "CREATE TABLE t (id INT) WITH (fillfactor=100",
            "CREATE TABLE t (id INT) WITH (=100)",
            "CREATE TABLE t (id INT) WITH (fillfactor=100,)",
            "CREATE TABLE t (id INT) WITH (fillfactor = -'x')",
        ] {
            assert!(crate::parse(sql).is_err(), "case: {sql}");
        }
    }

    #[test]
    fn parses_drop_table_if_exists() {
        use assert2::assert;
        assert!(
            one("DROP TABLE IF EXISTS t")
                == Statement::DropTable {
                    names: vec!["t".into()],
                    if_exists: true,
                }
        );
    }

    #[test]
    fn parses_multi_table_drop() {
        use assert2::assert;
        // pgbench -i's first statement: a comma-separated drop list.
        assert!(
            one(
                "DROP TABLE IF EXISTS pgbench_accounts, pgbench_branches, pgbench_history, pgbench_tellers"
            ) == Statement::DropTable {
                names: vec![
                    "pgbench_accounts".into(),
                    "pgbench_branches".into(),
                    "pgbench_history".into(),
                    "pgbench_tellers".into(),
                ],
                if_exists: true,
            }
        );
    }

    #[test]
    fn drop_table_rejects_trailing_comma() {
        use assert2::assert;
        assert!(crate::parse("DROP TABLE a, b,").is_err());
    }

    #[test]
    fn parses_vacuum_shapes_as_a_hint() {
        use assert2::assert;
        // pgbench -i's statement verbatim, plus the bare-option, parenthesized,
        // and table-list forms; the whole tail is discarded.
        for sql in [
            "vacuum analyze pgbench_branches",
            "VACUUM",
            "VACUUM FULL FREEZE VERBOSE ANALYZE",
            "VACUUM (ANALYZE, VERBOSE off) t1, t2",
            "VACUUM t1, t2",
        ] {
            assert!(one(sql) == Statement::Vacuum, "case: {sql}");
        }
        for sql in ["VACUUM (", "VACUUM ()", "VACUUM t1,", "VACUUM (analyze,) t"] {
            assert!(crate::parse(sql).is_err(), "case: {sql}");
        }
    }

    #[test]
    fn copy_accepts_and_ignores_the_freeze_hint() {
        use assert2::assert;
        // pgbench -i's COPY statement verbatim, plus value-form variants; all
        // parse identically to the un-hinted COPY.
        let bare = one("copy pgbench_accounts from stdin");
        for sql in [
            "copy pgbench_accounts from stdin with (freeze on)",
            "COPY pgbench_accounts FROM STDIN WITH (FREEZE)",
            "COPY pgbench_accounts FROM STDIN WITH (freeze off)",
            "COPY pgbench_accounts FROM STDIN WITH (freeze true)",
        ] {
            assert!(one(sql) == bare, "case: {sql}");
        }
        assert!(crate::parse("COPY t FROM STDIN WITH (freeze maybe)").is_err());
    }

    #[test]
    fn parses_schema_qualified_type_names_in_casts() {
        use assert2::assert;
        // pg_catalog-qualified names resolve to the bare type; regclass is a
        // recognized type in both spellings.
        let cases = [
            ("SELECT $1::pg_catalog.regclass", "SELECT $1::regclass"),
            ("SELECT $1::pg_catalog.text", "SELECT $1::text"),
            (
                "SELECT CAST(x AS pg_catalog.int8) FROM t",
                "SELECT CAST(x AS int8) FROM t",
            ),
        ];
        for (qualified, bare) in cases {
            assert!(one(qualified) == one(bare), "case: {qualified}");
        }
        assert!(crate::parse("SELECT $1::pg_catalog.not_a_type").is_err());
    }

    #[test]
    fn parses_truncate_shapes() {
        use assert2::assert;
        // (sql, names, restart_identity) — the pgbench -i statement verbatim,
        // the bare no-TABLE form, and the identity/cascade option tails.
        let cases: &[(&str, &[&str], bool)] = &[
            (
                "truncate table pgbench_accounts, pgbench_branches, pgbench_history, pgbench_tellers",
                &[
                    "pgbench_accounts",
                    "pgbench_branches",
                    "pgbench_history",
                    "pgbench_tellers",
                ],
                false,
            ),
            ("TRUNCATE t", &["t"], false),
            ("TRUNCATE TABLE t RESTART IDENTITY", &["t"], true),
            ("TRUNCATE t CONTINUE IDENTITY", &["t"], false),
            ("TRUNCATE t, u CASCADE", &["t", "u"], false),
            ("TRUNCATE t RESTART IDENTITY RESTRICT", &["t"], true),
        ];
        for (sql, names, restart_identity) in cases {
            assert!(
                one(sql)
                    == Statement::Truncate {
                        names: names.iter().map(|&n| n.into()).collect(),
                        restart_identity: *restart_identity,
                    },
                "case: {sql}"
            );
        }
    }

    #[test]
    fn truncate_rejects_malformed_tails() {
        use assert2::assert;
        for sql in [
            "TRUNCATE",
            "TRUNCATE TABLE",
            "TRUNCATE t,",
            "TRUNCATE t RESTART",
            "TRUNCATE t CONTINUE",
        ] {
            assert!(crate::parse(sql).is_err(), "case: {sql}");
        }
    }

    #[test]
    fn parses_drop_sequence_if_exists_list() {
        use assert2::assert;
        assert!(
            one("DROP SEQUENCE IF EXISTS s1, s2")
                == Statement::DropTable {
                    names: vec![
                        "__crabka_sequence__:s1".into(),
                        "__crabka_sequence__:s2".into(),
                    ],
                    if_exists: true,
                }
        );
    }

    #[test]
    fn parses_multi_row_insert_with_columns() {
        match one("INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')") {
            Statement::Insert {
                table,
                columns,
                rows,
                returning,
            } => {
                assert_eq!(table, "t");
                assert_eq!(columns, Some(vec!["a".into(), "b".into()]));
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].len(), 2);
                assert_eq!(returning, None);
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn parses_select_with_all_clauses() {
        let s = only_select("SELECT a, b AS bee FROM t WHERE a > 1 ORDER BY a DESC, b LIMIT 10");
        assert_eq!(s.projection.len(), 2);
        assert!(
            matches!(s.projection[1], SelectItem::Expr { alias: Some(ref n), .. } if n == "bee")
        );
        assert!(matches!(
            s.from.as_slice(),
            [crate::ast::TableExpr::Table { name, alias: None }] if name == "t"
        ));
        assert!(s.filter.is_some());
        assert_eq!(s.order_by.len(), 2);
        assert!(!s.order_by[0].asc); // DESC
        assert!(s.order_by[1].asc); // default ASC
        assert_eq!(s.limit, Some(10));
    }

    #[test]
    fn parses_aggregates_group_by_having() {
        use crate::ast::{FuncArgs, FuncCall};
        let s = only_select(
            "SELECT k, count(*), sum(v) FROM t WHERE v > 0 \
             GROUP BY k HAVING count(*) > 1 ORDER BY k LIMIT 5",
        );
        assert_eq!(s.projection.len(), 3);
        // count(*)
        assert!(matches!(
            s.projection[1],
            SelectItem::Expr {
                expr: Expr::Func(FuncCall { ref name, distinct: false, args: FuncArgs::Star }),
                ..
            } if name == "count"
        ));
        assert_eq!(
            s.group_by,
            vec![Expr::Column {
                table: None,
                name: "k".into()
            }]
        );
        assert!(s.having.is_some());
        assert_eq!(s.order_by.len(), 1);
        assert_eq!(s.limit, Some(5));
    }

    #[test]
    fn parses_count_distinct_and_func_args() {
        use crate::ast::{FuncArgs, FuncCall};
        let s = only_select("SELECT count(DISTINCT a + 1) FROM t");
        match &s.projection[0] {
            SelectItem::Expr {
                expr:
                    Expr::Func(FuncCall {
                        name,
                        distinct,
                        args,
                    }),
                ..
            } => {
                assert_eq!(name, "count");
                assert!(*distinct);
                match args {
                    FuncArgs::Exprs(v) => assert_eq!(v.len(), 1),
                    other @ FuncArgs::Star => panic!("expected Exprs, got {other:?}"),
                }
            }
            other => panic!("expected a Func projection, got {other:?}"),
        }
    }

    #[test]
    fn count_distinct_star_is_rejected() {
        // PostgreSQL rejects `count(DISTINCT *)` as a syntax error; so do we.
        assert!(parse("SELECT count(DISTINCT *) FROM t").is_err());
    }

    #[test]
    fn parses_multi_key_group_by() {
        let s = only_select("SELECT a, b, max(c) FROM t GROUP BY a, b");
        assert_eq!(
            s.group_by,
            vec![
                Expr::Column {
                    table: None,
                    name: "a".into()
                },
                Expr::Column {
                    table: None,
                    name: "b".into()
                }
            ]
        );
        assert!(s.having.is_none());
    }

    #[test]
    fn parses_select_star_no_from() {
        let s = only_select("SELECT *");
        assert_eq!(s.projection, vec![SelectItem::Wildcard]);
        assert!(s.from.is_empty());
    }

    #[test]
    fn parses_multiple_statements() {
        let v = parse("SELECT 1; SELECT 2;").expect("parse");
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn trailing_garbage_is_error() {
        assert!(parse("SELECT 1 foo bar").is_err());
    }

    #[test]
    fn parses_begin_variants() {
        assert_eq!(one("BEGIN"), Statement::Begin { isolation: None });
        assert_eq!(
            one("START TRANSACTION"),
            Statement::Begin { isolation: None }
        );
        assert_eq!(
            one("BEGIN ISOLATION LEVEL REPEATABLE READ"),
            Statement::Begin {
                isolation: Some(IsolationLevel::RepeatableRead)
            }
        );
        assert_eq!(
            one("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED"),
            Statement::Begin {
                isolation: Some(IsolationLevel::ReadCommitted)
            }
        );
    }

    #[test]
    fn start_requires_transaction_keyword() {
        // START TRANSACTION is valid; bare START is not a statement.
        assert_eq!(
            one("START TRANSACTION"),
            Statement::Begin { isolation: None }
        );
        assert!(parse("START").is_err());
    }

    #[test]
    fn parses_commit_rollback_aliases() {
        assert_eq!(one("COMMIT"), Statement::Commit);
        assert_eq!(one("END"), Statement::Commit);
        assert_eq!(one("ROLLBACK"), Statement::Rollback);
        assert_eq!(one("ABORT"), Statement::Rollback);
    }

    #[test]
    fn parses_update() {
        match one("UPDATE t SET a = 1, b = a + 2 WHERE id = 5") {
            Statement::Update {
                table,
                assignments,
                filter,
                returning,
            } => {
                assert_eq!(table, "t");
                assert_eq!(assignments.len(), 2);
                assert_eq!(assignments[0].0, "a");
                assert_eq!(assignments[1].0, "b");
                assert!(filter.is_some());
                assert_eq!(returning, None);
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn parses_select_for_update_and_share() {
        use crate::ast::RowLockStrength;
        assert_eq!(
            only_query("SELECT id FROM t FOR UPDATE").locking,
            Some(RowLockStrength::ForUpdate)
        );
        assert_eq!(
            only_query("SELECT id FROM t WHERE id > 1 FOR SHARE").locking,
            Some(RowLockStrength::ForShare)
        );
        assert_eq!(only_query("SELECT id FROM t").locking, None);
    }

    #[test]
    fn parses_delete() {
        match one("DELETE FROM t WHERE id > 3") {
            Statement::Delete {
                table,
                filter,
                returning,
            } => {
                assert_eq!(table, "t");
                assert!(filter.is_some());
                assert_eq!(returning, None);
            }
            other => panic!("expected Delete, got {other:?}"),
        }
        assert_eq!(
            one("DELETE FROM t"),
            Statement::Delete {
                table: "t".into(),
                filter: None,
                returning: None,
            }
        );
    }

    #[test]
    fn parses_dml_returning() {
        let Statement::Insert { returning, .. } = one("INSERT INTO t VALUES (1) RETURNING *")
        else {
            panic!("expected Insert");
        };
        assert_eq!(returning, Some(vec![SelectItem::Wildcard]));

        let Statement::Update { returning, .. } = one("UPDATE t SET a = 1 RETURNING a AS x, a + 1")
        else {
            panic!("expected Update");
        };
        assert!(matches!(
            returning.as_deref(),
            Some([
                SelectItem::Expr { alias: Some(alias), .. },
                SelectItem::Expr { alias: None, .. }
            ]) if alias == "x"
        ));

        let Statement::Delete { returning, .. } = one("DELETE FROM t RETURNING t.*") else {
            panic!("expected Delete");
        };
        assert_eq!(
            returning,
            Some(vec![SelectItem::QualifiedWildcard("t".into())])
        );
    }

    fn expr(sql: &str) -> Expr {
        // Wrap in a SELECT so the public parse() entry can reach it once
        // statements exist; until then, use the crate-internal expr parser.
        parse_expr_for_test(sql).expect("parse expr")
    }

    #[test]
    fn every_binary_operator_parses_to_its_op() {
        // Each operator token must map to its own BinaryOp arm in `expr` — pin all
        // ten so dropping any single arm (e.g. `<>`, `<=`, `-`, `/`) is caught.
        use crate::ast::BinaryOp::*;
        for (src, want) in [
            ("a = b", Eq),
            ("a <> b", Ne),
            ("a < b", Lt),
            ("a <= b", Le),
            ("a > b", Gt),
            ("a >= b", Ge),
            ("a + b", Add),
            ("a - b", Sub),
            ("a * b", Mul),
            ("a / b", Div),
            ("a || b", Concat),
        ] {
            match expr(src) {
                Expr::Binary { op, .. } => assert_eq!(op, want, "operator in `{src}`"),
                other => panic!("`{src}` should parse to a Binary expr, got {other:?}"),
            }
        }
    }

    #[test]
    fn bump_does_not_advance_past_eof() {
        // `bump` clamps at the trailing Eof token: a statement that runs out of
        // input (no table name) makes the parser bump AT Eof and then read the
        // next position for its error message. If `bump` advanced past Eof that
        // read would be out of bounds — instead we must get a clean parse error.
        assert!(parse("DROP TABLE").is_err());
        assert!(parse("CREATE TABLE").is_err());
    }

    #[test]
    fn precedence_mul_over_add() {
        // 1 + 2 * 3  ==  1 + (2 * 3)
        let e = expr("1 + 2 * 3");
        assert_eq!(
            e,
            Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::IntLiteral("1".into())),
                right: Box::new(Expr::Binary {
                    op: BinaryOp::Mul,
                    left: Box::new(Expr::IntLiteral("2".into())),
                    right: Box::new(Expr::IntLiteral("3".into())),
                }),
            }
        );
    }

    #[test]
    fn concat_precedence_and_associativity() {
        // `||` binds looser than `+` (PG): `a || b + c` == `a || (b + c)`.
        match expr("a || b + c") {
            Expr::Binary {
                op: BinaryOp::Concat,
                right,
                ..
            } => assert!(matches!(
                *right,
                Expr::Binary {
                    op: BinaryOp::Add,
                    ..
                }
            )),
            other => panic!("expected Concat(.., Add) , got {other:?}"),
        }
        // `||` binds tighter than `=` (PG): `a || b = c` == `(a || b) = c`.
        match expr("a || b = c") {
            Expr::Binary {
                op: BinaryOp::Eq,
                left,
                ..
            } => assert!(matches!(
                *left,
                Expr::Binary {
                    op: BinaryOp::Concat,
                    ..
                }
            )),
            other => panic!("expected Eq(Concat, ..), got {other:?}"),
        }
        // Left-associative: `a || b || c` == `(a || b) || c`.
        match expr("a || b || c") {
            Expr::Binary {
                op: BinaryOp::Concat,
                left,
                ..
            } => assert!(matches!(
                *left,
                Expr::Binary {
                    op: BinaryOp::Concat,
                    ..
                }
            )),
            other => panic!("expected left-nested Concat, got {other:?}"),
        }
        // `||` binds tighter than LIKE: `a || b LIKE p` == `(a || b) LIKE p`.
        match expr("a || b LIKE 'p'") {
            Expr::Like { expr, .. } => {
                assert!(matches!(
                    *expr,
                    Expr::Binary {
                        op: BinaryOp::Concat,
                        ..
                    }
                ));
            }
            other => panic!("expected Like over Concat, got {other:?}"),
        }
    }

    #[test]
    fn unary_minus_still_binds_tighter_than_star() {
        // After the SP29 renumber, `-a * b` must still be `(-a) * b`.
        match expr("-a * b") {
            Expr::Binary {
                op: BinaryOp::Mul,
                left,
                ..
            } => assert!(matches!(
                *left,
                Expr::Unary {
                    op: UnaryOp::Neg,
                    ..
                }
            )),
            other => panic!("expected Mul((-a), b), got {other:?}"),
        }
    }

    #[test]
    fn comparison_and_boolean_precedence() {
        // a = 1 AND b < 2  ==  (a = 1) AND (b < 2)
        let e = expr("a = 1 AND b < 2");
        assert!(matches!(
            e,
            Expr::Binary {
                op: BinaryOp::And,
                ..
            }
        ));
    }

    #[test]
    fn not_and_or_precedence() {
        // NOT x OR y  ==  (NOT x) OR y
        let e = expr("NOT x OR y");
        match e {
            Expr::Binary {
                op: BinaryOp::Or,
                left,
                ..
            } => {
                assert!(matches!(
                    *left,
                    Expr::Unary {
                        op: UnaryOp::Not,
                        ..
                    }
                ));
            }
            _ => panic!("expected OR at top, got {e:?}"),
        }
    }

    #[test]
    fn unary_minus_and_parens() {
        let e = expr("-(1 + 2)");
        assert!(matches!(
            e,
            Expr::Unary {
                op: UnaryOp::Neg,
                ..
            }
        ));
    }

    #[test]
    fn literals_columns_params() {
        assert_eq!(expr("'hi'"), Expr::StringLiteral("hi".into()));
        assert_eq!(expr("true"), Expr::BoolLiteral(true));
        assert_eq!(expr("null"), Expr::NullLiteral);
        assert_eq!(
            expr("col"),
            Expr::Column {
                table: None,
                name: "col".into()
            }
        );
        assert_eq!(expr("$2"), Expr::Param(2));
    }

    #[test]
    fn not_binds_tighter_than_and() {
        // NOT x AND y == (NOT x) AND y
        let e = expr("NOT x AND y");
        match e {
            Expr::Binary {
                op: BinaryOp::And,
                left,
                ..
            } => {
                assert!(
                    matches!(
                        *left,
                        Expr::Unary {
                            op: UnaryOp::Not,
                            ..
                        }
                    ),
                    "left of AND must be (NOT x), got {left:?}"
                );
            }
            _ => panic!("expected AND at root, got {e:?}"),
        }
    }

    #[test]
    fn comparison_binds_tighter_than_not() {
        // NOT a = 1 == NOT (a = 1)
        let e = expr("NOT a = 1");
        match e {
            Expr::Unary {
                op: UnaryOp::Not,
                expr,
            } => {
                assert!(
                    matches!(
                        *expr,
                        Expr::Binary {
                            op: BinaryOp::Eq,
                            ..
                        }
                    ),
                    "NOT operand must be (a = 1), got {expr:?}"
                );
            }
            _ => panic!("expected NOT at root, got {e:?}"),
        }
    }

    // ---- SP28: predicate + conditional expression breadth ----

    #[test]
    fn parses_is_null_and_is_not_null() {
        assert!(matches!(
            expr("a IS NULL"),
            Expr::IsNull { negated: false, .. }
        ));
        assert!(matches!(
            expr("a IS NOT NULL"),
            Expr::IsNull { negated: true, .. }
        ));
    }

    #[test]
    fn parses_in_and_not_in() {
        match expr("a IN (1, 2, 3)") {
            Expr::InList { list, negated, .. } => {
                assert_eq!(list.len(), 3);
                assert!(!negated);
            }
            other => panic!("expected InList, got {other:?}"),
        }
        assert!(matches!(
            expr("a NOT IN (1, 2)"),
            Expr::InList { negated: true, .. }
        ));
    }

    #[test]
    fn empty_in_list_is_rejected() {
        assert!(parse("SELECT a FROM t WHERE a IN ()").is_err());
    }

    #[test]
    fn not_in_is_infix_but_prefix_not_wraps_in() {
        // `x NOT IN (..)` is the infix negated predicate.
        assert!(matches!(
            expr("x NOT IN (1)"),
            Expr::InList { negated: true, .. }
        ));
        // `NOT x IN (..)` is prefix NOT over (x IN ..).
        match expr("NOT x IN (1)") {
            Expr::Unary {
                op: UnaryOp::Not,
                expr,
            } => assert!(matches!(*expr, Expr::InList { negated: false, .. })),
            other => panic!("expected NOT over InList, got {other:?}"),
        }
    }

    #[test]
    fn between_and_does_not_eat_boolean_and() {
        // `a BETWEEN 1 AND 2 AND b` == `(a BETWEEN 1 AND 2) AND b`.
        match expr("a BETWEEN 1 AND 2 AND b") {
            Expr::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => {
                assert!(matches!(*left, Expr::Between { negated: false, .. }));
                assert_eq!(
                    *right,
                    Expr::Column {
                        table: None,
                        name: "b".into()
                    }
                );
            }
            other => panic!("expected AND(Between, b), got {other:?}"),
        }
        assert!(matches!(
            expr("a NOT BETWEEN 1 AND 10"),
            Expr::Between { negated: true, .. }
        ));
    }

    #[test]
    fn parses_like_ilike_all_combinations() {
        assert!(matches!(
            expr("a LIKE 'x%'"),
            Expr::Like {
                negated: false,
                case_insensitive: false,
                ..
            }
        ));
        assert!(matches!(
            expr("a NOT LIKE 'x%'"),
            Expr::Like {
                negated: true,
                case_insensitive: false,
                ..
            }
        ));
        assert!(matches!(
            expr("a ILIKE 'x%'"),
            Expr::Like {
                negated: false,
                case_insensitive: true,
                ..
            }
        ));
        assert!(matches!(
            expr("a NOT ILIKE 'x%'"),
            Expr::Like {
                negated: true,
                case_insensitive: true,
                ..
            }
        ));
    }

    #[test]
    fn parses_searched_and_simple_case() {
        match expr("CASE WHEN a > 0 THEN 'pos' ELSE 'neg' END") {
            Expr::Case {
                operand,
                whens,
                else_result,
            } => {
                assert!(operand.is_none());
                assert_eq!(whens.len(), 1);
                assert!(else_result.is_some());
            }
            other => panic!("expected searched CASE, got {other:?}"),
        }
        match expr("CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END") {
            Expr::Case {
                operand,
                whens,
                else_result,
            } => {
                assert!(operand.is_some());
                assert_eq!(whens.len(), 2);
                assert!(else_result.is_none());
            }
            other => panic!("expected simple CASE, got {other:?}"),
        }
    }

    #[test]
    fn case_without_when_is_rejected() {
        assert!(parse("SELECT CASE END FROM t").is_err());
    }

    #[test]
    fn parses_select_distinct() {
        assert!(only_select("SELECT DISTINCT a FROM t").distinct);
        assert!(!only_select("SELECT a FROM t").distinct);
    }

    // ---- SP31: explicit casts ----

    #[test]
    fn parses_cast_both_forms_to_the_same_node() {
        use crabka_pgtypes::ColumnType;
        // `expr::type` and `CAST(expr AS type)` produce the identical Cast node.
        let want = Expr::Cast {
            expr: Box::new(Expr::IntLiteral("1".into())),
            ty: ColumnType::Int8,
        };
        assert_eq!(expr("1::int8"), want);
        assert_eq!(expr("CAST(1 AS int8)"), want);
        // `double precision` (two-word) and the other spellings resolve.
        assert!(matches!(
            expr("x::double precision"),
            Expr::Cast {
                ty: ColumnType::Float8,
                ..
            }
        ));
        assert!(matches!(
            expr("CAST(x AS integer)"),
            Expr::Cast {
                ty: ColumnType::Int4,
                ..
            }
        ));
        assert!(matches!(
            expr("x::text"),
            Expr::Cast {
                ty: ColumnType::Text,
                ..
            }
        ));
    }

    #[test]
    fn cast_binds_tighter_than_unary_minus_and_arithmetic() {
        // `-2::int8` == `-(2::int8)` — the cast binds to `2`, not to `-2`.
        match expr("-2::int8") {
            Expr::Unary {
                op: UnaryOp::Neg,
                expr,
            } => {
                assert!(matches!(*expr, Expr::Cast { .. }), "got {expr:?}");
            }
            other => panic!("expected Neg(Cast), got {other:?}"),
        }
        // `1 + 2::int8` == `1 + (2::int8)`.
        match expr("1 + 2::int8") {
            Expr::Binary {
                op: BinaryOp::Add,
                right,
                ..
            } => {
                assert!(matches!(*right, Expr::Cast { .. }), "got {right:?}");
            }
            other => panic!("expected Add(1, Cast), got {other:?}"),
        }
        // `a::int4 + b` == `(a::int4) + b`.
        match expr("a::int4 + b") {
            Expr::Binary {
                op: BinaryOp::Add,
                left,
                ..
            } => {
                assert!(matches!(*left, Expr::Cast { .. }), "got {left:?}");
            }
            other => panic!("expected Add(Cast, b), got {other:?}"),
        }
    }

    #[test]
    fn cast_is_left_associative_when_chained() {
        // `a::int4::text` == `(a::int4)::text`.
        match expr("a::int4::text") {
            Expr::Cast { expr: inner, ty } => {
                assert_eq!(ty, crabka_pgtypes::ColumnType::Text);
                assert!(
                    matches!(
                        *inner,
                        Expr::Cast {
                            ty: crabka_pgtypes::ColumnType::Int4,
                            ..
                        }
                    ),
                    "got {inner:?}"
                );
            }
            other => panic!("expected outer text Cast over int4 Cast, got {other:?}"),
        }
    }

    #[test]
    fn cast_to_unknown_type_is_a_parse_error() {
        assert!(parse("SELECT 1::widget").is_err());
        assert!(parse("SELECT CAST(1 AS widget)").is_err());
        // `cast` is a reserved keyword now, so `CAST(... )` requires `AS`.
        assert!(parse("SELECT CAST(1 int4)").is_err());
    }

    #[test]
    fn parses_uuid_type_in_create_table_and_cast() {
        use crate::ast::{Expr, Statement};

        let stmts = parse("CREATE TABLE t (id uuid)").expect("parse create");
        let Statement::CreateTable { columns, .. } = &stmts[0] else {
            panic!("expected create table");
        };
        assert_eq!(columns[0].ty, crabka_pgtypes::ColumnType::Uuid);
        assert!(matches!(
            parse_expr_for_test("'550e8400-e29b-41d4-a716-446655440000'::uuid")
                .expect("parse cast"),
            Expr::Cast {
                ty: crabka_pgtypes::ColumnType::Uuid,
                ..
            }
        ));
    }

    // ---- SP37: date/time type names, typed literals, EXTRACT, AT TIME ZONE ----

    #[test]
    fn parses_typed_datetime_literals() {
        use crate::ast::Expr;
        assert!(matches!(
            parse_expr_for_test("DATE '2024-01-01'").expect("d"),
            Expr::Cast { .. }
        ));
        assert!(matches!(
            parse_expr_for_test("INTERVAL '1 day'").expect("iv"),
            Expr::Cast { .. }
        ));
        assert!(matches!(
            parse_expr_for_test("TIMESTAMP '2024-01-01 00:00:00'").expect("ts"),
            Expr::Cast { .. }
        ));
        assert!(matches!(
            parse_expr_for_test("TIMESTAMPTZ '2024-01-01 00:00:00+00'").expect("tstz"),
            Expr::Cast { .. }
        ));
    }

    #[test]
    fn parses_extract_and_at_time_zone() {
        use crate::ast::Expr;
        assert!(matches!(
            parse_expr_for_test("extract(year from x)").expect("ex"),
            Expr::Func(_)
        ));
        let e = parse_expr_for_test("ts AT TIME ZONE 'UTC' = ts2").expect("attz");
        assert!(matches!(
            e,
            Expr::Binary {
                op: crate::ast::BinaryOp::Eq,
                ..
            }
        ));
    }

    #[test]
    fn parses_multiword_type_in_create_and_cast() {
        use crate::ast::{Expr, Statement};
        let stmts = crate::parser::parse(
            "CREATE TABLE t (a timestamp with time zone, b time without time zone)",
        )
        .expect("ct");
        assert!(matches!(&stmts[0], Statement::CreateTable { .. }));
        assert!(matches!(
            parse_expr_for_test("x::timestamp with time zone").expect("c"),
            Expr::Cast {
                ty: crabka_pgtypes::ColumnType::Timestamptz,
                ..
            }
        ));
    }

    // ---- SP37: SET / SHOW / RESET timezone GUC ----

    #[test]
    fn parses_set_timezone_all_spellings() {
        use crate::ast::SetValue;
        // SET timezone = '...' / SET timezone TO '...'
        assert_eq!(
            one("SET timezone = 'America/New_York'"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Value("America/New_York".into()),
            }
        );
        assert_eq!(
            one("SET timezone TO 'UTC'"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Value("UTC".into()),
            }
        );
        // SET TIME ZONE '...' (the special two-word spelling normalizes to `timezone`).
        assert_eq!(
            one("SET TIME ZONE 'America/New_York'"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Value("America/New_York".into()),
            }
        );
        // An identifier value (no quotes) is accepted too.
        assert_eq!(
            one("SET timezone TO utc"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Value("utc".into()),
            }
        );
        // The GUC name is normalized to lowercase.
        assert_eq!(
            one("SET TimeZone = 'UTC'"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Value("UTC".into()),
            }
        );
    }

    #[test]
    fn parses_set_local_flag_vs_local_value() {
        use crate::ast::SetValue;
        // `SET LOCAL timezone ...` — LOCAL is the flag (followed by a param name).
        assert_eq!(
            one("SET LOCAL timezone = 'UTC'"),
            Statement::Set {
                local: true,
                name: "timezone".into(),
                value: SetValue::Value("UTC".into()),
            }
        );
        assert_eq!(
            one("SET LOCAL TIME ZONE 'America/New_York'"),
            Statement::Set {
                local: true,
                name: "timezone".into(),
                value: SetValue::Value("America/New_York".into()),
            }
        );
        // `SET TIME ZONE LOCAL` — here LOCAL is the VALUE (→ Default), not the flag.
        assert_eq!(
            one("SET TIME ZONE LOCAL"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Default,
            }
        );
        // `SET TIME ZONE DEFAULT` is likewise the Default value.
        assert_eq!(
            one("SET TIME ZONE DEFAULT"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Default,
            }
        );
        // `SET timezone = DEFAULT` — DEFAULT as the value.
        assert_eq!(
            one("SET timezone = DEFAULT"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Default,
            }
        );
    }

    #[test]
    fn parses_show_and_reset() {
        use crate::ast::ResetTarget;
        assert_eq!(
            one("SHOW timezone"),
            Statement::Show {
                name: "timezone".into()
            }
        );
        assert_eq!(
            one("SHOW TIME ZONE"),
            Statement::Show {
                name: "timezone".into()
            }
        );
        assert_eq!(
            one("SHOW TimeZone"),
            Statement::Show {
                name: "timezone".into()
            }
        );
        assert_eq!(
            one("RESET timezone"),
            Statement::Reset {
                target: ResetTarget::Name("timezone".into())
            }
        );
        assert_eq!(
            one("RESET ALL"),
            Statement::Reset {
                target: ResetTarget::All
            }
        );
        assert_eq!(
            one("DISCARD ALL"),
            Statement::Set {
                local: false,
                name: "__discard_all".into(),
                value: crate::ast::SetValue::Default
            }
        );
        assert_eq!(
            one("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"),
            Statement::Set {
                local: false,
                name: "__set_transaction".into(),
                value: crate::ast::SetValue::Value("read committed".into())
            }
        );
    }

    #[test]
    fn parses_f1_guc_command_surface() {
        use crate::ast::{ResetTarget, SetValue};

        for sql in [
            "SET SESSION application_name TO 'session-app'",
            "SET application_name = 'session-app'",
        ] {
            assert_eq!(
                one(sql),
                Statement::Set {
                    local: false,
                    name: "application_name".into(),
                    value: SetValue::Value("session-app".into()),
                }
            );
        }
        assert_eq!(
            one("SET extra_float_digits = -15"),
            Statement::Set {
                local: false,
                name: "extra_float_digits".into(),
                value: SetValue::Value("-15".into()),
            }
        );
        assert_eq!(
            one("SET DateStyle TO ISO, MDY"),
            Statement::Set {
                local: false,
                name: "datestyle".into(),
                value: SetValue::Value("iso, mdy".into()),
            }
        );
        let statements = crate::parser::parse("SET DateStyle TO SQL DMY; SHOW DateStyle").unwrap();
        assert_eq!(
            statements[0],
            Statement::Set {
                local: false,
                name: "datestyle".into(),
                value: SetValue::Value("sql dmy".into()),
            }
        );
        assert_eq!(
            statements[1],
            Statement::Show {
                name: "datestyle".into(),
            }
        );
        assert_eq!(one("SHOW ALL"), Statement::Show { name: "all".into() });
        assert_eq!(
            one("RESET ALL"),
            Statement::Reset {
                target: ResetTarget::All,
            }
        );
        for unsupported in ["DISCARD PLANS", "DISCARD SEQUENCES", "DISCARD TEMP"] {
            let error = crate::parser::parse(unsupported).expect_err("unsupported DISCARD");
            assert!(error.to_string().contains("All"));
        }
    }

    #[test]
    fn set_show_reset_accept_unknown_names_at_parse_time() {
        // Name validation is the executor's job (42704); the parser accepts any name.
        use crate::ast::SetValue;
        assert_eq!(
            one("SET datestyle = 'ISO, MDY'"),
            Statement::Set {
                local: false,
                name: "datestyle".into(),
                value: SetValue::Value("ISO, MDY".into()),
            }
        );
        assert_eq!(
            one("SHOW search_path"),
            Statement::Show {
                name: "search_path".into()
            }
        );
    }

    #[test]
    fn parses_qualified_column() {
        use crate::ast::Expr;
        assert_eq!(
            expr("a.col"),
            Expr::Column {
                table: Some("a".into()),
                name: "col".into()
            }
        );
        assert_eq!(
            expr("col"),
            Expr::Column {
                table: None,
                name: "col".into()
            }
        );
    }

    #[test]
    fn parses_limit_and_offset_either_order() {
        for sql in [
            "SELECT a FROM t ORDER BY a LIMIT 5 OFFSET 10",
            "SELECT a FROM t ORDER BY a OFFSET 10 LIMIT 5",
        ] {
            let q = only_query(sql);
            assert_eq!(q.limit, Some(5));
            assert_eq!(q.offset, Some(10));
        }
    }

    #[test]
    fn parses_inner_join_on() {
        use crate::ast::{JoinConstraint, JoinKind, TableExpr};
        let s = only_select("SELECT a.x FROM a JOIN b ON a.id = b.id");
        assert_eq!(s.from.len(), 1);
        match &s.from[0] {
            TableExpr::Join {
                kind, constraint, ..
            } => {
                assert_eq!(*kind, JoinKind::Inner);
                assert!(matches!(constraint, JoinConstraint::On(_)));
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[test]
    fn parses_left_join_using_and_aliases_and_comma() {
        use crate::ast::{JoinConstraint, JoinKind, TableExpr};
        let s = only_select("SELECT * FROM a x LEFT OUTER JOIN b AS y USING (id), c");
        assert_eq!(s.from.len(), 2); // comma -> two top-level items
        match &s.from[0] {
            TableExpr::Join {
                kind,
                constraint,
                left,
                right,
            } => {
                assert_eq!(*kind, JoinKind::Left);
                assert_eq!(*constraint, JoinConstraint::Using(vec!["id".into()]));
                assert!(
                    matches!(**left, TableExpr::Table { ref alias, .. } if alias.as_deref() == Some("x"))
                );
                assert!(
                    matches!(**right, TableExpr::Table { ref alias, .. } if alias.as_deref() == Some("y"))
                );
            }
            other => panic!("expected Join, got {other:?}"),
        }
        assert!(matches!(&s.from[1], TableExpr::Table { name, alias: None } if name == "c"));
    }

    #[test]
    fn parses_natural_and_cross_and_derived_and_multiway() {
        use crate::ast::TableExpr;
        assert!(matches!(
            one("SELECT * FROM a NATURAL JOIN b"),
            Statement::Query(_)
        ));
        assert!(matches!(
            one("SELECT * FROM a CROSS JOIN b"),
            Statement::Query(_)
        ));
        assert!(matches!(
            one("SELECT * FROM a JOIN b ON a.id=b.id JOIN c ON b.id=c.id"),
            Statement::Query(_)
        ));
        let s = only_select("SELECT d.n FROM (SELECT n FROM t) AS d");
        assert!(matches!(&s.from[0], TableExpr::Derived { alias, .. } if alias == "d"));
    }

    #[test]
    fn parses_information_schema_qualified_tables() {
        let s = only_select(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
        );

        assert!(matches!(
            &s.from[..],
            [crate::ast::TableExpr::Table { name, alias: None }] if name == "information_schema.tables"
        ));
    }

    #[test]
    fn parses_qualified_wildcard() {
        use crate::ast::SelectItem;
        let s = only_select("SELECT a.* FROM a JOIN b ON a.id=b.id");
        assert_eq!(s.projection[0], SelectItem::QualifiedWildcard("a".into()));
    }

    #[test]
    fn derived_table_requires_alias() {
        assert!(parse("SELECT * FROM (SELECT 1)").is_err());
    }

    // ---- SP34: subquery expressions ----

    #[test]
    fn parses_scalar_subquery_in_expression_position() {
        match expr("(SELECT 1)") {
            Expr::ScalarSubquery(s) => {
                let crate::ast::SetExpr::Query(crate::ast::QueryBody::Select(select)) = &s.body
                else {
                    panic!("expected SELECT scalar subquery");
                };
                assert_eq!(select.projection.len(), 1);
                assert!(select.from.is_empty());
            }
            other => panic!("expected ScalarSubquery, got {other:?}"),
        }
        // Nested in arithmetic; and a plain parenthesised expr is still grouping.
        assert!(matches!(
            expr("1 + (SELECT a FROM t)"),
            Expr::Binary { right, .. } if matches!(*right, Expr::ScalarSubquery(_))
        ));
        assert!(matches!(expr("(1 + 2) * 3"), Expr::Binary { .. }));
    }

    #[test]
    fn parses_exists_and_not_exists() {
        assert!(matches!(expr("EXISTS (SELECT 1 FROM t)"), Expr::Exists(_)));
        match expr("NOT EXISTS (SELECT 1 FROM t)") {
            Expr::Unary {
                op: UnaryOp::Not,
                expr,
            } => {
                assert!(matches!(*expr, Expr::Exists(_)));
            }
            other => panic!("expected NOT(EXISTS …), got {other:?}"),
        }
    }

    #[test]
    fn parses_in_subquery_and_keeps_in_list_working() {
        assert!(matches!(expr("a IN (1, 2, 3)"), Expr::InList { .. }));
        match expr("a IN (SELECT id FROM t)") {
            Expr::InSubquery { negated, .. } => assert!(!negated),
            other => panic!("expected InSubquery, got {other:?}"),
        }
        match expr("a NOT IN (SELECT id FROM t)") {
            Expr::InSubquery { negated, .. } => assert!(negated),
            other => panic!("expected negated InSubquery, got {other:?}"),
        }
    }

    #[test]
    fn parses_quantified_any_all_some() {
        match expr("a = ANY (SELECT id FROM t)") {
            Expr::Quantified {
                op: BinaryOp::Eq,
                all,
                ..
            } => assert!(!all),
            other => panic!("expected ANY, got {other:?}"),
        }
        match expr("a > ALL (SELECT v FROM t)") {
            Expr::Quantified {
                op: BinaryOp::Gt,
                all,
                ..
            } => assert!(all),
            other => panic!("expected ALL, got {other:?}"),
        }
        match expr("a <> SOME (SELECT v FROM t)") {
            Expr::Quantified {
                op: BinaryOp::Ne,
                all,
                ..
            } => assert!(!all),
            other => panic!("expected SOME(=ANY), got {other:?}"),
        }
    }

    #[test]
    fn parses_with_in_nested_select_subquery_positions() {
        use crate::ast::{QueryBody, SetExpr, TableExpr};

        let sel = only_select("SELECT * FROM (WITH c AS (SELECT 1 AS x) SELECT * FROM c) AS d");
        let TableExpr::Derived { subquery, .. } = &sel.from[0] else {
            panic!("expected derived table");
        };
        assert!(subquery.with.is_some());
        let SetExpr::Query(QueryBody::Select(_)) = &subquery.body else {
            panic!("expected SELECT derived table");
        };

        match expr("(WITH c AS (SELECT 1 AS x) SELECT x FROM c)") {
            Expr::ScalarSubquery(s) => assert!(s.with.is_some()),
            other => panic!("expected scalar subquery, got {other:?}"),
        }
        match expr("EXISTS (WITH c AS (SELECT 1 AS x) SELECT x FROM c)") {
            Expr::Exists(s) => assert!(s.with.is_some()),
            other => panic!("expected EXISTS, got {other:?}"),
        }
        match expr("1 IN (WITH c AS (SELECT 1 AS x) SELECT x FROM c)") {
            Expr::InSubquery { subquery, .. } => assert!(subquery.with.is_some()),
            other => panic!("expected IN subquery, got {other:?}"),
        }
        match expr("1 = ANY (WITH c AS (SELECT 1 AS x) SELECT x FROM c)") {
            Expr::Quantified { subquery, .. } => assert!(subquery.with.is_some()),
            other => panic!("expected quantified subquery, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // Recursion-depth guard (54001 / statement_too_complex).
    //
    // Two distinct DoS crash modes, both must return a clean 54001 — never a
    // stack overflow that aborts the whole server process:
    //   mode 1  deep PARSE recursion: nested parens/CASE/NOT/unary minus all
    //           funnel through `expr`, so a guard there bounds them.
    //   mode 2  deep AST TREE from a flat left-assoc chain (`1+1+1+…`): the
    //           Pratt loop parses iteratively but builds an N-deep left-nested
    //           tree that then overflows in eval AND on recursive Box `Drop`.
    //           Capping the loop iterations prevents the over-deep tree.
    // ------------------------------------------------------------------

    /// Mode 1: `(((…1…)))` nested far beyond `MAX_DEPTH` → clean 54001, no crash.
    #[test]
    fn deeply_nested_parens_return_54001_not_a_crash() {
        let n = MAX_DEPTH * 4;
        let sql = format!("SELECT {}1{}", "(".repeat(n), ")".repeat(n));
        let err = parse(&sql).expect_err("too-deep parens must error, not crash");
        assert_eq!(err.sqlstate(), "54001", "got {err:?}");
        assert_eq!(err.message, "stack depth limit exceeded");
    }

    /// Mode 1: deep prefix `NOT` chain funnels through `expr` → 54001.
    #[test]
    fn deeply_nested_not_returns_54001() {
        let n = MAX_DEPTH * 4;
        let sql = format!("SELECT {}true", "NOT ".repeat(n));
        let err = parse(&sql).expect_err("too-deep NOT must error");
        assert_eq!(err.sqlstate(), "54001", "got {err:?}");
    }

    /// Mode 1: deeply nested scalar subqueries `(SELECT (SELECT …))` → 54001.
    #[test]
    fn deeply_nested_subqueries_return_54001() {
        let n = MAX_DEPTH * 2;
        let sql = format!("SELECT {}1{}", "(SELECT ".repeat(n), ")".repeat(n));
        let err = parse(&sql).expect_err("too-deep subqueries must error");
        assert_eq!(err.sqlstate(), "54001", "got {err:?}");
    }

    /// Mode 2: a long flat `1+1+1+…` chain is parsed iteratively but builds an
    /// N-deep left-nested tree; capping the Pratt loop returns 54001 so the
    /// tree (and its later eval/Drop) never over-deepens.
    #[test]
    fn long_left_assoc_chain_returns_54001() {
        let n = MAX_DEPTH * 4;
        let sql = format!("SELECT {}1", "1+".repeat(n));
        let err = parse(&sql).expect_err("too-long additive chain must error");
        assert_eq!(err.sqlstate(), "54001", "got {err:?}");
    }

    /// Crash-safety floor: a query nested right up to the limit must PARSE OK
    /// (no stack overflow). If this test ABORTS the process (stack overflow rather
    /// than a clean pass), `MAX_DEPTH` is too high for the runner's ~2 MiB stack
    /// and must be lowered. Each `(` adds one `expr` frame (one `DepthGuard`
    /// level); `select_core` + the outermost projection `expr` add 2 guard
    /// levels on top, so `MAX_DEPTH - 2` is the deepest paren query the parser
    /// admits — this test uses `MAX_DEPTH - 3` for one extra level of headroom.
    #[test]
    fn at_limit_parens_parse_ok() {
        let n = MAX_DEPTH - 3;
        let sql = format!("SELECT {}1{}", "(".repeat(n), ")".repeat(n));
        parse(&sql).expect("a query nested at the limit must parse, not crash");
    }

    /// The guard actually fires near the limit (not merely far away): a paren
    /// nest a few levels OVER `MAX_DEPTH` returns 54001, while the `at_limit`
    /// test above proves a nest just UNDER it still parses — so the boundary is
    /// where it is intended to be.
    #[test]
    fn parens_just_over_limit_returns_54001() {
        let n = MAX_DEPTH + 2;
        let sql = format!("SELECT {}1{}", "(".repeat(n), ")".repeat(n));
        assert_eq!(
            parse(&sql)
                .expect_err("just over the limit must error")
                .sqlstate(),
            "54001",
        );
    }

    /// A modest real-world nesting depth (well under the limit) parses fine —
    /// the guard does not reject ordinary queries.
    #[test]
    fn modest_nesting_parses_fine() {
        let sql = format!("SELECT {}1{}", "(".repeat(20), ")".repeat(20));
        parse(&sql).expect("modest nesting must parse");
        // A flat chain of 20 additions is fine too.
        parse(&format!("SELECT {}1", "1+".repeat(20))).expect("modest chain must parse");
    }

    /// Mode 2 (set ops): a long flat `… UNION ALL …` chain is parsed by the
    /// `set_expr` LOOP, building an N-deep left-nested `SetExpr` that would overflow
    /// the executor's `fold`/`resolve_set_columns` AND recursive `Drop`. The loop
    /// iteration cap returns a clean 54001.
    #[test]
    fn long_union_chain_returns_54001() {
        let n = MAX_DEPTH * 4;
        let sql = format!("SELECT 1{}", " UNION ALL SELECT 1".repeat(n));
        let err = parse(&sql).expect_err("too-long UNION chain must error, not crash");
        assert_eq!(err.sqlstate(), "54001", "got {err:?}");
    }

    /// Mode 1 (set ops): deeply nested parens around a query recurse
    /// `set_primary → set_expr → set_primary` (NOT through `expr`), so the
    /// `set_expr` guard must catch them → 54001.
    #[test]
    fn deeply_nested_query_parens_return_54001() {
        let n = MAX_DEPTH * 4;
        let sql = format!("{}SELECT 1{}", "(".repeat(n), ")".repeat(n));
        let err = parse(&sql).expect_err("too-deep query parens must error, not crash");
        assert_eq!(err.sqlstate(), "54001", "got {err:?}");
    }

    /// A modest `UNION` chain (well under the limit) parses fine — the cap does not
    /// reject ordinary set-op queries.
    #[test]
    fn modest_union_chain_parses_fine() {
        let sql = format!("SELECT 1{}", " UNION ALL SELECT 1".repeat(20));
        parse(&sql).expect("modest UNION chain must parse");
    }

    #[test]
    fn parses_standalone_values_query() {
        use crate::ast::{Expr, QueryBody, SetExpr};
        let q = only_query("VALUES (1, 'a'), (2, 'b') ORDER BY 1 LIMIT 1 OFFSET 1");
        let SetExpr::Query(QueryBody::Values(body)) = &q.body else {
            panic!("expected VALUES, got {q:?}")
        };
        assert_eq!(body.rows.len(), 2);
        assert_eq!(body.rows[0].len(), 2);
        assert!(matches!(body.rows[0][0], Expr::IntLiteral(_)));
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.limit, Some(1));
        assert_eq!(q.offset, Some(1));
    }

    #[test]
    fn parses_values_as_set_operation_branch() {
        use crate::ast::{QueryBody, SetExpr, SetOp};
        let q = only_query("VALUES (1) UNION ALL SELECT 2");
        let SetExpr::SetOp {
            op,
            all,
            left,
            right,
        } = &q.body
        else {
            panic!("expected set op body")
        };
        assert_eq!(*op, SetOp::Union);
        assert!(*all);
        assert!(matches!(
            left.as_ref(),
            SetExpr::Query(QueryBody::Values(_))
        ));
        assert!(matches!(
            right.as_ref(),
            SetExpr::Query(QueryBody::Select(_))
        ));
    }

    #[test]
    fn parses_values_derived_table_with_column_aliases() {
        use crate::ast::{QueryBody, SetExpr, TableExpr};
        let sel = only_select("SELECT id, name FROM (VALUES (1, 'a')) AS v(id, name)");
        let TableExpr::Derived {
            subquery,
            alias,
            columns,
        } = &sel.from[0]
        else {
            panic!("expected derived table")
        };
        assert!(matches!(
            subquery.body,
            SetExpr::Query(QueryBody::Values(_))
        ));
        assert_eq!(alias, "v");
        assert_eq!(
            columns.as_ref().expect("column aliases"),
            &vec!["id".to_string(), "name".to_string()]
        );
    }

    #[test]
    fn parses_select_derived_table_with_column_aliases() {
        use crate::ast::{QueryBody, SetExpr, TableExpr};
        let sel = only_select("SELECT n FROM (SELECT a FROM t) AS d(n)");
        let TableExpr::Derived {
            subquery,
            alias,
            columns,
        } = &sel.from[0]
        else {
            panic!("expected derived table")
        };
        assert!(matches!(
            subquery.body,
            SetExpr::Query(QueryBody::Select(_))
        ));
        assert_eq!(alias, "d");
        assert_eq!(
            columns.as_ref().expect("column aliases"),
            &vec!["n".to_string()]
        );
    }

    #[test]
    fn values_rows_must_have_at_least_one_expr() {
        assert!(crate::parse("VALUES ()").is_err());
    }

    #[test]
    fn parses_union_all_and_precedence() {
        use crate::ast::{SetExpr, SetOp};
        // INTERSECT binds tighter than UNION: A UNION B INTERSECT C => A UNION (B INTERSECT C)
        let q = only_query("SELECT 1 UNION SELECT 2 INTERSECT SELECT 3");
        let SetExpr::SetOp { op, all, right, .. } = &q.body else {
            panic!("expected top SetOp")
        };
        assert_eq!(*op, SetOp::Union);
        assert!(!*all);
        assert!(matches!(
            right.as_ref(),
            SetExpr::SetOp {
                op: SetOp::Intersect,
                ..
            }
        ));

        // UNION ALL sets `all`; left-associativity: A UNION B UNION C => (A UNION B) UNION C
        let q = only_query("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3");
        let SetExpr::SetOp { all, left, .. } = &q.body else {
            panic!()
        };
        assert!(*all);
        assert!(matches!(
            left.as_ref(),
            SetExpr::SetOp {
                op: SetOp::Union,
                ..
            }
        ));
    }

    #[test]
    fn union_order_by_limit_bind_to_whole_query() {
        let q = only_query("SELECT 1 UNION SELECT 2 ORDER BY 1 LIMIT 5 OFFSET 1");
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.limit, Some(5));
        assert_eq!(q.offset, Some(1));
    }

    #[test]
    fn parenthesized_branch_keeps_its_own_order_limit() {
        use crate::ast::{QueryBody, SetExpr};
        let q = only_query("(SELECT 1 ORDER BY 1 LIMIT 1) UNION SELECT 2");
        let SetExpr::SetOp { left, .. } = &q.body else {
            panic!("expected top SetOp")
        };
        let SetExpr::Query(QueryBody::Select(b)) = left.as_ref() else {
            panic!("left branch is a SELECT leaf")
        };
        assert_eq!(b.limit, Some(1));
        assert_eq!(b.order_by.len(), 1);
    }

    #[test]
    fn plain_select_is_unchanged() {
        let q = only_query("SELECT a FROM t ORDER BY a LIMIT 3");
        assert!(matches!(
            q.body,
            crate::ast::SetExpr::Query(crate::ast::QueryBody::Select(_))
        ));
        assert_eq!(q.limit, Some(3));
        assert_eq!(q.order_by.len(), 1);
    }

    #[test]
    fn for_update_with_set_op_is_rejected() {
        assert!(crate::parse("SELECT 1 UNION SELECT 2 FOR UPDATE").is_err());
    }

    #[test]
    fn parenthesized_tailed_query_exprs_can_be_set_op_branches() {
        use crate::ast::{QueryBody, SetExpr};

        let q = only_query("(SELECT 1 UNION SELECT 2 ORDER BY 1) UNION SELECT 3");
        let SetExpr::SetOp { left, .. } = &q.body else {
            panic!("expected top SetOp")
        };
        let SetExpr::Query(QueryBody::Nested(inner)) = left.as_ref() else {
            panic!("left branch preserves the tailed set-op as a nested QueryExpr")
        };
        assert_eq!(inner.order_by.len(), 1);
        assert!(matches!(inner.body, SetExpr::SetOp { .. }));

        let q = only_query("(VALUES (2), (1) ORDER BY 1 LIMIT 1) UNION SELECT 3");
        let SetExpr::SetOp { left, .. } = &q.body else {
            panic!("expected top SetOp")
        };
        let SetExpr::Query(QueryBody::Nested(inner)) = left.as_ref() else {
            panic!("left branch preserves the tailed VALUES as a nested QueryExpr")
        };
        assert_eq!(inner.order_by.len(), 1);
        assert_eq!(inner.limit, Some(1));
        assert!(matches!(inner.body, SetExpr::Query(QueryBody::Values(_))));
    }

    #[test]
    fn union_distinct_is_the_default_form() {
        use crate::ast::SetExpr;
        // `UNION DISTINCT` is the explicit spelling of the default (dedup) form:
        // it parses to the same tree as a bare `UNION` (all == false).
        let q = only_query("SELECT 1 UNION DISTINCT SELECT 2");
        let SetExpr::SetOp { all, .. } = &q.body else {
            panic!("expected SetOp")
        };
        assert!(!*all, "UNION DISTINCT is the dedup (all == false) form");
    }

    #[test]
    fn parses_with_select_values_and_setop_bodies() {
        use crate::ast::{QueryBody, SetExpr};

        let q = only_query("WITH a AS (SELECT 1 AS x), b(y) AS (VALUES (2)) SELECT x FROM a");
        let with = q.with.as_ref().expect("with clause");
        assert!(!with.recursive);
        assert_eq!(with.ctes.len(), 2);
        assert_eq!(with.ctes[0].name, "a");
        assert!(with.ctes[0].columns.is_none());
        assert_eq!(with.ctes[1].name, "b");
        assert_eq!(
            with.ctes[1].columns.as_deref(),
            Some(&["y".to_string()][..])
        );
        assert!(matches!(
            with.ctes[0].query.body,
            SetExpr::Query(QueryBody::Select(_))
        ));
        assert!(matches!(
            with.ctes[1].query.body,
            SetExpr::Query(QueryBody::Values(_))
        ));

        let q = only_query("WITH u AS (SELECT 1 UNION SELECT 2) SELECT * FROM u");
        assert!(matches!(
            q.with.as_ref().expect("with").ctes[0].query.body,
            SetExpr::SetOp { .. }
        ));
    }

    #[test]
    fn parses_with_recursive_and_rejects_duplicate_cte_names() {
        let q = only_query("WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r");
        assert!(q.with.as_ref().expect("with").recursive);

        let err = parse("WITH a AS (SELECT 1), a AS (SELECT 2) SELECT * FROM a")
            .expect_err("duplicate CTE names rejected during parse");
        assert_eq!(err.sqlstate(), "42712");
    }

    #[test]
    fn duplicate_cte_names_follow_identifier_normalization() {
        let err = parse("WITH a AS (SELECT 1), A AS (SELECT 2) SELECT * FROM a")
            .expect_err("unquoted identifiers normalize before duplicate CTE check");
        assert_eq!(err.sqlstate(), "42712");

        parse("WITH \"A\" AS (SELECT 1), a AS (SELECT 2) SELECT * FROM a")
            .expect("quoted case-distinct CTE names are parser-distinct");
    }

    // SP40: FDW DDL tests

    #[test]
    fn parses_create_server() {
        assert_eq!(
            one(
                "CREATE SERVER s FOREIGN DATA WRAPPER kafka_fdw OPTIONS (bootstrap 'h:9092', registry_url 'http://r')"
            ),
            Statement::CreateServer {
                name: "s".into(),
                wrapper: "kafka_fdw".into(),
                options: vec![
                    ("bootstrap".into(), "h:9092".into()),
                    ("registry_url".into(), "http://r".into()),
                ],
            }
        );
    }

    #[test]
    fn parses_create_foreign_table() {
        match one(
            "CREATE FOREIGN TABLE orders (id int4, total numeric) SERVER s OPTIONS (topic 'orders', value_format 'avro')",
        ) {
            Statement::CreateForeignTable {
                name,
                columns,
                server,
                options,
            } => {
                assert_eq!(name, "orders");
                assert_eq!(server, "s");
                assert_eq!(columns.len(), 2);
                assert_eq!(options[0], ("topic".into(), "orders".into()));
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn parses_import_foreign_schema_limit_to() {
        match one(
            "IMPORT FOREIGN SCHEMA kafka LIMIT TO (orders, payments) FROM SERVER s INTO public",
        ) {
            Statement::ImportForeignSchema {
                server,
                into_schema,
                selector,
                ..
            } => {
                assert_eq!(server, "s");
                assert_eq!(into_schema, "public");
                assert!(
                    matches!(selector, crate::ast::ImportSelector::LimitTo(ref v) if v == &["orders", "payments"])
                );
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn create_user_mapping_for_public() {
        match one(
            "CREATE USER MAPPING FOR PUBLIC SERVER s OPTIONS (sasl_mechanism 'SCRAM-SHA-256', username 'u', password 'p')",
        ) {
            Statement::CreateUserMapping { user, server, .. } => {
                assert_eq!(user, "public");
                assert_eq!(server, "s");
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn parses_create_fdw() {
        assert_eq!(
            one("CREATE FOREIGN DATA WRAPPER kafka_fdw OPTIONS (protocol 'kafka')"),
            Statement::CreateFdw {
                name: "kafka_fdw".into(),
                options: vec![("protocol".into(), "kafka".into())],
            }
        );
    }

    #[test]
    fn parses_drop_fdw() {
        assert_eq!(
            one("DROP FOREIGN DATA WRAPPER kafka_fdw"),
            Statement::DropFdw {
                name: "kafka_fdw".into(),
                if_exists: false,
            }
        );
        assert_eq!(
            one("DROP FOREIGN DATA WRAPPER IF EXISTS kafka_fdw"),
            Statement::DropFdw {
                name: "kafka_fdw".into(),
                if_exists: true,
            }
        );
    }

    #[test]
    fn parses_drop_server() {
        assert_eq!(
            one("DROP SERVER s"),
            Statement::DropServer {
                name: "s".into(),
                if_exists: false,
            }
        );
        assert_eq!(
            one("DROP SERVER IF EXISTS s"),
            Statement::DropServer {
                name: "s".into(),
                if_exists: true,
            }
        );
    }

    #[test]
    fn parses_alter_server() {
        assert_eq!(
            one("ALTER SERVER s OPTIONS (bootstrap 'b:9092')"),
            Statement::AlterServer {
                name: "s".into(),
                options: vec![("bootstrap".into(), "b:9092".into())],
            }
        );
    }

    #[test]
    fn parses_create_user_mapping_for_current_user() {
        match one("CREATE USER MAPPING FOR CURRENT_USER SERVER s OPTIONS (username 'u')") {
            Statement::CreateUserMapping {
                user,
                server,
                options,
            } => {
                assert_eq!(user, "current_user");
                assert_eq!(server, "s");
                assert_eq!(options[0], ("username".into(), "u".into()));
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn parses_alter_user_mapping() {
        match one("ALTER USER MAPPING FOR PUBLIC SERVER s OPTIONS (username 'newu')") {
            Statement::AlterUserMapping {
                user,
                server,
                options,
            } => {
                assert_eq!(user, "public");
                assert_eq!(server, "s");
                assert_eq!(options[0], ("username".into(), "newu".into()));
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn parses_drop_user_mapping() {
        assert_eq!(
            one("DROP USER MAPPING FOR PUBLIC SERVER s"),
            Statement::DropUserMapping {
                user: "public".into(),
                server: "s".into(),
                if_exists: false,
            }
        );
        assert_eq!(
            one("DROP USER MAPPING IF EXISTS FOR PUBLIC SERVER s"),
            Statement::DropUserMapping {
                user: "public".into(),
                server: "s".into(),
                if_exists: true,
            }
        );
    }

    #[test]
    fn parses_drop_foreign_table() {
        assert_eq!(
            one("DROP FOREIGN TABLE orders"),
            Statement::DropForeignTable {
                name: "orders".into(),
                if_exists: false,
            }
        );
        assert_eq!(
            one("DROP FOREIGN TABLE IF EXISTS orders"),
            Statement::DropForeignTable {
                name: "orders".into(),
                if_exists: true,
            }
        );
    }

    #[test]
    fn parses_import_foreign_schema_except() {
        match one("IMPORT FOREIGN SCHEMA remote EXCEPT (foo, bar) FROM SERVER s INTO myschema") {
            Statement::ImportForeignSchema {
                remote_schema,
                selector,
                server,
                into_schema,
            } => {
                assert_eq!(remote_schema, "remote");
                assert_eq!(server, "s");
                assert_eq!(into_schema, "myschema");
                assert!(
                    matches!(selector, crate::ast::ImportSelector::Except(ref v) if v == &["foo", "bar"])
                );
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn parses_import_foreign_schema_all() {
        match one("IMPORT FOREIGN SCHEMA kafka FROM SERVER s INTO public") {
            Statement::ImportForeignSchema {
                remote_schema,
                selector,
                server,
                into_schema,
            } => {
                assert_eq!(remote_schema, "kafka");
                assert_eq!(server, "s");
                assert_eq!(into_schema, "public");
                assert!(matches!(selector, crate::ast::ImportSelector::All));
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn parses_create_server_no_options() {
        assert_eq!(
            one("CREATE SERVER s FOREIGN DATA WRAPPER w"),
            Statement::CreateServer {
                name: "s".into(),
                wrapper: "w".into(),
                options: vec![],
            }
        );
    }

    #[test]
    fn parses_create_foreign_table_no_options() {
        match one("CREATE FOREIGN TABLE t (id int4) SERVER s") {
            Statement::CreateForeignTable {
                name,
                columns,
                server,
                options,
            } => {
                assert_eq!(name, "t");
                assert_eq!(server, "s");
                assert_eq!(columns.len(), 1);
                assert!(options.is_empty());
            }
            other => panic!("got {other:?}"),
        }
    }

    // Mutant-killing tests for DROP dispatch arms and ALTER guard

    #[test]
    fn drop_server_with_and_without_if_exists() {
        // Kills: Token::Keyword(Keyword::Server) arm deletion in DROP dispatch —
        // if this arm were deleted, DROP SERVER would fall through to drop_table and fail.
        assert_eq!(
            one("DROP SERVER myserver"),
            Statement::DropServer {
                name: "myserver".into(),
                if_exists: false,
            }
        );
        assert_eq!(
            one("DROP SERVER IF EXISTS myserver"),
            Statement::DropServer {
                name: "myserver".into(),
                if_exists: true,
            }
        );
    }

    #[test]
    fn drop_user_mapping_routes_correctly() {
        // Kills: Token::Keyword(Keyword::User) arm deletion in DROP dispatch
        assert_eq!(
            one("DROP USER MAPPING IF EXISTS FOR PUBLIC SERVER s"),
            Statement::DropUserMapping {
                user: "public".into(),
                server: "s".into(),
                if_exists: true,
            }
        );
    }

    #[test]
    fn drop_foreign_table_routes_to_correct_fn() {
        // Kills: Token::Keyword(Keyword::Foreign) arm + inner Table arm in DROP dispatch.
        // If the Foreign arm were deleted, this would fall to drop_table and fail on TABLE.
        assert_eq!(
            one("DROP FOREIGN TABLE IF EXISTS mytable"),
            Statement::DropForeignTable {
                name: "mytable".into(),
                if_exists: true,
            }
        );
        // Also verify plain DROP FOREIGN TABLE (no IF EXISTS)
        assert_eq!(
            one("DROP FOREIGN TABLE mytable"),
            Statement::DropForeignTable {
                name: "mytable".into(),
                if_exists: false,
            }
        );
    }

    #[test]
    fn drop_fdw_routes_to_correct_fn() {
        // Kills: Token::Keyword(Keyword::Data) arm inside DROP FOREIGN dispatch.
        // If this arm were deleted, DROP FOREIGN DATA WRAPPER would fail on DATA.
        assert_eq!(
            one("DROP FOREIGN DATA WRAPPER IF EXISTS myfdw"),
            Statement::DropFdw {
                name: "myfdw".into(),
                if_exists: true,
            }
        );
        assert_eq!(
            one("DROP FOREIGN DATA WRAPPER myfdw"),
            Statement::DropFdw {
                name: "myfdw".into(),
                if_exists: false,
            }
        );
    }

    #[test]
    fn alter_non_server_is_error() {
        // Kills: `s == "alter"` match guard replaced with true — verifies the guard
        // is necessary (non-"alter" idents must not be routed here)
        assert!(crate::parse("ALTER TABLE t ADD COLUMN x int4").is_err());
    }

    #[test]
    fn alter_ident_guard_is_case_sensitive_to_alter() {
        // Also kills: guard `s == "alter"` — "alters" is not "alter" and must error
        assert!(crate::parse("alters SERVER s OPTIONS (a 'b')").is_err());
    }

    #[test]
    fn eat_if_exists_requires_exists_after_if() {
        // Kills: `s.eq_ignore_ascii_case("exists")` match guard replaced with true/false —
        // "IF notexists" must NOT consume IF EXISTS
        assert!(crate::parse("DROP SERVER IF notexists myserver").is_err());
    }

    #[test]
    fn drop_if_without_exists_is_error() {
        // IF followed by a non-EXISTS token must produce a 42601 parse error, not
        // silently mis-parse the statement (e.g. treating the next ident as the
        // object name while `if_exists` comes back false).
        let e = crate::parse("DROP SERVER IF NOTEXIST s")
            .expect_err("IF not followed by EXISTS must fail");
        assert_eq!(e.sqlstate(), "42601");
    }

    #[test]
    fn drop_foreign_table_if_without_exists_is_error() {
        // Same invariant verified for a second DROP variant (DROP FOREIGN TABLE).
        let e = crate::parse("DROP FOREIGN TABLE IF foo t")
            .expect_err("IF not followed by EXISTS must fail");
        assert_eq!(e.sqlstate(), "42601");
    }

    /// Valid `IF EXISTS` and no-`IF` forms still parse correctly after the fix.
    #[test]
    fn drop_if_exists_valid_forms_still_parse() {
        crate::parse("DROP SERVER IF EXISTS s").expect("DROP SERVER IF EXISTS must parse");
        crate::parse("DROP SERVER s").expect("DROP SERVER without IF EXISTS must parse");
        crate::parse("DROP FOREIGN TABLE IF EXISTS t")
            .expect("DROP FOREIGN TABLE IF EXISTS must parse");
        crate::parse("DROP FOREIGN TABLE t")
            .expect("DROP FOREIGN TABLE without IF EXISTS must parse");
    }

    #[test]
    fn copy_from_stdin_parses_supported_text_subset() {
        let stmts = crate::parse("COPY accounts (id, name) FROM STDIN WITH (FORMAT text)")
            .expect("COPY FROM STDIN parses");
        let [crate::ast::Statement::Set { name, value, .. }] = stmts.as_slice() else {
            panic!("expected COPY sentinel statement, got {stmts:?}");
        };
        assert_eq!(name, crate::ast::COPY_FROM_STDIN_SENTINEL);
        assert_eq!(
            value,
            &crate::ast::SetValue::Value("text\taccounts\tid,name".into())
        );
    }

    #[test]
    fn copy_unsupported_paths_are_feature_not_supported() {
        for sql in [
            "COPY accounts TO STDOUT",
            "COPY accounts FROM '/tmp/accounts.tsv'",
            "COPY accounts FROM STDIN WITH (FORMAT binary)",
            "COPY accounts FROM STDIN WITH (DELIMITER ',')",
        ] {
            let err = crate::parse(sql).expect_err(sql);
            assert_eq!(err.sqlstate(), "0A000", "{sql}");
        }
    }
}
#[test]
fn explicit_compatibility_refusals_parse_to_typed_statements() {
    use crate::ast::{RefusalCommand, Statement};

    let cases = [
        (
            "ALTER DATABASE postgres RENAME TO other",
            RefusalCommand::AlterDatabase,
        ),
        ("CREATE DATABASE other", RefusalCommand::CreateDatabase),
        ("DROP DATABASE other", RefusalCommand::DropDatabase),
        (
            "ALTER EXTENSION plpgsql UPDATE",
            RefusalCommand::AlterExtension,
        ),
        ("DROP EXTENSION plpgsql", RefusalCommand::DropExtension),
        (
            "PREPARE TRANSACTION 'xid-1'",
            RefusalCommand::PrepareTransaction,
        ),
        ("COMMIT PREPARED 'xid-1'", RefusalCommand::CommitPrepared),
        (
            "ROLLBACK PREPARED 'xid-1'",
            RefusalCommand::RollbackPrepared,
        ),
    ];

    for (sql, command) in cases {
        let statements = parse(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(statements, vec![Statement::CompatibilityRefusal(command)]);
    }
}

#[test]
fn fdw_alter_refusals_share_typed_metadata() {
    use crate::ast::RefusalCommand;

    for (sql, expected) in [
        (
            "ALTER SERVER s OPTIONS (host 'localhost')",
            RefusalCommand::AlterServer,
        ),
        (
            "ALTER USER MAPPING FOR PUBLIC SERVER s OPTIONS (username 'u')",
            RefusalCommand::AlterUserMapping,
        ),
    ] {
        let statement = parse(sql).expect(sql).pop().expect("one statement");
        assert_eq!(statement.compatibility_refusal(), Some(expected));
    }
}

#[test]
fn dispatch_emits_exact_query_and_table_family_identities() {
    use crate::{command::CommandIdentity, parse_with_command_identities};

    for (sql, expected) in [
        ("SELECT 1", CommandIdentity::Select),
        (
            "WITH q AS (VALUES (1)) SELECT * FROM q",
            CommandIdentity::Select,
        ),
        ("VALUES (1)", CommandIdentity::Values),
        ("(VALUES (1))", CommandIdentity::Values),
        ("CREATE TABLE t (id int4)", CommandIdentity::CreateTable),
        ("ALTER TABLE t RENAME TO t2", CommandIdentity::AlterTable),
    ] {
        let parsed = parse_with_command_identities(sql).expect(sql);
        assert_eq!(parsed[0].1, expected, "{sql}");
    }

    for (family, sql) in [
        ("CREATE TABLE AS", "CREATE TABLE t AS SELECT 1"),
        (
            "CREATE TABLE INHERITS",
            "CREATE TABLE t (id int4) INHERITS (parent)",
        ),
        (
            "CREATE TABLE PARTITION BY",
            "CREATE TABLE t (id int4) PARTITION BY HASH (id)",
        ),
        (
            "CREATE TABLE PARTITION OF",
            "CREATE TABLE t PARTITION OF parent DEFAULT",
        ),
        (
            "ALTER TABLE ATTACH PARTITION",
            "ALTER TABLE t ATTACH PARTITION p DEFAULT",
        ),
        (
            "ALTER TABLE DETACH PARTITION",
            "ALTER TABLE t DETACH PARTITION p",
        ),
        (
            "ALTER TABLE ENABLE ROW LEVEL SECURITY",
            "ALTER TABLE t ENABLE ROW LEVEL SECURITY",
        ),
        ("TABLE", "TABLE t"),
    ] {
        assert!(
            parse_with_command_identities(sql).is_err(),
            "unsupported {family} must reject instead of emitting its generic parent identity: {sql}"
        );
    }
}

#[test]
fn multi_statement_dispatch_preserves_each_emitted_identity() {
    use crate::{command::CommandIdentity, parse_with_command_identities};

    let parsed = parse_with_command_identities(
        "BEGIN; VALUES (1); CREATE USER alice; END; ALTER TABLE t RENAME TO t2",
    )
    .expect("mixed statements parse");
    assert_eq!(
        parsed
            .into_iter()
            .map(|(_, identity)| identity)
            .collect::<Vec<_>>(),
        vec![
            CommandIdentity::Begin,
            CommandIdentity::Values,
            CommandIdentity::CreateUser,
            CommandIdentity::End,
            CommandIdentity::AlterTable,
        ]
    );
}

#[test]
fn shared_ast_fake_dispatch_cannot_exist_without_an_identity_argument() {
    use crate::{ast::Statement, command::CommandIdentity};

    fn fake_branch(identity: CommandIdentity) -> ParsedStatement {
        emitted(identity, Ok(Statement::Commit)).expect("fake branch emits")
    }

    assert_eq!(
        fake_branch(CommandIdentity::Commit).command_identity,
        CommandIdentity::Commit
    );
    assert_eq!(
        fake_branch(CommandIdentity::End).command_identity,
        CommandIdentity::End
    );
}

#[test]
fn explicit_compatibility_refusals_reject_malformed_neighbors() {
    for sql in [
        "CREATE DATABASE",
        "DROP DATABASE db unexpected",
        "ALTER DATABASE db",
        "ALTER EXTENSION ext UPDATE unexpected",
        "DROP EXTENSION",
        "PREPARE TRANSACTION xid",
        "COMMIT PREPARED xid",
        "ROLLBACK PREPARED 'xid' unexpected",
    ] {
        assert!(parse(sql).is_err(), "malformed refusal form parsed: {sql}");
    }
}

#[test]
fn every_non_goal_has_a_bounded_typed_refusal_probe() {
    use crate::ast::{NON_GOAL_REFUSALS, Statement};

    assert_eq!(NON_GOAL_REFUSALS.len(), 40);
    for spec in NON_GOAL_REFUSALS {
        assert_eq!(
            parse(spec.representative_sql),
            Ok(vec![Statement::CompatibilityRefusal(spec.command)]),
            "{}",
            spec.command.command_name(),
        );
        assert!(
            parse(&format!("{} unexpected", spec.representative_sql)).is_err(),
            "{} accepted an arbitrary trailing token",
            spec.command.command_name(),
        );
        let variant = refusal_variant_sql(spec.representative_sql);
        assert_ne!(variant, spec.representative_sql);
        assert_eq!(
            parse(&variant),
            Ok(vec![Statement::CompatibilityRefusal(spec.command)]),
            "{} variant: {variant}",
            spec.command.command_name(),
        );
    }
}

#[cfg(test)]
fn refusal_variant_sql(sql: &str) -> String {
    const PLACEHOLDERS: &[&str] = &[
        "conv",
        "conv2",
        "lang",
        "lang2",
        "postgres",
        "opc",
        "opc2",
        "opf",
        "opf2",
        "pub",
        "r",
        "r2",
        "sub",
        "ts",
        "ts2",
        "p",
        "p2",
        "t",
        "t2",
        "am",
        "handler_fn",
        "func",
        "int4eq",
        "f",
    ];
    let tokens = lex(sql).expect("representative lexes");
    let mut out = String::new();
    for (token, _) in tokens {
        if token == Token::Eof {
            break;
        }
        if !out.is_empty() {
            out.push(' ');
        }
        match token {
            Token::Ident(value) if PLACEHOLDERS.contains(&value.as_str()) => {
                out.push_str(&value);
                out.push_str("_variant");
            }
            Token::StringLit(_) => out.push_str("'variant'"),
            Token::IntLit(_) => out.push_str("42"),
            other => out.push_str(&token_sql(&other)),
        }
    }
    out
}

#[cfg(test)]
fn token_sql(token: &Token) -> String {
    match token {
        Token::Ident(value) => value.clone(),
        Token::Keyword(keyword) => format!("{keyword:?}").to_ascii_lowercase(),
        Token::LParen => "(".into(),
        Token::RParen => ")".into(),
        Token::Comma => ",".into(),
        Token::Eq => "=".into(),
        Token::Lt => "<".into(),
        Token::Plus => "+".into(),
        other => panic!("unhandled representative token {other:?}"),
    }
}
