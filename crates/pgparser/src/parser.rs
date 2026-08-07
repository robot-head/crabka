//! Recursive-descent statement parser with Pratt expression parsing.

use std::{cell::Cell, rc::Rc};

use crate::{
    ast::{ArraySubscript, BinaryOp, Expr, UnaryOp},
    error::ParseError,
    lexer::lex,
    token::{Keyword, Token},
};

/// Maximum nesting depth the parser builds before it returns `54001`
/// (`statement_too_complex`).
///
/// This bounds BOTH crash modes:
///   * mode 1: deep parse recursion (nested parens / subqueries / CASE / NOT /
///     unary minus, all of which funnel through `expr`/`query_expr`), and
///   * mode 2: a flat left-associative chain (`1+1+1+…`) whose Pratt loop is
///     iterative but builds an N-deep left-nested AST that would overflow later
///     in eval AND on recursive `Box` `Drop`. A cap on the loop iteration count
///     stops the over-deep tree from ever being built.
///
/// Chosen empirically (see the `at_limit_*` crash-safety tests): the server runs
/// on tokio's default ~2 MiB worker stack, and a query nested at `MAX_DEPTH` must
/// parse AND evaluate without overflowing while a deeper one returns a clean
/// error. Measured on that 2 MiB stack (both plain-debug AND llvm-cov-
/// instrumented builds, since CI runs `cargo llvm-cov nextest`), a deeply-nested
/// `(((…)))` paren parse — the heaviest recursion, an `expr`→`prefix`→`expr`
/// round-trip per level — can exhaust the stack before 50 levels once the full
/// pgwire/session call stack is included. `24` keeps the explicit 20-level
/// compatibility floor while leaving headroom for those enclosing frames; eval
/// (ceiling >12 000 on the same stack) and the AST's recursive `Box` `Drop` are
/// nowhere near it. Real queries nest well under 24 levels. This cap is
/// deliberately MUCH more conservative than `PostgreSQL`'s own (far higher)
/// `max_stack_depth` — both return `54001` for sufficiently deep input, which is
/// what matters for closing the `DoS`.
pub(crate) const MAX_DEPTH: usize = 24;

/// The result-level tail of a query expression: `ORDER BY` plus the row-count
/// window. `limit`/`offset` are arbitrary expressions because `PostgreSQL`
/// accepts any expression there, including a scalar subquery or a parameter.
#[derive(Debug, Default)]
struct SetTail {
    order_by: Vec<crate::ast::OrderItem>,
    limit: Option<crate::ast::Expr>,
    offset: Option<crate::ast::Expr>,
    with_ties: bool,
}

/// `PostgreSQL`'s cap on the grouping sets one query may expand to
/// (`parse_agg.c`), reported as `54001`.
const MAX_GROUPING_SETS: usize = 4096;

/// `PostgreSQL`'s cap on `CUBE`'s element list (`parse_clause.c`), reported as
/// `54011`. `ROLLUP` has no such limit. Its expansion is linear.
const MAX_CUBE_ELEMENTS: usize = 12;

/// A parsed `GROUP BY` clause: the flattened grouping expressions, and the set
/// structure over their indices when the clause expands to more than one set.
type GroupByClause = (Vec<crate::ast::Expr>, Option<crate::ast::GroupingClause>);

/// The three set-producing `GROUP BY` constructs, which lex as identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupingKeyword {
    Rollup,
    Cube,
    Sets,
}

/// Intern one grouping expression into the flattened list and return its index.
/// Repeating an expression in a `GROUP BY` clause repeats one grouping column,
/// which is why `GROUP BY ROLLUP(a), ROLLUP(a)` groups by `{a}` and not `{a, a}`.
fn intern_group_expr(exprs: &mut Vec<crate::ast::Expr>, expr: crate::ast::Expr) -> usize {
    if let Some(index) = exprs.iter().position(|e| *e == expr) {
        return index;
    }
    exprs.push(expr);
    exprs.len() - 1
}

/// How many grouping sets one `GROUP BY` item expands to.
fn grouping_set_count(item: &crate::ast::GroupItem) -> usize {
    use crate::ast::GroupItem;
    match item {
        GroupItem::Expr(_) | GroupItem::Empty | GroupItem::Composite(_) => 1,
        GroupItem::Rollup(elements) => elements.len() + 1,
        GroupItem::Cube(elements) => 1usize
            .checked_shl(u32::try_from(elements.len()).unwrap_or(u32::BITS))
            .unwrap_or(usize::MAX),
        GroupItem::GroupingSets(items) => items.iter().map(grouping_set_count).sum(),
    }
}

/// [`SetTail`] plus the `FOR UPDATE`/`FOR SHARE` clause that may follow it.
#[derive(Debug, Default)]
struct QueryTailAndLocking {
    tail: SetTail,
    locking: Option<crate::ast::LockingClause>,
}

/// The parenthesized list after a FROM-function alias: renaming its columns, or
/// declaring them for a record-returning function.
#[derive(Debug)]
enum FuncAliasColumns {
    Aliases(Vec<String>),
    Definitions(Vec<crate::ast::TableFuncColumnDef>),
}

pub(crate) struct Parser {
    toks: Vec<(Token, usize)>,
    source: String,
    pos: usize,
    /// Ordered schemas used to resolve an unqualified user type. `None` keeps
    /// the public parser entrypoint's legacy process-registry lookup.
    type_schemas: Option<Vec<String>>,
    /// Current recursion depth of the recursive productions (`expr`,
    /// `select_core`). Held behind an `Rc<Cell<…>>` so the RAII [`DepthGuard`]
    /// can hold an OWNED clone of the handle rather than a borrow of `self`.
    /// That lets the guarded method keep calling `&mut self` methods freely while
    /// the guard is alive (a `&self.depth` borrow would conflict with `&mut self`
    /// for the guard's whole lifetime). The guard's `Drop` decrements on EVERY
    /// exit path, including a `?` early-return, so the depth is always restored.
    depth: Rc<Cell<usize>>,
    /// The target named by a `SELECT … INTO <table>` seen while parsing the
    /// current statement. `INTO` sits between the projection and the `FROM`
    /// clause rather than at a statement boundary, so `select_core` records it
    /// here and [`Parser::query_statement`] turns the finished query into a
    /// [`crate::ast::Statement::CreateTableAs`].
    select_into: Option<crate::ast::RelationRef>,
    /// Whether the pending `SELECT … INTO` target was `TEMP`/`TEMPORARY`.
    select_into_temporary: bool,
    /// One frame per `SELECT` currently being parsed, innermost last: the window
    /// calls met so far in that SELECT. A subquery pushes its own frame, so a
    /// window call always lands on the SELECT that owns it.
    window_calls: Vec<Vec<crate::ast::WindowCall>>,
    /// How many window specifications enclose the cursor. A window definition is
    /// evaluated to place the rows a window call runs over, so it cannot itself
    /// contain one.
    window_spec_depth: usize,
    /// How many FROM-subqueries have already been given a synthesized alias, so
    /// each gets a distinct one. See [`Parser::unnamed_subquery_alias`].
    unnamed_subqueries: usize,
}

/// The parsed `transaction_mode` list of a `BEGIN`/`START TRANSACTION`/`SET
/// TRANSACTION`. Each field is `None` when the statement did not mention it, so
/// the session keeps its own default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct TransactionModes {
    isolation: Option<crate::ast::IsolationLevel>,
    read_only: Option<bool>,
    deferrable: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
struct ParsedStatement {
    statement: crate::ast::Statement,
    command_identity: crate::command::CommandIdentity,
}

/// `PostgreSQL`'s `schema_stmt`: the statements a `CREATE SCHEMA` element list
/// may contain. Anything else ends the list, so a following statement in the
/// same batch is not swallowed.
fn starts_schema_element(word: &str) -> bool {
    matches!(word, "grant")
}

/// The leading identifiers [`Parser::session_utility_statement`] claims.
fn is_session_utility_word(word: &str) -> bool {
    matches!(
        word,
        "savepoint"
            | "release"
            | "declare"
            | "fetch"
            | "move"
            | "close"
            | "prepare"
            | "execute"
            | "deallocate"
            | "lock"
            | "explain"
            | "analyze"
            | "cluster"
            | "reindex"
            | "checkpoint"
    )
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
            type_schemas: None,
            depth: Rc::new(Cell::new(0)),
            select_into: None,
            select_into_temporary: false,
            window_calls: Vec::new(),
            window_spec_depth: 0,
            unnamed_subqueries: 0,
        }
    }

    fn with_type_schemas(mut self, schemas: &[String]) -> Self {
        self.type_schemas = Some(schemas.to_vec());
        self
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
            // `public` is not a keyword in PostgreSQL at all — `pg_get_keywords()`
            // has no row for it, and `SELECT 1 AS public` is valid. It is lexed as
            // one here only so GRANT/REVOKE can match it in role position, which
            // PostgreSQL does by matching the STRING in its `RoleSpec`. Accepting
            // it wherever an identifier is wanted keeps those matches working while
            // letting `public.t`, and a column or alias called `public`, parse.
            Token::Keyword(Keyword::Public) => Ok("public".into()),
            // `DATA` is unreserved in PostgreSQL. This lexer promotes it only
            // to recognize FOREIGN DATA WRAPPER, so it remains an identifier
            // everywhere an ordinary name is accepted.
            Token::Keyword(Keyword::Data) => Ok("data".into()),
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

    /// `PostgreSQL`'s `ColLabel`: the label after `AS`, which may be an
    /// identifier or **any** keyword (`SELECT 1 AS true`, `AS select`, `AS from`).
    fn expect_col_label(&mut self) -> Result<String, ParseError> {
        if matches!(self.peek(), Token::Keyword(_)) {
            let label = self.keyword_label();
            self.bump();
            return Ok(label);
        }
        self.expect_ident()
    }

    /// `PostgreSQL`'s `BareColLabel`: the no-`AS` alias, which is an identifier
    /// or a keyword from the `bare_label_keyword` list. Consumes nothing and
    /// returns `None` when the cursor is on anything else (a clause keyword, an
    /// operator, the end of the statement).
    fn opt_bare_col_label(&mut self) -> Option<String> {
        // A quoted identifier is never a keyword, so `SELECT id "over"` labels the
        // column `over` even though the bare spelling is refused.
        if self.peek_is_quoted_ident()
            && let Token::Ident(name) = self.peek()
        {
            let name = name.clone();
            self.bump();
            return Some(name);
        }
        match self.peek() {
            Token::Ident(name) if is_bare_label_word(name) => {
                let name = name.clone();
                self.bump();
                Some(name)
            }
            Token::Keyword(_) if is_bare_label_word(&self.keyword_label()) => {
                let label = self.keyword_label();
                self.bump();
                Some(label)
            }
            _ => None,
        }
    }

    /// Is the token at the cursor a *quoted* identifier (`"select"`)? Quoting
    /// strips a word of every keyword property, so neither the `ColId` nor the
    /// `BareColLabel` restriction applies to it. The lexer folds the quotes away
    /// and keeps only the text, so this reads the source byte the token starts
    /// at.
    fn peek_is_quoted_ident(&self) -> bool {
        self.source.as_bytes().get(self.peek_pos()) == Some(&b'"')
    }

    /// The word at the cursor when it may be spelled as a `ColId`, whether it
    /// arrives as an identifier or as one of the words this lexer keywords.
    /// `None` for anything else, so the caller can leave the token for the
    /// clause it introduces.
    fn peek_col_id(&self) -> Option<String> {
        match self.peek() {
            Token::Ident(word) if self.peek_is_quoted_ident() || is_col_id_word(word) => {
                Some(word.clone())
            }
            Token::Keyword(_) => {
                let word = self.keyword_label();
                is_col_id_word(&word).then_some(word)
            }
            _ => None,
        }
    }

    /// Consume a `ColId`. Reports `PostgreSQL`'s own syntax error when the
    /// word at the cursor is reserved or a type/function-name keyword.
    fn expect_col_id(&mut self) -> Result<String, ParseError> {
        if let Some(name) = self.peek_col_id() {
            self.bump();
            return Ok(name);
        }
        let pos = self.peek_pos();
        match self.peek() {
            Token::Ident(word) => {
                let word = word.clone();
                Err(ParseError::new_sqlstate(
                    "42601",
                    format!("syntax error at or near \"{word}\""),
                    pos,
                ))
            }
            Token::Keyword(_) => {
                let word = self.keyword_label();
                Err(ParseError::new_sqlstate(
                    "42601",
                    format!("syntax error at or near \"{word}\""),
                    pos,
                ))
            }
            other => Err(ParseError::new(
                format!("expected identifier, found {other:?}"),
                pos,
            )),
        }
    }

    /// The source spelling of the keyword at the cursor, lowercased. This is the
    /// column name a keyword used as a label produces. A keyword token is never quoted
    /// (a quoted `"select"` lexes as an identifier), so its word is exactly the
    /// run of identifier characters at the token's byte offset.
    fn keyword_label(&self) -> String {
        let rest = &self.source[self.peek_pos()..];
        let end = rest
            .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
            .unwrap_or(rest.len());
        rest[..end].to_ascii_lowercase()
    }

    /// Match a word that may lex as an identifier *or* as a reserved keyword.
    /// The SQL/JSON grammar spells several of its option words with words that
    /// are keywords elsewhere (`ARRAY`, `WITH`, `UNIQUE`, `ON`, `NULL`,
    /// `WRAPPER`), so those productions match on the word, not the token kind.
    fn peek_word_eq(&self, want: &str) -> bool {
        match self.peek() {
            Token::Ident(s) => s.eq_ignore_ascii_case(want),
            Token::Keyword(kw) => keyword_word(*kw) == Some(want),
            _ => false,
        }
    }

    fn peek2_word_eq(&self, want: &str) -> bool {
        match self.peek2() {
            Token::Ident(s) => s.eq_ignore_ascii_case(want),
            Token::Keyword(kw) => keyword_word(*kw) == Some(want),
            _ => false,
        }
    }

    fn eat_word_eq(&mut self, want: &str) -> bool {
        if self.peek_word_eq(want) {
            self.bump();
            true
        } else {
            false
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

    /// Parse a SQL type name into a [`ColumnType`], shared by `CREATE TABLE`
    /// column definitions and the SP31 cast target (`CAST(_ AS ty)` / `_::ty`).
    /// Folds the two-word `double precision` (SP30) into one normalized name; an
    /// unknown type name is 42704 (`undefined_object`) with `PostgreSQL`'s
    /// "type … does not exist" message, in every context that names a type.
    fn parse_type_name(&mut self) -> Result<crabka_pgtypes::ColumnType, ParseError> {
        let type_pos = self.peek_pos();
        let mut type_word = self.expect_ident()?;
        let type_schema = if *self.peek() == Token::Dot {
            self.bump();
            let schema = std::mem::replace(&mut type_word, self.expect_ident()?);
            Some(schema)
        } else {
            None
        };
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
        if type_word.eq_ignore_ascii_case("bit")
            && matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("varying"))
        {
            self.bump();
            type_word = "bit varying".to_string();
        }
        // A `timestamp(2)` / `time(2)` / `interval(2)` fractional-seconds
        // precision sits before any `with time zone` qualifier. Crabka stores
        // every date/time value at microsecond resolution, so the precision is
        // parsed and discarded rather than rounding the stored value.
        if matches!(
            type_word.to_ascii_lowercase().as_str(),
            "timestamp" | "timestamptz" | "time" | "timetz" | "interval"
        ) && *self.peek() == Token::LParen
        {
            self.bump();
            self.expect_u16("fractional seconds precision")?;
            self.expect(&Token::RParen)?;
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
        let lookup_name = type_schema.as_ref().map_or_else(
            || type_word.clone(),
            |schema| format!("{schema}.{type_word}"),
        );
        // `pg_catalog` qualification exposes built-ins. Every other qualifier
        // reaches the user-type registry as two identity parts so a quoted dot
        // in either identifier is never mistaken for qualification.
        let ty = match type_schema.as_deref() {
            Some("pg_catalog") => crabka_pgtypes::ColumnType::from_builtin_sql_name(&type_word),
            Some(schema) => crabka_pgtypes::usertype::column_type_for_name_in(schema, &type_word),
            None => self.type_schemas.as_ref().map_or_else(
                || crabka_pgtypes::ColumnType::from_sql_name(&type_word),
                |schemas| {
                    schemas.iter().find_map(|schema| {
                        if schema == "pg_catalog" {
                            crabka_pgtypes::ColumnType::from_builtin_sql_name(&type_word)
                        } else {
                            crabka_pgtypes::usertype::column_type_for_name_in(schema, &type_word)
                        }
                    })
                },
            ),
        }
        .ok_or_else(|| {
            ParseError::new_sqlstate(
                "42704",
                format!("type \"{lookup_name}\" does not exist"),
                type_pos,
            )
        })?;
        // SP32: `numeric`/`decimal` may carry a `(precision[, scale])` modifier.
        let base = if ty.is_numeric() && *self.peek() == Token::LParen {
            self.parse_numeric_typmod()?
        } else if matches!(
            ty,
            crabka_pgtypes::ColumnType::Varchar(_) | crabka_pgtypes::ColumnType::Char(_)
        ) && *self.peek() == Token::LParen
        {
            self.parse_string_typmod(ty)?
        } else if matches!(
            ty,
            crabka_pgtypes::ColumnType::Bit(_) | crabka_pgtypes::ColumnType::VarBit(_)
        ) {
            self.parse_bit_typmod(ty, type_pos)?
        } else {
            ty
        };
        self.parse_array_type_suffix(base, &lookup_name, type_pos)
    }

    /// Consume an optional array suffix after a base type name: `[]`, `[N]`,
    /// any number of them (`int[][][]`), or the `ARRAY`/`ARRAY[N]` spelling.
    ///
    /// `PostgreSQL` ignores both the declared size and the declared *number* of
    /// dimensions: `int[4]`, `int[][]` and `integer ARRAY[4]` are all the one
    /// type `_int4`, and the real dimensionality lives in each value. Element
    /// types with no array type are refused with 0A000.
    fn parse_array_type_suffix(
        &mut self,
        base: crabka_pgtypes::ColumnType,
        type_word: &str,
        type_pos: usize,
    ) -> Result<crabka_pgtypes::ColumnType, ParseError> {
        // `ARRAY` / `ARRAY[N]` — the SQL-standard spelling of a one-`[]` suffix.
        let mut is_array = if *self.peek() == Token::Keyword(Keyword::Array) {
            self.bump();
            if *self.peek() == Token::LBracket {
                self.bump();
                if matches!(self.peek(), Token::IntLit(_)) {
                    self.bump();
                }
                self.expect(&Token::RBracket)?;
            }
            true
        } else {
            false
        };
        while *self.peek() == Token::LBracket {
            self.bump();
            // The declared dimension size is parsed and discarded, like PostgreSQL.
            if matches!(self.peek(), Token::IntLit(_)) {
                self.bump();
            }
            self.expect(&Token::RBracket)?;
            is_array = true;
        }
        if !is_array {
            return Ok(base);
        }
        crabka_pgtypes::ColumnType::array_of(base).ok_or_else(|| {
            ParseError::new_sqlstate(
                "0A000",
                format!("arrays of type \"{type_word}\" are not supported"),
                type_pos,
            )
        })
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

    /// Parse a `bit(n)` / `bit varying(n)` length modifier, and apply the
    /// grammar's default: a bare `bit` is `bit(1)`, while a bare `bit varying`
    /// stays unconstrained. That asymmetry is the SQL standard's, and it is why
    /// `'101'::bit` is `1` while `'101'::bit varying` is `101`.
    fn parse_bit_typmod(
        &mut self,
        ty: crabka_pgtypes::ColumnType,
        type_pos: usize,
    ) -> Result<crabka_pgtypes::ColumnType, ParseError> {
        let varying = matches!(ty, crabka_pgtypes::ColumnType::VarBit(_));
        if *self.peek() != Token::LParen {
            return Ok(if varying {
                crabka_pgtypes::ColumnType::VarBit(None)
            } else {
                crabka_pgtypes::ColumnType::Bit(Some(1))
            });
        }
        self.expect(&Token::LParen)?;
        let len = self.expect_i32("bit length")?;
        self.expect(&Token::RParen)?;
        crabka_pgtypes::bitstring::check_typmod(len, if varying { "varbit" } else { "bit" })
            .map_err(|error| {
                ParseError::new_sqlstate(error.sqlstate(), error.to_string(), type_pos)
            })?;
        Ok(if varying {
            crabka_pgtypes::ColumnType::VarBit(Some(len))
        } else {
            crabka_pgtypes::ColumnType::Bit(Some(len))
        })
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
    /// The same as [`Parser::expect_u16`] over the wider range a `bit(n)`
    /// length modifier occupies — `PostgreSQL` accepts up to 83,886,080 bits.
    fn expect_i32(&mut self, what: &str) -> Result<i32, ParseError> {
        let pos = self.peek_pos();
        match self.bump() {
            Token::IntLit(s) => s
                .parse::<i32>()
                .map_err(|_| ParseError::new(format!("invalid {what}"), pos)),
            other => Err(ParseError::new(
                format!("expected {what}, found {other:?}"),
                pos,
            )),
        }
    }

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
            // The tight-binding postfix operators (`::`, `COLLATE`, subscript,
            // `AT TIME ZONE`) are consumed first and without a `min_bp` gate.
            let (operand, extended) = self.tight_postfix(lhs)?;
            lhs = operand;
            if extended {
                continue;
            }
            // SP28: postfix predicates (IS [NOT] NULL, [NOT] IN, [NOT] BETWEEN,
            // [NOT] LIKE/ILIKE) bind at the comparison level (l_bp = 5). They are
            // handled before the binary-operator match so `a = 1 AND b IN (1,2)`
            // groups as `(a=1) AND (b IN (1,2))`.
            if 5 >= min_bp {
                use crate::ast::MatchKind;
                // Each of these words is ALSO a `bare_label_keyword`, so it is
                // only the operator when something that can continue it follows.
                // PostgreSQL's LALR grammar makes exactly this decision with one
                // token of lookahead, which is why `SELECT id is FROM w` names
                // the column `is` rather than reporting an unfinished predicate.
                match self.peek() {
                    Token::Keyword(Keyword::Is) if self.peek_continues_is_predicate() => {
                        lhs = self.parse_is_predicate(lhs)?;
                        continue;
                    }
                    Token::Keyword(Keyword::In) if *self.peek2() == Token::LParen => {
                        lhs = self.parse_in(lhs, false)?;
                        continue;
                    }
                    Token::Keyword(Keyword::Between) if Self::starts_expr(self.peek2()) => {
                        lhs = self.parse_between(lhs, false)?;
                        continue;
                    }
                    Token::Keyword(Keyword::Like) if Self::starts_expr(self.peek2()) => {
                        lhs = self.parse_like(lhs, false, MatchKind::Like)?;
                        continue;
                    }
                    Token::Keyword(Keyword::Ilike) if Self::starts_expr(self.peek2()) => {
                        lhs = self.parse_like(lhs, false, MatchKind::ILike)?;
                        continue;
                    }
                    // `x ISNULL` / `x NOTNULL` — PostgreSQL's postfix spellings of
                    // `IS NULL` / `IS NOT NULL`. Both words are
                    // type/function-name keywords there, so neither can be a
                    // column name or a bare label and this reading is never
                    // ambiguous.
                    Token::Ident(word)
                        if word.eq_ignore_ascii_case("isnull") && !self.peek_is_quoted_ident() =>
                    {
                        self.bump();
                        lhs = Expr::IsNull {
                            expr: Box::new(lhs),
                            negated: false,
                        };
                        continue;
                    }
                    Token::Ident(word)
                        if word.eq_ignore_ascii_case("notnull") && !self.peek_is_quoted_ident() =>
                    {
                        self.bump();
                        lhs = Expr::IsNull {
                            expr: Box::new(lhs),
                            negated: true,
                        };
                        continue;
                    }
                    // Infix `NOT` only when it leads a negated predicate
                    // (`x NOT IN/BETWEEN/LIKE/ILIKE/SIMILAR TO …`); otherwise `NOT`
                    // is the prefix operator handled in `prefix`. Two- (or, for
                    // `SIMILAR TO`, three-) token lookahead.
                    Token::Keyword(Keyword::Not)
                        if matches!(
                            self.peek2(),
                            Token::Keyword(
                                Keyword::In | Keyword::Between | Keyword::Like | Keyword::Ilike
                            )
                        ) || self.peek_is_similar_to(1) =>
                    {
                        self.bump(); // NOT
                        lhs = match self.peek() {
                            Token::Keyword(Keyword::In) => self.parse_in(lhs, true)?,
                            Token::Keyword(Keyword::Between) => self.parse_between(lhs, true)?,
                            Token::Keyword(Keyword::Like) => {
                                self.parse_like(lhs, true, MatchKind::Like)?
                            }
                            Token::Keyword(Keyword::Ilike) => {
                                self.parse_like(lhs, true, MatchKind::ILike)?
                            }
                            _ => self.parse_like(lhs, true, MatchKind::Similar)?,
                        };
                        continue;
                    }
                    _ if self.peek_is_similar_to(0) => {
                        lhs = self.parse_like(lhs, false, MatchKind::Similar)?;
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
            let Some((op, r_bp, op_pos)) = self.take_infix_operator(min_bp)? else {
                break;
            };
            // SP34: `op ANY|SOME|ALL ( SELECT … )` — any representable
            // operator is syntactically valid here. Type analysis is responsible
            // for requiring a boolean result, just as PostgreSQL does.
            if matches!(
                self.peek(),
                Token::Keyword(Keyword::Any | Keyword::Some | Keyword::All)
            ) {
                let all = matches!(self.peek(), Token::Keyword(Keyword::All));
                self.bump(); // ANY / SOME / ALL
                self.expect(&Token::LParen)?;
                // Two shapes share this syntax. `SELECT`/`VALUES`/`WITH`/`(`
                // after the paren is the subquery form; anything else is the
                // ARRAY form (`= ANY($1)`, `= ANY(ARRAY[…])`, `= ANY(tags)`),
                // which every driver emits when it binds an IN-list as one
                // parameter.
                // A `(` here is the subquery form only when a query keyword
                // follows it; otherwise it is an ordinary parenthesised array
                // expression, as in `= ANY((a)[2:4])`.
                lhs = if matches!(
                    self.peek(),
                    Token::Keyword(Keyword::Select | Keyword::Values | Keyword::With)
                ) || (*self.peek() == Token::LParen
                    && matches!(
                        self.peek2(),
                        Token::Keyword(Keyword::Select | Keyword::Values | Keyword::With)
                    )) {
                    Expr::Quantified {
                        expr: Box::new(lhs),
                        op,
                        all,
                        subquery: Box::new(self.query_expr_after_open_paren()?),
                    }
                } else {
                    let array = Box::new(self.expr(0)?);
                    self.expect(&Token::RParen)?;
                    Expr::QuantifiedArray {
                        expr: Box::new(lhs),
                        op,
                        all,
                        array,
                    }
                };
                continue;
            }
            let rhs = self.expr(r_bp)?;
            check_row_arity(&lhs, &rhs, op_pos)?;
            lhs = Expr::Binary {
                op,
                left: Box::new(lhs),
                right: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    /// Consume one infix operator whose left binding power reaches `min_bp`.
    /// Wrapper decoding stays out of [`Parser::expr`]'s recursive frame so the
    /// parser's near-limit stack-safety guarantee is unchanged.
    fn take_infix_operator(
        &mut self,
        min_bp: u8,
    ) -> Result<Option<(BinaryOp, u8, usize)>, ParseError> {
        let explicit = self.explicit_operator_starts();
        if explicit && 7 < min_bp {
            return Ok(None);
        }
        let explicit_token = if explicit {
            Some(self.explicit_operator_token()?)
        } else {
            None
        };
        let (token, position) = match explicit_token.as_ref() {
            Some((token, position)) => (token, *position),
            None => (self.peek(), self.peek_pos()),
        };
        let (op, l_bp, r_bp) = match token {
            // `AND` and `OR` are bare-label keywords too, so they are operators
            // only when an operand follows.
            Token::Keyword(Keyword::Or) if !explicit && Self::starts_expr(self.peek2()) => {
                (BinaryOp::Or, 1, 2)
            }
            Token::Keyword(Keyword::And) if !explicit && Self::starts_expr(self.peek2()) => {
                (BinaryOp::And, 3, 4)
            }
            Token::Eq => (BinaryOp::Eq, 5, 6),
            Token::Ne => (BinaryOp::Ne, 5, 6),
            Token::Lt => (BinaryOp::Lt, 5, 6),
            Token::Le => (BinaryOp::Le, 5, 6),
            Token::Gt => (BinaryOp::Gt, 5, 6),
            Token::Ge => (BinaryOp::Ge, 5, 6),
            Token::Concat => (BinaryOp::Concat, 7, 8),
            Token::JsonGet => (BinaryOp::JsonGet, 7, 8),
            Token::JsonGetText => (BinaryOp::JsonGetText, 7, 8),
            Token::JsonGetPath => (BinaryOp::JsonGetPath, 7, 8),
            Token::JsonGetPathText => (BinaryOp::JsonGetPathText, 7, 8),
            Token::Contains => (BinaryOp::Contains, 7, 8),
            Token::ContainedBy => (BinaryOp::ContainedBy, 7, 8),
            Token::KeyExists => (BinaryOp::KeyExists, 7, 8),
            Token::KeyExistsAny => (BinaryOp::KeyExistsAny, 7, 8),
            Token::KeyExistsAll => (BinaryOp::KeyExistsAll, 7, 8),
            Token::JsonPathExists => (BinaryOp::JsonPathExists, 7, 8),
            Token::JsonPathMatch => (BinaryOp::JsonPathMatch, 7, 8),
            Token::Overlaps => (BinaryOp::Overlaps, 7, 8),
            Token::Same => (BinaryOp::Same, 7, 8),
            Token::DoesNotExtendAbove => (BinaryOp::DoesNotExtendAbove, 7, 8),
            Token::DoesNotExtendBelow => (BinaryOp::DoesNotExtendBelow, 7, 8),
            Token::StrictlyBelow => (BinaryOp::StrictlyBelow, 7, 8),
            Token::StrictlyAbove => (BinaryOp::StrictlyAbove, 7, 8),
            Token::DoesNotExtendRight => (BinaryOp::DoesNotExtendRight, 7, 8),
            Token::DoesNotExtendLeft => (BinaryOp::DoesNotExtendLeft, 7, 8),
            Token::Adjacent => (BinaryOp::Adjacent, 7, 8),
            Token::Phrase => (BinaryOp::Phrase, 7, 8),
            Token::Tilde => (BinaryOp::Match, 7, 8),
            Token::TildeCi => (BinaryOp::MatchCi, 7, 8),
            Token::NotTilde => (BinaryOp::NotMatch, 7, 8),
            Token::NotTildeCi => (BinaryOp::NotMatchCi, 7, 8),
            Token::Amp => (BinaryOp::BitAnd, 7, 8),
            Token::Pipe => (BinaryOp::BitOr, 7, 8),
            Token::Hash => (BinaryOp::BitXor, 7, 8),
            Token::Shl => (BinaryOp::Shl, 7, 8),
            Token::Shr => (BinaryOp::Shr, 7, 8),
            Token::ContainedByOrEq => (BinaryOp::ContainedByOrEq, 7, 8),
            Token::ContainsOrEq => (BinaryOp::ContainsOrEq, 7, 8),
            Token::Plus => (BinaryOp::Add, 9, 10),
            Token::Minus => (BinaryOp::Sub, 9, 10),
            Token::Star => (BinaryOp::Mul, 11, 12),
            Token::Slash => (BinaryOp::Div, 11, 12),
            Token::Percent => (BinaryOp::Mod, 11, 12),
            Token::Caret => (BinaryOp::Pow, 13, 14),
            _ if explicit => return Err(ParseError::new("expected operator name", position)),
            _ => return Ok(None),
        };
        let (l_bp, r_bp) = if explicit { (7, 8) } else { (l_bp, r_bp) };
        if l_bp < min_bp {
            return Ok(None);
        }
        if !explicit {
            self.bump();
        }
        Ok(Some((op, r_bp, position)))
    }

    fn explicit_operator_starts(&self) -> bool {
        self.peek_ident_eq("operator")
            && !self.peek_is_quoted_ident()
            && *self.peek2() == Token::LParen
    }

    /// Parse `OPERATOR([schema.]symbol)` in prefix or infix position. Gres has
    /// no user-defined operators, so only an omitted schema or `pg_catalog` can
    /// name one of the built-ins represented by [`BinaryOp`] / [`UnaryOp`].
    fn explicit_operator_token(&mut self) -> Result<(Token, usize), ParseError> {
        self.bump(); // OPERATOR
        self.expect(&Token::LParen)?;
        if let Some(schema) = self.peek_col_id()
            && *self.peek2() == Token::Dot
        {
            let schema_pos = self.peek_pos();
            self.bump();
            self.bump(); // dot
            if schema != "pg_catalog" {
                return Err(ParseError::new_sqlstate(
                    "0A000",
                    format!("operator schema \"{schema}\" is not supported"),
                    schema_pos,
                ));
            }
            if self.peek_col_id().is_some() && *self.peek2() == Token::Dot {
                return Err(ParseError::new(
                    "multi-part operator qualification is not supported",
                    self.peek_pos(),
                ));
            }
        }
        let position = self.peek_pos();
        if *self.peek() == Token::RParen {
            return Err(ParseError::new("expected operator name", position));
        }
        let token = self.bump();
        if *self.peek() == Token::Dot {
            return Err(ParseError::new(
                "multi-part operator qualification is not supported",
                self.peek_pos(),
            ));
        }
        self.expect(&Token::RParen)?;
        Ok((token, position))
    }

    fn explicit_prefix_operator(&mut self) -> Result<Expr, ParseError> {
        let (token, position) = self.explicit_operator_token()?;
        let op = match token {
            Token::Minus => UnaryOp::Neg,
            Token::Plus => UnaryOp::Plus,
            Token::Tilde => UnaryOp::BitNot,
            Token::At => UnaryOp::Abs,
            Token::SquareRoot => UnaryOp::Sqrt,
            Token::CubeRoot => UnaryOp::Cbrt,
            Token::TsNot => UnaryOp::TsNot,
            _ => return Err(ParseError::new("expected prefix operator", position)),
        };
        Ok(Expr::Unary {
            op,
            // OPERATOR(...) has PostgreSQL's generic-operator precedence,
            // even when the wrapped spelling is `+` or `-`.
            expr: Box::new(self.expr(8)?),
        })
    }

    /// Consume one tight-binding postfix operator — `::`, `COLLATE`, an array
    /// subscript chain, or `AT TIME ZONE` — if the next tokens spell one.
    ///
    /// The operators are `::`, `COLLATE`, an array subscript chain, and
    /// `AT TIME ZONE`. All four bind tighter than every binary operator, so none
    /// takes a `min_bp` gate. The flag reports whether the parser consumed
    /// anything. When it is false, the function returns the operand untouched.
    /// `PostgreSQL`'s `opt_indirection` after a parenthesised expression:
    /// `(expr).field` selects one attribute of a composite value, and the chain
    /// may repeat (`(expr).a.b`). This production is reachable ONLY after a
    /// closing parenthesis, which is exactly why `a.b` elsewhere stays a
    /// table-qualified column reference.
    fn field_selection(&mut self, mut base: Expr) -> Result<Expr, ParseError> {
        while *self.peek() == Token::Dot {
            self.bump();
            if *self.peek() == Token::Star {
                self.bump();
                base = Expr::FieldSelectAll(Box::new(base));
                continue;
            }
            base = Expr::FieldSelect {
                base: Box::new(base),
                field: self.expect_ident()?,
            };
        }
        Ok(base)
    }

    fn tight_postfix(&mut self, lhs: Expr) -> Result<(Expr, bool), ParseError> {
        let mut lhs = lhs;
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
            return Ok((lhs, true));
        }
        // `COLLATE` binds as tightly as `::` in PostgreSQL's precedence
        // table, so it is likewise consumed with no `min_bp` gate and
        // `a COLLATE "C" = b` groups as `(a COLLATE "C") = b`.
        // `COLLATE` is a `bare_label_keyword` too, so — like the infix
        // operator keywords below — it is only the operator when a name that
        // could be a collation follows: `SELECT 1 collate` labels the column.
        if self.peek_ident_eq("collate") && matches!(self.peek2(), Token::Ident(_)) {
            self.bump();
            lhs = Expr::Collate {
                expr: Box::new(lhs),
                collation: self.expect_collation_name()?,
            };
            return Ok((lhs, true));
        }
        // Array subscripting binds as tightly as `::` and is likewise
        // consumed with no `min_bp` gate, so `a[1] + b` is `(a[1]) + b`.
        // The whole `[…][…]…` chain is taken at once: `PostgreSQL` treats it
        // as ONE array reference, so `a[2][3]` reaches into a
        // two-dimensional array rather than subscripting `a[2]`.
        if *self.peek() == Token::LBracket {
            let subscripts = self.subscript_chain()?;
            lhs = match <[_; 1]>::try_from(subscripts) {
                // A lone plain subscript keeps the simpler node, which is
                // also the jsonb subscripting form.
                Ok([ArraySubscript::Index(index)]) => Expr::Subscript {
                    base: Box::new(lhs),
                    index: Box::new(index),
                },
                Ok([one]) => Expr::ArrayRef {
                    base: Box::new(lhs),
                    subscripts: vec![one],
                },
                Err(many) => Expr::ArrayRef {
                    base: Box::new(lhs),
                    subscripts: many,
                },
            };
            return Ok((lhs, true));
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
                filter: None,
            });
            return Ok((lhs, true));
        }
        Ok((lhs, false))
    }

    /// The collation named after `COLLATE`. `PostgreSQL` writes it as `any_name`
    /// as a possibly schema-qualified identifier, and reports `42704` when no
    /// such collation exists. This engine's `pg_collation` holds exactly
    /// `default`, `C` and `POSIX`, all of which order text by byte value. The
    /// parser accepts those three, where they are semantically a no-op, and
    /// every other name gets `PostgreSQL`'s own undefined-object error.
    fn expect_collation_name(&mut self) -> Result<String, ParseError> {
        let pos = self.peek_pos();
        let mut name = self.expect_col_id()?;
        // `pg_catalog."C"` names the same collation as a bare `"C"`.
        while *self.peek() == Token::Dot {
            self.bump();
            name = self.expect_col_label()?;
        }
        if !matches!(name.as_str(), "default" | "C" | "POSIX") {
            return Err(ParseError::new_sqlstate(
                "42704",
                format!("collation \"{name}\" for encoding \"UTF8\" does not exist"),
                pos,
            ));
        }
        Ok(name)
    }

    /// Can `tok` begin an expression? Exactly the set [`Parser::prefix`] accepts,
    /// used as the one-token lookahead that tells an infix operator keyword from
    /// the same word used as a no-`AS` column label.
    fn starts_expr(tok: &Token) -> bool {
        matches!(
            tok,
            Token::Ident(_)
                | Token::IntLit(_)
                | Token::FloatLit(_)
                | Token::StringLit(_)
                | Token::Param(_)
                | Token::LParen
                | Token::Minus
                | Token::Plus
                | Token::Tilde
                | Token::At
                | Token::SquareRoot
                | Token::CubeRoot
                | Token::TsNot
                | Token::Keyword(
                    Keyword::Not
                        | Keyword::Exists
                        | Keyword::Array
                        | Keyword::True
                        | Keyword::False
                        | Keyword::Null
                        | Keyword::Case
                        | Keyword::Cast
                        | Keyword::CurrentUser
                        | Keyword::Left
                        | Keyword::Right
                )
        )
    }

    /// Does an `IS` at the cursor continue into a predicate? The words that may
    /// follow it are a closed set ([`Parser::parse_is_predicate`]), so anything
    /// else leaves `IS` free to be a column label.
    fn peek_continues_is_predicate(&self) -> bool {
        match self.peek2() {
            Token::Keyword(
                Keyword::Not | Keyword::Null | Keyword::Distinct | Keyword::True | Keyword::False,
            ) => true,
            Token::Ident(word) => {
                word.eq_ignore_ascii_case("unknown")
                    || word.eq_ignore_ascii_case("json")
                    || word.eq_ignore_ascii_case("document")
            }
            _ => false,
        }
    }

    fn prefix(&mut self) -> Result<Expr, ParseError> {
        if self.explicit_operator_starts() {
            return self.explicit_prefix_operator();
        }
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
                // Unary minus is the tightest-binding operator below `::`/`[]`:
                // tighter than `^` (13/14) and so than `* / %` and everything
                // looser, hence an operand power of 15. That is PostgreSQL's
                // ordering, and it is why `-2^2` is `(-2)^2` = 4.
                Ok(Expr::Unary {
                    op: UnaryOp::Neg,
                    expr: Box::new(self.expr(15)?),
                })
            }
            // Unary plus binds exactly as tightly as unary minus, but it is a
            // plain operator: PostgreSQL's `doNegate` folds a `-` into the
            // literal it precedes and there is no `doPositive` counterpart, so
            // `+1` never becomes an output-position constant.
            Token::Plus => {
                self.bump();
                Ok(Expr::Unary {
                    op: UnaryOp::Plus,
                    expr: Box::new(self.expr(15)?),
                })
            }
            // The generic PREFIX operators. Unlike unary minus these bind
            // LOOSELY — PostgreSQL gives them the "any other operator" level, so
            // their operand is parsed at 8 and `~ 5 + 1` is `~(5 + 1)` = -7 while
            // `~ 5 & 3` is `(~5) & 3` = 2. `~` here is bitwise NOT; the same
            // token in infix position is the regex-match operator.
            Token::Tilde | Token::At | Token::SquareRoot | Token::CubeRoot => {
                let op = match self.bump() {
                    Token::Tilde => UnaryOp::BitNot,
                    Token::At => UnaryOp::Abs,
                    Token::SquareRoot => UnaryOp::Sqrt,
                    _ => UnaryOp::Cbrt,
                };
                Ok(Expr::Unary {
                    op,
                    expr: Box::new(self.expr(8)?),
                })
            }
            Token::TsNot => {
                self.bump();
                Ok(Expr::Unary {
                    op: UnaryOp::TsNot,
                    expr: Box::new(self.expr(8)?),
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
                    let first = self.expr(0)?;
                    // `(a, b, …)` is the bare row-constructor spelling; a single
                    // parenthesised expression stays plain grouping.
                    if *self.peek() == Token::Comma {
                        let mut elements = vec![first];
                        while self.eat_comma() {
                            elements.push(self.expr(0)?);
                        }
                        self.expect(&Token::RParen)?;
                        return self.field_selection(Expr::Row(elements));
                    }
                    self.expect(&Token::RParen)?;
                    self.field_selection(first)
                }
            }
            Token::Keyword(Keyword::Exists) => {
                self.bump(); // EXISTS
                self.expect(&Token::LParen)?;
                let sub = self.query_expr_after_open_paren()?;
                Ok(Expr::Exists(Box::new(sub)))
            }
            Token::Keyword(Keyword::Array) => self.array_literal(),
            Token::IntLit(s) => {
                self.bump();
                if s.parse::<i64>().is_ok() {
                    Ok(Expr::IntLiteral(s))
                } else {
                    // PostgreSQL promotes an integer token through int4 and
                    // int8, then to arbitrary-precision numeric when it no
                    // longer fits either integer type.
                    Ok(Expr::NumericLiteral(s))
                }
            }
            Token::FloatLit(s) => {
                self.bump();
                Ok(Expr::NumericLiteral(s))
            }
            Token::StringLit(s) => {
                self.bump();
                Ok(Expr::StringLiteral(s))
            }
            // `B'…'` / `X'…'`. PostgreSQL types these `bit` with no length
            // modifier and runs `bit_in` while parsing, so a bad digit is a
            // syntax-time error pointing at the literal — which is what makes
            // `SELECT b' 0'` report `" " is not a valid binary digit` under a
            // caret rather than failing at execution. Decoding here also means
            // the rest of the parser sees an ordinary typed cast.
            Token::BitStringLit(raw) => {
                let pos = self.peek_pos();
                self.bump();
                let bits =
                    crabka_pgtypes::bitstring::BitString::parse(&raw, false).map_err(|error| {
                        ParseError::new_sqlstate(error.sqlstate(), error.to_string(), pos)
                    })?;
                Ok(Expr::BitStringLiteral(bits.to_text()))
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
                    filter: None,
                }))
            }
            // `left`/`right` are PostgreSQL scalar functions AND (LEFT/RIGHT) join
            // keywords. In expression position they are valid only as a function
            // call — `left(s, n)` / `right(s, n)` — so route them to `func_call`.
            Token::Keyword(Keyword::Left) => self.keyword_func_call("left"),
            Token::Keyword(Keyword::Right) => self.keyword_func_call("right"),
            Token::Keyword(_) if self.peek_col_id().is_some() => {
                let name = self
                    .peek_col_id()
                    .expect("guard guarantees a PostgreSQL column identifier");
                self.bump();
                self.col_id_expr(name)
            }
            Token::Param(n) => {
                self.bump();
                Ok(Expr::Param(n))
            }
            // `DEFAULT` is reserved in PostgreSQL and its grammar admits it
            // anywhere an `a_expr` may go, leaving parse analysis to refuse every
            // context but an INSERT value and an UPDATE assignment. Those two
            // positions are claimed by `insert_value_expr` before an expression is
            // parsed at all, so reaching `DEFAULT` here IS one of the refused
            // contexts — and refusing it during analysis, rather than when a row
            // happens to evaluate it, is what makes an empty table report it too.
            Token::Ident(s)
                if s.eq_ignore_ascii_case("default") && !self.peek_is_quoted_ident() =>
            {
                Err(ParseError::new_sqlstate(
                    "42601",
                    "DEFAULT is not allowed in this context",
                    self.peek_pos(),
                ))
            }
            Token::Ident(s) => {
                let lower = s.to_ascii_lowercase();
                // `ROW(a, b, …)` — the explicit row constructor. `ROW` is
                // reserved in PostgreSQL, so it never resolves to a function
                // call here; `row` used as a plain column name still does,
                // because only `row (` takes this path.
                if lower == "row" && *self.peek2() == Token::LParen {
                    self.bump(); // ROW
                    return self.row_constructor();
                }
                // `typename 'string'` — PostgreSQL's typed-constant syntax, which
                // means exactly `'string'::typename`. Checked BEFORE the
                // function-call and column paths so `int4 '0'` is a constant while
                // `date('…')` stays a function call and `foo 'x'` — where `foo` is
                // not a type — still reports PostgreSQL's own diagnosis.
                if let Some(literal) = self.typed_literal()? {
                    return Ok(literal);
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
                        filter: None,
                    }));
                }
                // SP37: `EXTRACT(field FROM source)` — a special call form that
                // lowers onto `extract('<field>', source)` (field lowercased to a
                // string literal). Checked before the generic comma-arg `func_call`
                // so the `FROM` keyword inside the parens is not mis-parsed.
                if lower == "extract" && *self.peek() == Token::LParen {
                    return self.extract_expr();
                }
                // The other SQL-standard call forms that spell their arguments
                // with keywords instead of commas. Each lowers onto the ordinary
                // function of the same name, so only the grammar is special.
                if *self.peek() == Token::LParen
                    && let Some(expr) = self.keyword_arg_call(&lower)?
                {
                    return Ok(expr);
                }
                // The SQL/JSON standard constructors and query functions. Each
                // has its own grammar inside the parentheses (`VALUE`, `ON NULL`,
                // `PASSING`, `RETURNING`, …), so none of them can reach the
                // generic comma-separated `func_call`.
                if *self.peek() == Token::LParen
                    && let Some(expr) = self.sql_json_expr(&lower)?
                {
                    return Ok(expr);
                }
                // SP27: `ident (` is a function call; a bare ident is a column.
                // SP33/F-2: `ident . ident` is either a schema-qualified function
                // call (`pg_catalog.format_type(...)`) or a table-qualified column.
                self.col_id_expr(s)
            }
            other => Err(ParseError::new(
                format!("unexpected token {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    /// A whole `[…][…]…` subscript chain, positioned at the first `[`.
    ///
    /// Each entry is an index or a slice; a slice bound may be omitted on either
    /// side (`a[:2]`, `a[2:]`), and `a[:]`, with both bounds omitted, is a whole-dimension
    /// slice, exactly as in `PostgreSQL`.
    fn subscript_chain(&mut self) -> Result<Vec<ArraySubscript>, ParseError> {
        let mut subscripts = Vec::new();
        while *self.peek() == Token::LBracket {
            self.bump();
            let lower = if *self.peek() == Token::Colon {
                None
            } else {
                Some(self.expr(0)?)
            };
            if *self.peek() == Token::Colon {
                self.bump();
                let upper = if *self.peek() == Token::RBracket {
                    None
                } else {
                    Some(self.expr(0)?)
                };
                subscripts.push(ArraySubscript::Slice { lower, upper });
            } else {
                let index = lower.ok_or_else(|| {
                    ParseError::new("expected a subscript expression", self.peek_pos())
                })?;
                subscripts.push(ArraySubscript::Index(index));
            }
            self.expect(&Token::RBracket)?;
            if subscripts.len() > crabka_pgtypes::MAX_ARRAY_DIM {
                return Err(ParseError::new_sqlstate(
                    "54000",
                    format!(
                        "number of array dimensions ({}) exceeds the maximum allowed ({})",
                        subscripts.len(),
                        crabka_pgtypes::MAX_ARRAY_DIM
                    ),
                    self.peek_pos(),
                ));
            }
        }
        Ok(subscripts)
    }

    /// `ARRAY[e1, e2, …]` or `ARRAY(subquery)`, positioned at the `ARRAY`
    /// keyword. The element list may be empty, and an element may itself be a
    /// braceless nested constructor (`ARRAY[[1,2],[3,4]]`), which adds a
    /// dimension exactly as a spelled-out `ARRAY[ARRAY[1,2],ARRAY[3,4]]` does.
    fn array_literal(&mut self) -> Result<Expr, ParseError> {
        self.expect(&Token::Keyword(Keyword::Array))?;
        if *self.peek() == Token::LParen {
            self.bump();
            let query = self.query_expr_after_open_paren()?;
            return Ok(Expr::ArraySubquery(Box::new(query)));
        }
        self.array_constructor_body()
    }

    /// The `[e1, e2, …]` part of an array constructor, positioned at the `[`.
    fn array_constructor_body(&mut self) -> Result<Expr, ParseError> {
        self.expect(&Token::LBracket)?;
        let mut elements = Vec::new();
        if *self.peek() != Token::RBracket {
            loop {
                // A nested `[…]` in element position is a sub-array, not a
                // subscript — there is no base expression for it to apply to.
                if *self.peek() == Token::LBracket {
                    elements.push(self.array_constructor_body()?);
                } else {
                    elements.push(self.expr(0)?);
                }
                if self.eat_comma() {
                    continue;
                }
                break;
            }
        }
        self.expect(&Token::RBracket)?;
        Ok(Expr::ArrayLiteral(elements))
    }

    /// `ROW(e1, e2, …)`, positioned at `(`, after the `ROW` word. The element
    /// list may be empty, and a single element is still a row (unlike the bare
    /// parenthesised form, where `(x)` is grouping).
    fn row_constructor(&mut self) -> Result<Expr, ParseError> {
        self.expect(&Token::LParen)?;
        let mut elements = Vec::new();
        if *self.peek() != Token::RParen {
            loop {
                elements.push(self.expr(0)?);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
        }
        self.expect(&Token::RParen)?;
        Ok(Expr::Row(elements))
    }

    /// `typename 'string'`: `PostgreSQL`'s constant-of-a-given-type syntax
    /// (`bool 't'`, `int4 '0'`, `numeric '1.5'`, `timestamp with time zone '…'`).
    /// It is defined to mean exactly `'string'::typename`, so it lowers onto the
    /// same [`Expr::Cast`], errors included.
    ///
    /// Returns `Ok(None)`, with the token cursor restored, when the tokens ahead
    /// are not a type name followed by a string literal; the caller then takes
    /// the ordinary column / function-call path. A *bare* name that is not a type
    /// but is directly followed by a string literal can only have been meant as
    /// this syntax, so it reports `PostgreSQL`'s own 42704 and does not degrade
    /// into a generic syntax error.
    fn typed_literal(&mut self) -> Result<Option<Expr>, ParseError> {
        let start = self.pos;
        let Ok(ty) = self.parse_type_name() else {
            self.pos = start;
            if let (Token::Ident(name), Token::StringLit(_)) = (self.peek(), self.peek2()) {
                return Err(ParseError::new_sqlstate(
                    "42704",
                    format!("type \"{name}\" does not exist"),
                    self.peek_pos(),
                ));
            }
            return Ok(None);
        };
        if !matches!(self.peek(), Token::StringLit(_)) {
            self.pos = start;
            return Ok(None);
        }
        let Token::StringLit(string) = self.bump() else {
            unreachable!("the peek above guaranteed a string literal");
        };
        if ty == crabka_pgtypes::ColumnType::Interval {
            return self.interval_literal(string).map(Some);
        }
        // `ConstBit` clears the length modifier that `Bit` supplies, so the
        // typed-constant `bit 'xff'` is eight bits while the cast `'xff'::bit`
        // is `bit(1)` and keeps only the first. A modifier the query WROTE
        // survives, so `bit(1) 'xff'` is still one bit — which is why this
        // looks for the parenthesis rather than for the value 1.
        let wrote_modifier = self.toks[start..self.pos]
            .iter()
            .any(|(token, _)| *token == Token::LParen);
        let ty = match ty {
            crabka_pgtypes::ColumnType::Bit(_) if !wrote_modifier => {
                crabka_pgtypes::ColumnType::Bit(None)
            }
            other => other,
        };
        Ok(Some(Expr::Cast {
            expr: Box::new(Expr::StringLiteral(string)),
            ty,
        }))
    }

    /// The tail of `INTERVAL 'string' [field [TO field]]`, positioned just after
    /// the string.
    ///
    /// A field qualifier does two things in `PostgreSQL`: it supplies the unit an
    /// unadorned quantity in the string is measured in (`interval '90' minute` is
    /// 90 minutes), and it truncates the result to the range's last field
    /// (`interval '1.5' day` is `1 day`). Both are properties of the *literal*, so
    /// the qualified form is decoded here, against the field range, and lowered to
    /// the plain interval literal it denotes.
    fn interval_literal(&mut self, string: String) -> Result<Expr, ParseError> {
        use crabka_pgtypes::datetime::IntervalField;

        let interval = |text: String| Expr::Cast {
            expr: Box::new(Expr::StringLiteral(text)),
            ty: crabka_pgtypes::ColumnType::Interval,
        };
        let field_pos = self.peek_pos();
        let Some(start) = self.interval_field() else {
            return Ok(interval(string));
        };
        self.bump(); // the field word
        let end = if *self.peek() == Token::Keyword(Keyword::To) {
            self.bump();
            let end = self
                .interval_field()
                .ok_or_else(|| ParseError::new("expected an interval field", self.peek_pos()))?;
            self.bump();
            end
        } else {
            start
        };
        let range = IntervalField::parse(start)
            .zip(IntervalField::parse(end))
            .ok_or_else(|| ParseError::new("expected an interval field", field_pos))?;
        let value = crabka_pgtypes::datetime::parse_interval_ranged(&string, Some(range))
            .map_err(|e| ParseError::new_sqlstate(e.sqlstate(), e.to_string(), field_pos))?;
        Ok(interval(crabka_pgtypes::datetime::interval_to_text(value)))
    }

    /// The single-word interval field qualifier at the cursor, if any. Keyword-
    /// free (the words are ordinary identifiers to this lexer), and singular
    /// only, as in `PostgreSQL`'s grammar.
    fn interval_field(&self) -> Option<&'static str> {
        let Token::Ident(word) = self.peek() else {
            return None;
        };
        ["year", "month", "day", "hour", "minute", "second"]
            .into_iter()
            .find(|field| word.eq_ignore_ascii_case(field))
    }

    /// Parse a function call after its name `ident`, positioned at `(`.
    /// `f(*)` yields [`FuncArgs::Star`]; `DISTINCT`/`ALL` may lead the argument
    /// list; otherwise a (possibly empty) comma-separated expression list.
    ///
    /// A trailing `FILTER (WHERE …)` and/or `OVER …` turns the call into a
    /// window call: it is recorded on the enclosing `SELECT` and the expression
    /// becomes a [`crate::ast::window_placeholder`].
    fn func_call(&mut self, name: String) -> Result<Expr, ParseError> {
        use crate::ast::{FuncArgs, FuncCall};
        self.expect(&Token::LParen)?;
        // `f(*)` — the star form (no DISTINCT, no other args).
        let (distinct, args, ordered) = if *self.peek() == Token::Star {
            self.bump();
            self.expect(&Token::RParen)?;
            (false, FuncArgs::Star, false)
        } else {
            let distinct = if self.eat_keyword(Keyword::Distinct) {
                true
            } else {
                // ALL is the default modifier; accept and ignore it.
                self.eat_keyword(Keyword::All);
                false
            };
            let mut args = Vec::new();
            let mut named: Vec<(String, Expr)> = Vec::new();
            if *self.peek() != Token::RParen {
                loop {
                    // `name := value` — a labeled argument. `ident : =` cannot begin
                    // an expression, so recognizing it here cannot change how any
                    // statement that parses today is read.
                    if let (Token::Ident(label), Token::Colon, Token::Eq) = (
                        self.peek().clone(),
                        self.peek2().clone(),
                        self.peek3().clone(),
                    ) {
                        self.bump();
                        self.bump();
                        self.bump();
                        named.push((label.to_ascii_lowercase(), self.expr(0)?));
                    } else {
                        args.push(self.expr(0)?);
                    }
                    if self.eat_comma() {
                        continue;
                    }
                    break;
                }
            }
            if !named.is_empty() {
                // PostgreSQL requires every positional argument to precede the
                // labeled ones.
                args.extend(Self::positional_from_named(
                    &name,
                    &args,
                    named,
                    self.peek_pos(),
                )?);
            }
            let ordered = self.eat_aggregate_order_by()?;
            self.expect(&Token::RParen)?;
            (distinct, FuncArgs::Exprs(args), ordered)
        };
        let filter = self.opt_filter_clause()?;
        let over = self.opt_over_clause()?;
        if ordered {
            // `PostgreSQL` refuses the windowed spelling itself, with this
            // SQLSTATE and this message. The plain spelling it executes; this
            // engine's aggregate path cannot order the values it accumulates, so
            // that one is refused here too rather than silently ignoring the sort.
            let message = if over.is_some() {
                "aggregate ORDER BY is not implemented for window functions"
            } else {
                "aggregate ORDER BY is not supported"
            };
            return Err(ParseError::new_sqlstate("0A000", message, self.peek_pos()));
        }
        let Some(over) = over else {
            // `FILTER` without `OVER` is the plain aggregate spelling; the call
            // carries the predicate and the aggregate path applies it per row.
            // A non-aggregate call cannot have one, which the executor rejects
            // once it knows whether the name is an aggregate.
            return Ok(Expr::Func(FuncCall {
                name,
                distinct,
                args,
                filter: filter.map(Box::new),
            }));
        };
        self.push_window_call(crate::ast::WindowCall {
            name,
            distinct,
            args,
            filter,
            over,
        })
    }

    /// `ORDER BY <sort item> [, …]` inside an aggregate's argument list, which
    /// orders the values fed to the aggregate. The parser consumes and discards
    /// it, because the caller refuses the call. So a malformed sort list is
    /// still a syntax error at the right place.
    fn eat_aggregate_order_by(&mut self) -> Result<bool, ParseError> {
        if !self.eat_keyword(Keyword::Order) {
            return Ok(false);
        }
        self.expect(&Token::Keyword(Keyword::By))?;
        loop {
            self.expr(0)?;
            if self.eat_keyword(Keyword::Using) {
                self.expr(0)?;
            } else if !self.eat_keyword(Keyword::Desc) {
                self.eat_keyword(Keyword::Asc);
            }
            if self.eat_ident_eq("nulls") && !self.eat_ident_eq("first") {
                self.expect_ident_eq("last")?;
            }
            if self.eat_comma() {
                continue;
            }
            break;
        }
        Ok(true)
    }

    /// Record a window call against the `SELECT` currently being parsed and
    /// return the placeholder that stands in for it.
    fn push_window_call(&mut self, call: crate::ast::WindowCall) -> Result<Expr, ParseError> {
        let pos = self.peek_pos();
        if self.window_spec_depth > 0 {
            // A window definition partitions and orders the rows the window node
            // runs over, so it is evaluated strictly below every window call.
            return Err(ParseError::new_sqlstate(
                "42P20",
                "window functions are not allowed in window definitions",
                pos,
            ));
        }
        let Some(calls) = self.window_calls.last_mut() else {
            // No enclosing SELECT: a window call in VALUES, an UPDATE SET, or a
            // constraint expression. PostgreSQL reports 42P20 for each of these.
            return Err(ParseError::new_sqlstate(
                "42P20",
                format!(
                    "window functions are not allowed in this context (\"{}\")",
                    call.name
                ),
                pos,
            ));
        };
        let index = calls.len();
        let label = call.name.clone();
        calls.push(call);
        Ok(crate::ast::window_placeholder(index, &label))
    }

    /// `FILTER ( WHERE <predicate> )` after a function call's argument list.
    /// `FILTER` is recognized unconditionally here: `PostgreSQL` treats it as a
    /// keyword in this position too, so `count(*) filter FROM t` is a syntax
    /// error there rather than an aliased aggregate.
    fn opt_filter_clause(&mut self) -> Result<Option<Expr>, ParseError> {
        if !self.eat_ident_eq("filter") {
            return Ok(None);
        }
        self.expect(&Token::LParen)?;
        self.expect(&Token::Keyword(Keyword::Where))?;
        let predicate = self.expr(0)?;
        self.expect(&Token::RParen)?;
        Ok(Some(predicate))
    }

    /// `OVER window_name` / `OVER ( window_specification )`.
    fn opt_over_clause(&mut self) -> Result<Option<crate::ast::WindowRef>, ParseError> {
        if !self.eat_ident_eq("over") {
            return Ok(None);
        }
        if *self.peek() == Token::LParen {
            return Ok(Some(crate::ast::WindowRef::Spec(Box::new(
                self.window_spec()?,
            ))));
        }
        Ok(Some(crate::ast::WindowRef::Named(self.expect_ident()?)))
    }

    /// `( [existing_window_name] [PARTITION BY …] [ORDER BY …] [frame_clause] )`,
    /// positioned at the opening parenthesis.
    ///
    /// Every word here (`partition`, `range`, `rows`, `groups`, …) is matched as
    /// a soft identifier, so none of them becomes reserved.
    fn window_spec(&mut self) -> Result<crate::ast::WindowSpec, ParseError> {
        self.window_spec_depth += 1;
        let spec = self.window_spec_body();
        self.window_spec_depth -= 1;
        spec
    }

    fn window_spec_body(&mut self) -> Result<crate::ast::WindowSpec, ParseError> {
        use crate::ast::WindowSpec;
        self.expect(&Token::LParen)?;
        // A leading bare identifier names the window this specification copies
        // from — unless it opens one of the clauses that may follow, which is how
        // `PostgreSQL` resolves the same ambiguity (`opt_existing_window_name` is
        // a `ColId`, and these four words are given clause precedence).
        let base = match self.peek() {
            Token::Ident(word)
                if !["partition", "rows", "range", "groups"]
                    .iter()
                    .any(|clause| word.eq_ignore_ascii_case(clause)) =>
            {
                let name = word.clone();
                self.bump();
                Some(name)
            }
            _ => None,
        };
        let mut partition_by = Vec::new();
        if self.peek_ident_eq("partition") {
            self.bump();
            self.expect(&Token::Keyword(Keyword::By))?;
            loop {
                partition_by.push(self.expr(0)?);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
        }
        let order_by = self.parse_order_by()?;
        let frame = self.opt_frame_clause()?;
        self.expect(&Token::RParen)?;
        Ok(WindowSpec {
            base,
            partition_by,
            order_by,
            frame,
        })
    }

    /// `{ ROWS | RANGE | GROUPS } { <bound> | BETWEEN <bound> AND <bound> }
    /// [ EXCLUDE { CURRENT ROW | GROUP | TIES | NO OTHERS } ]`.
    fn opt_frame_clause(&mut self) -> Result<Option<crate::ast::WindowFrame>, ParseError> {
        use crate::ast::{FrameBound, FrameMode, WindowFrame};
        let mode = if self.eat_ident_eq("rows") {
            FrameMode::Rows
        } else if self.eat_ident_eq("range") {
            FrameMode::Range
        } else if self.eat_ident_eq("groups") {
            FrameMode::Groups
        } else {
            return Ok(None);
        };
        let (start, end) = if self.eat_keyword(Keyword::Between) {
            let start = self.frame_bound()?;
            self.expect(&Token::Keyword(Keyword::And))?;
            (start, self.frame_bound()?)
        } else {
            (self.frame_bound()?, FrameBound::CurrentRow)
        };
        let exclusion = self.frame_exclusion()?;
        Ok(Some(WindowFrame {
            mode,
            start,
            end,
            exclusion,
        }))
    }

    /// One frame bound: `UNBOUNDED PRECEDING`, `<offset> PRECEDING`,
    /// `CURRENT ROW`, `<offset> FOLLOWING`, or `UNBOUNDED FOLLOWING`.
    fn frame_bound(&mut self) -> Result<crate::ast::FrameBound, ParseError> {
        use crate::ast::FrameBound;
        if self.eat_ident_eq("unbounded") {
            return if self.eat_ident_eq("preceding") {
                Ok(FrameBound::UnboundedPreceding)
            } else {
                self.expect_ident_eq("following")?;
                Ok(FrameBound::UnboundedFollowing)
            };
        }
        if self.peek_ident_eq("current")
            && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("row"))
        {
            self.bump();
            self.bump();
            return Ok(FrameBound::CurrentRow);
        }
        // Parsed just above `AND`'s left binding power so a `BETWEEN … AND …`
        // frame does not swallow its own `AND`.
        let offset = self.expr(4)?;
        if self.eat_ident_eq("preceding") {
            Ok(FrameBound::Preceding(offset))
        } else {
            self.expect_ident_eq("following")?;
            Ok(FrameBound::Following(offset))
        }
    }

    fn frame_exclusion(&mut self) -> Result<crate::ast::FrameExclusion, ParseError> {
        use crate::ast::FrameExclusion;
        if !self.eat_ident_eq("exclude") {
            return Ok(FrameExclusion::NoOthers);
        }
        if self.eat_ident_eq("ties") {
            return Ok(FrameExclusion::Ties);
        }
        if self.eat_keyword(Keyword::Group) {
            return Ok(FrameExclusion::Group);
        }
        if self.eat_ident_eq("current") {
            self.expect_ident_eq("row")?;
            return Ok(FrameExclusion::CurrentRow);
        }
        self.expect_ident_eq("no")?;
        self.expect_ident_eq("others")?;
        Ok(FrameExclusion::NoOthers)
    }

    /// `WINDOW name AS ( … ) [, …]`, between `HAVING` and `ORDER BY`.
    fn window_clause(&mut self) -> Result<Vec<crate::ast::NamedWindow>, ParseError> {
        use crate::ast::NamedWindow;
        let mut windows = Vec::new();
        if !self.eat_ident_eq("window") {
            return Ok(windows);
        }
        loop {
            let pos = self.peek_pos();
            let name = self.expect_ident()?;
            if windows
                .iter()
                .any(|w: &NamedWindow| w.name.eq_ignore_ascii_case(&name))
            {
                return Err(ParseError::new_sqlstate(
                    "42P20",
                    format!("window \"{name}\" is already defined"),
                    pos,
                ));
            }
            self.expect(&Token::Keyword(Keyword::As))?;
            let spec = self.window_spec()?;
            windows.push(NamedWindow { name, spec });
            if self.eat_comma() {
                continue;
            }
            break;
        }
        Ok(windows)
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

    fn col_id_expr(&mut self, name: String) -> Result<Expr, ParseError> {
        if *self.peek() == Token::LParen {
            return self.func_call(name);
        }
        if *self.peek() != Token::Dot {
            return Ok(Expr::Column { table: None, name });
        }
        self.bump();
        let field = self.expect_col_id()?;
        if *self.peek() == Token::LParen {
            self.func_call(field)
        } else {
            Ok(Expr::Column {
                table: Some(name),
                name: field,
            })
        }
    }

    /// The SQL/JSON standard expression forms, positioned at `(` after the
    /// function name. `None` means `name` is not one of them, so the caller
    /// falls through to the ordinary function-call path.
    ///
    /// `JSON_OBJECTAGG` and `JSON_ARRAYAGG` lower onto the existing
    /// `jsonb_object_agg` / `jsonb_agg` aggregates, which is exactly what they
    /// compute once `json` is stored as `jsonb`.
    fn sql_json_expr(&mut self, name: &str) -> Result<Option<Expr>, ParseError> {
        use crate::ast::{JsonQueryOp, SqlJsonExpr};

        let op = match name {
            "json_object" => return self.json_object_expr().map(Some),
            "json_array" => return self.json_array_expr().map(Some),
            "json_objectagg" | "json_arrayagg" => {
                return self.json_agg_expr(name).map(Some);
            }
            "json_scalar" => {
                self.expect(&Token::LParen)?;
                let expr = self.expr(0)?;
                self.expect(&Token::RParen)?;
                return Ok(Some(sql_json(SqlJsonExpr::Scalar(expr))));
            }
            "json_serialize" => {
                self.expect(&Token::LParen)?;
                let expr = self.expr(0)?;
                self.opt_format_json();
                let returning = self.opt_returning_type()?;
                self.expect(&Token::RParen)?;
                return Ok(Some(sql_json(SqlJsonExpr::Serialize { expr, returning })));
            }
            "json" => {
                self.expect(&Token::LParen)?;
                let expr = self.expr(0)?;
                self.opt_format_json();
                let unique_keys = self.opt_unique_keys();
                self.expect(&Token::RParen)?;
                return Ok(Some(sql_json(SqlJsonExpr::Parse { expr, unique_keys })));
            }
            "json_exists" => JsonQueryOp::Exists,
            "json_value" => JsonQueryOp::Value,
            "json_query" => JsonQueryOp::Query,
            "json_table" => {
                // `JSON_TABLE` is a FROM item, never an expression. PostgreSQL's
                // grammar has no production for it here, so the token after the
                // name is what its syntax error names.
                return Err(self.syntax_error_at_token());
            }
            _ => return Ok(None),
        };
        self.json_query_expr(op).map(Some)
    }

    /// `JSON_OBJECT( [ k {VALUE | ':'} v, … ] [{NULL | ABSENT} ON NULL]
    /// [{WITH | WITHOUT} UNIQUE [KEYS]] [RETURNING type] )`.
    fn json_object_expr(&mut self) -> Result<Expr, ParseError> {
        use crate::ast::SqlJsonExpr;

        let open = self.pos;
        self.expect(&Token::LParen)?;
        let mut entries = Vec::new();
        while !matches!(self.peek(), Token::RParen) && !self.peek_json_object_tail() {
            let key = self.expr(0)?;
            if !(self.eat_word_eq("value") || self.eat_token(&Token::Colon)) {
                // `json_object('{a,1}'::text[])` is the ordinary two-argument
                // function, not the constructor. PostgreSQL separates them in
                // the grammar; here the absence of `VALUE`/`:` after the first
                // argument decides, so rewind and take the function path.
                self.pos = open;
                return self.func_call("json_object".into());
            }
            let value = self.expr(0)?;
            self.opt_format_json();
            entries.push((key, value));
            if !self.eat_comma() {
                break;
            }
        }
        let absent_on_null = self.opt_on_null()?;
        let unique_keys = self.opt_unique_keys();
        let returning = self.opt_returning_type()?;
        self.expect(&Token::RParen)?;
        Ok(sql_json(SqlJsonExpr::Object {
            entries,
            absent_on_null,
            unique_keys,
            returning,
        }))
    }

    /// `JSON_ARRAY( [ e, … ] [{NULL | ABSENT} ON NULL] [RETURNING type] )`.
    fn json_array_expr(&mut self) -> Result<Expr, ParseError> {
        use crate::ast::SqlJsonExpr;

        self.expect(&Token::LParen)?;
        let mut items = Vec::new();
        while !matches!(self.peek(), Token::RParen) && !self.peek_json_object_tail() {
            items.push(self.expr(0)?);
            self.opt_format_json();
            if !self.eat_comma() {
                break;
            }
        }
        // `JSON_ARRAY` defaults to ABSENT ON NULL, the opposite of `JSON_OBJECT`.
        let absent_on_null = self.opt_on_null_raw()?.unwrap_or(true);
        let returning = self.opt_returning_type()?;
        self.expect(&Token::RParen)?;
        Ok(sql_json(SqlJsonExpr::Array {
            items,
            absent_on_null,
            returning,
        }))
    }

    /// `JSON_OBJECTAGG(k VALUE v …)` / `JSON_ARRAYAGG(e …)`, lowered onto the
    /// `jsonb_object_agg` / `jsonb_agg` aggregates.
    fn json_agg_expr(&mut self, name: &str) -> Result<Expr, ParseError> {
        use crate::ast::{FuncArgs, FuncCall};

        self.expect(&Token::LParen)?;
        let args = if name == "json_objectagg" {
            let key = self.expr(0)?;
            if !(self.eat_word_eq("value") || self.eat_token(&Token::Colon)) {
                return Err(ParseError::new(
                    "expected VALUE or ':' in JSON_OBJECTAGG entry",
                    self.peek_pos(),
                ));
            }
            let value = self.expr(0)?;
            self.opt_format_json();
            vec![key, value]
        } else {
            let item = self.expr(0)?;
            self.opt_format_json();
            let _ = self.parse_order_by()?;
            vec![item]
        };
        // The modifiers are parsed and refused rather than silently ignored:
        // they change which rows contribute, and the plain aggregates cannot.
        if self.opt_on_null_raw()?.is_some() || self.opt_unique_keys() {
            return Err(ParseError::new_sqlstate(
                "0A000",
                "ON NULL and UNIQUE KEYS are not supported on JSON aggregates",
                self.peek_pos(),
            ));
        }
        let _ = self.opt_returning_type()?;
        self.expect(&Token::RParen)?;
        Ok(Expr::Func(FuncCall {
            name: if name == "json_objectagg" {
                "jsonb_object_agg".into()
            } else {
                "jsonb_agg".into()
            },
            distinct: false,
            args: FuncArgs::Exprs(args),
            filter: None,
        }))
    }

    /// `JSON_EXISTS | JSON_VALUE | JSON_QUERY ( context, path
    /// [PASSING v AS name, …] [RETURNING type]
    /// [{WITHOUT | WITH [UNCONDITIONAL | CONDITIONAL]} [ARRAY] WRAPPER]
    /// [{KEEP | OMIT} QUOTES [ON SCALAR STRING]]
    /// [behavior ON EMPTY] [behavior ON ERROR] )`.
    fn json_query_expr(&mut self, op: crate::ast::JsonQueryOp) -> Result<Expr, ParseError> {
        use crate::ast::{JsonQuery, JsonWrapper, SqlJsonExpr};

        self.expect(&Token::LParen)?;
        let context = self.expr(0)?;
        self.opt_format_json();
        self.expect(&Token::Comma)?;
        let path = self.expr(0)?;
        let mut passing = Vec::new();
        if self.eat_word_eq("passing") {
            loop {
                let value = self.expr(0)?;
                self.expect(&Token::Keyword(Keyword::As))?;
                passing.push((self.expect_ident()?, value));
                if !self.eat_comma() {
                    break;
                }
            }
        }
        let returning = self.opt_returning_type()?;
        let mut wrapper = JsonWrapper::Without;
        if self.peek_word_eq("without") || self.peek_word_eq("with") {
            let with = self.peek_word_eq("with");
            // `WITHOUT`/`WITH` also introduce `UNIQUE KEYS`, which this form
            // does not take, so the word is only consumed for a wrapper.
            let conditional = ["wrapper", "array", "conditional", "unconditional"]
                .iter()
                .any(|w| self.peek2_word_eq(w));
            if conditional {
                self.bump();
                let cond = self.eat_word_eq("conditional");
                if !cond {
                    self.eat_word_eq("unconditional");
                }
                self.eat_word_eq("array");
                if !self.eat_word_eq("wrapper") {
                    return Err(ParseError::new("expected WRAPPER", self.peek_pos()));
                }
                wrapper = match (with, cond) {
                    (false, _) => JsonWrapper::Without,
                    (true, true) => JsonWrapper::Conditional,
                    (true, false) => JsonWrapper::Unconditional,
                };
            }
        }
        let mut omit_quotes = false;
        if self.peek_word_eq("omit") || self.peek_word_eq("keep") {
            omit_quotes = self.peek_word_eq("omit");
            self.bump();
            if !self.eat_word_eq("quotes") {
                return Err(ParseError::new("expected QUOTES", self.peek_pos()));
            }
            if self.eat_word_eq("on") {
                self.eat_word_eq("scalar");
                self.eat_word_eq("string");
            }
        }
        let mut on_empty = None;
        let mut on_error = None;
        while let Some((behavior, which)) = self.opt_json_behavior()? {
            match which {
                JsonOnClause::Empty => on_empty = Some(behavior),
                JsonOnClause::Error => on_error = Some(behavior),
            }
        }
        self.expect(&Token::RParen)?;
        Ok(sql_json(SqlJsonExpr::Query(Box::new(JsonQuery {
            op,
            context,
            path,
            passing,
            returning,
            wrapper,
            omit_quotes,
            on_empty,
            on_error,
        }))))
    }

    /// The behavior word that opens an `ON EMPTY` / `ON ERROR` clause, consumed
    /// without the `ON …` that follows it.
    fn json_behavior_word(&mut self) -> Result<Option<crate::ast::JsonBehavior>, ParseError> {
        use crate::ast::JsonBehavior;

        Ok(Some(if self.eat_word_eq("null") {
            JsonBehavior::Null
        } else if self.eat_word_eq("error") {
            JsonBehavior::Error
        } else if self.eat_word_eq("true") {
            JsonBehavior::True
        } else if self.eat_word_eq("false") {
            JsonBehavior::False
        } else if self.eat_word_eq("unknown") {
            JsonBehavior::Unknown
        } else if self.eat_word_eq("default") {
            JsonBehavior::Default(self.expr(0)?)
        } else if self.eat_word_eq("empty") {
            if self.eat_word_eq("object") {
                JsonBehavior::EmptyObject
            } else {
                self.eat_word_eq("array");
                JsonBehavior::EmptyArray
            }
        } else {
            return Ok(None);
        }))
    }

    /// One `<behavior> ON {EMPTY | ERROR}` clause, if the next tokens are one.
    fn opt_json_behavior(
        &mut self,
    ) -> Result<Option<(crate::ast::JsonBehavior, JsonOnClause)>, ParseError> {
        let start = self.pos;
        let Some(behavior) = self.json_behavior_word()? else {
            return Ok(None);
        };
        if !self.eat_word_eq("on") {
            // Not a behavior clause after all (e.g. a bare `RETURNING` tail).
            self.pos = start;
            return Ok(None);
        }
        let which = if self.eat_word_eq("empty") {
            JsonOnClause::Empty
        } else if self.eat_word_eq("error") {
            JsonOnClause::Error
        } else {
            return Err(ParseError::new("expected EMPTY or ERROR", self.peek_pos()));
        };
        Ok(Some((behavior, which)))
    }

    /// `{NULL | ABSENT} ON NULL`; `true` means ABSENT. Defaults to NULL ON NULL,
    /// which is `JSON_OBJECT`'s default.
    fn opt_on_null(&mut self) -> Result<bool, ParseError> {
        Ok(self.opt_on_null_raw()?.unwrap_or(false))
    }

    /// [`Self::opt_on_null`] without the default, so `JSON_ARRAY` can apply its
    /// own (ABSENT ON NULL).
    fn opt_on_null_raw(&mut self) -> Result<Option<bool>, ParseError> {
        let absent = if self.peek_word_eq("absent") {
            true
        } else if self.peek_word_eq("null") && self.peek2_word_eq("on") {
            false
        } else {
            return Ok(None);
        };
        self.bump();
        if !self.eat_word_eq("on") {
            return Err(ParseError::new("expected ON NULL", self.peek_pos()));
        }
        if !self.eat_word_eq("null") {
            return Err(ParseError::new("expected ON NULL", self.peek_pos()));
        }
        Ok(Some(absent))
    }

    /// `{WITH | WITHOUT} UNIQUE [KEYS]`; `true` means WITH.
    fn opt_unique_keys(&mut self) -> bool {
        let unique = if self.peek_word_eq("with") {
            true
        } else if self.peek_word_eq("without") {
            false
        } else {
            return false;
        };
        if !self.peek2_word_eq("unique") {
            return false;
        }
        self.bump();
        self.bump();
        self.eat_word_eq("keys");
        unique
    }

    /// `FORMAT JSON [ENCODING name]`, accepted and ignored. Crabka has one JSON
    /// representation and one server encoding.
    fn opt_format_json(&mut self) -> bool {
        if self.peek_word_eq("format") && self.peek2_word_eq("json") {
            self.bump();
            self.bump();
            if self.eat_word_eq("encoding") {
                let _ = self.eat_word_eq("utf8") || self.eat_word_eq("utf-8");
            }
            return true;
        }
        false
    }

    /// `RETURNING <type> [FORMAT JSON]`.
    fn opt_returning_type(&mut self) -> Result<Option<crabka_pgtypes::ColumnType>, ParseError> {
        if !matches!(self.peek(), Token::Keyword(Keyword::Returning)) {
            return Ok(None);
        }
        self.bump();
        let ty = self.parse_type_name()?;
        self.opt_format_json();
        Ok(Some(ty))
    }

    /// Does the next token open one of `JSON_OBJECT`'s trailing clauses rather
    /// than another entry? (`JSON_OBJECT()` and `JSON_OBJECT(RETURNING jsonb)`
    /// are both legal.)
    fn peek_json_object_tail(&self) -> bool {
        matches!(self.peek(), Token::Keyword(Keyword::Returning))
            || self.peek_word_eq("absent")
            || (self.peek_word_eq("with") || self.peek_word_eq("without"))
                && self.peek2_word_eq("unique")
            || self.peek_word_eq("null") && self.peek2_word_eq("on")
    }

    /// `EXTRACT(field FROM source)`, positioned at `(`, after the `extract` ident.
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
            filter: None,
        }))
    }

    /// The SQL-standard call forms that separate their arguments with keywords
    /// rather than commas, positioned at `(`. `None` means this name has no such
    /// form and the caller should fall through to the ordinary comma-argument
    /// `func_call`. That includes the comma spellings of these same functions,
    /// which `PostgreSQL` also accepts.
    ///
    /// Every form lowers onto the ordinary function of the same name, so the
    /// grammar is the only special part. `SUBSTRING` deliberately keeps both the
    /// numeric and the pattern forms as one parse: `PostgreSQL` distinguishes
    /// `substring(text, int, int)` from the regex `substring(text, text)` by
    /// argument type during overload resolution, not in the grammar.
    fn keyword_arg_call(&mut self, lower: &str) -> Result<Option<Expr>, ParseError> {
        match lower {
            "substring" => self.substring_expr(),
            "trim" => self.trim_expr(),
            "position" => self.position_expr(),
            "overlay" => self.overlay_expr(),
            "xmlparse" => self.xmlparse_expr(),
            "xmlserialize" => self.xmlserialize_expr(),
            "xmlconcat" => self.xmlconcat_expr(),
            _ => Ok(None),
        }
    }

    /// `XMLPARSE ( {DOCUMENT | CONTENT} value [{PRESERVE | STRIP} WHITESPACE] )`.
    ///
    /// Lowers onto `xmlparse('document'|'content', value)`, the way `EXTRACT`
    /// lowers onto `extract('field', src)`: the mode is grammar, not a value, so
    /// it can only arrive as a literal.
    ///
    /// The whitespace option is parsed and discarded. It selects whether libxml
    /// keeps ignorable whitespace in the *tree*, and `XMLPARSE` returns the
    /// input text rather than the tree, so nothing downstream can observe it.
    fn xmlparse_expr(&mut self) -> Result<Option<Expr>, ParseError> {
        self.expect(&Token::LParen)?;
        let mode = self.xml_option_word()?;
        let value = self.expr(0)?;
        if self.eat_word_eq("preserve") || self.eat_word_eq("strip") {
            self.expect_ident_eq("whitespace")?;
        }
        self.expect(&Token::RParen)?;
        Ok(Some(Self::call(
            "xmlparse",
            vec![Expr::StringLiteral(mode.to_string()), value],
        )))
    }

    /// `XMLSERIALIZE ( {DOCUMENT | CONTENT} value AS type [[NO] INDENT] )`.
    ///
    /// Lowers onto `xmlserialize('document'|'content', value, indent)` wrapped
    /// in a cast to the target type — which is `PostgreSQL`'s own shape, not an
    /// approximation of it: `pg_get_viewdef` prints
    /// `(XMLSERIALIZE(...))::character varying` for a non-`text` target because
    /// the parse tree really does carry that cast.
    fn xmlserialize_expr(&mut self) -> Result<Option<Expr>, ParseError> {
        let start = self.peek_pos();
        self.expect(&Token::LParen)?;
        let mode = self.xml_option_word()?;
        let value = Self::coerce_to_xml(self.expr(0)?);
        self.expect(&Token::Keyword(Keyword::As))?;
        let ty = self.parse_type_name()?;
        // `NO INDENT` is the default, so both spellings are accepted and only
        // `INDENT` alone turns formatting on.
        let indent = if self.eat_word_eq("no") {
            self.expect_ident_eq("indent")?;
            false
        } else {
            self.eat_word_eq("indent")
        };
        self.expect(&Token::RParen)?;
        // The target must be a character string type. PostgreSQL checks this in
        // the parser, pointing at the construct rather than the type name.
        if !matches!(
            ty,
            crabka_pgtypes::ColumnType::Text
                | crabka_pgtypes::ColumnType::Varchar(_)
                | crabka_pgtypes::ColumnType::Char(_)
        ) {
            return Err(ParseError::new_sqlstate(
                "42846",
                format!("cannot cast XMLSERIALIZE result to {}", ty.name()),
                start,
            ));
        }
        let serialized = Self::call(
            "xmlserialize",
            vec![
                Expr::StringLiteral(mode.to_string()),
                value,
                Expr::BoolLiteral(indent),
            ],
        );
        Ok(Some(Expr::Cast {
            expr: Box::new(serialized),
            ty,
        }))
    }

    /// `XMLCONCAT ( xml, … )`.
    ///
    /// Lowers onto `xmlconcat(…)` with each argument coerced to `xml`, which is
    /// what `PostgreSQL`'s parse analysis does — and the reason a view over it
    /// deparses as `XMLCONCAT('hello'::xml, 'you'::xml)` rather than naming the
    /// literals' own type.
    fn xmlconcat_expr(&mut self) -> Result<Option<Expr>, ParseError> {
        self.expect(&Token::LParen)?;
        let mut args = Vec::new();
        loop {
            args.push(Self::coerce_to_xml(self.expr(0)?));
            if !self.eat_comma() {
                break;
            }
        }
        self.expect(&Token::RParen)?;
        Ok(Some(Self::call("xmlconcat", args)))
    }

    /// Resolve an untyped literal argument to `xml`, as `PostgreSQL`'s parse
    /// analysis does — which is why a view over `XMLCONCAT('hello', 'you')`
    /// deparses as `XMLCONCAT('hello'::xml, 'you'::xml)`.
    ///
    /// Only an `unknown` literal is coerced. An argument that already has a
    /// type must arrive at the executor with it, so `XMLCONCAT(1, 2)` reports
    /// `argument of XMLCONCAT must be type xml, not type integer` rather than a
    /// cast failure.
    fn coerce_to_xml(expr: Expr) -> Expr {
        if !matches!(expr, Expr::StringLiteral(_)) {
            return expr;
        }
        Expr::Cast {
            expr: Box::new(expr),
            ty: crabka_pgtypes::ColumnType::Xml,
        }
    }

    /// The mandatory `DOCUMENT` / `CONTENT` word that opens `XMLPARSE` and
    /// `XMLSERIALIZE`. Neither is a keyword, so both arrive as identifiers.
    fn xml_option_word(&mut self) -> Result<&'static str, ParseError> {
        if self.eat_word_eq("document") {
            return Ok("document");
        }
        if self.eat_word_eq("content") {
            return Ok("content");
        }
        Err(ParseError::new(
            format!("expected DOCUMENT or CONTENT, found {:?}", self.peek()),
            self.peek_pos(),
        ))
    }

    /// `SUBSTRING(s FROM start FOR count)` and its shorter spellings. `FOR`
    /// without `FROM` starts at 1, which is what `PostgreSQL`'s grammar supplies.
    fn substring_expr(&mut self) -> Result<Option<Expr>, ParseError> {
        self.expect(&Token::LParen)?;
        let source = self.expr(0)?;
        // `substring(s, ...)` is the ordinary call; only the keyword spellings
        // are ours. Rewind is not available, so the comma form is handled by
        // finishing the argument list here.
        if *self.peek() == Token::Comma || *self.peek() == Token::RParen {
            let mut args = vec![source];
            while self.eat_comma() {
                args.push(self.expr(0)?);
            }
            self.expect(&Token::RParen)?;
            return Ok(Some(Self::call("substring", args)));
        }
        let mut args = vec![source];
        if self.eat_keyword(Keyword::From) {
            args.push(self.expr(0)?);
            if self.eat_keyword(Keyword::For) {
                args.push(self.expr(0)?);
            }
        } else if self.eat_keyword(Keyword::For) {
            // `SUBSTRING(s FOR n)` is `substring(s, 1, n)`.
            args.push(Expr::IntLiteral("1".into()));
            args.push(self.expr(0)?);
        } else if self.eat_word_eq("similar") {
            // `SUBSTRING(s SIMILAR pattern ESCAPE esc)` — the SQL-regex form,
            // which is the three-argument `substring(text, text, text)`.
            args.push(self.expr(0)?);
            self.expect_ident_eq("escape")?;
            args.push(self.expr(0)?);
        } else {
            return Err(ParseError::new(
                "expected FROM, FOR or SIMILAR in SUBSTRING".to_string(),
                self.peek_pos(),
            ));
        }
        self.expect(&Token::RParen)?;
        Ok(Some(Self::call("substring", args)))
    }

    /// `TRIM([BOTH|LEADING|TRAILING] [characters] FROM source)`, plus the comma
    /// spelling `TRIM(BOTH characters, source)`. The side chooses the function
    /// `btrim`, `ltrim` or `rtrim`, and defaults to `BOTH`. Omitted characters
    /// mean spaces, which is the one-argument form of each.
    fn trim_expr(&mut self) -> Result<Option<Expr>, ParseError> {
        self.expect(&Token::LParen)?;
        // A side keyword only counts when something follows it: `trim(leading)`
        // is an ordinary trim of a column called `leading`.
        let mut side = "both";
        for word in ["both", "leading", "trailing"] {
            if self.peek_word_eq(word) && !self.peek2_is_arg_end() {
                self.bump();
                side = word;
                break;
            }
        }
        let name = match side {
            "leading" => "ltrim",
            "trailing" => "rtrim",
            _ => "btrim",
        };
        // With a side keyword the characters may be omitted (`TRIM(BOTH FROM s)`);
        // without one there is always at least the source.
        let mut characters = None;
        if !self.eat_keyword(Keyword::From) {
            characters = Some(self.expr(0)?);
            if !self.eat_keyword(Keyword::From) && !self.eat_comma() {
                // `TRIM(s)` — what looked like the characters was the source.
                self.expect(&Token::RParen)?;
                return Ok(Some(Self::call(
                    name,
                    vec![characters.expect("parsed above")],
                )));
            }
        }
        let source = self.expr(0)?;
        self.expect(&Token::RParen)?;
        // PostgreSQL's argument order is (source, characters), the reverse of the
        // order the SQL spelling writes them in.
        let args = match characters {
            Some(characters) => vec![source, characters],
            None => vec![source],
        };
        Ok(Some(Self::call(name, args)))
    }

    /// `POSITION(substring IN string)`: `strpos` with the arguments swapped, as
    /// `PostgreSQL`'s grammar swaps them.
    fn position_expr(&mut self) -> Result<Option<Expr>, ParseError> {
        self.expect(&Token::LParen)?;
        // The needle is a `b_expr` in PostgreSQL's grammar, which excludes the
        // postfix predicates — otherwise `position(x IN (y))` reads its own
        // `IN` as an IN-list and then finds no separator. Binding power 6 is
        // the first level above them. It also excludes the comparisons, which
        // `b_expr` allows; `POSITION(a = b IN c)` is the only casualty and is
        // a type error in PostgreSQL regardless.
        let needle = self.expr(6)?;
        if !self.eat_keyword(Keyword::In) {
            // The comma spelling is `position(needle, haystack)`, in that order.
            self.expect(&Token::Comma)?;
            let haystack = self.expr(0)?;
            self.expect(&Token::RParen)?;
            return Ok(Some(Self::call("position", vec![haystack, needle])));
        }
        let haystack = self.expr(0)?;
        self.expect(&Token::RParen)?;
        Ok(Some(Self::call("position", vec![haystack, needle])))
    }

    /// `OVERLAY(string PLACING replacement FROM start [FOR count])`.
    fn overlay_expr(&mut self) -> Result<Option<Expr>, ParseError> {
        self.expect(&Token::LParen)?;
        let source = self.expr(0)?;
        let mut args = vec![source];
        if !self.eat_word_eq("placing") {
            // The comma spelling takes the same arguments in the same order.
            while self.eat_comma() {
                args.push(self.expr(0)?);
            }
            self.expect(&Token::RParen)?;
            return Ok(Some(Self::call("overlay", args)));
        }
        args.push(self.expr(0)?);
        self.expect(&Token::Keyword(Keyword::From))?;
        args.push(self.expr(0)?);
        if self.eat_keyword(Keyword::For) {
            args.push(self.expr(0)?);
        }
        self.expect(&Token::RParen)?;
        Ok(Some(Self::call("overlay", args)))
    }

    /// A name for a FROM-subquery that was written without one.
    ///
    /// `PostgreSQL` calls the first such subquery `unnamed_subquery` and makes it
    /// un-referenceable, so no query can name it and several may appear in one
    /// FROM. Crabka cannot mark a range-table entry un-referenceable, so each
    /// gets a distinct name instead. That keeps two unnamed subqueries in one
    /// FROM apart. The divergence is that these names *can* be written
    /// in a query, where `PostgreSQL` rejects them.
    fn unnamed_subquery_alias(&mut self) -> String {
        let n = self.unnamed_subqueries;
        self.unnamed_subqueries += 1;
        if n == 0 {
            "unnamed_subquery".to_string()
        } else {
            format!("unnamed_subquery_{n}")
        }
    }

    /// Turn labeled arguments into the positional tail the call needs.
    ///
    /// Only the functions whose parameter names are known here can take them.
    /// Crabka's built-ins are a positional table with no `pg_proc` parameter
    /// names to resolve against, so a name this function does not know is 42883.
    /// The function never drops it without a message. `make_interval` is the one
    /// the corpus exercises.
    fn positional_from_named(
        name: &str,
        positional: &[Expr],
        named: Vec<(String, Expr)>,
        pos: usize,
    ) -> Result<Vec<Expr>, ParseError> {
        const MAKE_INTERVAL: [&str; 7] =
            ["years", "months", "weeks", "days", "hours", "mins", "secs"];
        let params: &[&str] = match name {
            "make_interval" => &MAKE_INTERVAL,
            _ => {
                return Err(ParseError::new_sqlstate(
                    "42883",
                    format!("function {name} does not support named arguments here"),
                    pos,
                ));
            }
        };
        let mut slots: Vec<Option<Expr>> = vec![None; params.len()];
        for (index, arg) in positional.iter().enumerate() {
            *slots.get_mut(index).ok_or_else(|| {
                ParseError::new_sqlstate("42883", format!("too many arguments for {name}"), pos)
            })? = Some(arg.clone());
        }
        for (label, value) in named {
            let index = params.iter().position(|p| *p == label).ok_or_else(|| {
                ParseError::new_sqlstate(
                    "42883",
                    format!("{name} has no parameter named \"{label}\""),
                    pos,
                )
            })?;
            if slots[index].is_some() {
                return Err(ParseError::new_sqlstate(
                    "42P08",
                    format!("argument \"{label}\" of {name} specified more than once"),
                    pos,
                ));
            }
            slots[index] = Some(value);
        }
        // An unsupplied parameter takes the function's own default, which for every
        // `make_interval` field is zero.
        let filled = slots.len() - slots.iter().rev().take_while(|slot| slot.is_none()).count();
        Ok(slots
            .into_iter()
            .take(filled)
            .skip(positional.len())
            .map(|slot| slot.unwrap_or_else(|| Expr::IntLiteral("0".into())))
            .collect())
    }

    /// A positional call to `name`, the shape every keyword-argument form lowers
    /// onto.
    fn call(name: &str, args: Vec<Expr>) -> Expr {
        Expr::Func(crate::ast::FuncCall {
            name: name.into(),
            distinct: false,
            args: crate::ast::FuncArgs::Exprs(args),
            filter: None,
        })
    }

    /// Is the token after the current one the end of an argument, `)` or `,`?
    /// Used to tell a `TRIM` side keyword from a column of the same name.
    fn peek2_is_arg_end(&self) -> bool {
        matches!(self.peek2(), Token::RParen | Token::Comma)
    }

    /// The whole `IS` postfix family, positioned at `IS`: `IS [NOT] NULL`, the
    /// three boolean tests `IS [NOT] TRUE|FALSE|UNKNOWN`, and the null-safe
    /// comparison `IS [NOT] DISTINCT FROM expr`. Anything else after `IS` is a
    /// 42601. `UNKNOWN` is matched keyword-free (as a lowercased identifier), so
    /// a column named `unknown` is unaffected everywhere else.
    fn parse_is_predicate(&mut self, lhs: Expr) -> Result<Expr, ParseError> {
        self.expect(&Token::Keyword(Keyword::Is))?;
        let negated = self.eat_keyword(Keyword::Not);
        if self.eat_keyword(Keyword::Null) {
            return Ok(Expr::IsNull {
                expr: Box::new(lhs),
                negated,
            });
        }
        if self.eat_keyword(Keyword::Distinct) {
            self.expect(&Token::Keyword(Keyword::From))?;
            // Parsed at the comparison level's right binding power so the right
            // operand stays one comparand and a trailing `AND`/`OR` is left to
            // the enclosing Pratt loop.
            let right = self.expr(6)?;
            let op = if negated {
                BinaryOp::IsNotDistinctFrom
            } else {
                BinaryOp::IsDistinctFrom
            };
            return Ok(Expr::Binary {
                op,
                left: Box::new(lhs),
                right: Box::new(right),
            });
        }
        // `expr IS [NOT] DOCUMENT` — the XML predicate, which needs the
        // two-token lookahead above because `document` is unreserved and stays
        // a legal column name and alias.
        if self.peek_ident_eq("document") {
            self.bump();
            return Ok(Expr::Unary {
                op: if negated {
                    UnaryOp::IsNotDocument
                } else {
                    UnaryOp::IsDocument
                },
                expr: Box::new(lhs),
            });
        }
        // `expr IS [NOT] JSON [VALUE | SCALAR | ARRAY | OBJECT]
        // [{WITH | WITHOUT} UNIQUE [KEYS]]`.
        if self.peek_ident_eq("json") {
            use crate::ast::{JsonItemType, SqlJsonExpr};
            self.bump();
            let item = if self.eat_word_eq("scalar") {
                JsonItemType::Scalar
            } else if self.eat_word_eq("array") {
                JsonItemType::Array
            } else if self.eat_word_eq("object") {
                JsonItemType::Object
            } else {
                self.eat_word_eq("value");
                JsonItemType::Value
            };
            let unique_keys = self.opt_unique_keys();
            return Ok(sql_json(SqlJsonExpr::IsJson {
                expr: lhs,
                negated,
                item,
                unique_keys,
            }));
        }
        let op = if self.eat_keyword(Keyword::True) {
            if negated {
                UnaryOp::IsNotTrue
            } else {
                UnaryOp::IsTrue
            }
        } else if self.eat_keyword(Keyword::False) {
            if negated {
                UnaryOp::IsNotFalse
            } else {
                UnaryOp::IsFalse
            }
        } else if self.eat_ident_eq("unknown") {
            if negated {
                UnaryOp::IsNotUnknown
            } else {
                UnaryOp::IsUnknown
            }
        } else {
            return Err(ParseError::new(
                format!(
                    "expected NULL, TRUE, FALSE, UNKNOWN or DISTINCT FROM after IS, found {:?}",
                    self.peek()
                ),
                self.peek_pos(),
            ));
        };
        Ok(Expr::Unary {
            op,
            expr: Box::new(lhs),
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
            let pos = self.peek_pos();
            let item = self.expr(0)?;
            check_row_arity(&lhs, &item, pos)?;
            list.push(item);
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

    /// `expr [NOT] LIKE|ILIKE pat` / `expr [NOT] SIMILAR TO pat`, each with an
    /// optional `ESCAPE c` clause, positioned at the operator's first word. The
    /// pattern and the escape string are parsed at `min_bp = 6` (the right bp of
    /// the comparison level) so each stays a single comparand and does not
    /// swallow a trailing `AND`/`OR`.
    fn parse_like(
        &mut self,
        lhs: Expr,
        negated: bool,
        kind: crate::ast::MatchKind,
    ) -> Result<Expr, ParseError> {
        self.bump(); // LIKE / ILIKE / SIMILAR
        if kind == crate::ast::MatchKind::Similar {
            self.expect(&Token::Keyword(Keyword::To))?;
        }
        let pattern = self.expr(6)?;
        let escape = if self.eat_ident_eq("escape") {
            Some(Box::new(self.expr(6)?))
        } else {
            None
        };
        Ok(Expr::Like {
            expr: Box::new(lhs),
            pattern: Box::new(pattern),
            negated,
            kind,
            escape,
        })
    }

    /// Is the token `offset` positions ahead the start of a `SIMILAR TO`
    /// operator? Keyword-free: `similar` is an ordinary identifier to this
    /// lexer, so only the two-word sequence is the operator.
    fn peek_is_similar_to(&self, offset: usize) -> bool {
        matches!(self.peek_n(offset), Token::Ident(w) if w.eq_ignore_ascii_case("similar"))
            && *self.peek_n(offset + 1) == Token::Keyword(Keyword::To)
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

    /// `CAST(expr AS type)`, positioned at `CAST`. This is the functional
    /// spelling of the `::` operator. The parser parses the inner expression at
    /// the lowest precedence, because the surrounding parens delimit it.
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
    /// the original input, from its first token's offset up to the trailing `;`
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

    /// The offset of the object keyword in a `CREATE …` statement: 1 normally,
    /// more when persistence modifiers (`GLOBAL`/`LOCAL`/`TEMP`/`TEMPORARY`/
    /// `UNLOGGED`) sit between `CREATE` and the object kind.
    fn create_object_keyword_offset(&self) -> usize {
        let mut offset = 1;
        loop {
            let modifier = match self.peek_n(offset) {
                Token::Keyword(Keyword::Global | Keyword::Local) => true,
                Token::Ident(word) => {
                    word.eq_ignore_ascii_case("temp")
                        || word.eq_ignore_ascii_case("temporary")
                        || word.eq_ignore_ascii_case("unlogged")
                }
                _ => false,
            };
            if !modifier {
                return offset;
            }
            offset += 1;
        }
    }

    fn starts_alter_or_drop_text_search(&self) -> bool {
        matches!(
            (self.peek(), self.peek2(), self.peek3()),
            (
                Token::Ident(action),
                Token::Ident(text),
                Token::Ident(search)
            ) if action == "alter" && text == "text" && search == "search"
        ) || matches!(
            (self.peek(), self.peek2(), self.peek3()),
            (
                Token::Keyword(Keyword::Drop),
                Token::Ident(text),
                Token::Ident(search)
            ) if text == "text" && search == "search"
        )
    }

    fn alter_or_drop_text_search_statement(&mut self) -> Result<ParsedStatement, ParseError> {
        use crate::command::CommandIdentity as I;

        let alter = matches!(self.peek(), Token::Ident(action) if action == "alter");
        let kind = self.text_search_kind_at(3)?;
        let identity = match (alter, kind) {
            (true, crate::ast::TextSearchObjectKind::Configuration) => {
                I::AlterTextSearchConfiguration
            }
            (true, crate::ast::TextSearchObjectKind::Dictionary) => I::AlterTextSearchDictionary,
            (false, crate::ast::TextSearchObjectKind::Configuration) => {
                I::DropTextSearchConfiguration
            }
            (false, crate::ast::TextSearchObjectKind::Dictionary) => I::DropTextSearchDictionary,
        };
        let statement = if alter {
            self.alter_text_search()
        } else {
            self.drop_text_search()
        };
        emitted(identity, statement)
    }

    fn set_statement_dispatch(&mut self) -> Result<ParsedStatement, ParseError> {
        use crate::command::CommandIdentity as I;

        if matches!(self.peek2(), Token::Ident(role) if role == "role") {
            emitted(I::SetRole, self.set_role_stmt())
        } else if matches!(self.peek2(), Token::Keyword(Keyword::Transaction)) {
            emitted(I::SetTransaction, self.set_stmt())
        } else {
            self.set_statement()
        }
    }

    #[allow(clippy::too_many_lines)]
    fn statement(&mut self) -> Result<ParsedStatement, ParseError> {
        use crate::command::CommandIdentity as I;

        // A `WITH` list may scope over a data-modifying statement as well as a
        // query, so the CTE list is parsed first and the body decides which.
        if *self.peek() == Token::Keyword(Keyword::With) {
            let with = self.parse_with_clause()?;
            if self.starts_dml_statement() {
                let identity = self.dml_command_identity();
                let mut statement = self.dml_statement()?;
                match &mut statement {
                    crate::ast::Statement::Insert { with: slot, .. }
                    | crate::ast::Statement::Update { with: slot, .. }
                    | crate::ast::Statement::Delete { with: slot, .. }
                    | crate::ast::Statement::Merge { with: slot, .. } => *slot = with,
                    _ => unreachable!("dml_statement only builds DML statements"),
                }
                return emitted(identity, Ok(statement));
            }
            let query = self.query_expr_after_with(with)?;
            let statement = self.finish_query_statement(query);
            let identity = if matches!(statement, crate::ast::Statement::CreateTableAs { .. }) {
                I::SelectInto
            } else {
                I::Select
            };
            return emitted(identity, Ok(statement));
        }
        if self.starts_query_expr() {
            let identity = self.query_command_identity();
            let statement = self.query_statement()?;
            // `SELECT … INTO t` is `CREATE TABLE t AS SELECT …` under another
            // name, so it resolves to the same statement and its own identity.
            let identity = if matches!(statement, crate::ast::Statement::CreateTableAs { .. }) {
                I::SelectInto
            } else {
                identity
            };
            return emitted(identity, Ok(statement));
        }
        if self.starts_alter_or_drop_text_search() {
            return self.alter_or_drop_text_search_statement();
        }
        match self.peek() {
            Token::Ident(s) if s == "merge" && *self.peek2() == Token::Keyword(Keyword::Into) => {
                emitted(I::Merge, self.merge())
            }
            Token::Keyword(Keyword::Create) => self.create_statement(),
            Token::Keyword(Keyword::Drop) => {
                match self.peek2() {
                    Token::Ident(s)
                        if s == "event"
                            && matches!(self.peek3(), Token::Ident(t) if t == "trigger") =>
                    {
                        emitted(I::DropEventTrigger, self.drop_event_trigger())
                    }
                    Token::Ident(s) if s == "trigger" => {
                        emitted(I::DropTrigger, self.drop_trigger())
                    }
                    Token::Ident(s) if s == "policy" => emitted(I::DropPolicy, self.drop_policy()),
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
                    Token::Ident(s) if s == "function" || s == "procedure" || s == "routine" => {
                        self.drop_routine_statement()
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
                    Token::Ident(s) if s == "statistics" => {
                        emitted(I::DropStatistics, self.drop_statistics_stmt())
                    }
                    Token::Ident(s) if s == "tablespace" => {
                        emitted(I::DropTablespace, self.drop_tablespace())
                    }
                    Token::Ident(s)
                        if s == "operator"
                            && matches!(self.peek3(), Token::Ident(t) if t == "class") =>
                    {
                        emitted(I::DropOperatorClass, self.drop_operator_object())
                    }
                    Token::Ident(s)
                        if s == "operator"
                            && matches!(self.peek3(), Token::Ident(t) if t == "family") =>
                    {
                        emitted(I::DropOperatorFamily, self.drop_operator_object())
                    }
                    Token::Keyword(Keyword::Schema) => emitted(I::DropSchema, self.drop_schema()),
                    Token::Ident(s) if s == "type" => emitted(I::DropType, self.drop_type()),
                    Token::Ident(s) if s == "domain" => emitted(I::DropDomain, self.drop_domain()),
                    _ => emitted(I::DropTable, self.drop_table()),
                }
            }
            Token::Ident(s)
                if s == "comment" && matches!(self.peek2(), Token::Keyword(Keyword::On)) =>
            {
                emitted(I::Comment, self.comment_on())
            }
            Token::Ident(s)
                if s == "security"
                    && matches!(self.peek2(), Token::Ident(label) if label == "label") =>
            {
                emitted(I::SecurityLabel, self.security_label())
            }
            Token::Ident(s) if s == "load" => emitted(I::Load, self.load_stmt()),
            Token::Ident(s) if s == "listen" => emitted(I::Listen, self.listen_stmt()),
            Token::Ident(s) if s == "notify" => emitted(I::Notify, self.notify_stmt()),
            Token::Ident(s) if s == "unlisten" => emitted(I::Unlisten, self.unlisten_stmt()),
            Token::Ident(s) if s == "call" => emitted(I::Call, self.call_stmt()),
            Token::Ident(s) if s == "do" => emitted(I::Do, self.do_stmt()),
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
                let statement = self.commit_stmt();
                emitted(identity, Ok(statement))
            }
            Token::Keyword(keyword @ (Keyword::Rollback | Keyword::Abort)) => {
                let leading = *keyword;
                self.rollback_statement(leading)
            }
            Token::Keyword(Keyword::Update) => emitted(I::Update, self.update()),
            Token::Keyword(Keyword::Delete) => emitted(I::Delete, self.delete()),
            // SP37: GUC control. `SET` is a keyword; `SHOW`/`RESET`/`ALTER` are matched as
            // plain (lowercased) idents — keyword-free so they stay usable as names.
            Token::Keyword(Keyword::Set) => self.set_statement_dispatch(),
            // `PREPARE TRANSACTION` is the 2PC refusal below, not SQL PREPARE.
            Token::Ident(s)
                if is_session_utility_word(s)
                    && !(s == "prepare"
                        && matches!(self.peek2(), Token::Keyword(Keyword::Transaction))) =>
            {
                let word = s.clone();
                self.session_utility_statement(&word)
            }
            Token::Ident(s) if s == "show" => emitted(I::Show, self.show_stmt()),
            Token::Ident(s) if s == "reset" => self.reset_statement(),
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
                Token::Ident(s)
                    if s == "event"
                        && matches!(self.peek3(), Token::Ident(t) if t == "trigger") =>
                {
                    emitted(I::AlterEventTrigger, self.alter_event_trigger())
                }
                Token::Ident(s) if s == "trigger" => emitted(I::AlterTrigger, self.alter_trigger()),
                Token::Ident(s) if s == "policy" => emitted(I::AlterPolicy, self.alter_policy()),
                Token::Ident(s) if s == "function" || s == "procedure" || s == "routine" => {
                    self.alter_routine_statement()
                }
                // PostgreSQL's own synopsis lists the row-security subcommands
                // as their own entry in the command inventory, so a statement
                // carrying one reports that identity rather than plain
                // `ALTER TABLE` — the compatibility matrix has a row for it.
                Token::Keyword(Keyword::Table) => {
                    self.alter_table().map(|statement| ParsedStatement {
                        command_identity: match &statement {
                            crate::ast::Statement::AlterTable { actions, .. }
                                if actions.iter().any(|action| {
                                    matches!(
                                        action,
                                        crate::ast::AlterTableAction::EnableRowSecurity
                                            | crate::ast::AlterTableAction::DisableRowSecurity
                                            | crate::ast::AlterTableAction::ForceRowSecurity
                                            | crate::ast::AlterTableAction::NoForceRowSecurity
                                    )
                                }) =>
                            {
                                I::AlterTableEnableRowLevelSecurity
                            }
                            _ => I::AlterTable,
                        },
                        statement,
                    })
                }
                Token::Keyword(Keyword::View) => emitted(I::AlterView, self.alter_view()),
                Token::Keyword(Keyword::Index) => emitted(I::AlterIndex, self.alter_index()),
                Token::Keyword(Keyword::Schema) => emitted(I::AlterSchema, self.alter_schema()),
                Token::Keyword(Keyword::Server) => emitted(I::AlterServer, self.alter_server()),
                // `ALTER USER MAPPING …` and `ALTER USER name …` share a
                // prefix; only the former is followed by MAPPING.
                Token::Keyword(Keyword::User)
                    if matches!(self.peek3(), Token::Keyword(Keyword::Mapping)) =>
                {
                    emitted(I::AlterUserMapping, self.alter_user_mapping())
                }
                Token::Keyword(Keyword::User) => emitted(I::AlterRole, self.alter_role()),
                Token::Ident(s) if s == "database" => {
                    emitted(I::AlterDatabase, self.alter_database_refusal())
                }
                Token::Ident(s) if s == "extension" => {
                    emitted(I::AlterExtension, self.alter_extension_refusal())
                }
                Token::Ident(s) if s == "system" => {
                    emitted(I::AlterSystem, self.alter_system_stmt())
                }
                Token::Ident(s) if s == "statistics" => {
                    emitted(I::AlterStatistics, self.alter_statistics_stmt())
                }
                Token::Ident(s) if s == "tablespace" => {
                    emitted(I::AlterTablespace, self.alter_tablespace())
                }
                Token::Ident(s)
                    if s == "operator"
                        && matches!(self.peek3(), Token::Ident(t) if t == "class") =>
                {
                    emitted(I::AlterOperatorClass, self.alter_operator_object())
                }
                Token::Ident(s)
                    if s == "operator"
                        && matches!(self.peek3(), Token::Ident(t) if t == "family") =>
                {
                    emitted(I::AlterOperatorFamily, self.alter_operator_object())
                }
                Token::Ident(s) if s == "role" => emitted(I::AlterRole, self.alter_role()),
                Token::Ident(s) if s == "type" => emitted(I::AlterType, self.alter_type()),
                Token::Ident(s) if s == "domain" => emitted(I::AlterDomain, self.alter_domain()),
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
        match self.peek_n(offset) {
            Token::Keyword(Keyword::Values) => CommandIdentity::Values,
            Token::Keyword(Keyword::Table) => CommandIdentity::Table,
            _ => CommandIdentity::Select,
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
        use crate::ast::Statement;

        self.expect_ident_eq("alter")?;
        self.expect(&Token::Keyword(Keyword::Table))?;
        let if_exists = self.eat_if_exists()?;
        // `ONLY t` suppresses recursion into the relation's partitions and
        // inheritance children; the `t *` spelling is the explicit form of the
        // default, so it leaves `only` clear.
        let only = self.eat_ident_eq("only");
        let table = self.relation_ref()?;
        if *self.peek() == Token::Star {
            self.bump();
        }
        let mut actions = vec![self.alter_table_action()?];
        // Only the ALTER-subcommand form takes a comma list; RENAME and the
        // ownership/schema movers are standalone in PostgreSQL's grammar too.
        while self.eat_comma() {
            actions.push(self.alter_table_action()?);
        }
        Ok(Statement::AlterTable {
            table,
            if_exists,
            only,
            actions,
        })
    }

    fn alter_index(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("alter")?;
        self.expect(&Token::Keyword(Keyword::Index))?;
        let name = self.relation_ref()?;
        self.expect(&Token::Keyword(Keyword::Set))?;
        self.expect_ident_eq("tablespace")?;
        Ok(crate::ast::Statement::AlterIndexTablespace {
            name,
            tablespace: self.expect_ident()?,
        })
    }

    fn alter_table_action(&mut self) -> Result<crate::ast::AlterTableAction, ParseError> {
        use crate::ast::{AlterTableAction, ColumnDef};

        // Must precede the ENABLE/DISABLE TRIGGER production and the
        // `consume_unsupported_subcommand` catch-all at the end: both would
        // otherwise swallow these, and the catch-all turns a security-relevant
        // subcommand into a silent 0A000.
        if let Some(action) = self.row_security_action()? {
            return Ok(action);
        }

        if self.peek_ident_eq("enable") || self.peek_ident_eq("disable") {
            let enabled = self.eat_ident_eq("enable");
            if !enabled {
                self.expect_ident_eq("disable")?;
            }
            let mode = if !enabled {
                crate::ast::TriggerEnableMode::Disabled
            } else if self.eat_ident_eq("replica") {
                crate::ast::TriggerEnableMode::Replica
            } else if self.eat_ident_eq("always") {
                crate::ast::TriggerEnableMode::Always
            } else {
                crate::ast::TriggerEnableMode::Origin
            };
            self.expect_ident_eq("trigger")?;
            let selector = if self.eat_keyword(Keyword::All) {
                crate::ast::TriggerSelector::All
            } else if self.eat_keyword(Keyword::User) {
                crate::ast::TriggerSelector::User
            } else {
                crate::ast::TriggerSelector::Named(self.expect_object_name()?)
            };
            return Ok(AlterTableAction::SetTriggerMode { selector, mode });
        }

        if self.eat_ident_eq("add") {
            if self.starts_table_constraint() {
                return Ok(AlterTableAction::AddConstraint(self.table_constraint()?));
            }
            let explicit_column = self.eat_ident_eq("column");
            let if_not_exists = self.eat_if_not_exists();
            let _ = explicit_column;
            let name = self.expect_ident()?;
            let (ty, serial) = self.parse_column_type()?;
            let constraints = self.column_constraints()?;
            return Ok(AlterTableAction::AddColumn {
                if_not_exists,
                column: ColumnDef {
                    name,
                    ty,
                    serial,
                    constraints,
                },
            });
        }
        if self.eat_keyword(Keyword::Drop) {
            if self.eat_ident_eq("constraint") {
                let if_exists = self.eat_if_exists()?;
                let name = self.expect_ident()?;
                return Ok(AlterTableAction::DropConstraint {
                    name,
                    if_exists,
                    cascade: self.eat_drop_behavior(),
                });
            }
            self.eat_ident_eq("column");
            let if_exists = self.eat_if_exists()?;
            let column = self.expect_ident()?;
            return Ok(AlterTableAction::DropColumn {
                column,
                if_exists,
                cascade: self.eat_drop_behavior(),
            });
        }
        if self.eat_ident_eq("rename") {
            if self.eat_ident_eq("constraint") {
                let name = self.expect_ident()?;
                self.expect_keyword_or_ident(Keyword::To, "to")?;
                return Ok(AlterTableAction::RenameConstraint {
                    name,
                    new_name: self.expect_ident()?,
                });
            }
            if self.eat_keyword(Keyword::To) || self.eat_ident_eq("to") {
                return Ok(AlterTableAction::RenameTable {
                    new_name: self.expect_ident()?,
                });
            }
            self.eat_ident_eq("column");
            let column = self.expect_ident()?;
            self.expect_keyword_or_ident(Keyword::To, "to")?;
            return Ok(AlterTableAction::RenameColumn {
                column,
                new_name: self.expect_ident()?,
            });
        }
        if self.eat_ident_eq("validate") {
            self.expect_ident_eq("constraint")?;
            return Ok(AlterTableAction::ValidateConstraint(self.expect_ident()?));
        }
        if self.eat_ident_eq("owner") {
            self.expect_keyword_or_ident(Keyword::To, "to")?;
            return Ok(AlterTableAction::OwnerTo(self.expect_object_name()?));
        }
        if self.peek_ident_eq("attach")
            && matches!(self.peek2(), Token::Ident(word) if word.eq_ignore_ascii_case("partition"))
        {
            self.bump();
            self.bump();
            let partition = self.relation_ref()?;
            return Ok(AlterTableAction::AttachPartition {
                partition,
                bound: self.partition_bound()?,
            });
        }
        if self.peek_ident_eq("detach")
            && matches!(self.peek2(), Token::Ident(word) if word.eq_ignore_ascii_case("partition"))
        {
            self.bump();
            self.bump();
            let partition = self.relation_ref()?;
            let concurrently = self.eat_ident_eq("concurrently");
            let finalize = !concurrently && self.eat_ident_eq("finalize");
            return Ok(AlterTableAction::DetachPartition {
                partition,
                concurrently,
                finalize,
            });
        }
        if self.eat_ident_eq("alter") {
            return self.alter_column_action();
        }
        if self.eat_keyword(Keyword::Set) || self.eat_ident_eq("set") {
            if *self.peek() == Token::LParen {
                let params = self.storage_parameter_list()?;
                return Ok(AlterTableAction::SetStorageParameters(params));
            }
            if self.eat_ident_eq("tablespace") {
                return Ok(AlterTableAction::SetTablespace(self.expect_ident()?));
            }
            let label = self.consume_unsupported_subcommand("SET");
            return Ok(AlterTableAction::Unsupported(label));
        }
        if self.eat_ident_eq("reset") {
            self.expect(&Token::LParen)?;
            let mut params = Vec::new();
            loop {
                let mut key = self.expect_ident()?;
                if *self.peek() == Token::Dot {
                    self.bump();
                    key.push('.');
                    key.push_str(&self.expect_ident()?);
                }
                params.push(key);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
            return Ok(AlterTableAction::ResetStorageParameters(params));
        }
        let label = self.consume_unsupported_subcommand("ALTER TABLE");
        if label.is_empty() {
            return Err(ParseError::new(
                format!("unexpected ALTER TABLE subcommand {:?}", self.peek()),
                self.peek_pos(),
            ));
        }
        Ok(AlterTableAction::Unsupported(label))
    }

    /// `ALTER [COLUMN] <name> <action>` — the leading `ALTER` is already
    /// consumed. The knobs Crabka has no counterpart for (`SET STATISTICS`,
    /// `SET STORAGE`, `SET COMPRESSION`, the per-attribute option list) parse
    /// and become [`AlterTableAction::Unsupported`], so the refusal names the
    /// subcommand rather than a token.
    fn alter_column_action(&mut self) -> Result<crate::ast::AlterTableAction, ParseError> {
        use crate::ast::AlterTableAction;

        self.eat_ident_eq("column");
        let column = self.expect_ident()?;
        if self.eat_keyword(Keyword::Set) || self.eat_ident_eq("set") {
            if self.eat_keyword(Keyword::Not) {
                self.expect(&Token::Keyword(Keyword::Null))?;
                return Ok(AlterTableAction::SetNotNull(column));
            }
            if self.eat_ident_eq("default") {
                return Ok(AlterTableAction::SetDefault {
                    column,
                    expr: self.expr(0)?,
                });
            }
            // `SET EXPRESSION AS ( … )` — the parenthesized expression is
            // captured the same way a `GENERATED ALWAYS AS` one is.
            if self.eat_ident_eq("expression") {
                self.expect(&Token::Keyword(Keyword::As))?;
                return Ok(AlterTableAction::SetExpression {
                    column,
                    predicate: self.check_predicate()?,
                });
            }
            // `DATA` is a keyword token (FDW DDL), so both spellings must
            // be accepted here for `SET DATA TYPE`.
            if self.eat_keyword(Keyword::Data) || self.eat_ident_eq("data") {
                return self.alter_column_type(column);
            }
            // SET STATISTICS / STORAGE / COMPRESSION / (attoptions) tune
            // planner and TOAST knobs Crabka has no counterpart for.
            let label = self.consume_unsupported_subcommand("ALTER COLUMN … SET");
            return Ok(AlterTableAction::Unsupported(label));
        }
        if self.eat_keyword(Keyword::Drop) {
            if self.eat_keyword(Keyword::Not) {
                self.expect(&Token::Keyword(Keyword::Null))?;
                return Ok(AlterTableAction::DropNotNull(column));
            }
            if self.eat_ident_eq("default") {
                return Ok(AlterTableAction::DropDefault(column));
            }
            if self.eat_ident_eq("expression") {
                return Ok(AlterTableAction::DropExpression {
                    column,
                    if_exists: self.eat_if_exists()?,
                });
            }
            let label = self.consume_unsupported_subcommand("ALTER COLUMN … DROP");
            return Ok(AlterTableAction::Unsupported(label));
        }
        if matches!(self.peek(), Token::Ident(word) if word.eq_ignore_ascii_case("type")) {
            return self.alter_column_type(column);
        }
        let label = self.consume_unsupported_subcommand("ALTER COLUMN");
        Ok(AlterTableAction::Unsupported(label))
    }

    /// `{ENABLE | DISABLE | FORCE | NO FORCE} ROW LEVEL SECURITY`, or `None`
    /// when the subcommand ahead is something else.
    ///
    /// Every branch commits only after it has seen the whole `ROW LEVEL
    /// SECURITY` tail, so `ENABLE TRIGGER` and `ENABLE ALWAYS RULE` fall
    /// through untouched.
    fn row_security_action(&mut self) -> Result<Option<crate::ast::AlterTableAction>, ParseError> {
        use crate::ast::AlterTableAction;

        let action = if self.peek_ident_eq("enable") && self.peek2_ident_eq("row") {
            AlterTableAction::EnableRowSecurity
        } else if self.peek_ident_eq("disable") && self.peek2_ident_eq("row") {
            AlterTableAction::DisableRowSecurity
        } else if self.peek_ident_eq("force") && self.peek2_ident_eq("row") {
            AlterTableAction::ForceRowSecurity
        } else if self.peek_ident_eq("no")
            && matches!(self.peek2(), Token::Ident(word) if word.eq_ignore_ascii_case("force"))
        {
            self.bump();
            AlterTableAction::NoForceRowSecurity
        } else {
            return Ok(None);
        };
        // The lead-in word (and, for NO FORCE, the second one) is consumed here
        // rather than above so a failed match leaves the cursor where it was.
        self.bump();
        self.expect_ident_eq("row")?;
        // `LEVEL` is a keyword token (isolation levels), so both spellings have
        // to be accepted here.
        self.expect_keyword_or_ident(Keyword::Level, "level")?;
        self.expect_ident_eq("security")?;
        Ok(Some(action))
    }

    /// `TYPE <type> [COLLATE c] [USING <expr>]` — the `TYPE` (or `SET DATA
    /// TYPE`) lead-in is already consumed.
    fn alter_column_type(
        &mut self,
        column: String,
    ) -> Result<crate::ast::AlterTableAction, ParseError> {
        self.expect_ident_eq("type")?;
        let ty = self.parse_type_name()?;
        if self.eat_ident_eq("collate") {
            self.expect_ident()?;
        }
        let using = if self.eat_keyword(Keyword::Using) {
            Some(self.expr(0)?)
        } else {
            None
        };
        Ok(crate::ast::AlterTableAction::SetType { column, ty, using })
    }

    /// Consume the rest of one `ALTER TABLE` subcommand that Crabka's storage
    /// model has no counterpart for. Returns the source text so the executor
    /// can name it in its `0A000`. Stops at the subcommand separator (a
    /// top-level comma) or the end of the statement.
    fn consume_unsupported_subcommand(&mut self, prefix: &str) -> String {
        let start = self.peek_pos();
        let mut depth = 0usize;
        loop {
            match self.peek() {
                Token::Eof | Token::Semicolon => break,
                Token::Comma if depth == 0 => break,
                Token::LParen => depth += 1,
                Token::RParen => depth = depth.saturating_sub(1),
                _ => {}
            }
            self.bump();
        }
        let tail = self.source[start..self.peek_pos()].trim();
        if tail.is_empty() {
            prefix.to_string()
        } else {
            format!("{prefix} {tail}")
        }
    }

    fn create_role(&mut self, can_login: bool) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Create))?;
        if can_login {
            self.expect(&Token::Keyword(Keyword::User))?;
        } else {
            self.expect_ident_eq("role")?;
        }
        let name = self.expect_object_name()?;
        let mut member_of = Vec::new();
        let mut options = crate::ast::RoleOptions::default();
        while !matches!(self.peek(), Token::Semicolon | Token::Eof) {
            if self.eat_keyword(Keyword::In) {
                self.expect_ident_eq("role")?;
                loop {
                    member_of.push(self.expect_object_name()?);
                    if !self.eat_comma() {
                        break;
                    }
                }
            } else if !self.eat_role_option(&mut options) {
                // Options crabka does not model yet (PASSWORD, VALID UNTIL,
                // CONNECTION LIMIT, …) stay accepted metadata.
                self.bump();
            }
        }
        Ok(crate::ast::Statement::CreateRole {
            name,
            can_login,
            member_of,
            options,
        })
    }

    /// `ALTER ROLE name [WITH] option …` — the attribute form. Only the options
    /// written are applied; the rest keep their stored value. `ALTER USER name`
    /// is the same statement; only `ALTER USER MAPPING` takes the other path.
    fn alter_role(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("alter")?;
        if !self.eat_keyword(Keyword::User) {
            self.expect_ident_eq("role")?;
        }
        let name = self.expect_object_name()?;
        let mut options = crate::ast::RoleOptions::default();
        let mut saw_option = false;
        while !matches!(self.peek(), Token::Semicolon | Token::Eof) {
            if self.eat_role_option(&mut options) {
                saw_option = true;
            } else {
                self.bump();
            }
        }
        if !saw_option {
            return Err(ParseError::new(
                "expected at least one role option after ALTER ROLE",
                self.peek_pos(),
            ));
        }
        Ok(crate::ast::Statement::AlterRole { name, options })
    }

    /// Consume one `CREATE`/`ALTER ROLE` boolean attribute, if the next token is
    /// one. `WITH` is noise and is consumed the same way `PostgreSQL` ignores it.
    fn eat_role_option(&mut self, options: &mut crate::ast::RoleOptions) -> bool {
        if self.eat_keyword(Keyword::With) {
            return true;
        }
        let Token::Ident(word) = self.peek() else {
            return false;
        };
        let word = word.to_ascii_lowercase();
        let (field, value): (&mut Option<bool>, bool) = match word.as_str() {
            "superuser" => (&mut options.superuser, true),
            "nosuperuser" => (&mut options.superuser, false),
            "inherit" => (&mut options.inherit, true),
            "noinherit" => (&mut options.inherit, false),
            "createrole" => (&mut options.createrole, true),
            "nocreaterole" => (&mut options.createrole, false),
            "createdb" => (&mut options.createdb, true),
            "nocreatedb" => (&mut options.createdb, false),
            "login" => (&mut options.login, true),
            "nologin" => (&mut options.login, false),
            "replication" => (&mut options.replication, true),
            "noreplication" => (&mut options.replication, false),
            "bypassrls" => (&mut options.bypassrls, true),
            "nobypassrls" => (&mut options.bypassrls, false),
            _ => return false,
        };
        *field = Some(value);
        self.bump();
        true
    }

    fn drop_role(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Drop))?;
        if self.eat_keyword(Keyword::User) {
            // DROP USER name
        } else {
            self.expect_ident_eq("role")?;
        }
        let if_exists = self.eat_if_exists()?;
        Ok(crate::ast::Statement::DropRole {
            name: self.expect_object_name()?,
            if_exists,
        })
    }

    fn security_label(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{Statement, UtilityStatement};

        self.expect_ident_eq("security")?;
        self.expect_ident_eq("label")?;
        let provider = if self.eat_keyword(Keyword::For) {
            Some(self.expect_string_lit()?)
        } else {
            None
        };
        self.expect(&Token::Keyword(Keyword::On))?;
        if self.eat_keyword(Keyword::Table) {
            self.relation_ref()?;
        } else {
            self.expect_ident_eq("role")?;
            self.expect_object_name()?;
        }
        self.expect(&Token::Keyword(Keyword::Is))?;
        if !self.eat_keyword(Keyword::Null) {
            self.expect_string_lit()?;
        }
        Ok(Statement::Utility(UtilityStatement::SecurityLabel {
            provider,
        }))
    }

    fn load_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("load")?;
        Ok(crate::ast::Statement::Utility(
            crate::ast::UtilityStatement::Load {
                filename: self.expect_string_lit()?,
            },
        ))
    }

    fn grant_table_privileges(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("grant")?;
        // `GRANT a, b TO c` hands out role membership; `GRANT SELECT ON t TO c`
        // hands out a privilege. Both open with a comma-separated word list, so
        // the two are told apart the way PostgreSQL's grammar does: by whether
        // `ON` or `TO` closes the list.
        if self.at_role_grant_list() {
            let roles = self.object_name_list()?;
            self.expect(&Token::Keyword(Keyword::To))?;
            let members = self.object_name_list()?;
            let admin_option = self.eat_with_admin_option()?;
            return Ok(crate::ast::Statement::GrantRoles {
                roles,
                members,
                admin_option,
            });
        }
        let privileges = self.privilege_list_until_on()?;
        self.expect(&Token::Keyword(Keyword::On))?;
        if self.eat_keyword(Keyword::Schema) {
            let schemas = self.object_name_list()?;
            self.expect(&Token::Keyword(Keyword::To))?;
            let grantees = self.object_name_list()?;
            return Ok(crate::ast::Statement::GrantSchemaPrivileges {
                privileges,
                schemas,
                grantees,
            });
        }
        // PostgreSQL's `TABLE` object-type keyword is optional: `GRANT SELECT ON
        // t TO r` names a table exactly as `... ON TABLE t ...` does.
        self.eat_keyword(Keyword::Table);
        let tables = self.relation_ref_list()?;
        self.expect(&Token::Keyword(Keyword::To))?;
        let grantees = self.object_name_list()?;
        Ok(crate::ast::Statement::GrantTablePrivileges {
            privileges,
            tables,
            grantees,
        })
    }

    fn revoke_table_privileges(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("revoke")?;
        // `REVOKE ADMIN OPTION FOR a FROM b` strips the admin right and keeps
        // the membership; the bare form drops the membership itself.
        let admin_option = self.peek_ident_eq("admin")
            && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("option"))
            && *self.peek_n(2) == Token::Keyword(Keyword::For);
        if admin_option {
            self.bump();
            self.bump();
            self.bump();
        }
        if admin_option || self.at_role_grant_list() {
            let roles = self.object_name_list()?;
            self.expect(&Token::Keyword(Keyword::From))?;
            let members = self.object_name_list()?;
            return Ok(crate::ast::Statement::RevokeRoles {
                roles,
                members,
                admin_option,
            });
        }
        let privileges = self.privilege_list_until_on()?;
        self.expect(&Token::Keyword(Keyword::On))?;
        if self.eat_keyword(Keyword::Schema) {
            let schemas = self.object_name_list()?;
            self.expect(&Token::Keyword(Keyword::From))?;
            let grantees = self.object_name_list()?;
            return Ok(crate::ast::Statement::RevokeSchemaPrivileges {
                privileges,
                schemas,
                grantees,
            });
        }
        self.eat_keyword(Keyword::Table);
        let tables = self.relation_ref_list()?;
        self.expect(&Token::Keyword(Keyword::From))?;
        let grantees = self.object_name_list()?;
        Ok(crate::ast::Statement::RevokeTablePrivileges {
            privileges,
            tables,
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
        Ok(crate::ast::Statement::SetRole { role, reset: false })
    }

    /// Whether the word list starting here is a `GRANT`/`REVOKE` *role* list
    /// rather than a privilege list.
    ///
    /// A privilege list is closed by `ON`; a role list by `TO` (`GRANT`) or
    /// `FROM` (`REVOKE`). Scanning for whichever comes first is what makes
    /// `GRANT SELECT ON t TO r` and `GRANT selectors TO r` distinguishable
    /// without reserving any of the words in between. Parenthesised column
    /// lists (`GRANT SELECT (a, b) ON …`) are skipped so a `to` column name
    /// cannot masquerade as the list terminator.
    fn at_role_grant_list(&self) -> bool {
        let mut offset = 0;
        let mut depth = 0usize;
        loop {
            match self.peek_n(offset) {
                Token::Eof | Token::Semicolon => return false,
                Token::LParen => depth += 1,
                Token::RParen => depth = depth.saturating_sub(1),
                Token::Keyword(Keyword::On) if depth == 0 => return false,
                Token::Keyword(Keyword::To | Keyword::From) if depth == 0 => return true,
                _ => {}
            }
            offset += 1;
        }
    }

    /// The `[WITH ADMIN OPTION]` tail of `GRANT <role> TO <member>`.
    fn eat_with_admin_option(&mut self) -> Result<bool, ParseError> {
        if !self.eat_keyword(Keyword::With) {
            return Ok(false);
        }
        self.expect_ident_eq("admin")?;
        self.expect_ident_eq("option")?;
        Ok(true)
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

    /// The comma-separated relation list a `GRANT`/`REVOKE` names. `PostgreSQL`
    /// applies the whole privilege set to every relation in it, so a statement
    /// naming two tables is two grants, not one grant of a pair.
    fn relation_ref_list(&mut self) -> Result<Vec<crate::ast::RelationRef>, ParseError> {
        let mut names = vec![self.relation_ref()?];
        while self.eat_comma() {
            names.push(self.relation_ref()?);
        }
        Ok(names)
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

    /// A relation reference, with an optional schema qualifier: `t` or `s.t`.
    ///
    /// This is the one policy for a dotted relation name. Every statement that
    /// names a relation comes through here, so `SELECT * FROM s.t` and
    /// `INSERT INTO s.t` can no longer disagree about what the dot meant.
    ///
    /// The parser carries the qualifier verbatim and does not interpret it. It
    /// has no catalog. A missing schema is `3F000 schema "s" does not exist` or
    /// `42P01 relation "s.t" does not exist` depending on what the statement was
    /// going to do with it, which only the executor knows.
    ///
    /// The parser refuses a three-part name here, because the engine has one
    /// database. So `a.b.c` can only ever be the cross-database refusal.
    fn relation_ref(&mut self) -> Result<crate::ast::RelationRef, ParseError> {
        use crate::ast::RelationRef;
        let start = self.peek_pos();
        let first = self.expect_ident()?;
        if *self.peek() != Token::Dot {
            return Ok(RelationRef::bare(first));
        }
        self.bump();
        let second = self.expect_ident()?;
        if *self.peek() == Token::Dot {
            self.bump();
            let third = self.expect_ident()?;
            return Err(ParseError::new_sqlstate(
                "0A000",
                format!(
                    "cross-database references are not implemented: \"{first}.{second}.{third}\""
                ),
                start,
            ));
        }
        Ok(RelationRef::qualified(first, second))
    }

    /// A possibly-qualified name that is *not* a relation: a collation, an
    /// operator class, a set-returning function in `FROM` position, or a
    /// co-location group. These keep the flattened `a.b` spelling because
    /// nothing resolves them against a schema.
    fn qualified_name_text(&mut self) -> Result<String, ParseError> {
        let mut name = self.expect_ident()?;
        if *self.peek() == Token::Dot {
            self.bump();
            let object = self.expect_ident()?;
            name = format!("{name}.{object}");
        }
        Ok(name)
    }

    fn expect_privilege_name(&mut self) -> Result<String, ParseError> {
        match self.bump() {
            Token::Ident(s) => Ok(s.to_ascii_uppercase()),
            Token::Keyword(Keyword::Select) => Ok("SELECT".into()),
            Token::Keyword(Keyword::Insert) => Ok("INSERT".into()),
            Token::Keyword(Keyword::Update) => Ok("UPDATE".into()),
            Token::Keyword(Keyword::Delete) => Ok("DELETE".into()),
            Token::Keyword(Keyword::Create) => Ok("CREATE".into()),
            Token::Keyword(Keyword::All) => Ok("ALL".into()),
            other => Err(ParseError::new(
                format!("expected privilege name, found {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    /// SP37: `SET [LOCAL] <name> (= | TO) <value>` / `SET [LOCAL] TIME ZONE <value>`.
    /// Keyword-free for `LOCAL`/`TO`/`TIME ZONE`/`DEFAULT`/`LOCAL` (the value).
    /// The parser matches them as lowercased idents, so none becomes a reserved
    /// keyword. The parser normalizes the GUC name to lowercase, and `TIME ZONE`
    /// normalizes to `"timezone"`.
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
        // `SET CONSTRAINTS { ALL | name, … } { DEFERRED | IMMEDIATE }`.
        if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("constraints")) {
            self.bump();
            return self.set_constraints_tail();
        }
        // `SET [SESSION | LOCAL] SESSION AUTHORIZATION …`. The scope word is
        // consumed first, so the authorization spelling is checked before
        // `SESSION` is eaten as the (default) scope.
        if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("session"))
            && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("authorization"))
        {
            self.bump(); // session
            self.bump(); // authorization
            return self.set_session_authorization_tail();
        }
        // `SESSION` is the explicit spelling of the default SET scope.
        if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("session")) {
            self.bump();
            if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("authorization")) {
                self.bump();
                return self.set_session_authorization_tail();
            }
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
            if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("session"))
                && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("authorization"))
            {
                self.bump(); // session
                self.bump(); // authorization
                return self.set_session_authorization_tail();
            }
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
        let name = self.expect_guc_name()?;
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
        let mut items: Vec<String> = Vec::new();
        let mut item = String::new();
        loop {
            let part = match self.peek().clone() {
                Token::Plus | Token::Minus => {
                    let sign = if self.bump() == Token::Minus {
                        "-"
                    } else {
                        "+"
                    };
                    let (Token::IntLit(number) | Token::FloatLit(number)) = self.bump() else {
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
                    if items.is_empty() && item.is_empty() && *self.peek() != Token::Comma {
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
            item.push_str(&part);
            if self.eat_comma() {
                items.push(std::mem::take(&mut item));
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
                item.push(' ');
            } else {
                break;
            }
        }
        items.push(item);
        Ok(SetValue::Value(items))
    }

    /// D6: the tail of `SET CONSTRAINTS { ALL | <name> [, …] } { DEFERRED |
    /// IMMEDIATE }`, positioned after `CONSTRAINTS`.
    fn set_constraints_tail(&mut self) -> Result<crate::ast::Statement, ParseError> {
        let names = if self.eat_keyword(Keyword::All) {
            None
        } else {
            Some(self.object_name_list()?)
        };
        let pos = self.peek_pos();
        let deferred = if self.eat_ident_eq("deferred") {
            true
        } else if self.eat_ident_eq("immediate") {
            false
        } else {
            return Err(ParseError::new(
                format!(
                    "expected DEFERRED or IMMEDIATE in SET CONSTRAINTS, found {:?}",
                    self.peek()
                ),
                pos,
            ));
        };
        Ok(crate::ast::Statement::Utility(
            crate::ast::UtilityStatement::SetConstraints { names, deferred },
        ))
    }

    /// D8: the tail of `SET … SESSION AUTHORIZATION { <role> | DEFAULT }`,
    /// positioned after `AUTHORIZATION`.
    fn set_session_authorization_tail(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::UtilityStatement;
        let role = match self.peek().clone() {
            Token::Ident(word) if word.eq_ignore_ascii_case("default") => {
                self.bump();
                None
            }
            Token::StringLit(name) => {
                self.bump();
                Some(name)
            }
            _ => Some(self.expect_object_name()?),
        };
        Ok(crate::ast::Statement::Utility(
            UtilityStatement::SetSessionAuthorization { role, reset: false },
        ))
    }

    fn set_transaction_tail(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{SetValue, Statement};
        // `SET TRANSACTION` takes the same mode list as `BEGIN`; only the
        // isolation level reaches the GUC, the access mode being carried by the
        // transaction itself.
        let value = match self.transaction_modes()?.isolation {
            Some(level) => SetValue::Value(vec![level.render().into()]),
            None => SetValue::Default,
        };
        Ok(Statement::Set {
            local: false,
            name: "__set_transaction".into(),
            value,
        })
    }

    /// The optional `ISOLATION LEVEL { SERIALIZABLE | REPEATABLE READ | READ
    /// COMMITTED | READ UNCOMMITTED }` tail shared by `BEGIN`/`START
    /// TRANSACTION` and `SET TRANSACTION`.
    fn opt_isolation_level(&mut self) -> Result<Option<crate::ast::IsolationLevel>, ParseError> {
        use crate::ast::IsolationLevel;
        if !self.eat_keyword(Keyword::Isolation) {
            return Ok(None);
        }
        self.expect(&Token::Keyword(Keyword::Level))?;
        if self.eat_keyword(Keyword::Repeatable) {
            self.expect(&Token::Keyword(Keyword::Read))?;
            return Ok(Some(IsolationLevel::RepeatableRead));
        }
        if self.eat_ident_eq("serializable") {
            return Ok(Some(IsolationLevel::Serializable));
        }
        if self.eat_keyword(Keyword::Read) {
            if self.eat_keyword(Keyword::Committed) {
                return Ok(Some(IsolationLevel::ReadCommitted));
            }
            if self.eat_ident_eq("uncommitted") {
                return Ok(Some(IsolationLevel::ReadUncommitted));
            }
        }
        Err(ParseError::new(
            "expected SERIALIZABLE, REPEATABLE READ, READ COMMITTED or READ UNCOMMITTED",
            self.peek_pos(),
        ))
    }

    /// The value after `SET [LOCAL] TIME ZONE`: like [`set_value`], but the bare
    /// idents `LOCAL` and `DEFAULT` both mean "reset to default" (`PostgreSQL`).
    fn set_time_zone_value(&mut self) -> Result<crate::ast::SetValue, ParseError> {
        use crate::ast::SetValue;
        match self.peek().clone() {
            Token::StringLit(s) => {
                self.bump();
                Ok(SetValue::Value(vec![s]))
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
                Ok(SetValue::Value(vec![w]))
            }
            other => Err(ParseError::new(
                format!("expected a TIME ZONE value, found {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    /// A configuration parameter name: an identifier, or `PostgreSQL`'s
    /// two-part `extension.parameter` spelling for a customized option. The
    /// name is normalized to lowercase, as `PostgreSQL` normalizes it.
    fn expect_guc_name(&mut self) -> Result<String, ParseError> {
        let mut name = self.expect_ident()?.to_ascii_lowercase();
        if *self.peek() == Token::Dot {
            self.bump();
            name.push('.');
            name.push_str(&self.expect_ident()?.to_ascii_lowercase());
        }
        Ok(name)
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
        let name = self.expect_guc_name()?;
        Ok(Statement::Show { name })
    }

    /// SP37: `RESET <name>`. Positioned at the `reset` ident.
    fn reset_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.bump(); // reset
        if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("role")) {
            self.bump();
            return Ok(Statement::SetRole {
                role: None,
                reset: true,
            });
        }
        // `RESET SESSION AUTHORIZATION` is the `SET SESSION AUTHORIZATION DEFAULT`
        // spelling; PostgreSQL restores the authenticated user for both.
        if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("session"))
            && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("authorization"))
        {
            self.bump(); // session
            self.bump(); // authorization
            return Ok(Statement::Utility(
                crate::ast::UtilityStatement::SetSessionAuthorization {
                    role: None,
                    reset: true,
                },
            ));
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
        let name = self.expect_guc_name()?;
        Ok(Statement::Reset {
            target: crate::ast::ResetTarget::Name(name),
        })
    }

    /// F-1: `DISCARD { ALL | PLANS | SEQUENCES | TEMPORARY | TEMP }`.
    fn discard_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{DiscardTarget, Statement};
        self.bump(); // discard
        if self.eat_keyword(Keyword::All) {
            return Ok(Statement::Discard {
                target: DiscardTarget::All,
            });
        }
        let pos = self.peek_pos();
        let target = match self.bump() {
            Token::Ident(word) if word.eq_ignore_ascii_case("plans") => DiscardTarget::Plans,
            Token::Ident(word) if word.eq_ignore_ascii_case("sequences") => {
                DiscardTarget::Sequences
            }
            Token::Ident(word)
                if word.eq_ignore_ascii_case("temp") || word.eq_ignore_ascii_case("temporary") =>
            {
                DiscardTarget::Temporary
            }
            other => {
                return Err(ParseError::new(
                    format!(
                        "expected ALL, PLANS, SEQUENCES or TEMPORARY in DISCARD, found {other:?}"
                    ),
                    pos,
                ));
            }
        };
        Ok(Statement::Discard { target })
    }

    /// The `CREATE …` dispatch, split out of [`Self::statement`] so that
    /// function stays inside the readable-length budget.
    fn create_statement(&mut self) -> Result<ParsedStatement, ParseError> {
        use crate::command::CommandIdentity as I;

        // SP40: lookahead dispatch for FDW DDL vs CREATE TABLE. The
        // persistence modifiers (`TEMP`/`TEMPORARY`/`UNLOGGED`, and the
        // `GLOBAL`/`LOCAL` noise words that may precede them) sit
        // between CREATE and the object keyword, so dispatch looks past
        // them.
        match self.peek_n(self.create_object_keyword_offset()) {
            Token::Ident(word)
                if word == "event"
                    && matches!(self.peek_n(self.create_object_keyword_offset() + 1), Token::Ident(next) if next == "trigger") =>
            {
                emitted(I::CreateEventTrigger, self.create_event_trigger())
            }
            Token::Ident(word) if word == "trigger" || word == "constraint" => {
                emitted(I::CreateTrigger, self.create_trigger())
            }
            Token::Ident(word) if word == "policy" => {
                emitted(I::CreatePolicy, self.create_policy())
            }
            Token::Keyword(Keyword::Or)
                if matches!(self.peek_n(self.create_object_keyword_offset() + 1), Token::Ident(replace) if replace == "replace")
                    && matches!(self.peek_n(self.create_object_keyword_offset() + 2), Token::Ident(trigger) if trigger == "trigger") =>
            {
                emitted(I::CreateTrigger, self.create_trigger())
            }
            Token::Ident(word)
                if word == "text"
                    && matches!(self.peek_n(self.create_object_keyword_offset() + 1), Token::Ident(search) if search == "search") =>
            {
                let kind = self.text_search_kind_at(self.create_object_keyword_offset() + 2)?;
                let identity = match kind {
                    crate::ast::TextSearchObjectKind::Configuration => {
                        I::CreateTextSearchConfiguration
                    }
                    crate::ast::TextSearchObjectKind::Dictionary => I::CreateTextSearchDictionary,
                };
                emitted(identity, self.create_text_search())
            }
            Token::Keyword(Keyword::Or) if self.peeked_create_view() => {
                emitted(I::CreateView, self.create_view())
            }
            Token::Keyword(Keyword::Or) if self.peeked_create_routine().is_some() => {
                let object = self
                    .peeked_create_routine()
                    .expect("the guard matched a routine object word");
                emitted(
                    Self::routine_command_identity(object, true, false),
                    self.create_routine(),
                )
            }
            Token::Ident(word) if word == "function" || word == "procedure" => {
                let object = self
                    .peeked_create_routine()
                    .expect("the arm matched a routine object word");
                emitted(
                    Self::routine_command_identity(object, true, false),
                    self.create_routine(),
                )
            }
            Token::Keyword(Keyword::Index | Keyword::Unique) => {
                emitted(I::CreateIndex, self.create_index())
            }
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
                        format!("unexpected token after CREATE FOREIGN: {:?}", self.peek3()),
                        self.peek_pos(),
                    )),
                }
            }
            Token::Keyword(Keyword::Server) => emitted(I::CreateServer, self.create_server()),
            Token::Keyword(Keyword::User) => {
                if matches!(self.peek3(), Token::Keyword(Keyword::Mapping)) {
                    emitted(I::CreateUserMapping, self.create_user_mapping())
                } else {
                    emitted(I::CreateUser, self.create_role(true))
                }
            }
            Token::Ident(s) if s == "role" => emitted(I::CreateRole, self.create_role(false)),
            Token::Ident(s) if s == "sequence" => {
                emitted(I::CreateSequence, self.create_sequence())
            }
            Token::Ident(s) if s == "database" => {
                emitted(I::CreateDatabase, self.create_database_refusal())
            }
            Token::Ident(s) if s == "statistics" => {
                emitted(I::CreateStatistics, self.create_statistics_stmt())
            }
            Token::Keyword(Keyword::Schema) => emitted(I::CreateSchema, self.create_schema()),
            Token::Ident(s) if s == "type" => emitted(I::CreateType, self.create_type()),
            Token::Ident(s) if s == "domain" => emitted(I::CreateDomain, self.create_domain()),
            Token::Ident(s)
                if s == "operator"
                    && matches!(self.peek_n(self.create_object_keyword_offset() + 1), Token::Ident(next) if next == "class") =>
            {
                emitted(I::CreateOperatorClass, self.create_operator_class())
            }
            Token::Ident(s)
                if s == "operator"
                    && matches!(self.peek_n(self.create_object_keyword_offset() + 1), Token::Ident(next) if next == "family") =>
            {
                emitted(I::CreateOperatorFamily, self.create_operator_family())
            }
            Token::Ident(s) if s == "tablespace" => {
                emitted(I::CreateTablespace, self.create_tablespace())
            }
            _ if self.statement_has_top_level_as() => {
                emitted(I::CreateTableAs, self.create_table_as())
            }
            _ => self.create_table().map(|statement| {
                let command_identity = match &statement {
                    crate::ast::Statement::CreateTable { inherits, .. } if !inherits.is_empty() => {
                        I::CreateTableInherits
                    }
                    _ => I::CreateTable,
                };
                ParsedStatement {
                    statement,
                    command_identity,
                }
            }),
        }
    }

    /// `ROLLBACK`/`ABORT`, whose command identity follows the parsed shape:
    /// `ROLLBACK TO [SAVEPOINT] name` is its own PG18 command row.
    fn rollback_statement(&mut self, leading: Keyword) -> Result<ParsedStatement, ParseError> {
        use crate::command::CommandIdentity as I;
        let statement = self.rollback_stmt()?;
        let identity = match (&statement, leading) {
            (crate::ast::Statement::RollbackToSavepoint { .. }, _) => I::RollbackToSavepoint,
            (_, Keyword::Abort) => I::Abort,
            _ => I::Rollback,
        };
        emitted(identity, Ok(statement))
    }

    /// `SET …`, whose command identity follows the parsed shape: `SET
    /// CONSTRAINTS` and `SET … SESSION AUTHORIZATION` are their own rows.
    fn set_statement(&mut self) -> Result<ParsedStatement, ParseError> {
        use crate::{ast::UtilityStatement, command::CommandIdentity as I};
        let statement = self.set_stmt()?;
        let identity = match &statement {
            crate::ast::Statement::Utility(UtilityStatement::SetConstraints { .. }) => {
                I::SetConstraints
            }
            crate::ast::Statement::Utility(UtilityStatement::SetSessionAuthorization {
                ..
            }) => I::SetSessionAuthorization,
            _ => I::Set,
        };
        emitted(identity, Ok(statement))
    }

    /// `RESET …`, which is `SET SESSION AUTHORIZATION DEFAULT` in its
    /// `RESET SESSION AUTHORIZATION` spelling.
    fn reset_statement(&mut self) -> Result<ParsedStatement, ParseError> {
        use crate::{ast::UtilityStatement, command::CommandIdentity as I};
        let statement = self.reset_stmt()?;
        let identity = if matches!(
            &statement,
            crate::ast::Statement::Utility(UtilityStatement::SetSessionAuthorization { .. })
        ) {
            I::SetSessionAuthorization
        } else {
            I::Reset
        };
        emitted(identity, Ok(statement))
    }

    /// The S1/S2/S3/P5/S6 statements whose leading word is a plain identifier.
    /// Split out of [`Self::statement`] so that dispatch stays one arm wide.
    fn session_utility_statement(&mut self, word: &str) -> Result<ParsedStatement, ParseError> {
        use crate::command::CommandIdentity as I;
        match word {
            "savepoint" => emitted(I::Savepoint, self.savepoint_stmt()),
            "release" => emitted(I::ReleaseSavepoint, self.release_savepoint_stmt()),
            "declare" => emitted(I::Declare, self.declare_cursor()),
            "fetch" => emitted(I::Fetch, self.fetch_cursor(false)),
            "move" => emitted(I::Move, self.fetch_cursor(true)),
            "close" => emitted(I::Close, self.close_cursor()),
            "prepare" => emitted(I::Prepare, self.prepare_stmt()),
            "execute" => emitted(I::Execute, self.execute_stmt()),
            "deallocate" => emitted(I::Deallocate, self.deallocate_stmt()),
            "lock" => emitted(I::Lock, self.lock_stmt()),
            "explain" => emitted(I::Explain, self.explain_stmt()),
            "analyze" => emitted(I::Analyze, self.analyze_stmt()),
            "cluster" => emitted(I::Cluster, self.cluster_stmt()),
            "reindex" => emitted(I::Reindex, self.reindex_stmt()),
            "checkpoint" => {
                self.bump();
                emitted(
                    I::Checkpoint,
                    Ok(crate::ast::Statement::Utility(
                        crate::ast::UtilityStatement::Checkpoint,
                    )),
                )
            }
            other => Err(ParseError::new(
                format!("unexpected statement start {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    /// S1: `SAVEPOINT <name>`. Positioned at the `savepoint` ident.
    fn savepoint_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.bump(); // savepoint
        let name = self.expect_ident()?;
        Ok(crate::ast::Statement::Savepoint { name })
    }

    /// S1: `RELEASE [SAVEPOINT] <name>`. Positioned at the `release` ident.
    fn release_savepoint_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.bump(); // release
        self.eat_ident_eq("savepoint");
        let name = self.expect_ident()?;
        Ok(crate::ast::Statement::ReleaseSavepoint { name })
    }

    /// S1: the `ROLLBACK`/`ABORT` family, including `ROLLBACK TO [SAVEPOINT] <name>`.
    /// Positioned at `ROLLBACK`/`ABORT`.
    fn rollback_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.bump(); // ROLLBACK / ABORT
        // `WORK`/`TRANSACTION` are stock noise words on every spelling.
        if !self.eat_keyword(Keyword::Transaction) {
            self.eat_ident_eq("work");
        }
        if self.eat_keyword(Keyword::To) || self.eat_ident_eq("to") {
            self.eat_ident_eq("savepoint");
            let name = self.expect_ident()?;
            return Ok(crate::ast::Statement::RollbackToSavepoint { name });
        }
        let chain = self.eat_transaction_chain_tail();
        Ok(crate::ast::Statement::Rollback { chain })
    }

    /// S1: `COMMIT`/`END [WORK|TRANSACTION] [AND [NO] CHAIN]`. Positioned at the
    /// leading keyword.
    fn commit_stmt(&mut self) -> crate::ast::Statement {
        self.bump(); // COMMIT / END
        if !self.eat_keyword(Keyword::Transaction) {
            self.eat_ident_eq("work");
        }
        let chain = self.eat_transaction_chain_tail();
        crate::ast::Statement::Commit { chain }
    }

    /// The stock `AND [NO] CHAIN` tail; `true` only for the chaining spelling.
    fn eat_transaction_chain_tail(&mut self) -> bool {
        if !matches!(self.peek(), Token::Keyword(Keyword::And)) {
            return false;
        }
        let mark = self.pos;
        self.bump(); // AND
        let negated = self.eat_keyword(Keyword::Not) || self.eat_ident_eq("no");
        if self.eat_ident_eq("chain") {
            return !negated;
        }
        self.pos = mark;
        false
    }

    /// S2: `DECLARE <name> [BINARY] [INSENSITIVE|ASENSITIVE] [[NO] SCROLL] CURSOR
    /// [{WITH|WITHOUT} HOLD] FOR <query>`. Positioned at the `declare` ident.
    fn declare_cursor(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.bump(); // declare
        let name = self.expect_ident()?;
        let mut binary = false;
        let mut scroll = None;
        loop {
            if self.eat_ident_eq("binary") {
                binary = true;
                continue;
            }
            if self.eat_ident_eq("insensitive") || self.eat_ident_eq("asensitive") {
                continue;
            }
            if self.eat_ident_eq("scroll") {
                scroll = Some(true);
                continue;
            }
            if matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("no"))
                && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("scroll"))
            {
                self.bump();
                self.bump();
                scroll = Some(false);
                continue;
            }
            break;
        }
        self.expect_ident_eq("cursor")?;
        let mut hold = false;
        if self.eat_keyword(Keyword::With) {
            self.expect_ident_eq("hold")?;
            hold = true;
        } else if self.eat_ident_eq("without") {
            self.expect_ident_eq("hold")?;
        }
        self.expect(&Token::Keyword(Keyword::For))?;
        let query = self.query_expr()?;
        Ok(crate::ast::Statement::DeclareCursor {
            name,
            binary,
            scroll,
            hold,
            query: Box::new(query),
        })
    }

    /// S2: `FETCH`/`MOVE [<direction>] [FROM|IN] <cursor>`. Positioned at the
    /// leading ident.
    fn fetch_cursor(&mut self, move_only: bool) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{FetchCount, FetchDirection};
        self.bump(); // fetch / move
        let direction = if self.eat_ident_eq("next") {
            FetchDirection::Relative(FetchCount::Rows(1))
        } else if self.eat_ident_eq("prior") {
            FetchDirection::Relative(FetchCount::Rows(-1))
        } else if self.eat_ident_eq("first") {
            FetchDirection::Absolute(1)
        } else if self.eat_ident_eq("last") {
            FetchDirection::Absolute(-1)
        } else if self.eat_ident_eq("absolute") {
            FetchDirection::Absolute(self.signed_fetch_count()?)
        } else if self.eat_ident_eq("relative") {
            FetchDirection::RelativeOne(self.signed_fetch_count()?)
        } else if self.eat_keyword(Keyword::All) {
            FetchDirection::Relative(FetchCount::AllForward)
        } else if self.eat_ident_eq("forward") {
            FetchDirection::Relative(self.fetch_count(false)?)
        } else if self.eat_ident_eq("backward") {
            FetchDirection::Relative(self.fetch_count(true)?)
        } else if matches!(self.peek(), Token::IntLit(_) | Token::Minus | Token::Plus) {
            FetchDirection::Relative(FetchCount::Rows(self.signed_fetch_count()?))
        } else {
            FetchDirection::Relative(FetchCount::Rows(1))
        };
        // `FROM`/`IN` before the cursor name are optional noise words.
        if !self.eat_keyword(Keyword::From) {
            self.eat_keyword(Keyword::In);
        }
        let cursor = self.expect_ident()?;
        Ok(crate::ast::Statement::FetchCursor {
            cursor,
            direction,
            move_only,
        })
    }

    /// The count after `FORWARD`/`BACKWARD`: `ALL`, an explicit signed count, or
    /// the implicit `1`.
    fn fetch_count(&mut self, backward: bool) -> Result<crate::ast::FetchCount, ParseError> {
        use crate::ast::FetchCount;
        if self.eat_keyword(Keyword::All) {
            return Ok(if backward {
                FetchCount::AllBackward
            } else {
                FetchCount::AllForward
            });
        }
        let count = if matches!(self.peek(), Token::IntLit(_) | Token::Minus | Token::Plus) {
            self.signed_fetch_count()?
        } else {
            1
        };
        Ok(FetchCount::Rows(if backward {
            count.saturating_neg()
        } else {
            count
        }))
    }

    /// A `FETCH`/`MOVE` row count, which may carry an explicit sign.
    fn signed_fetch_count(&mut self) -> Result<i64, ParseError> {
        let negative = match self.peek() {
            Token::Minus => {
                self.bump();
                true
            }
            Token::Plus => {
                self.bump();
                false
            }
            _ => false,
        };
        let count = self.expect_int_count("FETCH")?;
        Ok(if negative { -count } else { count })
    }

    /// S2: `CLOSE { <name> | ALL }`. Positioned at the `close` ident.
    fn close_cursor(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::CursorTarget;
        self.bump(); // close
        let target = if self.eat_keyword(Keyword::All) {
            CursorTarget::All
        } else {
            CursorTarget::Name(self.expect_ident()?)
        };
        Ok(crate::ast::Statement::CloseCursor { target })
    }

    /// S2: `PREPARE <name> [ ( <type>, … ) ] AS <statement>`. Positioned at the
    /// `prepare` ident.
    fn prepare_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.bump(); // prepare
        let name = self.expect_ident()?;
        let mut param_types = Vec::new();
        if *self.peek() == Token::LParen {
            self.bump();
            loop {
                param_types.push(self.parse_type_name()?);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
        }
        self.expect(&Token::Keyword(Keyword::As))?;
        let statement = self.statement()?.statement;
        Ok(crate::ast::Statement::PrepareStatement {
            name,
            param_types,
            source: self.source.clone(),
            statement: Box::new(statement),
        })
    }

    /// S2: `EXECUTE <name> [ ( <expr>, … ) ]`. Positioned at the `execute` ident.
    fn execute_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.bump(); // execute
        let name = self.expect_ident()?;
        let mut args = Vec::new();
        if *self.peek() == Token::LParen {
            self.bump();
            loop {
                args.push(self.expr(0)?);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
        }
        Ok(crate::ast::Statement::ExecuteStatement { name, args })
    }

    /// S2: `DEALLOCATE [PREPARE] { <name> | ALL }`. Positioned at the
    /// `deallocate` ident.
    fn deallocate_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::CursorTarget;
        self.bump(); // deallocate
        self.eat_ident_eq("prepare");
        let target = if self.eat_keyword(Keyword::All) {
            CursorTarget::All
        } else {
            CursorTarget::Name(self.expect_ident()?)
        };
        Ok(crate::ast::Statement::Deallocate { target })
    }

    /// S3: `LOCK [TABLE] [ONLY] <name> [*] [, …] [IN <mode> MODE] [NOWAIT]`.
    /// Positioned at the `lock` ident.
    fn lock_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::TableLockMode;
        self.bump(); // lock
        self.eat_keyword(Keyword::Table);
        let mut tables = Vec::new();
        loop {
            self.eat_ident_eq("only");
            tables.push(self.relation_ref()?);
            if *self.peek() == Token::Star {
                self.bump();
            }
            if self.eat_comma() {
                continue;
            }
            break;
        }
        // PostgreSQL's default when `IN <mode> MODE` is omitted.
        let mut mode = TableLockMode::AccessExclusive;
        if self.eat_keyword(Keyword::In) {
            mode = self.table_lock_mode()?;
            self.expect_ident_eq("mode")?;
        }
        let nowait = self.eat_ident_eq("nowait");
        Ok(crate::ast::Statement::LockTable {
            tables,
            mode,
            nowait,
        })
    }

    /// The eight `PostgreSQL` table lock-mode spellings.
    fn table_lock_mode(&mut self) -> Result<crate::ast::TableLockMode, ParseError> {
        use crate::ast::TableLockMode;
        let pos = self.peek_pos();
        if self.eat_ident_eq("access") {
            if self.eat_keyword(Keyword::Share) {
                return Ok(TableLockMode::AccessShare);
            }
            self.expect_ident_eq("exclusive")?;
            return Ok(TableLockMode::AccessExclusive);
        }
        if self.eat_ident_eq("row") {
            if self.eat_keyword(Keyword::Share) {
                return Ok(TableLockMode::RowShare);
            }
            self.expect_ident_eq("exclusive")?;
            return Ok(TableLockMode::RowExclusive);
        }
        if self.eat_keyword(Keyword::Share) {
            // `UPDATE` is a keyword; `ROW`/`EXCLUSIVE` are plain identifiers.
            if self.eat_keyword(Keyword::Update) || self.eat_ident_eq("update") {
                self.expect_ident_eq("exclusive")?;
                return Ok(TableLockMode::ShareUpdateExclusive);
            }
            if self.eat_ident_eq("row") {
                self.expect_ident_eq("exclusive")?;
                return Ok(TableLockMode::ShareRowExclusive);
            }
            return Ok(TableLockMode::Share);
        }
        if self.eat_ident_eq("exclusive") {
            return Ok(TableLockMode::Exclusive);
        }
        Err(ParseError::new(
            format!("expected a table lock mode, found {:?}", self.peek()),
            pos,
        ))
    }

    /// S6: `EXPLAIN [ ( <option>, … ) | ANALYZE | VERBOSE ] <statement>`.
    /// Positioned at the `explain` ident.
    fn explain_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{ExplainFormat, ExplainOptions};
        self.bump(); // explain
        let mut options = ExplainOptions::default();
        if *self.peek() == Token::LParen {
            self.bump();
            loop {
                let pos = self.peek_pos();
                let name = self.expect_ident()?.to_ascii_lowercase();
                match name.as_str() {
                    "format" => {
                        let value_pos = self.peek_pos();
                        let value = self.expect_ident()?;
                        options.format = match value.to_ascii_lowercase().as_str() {
                            "text" => ExplainFormat::Text,
                            "json" => ExplainFormat::Json,
                            "yaml" => ExplainFormat::Yaml,
                            "xml" => ExplainFormat::Xml,
                            _ => {
                                // PostgreSQL reports a bad option *value* as
                                // 22023, not as a syntax error.
                                return Err(ParseError::new_sqlstate(
                                    "22023",
                                    format!(
                                        "unrecognized value for EXPLAIN option \"format\": \"{value}\""
                                    ),
                                    value_pos,
                                ));
                            }
                        };
                    }
                    "analyze" => options.analyze = self.explain_option_flag()?,
                    "verbose" => options.verbose = self.explain_option_flag()?,
                    "costs" => options.costs = self.explain_option_flag()?,
                    // The remaining stock options only change instrumentation
                    // detail the interpreter does not collect; accept their shape.
                    "buffers" | "wal" | "timing" | "summary" | "settings" | "generic_plan"
                    | "memory" => {
                        let _ = self.explain_option_flag()?;
                    }
                    "serialize" => {
                        if matches!(self.peek(), Token::Ident(_)) {
                            self.bump();
                        }
                    }
                    _ => {
                        return Err(ParseError::new(
                            format!("unrecognized EXPLAIN option \"{name}\""),
                            pos,
                        ));
                    }
                }
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
        } else {
            if self.eat_ident_eq("analyze") {
                options.analyze = true;
            }
            if self.eat_ident_eq("verbose") {
                options.verbose = true;
            }
        }
        let statement = self.statement()?.statement;
        Ok(crate::ast::Statement::Explain {
            options,
            statement: Box::new(statement),
        })
    }

    /// An `EXPLAIN` boolean option value: `TRUE`/`FALSE`/`ON`/`OFF`/`1`/`0`, or
    /// nothing at all (which means `TRUE`).
    fn explain_option_flag(&mut self) -> Result<bool, ParseError> {
        let pos = self.peek_pos();
        match self.peek().clone() {
            Token::Keyword(Keyword::True | Keyword::On) => {
                self.bump();
                Ok(true)
            }
            Token::Keyword(Keyword::False) => {
                self.bump();
                Ok(false)
            }
            Token::Ident(word) if word.eq_ignore_ascii_case("off") => {
                self.bump();
                Ok(false)
            }
            Token::IntLit(digits) => {
                self.bump();
                Ok(digits != "0")
            }
            Token::Comma | Token::RParen => Ok(true),
            other => Err(ParseError::new(
                format!("expected an EXPLAIN option value, found {other:?}"),
                pos,
            )),
        }
    }

    /// P5: `ANALYZE [ ( <option>, … ) ] [VERBOSE] [ <table> [ ( <col>, … ) ] [, …] ]`.
    fn analyze_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.bump(); // analyze
        self.eat_utility_option_list()?;
        self.eat_ident_eq("verbose");
        if matches!(self.peek(), Token::Ident(_)) {
            loop {
                self.expect_ident()?;
                if *self.peek() == Token::LParen {
                    self.parse_ident_list()?;
                }
                if self.eat_comma() {
                    continue;
                }
                break;
            }
        }
        Ok(crate::ast::Statement::Utility(
            crate::ast::UtilityStatement::Analyze,
        ))
    }

    /// P5: `CLUSTER [ ( <option>, … ) ] [VERBOSE] [ <table> [ USING <index> ] ]`.
    fn cluster_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.bump(); // cluster
        self.eat_utility_option_list()?;
        self.eat_ident_eq("verbose");
        if matches!(self.peek(), Token::Ident(_)) {
            self.expect_ident()?;
            if self.eat_keyword(Keyword::Using) {
                self.expect_ident()?;
            }
        }
        Ok(crate::ast::Statement::Utility(
            crate::ast::UtilityStatement::Cluster,
        ))
    }

    /// P5: `REINDEX [ ( <option>, … ) ] { INDEX | TABLE | SCHEMA | DATABASE |
    /// SYSTEM } [CONCURRENTLY] [ <name> ]`.
    fn reindex_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.bump(); // reindex
        self.eat_utility_option_list()?;
        let pos = self.peek_pos();
        let recognized = self.eat_keyword(Keyword::Index)
            || self.eat_keyword(Keyword::Table)
            || self.eat_keyword(Keyword::Schema)
            || self.eat_ident_eq("database")
            || self.eat_ident_eq("system");
        if !recognized {
            return Err(ParseError::new(
                format!(
                    "expected INDEX, TABLE, SCHEMA, DATABASE or SYSTEM in REINDEX, found {:?}",
                    self.peek()
                ),
                pos,
            ));
        }
        self.eat_ident_eq("concurrently");
        if matches!(
            self.peek(),
            Token::Ident(_) | Token::Keyword(Keyword::Public | Keyword::User)
        ) {
            self.expect_object_name()?;
        }
        Ok(crate::ast::Statement::Utility(
            crate::ast::UtilityStatement::Reindex,
        ))
    }

    /// P5: `ALTER SYSTEM SET <name> = <value>` / `ALTER SYSTEM RESET { <name> | ALL }`.
    fn alter_system_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::UtilityStatement;
        self.expect_ident_eq("alter")?;
        self.expect_ident_eq("system")?;
        if self.eat_keyword(Keyword::Set) {
            let name = self.expect_guc_name()?;
            if *self.peek() == Token::Eq
                || *self.peek() == Token::Keyword(Keyword::To)
                || matches!(self.peek(), Token::Ident(w) if w.eq_ignore_ascii_case("to"))
            {
                self.bump();
                self.set_value()?;
            }
            return Ok(crate::ast::Statement::Utility(
                UtilityStatement::AlterSystem { name: Some(name) },
            ));
        }
        self.expect_ident_eq("reset")?;
        if self.eat_keyword(Keyword::All) {
            return Ok(crate::ast::Statement::Utility(
                UtilityStatement::AlterSystem { name: None },
            ));
        }
        let name = self.expect_guc_name()?;
        Ok(crate::ast::Statement::Utility(
            UtilityStatement::AlterSystem { name: Some(name) },
        ))
    }

    /// P5: `CREATE STATISTICS [IF NOT EXISTS] <name> [ ( <kind>, … ) ] ON <expr>, …
    /// FROM <table>`.
    fn create_statistics_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect_ident_eq("statistics")?;
        if self.eat_keyword(Keyword::If) {
            self.expect(&Token::Keyword(Keyword::Not))?;
            self.expect(&Token::Keyword(Keyword::Exists))?;
        }
        if matches!(self.peek(), Token::Ident(_)) {
            self.expect_ident()?;
        }
        if *self.peek() == Token::LParen {
            self.parse_ident_list()?;
        }
        self.expect(&Token::Keyword(Keyword::On))?;
        loop {
            self.expr(0)?;
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::Keyword(Keyword::From))?;
        self.expect_ident()?;
        Ok(crate::ast::Statement::CompatibilityRefusal(
            crate::ast::RefusalCommand::CreateStatistics,
        ))
    }

    /// P5: `ALTER STATISTICS <name> { OWNER TO … | RENAME TO … | SET SCHEMA … |
    /// SET STATISTICS … }`.
    fn alter_statistics_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("alter")?;
        self.expect_ident_eq("statistics")?;
        self.expect_ident()?;
        let pos = self.peek_pos();
        if self.eat_ident_eq("owner") || self.eat_ident_eq("rename") {
            self.expect_keyword_or_ident(Keyword::To, "to")?;
            self.expect_object_name()?;
        } else if self.eat_keyword(Keyword::Set) {
            if self.eat_keyword(Keyword::Schema) {
                self.expect_ident()?;
            } else {
                self.expect_ident_eq("statistics")?;
                if !self.eat_ident_eq("default") {
                    self.signed_fetch_count()?;
                }
            }
        } else {
            return Err(ParseError::new(
                format!(
                    "expected OWNER TO, RENAME TO, SET SCHEMA or SET STATISTICS, found {:?}",
                    self.peek()
                ),
                pos,
            ));
        }
        Ok(crate::ast::Statement::CompatibilityRefusal(
            crate::ast::RefusalCommand::AlterStatistics,
        ))
    }

    /// P5: `DROP STATISTICS [IF EXISTS] <name> [, …]`.
    fn drop_statistics_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect_ident_eq("statistics")?;
        self.eat_if_exists()?;
        self.object_name_list()?;
        Ok(crate::ast::Statement::CompatibilityRefusal(
            crate::ast::RefusalCommand::DropStatistics,
        ))
    }

    /// The `( <name> [<value>] [, …] )` option list shared by the utility
    /// commands whose options carry no semantics here.
    fn eat_utility_option_list(&mut self) -> Result<bool, ParseError> {
        if *self.peek() != Token::LParen {
            return Ok(false);
        }
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
        Ok(true)
    }

    fn text_search_kind_at(
        &self,
        offset: usize,
    ) -> Result<crate::ast::TextSearchObjectKind, ParseError> {
        match self.peek_n(offset) {
            Token::Ident(word) if word == "configuration" => {
                Ok(crate::ast::TextSearchObjectKind::Configuration)
            }
            Token::Ident(word) if word == "dictionary" => {
                Ok(crate::ast::TextSearchObjectKind::Dictionary)
            }
            other => Err(ParseError::new(
                format!("expected CONFIGURATION or DICTIONARY, found {other:?}"),
                self.peek_pos(),
            )),
        }
    }

    fn text_search_kind(&mut self) -> Result<crate::ast::TextSearchObjectKind, ParseError> {
        let kind = self.text_search_kind_at(0)?;
        self.bump();
        Ok(kind)
    }

    fn text_search_object_name(&mut self) -> Result<String, ParseError> {
        let first = self.expect_object_name()?;
        if self.eat_token(&Token::Dot) {
            Ok(format!("{first}.{}", self.expect_object_name()?))
        } else {
            Ok(first)
        }
    }

    fn text_search_leadin(&mut self, create_or_drop: Option<Keyword>) -> Result<(), ParseError> {
        if let Some(keyword) = create_or_drop {
            self.expect(&Token::Keyword(keyword))?;
        } else {
            self.expect_ident_eq("alter")?;
        }
        self.expect_ident_eq("text")?;
        self.expect_ident_eq("search")
    }

    fn create_text_search(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{Statement, TextSearchDdl, TextSearchObjectKind, UtilityStatement};
        self.text_search_leadin(Some(Keyword::Create))?;
        let kind = self.text_search_kind()?;
        let name = self.text_search_object_name()?;
        self.expect(&Token::LParen)?;
        let mut base = None;
        loop {
            let option = self.expect_col_label()?;
            self.expect(&Token::Eq)?;
            let value = self.text_search_object_name()?;
            if matches!(
                (kind, option.as_str()),
                (TextSearchObjectKind::Configuration, "copy" | "parser")
                    | (TextSearchObjectKind::Dictionary, "template")
            ) {
                base = Some(
                    if option == "parser"
                        && matches!(value.as_str(), "default" | "pg_catalog.default")
                    {
                        "simple".into()
                    } else {
                        value
                    },
                );
            }
            if !self.eat_comma() {
                break;
            }
        }
        self.expect(&Token::RParen)?;
        let base = base.ok_or_else(|| {
            ParseError::new(
                match kind {
                    TextSearchObjectKind::Configuration => "expected COPY or PARSER option",
                    TextSearchObjectKind::Dictionary => "expected TEMPLATE option",
                },
                self.peek_pos(),
            )
        })?;
        Ok(Statement::Utility(UtilityStatement::TextSearch(
            TextSearchDdl::Create { kind, name, base },
        )))
    }

    fn alter_text_search(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{Statement, TextSearchDdl, UtilityStatement};
        self.text_search_leadin(None)?;
        let kind = self.text_search_kind()?;
        let name = self.text_search_object_name()?;
        let rename_to = if self.eat_ident_eq("rename") {
            self.expect_keyword_or_ident(Keyword::To, "to")?;
            Some(self.text_search_object_name()?)
        } else {
            // Mapping and dictionary-option alterations are durable metadata
            // updates but do not change the built-in Rust lexer/stemmer.
            while !matches!(self.peek(), Token::Semicolon | Token::Eof) {
                self.bump();
            }
            None
        };
        Ok(Statement::Utility(UtilityStatement::TextSearch(
            TextSearchDdl::Alter {
                kind,
                name,
                rename_to,
            },
        )))
    }

    fn drop_text_search(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{Statement, TextSearchDdl, UtilityStatement};
        self.text_search_leadin(Some(Keyword::Drop))?;
        let kind = self.text_search_kind()?;
        let if_exists = self.eat_if_exists()?;
        let name = self.text_search_object_name()?;
        self.eat_drop_behavior();
        Ok(Statement::Utility(UtilityStatement::TextSearch(
            TextSearchDdl::Drop {
                kind,
                name,
                if_exists,
            },
        )))
    }

    fn begin(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        let leading = self.bump(); // BEGIN or START
        if leading == Token::Keyword(Keyword::Start) {
            // START TRANSACTION is valid; bare START is not a statement.
            self.expect(&Token::Keyword(Keyword::Transaction))?;
        } else {
            // TRANSACTION is optional after BEGIN.
            self.eat_keyword(Keyword::Transaction);
        }
        let modes = self.transaction_modes()?;
        Ok(Statement::Begin {
            isolation: modes.isolation,
            read_only: modes.read_only,
            deferrable: modes.deferrable,
        })
    }

    /// The comma-separated `transaction_mode` list shared by `BEGIN`, `START
    /// TRANSACTION` and `SET TRANSACTION`: `ISOLATION LEVEL …`, `READ ONLY`,
    /// `READ WRITE`, and `[NOT] DEFERRABLE`, in any order. `PostgreSQL` also lets
    /// the commas be omitted, so the separator is optional.
    fn transaction_modes(&mut self) -> Result<TransactionModes, ParseError> {
        let mut modes = TransactionModes::default();
        loop {
            if let Some(isolation) = self.opt_isolation_level()? {
                modes.isolation = Some(isolation);
            } else if self.eat_keyword(Keyword::Read) {
                // `READ ONLY` / `READ WRITE`. `READ` also opens `READ COMMITTED`,
                // but only inside `ISOLATION LEVEL`, which is handled above.
                if self.eat_ident_eq("only") {
                    modes.read_only = Some(true);
                } else if self.eat_ident_eq("write") {
                    modes.read_only = Some(false);
                } else {
                    return Err(ParseError::new(
                        "expected ONLY or WRITE after READ",
                        self.peek_pos(),
                    ));
                }
            } else if self.eat_keyword(Keyword::Not) {
                self.expect_ident_eq("deferrable")?;
                modes.deferrable = Some(false);
            } else if self.eat_ident_eq("deferrable") {
                modes.deferrable = Some(true);
            } else {
                return Ok(modes);
            }
            // The comma is optional between modes.
            self.eat_comma();
        }
    }

    fn update(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Update))?;
        let only = self.eat_only();
        let table = self.relation_ref()?;
        let alias = self.opt_dml_target_alias()?;
        self.expect(&Token::Keyword(Keyword::Set))?;
        let assignments = self.assignment_list(&table.name)?;
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
        let returning = self.returning_clause()?;
        Ok(Statement::Update {
            table,
            only,
            with: None,
            alias,
            assignments,
            from,
            filter,
            returning,
        })
    }

    /// The optional `ONLY` in front of a DML target relation.
    ///
    /// `only` is a plain identifier to this lexer, so it is taken as the
    /// keyword only when another name follows it. That keeps `TRUNCATE only`
    /// meaning the table called `only`, which is all it could have meant here
    /// before, while `TRUNCATE ONLY t` reads as `PostgreSQL` reads it.
    fn eat_only(&mut self) -> bool {
        if self.peek_ident_eq("only")
            && matches!(
                self.peek2(),
                Token::Ident(_) | Token::Keyword(Keyword::Public | Keyword::Data)
            )
        {
            self.bump();
            return true;
        }
        false
    }

    /// The optional alias on an `UPDATE`/`DELETE`/`MERGE` target. Only the
    /// explicit `AS name` and a bare identifier are aliases; the clause keywords
    /// that may follow the target (`SET`, `USING`, `WHERE`, `RETURNING`, …) are
    /// keyword tokens, so a bare identifier here is never one of them.
    fn opt_dml_target_alias(&mut self) -> Result<Option<String>, ParseError> {
        if self.eat_keyword(Keyword::As) {
            return Ok(Some(self.expect_ident()?));
        }
        match self.peek() {
            Token::Ident(_) => Ok(Some(self.expect_ident()?)),
            _ => Ok(None),
        }
    }

    /// One or more `SET` entries: `col = expr` or the parenthesised multi-column
    /// `(a, b) = ROW(…) | (…) | (SELECT …)`.
    ///
    /// `relation` is the statement's target table, named only to report a
    /// relation-qualified target the way `PostgreSQL` does.
    fn assignment_list(
        &mut self,
        relation: &str,
    ) -> Result<Vec<crate::ast::Assignment>, ParseError> {
        use crate::ast::{Assignment, AssignmentValue, Expr};
        let mut assignments = Vec::new();
        loop {
            if *self.peek() == Token::LParen {
                let pos = self.peek_pos();
                let targets = self.parse_parenthesized_ident_list()?;
                self.expect(&Token::Eq)?;
                let value = self.expr(0)?;
                let value = match value {
                    Expr::Row(items) => AssignmentValue::Row(items),
                    Expr::ScalarSubquery(query) => AssignmentValue::Subquery(query),
                    // `SET (a) = (expr)` — a one-element parenthesised target list
                    // takes an ordinary expression, exactly as `SET a = expr` does.
                    other if targets.len() == 1 => AssignmentValue::Expr(other),
                    _ => {
                        return Err(ParseError::new_sqlstate(
                            "42601",
                            "source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression",
                            pos,
                        ));
                    }
                };
                assignments.push(Assignment {
                    targets,
                    subscripts: Vec::new(),
                    value,
                });
            } else {
                let pos = self.peek_pos();
                // `set_target` is `ColId opt_indirection`, so the target's first
                // component is a plain `ColId` — including words this lexer
                // keywords but PostgreSQL does not, such as `public`.
                let column = self.expect_col_id()?;
                // A SET target cannot be qualified with the relation name. The
                // grammar accepts `t.a` and `public.t.a` alike (the whole tail is
                // indirection), and PostgreSQL then fails to find a column named
                // by the FIRST component — there being no composite types here,
                // every qualified target ends that way.
                if *self.peek() == Token::Dot {
                    return Err(ParseError::new_sqlstate(
                        "42703",
                        format!("column \"{column}\" of relation \"{relation}\" does not exist"),
                        pos,
                    ));
                }
                // `SET j['a'][0] = e` / `SET a[1:2] = ARRAY[…]` — a subscripted
                // target updates the value *inside* the column.
                let subscripts = if *self.peek() == Token::LBracket {
                    self.subscript_chain()?
                } else {
                    Vec::new()
                };
                self.expect(&Token::Eq)?;
                let value = self.insert_value_expr()?;
                assignments.push(Assignment {
                    targets: vec![column],
                    subscripts,
                    value: AssignmentValue::Expr(value),
                });
            }
            if self.eat_comma() {
                continue;
            }
            break;
        }
        Ok(assignments)
    }

    fn delete(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Delete))?;
        self.expect(&Token::Keyword(Keyword::From))?;
        let only = self.eat_only();
        let table = self.relation_ref()?;
        let alias = self.opt_dml_target_alias()?;
        let using = if self.eat_keyword(Keyword::Using) {
            self.parse_from()?
        } else {
            Vec::new()
        };
        let filter = if self.eat_keyword(Keyword::Where) {
            Some(self.expr(0)?)
        } else {
            None
        };
        let returning = self.returning_clause()?;
        Ok(Statement::Delete {
            table,
            only,
            with: None,
            alias,
            using,
            filter,
            returning,
        })
    }

    /// `MERGE INTO target [AS alias] USING source [AS alias] ON cond WHEN … [RETURNING …]`.
    fn merge(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{MergeSource, Statement};
        self.expect_ident_eq("merge")?;
        self.expect(&Token::Keyword(Keyword::Into))?;
        let table = self.relation_ref()?;
        let alias = self.opt_dml_target_alias()?;
        self.expect(&Token::Keyword(Keyword::Using))?;
        let source = if *self.peek() == Token::LParen {
            self.bump();
            let query = self.query_expr_after_open_paren()?;
            let alias = self.opt_alias()?.ok_or_else(|| {
                ParseError::new(
                    "subquery in MERGE USING must have an alias",
                    self.peek_pos(),
                )
            })?;
            let columns = self.opt_column_aliases()?;
            MergeSource::Query {
                query: Box::new(query),
                alias,
                columns,
            }
        } else {
            let name = self.relation_ref()?;
            MergeSource::Table {
                name,
                alias: self.opt_dml_target_alias()?,
            }
        };
        self.expect(&Token::Keyword(Keyword::On))?;
        let on = self.expr(0)?;
        let mut clauses = Vec::new();
        while *self.peek() == Token::Keyword(Keyword::When) {
            clauses.push(self.merge_when(&table.name)?);
        }
        if clauses.is_empty() {
            return Err(ParseError::new_sqlstate(
                "42601",
                "MERGE requires at least one WHEN clause",
                self.peek_pos(),
            ));
        }
        let returning = self.returning_clause()?;
        Ok(Statement::Merge {
            table,
            with: None,
            alias,
            source,
            on,
            clauses,
            returning,
        })
    }

    fn merge_when(&mut self, relation: &str) -> Result<crate::ast::MergeWhen, ParseError> {
        use crate::ast::{MergeAction, MergeMatchKind, MergeWhen};
        self.expect(&Token::Keyword(Keyword::When))?;
        let kind = if self.eat_keyword(Keyword::Not) {
            self.expect_ident_eq("matched")?;
            if self.eat_keyword(Keyword::By) {
                if self.eat_ident_eq("source") {
                    MergeMatchKind::NotMatchedBySource
                } else {
                    self.expect_ident_eq("target")?;
                    MergeMatchKind::NotMatchedByTarget
                }
            } else {
                MergeMatchKind::NotMatchedByTarget
            }
        } else {
            self.expect_ident_eq("matched")?;
            MergeMatchKind::Matched
        };
        let condition = if self.eat_keyword(Keyword::And) {
            Some(self.expr(0)?)
        } else {
            None
        };
        self.expect(&Token::Keyword(Keyword::Then))?;
        let action = if self.eat_ident_eq("do") {
            self.expect_ident_eq("nothing")?;
            MergeAction::DoNothing
        } else if self.eat_keyword(Keyword::Update) {
            self.expect(&Token::Keyword(Keyword::Set))?;
            MergeAction::Update(self.assignment_list(relation)?)
        } else if self.eat_keyword(Keyword::Delete) {
            MergeAction::Delete
        } else {
            self.expect(&Token::Keyword(Keyword::Insert))?;
            let columns = if *self.peek() == Token::LParen {
                Some(self.parse_parenthesized_ident_list()?)
            } else {
                None
            };
            let values = if self.eat_ident_eq("default") {
                self.expect(&Token::Keyword(Keyword::Values))?;
                None
            } else {
                self.expect(&Token::Keyword(Keyword::Values))?;
                self.expect(&Token::LParen)?;
                let mut row = vec![self.insert_value_expr()?];
                while self.eat_comma() {
                    row.push(self.insert_value_expr()?);
                }
                self.expect(&Token::RParen)?;
                Some(row)
            };
            MergeAction::Insert { columns, values }
        };
        // PostgreSQL rejects the action/kind pairs that cannot make sense: only a
        // NOT MATCHED clause may INSERT, and only a MATCHED one may UPDATE/DELETE.
        let action_ok = matches!(
            (&kind, &action),
            (
                MergeMatchKind::NotMatchedByTarget,
                MergeAction::Insert { .. }
            ) | (
                MergeMatchKind::Matched | MergeMatchKind::NotMatchedBySource,
                MergeAction::Update(_) | MergeAction::Delete,
            ) | (_, MergeAction::DoNothing)
        );
        if !action_ok {
            return Err(ParseError::new_sqlstate(
                "42601",
                "MERGE WHEN clause action is not allowed for that match condition",
                self.peek_pos(),
            ));
        }
        Ok(MergeWhen {
            kind,
            condition,
            action,
        })
    }

    /// `CREATE TABLE [IF NOT EXISTS] name [(col, …)] AS <query> [WITH [NO] DATA]`.
    fn create_table_as(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Create))?;
        // The persistence modifiers sit between CREATE and TABLE in this
        // spelling too; `GLOBAL`/`LOCAL` are noise words on a temp table.
        self.eat_keyword(Keyword::Global);
        self.eat_keyword(Keyword::Local);
        let temporary = self.eat_ident_eq("temp") || self.eat_ident_eq("temporary");
        let _unlogged = self.eat_ident_eq("unlogged");
        self.expect(&Token::Keyword(Keyword::Table))?;
        let if_not_exists = self.eat_if_not_exists();
        let name = self.relation_ref()?;
        let columns = if *self.peek() == Token::LParen {
            Some(self.parse_parenthesized_ident_list()?)
        } else {
            None
        };
        let tablespace = self
            .eat_ident_eq("tablespace")
            .then(|| self.expect_ident())
            .transpose()?;
        self.expect(&Token::Keyword(Keyword::As))?;
        let query = self.query_expr()?;
        let with_data = if self.eat_keyword(Keyword::With) {
            let no = self.eat_keyword(Keyword::Not) || self.eat_ident_eq("no");
            self.expect(&Token::Keyword(Keyword::Data))?;
            !no
        } else {
            true
        };
        Ok(Statement::CreateTableAs {
            name,
            temporary,
            if_not_exists,
            columns,
            query: Box::new(query),
            with_data,
            tablespace,
        })
    }

    fn eat_if_not_exists(&mut self) -> bool {
        if *self.peek() == Token::Keyword(Keyword::If)
            && *self.peek2() == Token::Keyword(Keyword::Not)
            && *self.peek_n(2) == Token::Keyword(Keyword::Exists)
        {
            self.bump();
            self.bump();
            self.bump();
            return true;
        }
        false
    }

    /// True when a top-level (paren-depth zero) `AS` appears before the end of
    /// this statement. That `AS` tells `CREATE TABLE … AS <query>` apart
    /// from an ordinary `CREATE TABLE`, whose only `AS` spellings sit inside the
    /// column-definition parentheses.
    fn statement_has_top_level_as(&self) -> bool {
        let mut depth = 0usize;
        let mut offset = 0usize;
        loop {
            match self.peek_n(offset) {
                Token::Eof | Token::Semicolon => return false,
                Token::LParen => depth += 1,
                Token::RParen => depth = depth.saturating_sub(1),
                Token::Keyword(Keyword::As) if depth == 0 => return true,
                _ => {}
            }
            offset += 1;
        }
    }

    /// The standalone `TABLE name` query, which `PostgreSQL` defines as exactly
    /// `SELECT * FROM name`.
    fn table_query_body(&mut self) -> Result<crate::ast::SelectStmt, ParseError> {
        use crate::ast::{SelectItem, SelectStmt, TableExpr};
        self.expect(&Token::Keyword(Keyword::Table))?;
        let only = self.eat_only();
        let name = self.relation_ref()?;
        if *self.peek() == Token::Star {
            self.bump();
        }
        Ok(SelectStmt {
            projection: vec![SelectItem::Wildcard],
            from: vec![TableExpr::Table {
                name,
                only,
                alias: None,
                columns: None,
                sample: None,
            }],
            filter: None,
            distinct: crate::ast::DistinctClause::All,
            group_by: Vec::new(),
            grouping: None,
            having: None,
            windows: Vec::new(),
            window_calls: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            with_ties: false,
            locking: None,
        })
    }

    fn create_table(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{ColumnDef, HashShardingSpec, ShardingSpec, Statement};
        self.expect(&Token::Keyword(Keyword::Create))?;
        // PostgreSQL treats GLOBAL/LOCAL as noise words on a temp table.
        self.eat_keyword(Keyword::Global);
        self.eat_keyword(Keyword::Local);
        let temporary = self.eat_ident_eq("temp") || self.eat_ident_eq("temporary");
        let unlogged = self.eat_ident_eq("unlogged");
        let _ = unlogged;
        self.expect(&Token::Keyword(Keyword::Table))?;
        let if_not_exists = self.eat_if_not_exists();
        let name = self.relation_ref()?;
        let mut columns = Vec::new();
        let mut constraints = Vec::new();
        let mut like = Vec::new();
        // `PARTITION OF parent` takes the place of the column-definition list:
        // a partition inherits its parent's columns and may only add qualifiers.
        let partition_parent = if self.peek_ident_eq("partition")
            && matches!(self.peek2(), Token::Ident(word) if word.eq_ignore_ascii_case("of"))
        {
            self.bump();
            self.bump();
            Some(self.relation_ref()?)
        } else {
            None
        };
        let mut column_options = Vec::new();
        if partition_parent.is_some() {
            // `(a NOT NULL, b WITH OPTIONS DEFAULT 0, CHECK (…))` is optional.
            if *self.peek() == Token::LParen {
                self.bump();
                loop {
                    if self.starts_table_constraint() {
                        constraints.push(self.table_constraint()?);
                    } else {
                        let column = self.expect_ident()?;
                        if self.eat_keyword(Keyword::With) {
                            self.expect_ident_eq("options")?;
                        }
                        column_options.push((column, self.column_constraints()?));
                    }
                    if self.eat_comma() {
                        continue;
                    }
                    break;
                }
                self.expect(&Token::RParen)?;
            }
        } else {
            self.expect(&Token::LParen)?;
            // `CREATE TABLE t ()` is legal PostgreSQL — an element list may be empty.
            if *self.peek() != Token::RParen {
                loop {
                    if self.eat_keyword(Keyword::Like) {
                        like.push(self.like_clause()?);
                    } else if self.starts_table_constraint() {
                        constraints.push(self.table_constraint()?);
                    } else {
                        let col_name = self.expect_ident()?;
                        let (ty, serial) = self.parse_column_type()?;
                        let constraints = self.column_constraints()?;
                        columns.push(ColumnDef {
                            name: col_name,
                            ty,
                            serial,
                            constraints,
                        });
                    }
                    if self.eat_comma() {
                        continue;
                    }
                    break;
                }
            }
            self.expect(&Token::RParen)?;
        }
        let partition_of = match partition_parent {
            Some(parent) => Some(crate::ast::PartitionOf {
                parent,
                bound: self.partition_bound()?,
                column_options,
            }),
            None => None,
        };
        let inherits = if self.eat_ident_eq("inherits") {
            self.parse_relation_ref_list()?
        } else {
            Vec::new()
        };
        let partition_by = self.opt_partition_by()?;
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
                Some(self.qualified_name_text()?)
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
            self.storage_parameter_list()?;
        } else if self.eat_ident_eq("without") {
            self.expect_ident_eq("oids")?;
        }
        let on_commit = self.on_commit_action()?;
        let tablespace = self
            .eat_ident_eq("tablespace")
            .then(|| self.expect_ident())
            .transpose()?;
        Ok(Statement::CreateTable {
            name,
            columns,
            constraints,
            sharded,
            sharding,
            if_not_exists,
            temporary,
            like,
            inherits,
            on_commit,
            partition_by,
            partition_of,
            tablespace,
        })
    }

    /// `PARTITION BY <strategy> ( <key> [COLLATE c] [opclass], … )`, or `None`
    /// when the next tokens do not start one.
    ///
    /// The strategy word is *not* validated here: `PARTITION BY MAGIC (a)` is
    /// `PostgreSQL`'s 22023 from parse analysis, not a syntax error, so the word
    /// is carried to the executor verbatim.
    fn opt_partition_by(&mut self) -> Result<Option<crate::ast::PartitionBy>, ParseError> {
        use crate::ast::{PartitionBy, PartitionKeyElem};
        if !(self.peek_ident_eq("partition") && *self.peek2() == Token::Keyword(Keyword::By)) {
            return Ok(None);
        }
        self.bump();
        self.bump();
        let strategy = self.expect_ident()?.to_ascii_lowercase();
        self.expect(&Token::LParen)?;
        let mut keys = Vec::new();
        loop {
            let start = self.peek_pos();
            // A bare identifier followed by a key terminator, a `COLLATE`, or an
            // operator-class name is a plain column reference; anything else is
            // an expression key.
            let column = if matches!(self.peek(), Token::Ident(_))
                && matches!(self.peek2(), Token::Comma | Token::RParen | Token::Ident(_))
            {
                match self.bump() {
                    Token::Ident(name) => Some(name),
                    other => unreachable!("peeked an identifier, bumped {other:?}"),
                }
            } else {
                self.expr(0)?;
                None
            };
            let text = self.source[start..self.peek_pos()].trim().to_string();
            let collation = if self.eat_ident_eq("collate") {
                Some(self.qualified_name_text()?)
            } else {
                None
            };
            // An operator-class name is a bare identifier in the one position
            // where nothing else can appear.
            let opclass = if matches!(self.peek(), Token::Ident(_)) {
                Some(self.qualified_name_text()?)
            } else {
                None
            };
            keys.push(PartitionKeyElem {
                column,
                text,
                collation,
                opclass,
            });
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        Ok(Some(PartitionBy { strategy, keys }))
    }

    /// A partition's bound: `FOR VALUES {IN (…) | FROM (…) TO (…) | WITH (…)}`
    /// or `DEFAULT`.
    fn partition_bound(&mut self) -> Result<crate::ast::PartitionBound, ParseError> {
        use crate::ast::PartitionBound;
        if self.eat_ident_eq("default") {
            return Ok(PartitionBound::Default);
        }
        self.expect(&Token::Keyword(Keyword::For))?;
        self.expect(&Token::Keyword(Keyword::Values))?;
        if self.eat_keyword(Keyword::In) {
            self.expect(&Token::LParen)?;
            let mut values = Vec::new();
            loop {
                values.push(self.expr(0)?);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
            return Ok(PartitionBound::List(values));
        }
        if self.eat_keyword(Keyword::From) {
            let from = self.partition_range_bound_list()?;
            self.expect(&Token::Keyword(Keyword::To))?;
            let to = self.partition_range_bound_list()?;
            return Ok(PartitionBound::Range { from, to });
        }
        self.expect(&Token::Keyword(Keyword::With))?;
        self.expect(&Token::LParen)?;
        self.expect_ident_eq("modulus")?;
        let modulus = self.expect_unsigned_integer_literal()?;
        self.expect(&Token::Comma)?;
        self.expect_ident_eq("remainder")?;
        let remainder = self.expect_unsigned_integer_literal()?;
        self.expect(&Token::RParen)?;
        Ok(PartitionBound::Hash { modulus, remainder })
    }

    /// `( {MINVALUE | MAXVALUE | <expr>}, … )`: one side of a range bound.
    fn partition_range_bound_list(
        &mut self,
    ) -> Result<Vec<crate::ast::RangeBoundValue>, ParseError> {
        use crate::ast::RangeBoundValue;
        self.expect(&Token::LParen)?;
        let mut bounds = Vec::new();
        loop {
            if self.eat_ident_eq("minvalue") {
                bounds.push(RangeBoundValue::MinValue);
            } else if self.eat_ident_eq("maxvalue") {
                bounds.push(RangeBoundValue::MaxValue);
            } else {
                bounds.push(RangeBoundValue::Value(self.expr(0)?));
            }
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        Ok(bounds)
    }

    /// `PostgreSQL`'s `Iconst`: an unsigned integer literal. `MODULUS -1` is a
    /// syntax error there because the grammar has no place for the sign, and it
    /// is one here for the same reason.
    fn expect_unsigned_integer_literal(&mut self) -> Result<i64, ParseError> {
        let pos = self.peek_pos();
        match self.bump() {
            Token::IntLit(text) => text.parse::<i64>().map_err(|_| {
                ParseError::new(format!("{text} is out of range for an integer"), pos)
            }),
            other => Err(ParseError::new(
                format!("expected an integer, found {other:?}"),
                pos,
            )),
        }
    }

    /// `CREATE SCHEMA [IF NOT EXISTS] {name [AUTHORIZATION role] |
    /// AUTHORIZATION role} [<schema element> …]`.
    fn create_schema(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect(&Token::Keyword(Keyword::Schema))?;
        let if_not_exists = self.eat_if_not_exists();
        let name = if self.peek_ident_eq("authorization") {
            None
        } else {
            Some(self.expect_object_name()?)
        };
        let authorization = if self.eat_ident_eq("authorization") {
            Some(self.expect_object_name()?)
        } else {
            None
        };
        if name.is_none() && authorization.is_none() {
            return Err(ParseError::new(
                "CREATE SCHEMA requires a schema name or AUTHORIZATION",
                self.peek_pos(),
            ));
        }
        // The element list is written without separators — PostgreSQL's
        // `OptSchemaEltList` is a sequence of complete statements.
        let mut elements = Vec::new();
        while matches!(
            self.peek(),
            Token::Keyword(Keyword::Create) | Token::Ident(_)
        ) && !matches!(self.peek(), Token::Ident(word) if !starts_schema_element(word))
        {
            elements.push(self.statement()?.statement);
        }
        Ok(crate::ast::Statement::CreateSchema {
            name,
            authorization,
            if_not_exists,
            elements,
        })
    }

    /// `ALTER SCHEMA name {RENAME TO new | OWNER TO role}`.
    fn alter_schema(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::AlterSchemaAction;
        self.expect_ident_eq("alter")?;
        self.expect(&Token::Keyword(Keyword::Schema))?;
        let name = self.expect_object_name()?;
        let pos = self.peek_pos();
        let action = if self.eat_ident_eq("rename") {
            self.expect_keyword_or_ident(Keyword::To, "to")?;
            AlterSchemaAction::RenameTo(self.expect_object_name()?)
        } else if self.eat_ident_eq("owner") {
            self.expect_keyword_or_ident(Keyword::To, "to")?;
            AlterSchemaAction::OwnerTo(self.expect_object_name()?)
        } else {
            return Err(ParseError::new(
                format!("expected RENAME TO or OWNER TO, found {:?}", self.peek()),
                pos,
            ));
        };
        Ok(crate::ast::Statement::AlterSchema { name, action })
    }

    /// `DROP SCHEMA [IF EXISTS] name [, …] [CASCADE | RESTRICT]`.
    fn drop_schema(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect(&Token::Keyword(Keyword::Schema))?;
        let if_exists = self.eat_if_exists()?;
        let mut names = Vec::new();
        loop {
            names.push(self.expect_object_name()?);
            if self.eat_comma() {
                continue;
            }
            break;
        }
        Ok(crate::ast::Statement::DropSchema {
            names,
            if_exists,
            cascade: self.eat_drop_behavior(),
        })
    }

    /// `ON COMMIT { PRESERVE ROWS | DELETE ROWS | DROP }`.
    fn on_commit_action(&mut self) -> Result<Option<crate::ast::OnCommitAction>, ParseError> {
        use crate::ast::OnCommitAction;
        if !(matches!(self.peek(), Token::Keyword(Keyword::On))
            && matches!(self.peek2(), Token::Keyword(Keyword::Commit)))
        {
            return Ok(None);
        }
        self.bump();
        self.bump();
        if self.eat_keyword(Keyword::Drop) {
            return Ok(Some(OnCommitAction::Drop));
        }
        if self.eat_keyword(Keyword::Delete) {
            self.expect_ident_eq("rows")?;
            return Ok(Some(OnCommitAction::DeleteRows));
        }
        self.expect_ident_eq("preserve")?;
        self.expect_ident_eq("rows")?;
        Ok(Some(OnCommitAction::PreserveRows))
    }

    /// `(LIKE source [ {INCLUDING | EXCLUDING} <option> …])`. The `LIKE` itself
    /// is already consumed.
    fn like_clause(&mut self) -> Result<crate::ast::LikeClause, ParseError> {
        use crate::ast::LikeOption;
        let source = self.relation_ref()?;
        let mut clause = crate::ast::LikeClause {
            source,
            including: Vec::new(),
        };
        loop {
            let including = if self.eat_ident_eq("including") {
                true
            } else if self.eat_ident_eq("excluding") {
                false
            } else {
                break;
            };
            let pos = self.peek_pos();
            if self.eat_keyword(Keyword::All) {
                for option in LikeOption::ALL {
                    clause.set(*option, including);
                }
                continue;
            }
            let option = self.expect_ident()?;
            match option.as_str() {
                "defaults" => clause.set(LikeOption::Defaults, including),
                "constraints" => clause.set(LikeOption::Constraints, including),
                "indexes" => clause.set(LikeOption::Indexes, including),
                "identity" => clause.set(LikeOption::Identity, including),
                // The remaining PostgreSQL options (COMMENTS, GENERATED,
                // STATISTICS, STORAGE, COMPRESSION) name properties Crabka does
                // not carry on a column, so honoring them is a no-op either way.
                "comments" | "generated" | "statistics" | "storage" | "compression" => {}
                other => {
                    return Err(ParseError::new(
                        format!("unrecognized LIKE option \"{other}\""),
                        pos,
                    ));
                }
            }
        }
        Ok(clause)
    }

    /// True when the next tokens begin a table-level constraint rather than a
    /// column definition. A leading `CONSTRAINT <name>` always does; the bare
    /// keywords do only when the following token is not a type name, which the
    /// constraint keywords' own follow sets settle: `PRIMARY`/`FOREIGN` are
    /// followed by `KEY`, `UNIQUE`/`CHECK` by `(` or `NULLS`.
    fn starts_table_constraint(&self) -> bool {
        match self.peek() {
            // `constraint` is a plain identifier, so `CONSTRAINT <name> <kind>`
            // is only distinguishable from a column named `constraint` by the
            // constraint keyword that must follow the label.
            Token::Ident(s) if s.eq_ignore_ascii_case("constraint") => {
                matches!(self.peek2(), Token::Ident(_)) && starts_constraint_kind(self.peek3())
            }
            Token::Ident(s) if s.eq_ignore_ascii_case("primary") => {
                matches!(self.peek2(), Token::Ident(k) if k.eq_ignore_ascii_case("key"))
            }
            Token::Ident(s) if s.eq_ignore_ascii_case("check") => *self.peek2() == Token::LParen,
            Token::Keyword(Keyword::Foreign) => {
                matches!(self.peek2(), Token::Ident(k) if k.eq_ignore_ascii_case("key"))
            }
            Token::Keyword(Keyword::Unique) => {
                *self.peek2() == Token::LParen
                    || matches!(self.peek2(), Token::Ident(k) if k.eq_ignore_ascii_case("nulls"))
            }
            Token::Ident(s) if s.eq_ignore_ascii_case("exclude") => true,
            _ => false,
        }
    }

    /// One table-level constraint, including its optional `CONSTRAINT <name>`
    /// label and trailing deferrability clauses.
    fn table_constraint(&mut self) -> Result<crate::ast::TableConstraint, ParseError> {
        use crate::ast::{TableConstraint, TableConstraintKind};
        let name = if self.eat_ident_eq("constraint") {
            Some(self.expect_ident()?)
        } else {
            None
        };
        let pos = self.peek_pos();
        let kind = if self.eat_ident_eq("primary") {
            self.expect_ident_eq("key")?;
            let (columns, without_overlaps) = self.parse_key_column_list()?;
            TableConstraintKind::PrimaryKey {
                columns,
                without_overlaps,
            }
        } else if self.eat_keyword(Keyword::Unique) {
            let nulls_not_distinct = self.eat_nulls_not_distinct()?;
            let (columns, without_overlaps) = self.parse_key_column_list()?;
            TableConstraintKind::Unique {
                columns,
                nulls_not_distinct,
                without_overlaps,
            }
        } else if self.eat_ident_eq("check") {
            TableConstraintKind::Check(self.check_predicate()?)
        } else if self.eat_keyword(Keyword::Foreign) {
            self.expect_ident_eq("key")?;
            let (columns, period) = self.parse_period_column_list()?;
            TableConstraintKind::ForeignKey {
                columns,
                period,
                references: self.foreign_key_reference()?,
            }
        } else if self.eat_ident_eq("exclude") {
            let method = if self.eat_keyword(Keyword::Using) {
                self.expect_ident()?
            } else {
                "gist".into()
            };
            self.expect(&Token::LParen)?;
            let mut elements = Vec::new();
            loop {
                let column = self.expect_ident()?;
                self.expect_keyword_or_ident(Keyword::With, "with")?;
                let operator = match self.bump() {
                    Token::Eq => crate::ast::BinaryOp::Eq,
                    Token::Overlaps => crate::ast::BinaryOp::Overlaps,
                    token => {
                        return Err(ParseError::new(
                            format!("unsupported exclusion operator {token:?}"),
                            self.peek_pos(),
                        ));
                    }
                };
                elements.push(crate::ast::ExclusionElement { column, operator });
                if !self.eat_comma() {
                    break;
                }
            }
            self.expect(&Token::RParen)?;
            TableConstraintKind::Exclude { method, elements }
        } else {
            return Err(ParseError::new(
                format!("expected a table constraint, found {:?}", self.peek()),
                pos,
            ));
        };
        let attributes = self.eat_constraint_attributes(true)?;
        Ok(TableConstraint {
            name,
            kind,
            attributes,
        })
    }

    /// `( <predicate> )` for `CHECK`/`GENERATED ALWAYS AS`, capturing the exact
    /// source text between the parentheses alongside the parsed expression.
    fn check_predicate(&mut self) -> Result<crate::ast::CheckPredicate, ParseError> {
        self.expect(&Token::LParen)?;
        let start = self.peek_pos();
        let expr = self.expr(0)?;
        let end = self.peek_pos();
        self.expect(&Token::RParen)?;
        Ok(crate::ast::CheckPredicate {
            expr,
            text: self.source[start..end].trim().to_string(),
        })
    }

    /// `NULLS NOT DISTINCT` / `NULLS DISTINCT` after `UNIQUE`.
    fn eat_nulls_not_distinct(&mut self) -> Result<bool, ParseError> {
        if !matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("nulls")) {
            return Ok(false);
        }
        self.bump();
        let not = self.eat_keyword(Keyword::Not);
        self.expect(&Token::Keyword(Keyword::Distinct))?;
        Ok(not)
    }

    /// `REFERENCES <table> [(col, …)] [MATCH …] [ON DELETE …] [ON UPDATE …]`.
    ///
    /// `ON DELETE` and `ON UPDATE` may come in either order and at most once
    /// each; `PostgreSQL`'s grammar admits no repeat, so a second clause for the
    /// same side is a syntax error naming `DELETE`/`UPDATE`, and a third clause
    /// once both sides are set is a syntax error naming `ON`.
    fn foreign_key_reference(&mut self) -> Result<crate::ast::ForeignKeyRef, ParseError> {
        self.expect_ident_eq("references")?;
        let table = self.relation_ref()?;
        let (columns, period) = if *self.peek() == Token::LParen {
            self.parse_period_column_list()?
        } else {
            (Vec::new(), false)
        };
        let match_type = self.foreign_key_match()?;
        let mut on_delete = None;
        let mut on_update = None;
        let mut set_columns = Vec::new();
        while matches!(self.peek(), Token::Keyword(Keyword::On))
            && matches!(
                self.peek2(),
                Token::Keyword(Keyword::Delete | Keyword::Update)
            )
        {
            let clause_pos = self.peek_pos();
            if on_delete.is_some() && on_update.is_some() {
                return Err(ParseError::new(
                    "syntax error at or near \"ON\"".to_string(),
                    clause_pos,
                ));
            }
            self.bump();
            let side_pos = self.peek_pos();
            let on_delete_side = matches!(self.peek(), Token::Keyword(Keyword::Delete));
            self.bump();
            if on_delete_side && on_delete.is_some() {
                return Err(ParseError::new(
                    "syntax error at or near \"DELETE\"".to_string(),
                    side_pos,
                ));
            }
            if !on_delete_side && on_update.is_some() {
                return Err(ParseError::new(
                    "syntax error at or near \"UPDATE\"".to_string(),
                    side_pos,
                ));
            }
            let (action, action_columns) = self.referential_action(on_delete_side, clause_pos)?;
            if on_delete_side {
                on_delete = Some(action);
                set_columns = action_columns;
            } else {
                on_update = Some(action);
            }
        }
        Ok(crate::ast::ForeignKeyRef {
            table,
            columns,
            period,
            match_type,
            on_delete: on_delete.unwrap_or_default(),
            on_update: on_update.unwrap_or_default(),
            set_columns,
        })
    }

    /// `MATCH { FULL | PARTIAL | SIMPLE }`, absent meaning `SIMPLE`.
    ///
    /// `MATCH PARTIAL` is `PostgreSQL`'s own `0A000` refusal, reported at the
    /// `MATCH` keyword. Keep it, and do not invent semantics for a clause
    /// `PostgreSQL` has never implemented.
    fn foreign_key_match(&mut self) -> Result<crate::ast::MatchType, ParseError> {
        use crate::ast::MatchType;
        let match_pos = self.peek_pos();
        if !self.eat_ident_eq("match") {
            return Ok(MatchType::Simple);
        }
        if self.eat_ident_eq("simple") {
            return Ok(MatchType::Simple);
        }
        if self.eat_ident_eq("full") || self.eat_keyword(Keyword::Full) {
            return Ok(MatchType::Full);
        }
        if self.eat_ident_eq("partial") {
            return Err(ParseError::new_sqlstate(
                "0A000",
                "MATCH PARTIAL not yet implemented",
                match_pos,
            ));
        }
        Err(ParseError::new(
            format!(
                "expected FULL, PARTIAL, or SIMPLE after MATCH, found {:?}",
                self.peek()
            ),
            self.peek_pos(),
        ))
    }

    /// One referential action body, positioned after `ON DELETE` / `ON UPDATE`.
    ///
    /// Returns the action and the `SET { NULL | DEFAULT } (col, …)` column list,
    /// which is empty unless one was written. `clause_pos` is the offset of the
    /// clause's `ON`, where `PostgreSQL` puts the caret for the column-list
    /// refusal.
    fn referential_action(
        &mut self,
        on_delete_side: bool,
        clause_pos: usize,
    ) -> Result<(crate::ast::ReferentialAction, Vec<String>), ParseError> {
        use crate::ast::ReferentialAction;
        if self.eat_ident_eq("set") || self.eat_keyword(Keyword::Set) {
            let action = if self.eat_keyword(Keyword::Null) {
                ReferentialAction::SetNull
            } else if self.eat_ident_eq("default") {
                ReferentialAction::SetDefault
            } else {
                return Err(ParseError::new(
                    "expected NULL or DEFAULT after SET in a referential action",
                    self.peek_pos(),
                ));
            };
            if *self.peek() != Token::LParen {
                return Ok((action, Vec::new()));
            }
            if !on_delete_side {
                return Err(ParseError::new_sqlstate(
                    "0A000",
                    format!(
                        "a column list with {} is only supported for ON DELETE actions",
                        action.as_sql()
                    ),
                    clause_pos,
                ));
            }
            return Ok((action, self.parse_ident_list()?));
        }
        if self.eat_ident_eq("cascade") {
            return Ok((ReferentialAction::Cascade, Vec::new()));
        }
        if self.eat_ident_eq("restrict") {
            return Ok((ReferentialAction::Restrict, Vec::new()));
        }
        if self.eat_ident_eq("no") {
            self.expect_ident_eq("action")?;
            return Ok((ReferentialAction::NoAction, Vec::new()));
        }
        Err(ParseError::new(
            format!("expected a referential action, found {:?}", self.peek()),
            self.peek_pos(),
        ))
    }

    /// `[NOT] DEFERRABLE`, `INITIALLY {DEFERRED|IMMEDIATE}`, `NOT VALID`,
    /// `NO INHERIT`, `ENFORCED`/`NOT ENFORCED`, in any order.
    ///
    /// The parser accepts and discards `NO INHERIT` and the `ENFORCED`
    /// spellings. The rest reach the AST. Each of the two mutually exclusive
    /// pairs may be written at most once, and `INITIALLY DEFERRED` alone implies
    /// `DEFERRABLE`. This parser reproduces all three of `PostgreSQL`'s `42601`
    /// refusals here word for word, so the returned struct can never claim a
    /// combination `PostgreSQL` rejects.
    ///
    /// `NOT VALID` belongs to `PostgreSQL`'s *table* constraint grammar only, so
    /// `allow_not_valid` is false for a column constraint. `NOT VALID` there is
    /// a syntax error, not a no-op the parser accepts without a message.
    fn eat_constraint_attributes(
        &mut self,
        allow_not_valid: bool,
    ) -> Result<crate::ast::ConstraintAttributes, ParseError> {
        let mut attributes = crate::ast::ConstraintAttributes::default();
        let mut saw_deferrability = false;
        let mut saw_initially = false;
        loop {
            let pos = self.peek_pos();
            if self.eat_ident_eq("deferrable") {
                if saw_deferrability {
                    return Err(multiple_constraint_attribute(
                        "DEFERRABLE/NOT DEFERRABLE",
                        pos,
                    ));
                }
                saw_deferrability = true;
                attributes.deferrable = true;
                continue;
            }
            if self.eat_ident_eq("enforced") {
                continue;
            }
            if self.eat_ident_eq("initially") {
                let word_pos = self.peek_pos();
                let deferred = if self.eat_ident_eq("deferred") {
                    true
                } else if self.eat_ident_eq("immediate") {
                    false
                } else {
                    return Err(ParseError::new(
                        format!(
                            "expected DEFERRED or IMMEDIATE after INITIALLY, found {:?}",
                            self.peek()
                        ),
                        word_pos,
                    ));
                };
                if saw_initially {
                    return Err(multiple_constraint_attribute(
                        "INITIALLY IMMEDIATE/DEFERRED",
                        pos,
                    ));
                }
                saw_initially = true;
                attributes.initially_deferred = deferred;
                if deferred {
                    if saw_deferrability && !attributes.deferrable {
                        return Err(initially_deferred_must_be_deferrable(pos));
                    }
                    attributes.deferrable = true;
                }
                continue;
            }
            if matches!(self.peek(), Token::Keyword(Keyword::Not))
                && matches!(self.peek2(), Token::Ident(s)
                    if s.eq_ignore_ascii_case("deferrable")
                        || s.eq_ignore_ascii_case("valid")
                        || s.eq_ignore_ascii_case("enforced"))
            {
                let valid =
                    matches!(self.peek2(), Token::Ident(s) if s.eq_ignore_ascii_case("valid"));
                if valid && !allow_not_valid {
                    self.bump();
                    return Err(ParseError::new(
                        "syntax error at or near \"VALID\"".to_string(),
                        self.peek_pos(),
                    ));
                }
                let deferrable =
                    matches!(self.peek2(), Token::Ident(s) if s.eq_ignore_ascii_case("deferrable"));
                self.bump();
                self.bump();
                attributes.not_valid |= valid;
                if deferrable {
                    if saw_deferrability {
                        return Err(multiple_constraint_attribute(
                            "DEFERRABLE/NOT DEFERRABLE",
                            pos,
                        ));
                    }
                    saw_deferrability = true;
                    attributes.deferrable = false;
                    if attributes.initially_deferred {
                        return Err(initially_deferred_must_be_deferrable(pos));
                    }
                }
                continue;
            }
            if matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("no"))
                && matches!(self.peek2(), Token::Ident(s) if s.eq_ignore_ascii_case("inherit"))
            {
                self.bump();
                self.bump();
                continue;
            }
            break;
        }
        Ok(attributes)
    }

    /// `( key [= value] [, …] )`: a storage-parameter list, accepted and
    /// discarded.
    fn storage_parameter_list(&mut self) -> Result<Vec<(String, Option<String>)>, ParseError> {
        self.expect(&Token::LParen)?;
        let mut params = Vec::new();
        loop {
            let mut key = self.expect_ident()?;
            if *self.peek() == Token::Dot {
                self.bump();
                key.push('.');
                key.push_str(&self.expect_ident()?);
            }
            let value = if *self.peek() == Token::Eq {
                self.bump();
                Some(self.storage_parameter_value()?)
            } else {
                None
            };
            params.push((key, value));
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        Ok(params)
    }

    fn parse_column_type(
        &mut self,
    ) -> Result<(crabka_pgtypes::ColumnType, Option<crate::ast::SerialKind>), ParseError> {
        let type_pos = self.peek_pos();
        let type_name = self.expect_ident()?;
        match type_name.as_str() {
            "serial" | "serial4" => Ok((
                crabka_pgtypes::ColumnType::Int4,
                Some(crate::ast::SerialKind::Serial),
            )),
            "bigserial" | "serial8" => Ok((
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
        use crate::ast::{ColumnConstraint, ColumnConstraintKind, IdentitySpec};
        let mut constraints = Vec::new();
        loop {
            let name = if matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("constraint"))
                && matches!(self.peek2(), Token::Ident(_))
                && starts_column_constraint_kind(self.peek3())
            {
                self.bump();
                Some(self.expect_ident()?)
            } else {
                None
            };
            let kind = if self.eat_keyword(Keyword::Not) {
                self.expect(&Token::Keyword(Keyword::Null))?;
                ColumnConstraintKind::NotNull
            } else if self.eat_keyword(Keyword::Null) {
                ColumnConstraintKind::Null
            } else if self.eat_ident_eq("default") {
                ColumnConstraintKind::Default(self.expr(0)?)
            } else if self.eat_ident_eq("primary") {
                self.expect_ident_eq("key")?;
                ColumnConstraintKind::PrimaryKey
            } else if self.eat_keyword(Keyword::Unique) {
                ColumnConstraintKind::Unique {
                    nulls_not_distinct: self.eat_nulls_not_distinct()?,
                }
            } else if self.eat_ident_eq("check") {
                ColumnConstraintKind::Check(self.check_predicate()?)
            } else if matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("references"))
            {
                ColumnConstraintKind::References(self.foreign_key_reference()?)
            } else if self.eat_ident_eq("generated") {
                self.generated_column_constraint()?
            } else if name.is_some() {
                return Err(ParseError::new(
                    format!("expected a column constraint, found {:?}", self.peek()),
                    self.peek_pos(),
                ));
            } else {
                break;
            };
            // An identity column's `GENERATED … AS IDENTITY` tail is followed by
            // sequence options, not by constraint attributes.
            let attributes = if matches!(kind, ColumnConstraintKind::Identity(IdentitySpec { .. }))
            {
                crate::ast::ConstraintAttributes::default()
            } else {
                self.eat_constraint_attributes(false)?
            };
            constraints.push(ColumnConstraint {
                name,
                kind,
                attributes,
            });
        }
        Ok(constraints)
    }

    /// `GENERATED { ALWAYS | BY DEFAULT } AS
    /// { IDENTITY [(opts)] | (expr) [STORED | VIRTUAL] }` — the `GENERATED`
    /// keyword is already consumed.
    ///
    /// A generation expression may only be `GENERATED ALWAYS`; `BY DEFAULT`
    /// belongs to identity columns alone, and `PostgreSQL` says so with its own
    /// 42601 message rather than a bare syntax error. Neither `STORED` nor
    /// `VIRTUAL` need be written: `PostgreSQL` 18 defaults to `VIRTUAL`.
    fn generated_column_constraint(
        &mut self,
    ) -> Result<crate::ast::ColumnConstraintKind, ParseError> {
        use crate::ast::{ColumnConstraintKind, GeneratedKind, GeneratedSpec, IdentitySpec};
        let when_pos = self.peek_pos();
        let always = if self.eat_ident_eq("always") {
            true
        } else if self.eat_keyword(Keyword::By) {
            self.expect_ident_eq("default")?;
            false
        } else {
            return Err(ParseError::new(
                "expected ALWAYS or BY DEFAULT after GENERATED",
                self.peek_pos(),
            ));
        };
        self.expect(&Token::Keyword(Keyword::As))?;
        if self.eat_ident_eq("identity") {
            let options = if *self.peek() == Token::LParen {
                self.bump();
                let options = self.sequence_options(&Token::RParen)?;
                self.expect(&Token::RParen)?;
                options
            } else {
                crate::ast::SequenceOptions::default()
            };
            return Ok(ColumnConstraintKind::Identity(IdentitySpec {
                always,
                options,
            }));
        }
        let predicate = self.check_predicate()?;
        let kind = if self.eat_ident_eq("stored") {
            GeneratedKind::Stored
        } else {
            // `VIRTUAL` is optional — it is what an unqualified generation
            // expression means — but it is still the only other word the
            // grammar accepts here, so anything else falls through to whatever
            // follows a column constraint and is rejected there.
            self.eat_ident_eq("virtual");
            GeneratedKind::Virtual
        };
        // Reported after the whole clause, at the `ALWAYS`/`BY DEFAULT` word,
        // the way `PostgreSQL`'s grammar action does.
        if !always {
            return Err(ParseError::new_sqlstate(
                "42601",
                "for a generated column, GENERATED ALWAYS must be specified",
                when_pos,
            ));
        }
        Ok(ColumnConstraintKind::Generated(GeneratedSpec {
            predicate,
            kind,
        }))
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
        let concurrently = self.eat_ident_eq("concurrently");
        let if_not_exists = self.eat_if_not_exists();
        // `CREATE INDEX ON t (…)` lets PostgreSQL choose the index name.
        let name = if matches!(self.peek(), Token::Keyword(Keyword::On)) {
            None
        } else {
            Some(self.relation_ref()?)
        };
        self.expect(&Token::Keyword(Keyword::On))?;
        // `ONLY` restricts the build to the named relation, which is what a
        // non-partitioned table does anyway.
        self.eat_ident_eq("only");
        let table = self.relation_ref()?;
        let method = if self.eat_keyword(Keyword::Using) {
            Some(self.expect_ident()?.to_ascii_lowercase())
        } else {
            None
        };
        let keys = self.index_key_list()?;
        let include = if self.eat_ident_eq("include") {
            self.parse_ident_list()?
        } else {
            Vec::new()
        };
        if self.eat_ident_eq("nulls") {
            let not = self.eat_keyword(Keyword::Not);
            self.expect(&Token::Keyword(Keyword::Distinct))?;
            let _ = not;
        }
        if self.eat_keyword(Keyword::With) {
            self.storage_parameter_list()?;
        }
        let tablespace = self
            .eat_ident_eq("tablespace")
            .then(|| self.expect_ident())
            .transpose()?;
        let predicate = if self.eat_keyword(Keyword::Where) {
            let start = self.peek_pos();
            self.expr(0)?;
            let end = self.peek_pos();
            Some(self.source[start..end].trim().to_string())
        } else {
            None
        };
        Ok(Statement::CreateIndex {
            name,
            table,
            keys,
            unique,
            placement,
            if_not_exists,
            concurrently,
            method,
            include,
            predicate,
            tablespace,
        })
    }

    /// `( key [COLLATE c] [opclass] [ASC|DESC] [NULLS {FIRST|LAST}] [, …] )`.
    fn index_key_list(&mut self) -> Result<Vec<crate::ast::IndexKey>, ParseError> {
        use crate::ast::IndexKey;
        self.expect(&Token::LParen)?;
        let mut keys = Vec::new();
        loop {
            let start = self.peek_pos();
            // A bare identifier followed by a key terminator or an identifier
            // clause (`COLLATE` / operator class) is a plain column reference;
            // anything else is an expression key.
            let column = if self.peek_col_id().is_some()
                && matches!(
                    self.peek2(),
                    Token::Comma
                        | Token::RParen
                        | Token::Ident(_)
                        | Token::Keyword(Keyword::Asc | Keyword::Desc)
                ) {
                Some(self.expect_col_id()?)
            } else {
                self.expr(0)?;
                None
            };
            let text_end = self.peek_pos();
            let mut text = self.source[start..text_end].trim().to_string();
            if self.eat_ident_eq("collate") {
                self.expect_ident()?;
            }
            // An operator-class name is a bare identifier in the one position
            // where nothing else can appear.
            let opclass = if matches!(self.peek(), Token::Ident(_))
                && !matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("nulls"))
            {
                Some(self.qualified_name_text()?)
            } else {
                None
            };
            let descending = if self.eat_keyword(Keyword::Desc) {
                true
            } else {
                self.eat_keyword(Keyword::Asc);
                false
            };
            let nulls_first = if self.eat_ident_eq("nulls") {
                if self.eat_ident_eq("first") {
                    Some(true)
                } else {
                    self.expect_ident_eq("last")?;
                    Some(false)
                }
            } else {
                None
            };
            if text.is_empty() {
                text = column.clone().unwrap_or_default();
            }
            keys.push(IndexKey {
                column,
                text,
                opclass,
                descending,
                nulls_first,
            });
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        if keys.is_empty() {
            return Err(ParseError::new(
                "CREATE INDEX requires at least one key",
                self.peek_pos(),
            ));
        }
        Ok(keys)
    }

    /// `COMMENT ON <kind> <name> IS { 'text' | NULL }`.
    fn comment_on(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("comment")?;
        self.expect(&Token::Keyword(Keyword::On))?;
        let kind_pos = self.peek_pos();
        let mut object_kind = match self.bump() {
            Token::Ident(word) => word.to_ascii_lowercase(),
            Token::Keyword(Keyword::Table) => "table".to_string(),
            Token::Keyword(Keyword::View) => "view".to_string(),
            Token::Keyword(Keyword::Index) => "index".to_string(),
            Token::Keyword(Keyword::Schema) => "schema".to_string(),
            Token::Keyword(Keyword::Server) => "server".to_string(),
            other => {
                return Err(ParseError::new(
                    format!("expected a COMMENT ON object kind, found {other:?}"),
                    kind_pos,
                ));
            }
        };
        // Multi-word object kinds: MATERIALIZED VIEW, FOREIGN TABLE, …
        while !matches!(self.peek(), Token::Ident(_) | Token::Keyword(Keyword::On))
            || matches!(self.peek(), Token::Keyword(_))
        {
            match self.peek() {
                Token::Keyword(Keyword::View) => object_kind.push_str(" view"),
                Token::Keyword(Keyword::Table) => object_kind.push_str(" table"),
                Token::Keyword(Keyword::Index) => object_kind.push_str(" index"),
                Token::Keyword(Keyword::Data) => object_kind.push_str(" data"),
                Token::Keyword(Keyword::Wrapper) => object_kind.push_str(" wrapper"),
                Token::Keyword(Keyword::Mapping) => object_kind.push_str(" mapping"),
                _ => break,
            }
            self.bump();
        }
        let mut object_name = self.expect_object_name()?;
        if *self.peek() == Token::Dot {
            self.bump();
            object_name.push('.');
            object_name.push_str(&self.expect_ident()?);
        }
        // A routine signature: COMMENT ON FUNCTION f(int) IS …
        if *self.peek() == Token::LParen {
            let mut depth = 0usize;
            loop {
                match self.bump() {
                    Token::LParen => depth += 1,
                    Token::RParen => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    Token::Eof => {
                        return Err(ParseError::new(
                            "unterminated argument list in COMMENT ON",
                            self.peek_pos(),
                        ));
                    }
                    _ => {}
                }
            }
        }
        self.expect(&Token::Keyword(Keyword::Is))?;
        let pos = self.peek_pos();
        let comment = match self.bump() {
            Token::StringLit(text) => Some(text),
            Token::Keyword(Keyword::Null) => None,
            other => {
                return Err(ParseError::new(
                    format!("expected a comment string or NULL, found {other:?}"),
                    pos,
                ));
            }
        };
        Ok(crate::ast::Statement::Comment {
            object_kind,
            object_name,
            comment,
        })
    }

    /// `CASCADE` / `RESTRICT`; returns true for `CASCADE`.
    fn eat_drop_behavior(&mut self) -> bool {
        if self.eat_ident_eq("cascade") {
            return true;
        }
        self.eat_ident_eq("restrict");
        false
    }

    fn trigger_timing(&mut self) -> Result<crate::ast::TriggerTiming, ParseError> {
        use crate::ast::TriggerTiming;
        if self.eat_ident_eq("before") {
            return Ok(TriggerTiming::Before);
        }
        if self.eat_ident_eq("after") {
            return Ok(TriggerTiming::After);
        }
        if self.eat_ident_eq("instead") {
            self.expect_ident_eq("of")?;
            return Ok(TriggerTiming::InsteadOf);
        }
        Err(ParseError::new(
            format!(
                "expected BEFORE, AFTER, or INSTEAD OF, found {:?}",
                self.peek()
            ),
            self.peek_pos(),
        ))
    }

    fn trigger_event(&mut self) -> Result<crate::ast::TriggerEvent, ParseError> {
        use crate::ast::TriggerEvent;
        if self.eat_keyword(Keyword::Insert) {
            return Ok(TriggerEvent::Insert);
        }
        if self.eat_keyword(Keyword::Update) {
            let mut columns = Vec::new();
            if self.eat_ident_eq("of") {
                columns.push(self.expect_object_name()?);
                while self.eat_comma() {
                    columns.push(self.expect_object_name()?);
                }
            }
            return Ok(TriggerEvent::Update { columns });
        }
        if self.eat_keyword(Keyword::Delete) {
            return Ok(TriggerEvent::Delete);
        }
        if self.eat_ident_eq("truncate") {
            return Ok(TriggerEvent::Truncate);
        }
        Err(ParseError::new(
            format!(
                "expected INSERT, UPDATE, DELETE, or TRUNCATE, found {:?}",
                self.peek()
            ),
            self.peek_pos(),
        ))
    }

    /// `CREATE [OR REPLACE] [CONSTRAINT] TRIGGER` in `PostgreSQL` 18's complete
    /// grammar. Cross-clause semantic restrictions are catalog-aware and are
    /// therefore validated by the executor.
    fn create_trigger(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{CreateTrigger, Statement, TriggerLevel, TriggerTransition};

        self.expect(&Token::Keyword(Keyword::Create))?;
        let or_replace = if self.eat_keyword(Keyword::Or) {
            self.expect_ident_eq("replace")?;
            true
        } else {
            false
        };
        let constraint = self.eat_ident_eq("constraint");
        self.expect_ident_eq("trigger")?;
        let name = self.expect_object_name()?;
        let timing = self.trigger_timing()?;
        let mut events = vec![self.trigger_event()?];
        while self.eat_keyword(Keyword::Or) {
            events.push(self.trigger_event()?);
        }
        self.expect(&Token::Keyword(Keyword::On))?;
        let table = self.relation_ref()?;
        let referenced_table = if self.eat_keyword(Keyword::From) {
            Some(self.relation_ref()?)
        } else {
            None
        };

        let mut deferrable = false;
        let mut initially_deferred = false;
        if self.eat_keyword(Keyword::Not) {
            self.expect_ident_eq("deferrable")?;
        } else if self.eat_ident_eq("deferrable") {
            deferrable = true;
        }
        if self.eat_ident_eq("initially") {
            if self.eat_ident_eq("deferred") {
                initially_deferred = true;
            } else {
                self.expect_ident_eq("immediate")?;
            }
        }

        let mut transitions = Vec::new();
        if self.eat_ident_eq("referencing") {
            loop {
                let old = if self.eat_ident_eq("old") {
                    true
                } else if self.eat_ident_eq("new") {
                    false
                } else {
                    break;
                };
                self.expect(&Token::Keyword(Keyword::Table))?;
                self.eat_keyword(Keyword::As);
                transitions.push(TriggerTransition {
                    old,
                    name: self.expect_object_name()?,
                });
            }
            if transitions.is_empty() {
                return Err(ParseError::new(
                    "expected OLD TABLE or NEW TABLE after REFERENCING",
                    self.peek_pos(),
                ));
            }
        }

        let level = if self.eat_keyword(Keyword::For) {
            self.eat_ident_eq("each");
            if self.eat_ident_eq("row") {
                TriggerLevel::Row
            } else {
                self.expect_ident_eq("statement")?;
                TriggerLevel::Statement
            }
        } else {
            TriggerLevel::Statement
        };
        let (when, when_source) = if self.eat_keyword(Keyword::When) {
            self.expect(&Token::LParen)?;
            let start = self.peek_pos();
            let condition = self.expr(0)?;
            let end = self.peek_pos();
            self.expect(&Token::RParen)?;
            (
                Some(condition),
                Some(self.source[start..end].trim().to_string()),
            )
        } else {
            (None, None)
        };

        self.expect_ident_eq("execute")?;
        if !self.eat_ident_eq("function") {
            self.expect_ident_eq("procedure")?;
        }
        let function = self.qualified_name_text()?;
        self.expect(&Token::LParen)?;
        let mut arguments = Vec::new();
        if *self.peek() != Token::RParen {
            loop {
                let position = self.peek_pos();
                match self.bump() {
                    Token::StringLit(value)
                    | Token::Ident(value)
                    | Token::IntLit(value)
                    | Token::FloatLit(value) => arguments.push(value),
                    Token::Minus => match self.bump() {
                        Token::IntLit(value) | Token::FloatLit(value) => {
                            arguments.push(format!("-{value}"));
                        }
                        other => {
                            return Err(ParseError::new(
                                format!("expected numeric trigger argument, found {other:?}"),
                                position,
                            ));
                        }
                    },
                    other => {
                        return Err(ParseError::new(
                            format!("trigger arguments must be string literals, found {other:?}"),
                            position,
                        ));
                    }
                }
                if !self.eat_comma() {
                    break;
                }
            }
        }
        self.expect(&Token::RParen)?;

        Ok(Statement::CreateTrigger(CreateTrigger {
            name,
            or_replace,
            constraint,
            timing,
            events,
            table,
            referenced_table,
            deferrable,
            initially_deferred,
            transitions,
            level,
            when,
            when_source,
            function,
            arguments,
        }))
    }

    /// A parenthesised policy qual, captured both parsed and as written.
    ///
    /// The catalog stores the source text — that is what keeps a parser out of
    /// it — and the enforcement path evaluates the parsed form, so both come
    /// out of one production rather than the text being re-derived later.
    fn policy_qual(&mut self) -> Result<crate::ast::PolicyQual, ParseError> {
        self.expect(&Token::LParen)?;
        let start = self.peek_pos();
        let expr = self.expr(0)?;
        let end = self.peek_pos();
        self.expect(&Token::RParen)?;
        Ok(crate::ast::PolicyQual {
            expr,
            source: self.source[start..end].trim().to_string(),
        })
    }

    /// `TO role[, …]`. `PUBLIC` collapses to the empty list, which is how both
    /// `PostgreSQL` and the catalog encode "every role".
    fn policy_roles(&mut self) -> Result<Vec<String>, ParseError> {
        let named = self.object_name_list()?;
        if named.iter().any(|role| role == "public") {
            return Ok(Vec::new());
        }
        Ok(named)
    }

    /// `[USING (expr)] [WITH CHECK (expr)]`, in either order — `PostgreSQL`
    /// fixes the order, but accepting both costs nothing and neither clause is
    /// ambiguous.
    fn policy_quals(
        &mut self,
    ) -> Result<
        (
            Option<crate::ast::PolicyQual>,
            Option<crate::ast::PolicyQual>,
        ),
        ParseError,
    > {
        let mut using = None;
        let mut with_check = None;
        loop {
            if using.is_none() && self.eat_keyword(Keyword::Using) {
                using = Some(self.policy_qual()?);
                continue;
            }
            if with_check.is_none() && self.eat_keyword(Keyword::With) {
                self.expect_ident_eq("check")?;
                with_check = Some(self.policy_qual()?);
                continue;
            }
            break;
        }
        Ok((using, with_check))
    }

    /// `CREATE POLICY name ON table [AS {PERMISSIVE|RESTRICTIVE}]
    /// [FOR {ALL|SELECT|INSERT|UPDATE|DELETE}] [TO role[, …]]
    /// [USING (expr)] [WITH CHECK (expr)]`.
    fn create_policy(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{CreatePolicy, PolicyCommand, Statement};

        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect_ident_eq("policy")?;
        let name = self.expect_object_name()?;
        self.expect(&Token::Keyword(Keyword::On))?;
        let table = self.relation_ref()?;

        let permissive = if self.eat_keyword(Keyword::As) {
            let position = self.peek_pos();
            let kind = self.expect_ident()?;
            match kind.to_ascii_lowercase().as_str() {
                "permissive" => true,
                "restrictive" => false,
                other => {
                    return Err(ParseError::new(
                        format!("unrecognized row security option \"{other}\""),
                        position,
                    ));
                }
            }
        } else {
            true
        };

        let command = if self.eat_keyword(Keyword::For) {
            self.policy_command()?
        } else {
            PolicyCommand::All
        };

        let roles = if self.eat_keyword(Keyword::To) {
            self.policy_roles()?
        } else {
            Vec::new()
        };

        let (using, with_check) = self.policy_quals()?;
        Ok(Statement::CreatePolicy(CreatePolicy {
            name,
            table,
            permissive,
            command,
            roles,
            using,
            with_check,
        }))
    }

    fn policy_command(&mut self) -> Result<crate::ast::PolicyCommand, ParseError> {
        use crate::ast::PolicyCommand;
        let position = self.peek_pos();
        if self.eat_keyword(Keyword::All) {
            return Ok(PolicyCommand::All);
        }
        if self.eat_keyword(Keyword::Select) {
            return Ok(PolicyCommand::Select);
        }
        if self.eat_keyword(Keyword::Insert) {
            return Ok(PolicyCommand::Insert);
        }
        if self.eat_keyword(Keyword::Update) {
            return Ok(PolicyCommand::Update);
        }
        if self.eat_keyword(Keyword::Delete) {
            return Ok(PolicyCommand::Delete);
        }
        Err(ParseError::new(
            format!(
                "unrecognized policy command, expected ALL, SELECT, INSERT, UPDATE or DELETE, found {:?}",
                self.peek()
            ),
            position,
        ))
    }

    /// `ALTER POLICY name ON table {RENAME TO new | [TO roles] [USING (e)]
    /// [WITH CHECK (e)]}`.
    fn alter_policy(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{AlterPolicyAction, AlterPolicyChange, Statement};

        self.expect_ident_eq("alter")?;
        self.expect_ident_eq("policy")?;
        let name = self.expect_object_name()?;
        self.expect(&Token::Keyword(Keyword::On))?;
        let table = self.relation_ref()?;
        let action = if self.eat_ident_eq("rename") {
            self.expect_keyword_or_ident(Keyword::To, "to")?;
            AlterPolicyAction::RenameTo(self.expect_object_name()?)
        } else {
            let roles = if self.eat_keyword(Keyword::To) {
                Some(self.policy_roles()?)
            } else {
                None
            };
            let (using, with_check) = self.policy_quals()?;
            AlterPolicyAction::Change(Box::new(AlterPolicyChange {
                roles,
                using,
                with_check,
            }))
        };
        Ok(Statement::AlterPolicy {
            name,
            table,
            action,
        })
    }

    /// `DROP POLICY [IF EXISTS] name ON table [CASCADE | RESTRICT]`.
    fn drop_policy(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect_ident_eq("policy")?;
        let if_exists = self.eat_if_exists()?;
        let name = self.expect_object_name()?;
        self.expect(&Token::Keyword(Keyword::On))?;
        let table = self.relation_ref()?;
        let cascade = self.eat_drop_behavior();
        Ok(Statement::DropPolicy {
            name,
            table,
            if_exists,
            cascade,
        })
    }

    fn alter_trigger(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{AlterTriggerAction, Statement};
        self.expect_ident_eq("alter")?;
        self.expect_ident_eq("trigger")?;
        let name = self.expect_object_name()?;
        self.expect(&Token::Keyword(Keyword::On))?;
        let table = self.relation_ref()?;
        let action = if self.eat_ident_eq("rename") {
            self.expect(&Token::Keyword(Keyword::To))?;
            AlterTriggerAction::RenameTo(self.expect_object_name()?)
        } else {
            let dependent = !self.eat_ident_eq("no");
            self.expect_ident_eq("depends")?;
            self.expect(&Token::Keyword(Keyword::On))?;
            self.expect_ident_eq("extension")?;
            AlterTriggerAction::DependsOnExtension {
                extension: self.expect_object_name()?,
                dependent,
            }
        };
        Ok(Statement::AlterTrigger {
            name,
            table,
            action,
        })
    }

    fn drop_trigger(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect_ident_eq("trigger")?;
        let if_exists = self.eat_if_exists()?;
        let name = self.expect_object_name()?;
        self.expect(&Token::Keyword(Keyword::On))?;
        let table = self.relation_ref()?;
        let cascade = self.eat_drop_behavior();
        Ok(Statement::DropTrigger {
            name,
            table,
            if_exists,
            cascade,
        })
    }

    fn event_trigger_event(&mut self) -> Result<crate::ast::EventTriggerEvent, ParseError> {
        use crate::ast::EventTriggerEvent;
        let position = self.peek_pos();
        let event = self.expect_ident()?;
        match event.as_str() {
            "login" => Ok(EventTriggerEvent::Login),
            "ddl_command_start" => Ok(EventTriggerEvent::DdlCommandStart),
            "ddl_command_end" => Ok(EventTriggerEvent::DdlCommandEnd),
            "sql_drop" => Ok(EventTriggerEvent::SqlDrop),
            "table_rewrite" => Ok(EventTriggerEvent::TableRewrite),
            _ => Err(ParseError::new(
                format!("unrecognized event name \"{event}\""),
                position,
            )),
        }
    }

    fn create_event_trigger(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{CreateEventTrigger, EventTriggerFilter, Statement};
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect_ident_eq("event")?;
        self.expect_ident_eq("trigger")?;
        let name = self.expect_object_name()?;
        self.expect(&Token::Keyword(Keyword::On))?;
        let event = self.event_trigger_event()?;
        let mut filters = Vec::new();
        if self.eat_keyword(Keyword::When) {
            loop {
                let variable = self.expect_object_name()?;
                self.expect(&Token::Keyword(Keyword::In))?;
                self.expect(&Token::LParen)?;
                let mut values = Vec::new();
                loop {
                    let position = self.peek_pos();
                    match self.bump() {
                        Token::StringLit(value) => values.push(value),
                        other => {
                            return Err(ParseError::new(
                                format!(
                                    "event trigger filter values must be string literals, found {other:?}"
                                ),
                                position,
                            ));
                        }
                    }
                    if !self.eat_comma() {
                        break;
                    }
                }
                self.expect(&Token::RParen)?;
                filters.push(EventTriggerFilter { variable, values });
                if !self.eat_keyword(Keyword::And) {
                    break;
                }
            }
        }
        self.expect_ident_eq("execute")?;
        if !self.eat_ident_eq("function") {
            self.expect_ident_eq("procedure")?;
        }
        let function = self.qualified_name_text()?;
        self.expect(&Token::LParen)?;
        self.expect(&Token::RParen)?;
        Ok(Statement::CreateEventTrigger(CreateEventTrigger {
            name,
            event,
            filters,
            function,
        }))
    }

    fn alter_event_trigger(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{AlterEventTriggerAction, Statement, TriggerEnableMode};
        self.expect_ident_eq("alter")?;
        self.expect_ident_eq("event")?;
        self.expect_ident_eq("trigger")?;
        let name = self.expect_object_name()?;
        let action = if self.eat_ident_eq("disable") {
            AlterEventTriggerAction::Enable(TriggerEnableMode::Disabled)
        } else if self.eat_ident_eq("enable") {
            let mode = if self.eat_ident_eq("replica") {
                TriggerEnableMode::Replica
            } else if self.eat_ident_eq("always") {
                TriggerEnableMode::Always
            } else {
                TriggerEnableMode::Origin
            };
            AlterEventTriggerAction::Enable(mode)
        } else if self.eat_ident_eq("owner") {
            self.expect(&Token::Keyword(Keyword::To))?;
            AlterEventTriggerAction::OwnerTo(self.expect_object_name()?)
        } else {
            self.expect_ident_eq("rename")?;
            self.expect(&Token::Keyword(Keyword::To))?;
            AlterEventTriggerAction::RenameTo(self.expect_object_name()?)
        };
        Ok(Statement::AlterEventTrigger { name, action })
    }

    fn drop_event_trigger(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect_ident_eq("event")?;
        self.expect_ident_eq("trigger")?;
        let if_exists = self.eat_if_exists()?;
        let name = self.expect_object_name()?;
        let cascade = self.eat_drop_behavior();
        Ok(Statement::DropEventTrigger {
            name,
            if_exists,
            cascade,
        })
    }

    fn create_view(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;

        self.expect(&Token::Keyword(Keyword::Create))?;
        let or_replace = if self.eat_keyword(Keyword::Or) {
            self.expect_ident_eq("replace")?;
            true
        } else {
            false
        };
        let temporary = self.eat_ident_eq("temp") || self.eat_ident_eq("temporary");
        self.expect(&Token::Keyword(Keyword::View))?;
        let name = self.relation_ref()?;
        // `VIEW name (a, b, c)` renames the query's output columns positionally.
        let columns = self.opt_column_aliases()?;
        let options = self.view_options()?;
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
            or_replace,
            temporary,
            columns,
            options,
        })
    }

    /// The optional `WITH (…)` reloption list on `CREATE VIEW`.
    ///
    /// `PostgreSQL` gives a view three reloptions. Two are recorded —
    /// `security_invoker` and `security_barrier` — and `check_option` is
    /// accepted and dropped, because nothing here enforces a view's `WITH CHECK
    /// OPTION` yet. Anything else is the same `unrecognized parameter` refusal
    /// `PostgreSQL` raises, rather than a silent acceptance that would make a
    /// misspelled `security_barrier` look like it took effect.
    fn view_options(&mut self) -> Result<crate::ast::ViewOptions, ParseError> {
        let mut options = crate::ast::ViewOptions::default();
        if !self.eat_keyword(Keyword::With) {
            return Ok(options);
        }
        for (name, value) in self.view_option_settings()? {
            match name {
                crate::ast::ViewOptionName::SecurityInvoker => options.security_invoker = value,
                crate::ast::ViewOptionName::SecurityBarrier => options.security_barrier = value,
                crate::ast::ViewOptionName::CheckOption => {}
            }
        }
        Ok(options)
    }

    /// A parenthesized `(name [= value], …)` reloption list, as `CREATE VIEW …
    /// WITH (…)` and `ALTER VIEW … SET (…)` both write it.
    ///
    /// Shared so the two spellings cannot drift: an option `CREATE VIEW`
    /// refuses must not be one `ALTER VIEW` silently accepts.
    fn view_option_settings(
        &mut self,
    ) -> Result<Vec<(crate::ast::ViewOptionName, bool)>, ParseError> {
        self.expect(&Token::LParen)?;
        let mut settings = Vec::new();
        loop {
            let name = self.view_option_name()?;
            // `check_option` is an enum, not a boolean, so its value is taken
            // and dropped rather than run through `parse_bool`.
            let value = if name == crate::ast::ViewOptionName::CheckOption {
                if *self.peek() == Token::Eq {
                    self.bump();
                    self.expect_ident()?;
                }
                false
            } else {
                self.reloption_bool()?
            };
            settings.push((name, value));
            if !self.eat_comma() {
                break;
            }
        }
        self.expect(&Token::RParen)?;
        Ok(settings)
    }

    /// One reloption name, refusing anything a view does not have.
    fn view_option_name(&mut self) -> Result<crate::ast::ViewOptionName, ParseError> {
        use crate::ast::ViewOptionName;

        let start = self.peek_pos();
        let name = self.expect_ident()?.to_ascii_lowercase();
        match name.as_str() {
            "security_invoker" => Ok(ViewOptionName::SecurityInvoker),
            "security_barrier" => Ok(ViewOptionName::SecurityBarrier),
            "check_option" => Ok(ViewOptionName::CheckOption),
            other => Err(ParseError::new_sqlstate(
                "22023",
                format!("unrecognized parameter \"{other}\""),
                start,
            )),
        }
    }

    /// `ALTER VIEW [IF EXISTS] name { OWNER TO role | SET (…) | RESET (…) }`.
    ///
    /// The three `PostgreSQL` subcommands this engine can act on. `RENAME TO`,
    /// `SET SCHEMA` and the `ALTER COLUMN … SET DEFAULT` family reach the
    /// syntax error below rather than being consumed and ignored, so a
    /// statement that would change a view's identity never looks like it
    /// succeeded.
    fn alter_view(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::AlterViewAction;

        self.expect_ident_eq("alter")?;
        self.expect(&Token::Keyword(Keyword::View))?;
        let if_exists = self.eat_if_exists()?;
        let name = self.relation_ref()?;
        if self.eat_ident_eq("owner") {
            self.expect_keyword_or_ident(Keyword::To, "to")?;
            return Ok(crate::ast::Statement::AlterView {
                name,
                if_exists,
                action: AlterViewAction::OwnerTo(self.expect_object_name()?),
            });
        }
        if self.eat_keyword(Keyword::Set) {
            return Ok(crate::ast::Statement::AlterView {
                name,
                if_exists,
                action: AlterViewAction::SetOptions(self.view_option_settings()?),
            });
        }
        if self.eat_ident_eq("reset") {
            self.expect(&Token::LParen)?;
            let mut names = Vec::new();
            loop {
                names.push(self.view_option_name()?);
                if !self.eat_comma() {
                    break;
                }
            }
            self.expect(&Token::RParen)?;
            return Ok(crate::ast::Statement::AlterView {
                name,
                if_exists,
                action: AlterViewAction::ResetOptions(names),
            });
        }
        Err(ParseError::new(
            format!(
                "unsupported ALTER VIEW subcommand, found {:?}; \
                 expected OWNER TO, SET (…) or RESET (…)",
                self.peek()
            ),
            self.peek_pos(),
        ))
    }

    /// A boolean reloption's value. A bare name is `true`, which is how
    /// `WITH (security_barrier)` reads; `= <word>` takes `PostgreSQL`'s
    /// `parse_bool` spellings.
    fn reloption_bool(&mut self) -> Result<bool, ParseError> {
        if *self.peek() != Token::Eq {
            return Ok(true);
        }
        self.bump();
        let start = self.peek_pos();
        let written = match self.bump() {
            Token::Ident(word) => word,
            Token::Keyword(Keyword::True) => "true".into(),
            Token::Keyword(Keyword::False) => "false".into(),
            // `on` is a keyword to this lexer (`ON` joins); `off` is not.
            Token::Keyword(Keyword::On) => "on".into(),
            Token::IntLit(digits) => digits,
            other => {
                return Err(ParseError::new(
                    format!("expected a boolean reloption value, found {other:?}"),
                    start,
                ));
            }
        };
        match written.to_ascii_lowercase().as_str() {
            "true" | "on" | "yes" | "1" => Ok(true),
            "false" | "off" | "no" | "0" => Ok(false),
            other => Err(ParseError::new_sqlstate(
                "22023",
                format!("invalid value for boolean option: \"{other}\""),
                start,
            )),
        }
    }

    fn create_sequence(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.eat_ident_eq("temp");
        self.eat_ident_eq("temporary");
        self.expect_ident_eq("sequence")?;
        let if_not_exists = self.eat_if_not_exists();
        let name = self.relation_ref()?;
        let options = self.sequence_options(&Token::Semicolon)?;
        Ok(Statement::CreateIndex {
            name: Some(name),
            table: crate::ast::RelationRef::bare("__crabka_sequence__"),
            keys: encode_sequence_options(&options),
            unique: false,
            placement: crate::ast::IndexPlacement::Local,
            if_not_exists,
            concurrently: false,
            method: None,
            include: Vec::new(),
            predicate: None,
            tablespace: None,
        })
    }

    /// The shared `CREATE SEQUENCE` / identity-column option list, ending at
    /// `terminator` (or end of statement).
    fn sequence_options(
        &mut self,
        terminator: &Token,
    ) -> Result<crate::ast::SequenceOptions, ParseError> {
        use crate::ast::SequenceOptions;
        let mut options = SequenceOptions::default();
        while !matches!(self.peek(), Token::Semicolon | Token::Eof) && self.peek() != terminator {
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
            } else if self.eat_ident_eq("as") || self.eat_keyword(Keyword::As) {
                // `AS <integer type>` bounds the sequence range; the executor
                // clamps to int8 either way.
                self.parse_type_name()?;
            } else if self.eat_ident_eq("owned") {
                self.expect_ident_eq("by")?;
                if !self.eat_ident_eq("none") {
                    self.expect_ident()?;
                    if *self.peek() == Token::Dot {
                        self.bump();
                        self.expect_ident()?;
                    }
                }
            } else if self.eat_ident_eq("sequence") {
                self.expect_ident_eq("name")?;
                self.expect_ident()?;
            } else {
                return Err(ParseError::new(
                    format!("unexpected sequence option {:?}", self.peek()),
                    self.peek_pos(),
                ));
            }
        }
        Ok(options)
    }

    /// One `DROP SEQUENCE` name, tagged so the shared `DROP TABLE` arm knows a
    /// sequence was meant. The tag sits in the relation's own name, so any
    /// schema qualifier stays where the resolver can still see it.
    fn sequence_drop_ref(&mut self) -> Result<crate::ast::RelationRef, ParseError> {
        let mut reference = self.relation_ref()?;
        reference.name = format!("__crabka_sequence__:{}", reference.name);
        Ok(reference)
    }

    fn drop_sequence(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect_ident_eq("sequence")?;
        let if_exists = self.eat_if_exists()?;
        let mut names = vec![self.sequence_drop_ref()?];
        while *self.peek() == Token::Comma {
            self.bump();
            names.push(self.sequence_drop_ref()?);
        }
        let cascade = self.eat_drop_behavior();
        Ok(crate::ast::Statement::DropTable {
            names,
            if_exists,
            cascade,
        })
    }

    // ---------------------------------------------------------------------
    // T5: user-defined types. `CREATE`/`ALTER`/`DROP` of `TYPE` and `DOMAIN`.
    // `type` and `domain` are plain lowercased idents in this lexer, so
    // neither becomes reserved by being recognised here.
    // ---------------------------------------------------------------------

    fn create_tablespace(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect_ident_eq("tablespace")?;
        let name = self.expect_object_name()?;
        let owner = if self.eat_ident_eq("owner") {
            Some(self.expect_object_name()?)
        } else {
            None
        };
        self.expect_ident_eq("location")?;
        let location = self.expect_string_lit()?;
        let mut options = Vec::new();
        if self.eat_keyword(Keyword::With) {
            self.expect(&Token::LParen)?;
            loop {
                let option = self.expect_ident()?;
                self.expect(&Token::Eq)?;
                let value = self.storage_parameter_value()?;
                options.push((option, value));
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
        }
        Ok(crate::ast::Statement::Utility(
            crate::ast::UtilityStatement::CreateTablespace {
                name,
                owner,
                location,
                options,
            },
        ))
    }

    fn drop_tablespace(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect_ident_eq("tablespace")?;
        let if_exists = self.eat_if_exists()?;
        let name = self.expect_object_name()?;
        Ok(crate::ast::Statement::Utility(
            crate::ast::UtilityStatement::DropTablespace { name, if_exists },
        ))
    }

    fn alter_tablespace(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::TablespaceAlterAction;

        self.expect_ident_eq("alter")?;
        self.expect_ident_eq("tablespace")?;
        let name = self.expect_object_name()?;
        let action = if self.eat_keyword(Keyword::Set) {
            self.expect(&Token::LParen)?;
            let mut options = Vec::new();
            loop {
                let option = self.expect_ident()?;
                self.expect(&Token::Eq)?;
                options.push((option, self.storage_parameter_value()?));
                if !self.eat_comma() {
                    break;
                }
            }
            self.expect(&Token::RParen)?;
            TablespaceAlterAction::Set(options)
        } else if self.eat_ident_eq("reset") {
            self.expect(&Token::LParen)?;
            let mut options = Vec::new();
            loop {
                options.push(self.expect_ident()?);
                if !self.eat_comma() {
                    break;
                }
            }
            self.expect(&Token::RParen)?;
            TablespaceAlterAction::Reset(options)
        } else if self.eat_ident_eq("rename") {
            self.expect_keyword_or_ident(Keyword::To, "to")?;
            TablespaceAlterAction::RenameTo(self.expect_object_name()?)
        } else if self.eat_ident_eq("owner") {
            self.expect_keyword_or_ident(Keyword::To, "to")?;
            TablespaceAlterAction::OwnerTo(self.expect_object_name()?)
        } else {
            return Err(ParseError::new(
                "expected SET, RESET, RENAME or OWNER",
                self.peek_pos(),
            ));
        };
        Ok(crate::ast::Statement::Utility(
            crate::ast::UtilityStatement::AlterTablespace { name, action },
        ))
    }

    fn create_operator_class(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect_ident_eq("operator")?;
        self.expect_ident_eq("class")?;
        let name = self.relation_ref()?;
        let default = self.eat_ident_eq("default");
        self.expect_keyword_or_ident(Keyword::For, "for")?;
        self.expect_ident_eq("type")?;
        let input_type = self.parse_type_name()?;
        self.expect_keyword_or_ident(Keyword::Using, "using")?;
        let method = self.expect_object_name()?;
        let family = self
            .eat_ident_eq("family")
            .then(|| self.relation_ref())
            .transpose()?;
        self.expect_keyword_or_ident(Keyword::As, "as")?;
        // Each member is already validated when its referenced operator or
        // support function is used. Keep the DDL boundary strict (non-empty,
        // comma-separated) without duplicating those parsers here.
        let mut member_tokens = 0usize;
        let mut key_type = None;
        while !matches!(self.peek(), Token::Semicolon | Token::Eof) {
            if self.eat_ident_eq("storage") {
                key_type = Some(self.parse_type_name()?);
            } else {
                self.bump();
            }
            member_tokens += 1;
        }
        if member_tokens == 0 {
            return Err(ParseError::new(
                "operator class requires at least one member",
                self.peek_pos(),
            ));
        }
        Ok(crate::ast::Statement::Utility(
            crate::ast::UtilityStatement::CreateOperatorClass {
                name,
                default,
                input_type,
                method,
                family,
                key_type,
            },
        ))
    }

    fn create_operator_family(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect_ident_eq("operator")?;
        self.expect_ident_eq("family")?;
        let name = self.relation_ref()?;
        self.expect_keyword_or_ident(Keyword::Using, "using")?;
        Ok(crate::ast::Statement::Utility(
            crate::ast::UtilityStatement::CreateOperatorFamily {
                name,
                method: self.expect_object_name()?,
            },
        ))
    }

    fn alter_operator_object(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{OperatorObjectAlterAction, OperatorObjectKind, UtilityStatement};

        self.expect_ident_eq("alter")?;
        self.expect_ident_eq("operator")?;
        let kind = if self.eat_ident_eq("class") {
            OperatorObjectKind::Class
        } else {
            self.expect_ident_eq("family")?;
            OperatorObjectKind::Family
        };
        let name = self.relation_ref()?;
        self.expect_keyword_or_ident(Keyword::Using, "using")?;
        let method = self.expect_object_name()?;
        let action = if kind == OperatorObjectKind::Family && self.eat_ident_eq("add") {
            OperatorObjectAlterAction::AddMembers(self.operator_family_add_members()?)
        } else if kind == OperatorObjectKind::Family
            && (self.eat_keyword(Keyword::Drop) || self.eat_ident_eq("drop"))
        {
            OperatorObjectAlterAction::DropMembers(self.operator_family_drop_members()?)
        } else if self.eat_ident_eq("rename") {
            self.expect_keyword_or_ident(Keyword::To, "to")?;
            OperatorObjectAlterAction::RenameTo(self.expect_object_name()?)
        } else if self.eat_ident_eq("owner") {
            self.expect_keyword_or_ident(Keyword::To, "to")?;
            OperatorObjectAlterAction::OwnerTo(self.expect_object_name()?)
        } else if self.eat_keyword(Keyword::Set) {
            self.expect_keyword_or_ident(Keyword::Schema, "schema")?;
            OperatorObjectAlterAction::SetSchema(self.expect_object_name()?)
        } else {
            return Err(ParseError::new(
                "expected ADD, DROP, RENAME, OWNER or SET SCHEMA",
                self.peek_pos(),
            ));
        };
        Ok(crate::ast::Statement::Utility(
            UtilityStatement::AlterOperatorObject {
                kind,
                name,
                method,
                action,
            },
        ))
    }

    fn operator_family_add_members(
        &mut self,
    ) -> Result<Vec<crate::ast::OperatorFamilyMember>, ParseError> {
        use crate::ast::OperatorFamilyMember;

        let mut members = Vec::new();
        loop {
            let member = if self.eat_ident_eq("operator") {
                let number = self.expect_u16("operator number")?;
                let position = self.peek_pos();
                let token = self.bump();
                let operator = operator_spelling(&token)
                    .ok_or_else(|| ParseError::new("expected operator name", position))?
                    .to_string();
                if *self.peek() != Token::LParen {
                    return Err(ParseError::new_sqlstate(
                        "42601",
                        "operator argument types must be specified in ALTER OPERATOR FAMILY",
                        self.peek_pos(),
                    ));
                }
                let (left_type, right_type) = self.operator_family_type_pair(false)?;
                let order_family = if self.eat_keyword(Keyword::For) {
                    self.expect_keyword_or_ident(Keyword::Order, "order")?;
                    self.expect_keyword_or_ident(Keyword::By, "by")?;
                    Some(self.relation_ref()?)
                } else {
                    None
                };
                OperatorFamilyMember::Operator {
                    number,
                    operator,
                    left_type,
                    right_type,
                    order_family,
                }
            } else if self.eat_ident_eq("function") {
                let number = self.expect_u16("function number")?;
                let (left_type, right_type) = if *self.peek() == Token::LParen {
                    let (left, right) = self.operator_family_type_pair(true)?;
                    (Some(left), Some(right))
                } else {
                    (None, None)
                };
                let function = self.relation_ref()?;
                let argument_types = self.operator_family_type_list()?;
                OperatorFamilyMember::Function {
                    number,
                    left_type,
                    right_type,
                    function,
                    argument_types,
                }
            } else if self.eat_ident_eq("storage") {
                return Err(ParseError::new_sqlstate(
                    "42601",
                    "STORAGE cannot be specified in ALTER OPERATOR FAMILY",
                    self.peek_pos(),
                ));
            } else {
                return Err(ParseError::new(
                    "expected OPERATOR or FUNCTION",
                    self.peek_pos(),
                ));
            };
            members.push(member);
            if !self.eat_comma() {
                break;
            }
        }
        Ok(members)
    }

    fn operator_family_drop_members(
        &mut self,
    ) -> Result<Vec<crate::ast::OperatorFamilyMemberKey>, ParseError> {
        use crate::ast::OperatorFamilyMemberKey;

        let mut members = Vec::new();
        loop {
            let operator = self.eat_ident_eq("operator");
            if !operator {
                self.expect_ident_eq("function")?;
            }
            let number = self.expect_u16(if operator {
                "operator number"
            } else {
                "function number"
            })?;
            let (left_type, right_type) = self.operator_family_type_pair(true)?;
            members.push(if operator {
                OperatorFamilyMemberKey::Operator {
                    number,
                    left_type,
                    right_type,
                }
            } else {
                OperatorFamilyMemberKey::Function {
                    number,
                    left_type,
                    right_type,
                }
            });
            if !self.eat_comma() {
                break;
            }
        }
        Ok(members)
    }

    fn operator_family_type_pair(
        &mut self,
        allow_one: bool,
    ) -> Result<(crabka_pgtypes::ColumnType, crabka_pgtypes::ColumnType), ParseError> {
        self.expect(&Token::LParen)?;
        let left = self.parse_type_name()?;
        let right = if self.eat_comma() {
            let right = self.parse_type_name()?;
            if self.eat_comma() {
                return Err(ParseError::new_sqlstate(
                    "42601",
                    "one or two argument types must be specified",
                    self.peek_pos(),
                ));
            }
            right
        } else if allow_one {
            left
        } else {
            return Err(ParseError::new_sqlstate(
                "42601",
                "operator argument types must be specified in ALTER OPERATOR FAMILY",
                self.peek_pos(),
            ));
        };
        self.expect(&Token::RParen)?;
        Ok((left, right))
    }

    fn operator_family_type_list(
        &mut self,
    ) -> Result<Vec<crate::ast::OperatorFamilyFunctionType>, ParseError> {
        use crate::ast::OperatorFamilyFunctionType;

        self.expect(&Token::LParen)?;
        let mut types = Vec::new();
        if *self.peek() != Token::RParen {
            loop {
                types.push(if self.eat_ident_eq("internal") {
                    OperatorFamilyFunctionType::Internal
                } else {
                    OperatorFamilyFunctionType::Builtin(self.parse_type_name()?)
                });
                if !self.eat_comma() {
                    break;
                }
            }
        }
        self.expect(&Token::RParen)?;
        Ok(types)
    }

    fn drop_operator_object(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{OperatorObjectKind, UtilityStatement};

        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect_ident_eq("operator")?;
        let kind = if self.eat_ident_eq("class") {
            OperatorObjectKind::Class
        } else {
            self.expect_ident_eq("family")?;
            OperatorObjectKind::Family
        };
        let if_exists = self.eat_if_exists()?;
        let name = self.relation_ref()?;
        self.expect_keyword_or_ident(Keyword::Using, "using")?;
        let method = self.expect_object_name()?;
        let cascade = if self.eat_ident_eq("cascade") {
            true
        } else {
            self.eat_ident_eq("restrict");
            false
        };
        Ok(crate::ast::Statement::Utility(
            UtilityStatement::DropOperatorObject {
                kind,
                name,
                method,
                if_exists,
                cascade,
            },
        ))
    }

    /// `CREATE TYPE name [ AS { (field type, …) | ENUM (…) | RANGE (…) } ]`.
    fn create_type(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::CreateTypeDefinition;
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect_ident_eq("type")?;
        let name = self.relation_ref()?;
        if !self.eat_keyword(Keyword::As) {
            // A bare `CREATE TYPE name` is a shell type.
            return Ok(crate::ast::Statement::CreateType {
                name,
                definition: CreateTypeDefinition::Shell,
            });
        }
        if self.eat_ident_eq("enum") {
            return Ok(crate::ast::Statement::CreateType {
                name,
                definition: CreateTypeDefinition::Enum(self.enum_label_list()?),
            });
        }
        if self.eat_ident_eq("range") {
            self.expect(&Token::LParen)?;
            let mut subtype = None;
            let mut collation = None;
            let mut multirange_type_name = None;
            while *self.peek() != Token::RParen {
                let option = self.expect_ident()?;
                self.expect(&Token::Eq)?;
                match option.as_str() {
                    "subtype" => subtype = Some(self.parse_type_name()?),
                    "collation" => collation = Some(self.expect_collation_name()?),
                    "multirange_type_name" => {
                        multirange_type_name = Some(self.relation_ref()?);
                    }
                    // The remaining options name support functions or an
                    // explicit multirange type. Preserve the semantic options
                    // above and consume these object names for later catalog
                    // expansion.
                    _ => {
                        self.relation_ref()?;
                    }
                }
                if !self.eat_comma() {
                    break;
                }
            }
            self.expect(&Token::RParen)?;
            return Ok(crate::ast::Statement::CreateType {
                name,
                definition: CreateTypeDefinition::Range {
                    subtype: subtype.ok_or_else(|| {
                        ParseError::new("range subtype is required", self.peek_pos())
                    })?,
                    collation,
                    multirange_type_name,
                },
            });
        }
        self.expect(&Token::LParen)?;
        let mut fields = Vec::new();
        if *self.peek() != Token::RParen {
            loop {
                let field_name = self.expect_ident()?;
                let ty = self.parse_type_name()?;
                let collation = if self.eat_ident_eq("collate") {
                    Some(self.expect_collation_name()?)
                } else {
                    None
                };
                fields.push(crate::ast::CompositeFieldDef {
                    name: field_name,
                    ty,
                    collation,
                });
                if !self.eat_comma() {
                    break;
                }
            }
        }
        self.expect(&Token::RParen)?;
        Ok(crate::ast::Statement::CreateType {
            name,
            definition: CreateTypeDefinition::Composite(fields),
        })
    }

    /// `('a', 'b', …)`: an enum's labels, possibly empty.
    fn enum_label_list(&mut self) -> Result<Vec<String>, ParseError> {
        self.expect(&Token::LParen)?;
        let mut labels = Vec::new();
        if *self.peek() != Token::RParen {
            loop {
                labels.push(self.expect_string_lit()?);
                if !self.eat_comma() {
                    break;
                }
            }
        }
        self.expect(&Token::RParen)?;
        Ok(labels)
    }

    /// `ALTER TYPE name <action>`.
    fn alter_type(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{AlterTypeAction, EnumValuePosition};
        self.expect_ident_eq("alter")?;
        self.expect_ident_eq("type")?;
        let name = self.relation_ref()?;
        let pos = self.peek_pos();
        if self.eat_ident_eq("add") {
            if self.eat_ident_eq("value") {
                let if_not_exists = self.eat_if_not_exists();
                let label = self.expect_string_lit()?;
                let position = if self.eat_ident_eq("before") {
                    Some(EnumValuePosition::Before(self.expect_string_lit()?))
                } else if self.eat_ident_eq("after") {
                    Some(EnumValuePosition::After(self.expect_string_lit()?))
                } else {
                    None
                };
                return Ok(crate::ast::Statement::AlterType {
                    name,
                    action: AlterTypeAction::AddValue {
                        label,
                        if_not_exists,
                        position,
                    },
                });
            }
            self.expect_ident_eq("attribute")?;
            let field_name = self.expect_ident()?;
            let ty = self.parse_type_name()?;
            let collation = if self.eat_ident_eq("collate") {
                Some(self.expect_collation_name()?)
            } else {
                None
            };
            self.eat_ident_eq("cascade");
            self.eat_ident_eq("restrict");
            return Ok(crate::ast::Statement::AlterType {
                name,
                action: AlterTypeAction::AddAttribute(crate::ast::CompositeFieldDef {
                    name: field_name,
                    ty,
                    collation,
                }),
            });
        }
        if self.eat_ident_eq("rename") {
            if self.eat_ident_eq("value") {
                let from = self.expect_string_lit()?;
                self.expect(&Token::Keyword(Keyword::To))?;
                let to = self.expect_string_lit()?;
                return Ok(crate::ast::Statement::AlterType {
                    name,
                    action: AlterTypeAction::RenameValue { from, to },
                });
            }
            self.expect(&Token::Keyword(Keyword::To))?;
            let new_name = self.expect_ident()?;
            return Ok(crate::ast::Statement::AlterType {
                name,
                action: AlterTypeAction::RenameTo(new_name),
            });
        }
        if self.eat_ident_eq("owner") {
            self.expect(&Token::Keyword(Keyword::To))?;
            let role = self.expect_ident()?;
            return Ok(crate::ast::Statement::AlterType {
                name,
                action: AlterTypeAction::OwnerTo(role),
            });
        }
        Err(ParseError::new_sqlstate(
            "42601",
            format!(
                "unrecognized ALTER TYPE action at or near {:?}",
                self.peek()
            ),
            pos,
        ))
    }

    /// `DROP TYPE [IF EXISTS] name [, …] [CASCADE | RESTRICT]`.
    fn drop_type(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect_ident_eq("type")?;
        let (names, if_exists, cascade) = self.drop_name_list()?;
        Ok(crate::ast::Statement::DropType {
            names,
            if_exists,
            cascade,
        })
    }

    /// `DROP DOMAIN [IF EXISTS] name [, …] [CASCADE | RESTRICT]`.
    fn drop_domain(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect_ident_eq("domain")?;
        let (names, if_exists, cascade) = self.drop_name_list()?;
        Ok(crate::ast::Statement::DropDomain {
            names,
            if_exists,
            cascade,
        })
    }

    /// The `[IF EXISTS] name [, …] [CASCADE | RESTRICT]` tail both type drops share.
    fn drop_name_list(&mut self) -> Result<(Vec<crate::ast::RelationRef>, bool, bool), ParseError> {
        let if_exists = self.eat_if_exists()?;
        let mut names = vec![self.relation_ref()?];
        while self.eat_comma() {
            names.push(self.relation_ref()?);
        }
        let cascade = self.eat_drop_behavior();
        Ok((names, if_exists, cascade))
    }

    /// `CREATE DOMAIN name [AS] base [DEFAULT e] [[NOT] NULL] [CHECK (…)] …`.
    fn create_domain(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::DomainConstraint;
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect_ident_eq("domain")?;
        let name = self.relation_ref()?;
        // `AS` is optional in PostgreSQL's grammar.
        let _ = self.eat_keyword(Keyword::As);
        let base = self.parse_type_name()?;
        // A domain may carry a `COLLATE` before its constraints.
        if self.eat_ident_eq("collate") {
            let _ = self.expect_collation_name()?;
        }
        let mut constraints = Vec::new();
        loop {
            if self.eat_ident_eq("default") {
                constraints.push(DomainConstraint::Default(self.expression_source()?));
                continue;
            }
            if self.eat_keyword(Keyword::Not) {
                self.expect(&Token::Keyword(Keyword::Null))?;
                constraints.push(DomainConstraint::NotNull);
                continue;
            }
            if *self.peek() == Token::Keyword(Keyword::Null) {
                self.bump();
                constraints.push(DomainConstraint::Null);
                continue;
            }
            let named = if self.eat_ident_eq("constraint") {
                Some(self.expect_ident()?)
            } else {
                None
            };
            if self.eat_ident_eq("check") {
                constraints.push(DomainConstraint::Check {
                    name: named,
                    text: self.check_predicate()?.text,
                });
                continue;
            }
            if let Some(constraint) = named {
                return Err(ParseError::new_sqlstate(
                    "42601",
                    format!("expected CHECK after CONSTRAINT {constraint}"),
                    self.peek_pos(),
                ));
            }
            break;
        }
        Ok(crate::ast::Statement::CreateDomain {
            name,
            base,
            constraints,
        })
    }

    /// `ALTER DOMAIN name <action>`.
    fn alter_domain(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::AlterDomainAction;
        self.expect_ident_eq("alter")?;
        self.expect_ident_eq("domain")?;
        let name = self.relation_ref()?;
        let pos = self.peek_pos();
        let action = if self.eat_keyword(Keyword::Set) {
            if self.eat_ident_eq("default") {
                AlterDomainAction::SetDefault(self.expression_source()?)
            } else {
                self.expect(&Token::Keyword(Keyword::Not))?;
                self.expect(&Token::Keyword(Keyword::Null))?;
                AlterDomainAction::SetNotNull(true)
            }
        } else if self.eat_keyword(Keyword::Drop) {
            if self.eat_ident_eq("default") {
                AlterDomainAction::DropDefault
            } else if self.eat_keyword(Keyword::Not) {
                self.expect(&Token::Keyword(Keyword::Null))?;
                AlterDomainAction::SetNotNull(false)
            } else {
                self.expect_ident_eq("constraint")?;
                let if_exists = self.eat_if_exists()?;
                let constraint = self.expect_ident()?;
                let _ = self.eat_drop_behavior();
                AlterDomainAction::DropConstraint {
                    name: constraint,
                    if_exists,
                }
            }
        } else if self.eat_ident_eq("add") {
            let named = if self.eat_ident_eq("constraint") {
                Some(self.expect_ident()?)
            } else {
                None
            };
            self.expect_ident_eq("check")?;
            let text = self.check_predicate()?.text;
            let not_valid = self.eat_not_valid();
            AlterDomainAction::AddConstraint {
                name: named,
                text,
                not_valid,
            }
        } else if self.eat_ident_eq("validate") {
            self.expect_ident_eq("constraint")?;
            AlterDomainAction::ValidateConstraint(self.expect_ident()?)
        } else if self.eat_ident_eq("rename") {
            if self.eat_ident_eq("constraint") {
                let from = self.expect_ident()?;
                self.expect(&Token::Keyword(Keyword::To))?;
                let to = self.expect_ident()?;
                AlterDomainAction::RenameConstraint { from, to }
            } else {
                self.expect(&Token::Keyword(Keyword::To))?;
                AlterDomainAction::RenameTo(self.expect_ident()?)
            }
        } else if self.eat_ident_eq("owner") {
            self.expect(&Token::Keyword(Keyword::To))?;
            AlterDomainAction::OwnerTo(self.expect_ident()?)
        } else {
            return Err(ParseError::new_sqlstate(
                "42601",
                format!(
                    "unrecognized ALTER DOMAIN action at or near {:?}",
                    self.peek()
                ),
                pos,
            ));
        };
        Ok(crate::ast::Statement::AlterDomain { name, action })
    }

    /// `NOT VALID` after a domain or table constraint.
    fn eat_not_valid(&mut self) -> bool {
        if matches!(self.peek(), Token::Keyword(Keyword::Not))
            && matches!(self.peek2(), Token::Ident(w) if w.eq_ignore_ascii_case("valid"))
        {
            self.bump();
            self.bump();
            return true;
        }
        false
    }

    /// Parse an expression and return its source text. A domain default stores
    /// that text, because the executor re-parses and evaluates it per use.
    fn expression_source(&mut self) -> Result<String, ParseError> {
        let start = self.peek_pos();
        let _ = self.expr(0)?;
        let end = self.peek_pos();
        Ok(self.source[start..end].trim().to_string())
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
    /// [ANALYZE] [name [, ...]]`. The parser validates the whole tail for shape
    /// and discards it. Reclamation is autonomous, so the command is a hint.
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

    /// `LISTEN <channel>`. `listen` and the channel are plain identifiers, so
    /// the word stays usable as a column/table name; an unquoted channel folds
    /// to lowercase and a quoted one keeps its case, exactly like any other
    /// identifier.
    fn listen_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("listen")?;
        Ok(crate::ast::Statement::Listen {
            channel: self.expect_ident()?,
        })
    }

    /// `NOTIFY <channel> [, '<payload>']`. The payload is a string literal;
    /// `PostgreSQL` treats an omitted payload as the empty string, which the AST
    /// records as `None` so the executor can tell the two spellings apart.
    fn notify_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("notify")?;
        let channel = self.expect_ident()?;
        let payload = if self.eat_comma() {
            Some(self.expect_string_lit()?)
        } else {
            None
        };
        Ok(crate::ast::Statement::Notify { channel, payload })
    }

    /// `UNLISTEN { <channel> | * }`.
    fn unlisten_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::UnlistenTarget;
        self.expect_ident_eq("unlisten")?;
        let target = if *self.peek() == Token::Star {
            self.bump();
            UnlistenTarget::All
        } else {
            UnlistenTarget::Channel(self.expect_ident()?)
        };
        Ok(crate::ast::Statement::Unlisten { target })
    }

    /// `TRUNCATE [TABLE] name [, ...] [RESTART IDENTITY | CONTINUE IDENTITY]
    /// [CASCADE | RESTRICT]`. `CONTINUE IDENTITY` and `RESTRICT` are the
    /// `PostgreSQL` defaults; `CASCADE` widens the truncated set to the tables
    /// holding a foreign key onto one of `names`.
    fn truncate(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("truncate")?;
        let _ = self.eat_keyword(Keyword::Table);
        let mut targets = vec![self.truncate_target()?];
        while *self.peek() == Token::Comma {
            self.bump();
            targets.push(self.truncate_target()?);
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
        let cascade = if self.eat_ident_eq("cascade") {
            true
        } else {
            let _ = self.eat_ident_eq("restrict");
            false
        };
        Ok(crate::ast::Statement::Truncate {
            targets,
            restart_identity,
            cascade,
        })
    }

    /// One `[ONLY] name [*]` entry of a `TRUNCATE` list. The trailing `*` is
    /// `PostgreSQL`'s explicit spelling of the default (descend into children)
    /// and carries no information beyond the absence of `ONLY`.
    fn truncate_target(&mut self) -> Result<crate::ast::TruncateTarget, ParseError> {
        let only = self.eat_only();
        let name = self.relation_ref()?;
        if *self.peek() == Token::Star {
            self.bump();
        }
        Ok(crate::ast::TruncateTarget { name, only })
    }

    /// Consume one storage-parameter value (`WITH (key = value)`): a numeric
    /// literal (optionally negative), a string, or a bare word such as `on` /
    /// `off`. The value is validated for shape and discarded.
    fn storage_parameter_value(&mut self) -> Result<String, ParseError> {
        if *self.peek() == Token::Minus {
            self.bump();
            let pos = self.peek_pos();
            return match self.bump() {
                Token::IntLit(raw) | Token::FloatLit(raw) => Ok(format!("-{raw}")),
                other => Err(ParseError::new(
                    format!("expected numeric storage parameter value, found {other:?}"),
                    pos,
                )),
            };
        }
        let pos = self.peek_pos();
        match self.bump() {
            Token::IntLit(raw)
            | Token::FloatLit(raw)
            | Token::StringLit(raw)
            | Token::Ident(raw) => Ok(raw),
            Token::Keyword(keyword) => Ok(format!("{keyword:?}").to_ascii_lowercase()),
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
        let mut names = vec![self.relation_ref()?];
        while *self.peek() == Token::Comma {
            self.bump();
            names.push(self.relation_ref()?);
        }
        let cascade = self.eat_drop_behavior();
        Ok(Statement::DropTable {
            names,
            if_exists,
            cascade,
        })
    }

    fn drop_index(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;

        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect(&Token::Keyword(Keyword::Index))?;
        let if_exists = self.eat_if_exists()?;
        let name = self.relation_ref()?;
        Ok(Statement::DropIndex {
            name,
            if_exists,
            cascade: self.eat_drop_behavior(),
        })
    }

    fn drop_view(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;

        self.expect(&Token::Keyword(Keyword::Drop))?;
        self.expect(&Token::Keyword(Keyword::View))?;
        let if_exists = self.eat_if_exists()?;
        let name = self.relation_ref()?;
        Ok(Statement::DropView {
            name,
            if_exists,
            cascade: self.eat_drop_behavior(),
        })
    }

    fn insert(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;
        self.expect(&Token::Keyword(Keyword::Insert))?;
        self.expect(&Token::Keyword(Keyword::Into))?;
        let table = self.relation_ref()?;
        // `(` starts the target column list, unless it opens a parenthesised
        // query — `INSERT INTO t (SELECT …)` inserts that query's rows.
        let columns = if *self.peek() == Token::LParen && !self.paren_opens_query_expr() {
            self.bump();
            // `insert_column_item` is `ColId opt_indirection`, so a word this
            // lexer keywords but PostgreSQL classifies unreserved or col_name is
            // an ordinary target column here.
            let mut cols = vec![self.expect_col_id()?];
            while self.eat_comma() {
                cols.push(self.expect_col_id()?);
            }
            self.expect(&Token::RParen)?;
            Some(cols)
        } else {
            None
        };
        let source = self.insert_source()?;
        let on_conflict = self.on_conflict_clause()?;
        Ok(Statement::Insert {
            table,
            columns,
            source,
            with: None,
            on_conflict,
            returning: self.returning_clause()?,
        })
    }

    /// True when the `(` at the cursor opens a parenthesised query expression
    /// rather than an identifier list.
    fn paren_opens_query_expr(&self) -> bool {
        let mut offset = 0usize;
        while matches!(self.peek_n(offset), Token::LParen) {
            offset += 1;
        }
        // `VALUES` is a `col_name_keyword`, so `INSERT INTO t (values) VALUES (1)`
        // names a column rather than opening a `VALUES` list. What tells them
        // apart is the row list a real `VALUES` must be followed by.
        if matches!(self.peek_n(offset), Token::Keyword(Keyword::Values)) {
            return matches!(self.peek_n(offset + 1), Token::LParen);
        }
        matches!(
            self.peek_n(offset),
            Token::Keyword(Keyword::Select | Keyword::With | Keyword::Table)
        )
    }

    /// The rows an `INSERT` supplies: a `VALUES` list (which may contain
    /// `DEFAULT`), `DEFAULT VALUES`, or any query expression.
    fn insert_source(&mut self) -> Result<crate::ast::InsertSource, ParseError> {
        use crate::ast::InsertSource;
        if matches!(self.peek(), Token::Ident(s) if s == "default")
            && *self.peek2() == Token::Keyword(Keyword::Values)
        {
            self.bump();
            self.bump();
            return Ok(InsertSource::DefaultValues);
        }
        // A bare `VALUES` list keeps the classic row path so `DEFAULT` stays legal
        // in value position; every other spelling is a full query expression.
        if *self.peek() == Token::Keyword(Keyword::Values) {
            self.bump();
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
            return Ok(InsertSource::Values(rows));
        }
        Ok(InsertSource::Query(Box::new(self.query_expr()?)))
    }

    /// `ON CONFLICT [ ( col, … ) [WHERE pred] | ON CONSTRAINT name ]
    /// DO { NOTHING | UPDATE SET a = e, … [WHERE pred] }`, positioned where the
    /// clause would start (immediately after the VALUES list).
    ///
    /// `conflict`, `do`, `nothing` and `constraint` are matched as soft
    /// identifiers, so all four remain usable as column and table names. They
    /// are unreserved in `PostgreSQL` too.
    fn on_conflict_clause(&mut self) -> Result<Option<crate::ast::OnConflict>, ParseError> {
        use crate::ast::{OnConflict, OnConflictAction, OnConflictTarget};

        let clause_pos = self.peek_pos();
        if !self.eat_keyword(Keyword::On) {
            return Ok(None);
        }
        self.expect_ident_eq("conflict")?;
        let target = if *self.peek() == Token::LParen {
            self.bump();
            let mut columns = vec![self.expect_ident()?];
            while self.eat_comma() {
                columns.push(self.expect_ident()?);
            }
            self.expect(&Token::RParen)?;
            let index_predicate = if self.eat_keyword(Keyword::Where) {
                Some(self.expr(0)?)
            } else {
                None
            };
            OnConflictTarget::Columns {
                columns,
                index_predicate,
            }
        } else if self.eat_keyword(Keyword::On) {
            self.expect_ident_eq("constraint")?;
            OnConflictTarget::OnConstraint(self.expect_ident()?)
        } else {
            OnConflictTarget::None
        };
        self.expect_ident_eq("do")?;
        let action = if self.eat_ident_eq("nothing") {
            OnConflictAction::DoNothing
        } else {
            self.expect(&Token::Keyword(Keyword::Update))?;
            // PostgreSQL raises this during parse analysis, not in the grammar,
            // but the rule is purely syntactic: DO UPDATE has no way to pick an
            // arbiter index without an inference specification.
            if matches!(target, OnConflictTarget::None) {
                return Err(ParseError::new_sqlstate(
                    "42601",
                    "ON CONFLICT DO UPDATE requires inference specification or constraint name",
                    clause_pos,
                ));
            }
            self.expect(&Token::Keyword(Keyword::Set))?;
            let mut assignments = Vec::new();
            loop {
                let column = self.expect_ident()?;
                self.expect(&Token::Eq)?;
                let value = self.expr(0)?;
                assignments.push((column, value));
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
            OnConflictAction::DoUpdate {
                assignments,
                filter,
            }
        };
        Ok(Some(OnConflict { target, action }))
    }

    /// `RETURNING [ WITH ( { OLD | NEW } AS alias [, …] ) ] <projection>`. The
    /// `WITH` list is `PostgreSQL` 18's way of naming the pre- and post-image
    /// rows when the default `old`/`new` spellings would collide.
    fn returning_clause(&mut self) -> Result<Option<crate::ast::Returning>, ParseError> {
        if !self.eat_keyword(Keyword::Returning) {
            return Ok(None);
        }
        let mut old_alias = None;
        let mut new_alias = None;
        if self.eat_keyword(Keyword::With) {
            self.expect(&Token::LParen)?;
            loop {
                let pos = self.peek_pos();
                let which = self.expect_ident()?;
                self.expect(&Token::Keyword(Keyword::As))?;
                let alias = self.expect_ident()?;
                let slot = match which.as_str() {
                    "old" => &mut old_alias,
                    "new" => &mut new_alias,
                    _ => {
                        return Err(ParseError::new_sqlstate(
                            "42601",
                            format!("syntax error at or near \"{which}\""),
                            pos,
                        ));
                    }
                };
                if slot.is_some() {
                    return Err(ParseError::new_sqlstate(
                        "42601",
                        format!("{} specified more than once", which.to_uppercase()),
                        pos,
                    ));
                }
                *slot = Some(alias);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
        }
        Ok(Some(crate::ast::Returning {
            old_alias,
            new_alias,
            items: self.projection_list()?,
        }))
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
                    Some(self.expect_col_label()?)
                } else {
                    self.opt_bare_col_label()
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

    /// `COPY` — both directions, both target forms, both option spellings.
    ///
    /// `PostgreSQL`'s grammar has two productions. The relation form,
    /// `COPY [BINARY] t [(cols)] {FROM|TO} [PROGRAM] {STDIN|STDOUT|'file'}
    /// [[USING] DELIMITERS 'c'] [WITH] <options> [WHERE …]`, and the query form,
    /// `COPY ( <query> ) TO [PROGRAM] {STDOUT|'file'} [WITH] <options>`. The two
    /// are told apart by the token after `COPY`: only the query form can open
    /// with a parenthesis, because a relation name never does.
    fn copy_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{CopyStmt, CopyTarget, Statement};

        self.expect(&Token::Keyword(Keyword::Copy))?;

        if *self.peek() == Token::LParen {
            return self.copy_query_stmt();
        }

        // `opt_binary` — the pre-relation `COPY BINARY t …` spelling, which is
        // just `format binary` under another name. Quoted `"binary"` is a
        // relation name, never the flag.
        if !self.peek_is_quoted_ident() && self.peek_word_is("binary") {
            return Err(ParseError::new_sqlstate(
                "0A000",
                "COPY BINARY is not supported",
                self.peek_pos(),
            ));
        }

        let name = self.relation_ref()?;
        let columns = if *self.peek() == Token::LParen {
            Some(self.parse_parenthesized_ident_list()?)
        } else {
            None
        };

        let is_from = self.copy_direction_keyword()?;
        let direction = self.copy_endpoint(is_from)?;

        // `copy_delimiter` — `[USING] DELIMITERS 'c'`, which sits *before* the
        // `WITH` in the grammar and is spelled `DELIMITERS`, not `DELIMITER`.
        let mut written = Vec::new();
        if let Some(delimiters) = self.copy_legacy_delimiters()? {
            written.push(delimiters);
        }
        written.extend(self.copy_option_list()?);
        let options = copy_options(&written, is_from)?;

        // `where_clause` closes the relation form. It is a `COPY FROM` row
        // filter; PostgreSQL rejects it outright on the `TO` side.
        if *self.peek() == Token::Keyword(Keyword::Where) {
            return Err(if is_from {
                ParseError::new_sqlstate(
                    "0A000",
                    "WHERE clause in COPY FROM is not supported",
                    self.peek_pos(),
                )
            } else {
                ParseError::new_sqlstate(
                    "42601",
                    "WHERE clause not allowed with COPY TO",
                    self.peek_pos(),
                )
            });
        }

        self.expect_end_of_copy()?;
        Ok(Statement::Copy(Box::new(CopyStmt {
            target: CopyTarget::Table { name, columns },
            direction,
            options,
        })))
    }

    /// The `COPY ( <query> ) TO …` production, entered with the cursor on the
    /// opening parenthesis. Neither a column list nor a `WHERE` clause nor a
    /// `FROM` direction is part of this production, so each is the syntax error
    /// `PostgreSQL` reports for it.
    fn copy_query_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{CopyStmt, CopyTarget, Statement};

        self.expect(&Token::LParen)?;
        let query_pos = self.peek_pos();
        let query = self.copy_preparable_stmt(query_pos)?;
        self.expect(&Token::RParen)?;

        if *self.peek() != Token::Keyword(Keyword::To) {
            return Err(self.syntax_error_here());
        }
        self.bump();
        let direction = self.copy_endpoint(false)?;
        let options = copy_options(&self.copy_option_list()?, false)?;

        self.expect_end_of_copy()?;
        Ok(Statement::Copy(Box::new(CopyStmt {
            target: CopyTarget::Query(Box::new(query)),
            direction,
            options,
        })))
    }

    /// `PreparableStmt` inside `COPY ( … )`: a query body, or a data-modifying
    /// statement that must hand rows back through `RETURNING`.
    fn copy_preparable_stmt(
        &mut self,
        query_pos: usize,
    ) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;

        let statement = if *self.peek() == Token::Keyword(Keyword::With) {
            let with = self.parse_with_clause()?;
            if self.starts_dml_statement() {
                let mut statement = self.dml_statement()?;
                match &mut statement {
                    Statement::Insert { with: slot, .. }
                    | Statement::Update { with: slot, .. }
                    | Statement::Delete { with: slot, .. }
                    | Statement::Merge { with: slot, .. } => *slot = with,
                    _ => unreachable!("dml_statement only builds DML statements"),
                }
                statement
            } else {
                let query = self.query_expr_after_with(with)?;
                self.finish_query_statement(query)
            }
        } else if self.starts_dml_statement() {
            self.dml_statement()?
        } else if self.starts_query_expr() {
            self.query_statement()?
        } else {
            return Err(self.syntax_error_here());
        };

        // The grammar lets `SELECT … INTO t` through here; COPY does not run it.
        if matches!(statement, Statement::CreateTableAs { .. }) {
            return Err(ParseError::new_sqlstate(
                "0A000",
                "COPY (SELECT INTO) is not supported",
                query_pos,
            ));
        }
        let returning = match &statement {
            Statement::Insert { returning, .. }
            | Statement::Update { returning, .. }
            | Statement::Delete { returning, .. }
            | Statement::Merge { returning, .. } => Some(returning),
            _ => None,
        };
        if returning.is_some_and(Option::is_none) {
            return Err(ParseError::new_sqlstate(
                "0A000",
                "COPY query must have a RETURNING clause",
                query_pos,
            ));
        }
        Ok(statement)
    }

    /// `copy_from` — `FROM` (true) or `TO` (false).
    fn copy_direction_keyword(&mut self) -> Result<bool, ParseError> {
        if self.eat_keyword(Keyword::From) {
            Ok(true)
        } else if self.eat_keyword(Keyword::To) {
            Ok(false)
        } else {
            Err(self.syntax_error_here())
        }
    }

    /// `opt_program copy_file_name` — where the rows come from or go to.
    ///
    /// `STDIN` and `STDOUT` are interchangeable in the grammar: `copy_file_name`
    /// yields "no file" for either word and the direction decides which stream
    /// that is, so `COPY t TO STDIN` writes to the client exactly as `STDOUT`
    /// would. `PROGRAM` runs a shell command as the server's operating-system
    /// user, which this engine has no equivalent of.
    fn copy_endpoint(&mut self, is_from: bool) -> Result<crate::ast::CopyDirection, ParseError> {
        use crate::ast::{CopyDestination, CopyDirection, CopySource};

        let pos = self.peek_pos();
        if self.eat_ident_eq("program") {
            return Err(ParseError::new_sqlstate(
                "0A000",
                if is_from {
                    "COPY FROM PROGRAM is not supported"
                } else {
                    "COPY TO PROGRAM is not supported"
                },
                pos,
            ));
        }
        if self.eat_ident_eq("stdin") || self.eat_ident_eq("stdout") {
            return Ok(if is_from {
                CopyDirection::From(CopySource::Stdin)
            } else {
                CopyDirection::To(CopyDestination::Stdout)
            });
        }
        if let Token::StringLit(path) = self.peek().clone() {
            self.bump();
            return Ok(if is_from {
                CopyDirection::From(CopySource::File(path))
            } else {
                CopyDirection::To(CopyDestination::File(path))
            });
        }
        Err(self.syntax_error_here())
    }

    /// `copy_delimiter` — the pre-`WITH` `[USING] DELIMITERS 'c'` spelling,
    /// which sets the same option `DELIMITER 'c'` does.
    fn copy_legacy_delimiters(&mut self) -> Result<Option<CopyOption>, ParseError> {
        let using = *self.peek() == Token::Keyword(Keyword::Using);
        if !(self.peek_word_is("delimiters") || (using && self.peek2_word_is("delimiters"))) {
            return Ok(None);
        }
        if using {
            self.bump();
        }
        let pos = self.peek_pos();
        self.bump();
        Ok(Some(CopyOption {
            name: "delimiter".into(),
            arg: CopyOptionArg::Word(self.copy_string_lit()?),
            pos,
        }))
    }

    /// `opt_with copy_options` — the option tail in either spelling. `WITH` is
    /// optional before both, and both may be empty.
    fn copy_option_list(&mut self) -> Result<Vec<CopyOption>, ParseError> {
        self.eat_keyword(Keyword::With);
        if *self.peek() == Token::LParen {
            self.copy_generic_option_list()
        } else {
            self.copy_legacy_option_list()
        }
    }

    /// `'(' copy_generic_opt_list ')'` — the modern `(name value, …)` list. The
    /// grammar names no option here: every entry is a label with an optional
    /// argument, and which labels mean something is settled in [`copy_options`].
    fn copy_generic_option_list(&mut self) -> Result<Vec<CopyOption>, ParseError> {
        self.expect(&Token::LParen)?;
        let mut written = Vec::new();
        loop {
            let pos = self.peek_pos();
            let name = self.expect_col_label()?;
            let arg = self.copy_generic_option_arg()?;
            written.push(CopyOption { name, arg, pos });
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        Ok(written)
    }

    /// `copy_generic_opt_arg` — a word, a string, a number, `*`, a
    /// parenthesized word list, or nothing at all.
    fn copy_generic_option_arg(&mut self) -> Result<CopyOptionArg, ParseError> {
        if matches!(self.peek(), Token::Comma | Token::RParen) {
            return Ok(CopyOptionArg::Absent);
        }
        if *self.peek() == Token::Star {
            self.bump();
            return Ok(CopyOptionArg::Star);
        }
        if *self.peek() == Token::LParen {
            self.bump();
            let mut items = Vec::new();
            loop {
                items.push(self.copy_option_word()?);
                if self.eat_comma() {
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
            return Ok(CopyOptionArg::Columns(items));
        }
        let negated = match self.peek() {
            Token::Minus => {
                self.bump();
                true
            }
            Token::Plus => {
                self.bump();
                false
            }
            _ => false,
        };
        match self.peek().clone() {
            Token::IntLit(digits) => {
                self.bump();
                // An integer wider than `i64` is still a legal `NumericOnly`;
                // it only fails where an option wants a number, so it keeps its
                // written spelling until then.
                Ok(match digits.parse::<i64>() {
                    Ok(value) if negated => CopyOptionArg::Int(-value),
                    Ok(value) => CopyOptionArg::Int(value),
                    Err(_) if negated => CopyOptionArg::Word(format!("-{digits}")),
                    Err(_) => CopyOptionArg::Word(digits),
                })
            }
            Token::FloatLit(digits) => {
                self.bump();
                Ok(CopyOptionArg::Word(if negated {
                    format!("-{digits}")
                } else {
                    digits
                }))
            }
            _ if negated => Err(self.syntax_error_here()),
            _ => Ok(CopyOptionArg::Word(self.copy_option_word()?)),
        }
    }

    /// One `opt_boolean_or_string`: a quoted string, or a bare word.
    fn copy_option_word(&mut self) -> Result<String, ParseError> {
        if let Token::StringLit(text) = self.peek().clone() {
            self.bump();
            return Ok(text);
        }
        self.expect_col_label()
    }

    /// `copy_opt_list` — the legacy bare-keyword tail (`CSV HEADER QUOTE '"'`).
    /// It is comma-free and may be empty, so it ends at the first word that is
    /// not one of its options.
    fn copy_legacy_option_list(&mut self) -> Result<Vec<CopyOption>, ParseError> {
        let mut written = Vec::new();
        loop {
            let pos = self.peek_pos();
            let Some(word) = self.peek_word() else { break };
            let (name, arg) = match word.as_str() {
                "binary" => {
                    return Err(ParseError::new_sqlstate(
                        "0A000",
                        "COPY BINARY is not supported",
                        pos,
                    ));
                }
                "freeze" => {
                    self.bump();
                    ("freeze", CopyOptionArg::Absent)
                }
                "csv" => {
                    self.bump();
                    ("format", CopyOptionArg::Word("csv".into()))
                }
                "header" => {
                    self.bump();
                    ("header", CopyOptionArg::Absent)
                }
                "delimiter" | "null" | "quote" | "escape" => {
                    self.bump();
                    // `opt_as` — `DELIMITER AS '|'` is `DELIMITER '|'`.
                    self.eat_keyword(Keyword::As);
                    let canonical = match word.as_str() {
                        "delimiter" => "delimiter",
                        "null" => "null",
                        "quote" => "quote",
                        _ => "escape",
                    };
                    (canonical, CopyOptionArg::Word(self.copy_string_lit()?))
                }
                "encoding" => {
                    self.bump();
                    ("encoding", CopyOptionArg::Word(self.copy_string_lit()?))
                }
                "force" => {
                    self.bump();
                    let canonical = if self.eat_ident_eq("quote") {
                        "force_quote"
                    } else if self.eat_keyword(Keyword::Not) {
                        self.expect(&Token::Keyword(Keyword::Null))?;
                        "force_not_null"
                    } else if self.eat_keyword(Keyword::Null) {
                        "force_null"
                    } else {
                        return Err(self.syntax_error_here());
                    };
                    let arg = if *self.peek() == Token::Star {
                        self.bump();
                        CopyOptionArg::Star
                    } else {
                        CopyOptionArg::Columns(self.copy_bare_column_list()?)
                    };
                    (canonical, arg)
                }
                _ => break,
            };
            written.push(CopyOption {
                name: name.into(),
                arg,
                pos,
            });
        }
        Ok(written)
    }

    /// `columnList` — the legacy syntax's unparenthesized `a, b, c`.
    fn copy_bare_column_list(&mut self) -> Result<Vec<String>, ParseError> {
        let mut columns = vec![self.expect_col_id()?];
        while self.eat_comma() {
            columns.push(self.expect_col_id()?);
        }
        Ok(columns)
    }

    /// `Sconst` in a COPY option position, reported the way `PostgreSQL`
    /// reports it — pointing at the offending token, not past it.
    fn copy_string_lit(&mut self) -> Result<String, ParseError> {
        if let Token::StringLit(text) = self.peek().clone() {
            self.bump();
            Ok(text)
        } else {
            Err(self.syntax_error_here())
        }
    }

    /// The word at the cursor, lowercased, when it is spelled as an unquoted
    /// identifier or as a keyword. `None` for a quoted name (which is a plain
    /// identifier and never an option word) and for anything that is not a word.
    fn peek_word(&self) -> Option<String> {
        match self.peek() {
            Token::Ident(word) if !self.peek_is_quoted_ident() => Some(word.to_ascii_lowercase()),
            Token::Keyword(_) => Some(self.keyword_label()),
            _ => None,
        }
    }

    fn peek_word_is(&self, want: &str) -> bool {
        self.peek_word().is_some_and(|word| word == want)
    }

    fn peek2_word_is(&self, want: &str) -> bool {
        matches!(self.peek2(), Token::Ident(word) if word.eq_ignore_ascii_case(want))
    }

    /// `PostgreSQL`'s bare `syntax error at or near "…"` for the token at the
    /// cursor, quoting the token as it was written: the message echoes source
    /// text, so a keyword keeps the case it was typed in and a string literal
    /// keeps its quotes.
    fn syntax_error_here(&self) -> ParseError {
        let pos = self.peek_pos();
        if *self.peek() == Token::Eof {
            return ParseError::new_sqlstate("42601", "syntax error at end of input", pos);
        }
        // The next token's offset bounds this one; only layout sits between them.
        let end = self
            .toks
            .get(self.pos + 1)
            .map_or(self.source.len(), |(_, next)| *next);
        let lexeme = self.source[pos..end.min(self.source.len())].trim_end();
        ParseError::new_sqlstate(
            "42601",
            format!("syntax error at or near \"{lexeme}\""),
            pos,
        )
    }

    /// Nothing may follow a `COPY` but the end of the statement. Reporting it
    /// here names the offending token, where the statement splitter could only
    /// say that the statement did not end.
    fn expect_end_of_copy(&self) -> Result<(), ParseError> {
        if matches!(self.peek(), Token::Semicolon | Token::Eof) {
            Ok(())
        } else {
            Err(self.syntax_error_here())
        }
    }

    fn parse_parenthesized_ident_list(&mut self) -> Result<Vec<String>, ParseError> {
        self.expect(&Token::LParen)?;
        let mut cols = Vec::new();
        loop {
            // Every parenthesised column list in the grammar — an INSERT target
            // list, a COPY column list, a multi-column UPDATE target — is a list
            // of `ColId`.
            cols.push(self.expect_col_id()?);
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        Ok(cols)
    }

    /// Parse projection → HAVING. Leaves `order_by` / `limit` / `offset` / `locking` empty;
    /// the caller (single SELECT or set-op query) owns the tail.
    fn select_core(&mut self) -> Result<crate::ast::SelectStmt, ParseError> {
        // Mode-1 depth guard: EVERY SELECT body funnels through `select_core` — a
        // top-level set-op branch (`set_primary → select_core`), a derived table,
        // or a scalar/IN/EXISTS subquery (`query_expr → set_primary → select_core`) — so
        // guarding here bounds all nested-SELECT recursion (e.g. a derived-table
        // chain `( SELECT … FROM ( SELECT … ) )`). Subqueries also pass through
        // `expr` first; guarding both is belt-and-braces.
        let _guard = DepthGuard::enter(&self.depth, self.peek_pos())?;
        // Own a window-call frame for the whole body, so a window call written in
        // any of this SELECT's clauses lands here and one written in a nested
        // subquery lands on that subquery instead. Popped on every exit path.
        self.window_calls.push(Vec::new());
        // A subquery written inside a window specification is a separate query
        // level: PostgreSQL's window-definition ban applies to the SELECT that
        // owns the specification, not to any query nested inside it, so
        // `count(*) OVER (ORDER BY (SELECT rank() OVER ()))` is legal. Enter this
        // body with a fresh count and restore the enclosing one on the way out.
        let enclosing_window_spec_depth = std::mem::take(&mut self.window_spec_depth);
        let body = self.select_core_body();
        self.window_spec_depth = enclosing_window_spec_depth;
        let window_calls = self.window_calls.pop().unwrap_or_default();
        let mut select = body?;
        select.window_calls = window_calls;
        Ok(select)
    }

    fn select_core_body(&mut self) -> Result<crate::ast::SelectStmt, ParseError> {
        use crate::ast::SelectStmt;
        self.expect(&Token::Keyword(Keyword::Select))?;
        // SP28: SELECT DISTINCT / DISTINCT ON (…) (ALL is the default modifier —
        // accept and ignore).
        let distinct = self.distinct_clause()?;
        let projection = self.projection_list()?;
        self.opt_select_into()?;
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
        let (group_by, grouping) = self.parse_group_by()?;
        let having = if self.eat_keyword(Keyword::Having) {
            Some(self.expr(0)?)
        } else {
            None
        };
        let windows = self.window_clause()?;
        Ok(SelectStmt {
            projection,
            from,
            filter,
            distinct,
            group_by,
            grouping,
            having,
            windows,
            // Filled in by `select_core` from this SELECT's window-call frame.
            window_calls: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            with_ties: false,
            locking: None,
        })
    }

    /// `GROUP BY [ALL | DISTINCT] <group_by_item> [, …]`.
    ///
    /// Returns the flattened, deduplicated grouping expressions. It also
    /// returns the set structure over their indices, but only when the clause
    /// needs expansion into more than the one grouping set a plain `GROUP BY`
    /// produces.
    fn parse_group_by(&mut self) -> Result<GroupByClause, ParseError> {
        use crate::ast::GroupingClause;
        if !self.eat_keyword(Keyword::Group) {
            return Ok((Vec::new(), None));
        }
        self.expect(&Token::Keyword(Keyword::By))?;
        // PG18 `GROUP BY ALL` / `GROUP BY DISTINCT`: ALL is the default and keeps
        // duplicate grouping sets, DISTINCT collapses them.
        let distinct = self.eat_keyword(Keyword::Distinct);
        if !distinct {
            self.eat_keyword(Keyword::All);
        }
        let mut exprs = Vec::new();
        let mut items = Vec::new();
        let mut structured = false;
        loop {
            items.push(self.group_by_item(&mut exprs, &mut structured)?);
            if self.eat_comma() {
                continue;
            }
            break;
        }
        if !structured && !distinct {
            return Ok((exprs, None));
        }
        let clause = GroupingClause { distinct, items };
        self.check_grouping_set_count(&clause)?;
        Ok((exprs, Some(clause)))
    }

    /// One `group_by_item`: `()`, `ROLLUP (…)`, `CUBE (…)`, `GROUPING SETS (…)`,
    /// or an ordinary expression. `structured` records whether anything that
    /// expands to more than one grouping set has been seen.
    fn group_by_item(
        &mut self,
        exprs: &mut Vec<crate::ast::Expr>,
        structured: &mut bool,
    ) -> Result<crate::ast::GroupItem, ParseError> {
        use crate::ast::GroupItem;
        // A `GROUPING SETS` list nests arbitrarily, so this recursion needs the
        // same depth bound as the rest of the grammar.
        let _guard = DepthGuard::enter(&self.depth, self.peek_pos())?;
        if *self.peek() == Token::LParen && *self.peek2() == Token::RParen {
            self.bump();
            self.bump();
            *structured = true;
            return Ok(GroupItem::Empty);
        }
        if let Some(kind) = self.peek_grouping_keyword() {
            *structured = true;
            return self.grouping_set_item(kind, exprs, structured);
        }
        // A bare parenthesised tuple at the top of the list is not a row value:
        // PostgreSQL flattens `GROUP BY (a, b)` to `GROUP BY a, b`.
        self.group_element(exprs)
    }

    /// The `ROLLUP` / `CUBE` / `GROUPING SETS` construct at the cursor, if any.
    /// All three are unreserved in `PostgreSQL` and lex as identifiers here, so
    /// the parser recognizes them by the token that must follow them. That
    /// keeps a column named `rollup` usable as a grouping expression.
    fn peek_grouping_keyword(&self) -> Option<GroupingKeyword> {
        match self.peek() {
            Token::Ident(w)
                if w.eq_ignore_ascii_case("rollup") && *self.peek2() == Token::LParen =>
            {
                Some(GroupingKeyword::Rollup)
            }
            Token::Ident(w) if w.eq_ignore_ascii_case("cube") && *self.peek2() == Token::LParen => {
                Some(GroupingKeyword::Cube)
            }
            Token::Ident(w)
                if w.eq_ignore_ascii_case("grouping")
                    && matches!(self.peek2(), Token::Ident(s) if s.eq_ignore_ascii_case("sets"))
                    && *self.peek3() == Token::LParen =>
            {
                Some(GroupingKeyword::Sets)
            }
            _ => None,
        }
    }

    fn grouping_set_item(
        &mut self,
        kind: GroupingKeyword,
        exprs: &mut Vec<crate::ast::Expr>,
        structured: &mut bool,
    ) -> Result<crate::ast::GroupItem, ParseError> {
        use crate::ast::GroupItem;
        let pos = self.peek_pos();
        self.bump(); // ROLLUP / CUBE / GROUPING
        if kind == GroupingKeyword::Sets {
            self.bump(); // SETS
        }
        self.expect(&Token::LParen)?;
        let mut elements = Vec::new();
        loop {
            elements.push(match kind {
                // Only GROUPING SETS nests the full item grammar; a ROLLUP/CUBE
                // element is an expression or a parenthesised tuple.
                GroupingKeyword::Sets => self.group_by_item(exprs, structured)?,
                GroupingKeyword::Rollup | GroupingKeyword::Cube => self.group_element(exprs)?,
            });
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        match kind {
            GroupingKeyword::Rollup => Ok(GroupItem::Rollup(elements)),
            GroupingKeyword::Cube => {
                if elements.len() > MAX_CUBE_ELEMENTS {
                    return Err(ParseError::new_sqlstate(
                        "54011",
                        "CUBE is limited to 12 elements",
                        pos,
                    ));
                }
                Ok(GroupItem::Cube(elements))
            }
            GroupingKeyword::Sets => Ok(GroupItem::GroupingSets(elements)),
        }
    }

    /// One `ROLLUP`/`CUBE` element or plain `GROUP BY` entry: an expression, or a
    /// parenthesised tuple whose members join and leave a grouping set together.
    fn group_element(
        &mut self,
        exprs: &mut Vec<crate::ast::Expr>,
    ) -> Result<crate::ast::GroupItem, ParseError> {
        use crate::ast::{Expr, GroupItem};
        if *self.peek() == Token::LParen && *self.peek2() == Token::RParen {
            self.bump();
            self.bump();
            return Ok(GroupItem::Empty);
        }
        Ok(match self.expr(0)? {
            Expr::Row(members) => GroupItem::Composite(
                members
                    .into_iter()
                    .map(|member| intern_group_expr(exprs, member))
                    .collect(),
            ),
            other => GroupItem::Expr(intern_group_expr(exprs, other)),
        })
    }

    /// `PostgreSQL` caps a query at 4096 grouping sets. The item structure alone
    /// determines the count, so the bound is checked here rather than after
    /// expansion; with `GROUP BY DISTINCT` this counts sets before duplicates are
    /// removed, so it is an upper bound on `PostgreSQL`'s own count.
    fn check_grouping_set_count(
        &self,
        clause: &crate::ast::GroupingClause,
    ) -> Result<(), ParseError> {
        let mut total: usize = 1;
        for item in &clause.items {
            total = total.saturating_mul(grouping_set_count(item));
            if total > MAX_GROUPING_SETS {
                return Err(ParseError::new_sqlstate(
                    "54001",
                    "too many grouping sets present (maximum 4096)",
                    self.peek_pos(),
                ));
            }
        }
        Ok(())
    }

    /// `DISTINCT`, `DISTINCT ON (expr, …)`, or the `ALL` default, positioned
    /// just after `SELECT`.
    fn distinct_clause(&mut self) -> Result<crate::ast::DistinctClause, ParseError> {
        use crate::ast::DistinctClause;
        if !self.eat_keyword(Keyword::Distinct) {
            self.eat_keyword(Keyword::All);
            return Ok(DistinctClause::All);
        }
        if !self.eat_keyword(Keyword::On) {
            return Ok(DistinctClause::Distinct);
        }
        self.expect(&Token::LParen)?;
        let mut on = vec![self.expr(0)?];
        while self.eat_comma() {
            on.push(self.expr(0)?);
        }
        self.expect(&Token::RParen)?;
        Ok(DistinctClause::On(on))
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
        let order_by = self.parse_order_by()?;
        let mut tail = self.parse_limit_offset()?;
        // PostgreSQL rejects WITH TIES without ORDER BY in the grammar itself
        // (gram.y `insertSelectOptions`), so it is a 42601 syntax error.
        if tail.with_ties && order_by.is_empty() {
            return Err(ParseError::new_sqlstate(
                "42601",
                "WITH TIES cannot be specified without ORDER BY clause",
                self.peek_pos(),
            ));
        }
        tail.order_by = order_by;
        Ok(tail)
    }

    /// `ORDER BY expr [ASC|DESC|USING <op>] [NULLS FIRST|LAST] [, …]`.
    ///
    /// `NULLS` / `FIRST` / `LAST` are matched as soft identifiers so they remain
    /// usable as column names, as they are in `PostgreSQL`.
    fn parse_order_by(&mut self) -> Result<Vec<crate::ast::OrderItem>, ParseError> {
        use crate::ast::OrderItem;
        let mut order_by = Vec::new();
        if !self.eat_keyword(Keyword::Order) {
            return Ok(order_by);
        }
        self.expect(&Token::Keyword(Keyword::By))?;
        loop {
            let expr = self.expr(0)?;
            let asc = if self.eat_keyword(Keyword::Using) {
                self.order_using_ascending()?
            } else if self.eat_keyword(Keyword::Desc) {
                false
            } else {
                self.eat_keyword(Keyword::Asc);
                true
            };
            // PostgreSQL's defaults: NULLS LAST for ASC, NULLS FIRST for DESC.
            let nulls_first = if self.peek_ident_eq("nulls") {
                self.bump();
                if self.eat_ident_eq("first") {
                    true
                } else {
                    self.expect_ident_eq("last")?;
                    false
                }
            } else {
                !asc
            };
            order_by.push(OrderItem {
                expr,
                asc,
                nulls_first,
            });
            if self.eat_comma() {
                continue;
            }
            break;
        }
        Ok(order_by)
    }

    /// The direction an `ORDER BY … USING <op>` sorts in.
    ///
    /// `PostgreSQL` looks the operator up in the operand type's btree operator
    /// family and takes its strategy: the family's "less than" member sorts
    /// ascending and its "greater than" member descending. For every type crabka
    /// has those are spelled `<` and `>`; any other operator is `42809`.
    fn order_using_ascending(&mut self) -> Result<bool, ParseError> {
        let position = self.peek_pos();
        match self.peek() {
            Token::Lt => {
                self.bump();
                Ok(true)
            }
            Token::Gt => {
                self.bump();
                Ok(false)
            }
            other => {
                let Some(spelling) = operator_spelling(other) else {
                    return Err(ParseError::new(
                        "expected an ordering operator after USING",
                        position,
                    ));
                };
                self.bump();
                Err(ParseError::new_sqlstate(
                    "42809",
                    format!("operator {spelling} is not a valid ordering operator"),
                    position,
                ))
            }
        }
    }

    /// The row-count window: `LIMIT`/`OFFSET` in either order (`PostgreSQL`
    /// accepts both), or the SQL-standard `OFFSET n ROWS FETCH FIRST n ROWS
    /// ONLY` spelling. `FETCH` occupies the same slot as `LIMIT`.
    fn parse_limit_offset(&mut self) -> Result<SetTail, ParseError> {
        let mut tail = SetTail::default();
        let mut has_limit = false;
        loop {
            if !has_limit && self.eat_keyword(Keyword::Limit) {
                has_limit = true;
                // `LIMIT ALL` is spelled differently from an absent LIMIT but
                // means exactly the same thing, so both leave `limit` as None.
                if !self.eat_keyword(Keyword::All) {
                    tail.limit = Some(self.expr(0)?);
                }
            } else if !has_limit && self.peek_ident_eq("fetch") {
                has_limit = true;
                self.bump();
                self.parse_fetch_first(&mut tail)?;
            } else if tail.offset.is_none() && self.eat_keyword(Keyword::Offset) {
                tail.offset = Some(self.expr(0)?);
                // `OFFSET n ROW` / `ROWS` — a noise word in the standard form.
                let _ = self.eat_ident_eq("row") || self.eat_ident_eq("rows");
            } else {
                break;
            }
        }
        Ok(tail)
    }

    /// `FETCH {FIRST|NEXT} [count] {ROW|ROWS} {ONLY | WITH TIES}`, positioned
    /// just after `FETCH`. The count defaults to 1 when omitted.
    fn parse_fetch_first(&mut self, tail: &mut SetTail) -> Result<(), ParseError> {
        if !self.eat_ident_eq("first") && !self.eat_ident_eq("next") {
            return Err(ParseError::new(
                "expected FIRST or NEXT after FETCH",
                self.peek_pos(),
            ));
        }
        // The count is optional: `FETCH FIRST ROW ONLY` means one row.
        tail.limit = if self.peek_ident_eq("row") || self.peek_ident_eq("rows") {
            Some(crate::ast::Expr::IntLiteral("1".into()))
        } else {
            Some(self.expr(0)?)
        };
        if !self.eat_ident_eq("row") && !self.eat_ident_eq("rows") {
            return Err(ParseError::new(
                "expected ROW or ROWS in FETCH clause",
                self.peek_pos(),
            ));
        }
        if self.eat_ident_eq("only") {
            return Ok(());
        }
        self.expect(&Token::Keyword(Keyword::With))?;
        self.expect_ident_eq("ties")?;
        tail.with_ties = true;
        Ok(())
    }

    /// Parse the optional row-locking clauses.
    ///
    /// `PostgreSQL` allows several (`FOR UPDATE OF a FOR SHARE OF b`); they fold
    /// onto the strongest strength, the union of the named relations, and the
    /// strictest wait policy, which is indistinguishable from `PostgreSQL` for
    /// the single-base-table locking reads the executor supports.
    fn parse_locking(&mut self) -> Result<Option<crate::ast::LockingClause>, ParseError> {
        use crate::ast::{LockWaitPolicy, LockingClause, RowLockStrength};
        let mut folded: Option<LockingClause> = None;
        while *self.peek() == Token::Keyword(Keyword::For) {
            self.bump();
            // `FOR READ ONLY` is the SQL-standard spelling PostgreSQL accepts and
            // ignores: it locks nothing, so it contributes no clause at all.
            if self.eat_keyword(Keyword::Read) {
                self.expect_ident_eq("only")?;
                continue;
            }
            let strength = if self.eat_keyword(Keyword::Update) {
                RowLockStrength::ForUpdate
            } else if self.eat_ident_eq("no") {
                self.expect_ident_eq("key")?;
                self.expect(&Token::Keyword(Keyword::Update))?;
                RowLockStrength::ForNoKeyUpdate
            } else if self.eat_keyword(Keyword::Share) {
                RowLockStrength::ForShare
            } else if self.eat_ident_eq("key") {
                self.expect(&Token::Keyword(Keyword::Share))?;
                RowLockStrength::ForKeyShare
            } else {
                return Err(ParseError::new(
                    "expected UPDATE, NO KEY UPDATE, SHARE or KEY SHARE after FOR",
                    self.peek_pos(),
                ));
            };
            let mut of = Vec::new();
            if self.eat_ident_eq("of") {
                loop {
                    of.push(self.qualified_name_text()?);
                    if self.eat_comma() {
                        continue;
                    }
                    break;
                }
            }
            let wait = if self.eat_ident_eq("nowait") {
                LockWaitPolicy::NoWait
            } else if self.eat_ident_eq("skip") {
                self.expect_ident_eq("locked")?;
                LockWaitPolicy::SkipLocked
            } else {
                LockWaitPolicy::Wait
            };
            folded = Some(match folded {
                None => LockingClause { strength, of, wait },
                Some(mut prev) => {
                    prev.strength = prev.strength.max(strength);
                    prev.of.extend(of);
                    if prev.wait == LockWaitPolicy::Wait {
                        prev.wait = wait;
                    }
                    prev
                }
            });
        }
        Ok(folded)
    }

    /// Is the current token the identifier `want` (case-insensitively)? The
    /// non-consuming counterpart of [`Self::eat_ident_eq`].
    /// Whether the token *after* the next one is the identifier `want`.
    fn peek2_ident_eq(&self, want: &str) -> bool {
        matches!(self.peek2(), Token::Ident(word) if word.eq_ignore_ascii_case(want))
    }

    fn peek_ident_eq(&self, want: &str) -> bool {
        matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case(want))
    }

    fn dml_command_identity(&self) -> crate::command::CommandIdentity {
        use crate::command::CommandIdentity;
        match self.peek() {
            Token::Keyword(Keyword::Insert) => CommandIdentity::Insert,
            Token::Keyword(Keyword::Update) => CommandIdentity::Update,
            Token::Keyword(Keyword::Delete) => CommandIdentity::Delete,
            _ => CommandIdentity::Merge,
        }
    }

    fn query_statement(&mut self) -> Result<crate::ast::Statement, ParseError> {
        let query = self.query_expr()?;
        Ok(self.finish_query_statement(query))
    }

    fn finish_query_statement(&mut self, query: crate::ast::QueryExpr) -> crate::ast::Statement {
        match self.select_into.take() {
            Some(name) => crate::ast::Statement::CreateTableAs {
                name,
                temporary: std::mem::take(&mut self.select_into_temporary),
                if_not_exists: false,
                columns: None,
                query: Box::new(query),
                with_data: true,
                tablespace: None,
            },
            None => crate::ast::Statement::Query(query),
        }
    }

    /// `SELECT … INTO <table>` — record the target so [`Parser::query_statement`]
    /// can hand back a `CREATE TABLE … AS`. `TEMP`/`TEMPORARY` names the
    /// session's temporary namespace, exactly as it does on the `CREATE`
    /// spelling; `UNLOGGED` is accepted and ignored, there being one storage
    /// class here.
    fn opt_select_into(&mut self) -> Result<(), ParseError> {
        if !self.eat_keyword(Keyword::Into) {
            return Ok(());
        }
        let temporary = self.eat_ident_eq("temporary") || self.eat_ident_eq("temp");
        let _unlogged = !temporary && self.eat_ident_eq("unlogged");
        self.select_into_temporary = temporary;
        let name = self.relation_ref()?;
        if self.select_into.replace(name).is_some() {
            return Err(ParseError::new_sqlstate(
                "42601",
                "SELECT ... INTO specifies more than one target table",
                self.peek_pos(),
            ));
        }
        Ok(())
    }

    fn query_expr(&mut self) -> Result<crate::ast::QueryExpr, ParseError> {
        let with = self.parse_with_clause()?;
        self.query_expr_after_with(with)
    }

    /// The rest of a query expression once its `WITH` list has been parsed. The
    /// statement dispatcher needs the CTE list before it can tell a query body
    /// from a data-modifying one, so it consumes the list itself and re-enters
    /// here.
    fn query_expr_after_with(
        &mut self,
        with: Option<crate::ast::WithClause>,
    ) -> Result<crate::ast::QueryExpr, ParseError> {
        if *self.peek() == Token::LParen {
            let q = self.parenthesized_query_expr()?;
            if self.peek_is_set_op() {
                let left = self.query_expr_as_set_branch(q)?;
                let body = self.set_expr_rest(left, 0)?;
                let tail = self.parse_query_tail_and_locking()?;
                let mut q = self.finish_query_expr(body, tail)?;
                q.with = with;
                return Ok(q);
            }
            if !self.query_tail_or_locking_starts() {
                let mut q = q;
                q.with = with;
                return Ok(q);
            }
            let body = Self::query_expr_as_outer_primary(q);
            let tail = self.parse_query_tail_and_locking()?;
            let mut q = self.finish_query_expr(body, tail)?;
            q.with = with;
            return Ok(q);
        }
        let mut body = self.set_expr(0)?;
        let tail = self.parse_query_tail_for_body(&mut body)?;
        let mut q = self.finish_query_expr(body, tail)?;
        q.with = with;
        Ok(q)
    }

    fn parse_query_tail_and_locking(&mut self) -> Result<QueryTailAndLocking, ParseError> {
        let mut tail = self.parse_set_tail()?;
        let locking = self.parse_locking()?;
        // PostgreSQL's `select_no_parens` accepts the row-count window on either
        // side of the locking clause (`… FOR UPDATE LIMIT 2` is legal), so pick up
        // a window that follows it.
        if tail.limit.is_none() && tail.offset.is_none() {
            let trailing = self.parse_limit_offset()?;
            if trailing.with_ties && tail.order_by.is_empty() {
                return Err(ParseError::new_sqlstate(
                    "42601",
                    "WITH TIES cannot be specified without ORDER BY clause",
                    self.peek_pos(),
                ));
            }
            tail.limit = trailing.limit;
            tail.offset = trailing.offset;
            tail.with_ties = trailing.with_ties;
        }
        Ok(QueryTailAndLocking { tail, locking })
    }

    /// Parse the query tail with `body`'s window-call frame reinstated when the
    /// body is a lone SELECT. `ORDER BY` sits outside `select_core` in this
    /// grammar but belongs to that SELECT, so `ORDER BY rank() OVER (…)` has to
    /// register against it (indices continue where the body left off).
    fn parse_query_tail_for_body(
        &mut self,
        body: &mut crate::ast::SetExpr,
    ) -> Result<QueryTailAndLocking, ParseError> {
        use crate::ast::{QueryBody, SetExpr};
        let SetExpr::Query(QueryBody::Select(select)) = body else {
            return self.parse_query_tail_and_locking();
        };
        self.window_calls
            .push(std::mem::take(&mut select.window_calls));
        let tail = self.parse_query_tail_and_locking();
        select.window_calls = self.window_calls.pop().unwrap_or_default();
        tail
    }

    /// [`parse_query_tail_for_body`] for the row-count-only tail a parenthesized
    /// set-op branch carries.
    fn parse_set_tail_for_body(
        &mut self,
        body: &mut crate::ast::SetExpr,
    ) -> Result<SetTail, ParseError> {
        use crate::ast::{QueryBody, SetExpr};
        let SetExpr::Query(QueryBody::Select(select)) = body else {
            return self.parse_set_tail();
        };
        self.window_calls
            .push(std::mem::take(&mut select.window_calls));
        let tail = self.parse_set_tail();
        select.window_calls = self.window_calls.pop().unwrap_or_default();
        tail
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
        if let Some(locking) = &q.locking {
            return Err(Self::locking_not_allowed(
                locking,
                "UNION/INTERSECT/EXCEPT",
                self.peek_pos(),
            ));
        }
        match q.body {
            SetExpr::Query(QueryBody::Select(mut select)) => {
                select.order_by = q.order_by;
                select.limit = q.limit;
                select.offset = q.offset;
                select.with_ties = q.with_ties;
                Ok(SetExpr::Query(QueryBody::Select(select)))
            }
            body => Ok(SetExpr::Query(QueryBody::Nested(Box::new(
                crate::ast::QueryExpr {
                    with: q.with,
                    body,
                    order_by: q.order_by,
                    limit: q.limit,
                    offset: q.offset,
                    with_ties: q.with_ties,
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
        tail: QueryTailAndLocking,
    ) -> Result<crate::ast::QueryExpr, ParseError> {
        use crate::ast::{QueryBody, QueryExpr, SetExpr};
        let QueryTailAndLocking { tail, locking } = tail;
        if let Some(clause) = &locking {
            match &body {
                SetExpr::Query(QueryBody::Select(_)) => {}
                SetExpr::Query(QueryBody::Values(_)) => {
                    // PostgreSQL's `transformValuesClause` wording, verbatim.
                    return Err(ParseError::new_sqlstate(
                        "0A000",
                        format!("{} cannot be applied to VALUES", clause.strength.as_sql()),
                        self.peek_pos(),
                    ));
                }
                SetExpr::Query(QueryBody::Nested(_)) => {
                    return Err(Self::locking_not_allowed(
                        clause,
                        "a nested query expression",
                        self.peek_pos(),
                    ));
                }
                SetExpr::SetOp { .. } => {
                    return Err(Self::locking_not_allowed(
                        clause,
                        "UNION/INTERSECT/EXCEPT",
                        self.peek_pos(),
                    ));
                }
            }
        }
        Ok(QueryExpr {
            with: None,
            body,
            order_by: tail.order_by,
            limit: tail.limit,
            offset: tail.offset,
            with_ties: tail.with_ties,
            locking,
        })
    }

    /// `PostgreSQL`'s `CheckSelectLocking` refusal: `0A000`, naming the strength
    /// the query actually asked for and the clause it clashes with.
    fn locking_not_allowed(
        clause: &crate::ast::LockingClause,
        what: &str,
        position: usize,
    ) -> ParseError {
        ParseError::new_sqlstate(
            "0A000",
            format!("{} is not allowed with {what}", clause.strength.as_sql()),
            position,
        )
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
        ) || self.peek_ident_eq("fetch")
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
            let materialized = if self.eat_ident_eq("materialized") {
                Some(true)
            } else if *self.peek() == Token::Keyword(Keyword::Not)
                && matches!(self.peek2(), Token::Ident(s) if s == "materialized")
            {
                self.bump();
                self.bump();
                Some(false)
            } else {
                None
            };
            self.expect(&Token::LParen)?;
            let body = if self.starts_dml_statement() {
                crate::ast::CteBody::Dml(Box::new(self.dml_statement()?))
            } else {
                crate::ast::CteBody::Query(Box::new(self.query_expr()?))
            };
            self.expect(&Token::RParen)?;
            let search = self.parse_cte_search()?;
            let cycle = self.parse_cte_cycle()?;
            ctes.push(Cte {
                name,
                columns,
                body,
                materialized,
                search,
                cycle,
            });
            if !self.eat_comma() {
                break;
            }
        }
        Ok(Some(WithClause { recursive, ctes }))
    }

    /// `SEARCH { BREADTH | DEPTH } FIRST BY col [, …] SET col`, the optional
    /// traversal-order clause of a recursive `WITH` item. `SEARCH`, `BREADTH`,
    /// `DEPTH`, and `FIRST` are unreserved in `PostgreSQL` and lex as identifiers.
    fn parse_cte_search(&mut self) -> Result<Option<crate::ast::CteSearch>, ParseError> {
        if !self.eat_ident_eq("search") {
            return Ok(None);
        }
        let depth_first = if self.eat_ident_eq("depth") {
            true
        } else {
            self.expect_ident_eq("breadth")?;
            false
        };
        self.expect_ident_eq("first")?;
        self.expect(&Token::Keyword(Keyword::By))?;
        let by = self.ident_list()?;
        self.expect(&Token::Keyword(Keyword::Set))?;
        let set = self.expect_ident()?;
        Ok(Some(crate::ast::CteSearch {
            depth_first,
            by,
            set,
        }))
    }

    /// `CYCLE col [, …] SET col [TO value DEFAULT value] USING col`, the optional
    /// cycle-detection clause of a recursive `WITH` item.
    fn parse_cte_cycle(&mut self) -> Result<Option<crate::ast::CteCycle>, ParseError> {
        if !self.eat_ident_eq("cycle") {
            return Ok(None);
        }
        let by = self.ident_list()?;
        self.expect(&Token::Keyword(Keyword::Set))?;
        let set = self.expect_ident()?;
        let mark_values = if self.eat_keyword(Keyword::To) {
            let marked = self.expr(0)?;
            self.expect_ident_eq("default")?;
            Some((marked, self.expr(0)?))
        } else {
            None
        };
        self.expect(&Token::Keyword(Keyword::Using))?;
        let using = self.expect_ident()?;
        Ok(Some(crate::ast::CteCycle {
            by,
            set,
            mark_values,
            using,
        }))
    }

    /// A comma-separated list of at least one identifier.
    fn ident_list(&mut self) -> Result<Vec<String>, ParseError> {
        let mut out = vec![self.expect_ident()?];
        while self.eat_comma() {
            out.push(self.expect_ident()?);
        }
        Ok(out)
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
    /// SELECT branch (`select_core`, no tail, because the query owns the tail).
    fn parenthesized_query_expr(&mut self) -> Result<crate::ast::QueryExpr, ParseError> {
        self.expect(&Token::LParen)?;
        self.query_expr_after_open_paren()
    }

    fn query_expr_after_open_paren(&mut self) -> Result<crate::ast::QueryExpr, ParseError> {
        let with = self.parse_with_clause()?;
        let mut body = self.set_expr(0)?;
        // A parenthesized query owns its own tail, so its `ORDER BY rank() OVER
        // (…)` belongs to the SELECT inside the parentheses just as a top-level
        // one belongs to the top-level SELECT.
        let tail = self.parse_query_tail_for_body(&mut body)?;
        self.expect(&Token::RParen)?;
        let mut q = self.finish_query_expr(body, tail)?;
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
        } else if *self.peek() == Token::Keyword(Keyword::Table) {
            Ok(SetExpr::Query(QueryBody::Select(Box::new(
                self.table_query_body()?,
            ))))
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
        ) || self.peek_ident_eq("fetch");
        if !has_tail {
            return Ok(inner);
        }
        let mut inner = inner;
        let tail = self.parse_set_tail_for_body(&mut inner)?;
        match inner {
            SetExpr::Query(QueryBody::Select(mut s)) => {
                s.order_by = tail.order_by;
                s.limit = tail.limit;
                s.offset = tail.offset;
                s.with_ties = tail.with_ties;
                Ok(SetExpr::Query(QueryBody::Select(s)))
            }
            body => Ok(SetExpr::Query(QueryBody::Nested(Box::new(QueryExpr {
                with: None,
                body,
                order_by: tail.order_by,
                limit: tail.limit,
                offset: tail.offset,
                with_ties: tail.with_ties,
                locking: None,
            })))),
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
        let mut left = self.table_factor()?;
        while self.peek_starts_join() {
            left = self.join_onto(left)?;
        }
        Ok(left)
    }

    fn peek_starts_join(&self) -> bool {
        matches!(self.peek(), Token::Keyword(Keyword::Natural)) || self.peek_is_join_start()
    }

    /// Parse one join with `left` as its left operand.
    ///
    /// The right operand may itself be a join: SQL's `joined_table` is a
    /// `table_ref`, so `A LEFT JOIN B FULL JOIN C ON x ON y` groups as
    /// `A LEFT JOIN (B FULL JOIN C ON x) ON y` — the inner join claims the first
    /// qualifier and the outer one takes the next. A join that takes no
    /// qualifier (`CROSS`, `NATURAL`) ends there and stays left-associative.
    fn join_onto(
        &mut self,
        left: crate::ast::TableExpr,
    ) -> Result<crate::ast::TableExpr, ParseError> {
        use crate::ast::{JoinConstraint, JoinKind, TableExpr};
        let (kind, natural) = if self.eat_keyword(Keyword::Natural) {
            (self.join_kind()?, true)
        } else {
            (self.join_kind()?, false)
        };
        let mut right = self.table_factor()?;
        let takes_qualifier = !natural && kind != JoinKind::Cross;
        if takes_qualifier {
            while !matches!(self.peek(), Token::Keyword(Keyword::On | Keyword::Using))
                && self.peek_starts_join()
            {
                right = self.join_onto(right)?;
            }
        }
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
        Ok(TableExpr::Join {
            left: Box::new(left),
            right: Box::new(right),
            kind,
            constraint,
        })
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
            Token::Keyword(Keyword::Select | Keyword::Values | Keyword::With | Keyword::Table)
                | Token::LParen
        )
    }

    /// True when the cursor sits on a data-modifying statement, one of the four
    /// spellings `PostgreSQL` allows inside a `WITH` list.
    fn starts_dml_statement(&self) -> bool {
        matches!(
            self.peek(),
            Token::Keyword(Keyword::Insert | Keyword::Update | Keyword::Delete)
        ) || matches!(self.peek(), Token::Ident(s) if s == "merge")
    }

    fn dml_statement(&mut self) -> Result<crate::ast::Statement, ParseError> {
        match self.peek() {
            Token::Keyword(Keyword::Insert) => self.insert(),
            Token::Keyword(Keyword::Update) => self.update(),
            Token::Keyword(Keyword::Delete) => self.delete(),
            _ => self.merge(),
        }
    }

    /// A table factor: a base table (`t` / `t alias` / `t AS alias`), a derived
    /// table (`( SELECT … ) alias`), or a parenthesized join (`( … )`).
    fn table_factor(&mut self) -> Result<crate::ast::TableExpr, ParseError> {
        use crate::ast::TableExpr;
        // `LATERAL` marks the item that follows as correlated with the FROM
        // items to its left. It is matched as a soft identifier so it stays
        // usable as a column name.
        let lateral = self.eat_ident_eq("lateral");
        if *self.peek() == Token::LParen {
            self.bump();
            // `TABLE t` is a query body like any other — `set_primary` already
            // parses it — so a derived table may be spelled `(TABLE t) AS s`.
            if matches!(
                self.peek(),
                Token::Keyword(Keyword::Select | Keyword::Values | Keyword::With | Keyword::Table)
            ) {
                let subquery = self.query_expr_after_open_paren()?;
                // The alias is optional, as it has been since PostgreSQL 16.
                let alias = match self.opt_alias()? {
                    Some(alias) => alias,
                    None => self.unnamed_subquery_alias(),
                };
                let columns = self.opt_column_aliases()?;
                return Ok(TableExpr::Derived {
                    subquery,
                    alias,
                    columns,
                    lateral,
                });
            }
            let inner = self.join_tree()?;
            self.expect(&Token::RParen)?;
            return Ok(inner);
        }
        // `ROWS FROM (f(…), g(…))` — several functions expanded in lockstep.
        if self.peek_ident_eq("rows") && *self.peek2() == Token::Keyword(Keyword::From) {
            return self.rows_from(lateral);
        }
        // `JSON_TABLE(…)` is a FROM item of its own, not a function call: its
        // argument list is a grammar, not a list of expressions.
        if self.peek_ident_eq("json_table") && *self.peek2() == Token::LParen {
            self.bump();
            return self.json_table_item(lateral);
        }
        let only = self.eat_only();
        let name = self.relation_ref()?;
        // `ident (` in FROM position is a set-returning function call
        // (`unnest(tags) AS u(tag)`), never a table. A qualified call keeps its
        // dotted spelling, which is how function lookup names it.
        if *self.peek() == Token::LParen {
            return self.table_function(name.to_string(), lateral);
        }
        if lateral {
            return Err(ParseError::new(
                "LATERAL may only precede a subquery or a function call",
                self.peek_pos(),
            ));
        }
        let alias = self.opt_alias()?;
        // A base-table alias may rename columns too (`t AS q(x, y)`), exactly
        // like a derived table's.
        let columns = if alias.is_some() {
            self.opt_column_aliases()?
        } else {
            None
        };
        // PostgreSQL puts TABLESAMPLE *after* the alias, and allows it only on a
        // base table (`gram.y`: `relation_expr opt_alias_clause tablesample_clause`).
        let sample = self.opt_tablesample()?;
        Ok(TableExpr::Table {
            name,
            only,
            alias,
            columns,
            sample,
        })
    }

    /// `TABLESAMPLE <method> ( <percent> ) [ REPEATABLE ( <seed> ) ]`, or `None`
    /// when the next token does not start one.
    fn opt_tablesample(&mut self) -> Result<Option<crate::ast::TableSample>, ParseError> {
        if !self.eat_ident_eq("tablesample") {
            return Ok(None);
        }
        let method = self.expect_ident()?.to_ascii_lowercase();
        self.expect(&Token::LParen)?;
        let percent = self.expr(0)?;
        self.expect(&Token::RParen)?;
        let repeatable = if self.eat_keyword(Keyword::Repeatable) {
            self.expect(&Token::LParen)?;
            let seed = self.expr(0)?;
            self.expect(&Token::RParen)?;
            Some(seed)
        } else {
            None
        };
        Ok(Some(crate::ast::TableSample {
            method,
            percent,
            repeatable,
        }))
    }

    /// `ROWS FROM ( f(…) [AS (coldef…)] , … ) [WITH ORDINALITY] [alias]`,
    /// positioned at the `ROWS` identifier.
    fn rows_from(&mut self, lateral: bool) -> Result<crate::ast::TableExpr, ParseError> {
        self.bump(); // ROWS
        self.expect(&Token::Keyword(Keyword::From))?;
        self.expect(&Token::LParen)?;
        let mut functions = Vec::new();
        loop {
            let name = self.qualified_name_text()?;
            let args = self.table_function_args()?;
            // Inside ROWS FROM the only per-function tail is a column-definition
            // list, which is never an alias list.
            let column_defs = if self.eat_keyword(Keyword::As) {
                Some(self.column_definition_list()?)
            } else {
                None
            };
            functions.push(crate::ast::TableFuncCall {
                name,
                args,
                column_defs,
            });
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        self.finish_function_item(functions, true, lateral)
    }

    /// A function in FROM position, positioned at `(` after its name. Which
    /// function names are actually table-producing is the executor's decision;
    /// `LATERAL` is not part of the accepted grammar.
    fn table_function(
        &mut self,
        name: String,
        lateral: bool,
    ) -> Result<crate::ast::TableExpr, ParseError> {
        let args = self.table_function_args()?;
        self.finish_function_item(
            vec![crate::ast::TableFuncCall {
                name,
                args,
                column_defs: None,
            }],
            false,
            lateral,
        )
    }

    /// The parenthesized argument list of a FROM-position function call,
    /// positioned at its `(`.
    fn table_function_args(&mut self) -> Result<Vec<crate::ast::Expr>, ParseError> {
        self.expect(&Token::LParen)?;
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
        Ok(args)
    }

    /// The shared tail of every FROM-position function item: `WITH ORDINALITY`,
    /// then the alias clause. For a bare call the alias clause may be a
    /// column-*definition* list (`AS t(a int)`) and not a column-alias list
    /// (`AS t(a)`).
    fn finish_function_item(
        &mut self,
        mut functions: Vec<crate::ast::TableFuncCall>,
        rows_from: bool,
        lateral: bool,
    ) -> Result<crate::ast::TableExpr, ParseError> {
        use crate::ast::TableExpr;
        let with_ordinality = if self.eat_keyword(Keyword::With) {
            self.expect_ident_eq("ordinality")?;
            true
        } else {
            false
        };
        let mut alias = None;
        let mut column_aliases = None;
        if self.eat_keyword(Keyword::As) {
            // `AS (a int, …)` — a column-definition list with no table alias.
            if *self.peek() == Token::LParen {
                let defs = self.column_definition_list()?;
                Self::attach_column_defs(&mut functions, defs, rows_from, self.peek_pos())?;
            } else {
                alias = Some(self.expect_col_id()?);
            }
        } else if let Some(name) = self.peek_col_id() {
            self.bump();
            alias = Some(name);
        }
        if *self.peek() == Token::LParen {
            match self.function_alias_or_column_defs()? {
                FuncAliasColumns::Aliases(names) => column_aliases = Some(names),
                FuncAliasColumns::Definitions(defs) => {
                    Self::attach_column_defs(&mut functions, defs, rows_from, self.peek_pos())?;
                }
            }
        }
        Ok(TableExpr::Function {
            functions,
            rows_from,
            with_ordinality,
            lateral,
            alias,
            column_aliases,
        })
    }

    /// `JSON_TABLE( context [FORMAT JSON] , 'path' [AS name] [PASSING …]
    /// COLUMNS ( … ) [ {EMPTY [ARRAY] | ERROR} ON ERROR ] ) [alias [(cols)]]`,
    /// positioned at the `(`.
    fn json_table_item(&mut self, lateral: bool) -> Result<crate::ast::TableExpr, ParseError> {
        use crate::ast::{JsonTable, TableExpr};

        self.expect(&Token::LParen)?;
        let context = self.expr(0)?;
        self.opt_format_json();
        self.expect(&Token::Comma)?;
        let path = self.json_table_path("JSON_TABLE path specification")?;
        let path_name = if self.eat_keyword(Keyword::As) {
            Some(self.expect_col_id()?)
        } else {
            None
        };
        let passing = self.json_passing_clause()?;
        let columns_pos = self.peek_pos();
        if !self.eat_ident_eq("columns") {
            return Err(self.syntax_error_at_token());
        }
        let columns = self.json_table_column_list()?;
        let on_error = self.json_table_on_error()?;
        self.expect(&Token::RParen)?;
        let mut alias = None;
        let mut column_aliases = None;
        if self.eat_keyword(Keyword::As) {
            alias = Some(self.expect_col_id()?);
        } else if let Some(name) = self.peek_col_id() {
            self.bump();
            alias = Some(name);
        }
        if alias.is_some() {
            column_aliases = self.opt_column_aliases()?;
        }
        let table = JsonTable {
            context,
            path,
            path_name,
            passing,
            columns,
            on_error,
            alias,
            column_aliases,
            lateral,
        };
        Self::check_json_table_names(&table, columns_pos)?;
        Self::check_json_table_columns(&table.columns, columns_pos)?;
        Ok(TableExpr::JsonTable(Box::new(table)))
    }

    /// A jsonpath in `JSON_TABLE` position. `PostgreSQL`'s grammar takes only a
    /// string constant here, and says so rather than reporting a bare syntax
    /// error for `'$' || '.a'`.
    fn json_table_path(&mut self, what: &str) -> Result<String, ParseError> {
        let pos = self.peek_pos();
        let expr = self.expr(0)?;
        match expr {
            crate::ast::Expr::StringLiteral(text) => Ok(text),
            _ => Err(ParseError::new_sqlstate(
                "0A000",
                format!("only string constants are supported in {what}"),
                pos,
            )),
        }
    }

    /// `PASSING v AS name, …`, or an empty list when the clause is absent.
    fn json_passing_clause(&mut self) -> Result<Vec<(String, Expr)>, ParseError> {
        let mut passing = Vec::new();
        if self.eat_word_eq("passing") {
            loop {
                let value = self.expr(0)?;
                self.expect(&Token::Keyword(Keyword::As))?;
                passing.push((self.expect_col_id()?, value));
                if !self.eat_comma() {
                    break;
                }
            }
        }
        Ok(passing)
    }

    /// `COLUMNS ( column [, …] )`, positioned just after the `COLUMNS` word.
    /// An empty list is a syntax error in `PostgreSQL`'s grammar.
    fn json_table_column_list(&mut self) -> Result<Vec<crate::ast::JsonTableColumn>, ParseError> {
        self.expect(&Token::LParen)?;
        if *self.peek() == Token::RParen {
            return Err(self.syntax_error_at_token());
        }
        let mut columns = Vec::new();
        loop {
            columns.push(self.json_table_column()?);
            if !self.eat_comma() {
                break;
            }
        }
        self.expect(&Token::RParen)?;
        Ok(columns)
    }

    /// One `COLUMNS (…)` entry.
    fn json_table_column(&mut self) -> Result<crate::ast::JsonTableColumn, ParseError> {
        use crate::ast::{
            JsonTableColumn, JsonTableExistsColumn, JsonTableNestedColumns, JsonTableValueColumn,
        };

        if self.eat_word_eq("nested") {
            // `PATH` is optional in `NESTED [PATH] 'p'`.
            self.eat_word_eq("path");
            let path = self.json_table_path("JSON_TABLE path specification")?;
            let name = if self.eat_keyword(Keyword::As) {
                Some(self.expect_col_id()?)
            } else {
                None
            };
            if !self.eat_ident_eq("columns") {
                return Err(self.syntax_error_at_token());
            }
            let columns = self.json_table_column_list()?;
            return Ok(JsonTableColumn::Nested(Box::new(JsonTableNestedColumns {
                path,
                name,
                columns,
            })));
        }
        let name = self.expect_col_id()?;
        if self.eat_keyword(Keyword::For) {
            self.expect_ident_eq("ordinality")?;
            return Ok(JsonTableColumn::Ordinality { name });
        }
        let ty = self.parse_type_name()?;
        if self.eat_word_eq("exists") {
            let path = self.opt_json_table_column_path()?;
            let on_error = self.json_table_column_exists_on_error()?;
            return Ok(JsonTableColumn::Exists(Box::new(JsonTableExistsColumn {
                name,
                ty,
                path,
                on_error,
            })));
        }
        let format_json = self.opt_format_json();
        let path = self.opt_json_table_column_path()?;
        let wrapper = self.opt_json_wrapper()?;
        let omit_quotes = self.opt_json_quotes()?;
        let mut on_empty = None;
        let mut on_error = None;
        while let Some((behavior, which)) = self.opt_json_behavior()? {
            match which {
                JsonOnClause::Empty => on_empty = Some(behavior),
                JsonOnClause::Error => on_error = Some(behavior),
            }
        }
        Ok(JsonTableColumn::Value(Box::new(JsonTableValueColumn {
            name,
            ty,
            format_json,
            path,
            wrapper,
            omit_quotes,
            on_empty,
            on_error,
        })))
    }

    /// `PATH 'p'` on a column, or `None` for the implicit `$."name"`.
    fn opt_json_table_column_path(&mut self) -> Result<Option<String>, ParseError> {
        if self.eat_word_eq("path") {
            Ok(Some(self.json_table_path("JSON_TABLE path specification")?))
        } else {
            Ok(None)
        }
    }

    /// The `behavior ON ERROR` tail of an `EXISTS` column. `PostgreSQL` accepts
    /// any behavior word here and only then rejects the ones `JSON_EXISTS`
    /// cannot use, so `ON EMPTY` after one is a plain syntax error.
    fn json_table_column_exists_on_error(
        &mut self,
    ) -> Result<Option<crate::ast::JsonBehavior>, ParseError> {
        let start = self.pos;
        let Some(behavior) = self.json_behavior_word()? else {
            return Ok(None);
        };
        if !self.eat_word_eq("on") {
            self.pos = start;
            return Ok(None);
        }
        if !self.eat_word_eq("error") {
            return Err(self.syntax_error_at_token());
        }
        Ok(Some(behavior))
    }

    /// `{WITHOUT | WITH [CONDITIONAL | UNCONDITIONAL]} [ARRAY] WRAPPER`, or
    /// `None` when unwritten — which is what makes a column "scalar".
    fn opt_json_wrapper(&mut self) -> Result<Option<crate::ast::JsonWrapper>, ParseError> {
        use crate::ast::JsonWrapper;

        if !(self.peek_word_eq("without") || self.peek_word_eq("with")) {
            return Ok(None);
        }
        let with = self.peek_word_eq("with");
        if !["wrapper", "array", "conditional", "unconditional"]
            .iter()
            .any(|w| self.peek2_word_eq(w))
        {
            return Ok(None);
        }
        self.bump();
        let conditional = self.eat_word_eq("conditional");
        if !conditional {
            self.eat_word_eq("unconditional");
        }
        self.eat_word_eq("array");
        if !self.eat_word_eq("wrapper") {
            return Err(ParseError::new("expected WRAPPER", self.peek_pos()));
        }
        Ok(Some(match (with, conditional) {
            (false, _) => JsonWrapper::Without,
            (true, true) => JsonWrapper::Conditional,
            (true, false) => JsonWrapper::Unconditional,
        }))
    }

    /// `{KEEP | OMIT} QUOTES [ON SCALAR STRING]`, or `None` when unwritten.
    fn opt_json_quotes(&mut self) -> Result<Option<bool>, ParseError> {
        if !(self.peek_word_eq("omit") || self.peek_word_eq("keep")) {
            return Ok(None);
        }
        let omit = self.peek_word_eq("omit");
        self.bump();
        if !self.eat_word_eq("quotes") {
            return Err(ParseError::new("expected QUOTES", self.peek_pos()));
        }
        if self.eat_word_eq("on") {
            self.eat_word_eq("scalar");
            self.eat_word_eq("string");
        }
        Ok(Some(omit))
    }

    /// The `JSON_TABLE(…)`-level `ON ERROR` clause. Every behavior word parses
    /// here; which of them are *meaningful* is parse-analysis's decision, made
    /// by the executor alongside the per-column behavior checks.
    fn json_table_on_error(&mut self) -> Result<Option<crate::ast::JsonBehavior>, ParseError> {
        let start = self.pos;
        let Some(behavior) = self.json_behavior_word()? else {
            return Ok(None);
        };
        if !(self.eat_word_eq("on") && self.eat_word_eq("error")) {
            self.pos = start;
            return Ok(None);
        }
        Ok(Some(behavior))
    }

    /// `PostgreSQL`'s "duplicate `JSON_TABLE` column or path name" check: the row
    /// pattern's name, every column name and every nested path name share one
    /// namespace, checked depth-first in declaration order.
    fn check_json_table_names(
        table: &crate::ast::JsonTable,
        position: usize,
    ) -> Result<(), ParseError> {
        let mut seen: Vec<&str> = Vec::new();
        if let Some(name) = &table.path_name {
            seen.push(name);
        }
        Self::check_json_table_names_in(&table.columns, &mut seen, position)
    }

    fn check_json_table_names_in<'a>(
        columns: &'a [crate::ast::JsonTableColumn],
        seen: &mut Vec<&'a str>,
        position: usize,
    ) -> Result<(), ParseError> {
        use crate::ast::JsonTableColumn;

        for column in columns {
            match column {
                JsonTableColumn::Nested(nested) => {
                    if let Some(name) = &nested.name {
                        Self::register_json_table_name(name, seen, position)?;
                    }
                    Self::check_json_table_names_in(&nested.columns, seen, position)?;
                }
                JsonTableColumn::Ordinality { name } => {
                    Self::register_json_table_name(name, seen, position)?;
                }
                JsonTableColumn::Value(value) => {
                    Self::register_json_table_name(&value.name, seen, position)?;
                }
                JsonTableColumn::Exists(exists) => {
                    Self::register_json_table_name(&exists.name, seen, position)?;
                }
            }
        }
        Ok(())
    }

    fn register_json_table_name<'a>(
        name: &'a str,
        seen: &mut Vec<&'a str>,
        position: usize,
    ) -> Result<(), ParseError> {
        if seen.contains(&name) {
            return Err(ParseError::new_sqlstate(
                "42712",
                format!("duplicate JSON_TABLE column or path name: {name}"),
                position,
            ));
        }
        seen.push(name);
        Ok(())
    }

    /// The per-scan-level column checks the *grammar* owns: at most one `FOR
    /// ORDINALITY`, and a quotes clause only without a wrapper. Which behaviors
    /// each column kind admits is parse-analysis's job and lives in the
    /// executor, where the diagnostic can carry `PostgreSQL`'s `DETAIL` line.
    fn check_json_table_columns(
        columns: &[crate::ast::JsonTableColumn],
        position: usize,
    ) -> Result<(), ParseError> {
        use crate::ast::JsonTableColumn;

        let mut ordinality_found = false;
        for column in columns {
            match column {
                JsonTableColumn::Ordinality { .. } => {
                    if ordinality_found {
                        return Err(ParseError::new_sqlstate(
                            "42601",
                            "only one FOR ORDINALITY column is allowed",
                            position,
                        ));
                    }
                    ordinality_found = true;
                }
                JsonTableColumn::Value(value) => {
                    check_json_table_quotes(value, position)?;
                }
                JsonTableColumn::Exists(_) | JsonTableColumn::Nested(_) => {}
            }
        }
        for column in columns {
            if let JsonTableColumn::Nested(nested) = column {
                Self::check_json_table_columns(&nested.columns, position)?;
            }
        }
        Ok(())
    }

    /// The `syntax error at or near "…"` `PostgreSQL` reports for the token at
    /// the cursor.
    fn syntax_error_at_token(&self) -> ParseError {
        let word = match self.peek() {
            Token::Ident(word) | Token::StringLit(word) => word.clone(),
            Token::Keyword(_) => self.keyword_label(),
            Token::LParen => "(".into(),
            Token::RParen => ")".into(),
            Token::Comma => ",".into(),
            other => format!("{other:?}"),
        };
        ParseError::new_sqlstate(
            "42601",
            format!("syntax error at or near \"{word}\""),
            self.peek_pos(),
        )
    }

    /// Hang a top-level column-definition list on the item's single function.
    /// `ROWS FROM` gives each call its own list, so a trailing one there is a
    /// syntax error exactly as in `PostgreSQL`.
    fn attach_column_defs(
        functions: &mut [crate::ast::TableFuncCall],
        defs: Vec<crate::ast::TableFuncColumnDef>,
        rows_from: bool,
        position: usize,
    ) -> Result<(), ParseError> {
        let [function] = functions else {
            return Err(ParseError::new(
                "a column definition list is redundant for a function returning a row type",
                position,
            ));
        };
        if rows_from {
            return Err(ParseError::new(
                "a column definition list is redundant for a function returning a row type",
                position,
            ));
        }
        function.column_defs = Some(defs);
        Ok(())
    }

    /// The parenthesized list after a function item's alias, which is either a
    /// column-alias list (`(a, b)`) or a column-*definition* list (`(a int)`).
    /// They are told apart by what follows the first name: a `,` or `)` ends an
    /// alias, anything else begins a type.
    fn function_alias_or_column_defs(&mut self) -> Result<FuncAliasColumns, ParseError> {
        let alias_list = matches!(self.peek_n(2), Token::Comma | Token::RParen);
        if alias_list {
            let names = self
                .opt_column_aliases()?
                .expect("positioned at the opening paren of the list");
            return Ok(FuncAliasColumns::Aliases(names));
        }
        Ok(FuncAliasColumns::Definitions(
            self.column_definition_list()?,
        ))
    }

    /// `( name type [, …] )`, positioned at the opening paren.
    fn column_definition_list(
        &mut self,
    ) -> Result<Vec<crate::ast::TableFuncColumnDef>, ParseError> {
        self.expect(&Token::LParen)?;
        let mut defs = Vec::new();
        loop {
            let name = self.expect_ident()?;
            let ty = self.parse_type_name()?;
            defs.push(crate::ast::TableFuncColumnDef { name, ty });
            if self.eat_comma() {
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        Ok(defs)
    }

    /// An optional table alias: `AS ident`, or a bare `ident` that is not a
    /// keyword (so `FROM t JOIN …` does not read `JOIN` as an alias).
    ///
    /// A handful of clause-introducing words reach the parser as identifiers
    /// rather than keywords; they are `PostgreSQL`-reserved and so can never be
    /// a bare alias, and treating them as one would swallow the clause.
    fn opt_alias(&mut self) -> Result<Option<String>, ParseError> {
        // Both spellings take a `ColId`, not a `ColLabel`, so `FROM w AS window`
        // is a syntax error in PostgreSQL exactly as `FROM w window` is. That is
        // also what keeps the bare form from swallowing the next clause: every
        // word that may follow a FROM item — `WHERE`, `JOIN`, `TABLESAMPLE`,
        // `WINDOW`, `FETCH`, … — is reserved or a type/function-name keyword and
        // so is not a `ColId`.
        if self.eat_keyword(Keyword::As) {
            return Ok(Some(self.expect_col_id()?));
        }
        Ok(self.peek_col_id().inspect(|_| {
            self.bump();
        }))
    }

    fn opt_column_aliases(&mut self) -> Result<Option<Vec<String>>, ParseError> {
        if *self.peek() != Token::LParen {
            return Ok(None);
        }
        self.bump();
        // `alias_clause` renames columns with a `name_list`, and a `name` is a
        // `ColId`, so `v(between)` is accepted and `v(is)` is not.
        let mut cols = vec![self.expect_col_id()?];
        while self.eat_comma() {
            cols.push(self.expect_col_id()?);
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

    /// Consume `tok` if it is next.
    fn eat_token(&mut self, tok: &Token) -> bool {
        if self.peek() == tok {
            self.bump();
            true
        } else {
            false
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
        let cascade = self.eat_drop_behavior();
        Ok(Statement::DropFdw {
            name,
            if_exists,
            cascade,
        })
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
        let cascade = self.eat_drop_behavior();
        Ok(Statement::DropServer {
            name,
            if_exists,
            cascade,
        })
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
        let cascade = self.eat_drop_behavior();
        Ok(Statement::DropUserMapping {
            user,
            server,
            if_exists,
            cascade,
        })
    }

    /// `CREATE FOREIGN TABLE <name> (<col> <type>, …) SERVER <server> OPTIONS (…)`
    fn create_foreign_table(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{ColumnDef, Statement};
        self.expect(&Token::Keyword(Keyword::Create))?;
        self.expect(&Token::Keyword(Keyword::Foreign))?;
        self.expect(&Token::Keyword(Keyword::Table))?;
        let name = self.relation_ref()?;
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
        let name = self.relation_ref()?;
        let cascade = self.eat_drop_behavior();
        Ok(Statement::DropForeignTable {
            name,
            if_exists,
            cascade,
        })
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

    /// Parse `( ident, ident, … )`. `IMPORT FOREIGN SCHEMA` and
    /// `CREATE INDEX … INCLUDE` use it, and neither names a relation.
    fn parse_ident_list(&mut self) -> Result<Vec<String>, ParseError> {
        self.expect(&Token::LParen)?;
        let mut names = vec![self.expect_ident()?];
        while self.eat_comma() {
            names.push(self.expect_ident()?);
        }
        self.expect(&Token::RParen)?;
        Ok(names)
    }

    /// `( col, …, col [WITHOUT OVERLAPS] )` — the key list of a `PRIMARY KEY`
    /// or `UNIQUE` table constraint.
    ///
    /// `PostgreSQL`'s grammar admits `WITHOUT OVERLAPS` only on the *last*
    /// element, so the clause ends the list: anything but `)` after it is a
    /// syntax error, exactly as upstream reports for `PRIMARY KEY (b WITHOUT
    /// OVERLAPS, a)`.
    fn parse_key_column_list(&mut self) -> Result<(Vec<String>, bool), ParseError> {
        self.expect(&Token::LParen)?;
        let mut names = Vec::new();
        let without_overlaps = loop {
            names.push(self.expect_ident()?);
            if self.eat_without_overlaps() {
                break true;
            }
            if !self.eat_comma() {
                break false;
            }
        };
        self.expect(&Token::RParen)?;
        Ok((names, without_overlaps))
    }

    /// `WITHOUT OVERLAPS`, absent leaving the cursor untouched.
    ///
    /// `WITHOUT` is an ordinary identifier, so a lone `WITHOUT` that is not
    /// followed by `OVERLAPS` must not be consumed — it could be a column of
    /// that name in some other list. Only the pair commits.
    fn eat_without_overlaps(&mut self) -> bool {
        if !matches!(self.peek(), Token::Ident(word) if word.eq_ignore_ascii_case("without")) {
            return false;
        }
        if !matches!(self.peek2(), Token::Ident(word) if word.eq_ignore_ascii_case("overlaps")) {
            return false;
        }
        self.bump();
        self.bump();
        true
    }

    /// `( col, …, [PERIOD] col )` — the column list of a `FOREIGN KEY` clause
    /// or its `REFERENCES` target, where `PERIOD` marks the last column as the
    /// temporal one.
    ///
    /// `PostgreSQL` admits `PERIOD` only on the final element; a `PERIOD` on
    /// any earlier column is a syntax error, which falls out of requiring `)`
    /// once the marker is seen.
    fn parse_period_column_list(&mut self) -> Result<(Vec<String>, bool), ParseError> {
        self.expect(&Token::LParen)?;
        let mut names = Vec::new();
        let period = loop {
            let marked = self.eat_period_marker();
            names.push(self.expect_ident()?);
            if marked {
                break true;
            }
            if !self.eat_comma() {
                break false;
            }
        };
        self.expect(&Token::RParen)?;
        Ok((names, period))
    }

    /// The `PERIOD` marker introducing a temporal foreign-key column.
    ///
    /// `PERIOD` is an ordinary identifier, so it is only a marker when another
    /// identifier follows it; `FOREIGN KEY (period)` still names a column.
    fn eat_period_marker(&mut self) -> bool {
        if !matches!(self.peek(), Token::Ident(word) if word.eq_ignore_ascii_case("period")) {
            return false;
        }
        if !matches!(self.peek2(), Token::Ident(_)) {
            return false;
        }
        self.bump();
        true
    }

    /// Parse `( relation, relation, … )` — the `INHERITS` parent list.
    fn parse_relation_ref_list(&mut self) -> Result<Vec<crate::ast::RelationRef>, ParseError> {
        self.expect(&Token::LParen)?;
        let mut names = vec![self.relation_ref()?];
        while self.eat_comma() {
            names.push(self.relation_ref()?);
        }
        self.expect(&Token::RParen)?;
        Ok(names)
    }

    /// Consume `IF EXISTS` if present and return whether it was seen.
    ///
    /// Returns `Ok(true)` when `IF EXISTS` is consumed, `Ok(false)` when `IF`
    /// is absent, and `Err` (SQLSTATE 42601) when `IF` is present but `EXISTS`
    /// does not follow, as in a malformed clause like `DROP SERVER IF NOTEXIST s`.
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

    // ---------------------------------------------------------------------
    // P2: SQL routines. `CREATE`/`ALTER`/`DROP` of `FUNCTION`, `PROCEDURE`
    // and `ROUTINE`, plus `CALL` and `DO`. Every word in this grammar
    // (`function`, `returns`, `language`, `immutable`, …) is a plain
    // lowercased ident in this lexer, so none of them becomes reserved.
    // ---------------------------------------------------------------------

    /// The routine object word at `offset`, or `None` when the token there is
    /// not one of `FUNCTION`/`PROCEDURE`/`ROUTINE`.
    fn routine_object_at(&self, offset: usize) -> Option<crate::ast::RoutineObject> {
        use crate::ast::RoutineObject;
        let Token::Ident(word) = self.peek_n(offset) else {
            return None;
        };
        match word.as_str() {
            "function" => Some(RoutineObject::Function),
            "procedure" => Some(RoutineObject::Procedure),
            "routine" => Some(RoutineObject::Routine),
            _ => None,
        }
    }

    /// The routine kind a `CREATE …` statement defines, looking past the
    /// persistence modifiers and an `OR REPLACE`. `None` when the statement
    /// creates something else; `CREATE ROUTINE` is not `PostgreSQL` syntax.
    fn peeked_create_routine(&self) -> Option<crate::ast::RoutineObject> {
        use crate::ast::RoutineObject;
        let offset = self.create_object_keyword_offset();
        let offset = if *self.peek_n(offset) == Token::Keyword(Keyword::Or) {
            offset + 2
        } else {
            offset
        };
        match self.routine_object_at(offset) {
            Some(object @ (RoutineObject::Function | RoutineObject::Procedure)) => Some(object),
            _ => None,
        }
    }

    /// Whether the `CREATE` at the cursor is a `CREATE VIEW`, looking past an
    /// `OR REPLACE` and a `TEMP`/`TEMPORARY` storage-class word. Shaped like
    /// [`Parser::peeked_create_routine`], which has the same job for routines.
    fn peeked_create_view(&self) -> bool {
        let mut offset = self.create_object_keyword_offset();
        if *self.peek_n(offset) == Token::Keyword(Keyword::Or) {
            offset += 2;
        }
        if matches!(self.peek_n(offset), Token::Ident(word)
            if word.eq_ignore_ascii_case("temp") || word.eq_ignore_ascii_case("temporary"))
        {
            offset += 1;
        }
        *self.peek_n(offset) == Token::Keyword(Keyword::View)
    }

    /// The command identity of a `CREATE`/`ALTER`/`DROP` on `object`. `ROUTINE`
    /// is its own PG18 command row, not a synonym for either kind.
    fn routine_command_identity(
        object: crate::ast::RoutineObject,
        create: bool,
        drop: bool,
    ) -> crate::command::CommandIdentity {
        use crate::{ast::RoutineObject, command::CommandIdentity as I};
        match (object, create, drop) {
            // `CREATE ROUTINE` is not PostgreSQL syntax, so the kind-agnostic
            // spelling only ever reaches the ALTER/DROP rows.
            (RoutineObject::Function | RoutineObject::Routine, true, _) => I::CreateFunction,
            (RoutineObject::Procedure, true, _) => I::CreateProcedure,
            (RoutineObject::Function, _, true) => I::DropFunction,
            (RoutineObject::Procedure, _, true) => I::DropProcedure,
            (RoutineObject::Routine, _, true) => I::DropRoutine,
            (RoutineObject::Function, ..) => I::AlterFunction,
            (RoutineObject::Procedure, ..) => I::AlterProcedure,
            (RoutineObject::Routine, ..) => I::AlterRoutine,
        }
    }

    /// A routine's name, with the `public.` qualifier `PostgreSQL` resolves to
    /// the same schema stripped. Any other schema is `3F000`: Gres has exactly
    /// the three schemas `pg_catalog`, `information_schema` and `public`, and
    /// only `public` holds user routines.
    fn routine_name(&mut self) -> Result<String, ParseError> {
        let position = self.peek_pos();
        let first = self.expect_object_name()?;
        if *self.peek() != Token::Dot {
            return Ok(first);
        }
        self.bump();
        let object = self.expect_object_name()?;
        if first == "public" {
            return Ok(object);
        }
        Err(ParseError::new_sqlstate(
            "3F000",
            format!("schema \"{first}\" does not exist"),
            position,
        ))
    }

    /// A type written in a routine signature. Built-in names resolve through
    /// [`Parser::parse_type_name`]; anything else is carried through by name so
    /// the executor can resolve it against the catalog (a composite type named
    /// by its relation) or report `42704`.
    fn routine_type(&mut self) -> Result<crate::ast::RoutineType, ParseError> {
        use crate::ast::RoutineType;
        let start = self.pos;
        if let Ok(ty) = self.parse_type_name() {
            return Ok(RoutineType::builtin(ty, ty.name().to_string()));
        }
        self.pos = start;
        let mut name = self.routine_name()?;
        // `%TYPE` and array suffixes are accepted on an unresolved name so the
        // signature still parses; the executor reports what it cannot resolve.
        while *self.peek() == Token::LBracket {
            self.bump();
            if matches!(self.peek(), Token::IntLit(_)) {
                self.bump();
            }
            self.expect(&Token::RBracket)?;
            name.push_str("[]");
        }
        Ok(RoutineType::named(name))
    }

    /// Is the current token a word that ends a routine parameter?
    fn at_routine_arg_end(&self) -> bool {
        matches!(self.peek(), Token::Comma | Token::RParen | Token::Eq)
            || self.peek_ident_eq("default")
    }

    /// `[ argmode ] [ argname ] argtype [ { DEFAULT | = } expr ]`.
    ///
    /// `PostgreSQL` resolves the `name type` / `type` ambiguity by trying the
    /// bare type first; so does this, by parsing a type and keeping it only
    /// when the parameter ends right after it.
    fn routine_arg(&mut self) -> Result<crate::ast::RoutineArg, ParseError> {
        use crate::ast::RoutineArg;

        let mode = self.routine_arg_mode();
        let start = self.pos;
        let mut name = None;
        let mut ty = self.routine_type()?;
        if !self.at_routine_arg_end() {
            // The first word was the parameter name, not its type.
            self.pos = start;
            name = Some(self.expect_object_name()?);
            ty = self.routine_type()?;
        }
        let default = if self.eat_ident_eq("default") || *self.peek() == Token::Eq {
            if *self.peek() == Token::Eq {
                self.bump();
            }
            let start = self.peek_pos();
            let _ = self.expr(0)?;
            let end = self.peek_pos();
            Some(self.source[start..end].trim().to_string())
        } else {
            None
        };
        Ok(RoutineArg {
            name,
            mode,
            ty,
            default,
        })
    }

    /// An `IN`/`OUT`/`INOUT`/`VARIADIC` prefix, with `IN` as the default. The
    /// parser leaves a mode word that is the whole parameter (`f(out)`, a
    /// parameter of type `out`) alone, the same as `PostgreSQL`'s own
    /// resolution.
    fn routine_arg_mode(&mut self) -> crate::ast::RoutineArgMode {
        use crate::ast::RoutineArgMode;
        if *self.peek() == Token::Keyword(Keyword::In) {
            self.bump();
            // `INOUT` is one word; `IN OUT` is not PostgreSQL syntax.
            return RoutineArgMode::In;
        }
        let mode = match self.peek() {
            Token::Ident(word) if word == "out" => RoutineArgMode::Out,
            Token::Ident(word) if word == "inout" => RoutineArgMode::InOut,
            Token::Ident(word) if word == "variadic" => RoutineArgMode::Variadic,
            _ => return RoutineArgMode::In,
        };
        // Only a mode when something follows it inside the parameter.
        if matches!(self.peek_n(1), Token::Comma | Token::RParen) {
            return RoutineArgMode::In;
        }
        self.bump();
        mode
    }

    /// `( [ arg [, …] ] )`.
    fn routine_arg_list(&mut self) -> Result<Vec<crate::ast::RoutineArg>, ParseError> {
        self.expect(&Token::LParen)?;
        let mut args = Vec::new();
        if *self.peek() == Token::RParen {
            self.bump();
            return Ok(args);
        }
        loop {
            args.push(self.routine_arg()?);
            if *self.peek() == Token::Comma {
                self.bump();
                continue;
            }
            break;
        }
        self.expect(&Token::RParen)?;
        Ok(args)
    }

    /// The clause after `RETURNS`.
    fn routine_return(&mut self) -> Result<crate::ast::RoutineReturn, ParseError> {
        use crate::ast::{RoutineReturn, RoutineTableColumn};

        if *self.peek() == Token::Keyword(Keyword::Table) && matches!(self.peek2(), Token::LParen) {
            self.bump();
            self.bump();
            let mut columns = Vec::new();
            loop {
                let name = self.expect_object_name()?;
                let ty = self.routine_type()?;
                columns.push(RoutineTableColumn { name, ty });
                if *self.peek() == Token::Comma {
                    self.bump();
                    continue;
                }
                break;
            }
            self.expect(&Token::RParen)?;
            return Ok(RoutineReturn::Table(columns));
        }
        let setof = self.eat_ident_eq("setof");
        let ty = self.routine_type()?;
        Ok(RoutineReturn::Type { ty, setof })
    }

    /// The `SET name { TO | = } value | SET name FROM CURRENT | RESET name`
    /// clause shared by `CREATE FUNCTION` and `ALTER FUNCTION`.
    fn routine_set_option(&mut self, reset: bool) -> Result<crate::ast::RoutineOption, ParseError> {
        use crate::ast::RoutineOption;
        if reset {
            let name = if self.eat_keyword(Keyword::All) {
                "all".to_string()
            } else {
                self.expect_object_name()?
            };
            return Ok(RoutineOption::Set { name, value: None });
        }
        let name = self.expect_object_name()?;
        if self.eat_keyword(Keyword::From) {
            self.expect_ident_eq("current")?;
            return Ok(RoutineOption::Set { name, value: None });
        }
        if *self.peek() == Token::Eq {
            self.bump();
        } else {
            self.expect(&Token::Keyword(Keyword::To))?;
        }
        let mut parts = Vec::new();
        loop {
            parts.push(self.routine_set_value_token()?);
            if *self.peek() == Token::Comma {
                self.bump();
                continue;
            }
            break;
        }
        Ok(RoutineOption::Set {
            name,
            value: Some(parts.join(", ")),
        })
    }

    /// One `SET`-value token: an identifier, a literal, or `DEFAULT`.
    fn routine_set_value_token(&mut self) -> Result<String, ParseError> {
        let position = self.peek_pos();
        match self.bump() {
            Token::Ident(text)
            | Token::StringLit(text)
            | Token::IntLit(text)
            | Token::FloatLit(text) => Ok(text),
            Token::Keyword(Keyword::Public) => Ok("public".into()),
            Token::Keyword(Keyword::CurrentUser) => Ok("current_user".into()),
            Token::Keyword(Keyword::User) => Ok("user".into()),
            Token::Minus => match self.bump() {
                Token::IntLit(text) | Token::FloatLit(text) => Ok(format!("-{text}")),
                other => Err(ParseError::new(
                    format!("expected a number after `-`, found {other:?}"),
                    position,
                )),
            },
            other => Err(ParseError::new(
                format!("expected a configuration value, found {other:?}"),
                position,
            )),
        }
    }

    /// A signed numeric option value (`COST`/`ROWS`).
    fn routine_number(&mut self, what: &str) -> Result<f64, ParseError> {
        let position = self.peek_pos();
        let negative = if *self.peek() == Token::Minus {
            self.bump();
            true
        } else {
            false
        };
        let text = match self.bump() {
            Token::IntLit(text) | Token::FloatLit(text) => text,
            other => {
                return Err(ParseError::new(
                    format!("expected a number for {what}, found {other:?}"),
                    position,
                ));
            }
        };
        let value: f64 = text
            .parse()
            .map_err(|_| ParseError::new(format!("invalid number for {what}: {text}"), position))?;
        Ok(if negative { -value } else { value })
    }

    /// One `CREATE FUNCTION` / `ALTER FUNCTION` definition option, or `None`
    /// when the current token does not start one.
    fn routine_option(&mut self) -> Result<Option<crate::ast::RoutineOption>, ParseError> {
        use crate::ast::{RoutineOption, RoutineParallel, RoutineVolatility};

        if self.eat_ident_eq("language") {
            let language = match self.bump() {
                Token::Ident(word) => word,
                Token::StringLit(text) => text.to_ascii_lowercase(),
                other => {
                    return Err(ParseError::new(
                        format!("expected a language name, found {other:?}"),
                        self.peek_pos(),
                    ));
                }
            };
            return Ok(Some(RoutineOption::Language(language)));
        }
        if self.eat_ident_eq("immutable") {
            return Ok(Some(RoutineOption::Volatility(
                RoutineVolatility::Immutable,
            )));
        }
        if self.eat_ident_eq("stable") {
            return Ok(Some(RoutineOption::Volatility(RoutineVolatility::Stable)));
        }
        if self.eat_ident_eq("volatile") {
            return Ok(Some(RoutineOption::Volatility(RoutineVolatility::Volatile)));
        }
        if self.eat_ident_eq("strict") {
            return Ok(Some(RoutineOption::Strict(true)));
        }
        if self.eat_ident_eq("window") {
            return Ok(Some(RoutineOption::Window));
        }
        if self.eat_ident_eq("leakproof") {
            return Ok(Some(RoutineOption::Leakproof(true)));
        }
        if *self.peek() == Token::Keyword(Keyword::Not) && self.peek2_word_eq("leakproof") {
            self.bump();
            self.bump();
            return Ok(Some(RoutineOption::Leakproof(false)));
        }
        if self.eat_ident_eq("called") {
            self.expect(&Token::Keyword(Keyword::On))?;
            self.expect(&Token::Keyword(Keyword::Null))?;
            self.expect_ident_eq("input")?;
            return Ok(Some(RoutineOption::Strict(false)));
        }
        if self.peek_ident_eq("returns") && self.peek2_is_null() {
            self.bump();
            self.bump();
            self.expect(&Token::Keyword(Keyword::On))?;
            self.expect(&Token::Keyword(Keyword::Null))?;
            self.expect_ident_eq("input")?;
            return Ok(Some(RoutineOption::Strict(true)));
        }
        if self.peek_ident_eq("external") && self.peek2_word_eq("security") {
            self.bump();
        }
        if self.peek_ident_eq("security") {
            self.bump();
            if self.eat_ident_eq("definer") {
                return Ok(Some(RoutineOption::SecurityDefiner(true)));
            }
            self.expect_ident_eq("invoker")?;
            return Ok(Some(RoutineOption::SecurityDefiner(false)));
        }
        if self.eat_ident_eq("parallel") {
            let parallel = if self.eat_ident_eq("safe") {
                RoutineParallel::Safe
            } else if self.eat_ident_eq("restricted") {
                RoutineParallel::Restricted
            } else {
                self.expect_ident_eq("unsafe")?;
                RoutineParallel::Unsafe
            };
            return Ok(Some(RoutineOption::Parallel(parallel)));
        }
        if self.eat_ident_eq("cost") {
            return Ok(Some(RoutineOption::Cost(self.routine_number("COST")?)));
        }
        if self.eat_ident_eq("rows") {
            return Ok(Some(RoutineOption::Rows(self.routine_number("ROWS")?)));
        }
        if self.eat_ident_eq("support") {
            return Ok(Some(RoutineOption::Support(self.routine_name()?)));
        }
        if self.eat_ident_eq("transform") {
            let mut types = Vec::new();
            loop {
                self.expect(&Token::Keyword(Keyword::For))?;
                self.expect_ident_eq("type")?;
                types.push(self.routine_type()?.name);
                if *self.peek() == Token::Comma {
                    self.bump();
                    continue;
                }
                break;
            }
            return Ok(Some(RoutineOption::Transform(types)));
        }
        if *self.peek() == Token::Keyword(Keyword::Set) && !self.peek2_is_schema() {
            self.bump();
            return Ok(Some(self.routine_set_option(false)?));
        }
        if self.peek_ident_eq("reset") {
            self.bump();
            return Ok(Some(self.routine_set_option(true)?));
        }
        if *self.peek() == Token::Keyword(Keyword::As) {
            self.bump();
            let object_file = self.expect_string_lit()?;
            if self.eat_comma() {
                let link_symbol = self.expect_string_lit()?;
                return Ok(Some(RoutineOption::Body(
                    crate::ast::RoutineBody::External {
                        object_file,
                        link_symbol,
                    },
                )));
            }
            return Ok(Some(RoutineOption::Body(crate::ast::RoutineBody::Source(
                object_file,
            ))));
        }
        if *self.peek() == Token::Keyword(Keyword::Begin) && self.peek2_word_eq("atomic") {
            return Ok(Some(RoutineOption::Body(self.routine_atomic_body()?)));
        }
        if self.peek_ident_eq("return") {
            self.bump();
            let start = self.peek_pos();
            let expr = self.expr(0)?;
            let end = self.peek_pos();
            let text = self.source[start..end].trim().to_string();
            return Ok(Some(RoutineOption::Body(crate::ast::RoutineBody::Return {
                expr,
                text,
            })));
        }
        Ok(None)
    }

    /// Is the token after the current one `NULL`?
    fn peek2_is_null(&self) -> bool {
        *self.peek2() == Token::Keyword(Keyword::Null)
    }

    /// Is the token after the current one `SCHEMA`? Distinguishes
    /// `ALTER FUNCTION f() SET SCHEMA s` from `ALTER FUNCTION f() SET x = 1`.
    fn peek2_is_schema(&self) -> bool {
        *self.peek2() == Token::Keyword(Keyword::Schema)
    }

    /// `PostgreSQL` 14's `BEGIN ATOMIC <stmt>; … END` SQL body.
    fn routine_atomic_body(&mut self) -> Result<crate::ast::RoutineBody, ParseError> {
        self.expect(&Token::Keyword(Keyword::Begin))?;
        self.expect_ident_eq("atomic")?;
        let start = self.peek_pos();
        let mut statements = Vec::new();
        let mut end = start;
        loop {
            if *self.peek() == Token::Keyword(Keyword::End) {
                self.bump();
                break;
            }
            if *self.peek() == Token::Eof {
                return Err(ParseError::new(
                    "unterminated BEGIN ATOMIC body: expected END",
                    self.peek_pos(),
                ));
            }
            statements.push(self.statement()?.statement);
            end = self.peek_pos();
            // `PostgreSQL` requires a semicolon after every statement in the
            // body, including the last one.
            self.expect(&Token::Semicolon)?;
        }
        let text = self.source[start..end].trim().to_string();
        Ok(crate::ast::RoutineBody::Atomic { statements, text })
    }

    /// The run of definition options ending the statement.
    fn routine_options(&mut self) -> Result<Vec<crate::ast::RoutineOption>, ParseError> {
        let mut options = Vec::new();
        while let Some(option) = self.routine_option()? {
            options.push(option);
        }
        Ok(options)
    }

    /// `CREATE [OR REPLACE] { FUNCTION | PROCEDURE } name (args) …`.
    fn create_routine(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{CreateRoutineStmt, RoutineObject, RoutineReturn, Statement};

        self.expect(&Token::Keyword(Keyword::Create))?;
        let or_replace = if self.eat_keyword(Keyword::Or) {
            self.expect_ident_eq("replace")?;
            true
        } else {
            false
        };
        let object = match self.routine_object_at(0) {
            Some(object @ (RoutineObject::Function | RoutineObject::Procedure)) => {
                self.bump();
                object
            }
            _ => {
                return Err(ParseError::new(
                    format!("expected FUNCTION or PROCEDURE, found {:?}", self.peek()),
                    self.peek_pos(),
                ));
            }
        };
        let name = self.routine_name()?;
        let args = self.routine_arg_list()?;
        let returns = if self.eat_ident_eq("returns") {
            self.routine_return()?
        } else {
            RoutineReturn::Unspecified
        };
        let options = self.routine_options()?;
        self.expect_statement_end("CREATE FUNCTION")?;
        Ok(Statement::CreateRoutine(Box::new(CreateRoutineStmt {
            name,
            object,
            or_replace,
            args,
            returns,
            options,
        })))
    }

    /// A routine named for `DROP`/`ALTER`: `name` or `name(argtypes)`.
    fn routine_signature(&mut self) -> Result<crate::ast::RoutineSignature, ParseError> {
        let name = self.routine_name()?;
        let args = if *self.peek() == Token::LParen {
            Some(self.routine_arg_list()?)
        } else {
            None
        };
        Ok(crate::ast::RoutineSignature { name, args })
    }

    /// `DROP { FUNCTION | PROCEDURE | ROUTINE } …`, tagged with the PG18
    /// command row the spelling names.
    fn drop_routine_statement(&mut self) -> Result<ParsedStatement, ParseError> {
        let object = self
            .routine_object_at(1)
            .expect("the caller matched a routine object word");
        emitted(
            Self::routine_command_identity(object, false, true),
            self.drop_routine(),
        )
    }

    /// `ALTER { FUNCTION | PROCEDURE | ROUTINE } …`, tagged with the PG18
    /// command row the spelling names.
    fn alter_routine_statement(&mut self) -> Result<ParsedStatement, ParseError> {
        let object = self
            .routine_object_at(1)
            .expect("the caller matched a routine object word");
        emitted(
            Self::routine_command_identity(object, false, false),
            self.alter_routine(),
        )
    }

    /// `DROP { FUNCTION | PROCEDURE | ROUTINE } [IF EXISTS] sig [, …]
    /// [CASCADE | RESTRICT]`.
    fn drop_routine(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::Statement;

        self.expect(&Token::Keyword(Keyword::Drop))?;
        let object = self
            .routine_object_at(0)
            .expect("drop_routine is only reached on a routine object word");
        self.bump();
        let if_exists = self.eat_if_exists()?;
        let mut routines = Vec::new();
        loop {
            routines.push(self.routine_signature()?);
            if *self.peek() == Token::Comma {
                self.bump();
                continue;
            }
            break;
        }
        let cascade = self.eat_ident_eq("cascade");
        if !cascade {
            let _ = self.eat_ident_eq("restrict");
        }
        Ok(Statement::DropRoutine {
            object,
            if_exists,
            routines,
            cascade,
        })
    }

    /// `ALTER { FUNCTION | PROCEDURE | ROUTINE } sig <action>`.
    fn alter_routine(&mut self) -> Result<crate::ast::Statement, ParseError> {
        use crate::ast::{AlterRoutineAction, Statement};

        self.expect_ident_eq("alter")?;
        let object = self
            .routine_object_at(0)
            .expect("alter_routine is only reached on a routine object word");
        self.bump();
        let routine = self.routine_signature()?;
        let action = self.alter_routine_action()?;
        let _ = self.eat_ident_eq("restrict");
        Ok(Statement::AlterRoutine {
            object,
            routine,
            action: match action {
                Some(action) => action,
                None => AlterRoutineAction::Options(Vec::new()),
            },
        })
    }

    /// The action of an `ALTER { FUNCTION | PROCEDURE | ROUTINE }`.
    fn alter_routine_action(
        &mut self,
    ) -> Result<Option<crate::ast::AlterRoutineAction>, ParseError> {
        use crate::ast::AlterRoutineAction;

        if self.eat_ident_eq("rename") {
            self.expect(&Token::Keyword(Keyword::To))?;
            return Ok(Some(AlterRoutineAction::RenameTo(
                self.expect_object_name()?,
            )));
        }
        if self.eat_ident_eq("owner") {
            self.expect(&Token::Keyword(Keyword::To))?;
            return Ok(Some(AlterRoutineAction::OwnerTo(
                self.expect_object_name()?,
            )));
        }
        if *self.peek() == Token::Keyword(Keyword::Set) && self.peek2_is_schema() {
            self.bump();
            self.bump();
            return Ok(Some(AlterRoutineAction::SetSchema(
                self.expect_object_name()?,
            )));
        }
        let no = self.peek_ident_eq("no") && self.peek2_word_eq("depends");
        if no {
            self.bump();
        }
        if self.eat_ident_eq("depends") {
            self.expect(&Token::Keyword(Keyword::On))?;
            self.expect_ident_eq("extension")?;
            let name = self.expect_object_name()?;
            return Ok(Some(AlterRoutineAction::DependsOnExtension { name, no }));
        }
        let options = self.routine_options()?;
        if options.is_empty() {
            return Ok(None);
        }
        Ok(Some(AlterRoutineAction::Options(options)))
    }

    /// `CALL name ( [arg, …] )`.
    fn call_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("call")?;
        let name = self.routine_name()?;
        self.expect(&Token::LParen)?;
        let mut args = Vec::new();
        if *self.peek() != Token::RParen {
            loop {
                args.push(self.expr(0)?);
                if *self.peek() == Token::Comma {
                    self.bump();
                    continue;
                }
                break;
            }
        }
        self.expect(&Token::RParen)?;
        self.expect_statement_end("CALL")?;
        Ok(crate::ast::Statement::Call { name, args })
    }

    /// `DO [ LANGUAGE lang ] <body> [ LANGUAGE lang ]`. `PostgreSQL` defaults
    /// the language to `plpgsql`.
    fn do_stmt(&mut self) -> Result<crate::ast::Statement, ParseError> {
        self.expect_ident_eq("do")?;
        let mut language = None;
        if self.eat_ident_eq("language") {
            language = Some(self.do_language_name()?);
        }
        let body = self.expect_string_lit()?;
        if self.eat_ident_eq("language") {
            if language.is_some() {
                return Err(ParseError::new_sqlstate(
                    "42601",
                    "conflicting or redundant options",
                    self.peek_pos(),
                ));
            }
            language = Some(self.do_language_name()?);
        }
        self.expect_statement_end("DO")?;
        Ok(crate::ast::Statement::DoBlock {
            language: language.unwrap_or_else(|| "plpgsql".to_string()),
            body,
        })
    }

    /// The language name of a `DO` block: an identifier or a string literal.
    fn do_language_name(&mut self) -> Result<String, ParseError> {
        let position = self.peek_pos();
        match self.bump() {
            Token::Ident(word) => Ok(word),
            Token::StringLit(text) => Ok(text.to_ascii_lowercase()),
            other => Err(ParseError::new(
                format!("expected a language name, found {other:?}"),
                position,
            )),
        }
    }

    /// Require that `command` has consumed everything up to the statement
    /// separator, so a trailing clause is a syntax error rather than silently
    /// dropped.
    fn expect_statement_end(&mut self, command: &str) -> Result<(), ParseError> {
        if matches!(self.peek(), Token::Semicolon | Token::Eof) {
            return Ok(());
        }
        Err(ParseError::new_sqlstate(
            "42601",
            format!(
                "syntax error at end of {command}: unexpected {:?}",
                self.peek()
            ),
            self.peek_pos(),
        ))
    }
}

/// `PostgreSQL`'s 42601 for comparing two row constructors of different widths
/// (`ROW(1,2) = ROW(1,2,3)`, `(1,2) IN ((1,2,3))`), reported at parse time as
/// `PostgreSQL` does. A row compared against a non-row is left alone: that is a
/// type error for the executor to describe.
fn check_row_arity(left: &Expr, right: &Expr, position: usize) -> Result<(), ParseError> {
    if let (Expr::Row(l), Expr::Row(r)) = (left, right)
        && l.len() != r.len()
    {
        return Err(ParseError::new_sqlstate(
            "42601",
            "unequal number of entries in row expressions",
            position,
        ));
    }
    Ok(())
}

/// How an operator token is spelled, for error messages that quote the operator
/// the query wrote. `None` for a token that is not an operator at all.
fn operator_spelling(token: &Token) -> Option<&'static str> {
    Some(match token {
        Token::Lt => "<",
        Token::Gt => ">",
        Token::Le => "<=",
        Token::Ge => ">=",
        Token::Eq => "=",
        Token::Ne => "<>",
        Token::Plus => "+",
        Token::Minus => "-",
        Token::Star => "*",
        Token::Slash => "/",
        Token::Percent => "%",
        Token::Caret => "^",
        Token::Concat => "||",
        Token::Tilde => "~",
        Token::TildeCi => "~*",
        Token::NotTilde => "!~",
        Token::NotTildeCi => "!~*",
        Token::Contains => "@>",
        Token::ContainedBy => "<@",
        Token::Overlaps => "&&",
        Token::Amp => "&",
        Token::Pipe => "|",
        Token::Same => "~=",
        Token::DoesNotExtendAbove => "&<|",
        Token::DoesNotExtendBelow => "|&>",
        Token::StrictlyBelow => "<<|",
        Token::StrictlyAbove => "|>>",
        Token::DoesNotExtendRight => "&<",
        Token::DoesNotExtendLeft => "&>",
        Token::Adjacent => "-|-",
        Token::Hash => "#",
        Token::Shl => "<<",
        Token::Shr => ">>",
        Token::ContainedByOrEq => "<<=",
        Token::ContainsOrEq => ">>=",
        _ => return None,
    })
}

/// The words `PostgreSQL` 18 classifies `reserved_keyword` or
/// `type_func_name_keyword`, exactly the two categories `ColId` excludes.
/// Every other word (`unreserved_keyword`, `col_name_keyword`, and anything
/// that is not a keyword at all) is a `ColId`, so it may be a table alias or a
/// column-alias-list entry: `FROM w AS between` is accepted and `FROM w AS
/// verbose` is not.
///
/// Transcribed from `SELECT word FROM pg_get_keywords() WHERE catcode IN
/// ('R','T')` on `PostgreSQL` 18.4, and kept sorted for [`str::binary_search`].
/// Some of these words reach this parser as [`Token::Keyword`] and the rest as
/// [`Token::Ident`]. A classification by the word covers both.
const NOT_COL_ID_WORDS: &[&str] = &[
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "authorization",
    "binary",
    "both",
    "case",
    "cast",
    "check",
    "collate",
    "collation",
    "column",
    "concurrently",
    "constraint",
    "create",
    "cross",
    "current_catalog",
    "current_date",
    "current_role",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_user",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "false",
    "fetch",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "grant",
    "group",
    "having",
    "ilike",
    "in",
    "initially",
    "inner",
    "intersect",
    "into",
    "is",
    "isnull",
    "join",
    "lateral",
    "leading",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "natural",
    "not",
    "notnull",
    "null",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "outer",
    "overlaps",
    "placing",
    "primary",
    "references",
    "returning",
    "right",
    "select",
    "session_user",
    "similar",
    "some",
    "symmetric",
    "system_user",
    "table",
    "tablesample",
    "then",
    "to",
    "trailing",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "variadic",
    "verbose",
    "when",
    "where",
    "window",
    "with",
];

/// The words `PostgreSQL` 18 marks `barelabel = false`, the ones that may NOT
/// be a column alias written without `AS`, so `SELECT id over` is a syntax error
/// while `SELECT id is` names the column `is`. Independent of the `ColId`
/// categories: reserved words such as `in` and type/function-name words such as
/// `like` are bare labels, and unreserved words such as `filter` and `year` are
/// not.
///
/// Transcribed from `SELECT word FROM pg_get_keywords() WHERE NOT barelabel` on
/// `PostgreSQL` 18.4, and kept sorted for [`str::binary_search`].
const NOT_BARE_LABEL_WORDS: &[&str] = &[
    "array",
    "as",
    "char",
    "character",
    "create",
    "day",
    "except",
    "fetch",
    "filter",
    "for",
    "from",
    "grant",
    "group",
    "having",
    "hour",
    "intersect",
    "into",
    "isnull",
    "limit",
    "minute",
    "month",
    "notnull",
    "offset",
    "on",
    "order",
    "over",
    "overlaps",
    "precision",
    "returning",
    "second",
    "to",
    "union",
    "varying",
    "where",
    "window",
    "with",
    "within",
    "without",
    "year",
];

/// May `word` be spelled as a `ColId`: a table alias, or a name in a column
/// alias list?
fn is_col_id_word(word: &str) -> bool {
    NOT_COL_ID_WORDS
        .binary_search(&word.to_ascii_lowercase().as_str())
        .is_err()
}

/// May `word` be a column alias written without `AS` (`PostgreSQL`'s
/// `BareColLabel`)?
fn is_bare_label_word(word: &str) -> bool {
    NOT_BARE_LABEL_WORDS
        .binary_search(&word.to_ascii_lowercase().as_str())
        .is_err()
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

/// Public statement entry, implemented in Task 12.
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

/// Parse statements with an ordered, already-resolved type search path.
/// Built-ins resolve when `pg_catalog` is reached; user types resolve by their
/// exact `(schema, name)` identity in every other entry.
///
/// # Errors
///
/// Returns a parse error when the SQL text cannot be tokenized or parsed.
pub fn parse_with_type_schemas(
    sql: &str,
    schemas: &[String],
) -> Result<Vec<crate::ast::Statement>, ParseError> {
    if let Some((statement, _identity)) = bounded_non_goal_refusal(sql) {
        return Ok(vec![statement]);
    }
    let mut parser = Parser::new(lex(sql)?, sql.to_string()).with_type_schemas(schemas);
    Ok(parser
        .program_spanned()?
        .into_iter()
        .map(|(parsed, _)| parsed.statement)
        .collect())
}

/// Parse a standalone scalar expression — the stored source text of a `CHECK`
/// predicate, a generated-column expression, or a partial-index predicate.
///
/// # Errors
///
/// Returns a parse error when the text cannot be tokenized, is not a single
/// expression, or has trailing tokens.
pub fn parse_expression(sql: &str) -> Result<crate::ast::Expr, ParseError> {
    let mut parser = Parser::new(lex(sql)?, sql.to_string());
    let expr = parser.expr(0)?;
    match parser.peek() {
        Token::Eof | Token::Semicolon => Ok(expr),
        other => Err(ParseError::new(
            format!("unexpected token after expression: {other:?}"),
            parser.peek_pos(),
        )),
    }
}

/// Parse the type spelling used by a routine parameter or PL/pgSQL variable.
/// Unknown names are retained for catalog resolution.
///
/// # Errors
///
/// Returns a parse error when the input is not exactly one type name.
pub fn parse_routine_type(sql: &str) -> Result<crate::ast::RoutineType, ParseError> {
    let mut parser = Parser::new(lex(sql)?, sql.to_string());
    let ty = parser.routine_type()?;
    if *parser.peek() != Token::Eof {
        return Err(ParseError::new(
            "trailing tokens after type name",
            parser.peek_pos(),
        ));
    }
    Ok(ty)
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

/// Parse `sql` into statements, each paired with its EXACT source text.
///
/// The source text is the byte slice of `sql` that spans that statement,
/// trimmed of surrounding whitespace. The multi-range gateway uses this to
/// forward an INDIVIDUAL statement (not the whole `;`-separated simple-query
/// frame) to a remote range's leader. A frame that holds both a local and a
/// remote range then never re-runs the local statement on the remote node.
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

/// True when `token` begins a table-level constraint kind, which is what tells
/// `CONSTRAINT <name> …` apart from a column named `constraint`.
/// `PostgreSQL`'s refusal when a constraint carries two `DEFERRABLE` /
/// `NOT DEFERRABLE` clauses, or two `INITIALLY …` ones.
///
/// These are `42601` like an ordinary syntax error, but `PostgreSQL` words them
/// itself and does not report a token. So they skip the "syntax error at
/// position N" frame `ParseError::new` adds.
fn multiple_constraint_attribute(clause: &'static str, position: usize) -> ParseError {
    ParseError::new_sqlstate(
        "42601",
        format!("multiple {clause} clauses not allowed"),
        position,
    )
}

/// `PostgreSQL`'s refusal when `INITIALLY DEFERRED` meets an explicit
/// `NOT DEFERRABLE`, in either order. Written alone, `INITIALLY DEFERRED`
/// implies `DEFERRABLE` instead.
fn initially_deferred_must_be_deferrable(position: usize) -> ParseError {
    ParseError::new_sqlstate(
        "42601",
        "constraint declared INITIALLY DEFERRED must be DEFERRABLE",
        position,
    )
}

fn starts_constraint_kind(token: &Token) -> bool {
    match token {
        Token::Keyword(Keyword::Unique | Keyword::Foreign) => true,
        Token::Ident(word) => {
            word.eq_ignore_ascii_case("primary")
                || word.eq_ignore_ascii_case("check")
                || word.eq_ignore_ascii_case("exclude")
        }
        _ => false,
    }
}

/// True when `token` begins a column-level constraint kind.
fn starts_column_constraint_kind(token: &Token) -> bool {
    if starts_constraint_kind(token) {
        return true;
    }
    match token {
        Token::Keyword(Keyword::Not | Keyword::Null) => true,
        Token::Ident(word) => {
            word.eq_ignore_ascii_case("default")
                || word.eq_ignore_ascii_case("references")
                || word.eq_ignore_ascii_case("generated")
        }
        _ => false,
    }
}

fn encode_sequence_options(options: &crate::ast::SequenceOptions) -> Vec<crate::ast::IndexKey> {
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
        .into_iter()
        .map(|text| crate::ast::IndexKey {
            column: None,
            text,
            opclass: None,
            descending: false,
            nulls_first: None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crabka_pgtypes::ColumnType;

    use super::*;
    use crate::ast::{
        AlterTableAction, BinaryOp, ColumnConstraintKind, ColumnDef, Expr, HashShardingSpec,
        IndexPlacement, IsolationLevel, SelectItem, ShardingSpec, Statement, TableConstraint,
        TableConstraintKind, UnaryOp,
    };

    /// SQL's `joined_table` is itself a `table_ref`, so a join's right operand
    /// may be another join whose qualifier has not been written yet:
    /// `A LEFT JOIN B FULL JOIN C ON x ON y` means
    /// `A LEFT JOIN (B FULL JOIN C ON x) ON y`. The inner join claims the first
    /// `ON`. A join that takes no qualifier stays left-associative.
    #[test]
    fn a_join_right_operand_may_be_another_join() {
        use crate::ast::TableExpr;

        // The shape of a FROM item, as `kind(left, right)` with leaf names.
        fn shape(te: &TableExpr) -> String {
            match te {
                TableExpr::Table { name, .. } => name.name.clone(),
                TableExpr::Join {
                    left, right, kind, ..
                } => format!("{:?}({}, {})", kind, shape(left), shape(right)),
                other => format!("{other:?}"),
            }
        }
        fn from_shape(sql: &str) -> String {
            let Statement::Query(query) = one(sql) else {
                panic!("not a query: {sql}")
            };
            let crate::ast::SetExpr::Query(crate::ast::QueryBody::Select(select)) = query.body
            else {
                panic!("not a plain SELECT: {sql}")
            };
            shape(&select.from[0])
        }

        // (SQL, expected grouping)
        let cases: &[(&str, &str)] = &[
            // The deferred qualifier binds the inner join first.
            (
                "SELECT * FROM a LEFT JOIN b FULL JOIN c ON b.x = c.x ON a.x = b.x",
                "Left(a, Full(b, c))",
            ),
            // Which is exactly what the parenthesized spelling produces.
            (
                "SELECT * FROM a LEFT JOIN (b FULL JOIN c ON b.x = c.x) ON a.x = b.x",
                "Left(a, Full(b, c))",
            ),
            // An ordinary chain is still left-associative.
            (
                "SELECT * FROM a JOIN b ON a.x = b.x JOIN c ON b.x = c.x",
                "Inner(Inner(a, b), c)",
            ),
            // CROSS takes no qualifier, so it cannot absorb the following join.
            (
                "SELECT * FROM a CROSS JOIN b JOIN c ON b.x = c.x",
                "Inner(Cross(a, b), c)",
            ),
            // Three deferred qualifiers nest right-to-left.
            (
                "SELECT * FROM a LEFT JOIN b LEFT JOIN c LEFT JOIN d ON c.x = d.x ON b.x = c.x ON a.x = b.x",
                "Left(a, Left(b, Left(c, d)))",
            ),
        ];
        for (sql, expected) in cases {
            assert2::assert!(from_shape(sql) == *expected, "{sql}");
        }
    }

    /// The [`RelationRef`](crate::ast::RelationRef) a name is expected to parse
    /// to, written the way the SQL spells it. The split here is a test-writing
    /// convenience only. The parser splits on the token stream, never on a
    /// string.
    fn written_relation(spelling: &str) -> crate::ast::RelationRef {
        match spelling.split_once('.') {
            Some((schema, name)) => crate::ast::RelationRef::qualified(schema, name),
            None => crate::ast::RelationRef::bare(spelling),
        }
    }

    fn one(sql: &str) -> Statement {
        let mut v = parse(sql).expect("parse");
        assert_eq!(v.len(), 1);
        v.pop().expect("one statement")
    }

    /// `PostgreSQL`'s `TABLE` object-type keyword is optional, so `GRANT SELECT
    /// ON t TO r` names a table exactly as the explicit spelling does. `SCHEMA`
    /// still takes its keyword, because a bare name means a table.
    #[test]
    fn grant_and_revoke_accept_an_implicit_table_object_type() {
        use assert2::assert;
        for (implicit, explicit) in [
            ("GRANT SELECT ON t TO r", "GRANT SELECT ON TABLE t TO r"),
            (
                "GRANT SELECT, UPDATE ON s.t TO PUBLIC",
                "GRANT SELECT, UPDATE ON TABLE s.t TO PUBLIC",
            ),
            (
                "REVOKE SELECT ON t FROM CURRENT_USER",
                "REVOKE SELECT ON TABLE t FROM CURRENT_USER",
            ),
        ] {
            assert!(one(implicit) == one(explicit), "{implicit}");
        }
        assert!(matches!(
            one("GRANT USAGE ON SCHEMA s TO r"),
            Statement::GrantSchemaPrivileges { .. }
        ));
        assert!(matches!(
            one("REVOKE USAGE ON SCHEMA s FROM r"),
            Statement::RevokeSchemaPrivileges { .. }
        ));
    }

    /// `GRANT a TO b` is role membership, not a privilege grant. Both open with
    /// a comma-separated word list, so what closes it — `ON` for a privilege,
    /// `TO`/`FROM` for a role — is what tells them apart.
    #[test]
    fn grant_and_revoke_split_role_membership_from_privileges() {
        use assert2::assert;
        struct Case {
            sql: &'static str,
            want: Statement,
        }
        let cases = [
            Case {
                sql: "GRANT r1 TO r2",
                want: Statement::GrantRoles {
                    roles: vec!["r1".into()],
                    members: vec!["r2".into()],
                    admin_option: false,
                },
            },
            Case {
                sql: "GRANT r1, r2 TO r3, PUBLIC WITH ADMIN OPTION",
                want: Statement::GrantRoles {
                    roles: vec!["r1".into(), "r2".into()],
                    members: vec!["r3".into(), "public".into()],
                    admin_option: true,
                },
            },
            Case {
                sql: "REVOKE r1 FROM r2",
                want: Statement::RevokeRoles {
                    roles: vec!["r1".into()],
                    members: vec!["r2".into()],
                    admin_option: false,
                },
            },
            Case {
                sql: "REVOKE ADMIN OPTION FOR r1, r2 FROM r3",
                want: Statement::RevokeRoles {
                    roles: vec!["r1".into(), "r2".into()],
                    members: vec!["r3".into()],
                    admin_option: true,
                },
            },
        ];
        for case in cases {
            assert!(one(case.sql) == case.want, "case: {}", case.sql);
        }

        // A column list inside a privilege grant holds words that would
        // otherwise close a role list, so the scan skips it and the statement
        // still takes the privilege branch — where column-level grants are a
        // separate, unimplemented thing, which is what it fails as.
        assert!(
            crate::parse("GRANT SELECT (a, b) ON t TO r")
                .expect_err("column-level grants are not supported")
                .to_string()
                .contains("privilege list")
        );
        assert!(matches!(
            one("GRANT ALL ON SCHEMA s TO r"),
            Statement::GrantSchemaPrivileges { .. }
        ));
    }

    /// `CREATE VIEW … WITH (…)` records the reloptions it was written with, and
    /// refuses a name it does not know rather than dropping it.
    #[test]
    fn create_view_records_its_reloptions() {
        use assert2::assert;

        use crate::ast::ViewOptions;
        let cases = [
            ("CREATE VIEW v AS SELECT 1", ViewOptions::default()),
            (
                "CREATE VIEW v WITH (security_invoker) AS SELECT 1",
                ViewOptions {
                    security_invoker: true,
                    security_barrier: false,
                },
            ),
            (
                "CREATE VIEW v WITH (security_barrier = TRUE) AS SELECT 1",
                ViewOptions {
                    security_invoker: false,
                    security_barrier: true,
                },
            ),
            (
                "CREATE VIEW v WITH (security_invoker = on, security_barrier = off) AS SELECT 1",
                ViewOptions {
                    security_invoker: true,
                    security_barrier: false,
                },
            ),
            (
                // `check_option` is accepted and dropped: nothing enforces a
                // view's WITH CHECK OPTION yet.
                "CREATE VIEW v WITH (check_option = cascaded, security_barrier = 1) AS SELECT 1",
                ViewOptions {
                    security_invoker: false,
                    security_barrier: true,
                },
            ),
        ];
        for (sql, want) in cases {
            let Statement::CreateView { options, .. } = one(sql) else {
                panic!("expected CREATE VIEW from {sql}");
            };
            assert!(options == want, "case: {sql}");
        }
        for sql in [
            "CREATE VIEW v WITH (securty_invoker) AS SELECT 1",
            "CREATE VIEW v WITH (security_invoker = maybe) AS SELECT 1",
        ] {
            assert!(
                crate::parse(sql).expect_err("refused").sqlstate() == "22023",
                "case: {sql}"
            );
        }
    }

    /// `ALTER VIEW`'s three subcommands, and the ones it declines to swallow.
    #[test]
    fn alter_view_subcommands() {
        use assert2::assert;

        use crate::ast::{AlterViewAction as Action, ViewOptionName as Name};
        let cases = [
            (
                "ALTER VIEW v OWNER TO bob",
                "v",
                false,
                Action::OwnerTo("bob".into()),
            ),
            (
                "ALTER VIEW IF EXISTS s.v OWNER TO CURRENT_USER",
                "v",
                true,
                Action::OwnerTo("current_user".into()),
            ),
            (
                "ALTER VIEW v SET (security_invoker = true)",
                "v",
                false,
                Action::SetOptions(vec![(Name::SecurityInvoker, true)]),
            ),
            (
                // A bare name is `true`, and `check_option`'s enum value is
                // taken and dropped, exactly as on `CREATE VIEW`.
                "ALTER VIEW v SET (security_barrier, check_option = cascaded)",
                "v",
                false,
                Action::SetOptions(vec![
                    (Name::SecurityBarrier, true),
                    (Name::CheckOption, false),
                ]),
            ),
            (
                "ALTER VIEW v RESET (security_invoker, security_barrier)",
                "v",
                false,
                Action::ResetOptions(vec![Name::SecurityInvoker, Name::SecurityBarrier]),
            ),
        ];
        for (sql, name, if_exists, want) in cases {
            let Statement::AlterView {
                name: parsed_name,
                if_exists: parsed_if_exists,
                action,
            } = one(sql)
            else {
                panic!("expected ALTER VIEW from {sql}");
            };
            assert!(parsed_name.name == name, "case: {sql}");
            assert!(parsed_if_exists == if_exists, "case: {sql}");
            assert!(action == want, "case: {sql}");
        }
        // An unrecognized reloption is the same 22023 `CREATE VIEW` raises, so
        // a misspelling cannot take effect in one spelling and not the other.
        for sql in [
            "ALTER VIEW v SET (securty_invoker = true)",
            "ALTER VIEW v RESET (securty_invoker)",
            "ALTER VIEW v SET (security_invoker = maybe)",
        ] {
            assert!(
                crate::parse(sql).expect_err("refused").sqlstate() == "22023",
                "case: {sql}"
            );
        }
        // The subcommands this engine cannot act on are refused rather than
        // consumed and ignored.
        for sql in [
            "ALTER VIEW v RENAME TO w",
            "ALTER VIEW v SET SCHEMA other",
            "ALTER VIEW v ALTER COLUMN a SET DEFAULT 1",
        ] {
            assert!(crate::parse(sql).is_err(), "case: {sql}");
        }
    }

    /// `ONLY` in front of a DML target is the keyword, not the table name. It
    /// stays an ordinary identifier when no name follows it, so a relation
    /// actually called `only` is still reachable.
    #[test]
    fn only_binds_to_the_dml_target() {
        use assert2::assert;
        let Statement::Update { table, only, .. } = one("UPDATE ONLY t SET a = 1") else {
            panic!("expected UPDATE");
        };
        assert!(table == crate::ast::RelationRef::bare("t"));
        assert!(only);

        let Statement::Update {
            table, only, alias, ..
        } = one("UPDATE t SET a = 1")
        else {
            panic!("expected UPDATE");
        };
        assert!(table == crate::ast::RelationRef::bare("t"));
        assert!(!only);
        assert!(alias == None);

        let Statement::Delete { table, only, .. } = one("DELETE FROM ONLY public.t") else {
            panic!("expected DELETE");
        };
        assert!(table == written_relation("public.t"));
        assert!(only);

        // A bare `only` with no name after it is the relation `only`, on every
        // statement that takes the keyword.
        for sql in [
            "UPDATE only SET a = 1",
            "DELETE FROM only",
            "SELECT * FROM only",
            "TABLE only",
        ] {
            assert!(
                crate::parse(sql).is_ok(),
                "case: {sql} — `only` alone names a relation"
            );
        }
        let Statement::Update { table, only, .. } = one("UPDATE only SET a = 1") else {
            panic!("expected UPDATE");
        };
        assert!(table == crate::ast::RelationRef::bare("only"));
        assert!(!only);
    }

    /// `TABLE t` is a query body, so it may spell a derived table exactly as
    /// the equivalent `SELECT *` does.
    #[test]
    fn a_derived_table_may_be_spelled_with_the_table_query_form() {
        use assert2::assert;
        assert!(
            one("SELECT * FROM (TABLE int2_tbl) AS s (a, b)")
                == one("SELECT * FROM (SELECT * FROM int2_tbl) AS s (a, b)")
        );
        assert!(matches!(
            one("SELECT * FROM (TABLE t) AS s"),
            Statement::Query(_)
        ));
    }

    /// `CREATE`/`ALTER ROLE … WITH` record the boolean attributes, and an
    /// option the statement does not write stays `None` so `ALTER ROLE` leaves
    /// the stored value alone.
    #[test]
    fn role_options_are_parsed_and_only_written_ones_are_set() {
        use assert2::assert;

        use crate::ast::RoleOptions;
        let Statement::CreateRole {
            options, can_login, ..
        } = one("CREATE ROLE r WITH SUPERUSER CREATEDB NOINHERIT")
        else {
            panic!("CREATE ROLE")
        };
        assert!(!can_login);
        assert!(
            options
                == RoleOptions {
                    superuser: Some(true),
                    createdb: Some(true),
                    inherit: Some(false),
                    ..RoleOptions::default()
                }
        );

        let Statement::AlterRole { name, options } = one("ALTER ROLE r WITH NOSUPERUSER") else {
            panic!("ALTER ROLE")
        };
        assert!(name == "r");
        assert!(
            options
                == RoleOptions {
                    superuser: Some(false),
                    ..RoleOptions::default()
                }
        );

        for (sql, field) in [
            ("ALTER ROLE r WITH LOGIN", "login"),
            ("ALTER ROLE r NOLOGIN", "login"),
            ("ALTER ROLE r WITH BYPASSRLS", "bypassrls"),
            ("ALTER ROLE r WITH NOREPLICATION", "replication"),
            ("ALTER ROLE r WITH CREATEROLE", "createrole"),
            // `ALTER USER name` is the same statement as `ALTER ROLE name`.
            ("ALTER USER r WITH LOGIN", "login"),
        ] {
            let Statement::AlterRole { options, .. } = one(sql) else {
                panic!("{sql}")
            };
            let set = [
                ("login", options.login),
                ("bypassrls", options.bypassrls),
                ("replication", options.replication),
                ("createrole", options.createrole),
            ];
            assert!(
                set.iter().filter(|(_, v)| v.is_some()).count() == 1,
                "{sql} sets exactly one option"
            );
            assert!(
                set.iter().any(|(name, v)| *name == field && v.is_some()),
                "{sql} sets {field}"
            );
        }

        assert!(
            parse("ALTER ROLE r").is_err(),
            "an empty option list is an error"
        );
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
        select.with_ties = q.with_ties;
        select.locking = q.locking;
        select
    }

    #[test]
    fn shared_setup_catalog_ddl_parses_to_supported_statements() {
        use crate::ast::{CreateTypeDefinition, RelationRef, UtilityStatement};

        assert!(matches!(
            one("CREATE TABLESPACE regress_tblspace LOCATION ''"),
            Statement::Utility(UtilityStatement::CreateTablespace { .. })
        ));
        assert!(matches!(
            one(
                "CREATE TYPE textrange AS RANGE (SUBTYPE = text, MULTIRANGE_TYPE_NAME = multirange_of_text)"
            ),
            Statement::CreateType {
                definition: CreateTypeDefinition::Range {
                    multirange_type_name: Some(RelationRef { ref name, schema: None }),
                    ..
                },
                ..
            } if name == "multirange_of_text"
        ));
        assert!(matches!(
            one(
                "CREATE OPERATOR CLASS opc FOR TYPE int4 USING hash AS OPERATOR 1 =, FUNCTION 2 f(int4, int8)"
            ),
            Statement::Utility(UtilityStatement::CreateOperatorClass { .. })
        ));
        assert!(matches!(
            one("CREATE TYPE textrange AS RANGE (SUBTYPE = text, COLLATION = \"C\")"),
            Statement::CreateType {
                definition: CreateTypeDefinition::Range {
                    subtype: ColumnType::Text,
                    collation: Some(ref name),
                    multirange_type_name: None,
                },
                ..
            } if name == "C"
        ));
    }

    #[test]
    fn row_producing_statements_share_query_expr_shape() {
        use crate::ast::{QueryBody, SetExpr};

        let q = only_query("SELECT 1 ORDER BY 1 LIMIT 1");
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.limit, Some(Expr::IntLiteral("1".into())));
        assert!(matches!(q.body, SetExpr::Query(QueryBody::Select(_))));

        let q = only_query("VALUES (1), (2) ORDER BY 1 OFFSET 1");
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.offset, Some(Expr::IntLiteral("1".into())));
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
            or_replace,
            temporary,
            columns,
            options,
        } = one("CREATE VIEW \"Sales View\" AS SELECT id FROM orders WHERE id > 1")
        else {
            panic!("expected CREATE VIEW");
        };
        assert2::assert!(name == crate::ast::RelationRef::bare("Sales View"));
        assert_eq!(definition, "SELECT id FROM orders WHERE id > 1");
        assert!(!or_replace);
        assert!(!temporary);
        assert_eq!(columns, None);
        assert2::assert!(options == crate::ast::ViewOptions::default());

        // `OR REPLACE`, the storage-class words, and the positional column alias
        // list all reach the same statement. `TEMP`/`TEMPORARY` is carried
        // rather than swallowed: the view it creates lives in the session's
        // temporary namespace.
        for (sql, want_replace, want_temporary, want_columns) in [
            ("CREATE OR REPLACE VIEW v AS SELECT 1", true, false, None),
            ("CREATE TEMP VIEW v AS SELECT 1", false, true, None),
            (
                "CREATE OR REPLACE TEMPORARY VIEW v (x) AS SELECT 1",
                true,
                true,
                Some(vec!["x".to_string()]),
            ),
            (
                "CREATE VIEW v (x, y) AS SELECT 1, 2",
                false,
                false,
                Some(vec!["x".to_string(), "y".to_string()]),
            ),
        ] {
            let Statement::CreateView {
                or_replace,
                temporary,
                columns,
                ..
            } = one(sql)
            else {
                panic!("expected CREATE VIEW for {sql}");
            };
            assert_eq!(or_replace, want_replace, "{sql}");
            assert_eq!(temporary, want_temporary, "{sql}");
            assert_eq!(columns, want_columns, "{sql}");
        }
        assert!(matches!(
            query.body,
            crate::ast::SetExpr::Query(crate::ast::QueryBody::Select(_))
        ));
        assert_eq!(
            one("DROP VIEW IF EXISTS \"Sales View\""),
            Statement::DropView {
                name: "Sales View".into(),
                if_exists: true,
                cascade: false,
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

        let Statement::CreateIndex { keys, .. } = one("CREATE INDEX i ON t (data)") else {
            panic!("expected CREATE INDEX");
        };
        assert_eq!(keys[0].column.as_deref(), Some("data"));
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
        assert_eq!(q.limit, Some(Expr::IntLiteral("1".into())));
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
        assert_eq!(subquery.limit, Some(Expr::IntLiteral("1".into())));

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
        assert_eq!(subquery.limit, Some(Expr::IntLiteral("1".into())));
    }

    #[test]
    fn quantified_query_expr_preserves_tail() {
        let Expr::Quantified { subquery, .. } =
            expr("1 = ANY (SELECT 1 ORDER BY 1 LIMIT 1 OFFSET 0)")
        else {
            panic!("expected quantified query expression");
        };
        assert_eq!(subquery.order_by.len(), 1);
        assert_eq!(subquery.limit, Some(Expr::IntLiteral("1".into())));
        assert_eq!(subquery.offset, Some(Expr::IntLiteral("0".into())));
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
        assert_eq!(
            subquery.locking.as_ref().map(|clause| clause.strength),
            Some(RowLockStrength::ForUpdate)
        );

        assert!(crate::parse("SELECT (VALUES (1) FOR UPDATE)").is_err());
        assert!(crate::parse("SELECT (SELECT 1 UNION SELECT 2 FOR UPDATE)").is_err());
    }

    #[test]
    fn top_level_parenthesized_query_expr_tail_is_preserved() {
        use crate::ast::{QueryBody, SetExpr};

        let q = only_query("(VALUES (2), (1) ORDER BY 1 LIMIT 1)");
        assert!(matches!(q.body, SetExpr::Query(QueryBody::Values(_))));
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.limit, Some(Expr::IntLiteral("1".into())));

        let q = only_query("(SELECT 2 UNION SELECT 1 ORDER BY 1 LIMIT 1)");
        assert!(matches!(q.body, SetExpr::SetOp { .. }));
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.limit, Some(Expr::IntLiteral("1".into())));
    }

    #[test]
    fn parenthesized_query_expr_outer_tail_preserves_inner_values_and_setop_tails() {
        use crate::ast::{QueryBody, SetExpr};

        let q = only_query("(VALUES (2), (1) ORDER BY 1) LIMIT 1");
        assert_eq!(q.limit, Some(Expr::IntLiteral("1".into())));
        assert!(q.order_by.is_empty());
        let SetExpr::Query(QueryBody::Nested(inner)) = q.body else {
            panic!("expected nested VALUES query body");
        };
        assert_eq!(inner.order_by.len(), 1);
        assert_eq!(inner.limit, None);
        assert!(matches!(inner.body, SetExpr::Query(QueryBody::Values(_))));

        let q = only_query("(SELECT 2 UNION SELECT 1 ORDER BY 1) LIMIT 1");
        assert_eq!(q.limit, Some(Expr::IntLiteral("1".into())));
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
        assert_eq!(q.limit, Some(Expr::IntLiteral("1".into())));
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
        assert_eq!(q.limit, Some(Expr::IntLiteral("1".into())));
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
        assert_eq!(q.limit, Some(Expr::IntLiteral("1".into())));
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
    fn unreserved_lexer_keywords_remain_column_identifiers() {
        use assert2::assert;

        assert!(
            parse_expr_for_test("data").expect("unqualified DATA")
                == Expr::Column {
                    table: None,
                    name: "data".into(),
                }
        );
        assert!(
            parse_expr_for_test("schema.data").expect("qualified DATA")
                == Expr::Column {
                    table: Some("schema".into()),
                    name: "data".into(),
                }
        );
        assert!(parse("CREATE TABLE t (data text)").is_ok());
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

    /// A `CREATE TABLE` statement with every clause at its default, so a test
    /// only has to state the part it exercises.
    fn create_table_stmt(
        name: &str,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
    ) -> Statement {
        sharded_create_table_stmt(name, columns, constraints, false, None)
    }

    fn sharded_create_table_stmt(
        name: &str,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
        sharded: bool,
        sharding: Option<ShardingSpec>,
    ) -> Statement {
        Statement::CreateTable {
            name: name.into(),
            columns,
            constraints,
            sharded,
            sharding,
            if_not_exists: false,
            temporary: false,
            like: Vec::new(),
            inherits: Vec::new(),
            on_commit: None,
            partition_by: None,
            partition_of: None,
            tablespace: None,
        }
    }

    fn plain_column(name: &str, ty: ColumnType) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            ty,
            serial: None,
            constraints: Vec::new(),
        }
    }

    #[test]
    fn parses_create_table() {
        assert_eq!(
            one("CREATE TABLE t (id int4, name text)"),
            create_table_stmt(
                "t",
                vec![
                    plain_column("id", ColumnType::Int4),
                    plain_column("name", ColumnType::Text),
                ],
                Vec::new(),
            )
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
            columns[0].constraints[0].kind,
            ColumnConstraintKind::PrimaryKey
        ));
        assert!(matches!(
            columns[1].constraints[0].kind,
            ColumnConstraintKind::NotNull
        ));
        assert!(matches!(
            columns[1].constraints[1].kind,
            ColumnConstraintKind::Default(_)
        ));
        assert!(matches!(constraints[0].kind, TableConstraintKind::Check(_)));
        assert!(matches!(
            constraints[1].kind,
            TableConstraintKind::Unique { .. }
        ));

        let Statement::Insert { source, .. } = one("INSERT INTO t (id, name) VALUES (1, DEFAULT)")
        else {
            panic!("expected insert");
        };
        let crate::ast::InsertSource::Values(rows) = source else {
            panic!("expected a VALUES source");
        };
        assert!(matches!(rows[0][1], Expr::Default));
    }

    /// The generation expression `a * 2` every generated-column case below is
    /// written with, and the text `CheckPredicate` keeps for the catalog.
    fn a_times(literal: &str) -> crate::ast::CheckPredicate {
        crate::ast::CheckPredicate {
            expr: Expr::Binary {
                op: BinaryOp::Mul,
                left: Box::new(Expr::Column {
                    table: None,
                    name: "a".into(),
                }),
                right: Box::new(Expr::IntLiteral(literal.into())),
            },
            text: format!("a * {literal}"),
        }
    }

    /// `PostgreSQL` 18 added `VIRTUAL` generated columns and made them the
    /// default: `STORED` has to be written to get a stored column, `VIRTUAL`
    /// may be written for the default, and omitting both means `VIRTUAL`.
    #[test]
    fn a_generation_expression_defaults_to_virtual() {
        use assert2::assert;

        use crate::ast::{ColumnConstraint, ConstraintAttributes, GeneratedKind, GeneratedSpec};

        fn constraints(tail: &str) -> Vec<ColumnConstraint> {
            let sql = format!("CREATE TABLE t (a int4, b int4 GENERATED ALWAYS AS (a * 2){tail})");
            let Statement::CreateTable { mut columns, .. } = one(&sql) else {
                panic!("expected create table: {sql}");
            };
            columns.pop().expect("two columns").constraints
        }

        let expected = |kind| {
            vec![ColumnConstraint {
                name: None,
                kind: ColumnConstraintKind::Generated(GeneratedSpec {
                    predicate: a_times("2"),
                    kind,
                }),
                attributes: ConstraintAttributes::default(),
            }]
        };

        // (the text after the generation expression, the kind it means)
        for (tail, kind) in [
            (" STORED", GeneratedKind::Stored),
            (" stored", GeneratedKind::Stored),
            (" Stored", GeneratedKind::Stored),
            (" VIRTUAL", GeneratedKind::Virtual),
            (" virtual", GeneratedKind::Virtual),
            (" Virtual", GeneratedKind::Virtual),
            ("", GeneratedKind::Virtual),
        ] {
            assert!(constraints(tail) == expected(kind), "tail: {tail:?}");
        }

        // Dropping the requirement does not make the slot accept any word.
        assert!(
            crate::parse("CREATE TABLE t (a int4, b int4 GENERATED ALWAYS AS (a * 2) frobnicate)")
                .is_err()
        );
    }

    /// `BY DEFAULT` belongs to identity columns alone. With a generation
    /// expression `PostgreSQL` refuses with a message of its own rather than a
    /// bare "syntax error at or near", still under 42601.
    #[test]
    fn a_generation_expression_requires_generated_always() {
        use assert2::assert;

        use crate::ast::{IdentitySpec, SequenceOptions};

        let err = crate::parse("CREATE TABLE t (a int4, b int4 GENERATED BY DEFAULT AS (a * 2))")
            .expect_err("BY DEFAULT with a generation expression");
        assert!(err.message == "for a generated column, GENERATED ALWAYS must be specified");
        assert!(err.sqlstate() == "42601");

        // The identity spelling of `BY DEFAULT` is untouched.
        let Statement::CreateTable { columns, .. } =
            one("CREATE TABLE t (b int4 GENERATED BY DEFAULT AS IDENTITY)")
        else {
            panic!("expected create table");
        };
        assert!(
            columns[0].constraints[0].kind
                == ColumnConstraintKind::Identity(IdentitySpec {
                    always: false,
                    options: SequenceOptions::default(),
                })
        );
    }

    /// `SET EXPRESSION AS (…)` retargets a generated column and `DROP
    /// EXPRESSION` demotes it to an ordinary one; `COLUMN` is optional on both,
    /// as it is for every other `ALTER … ALTER` subcommand.
    #[test]
    fn alter_column_sets_and_drops_a_generation_expression() {
        use assert2::assert;

        // (the statement, the single action it produces)
        for (sql, expected) in [
            (
                "ALTER TABLE t ALTER COLUMN b SET EXPRESSION AS (a * 3)",
                AlterTableAction::SetExpression {
                    column: "b".into(),
                    predicate: a_times("3"),
                },
            ),
            (
                "ALTER TABLE t ALTER b SET EXPRESSION AS (a * 3)",
                AlterTableAction::SetExpression {
                    column: "b".into(),
                    predicate: a_times("3"),
                },
            ),
            (
                "ALTER TABLE t ALTER b DROP EXPRESSION",
                AlterTableAction::DropExpression {
                    column: "b".into(),
                    if_exists: false,
                },
            ),
            (
                "ALTER TABLE t ALTER COLUMN b DROP EXPRESSION IF EXISTS",
                AlterTableAction::DropExpression {
                    column: "b".into(),
                    if_exists: true,
                },
            ),
        ] {
            let Statement::AlterTable { actions, .. } = one(sql) else {
                panic!("expected alter table: {sql}");
            };
            assert!(actions == vec![expected], "{sql}");
        }
    }

    #[test]
    fn parses_exclusion_constraints() {
        let Statement::CreateTable { constraints, .. } =
            one("CREATE TABLE t (room int4range, during tstzrange, \
             EXCLUDE USING gist (room WITH =, during WITH &&))")
        else {
            panic!("expected create table");
        };
        let TableConstraintKind::Exclude { method, elements } = &constraints[0].kind else {
            panic!("expected exclusion constraint");
        };
        assert_eq!(method, "gist");
        assert_eq!(elements[0].column, "room");
        assert_eq!(elements[0].operator, BinaryOp::Eq);
        assert_eq!(elements[1].column, "during");
        assert_eq!(elements[1].operator, BinaryOp::Overlaps);
    }

    #[test]
    fn parses_create_table_sharded_suffix() {
        assert_eq!(
            one("CREATE TABLE t (id int4) SHARDED"),
            sharded_create_table_stmt(
                "t",
                vec![plain_column("id", ColumnType::Int4)],
                Vec::new(),
                true,
                None,
            )
        );
    }

    #[test]
    fn parses_create_table_hash_sharded_suffix() {
        assert_eq!(
            one("CREATE TABLE t (id int4) SHARDED BY HASH (id) BUCKETS 16"),
            sharded_create_table_stmt(
                "t",
                vec![plain_column("id", ColumnType::Int4)],
                Vec::new(),
                true,
                Some(ShardingSpec::Hash(HashShardingSpec {
                    columns: vec!["id".into()],
                    buckets: 16,
                    co_location_group: None,
                })),
            )
        );
    }

    #[test]
    fn parses_create_index_metadata() {
        assert_eq!(
            one("CREATE UNIQUE GLOBAL INDEX users_email_idx ON users (email, id)"),
            Statement::CreateIndex {
                name: Some("users_email_idx".into()),
                table: "users".into(),
                keys: vec![plain_index_key("email"), plain_index_key("id")],
                unique: true,
                placement: IndexPlacement::Global,
                if_not_exists: false,
                concurrently: false,
                method: None,
                include: Vec::new(),
                predicate: None,
                tablespace: None,
            }
        );
        assert_eq!(
            one("CREATE INDEX users_id_idx ON users (id)"),
            Statement::CreateIndex {
                name: Some("users_id_idx".into()),
                table: "users".into(),
                keys: vec![plain_index_key("id")],
                unique: false,
                placement: IndexPlacement::Local,
                if_not_exists: false,
                concurrently: false,
                method: None,
                include: Vec::new(),
                predicate: None,
                tablespace: None,
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
                cascade: false,
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
    /// An unrecognised type name is `42704 type "widget" does not exist`, the
    /// same SQLSTATE and message `PostgreSQL` 18.4 gives, not a generic syntax
    /// error. Verified against the oracle.
    fn unknown_column_type_is_error() {
        let e = parse("CREATE TABLE t (x widget)").expect_err("bad type");
        assert_eq!(e.sqlstate(), "42704");
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
                    filter: None,
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
                filter: None,
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
        assert_eq!(
            expr("9223372036854775807"),
            Expr::IntLiteral("9223372036854775807".into())
        );
        assert_eq!(
            expr("11528652096115048448"),
            Expr::NumericLiteral("11528652096115048448".into())
        );
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
                    cascade: false,
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
                    cascade: false,
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
                cascade: false,
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

    /// `FREEZE` is a load-time visibility hint the executor is free to ignore,
    /// but it still has to reach the AST — and in every spelling `pgbench -i`
    /// and `PostgreSQL`'s own boolean grammar allow.
    #[test]
    fn copy_records_the_freeze_hint_in_every_boolean_spelling() {
        use assert2::assert;
        for (sql, freeze) in [
            ("copy pgbench_accounts from stdin", false),
            ("copy pgbench_accounts from stdin with (freeze on)", true),
            ("COPY pgbench_accounts FROM STDIN WITH (FREEZE)", true),
            ("COPY pgbench_accounts FROM STDIN WITH (freeze off)", false),
            ("COPY pgbench_accounts FROM STDIN WITH (freeze true)", true),
            ("COPY pgbench_accounts FROM STDIN WITH FREEZE", true),
        ] {
            let crate::ast::Statement::Copy(copy) = one(sql) else {
                panic!("case: {sql}");
            };
            assert!(copy.options.freeze == freeze, "case: {sql}");
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
        struct Case {
            sql: &'static str,
            /// Each written name and whether `ONLY` preceded it.
            targets: &'static [(&'static str, bool)],
            restart_identity: bool,
            cascade: bool,
        }
        // The pgbench -i statement verbatim, the bare no-TABLE form, the
        // identity option tails, and both drop-behaviour spellings. Every name
        // in the list takes a schema qualifier, not just the first, and `ONLY`
        // binds to one name rather than to the whole list.
        let cases = &[
            Case {
                sql: "truncate table pgbench_accounts, pgbench_branches, pgbench_history, pgbench_tellers",
                targets: &[
                    ("pgbench_accounts", false),
                    ("pgbench_branches", false),
                    ("pgbench_history", false),
                    ("pgbench_tellers", false),
                ],
                restart_identity: false,
                cascade: false,
            },
            Case {
                sql: "TRUNCATE t",
                targets: &[("t", false)],
                restart_identity: false,
                cascade: false,
            },
            Case {
                sql: "TRUNCATE TABLE t RESTART IDENTITY",
                targets: &[("t", false)],
                restart_identity: true,
                cascade: false,
            },
            Case {
                sql: "TRUNCATE t CONTINUE IDENTITY",
                targets: &[("t", false)],
                restart_identity: false,
                cascade: false,
            },
            Case {
                sql: "TRUNCATE t, u CASCADE",
                targets: &[("t", false), ("u", false)],
                restart_identity: false,
                cascade: true,
            },
            Case {
                sql: "TRUNCATE t RESTART IDENTITY RESTRICT",
                targets: &[("t", false)],
                restart_identity: true,
                cascade: false,
            },
            Case {
                sql: "TRUNCATE t RESTART IDENTITY CASCADE",
                targets: &[("t", false)],
                restart_identity: true,
                cascade: true,
            },
            Case {
                sql: "TRUNCATE a, public.b",
                targets: &[("a", false), ("public.b", false)],
                restart_identity: false,
                cascade: false,
            },
            Case {
                sql: "TRUNCATE public.a, pg_temp.b, c CASCADE",
                targets: &[("public.a", false), ("pg_temp.b", false), ("c", false)],
                restart_identity: false,
                cascade: true,
            },
            Case {
                sql: "TRUNCATE ONLY t",
                targets: &[("t", true)],
                restart_identity: false,
                cascade: false,
            },
            Case {
                sql: "TRUNCATE TABLE ONLY public.a, b, ONLY c CASCADE",
                targets: &[("public.a", true), ("b", false), ("c", true)],
                restart_identity: false,
                cascade: true,
            },
            // The trailing `*` is PostgreSQL's explicit spelling of the default,
            // so it parses to exactly the same statement as the bare name.
            Case {
                sql: "TRUNCATE t *",
                targets: &[("t", false)],
                restart_identity: false,
                cascade: false,
            },
            // `only` with nothing after it is still the table called `only`.
            Case {
                sql: "TRUNCATE only",
                targets: &[("only", false)],
                restart_identity: false,
                cascade: false,
            },
        ];
        for case in cases {
            assert!(
                one(case.sql)
                    == Statement::Truncate {
                        targets: case
                            .targets
                            .iter()
                            .map(|&(name, only)| crate::ast::TruncateTarget {
                                name: written_relation(name),
                                only,
                            })
                            .collect(),
                        restart_identity: case.restart_identity,
                        cascade: case.cascade,
                    },
                "case: {}",
                case.sql
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
                    cascade: false,
                }
        );
    }

    #[test]
    fn parses_multi_row_insert_with_columns() {
        match one("INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')") {
            Statement::Insert {
                table,
                columns,
                source: crate::ast::InsertSource::Values(rows),
                on_conflict,
                returning,
                with,
            } => {
                assert2::assert!(table == crate::ast::RelationRef::bare("t"));
                assert_eq!(columns, Some(vec!["a".into(), "b".into()]));
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].len(), 2);
                assert_eq!(on_conflict, None);
                assert_eq!(returning, None);
                assert_eq!(with, None);
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
            [crate::ast::TableExpr::Table { name, alias: None, .. }] if *name == crate::ast::RelationRef::bare("t")
        ));
        assert!(s.filter.is_some());
        assert_eq!(s.order_by.len(), 2);
        assert!(!s.order_by[0].asc); // DESC
        assert!(s.order_by[1].asc); // default ASC
        assert_eq!(s.limit, Some(Expr::IntLiteral("10".into())));
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
                expr: Expr::Func(FuncCall {
                    ref name,
                    distinct: false,
                    args: FuncArgs::Star,
                    filter: None,
                }),
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
        assert_eq!(s.limit, Some(Expr::IntLiteral("5".into())));
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
                        filter: None,
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

    /// `BEGIN`/`START TRANSACTION` and the whole `transaction_mode` list: the
    /// isolation level, the access mode, and the deferrable flag, in any order,
    /// with the commas optional. A mode the statement omits stays `None` so the
    /// session can apply its own default.
    #[test]
    fn parses_begin_variants() {
        let begin = |isolation, read_only, deferrable| Statement::Begin {
            isolation,
            read_only,
            deferrable,
        };
        let cases: &[(&str, Statement)] = &[
            ("BEGIN", begin(None, None, None)),
            ("START TRANSACTION", begin(None, None, None)),
            (
                "BEGIN ISOLATION LEVEL REPEATABLE READ",
                begin(Some(IsolationLevel::RepeatableRead), None, None),
            ),
            (
                "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED",
                begin(Some(IsolationLevel::ReadCommitted), None, None),
            ),
            // The access mode alone, either way round.
            ("BEGIN READ ONLY", begin(None, Some(true), None)),
            (
                "START TRANSACTION READ WRITE",
                begin(None, Some(false), None),
            ),
            // The deferrable flag, and its negation.
            ("BEGIN DEFERRABLE", begin(None, None, Some(true))),
            ("BEGIN NOT DEFERRABLE", begin(None, None, Some(false))),
            // Several modes, comma-separated and not, in either order.
            (
                "BEGIN READ ONLY, DEFERRABLE",
                begin(None, Some(true), Some(true)),
            ),
            (
                "BEGIN READ ONLY NOT DEFERRABLE",
                begin(None, Some(true), Some(false)),
            ),
            (
                "START TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE",
                begin(Some(IsolationLevel::Serializable), Some(true), Some(true)),
            ),
            (
                "START TRANSACTION READ ONLY, ISOLATION LEVEL REPEATABLE READ",
                begin(Some(IsolationLevel::RepeatableRead), Some(true), None),
            ),
        ];
        for (sql, want) in cases {
            assert_eq!(&one(sql), want, "{sql}");
        }
        // `READ` must be followed by one of the two access modes.
        assert!(parse("BEGIN READ").is_err());
        assert!(parse("BEGIN READ SIDEWAYS").is_err());
    }

    #[test]
    fn start_requires_transaction_keyword() {
        // START TRANSACTION is valid; bare START is not a statement.
        assert_eq!(
            one("START TRANSACTION"),
            Statement::Begin {
                isolation: None,
                read_only: None,
                deferrable: None
            }
        );
        assert!(parse("START").is_err());
    }

    #[test]
    fn parses_commit_rollback_aliases() {
        assert_eq!(one("COMMIT"), Statement::Commit { chain: false });
        assert_eq!(one("END"), Statement::Commit { chain: false });
        assert_eq!(one("ROLLBACK"), Statement::Rollback { chain: false });
        assert_eq!(one("ABORT"), Statement::Rollback { chain: false });
        // Only the bare `AND CHAIN` spelling chains; `AND NO CHAIN` is the
        // explicit opt-out and must not be confused with it.
        for (sql, chain) in [
            ("COMMIT AND CHAIN", true),
            ("COMMIT WORK AND CHAIN", true),
            ("END AND CHAIN", true),
            ("COMMIT AND NO CHAIN", false),
            ("COMMIT TRANSACTION AND NO CHAIN", false),
        ] {
            assert_eq!(one(sql), Statement::Commit { chain }, "{sql}");
        }
        for (sql, chain) in [
            ("ROLLBACK AND CHAIN", true),
            ("ABORT AND CHAIN", true),
            ("ROLLBACK AND NO CHAIN", false),
            ("ABORT WORK AND NO CHAIN", false),
        ] {
            assert_eq!(one(sql), Statement::Rollback { chain }, "{sql}");
        }
    }

    #[test]
    fn parses_update() {
        match one("UPDATE t SET a = 1, b = a + 2 WHERE id = 5") {
            Statement::Update {
                table,
                assignments,
                from,
                filter,
                returning,
                ..
            } => {
                assert2::assert!(table == crate::ast::RelationRef::bare("t"));
                assert_eq!(assignments.len(), 2);
                assert_eq!(assignments[0].targets, vec!["a".to_string()]);
                assert_eq!(assignments[1].targets, vec!["b".to_string()]);
                assert!(from.is_empty());
                assert!(filter.is_some());
                assert_eq!(returning, None);
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn parses_select_for_update_and_share() {
        use crate::ast::RowLockStrength;
        let strength = |sql: &str| only_query(sql).locking.map(|clause| clause.strength);
        assert_eq!(
            strength("SELECT id FROM t FOR UPDATE"),
            Some(RowLockStrength::ForUpdate)
        );
        assert_eq!(
            strength("SELECT id FROM t WHERE id > 1 FOR SHARE"),
            Some(RowLockStrength::ForShare)
        );
        assert_eq!(strength("SELECT id FROM t"), None);
    }

    #[test]
    fn parses_delete() {
        match one("DELETE FROM t WHERE id > 3") {
            Statement::Delete {
                table,
                using,
                filter,
                returning,
                ..
            } => {
                assert2::assert!(table == crate::ast::RelationRef::bare("t"));
                assert!(using.is_empty());
                assert!(filter.is_some());
                assert_eq!(returning, None);
            }
            other => panic!("expected Delete, got {other:?}"),
        }
        assert_eq!(
            one("DELETE FROM t"),
            Statement::Delete {
                table: "t".into(),
                only: false,
                with: None,
                alias: None,
                filter: None,
                using: Vec::new(),
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
        assert_eq!(returning.map(|r| r.items), Some(vec![SelectItem::Wildcard]));

        let Statement::Update { returning, .. } = one("UPDATE t SET a = 1 RETURNING a AS x, a + 1")
        else {
            panic!("expected Update");
        };
        assert!(matches!(
            returning.as_ref().map(|r| r.items.as_slice()),
            Some([
                SelectItem::Expr { alias: Some(alias), .. },
                SelectItem::Expr { alias: None, .. }
            ]) if alias == "x"
        ));

        let Statement::Delete { returning, .. } = one("DELETE FROM t RETURNING t.*") else {
            panic!("expected Delete");
        };
        assert_eq!(
            returning.map(|r| r.items),
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
    fn parses_like_ilike_similar_all_combinations() {
        use crate::ast::MatchKind;
        // (SQL, negated, kind) — every spelling of the three pattern operators.
        let cases: &[(&str, bool, MatchKind)] = &[
            ("a LIKE 'x%'", false, MatchKind::Like),
            ("a NOT LIKE 'x%'", true, MatchKind::Like),
            ("a ILIKE 'x%'", false, MatchKind::ILike),
            ("a NOT ILIKE 'x%'", true, MatchKind::ILike),
            ("a SIMILAR TO 'x%'", false, MatchKind::Similar),
            ("a NOT SIMILAR TO 'x%'", true, MatchKind::Similar),
        ];
        for (sql, negated, kind) in cases {
            let expected = Expr::Like {
                expr: Box::new(Expr::Column {
                    table: None,
                    name: "a".into(),
                }),
                pattern: Box::new(Expr::StringLiteral("x%".into())),
                negated: *negated,
                kind: *kind,
                escape: None,
            };
            assert2::assert!(expr(sql) == expected, "{sql}");
        }
    }

    #[test]
    fn pattern_operators_take_an_escape_clause() {
        use crate::ast::MatchKind;
        for sql in [
            "a LIKE 'x%' ESCAPE '#'",
            "a ILIKE 'x%' ESCAPE '#'",
            "a SIMILAR TO 'x%' ESCAPE '#'",
        ] {
            let Expr::Like { escape, .. } = expr(sql) else {
                panic!("{sql} is a pattern match");
            };
            assert2::assert!(
                escape == Some(Box::new(Expr::StringLiteral("#".into()))),
                "{sql}"
            );
        }
        // The escape string binds to the pattern, not to a following conjunct.
        let Expr::Binary { op, left, .. } = expr("a LIKE 'x' ESCAPE '#' AND b") else {
            panic!("the AND is the outermost node");
        };
        assert2::assert!(op == BinaryOp::And);
        assert2::assert!(matches!(
            *left,
            Expr::Like {
                kind: MatchKind::Like,
                escape: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn typed_literals_lower_onto_the_equivalent_cast() {
        use crabka_pgtypes::ColumnType;
        // PostgreSQL's `typename 'string'` is defined to mean `'string'::typename`,
        // so every spelling must produce exactly that cast node.
        let cases: &[(&str, &str, ColumnType)] = &[
            ("bool 't'", "t", ColumnType::Bool),
            ("int4 '0'", "0", ColumnType::Int4),
            ("bigint '9'", "9", ColumnType::Int8),
            ("numeric '1.5'", "1.5", ColumnType::Numeric(None)),
            ("text 'x'", "x", ColumnType::Text),
            ("date '2024-01-01'", "2024-01-01", ColumnType::Date),
            ("interval '1 day'", "1 day", ColumnType::Interval),
            ("double precision '1.5'", "1.5", ColumnType::Float8),
            (
                "timestamp with time zone '2024-01-01 00:00:00+00'",
                "2024-01-01 00:00:00+00",
                ColumnType::Timestamptz,
            ),
            ("pg_catalog.text 'x'", "x", ColumnType::Text),
        ];
        for (sql, string, ty) in cases {
            let expected = Expr::Cast {
                expr: Box::new(Expr::StringLiteral((*string).into())),
                ty: *ty,
            };
            assert2::assert!(expr(sql) == expected, "{sql}");
        }
    }

    #[test]
    fn a_name_before_a_string_is_a_typed_literal_only_when_it_names_a_type() {
        // A type name followed by `(` is still a function call, and a plain name
        // with nothing after it is still a column.
        assert2::assert!(matches!(expr("date('2024-01-01')"), Expr::Func(_)));
        assert2::assert!(matches!(expr("text"), Expr::Column { .. }));
        assert2::assert!(matches!(expr("interval + 1"), Expr::Binary { .. }));
        // A name that is NOT a type, directly before a string, can only have been
        // meant as this syntax — PostgreSQL's own 42704.
        let error = parse_expr_for_test("widget 'x'").expect_err("not a type");
        assert2::assert!(error.sqlstate() == "42704");
        assert2::assert!(error.message == "type \"widget\" does not exist");
    }

    #[test]
    fn interval_literals_take_a_field_qualifier_or_a_field_range() {
        // `INTERVAL 'n' <field> [TO <field>]` gives an unadorned quantity its
        // unit and truncates to the range's last field. Both are properties of
        // the literal, so the qualified form is decoded here and lowered to the
        // plain interval literal it denotes.
        let interval = |text: &str| Expr::Cast {
            expr: Box::new(Expr::StringLiteral(text.into())),
            ty: crabka_pgtypes::ColumnType::Interval,
        };
        let cases: &[(&str, &str)] = &[
            ("interval '1' day", "1 day"),
            ("interval '1.5' day", "1 day"),
            // SECOND keeps its fractional part, so it is not truncated.
            ("interval '1.5' second", "00:00:01.5"),
            ("interval '90' minute", "01:30:00"),
            // A bare quantity takes the range's LAST field, and each quantity to
            // its left takes the next coarser one.
            ("interval '1' year to month", "1 mon"),
            ("interval '1 2' day to hour", "1 day 02:00:00"),
            ("interval '1-2' year to month", "1 year 2 mons"),
            ("interval '2:03' hour to minute", "02:03:00"),
        ];
        for (sql, expected) in cases {
            assert2::assert!(expr(sql) == interval(expected), "{sql}");
        }
        // An unqualified literal is passed through untouched.
        assert2::assert!(expr("interval '1 day 2 hours'") == interval("1 day 2 hours"));
        // A malformed literal keeps PostgreSQL's 22007.
        let error = parse_expr_for_test("interval 'garbage' day").expect_err("refused");
        assert2::assert!(error.sqlstate() == "22007");
    }

    #[test]
    fn the_is_family_covers_null_the_boolean_tests_and_distinct_from() {
        use crate::ast::UnaryOp;
        let column = || {
            Box::new(Expr::Column {
                table: None,
                name: "a".into(),
            })
        };
        let tests: &[(&str, UnaryOp)] = &[
            ("a IS TRUE", UnaryOp::IsTrue),
            ("a IS NOT TRUE", UnaryOp::IsNotTrue),
            ("a IS FALSE", UnaryOp::IsFalse),
            ("a IS NOT FALSE", UnaryOp::IsNotFalse),
            ("a IS UNKNOWN", UnaryOp::IsUnknown),
            ("a IS NOT UNKNOWN", UnaryOp::IsNotUnknown),
        ];
        for (sql, op) in tests {
            assert2::assert!(
                expr(sql)
                    == Expr::Unary {
                        op: *op,
                        expr: column()
                    },
                "{sql}"
            );
        }
        for (sql, negated) in [("a IS NULL", false), ("a IS NOT NULL", true)] {
            assert2::assert!(
                expr(sql)
                    == Expr::IsNull {
                        expr: column(),
                        negated
                    },
                "{sql}"
            );
        }
        for (sql, op) in [
            ("a IS DISTINCT FROM b", BinaryOp::IsDistinctFrom),
            ("a IS NOT DISTINCT FROM b", BinaryOp::IsNotDistinctFrom),
        ] {
            assert2::assert!(
                expr(sql)
                    == Expr::Binary {
                        op,
                        left: column(),
                        right: Box::new(Expr::Column {
                            table: None,
                            name: "b".into(),
                        }),
                    },
                "{sql}"
            );
        }
        // The right operand of DISTINCT FROM stays one comparand.
        assert2::assert!(matches!(
            expr("a IS DISTINCT FROM b AND c"),
            Expr::Binary {
                op: BinaryOp::And,
                ..
            }
        ));
        assert2::assert!(parse_expr_for_test("a IS 1").is_err());
    }

    #[test]
    fn row_constructors_parse_in_both_spellings() {
        let one = Expr::IntLiteral("1".into());
        let two = Expr::IntLiteral("2".into());
        assert2::assert!(expr("ROW(1, 2)") == Expr::Row(vec![one.clone(), two.clone()]));
        assert2::assert!(expr("(1, 2)") == Expr::Row(vec![one.clone(), two.clone()]));
        // ROW keeps a one-element or empty list; bare parentheses do not.
        assert2::assert!(expr("ROW(1)") == Expr::Row(vec![one.clone()]));
        assert2::assert!(expr("ROW()") == Expr::Row(vec![]));
        assert2::assert!(expr("(1)") == one);
        // A row compared with a row of a different width is PostgreSQL's 42601.
        for sql in ["ROW(1,2) = ROW(1,2,3)", "(1,2) IN ((1,2,3))"] {
            let error = parse_expr_for_test(sql).expect_err("unequal widths");
            assert2::assert!(error.sqlstate() == "42601", "{sql}");
            assert2::assert!(error.message == "unequal number of entries in row expressions");
        }
    }

    #[test]
    fn column_labels_accept_keywords() {
        use crate::ast::SelectItem;
        let label = |sql: &str| -> Option<String> {
            match only_select(sql).projection.into_iter().next() {
                Some(SelectItem::Expr { alias, .. }) => alias,
                other => panic!("{sql} projects one expression, got {other:?}"),
            }
        };
        // After AS, PostgreSQL's ColLabel admits ANY keyword — including the
        // reserved ones that can never be a bare label.
        for keyword in ["true", "select", "from", "order", "array", "as"] {
            let sql = format!("SELECT 1 AS {keyword}");
            assert2::assert!(label(&sql).as_deref() == Some(keyword), "{sql}");
        }
        // Case folds, like any unquoted identifier.
        assert2::assert!(label("SELECT 1 AS TRUE").as_deref() == Some("true"));
        // Without AS only the bare_label_keyword list applies.
        for keyword in ["select", "values", "table", "distinct", "user"] {
            let sql = format!("SELECT 1 {keyword}");
            assert2::assert!(label(&sql).as_deref() == Some(keyword), "{sql}");
        }
        // …and the excluded words still start their own clause.
        assert2::assert!(parse("SELECT 1 from").is_err());
        assert2::assert!(parse("SELECT 1 order").is_err());
        assert2::assert!(parse("SELECT 1 with").is_err());
    }

    #[test]
    fn from_item_alias_takes_a_col_id() {
        use assert2::assert;
        let alias = |sql: &str| -> Option<String> {
            match only_select(sql).from.into_iter().next() {
                Some(crate::ast::TableExpr::Table { alias, .. }) => alias,
                other => panic!("{sql} has one base-table item, got {other:?}"),
            }
        };
        // `AS ColId` and the bare `ColId` accept every unreserved and col_name
        // keyword, whichever spelling this lexer gives the word.
        for word in [
            "between", "exists", "values", "char", "row", "time", "copy", "set", "index", "delete",
            "if", "level", "schema",
        ] {
            for sql in [
                format!("SELECT * FROM w AS {word}"),
                format!("SELECT * FROM w {word}"),
            ] {
                assert!(alias(&sql).as_deref() == Some(word), "{sql}");
            }
        }
        // The reserved and type/function-name keywords are refused in BOTH
        // spellings — which is also what stops the bare form from swallowing the
        // clause each of them introduces.
        for word in [
            "authorization",
            "collation",
            "verbose",
            "binary",
            "freeze",
            "is",
            "like",
            "tablesample",
            "window",
            "fetch",
            "select",
            "from",
            "lateral",
        ] {
            for sql in [
                format!("SELECT * FROM w AS {word}"),
                format!("SELECT * FROM w {word}"),
            ] {
                assert!(parse(&sql).is_err(), "{sql}");
            }
        }
        // Quoting strips the word of every keyword property.
        assert!(alias("SELECT * FROM w AS \"window\"").as_deref() == Some("window"));
        assert!(alias("SELECT * FROM w \"tablesample\"").as_deref() == Some("tablesample"));
        // The refusal is PostgreSQL's own syntax error, not a bare "expected
        // identifier".
        let error = parse("SELECT * FROM w AS verbose").expect_err("type_func_name keyword");
        assert!(error.sqlstate() == "42601");
        assert!(error.message == "syntax error at or near \"verbose\"");
    }

    #[test]
    fn column_alias_lists_take_col_ids() {
        use assert2::assert;
        let columns = |sql: &str| -> Option<Vec<String>> {
            match only_select(sql).from.into_iter().next() {
                Some(crate::ast::TableExpr::Derived { columns, .. }) => columns,
                other => panic!("{sql} has one derived item, got {other:?}"),
            }
        };
        assert!(
            columns("SELECT * FROM (SELECT 1) v(between)") == Some(vec!["between".to_string()])
        );
        assert!(parse("SELECT * FROM (SELECT 1) v(is)").is_err());
        assert!(parse("SELECT * FROM (SELECT 1) v(tablesample)").is_err());
    }

    #[test]
    fn bare_column_labels_follow_the_barelabel_list() {
        use assert2::assert;

        use crate::ast::SelectItem;
        let label = |sql: &str| -> Option<String> {
            match only_select(sql).projection.into_iter().next() {
                Some(SelectItem::Expr { alias, .. }) => alias,
                other => panic!("{sql} projects one expression, got {other:?}"),
            }
        };
        // `barelabel = true` even though several of these are reserved or
        // type/function-name keywords, and several are also infix operators here.
        for word in [
            "is", "like", "ilike", "and", "or", "in", "between", "not", "null", "true", "similar",
            "collate", "cross", "join", "natural",
        ] {
            let sql = format!("SELECT 1 {word}");
            assert!(label(&sql).as_deref() == Some(word), "{sql}");
        }
        // `barelabel = false`, so each of these is a syntax error where a bare
        // label would go — including the unreserved ones.
        for word in [
            "over",
            "filter",
            "window",
            "fetch",
            "grant",
            "char",
            "character",
            "precision",
            "day",
            "hour",
            "minute",
            "month",
            "second",
            "year",
            "varying",
            "within",
            "without",
            "overlaps",
        ] {
            let sql = format!("SELECT 1 {word}");
            assert!(parse(&sql).is_err(), "{sql}");
        }
        // Quoted, they are ordinary labels again.
        assert!(label("SELECT 1 \"over\"").as_deref() == Some("over"));
        assert!(label("SELECT 1 \"year\"").as_deref() == Some("year"));
    }

    #[test]
    fn infix_operator_keywords_stay_operators_when_an_operand_follows() {
        use assert2::assert;
        // The lookahead that lets `SELECT 1 and` label a column must not disturb
        // the operator reading when the operand is there.
        assert!(matches!(
            expr("a AND b"),
            Expr::Binary {
                op: BinaryOp::And,
                ..
            }
        ));
        assert!(matches!(
            expr("a OR b"),
            Expr::Binary {
                op: BinaryOp::Or,
                ..
            }
        ));
        assert!(matches!(
            expr("a IS NULL"),
            Expr::IsNull { negated: false, .. }
        ));
        assert!(matches!(
            expr("a IS NOT NULL"),
            Expr::IsNull { negated: true, .. }
        ));
        assert!(matches!(expr("a IN (1, 2)"), Expr::InList { .. }));
        assert!(matches!(expr("a BETWEEN 1 AND 2"), Expr::Between { .. }));
        assert!(matches!(expr("a LIKE 'x'"), Expr::Like { .. }));
        // `ISNULL` / `NOTNULL` are PostgreSQL's postfix spellings; neither word
        // can be a column name or a bare label there, so this reading is total.
        assert!(matches!(
            expr("a ISNULL"),
            Expr::IsNull { negated: false, .. }
        ));
        assert!(matches!(
            expr("a NOTNULL"),
            Expr::IsNull { negated: true, .. }
        ));
    }

    #[test]
    fn default_outside_a_value_position_is_not_an_undefined_column() {
        use assert2::assert;
        // PostgreSQL's grammar admits DEFAULT in any `a_expr` and leaves parse
        // analysis to refuse every context but an INSERT value / UPDATE
        // assignment. Refusing it during analysis reports the same 42601 for the
        // same contexts, and does so whether or not a row ever evaluates it.
        for sql in [
            "SELECT DEFAULT",
            "SELECT id, DEFAULT FROM w",
            "SELECT DEFAULT + 1",
            "SELECT * FROM w WHERE id = DEFAULT",
            "SELECT id FROM w ORDER BY DEFAULT",
            "INSERT INTO w SELECT DEFAULT, 1",
            "VALUES (DEFAULT)",
        ] {
            let error = parse(sql).expect_err("DEFAULT outside a value position");
            assert!(error.sqlstate() == "42601", "{sql}");
            assert!(
                error.message == "DEFAULT is not allowed in this context",
                "{sql}"
            );
        }
        // The two positions PostgreSQL does allow are unaffected.
        assert!(parse("INSERT INTO w VALUES (DEFAULT, 1)").is_ok());
        assert!(parse("UPDATE w SET v = DEFAULT").is_ok());
        // Quoted it is an ordinary column again.
        assert!(matches!(
            expr("\"default\""),
            Expr::Column { table: None, .. }
        ));
    }

    #[test]
    fn qualified_update_set_target_is_an_undefined_column() {
        use assert2::assert;
        // `set_target` is `ColId opt_indirection`, so the whole qualification is
        // indirection and PostgreSQL reports the FIRST component as the missing
        // column — including when it is a word this lexer keywords.
        for (sql, column) in [
            ("UPDATE t SET t.a = 1", "t"),
            ("UPDATE t SET public.t.a = 1", "public"),
            ("UPDATE t SET \"public\".t.a = 1", "public"),
            ("UPDATE t SET db.public.t.a = 1", "db"),
        ] {
            let error = parse(sql).expect_err("qualified SET target");
            assert!(error.sqlstate() == "42703", "{sql}");
            assert!(
                error.message == format!("column \"{column}\" of relation \"t\" does not exist"),
                "{sql}"
            );
        }
        // A reserved word there is still a syntax error.
        assert!(parse("UPDATE t SET from.a = 1").is_err());
    }

    #[test]
    fn window_call_inside_a_window_specs_subquery_is_a_separate_query_level() {
        use assert2::assert;
        // The ban applies to the SELECT that owns the specification, not to a
        // query nested inside it.
        for sql in [
            "SELECT count(*) OVER (ORDER BY (SELECT rank() OVER ())) FROM w",
            "SELECT count(*) OVER (PARTITION BY (SELECT count(*) OVER () FROM w x LIMIT 1)) FROM w",
            "SELECT count(*) OVER (ORDER BY (SELECT max(r) FROM (SELECT row_number() OVER () r FROM w y) s)) FROM w",
        ] {
            assert!(parse(sql).is_ok(), "{sql}");
        }
        // A window call written directly in the specification is still 42P20 —
        // and the enclosing count is restored after the nested level.
        for sql in [
            "SELECT count(*) OVER (ORDER BY rank() OVER ()) FROM w",
            "SELECT count(*) OVER (PARTITION BY row_number() OVER ()) FROM w",
            "SELECT count(*) OVER (ORDER BY (SELECT 1), rank() OVER ()) FROM w",
        ] {
            let error = parse(sql).expect_err("window call in a window definition");
            assert!(error.sqlstate() == "42P20", "{sql}");
        }
    }

    #[test]
    fn aggregate_order_by_is_refused_as_unsupported() {
        use assert2::assert;
        // PostgreSQL refuses the windowed spelling itself, with this SQLSTATE and
        // this message.
        for sql in [
            "SELECT array_agg(v ORDER BY v) OVER (ORDER BY id) FROM w",
            "SELECT array_agg(v ORDER BY v DESC NULLS LAST) OVER () FROM w",
            "SELECT array_agg(v ORDER BY v) FILTER (WHERE v > 1) OVER () FROM w",
        ] {
            let error = parse(sql).expect_err("aggregate ORDER BY over a window");
            assert!(error.sqlstate() == "0A000", "{sql}");
            assert!(
                error.message == "aggregate ORDER BY is not implemented for window functions",
                "{sql}"
            );
        }
        // The plain spelling PostgreSQL executes; this engine cannot order an
        // aggregate's inputs, so it says so rather than ignoring the sort.
        for sql in [
            "SELECT string_agg(g, ',' ORDER BY g) FROM w",
            "SELECT array_agg(DISTINCT v ORDER BY v) FROM w",
        ] {
            let error = parse(sql).expect_err("aggregate ORDER BY");
            assert!(error.sqlstate() == "0A000", "{sql}");
            assert!(
                error.message == "aggregate ORDER BY is not supported",
                "{sql}"
            );
        }
        // An ordinary trailing ORDER BY is untouched.
        assert!(parse("SELECT array_agg(v) FROM w ORDER BY 1").is_ok());
    }

    #[test]
    fn collate_parses_as_a_postfix_operator() {
        use assert2::assert;
        assert!(
            expr("a COLLATE \"C\"")
                == Expr::Collate {
                    expr: Box::new(Expr::Column {
                        table: None,
                        name: "a".into()
                    }),
                    collation: "C".into(),
                }
        );
        // It binds as tightly as `::`, so the comparison sees the collated value.
        assert!(matches!(
            expr("a COLLATE \"C\" = b"),
            Expr::Binary {
                op: BinaryOp::Eq,
                ..
            }
        ));
        assert!(parse("SELECT c FROM w ORDER BY c COLLATE \"POSIX\"").is_ok());
        assert!(parse("SELECT 'a' COLLATE pg_catalog.\"C\"").is_ok());
        // This engine's pg_collation holds exactly `default`, `C` and `POSIX`;
        // any other name is PostgreSQL's undefined-object error, and the name is
        // case-sensitive as an identifier is.
        for (sql, collation) in [
            ("SELECT 'a' COLLATE \"en_US\"", "en_US"),
            ("SELECT 'a' COLLATE c", "c"),
            ("SELECT 'a' COLLATE \"nope\"", "nope"),
        ] {
            let error = parse(sql).expect_err("unknown collation");
            assert!(error.sqlstate() == "42704", "{sql}");
            assert!(
                error.message
                    == format!("collation \"{collation}\" for encoding \"UTF8\" does not exist"),
                "{sql}"
            );
        }
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
        use assert2::assert;

        use crate::ast::DistinctClause;
        assert!(only_select("SELECT DISTINCT a FROM t").distinct == DistinctClause::Distinct);
        assert!(only_select("SELECT a FROM t").distinct == DistinctClause::All);
        assert!(only_select("SELECT ALL a FROM t").distinct == DistinctClause::All);
    }

    #[test]
    fn parses_distinct_on_expression_list() {
        use assert2::assert;

        use crate::ast::DistinctClause;
        let cases: [(&str, DistinctClause); 3] = [
            (
                "SELECT DISTINCT ON (a) a FROM t",
                DistinctClause::On(vec![col("a")]),
            ),
            (
                "SELECT DISTINCT ON (a, b) a FROM t",
                DistinctClause::On(vec![col("a"), col("b")]),
            ),
            (
                "SELECT DISTINCT ON (t.a) a FROM t",
                DistinctClause::On(vec![Expr::Column {
                    table: Some("t".into()),
                    name: "a".into(),
                }]),
            ),
        ];
        for (sql, want) in cases {
            assert!(only_select(sql).distinct == want, "{sql}");
        }
    }

    #[test]
    fn distinct_on_requires_a_parenthesized_list() {
        use assert2::assert;

        assert!(parse("SELECT DISTINCT ON a FROM t").is_err());
        assert!(parse("SELECT DISTINCT ON () a FROM t").is_err());
    }

    fn col(name: &str) -> Expr {
        Expr::Column {
            table: None,
            name: name.into(),
        }
    }

    /// The ORDER BY items of a parsed query, as (ascending, nulls-first) pairs.
    fn order_directions(sql: &str) -> Vec<(bool, bool)> {
        only_select(sql)
            .order_by
            .iter()
            .map(|item| (item.asc, item.nulls_first))
            .collect()
    }

    #[test]
    fn order_by_resolves_null_placement_defaults_and_overrides() {
        use assert2::assert;

        let cases: [(&str, Vec<(bool, bool)>); 8] = [
            ("SELECT a FROM t ORDER BY a", vec![(true, false)]),
            ("SELECT a FROM t ORDER BY a ASC", vec![(true, false)]),
            ("SELECT a FROM t ORDER BY a DESC", vec![(false, true)]),
            ("SELECT a FROM t ORDER BY a NULLS FIRST", vec![(true, true)]),
            ("SELECT a FROM t ORDER BY a NULLS LAST", vec![(true, false)]),
            (
                "SELECT a FROM t ORDER BY a DESC NULLS LAST",
                vec![(false, false)],
            ),
            (
                "SELECT a FROM t ORDER BY a ASC NULLS FIRST",
                vec![(true, true)],
            ),
            (
                "SELECT a FROM t ORDER BY a DESC, b NULLS FIRST, c",
                vec![(false, true), (true, true), (true, false)],
            ),
        ];
        for (sql, want) in cases {
            assert!(order_directions(sql) == want, "{sql}");
        }
    }

    #[test]
    fn nulls_first_and_last_remain_usable_as_identifiers() {
        use assert2::assert;

        // PostgreSQL leaves NULLS/FIRST/LAST unreserved, so a column may be
        // called `nulls` and a table alias `first`.
        assert!(parse("SELECT nulls FROM t ORDER BY nulls").is_ok());
        assert!(parse("SELECT * FROM t AS first").is_ok());
        assert!(parse("SELECT * FROM t AS last").is_ok());
    }

    /// A parsed query's row-count window: `(limit, offset, with_ties)`.
    type RowWindow = (Option<Expr>, Option<Expr>, bool);

    /// The row-count window of a parsed query.
    fn row_window(sql: &str) -> RowWindow {
        let q = only_query(sql);
        (q.limit, q.offset, q.with_ties)
    }

    #[test]
    fn parses_every_row_count_spelling() {
        use assert2::assert;

        let one = || Some(Expr::IntLiteral("1".into()));
        let two = || Some(Expr::IntLiteral("2".into()));
        let cases: [(&str, RowWindow); 12] = [
            ("SELECT a FROM t", (None, None, false)),
            ("SELECT a FROM t LIMIT 2", (two(), None, false)),
            ("SELECT a FROM t LIMIT ALL", (None, None, false)),
            ("SELECT a FROM t OFFSET 1", (None, one(), false)),
            ("SELECT a FROM t OFFSET 1 ROW", (None, one(), false)),
            ("SELECT a FROM t OFFSET 1 ROWS", (None, one(), false)),
            ("SELECT a FROM t LIMIT 2 OFFSET 1", (two(), one(), false)),
            ("SELECT a FROM t OFFSET 1 LIMIT 2", (two(), one(), false)),
            (
                "SELECT a FROM t FETCH FIRST 2 ROWS ONLY",
                (two(), None, false),
            ),
            (
                "SELECT a FROM t FETCH NEXT 2 ROW ONLY",
                (two(), None, false),
            ),
            ("SELECT a FROM t FETCH FIRST ROW ONLY", (one(), None, false)),
            (
                "SELECT a FROM t OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
                (two(), one(), false),
            ),
        ];
        for (sql, want) in cases {
            assert!(row_window(sql) == want, "{sql}");
        }
    }

    #[test]
    fn limit_and_offset_take_arbitrary_expressions() {
        use assert2::assert;

        let (limit, offset, _) = row_window("SELECT a FROM t LIMIT 1 + 1 OFFSET $1");
        assert!(matches!(limit, Some(Expr::Binary { .. })));
        assert!(offset == Some(Expr::Param(1)));
        let (limit, _, _) = row_window("SELECT a FROM t LIMIT (SELECT 2)");
        assert!(matches!(limit, Some(Expr::ScalarSubquery(_))));
    }

    #[test]
    fn fetch_with_ties_needs_order_by() {
        use assert2::assert;

        let (limit, _, with_ties) =
            row_window("SELECT a FROM t ORDER BY a FETCH FIRST 2 ROWS WITH TIES");
        assert!(limit == Some(Expr::IntLiteral("2".into())));
        assert!(with_ties);
        // PostgreSQL rejects this in the grammar with a 42601.
        let error = parse("SELECT a FROM t FETCH FIRST 2 ROWS WITH TIES").expect_err("no ORDER BY");
        assert!(error.sqlstate() == "42601");
        assert!(error.to_string().contains("WITH TIES"));
    }

    #[test]
    fn parses_every_locking_strength_and_wait_policy() {
        use assert2::assert;

        use crate::ast::{LockWaitPolicy, LockingClause, RowLockStrength};
        let clause = |strength, of: &[&str], wait| {
            Some(LockingClause {
                strength,
                of: of.iter().map(|s| (*s).to_string()).collect(),
                wait,
            })
        };
        let cases: [(&str, Option<LockingClause>); 9] = [
            ("SELECT a FROM t", None),
            (
                "SELECT a FROM t FOR UPDATE",
                clause(RowLockStrength::ForUpdate, &[], LockWaitPolicy::Wait),
            ),
            (
                "SELECT a FROM t FOR NO KEY UPDATE",
                clause(RowLockStrength::ForNoKeyUpdate, &[], LockWaitPolicy::Wait),
            ),
            (
                "SELECT a FROM t FOR SHARE",
                clause(RowLockStrength::ForShare, &[], LockWaitPolicy::Wait),
            ),
            (
                "SELECT a FROM t FOR KEY SHARE",
                clause(RowLockStrength::ForKeyShare, &[], LockWaitPolicy::Wait),
            ),
            (
                "SELECT a FROM t FOR UPDATE NOWAIT",
                clause(RowLockStrength::ForUpdate, &[], LockWaitPolicy::NoWait),
            ),
            (
                "SELECT a FROM t FOR SHARE SKIP LOCKED",
                clause(RowLockStrength::ForShare, &[], LockWaitPolicy::SkipLocked),
            ),
            (
                "SELECT a FROM t FOR UPDATE OF t",
                clause(RowLockStrength::ForUpdate, &["t"], LockWaitPolicy::Wait),
            ),
            (
                "SELECT a FROM t, u FOR UPDATE OF t, u NOWAIT",
                clause(
                    RowLockStrength::ForUpdate,
                    &["t", "u"],
                    LockWaitPolicy::NoWait,
                ),
            ),
        ];
        for (sql, want) in cases {
            assert!(only_query(sql).locking == want, "{sql}");
        }
    }

    #[test]
    fn repeated_locking_clauses_fold_onto_the_strongest() {
        use assert2::assert;

        use crate::ast::{LockWaitPolicy, LockingClause, RowLockStrength};
        assert!(
            only_query("SELECT a FROM t, u FOR SHARE OF t FOR UPDATE OF u NOWAIT").locking
                == Some(LockingClause {
                    strength: RowLockStrength::ForUpdate,
                    of: vec!["t".into(), "u".into()],
                    wait: LockWaitPolicy::NoWait,
                })
        );
    }

    #[test]
    fn locking_refusals_carry_postgres_sqlstate_and_strength() {
        use assert2::assert;

        let cases: [(&str, &str, &str); 3] = [
            (
                "SELECT a FROM t UNION SELECT 1 FOR UPDATE",
                "0A000",
                "FOR UPDATE is not allowed with UNION/INTERSECT/EXCEPT",
            ),
            (
                "VALUES (1) FOR SHARE",
                "0A000",
                "FOR SHARE cannot be applied to VALUES",
            ),
            (
                "SELECT a FROM t INTERSECT SELECT 1 FOR KEY SHARE",
                "0A000",
                "FOR KEY SHARE is not allowed with UNION/INTERSECT/EXCEPT",
            ),
        ];
        for (sql, sqlstate, message) in cases {
            let error = parse(sql).expect_err(sql);
            assert!(error.sqlstate() == sqlstate, "{sql}");
            assert!(error.to_string() == message, "{sql}: {error}");
        }
    }

    #[test]
    fn parses_lateral_derived_tables_and_functions() {
        use assert2::assert;

        use crate::ast::TableExpr;
        let from = |sql: &str| only_select(sql).from;
        assert!(matches!(
            from("SELECT * FROM t, LATERAL (SELECT 1) u").as_slice(),
            [_, TableExpr::Derived { lateral: true, .. }]
        ));
        assert!(matches!(
            from("SELECT * FROM t, (SELECT 1) u").as_slice(),
            [_, TableExpr::Derived { lateral: false, .. }]
        ));
        assert!(matches!(
            from("SELECT * FROM t, LATERAL unnest(t.a) u").as_slice(),
            [_, TableExpr::Function { lateral: true, .. }]
        ));
        assert!(matches!(
            from("SELECT * FROM t, unnest(t.a) u").as_slice(),
            [_, TableExpr::Function { lateral: false, .. }]
        ));
        assert!(matches!(
            from("SELECT * FROM t JOIN LATERAL (SELECT 1) u ON true").as_slice(),
            [TableExpr::Join { .. }]
        ));
        // LATERAL is unreserved here, so it stays usable as a name.
        assert!(parse("SELECT lateral FROM t").is_ok());
        // …but it may not precede a plain table.
        assert!(parse("SELECT * FROM LATERAL t").is_err());
    }

    #[test]
    fn parses_function_items_with_ordinality_and_rows_from() {
        use assert2::assert;

        use crate::ast::TableExpr;
        let function = |sql: &str| match only_select(sql).from.pop() {
            Some(item @ TableExpr::Function { .. }) => item,
            other => panic!("expected a function FROM item, got {other:?}"),
        };
        let TableExpr::Function {
            functions,
            rows_from,
            with_ordinality,
            alias,
            column_aliases,
            ..
        } = function("SELECT * FROM generate_series(1, 2) WITH ORDINALITY AS g(v, n)")
        else {
            unreachable!("matched above")
        };
        assert!(functions.len() == 1);
        assert!(functions[0].name == "generate_series");
        assert!(!rows_from);
        assert!(with_ordinality);
        assert!(alias.as_deref() == Some("g"));
        assert!(column_aliases == Some(vec!["v".into(), "n".into()]));

        let TableExpr::Function {
            functions,
            rows_from,
            with_ordinality,
            ..
        } = function("SELECT * FROM ROWS FROM (generate_series(1, 2), unnest(a)) WITH ORDINALITY")
        else {
            unreachable!("matched above")
        };
        assert!(rows_from);
        assert!(with_ordinality);
        assert!(
            functions
                .iter()
                .map(|call| call.name.clone())
                .collect::<Vec<_>>()
                == vec!["generate_series".to_string(), "unnest".to_string()]
        );
        // `rows` is not reserved, so a table may still be called that.
        assert!(parse("SELECT * FROM rows").is_ok());
    }

    #[test]
    fn tells_column_alias_lists_apart_from_column_definition_lists() {
        use assert2::assert;

        use crate::ast::TableExpr;
        let TableExpr::Function {
            functions,
            column_aliases,
            ..
        } = only_select("SELECT * FROM f(1) AS t(a int4, b text)")
            .from
            .pop()
            .expect("one FROM item")
        else {
            panic!("expected a function FROM item");
        };
        assert!(column_aliases.is_none());
        let defs = functions[0].column_defs.as_ref().expect("definition list");
        assert!(defs.len() == 2);
        assert!(defs[0].name == "a" && defs[0].ty == crabka_pgtypes::ColumnType::Int4);
        assert!(defs[1].name == "b" && defs[1].ty == crabka_pgtypes::ColumnType::Text);

        let TableExpr::Function {
            functions,
            column_aliases,
            ..
        } = only_select("SELECT * FROM f(1) AS t(a, b)")
            .from
            .pop()
            .expect("one FROM item")
        else {
            panic!("expected a function FROM item");
        };
        assert!(functions[0].column_defs.is_none());
        assert!(column_aliases == Some(vec!["a".into(), "b".into()]));
    }

    #[test]
    fn parses_tablesample_after_the_alias() {
        use assert2::assert;

        use crate::ast::{TableExpr, TableSample};
        let sample = |sql: &str| match only_select(sql).from.pop() {
            Some(TableExpr::Table { sample, .. }) => sample,
            other => panic!("expected a base table, got {other:?}"),
        };
        assert!(
            sample("SELECT * FROM t TABLESAMPLE BERNOULLI (50)")
                == Some(TableSample {
                    method: "bernoulli".into(),
                    percent: Expr::IntLiteral("50".into()),
                    repeatable: None,
                })
        );
        assert!(
            sample("SELECT * FROM t AS x TABLESAMPLE SYSTEM (10) REPEATABLE (7)")
                == Some(TableSample {
                    method: "system".into(),
                    percent: Expr::IntLiteral("10".into()),
                    repeatable: Some(Expr::IntLiteral("7".into())),
                })
        );
        assert!(sample("SELECT * FROM t").is_none());
        // PostgreSQL only allows TABLESAMPLE on a base table.
        assert!(parse("SELECT * FROM (SELECT 1) x TABLESAMPLE SYSTEM (1)").is_err());
        assert!(parse("SELECT * FROM f(1) TABLESAMPLE SYSTEM (1)").is_err());
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
                value: SetValue::Value(vec!["America/New_York".into()]),
            }
        );
        assert_eq!(
            one("SET timezone TO 'UTC'"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Value(vec!["UTC".into()]),
            }
        );
        // SET TIME ZONE '...' (the special two-word spelling normalizes to `timezone`).
        assert_eq!(
            one("SET TIME ZONE 'America/New_York'"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Value(vec!["America/New_York".into()]),
            }
        );
        // An identifier value (no quotes) is accepted too.
        assert_eq!(
            one("SET timezone TO utc"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Value(vec!["utc".into()]),
            }
        );
        // The GUC name is normalized to lowercase.
        assert_eq!(
            one("SET TimeZone = 'UTC'"),
            Statement::Set {
                local: false,
                name: "timezone".into(),
                value: SetValue::Value(vec!["UTC".into()]),
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
                value: SetValue::Value(vec!["UTC".into()]),
            }
        );
        assert_eq!(
            one("SET LOCAL TIME ZONE 'America/New_York'"),
            Statement::Set {
                local: true,
                name: "timezone".into(),
                value: SetValue::Value(vec!["America/New_York".into()]),
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
            Statement::Discard {
                target: crate::ast::DiscardTarget::All
            }
        );
        assert_eq!(
            one("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"),
            Statement::Set {
                local: false,
                name: "__set_transaction".into(),
                value: crate::ast::SetValue::Value(vec!["read committed".into()])
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
                    value: SetValue::Value(vec!["session-app".into()]),
                }
            );
        }
        assert_eq!(
            one("SET extra_float_digits = -15"),
            Statement::Set {
                local: false,
                name: "extra_float_digits".into(),
                value: SetValue::Value(vec!["-15".into()]),
            }
        );
        // A comma keeps the items apart, because a list parameter re-quotes
        // each one on output; a scalar one such as `DateStyle` joins them back
        // with `", "` when it is set.
        assert_eq!(
            one("SET DateStyle TO ISO, MDY"),
            Statement::Set {
                local: false,
                name: "datestyle".into(),
                value: SetValue::Value(vec!["iso".into(), "mdy".into()]),
            }
        );
        let Statement::Set { value, .. } = one("SET DateStyle TO ISO, MDY") else {
            panic!("SET parses as a Set statement");
        };
        assert_eq!(value.plain(), "iso, mdy");
        let statements = crate::parser::parse("SET DateStyle TO SQL DMY; SHOW DateStyle").unwrap();
        assert_eq!(
            statements[0],
            Statement::Set {
                local: false,
                name: "datestyle".into(),
                value: SetValue::Value(vec!["sql dmy".into()]),
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
        // Every DISCARD target parses to its own typed statement.
        for (sql, target) in [
            ("DISCARD ALL", crate::ast::DiscardTarget::All),
            ("DISCARD PLANS", crate::ast::DiscardTarget::Plans),
            ("DISCARD SEQUENCES", crate::ast::DiscardTarget::Sequences),
            ("DISCARD TEMP", crate::ast::DiscardTarget::Temporary),
            ("DISCARD TEMPORARY", crate::ast::DiscardTarget::Temporary),
        ] {
            assert_eq!(one(sql), Statement::Discard { target }, "{sql}");
        }
        let error = crate::parser::parse("DISCARD NOTHING").expect_err("unknown DISCARD target");
        assert!(error.to_string().contains("PLANS"));
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
                value: SetValue::Value(vec!["ISO, MDY".into()]),
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
            assert_eq!(q.limit, Some(Expr::IntLiteral("5".into())));
            assert_eq!(q.offset, Some(Expr::IntLiteral("10".into())));
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
        assert!(
            matches!(&s.from[1], TableExpr::Table { name, alias: None, .. } if *name == crate::ast::RelationRef::bare("c"))
        );
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
            [crate::ast::TableExpr::Table { name, alias: None, .. }] if *name == crate::ast::RelationRef::qualified("information_schema", "tables")
        ));
    }

    #[test]
    fn parses_qualified_wildcard() {
        use crate::ast::SelectItem;
        let s = only_select("SELECT a.* FROM a JOIN b ON a.id=b.id");
        assert_eq!(s.projection[0], SelectItem::QualifiedWildcard("a".into()));
    }

    /// A derived table's alias is optional, as it has been since `PostgreSQL` 16.
    /// Each subquery written without one gets its own synthesized name, so two in
    /// the same FROM do not collide.
    #[test]
    fn derived_table_alias_is_optional() {
        use crate::ast::TableExpr;

        let names = |sql| {
            only_select(sql)
                .from
                .into_iter()
                .map(|item| match item {
                    TableExpr::Derived { alias, .. } => alias,
                    other => panic!("expected a derived table, got {other:?}"),
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(names("SELECT * FROM (SELECT 1)"), ["unnamed_subquery"]);
        assert_eq!(names("SELECT * FROM (SELECT 1) q"), ["q"]);
        assert_eq!(
            names("SELECT * FROM (SELECT 1), (SELECT 2)"),
            ["unnamed_subquery", "unnamed_subquery_1"]
        );
        // An alias in the middle does not consume a synthesized name.
        assert_eq!(
            names("SELECT * FROM (SELECT 1), (SELECT 2) q, (SELECT 3)"),
            ["unnamed_subquery", "q", "unnamed_subquery_1"]
        );
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
    /// admits. This test uses `MAX_DEPTH - 3` for one extra level of headroom.
    #[test]
    fn at_limit_parens_parse_ok() {
        let n = MAX_DEPTH - 3;
        let sql = format!("SELECT {}1{}", "(".repeat(n), ")".repeat(n));
        parse(&sql).expect("a query nested at the limit must parse, not crash");
    }

    /// The guard actually fires near the limit (not merely far away): a paren
    /// nest a few levels OVER `MAX_DEPTH` returns 54001, while the `at_limit`
    /// test above proves a nest just UNDER it still parses. So the boundary is
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

    /// A modest real-world nesting depth (well under the limit) parses fine.
    /// The guard does not reject ordinary queries.
    #[test]
    fn modest_nesting_parses_fine() {
        let sql = format!("SELECT {}1{}", "(".repeat(20), ")".repeat(20));
        parse(&sql).expect("modest nesting must parse");
        // A flat chain of 20 additions is fine too.
        parse(&format!("SELECT {}1", "1+".repeat(20))).expect("modest chain must parse");
    }

    /// Mode 2 (set ops): the `set_expr` LOOP parses a long flat
    /// `… UNION ALL …` chain. The loop builds an N-deep left-nested `SetExpr`
    /// that would overflow the executor's `fold`/`resolve_set_columns` AND
    /// recursive `Drop`. The loop iteration cap returns a clean 54001.
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

    /// A modest `UNION` chain (well under the limit) parses fine. The cap does
    /// not reject ordinary set-op queries.
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
        assert_eq!(q.limit, Some(Expr::IntLiteral("1".into())));
        assert_eq!(q.offset, Some(Expr::IntLiteral("1".into())));
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
            ..
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
            ..
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
        assert_eq!(q.limit, Some(Expr::IntLiteral("5".into())));
        assert_eq!(q.offset, Some(Expr::IntLiteral("1".into())));
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
        assert_eq!(b.limit, Some(Expr::IntLiteral("1".into())));
        assert_eq!(b.order_by.len(), 1);
    }

    #[test]
    fn plain_select_is_unchanged() {
        let q = only_query("SELECT a FROM t ORDER BY a LIMIT 3");
        assert!(matches!(
            q.body,
            crate::ast::SetExpr::Query(crate::ast::QueryBody::Select(_))
        ));
        assert_eq!(q.limit, Some(Expr::IntLiteral("3".into())));
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
        assert_eq!(inner.limit, Some(Expr::IntLiteral("1".into())));
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
            with.ctes[0].body.as_query().expect("query body").body,
            SetExpr::Query(QueryBody::Select(_))
        ));
        assert!(matches!(
            with.ctes[1].body.as_query().expect("query body").body,
            SetExpr::Query(QueryBody::Values(_))
        ));

        let q = only_query("WITH u AS (SELECT 1 UNION SELECT 2) SELECT * FROM u");
        assert!(matches!(
            q.with.as_ref().expect("with").ctes[0]
                .body
                .as_query()
                .expect("query body")
                .body,
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
                assert2::assert!(name == crate::ast::RelationRef::bare("orders"));
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
                cascade: false,
            }
        );
        assert_eq!(
            one("DROP FOREIGN DATA WRAPPER IF EXISTS kafka_fdw"),
            Statement::DropFdw {
                name: "kafka_fdw".into(),
                if_exists: true,
                cascade: false,
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
                cascade: false,
            }
        );
        assert_eq!(
            one("DROP SERVER IF EXISTS s"),
            Statement::DropServer {
                name: "s".into(),
                if_exists: true,
                cascade: false,
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
                cascade: false,
            }
        );
        assert_eq!(
            one("DROP USER MAPPING IF EXISTS FOR PUBLIC SERVER s"),
            Statement::DropUserMapping {
                user: "public".into(),
                server: "s".into(),
                if_exists: true,
                cascade: false,
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
                cascade: false,
            }
        );
        assert_eq!(
            one("DROP FOREIGN TABLE IF EXISTS orders"),
            Statement::DropForeignTable {
                name: "orders".into(),
                if_exists: true,
                cascade: false,
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
                assert2::assert!(name == crate::ast::RelationRef::bare("t"));
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
                cascade: false,
            }
        );
        assert_eq!(
            one("DROP SERVER IF EXISTS myserver"),
            Statement::DropServer {
                name: "myserver".into(),
                if_exists: true,
                cascade: false,
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
                cascade: false,
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
                cascade: false,
            }
        );
        // Also verify plain DROP FOREIGN TABLE (no IF EXISTS)
        assert_eq!(
            one("DROP FOREIGN TABLE mytable"),
            Statement::DropForeignTable {
                name: "mytable".into(),
                if_exists: false,
                cascade: false,
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
                cascade: false,
            }
        );
        assert_eq!(
            one("DROP FOREIGN DATA WRAPPER myfdw"),
            Statement::DropFdw {
                name: "myfdw".into(),
                if_exists: false,
                cascade: false,
            }
        );
    }

    fn plain_index_key(column: &str) -> crate::ast::IndexKey {
        crate::ast::IndexKey {
            column: Some(column.into()),
            text: column.into(),
            opclass: None,
            descending: false,
            nulls_first: None,
        }
    }

    #[test]
    fn create_index_keeps_opclass_keys_as_plain_columns() {
        use assert2::assert;

        for (sql, expected) in [
            ("CREATE INDEX i ON t (a int4_ops)", "int4_ops"),
            (
                "CREATE INDEX i ON t (a pg_catalog.int4_ops)",
                "pg_catalog.int4_ops",
            ),
            (
                "CREATE INDEX i ON t (a COLLATE c int4_ops DESC)",
                "int4_ops",
            ),
        ] {
            let Statement::CreateIndex { keys, .. } = one(sql) else {
                panic!("expected CREATE INDEX: {sql}");
            };
            assert!(keys[0].column.as_deref() == Some("a"), "{sql}");
            assert!(keys[0].opclass.as_deref() == Some(expected), "{sql}");
        }

        let Statement::CreateIndex { keys, .. } = one("CREATE INDEX i ON t ((lower(a)) text_ops)")
        else {
            panic!("expected CREATE INDEX");
        };
        assert!(keys[0].column.is_none());
    }

    fn alter_table_stmt(table: &str, actions: Vec<AlterTableAction>) -> Statement {
        Statement::AlterTable {
            table: table.into(),
            if_exists: false,
            only: false,
            actions,
        }
    }

    fn primary_key_action(name: Option<&str>, columns: &[&str]) -> AlterTableAction {
        AlterTableAction::AddConstraint(TableConstraint {
            name: name.map(Into::into),
            kind: TableConstraintKind::PrimaryKey {
                columns: columns.iter().map(|column| (*column).to_string()).collect(),
                without_overlaps: false,
            },
            attributes: crate::ast::ConstraintAttributes::default(),
        })
    }

    /// `ONLY` is what stops a column-shape subcommand from reaching the
    /// relation's partitions and inheritance children, so it has to survive
    /// parsing rather than being eaten as noise. `t *` is the explicit spelling
    /// of the default and must not set it.
    #[test]
    fn alter_table_carries_the_only_flag_that_suppresses_recursion() {
        use assert2::assert;
        for (sql, expected) in [
            ("ALTER TABLE ONLY t DROP COLUMN c", true),
            ("alter table only t drop column c", true),
            ("ALTER TABLE IF EXISTS ONLY t DROP COLUMN c", true),
            ("ALTER TABLE ONLY s.t DROP COLUMN c", true),
            ("ALTER TABLE t DROP COLUMN c", false),
            ("ALTER TABLE t * DROP COLUMN c", false),
        ] {
            let Statement::AlterTable { only, .. } = one(sql) else {
                panic!("expected ALTER TABLE for {sql}");
            };
            assert!(only == expected, "{sql}");
        }
    }

    #[test]
    fn alter_table_add_primary_key_parses_bare_multi_column_and_named_forms() {
        use assert2::assert;
        for (sql, expected) in [
            (
                "ALTER TABLE pgbench_branches ADD PRIMARY KEY (bid)",
                alter_table_stmt("pgbench_branches", vec![primary_key_action(None, &["bid"])]),
            ),
            (
                "alter table pgbench_accounts add primary key (aid)",
                alter_table_stmt("pgbench_accounts", vec![primary_key_action(None, &["aid"])]),
            ),
            (
                "ALTER TABLE t ADD PRIMARY KEY (a, b, c)",
                alter_table_stmt("t", vec![primary_key_action(None, &["a", "b", "c"])]),
            ),
            (
                "ALTER TABLE t ADD CONSTRAINT custom_pk PRIMARY KEY (a, b)",
                alter_table_stmt(
                    "t",
                    vec![primary_key_action(Some("custom_pk"), &["a", "b"])],
                ),
            ),
        ] {
            assert!(one(sql) == expected, "{sql}");
        }
    }

    #[test]
    fn alter_table_add_primary_key_rejects_malformed_tails() {
        use assert2::assert;
        for sql in [
            "ALTER TABLE t ADD PRIMARY KEY",
            "ALTER TABLE t ADD PRIMARY KEY ()",
            "ALTER TABLE t ADD PRIMARY KEY (id,)",
            "ALTER TABLE t ADD PRIMARY KEY id",
            "ALTER TABLE t ADD CONSTRAINT PRIMARY KEY (id)",
        ] {
            assert!(crate::parse(sql).is_err(), "{sql}");
        }
    }

    /// Every `ALTER TABLE` subcommand family lands on its own typed action, and
    /// the comma form keeps them in written order in one statement.
    #[test]
    fn alter_table_parses_every_subcommand_family() {
        use assert2::assert;
        let cases: &[(&str, Statement)] = &[
            (
                "ALTER TABLE t DROP COLUMN c CASCADE",
                alter_table_stmt(
                    "t",
                    vec![AlterTableAction::DropColumn {
                        column: "c".into(),
                        if_exists: false,
                        cascade: true,
                    }],
                ),
            ),
            (
                "ALTER TABLE IF EXISTS t DROP IF EXISTS c",
                Statement::AlterTable {
                    table: "t".into(),
                    if_exists: true,
                    only: false,
                    actions: vec![AlterTableAction::DropColumn {
                        column: "c".into(),
                        if_exists: true,
                        cascade: false,
                    }],
                },
            ),
            (
                "ALTER TABLE t ALTER COLUMN c SET NOT NULL",
                alter_table_stmt("t", vec![AlterTableAction::SetNotNull("c".into())]),
            ),
            (
                "ALTER TABLE t ALTER c DROP NOT NULL",
                alter_table_stmt("t", vec![AlterTableAction::DropNotNull("c".into())]),
            ),
            (
                "ALTER TABLE t ALTER COLUMN c DROP DEFAULT",
                alter_table_stmt("t", vec![AlterTableAction::DropDefault("c".into())]),
            ),
            (
                "ALTER TABLE t ALTER COLUMN c TYPE bigint",
                alter_table_stmt(
                    "t",
                    vec![AlterTableAction::SetType {
                        column: "c".into(),
                        ty: ColumnType::Int8,
                        using: None,
                    }],
                ),
            ),
            (
                "ALTER TABLE t RENAME COLUMN a TO b",
                alter_table_stmt(
                    "t",
                    vec![AlterTableAction::RenameColumn {
                        column: "a".into(),
                        new_name: "b".into(),
                    }],
                ),
            ),
            (
                "ALTER TABLE t RENAME CONSTRAINT a TO b",
                alter_table_stmt(
                    "t",
                    vec![AlterTableAction::RenameConstraint {
                        name: "a".into(),
                        new_name: "b".into(),
                    }],
                ),
            ),
            (
                "ALTER TABLE t RENAME TO u",
                alter_table_stmt(
                    "t",
                    vec![AlterTableAction::RenameTable {
                        new_name: "u".into(),
                    }],
                ),
            ),
            (
                "ALTER TABLE t VALIDATE CONSTRAINT ck",
                alter_table_stmt("t", vec![AlterTableAction::ValidateConstraint("ck".into())]),
            ),
            (
                "ALTER TABLE t OWNER TO bob",
                alter_table_stmt("t", vec![AlterTableAction::OwnerTo("bob".into())]),
            ),
            (
                "ALTER TABLE t SET (fillfactor = 70, autovacuum_enabled)",
                alter_table_stmt(
                    "t",
                    vec![AlterTableAction::SetStorageParameters(vec![
                        ("fillfactor".into(), Some("70".into())),
                        ("autovacuum_enabled".into(), None),
                    ])],
                ),
            ),
            (
                "ALTER TABLE t RESET (fillfactor)",
                alter_table_stmt(
                    "t",
                    vec![AlterTableAction::ResetStorageParameters(vec![
                        "fillfactor".into(),
                    ])],
                ),
            ),
            (
                "ALTER TABLE t DROP CONSTRAINT IF EXISTS ck RESTRICT",
                alter_table_stmt(
                    "t",
                    vec![AlterTableAction::DropConstraint {
                        name: "ck".into(),
                        if_exists: true,
                        cascade: false,
                    }],
                ),
            ),
        ];
        for (sql, expected) in cases {
            assert!(one(sql) == *expected, "{sql}");
        }
    }

    #[test]
    fn alter_table_comma_form_keeps_actions_in_written_order() {
        use assert2::assert;
        let Statement::AlterTable { actions, .. } =
            one("ALTER TABLE t ADD COLUMN a int4, DROP COLUMN b, ALTER COLUMN c SET NOT NULL")
        else {
            panic!("expected ALTER TABLE");
        };
        assert!(matches!(actions[0], AlterTableAction::AddColumn { .. }));
        assert!(matches!(actions[1], AlterTableAction::DropColumn { .. }));
        assert!(matches!(actions[2], AlterTableAction::SetNotNull(_)));
        assert!(actions.len() == 3);
    }

    /// `ALTER TABLE` subcommands with no counterpart in Crabka's storage model
    /// still parse and carry their source text, so the executor can name them.
    #[test]
    fn alter_table_records_unsupported_subcommands_with_their_text() {
        use assert2::assert;
        let Statement::AlterTable { actions, .. } = one("ALTER TABLE t SET SCHEMA other") else {
            panic!("expected ALTER TABLE");
        };
        assert!(actions == vec![AlterTableAction::Unsupported("SET SCHEMA other".into())]);
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

    /// The parsed statement, not its rendering: `COPY` now lands as a typed
    /// [`crate::ast::Statement::Copy`], and the wider grammar is exercised in
    /// `tests/copy.rs`.
    #[test]
    fn copy_from_stdin_parses_to_a_typed_statement() {
        use crate::ast::{
            CopyDirection, CopyOptions, CopySource, CopyStmt, CopyTarget, RelationRef, Statement,
        };

        let stmts = crate::parse("COPY s1.accounts (id, name) FROM STDIN").expect("COPY parses");
        assert2::assert!(
            stmts
                == vec![Statement::Copy(Box::new(CopyStmt {
                    target: CopyTarget::Table {
                        name: RelationRef::qualified("s1", "accounts"),
                        columns: Some(vec!["id".into(), "name".into()]),
                    },
                    direction: CopyDirection::From(CopySource::Stdin),
                    options: CopyOptions::default(),
                }))]
        );
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
        (
            "ALTER TABLE t ADD PRIMARY KEY (id)",
            CommandIdentity::AlterTable,
        ),
        ("CREATE TABLE t AS SELECT 1", CommandIdentity::CreateTableAs),
        ("SELECT 1 AS a INTO t", CommandIdentity::SelectInto),
        ("TABLE t", CommandIdentity::Table),
    ] {
        let parsed = parse_with_command_identities(sql).expect(sql);
        assert_eq!(parsed[0].1, expected, "{sql}");
    }

    // Families the parser itself still refuses: their syntax is not recognized,
    // so they must not be swallowed by the generic parent's identity.
    //
    // `CREATE TABLE … INHERITS`, `ALTER TABLE … ATTACH/DETACH PARTITION` and
    // `ALTER TABLE … ENABLE ROW LEVEL SECURITY` have deliberately moved off this
    // list: the matrix's bounded-refusal rule prefers recognizing the stock
    // PostgreSQL syntax and refusing it at the session boundary with a typed
    // SQLSTATE, so they now parse and are rejected by the executor. That
    // refusal is pinned by the session behavior probes the compatibility-matrix
    // checker runs, not here.
    //
    // `CREATE TABLE … PARTITION BY` and `… PARTITION OF` have moved off it for
    // the stronger reason that they are implemented: a partitioned parent takes
    // hash and range partitions and routes inserted rows to the right leaf. They
    // are covered by the partitioning tests, not by a parse-rejection assertion.
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
        emitted(identity, Ok(Statement::Commit { chain: false })).expect("fake branch emits")
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

    assert_eq!(NON_GOAL_REFUSALS.len(), 29);
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

#[test]
fn drop_role_and_user_retain_if_exists() {
    use crate::ast::Statement;

    for (sql, name, if_exists) in [
        ("DROP ROLE r", "r", false),
        ("DROP ROLE IF EXISTS r", "r", true),
        ("DROP USER u", "u", false),
        ("DROP USER IF EXISTS u", "u", true),
    ] {
        assert_eq!(
            parse(sql),
            Ok(vec![Statement::DropRole {
                name: name.into(),
                if_exists,
            }]),
            "{sql}",
        );
    }
}

#[test]
fn security_label_parses_upstream_provider_first_failure_shapes() {
    use crate::{
        ast::{Statement, UtilityStatement},
        command::CommandIdentity,
    };

    for (sql, provider) in [
        (
            "SECURITY LABEL ON TABLE seclabel_tbl1 IS 'classified'",
            None,
        ),
        (
            "SECURITY LABEL FOR 'dummy' ON TABLE seclabel_tbl1 IS 'classified'",
            Some("dummy"),
        ),
        (
            "SECURITY LABEL ON TABLE seclabel_tbl1 IS '...invalid label...'",
            None,
        ),
        (
            "SECURITY LABEL ON TABLE seclabel_tbl3 IS 'unclassified'",
            None,
        ),
        (
            "SECURITY LABEL ON ROLE regress_seclabel_user1 IS 'classified'",
            None,
        ),
        (
            "SECURITY LABEL FOR 'dummy' ON ROLE regress_seclabel_user1 IS 'classified'",
            Some("dummy"),
        ),
        (
            "SECURITY LABEL ON ROLE regress_seclabel_user1 IS '...invalid label...'",
            None,
        ),
        (
            "SECURITY LABEL ON ROLE regress_seclabel_user3 IS 'unclassified'",
            None,
        ),
        ("SECURITY LABEL ON TABLE public.t IS NULL", None),
    ] {
        let expected = Statement::Utility(UtilityStatement::SecurityLabel {
            provider: provider.map(str::to_owned),
        });
        assert_eq!(parse(sql), Ok(vec![expected.clone()]), "{sql}");
        assert_eq!(
            parse_with_command_identities(sql),
            Ok(vec![(expected, CommandIdentity::SecurityLabel)]),
            "{sql}",
        );
    }
}

#[test]
fn load_and_c_routine_external_symbols_keep_typed_metadata() {
    use crate::{
        ast::{RoutineBody, RoutineOption, Statement, UtilityStatement},
        command::CommandIdentity,
    };

    let filename = "/tmp/regress.so";
    let load = Statement::Utility(UtilityStatement::Load {
        filename: filename.into(),
    });
    assert_eq!(parse("LOAD '/tmp/regress.so'"), Ok(vec![load.clone()]));
    assert_eq!(
        parse_with_command_identities("LOAD '/tmp/regress.so'"),
        Ok(vec![(load, CommandIdentity::Load)]),
    );

    for (sql, expected) in [
        (
            "CREATE FUNCTION test1(int) RETURNS int LANGUAGE C AS 'nosuchfile'",
            RoutineBody::Source("nosuchfile".into()),
        ),
        (
            "CREATE FUNCTION test1(int) RETURNS int LANGUAGE C AS '/tmp/regress.so', 'nosuchsymbol'",
            RoutineBody::External {
                object_file: filename.into(),
                link_symbol: "nosuchsymbol".into(),
            },
        ),
    ] {
        let statements = parse(sql).expect(sql);
        let [Statement::CreateRoutine(routine)] = statements.as_slice() else {
            panic!("{sql} did not parse as CREATE FUNCTION");
        };
        assert_eq!(
            routine.options.iter().find_map(|option| match option {
                RoutineOption::Body(body) => Some(body),
                _ => None,
            }),
            Some(&expected),
            "{sql}",
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

/// Parse-shape tests for the jsonb/array expression grammar, `ON CONFLICT`, and
/// `LISTEN`/`NOTIFY`/`UNLISTEN`.
#[cfg(test)]
mod json_array_conflict_notify_tests {
    use assert2::assert;

    use crate::{
        ast::{
            ArraySubscript, BinaryOp, ColumnDef, Expr, OnConflict, OnConflictAction,
            OnConflictTarget, SelectItem, SelectStmt, Statement, TableExpr, UnaryOp,
            UnlistenTarget,
        },
        command::CommandIdentity,
        parse, parse_with_command_identities,
    };

    fn one(sql: &str) -> Statement {
        let mut parsed = parse(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert!(parsed.len() == 1, "{sql}");
        parsed.pop().expect("one statement")
    }

    fn select(sql: &str) -> SelectStmt {
        use crate::ast::{QueryBody, SetExpr};
        let Statement::Query(query) = one(sql) else {
            panic!("expected a query: {sql}");
        };
        let SetExpr::Query(QueryBody::Select(select)) = query.body else {
            panic!("expected a SELECT body: {sql}");
        };
        *select
    }

    /// The single projected expression of `SELECT <expr> …`.
    fn projected(sql: &str) -> Expr {
        let mut select = select(sql);
        assert!(select.projection.len() == 1, "{sql}");
        match select.projection.pop().expect("one projection") {
            SelectItem::Expr { expr, .. } => expr,
            other => panic!("expected an expression projection, got {other:?}"),
        }
    }

    fn column(name: &str) -> Expr {
        Expr::Column {
            table: None,
            name: name.into(),
        }
    }

    fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
        Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn error(sql: &str) -> crate::ParseError {
        parse(sql).expect_err(sql)
    }

    #[test]
    fn regex_bitwise_and_arithmetic_operators_parse_to_their_binary_ops() {
        let cases: &[(&str, BinaryOp)] = &[
            ("a ~ b", BinaryOp::Match),
            ("a ~* b", BinaryOp::MatchCi),
            ("a !~ b", BinaryOp::NotMatch),
            ("a !~* b", BinaryOp::NotMatchCi),
            ("a & b", BinaryOp::BitAnd),
            ("a | b", BinaryOp::BitOr),
            ("a # b", BinaryOp::BitXor),
            ("a << b", BinaryOp::Shl),
            ("a >> b", BinaryOp::Shr),
            // The inet/cidr containment operators, whose shorter prefixes are
            // the shift operators just above.
            ("a <<= b", BinaryOp::ContainedByOrEq),
            ("a >>= b", BinaryOp::ContainsOrEq),
            ("a ^ b", BinaryOp::Pow),
            ("a % b", BinaryOp::Mod),
            // `!=` is PostgreSQL's alternative spelling of `<>`.
            ("a != b", BinaryOp::Ne),
        ];
        for (expression, op) in cases {
            assert!(
                projected(&format!("SELECT {expression}")) == binary(*op, column("a"), column("b")),
                "{expression}"
            );
        }
    }

    #[test]
    fn explicit_operator_wrapper_reuses_the_binary_operator() {
        let expected = projected("SELECT relname ~ '^x' COLLATE pg_catalog.default");
        for sql in [
            "SELECT relname OPERATOR(pg_catalog.~) '^x' COLLATE pg_catalog.default",
            "SELECT relname OPERATOR(~) '^x' COLLATE pg_catalog.default",
        ] {
            assert_eq!(projected(sql), expected, "{sql}");
        }
        assert_eq!(
            projected("SELECT 1 + 2 OPERATOR(pg_catalog.*) 3"),
            projected("SELECT (1 + 2) * 3")
        );
        assert_eq!(
            projected("SELECT 1 OPERATOR(pg_catalog.+) 2 * 3"),
            projected("SELECT 1 + (2 * 3)")
        );
        assert_eq!(
            projected("SELECT 1 OPERATOR(+) 2 OPERATOR(*) 3"),
            projected("SELECT (1 + 2) * 3")
        );
    }

    #[test]
    fn explicit_operator_wrapper_supports_prefix_and_quantified_forms() {
        let prefix_cases: &[(&str, UnaryOp)] = &[
            ("OPERATOR(pg_catalog.-) a", UnaryOp::Neg),
            ("OPERATOR(+) a", UnaryOp::Plus),
            ("OPERATOR(pg_catalog.~) a", UnaryOp::BitNot),
            ("OPERATOR(pg_catalog.@) a", UnaryOp::Abs),
            ("OPERATOR(pg_catalog.|/) a", UnaryOp::Sqrt),
            ("OPERATOR(pg_catalog.||/) a", UnaryOp::Cbrt),
            ("OPERATOR(pg_catalog.!!) a", UnaryOp::TsNot),
        ];
        for (expression, op) in prefix_cases {
            assert_eq!(
                projected(&format!("SELECT {expression}")),
                Expr::Unary {
                    op: *op,
                    expr: Box::new(column("a")),
                },
                "{expression}"
            );
        }
        assert_eq!(
            projected("SELECT OPERATOR(pg_catalog.-) 2 ^ 2"),
            projected("SELECT -(2 ^ 2)")
        );

        assert_eq!(
            projected("SELECT value OPERATOR(pg_catalog.~) ANY(ARRAY['^x', '^y'])"),
            Expr::QuantifiedArray {
                expr: Box::new(column("value")),
                op: BinaryOp::Match,
                all: false,
                array: Box::new(Expr::ArrayLiteral(vec![
                    Expr::StringLiteral("^x".into()),
                    Expr::StringLiteral("^y".into()),
                ])),
            }
        );
        assert!(matches!(
            projected("SELECT value OPERATOR(pg_catalog.~) ALL(SELECT pattern FROM patterns)"),
            Expr::Quantified {
                op: BinaryOp::Match,
                all: true,
                ..
            }
        ));
        assert!(matches!(
            projected("SELECT value OPERATOR(pg_catalog.+) SOME(values_array)"),
            Expr::QuantifiedArray {
                op: BinaryOp::Add,
                all: false,
                ..
            }
        ));
    }

    #[test]
    fn explicit_operator_wrapper_rejects_unsupported_names_without_stealing_aliases() {
        let bare_alias = select("SELECT relname operator FROM relations");
        assert!(matches!(
            &bare_alias.projection[0],
            SelectItem::Expr {
                alias: Some(alias),
                ..
            } if alias == "operator"
        ));
        let quoted_alias = select("SELECT relname \"operator\" FROM relations");
        assert!(matches!(
            &quoted_alias.projection[0],
            SelectItem::Expr {
                alias: Some(alias),
                ..
            } if alias == "operator"
        ));

        let schema_error = error("SELECT 1 OPERATOR(public.+) 2");
        assert_eq!(schema_error.sqlstate(), "0A000");
        assert!(
            schema_error
                .message
                .contains("operator schema \"public\" is not supported")
        );
        assert!(
            error("SELECT 1 OPERATOR(pg_catalog.public.+) 2")
                .message
                .contains("multi-part operator qualification")
        );

        for sql in [
            "SELECT 1 OPERATOR() 2",
            "SELECT OPERATOR() 1",
            "SELECT 1 OPERATOR(pg_catalog.) 2",
            "SELECT 1 OPERATOR(pg_catalog.+ 2",
        ] {
            assert!(crate::parse(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn generic_prefix_operators_parse_to_their_unary_ops() {
        let cases: &[(&str, UnaryOp)] = &[
            ("~ a", UnaryOp::BitNot),
            ("@ a", UnaryOp::Abs),
            ("|/ a", UnaryOp::Sqrt),
            ("||/ a", UnaryOp::Cbrt),
        ];
        for (expression, op) in cases {
            assert!(
                projected(&format!("SELECT {expression}"))
                    == Expr::Unary {
                        op: *op,
                        expr: Box::new(column("a")),
                    },
                "{expression}"
            );
        }
    }

    #[test]
    fn new_operators_bind_at_postgres_precedence() {
        // Every case here is a grouping PostgreSQL's precedence table fixes and
        // that a naive binding-power choice gets wrong. The expected value is the
        // whole tree, so a mis-grouping cannot hide behind a matching root node.
        let int = |n: &str| Expr::IntLiteral(n.into());
        let unary = |op, e| Expr::Unary {
            op,
            expr: Box::new(e),
        };
        let cases: &[(&str, Expr)] = &[
            // The bitwise/regex family is ONE left-associative level.
            (
                "1 # 2 & 3",
                binary(
                    BinaryOp::BitAnd,
                    binary(BinaryOp::BitXor, int("1"), int("2")),
                    int("3"),
                ),
            ),
            (
                "1 | 2 # 3",
                binary(
                    BinaryOp::BitXor,
                    binary(BinaryOp::BitOr, int("1"), int("2")),
                    int("3"),
                ),
            ),
            // ...and it binds LOOSER than `+ -`, so the shift takes `2 + 1`.
            (
                "1 << 2 + 1",
                binary(
                    BinaryOp::Shl,
                    int("1"),
                    binary(BinaryOp::Add, int("2"), int("1")),
                ),
            ),
            // ...and TIGHTER than the comparisons.
            (
                "1 & 2 = 3",
                binary(
                    BinaryOp::Eq,
                    binary(BinaryOp::BitAnd, int("1"), int("2")),
                    int("3"),
                ),
            ),
            // `^` is LEFT-associative in PostgreSQL: 2^3^2 is 64, not 512.
            (
                "2^3^2",
                binary(
                    BinaryOp::Pow,
                    binary(BinaryOp::Pow, int("2"), int("3")),
                    int("2"),
                ),
            ),
            // `^` binds tighter than `*`...
            (
                "2 ^ 2 * 3",
                binary(
                    BinaryOp::Mul,
                    binary(BinaryOp::Pow, int("2"), int("2")),
                    int("3"),
                ),
            ),
            // ...and LOOSER than unary minus, which is why `-2^2` is 4.
            (
                "-2^2",
                binary(BinaryOp::Pow, unary(UnaryOp::Neg, int("2")), int("2")),
            ),
            // `%` sits with `*` and `/`.
            (
                "4 % 3 + 1",
                binary(
                    BinaryOp::Add,
                    binary(BinaryOp::Mod, int("4"), int("3")),
                    int("1"),
                ),
            ),
            // A generic PREFIX operator binds loosely — unlike unary minus, its
            // operand swallows the following `+`.
            (
                "~ 5 + 1",
                unary(UnaryOp::BitNot, binary(BinaryOp::Add, int("5"), int("1"))),
            ),
            (
                "@ 5 - 8",
                unary(UnaryOp::Abs, binary(BinaryOp::Sub, int("5"), int("8"))),
            ),
            // ...but it stops at its own level, so `&` takes the completed `~5`.
            (
                "~ 5 & 3",
                binary(BinaryOp::BitAnd, unary(UnaryOp::BitNot, int("5")), int("3")),
            ),
            (
                "-5 * 2",
                binary(BinaryOp::Mul, unary(UnaryOp::Neg, int("5")), int("2")),
            ),
        ];
        for (expression, want) in cases {
            assert!(
                projected(&format!("SELECT {expression}")) == *want,
                "{expression}"
            );
        }
    }

    #[test]
    fn every_jsonb_and_array_operator_parses_to_its_binary_op() {
        let cases: &[(&str, BinaryOp)] = &[
            ("a -> b", BinaryOp::JsonGet),
            ("a ->> b", BinaryOp::JsonGetText),
            ("a #> b", BinaryOp::JsonGetPath),
            ("a #>> b", BinaryOp::JsonGetPathText),
            ("a @> b", BinaryOp::Contains),
            ("a <@ b", BinaryOp::ContainedBy),
            ("a ? b", BinaryOp::KeyExists),
            ("a ?| b", BinaryOp::KeyExistsAny),
            ("a ?& b", BinaryOp::KeyExistsAll),
            ("a && b", BinaryOp::Overlaps),
            // `||` and `-` are shared with text concatenation and subtraction:
            // the operand types, not the parse, pick the jsonb/array meaning.
            ("a || b", BinaryOp::Concat),
            ("a - b", BinaryOp::Sub),
        ];
        for (expression, op) in cases {
            assert!(
                projected(&format!("SELECT {expression}")) == binary(*op, column("a"), column("b")),
                "{expression}"
            );
        }
    }

    #[test]
    fn jsonb_operators_bind_tighter_than_comparison_and_looser_than_arithmetic() {
        // `(a->>'k') = 'v'` — the whole point of the (7, 8) slot: a driver's
        // `WHERE doc->>'id' = $1` must not parse as `doc ->> ('id' = $1)`.
        assert!(
            projected("SELECT a ->> 'k' = 'v'")
                == binary(
                    BinaryOp::Eq,
                    binary(
                        BinaryOp::JsonGetText,
                        column("a"),
                        Expr::StringLiteral("k".into())
                    ),
                    Expr::StringLiteral("v".into()),
                )
        );
        // Looser than `+`: `a -> (b + c)`.
        assert!(
            projected("SELECT a -> b + c")
                == binary(
                    BinaryOp::JsonGet,
                    column("a"),
                    binary(BinaryOp::Add, column("b"), column("c")),
                )
        );
        // Looser than the boolean operators too, in the other direction.
        assert!(
            projected("SELECT a @> b AND c")
                == binary(
                    BinaryOp::And,
                    binary(BinaryOp::Contains, column("a"), column("b")),
                    column("c"),
                )
        );
    }

    #[test]
    fn operators_at_the_concat_level_are_left_associative() {
        assert!(
            projected("SELECT x || y || z")
                == binary(
                    BinaryOp::Concat,
                    binary(BinaryOp::Concat, column("x"), column("y")),
                    column("z"),
                )
        );
        // A mixed chain at the same level also folds left, so `a->'x'->'y'`
        // walks two levels down rather than nesting to the right.
        assert!(
            projected("SELECT a -> 'x' ->> 'y'")
                == binary(
                    BinaryOp::JsonGetText,
                    binary(
                        BinaryOp::JsonGet,
                        column("a"),
                        Expr::StringLiteral("x".into())
                    ),
                    Expr::StringLiteral("y".into()),
                )
        );
    }

    #[test]
    fn array_literals_parse_including_the_empty_and_nested_forms() {
        assert!(
            projected("SELECT ARRAY[1, 2]")
                == Expr::ArrayLiteral(vec![
                    Expr::IntLiteral("1".into()),
                    Expr::IntLiteral("2".into())
                ])
        );
        assert!(projected("SELECT ARRAY[]") == Expr::ArrayLiteral(vec![]));
        assert!(
            projected("SELECT ARRAY[a || b]")
                == Expr::ArrayLiteral(vec![binary(BinaryOp::Concat, column("a"), column("b"))])
        );
        // A braceless nested constructor is an element that is itself a
        // constructor, so it parses identically to the spelled-out form.
        let rows = Expr::ArrayLiteral(vec![
            Expr::ArrayLiteral(vec![
                Expr::IntLiteral("1".into()),
                Expr::IntLiteral("2".into()),
            ]),
            Expr::ArrayLiteral(vec![
                Expr::IntLiteral("3".into()),
                Expr::IntLiteral("4".into()),
            ]),
        ]);
        assert!(projected("SELECT ARRAY[[1,2],[3,4]]") == rows);
        assert!(projected("SELECT ARRAY[ARRAY[1,2],ARRAY[3,4]]") == rows);
    }

    /// `PostgreSQL` has no nested array TYPE: every `[]` suffix spelling, and the
    /// SQL-standard `ARRAY` spelling, resolve to the same one-array type.
    #[test]
    fn every_array_type_suffix_spelling_resolves_to_one_array_type() {
        use crabka_pgtypes::ColumnType;

        let int_array = ColumnType::array_of(ColumnType::Int4).expect("int4[]");
        for sql in [
            "SELECT $1::int4[]",
            "SELECT $1::int4[][]",
            "SELECT $1::int4[][][]",
            "SELECT $1::int4[4]",
            "SELECT $1::int4 ARRAY",
            "SELECT $1::int4 ARRAY[4]",
        ] {
            let Expr::Cast { ty, .. } = projected(sql) else {
                panic!("{sql} parses to a cast");
            };
            assert!(ty == int_array, "{sql}");
        }
        // The length-modified string types now have array types too.
        let Expr::Cast { ty, .. } = projected("SELECT $1::varchar(5)[]") else {
            panic!("varchar(5)[] parses to a cast");
        };
        assert!(ty == ColumnType::array_of(ColumnType::Varchar(Some(5))).expect("varchar(5)[]"));
    }

    #[test]
    fn array_subquery_constructor_parses_to_its_own_node() {
        let Expr::ArraySubquery(query) = projected("SELECT ARRAY(SELECT 1)") else {
            panic!("ARRAY(subquery) parses to ArraySubquery");
        };
        assert!(matches!(
            query.body,
            crate::ast::SetExpr::Query(crate::ast::QueryBody::Select(_))
        ));
        // The subquery may carry its own tail, exactly as a scalar one may.
        assert!(matches!(
            projected("SELECT ARRAY(SELECT x FROM t ORDER BY x DESC LIMIT 3)"),
            Expr::ArraySubquery(_)
        ));
    }

    #[test]
    fn subscripts_bind_tightest_and_chain() {
        assert!(
            projected("SELECT a[1]")
                == Expr::Subscript {
                    base: Box::new(column("a")),
                    index: Box::new(Expr::IntLiteral("1".into())),
                }
        );
        // Tighter than `+`: `(a[1]) + 2`, never `a[1 + 2]`.
        assert!(
            projected("SELECT a[1] + 2")
                == binary(
                    BinaryOp::Add,
                    Expr::Subscript {
                        base: Box::new(column("a")),
                        index: Box::new(Expr::IntLiteral("1".into())),
                    },
                    Expr::IntLiteral("2".into()),
                )
        );
        // A `[…][…]` chain is ONE array reference, not a subscript of a
        // subscript: PostgreSQL reaches into a two-dimensional array, so
        // `(ARRAY[[1,2],[3,4]])[2][1]` is 3 rather than an error. The index of
        // each level is a full expression.
        assert!(
            projected("SELECT a[i + 1][2]")
                == Expr::ArrayRef {
                    base: Box::new(column("a")),
                    subscripts: vec![
                        ArraySubscript::Index(binary(
                            BinaryOp::Add,
                            column("i"),
                            Expr::IntLiteral("1".into())
                        )),
                        ArraySubscript::Index(Expr::IntLiteral("2".into())),
                    ],
                }
        );
    }

    #[test]
    fn every_slice_spelling_parses_into_one_array_reference() {
        let int = |n: &str| Expr::IntLiteral(n.to_string());
        let cases: &[(&str, Vec<ArraySubscript>)] = &[
            (
                "SELECT a[1:2]",
                vec![ArraySubscript::Slice {
                    lower: Some(int("1")),
                    upper: Some(int("2")),
                }],
            ),
            (
                "SELECT a[:2]",
                vec![ArraySubscript::Slice {
                    lower: None,
                    upper: Some(int("2")),
                }],
            ),
            (
                "SELECT a[1:]",
                vec![ArraySubscript::Slice {
                    lower: Some(int("1")),
                    upper: None,
                }],
            ),
            (
                "SELECT a[:]",
                vec![ArraySubscript::Slice {
                    lower: None,
                    upper: None,
                }],
            ),
            (
                "SELECT a[1][2]",
                vec![
                    ArraySubscript::Index(int("1")),
                    ArraySubscript::Index(int("2")),
                ],
            ),
            (
                "SELECT a[1:2][3]",
                vec![
                    ArraySubscript::Slice {
                        lower: Some(int("1")),
                        upper: Some(int("2")),
                    },
                    ArraySubscript::Index(int("3")),
                ],
            ),
        ];
        for (sql, subscripts) in cases {
            assert!(
                projected(sql)
                    == Expr::ArrayRef {
                        base: Box::new(column("a")),
                        subscripts: subscripts.clone(),
                    },
                "{sql}"
            );
        }
        // More than PostgreSQL's six dimensions is 54000, not a parse failure.
        let error = error("SELECT a[1][2][3][4][5][6][7]");
        assert!(error.sqlstate() == "54000");
    }

    #[test]
    fn quantified_comparisons_split_between_the_array_and_subquery_forms() {
        // The array form — what sqlx/Diesel/asyncpg emit for every IN-list bind.
        let cases: &[(&str, BinaryOp, bool, Expr)] = &[
            ("a = ANY($1)", BinaryOp::Eq, false, Expr::Param(1)),
            (
                "a = ANY(ARRAY[1, 2])",
                BinaryOp::Eq,
                false,
                Expr::ArrayLiteral(vec![
                    Expr::IntLiteral("1".into()),
                    Expr::IntLiteral("2".into()),
                ]),
            ),
            ("a <> ALL(tags)", BinaryOp::Ne, true, column("tags")),
            ("a = SOME(tags)", BinaryOp::Eq, false, column("tags")),
            (
                "a > ANY($1::int8[])",
                BinaryOp::Gt,
                false,
                Expr::Cast {
                    expr: Box::new(Expr::Param(1)),
                    ty: crabka_pgtypes::ColumnType::array_of(crabka_pgtypes::ColumnType::Int8)
                        .expect("int8[] is supported"),
                },
            ),
        ];
        for (expression, op, all, array) in cases {
            assert!(
                projected(&format!("SELECT {expression}"))
                    == Expr::QuantifiedArray {
                        expr: Box::new(column("a")),
                        op: *op,
                        all: *all,
                        array: Box::new(array.clone()),
                    },
                "{expression}"
            );
        }
        // The subquery form is untouched.
        for sql in [
            "SELECT a = ANY(SELECT id FROM t)",
            "SELECT a = ANY(VALUES (1))",
            "SELECT a = ALL(WITH c AS (SELECT 1) SELECT * FROM c)",
        ] {
            assert!(matches!(projected(sql), Expr::Quantified { .. }), "{sql}");
        }
    }

    #[test]
    fn array_type_suffix_parses_in_ddl_and_casts() {
        use crabka_pgtypes::ColumnType;

        let array_of = |elem| ColumnType::array_of(elem).expect("array type exists");
        let Statement::CreateTable { columns, .. } =
            one("CREATE TABLE t (a int4[], b text[5], c jsonb, d numeric(10, 2)[])")
        else {
            panic!("expected CREATE TABLE");
        };
        assert!(
            columns
                .iter()
                .map(|ColumnDef { name, ty, .. }| (name.as_str(), *ty))
                .collect::<Vec<_>>()
                == vec![
                    ("a", array_of(ColumnType::Int4)),
                    ("b", array_of(ColumnType::Text)),
                    ("c", ColumnType::Jsonb),
                    (
                        "d",
                        array_of(ColumnType::Numeric(Some(crabka_pgtypes::numeric::Typmod {
                            precision: 10,
                            scale: 2,
                        })))
                    ),
                ]
        );
        assert!(
            projected("SELECT $1::text[]")
                == Expr::Cast {
                    expr: Box::new(Expr::Param(1)),
                    ty: array_of(ColumnType::Text),
                }
        );
    }

    #[test]
    fn unsupported_array_type_suffixes_are_refused_by_name() {
        let cases: &[(&str, &str)] = &[(
            "SELECT $1::regclass[]",
            "arrays of type \"regclass\" are not supported",
        )];
        for (sql, message) in cases {
            let error = error(sql);
            assert!(error.sqlstate() == "0A000", "{sql}");
            assert!(error.message.contains(message), "{sql}: {}", error.message);
        }
    }

    #[test]
    fn functions_in_from_position_parse_with_and_without_aliases() {
        let call = |name: &str, args: Vec<Expr>| crate::ast::TableFuncCall {
            name: name.into(),
            args,
            column_defs: None,
        };
        assert!(
            select("SELECT tag FROM unnest(tags) AS u(tag)").from
                == vec![TableExpr::Function {
                    functions: vec![call("unnest", vec![column("tags")])],
                    rows_from: false,
                    with_ordinality: false,
                    lateral: false,
                    alias: Some("u".into()),
                    column_aliases: Some(vec!["tag".into()]),
                }]
        );
        assert!(
            select("SELECT * FROM unnest(ARRAY[1, 2])").from
                == vec![TableExpr::Function {
                    functions: vec![call(
                        "unnest",
                        vec![Expr::ArrayLiteral(vec![
                            Expr::IntLiteral("1".into()),
                            Expr::IntLiteral("2".into()),
                        ])]
                    )],
                    rows_from: false,
                    with_ordinality: false,
                    lateral: false,
                    alias: None,
                    column_aliases: None,
                }]
        );
        // Joining a table against a function keeps both FROM item shapes.
        let from = select("SELECT * FROM t JOIN unnest(t.tags) u ON true").from;
        assert!(matches!(from.as_slice(), [TableExpr::Join { .. }]));
    }

    #[test]
    fn on_conflict_clauses_parse_into_whole_target_and_action_structs() {
        fn on_conflict(sql: &str) -> Option<OnConflict> {
            let Statement::Insert { on_conflict, .. } = one(sql) else {
                panic!("expected INSERT: {sql}");
            };
            on_conflict
        }

        assert!(on_conflict("INSERT INTO t VALUES (1)") == None);
        assert!(
            on_conflict("INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING")
                == Some(OnConflict {
                    target: OnConflictTarget::None,
                    action: OnConflictAction::DoNothing,
                })
        );
        assert!(
            on_conflict("INSERT INTO t VALUES (1) ON CONFLICT (a, b) DO NOTHING")
                == Some(OnConflict {
                    target: OnConflictTarget::Columns {
                        columns: vec!["a".into(), "b".into()],
                        index_predicate: None,
                    },
                    action: OnConflictAction::DoNothing,
                })
        );
        assert!(
            on_conflict("INSERT INTO t VALUES (1) ON CONFLICT (a) WHERE a > 0 DO NOTHING")
                == Some(OnConflict {
                    target: OnConflictTarget::Columns {
                        columns: vec!["a".into()],
                        index_predicate: Some(binary(
                            BinaryOp::Gt,
                            column("a"),
                            Expr::IntLiteral("0".into())
                        )),
                    },
                    action: OnConflictAction::DoNothing,
                })
        );
        assert!(
            on_conflict("INSERT INTO t VALUES (1) ON CONFLICT ON CONSTRAINT t_pkey DO NOTHING")
                == Some(OnConflict {
                    target: OnConflictTarget::OnConstraint("t_pkey".into()),
                    action: OnConflictAction::DoNothing,
                })
        );
        assert!(
            on_conflict(
                "INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET v = excluded.v, n = t.n + 1 WHERE t.n < 10"
            ) == Some(OnConflict {
                target: OnConflictTarget::Columns {
                    columns: vec!["id".into()],
                    index_predicate: None,
                },
                action: OnConflictAction::DoUpdate {
                    assignments: vec![
                        (
                            "v".into(),
                            Expr::Column {
                                table: Some("excluded".into()),
                                name: "v".into(),
                            }
                        ),
                        (
                            "n".into(),
                            binary(
                                BinaryOp::Add,
                                Expr::Column {
                                    table: Some("t".into()),
                                    name: "n".into(),
                                },
                                Expr::IntLiteral("1".into()),
                            )
                        ),
                    ],
                    filter: Some(binary(
                        BinaryOp::Lt,
                        Expr::Column {
                            table: Some("t".into()),
                            name: "n".into(),
                        },
                        Expr::IntLiteral("10".into()),
                    )),
                },
            })
        );
    }

    #[test]
    fn on_conflict_composes_with_returning_and_multi_row_values() {
        let Statement::Insert {
            source,
            on_conflict,
            returning,
            ..
        } = one(
            "INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b') ON CONFLICT (id) DO UPDATE SET v = excluded.v RETURNING id",
        )
        else {
            panic!("expected INSERT");
        };
        let crate::ast::InsertSource::Values(rows) = &source else {
            panic!("expected a VALUES source");
        };
        assert!(rows.len() == 2);
        assert!(on_conflict.is_some());
        assert!(
            returning.map(|r| r.items)
                == Some(vec![SelectItem::Expr {
                    expr: column("id"),
                    alias: None,
                }])
        );
    }

    #[test]
    fn do_update_without_an_inference_specification_is_a_syntax_error() {
        // PostgreSQL raises exactly this during parse analysis (42601).
        let error = error("INSERT INTO t VALUES (1) ON CONFLICT DO UPDATE SET v = 1");
        assert!(error.sqlstate() == "42601");
        assert!(
            error.message
                == "ON CONFLICT DO UPDATE requires inference specification or constraint name"
        );
    }

    #[test]
    fn malformed_on_conflict_tails_are_rejected() {
        for sql in [
            "INSERT INTO t VALUES (1) ON DO NOTHING",
            "INSERT INTO t VALUES (1) ON CONFLICT",
            "INSERT INTO t VALUES (1) ON CONFLICT DO",
            "INSERT INTO t VALUES (1) ON CONFLICT (id) DO SOMETHING",
            "INSERT INTO t VALUES (1) ON CONFLICT () DO NOTHING",
            "INSERT INTO t VALUES (1) ON CONFLICT (a + 1) DO NOTHING",
            "INSERT INTO t VALUES (1) ON CONFLICT ON CONSTRAINT DO NOTHING",
            "INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE",
            "INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET",
        ] {
            assert!(parse(sql).is_err(), "must reject: {sql}");
        }
    }

    #[test]
    fn on_conflict_words_remain_usable_as_identifiers() {
        // `conflict`, `do`, `nothing` and `constraint` are unreserved in
        // PostgreSQL and matched as soft idents here, so they stay legal names.
        let Statement::CreateTable { name, columns, .. } =
            one("CREATE TABLE conflict (do int4, nothing int4, constraint int4)")
        else {
            panic!("expected CREATE TABLE");
        };
        assert!(name == crate::ast::RelationRef::bare("conflict"));
        assert!(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>()
                == vec!["do", "nothing", "constraint"]
        );
        assert!(matches!(
            one("SELECT do FROM conflict"),
            Statement::Query(_)
        ));
    }

    #[test]
    fn excluded_qualified_columns_need_no_dedicated_parser_support() {
        // `excluded.v` is an ordinary qualified column reference; the lexer's
        // ident lowercasing means the executor can match on `"excluded"`.
        assert!(
            projected("SELECT EXCLUDED.v")
                == Expr::Column {
                    table: Some("excluded".into()),
                    name: "v".into(),
                }
        );
    }

    #[test]
    fn listen_notify_and_unlisten_parse_with_their_command_identities() {
        let cases: &[(&str, Statement, CommandIdentity)] = &[
            (
                "LISTEN chan",
                Statement::Listen {
                    channel: "chan".into(),
                },
                CommandIdentity::Listen,
            ),
            (
                // Unquoted channel names fold to lowercase, quoted ones do not.
                "LISTEN Chan",
                Statement::Listen {
                    channel: "chan".into(),
                },
                CommandIdentity::Listen,
            ),
            (
                "LISTEN \"MixedCase\"",
                Statement::Listen {
                    channel: "MixedCase".into(),
                },
                CommandIdentity::Listen,
            ),
            (
                "NOTIFY chan",
                Statement::Notify {
                    channel: "chan".into(),
                    payload: None,
                },
                CommandIdentity::Notify,
            ),
            (
                "NOTIFY chan, 'hello'",
                Statement::Notify {
                    channel: "chan".into(),
                    payload: Some("hello".into()),
                },
                CommandIdentity::Notify,
            ),
            (
                "UNLISTEN chan",
                Statement::Unlisten {
                    target: UnlistenTarget::Channel("chan".into()),
                },
                CommandIdentity::Unlisten,
            ),
            (
                "UNLISTEN *",
                Statement::Unlisten {
                    target: UnlistenTarget::All,
                },
                CommandIdentity::Unlisten,
            ),
        ];
        for (sql, statement, identity) in cases {
            let parsed =
                parse_with_command_identities(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert!(parsed.len() == 1, "{sql}");
            assert!(parsed[0].0 == *statement, "{sql}");
            assert!(parsed[0].1 == *identity, "{sql}");
        }
    }

    #[test]
    fn malformed_listen_family_statements_are_rejected() {
        for sql in [
            "LISTEN",
            "LISTEN a b",
            "LISTEN *",
            "NOTIFY",
            "NOTIFY chan, payload",
            "NOTIFY chan 'hello'",
            "UNLISTEN",
            "UNLISTEN a, b",
        ] {
            assert!(parse(sql).is_err(), "must reject: {sql}");
        }
    }

    #[test]
    fn listen_family_words_remain_usable_as_identifiers() {
        let Statement::CreateTable { name, columns, .. } =
            one("CREATE TABLE listen (notify int4, unlisten int4)")
        else {
            panic!("expected CREATE TABLE");
        };
        assert!(name == crate::ast::RelationRef::bare("listen"));
        assert!(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>()
                == vec!["notify", "unlisten"]
        );
    }
}

#[cfg(test)]
mod q1_statement_completeness_tests {
    use assert2::assert;

    use crate::{
        ast::{
            ArraySubscript, Assignment, AssignmentValue, CteBody, Expr, InsertSource, MergeAction,
            MergeMatchKind, MergeSource, SelectItem, Statement, TableExpr,
        },
        command::CommandIdentity,
        parse, parse_with_command_identities,
    };

    fn one(sql: &str) -> Statement {
        let mut parsed = parse(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert!(parsed.len() == 1, "{sql}");
        parsed.pop().expect("one statement")
    }

    fn identity(sql: &str) -> CommandIdentity {
        let parsed = parse_with_command_identities(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert!(parsed.len() == 1, "{sql}");
        parsed[0].1
    }

    fn sqlstate(sql: &str) -> String {
        parse(sql)
            .err()
            .unwrap_or_else(|| panic!("{sql} parsed unexpectedly"))
            .sqlstate()
            .to_string()
    }

    #[test]
    fn command_identities_cover_the_new_statement_rows() {
        let cases = [
            ("INSERT INTO t SELECT 1", CommandIdentity::Insert),
            (
                "MERGE INTO t USING s ON true WHEN MATCHED THEN DELETE",
                CommandIdentity::Merge,
            ),
            ("CREATE TABLE t AS SELECT 1", CommandIdentity::CreateTableAs),
            ("SELECT 1 AS a INTO t", CommandIdentity::SelectInto),
            ("TABLE t", CommandIdentity::Table),
            (
                "WITH c AS (SELECT 1) SELECT * FROM c",
                CommandIdentity::Select,
            ),
            (
                "WITH c AS (SELECT 1) UPDATE t SET a = 1",
                CommandIdentity::Update,
            ),
            (
                "WITH c AS (DELETE FROM t RETURNING a) INSERT INTO u SELECT a FROM c",
                CommandIdentity::Insert,
            ),
        ];
        for (sql, expected) in cases {
            assert!(identity(sql) == expected, "{sql}");
        }
    }

    #[test]
    fn insert_sources_cover_values_query_and_default_values() {
        let Statement::Insert { source, .. } = one("INSERT INTO t VALUES (1, DEFAULT)") else {
            panic!("expected INSERT");
        };
        assert!(
            source == InsertSource::Values(vec![vec![Expr::IntLiteral("1".into()), Expr::Default]])
        );

        let Statement::Insert { source, .. } = one("INSERT INTO t DEFAULT VALUES") else {
            panic!("expected INSERT");
        };
        assert!(source == InsertSource::DefaultValues);

        for sql in [
            "INSERT INTO t SELECT a FROM s",
            "INSERT INTO t TABLE s",
            "INSERT INTO t (SELECT a FROM s)",
            "INSERT INTO t SELECT 1 UNION ALL SELECT 2",
        ] {
            let Statement::Insert { source, .. } = one(sql) else {
                panic!("expected INSERT: {sql}");
            };
            assert!(matches!(source, InsertSource::Query(_)), "{sql}");
        }

        // A parenthesised list of names is still a column list.
        let Statement::Insert { columns, .. } = one("INSERT INTO t (a, b) VALUES (1, 2)") else {
            panic!("expected INSERT");
        };
        assert!(columns == Some(vec!["a".into(), "b".into()]));
    }

    #[test]
    fn update_parses_alias_from_list_and_every_assignment_form() {
        let Statement::Update {
            table,
            alias,
            assignments,
            from,
            ..
        } = one(
            "UPDATE t AS x SET a = 1, (b, c) = ROW(2, 3), (d, e) = (SELECT 4, 5), (f) = (6) FROM s, u WHERE x.k = s.k",
        )
        else {
            panic!("expected UPDATE");
        };
        assert!(table == crate::ast::RelationRef::bare("t"));
        assert!(alias == Some("x".into()));
        assert!(from.len() == 2);
        assert!(matches!(from[0], TableExpr::Table { .. }));
        assert!(
            assignments[0]
                == Assignment {
                    targets: vec!["a".into()],
                    subscripts: Vec::new(),
                    value: AssignmentValue::Expr(Expr::IntLiteral("1".into())),
                }
        );
        assert!(
            assignments[1]
                == Assignment {
                    targets: vec!["b".into(), "c".into()],
                    subscripts: Vec::new(),
                    value: AssignmentValue::Row(vec![
                        Expr::IntLiteral("2".into()),
                        Expr::IntLiteral("3".into()),
                    ]),
                }
        );
        assert!(matches!(
            &assignments[2],
            Assignment { targets, value: AssignmentValue::Subquery(_), .. } if targets.len() == 2
        ));
        assert!(
            assignments[3]
                == Assignment {
                    targets: vec!["f".into()],
                    subscripts: Vec::new(),
                    value: AssignmentValue::Expr(Expr::IntLiteral("6".into())),
                }
        );
    }

    /// A subscripted `SET` target keeps the column name and records the
    /// subscripts separately, so the executor can write into the column's
    /// current value instead of replacing it.
    #[test]
    fn update_parses_subscripted_set_targets() {
        let Statement::Update { assignments, .. } = one("UPDATE t SET j['a'][0] = '1', k = 2")
        else {
            panic!("expected UPDATE");
        };
        assert!(
            assignments[0]
                == Assignment {
                    targets: vec!["j".into()],
                    subscripts: vec![
                        ArraySubscript::Index(Expr::StringLiteral("a".into())),
                        ArraySubscript::Index(Expr::IntLiteral("0".into())),
                    ],
                    value: AssignmentValue::Expr(Expr::StringLiteral("1".into())),
                }
        );
        assert!(assignments[1].subscripts.is_empty());
    }

    #[test]
    fn delete_parses_alias_and_using_list() {
        let Statement::Delete {
            table,
            alias,
            using,
            filter,
            ..
        } = one("DELETE FROM t AS x USING s WHERE x.k = s.k")
        else {
            panic!("expected DELETE");
        };
        assert!(table == crate::ast::RelationRef::bare("t"));
        assert!(alias == Some("x".into()));
        assert!(using.len() == 1);
        assert!(filter.is_some());
    }

    #[test]
    fn returning_parses_the_pg18_old_new_alias_list() {
        let Statement::Update { returning, .. } =
            one("UPDATE t SET a = 1 RETURNING WITH (OLD AS o, NEW AS n) o.a, n.a")
        else {
            panic!("expected UPDATE");
        };
        let returning = returning.expect("RETURNING");
        assert!(returning.old_alias == Some("o".into()));
        assert!(returning.new_alias == Some("n".into()));
        assert!(returning.items.len() == 2);

        let Statement::Delete { returning, .. } = one("DELETE FROM t RETURNING *") else {
            panic!("expected DELETE");
        };
        let returning = returning.expect("RETURNING");
        assert!(returning.old_alias.is_none());
        assert!(returning.new_alias.is_none());
        assert!(returning.items == vec![SelectItem::Wildcard]);
    }

    #[test]
    fn merge_parses_every_when_clause_shape() {
        let Statement::Merge {
            table,
            alias,
            source,
            clauses,
            returning,
            ..
        } = one("MERGE INTO t AS x USING (SELECT 1 AS k) AS s ON x.k = s.k \
             WHEN MATCHED AND s.k > 0 THEN UPDATE SET a = 1 \
             WHEN MATCHED THEN DELETE \
             WHEN NOT MATCHED BY TARGET THEN INSERT (k) VALUES (s.k) \
             WHEN NOT MATCHED BY SOURCE THEN DO NOTHING \
             RETURNING merge_action(), x.k")
        else {
            panic!("expected MERGE");
        };
        assert!(table == crate::ast::RelationRef::bare("t"));
        assert!(alias == Some("x".into()));
        assert!(matches!(source, MergeSource::Query { .. }));
        assert!(returning.is_some());
        let kinds: Vec<MergeMatchKind> = clauses.iter().map(|c| c.kind).collect();
        assert!(
            kinds
                == vec![
                    MergeMatchKind::Matched,
                    MergeMatchKind::Matched,
                    MergeMatchKind::NotMatchedByTarget,
                    MergeMatchKind::NotMatchedBySource,
                ]
        );
        assert!(clauses[0].condition.is_some());
        assert!(matches!(clauses[0].action, MergeAction::Update(_)));
        assert!(clauses[1].action == MergeAction::Delete);
        assert!(matches!(
            clauses[2].action,
            MergeAction::Insert {
                values: Some(_),
                ..
            }
        ));
        assert!(clauses[3].action == MergeAction::DoNothing);

        // `WHEN NOT MATCHED` without a BY clause means BY TARGET.
        let Statement::Merge { clauses, .. } =
            one("MERGE INTO t USING s ON true WHEN NOT MATCHED THEN INSERT DEFAULT VALUES")
        else {
            panic!("expected MERGE");
        };
        assert!(clauses[0].kind == MergeMatchKind::NotMatchedByTarget);
        assert!(
            clauses[0].action
                == MergeAction::Insert {
                    columns: None,
                    values: None
                }
        );
    }

    #[test]
    fn create_table_as_and_select_into_share_one_statement() {
        let Statement::CreateTableAs {
            name,
            if_not_exists,
            columns,
            with_data,
            ..
        } = one("CREATE TABLE IF NOT EXISTS t (a, b) AS SELECT 1, 2 WITH NO DATA")
        else {
            panic!("expected CREATE TABLE AS");
        };
        assert!(name == crate::ast::RelationRef::bare("t"));
        assert!(if_not_exists);
        assert!(columns == Some(vec!["a".into(), "b".into()]));
        assert!(!with_data);

        let Statement::CreateTableAs {
            name,
            if_not_exists,
            columns,
            with_data,
            ..
        } = one("SELECT a INTO TEMP t FROM s")
        else {
            panic!("expected SELECT INTO");
        };
        assert!(name == crate::ast::RelationRef::bare("t"));
        assert!(!if_not_exists);
        assert!(columns.is_none());
        assert!(with_data);

        // An ordinary CREATE TABLE still parses as one, `AS` inside a column
        // default notwithstanding.
        assert!(matches!(
            one("CREATE TABLE t (a int4 DEFAULT CAST(1 AS int4))"),
            Statement::CreateTable { .. }
        ));
    }

    #[test]
    fn table_statement_is_select_star_from_name() {
        for sql in [
            "TABLE t",
            "TABLE ONLY t",
            "TABLE t *",
            "(TABLE t)",
            "TABLE t ORDER BY a LIMIT 1",
            "TABLE t UNION ALL TABLE u",
            "WITH c AS (TABLE t) SELECT * FROM c",
        ] {
            assert!(matches!(one(sql), Statement::Query(_)), "{sql}");
        }
        let Statement::Query(query) = one("TABLE t") else {
            panic!("expected a query");
        };
        let crate::ast::SetExpr::Query(crate::ast::QueryBody::Select(select)) = query.body else {
            panic!("expected a SELECT body");
        };
        assert!(select.projection == vec![SelectItem::Wildcard]);
        assert!(
            select.from
                == vec![TableExpr::Table {
                    name: "t".into(),
                    only: false,
                    alias: None,
                    columns: None,
                    sample: None,
                }]
        );
    }

    #[test]
    fn data_modifying_ctes_parse_as_dml_bodies() {
        let Statement::Query(query) = one(
            "WITH i AS (INSERT INTO t VALUES (1) RETURNING a), d AS (DELETE FROM u RETURNING b), q AS (SELECT 1) SELECT * FROM i",
        ) else {
            panic!("expected a query");
        };
        let with = query.with.expect("WITH");
        assert!(with.has_data_modifying_cte());
        assert!(matches!(with.ctes[0].body, CteBody::Dml(_)));
        assert!(matches!(with.ctes[1].body, CteBody::Dml(_)));
        assert!(matches!(with.ctes[2].body, CteBody::Query(_)));

        let Statement::Insert { with, .. } =
            one("WITH d AS (DELETE FROM t RETURNING a) INSERT INTO u SELECT a FROM d")
        else {
            panic!("expected INSERT");
        };
        let with = with.expect("WITH");
        assert!(with.has_data_modifying_cte());

        let Statement::Query(query) = one("WITH q AS (SELECT 1) SELECT * FROM q") else {
            panic!("expected a query");
        };
        assert!(!query.with.expect("WITH").has_data_modifying_cte());
    }

    #[test]
    fn rejected_shapes_carry_postgresql_sqlstates() {
        let cases = [
            // An action that cannot pair with its match condition.
            (
                "MERGE INTO t USING s ON true WHEN MATCHED THEN INSERT (a) VALUES (1)",
                "42601",
            ),
            (
                "MERGE INTO t USING s ON true WHEN NOT MATCHED THEN DELETE",
                "42601",
            ),
            ("MERGE INTO t USING s ON true", "42601"),
            // A multi-column SET needs a ROW() or a sub-SELECT.
            ("UPDATE t SET (a, b) = 1", "42601"),
            // Only OLD and NEW may be renamed.
            ("UPDATE t SET a = 1 RETURNING WITH (BOTH AS b) a", "42601"),
            (
                "UPDATE t SET a = 1 RETURNING WITH (OLD AS o, OLD AS p) a",
                "42601",
            ),
        ];
        for (sql, expected) in cases {
            assert!(sqlstate(sql) == expected, "{sql}");
        }
    }

    #[test]
    fn alter_type_add_attribute_preserves_the_field_definition() {
        let Statement::AlterType { action, .. } =
            one("ALTER TYPE pair ADD ATTRIBUTE label text COLLATE \"C\" CASCADE")
        else {
            panic!("expected ALTER TYPE");
        };
        assert!(
            action
                == crate::ast::AlterTypeAction::AddAttribute(crate::ast::CompositeFieldDef {
                    name: "label".into(),
                    ty: crabka_pgtypes::ColumnType::Text,
                    collation: Some("C".into()),
                })
        );
    }

    #[test]
    fn alter_operator_family_members_preserve_catalog_keys() {
        use crabka_pgtypes::ColumnType;

        use crate::ast::{
            OperatorFamilyFunctionType, OperatorFamilyMember, OperatorFamilyMemberKey,
            OperatorObjectAlterAction,
        };

        let Statement::Utility(crate::ast::UtilityStatement::AlterOperatorObject {
            action: OperatorObjectAlterAction::AddMembers(add),
            ..
        }) = one("ALTER OPERATOR FAMILY f USING btree ADD \
             OPERATOR 1 < (int4, int2), FUNCTION 1 btint42cmp(int4, int2)")
        else {
            panic!("expected operator-family ADD");
        };
        assert!(matches!(
            &add[0],
            OperatorFamilyMember::Operator {
                number: 1,
                operator,
                left_type: ColumnType::Int4,
                right_type: ColumnType::Int2,
                ..
            } if operator == "<"
        ));
        assert!(matches!(
            &add[1],
            OperatorFamilyMember::Function {
                number: 1,
                argument_types,
                ..
            } if argument_types == &[
                OperatorFamilyFunctionType::Builtin(ColumnType::Int4),
                OperatorFamilyFunctionType::Builtin(ColumnType::Int2),
            ]
        ));
        let Statement::Utility(crate::ast::UtilityStatement::AlterOperatorObject {
            action: OperatorObjectAlterAction::AddMembers(add),
            ..
        }) = one("ALTER OPERATOR FAMILY f USING btree ADD \
             FUNCTION 6 (int4, int2) btint4skipsupport(internal)")
        else {
            panic!("expected internal support function");
        };
        assert!(matches!(
            &add[0],
            OperatorFamilyMember::Function { argument_types, .. }
                if argument_types == &[OperatorFamilyFunctionType::Internal]
        ));

        let Statement::Utility(crate::ast::UtilityStatement::AlterOperatorObject {
            action: OperatorObjectAlterAction::DropMembers(drop),
            ..
        }) = one("ALTER OPERATOR FAMILY f USING btree DROP \
             OPERATOR 1 (int4, int2), FUNCTION 1 (int4)")
        else {
            panic!("expected operator-family DROP");
        };
        assert!(matches!(
            drop.as_slice(),
            [
                OperatorFamilyMemberKey::Operator { number: 1, .. },
                OperatorFamilyMemberKey::Function {
                    number: 1,
                    left_type: ColumnType::Int4,
                    right_type: ColumnType::Int4
                }
            ]
        ));
    }
}

/// Which `ON` clause a [`crate::ast::JsonBehavior`] was written for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonOnClause {
    Empty,
    Error,
}

/// Box a SQL/JSON node into an [`Expr`].
fn sql_json(expr: crate::ast::SqlJsonExpr) -> Expr {
    Expr::SqlJson(Box::new(expr))
}

/// A quotes clause is meaningless when a wrapper is asked for, and `PostgreSQL`
/// rejects it in parse analysis rather than at run time. Only `OMIT QUOTES`
/// conflicts: `KEEP QUOTES` is the default a wrapper already implies, so writing
/// it out is accepted.
fn check_json_table_quotes(
    column: &crate::ast::JsonTableValueColumn,
    position: usize,
) -> Result<(), ParseError> {
    use crate::ast::JsonWrapper;

    if column.omit_quotes == Some(true)
        && matches!(
            column.wrapper,
            Some(JsonWrapper::Conditional | JsonWrapper::Unconditional)
        )
    {
        return Err(ParseError::new_sqlstate(
            "42601",
            "SQL/JSON QUOTES behavior must not be specified when WITH WRAPPER is used",
            position,
        ));
    }
    Ok(())
}

/// The lowercase spelling of the reserved keywords the SQL/JSON grammar also
/// uses as ordinary option words. `None` for every other keyword, so a word
/// match never accepts an unrelated reserved word.
/// One `name [value]` entry of a `COPY` option list, in the shape
/// `PostgreSQL` carries it between grammar and command: the argument stays
/// untyped until the option that owns it says what it means, because the same
/// spelling reads as a boolean to one option and as a column list to another.
struct CopyOption {
    /// The option's name exactly as written. Unquoted words arrive lowercased
    /// and quoted ones do not, which is what makes `("FORMAT" csv)` an
    /// unrecognized option in `PostgreSQL` while `(FORMAT csv)` is not.
    name: String,
    arg: CopyOptionArg,
    /// Byte offset of the name, which is where `PostgreSQL` points when either
    /// the option or its value is rejected.
    pos: usize,
}

/// The argument of a [`CopyOption`], before any option-specific reading of it.
enum CopyOptionArg {
    /// Written bare (`(freeze)`), which every boolean option reads as true.
    Absent,
    Word(String),
    Int(i64),
    Star,
    Columns(Vec<String>),
}

impl CopyOptionArg {
    /// The argument as text, or the "requires a parameter" error a bare option
    /// earns where a value was wanted.
    fn text(&self, option: &CopyOption) -> Result<String, ParseError> {
        match self {
            CopyOptionArg::Absent => Err(ParseError::new_sqlstate(
                "42601",
                format!("{} requires a parameter", option.name),
                option.pos,
            )),
            CopyOptionArg::Word(text) => Ok(text.clone()),
            CopyOptionArg::Int(value) => Ok(value.to_string()),
            CopyOptionArg::Star => Ok("*".into()),
            // A name list renders the way PostgreSQL renders one: dotted.
            CopyOptionArg::Columns(items) => Ok(items.join(".")),
        }
    }

    /// The argument as a boolean: `0`/`1`, or `true`/`false`/`on`/`off` in any
    /// case, or bare for true.
    fn boolean(&self, option: &CopyOption) -> Result<bool, ParseError> {
        match self {
            CopyOptionArg::Int(0) => return Ok(false),
            CopyOptionArg::Absent | CopyOptionArg::Int(1) => return Ok(true),
            _ => {}
        }
        match self.text(option)?.to_ascii_lowercase().as_str() {
            "true" | "on" => Ok(true),
            "false" | "off" => Ok(false),
            _ => Err(ParseError::new_sqlstate(
                "42601",
                format!("{} requires a Boolean value", option.name),
                option.pos,
            )),
        }
    }
}

/// Fold a written `COPY` option list into [`crate::ast::CopyOptions`],
/// rejecting what `PostgreSQL` rejects while the statement alone can tell.
///
/// Options that only make sense in one direction, and those that only make
/// sense in CSV mode, are caught here because nothing outside the statement is
/// needed to see the conflict. Checks that need the *resolved* option set —
/// whether the delimiter is a legal single character once the format's default
/// has been filled in, whether an encoding name is one this build knows — are
/// left to the executor, which is where those defaults live.
fn copy_options(
    written: &[CopyOption],
    is_from: bool,
) -> Result<crate::ast::CopyOptions, ParseError> {
    use crate::ast::{CopyFormat, CopyLogVerbosity, CopyOnError};

    let mut options = crate::ast::CopyOptions::default();
    // Every option may be written at most once; the value doubles as the
    // position to report a later conflict at.
    let mut at: std::collections::HashMap<&'static str, usize> = std::collections::HashMap::new();

    for option in written {
        let canonical = match option.name.as_str() {
            "format" => "format",
            "freeze" => "freeze",
            "delimiter" => "delimiter",
            "null" => "null",
            "default" => "default",
            "header" => "header",
            "quote" => "quote",
            "escape" => "escape",
            "force_quote" => "force_quote",
            "force_not_null" => "force_not_null",
            "force_null" => "force_null",
            "convert_selectively" => "convert_selectively",
            "encoding" => "encoding",
            "on_error" => "on_error",
            "log_verbosity" => "log_verbosity",
            "reject_limit" => "reject_limit",
            other => {
                return Err(ParseError::new_sqlstate(
                    "42601",
                    format!("option \"{other}\" not recognized"),
                    option.pos,
                ));
            }
        };
        if at.insert(canonical, option.pos).is_some() {
            return Err(ParseError::new_sqlstate(
                "42601",
                "conflicting or redundant options",
                option.pos,
            ));
        }

        match canonical {
            "format" => {
                // Matched case-sensitively, as PostgreSQL matches it: a bare
                // `CSV` folds to lowercase on the way in, a quoted `'CSV'` does
                // not, and only the first is a format name.
                let format = option.arg.text(option)?;
                options.format = match format.as_str() {
                    "text" => CopyFormat::Text,
                    "csv" => CopyFormat::Csv,
                    "binary" => {
                        return Err(ParseError::new_sqlstate(
                            "0A000",
                            "COPY BINARY is not supported",
                            option.pos,
                        ));
                    }
                    _ => {
                        return Err(ParseError::new_sqlstate(
                            "22023",
                            format!("COPY format \"{format}\" not recognized"),
                            option.pos,
                        ));
                    }
                };
            }
            "freeze" => options.freeze = option.arg.boolean(option)?,
            "delimiter" => options.delimiter = Some(option.arg.text(option)?),
            "null" => options.null = Some(option.arg.text(option)?),
            "default" => options.default = Some(option.arg.text(option)?),
            "header" => options.header = Some(copy_header(option, is_from)?),
            "quote" => options.quote = Some(option.arg.text(option)?),
            "escape" => options.escape = Some(option.arg.text(option)?),
            "force_quote" => options.force_quote = Some(copy_option_columns(option)?),
            "force_not_null" => options.force_not_null = Some(copy_option_columns(option)?),
            "force_null" => options.force_null = Some(copy_option_columns(option)?),
            "convert_selectively" => {
                // Alone among the column-list options this one may be written
                // bare, which selects nothing rather than everything.
                options.convert_selectively = Some(match &option.arg {
                    CopyOptionArg::Absent => Vec::new(),
                    CopyOptionArg::Columns(columns) => columns.clone(),
                    _ => {
                        return Err(ParseError::new_sqlstate(
                            "22023",
                            format!(
                                "argument to option \"{}\" must be a list of column names",
                                option.name
                            ),
                            option.pos,
                        ));
                    }
                });
            }
            "encoding" => options.encoding = Some(option.arg.text(option)?),
            "on_error" => {
                if !is_from {
                    return Err(copy_wrong_direction("ON_ERROR", is_from, option.pos));
                }
                let choice = option.arg.text(option)?;
                options.on_error = Some(match choice.to_ascii_lowercase().as_str() {
                    "stop" => CopyOnError::Stop,
                    "ignore" => CopyOnError::Ignore,
                    _ => {
                        return Err(ParseError::new_sqlstate(
                            "22023",
                            format!("COPY ON_ERROR \"{choice}\" not recognized"),
                            option.pos,
                        ));
                    }
                });
            }
            "log_verbosity" => {
                let choice = option.arg.text(option)?;
                options.log_verbosity = Some(match choice.to_ascii_lowercase().as_str() {
                    "silent" => CopyLogVerbosity::Silent,
                    "default" => CopyLogVerbosity::Default,
                    "verbose" => CopyLogVerbosity::Verbose,
                    _ => {
                        return Err(ParseError::new_sqlstate(
                            "22023",
                            format!("COPY LOG_VERBOSITY \"{choice}\" not recognized"),
                            option.pos,
                        ));
                    }
                });
            }
            _ => options.reject_limit = Some(copy_reject_limit(option)?),
        }
    }

    copy_options_are_consistent(&options, is_from, &at)?;
    Ok(options)
}

/// The direction and format checks `PostgreSQL` runs once the whole option list
/// is in hand, in its order — a `FORCE_NOT_NULL` outside CSV mode is reported as
/// a mode error even when it is also on the wrong side of the copy.
fn copy_options_are_consistent(
    options: &crate::ast::CopyOptions,
    is_from: bool,
    at: &std::collections::HashMap<&'static str, usize>,
) -> Result<(), ParseError> {
    let pos_of = |option: &str| at.get(option).copied().unwrap_or_default();
    let csv = options.format == crate::ast::CopyFormat::Csv;

    if !csv && options.quote.is_some() {
        return Err(copy_requires_csv("QUOTE", pos_of("quote")));
    }
    if !csv && options.escape.is_some() {
        return Err(copy_requires_csv("ESCAPE", pos_of("escape")));
    }
    if options.force_quote.is_some() {
        if !csv {
            return Err(copy_requires_csv("FORCE_QUOTE", pos_of("force_quote")));
        }
        if is_from {
            return Err(ParseError::new_sqlstate(
                "0A000",
                "COPY FORCE_QUOTE cannot be used with COPY FROM",
                pos_of("force_quote"),
            ));
        }
    }
    if options.force_not_null.is_some() {
        if !csv {
            return Err(copy_requires_csv(
                "FORCE_NOT_NULL",
                pos_of("force_not_null"),
            ));
        }
        if !is_from {
            return Err(copy_wrong_direction(
                "FORCE_NOT_NULL",
                is_from,
                pos_of("force_not_null"),
            ));
        }
    }
    if options.force_null.is_some() {
        if !csv {
            return Err(copy_requires_csv("FORCE_NULL", pos_of("force_null")));
        }
        if !is_from {
            return Err(copy_wrong_direction(
                "FORCE_NULL",
                is_from,
                pos_of("force_null"),
            ));
        }
    }
    if options.freeze && !is_from {
        return Err(copy_wrong_direction("FREEZE", is_from, pos_of("freeze")));
    }
    if options.default.is_some() && !is_from {
        return Err(ParseError::new_sqlstate(
            "0A000",
            "COPY DEFAULT cannot be used with COPY TO",
            pos_of("default"),
        ));
    }
    if options.reject_limit.is_some() && options.on_error != Some(crate::ast::CopyOnError::Ignore) {
        return Err(ParseError::new_sqlstate(
            "22023",
            "COPY REJECT_LIMIT requires ON_ERROR to be set to IGNORE",
            pos_of("reject_limit"),
        ));
    }
    Ok(())
}

fn copy_requires_csv(option: &str, pos: usize) -> ParseError {
    ParseError::new_sqlstate("0A000", format!("COPY {option} requires CSV mode"), pos)
}

fn copy_wrong_direction(option: &str, is_from: bool, pos: usize) -> ParseError {
    let direction = if is_from { "COPY FROM" } else { "COPY TO" };
    ParseError::new_sqlstate(
        "22023",
        format!("COPY {option} cannot be used with {direction}"),
        pos,
    )
}

/// The `HEADER` option: a boolean, or `MATCH` — which asks a `COPY FROM` to
/// check the incoming header against the column list and so has no reading on
/// the `TO` side.
fn copy_header(option: &CopyOption, is_from: bool) -> Result<crate::ast::CopyHeader, ParseError> {
    use crate::ast::CopyHeader;

    match &option.arg {
        CopyOptionArg::Int(0) => return Ok(CopyHeader::False),
        CopyOptionArg::Absent | CopyOptionArg::Int(1) => return Ok(CopyHeader::True),
        _ => {}
    }
    let choice = option.arg.text(option)?;
    match choice.to_ascii_lowercase().as_str() {
        "true" | "on" => Ok(CopyHeader::True),
        "false" | "off" => Ok(CopyHeader::False),
        "match" if is_from => Ok(CopyHeader::Match),
        "match" => Err(ParseError::new_sqlstate(
            "0A000",
            format!("cannot use \"{choice}\" with HEADER in COPY TO"),
            option.pos,
        )),
        _ => Err(ParseError::new_sqlstate(
            "42601",
            format!("{} requires a Boolean value or \"match\"", option.name),
            option.pos,
        )),
    }
}

/// The argument of `FORCE_QUOTE` / `FORCE_NOT_NULL` / `FORCE_NULL`: a column
/// list, or `*` for every column.
fn copy_option_columns(option: &CopyOption) -> Result<crate::ast::CopyColumns, ParseError> {
    use crate::ast::CopyColumns;

    match &option.arg {
        CopyOptionArg::Star => Ok(CopyColumns::All),
        CopyOptionArg::Columns(columns) => Ok(CopyColumns::Named(columns.clone())),
        _ => Err(ParseError::new_sqlstate(
            "22023",
            format!(
                "argument to option \"{}\" must be a list of column names",
                option.name
            ),
            option.pos,
        )),
    }
}

/// The `REJECT_LIMIT` option: a positive `bigint`, whether written as a number
/// or as a string that reads as one.
fn copy_reject_limit(option: &CopyOption) -> Result<i64, ParseError> {
    let limit = match &option.arg {
        CopyOptionArg::Absent => {
            return Err(ParseError::new_sqlstate(
                "42601",
                format!("{} requires a numeric value", option.name),
                option.pos,
            ));
        }
        CopyOptionArg::Int(value) => *value,
        arg => {
            let written = arg.text(option)?;
            written.trim().parse::<i64>().map_err(|_| {
                ParseError::new_sqlstate(
                    "22P02",
                    format!("invalid input syntax for type bigint: \"{written}\""),
                    option.pos,
                )
            })?
        }
    };
    if limit <= 0 {
        return Err(ParseError::new_sqlstate(
            "22023",
            format!("REJECT_LIMIT ({limit}) must be greater than zero"),
            option.pos,
        ));
    }
    Ok(limit)
}

fn keyword_word(kw: Keyword) -> Option<&'static str> {
    Some(match kw {
        Keyword::Array => "array",
        Keyword::With => "with",
        Keyword::Unique => "unique",
        Keyword::On => "on",
        Keyword::Null => "null",
        Keyword::True => "true",
        Keyword::False => "false",
        Keyword::Wrapper => "wrapper",
        Keyword::Returning => "returning",
        Keyword::Exists => "exists",
        _ => return None,
    })
}
