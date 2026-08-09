//! `PostgreSQL`'s `jsonpath` language: the path expressions behind `@?`, `@@`
//! and the `jsonb_path_*` function family.
//!
//! [`JsonPath::parse`] compiles a jsonpath once into a tree of [`Node`]s and
//! [`Pred`]icates. [`JsonPath::query`] / [`JsonPath::exists`] /
//! [`JsonPath::predicate`] then run it over a `jsonb` target.
//!
//! Two things make the language unlike an ordinary expression evaluator, and
//! this module reproduces both:
//!
//! - **Every expression evaluates to a *sequence* of JSON items**, not to one
//!   value. `$.a` over `{"a": 1}` is `[1]`, over `{}` it is `[]`, and `$[*]`
//!   over `[1, 2]` is `[1, 2]`. So comparisons have existential semantics over
//!   the cross product of their operands' sequences.
//! - **`lax` mode (the default) auto-unwraps and auto-wraps.** A member
//!   accessor on an array applies to each element instead. An array accessor on
//!   a non-array treats it as a one-element array. `strict` mode raises the
//!   structural error instead. This is why `lax $.a` over `[{"a": 1}]` is `[1]`
//!   while `strict $.a` is `2203A`.
//!
//! Predicates are three-valued ([`Tri`]): a structural error inside a filter is
//! `Unknown` rather than a raised error, which is what makes
//! `$ ? (@.missing == 1)` a quiet no-match.

use std::fmt::Write as _;

use bigdecimal::{BigDecimal, One, ToPrimitive, Zero};
use crabka_pgtypes::JsonbValue;

use crate::error::ExecError;

/// The maximum accessor-chain nesting the parser accepts, so an adversarial
/// path cannot overflow the recursive-descent parser's stack.
const MAX_DEPTH: u32 = 128;

/// The maximum number of items one path evaluation may produce.
///
/// `PostgreSQL` has no such cap. Without it, `.**` over a deeply nested
/// document is unbounded work inside a single statement.
const MAX_ITEMS: usize = 1_000_000;

/// A compiled jsonpath.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonPath {
    /// `strict` mode: structural mismatches are errors, and the engine does
    /// not unwrap them away. `lax` is the default when no mode word is
    /// written.
    pub strict: bool,
    /// `true` when the whole path is a *predicate* (`$.a == 1`) rather than a
    /// path expression. `jsonb_path_match` needs this shape, and
    /// `jsonb_path_query` renders the boolean as a JSON `true`/`false`.
    pub is_predicate: bool,
    root: Node,
}

/// One node of a path expression.
#[derive(Debug, Clone, PartialEq)]
enum Node {
    /// `$`: the query target.
    Root,
    /// `@`: the item the innermost enclosing filter tests.
    Current,
    /// `last`: the last subscript of the indexed array.
    Last,
    /// `$name` / `$"name"`: a member of the `vars` argument.
    Var(String),
    /// A literal number, string, `true`, `false` or `null`.
    Literal(JsonbValue),
    /// `base <accessor>`.
    Accessor { base: Box<Node>, op: Accessor },
    /// Unary `+` / `-`.
    Neg { arg: Box<Node>, negate: bool },
    /// `left <op> right`, one of `+ - * / %`.
    Arith {
        op: ArithOp,
        left: Box<Node>,
        right: Box<Node>,
    },
    /// A predicate in value position, the top-level `$.a == 1` form, whose
    /// value is the JSON boolean the predicate evaluates to.
    Predicate(Box<Pred>),
}

/// A postfix accessor applied to the item(s) a [`Node`] produced.
#[derive(Debug, Clone, PartialEq)]
enum Accessor {
    /// `.key` / `."key"`.
    Member(String),
    /// `.*`: every member value of an object.
    MemberAll,
    /// `[i]`, `[i to j]`, and the comma-separated list of both.
    Index(Vec<(Node, Option<Node>)>),
    /// `[*]`: every element of an array.
    IndexAll,
    /// `.**`, `.**{n}`, `.**{n to m}`: recursive descent, at the given depths.
    Any { from: u32, to: u32 },
    /// `.type()`, `.size()`, …: an item method.
    Method(Method),
    /// `? (predicate)`: keep only the items the predicate is true for.
    Filter(Box<Pred>),
}

/// The item methods, spelled as `PostgreSQL` spells them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Method {
    Type,
    Size,
    Double,
    Ceiling,
    Floor,
    Abs,
    KeyValue,
    Bigint,
    Boolean,
    Decimal,
    Integer,
    Number,
    String,
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
    Datetime,
}

/// The arithmetic operators, in `PostgreSQL`'s spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl ArithOp {
    fn symbol(self) -> &'static str {
        match self {
            ArithOp::Add => "+",
            ArithOp::Sub => "-",
            ArithOp::Mul => "*",
            ArithOp::Div => "/",
            ArithOp::Mod => "%",
        }
    }
}

/// The comparison operators usable inside a predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A jsonpath predicate.
#[derive(Debug, Clone, PartialEq)]
enum Pred {
    And(Box<Pred>, Box<Pred>),
    Or(Box<Pred>, Box<Pred>),
    Not(Box<Pred>),
    /// `(predicate) is unknown`.
    IsUnknown(Box<Pred>),
    /// `exists (path)`.
    Exists(Box<Node>),
    Compare {
        op: CmpOp,
        left: Box<Node>,
        right: Box<Node>,
    },
    /// `expr starts with prefix`.
    StartsWith {
        value: Box<Node>,
        prefix: Box<Node>,
    },
    /// `expr like_regex "pattern" [flag "flags"]`.
    LikeRegex {
        value: Box<Node>,
        pattern: String,
        flags: String,
    },
}

/// SQL/JSON three-valued logic.
///
/// A structural error inside a predicate is [`Tri::Unknown`], which is how
/// `$ ? (@.missing > 1)` quietly matches nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tri {
    True,
    False,
    Unknown,
}

impl Tri {
    fn of(value: bool) -> Self {
        if value { Tri::True } else { Tri::False }
    }

    fn not(self) -> Self {
        match self {
            Tri::True => Tri::False,
            Tri::False => Tri::True,
            Tri::Unknown => Tri::Unknown,
        }
    }
}

// ---- errors ----

/// A jsonpath *evaluation* error, carrying `PostgreSQL`'s SQLSTATE and message.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PathError {
    sqlstate: &'static str,
    message: String,
}

impl PathError {
    fn new(sqlstate: &'static str, message: impl Into<String>) -> Self {
        PathError {
            sqlstate,
            message: message.into(),
        }
    }

    fn into_exec(self) -> ExecError {
        ExecError::FunctionError {
            sqlstate: self.sqlstate,
            message: self.message,
        }
    }
}

type PathResult<T> = Result<T, PathError>;

/// A jsonpath *syntax* error.
///
/// `PostgreSQL` reports every one of these as 42601, with a message that names
/// the offending token.
fn syntax(message: impl Into<String>) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42601",
        message: message.into(),
    }
}

// ---- lexer ----

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// A bare word: a keyword, method name or object key.
    Word(String),
    /// A double-quoted string (a key or a string literal).
    Str(String),
    /// `$name` or `$"name"`.
    Var(String),
    Num(BigDecimal),
    Dollar,
    At,
    Star,
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Comma,
    Dot,
    Question,
    Plus,
    Minus,
    Slash,
    Percent,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Not,
    Eof,
}

impl Tok {
    /// The spelling `PostgreSQL` prints for this token in a syntax error.
    fn spelling(&self) -> String {
        match self {
            Tok::Word(w) => w.clone(),
            Tok::Str(s) => format!("\"{s}\""),
            Tok::Var(v) => format!("${v}"),
            Tok::Num(n) => n.to_string(),
            Tok::Dollar => "$".into(),
            Tok::At => "@".into(),
            Tok::Star => "*".into(),
            Tok::LParen => "(".into(),
            Tok::RParen => ")".into(),
            Tok::LBracket => "[".into(),
            Tok::RBracket => "]".into(),
            Tok::LBrace => "{".into(),
            Tok::RBrace => "}".into(),
            Tok::Comma => ",".into(),
            Tok::Dot => ".".into(),
            Tok::Question => "?".into(),
            Tok::Plus => "+".into(),
            Tok::Minus => "-".into(),
            Tok::Slash => "/".into(),
            Tok::Percent => "%".into(),
            Tok::Eq => "==".into(),
            Tok::Ne => "!=".into(),
            Tok::Lt => "<".into(),
            Tok::Le => "<=".into(),
            Tok::Gt => ">".into(),
            Tok::Ge => ">=".into(),
            Tok::And => "&&".into(),
            Tok::Or => "||".into(),
            Tok::Not => "!".into(),
            Tok::Eof => "end of input".into(),
        }
    }
}

/// Split a jsonpath into tokens.
///
/// `PostgreSQL` reports every lexical problem as 42601 "syntax error … of
/// jsonpath input".
fn lex(src: &str) -> Result<Vec<Tok>, ExecError> {
    let chars: Vec<char> = src.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        // `/* … */` is jsonpath's only comment form.
        if c == '/' && chars.get(i + 1) == Some(&'*') {
            let mut j = i + 2;
            loop {
                if j + 1 >= chars.len() {
                    return Err(syntax("unexpected end of comment of jsonpath input"));
                }
                if chars[j] == '*' && chars[j + 1] == '/' {
                    break;
                }
                j += 1;
            }
            i = j + 2;
            continue;
        }
        let two: Option<char> = chars.get(i + 1).copied();
        let (tok, len) = match (c, two) {
            ('=', Some('=')) => (Tok::Eq, 2),
            ('!', Some('=')) => (Tok::Ne, 2),
            ('<', Some('>')) => (Tok::Ne, 2),
            ('<', Some('=')) => (Tok::Le, 2),
            ('>', Some('=')) => (Tok::Ge, 2),
            ('&', Some('&')) => (Tok::And, 2),
            ('|', Some('|')) => (Tok::Or, 2),
            ('<', _) => (Tok::Lt, 1),
            ('>', _) => (Tok::Gt, 1),
            ('!', _) => (Tok::Not, 1),
            ('@', _) => (Tok::At, 1),
            ('*', _) => (Tok::Star, 1),
            ('(', _) => (Tok::LParen, 1),
            (')', _) => (Tok::RParen, 1),
            ('[', _) => (Tok::LBracket, 1),
            (']', _) => (Tok::RBracket, 1),
            ('{', _) => (Tok::LBrace, 1),
            ('}', _) => (Tok::RBrace, 1),
            (',', _) => (Tok::Comma, 1),
            ('?', _) => (Tok::Question, 1),
            ('+', _) => (Tok::Plus, 1),
            ('-', _) => (Tok::Minus, 1),
            ('/', _) => (Tok::Slash, 1),
            ('%', _) => (Tok::Percent, 1),
            ('.', Some(d)) if d.is_ascii_digit() => {
                let (num, len) = lex_number(&chars, i)?;
                out.push(Tok::Num(num));
                i += len;
                continue;
            }
            ('.', _) => (Tok::Dot, 1),
            ('"', _) => {
                let (s, len) = lex_quoted(&chars, i)?;
                out.push(Tok::Str(s));
                i += len;
                continue;
            }
            ('$', Some('"')) => {
                let (s, len) = lex_quoted(&chars, i + 1)?;
                out.push(Tok::Var(s));
                i += len + 1;
                continue;
            }
            ('$', Some(d)) if is_ident_start(d) => {
                let mut j = i + 1;
                while j < chars.len() && is_ident_cont(chars[j]) {
                    j += 1;
                }
                out.push(Tok::Var(chars[i + 1..j].iter().collect()));
                i = j;
                continue;
            }
            ('$', _) => (Tok::Dollar, 1),
            (d, _) if d.is_ascii_digit() => {
                let (num, len) = lex_number(&chars, i)?;
                out.push(Tok::Num(num));
                i += len;
                continue;
            }
            (w, _) if is_ident_start(w) => {
                let mut j = i;
                while j < chars.len() && is_ident_cont(chars[j]) {
                    j += 1;
                }
                out.push(Tok::Word(chars[i..j].iter().collect()));
                i = j;
                continue;
            }
            _ => {
                return Err(syntax(format!(
                    "syntax error at or near \"{c}\" of jsonpath input"
                )));
            }
        };
        out.push(tok);
        i += len;
    }
    out.push(Tok::Eof);
    Ok(out)
}

fn is_ident_start(c: char) -> bool {
    c == '_' || c.is_alphabetic() || !c.is_ascii()
}

fn is_ident_cont(c: char) -> bool {
    is_ident_start(c) || c.is_ascii_digit() || c == '$'
}

/// A number literal: optional integer part, optional fraction, optional
/// exponent.
///
/// `PostgreSQL` also accepts `0x`/`0o`/`0b` and `_` separators. This parser
/// deliberately does not accept those. See the module divergence list.
fn lex_number(chars: &[char], start: usize) -> Result<(BigDecimal, usize), ExecError> {
    let mut j = start;
    while j < chars.len() && chars[j].is_ascii_digit() {
        j += 1;
    }
    if chars.get(j) == Some(&'.') && chars.get(j + 1).is_some_and(char::is_ascii_digit) {
        j += 1;
        while j < chars.len() && chars[j].is_ascii_digit() {
            j += 1;
        }
    }
    if matches!(chars.get(j), Some('e' | 'E')) {
        let mut k = j + 1;
        if matches!(chars.get(k), Some('+' | '-')) {
            k += 1;
        }
        if chars.get(k).is_some_and(char::is_ascii_digit) {
            while k < chars.len() && chars[k].is_ascii_digit() {
                k += 1;
            }
            j = k;
        }
    }
    let text: String = chars[start..j].iter().collect();
    let value = text.parse::<BigDecimal>().map_err(|_| {
        syntax(format!(
            "syntax error at or near \"{text}\" of jsonpath input"
        ))
    })?;
    Ok((value, j - start))
}

/// A `"…"` string, with JSON's escape set.
fn lex_quoted(chars: &[char], start: usize) -> Result<(String, usize), ExecError> {
    let mut out = String::new();
    let mut j = start + 1;
    loop {
        let Some(&c) = chars.get(j) else {
            return Err(syntax("unexpected end of quoted string of jsonpath input"));
        };
        match c {
            '"' => {
                j += 1;
                break;
            }
            '\\' => {
                let Some(&e) = chars.get(j + 1) else {
                    return Err(syntax("unexpected end of quoted string of jsonpath input"));
                };
                j += 2;
                match e {
                    '"' => out.push('"'),
                    '\\' => out.push('\\'),
                    '/' => out.push('/'),
                    'b' => out.push('\u{8}'),
                    'f' => out.push('\u{c}'),
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    't' => out.push('\t'),
                    'u' => {
                        let hex: String = chars.get(j..j + 4).unwrap_or_default().iter().collect();
                        let code = u32::from_str_radix(&hex, 16)
                            .map_err(|_| syntax("invalid unicode escape of jsonpath input"))?;
                        j += 4;
                        out.push(
                            char::from_u32(code).ok_or_else(|| {
                                syntax("invalid unicode escape of jsonpath input")
                            })?,
                        );
                    }
                    other => {
                        return Err(syntax(format!(
                            "syntax error at or near \"\\{other}\" of jsonpath input"
                        )));
                    }
                }
            }
            other => {
                out.push(other);
                j += 1;
            }
        }
    }
    Ok((out, j - start))
}

// ---- parser ----

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
    depth: u32,
}

/// The method names that follow a `.` and an opening parenthesis.
fn method_of(word: &str) -> Option<Method> {
    Some(match word.to_ascii_lowercase().as_str() {
        "type" => Method::Type,
        "size" => Method::Size,
        "double" => Method::Double,
        "ceiling" => Method::Ceiling,
        "floor" => Method::Floor,
        "abs" => Method::Abs,
        "keyvalue" => Method::KeyValue,
        "bigint" => Method::Bigint,
        "boolean" => Method::Boolean,
        "decimal" => Method::Decimal,
        "integer" => Method::Integer,
        "number" => Method::Number,
        "string" => Method::String,
        "date" => Method::Date,
        "time" => Method::Time,
        "time_tz" => Method::TimeTz,
        "timestamp" => Method::Timestamp,
        "timestamp_tz" => Method::TimestampTz,
        "datetime" => Method::Datetime,
        _ => return None,
    })
}

impl Parser {
    fn peek(&self) -> &Tok {
        self.toks.get(self.pos).unwrap_or(&Tok::Eof)
    }

    fn peek2(&self) -> &Tok {
        self.toks.get(self.pos + 1).unwrap_or(&Tok::Eof)
    }

    fn bump(&mut self) -> Tok {
        let t = self.peek().clone();
        self.pos += 1;
        t
    }

    fn eat(&mut self, tok: &Tok) -> bool {
        if self.peek() == tok {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn eat_word(&mut self, word: &str) -> bool {
        if matches!(self.peek(), Tok::Word(w) if w.eq_ignore_ascii_case(word)) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn peek_word(&self, word: &str) -> bool {
        matches!(self.peek(), Tok::Word(w) if w.eq_ignore_ascii_case(word))
    }

    fn expect(&mut self, tok: &Tok) -> Result<(), ExecError> {
        if self.eat(tok) {
            Ok(())
        } else {
            Err(self.error_here())
        }
    }

    fn error_here(&self) -> ExecError {
        match self.peek() {
            Tok::Eof => syntax("syntax error at end of jsonpath input"),
            other => syntax(format!(
                "syntax error at or near \"{}\" of jsonpath input",
                other.spelling()
            )),
        }
    }

    fn enter(&mut self) -> Result<(), ExecError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(syntax("jsonpath input is too deeply nested"));
        }
        Ok(())
    }

    fn leave(&mut self) {
        self.depth -= 1;
    }

    /// `expr_or_predicate`, the whole path body after the optional mode word.
    ///
    /// The parser recognizes a predicate when it parses an expression and then
    /// finds a predicate operator, or from the leading `!` / `exists` forms.
    fn expr_or_predicate(&mut self) -> Result<(Node, bool), ExecError> {
        let pred = self.predicate_or_expr()?;
        Ok(match pred {
            Either::Expr(node) => (node, false),
            Either::Pred(p) => (Node::Predicate(Box::new(p)), true),
        })
    }

    /// Parse whatever comes next and report whether it is a predicate or a
    /// plain path expression.
    ///
    /// This mirrors `PostgreSQL`'s grammar, where the two share a prefix and
    /// only the operator after the first operand decides.
    fn predicate_or_expr(&mut self) -> Result<Either, ExecError> {
        self.enter()?;
        let result = self.or_level();
        self.leave();
        result
    }

    fn or_level(&mut self) -> Result<Either, ExecError> {
        let mut left = self.and_level()?;
        while self.eat(&Tok::Or) {
            let right = self.and_level()?;
            left = Either::Pred(Pred::Or(
                Box::new(left.into_pred()?),
                Box::new(right.into_pred()?),
            ));
        }
        Ok(left)
    }

    fn and_level(&mut self) -> Result<Either, ExecError> {
        let mut left = self.compare_level()?;
        while self.eat(&Tok::And) {
            let right = self.compare_level()?;
            left = Either::Pred(Pred::And(
                Box::new(left.into_pred()?),
                Box::new(right.into_pred()?),
            ));
        }
        Ok(left)
    }

    /// The comparison / `starts with` / `like_regex` / `is unknown` level.
    ///
    /// Each is non-associative in `PostgreSQL`'s grammar, so exactly one may
    /// appear.
    fn compare_level(&mut self) -> Result<Either, ExecError> {
        let left = self.additive_or_unary()?;
        let op = match self.peek() {
            Tok::Eq => Some(CmpOp::Eq),
            Tok::Ne => Some(CmpOp::Ne),
            Tok::Lt => Some(CmpOp::Lt),
            Tok::Le => Some(CmpOp::Le),
            Tok::Gt => Some(CmpOp::Gt),
            Tok::Ge => Some(CmpOp::Ge),
            _ => None,
        };
        if let Some(op) = op {
            self.bump();
            let right = self.additive_or_unary()?.into_expr()?;
            return Ok(Either::Pred(Pred::Compare {
                op,
                left: Box::new(left.into_expr()?),
                right: Box::new(right),
            }));
        }
        if self.peek_word("starts") {
            self.bump();
            if !self.eat_word("with") {
                return Err(self.error_here());
            }
            let prefix = self.starts_with_initial()?;
            return Ok(Either::Pred(Pred::StartsWith {
                value: Box::new(left.into_expr()?),
                prefix: Box::new(prefix),
            }));
        }
        if self.peek_word("like_regex") {
            self.bump();
            let Tok::Str(pattern) = self.bump() else {
                return Err(self.error_here());
            };
            let mut flags = String::new();
            if self.peek_word("flag") {
                self.bump();
                let Tok::Str(f) = self.bump() else {
                    return Err(self.error_here());
                };
                flags = f;
            }
            return Ok(Either::Pred(Pred::LikeRegex {
                value: Box::new(left.into_expr()?),
                pattern,
                flags,
            }));
        }
        if self.peek_word("is")
            && matches!(self.peek2(), Tok::Word(w) if w.eq_ignore_ascii_case("unknown"))
        {
            self.bump();
            self.bump();
            return Ok(Either::Pred(Pred::IsUnknown(Box::new(left.into_pred()?))));
        }
        Ok(left)
    }

    /// `STRING | $var`: the only two spellings `starts with` accepts.
    fn starts_with_initial(&mut self) -> Result<Node, ExecError> {
        match self.bump() {
            Tok::Str(s) => Ok(Node::Literal(JsonbValue::String(s))),
            Tok::Var(v) => Ok(Node::Var(v)),
            _ => {
                self.pos -= 1;
                Err(self.error_here())
            }
        }
    }

    fn additive_or_unary(&mut self) -> Result<Either, ExecError> {
        if self.eat(&Tok::Not) {
            let arg = self.delimited_predicate()?;
            return Ok(Either::Pred(Pred::Not(Box::new(arg))));
        }
        let mut left = self.multiplicative()?;
        loop {
            let op = match self.peek() {
                Tok::Plus => ArithOp::Add,
                Tok::Minus => ArithOp::Sub,
                _ => break,
            };
            self.bump();
            let right = self.multiplicative()?;
            left = Either::Expr(Node::Arith {
                op,
                left: Box::new(left.into_expr()?),
                right: Box::new(right.into_expr()?),
            });
        }
        Ok(left)
    }

    fn multiplicative(&mut self) -> Result<Either, ExecError> {
        let mut left = self.unary()?;
        loop {
            let op = match self.peek() {
                Tok::Star => ArithOp::Mul,
                Tok::Slash => ArithOp::Div,
                Tok::Percent => ArithOp::Mod,
                _ => break,
            };
            self.bump();
            let right = self.unary()?;
            left = Either::Expr(Node::Arith {
                op,
                left: Box::new(left.into_expr()?),
                right: Box::new(right.into_expr()?),
            });
        }
        Ok(left)
    }

    fn unary(&mut self) -> Result<Either, ExecError> {
        if self.eat(&Tok::Minus) {
            let arg = self.unary()?.into_expr()?;
            return Ok(Either::Expr(Node::Neg {
                arg: Box::new(arg),
                negate: true,
            }));
        }
        if self.eat(&Tok::Plus) {
            let arg = self.unary()?.into_expr()?;
            return Ok(Either::Expr(Node::Neg {
                arg: Box::new(arg),
                negate: false,
            }));
        }
        self.accessor_expr()
    }

    /// `exists ( expr )` or `( predicate )`, the two forms `!` and the
    /// grammar's `delimited_predicate` accept.
    fn delimited_predicate(&mut self) -> Result<Pred, ExecError> {
        if self.peek_word("exists") {
            self.bump();
            self.expect(&Tok::LParen)?;
            let inner = self.predicate_or_expr()?.into_expr()?;
            self.expect(&Tok::RParen)?;
            return Ok(Pred::Exists(Box::new(inner)));
        }
        self.expect(&Tok::LParen)?;
        let inner = self.predicate_or_expr()?.into_pred()?;
        self.expect(&Tok::RParen)?;
        Ok(inner)
    }

    fn accessor_expr(&mut self) -> Result<Either, ExecError> {
        self.enter()?;
        let result = self.accessor_expr_inner();
        self.leave();
        result
    }

    fn accessor_expr_inner(&mut self) -> Result<Either, ExecError> {
        // `exists (…)` is a predicate and never carries accessors.
        if self.peek_word("exists") && *self.peek2() == Tok::LParen {
            self.bump();
            self.expect(&Tok::LParen)?;
            let inner = self.predicate_or_expr()?.into_expr()?;
            self.expect(&Tok::RParen)?;
            return Ok(Either::Pred(Pred::Exists(Box::new(inner))));
        }
        let mut base = self.primary()?;
        loop {
            match self.peek() {
                Tok::Dot => {
                    self.bump();
                    let op = self.dot_accessor()?;
                    base = Either::Expr(Node::Accessor {
                        base: Box::new(base.into_expr()?),
                        op,
                    });
                }
                Tok::LBracket => {
                    self.bump();
                    let op = if self.eat(&Tok::Star) {
                        self.expect(&Tok::RBracket)?;
                        Accessor::IndexAll
                    } else {
                        let mut subs = Vec::new();
                        loop {
                            let lo = self.predicate_or_expr()?.into_expr()?;
                            let hi = if self.eat_word("to") {
                                Some(self.predicate_or_expr()?.into_expr()?)
                            } else {
                                None
                            };
                            subs.push((lo, hi));
                            if self.eat(&Tok::Comma) {
                                continue;
                            }
                            break;
                        }
                        self.expect(&Tok::RBracket)?;
                        Accessor::Index(subs)
                    };
                    base = Either::Expr(Node::Accessor {
                        base: Box::new(base.into_expr()?),
                        op,
                    });
                }
                Tok::Question => {
                    self.bump();
                    self.expect(&Tok::LParen)?;
                    let pred = self.predicate_or_expr()?.into_pred()?;
                    self.expect(&Tok::RParen)?;
                    base = Either::Expr(Node::Accessor {
                        base: Box::new(base.into_expr()?),
                        op: Accessor::Filter(Box::new(pred)),
                    });
                }
                _ => break,
            }
        }
        Ok(base)
    }

    /// The accessor after a `.`: a member name, `*`, `**`, or a method call.
    fn dot_accessor(&mut self) -> Result<Accessor, ExecError> {
        if self.eat(&Tok::Star) {
            // `.**` — recursive descent, optionally depth-bounded by `{…}`.
            if self.eat(&Tok::Star) {
                if self.eat(&Tok::LBrace) {
                    let from = self.level_number()?;
                    let to = if self.eat_word("to") {
                        if self.eat_word("last") {
                            u32::MAX
                        } else {
                            self.level_number()?
                        }
                    } else {
                        from
                    };
                    self.expect(&Tok::RBrace)?;
                    return Ok(Accessor::Any { from, to });
                }
                return Ok(Accessor::Any {
                    from: 0,
                    to: u32::MAX,
                });
            }
            return Ok(Accessor::MemberAll);
        }
        match self.bump() {
            Tok::Str(s) => Ok(Accessor::Member(s)),
            Tok::Word(w) => {
                if *self.peek() == Tok::LParen {
                    let Some(method) = method_of(&w) else {
                        return Err(self.error_here());
                    };
                    self.bump();
                    // `.datetime("template")` takes an optional template; the
                    // template is parsed and rejected (see the divergence list).
                    if let Tok::Str(_) = self.peek() {
                        if method == Method::Datetime {
                            return Err(ExecError::Unsupported(
                                "jsonpath .datetime(template) is not supported".into(),
                            ));
                        }
                        return Err(self.error_here());
                    }
                    self.expect(&Tok::RParen)?;
                    return Ok(Accessor::Method(method));
                }
                Ok(Accessor::Member(w))
            }
            _ => {
                self.pos -= 1;
                Err(self.error_here())
            }
        }
    }

    fn level_number(&mut self) -> Result<u32, ExecError> {
        match self.bump() {
            Tok::Num(n) => n
                .to_u32()
                .ok_or_else(|| syntax("invalid nesting level in jsonpath input")),
            _ => {
                self.pos -= 1;
                Err(self.error_here())
            }
        }
    }

    fn primary(&mut self) -> Result<Either, ExecError> {
        match self.bump() {
            Tok::Dollar => Ok(Either::Expr(Node::Root)),
            Tok::At => Ok(Either::Expr(Node::Current)),
            Tok::Var(v) => Ok(Either::Expr(Node::Var(v))),
            Tok::Num(n) => Ok(Either::Expr(Node::Literal(JsonbValue::Number(n)))),
            Tok::Str(s) => Ok(Either::Expr(Node::Literal(JsonbValue::String(s)))),
            Tok::LParen => {
                self.enter()?;
                let inner = self.predicate_or_expr();
                self.leave();
                let inner = inner?;
                self.expect(&Tok::RParen)?;
                Ok(inner)
            }
            Tok::Word(w) => Ok(Either::Expr(match w.to_ascii_lowercase().as_str() {
                "null" => Node::Literal(JsonbValue::Null),
                "true" => Node::Literal(JsonbValue::Bool(true)),
                "false" => Node::Literal(JsonbValue::Bool(false)),
                "last" => Node::Last,
                _ => {
                    self.pos -= 1;
                    return Err(self.error_here());
                }
            })),
            _ => {
                self.pos -= 1;
                Err(self.error_here())
            }
        }
    }
}

/// A parsed fragment that is still ambiguous between a path expression and a
/// predicate.
///
/// The two share a prefix in `PostgreSQL`'s grammar.
enum Either {
    Expr(Node),
    Pred(Pred),
}

impl Either {
    fn into_expr(self) -> Result<Node, ExecError> {
        match self {
            Either::Expr(node) => Ok(node),
            // `$ ? (@.a == 1 == 2)` and friends: a predicate cannot be an
            // operand, which is what `%nonassoc` on the comparison level means.
            Either::Pred(_) => Err(syntax(
                "syntax error at or near comparison operator of jsonpath input",
            )),
        }
    }

    fn into_pred(self) -> Result<Pred, ExecError> {
        match self {
            Either::Pred(p) => Ok(p),
            Either::Expr(_) => Err(syntax(
                "syntax error, expected a predicate of jsonpath input",
            )),
        }
    }
}

impl JsonPath {
    /// Compile a jsonpath.
    ///
    /// Every failure is `PostgreSQL`'s 42601 "syntax error … of jsonpath
    /// input".
    pub fn parse(src: &str) -> Result<Self, ExecError> {
        let toks = lex(src)?;
        let mut p = Parser {
            toks,
            pos: 0,
            depth: 0,
        };
        let strict = if p.peek_word("strict") {
            p.bump();
            true
        } else {
            if p.peek_word("lax") {
                p.bump();
            }
            false
        };
        let (root, is_predicate) = p.expr_or_predicate()?;
        if *p.peek() != Tok::Eof {
            return Err(p.error_here());
        }
        Ok(JsonPath {
            strict,
            is_predicate,
            root,
        })
    }

    /// Every item the path produces over `target`.
    ///
    /// `vars` is the optional `vars` argument (a jsonb object). `silent`
    /// suppresses the structural errors `strict` mode would otherwise raise.
    /// That is the `silent => true` flag of `jsonb_path_query`, and the
    /// behavior `@?` and `@@` always use.
    pub fn query(
        &self,
        target: &JsonbValue,
        vars: Option<&JsonbValue>,
        silent: bool,
    ) -> Result<Vec<JsonbValue>, ExecError> {
        let exec = Exec {
            strict: self.strict,
            vars,
            root: target,
            last: None,
        };
        match exec.eval(&self.root, target) {
            Ok(items) => Ok(items),
            Err(e) if silent && is_structural(&e) => Ok(Vec::new()),
            Err(e) => Err(e.into_exec()),
        }
    }

    /// `@?` / `jsonb_path_exists`: does the path produce at least one item?
    pub fn exists(
        &self,
        target: &JsonbValue,
        vars: Option<&JsonbValue>,
        silent: bool,
    ) -> Result<Option<bool>, ExecError> {
        let exec = Exec {
            strict: self.strict,
            vars,
            root: target,
            last: None,
        };
        match exec.eval(&self.root, target) {
            Ok(items) => Ok(Some(!items.is_empty())),
            Err(e) if silent && is_structural(&e) => Ok(None),
            Err(e) => Err(e.into_exec()),
        }
    }

    /// `@@` / `jsonb_path_match`: the path must produce exactly one boolean.
    pub fn predicate(
        &self,
        target: &JsonbValue,
        vars: Option<&JsonbValue>,
        silent: bool,
    ) -> Result<Option<bool>, ExecError> {
        let exec = Exec {
            strict: self.strict,
            vars,
            root: target,
            last: None,
        };
        let items = match exec.eval(&self.root, target) {
            Ok(items) => items,
            Err(e) if silent && is_structural(&e) => return Ok(None),
            Err(e) => return Err(e.into_exec()),
        };
        match items.as_slice() {
            [JsonbValue::Bool(b)] => Ok(Some(*b)),
            [JsonbValue::Null] => Ok(None),
            [] => {
                if silent {
                    Ok(None)
                } else {
                    Err(no_boolean_result())
                }
            }
            _ => {
                if silent {
                    Ok(None)
                } else {
                    Err(no_boolean_result())
                }
            }
        }
    }
}

fn no_boolean_result() -> ExecError {
    ExecError::FunctionError {
        sqlstate: "2203A",
        message: "single boolean result is expected".into(),
    }
}

/// A structural error is one `silent => true` swallows: `PostgreSQL` silences
/// the `strict`-mode structural checks and the numeric domain errors, but not a
/// missing variable.
fn is_structural(e: &PathError) -> bool {
    e.sqlstate != "42704"
}

// ---- evaluation ----

struct Exec<'a> {
    strict: bool,
    vars: Option<&'a JsonbValue>,
    root: &'a JsonbValue,
    /// The array length `last` resolves against.
    ///
    /// The evaluator sets this while it evaluates a subscript.
    last: Option<usize>,
}

impl Exec<'_> {
    fn with_last(&self, last: Option<usize>) -> Exec<'_> {
        Exec {
            strict: self.strict,
            vars: self.vars,
            root: self.root,
            last,
        }
    }

    fn eval(&self, node: &Node, current: &JsonbValue) -> PathResult<Vec<JsonbValue>> {
        match node {
            Node::Root => Ok(vec![self.root.clone()]),
            Node::Current => Ok(vec![current.clone()]),
            Node::Literal(v) => Ok(vec![v.clone()]),
            Node::Last => {
                let Some(last) = self.last else {
                    return Err(PathError::new(
                        "42601",
                        "LAST is allowed only in array subscripts",
                    ));
                };
                Ok(vec![JsonbValue::Number(BigDecimal::from(
                    i64::try_from(last).unwrap_or(i64::MAX) - 1,
                ))])
            }
            Node::Var(name) => {
                let value = self
                    .vars
                    .and_then(|v| v.object_get(name))
                    .ok_or_else(|| {
                        PathError::new(
                            "42704",
                            format!("could not find jsonpath variable \"{name}\""),
                        )
                    })?
                    .clone();
                Ok(vec![value])
            }
            Node::Predicate(p) => Ok(vec![match self.eval_pred(p, current)? {
                Tri::True => JsonbValue::Bool(true),
                Tri::False => JsonbValue::Bool(false),
                Tri::Unknown => JsonbValue::Null,
            }]),
            Node::Neg { arg, negate } => {
                let items = self.eval(arg, current)?;
                let items = if self.strict {
                    items
                } else {
                    unwrap_arrays(items)
                };
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    let JsonbValue::Number(n) = item else {
                        return Err(PathError::new(
                            "22033",
                            "operand of unary jsonpath operator - is not a numeric value",
                        ));
                    };
                    out.push(JsonbValue::Number(if *negate { -n } else { n }));
                }
                Ok(out)
            }
            Node::Arith { op, left, right } => {
                let l = self.single_number(left, current, *op, "left")?;
                let r = self.single_number(right, current, *op, "right")?;
                Ok(vec![JsonbValue::Number(arith(*op, &l, &r)?)])
            }
            Node::Accessor { base, op } => {
                let items = self.eval(base, current)?;
                let mut out = Vec::new();
                for item in &items {
                    self.apply(op, item, current, &mut out)?;
                    if out.len() > MAX_ITEMS {
                        return Err(PathError::new(
                            "54000",
                            "jsonpath query produced too many items",
                        ));
                    }
                }
                Ok(out)
            }
        }
    }

    /// An arithmetic operand: exactly one numeric item, else `PostgreSQL`'s
    /// "is not a single numeric value" error.
    fn single_number(
        &self,
        node: &Node,
        current: &JsonbValue,
        op: ArithOp,
        side: &str,
    ) -> PathResult<BigDecimal> {
        let items = self.eval(node, current)?;
        let items = if self.strict {
            items
        } else {
            unwrap_arrays(items)
        };
        match items.as_slice() {
            [JsonbValue::Number(n)] => Ok(n.clone()),
            _ => Err(PathError::new(
                "22033",
                format!(
                    "{side} operand of jsonpath operator {} is not a single numeric value",
                    op.symbol()
                ),
            )),
        }
    }

    /// Apply one accessor to one item, and append the results to `out`.
    fn apply(
        &self,
        op: &Accessor,
        item: &JsonbValue,
        current: &JsonbValue,
        out: &mut Vec<JsonbValue>,
    ) -> PathResult<()> {
        match op {
            Accessor::Member(key) => self.member(item, key, out),
            Accessor::MemberAll => self.member_all(item, out),
            Accessor::Index(subs) => self.index(item, subs, current, out),
            Accessor::IndexAll => self.index_all(item, out),
            Accessor::Any { from, to } => {
                descend(item, 0, *from, *to, out);
                Ok(())
            }
            Accessor::Method(m) => self.method(*m, item, out),
            Accessor::Filter(pred) => {
                let candidates: Vec<&JsonbValue> = match item {
                    JsonbValue::Array(items) if !self.strict => items.iter().collect(),
                    other => vec![other],
                };
                for candidate in candidates {
                    // A *structural* error inside a filter is Unknown, never
                    // raised — that is what makes `$ ? (@.missing > 1)` a quiet
                    // no-match. A missing variable is not structural and still
                    // reaches the caller.
                    if self.eval_pred(pred, candidate)? == Tri::True {
                        out.push(candidate.clone());
                    }
                }
                Ok(())
            }
        }
    }

    fn member(&self, item: &JsonbValue, key: &str, out: &mut Vec<JsonbValue>) -> PathResult<()> {
        match item {
            JsonbValue::Object(_) => {
                if let Some(v) = item.object_get(key) {
                    out.push(v.clone());
                } else if self.strict {
                    return Err(PathError::new(
                        "2203A",
                        format!("JSON object does not contain key \"{key}\""),
                    ));
                }
                Ok(())
            }
            JsonbValue::Array(items) if !self.strict => {
                for elem in items {
                    // Auto-unwrapping is one level deep and never raises for a
                    // non-object element.
                    if let JsonbValue::Object(_) = elem
                        && let Some(v) = elem.object_get(key)
                    {
                        out.push(v.clone());
                    }
                }
                Ok(())
            }
            _ => {
                if self.strict {
                    Err(PathError::new(
                        "2203A",
                        "jsonpath member accessor can only be applied to an object",
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }

    fn member_all(&self, item: &JsonbValue, out: &mut Vec<JsonbValue>) -> PathResult<()> {
        match item {
            JsonbValue::Object(pairs) => {
                out.extend(pairs.iter().map(|(_, v)| v.clone()));
                Ok(())
            }
            JsonbValue::Array(items) if !self.strict => {
                for elem in items {
                    if let JsonbValue::Object(pairs) = elem {
                        out.extend(pairs.iter().map(|(_, v)| v.clone()));
                    }
                }
                Ok(())
            }
            _ => {
                if self.strict {
                    Err(PathError::new(
                        "2203A",
                        "jsonpath wildcard member accessor can only be applied to an object",
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }

    fn index(
        &self,
        item: &JsonbValue,
        subs: &[(Node, Option<Node>)],
        current: &JsonbValue,
        out: &mut Vec<JsonbValue>,
    ) -> PathResult<()> {
        let wrapped;
        let items: &[JsonbValue] = match item {
            JsonbValue::Array(items) => items,
            other if !self.strict => {
                wrapped = [other.clone()];
                &wrapped
            }
            _ => {
                return Err(PathError::new(
                    "22039",
                    "jsonpath array accessor can only be applied to an array",
                ));
            }
        };
        let inner = self.with_last(Some(items.len()));
        for (lo, hi) in subs {
            let from = inner.subscript(lo, current)?;
            let to = match hi {
                Some(hi) => inner.subscript(hi, current)?,
                None => from,
            };
            if self.strict
                && (from < 0
                    || to < 0
                    || from >= i64::try_from(items.len()).unwrap_or(i64::MAX)
                    || to >= i64::try_from(items.len()).unwrap_or(i64::MAX))
            {
                return Err(PathError::new(
                    "22033",
                    "jsonpath array subscript is out of bounds",
                ));
            }
            let mut i = from.max(0);
            while i <= to {
                let Ok(idx) = usize::try_from(i) else {
                    break;
                };
                let Some(v) = items.get(idx) else {
                    break;
                };
                out.push(v.clone());
                i += 1;
            }
        }
        Ok(())
    }

    /// One subscript expression, which must yield exactly one integral number.
    fn subscript(&self, node: &Node, current: &JsonbValue) -> PathResult<i64> {
        let items = self.eval(node, current)?;
        match items.as_slice() {
            [JsonbValue::Number(n)] => n.to_i64().ok_or_else(|| {
                PathError::new("22033", "jsonpath array subscript is out of integer range")
            }),
            _ => Err(PathError::new(
                "22033",
                "jsonpath array subscript is not a single numeric value",
            )),
        }
    }

    fn index_all(&self, item: &JsonbValue, out: &mut Vec<JsonbValue>) -> PathResult<()> {
        match item {
            JsonbValue::Array(items) => {
                out.extend(items.iter().cloned());
                Ok(())
            }
            other => {
                if self.strict {
                    Err(PathError::new(
                        "22039",
                        "jsonpath wildcard array accessor can only be applied to an array",
                    ))
                } else {
                    out.push(other.clone());
                    Ok(())
                }
            }
        }
    }

    /// An item method.
    ///
    /// `.type()`, `.size()` and `.keyvalue()` inspect the item as a whole. The
    /// numeric and string methods auto-unwrap an array in lax mode and apply
    /// element-wise.
    fn method(&self, m: Method, item: &JsonbValue, out: &mut Vec<JsonbValue>) -> PathResult<()> {
        match m {
            Method::Type => {
                out.push(JsonbValue::String(item.type_name().to_string()));
                Ok(())
            }
            Method::Size => match item {
                JsonbValue::Array(items) => {
                    out.push(JsonbValue::Number(BigDecimal::from(
                        i64::try_from(items.len()).unwrap_or(i64::MAX),
                    )));
                    Ok(())
                }
                _ if self.strict => Err(PathError::new(
                    "22039",
                    "jsonpath item method .size() can only be applied to an array",
                )),
                _ => {
                    out.push(JsonbValue::Number(BigDecimal::one()));
                    Ok(())
                }
            },
            Method::KeyValue => {
                let objects: Vec<&JsonbValue> = match item {
                    JsonbValue::Object(_) => vec![item],
                    JsonbValue::Array(items) if !self.strict => items.iter().collect(),
                    _ => {
                        return Err(PathError::new(
                            "2203C",
                            "jsonpath item method .keyvalue() can only be applied to an object",
                        ));
                    }
                };
                for object in objects {
                    let JsonbValue::Object(pairs) = object else {
                        return Err(PathError::new(
                            "2203C",
                            "jsonpath item method .keyvalue() can only be applied to an object",
                        ));
                    };
                    for (key, value) in pairs {
                        out.push(JsonbValue::object_from_pairs(vec![
                            ("id".into(), JsonbValue::Number(BigDecimal::zero())),
                            ("key".into(), JsonbValue::String(key.clone())),
                            ("value".into(), value.clone()),
                        ]));
                    }
                }
                Ok(())
            }
            _ => {
                let targets: Vec<&JsonbValue> = match item {
                    JsonbValue::Array(items) if !self.strict => items.iter().collect(),
                    other => vec![other],
                };
                for target in targets {
                    out.push(scalar_method(m, target)?);
                }
                Ok(())
            }
        }
    }

    fn eval_pred(&self, pred: &Pred, current: &JsonbValue) -> PathResult<Tri> {
        Ok(match pred {
            Pred::And(a, b) => match (self.eval_pred(a, current)?, self.eval_pred(b, current)?) {
                (Tri::False, _) | (_, Tri::False) => Tri::False,
                (Tri::True, Tri::True) => Tri::True,
                _ => Tri::Unknown,
            },
            Pred::Or(a, b) => match (self.eval_pred(a, current)?, self.eval_pred(b, current)?) {
                (Tri::True, _) | (_, Tri::True) => Tri::True,
                (Tri::False, Tri::False) => Tri::False,
                _ => Tri::Unknown,
            },
            Pred::Not(a) => self.eval_pred(a, current)?.not(),
            Pred::IsUnknown(a) => Tri::of(self.eval_pred(a, current)? == Tri::Unknown),
            Pred::Exists(node) => match self.eval(node, current) {
                Ok(items) => Tri::of(!items.is_empty()),
                Err(e) if is_structural(&e) => Tri::Unknown,
                Err(e) => return Err(e),
            },
            Pred::Compare { op, left, right } => {
                let (Some(ls), Some(rs)) = (
                    self.pred_operand(left, current)?,
                    self.pred_operand(right, current)?,
                ) else {
                    return Ok(Tri::Unknown);
                };
                let mut saw_unknown = false;
                for l in &ls {
                    for r in &rs {
                        match compare(*op, l, r) {
                            Tri::True => return Ok(Tri::True),
                            Tri::Unknown => saw_unknown = true,
                            Tri::False => {}
                        }
                    }
                }
                if saw_unknown {
                    Tri::Unknown
                } else {
                    Tri::False
                }
            }
            Pred::StartsWith { value, prefix } => {
                let (Some(vs), Some(ps)) = (
                    self.pred_operand(value, current)?,
                    self.pred_operand(prefix, current)?,
                ) else {
                    return Ok(Tri::Unknown);
                };
                let mut saw_unknown = false;
                for v in &vs {
                    for p in &ps {
                        match (v, p) {
                            (JsonbValue::String(v), JsonbValue::String(p)) => {
                                if v.starts_with(p.as_str()) {
                                    return Ok(Tri::True);
                                }
                            }
                            _ => saw_unknown = true,
                        }
                    }
                }
                if saw_unknown {
                    Tri::Unknown
                } else {
                    Tri::False
                }
            }
            Pred::LikeRegex {
                value,
                pattern,
                flags,
            } => {
                let Some(vs) = self.pred_operand(value, current)? else {
                    return Ok(Tri::Unknown);
                };
                let re = compile_regex(pattern, flags)?;
                let mut saw_unknown = false;
                for v in &vs {
                    match v {
                        JsonbValue::String(s) => {
                            if re.is_match(s) {
                                return Ok(Tri::True);
                            }
                        }
                        _ => saw_unknown = true,
                    }
                }
                if saw_unknown {
                    Tri::Unknown
                } else {
                    Tri::False
                }
            }
        })
    }

    /// A predicate operand's item sequence.
    ///
    /// A structural error makes the whole predicate Unknown (`None`) and raises
    /// nothing.
    fn pred_operand(
        &self,
        node: &Node,
        current: &JsonbValue,
    ) -> PathResult<Option<Vec<JsonbValue>>> {
        match self.eval(node, current) {
            Ok(items) => Ok(Some(if self.strict {
                items
            } else {
                unwrap_arrays(items)
            })),
            Err(e) if is_structural(&e) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// `PostgreSQL`'s lax-mode operand unwrapping: an array operand contributes its
/// elements, everything else contributes itself.
fn unwrap_arrays(items: Vec<JsonbValue>) -> Vec<JsonbValue> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            JsonbValue::Array(elems) => out.extend(elems),
            other => out.push(other),
        }
    }
    out
}

/// `.**{from to to}`: every descendant at a depth within the window, and the
/// item itself counts as depth 0.
///
/// Pre-order, which matches `PostgreSQL`'s output order.
fn descend(item: &JsonbValue, depth: u32, from: u32, to: u32, out: &mut Vec<JsonbValue>) {
    if depth >= from && depth <= to {
        out.push(item.clone());
    }
    if depth >= to || out.len() > MAX_ITEMS {
        return;
    }
    match item {
        JsonbValue::Array(items) => {
            for elem in items {
                descend(elem, depth + 1, from, to, out);
            }
        }
        JsonbValue::Object(pairs) => {
            for (_, value) in pairs {
                descend(value, depth + 1, from, to, out);
            }
        }
        _ => {}
    }
}

/// `PostgreSQL`'s `compareItems`: values of different types never compare equal
/// (except against JSON `null`), and arrays/objects are not comparable at all.
fn compare(op: CmpOp, left: &JsonbValue, right: &JsonbValue) -> Tri {
    use std::cmp::Ordering;

    let ord: Ordering = match (left, right) {
        (JsonbValue::Null, JsonbValue::Null) => Ordering::Equal,
        // Comparing null to a non-null is false for every operator but `!=`.
        (JsonbValue::Null, _) | (_, JsonbValue::Null) => {
            return Tri::of(op == CmpOp::Ne);
        }
        (JsonbValue::Bool(a), JsonbValue::Bool(b)) => a.cmp(b),
        (JsonbValue::Number(a), JsonbValue::Number(b)) => a.cmp(b),
        (JsonbValue::String(a), JsonbValue::String(b)) => a.as_bytes().cmp(b.as_bytes()),
        // Non-null items of different types, and containers, are not comparable.
        _ => return Tri::Unknown,
    };
    Tri::of(match op {
        CmpOp::Eq => ord == Ordering::Equal,
        CmpOp::Ne => ord != Ordering::Equal,
        CmpOp::Lt => ord == Ordering::Less,
        CmpOp::Le => ord != Ordering::Greater,
        CmpOp::Gt => ord == Ordering::Greater,
        CmpOp::Ge => ord != Ordering::Less,
    })
}

/// jsonpath arithmetic is `numeric` arithmetic, so it goes through the same
/// operators SQL `+`/`-`/`*`/`/`/`%` use.
///
/// This is why `$.a / 2` over `3` produces `1.5000000000000000` and not
/// `1.5`.
fn arith(op: ArithOp, l: &BigDecimal, r: &BigDecimal) -> PathResult<BigDecimal> {
    use crabka_pgtypes::numeric::{self, NumericValue};

    let (a, b) = (
        NumericValue::Finite(l.clone()),
        NumericValue::Finite(r.clone()),
    );
    let result = match op {
        ArithOp::Add => numeric::add(&a, &b),
        ArithOp::Sub => numeric::sub(&a, &b),
        ArithOp::Mul => numeric::mul(&a, &b),
        ArithOp::Div => {
            numeric::div(&a, &b).map_err(|_| PathError::new("22012", "division by zero"))?
        }
        ArithOp::Mod => {
            numeric::rem(&a, &b).map_err(|_| PathError::new("22012", "division by zero"))?
        }
    };
    match result {
        NumericValue::Finite(value) => Ok(value),
        // Neither operand can be a special: a JSON number is always finite.
        _ => Err(PathError::new(
            "22033",
            "jsonpath arithmetic produced a non-finite value",
        )),
    }
}

/// The per-item numeric/string conversion methods.
fn scalar_method(m: Method, item: &JsonbValue) -> PathResult<JsonbValue> {
    let name = method_name(m);
    match m {
        Method::Abs | Method::Ceiling | Method::Floor => {
            let JsonbValue::Number(n) = item else {
                return Err(PathError::new(
                    "22036",
                    format!("jsonpath item method {name} can only be applied to a numeric value"),
                ));
            };
            Ok(JsonbValue::Number(match m {
                Method::Abs => n.abs(),
                Method::Ceiling => ceiling(n),
                _ => floor(n),
            }))
        }
        Method::Double => {
            let text = numeric_source(item, name)?;
            let value: f64 = text
                .trim()
                .parse()
                .map_err(|_| invalid_for(name, &text, "double precision"))?;
            if !value.is_finite() {
                return Err(invalid_for(name, &text, "double precision"));
            }
            // `float8out` first, then back to numeric, so `.double()` reproduces
            // PostgreSQL's shortest-round-trip rendering (`'1.5'` → `1.5`).
            let rendered = String::from_utf8(crabka_pgtypes::encoding::encode_text(
                &crabka_pgtypes::Datum::Float8(value),
                &jiff::tz::TimeZone::UTC,
            ))
            .map_err(|_| invalid_for(name, &text, "double precision"))?;
            let decimal = rendered
                .parse::<BigDecimal>()
                .map_err(|_| invalid_for(name, &text, "double precision"))?;
            Ok(JsonbValue::Number(decimal))
        }
        Method::Bigint | Method::Integer => {
            let text = numeric_source(item, name)?;
            let target = if m == Method::Bigint {
                "bigint"
            } else {
                "integer"
            };
            let decimal = text
                .parse::<BigDecimal>()
                .map_err(|_| invalid_for(name, &text, target))?;
            let rounded = decimal.round(0);
            let value = rounded
                .to_i64()
                .ok_or_else(|| invalid_for(name, &text, target))?;
            if m == Method::Integer && i32::try_from(value).is_err() {
                return Err(invalid_for(name, &text, target));
            }
            Ok(JsonbValue::Number(BigDecimal::from(value)))
        }
        Method::Number | Method::Decimal => {
            let text = numeric_source(item, name)?;
            let decimal = text
                .parse::<BigDecimal>()
                .map_err(|_| invalid_for(name, &text, "numeric"))?;
            Ok(JsonbValue::Number(decimal))
        }
        Method::Boolean => match item {
            JsonbValue::Bool(b) => Ok(JsonbValue::Bool(*b)),
            JsonbValue::Number(n) => Ok(JsonbValue::Bool(!n.is_zero())),
            JsonbValue::String(s) => match s.to_ascii_lowercase().as_str() {
                "true" | "t" | "yes" | "y" | "on" | "1" => Ok(JsonbValue::Bool(true)),
                "false" | "f" | "no" | "n" | "off" | "0" => Ok(JsonbValue::Bool(false)),
                _ => Err(invalid_for(name, s, "boolean")),
            },
            _ => Err(PathError::new(
                "22036",
                format!(
                    "jsonpath item method {name} can only be applied to a boolean, string, or numeric value"
                ),
            )),
        },
        Method::String => Ok(JsonbValue::String(match item {
            JsonbValue::String(s) => s.clone(),
            JsonbValue::Number(n) => crabka_pgtypes::numeric::finite_to_text(n),
            JsonbValue::Bool(true) => "true".into(),
            JsonbValue::Bool(false) => "false".into(),
            _ => {
                return Err(PathError::new(
                    "22036",
                    format!(
                        "jsonpath item method {name} can only be applied to a boolean, string, numeric, or datetime value"
                    ),
                ));
            }
        })),
        Method::Date
        | Method::Time
        | Method::TimeTz
        | Method::Timestamp
        | Method::TimestampTz
        | Method::Datetime => datetime_method(m, item),
        // `type`, `size` and `keyvalue` never reach here.
        Method::Type | Method::Size | Method::KeyValue => Ok(item.clone()),
    }
}

fn method_name(m: Method) -> &'static str {
    match m {
        Method::Type => ".type()",
        Method::Size => ".size()",
        Method::Double => ".double()",
        Method::Ceiling => ".ceiling()",
        Method::Floor => ".floor()",
        Method::Abs => ".abs()",
        Method::KeyValue => ".keyvalue()",
        Method::Bigint => ".bigint()",
        Method::Boolean => ".boolean()",
        Method::Decimal => ".decimal()",
        Method::Integer => ".integer()",
        Method::Number => ".number()",
        Method::String => ".string()",
        Method::Date => ".date()",
        Method::Time => ".time()",
        Method::TimeTz => ".time_tz()",
        Method::Timestamp => ".timestamp()",
        Method::TimestampTz => ".timestamp_tz()",
        Method::Datetime => ".datetime()",
    }
}

/// The text a numeric conversion method reads: a JSON string is converted, a
/// JSON number is already numeric, anything else is a type error.
fn numeric_source(item: &JsonbValue, name: &'static str) -> PathResult<String> {
    match item {
        JsonbValue::String(s) => Ok(s.clone()),
        JsonbValue::Number(n) => Ok(crabka_pgtypes::numeric::finite_to_text(n)),
        _ => Err(PathError::new(
            "22036",
            format!("jsonpath item method {name} can only be applied to a string or numeric value"),
        )),
    }
}

fn invalid_for(name: &'static str, text: &str, target: &str) -> PathError {
    PathError::new(
        "22036",
        format!("argument \"{text}\" of jsonpath item method {name} is invalid for type {target}"),
    )
}

/// The date/time methods, rendered back as JSON strings in `PostgreSQL`'s
/// canonical ISO-8601 output.
///
/// See the module divergence list: crabka has no jsonpath datetime item type,
/// so these results compare as strings.
fn datetime_method(m: Method, item: &JsonbValue) -> PathResult<JsonbValue> {
    use crabka_pgtypes::{ColumnType, Datum};

    let name = method_name(m);
    let JsonbValue::String(text) = item else {
        return Err(PathError::new(
            "22031",
            format!("jsonpath item method {name} can only be applied to a string"),
        ));
    };
    // `.datetime()` with no template tries each type in PostgreSQL's order and
    // keeps the first that parses.
    let targets: &[ColumnType] = match m {
        Method::Date => &[ColumnType::Date],
        Method::Time => &[ColumnType::Time],
        Method::TimeTz => &[ColumnType::Timetz],
        Method::Timestamp => &[ColumnType::Timestamp],
        Method::TimestampTz => &[ColumnType::Timestamptz],
        _ => &[
            ColumnType::Date,
            ColumnType::Timetz,
            ColumnType::Time,
            ColumnType::Timestamptz,
            ColumnType::Timestamp,
        ],
    };
    let tz = jiff::tz::TimeZone::UTC;
    let source = Datum::Text(text.clone());
    let parsed = targets
        .iter()
        .find_map(|target| crabka_pgtypes::cast::cast(&source, *target, &tz).ok())
        .ok_or_else(|| {
            PathError::new(
                "22007",
                format!("{name} format is not recognized: \"{text}\""),
            )
        })?;
    let mut rendered = String::from_utf8(crabka_pgtypes::encoding::encode_text(&parsed, &tz))
        .map_err(|_| PathError::new("22007", format!("{name} produced invalid text")))?;
    // A jsonpath datetime renders ISO-8601 with a `T` separator, unlike SQL's
    // space-separated `timestamp` output.
    if let Some(space) = rendered.find(' ') {
        rendered.replace_range(space..=space, "T");
    }
    Ok(JsonbValue::String(rendered))
}

fn ceiling(n: &BigDecimal) -> BigDecimal {
    let truncated = n.with_scale(0);
    if &truncated < n {
        truncated + BigDecimal::one()
    } else {
        truncated
    }
}

fn floor(n: &BigDecimal) -> BigDecimal {
    let truncated = n.with_scale(0);
    if &truncated > n {
        truncated - BigDecimal::one()
    } else {
        truncated
    }
}

/// `like_regex` uses POSIX-ish XQuery regexes with `i`, `s`, `m`, `x` and `q`
/// flags. crabka compiles them with the `regex` crate, exactly as the `~`
/// operator does.
fn compile_regex(pattern: &str, flags: &str) -> PathResult<regex::Regex> {
    let mut builder = String::new();
    let mut literal = false;
    for flag in flags.chars() {
        match flag {
            'i' => builder.push('i'),
            's' => builder.push('s'),
            'm' => builder.push('m'),
            'x' => builder.push('x'),
            'q' => literal = true,
            other => {
                return Err(PathError::new(
                    "22025",
                    format!(
                        "invalid input syntax for type jsonpath: unrecognized flag character \"{other}\" in LIKE_REGEX predicate"
                    ),
                ));
            }
        }
    }
    let body = if literal {
        regex::escape(pattern)
    } else {
        pattern.to_string()
    };
    let source = if builder.is_empty() {
        body
    } else {
        format!("(?{builder}){body}")
    };
    regex::Regex::new(&source).map_err(|e| {
        PathError::new(
            "2201B",
            format!("invalid regular expression: {}", first_line(&e.to_string())),
        )
    })
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or_default().trim().to_string()
}

/// Render a compiled path back to its canonical `PostgreSQL` text, the output
/// form of the `jsonpath` type.
impl std::fmt::Display for JsonPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.strict {
            f.write_str("strict ")?;
        }
        let mut out = String::new();
        write_node(&self.root, &mut out);
        f.write_str(&out)
    }
}

fn write_node(node: &Node, out: &mut String) {
    match node {
        Node::Root => out.push('$'),
        Node::Current => out.push('@'),
        Node::Last => out.push_str("last"),
        Node::Var(name) => {
            out.push('$');
            out.push_str(name);
        }
        Node::Literal(v) => out.push_str(&v.to_text()),
        Node::Neg { arg, negate } => {
            out.push(if *negate { '-' } else { '+' });
            write_node(arg, out);
        }
        Node::Arith { op, left, right } => {
            out.push('(');
            write_node(left, out);
            let _ = write!(out, " {} ", op.symbol());
            write_node(right, out);
            out.push(')');
        }
        Node::Predicate(p) => write_pred(p, out),
        Node::Accessor { base, op } => {
            write_node(base, out);
            match op {
                Accessor::Member(key) => {
                    let _ = write!(out, ".\"{key}\"");
                }
                Accessor::MemberAll => out.push_str(".*"),
                Accessor::IndexAll => out.push_str("[*]"),
                Accessor::Index(subs) => {
                    out.push('[');
                    for (i, (lo, hi)) in subs.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        write_node(lo, out);
                        if let Some(hi) = hi {
                            out.push_str(" to ");
                            write_node(hi, out);
                        }
                    }
                    out.push(']');
                }
                Accessor::Any { from, to } => {
                    out.push_str(".**");
                    if !(*from == 0 && *to == u32::MAX) {
                        if from == to {
                            let _ = write!(out, "{{{from}}}");
                        } else {
                            let _ = write!(out, "{{{from} to {to}}}");
                        }
                    }
                }
                Accessor::Method(m) => out.push_str(method_name(*m)),
                Accessor::Filter(p) => {
                    out.push_str("?(");
                    write_pred(p, out);
                    out.push(')');
                }
            }
        }
    }
}

fn write_pred(pred: &Pred, out: &mut String) {
    match pred {
        Pred::And(a, b) => {
            out.push('(');
            write_pred(a, out);
            out.push_str(" && ");
            write_pred(b, out);
            out.push(')');
        }
        Pred::Or(a, b) => {
            out.push('(');
            write_pred(a, out);
            out.push_str(" || ");
            write_pred(b, out);
            out.push(')');
        }
        Pred::Not(a) => {
            out.push_str("!(");
            write_pred(a, out);
            out.push(')');
        }
        Pred::IsUnknown(a) => {
            out.push('(');
            write_pred(a, out);
            out.push_str(") is unknown");
        }
        Pred::Exists(node) => {
            out.push_str("exists (");
            write_node(node, out);
            out.push(')');
        }
        Pred::Compare { op, left, right } => {
            out.push('(');
            write_node(left, out);
            let _ = write!(
                out,
                " {} ",
                match op {
                    CmpOp::Eq => "==",
                    CmpOp::Ne => "!=",
                    CmpOp::Lt => "<",
                    CmpOp::Le => "<=",
                    CmpOp::Gt => ">",
                    CmpOp::Ge => ">=",
                }
            );
            write_node(right, out);
            out.push(')');
        }
        Pred::StartsWith { value, prefix } => {
            out.push('(');
            write_node(value, out);
            out.push_str(" starts with ");
            write_node(prefix, out);
            out.push(')');
        }
        Pred::LikeRegex {
            value,
            pattern,
            flags,
        } => {
            out.push('(');
            write_node(value, out);
            let _ = write!(out, " like_regex \"{pattern}\"");
            if !flags.is_empty() {
                let _ = write!(out, " flag \"{flags}\"");
            }
            out.push(')');
        }
    }
}

/// The `vars` argument must be a jsonb *object*; anything else is 22023.
pub fn check_vars(vars: &JsonbValue) -> Result<(), ExecError> {
    if matches!(vars, JsonbValue::Object(_)) {
        Ok(())
    } else {
        Err(ExecError::FunctionError {
            sqlstate: "22023",
            message: "\"vars\" argument is not an object".into(),
        })
    }
}

#[cfg(test)]
mod tests;
