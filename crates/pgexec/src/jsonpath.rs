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

use bigdecimal::{BigDecimal, One, RoundingMode, ToPrimitive, Zero};
use crabka_pgtypes::{ArrayValue, Datum, ElemType, JsonbValue, TypeError};
use jiff::ToSpan;

use crate::error::ExecError;

/// The maximum accessor-chain nesting the parser accepts, so an adversarial
/// path cannot overflow the recursive-descent parser's stack.
const MAX_DEPTH: u32 = 128;

/// The maximum number of items one path evaluation may produce.
///
/// `PostgreSQL` has no such cap. Without it, `.**` over a deeply nested
/// document is unbounded work inside a single statement.
const MAX_ITEMS: usize = 1_000_000;

/// Run the `jsonpath` input function and keep its canonical text representation.
pub(crate) fn canonical_datum(src: &str) -> Result<Datum, ExecError> {
    if src.is_empty() {
        return Err(TypeError::InvalidText {
            type_name: "jsonpath",
            value: String::new(),
        }
        .into());
    }
    Ok(Datum::JsonPath(JsonPath::parse(src)?.to_string()))
}

pub(crate) fn cast_datum(value: &Datum) -> Result<Datum, ExecError> {
    match value {
        Datum::Null => Ok(Datum::Null),
        Datum::Text(text) => canonical_datum(text),
        Datum::JsonPath(text) => Ok(Datum::JsonPath(text.clone())),
        other => Err(TypeError::CannotCast {
            from: other
                .column_type()
                .map_or("unknown", crabka_pgtypes::ColumnType::name),
            to: "jsonpath",
        }
        .into()),
    }
}

/// Run `jsonpath_in` over each non-NULL element while preserving dimensions.
pub(crate) fn cast_array_datum(value: &Datum) -> Result<Datum, ExecError> {
    match value {
        Datum::Null => Ok(Datum::Null),
        Datum::Text(text) => {
            let literal = crabka_pgtypes::array::parse_literal(text)?;
            let elems = literal
                .elements
                .into_iter()
                .map(|elem| match elem {
                    None => Ok(Datum::Null),
                    Some(text) => canonical_datum(&text),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Datum::Array(ArrayValue::with_dims(
                ElemType::JsonPath,
                elems,
                literal.dims,
            )))
        }
        Datum::Array(array) => {
            let elems = array
                .elems
                .iter()
                .map(cast_datum)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Datum::Array(ArrayValue::with_dims(
                ElemType::JsonPath,
                elems,
                array.dims.clone(),
            )))
        }
        other => Err(TypeError::CannotCast {
            from: other
                .column_type()
                .map_or("unknown", crabka_pgtypes::ColumnType::name),
            to: "jsonpath[]",
        }
        .into()),
    }
}

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
    Any {
        from: DepthBound,
        to: DepthBound,
        explicit_bounds: bool,
    },
    /// `.type()`, `.size()`, …: an item method, with `.datetime`'s optional
    /// format template.
    Method(Method, MethodArgs),
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

/// The only item-method arguments PostgreSQL accepts.
#[derive(Debug, Clone, PartialEq, Eq)]
enum MethodArgs {
    None,
    Datetime(String),
    Decimal { precision: i32, scale: Option<i32> },
    TemporalPrecision(BigDecimal),
}

/// A recursive-descent bound. `last` behaves as an unbounded depth while
/// retaining PostgreSQL's canonical spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DepthBound {
    Number(u32),
    Last,
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

fn collect_precision_warnings_node(node: &Node, warnings: &mut Vec<String>) {
    match node {
        Node::Accessor { base, op } => {
            collect_precision_warnings_node(base, warnings);
            match op {
                Accessor::Index(items) => {
                    for (lower, upper) in items {
                        collect_precision_warnings_node(lower, warnings);
                        if let Some(upper) = upper {
                            collect_precision_warnings_node(upper, warnings);
                        }
                    }
                }
                Accessor::Method(method, MethodArgs::TemporalPrecision(precision))
                    if precision
                        .to_i32()
                        .is_some_and(|precision| (7..=i32::MAX).contains(&precision)) =>
                {
                    let precision = precision.to_i32().expect("guard admits an int4");
                    let type_name = match method {
                        Method::Time => format!("TIME({precision})"),
                        Method::TimeTz => format!("TIME({precision}) WITH TIME ZONE"),
                        Method::Timestamp => format!("TIMESTAMP({precision})"),
                        Method::TimestampTz => {
                            format!("TIMESTAMP({precision}) WITH TIME ZONE")
                        }
                        _ => unreachable!("only temporal methods have temporal precision"),
                    };
                    warnings.push(format!(
                        "{type_name} precision reduced to maximum allowed, 6"
                    ));
                }
                Accessor::Filter(predicate) => {
                    collect_precision_warnings_pred(predicate, warnings);
                }
                _ => {}
            }
        }
        Node::Neg { arg, .. } => collect_precision_warnings_node(arg, warnings),
        Node::Arith { left, right, .. } => {
            collect_precision_warnings_node(left, warnings);
            collect_precision_warnings_node(right, warnings);
        }
        Node::Predicate(predicate) => collect_precision_warnings_pred(predicate, warnings),
        Node::Root | Node::Current | Node::Last | Node::Var(_) | Node::Literal(_) => {}
    }
}

fn collect_precision_warnings_pred(predicate: &Pred, warnings: &mut Vec<String>) {
    match predicate {
        Pred::And(left, right) | Pred::Or(left, right) => {
            collect_precision_warnings_pred(left, warnings);
            collect_precision_warnings_pred(right, warnings);
        }
        Pred::Not(inner) | Pred::IsUnknown(inner) => {
            collect_precision_warnings_pred(inner, warnings);
        }
        Pred::Exists(node) => collect_precision_warnings_node(node, warnings),
        Pred::Compare { left, right, .. }
        | Pred::StartsWith {
            value: left,
            prefix: right,
        } => {
            collect_precision_warnings_node(left, warnings);
            collect_precision_warnings_node(right, warnings);
        }
        Pred::LikeRegex { value, .. } => collect_precision_warnings_node(value, warnings),
    }
}

fn validate_context_node(
    node: &Node,
    in_subscript: bool,
    in_filter: bool,
) -> Result<(), ExecError> {
    match node {
        Node::Last if !in_subscript => Err(syntax("LAST is allowed only in array subscripts")),
        Node::Current if !in_filter => Err(syntax("@ is not allowed in root expressions")),
        Node::Accessor { base, op } => {
            validate_context_node(base, in_subscript, in_filter)?;
            match op {
                Accessor::Index(items) => {
                    for (lower, upper) in items {
                        validate_context_node(lower, true, in_filter)?;
                        if let Some(upper) = upper {
                            validate_context_node(upper, true, in_filter)?;
                        }
                    }
                    Ok(())
                }
                Accessor::Filter(predicate) => validate_context_pred(predicate, in_subscript, true),
                Accessor::Member(_)
                | Accessor::MemberAll
                | Accessor::IndexAll
                | Accessor::Any { .. }
                | Accessor::Method(..) => Ok(()),
            }
        }
        Node::Neg { arg, .. } => validate_context_node(arg, in_subscript, in_filter),
        Node::Arith { left, right, .. } => {
            validate_context_node(left, in_subscript, in_filter)?;
            validate_context_node(right, in_subscript, in_filter)
        }
        Node::Predicate(predicate) => validate_context_pred(predicate, in_subscript, in_filter),
        Node::Root | Node::Current | Node::Last | Node::Var(_) | Node::Literal(_) => Ok(()),
    }
}

fn validate_context_pred(
    predicate: &Pred,
    in_subscript: bool,
    in_filter: bool,
) -> Result<(), ExecError> {
    match predicate {
        Pred::And(left, right) | Pred::Or(left, right) => {
            validate_context_pred(left, in_subscript, in_filter)?;
            validate_context_pred(right, in_subscript, in_filter)
        }
        Pred::Not(inner) | Pred::IsUnknown(inner) => {
            validate_context_pred(inner, in_subscript, in_filter)
        }
        Pred::Exists(node) => validate_context_node(node, in_subscript, in_filter),
        Pred::Compare { left, right, .. }
        | Pred::StartsWith {
            value: left,
            prefix: right,
        } => {
            validate_context_node(left, in_subscript, in_filter)?;
            validate_context_node(right, in_subscript, in_filter)
        }
        Pred::LikeRegex { value, .. } => validate_context_node(value, in_subscript, in_filter),
    }
}

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
            ('_', Some(d)) if d.is_ascii_digit() => {
                return Err(syntax("syntax error at end of jsonpath input"));
            }
            (w, _) if is_ident_start(w) => {
                let (word, len) = lex_word(&chars, i)?;
                out.push(Tok::Word(word));
                i += len;
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

fn lex_word(chars: &[char], start: usize) -> Result<(String, usize), ExecError> {
    let mut end = start;
    while end < chars.len() {
        if is_ident_cont(chars[end]) {
            end += 1;
            continue;
        }
        if chars[end] != '\\' {
            break;
        }
        end += match chars.get(end + 1) {
            Some('x') => 4,
            Some('u') if chars.get(end + 2) == Some(&'{') => {
                let offset = chars[end + 3..]
                    .iter()
                    .position(|c| *c == '}')
                    .ok_or_else(|| syntax("invalid unicode escape of jsonpath input"))?;
                offset + 4
            }
            Some('u') => 6,
            Some(_) => 2,
            None => 1,
        };
    }
    let mut quoted = Vec::with_capacity(end - start + 2);
    quoted.push('"');
    quoted.extend_from_slice(&chars[start..end]);
    quoted.push('"');
    let (word, _) = lex_quoted(&quoted, 0)?;
    Ok((word, end - start))
}

fn is_ident_start(c: char) -> bool {
    c == '_' || c.is_alphabetic() || !c.is_ascii()
}

fn is_ident_cont(c: char) -> bool {
    is_ident_start(c) || c.is_ascii_digit() || c == '$'
}

/// A number literal: optional integer part, optional fraction, optional
/// exponent, PostgreSQL digit separators, or a PostgreSQL non-decimal integer.
fn lex_number(chars: &[char], start: usize) -> Result<(BigDecimal, usize), ExecError> {
    let mut j = start;
    let nondecimal = matches!(
        (chars.get(start), chars.get(start + 1)),
        (Some('0'), Some('x' | 'X' | 'o' | 'O' | 'b' | 'B'))
    );
    if nondecimal {
        let radix = match chars[start + 1].to_ascii_lowercase() {
            'x' => 16,
            'o' => 8,
            'b' => 2,
            _ => unreachable!("matched non-decimal prefix"),
        };
        j += 2;
        if chars.get(j) == Some(&'_') {
            return Err(syntax("syntax error at end of jsonpath input"));
        }
        while chars.get(j).is_some_and(|c| c.is_digit(radix) || *c == '_') {
            j += 1;
        }
    } else {
        while chars
            .get(j)
            .is_some_and(|c| c.is_ascii_digit() || *c == '_')
        {
            j += 1;
        }
        if chars.get(j) == Some(&'.') {
            j += 1;
            while chars
                .get(j)
                .is_some_and(|c| c.is_ascii_digit() || *c == '_')
            {
                j += 1;
            }
        }
        if matches!(chars.get(j), Some('e' | 'E')) {
            j += 1;
            if matches!(chars.get(j), Some('+' | '-')) {
                j += 1;
            }
            while chars
                .get(j)
                .is_some_and(|c| c.is_ascii_digit() || *c == '_')
            {
                j += 1;
            }
        }
    }
    let text: String = chars[start..j].iter().collect();
    if !nondecimal && text.contains('_') {
        if text.contains("__") {
            return Err(syntax("syntax error at end of jsonpath input"));
        }
        let prefix = text
            .find("_.")
            .map(|index| &text[..index + 1])
            .or_else(|| text.find("._").map(|index| &text[..index + 2]))
            .or_else(|| {
                text.find("e_")
                    .or_else(|| text.find("E_"))
                    .map(|index| &text[..index + 1])
            })
            .or_else(|| text.ends_with('_').then_some(text.as_str()));
        if let Some(prefix) = prefix {
            return Err(syntax(format!(
                "trailing junk after numeric literal at or near \"{prefix}\" of jsonpath input"
            )));
        }
    }
    if nondecimal && chars.get(j).is_some_and(|c| is_ident_start(*c)) {
        return Err(syntax("syntax error at end of jsonpath input"));
    }
    if !nondecimal && chars.get(j).is_some_and(|c| is_ident_start(*c)) {
        let end = if chars[j - 1] == '.' {
            j + 1
        } else {
            let mut end = j;
            while chars.get(end).is_some_and(|c| is_ident_cont(*c)) {
                end += 1;
            }
            end
        };
        let token: String = chars[start..end].iter().collect();
        return Err(syntax(format!(
            "trailing junk after numeric literal at or near \"{token}\" of jsonpath input"
        )));
    }
    if nondecimal && j == start + 2 {
        return Err(syntax(format!(
            "trailing junk after numeric literal at or near \"{text}\" of jsonpath input"
        )));
    }
    if !nondecimal
        && chars[start] == '0'
        && chars
            .get(start + 1)
            .is_some_and(|c| c.is_ascii_digit() || *c == '_')
    {
        if chars.get(start + 1) != Some(&'0') {
            return Err(syntax("syntax error at end of jsonpath input"));
        }
        return Err(syntax(format!(
            "trailing junk after numeric literal at or near \"{text}\" of jsonpath input"
        )));
    }
    let value = match crabka_pgtypes::numeric::parse(&text) {
        Some(crabka_pgtypes::numeric::NumericValue::Finite(value)) => value,
        _ if !nondecimal && !text.contains('_') => {
            return Err(syntax(format!(
                "trailing junk after numeric literal at or near \"{text}\" of jsonpath input"
            )));
        }
        _ => {
            return Err(syntax(format!(
                "syntax error at or near \"{text}\" of jsonpath input"
            )));
        }
    };
    Ok((value, j - start))
}

/// A `"…"` string, with PostgreSQL JSONPath escapes.
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
                    'v' => out.push('\u{b}'),
                    'x' => {
                        let hex: String = chars.get(j..j + 2).unwrap_or_default().iter().collect();
                        let code = u32::from_str_radix(&hex, 16)
                            .map_err(|_| syntax("invalid hexadecimal escape of jsonpath input"))?;
                        j += 2;
                        out.push(char::from_u32(code).ok_or_else(|| {
                            syntax("invalid hexadecimal escape of jsonpath input")
                        })?);
                    }
                    'u' => {
                        let hex: String;
                        if chars.get(j) == Some(&'{') {
                            let end = chars[j + 1..]
                                .iter()
                                .position(|c| *c == '}')
                                .map(|i| j + 1 + i)
                                .ok_or_else(|| {
                                    syntax("invalid unicode escape of jsonpath input")
                                })?;
                            if end == j + 1 || end > j + 7 {
                                return Err(syntax("invalid unicode escape of jsonpath input"));
                            }
                            hex = chars[j + 1..end].iter().collect();
                            j = end + 1;
                        } else {
                            hex = chars.get(j..j + 4).unwrap_or_default().iter().collect();
                            j += 4;
                        }
                        let code = u32::from_str_radix(&hex, 16)
                            .map_err(|_| syntax("invalid unicode escape of jsonpath input"))?;
                        if (0xD800..=0xDBFF).contains(&code) {
                            if chars.get(j) != Some(&'\\') || chars.get(j + 1) != Some(&'u') {
                                return Err(syntax(
                                    "Unicode low surrogate must follow a high surrogate",
                                ));
                            }
                            let low_hex: String =
                                chars.get(j + 2..j + 6).unwrap_or_default().iter().collect();
                            let low = u32::from_str_radix(&low_hex, 16).map_err(|_| {
                                syntax("Unicode low surrogate must follow a high surrogate")
                            })?;
                            if !(0xDC00..=0xDFFF).contains(&low) {
                                return Err(syntax(
                                    "Unicode low surrogate must follow a high surrogate",
                                ));
                            }
                            j += 6;
                            let scalar = 0x1_0000 + ((code - 0xD800) << 10) + low - 0xDC00;
                            out.push(char::from_u32(scalar).ok_or_else(|| {
                                syntax("invalid unicode escape of jsonpath input")
                            })?);
                        } else if (0xDC00..=0xDFFF).contains(&code) {
                            return Err(syntax("Unicode low surrogate without a high surrogate"));
                        } else {
                            out.push(char::from_u32(code).ok_or_else(|| {
                                syntax("invalid unicode escape of jsonpath input")
                            })?);
                        }
                    }
                    other => out.push(other),
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
            Either::Pred(p) | Either::GroupedPred(p) => (Node::Predicate(Box::new(p)), true),
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
            validate_like_regex(&pattern, &flags)?;
            flags = ['i', 's', 'm', 'x', 'q']
                .into_iter()
                .filter(|flag| flags.contains(*flag))
                .collect();
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
        let negate = if self.eat(&Tok::Minus) {
            true
        } else if self.eat(&Tok::Plus) {
            false
        } else {
            return self.accessor_expr();
        };
        let arg = self.unary()?.into_expr()?;
        let arg = match arg {
            Node::Literal(JsonbValue::Number(number)) => {
                Node::Literal(JsonbValue::Number(if negate { -number } else { number }))
            }
            arg => Node::Neg {
                arg: Box::new(arg),
                negate,
            },
        };
        Ok(Either::Expr(arg))
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
                    let from = self.depth_bound()?;
                    let to = if self.eat_word("to") {
                        self.depth_bound()?
                    } else {
                        from
                    };
                    self.expect(&Tok::RBrace)?;
                    return Ok(Accessor::Any {
                        from,
                        to,
                        explicit_bounds: true,
                    });
                }
                return Ok(Accessor::Any {
                    from: DepthBound::Number(0),
                    to: DepthBound::Last,
                    explicit_bounds: false,
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
                    let args = match self.peek() {
                        Tok::Str(template) if method == Method::Datetime => {
                            let template = template.clone();
                            self.bump();
                            MethodArgs::Datetime(template)
                        }
                        Tok::Num(_) | Tok::Plus | Tok::Minus if method == Method::Decimal => {
                            let precision = self.decimal_argument("precision")?;
                            let scale = self
                                .eat(&Tok::Comma)
                                .then(|| self.decimal_argument("scale"))
                                .transpose()?;
                            MethodArgs::Decimal { precision, scale }
                        }
                        Tok::Num(_)
                            if matches!(
                                method,
                                Method::Time
                                    | Method::TimeTz
                                    | Method::Timestamp
                                    | Method::TimestampTz
                            ) =>
                        {
                            MethodArgs::TemporalPrecision(self.temporal_precision_argument()?)
                        }
                        Tok::Minus
                            if matches!(
                                method,
                                Method::Time
                                    | Method::TimeTz
                                    | Method::Timestamp
                                    | Method::TimestampTz
                            ) =>
                        {
                            MethodArgs::TemporalPrecision(self.temporal_precision_argument()?)
                        }
                        Tok::RParen => MethodArgs::None,
                        _ => return Err(self.error_here()),
                    };
                    self.expect(&Tok::RParen)?;
                    return Ok(Accessor::Method(method, args));
                }
                Ok(Accessor::Member(w))
            }
            _ => {
                self.pos -= 1;
                Err(self.error_here())
            }
        }
    }

    fn depth_bound(&mut self) -> Result<DepthBound, ExecError> {
        match self.bump() {
            Tok::Num(n) => n
                .to_u32()
                .map(DepthBound::Number)
                .ok_or_else(|| syntax("invalid nesting level in jsonpath input")),
            Tok::Word(word) if word == "last" => Ok(DepthBound::Last),
            _ => {
                self.pos -= 1;
                Err(self.error_here())
            }
        }
    }

    fn decimal_argument(&mut self, name: &str) -> Result<i32, ExecError> {
        let sign = if self.eat(&Tok::Plus) {
            1
        } else if self.eat(&Tok::Minus) {
            -1
        } else {
            1
        };
        match self.bump() {
            Tok::Num(value) if value.fractional_digit_count() == 0 => value
                .to_i32()
                .and_then(|value| value.checked_mul(sign))
                .ok_or_else(|| ExecError::FunctionError {
                    sqlstate: "22031",
                    message: format!(
                        "{name} of jsonpath item method .decimal() is out of range for type integer"
                    ),
                }),
            _ => Err(self.error_here()),
        }
    }

    fn temporal_precision_argument(&mut self) -> Result<BigDecimal, ExecError> {
        let negative = self.eat(&Tok::Minus);
        match self.bump() {
            Tok::Num(value) if value.fractional_digit_count() == 0 => {
                if negative {
                    Ok(-value)
                } else {
                    Ok(value)
                }
            }
            _ => Err(self.error_here()),
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
                Ok(match inner {
                    Either::Pred(pred) | Either::GroupedPred(pred) => Either::GroupedPred(pred),
                    expr => expr,
                })
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
    /// A parenthesized predicate may itself be the base of an accessor.
    GroupedPred(Pred),
}

impl Either {
    fn into_expr(self) -> Result<Node, ExecError> {
        match self {
            Either::Expr(node) => Ok(node),
            Either::GroupedPred(pred) => Ok(Node::Predicate(Box::new(pred))),
            // `$ ? (@.a == 1 == 2)` and friends: a predicate cannot be an
            // operand, which is what `%nonassoc` on the comparison level means.
            Either::Pred(_) => Err(syntax(
                "syntax error at or near comparison operator of jsonpath input",
            )),
        }
    }

    fn into_pred(self) -> Result<Pred, ExecError> {
        match self {
            Either::Pred(p) | Either::GroupedPred(p) => Ok(p),
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
        validate_context_node(&root, false, false)?;
        Ok(JsonPath {
            strict,
            is_predicate,
            root,
        })
    }

    /// Warnings emitted when temporal item methods exceed PostgreSQL's
    /// microsecond precision ceiling.
    pub(crate) fn precision_warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        collect_precision_warnings_node(&self.root, &mut warnings);
        warnings
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
        self.query_in(target, vars, silent, None, false)
    }

    /// The normal function family still renders zone-aware methods in the
    /// session zone; unlike its `_tz` counterpart it cannot promote operands.
    pub(crate) fn query_with_session_time_zone(
        &self,
        target: &JsonbValue,
        vars: Option<&JsonbValue>,
        silent: bool,
        time_zone: &jiff::tz::TimeZone,
    ) -> Result<Vec<JsonbValue>, ExecError> {
        self.query_in(target, vars, silent, Some(time_zone), false)
    }

    pub(crate) fn query_first_with_session_time_zone(
        &self,
        target: &JsonbValue,
        vars: Option<&JsonbValue>,
        silent: bool,
        time_zone: &jiff::tz::TimeZone,
    ) -> Result<Option<JsonbValue>, ExecError> {
        self.query_first_in(target, vars, silent, Some(time_zone), false)
    }

    /// The time-zone-aware `jsonb_path_*_tz` query entry point.
    pub fn query_tz(
        &self,
        target: &JsonbValue,
        vars: Option<&JsonbValue>,
        silent: bool,
        time_zone: &jiff::tz::TimeZone,
    ) -> Result<Vec<JsonbValue>, ExecError> {
        self.query_in(target, vars, silent, Some(time_zone), true)
    }

    pub(crate) fn query_first_tz(
        &self,
        target: &JsonbValue,
        vars: Option<&JsonbValue>,
        silent: bool,
        time_zone: &jiff::tz::TimeZone,
    ) -> Result<Option<JsonbValue>, ExecError> {
        self.query_first_in(target, vars, silent, Some(time_zone), true)
    }

    fn query_in(
        &self,
        target: &JsonbValue,
        vars: Option<&JsonbValue>,
        silent: bool,
        time_zone: Option<&jiff::tz::TimeZone>,
        allow_zone_conversions: bool,
    ) -> Result<Vec<JsonbValue>, ExecError> {
        let exec = Exec {
            strict: self.strict,
            stop_after_one: false,
            vars,
            root: target,
            last: None,
            time_zone: time_zone.cloned(),
            allow_zone_conversions,
            current_temporal: None,
        };
        match exec.eval(&self.root, target) {
            Ok(items) => Ok(items.into_iter().map(Item::into_json).collect()),
            Err(e) if silent && is_structural(&e) => Ok(Vec::new()),
            Err(e) => Err(e.into_exec()),
        }
    }

    fn query_first_in(
        &self,
        target: &JsonbValue,
        vars: Option<&JsonbValue>,
        silent: bool,
        time_zone: Option<&jiff::tz::TimeZone>,
        allow_zone_conversions: bool,
    ) -> Result<Option<JsonbValue>, ExecError> {
        let exec = Exec {
            strict: self.strict,
            stop_after_one: true,
            vars,
            root: target,
            last: None,
            time_zone: time_zone.cloned(),
            allow_zone_conversions,
            current_temporal: None,
        };
        match exec.eval(&self.root, target) {
            Ok(items) => Ok(items.into_iter().next().map(Item::into_json)),
            Err(e) if silent && is_structural(&e) => Ok(None),
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
        self.exists_in(target, vars, silent, None, false)
    }

    pub(crate) fn exists_with_session_time_zone(
        &self,
        target: &JsonbValue,
        vars: Option<&JsonbValue>,
        silent: bool,
        time_zone: &jiff::tz::TimeZone,
    ) -> Result<Option<bool>, ExecError> {
        self.exists_in(target, vars, silent, Some(time_zone), false)
    }

    /// The time-zone-aware `jsonb_path_exists_tz` entry point.
    pub fn exists_tz(
        &self,
        target: &JsonbValue,
        vars: Option<&JsonbValue>,
        silent: bool,
        time_zone: &jiff::tz::TimeZone,
    ) -> Result<Option<bool>, ExecError> {
        self.exists_in(target, vars, silent, Some(time_zone), true)
    }

    fn exists_in(
        &self,
        target: &JsonbValue,
        vars: Option<&JsonbValue>,
        silent: bool,
        time_zone: Option<&jiff::tz::TimeZone>,
        allow_zone_conversions: bool,
    ) -> Result<Option<bool>, ExecError> {
        let exec = Exec {
            strict: self.strict,
            stop_after_one: false,
            vars,
            root: target,
            last: None,
            time_zone: time_zone.cloned(),
            allow_zone_conversions,
            current_temporal: None,
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
        self.predicate_in(target, vars, silent, None, false)
    }

    pub(crate) fn predicate_with_session_time_zone(
        &self,
        target: &JsonbValue,
        vars: Option<&JsonbValue>,
        silent: bool,
        time_zone: &jiff::tz::TimeZone,
    ) -> Result<Option<bool>, ExecError> {
        self.predicate_in(target, vars, silent, Some(time_zone), false)
    }

    /// The time-zone-aware `jsonb_path_match_tz` entry point.
    pub fn predicate_tz(
        &self,
        target: &JsonbValue,
        vars: Option<&JsonbValue>,
        silent: bool,
        time_zone: &jiff::tz::TimeZone,
    ) -> Result<Option<bool>, ExecError> {
        self.predicate_in(target, vars, silent, Some(time_zone), true)
    }

    fn predicate_in(
        &self,
        target: &JsonbValue,
        vars: Option<&JsonbValue>,
        silent: bool,
        time_zone: Option<&jiff::tz::TimeZone>,
        allow_zone_conversions: bool,
    ) -> Result<Option<bool>, ExecError> {
        let exec = Exec {
            strict: self.strict,
            stop_after_one: false,
            vars,
            root: target,
            last: None,
            time_zone: time_zone.cloned(),
            allow_zone_conversions,
            current_temporal: None,
        };
        let items = match exec.eval(&self.root, target) {
            Ok(items) => items,
            Err(e) if silent && is_structural(&e) => return Ok(None),
            Err(e) => return Err(e.into_exec()),
        };
        match items.as_slice() {
            [item] if matches!(&item.json, JsonbValue::Bool(_)) => match &item.json {
                JsonbValue::Bool(b) => Ok(Some(*b)),
                _ => unreachable!(),
            },
            [item] if matches!(&item.json, JsonbValue::Null) => Ok(None),
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
    stop_after_one: bool,
    vars: Option<&'a JsonbValue>,
    root: &'a JsonbValue,
    /// The array length `last` resolves against.
    ///
    /// The evaluator sets this while it evaluates a subscript.
    last: Option<usize>,
    /// The session zone used to render zone-aware item methods.
    time_zone: Option<jiff::tz::TimeZone>,
    /// Only the `_tz` family may use that zone for implicit conversions.
    allow_zone_conversions: bool,
    /// A filter's current item can be a datetime result whose JSON rendering
    /// alone no longer carries its SQL temporal type.
    current_temporal: Option<Datum>,
}

/// A JSONPath item is normally a JSON value. Date/time methods also retain the
/// parsed SQL datum until the public query boundary, where it becomes its JSON
/// string representation.
#[derive(Clone)]
struct Item {
    json: JsonbValue,
    temporal: Option<Datum>,
}

impl Item {
    fn json(value: JsonbValue) -> Self {
        Self {
            json: value,
            temporal: None,
        }
    }

    fn temporal(value: Datum, json: JsonbValue) -> Self {
        Self {
            json,
            temporal: Some(value),
        }
    }

    fn into_json(self) -> JsonbValue {
        self.json
    }

    fn json_ref(&self) -> &JsonbValue {
        &self.json
    }

    fn type_name(&self) -> &str {
        match self.temporal.as_ref() {
            Some(Datum::Date(_)) => "date",
            Some(Datum::Time(_)) => "time without time zone",
            Some(Datum::Timetz(_)) => "time with time zone",
            Some(Datum::Timestamp(_)) => "timestamp without time zone",
            Some(Datum::Timestamptz(_)) => "timestamp with time zone",
            _ => self.json.type_name(),
        }
    }
}

impl Exec<'_> {
    fn with_last(&self, last: Option<usize>) -> Exec<'_> {
        Exec {
            strict: self.strict,
            stop_after_one: self.stop_after_one,
            vars: self.vars,
            root: self.root,
            last,
            time_zone: self.time_zone.clone(),
            allow_zone_conversions: self.allow_zone_conversions,
            current_temporal: self.current_temporal.clone(),
        }
    }

    fn with_current_temporal(&self, temporal: Option<Datum>) -> Exec<'_> {
        Exec {
            strict: self.strict,
            stop_after_one: self.stop_after_one,
            vars: self.vars,
            root: self.root,
            last: self.last,
            time_zone: self.time_zone.clone(),
            allow_zone_conversions: self.allow_zone_conversions,
            current_temporal: temporal,
        }
    }

    fn eval(&self, node: &Node, current: &JsonbValue) -> PathResult<Vec<Item>> {
        match node {
            Node::Root => Ok(vec![Item::json(self.root.clone())]),
            Node::Current => Ok(vec![Item {
                json: current.clone(),
                temporal: self.current_temporal.clone(),
            }]),
            Node::Literal(v) => Ok(vec![Item::json(v.clone())]),
            Node::Last => {
                let Some(last) = self.last else {
                    return Err(PathError::new(
                        "42601",
                        "LAST is allowed only in array subscripts",
                    ));
                };
                Ok(vec![Item::json(JsonbValue::Number(BigDecimal::from(
                    i64::try_from(last).unwrap_or(i64::MAX) - 1,
                )))])
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
                Ok(vec![Item::json(value)])
            }
            Node::Predicate(p) => Ok(vec![Item::json(match self.eval_pred(p, current)? {
                Tri::True => JsonbValue::Bool(true),
                Tri::False => JsonbValue::Bool(false),
                Tri::Unknown => JsonbValue::Null,
            })]),
            Node::Neg { arg, negate } => {
                let items = self.eval(arg, current)?;
                let items = if self.strict {
                    items
                } else {
                    unwrap_arrays(items)
                };
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    let JsonbValue::Number(n) = item.json else {
                        return Err(PathError::new(
                            "22033",
                            format!(
                                "operand of unary jsonpath operator {} is not a numeric value",
                                if *negate { '-' } else { '+' }
                            ),
                        ));
                    };
                    out.push(Item::json(JsonbValue::Number(if *negate { -n } else { n })));
                }
                Ok(out)
            }
            Node::Arith { op, left, right } => {
                let l = self.single_number(left, current, *op, "left")?;
                let r = self.single_number(right, current, *op, "right")?;
                Ok(vec![Item::json(JsonbValue::Number(arith(*op, &l, &r)?))])
            }
            Node::Accessor { base, op } => {
                let items = self.eval(base, current)?;
                let mut out = Vec::new();
                for item in &items {
                    self.apply(op, item, current, &mut out)?;
                    if self.stop_after_one && !out.is_empty() {
                        break;
                    }
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
            [item] => match &item.json {
                JsonbValue::Number(n) => Ok(n.clone()),
                _ => Err(PathError::new(
                    "22033",
                    format!(
                        "{side} operand of jsonpath operator {} is not a single numeric value",
                        op.symbol()
                    ),
                )),
            },
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
        item: &Item,
        current: &JsonbValue,
        out: &mut Vec<Item>,
    ) -> PathResult<()> {
        let value = item.json_ref();
        match op {
            Accessor::Member(key) => self.member(value, key, out),
            Accessor::MemberAll => self.member_all(value, out),
            Accessor::Index(subs) => self.index(value, subs, current, out),
            Accessor::IndexAll => self.index_all(value, out),
            Accessor::Any { from, to, .. } => {
                let last = descendant_depth(value);
                let bound = |bound| match bound {
                    DepthBound::Number(value) => value,
                    DepthBound::Last => last,
                };
                descend(value, 0, bound(*from), bound(*to), out);
                Ok(())
            }
            Accessor::Method(m, args) => self.method(*m, args, item, out),
            Accessor::Filter(pred) => {
                let candidates: Vec<Item> = match value {
                    JsonbValue::Array(items) if !self.strict => {
                        items.iter().cloned().map(Item::json).collect()
                    }
                    _ => vec![item.clone()],
                };
                for candidate in candidates {
                    // A *structural* error inside a filter is Unknown, never
                    // raised — that is what makes `$ ? (@.missing > 1)` a quiet
                    // no-match. A missing variable is not structural and still
                    // reaches the caller.
                    let scoped = self.with_current_temporal(candidate.temporal.clone());
                    if scoped.eval_pred(pred, candidate.json_ref())? == Tri::True {
                        out.push(Item::json(candidate.json));
                    }
                }
                Ok(())
            }
        }
    }

    fn member(&self, item: &JsonbValue, key: &str, out: &mut Vec<Item>) -> PathResult<()> {
        match item {
            JsonbValue::Object(_) => {
                if let Some(v) = item.object_get(key) {
                    out.push(Item::json(v.clone()));
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
                        out.push(Item::json(v.clone()));
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

    fn member_all(&self, item: &JsonbValue, out: &mut Vec<Item>) -> PathResult<()> {
        match item {
            JsonbValue::Object(pairs) => {
                out.extend(pairs.iter().map(|(_, v)| Item::json(v.clone())));
                Ok(())
            }
            JsonbValue::Array(items) if !self.strict => {
                for elem in items {
                    if let JsonbValue::Object(pairs) = elem {
                        out.extend(pairs.iter().map(|(_, v)| Item::json(v.clone())));
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
        out: &mut Vec<Item>,
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
                out.push(Item::json(v.clone()));
                i += 1;
            }
        }
        Ok(())
    }

    /// One subscript expression, which must yield exactly one integral number.
    fn subscript(&self, node: &Node, current: &JsonbValue) -> PathResult<i64> {
        let items = self.eval(node, current)?;
        match items.as_slice() {
            [item] if matches!(&item.json, JsonbValue::Number(_)) => match &item.json {
                JsonbValue::Number(n) => n.to_i32().map(i64::from).ok_or_else(|| {
                    PathError::new("22033", "jsonpath array subscript is out of integer range")
                }),
                _ => unreachable!(),
            },
            _ => Err(PathError::new(
                "22033",
                "jsonpath array subscript is not a single numeric value",
            )),
        }
    }

    fn index_all(&self, item: &JsonbValue, out: &mut Vec<Item>) -> PathResult<()> {
        match item {
            JsonbValue::Array(items) => {
                out.extend(items.iter().cloned().map(Item::json));
                Ok(())
            }
            other => {
                if self.strict {
                    Err(PathError::new(
                        "22039",
                        "jsonpath wildcard array accessor can only be applied to an array",
                    ))
                } else {
                    out.push(Item::json(other.clone()));
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
    fn method(
        &self,
        m: Method,
        args: &MethodArgs,
        item: &Item,
        out: &mut Vec<Item>,
    ) -> PathResult<()> {
        let value = item.json_ref();
        match m {
            Method::Type => {
                out.push(Item::json(JsonbValue::String(item.type_name().to_string())));
                Ok(())
            }
            Method::Size => match value {
                JsonbValue::Array(items) => {
                    out.push(Item::json(JsonbValue::Number(BigDecimal::from(
                        i64::try_from(items.len()).unwrap_or(i64::MAX),
                    ))));
                    Ok(())
                }
                _ if self.strict => Err(PathError::new(
                    "22039",
                    "jsonpath item method .size() can only be applied to an array",
                )),
                _ => {
                    out.push(Item::json(JsonbValue::Number(BigDecimal::one())));
                    Ok(())
                }
            },
            Method::KeyValue => {
                let objects: Vec<&JsonbValue> = match value {
                    JsonbValue::Object(_) => vec![value],
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
                        out.push(Item::json(JsonbValue::object_from_pairs(vec![
                            ("id".into(), JsonbValue::Number(BigDecimal::zero())),
                            ("key".into(), JsonbValue::String(key.clone())),
                            ("value".into(), value.clone()),
                        ])));
                    }
                }
                Ok(())
            }
            _ => {
                let targets: Vec<Item> = match value {
                    JsonbValue::Array(items) if !self.strict => {
                        items.iter().cloned().map(Item::json).collect()
                    }
                    _ => vec![item.clone()],
                };
                for target in targets {
                    out.push(scalar_method(
                        m,
                        args,
                        &target,
                        self.time_zone.as_ref(),
                        self.allow_zone_conversions,
                    )?);
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
                        match self.compare(*op, l, r)? {
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
                        match (&v.json, &p.json) {
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
                    match &v.json {
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
    fn pred_operand(&self, node: &Node, current: &JsonbValue) -> PathResult<Option<Vec<Item>>> {
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

    fn compare(&self, op: CmpOp, left: &Item, right: &Item) -> PathResult<Tri> {
        let (Some(left), Some(right)) = (&left.temporal, &right.temporal) else {
            return Ok(compare_json(op, &left.json, &right.json));
        };
        let promotion_zone = if self.allow_zone_conversions {
            self.time_zone.as_ref()
        } else {
            None
        };
        let (left, right) = promote_temporal_pair(left, right, promotion_zone)?;
        let Some(ord) = crabka_pgtypes::ops::compare(&left, &right).ok().flatten() else {
            return Ok(Tri::Unknown);
        };
        Ok(compare_ordering(op, ord))
    }
}

/// `PostgreSQL`'s lax-mode operand unwrapping: an array operand contributes its
/// elements, everything else contributes itself.
fn unwrap_arrays(items: Vec<Item>) -> Vec<Item> {
    let mut out = Vec::with_capacity(items.len());
    for Item { json, temporal } in items {
        match (json, temporal) {
            (JsonbValue::Array(elems), None) => {
                out.extend(elems.into_iter().map(Item::json));
            }
            (json, temporal) => out.push(Item { json, temporal }),
        }
    }
    out
}

/// `.**{from to to}`: every descendant at a depth within the window, and the
/// item itself counts as depth 0.
///
/// Pre-order, which matches `PostgreSQL`'s output order.
fn descend(item: &JsonbValue, depth: u32, from: u32, to: u32, out: &mut Vec<Item>) {
    if depth >= from && depth <= to {
        out.push(Item::json(item.clone()));
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

fn descendant_depth(item: &JsonbValue) -> u32 {
    match item {
        JsonbValue::Array(items) => items
            .iter()
            .map(|item| descendant_depth(item).saturating_add(1))
            .max()
            .unwrap_or(0),
        JsonbValue::Object(items) => items
            .iter()
            .map(|(_, item)| descendant_depth(item).saturating_add(1))
            .max()
            .unwrap_or(0),
        _ => 0,
    }
}

/// `PostgreSQL`'s `compareItems`: values of different types never compare equal
/// (except against JSON `null`), and arrays/objects are not comparable at all.
fn compare_json(op: CmpOp, left: &JsonbValue, right: &JsonbValue) -> Tri {
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
    compare_ordering(op, ord)
}

fn compare_ordering(op: CmpOp, ord: std::cmp::Ordering) -> Tri {
    use std::cmp::Ordering;

    Tri::of(match op {
        CmpOp::Eq => ord == Ordering::Equal,
        CmpOp::Ne => ord != Ordering::Equal,
        CmpOp::Lt => ord == Ordering::Less,
        CmpOp::Le => ord != Ordering::Greater,
        CmpOp::Gt => ord == Ordering::Greater,
        CmpOp::Ge => ord != Ordering::Less,
    })
}

fn promote_temporal_pair(
    left: &Datum,
    right: &Datum,
    time_zone: Option<&jiff::tz::TimeZone>,
) -> PathResult<(Datum, Datum)> {
    use crabka_pgtypes::ColumnType;

    let (target, source) = match (left, right) {
        (Datum::Date(_), Datum::Timestamptz(_)) => (ColumnType::Timestamptz, left),
        (Datum::Timestamptz(_), Datum::Date(_)) => (ColumnType::Timestamptz, right),
        (Datum::Timestamp(_), Datum::Timestamptz(_)) => (ColumnType::Timestamptz, left),
        (Datum::Timestamptz(_), Datum::Timestamp(_)) => (ColumnType::Timestamptz, right),
        (Datum::Time(_), Datum::Timetz(_)) => (ColumnType::Timetz, left),
        (Datum::Timetz(_), Datum::Time(_)) => (ColumnType::Timetz, right),
        _ => return Ok((left.clone(), right.clone())),
    };
    let Some(time_zone) = time_zone else {
        return Err(PathError::new(
            "0A000",
            format!(
                "cannot convert value from {} to {} without time zone usage\nHINT:  Use *_tz() function for time zone support.",
                temporal_name(source),
                match target {
                    ColumnType::Timetz => "timetz",
                    ColumnType::Timestamptz => "timestamptz",
                    _ => unreachable!("only zoned temporal target types are selected"),
                },
            ),
        ));
    };
    let left = crabka_pgtypes::cast::cast(left, target, time_zone)
        .map_err(|error| PathError::new("22008", error.to_string()))?;
    let right = crabka_pgtypes::cast::cast(right, target, time_zone)
        .map_err(|error| PathError::new("22008", error.to_string()))?;
    Ok((left, right))
}

fn temporal_name(value: &Datum) -> &'static str {
    match value {
        Datum::Date(_) => "date",
        Datum::Time(_) => "time",
        Datum::Timetz(_) => "timetz",
        Datum::Timestamp(_) => "timestamp",
        Datum::Timestamptz(_) => "timestamptz",
        _ => "unknown",
    }
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
fn scalar_method(
    m: Method,
    args: &MethodArgs,
    item: &Item,
    time_zone: Option<&jiff::tz::TimeZone>,
    allow_zone_conversions: bool,
) -> PathResult<Item> {
    let name = method_name(m);
    if matches!(
        m,
        Method::Date
            | Method::Time
            | Method::TimeTz
            | Method::Timestamp
            | Method::TimestampTz
            | Method::Datetime
    ) {
        let precision = match args {
            MethodArgs::TemporalPrecision(precision) => Some(temporal_precision(m, precision)?),
            _ => None,
        };
        return datetime_method(
            m,
            match args {
                MethodArgs::Datetime(template) => Some(template),
                _ => None,
            },
            precision,
            item.json_ref(),
            time_zone,
            allow_zone_conversions,
        );
    }
    let item = item.json_ref();
    Ok(Item::json(match m {
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
            if jsonpath_nonfinite(&text) {
                return Err(PathError::new(
                    "22036",
                    format!("NaN or Infinity is not allowed for jsonpath item method {name}"),
                ));
            }
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
            let integer = match item {
                JsonbValue::String(_) if decimal.fractional_digit_count() != 0 => {
                    return Err(invalid_for(name, &text, target));
                }
                JsonbValue::String(_) => decimal,
                JsonbValue::Number(_) => decimal.round(0),
                _ => unreachable!("numeric_source rejects non-string and non-numeric values"),
            };
            let value = integer
                .to_i64()
                .ok_or_else(|| invalid_for(name, &text, target))?;
            if m == Method::Integer && i32::try_from(value).is_err() {
                return Err(invalid_for(name, &text, target));
            }
            Ok(JsonbValue::Number(BigDecimal::from(value)))
        }
        Method::Number => {
            let text = numeric_source(item, name)?;
            if jsonpath_nonfinite(&text) {
                return Err(PathError::new(
                    "22036",
                    format!("NaN or Infinity is not allowed for jsonpath item method {name}"),
                ));
            }
            let decimal = text
                .parse::<BigDecimal>()
                .map_err(|_| invalid_for(name, &text, "numeric"))?;
            Ok(JsonbValue::Number(decimal))
        }
        Method::Decimal => {
            let text = numeric_source(item, name)?;
            if jsonpath_nonfinite(&text) {
                return Err(PathError::new(
                    "22036",
                    "NaN or Infinity is not allowed for jsonpath item method .decimal()",
                ));
            }
            let decimal = text
                .parse::<BigDecimal>()
                .map_err(|_| invalid_for(name, &text, "numeric"))?;
            let decimal = match args {
                MethodArgs::Decimal { precision, scale } => {
                    decimal_with_typmod(decimal, *precision, scale.unwrap_or(0)).map_err(
                        |error| {
                            if error.sqlstate == "22003" {
                                invalid_for(name, &text, "numeric")
                            } else {
                                error
                            }
                        },
                    )?
                }
                _ => decimal,
            };
            Ok(JsonbValue::Number(decimal))
        }
        Method::Boolean => match item {
            JsonbValue::Bool(b) => Ok(JsonbValue::Bool(*b)),
            JsonbValue::Number(n)
                if n.fractional_digit_count() == 0 && n.to_f64().is_some_and(f64::is_finite) =>
            {
                Ok(JsonbValue::Bool(!n.is_zero()))
            }
            JsonbValue::Number(n) => Err(invalid_for(
                name,
                &crabka_pgtypes::numeric::finite_to_text(n),
                "boolean",
            )),
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
        | Method::Datetime => unreachable!(),
        // `type`, `size` and `keyvalue` never reach here.
        Method::Type | Method::Size | Method::KeyValue => Ok(item.clone()),
    }?))
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

fn jsonpath_nonfinite(text: &str) -> bool {
    matches!(text.to_ascii_lowercase().as_str(), "nan" | "inf" | "-inf")
}

fn decimal_with_typmod(value: BigDecimal, precision: i32, scale: i32) -> PathResult<BigDecimal> {
    if !(1..=1000).contains(&precision) {
        return Err(PathError::new(
            "22023",
            format!("NUMERIC precision {precision} must be between 1 and 1000"),
        ));
    }
    if !(-1000..=1000).contains(&scale) {
        return Err(PathError::new(
            "22023",
            format!("NUMERIC scale {scale} must be between -1000 and 1000"),
        ));
    }
    let rounded = value.with_scale_round(i64::from(scale), RoundingMode::HalfUp);
    // JSON number rendering stores the coefficient with a nonnegative scale.
    let rounded = if scale < 0 {
        rounded
            .to_string()
            .parse()
            .map_err(|_| PathError::new("22003", "numeric field overflow"))?
    } else {
        rounded
    };
    if !rounded.is_zero() {
        let (mantissa, decimal_scale) = rounded.as_bigint_and_exponent();
        let digits = mantissa.to_string().trim_start_matches('-').len() as i64;
        let integer_digits = digits - decimal_scale;
        if integer_digits > i64::from(precision) - i64::from(scale) {
            return Err(PathError::new("22003", "numeric field overflow"));
        }
    }
    Ok(rounded)
}

fn temporal_precision(m: Method, precision: &BigDecimal) -> PathResult<i32> {
    let precision = precision.to_i32().ok_or_else(|| {
        PathError::new(
            "22031",
            format!(
                "time precision of jsonpath item method {} is out of range for type integer",
                method_name(m)
            ),
        )
    })?;
    if precision < 0 {
        let type_name = match m {
            Method::Time => "TIME",
            Method::TimeTz => "TIME WITH TIME ZONE",
            Method::Timestamp => "TIMESTAMP",
            Method::TimestampTz => "TIMESTAMP WITH TIME ZONE",
            _ => unreachable!("only explicit temporal methods accept a precision"),
        };
        return Err(PathError::new(
            "22023",
            format!("{type_name}({precision}) precision must not be negative"),
        ));
    }
    Ok(precision)
}

/// The date/time methods retain their parsed value for comparisons and render
/// the canonical ISO-8601 string only when a query returns the item.
fn datetime_method(
    m: Method,
    template: Option<&str>,
    precision: Option<i32>,
    item: &JsonbValue,
    time_zone: Option<&jiff::tz::TimeZone>,
    allow_zone_conversions: bool,
) -> PathResult<Item> {
    use crabka_pgtypes::{ColumnType, Datum, TemporalType};

    let name = method_name(m);
    let format_name = name.trim_start_matches('.').trim_end_matches("()");
    let JsonbValue::String(text) = item else {
        return Err(PathError::new(
            "22031",
            format!("jsonpath item method {name} can only be applied to a string"),
        ));
    };
    if let Some(template) = template {
        let fields = crabka_pgtypes::datetime::template_fields(template);
        let parsed =
            crabka_pgtypes::datetime::parse_by_template_exact(template, text).map_err(|e| {
                let error = ExecError::from(e).into_pg();
                let sqlstate = match error.code.as_str() {
                    "22008" => "22008",
                    "22009" => "22009",
                    _ => "22007",
                };
                PathError::new(sqlstate, error.message)
            })?;
        let date = jiff::civil::Date::new(
            i16::try_from(parsed.year).map_err(|_| invalid_for(name, text, "datetime"))?,
            i8::try_from(parsed.month).map_err(|_| invalid_for(name, text, "datetime"))?,
            i8::try_from(parsed.day).map_err(|_| invalid_for(name, text, "datetime"))?,
        )
        .map_err(|_| invalid_for(name, text, "datetime"))?;
        let micros = match fields.fractional_precision {
            Some(precision) if precision < 6 => {
                let scale = 10_i64.pow(u32::from(6 - precision));
                (i64::from(parsed.micros) + scale / 2) / scale * scale
            }
            _ => i64::from(parsed.micros),
        };
        let time = jiff::civil::Time::new(
            i8::try_from(parsed.hour).map_err(|_| invalid_for(name, text, "datetime"))?,
            i8::try_from(parsed.minute).map_err(|_| invalid_for(name, text, "datetime"))?,
            i8::try_from(parsed.second).map_err(|_| invalid_for(name, text, "datetime"))?,
            i32::try_from(parsed.micros).map_err(|_| invalid_for(name, text, "datetime"))? * 1_000,
        )
        .map_err(|_| invalid_for(name, text, "datetime"))?;
        let datetime = date
            .to_datetime(time)
            .checked_add((micros - i64::from(parsed.micros)).microseconds())
            .map_err(|_| invalid_for(name, text, "datetime"))?;
        let mut rendered = match (fields.has_date, fields.has_time) {
            (true, false) => date.to_string(),
            (false, true) => datetime.time().to_string(),
            (true, true) => datetime.to_string(),
            (false, false) => return Err(invalid_for(name, text, "datetime")),
        };
        if let Some(offset) = parsed.tz_offset_secs {
            let sign = if offset < 0 { '-' } else { '+' };
            let minutes = offset.unsigned_abs() / 60;
            let _ = write!(rendered, "{sign}{:02}:{:02}", minutes / 60, minutes % 60);
        }
        let target = match (
            fields.has_date,
            fields.has_time,
            parsed.tz_offset_secs.is_some(),
        ) {
            (true, false, _) => ColumnType::Date,
            (false, true, true) => ColumnType::Timetz,
            (false, true, false) => ColumnType::Time,
            (true, true, true) => ColumnType::Timestamptz,
            (true, true, false) => ColumnType::Timestamp,
            (false, false, _) => return Err(invalid_for(name, text, "datetime")),
        };
        let datum = crabka_pgtypes::cast::cast(
            &Datum::Text(rendered.clone()),
            target,
            &jiff::tz::TimeZone::UTC,
        )
        .map_err(|_| invalid_for(name, text, "datetime"))?;
        return Ok(Item::temporal(datum, JsonbValue::String(rendered)));
    }
    // `.datetime()` chooses the temporal family from the spelling before
    // parsing. `date_in` accepts a timestamp prefix, but JSONPath must retain
    // an explicit offset so the plain and `_tz` comparison families differ.
    let has_time = text.contains(':');
    let has_date = text.contains('/')
        || text
            .find('-')
            .is_some_and(|dash| text.find(':').is_none_or(|clock| dash < clock));
    let has_offset = text.ends_with('Z')
        || text
            .rfind(['+', '-'])
            .is_some_and(|at| text[..at].contains(':'))
        || (has_time && text.as_bytes().last().is_some_and(u8::is_ascii_alphabetic));
    let source_type = match (has_date, has_time, has_offset) {
        (true, true, true) => ColumnType::Timestamptz,
        (true, true, false) => ColumnType::Timestamp,
        (false, true, true) => ColumnType::Timetz,
        (false, true, false) => ColumnType::Time,
        _ => ColumnType::Date,
    };
    let target = match m {
        Method::Date => ColumnType::Date,
        Method::Time => ColumnType::Time,
        Method::TimeTz => ColumnType::Timetz,
        Method::Timestamp => ColumnType::Timestamp,
        Method::TimestampTz => ColumnType::Timestamptz,
        _ => source_type,
    };
    let source_is_zoned = matches!(source_type, ColumnType::Timetz | ColumnType::Timestamptz);
    let target_is_zoned = matches!(target, ColumnType::Timetz | ColumnType::Timestamptz);
    let can_convert_zone = match target {
        ColumnType::Date | ColumnType::Timestamp | ColumnType::Timestamptz => has_date,
        ColumnType::Time => has_time,
        ColumnType::Timetz => has_time,
        _ => false,
    };
    let default_tz = jiff::tz::TimeZone::UTC;
    let tz = time_zone.unwrap_or(&default_tz);
    let source = Datum::Text(
        text.get(10..11)
            .filter(|separator| *separator == "T")
            .map_or_else(
                || text.clone(),
                |_| format!("{} {}", &text[..10], &text[11..]),
            ),
    );
    let parsed = if source_type != target && can_convert_zone {
        if source_is_zoned != target_is_zoned && !allow_zone_conversions {
            let source_name = match source_type {
                ColumnType::Date => "date",
                ColumnType::Time => "time",
                ColumnType::Timetz => "timetz",
                ColumnType::Timestamp => "timestamp",
                ColumnType::Timestamptz => "timestamptz",
                _ => unreachable!("only temporal source types are selected"),
            };
            let target_name = match target {
                ColumnType::Date => "date",
                ColumnType::Time => "time",
                ColumnType::Timetz => "timetz",
                ColumnType::Timestamp => "timestamp",
                ColumnType::Timestamptz => "timestamptz",
                _ => unreachable!("only temporal target types are selected"),
            };
            return Err(PathError::new(
                "0A000",
                format!(
                    "cannot convert value from {source_name} to {target_name} without time zone usage\nHINT:  Use *_tz() function for time zone support."
                ),
            ));
        }
        let parsed = crabka_pgtypes::cast::cast(&source, source_type, tz).map_err(|_| {
            PathError::new(
                "22007",
                format!("{format_name} format is not recognized: \"{text}\""),
            )
        })?;
        match (&parsed, target) {
            // The normal SQL cast deliberately only removes the `timetz`
            // offset. JSONPath's `_tz` variant instead expresses it in the
            // supplied session zone before discarding that zone.
            (Datum::Timetz(value), ColumnType::Time) => {
                let offset = tz.to_offset(jiff::Timestamp::now());
                let micros = (value.utc_micros() + i64::from(offset.seconds()) * 1_000_000)
                    .rem_euclid(86_400_000_000);
                Datum::Time(crabka_pgtypes::datetime::time_from_micros_of_day_public(
                    micros,
                ))
            }
            _ => crabka_pgtypes::cast::cast(&parsed, target, tz).map_err(|_| {
                PathError::new(
                    "22007",
                    format!("{format_name} format is not recognized: \"{text}\""),
                )
            })?,
        }
    } else {
        crabka_pgtypes::cast::cast(&source, target, tz).map_err(|_| {
            PathError::new(
                "22007",
                format!("{format_name} format is not recognized: \"{text}\""),
            )
        })?
    };
    let parsed = match precision {
        Some(precision) => {
            let temporal = match m {
                Method::Time => TemporalType::Time,
                Method::TimeTz => TemporalType::Timetz,
                Method::Timestamp => TemporalType::Timestamp,
                Method::TimestampTz => TemporalType::Timestamptz,
                _ => unreachable!("only explicit temporal methods accept a precision"),
            };
            crabka_pgtypes::cast::cast(
                &parsed,
                ColumnType::Temporal(temporal, precision.min(6) as u8),
                tz,
            )
            .map_err(|_| invalid_for(name, text, "datetime"))?
        }
        None => parsed,
    };
    let render_tz = if matches!(source_type, ColumnType::Timestamptz)
        && matches!(target, ColumnType::Timestamptz)
    {
        let Datum::Timetz(value) = crabka_pgtypes::cast::cast(&source, ColumnType::Timetz, tz)
            .map_err(|_| {
                PathError::new(
                    "22007",
                    format!("{format_name} format is not recognized: \"{text}\""),
                )
            })?
        else {
            unreachable!("a timestamp with time zone has a time with time zone projection")
        };
        jiff::tz::TimeZone::fixed(value.offset)
    } else {
        tz.clone()
    };
    let mut rendered =
        String::from_utf8(crabka_pgtypes::encoding::encode_text(&parsed, &render_tz))
            .map_err(|_| PathError::new("22007", format!("{format_name} produced invalid text")))?;
    // A jsonpath datetime renders ISO-8601 with a `T` separator, unlike SQL's
    // space-separated `timestamp` output.
    if let Some(space) = rendered.find(' ') {
        rendered.replace_range(space..=space, "T");
    }
    if matches!(&parsed, Datum::Timetz(_) | Datum::Timestamptz(_))
        && let Some(start) = rendered.len().checked_sub(3)
        && matches!(rendered.as_bytes()[start], b'+' | b'-')
        && rendered.as_bytes()[start + 1..]
            .iter()
            .all(u8::is_ascii_digit)
    {
        rendered.push_str(":00");
    }
    Ok(Item::temporal(parsed, JsonbValue::String(rendered)))
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
    for flag in ['i', 'm', 's', 'x'] {
        if flags.contains(flag) {
            builder.push(flag);
        }
    }
    let literal = flags.contains('q');
    if let Some(other) = flags
        .chars()
        .find(|flag| !matches!(flag, 'i' | 's' | 'm' | 'x' | 'q'))
    {
        return Err(PathError::new(
            "22025",
            format!(
                "invalid input syntax for type jsonpath: unrecognized flag character \"{other}\" in LIKE_REGEX predicate"
            ),
        ));
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
        let detail = e.to_string();
        let detail = if detail.contains("unclosed group") {
            "parentheses () not balanced".into()
        } else {
            first_line(&detail)
        };
        PathError::new("2201B", format!("invalid regular expression: {detail}"))
    })
}

/// PostgreSQL validates `like_regex` when it constructs the jsonpath value,
/// before that path is evaluated against any JSON document.
fn validate_like_regex(pattern: &str, flags: &str) -> Result<(), ExecError> {
    if flags.contains('x') && !flags.contains('q') {
        return Err(ExecError::Unsupported(
            "XQuery \"x\" flag (expanded regular expressions) is not implemented".into(),
        ));
    }
    for flag in flags.chars() {
        match flag {
            'i' | 's' | 'm' | 'x' | 'q' => {}
            other => {
                return Err(ExecError::Remote(
                    crabka_pgwire::error::PgError::error(
                        "42601",
                        "invalid input syntax for type jsonpath",
                    )
                    .with_detail(format!(
                        "Unrecognized flag character \"{other}\" in LIKE_REGEX predicate."
                    )),
                ));
            }
        }
    }
    compile_regex(pattern, flags)
        .map(|_| ())
        .map_err(PathError::into_exec)
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

fn write_depth_bound(bound: DepthBound, out: &mut String) {
    match bound {
        DepthBound::Number(value) => {
            let _ = write!(out, "{value}");
        }
        DepthBound::Last => out.push_str("last"),
    }
}

fn write_node(node: &Node, out: &mut String) {
    if matches!(
        node,
        Node::Predicate(_) | Node::Arith { .. } | Node::Neg { .. }
    ) {
        out.push('(');
        write_node_prec(node, 0, out);
        out.push(')');
    } else {
        write_node_prec(node, 0, out);
    }
}

fn node_precedence(node: &Node) -> u8 {
    match node {
        Node::Predicate(_) => 0,
        Node::Arith { op, .. } => match op {
            ArithOp::Add | ArithOp::Sub => 1,
            ArithOp::Mul | ArithOp::Div | ArithOp::Mod => 2,
        },
        Node::Neg { .. } => 3,
        Node::Root
        | Node::Current
        | Node::Last
        | Node::Var(_)
        | Node::Literal(_)
        | Node::Accessor { .. } => 4,
    }
}

fn write_node_prec(node: &Node, min_precedence: u8, out: &mut String) {
    let needs_parens = node_precedence(node) < min_precedence;
    if needs_parens {
        out.push('(');
    }
    match node {
        Node::Root => out.push('$'),
        Node::Current => out.push('@'),
        Node::Last => out.push_str("last"),
        Node::Var(name) => {
            out.push('$');
            out.push_str(&JsonbValue::String(name.clone()).to_text());
        }
        Node::Literal(v) => out.push_str(&v.to_text()),
        Node::Neg { arg, negate } => {
            out.push(if *negate { '-' } else { '+' });
            write_node_prec(arg, 4, out);
        }
        Node::Arith { op, left, right } => {
            let precedence = node_precedence(node);
            write_node_prec(left, precedence, out);
            let _ = write!(out, " {} ", op.symbol());
            write_node_prec(right, precedence + 1, out);
        }
        Node::Predicate(p) => write_pred(p, out),
        Node::Accessor { base, op } => {
            if matches!(base.as_ref(), Node::Literal(JsonbValue::Number(_))) {
                out.push('(');
                write_node_prec(base, 0, out);
                out.push(')');
            } else {
                write_node_prec(base, 4, out);
            }
            match op {
                Accessor::Member(key) => {
                    out.push('.');
                    out.push_str(&JsonbValue::String(key.clone()).to_text());
                }
                Accessor::MemberAll => out.push_str(".*"),
                Accessor::IndexAll => out.push_str("[*]"),
                Accessor::Index(subs) => {
                    out.push('[');
                    for (i, (lo, hi)) in subs.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        write_node_prec(lo, 0, out);
                        if let Some(hi) = hi {
                            out.push_str(" to ");
                            write_node_prec(hi, 0, out);
                        }
                    }
                    out.push(']');
                }
                Accessor::Any {
                    from,
                    to,
                    explicit_bounds,
                } => {
                    out.push_str(".**");
                    if *explicit_bounds {
                        if from == to {
                            out.push('{');
                            write_depth_bound(*from, out);
                            out.push('}');
                        } else {
                            out.push('{');
                            write_depth_bound(*from, out);
                            out.push_str(" to ");
                            write_depth_bound(*to, out);
                            out.push('}');
                        }
                    }
                }
                Accessor::Method(m, args) => match args {
                    MethodArgs::None => out.push_str(method_name(*m)),
                    MethodArgs::Datetime(template) => {
                        out.push_str(".datetime(");
                        out.push_str(&JsonbValue::String(template.clone()).to_text());
                        out.push(')');
                    }
                    MethodArgs::Decimal { precision, scale } => {
                        let _ = write!(out, ".decimal({precision}");
                        if let Some(scale) = scale {
                            let _ = write!(out, ",{scale}");
                        }
                        out.push(')');
                    }
                    MethodArgs::TemporalPrecision(precision) => {
                        let _ = write!(
                            out,
                            "{}({precision})",
                            method_name(*m).trim_end_matches("()")
                        );
                    }
                },
                Accessor::Filter(p) => {
                    out.push_str("?(");
                    write_pred(p, out);
                    out.push(')');
                }
            }
        }
    }
    if needs_parens {
        out.push(')');
    }
}

fn write_pred(pred: &Pred, out: &mut String) {
    match pred {
        Pred::And(a, b) => {
            write_pred_prec(a, 2, out);
            out.push_str(" && ");
            write_pred_prec(b, 2, out);
        }
        Pred::Or(a, b) => {
            write_pred_prec(a, 1, out);
            out.push_str(" || ");
            write_pred_prec(b, 1, out);
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
            write_node_prec(node, 0, out);
            out.push(')');
        }
        Pred::Compare { op, left, right } => {
            write_node_prec(left, 0, out);
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
            write_node_prec(right, 0, out);
        }
        Pred::StartsWith { value, prefix } => {
            write_node_prec(value, 0, out);
            out.push_str(" starts with ");
            write_node_prec(prefix, 0, out);
        }
        Pred::LikeRegex {
            value,
            pattern,
            flags,
        } => {
            write_node_prec(value, 0, out);
            let _ = write!(out, " like_regex \"{pattern}\"");
            if !flags.is_empty() {
                let _ = write!(out, " flag \"{flags}\"");
            }
        }
    }
}

fn pred_precedence(pred: &Pred) -> u8 {
    match pred {
        Pred::Or(_, _) => 1,
        Pred::And(_, _) => 2,
        Pred::Not(_) => 3,
        Pred::IsUnknown(_)
        | Pred::Exists(_)
        | Pred::Compare { .. }
        | Pred::StartsWith { .. }
        | Pred::LikeRegex { .. } => 4,
    }
}

fn write_pred_prec(pred: &Pred, min_precedence: u8, out: &mut String) {
    let needs_parens = pred_precedence(pred) < min_precedence;
    if needs_parens {
        out.push('(');
    }
    write_pred(pred, out);
    if needs_parens {
        out.push(')');
    }
}

/// The `vars` argument must be a jsonb *object*; anything else is 22023.
pub fn check_vars(vars: &JsonbValue) -> Result<(), ExecError> {
    if matches!(vars, JsonbValue::Object(_)) {
        Ok(())
    } else {
        Err(ExecError::FunctionErrorWithMessageDetail {
            sqlstate: "22023",
            message: "\"vars\" argument is not an object".into(),
            detail: "Jsonpath parameters should be encoded as key-value pairs of \"vars\" object.",
        })
    }
}

#[cfg(test)]
mod tests;
