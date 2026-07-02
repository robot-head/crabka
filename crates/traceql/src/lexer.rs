//! `TraceQL` lexer.

use crate::error::{Result, TraceqlError};

#[derive(Clone, Debug, PartialEq)]
pub enum Token {
    LBrace,
    RBrace,
    LParen,
    RParen,
    Pipe,
    And,
    Or,
    Not,
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    Re,
    Nre,
    Plus,
    Minus,
    Star,
    Slash,
    Mod,
    Caret,
    Desc,
    Anc,
    Child,
    Parent,
    Sibling,
    NegDesc,
    NegAnc,
    NegChild,
    NegParent,
    UnionDesc,
    UnionAnc,
    UnionChild,
    UnionParent,
    UnionSibling,
    Dot,
    Colon,
    Comma,
    Ident(String),
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Nil,
    Eof,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Prev {
    None,
    Dot,
    Ident,
    Other,
}

pub fn lex(input: &str) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut i = 0;
    let mut prev = Prev::None;

    while i < input.len() {
        let rest = &input[i..];
        let ch = rest.chars().next().unwrap();
        if ch.is_whitespace() {
            let next = advance(input, i, ch.len_utf8())?;
            if next <= i {
                return Err(no_progress(i));
            }
            i = next;
            continue;
        }

        if rest.starts_with("==") {
            return Err(TraceqlError::Parse(format!(
                "use single = for equality; == is not TraceQL at byte {i}"
            )));
        }

        // A `.` immediately followed by a digit (e.g. `.05`, `.99`) is a
        // leading-dot fractional number, lexed as a single `Token::Float` so
        // leading zeros survive. A `.` followed by an identifier (e.g.
        // `.service`) remains a `Dot` for attribute-scope syntax.
        if ch == '.'
            && rest[1..]
                .chars()
                .next()
                .is_some_and(|next| next.is_ascii_digit())
        {
            let (tok, len) = scan_number_or_duration(rest)?;
            tokens.push(tok);
            let next = advance(input, i, len)?;
            if next <= i {
                return Err(no_progress(i));
            }
            i = next;
            prev = Prev::Ident;
            continue;
        }

        if let Some((tok, len)) = op_token(rest) {
            let next = advance(input, i, len)?;
            if next <= i {
                return Err(no_progress(i));
            }
            i = next;
            prev = match tok {
                Token::Dot => Prev::Dot,
                Token::Ident(_) => Prev::Ident,
                _ => Prev::Other,
            };
            tokens.push(tok);
            continue;
        }

        if ch == '"' {
            let (s, len) = scan_string(rest)?;
            tokens.push(Token::Str(s));
            let next = advance(input, i, len)?;
            if next <= i {
                return Err(no_progress(i));
            }
            i = next;
            prev = Prev::Other;
            continue;
        }

        if ch.is_ascii_digit() {
            let (tok, len) = scan_number_or_duration(rest)?;
            tokens.push(tok);
            let next = advance(input, i, len)?;
            if next <= i {
                return Err(no_progress(i));
            }
            i = next;
            prev = Prev::Ident;
            continue;
        }

        if is_ident_start(ch) {
            let allow_dots = prev == Prev::Dot;
            let (ident, len) = scan_ident(rest, allow_dots);
            tokens.push(keyword_or_ident(ident));
            let next = advance(input, i, len)?;
            if next <= i {
                return Err(no_progress(i));
            }
            i = next;
            prev = Prev::Ident;
            continue;
        }

        return Err(TraceqlError::Parse(format!(
            "unexpected character {ch:?} at byte {i}"
        )));
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

fn no_progress(pos: usize) -> TraceqlError {
    TraceqlError::Parse(format!("lexer made no progress at byte {pos}"))
}

fn advance(input: &str, pos: usize, len: usize) -> Result<usize> {
    if len == 0 {
        return Err(no_progress(pos));
    }
    let next = pos
        .checked_add(len)
        .ok_or_else(|| TraceqlError::Parse(format!("lexer position overflow at byte {pos}")))?;
    if next > input.len() {
        return Err(TraceqlError::Parse(format!(
            "lexer advanced past end of input at byte {pos}"
        )));
    }
    Ok(next)
}

fn op_token(s: &str) -> Option<(Token, usize)> {
    for (raw, tok) in [
        ("!>>", Token::NegDesc),
        ("&>>", Token::UnionDesc),
        ("!<<", Token::NegAnc),
        ("&<<", Token::UnionAnc),
        (">>", Token::Desc),
        ("<<", Token::Anc),
        ("!>", Token::NegChild),
        ("!<", Token::NegParent),
        ("&>", Token::UnionChild),
        ("&<", Token::UnionParent),
        ("&~", Token::UnionSibling),
        ("&&", Token::And),
        ("||", Token::Or),
        (">=", Token::Gte),
        ("<=", Token::Lte),
        ("=~", Token::Re),
        ("!~", Token::Nre),
        ("!=", Token::Neq),
    ] {
        if s.starts_with(raw) {
            return Some((tok, raw.len()));
        }
    }

    let ch = s.chars().next()?;
    let tok = match ch {
        '=' => Token::Eq,
        '<' => Token::Parent,
        '>' => Token::Child,
        '~' => Token::Sibling,
        '!' => Token::Not,
        '+' => Token::Plus,
        '-' => Token::Minus,
        '*' => Token::Star,
        '/' => Token::Slash,
        '%' => Token::Mod,
        '^' => Token::Caret,
        '.' => Token::Dot,
        ':' => Token::Colon,
        ',' => Token::Comma,
        '(' => Token::LParen,
        ')' => Token::RParen,
        '{' => Token::LBrace,
        '}' => Token::RBrace,
        '|' => Token::Pipe,
        '&' => {
            return Some((Token::Ident("&".into()), ch.len_utf8()));
        }
        _ => return None,
    };
    Some((tok, ch.len_utf8()))
}

fn scan_string(s: &str) -> Result<(String, usize)> {
    let mut out = String::new();
    let mut escaped = false;
    for (idx, ch) in s.char_indices().skip(1) {
        if escaped {
            out.push(match ch {
                '"' => '"',
                '\\' => '\\',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                other => other,
            });
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            return Ok((out, idx + ch.len_utf8()));
        } else {
            out.push(ch);
        }
    }
    Err(TraceqlError::Parse("unterminated string literal".into()))
}

fn scan_number_or_duration(s: &str) -> Result<(Token, usize)> {
    let mut end = 0;
    let mut has_dot = false;
    let mut chars = s.char_indices().peekable();
    while let Some((idx, ch)) = chars.peek().copied() {
        if ch.is_ascii_digit() {
            end = idx + ch.len_utf8();
            chars.next();
        } else if ch == '.'
            && !has_dot
            && chars
                .clone()
                .nth(1)
                .is_some_and(|(_, next)| next.is_ascii_digit())
        {
            has_dot = true;
            end = idx + 1;
            chars.next();
        } else {
            break;
        }
    }

    let mut ident_end = end;
    for (idx, ch) in s[end..].char_indices() {
        if is_ident_continue(ch) || ch == 'µ' {
            ident_end = end + idx + ch.len_utf8();
        } else {
            break;
        }
    }
    if ident_end > end {
        return Ok((Token::Ident(s[..ident_end].to_string()), ident_end));
    }

    if has_dot {
        let v = s[..end]
            .parse::<f64>()
            .map_err(|e| TraceqlError::Parse(e.to_string()))?;
        Ok((Token::Float(v), end))
    } else {
        let v = s[..end]
            .parse::<i64>()
            .map_err(|e| TraceqlError::Parse(e.to_string()))?;
        Ok((Token::Int(v), end))
    }
}

fn scan_ident(s: &str, allow_dots: bool) -> (String, usize) {
    let mut end = 0;
    for (idx, ch) in s.char_indices() {
        if is_ident_continue(ch) || (allow_dots && ch == '.') {
            end = idx + ch.len_utf8();
        } else {
            break;
        }
    }
    (s[..end].to_string(), end)
}

fn keyword_or_ident(s: String) -> Token {
    match s.as_str() {
        "nil" => Token::Nil,
        "true" => Token::Bool(true),
        "false" => Token::Bool(false),
        _ => Token::Ident(s),
    }
}

fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_alphabetic()
}

fn is_ident_continue(ch: char) -> bool {
    ch == '_' || ch == '-' || ch.is_alphanumeric()
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    fn toks(s: &str) -> Vec<Token> {
        let mut t = lex(s).unwrap();
        assert!(t.pop() == Some(Token::Eof));
        t
    }

    #[test]
    fn single_equals_no_double() {
        assert!(
            toks(".http.status = 200")
                == vec![
                    Token::Dot,
                    Token::Ident("http.status".into()),
                    Token::Eq,
                    Token::Int(200),
                ]
        );
        assert!(lex("a == b").is_err());
    }

    #[test]
    fn structural_maximal_munch() {
        for (input, mid) in [
            ("a >> b", Token::Desc),
            ("a !>> b", Token::NegDesc),
            ("a &>> b", Token::UnionDesc),
            ("a !> b", Token::NegChild),
            ("a &~ b", Token::UnionSibling),
        ] {
            assert!(
                toks(input) == vec![Token::Ident("a".into()), mid, Token::Ident("b".into())],
                "input: {input}"
            );
        }
    }

    #[test]
    fn comparison_and_regex_and_ge() {
        for (input, want) in [
            (
                "x =~ \"a.*\"",
                vec![
                    Token::Ident("x".into()),
                    Token::Re,
                    Token::Str("a.*".into()),
                ],
            ),
            (
                "x !~ \"a\"",
                vec![Token::Ident("x".into()), Token::Nre, Token::Str("a".into())],
            ),
            (
                "d >= 5",
                vec![Token::Ident("d".into()), Token::Gte, Token::Int(5)],
            ),
            (
                "d <= 5",
                vec![Token::Ident("d".into()), Token::Lte, Token::Int(5)],
            ),
        ] {
            assert!(toks(input) == want, "input: {input}");
        }
    }

    #[test]
    fn colon_intrinsic_vs_dot_scope() {
        assert!(
            toks("span:duration")
                == vec![
                    Token::Ident("span".into()),
                    Token::Colon,
                    Token::Ident("duration".into()),
                ]
        );
        assert!(
            toks("span.foo")
                == vec![
                    Token::Ident("span".into()),
                    Token::Dot,
                    Token::Ident("foo".into()),
                ]
        );
    }

    #[test]
    fn literals_and_nil_and_durations() {
        for (input, want) in [
            ("nil", vec![Token::Nil]),
            ("true false", vec![Token::Bool(true), Token::Bool(false)]),
            ("1.5", vec![Token::Float(1.5)]),
            ("100ms", vec![Token::Ident("100ms".into())]),
        ] {
            assert!(toks(input) == want, "input: {input}");
        }
    }

    #[test]
    fn identifier_character_helpers_match_traceql_grammar() {
        for (ch, want) in [('_', true), ('a', true), ('1', false), ('@', false)] {
            assert!(is_ident_start(ch) == want, "ch: {ch:?}");
        }

        for (ch, want) in [('_', true), ('-', true), ('1', true), ('@', false)] {
            assert!(is_ident_continue(ch) == want, "ch: {ch:?}");
        }
    }

    #[test]
    fn leading_dot_fraction_is_single_float_preserving_zeros() {
        for (input, want) in [(".05", 0.05), (".99", 0.99), (".5", 0.5), (".009", 0.009)] {
            assert!(toks(input) == vec![Token::Float(want)], "input: {input}");
        }
    }

    #[test]
    fn leading_dot_ident_remains_dot_scope() {
        // A dot followed by an identifier must stay `Dot` + `Ident`, never a float.
        assert!(toks(".service") == vec![Token::Dot, Token::Ident("service".into())]);
        assert!(toks(".http.status") == vec![Token::Dot, Token::Ident("http.status".into())]);
    }

    #[test]
    fn lone_ampersand_lexes_as_ident_token() {
        // A bare `&` (not part of a union/and operator) lexes to an Ident("&"),
        // not a parse error. Deleting the `'&'` arm in op_token would make the
        // lexer reject it as an unexpected character.
        assert!(toks("&") == vec![Token::Ident("&".into())]);
        assert!(lex("&").is_ok());
    }

    #[test]
    fn number_scan_stops_at_a_non_dot_operator_before_a_digit() {
        // `scan_number_or_duration` only folds a `.` into the number when the
        // following char is a digit. The two `&&`s in that guard each matter:
        // weakening either to `||` makes a non-dot operator that precedes a
        // digit (e.g. the `+` in `1+2`) get swallowed into the number, so the
        // whole run is parsed as one float and fails. Asserting `1+2` lexes as
        // three tokens kills both mutants; `1.5` guards the legit-float path.
        assert!(toks("1+2") == vec![Token::Int(1), Token::Plus, Token::Int(2)]);
        assert!(toks("1.5") == vec![Token::Float(1.5)]);
    }

    #[test]
    fn advance_rejects_zero_or_out_of_bounds_progress() {
        for (pos, len, want) in [(0, 1, Some(1)), (1, 0, None), (2, 2, None)] {
            assert!(
                advance("abc", pos, len).ok() == want,
                "pos: {pos}, len: {len}"
            );
        }
    }
}
