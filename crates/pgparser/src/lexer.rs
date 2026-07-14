//! Hand-written lexer. Produces (Token, byte-offset) pairs; offsets feed
//! 42601 error positions. Integer literals only (the slice has no float type).

use crate::{
    error::ParseError,
    token::{Keyword, Token},
};

/// Tokenize SQL text and preserve each token's byte offset.
///
/// # Errors
///
/// Returns a parse error for malformed literals, identifiers, comments, or
/// unsupported token forms.
pub fn lex(sql: &str) -> Result<Vec<(Token, usize)>, ParseError> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let start = i;
                let mut depth = 1usize;
                i += 2;
                while i < bytes.len() {
                    if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
                        depth += 1;
                        i += 2;
                        continue;
                    }
                    if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                        depth -= 1;
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                        continue;
                    }
                    i += 1;
                }
                if depth != 0 {
                    return Err(ParseError::new("unterminated block comment", start));
                }
            }
            b'\'' => {
                let start = i;
                i += 1;
                let mut s = String::new();
                loop {
                    match bytes.get(i) {
                        None => return Err(ParseError::new("unterminated string literal", start)),
                        Some(&b'\'') if bytes.get(i + 1) == Some(&b'\'') => {
                            s.push('\'');
                            i += 2;
                        }
                        Some(&b'\'') => {
                            i += 1;
                            break;
                        }
                        Some(&b) => {
                            s.push(b as char);
                            i += 1;
                        }
                    }
                }
                out.push((Token::StringLit(s), start));
            }
            b'"' => {
                let start = i;
                i += 1;
                let mut s = String::new();
                loop {
                    match bytes.get(i) {
                        None => {
                            return Err(ParseError::new("unterminated quoted identifier", start));
                        }
                        Some(&b'"') if bytes.get(i + 1) == Some(&b'"') => {
                            s.push('"');
                            i += 2;
                        }
                        Some(&b'"') => {
                            i += 1;
                            break;
                        }
                        Some(&b) => {
                            s.push(b as char);
                            i += 1;
                        }
                    }
                }
                out.push((Token::Ident(s), start));
            }
            b'<' if bytes.get(i + 1) == Some(&b'=') => {
                out.push((Token::Le, i));
                i += 2;
            }
            b'>' if bytes.get(i + 1) == Some(&b'=') => {
                out.push((Token::Ge, i));
                i += 2;
            }
            b'<' if bytes.get(i + 1) == Some(&b'>') => {
                out.push((Token::Ne, i));
                i += 2;
            }
            b'|' if bytes.get(i + 1) == Some(&b'|') => {
                out.push((Token::Concat, i));
                i += 2;
            }
            // SP31: `::` is the cast operator. A lone `:` is not a token in this
            // slice (no array slices / named-parameter `:=`), so it falls through
            // to the "unexpected character" arm.
            b':' if bytes.get(i + 1) == Some(&b':') => {
                out.push((Token::TypeCast, i));
                i += 2;
            }
            b'(' => push1(&mut out, Token::LParen, &mut i),
            b')' => push1(&mut out, Token::RParen, &mut i),
            b',' => push1(&mut out, Token::Comma, &mut i),
            b';' => push1(&mut out, Token::Semicolon, &mut i),
            b'*' => push1(&mut out, Token::Star, &mut i),
            b'+' => push1(&mut out, Token::Plus, &mut i),
            b'-' => push1(&mut out, Token::Minus, &mut i),
            b'/' => push1(&mut out, Token::Slash, &mut i),
            b'=' => push1(&mut out, Token::Eq, &mut i),
            b'<' => push1(&mut out, Token::Lt, &mut i),
            b'>' => push1(&mut out, Token::Gt, &mut i),
            b'$' if bytes.get(i + 1).is_some_and(u8::is_ascii_digit) => {
                let start = i;
                i += 1;
                let ds = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                let n: u32 = sql[ds..i]
                    .parse()
                    .map_err(|_| ParseError::new("parameter number out of range", start))?;
                out.push((Token::Param(n), start));
            }
            // SP30: a numeric literal — an integer, or a `float8` literal if it has a
            // fractional part (`.`) or an exponent (`e`/`E`). A leading `.` only starts
            // a number when a digit follows; a bare `.` falls through to the SP33 Dot arm.
            c if c.is_ascii_digit()
                || (c == b'.' && bytes.get(i + 1).is_some_and(u8::is_ascii_digit)) =>
            {
                let start = i;
                let mut is_float = false;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                if i < bytes.len() && bytes[i] == b'.' {
                    is_float = true;
                    i += 1;
                    while i < bytes.len() && bytes[i].is_ascii_digit() {
                        i += 1;
                    }
                }
                if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
                    // Consume the exponent only if a (signed) digit run actually
                    // follows; otherwise leave `e` for the identifier lexer.
                    let mut j = i + 1;
                    if matches!(bytes.get(j), Some(b'+' | b'-')) {
                        j += 1;
                    }
                    if bytes.get(j).is_some_and(u8::is_ascii_digit) {
                        is_float = true;
                        i = j;
                        while i < bytes.len() && bytes[i].is_ascii_digit() {
                            i += 1;
                        }
                    }
                }
                let text = sql[start..i].to_string();
                if is_float {
                    out.push((Token::FloatLit(text), start));
                } else {
                    out.push((Token::IntLit(text), start));
                }
            }
            // SP33: a `.` that does not begin a number lexeme is the qualified-name
            // separator. The numeric arm above already claimed `.5`/`2.`, so any `.`
            // reaching here is a separator (`a.col`).
            b'.' => push1(&mut out, Token::Dot, &mut i),
            c if c == b'_' || c.is_ascii_alphabetic() => {
                let start = i;
                while i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphanumeric()) {
                    i += 1;
                }
                let word = sql[start..i].to_ascii_lowercase();
                let tok = match Keyword::from_word(&word) {
                    Some(kw) => Token::Keyword(kw),
                    None => Token::Ident(word),
                };
                out.push((tok, start));
            }
            _ => {
                return Err(ParseError::new(
                    format!("unexpected character {:?}", c as char),
                    i,
                ));
            }
        }
    }
    out.push((Token::Eof, sql.len()));
    Ok(out)
}

fn push1(out: &mut Vec<(Token, usize)>, tok: Token, i: &mut usize) {
    out.push((tok, *i));
    *i += 1;
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::token::{Keyword, Token};

    fn toks(sql: &str) -> Vec<Token> {
        lex(sql).expect("lex").into_iter().map(|(t, _)| t).collect()
    }

    #[test]
    fn keywords_idents_literals() {
        assert_eq!(
            toks("SELECT id FROM t WHERE x = 'a'"),
            vec![
                Token::Keyword(Keyword::Select),
                Token::Ident("id".into()),
                Token::Keyword(Keyword::From),
                Token::Ident("t".into()),
                Token::Keyword(Keyword::Where),
                Token::Ident("x".into()),
                Token::Eq,
                Token::StringLit("a".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn keywords_are_case_insensitive_idents_lowercased() {
        assert_eq!(toks("Select FOO")[0], Token::Keyword(Keyword::Select));
        assert_eq!(toks("Select FOO")[1], Token::Ident("foo".into()));
    }

    #[test]
    fn quoted_ident_preserves_case() {
        assert_eq!(toks("\"MixedCase\"")[0], Token::Ident("MixedCase".into()));
    }

    #[test]
    fn quoted_ident_escapes_doubled_quote() {
        assert_eq!(toks("\"a\"\"b\"")[0], Token::Ident("a\"b".into()));
    }

    #[test]
    fn string_escaping_doubles_quote() {
        assert_eq!(toks("'it''s'")[0], Token::StringLit("it's".into()));
    }

    #[test]
    fn comments_are_skipped() {
        assert_eq!(
            toks("1 -- c\n+ /* x */ 2"),
            vec![
                Token::IntLit("1".into()),
                Token::Plus,
                Token::IntLit("2".into()),
                Token::Eof
            ]
        );
    }

    #[test]
    fn operators_lex() {
        assert_eq!(
            toks("<= >= <> < > = + - * / ( ) , ;"),
            vec![
                Token::Le,
                Token::Ge,
                Token::Ne,
                Token::Lt,
                Token::Gt,
                Token::Eq,
                Token::Plus,
                Token::Minus,
                Token::Star,
                Token::Slash,
                Token::LParen,
                Token::RParen,
                Token::Comma,
                Token::Semicolon,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn concat_operator_lexes_and_a_lone_pipe_is_rejected() {
        // `||` is one token; with no surrounding spaces a slip in the two-byte
        // advance would mis-read the next byte.
        assert_eq!(
            toks("a||b"),
            vec![
                Token::Ident("a".into()),
                Token::Concat,
                Token::Ident("b".into()),
                Token::Eof,
            ]
        );
        // A single `|` is not a token in this slice (no bitwise-or).
        let e = lex("a | b").expect_err("lone pipe");
        assert!(e.message.contains("unexpected character"));
    }

    #[test]
    fn cast_operator_lexes_and_a_lone_colon_is_rejected() {
        // `::` is one token; with no surrounding spaces a slip in the two-byte
        // advance would mis-read the next byte.
        assert_eq!(
            toks("x::int4"),
            vec![
                Token::Ident("x".into()),
                Token::TypeCast,
                Token::Ident("int4".into()),
                Token::Eof,
            ]
        );
        // A single `:` is not a token in this slice.
        let e = lex("a : b").expect_err("lone colon");
        assert!(e.message.contains("unexpected character"));
    }

    #[test]
    fn float_literals_lex_distinctly_from_ints() {
        // Fractional, leading-dot, trailing-dot, and exponent forms are FloatLit;
        // a bare integer stays IntLit.
        assert_eq!(toks("42"), vec![Token::IntLit("42".into()), Token::Eof]);
        assert_eq!(toks("1.5"), vec![Token::FloatLit("1.5".into()), Token::Eof]);
        assert_eq!(toks(".5"), vec![Token::FloatLit(".5".into()), Token::Eof]);
        assert_eq!(toks("2."), vec![Token::FloatLit("2.".into()), Token::Eof]);
        assert_eq!(
            toks("1e10"),
            vec![Token::FloatLit("1e10".into()), Token::Eof]
        );
        assert_eq!(
            toks("1.5E-3"),
            vec![Token::FloatLit("1.5E-3".into()), Token::Eof]
        );
        assert_eq!(
            toks("6e+2"),
            vec![Token::FloatLit("6e+2".into()), Token::Eof]
        );
        // `1 + 2.5` keeps the operator separate from the float.
        assert_eq!(
            toks("1 + 2.5"),
            vec![
                Token::IntLit("1".into()),
                Token::Plus,
                Token::FloatLit("2.5".into()),
                Token::Eof,
            ]
        );
        // A bare `.` with no following digit is now the SP33 Dot token (qualified-name
        // separator), not an error. The numeric arm only claims `.` when a digit follows.
        assert_eq!(toks("."), vec![Token::Dot, Token::Eof]);
        // An `e` not followed by a (signed) digit is left for the identifier lexer.
        assert_eq!(
            toks("3 e"),
            vec![
                Token::IntLit("3".into()),
                Token::Ident("e".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn unterminated_string_errors_with_position() {
        let e = lex("'abc").expect_err("unterminated");
        assert_eq!(e.position, 0);
    }

    #[test]
    fn lexes_parameter_placeholder() {
        assert_eq!(toks("$1")[0], Token::Param(1));
    }

    #[test]
    fn two_char_operators_advance_exactly_two_bytes() {
        // No surrounding spaces: a position-arithmetic slip in the two-byte
        // advance would mis-read the following byte as its own token.
        assert_eq!(
            toks("1<=2"),
            vec![
                Token::IntLit("1".into()),
                Token::Le,
                Token::IntLit("2".into()),
                Token::Eof
            ]
        );
        assert_eq!(
            toks("1>=2"),
            vec![
                Token::IntLit("1".into()),
                Token::Ge,
                Token::IntLit("2".into()),
                Token::Eof
            ]
        );
        assert_eq!(
            toks("1<>2"),
            vec![
                Token::IntLit("1".into()),
                Token::Ne,
                Token::IntLit("2".into()),
                Token::Eof
            ]
        );
    }

    #[test]
    fn line_comment_runs_to_eof_without_a_newline() {
        // A `--` comment with no trailing newline must end cleanly at EOF, never
        // reading one byte past the buffer.
        assert_eq!(toks("1 --eof"), vec![Token::IntLit("1".into()), Token::Eof]);
    }

    #[test]
    fn block_comment_at_start_of_input() {
        // A `/* */` comment at offset 0 exercises the comment-open advance from
        // the very first byte.
        assert_eq!(
            toks("/* c */1"),
            vec![Token::IntLit("1".into()), Token::Eof]
        );
    }

    #[test]
    fn block_comment_with_internal_star_only_closes_at_star_slash() {
        // A lone `*` inside a block comment is NOT the terminator — only `*/` is.
        assert_eq!(
            toks("/* a * b */1"),
            vec![Token::IntLit("1".into()), Token::Eof]
        );
    }

    #[test]
    fn nested_block_comments_are_skipped() {
        assert_eq!(
            toks("1 /* outer /* inner */ still outer */ + 2"),
            vec![
                Token::IntLit("1".into()),
                Token::Plus,
                Token::IntLit("2".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn unterminated_block_comment_errors_at_its_start() {
        let e = lex("/* c").expect_err("unterminated block comment");
        assert!(e.message.contains("unterminated block comment"));
        assert_eq!(e.position, 0);
    }

    #[test]
    fn unterminated_nested_block_comment_errors_at_outer_start() {
        let e = lex("1 + /* outer /* inner */ 2").expect_err("unterminated nested block comment");
        assert!(e.message.contains("unterminated block comment"));
        assert_eq!(e.position, 4);
    }

    #[test]
    fn unterminated_block_comment_ending_in_a_star_does_not_read_past_eof() {
        // A trailing `*` makes the `bytes[i] == b'*'` half of the terminator
        // check true, so the scan would read `bytes[i + 1]` — its `i + 1 < len`
        // bound is what stops that from running off the end. ("/* c" can't catch
        // this: 'c' is not `*`, so the `&&` short-circuits before bytes[i + 1].)
        let e = lex("/* *").expect_err("unterminated block comment");
        assert!(e.message.contains("unterminated block comment"));
        assert_eq!(e.position, 0);
    }

    #[test]
    fn lone_dollar_is_an_unexpected_character_not_a_bad_param() {
        // `$` only begins a parameter when a digit follows; otherwise it is an
        // unexpected character (this lexer has no dollar-quoting).
        let e = lex("$x").expect_err("$x is not a token");
        assert!(e.message.contains("unexpected character"));
        assert_eq!(e.position, 0);
    }

    #[test]
    fn dot_is_a_token_between_identifiers() {
        use crate::token::Token;
        assert_eq!(
            toks("a.col"),
            vec![
                Token::Ident("a".into()),
                Token::Dot,
                Token::Ident("col".into()),
                Token::Eof
            ]
        );
    }

    #[test]
    fn dot_does_not_disturb_numeric_literals() {
        use crate::token::Token;
        // A leading/trailing-dot float is still one FloatLit, not <int> Dot.
        assert_eq!(toks(".5"), vec![Token::FloatLit(".5".into()), Token::Eof]);
        assert_eq!(toks("2."), vec![Token::FloatLit("2.".into()), Token::Eof]);
    }

    #[test]
    fn join_keywords_lex() {
        use crate::token::{Keyword, Token};
        assert_eq!(
            toks("INNER JOIN ON USING NATURAL LEFT RIGHT FULL OUTER CROSS"),
            vec![
                Token::Keyword(Keyword::Inner),
                Token::Keyword(Keyword::Join),
                Token::Keyword(Keyword::On),
                Token::Keyword(Keyword::Using),
                Token::Keyword(Keyword::Natural),
                Token::Keyword(Keyword::Left),
                Token::Keyword(Keyword::Right),
                Token::Keyword(Keyword::Full),
                Token::Keyword(Keyword::Outer),
                Token::Keyword(Keyword::Cross),
                Token::Eof,
            ]
        );
    }

    proptest! {
        #[test]
        fn lex_never_panics(s: String) {
            // The lexer must never panic on arbitrary (valid-UTF-8) input —
            // it returns Ok(tokens) or Err(ParseError), never unwinds.
            let _ = lex(&s);
        }
    }
}
