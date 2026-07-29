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
            b'.' => {
                out.push((Token::Dot, i));
                i += 1;
            }
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
            // Every operator/punctuation lexeme. Reached last so the arms
            // above keep their claim on `--`, `/*`, quotes, `$1`, numbers and
            // words; within it, maximal munch is `punctuation`'s job.
            _ => {
                let Some((token, len)) = punctuation(bytes, i) else {
                    return Err(ParseError::new(
                        format!("unexpected character {:?}", c as char),
                        i,
                    ));
                };
                out.push((token, i));
                i += len;
            }
        }
    }
    out.push((Token::Eof, sql.len()));
    Ok(out)
}

/// Match the longest operator/punctuation lexeme starting at `bytes[i]`,
/// returning it with its byte length.
///
/// MAXIMAL MUNCH is the whole contract of this function: every spelling whose
/// first byte also begins a shorter spelling is listed longest-first — `->>`
/// before `->` before `-`, `#>>` before `#>`, `?|`/`?&` before `?`, `<@` before
/// `<=`/`<>`/`<`, `::` before `:`, `||` before a bare `|` (which is not a
/// lexeme). A slip re-reads `a->>'k'` as `a -> >'k'`, whose tail still lexes, so
/// the lexer tests pin each neighbouring shorter spelling explicitly.
///
/// `--` and `/*` are claimed by the comment arms in [`lex`] before this runs, so
/// a `-` or `/` reaching here is always the operator.
fn punctuation(bytes: &[u8], i: usize) -> Option<(Token, usize)> {
    let next_is = |byte: u8| bytes.get(i + 1) == Some(&byte);
    let next_two_are = |first: u8, second: u8| next_is(first) && bytes.get(i + 2) == Some(&second);
    Some(match bytes[i] {
        b'-' if next_two_are(b'>', b'>') => (Token::JsonGetText, 3),
        b'-' if next_is(b'>') => (Token::JsonGet, 2),
        b'#' if next_two_are(b'>', b'>') => (Token::JsonGetPathText, 3),
        b'#' if next_is(b'>') => (Token::JsonGetPath, 2),
        b'@' if next_is(b'>') => (Token::Contains, 2),
        b'<' if next_is(b'@') => (Token::ContainedBy, 2),
        b'?' if next_is(b'|') => (Token::KeyExistsAny, 2),
        b'?' if next_is(b'&') => (Token::KeyExistsAll, 2),
        b'?' => (Token::KeyExists, 1),
        b'&' if next_is(b'&') => (Token::Overlaps, 2),
        b'<' if next_is(b'=') => (Token::Le, 2),
        b'>' if next_is(b'=') => (Token::Ge, 2),
        b'<' if next_is(b'>') => (Token::Ne, 2),
        b'|' if next_is(b'|') => (Token::Concat, 2),
        b':' if next_is(b':') => (Token::TypeCast, 2),
        // A lone `:` is only grammatical inside an array slice (`a[1:2]`); it is
        // lexed so the parser can refuse slices by name rather than failing with
        // a character-level lexer error.
        b':' => (Token::Colon, 1),
        b'[' => (Token::LBracket, 1),
        b']' => (Token::RBracket, 1),
        b'(' => (Token::LParen, 1),
        b')' => (Token::RParen, 1),
        b',' => (Token::Comma, 1),
        b';' => (Token::Semicolon, 1),
        b'*' => (Token::Star, 1),
        b'+' => (Token::Plus, 1),
        b'-' => (Token::Minus, 1),
        b'/' => (Token::Slash, 1),
        b'=' => (Token::Eq, 1),
        b'<' => (Token::Lt, 1),
        b'>' => (Token::Gt, 1),
        _ => return None,
    })
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
    fn cast_operator_wins_maximal_munch_over_the_lone_colon() {
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
        // A single `:` is its own token (array-slice syntax, which the parser
        // refuses by name); it must never be mis-lexed as a cast.
        assert_eq!(
            toks("a : b"),
            vec![
                Token::Ident("a".into()),
                Token::Colon,
                Token::Ident("b".into()),
                Token::Eof,
            ]
        );
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

    #[test]
    fn jsonb_and_array_operators_lex_with_maximal_munch() {
        use assert2::assert;

        // Every new operator, written WITHOUT surrounding spaces: a munch-order
        // slip would split `->>` into `->` `>` (etc.) and the trailing operand
        // would still lex, so only the exact token vector catches it.
        let cases: &[(&str, &[Token])] = &[
            ("a->'k'", &[Token::JsonGet, Token::StringLit("k".into())]),
            (
                "a->>'k'",
                &[Token::JsonGetText, Token::StringLit("k".into())],
            ),
            (
                "a#>'{k}'",
                &[Token::JsonGetPath, Token::StringLit("{k}".into())],
            ),
            (
                "a#>>'{k}'",
                &[Token::JsonGetPathText, Token::StringLit("{k}".into())],
            ),
            ("a@>b", &[Token::Contains, Token::Ident("b".into())]),
            ("a<@b", &[Token::ContainedBy, Token::Ident("b".into())]),
            ("a?'k'", &[Token::KeyExists, Token::StringLit("k".into())]),
            ("a?|b", &[Token::KeyExistsAny, Token::Ident("b".into())]),
            ("a?&b", &[Token::KeyExistsAll, Token::Ident("b".into())]),
            ("a&&b", &[Token::Overlaps, Token::Ident("b".into())]),
            (
                "a[1]",
                &[Token::LBracket, Token::IntLit("1".into()), Token::RBracket],
            ),
        ];
        for (sql, tail) in cases {
            let mut expected = vec![Token::Ident("a".into())];
            expected.extend_from_slice(tail);
            expected.push(Token::Eof);
            assert!(toks(sql) == expected, "lexing {sql:?}");
        }
    }

    #[test]
    fn new_operators_do_not_steal_the_shorter_spellings_they_share_a_prefix_with() {
        use assert2::assert;

        // `->` must not swallow the subtraction in `a-1`, `<@` must not shadow
        // `<=`/`<>`/`<`, and `||` must still be Concat now that `?|` exists.
        let cases: &[(&str, &[Token])] = &[
            ("a-1", &[Token::Minus, Token::IntLit("1".into())]),
            ("a<=b", &[Token::Le, Token::Ident("b".into())]),
            ("a<b", &[Token::Lt, Token::Ident("b".into())]),
            ("a<>b", &[Token::Ne, Token::Ident("b".into())]),
            ("a>=b", &[Token::Ge, Token::Ident("b".into())]),
            ("a>b", &[Token::Gt, Token::Ident("b".into())]),
            ("a||b", &[Token::Concat, Token::Ident("b".into())]),
            ("a::b", &[Token::TypeCast, Token::Ident("b".into())]),
        ];
        for (sql, tail) in cases {
            let mut expected = vec![Token::Ident("a".into())];
            expected.extend_from_slice(tail);
            expected.push(Token::Eof);
            assert!(toks(sql) == expected, "lexing {sql:?}");
        }
        // A `--` line comment still wins over `->`-style munching.
        assert!(toks("a --x") == vec![Token::Ident("a".into()), Token::Eof]);
    }

    #[test]
    fn lone_hash_and_ampersand_remain_unexpected_characters() {
        use assert2::assert;

        for sql in ["a # b", "a & b", "a | b", "a @ b"] {
            let e = lex(sql).expect_err("lone operator byte");
            assert!(e.message.contains("unexpected character"), "{sql}");
        }
    }

    #[test]
    fn array_keyword_lexes_as_a_keyword() {
        use assert2::assert;

        assert!(
            toks("ARRAY[1]")
                == vec![
                    Token::Keyword(Keyword::Array),
                    Token::LBracket,
                    Token::IntLit("1".into()),
                    Token::RBracket,
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
