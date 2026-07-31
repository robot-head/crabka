use assert2::assert;
use crabka_pgparser::parser::parse_expr_for_test as pexpr;
use crabka_pgtypes::{ColumnType, Datum};

use crate::{clock::EvalCtx, scope::Scope};

/// Evaluate a FROM-less expression and render it the way the wire would.
fn text_of(sql: &str) -> String {
    let ctx = EvalCtx::test_default();
    let value =
        crate::eval::eval(&pexpr(sql).expect("parse"), &Scope::empty(), &[], &ctx).expect("eval");
    match value {
        Datum::Null => "<null>".to_string(),
        other => crate::func::text_render(&other, &ctx.time_zone),
    }
}

/// The SQLSTATE and message a failing expression reports, taken from whichever
/// of the plan-time (`infer_type`) and run-time (`eval`) paths rejects it —
/// exactly the order a real statement goes through.
fn error_of(sql: &str) -> (String, String) {
    let ctx = EvalCtx::test_default();
    let expr = pexpr(sql).expect("parse");
    let scope = Scope::empty();
    let error = match crate::eval::infer_type(&expr, &scope) {
        Err(e) => e,
        Ok(_) => crate::eval::eval(&expr, &scope, &[], &ctx).expect_err("expected an error"),
    }
    .into_pg();
    (error.code, error.message)
}

fn sqlstate(sql: &str) -> String {
    error_of(sql).0
}

fn message(sql: &str) -> String {
    error_of(sql).1
}

fn result_type(sql: &str) -> ColumnType {
    crate::eval::infer_type(&pexpr(sql).expect("parse"), &Scope::empty()).expect("infer")
}

#[test]
fn format_expands_every_conversion() {
    let cases = [
        ("format('%s and %s', 'a', 1)", "a and 1"),
        ("format('%I', 'foo bar')", "\"foo bar\""),
        ("format('%L', 'a''b')", "'a''b'"),
        ("format('%L', NULL::text)", "NULL"),
        ("format('%%')", "%"),
        ("format('%2$s %1$s', 'a', 'b')", "b a"),
        ("format('%1$s %1$s', 'a')", "a a"),
        ("format('%s', NULL)", ""),
        ("format('hello')", "hello"),
        ("format('[%10s]', 'a')", "[         a]"),
        ("format('[%-10s]', 'a')", "[a         ]"),
        ("format('[%5L]', 'a')", "[  'a']"),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
    assert!(text_of("format(NULL)") == "<null>");
}

#[test]
fn format_diagnostics_match_postgres() {
    let cases = [
        (
            "format('%s %s', 'a')",
            "22023",
            "too few arguments for format()",
        ),
        (
            "format('%z', 'a')",
            "22023",
            "unrecognized format() type specifier \"z\"",
        ),
        (
            "format('%I', NULL)",
            "22004",
            "null values cannot be formatted as an SQL identifier",
        ),
        (
            "format('%0$s', 'a')",
            "22023",
            "format specifies argument 0, but arguments are numbered from 1",
        ),
        (
            "format('%')",
            "22023",
            "unterminated format() type specifier",
        ),
    ];
    for (sql, code, text) in cases {
        assert!(sqlstate(sql) == code, "{sql}");
        assert!(message(sql) == text, "{sql}");
    }
}

#[test]
fn digests_match_the_published_vectors() {
    let cases = [
        ("md5('')", "d41d8cd98f00b204e9800998ecf8427e"),
        ("md5('abc')", "900150983cd24fb0d6963f7d28e17f72"),
        (
            "encode(sha224('abc'), 'hex')",
            "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7",
        ),
        (
            "encode(sha256('abc'), 'hex')",
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        ),
        (
            "encode(sha384('abc'), 'hex')",
            "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7",
        ),
        (
            "encode(sha512('abc'), 'hex')",
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f",
        ),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
}

#[test]
fn encode_and_decode_round_trip_every_encoding() {
    let cases = [
        ("encode(decode('616263', 'hex'), 'hex')", "616263"),
        ("encode(decode('616263', 'hex'), 'base64')", "YWJj"),
        ("encode(decode('616263', 'hex'), 'escape')", "abc"),
        ("encode(decode('00ff41', 'hex'), 'escape')", "\\000\\377A"),
        ("encode(decode('00ff41', 'hex'), 'base64')", "AP9B"),
        ("decode('616263', 'hex')", "\\x616263"),
        ("decode('YWJj', 'base64')", "\\x616263"),
        ("decode('abc', 'escape')", "\\x616263"),
        ("encode(decode('', 'hex'), 'hex')", ""),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
    assert!(sqlstate("encode(decode('61', 'hex'), 'nope')") == "22023");
    assert!(message("encode(decode('61', 'hex'), 'nope')") == "unrecognized encoding: \"nope\"");
    assert!(sqlstate("decode('zz', 'hex')") == "22023");
    assert!(message("decode('zz', 'hex')") == "invalid hexadecimal digit: \"z\"");
}

#[test]
fn to_hex_prints_the_two_complement_pattern() {
    let cases = [
        ("to_hex(255)", "ff"),
        ("to_hex(255::int8)", "ff"),
        ("to_hex(0)", "0"),
        ("to_hex(-1)", "ffffffff"),
        ("to_hex((-1)::int8)", "ffffffffffffffff"),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
}

/// `quote_ident` leaves a value bare only when re-parsing it unquoted would
/// yield the same identifier — so keywords and anything outside `[a-z0-9_]`
/// come back quoted.
#[test]
fn quoting_matches_postgres() {
    let cases = [
        ("quote_ident('foo')", "foo"),
        ("quote_ident('_x')", "_x"),
        ("quote_ident('x1')", "x1"),
        ("quote_ident('foo bar')", "\"foo bar\""),
        ("quote_ident('Foo')", "\"Foo\""),
        ("quote_ident('a\"b')", "\"a\"\"b\""),
        ("quote_ident('select')", "\"select\""),
        // `value` is an UNRESERVED keyword, so PostgreSQL leaves it bare.
        ("quote_ident('value')", "value"),
        ("quote_ident('1x')", "\"1x\""),
        ("quote_ident('')", "\"\""),
        ("quote_literal('a''b')", "'a''b'"),
        ("quote_literal(42)", "'42'"),
        ("quote_literal(NULL)", "<null>"),
        ("quote_nullable('a')", "'a'"),
        ("quote_nullable(NULL)", "NULL"),
        ("quote_nullable(42)", "'42'"),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
}

#[test]
fn search_and_rewrite_helpers_match_postgres() {
    let cases = [
        ("split_part('a,b,c', ',', 2)", "b"),
        ("split_part('a,b,c', ',', -1)", "c"),
        ("split_part('a,b,c', ',', 9)", ""),
        ("split_part('a,b,c', ',', -9)", ""),
        ("split_part('abc', '', 1)", "abc"),
        ("translate('abcdef', 'abc', 'xy')", "xydef"),
        ("translate('abc', '', '')", "abc"),
        ("translate('12345', '143', 'ax')", "a2x5"),
        ("starts_with('abc', 'ab')", "t"),
        ("starts_with('abc', 'b')", "f"),
        ("starts_with('abc', '')", "t"),
        ("octet_length('abc')", "3"),
        ("bit_length('abc')", "24"),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
    assert!(sqlstate("split_part('a,b', ',', 0)") == "22023");
    assert!(message("split_part('a,b', ',', 0)") == "field position must not be zero");
}

#[test]
fn concat_and_concat_ws_skip_nulls() {
    let cases = [
        ("concat('a')", "a"),
        ("concat(1, 2, NULL, 'a')", "12a"),
        // `concat` renders through the OUTPUT function, so a bool is `t`/`f`.
        ("concat(true, false)", "tf"),
        ("concat_ws('-', 'a', 'b')", "a-b"),
        ("concat_ws('-', 1, NULL, 'a', true)", "1-a-t"),
        ("concat_ws('-', NULL, NULL)", ""),
        ("concat_ws(NULL, 1, 2)", "<null>"),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
    // PostgreSQL has no zero-argument candidate for either.
    assert!(sqlstate("concat()") == "42883");
    assert!(message("concat()") == "function concat() does not exist");
    assert!(sqlstate("concat_ws('-')") == "42883");
    assert!(message("concat_ws('-')") == "function concat_ws(unknown) does not exist");
}

#[test]
fn unicode_helpers_match_postgres() {
    let cases = [
        ("unistr('d\\0061t\\+000061')", "data"),
        ("unistr('a\\\\b')", "a\\b"),
        ("normalize('abc')", "abc"),
        ("is_normalized('abc')", "t"),
        ("is_normalized('a', 'NFC')", "t"),
        ("is_normalized('a', 'NFD')", "t"),
        ("parse_ident('a.b')", "{a,b}"),
        ("parse_ident('\"A\".b')", "{A,b}"),
        ("parse_ident('a.b.c.d')", "{a,b,c,d}"),
        ("parse_ident('  x . y  ')", "{x,y}"),
        // An unquoted part is folded to lower case, as the lexer folds it.
        ("parse_ident('Foo.Bar')", "{foo,bar}"),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
    assert!(sqlstate("unistr('bad\\')") == "42601");
    assert!(sqlstate("parse_ident('1abc')") == "22023");
    assert!(message("parse_ident('1abc')") == "string is not a valid identifier: \"1abc\"");
    assert!(sqlstate("parse_ident('a.b[]', true)") == "22023");
    assert!(sqlstate("is_normalized('a', 'NFZ')") == "22023");
    assert!(message("is_normalized('a', 'NFZ')") == "invalid normalization form: NFZ");
    // crabka is UTF-8 only, so `to_ascii` always raises PostgreSQL's 0A000.
    assert!(sqlstate("to_ascii('abc')") == "0A000");
}

#[test]
fn result_types_match_postgres() {
    let cases = [
        ("format('%s', 1)", ColumnType::Text),
        ("concat_ws('-', 'a')", ColumnType::Text),
        ("md5('a')", ColumnType::Text),
        ("sha256('a')", ColumnType::Bytea),
        ("encode(decode('61', 'hex'), 'hex')", ColumnType::Text),
        ("decode('61', 'hex')", ColumnType::Bytea),
        ("to_hex(1)", ColumnType::Text),
        ("quote_ident('a')", ColumnType::Text),
        ("split_part('a', 'b', 1)", ColumnType::Text),
        ("starts_with('a', 'a')", ColumnType::Bool),
        ("is_normalized('a')", ColumnType::Bool),
        (
            "parse_ident('a')",
            ColumnType::Array(crabka_pgtypes::ElemType::Text),
        ),
        ("octet_length('a')", ColumnType::Int4),
        ("bit_length('a')", ColumnType::Int4),
    ];
    for (sql, expected) in cases {
        assert!(result_type(sql) == expected, "{sql}");
    }
}

/// Every function here except `format`, `concat_ws` and `quote_nullable` is
/// strict; those three have their own documented NULL behavior.
#[test]
fn strictness_matches_postgres() {
    let null_returning = [
        "md5(NULL)",
        "sha256(NULL)",
        "encode(NULL, 'hex')",
        "decode(NULL, 'hex')",
        "to_hex(NULL)",
        "quote_ident(NULL)",
        "quote_literal(NULL)",
        "split_part(NULL, ',', 1)",
        "translate(NULL, 'a', 'b')",
        "starts_with(NULL, 'a')",
        "unistr(NULL)",
        "parse_ident(NULL)",
        "normalize(NULL)",
        "is_normalized(NULL)",
        "octet_length(NULL)",
        "concat_ws(NULL, 'a')",
        "format(NULL)",
    ];
    for sql in null_returning {
        assert!(text_of(sql) == "<null>", "{sql}");
    }
    assert!(text_of("quote_nullable(NULL)") == "NULL");
}
