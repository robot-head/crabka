//! Every expected value here is PostgreSQL 18.4's, taken from the statements
//! in `src/test/regress/sql/strings.sql` that exercise the `bytea` surface.

use assert2::assert;
use crabka_pgparser::parser::parse_expr_for_test as pexpr;
use crabka_pgtypes::{
    ColumnType, Datum,
    encoding::{ByteaOutput, OutputStyle},
};

use crate::{clock::EvalCtx, scope::Scope};

/// Evaluate a FROM-less expression and render it as the wire would under
/// `bytea_output`.
fn text_of_in(sql: &str, bytea_output: ByteaOutput) -> String {
    let ctx = EvalCtx {
        bytea_output,
        ..EvalCtx::test_default()
    };
    let value =
        crate::eval::eval(&pexpr(sql).expect("parse"), &Scope::empty(), &[], &ctx).expect("eval");
    match value {
        Datum::Null => "<null>".to_string(),
        other => String::from_utf8(crabka_pgtypes::encoding::encode_text_in(
            &other,
            OutputStyle {
                bytea_output,
                ..ctx.output_style()
            },
        ))
        .expect("the corpus renders as UTF-8"),
    }
}

/// As [`text_of_in`], in PostgreSQL's default `hex` spelling.
fn text_of(sql: &str) -> String {
    text_of_in(sql, ByteaOutput::Hex)
}

/// The static result type the plan gate infers.
fn type_of(sql: &str) -> ColumnType {
    crate::eval::infer_type(&pexpr(sql).expect("parse"), &Scope::empty()).expect("infer")
}

/// The SQLSTATE and message a failing expression reports, from whichever of the
/// plan-time or run-time path rejects it.
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

#[test]
fn byteaout_spells_the_same_value_two_ways() {
    // `SET bytea_output TO escape` and back, over strings.sql's six literals.
    for (sql, hex, escape) in [
        (r"E'\\xDeAdBeEf'::bytea", r"\xdeadbeef", r"\336\255\276\357"),
        (
            r"E'\\x De Ad Be Ef '::bytea",
            r"\xdeadbeef",
            r"\336\255\276\357",
        ),
        (r"E'\\xDe00BeEf'::bytea", r"\xde00beef", r"\336\000\276\357"),
        (r"E'DeAdBeEf'::bytea", r"\x4465416442654566", "DeAdBeEf"),
        (
            r"E'De\\000dBeEf'::bytea",
            r"\x4465006442654566",
            r"De\000dBeEf",
        ),
        (r"E'De\\123dBeEf'::bytea", r"\x4465536442654566", "DeSdBeEf"),
    ] {
        assert!(text_of_in(sql, ByteaOutput::Hex) == hex, "{sql}");
        assert!(text_of_in(sql, ByteaOutput::Escape) == escape, "{sql}");
    }
}

#[test]
fn escape_output_doubles_a_backslash_and_leaves_printable_ascii_alone() {
    let value = Datum::Bytea(vec![b'\\', b' ', b'~', 0x7f, 0x80]);
    let utc = jiff::tz::TimeZone::UTC;
    let style = OutputStyle {
        bytea_output: ByteaOutput::Escape,
        ..OutputStyle::with_zone(&utc)
    };
    let rendered = String::from_utf8(crabka_pgtypes::encoding::encode_text_in(&value, style))
        .expect("ASCII and octal escapes");
    // 0x7f is unprintable despite being below the high half, and 0x80 opens it.
    assert!(rendered == r"\\ ~\177\200");
}

#[test]
fn substring_counts_bytes_and_runs_to_the_end_on_an_overflowing_length() {
    for (sql, expected) in [
        (r"SUBSTRING('1234567890'::bytea FROM 3)", "34567890"),
        (r"SUBSTRING('1234567890'::bytea FROM 4 FOR 3)", "456"),
        // `2 + 2147483646` overflows int32 in PostgreSQL, which runs the window
        // to the end rather than raising.
        (r"SUBSTRING('string'::bytea FROM 2 FOR 2147483646)", "tring"),
        (
            r"SUBSTRING('string'::bytea FROM -10 FOR 2147483646)",
            "string",
        ),
        // A window entirely before the value is empty, not an error.
        (r"SUBSTRING('string'::bytea FROM -10 FOR 5)", ""),
    ] {
        assert!(text_of_in(sql, ByteaOutput::Escape) == expected, "{sql}");
    }
}

/// The difference that makes `bytea` its own overload rather than a cast away
/// from `text`: one two-byte character is two `substring` positions.
#[test]
fn substring_over_bytea_splits_a_multibyte_character_that_text_keeps_whole() {
    assert!(text_of("SUBSTRING('é'::text FROM 1 FOR 1)") == "é");
    assert!(text_of("SUBSTRING('é'::bytea FROM 1 FOR 1)") == r"\xc3");
    assert!(text_of("length('é'::text)") == "1");
    assert!(text_of("length('é'::bytea)") == "2");
}

#[test]
fn substring_refuses_a_negative_length() {
    let (sqlstate, message) = error_of("SUBSTRING('string'::bytea FROM -10 FOR -2147483646)");
    assert!(sqlstate == "22011");
    assert!(message == "negative substring length not allowed");
}

#[test]
fn position_counts_bytes_from_one_and_finds_the_empty_needle_first() {
    for (sql, expected) in [
        (r"POSITION('\x11'::bytea IN ''::bytea)", "0"),
        (r"POSITION('\x33'::bytea IN '\x1122'::bytea)", "0"),
        (r"POSITION(''::bytea IN '\x1122'::bytea)", "1"),
        (r"POSITION('\x22'::bytea IN '\x1122'::bytea)", "2"),
        (r"POSITION('\x5678'::bytea IN '\x1234567890'::bytea)", "3"),
    ] {
        assert!(text_of(sql) == expected, "{sql}");
    }
}

#[test]
fn trim_strips_the_byte_set_from_the_ends_the_spelling_names() {
    for (sql, expected) in [
        (r"trim(E'\\000'::bytea from E'\\000Tom\\000'::bytea)", "Tom"),
        (
            r"trim(leading E'\\000'::bytea from E'\\000Tom\\000'::bytea)",
            r"Tom\000",
        ),
        (
            r"trim(trailing E'\\000'::bytea from E'\\000Tom\\000'::bytea)",
            r"\000Tom",
        ),
        (r"btrim(E'\\000trim\\000'::bytea, E'\\000'::bytea)", "trim"),
        (r"btrim(''::bytea, E'\\000'::bytea)", ""),
        // An empty set strips nothing.
        (
            r"btrim(E'\\000trim\\000'::bytea, ''::bytea)",
            r"\000trim\000",
        ),
    ] {
        assert!(text_of_in(sql, ByteaOutput::Escape) == expected, "{sql}");
    }
}

#[test]
fn reverse_over_bytea_reverses_bytes() {
    for (sql, expected) in [
        (r"reverse(''::bytea)", r"\x"),
        (r"reverse('\xaa'::bytea)", r"\xaa"),
        (r"reverse('\xabcd'::bytea)", r"\xcdab"),
    ] {
        assert!(text_of(sql) == expected, "{sql}");
    }
    assert!(type_of(r"reverse('\xabcd'::bytea)") == ColumnType::Bytea);
    // The text overload is untouched, and it reverses characters.
    assert!(text_of("reverse('abc')") == "cba");
}

/// A wrong arity must reach the arity check, not an argument read. Selecting
/// the `bytea` overload from a match guard indexed `args[0]` before the check
/// and panicked the backend on a bare `reverse()`.
#[test]
fn a_bytea_overload_never_reads_an_argument_before_checking_the_arity() {
    // `position` and `overlay` have their own grammar, so a bare pair of
    // parentheses never reaches name resolution for those two.
    for sql in ["reverse()", "btrim()", "ltrim()", "rtrim()", "substr()"] {
        let (sqlstate, _) = error_of(sql);
        assert!(sqlstate == "42883", "{sql}");
    }
}

#[test]
fn overlay_replaces_a_byte_window() {
    for (sql, expected) in [
        (
            r"encode(overlay(E'Th\\000omas'::bytea placing E'Th\\001omas'::bytea from 2),'hex')",
            "545468016f6d6173",
        ),
        (
            r"encode(overlay(E'Th\\000omas'::bytea placing E'\\002\\003'::bytea from 8),'hex')",
            "5468006f6d61730203",
        ),
        (
            r"encode(overlay(E'Th\\000omas'::bytea placing E'\\002\\003'::bytea from 5 for 3),'hex')",
            "5468006f0203",
        ),
    ] {
        assert!(text_of(sql) == expected, "{sql}");
    }
}

#[test]
fn overlay_refuses_a_non_positive_start() {
    let (sqlstate, message) = error_of(r"overlay('\x1234'::bytea placing '\x56'::bytea from 0)");
    assert!(sqlstate == "22011");
    assert!(message == "negative substring length not allowed");
}

#[test]
fn get_byte_and_set_byte_address_bytes() {
    assert!(text_of(r"get_byte('\x1234567890abcdef00'::bytea, 3)") == "120");
    assert!(text_of(r"set_byte('\x1234567890abcdef00'::bytea, 7, 11)") == r"\x1234567890abcd0b00");
    for sql in [
        r"get_byte('\x1234567890abcdef00'::bytea, 99)",
        r"set_byte('\x1234567890abcdef00'::bytea, 99, 11)",
    ] {
        let (sqlstate, message) = error_of(sql);
        assert!(sqlstate == "2202E", "{sql}");
        assert!(message == "index 99 out of valid range, 0..8", "{sql}");
    }
}

#[test]
fn checksums_match_the_reference_vectors() {
    for (sql, expected) in [
        ("crc32('')", "0"),
        (
            "crc32('The quick brown fox jumps over the lazy dog.')",
            "1368401385",
        ),
        ("crc32c('')", "0"),
        (
            "crc32c('The quick brown fox jumps over the lazy dog.')",
            "419469235",
        ),
        ("crc32c(repeat('A', 127)::bytea)", "291820082"),
        ("crc32c(repeat('A', 128)::bytea)", "816091258"),
        ("crc32c(repeat('A', 129)::bytea)", "4213642571"),
        ("crc32c(repeat('A', 800)::bytea)", "3134039419"),
    ] {
        assert!(text_of(sql) == expected, "{sql}");
    }
    // `bigint`, so a residue above 2^31 stays positive.
    assert!(type_of("crc32('')") == ColumnType::Int8);
}

/// `encode` has only a `bytea` parameter, so an untyped literal is coerced
/// through `byteain` — reading its UTF-8 instead would hex the backslash.
/// `encode(bytea, 'escape')` is `esc_encode`, not `byteaout`'s escape: it
/// escapes only NUL and the high half, so a control byte survives raw. psql is
/// what renders that byte as `\x01` in the regression output.
#[test]
fn encode_escape_passes_control_bytes_through_raw() {
    assert!(
        text_of(
            r"encode(overlay(E'Th\\000omas'::bytea placing E'Th\\001omas'::bytea from 2),'escape')"
        ) == "TTh\u{1}omas"
    );
    assert!(
        text_of(r"encode('\x1234567890abcdef00'::bytea, 'escape')")
            == "\u{12}4Vx\\220\\253\\315\\357\\000"
    );
    // The same bytes under `byteaout`'s escape rule do escape the control byte.
    assert!(
        text_of_in(r"'\x1234567890abcdef00'::bytea", ByteaOutput::Escape)
            == r"\022".to_string() + r"4Vx\220\253\315\357\000"
    );
    // `decode` still reads back what `encode` wrote.
    assert!(text_of(r"decode(encode('\x01ff'::bytea, 'escape'), 'escape')") == r"\x01ff");
}

#[test]
fn encode_coerces_an_untyped_literal_through_byteain() {
    assert!(text_of(r"encode('\x1234567890abcdef00', 'hex')") == "1234567890abcdef00");
    assert!(
        text_of(r"decode(encode('\x1234567890abcdef00', 'escape'), 'escape')")
            == r"\x1234567890abcdef00"
    );
}

#[test]
fn like_over_bytea_matches_byte_by_byte() {
    for (sql, expected) in [
        (r"'abc'::bytea LIKE '_b_'::bytea", "t"),
        (r"'abc'::bytea NOT LIKE '_b_'::bytea", "f"),
        (r"'a_c'::bytea LIKE 'a$__'::bytea ESCAPE '$'::bytea", "t"),
        (
            r"'a_c'::bytea NOT LIKE 'a$__'::bytea ESCAPE '$'::bytea",
            "f",
        ),
        // `_` is one *byte*, so a two-byte character needs two of them.
        (r"'é'::bytea LIKE '_'::bytea", "f"),
        (r"'é'::bytea LIKE '__'::bytea", "t"),
        (r"'abc'::bytea LIKE '%'::bytea", "t"),
        (r"'abc'::bytea LIKE 'a%c'::bytea", "t"),
        (r"'abc'::bytea LIKE 'a%d'::bytea", "f"),
    ] {
        assert!(text_of(sql) == expected, "{sql}");
    }
}

/// An `unknown` literal must not steal the `text` overload's calls.
#[test]
fn an_untyped_literal_still_selects_the_text_overload() {
    assert!(type_of("substr('abcdef', 2, 3)") == ColumnType::Text);
    assert!(text_of("substr('abcdef', 2, 3)") == "bcd");
    assert!(type_of("btrim('  x  ')") == ColumnType::Text);
    assert!(type_of("position('cd' in 'abcdef')") == ColumnType::Int4);
}

/// PostgreSQL declares no one-argument `bytea` trim, because "whitespace" is a
/// property of text rather than of bytes.
#[test]
fn the_bytea_trims_require_their_byte_set() {
    let (sqlstate, _) = error_of(r"btrim('\x2020'::bytea)");
    assert!(sqlstate == "42883");
}
