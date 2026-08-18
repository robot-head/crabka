use assert2::assert;
use crabka_pgparser::parser::parse_expr_for_test as pexpr;
use crabka_pgtypes::{ColumnType, Datum, ElemType};

use crate::{clock::EvalCtx, regexp_fn::compile_pattern, scope::Scope};

/// Evaluate a FROM-less expression and render it the way the wire would.
///
/// An array result then reads as PostgreSQL prints it, for example `{a,b}`.
fn text_of(sql: &str) -> String {
    let ctx = EvalCtx::test_default();
    let value =
        crate::eval::eval(&pexpr(sql).expect("parse"), &Scope::empty(), &[], &ctx).expect("eval");
    match value {
        Datum::Null => "<null>".to_string(),
        other => crate::func::text_render(&other, &ctx.time_zone),
    }
}

/// The SQLSTATE and message a failing expression reports.
///
/// The result comes from whichever path rejects the expression: the plan-time
/// path `infer_type` or the run-time path `eval`. This is the same order a real
/// statement goes through.
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
fn regexp_replace_handles_flags_start_and_occurrence() {
    let cases = [
        ("regexp_replace('foobarbaz', 'b..', 'X')", "fooXbaz"),
        ("regexp_replace('foobarbaz', 'b..', 'X', 'g')", "fooXX"),
        (
            "regexp_replace('foobarbaz', 'b(..)', '[\\1]', 'g')",
            "foo[ar][az]",
        ),
        (
            "regexp_replace('foobarbaz', 'b(..)', 'X\\1Y')",
            "fooXarYbaz",
        ),
        ("regexp_replace('ABC', 'b', 'x', 'i')", "AxC"),
        ("regexp_replace('a1b2', '[0-9]', '#', 'g')", "a#b#"),
        ("regexp_replace('AbcAbc', 'a', 'X', 'gi')", "XbcXbc"),
        ("regexp_replace('a.b', '.', 'X')", "X.b"),
        ("regexp_replace('abc', '(a)(b)', '\\2\\1')", "bac"),
        // start / N select which occurrence is rewritten; N = 0 means all.
        ("regexp_replace('abcabc', 'b', 'X', 1, 2)", "abcaXc"),
        ("regexp_replace('abcabc', 'b', 'X', 1, 0)", "aXcaXc"),
        ("regexp_replace('abcabc', 'b', 'X', 4)", "abcaXc"),
        // `\&` is the whole match; `\\` is a literal backslash.
        ("regexp_replace('abc', 'b', '[\\&]')", "a[b]c"),
        ("regexp_replace('abc', 'b', '\\\\')", "a\\c"),
        ("regexp_replace(NULL, 'a', 'b')", "<null>"),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
    assert!(sqlstate("regexp_replace('abc', 'b', 'x', 0)") == "22023");
    assert!(
        message("regexp_replace('abc', 'b', 'x', 0)") == "invalid value for parameter \"start\": 0"
    );
    assert!(sqlstate("regexp_replace('abc', 'b', 'x', 1, -1)") == "22023");
}

#[test]
fn are_word_boundaries_and_absolute_anchors_translate_to_regex_assertions() {
    assert!(
        compile_pattern("regexp_like()", false, r"\mbar\M", "")
            .expect("ARE word boundaries")
            .is_match("a bar!")
    );
    assert!(
        !compile_pattern("regexp_like()", false, r"\mbar\M", "")
            .expect("ARE word boundaries")
            .is_match("foobar")
    );
    assert!(
        compile_pattern("regexp_like()", false, r"\ybar\y", "")
            .expect("ARE word boundary")
            .is_match("a bar!")
    );
    assert!(
        compile_pattern("regexp_like()", false, r"a\Yb", "")
            .expect("ARE non-boundary")
            .is_match("ab")
    );
    assert!(
        compile_pattern("regexp_like()", false, r"\Aword\Z", "")
            .expect("ARE absolute anchors")
            .is_match("word")
    );
    assert!(
        !compile_pattern("regexp_like()", false, r"\Aword\Z", "")
            .expect("ARE absolute anchors")
            .is_match("word\n")
    );
}

#[test]
fn are_anchor_escapes_stay_literal_inside_character_classes() {
    assert_eq!(
        super::translate_are_anchors(r"[\m\M\y\Y\Z]\m"),
        r"[\m\M\y\Y\Z]\b{start}"
    );
}

#[test]
fn regexp_count_counts_non_overlapping_matches() {
    let cases = [
        ("regexp_count('aaa', 'a')", "3"),
        ("regexp_count('abcabc', 'bc')", "2"),
        ("regexp_count('ABAB', 'a', 1, 'i')", "2"),
        ("regexp_count('aaa', 'a', 2)", "2"),
        ("regexp_count('abcabcabc', 'bc', 4)", "2"),
        ("regexp_count('', 'a')", "0"),
        // An empty pattern matches at every position, including the end.
        ("regexp_count('aaa', '')", "4"),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
    assert!(sqlstate("regexp_count('a', 'a', 0)") == "22023");
    assert!(sqlstate("regexp_count('a', 'a', 1, 'g')") == "22023");
    assert!(
        message("regexp_count('a', 'a', 1, 'g')")
            == "regexp_count() does not support the \"global\" option"
    );
}

#[test]
fn regexp_instr_reports_character_positions() {
    let cases = [
        ("regexp_instr('abcdef', 'cd')", "3"),
        ("regexp_instr('abcabc', 'bc', 1, 2)", "5"),
        ("regexp_instr('abc', 'x')", "0"),
        // endoption 1 reports the position just past the match.
        ("regexp_instr('abcdef', 'cd', 1, 1, 1)", "5"),
        ("regexp_instr('abcabc', 'b', 1, 2)", "5"),
        ("regexp_instr('abcabc', 'b', 1, 2, 1)", "6"),
        // subexpr selects a capture group.
        ("regexp_instr('abcabc', '(b)(c)', 1, 1, 0, '', 2)", "3"),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
    assert!(sqlstate("regexp_instr('abc', 'x', 0)") == "22023");
    assert!(sqlstate("regexp_instr('abc', 'b', 1, 1, 2)") == "22023");
    assert!(
        message("regexp_instr('abc', 'b', 1, 1, 2)")
            == "invalid value for parameter \"endoption\": 2"
    );
}

#[test]
fn regexp_like_and_substr_match_postgres() {
    let cases = [
        ("regexp_like('abc', 'b')", "t"),
        ("regexp_like('ABC', 'b', 'i')", "t"),
        ("regexp_like('abc', 'x')", "f"),
        ("regexp_like(NULL, 'a')", "<null>"),
        ("regexp_substr('abcdef', 'c.e')", "cde"),
        ("regexp_substr('abc', 'x')", "<null>"),
        ("regexp_substr('abcabc', 'b(c)', 1, 1, '', 1)", "c"),
        ("regexp_substr('abcabc', 'b(c)', 1, 2)", "bc"),
        ("regexp_substr('abcabc', 'b(c)', 1, 2, '', 1)", "c"),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
    assert!(sqlstate("regexp_like('a', 'a', 'g')") == "22023");
    assert!(sqlstate("regexp_substr('a', 'a', 1, 1, 'g')") == "22023");
    assert!(sqlstate("regexp_substr('abc', 'b', 1, 1, '', -1)") == "22023");
}

#[test]
fn regexp_match_and_split_build_text_arrays() {
    let cases = [
        ("regexp_match('foobarbaz', 'b(..)')", "{ar}"),
        ("regexp_match('abc', 'x')", "<null>"),
        // No capture group means the whole match is the single element.
        ("regexp_match('abc', 'b')", "{b}"),
        // A non-participating group is a NULL element.
        ("regexp_match('abc', '(x)?(b)')", "{NULL,b}"),
        ("regexp_match('abc', '(a)(b)')", "{a,b}"),
        ("regexp_split_to_array('a,b,,c', ',')", "{a,b,\"\",c}"),
        // A zero-length match at the ends, or right after a match, is ignored.
        ("regexp_split_to_array('abc', '')", "{a,b,c}"),
        ("regexp_split_to_array('a1b22c', '[0-9]+')", "{a,b,c}"),
        (
            "regexp_split_to_array('helloWORLD', '[A-Z]')",
            "{hello,\"\",\"\",\"\",\"\",\"\"}",
        ),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
    assert!(sqlstate("regexp_match('a', 'a', 'g')") == "22023");
}

#[test]
fn an_uncompilable_pattern_is_2201b() {
    assert!(sqlstate("regexp_replace('a', '[', 'b')") == "2201B");
    assert!(message("regexp_replace('a', '[', 'b')").starts_with("invalid regular expression:"));
    assert!(sqlstate("regexp_like('a', 'a', 'z')") == "22023");
    assert!(message("regexp_like('a', 'a', 'z')") == "invalid regular expression option: \"z\"");
}

#[test]
fn result_types_match_postgres() {
    let cases = [
        ("regexp_replace('a', 'a', 'b')", ColumnType::Text),
        ("regexp_count('a', 'a')", ColumnType::Int4),
        ("regexp_instr('a', 'a')", ColumnType::Int4),
        ("regexp_like('a', 'a')", ColumnType::Bool),
        ("regexp_substr('a', 'a')", ColumnType::Text),
        ("regexp_match('a', 'a')", ColumnType::Array(ElemType::Text)),
        (
            "regexp_split_to_array('a', 'b')",
            ColumnType::Array(ElemType::Text),
        ),
    ];
    for (sql, expected) in cases {
        assert!(result_type(sql) == expected, "{sql}");
    }
}

#[test]
fn every_regexp_function_is_strict() {
    let cases = [
        "regexp_replace(NULL, 'a', 'b')",
        "regexp_count(NULL, 'a')",
        "regexp_instr(NULL, 'a')",
        "regexp_like(NULL, 'a')",
        "regexp_substr(NULL, 'a')",
        "regexp_match(NULL, 'a')",
        "regexp_split_to_array(NULL, 'a')",
        "regexp_replace('a', NULL, 'b')",
    ];
    for sql in cases {
        assert!(text_of(sql) == "<null>", "{sql}");
    }
}
