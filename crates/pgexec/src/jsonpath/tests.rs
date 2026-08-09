use assert2::assert;
use crabka_pgtypes::{JsonbValue, jsonb};

use super::JsonPath;

/// Run `path` over `target` and render the result the way
/// `jsonb_path_query_array` does, so a case reads exactly like its oracle
/// probe.
fn query(target: &str, path: &str) -> Result<String, String> {
    query_vars(target, path, None)
}

fn query_vars(target: &str, path: &str, vars: Option<&str>) -> Result<String, String> {
    let target = jsonb::parse(target).expect("target");
    let vars = vars.map(|v| jsonb::parse(v).expect("vars"));
    let compiled = JsonPath::parse(path).map_err(|e| e.into_pg().code)?;
    compiled
        .query(&target, vars.as_ref(), false)
        .map(|items| JsonbValue::Array(items).to_text())
        .map_err(|e| e.into_pg().code)
}

#[test]
fn accessors_produce_postgresql_item_sequences() {
    let cases: &[(&str, &str, &str)] = &[
        // target, path, jsonb_path_query_array(target, path)
        (r#"{"a":[1,2,3]}"#, "$.a[*]", "[1, 2, 3]"),
        (r#"{"a":[1,2,3]}"#, "$.a[0 to 1]", "[1, 2]"),
        (r#"{"a":[1,2,3]}"#, "$.a[last]", "[3]"),
        (r#"{"a":[1,2,3]}"#, "$.a[last-1]", "[2]"),
        (r#"{"a":[1,2,3]}"#, "$.a[1 to last]", "[2, 3]"),
        ("[1,2,3]", "$[0,2]", "[1, 3]"),
        ("[1,2,3]", "$[0 to 1, 2]", "[1, 2, 3]"),
        (r#"{"a":1}"#, "$.*", "[1]"),
        (r#"[{"a":1},{"a":2}]"#, "$[*].a", "[1, 2]"),
        ("{}", "$.a.b", "[]"),
        (
            r#"{"a":[1,2,3],"b":{"c":4}}"#,
            "$.**",
            "[{\"a\": [1, 2, 3], \"b\": {\"c\": 4}}, [1, 2, 3], 1, 2, 3, {\"c\": 4}, 4]",
        ),
        (r#"{"a":{"b":{"c":1}}}"#, "$.**{1}", "[{\"b\": {\"c\": 1}}]"),
        (
            r#"{"a":{"b":{"c":1}}}"#,
            "$.**{1 to 2}",
            "[{\"b\": {\"c\": 1}}, {\"c\": 1}]",
        ),
        (r#"{"a":{"b":1},"c":2}"#, "$.**.b", "[1]"),
        (r#"{"a":1}"#, "$.a + 1", "[2]"),
        (r#"{"a":1}"#, "-$.a", "[-1]"),
    ];
    for (target, path, want) in cases {
        assert!(query(target, path) == Ok((*want).to_string()), "{path}");
    }
}

#[test]
fn filters_and_predicates_follow_three_valued_logic() {
    let cases: &[(&str, &str, &str)] = &[
        ("[1,2,3]", "$[*] ? (@ > 1)", "[2, 3]"),
        ("[1,2,3]", "$[*] ? (@ >= 2 && @ <= 3)", "[2, 3]"),
        ("[1,2,3]", "$[*] ? (@ != 2)", "[1, 3]"),
        (r#"{"a":1}"#, "$ ? (@.a == 1 || @.b == 2)", "[{\"a\": 1}]"),
        (r#"{"a":1}"#, "$ ? (!(@.a == 2))", "[{\"a\": 1}]"),
        // `@.b` is absent, so the comparison has no pairs and is false, not
        // unknown — `is unknown` therefore filters everything out.
        (r#"{"a":1}"#, "$ ? ((@.b == 2) is unknown)", "[]"),
        (r#"{"a":1}"#, "$ ? (exists(@.a))", "[{\"a\": 1}]"),
        (
            r#"{"a":"abc"}"#,
            r#"$.a ? (@ starts with "ab")"#,
            r#"["abc"]"#,
        ),
        (
            r#"{"a":"abc"}"#,
            r#"$.a ? (@ like_regex "^a.c$")"#,
            r#"["abc"]"#,
        ),
        (
            r#"{"a":"x"}"#,
            r#"$.a ? (@ like_regex "X" flag "i")"#,
            r#"["x"]"#,
        ),
        (r#"{"a":1}"#, "$.a ? (@ == 1) ? (@ > 0)", "[1]"),
        // A string never compares equal to a number, so the mixed array keeps
        // only the numeric element.
        (r#"[1,"a"]"#, "lax $[*] ? (@ > 0)", "[1]"),
        // A top-level predicate is a value, and `jsonb_path_query` renders it.
        (r#"{"a":1}"#, "$.a == 1", "[true]"),
    ];
    for (target, path, want) in cases {
        assert!(query(target, path) == Ok((*want).to_string()), "{path}");
    }
}

#[test]
fn lax_unwraps_and_wraps_where_strict_raises() {
    let cases: &[(&str, &str, Result<&str, &str>)] = &[
        (r#"{"a":1}"#, "lax $.b", Ok("[]")),
        (r#"{"a":1}"#, "strict $.b", Err("2203A")),
        (r#"[{"a":1},{"a":2}]"#, "lax $.a", Ok("[1, 2]")),
        (r#"[{"a":1},{"a":2}]"#, "strict $.a", Err("2203A")),
        (r#"{"a":1}"#, "lax $[*]", Ok(r#"[{"a": 1}]"#)),
        (r#"{"a":1}"#, "strict $[*]", Err("22039")),
        ("1", "lax $[0]", Ok("[1]")),
        ("1", "strict $[0]", Err("22039")),
        ("[1,2]", "lax $[5]", Ok("[]")),
        ("[1,2]", "strict $[5]", Err("22033")),
        ("[[1,2],[3,4]]", "lax $[*][*]", Ok("[1, 2, 3, 4]")),
        (r#"{"a":[1,2]}"#, "lax $.a ? (@ > 1)", Ok("[2]")),
        (r#"{"a":[1,2]}"#, "strict $.a ? (@ > 1)", Ok("[]")),
        ("1", "lax $.size()", Ok("[1]")),
        ("1", "strict $.size()", Err("22039")),
        (r#"[{"a":1}]"#, "strict $.keyvalue()", Err("2203C")),
    ];
    for (target, path, want) in cases {
        let got = query(target, path);
        let want = want.map(str::to_string).map_err(str::to_string);
        assert!(got == want, "{path}");
    }
}

#[test]
fn item_methods_match_postgresql() {
    let cases: &[(&str, &str, &str)] = &[
        (r#"{"a":-3}"#, "$.a.abs()", "[3]"),
        (r#"{"a":1.7}"#, "$.a.ceiling()", "[2]"),
        (r#"{"a":-1.7}"#, "$.a.ceiling()", "[-1]"),
        (r#"{"a":1.7}"#, "$.a.floor()", "[1]"),
        (r#"{"a":-1.7}"#, "$.a.floor()", "[-2]"),
        (r#"{"a":"1.5"}"#, "$.a.double()", "[1.5]"),
        (r#"{"a":[1,2]}"#, "$.a.size()", "[2]"),
        (r#"{"a":[1,2]}"#, "$.a.type()", r#"["array"]"#),
        ("null", "$.type()", r#"["null"]"#),
        ("[]", "$.type()", r#"["array"]"#),
        (r#"{"a":1}"#, "$.a.type()", r#"["number"]"#),
        (r#"{"a":"3"}"#, "$.a.bigint()", "[3]"),
        (r#"{"a":"3"}"#, "$.a.integer()", "[3]"),
        (r#"{"a":"3.7"}"#, "$.a.decimal()", "[3.7]"),
        (r#"{"a":"3.5"}"#, "$.a.number()", "[3.5]"),
        (r#"{"a":3.5}"#, "$.a.string()", r#"["3.5"]"#),
        (r#"{"a":"true"}"#, "$.a.boolean()", "[true]"),
        (r#"{"a":1}"#, "$.a.boolean()", "[true]"),
        (
            r#"{"a":1,"b":2}"#,
            "$.keyvalue()",
            r#"[{"id": 0, "key": "a", "value": 1}, {"id": 0, "key": "b", "value": 2}]"#,
        ),
        (r#""2023-01-02""#, "$.date()", r#"["2023-01-02"]"#),
        (
            r#""2023-01-02 10:11:12""#,
            "$.timestamp()",
            r#"["2023-01-02T10:11:12"]"#,
        ),
        (r#""10:11:12""#, "$.time()", r#"["10:11:12"]"#),
    ];
    for (target, path, want) in cases {
        assert!(query(target, path) == Ok((*want).to_string()), "{path}");
    }
    // A non-numeric string is PostgreSQL's 22036, not a silent miss.
    assert!(query(r#"{"a":"x"}"#, "$.a.double()") == Err("22036".into()));
}

#[test]
fn variables_resolve_from_the_vars_argument() {
    assert!(query_vars(r#"{"x":1}"#, "$.x ? (@ == $v)", Some(r#"{"v":1}"#)) == Ok("[1]".into()));
    // A variable the `vars` object does not define is 42704, and — unlike a
    // structural error — is never silenced.
    assert!(query_vars(r#"{"x":1}"#, "$.x ? (@ == $v)", None) == Err("42704".into()));
}

#[test]
fn exists_and_predicate_entry_points() {
    let target = jsonb::parse(r#"{"a":1}"#).expect("target");
    let exists = JsonPath::parse("$.a").expect("parse");
    assert!(exists.exists(&target, None, true) == Ok(Some(true)));
    let missing = JsonPath::parse("$.b").expect("parse");
    assert!(missing.exists(&target, None, true) == Ok(Some(false)));
    let matched = JsonPath::parse("$.a > 0").expect("parse");
    assert!(matched.predicate(&target, None, true) == Ok(Some(true)));
    let unmatched = JsonPath::parse("$.a > 5").expect("parse");
    assert!(unmatched.predicate(&target, None, true) == Ok(Some(false)));
    // `@@` over a path that is not a single boolean is NULL under silent mode.
    let not_boolean = JsonPath::parse("$.a").expect("parse");
    assert!(not_boolean.predicate(&target, None, true) == Ok(None));
}

#[test]
fn syntax_errors_are_42601() {
    for bad in ["$.", "$.a.zzz()", "bogus path", "$[", "$ ? (", "@@"] {
        let err = JsonPath::parse(bad).expect_err(bad);
        assert!(err.into_pg().code == "42601", "{bad}");
    }
}

#[test]
fn compiled_paths_round_trip_through_their_text() {
    for src in [
        "$",
        "strict $",
        "$.a[*]",
        "$.**",
        "$.a?(@ > 1)",
        "$.a.type()",
    ] {
        let once = JsonPath::parse(src).expect(src);
        let twice = JsonPath::parse(&once.to_string()).expect(src);
        assert!(once == twice, "{src}");
    }
}
