use assert2::assert;
use crabka_pgtypes::{Datum, JsonbValue, jsonb};

use super::{Exec, JsonPath};

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

fn query_tz(target: &str, path: &str) -> Result<String, String> {
    query_in_tz(target, path, &jiff::tz::TimeZone::UTC)
}

fn query_in_tz(target: &str, path: &str, time_zone: &jiff::tz::TimeZone) -> Result<String, String> {
    let target = jsonb::parse(target).expect("target");
    let compiled = JsonPath::parse(path).map_err(|e| e.into_pg().code)?;
    compiled
        .query_tz(&target, None, false, time_zone)
        .map(|items| JsonbValue::Array(items).to_text())
        .map_err(|e| e.into_pg().code)
}

fn query_in_session_time_zone(
    target: &str,
    path: &str,
    time_zone: &jiff::tz::TimeZone,
) -> Result<String, String> {
    let target = jsonb::parse(target).expect("target");
    let compiled = JsonPath::parse(path).map_err(|e| e.into_pg().code)?;
    compiled
        .query_with_session_time_zone(&target, None, false, time_zone)
        .map(|items| JsonbValue::Array(items).to_text())
        .map_err(|e| e.into_pg().code)
}

#[test]
fn temporal_comparisons_need_the_tz_variant_for_implicit_conversion() {
    let target = jsonb::parse(r#"["2015-08-01 12:00:00-05"]"#).expect("target");
    let path = JsonPath::parse(r#"$[*] ? (@.datetime() < "2015-08-02".datetime())"#).expect("path");

    assert!(
        path.exists(&target, None, false)
            .expect_err("plain variant must refuse an implicit zone")
            .into_pg()
            .code
            == "0A000"
    );
    assert!(path.exists_tz(&target, None, false, &jiff::tz::TimeZone::UTC) == Ok(Some(true)));

    for (target, source) in [
        (
            r#""2015-08-01 12:00:00""#,
            r#"$.timestamp() == "2015-08-01 12:00:00+00".timestamp_tz()"#,
        ),
        (r#""12:00:00""#, r#"$.time() == "12:00:00+00".time_tz()"#),
    ] {
        let target = jsonb::parse(target).expect("target");
        let path = JsonPath::parse(source).expect("path");
        assert!(
            path.exists(&target, None, false)
                .expect_err("plain variant")
                .into_pg()
                .code
                == "0A000"
        );
        assert!(path.exists_tz(&target, None, false, &jiff::tz::TimeZone::UTC) == Ok(Some(true)));
    }

    assert_eq!(
        query(r#""12:34:56""#, r#"$.datetime() == "2017-03-10".date()"#),
        Ok("[null]".into())
    );

    assert_eq!(
        query(
            r#"["2017-03-10 12:35:00", "2017-03-10 12:35:00+01"]"#,
            r#"$[*].datetime() ? (@ == "10.03.2017 12:35 +1".datetime("dd.mm.yyyy HH24:MI TZH"))"#,
        ),
        Err("0A000".into())
    );
}

#[test]
fn datetime_template_retains_its_zoned_datum() {
    let target = jsonb::parse(r#""10.03.2017 12:35 +1""#).expect("target");
    let path = JsonPath::parse(r#"$.datetime("dd.mm.yyyy HH24:MI TZH")"#).expect("path");
    let exec = Exec {
        strict: false,
        stop_after_one: false,
        vars: None,
        root: &target,
        last: None,
        time_zone: None,
        allow_zone_conversions: false,
        current_temporal: None,
    };
    let items = exec.eval(&path.root, &target).expect("eval");
    assert!(
        matches!(items.as_slice(), [item] if matches!(item.temporal, Some(Datum::Timestamptz(_))))
    );
    let unzoned = jsonb::parse(r#""2017-03-10 12:35:00""#).expect("unzoned");
    let unzoned_path = JsonPath::parse("$.datetime()").expect("path");
    let unzoned_exec = Exec {
        strict: false,
        stop_after_one: false,
        vars: None,
        root: &unzoned,
        last: None,
        time_zone: None,
        allow_zone_conversions: false,
        current_temporal: None,
    };
    let unzoned_items = unzoned_exec
        .eval(&unzoned_path.root, &unzoned)
        .expect("eval");
    let timezone_required = Err(super::PathError::new(
        "0A000",
        "cannot convert value from timestamp to timestamptz without time zone usage\nHINT:  Use *_tz() function for time zone support.",
    ));
    assert_eq!(
        exec.compare(super::CmpOp::Eq, &unzoned_items[0], &items[0]),
        timezone_required
    );
    assert_eq!(
        exec.compare(super::CmpOp::Eq, &items[0], &unzoned_items[0]),
        timezone_required
    );
    let date = jsonb::parse(r#""2017-03-10""#).expect("date");
    let date_path = JsonPath::parse(r#"$.datetime("YYYY-MM-DD")"#).expect("path");
    let date_exec = Exec {
        strict: false,
        stop_after_one: false,
        vars: None,
        root: &date,
        last: None,
        time_zone: None,
        allow_zone_conversions: false,
        current_temporal: None,
    };
    let date_items = date_exec.eval(&date_path.root, &date).expect("eval");
    assert_eq!(
        exec.compare(super::CmpOp::Eq, &date_items[0], &items[0]),
        Err(super::PathError::new(
            "0A000",
            "cannot convert value from date to timestamptz without time zone usage\nHINT:  Use *_tz() function for time zone support.",
        ))
    );
    let time = jsonb::parse(r#""12:00:00""#).expect("time");
    let timetz = jsonb::parse(r#""12:00:00+01""#).expect("timetz");
    let time_exec = Exec {
        strict: false,
        stop_after_one: false,
        vars: None,
        root: &time,
        last: None,
        time_zone: None,
        allow_zone_conversions: false,
        current_temporal: None,
    };
    let timetz_exec = Exec {
        strict: false,
        stop_after_one: false,
        vars: None,
        root: &timetz,
        last: None,
        time_zone: None,
        allow_zone_conversions: false,
        current_temporal: None,
    };
    let time_items = time_exec.eval(&unzoned_path.root, &time).expect("eval");
    let timetz_items = timetz_exec.eval(&unzoned_path.root, &timetz).expect("eval");
    assert_eq!(
        exec.compare(super::CmpOp::Eq, &timetz_items[0], &time_items[0]),
        Err(super::PathError::new(
            "0A000",
            "cannot convert value from time to timetz without time zone usage\nHINT:  Use *_tz() function for time zone support.",
        ))
    );
}

#[test]
fn temporal_methods_require_tz_for_cross_zone_conversions() {
    for (target, path) in [
        (r#""12:34:56+05""#, "$.time()"),
        (r#""2023-08-15 12:34:56+05""#, "$.timestamp()"),
        (r#""2023-08-15""#, "$.timestamp_tz()"),
    ] {
        assert_eq!(query(target, path), Err("0A000".into()), "{path}");
    }
    assert_eq!(
        query_tz(r#""12:34:56+05""#, "$.time()"),
        Ok(r#"["07:34:56"]"#.into())
    );
    assert_eq!(
        query_tz(r#""2023-08-15 12:34:56+05""#, "$.timestamp()"),
        Ok(r#"["2023-08-15T07:34:56"]"#.into())
    );
    let plus_two = jiff::tz::TimeZone::fixed(jiff::tz::Offset::constant(2));
    assert_eq!(
        query_in_tz(r#""23:34:56+05""#, "$.time()", &plus_two),
        Ok(r#"["20:34:56"]"#.into())
    );
    assert_eq!(
        query_in_session_time_zone(r#""2023-08-15 12:34:56+05:30""#, "$.time_tz()", &plus_two),
        Ok(r#"["09:04:56+02:00"]"#.into())
    );
    assert_eq!(
        query_in_session_time_zone(r#""2023-08-15 12:34:56""#, "$.timestamp_tz()", &plus_two),
        Err("0A000".into())
    );
    assert_eq!(
        query(
            r#""2023-08-15 12:34:56+05:30""#,
            "$.timestamp_tz().string()"
        ),
        Ok(r#"["2023-08-15T12:34:56+05:30"]"#.into())
    );
    assert_eq!(
        query(r#""12:34:56+03""#, "$.datetime()"),
        Ok(r#"["12:34:56+03:00"]"#.into())
    );
    assert_eq!(
        query(r#""2023-08-15 12:34:56+03""#, "$.datetime()"),
        Ok(r#"["2023-08-15T12:34:56+03:00"]"#.into())
    );
    assert_eq!(
        query(r#""12:34:56-02""#, "$.datetime()"),
        Ok(r#"["12:34:56-02:00"]"#.into())
    );
    assert_eq!(
        query(r#""2017-03-10T12:34:56+3:10""#, "$.datetime()"),
        Ok(r#"["2017-03-10T12:34:56+03:10"]"#.into())
    );
    assert_eq!(
        query(r#""2017-03-10T12:34:56.789Z""#, "$.datetime()"),
        Ok(r#"["2017-03-10T12:34:56.789+00:00"]"#.into())
    );
    assert_eq!(
        query_tz(r#""2023-08-15 12:34:56+05:30""#, "$.time_tz().string()"),
        Ok(r#"["07:04:56+00:00"]"#.into())
    );
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
        (
            r#"[["a","b"],["c","d","e"]]"#,
            "$[*][last]",
            r#"["b", "e"]"#,
        ),
        (r#"[["a","b"],["c","d","e"]]"#, "$[last][last]", r#"["e"]"#),
        (
            r#"[["a","b"],["c","d","e"]]"#,
            "$[*][last - 1]",
            r#"["a", "d"]"#,
        ),
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
        (r#"{"a":{"b":1}}"#, "$.**{last}", "[1]"),
        (r#"{"a":{"b":1},"c":2}"#, "$.**.b", "[1]"),
        (r#"{"a":1}"#, "$.a + 1", "[2]"),
        (r#"{"a":1}"#, "-$.a", "[-1]"),
    ];
    for (target, path, want) in cases {
        assert!(query(target, path) == Ok((*want).to_string()), "{path}");
    }
    assert!(query("[1]", "$[10000000000000000]") == Err("22033".into()));
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
        ("null", "$ == null", "[true]"),
        ("null", "$ == true", "[false]"),
        ("null", "$ != true", "[true]"),
        ("true", "$ == true", "[true]"),
        ("true", "$ == false", "[false]"),
        (r#""foo""#, r#"$ == "foo""#, "[true]"),
        (r#""foo""#, r#"$ == "bar""#, "[false]"),
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
        ("[1,2]", "strict $[-1 to 0]", Err("22033")),
        ("[1,2]", "strict $[0 to -1]", Err("22033")),
        ("[1,2]", "strict $[2 to 0]", Err("22033")),
        ("[1,2]", "strict $[0 to 2]", Err("22033")),
        ("[1,2]", r#"strict $["zero"]"#, Err("22033")),
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
        (r#""2023-08-15""#, "$.date().type()", r#"["date"]"#),
        (
            r#""12:34:56""#,
            "$.time().type()",
            r#"["time without time zone"]"#,
        ),
        (
            r#""12:34:56+05""#,
            "$.time_tz().type()",
            r#"["time with time zone"]"#,
        ),
        (
            r#""2023-08-15 12:34:56""#,
            "$.timestamp().type()",
            r#"["timestamp without time zone"]"#,
        ),
        (
            r#""2023-08-15 12:34:56+05""#,
            "$.timestamp_tz().type()",
            r#"["timestamp with time zone"]"#,
        ),
        (r#"{"a":"3"}"#, "$.a.bigint()", "[3]"),
        (r#"{"a":1.8}"#, "$.a.bigint()", "[2]"),
        (r#"{"a":"3"}"#, "$.a.integer()", "[3]"),
        (r#"{"a":-1.8}"#, "$.a.integer()", "[-2]"),
        (r#"{"a":"3.7"}"#, "$.a.decimal()", "[3.7]"),
        (r#"{"a":"123.456"}"#, "$.a.decimal(5)", "[123]"),
        (r#"{"a":"123.456"}"#, "$.a.decimal(5,2)", "[123.46]"),
        (r#"{"a":3.7}"#, "$.a.decimal(5,2)", "[3.70]"),
        (r#"{"a":"3.5"}"#, "$.a.number()", "[3.5]"),
        (r#"{"a":3.5}"#, "$.a.string()", r#"["3.5"]"#),
        (r#"{"a":"true"}"#, "$.a.boolean()", "[true]"),
        (r#"{"a":false}"#, "$.a.boolean()", "[false]"),
        (r#"{"a":1}"#, "$.a.boolean()", "[true]"),
        (r#"{"a":0}"#, "$.a.boolean()", "[false]"),
        (r#"{"a":"NO"}"#, "$.a.boolean()", "[false]"),
        (r#"{"a":false}"#, "$.a.string()", r#"["false"]"#),
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
    assert!(query(r#"{"a":"1.8"}"#, "$.a.bigint()") == Err("22036".into()));
    assert!(query(r#"{"a":"-1.8"}"#, "$.a.integer()") == Err("22036".into()));
    assert!(query(r#"{"a":1.23}"#, "$.a.boolean()") == Err("22036".into()));
    assert!(query(r#"{"a":1e1000}"#, "$.a.boolean()") == Err("22036".into()));
    for value in ["nan", "NaN", "inf", "-inf"] {
        assert!(query(&format!(r#"{{"a":"{value}"}}"#), "$.a.decimal()") == Err("22036".into()));
        assert!(query(&format!(r#"{{"a":"{value}"}}"#), "$.a.number()") == Err("22036".into()));
        assert!(query(&format!(r#"{{"a":"{value}"}}"#), "$.a.double()") == Err("22036".into()));
    }
    let target = jsonb::parse(r#"{"a":"nan"}"#).expect("target");
    let error = JsonPath::parse("$.a.number()")
        .expect("path")
        .query(&target, None, false)
        .expect_err("non-finite number")
        .into_pg();
    assert_eq!(
        error.message,
        "NaN or Infinity is not allowed for jsonpath item method .number()"
    );
    let target = jsonb::parse(r#""bogus""#).expect("target");
    let error = JsonPath::parse("$.date()")
        .expect("path")
        .query(&target, None, false)
        .expect_err("invalid date")
        .into_pg();
    assert_eq!(error.message, "date format is not recognized: \"bogus\"");
}

#[test]
fn temporal_item_methods_accept_and_apply_precision() {
    for (source, expected) in [
        ("$.time(6)", "$.time(6)"),
        ("$.time_tz(4)", "$.time_tz(4)"),
        ("$.timestamp(2)", "$.timestamp(2)"),
        ("$.timestamp_tz(0)", "$.timestamp_tz(0)"),
        ("$.time(7)", "$.time(7)"),
        ("$.time(2147483647)", "$.time(2147483647)"),
    ] {
        assert!(JsonPath::parse(source).expect(source).to_string() == expected);
    }
    assert!(
        query(r#""2023-01-02 10:11:12.345678""#, "$.timestamp(2)")
            == Ok(r#"["2023-01-02T10:11:12.35"]"#.into())
    );
    assert!(
        query(r#""10:11:12.345678+05""#, "$.time_tz(4)") == Ok(r#"["10:11:12.3457+05:00"]"#.into())
    );
    assert!(query(r#""10:11:12.345678""#, "$.time(7)") == Ok(r#"["10:11:12.345678"]"#.into()));
    assert!(
        query(r#""2023-01-02 10:11:12.345678+05""#, "$.timestamp_tz(2)")
            == Ok(r#"["2023-01-02T10:11:12.35+05:00"]"#.into())
    );
    assert!(query(r#""10:11:12""#, "$.time(-1)") == Err("22023".into()));
    assert!(query(r#""10:11:12""#, "$.time(12345678901)") == Err("22031".into()));
    assert!(
        query(r#""10:11:12""#, "$.time(999999999999999999999999999999)") == Err("22031".into())
    );
    assert!(query("null", "$.time(12345678901)") == Err("22031".into()));
}

#[test]
fn decimal_method_accepts_signed_typmods_and_checks_their_bounds() {
    for (target, path, expected) in [
        ("1234.5678", "$.decimal(+6, +2)", "[1234.57]"),
        ("1234.5678", "$.decimal(+6, -2)", "[1200]"),
        ("-1234.5678", "$.decimal(+6, -2)", "[-1200]"),
        ("-0.00123456", "$.decimal(2, -4)", "[0]"),
        ("0", "$.decimal(1, 2)", "[0.00]"),
    ] {
        assert_eq!(query(target, path), Ok(expected.into()), "{path}");
    }
    for (path, code) in [
        ("$.decimal(0, 6)", "22023"),
        ("$.decimal(1001, 6)", "22023"),
        ("$.decimal(6, -1001)", "22023"),
        ("$.decimal(6, 1001)", "22023"),
        ("$.decimal(6.5, 1)", "42601"),
        ("$.decimal(12345678901, 1)", "22031"),
        ("$.decimal(1, 12345678901)", "22031"),
        ("$.decimal(2, 2)", "22036"),
    ] {
        assert_eq!(query("12.3", path), Err(code.into()), "{path}");
    }
}

#[test]
fn temporal_precision_warnings_match_postgresql() {
    for (path, expected) in [
        (
            "$.time(10)",
            "TIME(10) precision reduced to maximum allowed, 6",
        ),
        (
            "$.time_tz(8)",
            "TIME(8) WITH TIME ZONE precision reduced to maximum allowed, 6",
        ),
        (
            "$.timestamp(10)",
            "TIMESTAMP(10) precision reduced to maximum allowed, 6",
        ),
        (
            "$.timestamp_tz(8)",
            "TIMESTAMP(8) WITH TIME ZONE precision reduced to maximum allowed, 6",
        ),
    ] {
        assert_eq!(
            JsonPath::parse(path).expect(path).precision_warnings(),
            [expected]
        );
    }
    assert!(
        JsonPath::parse("$.time(6)")
            .expect("path")
            .precision_warnings()
            .is_empty()
    );

    for path in ["$[$.time(10)]", r#"$ ? (@.time(10) == "10:00".time())"#] {
        assert_eq!(
            JsonPath::parse(path).expect(path).precision_warnings(),
            ["TIME(10) precision reduced to maximum allowed, 6"],
            "{path}"
        );
    }
}

#[test]
fn variables_resolve_from_the_vars_argument() {
    assert!(query_vars(r#"{"x":1}"#, "$.x ? (@ == $v)", Some(r#"{"v":1}"#)) == Ok("[1]".into()));
    // A variable the `vars` object does not define is 42704, and — unlike a
    // structural error — is never silenced.
    assert!(query_vars(r#"{"x":1}"#, "$.x ? (@ == $v)", None) == Err("42704".into()));
    assert!(query_vars(r#"{"x":1}"#, "$ ? (exists ($v))", None) == Err("42704".into()));

    // `exists` converts a structural miss to SQL/JSON unknown instead of
    // raising it from the filter.
    assert!(query(r#"{"x":1}"#, "strict $ ? (exists (@.missing))") == Ok("[]".into()));
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
    let null = JsonPath::parse("null").expect("parse");
    assert!(null.predicate(&target, None, false) == Ok(None));
    let empty = JsonPath::parse("$.missing").expect("parse");
    assert!(empty.predicate(&target, None, true) == Ok(None));
    assert!(empty.predicate(&target, None, false).is_err());
    let multiple = JsonPath::parse("$.*").expect("parse");
    assert!(multiple.predicate(&target, None, true) == Ok(None));
    assert!(multiple.predicate(&target, None, false).is_err());
}

#[test]
fn tz_and_silent_entry_points_preserve_their_distinct_results() {
    let target = jsonb::parse(r#"{"a":1}"#).expect("target");
    let time_zone = jiff::tz::TimeZone::UTC;
    let path = JsonPath::parse("$.a").expect("parse");
    assert!(
        path.query_tz(&target, None, false, &time_zone)
            .map(|items| JsonbValue::Array(items).to_text())
            == Ok("[1]".into())
    );

    let first_target = jsonb::parse(r#"[{"a":1},{"a":2},{}]"#).expect("target");
    let first_path = JsonPath::parse("strict $[*].a").expect("parse");
    assert_eq!(
        first_path.query_first_with_session_time_zone(&first_target, None, true, &time_zone),
        Ok(Some(jsonb::parse("1").expect("first item")))
    );

    let predicate = JsonPath::parse("$.a == 1").expect("parse");
    assert!(predicate.predicate_tz(&target, None, false, &time_zone) == Ok(Some(true)));

    let missing = JsonPath::parse("strict $.missing").expect("parse");
    assert!(missing.query(&target, None, true) == Ok(Vec::new()));
    assert_eq!(
        missing.query_first_with_session_time_zone(&target, None, true, &time_zone),
        Ok(None)
    );
    let undefined = JsonPath::parse("$v").expect("parse");
    assert_eq!(
        undefined
            .query_first_with_session_time_zone(&target, None, true, &time_zone)
            .expect_err("undefined variables are not structural errors")
            .into_pg()
            .code,
        "42704"
    );
    assert!(missing.exists(&target, None, true) == Ok(None));
    let missing_predicate = JsonPath::parse("strict $.missing == 1").expect("parse");
    assert!(missing_predicate.predicate(&target, None, true) == Ok(None));
    assert!(missing_predicate.predicate(&target, None, false) == Ok(None));
}

#[test]
fn syntax_errors_are_42601() {
    for bad in [
        "$.a.zzz()",
        "$.",
        "bogus path",
        "$[",
        "$ ? (",
        "@@",
        "00",
        "0755",
        "1__0",
        "0b",
        "0x_",
        "0b_10_0101",
        "0o_273",
        "0x_42F",
        r#"$.a\u{51"#,
        "last",
        "$ ? (last > 0)",
        "@ + 1",
    ] {
        let err = JsonPath::parse(bad).expect_err(bad);
        assert!(err.into_pg().code == "42601", "{bad}");
    }

    let error = JsonPath::parse("$.")
        .expect_err("incomplete accessor")
        .into_pg();
    assert!(error.message.contains("end of jsonpath input"), "{error:?}");

    let error = JsonPath::parse("bogus path")
        .expect_err("unexpected token")
        .into_pg();
    assert!(error.message.contains("\"bogus\""), "{error:?}");
}

#[test]
fn last_is_accepted_only_inside_array_subscripts() {
    for source in ["$[last]", "$[$[0] ? (last > 0)]"] {
        assert!(JsonPath::parse(source).is_ok(), "{source}");
    }
}

#[test]
fn current_item_is_accepted_only_inside_filters() {
    for source in ["$ ? (@ == 1)", "$ ? (exists (@.a))"] {
        assert!(JsonPath::parse(source).is_ok(), "{source}");
    }
}

#[test]
fn comments_are_ignored_and_unclosed_comments_are_syntax_errors() {
    assert_eq!(
        JsonPath::parse("/* a comment */ $ /* another comment */")
            .expect("commented path")
            .to_string(),
        "$"
    );
    assert_eq!(
        JsonPath::parse("$          /**/")
            .expect("late short comment")
            .to_string(),
        "$"
    );
    assert_eq!(
        JsonPath::parse("/* an interior * is not the terminator */ $")
            .expect("comment with interior star")
            .to_string(),
        "$"
    );

    for source in ["/* unclosed", "/* *"] {
        let parsed = std::panic::catch_unwind(|| JsonPath::parse(source));
        let error = parsed
            .expect("unclosed comment must not panic")
            .expect_err("unclosed comment")
            .into_pg();
        assert_eq!(error.code, "42601");
        assert!(
            error
                .message
                .contains("unexpected end of comment of jsonpath input"),
            "{error:?}"
        );
    }
}

#[test]
fn like_regex_is_validated_when_the_path_is_parsed() {
    for (path, code) in [
        (r#"$ ? (@ like_regex "(invalid pattern")"#, "2201B"),
        (r#"$ ? (@ like_regex "pattern" flag "a")"#, "42601"),
        (r#"$ ? (@ like_regex "pattern" flag "x")"#, "0A000"),
    ] {
        let error = JsonPath::parse(path).expect_err(path).into_pg();
        assert!(error.code == code, "{path}: {error:?}");
        if code == "2201B" {
            assert_eq!(
                error.message,
                "invalid regular expression: parentheses () not balanced"
            );
        }
    }
    let path = JsonPath::parse(r#"$ ? (@ like_regex "a b" flag "smixq")"#).expect("x flag");
    assert_eq!(path.to_string(), r#"$?(@ like_regex "a b" flag "ismxq")"#);
}

#[test]
fn numeric_literals_use_postgresqls_numeric_grammar() {
    for (source, expected) in [
        ("1.", "1"),
        ("1e3", "1000"),
        (".1e-1", "0.01"),
        ("1_000.000_005", "1000.000005"),
        ("0b100101", "37"),
        ("0o273", "187"),
        ("0x42F", "1071"),
        ("0x1EEE_FFFF", "518979583"),
    ] {
        assert!(
            JsonPath::parse(source).expect(source).to_string() == expected,
            "{source}"
        );
    }
}

#[test]
fn malformed_numeric_literals_keep_postgresqls_trailing_junk_diagnostic() {
    for source in [
        "1a", "1b", "1.2a", "1.2e3a", "1e", "1.e", "1.2e", "0b", "0o", "0x",
    ] {
        let error = JsonPath::parse(source).expect_err(source).into_pg();
        assert_eq!(
            error.message,
            format!(
                "trailing junk after numeric literal at or near \"{source}\" of jsonpath input"
            ),
            "{source}"
        );
    }
}

#[test]
fn decimal_accessor_error_keeps_postgresqls_diagnostic_boundary() {
    let error = JsonPath::parse("1.type()")
        .expect_err("must reject accessor after decimal")
        .into_pg();
    assert_eq!(
        error.message,
        "trailing junk after numeric literal at or near \"1.t\" of jsonpath input"
    );
}

#[test]
fn malformed_numeric_literals_keep_postgresqls_end_of_input_diagnostic() {
    for source in [
        "0755", "0b0x", "0o0x", "0x0y", "_100", "_1_000.5", "100__000",
    ] {
        let error = JsonPath::parse(source).expect_err(source).into_pg();
        assert_eq!(
            error.message, "syntax error at end of jsonpath input",
            "{source}"
        );
    }
}

#[test]
fn malformed_numeric_separators_keep_postgresqls_trailing_junk_diagnostic() {
    for (source, token) in [
        ("100_", "100_"),
        ("1_000_.5", "1_000_"),
        ("1_000._5", "1_000._"),
        ("1_000.5_", "1_000.5_"),
        ("1_000.5e_1", "1_000.5e"),
    ] {
        let error = JsonPath::parse(source).expect_err(source).into_pg();
        assert_eq!(
            error.message,
            format!("trailing junk after numeric literal at or near \"{token}\" of jsonpath input"),
            "{source}"
        );
    }
}

#[test]
fn arithmetic_and_alternate_not_equal_spelling_evaluate() {
    for (path, expected) in [
        ("$ * 2", "[20]"),
        ("$ / 2", "[5.0000000000000000]"),
        ("$ % 3", "[1]"),
        ("$ <> 10", "[false]"),
    ] {
        assert_eq!(query("10", path), Ok(expected.into()), "{path}");
    }
}

#[test]
fn quoted_jsonpath_strings_accept_postgresql_escape_forms() {
    for (source, expected) in [
        (r#""\b\f\r\n\t\v\"'\\""#, r#""\b\f\r\n\t\u000b\"'\\""#),
        (r#""\x50\u0067\u{53}\u{051}\u{00004C}""#, r#""PgSQL""#),
        (r#""\uD83D\uDE00""#, r#""😀""#),
        (r#""\z""#, r#""z""#),
    ] {
        assert_eq!(
            JsonPath::parse(source).unwrap().to_string(),
            expected,
            "{source}"
        );
    }
}

#[test]
fn quoted_jsonpath_strings_reject_unpaired_utf16_surrogates() {
    for source in [r#""\uD83D""#, r#""\uDE00""#] {
        let error = JsonPath::parse(source).expect_err(source).into_pg();
        assert_eq!(error.code, "42601", "{source}");
    }
}

#[test]
fn quoted_variable_and_member_names_canonicalize_like_postgresql() {
    for (source, expected) in [
        ("$a", r#"$"a""#),
        (r#"$"a b""#, r#"$"a b""#),
        ("$.a", r#"$."a""#),
        (r#"$."a\\b""#, r#"$."a\\b""#),
        (
            r#"$.foo\x50\u0067\u{53}\u{051}\u{00004C}\t\"bar"#,
            r#"$."fooPgSQL\t\"bar""#,
        ),
        (r#"$.a\u0062.c"#, r#"$."ab"."c""#),
        (r#"$.a\u{051}.c"#, r#"$."aQ"."c""#),
    ] {
        assert_eq!(
            JsonPath::parse(source).unwrap().to_string(),
            expected,
            "{source}"
        );
    }
}

#[test]
fn arithmetic_and_predicate_rendering_use_postgresql_precedence() {
    for (source, expected) in [
        ("1 * 2 + 4 % -3 != false", "(1 * 2 + 4 % -3 != false)"),
        ("1 + (2 + 3)", "(1 + (2 + 3))"),
        (r#"$.a[1,2,3]"#, r#"$."a"[1,2,3]"#),
        (
            "$.g ? (@.a == 1 || @.a == 4 && @.b == 7)",
            r#"$."g"?(@."a" == 1 || @."a" == 4 && @."b" == 7)"#,
        ),
        (
            "$.g ? (@.a == 1 || @.b == 2 || @.c == 3)",
            r#"$."g"?(@."a" == 1 || @."b" == 2 || @."c" == 3)"#,
        ),
        (
            "$.g ? (@.a == 1 || !(@.a == 4) && @.b == 7)",
            r#"$."g"?(@."a" == 1 || !(@."a" == 4) && @."b" == 7)"#,
        ),
        (
            "$.g ? ((@.x >= 123 || @.a == 4) is unknown)",
            r#"$."g"?((@."x" >= 123 || @."a" == 4) is unknown)"#,
        ),
        (
            "($.a.b + -$.x.y).c.d",
            r#"($."a"."b" + -$."x"."y")."c"."d""#,
        ),
        ("1 + ($.a.b > 2).c.d", r#"(1 + ($."a"."b" > 2)."c"."d")"#),
        (
            "$.g ? (@ like_regex \"pattern\" flag \"i\")",
            r#"$."g"?(@ like_regex "pattern" flag "i")"#,
        ),
        (
            "$.g ? (@ like_regex \"pattern\" flag \"isim\")",
            r#"$."g"?(@ like_regex "pattern" flag "ism")"#,
        ),
    ] {
        assert_eq!(
            JsonPath::parse(source).unwrap().to_string(),
            expected,
            "{source}"
        );
    }
}

#[test]
fn unary_numeric_literals_fold_and_dynamic_values_stay_parenthesized() {
    for (source, expected) in [
        ("-1", "-1"),
        ("-(-1)", "1"),
        ("+1", "1"),
        ("+(+1)", "1"),
        ("-$.a", r#"(-$."a")"#),
        ("-(-$.a)", r#"(-(-$."a"))"#),
        ("+$.a", r#"(+$."a")"#),
        ("-($.a + 1)", r#"(-($."a" + 1))"#),
    ] {
        assert_eq!(
            JsonPath::parse(source).unwrap().to_string(),
            expected,
            "{source}"
        );
    }
    let target = jsonb::parse(r#""a""#).expect("target");
    let error = JsonPath::parse("+$")
        .expect("path")
        .query(&target, None, false)
        .expect_err("non-numeric unary plus")
        .into_pg();
    assert_eq!(
        error.message,
        "operand of unary jsonpath operator + is not a numeric value"
    );
}

#[test]
fn datetime_template_uses_the_shared_postgresql_template_parser() {
    let cases = [
        (
            r#""2023-01-02""#,
            r#"$.datetime("YYYY-MM-DD")"#,
            r#"["2023-01-02"]"#,
        ),
        (
            r#""2023-01-02 10:11:12""#,
            r#"$.datetime("YYYY-MM-DD HH24:MI:SS")"#,
            r#"["2023-01-02T10:11:12"]"#,
        ),
        (
            r#""10:11:12""#,
            r#"$.datetime("HH24:MI:SS")"#,
            r#"["10:11:12"]"#,
        ),
        (
            r#""2023-01-02 10:11:12 +03""#,
            r#"$.datetime("YYYY-MM-DD HH24:MI:SS TZH")"#,
            r#"["2023-01-02T10:11:12+03:00"]"#,
        ),
        (
            r#""2023-01-02 10:11:12.345""#,
            r#"$.datetime("YYYY-MM-DD HH24:MI:SS.FF2")"#,
            r#"["2023-01-02T10:11:12.35"]"#,
        ),
        (
            r#""2023-01-02 10:11:12.999""#,
            r#"$.datetime("YYYY-MM-DD HH24:MI:SS.FF2")"#,
            r#"["2023-01-02T10:11:13"]"#,
        ),
    ];
    for (target, path, expected) in cases {
        assert!(query(target, path) == Ok(expected.to_string()), "{path}");
    }
    for (target, path) in [
        (r#""10-03-2017 12:34""#, r#"$.datetime("dd-mm-yyyy")"#),
        (
            r#""10-03-2017t12:34:56""#,
            r#"$.datetime("dd-mm-yyyy\"T\"HH24:MI:SS")"#,
        ),
        (r#""12:34""#, r#"$.datetime("HH24:MI TZH")"#),
    ] {
        assert_eq!(query(target, path), Err("22007".to_string()), "{path}");
    }
}

#[test]
fn compiled_paths_round_trip_through_their_text() {
    for src in [
        "$",
        "strict $",
        "$.a[*]",
        "$.**",
        "$.a.**{5 to last}.b",
        "$.a.**{last}.b",
        "$.a.**{last to 5}.b",
        "$.a?(@ > 1)",
        "$.a.type()",
    ] {
        let once = JsonPath::parse(src).expect(src);
        let twice = JsonPath::parse(&once.to_string()).expect(src);
        assert!(once == twice, "{src}");
    }

    for (src, expected) in [("1.2.e", r#"(1.2)."e""#), ("1?(2>3)", "(1)?(2 > 3)")] {
        assert_eq!(
            JsonPath::parse(src).expect(src).to_string(),
            expected,
            "{src}"
        );
    }
}
