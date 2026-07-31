use assert2::assert;
use crabka_pgparser::parser::parse_expr_for_test as pexpr;
use crabka_pgtypes::{ColumnType, Datum};

use super::Prng;
use crate::{clock::EvalCtx, scope::Scope};

/// Evaluate a FROM-less expression and render it the way the wire would, so a
/// case's expectation is exactly the text PostgreSQL prints.
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

/// The static result type inference reports for a FROM-less call.
fn result_type(sql: &str) -> ColumnType {
    let expr = pexpr(sql).expect("parse");
    crate::eval::infer_type(&expr, &Scope::empty()).expect("infer")
}

#[test]
fn number_theory_matches_postgres() {
    let cases = [
        ("gcd(8, 12)", "4"),
        ("gcd(0, 0)", "0"),
        ("gcd(-4, 6)", "2"),
        ("gcd(4, -6)", "2"),
        ("gcd(0, 5)", "5"),
        ("gcd(5, 0)", "5"),
        ("gcd(1.5, 2.5)", "0.5"),
        ("lcm(4, 6)", "12"),
        ("lcm(0, 5)", "0"),
        ("lcm(-4, 6)", "12"),
        ("lcm(1.5, 2.5)", "7.5"),
        ("factorial(0)", "1"),
        ("factorial(1)", "1"),
        ("factorial(5)", "120"),
        ("div(9, 4)", "2"),
        ("div(-9, 4)", "-2"),
        ("div(9, -4)", "-2"),
        ("div(9.9, 3.3)", "3"),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
}

#[test]
fn number_theory_overflow_and_domain_errors() {
    let cases = [
        ("gcd((-2147483648)::int4, 0)", "22003"),
        ("lcm(2147483647, 2147483646)", "22003"),
        ("factorial(-1)", "22003"),
        ("div(9, 0)", "22012"),
    ];
    for (sql, expected) in cases {
        assert!(sqlstate(sql) == expected, "{sql}");
    }
}

#[test]
fn numeric_scale_introspection_matches_postgres() {
    let cases = [
        ("scale(1.230)", "3"),
        ("scale(1)", "0"),
        ("min_scale(1.230)", "2"),
        ("min_scale(1.200)", "1"),
        ("min_scale(0)", "0"),
        ("min_scale(1.0)", "0"),
        ("trim_scale(1.230)", "1.23"),
        ("trim_scale(1.000)", "1"),
        ("trim_scale(0.000)", "0"),
        ("trim_scale(100)", "100"),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
}

#[test]
fn width_bucket_numbers_both_bound_orders() {
    let cases = [
        ("width_bucket(5.0, 1.0, 10.0, 3)", "2"),
        ("width_bucket(0.0, 1.0, 10.0, 3)", "0"),
        ("width_bucket(20.0, 1.0, 10.0, 3)", "4"),
        ("width_bucket(2.5, 1, 10, 3)", "1"),
        // Reversed bounds count from the other end.
        ("width_bucket(1.0, 10.0, 1.0, 3)", "4"),
        ("width_bucket(5, 10, 1, 3)", "2"),
        (
            "width_bucket(5.0::float8, 1.0::float8, 10.0::float8, 3)",
            "2",
        ),
        ("width_bucket(11::float8, 10::float8, 1::float8, 3)", "0"),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
    assert!(sqlstate("width_bucket(1.0, 1.0, 1.0, 3)") == "2201G");
    assert!(sqlstate("width_bucket(1.0, 1.0, 10.0, 0)") == "2201G");
}

/// PostgreSQL guarantees the degree functions return EXACT answers at the
/// quadrant marks; a naive `sin(x * pi / 180)` misses several of these.
#[test]
fn degree_trigonometry_is_exact_at_the_quadrant_marks() {
    let cases = [
        ("sind(0)", "0"),
        ("sind(30)", "0.5"),
        ("sind(90)", "1"),
        ("sind(180)", "0"),
        ("sind(270)", "-1"),
        ("sind(360)", "0"),
        ("cosd(0)", "1"),
        ("cosd(60)", "0.5"),
        ("cosd(90)", "0"),
        ("cosd(180)", "-1"),
        ("cosd(270)", "0"),
        ("tand(0)", "0"),
        ("tand(45)", "1"),
        ("tand(90)", "Infinity"),
        ("tand(135)", "-1"),
        ("cotd(45)", "1"),
        ("asind(0)", "0"),
        ("asind(0.5)", "30"),
        ("asind(1)", "90"),
        ("asind(-1)", "-90"),
        ("acosd(0)", "90"),
        ("acosd(0.5)", "60"),
        ("acosd(1)", "0"),
        ("acosd(-1)", "180"),
        ("atand(1)", "45"),
        ("atan2d(1, 1)", "45"),
        ("atan2d(1, 0)", "90"),
        ("atan2d(0, 1)", "0"),
        ("atan2d(-1, 0)", "-90"),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
}

#[test]
fn transcendental_domain_errors() {
    let cases = [
        ("asin(2)", "22003"),
        ("acos(2)", "22003"),
        ("asind(2)", "22003"),
        ("acosd(2)", "22003"),
        ("acosh(0.5)", "22003"),
        ("atanh(2)", "22003"),
        ("sin('Infinity'::float8)", "22003"),
        ("sind('Infinity'::float8)", "22003"),
        ("log10(0)", "2201E"),
    ];
    for (sql, expected) in cases {
        assert!(sqlstate(sql) == expected, "{sql}");
    }
}

#[test]
fn hyperbolic_and_root_values() {
    let cases = [
        ("sinh(0)", "0"),
        ("cosh(0)", "1"),
        ("tanh(0)", "0"),
        ("asinh(0)", "0"),
        ("acosh(1)", "0"),
        ("atanh(0)", "0"),
        ("atanh(1)", "Infinity"),
        ("degrees(0)", "0"),
        ("radians(0)", "0"),
        ("log10(1)", "0"),
        ("log10(100)", "2"),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
}

/// PostgreSQL's `cbrt` is the C library's, which is NOT correctly rounded —
/// `cbrt(27)` lands one ULP above 3. Rust's `f64::cbrt` returns exactly 3, so
/// the ported routine is what keeps crabka's answer identical to the oracle's.
#[test]
fn cbrt_reproduces_the_c_library_rounding() {
    let cases = [
        ("cbrt(1)", "1"),
        ("cbrt(8)", "2"),
        ("cbrt(27)", "3.0000000000000004"),
        ("cbrt(-27)", "-3.0000000000000004"),
        ("cbrt(64)", "4"),
        ("cbrt(125)", "5"),
        ("cbrt(1000)", "10"),
        ("cbrt(0.001)", "0.1"),
        ("cbrt(2)", "1.2599210498948734"),
        ("cbrt(10)", "2.1544346900318834"),
        ("cbrt(0)", "0"),
    ];
    for (sql, expected) in cases {
        assert!(text_of(sql) == expected, "{sql}");
    }
}

#[test]
fn result_types_follow_the_argument_family() {
    let cases = [
        ("gcd(8, 12)", ColumnType::Int4),
        ("gcd(8::int8, 12)", ColumnType::Int8),
        ("gcd(1.5, 2.5)", ColumnType::Numeric(None)),
        ("factorial(5)", ColumnType::Numeric(None)),
        ("div(9, 4)", ColumnType::Numeric(None)),
        ("scale(1.5)", ColumnType::Int4),
        ("min_scale(1.5)", ColumnType::Int4),
        ("trim_scale(1.5)", ColumnType::Numeric(None)),
        ("width_bucket(5.0, 1.0, 10.0, 3)", ColumnType::Int4),
        ("sind(30)", ColumnType::Float8),
        ("cbrt(8)", ColumnType::Float8),
        ("log10(100)", ColumnType::Float8),
        ("log10(100.0)", ColumnType::Numeric(None)),
        ("random()", ColumnType::Float8),
        ("random(1, 10)", ColumnType::Int4),
        ("random(1::int8, 10::int8)", ColumnType::Int8),
        ("random(1.0, 10.0)", ColumnType::Numeric(None)),
    ];
    for (sql, expected) in cases {
        assert!(result_type(sql) == expected, "{sql}");
    }
}

/// Every function in the module is strict: one NULL argument makes the result
/// NULL rather than an error.
#[test]
fn every_math_function_is_strict() {
    let cases = [
        "gcd(NULL::int4, 1)",
        "lcm(NULL::int4, 1)",
        "factorial(NULL)",
        "div(NULL, 1)",
        "scale(NULL::numeric)",
        "min_scale(NULL::numeric)",
        "trim_scale(NULL::numeric)",
        "width_bucket(NULL, 1, 10, 3)",
        "sind(NULL)",
        "cbrt(NULL)",
        "log10(NULL)",
        "atan2(NULL, 1)",
        "setseed(NULL)",
    ];
    for sql in cases {
        assert!(text_of(sql) == "<null>", "{sql}");
    }
}

#[test]
fn setseed_rejects_a_seed_outside_the_unit_interval() {
    assert!(sqlstate("setseed(2)") == "22023");
    assert!(sqlstate("setseed(-1.5)") == "22023");
    // The endpoints are inclusive, and the result is `void`'s empty rendering.
    assert!(text_of("setseed(1)").is_empty());
    assert!(text_of("setseed(-1)").is_empty());
}

#[test]
fn random_stays_inside_its_bounds() {
    assert!(text_of("random(5, 5)") == "5");
    assert!(text_of("random(-3, -3)") == "-3");
    assert!(text_of("random(2.50, 2.50)") == "2.50");
    assert!(text_of("random() >= 0 AND random() < 1") == "t");
    assert!(text_of("random(1, 10) BETWEEN 1 AND 10") == "t");
    assert!(sqlstate("random(10, 1)") == "22023");
}

/// The same seed must replay the same stream, and different seeds must not
/// share one — the property `setseed` exists to provide.
#[test]
fn the_generator_is_reproducible_per_seed() {
    // Compare the raw generator words rather than the doubles derived from
    // them: identical words are a strictly stronger reproducibility claim, and
    // the derived `[0, 1)` range is asserted separately below.
    let draw = |seed: f64| {
        let mut prng = Prng::seeded(0);
        prng.seed_double(seed);
        [prng.next_u64(), prng.next_u64(), prng.next_u64()]
    };
    assert!(draw(0.5) == draw(0.5));
    assert!(draw(0.5) != draw(0.25));
    let mut prng = Prng::seeded(0);
    prng.seed_double(0.5);
    for _ in 0..64 {
        let value = prng.next_double();
        assert!(value >= 0.0 && value < 1.0);
    }
}

/// The bounded draw must never leave `[0, range]`, for ranges either side of a
/// power-of-two boundary (where the rejection loop does the work).
#[test]
fn the_bounded_draw_never_escapes_its_range() {
    let mut prng = Prng::seeded(12345);
    for range in [0, 1, 2, 3, 7, 8, 9, 99, 1_000_000, u64::MAX] {
        for _ in 0..64 {
            assert!(prng.next_below(range) <= range, "range {range}");
        }
    }
}
