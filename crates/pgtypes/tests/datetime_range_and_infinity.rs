//! `interval` scaling and subtraction, the `make_*` constructors, and the way
//! the non-finite `date` and `interval` values travel through both.
//!
//! Every expected value here was read off a running `PostgreSQL` 18.4, not
//! inferred from the C, because the two disagree in places the C does not make
//! obvious — `interval '15 days 12:00:00' * 2` is `30 days 24:00:00` and not
//! `31 days`, and `interval '1 hour' / 'infinity'` is `00:00:00` rather than an
//! error.

use assert2::assert;
use crabka_pgtypes::{
    TypeError,
    datetime::{
        DATE_INFINITY, DATE_NEG_INFINITY, Interval, PgTime, TIMESTAMP_INFINITY,
        TIMESTAMP_NEG_INFINITY, combine_date_time, div_interval, interval_to_time, justify_days,
        justify_hours, justify_interval, make_date, make_interval, make_time, make_timestamp_civil,
        mul_interval, parse_date, parse_interval, parse_time, sub_interval,
    },
};

/// The three stored fields. `Interval`'s own equality compares the canonical
/// 30-day-month value, which cannot tell `1 mon 6 days` from
/// `1 mon 5 days 24:00:00` — and telling those apart is the whole point of the
/// cascade rules.
fn fields(iv: Interval) -> (i32, i32, i64) {
    (iv.months, iv.days, iv.micros)
}

fn interval(text: &str) -> Interval {
    parse_interval(text).unwrap_or_else(|error| panic!("interval {text:?}: {error}"))
}

/// `PostgreSQL`'s `INTERVAL_MULDIV_TBL` and the four scalings the `interval`
/// regression test applies to it. The rows exercise every branch of the
/// cascade: a fraction of a month landing in days, a fraction of a day landing
/// in microseconds, a seconds remainder that grows past a whole day and has to
/// be lifted back out, and mixed signs throughout.
#[test]
fn scaling_an_interval_cascades_fractions_down_and_never_back_up() {
    let rows: &[(&str, &str, &str, &str, &str)] = &[
        (
            "41 mon 12 days 360:00",
            "1 year 12 days 122:24:00",
            "28 years 104 days 2961:36:00",
            "4 mons 4 days 40:48:00",
            "12 days 13:40:48",
        ),
        (
            "-41 mon -12 days +360:00",
            "-1 years -12 days +93:36:00",
            "-28 years -104 days +2942:24:00",
            "-4 mons -4 days +31:12:00",
            "-12 days -06:28:48",
        ),
        (
            "-12 days",
            "-3 days -14:24:00",
            "-98 days -09:36:00",
            "-1 days -04:48:00",
            "-02:52:48",
        ),
        (
            "9 mon -27 days 12:34:56",
            "2 mons 13 days 01:22:28.8",
            "6 years 1 mon -197 days +93:34:27.2",
            "25 days -15:32:30.4",
            "2 days 10:26:44.96",
        ),
        (
            "-3 years 482 days 76:54:32.189",
            "-10 mons +120 days 37:28:21.6567",
            "-24 years -7 mons +3946 days 640:15:11.9498",
            "-3 mons +30 days 12:29:27.2189",
            "-6 days +01:14:56.72189",
        ),
        (
            "4 mon",
            "1 mon 6 days",
            "2 years 8 mons 24 days",
            "12 days",
            "1 day 04:48:00",
        ),
        (
            "14 mon",
            "4 mons 6 days",
            "9 years 6 mons 24 days",
            "1 mon 12 days",
            "4 days 04:48:00",
        ),
        (
            "999 mon 999 days",
            "24 years 11 mons 320 days 16:48:00",
            "682 years 7 mons 8215 days 19:12:00",
            "8 years 3 mons 126 days 21:36:00",
            "9 mons 39 days 16:33:36",
        ),
    ];
    for (span, times_point_three, times_eight_point_two, over_ten, over_hundred) in rows {
        let span_value = interval(span);
        assert!(
            fields(mul_interval(span_value, 0.3).expect("product"))
                == fields(interval(times_point_three)),
            "{span} * 0.3"
        );
        assert!(
            fields(mul_interval(span_value, 8.2).expect("product"))
                == fields(interval(times_eight_point_two)),
            "{span} * 8.2"
        );
        assert!(
            fields(div_interval(span_value, 10.0).expect("quotient")) == fields(interval(over_ten)),
            "{span} / 10"
        );
        assert!(
            fields(div_interval(span_value, 100.0).expect("quotient"))
                == fields(interval(over_hundred)),
            "{span} / 100"
        );
    }
}

/// Dividing is not multiplying by the reciprocal. `1/3` is not exact in binary,
/// so the two routes disagree, and `PostgreSQL` divides.
#[test]
fn dividing_is_not_multiplying_by_the_reciprocal() {
    assert!(fields(div_interval(interval("1 mon"), 3.0).expect("quotient")) == (0, 10, 0));
    assert!(fields(div_interval(interval("4 mon"), 10.0).expect("quotient")) == (0, 12, 0));
}

/// An infinite operand carries through with the product's sign. The two
/// combinations that would name a quantity with no sign at all are refused,
/// because `interval` has no `NaN` to return.
#[test]
fn scaling_carries_an_infinity_through_with_the_result_sign() {
    let cases: &[(&str, f64, Option<&str>)] = &[
        ("5 days", f64::INFINITY, Some("infinity")),
        ("5 days", f64::NEG_INFINITY, Some("-infinity")),
        ("-5 days", f64::INFINITY, Some("-infinity")),
        ("-5 days", f64::NEG_INFINITY, Some("infinity")),
        ("infinity", 2.0, Some("infinity")),
        ("infinity", -2.0, Some("-infinity")),
        ("-infinity", -2.0, Some("infinity")),
        // A zero-length interval has no sign for an infinite factor to take.
        ("0 days", f64::INFINITY, None),
        ("0 days", f64::NEG_INFINITY, None),
        // An infinite interval times nothing is the same impasse.
        ("infinity", 0.0, None),
        ("-infinity", 0.0, None),
        ("infinity", f64::NAN, None),
        ("5 days", f64::NAN, None),
    ];
    for (span, factor, expected) in cases {
        let product = mul_interval(interval(span), *factor);
        if let Some(text) = expected {
            assert!(
                fields(product.expect("product")) == fields(interval(text)),
                "{span} * {factor}"
            );
        } else {
            let refused = product.expect_err("no signed answer");
            assert!(
                refused.to_string() == "interval out of range",
                "{span} * {factor}"
            );
            assert!(refused.sqlstate() == "22008");
        }
    }
}

/// Dividing a FINITE interval by an infinity is ordinary arithmetic: every
/// field goes to zero. Only `infinity/infinity` has no answer, and only a zero
/// divisor is a division by zero.
#[test]
fn dividing_by_an_infinity_empties_a_finite_interval() {
    for divisor in [f64::INFINITY, f64::NEG_INFINITY] {
        assert!(fields(div_interval(interval("1 hour"), divisor).expect("quotient")) == (0, 0, 0));
        let refused = div_interval(interval("infinity"), divisor).expect_err("no answer");
        assert!(refused.to_string() == "interval out of range");
    }
    assert!(
        fields(div_interval(interval("infinity"), 3.0).expect("quotient"))
            == fields(Interval::INFINITY)
    );
    assert!(
        fields(div_interval(interval("infinity"), -3.0).expect("quotient"))
            == fields(Interval::NEG_INFINITY)
    );
    assert!(let Err(TypeError::DivisionByZero) = div_interval(interval("1 hour"), 0.0));
    assert!(
        div_interval(interval("1 hour"), f64::NAN)
            .expect_err("no answer")
            .to_string()
            == "interval out of range"
    );
}

/// Subtracting an interval from itself is zero even at the bottom of the range,
/// where negating the right operand on its own would leave it.
#[test]
fn subtracting_works_on_fields_rather_than_by_negating() {
    let extremes = [
        "2147483647 months 2147483647 days 9223372036854775806 us",
        "-2147483648 months -2147483648 days -9223372036854775807 us",
    ];
    for text in extremes {
        let span = interval(text);
        assert!(
            fields(sub_interval(span, span).expect("zero")) == (0, 0, 0),
            "{text}"
        );
    }
}

/// The infinity table for subtraction: same-signed infinities cancel to nothing
/// definable, and otherwise the infinite side decides.
#[test]
fn subtracting_infinities_follows_the_difference_sign() {
    let cases: &[(&str, &str, Option<&str>)] = &[
        ("infinity", "infinity", None),
        ("-infinity", "-infinity", None),
        ("infinity", "-infinity", Some("infinity")),
        ("-infinity", "infinity", Some("-infinity")),
        ("infinity", "2 months", Some("infinity")),
        ("2 months", "infinity", Some("-infinity")),
        ("2 months", "-infinity", Some("infinity")),
    ];
    for (left, right, expected) in cases {
        let difference = sub_interval(interval(left), interval(right));
        if let Some(text) = expected {
            assert!(
                fields(difference.expect("difference")) == fields(interval(text)),
                "{left} - {right}"
            );
        } else {
            assert!(
                difference.expect_err("no answer").to_string() == "interval out of range",
                "{left} - {right}"
            );
        }
    }
}

/// A `date + time` on a non-finite date is that infinity, not a clock reading
/// on the last representable day.
#[test]
fn a_non_finite_date_swallows_the_time_it_is_combined_with() {
    let noon = parse_time("12:00:00").expect("time");
    assert!(combine_date_time(DATE_INFINITY, noon) == Some(TIMESTAMP_INFINITY));
    assert!(combine_date_time(DATE_NEG_INFINITY, noon) == Some(TIMESTAMP_NEG_INFINITY));
    // A finite date still carries the reading, `24:00:00` included.
    let day = parse_date("2020-01-01").expect("date");
    let end_of_day = parse_time("24:00:00").expect("time");
    assert!(combine_date_time(day, PgTime::MIDNIGHT).is_some());
    assert!(
        combine_date_time(day, end_of_day) == Some(parse_date("2020-01-02").expect("date").into())
    );
}

/// A literal whose fields are all in range but whose day is not reachable is
/// reported by the TYPE, not by a field. The order is observable: the field
/// checks run first, so a bad day inside an unreachable year is still a field
/// fault.
#[test]
fn a_literal_out_of_the_types_range_is_named_by_the_type() {
    let dates: &[(&str, &str, Option<&str>)] = &[
        (
            "6874898-01-01",
            "date out of range: \"6874898-01-01\"",
            None,
        ),
        (
            "5874898-01-01",
            "date out of range: \"5874898-01-01\"",
            None,
        ),
        (
            "4714-11-23 BC",
            "date out of range: \"4714-11-23 BC\"",
            None,
        ),
        (
            "5874898-02-30",
            "date/time field value out of range: \"5874898-02-30\"",
            None,
        ),
        (
            "1997-02-29",
            "date/time field value out of range: \"1997-02-29\"",
            None,
        ),
        (
            "10000-13-01",
            "date/time field value out of range: \"10000-13-01\"",
            Some("Perhaps you need a different \"DateStyle\" setting."),
        ),
    ];
    for (literal, expected, hint) in dates {
        let refused = parse_date(literal).expect_err("refused");
        assert!(refused.to_string() == *expected, "date {literal:?}");
        assert!(refused.sqlstate() == "22008", "date {literal:?}");
        assert!(refused.hint() == *hint, "date {literal:?}");
    }
    // `timestamptz` borrows `timestamp`'s wording: the offset plays no part in
    // the day being unreachable.
    for literal in ["294277-01-01", "6874898-06-01"] {
        let expected = format!("timestamp out of range: \"{literal}\"");
        assert!(
            crabka_pgtypes::datetime::parse_timestamp(literal)
                .expect_err("refused")
                .to_string()
                == expected
        );
    }
}

/// Reading an `interval` as a `time` keeps only the microseconds and takes the
/// floor of what is left, so a negative interval reads as a clock time near the
/// end of the day rather than as a negative one.
#[test]
fn an_interval_reads_as_a_time_by_its_microseconds_alone() {
    let cases: &[(&str, &str)] = &[
        ("25:00:00", "01:00:00"),
        ("-1 hour", "23:00:00"),
        ("1 month 2 days 3 hours", "03:00:00"),
        ("-25:00:00", "23:00:00"),
        ("0", "00:00:00"),
    ];
    for (span, expected) in cases {
        let read = interval_to_time(interval(span)).unwrap_or_else(|e| panic!("{span}: {e}"));
        assert!(
            crabka_pgtypes::datetime::time_to_text(read) == *expected,
            "interval {span:?} as time"
        );
    }
    for span in ["infinity", "-infinity"] {
        let refused = interval_to_time(interval(span)).expect_err("no reading");
        assert!(refused.to_string() == "cannot convert infinite interval to time");
        assert!(refused.sqlstate() == "22008");
    }
}

/// `make_date` reads a negative year as the BC era, so there is no year zero on
/// either side of the boundary and `-44` is 44 BC rather than the astronomical
/// year -44.
#[test]
fn make_date_reads_a_negative_year_as_the_bc_era() {
    let cases: &[(i32, i32, i32, &str)] = &[
        (-44, 3, 15, "0044-03-15 BC"),
        (-1, 1, 1, "0001-01-01 BC"),
        (2013, 2, 28, "2013-02-28"),
        (-4714, 11, 24, "4714-11-24 BC"),
    ];
    for (year, month, day, expected) in cases {
        let built = make_date(*year, *month, *day)
            .unwrap_or_else(|error| panic!("make_date({year},{month},{day}): {error}"));
        assert!(
            crabka_pgtypes::datetime::date_to_text(built) == *expected,
            "make_date({year},{month},{day})"
        );
    }
}

/// The `make_*` constructors word their complaints the way `PostgreSQL` words
/// them: the fields unquoted, the month and day zero-padded to two characters
/// with the sign counting towards the width, and the seconds spelled `%02g`.
#[test]
fn the_make_constructors_word_a_refused_field_as_postgresql_does() {
    let dates: &[(i32, i32, i32, &str)] = &[
        (0, 7, 15, "date field value out of range: 0-07-15"),
        (2013, 2, 30, "date field value out of range: 2013-02-30"),
        (2013, 13, 1, "date field value out of range: 2013-13-01"),
        (2013, 11, -1, "date field value out of range: 2013-11--1"),
        (-44, 13, 1, "date field value out of range: -43-13-01"),
        (
            i32::MIN,
            1,
            1,
            "date field value out of range: -2147483648-01-01",
        ),
    ];
    for (year, month, day, expected) in dates {
        let refused = make_date(*year, *month, *day)
            .expect_err("refused")
            .to_string();
        assert!(refused == *expected, "make_date({year},{month},{day})");
    }

    let times: &[(i32, i32, f64, &str)] = &[
        (10, 55, 100.1, "time field value out of range: 10:55:100.1"),
        (24, 0, 2.1, "time field value out of range: 24:00:2.1"),
        (-1, 0, 0.0, "time field value out of range: -1:00:00"),
        (1, -2, 3.0, "time field value out of range: 1:-2:03"),
        (
            1,
            2,
            1_234_567.0,
            "time field value out of range: 1:02:1.23457e+06",
        ),
        (1, 2, 1e20, "time field value out of range: 1:02:1e+20"),
        (
            1,
            2,
            f64::INFINITY,
            "time field value out of range: 1:02:Infinity",
        ),
        (
            1,
            2,
            f64::NEG_INFINITY,
            "time field value out of range: 1:02:-Infinity",
        ),
        (1, 2, f64::NAN, "time field value out of range: 1:02:NaN"),
        (
            1,
            2,
            123_456.75,
            "time field value out of range: 1:02:123457",
        ),
        (1, 2, -0.5, "time field value out of range: 1:02:-0.5"),
        (
            1,
            2,
            1_000_000.5,
            "time field value out of range: 1:02:1e+06",
        ),
    ];
    for (hour, min, sec, expected) in times {
        let refused = make_time(*hour, *min, *sec)
            .expect_err("refused")
            .to_string();
        assert!(refused == *expected, "make_time({hour},{min},{sec})");
    }
}

/// `make_time` sums the fields rather than assembling them, so 60 seconds rolls
/// into the next minute and only the total is bounded.
#[test]
fn make_time_sums_its_fields_and_bounds_only_the_total() {
    let cases: &[(i32, i32, f64, &str)] = &[
        (1, 2, 0.0, "01:02:00"),
        (1, 2, 60.0, "01:03:00"),
        (1, 2, 59.999_999_9, "01:03:00"),
        (13, 45, 6.5, "13:45:06.5"),
        (24, 0, 0.0, "24:00:00"),
        (23, 59, 60.0, "24:00:00"),
    ];
    for (hour, min, sec, expected) in cases {
        let built = make_time(*hour, *min, *sec)
            .unwrap_or_else(|error| panic!("make_time({hour},{min},{sec}): {error}"));
        assert!(
            crabka_pgtypes::datetime::time_to_text(built) == *expected,
            "make_time({hour},{min},{sec})"
        );
    }
}

/// `make_timestamp` borrows both halves' complaints, and reports its own only
/// for a field set both halves accept.
#[test]
fn make_timestamp_borrows_the_wording_of_the_half_that_refused() {
    assert!(
        make_timestamp_civil(0, 3, 15, 1, 2, 3.0)
            .expect_err("refused")
            .to_string()
            == "date field value out of range: 0-03-15"
    );
    assert!(
        make_timestamp_civil(2013, 2, 3, 25, 2, 3.0)
            .expect_err("refused")
            .to_string()
            == "time field value out of range: 25:02:03"
    );
    let built = make_timestamp_civil(-44, 3, 15, 1, 2, 3.0).expect("44 BC");
    assert!(crabka_pgtypes::datetime::timestamp_to_text(built) == "0044-03-15 01:02:03 BC");
}

/// Every `interval` overflow is `interval out of range`. `integer out of range`
/// names a type that is not involved, and the `make_*`/`justify_*` helpers used
/// to leak their own function name into the message.
#[test]
fn every_interval_overflow_is_worded_as_an_interval_overflow() {
    let refusals: Vec<TypeError> = vec![
        make_interval(178_956_971, 0, 0, 0, 0, 0, 0.0).expect_err("years"),
        make_interval(1, 2_147_483_647, 0, 0, 0, 0, 0.0).expect_err("months"),
        make_interval(0, 0, 306_783_379, 0, 0, 0, 0.0).expect_err("weeks"),
        make_interval(0, 0, 0, 0, 0, 0, 1e18).expect_err("seconds"),
        make_interval(0, 0, 0, 0, 0, 1, 9_223_372_036_800.0).expect_err("minutes and seconds"),
        make_interval(0, 0, 0, 0, 0, 0, f64::INFINITY).expect_err("infinite seconds"),
        make_interval(0, 0, 0, 0, 0, 0, f64::NAN).expect_err("seconds is not a number"),
        justify_hours(interval("2147483647 days 24 hrs")).expect_err("justify_hours"),
        justify_days(interval("2147483647 months 30 days")).expect_err("justify_days"),
        justify_interval(interval("2147483647 months 30 days")).expect_err("justify_interval"),
    ];
    for refused in refusals {
        assert!(refused.to_string() == "interval out of range");
        assert!(refused.sqlstate() == "22008");
    }
    // Scaling the seconds argument past `f64` is the multiplication overflowing,
    // not the interval, and PostgreSQL says so.
    let refused = make_interval(0, 0, 0, 0, 0, 0, 1e308).expect_err("seconds");
    assert!(refused.to_string() == "value out of range: overflow");
    assert!(refused.sqlstate() == "22003");
}
