//! The `24:00:00` boundary of `time` and `timetz`.
//!
//! `PostgreSQL` closes the `time` domain at `24:00:00` inclusive: the value is
//! legal to write, `'23:59:60'` and a fraction that rounds past the last
//! microsecond both land on it, and one microsecond more is out of range. It is
//! a real value rather than a spelling — it stores, compares above every other
//! reading, and reads back through `EXTRACT` as hour 24.
//!
//! Every expectation here comes from a live `PostgreSQL` 18.4 oracle with
//! `DateStyle = 'ISO, MDY'` and `TimeZone = 'Etc/UTC'`.

use assert2::assert;
use crabka_pgtypes::{
    TypeError,
    datetime::{
        Interval, PgTime, combine_date_time, make_time, parse_time, parse_timestamp, parse_timetz,
        time_from_binary, time_plus_interval, time_to_binary, time_to_micros_of_day, time_to_text,
        timestamp_to_text, timetz_from_binary, timetz_to_binary, timetz_to_text,
    },
};
use jiff::tz::TimeZone;

fn utc() -> TimeZone {
    TimeZone::UTC
}

/// Microseconds in a day, the value `24:00:00` stores.
const END_OF_DAY_MICROS: i64 = 86_400_000_000;

#[test]
fn time_literals_at_the_boundary_round_up_to_twenty_four_hundred() {
    let cases: &[(&str, &str)] = &[
        // Written out, allowed as-is.
        ("24:00:00", "24:00:00"),
        ("24:00", "24:00:00"),
        ("24:00:00.000000", "24:00:00"),
        // A leap-second spelling rounds up onto the boundary. A zero fraction on
        // it is still the same reading; a non-zero one is refused below.
        ("23:59:60", "24:00:00"),
        ("23:59:60.0", "24:00:00"),
        // So does a fraction that carries past the last microsecond. The tie at
        // exactly one half goes to the even neighbour, which here is the carry.
        ("23:59:59.9999995", "24:00:00"),
        ("23:59:59.9999999", "24:00:00"),
        // One microsecond below the boundary is untouched.
        ("23:59:59.999999", "23:59:59.999999"),
        ("23:59:59.9999994", "23:59:59.999999"),
    ];
    for (input, expected) in cases {
        let parsed = parse_time(input).unwrap_or_else(|e| panic!("{input}: {e}"));
        assert!(time_to_text(parsed) == *expected, "input {input}");
    }
}

#[test]
fn time_literals_past_the_boundary_stay_out_of_range() {
    // Each of these is `date/time field value out of range` in PostgreSQL 18.4.
    // A fix that widens the type must not reach them.
    let cases: &[&str] = &[
        "24:00:00.000001",
        "24:00:00.01",
        "24:00:01",
        "24:01:00",
        "25:00:00",
        // `23:59:60` is the only leap-second reading PostgreSQL takes; a
        // fraction on it is not.
        "23:59:60.5",
        "23:59:60.000001",
        "23:59:61",
    ];
    for input in cases {
        assert!(
            let Err(TypeError::DatetimeFieldOverflow { .. }) = parse_time(input),
            "input {input}"
        );
    }
}

#[test]
fn timetz_literals_follow_the_same_boundary() {
    let accepted: &[(&str, &str)] = &[
        ("24:00:00 PDT", "24:00:00-07"),
        ("23:59:60 PDT", "24:00:00-07"),
        ("23:59:59.9999999 PDT", "24:00:00-07"),
        ("23:59:59.999999 PDT", "23:59:59.999999-07"),
    ];
    for (input, expected) in accepted {
        let parsed = parse_timetz(input, &utc()).unwrap_or_else(|e| panic!("{input}: {e}"));
        assert!(timetz_to_text(parsed) == *expected, "input {input}");
    }

    let refused: &[&str] = &[
        "24:00:00.000001 PDT",
        "24:00:00.01 PDT",
        "24:00:01 PDT",
        "25:00:00 PDT",
        "23:59:60.5 PDT",
    ];
    for input in refused {
        assert!(
            let Err(TypeError::DatetimeFieldOverflow { .. }) = parse_timetz(input, &utc()),
            "input {input}"
        );
    }
}

#[test]
fn the_boundary_is_a_distinct_value_above_every_other_reading() {
    let end = parse_time("24:00:00").expect("24:00:00");
    let last = parse_time("23:59:59.999999").expect("23:59:59.999999");
    let midnight = parse_time("00:00:00").expect("00:00:00");

    assert!(end == PgTime::END_OF_DAY);
    assert!(end > last);
    assert!(end != midnight);
    assert!(midnight == PgTime::MIDNIGHT);
    assert!(time_to_micros_of_day(end) == END_OF_DAY_MICROS);

    // Ordering is total and by the microsecond count, so a sort puts the
    // boundary last rather than folding it back onto midnight.
    let mut sorted = vec![end, midnight, last];
    sorted.sort_unstable();
    assert!(sorted == vec![midnight, last, end]);
}

#[test]
fn the_boundary_reads_back_as_hour_twenty_four() {
    let end = parse_time("24:00:00").expect("24:00:00");
    assert!(
        (
            end.hour(),
            end.minute(),
            end.second(),
            end.subsec_nanosecond()
        ) == (24, 0, 0, 0)
    );
    // It has no civil clock reading, which is why the type is not one.
    assert!(end.to_civil().is_none());

    let ordinary = parse_time("13:45:06.5").expect("13:45:06.5");
    assert!(
        (
            ordinary.hour(),
            ordinary.minute(),
            ordinary.second(),
            ordinary.subsec_nanosecond()
        ) == (13, 45, 6, 500_000_000)
    );
    assert!(ordinary.to_civil() == Some(jiff::civil::time(13, 45, 6, 500_000_000)));
}

#[test]
fn the_boundary_survives_the_binary_wire_format() {
    let end = parse_time("24:00:00").expect("24:00:00");
    assert!(time_to_binary(end) == END_OF_DAY_MICROS.to_be_bytes());
    assert!(time_from_binary(&END_OF_DAY_MICROS.to_be_bytes()).expect("recv") == end);

    let tz_end = parse_timetz("24:00:00 PDT", &utc()).expect("24:00:00 PDT");
    assert!(timetz_from_binary(&timetz_to_binary(tz_end)).expect("recv") == tz_end);

    // One microsecond past the boundary is not a value, on the wire either.
    for micros in [END_OF_DAY_MICROS + 1, -1] {
        assert!(
            let Err(TypeError::DatetimeFieldOverflow { .. }) =
                time_from_binary(&micros.to_be_bytes()),
            "micros {micros}"
        );
    }
}

#[test]
fn time_plus_interval_reduces_the_boundary_into_the_day() {
    // PostgreSQL's `time_pl_interval` takes a whole day off a sum that reaches
    // one, so no shift of a `time` ever *produces* `24:00:00` — not even a zero
    // shift of `24:00:00` itself.
    let shift = |literal: &str, micros: i64| {
        let t = parse_time(literal).unwrap_or_else(|e| panic!("{literal}: {e}"));
        time_to_text(time_plus_interval(
            t,
            Interval {
                months: 0,
                days: 0,
                micros,
            },
        ))
    };
    let cases: &[(&str, i64, &str)] = &[
        ("24:00:00", 0, "00:00:00"),
        ("24:00:00", 1_000_000, "00:00:01"),
        ("24:00:00", -1_000_000, "23:59:59"),
        ("23:59:59", 1_000_000, "00:00:00"),
        ("00:30:00", -3_600_000_000, "23:30:00"),
    ];
    for (literal, micros, expected) in cases {
        assert!(shift(literal, *micros) == *expected, "{literal} + {micros}");
    }
}

#[test]
fn date_plus_the_boundary_lands_on_the_next_day() {
    let end = parse_time("24:00:00").expect("24:00:00");
    let combined = combine_date_time(jiff::civil::date(2020, 1, 1).into(), end).expect("in range");
    assert!(timestamp_to_text(combined) == "2020-01-02 00:00:00");

    let ordinary = parse_time("13:45:06").expect("13:45:06");
    let same_day =
        combine_date_time(jiff::civil::date(2020, 1, 1).into(), ordinary).expect("in range");
    assert!(timestamp_to_text(same_day) == "2020-01-01 13:45:06");
}

#[test]
fn a_timestamp_rounding_past_midnight_carries_the_date_not_the_hour() {
    // The shared rounding path must never leave `24:00:00` sitting on the day it
    // started in: for a timestamp the carry belongs to the date.
    let cases: &[(&str, &str)] = &[
        ("2020-01-01 24:00:00", "2020-01-02 00:00:00"),
        ("2020-01-01 23:59:60", "2020-01-02 00:00:00"),
        ("2020-01-01 23:59:59.9999999", "2020-01-02 00:00:00"),
        ("2020-01-01 23:59:59.999999", "2020-01-01 23:59:59.999999"),
    ];
    for (input, expected) in cases {
        let parsed = parse_timestamp(input).unwrap_or_else(|e| panic!("{input}: {e}"));
        assert!(timestamp_to_text(parsed) == *expected, "input {input}");
    }
}

#[test]
fn make_time_builds_the_boundary_and_nothing_past_it() {
    let cases: &[(i32, i32, f64, Option<&str>)] = &[
        (24, 0, 0.0, Some("24:00:00")),
        (13, 45, 6.5, Some("13:45:06.5")),
        (24, 0, 1.0, None),
        (24, 1, 0.0, None),
        (24, 0, 0.000_001, None),
        (25, 0, 0.0, None),
    ];
    for (hour, minute, second, expected) in cases {
        match (make_time(*hour, *minute, *second), expected) {
            (Ok(t), Some(text)) => {
                assert!(
                    time_to_text(t) == *text,
                    "make_time({hour},{minute},{second})"
                );
            }
            (Err(e), None) => assert!(e.sqlstate() == "22008"),
            (got, _) => panic!("make_time({hour},{minute},{second}) gave {got:?}"),
        }
    }
}
