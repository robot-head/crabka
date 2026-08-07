//! Date/time literal decoding, checked against `PostgreSQL` 18.4's accepted set.
//!
//! Every expectation here comes from a live `PostgreSQL` 18.4 oracle with
//! `DateStyle = 'ISO, MDY'` and `TimeZone = 'Etc/UTC'`.

use assert2::assert;
use crabka_pgtypes::{
    TypeError,
    datetime::{
        DateOrder, date_to_text, interval_to_text, parse_date, parse_date_in, parse_interval,
        parse_time, parse_timestamp, parse_timestamptz, time_to_text, timestamp_to_text,
        timestamptz_to_text,
    },
};
use jiff::tz::TimeZone;

fn utc() -> TimeZone {
    TimeZone::UTC
}

#[test]
fn timestamp_input_spellings_match_postgres() {
    let cases: &[(&str, &str)] = &[
        ("Mon Feb 10 17:32:01 1997 PST", "1997-02-10 17:32:01"),
        ("Mon Feb 10 17:32:01.4 1997 PST", "1997-02-10 17:32:01.4"),
        (
            "Mon Feb 10 17:32:01.999999 1997 PST",
            "1997-02-10 17:32:01.999999",
        ),
        ("1997-01-02", "1997-01-02 00:00:00"),
        ("1997-01-02 03:04:05", "1997-01-02 03:04:05"),
        ("1997-02-10 17:32:01-08", "1997-02-10 17:32:01"),
        ("1997-02-10 17:32:01-0800", "1997-02-10 17:32:01"),
        ("1997-02-10 17:32:01 -08:00", "1997-02-10 17:32:01"),
        ("19970210 173201 -0800", "1997-02-10 17:32:01"),
        ("2001-09-22T18:19:20", "2001-09-22 18:19:20"),
        ("2000-03-15 08:14:01 GMT+8", "2000-03-15 08:14:01"),
        ("Feb 10 17:32:01 1997 -0800", "1997-02-10 17:32:01"),
        ("Feb 10 17:32:01 1997", "1997-02-10 17:32:01"),
        ("Feb 10 5:32PM 1997", "1997-02-10 17:32:00"),
        ("1997/02/10 17:32:01-0800", "1997-02-10 17:32:01"),
        ("1997-02-10 17:32:01 PST", "1997-02-10 17:32:01"),
        ("Feb-10-1997 17:32:01 PST", "1997-02-10 17:32:01"),
        ("02-10-1997 17:32:01 PST", "1997-02-10 17:32:01"),
        ("19970210 173201 PST", "1997-02-10 17:32:01"),
        ("1997.041 17:32:01 UTC", "1997-02-10 17:32:01"),
        ("19970210 173201 America/New_York", "1997-02-10 17:32:01"),
        ("Feb 16 17:32:01 0097 BC", "0097-02-16 17:32:01 BC"),
        ("Feb 16 17:32:01 0097", "0097-02-16 17:32:01"),
        ("Feb 16 17:32:01 2097", "2097-02-16 17:32:01"),
        ("Feb 29 17:32:01 1996", "1996-02-29 17:32:01"),
        ("epoch", "1970-01-01 00:00:00"),
        ("infinity", "infinity"),
        ("-infinity", "-infinity"),
        ("+infinity", "infinity"),
        ("1999-12-31 24:00:00", "2000-01-01 00:00:00"),
    ];
    for (input, expected) in cases {
        let parsed = parse_timestamp(input).unwrap_or_else(|e| panic!("{input}: {e}"));
        assert!(timestamp_to_text(parsed) == *expected, "input {input}");
    }
}

#[test]
fn timestamptz_input_applies_the_literal_zone() {
    let cases: &[(&str, &str)] = &[
        ("Mon Feb 10 17:32:01 1997 PST", "1997-02-11 01:32:01+00"),
        ("1997-02-10 17:32:01-08", "1997-02-11 01:32:01+00"),
        ("2000-03-15 08:14:01 GMT+8", "2000-03-15 16:14:01+00"),
        ("2000-03-15 13:14:02 GMT-1", "2000-03-15 12:14:02+00"),
        ("2000-03-15 03:14:04 PST+8", "2000-03-15 11:14:04+00"),
        ("2000-03-15 02:14:05 MST+7:00", "2000-03-15 09:14:05+00"),
        ("19970210 173201 America/New_York", "1997-02-10 22:32:01+00"),
        ("2011-03-27 00:00:00 MSK", "2011-03-26 21:00:00+00"),
        (
            "2011-03-27 00:00:00 Europe/Moscow",
            "2011-03-26 21:00:00+00",
        ),
        ("2014-10-26 00:00:00 MSK", "2014-10-25 20:00:00+00"),
        ("2001-09-22T18:19:20Z", "2001-09-22 18:19:20+00"),
        ("Feb 16 17:32:01 0097 BC", "0097-02-16 17:32:01+00 BC"),
        ("infinity", "infinity"),
        ("-infinity", "-infinity"),
    ];
    for (input, expected) in cases {
        let parsed = parse_timestamptz(input, &utc()).unwrap_or_else(|e| panic!("{input}: {e}"));
        assert!(
            timestamptz_to_text(parsed, &utc()) == *expected,
            "input {input}"
        );
    }
}

#[test]
fn date_input_spellings_match_postgres_in_mdy_order() {
    let cases: &[(&str, &str)] = &[
        ("1957-04-09", "1957-04-09"),
        ("2040-04-10 BC", "2040-04-10 BC"),
        ("January 8, 1999", "1999-01-08"),
        ("1/8/1999", "1999-01-08"),
        ("19990108", "1999-01-08"),
        ("990108", "1999-01-08"),
        ("1999.008", "1999-01-08"),
        ("J2451187", "1999-01-08"),
        ("January 8, 99 BC", "0099-01-08 BC"),
        ("1999-Jan-08", "1999-01-08"),
        ("08-Jan-1999", "1999-01-08"),
        ("Jan-08-1999", "1999-01-08"),
        ("1999 Jan 08", "1999-01-08"),
        ("08 Jan 1999", "1999-01-08"),
        ("Jan 08 1999", "1999-01-08"),
        ("01-08-1999", "1999-01-08"),
        ("01 08 1999", "1999-01-08"),
        ("01/02/03", "2003-01-02"),
        ("epoch", "1970-01-01"),
        ("infinity", "infinity"),
        ("-infinity", "-infinity"),
    ];
    for (input, expected) in cases {
        let parsed = parse_date(input).unwrap_or_else(|e| panic!("{input}: {e}"));
        assert!(date_to_text(parsed) == *expected, "input {input}");
    }
}

#[test]
fn date_order_decides_an_otherwise_ambiguous_numeric_date() {
    let cases: &[(&str, DateOrder, &str)] = &[
        ("99 01 08", DateOrder::Ymd, "1999-01-08"),
        ("08 01 99", DateOrder::Dmy, "1999-01-08"),
        ("01 08 99", DateOrder::Mdy, "1999-01-08"),
        ("18/1/1999", DateOrder::Dmy, "1999-01-18"),
        ("1/18/1999", DateOrder::Mdy, "1999-01-18"),
        ("01/02/03", DateOrder::Dmy, "2003-02-01"),
        ("01/02/03", DateOrder::Ymd, "2001-02-03"),
    ];
    for (input, order, expected) in cases {
        let parsed =
            parse_date_in(input, *order, &utc()).unwrap_or_else(|e| panic!("{input}: {e}"));
        assert!(
            date_to_text(parsed) == *expected,
            "input {input} order {order:?}"
        );
    }
    // `99 01 08` in MDY order puts 99 in the month slot, which is out of range.
    assert!(
        let Err(TypeError::DatetimeFieldOverflow { .. }) =
            parse_date_in("99 01 08", DateOrder::Mdy, &utc())
    );
}

#[test]
fn time_input_discards_a_leading_date_and_a_trailing_zone() {
    let cases: &[(&str, &str)] = &[
        ("00:00", "00:00:00"),
        ("02:03 PST", "02:03:00"),
        ("11:59 EDT", "11:59:00"),
        ("11:59:59.99 PM", "23:59:59.99"),
        ("2003-03-07 15:36:39 America/New_York", "15:36:39"),
        ("23:59:59.999999", "23:59:59.999999"),
        ("12:34.5", "00:12:34.5"),
        ("allballs", "00:00:00"),
    ];
    for (input, expected) in cases {
        let parsed = parse_time(input).unwrap_or_else(|e| panic!("{input}: {e}"));
        assert!(time_to_text(parsed) == *expected, "input {input}");
    }
    // A zone name is only resolvable with a date, so this is malformed text.
    assert!(
        let Err(TypeError::InvalidDatetimeFormat { .. }) = parse_time("15:36:39 America/New_York")
    );
}

#[test]
fn out_of_range_clock_fields_are_22008_not_22007() {
    for input in ["25:00:00", "24:01:00", "24:00:00.01", "23:59:60.01"] {
        let err = parse_time(input).expect_err(input);
        assert!(err.sqlstate() == "22008", "input {input} gave {err}");
    }
    // A malformed literal keeps 22007.
    let err = parse_time("garbage").expect_err("garbage");
    assert!(err.sqlstate() == "22007");
}

#[test]
fn literal_errors_carry_postgres_sqlstates() {
    let cases: &[(&str, &str)] = &[
        ("garbage", "22007"),
        ("2023-02-29", "22008"),
        ("Feb 29 17:32:01 1997", "22008"),
        ("Feb 16 17:32:01 -0097", "22009"),
        ("19970710 173201 America/Does_not_exist", "22023"),
    ];
    for (input, sqlstate) in cases {
        let err = parse_timestamp(input).expect_err(input);
        assert!(err.sqlstate() == *sqlstate, "input {input} gave {err}");
    }
}

#[test]
fn interval_input_spellings_match_postgres() {
    let cases: &[(&str, &str)] = &[
        ("-3 days 4 hours 5 min 6 sec", "-3 days +04:05:06"),
        ("3 days -4 hours", "3 days -04:00:00"),
        (
            "-1 year -2 mons +3 days 4:05:06",
            "-1 years -2 mons +3 days 04:05:06",
        ),
        ("@ 1 year 2 mons", "1 year 2 mons"),
        ("@ 1 year 2 mons ago", "-1 years -2 mons"),
        ("P1Y2M3DT4H5M6S", "1 year 2 mons 3 days 04:05:06"),
        ("P0001-02-03T04:05:06", "1 year 2 mons 3 days 04:05:06"),
        ("1-2", "1 year 2 mons"),
        ("1 2:03:04", "1 day 02:03:04"),
        ("1.5 weeks", "10 days 12:00:00"),
        ("10 decades", "100 years"),
        ("1 millennium 2 centuries", "1200 years"),
        (
            "1 year 2 mons 3 days 04:05:06.699999",
            "1 year 2 mons 3 days 04:05:06.699999",
        ),
        ("infinity", "infinity"),
        ("-infinity", "-infinity"),
        ("0", "00:00:00"),
    ];
    for (input, expected) in cases {
        let parsed = parse_interval(input).unwrap_or_else(|e| panic!("{input}: {e}"));
        assert!(interval_to_text(parsed) == *expected, "input {input}");
    }
    assert!(let Err(TypeError::InvalidDatetimeFormat { .. }) = parse_interval("garbage"));
}

// ---------------------------------------------------------------------------
// Oracle rows, rendered the way a client sees them: either the output text or
// the SQLSTATE and message. Writing the whole answer as one string keeps each
// table row an exact transcript of what `PostgreSQL` 18.4 replied, so a row is
// checkable against the oracle by eye and a wrong SQLSTATE cannot hide behind a
// right message.
// ---------------------------------------------------------------------------

/// Render a result the way the oracle rows below spell it.
fn outcome<T>(result: Result<T, TypeError>, render: impl FnOnce(T) -> String) -> String {
    match result {
        Ok(value) => format!("OK|{}", render(value)),
        Err(error) => format!("ERR|{}|{error}", error.sqlstate()),
    }
}

fn interval_outcome(input: &str) -> String {
    outcome(parse_interval(input), interval_to_text)
}

fn timestamp_outcome(input: &str) -> String {
    outcome(parse_timestamp(input), timestamp_to_text)
}

fn check(rows: &[(&str, &str)], probe: impl Fn(&str) -> String) {
    for (input, expected) in rows {
        assert!(probe(input) == *expected, "input {input}");
    }
}

#[test]
fn interval_field_overflow_is_22015_and_the_assembly_overflow_is_22008() {
    check(
        &[
            // A field that leaves its accumulator mid-decode.
            (
                "-2147483649 years",
                r#"ERR|22015|interval field value out of range: "-2147483649 years""#,
            ),
            (
                "2147483648 months",
                r#"ERR|22015|interval field value out of range: "2147483648 months""#,
            ),
            (
                "9223372036854775808 microsecond",
                r#"ERR|22015|interval field value out of range: "9223372036854775808 microsecond""#,
            ),
            // `ago` cannot negate a field already at its minimum.
            (
                "-2147483648 months ago",
                r#"ERR|22015|interval field value out of range: "-2147483648 months ago""#,
            ),
            (
                "P2147483648",
                r#"ERR|22015|interval field value out of range: "P2147483648""#,
            ),
            (
                "PT2562047789",
                r#"ERR|22015|interval field value out of range: "PT2562047789""#,
            ),
            (
                "P0.1Y2147483647M",
                r#"ERR|22015|interval field value out of range: "P0.1Y2147483647M""#,
            ),
            (
                "2562047788.1:0:54.775807",
                r#"ERR|22015|interval field value out of range: "2562047788.1:0:54.775807""#,
            ),
            (
                "0.1 2562047788:0:54.775807",
                r#"ERR|22015|interval field value out of range: "0.1 2562047788:0:54.775807""#,
            ),
            // One step under each of those is a perfectly ordinary interval.
            ("2562047788:0:54.775807", "OK|2562047788:00:54.775807"),
            // The years field holds these; only folding years into months fails,
            // and that is a different error with a different SQLSTATE.
            ("-2147483648 years", "ERR|22008|interval out of range"),
            ("2147483647 years", "ERR|22008|interval out of range"),
            (
                "P1-2147483647-2147483647",
                "ERR|22008|interval out of range",
            ),
            // Malformed text keeps 22007 and never becomes an overflow.
            (
                "garbage",
                r#"ERR|22007|invalid input syntax for type interval: "garbage""#,
            ),
        ],
        interval_outcome,
    );
}

#[test]
fn interval_field_splitting_follows_postgres_rather_than_whitespace() {
    check(
        &[
            // A unit word ends the number that precedes it, with no gap needed.
            ("1mon", "OK|1 mon"),
            (
                "4 millenniums 5 centuries 4 decades 1 year 4 months 4 days 17 minutes 31 seconds",
                "OK|4541 years 4 mons 4 days 00:17:31",
            ),
            // A lone sign binds to the number after it.
            ("1 month - 1 second", "OK|1 mon -00:00:01"),
            ("2 days - 12:34:56", "OK|2 days -12:34:56"),
            // A fraction on the second field of a two-field clock reading shifts
            // the whole reading down to minutes and seconds.
            ("12:34.5678", "OK|00:12:34.5678"),
            ("1 2:03.4567", "OK|1 day 00:02:03.4567"),
            // The reserved non-finite encodings are reachable as literals.
            (
                "-2147483648 months -2147483648 days -9223372036854775808 us",
                "OK|-infinity",
            ),
            (
                "2147483647 months 2147483647 days 9223372036854775807 us",
                "OK|infinity",
            ),
        ],
        interval_outcome,
    );
}

#[test]
fn iso8601_interval_forms_match_postgres() {
    check(
        &[
            ("P0002", "OK|2 years"),
            ("P0002-10", "OK|2 years 10 mons"),
            ("P0002-10-15T1S", "OK|2 years 10 mons 15 days 00:00:01"),
            ("P00021015T103020", "OK|2 years 10 mons 15 days 10:30:20"),
            ("PT10", "OK|10:00:00"),
            ("PT10:30", "OK|10:30:00"),
            // Exponent notation is `strtod`'s, and PostgreSQL keeps it here.
            ("P10.5e4Y", "OK|105000 years"),
            (
                "P-1Y-2M-3DT-4H-5M-6.7S",
                "OK|-1 years -2 mons -3 days -04:05:06.7",
            ),
        ],
        interval_outcome,
    );
}

#[test]
fn a_leading_minus_lexes_as_a_zone_so_a_negative_year_reports_displacement() {
    check(
        &[
            (
                "-44-02-01",
                r#"ERR|22009|time zone displacement out of range: "-44-02-01""#,
            ),
            (
                "-44-02-01 11:12:13 BC",
                r#"ERR|22009|time zone displacement out of range: "-44-02-01 11:12:13 BC""#,
            ),
            (
                "-2147483648-1-1",
                r#"ERR|22009|time zone displacement out of range: "-2147483648-1-1""#,
            ),
            // The same grammar reading ordinary offsets, on both sides of the
            // ±15:59:59 limit.
            ("1997-02-10 17:32:01 -08", "OK|1997-02-10 17:32:01"),
            ("1997-02-10 17:32:01 -0800", "OK|1997-02-10 17:32:01"),
            ("1997-02-10 17:32:01 -800", "OK|1997-02-10 17:32:01"),
            ("1997-02-10 17:32:01 -15:59:59", "OK|1997-02-10 17:32:01"),
            (
                "1997-02-10 17:32:01 -16",
                r#"ERR|22009|time zone displacement out of range: "1997-02-10 17:32:01 -16""#,
            ),
            // There is no run-together `hhmmss` offset: this reads as 8000 hours.
            (
                "1997-02-10 17:32:01 -080000",
                r#"ERR|22009|time zone displacement out of range: "1997-02-10 17:32:01 -080000""#,
            ),
        ],
        timestamp_outcome,
    );
}

#[test]
fn a_punctuated_date_field_must_carry_a_whole_date_and_stand_alone() {
    check(
        &[
            // A year and a month with no day: malformed, not out of range.
            (
                "040506.07",
                r#"ERR|22007|invalid input syntax for type timestamp: "040506.07""#,
            ),
            (
                "T040506.789+08",
                r#"ERR|22007|invalid input syntax for type timestamp: "T040506.789+08""#,
            ),
            // The date half lexes as `1999-08-`, and a trailing separator with
            // nothing behind it is malformed.
            (
                "1999-08-Jan",
                r#"ERR|22007|invalid input syntax for type timestamp: "1999-08-Jan""#,
            ),
            // A weekday BEFORE a punctuated date is malformed; after it is fine.
            (
                "Fri 1-January-1999",
                r#"ERR|22007|invalid input syntax for type timestamp: "Fri 1-January-1999""#,
            ),
            ("1-January-1999 Fri", "OK|1999-01-01 00:00:00"),
            // Five digits is a year, so the `24` lands in the month slot and it
            // is the month that is out of range — the day never gets a say.
            (
                "19971)24",
                r#"ERR|22008|date/time field value out of range: "19971)24""#,
            ),
        ],
        timestamp_outcome,
    );
}

#[test]
fn julian_days_take_an_attached_zone_and_a_fractional_day() {
    check(
        &[
            ("J2452271", "OK|2001-12-27 00:00:00"),
            ("J2452271-08", "OK|2001-12-27 00:00:00"),
            ("J2452271.5+08", "OK|2001-12-27 12:00:00"),
            // `DST` on its own modifies the zone already named.
            (
                "2001-12-27 04:05:06.789 MET DST",
                "OK|2001-12-27 04:05:06.789",
            ),
        ],
        timestamp_outcome,
    );
}

#[test]
fn timestamptz_range_is_checked_on_the_instant_not_the_local_reading() {
    // The first representable instant, written at a wall clock that falls on the
    // day before it. Checking the local reading would reject this.
    let first = parse_timestamptz("4714-11-23 16:00:00-08 BC", &utc()).expect("first instant");
    assert!(timestamptz_to_text(first, &utc()) == "4714-11-24 00:00:00+00 BC");
    let before = parse_timestamptz("4714-11-23 15:59:59-08 BC", &utc()).expect_err("one earlier");
    assert!(before.sqlstate() == "22008", "got {before}");
    assert!(
        format!("{before}") == r#"timestamp out of range: "4714-11-23 15:59:59-08 BC""#,
        "got {before}"
    );
}
