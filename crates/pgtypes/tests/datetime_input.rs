//! Date/time literal decoding, checked against `PostgreSQL` 18.4's accepted set.
//!
//! Every expectation here comes from a live `PostgreSQL` 18.4 oracle with
//! `DateStyle = 'ISO, MDY'` and `TimeZone = 'Etc/UTC'`.

use assert2::assert;
use crabka_pgtypes::{
    TypeError,
    datetime::{
        DateOrder, ParsedDateTime, date_to_text, interval_to_text, parse_by_template, parse_date,
        parse_date_in, parse_interval, parse_time, parse_timestamp, parse_timestamptz,
        parse_timetz, time_to_text, timestamp_to_text, timestamptz_to_text, timetz_to_text,
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
    // A month or a day out of range is the one field overflow PostgreSQL points
    // at `DateStyle`, because the ordering is what put the value there.
    let refused = parse_date_in("99 01 08", DateOrder::Mdy, &utc()).expect_err("month 99");
    assert!(refused.sqlstate() == "22008");
    assert!(refused.to_string() == "date/time field value out of range: \"99 01 08\"");
    assert!(refused.hint() == Some("Perhaps you need a different \"DateStyle\" setting."));

    // `1997-02-29` names a real month and a day inside 1..=31, so the ordering
    // cannot be what is wrong with it, and PostgreSQL leaves off the HINT.
    let refused = parse_date_in("1997-02-29", DateOrder::Ymd, &utc()).expect_err("Feb 29 of 1997");
    assert!(refused.hint().is_none());
    assert!(let TypeError::DatetimeFieldOverflow { .. } = refused);
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

// ---------------------------------------------------------------------------
// `DecodeTimeOnly`: the second decoder, behind `time` and `timetz`.
//
// PostgreSQL does not read a time-only literal with the decoder it reads a
// timestamp with. The difference shows on every run-together number, which is a
// date to one decoder and a clock reading to the other.
// ---------------------------------------------------------------------------

fn time_outcome(input: &str) -> String {
    outcome(parse_time(input), time_to_text)
}

fn timetz_outcome(input: &str) -> String {
    outcome(parse_timetz(input, &utc()), timetz_to_text)
}

#[test]
fn a_run_together_number_is_a_clock_reading_for_a_time_literal() {
    check(
        &[
            ("040506", "OK|04:05:06"),
            ("0405", "OK|04:05:00"),
            ("T040506", "OK|04:05:06"),
            ("T0405", "OK|04:05:00"),
            ("040506.07", "OK|04:05:06.07"),
            ("T040506.07", "OK|04:05:06.07"),
            // A trailing zone is read and then discarded by this type, but a
            // negative one arrives glued to the reading because `-` also
            // delimits a date.
            ("040506.789+08", "OK|04:05:06.789"),
            ("040506.789-08", "OK|04:05:06.789"),
            ("T040506.789 -08", "OK|04:05:06.789"),
            ("15:36:39 UTC", "OK|15:36:39"),
        ],
        time_outcome,
    );
}

#[test]
fn a_run_together_number_keeps_its_zone_for_a_timetz_literal() {
    check(
        &[
            ("040506+08", "OK|04:05:06+08"),
            ("0405+08", "OK|04:05:00+08"),
            ("T040506.07+08", "OK|04:05:06.07+08"),
            ("040506.789-08", "OK|04:05:06.789-08"),
            ("T040506.789 -08", "OK|04:05:06.789-08"),
            ("15:36:39 UTC", "OK|15:36:39+00"),
        ],
        timetz_outcome,
    );
}

#[test]
fn a_time_literal_needs_a_whole_clock_reading_and_a_resolvable_zone() {
    check(
        &[
            // A date with no time behind it has nothing to yield.
            (
                "2003-03-07",
                r#"ERR|22007|invalid input syntax for type time: "2003-03-07""#,
            ),
            // A zone whose offset moves needs a date to resolve against.
            (
                "15:36:39 America/New_York",
                r#"ERR|22007|invalid input syntax for type time: "15:36:39 America/New_York""#,
            ),
            // A bare zone supplies no clock reading at all.
            (
                "zulu",
                r#"ERR|22007|invalid input syntax for type time: "zulu""#,
            ),
            // The date spellings have no clock reading to contribute either.
            (
                "today",
                r#"ERR|22007|invalid input syntax for type time: "today""#,
            ),
            // With a date in front, the same named zone resolves.
            ("2003-03-07 15:36:39 America/New_York", "OK|15:36:39"),
        ],
        time_outcome,
    );
}

#[test]
fn a_unit_keyword_stops_a_zone_name_a_bare_letter_run_does_not() {
    // `m` is a keyword of PostgreSQL's own, so the digits after it start a new
    // field and the literal has two fields that mean nothing; `x` is not, so the
    // same shape is one field and a legal POSIX zone eight hours west.
    check(
        &[
            (
                "15:36:39 m2",
                r#"ERR|22007|invalid input syntax for type time with time zone: "15:36:39 m2""#,
            ),
            (
                "15:36:39 MSK m2",
                r#"ERR|22007|invalid input syntax for type time with time zone: "15:36:39 MSK m2""#,
            ),
            ("15:36:39 X8", "OK|15:36:39-08"),
            ("15:36:39 GMT+8", "OK|15:36:39-08"),
        ],
        timetz_outcome,
    );
}

// ---------------------------------------------------------------------------
// Reserved spellings.
//
// PostgreSQL splits these in two. `epoch`, `infinity` and `-infinity` name a
// whole value and may share the literal with nothing else; `now`, `today`,
// `tomorrow` and `yesterday` fill fields from the clock and compose with the
// rest of the text.
// ---------------------------------------------------------------------------

#[test]
fn clock_relative_spellings_compose_with_a_time_from_the_text() {
    let today = jiff::Zoned::now().with_time_zone(utc()).date();
    let cases: &[(&str, jiff::Span, &str)] = &[
        ("today 10:30", jiff::Span::new(), "10:30:00"),
        ("10:30 today", jiff::Span::new(), "10:30:00"),
        ("tomorrow 16:00:00", jiff::Span::new().days(1), "16:00:00"),
        ("yesterday 12:34:56", jiff::Span::new().days(-1), "12:34:56"),
        ("tomorrow", jiff::Span::new().days(1), "00:00:00"),
    ];
    for (input, shift, clock) in cases {
        let day = today.checked_add(*shift).expect("in range");
        let expected = format!("OK|{day} {clock}");
        assert!(timestamp_outcome(input) == expected, "input {input}");
    }
}

#[test]
fn a_whole_value_spelling_may_not_share_the_literal() {
    check(
        &[
            (
                "1995-08-06 epoch",
                r#"ERR|22007|invalid input syntax for type timestamp: "1995-08-06 epoch""#,
            ),
            (
                "epoch 01:01:01",
                r#"ERR|22007|invalid input syntax for type timestamp: "epoch 01:01:01""#,
            ),
            (
                "1995-08-06 infinity",
                r#"ERR|22007|invalid input syntax for type timestamp: "1995-08-06 infinity""#,
            ),
            (
                "infinity 01:01:01",
                r#"ERR|22007|invalid input syntax for type timestamp: "infinity 01:01:01""#,
            ),
            (
                "-infinity 01:01:01",
                r#"ERR|22007|invalid input syntax for type timestamp: "-infinity 01:01:01""#,
            ),
            (
                "-infinity infinity",
                r#"ERR|22007|invalid input syntax for type timestamp: "-infinity infinity""#,
            ),
            (
                "now epoch",
                r#"ERR|22007|invalid input syntax for type timestamp: "now epoch""#,
            ),
            (
                "today infinity",
                r#"ERR|22007|invalid input syntax for type timestamp: "today infinity""#,
            ),
            // A second Julian label has nothing left to label.
            (
                "J J 1520447",
                r#"ERR|22007|invalid input syntax for type timestamp: "J J 1520447""#,
            ),
        ],
        timestamp_outcome,
    );
}

#[test]
fn a_negative_run_together_zone_is_split_off_the_clock_reading() {
    // `+` is not a date delimiter, so `040506+08` reaches the decoder already
    // split; `-` is, so `040506-08` arrives as one field and has to be taken
    // apart. Both spellings must land on the same instant.
    let cases: &[(&str, &str)] = &[
        ("20011227 040506+08", "2001-12-26 20:05:06+00"),
        ("20011227 040506-08", "2001-12-27 12:05:06+00"),
        ("20011227T040506-08", "2001-12-27 12:05:06+00"),
        ("J2452271T040506-08", "2001-12-27 12:05:06+00"),
        ("20011227 040506.789-08", "2001-12-27 12:05:06.789+00"),
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
fn a_boundary_reading_resolves_to_the_later_instant() {
    // Moscow left UTC+4 for UTC+3 at 02:00 on 2014-10-26, so 01:00 that day
    // happened twice. PostgreSQL reads it at the offset in force AFTER the
    // transition, which is the later of the two instants; reading it at the
    // earlier one is an hour out.
    let moscow = crabka_pgtypes::datetime::zone_by_name("Europe/Moscow").expect("zone");
    let cases: &[(&str, &str)] = &[
        ("2014-10-26 01:00:00", "2014-10-25 22:00:00+00"),
        ("2014-10-26 01:00:01", "2014-10-25 22:00:01+00"),
        // Outside the fold the reading is unambiguous either way.
        ("2014-10-26 03:00:00", "2014-10-26 00:00:00+00"),
        ("2014-10-25 23:00:00", "2014-10-25 19:00:00+00"),
        // A spring-forward gap resolves to the later instant too, which is the
        // pre-transition offset.
        ("2011-03-27 02:00:00", "2011-03-26 23:00:00+00"),
    ];
    for (input, expected) in cases {
        let parsed = parse_timestamptz(input, &moscow).unwrap_or_else(|e| panic!("{input}: {e}"));
        assert!(
            timestamptz_to_text(parsed, &utc()) == *expected,
            "input {input}"
        );
    }
}

// ---------------------------------------------------------------------------
// SP38: `to_timestamp`/`to_date` template parsing.
//
// Every expectation below was read off the same PostgreSQL 18.4 oracle, through
// `to_char(to_timestamp(input, template) AT TIME ZONE 'UTC',
// 'YYYY-MM-DD HH24:MI:SS.US BC')` for the field cases and `EXTRACT(epoch …)` for
// the zone cases, so the years are astronomical (1 BC is year 0) exactly as
// `ParsedDateTime` reports them.
// ---------------------------------------------------------------------------

/// The reading a template parse should produce, spelled out in full so a case
/// compares as one value rather than as a chain of field assertions.
fn reading(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    micros: u32,
) -> ParsedDateTime {
    ParsedDateTime {
        year,
        month,
        day,
        hour,
        minute,
        second,
        micros,
        tz_offset_secs: None,
        fractional_precision: None,
    }
}

#[test]
fn template_parse_fields_match_postgres() {
    let cases: &[(&str, &str, ParsedDateTime)] = &[
        (
            "YYYY/Mon/DD --> HH:MI:SS",
            "0097/Feb/16 --> 08:14:30",
            reading(97, 2, 16, 8, 14, 30, 0),
        ),
        (
            "FMYYYY/FMMM/FMDD FMHH:FMMI:FMSS",
            "97/2/16 8:14:30",
            reading(97, 2, 16, 8, 14, 30, 0),
        ),
        (
            "YYYY-MM-DD HH24:MI:SS",
            "2011$03!18 23_38_15",
            reading(2011, 3, 18, 23, 38, 15, 0),
        ),
        (
            "YYYY FMMonth DD",
            "1985 January 12",
            reading(1985, 1, 12, 0, 0, 0, 0),
        ),
        (
            "Y,YYYth FMRM DD",
            "1,582nd VIII 21",
            reading(1582, 8, 21, 0, 0, 0, 0),
        ),
        (
            "MMDDHH24MISSYYYY",
            "05121445482000",
            reading(2000, 5, 12, 14, 45, 48, 0),
        ),
        (
            "YYYYFMMonthDDFMDay",
            "2000January09Sunday",
            reading(2000, 1, 9, 0, 0, 0, 0),
        ),
        ("FXYY:Mon:DD", "97/Feb/16", reading(1997, 2, 16, 0, 0, 0, 0)),
        ("YYYYMMDD", "19971116", reading(1997, 11, 16, 0, 0, 0, 0)),
        (
            "YYYY BC MM DD",
            "1997 AD 11 16",
            reading(1997, 11, 16, 0, 0, 0, 0),
        ),
        (
            "YYYY BC MM DD",
            "1997 BC 11 16",
            reading(-1996, 11, 16, 0, 0, 0, 0),
        ),
        (
            "YYYY B.C. MM DD",
            "1997 B.C. 11 16",
            reading(-1996, 11, 16, 0, 0, 0, 0),
        ),
        ("Y-MMDD", "9-1116", reading(2009, 11, 16, 0, 0, 0, 0)),
        ("YY-MMDD", "95-1116", reading(1995, 11, 16, 0, 0, 0, 0)),
        ("YYY-MMDD", "995-1116", reading(1995, 11, 16, 0, 0, 0, 0)),
        ("YYYYWWD", "2005426", reading(2005, 10, 15, 0, 0, 0, 0)),
        ("YYYYDDD", "2005300", reading(2005, 10, 27, 0, 0, 0, 0)),
        ("IYYYIWID", "2005527", reading(2006, 1, 1, 0, 0, 0, 0)),
        ("IYYIWID", "005527", reading(2006, 1, 1, 0, 0, 0, 0)),
        ("IYIWID", "05527", reading(2006, 1, 1, 0, 0, 0, 0)),
        ("IYYYIDDD", "2005364", reading(2006, 1, 1, 0, 0, 0, 0)),
        ("YYYYMMDD", "  20050302", reading(2005, 3, 2, 0, 0, 0, 0)),
        (
            "YYYY-MM-DD HH12:MI PM",
            "2011-12-18 11:38 AM",
            reading(2011, 12, 18, 11, 38, 0, 0),
        ),
        (
            "YYYY-MM-DD HH12:MI P.M.",
            "2011-12-18 11:38 P.M.",
            reading(2011, 12, 18, 23, 38, 0, 0),
        ),
        (
            "YYYY-MM-DD HH24:MI:SS.MS",
            "2018-11-02 12:34:56.025",
            reading(2018, 11, 2, 12, 34, 56, 25000),
        ),
        ("Q MM YYYY", "1 4 1902", reading(1902, 4, 1, 0, 0, 0, 0)),
        ("W MM CC YY", "3 4 21 01", reading(2001, 4, 15, 0, 0, 0, 0)),
        ("J", "2458872", reading(2020, 1, 23, 0, 0, 0, 0)),
        (
            "YYYY-MM-DD BC",
            "44-02-01 BC",
            reading(-43, 2, 1, 0, 0, 0, 0),
        ),
        ("YYYY-MM-DD", "-44-02-01", reading(-43, 2, 1, 0, 0, 0, 0)),
        (
            "YYYY-MM-DD BC",
            "-44-02-01 BC",
            reading(44, 2, 1, 0, 0, 0, 0),
        ),
        (
            "YYYY   MON",
            "2000 + + JUN",
            reading(2000, 6, 1, 0, 0, 0, 0),
        ),
        (
            "YYYY-MM-DD SSSS",
            "2015-02-11 86000",
            reading(2015, 2, 11, 23, 53, 20, 0),
        ),
        ("YYYY DDD", "2016 366", reading(2016, 12, 31, 0, 0, 0, 0)),
        ("YYYY-MM-DD", "0000-02-01", reading(0, 2, 1, 0, 0, 0, 0)),
        ("CC YY", "21 99", reading(2099, 1, 1, 0, 0, 0, 0)),
        ("CC YY", "21 00", reading(2100, 1, 1, 0, 0, 0, 0)),
        ("CC YY", "-6 01", reading(-500, 1, 1, 0, 0, 0, 0)),
        ("CC", "20", reading(1901, 1, 1, 0, 0, 0, 0)),
        ("YYYY rm", "1902 viii", reading(1902, 8, 1, 0, 0, 0, 0)),
        ("MM-DD", "02-30", reading(0, 3, 1, 0, 0, 0, 0)),
        ("DD", "00", reading(0, 1, 1, 0, 0, 0, 0)),
        ("YYYY", "2011", reading(2011, 1, 1, 0, 0, 0, 0)),
    ];
    for &(template, input, expected) in cases {
        let parsed = parse_by_template(template, input)
            .unwrap_or_else(|e| panic!("{template:?} on {input:?}: {e}"));
        assert!(parsed == expected, "{template:?} on {input:?}");
    }
}

#[test]
fn template_parse_zone_offsets_match_postgres() {
    // The offset PostgreSQL applied, recovered as (civil reading) - (instant).
    // The two `'2000 -10'` rows are the pair that pins the sign-reclaim rule: a
    // single space node eats the minus and `TZH` takes it back, while a second
    // space node leaves nothing to reclaim and the offset comes out positive.
    let cases: &[(&str, &str, i32)] = &[
        ("YYYY-MM-DD HH12:MI TZH", "2011-12-18 11:38 +05", 18_000),
        ("YYYY-MM-DD HH12:MI TZH", "2011-12-18 11:38 -05", -18_000),
        (
            "YYYY-MM-DD HH12:MI TZH:TZM",
            "2011-12-18 11:38 +05:20",
            19_200,
        ),
        (
            "YYYY-MM-DD HH12:MI TZH:TZM",
            "2011-12-18 11:38 -05:20",
            -19_200,
        ),
        ("YYYY-MM-DD HH12:MI TZM", "2011-12-18 11:38 20", 1_200),
        ("YYYY-MM-DD HH12:MI TZ", "2011-12-18 11:38 EST", -18_000),
        ("YYYY-MM-DD HH12:MI OF", "2011-12-18 11:38 +01:30", 5_400),
        ("YYYY-MM-DD HH12:MI OF", "2011-12-18 11:38 -05", -18_000),
        // `MSK` is a dynamic abbreviation: +4 in December 2011, not the +3 it
        // means today, so it can only be resolved once the date is known.
        ("YYYY-MM-DD HH12:MI TZ", "2011-12-18 11:38 MSK", 14_400),
        ("YYYY TZH", "2000 -10", -36_000),
        ("YYYY  TZH", "2000 -10", 36_000),
    ];
    for &(template, input, offset) in cases {
        let parsed = parse_by_template(template, input)
            .unwrap_or_else(|e| panic!("{template:?} on {input:?}: {e}"));
        assert!(
            parsed.tz_offset_secs == Some(offset),
            "{template:?} on {input:?}"
        );
    }
}

#[test]
fn template_parse_rejections_match_postgres() {
    let cases: &[(&str, &str, &str, &str)] = &[
        (
            "YYYYIWID",
            "2005527",
            "22007",
            "invalid combination of date conventions",
        ),
        (
            "YYYYMMDD",
            "19971",
            "22007",
            "source string too short for \"MM\" formatting field",
        ),
        (
            "YYYYMMDD",
            "19971)24",
            "22007",
            "invalid value \"1)\" for \"MM\"",
        ),
        (
            "DY DD MON YYYY",
            "Friday 1-January-1999",
            "22007",
            "invalid value \"da\" for \"DD\"",
        ),
        (
            "DY DD MON YYYY",
            "Fri 1-January-1999",
            "22007",
            "invalid value \"uary\" for \"YYYY\"",
        ),
        (
            "YYYY-MM-Mon-DD",
            "1997-11-Jan-16",
            "22007",
            "conflicting values for \"Mon\" field in formatting string",
        ),
        (
            "YYYYMMDD",
            "199711xy",
            "22007",
            "invalid value \"xy\" for \"DD\"",
        ),
        (
            "FMYYYY",
            "10000000000",
            "22008",
            "value for \"YYYY\" in source string is out of range",
        ),
        (
            "YYYY-MM-DD HH24:MI:SS",
            "2016-06-13 25:00:00",
            "22008",
            "date/time field value out of range: \"2016-06-13 25:00:00\"",
        ),
        (
            "YYYY-MM-DD HH24:MI:SS",
            "2016-06-13 15:60:00",
            "22008",
            "date/time field value out of range: \"2016-06-13 15:60:00\"",
        ),
        (
            "YYYY-MM-DD HH:MI:SS",
            "2016-06-13 15:50:55",
            "22007",
            "hour \"15\" is invalid for the 12-hour clock",
        ),
        (
            "YYYY-MM-DD HH24:MI:SS",
            "2016-13-01 15:50:55",
            "22008",
            "date/time field value out of range: \"2016-13-01 15:50:55\"",
        ),
        (
            "YYYY-MM-DD HH24:MI:SS",
            "2016-02-30 15:50:55",
            "22008",
            "date/time field value out of range: \"2016-02-30 15:50:55\"",
        ),
        (
            "YYYY-MM-DD HH24:MI:SS",
            "2015-02-29 15:50:55",
            "22008",
            "date/time field value out of range: \"2015-02-29 15:50:55\"",
        ),
        (
            "YYYY-MM-DD SSSS",
            "2015-02-11 86400",
            "22008",
            "date/time field value out of range: \"2015-02-11 86400\"",
        ),
        (
            "Y,YYY",
            "1000000000,999",
            "22008",
            "value for \"Y,YYY\" in source string is out of range",
        ),
        (
            "SS.MS",
            "0.-2147483648",
            "22008",
            "date/time field value out of range: \"0.-2147483648\"",
        ),
        (
            "W",
            "613566758",
            "22008",
            "date/time field value out of range: \"613566758\"",
        ),
        (
            "YYYY WW D",
            "2024 613566758 1",
            "22008",
            "date/time field value out of range: \"2024 613566758 1\"",
        ),
        (
            "YYYY DDD",
            "2016 367",
            "22008",
            "date/time field value out of range: \"2016 367\"",
        ),
        (
            "CC",
            "100000000",
            "22008",
            "date/time field value out of range: \"100000000\"",
        ),
        (
            "CC YY",
            "-2147483648 01",
            "22008",
            "date/time field value out of range: \"-2147483648 01\"",
        ),
        (
            "YYYY-MM-DD HH12:MI TZ",
            "2011-12-18 11:38 JUNK",
            "22007",
            "invalid value \"JUNK\" for \"TZ\"",
        ),
        (
            "YYYY-MM-DD HH12:MI OF",
            "2011-12-18 11:38 +xyz",
            "22007",
            "invalid value \"xy\" for \"OF\"",
        ),
        (
            "YYYY-MM-DD HH12:MI OF",
            "2011-12-18 11:38 +16:00",
            "22009",
            "time zone displacement out of range: \"2011-12-18 11:38 +16:00\"",
        ),
        (
            "YYYY YYYY",
            "2011 2012",
            "22007",
            "conflicting values for \"YYYY\" field in formatting string",
        ),
        (
            "MM MM",
            "05 06",
            "22007",
            "conflicting values for \"MM\" field in formatting string",
        ),
        (
            "YYYY MM RM",
            "1902 09 VIII",
            "22007",
            "conflicting values for \"RM\" field in formatting string",
        ),
        (
            "HH12 AM",
            "00 AM",
            "22007",
            "hour \"0\" is invalid for the 12-hour clock",
        ),
        (
            "IYYY MM",
            "2005 527",
            "22007",
            "invalid combination of date conventions",
        ),
        (
            "YYMonDD",
            "97/Feb/16",
            "22007",
            "invalid value \"/Feb/16\" for \"Mon\"",
        ),
        (
            "YYYYxMMxDD",
            "2011 x12 x18",
            "22007",
            "invalid value \"x1\" for \"MM\"",
        ),
    ];
    for &(template, input, sqlstate, message) in cases {
        let err = parse_by_template(template, input)
            .expect_err(&format!("{template:?} on {input:?} should be rejected"));
        assert!(err.sqlstate() == sqlstate, "{template:?} on {input:?}");
        assert!(err.to_string() == message, "{template:?} on {input:?}");
    }
}

#[test]
fn template_parse_accepts_a_second_value_only_when_the_first_was_zero() {
    // `from_char_set_int` treats a destination still holding zero as unset, so a
    // meridiem that stored 0 (`AM`) can be overwritten by one that stores 1
    // (`PM`) without complaint, while two non-zero years collide. Both halves
    // are PostgreSQL's, verified against the oracle.
    let both = parse_by_template("HH12 AM AM", "11 AM AM").expect("AM twice is accepted");
    assert!(both.hour == 11);
    let flipped = parse_by_template("HH12 AM PM", "11 AM PM").expect("AM then PM is accepted");
    assert!(flipped.hour == 23);
    let clash = parse_by_template("YYYY YYYY", "2011 2012").expect_err("two years collide");
    assert!(clash.to_string() == "conflicting values for \"YYYY\" field in formatting string");
}

#[test]
fn template_parse_rounds_to_the_fractional_precision_requested() {
    // `FF`n does not cap what is read — every digit is parsed — it records a
    // precision the caller rounds the finished instant to.
    for (n, expected) in [(1u8, 1u8), (3, 3), (6, 6)] {
        let parsed = parse_by_template(
            &format!("YYYY-MM-DD HH24:MI:SS.FF{n}"),
            "2018-11-02 12:34:56.123456",
        )
        .expect("FF parses");
        assert!(parsed.micros == 123_456, "FF{n}");
        assert!(parsed.fractional_precision == Some(expected), "FF{n}");
    }
    // `US` and `MS` set no precision, so nothing is rounded afterwards.
    let us = parse_by_template("HH24:MI:SS.US", "01:02:03.123456").expect("US");
    assert!(us.fractional_precision == None);
    let ms = parse_by_template("HH24:MI:SS.MS", "01:02:03.25").expect("MS");
    assert!(ms.micros == 250_000 && ms.fractional_precision == None);
}
