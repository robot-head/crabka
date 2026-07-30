//! Date/time literal decoding, checked against `PostgreSQL` 18.4's accepted set.
//!
//! Every expectation here was taken from a live `PostgreSQL` 18.4 oracle with
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
