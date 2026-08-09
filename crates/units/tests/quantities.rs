//! Behaviour of the quantity vocabulary: construction, wire conversion, parsing,
//! rendering, and `serde`.

use assert2::{assert, check};
use crabka_units::{
    convert::wire, fmt::Human as _, parse, parse::ParseError, prelude::*, serde_units,
};
use proptest::prelude::*;
use serde::{Deserialize, Serialize};

/// Every constructor lands on the same byte count the unit implies.
#[test]
fn size_constructors_agree_on_bytes() {
    let cases = [
        (bytes(1), 1_u64),
        (kibibytes(1), 1024),
        (mebibytes(1), 1_048_576),
        (gibibytes(1), 1_073_741_824),
        (mebibytes(512), 536_870_912),
    ];
    for (size, expected) in cases {
        check!(size.bytes_u64() == expected);
    }
}

/// Every constructor lands on the same millisecond extent the unit implies.
#[test]
fn time_constructors_agree_on_millis() {
    let cases = [
        (nanos(1_000_000), 1_i64),
        (micros(1_000), 1),
        (millis(1), 1),
        (secs(1), 1_000),
        (minutes(1), 60_000),
        (hours(1), 3_600_000),
        (days(7), 604_800_000),
    ];
    for (extent, expected) in cases {
        check!(extent.millis_i64() == expected);
    }
}

/// Equal magnitudes written in different units are the same value.
#[test]
fn units_of_the_same_magnitude_compare_equal() {
    check!(kibibytes(1024) == mebibytes(1));
    check!(millis(1_000) == secs(1));
    check!(days(1) == hours(24));
    check!(percent(25) == fraction(0.25));
    check!(mebibytes_per_sec(1) == bytes_per_sec(1_048_576));
    check!(kibibytes(1) < mebibytes(1));
}

/// A size divided by a time gives the rate, and the rate multiplied back gives
/// the size. This is the dimensional analysis this crate exists for.
#[test]
fn size_over_time_is_a_rate() {
    let rate = byte_rate(mebibytes(8), secs(2));
    check!(rate == mebibytes_per_sec(4));

    let transferred: ByteSize = (rate * secs(4)).into();
    check!(transferred == mebibytes(16));

    check!(rate.time_to_transfer(mebibytes(8)) == secs(2));
    check!(per_sec(100).period() == millis(10));
}

/// An unset rate of zero transfers instantly and does not give an infinity.
#[test]
fn zero_rate_transfers_instantly() {
    check!(ByteRate::ZERO.time_to_transfer(gibibytes(1)) == Time::ZERO);
    check!(Frequency::ZERO.period() == Time::ZERO);
}

/// Scaling a size by a fraction keeps the size dimension.
#[test]
fn scaling_a_size_by_a_fraction_keeps_bytes() {
    let scaled: ByteSize = (mebibytes(8) * percent(25)).into();
    check!(scaled == mebibytes(2));
}

/// The wire seam round-trips Kafka's integer fields exactly.
#[test]
fn wire_conversions_round_trip() {
    check!(Time::from_millis(30_000).millis_i32() == 30_000);
    check!(Time::from_millis(604_800_000).millis_i64() == 604_800_000);
    check!(Time::from_nanos(1_234_567).nanos_i64() == 1_234_567);
    check!(Time::from_micros(999).micros_i64() == 999);
    check!(Time::from_secs(86_400).secs_i64() == 86_400);
    check!(ByteSize::from_bytes(1_099_511_627_776).bytes_u64() == 1_099_511_627_776);
    check!(ByteSize::from_bytes_i64(-4_096).bytes_i64() == -4_096);
    check!(ByteRate::from_bytes_per_sec(1_048_576).bytes_per_sec_i64() == 1_048_576);
}

/// Truncated extraction matches an external format that integer-divides, and
/// does not lose a millisecond to float error on an exact extent.
#[test]
fn truncating_millis_matches_integer_division() {
    check!(millis(42).millis_i64_trunc() == 42);
    check!(nanos(41_999_999).millis_i64_trunc() == 41);
    check!(nanos(42_000_001).millis_i64_trunc() == 42);
    check!(micros(1_500).millis_i64_trunc() == 1);
    check!(Time::ZERO.millis_i64_trunc() == 0);
    // Where rounding and truncation disagree, this is the half that floors.
    check!(micros(1_999).millis_i64_trunc() == 1);
    check!(micros(1_999).millis_i64() == 2);
}

/// A `Duration` survives the trip through a quantity, and a negative extent
/// clamps and does not panic.
#[test]
fn std_duration_round_trips() {
    let original = core::time::Duration::from_millis(1_500);
    check!(original.as_time().to_std() == original);
    check!(secs(30).to_std() == core::time::Duration::from_secs(30));
    check!((Time::ZERO - secs(5)).to_std() == core::time::Duration::ZERO);
}

/// Extraction saturates at the target type's bounds instead of wrapping, and maps
/// `NaN` to zero.
#[test]
fn extraction_saturates_rather_than_wrapping() {
    check!(days(365 * 1_000).millis_i32() == i32::MAX);
    check!((Time::ZERO - days(365 * 1_000)).millis_i32() == i32::MIN);
    check!(ByteSize::from_bytes_i64(-1).bytes_u64() == 0);
    check!(ByteSize::from_bytes_f64(f64::NAN).bytes_i64() == 0);
    check!(ByteSize::from_bytes_f64(f64::INFINITY).bytes_i32() == i32::MAX);
}

/// Kafka's `-1` sentinel becomes an absence and back.
#[test]
fn minus_one_is_an_absence() {
    check!(wire::opt_size_from_bytes_i32(-1) == None);
    check!(wire::opt_size_from_bytes_i32(0) == Some(ByteSize::ZERO));
    check!(wire::opt_size_to_bytes_i32(None) == -1);
    check!(wire::opt_size_to_bytes_i32(Some(kibibytes(4))) == 4_096);

    check!(wire::opt_size_from_bytes_i64(-1) == None);
    check!(wire::opt_size_to_bytes_i64(Some(mebibytes(1))) == 1_048_576);

    check!(wire::opt_time_from_millis_i64(-1) == None);
    check!(wire::opt_time_from_millis_i64(30_000) == Some(secs(30)));
    check!(wire::opt_time_to_millis_i64(None) == -1);

    check!(wire::opt_rate_from_bytes_per_sec_i64(-1) == None);
    check!(wire::opt_rate_to_bytes_per_sec_i64(Some(mebibytes_per_sec(1))) == 1_048_576);
}

/// The human forms an operator writes parse to the quantities they name.
#[test]
fn human_forms_parse() {
    assert!(let Ok(size) = parse::byte_size("512MiB"));
    check!(size == mebibytes(512));
    assert!(let Ok(size) = parse::byte_size("1.5 GiB"));
    check!(size.bytes_u64() == 1_610_612_736);
    assert!(let Ok(size) = parse::byte_size("2MB"));
    check!(size.bytes_u64() == 2_000_000);
    assert!(let Ok(size) = parse::byte_size("1024 bytes"));
    check!(size == ByteSize::from_bytes(1024));

    assert!(let Ok(extent) = parse::time("30s"));
    check!(extent == secs(30));
    assert!(let Ok(extent) = parse::time("7d"));
    check!(extent == days(7));
    assert!(let Ok(extent) = parse::time("250 US"));
    check!(extent == micros(250));
    assert!(let Ok(extent) = parse::time("1.5h"));
    check!(extent == minutes(90));

    assert!(let Ok(rate) = parse::byte_rate("10MiB/s"));
    check!(rate == mebibytes_per_sec(10));
    assert!(let Ok(rate) = parse::byte_rate("64KiBps"));
    check!(rate == kibibytes_per_sec(64));
    assert!(let Ok(rate) = parse::byte_rate("3600MiB/h"));
    check!(rate == mebibytes_per_sec(1));

    assert!(let Ok(rate) = parse::frequency("100/s"));
    check!(rate == per_sec(100));
    assert!(let Ok(rate) = parse::frequency("2.5Hz"));
    check!(rate.per_sec_f64() > 2.4);
    assert!(let Ok(rate) = parse::frequency("60/min"));
    check!(rate == per_sec(1));

    assert!(let Ok(part) = parse::ratio("25%"));
    check!(part == percent(25));
    assert!(let Ok(part) = parse::ratio("0.25"));
    check!(part == percent(25));
}

/// Zero needs no unit, because it means the same in all of them.
#[test]
fn zero_parses_without_a_unit() {
    assert!(let Ok(size) = parse::byte_size("0"));
    check!(size == ByteSize::ZERO);
    assert!(let Ok(extent) = parse::time("0"));
    check!(extent == Time::ZERO);
    assert!(let Ok(rate) = parse::byte_rate("0"));
    check!(rate == ByteRate::ZERO);
}

#[test]
fn config_quantity_parsers_validate_sign_and_dimension() {
    check!(parse::positive_time("500ms") == Ok(millis(500)));
    check!(parse::non_negative_time("0") == Ok(Time::ZERO));
    check!(parse::positive_byte_size("4MiB") == Ok(mebibytes(4)));
    check!(parse::non_negative_byte_size("0") == Ok(ByteSize::ZERO));
    check!(parse::positive_byte_rate("10MiB/s") == Ok(mebibytes_per_sec(10)));
    check!(parse::non_negative_byte_rate("0") == Ok(ByteRate::ZERO));
    check!(parse::positive_ratio("25%") == Ok(percent(25)));
    check!(parse::non_negative_ratio("0") == Ok(fraction(0.0)));
    check!(parse::unit_ratio("100%") == Ok(Ratio::ONE));

    for input in ["0s", "-1s", "NaNs", "infs"] {
        check!(parse::positive_time(input).is_err());
    }
    for input in ["-1ms", "NaNs", "infs"] {
        check!(parse::non_negative_time(input).is_err());
    }
    check!(parse::positive_byte_size("0B").is_err());
    check!(parse::non_negative_byte_size("-1B").is_err());
    check!(parse::positive_byte_rate("0B/s").is_err());
    check!(parse::non_negative_byte_rate("-1B/s").is_err());
    check!(parse::positive_ratio("0").is_err());
    check!(parse::non_negative_ratio("-1%").is_err());
    check!(parse::unit_ratio("101%").is_err());

    for input in ["30", "1B", "1MiB/s"] {
        check!(parse::positive_time(input).is_err());
    }
    check!(parse::positive_byte_size("1s").is_err());
    check!(parse::positive_byte_rate("1MiB").is_err());
    check!(parse::positive_ratio("1s").is_err());
}

/// A dimensioned value without a unit is a mistake, not a default.
#[test]
fn parse_rejects_ambiguous_and_malformed_input() {
    assert!(let Err(ParseError::MissingUnit { .. }) = parse::byte_size("1048576 "));
    assert!(let Err(ParseError::MissingUnit { .. }) = parse::time("30000"));
    assert!(let Err(ParseError::Empty) = parse::time("   "));
    assert!(let Err(ParseError::UnknownUnit { dimension, .. }) = parse::byte_size("512furlongs"));
    check!(dimension == "size");
    assert!(let Err(ParseError::UnknownUnit { .. }) = parse::time("30 bytes"));
    assert!(let Err(ParseError::UnknownUnit { .. }) = parse::byte_rate("10MiB/furlong"));
    assert!(let Err(ParseError::InvalidNumber { .. }) = parse::byte_size("1.2.3MiB"));
    assert!(let Err(ParseError::InvalidNumber { .. }) = parse::byte_size("MiB"));
    assert!(let Err(ParseError::InvalidNumber { .. }) = parse::ratio("many%"));
}

#[test]
fn parse_rejects_unit_scaling_overflow() {
    assert!(matches!(
        parse::byte_size(&format!("{}TiB", f64::MAX)),
        Err(ParseError::NotFinite { .. })
    ));
    assert!(matches!(
        parse::time(&format!("{}d", f64::MAX)),
        Err(ParseError::NotFinite { .. })
    ));
    assert!(matches!(
        parse::byte_rate(&format!("{}TiB/s", f64::MAX)),
        Err(ParseError::NotFinite { .. })
    ));
    assert!(matches!(
        parse::frequency(&format!("{}/ns", f64::MAX)),
        Err(ParseError::NotFinite { .. })
    ));
}

/// Rendering picks the unit an operator would have written.
#[test]
fn rendering_picks_the_operator_unit() {
    let cases = [
        (bytes(0), "0B"),
        (bytes(512), "512B"),
        (kibibytes(4), "4KiB"),
        (mebibytes(512), "512MiB"),
        (gibibytes(2), "2GiB"),
        (bytes(1536), "1.5KiB"),
        (ByteSize::from_bytes(1_099_511_627_776), "1TiB"),
        // One byte past a whole TiB cannot be written in TiB without rounding, so
        // it falls back to the unit that keeps it exact.
        (ByteSize::from_bytes(1_099_511_627_777), "1099511627777B"),
        (ByteSize::from_bytes_f64(0.5), "0.5B"),
    ];
    for (size, expected) in cases {
        check!(size.human().to_string() == expected);
    }

    let extents = [
        (Time::ZERO, "0s"),
        (nanos(250), "250ns"),
        (micros(5), "5µs"),
        (millis(100), "100ms"),
        (secs(30), "30s"),
        (minutes(90), "1.5h"),
        (days(7), "7d"),
    ];
    for (extent, expected) in extents {
        check!(extent.human().to_string() == expected);
    }

    check!(mebibytes_per_sec(10).human().to_string() == "10MiB/s");
    check!(bytes_per_sec(0).human().to_string() == "0B/s");
    check!(per_sec(100).human().to_string() == "100/s");
    check!(percent(25).human().to_string() == "25%");
}

/// A magnitude too large for the thousandths arithmetic renders its base unit
/// instead of overflowing.
#[test]
fn absurd_magnitudes_fall_back_to_the_base_unit() {
    assert!(let Ok(huge) = parse::byte_size("1000000000000000000000000000000000000B"));
    let rendered = huge.human().to_string();
    check!(rendered.ends_with('B'));
    assert!(let Ok(reparsed) = parse::byte_size(&rendered));
    check!(reparsed == huge);
}

/// A non-finite quantity still renders, for diagnostics.
#[test]
fn rendering_survives_non_finite_values() {
    check!(ByteSize::from_bytes_f64(f64::NAN).human().to_string() == "NaNB");
    check!(Time::from_secs_f64(f64::INFINITY).human().to_string() == "infs");
}

/// A config struct holding quantities reads the operator form and the exact
/// integer form, and writes each back unchanged.
#[test]
fn serde_round_trips_both_encodings() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Config {
        #[serde(with = "serde_units::human::byte_size")]
        segment_size: ByteSize,
        #[serde(with = "serde_units::human::time")]
        retention: Time,
        #[serde(with = "serde_units::human::byte_rate")]
        quota: ByteRate,
        #[serde(with = "serde_units::human::option_byte_size")]
        max_message_size: Option<ByteSize>,
        #[serde(with = "serde_units::human::ratio")]
        fill_target: Ratio,
        #[serde(with = "serde_units::numeric::millis_i64")]
        session_timeout: Time,
        #[serde(with = "serde_units::numeric::bytes_u64")]
        fetch_max: ByteSize,
        #[serde(with = "serde_units::numeric::option_bytes_i64")]
        retention_bytes: Option<ByteSize>,
    }

    let json = r#"{
        "segment_size": "512MiB",
        "retention": "7d",
        "quota": "10MiB/s",
        "max_message_size": null,
        "fill_target": 0.75,
        "session_timeout": 30000,
        "fetch_max": 1048576,
        "retention_bytes": 2147483648
    }"#;

    assert!(let Ok(config) = serde_json::from_str::<Config>(json));
    check!(
        config
            == Config {
                segment_size: mebibytes(512),
                retention: days(7),
                quota: mebibytes_per_sec(10),
                max_message_size: None,
                fill_target: percent(75),
                session_timeout: secs(30),
                fetch_max: mebibytes(1),
                retention_bytes: Some(gibibytes(2)),
            }
    );

    assert!(let Ok(encoded) = serde_json::to_string(&config));
    assert!(let Ok(reparsed) = serde_json::from_str::<Config>(&encoded));
    check!(reparsed == config);
    check!(encoded.contains(r#""segment_size":"512MiB""#));
    check!(encoded.contains(r#""session_timeout":30000"#));
    check!(encoded.contains(r#""retention_bytes":2147483648"#));
}

/// An optional fraction round-trips in every encoding it accepts.
#[test]
fn optional_fractions_round_trip() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Config {
        #[serde(with = "serde_units::human::option_ratio")]
        dirty_ratio: Option<Ratio>,
    }

    assert!(let Ok(set) = serde_json::from_str::<Config>(r#"{"dirty_ratio": "2.5%"}"#));
    check!(set.dirty_ratio == Some(fraction(0.025)));

    assert!(let Ok(bare) = serde_json::from_str::<Config>(r#"{"dirty_ratio": 0.025}"#));
    check!(bare.dirty_ratio == set.dirty_ratio);

    assert!(let Ok(unset) = serde_json::from_str::<Config>(r#"{"dirty_ratio": null}"#));
    check!(unset.dirty_ratio == None);

    assert!(let Ok(encoded) = serde_json::to_string(&set));
    check!(encoded == r#"{"dirty_ratio":"2.5%"}"#);
    assert!(let Ok(reparsed) = serde_json::from_str::<Config>(&encoded));
    check!(reparsed == set);
}

/// A human-form field rejects a bare number and does not guess its unit.
#[test]
fn serde_human_form_requires_a_unit() {
    #[derive(Debug, Deserialize)]
    struct Config {
        #[serde(with = "serde_units::human::time")]
        retention: Time,
    }

    assert!(let Err(error) = serde_json::from_str::<Config>(r#"{"retention": 604800000}"#));
    check!(error.to_string().contains("string"));

    assert!(let Ok(config) = serde_json::from_str::<Config>(r#"{"retention": "7d"}"#));
    check!(config.retention == days(7));
}

proptest! {
    /// Every millisecond extent Kafka can express survives the round trip through
    /// a quantity exactly.
    #[test]
    fn millis_round_trip_exactly(raw in -1_000_000_000_000_i64..1_000_000_000_000) {
        prop_assert_eq!(Time::from_millis(raw).millis_i64(), raw);
    }

    /// Every byte count survives the round trip through a quantity exactly.
    #[test]
    fn bytes_round_trip_exactly(raw in 0_u64..(1_u64 << 52)) {
        prop_assert_eq!(ByteSize::from_bytes(raw).bytes_u64(), raw);
    }

    /// Rendering and parsing are inverses over the sizes an operator writes.
    #[test]
    fn rendered_sizes_parse_back(raw in 0_u64..(1_u64 << 48)) {
        let size = ByteSize::from_bytes(raw);
        let rendered = size.human().to_string();
        let reparsed = parse::byte_size(&rendered).expect("rendered size should parse");
        prop_assert_eq!(reparsed.bytes_u64(), raw);
    }

    /// Rendering and parsing are inverses over the millisecond extents an operator
    /// writes.
    #[test]
    fn rendered_extents_parse_back(raw in 0_i64..1_000_000_000_000) {
        let extent = Time::from_millis(raw);
        let rendered = extent.human().to_string();
        let reparsed = parse::time(&rendered).expect("rendered extent should parse");
        prop_assert_eq!(reparsed.millis_i64(), raw);
    }
}
