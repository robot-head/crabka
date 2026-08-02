//! Text and binary wire encodings for Datums. The wire layer signals NULL
//! out-of-band (DataRow value length -1), so an encode of a NULL Datum panics.
//! That panic shows a caller bug, and valid execution never reaches it.

#![expect(
    clippy::pedantic,
    reason = "vendored PostgreSQL-compatible wire encodings kept structurally close to donor"
)]

use crate::Datum;

/// Everything the text output functions read out of the session: the zone a
/// `timestamptz` is rendered in, plus the `DateStyle` and `IntervalStyle` GUCs
/// that decide how a date/time or interval is spelled.
///
/// PostgreSQL reads these at output time, so one stored value has several valid
/// renderings and the session settings have to reach the encoder.
#[derive(Debug, Clone, Copy)]
pub struct OutputStyle<'a> {
    pub time_zone: &'a jiff::tz::TimeZone,
    pub date_style: crate::datetime::DateStyle,
    pub date_order: crate::datetime::DateOrder,
    pub interval_style: crate::datetime::IntervalStyle,
}

impl<'a> OutputStyle<'a> {
    /// A zone with every style left at its PostgreSQL default (`ISO, MDY` and
    /// `postgres`). Use it for the callers that render a canonical value rather
    /// than a session-visible one: storage keys, catalog defaults, and tests.
    #[must_use]
    pub fn with_zone(time_zone: &'a jiff::tz::TimeZone) -> Self {
        Self {
            time_zone,
            date_style: crate::datetime::DateStyle::default(),
            date_order: crate::datetime::DateOrder::default(),
            interval_style: crate::datetime::IntervalStyle::default(),
        }
    }
}

/// PostgreSQL text-format encoding of a (non-null) value, in the session's
/// default styles. Prefer [`encode_text_in`] wherever the session is available.
/// This spelling is the canonical one (`ISO` dates, `postgres` intervals).
pub fn encode_text(d: &Datum, tz: &jiff::tz::TimeZone) -> Vec<u8> {
    encode_text_in(d, OutputStyle::with_zone(tz))
}

/// PostgreSQL text-format encoding of a (non-null) value in the session's
/// output styles.
pub fn encode_text_in(d: &Datum, style: OutputStyle<'_>) -> Vec<u8> {
    let tz = style.time_zone;
    match d {
        Datum::Null => panic!("encode_text called on NULL; NULL is signalled out-of-band"),
        Datum::Bool(b) => (if *b { "t" } else { "f" }).as_bytes().to_vec(),
        Datum::Int2(n) => n.to_string().into_bytes(),
        Datum::Int4(n) => n.to_string().into_bytes(),
        Datum::Int8(n) => n.to_string().into_bytes(),
        Datum::Text(s) => s.clone().into_bytes(),
        Datum::Float4(f) => encode_float4_text(*f).into_bytes(),
        Datum::Float8(f) => encode_float8_text(*f).into_bytes(),
        Datum::Point(point) => format!(
            "({},{})",
            encode_float8_text(point.x),
            encode_float8_text(point.y)
        )
        .into_bytes(),
        Datum::Path(path) => {
            let mut out = String::from(if path.closed { "(" } else { "[" });
            for (index, point) in path.points.iter().enumerate() {
                if index != 0 {
                    out.push(',');
                }
                out.push('(');
                out.push_str(&encode_float8_text(point.x));
                out.push(',');
                out.push_str(&encode_float8_text(point.y));
                out.push(')');
            }
            out.push(if path.closed { ')' } else { ']' });
            out.into_bytes()
        }
        // SP32: PostgreSQL `numeric_out` (plain decimal, dscale fractional digits).
        Datum::Numeric(d) => crate::numeric::to_text(d).into_bytes(),
        // SP37: text encodings. `Timestamptz` renders in the supplied session zone.
        Datum::Date(d) => {
            crate::datetime::date_to_text_in(*d, style.date_style, style.date_order).into_bytes()
        }
        Datum::Time(t) => crate::datetime::time_to_text(*t).into_bytes(),
        Datum::Timetz(t) => crate::datetime::timetz_to_text(*t).into_bytes(),
        Datum::Timestamp(ts) => {
            crate::datetime::timestamp_to_text_in(*ts, style.date_style, style.date_order)
                .into_bytes()
        }
        Datum::Timestamptz(ts) => {
            crate::datetime::timestamptz_to_text_in(*ts, tz, style.date_style, style.date_order)
                .into_bytes()
        }
        Datum::Interval(i) => {
            crate::datetime::interval_to_text_in(*i, style.interval_style).into_bytes()
        }
        // SP40: PostgreSQL `byteaout` hex format: `\x` + lowercase hex digits.
        Datum::Bytea(b) => {
            let mut out = Vec::with_capacity(2 + b.len() * 2);
            out.extend_from_slice(b"\\x");
            for byte in b {
                out.push(b"0123456789abcdef"[usize::from(*byte >> 4)]);
                out.push(b"0123456789abcdef"[usize::from(*byte & 0xf)]);
            }
            out
        }
        // `jsonb_out`: the canonical re-rendering of the decomposed value.
        Datum::Jsonb(j) => j.to_text().into_bytes(),
        // `array_out`: `{...}` with each element rendered by its own output
        // function and quoted when PostgreSQL would quote it.
        Datum::Array(a) => {
            let elements: Vec<Option<String>> = a
                .elems
                .iter()
                .map(|e| {
                    (!e.is_null()).then(|| {
                        String::from_utf8(encode_text_in(e, style))
                            .expect("a Datum's text encoding is always valid UTF-8")
                    })
                })
                .collect();
            crate::array::literal_text(&a.dims, &elements).into_bytes()
        }
        // `record_out`: `(f1,f2,…)`, a NULL field written as nothing and any
        // field that would otherwise be ambiguous double-quoted.
        Datum::Record(r) => {
            let fields: Vec<Option<String>> = r
                .values
                .iter()
                .map(|v| {
                    (!v.is_null()).then(|| {
                        String::from_utf8(encode_text_in(v, style))
                            .expect("a Datum's text encoding is always valid UTF-8")
                    })
                })
                .collect();
            crate::composite::record_out(&fields).into_bytes()
        }
        // `enum_out`: the label, verbatim.
        Datum::Enum(e) => e.label.clone().into_bytes(),
        // `regclassout`: the relation name the oid resolved to — already quoted
        // and, for an oid no relation matches, already the bare number.
        Datum::Regclass(r) => r.name.as_bytes().to_vec(),
        Datum::TsVector(vector) => vector.to_string().into_bytes(),
        Datum::TsQuery(query) => query.to_string().into_bytes(),
    }
}

/// The `jsonb` binary-format version byte. PostgreSQL prefixes `jsonb_send`
/// output with it; only version 1 is defined.
pub const JSONB_BINARY_VERSION: u8 = 1;

/// PostgreSQL `float8out` text rendering.
///
/// This function spells the IEEE specials exactly as PostgreSQL does
/// (`Infinity`/`-Infinity`/`NaN`). Finite values use Rust's `f64` `Display`,
/// which is the shortest round-tripping decimal, as PG has been since v12. The
/// two therefore agree byte-for-byte for moderate magnitudes (`1.5`, `2.0`→`2`,
/// `-0.0`→`-0`). The one documented divergence is scientific notation for
/// |x| ≥ 1e16 / 0 < |x| < 1e-4, which PG emits and Rust does not.
fn encode_float8_text(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f > 0.0 { "Infinity" } else { "-Infinity" }.to_string()
    } else {
        format!("{f}")
    }
}

/// PostgreSQL `float4out` text rendering (`extra_float_digits >= 1`, the default
/// since PG 12).
///
/// The output is the shortest decimal that round-trips through `f32`, laid out
/// like `printf %g` with precision `FLT_DIG`. It is fixed-point while the
/// decimal exponent is in `-4 ..= 5` and scientific outside it, with a signed
/// two-digit exponent (`1e+06`, `1e-05`, `3.4028235e+38`). The IEEE specials are
/// spelled `Infinity` / `-Infinity` / `NaN`.
///
/// Rust supplies both halves: `{:e}` is the shortest round-tripping mantissa
/// plus the decimal exponent, and `{}` is the same digits laid out fixed-point.
fn encode_float4_text(f: f32) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    let scientific = format!("{f:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("Rust's LowerExp always emits an `e`");
    let exponent: i32 = exponent
        .parse()
        .expect("Rust's LowerExp exponent is a decimal integer");
    if (-4..FLOAT4_FIXED_EXPONENT_LIMIT).contains(&exponent) {
        format!("{f}")
    } else {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{mantissa}e{sign}{:02}", exponent.abs())
    }
}

/// `FLT_DIG`: the decimal exponent at which `float4out` switches to scientific
/// notation. It matches `%g`'s precision-driven threshold.
const FLOAT4_FIXED_EXPONENT_LIMIT: i32 = 6;

/// PostgreSQL binary-format encoding of a (non-null) value.
pub fn encode_binary(d: &Datum) -> Vec<u8> {
    match d {
        Datum::Null => panic!("encode_binary called on NULL; NULL is signalled out-of-band"),
        Datum::Bool(b) => vec![u8::from(*b)],
        Datum::Int2(n) => n.to_be_bytes().to_vec(),
        Datum::Int4(n) => n.to_be_bytes().to_vec(),
        Datum::Int8(n) => n.to_be_bytes().to_vec(),
        Datum::Text(s) => s.clone().into_bytes(),
        // IEEE-754 big-endian, matching PostgreSQL's float4send / float8send.
        Datum::Float4(f) => f.to_be_bytes().to_vec(),
        Datum::Float8(f) => f.to_be_bytes().to_vec(),
        Datum::Point(point) => {
            let mut out = Vec::with_capacity(16);
            out.extend_from_slice(&point.x.to_be_bytes());
            out.extend_from_slice(&point.y.to_be_bytes());
            out
        }
        Datum::Path(path) => {
            let mut out = Vec::with_capacity(5 + path.points.len() * 16);
            out.push(u8::from(path.closed));
            out.extend_from_slice(
                &i32::try_from(path.points.len())
                    .expect("path has more than i32::MAX points")
                    .to_be_bytes(),
            );
            for point in &path.points {
                out.extend_from_slice(&point.x.to_be_bytes());
                out.extend_from_slice(&point.y.to_be_bytes());
            }
            out
        }
        // SP32: PostgreSQL `numeric_send` (base-10000 NBASE wire format).
        Datum::Numeric(d) => crate::numeric::binary(d),
        // SP37: binary encodings (Task 4). PG 2000-01-01 epoch for date/timestamp.
        Datum::Date(d) => crate::datetime::date_to_binary(*d).to_vec(),
        Datum::Time(t) => crate::datetime::time_to_binary(*t).to_vec(),
        Datum::Timetz(t) => crate::datetime::timetz_to_binary(*t).to_vec(),
        Datum::Timestamp(ts) => crate::datetime::timestamp_to_binary(*ts).to_vec(),
        Datum::Timestamptz(ts) => crate::datetime::timestamptz_to_binary(*ts).to_vec(),
        Datum::Interval(i) => crate::datetime::interval_to_binary(*i).to_vec(),
        // SP40: `byteasend` — raw bytes (no transformation).
        Datum::Bytea(b) => b.clone(),
        // `jsonb_send`: a version byte then the canonical JSON text.
        Datum::Jsonb(j) => {
            let text = j.to_text();
            let mut out = Vec::with_capacity(1 + text.len());
            out.push(JSONB_BINARY_VERSION);
            out.extend_from_slice(text.as_bytes());
            out
        }
        // `array_send`: the standard big-endian array header then each element
        // as `i32 length` (-1 for NULL) plus its own binary encoding.
        Datum::Array(a) => encode_array_binary(a),
        // `record_send`: the field count, then per field its type oid and its
        // own binary encoding with an `i32` length (-1 for NULL).
        Datum::Record(r) => encode_record_binary(r),
        // `enum_send` is `textsend` of the label.
        Datum::Enum(e) => e.label.clone().into_bytes(),
        // `regclasssend` is `oidsend`: the 4-byte oid, not the name.
        Datum::Regclass(r) => r.oid.to_be_bytes().to_vec(),
        // Deliberate wire divergence: text-search values use their canonical
        // UTF-8 representation in binary fields until their internal varlena
        // layouts are implemented. Their OIDs and text format are exact.
        Datum::TsVector(vector) => vector.to_string().into_bytes(),
        Datum::TsQuery(query) => query.to_string().into_bytes(),
    }
}

/// PostgreSQL `record_send`: `int32 nfields`, then for each field an `Oid`, an
/// `int32` length (-1 for NULL) and the field's own binary encoding.
fn encode_record_binary(r: &crate::datum::RecordValue) -> Vec<u8> {
    let nfields = i32::try_from(r.values.len()).expect("record fields exceed i32");
    let mut out = Vec::with_capacity(4 + r.values.len() * 12);
    out.extend_from_slice(&nfields.to_be_bytes());
    for value in &r.values {
        // A NULL field carries no type information in the value, and
        // PostgreSQL sends the column's declared oid; crabka has only the
        // value, so a NULL field is sent as `text` (25) with a -1 length. The
        // length is what a client reads to know the field is NULL.
        let oid = value.column_type().map_or(crate::oids::TEXT, |ty| ty.oid());
        out.extend_from_slice(&oid.to_be_bytes());
        if value.is_null() {
            out.extend_from_slice(&(-1i32).to_be_bytes());
        } else {
            let bytes = encode_binary(value);
            let len = i32::try_from(bytes.len()).expect("record field exceeds i32 bytes");
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(&bytes);
        }
    }
    out
}

/// PostgreSQL `array_send`. An empty array is the 12-byte `ndim = 0` header with
/// no dimension block, the form libpq and `tokio-postgres` emit, which must
/// round-trip. Every other array carries one `(length, lower bound)` pair per
/// dimension ahead of its row-major elements.
fn encode_array_binary(a: &crate::datum::ArrayValue) -> Vec<u8> {
    let has_null = a.elems.iter().any(Datum::is_null);
    let ndim = i32::try_from(a.dims.len()).expect("array dimensions exceed i32");
    let mut out = Vec::with_capacity(20 + a.elems.len() * 8);
    out.extend_from_slice(&ndim.to_be_bytes());
    out.extend_from_slice(&i32::from(has_null).to_be_bytes());
    out.extend_from_slice(&a.elem.oid().to_be_bytes());
    for dim in &a.dims {
        out.extend_from_slice(&dim.len.to_be_bytes());
        out.extend_from_slice(&dim.lower.to_be_bytes());
    }
    for elem in &a.elems {
        if elem.is_null() {
            out.extend_from_slice(&(-1i32).to_be_bytes());
        } else {
            let bytes = encode_binary(elem);
            let len = i32::try_from(bytes.len()).expect("array element exceeds i32 bytes");
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(&bytes);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Datum;

    fn utc() -> jiff::tz::TimeZone {
        jiff::tz::TimeZone::UTC
    }

    #[test]
    fn text_encoding_matches_postgres() {
        let tz = utc();
        assert_eq!(encode_text(&Datum::Bool(true), &tz), b"t");
        assert_eq!(encode_text(&Datum::Bool(false), &tz), b"f");
        assert_eq!(encode_text(&Datum::Int4(-5), &tz), b"-5");
        assert_eq!(encode_text(&Datum::Int8(9_000_000_000), &tz), b"9000000000");
        assert_eq!(encode_text(&Datum::Text("hi".into()), &tz), b"hi");
    }

    #[test]
    fn float8_text_encoding_matches_postgres() {
        let tz = utc();
        // Shortest round-trip for moderate magnitudes (agrees with PG float8out).
        assert_eq!(encode_text(&Datum::Float8(1.5), &tz), b"1.5");
        assert_eq!(encode_text(&Datum::Float8(2.0), &tz), b"2"); // PG: `2`
        assert_eq!(encode_text(&Datum::Float8(0.1), &tz), b"0.1");
        assert_eq!(encode_text(&Datum::Float8(-0.0), &tz), b"-0"); // PG: `-0`
        assert_eq!(
            encode_text(&Datum::Float8(1.0 / 3.0), &tz),
            b"0.3333333333333333"
        );
        // IEEE specials spelled as PostgreSQL spells them.
        assert_eq!(encode_text(&Datum::Float8(f64::INFINITY), &tz), b"Infinity");
        assert_eq!(
            encode_text(&Datum::Float8(f64::NEG_INFINITY), &tz),
            b"-Infinity"
        );
        assert_eq!(encode_text(&Datum::Float8(f64::NAN), &tz), b"NaN");
    }

    /// Every pair here is `SELECT <input>::float4::text` on PostgreSQL 18.4.
    /// The pairs cover both sides of the fixed/scientific threshold, the
    /// two-digit exponent padding, negative zero, and the IEEE specials.
    #[test]
    fn float4_text_matches_postgres_float4out() {
        use assert2::assert;
        let tz = utc();
        let cases: &[(f32, &str)] = &[
            (0.0, "0"),
            (-0.0, "-0"),
            (1.0, "1"),
            (1.5, "1.5"),
            (-2.25, "-2.25"),
            (0.1, "0.1"),
            (1.0 / 3.0, "0.33333334"),
            (2.0, "2"),
            // Fixed while the decimal exponent is in -4 ..= 5 …
            (100_000.0, "100000"),
            (999_999.0, "999999"),
            (123_456.0, "123456"),
            (123_456.7, "123456.7"),
            (999_999.9, "999999.9"),
            (0.0001, "0.0001"),
            // … scientific outside it, with a signed two-digit exponent.
            (1_000_000.0, "1e+06"),
            (1_234_567.0, "1.234567e+06"),
            (16_777_216.0, "1.6777216e+07"),
            (12_345_678.0, "1.2345678e+07"),
            (1.5e8, "1.5e+08"),
            (1e20, "1e+20"),
            (6.02e23, "6.02e+23"),
            (0.00001, "1e-05"),
            (9.999_999e-5, "9.999999e-05"),
            (-1e-7, "-1e-07"),
            (1e-10, "1e-10"),
            // Range extremes, including a subnormal.
            (3.402_823_5e38, "3.4028235e+38"),
            (-3.402_823_5e38, "-3.4028235e+38"),
            (1.175_494_4e-38, "1.1754944e-38"),
            (1e-45, "1e-45"),
            (f32::INFINITY, "Infinity"),
            (f32::NEG_INFINITY, "-Infinity"),
            (f32::NAN, "NaN"),
        ];
        for (value, expected) in cases {
            assert!(
                encode_text(&Datum::Float4(*value), &tz) == expected.as_bytes(),
                "float4out({value:?})"
            );
        }
    }

    #[test]
    fn int2_and_float4_wire_encodings_are_two_and_four_network_order_bytes() {
        use assert2::assert;
        let tz = utc();
        assert!(encode_text(&Datum::Int2(-32_768), &tz) == b"-32768");
        assert!(encode_text(&Datum::Int2(32_767), &tz) == b"32767");
        assert!(encode_binary(&Datum::Int2(-2)) == (-2i16).to_be_bytes().to_vec());
        assert!(encode_binary(&Datum::Int2(-2)).len() == 2);
        assert!(encode_binary(&Datum::Float4(1.5)) == 1.5f32.to_be_bytes().to_vec());
        assert!(encode_binary(&Datum::Float4(1.5)).len() == 4);
    }

    #[test]
    fn binary_encoding_is_network_order() {
        assert_eq!(encode_binary(&Datum::Bool(true)), vec![1]);
        assert_eq!(encode_binary(&Datum::Bool(false)), vec![0]);
        assert_eq!(encode_binary(&Datum::Int4(1)), 1i32.to_be_bytes().to_vec());
        assert_eq!(encode_binary(&Datum::Int8(1)), 1i64.to_be_bytes().to_vec());
        assert_eq!(encode_binary(&Datum::Text("hi".into())), b"hi".to_vec());
        // float8 is IEEE-754 big-endian (PG float8send).
        assert_eq!(
            encode_binary(&Datum::Float8(1.5)),
            1.5f64.to_be_bytes().to_vec()
        );
    }

    #[test]
    #[should_panic]
    fn encoding_null_is_a_caller_error() {
        let _ = encode_text(&Datum::Null, &utc());
    }

    // ---- jsonb / array wire formats ----

    fn jsonb(text: &str) -> Datum {
        Datum::Jsonb(crate::jsonb::parse(text).expect("valid jsonb"))
    }

    fn int_array(values: &[Option<i32>]) -> Datum {
        Datum::Array(crate::datum::ArrayValue::new(
            crate::ElemType::Int4,
            values
                .iter()
                .map(|v| v.map_or(Datum::Null, Datum::Int4))
                .collect(),
        ))
    }

    #[test]
    fn jsonb_text_is_the_canonical_rendering() {
        use assert2::assert;
        let tz = utc();
        assert!(encode_text(&jsonb(r#"{"b":1,"a":[1,2]}"#), &tz) == br#"{"a": [1, 2], "b": 1}"#);
    }

    #[test]
    fn jsonb_binary_is_a_version_byte_then_canonical_text() {
        use assert2::assert;
        let encoded = encode_binary(&jsonb(r#"{"b":1,"a":2}"#));
        let mut expected = vec![1u8];
        expected.extend_from_slice(br#"{"a": 2, "b": 1}"#);
        assert!(encoded == expected);
        assert!(encoded[0] == JSONB_BINARY_VERSION);
    }

    #[test]
    fn array_text_quotes_like_array_out() {
        use assert2::assert;
        let tz = utc();
        assert!(encode_text(&int_array(&[Some(1), None, Some(3)]), &tz) == b"{1,NULL,3}");
        assert!(encode_text(&int_array(&[]), &tz) == b"{}");
        let texts = Datum::Array(crate::datum::ArrayValue::new(
            crate::ElemType::Text,
            vec![
                Datum::Text("plain".into()),
                Datum::Text("a,b".into()),
                Datum::Text("NULL".into()),
                Datum::Null,
            ],
        ));
        assert!(encode_text(&texts, &tz) == br#"{plain,"a,b","NULL",NULL}"#);
    }

    #[test]
    fn array_binary_matches_the_postgres_layout() {
        use assert2::assert;
        // ndim=1, hasnull=0, elemoid=23 (int4), dim len=2, lbound=1, then
        // {len=4, 1}, {len=4, 2}.
        let expected: Vec<u8> = [
            &1i32.to_be_bytes()[..],
            &0i32.to_be_bytes()[..],
            &23u32.to_be_bytes()[..],
            &2i32.to_be_bytes()[..],
            &1i32.to_be_bytes()[..],
            &4i32.to_be_bytes()[..],
            &1i32.to_be_bytes()[..],
            &4i32.to_be_bytes()[..],
            &2i32.to_be_bytes()[..],
        ]
        .concat();
        assert!(encode_binary(&int_array(&[Some(1), Some(2)])) == expected);
    }

    #[test]
    fn array_binary_flags_nulls_and_uses_minus_one_lengths() {
        use assert2::assert;
        let encoded = encode_binary(&int_array(&[None, Some(7)]));
        // hasnull is the second i32.
        assert!(encoded[4..8] == 1i32.to_be_bytes());
        // The NULL element is a bare -1 length with no payload.
        let tail: Vec<u8> = [
            &(-1i32).to_be_bytes()[..],
            &4i32.to_be_bytes()[..],
            &7i32.to_be_bytes()[..],
        ]
        .concat();
        assert!(encoded[20..] == tail[..]);
    }

    #[test]
    fn empty_array_binary_is_the_twelve_byte_ndim_zero_form() {
        use assert2::assert;
        // What tokio-postgres emits for an empty array: no dimension block.
        let encoded = encode_binary(&int_array(&[]));
        let expected: Vec<u8> = [
            &0i32.to_be_bytes()[..],
            &0i32.to_be_bytes()[..],
            &23u32.to_be_bytes()[..],
        ]
        .concat();
        assert!(encoded == expected);
        assert!(encoded.len() == 12);
    }

    #[test]
    fn jsonb_inside_an_array_is_quoted_and_length_prefixed() {
        use assert2::assert;
        let tz = utc();
        let value = Datum::Array(crate::datum::ArrayValue::new(
            crate::ElemType::Jsonb,
            vec![jsonb(r#"{"a":1}"#)],
        ));
        assert!(encode_text(&value, &tz) == br#"{"{\"a\": 1}"}"#);
        let encoded = encode_binary(&value);
        // elem oid is jsonb (3802) and the element carries its version byte.
        assert!(encoded[8..12] == 3802u32.to_be_bytes());
        assert!(encoded[24] == JSONB_BINARY_VERSION);
    }

    #[test]
    fn timestamptz_text_uses_supplied_zone() {
        let ny = jiff::tz::TimeZone::get("America/New_York").expect("ny");
        let ts =
            crate::datetime::parse_timestamptz("2024-01-15 12:00:00", &jiff::tz::TimeZone::UTC)
                .expect("ts");
        assert_eq!(
            encode_text(&Datum::Timestamptz(ts), &ny),
            b"2024-01-15 07:00:00-05"
        );
        assert_eq!(
            encode_text(&Datum::Timestamptz(ts), &jiff::tz::TimeZone::UTC),
            b"2024-01-15 12:00:00+00"
        );
        // Non-timestamptz variants ignore tz.
        assert_eq!(encode_text(&Datum::Int4(5), &ny), b"5");
    }
}
