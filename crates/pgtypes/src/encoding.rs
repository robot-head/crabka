//! Text and binary wire encodings for Datums. NULL is signalled out-of-band by
//! the wire layer (DataRow value length -1), so encoding a NULL Datum panics —
//! it indicates a caller bug, never reachable from valid execution.

#![expect(
    clippy::pedantic,
    reason = "vendored PostgreSQL-compatible wire encodings kept structurally close to donor"
)]

use crate::Datum;

/// PostgreSQL text-format encoding of a (non-null) value.
///
/// `tz` is the session time zone used to render `Datum::Timestamptz` values.
/// All other variants ignore `tz`. Pass `&jiff::tz::TimeZone::UTC` when no
/// session zone is available yet (Task 9 threads the real zone from `EvalCtx`).
pub fn encode_text(d: &Datum, tz: &jiff::tz::TimeZone) -> Vec<u8> {
    match d {
        Datum::Null => panic!("encode_text called on NULL; NULL is signalled out-of-band"),
        Datum::Bool(b) => (if *b { "t" } else { "f" }).as_bytes().to_vec(),
        Datum::Int4(n) => n.to_string().into_bytes(),
        Datum::Int8(n) => n.to_string().into_bytes(),
        Datum::Text(s) => s.clone().into_bytes(),
        Datum::Float8(f) => encode_float8_text(*f).into_bytes(),
        // SP32: PostgreSQL `numeric_out` (plain decimal, dscale fractional digits).
        Datum::Numeric(d) => crate::numeric::to_text(d).into_bytes(),
        // SP37: text encodings. `Timestamptz` renders in the supplied session zone.
        Datum::Date(d) => crate::datetime::date_to_text(*d).into_bytes(),
        Datum::Time(t) => crate::datetime::time_to_text(*t).into_bytes(),
        Datum::Timestamp(ts) => crate::datetime::timestamp_to_text(*ts).into_bytes(),
        Datum::Timestamptz(ts) => crate::datetime::timestamptz_to_text(*ts, tz).into_bytes(),
        Datum::Interval(i) => crate::datetime::interval_to_text(*i).into_bytes(),
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
                        String::from_utf8(encode_text(e, tz))
                            .expect("a Datum's text encoding is always valid UTF-8")
                    })
                })
                .collect();
            crate::array::literal_text(&elements).into_bytes()
        }
    }
}

/// The `jsonb` binary-format version byte. PostgreSQL prefixes `jsonb_send`
/// output with it; only version 1 is defined.
pub const JSONB_BINARY_VERSION: u8 = 1;

/// PostgreSQL `float8out` text rendering. The IEEE specials are spelled exactly as
/// PostgreSQL does (`Infinity`/`-Infinity`/`NaN`); finite values use Rust's `f64`
/// `Display`, which — like PG since v12 — is the shortest round-tripping decimal, so the
/// two agree byte-for-byte for moderate magnitudes (`1.5`, `2.0`→`2`, `-0.0`→`-0`). The
/// one documented divergence is scientific notation for |x| ≥ 1e16 / 0 < |x| < 1e-4,
/// which PG emits and Rust does not.
fn encode_float8_text(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f > 0.0 { "Infinity" } else { "-Infinity" }.to_string()
    } else {
        format!("{f}")
    }
}

/// PostgreSQL binary-format encoding of a (non-null) value.
pub fn encode_binary(d: &Datum) -> Vec<u8> {
    match d {
        Datum::Null => panic!("encode_binary called on NULL; NULL is signalled out-of-band"),
        Datum::Bool(b) => vec![u8::from(*b)],
        Datum::Int4(n) => n.to_be_bytes().to_vec(),
        Datum::Int8(n) => n.to_be_bytes().to_vec(),
        Datum::Text(s) => s.clone().into_bytes(),
        // IEEE-754 big-endian, matching PostgreSQL's float8send.
        Datum::Float8(f) => f.to_be_bytes().to_vec(),
        // SP32: PostgreSQL `numeric_send` (base-10000 NBASE wire format).
        Datum::Numeric(d) => crate::numeric::binary(d),
        // SP37: binary encodings (Task 4). PG 2000-01-01 epoch for date/timestamp.
        Datum::Date(d) => crate::datetime::date_to_binary(*d).to_vec(),
        Datum::Time(t) => crate::datetime::time_to_binary(*t).to_vec(),
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
    }
}

/// PostgreSQL `array_send` for a one-dimensional array. An empty array is the
/// 12-byte `ndim = 0` header with no dimension block — the form libpq and
/// `tokio-postgres` emit, which must round-trip.
fn encode_array_binary(a: &crate::datum::ArrayValue) -> Vec<u8> {
    let has_null = a.elems.iter().any(Datum::is_null);
    let ndim: i32 = if a.elems.is_empty() { 0 } else { 1 };
    let mut out = Vec::with_capacity(20 + a.elems.len() * 8);
    out.extend_from_slice(&ndim.to_be_bytes());
    out.extend_from_slice(&i32::from(has_null).to_be_bytes());
    out.extend_from_slice(&a.elem.oid().to_be_bytes());
    if ndim == 1 {
        let len = i32::try_from(a.elems.len()).expect("array length exceeds i32");
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&1i32.to_be_bytes()); // lower bound
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
