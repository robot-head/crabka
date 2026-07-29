//! Versioned row value encoding: a leading version byte then one tagged field
//! per column. NOT order-preserving — values are never sorted by raw bytes.

use crabka_pgtypes::Datum;

use crate::KvError;

/// Current row-value format version.
pub const ROW_VERSION: u8 = 1;

mod tag {
    pub const NULL: u8 = 0;
    pub const BOOL: u8 = 1;
    pub const INT4: u8 = 2;
    pub const INT8: u8 = 3;
    pub const TEXT: u8 = 4;
    pub const FLOAT8: u8 = 5;
    pub const NUMERIC: u8 = 6;
    pub const DATE: u8 = 7;
    pub const TIME: u8 = 8;
    pub const TIMESTAMP: u8 = 9;
    pub const TIMESTAMPTZ: u8 = 10;
    pub const INTERVAL: u8 = 11;
    pub const BYTEA: u8 = 12;
    /// `jsonb`, stored as its canonical text (`[13][u32 len][text]`); decoding
    /// re-parses. Append-only — no version bump.
    pub const JSONB: u8 = 13;
    /// A one-dimensional array (`[14][elem code][u32 count][elements...]`), each
    /// element encoded by the same tagged-field format. Append-only.
    pub const ARRAY: u8 = 14;
}

/// Encode one row using the current storage format.
///
/// # Panics
///
/// Panics when a variable-width datum exceeds the format's 4 GiB field limit.
#[must_use]
pub fn encode_row(cols: &[Datum]) -> Vec<u8> {
    let mut out = vec![ROW_VERSION];
    encode_fields(cols, &mut out);
    out
}

/// Append `cols` as tagged fields (the row body, without the version byte —
/// also the payload format for array elements).
fn encode_fields(cols: &[Datum], out: &mut Vec<u8>) {
    for d in cols {
        match d {
            Datum::Null => out.push(tag::NULL),
            Datum::Bool(b) => {
                out.push(tag::BOOL);
                out.push(u8::from(*b));
            }
            Datum::Int4(n) => {
                out.push(tag::INT4);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Datum::Int8(n) => {
                out.push(tag::INT8);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Datum::Text(s) => {
                out.push(tag::TEXT);
                let len = u32::try_from(s.len()).expect("text column exceeds 4 GiB");
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            Datum::Float8(f) => {
                out.push(tag::FLOAT8);
                out.extend_from_slice(&f.to_be_bytes());
            }
            Datum::Numeric(d) => {
                out.push(tag::NUMERIC);
                let s = crabka_pgtypes::numeric::to_text(d);
                let len = u32::try_from(s.len()).expect("numeric text exceeds 4 GiB");
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            Datum::Date(d) => {
                out.push(tag::DATE);
                out.extend_from_slice(&crabka_pgtypes::datetime::date_to_binary(*d));
            }
            Datum::Time(t) => {
                out.push(tag::TIME);
                out.extend_from_slice(&crabka_pgtypes::datetime::time_to_binary(*t));
            }
            Datum::Timestamp(ts) => {
                out.push(tag::TIMESTAMP);
                out.extend_from_slice(&crabka_pgtypes::datetime::timestamp_to_binary(*ts));
            }
            Datum::Timestamptz(ts) => {
                out.push(tag::TIMESTAMPTZ);
                out.extend_from_slice(&crabka_pgtypes::datetime::timestamptz_to_binary(*ts));
            }
            Datum::Interval(iv) => {
                out.push(tag::INTERVAL);
                out.extend_from_slice(&crabka_pgtypes::datetime::interval_to_binary(*iv));
            }
            Datum::Bytea(b) => {
                out.push(tag::BYTEA);
                let len = u32::try_from(b.len()).expect("bytea column exceeds 4 GiB");
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(b);
            }
            Datum::Jsonb(j) => {
                out.push(tag::JSONB);
                let text = j.to_text();
                let len = u32::try_from(text.len()).expect("jsonb column exceeds 4 GiB");
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(text.as_bytes());
            }
            Datum::Array(a) => {
                out.push(tag::ARRAY);
                out.push(a.elem.code());
                let count = u32::try_from(a.elems.len()).expect("array exceeds 4G elements");
                out.extend_from_slice(&count.to_be_bytes());
                // Elements reuse the same tagged-field encoding, so a `jsonb`
                // inside an array (or a NULL element) needs no special case.
                encode_fields(&a.elems, out);
            }
        }
    }
}

/// Decode row bytes into datum values.
///
/// # Errors
///
/// Returns [`KvError::CorruptRow`] when the version, field tag, field length, or
/// encoded value is invalid.
///
/// # Panics
///
/// Panics only if a fixed-width slice validated by the decoder cannot be
/// converted to its corresponding fixed-size array.
pub fn decode_row(bytes: &[u8]) -> Result<Vec<Datum>, KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if version != ROW_VERSION {
        return Err(KvError::CorruptRow(format!(
            "unknown row version {version}"
        )));
    }
    let mut cols = Vec::new();
    while !cur.is_empty() {
        cols.push(decode_field(&mut cur)?);
    }
    Ok(cols)
}

/// Decode one tagged field, advancing `cur` past it.
fn decode_field(cur: &mut &[u8]) -> Result<Datum, KvError> {
    let t = take_u8(cur)?;
    Ok(match t {
        tag::NULL => Datum::Null,
        tag::BOOL => Datum::Bool(take_bool(cur)?),
        tag::INT4 => {
            let raw = take_n(cur, 4)?;
            Datum::Int4(i32::from_be_bytes(raw.try_into().expect("4 bytes fit i32")))
        }
        tag::INT8 => {
            let raw = take_n(cur, 8)?;
            Datum::Int8(i64::from_be_bytes(raw.try_into().expect("8 bytes fit i64")))
        }
        tag::TEXT => {
            let len = take_u32_len(cur)?;
            let raw = take_n(cur, len)?;
            Datum::Text(
                String::from_utf8(raw.to_vec())
                    .map_err(|_| KvError::CorruptRow("text is not valid UTF-8".into()))?,
            )
        }
        tag::FLOAT8 => {
            let raw = take_n(cur, 8)?;
            Datum::Float8(f64::from_be_bytes(raw.try_into().expect("8 bytes fit f64")))
        }
        tag::NUMERIC => {
            let len = take_u32_len(cur)?;
            let raw = take_n(cur, len)?;
            let s = std::str::from_utf8(raw)
                .map_err(|_| KvError::CorruptRow("numeric text is not valid UTF-8".into()))?;
            Datum::Numeric(
                crabka_pgtypes::numeric::parse(s)
                    .ok_or_else(|| KvError::CorruptRow(format!("invalid numeric {s:?}")))?,
            )
        }
        tag::DATE => {
            let raw = take_n(cur, 4)?;
            Datum::Date(
                crabka_pgtypes::datetime::date_from_binary(raw)
                    .map_err(|e| KvError::CorruptRow(format!("corrupt date: {e}")))?,
            )
        }
        tag::TIME => {
            let raw = take_n(cur, 8)?;
            Datum::Time(
                crabka_pgtypes::datetime::time_from_binary(raw)
                    .map_err(|e| KvError::CorruptRow(format!("corrupt time: {e}")))?,
            )
        }
        tag::TIMESTAMP => {
            let raw = take_n(cur, 8)?;
            Datum::Timestamp(
                crabka_pgtypes::datetime::timestamp_from_binary(raw)
                    .map_err(|e| KvError::CorruptRow(format!("corrupt timestamp: {e}")))?,
            )
        }
        tag::TIMESTAMPTZ => {
            let raw = take_n(cur, 8)?;
            Datum::Timestamptz(
                crabka_pgtypes::datetime::timestamptz_from_binary(raw)
                    .map_err(|e| KvError::CorruptRow(format!("corrupt timestamptz: {e}")))?,
            )
        }
        tag::INTERVAL => {
            let raw = take_n(cur, 16)?;
            Datum::Interval(
                crabka_pgtypes::datetime::interval_from_binary(raw)
                    .map_err(|e| KvError::CorruptRow(format!("corrupt interval: {e}")))?,
            )
        }
        tag::BYTEA => {
            let len = take_u32_len(cur)?;
            let raw = take_n(cur, len)?;
            Datum::Bytea(raw.to_vec())
        }
        tag::JSONB => {
            let len = take_u32_len(cur)?;
            let raw = take_n(cur, len)?;
            let text = std::str::from_utf8(raw)
                .map_err(|_| KvError::CorruptRow("jsonb text is not valid UTF-8".into()))?;
            Datum::Jsonb(
                crabka_pgtypes::jsonb::parse(text)
                    .map_err(|e| KvError::CorruptRow(format!("corrupt jsonb: {e}")))?,
            )
        }
        tag::ARRAY => {
            let code = take_u8(cur)?;
            let elem = crabka_pgtypes::ElemType::from_code(code)
                .ok_or_else(|| KvError::CorruptRow(format!("unknown array element code {code}")))?;
            let count = take_u32_len(cur)?;
            let mut elems = Vec::new();
            for _ in 0..count {
                elems.push(decode_field(cur)?);
            }
            Datum::Array(crabka_pgtypes::ArrayValue::new(elem, elems))
        }
        other => return Err(KvError::CorruptRow(format!("unknown field tag {other}"))),
    })
}

fn take_u8(cur: &mut &[u8]) -> Result<u8, KvError> {
    let (head, rest) = cur
        .split_first()
        .ok_or_else(|| KvError::CorruptRow("truncated".into()))?;
    *cur = rest;
    Ok(*head)
}

fn take_bool(cur: &mut &[u8]) -> Result<bool, KvError> {
    match take_u8(cur)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(KvError::CorruptRow(format!("invalid bool payload {other}"))),
    }
}

fn take_u32_len(cur: &mut &[u8]) -> Result<usize, KvError> {
    let len_raw = take_n(cur, 4)?;
    let len = u32::from_be_bytes(len_raw.try_into().expect("4 bytes fit u32"));
    usize::try_from(len).map_err(|_| KvError::CorruptRow("length does not fit usize".into()))
}

fn take_n<'a>(cur: &mut &'a [u8], n: usize) -> Result<&'a [u8], KvError> {
    if cur.len() < n {
        return Err(KvError::CorruptRow("truncated field".into()));
    }
    let (head, rest) = cur.split_at(n);
    *cur = rest;
    Ok(head)
}

#[cfg(test)]
mod tests {
    use crabka_pgtypes::Datum;
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn roundtrip_all_datum_kinds_including_null() {
        let row = vec![
            Datum::Null,
            Datum::Bool(true),
            Datum::Int4(i32::MIN),
            Datum::Int8(i64::MIN),
            Datum::Text("héllo".into()),
            Datum::Float8(-1.5),
            Datum::Float8(f64::NAN),
            Datum::Float8(-0.0),
            Datum::Numeric(crabka_pgtypes::numeric::parse("1.50").expect("n")),
            Datum::Numeric(crabka_pgtypes::numeric::parse("-9999999999999999999.0001").expect("n")),
        ];
        let bytes = encode_row(&row);
        assert_eq!(decode_row(&bytes).expect("decode"), row);
    }

    #[test]
    fn jsonb_and_array_tags_round_trip() {
        use assert2::assert;
        use crabka_pgtypes::{ArrayValue, ElemType};

        let json = |s: &str| Datum::Jsonb(crabka_pgtypes::jsonb::parse(s).expect("jsonb"));
        let row = vec![
            json(r#"{"b":1,"a":[1,2],"c":"x"}"#),
            json("null"),
            // Empty, NULL elements, and a jsonb inside an array.
            Datum::Array(ArrayValue::new(ElemType::Int4, vec![])),
            Datum::Array(ArrayValue::new(
                ElemType::Int4,
                vec![Datum::Int4(1), Datum::Null, Datum::Int4(3)],
            )),
            Datum::Array(ArrayValue::new(
                ElemType::Text,
                vec![Datum::Text("a,b".into()), Datum::Text(String::new())],
            )),
            Datum::Array(ArrayValue::new(
                ElemType::Jsonb,
                vec![json(r#"{"z":1}"#), Datum::Null],
            )),
            Datum::Array(ArrayValue::new(
                ElemType::Numeric,
                vec![Datum::Numeric(
                    crabka_pgtypes::numeric::parse("1.50").expect("n"),
                )],
            )),
            // A scalar after the arrays pins that element counts are honoured.
            Datum::Int4(9),
        ];
        assert!(decode_row(&encode_row(&row)).expect("decode") == row);
    }

    #[test]
    fn jsonb_tag_layout_is_a_length_prefixed_canonical_text() {
        use assert2::assert;

        let value = Datum::Jsonb(crabka_pgtypes::jsonb::parse(r#"{"b":1,"a":2}"#).expect("jsonb"));
        let text = br#"{"a": 2, "b": 1}"#;
        let mut expected = vec![ROW_VERSION, tag::JSONB];
        expected.extend_from_slice(&u32::try_from(text.len()).expect("small").to_be_bytes());
        expected.extend_from_slice(text);
        assert!(encode_row(&[value]) == expected);
    }

    #[test]
    fn corrupt_jsonb_and_array_payloads_error_not_panic() {
        use assert2::assert;

        // An unknown element type code.
        let mut bytes = vec![ROW_VERSION, tag::ARRAY, 200];
        bytes.extend_from_slice(&0u32.to_be_bytes());
        assert!(decode_row(&bytes).is_err());
        // Fewer elements than the declared count.
        let mut bytes = vec![ROW_VERSION, tag::ARRAY, 1];
        bytes.extend_from_slice(&3u32.to_be_bytes());
        assert!(decode_row(&bytes).is_err());
        // Text that is not valid JSON.
        let mut bytes = vec![ROW_VERSION, tag::JSONB];
        bytes.extend_from_slice(&2u32.to_be_bytes());
        bytes.extend_from_slice(b"{{");
        assert!(decode_row(&bytes).is_err());
    }

    #[test]
    fn version_byte_is_present() {
        assert_eq!(encode_row(&[Datum::Int4(1)])[0], ROW_VERSION);
    }

    #[test]
    fn truncated_value_errors_not_panics() {
        assert!(decode_row(&[ROW_VERSION, 2, 0, 0]).is_err());
    }

    #[test]
    fn invalid_bool_payload_errors() {
        let err = decode_row(&[ROW_VERSION, tag::BOOL, 2]).expect_err("invalid bool payload");
        assert_eq!(err, KvError::CorruptRow("invalid bool payload 2".into()));
    }

    #[test]
    fn numeric_with_an_overflowing_exponent_errors_not_ooms() {
        let text = b"8e88888888";
        let mut bytes = vec![ROW_VERSION, 6];
        bytes.extend_from_slice(&u32::try_from(text.len()).expect("small text").to_be_bytes());
        bytes.extend_from_slice(text);
        assert!(decode_row(&bytes).is_err());
    }

    #[test]
    fn adversarial_datetime_values_error_not_panic() {
        for (tag, width) in [(7u8, 4usize), (8, 8), (9, 8), (10, 8), (11, 16)] {
            for fill in [0xFFu8, 0x7F, 0x80, 0x00] {
                let mut bytes = vec![ROW_VERSION, tag];
                bytes.extend(vec![fill; width]);
                let _ = decode_row(&bytes);
            }
        }
        let mut tstz_max = vec![ROW_VERSION, 10];
        tstz_max.extend_from_slice(&i64::MAX.to_be_bytes());
        assert!(decode_row(&tstz_max).is_err());
    }

    #[test]
    fn unknown_version_errors() {
        assert!(decode_row(&[99, 1, 1]).is_err());
    }

    fn arb_datum() -> impl Strategy<Value = Datum> {
        prop_oneof![
            Just(Datum::Null),
            any::<bool>().prop_map(Datum::Bool),
            any::<i32>().prop_map(Datum::Int4),
            any::<i64>().prop_map(Datum::Int8),
            ".*".prop_map(Datum::Text),
            any::<f64>().prop_map(Datum::Float8),
            (any::<i64>(), 0u32..6).prop_map(|(m, s)| {
                Datum::Numeric(
                    crabka_pgtypes::numeric::parse(&format!("{m}e-{s}")).expect("numeric"),
                )
            }),
        ]
    }

    proptest! {
        #[test]
        fn roundtrip_arbitrary_rows(row in prop::collection::vec(arb_datum(), 0..8)) {
            let bytes = encode_row(&row);
            prop_assert_eq!(decode_row(&bytes).expect("decode"), row);
        }
    }

    #[test]
    fn datetime_row_round_trip() {
        use crabka_pgtypes::datetime::Interval;

        let row = vec![
            Datum::Date(crabka_pgtypes::datetime::parse_date("2024-01-15").expect("d")),
            Datum::Time(crabka_pgtypes::datetime::parse_time("13:45:06.5").expect("t")),
            Datum::Timestamp(
                crabka_pgtypes::datetime::parse_timestamp("2024-01-15 13:45:06").expect("ts"),
            ),
            Datum::Timestamptz(
                crabka_pgtypes::datetime::parse_timestamptz(
                    "2024-01-15 13:45:06+00",
                    &jiff::tz::TimeZone::UTC,
                )
                .expect("tstz"),
            ),
            Datum::Interval(Interval {
                months: 14,
                days: -3,
                micros: 4_500_000,
            }),
        ];
        assert_eq!(decode_row(&encode_row(&row)).expect("decode"), row);
    }
}
