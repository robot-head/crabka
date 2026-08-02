use std::cmp::Ordering;

use crate::{ColumnType, Datum, TypeError};

pub fn canonicalize(
    input: &str,
    subtype: ColumnType,
    tz: &jiff::tz::TimeZone,
) -> Result<String, TypeError> {
    let value = input.trim();
    if value.eq_ignore_ascii_case("empty") {
        return Ok("empty".into());
    }
    let bytes = value.as_bytes();
    if bytes.len() < 2
        || !matches!(bytes[0], b'[' | b'(')
        || !matches!(bytes.last(), Some(b']' | b')'))
    {
        return Err(malformed(input));
    }
    let inner = &value[1..value.len() - 1];
    let comma = separator(inner, input)?;
    let mut lower = bound(&inner[..comma], subtype, tz, input)?;
    let mut upper = bound(&inner[comma + 1..], subtype, tz, input)?;
    let mut lower_inclusive = lower.is_some() && bytes[0] == b'[';
    let mut upper_inclusive = upper.is_some() && bytes[bytes.len() - 1] == b']';
    if is_discrete(subtype) {
        if lower.is_some() && !lower_inclusive {
            lower = lower.map(increment).transpose()?;
            lower_inclusive = true;
        }
        if upper.is_some() && upper_inclusive {
            upper = upper.map(increment).transpose()?;
            upper_inclusive = false;
        }
    }
    if let (Some(lower), Some(upper)) = (&lower, &upper) {
        match crate::ops::compare(lower, upper)? {
            Some(Ordering::Greater) => {
                return Err(TypeError::Coded {
                    sqlstate: "22000",
                    message: "range lower bound must be less than or equal to range upper bound"
                        .into(),
                });
            }
            Some(Ordering::Equal) if !lower_inclusive || !upper_inclusive => {
                return Ok("empty".into());
            }
            _ => {}
        }
    }
    let left = if lower_inclusive { '[' } else { '(' };
    let right = if upper_inclusive { ']' } else { ')' };
    let lower = lower.as_ref().map_or_else(String::new, |v| render(v, tz));
    let upper = upper.as_ref().map_or_else(String::new, |v| render(v, tz));
    Ok(format!("{left}{lower},{upper}{right}"))
}

fn is_discrete(subtype: ColumnType) -> bool {
    matches!(
        subtype,
        ColumnType::Int4 | ColumnType::Int8 | ColumnType::Date
    )
}

fn increment(value: Datum) -> Result<Datum, TypeError> {
    match value {
        Datum::Int4(value) => value
            .checked_add(1)
            .map(Datum::Int4)
            .ok_or_else(|| TypeError::out_of_range_for("integer")),
        Datum::Int8(value) => value
            .checked_add(1)
            .map(Datum::Int8)
            .ok_or_else(|| TypeError::out_of_range_for("bigint")),
        Datum::Date(value) => value
            .tomorrow()
            .map(Datum::Date)
            .map_err(|_| TypeError::Coded {
                sqlstate: "22008",
                message: "date out of range".into(),
            }),
        _ => Ok(value),
    }
}

fn separator(inner: &str, whole: &str) -> Result<usize, TypeError> {
    let mut quoted = false;
    let mut escaped = false;
    let mut comma = None;
    for (index, byte) in inner.bytes().enumerate() {
        if escaped {
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            quoted = !quoted;
        } else if byte == b',' && !quoted {
            if comma.replace(index).is_some() {
                return Err(malformed(whole));
            }
        }
    }
    if quoted || escaped {
        return Err(malformed(whole));
    }
    comma.ok_or_else(|| malformed(whole))
}

fn bound(
    raw: &str,
    subtype: ColumnType,
    tz: &jiff::tz::TimeZone,
    whole: &str,
) -> Result<Option<Datum>, TypeError> {
    if raw.is_empty() {
        return Ok(None);
    }
    let mut decoded = String::with_capacity(raw.len());
    let mut quoted = false;
    let mut escaped = false;
    for byte in raw.bytes() {
        if escaped {
            decoded.push(char::from(byte));
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            quoted = !quoted;
        } else if !quoted && matches!(byte, b')' | b']') {
            return Err(malformed(whole));
        } else {
            decoded.push(char::from(byte));
        }
    }
    if quoted || escaped {
        return Err(malformed(whole));
    }
    crate::cast::cast(&Datum::Text(decoded), subtype, tz).map(Some)
}

fn render(value: &Datum, tz: &jiff::tz::TimeZone) -> String {
    let text = String::from_utf8_lossy(&crate::encoding::encode_text(value, tz)).into_owned();
    if text.is_empty()
        || text.bytes().any(|b| {
            b.is_ascii_whitespace() || matches!(b, b'"' | b'\\' | b',' | b'(' | b')' | b'[' | b']')
        })
    {
        format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        text
    }
}

fn malformed(value: &str) -> TypeError {
    TypeError::Coded {
        sqlstate: "22P02",
        message: format!("malformed range literal: \"{value}\""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_ranges_validate_and_canonicalize() {
        let tz = jiff::tz::TimeZone::UTC;
        assert_eq!(
            canonicalize("  empty  ", ColumnType::Text, &tz),
            Ok("empty".into())
        );
        assert_eq!(
            canonicalize("[,z]", ColumnType::Text, &tz),
            Ok("(,z]".into())
        );
        assert_eq!(
            canonicalize("[a,]", ColumnType::Text, &tz),
            Ok("[a,)".into())
        );
        assert_eq!(
            canonicalize("[a,a)", ColumnType::Text, &tz),
            Ok("empty".into())
        );
        assert!(canonicalize("[z,a]", ColumnType::Text, &tz).is_err());
        assert!(canonicalize("(,,a)", ColumnType::Text, &tz).is_err());
        assert!(canonicalize("(),a)", ColumnType::Text, &tz).is_err());
        assert!(canonicalize("(a,])", ColumnType::Text, &tz).is_err());
        assert_eq!(
            canonicalize("(\"]\",a)", ColumnType::Text, &tz),
            Ok("(\"]\",a)".into())
        );
        assert_eq!(
            canonicalize("((,z)", ColumnType::Text, &tz),
            Ok("(\"(\",z)".into())
        );
    }

    #[test]
    fn discrete_ranges_use_inclusive_exclusive_canonical_form() {
        let tz = jiff::tz::TimeZone::UTC;
        assert_eq!(
            canonicalize("(1,4]", ColumnType::Int4, &tz),
            Ok("[2,5)".into())
        );
        assert_eq!(
            canonicalize("(1,2)", ColumnType::Int4, &tz),
            Ok("empty".into())
        );
        assert_eq!(
            canonicalize("[1,2147483647]", ColumnType::Int4, &tz)
                .expect_err("inclusive maximum cannot be canonicalized")
                .sqlstate(),
            "22003"
        );
    }
}
