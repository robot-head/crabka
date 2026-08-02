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
    let lower = bound(&inner[..comma], subtype, tz, input)?;
    let upper = bound(&inner[comma + 1..], subtype, tz, input)?;
    if let (Some(lower), Some(upper)) = (&lower, &upper) {
        match crate::ops::compare(lower, upper)? {
            Some(Ordering::Greater) => {
                return Err(TypeError::Coded {
                    sqlstate: "22000",
                    message: "range lower bound must be less than or equal to range upper bound"
                        .into(),
                });
            }
            Some(Ordering::Equal) if bytes[0] == b'(' || bytes[bytes.len() - 1] == b')' => {
                return Ok("empty".into());
            }
            _ => {}
        }
    }
    let left = if lower.is_some() && bytes[0] == b'[' {
        '['
    } else {
        '('
    };
    let right = if upper.is_some() && bytes[bytes.len() - 1] == b']' {
        ']'
    } else {
        ')'
    };
    let lower = lower.as_ref().map_or_else(String::new, |v| render(v, tz));
    let upper = upper.as_ref().map_or_else(String::new, |v| render(v, tz));
    Ok(format!("{left}{lower},{upper}{right}"))
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
}
