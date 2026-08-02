use std::cmp::Ordering;

use crate::{ColumnType, Datum, RangeValue, TypeError, usertype::RangeRef};

pub fn parse(input: &str, ty: RangeRef, tz: &jiff::tz::TimeZone) -> Result<RangeValue, TypeError> {
    let canonical = canonicalize(input, *ty.subtype, tz)?;
    if canonical == "empty" {
        return Ok(RangeValue {
            ty,
            lower: None,
            upper: None,
            lower_inclusive: false,
            upper_inclusive: false,
            empty: true,
        });
    }
    let bytes = canonical.as_bytes();
    let inner = &canonical[1..canonical.len() - 1];
    let comma = separator(inner, &canonical)?;
    Ok(RangeValue {
        ty,
        lower: bound(&inner[..comma], *ty.subtype, tz, &canonical)?.map(Box::new),
        upper: bound(&inner[comma + 1..], *ty.subtype, tz, &canonical)?.map(Box::new),
        lower_inclusive: bytes[0] == b'[',
        upper_inclusive: bytes[bytes.len() - 1] == b']',
        empty: false,
    })
}

pub fn to_text(range: &RangeValue, mut encode: impl FnMut(&Datum) -> String) -> String {
    if range.empty {
        return "empty".into();
    }
    let lower = range
        .lower
        .as_deref()
        .map(|value| quote_bound(&encode(value)))
        .unwrap_or_default();
    let upper = range
        .upper
        .as_deref()
        .map(|value| quote_bound(&encode(value)))
        .unwrap_or_default();
    format!(
        "{}{},{}{}",
        if range.lower_inclusive { '[' } else { '(' },
        lower,
        upper,
        if range.upper_inclusive { ']' } else { ')' }
    )
}

pub fn contains_range(outer: &RangeValue, inner: &RangeValue) -> Result<bool, TypeError> {
    if outer.ty != inner.ty {
        return Err(TypeError::TypeMismatch {
            message: "range types do not match".into(),
        });
    }
    if inner.empty {
        return Ok(true);
    }
    if outer.empty {
        return Ok(false);
    }
    Ok(lower_contains(outer, inner)? && upper_contains(outer, inner)?)
}

pub fn contains_element(range: &RangeValue, value: &Datum) -> Result<bool, TypeError> {
    if range.empty {
        return Ok(false);
    }
    let above_lower = match range.lower.as_deref() {
        None => true,
        Some(lower) => match crate::ops::compare(value, lower)?.expect("range bound is non-null") {
            Ordering::Greater => true,
            Ordering::Equal => range.lower_inclusive,
            Ordering::Less => false,
        },
    };
    let below_upper = match range.upper.as_deref() {
        None => true,
        Some(upper) => match crate::ops::compare(value, upper)?.expect("range bound is non-null") {
            Ordering::Less => true,
            Ordering::Equal => range.upper_inclusive,
            Ordering::Greater => false,
        },
    };
    Ok(above_lower && below_upper)
}

pub fn overlaps(a: &RangeValue, b: &RangeValue) -> Result<bool, TypeError> {
    if a.ty != b.ty {
        return Err(TypeError::TypeMismatch {
            message: "range types do not match".into(),
        });
    }
    if a.empty || b.empty {
        return Ok(false);
    }
    Ok(!strictly_left(a, b)? && !strictly_left(b, a)?)
}

pub fn strictly_left(a: &RangeValue, b: &RangeValue) -> Result<bool, TypeError> {
    same_type(a, b)?;
    if a.empty || b.empty {
        return Ok(false);
    }
    left_of(a, b)
}

pub fn strictly_right(a: &RangeValue, b: &RangeValue) -> Result<bool, TypeError> {
    strictly_left(b, a)
}

pub fn does_not_extend_right(a: &RangeValue, b: &RangeValue) -> Result<bool, TypeError> {
    same_type(a, b)?;
    if a.empty || b.empty {
        return Ok(false);
    }
    Ok(compare_upper_bound(a, b)? != Ordering::Greater)
}

pub fn does_not_extend_left(a: &RangeValue, b: &RangeValue) -> Result<bool, TypeError> {
    same_type(a, b)?;
    if a.empty || b.empty {
        return Ok(false);
    }
    Ok(compare_lower_bound(a, b)? != Ordering::Less)
}

pub fn adjacent(a: &RangeValue, b: &RangeValue) -> Result<bool, TypeError> {
    same_type(a, b)?;
    if a.empty || b.empty {
        return Ok(false);
    }
    Ok(adjacent_on(a, b)? || adjacent_on(b, a)?)
}

pub fn merge(a: &RangeValue, b: &RangeValue) -> Result<RangeValue, TypeError> {
    same_type(a, b)?;
    if a.empty {
        return Ok(b.clone());
    }
    if b.empty {
        return Ok(a.clone());
    }
    let lower_order = compare_lower(a, b)?;
    let upper_order = compare_upper(a, b)?;
    Ok(RangeValue {
        ty: a.ty.clone(),
        lower: if lower_order == Ordering::Greater {
            b.lower.clone()
        } else {
            a.lower.clone()
        },
        upper: if upper_order == Ordering::Less {
            b.upper.clone()
        } else {
            a.upper.clone()
        },
        lower_inclusive: match lower_order {
            Ordering::Less => a.lower_inclusive,
            Ordering::Greater => b.lower_inclusive,
            Ordering::Equal => a.lower_inclusive || b.lower_inclusive,
        },
        upper_inclusive: match upper_order {
            Ordering::Greater => a.upper_inclusive,
            Ordering::Less => b.upper_inclusive,
            Ordering::Equal => a.upper_inclusive || b.upper_inclusive,
        },
        empty: false,
    })
}

pub fn union(a: &RangeValue, b: &RangeValue) -> Result<RangeValue, TypeError> {
    if !a.empty && !b.empty && !overlaps(a, b)? && !adjacent(a, b)? {
        return Err(TypeError::Coded {
            sqlstate: "22000",
            message: "result of range union would not be contiguous".into(),
        });
    }
    merge(a, b)
}

pub fn intersection(a: &RangeValue, b: &RangeValue) -> Result<RangeValue, TypeError> {
    same_type(a, b)?;
    if a.empty || b.empty || !overlaps(a, b)? {
        return Ok(empty(a));
    }
    let lower_order = compare_lower(a, b)?;
    let upper_order = compare_upper(a, b)?;
    Ok(RangeValue {
        ty: a.ty.clone(),
        lower: if lower_order == Ordering::Less {
            b.lower.clone()
        } else {
            a.lower.clone()
        },
        upper: if upper_order == Ordering::Greater {
            b.upper.clone()
        } else {
            a.upper.clone()
        },
        lower_inclusive: match lower_order {
            Ordering::Less => b.lower_inclusive,
            Ordering::Greater => a.lower_inclusive,
            Ordering::Equal => a.lower_inclusive && b.lower_inclusive,
        },
        upper_inclusive: match upper_order {
            Ordering::Greater => b.upper_inclusive,
            Ordering::Less => a.upper_inclusive,
            Ordering::Equal => a.upper_inclusive && b.upper_inclusive,
        },
        empty: false,
    })
}

pub fn difference(a: &RangeValue, b: &RangeValue) -> Result<RangeValue, TypeError> {
    same_type(a, b)?;
    if a.empty || b.empty || !overlaps(a, b)? {
        return Ok(a.clone());
    }
    if contains_range(b, a)? {
        return Ok(empty(a));
    }
    let cuts_left = compare_lower_bound(b, a)? != Ordering::Greater;
    let cuts_right = compare_upper_bound(b, a)? != Ordering::Less;
    if !cuts_left && !cuts_right {
        return Err(TypeError::Coded {
            sqlstate: "22000",
            message: "result of range difference would not be contiguous".into(),
        });
    }
    let mut result = a.clone();
    if cuts_left {
        result.lower = b.upper.clone();
        result.lower_inclusive = !b.upper_inclusive;
    } else {
        result.upper = b.lower.clone();
        result.upper_inclusive = !b.lower_inclusive;
    }
    Ok(result)
}

fn lower_contains(outer: &RangeValue, inner: &RangeValue) -> Result<bool, TypeError> {
    match (outer.lower.as_deref(), inner.lower.as_deref()) {
        (None, _) => Ok(true),
        (Some(_), None) => Ok(false),
        (Some(a), Some(b)) => Ok(
            match crate::ops::compare(a, b)?.expect("bounds are non-null") {
                Ordering::Less => true,
                Ordering::Greater => false,
                Ordering::Equal => outer.lower_inclusive || !inner.lower_inclusive,
            },
        ),
    }
}

fn upper_contains(outer: &RangeValue, inner: &RangeValue) -> Result<bool, TypeError> {
    match (outer.upper.as_deref(), inner.upper.as_deref()) {
        (None, _) => Ok(true),
        (Some(_), None) => Ok(false),
        (Some(a), Some(b)) => Ok(
            match crate::ops::compare(a, b)?.expect("bounds are non-null") {
                Ordering::Greater => true,
                Ordering::Less => false,
                Ordering::Equal => outer.upper_inclusive || !inner.upper_inclusive,
            },
        ),
    }
}

fn left_of(a: &RangeValue, b: &RangeValue) -> Result<bool, TypeError> {
    let (Some(upper), Some(lower)) = (a.upper.as_deref(), b.lower.as_deref()) else {
        return Ok(false);
    };
    Ok(
        match crate::ops::compare(upper, lower)?.expect("bounds are non-null") {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => !(a.upper_inclusive && b.lower_inclusive),
        },
    )
}

fn same_type(a: &RangeValue, b: &RangeValue) -> Result<(), TypeError> {
    if a.ty == b.ty {
        Ok(())
    } else {
        Err(TypeError::TypeMismatch {
            message: "range types do not match".into(),
        })
    }
}

fn adjacent_on(a: &RangeValue, b: &RangeValue) -> Result<bool, TypeError> {
    let (Some(upper), Some(lower)) = (a.upper.as_deref(), b.lower.as_deref()) else {
        return Ok(false);
    };
    Ok(crate::ops::compare(upper, lower)? == Some(Ordering::Equal)
        && a.upper_inclusive != b.lower_inclusive)
}

fn compare_lower(a: &RangeValue, b: &RangeValue) -> Result<Ordering, TypeError> {
    match (a.lower.as_deref(), b.lower.as_deref()) {
        (None, None) => Ok(Ordering::Equal),
        (None, Some(_)) => Ok(Ordering::Less),
        (Some(_), None) => Ok(Ordering::Greater),
        (Some(a), Some(b)) => Ok(crate::ops::compare(a, b)?.expect("bounds are non-null")),
    }
}

fn compare_upper(a: &RangeValue, b: &RangeValue) -> Result<Ordering, TypeError> {
    match (a.upper.as_deref(), b.upper.as_deref()) {
        (None, None) => Ok(Ordering::Equal),
        (None, Some(_)) => Ok(Ordering::Greater),
        (Some(_), None) => Ok(Ordering::Less),
        (Some(a), Some(b)) => Ok(crate::ops::compare(a, b)?.expect("bounds are non-null")),
    }
}

fn compare_lower_bound(a: &RangeValue, b: &RangeValue) -> Result<Ordering, TypeError> {
    let order = compare_lower(a, b)?;
    if order != Ordering::Equal || a.lower.is_none() {
        return Ok(order);
    }
    Ok(match (a.lower_inclusive, b.lower_inclusive) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => Ordering::Equal,
    })
}

fn compare_upper_bound(a: &RangeValue, b: &RangeValue) -> Result<Ordering, TypeError> {
    let order = compare_upper(a, b)?;
    if order != Ordering::Equal || a.upper.is_none() {
        return Ok(order);
    }
    Ok(match (a.upper_inclusive, b.upper_inclusive) {
        (false, true) => Ordering::Less,
        (true, false) => Ordering::Greater,
        _ => Ordering::Equal,
    })
}

fn empty(range: &RangeValue) -> RangeValue {
    RangeValue {
        ty: range.ty.clone(),
        lower: None,
        upper: None,
        lower_inclusive: false,
        upper_inclusive: false,
        empty: true,
    }
}

fn quote_bound(text: &str) -> String {
    if text.is_empty()
        || text.bytes().any(|b| {
            b.is_ascii_whitespace() || matches!(b, b'"' | b'\\' | b',' | b'(' | b')' | b'[' | b']')
        })
    {
        format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        text.to_string()
    }
}

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
    quote_bound(&text)
}

fn malformed(value: &str) -> TypeError {
    TypeError::RangeMalformed {
        value: value.into(),
        detail: malformed_detail(value),
    }
}

fn malformed_detail(value: &str) -> &'static str {
    let value = value.trim();
    let Some(first) = value.as_bytes().first() else {
        return "Missing left parenthesis or bracket.";
    };
    if !matches!(first, b'[' | b'(') {
        return "Missing left parenthesis or bracket.";
    }
    let Some(last) = value.as_bytes().last() else {
        return "Unexpected end of input.";
    };
    if !matches!(last, b']' | b')') {
        return if value.bytes().any(|byte| matches!(byte, b']' | b')')) {
            "Junk after right parenthesis or bracket."
        } else {
            "Unexpected end of input."
        };
    }

    let mut quoted = false;
    let mut escaped = false;
    let mut commas = 0;
    for byte in value[1..value.len() - 1].bytes() {
        if escaped {
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            quoted = !quoted;
        } else if !quoted && byte == b',' {
            commas += 1;
        } else if !quoted && matches!(byte, b']' | b')') {
            return if commas == 0 {
                "Missing comma after lower bound."
            } else {
                "Junk after right parenthesis or bracket."
            };
        }
    }
    if quoted || escaped {
        "Unexpected end of input."
    } else if commas > 1 {
        "Too many commas."
    } else {
        "Missing comma after lower bound."
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
    fn malformed_ranges_report_postgres_details() {
        for (input, detail) in [
            ("", "Missing left parenthesis or bracket."),
            ("-[a,z)", "Missing left parenthesis or bracket."),
            ("[a,z) - ", "Junk after right parenthesis or bracket."),
            ("(\",a)", "Unexpected end of input."),
            ("(,,a)", "Too many commas."),
            ("(),a)", "Missing comma after lower bound."),
            ("(a,))", "Junk after right parenthesis or bracket."),
            ("(],a)", "Missing comma after lower bound."),
            ("(a,])", "Junk after right parenthesis or bracket."),
        ] {
            let error = canonicalize(input, ColumnType::Text, &jiff::tz::TimeZone::UTC)
                .expect_err("malformed range");
            assert_eq!(error.detail(), Some(detail), "{input}");
        }
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
