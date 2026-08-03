use crate::{Datum, MultirangeValue, RangeValue, TypeError, usertype::MultirangeRef};

/// Parses and canonicalizes a multirange literal.
///
/// # Errors
///
/// Returns an error if the literal or one of its component ranges is malformed,
/// or if the component bounds cannot be normalized.
pub fn parse(
    input: &str,
    ty: MultirangeRef,
    tz: &jiff::tz::TimeZone,
) -> Result<MultirangeValue, TypeError> {
    let text = input.trim();
    let Some(inner) = text.strip_prefix('{').and_then(|s| s.strip_suffix('}')) else {
        return Err(malformed(input));
    };
    let mut ranges = Vec::new();
    for part in parts(inner).ok_or_else(|| malformed(input))? {
        let range = crate::range::parse(part, ty.range, tz)?;
        if !range.empty {
            ranges.push(range);
        }
    }
    normalize(ty, ranges)
}

/// Builds and canonicalizes a multirange from component ranges.
///
/// # Errors
///
/// Returns an error if a component has the wrong range type or its bounds
/// cannot be normalized.
pub fn from_ranges(
    ty: MultirangeRef,
    ranges: Vec<RangeValue>,
) -> Result<MultirangeValue, TypeError> {
    if ranges.iter().any(|range| range.ty != ty.range) {
        return Err(TypeError::TypeMismatch {
            message: "multirange component type does not match".into(),
        });
    }
    normalize(ty, ranges)
}

fn normalize(ty: MultirangeRef, mut ranges: Vec<RangeValue>) -> Result<MultirangeValue, TypeError> {
    ranges.retain(|range| !range.empty);
    ranges.sort_by(|a, b| {
        crate::ops::compare(&Datum::Range(a.clone()), &Datum::Range(b.clone()))
            .expect("same-type ranges are comparable")
            .expect("range comparison is never NULL")
    });
    let mut canonical: Vec<RangeValue> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(last) = canonical.last_mut()
            && (crate::range::overlaps(last, &range)? || crate::range::adjacent(last, &range)?)
        {
            *last = crate::range::merge(last, &range)?;
            continue;
        }
        canonical.push(range);
    }
    Ok(MultirangeValue {
        ty,
        ranges: canonical,
    })
}

pub fn to_text(value: &MultirangeValue, mut encode: impl FnMut(&Datum) -> String) -> String {
    format!(
        "{{{}}}",
        value
            .ranges
            .iter()
            .map(|range| crate::range::to_text(range, &mut encode))
            .collect::<Vec<_>>()
            .join(",")
    )
}

/// Tests whether a multirange contains a range.
///
/// # Errors
///
/// Returns an error if the range and multirange types differ or their bounds
/// cannot be compared.
pub fn contains_range(multirange: &MultirangeValue, range: &RangeValue) -> Result<bool, TypeError> {
    same_type(multirange, range)?;
    if range.empty {
        return Ok(true);
    }
    for component in &multirange.ranges {
        if crate::range::contains_range(component, range)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Tests whether a multirange contains an element.
///
/// # Errors
///
/// Returns an error if the element cannot be compared with the component range
/// subtype.
pub fn contains_element(multirange: &MultirangeValue, element: &Datum) -> Result<bool, TypeError> {
    for component in &multirange.ranges {
        if crate::range::contains_element(component, element)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Tests whether a range contains every component of a multirange.
///
/// # Errors
///
/// Returns an error if the range and multirange types differ or their bounds
/// cannot be compared.
pub fn range_contains(range: &RangeValue, multirange: &MultirangeValue) -> Result<bool, TypeError> {
    same_type(multirange, range)?;
    for component in &multirange.ranges {
        if !crate::range::contains_range(range, component)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Tests whether one multirange contains another.
///
/// # Errors
///
/// Returns an error if the multirange types differ or their bounds cannot be
/// compared.
pub fn contains(outer: &MultirangeValue, inner: &MultirangeValue) -> Result<bool, TypeError> {
    if outer.ty != inner.ty {
        return Err(TypeError::TypeMismatch {
            message: "multirange types do not match".into(),
        });
    }
    for range in &inner.ranges {
        if !contains_range(outer, range)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Tests whether two multiranges overlap.
///
/// # Errors
///
/// Returns an error if the multirange types differ or their bounds cannot be
/// compared.
pub fn overlaps(a: &MultirangeValue, b: &MultirangeValue) -> Result<bool, TypeError> {
    if a.ty != b.ty {
        return Err(TypeError::TypeMismatch {
            message: "multirange types do not match".into(),
        });
    }
    for range in &b.ranges {
        if overlaps_range(a, range)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Tests whether a multirange overlaps a range.
///
/// # Errors
///
/// Returns an error if the range and multirange types differ or their bounds
/// cannot be compared.
pub fn overlaps_range(multirange: &MultirangeValue, range: &RangeValue) -> Result<bool, TypeError> {
    same_type(multirange, range)?;
    for component in &multirange.ranges {
        if crate::range::overlaps(component, range)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Returns the union of two multiranges.
///
/// # Errors
///
/// Returns an error if the multirange types differ or their component ranges
/// cannot be normalized.
pub fn union(a: &MultirangeValue, b: &MultirangeValue) -> Result<MultirangeValue, TypeError> {
    same_multirange_type(a, b)?;
    from_ranges(a.ty, a.ranges.iter().chain(&b.ranges).cloned().collect())
}

/// Returns the intersection of two multiranges.
///
/// # Errors
///
/// Returns an error if the multirange types differ or their component bounds
/// cannot be compared or normalized.
pub fn intersection(
    a: &MultirangeValue,
    b: &MultirangeValue,
) -> Result<MultirangeValue, TypeError> {
    same_multirange_type(a, b)?;
    let mut ranges = Vec::new();
    for left in &a.ranges {
        for right in &b.ranges {
            let overlap = crate::range::intersection(left, right)?;
            if !overlap.empty {
                ranges.push(overlap);
            }
        }
    }
    from_ranges(a.ty, ranges)
}

/// Subtracts one multirange from another.
///
/// # Errors
///
/// Returns an error if the multirange types differ or their component bounds
/// cannot be compared or normalized.
pub fn difference(a: &MultirangeValue, b: &MultirangeValue) -> Result<MultirangeValue, TypeError> {
    same_multirange_type(a, b)?;
    let mut ranges = a.ranges.clone();
    for cut in &b.ranges {
        let mut next = Vec::new();
        for source in ranges {
            let overlap = crate::range::intersection(&source, cut)?;
            if overlap.empty {
                next.push(source);
                continue;
            }
            if overlap.lower.is_some() {
                let left_mask = RangeValue {
                    ty: source.ty,
                    lower: None,
                    upper: overlap.lower.clone(),
                    lower_inclusive: false,
                    upper_inclusive: !overlap.lower_inclusive,
                    empty: false,
                };
                let left = crate::range::intersection(&source, &left_mask)?;
                if !left.empty {
                    next.push(left);
                }
            }
            if overlap.upper.is_some() {
                let right_mask = RangeValue {
                    ty: source.ty,
                    lower: overlap.upper.clone(),
                    upper: None,
                    lower_inclusive: !overlap.upper_inclusive,
                    upper_inclusive: false,
                    empty: false,
                };
                let right = crate::range::intersection(&source, &right_mask)?;
                if !right.empty {
                    next.push(right);
                }
            }
        }
        ranges = next;
    }
    from_ranges(a.ty, ranges)
}

/// Applies a range relation to the first or last multirange component.
///
/// # Errors
///
/// Returns an error if the range and multirange types differ or `relation`
/// returns an error.
pub fn range_relation(
    range: &RangeValue,
    multirange: &MultirangeValue,
    relation: fn(&RangeValue, &RangeValue) -> Result<bool, TypeError>,
    use_last: bool,
) -> Result<bool, TypeError> {
    same_type(multirange, range)?;
    let component = if use_last {
        multirange.ranges.last()
    } else {
        multirange.ranges.first()
    };
    component.map_or(Ok(false), |component| relation(range, component))
}

/// Tests whether a range is adjacent to the outside edge of a multirange.
///
/// # Errors
///
/// Returns an error if the range and multirange types differ or their bounds
/// cannot be compared.
pub fn adjacent_range(multirange: &MultirangeValue, range: &RangeValue) -> Result<bool, TypeError> {
    same_type(multirange, range)?;
    let before = match multirange.ranges.first() {
        Some(first) => {
            crate::range::strictly_left(range, first)? && crate::range::adjacent(range, first)?
        }
        None => false,
    };
    let after = match multirange.ranges.last() {
        Some(last) => {
            crate::range::strictly_right(range, last)? && crate::range::adjacent(range, last)?
        }
        None => false,
    };
    Ok(before || after)
}

fn same_type(multirange: &MultirangeValue, range: &RangeValue) -> Result<(), TypeError> {
    if multirange.ty.range == range.ty {
        Ok(())
    } else {
        Err(TypeError::TypeMismatch {
            message: "range and multirange types do not match".into(),
        })
    }
}

fn same_multirange_type(a: &MultirangeValue, b: &MultirangeValue) -> Result<(), TypeError> {
    if a.ty == b.ty {
        Ok(())
    } else {
        Err(TypeError::TypeMismatch {
            message: "multirange types do not match".into(),
        })
    }
}

fn parts(input: &str) -> Option<Vec<&str>> {
    if input.trim().is_empty() {
        return Some(Vec::new());
    }
    let mut out = Vec::new();
    let mut start = 0;
    let mut quote = false;
    let mut escape = false;
    let mut depth = 0_u8;
    for (index, byte) in input.bytes().enumerate() {
        if escape {
            escape = false;
        } else if byte == b'\\' {
            escape = true;
        } else if byte == b'"' {
            quote = !quote;
        } else if !quote && matches!(byte, b'[' | b'(') {
            depth = depth.checked_add(1)?;
        } else if !quote && matches!(byte, b']' | b')') {
            depth = depth.checked_sub(1)?;
        } else if !quote && depth == 0 && byte == b',' {
            out.push(input[start..index].trim());
            start = index + 1;
        }
    }
    if quote || escape || depth != 0 {
        return None;
    }
    out.push(input[start..].trim());
    Some(out)
}

fn malformed(input: &str) -> TypeError {
    TypeError::Coded {
        sqlstate: "22P02",
        message: format!("malformed multirange literal: \"{input}\""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnType;

    #[test]
    fn input_sorts_merges_and_drops_empty_ranges() {
        let ColumnType::Multirange(ty) =
            ColumnType::builtin_multirange(crate::oids::INT4MULTIRANGE).expect("int4multirange")
        else {
            unreachable!()
        };
        let value = parse("{[5,8),empty,[1,3),[3,5)}", ty, &jiff::tz::TimeZone::UTC)
            .expect("multirange input");
        assert_eq!(
            to_text(&value, |bound| match bound {
                Datum::Int4(value) => value.to_string(),
                _ => unreachable!(),
            }),
            "{[1,8)}"
        );
        let split =
            parse("{[10,20),[30,40)}", ty, &jiff::tz::TimeZone::UTC).expect("split multirange");
        let internal = crate::range::parse("[20,25)", ty.range, &jiff::tz::TimeZone::UTC)
            .expect("internal range");
        let external = crate::range::parse("[40,50)", ty.range, &jiff::tz::TimeZone::UTC)
            .expect("external range");
        assert!(!adjacent_range(&split, &internal).expect("internal adjacency"));
        assert!(adjacent_range(&split, &external).expect("external adjacency"));

        let left = parse("{[1,5),[10,15)}", ty, &jiff::tz::TimeZone::UTC).expect("left");
        let right = parse("{[3,12)}", ty, &jiff::tz::TimeZone::UTC).expect("right");
        let render = |value: &MultirangeValue| {
            to_text(value, |bound| match bound {
                Datum::Int4(value) => value.to_string(),
                _ => unreachable!(),
            })
        };
        assert_eq!(render(&union(&left, &right).expect("union")), "{[1,15)}");
        assert_eq!(
            render(&intersection(&left, &right).expect("intersection")),
            "{[3,5),[10,12)}"
        );
        assert_eq!(
            render(&difference(&left, &right).expect("difference")),
            "{[1,3),[12,15)}"
        );
    }
}
