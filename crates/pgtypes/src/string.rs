//! `PostgreSQL` length modifiers for `varchar(n)` and `char(n)`.

use crate::TypeError;

/// Which coercion produced this length check.
///
/// `PostgreSQL` makes the same distinction with the `isExplicit` argument of its
/// `varchar(varchar, int4, bool)` and `bpchar(...)` cast functions. The SQL
/// standard requires both halves: an explicit cast truncates, an assignment
/// raises `string_data_right_truncation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Coercion {
    /// A cast the query wrote out (`v::varchar(3)`, `CAST(v AS char(2))`). It
    /// truncates an over-long value silently.
    Explicit,
    /// An implicit or assignment coercion, such as a store into a column or a
    /// parameter feed. It rejects an over-long value unless the characters it
    /// would discard are all spaces.
    Assignment,
}

/// Apply a `varchar(n)` modifier to a text value.
///
/// # Errors
///
/// Under [`Coercion::Assignment`], returns an error when a value exceeds the
/// limit and the excess characters are not all spaces.
pub fn apply_varchar_typmod(
    value: &str,
    limit: Option<u16>,
    how: Coercion,
) -> Result<String, TypeError> {
    apply_string_typmod(value, limit, false, how)
}

/// Apply a `char(n)`/`character(n)` modifier to a text value.
///
/// # Errors
///
/// Under [`Coercion::Assignment`], returns an error when a value exceeds the
/// limit and the excess characters are not all spaces.
pub fn apply_char_typmod(
    value: &str,
    limit: Option<u16>,
    how: Coercion,
) -> Result<String, TypeError> {
    apply_string_typmod(value, limit, true, how)
}

fn apply_string_typmod(
    value: &str,
    limit: Option<u16>,
    pad_to_limit: bool,
    how: Coercion,
) -> Result<String, TypeError> {
    let Some(limit) = limit else {
        return Ok(value.to_string());
    };
    let limit = usize::from(limit);
    let char_count = value.chars().count();
    if char_count > limit {
        return truncate(
            value,
            limit,
            how,
            if pad_to_limit {
                "character"
            } else {
                "character varying"
            },
        );
    }
    if !pad_to_limit || char_count == limit {
        return Ok(value.to_string());
    }

    let mut out = String::with_capacity(value.len() + limit - char_count);
    out.push_str(value);
    out.extend(std::iter::repeat_n(' ', limit - char_count));
    Ok(out)
}

/// Cut `value` to `limit` characters, then decide if the loss of the rest is
/// allowed. Trailing spaces are always safe to discard: they hold no information
/// for a bounded string type, so even an assignment accepts them.
fn truncate(
    value: &str,
    limit: usize,
    how: Coercion,
    type_name: &str,
) -> Result<String, TypeError> {
    let mut out = String::new();
    let mut chars = value.chars();
    for _ in 0..limit {
        let Some(ch) = chars.next() else {
            return Ok(out);
        };
        out.push(ch);
    }
    if how == Coercion::Explicit || chars.all(|ch| ch == ' ') {
        Ok(out)
    } else {
        Err(TypeError::StringDataRightTruncation {
            type_name: format!("{type_name}({limit})"),
        })
    }
}

#[cfg(test)]
mod tests {
    use Coercion::{Assignment, Explicit};

    use super::*;

    #[test]
    fn varchar_typmod_enforces_character_length() {
        let ok = |v, n| apply_varchar_typmod(v, n, Assignment).expect("ok");
        assert_eq!(ok("abc", Some(3)), "abc");
        assert_eq!(ok("éx", Some(2)), "éx");
        assert_eq!(ok("abc  ", Some(3)), "abc");
        assert!(matches!(
            apply_varchar_typmod("abcd", Some(3), Assignment),
            Err(TypeError::StringDataRightTruncation { .. })
        ));
    }

    #[test]
    fn char_typmod_pads_and_truncates_spaces_only() {
        let ok = |v, n| apply_char_typmod(v, n, Assignment).expect("ok");
        assert_eq!(ok("a", Some(3)), "a  ");
        assert_eq!(ok("abc  ", Some(3)), "abc");
        assert_eq!(ok("unconstrained", None), "unconstrained");
        assert!(matches!(
            apply_char_typmod("abcd", Some(3), Assignment),
            Err(TypeError::StringDataRightTruncation { .. })
        ));
    }

    /// An explicit cast truncates instead of an error. The SQL standard requires
    /// it, and `PostgreSQL` implements it with the `isExplicit` cast argument.
    #[test]
    fn an_explicit_cast_truncates_where_an_assignment_would_reject() {
        assert_eq!(
            apply_varchar_typmod("abcd", Some(3), Explicit).expect("truncates"),
            "abc"
        );
        assert_eq!(
            apply_char_typmod("abcd", Some(2), Explicit).expect("truncates"),
            "ab"
        );
        // Truncation counts characters, not bytes.
        assert_eq!(
            apply_varchar_typmod("héllo", Some(2), Explicit).expect("truncates"),
            "hé"
        );
        // Padding still applies when the value is short of an explicit char(n).
        assert_eq!(
            apply_char_typmod("a", Some(3), Explicit).expect("pads"),
            "a  "
        );
    }
}
