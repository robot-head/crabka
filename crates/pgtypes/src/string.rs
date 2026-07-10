//! `PostgreSQL` length modifiers for `varchar(n)` and `char(n)`.

use crate::TypeError;

/// Apply a `varchar(n)` modifier to a text value.
pub fn apply_varchar_typmod(value: &str, limit: Option<u16>) -> Result<String, TypeError> {
    apply_string_typmod(value, limit, false)
}

/// Apply a `char(n)`/`character(n)` modifier to a text value.
pub fn apply_char_typmod(value: &str, limit: Option<u16>) -> Result<String, TypeError> {
    apply_string_typmod(value, limit, true)
}

fn apply_string_typmod(
    value: &str,
    limit: Option<u16>,
    pad_to_limit: bool,
) -> Result<String, TypeError> {
    let Some(limit) = limit else {
        return Ok(value.to_string());
    };
    let limit = usize::from(limit);
    let char_count = value.chars().count();
    if char_count > limit {
        return truncate_if_only_trailing_spaces(value, limit);
    }
    if !pad_to_limit || char_count == limit {
        return Ok(value.to_string());
    }

    let mut out = String::with_capacity(value.len() + limit - char_count);
    out.push_str(value);
    out.extend(std::iter::repeat_n(' ', limit - char_count));
    Ok(out)
}

fn truncate_if_only_trailing_spaces(value: &str, limit: usize) -> Result<String, TypeError> {
    let mut out = String::new();
    let mut chars = value.chars();
    for _ in 0..limit {
        let Some(ch) = chars.next() else {
            return Ok(out);
        };
        out.push(ch);
    }
    if chars.all(|ch| ch == ' ') {
        Ok(out)
    } else {
        Err(TypeError::StringDataRightTruncation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varchar_typmod_enforces_character_length() {
        assert_eq!(apply_varchar_typmod("abc", Some(3)).expect("ok"), "abc");
        assert_eq!(apply_varchar_typmod("éx", Some(2)).expect("ok"), "éx");
        assert_eq!(apply_varchar_typmod("abc  ", Some(3)).expect("ok"), "abc");
        assert!(matches!(
            apply_varchar_typmod("abcd", Some(3)),
            Err(TypeError::StringDataRightTruncation)
        ));
    }

    #[test]
    fn char_typmod_pads_and_truncates_spaces_only() {
        assert_eq!(apply_char_typmod("a", Some(3)).expect("ok"), "a  ");
        assert_eq!(apply_char_typmod("abc  ", Some(3)).expect("ok"), "abc");
        assert_eq!(
            apply_char_typmod("unconstrained", None).expect("ok"),
            "unconstrained"
        );
        assert!(matches!(
            apply_char_typmod("abcd", Some(3)),
            Err(TypeError::StringDataRightTruncation)
        ));
    }
}
