//! `PostgreSQL`'s `"char"` type (OID 18) — the port of `char.c`.
//!
//! One byte, written in double quotes so the grammar's unquoted `char` keyword
//! keeps meaning `character(1)`. The two types share nothing: `character(1)` is
//! a varlena holding one *character* of the database encoding, and this holds
//! one *byte* that need not be a character at all. `pg_class.relkind`,
//! `pg_type.typtype` and every other single-letter catalog code is this type.
//!
//! The text form is not the byte. `charin` decodes a three-digit octal escape,
//! so `'\101'` is `A`, and `charout` re-escapes any byte with the high bit set,
//! so `0xFF` prints `\377` and reads back as itself. `0x00` has no printable
//! form at all and prints as the empty string, which is why the type cannot
//! represent "no value" separately from NUL.
//!
//! # Key Functions
//!
//! - [`parse`] / [`to_text`] — `charin` / `charout`, and by the note in
//!   `char.c` also `char(text)` / `text(char)`: the two pairs differ only in
//!   how they reach the empty string, and reach the same byte from it.
//! - [`to_int4`] / [`from_int4`] — `chartoi4` / `i4tochar`.

use crate::TypeError;

/// `PostgreSQL` `"char"` type OID.
pub const OID: u32 = 18;

/// `charin`, and equally `char(text)`: the byte a text form names.
///
/// A four-character `\ooo` with three octal digits is that byte; anything else
/// contributes its first byte, and the rest is silently discarded, which
/// `char.c` keeps as a backwards-compatibility provision for multibyte input.
/// The empty string is `0x00` — `charin` reads it off the terminating NUL of
/// the C string, and `text_char` says so outright.
#[must_use]
pub fn parse(text: &str) -> u8 {
    let bytes = text.as_bytes();
    if let [b'\\', a, b, c] = bytes
        && let (Some(a), Some(b), Some(c)) = (octal(*a), octal(*b), octal(*c))
    {
        return (a << 6) + (b << 3) + c;
    }
    bytes.first().copied().unwrap_or(0)
}

/// `charout`, and equally `text(char)`: the text form of one byte.
///
/// `0x00` is the empty string, `0x01`..=`0x7F` is the byte itself, and a byte
/// with the high bit set is `\ooo` — the escape form `bytea`'s traditional
/// output uses, and the one [`parse`] reads back.
#[must_use]
pub fn to_text(value: u8) -> String {
    if value & 0x80 != 0 {
        format!("\\{value:03o}")
    } else if value == 0 {
        String::new()
    } else {
        char::from(value).to_string()
    }
}

/// `chartoi4`: the byte read as a **signed** 8-bit integer, so `\377` is -1.
///
/// The asymmetry is `char.c`'s own, and deliberate there: comparisons treat the
/// byte as unsigned and the integer conversions treat it as signed.
#[must_use]
pub fn to_int4(value: u8) -> i32 {
    i32::from(value.cast_signed())
}

/// `i4tochar`: an integer narrowed to one byte, signed as [`to_int4`] is.
///
/// # Errors
///
/// 22003 for anything outside -128..=127. `PostgreSQL` will not wrap here even
/// though every value of the type is reachable from some integer in range.
pub fn from_int4(value: i32) -> Result<u8, TypeError> {
    i8::try_from(value)
        .map(i8::cast_unsigned)
        .map_err(|_| TypeError::OutOfRange {
            message: "\"char\" out of range".to_string(),
        })
}

/// The value of one octal digit, `None` for any other byte.
fn octal(byte: u8) -> Option<u8> {
    (b'0'..=b'7').contains(&byte).then(|| byte - b'0')
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{from_int4, parse, to_int4, to_text};

    #[test]
    fn parse_reads_the_forms_charin_accepts() {
        for (text, expected) in [
            ("a", b'a'),
            ("A", b'A'),
            ("", 0),
            ("\\101", b'A'),
            ("\\377", 0xFF),
            ("\\000", 0),
            // Not four characters, or not three octal digits: the first byte.
            ("\\10", b'\\'),
            ("\\1015", b'\\'),
            ("\\389", b'\\'),
            ("\\\\", b'\\'),
            // The multibyte provision: the first byte, rest discarded.
            ("cd", b'c'),
        ] {
            assert!(parse(text) == expected, "charin({text:?})");
        }
    }

    #[test]
    fn to_text_round_trips_every_byte() {
        for byte in 0..=u8::MAX {
            assert!(parse(&to_text(byte)) == byte, "byte {byte}");
        }
    }

    #[test]
    fn to_text_escapes_only_the_high_half() {
        assert!(to_text(0) == "");
        assert!(to_text(b'a') == "a");
        assert!(to_text(0x7F) == "\u{7f}");
        assert!(to_text(0x80) == "\\200");
        assert!(to_text(0xFF) == "\\377");
    }

    #[test]
    fn integer_conversions_are_signed_and_range_checked() {
        assert!(to_int4(0xFF) == -1);
        assert!(to_int4(0x80) == -128);
        assert!(to_int4(b'a') == 97);
        assert!(from_int4(-1) == Ok(0xFF));
        assert!(from_int4(97) == Ok(b'a'));
        assert!(from_int4(128).is_err());
        assert!(from_int4(-129).is_err());
    }
}
