//! PostgreSQL-compatible `uuid` parsing and output helpers.

use crate::TypeError;

/// Parsed UUID bytes in `PostgreSQL`'s network byte order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UuidBytes(pub [u8; 16]);

impl UuidBytes {
    /// Parse a UUID input string accepted by this starter.
    ///
    /// Supports canonical hyphenated text, uppercase hex, optional surrounding
    /// braces, and the common 32-hex-digit hyphenless form. Output is always the
    /// canonical lowercase hyphenated spelling.
    ///
    /// # Errors
    ///
    /// Returns `22P02` invalid text representation for malformed input.
    pub fn parse(input: &str) -> Result<Self, TypeError> {
        parse_uuid(input).ok_or_else(|| TypeError::InvalidText {
            type_name: "uuid",
            value: input.to_string(),
        })
    }

    /// Return `PostgreSQL`'s canonical lowercase hyphenated UUID text.
    #[must_use]
    pub fn to_canonical_text(self) -> String {
        let mut out = String::with_capacity(36);
        for (idx, byte) in self.0.iter().enumerate() {
            if matches!(idx, 4 | 6 | 8 | 10) {
                out.push('-');
            }
            out.push(nibble_to_hex(byte >> 4));
            out.push(nibble_to_hex(byte & 0x0f));
        }
        out
    }
}

fn parse_uuid(input: &str) -> Option<UuidBytes> {
    let trimmed = input.trim();
    let body = strip_matching_braces(trimmed)?;
    if !has_supported_hyphen_layout(body) {
        return None;
    }
    let mut hex = [0u8; 32];
    let mut len = 0usize;
    for byte in body.bytes() {
        if byte == b'-' {
            continue;
        }
        if len == hex.len() || !byte.is_ascii_hexdigit() {
            return None;
        }
        hex[len] = byte;
        len += 1;
    }
    if len != hex.len() {
        return None;
    }

    let mut bytes = [0u8; 16];
    for (dst, pair) in bytes.iter_mut().zip(hex.as_chunks::<2>().0) {
        *dst = (hex_value(pair[0])? << 4) | hex_value(pair[1])?;
    }
    Some(UuidBytes(bytes))
}

fn has_supported_hyphen_layout(input: &str) -> bool {
    if !input.as_bytes().contains(&b'-') {
        return input.len() == 32;
    }
    input.len() == 36
        && input
            .bytes()
            .enumerate()
            .all(|(idx, byte)| matches!((idx, byte), (8 | 13 | 18 | 23, b'-') | (_, b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F')))
}

fn strip_matching_braces(input: &str) -> Option<&str> {
    if let Some(without_open) = input.strip_prefix('{') {
        return without_open.strip_suffix('}');
    }
    if input.ends_with('}') {
        return None;
    }
    Some(input)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn nibble_to_hex(nibble: u8) -> char {
    char::from(b"0123456789abcdef"[usize::from(nibble)])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_input_accepts_canonical_uppercase_braces_and_hyphenless() {
        let canonical = "550e8400-e29b-41d4-a716-446655440000";
        for input in [
            canonical,
            "550E8400-E29B-41D4-A716-446655440000",
            "{550e8400-e29b-41d4-a716-446655440000}",
            "550e8400e29b41d4a716446655440000",
        ] {
            assert_eq!(
                UuidBytes::parse(input).expect(input).to_canonical_text(),
                canonical
            );
        }
    }

    #[test]
    fn uuid_input_rejects_malformed_text_with_22p02() {
        for input in [
            "",
            "not-a-uuid",
            "550e8400-e29b-41d4-a716",
            "550e-8400e29b41d4a716446655440000",
            "{550e8400-e29b-41d4-a716-446655440000",
            "550e8400-e29b-41d4-a716-446655440000}",
        ] {
            let err = UuidBytes::parse(input).expect_err(input);
            assert_eq!(err.sqlstate(), "22P02");
        }
    }
}
