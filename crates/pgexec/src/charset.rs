//! The character sets a `COPY` payload can arrive in or leave in.
//!
//! The server encoding is UTF-8 and stays that way: every string this engine
//! holds is a Rust `String`, so the server side of every conversion is fixed.
//! `COPY`'s `ENCODING` option names the encoding of the *payload* — the file on
//! disk, or the bytes on the copy stream — and this module converts between
//! that and the server's own.
//!
//! The supported set is narrower than `PostgreSQL`'s, which carries a converter
//! for every ordered pair of its built-in encodings. A character set earns a
//! place here when this engine can convert it exactly:
//!
//! - `UTF8` needs no conversion at all.
//! - `SQL_ASCII` is `PostgreSQL`'s "do not convert", so a copy under it moves
//!   the server's own bytes in either direction. The one divergence is on the
//!   way in: `PostgreSQL` stores whatever bytes arrive, valid UTF-8 or not, and
//!   this engine still requires them to be UTF-8, so it refuses a payload
//!   `PostgreSQL` would have stored and only been unable to read back.
//! - `LATIN1` is ISO 8859-1, whose 256 bytes are the first 256 code points. The
//!   mapping is the identity in both directions and needs no table.
//! - `EUC_JP` has no such shortcut. Its byte grammar is checked here, because
//!   the grammar is what the error message reports on, and `encoding_rs`
//!   supplies the code-point mapping.
//!
//! Every name outside that set is refused by the caller rather than silently
//! treated as UTF-8, because ignoring an `ENCODING` would load *wrong rows*.
//!
//! ponytail: `convert_from` carries its own `EUC_KR` decoder in `string_fn`.
//! Fold both into this module when a third caller wants either of them.

use std::borrow::Cow;

/// A character set a `COPY` payload can be written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Charset {
    /// The server encoding. Neither direction converts anything.
    #[default]
    Utf8,
    /// `PostgreSQL`'s "no conversion" pseudo-encoding. See the module note on
    /// the one place this engine is stricter.
    SqlAscii,
    /// ISO 8859-1: byte `n` is code point `U+00n`.
    Latin1,
    /// Extended Unix Code, Japanese.
    EucJp,
}

/// A byte sequence the source character set does not allow.
///
/// The offset locates the sequence so the caller can name the line it fell on;
/// `bytes` is what `PostgreSQL` quotes back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InvalidSequence {
    /// Offset of the first byte of the offending sequence.
    pub(crate) offset: usize,
    /// The bytes to quote: the character length the lead byte promises, cut
    /// short by the end of the payload.
    pub(crate) bytes: Vec<u8>,
}

/// A character the target character set has no byte sequence for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Unmappable {
    /// The character that has no equivalent.
    pub(crate) character: char,
}

impl InvalidSequence {
    /// The byte list `PostgreSQL` prints after the encoding name.
    pub(crate) fn rendered(&self) -> String {
        hex_bytes(&self.bytes)
    }
}

impl Unmappable {
    /// The character's UTF-8 bytes, as `PostgreSQL` prints them.
    pub(crate) fn rendered(self) -> String {
        let mut buffer = [0_u8; 4];
        hex_bytes(self.character.encode_utf8(&mut buffer).as_bytes())
    }
}

/// Lower-case hex, one `0x` prefix per byte, space separated.
fn hex_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("0x{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

impl Charset {
    /// The character set an encoding name asks for, or `None` when this engine
    /// cannot convert it.
    ///
    /// The caller separates "not a name `PostgreSQL` knows" from "a name it
    /// knows and this engine does not", because the two have different
    /// messages. `PostgreSQL` matches these names on their alphanumeric
    /// characters alone, so `EUC-JP`, `eucjp` and `EUC_JP` are one name.
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        let normalized = name
            .bytes()
            .filter(u8::is_ascii_alphanumeric)
            .map(|byte| byte.to_ascii_lowercase())
            .collect::<Vec<_>>();
        match normalized.as_slice() {
            b"utf8" | b"unicode" => Some(Self::Utf8),
            b"sqlascii" => Some(Self::SqlAscii),
            b"latin1" | b"iso88591" => Some(Self::Latin1),
            b"eucjp" => Some(Self::EucJp),
            _ => None,
        }
    }

    /// The spelling `PostgreSQL` reports this set by.
    pub(crate) const fn canonical_name(self) -> &'static str {
        match self {
            Self::Utf8 => "UTF8",
            Self::SqlAscii => "SQL_ASCII",
            Self::Latin1 => "LATIN1",
            Self::EucJp => "EUC_JP",
        }
    }

    /// Whether a payload in this set is already the server's own bytes, so that
    /// a copy can move it without looking at it.
    pub(crate) const fn is_passthrough(self) -> bool {
        matches!(self, Self::Utf8 | Self::SqlAscii)
    }

    /// The number of bytes the character starting with `lead` occupies.
    ///
    /// This is `PostgreSQL`'s `pg_encoding_mblen`, and it answers for a lead
    /// byte that starts no valid character at all — the invalid-sequence
    /// message quotes this many bytes, so the two have to agree.
    const fn char_len(self, lead: u8) -> usize {
        match self {
            Self::Utf8 | Self::SqlAscii => match lead {
                0xc0..=0xdf => 2,
                0xe0..=0xef => 3,
                0xf0..=0xf7 => 4,
                _ => 1,
            },
            Self::Latin1 => 1,
            // SS3 introduces JIS X 0212 and takes two more bytes; SS2 and every
            // other high byte take one more.
            Self::EucJp => match lead {
                0x8f => 3,
                0x80..=0xff => 2,
                _ => 1,
            },
        }
    }

    /// Describe the sequence starting at `offset` as the one that is invalid.
    fn invalid_at(self, bytes: &[u8], offset: usize) -> InvalidSequence {
        let width = bytes
            .get(offset)
            .map_or(0, |lead| self.char_len(*lead).min(bytes.len() - offset));
        InvalidSequence {
            offset,
            bytes: bytes[offset..offset + width].to_vec(),
        }
    }

    /// Convert a payload in this character set to the server's UTF-8.
    ///
    /// A payload that is already the server's bytes is borrowed rather than
    /// copied, which is every ordinary copy.
    pub(crate) fn decode(self, bytes: &[u8]) -> Result<Cow<'_, str>, InvalidSequence> {
        match self {
            Self::Utf8 | Self::SqlAscii => std::str::from_utf8(bytes)
                .map(Cow::Borrowed)
                .map_err(|error| self.invalid_at(bytes, error.valid_up_to())),
            // ISO 8859-1 and UTF-8 agree exactly on ASCII, so an all-ASCII
            // payload needs no copy. Anything else has to be re-read byte by
            // byte, because the two disagree on every byte above 0x7f.
            Self::Latin1 => Ok(match std::str::from_utf8(bytes) {
                Ok(text) if text.is_ascii() => Cow::Borrowed(text),
                _ => Cow::Owned(bytes.iter().copied().map(char::from).collect()),
            }),
            Self::EucJp => {
                if let Some(offset) = first_invalid_eucjp(bytes) {
                    return Err(self.invalid_at(bytes, offset));
                }
                encoding_rs::EUC_JP
                    .decode_without_bom_handling_and_without_replacement(bytes)
                    .ok_or_else(|| {
                        // The grammar checked out, so the payload holds a
                        // sequence that is well formed and unassigned. Walk the
                        // characters to find which, because the message quotes
                        // its bytes.
                        self.invalid_at(bytes, first_undecodable_eucjp(bytes).unwrap_or(0))
                    })
            }
        }
    }

    /// Convert server text to a payload in this character set.
    pub(crate) fn encode(self, text: &str) -> Result<Cow<'_, [u8]>, Unmappable> {
        if self.is_passthrough() || text.is_ascii() {
            return Ok(Cow::Borrowed(text.as_bytes()));
        }
        match self {
            Self::Utf8 | Self::SqlAscii => Ok(Cow::Borrowed(text.as_bytes())),
            Self::Latin1 => text
                .chars()
                .map(|character| {
                    u8::try_from(u32::from(character)).map_err(|_| Unmappable { character })
                })
                .collect::<Result<Vec<u8>, Unmappable>>()
                .map(Cow::Owned),
            Self::EucJp => {
                let (encoded, _, had_errors) = encoding_rs::EUC_JP.encode(text);
                if had_errors {
                    return Err(Unmappable {
                        character: first_unencodable_eucjp(text)
                            .unwrap_or(char::REPLACEMENT_CHARACTER),
                    });
                }
                Ok(Cow::Owned(encoded.into_owned()))
            }
        }
    }
}

/// The offset of the first byte sequence EUC-JP does not allow.
///
/// This is `PostgreSQL`'s `pg_eucjp_verifychar`: ASCII stands alone, SS2
/// introduces one half-width katakana byte, SS3 introduces two JIS X 0212
/// bytes, and any other high byte is the first of a JIS X 0208 pair. Every byte
/// of a multi-byte character other than the SS2 tail lies in `0xa1..=0xfe`.
fn first_invalid_eucjp(bytes: &[u8]) -> Option<usize> {
    const JIS: std::ops::RangeInclusive<u8> = 0xa1..=0xfe;
    let mut offset = 0;
    while let Some(&lead) = bytes.get(offset) {
        let rest = &bytes[offset + 1..];
        let valid = match lead {
            0x00..=0x7f => true,
            // SS2: one byte of JIS X 0201 half-width katakana.
            0x8e => matches!(rest.first(), Some(byte) if (0xa1..=0xdf).contains(byte)),
            // SS3: two bytes of JIS X 0212.
            0x8f => {
                matches!(rest, [first, second, ..] if JIS.contains(first) && JIS.contains(second))
            }
            // JIS X 0208, whose lead byte is itself held to the same range.
            _ => JIS.contains(&lead) && matches!(rest.first(), Some(byte) if JIS.contains(byte)),
        };
        if !valid {
            return Some(offset);
        }
        offset += Charset::EucJp.char_len(lead);
    }
    None
}

/// The offset of the first grammatical EUC-JP character that maps to no code
/// point. Only reached when a whole-payload decode has already failed.
fn first_undecodable_eucjp(bytes: &[u8]) -> Option<usize> {
    let mut offset = 0;
    while let Some(&lead) = bytes.get(offset) {
        let width = Charset::EucJp.char_len(lead).min(bytes.len() - offset);
        if encoding_rs::EUC_JP
            .decode_without_bom_handling_and_without_replacement(&bytes[offset..offset + width])
            .is_none()
        {
            return Some(offset);
        }
        offset += width;
    }
    None
}

/// The first character EUC-JP has no byte sequence for. Only reached when a
/// whole-string encode has already reported errors.
fn first_unencodable_eucjp(text: &str) -> Option<char> {
    text.chars().find(|character| {
        let mut buffer = [0_u8; 4];
        let (_, _, had_errors) = encoding_rs::EUC_JP.encode(character.encode_utf8(&mut buffer));
        had_errors
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{Charset, InvalidSequence};

    #[test]
    fn names_resolve_on_alphanumerics_alone() {
        let cases = [
            ("UTF8", Some(Charset::Utf8)),
            ("utf-8", Some(Charset::Utf8)),
            ("Unicode", Some(Charset::Utf8)),
            ("SQL_ASCII", Some(Charset::SqlAscii)),
            ("sqlascii", Some(Charset::SqlAscii)),
            ("LATIN1", Some(Charset::Latin1)),
            ("iso-8859-1", Some(Charset::Latin1)),
            ("EUC_JP", Some(Charset::EucJp)),
            ("eucjp", Some(Charset::EucJp)),
            // Known to PostgreSQL, not convertible here.
            ("EUC_KR", None),
            ("LATIN2", None),
            ("BOGUS", None),
        ];
        for (name, expected) in cases {
            assert!(Charset::from_name(name) == expected, "{name}");
        }
    }

    #[test]
    fn latin1_maps_every_byte_to_its_own_code_point() {
        let bytes: Vec<u8> = (0..=255).collect();
        let decoded = Charset::Latin1.decode(&bytes).expect("LATIN1 never fails");
        let expected: String = (0..=255_u8).map(char::from).collect();
        assert!(decoded == expected);
    }

    #[test]
    fn latin1_reads_utf8_hiragana_as_three_characters() {
        // The copyencoding case: U+3042 written as UTF-8 and read back as
        // LATIN1 is three characters, not one.
        let decoded = Charset::Latin1
            .decode("\u{3042}".as_bytes())
            .expect("LATIN1 never fails");
        assert!(decoded == "\u{e3}\u{81}\u{82}");
    }

    #[test]
    fn round_trips_survive_both_directions() {
        let cases = [
            (Charset::Utf8, "\u{3042}plain"),
            (Charset::SqlAscii, "plain"),
            (Charset::Latin1, "caf\u{e9}"),
            (Charset::EucJp, "\u{3042}\u{3044}"),
        ];
        for (charset, text) in cases {
            let encoded = charset.encode(text).expect("mappable");
            let decoded = charset.decode(&encoded).expect("valid");
            assert!(decoded == text, "{}", charset.canonical_name());
        }
    }

    #[test]
    fn invalid_sequences_report_the_bytes_postgresql_quotes() {
        // Each case is PostgreSQL 18.4's observed report for the same payload.
        let cases = [
            (Charset::EucJp, b"\xe3\x81\x82".as_slice(), 0, "0xe3 0x81"),
            (Charset::EucJp, b"ok\n\x8f\xa1\n", 3, "0x8f 0xa1 0x0a"),
            (Charset::EucJp, b"ok\n\x8e\x20", 3, "0x8e 0x20"),
            (Charset::EucJp, b"ok\n\xa1", 3, "0xa1"),
            (Charset::Utf8, b"\xc3", 0, "0xc3"),
            (Charset::Utf8, b"\xc3\x00", 0, "0xc3 0x00"),
            (Charset::Utf8, b"\xaf", 0, "0xaf"),
            (Charset::Utf8, b"\xf0\x80\x80\x80", 0, "0xf0 0x80 0x80 0x80"),
        ];
        for (charset, payload, offset, rendered) in cases {
            let error = charset.decode(payload).expect_err("invalid");
            let expected = InvalidSequence {
                offset,
                bytes: rendered
                    .split(' ')
                    .map(|byte| u8::from_str_radix(&byte[2..], 16).expect("hex"))
                    .collect(),
            };
            assert!(error == expected, "{rendered}");
            assert!(error.rendered() == rendered);
        }
    }

    #[test]
    fn ascii_is_accepted_by_every_character_set() {
        for charset in [
            Charset::Utf8,
            Charset::SqlAscii,
            Charset::Latin1,
            Charset::EucJp,
        ] {
            let decoded = charset.decode(b"a,b\n1,2\n").expect("ASCII is valid");
            assert!(decoded == "a,b\n1,2\n", "{}", charset.canonical_name());
        }
    }

    #[test]
    fn unmappable_characters_report_their_utf8_bytes() {
        let cases = [
            (Charset::Latin1, '\u{3042}', "0xe3 0x81 0x82"),
            (Charset::EucJp, '\u{20ac}', "0xe2 0x82 0xac"),
        ];
        for (charset, character, rendered) in cases {
            let error = charset
                .encode(&character.to_string())
                .expect_err("unmappable");
            assert!(error.character == character, "{rendered}");
            assert!(error.rendered() == rendered);
        }
    }
}
