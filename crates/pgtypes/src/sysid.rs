//! `PostgreSQL`'s system identifier types — the ports of `oid.c`, `xid.c`,
//! `tid.c` and `pg_lsn.c`.
//!
//! Five of the six types here are unsigned integers that `PostgreSQL` reads
//! with one routine, `uint32in_subr`, and it is `strtoul(s, &endptr, 0)` — base
//! **zero**, not ten. That single detail decides most of the observable
//! behaviour: `'010'::oid` is 8, `'0x10'::oid` is 16, `'0b101'::oid` is 5 on a
//! glibc new enough to take the binary prefix, and `'-1'::oid` is 4294967295
//! because `strtoul` negates in `unsigned long` and the range check accepts a
//! value that matches after *either* signed or unsigned extension.
//!
//! | type | width | input | ordering |
//! |------|-------|-------|----------|
//! | `oid` (26) | `u32` | [`uint32_in`] | full, unsigned |
//! | `xid` (28) | `u32` | [`uint32_in`] | **equality only** |
//! | `xid8` (5069) | `u64` | [`uint64_in`] | full, unsigned |
//! | `cid` (29) | `u32` | [`uint32_in`] | **equality only** |
//! | `tid` (27) | `(u32, u16)` | [`Tid::parse`] | full, block then offset |
//! | `pg_lsn` (3220) | `u64` | [`lsn_in`] | full, unsigned |
//!
//! `xid` and `cid` are the two without a B-tree opclass. `xid`'s reason is in
//! `xid.c`'s own comment — transaction ids compare with modular arithmetic,
//! which does not respect the triangle inequality, so there is no total order
//! to give a B-tree. Callers must therefore refuse `<`, `<=`, `>` and `>=` for
//! both rather than inventing one, which is what [`ordered`] answers.
//!
//! # Key Functions
//!
//! - [`uint32_in`] / [`uint64_in`] — `uint32in_subr` / `uint64in_subr`.
//! - [`Tid`] — `tidin` / `tidout` and `ItemPointerCompare`.
//! - [`lsn_in`] / [`lsn_to_text`] — `pg_lsn_in` / `pg_lsn_out`.
//! - [`lsn_add`] / [`lsn_sub`] / [`lsn_diff`] — `pg_lsn_pli` / `pg_lsn_mii` /
//!   `pg_lsn_mi`.

use crate::{
    TypeError,
    numeric::{self, NumericValue},
};

/// `PostgreSQL` `oid` type OID.
pub const OID: u32 = 26;
/// `PostgreSQL` `xid` type OID.
pub const XID: u32 = 28;
/// `PostgreSQL` `xid8` type OID.
pub const XID8: u32 = 5069;
/// `PostgreSQL` `cid` type OID.
pub const CID: u32 = 29;
/// `PostgreSQL` `tid` type OID.
pub const TID: u32 = 27;
/// `PostgreSQL` `pg_lsn` type OID.
pub const PG_LSN: u32 = 3220;

/// The largest `pg_lsn` component `pg_lsn_in` accepts, in hex digits.
const MAX_LSN_COMPONENT: usize = 8;

/// `isspace()` in the C locale — the set `strtoul` skips and `uint32in_subr`
/// allows after the digits.
fn is_c_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// The outcome of a C `strtoul`: the converted value, how many bytes it
/// consumed, and whether it set `ERANGE`.
struct StrToUl {
    value: u64,
    consumed: usize,
    range_error: bool,
}

/// What C's `strtoul` leaves behind when it converts nothing: zero, with
/// `endptr` still at the start of the string and `errno` untouched.
const NO_DIGITS: StrToUl = StrToUl {
    value: 0,
    consumed: 0,
    range_error: false,
};

/// `strtoul(s, &endptr, base)` over a byte slice, with `unsigned long` 64 bits
/// wide — which is what every platform crabka builds for has, and what makes
/// `'-1'::oid` land on 4294967295 rather than erroring.
///
/// `base` 0 selects the base from the prefix: `0x`/`0X` hex, `0b`/`0B` binary
/// (a glibc extension the pinned oracle has), a leading `0` octal, otherwise
/// decimal. Returns `None` when no digits were converted, which C signals by
/// leaving `endptr == s`.
fn strtoul(s: &[u8], base: u32) -> Option<StrToUl> {
    let mut i = 0;
    while i < s.len() && is_c_space(s[i]) {
        i += 1;
    }
    let negative = match s.get(i) {
        Some(b'-') => {
            i += 1;
            true
        }
        Some(b'+') => {
            i += 1;
            false
        }
        _ => false,
    };
    // Prefix detection. A prefix with no digit after it is not a prefix at all:
    // `strtoul("0x", …, 0)` converts the `0` and leaves `endptr` on the `x`.
    let mut base = base;
    if (base == 0 || base == 16)
        && s.get(i) == Some(&b'0')
        && matches!(s.get(i + 1), Some(b'x' | b'X'))
        && s.get(i + 2).is_some_and(u8::is_ascii_hexdigit)
    {
        i += 2;
        base = 16;
    } else if (base == 0 || base == 2)
        && s.get(i) == Some(&b'0')
        && matches!(s.get(i + 1), Some(b'b' | b'B'))
        && matches!(s.get(i + 2), Some(b'0' | b'1'))
    {
        i += 2;
        base = 2;
    } else if base == 0 {
        base = if s.get(i) == Some(&b'0') { 8 } else { 10 };
    }
    let digits_start = i;
    let mut value: u64 = 0;
    let mut range_error = false;
    while let Some(digit) = s.get(i).and_then(|b| (*b as char).to_digit(36)) {
        if digit >= base {
            break;
        }
        value = value
            .checked_mul(u64::from(base))
            .and_then(|v| v.checked_add(u64::from(digit)))
            .unwrap_or_else(|| {
                range_error = true;
                u64::MAX
            });
        i += 1;
    }
    if i == digits_start {
        return None;
    }
    if range_error {
        // C leaves ULONG_MAX in `result` and sets ERANGE, having consumed every
        // digit regardless.
        return Some(StrToUl {
            value: u64::MAX,
            consumed: i,
            range_error: true,
        });
    }
    Some(StrToUl {
        value: if negative {
            value.wrapping_neg()
        } else {
            value
        },
        consumed: i,
        range_error: false,
    })
}

/// `PostgreSQL`'s `uint32in_subr` — the input function `oid`, `xid` and `cid`
/// all share, differing only in the type name their errors name.
///
/// # Errors
///
/// 22P02 when nothing converted or non-space text follows the digits, 22003
/// when the value does not fit `u32` after either signed or unsigned extension.
pub fn uint32_in(s: &str, type_name: &'static str) -> Result<u32, TypeError> {
    let invalid = || TypeError::InvalidText {
        type_name,
        value: s.to_string(),
    };
    let out_of_range = || TypeError::OutOfRange {
        message: format!("value \"{s}\" is out of range for type {type_name}"),
    };
    let parsed = strtoul(s.as_bytes(), 0).ok_or_else(invalid)?;
    if parsed.range_error {
        return Err(out_of_range());
    }
    let rest = &s.as_bytes()[parsed.consumed..];
    if rest.iter().any(|b| !is_c_space(*b)) {
        return Err(invalid());
    }
    // `unsigned long` is wider than `uint32` here, so `strtoul` did not report
    // the values that overflow the narrower type. `uint32in_subr` accepts one
    // that round-trips through *either* extension, which is what lets a value
    // written with a minus sign through.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the truncation is the conversion uint32in_subr performs"
    )]
    let result = parsed.value as u32;
    let signed = i64::from(result.cast_signed()).cast_unsigned();
    if parsed.value != u64::from(result) && parsed.value != signed {
        return Err(out_of_range());
    }
    Ok(result)
}

/// `PostgreSQL`'s `uint64in_subr` — `xid8`'s input function.
///
/// The same routine as [`uint32_in`] without the narrowing check, because
/// `strtou64` already reports every value that leaves `u64`.
///
/// # Errors
///
/// 22P02 when nothing converted or non-space text follows the digits, 22003 on
/// `ERANGE`.
pub fn uint64_in(s: &str, type_name: &'static str) -> Result<u64, TypeError> {
    let parsed = strtoul(s.as_bytes(), 0).ok_or_else(|| TypeError::InvalidText {
        type_name,
        value: s.to_string(),
    })?;
    if parsed.range_error {
        return Err(TypeError::OutOfRange {
            message: format!("value \"{s}\" is out of range for type {type_name}"),
        });
    }
    let rest = &s.as_bytes()[parsed.consumed..];
    if rest.iter().any(|b| !is_c_space(*b)) {
        return Err(TypeError::InvalidText {
            type_name,
            value: s.to_string(),
        });
    }
    Ok(parsed.value)
}

/// A `tid` — `PostgreSQL`'s `ItemPointerData`, a block number and a one-based
/// offset inside that block.
///
/// Ordering is `ItemPointerCompare`: block first, then offset, both unsigned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tid {
    pub block: u32,
    pub offset: u16,
}

impl Tid {
    /// `tidin`.
    ///
    /// The grammar is looser than `(block,offset)` suggests, and deliberately
    /// ported rather than tightened: the scan takes the **first** `(` and the
    /// **first** `,` before a `)`, so `x(1,2)` and `(1,2)junk` both parse, while
    /// `(1 , 2)` does not — the byte after the block number must be the comma
    /// itself. Both components are read in base 10 (not `strtoul`'s base 0, as
    /// the shared unsigned reader uses), so `(0x10,0x2)` is a syntax error.
    ///
    /// # Errors
    ///
    /// 22P02 for every rejection, including the two that are semantically range
    /// errors — a block past `u32` and an offset past `u16` are both
    /// `invalid input syntax for type tid`, not 22003.
    pub fn parse(s: &str) -> Result<Self, TypeError> {
        let invalid = || TypeError::InvalidText {
            type_name: "tid",
            value: s.to_string(),
        };
        let bytes = s.as_bytes();
        let mut coord: [Option<&[u8]>; 2] = [None, None];
        let mut found = 0;
        for (i, byte) in bytes.iter().enumerate() {
            if found >= 2 || *byte == b')' {
                break;
            }
            if *byte == b',' || (*byte == b'(' && found == 0) {
                coord[found] = Some(&bytes[i + 1..]);
                found += 1;
            }
        }
        let [Some(block_text), Some(offset_text)] = coord else {
            return Err(invalid());
        };
        // `tidin` checks only `errno` and the delimiter — never `endptr == s` —
        // so a component with no digits at all converts to 0 and `(,)` is
        // `(0,0)`. That is why this reads the `None` case as a zero rather than
        // rejecting it the way [`uint32_in`] does.
        let parsed = strtoul(block_text, 10).unwrap_or(NO_DIGITS);
        if parsed.range_error || block_text.get(parsed.consumed) != Some(&b',') {
            return Err(invalid());
        }
        #[expect(
            clippy::cast_possible_truncation,
            reason = "the truncation is the conversion tidin performs"
        )]
        let block = parsed.value as u32;
        let signed = i64::from(block.cast_signed()).cast_unsigned();
        if parsed.value != u64::from(block) && parsed.value != signed {
            return Err(invalid());
        }
        let parsed = strtoul(offset_text, 10).unwrap_or(NO_DIGITS);
        if parsed.range_error
            || offset_text.get(parsed.consumed) != Some(&b')')
            || parsed.value > u64::from(u16::MAX)
        {
            return Err(invalid());
        }
        #[expect(
            clippy::cast_possible_truncation,
            reason = "the bound above is exactly u16::MAX"
        )]
        let offset = parsed.value as u16;
        Ok(Tid { block, offset })
    }

    /// `tidout` — `(block,offset)` with no space.
    #[must_use]
    pub fn to_text(self) -> String {
        format!("({},{})", self.block, self.offset)
    }
}

/// `pg_lsn_in` — `X/Y`, both components 1..=8 hexadecimal digits and nothing
/// else, not even surrounding whitespace.
///
/// # Errors
///
/// 22P02 for anything the grammar rejects.
pub fn lsn_in(s: &str) -> Result<u64, TypeError> {
    let invalid = || TypeError::InvalidText {
        type_name: "pg_lsn",
        value: s.to_string(),
    };
    let bytes = s.as_bytes();
    let len1 = bytes.iter().take_while(|b| b.is_ascii_hexdigit()).count();
    if !(1..=MAX_LSN_COMPONENT).contains(&len1) || bytes.get(len1) != Some(&b'/') {
        return Err(invalid());
    }
    let tail = &bytes[len1 + 1..];
    let len2 = tail.iter().take_while(|b| b.is_ascii_hexdigit()).count();
    if !(1..=MAX_LSN_COMPONENT).contains(&len2) || tail.len() != len2 {
        return Err(invalid());
    }
    let high = u64::from_str_radix(&s[..len1], 16).map_err(|_| invalid())?;
    let low = u64::from_str_radix(&s[len1 + 1..], 16).map_err(|_| invalid())?;
    Ok((high << 32) | low)
}

/// `pg_lsn_out` — `%X/%X`, upper case and unpadded.
#[must_use]
pub fn lsn_to_text(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

/// `pg_lsn_mi` — the signed byte distance between two LSNs, as `numeric`.
///
/// # Panics
///
/// Never: the only input to `numeric::parse` is a `u64`'s decimal spelling with
/// an optional leading minus, which is always a valid numeric.
#[must_use]
pub fn lsn_diff(left: u64, right: u64) -> NumericValue {
    let magnitude = left.abs_diff(right);
    let text = if left < right {
        format!("-{magnitude}")
    } else {
        magnitude.to_string()
    };
    numeric::parse(&text).expect("a u64 magnitude with an optional sign is a valid numeric")
}

/// `numeric_pg_lsn` — round to the nearest integer, then require `0 ..= 2^64-1`.
///
/// # Errors
///
/// 0A000 for `NaN` and the infinities, 22023 (`pg_lsn out of range`) otherwise.
pub fn lsn_from_numeric(value: &NumericValue) -> Result<u64, TypeError> {
    let out_of_range = || TypeError::Coded {
        sqlstate: "22023",
        message: "pg_lsn out of range".to_string(),
    };
    match value {
        NumericValue::Finite(_) => {}
        NumericValue::NaN => {
            return Err(TypeError::Domain {
                sqlstate: "0A000",
                message: "cannot convert NaN to pg_lsn",
            });
        }
        NumericValue::Infinity | NumericValue::NegInfinity => {
            return Err(TypeError::Domain {
                sqlstate: "0A000",
                message: "cannot convert infinity to pg_lsn",
            });
        }
    }
    numeric::to_text(&numeric::round(value, 0))
        .parse::<u64>()
        .map_err(|_| out_of_range())
}

/// `pg_lsn_pli` — `lsn + nbytes`, through `numeric`.
///
/// # Errors
///
/// 0A000 for a `NaN` addend (with `pg_lsn_pli`'s own wording, which differs
/// from the conversion's), otherwise whatever [`lsn_from_numeric`] reports.
pub fn lsn_add(lsn: u64, nbytes: &NumericValue) -> Result<u64, TypeError> {
    if matches!(nbytes, NumericValue::NaN) {
        return Err(TypeError::Domain {
            sqlstate: "0A000",
            message: "cannot add NaN to pg_lsn",
        });
    }
    lsn_from_numeric(&numeric::add(&lsn_as_numeric(lsn), nbytes))
}

/// `pg_lsn_mii` — `lsn - nbytes`, through `numeric`.
///
/// # Errors
///
/// As [`lsn_add`], with `pg_lsn_mii`'s wording for the `NaN` case.
pub fn lsn_sub(lsn: u64, nbytes: &NumericValue) -> Result<u64, TypeError> {
    if matches!(nbytes, NumericValue::NaN) {
        return Err(TypeError::Domain {
            sqlstate: "0A000",
            message: "cannot subtract NaN from pg_lsn",
        });
    }
    lsn_from_numeric(&numeric::sub(&lsn_as_numeric(lsn), nbytes))
}

fn lsn_as_numeric(lsn: u64) -> NumericValue {
    numeric::parse(&lsn.to_string()).expect("a u64's decimal spelling is a valid numeric")
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    /// Every case is `uint32in_subr` run against the pinned 18.4 oracle. The
    /// base-0 rows are the point: a leading `0` is octal and `0x`/`0b` are
    /// prefixes, so three of these are not the decimal reading.
    #[test]
    fn uint32_in_matches_postgresql() {
        let cases: &[(&str, Result<u32, &str>)] = &[
            ("0", Ok(0)),
            ("1234", Ok(1234)),
            ("4294967295", Ok(4_294_967_295)),
            ("010", Ok(8)),
            ("0x10", Ok(16)),
            ("0xFFFFFFFF", Ok(4_294_967_295)),
            ("0b101", Ok(5)),
            ("00", Ok(0)),
            ("-0", Ok(0)),
            ("+5", Ok(5)),
            ("  5  ", Ok(5)),
            ("\t  15 \t  ", Ok(15)),
            ("5\n", Ok(5)),
            ("-1", Ok(4_294_967_295)),
            ("-1040", Ok(4_294_966_256)),
            ("18446744073709551615", Ok(4_294_967_295)),
            ("-18446744073709551615", Ok(1)),
            ("", Err("22P02")),
            ("    ", Err("22P02")),
            ("asdfasd", Err("22P02")),
            ("99asdfasd", Err("22P02")),
            ("5    d", Err("22P02")),
            ("    5d", Err("22P02")),
            ("5    5", Err("22P02")),
            (" - 500", Err("22P02")),
            ("0x", Err("22P02")),
            ("0b", Err("22P02")),
            ("0b2", Err("22P02")),
            ("08", Err("22P02")),
            ("09", Err("22P02")),
            ("0xg", Err("22P02")),
            ("99999999999", Err("22003")),
            ("4294967296", Err("22003")),
            ("18446744073709551616", Err("22003")),
            ("32958209582039852935", Err("22003")),
            ("-23582358720398502385", Err("22003")),
            ("0xFFFFFFFFF", Err("22003")),
        ];
        for (input, want) in cases {
            let got = uint32_in(input, "oid");
            match want {
                Ok(value) => assert!(got == Ok(*value), "{input:?}"),
                Err("22P02") => assert!(
                    got == Err(TypeError::InvalidText {
                        type_name: "oid",
                        value: (*input).to_string()
                    }),
                    "{input:?}"
                ),
                Err(_) => assert!(
                    got == Err(TypeError::OutOfRange {
                        message: format!("value \"{input}\" is out of range for type oid")
                    }),
                    "{input:?}"
                ),
            }
        }
    }

    #[test]
    fn uint64_in_matches_postgresql() {
        let cases: &[(&str, Result<u64, &str>)] = &[
            ("0", Ok(0)),
            ("010", Ok(8)),
            ("42", Ok(42)),
            ("0xffffffffffffffff", Ok(u64::MAX)),
            ("-1", Ok(u64::MAX)),
            ("0b101", Ok(5)),
            ("18446744073709551615", Ok(u64::MAX)),
            ("18446744073709551616", Err("22003")),
            ("", Err("22P02")),
            ("asdf", Err("22P02")),
        ];
        for (input, want) in cases {
            let got = uint64_in(input, "xid8");
            match want {
                Ok(value) => assert!(got == Ok(*value), "{input:?}"),
                Err("22P02") => assert!(
                    got == Err(TypeError::InvalidText {
                        type_name: "xid8",
                        value: (*input).to_string()
                    }),
                    "{input:?}"
                ),
                Err(_) => assert!(
                    got == Err(TypeError::OutOfRange {
                        message: format!("value \"{input}\" is out of range for type xid8")
                    }),
                    "{input:?}"
                ),
            }
        }
    }

    #[test]
    fn tid_parse_matches_postgresql() {
        let cases: &[(&str, Option<(u32, u16)>)] = &[
            ("(0,0)", Some((0, 0))),
            ("(0,1)", Some((0, 1))),
            ("(-1,0)", Some((4_294_967_295, 0))),
            ("(4294967295,65535)", Some((4_294_967_295, 65535))),
            ("(18446744073709551615,1)", Some((4_294_967_295, 1))),
            // The scan takes the first `(` wherever it is, and stops at `)`.
            ("x(1,2)", Some((1, 2))),
            (" (1,2) ", Some((1, 2))),
            ("(1,2)junk", Some((1, 2))),
            ("(4294967296,1)", None),
            ("(1,65536)", None),
            ("(0)", None),
            ("(0,-1)", None),
            ("(1 , 2)", None),
            ("((1,2)", None),
            ("(0x10,0x2)", None),
            // `tidin` never checks whether any digits converted, only that the
            // delimiter follows, so an empty component is a zero.
            ("(,)", Some((0, 0))),
            ("()", None),
            ("", None),
        ];
        for (input, want) in cases {
            let got = Tid::parse(input);
            match want {
                Some((block, offset)) => {
                    assert!(
                        got == Ok(Tid {
                            block: *block,
                            offset: *offset
                        }),
                        "{input:?}"
                    );
                }
                None => assert!(
                    got == Err(TypeError::InvalidText {
                        type_name: "tid",
                        value: (*input).to_string()
                    }),
                    "{input:?}"
                ),
            }
        }
    }

    #[test]
    fn tid_round_trips_through_its_text_form() {
        for tid in [
            Tid {
                block: 0,
                offset: 0,
            },
            Tid {
                block: 1,
                offset: 2,
            },
            Tid {
                block: u32::MAX,
                offset: u16::MAX,
            },
        ] {
            assert!(Tid::parse(&tid.to_text()) == Ok(tid));
        }
    }

    #[test]
    fn tid_orders_by_block_then_offset() {
        let mut tids = vec![
            Tid {
                block: 1,
                offset: 0,
            },
            Tid {
                block: 0,
                offset: 5,
            },
            Tid {
                block: 1,
                offset: 4,
            },
            Tid {
                block: 0,
                offset: 1,
            },
        ];
        tids.sort_unstable();
        assert!(
            tids == vec![
                Tid {
                    block: 0,
                    offset: 1
                },
                Tid {
                    block: 0,
                    offset: 5
                },
                Tid {
                    block: 1,
                    offset: 0
                },
                Tid {
                    block: 1,
                    offset: 4
                },
            ]
        );
    }

    #[test]
    fn lsn_in_matches_postgresql() {
        let cases: &[(&str, Option<u64>)] = &[
            ("0/0", Some(0)),
            ("0/1", Some(1)),
            ("1/0", Some(1 << 32)),
            ("FFFFFFFF/FFFFFFFF", Some(u64::MAX)),
            ("ffffffff/ffffffff", Some(u64::MAX)),
            ("abc/DEF", Some((0xabc << 32) | 0xDEF)),
            ("0/12345678", Some(0x1234_5678)),
            ("G/0", None),
            ("-1/0", None),
            (" 0/12345678", None),
            ("ABCD/", None),
            ("/ABCD", None),
            ("123456789/1", None),
            ("1/123456789", None),
            ("16AE7F7", None),
            ("0/0 ", None),
            ("0//1", None),
            ("", None),
        ];
        for (input, want) in cases {
            let got = lsn_in(input);
            match want {
                Some(value) => assert!(got == Ok(*value), "{input:?}"),
                None => assert!(
                    got == Err(TypeError::InvalidText {
                        type_name: "pg_lsn",
                        value: (*input).to_string()
                    }),
                    "{input:?}"
                ),
            }
        }
    }

    #[test]
    fn lsn_text_round_trips_in_postgresqls_spelling() {
        let cases: &[(u64, &str)] = &[
            (0, "0/0"),
            (1, "0/1"),
            (0x0000_0000_016A_E7F7, "0/16AE7F7"),
            (u64::MAX, "FFFFFFFF/FFFFFFFF"),
        ];
        for (value, text) in cases {
            assert!(lsn_to_text(*value) == *text);
            assert!(lsn_in(text) == Ok(*value));
        }
    }

    #[test]
    fn lsn_arithmetic_matches_postgresql() {
        let sixteen = numeric::parse("16").expect("literal");
        assert!(lsn_add(0x016A_E7F7, &sixteen) == Ok(0x016A_E807));
        assert!(lsn_sub(0x016A_E7F7, &sixteen) == Ok(0x016A_E7E7));
        // `numericvar_to_uint64` rounds rather than truncating: 0x10 + 1.7 is
        // 17.7, which lands on 18 (0x12).
        let one_point_seven = numeric::parse("1.7").expect("literal");
        assert!(lsn_add(0x10, &one_point_seven) == Ok(0x12));
        // Either end of the range is 22023, not a wrap.
        let two = numeric::parse("2").expect("literal");
        assert!(
            lsn_add(u64::MAX - 1, &two)
                == Err(TypeError::Coded {
                    sqlstate: "22023",
                    message: "pg_lsn out of range".to_string()
                })
        );
        assert!(
            lsn_sub(1, &two)
                == Err(TypeError::Coded {
                    sqlstate: "22023",
                    message: "pg_lsn out of range".to_string()
                })
        );
        // The two NaN messages differ by operation, and neither is the
        // conversion's own wording.
        assert!(
            lsn_add(1, &NumericValue::NaN)
                == Err(TypeError::Domain {
                    sqlstate: "0A000",
                    message: "cannot add NaN to pg_lsn"
                })
        );
        assert!(
            lsn_sub(1, &NumericValue::NaN)
                == Err(TypeError::Domain {
                    sqlstate: "0A000",
                    message: "cannot subtract NaN from pg_lsn"
                })
        );
        assert!(
            lsn_add(1, &NumericValue::Infinity)
                == Err(TypeError::Domain {
                    sqlstate: "0A000",
                    message: "cannot convert infinity to pg_lsn"
                })
        );
    }

    #[test]
    fn lsn_diff_is_signed_and_spans_the_whole_range() {
        assert!(lsn_diff(0x016A_E7F7, 0x016A_E7F8) == numeric::parse("-1").expect("literal"));
        assert!(lsn_diff(0x016A_E7F8, 0x016A_E7F7) == numeric::parse("1").expect("literal"));
        assert!(lsn_diff(u64::MAX, 0) == numeric::parse("18446744073709551615").expect("literal"));
    }
}
