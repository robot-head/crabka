//! The byte-level primitives behind `bytea`'s function surface.
//!
//! Every `bytea` overload here has a `text` counterpart of the same name, and
//! the two differ in exactly one way that matters: **`bytea` is indexed by
//! byte, `text` by character.** `substring('héllo'::text from 2 for 1)` is
//! `é`; `substring('héllo'::bytea from 2 for 1)` is the first of that
//! character's two UTF-8 bytes. Routing a `bytea` through the text
//! implementation therefore does not fail — it silently answers a different
//! question — so the dispatch sites match on [`Datum::Bytea`] before they ever
//! reach a `&str`, and nothing in this module borrows a value as text.
//!
//! The functions declared over `bytea` *alone* (`get_byte`, `set_byte`,
//! `crc32`, `crc32c`) live with the families that own their names, the same way
//! `bit_fn` keeps `get_bit` beside `set_bit`; only their bodies are here.

use crabka_pgtypes::Datum;

use crate::error::ExecError;

/// `bytea_substring` — the byte-indexed `substring(bytea from int [for int])`.
///
/// `start` is 1-based and may be zero or negative, which shifts the window's
/// left edge before the value and shortens it. A `None` count runs to the end.
///
/// PostgreSQL computes the end position in `int32` and treats an overflow as
/// "run to the end of the string" rather than as an error, which is what makes
/// `substring('string'::bytea from 2 for 2147483646)` return `tring` instead of
/// raising. Widening the arithmetic to `i64` reproduces that: the sum no longer
/// overflows, and clamping it to the length gives the same window.
pub(crate) fn substring(bytes: &[u8], start: i64, count: Option<i64>) -> Result<Datum, ExecError> {
    let len = i64::try_from(bytes.len()).unwrap_or(i64::MAX);
    let end = match count {
        None => len + 1,
        Some(count) => {
            if count < 0 {
                return Err(negative_substring_length());
            }
            start.saturating_add(count)
        }
    };
    let lo = start.max(1);
    let hi = end.min(len + 1);
    if lo >= hi {
        return Ok(Datum::Bytea(Vec::new()));
    }
    let (lo, hi) = (
        usize::try_from(lo - 1).expect("clamped to at least 1"),
        usize::try_from(hi - 1).expect("clamped above lo"),
    );
    Ok(Datum::Bytea(bytes[lo..hi].to_vec()))
}

/// `bytea_overlay` — replace `count` bytes of `bytes` from `start` with
/// `replacement`.
///
/// PostgreSQL reports a *substring* error for a non-positive start, because
/// SQL defines `OVERLAY` in terms of `SUBSTRING` and that is the error the
/// definition produces.
pub(crate) fn overlay(
    bytes: &[u8],
    replacement: &[u8],
    start: i64,
    count: i64,
) -> Result<Datum, ExecError> {
    if start <= 0 {
        return Err(negative_substring_length());
    }
    let (Datum::Bytea(prefix), Datum::Bytea(suffix)) = (
        substring(bytes, 1, Some(start - 1))?,
        substring(bytes, start.saturating_add(count), None)?,
    ) else {
        unreachable!("substring of bytea is bytea");
    };
    let mut out = prefix;
    out.extend_from_slice(replacement);
    out.extend_from_slice(&suffix);
    Ok(Datum::Bytea(out))
}

/// `byteapos` — the 1-based byte offset of `needle` in `haystack`, or 0 when it
/// does not occur. An empty needle is at position 1, even in an empty haystack.
pub(crate) fn position(haystack: &[u8], needle: &[u8]) -> i32 {
    if needle.is_empty() {
        return 1;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
        .map_or(0, |found| i32::try_from(found + 1).unwrap_or(i32::MAX))
}

/// Which ends `dobyteatrim` strips.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrimEnds {
    /// `ltrim(bytea, bytea)`.
    Leading,
    /// `rtrim(bytea, bytea)`.
    Trailing,
    /// `btrim(bytea, bytea)`.
    Both,
}

/// `dobyteatrim` — strip every leading and/or trailing byte that occurs in
/// `set`. An empty `set` strips nothing, which is why
/// `btrim(E'\\000trim\\000'::bytea, ''::bytea)` returns its argument unchanged.
pub(crate) fn trim(bytes: &[u8], set: &[u8], ends: TrimEnds) -> Datum {
    if bytes.is_empty() || set.is_empty() {
        return Datum::Bytea(bytes.to_vec());
    }
    let mut window = bytes;
    if ends != TrimEnds::Trailing {
        while let [first, rest @ ..] = window {
            if !set.contains(first) {
                break;
            }
            window = rest;
        }
    }
    if ends != TrimEnds::Leading {
        while let [rest @ .., last] = window {
            if !set.contains(last) {
                break;
            }
            window = rest;
        }
    }
    Datum::Bytea(window.to_vec())
}

/// `crc32` — the IEEE 802.3 CRC-32 that zlib and gzip use, widened to the
/// `bigint` PostgreSQL returns so that the result is never negative.
pub(crate) fn crc32(bytes: &[u8]) -> i64 {
    i64::from(crc(bytes, 0xEDB8_8320))
}

/// `crc32c` — the Castagnoli CRC-32, which differs from [`crc32`] only in its
/// polynomial.
pub(crate) fn crc32c(bytes: &[u8]) -> i64 {
    i64::from(crc(bytes, 0x82F6_3B78))
}

/// The reflected CRC-32 both variants share: initialise to all ones, feed each
/// byte low bit first, and invert the residue.
fn crc(bytes: &[u8], polynomial: u32) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let lsb_set = crc & 1 == 1;
            crc >>= 1;
            if lsb_set {
                crc ^= polynomial;
            }
        }
    }
    !crc
}

/// SQL `LIKE` over bytes, the `bytea` reading of `~~`.
///
/// The pattern language is the text matcher's — `%` for any run, `_` for
/// exactly one — but the unit is the byte, so `_` matches one byte of a
/// multi-byte character rather than the character. PostgreSQL declares no
/// case-insensitive `bytea` form, so there is no folding here.
pub(crate) fn like_match(
    subject: &[u8],
    pattern: &[u8],
    escape: Option<u8>,
) -> Result<bool, ExecError> {
    let (mut si, mut pi) = (0usize, 0usize);
    // The last `%` seen: the pattern index just past it, and the subject index
    // to resume from, advanced by one on each backtrack.
    let mut star: Option<usize> = None;
    let mut star_si = 0usize;
    while si < subject.len() {
        let matched = match pattern.get(pi) {
            Some(byte) if Some(*byte) == escape => {
                let literal = *pattern
                    .get(pi + 1)
                    .ok_or(ExecError::Type(crabka_pgtypes::TypeError::InvalidEscape))?;
                let hit = subject[si] == literal;
                if hit {
                    pi += 2;
                    si += 1;
                }
                hit
            }
            Some(b'%') => {
                pi += 1;
                star = Some(pi);
                star_si = si;
                continue;
            }
            Some(b'_') => {
                pi += 1;
                si += 1;
                true
            }
            Some(byte) => {
                let hit = *byte == subject[si];
                if hit {
                    pi += 1;
                    si += 1;
                }
                hit
            }
            None => false,
        };
        if matched {
            continue;
        }
        // Backtrack to just after the last `%` and let it swallow one more byte.
        let Some(resume) = star else {
            return Ok(false);
        };
        star_si += 1;
        si = star_si;
        pi = resume;
    }
    // Only a run of `%` may remain once the subject is spent.
    while pattern.get(pi) == Some(&b'%') {
        pi += 1;
    }
    Ok(pi == pattern.len())
}

/// 22011, shared by `substring` and `overlay`.
fn negative_substring_length() -> ExecError {
    ExecError::FunctionError {
        sqlstate: "22011",
        message: "negative substring length not allowed".into(),
    }
}

#[cfg(test)]
mod tests;
