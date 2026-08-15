//! `PostgreSQL`'s `bit` and `bit varying` — a packed string of bits.
//!
//! One value type serves both SQL types, because `PostgreSQL`'s are the same
//! `VarBit` struct and the two are binary-coercible to each other in both
//! directions (`pg_cast` gives them `castmethod = 'b'`). What differs is the
//! declared type: `bit(n)` is fixed-length and rejects a length mismatch, while
//! `bit varying(n)` only rejects being too long. The [`BitString::varying`]
//! flag records which spelling produced a value so the output paths and the
//! length coercions can branch on it; comparison, equality and hashing ignore
//! it, exactly as `bitcmp` and `varbitcmp` are the same C function.
//!
//! Ported from `src/backend/utils/adt/varbit.c`. Bit zero is the *most*
//! significant bit of byte zero — the leftmost character of the text form — and
//! the pad bits after the last significant bit are always zero, an invariant
//! several of `PostgreSQL`'s own routines rely on and which this module
//! maintains at every construction point.

use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
};

use crate::error::TypeError;

/// `VARBITMAXLEN` — the longest bit string `PostgreSQL` will build.
pub const MAX_LEN: i32 = i32::MAX - 8 + 1;

/// `MaxAttrSize * BITS_PER_BYTE` — the largest `bit(n)` / `bit varying(n)`
/// length modifier `anybit_typmodin` accepts.
pub const MAX_TYPMOD: i32 = 10 * 1024 * 1024 * 8;

/// A `bit` or `bit varying` value.
#[derive(Debug, Clone)]
pub struct BitString {
    /// `true` for `bit varying`, `false` for `bit`. Read only by the output and
    /// length-coercion paths — never by comparison.
    pub varying: bool,
    /// The number of significant bits.
    len: u32,
    /// The bits, most significant first, `ceil(len / 8)` bytes. Every bit at or
    /// past `len` is zero.
    bytes: Vec<u8>,
}

impl PartialEq for BitString {
    fn eq(&self, other: &Self) -> bool {
        // `varying` is excluded on purpose: `bit` and `bit varying` are
        // binary-coercible in both directions, so `B'101' = '101'::varbit`.
        self.len == other.len && self.bytes == other.bytes
    }
}

impl Eq for BitString {}

impl Hash for BitString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.len.hash(state);
        self.bytes.hash(state);
    }
}

impl PartialOrd for BitString {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BitString {
    /// `bit_cmp`: `memcmp` over the bytes the two have in common, then the
    /// shorter string first. Because the pad bits are zero, this orders
    /// lexicographically by bit — `B'01'` sorts before `B'010'`, which sorts
    /// before `B'1'`.
    fn cmp(&self, other: &Self) -> Ordering {
        let common = self.bytes.len().min(other.bytes.len());
        match self.bytes[..common].cmp(&other.bytes[..common]) {
            Ordering::Equal => self.len.cmp(&other.len),
            order => order,
        }
    }
}

/// The number of bytes a `len`-bit string occupies.
const fn byte_len(len: u32) -> usize {
    len.div_ceil(8) as usize
}

impl BitString {
    /// An all-zero string of `len` bits.
    #[must_use]
    pub fn zeros(varying: bool, len: u32) -> BitString {
        BitString {
            varying,
            len,
            bytes: vec![0; byte_len(len)],
        }
    }

    /// Rebuild a value from its stored parts, rejecting a byte count that does
    /// not match `len`. Pad bits are cleared rather than trusted, so a corrupt
    /// row can never produce a value whose ordering contradicts its text form.
    #[must_use]
    pub fn from_parts(varying: bool, len: u32, mut bytes: Vec<u8>) -> Option<BitString> {
        if bytes.len() != byte_len(len) {
            return None;
        }
        clear_pad(&mut bytes, len);
        Some(BitString {
            varying,
            len,
            bytes,
        })
    }

    /// The number of significant bits.
    #[must_use]
    pub fn len(&self) -> u32 {
        self.len
    }

    /// Whether the string has no bits at all — `B''`, a legal `bit varying`
    /// value.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The packed bits, most significant first.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// `bitoctetlength` — the number of bytes the bits occupy.
    #[must_use]
    pub fn octet_len(&self) -> i32 {
        i32::try_from(self.bytes.len()).unwrap_or(i32::MAX)
    }

    /// The same bits relabelled as `bit` or as `bit varying`. This is the whole
    /// content of `PostgreSQL`'s binary-coercible cast between the two.
    #[must_use]
    pub fn relabel(&self, varying: bool) -> BitString {
        BitString {
            varying,
            len: self.len,
            bytes: self.bytes.clone(),
        }
    }

    /// Bit `index`, counting from zero at the leftmost (most significant) bit.
    #[must_use]
    pub fn bit(&self, index: u32) -> bool {
        if index >= self.len {
            return false;
        }
        let byte = self.bytes[(index / 8) as usize];
        (byte >> (7 - index % 8)) & 1 == 1
    }

    /// `bit_in` / `varbit_in` with no length modifier. A leading `b`/`B` marks a
    /// binary string and `x`/`X` a hexadecimal one; anything else is read as
    /// binary, which is what lets `cast('1001' as bit)` work.
    ///
    /// # Errors
    ///
    /// `22P02` `"x" is not a valid binary digit` / `… hexadecimal digit`, and
    /// `54000` for an input longer than [`MAX_LEN`] bits.
    pub fn parse(text: &str, varying: bool) -> Result<BitString, TypeError> {
        Self::parse_with_typmod(text, None, varying)
    }

    /// `bit_in` / `varbit_in`. `typmod` is the declared length: `bit(n)` demands
    /// exactly `n` bits and `bit varying(n)` at most `n`, and both checks run
    /// *before* the digits are validated, so `'0101Z'::bit(4)` is a length
    /// error rather than a bad-digit one.
    ///
    /// # Errors
    ///
    /// As [`BitString::parse`], plus `22026` `bit string length N does not
    /// match type bit(M)` for a `bit(M)` mismatch and `22001` `bit string too
    /// long for type bit varying(M)` for an over-long `bit varying(M)`.
    pub fn parse_with_typmod(
        text: &str,
        typmod: Option<i32>,
        varying: bool,
    ) -> Result<BitString, TypeError> {
        let (digits, hex) = match text.as_bytes().first() {
            Some(b'b' | b'B') => (&text[1..], false),
            Some(b'x' | b'X') => (&text[1..], true),
            _ => (text, false),
        };
        let bit_len = if hex {
            let nibbles = i64::try_from(digits.len()).unwrap_or(i64::MAX);
            if nibbles > i64::from(MAX_LEN) / 4 {
                return Err(length_limit_exceeded());
            }
            nibbles * 4
        } else {
            i64::try_from(digits.len()).unwrap_or(i64::MAX)
        };
        let bit_len = i32::try_from(bit_len).map_err(|_| length_limit_exceeded())?;
        let declared = typmod.filter(|&n| n > 0);
        match declared {
            Some(n) if varying && bit_len > n => return Err(too_long_for_varbit(n)),
            Some(n) if !varying && bit_len != n => return Err(length_mismatch(bit_len, n)),
            _ => {}
        }
        // A `bit(n)` shorter than `n` is already rejected above, so the declared
        // length only ever widens `bit varying`, which it never does.
        let mut out = BitString::zeros(varying, bit_len.unsigned_abs());
        let mut index: u32 = 0;
        if hex {
            for ch in digits.chars() {
                let value = ch
                    .to_digit(16)
                    .ok_or_else(|| not_a_valid_digit(ch, "hexadecimal"))?;
                for offset in 0..4 {
                    if (value >> (3 - offset)) & 1 == 1 {
                        out.set_bit_unchecked(index + offset, true);
                    }
                }
                index += 4;
            }
        } else {
            for ch in digits.chars() {
                match ch {
                    '0' => {}
                    '1' => out.set_bit_unchecked(index, true),
                    _ => return Err(not_a_valid_digit(ch, "binary")),
                }
                index += 1;
            }
        }
        Ok(out)
    }

    /// `bit_out` / `varbit_out` — one character per bit, most significant first.
    #[must_use]
    pub fn to_text(&self) -> String {
        (0..self.len)
            .map(|i| if self.bit(i) { '1' } else { '0' })
            .collect()
    }

    /// `bit()` — coerce to a declared `bit(len)`. An explicit cast silently
    /// zero-pads on the right or truncates; an implicit or assignment cast
    /// rejects any mismatch, which is why `INSERT INTO t(bit(11)) VALUES
    /// (B'10')` fails where `B'10'::bit(11)` succeeds.
    ///
    /// # Errors
    ///
    /// `22026` `bit string length N does not match type bit(M)` when the
    /// coercion is not explicit and the lengths differ.
    pub fn coerce_bit(&self, len: i32, explicit: bool) -> Result<BitString, TypeError> {
        if len <= 0 || len > MAX_LEN || u32::try_from(len).is_ok_and(|n| n == self.len) {
            return Ok(self.relabel(false));
        }
        if !explicit {
            return Err(length_mismatch(
                i32::try_from(self.len).unwrap_or(i32::MAX),
                len,
            ));
        }
        Ok(self.resize(false, len.unsigned_abs()))
    }

    /// `varbit()` — coerce to a declared `bit varying(len)`. An explicit cast
    /// truncates; anything else rejects a value that is too long. A value
    /// shorter than the declared maximum is left alone.
    ///
    /// # Errors
    ///
    /// `22001` `bit string too long for type bit varying(M)` when the coercion
    /// is not explicit and the value is longer than the declared maximum.
    pub fn coerce_varbit(&self, len: i32, explicit: bool) -> Result<BitString, TypeError> {
        if len <= 0 || u32::try_from(len).is_ok_and(|n| n >= self.len) {
            return Ok(self.relabel(true));
        }
        if !explicit {
            return Err(too_long_for_varbit(len));
        }
        Ok(self.resize(true, len.unsigned_abs()))
    }

    /// `bitcat` — concatenation, whose result is `bit varying`.
    ///
    /// # Errors
    ///
    /// `54000` when the two together would exceed [`MAX_LEN`] bits.
    pub fn concat(&self, other: &BitString) -> Result<BitString, TypeError> {
        let len = u64::from(self.len) + u64::from(other.len);
        if len > u64::from(MAX_LEN.unsigned_abs()) {
            return Err(length_limit_exceeded());
        }
        let mut out = BitString::zeros(true, u32::try_from(len).unwrap_or(u32::MAX));
        out.copy_from(0, self, 0, self.len);
        out.copy_from(self.len, other, 0, other.len);
        Ok(out)
    }

    /// `bit_and` / `bit_or` / `bit_xor` — element-wise over two equal-length
    /// strings. `PostgreSQL` names the operation in the error, so the caller
    /// passes the spelling it wants: `AND`, `OR` or `XOR`.
    ///
    /// # Errors
    ///
    /// `22026` `cannot AND bit strings of different sizes` (and the `OR`/`XOR`
    /// wordings) when the lengths differ.
    pub fn bitwise(&self, other: &BitString, op: BitwiseOp) -> Result<BitString, TypeError> {
        if self.len != other.len {
            return Err(TypeError::Coded {
                sqlstate: "22026",
                message: format!("cannot {} bit strings of different sizes", op.name()),
            });
        }
        let bytes = self
            .bytes
            .iter()
            .zip(&other.bytes)
            .map(|(&a, &b)| match op {
                BitwiseOp::And => a & b,
                BitwiseOp::Or => a | b,
                BitwiseOp::Xor => a ^ b,
            })
            .collect();
        Ok(BitString {
            // The operators are declared over `bit`, so `varbit & varbit` is a
            // `bit` after the implicit coercion, exactly as in PostgreSQL.
            varying: false,
            len: self.len,
            bytes,
        })
    }

    /// `bitnot` — every bit inverted, the length unchanged.
    #[must_use]
    pub fn not(&self) -> BitString {
        let mut bytes: Vec<u8> = self.bytes.iter().map(|b| !b).collect();
        clear_pad(&mut bytes, self.len);
        BitString {
            varying: false,
            len: self.len,
            bytes,
        }
    }

    /// `bitshiftleft` — towards the beginning of the string, zero-filling on the
    /// right and keeping the length. A negative count shifts the other way.
    #[must_use]
    pub fn shift_left(&self, count: i32) -> BitString {
        if count < 0 {
            return self.shift_right(negate_shift(count));
        }
        let mut out = BitString::zeros(false, self.len);
        let count = count.unsigned_abs();
        if count < self.len {
            out.copy_from(0, self, count, self.len - count);
        }
        out
    }

    /// `bitshiftright` — towards the end of the string, zero-filling on the left
    /// and keeping the length. A negative count shifts the other way.
    #[must_use]
    pub fn shift_right(&self, count: i32) -> BitString {
        if count < 0 {
            return self.shift_left(negate_shift(count));
        }
        let mut out = BitString::zeros(false, self.len);
        let count = count.unsigned_abs();
        if count < self.len {
            out.copy_from(count, self, 0, self.len - count);
        }
        out
    }

    /// `bitsubstring` — the SQL `SUBSTRING(b FROM s FOR l)`, one-based. `length`
    /// of `None` runs to the end of the string; a negative length is 22011,
    /// while a start before the string simply clamps.
    ///
    /// # Errors
    ///
    /// `22011` `negative substring length not allowed`.
    pub fn substring(&self, start: i32, length: Option<i32>) -> Result<BitString, TypeError> {
        let bit_len = i32::try_from(self.len).unwrap_or(i32::MAX);
        let first = start.max(1);
        let end = match length {
            None => bit_len.saturating_add(1),
            Some(l) if l < 0 => {
                return Err(TypeError::Coded {
                    sqlstate: "22011",
                    message: "negative substring length not allowed".into(),
                });
            }
            // `S + L` overflowing means the substring runs to the end, which is
            // how `SUBSTRING(b FROM 2 FOR 2147483646)` returns the whole tail
            // rather than erroring.
            Some(l) => start
                .checked_add(l)
                .map_or_else(|| bit_len.saturating_add(1), |e| e.min(bit_len + 1)),
        };
        if first > bit_len || end <= first {
            return Ok(BitString::zeros(false, 0));
        }
        let count = (end - first).unsigned_abs();
        let from = (first - 1).unsigned_abs();
        let mut out = BitString::zeros(false, count);
        out.copy_from(0, self, from, count);
        Ok(out)
    }

    /// `bit_overlay` — replace `length` bits of `self` starting at `start` with
    /// `replacement`, defined by the standard as substring + concatenation, and
    /// implemented that way here so the edge cases agree.
    ///
    /// # Errors
    ///
    /// `22011` for a start at or below zero, `22003` `integer out of range`
    /// when `start + length` overflows, and whatever [`BitString::concat`]
    /// raises.
    pub fn overlay(
        &self,
        replacement: &BitString,
        start: i32,
        length: Option<i32>,
    ) -> Result<BitString, TypeError> {
        let length = length.unwrap_or_else(|| i32::try_from(replacement.len).unwrap_or(i32::MAX));
        if start <= 0 {
            return Err(TypeError::Coded {
                sqlstate: "22011",
                message: "negative substring length not allowed".into(),
            });
        }
        let tail_start = start.checked_add(length).ok_or(TypeError::Overflow)?;
        let head = self.substring(1, Some(start - 1))?;
        let tail = self.substring(tail_start, None)?;
        // `overlay(bit, bit, int[, int])` is declared to return `bit`, even
        // though its body is the `bit varying`-valued concatenation.
        Ok(head.concat(replacement)?.concat(&tail)?.relabel(false))
    }

    /// `bitposition` — the one-based index of `needle` in `self`, or zero when
    /// it does not occur. An empty needle is 1, except in an empty haystack,
    /// which is 0 whatever the needle.
    #[must_use]
    pub fn position(&self, needle: &BitString) -> i32 {
        if self.len == 0 || needle.len > self.len {
            return 0;
        }
        for start in 0..=(self.len - needle.len) {
            if (0..needle.len).all(|i| self.bit(start + i) == needle.bit(i)) {
                return i32::try_from(start + 1).unwrap_or(i32::MAX);
            }
        }
        0
    }

    /// `bitgetbit` — bit `index`, counting from zero at the left.
    ///
    /// # Errors
    ///
    /// `2202E` `bit index N out of valid range (0..M)`.
    pub fn get_bit(&self, index: i32) -> Result<i32, TypeError> {
        let len = i32::try_from(self.len).unwrap_or(i32::MAX);
        if index < 0 || index >= len {
            return Err(bit_index_out_of_range(index, len));
        }
        Ok(i32::from(self.bit(index.unsigned_abs())))
    }

    /// `bitsetbit` — a copy with bit `index` set to `value`, which must be 0 or
    /// 1.
    ///
    /// # Errors
    ///
    /// `2202E` for an index out of range, `22023` `new bit must be 0 or 1` for
    /// any other new value.
    pub fn set_bit(&self, index: i32, value: i32) -> Result<BitString, TypeError> {
        let len = i32::try_from(self.len).unwrap_or(i32::MAX);
        if index < 0 || index >= len {
            return Err(bit_index_out_of_range(index, len));
        }
        if value != 0 && value != 1 {
            return Err(TypeError::Coded {
                sqlstate: "22023",
                message: "new bit must be 0 or 1".into(),
            });
        }
        let mut out = self.clone();
        out.varying = false;
        out.set_bit_unchecked(index.unsigned_abs(), value == 1);
        Ok(out)
    }

    /// `bit_bit_count` — how many bits are set.
    #[must_use]
    pub fn count_ones(&self) -> i64 {
        self.bytes.iter().map(|b| i64::from(b.count_ones())).sum()
    }

    /// `bittoint4` — the bits read as a big-endian two's-complement integer,
    /// right-aligned. More than 32 bits is 22003.
    ///
    /// # Errors
    ///
    /// `22003` `integer out of range` for more than 32 bits.
    pub fn to_int4(&self) -> Result<i32, TypeError> {
        if self.len > 32 {
            return Err(TypeError::Overflow);
        }
        let mut value: u32 = 0;
        for i in 0..self.len {
            value = (value << 1) | u32::from(self.bit(i));
        }
        Ok(value.cast_signed())
    }

    /// `bittoint8` — as [`BitString::to_int4`], over 64 bits.
    ///
    /// # Errors
    ///
    /// `22003` `bigint out of range` for more than 64 bits.
    pub fn to_int8(&self) -> Result<i64, TypeError> {
        if self.len > 64 {
            return Err(TypeError::OutOfRange {
                message: "bigint out of range".into(),
            });
        }
        let mut value: u64 = 0;
        for i in 0..self.len {
            value = (value << 1) | u64::from(self.bit(i));
        }
        Ok(value.cast_signed())
    }

    /// `bitfromint4` / `bitfromint8` — the low `len` bits of `value`'s two's
    /// complement, sign-extended when `len` exceeds the source width. An absent
    /// or out-of-range length modifier means `bit(1)`, not the source width.
    #[must_use]
    pub fn from_int(value: i64, typmod: Option<i32>) -> BitString {
        let len = match typmod {
            Some(n) if n > 0 && n <= MAX_LEN => n.unsigned_abs(),
            _ => 1,
        };
        let mut out = BitString::zeros(false, len);
        for i in 0..len {
            // Bit `i` from the left is bit `len - 1 - i` of the value; an
            // arithmetic shift past bit 63 keeps yielding the sign bit.
            let place = len - 1 - i;
            let bit = if place >= 64 {
                value < 0
            } else {
                (value >> place) & 1 == 1
            };
            if bit {
                out.set_bit_unchecked(i, true);
            }
        }
        out
    }

    /// `bit_send` / `varbit_send` — a big-endian `int32` bit count followed by
    /// the packed bytes.
    #[must_use]
    pub fn to_binary(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.bytes.len());
        out.extend_from_slice(&i32::try_from(self.len).unwrap_or(i32::MAX).to_be_bytes());
        out.extend_from_slice(&self.bytes);
        out
    }

    /// `bit_recv` / `varbit_recv`, including their rejection of a bit count that
    /// does not match the byte count supplied.
    ///
    /// # Errors
    ///
    /// `22P03` `invalid length in external bit string` for a truncated message,
    /// a negative or over-long bit count, or a byte count that disagrees with
    /// it.
    pub fn from_binary(bytes: &[u8], varying: bool) -> Result<BitString, TypeError> {
        let invalid = || TypeError::Coded {
            sqlstate: "22P03",
            message: "invalid length in external bit string".into(),
        };
        let (head, rest) = bytes.split_first_chunk::<4>().ok_or_else(invalid)?;
        let len = i32::from_be_bytes(*head);
        if !(0..=MAX_LEN).contains(&len) {
            return Err(invalid());
        }
        let len = len.unsigned_abs();
        if rest.len() != byte_len(len) {
            return Err(invalid());
        }
        BitString::from_parts(varying, len, rest.to_vec()).ok_or_else(invalid)
    }

    /// Zero-pad or truncate to `len` bits.
    fn resize(&self, varying: bool, len: u32) -> BitString {
        let mut out = BitString::zeros(varying, len);
        let keep = len.min(self.len);
        out.copy_from(0, self, 0, keep);
        out
    }

    /// Copy `count` bits from `src` starting at `src_from` into `self` starting
    /// at `dst_from`.
    fn copy_from(&mut self, dst_from: u32, src: &BitString, src_from: u32, count: u32) {
        for i in 0..count {
            if src.bit(src_from + i) {
                self.set_bit_unchecked(dst_from + i, true);
            }
        }
    }

    /// Set bit `index`, which the caller has already established is in range.
    fn set_bit_unchecked(&mut self, index: u32, value: bool) {
        let byte = &mut self.bytes[(index / 8) as usize];
        let mask = 1u8 << (7 - index % 8);
        if value {
            *byte |= mask;
        } else {
            *byte &= !mask;
        }
    }
}

/// Which of `PostgreSQL`'s three element-wise bit operations to apply. They
/// differ only in the operation and in the word their length-mismatch error
/// uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitwiseOp {
    And,
    Or,
    Xor,
}

impl BitwiseOp {
    fn name(self) -> &'static str {
        match self {
            BitwiseOp::And => "AND",
            BitwiseOp::Or => "OR",
            BitwiseOp::Xor => "XOR",
        }
    }
}

/// Clear every bit at or past `len`, restoring the zero-pad invariant after an
/// operation that could have set them.
fn clear_pad(bytes: &mut [u8], len: u32) {
    let pad = (8 - len % 8) % 8;
    if pad > 0
        && let Some(last) = bytes.last_mut()
    {
        *last &= !0u8 << pad;
    }
}

/// `bitshiftleft`/`bitshiftright` clamp the count before negating it so that
/// `i32::MIN` does not overflow.
fn negate_shift(count: i32) -> i32 {
    -count.max(-MAX_LEN)
}

fn length_limit_exceeded() -> TypeError {
    TypeError::Coded {
        sqlstate: "54000",
        message: format!("bit string length exceeds the maximum allowed ({MAX_LEN})"),
    }
}

fn length_mismatch(actual: i32, declared: i32) -> TypeError {
    TypeError::Coded {
        sqlstate: "22026",
        message: format!("bit string length {actual} does not match type bit({declared})"),
    }
}

fn too_long_for_varbit(declared: i32) -> TypeError {
    TypeError::Coded {
        sqlstate: "22001",
        message: format!("bit string too long for type bit varying({declared})"),
    }
}

fn not_a_valid_digit(ch: char, kind: &'static str) -> TypeError {
    TypeError::Coded {
        sqlstate: "22P02",
        message: format!("\"{ch}\" is not a valid {kind} digit"),
    }
}

fn bit_index_out_of_range(index: i32, len: i32) -> TypeError {
    TypeError::Coded {
        sqlstate: "2202E",
        message: format!("bit index {index} out of valid range (0..{})", len - 1),
    }
}

/// `length for type bit must be at least 1` / `… cannot exceed …` — the
/// `anybit_typmodin` range check, shared by `bit` and `bit varying`.
///
/// # Errors
///
/// `22023` for a length below 1 or above [`MAX_TYPMOD`].
pub fn check_typmod(len: i32, type_name: &str) -> Result<(), TypeError> {
    if len < 1 {
        return Err(TypeError::Coded {
            sqlstate: "22023",
            message: format!("length for type {type_name} must be at least 1"),
        });
    }
    if len > MAX_TYPMOD {
        return Err(TypeError::Coded {
            sqlstate: "22023",
            message: format!("length for type {type_name} cannot exceed {MAX_TYPMOD}"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{BitString, BitwiseOp, MAX_TYPMOD, check_typmod};

    /// The text form round-trips through both input notations, and the length
    /// modifier is what the *input function* enforces — a mismatch is an error,
    /// never a silent pad.
    #[test]
    fn parses_binary_and_hexadecimal_notations() {
        let cases: &[(&str, Option<i32>, bool, &str)] = &[
            ("", None, false, ""),
            ("0", None, false, "0"),
            ("10101", None, false, "10101"),
            ("b10101", None, false, "10101"),
            ("B10101", None, false, "10101"),
            ("x1a", None, false, "00011010"),
            ("X1A", None, false, "00011010"),
            ("xff", None, false, "11111111"),
            // A bare marker is the empty string in both notations.
            ("b", None, false, ""),
            ("x", None, false, ""),
            ("0101", Some(4), false, "0101"),
            ("x1a", Some(8), false, "00011010"),
            // `bit varying(n)` accepts anything up to `n`.
            ("0101", Some(11), true, "0101"),
            ("", Some(11), true, ""),
        ];
        for &(input, typmod, varying, expected) in cases {
            let value = BitString::parse_with_typmod(input, typmod, varying)
                .unwrap_or_else(|error| panic!("{input:?} with {typmod:?}: {error}"));
            assert!(value.to_text() == expected, "{input:?}");
            assert!(value.varying == varying);
            assert!(value.len() as usize == expected.len());
        }
    }

    /// The length check runs BEFORE the digits are validated, so `'0101Z'` is a
    /// length error at `bit(4)` and a bad-digit one at `bit(5)`.
    #[test]
    fn rejects_bad_digits_and_length_mismatches() {
        let cases: &[(&str, Option<i32>, bool, &str, &str)] = &[
            (
                "0101Z",
                Some(4),
                false,
                "22026",
                "bit string length 5 does not match type bit(4)",
            ),
            (
                "0101Z",
                Some(5),
                false,
                "22P02",
                "\"Z\" is not a valid binary digit",
            ),
            (
                " 0",
                None,
                false,
                "22P02",
                "\" \" is not a valid binary digit",
            ),
            (
                "0 ",
                None,
                false,
                "22P02",
                "\" \" is not a valid binary digit",
            ),
            (
                "x01010Z01",
                None,
                false,
                "22P02",
                "\"Z\" is not a valid hexadecimal digit",
            ),
            (
                "01010001",
                Some(10),
                false,
                "22026",
                "bit string length 8 does not match type bit(10)",
            ),
            (
                "101011111010",
                Some(11),
                true,
                "22001",
                "bit string too long for type bit varying(11)",
            ),
        ];
        for &(input, typmod, varying, sqlstate, message) in cases {
            let error = BitString::parse_with_typmod(input, typmod, varying).expect_err(input);
            assert!(error.sqlstate() == sqlstate, "{input:?}");
            assert!(error.to_string() == message, "{input:?}");
        }
    }

    /// The two length coercions differ in both directions: `bit(n)` pads and
    /// truncates under an explicit cast and rejects any mismatch otherwise,
    /// while `bit varying(n)` only ever objects to being too long.
    #[test]
    fn length_coercions_pad_truncate_or_reject() {
        let value = BitString::parse("1011", false).expect("valid");
        assert!(value.coerce_bit(8, true).expect("pads").to_text() == "10110000");
        assert!(value.coerce_bit(2, true).expect("truncates").to_text() == "10");
        assert!(value.coerce_bit(4, false).expect("exact").to_text() == "1011");
        // No modifier only relabels, whichever direction.
        assert!(!value.coerce_bit(-1, false).expect("relabels").varying);
        assert!(value.coerce_varbit(-1, false).expect("relabels").varying);

        let error = value.coerce_bit(11, false).expect_err("length mismatch");
        assert!(error.sqlstate() == "22026");
        assert!(error.to_string() == "bit string length 4 does not match type bit(11)");

        assert!(value.coerce_varbit(2, true).expect("truncates").to_text() == "10");
        assert!(value.coerce_varbit(11, false).expect("fits").to_text() == "1011");
        let error = value.coerce_varbit(2, false).expect_err("too long");
        assert!(error.sqlstate() == "22001");
        assert!(error.to_string() == "bit string too long for type bit varying(2)");
    }

    #[test]
    fn concatenation_shifts_across_the_byte_boundary() {
        let cases: &[(&str, &str, &str)] = &[
            ("", "", ""),
            ("", "11011000000", "11011000000"),
            ("0", "00000000000", "000000000000"),
            ("010101", "00000000000", "01010100000000000"),
            ("01010101010", "01010101010", "0101010101001010101010"),
        ];
        for &(a, b, expected) in cases {
            let left = BitString::parse(a, true).expect("valid");
            let right = BitString::parse(b, false).expect("valid");
            let joined = left.concat(&right).expect("within the limit");
            assert!(joined.to_text() == expected, "{a:?} || {b:?}");
            // `bitcat` is declared over `bit varying`, so its result is one.
            assert!(joined.varying);
        }
    }

    #[test]
    fn bitwise_operations_require_equal_lengths() {
        let a = BitString::parse("x0F00", false).expect("valid");
        let b = BitString::parse("x1000", false).expect("valid");
        assert!(a.bitwise(&b, BitwiseOp::And).expect("same size").to_text() == "0000000000000000");
        assert!(a.bitwise(&b, BitwiseOp::Or).expect("same size").to_text() == "0001111100000000");
        assert!(a.bitwise(&b, BitwiseOp::Xor).expect("same size").to_text() == "0001111100000000");
        assert!(a.not().to_text() == "1111000011111111");

        let short = BitString::parse("10", false).expect("valid");
        for (op, word) in [
            (BitwiseOp::And, "AND"),
            (BitwiseOp::Or, "OR"),
            (BitwiseOp::Xor, "XOR"),
        ] {
            let error = a.bitwise(&short, op).expect_err("different sizes");
            assert!(error.sqlstate() == "22026");
            assert!(error.to_string() == format!("cannot {word} bit strings of different sizes"));
        }
    }

    /// Shifts keep the length and zero-fill; a negative count reverses the
    /// direction, and `i32::MIN` must not overflow on negation.
    #[test]
    fn shifts_keep_the_length() {
        let value = BitString::parse("1101100000000000", false).expect("valid");
        let cases: &[(i32, &str, &str)] = &[
            (0, "1101100000000000", "1101100000000000"),
            (1, "1011000000000000", "0110110000000000"),
            (8, "0000000000000000", "0000000011011000"),
            (16, "0000000000000000", "0000000000000000"),
            (100, "0000000000000000", "0000000000000000"),
            (-1, "0110110000000000", "1011000000000000"),
            (i32::MIN, "0000000000000000", "0000000000000000"),
            (i32::MAX, "0000000000000000", "0000000000000000"),
        ];
        for &(count, left, right) in cases {
            assert!(value.shift_left(count).to_text() == left, "<< {count}");
            assert!(value.shift_right(count).to_text() == right, ">> {count}");
        }
    }

    /// `SUBSTRING(b FROM s FOR l)` clamps a start before the string, treats an
    /// overflowing `s + l` as "to the end", and rejects a negative length.
    #[test]
    fn substring_clamps_and_overflows_to_the_end() {
        let value = BitString::parse("01010101", false).expect("valid");
        let cases: &[(i32, Option<i32>, &str)] = &[
            (2, Some(4), "1010"),
            (7, Some(13), "01"),
            (6, None, "101"),
            (2, Some(2_147_483_646), "1010101"),
            (-10, Some(2_147_483_646), "01010101"),
            (0, Some(6), "01010"),
            (11, Some(6), ""),
            (1, Some(0), ""),
            (1, Some(11), "01010101"),
        ];
        for &(start, length, expected) in cases {
            let out = value
                .substring(start, length)
                .unwrap_or_else(|error| panic!("{start},{length:?}: {error}"));
            assert!(out.to_text() == expected, "{start},{length:?}");
            // `bitsubstr` is declared to return `bit`.
            assert!(!out.varying);
        }
        let error = value
            .substring(-10, Some(-2_147_483_646))
            .expect_err("negative length");
        assert!(error.sqlstate() == "22011");
        assert!(error.to_string() == "negative substring length not allowed");
    }

    #[test]
    fn overlay_replaces_by_substring_and_concatenation() {
        let value = BitString::parse("0101011100", false).expect("valid");
        let placing = BitString::parse("001", false).expect("valid");
        let cases: &[(i32, Option<i32>, &str)] = &[
            (2, Some(3), "0001011100"),
            (6, None, "0101000100"),
            (11, None, "0101011100001"),
            (20, None, "0101011100001"),
        ];
        for &(start, length, expected) in cases {
            let out = value
                .overlay(&placing, start, length)
                .unwrap_or_else(|error| panic!("{start},{length:?}: {error}"));
            assert!(out.to_text() == expected, "{start},{length:?}");
        }
        let error = value.overlay(&placing, 0, None).expect_err("start <= 0");
        assert!(error.sqlstate() == "22011");
        assert!(error.to_string() == "negative substring length not allowed");
    }

    /// `POSITION` is one-based; an empty needle is 1 except in an empty
    /// haystack, which is 0 whatever the needle.
    #[test]
    fn position_finds_the_first_occurrence() {
        let cases: &[(&str, &str, i32)] = &[
            ("0000101", "1010", 0),
            ("00001010", "1010", 5),
            ("00000101", "1010", 0),
            ("000001010", "1010", 6),
            ("00001010", "", 1),
            ("", "0", 0),
            ("", "", 0),
            ("001011011011011000", "101101", 3),
            ("001011011011010", "10110110", 3),
            ("001011011011011", "1011011011011", 3),
            ("00001011011011011", "1011011011011", 5),
            ("0000011101011111010110", "111010110", 14),
            (
                "000000000011101011111010110",
                "000000000011101011111010110",
                1,
            ),
            (
                "000000000011101011111010110",
                "00000000011101011111010110",
                2,
            ),
            (
                "000000000011101011111010110",
                "0000000000011101011111010110",
                0,
            ),
        ];
        for &(haystack, needle, expected) in cases {
            let haystack = BitString::parse(haystack, false).expect("valid");
            let needle = BitString::parse(needle, false).expect("valid");
            assert!(
                haystack.position(&needle) == expected,
                "{} in {}",
                needle.to_text(),
                haystack.to_text()
            );
        }
    }

    /// Bit zero is the LEFTMOST bit, the opposite of `bytea`'s convention.
    #[test]
    fn get_and_set_bit_index_from_the_left() {
        let value = BitString::parse("0101011000100", false).expect("valid");
        assert!(value.get_bit(10).expect("in range") == 1);
        assert!(value.get_bit(0).expect("in range") == 0);
        assert!(value.get_bit(1).expect("in range") == 1);

        let wider = BitString::parse("0101011000100100", false).expect("valid");
        assert!(wider.set_bit(15, 1).expect("in range").to_text() == "0101011000100101");
        assert!(wider.set_bit(1, 0).expect("in range").to_text() == "0001011000100100");
        for index in [-1, 16, i32::MAX] {
            let error = wider.set_bit(index, 1).expect_err("out of range");
            assert!(error.sqlstate() == "2202E");
            assert!(error.to_string() == format!("bit index {index} out of valid range (0..15)"));
            let error = wider.get_bit(index).expect_err("out of range");
            assert!(error.to_string() == format!("bit index {index} out of valid range (0..15)"));
        }
        let error = wider.set_bit(0, 2).expect_err("not a bit");
        assert!(error.sqlstate() == "22023");
        assert!(error.to_string() == "new bit must be 0 or 1");
    }

    #[test]
    fn counts_bits_and_bytes() {
        let cases: &[(&str, i64, i32, u32)] = &[
            ("0101011100", 5, 2, 10),
            ("1111111111", 10, 2, 10),
            ("", 0, 0, 0),
            ("1", 1, 1, 1),
            ("10101010", 4, 1, 8),
            ("101010101", 5, 2, 9),
        ];
        for &(text, ones, octets, len) in cases {
            let value = BitString::parse(text, false).expect("valid");
            assert!(value.count_ones() == ones, "{text}");
            assert!(value.octet_len() == octets, "{text}");
            assert!(value.len() == len, "{text}");
        }
    }

    /// The integer conversions read the bits right-aligned as two's complement,
    /// and sign-extend on the way back.
    #[test]
    fn converts_to_and_from_the_integer_types() {
        let cases: &[(&str, i32, i64)] = &[
            ("", 0, 0),
            ("1010", 10, 10),
            ("11111111111111111111111111111111", -1, 4_294_967_295),
            ("0", 0, 0),
        ];
        for &(text, as_int4, as_int8) in cases {
            let value = BitString::parse(text, false).expect("valid");
            assert!(value.to_int4().expect("fits") == as_int4, "{text}");
            assert!(value.to_int8().expect("fits") == as_int8, "{text}");
        }
        let wide = BitString::parse(&"1".repeat(33), false).expect("valid");
        let error = wide.to_int4().expect_err("too wide");
        assert!(error.sqlstate() == "22003");
        assert!(error.to_string() == "integer out of range");
        let wider = BitString::parse(&"1".repeat(65), false).expect("valid");
        let error = wider.to_int8().expect_err("too wide");
        assert!(error.to_string() == "bigint out of range");

        let from: &[(i64, Option<i32>, &str)] = &[
            (5, Some(4), "0101"),
            (10, Some(8), "00001010"),
            (-1, Some(8), "11111111"),
            (-1, Some(40), "1111111111111111111111111111111111111111"),
            (5, Some(40), "0000000000000000000000000000000000000101"),
            // An absent or out-of-range modifier means `bit(1)`, not the
            // source's own width.
            (5, None, "1"),
            (5, Some(0), "1"),
        ];
        for &(value, typmod, expected) in from {
            assert!(
                BitString::from_int(value, typmod).to_text() == expected,
                "{value} at {typmod:?}"
            );
        }
    }

    /// `bitcmp` is lexicographic by bit, with the shorter string first on a
    /// common prefix — and it ignores which of the two SQL types produced a
    /// value, because `PostgreSQL` routes both through the same comparison.
    #[test]
    fn ordering_is_lexicographic_and_ignores_the_spelling() {
        let mut values: Vec<BitString> = ["1", "01", "010", "0", "", "10", "00000000", "000000001"]
            .iter()
            .map(|text| BitString::parse(text, false).expect("valid"))
            .collect();
        values.sort();
        let sorted: Vec<String> = values.iter().map(BitString::to_text).collect();
        assert!(sorted == vec!["", "0", "00000000", "000000001", "01", "010", "1", "10"]);

        let fixed = BitString::parse("101", false).expect("valid");
        let varying = BitString::parse("101", true).expect("valid");
        assert!(fixed == varying);
        assert!(fixed.cmp(&varying) == std::cmp::Ordering::Equal);
    }

    #[test]
    fn binary_round_trips_and_rejects_a_bad_length() {
        for text in ["", "1", "10101", "x1a", "xffff"] {
            let value = BitString::parse(text, false).expect("valid");
            let encoded = value.to_binary();
            let decoded = BitString::from_binary(&encoded, false).expect("round trips");
            assert!(decoded == value, "{text}");
        }
        // A bit count that disagrees with the byte count is 22P03.
        let error = BitString::from_binary(&[0, 0, 0, 16, 0xff], false).expect_err("short");
        assert!(error.sqlstate() == "22P03");
        assert!(error.to_string() == "invalid length in external bit string");
        assert!(BitString::from_binary(&[0, 0], false).is_err());
    }

    #[test]
    fn typmod_range_is_checked() {
        assert!(check_typmod(1, "bit").is_ok());
        assert!(check_typmod(MAX_TYPMOD, "bit").is_ok());
        let error = check_typmod(0, "bit").expect_err("too small");
        assert!(error.sqlstate() == "22023");
        assert!(error.to_string() == "length for type bit must be at least 1");
        let error = check_typmod(MAX_TYPMOD + 1, "varbit").expect_err("too large");
        assert!(error.to_string() == format!("length for type varbit cannot exceed {MAX_TYPMOD}"));
    }

    /// The pad bits after the last significant one are always zero, which is
    /// what makes the byte-wise comparison agree with the bit-wise text form.
    #[test]
    fn pad_bits_stay_zero_through_every_operation() {
        let value = BitString::parse("101", false).expect("valid");
        let zero_padded = |value: &BitString| {
            let pad = value.bytes().len() * 8 - value.len() as usize;
            pad == 0 || value.bytes().last().copied().unwrap_or(0) & ((1u8 << pad) - 1) == 0
        };
        assert!(zero_padded(&value.not()));
        assert!(zero_padded(&value.shift_left(1)));
        assert!(zero_padded(&value.shift_right(1)));
        assert!(zero_padded(&value.substring(2, None).expect("valid")));
        assert!(zero_padded(&value.concat(&value).expect("valid")));
        assert!(zero_padded(&value.set_bit(2, 1).expect("valid")));
        assert!(zero_padded(&BitString::from_int(-1, Some(3))));
        // `from_parts` clears them rather than trusting the caller.
        let salvaged = BitString::from_parts(false, 3, vec![0b1010_1111]).expect("3 bits");
        assert!(salvaged.to_text() == "101");
        assert!(salvaged.bytes() == [0b1010_0000]);
        assert!(BitString::from_parts(false, 3, vec![0, 0]).is_none());
    }
}
