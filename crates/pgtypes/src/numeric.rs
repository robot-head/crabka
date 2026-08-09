//! SP32: arbitrary-precision exact `numeric` / `decimal` (OID 1700).
//! `bigdecimal::BigDecimal` backs a finite value; [`NumericValue`] adds the three
//! special values PostgreSQL 14+ supports (`NaN`, `Infinity`, `-Infinity`). This
//! module is the value layer for numeric: parsing, PostgreSQL-faithful text +
//! binary output, the arithmetic scale rules (`select_div_scale` for
//! division/AVG), rounding, `numeric(p,s)` typmod enforcement, and the casts
//! to/from the other types.
//!
//! Invariant: every finite numeric `Datum` is **canonical**. Its display scale
//! (dscale) is `>= 0`, as in PostgreSQL (a literal like `1e3` parses to scale
//! 0, not the negative scale `bigdecimal` would otherwise keep).

#![expect(
    clippy::pedantic,
    reason = "vendored PostgreSQL-compatible numeric semantics kept structurally close to donor"
)]

use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
};

use bigdecimal::{BigDecimal, RoundingMode, ToPrimitive};

use crate::TypeError;

/// PostgreSQL `numeric` type OID.
pub const OID: u32 = 1700;

/// PostgreSQL division/AVG significant-digit floor (`NUMERIC_MIN_SIG_DIGITS`) and
/// the base-10000 digit width (`DEC_DIGITS`).
const MIN_SIG_DIGITS: i64 = 16;
const DEC_DIGITS: i64 = 4;
const MAX_DISPLAY_SCALE: i64 = 1000;

/// PostgreSQL's hard numeric-format limits: at most `131072` digits before the
/// decimal point (leading-digit weight ≤ `131071`) and `16383` after it. A value
/// outside these "overflows numeric format". PostgreSQL rejects it, and this
/// crate rejects it too. That ALSO bounds materialization: a literal like
/// `8e88888888` would otherwise expand to ~88M digits and OOM, as the
/// `decode_row` fuzzer found.
const MAX_WEIGHT: i64 = 131071;
const MAX_DSCALE: i64 = 16383;

/// Optional `numeric(precision, scale)` type modifier. Absent = unconstrained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Typmod {
    pub precision: u16,
    pub scale: u16,
}

/// A PostgreSQL `numeric` value: an exact decimal, or one of the three special
/// values `numeric` has carried since PostgreSQL 14.
///
/// Equality is by VALUE for finite operands (`1.0 == 1.00`, as `bigdecimal`'s
/// `PartialEq` already does) and, matching PostgreSQL's `numeric_eq`, `NaN` is
/// equal to itself. The total order is PostgreSQL's btree order:
/// `-Infinity < finite < Infinity < NaN`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NumericValue {
    /// An exact decimal with a display scale.
    Finite(BigDecimal),
    /// PostgreSQL's `NaN`: equal to itself, greater than every other numeric.
    NaN,
    /// `+Infinity`.
    Infinity,
    /// `-Infinity`.
    NegInfinity,
}

impl NumericValue {
    /// The finite decimal behind this value, or `None` for a special.
    #[must_use]
    pub fn as_finite(&self) -> Option<&BigDecimal> {
        match self {
            NumericValue::Finite(bd) => Some(bd),
            _ => None,
        }
    }

    /// Is this one of `NaN` / `±Infinity`?
    #[must_use]
    pub fn is_special(&self) -> bool {
        !matches!(self, NumericValue::Finite(_))
    }

    #[must_use]
    pub fn is_nan(&self) -> bool {
        matches!(self, NumericValue::NaN)
    }

    #[must_use]
    pub fn is_infinite(&self) -> bool {
        matches!(self, NumericValue::Infinity | NumericValue::NegInfinity)
    }

    /// Exactly zero at any display scale (`0`, `0.00`, `-0`)? A special is never
    /// zero.
    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.as_finite().is_some_and(finite_is_zero)
    }

    /// `-1` / `0` / `+1` for a finite value's sign; `+1` for `Infinity`, `-1` for
    /// `-Infinity`, and `0` for `NaN`, whose sign is not ordered. Callers that
    /// care about `NaN` test [`NumericValue::is_nan`] first.
    #[must_use]
    fn signum(&self) -> i32 {
        match self {
            NumericValue::Infinity => 1,
            NumericValue::NegInfinity => -1,
            NumericValue::NaN => 0,
            NumericValue::Finite(bd) => match bd.sign() {
                bigdecimal::num_bigint::Sign::Minus => -1,
                bigdecimal::num_bigint::Sign::NoSign => 0,
                bigdecimal::num_bigint::Sign::Plus => 1,
            },
        }
    }

    /// The infinity with the given sign (`sign` must be non-zero).
    fn infinity_with_sign(sign: i32) -> NumericValue {
        if sign < 0 {
            NumericValue::NegInfinity
        } else {
            NumericValue::Infinity
        }
    }

    /// The btree rank that orders the specials around the finite values.
    fn order_rank(&self) -> u8 {
        match self {
            NumericValue::NegInfinity => 0,
            NumericValue::Finite(_) => 1,
            NumericValue::Infinity => 2,
            NumericValue::NaN => 3,
        }
    }
}

impl From<BigDecimal> for NumericValue {
    fn from(bd: BigDecimal) -> Self {
        NumericValue::Finite(canonical(bd))
    }
}

impl From<i64> for NumericValue {
    fn from(n: i64) -> Self {
        NumericValue::Finite(BigDecimal::from(n))
    }
}

impl From<i32> for NumericValue {
    fn from(n: i32) -> Self {
        NumericValue::Finite(BigDecimal::from(n))
    }
}

impl From<i16> for NumericValue {
    fn from(n: i16) -> Self {
        NumericValue::Finite(BigDecimal::from(n))
    }
}

impl Ord for NumericValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (NumericValue::Finite(a), NumericValue::Finite(b)) => a.cmp(b),
            _ => self.order_rank().cmp(&other.order_rank()),
        }
    }
}

impl PartialOrd for NumericValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for NumericValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        // Hash the scale-normalized form so values that compare equal (`1.0` and
        // `1.00`) hash equally, as the Hash/Eq contract requires.
        if let NumericValue::Finite(bd) = self {
            bd.normalized().to_string().hash(state);
        }
    }
}

impl std::fmt::Display for NumericValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&to_text(self))
    }
}

/// Canonicalize a `BigDecimal` to a PostgreSQL dscale (`>= 0`). A negative scale
/// (e.g. from `1e3`) materializes to scale 0, which is exact because it only
/// appends zeros.
pub fn canonical(bd: BigDecimal) -> BigDecimal {
    if bd.fractional_digit_count() < 0 {
        bd.with_scale(0)
    } else {
        bd
    }
}

/// Parse a numeric literal / text value (PostgreSQL `numeric_in`).
/// Leading/trailing whitespace is trimmed. Returns `None` on bad syntax OR a
/// value that overflows the numeric format (the caller maps either to 22P02 /
/// 22003). The overflow check runs BEFORE [`canonical`], whose `with_scale`
/// would otherwise materialize an adversarial exponent's digits and OOM.
///
/// The special spellings follow `numeric_in` exactly: `NaN` (case-insensitive,
/// with NO sign, because PostgreSQL rejects `-nan`), and `Infinity` / `inf`
/// (case-insensitive) with an optional leading `+` or `-`.
pub fn parse(s: &str) -> Option<NumericValue> {
    let t = s.trim();
    if let Some(special) = parse_special(t) {
        return Some(special);
    }
    if let Some(bd) = parse_nondecimal(t) {
        return Some(NumericValue::Finite(bd));
    }
    parse_finite(&strip_digit_separators(t, 10)?).map(NumericValue::Finite)
}

/// PostgreSQL 16+ digit separators: an `_` inside a numeric string is ignored,
/// but only where it sits BETWEEN two digits of `radix` (`1_000.000_1` is
/// `1000.0001`, `1_0e1_0` is `100000000000`, `0x30b1_F33a` is `816968506`), so
/// `_10`, `10_`, `1__0`, `1_.5`, `1e_5` and `0xF__F` are all 22P02. Returns the
/// separator-free text, or `None` for a misplaced one.
fn strip_digit_separators(t: &str, radix: u32) -> Option<String> {
    if !t.contains('_') {
        return Some(t.to_string());
    }
    let bytes = t.as_bytes();
    let mut out = String::with_capacity(t.len());
    let is_digit = |b: &u8| char::from(*b).is_digit(radix);
    for (i, ch) in t.char_indices() {
        if ch != '_' {
            out.push(ch);
            continue;
        }
        if !bytes.get(i.checked_sub(1)?).is_some_and(is_digit)
            || !bytes.get(i + 1).is_some_and(is_digit)
        {
            return None;
        }
    }
    Some(out)
}

/// The largest digit run a non-decimal numeric literal may carry. Binary is the
/// widest radix, and a value at PostgreSQL's weight ceiling (`10^131072`) needs
/// about 435 000 bits, so anything past this bound is out of format anyway. The
/// cap only stops an adversarial input from an unlimited allocation.
const MAX_NONDECIMAL_DIGITS: usize = 500_000;

/// PostgreSQL 16+ non-decimal integer input: a `0x` / `0o` / `0b` prefix
/// (case-insensitive) after an optional sign, then digits of that radix. The
/// first group may lead with ONE separator (`0x_1F` is 31); every other `_` still
/// has to sit between two digits. There is no fraction or exponent in this form,
/// so the result always has display scale 0.
///
/// `None` means "not this form". The caller falls through to the decimal
/// grammar, which reports the syntax error.
fn parse_nondecimal(t: &str) -> Option<BigDecimal> {
    let (negative, rest) = match t.as_bytes().first() {
        Some(b'-') => (true, &t[1..]),
        Some(b'+') => (false, &t[1..]),
        _ => (false, t),
    };
    let prefix = rest.get(..2)?;
    let radix = match prefix.as_bytes()[1] | 0x20 {
        _ if prefix.as_bytes()[0] != b'0' => return None,
        b'x' => 16,
        b'o' => 8,
        b'b' => 2,
        _ => return None,
    };
    let body = rest.get(2..)?.strip_prefix('_').unwrap_or(&rest[2..]);
    let digits = strip_digit_separators(body, radix)?;
    if digits.is_empty() || digits.len() > MAX_NONDECIMAL_DIGITS {
        return None;
    }
    let magnitude = bigdecimal::num_bigint::BigInt::parse_bytes(digits.as_bytes(), radix)?;
    let bd = BigDecimal::from(if negative { -magnitude } else { magnitude });
    within_format_limits(&bd).then_some(bd)
}

/// The `numeric_in` special-value spellings, or `None` if `t` is not one.
fn parse_special(t: &str) -> Option<NumericValue> {
    if t.eq_ignore_ascii_case("nan") {
        return Some(NumericValue::NaN);
    }
    let (sign, rest) = match t.as_bytes().first() {
        Some(b'-') => (-1, &t[1..]),
        Some(b'+') => (1, &t[1..]),
        _ => (1, t),
    };
    if rest.eq_ignore_ascii_case("infinity") || rest.eq_ignore_ascii_case("inf") {
        return Some(NumericValue::infinity_with_sign(sign));
    }
    None
}

/// Parse a FINITE numeric literal and reject the special spellings. This is the
/// form JSON numbers use, because RFC 8259 has no `NaN`/`Infinity`. It is also
/// the inner half of [`parse`]. Leading/trailing whitespace is trimmed.
pub fn parse_finite(s: &str) -> Option<BigDecimal> {
    use std::str::FromStr;
    let t = s.trim();
    // `BigDecimal::from_str` ignores `_` wherever it appears, which would accept
    // `1__0` and `10_`. The PostgreSQL separator rule is enforced by
    // [`strip_digit_separators`] on the way in, so by here a `_` is always a
    // syntax error — and a JSON number never has one at all.
    if t.is_empty() || t.contains('_') {
        return None;
    }
    let bd = BigDecimal::from_str(t).ok()?;
    if !within_format_limits(&bd) {
        return None;
    }
    Some(canonical(bd))
}

/// Is `bd` within PostgreSQL's numeric-format limits (weight ≤ 131071, dscale ≤
/// 16383)? Computed from the compact `(mantissa, exponent)` form WITHOUT
/// materializing, so an extreme exponent is rejected cheaply.
fn within_format_limits(bd: &BigDecimal) -> bool {
    let (mant, exp) = bd.as_bigint_and_exponent();
    // dscale = displayed fractional digits = max(0, exp).
    if exp > MAX_DSCALE {
        return false;
    }
    // Decimal weight of the leading digit = (#mantissa digits) − 1 − exp.
    let ndigits = mant.to_string().trim_start_matches('-').len() as i64;
    ndigits - 1 - exp <= MAX_WEIGHT
}

/// PostgreSQL `numeric_out`, including the special spellings `NaN`, `Infinity`
/// and `-Infinity`.
pub fn to_text(value: &NumericValue) -> String {
    match value {
        NumericValue::Finite(bd) => finite_to_text(bd),
        NumericValue::NaN => "NaN".to_string(),
        NumericValue::Infinity => "Infinity".to_string(),
        NumericValue::NegInfinity => "-Infinity".to_string(),
    }
}

/// PostgreSQL `numeric_out` for a FINITE value: a plain decimal string (never
/// scientific notation), with exactly `dscale` fractional digits. (`bigdecimal`'s
/// own `Display` switches to `E` notation for small magnitudes, so this is
/// hand-written.)
pub fn finite_to_text(bd: &BigDecimal) -> String {
    let (mant, scale) = bd.as_bigint_and_exponent();
    let s = mant.to_string();
    let neg = s.starts_with('-');
    let digits = s.trim_start_matches('-');
    let scale = scale.max(0) as usize;
    let body = if scale == 0 {
        digits.to_string()
    } else if digits.len() > scale {
        let point = digits.len() - scale;
        format!("{}.{}", &digits[..point], &digits[point..])
    } else {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    };
    if neg && digits != "0" {
        format!("-{body}")
    } else {
        body
    }
}

/// The reserved `numeric_send` sign words for the three special values, and the
/// `dscale` word PostgreSQL emits alongside each. `NUMERIC_NAN` sends dscale 0;
/// `NUMERIC_PINF` / `NUMERIC_NINF` send 32 (byte-verified against PostgreSQL
/// 18.4's `COPY … (FORMAT binary)` output).
const SIGN_NAN: u16 = 0xC000;
const SIGN_PINF: u16 = 0xD000;
const SIGN_NINF: u16 = 0xF000;
const SPECIAL_INF_DSCALE: i16 = 32;

/// PostgreSQL `numeric_send` (binary): `int16 ndigits`, `int16 weight`,
/// `uint16 sign` (0x0000 +, 0x4000 −, and the reserved special words above),
/// `int16 dscale`, then `ndigits` base-10000 groups (`int16`, most significant
/// first), with leading/trailing zero groups stripped.
pub fn binary(value: &NumericValue) -> Vec<u8> {
    let special = |sign: u16, dscale: i16| {
        let mut out = Vec::with_capacity(8);
        out.extend_from_slice(&0i16.to_be_bytes()); // ndigits
        out.extend_from_slice(&0i16.to_be_bytes()); // weight
        out.extend_from_slice(&sign.to_be_bytes());
        out.extend_from_slice(&dscale.to_be_bytes());
        out
    };
    let bd = match value {
        NumericValue::NaN => return special(SIGN_NAN, 0),
        NumericValue::Infinity => return special(SIGN_PINF, SPECIAL_INF_DSCALE),
        NumericValue::NegInfinity => return special(SIGN_NINF, SPECIAL_INF_DSCALE),
        NumericValue::Finite(bd) => bd,
    };
    let (mant, scale) = bd.as_bigint_and_exponent();
    let dscale = scale.max(0) as u16;
    let s = mant.to_string();
    let neg = s.starts_with('-');
    let digits = s.trim_start_matches('-');
    let scale_u = scale.max(0) as usize;

    // Split into integer and fractional decimal-digit strings.
    let (int_str, frac_str) = if digits.len() > scale_u {
        (
            digits[..digits.len() - scale_u].to_string(),
            digits[digits.len() - scale_u..].to_string(),
        )
    } else {
        (
            String::new(),
            format!("{}{}", "0".repeat(scale_u - digits.len()), digits),
        )
    };

    // Base-10000 groups, aligned at the decimal point: integer part left-padded,
    // fractional part right-padded, to a multiple of 4.
    let mut nbase: Vec<i16> = Vec::new();
    let int_pad = (DEC_DIGITS as usize - int_str.len() % DEC_DIGITS as usize) % DEC_DIGITS as usize;
    let int_padded = format!("{}{}", "0".repeat(int_pad), int_str);
    let int_group_count = int_padded.len() / DEC_DIGITS as usize;
    for g in 0..int_group_count {
        let chunk = &int_padded[g * 4..g * 4 + 4];
        nbase.push(chunk.parse::<i16>().unwrap_or(0));
    }
    let frac_pad =
        (DEC_DIGITS as usize - frac_str.len() % DEC_DIGITS as usize) % DEC_DIGITS as usize;
    let frac_padded = format!("{}{}", frac_str, "0".repeat(frac_pad));
    for g in 0..frac_padded.len() / DEC_DIGITS as usize {
        let chunk = &frac_padded[g * 4..g * 4 + 4];
        nbase.push(chunk.parse::<i16>().unwrap_or(0));
    }

    // Weight of the first group, then strip leading/trailing zero groups.
    let mut weight = int_group_count as i64 - 1;
    while nbase.first() == Some(&0) {
        nbase.remove(0);
        weight -= 1;
    }
    while nbase.last() == Some(&0) {
        nbase.pop();
    }
    let sign: u16 = if nbase.is_empty() {
        weight = 0;
        0x0000
    } else if neg {
        0x4000
    } else {
        0x0000
    };

    let mut out = Vec::with_capacity(8 + nbase.len() * 2);
    out.extend_from_slice(&(nbase.len() as i16).to_be_bytes());
    out.extend_from_slice(&(weight as i16).to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&(dscale as i16).to_be_bytes());
    for d in nbase {
        out.extend_from_slice(&d.to_be_bytes());
    }
    out
}

/// Decode PostgreSQL's `numeric_recv` base-10000 binary representation.
///
/// Returns `None` when the wire representation is malformed or encodes a numeric
/// value outside PostgreSQL's supported format limits.
///
/// The three reserved special sign words are accepted. `numeric_recv` still
/// reads and range-checks `ndigits` base-10000 groups before discarding them for
/// a special, and it ignores the rest of the header. PostgreSQL sends `dscale`
/// 32 with `±Infinity` and 0 with `NaN`, but accepts either.
pub fn from_binary(input: &[u8]) -> Option<NumericValue> {
    let header: [u8; 8] = input.get(..8)?.try_into().ok()?;
    let ndigits = usize::try_from(i16::from_be_bytes(header[0..2].try_into().ok()?)).ok()?;
    let weight = i32::from(i16::from_be_bytes(header[2..4].try_into().ok()?));
    let sign = u16::from_be_bytes(header[4..6].try_into().ok()?);
    let dscale = usize::try_from(i16::from_be_bytes(header[6..8].try_into().ok()?)).ok()?;
    let special = match sign {
        SIGN_NAN => Some(NumericValue::NaN),
        SIGN_PINF => Some(NumericValue::Infinity),
        SIGN_NINF => Some(NumericValue::NegInfinity),
        0x0000 | 0x4000 if dscale <= MAX_DSCALE as usize => None,
        _ => return None,
    };
    let expected_len = 8usize.checked_add(ndigits.checked_mul(2)?)?;
    if input.len() != expected_len {
        return None;
    }
    let digits = input[8..]
        .as_chunks::<2>()
        .0
        .iter()
        .map(|bytes| u16::from_be_bytes(*bytes))
        .collect::<Vec<_>>();
    if digits.iter().any(|digit| *digit >= 10_000) {
        return None;
    }
    if special.is_some() {
        return special;
    }

    let is_zero = digits.iter().all(|digit| *digit == 0);
    let mut text = numeric_binary_to_text(&digits, weight, dscale)?;
    if sign == 0x4000 && !is_zero {
        text.insert(0, '-');
    }
    parse_finite(&text).map(NumericValue::Finite)
}

fn numeric_binary_to_text(digits: &[u16], weight: i32, dscale: usize) -> Option<String> {
    let integer_groups = weight.saturating_add(1).max(0);
    let fractional_groups = dscale.checked_add(3)?.checked_div(4)?;
    let mut text = String::new();

    if integer_groups == 0 {
        text.push('0');
    } else {
        for group in (0..integer_groups).rev() {
            let digit = numeric_binary_group(digits, weight, group)?;
            if group == integer_groups - 1 {
                text.push_str(&digit.to_string());
            } else {
                use std::fmt::Write;
                write!(text, "{digit:04}").ok()?;
            }
        }
    }

    if dscale == 0 {
        return Some(text);
    }
    text.push('.');
    for group in 1..=fractional_groups {
        use std::fmt::Write;
        let group = i32::try_from(group).ok()?.checked_neg()?;
        let digit = numeric_binary_group(digits, weight, group)?;
        write!(text, "{digit:04}").ok()?;
    }
    text.truncate(text.len().checked_sub(fractional_groups * 4 - dscale)?);
    Some(text)
}

fn numeric_binary_group(digits: &[u16], weight: i32, group: i32) -> Option<u16> {
    let index = weight.checked_sub(group)?;
    let Ok(index) = usize::try_from(index) else {
        return Some(0);
    };
    Some(*digits.get(index).unwrap_or(&0))
}

/// Both operands as finite decimals. Every arithmetic entry point below returns
/// for each special combination first, so by the time it calls this the operands
/// are known finite.
fn finite_pair<'a>(a: &'a NumericValue, b: &'a NumericValue) -> (&'a BigDecimal, &'a BigDecimal) {
    const REASON: &str = "special operands are handled before the finite path";
    (a.as_finite().expect(REASON), b.as_finite().expect(REASON))
}

/// `a + b` (result dscale = max input dscale; `bigdecimal` matches PostgreSQL).
/// `Infinity + -Infinity` is `NaN`; any other infinity dominates.
pub fn add(a: &NumericValue, b: &NumericValue) -> NumericValue {
    if a.is_nan() || b.is_nan() {
        return NumericValue::NaN;
    }
    match (a.is_infinite(), b.is_infinite()) {
        (true, true) if a.signum() != b.signum() => return NumericValue::NaN,
        (true, _) => return a.clone(),
        (_, true) => return b.clone(),
        (false, false) => {}
    }
    let (x, y) = finite_pair(a, b);
    NumericValue::Finite(canonical(x + y))
}

/// `a - b` (result dscale = max input dscale). `Infinity - Infinity` is `NaN`.
pub fn sub(a: &NumericValue, b: &NumericValue) -> NumericValue {
    if a.is_nan() || b.is_nan() {
        return NumericValue::NaN;
    }
    match (a.is_infinite(), b.is_infinite()) {
        (true, true) if a.signum() == b.signum() => return NumericValue::NaN,
        (true, _) => return a.clone(),
        (_, true) => return NumericValue::infinity_with_sign(-b.signum()),
        (false, false) => {}
    }
    let (x, y) = finite_pair(a, b);
    NumericValue::Finite(canonical(x - y))
}

/// `a * b` (result dscale = sum of input dscales). `0 * Infinity` is `NaN`;
/// otherwise an infinite operand gives the infinity with the product's sign.
pub fn mul(a: &NumericValue, b: &NumericValue) -> NumericValue {
    if a.is_nan() || b.is_nan() {
        return NumericValue::NaN;
    }
    if a.is_infinite() || b.is_infinite() {
        let sign = a.signum() * b.signum();
        if sign == 0 {
            return NumericValue::NaN;
        }
        return NumericValue::infinity_with_sign(sign);
    }
    let (x, y) = finite_pair(a, b);
    NumericValue::Finite(canonical(x * y))
}

/// Unary minus (PostgreSQL `numeric_uminus`). `-NaN` is `NaN`.
pub fn neg(a: &NumericValue) -> NumericValue {
    match a {
        NumericValue::NaN => NumericValue::NaN,
        NumericValue::Infinity => NumericValue::NegInfinity,
        NumericValue::NegInfinity => NumericValue::Infinity,
        NumericValue::Finite(bd) => NumericValue::Finite(canonical(-bd)),
    }
}

/// `a / b` with PostgreSQL's display-scale rule (`select_div_scale`), rounded
/// half-away-from-zero.
///
/// The special rules, and the order PostgreSQL applies them in: a `NaN` operand
/// wins outright (so `NaN / 0` is `NaN`, NOT 22012); then a zero divisor is
/// 22012 (so `Infinity / 0` errors); then `Infinity / Infinity` is `NaN`, a
/// finite numerator over an infinity is `0` at scale 0, and an infinite
/// numerator over a finite divisor is the infinity with the quotient's sign.
pub fn div(a: &NumericValue, b: &NumericValue) -> Result<NumericValue, TypeError> {
    if a.is_nan() || b.is_nan() {
        return Ok(NumericValue::NaN);
    }
    if b.is_zero() {
        return Err(TypeError::DivisionByZero);
    }
    match (a.is_infinite(), b.is_infinite()) {
        (true, true) => return Ok(NumericValue::NaN),
        (true, false) => return Ok(NumericValue::infinity_with_sign(a.signum() * b.signum())),
        (false, true) => return Ok(NumericValue::from(0i64)),
        (false, false) => {}
    }
    let (x, y) = finite_pair(a, b);
    let rscale = select_div_scale(x, y);
    Ok(NumericValue::Finite(
        (x / y).with_scale_round(rscale, RoundingMode::HalfUp),
    ))
}

/// `div(a, b)`, PostgreSQL `numeric_div_trunc`: the quotient truncated toward
/// zero, at scale 0. Same special-value ordering as [`div`].
pub fn div_trunc(a: &NumericValue, b: &NumericValue) -> Result<NumericValue, TypeError> {
    if a.is_nan() || b.is_nan() {
        return Ok(NumericValue::NaN);
    }
    if b.is_zero() {
        return Err(TypeError::DivisionByZero);
    }
    match (a.is_infinite(), b.is_infinite()) {
        (true, true) => return Ok(NumericValue::NaN),
        (true, false) => return Ok(NumericValue::infinity_with_sign(a.signum() * b.signum())),
        (false, true) => return Ok(NumericValue::from(0i64)),
        (false, false) => {}
    }
    let (x, y) = finite_pair(a, b);
    Ok(NumericValue::Finite(canonical(
        (x / y).with_scale_round(0, RoundingMode::Down),
    )))
}

/// `mod(a, b)` for numeric (the remainder takes the dividend's sign, like PG).
///
/// Specials, in PostgreSQL's order: `NaN` wins (so `NaN % 0` is `NaN`), then a
/// zero divisor is 22012 (so `Infinity % 0` errors), then an infinite DIVIDEND
/// is `NaN` and a finite dividend over an infinity is the dividend unchanged
/// (`4.2 % Infinity` is `4.2`, keeping its display scale).
pub fn rem(a: &NumericValue, b: &NumericValue) -> Result<NumericValue, TypeError> {
    if a.is_nan() || b.is_nan() {
        return Ok(NumericValue::NaN);
    }
    if b.is_zero() {
        return Err(TypeError::DivisionByZero);
    }
    if a.is_infinite() {
        return Ok(NumericValue::NaN);
    }
    if b.is_infinite() {
        return Ok(a.clone());
    }
    let (x, y) = finite_pair(a, b);
    Ok(NumericValue::Finite(canonical(x % y)))
}

/// `abs(x)`: `abs(-Infinity)` is `Infinity`, `abs(NaN)` is `NaN`.
pub fn abs(value: &NumericValue) -> NumericValue {
    match value {
        NumericValue::NaN => NumericValue::NaN,
        NumericValue::Infinity | NumericValue::NegInfinity => NumericValue::Infinity,
        NumericValue::Finite(bd) => NumericValue::Finite(bd.abs()),
    }
}

/// `floor(x)`: round toward −∞ (PostgreSQL `numeric_floor`); scale 0. A special
/// comes back unchanged.
pub fn floor(value: &NumericValue) -> NumericValue {
    match value.as_finite() {
        None => value.clone(),
        Some(bd) => NumericValue::Finite(canonical(bd.with_scale_round(0, RoundingMode::Floor))),
    }
}

/// `ceil(x)` / `ceiling(x)`: round toward +∞ (PostgreSQL `numeric_ceil`); scale
/// 0. A special comes back unchanged.
pub fn ceil(value: &NumericValue) -> NumericValue {
    match value.as_finite() {
        None => value.clone(),
        Some(bd) => NumericValue::Finite(canonical(bd.with_scale_round(0, RoundingMode::Ceiling))),
    }
}

/// `round(x, n)`: round to `n` decimal places, half-away-from-zero (PostgreSQL
/// `numeric_round`). `n` may be negative (round to tens/hundreds/…). The result
/// carries scale `max(n, 0)`. `n` is clamped to `MAX_DSCALE`, so an adversarial
/// huge scale cannot materialize billions of fractional digits and OOM. This is
/// the same format-limit discipline [`within_format_limits`] enforces on
/// `parse`. A special comes back unchanged, at every `n`.
pub fn round(value: &NumericValue, n: i64) -> NumericValue {
    match value.as_finite() {
        None => value.clone(),
        Some(bd) => NumericValue::Finite(canonical(
            bd.with_scale_round(n.min(MAX_DSCALE), RoundingMode::HalfUp),
        )),
    }
}

/// `trunc(x, n)`: truncate to `n` decimal places, toward zero (PostgreSQL
/// `numeric_trunc`). `n` may be negative; clamped to `MAX_DSCALE` (see
/// [`round`]). A special comes back unchanged.
pub fn trunc(value: &NumericValue, n: i64) -> NumericValue {
    match value.as_finite() {
        None => value.clone(),
        Some(bd) => NumericValue::Finite(canonical(
            bd.with_scale_round(n.min(MAX_DSCALE), RoundingMode::Down),
        )),
    }
}

/// `sign(x)`: −1 / 0 / 1 as a numeric (PostgreSQL `numeric_sign`).
/// `sign(±Infinity)` is `±1`; `sign(NaN)` is `NaN`.
pub fn sign(value: &NumericValue) -> NumericValue {
    match value {
        NumericValue::NaN => NumericValue::NaN,
        NumericValue::Infinity => NumericValue::from(1i64),
        NumericValue::NegInfinity => NumericValue::from(-1i64),
        NumericValue::Finite(bd) => {
            NumericValue::from(i64::from(match bd.cmp(&BigDecimal::from(0)) {
                Ordering::Less => -1i8,
                Ordering::Equal => 0,
                Ordering::Greater => 1,
            }))
        }
    }
}

/// Is this exactly zero, at any display scale (`0`, `0.00`, `-0`)?
fn finite_is_zero(bd: &BigDecimal) -> bool {
    bd.as_bigint_and_exponent()
        .0
        .to_string()
        .trim_start_matches('-')
        == "0"
}

/// PostgreSQL `select_div_scale`: the division/AVG display scale. In base-10000
/// units, `rscale = clamp(max(16 − qweight·4, s1, s2), 0, 1000)` where `qweight`
/// is the quotient's leading-digit weight estimate.
///
/// Equal leading digits still decrement `qweight`. PostgreSQL cannot tell which
/// operand is larger from the leading digit alone, so it assumes the quotient is
/// below one, which is what gives `70.0 / 70` twenty fractional digits rather
/// than sixteen. The two display scales are also a floor *individually*, not a
/// sum: the sum is the rule for multiplication, not division.
fn select_div_scale(a: &BigDecimal, b: &BigDecimal) -> i64 {
    let (w1, f1) = nbase_weight_and_lead(a);
    let (w2, f2) = nbase_weight_and_lead(b);
    let mut qweight = w1 - w2;
    if f1 <= f2 {
        qweight -= 1;
    }
    let s1 = a.fractional_digit_count().max(0);
    let s2 = b.fractional_digit_count().max(0);
    (MIN_SIG_DIGITS - qweight * DEC_DIGITS)
        .max(s1)
        .max(s2)
        .clamp(0, MAX_DISPLAY_SCALE)
}

/// The base-10000 weight of the leading digit, and that leading group's value
/// (right-padded to four decimal digits). These are the two inputs
/// `select_div_scale` needs. Zero has weight 0 and leading group 0.
fn nbase_weight_and_lead(bd: &BigDecimal) -> (i64, u64) {
    let (mant, scale) = bd.as_bigint_and_exponent();
    let s = mant.to_string();
    let digits = s.trim_start_matches('-');
    if digits == "0" {
        return (0, 0);
    }
    let dweight = digits.len() as i64 - 1 - scale; // decimal weight of leading digit
    let w = dweight.div_euclid(DEC_DIGITS); // base-10000 weight (floor division)
    let count = (dweight - DEC_DIGITS * w + 1) as usize; // 1..=4 leading decimal digits
    let mut lead: String = digits.chars().take(count).collect();
    while lead.len() < count {
        lead.push('0');
    }
    (w, lead.parse::<u64>().unwrap_or(0))
}

/// PostgreSQL clamp bound for a transcendental result display scale.
const TRANSC_MAX_SCALE: i64 = 1000;

/// The decimal weight of a value's leading significant digit (its position as a
/// power of ten): 1234 -> 3, 0.0067 -> -3, 0 -> 0.
fn decimal_weight(bd: &BigDecimal) -> i64 {
    if finite_is_zero(bd) {
        return 0;
    }
    let (mant, scale) = bd.as_bigint_and_exponent();
    let len = mant.to_string().trim_start_matches('-').len() as i64;
    len - 1 - scale
}

/// sqrt rscale (PostgreSQL `sqrt_var`): `sweight = w*DEC_DIGITS/2 + 1`.
fn sqrt_rscale(arg: &BigDecimal) -> i64 {
    let (w, _) = nbase_weight_and_lead(arg);
    let sweight = w * DEC_DIGITS / 2 + 1;
    (MIN_SIG_DIGITS - sweight)
        .max(arg.fractional_digit_count().max(0))
        .clamp(0, TRANSC_MAX_SCALE)
}

/// exp rscale (PostgreSQL `exp_var`): `ln_dweight = trunc(val * log10(e))`.
fn exp_rscale(arg: &BigDecimal) -> i64 {
    let val = arg.to_f64().unwrap_or(0.0);
    let ln_dweight = (val * std::f64::consts::LOG10_E) as i64; // C-style truncation toward zero
    // PostgreSQL also floors rscale at the input's own dscale, so e.g.
    // exp(123.456) keeps 3 fractional digits even though the integer part is huge.
    (MIN_SIG_DIGITS - ln_dweight)
        .max(arg.fractional_digit_count().max(0))
        .clamp(0, TRANSC_MAX_SCALE)
}

/// PostgreSQL `estimate_ln_dweight`: an estimate of the decimal weight of `ln(arg)`.
///
/// Between 0.9 and 1.1 the logarithm is tiny and its weight is arbitrarily
/// negative, so PostgreSQL estimates it from `ln(1 + x) ≈ x`, which is the
/// weight of `arg − 1`. That branch gives `ln(1.0000000001)` twenty-six fractional
/// digits instead of the sixteen a non-negative estimate would allow.
fn estimate_ln_dweight(arg: &BigDecimal) -> i64 {
    let near_one = |bound: &str| parse_finite(bound).expect("a literal bound parses");
    if *arg >= near_one("0.9") && *arg <= near_one("1.1") {
        return decimal_weight(&(arg - BigDecimal::from(1)));
    }
    let dw = decimal_weight(arg);
    if dw == 0 {
        0
    } else {
        let est = ((dw.unsigned_abs() as f64) * std::f64::consts::LN_10)
            .log10()
            .floor() as i64;
        est.max(0)
    }
}

/// ln/log (base-10) rscale (PostgreSQL `ln_var` / `log_var` with base 10).
fn ln_rscale(arg: &BigDecimal) -> i64 {
    (MIN_SIG_DIGITS - estimate_ln_dweight(arg))
        .max(arg.fractional_digit_count().max(0))
        .clamp(0, TRANSC_MAX_SCALE)
}

/// `numeric → int2` / `int4` / `int8`: round half-away-from-zero (PostgreSQL
/// `numeric_int4`, distinct from `float8 → int`'s round-half-to-even), then
/// range-check (22003).
///
/// A special value has no integer form: PostgreSQL raises `0A000` with
/// `cannot convert NaN to integer` / `cannot convert infinity to bigint` (the
/// noun is the SQL spelling of the target type), which is what `type_name`
/// selects here.
fn to_integer(value: &NumericValue, type_name: &'static str) -> Result<BigDecimal, TypeError> {
    match value {
        NumericValue::Finite(bd) => Ok(bd.with_scale_round(0, RoundingMode::HalfUp)),
        NumericValue::NaN => Err(TypeError::FeatureNotSupported {
            message: format!("cannot convert NaN to {type_name}"),
        }),
        NumericValue::Infinity | NumericValue::NegInfinity => Err(TypeError::FeatureNotSupported {
            message: format!("cannot convert infinity to {type_name}"),
        }),
    }
}

pub fn to_i16(value: &NumericValue) -> Result<i16, TypeError> {
    to_integer(value, "smallint")?
        .to_i16()
        .ok_or_else(|| TypeError::out_of_range_for("smallint"))
}

pub fn to_i32(value: &NumericValue) -> Result<i32, TypeError> {
    to_integer(value, "integer")?
        .to_i32()
        .ok_or(TypeError::Overflow)
}

pub fn to_i64(value: &NumericValue) -> Result<i64, TypeError> {
    to_integer(value, "bigint")?
        .to_i64()
        .ok_or_else(|| TypeError::out_of_range_for("bigint"))
}

pub fn from_i64(n: i64) -> NumericValue {
    NumericValue::Finite(BigDecimal::from(n))
}

/// `float8 → numeric` through the float's shortest round-tripping text (PostgreSQL
/// `float8_numeric`), so `0.1::float8::numeric` is `0.1`, not the exact binary
/// expansion. A non-finite float maps to the matching numeric special
/// (PostgreSQL 14+ `float8_numeric`).
pub fn from_f64(f: f64) -> NumericValue {
    if f.is_nan() {
        return NumericValue::NaN;
    }
    if f.is_infinite() {
        return NumericValue::infinity_with_sign(if f < 0.0 { -1 } else { 1 });
    }
    NumericValue::Finite(
        parse_finite(&format!("{f}")).expect("a finite f64 always lands inside the numeric format"),
    )
}

/// `float4 → numeric` (PostgreSQL `float4_numeric`), which converts through
/// `snprintf("%.*g", FLT_DIG, val)`, which is **six** significant digits, not
/// the shortest round-tripping text `float4out` emits. That is why
/// `16777216::float4::numeric` is `16777200` and `3.4028235e38::float4::numeric`
/// is `340282000000000000000000000000000000000`. A non-finite float maps to the
/// matching numeric special.
pub fn from_f32(f: f32) -> NumericValue {
    if f.is_nan() {
        return NumericValue::NaN;
    }
    if f.is_infinite() {
        return NumericValue::infinity_with_sign(if f < 0.0 { -1 } else { 1 });
    }
    NumericValue::Finite(
        parse_finite(&six_significant_digits(f))
            .expect("a finite f32 always lands inside the numeric format"),
    )
}

/// `%.6g` of `f`, in the scientific spelling `BigDecimal` parses identically to
/// the fixed one. `{:.5e}` is six significant digits; `%g` then drops the
/// trailing fractional zeros, which is what keeps the resulting numeric's
/// display scale down (`0.1::float4::numeric` is `0.1`, not `0.100000`).
fn six_significant_digits(f: f32) -> String {
    let scientific = format!("{f:.5e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("Rust's LowerExp always emits an `e`");
    let mantissa = match mantissa.split_once('.') {
        None => mantissa,
        Some(_) => mantissa.trim_end_matches('0').trim_end_matches('.'),
    };
    format!("{mantissa}e{exponent}")
}

/// `numeric → float8` (PostgreSQL `numeric_float8`). The specials map across
/// directly, and a finite magnitude beyond `f64` range becomes `±Infinity`.
pub fn to_f64(value: &NumericValue) -> f64 {
    match value {
        NumericValue::NaN => f64::NAN,
        NumericValue::Infinity => f64::INFINITY,
        NumericValue::NegInfinity => f64::NEG_INFINITY,
        NumericValue::Finite(bd) => bd.to_f64().unwrap_or_else(|| {
            if bd.sign() == bigdecimal::num_bigint::Sign::Minus {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            }
        }),
    }
}

/// Apply a `numeric(precision, scale)` type modifier: round to `scale`
/// (half-away-from-zero) then check the integer-digit budget `precision − scale`;
/// an overflow is 22003 ("numeric field overflow").
///
/// PostgreSQL's `apply_typmod_special` accepts `NaN` under ANY typmod (it has no
/// digits to overflow) but rejects `±Infinity` with 22003, so a `numeric(4,4)`
/// column stores `NaN` and refuses `Inf`.
pub fn apply_typmod(value: &NumericValue, tm: Typmod) -> Result<NumericValue, TypeError> {
    let bd = match value {
        NumericValue::NaN => return Ok(NumericValue::NaN),
        NumericValue::Infinity | NumericValue::NegInfinity => return Err(TypeError::Overflow),
        NumericValue::Finite(bd) => bd,
    };
    let r = bd.with_scale_round(i64::from(tm.scale), RoundingMode::HalfUp);
    if !finite_is_zero(&r) {
        let (mant, scale) = r.as_bigint_and_exponent();
        let len = mant.to_string().trim_start_matches('-').len() as i64;
        let int_digits = len - scale; // integer-part digit count
        if int_digits > i64::from(tm.precision) - i64::from(tm.scale) {
            return Err(TypeError::Overflow);
        }
    }
    Ok(NumericValue::Finite(canonical(r)))
}

// ---------------------------------------------------------------------------
// SP38: the numeric `to_char` engine (`format_numeric`).
//
// This is an INDEPENDENT engine from the date/time `to_char` (in `datetime.rs`):
// the numeric template language is a positional digit grid (`9 0 . , S MI …`),
// not the date/time field-name tokenizer. It mirrors PostgreSQL's `formatting.c`
// `NUM_processor` / `NUM_prepare_locale` for the C locale.
//
// THE GENERAL SHAPE (PG `NUM_processor`):
//   1. Parse the template ONCE into a `NumDesc` descriptor: the count of integer
//      and fractional digit positions (`9`/`0`), where the decimal point sits,
//      where group separators sit, the sign mode + its anchor, currency + its
//      anchor, the `V` shift, and the `FM`/`TH`/`B`/`pre_lsign` flags.
//   2. Apply the `V` shift (multiply by 10^n) if present.
//   3. Round the value (half-away-from-zero) to the fractional-digit count.
//   4. Lay the integer digits right-to-left into the integer positions, then the
//      fractional digits left-to-right; place group separators; place the point.
//   5. Render the sign / currency / brackets per the mode at their anchors.
//   6. Integer-part overflow → `#`-fill the digit positions (the sign/currency
//      decoration still renders normally).
//   7. `FM` strips padding; `TH`/`th` appends an ordinal; `B` is a no-op in PG 18.
//
// Every exact spacing (the C-locale currency glyph, the `#`-overflow composition,
// the default/`S`/`MI`/`PL`/`SG`/`PR` sign placement, and the `B` no-op) was
// VALIDATED against a live PostgreSQL 18 oracle in SP38 Task 9; the relevant rule
// comments cite the oracle-confirmed `to_char(...)` example.
// ---------------------------------------------------------------------------

/// Where a sign / currency marker is anchored relative to the number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Anchor {
    /// Before the first digit position (left of the number).
    Leading,
    /// After the last digit position (right of the number).
    Trailing,
}

/// The sign-handling mode selected by the template.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SignMode {
    /// No explicit sign pattern: PostgreSQL reserves ONE leading column, a
    /// blank for a non-negative value and `-` for a negative one. `FM` strips
    /// that blank.
    Default,
    /// `S`: sign ANCHORED to the number (it floats right up against the first/last
    /// printed digit, consuming a leading/trailing blank), always `-` or `+`. PG
    /// doc: `to_char(-12,'S9999')` → `'  -12'` (the `-` is glued to `12`, not at the
    /// far-left column).
    S(Anchor),
    /// `MI`: `-` if negative, blank otherwise, at a FIXED position (NOT anchored).
    /// PG doc: `to_char(-12,'MI9999')` → `'-  12'` (the `-` is at the far-left
    /// fixed column, the digits float).
    Mi(Anchor),
    /// `PL`: `+` if the number is `> 0` (PG: "plus sign … if number > 0"), at a
    /// FIXED position; otherwise a blank in that column.
    Pl(Anchor),
    /// `SG`: plus OR minus sign at a FIXED position (NOT anchored).
    Sg(Anchor),
    /// `PR`: a negative value is wrapped `<…>`; a non-negative value gets a
    /// leading + trailing blank instead of the brackets.
    Pr,
}

/// The parsed numeric template descriptor (PG `NUMDesc`).
#[derive(Debug, Clone)]
struct NumDesc {
    /// Number of digit positions before the decimal point.
    pre: usize,
    /// Number of digit positions after the decimal point.
    post: usize,
    /// `true` at integer position `i` (counted from the LEFT, 0-based) if that
    /// position is a `0` (zero-fill); `false` for a `9`. `int_zero[i]`.
    int_zero: Vec<bool>,
    /// Group-separator positions: the index (0-based, from the LEFT of the integer
    /// digit run) AFTER which a separator is emitted. PG emits the separator
    /// BETWEEN the digit at `idx-1` and `idx`; this field holds the count of
    /// digits to the left of each separator.
    group_before: Vec<usize>,
    /// Does the template contain a decimal point at all?
    has_point: bool,
    /// Sign rendering mode + (for the anchored modes) whether the sign char was
    /// seen before or after the digit run.
    sign: SignMode,
    /// Currency marker (`L` or `$`) anchor, if present.
    currency: Option<Anchor>,
    /// `V` shift amount = number of `9`/`0` digits following the `V` (multiply by
    /// 10^shift). `None` if no `V`.
    v_shift: Option<u32>,
    /// `FM` fill-mode: suppress the reserved sign blank + leading/trailing blanks.
    fill_mode: bool,
    /// `TH`/`th` ordinal suffix; `Some(true)` = upper (`TH`), `Some(false)` = lower.
    ordinal: Option<bool>,
}

/// Parse a numeric `to_char` template into a [`NumDesc`]. Patterns are matched
/// left-to-right, longest-first for the multi-char ones (`MI`/`PL`/`SG`/`PR`/`TH`/
/// `FM`/`EEEE`-not-supported). Unrecognized characters are kept as literals by the
/// renderer, so this only records the STRUCTURAL pattern positions.
fn parse_num_template(template: &str) -> NumDesc {
    let chars: Vec<char> = template.chars().collect();
    let mut int_zero: Vec<bool> = Vec::new();
    let mut post = 0usize;
    let mut group_before: Vec<usize> = Vec::new();
    let mut has_point = false;
    let mut sign = SignMode::Default;
    let mut currency: Option<Anchor> = None;
    let mut v_shift: Option<u32> = None;
    let mut fill_mode = false;
    let mut ordinal: Option<bool> = None;
    let mut seen_digit = false; // have we passed any 9/0 yet? (anchors sign/currency)

    let mut i = 0;
    while i < chars.len() {
        // Multi-character patterns first (case-insensitive where PG is).
        if matches_ci(&chars, i, "FM") {
            fill_mode = true;
            i += 2;
            continue;
        }
        if matches_at(&chars, i, "TH") {
            ordinal = Some(true);
            i += 2;
            continue;
        }
        if matches_at(&chars, i, "th") {
            ordinal = Some(false);
            i += 2;
            continue;
        }
        if matches_ci(&chars, i, "MI") {
            sign = SignMode::Mi(anchor_of(seen_digit));
            i += 2;
            continue;
        }
        if matches_ci(&chars, i, "PL") {
            sign = SignMode::Pl(anchor_of(seen_digit));
            i += 2;
            continue;
        }
        if matches_ci(&chars, i, "SG") {
            sign = SignMode::Sg(anchor_of(seen_digit));
            i += 2;
            continue;
        }
        if matches_ci(&chars, i, "PR") {
            sign = SignMode::Pr;
            i += 2;
            continue;
        }
        // `V` shift: the 9/0 digits that FOLLOW `V` are the shift amount. PG
        // MULTIPLIES the value by 10^n AND counts those n positions as additional
        // INTEGER positions (so `to_char(12.4, '99V999')` → `12.4*1000 = 12400`,
        // laid into 2+3 = 5 integer slots → ' 12400'). They are NOT fractional.
        if chars[i] == 'V' || chars[i] == 'v' {
            let mut n = 0u32;
            let mut j = i + 1;
            while j < chars.len() && (chars[j] == '9' || chars[j] == '0') {
                int_zero.push(chars[j] == '0');
                n += 1;
                j += 1;
            }
            v_shift = Some(n);
            seen_digit = true;
            i = j;
            continue;
        }
        match chars[i] {
            '9' | '0' => {
                let is_zero = chars[i] == '0';
                if has_point {
                    post += 1;
                } else {
                    int_zero.push(is_zero);
                }
                seen_digit = true;
                i += 1;
            }
            '.' | 'D' | 'd' => {
                has_point = true;
                i += 1;
            }
            ',' | 'G' | 'g' => {
                // A separator's position = the count of integer digits seen so far.
                if !has_point {
                    group_before.push(int_zero.len());
                }
                i += 1;
            }
            'S' | 's' => {
                sign = SignMode::S(anchor_of(seen_digit));
                i += 1;
            }
            'L' | 'l' | '$' => {
                currency = Some(anchor_of(seen_digit));
                i += 1;
            }
            // `B` (blank-on-zero) is a documented PG pattern, but PostgreSQL 18's
            // `NUM_processor` effectively never blanks the result for the in-scope
            // templates (oracle-confirmed: `to_char(0,'B9999')` → `'    0'`). So `B`
            // is consumed as a no-op (it is NOT emitted as a literal).
            'B' | 'b' => {
                i += 1;
            }
            // Any other character is a literal handled at render time.
            _ => {
                i += 1;
            }
        }
    }

    NumDesc {
        pre: int_zero.len(),
        post,
        int_zero,
        group_before,
        has_point,
        sign,
        currency,
        v_shift,
        fill_mode,
        ordinal,
    }
}

/// A sign/currency marker seen BEFORE any digit anchors leading, else trailing.
fn anchor_of(seen_digit: bool) -> Anchor {
    if seen_digit {
        Anchor::Trailing
    } else {
        Anchor::Leading
    }
}

/// Case-insensitive multi-char match at `chars[i..]`.
fn matches_ci(chars: &[char], i: usize, pat: &str) -> bool {
    let p: Vec<char> = pat.chars().collect();
    if i + p.len() > chars.len() {
        return false;
    }
    chars[i..i + p.len()]
        .iter()
        .zip(&p)
        .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

/// Exact (case-sensitive) multi-char match at `chars[i..]` (for `TH` vs `th`).
fn matches_at(chars: &[char], i: usize, pat: &str) -> bool {
    let p: Vec<char> = pat.chars().collect();
    if i + p.len() > chars.len() {
        return false;
    }
    chars[i..i + p.len()].iter().zip(&p).all(|(a, b)| a == b)
}

/// The numeric `to_char` engine (independent of the date/time one). Format `value`
/// per the PostgreSQL numeric template. See the SP38 spec §1.2 for the in-scope
/// pattern set. Returns text; on integer-part overflow the field is `#`-filled.
///
/// PostgreSQL's `to_char(numeric, text)` is extremely lenient. It never raises
/// an error for a malformed template; it emits an unsupported character
/// literally and `#`-fills an oversized integer part. This function therefore
/// returns a `Result` only to match the engine signature contract; in practice
/// it is always `Ok`.
pub fn format_numeric(template: &str, value: &NumericValue) -> Result<String, TypeError> {
    let desc = parse_num_template(template);
    let value = match value.as_finite() {
        Some(bd) => bd,
        None => return format_special(&desc, value),
    };

    // (1) Apply the `V` shift: multiply by 10^shift. The shift digits were already
    // folded into `desc.pre` (integer positions) by the template parser. Build the
    // multiplier from text ("1" + n zeros) so a large `n` never overflows a `u64`.
    let shifted = match desc.v_shift {
        Some(0) | None => value.clone(),
        Some(n) => {
            let pow10 = parse_finite(&format!("1{}", "0".repeat(n as usize)))
                .unwrap_or_else(|| BigDecimal::from(1));
            canonical(value * pow10)
        }
    };

    // (2) Round half-away-from-zero to the fractional-digit count.
    let rounded = canonical(
        shifted.with_scale_round((desc.post as i64).min(MAX_DSCALE), RoundingMode::HalfUp),
    );
    let negative =
        rounded.sign() == bigdecimal::num_bigint::Sign::Minus && !finite_is_zero(&rounded);

    // (3) Extract the integer + fractional decimal-digit strings of |value|.
    let (int_digits, frac_digits) = split_decimal(&rounded, desc.post);

    // (4) Integer-part overflow: more significant integer digits than positions.
    // PG `#`-fills every DIGIT/separator/point position but still renders the sign
    // and currency decoration normally (oracle-confirmed, PG 18: `to_char(123456,
    // '999')` → `' ###'`, `to_char(-123456,'99.99')` → `'-##.##'`), so the overflow
    // core is routed through the SAME `decorate` path as a normal value.
    // A template with NO integer digit positions (e.g. `.99`) cannot represent any
    // value's integer part — not even the implicit leading `0` — so PG `#`-overflows
    // it for every value (oracle-confirmed, PG 18: `to_char(0,'.99')` → `' .##'`).
    let int_significant = int_digits.trim_start_matches('0');
    if int_significant.len() > desc.pre || desc.pre == 0 {
        let core = overflow_core(&desc);
        // The `TH` ordinal is suppressed on overflow (no integer value to ordinalize).
        let mut d = desc.clone();
        d.ordinal = None;
        return Ok(decorate(&d, core, negative, &rounded));
    }

    // (5) Lay out the digit grid.
    let core = lay_out_digits(&desc, &int_digits, &frac_digits);

    // (6) Decorate with sign + currency, then FM / ordinal.
    Ok(decorate(&desc, core, negative, &rounded))
}

/// `to_char` for `NaN` / `±Infinity`.
///
/// PostgreSQL's `numeric_to_char` runs `numeric_out` first and then lays the
/// resulting STRING into the same digit grid, so a special is treated exactly
/// like a run of integer digits: it fits (`to_char('NaN','999')` → `' NaN'`,
/// `to_char('Infinity','99999999')` → `' Infinity'`), or it `#`-overflows
/// (`to_char('Infinity','999')` → `' ###'`). The decimal point and the
/// fractional positions drop when it fits, because `numeric_out` emitted no
/// `.` for the consumer to place (`to_char('NaN','999.99')` → `' NaN'`). They
/// are `#`-filled like any other position when it overflows
/// (`to_char('Infinity','FM999.999')` → `'###.###'`). All oracle-confirmed
/// against PostgreSQL 18.4.
fn format_special(desc: &NumDesc, value: &NumericValue) -> Result<String, TypeError> {
    let negative = matches!(value, NumericValue::NegInfinity);
    let digits = if value.is_nan() { "NaN" } else { "Infinity" };
    // `decorate` reads the value only to ordinalize it, which never happens here.
    let unused = BigDecimal::from(0);

    if digits.len() > desc.pre || desc.pre == 0 {
        let mut overflowed = desc.clone();
        overflowed.ordinal = None;
        return Ok(decorate(
            &overflowed,
            overflow_core(desc),
            negative,
            &unused,
        ));
    }
    // A `TH`/`th` ordinal over a special that FITS is PostgreSQL's `get_th`
    // failure, 22P02 `"NaN" is not a number`.
    if desc.ordinal.is_some() {
        return Err(TypeError::Domain {
            sqlstate: "22P02",
            message: if value.is_nan() {
                "\"NaN\" is not a number"
            } else {
                "\"Infinity\" is not a number"
            },
        });
    }
    // Lay the spelling into the integer grid alone: no point, no fraction.
    let mut pointless = desc.clone();
    pointless.has_point = false;
    pointless.post = 0;
    let core = lay_out_digits(&pointless, digits, "");
    Ok(decorate(&pointless, core, negative, &unused))
}

/// The `#`-filled overflow CORE: the digit grid only. PG `#`-fills every digit
/// position (a `9` and a `0` alike), renders each group separator as its literal
/// char (there is always a `#` to its left), and places the decimal point; the
/// sign / currency decoration is applied by `decorate`, NOT `#`-filled (oracle-
/// confirmed, PG 18: `to_char(123456,'999')` → `' ###'`, `to_char(123456,'9,99')`
/// → `' #,##'`, `to_char(123456,'L99')` → `'$ ##'`). FM trimming does not apply.
fn overflow_core(desc: &NumDesc) -> String {
    let mut int_out = String::new();
    for idx in 0..desc.pre {
        // A separator sits BEFORE digit slot `idx` when `group_before` records it.
        for &g in &desc.group_before {
            if g == idx && g != 0 {
                int_out.push(',');
            }
        }
        int_out.push('#');
    }
    let mut core = int_out;
    if desc.has_point {
        core.push('.');
        core.push_str(&"#".repeat(desc.post));
    }
    core
}

/// Split a rounded value into (integer-digit-string, fractional-digit-string),
/// where the fractional string is exactly `post` digits (zero-padded/right-trimmed
/// to that width). Always uses the ABSOLUTE value (the sign is handled separately).
fn split_decimal(rounded: &BigDecimal, post: usize) -> (String, String) {
    let abs = rounded.abs();
    // Force exactly `post` fractional digits so the grid lay-out is uniform.
    let scaled = abs.with_scale_round(post as i64, RoundingMode::HalfUp);
    let (mant, scale) = scaled.as_bigint_and_exponent();
    let s = mant.to_string();
    let digits = s.trim_start_matches('-');
    let scale_u = scale.max(0) as usize;
    if scale_u == 0 {
        return (digits.to_string(), String::new());
    }
    if digits.len() > scale_u {
        let point = digits.len() - scale_u;
        (digits[..point].to_string(), digits[point..].to_string())
    } else {
        // |value| < 1: no integer digits, fractional left-padded with zeros.
        (
            "0".to_string(),
            format!("{}{}", "0".repeat(scale_u - digits.len()), digits),
        )
    }
}

/// Lay the integer digits right-to-left into the `pre` positions and the
/// fractional digits left-to-right into the `post` positions, inserting group
/// separators and the decimal point. Produces the bare numeric core (no sign,
/// no currency, no FM trimming yet).
fn lay_out_digits(desc: &NumDesc, int_digits: &str, frac_digits: &str) -> String {
    // Right-align the integer significant digits in `pre` slots. A `9` slot with no
    // significant digit (a leading zero) renders BLANK; a `0` slot renders `0`.
    let int_chars: Vec<char> = int_digits.trim_start_matches('0').chars().collect();
    let mut slots: Vec<char> = vec![' '; desc.pre];
    // Fill from the right with the significant digits.
    let n = int_chars.len();
    for (k, ch) in int_chars.iter().rev().enumerate() {
        if k < desc.pre {
            slots[desc.pre - 1 - k] = *ch;
        }
    }
    // For `0` positions to the LEFT of the first significant digit, force a `0`.
    // `int_zero[i]` (from the left) marks a zero-fill slot. The first significant
    // digit sits at slot `pre - n`; positions `>= pre - n` already hold digits.
    let first_sig = desc.pre.saturating_sub(n);
    for (i, slot) in slots.iter_mut().enumerate().take(first_sig) {
        if desc.int_zero.get(i).copied().unwrap_or(false) {
            *slot = '0';
        }
    }
    // The ones place (the last integer position) for a value with NO significant
    // integer digit (a whole zero or a sub-1 value): PostgreSQL renders a `0` ONLY
    // when the ones-place pattern is a `0`, OR when the template has NO fractional
    // positions; a `9` ones place over a fraction-bearing template BLANKS instead
    // (oracle-confirmed, PG 18: `to_char(0,'9999')` → `'    0'` but
    // `to_char(0.5,'9.9')` → `'  .5'`, `to_char(0.5,'0.9')` → `' 0.5'`).
    if desc.pre > 0 && n == 0 {
        let ones_is_zero_pattern = desc.int_zero.get(desc.pre - 1).copied().unwrap_or(false);
        if ones_is_zero_pattern || desc.post == 0 {
            slots[desc.pre - 1] = '0';
        }
    }

    // Insert group separators. `group_before[k]` = number of integer digit slots
    // to the LEFT of separator k. PG renders the separator as its literal char if
    // there is a printable (non-blank) digit to its left, else blank (oracle-
    // confirmed, PG 18: `to_char(123,'9,999')` → `'   123'` — the comma is blanked
    // because every slot to its left is a suppressed leading zero).
    let mut int_out = String::new();
    for (idx, &slot) in slots.iter().enumerate() {
        // Emit any separators whose position equals `idx` (i.e. they sit BEFORE
        // this slot, counted from the left).
        for &g in &desc.group_before {
            if g == idx && g != 0 {
                let left_blank = slots[..idx].iter().all(|c| *c == ' ');
                int_out.push(if left_blank { ' ' } else { ',' });
            }
        }
        int_out.push(slot);
    }

    let mut core = int_out;
    if desc.has_point {
        core.push('.');
        // Fractional digits, left-to-right, exactly `post` of them.
        let fc: Vec<char> = frac_digits.chars().collect();
        for i in 0..desc.post {
            core.push(fc.get(i).copied().unwrap_or('0'));
        }
    }
    core
}

/// Apply the sign / currency / brackets, then `FM` trimming and the `TH` ordinal,
/// producing the final string.
///
/// Sign placement follows PG's two distinct behaviors:
///  * The DEFAULT sign and `MI`/`PL`/`SG` occupy a FIXED column at the far left
///    (or right) of the field, and the digits do NOT move toward the sign.
///    e.g. `to_char(-12,'MI9999')` → `'-  12'`.
///  * `S` is ANCHORED: the sign floats right up against the number, consuming the
///    blank immediately adjacent to the first/last significant digit.
///    e.g. `to_char(-12,'S9999')` → `'  -12'`.
fn decorate(desc: &NumDesc, core: String, negative: bool, rounded: &BigDecimal) -> String {
    // Under FM, PG suppresses TRAILING fractional zeros (and the decimal point if
    // the whole fraction is dropped): `to_char(148.5,'FM999.999')` → `'148.5'`.
    // Do this on the core BEFORE the sign/blank handling below.
    // The ones place is a `0` pattern (forced) vs a `9` (a sub-1 leading zero that
    // FM may strip).
    let ones_is_zero_pattern =
        desc.pre > 0 && desc.int_zero.get(desc.pre - 1).copied().unwrap_or(false);
    let core = if desc.fill_mode && desc.has_point {
        fm_trim_fraction(&core, ones_is_zero_pattern)
    } else {
        core
    };
    // FM strips the lay-out's leading blanks (suppressed leading zeros) and any
    // trailing padding from the numeric core. (The reserved sign blank is handled
    // per-mode below.)
    let core_for_anchor = core.clone();
    let mut lead = String::new();
    let mut trail = String::new();
    let mut body = core;
    let mut anchored = false;

    match desc.sign {
        SignMode::Default => {
            // PG's DEFAULT sign is ANCHORED: a negative `-` hugs the first significant
            // digit, keeping the grid's leading blanks to its left (oracle-confirmed,
            // PG 18: `to_char(-1,'999')` → `'  -1'`, `to_char(-12,'9999')` → `'  -12'`).
            // A non-negative value reserves ONE leading blank column (FM strips it).
            if negative {
                body = anchor_sign(&core_for_anchor, '-', Anchor::Leading);
                anchored = true;
            } else if !desc.fill_mode {
                lead.push(' ');
            }
        }
        SignMode::S(anchor) => {
            // ANCHORED: inject the sign adjacent to the number's digits.
            let sgn = if negative { '-' } else { '+' };
            body = anchor_sign(&core_for_anchor, sgn, anchor);
            anchored = true;
        }
        SignMode::Mi(anchor) => {
            // FIXED minus column REPLACING the default sign: `-` if negative, else a
            // blank (FM drops the blank). Oracle-confirmed, PG 18: `to_char(-12,
            // 'MI9999')` → `'-  12'`, `to_char(12,'MI9999')` → `'   12'`.
            let ch = if negative {
                Some('-')
            } else if desc.fill_mode {
                None
            } else {
                Some(' ')
            };
            push_fixed(&mut lead, &mut trail, ch, anchor);
        }
        SignMode::Pl(anchor) => {
            // `PL` is ADDITIVE to PG's default sign behavior (oracle-confirmed, PG 18):
            // `to_char(12,'PL99')` → `'+ 12'` (PL's `+`, then the default reserved
            // blank), `to_char(-12,'PL999')` → `'  -12'` (PL's leading blank, then the
            // default `-` ANCHORED to the digit). PL emits `+` for non-negative
            // (including zero) / a blank for negative at its own fixed column.
            let pl_ch = if !negative {
                Some('+')
            } else if desc.fill_mode {
                None
            } else {
                Some(' ')
            };
            push_fixed(&mut lead, &mut trail, pl_ch, anchor);
            // The default sign: an ANCHORED `-` for negative, else a reserved leading
            // blank (FM strips the blank).
            if negative {
                body = anchor_sign(&core_for_anchor, '-', Anchor::Leading);
                anchored = true;
            } else if !desc.fill_mode {
                lead.push(' ');
            }
        }
        SignMode::Sg(anchor) => {
            // FIXED sign column REPLACING the default sign: always `+` or `-`.
            // Oracle-confirmed, PG 18: `to_char(12,'SG99')` → `'+12'`.
            let sgn = if negative { '-' } else { '+' };
            push_fixed(&mut lead, &mut trail, Some(sgn), anchor);
        }
        SignMode::Pr => {
            // `PR` brackets HUG the number, preserving the grid's leading blanks
            // (oracle-confirmed, PG 18): `to_char(-12,'9999PR')` → `'  <12>'` (the two
            // leading blanks of `'  12'` stay, the `<` is glued before the first digit,
            // the `>` is appended). A non-negative value gets a leading + trailing blank
            // in the bracket positions instead.
            if negative {
                body = anchor_sign(&core_for_anchor, '<', Anchor::Leading);
                trail.push('>');
                anchored = true;
            } else {
                lead.push(' ');
                trail.push(' ');
            }
        }
    }

    // Currency marker. Oracle-confirmed against PostgreSQL 18's default (C) locale:
    // both `L` (the `lc_monetary` currency symbol) and `$` render a literal `$` at
    // their anchor (`to_char(485,'L999')` → `'$ 485'`, `to_char(485,'999$')` →
    // `' 485$'`). Currency sits OUTSIDE (left of / right of) the sign column.
    if let Some(anchor) = desc.currency {
        match anchor {
            // Currency is the OUTERMOST leading element (before the sign column), so
            // `L999`(485) → `$` + ` 485` = `$ 485`.
            Anchor::Leading => lead.insert(0, '$'),
            Anchor::Trailing => trail.push('$'),
        }
    }

    // FM trims the lay-out blanks from the core (unless the sign was anchored into
    // it, in which case `anchor_sign` already produced the tight form).
    if desc.fill_mode && !anchored {
        body = body.trim().to_string();
    } else if desc.fill_mode && anchored {
        body = body.trim_start().to_string();
    }

    let mut s = format!("{lead}{body}{trail}");

    // `TH`/`th`: append the ordinal suffix of the integer value. PostgreSQL
    // SUPPRESSES the suffix for a NEGATIVE value (oracle-confirmed, PG 18:
    // `to_char(-12,'FM999TH')` → `'-12'`, `to_char(-1,'999TH')` → `'  -1'`).
    if let Some(upper) = desc.ordinal
        && !negative
    {
        let int_val = rounded.with_scale_round(0, RoundingMode::Down);
        let n = int_val.to_i64().unwrap_or(0);
        s.push_str(&num_ordinal_suffix(n, upper));
    }

    s
}

/// Under FM, drop trailing zeros from the fractional part of `core`, drop a
/// now-bare decimal point, and (when a fraction survives) drop a sub-1 value's
/// sole leading integer `0`. `core` is the laid-out body (it may carry leading
/// blanks from suppressed leading zeros, which the caller trims separately).
/// Oracle-confirmed, PG 18: `to_char(148.5,'FM999.999')` → `148.5`;
/// `to_char(-0.1,'FM9.99')` → `-.1` (the sub-1 leading `0` is dropped BECAUSE a
/// fraction remains); `to_char(0.5,'FM9.9')` → `.5` but `to_char(0.5,'FM0.9')` →
/// `0.5` (a `0`-pattern ones place is forced, never dropped). When the template has
/// fractional positions, FM strips the trailing fraction ZEROS but KEEPS the decimal
/// point: `to_char(5,'FM9.99')` → `5.`, `to_char(100,'FM999.99')` → `100.`,
/// `to_char(0,'FM9.99')` → `0.`. (A template with no point keeps no digit beyond the
/// integer: `to_char(0,'FM9')` → `0`.)
fn fm_trim_fraction(core: &str, ones_is_zero_pattern: bool) -> String {
    match core.split_once('.') {
        Some((int_part, frac)) => {
            let trimmed = frac.trim_end_matches('0');
            // The integer part is "effectively zero" if it has no significant digit
            // (blank — a sub-1 / zero `9` ones place — or a forced `0`).
            let int_is_zero = int_part.trim().is_empty() || int_part.trim() == "0";
            let int_render = if int_is_zero && !ones_is_zero_pattern {
                if trimmed.is_empty() {
                    // A WHOLE zero (no fraction survives): PG shows the `0` (→ `0.`).
                    "0".to_string()
                } else {
                    // A SUB-1 value whose `9` ones place is a leading zero AND a
                    // fraction survives: PG drops the integer `0` (→ `.5`).
                    String::new()
                }
            } else if int_is_zero {
                // A `0`-pattern ones place is forced — always shown as `0`.
                "0".to_string()
            } else {
                int_part.to_string()
            };
            // PG keeps the decimal point even when every fraction digit is stripped.
            format!("{int_render}.{trimmed}")
        }
        None => core.to_string(),
    }
}

/// Push a fixed-column sign char (or nothing) to the leading or trailing side.
fn push_fixed(lead: &mut String, trail: &mut String, ch: Option<char>, anchor: Anchor) {
    if let Some(c) = ch {
        match anchor {
            Anchor::Leading => lead.push(c),
            Anchor::Trailing => trail.push(c),
        }
    }
}

/// Inject an ANCHORED sign (`S`) adjacent to the number. PG keeps the full field
/// width and adds the sign as its own column right before/after the digits:
/// `to_char(-12,'S9999')` → `'  -12'` (the two leading blanks of `'  12'` are
/// preserved and the `-` is inserted just before the `1`). For a trailing anchor
/// the sign is appended after the last char.
fn anchor_sign(core: &str, sgn: char, anchor: Anchor) -> String {
    match anchor {
        Anchor::Trailing => format!("{core}{sgn}"),
        Anchor::Leading => {
            let chars: Vec<char> = core.chars().collect();
            // Insert the sign immediately BEFORE the first non-blank char, keeping
            // all leading blanks to its left (the field widens by one column).
            match chars.iter().position(|c| *c != ' ') {
                Some(p) => {
                    let mut out: String = chars[..p].iter().collect();
                    out.push(sgn);
                    out.extend(&chars[p..]);
                    out
                }
                None => format!("{sgn}{core}"), // all blanks (zero value)
            }
        }
    }
}

/// The English ordinal suffix for `to_char(numeric, 'FM999TH')` etc. Same rule as
/// the date/time engine: keyed off the last two decimal digits (11/12/13 → `th`).
fn num_ordinal_suffix(n: i64, upper: bool) -> String {
    let abs = n.unsigned_abs() % 100;
    let s = if (11..=13).contains(&abs) {
        "th"
    } else {
        match abs % 10 {
            1 => "st",
            2 => "nd",
            3 => "rd",
            _ => "th",
        }
    };
    if upper {
        s.to_ascii_uppercase()
    } else {
        s.to_string()
    }
}

// ---------------------------------------------------------------------------
// dashu-float wrappers: arbitrary-precision exp / ln / sqrt / powf
//
// These thin helpers isolate the dashu API behind a stable interface. Later
// tasks in SP34 call them from within this module to implement the SQL
// math functions `exp`, `ln`, `log10`, `sqrt`, and `power`.
//
// `DBig` (= `FBig<HalfAway, 10>`) is a decimal arbitrary-precision float.
// Precision is set at construction time via `.with_precision(prec).value()`.
// The method forms (`.exp()`, `.ln()`, `.sqrt()`, `.powf()`) use the embedded
// context, so we carry `prec` only to the `num_to_bf` constructor.
// ---------------------------------------------------------------------------
use dashu_float::DBig;

/// Parse a plain-decimal string into a `DBig` with `prec` significant digits.
fn num_to_bf(s: &str, prec: usize) -> DBig {
    use core::str::FromStr;
    DBig::from_str(s)
        .expect("valid decimal text")
        .with_precision(prec)
        .value()
}

/// Render a `DBig` to a plain-decimal string.
/// `DBig`'s `Display` is plain decimal (never scientific notation for finite
/// values), so `to_string()` is correct.
fn bf_to_text(x: &DBig) -> String {
    x.to_string()
}

/// `exp(x)` at `prec` significant digits.
fn bf_exp(x: &DBig, _prec: usize) -> DBig {
    x.exp()
}

/// `ln(x)` at `prec` significant digits; `None` for `x <= 0`.
/// `DBig::ln` panics on non-positive input, so this function guards first.
fn bf_ln(x: &DBig, _prec: usize) -> Option<DBig> {
    // Use comparison to DBig::ZERO: PartialOrd is implemented for DBig.
    // is_zero() is on Repr, so check sign via comparison.
    if *x <= DBig::ZERO {
        return None;
    }
    Some(x.ln())
}

/// `sqrt(x)` at `prec` significant digits; `None` for `x < 0`.
/// `DBig::sqrt` (through the `SquareRoot` trait) panics on negative input, so
/// this function guards first.
fn bf_sqrt(x: &DBig, _prec: usize) -> Option<DBig> {
    if *x < DBig::ZERO {
        return None;
    }
    Some(x.sqrt())
}

/// `pow(base, exp)` at `prec` significant digits; `None` for non-positive base.
/// `DBig::powf` panics on non-positive base, so this function guards first.
fn bf_powf(base: &DBig, exp: &DBig, _prec: usize) -> Option<DBig> {
    if *base <= DBig::ZERO {
        return None;
    }
    Some(base.powf(exp))
}

// ---------------------------------------------------------------------------
// Public transcendental functions (SP34 Task 3)
// ---------------------------------------------------------------------------

/// Significant-digit precision for the dashu computation: cover the result's
/// integer digits + the requested fractional rscale + a guard margin. Saturating
/// (so a degenerate caller cannot panic) and capped. Callers bound the magnitude
/// up front (`MAX_WEIGHT`), so this cap is only defense-in-depth.
fn transc_prec(result_dweight: i64, rscale: i64) -> usize {
    result_dweight
        .max(0)
        .saturating_add(rscale.max(0))
        .saturating_add(16)
        .clamp(24, MAX_WEIGHT + 64) as usize
}

/// Round a dashu result (as text) to `rscale` fractional digits, half-away. The
/// caller guarantees (via an up-front `MAX_WEIGHT` bound) that the rounded value
/// is within the numeric format, so `parse_finite` always succeeds here.
fn finish_transc(value_text: &str, rscale: i64) -> BigDecimal {
    let bd =
        parse_finite(value_text).expect("bounded transcendental result is within numeric format");
    canonical(bd.with_scale_round(rscale, RoundingMode::HalfUp))
}

/// 2201F: square root of a negative number.
fn err_sqrt_negative() -> TypeError {
    TypeError::Domain {
        sqlstate: "2201F",
        message: "cannot take square root of a negative number",
    }
}
/// 2201E: logarithm of zero. PostgreSQL distinguishes this from the negative
/// case: `ln(0)` is "cannot take logarithm of zero", `ln(-1)` is "cannot take
/// logarithm of a negative number".
fn err_log_zero() -> TypeError {
    TypeError::Domain {
        sqlstate: "2201E",
        message: "cannot take logarithm of zero",
    }
}
/// 2201E: logarithm of a negative number.
fn err_log_negative() -> TypeError {
    TypeError::Domain {
        sqlstate: "2201E",
        message: "cannot take logarithm of a negative number",
    }
}
/// 2201F: zero raised to a negative power.
fn err_zero_neg_power() -> TypeError {
    TypeError::Domain {
        sqlstate: "2201F",
        message: "zero raised to a negative power is undefined",
    }
}
/// 2201F: a negative base raised to a non-integer power (complex result).
fn err_neg_noninteger_power() -> TypeError {
    TypeError::Domain {
        sqlstate: "2201F",
        message: "a negative number raised to a non-integer power yields a complex result",
    }
}

/// numeric sqrt; `Err(2201F)` for a negative argument. (sqrt shrinks magnitude,
/// so it never overflows the numeric format.) `sqrt(NaN)` is `NaN`,
/// `sqrt(Infinity)` is `Infinity`, and `sqrt(-Infinity)` raises the same 2201F
/// a negative finite argument does.
pub fn num_sqrt(arg: &NumericValue) -> Result<NumericValue, TypeError> {
    match arg {
        NumericValue::NaN => Ok(NumericValue::NaN),
        NumericValue::Infinity => Ok(NumericValue::Infinity),
        NumericValue::NegInfinity => Err(err_sqrt_negative()),
        NumericValue::Finite(bd) => finite_sqrt(bd).map(NumericValue::Finite),
    }
}

fn finite_sqrt(arg: &BigDecimal) -> Result<BigDecimal, TypeError> {
    let rscale = sqrt_rscale(arg);
    if finite_is_zero(arg) {
        return Ok(canonical(
            BigDecimal::from(0).with_scale_round(rscale, RoundingMode::HalfUp),
        ));
    }
    if arg.sign() == bigdecimal::num_bigint::Sign::Minus {
        return Err(err_sqrt_negative());
    }
    let prec = transc_prec(decimal_weight(arg) / 2, rscale);
    let v = bf_sqrt(&num_to_bf(&finite_to_text(arg), prec), prec).ok_or_else(err_sqrt_negative)?;
    Ok(finish_transc(&bf_to_text(&v), rscale))
}

/// The 2201E a non-positive logarithm argument raises, or `None` when `arg` is
/// a legal (strictly positive) logarithm argument. `-Infinity` counts as
/// negative; `NaN` and `Infinity` are legal and handled by the callers.
fn log_domain_error(arg: &NumericValue) -> Option<TypeError> {
    match arg {
        NumericValue::NegInfinity => Some(err_log_negative()),
        NumericValue::Infinity | NumericValue::NaN => None,
        NumericValue::Finite(bd) if finite_is_zero(bd) => Some(err_log_zero()),
        NumericValue::Finite(bd) if bd.sign() == bigdecimal::num_bigint::Sign::Minus => {
            Some(err_log_negative())
        }
        NumericValue::Finite(_) => None,
    }
}

/// numeric ln; `Err(2201E)` for arg <= 0 (including `-Infinity`). `ln(NaN)` is
/// `NaN` and `ln(Infinity)` is `Infinity`. (ln of an in-format value never
/// overflows, because its magnitude is at most ~`ln(10)·weight`.)
pub fn num_ln(arg: &NumericValue) -> Result<NumericValue, TypeError> {
    if let Some(err) = log_domain_error(arg) {
        return Err(err);
    }
    let bd = match arg {
        NumericValue::NaN => return Ok(NumericValue::NaN),
        NumericValue::Infinity | NumericValue::NegInfinity => return Ok(NumericValue::Infinity),
        NumericValue::Finite(bd) => bd,
    };
    let rscale = ln_rscale(bd);
    let prec = transc_prec(estimate_ln_dweight(bd) + 1, rscale);
    let v = bf_ln(&num_to_bf(&finite_to_text(bd), prec), prec).ok_or_else(err_log_negative)?;
    Ok(NumericValue::Finite(finish_transc(&bf_to_text(&v), rscale)))
}

/// numeric log base 10; same domain and special rules as [`num_ln`].
pub fn num_log10(arg: &NumericValue) -> Result<NumericValue, TypeError> {
    if let Some(err) = log_domain_error(arg) {
        return Err(err);
    }
    let bd = match arg {
        NumericValue::NaN => return Ok(NumericValue::NaN),
        NumericValue::Infinity | NumericValue::NegInfinity => return Ok(NumericValue::Infinity),
        NumericValue::Finite(bd) => bd,
    };
    let rscale = ln_rscale(bd);
    let prec = transc_prec(estimate_ln_dweight(bd) + 1, rscale) + 8;
    // log10(x) = ln(x) / ln(10), both at high precision, then round to rscale.
    let lnx = bf_ln(&num_to_bf(&finite_to_text(bd), prec), prec).ok_or_else(err_log_negative)?;
    let ln10 = bf_ln(&num_to_bf("10", prec), prec).expect("ln(10) defined");
    let lnx_bd = parse_finite(&bf_to_text(&lnx)).expect("ln result is a valid numeric");
    let ln10_bd = parse_finite(&bf_to_text(&ln10)).expect("ln10 is a valid numeric");
    let quotient = (lnx_bd / ln10_bd).with_scale_round(rscale + 4, RoundingMode::HalfUp);
    Ok(NumericValue::Finite(canonical(
        quotient.with_scale_round(rscale, RoundingMode::HalfUp),
    )))
}

/// numeric exp; `Err(22003)` when the result overflows the numeric format.
/// PostgreSQL `exp_var` overflows for `arg >= NUMERIC_MAX_RESULT_SCALE*3 = 6000`
/// (a one-sided bound: a large NEGATIVE argument underflows toward 0, not an error).
pub fn num_exp(arg: &NumericValue) -> Result<NumericValue, TypeError> {
    let arg = match arg {
        NumericValue::NaN => return Ok(NumericValue::NaN),
        NumericValue::Infinity => return Ok(NumericValue::Infinity),
        // `exp(-Infinity)` underflows all the way to an exact 0 (scale 0).
        NumericValue::NegInfinity => return Ok(NumericValue::from(0i64)),
        NumericValue::Finite(bd) => bd,
    };
    // A magnitude beyond f64 range maps to ±∞ by sign, so the >= 6000 test still
    // fires for an enormous positive argument.
    let val = arg
        .to_f64()
        .unwrap_or(if arg.sign() == bigdecimal::num_bigint::Sign::Minus {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        });
    if val >= 6000.0 {
        return Err(TypeError::Overflow);
    }
    let rscale = exp_rscale(arg);
    let result_dweight = (val * std::f64::consts::LOG10_E) as i64;
    let prec = transc_prec(result_dweight, rscale);
    let v = bf_exp(&num_to_bf(&finite_to_text(arg), prec), prec);
    Ok(NumericValue::Finite(finish_transc(&bf_to_text(&v), rscale)))
}

/// numeric power; `Err(2201F)` on a domain error (0^neg, negative^non-integer),
/// `Err(22003)` when the result overflows the numeric format.
///
/// PostgreSQL follows POSIX `pow(3)` for the specials, which is why the two
/// `NaN` rules below are asymmetric: `NaN ^ 0` and `1 ^ NaN` are both `1`, and
/// every other `NaN` combination is `NaN`. An infinite operand then resolves by
/// magnitude: `|base| > 1` diverges under `+Infinity` and vanishes under
/// `-Infinity`, `|base| = 1` is `1`, and a `-Infinity` base still needs an
/// integer exponent. All arms oracle-confirmed against PostgreSQL 18.4.
pub fn num_power(base: &NumericValue, exp: &NumericValue) -> Result<NumericValue, TypeError> {
    if base.is_special() || exp.is_special() {
        return special_power(base, exp);
    }
    let (base, exp) = finite_pair(base, exp);
    finite_power(base, exp).map(NumericValue::Finite)
}

/// [`num_power`] when at least one operand is a special value.
fn special_power(base: &NumericValue, exp: &NumericValue) -> Result<NumericValue, TypeError> {
    if base.is_nan() {
        // NaN ^ 0 = 1; every other NaN base yields NaN.
        return Ok(if exp.is_zero() {
            NumericValue::from(1i64)
        } else {
            NumericValue::NaN
        });
    }
    if exp.is_nan() {
        // 1 ^ NaN = 1; every other NaN exponent yields NaN.
        let base_is_one = base.as_finite() == Some(&BigDecimal::from(1));
        return Ok(if base_is_one {
            NumericValue::from(1i64)
        } else {
            NumericValue::NaN
        });
    }
    // At least one operand is infinite and neither is NaN.
    match base {
        NumericValue::Infinity => {
            return Ok(match exp_direction(exp) {
                Ordering::Greater => NumericValue::Infinity,
                Ordering::Equal => NumericValue::from(1i64),
                Ordering::Less => NumericValue::from(0i64),
            });
        }
        NumericValue::NegInfinity => {
            // A negative base still rejects a non-integer exponent.
            if let Some(e) = exp.as_finite()
                && !e.is_integer()
            {
                return Err(err_neg_noninteger_power());
            }
            let odd = exp
                .as_finite()
                .is_some_and(|e| !finite_is_zero(&(e % BigDecimal::from(2))));
            return Ok(match exp_direction(exp) {
                Ordering::Greater if odd => NumericValue::NegInfinity,
                Ordering::Greater => NumericValue::Infinity,
                Ordering::Equal => NumericValue::from(1i64),
                Ordering::Less => NumericValue::from(0i64),
            });
        }
        NumericValue::NaN | NumericValue::Finite(_) => {}
    }
    // A finite base with an infinite exponent: decide by |base| against 1.
    let magnitude = base
        .as_finite()
        .expect("an infinite base returned above")
        .abs();
    let toward_zero = matches!(exp, NumericValue::NegInfinity);
    match magnitude.cmp(&BigDecimal::from(1)) {
        Ordering::Equal => Ok(NumericValue::from(1i64)),
        Ordering::Greater => Ok(if toward_zero {
            NumericValue::from(0i64)
        } else {
            NumericValue::Infinity
        }),
        Ordering::Less => {
            if toward_zero {
                // `0 ^ -Infinity` is the same 2201F `0 ^ -1` raises.
                if base.is_zero() {
                    return Err(err_zero_neg_power());
                }
                Ok(NumericValue::Infinity)
            } else {
                Ok(NumericValue::from(0i64))
            }
        }
    }
}

/// Whether an exponent pushes an infinite base away from zero, holds it at 1,
/// or collapses it to zero.
fn exp_direction(exp: &NumericValue) -> Ordering {
    if exp.is_zero() {
        return Ordering::Equal;
    }
    match exp.signum() {
        s if s < 0 => Ordering::Less,
        _ => Ordering::Greater,
    }
}

fn finite_power(base: &BigDecimal, exp: &BigDecimal) -> Result<BigDecimal, TypeError> {
    use bigdecimal::num_bigint::Sign;
    if finite_is_zero(base) {
        if exp.sign() == Sign::Minus {
            return Err(err_zero_neg_power());
        }
        let rscale = power_rscale(0, base, exp);
        let value = if finite_is_zero(exp) {
            BigDecimal::from(1)
        } else {
            BigDecimal::from(0)
        };
        return Ok(canonical(
            value.with_scale_round(rscale, RoundingMode::HalfUp),
        ));
    }
    // A negative base with a non-integer exponent is a complex result (check this
    // domain error before the overflow bound).
    if base.sign() == Sign::Minus && !exp.is_integer() {
        return Err(err_neg_noninteger_power());
    }
    // Overflow bound: the result's decimal weight is ≈ exp · log10(|base|). Reject
    // (22003) when it exceeds the numeric format BEFORE materializing it — this
    // bounds both `powi` (exact integer power) and the dashu `powf` path, and also
    // covers an integer exponent too large for i64 (`exp.to_f64()` → ±∞).
    let exp_f64 = exp.to_f64().unwrap_or(if exp.sign() == Sign::Minus {
        f64::NEG_INFINITY
    } else {
        f64::INFINITY
    });
    let base_log10 = base.to_f64().map_or(f64::INFINITY, |b| b.abs().log10());
    let est_weight = exp_f64 * base_log10;
    if est_weight > MAX_WEIGHT as f64 {
        return Err(TypeError::Overflow);
    }
    // exact integer exponent -> powi (handles negative base + negative exponent).
    // PostgreSQL reaches this through `power_var_int`, whose weight estimate is
    // the exact one; the transcendental path below deliberately uses a rounded
    // one instead. C-style truncation toward zero matches PostgreSQL's `(int)`.
    if exp.is_integer()
        && let Some(e) = exp.with_scale_round(0, RoundingMode::HalfUp).to_i64()
    {
        let rscale = power_rscale(est_weight as i64, base, exp);
        return Ok(canonical(
            base.powi(e).with_scale_round(rscale, RoundingMode::HalfUp),
        ));
    }
    // non-integer exponent: base must be > 0 (the negative case returned above).
    let rweight = estimated_power_weight(base, exp_f64);
    let rscale = power_rscale(rweight, base, exp);
    let prec = transc_prec(rweight, rscale);
    let v = bf_powf(
        &num_to_bf(&finite_to_text(base), prec),
        &num_to_bf(&finite_to_text(exp), prec),
        prec,
    )
    .ok_or_else(err_neg_noninteger_power)?;
    Ok(finish_transc(&bf_to_text(&v), rscale))
}

/// PostgreSQL's `power_var` estimate of `base ^ exp`'s decimal weight.
///
/// The estimate is deliberately LOW precision, because PostgreSQL's is: it takes
/// `ln(|base|)` to about eight significant digits, multiplies by the exponent at
/// that same scale, converts to a decimal weight, and truncates toward zero.
/// An exact estimate lands on the other side of an integer boundary whenever the
/// true weight is a whole number. `0.0001 ^ 2.25` weighs exactly -9, and
/// PostgreSQL's rounded intermediate makes that -8.999999988, one display digit
/// fewer.
fn estimated_power_weight(base: &BigDecimal, exp: f64) -> i64 {
    let magnitude = base.abs();
    // `f64` carries about 17 significant digits, so rounding beyond a few hundred
    // decimal places cannot change the value and would overflow the scale factor.
    let local_rscale = (8 - estimate_ln_dweight(&magnitude)).clamp(0, 300);
    let factor = 10f64.powi(i32::try_from(local_rscale).unwrap_or(300));
    let round_at = |v: f64| (v * factor).round() / factor;
    let ln_base = magnitude.to_f64().map_or(f64::INFINITY, f64::ln);
    let ln_num = round_at(round_at(ln_base) * exp);
    // C-style truncation toward zero, matching PostgreSQL's `(int)` cast.
    (ln_num * std::f64::consts::LOG10_E) as i64
}

/// PostgreSQL's display scale for `base ^ exp`, shared by the exact
/// integer-exponent path and the transcendental one because PostgreSQL picks it
/// the same way for both: enough fractional digits for sixteen significant ones
/// at the result's estimated decimal weight `rweight` (≈ `exp · log10|base|`,
/// truncated toward zero), floored by each operand's OWN display scale.
///
/// That floor is what keeps `10.0 ^ 20` at `100000000000000000000.0` and
/// `3.789 ^ 21.0000000000000000` at sixteen fractional digits, where the
/// significant-digit term alone would give none.
fn power_rscale(rweight: i64, base: &BigDecimal, exp: &BigDecimal) -> i64 {
    // `rweight` comes from user input, so the subtraction can leave `i64`;
    // saturating keeps the clamp meaningful instead of wrapping to the other end.
    MIN_SIG_DIGITS
        .saturating_sub(rweight)
        .max(base.fractional_digit_count().max(0))
        .max(exp.fractional_digit_count().max(0))
        .clamp(0, TRANSC_MAX_SCALE)
}

/// PostgreSQL `numeric_stddev_internal`: the shared finalizer behind
/// `var_pop`, `var_samp`, `variance`, `stddev`, `stddev_pop` and `stddev_samp`
/// over a `numeric` input.
///
/// The running state is the row count `n`, `sum` = Σx and `sum2` = Σx². The
/// variance is `(n·Σx² − (Σx)²) / d` with `d = n²` (population) or `n·(n−1)`
/// (sample), computed at [`select_div_scale`]'s display scale, and the standard
/// deviation is that value's square root *at the same scale*. That is why this
/// cannot be composed from the public `div` and `num_sqrt`, whose scales are
/// chosen independently.
///
/// `None` is SQL NULL: the population forms are undefined for zero rows and the
/// sample forms for fewer than two. A numerator driven negative by rounding is
/// clamped to zero, as PostgreSQL clamps it.
///
/// A special value anywhere in the input makes the whole result `NaN`.
/// PostgreSQL's `NumericAggState` counts `NaN`s and infinities separately from
/// the running sums and short-circuits the finalizer on either.
#[must_use]
pub fn stddev_internal(
    n: i64,
    sum: &NumericValue,
    sum2: &NumericValue,
    sample: bool,
    sqrt: bool,
) -> Option<NumericValue> {
    if n <= i64::from(sample) {
        return None;
    }
    if sum.is_special() || sum2.is_special() {
        return Some(NumericValue::NaN);
    }
    let (sum, sum2) = finite_pair(sum, sum2);
    let vn = BigDecimal::from(n);
    let numerator = &vn * sum2 - sum * sum;
    if numerator.sign() != bigdecimal::num_bigint::Sign::Plus {
        return Some(NumericValue::from(0i64));
    }
    let denominator = if sample {
        &vn * BigDecimal::from(n - 1)
    } else {
        &vn * &vn
    };
    let rscale = select_div_scale(&numerator, &denominator);
    let variance = (&numerator / &denominator).with_scale_round(rscale, RoundingMode::HalfUp);
    if !sqrt {
        return Some(NumericValue::Finite(canonical(variance)));
    }
    let prec = transc_prec(decimal_weight(&variance) / 2, rscale);
    let root = bf_sqrt(&num_to_bf(&finite_to_text(&variance), prec), prec)?;
    Some(NumericValue::Finite(finish_transc(
        &bf_to_text(&root),
        rscale,
    )))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn n(s: &str) -> NumericValue {
        parse(s).expect("parse")
    }

    /// The display scale of a quotient, a power, and a logarithm is part of the
    /// answer. `70.0 / 70` is `1.00000000000000000000`, not
    /// `1.0000000000000000`, so a scale that is off by even one digit is a wrong
    /// answer that returns cleanly. Every expectation is a PostgreSQL 18.4
    /// value.
    #[test]
    fn transcendental_and_division_display_scales_match_postgres() {
        use assert2::assert;

        let quotients: [(&str, &str, &str); 6] = [
            // Equal leading digits still assume a quotient below one.
            ("70.0", "70", "1.00000000000000000000"),
            ("1.00", "1.00", "1.00000000000000000000"),
            ("12345.6789", "1.1", "11223.3444545454545455"),
            ("1.0", "3", "0.33333333333333333333"),
            // A display scale is a floor INDIVIDUALLY, not summed.
            (
                "1.00000000000000000000",
                "1.00000000000000000000",
                "1.00000000000000000000",
            ),
            (
                "1.5",
                "0.0000000000000000000000001",
                "15000000000000000000000000.0000000000000000000000000",
            ),
        ];
        for (a, b, expected) in quotients {
            let got = to_text(&div(&n(a), &n(b)).expect("division"));
            assert!(got == expected, "{a} / {b} = {got}");
        }

        let powers: [(&str, &str, &str); 10] = [
            // The significant-digit term.
            ("2", "10", "1024.0000000000000"),
            ("0.2", "2", "0.04000000000000000"),
            ("0.1", "3", "0.0010000000000000000"),
            // ... floored by each operand's own display scale.
            ("10.0", "20", "100000000000000000000.0"),
            ("0.000001", "-3", "1000000000000000000.000000"),
            (
                "3.789",
                "21.0000000000000000",
                "1409343026052.8716016316022141",
            ),
            ("0.5678", "-85", "782333637740774446257.7719"),
            // The transcendental path uses PostgreSQL's ROUNDED weight estimate.
            ("32.1", "9.8", "580429286790711.10"),
            ("32.1", "-9.8", "0.000000000000001722862754788209"),
            ("0.0001", "2.25", "0.000000001000000000000000"),
        ];
        for (base, exp, expected) in powers {
            let got = to_text(&num_power(&n(base), &n(exp)).expect("power"));
            assert!(got == expected, "{base} ^ {exp} = {got}");
        }

        // Between 0.9 and 1.1 the logarithm's weight comes from `arg - 1`, so the
        // result keeps far more digits than a non-negative estimate would allow.
        let logarithms: [(&str, &str); 5] = [
            ("0.99949452", "-0.00050560779808326467"),
            ("1.00049687395", "0.00049675054901370394"),
            ("1.0000000001", "0.00000000009999999999500000"),
            // Outside that window the estimate is unchanged.
            ("2", "0.6931471805599453"),
            ("0.89", "-0.1165338162559515"),
        ];
        for (arg, expected) in logarithms {
            let got = to_text(&num_ln(&n(arg)).expect("ln"));
            assert!(got == expected, "ln({arg}) = {got}");
        }
    }

    fn fin(s: &str) -> BigDecimal {
        parse_finite(s).expect("parse")
    }

    #[test]
    fn parse_canonicalizes_scale_and_rejects_garbage() {
        assert_eq!(to_text(&n("1.50")), "1.50"); // trailing zeros preserved
        assert_eq!(to_text(&n("1e3")), "1000"); // exponent → scale 0
        assert_eq!(to_text(&n("1.5e-3")), "0.0015");
        assert_eq!(to_text(&n("2.")), "2");
        assert_eq!(to_text(&n(".5")), "0.5");
        assert_eq!(to_text(&n("  -7.25 ")), "-7.25");
        assert!(parse("abc").is_none());
        assert!(parse("").is_none());
    }

    #[test]
    fn parse_rejects_values_that_overflow_the_numeric_format() {
        // PostgreSQL's boundary: weight ≤ 131071 (integer side), dscale ≤ 16383.
        // Beyond it PG raises "value overflows numeric format"; we reject (None) —
        // which ALSO prevents the OOM the `decode_row` fuzzer found (an adversarial
        // exponent like `8e88888888` would otherwise materialize ~88M digits).
        assert!(parse("8e88888888").is_none());
        assert!(parse("8e-88888888").is_none());
        assert!(parse("1e131072").is_none()); // just over the weight limit
        assert!(parse("1e-16384").is_none()); // just over the dscale limit
        // The in-range boundary values still parse (PG accepts these).
        assert!(parse("1e131071").is_some());
        assert!(parse("1e-16383").is_some());
    }

    #[test]
    fn text_output_is_plain_decimal_never_scientific() {
        assert_eq!(to_text(&n("1.5e-10")), "0.00000000015");
        assert_eq!(to_text(&n("1e30")), "1000000000000000000000000000000");
        assert_eq!(to_text(&n("0.0")), "0.0");
        assert_eq!(to_text(&n("0")), "0");
        assert_eq!(to_text(&n("-0.0")), "0.0"); // negative zero prints unsigned
        assert_eq!(to_text(&n("100.00")), "100.00");
    }

    #[test]
    fn arithmetic_scale_rules_match_postgres() {
        assert_eq!(to_text(&add(&n("1.50"), &n("1.5"))), "3.00"); // max scale
        assert_eq!(to_text(&sub(&n("2.5"), &n("1.25"))), "1.25");
        assert_eq!(to_text(&mul(&n("1.5"), &n("1.5"))), "2.25"); // scales add
        assert_eq!(to_text(&mul(&n("1.50"), &n("2"))), "3.00");
        assert_eq!(to_text(&add(&n("1e3"), &n("0.0"))), "1000.0");
    }

    #[test]
    fn division_display_scale_matches_select_div_scale() {
        // Cases captured from PostgreSQL 16 (identical to 18).
        for (a, b, want) in [
            ("1.0", "3", "0.33333333333333333333"),
            ("10", "3.0", "3.3333333333333333"),
            ("6.0", "2.0", "3.0000000000000000"),
            ("22.0", "7", "3.1428571428571429"),
            ("100.0", "8", "12.5000000000000000"),
            ("1000000.0", "7", "142857.142857142857"),
            ("0.0001", "7", "0.000014285714285714285714"),
            ("0.3", "3", "0.10000000000000000000"),
            ("1.0", "30000", "0.000033333333333333333333"),
            ("0.0", "3", "0.00000000000000000000"),
        ] {
            assert_eq!(to_text(&div(&n(a), &n(b)).expect("div")), want, "{a}/{b}");
        }
        assert!(matches!(
            div(&n("1.5"), &n("0")),
            Err(TypeError::DivisionByZero)
        ));
    }

    #[test]
    fn numeric_to_int_rounds_half_away_from_zero() {
        // Distinct from float8→int (half-to-even): 2.5 → 3 here.
        assert_eq!(to_i32(&n("2.5")).expect("i"), 3);
        assert_eq!(to_i32(&n("3.5")).expect("i"), 4);
        assert_eq!(to_i32(&n("-2.5")).expect("i"), -3);
        assert_eq!(to_i32(&n("2.4")).expect("i"), 2);
        assert_eq!(to_i64(&n("9999999999")).expect("i"), 9_999_999_999);
        assert!(matches!(
            to_i32(&n("99999999999")),
            Err(TypeError::Overflow)
        ));
    }

    #[test]
    fn float_numeric_conversions_use_shortest_text() {
        assert_eq!(to_text(&from_f64(0.1)), "0.1");
        assert_eq!(to_text(&from_f64(2.5)), "2.5");
        assert_eq!(to_f64(&n("1.5")), 1.5);
    }

    #[test]
    fn numeric_to_float_overflow_preserves_sign() {
        assert_eq!(to_f64(&n("1e131071")), f64::INFINITY);
        assert_eq!(to_f64(&n("-1e131071")), f64::NEG_INFINITY);
        assert_eq!(to_f64(&n("-1.5")), -1.5);
    }

    #[test]
    fn typmod_rounds_to_scale_and_overflows_on_precision() {
        let tm = Typmod {
            precision: 4,
            scale: 1,
        };
        assert_eq!(
            to_text(&apply_typmod(&n("123.45"), tm).expect("ok")),
            "123.5"
        );
        assert!(matches!(
            apply_typmod(&n("1234.5"), tm),
            Err(TypeError::Overflow)
        ));
        let tm2 = Typmod {
            precision: 3,
            scale: 2,
        };
        assert_eq!(to_text(&apply_typmod(&n("9.99"), tm2).expect("ok")), "9.99");
        // rounds to 10.00 → 2 integer digits > precision-scale=1 → overflow.
        assert!(matches!(
            apply_typmod(&n("9.999"), tm2),
            Err(TypeError::Overflow)
        ));
    }

    #[test]
    fn binary_nbase_encoding_matches_numeric_send() {
        // 1.5 → ndigits 2, weight 0, sign +, dscale 1, digits [1, 5000].
        assert_eq!(
            binary(&n("1.5")),
            vec![0, 2, 0, 0, 0, 0, 0, 1, 0, 1, 0x13, 0x88]
        );
        // 0 → ndigits 0, weight 0, sign +, dscale 0.
        assert_eq!(binary(&n("0")), vec![0, 0, 0, 0, 0, 0, 0, 0]);
        // 10000 → ndigits 1, weight 1, dscale 0, digits [1].
        assert_eq!(binary(&n("10000")), vec![0, 1, 0, 1, 0, 0, 0, 0, 0, 1]);
        // -2.5 → ndigits 2, weight 0, sign 0x4000, dscale 1, digits [2, 5000].
        assert_eq!(
            binary(&n("-2.5")),
            vec![0, 2, 0, 0, 0x40, 0, 0, 1, 0, 2, 0x13, 0x88]
        );
    }

    #[test]
    fn binary_nbase_decoding_round_trips_scale_and_rejects_malformed_input() {
        for input in ["0", "1.50", "-2.5", "0.0001", "10000"] {
            let decoded = from_binary(&binary(&n(input))).expect("valid numeric binary");
            assert_eq!(to_text(&decoded), input);
        }
        assert!(from_binary(&[0; 7]).is_none());
        assert!(from_binary(&[0, 1, 0, 0, 0, 0, 0, 0]).is_none());
        // Reserved-but-unassigned sign words stay rejected.
        assert!(from_binary(&[0, 0, 0, 0, 0xe0, 0, 0, 0]).is_none());
        // `numeric_recv` reads and range-checks the digits even for a special,
        // so a short message or an out-of-range group is still malformed.
        assert!(from_binary(&[0, 1, 0, 0, 0xc0, 0, 0, 0]).is_none());
        assert!(from_binary(&[0, 1, 0, 0, 0xc0, 0, 0, 0, 0x27, 0x10]).is_none());
        assert_eq!(
            from_binary(&[0, 1, 0, 0, 0xc0, 0, 0, 0, 0, 5]),
            Some(NumericValue::NaN)
        );
    }

    #[test]
    fn abs_and_rem_match_postgres() {
        assert_eq!(to_text(&abs(&n("-2.5"))), "2.5");
        assert_eq!(to_text(&abs(&n("2.5"))), "2.5");
        // mod takes the dividend's sign; a zero divisor is 22012.
        assert_eq!(to_text(&rem(&n("7.5"), &n("2")).expect("rem")), "1.5");
        assert_eq!(to_text(&rem(&n("-7.5"), &n("2")).expect("rem")), "-1.5");
        assert!(matches!(
            rem(&n("1.5"), &n("0")),
            Err(TypeError::DivisionByZero)
        ));
    }

    #[test]
    fn grouping_equality_ignores_scale() {
        // 1.50 and 1.5 are the same value (PG grouping equality).
        assert_eq!(n("1.50"), n("1.5"));
        assert_eq!(
            NumericValue::Finite(BigDecimal::from_str("1.50").expect("x")),
            n("1.5")
        );
        assert_ne!(n("1.5"), n("1.6"));
    }

    #[test]
    fn rounding_primitives_match_postgres() {
        let n = |s: &str| parse(s).expect("parse");
        // floor toward −∞, ceil toward +∞ (scale 0)
        assert_eq!(to_text(&floor(&n("2.9"))), "2");
        assert_eq!(to_text(&floor(&n("-2.1"))), "-3");
        assert_eq!(to_text(&ceil(&n("2.1"))), "3");
        assert_eq!(to_text(&ceil(&n("-2.9"))), "-2");
        // round half-away-from-zero; preserves requested scale
        assert_eq!(to_text(&round(&n("2.5"), 0)), "3");
        assert_eq!(to_text(&round(&n("-2.5"), 0)), "-3");
        assert_eq!(to_text(&round(&n("2.567"), 2)), "2.57");
        assert_eq!(to_text(&round(&n("1234"), -2)), "1200");
        // trunc toward zero
        assert_eq!(to_text(&trunc(&n("2.99"), 0)), "2");
        assert_eq!(to_text(&trunc(&n("-2.99"), 0)), "-2");
        assert_eq!(to_text(&trunc(&n("2.567"), 1)), "2.5");
        // sign
        assert_eq!(to_text(&sign(&n("-5.5"))), "-1");
        assert_eq!(to_text(&sign(&n("0"))), "0");
        assert_eq!(to_text(&sign(&n("0.3"))), "1");
    }

    #[test]
    fn dashu_wrappers_compute_known_values() {
        let p = 40; // 40 significant digits — plenty for these checks.
        // exp(0) = 1
        assert_eq!(bf_to_text(&bf_exp(&num_to_bf("0", p), p)), "1");
        // sqrt(2) starts 1.41421356237309504880…
        let s2 = bf_to_text(&bf_sqrt(&num_to_bf("2", p), p).expect("sqrt"));
        assert!(s2.starts_with("1.4142135623730950488"), "got {s2}");
        // ln(2) starts 0.69314718055994530941…
        let l2 = bf_to_text(&bf_ln(&num_to_bf("2", p), p).expect("ln"));
        assert!(l2.starts_with("0.6931471805599453094"), "got {l2}");
        // powf(2, 0.5) ≈ sqrt(2)
        let p2 = bf_to_text(&bf_powf(&num_to_bf("2", p), &num_to_bf("0.5", p), p).expect("powf"));
        assert!(p2.starts_with("1.4142135623730950488"), "got {p2}");
        // domain guards: ln of non-positive, sqrt of negative -> None; but sqrt(0)
        // is DEFINED (the guard is `< 0`, not `<= 0`).
        assert!(bf_ln(&num_to_bf("0", p), p).is_none());
        assert!(bf_sqrt(&num_to_bf("-1", p), p).is_none());
        assert_eq!(
            bf_to_text(&bf_sqrt(&num_to_bf("0", p), p).expect("sqrt0")),
            "0"
        );
    }

    #[test]
    fn rscale_rules_match_postgres() {
        let n = fin;
        // sqrt: rscale = clamp(16 - (w*2 + 1), max(dscale,0), 1000), w = base-10000 weight
        assert_eq!(sqrt_rscale(&n("2")), 15);
        assert_eq!(sqrt_rscale(&n("1000000")), 13);
        assert_eq!(sqrt_rscale(&n("0.04")), 17);
        // exp: rscale = clamp(16 - trunc(val * log10(e)), 0, 1000)
        assert_eq!(exp_rscale(&n("1")), 16);
        assert_eq!(exp_rscale(&n("2.5")), 15);
        assert_eq!(exp_rscale(&n("10")), 12);
        assert_eq!(exp_rscale(&n("100")), 0);
        assert_eq!(exp_rscale(&n("-5")), 18);
        // ln/log: rscale = clamp(16 - estimate_ln_dweight, max(dscale,0), 1000)
        assert_eq!(ln_rscale(&n("2")), 16);
        assert_eq!(ln_rscale(&n("1000000")), 15);
        assert_eq!(ln_rscale(&n("0.000001")), 15);
        assert_eq!(ln_rscale(&n("0.0001")), 16);
        assert_eq!(ln_rscale(&n("1000000000000")), 15);
        // decimal_weight: position of the leading significant digit
        assert_eq!(decimal_weight(&n("1234")), 3);
        assert_eq!(decimal_weight(&n("0.0067")), -3);
        assert_eq!(decimal_weight(&n("0")), 0);
    }

    #[test]
    fn numeric_transcendentals_match_postgres() {
        let t = |v: &NumericValue| to_text(v);
        let n = |s: &str| parse(s).expect("parse");
        assert_eq!(t(&num_sqrt(&n("2")).expect("sqrt")), "1.414213562373095");
        assert_eq!(t(&num_sqrt(&n("4")).expect("sqrt")), "2.000000000000000");
        assert_eq!(
            t(&num_sqrt(&n("0.04")).expect("sqrt")),
            "0.20000000000000000"
        );
        assert!(num_sqrt(&n("-1")).is_err());
        assert_eq!(t(&num_ln(&n("2")).expect("ln")), "0.6931471805599453");
        assert_eq!(t(&num_ln(&n("1000000")).expect("ln")), "13.815510557964274");
        assert!(num_ln(&n("0")).is_err());
        assert_eq!(t(&num_log10(&n("100")).expect("log")), "2.0000000000000000");
        // a NON-exact log/ln (every digit matters) pins the `ln(x)/ln(10)` division
        // precision + intermediate scale — exact powers of ten alone can't.
        assert_eq!(t(&num_log10(&n("2")).expect("log")), "0.3010299956639812");
        assert_eq!(t(&num_log10(&n("5")).expect("log")), "0.6989700043360188");
        assert_eq!(
            t(&num_log10(&n("1000000")).expect("log")),
            "6.000000000000000"
        );
        assert_eq!(t(&num_exp(&n("0")).expect("exp")), "1.0000000000000000");
        assert_eq!(t(&num_exp(&n("1")).expect("exp")), "2.7182818284590452");
        assert_eq!(t(&num_exp(&n("10")).expect("exp")), "22026.465794806717");
        // power: exact integer exponent (incl. negative + large), and non-integer via powf
        assert_eq!(
            t(&num_power(&n("2"), &n("10")).expect("pow")),
            "1024.0000000000000"
        );
        assert_eq!(
            t(&num_power(&n("2"), &n("3")).expect("pow")),
            "8.0000000000000000"
        );
        assert_eq!(
            t(&num_power(&n("3"), &n("4")).expect("pow")),
            "81.000000000000000"
        );
        assert_eq!(
            t(&num_power(&n("-2"), &n("3")).expect("pow")),
            "-8.0000000000000000"
        );
        assert_eq!(
            t(&num_power(&n("5"), &n("-2")).expect("pow")),
            "0.04000000000000000"
        );
        assert_eq!(
            t(&num_power(&n("2"), &n("100")).expect("pow")),
            "1267650600228229401496703205376"
        );
        assert_eq!(
            t(&num_power(&n("2"), &n("0.5")).expect("pow")),
            "1.4142135623730950"
        );
        assert!(num_power(&n("0"), &n("-1")).is_err()); // 0^negative -> domain error
        assert!(num_power(&n("-2"), &n("0.5")).is_err()); // negative^non-integer -> domain error
        // overflow guards (22003): exp(>=6000), an over-format power, and an
        // integer exponent too large for i64 — none must panic or hang.
        assert!(matches!(num_exp(&n("6000")), Err(TypeError::Overflow)));
        assert!(num_exp(&n("5999")).is_ok());
        assert!(matches!(
            num_power(&n("10"), &n("200000")),
            Err(TypeError::Overflow)
        ));
        assert!(num_power(&n("10"), &n("5000")).is_ok()); // 5001 digits, comfortably in-format
        // huge integer exponent: error, not panic
        assert!(matches!(
            num_power(&n("10"), &n("1e30")),
            Err(TypeError::Overflow)
        ));
        // --- rscale/overflow-estimate edges (pin the exact arithmetic) ---
        let t = |v: &NumericValue| to_text(v);
        // is_power_of_ten: an exact power-of-ten integer-power result keeps one
        // EXTRA fractional digit (19), vs 18 for a non-power-of-ten sub-1 result.
        assert_eq!(
            t(&num_power(&n("10"), &n("-3")).expect("p")),
            "0.0010000000000000000"
        );
        // non-integer-power rscale = 16 - (exp · decimal_weight(base)): for
        // power(1000, 0.5) that is 16 - (0.5·3 → 1) = 15 fractional digits. A `+`/`/`
        // mutation of the `exp·weight` product, or a `+` for the `16 - rweight`,
        // changes the digit count.
        assert_eq!(
            t(&num_power(&n("1000"), &n("0.5")).expect("p")),
            "31.622776601683793"
        );
        // The overflow estimate is `exp · log10(base)`: power(2, 200000) has weight
        // ≈ 60206 (in-format), so it must NOT be rejected — a `+`/`/` mutation of the
        // product would wrongly compute ≈200000 / ≈664000 and overflow it.
        assert!(num_power(&n("2"), &n("200000")).is_ok());
    }

    #[test]
    fn round_trunc_clamp_scale_to_avoid_oom() {
        let n = |s: &str| parse(s).expect("parse");
        // An adversarially huge scale must not materialize billions of digits:
        // it is clamped to MAX_DSCALE, so the result stays bounded.
        let scale = |v: &NumericValue| v.as_finite().expect("finite").fractional_digit_count();
        assert!(scale(&round(&n("2.5"), 2_000_000_000)) <= MAX_DSCALE);
        assert!(scale(&trunc(&n("2.5"), 2_000_000_000)) <= MAX_DSCALE);
        // Ordinary scales are unaffected.
        assert_eq!(to_text(&round(&n("2.567"), 2)), "2.57");
    }

    // ----- SP38: numeric `to_char` (`format_numeric`) -----

    #[test]
    fn format_numeric_core() {
        use super::{format_numeric, parse};
        let n = |s: &str| parse(s).expect(s);
        let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
        // default reserves a leading sign column → leading blank for non-negative.
        assert_eq!(fmt("485", "999"), " 485");
        assert_eq!(fmt("-485", "999"), "-485");
        assert_eq!(fmt("485", "FM999"), "485"); // FM strips the sign blank
        assert_eq!(fmt("485", "0999"), " 0485"); // 0 forces a leading zero
        assert_eq!(fmt("12", "99"), " 12");
        assert_eq!(fmt("1234567", "9,999,999"), " 1,234,567");
        assert_eq!(fmt("1234567", "FM9,999,999"), "1,234,567");
        assert_eq!(fmt("1234.5", "9,999.9"), " 1,234.5");
        // rounding to the fractional digit count (half away from zero).
        assert_eq!(fmt("1.235", "9.99"), " 1.24");
    }

    #[test]
    fn format_numeric_digit_positions_and_blanks() {
        use super::{format_numeric, parse};
        let n = |s: &str| parse(s).expect(s);
        let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
        // A `9` suppresses a leading zero (renders blank); a `0` zero-fills.
        assert_eq!(fmt("12", "9999"), "   12"); // sign col + 2 blanks + "12"
        assert_eq!(fmt("12", "0000"), " 0012");
        assert_eq!(fmt("12", "FM9999"), "12"); // FM trims leading blanks
        // PG renders the ones place even for a zero `9`-value: to_char(0,'9') → ' 0'.
        assert_eq!(fmt("0", "9"), " 0"); // sign col + forced ones-place zero
        assert_eq!(fmt("0", "0"), " 0"); // sign col + forced zero
        assert_eq!(fmt("0", "FM9"), "0"); // FM trims the sign blank → "0"
        assert_eq!(fmt("0", "FM0"), "0");
        // Fractional zero-fill always shows (non-FM); FM drops trailing zeros.
        assert_eq!(fmt("1.5", "9.999"), " 1.500");
        assert_eq!(fmt("1.5", "FM9.999"), "1.5");
    }

    #[test]
    fn format_numeric_rounding_half_away_from_zero() {
        use super::{format_numeric, parse};
        let n = |s: &str| parse(s).expect(s);
        let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
        assert_eq!(fmt("1.235", "9.99"), " 1.24"); // .235 → .24 (half away)
        assert_eq!(fmt("1.245", "9.99"), " 1.25");
        assert_eq!(fmt("-1.235", "9.99"), "-1.24");
        assert_eq!(fmt("2.5", "9"), " 3"); // .5 rounds the integer up
        assert_eq!(fmt("-2.5", "9"), "-3");
        // Rounding can carry into a new integer digit (still fits 999).
        assert_eq!(fmt("99.6", "999"), " 100");
    }

    #[test]
    fn format_numeric_groups() {
        use super::{format_numeric, parse};
        let n = |s: &str| parse(s).expect(s);
        let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
        assert_eq!(fmt("1234567", "9,999,999"), " 1,234,567");
        // `G` is the same as `,`.
        assert_eq!(fmt("1234567", "9G999G999"), " 1,234,567");
        // A separator whose entire left side is blank renders blank (oracle-confirmed,
        // PG 18: `to_char(12,'9,999')` → `'    12'`).
        assert_eq!(fmt("12", "9,999"), "    12");
        assert_eq!(fmt("1234", "9,999"), " 1,234");
    }

    #[test]
    fn format_numeric_groups_fm() {
        use super::{format_numeric, parse};
        let n = |s: &str| parse(s).expect(s);
        let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
        assert_eq!(fmt("1234567", "FM9,999,999"), "1,234,567");
        assert_eq!(fmt("1234.5", "9,999.9"), " 1,234.5");
    }

    #[test]
    fn format_numeric_fm_trims_trailing_fraction_zeros() {
        use super::{format_numeric, parse};
        let n = |s: &str| parse(s).expect(s);
        let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
        // PG: to_char(148.5,'FM999.999') → '148.5' (trailing fraction zeros gone).
        assert_eq!(fmt("148.5", "FM999.999"), "148.5");
        // PG: to_char(-0.1,'FM9.99') → '-.1'.
        assert_eq!(fmt("-0.1", "FM9.99"), "-.1");
        // FM strips the trailing fraction ZEROS but KEEPS the decimal point when the
        // template has fractional positions (oracle-confirmed, PG 18: 'FM9.99' over 5
        // → '5.', over 100 → '100.', over 0 → '0.').
        assert_eq!(fmt("5", "FM9.99"), "5.");
        assert_eq!(fmt("100", "FM999.99"), "100.");
        assert_eq!(fmt("0", "FM9.99"), "0.");
        // Without FM the trailing zeros are kept (and padding blank).
        assert_eq!(fmt("148.5", "999.999"), " 148.500");
        // A `0`-pattern ones place is KEPT under FM (it is forced), unlike a `9`.
        // PG: to_char(0.5,'FM9.9') → '.5' ; to_char(0.5,'FM0.9') → '0.5'.
        assert_eq!(fmt("0.5", "FM9.9"), ".5");
        assert_eq!(fmt("0.5", "FM0.9"), "0.5");
        // A whole zero with NO fraction keeps its digit (PG: to_char(0,'FM9') → '0').
        assert_eq!(fmt("0", "FM9"), "0");
    }

    #[test]
    fn format_numeric_decimal_point_d() {
        use super::{format_numeric, parse};
        let n = |s: &str| parse(s).expect(s);
        let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
        // `D` is the locale decimal point (C locale → `.`).
        assert_eq!(fmt("12.34", "99D99"), " 12.34");
        assert_eq!(fmt("12.34", "99.99"), " 12.34");
    }

    #[test]
    fn format_numeric_sign_modes() {
        use super::{format_numeric, parse};
        let n = |s: &str| parse(s).expect(s);
        let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
        // S — leading sign glued to the number (always shows + or -). Oracle-pinned.
        assert_eq!(fmt("485", "S999"), "+485");
        assert_eq!(fmt("-485", "S999"), "-485");
        // S — trailing.
        assert_eq!(fmt("485", "999S"), "485+");
        assert_eq!(fmt("-485", "999S"), "485-");
        // MI — trailing minus, blank if non-negative.
        assert_eq!(fmt("485", "999MI"), "485 ");
        assert_eq!(fmt("-485", "999MI"), "485-");
        assert_eq!(fmt("485", "FM999MI"), "485"); // FM drops the blank
        // PL is ADDITIVE to the default sign (PG 18): `+` then the reserved blank for
        // non-negative; a leading blank then the default `-` ANCHORED to the digit for
        // negative (`to_char(-12,'PL999')` → `'  -12'`).
        assert_eq!(fmt("485", "PL999"), "+ 485");
        assert_eq!(fmt("-485", "PL999"), " -485");
        assert_eq!(fmt("-12", "PL999"), "  -12");
        assert_eq!(fmt("485", "999PL"), " 485+");
        assert_eq!(fmt("-1", "999PL"), "  -1 ");
        // SG — plus or minus REPLACING the default sign column.
        assert_eq!(fmt("485", "SG999"), "+485");
        assert_eq!(fmt("-485", "SG999"), "-485");
    }

    #[test]
    fn format_numeric_pr_brackets() {
        use super::{format_numeric, parse};
        let n = |s: &str| parse(s).expect(s);
        let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
        // PR — negative wrapped in <…> (brackets HUG the number, leading grid blanks
        // preserved); non-negative gets a leading + trailing space. Oracle-pinned.
        assert_eq!(fmt("-485", "999PR"), "<485>");
        assert_eq!(fmt("485", "999PR"), " 485 ");
        // The bracket hugs the digit when the grid is wider than the number.
        assert_eq!(fmt("-12", "9999PR"), "  <12>");
    }

    #[test]
    fn format_numeric_currency() {
        use super::{format_numeric, parse};
        let n = |s: &str| parse(s).expect(s);
        let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
        // Currency `L`/`$`: PostgreSQL 18's C/default locale emits a literal `$`
        // (oracle-confirmed: `to_char(485,'L999')` → `'$ 485'`). `$` is also a
        // currency anchor. Both render `$` at the anchor, outside the sign column.
        assert_eq!(fmt("485", "L999"), "$ 485");
        assert_eq!(fmt("485", "$999"), "$ 485");
        assert_eq!(fmt("485", "999L"), " 485$");
    }

    #[test]
    fn format_numeric_v_shift() {
        use super::{format_numeric, parse};
        let n = |s: &str| parse(s).expect(s);
        let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
        // V shifts left by the number of 9/0 digits FOLLOWING it (multiply by 10^n).
        // `to_char(12.4, '99V999')` → 12.4 * 1000 = 12400 → "12400". Oracle-pinned.
        assert_eq!(fmt("12.4", "99V999"), " 12400");
        assert_eq!(fmt("1", "9V9"), " 10");
    }

    #[test]
    fn format_numeric_th_ordinal() {
        use super::{format_numeric, parse};
        let n = |s: &str| parse(s).expect(s);
        let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
        // TH/th append the ordinal suffix of the integer value. Oracle-pinned.
        assert_eq!(fmt("1", "FM9TH"), "1ST");
        assert_eq!(fmt("2", "FM9th"), "2nd");
        assert_eq!(fmt("11", "FM99TH"), "11TH");
        assert_eq!(fmt("23", "FM99TH"), "23RD");
        // PG SUPPRESSES the ordinal for a NEGATIVE value.
        assert_eq!(fmt("-12", "FM999TH"), "-12");
        assert_eq!(fmt("-1", "999TH"), "  -1");
    }

    #[test]
    fn format_numeric_blank_zero_is_noop() {
        use super::{format_numeric, parse};
        let n = |s: &str| parse(s).expect(s);
        let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
        // `B` (blank-on-zero) is a NO-OP in PostgreSQL 18 — a zero renders normally
        // (oracle-confirmed: `to_char(0,'B9999')` → `'    0'`, NOT blank).
        assert_eq!(fmt("0", "B9999"), "    0");
        assert_eq!(fmt("0", "B0000"), " 0000");
        assert_eq!(fmt("12", "B9999"), "   12"); // non-zero unaffected
    }

    #[test]
    fn format_numeric_overflow_fill() {
        use super::{format_numeric, parse};
        let n = |s: &str| parse(s).expect(s);
        let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
        // Integer part wider than the template → `#`-fill the DIGIT positions, but the
        // sign column renders normally (oracle-confirmed, PG 18). Default non-negative
        // reserves the leading blank: `to_char(1234,'999')` → `' ###'`.
        assert_eq!(fmt("1234", "999"), " ###");
        // With a fractional part: blank + 3 int `#` + point + 2 frac `#`.
        assert_eq!(fmt("1234.5", "999.99"), " ###.##");
        // A negative overflow keeps the anchored `-`; a separator stays literal.
        assert_eq!(fmt("-1234", "999"), "-###");
        assert_eq!(fmt("123456", "9,99"), " #,##");
        // FM drops the leading sign blank.
        assert_eq!(fmt("1234", "FM999"), "###");
    }

    #[test]
    fn format_numeric_negatives_and_zero_edges() {
        use super::{format_numeric, parse};
        let n = |s: &str| parse(s).expect(s);
        let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
        // -0 (rounds to zero) is NOT negative → no `-`. A sub-1 value over a `9`
        // ones place + a fraction BLANKS the integer (PG 18): to_char(-0.001,'9.9')
        // → '  .0'.
        assert_eq!(fmt("-0.001", "9.9"), "  .0");
        // A value <1 BLANKS the ones-place `9` when a fraction is present (PG: '  .5').
        assert_eq!(fmt("0.5", "9.9"), "  .5");
        // ... but a `0` ones place zero-fills the integer position (PG: ' 0.5').
        assert_eq!(fmt("0.5", "0.9"), " 0.5");
    }

    #[test]
    fn format_numeric_edge_cases() {
        use super::{format_numeric, parse};
        let n = |s: &str| parse(s).expect(s);
        let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
        // Rounding carries into a new integer digit that no longer fits → overflow
        // (`#`-filled digits, leading sign blank): 99.6 → 100, 3 digits > 2 positions.
        assert_eq!(fmt("99.6", "99"), " ##");
        // A negative value with a currency marker (anchored `-` + leading currency).
        assert_eq!(fmt("-485", "L999"), "$-485");
        // Trailing currency on a negative.
        assert_eq!(fmt("-485", "999L"), "-485$");
        // A V-shift with a fractional input that rounds.
        // to_char(12.45, '99V9') → 12.45*10 = 124.5 → round to 0 frac → 125.
        assert_eq!(fmt("12.45", "99V9"), " 125");
        // An ABSURD V-shift must not panic (bounded by the format-limit fallback).
        let _ = format_numeric("9V999999999", &n("1")); // just must not panic
        // No integer positions at all (template `.99`) → PG `#`-overflows for ANY
        // value, since not even the implicit leading `0` fits (oracle-confirmed).
        assert_eq!(fmt("0.25", ".99"), " .##");
        assert_eq!(fmt("0", ".99"), " .##");
        // Group separator with a fully-blank left side renders blank, not ','.
        assert_eq!(fmt("5", "9,999"), "     5");
    }

    // ----- special values: NaN and ±Infinity -----

    #[test]
    fn parses_every_postgres_special_spelling() {
        use assert2::assert;
        for (text, want) in [
            ("NaN", NumericValue::NaN),
            ("nan", NumericValue::NaN),
            ("NAN", NumericValue::NaN),
            ("nAn", NumericValue::NaN),
            ("  NaN  ", NumericValue::NaN),
            ("Infinity", NumericValue::Infinity),
            ("infinity", NumericValue::Infinity),
            ("INFINITY", NumericValue::Infinity),
            ("inf", NumericValue::Infinity),
            ("INF", NumericValue::Infinity),
            ("+inf", NumericValue::Infinity),
            ("+Infinity", NumericValue::Infinity),
            ("  +inf ", NumericValue::Infinity),
            ("-Infinity", NumericValue::NegInfinity),
            ("-infinity", NumericValue::NegInfinity),
            ("-inf", NumericValue::NegInfinity),
            ("  -inf  ", NumericValue::NegInfinity),
        ] {
            assert!(parse(text) == Some(want), "{text}");
        }
        // PostgreSQL takes no sign on NaN and no trailing junk on inf.
        for text in [
            "-nan",
            "+nan",
            "infi",
            "infinit",
            "infinityy",
            " + inf",
            "abc",
            "",
        ] {
            assert!(parse(text).is_none(), "{text}");
        }
        // The JSON-facing parser never accepts a special.
        for text in ["NaN", "inf", "-Infinity"] {
            assert!(parse_finite(text).is_none(), "{text}");
        }
    }

    #[test]
    fn parses_postgres_16_separators_and_non_decimal_input() {
        use assert2::assert;
        // Every expectation oracle-confirmed against PostgreSQL 18.4.
        for (text, want) in [
            ("1_0", "10"),
            ("1_0.5", "10.5"),
            (".5_5", "0.55"),
            ("1.5_5", "1.55"),
            ("1_000.000_1", "1000.0001"),
            ("1.5e1_0", "15000000000"),
            ("1_0e1_0", "100000000000"),
            ("23_000_000_000e-1_0", "2.3000000000"),
            (".000_000_000_123e+1_1", "12.3"),
            ("0xff", "255"),
            ("0X1f", "31"),
            ("-0xff", "-255"),
            ("0x_1F", "31"),
            ("0x30b1_F33a", "816968506"),
            ("0xF_F", "255"),
            ("0b1010_1011", "171"),
            ("0o12_34", "668"),
            ("0x_F_F", "255"),
            (
                " 0X_30b1_F33a_6DF0_bD4E_64DF_9BdA_7D15 ",
                "987654321234567898765432123456789",
            ),
            ("0x0000000000000000000000000deadbeef", "3735928559"),
            ("0b10001110111100111100001001010", "299792458"),
            ("0b_1", "1"),
            ("  +0o112402761777 ", "9999999999"),
            ("0o_7", "7"),
            ("0x0", "0"),
        ] {
            let got = parse(text).unwrap_or_else(|| panic!("{text} should parse"));
            assert!(to_text(&got) == want, "{text}");
        }
        for text in [
            "_10", "10_", "1__0", "1_.5", "1._5", "1e_5", "1e5_", "0x", "0x_", "0x1F_", "0xG",
            "00x1", "0b2", "0o8", "0x1.5", "+_10", "0xF_", "0xF__F", "0b1_2",
        ] {
            assert!(parse(text).is_none(), "{text}");
        }
        // The JSON number grammar has neither form.
        for text in ["1_0", "0xff"] {
            assert!(parse_finite(text).is_none());
        }
    }

    #[test]
    fn special_output_spelling_matches_numeric_out() {
        use assert2::assert;
        assert!(to_text(&NumericValue::NaN) == "NaN");
        assert!(to_text(&NumericValue::Infinity) == "Infinity");
        assert!(to_text(&NumericValue::NegInfinity) == "-Infinity");
        assert!(NumericValue::NegInfinity.to_string() == "-Infinity");
    }

    #[test]
    fn special_arithmetic_matches_postgres() {
        use assert2::assert;
        let cases: &[(&str, &str, &str, &str, &str)] = &[
            // (a, b, a+b, a-b, a*b)
            ("0", "inf", "Infinity", "-Infinity", "NaN"),
            ("0", "-inf", "-Infinity", "Infinity", "NaN"),
            ("1", "inf", "Infinity", "-Infinity", "Infinity"),
            ("-1", "inf", "Infinity", "-Infinity", "-Infinity"),
            ("4.2", "-inf", "-Infinity", "Infinity", "-Infinity"),
            ("inf", "inf", "Infinity", "NaN", "Infinity"),
            ("inf", "-inf", "NaN", "Infinity", "-Infinity"),
            ("-inf", "-inf", "-Infinity", "NaN", "Infinity"),
            ("inf", "0", "Infinity", "Infinity", "NaN"),
            ("nan", "1", "NaN", "NaN", "NaN"),
            ("nan", "inf", "NaN", "NaN", "NaN"),
            ("1", "nan", "NaN", "NaN", "NaN"),
        ];
        for (a, b, sum, diff, prod) in cases {
            let (x, y) = (n(a), n(b));
            assert!(to_text(&add(&x, &y)) == *sum, "{a} + {b}");
            assert!(to_text(&sub(&x, &y)) == *diff, "{a} - {b}");
            assert!(to_text(&mul(&x, &y)) == *prod, "{a} * {b}");
        }
        assert!(to_text(&neg(&n("inf"))) == "-Infinity");
        assert!(to_text(&neg(&n("-inf"))) == "Infinity");
        assert!(to_text(&neg(&n("nan"))) == "NaN");
    }

    #[test]
    fn special_division_and_modulo_order_the_zero_check_after_nan() {
        use assert2::assert;
        // (a, b, a/b, a%b, div(a,b)) — every value oracle-checked on PG 18.4.
        let cases: &[(&str, &str, &str, &str, &str)] = &[
            ("inf", "1", "Infinity", "NaN", "Infinity"),
            ("inf", "-1", "-Infinity", "NaN", "-Infinity"),
            ("-inf", "4.2", "-Infinity", "NaN", "-Infinity"),
            ("1", "inf", "0", "1", "0"),
            ("-1", "inf", "0", "-1", "0"),
            ("4.2", "-inf", "0", "4.2", "0"),
            ("inf", "inf", "NaN", "NaN", "NaN"),
            ("nan", "1", "NaN", "NaN", "NaN"),
            ("1", "nan", "NaN", "NaN", "NaN"),
        ];
        for (a, b, quot, rem_, divt) in cases {
            let (x, y) = (n(a), n(b));
            assert!(to_text(&div(&x, &y).expect("div")) == *quot, "{a} / {b}");
            assert!(to_text(&rem(&x, &y).expect("mod")) == *rem_, "{a} % {b}");
            assert!(
                to_text(&div_trunc(&x, &y).expect("div_trunc")) == *divt,
                "div({a}, {b})"
            );
        }
        // A NaN operand beats a zero divisor; an infinite one does not.
        let zero = n("0");
        assert!(to_text(&div(&n("nan"), &zero).expect("nan/0")) == "NaN");
        assert!(to_text(&rem(&n("nan"), &zero).expect("nan%0")) == "NaN");
        assert!(to_text(&div_trunc(&n("nan"), &zero).expect("div")) == "NaN");
        for a in ["inf", "-inf", "0"] {
            assert!(matches!(div(&n(a), &zero), Err(TypeError::DivisionByZero)));
            assert!(matches!(rem(&n(a), &zero), Err(TypeError::DivisionByZero)));
            assert!(matches!(
                div_trunc(&n(a), &zero),
                Err(TypeError::DivisionByZero)
            ));
        }
    }

    #[test]
    fn special_rounding_and_sign_pass_through() {
        use assert2::assert;
        for (text, sign_text) in [("inf", "1"), ("-inf", "-1"), ("nan", "NaN")] {
            let v = n(text);
            let want = to_text(&v);
            assert!(to_text(&floor(&v)) == want, "floor {text}");
            assert!(to_text(&ceil(&v)) == want, "ceil {text}");
            assert!(to_text(&round(&v, 0)) == want, "round {text}");
            assert!(to_text(&round(&v, 2)) == want, "round2 {text}");
            assert!(to_text(&trunc(&v, -3)) == want, "trunc {text}");
            assert!(to_text(&sign(&v)) == sign_text, "sign {text}");
        }
        assert!(to_text(&abs(&n("-inf"))) == "Infinity");
        assert!(to_text(&abs(&n("inf"))) == "Infinity");
        assert!(to_text(&abs(&n("nan"))) == "NaN");
    }

    #[test]
    fn special_transcendentals_match_postgres() {
        use assert2::assert;
        assert!(to_text(&num_sqrt(&n("inf")).expect("sqrt")) == "Infinity");
        assert!(to_text(&num_sqrt(&n("nan")).expect("sqrt")) == "NaN");
        assert!(num_sqrt(&n("-inf")).unwrap_err().sqlstate() == "2201F");
        assert!(to_text(&num_ln(&n("inf")).expect("ln")) == "Infinity");
        assert!(to_text(&num_ln(&n("nan")).expect("ln")) == "NaN");
        assert!(to_text(&num_log10(&n("inf")).expect("log")) == "Infinity");
        assert!(to_text(&num_log10(&n("nan")).expect("log")) == "NaN");
        assert!(to_text(&num_exp(&n("inf")).expect("exp")) == "Infinity");
        assert!(to_text(&num_exp(&n("-inf")).expect("exp")) == "0");
        assert!(to_text(&num_exp(&n("nan")).expect("exp")) == "NaN");
        // PostgreSQL spells the zero and negative logarithm domains differently.
        assert!(num_ln(&n("0")).unwrap_err() == err_log_zero());
        assert!(num_ln(&n("-1")).unwrap_err() == err_log_negative());
        assert!(num_ln(&n("-inf")).unwrap_err() == err_log_negative());
        assert!(num_log10(&n("0")).unwrap_err() == err_log_zero());
        assert!(num_log10(&n("-inf")).unwrap_err() == err_log_negative());
    }

    #[test]
    fn special_power_follows_posix_pow() {
        use assert2::assert;
        let cases: &[(&str, &str, &str)] = &[
            ("nan", "0", "1"),
            ("1", "nan", "1"),
            ("nan", "2", "NaN"),
            ("2", "nan", "NaN"),
            ("0", "nan", "NaN"),
            ("nan", "nan", "NaN"),
            ("nan", "inf", "NaN"),
            ("inf", "nan", "NaN"),
            ("inf", "2", "Infinity"),
            ("inf", "0", "1"),
            ("inf", "-2", "0"),
            ("inf", "inf", "Infinity"),
            ("inf", "-inf", "0"),
            ("-inf", "2", "Infinity"),
            ("-inf", "3", "-Infinity"),
            ("-inf", "0", "1"),
            ("-inf", "-2", "0"),
            ("-inf", "-3", "0"),
            ("-inf", "inf", "Infinity"),
            ("-inf", "-inf", "0"),
            ("2", "inf", "Infinity"),
            ("0.5", "inf", "0"),
            ("1", "inf", "1"),
            ("-1", "inf", "1"),
            ("-1", "-inf", "1"),
            ("-2", "inf", "Infinity"),
            ("-0.5", "inf", "0"),
            ("2", "-inf", "0"),
            ("0.5", "-inf", "Infinity"),
            ("0", "inf", "0"),
        ];
        for (base, exp, want) in cases {
            let got = num_power(&n(base), &n(exp)).expect("power");
            assert!(to_text(&got) == *want, "{base} ^ {exp}");
        }
        assert!(num_power(&n("0"), &n("-inf")).unwrap_err() == err_zero_neg_power());
        assert!(num_power(&n("-inf"), &n("4.5")).unwrap_err() == err_neg_noninteger_power());
    }

    #[test]
    fn special_ordering_puts_nan_above_infinity() {
        use assert2::assert;
        let mut values = [
            NumericValue::NaN,
            NumericValue::Infinity,
            n("0"),
            NumericValue::NegInfinity,
            n("-1"),
        ];
        values.sort();
        let sorted: Vec<String> = values.iter().map(to_text).collect();
        assert!(sorted == vec!["-Infinity", "-1", "0", "Infinity", "NaN"]);
        assert!(NumericValue::NaN == NumericValue::NaN);
        assert!(NumericValue::NaN > NumericValue::Infinity);
        assert!(NumericValue::NegInfinity < n("-1e1000"));
        assert!(NumericValue::Infinity > n("1e1000"));
    }

    #[test]
    fn special_binary_uses_the_reserved_sign_words() {
        use assert2::assert;
        // ndigits 0, weight 0, sign word, dscale — byte-verified against
        // PostgreSQL 18.4's `COPY … (FORMAT binary)` output.
        assert!(binary(&NumericValue::NaN) == vec![0, 0, 0, 0, 0xC0, 0, 0, 0]);
        assert!(binary(&NumericValue::Infinity) == vec![0, 0, 0, 0, 0xD0, 0, 0, 32]);
        assert!(binary(&NumericValue::NegInfinity) == vec![0, 0, 0, 0, 0xF0, 0, 0, 32]);
        // `numeric_recv` ignores the rest of the header for a special sign word.
        assert!(from_binary(&binary(&NumericValue::NaN)) == Some(NumericValue::NaN));
        assert!(from_binary(&binary(&NumericValue::Infinity)) == Some(NumericValue::Infinity));
        assert!(
            from_binary(&binary(&NumericValue::NegInfinity)) == Some(NumericValue::NegInfinity)
        );
        assert!(from_binary(&[0, 0, 0, 0, 0xD0, 0, 0, 0]) == Some(NumericValue::Infinity));
        assert!(from_binary(&[0, 0, 0, 0, 0xC0, 0, 0, 32]) == Some(NumericValue::NaN));
        // Neither reserved-but-unassigned word is a numeric.
        assert!(from_binary(&[0, 0, 0, 0, 0xE0, 0, 0, 0]).is_none());
        assert!(from_binary(&[0, 0, 0, 0, 0x80, 0, 0, 0]).is_none());
    }

    #[test]
    fn special_integer_casts_are_feature_not_supported() {
        use assert2::assert;
        for (value, smallint, integer, bigint) in [
            (
                "nan",
                "cannot convert NaN to smallint",
                "cannot convert NaN to integer",
                "cannot convert NaN to bigint",
            ),
            (
                "inf",
                "cannot convert infinity to smallint",
                "cannot convert infinity to integer",
                "cannot convert infinity to bigint",
            ),
            (
                "-inf",
                "cannot convert infinity to smallint",
                "cannot convert infinity to integer",
                "cannot convert infinity to bigint",
            ),
        ] {
            let v = n(value);
            let e16 = to_i16(&v).unwrap_err();
            let e32 = to_i32(&v).unwrap_err();
            let e64 = to_i64(&v).unwrap_err();
            assert!(e16.sqlstate() == "0A000" && e16.to_string() == smallint);
            assert!(e32.sqlstate() == "0A000" && e32.to_string() == integer);
            assert!(e64.sqlstate() == "0A000" && e64.to_string() == bigint);
        }
    }

    #[test]
    fn special_float_casts_map_across_in_both_directions() {
        use assert2::assert;
        assert!(to_f64(&NumericValue::NaN).is_nan());
        assert!(to_f64(&NumericValue::Infinity) == f64::INFINITY);
        assert!(to_f64(&NumericValue::NegInfinity) == f64::NEG_INFINITY);
        assert!(from_f64(f64::NAN) == NumericValue::NaN);
        assert!(from_f64(f64::INFINITY) == NumericValue::Infinity);
        assert!(from_f64(f64::NEG_INFINITY) == NumericValue::NegInfinity);
        assert!(from_f32(f32::NAN) == NumericValue::NaN);
        assert!(from_f32(f32::INFINITY) == NumericValue::Infinity);
        assert!(from_f32(f32::NEG_INFINITY) == NumericValue::NegInfinity);
    }

    #[test]
    fn typmod_accepts_nan_and_rejects_infinity() {
        use assert2::assert;
        let tm = Typmod {
            precision: 4,
            scale: 4,
        };
        assert!(apply_typmod(&NumericValue::NaN, tm) == Ok(NumericValue::NaN));
        assert!(matches!(
            apply_typmod(&NumericValue::Infinity, tm),
            Err(TypeError::Overflow)
        ));
        assert!(matches!(
            apply_typmod(&NumericValue::NegInfinity, tm),
            Err(TypeError::Overflow)
        ));
    }

    #[test]
    fn stddev_over_a_special_running_sum_is_nan() {
        use assert2::assert;
        for (sum, sum2) in [
            (NumericValue::NaN, NumericValue::NaN),
            (NumericValue::Infinity, NumericValue::Infinity),
            (NumericValue::NegInfinity, NumericValue::Infinity),
        ] {
            assert!(stddev_internal(3, &sum, &sum2, false, false) == Some(NumericValue::NaN));
            assert!(stddev_internal(3, &sum, &sum2, true, true) == Some(NumericValue::NaN));
        }
        // Too few rows is still NULL, special sums included.
        assert!(stddev_internal(1, &NumericValue::NaN, &NumericValue::NaN, true, false).is_none());
    }

    #[test]
    fn to_char_lays_a_special_into_the_digit_grid() {
        use assert2::assert;
        let fmt =
            |value: &str, template: &str| format_numeric(template, &n(value)).expect("to_char");
        // Every expectation oracle-confirmed against PostgreSQL 18.4.
        let cases: &[(&str, &str, &str)] = &[
            ("nan", "999", " NaN"),
            ("inf", "999", " ###"),
            ("-inf", "999", "-###"),
            ("nan", "99", " ##"),
            ("nan", "99999", "   NaN"),
            ("nan", "0999", " 0NaN"),
            ("nan", "999.99", " NaN"),
            ("nan", "9.9", " #.#"),
            ("inf", "9.9", " #.#"),
            ("-inf", "9.9", "-#.#"),
            ("nan", "FM999.999", "NaN"),
            ("inf", "FM999.999", "###.###"),
            ("-inf", "FM999.999", "-###.###"),
            ("inf", "99999999", " Infinity"),
            ("-inf", "99999999", "-Infinity"),
            ("nan", "S999", "+NaN"),
            ("-inf", "S999", "-###"),
            ("nan", "MI999", " NaN"),
            ("-inf", "MI999", "-###"),
            ("nan", "L999", "$ NaN"),
            ("-inf", "L999", "$-###"),
            ("nan", "999PR", " NaN "),
            ("inf", "999PR", " ### "),
            ("-inf", "999PR", "<###>"),
            ("nan", "99V99", "  NaN"),
            ("inf", "99V99", " ####"),
            ("inf", "999TH", " ###"),
        ];
        for (value, template, want) in cases {
            assert!(
                fmt(value, template) == *want,
                "to_char({value}, {template})"
            );
        }
        // An ordinal over a special that FITS is PostgreSQL's `get_th` 22P02.
        let err = format_numeric("999999999TH", &n("inf")).unwrap_err();
        assert!(err.sqlstate() == "22P02" && err.to_string() == "\"Infinity\" is not a number");
        let err = format_numeric("99999TH", &n("nan")).unwrap_err();
        assert!(err.sqlstate() == "22P02" && err.to_string() == "\"NaN\" is not a number");
    }
}
