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
    fmt::LowerExp,
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

/// C's `DBL_DIG` / `FLT_DIG`: the significant-digit counts `float8_numeric` and
/// `float4_numeric` hand to `snprintf` as the `%g` precision.
const DBL_DIG: usize = 15;
const FLT_DIG: usize = 6;

/// `float8 → numeric` (PostgreSQL `float8_numeric`), which converts through
/// `snprintf("%.*g", DBL_DIG, val)`, which is **fifteen** significant digits, not
/// the shortest round-tripping text `float8out` emits. That is why
/// `(1.0/3.0)::float8::numeric` is `0.333333333333333` (fifteen threes, not
/// sixteen) and `1234567890123456::float8::numeric` is `1234567890123460`. A
/// non-finite float maps to the matching numeric special. `numeric` gained its
/// infinities in PostgreSQL 14.
pub fn from_f64(f: f64) -> NumericValue {
    if f.is_nan() {
        return NumericValue::NaN;
    }
    if f.is_infinite() {
        return NumericValue::infinity_with_sign(if f < 0.0 { -1 } else { 1 });
    }
    NumericValue::Finite(
        parse_finite(&significant_digits(f, DBL_DIG))
            .expect("a finite f64 always lands inside the numeric format"),
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
        parse_finite(&significant_digits(f, FLT_DIG))
            .expect("a finite f32 always lands inside the numeric format"),
    )
}

/// `%.*g` of `f` at `digits` significant digits, in the scientific spelling
/// `BigDecimal` parses identically to the fixed one. Rust's `{:.p$e}` writes
/// `p + 1` significant digits; `%g` then drops the trailing fractional zeros,
/// which is what keeps the resulting numeric's display scale down
/// (`0.1::float8::numeric` is `0.1`, not `0.100000000000000`).
///
/// The two spellings only ever differ in shape. `%g` switches to the fixed style
/// once the exponent is in `-4..digits`, and PostgreSQL immediately parses the
/// buffer back with `set_var_from_str`, which reads an exponent as readily as a
/// decimal point. Rust rounds the exact binary value half-to-even, which is what
/// glibc's `printf` does too, so the digits themselves agree. They agree on an
/// exact tie as well: `4429515941059445::float8` goes *down* to
/// `4429515941059440` in both.
fn significant_digits<T: LowerExp>(f: T, digits: usize) -> String {
    let precision = digits - 1;
    let scientific = format!("{f:.precision$e}");
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
// The numeric `to_char` engine (`format_numeric`).
//
// This is an INDEPENDENT engine from the date/time `to_char` (in `datetime.rs`):
// the numeric template language is a positional digit grid (`9 0 . , S MI …`),
// not the date/time field-name tokenizer.
//
// The engine is a port of PostgreSQL 18's `formatting.c`, which is a STREAM
// processor, not a grid filler. That distinction is the whole design:
//
//   1. `parse_num_template` splits the template into an ORDERED list of
//      [`NumNode`]s — one per pattern or literal character — and accumulates the
//      structural totals into a [`NumDesc`] (PG `NUMDesc` / `NUMDesc_prepare`).
//   2. A driver (`numeric_to_char`) turns the value into a plain digit STRING
//      (`numstr`), a `sign`, and `out_pre_spaces`, the count of leading digit
//      positions the value does not reach (PG `numeric_to_char`).
//   3. `NumProc::run` walks the node list in TEMPLATE ORDER, consuming one digit
//      of `numstr` per digit node and emitting each literal, separator, currency
//      and sign node exactly where the template put it (PG `NUM_processor` /
//      `NUM_numpart_to_char`).
//
// Walking the nodes in order is what makes `to_char(0, '999999SG9999999999')`
// place its sign in the MIDDLE of the field, `to_char(-34338492.2, 'S 9 9 9')`
// interleave the template's literal spaces with the digits, and
// `to_char(100, 'f"ool"999')` emit a double-quoted run verbatim. A grid filler
// cannot express any of those, because it has thrown the ordering away.
//
// `RN` (Roman numerals) and `EEEE` (scientific notation) short-circuit in the
// driver: PG renders the value to its own string and, for `EEEE`, returns that
// string as the whole result without running the node walk at all.
//
// The C locale supplies every locale symbol (PG `NUM_prepare_locale`): the
// decimal point is `.`, the thousands separator is `,`, the signs are `+` / `-`,
// and — the one non-obvious value — the currency symbol is a single SPACE, so
// `L` widens the field by one blank rather than emitting a glyph.
// ---------------------------------------------------------------------------

/// One parsed element of a numeric `to_char` template, in template order
/// (PG `FormatNode`). Patterns PG parses but never emits during `to_char`
/// (`FM`, `S`, `PR`, `V`, `B`, `C`, `SP`, `EEEE`) become [`NumNode::Silent`]:
/// they have already had their effect on the [`NumDesc`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum NumNode {
    /// A `9` digit position: a leading zero renders blank.
    Nine,
    /// A `0` digit position: a leading zero renders `0`.
    Zero,
    /// A literal `.` decimal point.
    Dec,
    /// A `D` locale decimal point.
    LocaleDec,
    /// A literal `,` group separator.
    Comma,
    /// A `G` locale group separator.
    Group,
    /// An `L` locale currency symbol.
    Currency,
    /// `RN` / `rn`: Roman numerals. `true` = upper case.
    Roman(bool),
    /// `TH` / `th`: the English ordinal suffix. `true` = upper case.
    Ordinal(bool),
    /// `MI`: `-` when negative, else a blank (dropped under `FM`).
    Minus,
    /// `PL`: `+` when non-negative, else a blank (dropped under `FM`).
    Plus,
    /// `SG`: always the sign, at this exact column.
    SignHere,
    /// A pattern with no output of its own; its effect is in the [`NumDesc`].
    Silent,
    /// A literal character copied straight through.
    Literal(char),
}

/// Where an `S` locale sign sits relative to the digits (PG `NUM_LSIGN_*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LSign {
    /// No `S` in the template.
    None,
    /// `S` before the digits: the sign hugs the first significant digit.
    Pre,
    /// `S` after the digits: the sign follows the last digit position.
    Post,
}

/// The structural totals a numeric template accumulates (PG `NUMDesc`).
///
/// The counts are what the driver needs in order to render the value to a digit
/// string; the flags are what the node walk consults as it emits.
#[derive(Debug, Clone)]
struct NumDesc {
    /// Integer digit positions.
    pre: usize,
    /// Fractional digit positions.
    post: usize,
    /// `V` shift: the count of digit positions that follow the `V`.
    multi: usize,
    /// 1-based index of the first `0` integer position, decremented to 0-based
    /// by [`NumProc::new`] exactly as PG does.
    zero_start: usize,
    /// `pre + post` at the last `0`, the furthest position `FM` may not trim.
    zero_end: usize,
    /// Integer positions seen when an `S` was parsed, for the `Pre` → `Post`
    /// demotion of a trailing `S`.
    pre_lsign_num: usize,
    /// Any `0` position was seen.
    zero: bool,
    /// `FM`: suppress padding blanks and trailing fractional zeros.
    fill_mode: bool,
    /// A decimal point (`.` or `D`) was seen.
    decimal: bool,
    /// Where an `S` sign sits.
    lsign: LSign,
    /// `PR`: render a negative in angle brackets.
    bracket: bool,
    /// `MI` or `SG` was seen.
    minus: bool,
    /// `PL` or `SG` was seen.
    plus: bool,
    /// `RN` / `rn`: render the value as a Roman numeral.
    roman: bool,
    /// `V` was seen.
    has_multi: bool,
    /// `EEEE`: render the value in scientific notation.
    eeee: bool,
    /// `B` blank-on-zero was seen before any digit position.
    blank: bool,
    /// The first template combination PostgreSQL refuses outright. `to_char` is
    /// otherwise total, so only `to_number` consults this.
    refusal: Option<RomanRefusal>,
}

impl NumDesc {
    /// A template with no patterns at all.
    fn new() -> Self {
        Self {
            pre: 0,
            post: 0,
            multi: 0,
            zero_start: 0,
            zero_end: 0,
            pre_lsign_num: 0,
            zero: false,
            fill_mode: false,
            decimal: false,
            lsign: LSign::None,
            bracket: false,
            minus: false,
            plus: false,
            roman: false,
            has_multi: false,
            eeee: false,
            blank: false,
            refusal: None,
        }
    }

    /// PostgreSQL's post-keyword guard: `RN` tolerates only `FM` beside it.
    fn check_roman(&mut self) {
        if !self.roman || self.refusal.is_some() {
            return;
        }
        if self.zero
            || self.blank
            || self.lsign != LSign::None
            || self.bracket
            || self.minus
            || self.plus
            || self.has_multi
            || self.decimal
            || self.eeee
        {
            self.refusal = Some(RomanRefusal::Incompatible);
        }
    }
}

/// A numeric template PostgreSQL refuses to compile because of how `RN` is used.
/// Both are 42601 syntax errors raised before the value is even looked at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RomanRefusal {
    /// `"RN" is incompatible with other formats`.
    Incompatible,
    /// `cannot use "RN" twice`.
    Twice,
}

/// The numeric template keywords, in PostgreSQL's `NUM_keywords` order: matching
/// is CASE-SENSITIVE and longest-first within a starting character, so `SG` wins
/// over `S` but `Sg` matches neither and falls through to a literal `S`.
const NUM_KEYWORDS: &[(&str, NumNode)] = &[
    (",", NumNode::Comma),
    (".", NumNode::Dec),
    ("0", NumNode::Zero),
    ("9", NumNode::Nine),
    ("B", NumNode::Silent),
    ("C", NumNode::Silent),
    ("D", NumNode::LocaleDec),
    ("EEEE", NumNode::Silent),
    ("FM", NumNode::Silent),
    ("G", NumNode::Group),
    ("L", NumNode::Currency),
    ("MI", NumNode::Minus),
    ("PL", NumNode::Plus),
    ("PR", NumNode::Silent),
    ("RN", NumNode::Roman(true)),
    ("SG", NumNode::SignHere),
    ("SP", NumNode::Silent),
    ("S", NumNode::Silent),
    ("TH", NumNode::Ordinal(true)),
    ("V", NumNode::Silent),
    ("b", NumNode::Silent),
    ("c", NumNode::Silent),
    ("d", NumNode::LocaleDec),
    ("eeee", NumNode::Silent),
    ("fm", NumNode::Silent),
    ("g", NumNode::Group),
    ("l", NumNode::Currency),
    ("mi", NumNode::Minus),
    ("pl", NumNode::Plus),
    ("pr", NumNode::Silent),
    ("rn", NumNode::Roman(false)),
    ("sg", NumNode::SignHere),
    ("sp", NumNode::Silent),
    ("s", NumNode::Silent),
    ("th", NumNode::Ordinal(false)),
    ("v", NumNode::Silent),
];

/// Fold the keyword just matched into the running descriptor
/// (PG `NUMDesc_prepare`). PostgreSQL raises a syntax error for several
/// contradictory combinations; `to_char` is otherwise total, and gres keeps that
/// contract by simply IGNORING a contradictory second pattern rather than
/// failing the query. The regress corpus exercises none of the error cases.
fn numdesc_prepare(num: &mut NumDesc, key: &str) {
    match key {
        "9" => {
            if num.has_multi {
                num.multi += 1;
            } else if num.decimal {
                num.post += 1;
            } else {
                num.pre += 1;
            }
        }
        "0" => {
            if !num.zero && !num.decimal {
                num.zero = true;
                num.zero_start = num.pre + 1;
            }
            if num.decimal {
                num.post += 1;
            } else {
                num.pre += 1;
            }
            num.zero_end = num.pre + num.post;
        }
        "." | "D" | "d" => num.decimal = true,
        "FM" | "fm" => num.fill_mode = true,
        "S" | "s" => {
            if num.decimal {
                if num.lsign == LSign::None {
                    num.lsign = LSign::Post;
                }
            } else {
                num.lsign = LSign::Pre;
                num.pre_lsign_num = num.pre;
            }
        }
        "MI" | "mi" => num.minus = true,
        "PL" | "pl" => num.plus = true,
        "SG" | "sg" => {
            num.minus = true;
            num.plus = true;
        }
        "PR" | "pr" => num.bracket = true,
        "B" | "b" => {
            if num.pre == 0 && num.post == 0 && !num.zero {
                num.blank = true;
            }
        }
        "RN" | "rn" => {
            if num.roman {
                num.refusal = Some(RomanRefusal::Twice);
            }
            num.roman = true;
        }
        "V" | "v" => num.has_multi = true,
        "EEEE" | "eeee" => num.eeee = true,
        _ => {}
    }
    num.check_roman();
}

/// How `to_number` must read a template: as Roman numerals, as ordinary digits,
/// or not at all because PostgreSQL refuses the template.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumberTemplate {
    /// No `RN`: read the input as digits.
    Digits,
    /// `RN` (optionally with `FM`): read the input as a Roman numeral.
    Roman,
    /// PostgreSQL will not compile this template.
    Refused(RomanRefusal),
}

/// Classify a `to_number` template.
#[must_use]
pub fn number_template(template: &str) -> NumberTemplate {
    let (_, desc) = parse_num_template(template);
    match desc.refusal {
        Some(refusal) => NumberTemplate::Refused(refusal),
        None if desc.roman => NumberTemplate::Roman,
        None => NumberTemplate::Digits,
    }
}

/// PostgreSQL `roman_to_int`: decode a leading Roman numeral, returning `None`
/// for anything that is not a well-formed one.
///
/// Well-formed is narrower than "decodable": a subtractive pair may not be
/// followed by a numeral as large as the one subtracted (`VIX`), `V` / `L` / `D`
/// may neither repeat nor precede something larger (`VV`, `IL`), a symbol may not
/// repeat more than three times (`MMMM`), and a subtraction may not follow a
/// repeat of the same symbol (`MCCM`). At most 15 numerals are consumed, and
/// leading whitespace is skipped; anything after the numerals is ignored, which
/// is why `to_number('M CC', 'RN')` is 1000.
#[must_use]
pub fn roman_to_int(input: &str) -> Option<i32> {
    fn value_of(c: char) -> i32 {
        match c {
            'I' => 1,
            'V' => 5,
            'X' => 10,
            'L' => 50,
            'C' => 100,
            'D' => 500,
            'M' => 1000,
            _ => 0,
        }
    }
    // Only `IV IX XL XC CD CM` subtract; nothing else may.
    fn valid_sub(smaller: char, larger: char) -> bool {
        matches!(
            (smaller, larger),
            ('I', 'V') | ('I', 'X') | ('X', 'L') | ('X', 'C') | ('C', 'D') | ('C', 'M')
        )
    }

    let chars: Vec<char> = input
        .trim_start()
        .chars()
        .map(|c| c.to_ascii_uppercase())
        .take_while(|c| value_of(*c) != 0)
        .take(MAX_ROMAN_LEN)
        .collect();
    if chars.is_empty() {
        return None;
    }

    let mut result: i32 = 0;
    let mut repeat_count = 1u32;
    let (mut v_count, mut l_count, mut d_count) = (0u32, 0u32, 0u32);
    let mut subtracted: Option<i32> = None;
    let mut i = 0;
    while i < chars.len() {
        let curr = chars[i];
        let curr_value = value_of(curr);
        // Nothing at or above the subtracted numeral may follow a subtraction.
        if subtracted.is_some_and(|last| curr_value >= last) {
            return None;
        }
        // `V` / `L` / `D` may neither repeat nor precede a larger numeral.
        fn seen_half(c: char, value: i32, v: &mut u32, l: &mut u32, d: &mut u32) -> bool {
            if (*v > 0 && value >= 5) || (*l > 0 && value >= 50) || (*d > 0 && value >= 500) {
                return false;
            }
            match c {
                'V' => *v += 1,
                'L' => *l += 1,
                'D' => *d += 1,
                _ => {}
            }
            true
        }
        if !seen_half(curr, curr_value, &mut v_count, &mut l_count, &mut d_count) {
            return None;
        }

        match chars.get(i + 1) {
            Some(&next) if value_of(next) > curr_value => {
                let next_value = value_of(next);
                if !valid_sub(curr, next) || repeat_count > 1 {
                    return None;
                }
                if !seen_half(next, next_value, &mut v_count, &mut l_count, &mut d_count) {
                    return None;
                }
                repeat_count = 1;
                subtracted = Some(curr_value);
                result += next_value - curr_value;
                i += 2;
            }
            Some(&next) => {
                if next == curr {
                    repeat_count += 1;
                    if repeat_count > 3 {
                        return None;
                    }
                } else {
                    repeat_count = 1;
                }
                result += curr_value;
                i += 1;
            }
            None => {
                result += curr_value;
                i += 1;
            }
        }
    }
    Some(result)
}

/// Split a numeric `to_char` template into its ordered node list plus the
/// structural descriptor (PG `parse_format` with the `NUM_FLAG`).
///
/// Outside a double-quoted run, a backslash is special only immediately before a
/// `"`. Inside one, a backslash quotes whatever follows it, and the run ends at
/// the next unescaped `"` or at the end of the template.
fn parse_num_template(template: &str) -> (Vec<NumNode>, NumDesc) {
    let chars: Vec<char> = template.chars().collect();
    let mut nodes = Vec::new();
    let mut desc = NumDesc::new();
    let mut i = 0;
    while i < chars.len() {
        if let Some((key, node)) = match_keyword(&chars, i) {
            numdesc_prepare(&mut desc, key);
            nodes.push(node);
            i += key.chars().count();
            continue;
        }
        if chars[i] == '"' {
            i += 1;
            while i < chars.len() {
                if chars[i] == '"' {
                    i += 1;
                    break;
                }
                // A backslash quotes the next character, if any.
                if chars[i] == '\\' && i + 1 < chars.len() {
                    i += 1;
                }
                nodes.push(NumNode::Literal(chars[i]));
                i += 1;
            }
            continue;
        }
        // Outside a quoted run, `\` is special only before a `"`.
        if chars[i] == '\\' && chars.get(i + 1) == Some(&'"') {
            i += 1;
        }
        nodes.push(NumNode::Literal(chars[i]));
        i += 1;
    }
    (nodes, desc)
}

/// Longest-first, case-sensitive keyword match at `chars[i..]`.
fn match_keyword(chars: &[char], i: usize) -> Option<(&'static str, NumNode)> {
    let first = *chars.get(i)?;
    NUM_KEYWORDS
        .iter()
        .filter(|(name, _)| name.starts_with(first))
        .find(|(name, _)| {
            let n = name.chars().count();
            i + n <= chars.len() && name.chars().zip(&chars[i..i + n]).all(|(a, b)| a == *b)
        })
        .map(|(name, node)| (*name, node.clone()))
}

/// The C-locale currency symbol PG's `NUM_prepare_locale` falls back to: a single
/// SPACE, not a glyph. So `to_char(0, 'L99')` is `'   0'`, three columns wide.
const LOCALE_CURRENCY: char = ' ';

/// The state of one `to_char` node walk (PG `NUMProc`).
struct NumProc<'a> {
    desc: NumDesc,
    /// The value rendered as a plain digit string, without a sign. May instead be
    /// a run of `#` (integer overflow) or a special's spelling (`NaN`).
    number: &'a [char],
    /// How far the walk has consumed `number`.
    number_p: usize,
    /// `'+'` or `'-'` for the value's sign, or `'\0'` when the driver did not set
    /// one (the `RN` path).
    sign: char,
    sign_wrote: bool,
    /// Digit positions the value does not reach, at the left of the field.
    out_pre_spaces: usize,
    /// Total digit positions the walk will visit, for the trailing-sign trigger.
    num_count: usize,
    /// Digit positions visited so far.
    num_curr: usize,
    /// Did the position just emitted produce a digit? Group separators blank
    /// themselves when it did not.
    num_in: bool,
    /// Index into `number` of the last digit `FM` may not trim, or of the `.`
    /// when the whole fraction is zeros. `None` disables `FM` fraction trimming.
    last_relevant: Option<usize>,
    out: String,
}

impl<'a> NumProc<'a> {
    /// Set up the walk from the driver's rendered value (PG `NUM_processor`'s
    /// prologue).
    fn new(mut desc: NumDesc, number: &'a [char], sign: char, out_pre_spaces: usize) -> Self {
        // PG stores `zero_start` 1-based and decrements it here.
        desc.zero_start = desc.zero_start.saturating_sub(1);

        let mut sign_wrote;
        if desc.plus || desc.minus {
            // `MI` / `PL` / `SG` write the sign at their own node, so the digit
            // walk must not write one — except for a bare `PL`, whose `+` is
            // ADDITIVE to the default sign.
            sign_wrote = !(desc.plus && !desc.minus);
        } else {
            if sign != '-' && desc.fill_mode {
                desc.bracket = false;
            }
            sign_wrote = sign == '+' && desc.fill_mode && desc.lsign == LSign::None;
            // A trailing `S` that follows every integer position is a POST sign.
            if desc.lsign == LSign::Pre && desc.pre == desc.pre_lsign_num {
                desc.lsign = LSign::Post;
            }
        }
        if desc.roman {
            sign_wrote = false;
        }

        let mut num_count = desc.post + desc.pre;
        num_count = num_count.saturating_sub(1);

        // `FM` may trim trailing fractional zeros, but never past the last `0`
        // position the template pinned.
        let mut last_relevant = None;
        if desc.fill_mode && desc.decimal {
            last_relevant = last_relevant_decnum(number);
            if let Some(lr) = last_relevant
                && desc.zero_end > out_pre_spaces
            {
                let last_zero = (number.len() - 1).min(desc.zero_end - out_pre_spaces);
                if lr < last_zero {
                    last_relevant = Some(last_zero);
                }
            }
        }
        if !sign_wrote && out_pre_spaces == 0 {
            num_count += 1;
        }

        Self {
            desc,
            number,
            number_p: 0,
            sign,
            sign_wrote,
            out_pre_spaces,
            num_count,
            num_curr: 0,
            num_in: false,
            last_relevant,
            out: String::new(),
        }
    }

    /// The character `number_p` points at, or a NUL past the end. PostgreSQL
    /// walks a NUL-terminated string and WRITES that NUL into the output when a
    /// digit node outruns the value; the final `strlen` then truncates the result
    /// there. `run` reproduces that by truncating at the first NUL.
    fn cur(&self) -> char {
        self.number.get(self.number_p).copied().unwrap_or('\0')
    }

    /// PG `IS_PREDEC_SPACE`: a sub-1 value's leading `0` in a `9` template over a
    /// fraction renders blank, so `to_char(0.1, '9.9')` is `'  .1'`.
    fn predec_space(&self) -> bool {
        !self.desc.zero
            && self.number_p == 0
            && self.number.first() == Some(&'0')
            && self.desc.post != 0
    }

    /// Emit one digit / decimal-point position (PG `NUM_numpart_to_char`).
    fn numpart(&mut self, is_zero_node: bool) {
        if self.desc.roman {
            return;
        }
        self.num_in = false;

        // The sign goes in front of the first position that renders something.
        if !self.sign_wrote
            && (self.num_curr >= self.out_pre_spaces
                || (self.desc.zero && self.desc.zero_start == self.num_curr))
            && (!self.predec_space() || self.last_relevant_is_point())
        {
            if self.desc.lsign != LSign::None {
                // A POST `S` writes nothing here and leaves `sign_wrote` false:
                // its sign lands after the last digit position instead.
                if self.desc.lsign == LSign::Pre {
                    self.out.push(self.sign);
                    self.sign_wrote = true;
                }
            } else if self.desc.bracket {
                self.out.push(if self.sign == '+' { ' ' } else { '<' });
                self.sign_wrote = true;
            } else if self.sign == '+' {
                if !self.desc.fill_mode {
                    self.out.push(' ');
                }
                self.sign_wrote = true;
            } else if self.sign == '-' {
                self.out.push('-');
                self.sign_wrote = true;
            }
        }

        if self.num_curr < self.out_pre_spaces
            && (self.desc.zero_start > self.num_curr || !self.desc.zero)
        {
            // A leading position the value does not reach.
            if !self.desc.fill_mode {
                self.out.push(' ');
            }
        } else if self.desc.zero
            && self.num_curr < self.out_pre_spaces
            && self.desc.zero_start <= self.num_curr
        {
            // A leading position pinned by a `0` in the template.
            self.out.push('0');
            self.num_in = true;
        } else {
            if self.cur() == '.' {
                // The decimal point survives `FM` even when the whole fraction is
                // trimmed, so `to_char(5, 'FM9.99')` is `'5.'`.
                if !self.last_relevant_is_point() || self.desc.fill_mode {
                    self.out.push('.');
                }
            } else if self.last_relevant.is_some_and(|lr| self.number_p > lr) && !is_zero_node {
                // Trimmed by `FM`: emit nothing.
            } else if self.predec_space() {
                if !self.desc.fill_mode {
                    self.out.push(' ');
                } else if self.last_relevant_is_point() {
                    self.out.push('0');
                }
            } else {
                self.out.push(self.cur());
                self.num_in = true;
            }
            if self.cur() != '\0' {
                self.number_p += 1;
            }
        }

        // The trailing bracket / `S` sign fires on the LAST position visited.
        let mut end =
            self.num_count + usize::from(self.out_pre_spaces > 0) + usize::from(self.desc.decimal);
        if self.last_relevant == Some(self.number_p) {
            end = self.num_curr;
        }
        if self.num_curr + 1 == end {
            if self.sign_wrote && self.desc.bracket {
                self.out.push(if self.sign == '+' { ' ' } else { '>' });
            } else if self.desc.lsign == LSign::Post {
                self.out.push(self.sign);
            }
        }
        self.num_curr += 1;
    }

    /// Does `last_relevant` point at the decimal point itself, that is, is the
    /// whole fraction zeros?
    fn last_relevant_is_point(&self) -> bool {
        self.last_relevant
            .is_some_and(|lr| self.number.get(lr) == Some(&'.'))
    }

    /// Walk the node list and return the formatted text (PG `NUM_processor`'s
    /// main loop).
    fn run(mut self, nodes: &[NumNode]) -> Result<String, TypeError> {
        for node in nodes {
            match node {
                NumNode::Nine => self.numpart(false),
                NumNode::Zero => self.numpart(true),
                NumNode::Dec | NumNode::LocaleDec => self.numpart(false),
                NumNode::Comma | NumNode::Group => {
                    if self.num_in {
                        self.out.push(',');
                    } else if !self.desc.fill_mode {
                        self.out.push(' ');
                    }
                }
                NumNode::Currency => self.out.push(LOCALE_CURRENCY),
                NumNode::Roman(upper) => {
                    let roman: String = self.number[self.number_p..].iter().collect();
                    let roman = if *upper {
                        roman
                    } else {
                        roman.to_ascii_lowercase()
                    };
                    if self.desc.fill_mode {
                        self.out.push_str(&roman);
                    } else {
                        // PG's `sprintf("%15s", …)`: right-aligned in 15 columns.
                        let pad = MAX_ROMAN_LEN.saturating_sub(roman.chars().count());
                        self.out.push_str(&" ".repeat(pad));
                        self.out.push_str(&roman);
                    }
                }
                NumNode::Ordinal(upper) => {
                    if self.desc.roman
                        || self.number.first() == Some(&'#')
                        || self.sign == '-'
                        || self.desc.decimal
                    {
                        continue;
                    }
                    match ordinal_suffix(self.number, *upper) {
                        Some(suffix) => self.out.push_str(suffix),
                        // PG's `get_th` raises 22P02 when the value's spelling
                        // does not end in a digit, which is every special.
                        None => {
                            return Err(TypeError::Domain {
                                sqlstate: "22P02",
                                message: if self.number.first() == Some(&'N') {
                                    "\"NaN\" is not a number"
                                } else {
                                    "\"Infinity\" is not a number"
                                },
                            });
                        }
                    }
                }
                NumNode::Minus => {
                    if self.sign == '-' {
                        self.out.push('-');
                    } else if !self.desc.fill_mode {
                        self.out.push(' ');
                    }
                }
                NumNode::Plus => {
                    if self.sign == '+' {
                        self.out.push('+');
                    } else if !self.desc.fill_mode {
                        self.out.push(' ');
                    }
                }
                NumNode::SignHere => self.out.push(self.sign),
                NumNode::Silent => {}
                NumNode::Literal(c) => self.out.push(*c),
            }
        }
        // PG returns a NUL-terminated buffer and the caller takes its `strlen`.
        Ok(match self.out.find('\0') {
            Some(at) => self.out[..at].to_string(),
            None => self.out,
        })
    }
}

/// PG `get_last_relevant_decnum`: the index of the last non-`0` character after
/// the decimal point, or of the point itself when the fraction is all zeros.
/// `None` when the string has no decimal point, which disables `FM` trimming.
fn last_relevant_decnum(number: &[char]) -> Option<usize> {
    let point = number.iter().position(|c| *c == '.')?;
    let mut result = point;
    for (i, c) in number.iter().enumerate().skip(point + 1) {
        if *c != '0' {
            result = i;
        }
    }
    Some(result)
}

/// PG `get_th`: the English ordinal suffix for the digit string's last digit.
/// `None` when that character is not a digit, which is PG's 22P02 case.
fn ordinal_suffix(number: &[char], upper: bool) -> Option<&'static str> {
    let last = *number.last()?;
    if !last.is_ascii_digit() {
        return None;
    }
    // Every "teen" takes `TH`; only a non-teen 1 / 2 / 3 takes `ST` / `ND` / `RD`.
    let teen = number.len() > 1 && number[number.len() - 2] == '1';
    Some(match (teen, last, upper) {
        (false, '1', true) => "ST",
        (false, '1', false) => "st",
        (false, '2', true) => "ND",
        (false, '2', false) => "nd",
        (false, '3', true) => "RD",
        (false, '3', false) => "rd",
        (_, _, true) => "TH",
        (_, _, false) => "th",
    })
}

/// PG's `MAX_ROMAN_LEN`: the width `RN` right-aligns into, and the width of the
/// `#` run it emits for a value outside 1..=3999.
const MAX_ROMAN_LEN: usize = 15;

/// PG `int_to_roman`. Outside 1..=3999 the answer is a run of `#`, because a
/// valid Roman numeral never repeats a symbol more than three times.
fn int_to_roman(number: i32) -> String {
    const RM1: [&str; 9] = ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX"];
    const RM10: [&str; 9] = ["X", "XX", "XXX", "XL", "L", "LX", "LXX", "LXXX", "XC"];
    const RM100: [&str; 9] = ["C", "CC", "CCC", "CD", "D", "DC", "DCC", "DCCC", "CM"];

    if !(1..=3999).contains(&number) {
        return "#".repeat(MAX_ROMAN_LEN);
    }
    let numstr = number.to_string();
    let mut result = String::new();
    let mut len = numstr.len();
    for ch in numstr.chars() {
        let num = ch as usize;
        // `'0'` contributes nothing; `'1'..='9'` index the column's table.
        if num > '0' as usize {
            let idx = num - '1' as usize;
            match len {
                4 => result.push_str(&"M".repeat(idx + 1)),
                3 => result.push_str(RM100[idx]),
                2 => result.push_str(RM10[idx]),
                _ => result.push_str(RM1[idx]),
            }
        }
        len -= 1;
    }
    result
}

/// How wide a value's own type lets `to_char` render its fraction. PostgreSQL's
/// `float8_to_char` / `float4_to_char` clamp the template's fractional positions
/// to the type's decimal digits, so `to_char(12345678901::float8, 'FM…D99999…')`
/// keeps only `DBL_DIG - 11` of them. An exact type imposes no clamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumPrecision {
    /// `numeric` and the integer types: every template position is honored.
    Exact,
    /// `real`: `FLT_DIG` significant decimal digits.
    Float4,
    /// `double precision`: `DBL_DIG` significant decimal digits.
    Float8,
}

impl NumPrecision {
    /// The type's significant decimal digits, or `None` for an exact type.
    fn digits(self) -> Option<usize> {
        match self {
            Self::Exact => None,
            Self::Float4 => Some(6),
            Self::Float8 => Some(15),
        }
    }
}

/// The numeric `to_char` engine. Format `value` per the PostgreSQL numeric
/// template, at the fractional width `precision` allows.
///
/// PostgreSQL's `to_char(numeric, text)` is extremely lenient: it never raises
/// for a malformed template, emitting an unrecognized character literally and
/// `#`-filling an integer part too wide for the template. The one exception is
/// `TH` over a value whose spelling does not end in a digit, which is 22P02.
pub fn format_numeric_prec(
    template: &str,
    value: &NumericValue,
    precision: NumPrecision,
) -> Result<String, TypeError> {
    let (nodes, mut desc) = parse_num_template(template);

    if desc.roman {
        // PG rounds to int4 and lets `int_to_roman` reject anything out of range.
        // A float rounds through `rint`, which is half to even, so
        // `to_char(0.5::float8, 'RN')` reaches 0 and is refused.
        let mode = if precision.digits().is_some() {
            RoundingMode::HalfEven
        } else {
            RoundingMode::HalfUp
        };
        let rounded = match value.as_finite() {
            Some(bd) => bd.with_scale_round(0, mode).to_string(),
            None => String::new(),
        };
        let intvalue = rounded.parse::<i32>().unwrap_or(i32::MAX);
        let numstr: Vec<char> = int_to_roman(intvalue).chars().collect();
        return NumProc::new(desc, &numstr, '\0', 0).run(&nodes);
    }

    if desc.eeee {
        // `EEEE` short-circuits the whole node walk: PG returns the scientific
        // spelling as the entire result.
        return Ok(format_scientific(&desc, value));
    }

    // The `V` shift widens the integer field whatever the value is, so it lands
    // before the special short-circuit: PG multiplies `NaN` by 10^n too.
    if desc.has_multi {
        desc.pre += desc.multi;
    }

    let Some(value) = value.as_finite() else {
        // A special renders through `numeric_out`, so `NaN` / `Infinity` lays
        // into the digit positions exactly like a run of digits would.
        let (spelling, sign) = match value {
            NumericValue::NegInfinity => ("Infinity", '-'),
            NumericValue::Infinity => ("Infinity", '+'),
            _ => ("NaN", '+'),
        };
        return run_with_numstr(&nodes, &desc, spelling, sign);
    };
    let value = if desc.has_multi && desc.multi > 0 {
        let pow10 = parse_finite(&format!("1{}", "0".repeat(desc.multi)))
            .unwrap_or_else(|| BigDecimal::from(1));
        canonical(value * pow10)
    } else {
        value.clone()
    };

    // A float's own precision caps the fractional positions PG will render.
    if let Some(dig) = precision.digits() {
        let int_len = value
            .with_scale_round(0, RoundingMode::HalfEven)
            .abs()
            .to_string()
            .len();
        if int_len >= dig {
            desc.post = 0;
        } else if int_len + desc.post > dig {
            desc.post = dig - int_len;
        }
    }

    // A float renders through C's `%.*f`, which rounds half to EVEN and keeps the
    // negative sign even when the magnitude rounds away. `numeric` renders through
    // `numeric_round`, which rounds half AWAY from zero and has no negative zero.
    // Oracle-confirmed, PG 18: `to_char(2.5::float8,'999')` → `'   2'` against
    // `to_char(2.5,'999')` → `'   3'`, and `to_char(-0.0001::float8,'999')` →
    // `'  -0'` against `to_char(-0.0001,'999')` → `'   0'`.
    let is_float = precision.digits().is_some();
    let mode = if is_float {
        RoundingMode::HalfEven
    } else {
        RoundingMode::HalfUp
    };
    let rounded = value.with_scale_round((desc.post as i64).min(MAX_DSCALE), mode);
    let negative =
        if is_float { &value } else { &rounded }.sign() == bigdecimal::num_bigint::Sign::Minus;
    let sign = if negative { '-' } else { '+' };
    // PG's `numeric_out` of a rounded value keeps exactly `post` fractional
    // digits, so the digit string's shape is fixed by the template, not by the
    // value's own scale.
    let mut numstr = format!("{:.*}", desc.post.min(MAX_DSCALE as usize), rounded.abs());
    if desc.post == 0 {
        numstr = numstr.trim_end_matches('.').to_string();
    }

    run_with_numstr(&nodes, &desc, &numstr, sign)
}

/// Finish a `to_char` from an already-rendered digit string: work out the
/// left padding, `#`-fill an integer part too wide for the template, and walk
/// the nodes (PG's `numeric_to_char` tail plus `NUM_TOCHAR_finish`).
fn run_with_numstr(
    nodes: &[NumNode],
    desc: &NumDesc,
    numstr: &str,
    sign: char,
) -> Result<String, TypeError> {
    let pre_len = numstr.find('.').unwrap_or(numstr.len());
    let (owned, out_pre_spaces) = if pre_len < desc.pre {
        (numstr.to_string(), desc.pre - pre_len)
    } else if pre_len > desc.pre {
        // Integer overflow: PG replaces the whole value with `#`s and puts the
        // decimal point back at the template's own position.
        let mut fill: Vec<char> = vec!['#'; desc.pre + desc.post + 1];
        if desc.pre < fill.len() {
            fill[desc.pre] = '.';
        }
        (fill.into_iter().collect(), 0)
    } else {
        (numstr.to_string(), 0)
    };
    let chars: Vec<char> = owned.chars().collect();
    NumProc::new(desc.clone(), &chars, sign, out_pre_spaces).run(nodes)
}

/// `EEEE`: PostgreSQL renders the value with `numeric_out_sci` at the template's
/// fractional width and returns that string as the WHOLE result, prefixed with a
/// blank when the value is non-negative so signs stay aligned. A special becomes
/// a `#` run of the same shape.
fn format_scientific(desc: &NumDesc, value: &NumericValue) -> String {
    let Some(bd) = value.as_finite() else {
        // PG allows 6 characters for the sign, point, `e`, exponent sign and two
        // exponent digits, then overwrites the first with a blank and plants the
        // point just past the integer positions.
        let mut fill: Vec<char> = vec!['#'; desc.pre + desc.post + 6];
        fill[0] = ' ';
        if desc.pre + 1 < fill.len() {
            fill[desc.pre + 1] = '.';
        }
        return fill.into_iter().collect();
    };
    let sci = scientific_notation(bd, desc.post);
    if sci.starts_with('-') {
        sci
    } else {
        format!(" {sci}")
    }
}

/// PG `get_str_from_var_sci`: one integer digit, `scale` fractional digits, then
/// `e` and a signed exponent of at least two digits.
fn scientific_notation(value: &BigDecimal, scale: usize) -> String {
    if finite_is_zero(value) {
        let mantissa = format!("{:.*}", scale, BigDecimal::from(0));
        return format!("{mantissa}e+00");
    }
    // The exponent is the power of ten of the leading significant digit.
    let (mant, exp) = value.as_bigint_and_exponent();
    let digits = mant.to_string();
    let digit_count = digits.trim_start_matches('-').len() as i64;
    let exponent = digit_count - exp - 1;
    // PG fixes the exponent from the UNROUNDED value and never re-normalizes, so
    // rounding the significand up to 10 stays there: `to_char(999.5, '9.99EEEE')`
    // is `' 10.00e+02'`, not `' 1.00e+03'`.
    let mantissa =
        (value / power_of_ten(exponent)).with_scale_round(scale as i64, RoundingMode::HalfUp);
    let sign = if exponent < 0 { '-' } else { '+' };
    format!(
        "{:.*}e{}{:02}",
        scale,
        mantissa,
        sign,
        exponent.unsigned_abs()
    )
}

/// `10^exp` as an exact `BigDecimal`, for either sign of `exp`.
fn power_of_ten(exp: i64) -> BigDecimal {
    let magnitude = parse_finite(&format!("1{}", "0".repeat(exp.unsigned_abs() as usize)))
        .unwrap_or_else(|| BigDecimal::from(1));
    if exp < 0 {
        BigDecimal::from(1) / magnitude
    } else {
        magnitude
    }
}

/// The numeric `to_char` engine for the exact types (`numeric` and the integers).
pub fn format_numeric(template: &str, value: &NumericValue) -> Result<String, TypeError> {
    format_numeric_prec(template, value, NumPrecision::Exact)
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
    Ok(NumericValue::Finite(finite_ln(bd, ln_rscale(bd))?))
}

/// `ln(bd)` rounded to `rscale` fractional digits (PostgreSQL `ln_var`).
///
/// `num_ln` lets `ln_rscale` pick the scale; `log_var` (see [`num_log`])
/// instead asks for a scale of its own, because it divides two logarithms and
/// needs guard digits in each of them.
fn finite_ln(bd: &BigDecimal, rscale: i64) -> Result<BigDecimal, TypeError> {
    let prec = transc_prec(estimate_ln_dweight(bd) + 1, rscale);
    let v = bf_ln(&num_to_bf(&finite_to_text(bd), prec), prec).ok_or_else(err_log_negative)?;
    Ok(finish_transc(&bf_to_text(&v), rscale))
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

/// numeric log base `base` of `num` (PostgreSQL `numeric_log` / `log_var`).
///
/// The domain errors are `ln`'s, taken on the base before the argument, so
/// `log(0, -1)` reports zero rather than a negative number. A base of exactly
/// one makes `ln(base)` zero and the division 22012, which is how PostgreSQL
/// reports `log(1.0, 12.34)`.
pub fn num_log(base: &NumericValue, num: &NumericValue) -> Result<NumericValue, TypeError> {
    let (b, n) = match (base, num) {
        (NumericValue::Finite(b), NumericValue::Finite(n)) => (b, n),
        _ => return special_log(base, num),
    };
    if let Some(err) = log_domain_error(base).or_else(|| log_domain_error(num)) {
        return Err(err);
    }
    finite_log(b, n).map(NumericValue::Finite)
}

/// `log(base, num)` where at least one operand is `NaN` or an infinity.
///
/// PostgreSQL screens the signs and zeros of *both* operands before it looks at
/// which one is infinite, so `log(Infinity, 0)` is the zero error rather than a
/// shortcut answer.
fn special_log(base: &NumericValue, num: &NumericValue) -> Result<NumericValue, TypeError> {
    if base.is_nan() || num.is_nan() {
        return Ok(NumericValue::NaN);
    }
    if base.signum() < 0 || num.signum() < 0 {
        return Err(err_log_negative());
    }
    if base.is_zero() || num.is_zero() {
        return Err(err_log_zero());
    }
    if base.is_infinite() {
        // Infinity/Infinity is indeterminate; over an infinite base a finite
        // logarithm underflows to plain zero rather than raising.
        if num.is_infinite() {
            return Ok(NumericValue::NaN);
        }
        return Ok(NumericValue::from(0i64));
    }
    Ok(NumericValue::Infinity)
}

/// PostgreSQL `log_var`: divide the two natural logarithms, each computed with
/// eight guard digits past what the quotient's own scale needs.
fn finite_log(base: &BigDecimal, num: &BigDecimal) -> Result<BigDecimal, TypeError> {
    let ln_base_dweight = estimate_ln_dweight(base);
    let ln_num_dweight = estimate_ln_dweight(num);
    let result_dweight = ln_num_dweight - ln_base_dweight;
    let rscale = (MIN_SIG_DIGITS - result_dweight)
        .max(base.fractional_digit_count().max(0))
        .max(num.fractional_digit_count().max(0))
        .clamp(0, TRANSC_MAX_SCALE);
    let ln_base = finite_ln(base, (rscale + result_dweight - ln_base_dweight + 8).max(0))?;
    let ln_num = finite_ln(num, (rscale + result_dweight - ln_num_dweight + 8).max(0))?;
    if finite_is_zero(&ln_base) {
        return Err(TypeError::DivisionByZero);
    }
    Ok(canonical(
        (ln_num / ln_base).with_scale_round(rscale, RoundingMode::HalfUp),
    ))
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

    /// `float8_numeric` converts through `snprintf("%.*g", DBL_DIG, val)`, so a
    /// float8 reaches numeric with at most **fifteen** significant digits — not
    /// the seventeen that `float8out` can need to round-trip. Every expectation
    /// is the pinned PostgreSQL 18.4 answer to `<value>::float8::numeric`, and
    /// every literal was checked against that server's `float8send` so the two
    /// sides start from the same bits.
    #[test]
    fn float8_to_numeric_rounds_to_fifteen_significant_digits() {
        use assert2::assert;
        for (value, expected) in [
            (0.0, "0"),
            // numeric has no negative zero.
            (-0.0, "0"),
            (0.1, "0.1"),
            // Fifteen threes, not the sixteen `0.3333333333333333` would give.
            (1.0 / 3.0, "0.333333333333333"),
            // The sixteenth digit rounds the fifteenth up.
            (2.0 / 3.0, "0.666666666666667"),
            (std::f64::consts::PI, "3.14159265358979"),
            // The smallest float8 above one loses its whole fractional part.
            (1.0000000000000002, "1"),
            (2.5, "2.5"),
            (1e15, "1000000000000000"),
            // Sixteen integer digits: the last is zeroed, not preserved.
            (1_234_567_890_123_456.0, "1234567890123460"),
            (-1_234_567_890_123_456.0, "-1234567890123460"),
            (9_007_199_254_740_992.0, "9007199254740990"),
            // An exact tie at the sixteenth digit. Both PostgreSQL and this go
            // to even, so `…445` falls to `…440` rather than rising to `…450`.
            (4_429_515_941_059_445.0, "4429515941059440"),
            (1.234_567_890_123_456_8e20, "123456789012346000000"),
            (1e-5, "0.00001"),
            (0.000_123_456_789_012_345_67, "0.000123456789012346"),
            (-0.7, "-0.7"),
        ] {
            assert!(to_text(&from_f64(value)) == expected);
        }
    }

    /// The same rule at both ends of the float8 range, where PostgreSQL's texts
    /// run to hundreds of characters. Each is spelled as the significant digits
    /// `%g` keeps — fewer than fifteen once it drops trailing zeros — and the
    /// run of zeros that places them.
    #[test]
    fn float8_to_numeric_holds_fifteen_digits_across_the_double_range() {
        use assert2::assert;
        for (value, expected) in [
            (
                f64::MIN_POSITIVE,
                format!("0.{}22250738585072", "0".repeat(307)),
            ),
            (5e-324, format!("0.{}494065645841247", "0".repeat(323))),
            (1e100, format!("1{}", "0".repeat(100))),
            (f64::MAX, format!("179769313486232{}", "0".repeat(294))),
        ] {
            assert!(to_text(&from_f64(value)) == expected);
        }
    }

    /// The float4 half of the same `%.*g` rule, at `FLT_DIG` = six digits.
    /// Expectations are PostgreSQL 18.4's `<value>::float4::numeric`, with the
    /// literals checked against its `float4send`.
    #[test]
    fn float4_to_numeric_rounds_to_six_significant_digits() {
        use assert2::assert;
        for (value, expected) in [
            (0.1, "0.1"),
            (-0.0, "0"),
            (1e-5, "0.00001"),
            // Seven integer digits, so the last is zeroed.
            (16_777_216.0, "16777200"),
            (123_456.7, "123457"),
            (
                f32::MIN_POSITIVE,
                "0.0000000000000000000000000000000000000117549",
            ),
            (f32::MAX, "340282000000000000000000000000000000000"),
        ] {
            assert!(to_text(&from_f32(value)) == expected);
        }
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
        // `L` emits the C locale's currency symbol, which PostgreSQL's
        // `NUM_prepare_locale` falls back to a single SPACE — not a glyph
        // (oracle-confirmed, PG 18: `to_char(485,'L999')` → `'  485'`).
        assert_eq!(fmt("485", "L999"), "  485");
        assert_eq!(fmt("485", "999L"), " 485 ");
        // `$` is NOT a numeric pattern at all; it is copied through as a literal,
        // so the sign lands AFTER it (oracle-confirmed: `to_char(-485,'$999')` →
        // `'$-485'`).
        assert_eq!(fmt("485", "$999"), "$ 485");
        assert_eq!(fmt("-485", "$999"), "$-485");
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
        // A negative value with a currency marker (anchored `-` + the C locale's
        // blank currency symbol).
        assert_eq!(fmt("-485", "L999"), " -485");
        // Trailing currency on a negative.
        assert_eq!(fmt("-485", "999L"), "-485 ");
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

    /// `log_var` picks the result scale from the estimated weight of the
    /// quotient and floors it at either input's own display scale, so the
    /// printed digit count is part of the answer.
    #[test]
    fn two_argument_log_matches_postgres_value_and_display_scale() {
        use assert2::assert;
        let cases: &[(&str, &str, &str)] = &[
            ("2", "2", "1.0000000000000000"),
            ("2", "4.2", "2.0703893278913979"),
            ("4.2", "2", "0.4830009440873890"),
            ("10", "1000", "3.0000000000000000"),
            ("0.99923", "4.58934e34", "-103611.55579544132"),
            ("1.000016", "8.452010e18", "2723830.2877097365"),
            // A base and argument at opposite ends of the format keep every
            // digit their display scales ask for.
            (
                "1.23e-89",
                "6.4689e45",
                "-0.5152489207781856983977054971756484879653568168479201885425588841094788842469115325262329756",
            ),
        ];
        for (base, num, expected) in cases {
            let got = to_text(&num_log(&n(base), &n(num)).expect("log"));
            assert!(got == *expected, "log({base}, {num}) gave {got}");
        }
    }

    #[test]
    fn two_argument_log_specials_and_domains_match_postgres() {
        use assert2::assert;
        assert!(to_text(&num_log(&n("2"), &n("inf")).expect("log")) == "Infinity");
        assert!(to_text(&num_log(&n("inf"), &n("2")).expect("log")) == "0");
        assert!(to_text(&num_log(&n("inf"), &n("inf")).expect("log")) == "NaN");
        assert!(to_text(&num_log(&n("nan"), &n("2")).expect("log")) == "NaN");
        assert!(to_text(&num_log(&n("2"), &n("nan")).expect("log")) == "NaN");
        // A NaN operand wins over the sign and zero screens.
        assert!(to_text(&num_log(&n("nan"), &n("0")).expect("log")) == "NaN");
        assert!(num_log(&n("0"), &n("10")).unwrap_err() == err_log_zero());
        assert!(num_log(&n("10"), &n("0")).unwrap_err() == err_log_zero());
        assert!(num_log(&n("-inf"), &n("10")).unwrap_err() == err_log_negative());
        assert!(num_log(&n("10"), &n("-inf")).unwrap_err() == err_log_negative());
        // The sign screen runs before the zero screen, even across operands.
        assert!(num_log(&n("inf"), &n("0")).unwrap_err() == err_log_zero());
        assert!(num_log(&n("-inf"), &n("inf")).unwrap_err() == err_log_negative());
        // ln(1) is zero, so there is no divisor.
        assert!(num_log(&n("1.0"), &n("12.34")).unwrap_err() == TypeError::DivisionByZero);
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
    fn roman_numerals_decode_exactly_as_postgres_does() {
        use assert2::assert;

        use super::{NumberTemplate, RomanRefusal, number_template, roman_to_int};
        // A well-formed numeral, its value, and what PostgreSQL 18.4 answers.
        // `None` is `roman_to_int`'s rejection, which `to_number` reports as
        // `invalid Roman numeral`. Every expectation oracle-confirmed.
        let cases: &[(&str, Option<i32>)] = &[
            ("CvIiI", Some(108)),
            ("MMXX  ", Some(2020)),
            ("  XIV", Some(14)),
            ("  XIV  ", Some(14)),
            // Decoding stops at the first non-numeral, so the tail is ignored.
            ("M CC", Some(1000)),
            ("MMMCMXCIX", Some(3999)),
            ("MCMXCIV", Some(1994)),
            ("IV", Some(4)),
            // A subtraction may not be followed by anything as large as the
            // numeral it subtracted, nor repeat the numeral it subtracts from.
            ("viv", None),
            ("VIX", None),
            ("DCCCD", None),
            ("MCCM", None),
            ("XIXL", None),
            // `V`, `L` and `D` may neither repeat nor precede something larger.
            ("VV", None),
            ("IL", None),
            ("LXC", None),
            ("DCM", None),
            ("MMMDCM", None),
            ("CLXC", None),
            // No symbol may repeat more than three times.
            ("MMMM", None),
            ("IIII", None),
            ("XXXX", None),
            // Not a numeral at all.
            ("qiv", None),
            (" ", None),
            ("", None),
        ];
        for (input, want) in cases {
            assert!(roman_to_int(input) == *want, "roman_to_int({input:?})");
        }
        // `RN` tolerates only `FM`; anything else is a template PG will not
        // compile, and a second `RN` is its own refusal.
        assert!(number_template("RN") == NumberTemplate::Roman);
        assert!(number_template("  RN") == NumberTemplate::Roman);
        assert!(number_template("FMRN") == NumberTemplate::Roman);
        assert!(number_template("999") == NumberTemplate::Digits);
        assert!(number_template("MIRN") == NumberTemplate::Refused(RomanRefusal::Incompatible));
        assert!(number_template("RNRN") == NumberTemplate::Refused(RomanRefusal::Twice));
    }

    #[test]
    fn to_char_walks_the_template_in_order() {
        use assert2::assert;

        use super::{NumPrecision, format_numeric_prec};
        // The node-stream features a digit-grid renderer cannot express: a `0`
        // that zero-fills every position to its right, the C locale's BLANK
        // currency symbol, an `SG` sign parked mid-field, literals interleaved
        // with the digits, `G` inside the fraction, double-quoted and
        // backslash-escaped runs, `EEEE` scientific notation, `RN` Roman
        // numerals, and the `TH` ordinal PG drops over a fractional template.
        // Every expectation oracle-confirmed against PostgreSQL 18.4.
        let cases: &[(&str, NumPrecision, &str, &str)] = &[
            (
                "0",
                NumPrecision::Exact,
                "0999999999999999.999",
                " 0000000000000000.000",
            ),
            (
                "-34338492.215397047",
                NumPrecision::Exact,
                "0999999999999999.999",
                "-0000000034338492.215",
            ),
            (
                "4.31",
                NumPrecision::Exact,
                "FM0999999999999999.999909999",
                "0000000000000004.31000",
            ),
            ("0", NumPrecision::Exact, "L9999.099", "      .000"),
            (
                "-83028485",
                NumPrecision::Exact,
                "L9999999999999999.099999999999999",
                "         -83028485.000000000000000",
            ),
            (
                "0",
                NumPrecision::Exact,
                "999999SG9999999999",
                "      +         0",
            ),
            (
                "-34338492.215397047",
                NumPrecision::Exact,
                "999999SG9999999999",
                "      -  34338492",
            ),
            (
                "0",
                NumPrecision::Exact,
                "SG9999999999999999.999999999999999th",
                "+                .000000000000000",
            ),
            (
                "4.31",
                NumPrecision::Exact,
                "FM9999999999999999.999999999999999THPR",
                "4.31",
            ),
            (
                "-34338492.215397047",
                NumPrecision::Exact,
                "FM9999999999999999.999999999999999THPR",
                "<34338492.215397047>",
            ),
            (
                "-34338492.215397047",
                NumPrecision::Exact,
                "S 9 9 9 9 9 9 9 9 . 9 9 9",
                " -3 4 3 3 8 4 9 2 . 2 1 5",
            ),
            (
                "0",
                NumPrecision::Exact,
                "S 9 9 9 9 9 9 9 9 . 9 9 9",
                "                 +. 0 0 0",
            ),
            (
                "-34338492.215397047",
                NumPrecision::Exact,
                "9G999G999G999D999G999",
                "   -34,338,492.215,397",
            ),
            (
                "0",
                NumPrecision::Exact,
                "9G999G999G999D999G999",
                "              .000,000",
            ),
            ("100", NumPrecision::Exact, "f\"ool\"999", "fool 100"),
            ("100", NumPrecision::Exact, "f\\oo999", "f\\oo 100"),
            ("100", NumPrecision::Exact, "f\"ool\\\"999", "fool\"999"),
            ("100", NumPrecision::Exact, "f\"\\\\ool\"999", "f\\ool 100"),
            ("0", NumPrecision::Exact, "9.999EEEE", " 0.000e+00"),
            (
                "-34338492.215397047",
                NumPrecision::Exact,
                "9.999EEEE",
                "-3.434e+07",
            ),
            ("999.5", NumPrecision::Exact, "9.99EEEE", " 10.00e+02"),
            ("12345", NumPrecision::Exact, "99EEEE", " 1e+04"),
            ("NaN", NumPrecision::Exact, "9.999EEEE", " #.#######"),
            ("1.2345e-9", NumPrecision::Exact, "9.999EEEE", " 1.235e-09"),
            ("1234", NumPrecision::Exact, "RN", "       MCCXXXIV"),
            ("4", NumPrecision::Exact, "FMRN", "IV"),
            ("3999", NumPrecision::Exact, "rn", "      mmmcmxcix"),
            (
                "100000000000000000000",
                NumPrecision::Exact,
                "FMRN",
                "###############",
            ),
            ("123", NumPrecision::Exact, "999TH", " 123RD"),
            ("12", NumPrecision::Exact, "FM999th", "12th"),
            (
                "-4567890123456789",
                NumPrecision::Exact,
                "9G999G999G999G999G999D999G999",
                "-4,567,890,123,456,789.000,000",
            ),
            (
                "4567890123456789",
                NumPrecision::Exact,
                "FM9999999999999999.000",
                "4567890123456789.000",
            ),
            (
                "4567890123456789",
                NumPrecision::Exact,
                "L9999999999999999.000",
                "  4567890123456789.000",
            ),
            (
                "4567890123456789",
                NumPrecision::Exact,
                "999999SG9999999999",
                "456789+0123456789",
            ),
            (
                "4567890123456789",
                NumPrecision::Exact,
                "FM9999999999999999THPR",
                "4567890123456789TH",
            ),
            (
                "-4567890123456789",
                NumPrecision::Exact,
                "FM9999999999999999THPR",
                "<4567890123456789>",
            ),
            ("1234", NumPrecision::Exact, "9.99EEEE", " 1.23e+03"),
            ("-1234", NumPrecision::Exact, "9.99eeee", "-1.23e+03"),
            (
                "12345678901",
                NumPrecision::Float8,
                "FM9999999999D9999900000000000000000",
                "##########.####",
            ),
            ("2.5", NumPrecision::Float8, "999", "   2"),
            ("-0.0001", NumPrecision::Float8, "999", "  -0"),
            ("0.5", NumPrecision::Float8, "RN", "###############"),
            ("4.31", NumPrecision::Float8, "9.999EEEE", " 4.310e+00"),
            ("4.31", NumPrecision::Float4, "9.999EEEE", " 4.310e+00"),
        ];
        for (value, precision, template, want) in cases {
            let got = format_numeric_prec(template, &n(value), *precision)
                .unwrap_or_else(|e| panic!("to_char({value}, {template}): {e}"));
            assert!(
                got == *want,
                "to_char({value}, {template}) = {got:?}, want {want:?}"
            );
        }
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
            ("nan", "L999", "  NaN"),
            ("-inf", "L999", " -###"),
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
