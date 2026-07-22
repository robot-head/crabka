//! SP32: arbitrary-precision exact `numeric` / `decimal` (OID 1700), backed by
//! `bigdecimal::BigDecimal`. This module is the value layer for numeric: parsing,
//! PostgreSQL-faithful text + binary output, the arithmetic scale rules
//! (`select_div_scale` for division/AVG), rounding, `numeric(p,s)` typmod
//! enforcement, and the casts to/from the other types.
//!
//! Invariant: every numeric `Datum` is **canonical** — its display scale (dscale)
//! is `>= 0`, matching PostgreSQL (a literal like `1e3` parses to scale 0, not the
//! negative scale `bigdecimal` would otherwise keep). The deferred non-goals are
//! the `NaN`/`±Infinity` specials (so no special-value propagation here).

#![expect(
    clippy::pedantic,
    reason = "vendored PostgreSQL-compatible numeric semantics kept structurally close to donor"
)]

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
/// outside these "overflows numeric format" — PostgreSQL rejects it, and so do we
/// (which ALSO bounds materialization: a literal like `8e88888888` would otherwise
/// expand to ~88M digits and OOM, as the `decode_row` fuzzer found).
const MAX_WEIGHT: i64 = 131071;
const MAX_DSCALE: i64 = 16383;

/// Optional `numeric(precision, scale)` type modifier. Absent = unconstrained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Typmod {
    pub precision: u16,
    pub scale: u16,
}

/// Canonicalize a `BigDecimal` to a PostgreSQL dscale (`>= 0`). A negative scale
/// (e.g. from `1e3`) is materialized to scale 0 (exact — only appends zeros).
pub fn canonical(bd: BigDecimal) -> BigDecimal {
    if bd.fractional_digit_count() < 0 {
        bd.with_scale(0)
    } else {
        bd
    }
}

/// Parse a numeric literal / text value (PostgreSQL `numeric_in`, minus the
/// deferred `NaN`/`Infinity` spellings). Leading/trailing whitespace is trimmed.
/// Returns `None` on bad syntax OR a value that overflows the numeric format
/// (the caller maps either to an error). The overflow check runs BEFORE
/// [`canonical`], whose `with_scale` would otherwise materialize an adversarial
/// exponent's digits and OOM.
pub fn parse(s: &str) -> Option<BigDecimal> {
    use std::str::FromStr;
    let t = s.trim();
    if t.is_empty() {
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

/// PostgreSQL `numeric_out`: a plain decimal string (never scientific notation),
/// with exactly `dscale` fractional digits. (`bigdecimal`'s own `Display` switches
/// to `E` notation for small magnitudes, so this is hand-written.)
pub fn to_text(bd: &BigDecimal) -> String {
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

/// PostgreSQL `numeric_send` (binary): `int16 ndigits`, `int16 weight`,
/// `uint16 sign` (0x0000 +, 0x4000 −), `int16 dscale`, then `ndigits` base-10000
/// groups (`int16`, most significant first), with leading/trailing zero groups
/// stripped. Exercised only by binary-format clients (the text path covers the
/// wire tests + conformance), so it is proven by unit tests over hand-computed
/// vectors.
pub fn binary(bd: &BigDecimal) -> Vec<u8> {
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
/// value outside PostgreSQL's supported format limits. PostgreSQL's `NaN` binary
/// representation is deliberately unsupported because [`Datum`] has no numeric
/// special-value variant.
pub fn from_binary(input: &[u8]) -> Option<BigDecimal> {
    let header: [u8; 8] = input.get(..8)?.try_into().ok()?;
    let ndigits = usize::try_from(i16::from_be_bytes(header[0..2].try_into().ok()?)).ok()?;
    let weight = i32::from(i16::from_be_bytes(header[2..4].try_into().ok()?));
    let sign = u16::from_be_bytes(header[4..6].try_into().ok()?);
    let dscale = usize::try_from(i16::from_be_bytes(header[6..8].try_into().ok()?)).ok()?;
    if !matches!(sign, 0x0000 | 0x4000) || dscale > MAX_DSCALE as usize {
        return None;
    }
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

    let is_zero = digits.iter().all(|digit| *digit == 0);
    let mut text = numeric_binary_to_text(&digits, weight, dscale)?;
    if sign == 0x4000 && !is_zero {
        text.insert(0, '-');
    }
    parse(&text)
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

/// `a + b` (result dscale = max input dscale — `bigdecimal` matches PostgreSQL).
pub fn add(a: &BigDecimal, b: &BigDecimal) -> BigDecimal {
    canonical(a + b)
}
/// `a - b` (result dscale = max input dscale).
pub fn sub(a: &BigDecimal, b: &BigDecimal) -> BigDecimal {
    canonical(a - b)
}
/// `a * b` (result dscale = sum of input dscales).
pub fn mul(a: &BigDecimal, b: &BigDecimal) -> BigDecimal {
    canonical(a * b)
}

/// `a / b` with PostgreSQL's display-scale rule (`select_div_scale`), rounded
/// half-away-from-zero. A zero divisor is 22012.
pub fn div(a: &BigDecimal, b: &BigDecimal) -> Result<BigDecimal, TypeError> {
    if is_zero(b) {
        return Err(TypeError::DivisionByZero);
    }
    let rscale = select_div_scale(a, b);
    Ok((a / b).with_scale_round(rscale, RoundingMode::HalfUp))
}

/// `mod(a, b)` for numeric (the remainder takes the dividend's sign, like PG). A
/// zero divisor is 22012.
pub fn rem(a: &BigDecimal, b: &BigDecimal) -> Result<BigDecimal, TypeError> {
    if is_zero(b) {
        return Err(TypeError::DivisionByZero);
    }
    Ok(canonical(a % b))
}

pub fn abs(bd: &BigDecimal) -> BigDecimal {
    bd.abs()
}

/// `floor(x)` — round toward −∞ (PostgreSQL `numeric_floor`); scale 0.
pub fn floor(bd: &BigDecimal) -> BigDecimal {
    canonical(bd.with_scale_round(0, RoundingMode::Floor))
}

/// `ceil(x)` / `ceiling(x)` — round toward +∞ (PostgreSQL `numeric_ceil`); scale 0.
pub fn ceil(bd: &BigDecimal) -> BigDecimal {
    canonical(bd.with_scale_round(0, RoundingMode::Ceiling))
}

/// `round(x, n)` — round to `n` decimal places, half-away-from-zero (PostgreSQL
/// `numeric_round`). `n` may be negative (round to tens/hundreds/…). The result
/// carries scale `max(n, 0)`. `n` is clamped to `MAX_DSCALE` so an adversarial
/// huge scale can't materialize billions of fractional digits and OOM — the same
/// format-limit discipline [`within_format_limits`] enforces on `parse`.
pub fn round(bd: &BigDecimal, n: i64) -> BigDecimal {
    canonical(bd.with_scale_round(n.min(MAX_DSCALE), RoundingMode::HalfUp))
}

/// `trunc(x, n)` — truncate to `n` decimal places, toward zero (PostgreSQL
/// `numeric_trunc`). `n` may be negative; clamped to `MAX_DSCALE` (see [`round`]).
pub fn trunc(bd: &BigDecimal, n: i64) -> BigDecimal {
    canonical(bd.with_scale_round(n.min(MAX_DSCALE), RoundingMode::Down))
}

/// `sign(x)` — −1 / 0 / 1 as a numeric (PostgreSQL `numeric_sign`).
pub fn sign(bd: &BigDecimal) -> BigDecimal {
    use std::cmp::Ordering;
    match bd.cmp(&BigDecimal::from(0)) {
        Ordering::Less => BigDecimal::from(-1),
        Ordering::Equal => BigDecimal::from(0),
        Ordering::Greater => BigDecimal::from(1),
    }
}

fn is_zero(bd: &BigDecimal) -> bool {
    bd.as_bigint_and_exponent()
        .0
        .to_string()
        .trim_start_matches('-')
        == "0"
}

/// PostgreSQL `select_div_scale`: the division/AVG display scale. In base-10000
/// units, `rscale = clamp(max(16 − qweight·4, s1 + s2), 0, 1000)` where `qweight`
/// is the quotient's leading-digit weight estimate.
fn select_div_scale(a: &BigDecimal, b: &BigDecimal) -> i64 {
    let (w1, f1) = nbase_weight_and_lead(a);
    let (w2, f2) = nbase_weight_and_lead(b);
    let mut qweight = w1 - w2;
    if f1 < f2 {
        qweight -= 1;
    }
    let s1 = a.fractional_digit_count().max(0);
    let s2 = b.fractional_digit_count().max(0);
    (MIN_SIG_DIGITS - qweight * DEC_DIGITS)
        .max(s1 + s2)
        .clamp(0, MAX_DISPLAY_SCALE)
}

/// The base-10000 weight of the leading digit, and that leading group's value
/// (right-padded to four decimal digits) — the two inputs `select_div_scale`
/// needs. Zero has weight 0 and leading group 0.
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
    if is_zero(bd) {
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
fn estimate_ln_dweight(arg: &BigDecimal) -> i64 {
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

/// `numeric → int4` / `int8`: round half-away-from-zero (PostgreSQL `numeric_int4`,
/// distinct from `float8 → int`'s round-half-to-even), then range-check (22003).
pub fn to_i32(bd: &BigDecimal) -> Result<i32, TypeError> {
    bd.with_scale_round(0, RoundingMode::HalfUp)
        .to_i32()
        .ok_or(TypeError::Overflow)
}
pub fn to_i64(bd: &BigDecimal) -> Result<i64, TypeError> {
    bd.with_scale_round(0, RoundingMode::HalfUp)
        .to_i64()
        .ok_or(TypeError::Overflow)
}

pub fn from_i64(n: i64) -> BigDecimal {
    BigDecimal::from(n)
}

/// `float8 → numeric` via the float's shortest round-tripping text (PostgreSQL
/// `float8_numeric`), so `0.1::float8::numeric` is `0.1`, not the exact binary
/// expansion. A non-finite float has no numeric value here (deferred specials).
pub fn from_f64(f: f64) -> Result<BigDecimal, TypeError> {
    if !f.is_finite() {
        return Err(TypeError::Overflow);
    }
    parse(&format!("{f}")).ok_or(TypeError::Overflow)
}

/// `numeric → float8`. A magnitude beyond `f64` range becomes `±Infinity`, like
/// PostgreSQL's `numeric_float8`.
pub fn to_f64(bd: &BigDecimal) -> f64 {
    bd.to_f64().unwrap_or_else(|| {
        if bd.sign() == bigdecimal::num_bigint::Sign::Minus {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        }
    })
}

/// Apply a `numeric(precision, scale)` type modifier: round to `scale`
/// (half-away-from-zero) then check the integer-digit budget `precision − scale`;
/// an overflow is 22003 ("numeric field overflow").
pub fn apply_typmod(bd: &BigDecimal, tm: Typmod) -> Result<BigDecimal, TypeError> {
    let r = bd.with_scale_round(i64::from(tm.scale), RoundingMode::HalfUp);
    if !is_zero(&r) {
        let (mant, scale) = r.as_bigint_and_exponent();
        let len = mant.to_string().trim_start_matches('-').len() as i64;
        let int_digits = len - scale; // integer-part digit count
        if int_digits > i64::from(tm.precision) - i64::from(tm.scale) {
            return Err(TypeError::Overflow);
        }
    }
    Ok(canonical(r))
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
    /// No explicit sign pattern: PostgreSQL reserves ONE leading column — a blank
    /// for a non-negative value, `-` for a negative one. `FM` strips that blank.
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
    /// BETWEEN the digit at `idx-1` and `idx`; we store the count of digits to the
    /// left of each separator.
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
/// PostgreSQL's `to_char(numeric, text)` is extremely lenient — it never raises an
/// error for a malformed template; an unsupported character is emitted literally
/// and an oversized integer part is `#`-filled. So this function only returns a
/// `Result` to match the engine signature contract; in practice it is always `Ok`.
pub fn format_numeric(template: &str, value: &BigDecimal) -> Result<String, TypeError> {
    let desc = parse_num_template(template);

    // (1) Apply the `V` shift: multiply by 10^shift. The shift digits were already
    // folded into `desc.pre` (integer positions) by the template parser. Build the
    // multiplier from text ("1" + n zeros) so a large `n` never overflows a `u64`.
    let shifted = match desc.v_shift {
        Some(0) | None => value.clone(),
        Some(n) => {
            let pow10 = parse(&format!("1{}", "0".repeat(n as usize)))
                .unwrap_or_else(|| BigDecimal::from(1));
            canonical(value * pow10)
        }
    };

    // (2) Round half-away-from-zero to the fractional-digit count.
    let rounded = round(&shifted, desc.post as i64);
    let negative = rounded.sign() == bigdecimal::num_bigint::Sign::Minus && !is_zero(&rounded);

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

/// The `#`-filled overflow CORE — the digit grid only. PG `#`-fills every digit
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
///    (or right) of the field — the digits do NOT move toward the sign.
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
/// `DBig::ln` panics on non-positive input, so we guard first.
fn bf_ln(x: &DBig, _prec: usize) -> Option<DBig> {
    // Use comparison to DBig::ZERO: PartialOrd is implemented for DBig.
    // is_zero() is on Repr, so check sign via comparison.
    if *x <= DBig::ZERO {
        return None;
    }
    Some(x.ln())
}

/// `sqrt(x)` at `prec` significant digits; `None` for `x < 0`.
/// `DBig::sqrt` (via the `SquareRoot` trait) panics on negative input, so we guard first.
fn bf_sqrt(x: &DBig, _prec: usize) -> Option<DBig> {
    if *x < DBig::ZERO {
        return None;
    }
    Some(x.sqrt())
}

/// `pow(base, exp)` at `prec` significant digits; `None` for non-positive base.
/// `DBig::powf` panics on non-positive base, so we guard first.
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
/// (so a degenerate caller can't panic) and capped — callers bound the magnitude
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
/// is within the numeric format, so `parse` always succeeds here.
fn finish_transc(value_text: &str, rscale: i64) -> BigDecimal {
    let bd = parse(value_text).expect("bounded transcendental result is within numeric format");
    canonical(bd.with_scale_round(rscale, RoundingMode::HalfUp))
}

/// 2201F — square root of a negative number.
fn err_sqrt_negative() -> TypeError {
    TypeError::Domain {
        sqlstate: "2201F",
        message: "cannot take square root of a negative number",
    }
}
/// 2201E — logarithm of a non-positive number.
fn err_log_nonpositive() -> TypeError {
    TypeError::Domain {
        sqlstate: "2201E",
        message: "cannot take logarithm of a non-positive number",
    }
}
/// 2201F — zero raised to a negative power.
fn err_zero_neg_power() -> TypeError {
    TypeError::Domain {
        sqlstate: "2201F",
        message: "zero raised to a negative power is undefined",
    }
}
/// 2201F — a negative base raised to a non-integer power (complex result).
fn err_neg_noninteger_power() -> TypeError {
    TypeError::Domain {
        sqlstate: "2201F",
        message: "a negative number raised to a non-integer power yields a complex result",
    }
}

/// numeric sqrt; `Err(2201F)` for a negative argument. (sqrt shrinks magnitude,
/// so it never overflows the numeric format.)
pub fn num_sqrt(arg: &BigDecimal) -> Result<BigDecimal, TypeError> {
    let rscale = sqrt_rscale(arg);
    if is_zero(arg) {
        return Ok(canonical(
            BigDecimal::from(0).with_scale_round(rscale, RoundingMode::HalfUp),
        ));
    }
    if arg.sign() == bigdecimal::num_bigint::Sign::Minus {
        return Err(err_sqrt_negative());
    }
    let prec = transc_prec(decimal_weight(arg) / 2, rscale);
    let v = bf_sqrt(&num_to_bf(&to_text(arg), prec), prec).ok_or_else(err_sqrt_negative)?;
    Ok(finish_transc(&bf_to_text(&v), rscale))
}

/// numeric ln; `Err(2201E)` for arg <= 0. (ln of an in-format value never
/// overflows — its magnitude is at most ~`ln(10)·weight`.)
pub fn num_ln(arg: &BigDecimal) -> Result<BigDecimal, TypeError> {
    if is_zero(arg) || arg.sign() == bigdecimal::num_bigint::Sign::Minus {
        return Err(err_log_nonpositive());
    }
    let rscale = ln_rscale(arg);
    let prec = transc_prec(estimate_ln_dweight(arg) + 1, rscale);
    let v = bf_ln(&num_to_bf(&to_text(arg), prec), prec).ok_or_else(err_log_nonpositive)?;
    Ok(finish_transc(&bf_to_text(&v), rscale))
}

/// numeric log base 10; `Err(2201E)` for arg <= 0.
pub fn num_log10(arg: &BigDecimal) -> Result<BigDecimal, TypeError> {
    if is_zero(arg) || arg.sign() == bigdecimal::num_bigint::Sign::Minus {
        return Err(err_log_nonpositive());
    }
    let rscale = ln_rscale(arg);
    let prec = transc_prec(estimate_ln_dweight(arg) + 1, rscale) + 8;
    // log10(x) = ln(x) / ln(10), both at high precision, then round to rscale.
    let lnx = bf_ln(&num_to_bf(&to_text(arg), prec), prec).ok_or_else(err_log_nonpositive)?;
    let ln10 = bf_ln(&num_to_bf("10", prec), prec).expect("ln(10) defined");
    let lnx_bd = parse(&bf_to_text(&lnx)).expect("ln result is a valid numeric");
    let ln10_bd = parse(&bf_to_text(&ln10)).expect("ln10 is a valid numeric");
    let quotient = (lnx_bd / ln10_bd).with_scale_round(rscale + 4, RoundingMode::HalfUp);
    Ok(canonical(
        quotient.with_scale_round(rscale, RoundingMode::HalfUp),
    ))
}

/// numeric exp; `Err(22003)` when the result overflows the numeric format.
/// PostgreSQL `exp_var` overflows for `arg >= NUMERIC_MAX_RESULT_SCALE*3 = 6000`
/// (a one-sided bound: a large NEGATIVE argument underflows toward 0, not an error).
pub fn num_exp(arg: &BigDecimal) -> Result<BigDecimal, TypeError> {
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
    let v = bf_exp(&num_to_bf(&to_text(arg), prec), prec);
    Ok(finish_transc(&bf_to_text(&v), rscale))
}

/// numeric power; `Err(2201F)` on a domain error (0^neg, negative^non-integer),
/// `Err(22003)` when the result overflows the numeric format.
pub fn num_power(base: &BigDecimal, exp: &BigDecimal) -> Result<BigDecimal, TypeError> {
    use bigdecimal::num_bigint::Sign;
    if is_zero(base) {
        if exp.sign() == Sign::Minus {
            return Err(err_zero_neg_power());
        }
        if is_zero(exp) {
            return Ok(power_finish(BigDecimal::from(1)));
        }
        return Ok(power_finish(BigDecimal::from(0)));
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
    // exact integer exponent -> powi (handles negative base + negative exponent)
    if exp.is_integer()
        && let Ok(e) = to_i64(exp)
    {
        return Ok(power_finish(base.powi(e)));
    }
    // non-integer exponent: base must be > 0 (the negative case returned above).
    let rweight = (exp_f64 * decimal_weight(base) as f64) as i64;
    let rscale = (MIN_SIG_DIGITS - rweight).clamp(0, TRANSC_MAX_SCALE);
    let prec = transc_prec(rweight, rscale);
    let v = bf_powf(
        &num_to_bf(&to_text(base), prec),
        &num_to_bf(&to_text(exp), prec),
        prec,
    )
    .ok_or_else(err_neg_noninteger_power)?;
    Ok(finish_transc(&bf_to_text(&v), rscale))
}

/// Is `value` an exact power of ten (its significant digits are just "1")?
/// e.g. 0.001, 0.1, 1000 — but not 0.04 or 0.125. PostgreSQL's integer-power
/// display scale gives these one MORE fractional digit than other sub-1 results.
fn is_power_of_ten(value: &BigDecimal) -> bool {
    if is_zero(value) {
        return false;
    }
    let (mant, _) = value.as_bigint_and_exponent();
    mant.to_string()
        .trim_start_matches('-')
        .trim_end_matches('0')
        == "1"
}

/// Round an exact (integer-exponent) power result to PostgreSQL's `power_var_int`
/// display scale (validated against PostgreSQL 17.10 across a battery). The rule:
/// weight ≥ 0 → `16 - weight` (1024 → 13, 8 → 16); a sub-1 result → `15 - weight`
/// (0.04 → 17, 0.125 → 16), EXCEPT an exact power of ten keeps one more digit,
/// `16 - weight` (0.001 → 19, 0.1 → 17). The leading digit is the first
/// significant digit at position `weight`, so a sub-1 result needs one fewer
/// rscale digit to reach 16 significant digits — except a power of ten, whose
/// single significant digit keeps the extra one.
fn power_finish(value: BigDecimal) -> BigDecimal {
    let rweight = decimal_weight(&value);
    let rscale = if rweight < 0 && !is_power_of_ten(&value) {
        (MIN_SIG_DIGITS - rweight - 1).clamp(0, TRANSC_MAX_SCALE)
    } else {
        (MIN_SIG_DIGITS - rweight).clamp(0, TRANSC_MAX_SCALE)
    };
    canonical(value.with_scale_round(rscale, RoundingMode::HalfUp))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn n(s: &str) -> BigDecimal {
        parse(s).expect("parse")
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
        assert!(parse("NaN").is_none()); // specials deferred
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
        assert_eq!(to_text(&from_f64(0.1).expect("f")), "0.1");
        assert_eq!(to_text(&from_f64(2.5).expect("f")), "2.5");
        assert_eq!(to_f64(&n("1.5")), 1.5);
        assert!(matches!(from_f64(f64::INFINITY), Err(TypeError::Overflow)));
        assert!(matches!(from_f64(f64::NAN), Err(TypeError::Overflow)));
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
        assert!(from_binary(&[0, 1, 0, 0, 0xc0, 0, 0, 0, 0, 0]).is_none());
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
        assert_eq!(BigDecimal::from_str("1.50").expect("x"), n("1.5"));
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
        let n = |s: &str| parse(s).expect("parse");
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
        let t = |bd: &BigDecimal| to_text(bd);
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
        let t = |bd: &BigDecimal| to_text(bd);
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
        assert!(round(&n("2.5"), 2_000_000_000).fractional_digit_count() <= MAX_DSCALE);
        assert!(trunc(&n("2.5"), 2_000_000_000).fractional_digit_count() <= MAX_DSCALE);
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
}
