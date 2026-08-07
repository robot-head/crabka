//! `PostgreSQL`'s `money` type (OID 790) — the port of `cash.c`.
//!
//! A money value is an `i64` count of minor currency units (cents), which is
//! why every operation here is exact integer arithmetic and why the type spans
//! exactly `-$92,233,720,368,547,758.08` … `$92,233,720,368,547,758.07`.
//!
//! `PostgreSQL` reads its formatting from `lc_monetary` through
//! `PGLC_localeconv()`. Crabka has no locale support, so the only locale is `C`,
//! whose `struct lconv` carries empty strings for every symbol and `CHAR_MAX`
//! for every numeric member. [`MonetaryLocale`] names the constants `cash.c`'s
//! fallbacks derive from that — they are what makes `0` print as `$0.00` and
//! `-1234500` as `-$12,345.00`.
//!
//! # Key Functions
//!
//! - [`parse`] / [`to_text`] — `cash_in` / `cash_out`.
//! - [`words`] — `cash_words`, the "One dollar and one cent" spelling.
//! - [`to_numeric`] / [`from_numeric`] — `cash_numeric` / `numeric_cash`.
//! - [`Money`] — the newtype a `Datum` variant holds; every method delegates to
//!   the free function of the same name.

use std::fmt;

use bigdecimal::{BigDecimal, ToPrimitive, num_bigint::BigInt};

use crate::{
    TypeError,
    numeric::{self, NumericValue},
};

/// `PostgreSQL` `money` type OID.
pub const OID: u32 = 790;

/// The monetary formatting parameters `cash_in` and `cash_out` derive from
/// `lconv`.
///
/// Every field is the value `cash.c`'s fallbacks produce for the C locale,
/// where the symbol members are empty strings and the numeric members are
/// `CHAR_MAX`. The one asymmetry worth spelling out is the positive sign:
/// `cash_in` defaults it to `+`, `cash_out` does **not** default it at all, so
/// `+1.00` parses but a positive value never prints a sign.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MonetaryLocale {
    /// `frac_digits` — digits after the decimal point. C gives `CHAR_MAX`,
    /// which is outside the plausible `0..=10` range `cash.c` range-checks, so
    /// the fallback 2 applies.
    pub frac_digits: u32,
    /// `mon_decimal_point`, restricted to a single byte. C gives `""`, so `.`.
    pub decimal_point: u8,
    /// `mon_thousands_sep`. C gives `""`, and the fallback is `,` unless the
    /// decimal point is itself `,` — the two must never collide.
    pub thousands_sep: &'static str,
    /// `currency_symbol`. C gives `""`, so `$`.
    pub currency_symbol: &'static str,
    /// `*mon_grouping` — digits per group left of the radix point. C gives `0`,
    /// outside the `1..=6` range check, so the fallback 3 applies.
    pub grouping: i64,
    /// `negative_sign`. C gives `""`, so `-`.
    pub negative_sign: &'static str,
    /// `positive_sign` as `cash_out` reads it: taken verbatim, never defaulted,
    /// so a positive amount prints with no sign at all.
    pub positive_sign_out: &'static str,
    /// `positive_sign` as `cash_in` reads it: defaulted to `+` when empty, so a
    /// leading or trailing `+` is accepted on input.
    pub positive_sign_in: &'static str,
    /// `p_sign_posn` / `n_sign_posn`. C gives `CHAR_MAX`, which falls through
    /// `cash_out`'s `switch` to the `default:` arm — the same body as POSIX
    /// position 1, "the sign string precedes the quantity and the currency
    /// symbol".
    pub sign_posn: u8,
    /// `p_cs_precedes` / `n_cs_precedes`. C gives `CHAR_MAX`, which is nonzero,
    /// so the currency symbol comes before the digits.
    pub cs_precedes: bool,
    /// `p_sep_by_space` / `n_sep_by_space`. C gives `CHAR_MAX`, which is
    /// neither 1 nor 2, so no space is inserted anywhere.
    pub sep_by_space: u8,
}

/// The C locale, the only `lc_monetary` Crabka has.
pub const C_LOCALE: MonetaryLocale = MonetaryLocale {
    frac_digits: 2,
    decimal_point: b'.',
    thousands_sep: ",",
    currency_symbol: "$",
    grouping: 3,
    negative_sign: "-",
    positive_sign_out: "",
    positive_sign_in: "+",
    sign_posn: 1,
    cs_precedes: true,
    sep_by_space: 0,
};

/// `10^frac_digits` — the scale factor `cash_numeric`, `numeric_cash`,
/// `int4_cash` and `int8_cash` each build with a multiply loop.
const SCALE_FACTOR: i64 = 10_i64.pow(C_LOCALE.frac_digits);

/// `FLOAT8_FITS_IN_INT64`'s lower bound (`c.h`): `PG_INT64_MIN` is an exact
/// power of two, so it survives the round trip through `float8` and the bound
/// is inclusive.
const FLOAT8_INT64_MIN: f64 = -9_223_372_036_854_775_808.0;

/// `FLOAT8_FITS_IN_INT64`'s upper bound: `PG_INT64_MAX` is *not* exactly
/// representable, so `c.h` uses `-(float8) PG_INT64_MIN` and tests it
/// exclusively. This is why `'92233720368547758.07'::money * 1.0` overflows —
/// `INT64_MAX` rounds *up* to this bound when it becomes a `float8`.
const FLOAT8_INT64_MAX_EXCLUSIVE: f64 = 9_223_372_036_854_775_808.0;

/// A `PostgreSQL` `money` value: a signed count of minor currency units.
///
/// The wrapper exists so a `Datum` variant cannot be confused with a plain
/// `bigint`; the value semantics are exactly `i64`'s, and every method here
/// forwards to the free function of the same name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Money(pub i64);

impl Money {
    /// `cash_in` — see [`parse`].
    ///
    /// # Errors
    ///
    /// `22003` for a value outside the type's range, `22P02` for bad syntax.
    pub fn parse(input: &str) -> Result<Self, TypeError> {
        parse(input).map(Money)
    }

    /// `cash_out` — see [`to_text`].
    #[must_use]
    pub fn to_text(self) -> String {
        to_text(self.0)
    }

    /// `cash_words` — see [`words`].
    #[must_use]
    pub fn words(self) -> String {
        words(self.0)
    }

    /// `cash_pl` — see [`add`].
    ///
    /// # Errors
    ///
    /// `22003` `money out of range` on overflow.
    pub fn checked_add(self, other: Self) -> Result<Self, TypeError> {
        add(self.0, other.0).map(Money)
    }

    /// `cash_mi` — see [`sub`].
    ///
    /// # Errors
    ///
    /// `22003` `money out of range` on overflow.
    pub fn checked_sub(self, other: Self) -> Result<Self, TypeError> {
        sub(self.0, other.0).map(Money)
    }

    /// `cash_mul_flt8` — see [`mul_float8`].
    ///
    /// # Errors
    ///
    /// `22003`, either `money out of range` or the float layer's
    /// `value out of range: overflow`.
    pub fn mul_float8(self, f: f64) -> Result<Self, TypeError> {
        mul_float8(self.0, f).map(Money)
    }

    /// `cash_div_flt8` — see [`div_float8`].
    ///
    /// # Errors
    ///
    /// `22012` for a zero divisor; `22003` on overflow.
    pub fn div_float8(self, f: f64) -> Result<Self, TypeError> {
        div_float8(self.0, f).map(Money)
    }

    /// `cash_mul_int8` — see [`mul_int64`].
    ///
    /// # Errors
    ///
    /// `22003` `money out of range` on overflow.
    pub fn mul_int64(self, i: i64) -> Result<Self, TypeError> {
        mul_int64(self.0, i).map(Money)
    }

    /// `cash_div_int8` — see [`div_int64`].
    ///
    /// # Errors
    ///
    /// `22012` `division by zero` for a zero divisor.
    pub fn div_int64(self, i: i64) -> Result<Self, TypeError> {
        div_int64(self.0, i).map(Money)
    }

    /// `cash_div_cash` — see [`div_cash`].
    ///
    /// # Errors
    ///
    /// `22012` `division by zero` for a zero divisor.
    pub fn div_cash(self, other: Self) -> Result<f64, TypeError> {
        div_cash(self.0, other.0)
    }

    /// `cashlarger` — see [`larger`].
    #[must_use]
    pub fn larger(self, other: Self) -> Self {
        Money(larger(self.0, other.0))
    }

    /// `cashsmaller` — see [`smaller`].
    #[must_use]
    pub fn smaller(self, other: Self) -> Self {
        Money(smaller(self.0, other.0))
    }

    /// `cash_numeric` — see [`to_numeric`].
    #[must_use]
    pub fn to_numeric(self) -> NumericValue {
        to_numeric(self.0)
    }

    /// `numeric_cash` — see [`from_numeric`].
    ///
    /// # Errors
    ///
    /// `22003` `bigint out of range`, or `0A000` for a `NaN` / infinite input.
    pub fn from_numeric(amount: &NumericValue) -> Result<Self, TypeError> {
        from_numeric(amount).map(Money)
    }

    /// `int8_cash` — see [`from_int8`].
    ///
    /// # Errors
    ///
    /// `22003` `bigint out of range` on overflow.
    pub fn from_int8(amount: i64) -> Result<Self, TypeError> {
        from_int8(amount).map(Money)
    }

    /// `int4_cash` — see [`from_int4`].
    ///
    /// # Errors
    ///
    /// `22003` `bigint out of range` on overflow (unreachable from `int4`).
    pub fn from_int4(amount: i32) -> Result<Self, TypeError> {
        from_int4(amount).map(Money)
    }

    /// `cash_send` — see [`to_binary`].
    #[must_use]
    pub fn to_binary(self) -> Vec<u8> {
        to_binary(self.0)
    }

    /// `cash_recv` — see [`from_binary`].
    ///
    /// # Errors
    ///
    /// `08P01` for a short message, `22P03` for a long one.
    pub fn from_binary(bytes: &[u8]) -> Result<Self, TypeError> {
        from_binary(bytes).map(Money)
    }
}

impl fmt::Display for Money {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&to_text(self.0))
    }
}

/// `cash_in`: parse a `money` literal, in the format `[$]###[,]###[.##]`.
///
/// The grammar is `cash.c`'s, which is looser than the documentation suggests:
/// the currency symbol may appear twice at the front (once before the sign and
/// once after), the thousands separator is skipped **anywhere** in the digit run
/// (`1,2,3` is `$123.00`), a leading `(` opens a negative without any check that
/// it is ever closed, and the trailing run accepts whitespace, `)`, either sign
/// and the currency symbol in any order and any number. A trailing `-` therefore
/// makes the whole value negative wherever it lands.
///
/// Digits are accumulated **negative** so that `INT64_MIN` is representable, and
/// the fractional part is rounded half-**up** by looking at the single digit
/// past `frac_digits`.
///
/// # Errors
///
/// [`TypeError::OutOfRange`] (`22003`) with
/// `value "…" is out of range for type money` when the digits overflow, and
/// [`TypeError::InvalidText`] (`22P02`) with
/// `invalid input syntax for type money: "…"` for anything the trailing run
/// cannot consume.
pub fn parse(input: &str) -> Result<i64, TypeError> {
    let out_of_range = || TypeError::value_out_of_range(input, "money");
    let invalid = || TypeError::InvalidText {
        type_name: "money",
        value: input.to_string(),
    };

    let bytes = input.as_bytes();
    let mut at = 0usize;
    let mut negative = false;

    // Strip leading whitespace and any leading currency symbol.
    skip_spaces(bytes, &mut at);
    skip_symbol(bytes, &mut at, C_LOCALE.currency_symbol);
    skip_spaces(bytes, &mut at);

    // A leading minus or paren signifies a negative number. `cash_in` does not
    // check that the paren is ever balanced.
    if starts_with(bytes, at, C_LOCALE.negative_sign) {
        negative = true;
        at += C_LOCALE.negative_sign.len();
    } else if bytes.get(at) == Some(&b'(') {
        negative = true;
        at += 1;
    } else if starts_with(bytes, at, C_LOCALE.positive_sign_in) {
        at += C_LOCALE.positive_sign_in.len();
    }

    // Whitespace and a currency symbol are allowed after the sign, too.
    skip_spaces(bytes, &mut at);
    skip_symbol(bytes, &mut at, C_LOCALE.currency_symbol);
    skip_spaces(bytes, &mut at);

    // The magnitude is built in the negative, so INT64_MIN is reachable and the
    // overflow check is the same one on both sides of zero.
    let mut value: i64 = 0;
    let mut dec: u32 = 0;
    let mut seen_dot = false;

    while let Some(&byte) = bytes.get(at) {
        if byte.is_ascii_digit() && (!seen_dot || dec < C_LOCALE.frac_digits) {
            let digit = i64::from(byte - b'0');
            value = value
                .checked_mul(10)
                .and_then(|scaled| scaled.checked_sub(digit))
                .ok_or_else(out_of_range)?;
            if seen_dot {
                dec += 1;
            }
        } else if byte == C_LOCALE.decimal_point && !seen_dot {
            seen_dot = true;
        } else if starts_with(bytes, at, C_LOCALE.thousands_sep) {
            at += C_LOCALE.thousands_sep.len() - 1;
        } else {
            break;
        }
        at += 1;
    }

    // Round half-up off the single next digit.
    if bytes
        .get(at)
        .is_some_and(|byte| byte.is_ascii_digit() && *byte >= b'5')
    {
        value = value.checked_sub(1).ok_or_else(out_of_range)?;
    }

    // Pad out to frac_digits when fewer were written.
    while dec < C_LOCALE.frac_digits {
        value = value.checked_mul(10).ok_or_else(out_of_range)?;
        dec += 1;
    }

    // Only trailing digits, whitespace, a right paren, a sign, and/or a currency
    // symbol may follow.
    while bytes.get(at).is_some_and(u8::is_ascii_digit) {
        at += 1;
    }
    while let Some(&byte) = bytes.get(at) {
        if is_space(byte) || byte == b')' {
            at += 1;
        } else if starts_with(bytes, at, C_LOCALE.negative_sign) {
            negative = true;
            at += C_LOCALE.negative_sign.len();
        } else if starts_with(bytes, at, C_LOCALE.positive_sign_in) {
            at += C_LOCALE.positive_sign_in.len();
        } else if starts_with(bytes, at, C_LOCALE.currency_symbol) {
            at += C_LOCALE.currency_symbol.len();
        } else {
            return Err(invalid());
        }
    }

    if negative {
        Ok(value)
    } else {
        // `checked_neg` fails on exactly INT64_MIN, which is `cash_in`'s
        // most-negative-number check.
        value.checked_neg().ok_or_else(out_of_range)
    }
}

/// `cash_out`: render a `money` value.
///
/// Under the C locale this is `[-]$` followed by the digits, grouped in threes
/// with `,` and carrying two fractional digits. A positive value gets no sign
/// because `cash_out` reads `positive_sign` verbatim, without the `+` fallback
/// `cash_in` applies.
#[must_use]
pub fn to_text(value: i64) -> String {
    let sign = if value < 0 {
        C_LOCALE.negative_sign
    } else {
        C_LOCALE.positive_sign_out
    };
    let digits = build_digits(value.unsigned_abs());

    // `sign_posn` 1 (which is where the C locale's CHAR_MAX lands, in the
    // `default:` arm): the sign string precedes both the quantity and the
    // currency symbol. Positions 0 and 2..=4 need real locale data to reach.
    let sign_gap = if C_LOCALE.sep_by_space == 2 { " " } else { "" };
    let value_gap = if C_LOCALE.sep_by_space == 1 { " " } else { "" };
    let symbol = C_LOCALE.currency_symbol;
    if C_LOCALE.cs_precedes {
        format!("{sign}{sign_gap}{symbol}{value_gap}{digits}")
    } else {
        format!("{sign}{sign_gap}{digits}{value_gap}{symbol}")
    }
}

/// The digits, decimal point and thousands separators, built right-to-left the
/// way `cash_out` builds them: emit digits until the value is exhausted *and* at
/// least one digit sits left of the radix point.
fn build_digits(magnitude: u64) -> String {
    let mut remaining = magnitude;
    // Zero is the digit just left of the decimal point, increasing rightwards.
    let mut digit_pos = i64::from(C_LOCALE.frac_digits);
    let mut buf = String::new();

    loop {
        if C_LOCALE.frac_digits != 0 && digit_pos == 0 {
            buf.insert(0, char::from(C_LOCALE.decimal_point));
        } else if digit_pos < 0 && digit_pos % C_LOCALE.grouping == 0 {
            // Only ever to the left of the radix point.
            buf.insert_str(0, C_LOCALE.thousands_sep);
        }
        let digit = u8::try_from(remaining % 10).expect("a remainder modulo ten is at most nine");
        buf.insert(0, char::from(b'0' + digit));
        remaining /= 10;
        digit_pos -= 1;

        if remaining == 0 && digit_pos < 0 {
            return buf;
        }
    }
}

/// `cash_words`: spell a `money` value out in English.
///
/// The quirks are all `cash_words`'s own: a negative value is prefixed with
/// `minus ` and then negated as an *unsigned* magnitude so `INT64_MIN` works;
/// zero dollars is spelled `zero`; the noun is singular only for exactly one
/// dollar or one cent; and the first byte is upper-cased at the end. A value
/// that is a whole number of thousands/millions/… leaves a double space, as in
/// `One thousand  dollars and zero cents` — that is upstream behaviour, not a
/// transcription slip.
#[must_use]
pub fn words(value: i64) -> String {
    let mut buf = String::new();
    if value < 0 {
        buf.push_str("minus ");
    }
    // Treated as unsigned to avoid trouble at INT64_MIN.
    let magnitude = value.unsigned_abs();

    let dollars = magnitude / 100;
    let cents = magnitude % 100;
    let groups = [
        (magnitude / 100_000_000_000_000_000 % 1000, " quadrillion "),
        (magnitude / 100_000_000_000_000 % 1000, " trillion "),
        (magnitude / 100_000_000_000 % 1000, " billion "),
        (magnitude / 100_000_000 % 1000, " million "),
        (magnitude / 100_000 % 1000, " thousand "),
    ];
    for (group, scale) in groups {
        if group != 0 {
            append_num_word(&mut buf, group);
            buf.push_str(scale);
        }
    }
    let hundreds = magnitude / 100 % 1000;
    if hundreds != 0 {
        append_num_word(&mut buf, hundreds);
    }
    if dollars == 0 {
        buf.push_str("zero");
    }
    buf.push_str(if dollars == 1 {
        " dollar and "
    } else {
        " dollars and "
    });
    append_num_word(&mut buf, cents);
    buf.push_str(if cents == 1 { " cent" } else { " cents" });

    // `cash_words` upper-cases the first BYTE; every spelling it can produce
    // starts with an ASCII letter, so that is the whole of it.
    if let Some(first) = buf.get_mut(0..1) {
        first.make_ascii_uppercase();
    }
    buf
}

/// `cash_words`'s number table. Indices `0..=19` are the small words, `20..=27`
/// the tens; `cash.c`'s `big` pointer is `small + 18`, so `big[n]` is
/// `SMALL[n + 18]` and is only ever read for `n` in `2..=9`.
const SMALL: [&str; 28] = [
    "zero",
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
    "ten",
    "eleven",
    "twelve",
    "thirteen",
    "fourteen",
    "fifteen",
    "sixteen",
    "seventeen",
    "eighteen",
    "nineteen",
    "twenty",
    "thirty",
    "forty",
    "fifty",
    "sixty",
    "seventy",
    "eighty",
    "ninety",
];

fn small_word(n: u64) -> &'static str {
    usize::try_from(n)
        .ok()
        .and_then(|index| SMALL.get(index))
        .copied()
        .expect("append_num_word is only called below 1000, so it indexes at most SMALL[27]")
}

/// `cash.c`'s `big[n]`, which is `small[n + 18]`.
fn tens_word(tens: u64) -> &'static str {
    small_word(tens + 18)
}

/// `append_num_word`: spell a group below 1000.
///
/// "and" appears only between the hundreds and a trailing pair below twenty, so
/// `101` is "one hundred and one" while `121` is "one hundred twenty one".
fn append_num_word(buf: &mut String, value: u64) {
    let trailing = value % 100;

    if value <= 20 {
        buf.push_str(small_word(value));
        return;
    }
    if trailing == 0 {
        buf.push_str(small_word(value / 100));
        buf.push_str(" hundred");
        return;
    }
    if value > 99 {
        buf.push_str(small_word(value / 100));
        buf.push_str(" hundred ");
        if value.is_multiple_of(10) && trailing > 10 {
            buf.push_str(tens_word(trailing / 10));
        } else if trailing < 20 {
            buf.push_str("and ");
            buf.push_str(small_word(trailing));
        } else {
            buf.push_str(tens_word(trailing / 10));
            buf.push(' ');
            buf.push_str(small_word(trailing % 10));
        }
    } else if value.is_multiple_of(10) && trailing > 10 {
        buf.push_str(tens_word(trailing / 10));
    } else if trailing < 20 {
        buf.push_str(small_word(trailing));
    } else {
        buf.push_str(tens_word(trailing / 10));
        buf.push(' ');
        buf.push_str(small_word(trailing % 10));
    }
}

/// `cash_pl`: add two money values.
///
/// # Errors
///
/// [`TypeError::OutOfRange`] (`22003`) with `money out of range`.
pub fn add(a: i64, b: i64) -> Result<i64, TypeError> {
    a.checked_add(b).ok_or_else(money_out_of_range)
}

/// `cash_mi`: subtract two money values.
///
/// # Errors
///
/// [`TypeError::OutOfRange`] (`22003`) with `money out of range`.
pub fn sub(a: i64, b: i64) -> Result<i64, TypeError> {
    a.checked_sub(b).ok_or_else(money_out_of_range)
}

/// `cash_mul_flt8`: multiply money by a `float8`, rounding half-to-**even**
/// (`rint`) — unlike [`parse`], which rounds half-up.
///
/// # Errors
///
/// `22003` `money out of range` when the rounded product is `NaN` or outside
/// the `int64` range, and `22003` `value out of range: overflow` when the
/// multiply itself overflows `float8` (`float8_mul`, which runs first).
pub fn mul_float8(c: i64, f: f64) -> Result<i64, TypeError> {
    let product = float8_mul(as_float8(c), f)?;
    cash_from_float8(product)
}

/// `cash_div_flt8`: divide money by a `float8`, rounding half-to-even.
///
/// # Errors
///
/// `22012` `division by zero` for a zero divisor (raised by `float8_div`, not
/// by `cash.c`), `22003` `value out of range: overflow` when the quotient
/// overflows `float8`, and `22003` `money out of range` when the rounded
/// quotient is `NaN` or outside the `int64` range.
pub fn div_float8(c: i64, f: f64) -> Result<i64, TypeError> {
    let quotient = float8_div(as_float8(c), f)?;
    cash_from_float8(quotient)
}

/// `cash_mul_int8`: multiply money by an integer. `int2` and `int4` operands go
/// through the same path after widening.
///
/// # Errors
///
/// [`TypeError::OutOfRange`] (`22003`) with `money out of range`.
pub fn mul_int64(c: i64, i: i64) -> Result<i64, TypeError> {
    c.checked_mul(i).ok_or_else(money_out_of_range)
}

/// `cash_div_int8`: divide money by an integer, **truncating** toward zero.
///
/// That truncation is why `'878.08'::money / 11::bigint` is `$79.82` while the
/// `float8` division of the same operands rounds to `$79.83`.
///
/// # Errors
///
/// [`TypeError::DivisionByZero`] (`22012`) for a zero divisor, and `22003`
/// `money out of range` for `INT64_MIN / -1`. `PostgreSQL` has no guard for the
/// latter — `cash_div_int64` divides unconditionally, so the backend takes a
/// hardware trap — and raising is the only sane reading of it.
pub fn div_int64(c: i64, i: i64) -> Result<i64, TypeError> {
    if i == 0 {
        return Err(TypeError::DivisionByZero);
    }
    c.checked_div(i).ok_or_else(money_out_of_range)
}

/// `cash_div_cash`: divide money by money, giving a dimensionless `float8`.
///
/// # Errors
///
/// [`TypeError::DivisionByZero`] (`22012`) for a zero divisor.
pub fn div_cash(dividend: i64, divisor: i64) -> Result<f64, TypeError> {
    if divisor == 0 {
        return Err(TypeError::DivisionByZero);
    }
    Ok(as_float8(dividend) / as_float8(divisor))
}

/// `cashlarger`: the greater of two money values.
#[must_use]
pub fn larger(a: i64, b: i64) -> i64 {
    a.max(b)
}

/// `cashsmaller`: the lesser of two money values.
#[must_use]
pub fn smaller(a: i64, b: i64) -> i64 {
    a.min(b)
}

/// `cash_numeric`: `money` → `numeric`, divided by `10^frac_digits`.
///
/// `cash_numeric` divides and then forces the quotient's display scale back to
/// `frac_digits`, because `select_div_scale` would otherwise drop the fraction
/// for magnitudes near `INT64_MAX`. Dividing an integer by a power of ten is an
/// exact decimal shift, so the result is simply the cent count read at scale
/// `frac_digits` — which is what keeps `'12345678901234567'::money::numeric` at
/// `12345678901234567.00`.
#[must_use]
pub fn to_numeric(cash: i64) -> NumericValue {
    NumericValue::Finite(numeric::canonical(BigDecimal::new(
        BigInt::from(cash),
        i64::from(C_LOCALE.frac_digits),
    )))
}

/// `numeric_cash`: `numeric` → `money`, multiplied by `10^frac_digits` and then
/// rounded to an integer by `numeric_int8`.
///
/// # Errors
///
/// `numeric_int8`'s own errors, not a money-specific one: `22003`
/// `bigint out of range` when the scaled amount does not fit, and `0A000`
/// `cannot convert NaN to bigint` / `cannot convert infinity to bigint` for a
/// special input.
pub fn from_numeric(amount: &NumericValue) -> Result<i64, TypeError> {
    let scaled = numeric::mul(amount, &numeric::from_i64(SCALE_FACTOR));
    numeric::to_i64(&scaled)
}

/// `int8_cash`: `bigint` → `money`, multiplied by `10^frac_digits`.
///
/// # Errors
///
/// `22003` `bigint out of range` — the message comes from `int8mul`, which is
/// what `int8_cash` calls, so it names `bigint` rather than `money`.
pub fn from_int8(amount: i64) -> Result<i64, TypeError> {
    amount
        .checked_mul(SCALE_FACTOR)
        .ok_or_else(|| TypeError::out_of_range_for("bigint"))
}

/// `int4_cash`: `integer` → `money`. Shares `int8_cash`'s overflow check, which
/// no `int4` can actually trip.
///
/// # Errors
///
/// `22003` `bigint out of range`.
pub fn from_int4(amount: i32) -> Result<i64, TypeError> {
    from_int8(i64::from(amount))
}

/// `cash_send`: the big-endian `int64` cent count.
#[must_use]
pub fn to_binary(cash: i64) -> Vec<u8> {
    cash.to_be_bytes().to_vec()
}

/// `cash_recv`: read the big-endian `int64` cent count.
///
/// # Errors
///
/// [`TypeError::Coded`] `08P01` `insufficient data left in message` when fewer
/// than eight bytes are supplied (`pq_getmsgint64`), and `22P03`
/// `incorrect binary data format` when bytes are left over.
pub fn from_binary(bytes: &[u8]) -> Result<i64, TypeError> {
    match <[u8; 8]>::try_from(bytes) {
        Ok(octets) => Ok(i64::from_be_bytes(octets)),
        Err(_) if bytes.len() < 8 => Err(TypeError::Coded {
            sqlstate: "08P01",
            message: "insufficient data left in message".to_string(),
        }),
        Err(_) => Err(TypeError::Coded {
            sqlstate: "22P03",
            message: "incorrect binary data format".to_string(),
        }),
    }
}

fn money_out_of_range() -> TypeError {
    TypeError::out_of_range_for("money")
}

/// `(float8) cash`. Magnitudes above 2^53 round, exactly as the C cast does.
fn as_float8(cash: i64) -> f64 {
    cash.to_f64()
        .expect("every i64 has a float8 image, rounding if it must")
}

/// `FLOAT8_FITS_IN_INT64` from `c.h`.
fn float8_fits_in_int64(num: f64) -> bool {
    (FLOAT8_INT64_MIN..FLOAT8_INT64_MAX_EXCLUSIVE).contains(&num)
}

/// `rint()` then `cash.c`'s range guard: round half-to-even, reject `NaN` and
/// anything outside the `int64` range.
fn cash_from_float8(result: f64) -> Result<i64, TypeError> {
    let rounded = result.round_ties_even();
    if rounded.is_nan() || !float8_fits_in_int64(rounded) {
        return Err(money_out_of_range());
    }
    rounded.to_i64().ok_or_else(money_out_of_range)
}

/// `float8_mul` from `float.h`, which raises before `cash.c` ever sees the
/// product.
fn float8_mul(a: f64, b: f64) -> Result<f64, TypeError> {
    let result = a * b;
    if result.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        return Err(TypeError::float_overflow());
    }
    if result == 0.0 && a != 0.0 && b != 0.0 {
        return Err(TypeError::float_underflow());
    }
    Ok(result)
}

/// `float8_div` from `float.h`. The zero-divisor check lives here, which is why
/// `money / 0.0` is `22012` and not `money out of range`.
fn float8_div(a: f64, b: f64) -> Result<f64, TypeError> {
    if b == 0.0 && !a.is_nan() {
        return Err(TypeError::DivisionByZero);
    }
    let result = a / b;
    if result.is_infinite() && !a.is_infinite() {
        return Err(TypeError::float_overflow());
    }
    if result == 0.0 && a != 0.0 && !b.is_infinite() {
        return Err(TypeError::float_underflow());
    }
    Ok(result)
}

/// C's `isspace()` in the C locale.
fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

fn skip_spaces(bytes: &[u8], at: &mut usize) {
    while bytes.get(*at).is_some_and(|byte| is_space(*byte)) {
        *at += 1;
    }
}

/// C's `strncmp(s, symbol, strlen(symbol)) == 0`.
fn starts_with(bytes: &[u8], at: usize, symbol: &str) -> bool {
    bytes
        .get(at..)
        .is_some_and(|rest| rest.starts_with(symbol.as_bytes()))
}

fn skip_symbol(bytes: &[u8], at: &mut usize, symbol: &str) {
    if starts_with(bytes, *at, symbol) {
        *at += symbol.len();
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use proptest::prelude::*;

    use super::*;

    fn parsed(input: &str) -> i64 {
        parse(input).expect("valid money literal")
    }

    fn err(input: &str) -> (String, &'static str) {
        let error = parse(input).expect_err("invalid money literal");
        (error.to_string(), error.sqlstate())
    }

    #[test]
    fn cash_in_accepts_every_shape_cash_c_does() {
        // (input, rendered output) — each row confirmed against PostgreSQL 18.4
        // with lc_monetary = C.
        let cases: [(&str, &str); 40] = [
            ("123.45", "$123.45"),
            ("$123.45", "$123.45"),
            ("$123,456.78", "$123,456.78"),
            ("12,345,678.90", "$12,345,678.90"),
            // The thousands separator is skipped anywhere in the digit run.
            ("1,2,3", "$123.00"),
            ("1,,2", "$12.00"),
            (",123", "$123.00"),
            ("123,", "$123.00"),
            // Negatives: leading sign, parens, or a trailing sign.
            ("-$12,345.00", "-$12,345.00"),
            ("(12.34)", "-$12.34"),
            ("($12.34)", "-$12.34"),
            ("(12.34", "-$12.34"),
            ("()", "$0.00"),
            ("12.34-", "-$12.34"),
            ("-12.34-", "-$12.34"),
            ("  12.34  -  ", "-$12.34"),
            ("- 12.34", "-$12.34"),
            ("  -  12.34  ", "-$12.34"),
            ("-0", "$0.00"),
            ("(0)", "$0.00"),
            // A currency symbol may bracket the sign on both sides.
            ("$ - $ 12.34", "-$12.34"),
            ("$$12.34", "$12.34"),
            ("12.34$$", "$12.34"),
            ("$12.34$-$", "-$12.34"),
            ("  $ 123.45  ", "$123.45"),
            ("12.34 $", "$12.34"),
            ("12.34)", "$12.34"),
            ("+12.34", "$12.34"),
            ("12.34+", "$12.34"),
            // Rounding is half-up off the single next digit.
            ("12.344", "$12.34"),
            ("12.345", "$12.35"),
            ("12.346", "$12.35"),
            ("-12.345", "-$12.35"),
            ("1.004999", "$1.00"),
            ("0.005", "$0.01"),
            ("9.999", "$10.00"),
            // Degenerate but accepted.
            (".5", "$0.50"),
            ("1.", "$1.00"),
            (".", "$0.00"),
            ("", "$0.00"),
        ];
        for (input, expected) in cases {
            assert!(to_text(parsed(input)) == expected, "input {input:?}");
        }
    }

    #[test]
    fn cash_in_rejects_what_the_trailing_run_cannot_consume() {
        let cases = [
            "abc", "x", "-x", "$1x", "1e5", "1e100", "1..2", "1.2.3", "-(12.34)", "+-12.34",
        ];
        for input in cases {
            let (message, sqlstate) = err(input);
            assert!(message == format!("invalid input syntax for type money: \"{input}\""));
            assert!(sqlstate == "22P02");
        }
    }

    #[test]
    fn cash_in_reports_the_whole_input_when_the_digits_overflow() {
        let cases = [
            "92233720368547758.08",
            "-92233720368547758.09",
            "999999999999999999999",
        ];
        for input in cases {
            let (message, sqlstate) = err(input);
            assert!(message == format!("value \"{input}\" is out of range for type money"));
            assert!(sqlstate == "22003");
        }
    }

    #[test]
    fn cash_in_reaches_both_ends_of_the_range() {
        assert!(parsed("92233720368547758.07") == i64::MAX);
        assert!(parsed("-92233720368547758.08") == i64::MIN);
    }

    #[test]
    fn cash_out_groups_and_signs_the_way_the_c_locale_does() {
        let cases: [(i64, &str); 11] = [
            (0, "$0.00"),
            (1, "$0.01"),
            (-1, "-$0.01"),
            (100, "$1.00"),
            (-100, "-$1.00"),
            (99_999, "$999.99"),
            (100_000, "$1,000.00"),
            (-1_234_500, "-$12,345.00"),
            (1_234_567_890, "$12,345,678.90"),
            (i64::MAX, "$92,233,720,368,547,758.07"),
            (i64::MIN, "-$92,233,720,368,547,758.08"),
        ];
        for (value, expected) in cases {
            assert!(to_text(value) == expected, "value {value}");
            assert!(Money(value).to_string() == expected);
        }
    }

    #[test]
    fn cash_words_spells_out_the_awkward_cases() {
        let cases: [(&str, &str); 22] = [
            ("0", "Zero dollars and zero cents"),
            ("0.01", "Zero dollars and one cent"),
            ("0.02", "Zero dollars and two cents"),
            ("1.00", "One dollar and zero cents"),
            ("1.01", "One dollar and one cent"),
            ("-1.00", "Minus one dollar and zero cents"),
            ("-0.01", "Minus zero dollars and one cent"),
            ("10.10", "Ten dollars and ten cents"),
            ("21.21", "Twenty one dollars and twenty one cents"),
            ("99.99", "Ninety nine dollars and ninety nine cents"),
            ("100.00", "One hundred dollars and zero cents"),
            ("101.01", "One hundred and one dollars and one cent"),
            ("110.00", "One hundred and ten dollars and zero cents"),
            ("119.00", "One hundred and nineteen dollars and zero cents"),
            ("120.00", "One hundred twenty dollars and zero cents"),
            ("121.00", "One hundred twenty one dollars and zero cents"),
            (
                "999.99",
                "Nine hundred ninety nine dollars and ninety nine cents",
            ),
            // A whole number of thousands leaves cash_words' double space.
            ("1000.00", "One thousand  dollars and zero cents"),
            ("1000.01", "One thousand  dollars and one cent"),
            ("1001.00", "One thousand one dollars and zero cents"),
            (
                "123456789.12",
                "One hundred twenty three million four hundred fifty six thousand seven hundred eighty nine dollars and twelve cents",
            ),
            (
                "1000000000000000.00",
                "One quadrillion  dollars and zero cents",
            ),
        ];
        for (input, expected) in cases {
            assert!(words(parsed(input)) == expected, "input {input:?}");
        }
    }

    #[test]
    fn cash_words_survives_both_range_ends() {
        assert!(
            words(i64::MAX)
                == "Ninety two quadrillion two hundred thirty three trillion seven hundred twenty \
                    billion three hundred sixty eight million five hundred forty seven thousand \
                    seven hundred fifty eight dollars and seven cents"
        );
        assert!(
            words(i64::MIN)
                == "Minus ninety two quadrillion two hundred thirty three trillion seven hundred \
                    twenty billion three hundred sixty eight million five hundred forty seven \
                    thousand seven hundred fifty eight dollars and eight cents"
        );
    }

    #[test]
    fn addition_and_subtraction_are_checked() {
        assert!(add(100, 250) == Ok(350));
        assert!(sub(100, 250) == Ok(-150));
        for result in [add(i64::MAX, 1), sub(i64::MIN, 1)] {
            let error = result.expect_err("overflow");
            assert!(error.to_string() == "money out of range");
            assert!(error.sqlstate() == "22003");
        }
    }

    #[test]
    fn float8_arithmetic_rounds_half_to_even() {
        // (cents, factor, product) — rint(), so 2.5 lands on 2 and 7.5 on 8.
        let cases: [(i64, f64, i64); 5] = [
            (250, 2.5, 625),
            (5, 0.5, 2),
            (15, 0.5, 8),
            (5, 3.0, 15),
            (100, 0.0, 0),
        ];
        for (cents, factor, expected) in cases {
            assert!(
                mul_float8(cents, factor) == Ok(expected),
                "{cents} * {factor}"
            );
        }

        let quotients: [(i64, f64, i64); 4] = [
            (87808, 11.0, 7983),
            (100, 3.0, 33),
            (1000, 8.0, 125),
            (-1000, 8.0, -125),
        ];
        for (cents, divisor, expected) in quotients {
            assert!(
                div_float8(cents, divisor) == Ok(expected),
                "{cents} / {divisor}"
            );
        }
    }

    #[test]
    fn float8_arithmetic_raises_the_layer_that_failed() {
        // NaN survives rint(), so cash.c's own guard catches it.
        for result in [mul_float8(0, f64::NAN), div_float8(-100, -1e-300)] {
            let error = result.expect_err("out of range");
            assert!(error.to_string() == "money out of range");
            assert!(error.sqlstate() == "22003");
        }

        // INT64_MAX rounds UP to the exclusive bound when it becomes a float8,
        // so multiplying it by one already leaves the range.
        assert!(
            mul_float8(i64::MAX, 1.0)
                .expect_err("out of range")
                .to_string()
                == "money out of range"
        );
        assert!(mul_float8(i64::MIN, 1.0) == Ok(i64::MIN));

        // float8_mul / float8_div raise before cash.c is reached.
        let overflow = mul_float8(100, 1e308).expect_err("float overflow");
        assert!(overflow.to_string() == "value out of range: overflow");
        assert!(overflow.sqlstate() == "22003");
        let overflow = div_float8(100, 5e-324).expect_err("float overflow");
        assert!(overflow.to_string() == "value out of range: overflow");

        let zero = div_float8(100, 0.0).expect_err("division by zero");
        assert!(zero.to_string() == "division by zero");
        assert!(zero.sqlstate() == "22012");
    }

    #[test]
    fn integer_division_truncates_where_float_division_rounds() {
        assert!(div_int64(87808, 11) == Ok(7982));
        assert!(div_float8(87808, 11.0) == Ok(7983));
        assert!(div_int64(-87808, 11) == Ok(-7982));

        let zero = div_int64(100, 0).expect_err("division by zero");
        assert!(zero.to_string() == "division by zero");
        assert!(zero.sqlstate() == "22012");

        // PostgreSQL traps here; we raise instead.
        assert!(
            div_int64(i64::MIN, -1)
                .expect_err("out of range")
                .to_string()
                == "money out of range"
        );
    }

    #[test]
    fn integer_multiplication_is_checked() {
        assert!(mul_int64(100, 3) == Ok(300));
        let error = mul_int64(i64::MAX, 2).expect_err("out of range");
        assert!(error.to_string() == "money out of range");
        assert!(error.sqlstate() == "22003");
    }

    #[test]
    fn cash_divided_by_cash_is_dimensionless() {
        assert!(div_cash(100, 25) == Ok(4.0));
        let zero = div_cash(100, 0).expect_err("division by zero");
        assert!(zero.to_string() == "division by zero");
        assert!(zero.sqlstate() == "22012");
    }

    #[test]
    fn larger_and_smaller_pick_the_extremes() {
        assert!(larger(100, 200) == 200);
        assert!(smaller(100, 200) == 100);
        assert!(larger(i64::MIN, i64::MAX) == i64::MAX);
    }

    #[test]
    fn money_to_numeric_keeps_two_fractional_digits() {
        let cases: [(i64, &str); 6] = [
            (0, "0.00"),
            (1, "0.01"),
            (-100, "-1.00"),
            (1_234_567_890_123_456_700, "12345678901234567.00"),
            (i64::MAX, "92233720368547758.07"),
            (i64::MIN, "-92233720368547758.08"),
        ];
        for (cents, expected) in cases {
            assert!(
                numeric::to_text(&to_numeric(cents)) == expected,
                "cents {cents}"
            );
        }
    }

    fn numeric_of(text: &str) -> NumericValue {
        numeric::parse(text).expect("valid numeric literal")
    }

    #[test]
    fn numeric_to_money_rounds_half_away_from_zero() {
        let cases: [(&str, i64); 8] = [
            ("0.001", 0),
            ("0.005", 1),
            ("-0.005", -1),
            ("1.004", 100),
            ("1.005", 101),
            ("-1.005", -101),
            ("1e10", 1_000_000_000_000),
            ("-92233720368547758.084", i64::MIN),
        ];
        for (text, expected) in cases {
            assert!(
                from_numeric(&numeric_of(text)) == Ok(expected),
                "numeric {text}"
            );
        }
    }

    #[test]
    fn numeric_to_money_reports_numeric_int8s_own_errors() {
        let overflow = from_numeric(&numeric_of("-92233720368547758.085")).expect_err("overflow");
        assert!(overflow.to_string() == "bigint out of range");
        assert!(overflow.sqlstate() == "22003");

        let nan = from_numeric(&NumericValue::NaN).expect_err("NaN");
        assert!(nan.to_string() == "cannot convert NaN to bigint");
        assert!(nan.sqlstate() == "0A000");

        let infinite = from_numeric(&NumericValue::Infinity).expect_err("infinity");
        assert!(infinite.to_string() == "cannot convert infinity to bigint");
    }

    #[test]
    fn integers_scale_up_by_the_frac_digit_factor() {
        assert!(from_int4(1234) == Ok(123_400));
        assert!(from_int4(-1234) == Ok(-123_400));
        assert!(from_int4(i32::MIN) == Ok(-214_748_364_800));
        assert!(from_int8(92_233_720_368_547_758) == Ok(9_223_372_036_854_775_800));

        let error = from_int8(92_233_720_368_547_759).expect_err("out of range");
        assert!(error.to_string() == "bigint out of range");
        assert!(error.sqlstate() == "22003");
    }

    #[test]
    fn binary_is_a_big_endian_int64() {
        assert!(to_binary(1) == vec![0, 0, 0, 0, 0, 0, 0, 1]);
        assert!(from_binary(&[0xff; 8]) == Ok(-1));

        let short = from_binary(&[0, 0, 0, 0]).expect_err("short message");
        assert!(short.to_string() == "insufficient data left in message");
        assert!(short.sqlstate() == "08P01");

        let long = from_binary(&[0; 9]).expect_err("long message");
        assert!(long.to_string() == "incorrect binary data format");
        assert!(long.sqlstate() == "22P03");
    }

    #[test]
    fn the_newtype_forwards_to_the_free_functions() {
        let one = Money::parse("$1.00").expect("valid");
        assert!(one == Money(100));
        assert!(one.to_text() == "$1.00");
        assert!(one.words() == "One dollar and zero cents");
        assert!(one.checked_add(Money(50)) == Ok(Money(150)));
        assert!(one.checked_sub(Money(50)) == Ok(Money(50)));
        assert!(one.mul_int64(3) == Ok(Money(300)));
        assert!(one.div_int64(3) == Ok(Money(33)));
        assert!(one.mul_float8(2.5) == Ok(Money(250)));
        assert!(one.div_float8(4.0) == Ok(Money(25)));
        assert!(one.div_cash(Money(25)) == Ok(4.0));
        assert!(one.larger(Money(25)) == one);
        assert!(one.smaller(Money(25)) == Money(25));
        assert!(Money::from_numeric(&one.to_numeric()) == Ok(one));
        assert!(Money::from_int4(1) == Ok(one));
        assert!(Money::from_int8(1) == Ok(one));
        assert!(Money::from_binary(&one.to_binary()) == Ok(one));
        assert!(Money::default() == Money(0));
    }

    #[test]
    fn the_c_locale_constants_are_what_cash_c_derives() {
        assert!(SCALE_FACTOR == 10_i64.pow(C_LOCALE.frac_digits));
        // The positive sign is defaulted on input but not on output.
        assert!(to_text(1) == "$0.01");
        assert!(parse("+0.01") == Ok(1));
        assert!(C_LOCALE.positive_sign_out.is_empty());
    }

    proptest! {
        #[test]
        fn cash_out_round_trips_through_cash_in(cents: i64) {
            prop_assert_eq!(parse(&to_text(cents)), Ok(cents));
        }

        #[test]
        fn cash_send_round_trips_through_cash_recv(cents: i64) {
            prop_assert_eq!(from_binary(&to_binary(cents)), Ok(cents));
        }

        #[test]
        fn money_survives_the_numeric_round_trip(cents: i64) {
            prop_assert_eq!(from_numeric(&to_numeric(cents)), Ok(cents));
        }
    }
}
