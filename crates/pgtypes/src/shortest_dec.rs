//! `PostgreSQL`'s shortest round-tripping float output — the port of
//! `src/common/f2s.c` and `src/common/d2s.c`.
//!
//! `float4out` and `float8out` print the shortest decimal that reads back as
//! the same float whenever `extra_float_digits > 0`, which has been the default
//! since `PostgreSQL` 12. They take that spelling from Ryu, but **not** from
//! stock Ryu: `src/common/ryu_common.h` sets `STRICTLY_SHORTEST` to 0, so
//! `PostgreSQL` never prints the exact midpoint between two neighbouring
//! floats. Its comment gives the reason — a midpoint reads back correctly only
//! if the reader applies round-half-to-even, and readers often get that wrong.
//!
//! That one flag is why Rust's own `{}` is not a substitute, and why the `ryu`
//! crate is not one either. Both take the midpoint when it is the shorter
//! spelling. `9e+09` and `8.999999e+09` are the same `float4`, and the midpoint
//! rule is the whole difference between them. The second difference is the
//! tie: when the value sits exactly on a decimal half, Ryu rounds the last
//! digit to even, so `305404.125` prints as `305404.12`, not `305404.13`.
//!
//! With `STRICTLY_SHORTEST` at 0, upstream Ryu's `acceptBounds` flag is a
//! compile-time `false`. The branches it guards are dropped here rather than
//! carried as dead code. That also collapses the trailing-zero bookkeeping for
//! the lower bound — it can never become true — and merges Ryu's two
//! digit-removal loops into one.
//!
//! # Key Functions
//!
//! - [`float4_shortest`] — `float_to_shortest_decimal_buf`, for `real`.
//! - [`float8_shortest`] — `double_to_shortest_decimal_buf`, for
//!   `double precision`.
//!
//! Both spell the IEEE specials the way `ryu_common.h`'s `copy_special_str`
//! does: `NaN`, `Infinity`, `-Infinity`, `0` and `-0`.

/// Mantissa bits in an IEEE 754 binary32.
const FLOAT_MANTISSA_BITS: i32 = 23;
/// Exponent bits in an IEEE 754 binary32.
const FLOAT_EXPONENT_BITS: i32 = 8;
/// Exponent bias of an IEEE 754 binary32.
const FLOAT_BIAS: i32 = 127;
/// Mantissa bits in an IEEE 754 binary64.
const DOUBLE_MANTISSA_BITS: i32 = 52;
/// Exponent bits in an IEEE 754 binary64.
const DOUBLE_EXPONENT_BITS: i32 = 11;
/// Exponent bias of an IEEE 754 binary64.
const DOUBLE_BIAS: i32 = 1023;

/// Precision of a [`FLOAT_POW5_INV_SPLIT`] entry, in bits.
const FLOAT_POW5_INV_BITCOUNT: i32 = 59;
/// Precision of a [`FLOAT_POW5_SPLIT`] entry, in bits.
const FLOAT_POW5_BITCOUNT: i32 = 61;
/// Precision of a [`DOUBLE_POW5_INV_SPLIT`] entry, in bits.
const DOUBLE_POW5_INV_BITCOUNT: i32 = 122;
/// Precision of a [`DOUBLE_POW5_SPLIT`] entry, in bits.
const DOUBLE_POW5_BITCOUNT: i32 = 121;

/// The display exponent at which `float4out` leaves fixed point for scientific
/// notation, chosen in `f2s.c` to match `printf`'s `%g` default at `FLT_DIG`.
const FLOAT_FIXED_UPPER: i32 = 6;
/// [`FLOAT_FIXED_UPPER`]'s `float8out` counterpart, at `DBL_DIG`.
const DOUBLE_FIXED_UPPER: i32 = 15;

/// A decimal `mantissa * 10^exponent`: Ryu's `floating_decimal_32`/`_64`.
struct Decimal {
    /// The significand, with no leading zero and — outside the small-integer
    /// fast path — no trailing zero either.
    mantissa: u64,
    /// The power of ten [`Decimal::mantissa`] is scaled by.
    exponent: i32,
}

/// `ceil(log_2(5^e))`, or 1 at `e == 0`.
fn pow5bits(e: i32) -> i32 {
    let e = u32::try_from(e).expect("Ryu only takes pow5bits of a non-negative exponent");
    i32::try_from((e * 1_217_359) >> 19).expect("the approximation stays far below i32::MAX") + 1
}

/// `floor(log_10(2^e))`.
fn log10_pow2(e: i32) -> i32 {
    let e = u32::try_from(e).expect("Ryu only takes log10_pow2 of a non-negative exponent");
    i32::try_from((e * 78913) >> 18).expect("the approximation stays far below i32::MAX")
}

/// `floor(log_10(5^e))`.
fn log10_pow5(e: i32) -> i32 {
    let e = u32::try_from(e).expect("Ryu only takes log10_pow5 of a non-negative exponent");
    i32::try_from((e * 732_923) >> 20).expect("the approximation stays far below i32::MAX")
}

/// How many times five divides `value`.
fn pow5_factor(mut value: u64) -> i32 {
    let mut count = 0;
    while value != 0 && value.is_multiple_of(5) {
        value /= 5;
        count += 1;
    }
    count
}

/// Whether `5^p` divides `value`.
fn multiple_of_power_of_5(value: u64, p: i32) -> bool {
    pow5_factor(value) >= p
}

/// Whether `2^p` divides `value`.
fn multiple_of_power_of_2(value: u64, p: i32) -> bool {
    let p = u32::try_from(p).expect("Ryu only tests a non-negative power of two");
    value.trailing_zeros() >= p
}

/// `f2s.c`'s `mulShift`: the top bits of `m * factor`, shifted right by
/// `shift`, which is always above 32.
fn mul_shift_32(m: u32, factor: u64, shift: i32) -> u32 {
    let bits0 = u64::from(m) * (factor & u64::from(u32::MAX));
    let bits1 = u64::from(m) * (factor >> 32);
    let sum = (bits0 >> 32) + bits1;
    let shift = u32::try_from(shift - 32).expect("f2s.c's shift is always above 32");
    u32::try_from(sum >> shift).expect("Ryu scales its tables so the result fits in 32 bits")
}

/// `d2s.c`'s `mulShift`: the top bits of the 64-by-128-bit product
/// `m * factor`, shifted right by `j`, which is always above 64.
fn mul_shift_64(m: u64, factor: [u64; 2], j: i32) -> u64 {
    let b0 = u128::from(m) * u128::from(factor[0]);
    let b2 = u128::from(m) * u128::from(factor[1]);
    let j = u32::try_from(j - 64).expect("d2s.c's shift is always above 64");
    let shifted = ((b0 >> 64) + b2) >> j;
    u64::try_from(shifted & u128::from(u64::MAX)).expect("the mask leaves 64 bits")
}

/// Lay `digits * 10^exponent` out the way `to_chars` does.
///
/// `fixed_upper` is the type's [`FLOAT_FIXED_UPPER`] equivalent: fixed point
/// while the display exponent is in `-4 .. fixed_upper`, scientific with a
/// signed, at-least-two-digit exponent outside it.
///
/// The one wrinkle is the small-integer fast path, whose mantissa keeps its
/// decimal trailing zeros. `to_chars` moves those into the exponent for
/// scientific notation, where they would read as precision the value does not
/// have, and leaves them alone in fixed point, where they are ordinary digits.
fn layout(digits: &str, exponent: i32, negative: bool, fixed_upper: i32) -> String {
    let olength = i32::try_from(digits.len()).expect("a float's mantissa has at most 17 digits");
    let display_exponent = exponent + olength - 1;
    let sign = if negative { "-" } else { "" };

    if (-4..fixed_upper).contains(&display_exponent) {
        let point = exponent + olength;
        if point <= 0 {
            let zeros = usize::try_from(-point).expect("point is not positive here");
            return format!("{sign}0.{}{digits}", "0".repeat(zeros));
        }
        if exponent < 0 {
            let point = usize::try_from(point).expect("point is positive here");
            return format!("{sign}{}.{}", &digits[..point], &digits[point..]);
        }
        let zeros = usize::try_from(exponent).expect("exponent is not negative here");
        return format!("{sign}{digits}{}", "0".repeat(zeros));
    }

    let digits = if exponent == 0 {
        digits.trim_end_matches('0')
    } else {
        digits
    };
    let (lead, rest) = digits.split_at(1);
    let fraction = if rest.is_empty() {
        String::new()
    } else {
        format!(".{rest}")
    };
    let exponent_sign = if display_exponent < 0 { '-' } else { '+' };
    format!(
        "{sign}{lead}{fraction}e{exponent_sign}{:02}",
        display_exponent.abs()
    )
}

/// Ryu's step 4, shared by both widths: drop digits while the interval
/// `(vm, vp)` still pins the value down, then round what survives.
///
/// The interval is open at both ends because `PostgreSQL` compiles Ryu with
/// `STRICTLY_SHORTEST` at 0. `vr_is_trailing_zeros` says whether every digit
/// dropped so far was a zero, which is what makes the half-to-even tie break
/// below reachable: only then does the value sit *exactly* on a decimal half.
///
/// `last_removed_digit` seeds the rounding digit for the case where the loop
/// below never runs, which `f2s.c` computes up front and `d2s.c` cannot reach.
///
/// Returns the surviving mantissa and how many digits went.
fn shorten(
    mut vr: u64,
    mut vp: u64,
    mut vm: u64,
    mut vr_is_trailing_zeros: bool,
    mut last_removed_digit: u64,
) -> (u64, i32) {
    let mut removed = 0;
    while vp / 10 > vm / 10 {
        vr_is_trailing_zeros &= last_removed_digit == 0;
        last_removed_digit = vr % 10;
        vr /= 10;
        vp /= 10;
        vm /= 10;
        removed += 1;
    }
    if vr_is_trailing_zeros && last_removed_digit == 5 && vr.is_multiple_of(2) {
        last_removed_digit = 4;
    }
    (vr + u64::from(vr == vm || last_removed_digit >= 5), removed)
}

/// `f2s.c`'s `f2d`: the shortest decimal for a `float4`'s IEEE fields.
fn f2d(ieee_mantissa: u32, ieee_exponent: i32) -> Decimal {
    let (e2, m2) = if ieee_exponent == 0 {
        (1 - FLOAT_BIAS - FLOAT_MANTISSA_BITS - 2, ieee_mantissa)
    } else {
        (
            ieee_exponent - FLOAT_BIAS - FLOAT_MANTISSA_BITS - 2,
            (1 << FLOAT_MANTISSA_BITS) | ieee_mantissa,
        )
    };

    // The half-ulp bounds of the value, scaled by four so that they stay
    // integral. `mm_shift` is the narrower step down at a power of two.
    let mv = 4 * m2;
    let mm_shift = u32::from(ieee_mantissa != 0 || ieee_exponent <= 1);
    let mp = mv + 2;
    let mm = mv - 1 - mm_shift;

    let mut vr_is_trailing_zeros = false;
    let mut last_removed_digit = 0;
    let (vr, mut vp, vm, e10);
    if e2 >= 0 {
        let q = log10_pow2(e2);
        e10 = q;
        let k = FLOAT_POW5_INV_BITCOUNT + pow5bits(q) - 1;
        let i = -e2 + q + k;
        let factor = FLOAT_POW5_INV_SPLIT[index(q)];
        vr = mul_shift_32(mv, factor, i);
        vp = mul_shift_32(mp, factor, i);
        vm = mul_shift_32(mm, factor, i);
        if q != 0 && (vp - 1) / 10 <= vm / 10 {
            // One digit past the shortest is still needed to round with, and
            // taking it at q - 1 keeps the arithmetic inside 32 bits.
            let l = FLOAT_POW5_INV_BITCOUNT + pow5bits(q - 1) - 1;
            let factor = FLOAT_POW5_INV_SPLIT[index(q - 1)];
            last_removed_digit = mul_shift_32(mv, factor, -e2 + q - 1 + l) % 10;
        }
        if q <= 9 {
            // At most one of mm, mv and mp is a multiple of five.
            if mv % 5 == 0 {
                vr_is_trailing_zeros = multiple_of_power_of_5(u64::from(mv), q);
            } else {
                vp -= u32::from(multiple_of_power_of_5(u64::from(mp), q));
            }
        }
    } else {
        let q = log10_pow5(-e2);
        e10 = q + e2;
        let i = -e2 - q;
        let j = q - (pow5bits(i) - FLOAT_POW5_BITCOUNT);
        let factor = FLOAT_POW5_SPLIT[index(i)];
        vr = mul_shift_32(mv, factor, j);
        vp = mul_shift_32(mp, factor, j);
        vm = mul_shift_32(mm, factor, j);
        if q != 0 && (vp - 1) / 10 <= vm / 10 {
            let j = q - 1 - (pow5bits(i + 1) - FLOAT_POW5_BITCOUNT);
            last_removed_digit = mul_shift_32(mv, FLOAT_POW5_SPLIT[index(i + 1)], j) % 10;
        }
        if q <= 1 {
            // mv is 4 * m2, so it always ends in two zero bits, and mp is
            // mv + 2, so it always ends in one.
            vr_is_trailing_zeros = true;
            vp -= 1;
        } else if q < 31 {
            vr_is_trailing_zeros = multiple_of_power_of_2(u64::from(mv), q - 1);
        }
    }

    let (mantissa, removed) = shorten(
        u64::from(vr),
        u64::from(vp),
        u64::from(vm),
        vr_is_trailing_zeros,
        u64::from(last_removed_digit),
    );
    Decimal {
        mantissa,
        exponent: e10 + removed,
    }
}

/// `d2s.c`'s `d2d`: the shortest decimal for a `float8`'s IEEE fields.
fn d2d(ieee_mantissa: u64, ieee_exponent: i32) -> Decimal {
    let (e2, m2) = if ieee_exponent == 0 {
        (1 - DOUBLE_BIAS - DOUBLE_MANTISSA_BITS - 2, ieee_mantissa)
    } else {
        (
            ieee_exponent - DOUBLE_BIAS - DOUBLE_MANTISSA_BITS - 2,
            (1 << DOUBLE_MANTISSA_BITS) | ieee_mantissa,
        )
    };

    let mv = 4 * m2;
    let mm_shift = u64::from(ieee_mantissa != 0 || ieee_exponent <= 1);
    let mp = mv + 2;
    let mm = mv - 1 - mm_shift;

    let mut vr_is_trailing_zeros = false;
    let (vr, mut vp, vm, e10);
    if e2 >= 0 {
        let q = log10_pow2(e2) - i32::from(e2 > 3);
        e10 = q;
        let k = DOUBLE_POW5_INV_BITCOUNT + pow5bits(q) - 1;
        let i = -e2 + q + k;
        let factor = DOUBLE_POW5_INV_SPLIT[index(q)];
        vr = mul_shift_64(mv, factor, i);
        vp = mul_shift_64(mp, factor, i);
        vm = mul_shift_64(mm, factor, i);
        if q <= 21 {
            // At most one of mm, mv and mp is a multiple of five.
            if mv % 5 == 0 {
                vr_is_trailing_zeros = multiple_of_power_of_5(mv, q);
            } else {
                vp -= u64::from(multiple_of_power_of_5(mp, q));
            }
        }
    } else {
        let q = log10_pow5(-e2) - i32::from(-e2 > 1);
        e10 = q + e2;
        let i = -e2 - q;
        let j = q - (pow5bits(i) - DOUBLE_POW5_BITCOUNT);
        let factor = DOUBLE_POW5_SPLIT[index(i)];
        vr = mul_shift_64(mv, factor, j);
        vp = mul_shift_64(mp, factor, j);
        vm = mul_shift_64(mm, factor, j);
        if q <= 1 {
            // mv is 4 * m2, so it always ends in two zero bits, and mp is
            // mv + 2, so it always ends in one.
            vr_is_trailing_zeros = true;
            vp -= 1;
        } else if q < 63 {
            vr_is_trailing_zeros = multiple_of_power_of_2(mv, q - 1);
        }
    }

    let (mantissa, removed) = shorten(vr, vp, vm, vr_is_trailing_zeros, 0);
    Decimal {
        mantissa,
        exponent: e10 + removed,
    }
}

/// A power-of-five table subscript, which Ryu's bounds keep non-negative.
fn index(i: i32) -> usize {
    usize::try_from(i).expect("Ryu's table subscripts are never negative")
}

/// Ryu's small-integer fast path: a float that is already a whole number below
/// `2^mantissa_bits` is its own shortest decimal, trailing zeros and all.
fn small_int(ieee_mantissa: u64, ieee_exponent: i32, mantissa_bits: i32, bias: i32) -> Option<u64> {
    let e2 = ieee_exponent - bias - mantissa_bits;
    if e2 < -mantissa_bits || e2 > 0 {
        return None;
    }
    // The implied leading one can never be part of the fraction here, so the
    // stored mantissa alone decides whether there is one.
    let shift = u32::try_from(-e2).expect("e2 is not positive here");
    if ieee_mantissa & ((1 << shift) - 1) != 0 {
        return None;
    }
    Some(((1 << mantissa_bits) | ieee_mantissa) >> shift)
}

/// The stored exponent field of an IEEE float, which is always small enough to
/// do Ryu's signed arithmetic in.
fn stored_exponent(bits: u64, mantissa_bits: i32, exponent_bits: i32) -> i32 {
    let field = (bits >> mantissa_bits) & ((1 << exponent_bits) - 1);
    i32::try_from(field).expect("an exponent field is at most 11 bits wide")
}

/// `copy_special_str`: how a zero, an infinity or a NaN prints, or `None` for
/// an ordinary finite non-zero float.
fn special(
    negative: bool,
    ieee_exponent: i32,
    exponent_bits: i32,
    mantissa: u64,
) -> Option<String> {
    let sign = if negative { "-" } else { "" };
    if ieee_exponent == (1 << exponent_bits) - 1 {
        return Some(if mantissa == 0 {
            format!("{sign}Infinity")
        } else {
            "NaN".to_string()
        });
    }
    (ieee_exponent == 0 && mantissa == 0).then(|| format!("{sign}0"))
}

/// `float4out`'s spelling of `value` at `extra_float_digits > 0`: the shortest
/// decimal that reads back as the same `float4`, never the midpoint between
/// two of them.
#[must_use]
pub fn float4_shortest(value: f32) -> String {
    let bits = value.to_bits();
    let negative = bits >> (FLOAT_MANTISSA_BITS + FLOAT_EXPONENT_BITS) != 0;
    let ieee_mantissa = bits & ((1 << FLOAT_MANTISSA_BITS) - 1);
    let ieee_exponent = stored_exponent(u64::from(bits), FLOAT_MANTISSA_BITS, FLOAT_EXPONENT_BITS);

    if let Some(text) = special(
        negative,
        ieee_exponent,
        FLOAT_EXPONENT_BITS,
        u64::from(ieee_mantissa),
    ) {
        return text;
    }
    let decimal = small_int(
        u64::from(ieee_mantissa),
        ieee_exponent,
        FLOAT_MANTISSA_BITS,
        FLOAT_BIAS,
    )
    .map_or_else(
        || f2d(ieee_mantissa, ieee_exponent),
        |mantissa| Decimal {
            mantissa,
            exponent: 0,
        },
    );
    layout(
        &decimal.mantissa.to_string(),
        decimal.exponent,
        negative,
        FLOAT_FIXED_UPPER,
    )
}

/// `float8out`'s spelling of `value` at `extra_float_digits > 0`: the shortest
/// decimal that reads back as the same `float8`, never the midpoint between
/// two of them.
#[must_use]
pub fn float8_shortest(value: f64) -> String {
    let bits = value.to_bits();
    let negative = bits >> (DOUBLE_MANTISSA_BITS + DOUBLE_EXPONENT_BITS) != 0;
    let ieee_mantissa = bits & ((1 << DOUBLE_MANTISSA_BITS) - 1);
    let ieee_exponent = stored_exponent(bits, DOUBLE_MANTISSA_BITS, DOUBLE_EXPONENT_BITS);

    if let Some(text) = special(negative, ieee_exponent, DOUBLE_EXPONENT_BITS, ieee_mantissa) {
        return text;
    }
    let decimal = small_int(
        ieee_mantissa,
        ieee_exponent,
        DOUBLE_MANTISSA_BITS,
        DOUBLE_BIAS,
    )
    .map_or_else(
        || d2d(ieee_mantissa, ieee_exponent),
        |mantissa| Decimal {
            mantissa,
            exponent: 0,
        },
    );
    layout(
        &decimal.mantissa.to_string(),
        decimal.exponent,
        negative,
        DOUBLE_FIXED_UPPER,
    )
}

/// `f2s.c`'s `FLOAT_POW5_INV_SPLIT`: the top 59 bits of `2^k / 5^q`.
const FLOAT_POW5_INV_SPLIT: [u64; 31] = [
    576_460_752_303_423_489,
    461_168_601_842_738_791,
    368_934_881_474_191_033,
    295_147_905_179_352_826,
    472_236_648_286_964_522,
    377_789_318_629_571_618,
    302_231_454_903_657_294,
    483_570_327_845_851_670,
    386_856_262_276_681_336,
    309_485_009_821_345_069,
    495_176_015_714_152_110,
    396_140_812_571_321_688,
    316_912_650_057_057_351,
    507_060_240_091_291_761,
    405_648_192_073_033_409,
    324_518_553_658_426_727,
    519_229_685_853_482_763,
    415_383_748_682_786_211,
    332_306_998_946_228_969,
    531_691_198_313_966_350,
    425_352_958_651_173_080,
    340_282_366_920_938_464,
    544_451_787_073_501_542,
    435_561_429_658_801_234,
    348_449_143_727_040_987,
    557_518_629_963_265_579,
    446_014_903_970_612_463,
    356_811_923_176_489_971,
    570_899_077_082_383_953,
    456_719_261_665_907_162,
    365_375_409_332_725_730,
];

/// `f2s.c`'s `FLOAT_POW5_SPLIT`: the top 61 bits of `5^i`.
const FLOAT_POW5_SPLIT: [u64; 47] = [
    1_152_921_504_606_846_976,
    1_441_151_880_758_558_720,
    1_801_439_850_948_198_400,
    2_251_799_813_685_248_000,
    1_407_374_883_553_280_000,
    1_759_218_604_441_600_000,
    2_199_023_255_552_000_000,
    1_374_389_534_720_000_000,
    1_717_986_918_400_000_000,
    2_147_483_648_000_000_000,
    1_342_177_280_000_000_000,
    1_677_721_600_000_000_000,
    2_097_152_000_000_000_000,
    1_310_720_000_000_000_000,
    1_638_400_000_000_000_000,
    2_048_000_000_000_000_000,
    1_280_000_000_000_000_000,
    1_600_000_000_000_000_000,
    2_000_000_000_000_000_000,
    1_250_000_000_000_000_000,
    1_562_500_000_000_000_000,
    1_953_125_000_000_000_000,
    1_220_703_125_000_000_000,
    1_525_878_906_250_000_000,
    1_907_348_632_812_500_000,
    1_192_092_895_507_812_500,
    1_490_116_119_384_765_625,
    1_862_645_149_230_957_031,
    1_164_153_218_269_348_144,
    1_455_191_522_836_685_180,
    1_818_989_403_545_856_475,
    2_273_736_754_432_320_594,
    1_421_085_471_520_200_371,
    1_776_356_839_400_250_464,
    2_220_446_049_250_313_080,
    1_387_778_780_781_445_675,
    1_734_723_475_976_807_094,
    2_168_404_344_971_008_868,
    1_355_252_715_606_880_542,
    1_694_065_894_508_600_678,
    2_117_582_368_135_750_847,
    1_323_488_980_084_844_279,
    1_654_361_225_106_055_349,
    2_067_951_531_382_569_187,
    1_292_469_707_114_105_741,
    1_615_587_133_892_632_177,
    2_019_483_917_365_790_221,
];

/// `d2s_full_table.h`'s `DOUBLE_POW5_INV_SPLIT`: the top 122 bits of
/// `2^k / 5^q`, as `[low, high]` 64-bit halves.
const DOUBLE_POW5_INV_SPLIT: [[u64; 2]; 292] = [
    [1, 288_230_376_151_711_744],
    [3_689_348_814_741_910_324, 230_584_300_921_369_395],
    [2_951_479_051_793_528_259, 184_467_440_737_095_516],
    [17_118_578_500_402_463_900, 147_573_952_589_676_412],
    [12_632_330_341_676_300_947, 236_118_324_143_482_260],
    [10_105_864_273_341_040_758, 188_894_659_314_785_808],
    [15_463_389_048_156_653_253, 151_115_727_451_828_646],
    [17_362_724_847_566_824_558, 241_785_163_922_925_834],
    [17_579_528_692_795_369_969, 193_428_131_138_340_667],
    [6_684_925_324_752_475_329, 154_742_504_910_672_534],
    [18_074_578_149_087_781_173, 247_588_007_857_076_054],
    [18_149_011_334_012_135_262, 198_070_406_285_660_843],
    [3_451_162_622_983_977_240, 158_456_325_028_528_675],
    [5_521_860_196_774_363_583, 253_530_120_045_645_880],
    [4_417_488_157_419_490_867, 202_824_096_036_516_704],
    [7_223_339_340_677_503_017, 162_259_276_829_213_363],
    [7_867_994_130_342_094_503, 259_614_842_926_741_381],
    [2_605_046_489_531_765_280, 207_691_874_341_393_105],
    [2_084_037_191_625_412_224, 166_153_499_473_114_484],
    [10_713_157_136_084_480_204, 265_845_599_156_983_174],
    [12_259_874_523_609_494_487, 212_676_479_325_586_539],
    [13_497_248_433_629_505_913, 170_141_183_460_469_231],
    [14_216_899_864_323_388_813, 272_225_893_536_750_770],
    [11_373_519_891_458_711_051, 217_780_714_829_400_616],
    [5_409_467_098_425_058_518, 174_224_571_863_520_493],
    [4_965_798_542_738_183_305, 278_759_314_981_632_789],
    [7_661_987_648_932_456_967, 223_007_451_985_306_231],
    [2_440_241_304_404_055_250, 178_405_961_588_244_985],
    [3_904_386_087_046_488_400, 285_449_538_541_191_976],
    [17_880_904_128_604_832_013, 228_359_630_832_953_580],
    [14_304_723_302_883_865_611, 182_687_704_666_362_864],
    [15_133_127_457_049_002_812, 146_150_163_733_090_291],
    [16_834_306_301_794_583_852, 233_840_261_972_944_466],
    [9_778_096_226_693_756_759, 187_072_209_578_355_573],
    [15_201_174_610_838_826_053, 149_657_767_662_684_458],
    [2_185_786_488_890_659_746, 239_452_428_260_295_134],
    [5_437_978_005_854_438_120, 191_561_942_608_236_107],
    [15_418_428_848_909_281_466, 153_249_554_086_588_885],
    [6_222_742_084_545_298_729, 245_199_286_538_542_217],
    [16_046_240_111_861_969_953, 196_159_429_230_833_773],
    [1_768_945_645_263_844_993, 156_927_543_384_667_019],
    [10_209_010_661_905_972_635, 251_084_069_415_467_230],
    [8_167_208_529_524_778_108, 200_867_255_532_373_784],
    [10_223_115_638_361_732_810, 160_693_804_425_899_027],
    [1_599_589_762_411_131_202, 257_110_087_081_438_444],
    [4_969_020_624_670_815_285, 205_688_069_665_150_755],
    [3_975_216_499_736_652_228, 164_550_455_732_120_604],
    [13_739_044_029_062_464_211, 263_280_729_171_392_966],
    [7_301_886_408_508_061_046, 210_624_583_337_114_373],
    [13_220_206_756_290_269_483, 168_499_666_669_691_498],
    [17_462_981_995_322_520_850, 269_599_466_671_506_397],
    [6_591_687_966_774_196_033, 215_679_573_337_205_118],
    [12_652_048_002_903_177_473, 172_543_658_669_764_094],
    [9_175_230_360_419_352_987, 276_069_853_871_622_551],
    [3_650_835_473_593_572_067, 220_855_883_097_298_041],
    [17_678_063_637_842_498_946, 176_684_706_477_838_432],
    [13_527_506_561_580_357_021, 282_695_530_364_541_492],
    [3_443_307_619_780_464_970, 226_156_424_291_633_194],
    [6_443_994_910_566_282_300, 180_925_139_433_306_555],
    [5_155_195_928_453_025_840, 144_740_111_546_645_244],
    [15_627_011_115_008_661_990, 231_584_178_474_632_390],
    [12_501_608_892_006_929_592, 185_267_342_779_705_912],
    [2_622_589_484_121_723_027, 148_213_874_223_764_730],
    [4_196_143_174_594_756_843, 237_142_198_758_023_568],
    [10_735_612_169_159_626_121, 189_713_759_006_418_854],
    [12_277_838_550_069_611_220, 151_771_007_205_135_083],
    [15_955_192_865_369_467_629, 242_833_611_528_216_133],
    [1_696_107_848_069_843_133, 194_266_889_222_572_907],
    [12_424_932_722_681_605_476, 155_413_511_378_058_325],
    [1_433_148_282_581_017_146, 248_661_618_204_893_321],
    [15_903_913_885_032_455_010, 198_929_294_563_914_656],
    [9_033_782_293_284_053_685, 159_143_435_651_131_725],
    [14_454_051_669_254_485_895, 254_629_497_041_810_760],
    [11_563_241_335_403_588_716, 203_703_597_633_448_608],
    [16_629_290_697_806_691_620, 162_962_878_106_758_886],
    [781_423_413_297_334_329, 260_740_604_970_814_219],
    [4_314_487_545_379_777_786, 208_592_483_976_651_375],
    [3_451_590_036_303_822_229, 166_873_987_181_321_100],
    [5_522_544_058_086_115_566, 266_998_379_490_113_760],
    [4_418_035_246_468_892_453, 213_598_703_592_091_008],
    [10_913_125_826_658_934_609, 170_878_962_873_672_806],
    [10_082_303_693_170_474_728, 273_406_340_597_876_490],
    [8_065_842_954_536_379_782, 218_725_072_478_301_192],
    [17_520_720_807_854_834_795, 174_980_057_982_640_953],
    [5_897_060_404_116_273_733, 279_968_092_772_225_526],
    [1_028_299_508_551_108_663, 223_974_474_217_780_421],
    [15_580_034_865_808_528_224, 179_179_579_374_224_336],
    [17_549_358_155_809_824_511, 286_687_326_998_758_938],
    [2_971_440_080_422_128_639, 229_349_861_599_007_151],
    [17_134_547_323_305_344_204, 183_479_889_279_205_720],
    [13_707_637_858_644_275_364, 146_783_911_423_364_576],
    [14_553_522_944_347_019_935, 234_854_258_277_383_322],
    [4_264_120_725_993_795_302, 187_883_406_621_906_658],
    [10_789_994_210_278_856_888, 150_306_725_297_525_326],
    [9_885_293_106_962_350_374, 240_490_760_476_040_522],
    [529_536_856_086_059_653, 192_392_608_380_832_418],
    [7_802_327_114_352_668_369, 153_914_086_704_665_934],
    [1_415_676_938_738_538_420, 246_262_538_727_465_495],
    [1_132_541_550_990_830_736, 197_010_030_981_972_396],
    [15_663_428_499_760_305_882, 157_608_024_785_577_916],
    [17_682_787_970_132_668_764, 252_172_839_656_924_666],
    [10_456_881_561_364_224_688, 201_738_271_725_539_733],
    [15_744_202_878_575_200_397, 161_390_617_380_431_786],
    [17_812_026_976_236_499_989, 258_224_987_808_690_858],
    [3_181_575_136_763_469_022, 206_579_990_246_952_687],
    [13_613_306_553_636_506_187, 165_263_992_197_562_149],
    [10_713_244_041_592_678_929, 264_422_387_516_099_439],
    [12_259_944_048_016_053_467, 211_537_910_012_879_551],
    [6_118_606_423_670_932_450, 169_230_328_010_303_641],
    [2_411_072_648_389_671_274, 270_768_524_816_485_826],
    [16_686_253_377_679_378_312, 216_614_819_853_188_660],
    [13_349_002_702_143_502_650, 173_291_855_882_550_928],
    [17_669_055_508_687_693_916, 277_266_969_412_081_485],
    [14_135_244_406_950_155_133, 221_813_575_529_665_188],
    [240_149_081_334_393_137, 177_450_860_423_732_151],
    [11_452_284_974_360_759_988, 283_921_376_677_971_441],
    [5_472_479_164_746_697_667, 227_137_101_342_377_153],
    [11_756_680_961_281_178_780, 181_709_681_073_901_722],
    [2_026_647_139_541_122_378, 145_367_744_859_121_378],
    [18_000_030_682_233_437_097, 232_588_391_774_594_204],
    [18_089_373_360_528_660_001, 186_070_713_419_675_363],
    [3_403_452_244_197_197_031, 148_856_570_735_740_291],
    [16_513_570_034_941_246_220, 238_170_513_177_184_465],
    [13_210_856_027_952_996_976, 190_536_410_541_747_572],
    [3_189_987_192_878_576_934, 152_429_128_433_398_058],
    [1_414_630_693_863_812_771, 243_886_605_493_436_893],
    [8_510_402_184_574_870_864, 195_109_284_394_749_514],
    [10_497_670_562_401_807_014, 156_087_427_515_799_611],
    [9_417_575_270_359_070_576, 249_739_884_025_279_378],
    [14_912_757_845_771_077_107, 199_791_907_220_223_502],
    [4_551_508_647_133_041_040, 159_833_525_776_178_802],
    [10_971_762_650_154_775_986, 255_733_641_241_886_083],
    [16_156_107_749_607_641_435, 204_586_912_993_508_866],
    [9_235_537_384_944_202_825, 163_669_530_394_807_093],
    [11_087_511_001_168_814_197, 261_871_248_631_691_349],
    [12_559_357_615_676_961_681, 209_496_998_905_353_079],
    [13_736_834_907_283_479_668, 167_597_599_124_282_463],
    [18_289_587_036_911_657_145, 268_156_158_598_851_941],
    [10_942_320_814_787_415_393, 214_524_926_879_081_553],
    [16_132_554_281_313_752_961, 171_619_941_503_265_242],
    [11_054_691_591_134_363_444, 274_591_906_405_224_388],
    [16_222_450_902_391_311_402, 219_673_525_124_179_510],
    [12_977_960_721_913_049_122, 175_738_820_099_343_608],
    [17_075_388_340_318_968_271, 281_182_112_158_949_773],
    [2_592_264_228_029_443_648, 224_945_689_727_159_819],
    [5_763_160_197_165_465_241, 179_956_551_781_727_855],
    [9_221_056_315_464_744_386, 287_930_482_850_764_568],
    [14_755_542_681_855_616_155, 230_344_386_280_611_654],
    [15_493_782_960_226_403_247, 184_275_509_024_489_323],
    [1_326_979_923_955_391_628, 147_420_407_219_591_459],
    [9_501_865_507_812_447_252, 235_872_651_551_346_334],
    [11_290_841_220_991_868_125, 188_698_121_241_077_067],
    [1_653_975_347_309_673_853, 150_958_496_992_861_654],
    [10_025_058_185_179_298_811, 241_533_595_188_578_646],
    [4_330_697_733_401_528_726, 193_226_876_150_862_917],
    [14_532_604_630_946_953_951, 154_581_500_920_690_333],
    [1_116_074_521_063_664_381, 247_330_401_473_104_534],
    [4_582_208_431_592_841_828, 197_864_321_178_483_627],
    [14_733_813_189_500_004_432, 158_291_456_942_786_901],
    [16_195_403_473_716_186_445, 253_266_331_108_459_042],
    [5_577_625_149_489_128_510, 202_613_064_886_767_234],
    [8_151_448_934_333_213_131, 162_090_451_909_413_787],
    [16_731_667_109_675_051_333, 259_344_723_055_062_059],
    [17_074_682_502_481_951_390, 207_475_778_444_049_647],
    [6_281_048_372_501_740_465, 165_980_622_755_239_718],
    [6_360_328_581_260_874_421, 265_568_996_408_383_549],
    [8_777_611_679_750_609_860, 212_455_197_126_706_839],
    [10_711_438_158_542_398_211, 169_964_157_701_365_471],
    [9_759_603_424_184_016_492, 271_942_652_322_184_754],
    [11_497_031_554_089_123_517, 217_554_121_857_747_803],
    [16_576_322_872_755_119_460, 174_043_297_486_198_242],
    [11_764_721_337_440_549_842, 278_469_275_977_917_188],
    [16_790_474_699_436_260_520, 222_775_420_782_333_750],
    [13_432_379_759_549_008_416, 178_220_336_625_867_000],
    [3_045_063_541_568_861_850, 285_152_538_601_387_201],
    [17_193_446_092_222_730_773, 228_122_030_881_109_760],
    [13_754_756_873_778_184_618, 182_497_624_704_887_808],
    [18_382_503_128_506_368_341, 145_998_099_763_910_246],
    [3_586_563_302_416_817_083, 233_596_959_622_256_395],
    [2_869_250_641_933_453_667, 186_877_567_697_805_116],
    [17_052_795_772_514_404_226, 149_502_054_158_244_092],
    [12_527_077_977_055_405_469, 239_203_286_653_190_548],
    [17_400_360_011_128_145_022, 191_362_629_322_552_438],
    [2_852_241_564_676_785_048, 153_090_103_458_041_951],
    [15_631_632_947_708_587_046, 244_944_165_532_867_121],
    [8_815_957_543_424_959_314, 195_955_332_426_293_697],
    [18_120_812_478_965_698_421, 156_764_265_941_034_957],
    [14_235_904_707_377_476_180, 250_822_825_505_655_932],
    [4_010_026_136_418_160_298, 200_658_260_404_524_746],
    [17_965_416_168_102_169_531, 160_526_608_323_619_796],
    [2_919_224_165_770_098_987, 256_842_573_317_791_675],
    [2_335_379_332_616_079_190, 205_474_058_654_233_340],
    [1_868_303_466_092_863_352, 164_379_246_923_386_672],
    [6_678_634_360_490_491_686, 263_006_795_077_418_675],
    [5_342_907_488_392_393_349, 210_405_436_061_934_940],
    [4_274_325_990_713_914_679, 168_324_348_849_547_952],
    [10_528_270_399_884_173_809, 269_318_958_159_276_723],
    [15_801_313_949_391_159_694, 215_455_166_527_421_378],
    [1_573_004_715_287_196_786, 172_364_133_221_937_103],
    [17_274_202_803_427_156_150, 275_782_613_155_099_364],
    [17_508_711_057_483_635_243, 220_626_090_524_079_491],
    [10_317_620_031_244_997_871, 176_500_872_419_263_593],
    [12_818_843_235_250_086_271, 282_401_395_870_821_749],
    [13_944_423_402_941_979_340, 225_921_116_696_657_399],
    [14_844_887_537_095_493_795, 180_736_893_357_325_919],
    [15_565_258_844_418_305_359, 144_589_514_685_860_735],
    [6_457_670_077_359_736_959, 231_343_223_497_377_177],
    [16_234_182_506_113_520_537, 185_074_578_797_901_741],
    [9_297_997_190_148_906_106, 148_059_663_038_321_393],
    [11_187_446_689_496_339_446, 236_895_460_861_314_229],
    [12_639_306_166_338_981_880, 189_516_368_689_051_383],
    [17_490_142_562_555_006_151, 151_613_094_951_241_106],
    [2_158_786_396_894_637_579, 242_580_951_921_985_771],
    [16_484_424_376_483_351_356, 194_064_761_537_588_616],
    [9_498_190_686_444_770_762, 155_251_809_230_070_893],
    [11_507_756_283_569_722_895, 248_402_894_768_113_429],
    [12_895_553_841_597_688_639, 198_722_315_814_490_743],
    [17_695_140_702_761_971_558, 158_977_852_651_592_594],
    [17_244_178_680_193_423_523, 254_364_564_242_548_151],
    [10_105_994_129_412_828_495, 203_491_651_394_038_521],
    [4_395_446_488_788_352_473, 162_793_321_115_230_817],
    [10_722_063_196_803_274_280, 260_469_313_784_369_307],
    [1_198_952_927_958_798_777, 208_375_451_027_495_446],
    [15_716_557_601_334_680_315, 166_700_360_821_996_356],
    [17_767_794_532_651_667_857, 266_720_577_315_194_170],
    [14_214_235_626_121_334_286, 213_376_461_852_155_336],
    [7_682_039_686_155_157_106, 170_701_169_481_724_269],
    [1_223_217_053_622_520_399, 273_121_871_170_758_831],
    [15_735_968_901_865_657_612, 218_497_496_936_607_064],
    [16_278_123_936_234_436_413, 174_797_997_549_285_651],
    [219_556_594_781_725_998, 279_676_796_078_857_043],
    [7_554_342_905_309_201_445, 223_741_436_863_085_634],
    [9_732_823_138_989_271_479, 178_993_149_490_468_507],
    [815_121_763_415_193_074, 286_389_039_184_749_612],
    [11_720_143_854_957_885_429, 229_111_231_347_799_689],
    [13_065_463_898_708_218_666, 183_288_985_078_239_751],
    [6_763_022_304_224_664_610, 146_631_188_062_591_801],
    [3_442_138_057_275_642_729, 234_609_900_900_146_882],
    [13_821_756_890_046_245_153, 187_687_920_720_117_505],
    [11_057_405_512_036_996_122, 150_150_336_576_094_004],
    [6_623_802_375_033_462_826, 240_240_538_521_750_407],
    [16_367_088_344_252_501_231, 192_192_430_817_400_325],
    [13_093_670_675_402_000_985, 153_753_944_653_920_260],
    [2_503_129_006_933_649_959, 246_006_311_446_272_417],
    [13_070_549_649_772_650_937, 196_805_049_157_017_933],
    [17_835_137_349_301_941_396, 157_444_039_325_614_346],
    [2_710_778_055_689_733_971, 251_910_462_920_982_955],
    [2_168_622_444_551_787_177, 201_528_370_336_786_364],
    [5_424_246_770_383_340_065, 161_222_696_269_429_091],
    [1_300_097_203_129_523_457, 257_956_314_031_086_546],
    [15_797_473_021_471_260_058, 206_365_051_224_869_236],
    [8_948_629_602_435_097_724, 165_092_040_979_895_389],
    [3_249_760_919_670_425_388, 264_147_265_567_832_623],
    [9_978_506_365_220_160_957, 211_317_812_454_266_098],
    [15_361_502_721_659_949_412, 169_054_249_963_412_878],
    [2_442_311_466_204_457_120, 270_486_799_941_460_606],
    [16_711_244_431_931_206_989, 216_389_439_953_168_484],
    [17_058_344_360_286_875_914, 173_111_551_962_534_787],
    [12_535_955_717_491_360_170, 276_978_483_140_055_660],
    [10_028_764_573_993_088_136, 221_582_786_512_044_528],
    [15_401_709_288_678_291_155, 177_266_229_209_635_622],
    [9_885_339_602_917_624_555, 283_625_966_735_416_996],
    [4_218_922_867_592_189_321, 226_900_773_388_333_597],
    [14_443_184_738_299_482_427, 181_520_618_710_666_877],
    [4_175_850_161_155_765_295, 145_216_494_968_533_502],
    [10_370_709_072_591_134_795, 232_346_391_949_653_603],
    [15_675_264_887_556_728_482, 185_877_113_559_722_882],
    [5_161_514_280_561_562_140, 148_701_690_847_778_306],
    [879_725_219_414_678_777, 237_922_705_356_445_290],
    [703_780_175_531_743_021, 190_338_164_285_156_232],
    [11_631_070_584_651_125_387, 152_270_531_428_124_985],
    [162_968_861_732_249_003, 243_632_850_284_999_977],
    [11_198_421_533_611_530_172, 194_906_280_227_999_981],
    [5_269_388_412_147_313_814, 155_925_024_182_399_985],
    [8_431_021_459_435_702_103, 249_480_038_691_839_976],
    [3_055_468_352_806_651_359, 199_584_030_953_471_981],
    [17_201_769_941_212_962_380, 159_667_224_762_777_584],
    [16_454_785_461_715_008_838, 255_467_559_620_444_135],
    [13_163_828_369_372_007_071, 204_374_047_696_355_308],
    [17_909_760_324_981_426_303, 163_499_238_157_084_246],
    [2_830_174_816_776_909_822, 261_598_781_051_334_795],
    [2_264_139_853_421_527_858, 209_279_024_841_067_836],
    [16_568_707_141_704_863_579, 167_423_219_872_854_268],
    [4_373_838_538_276_319_787, 267_877_151_796_566_830],
    [3_499_070_830_621_055_830, 214_301_721_437_253_464],
    [6_488_605_479_238_754_987, 171_441_377_149_802_771],
    [3_003_071_137_298_187_333, 274_306_203_439_684_434],
    [6_091_805_724_580_460_189, 219_444_962_751_747_547],
    [15_941_491_023_890_099_121, 175_555_970_201_398_037],
    [10_748_990_379_256_517_301, 280_889_552_322_236_860],
    [8_599_192_303_405_213_841, 224_711_641_857_789_488],
    [14_258_051_472_207_991_719, 179_769_313_486_231_590],
];

/// `d2s_full_table.h`'s `DOUBLE_POW5_SPLIT`: the top 121 bits of `5^i`, as
/// `[low, high]` 64-bit halves.
const DOUBLE_POW5_SPLIT: [[u64; 2]; 326] = [
    [0, 72_057_594_037_927_936],
    [0, 90_071_992_547_409_920],
    [0, 112_589_990_684_262_400],
    [0, 140_737_488_355_328_000],
    [0, 87_960_930_222_080_000],
    [0, 109_951_162_777_600_000],
    [0, 137_438_953_472_000_000],
    [0, 85_899_345_920_000_000],
    [0, 107_374_182_400_000_000],
    [0, 134_217_728_000_000_000],
    [0, 83_886_080_000_000_000],
    [0, 104_857_600_000_000_000],
    [0, 131_072_000_000_000_000],
    [0, 81_920_000_000_000_000],
    [0, 102_400_000_000_000_000],
    [0, 128_000_000_000_000_000],
    [0, 80_000_000_000_000_000],
    [0, 100_000_000_000_000_000],
    [0, 125_000_000_000_000_000],
    [0, 78_125_000_000_000_000],
    [0, 97_656_250_000_000_000],
    [0, 122_070_312_500_000_000],
    [0, 76_293_945_312_500_000],
    [0, 95_367_431_640_625_000],
    [0, 119_209_289_550_781_250],
    [4_611_686_018_427_387_904, 74_505_805_969_238_281],
    [10_376_293_541_461_622_784, 93_132_257_461_547_851],
    [8_358_680_908_399_640_576, 116_415_321_826_934_814],
    [612_489_549_322_387_456, 72_759_576_141_834_259],
    [14_600_669_991_935_148_032, 90_949_470_177_292_823],
    [13_639_151_471_491_547_136, 113_686_837_721_616_029],
    [3_213_881_284_082_270_208, 142_108_547_152_020_037],
    [4_314_518_811_765_112_832, 88_817_841_970_012_523],
    [781_462_496_279_003_136, 111_022_302_462_515_654],
    [10_200_200_157_203_529_728, 138_777_878_078_144_567],
    [13_292_654_125_893_287_936, 86_736_173_798_840_354],
    [7_392_445_620_511_834_112, 108_420_217_248_550_443],
    [4_628_871_007_212_404_736, 135_525_271_560_688_054],
    [16_728_102_434_789_916_672, 84_703_294_725_430_033],
    [7_075_069_988_205_232_128, 105_879_118_406_787_542],
    [18_067_209_522_111_315_968, 132_348_898_008_484_427],
    [8_986_162_942_105_878_528, 82_718_061_255_302_767],
    [6_621_017_659_204_960_256, 103_397_576_569_128_459],
    [3_664_586_055_578_812_416, 129_246_970_711_410_574],
    [16_125_424_340_018_921_472, 80_779_356_694_631_608],
    [1_710_036_351_314_100_224, 100_974_195_868_289_511],
    [15_972_603_494_424_788_992, 126_217_744_835_361_888],
    [9_982_877_184_015_493_120, 78_886_090_522_101_180],
    [12_478_596_480_019_366_400, 98_607_613_152_626_475],
    [10_986_559_581_596_820_096, 123_259_516_440_783_094],
    [2_254_913_720_070_624_656, 77_037_197_775_489_434],
    [12_042_014_186_943_056_628, 96_296_497_219_361_792],
    [15_052_517_733_678_820_785, 120_370_621_524_202_240],
    [9_407_823_583_549_262_990, 75_231_638_452_626_400],
    [11_759_779_479_436_578_738, 94_039_548_065_783_000],
    [14_699_724_349_295_723_422, 117_549_435_082_228_750],
    [4_575_641_699_882_439_235, 73_468_396_926_392_969],
    [10_331_238_143_280_436_948, 91_835_496_157_991_211],
    [8_302_361_660_673_158_281, 114_794_370_197_489_014],
    [1_154_580_038_986_672_043, 143_492_962_746_861_268],
    [9_944_984_561_221_445_835, 89_683_101_716_788_292],
    [12_431_230_701_526_807_293, 112_103_877_145_985_365],
    [1_703_980_321_626_345_405, 140_129_846_432_481_707],
    [17_205_888_765_512_323_542, 87_581_154_020_301_066],
    [12_283_988_920_035_628_619, 109_476_442_525_376_333],
    [1_519_928_094_762_372_062, 136_845_553_156_720_417],
    [12_479_170_105_294_952_299, 85_528_470_722_950_260],
    [15_598_962_631_618_690_374, 106_910_588_403_687_825],
    [5_663_645_234_241_199_255, 133_638_235_504_609_782],
    [17_374_836_326_682_913_246, 83_523_897_190_381_113],
    [7_883_487_353_071_477_846, 104_404_871_487_976_392],
    [9_854_359_191_339_347_308, 130_506_089_359_970_490],
    [10_770_660_513_014_479_971, 81_566_305_849_981_556],
    [13_463_325_641_268_099_964, 101_957_882_312_476_945],
    [2_994_098_996_302_961_243, 127_447_352_890_596_182],
    [15_706_369_927_971_514_489, 79_654_595_556_622_613],
    [5_797_904_354_682_229_399, 99_568_244_445_778_267],
    [2_635_694_424_925_398_845, 124_460_305_557_222_834],
    [6_258_995_034_005_762_182, 77_787_690_973_264_271],
    [3_212_057_774_079_814_824, 97_234_613_716_580_339],
    [17_850_130_272_881_932_242, 121_543_267_145_725_423],
    [18_073_860_448_192_289_507, 75_964_541_966_078_389],
    [8_757_267_504_958_198_172, 94_955_677_457_597_987],
    [6_334_898_362_770_359_811, 118_694_596_821_997_484],
    [13_182_683_513_586_250_689, 74_184_123_013_748_427],
    [11_866_668_373_555_425_458, 92_730_153_767_185_534],
    [5_609_963_430_089_506_015, 115_912_692_208_981_918],
    [17_341_285_199_088_104_971, 72_445_432_630_613_698],
    [12_453_234_462_005_355_406, 90_556_790_788_267_123],
    [10_954_857_059_079_306_353, 113_195_988_485_333_904],
    [13_693_571_323_849_132_942, 141_494_985_606_667_380],
    [17_781_854_114_260_483_896, 88_434_366_004_167_112],
    [3_780_573_569_116_053_255, 110_542_957_505_208_891],
    [114_030_942_967_678_664, 138_178_696_881_511_114],
    [4_682_955_357_782_187_069, 86_361_685_550_944_446],
    [15_077_066_234_082_509_644, 107_952_106_938_680_557],
    [5_011_274_737_320_973_344, 134_940_133_673_350_697],
    [14_661_261_756_894_078_100, 84_337_583_545_844_185],
    [4_491_519_140_835_433_913, 105_421_979_432_305_232],
    [5_614_398_926_044_292_391, 131_777_474_290_381_540],
    [12_732_371_365_632_458_552, 82_360_921_431_488_462],
    [6_692_092_170_185_797_382, 102_951_151_789_360_578],
    [17_588_487_249_587_022_536, 128_688_939_736_700_722],
    [15_604_490_549_419_276_989, 80_430_587_335_437_951],
    [14_893_927_168_346_708_332, 100_538_234_169_297_439],
    [14_005_722_942_005_997_511, 125_672_792_711_621_799],
    [15_671_105_866_394_830_300, 78_545_495_444_763_624],
    [1_142_138_259_283_986_260, 98_181_869_305_954_531],
    [15_262_730_879_387_146_537, 122_727_336_632_443_163],
    [7_233_363_790_403_272_633, 76_704_585_395_276_977],
    [13_653_390_756_431_478_696, 95_880_731_744_096_221],
    [3_231_680_390_257_184_658, 119_850_914_680_120_277],
    [4_325_643_253_124_434_363, 74_906_821_675_075_173],
    [10_018_740_084_832_930_858, 93_633_527_093_843_966],
    [3_300_053_069_186_387_764, 117_041_908_867_304_958],
    [15_897_591_223_523_656_064, 73_151_193_042_065_598],
    [10_648_616_992_549_794_273, 91_438_991_302_581_998],
    [4_087_399_203_832_467_033, 114_298_739_128_227_498],
    [14_332_621_041_645_359_599, 142_873_423_910_284_372],
    [18_181_260_187_883_125_557, 89_295_889_943_927_732],
    [4_279_831_161_144_355_331, 111_619_862_429_909_666],
    [14_573_160_988_285_219_972, 139_524_828_037_387_082],
    [13_719_911_636_105_650_386, 87_203_017_523_366_926],
    [7_926_517_508_277_287_175, 109_003_771_904_208_658],
    [684_774_848_491_833_161, 136_254_714_880_260_823],
    [7_345_513_307_948_477_581, 85_159_196_800_163_014],
    [18_405_263_671_790_372_785, 106_448_996_000_203_767],
    [18_394_893_571_310_578_077, 133_061_245_000_254_709],
    [13_802_651_491_282_805_250, 83_163_278_125_159_193],
    [3_418_256_308_821_342_851, 103_954_097_656_448_992],
    [4_272_820_386_026_678_563, 129_942_622_070_561_240],
    [2_670_512_741_266_674_102, 81_214_138_794_100_775],
    [17_173_198_981_865_506_339, 101_517_673_492_625_968],
    [3_019_754_653_622_331_308, 126_897_091_865_782_461],
    [4_193_189_667_727_651_020, 79_310_682_416_114_038],
    [14_464_859_121_514_339_583, 99_138_353_020_142_547],
    [13_469_387_883_465_536_574, 123_922_941_275_178_184],
    [8_418_367_427_165_960_359, 77_451_838_296_986_365],
    [15_134_645_302_384_838_353, 96_814_797_871_232_956],
    [471_562_554_271_496_325, 121_018_497_339_041_196],
    [9_518_098_633_274_461_011, 75_636_560_836_900_747],
    [7_285_937_273_165_688_360, 94_545_701_046_125_934],
    [18_330_793_628_311_886_258, 118_182_126_307_657_417],
    [4_539_216_990_053_847_055, 73_863_828_942_285_886],
    [14_897_393_274_422_084_627, 92_329_786_177_857_357],
    [4_786_683_537_745_442_072, 115_412_232_722_321_697],
    [14_520_892_257_159_371_055, 72_132_645_451_451_060],
    [18_151_115_321_449_213_818, 90_165_806_814_313_825],
    [8_853_836_096_529_353_561, 112_707_258_517_892_282],
    [1_843_923_083_806_916_143, 140_884_073_147_365_353],
    [12_681_666_973_447_792_349, 88_052_545_717_103_345],
    [2_017_025_661_527_576_725, 110_065_682_146_379_182],
    [11_744_654_113_764_246_714, 137_582_102_682_973_977],
    [422_879_793_461_572_340, 85_988_814_176_858_736],
    [528_599_741_826_965_425, 107_486_017_721_073_420],
    [660_749_677_283_706_782, 134_357_522_151_341_775],
    [7_330_497_575_943_398_595, 83_973_451_344_588_609],
    [13_774_807_988_356_636_147, 104_966_814_180_735_761],
    [3_383_451_930_163_631_472, 131_208_517_725_919_702],
    [15_949_715_511_634_433_382, 82_005_323_578_699_813],
    [6_102_086_334_260_878_016, 102_506_654_473_374_767],
    [3_015_921_899_398_709_616, 128_133_318_091_718_459],
    [18_025_852_251_620_051_174, 80_083_323_807_324_036],
    [4_085_571_240_815_512_351, 100_104_154_759_155_046],
    [14_330_336_087_874_166_247, 125_130_193_448_943_807],
    [15_873_989_082_562_435_760, 78_206_370_905_589_879],
    [15_230_800_334_775_656_796, 97_757_963_631_987_349],
    [5_203_442_363_187_407_284, 122_197_454_539_984_187],
    [946_308_467_778_435_600, 76_373_409_087_490_117],
    [5_794_571_603_150_432_404, 95_466_761_359_362_646],
    [16_466_586_540_792_816_313, 119_333_451_699_203_307],
    [7_985_773_578_781_816_244, 74_583_407_312_002_067],
    [5_370_530_955_049_882_401, 93_229_259_140_002_584],
    [6_713_163_693_812_353_001, 116_536_573_925_003_230],
    [18_030_785_363_914_884_337, 72_835_358_703_127_018],
    [13_315_109_668_038_829_614, 91_044_198_378_908_773],
    [2_808_829_029_766_373_305, 113_805_247_973_635_967],
    [17_346_094_342_490_130_344, 142_256_559_967_044_958],
    [6_229_622_945_628_943_561, 88_910_349_979_403_099],
    [3_175_342_663_608_791_547, 111_137_937_474_253_874],
    [13_192_550_366_365_765_242, 138_922_421_842_817_342],
    [3_633_657_960_551_215_372, 86_826_513_651_760_839],
    [18_377_130_505_971_182_927, 108_533_142_064_701_048],
    [4_524_669_058_754_427_043, 135_666_427_580_876_311],
    [9_745_447_189_362_598_758, 84_791_517_238_047_694],
    [2_958_436_949_848_472_639, 105_989_396_547_559_618],
    [12_921_418_224_165_366_607, 132_486_745_684_449_522],
    [12_687_572_408_530_742_033, 82_804_216_052_780_951],
    [11_247_779_492_236_039_638, 103_505_270_065_976_189],
    [224_666_310_012_885_835, 129_381_587_582_470_237],
    [2_446_259_452_971_747_599, 80_863_492_239_043_898],
    [12_281_196_353_069_460_307, 101_079_365_298_804_872],
    [15_351_495_441_336_825_384, 126_349_206_623_506_090],
    [14_206_370_669_262_903_769, 78_968_254_139_691_306],
    [8_534_591_299_723_853_903, 98_710_317_674_614_133],
    [15_279_925_143_082_205_283, 123_387_897_093_267_666],
    [14_161_639_232_853_766_206, 77_117_435_683_292_291],
    [13_090_363_022_639_819_853, 96_396_794_604_115_364],
    [16_362_953_778_299_774_816, 120_495_993_255_144_205],
    [12_532_689_120_651_053_212, 75_309_995_784_465_128],
    [15_665_861_400_813_816_515, 94_137_494_730_581_410],
    [10_358_954_714_162_494_836, 117_671_868_413_226_763],
    [4_168_503_687_137_865_320, 73_544_917_758_266_727],
    [598_943_590_494_943_747, 91_931_147_197_833_409],
    [5_360_365_506_546_067_587, 114_913_933_997_291_761],
    [11_312_142_901_609_972_388, 143_642_417_496_614_701],
    [9_375_932_322_719_926_695, 89_776_510_935_384_188],
    [11_719_915_403_399_908_368, 112_220_638_669_230_235],
    [10_038_208_235_822_497_557, 140_275_798_336_537_794],
    [10_885_566_165_816_448_877, 87_672_373_960_336_121],
    [18_218_643_725_697_949_000, 109_590_467_450_420_151],
    [18_161_618_638_695_048_346, 136_988_084_313_025_189],
    [13_656_854_658_398_099_168, 85_617_552_695_640_743],
    [12_459_382_304_570_236_056, 107_021_940_869_550_929],
    [1_739_169_825_430_631_358, 133_777_426_086_938_662],
    [14_922_039_196_176_308_311, 83_610_891_304_336_663],
    [14_040_862_976_792_997_485, 104_513_614_130_420_829],
    [3_716_020_665_709_083_144, 130_642_017_663_026_037],
    [4_628_355_925_281_870_917, 81_651_261_039_391_273],
    [10_397_130_925_029_726_550, 102_064_076_299_239_091],
    [8_384_727_637_859_770_284, 127_580_095_374_048_864],
    [5_240_454_773_662_356_427, 79_737_559_608_780_540],
    [6_550_568_467_077_945_534, 99_671_949_510_975_675],
    [3_576_524_565_420_044_014, 124_589_936_888_719_594],
    [6_847_013_871_814_915_412, 77_868_710_555_449_746],
    [17_782_139_376_623_420_074, 97_335_888_194_312_182],
    [13_004_302_183_924_499_284, 121_669_860_242_890_228],
    [17_351_060_901_807_587_860, 76_043_662_651_806_392],
    [3_242_082_053_549_933_210, 95_054_578_314_757_991],
    [17_887_660_622_219_580_224, 118_818_222_893_447_488],
    [11_179_787_888_887_237_640, 74_261_389_308_404_680],
    [13_974_734_861_109_047_050, 92_826_736_635_505_850],
    [8_245_046_539_531_533_005, 116_033_420_794_382_313],
    [16_682_369_133_275_677_888, 72_520_887_996_488_945],
    [7_017_903_361_312_433_648, 90_651_109_995_611_182],
    [17_995_751_238_495_317_868, 113_313_887_494_513_977],
    [8_659_630_992_836_983_623, 141_642_359_368_142_472],
    [5_412_269_370_523_114_764, 88_526_474_605_089_045],
    [11_377_022_731_581_281_359, 110_658_093_256_361_306],
    [4_997_906_377_621_825_891, 138_322_616_570_451_633],
    [14_652_906_532_082_110_942, 86_451_635_356_532_270],
    [9_092_761_128_247_862_869, 108_064_544_195_665_338],
    [2_142_579_373_455_052_779, 135_080_680_244_581_673],
    [12_868_327_154_477_877_747, 84_425_425_152_863_545],
    [2_250_350_887_815_183_471, 105_531_781_441_079_432],
    [2_812_938_609_768_979_339, 131_914_726_801_349_290],
    [6_369_772_649_532_999_991, 82_446_704_250_843_306],
    [17_185_587_848_771_025_797, 103_058_380_313_554_132],
    [3_035_240_737_254_230_630, 128_822_975_391_942_666],
    [6_508_711_479_211_282_048, 80_514_359_619_964_166],
    [17_359_261_385_868_878_368, 100_642_949_524_955_207],
    [17_087_390_713_908_710_056, 125_803_686_906_194_009],
    [3_762_090_168_551_861_929, 78_627_304_316_371_256],
    [4_702_612_710_689_827_411, 98_284_130_395_464_070],
    [15_101_637_925_217_060_072, 122_855_162_994_330_087],
    [16_356_052_730_901_744_401, 76_784_476_871_456_304],
    [1_998_321_839_917_628_885, 95_980_596_089_320_381],
    [7_109_588_318_324_424_010, 119_975_745_111_650_476],
    [13_666_864_735_807_540_814, 74_984_840_694_781_547],
    [12_471_894_901_332_038_114, 93_731_050_868_476_934],
    [6_366_496_589_810_271_835, 117_163_813_585_596_168],
    [3_979_060_368_631_419_896, 73_227_383_490_997_605],
    [9_585_511_479_216_662_775, 91_534_229_363_747_006],
    [2_758_517_312_166_052_660, 114_417_786_704_683_758],
    [12_671_518_677_062_341_634, 143_022_233_380_854_697],
    [1_002_170_145_522_881_665, 89_388_895_863_034_186],
    [10_476_084_718_758_377_889, 111_736_119_828_792_732],
    [13_095_105_898_447_972_362, 139_670_149_785_990_915],
    [5_878_598_177_316_288_774, 87_293_843_616_244_322],
    [16_571_619_758_500_136_775, 109_117_304_520_305_402],
    [11_491_152_661_270_395_161, 136_396_630_650_381_753],
    [264_441_385_652_915_120, 85_247_894_156_488_596],
    [330_551_732_066_143_900, 106_559_867_695_610_745],
    [5_024_875_683_510_067_779, 133_199_834_619_513_431],
    [10_058_076_329_834_874_218, 83_249_896_637_195_894],
    [3_349_223_375_438_816_964, 104_062_370_796_494_868],
    [4_186_529_219_298_521_205, 130_077_963_495_618_585],
    [14_145_795_808_130_045_513, 81_298_727_184_761_615],
    [13_070_558_741_735_168_987, 101_623_408_980_952_019],
    [11_726_512_408_741_573_330, 127_029_261_226_190_024],
    [7_329_070_255_463_483_331, 79_393_288_266_368_765],
    [13_773_023_837_756_742_068, 99_241_610_332_960_956],
    [17_216_279_797_195_927_585, 124_052_012_916_201_195],
    [8_454_331_864_033_760_789, 77_532_508_072_625_747],
    [5_956_228_811_614_813_082, 96_915_635_090_782_184],
    [7_445_286_014_518_516_353, 121_144_543_863_477_730],
    [9_264_989_777_501_460_624, 75_715_339_914_673_581],
    [16_192_923_240_304_213_684, 94_644_174_893_341_976],
    [1_794_409_976_670_715_490, 118_305_218_616_677_471],
    [8_039_035_263_060_279_037, 73_940_761_635_423_419],
    [5_437_108_060_397_960_892, 92_425_952_044_279_274],
    [16_019_757_112_352_226_923, 115_532_440_055_349_092],
    [788_976_158_365_366_019, 72_207_775_034_593_183],
    [14_821_278_253_238_871_236, 90_259_718_793_241_478],
    [9_303_225_779_693_813_237, 112_824_648_491_551_848],
    [11_629_032_224_617_266_546, 141_030_810_614_439_810],
    [11_879_831_158_813_179_495, 88_144_256_634_024_881],
    [1_014_730_893_234_310_657, 110_180_320_792_531_102],
    [10_491_785_653_397_664_129, 137_725_400_990_663_877],
    [8_863_209_042_587_234_033, 86_078_375_619_164_923],
    [6_467_325_284_806_654_637, 107_597_969_523_956_154],
    [17_307_528_642_863_094_104, 134_497_461_904_945_192],
    [10_817_205_401_789_433_815, 84_060_913_690_590_745],
    [18_133_192_770_664_180_173, 105_076_142_113_238_431],
    [18_054_804_944_902_837_312, 131_345_177_641_548_039],
    [18_201_782_118_205_355_176, 82_090_736_025_967_524],
    [4_305_483_574_047_142_354, 102_613_420_032_459_406],
    [14_605_226_504_413_703_751, 128_266_775_040_574_257],
    [2_210_737_537_617_482_988, 80_166_734_400_358_911],
    [16_598_479_977_304_017_447, 100_208_418_000_448_638],
    [11_524_727_934_775_246_001, 125_260_522_500_560_798],
    [2_591_268_940_807_140_847, 78_287_826_562_850_499],
    [17_074_144_231_291_089_770, 97_859_783_203_563_123],
    [16_730_994_270_686_474_309, 122_324_729_004_453_904],
    [10_456_871_419_179_046_443, 76_452_955_627_783_690],
    [3_847_717_237_119_032_246, 95_566_194_534_729_613],
    [9_421_332_564_826_178_211, 119_457_743_168_412_016],
    [5_888_332_853_016_361_382, 74_661_089_480_257_510],
    [16_583_788_103_125_227_536, 93_326_361_850_321_887],
    [16_118_049_110_479_146_516, 116_657_952_312_902_359],
    [16_991_309_721_690_548_428, 72_911_220_195_563_974],
    [12_015_765_115_258_409_727, 91_139_025_244_454_968],
    [15_019_706_394_073_012_159, 113_923_781_555_568_710],
    [9_551_260_955_736_489_391, 142_404_726_944_460_888],
    [5_969_538_097_335_305_869, 89_002_954_340_288_055],
    [2_850_236_603_241_744_433, 111_253_692_925_360_069],
];

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{float4_shortest, float8_shortest};

    #[test]
    fn float4_never_prints_the_midpoint_between_two_floats() {
        // 9e+09 reads back as this float, and is shorter, but it is exactly
        // the midpoint to the next float up, so `float4out` refuses it.
        assert!(float4_shortest(f32::from_bits(0x5006_1c46)) == "8.999999e+09");
        assert!(float4_shortest(f32::from_bits(0x4c00_0004)) == "3.3554448e+07");
        assert!(float4_shortest(f32::from_bits(0x5100_06a8)) == "3.4366718e+10");
    }

    #[test]
    fn float4_breaks_an_exact_decimal_tie_to_even() {
        // 305404.125 exactly: the last digit goes to even, not away from zero.
        assert!(float4_shortest(f32::from_bits(0x4895_1f84)) == "305404.12");
        assert!(float4_shortest(f32::from_bits(0x45fd_1840)) == "8099.0312");
        assert!(float4_shortest(f32::from_bits(0x3980_0000)) == "0.00024414062");
        assert!(float4_shortest(f32::from_bits(0x3b20_0000)) == "0.0024414062");
    }

    #[test]
    fn float4_lays_values_out_like_printf_g() {
        assert!(float4_shortest(0.0) == "0");
        assert!(float4_shortest(-0.0) == "-0");
        assert!(float4_shortest(1.0) == "1");
        assert!(float4_shortest(-34.84) == "-34.84");
        assert!(float4_shortest(1.0e10) == "1e+10");
        assert!(float4_shortest(1.234_567_8e-5) == "1.2345678e-05");
        assert!(float4_shortest(100_000.0) == "100000");
        assert!(float4_shortest(1_000_000.0) == "1e+06");
        assert!(float4_shortest(0.000_1) == "0.0001");
        assert!(float4_shortest(0.000_01) == "1e-05");
        assert!(float4_shortest(f32::MIN_POSITIVE) == "1.1754944e-38");
        assert!(float4_shortest(f32::from_bits(1)) == "1e-45");
        assert!(float4_shortest(f32::INFINITY) == "Infinity");
        assert!(float4_shortest(f32::NEG_INFINITY) == "-Infinity");
        assert!(float4_shortest(f32::NAN) == "NaN");
    }

    #[test]
    fn float8_never_prints_the_midpoint_between_two_floats() {
        // Every one of these is exactly the midpoint to the next float up or
        // down, so `float8out` spends the extra digits rather than print it.
        assert!(float8_shortest(1.9e22) == "1.9000000000000002e+22");
        assert!(float8_shortest(5.0e22) == "4.9999999999999996e+22");
        assert!(float8_shortest(7.0e22) == "7.0000000000000004e+22");
    }

    #[test]
    fn float8_lays_values_out_like_printf_g() {
        assert!(float8_shortest(0.0) == "0");
        assert!(float8_shortest(-0.0) == "-0");
        assert!(float8_shortest(1.0) == "1");
        assert!(float8_shortest(0.1) == "0.1");
        assert!(float8_shortest(1.0 / 3.0) == "0.3333333333333333");
        assert!(float8_shortest(1.0e14) == "100000000000000");
        assert!(float8_shortest(1.0e15) == "1e+15");
        assert!(float8_shortest(0.000_1) == "0.0001");
        assert!(float8_shortest(0.000_01) == "1e-05");
        assert!(float8_shortest(f64::MIN_POSITIVE) == "2.2250738585072014e-308");
        assert!(float8_shortest(f64::from_bits(1)) == "5e-324");
        assert!(float8_shortest(f64::INFINITY) == "Infinity");
        assert!(float8_shortest(f64::NEG_INFINITY) == "-Infinity");
        assert!(float8_shortest(f64::NAN) == "NaN");
        assert!(float8_shortest(-1.234_567_890_123_456_7e-30) == "-1.2345678901234567e-30");
    }

    #[test]
    fn every_float_reads_back_as_itself() {
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        for _ in 0..100_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let single = f32::from_bits(u32::try_from(state >> 32).expect("shifted to 32 bits"));
            if single.is_finite() {
                let text = float4_shortest(single);
                assert!(text.parse::<f32>() == Ok(single), "{text} for {single:?}");
            }
            let double = f64::from_bits(state);
            if double.is_finite() {
                let text = float8_shortest(double);
                assert!(text.parse::<f64>() == Ok(double), "{text} for {double:?}");
            }
        }
    }
}
