use num_traits::ToPrimitive;

pub(crate) fn to_f64<T: ToPrimitive + Copy>(value: T) -> f64 {
    value
        .to_f64()
        .expect("primitive numeric values are representable as f64")
}

pub(crate) fn nonnegative_f64_to_u64(value: f64) -> u64 {
    if value.is_nan() || value <= 0.0 {
        0
    } else if value >= to_f64(u64::MAX) {
        u64::MAX
    } else {
        value.to_u64().unwrap_or_default()
    }
}

pub(crate) fn saturating_u128_to_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(crate) fn nonnegative_i64_to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or_default()
}
