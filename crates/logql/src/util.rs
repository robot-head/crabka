use std::fmt;

use crabka_units::{ByteSize, convert::ByteSizeExt};

pub(crate) fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch == ':' || ch == '.' || ch.is_ascii_alphabetic()
}

pub(crate) fn is_ident_char(ch: char) -> bool {
    is_ident_start(ch) || ch.is_ascii_digit()
}

pub(crate) fn gcd_u64(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

pub(crate) struct QuotedChar(pub(crate) char);

pub(crate) fn decode_quoted_escape(escaped: char) -> char {
    match escaped {
        'n' => '\n',
        'r' => '\r',
        't' => '\t',
        '"' => '"',
        '\\' => '\\',
        other => other,
    }
}

pub(crate) fn duration_unit(unit: &str) -> Option<(u8, u16, i128)> {
    match unit {
        "y" => Some((0, 0x001, 31_536_000_000_000_000)),
        "w" => Some((1, 0x002, 604_800_000_000_000)),
        "d" => Some((2, 0x004, 86_400_000_000_000)),
        "h" => Some((3, 0x008, 3_600_000_000_000)),
        "m" => Some((4, 0x010, 60_000_000_000)),
        "s" => Some((5, 0x020, 1_000_000_000)),
        "ms" => Some((6, 0x040, 1_000_000)),
        "us" => Some((7, 0x080, 1_000)),
        "ns" => Some((8, 0x100, 1)),
        _ => None,
    }
}

pub(crate) fn parse_prometheus_duration_literal(value: &str) -> Option<i64> {
    let mut rest = value;
    let mut parsed_chunk = false;
    let mut previous_unit_order = None;
    let mut total_ns = 0_i128;

    while !rest.is_empty() {
        let amount_len = rest.bytes().take_while(u8::is_ascii_digit).count();
        if amount_len == 0 {
            return None;
        }
        let amount = rest.get(..amount_len)?.parse::<i128>().ok()?;
        rest = rest.get(amount_len..)?;

        let unit_len = rest.bytes().take_while(u8::is_ascii_alphabetic).count();
        let (unit_order, _unit_bit, multiplier) = duration_unit(rest.get(..unit_len)?)?;
        rest = rest.get(unit_len..)?;
        if previous_unit_order.is_some_and(|previous| unit_order <= previous) {
            return None;
        }

        let chunk_ns = amount.checked_mul(multiplier)?;
        total_ns = total_ns.checked_add(chunk_ns)?;
        previous_unit_order = Some(unit_order);
        parsed_chunk = true;
    }

    if !parsed_chunk {
        return None;
    }
    i64::try_from(total_ns).ok()
}

pub(crate) fn format_decimal_ratio(numerator: u128, denominator: u128) -> String {
    let whole = numerator / denominator;
    let mut remainder = numerator % denominator;
    if remainder == 0 {
        return whole.to_string();
    }

    let mut decimals = String::new();
    for _ in 0..9 {
        if remainder == 0 {
            break;
        }
        remainder *= 10;
        let digit = u8::try_from(remainder / denominator).expect("decimal digit is less than 10");
        decimals.push(char::from(b'0' + digit));
        remainder %= denominator;
    }
    while decimals.ends_with('0') {
        decimals.pop();
    }
    format!("{whole}.{decimals}")
}

pub(crate) fn parse_bytes_literal(value: &str) -> Option<ByteSize> {
    let unit_start = value
        .find(|ch: char| ch.is_ascii_alphabetic())
        .unwrap_or(value.len());
    let amount = value[..unit_start].parse::<f64>().ok()?;
    if !amount.is_finite() || amount < 0.0 {
        return None;
    }
    let multiplier = bytes_unit_multiplier(&value[unit_start..])?;
    Some(ByteSize::from_bytes_f64(amount * multiplier))
}

/// The size units that the `LogQL` grammar itself admits.
///
/// The table is here and not in `crabka_units::parse::byte_size` because Loki
/// matches these units case-sensitively. Loki accepts `KiB`, `kB`, `KB`, and
/// `MB`, and it rejects `kib` and `mb`. The shared parser is case-insensitive,
/// and its use would widen the query language that this crate is a compatible
/// front-end for.
fn bytes_unit_multiplier(unit: &str) -> Option<f64> {
    match unit {
        "" | "B" => Some(1.0),
        "kB" | "KB" => Some(1_000.0),
        "MB" => Some(1_000_000.0),
        "GB" => Some(1_000_000_000.0),
        "TB" => Some(1_000_000_000_000.0),
        "KiB" => Some(1024.0),
        "MiB" => Some(1_048_576.0),
        "GiB" => Some(1_073_741_824.0),
        "TiB" => Some(1_099_511_627_776.0),
        _ => None,
    }
}

impl fmt::Display for QuotedChar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "'{}'", self.0)
    }
}

#[cfg(test)]
mod tests {
    use crabka_units::{ByteSize, convert::ByteSizeExt};

    use super::{
        QuotedChar, duration_unit, format_decimal_ratio, is_ident_start, parse_bytes_literal,
        parse_prometheus_duration_literal,
    };

    #[test]
    fn ident_start_accepts_logql_identifier_prefixes() {
        for ch in ['_', ':', '.', 'a', 'Z'] {
            assert!(is_ident_start(ch), "{ch:?} should start an identifier");
        }
        assert!(!is_ident_start('0'));
        assert!(!is_ident_start('-'));
    }

    #[test]
    fn duration_units_cover_every_supported_unit() {
        let units = [
            ("y", (0, 0x001, 31_536_000_000_000_000)),
            ("w", (1, 0x002, 604_800_000_000_000)),
            ("d", (2, 0x004, 86_400_000_000_000)),
            ("h", (3, 0x008, 3_600_000_000_000)),
            ("m", (4, 0x010, 60_000_000_000)),
            ("s", (5, 0x020, 1_000_000_000)),
            ("ms", (6, 0x040, 1_000_000)),
            ("us", (7, 0x080, 1_000)),
            ("ns", (8, 0x100, 1)),
        ];

        for (unit, expected) in units {
            assert_eq!(duration_unit(unit), Some(expected), "unit {unit}");
        }
        assert_eq!(duration_unit("fortnight"), None);
    }

    #[test]
    fn prometheus_duration_literals_parse_long_and_short_units() {
        assert_eq!(
            parse_prometheus_duration_literal("1y2w3d4h5m6s7ms8us9ns"),
            Some(
                31_536_000_000_000_000
                    + 2 * 604_800_000_000_000
                    + 3 * 86_400_000_000_000
                    + 4 * 3_600_000_000_000
                    + 5 * 60_000_000_000
                    + 6 * 1_000_000_000
                    + 7 * 1_000_000
                    + 8 * 1_000
                    + 9
            )
        );
        assert_eq!(parse_prometheus_duration_literal("1us"), Some(1_000));
        assert_eq!(parse_prometheus_duration_literal("1ns"), Some(1));
        assert_eq!(parse_prometheus_duration_literal("1m1h"), None);
        assert_eq!(parse_prometheus_duration_literal(""), None);
    }

    #[test]
    fn decimal_ratios_stop_at_nine_fractional_digits() {
        assert_eq!(format_decimal_ratio(1, 2), "0.5");
        assert_eq!(format_decimal_ratio(1, 3), "0.333333333");
        assert_eq!(format_decimal_ratio(1_234, 1_000), "1.234");
    }

    #[test]
    fn bytes_literals_cover_decimal_binary_and_invalid_amounts() {
        use assert2::{assert, check};

        for (literal, expected) in [
            ("0B", 0.0),
            ("2GB", 2_000_000_000.0),
            ("3TB", 3_000_000_000_000.0),
            ("4KiB", 4_096.0),
            ("5GiB", 5_368_709_120.0),
            ("6TiB", 6_597_069_766_656.0),
        ] {
            check!(
                parse_bytes_literal(literal) == Some(ByteSize::from_bytes_f64(expected)),
                "{literal}"
            );
        }
        assert!(parse_bytes_literal("-1B").is_none());
    }

    #[test]
    fn quoted_char_display_wraps_character() {
        assert_eq!(QuotedChar('"').to_string(), "'\"'");
        assert_eq!(QuotedChar('x').to_string(), "'x'");
    }
}
