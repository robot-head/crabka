use std::fmt;

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
        "y" => Some((0, 1 << 0, 31_536_000_000_000_000)),
        "w" => Some((1, 1 << 1, 604_800_000_000_000)),
        "d" => Some((2, 1 << 2, 86_400_000_000_000)),
        "h" => Some((3, 1 << 3, 3_600_000_000_000)),
        "m" => Some((4, 1 << 4, 60_000_000_000)),
        "s" => Some((5, 1 << 5, 1_000_000_000)),
        "ms" => Some((6, 1 << 6, 1_000_000)),
        "us" => Some((7, 1 << 7, 1_000)),
        "ns" => Some((8, 1 << 8, 1)),
        _ => None,
    }
}

pub(crate) fn parse_prometheus_duration_literal(value: &str) -> Option<i64> {
    let mut pos = 0;
    let mut parsed_chunk = false;
    let mut previous_unit_order = None;
    let mut seen_units = 0_u16;
    let mut total_ns = 0_i128;

    while pos < value.len() {
        let value_start = pos;
        while value.as_bytes().get(pos).is_some_and(u8::is_ascii_digit) {
            pos += 1;
        }
        if pos == value_start {
            return None;
        }
        let amount = value[value_start..pos].parse::<i128>().ok()?;

        let unit_start = pos;
        while value
            .as_bytes()
            .get(pos)
            .is_some_and(u8::is_ascii_alphabetic)
        {
            pos += 1;
        }
        let (unit_order, unit_bit, multiplier) = duration_unit(&value[unit_start..pos])?;
        if seen_units & unit_bit != 0 {
            return None;
        }
        if previous_unit_order.is_some_and(|previous| unit_order <= previous) {
            return None;
        }

        let chunk_ns = amount.checked_mul(multiplier)?;
        total_ns = total_ns.checked_add(chunk_ns)?;
        seen_units |= unit_bit;
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
    while remainder != 0 && decimals.len() < 9 {
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

pub(crate) fn parse_bytes_literal(value: &str) -> Option<f64> {
    let unit_start = value
        .find(|ch: char| ch.is_ascii_alphabetic())
        .unwrap_or(value.len());
    let amount = value[..unit_start].parse::<f64>().ok()?;
    if !amount.is_finite() || amount < 0.0 {
        return None;
    }
    let multiplier = bytes_unit_multiplier(&value[unit_start..])?;
    Some(amount * multiplier)
}

fn bytes_unit_multiplier(unit: &str) -> Option<f64> {
    match unit {
        "" | "B" => Some(1.0),
        "kB" | "KB" => Some(1_000.0),
        "MB" => Some(1_000_000.0),
        "GB" => Some(1_000_000_000.0),
        "TB" => Some(1_000_000_000_000.0),
        "KiB" => Some(1024.0),
        "MiB" => Some(1024.0 * 1024.0),
        "GiB" => Some(1024.0 * 1024.0 * 1024.0),
        "TiB" => Some(1024.0 * 1024.0 * 1024.0 * 1024.0),
        _ => None,
    }
}

impl fmt::Display for QuotedChar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "'{}'", self.0)
    }
}
