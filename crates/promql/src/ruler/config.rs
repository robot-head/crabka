use std::collections::BTreeMap;

use crate::PromqlError;

pub(super) fn yaml_string_map(value: &serde_yaml::Value, key: &str) -> BTreeMap<String, String> {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_mapping)
        .map(|mapping| {
            mapping
                .iter()
                .filter_map(|(key, value)| Some((key.as_str()?, value.as_str()?)))
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// Parse a Prometheus duration for the given rule field, surfacing malformed
/// values as a hard error to the caller. A missing field is `0` (no duration);
/// an empty, negative, or otherwise unparseable value is rejected rather than
/// silently coerced to `0` (which would make `for`/`interval` fire immediately).
pub(super) fn yaml_duration_ms(value: &serde_yaml::Value, key: &str) -> Result<i64, PromqlError> {
    match yaml_optional_string(value, key) {
        Some(duration) => parse_duration_ms(&duration),
        None => Ok(0),
    }
}

/// Parse a Prometheus duration string into milliseconds.
///
/// Supports the full Prometheus unit set (`ms`, `s`, `m`, `h`, `d`, `w`, `y`)
/// and compound durations such as `1h30m`. Mirrors the conformance harness'
/// `parse_duration_ms`. Empty, negative, or unparseable input is a hard error.
pub(super) fn parse_duration_ms(duration: &str) -> Result<i64, PromqlError> {
    let src = duration.trim();
    if src.is_empty() {
        return Err(PromqlError::Exec("empty duration".into()));
    }
    if src == "0" {
        return Ok(0);
    }
    if src.starts_with('-') {
        return Err(PromqlError::Exec(format!("negative duration `{src}`")));
    }

    let mut total_ms = 0_i64;
    let mut index = 0;
    let bytes = src.as_bytes();

    while index < bytes.len() {
        let start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if start == index {
            return Err(PromqlError::Exec(format!("invalid duration `{src}`")));
        }
        let amount = src[start..index]
            .parse::<i64>()
            .map_err(|err| PromqlError::Exec(format!("invalid duration amount `{src}`: {err}")))?;
        let unit_start = index;
        while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
            index += 1;
        }
        let unit = &src[unit_start..index];
        let multiplier = match unit {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            "w" => 604_800_000,
            "y" => 31_536_000_000,
            _ => return Err(PromqlError::Exec(format!("invalid duration unit `{unit}`"))),
        };
        total_ms += amount
            .checked_mul(multiplier)
            .ok_or_else(|| PromqlError::Exec(format!("duration overflow `{src}`")))?;
    }

    Ok(total_ms)
}

pub(super) fn yaml_optional_string(value: &serde_yaml::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_str)
        .map(str::to_string)
}

pub(super) fn yaml_required_string(
    value: &serde_yaml::Value,
    key: &str,
) -> Result<String, PromqlError> {
    yaml_optional_string(value, key)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| PromqlError::Exec(format!("recording rule must contain a non-empty {key}")))
}

pub(super) fn stable_hash_parts(parts: &[&str]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = OFFSET;
    for part in parts {
        for byte in part.as_bytes().iter().copied().chain(std::iter::once(0)) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(PRIME);
        }
    }
    hash
}
