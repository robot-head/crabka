//! Segment filename parsing. Kafka names segments by 20-digit
//! zero-padded base offset, with `.log`, `.index`, `.timeindex` extensions.

use std::path::Path;

use crate::error::LogError;

pub const FILENAME_DIGITS: usize = 20;

/// `0` → `"00000000000000000000"`. `1847` → `"00000000000000001847"`.
#[must_use]
pub fn format_base_offset(base_offset: i64) -> String {
    format!("{base_offset:020}")
}

/// Parse a `.log` filename and return its base offset.
/// `"00000000000000001847.log"` → `Ok(1847)`.
pub fn parse_log_filename(name: &str) -> Result<i64, LogError> {
    let stem = name
        .strip_suffix(".log")
        .ok_or_else(|| LogError::BadSegmentName(name.into()))?;
    if stem.len() != FILENAME_DIGITS {
        return Err(LogError::BadSegmentName(name.into()));
    }
    stem.parse::<i64>()
        .map_err(|_| LogError::BadSegmentName(name.into()))
}

pub fn log_path(dir: &Path, base_offset: i64) -> std::path::PathBuf {
    dir.join(format!("{}.log", format_base_offset(base_offset)))
}

pub fn index_path(dir: &Path, base_offset: i64) -> std::path::PathBuf {
    dir.join(format!("{}.index", format_base_offset(base_offset)))
}

pub fn timeindex_path(dir: &Path, base_offset: i64) -> std::path::PathBuf {
    dir.join(format!("{}.timeindex", format_base_offset(base_offset)))
}

pub fn txnindex_path(dir: &Path, base_offset: i64) -> std::path::PathBuf {
    dir.join(format!("{}.txnindex", format_base_offset(base_offset)))
}

/// Path to the per-partition `.leader-epoch-checkpoint` file.
pub fn leader_epoch_checkpoint_path(dir: &Path) -> std::path::PathBuf {
    dir.join("leader-epoch-checkpoint")
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    macro_rules! offset_case {
        ($name:ident, $offset:expr, $expected_filename:expr) => {
            #[test]
            fn $name() {
                let formatted = format_base_offset($offset);
                assert!(formatted == $expected_filename);
                let parsed = parse_log_filename(&format!("{formatted}.log")).unwrap();
                assert!(parsed == $offset);
            }
        };
    }

    offset_case!(zero, 0, "00000000000000000000");
    offset_case!(small, 1847, "00000000000000001847");
    // Plan had "00000000001000000000000" (23 chars) which is wrong:
    // `{:020}` zero-pads to 20 chars, and 1_000_000_000_000 is 13 digits,
    // so we expect 7 leading zeros + "1000000000000" = 20 chars total.
    offset_case!(large, 1_000_000_000_000, "00000001000000000000");

    #[test]
    fn rejects_non_log_extension() {
        assert!(parse_log_filename("00000000000000000000.index").is_err());
    }

    #[test]
    fn rejects_wrong_digit_count() {
        assert!(parse_log_filename("123.log").is_err());
        assert!(parse_log_filename("000000000000000001847.log").is_err()); // 21 digits
    }
}
