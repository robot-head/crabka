//! Parses the `--metrics-scrape-targets` CLI value:
//! `id:host:port,id:host:port,...` into a list of `ScrapeTarget`s.
//! Empty input is fine (scraper disabled). Malformed entries return
//! a typed error rather than panicking.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScrapeTarget {
    pub broker_id: i32,
    pub addr: String, // host:port; resolved at scrape time
}

#[derive(Debug, thiserror::Error)]
pub enum TargetParseError {
    #[error("malformed entry `{0}` (expected `id:host:port`)")]
    Malformed(String),
    #[error("invalid broker id in `{0}`")]
    BadId(String),
}

pub fn parse_targets(spec: &str) -> Result<Vec<ScrapeTarget>, TargetParseError> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Ok(Vec::new());
    }
    spec.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|entry| {
            let (id_str, addr) = entry
                .split_once(':')
                .ok_or_else(|| TargetParseError::Malformed(entry.to_string()))?;
            // After splitting on the first `:`, `addr` is `host:port`.
            if !addr.contains(':') {
                return Err(TargetParseError::Malformed(entry.to_string()));
            }
            let broker_id: i32 = id_str
                .parse()
                .map_err(|_| TargetParseError::BadId(entry.to_string()))?;
            Ok(ScrapeTarget {
                broker_id,
                addr: addr.to_string(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_returns_empty_vec() {
        assert!(parse_targets("").unwrap().is_empty());
        assert!(parse_targets("   ").unwrap().is_empty());
    }

    #[test]
    fn well_formed_entries_parse() {
        let out = parse_targets("1:broker1:9100,2:broker2:9100,3:broker3:9100").unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].broker_id, 1);
        assert_eq!(out[0].addr, "broker1:9100");
        assert_eq!(out[2].broker_id, 3);
        assert_eq!(out[2].addr, "broker3:9100");
    }

    #[test]
    fn malformed_entry_errors() {
        let err = parse_targets("nope").unwrap_err();
        assert!(matches!(err, TargetParseError::Malformed(_)));
        let err = parse_targets("1:host_without_port").unwrap_err();
        assert!(matches!(err, TargetParseError::Malformed(_)));
        let err = parse_targets("abc:host:9100").unwrap_err();
        assert!(matches!(err, TargetParseError::BadId(_)));
    }
}
