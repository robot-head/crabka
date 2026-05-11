//! Per-partition directory layout: `<log_dir>/<topic>-<partition>/`.
//! Mirrors the Apache Kafka convention so `crabka-log` can open existing
//! Kafka log directories byte-compatibly.

// Helpers are wired into `Broker::start` in a later batch.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use crate::error::BrokerError;

/// Build the directory path for a (topic, partition).
#[must_use]
pub fn partition_dir(log_dir: &Path, topic: &str, partition: i32) -> PathBuf {
    log_dir.join(format!("{topic}-{partition}"))
}

/// Parse `<topic>-<partition>` from a directory name.
/// Returns `None` if the name doesn't match the pattern.
#[must_use]
pub fn parse_partition_dir(name: &str) -> Option<(String, i32)> {
    let (topic, part) = name.rsplit_once('-')?;
    if topic.is_empty() || topic.ends_with('-') {
        // Empty topic or trailing `-` in the topic indicates a malformed
        // name like "-0" or "foo--1" (which would otherwise parse the
        // tail as a positive partition number).
        return None;
    }
    let partition = part.parse::<i32>().ok()?;
    if partition < 0 {
        return None;
    }
    Some((topic.to_string(), partition))
}

/// Walk `log_dir` and return every `(topic, partition)` whose directory
/// exists. Used at broker startup to repopulate the metadata image +
/// partition registry from whatever was on disk last run.
pub fn scan(log_dir: &Path) -> Result<Vec<(String, i32)>, BrokerError> {
    if !log_dir.exists() {
        std::fs::create_dir_all(log_dir)?;
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(log_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Ok(name) = entry.file_name().into_string() else {
            continue; // non-UTF-8 dir name: ignore
        };
        if let Some((topic, partition)) = parse_partition_dir(&name) {
            out.push((topic, partition));
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn round_trip_partition_dir() {
        let p = partition_dir(Path::new("/tmp"), "foo", 7);
        let name = p
            .file_name()
            .expect("path has a file name")
            .to_str()
            .expect("file name is utf-8");
        assert_eq!(parse_partition_dir(name), Some(("foo".to_string(), 7)));
    }

    #[test]
    fn rejects_negative_partition() {
        assert_eq!(parse_partition_dir("foo--1"), None);
    }

    #[test]
    fn rejects_no_dash() {
        assert_eq!(parse_partition_dir("foo"), None);
    }

    #[test]
    fn handles_topic_with_dashes() {
        // Topic names can themselves contain hyphens; rsplit takes the last.
        assert_eq!(
            parse_partition_dir("my-cool-topic-3"),
            Some(("my-cool-topic".to_string(), 3))
        );
    }

    #[test]
    fn scan_creates_dir_when_missing() {
        let dir = tempdir().expect("tempdir");
        let log_dir = dir.path().join("does-not-exist");
        let out = scan(&log_dir).expect("scan ok");
        assert!(out.is_empty());
        assert!(log_dir.exists());
    }

    #[test]
    fn scan_returns_existing_partitions() {
        let dir = tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join("foo-0")).expect("mkdir foo-0");
        std::fs::create_dir(dir.path().join("foo-1")).expect("mkdir foo-1");
        std::fs::create_dir(dir.path().join("bar-0")).expect("mkdir bar-0");
        std::fs::create_dir(dir.path().join("not_a_partition")).expect("mkdir other");
        let mut out = scan(dir.path()).expect("scan ok");
        out.sort();
        assert_eq!(
            out,
            vec![("bar".into(), 0), ("foo".into(), 0), ("foo".into(), 1),]
        );
    }
}
