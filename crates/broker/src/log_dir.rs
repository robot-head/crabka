//! Per-partition directory layout: `<log_dir>/<topic>-<partition>/`.
//! Mirrors the Apache Kafka convention so `crabka-log` can open existing
//! Kafka log directories byte-compatibly.

use std::path::{Path, PathBuf};

use crate::error::BrokerError;

/// Suffix appended to a future-log partition directory while a
/// KIP-113 intra-broker move is in progress. The directory at
/// `<target_log_dir>/<topic>-<partition><FUTURE_SUFFIX>` accumulates
/// copied batches until the future log catches up and is renamed
/// in-place to `<topic>-<partition>`. Mirrors Apache Kafka's
/// `LogManager.FutureDirSuffix` so cp-kafka tooling expectations
/// (`kafka-log-dirs`) line up byte-for-byte on disk.
pub const FUTURE_SUFFIX: &str = "-future";

/// Build the directory path for a (topic, partition).
#[must_use]
pub fn partition_dir(log_dir: &Path, topic: &str, partition: i32) -> PathBuf {
    log_dir.join(format!("{topic}-{partition}"))
}

/// Build the future-log directory path for a (topic, partition) in
/// `log_dir`. Used by `AlterReplicaLogDirs` / KIP-113 — the future
/// log accumulates copied batches and is atomically renamed to the
/// canonical `<topic>-<partition>` on swap.
#[must_use]
pub fn future_partition_dir(log_dir: &Path, topic: &str, partition: i32) -> PathBuf {
    log_dir.join(format!("{topic}-{partition}{FUTURE_SUFFIX}"))
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

/// Parse a `<topic>-<partition>-future` directory name back to
/// `(topic, partition)`. Strips the [`FUTURE_SUFFIX`] then defers to
/// [`parse_partition_dir`] so the topic-name handling stays in one
/// place. Returns `None` if the name doesn't carry the suffix or the
/// stripped remainder isn't a valid partition directory.
#[must_use]
pub fn parse_future_partition_dir(name: &str) -> Option<(String, i32)> {
    let base = name.strip_suffix(FUTURE_SUFFIX)?;
    parse_partition_dir(base)
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

/// Walk `log_dir` and return every `(topic, partition)` whose
/// future-log directory (`<topic>-<partition>-future`) is present.
/// Used by `DescribeLogDirs` to surface in-progress KIP-113 moves
/// with `is_future_key=true`, and by broker startup to resume moves
/// that were interrupted by a crash.
pub fn scan_future(log_dir: &Path) -> Result<Vec<(String, i32)>, BrokerError> {
    if !log_dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(log_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if let Some((topic, partition)) = parse_future_partition_dir(&name) {
            out.push((topic, partition));
        }
    }
    out.sort();
    Ok(out)
}

/// Count the partition subdirectories (`<topic>-<partition>/`) directly
/// under `dir`. Used for least-loaded JBOD placement. A missing directory
/// counts as zero. Non-partition entries (e.g. `__cluster_metadata`,
/// stray files) are ignored.
#[must_use]
pub fn count_partitions(dir: &Path) -> usize {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return 0;
    };
    rd.filter_map(Result::ok)
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|name| parse_partition_dir(name).is_some())
        .count()
}

/// Resolve the directory a `(topic, partition)` should live in across a
/// JBOD set of `log_dirs` (KIP-113 placement), returning the full
/// `<dir>/<topic>-<partition>` path.
///
/// - If the partition directory already exists under one of `log_dirs`,
///   that existing location wins (idempotent — handles restart, recovery,
///   and concurrent re-materialization).
/// - Otherwise it is placed in the least-loaded directory (fewest
///   partition subdirs), ties broken by `log_dirs` order. This mirrors
///   Kafka's `LogManager` round-robin-by-count default.
///
/// `log_dirs` must be non-empty; the caller guarantees this via
/// [`crate::BrokerConfig::all_log_dirs`], which always includes the
/// primary `log_dir`.
#[must_use]
pub fn place_partition_dir(log_dirs: &[PathBuf], topic: &str, partition: i32) -> PathBuf {
    debug_assert!(!log_dirs.is_empty(), "log_dirs must be non-empty");
    let leaf = format!("{topic}-{partition}");

    // Existing location wins.
    for dir in log_dirs {
        let candidate = dir.join(&leaf);
        if candidate.exists() {
            return candidate;
        }
    }

    // Least-loaded placement, ties broken by order.
    let chosen = log_dirs
        .iter()
        .min_by_key(|dir| count_partitions(dir))
        .unwrap_or(&log_dirs[0]);
    chosen.join(&leaf)
}

/// Scan every directory in `log_dirs` and return each discovered
/// `(topic, partition, owning_dir)`. If the same partition appears in more
/// than one directory (a misconfiguration) the first occurrence wins and a
/// warning is logged. Results are sorted by `(topic, partition)`.
pub fn scan_all(log_dirs: &[PathBuf]) -> Result<Vec<(String, i32, PathBuf)>, BrokerError> {
    use std::collections::HashMap;
    let mut seen: HashMap<(String, i32), PathBuf> = HashMap::new();
    for dir in log_dirs {
        for (topic, partition) in scan(dir)? {
            match seen.entry((topic.clone(), partition)) {
                std::collections::hash_map::Entry::Occupied(existing) => {
                    tracing::warn!(
                        topic = %topic, partition,
                        first = %existing.get().display(),
                        duplicate = %dir.display(),
                        "partition present in multiple log dirs; ignoring duplicate"
                    );
                }
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(dir.clone());
                }
            }
        }
    }
    let mut out: Vec<(String, i32, PathBuf)> =
        seen.into_iter().map(|((t, p), dir)| (t, p, dir)).collect();
    out.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn round_trip_partition_dir() {
        let p = partition_dir(Path::new("/tmp"), "foo", 7);
        let name = p
            .file_name()
            .expect("path has a file name")
            .to_str()
            .expect("file name is utf-8");
        assert!(parse_partition_dir(name) == Some(("foo".to_string(), 7)));
    }

    #[test]
    fn rejects_negative_partition() {
        assert!(parse_partition_dir("foo--1") == None);
    }

    #[test]
    fn rejects_no_dash() {
        assert!(parse_partition_dir("foo") == None);
    }

    #[test]
    fn handles_topic_with_dashes() {
        // Topic names can themselves contain hyphens; rsplit takes the last.
        assert!(parse_partition_dir("my-cool-topic-3") == Some(("my-cool-topic".to_string(), 3)));
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
        assert!(out == vec![("bar".into(), 0), ("foo".into(), 0), ("foo".into(), 1),]);
    }

    #[test]
    fn count_partitions_ignores_non_partition_entries() {
        let dir = tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join("foo-0")).unwrap();
        std::fs::create_dir(dir.path().join("foo-1")).unwrap();
        std::fs::create_dir(dir.path().join("__cluster_metadata")).unwrap();
        std::fs::write(dir.path().join("bootstrap.json"), b"{}").unwrap();
        assert!(count_partitions(dir.path()) == 2);
    }

    #[test]
    fn count_partitions_missing_dir_is_zero() {
        let dir = tempdir().expect("tempdir");
        assert!(count_partitions(&dir.path().join("nope")) == 0);
    }

    #[test]
    fn place_reuses_existing_location() {
        let a = tempdir().unwrap();
        let b = tempdir().unwrap();
        let dirs = vec![a.path().to_path_buf(), b.path().to_path_buf()];
        // Pre-create the partition in the *second* dir.
        std::fs::create_dir(b.path().join("t-0")).unwrap();
        let placed = place_partition_dir(&dirs, "t", 0);
        assert!(placed == b.path().join("t-0"));
    }

    #[test]
    fn place_picks_least_loaded_then_order() {
        let a = tempdir().unwrap();
        let b = tempdir().unwrap();
        let dirs = vec![a.path().to_path_buf(), b.path().to_path_buf()];
        // Empty cluster: tie → first dir.
        assert!(place_partition_dir(&dirs, "t", 0) == a.path().join("t-0"));
        // Load `a` with two partitions; next placement should go to `b`.
        std::fs::create_dir(a.path().join("t-0")).unwrap();
        std::fs::create_dir(a.path().join("t-1")).unwrap();
        assert!(place_partition_dir(&dirs, "t", 2) == b.path().join("t-2"));
    }

    #[test]
    fn scan_all_merges_dirs_and_sorts() {
        let a = tempdir().unwrap();
        let b = tempdir().unwrap();
        std::fs::create_dir(a.path().join("foo-0")).unwrap();
        std::fs::create_dir(b.path().join("bar-1")).unwrap();
        let dirs = vec![a.path().to_path_buf(), b.path().to_path_buf()];
        let out = scan_all(&dirs).expect("scan_all ok");
        assert!(
            out == vec![
                ("bar".to_string(), 1, b.path().to_path_buf()),
                ("foo".to_string(), 0, a.path().to_path_buf()),
            ]
        );
    }

    #[test]
    fn future_partition_dir_round_trips() {
        let p = future_partition_dir(Path::new("/tmp"), "foo", 7);
        let name = p
            .file_name()
            .expect("path has a file name")
            .to_str()
            .expect("file name is utf-8");
        assert!(name == "foo-7-future");
        assert!(parse_future_partition_dir(name) == Some(("foo".to_string(), 7)));
    }

    #[test]
    fn parse_future_rejects_non_future_name() {
        // Plain partition dir has no `-future` suffix.
        assert!(parse_future_partition_dir("foo-7") == None);
        // Suffix present but the remainder isn't a partition dir.
        assert!(parse_future_partition_dir("garbage-future") == None);
    }

    #[test]
    fn scan_does_not_pick_up_future_dirs() {
        let dir = tempdir().unwrap();
        std::fs::create_dir(dir.path().join("foo-0")).unwrap();
        std::fs::create_dir(dir.path().join("foo-1-future")).unwrap();
        let out = scan(dir.path()).expect("scan ok");
        assert!(out == vec![("foo".into(), 0)]);
    }

    #[test]
    fn scan_future_returns_only_future_dirs() {
        let dir = tempdir().unwrap();
        std::fs::create_dir(dir.path().join("foo-0")).unwrap();
        std::fs::create_dir(dir.path().join("foo-1-future")).unwrap();
        std::fs::create_dir(dir.path().join("bar-3-future")).unwrap();
        let mut out = scan_future(dir.path()).expect("scan_future ok");
        out.sort();
        assert!(out == vec![("bar".into(), 3), ("foo".into(), 1)]);
    }

    #[test]
    fn scan_future_missing_dir_is_empty() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("nope");
        assert!(scan_future(&missing).expect("ok").is_empty());
    }

    #[test]
    fn count_partitions_ignores_future_dirs() {
        // KIP-113 placement should not see future-log dirs as load —
        // they are transient state belonging to an in-flight move.
        let dir = tempdir().unwrap();
        std::fs::create_dir(dir.path().join("foo-0")).unwrap();
        std::fs::create_dir(dir.path().join("foo-1-future")).unwrap();
        assert!(count_partitions(dir.path()) == 1);
    }

    #[test]
    fn scan_all_first_dir_wins_on_duplicate() {
        let a = tempdir().unwrap();
        let b = tempdir().unwrap();
        std::fs::create_dir(a.path().join("foo-0")).unwrap();
        std::fs::create_dir(b.path().join("foo-0")).unwrap();
        let dirs = vec![a.path().to_path_buf(), b.path().to_path_buf()];
        let out = scan_all(&dirs).expect("scan_all ok");
        assert!(out == vec![("foo".to_string(), 0, a.path().to_path_buf())]);
    }
}
