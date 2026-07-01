//! Per-log-dir online/offline status tracking — the broker-side half of
//! KIP-113's offline-dir story.
//!
//! When a configured log directory fails a startup writability probe
//! (mount-point gone, filesystem remounted read-only, permission flipped
//! on an operator typo), the broker keeps booting against the dirs that
//! *did* probe healthy and records the failure on the
//! [`LogDirRegistry`]. From there, three things follow:
//!
//! 1. [`crate::handlers::describe_log_dirs`] surfaces the offline dir
//!    with `error_code = KAFKA_STORAGE_ERROR` so `kafka-log-dirs
//!    --describe` matches the JVM behavior.
//! 2. JBOD placement
//!    ([`crate::log_dir::place_partition_dir`]) is fed only the online
//!    subset, so newly materialized partitions never land on an offline
//!    dir.
//! 3. Runtime write/fsync failures flip a dir online → offline mid-life:
//!    `crate::partition_writer::flag_storage_failure` calls
//!    [`LogDirRegistry::mark_offline`] on any `LogError::Io` from a
//!    partition mutation, so a disk that dies under live traffic is
//!    refused thereafter without restarting the broker.
//!
//! Both startup probing and runtime offline-flips are wired; the registry
//! is shared (`DashMap`) so a flip is visible immediately to every handler,
//! the heartbeat client (which reports offline dir UUIDs to the
//! controller), and JBOD placement.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;

/// Sentinel filename written into each log dir at startup to verify the
/// dir is writable. Created, fsynced, then removed; absent in steady
/// state. Matches Apache Kafka's `meta.properties` probe in spirit
/// without colliding with that file's role.
const PROBE_FILENAME: &str = ".crabka-write-probe";

/// Per-dir health snapshot — `None` means online, `Some(reason)` means
/// the startup probe failed with that human-readable reason.
type Status = Option<String>;

/// Shared, lock-free per-log-dir status table. Cloning the `Arc` is
/// cheap; every consumer (handler, supervisor, placement) reads through
/// the same table so a future runtime-offline flip is visible
/// immediately everywhere.
#[derive(Clone, Default)]
pub struct LogDirRegistry {
    inner: Arc<DashMap<PathBuf, Status>>,
}

impl LogDirRegistry {
    /// Probe every entry in `log_dirs` and build a registry. A dir
    /// probes online if the broker can create the directory (when
    /// missing), write a small sentinel, fsync it, and remove it
    /// without error. Anything else marks the dir offline with the
    /// underlying error message attached.
    ///
    /// Probing is intentionally synchronous: `Broker::start` runs it
    /// before any handler accepts traffic, so blocking briefly per dir
    /// is the right trade.
    #[must_use]
    pub fn probe(log_dirs: &[PathBuf]) -> Self {
        let inner: DashMap<PathBuf, Status> = DashMap::new();
        for dir in log_dirs {
            match probe_one(dir) {
                Ok(()) => {
                    inner.insert(dir.clone(), None);
                }
                Err(reason) => {
                    tracing::warn!(
                        log_dir = %dir.display(),
                        reason = %reason,
                        "log dir failed startup writability probe; marking offline",
                    );
                    inner.insert(dir.clone(), Some(reason));
                }
            }
        }
        Self {
            inner: Arc::new(inner),
        }
    }

    /// True when the dir has been registered AND is currently marked
    /// offline. An unknown dir (never probed) returns `false` so a
    /// stale path in operator config doesn't accidentally fail every
    /// produce.
    #[must_use]
    pub fn is_offline(&self, dir: &Path) -> bool {
        self.inner
            .get(dir)
            .is_some_and(|entry| entry.value().is_some())
    }

    /// Offline dirs paired with their probe-failure reason. Used by
    /// `DescribeLogDirs` to fill `error_code = KAFKA_STORAGE_ERROR` and
    /// (in the future) a structured offline-reason log line.
    #[must_use]
    pub fn offline(&self) -> Vec<(PathBuf, String)> {
        let mut out: Vec<(PathBuf, String)> = self
            .inner
            .iter()
            .filter_map(|entry| {
                entry
                    .value()
                    .as_ref()
                    .map(|reason| (entry.key().clone(), reason.clone()))
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Filter `log_dirs` down to the entries that are not currently
    /// offline. Used by JBOD placement so new partitions never land on
    /// a known-bad dir. Returns the unfiltered list when every entry
    /// is offline — callers (placement) treat this as a hard failure
    /// to materialize and the caller raises `KAFKA_STORAGE_ERROR`
    /// rather than silently using an offline dir.
    #[must_use]
    pub fn online_subset(&self, log_dirs: &[PathBuf]) -> Vec<PathBuf> {
        log_dirs
            .iter()
            .filter(|d| !self.is_offline(d))
            .cloned()
            .collect()
    }

    /// Runtime offline-flip: mark `dir` offline with `reason` because a
    /// live write / fsync to it just failed. Idempotent — calling this
    /// on an already-offline dir is a no-op (the original reason
    /// stands). Calling this on a dir that was never probed inserts a
    /// fresh offline entry, which is the right thing for partitions
    /// materialized on a dir the operator added after broker start
    /// (not supported yet, but the registry shape is friendly).
    ///
    /// Returns `true` when the call actually flipped the dir
    /// (previously online or unknown) — useful for logging the
    /// transition exactly once.
    pub fn mark_offline(&self, dir: &Path, reason: &str) -> bool {
        // `entry()` would short-circuit on Vacant, but the existing
        // entry's value is `Option<String>`; we want to flip `None` →
        // `Some(reason)` without overwriting a pre-existing
        // `Some(other_reason)`.
        let flipped = if let Some(mut entry) = self.inner.get_mut(dir) {
            if entry.value().is_some() {
                return false;
            }
            *entry.value_mut() = Some(reason.to_owned());
            true
        } else {
            self.inner
                .insert(dir.to_path_buf(), Some(reason.to_owned()));
            true
        };
        if flipped {
            tracing::error!(
                log_dir = %dir.display(),
                reason = %reason,
                "log dir flipped to OFFLINE at runtime; subsequent produce/fetch on partitions \
                 in this dir will return KAFKA_STORAGE_ERROR until broker restart",
            );
        }
        flipped
    }
}

impl std::fmt::Debug for LogDirRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let offline = self.offline();
        f.debug_struct("LogDirRegistry")
            .field("offline_count", &offline.len())
            .field("offline", &offline)
            .finish()
    }
}

/// Single-dir probe: `create_dir_all` → write a sentinel → `sync_data`
/// → remove. Returns the underlying error's display string on any
/// failure so the registry can surface it to operators.
fn probe_one(dir: &Path) -> Result<(), String> {
    use std::io::Write;

    std::fs::create_dir_all(dir).map_err(|e| format!("create_dir_all: {e}"))?;
    let probe_path = dir.join(PROBE_FILENAME);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&probe_path)
        .map_err(|e| format!("open probe: {e}"))?;
    file.write_all(b"crabka")
        .map_err(|e| format!("write probe: {e}"))?;
    // `sync_data` catches a remounted-read-only filesystem that lets
    // the write buffer succeed but rejects the actual flush. Without
    // it, a r/o-remount only surfaces on the next segment fsync — far
    // too late for the JBOD broker to refuse traffic gracefully.
    file.sync_data().map_err(|e| format!("sync probe: {e}"))?;
    drop(file);
    std::fs::remove_file(&probe_path).map_err(|e| format!("remove probe: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use tempfile::tempdir;

    #[test]
    fn probe_writable_tempdir_is_online() {
        let tmp = tempdir().unwrap();
        let reg = LogDirRegistry::probe(&[tmp.path().to_path_buf()]);
        assert!(
            (
                reg.is_offline(tmp.path()),
                reg.offline(),
                reg.online_subset(&[tmp.path().to_path_buf()]),
            ) == (false, Vec::new(), vec![tmp.path().to_path_buf()])
        );
    }

    #[test]
    fn probe_creates_missing_dir() {
        let tmp = tempdir().unwrap();
        let nested = tmp.path().join("nested").join("brand-new");
        assert!(!nested.exists());
        let reg = LogDirRegistry::probe(std::slice::from_ref(&nested));
        assert!(!reg.is_offline(&nested));
        assert!(nested.is_dir(), "probe should have created the dir");
    }

    /// Probe must leave nothing behind — the sentinel file is removed
    /// after a successful round-trip. Catches the regression where a
    /// stray `.crabka-write-probe` would later be misparsed as a
    /// partition directory by `log_dir::scan`.
    #[test]
    fn probe_cleans_up_sentinel_on_success() {
        let tmp = tempdir().unwrap();
        let _ = LogDirRegistry::probe(&[tmp.path().to_path_buf()]);
        assert!(!tmp.path().join(PROBE_FILENAME).exists());
    }

    /// A path that can't be created (a regular file is in the way)
    /// must be marked offline with a reason string — not panic or
    /// kill the probe-builder for siblings.
    #[test]
    fn probe_path_blocked_by_file_is_offline() {
        let tmp = tempdir().unwrap();
        let blocker = tmp.path().join("blocker");
        std::fs::write(&blocker, b"i am not a directory").unwrap();
        let reg = LogDirRegistry::probe(std::slice::from_ref(&blocker));
        assert!(reg.is_offline(&blocker));
        // The reason string is OS-dependent, so pin (path, reason-is-empty)
        // pairs instead of the raw message.
        let offline: Vec<(PathBuf, bool)> = reg
            .offline()
            .iter()
            .map(|(path, reason)| (path.clone(), reason.is_empty()))
            .collect();
        assert!(
            offline == vec![(blocker, false)],
            "offline entry must carry a non-empty reason",
        );
    }

    /// One bad dir must not poison a sibling-good dir's status. The
    /// startup probe builds the registry from a list, and the JBOD
    /// broker's whole reason for existing is that *some* dirs can
    /// keep serving while others are gone.
    #[test]
    fn probe_one_offline_does_not_take_out_siblings() {
        let tmp = tempdir().unwrap();
        let good = tmp.path().join("good");
        let blocker = tmp.path().join("bad");
        std::fs::write(&blocker, b"file blocking the path").unwrap();
        let reg = LogDirRegistry::probe(&[good.clone(), blocker.clone()]);
        assert!(
            (
                reg.is_offline(&good),
                reg.is_offline(&blocker),
                reg.online_subset(&[good.clone(), blocker]),
            ) == (false, true, vec![good])
        );
    }

    /// Unknown dirs (never probed) report `is_offline = false`. This
    /// matches the registry's "known offline = bad, everything else =
    /// assume good" semantics; the alternative would block any newly-
    /// added dir until the broker restarts.
    #[test]
    fn unknown_dir_is_not_offline() {
        let reg = LogDirRegistry::default();
        assert!(!reg.is_offline(Path::new("/never/probed/anywhere")));
    }

    /// Runtime flip on a previously-online dir: registry transitions
    /// `None` → `Some(reason)`, `is_offline` returns `true`,
    /// `offline()` includes the new entry, and `online_subset` no
    /// longer contains the dir. `mark_offline` returns `true` to
    /// signal the actual transition happened.
    #[test]
    fn mark_offline_flips_online_dir_and_returns_true() {
        let tmp = tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let reg = LogDirRegistry::probe(std::slice::from_ref(&dir));
        assert!(!reg.is_offline(&dir));

        let flipped = reg.mark_offline(&dir, "EIO from segment fsync");
        assert!(flipped, "first mark_offline must flip and return true");

        assert!(
            (
                reg.is_offline(&dir),
                reg.offline(),
                reg.online_subset(std::slice::from_ref(&dir)),
            ) == (
                true,
                vec![(dir.clone(), "EIO from segment fsync".to_string())],
                Vec::new(),
            )
        );
    }

    /// `mark_offline` is idempotent: a second call returns `false` and
    /// the original reason wins. Lets callers log the offline-flip
    /// exactly once per dir even if a hundred partitions on the same
    /// dir all hit fsync errors simultaneously.
    #[test]
    fn mark_offline_is_idempotent() {
        let tmp = tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let reg = LogDirRegistry::probe(std::slice::from_ref(&dir));
        let first = reg.mark_offline(&dir, "first reason");
        let second = reg.mark_offline(&dir, "second reason");
        assert!(
            (first, second, reg.offline()[0].1.clone())
                == (true, false, "first reason".to_string()),
            "first call must flip; second must be a no-op with the original reason winning",
        );
    }

    /// Marking an unknown dir (never probed) offline still records
    /// the entry — useful when a partition was materialized on a dir
    /// that the broker hasn't probed (operator added the dir
    /// post-start; not supported yet but the registry is
    /// future-proofed).
    #[test]
    fn mark_offline_on_unknown_dir_inserts_entry() {
        let reg = LogDirRegistry::default();
        let ghost = Path::new("/tmp/crabka-ghost-dir");
        assert!(reg.mark_offline(ghost, "synthetic test"));
        assert!(reg.is_offline(ghost));
    }

    #[test]
    fn debug_includes_offline_count_and_entries() {
        let reg = LogDirRegistry::default();
        let ghost = Path::new("/tmp/crabka-debug-offline-dir");
        assert!(reg.mark_offline(ghost, "debug reason"));

        let rendered = format!("{reg:?}");

        let contains = (
            rendered.contains("LogDirRegistry"),
            rendered.contains("offline_count"),
            rendered.contains("debug reason"),
            rendered.contains("crabka-debug-offline-dir"),
        );
        assert!(contains == (true, true, true, true), "rendered: {rendered}");
    }
}
