//! Per-log-dir online/offline status tracking: the broker-side half of
//! KIP-113 offline dirs.
//!
//! A configured log directory can fail the startup writability probe.
//! The mount point can be gone, the filesystem can be remounted
//! read-only, or an operator typo can flip a permission. The broker then
//! keeps booting against the dirs that *did* probe healthy and records
//! the failure on the [`LogDirRegistry`]. Three things follow:
//!
//! 1. [`crate::handlers::describe_log_dirs`] reports the offline dir
//!    with `error_code = KAFKA_STORAGE_ERROR` so `kafka-log-dirs
//!    --describe` matches the JVM behavior.
//! 2. The broker gives JBOD placement
//!    ([`crate::log_dir::place_partition_dir`]) only the online subset,
//!    so newly materialized partitions never land on an offline dir.
//! 3. Runtime write/fsync failures flip a dir from online to offline.
//!    `crate::partition_writer::flag_storage_failure` calls
//!    [`LogDirRegistry::mark_offline`] on any `LogError::Io` from a
//!    partition mutation. The broker then refuses a disk that dies under
//!    live traffic, and a broker restart is not necessary.
//!
//! The broker wires up both startup probing and runtime offline flips.
//! The registry is a shared `DashMap`, so every handler, the heartbeat
//! client, and JBOD placement all see a flip immediately. The heartbeat
//! client reports offline dir UUIDs to the controller.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use dashmap::DashMap;

/// Sentinel filename written into each log dir at startup to verify the
/// dir is writable. The broker creates the file, fsyncs it, then removes
/// it. The file is absent in steady state. The probe is similar to
/// Apache Kafka's `meta.properties` probe, and it does not collide with
/// that file's role.
const PROBE_FILENAME: &str = ".crabka-write-probe";

/// Per-dir health snapshot. `None` means online. `Some(reason)` means
/// the startup probe failed with that human-readable reason.
type Status = Option<String>;

/// Shared, lock-free per-log-dir status table. Cloning the `Arc` is
/// cheap. The handlers, the supervisor, and placement all read through
/// the same table, so every consumer sees a runtime offline flip
/// immediately.
#[derive(Clone, Default)]
pub struct LogDirRegistry {
    inner: Arc<DashMap<PathBuf, Status>>,
}

impl LogDirRegistry {
    /// Probe every entry in `log_dirs` and build a registry. A dir
    /// probes online if the broker can create the directory when it is
    /// missing, write a small sentinel, fsync it, and remove it
    /// without error. Anything else marks the dir offline with the
    /// underlying error message attached.
    ///
    /// The probe is intentionally synchronous. `Broker::start` runs it
    /// before any handler accepts traffic, so a short block per dir is
    /// acceptable.
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
    /// offline. A dir that the broker never probed returns `false`, so
    /// a stale path in operator config does not accidentally fail
    /// every produce.
    #[must_use]
    pub fn is_offline(&self, dir: &Path) -> bool {
        self.inner
            .get(dir)
            .is_some_and(|entry| entry.value().is_some())
    }

    /// Offline dirs paired with their probe-failure reason.
    /// `DescribeLogDirs` uses them to fill
    /// `error_code = KAFKA_STORAGE_ERROR`. A structured offline-reason
    /// log line will use them later.
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
    /// offline. JBOD placement uses this so new partitions never land
    /// on a known-bad dir. Returns the unfiltered list when every
    /// entry is offline. Placement treats this as a hard failure to
    /// materialize and raises `KAFKA_STORAGE_ERROR` rather than
    /// silently using an offline dir.
    #[must_use]
    pub fn online_subset(&self, log_dirs: &[PathBuf]) -> Vec<PathBuf> {
        log_dirs
            .iter()
            .filter(|d| !self.is_offline(d))
            .cloned()
            .collect()
    }

    /// Runtime offline-flip: mark `dir` offline with `reason` because a
    /// live write or fsync to it just failed. This function is
    /// idempotent. A call on an already-offline dir is a no-op, and the
    /// original reason stands. A call on a dir that was never probed
    /// inserts a fresh offline entry. That is correct for partitions
    /// materialized on a dir the operator added after broker start. The
    /// broker does not support that yet, but the registry shape allows
    /// it.
    ///
    /// Returns `true` when the call flipped the dir, that is when the
    /// dir was previously online or unknown. Use the return value to
    /// log the transition exactly once.
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
/// failure so the registry can show it to operators.
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
    use assert2::{assert, check};
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn probe_writable_tempdir_is_online() {
        let tmp = tempdir().unwrap();
        let reg = LogDirRegistry::probe(&[tmp.path().to_path_buf()]);
        check!(!reg.is_offline(tmp.path()));
        check!(reg.offline().is_empty());
        check!(reg.online_subset(&[tmp.path().to_path_buf()]) == vec![tmp.path().to_path_buf()]);
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

    /// The probe must leave nothing behind. It removes the sentinel
    /// file after a successful round-trip. This test catches the
    /// regression where `log_dir::scan` later misparses a stray
    /// `.crabka-write-probe` as a partition directory.
    #[test]
    fn probe_cleans_up_sentinel_on_success() {
        let tmp = tempdir().unwrap();
        let _ = LogDirRegistry::probe(&[tmp.path().to_path_buf()]);
        assert!(!tmp.path().join(PROBE_FILENAME).exists());
    }

    /// A path that the broker cannot create must be marked offline
    /// with a reason string. A regular file in the way is one such
    /// case. The probe must not panic and must not kill the
    /// probe-builder for siblings.
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

    /// One bad dir must not change a sibling-good dir's status. The
    /// startup probe builds the registry from a list. The JBOD broker
    /// exists because *some* dirs can keep serving while others are
    /// gone.
    #[test]
    fn probe_one_offline_does_not_take_out_siblings() {
        let tmp = tempdir().unwrap();
        let good = tmp.path().join("good");
        let blocker = tmp.path().join("bad");
        std::fs::write(&blocker, b"file blocking the path").unwrap();
        let reg = LogDirRegistry::probe(&[good.clone(), blocker.clone()]);
        check!(!reg.is_offline(&good));
        check!(reg.is_offline(&blocker));
        check!(reg.online_subset(&[good.clone(), blocker]) == vec![good]);
    }

    /// Dirs the broker never probed report `is_offline = false`. This
    /// matches the registry semantics: known offline is bad, and
    /// everything else is assumed good. The alternative would block
    /// any newly added dir until the broker restarts.
    #[test]
    fn unknown_dir_is_not_offline() {
        let reg = LogDirRegistry::default();
        assert!(!reg.is_offline(Path::new("/never/probed/anywhere")));
    }

    /// Runtime flip on a previously-online dir. The registry
    /// transitions `None` → `Some(reason)` and `is_offline` returns
    /// `true`. Then `offline()` includes the new entry, and
    /// `online_subset` no longer contains the dir. `mark_offline`
    /// returns `true` to show that the transition happened.
    #[test]
    fn mark_offline_flips_online_dir_and_returns_true() {
        let tmp = tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let reg = LogDirRegistry::probe(std::slice::from_ref(&dir));
        assert!(!reg.is_offline(&dir));

        let flipped = reg.mark_offline(&dir, "EIO from segment fsync");
        assert!(flipped, "first mark_offline must flip and return true");

        check!(reg.is_offline(&dir));
        check!(reg.offline() == vec![(dir.clone(), "EIO from segment fsync".to_string())]);
        check!(reg.online_subset(std::slice::from_ref(&dir)).is_empty());
    }

    /// `mark_offline` is idempotent: a second call returns `false` and
    /// the original reason wins. Callers can then log the offline-flip
    /// exactly once per dir, even if a hundred partitions on the same
    /// dir all hit fsync errors at the same time.
    #[test]
    fn mark_offline_is_idempotent() {
        let tmp = tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let reg = LogDirRegistry::probe(std::slice::from_ref(&dir));
        let first = reg.mark_offline(&dir, "first reason");
        let second = reg.mark_offline(&dir, "second reason");
        check!(first, "first call must flip");
        check!(!second, "second call must be a no-op");
        check!(
            reg.offline()[0].1 == "first reason",
            "the original reason must win",
        );
    }

    /// `mark_offline` on a dir the broker never probed still records
    /// the entry. This helps when a partition was materialized on an
    /// unprobed dir, for example when the operator added the dir after
    /// start. The broker does not support that yet, but the registry
    /// allows it.
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

        let needles = [
            "LogDirRegistry",
            "offline_count",
            "debug reason",
            "crabka-debug-offline-dir",
        ];
        for needle in needles {
            assert!(
                rendered.contains(needle),
                "missing {needle:?} in rendered: {rendered}"
            );
        }
    }
}
