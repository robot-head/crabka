//! Continuous range-0 follower tailing for a node that does not host range 0.
//!
//! The loop keeps a local catalog store in step with the committed range-0 WAL
//! so read barriers on this node can be released. Its one non-obvious duty is
//! surviving a trim: the checkpointer prunes the WAL behind itself, and a
//! follower that falls behind the retained log start can never fetch its way
//! forward again. That case rebuilds from the newest checkpoint instead of
//! retrying a fetch that is now guaranteed to fail.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crabka_gres_substrate::{
    LiveCommittedEndSampler, LiveRecoveryConfig, ReadOnlyRange0Follower,
    checkpoint::CheckpointStore,
};
use crabka_pgkv::{FjallKv, MemKv, RestoreKv};

/// Delay before a rebuild that immediately follows another one. A trim landing
/// between two rebuilds is legitimate; a tight rebuild loop is not.
const REBUILD_BACKOFF_FLOOR: Duration = Duration::from_millis(250);
/// Ceiling for the doubling rebuild backoff.
const REBUILD_BACKOFF_CEILING: Duration = Duration::from_secs(30);
/// Directory-name prefix of every follower cache generation.
const FOLLOWER_STORE_PREFIX: &str = "r0-follower";

/// Open an empty local store for one follower generation.
///
/// The store holds nothing authoritative: it is rebuilt from range 0's latest
/// checkpoint plus the committed tail after it. It must start empty, because
/// restoring a checkpoint into a warm cache is rejected, and a warm cache left
/// unrestored would silently miss every record the checkpointer has already
/// trimmed out of the WAL.
///
/// Each generation gets its own directory: a rebuild opens the next generation
/// while the previous one is still serving reads, and only stops serving once
/// the swap has happened.
pub(crate) fn open_follower_store(
    cache_dir: Option<&Path>,
    generation: u64,
) -> std::io::Result<Arc<dyn RestoreKv>> {
    let Some(parent) = cache_dir else {
        return Ok(Arc::new(MemKv::default()));
    };
    let dir = follower_store_dir(parent, generation);
    if dir.exists() {
        std::fs::remove_dir_all(&dir)?;
    }
    std::fs::create_dir_all(&dir)?;
    Ok(Arc::new(FjallKv::open_cache(&dir).map_err(|error| {
        std::io::Error::other(format!("range-0 follower cache: {error:?}"))
    })?))
}

/// Delete every follower cache generation other than `keep`.
///
/// Called after a rebuild has swapped in `keep` and dropped its predecessor,
/// so the directories removed here back stores nothing reads through any more.
pub(crate) fn remove_other_follower_stores(cache_dir: Option<&Path>, keep: u64) {
    let Some(parent) = cache_dir else { return };
    let kept = follower_store_dir(parent, keep);
    let entries = match std::fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(error) => {
            tracing::debug!(%error, "range-0 follower cache sweep could not list the cache dir");
            return;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path == kept {
            continue;
        }
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(FOLLOWER_STORE_PREFIX)
            && let Err(error) = std::fs::remove_dir_all(&path)
        {
            tracing::debug!(%error, path = %path.display(), "stale range-0 follower cache left in place");
        }
    }
}

fn follower_store_dir(parent: &Path, generation: u64) -> PathBuf {
    parent.join(format!("{FOLLOWER_STORE_PREFIX}-{generation}"))
}

/// Everything the follower tail loop needs to keep tailing — and to rebuild
/// itself when the WAL is trimmed past what it has applied.
pub(crate) struct Range0FollowerTail {
    follower: ReadOnlyRange0Follower,
    config: LiveRecoveryConfig,
    end_sampler: Arc<LiveCommittedEndSampler>,
    checkpoints: Option<Arc<dyn CheckpointStore>>,
    cache_dir: Option<PathBuf>,
    poll_interval: Duration,
    refresh_poke: Arc<tokio::sync::Notify>,
    store_generation: u64,
    rebuilds: u64,
    consecutive_rebuilds: u32,
}

impl Range0FollowerTail {
    pub(crate) const fn new(
        follower: ReadOnlyRange0Follower,
        config: LiveRecoveryConfig,
        end_sampler: Arc<LiveCommittedEndSampler>,
        checkpoints: Option<Arc<dyn CheckpointStore>>,
        cache_dir: Option<PathBuf>,
        poll_interval: Duration,
        refresh_poke: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            follower,
            config,
            end_sampler,
            checkpoints,
            cache_dir,
            poll_interval,
            refresh_poke,
            store_generation: 0,
            rebuilds: 0,
            consecutive_rebuilds: 0,
        }
    }

    /// Tail the committed range-0 WAL until the process exits.
    pub(crate) async fn run(mut self) {
        loop {
            self.poll_once().await;
            // A catalog barrier pokes the refresh so waiters catch up
            // immediately instead of on the next periodic tick.
            wait_for_refresh(&self.refresh_poke, self.poll_interval).await;
        }
    }

    async fn poll_once(&mut self) {
        let applied = self.follower.tail().applied_offset();
        let end = match self.end_sampler.committed_end().await {
            Ok(end) => end,
            Err(error) => {
                tracing::warn!(%error, "range-0 follower end sample failed");
                return;
            }
        };
        if end <= applied {
            // Caught up with the committed end: whatever forced the last
            // rebuild is behind us, so the next one starts from no backoff.
            self.consecutive_rebuilds = 0;
            return;
        }
        match crabka_gres_substrate::read_live_committed_tail(&self.config, applied, end).await {
            Ok(items) => {
                self.consecutive_rebuilds = 0;
                for item in &items {
                    if self.follower.apply_committed(item).is_err() {
                        break;
                    }
                }
            }
            Err(error) => self.handle_read_failure(applied, &error).await,
        }
    }

    /// Separate the one unrecoverable failure from every retryable one.
    ///
    /// A trimmed WAL is the only failure retrying cannot fix: the frames the
    /// follower needs are gone, so the same fetch fails forever and this node's
    /// read barriers stall behind it. Everything else — a broker blip, a
    /// timeout, a topic that is momentarily unresolvable — keeps retrying from
    /// the same offset, because a needless rebuild throws away a warm store and
    /// skips frames the observer would otherwise have seen.
    async fn handle_read_failure(
        &mut self,
        applied: i64,
        error: &crabka_gres_substrate::SubstrateError,
    ) {
        match crabka_gres_substrate::live_wal_trimmed_past_applied(&self.config, applied).await {
            Ok(true) => self.rebuild_from_checkpoint(applied).await,
            Ok(false) => {
                tracing::warn!(%error, applied, "range-0 follower tail read failed");
            }
            Err(probe_error) => {
                tracing::warn!(
                    %error,
                    %probe_error,
                    applied,
                    "range-0 follower tail read failed and the retained log start could not be read"
                );
            }
        }
    }

    async fn rebuild_from_checkpoint(&mut self, applied: i64) {
        if self.consecutive_rebuilds > 0 {
            let backoff = rebuild_backoff(self.consecutive_rebuilds);
            tracing::warn!(
                consecutive_rebuilds = self.consecutive_rebuilds,
                backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
                applied,
                "range-0 follower is being trimmed faster than it can rebuild; backing off"
            );
            tokio::time::sleep(backoff).await;
        }
        self.consecutive_rebuilds = self.consecutive_rebuilds.saturating_add(1);

        let generation = self.store_generation.saturating_add(1);
        let fresh_store = match open_follower_store(self.cache_dir.as_deref(), generation) {
            Ok(store) => store,
            Err(error) => {
                tracing::error!(%error, "range-0 follower rebuild could not open a fresh cache");
                return;
            }
        };
        match crabka_gres_substrate::rebuild_live_range0_tail_from_checkpoint(
            &self.config,
            &self.follower.tail(),
            fresh_store,
            self.checkpoints.as_deref(),
        )
        .await
        {
            Ok(covered_offset) => {
                self.store_generation = generation;
                self.rebuilds = self.rebuilds.saturating_add(1);
                // Operator-visible on purpose: a rebuild means this node
                // skipped committed frames, so any cross-node NOTIFY committed
                // between `applied` and `covered_offset` was never delivered
                // here. That is at-most-once, exactly as PostgreSQL treats a
                // listener that was disconnected while the NOTIFY was sent.
                tracing::warn!(
                    from_offset = applied,
                    to_offset = covered_offset,
                    rebuilds = self.rebuilds,
                    "range-0 follower WAL was trimmed past its applied offset; rebuilt from the newest checkpoint, skipping the frames in between"
                );
                remove_other_follower_stores(self.cache_dir.as_deref(), generation);
            }
            Err(error) => {
                tracing::error!(
                    %error,
                    applied,
                    rebuilds = self.rebuilds,
                    "range-0 follower rebuild from checkpoint failed; reads on this node stay stalled"
                );
            }
        }
    }
}

pub(crate) async fn wait_for_refresh(refresh_poke: &tokio::sync::Notify, poll_interval: Duration) {
    tokio::select! {
        () = refresh_poke.notified() => {}
        () = tokio::time::sleep(poll_interval) => {}
    }
}

/// Doubling backoff for consecutive rebuilds, floored and capped.
fn rebuild_backoff(consecutive_rebuilds: u32) -> Duration {
    let doublings = consecutive_rebuilds.saturating_sub(1).min(16);
    REBUILD_BACKOFF_FLOOR
        .saturating_mul(1_u32 << doublings)
        .min(REBUILD_BACKOFF_CEILING)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn rebuild_backoff_starts_at_the_floor_doubles_and_caps() {
        assert!(rebuild_backoff(1) == REBUILD_BACKOFF_FLOOR);
        assert!(rebuild_backoff(2) == REBUILD_BACKOFF_FLOOR * 2);
        assert!(rebuild_backoff(3) == REBUILD_BACKOFF_FLOOR * 4);
        assert!(rebuild_backoff(u32::MAX) == REBUILD_BACKOFF_CEILING);
    }

    #[test]
    fn each_follower_generation_gets_its_own_directory() {
        let parent = tempfile::tempdir().expect("temp dir");
        let first = open_follower_store(Some(parent.path()), 0).expect("first generation");
        let second = open_follower_store(Some(parent.path()), 1).expect("second generation");

        // Both stores are open and independent at the same time: a rebuild
        // restores into the new one while the old one still serves reads.
        first.put(b"a".to_vec(), b"1".to_vec()).expect("put");
        assert!(second.get(b"a").expect("get") == None);

        remove_other_follower_stores(Some(parent.path()), 1);

        assert!(!parent.path().join("r0-follower-0").exists());
        assert!(parent.path().join("r0-follower-1").exists());
    }

    #[test]
    fn a_cacheless_follower_store_is_in_memory_and_empty() {
        let store = open_follower_store(None, 3).expect("mem store");

        assert!(store.get(b"anything").expect("get") == None);
    }
}
