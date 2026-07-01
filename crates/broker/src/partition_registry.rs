//! Concurrent registry of locally-hosted partitions.
//!
//! Replaces the former `Arc<DashMap<(String, i32), Arc<Partition>>>` keying,
//! which forced every lookup to allocate an owned `String` to build the tuple
//! key (`partitions.get(&(topic.to_string(), idx))`). On the produce/fetch hot
//! path a single request may resolve hundreds of partitions, turning that into
//! an O(partitions) allocation storm. Backing the registry with a nested
//! `DashMap<String, DashMap<i32, Arc<Partition>>>` lets lookups run with a
//! borrowed `&str` topic name and no per-lookup allocation.

use std::sync::Arc;

use dashmap::DashMap;

use crate::partition::Partition;

/// Concurrent registry of locally-hosted partitions, keyed by (topic, partition).
///
/// Backed by a nested `DashMap<String, DashMap<i32, Arc<Partition>>>` so lookups
/// can be performed with a borrowed `&str` topic name — no per-lookup `String`
/// allocation, which matters on the produce/fetch hot path where a single
/// request may resolve hundreds of partitions.
#[derive(Debug, Default)]
pub(crate) struct PartitionRegistry {
    inner: DashMap<String, DashMap<i32, Arc<Partition>>>,
}

impl PartitionRegistry {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Alloc-free lookup. Returns a cheap `Arc` clone of the partition, if present.
    #[must_use]
    pub(crate) fn get(&self, topic: &str, partition: i32) -> Option<Arc<Partition>> {
        self.inner
            .get(topic)
            .and_then(|m| m.get(&partition).map(|p| Arc::clone(&p)))
    }

    /// Returns `true` if the given (topic, partition) is hosted locally.
    #[must_use]
    pub(crate) fn contains(&self, topic: &str, partition: i32) -> bool {
        self.inner
            .get(topic)
            .is_some_and(|m| m.contains_key(&partition))
    }

    /// Insert (replace) a partition. Returns the previous value if any.
    pub(crate) fn insert(
        &self,
        topic: String,
        partition: i32,
        part: Arc<Partition>,
    ) -> Option<Arc<Partition>> {
        self.inner.entry(topic).or_default().insert(partition, part)
    }

    /// Remove a partition, returning it if present. Prunes the topic's inner map
    /// if it becomes empty.
    pub(crate) fn remove(&self, topic: &str, partition: i32) -> Option<Arc<Partition>> {
        let removed = self
            .inner
            .get(topic)
            .and_then(|m| m.remove(&partition).map(|(_, v)| v));
        // Prune empty inner map (best-effort; safe because a later `insert`
        // re-creates it via `entry(..).or_default()`).
        self.inner.remove_if(topic, |_, m| m.is_empty());
        removed
    }

    /// Atomically materialize a partition only if absent, running `build` UNDER
    /// the per-key lock so two concurrent materializations of the same
    /// (topic, partition) can never both build (preserves the KIP-113
    /// TOCTOU-free guarantee that the old `DashMap::entry` provided). `build` is
    /// only called when the slot is vacant.
    pub(crate) fn materialize_if_vacant<E>(
        &self,
        topic: &str,
        partition: i32,
        build: impl FnOnce() -> Result<Arc<Partition>, E>,
    ) -> Result<(), E> {
        use dashmap::mapref::entry::Entry;
        let inner = self.inner.entry(topic.to_string()).or_default();
        match inner.entry(partition) {
            Entry::Occupied(_) => Ok(()),
            Entry::Vacant(slot) => {
                let part = build()?;
                slot.insert(part);
                Ok(())
            }
        }
    }

    /// Partition indices currently hosted for `topic`. Empty if the topic
    /// hosts nothing locally. Used by `DeleteTopics` to enumerate the local
    /// partitions to tear down without a full-registry key scan.
    #[must_use]
    pub(crate) fn partitions_of(&self, topic: &str) -> Vec<i32> {
        self.inner
            .get(topic)
            .map(|m| m.iter().map(|e| *e.key()).collect())
            .unwrap_or_default()
    }

    /// Snapshot of all partition handles (cheap `Arc` clones). For maintenance sweeps.
    #[must_use]
    pub(crate) fn arcs(&self) -> Vec<Arc<Partition>> {
        self.inner
            .iter()
            .flat_map(|m| m.value().iter().map(|p| Arc::clone(&p)).collect::<Vec<_>>())
            .collect()
    }

    /// Total number of hosted partitions across all topics.
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.inner.iter().map(|m| m.value().len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use std::path::Path;
    use std::sync::Arc;

    use crabka_log::{Log, LogConfig};
    use tempfile::tempdir;

    use super::PartitionRegistry;
    use crate::partition::Partition;

    /// Build a `Partition` rooted at `<log_dir>/<topic>-<partition>` via the
    /// real `spawn_partition` path, mirroring `future_log`'s test fixture.
    fn fixture_partition(log_dir: &Path, topic: &str, partition: i32) -> Arc<Partition> {
        let part_dir = crate::log_dir::partition_dir(log_dir, topic, partition);
        std::fs::create_dir_all(&part_dir).unwrap();
        let log = Log::open(&part_dir, LogConfig::default()).unwrap();
        crate::broker::spawn_partition(
            topic.to_string(),
            partition,
            log_dir.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
        )
    }

    #[tokio::test]
    async fn insert_get_contains_remove() {
        let dir = tempdir().unwrap();
        let reg = PartitionRegistry::new();
        assert!(
            (
                reg.arcs().is_empty(),
                reg.arcs().len(),
                reg.get("t", 0).is_none(),
                reg.contains("t", 0),
            ) == (true, 0, true, false)
        );

        let p = fixture_partition(dir.path(), "t", 0);
        assert!(
            (
                reg.insert("t".to_string(), 0, Arc::clone(&p)).is_none(),
                reg.contains("t", 0),
                reg.arcs().is_empty(),
                reg.arcs().len(),
            ) == (true, true, false, 1)
        );

        let got = reg.get("t", 0).expect("present");
        assert!(Arc::ptr_eq(&got, &p));

        // Replace returns previous.
        let p2 = fixture_partition(dir.path(), "t", 0);
        let prev = reg
            .insert("t".to_string(), 0, Arc::clone(&p2))
            .expect("prev");
        assert!(Arc::ptr_eq(&prev, &p));
        assert!(reg.arcs().len() == 1);

        let removed = reg.remove("t", 0).expect("removed");
        assert!(
            (
                Arc::ptr_eq(&removed, &p2),
                reg.remove("t", 0).is_none(),
                reg.contains("t", 0),
                reg.arcs().is_empty(),
            ) == (true, true, false, true)
        );
    }

    #[tokio::test]
    async fn partitions_of_and_len_track_topics_and_removals() {
        let dir = tempdir().unwrap();
        let reg = PartitionRegistry::new();
        assert!(reg.partitions_of("missing").is_empty());
        assert!(reg.len() == 0);

        reg.insert("a".to_string(), 2, fixture_partition(dir.path(), "a", 2));
        reg.insert("a".to_string(), 4, fixture_partition(dir.path(), "a", 4));
        reg.insert("b".to_string(), 7, fixture_partition(dir.path(), "b", 7));

        let mut a_parts = reg.partitions_of("a");
        a_parts.sort_unstable();
        assert!((a_parts, reg.partitions_of("b"), reg.len()) == (vec![2, 4], vec![7], 3));

        let removed = reg.remove("a", 2).expect("removed");
        drop(removed);

        assert!(
            (
                reg.partitions_of("a"),
                reg.partitions_of("missing"),
                reg.len()
            ) == (vec![4], vec![], 2)
        );
    }

    #[tokio::test]
    async fn materialize_if_vacant_builds_once() {
        let dir = tempdir().unwrap();
        let reg = PartitionRegistry::new();
        let p = fixture_partition(dir.path(), "t", 1);
        reg.materialize_if_vacant::<String>("t", 1, || Ok(Arc::clone(&p)))
            .expect("build ok");
        assert!(reg.contains("t", 1));

        // Already occupied: build closure must not run.
        reg.materialize_if_vacant::<String>("t", 1, || {
            panic!("build must not be called when slot is occupied");
        })
        .expect("occupied ok");

        let got = reg.get("t", 1).expect("present");
        assert!(Arc::ptr_eq(&got, &p));
    }

    #[tokio::test]
    async fn materialize_if_vacant_propagates_error() {
        let reg = PartitionRegistry::new();
        let err = reg.materialize_if_vacant::<String>("t", 2, || Err("boom".to_string()));
        assert!(err == Err("boom".to_string()));
        assert!(!reg.contains("t", 2));
    }

    #[tokio::test]
    async fn arcs_snapshots_all_partitions() {
        let dir = tempdir().unwrap();
        let reg = PartitionRegistry::new();
        reg.insert("a".to_string(), 0, fixture_partition(dir.path(), "a", 0));
        reg.insert("a".to_string(), 1, fixture_partition(dir.path(), "a", 1));
        reg.insert("b".to_string(), 0, fixture_partition(dir.path(), "b", 0));
        let arcs = reg.arcs();
        assert!(arcs.len() == 3);
    }
}
