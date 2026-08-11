//! Unified group-coordinator subsystem for KIP-848.
//!
//! The subsystem gives shared infrastructure and persistence to both the
//! classic and the next-gen group protocols.
//!
//! [`GroupCoordinator`] is the single owner of the next-gen consumer-group
//! machinery. It spawns per-group actors, tracks each group's locked type,
//! and replays persisted state during bootstrap.
pub mod actor;
pub mod assignor;
pub(crate) mod classic_ops;
pub(crate) mod classic_state;
pub mod config;
pub(crate) mod consumer_state;
pub(crate) mod group;
pub(crate) mod migration;
pub mod offsets_log;
pub(crate) mod persistence;
pub mod persistence_next_gen;
pub mod reconciler;
pub mod share;
pub mod streams;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use actor::{GroupActorHandle, GroupActorMessage, GroupKindTag, MetadataProvider};
use bytes::Bytes;
use config::NextGenConfig;
use crabka_protocol::records::{Record, RecordBatch};
use dashmap::DashMap;
use group::CoordinatorGroup;
use offsets_log::OffsetsLog;
use share::{
    actor::{ShareGroupActorHandle, ShareGroupActorMessage},
    config::ShareGroupConfig,
};
use streams::{
    actor::{StreamsGroupActorHandle, StreamsGroupActorMessage},
    config::StreamsGroupConfig,
};
use tokio::sync::oneshot;

use crate::{
    codes,
    coordinator::{DeleteGroupError, GroupSnapshot},
};

pub(crate) fn first_join_member_id(request_member_id: &str) -> String {
    if request_member_id.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        request_member_id.to_string()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ClientIdentity<'a> {
    pub id: &'a str,
    pub host: &'a str,
}

pub(crate) fn validate_member_epoch(
    current_epoch: Option<i32>,
    requested_epoch: i32,
) -> Result<i32, i16> {
    match current_epoch {
        None => Err(codes::UNKNOWN_MEMBER_ID),
        Some(epoch) if requested_epoch < epoch => Err(codes::STALE_MEMBER_EPOCH),
        Some(epoch) if requested_epoch > epoch => Err(codes::FENCED_MEMBER_EPOCH),
        Some(epoch) => Ok(epoch),
    }
}

pub(crate) fn expired_member_ids<'a>(
    members: impl IntoIterator<Item = (&'a str, Instant)>,
    now: Instant,
    session_timeout: Duration,
) -> Vec<String> {
    members
        .into_iter()
        .filter(|(_, last_seen)| now.duration_since(*last_seen) > session_timeout)
        .map(|(id, _)| id.to_string())
        .collect()
}

#[cfg(test)]
mod helper_tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn first_join_member_id_preserves_client_supplied_id() {
        assert!(first_join_member_id("member-a") == "member-a");
    }

    #[test]
    fn first_join_member_id_mints_uuid_for_empty_id() {
        let member_id = first_join_member_id("");

        check!(!member_id.is_empty());
        assert!(uuid::Uuid::parse_str(&member_id).is_ok());
    }

    #[test]
    fn validate_member_epoch_maps_all_fencing_outcomes() {
        assert!(validate_member_epoch(None, 7) == Err(codes::UNKNOWN_MEMBER_ID));
        assert!(validate_member_epoch(Some(5), 4) == Err(codes::STALE_MEMBER_EPOCH));
        assert!(validate_member_epoch(Some(5), 6) == Err(codes::FENCED_MEMBER_EPOCH));
        assert!(validate_member_epoch(Some(5), 5) == Ok(5));
    }

    #[test]
    fn expired_member_ids_returns_only_members_past_timeout() {
        let now = Instant::now();
        let session_timeout = Duration::from_secs(10);
        let expired = now
            .checked_sub(Duration::from_secs(11))
            .expect("past instant");
        let active = now
            .checked_sub(Duration::from_secs(10))
            .expect("past instant");
        let future = now
            .checked_add(Duration::from_secs(1))
            .expect("future instant");

        let expired = expired_member_ids(
            [("expired", expired), ("active", active), ("future", future)],
            now,
            session_timeout,
        );

        assert!(expired == vec!["expired".to_string()]);
    }
}

#[derive(Default)]
pub(crate) struct OffsetRecordBatchBuilder {
    records: Vec<Record>,
}

impl OffsetRecordBatchBuilder {
    pub(crate) fn push(&mut self, key: Bytes, value: Option<Bytes>) {
        let delta = i32::try_from(self.records.len()).expect("batch size fits i32");
        self.records.push(Record {
            offset_delta: delta,
            key: Some(key),
            value,
            ..Default::default()
        });
    }

    pub(crate) fn finish(self, now_ms: i64) -> RecordBatch {
        let last_delta = i32::try_from(self.records.len().saturating_sub(1)).unwrap_or(0);
        RecordBatch {
            max_timestamp: now_ms,
            records: self.records,
            last_offset_delta: last_delta,
            ..RecordBatch::default()
        }
    }
}

/// Locked protocol identity for a `group_id`.
///
/// Classic and next-gen actors enforce their lock through the actor's
/// [`GroupKindTag`]. Share groups from KIP-932 live in a separate
/// `share_groups` registry and record their lock here, so that the
/// classic/next-gen namespace and the share namespace cannot collide on the
/// same id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupType {
    Classic,
    NextGen,
    Share,
    Streams,
}

#[derive(Debug)]
pub struct GroupCoordinator {
    pub config: Arc<NextGenConfig>,
    pub share_config: Arc<ShareGroupConfig>,
    pub metadata: Arc<dyn MetadataProvider>,
    pub offsets_log: Arc<dyn OffsetsLog>,
    pub groups: Arc<DashMap<String, Arc<GroupActorHandle>>>,
    /// Per-`group_id` share-group actor handles (KIP-932).
    pub share_groups: Arc<DashMap<String, Arc<ShareGroupActorHandle>>>,
    /// The first record persisted for a `group_id` locks its type for life.
    /// This is the classic↔next-gen↔share namespace guard.
    pub group_types: Arc<DashMap<String, GroupType>>,
    /// Bootstrap-time accumulator for next-gen state. `finalize_bootstrap`
    /// drains it.
    pub seeds: Arc<DashMap<String, GroupSeed>>,
    /// Bootstrap-time share-group accumulator. `finalize_bootstrap` drains it.
    pub share_seeds: Arc<DashMap<String, ShareGroupSeed>>,
    /// Last-known-good next-gen state per group. Every successful actor write
    /// also writes here. The coordinator seeds a fresh actor from this cache
    /// when the previous instance crashed after a log-write failure.
    pub seeds_cache: Arc<DashMap<String, GroupSeed>>,
    /// Last-known-good share-group state, the share-group analogue of
    /// `seeds_cache`.
    pub share_seeds_cache: Arc<DashMap<String, ShareGroupSeed>>,
    /// KIP-932 group-coordinator → share-state-persister bridge.
    ///
    /// `Broker::start` sets it once, after both the `ShareCoordinator` and
    /// this coordinator exist. Per-group share actors read it through
    /// [`Self::share_persister`] to drive the Initialize and Delete lifecycle
    /// calls after reconcile. It is `None` in the pure-coordinator unit tests,
    /// where the lifecycle hook does nothing.
    pub(crate) share_persister:
        std::sync::OnceLock<Arc<crate::share_coordinator::persister_client::SharePersister>>,

    // ── KIP-1071 streams groups ──────────────────────────────────────────
    pub streams_config: Arc<StreamsGroupConfig>,
    /// Per-`group_id` streams-group actor handles (KIP-1071).
    pub streams_groups: Arc<DashMap<String, Arc<StreamsGroupActorHandle>>>,
    /// Bootstrap-time streams-group accumulator. `finalize_bootstrap` drains
    /// it.
    pub streams_seeds: Arc<DashMap<String, StreamsGroupSeed>>,
    /// Last-known-good streams-group state, the streams analogue of
    /// `seeds_cache`.
    pub streams_seeds_cache: Arc<DashMap<String, StreamsGroupSeed>>,
    /// KIP-1071 metadata authority.
    ///
    /// `Broker::start` sets it once. Per-group streams actors read it through
    /// [`Self::metadata_source`] for the full `MetadataImage`, which they need
    /// for topology resolution and internal-topic creation. It is `None` in
    /// the pure-coordinator unit tests, where reconcile does nothing and
    /// returns `NotReady`.
    pub(crate) metadata_source: std::sync::OnceLock<MetadataSourceHandle>,
}

/// `Debug`-able wrapper around an `Arc<dyn MetadataSource>` so that it can
/// live in the `#[derive(Debug)]` [`GroupCoordinator`].
///
/// The trait object itself is not `Debug`. This wrapper prints an opaque
/// placeholder.
#[derive(Clone)]
pub(crate) struct MetadataSourceHandle(pub(crate) Arc<dyn crate::metadata_source::MetadataSource>);

impl std::fmt::Debug for MetadataSourceHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataSourceHandle")
            .finish_non_exhaustive()
    }
}

impl GroupCoordinator {
    pub fn new(
        config: NextGenConfig,
        share_config: ShareGroupConfig,
        metadata: Arc<dyn MetadataProvider>,
        offsets_log: Arc<dyn OffsetsLog>,
        streams_config: StreamsGroupConfig,
    ) -> Self {
        Self {
            config: Arc::new(config),
            share_config: Arc::new(share_config),
            metadata,
            offsets_log,
            groups: Arc::new(DashMap::new()),
            share_groups: Arc::new(DashMap::new()),
            group_types: Arc::new(DashMap::new()),
            seeds: Arc::new(DashMap::new()),
            share_seeds: Arc::new(DashMap::new()),
            seeds_cache: Arc::new(DashMap::new()),
            share_seeds_cache: Arc::new(DashMap::new()),
            share_persister: std::sync::OnceLock::new(),
            streams_config: Arc::new(streams_config),
            streams_groups: Arc::new(DashMap::new()),
            streams_seeds: Arc::new(DashMap::new()),
            streams_seeds_cache: Arc::new(DashMap::new()),
            metadata_source: std::sync::OnceLock::new(),
        }
    }

    /// Install the KIP-932 share-state persister bridge.
    ///
    /// `Broker::start` calls this once. A second call does nothing, because
    /// the `OnceLock` keeps the first value. Construction order therefore does
    /// not matter.
    pub(crate) fn set_share_persister(
        &self,
        persister: Arc<crate::share_coordinator::persister_client::SharePersister>,
    ) {
        let _ = self.share_persister.set(persister);
    }

    /// The installed share-state persister, if there is one.
    ///
    /// It is `None` in the unit tests that construct a bare
    /// `GroupCoordinator`. The lifecycle hook then does nothing.
    #[must_use]
    pub(crate) fn share_persister(
        &self,
    ) -> Option<&Arc<crate::share_coordinator::persister_client::SharePersister>> {
        self.share_persister.get()
    }

    /// Install the KIP-1071 metadata source.
    ///
    /// `Broker::start` calls this once. A second call does nothing, because
    /// the `OnceLock` keeps the first value.
    pub(crate) fn set_metadata_source(&self, src: Arc<dyn crate::metadata_source::MetadataSource>) {
        let _ = self.metadata_source.set(MetadataSourceHandle(src));
    }

    /// The installed metadata source, if there is one.
    ///
    /// It is `None` in the unit tests that construct a bare
    /// `GroupCoordinator`. The streams reconcile then does nothing and returns
    /// `NotReady`.
    #[must_use]
    pub(crate) fn metadata_source(
        &self,
    ) -> Option<Arc<dyn crate::metadata_source::MetadataSource>> {
        self.metadata_source.get().map(|h| h.0.clone())
    }

    /// Mutate the cached seed for `group_id` after a successful durable write.
    ///
    /// Applying only the records in the write avoids cloning the whole group
    /// after a one-member heartbeat while keeping cache and replay semantics
    /// identical.
    pub(crate) fn update_cached_seed(&self, group_id: &str, update: impl FnOnce(&mut GroupSeed)) {
        let mut seed = self.seeds_cache.entry(group_id.into()).or_default();
        update(seed.value_mut());
    }

    /// Remove a cached next-gen seed after its group-metadata tombstone is
    /// durable.
    pub(crate) fn remove_cached_seed(&self, group_id: &str) {
        self.seeds_cache.remove(group_id);
    }

    /// Fetch the most recently cached seed for `group_id`, if any.
    #[must_use]
    pub fn cached_seed(&self, group_id: &str) -> Option<GroupSeed> {
        self.seeds_cache.get(group_id).map(|e| e.value().clone())
    }

    /// The locked protocol type for `group_id`, if the coordinator recorded
    /// one.
    ///
    /// Share groups from KIP-932 record their lock here with
    /// [`mark_share`](Self::mark_share). Classic and next-gen actors also
    /// enforce their lock through the actor [`GroupKindTag`].
    #[must_use]
    pub fn group_type(&self, group_id: &str) -> Option<GroupType> {
        self.group_types.get(group_id).map(|e| *e.value())
    }

    pub fn mark_classic(&self, group_id: &str) {
        self.group_types
            .entry(group_id.into())
            .or_insert(GroupType::Classic);
    }

    /// After an in-place KIP-848 downgrade, drop the consumer seed and record
    /// the group as classic.
    ///
    /// The dropped seed keeps a respawn from hydrating the group as next-gen
    /// again. [`Self::mark_classic`] keeps the first mark through `or_insert`,
    /// but this method FORCES the type to `Classic`. A downgrade must override
    /// any earlier `NextGen` lock that the group carried while it was a
    /// consumer group.
    pub fn mark_classic_after_downgrade(&self, group_id: &str) {
        self.seeds.remove(group_id);
        self.seeds_cache.remove(group_id);
        self.group_types.insert(group_id.into(), GroupType::Classic);
    }

    /// After an in-place classic→streams upgrade from KIP-1071, drop the
    /// classic seed and record the group as streams.
    ///
    /// The dropped seed keeps a respawn from hydrating the group as classic
    /// again. [`Self::mark_streams`] keeps the first mark through `or_insert`,
    /// but this method FORCES the type to `Streams`. It overrides any earlier
    /// `Classic` lock that the group carried while it was a classic group.
    pub fn mark_streams_after_upgrade(&self, group_id: &str) {
        self.seeds.remove(group_id);
        self.seeds_cache.remove(group_id);
        self.group_types.insert(group_id.into(), GroupType::Streams);
    }

    /// After an in-place streams→classic downgrade from KIP-1071, drop the
    /// streams seed and record the group as classic.
    ///
    /// The dropped seed keeps a respawn from hydrating the group as streams
    /// again. [`Self::mark_classic`] keeps the first mark through `or_insert`,
    /// but this method FORCES the type to `Classic`. It overrides any earlier
    /// `Streams` lock. It is the mirror of
    /// [`Self::mark_streams_after_upgrade`]. It drops the **streams** seeds,
    /// which are `streams_seeds` and `streams_seeds_cache`. It does not drop
    /// the consumer `seeds` that [`Self::mark_classic_after_downgrade`] drops.
    pub fn mark_classic_after_streams_downgrade(&self, group_id: &str) {
        self.streams_seeds.remove(group_id);
        self.streams_seeds_cache.remove(group_id);
        self.group_types.insert(group_id.into(), GroupType::Classic);
    }

    pub fn mark_next_gen(&self, group_id: &str) {
        self.group_types
            .entry(group_id.into())
            .or_insert(GroupType::NextGen);
    }

    pub fn mark_share(&self, group_id: &str) {
        self.group_types
            .entry(group_id.into())
            .or_insert(GroupType::Share);
    }

    pub fn mark_streams(&self, group_id: &str) {
        self.group_types
            .entry(group_id.into())
            .or_insert(GroupType::Streams);
    }

    /// Replace the cached share-group seed for `group_id`.
    ///
    /// The share actor calls this after every successful
    /// `OffsetsLog::append`.
    pub fn update_share_cache(&self, group_id: &str, seed: ShareGroupSeed) {
        self.share_seeds_cache.insert(group_id.into(), seed);
    }

    /// Fetch the most recently cached share-group seed for `group_id`, if any.
    #[must_use]
    pub fn cached_share_seed(&self, group_id: &str) -> Option<ShareGroupSeed> {
        self.share_seeds_cache
            .get(group_id)
            .map(|e| e.value().clone())
    }

    /// Replace the cached streams-group seed for `group_id`.
    ///
    /// The streams actor calls this after every successful
    /// `OffsetsLog::append`.
    pub fn update_streams_cache(&self, group_id: &str, seed: StreamsGroupSeed) {
        self.streams_seeds_cache.insert(group_id.into(), seed);
    }

    /// Fetch the most recently cached streams-group seed for `group_id`, if any.
    #[must_use]
    pub fn cached_streams_seed(&self, group_id: &str) -> Option<StreamsGroupSeed> {
        self.streams_seeds_cache
            .get(group_id)
            .map(|e| e.value().clone())
    }

    /// Get the one actor for `group_id`, and spawn it with `initial_kind` when
    /// it is absent.
    ///
    /// The kind argument only decides the spawn kind for a brand-new group.
    /// Both families route to one actor; the actor rejects the family it does
    /// not currently serve and can change kind in place, so a group is not
    /// pinned to its spawn kind. Keeps the dead-actor
    /// (closed tx) respawn and the consumer re-hydrate-from-seed paths.
    #[must_use]
    pub fn get_or_create_group(
        self: &Arc<Self>,
        group_id: &str,
        initial_kind: GroupKindTag,
    ) -> Arc<GroupActorHandle> {
        if let Some(h) = self.groups.get(group_id) {
            // Dead-actor detection: if the mpsc sender is closed, the actor
            // has exited (typically after a log-write failure). Drop the
            // entry and fall through to spawn a fresh actor.
            if !h.value().tx.is_closed() {
                return h.value().clone();
            }
            drop(h);
            self.groups.remove(group_id);
        }
        let h = Arc::new(GroupActorHandle::spawn(
            group_id.into(),
            initial_kind,
            self.config.clone(),
            self.metadata.clone(),
            self.offsets_log.clone(),
            self.clone(),
        ));
        let inserted = self
            .groups
            .entry(group_id.into())
            .or_insert(h)
            .value()
            .clone();
        // Re-hydrate a respawned consumer actor from its last-known-good state.
        if initial_kind == GroupKindTag::Consumer
            && let Some(seed) = self.cached_seed(group_id)
        {
            let _ = inserted.tx.try_send(GroupActorMessage::Seed(seed));
        }
        inserted
    }

    /// Get or create a classic-protocol actor.
    ///
    /// This method spawns a classic actor for a brand-new id. For an id that
    /// exists, it returns the one actor whatever its kind. The actor then
    /// serves or rejects the request per its live kind.
    #[must_use]
    pub fn get_or_create_classic(self: &Arc<Self>, group_id: &str) -> Arc<GroupActorHandle> {
        self.get_or_create_group(group_id, GroupKindTag::Classic)
    }

    /// Get or create a next-gen consumer-protocol actor.
    ///
    /// This method spawns a consumer actor for a brand-new id. For an id that
    /// exists, it returns the one actor whatever its kind. The actor then
    /// serves or rejects the request per its live kind.
    #[must_use]
    pub fn get_or_create_consumer(self: &Arc<Self>, group_id: &str) -> Arc<GroupActorHandle> {
        self.get_or_create_group(group_id, GroupKindTag::Consumer)
    }

    #[must_use]
    pub fn find(&self, group_id: &str) -> Option<Arc<GroupActorHandle>> {
        self.groups.get(group_id).map(|e| e.value().clone())
    }

    #[must_use]
    pub fn get_or_create_share(self: &Arc<Self>, group_id: &str) -> Arc<ShareGroupActorHandle> {
        if let Some(h) = self.share_groups.get(group_id) {
            // Dead-actor detection: a closed mpsc sender means the actor exited
            // (typically after a log-write failure). Drop the entry and respawn.
            if !h.value().tx.is_closed() {
                return h.value().clone();
            }
            drop(h);
            self.share_groups.remove(group_id);
        }
        let h = Arc::new(ShareGroupActorHandle::spawn(
            group_id.into(),
            self.share_config.clone(),
            self.metadata.clone(),
            self.offsets_log.clone(),
            self.clone(),
        ));
        let inserted = self
            .share_groups
            .entry(group_id.into())
            .or_insert(h)
            .value()
            .clone();
        if let Some(seed) = self.cached_share_seed(group_id) {
            let _ = inserted.tx.try_send(ShareGroupActorMessage::Seed(seed));
        }
        inserted
    }

    #[must_use]
    pub fn find_share(&self, group_id: &str) -> Option<Arc<ShareGroupActorHandle>> {
        self.share_groups.get(group_id).map(|e| e.value().clone())
    }

    /// Snapshot the ids of every live share group, per KIP-932.
    ///
    /// The call is synchronous and cheap. It reads the registry keys and makes
    /// no actor round-trip. `ListGroups`, `api_key` 16, can therefore include
    /// share groups together with classic ones without the per-group
    /// `Describe` mpsc hop.
    #[must_use]
    pub fn share_group_ids(&self) -> Vec<String> {
        self.share_groups.iter().map(|e| e.key().clone()).collect()
    }

    // ── KIP-1071 streams-group registry ──────────────────────────────────

    #[must_use]
    pub fn get_or_create_streams(self: &Arc<Self>, group_id: &str) -> Arc<StreamsGroupActorHandle> {
        if let Some(h) = self.streams_groups.get(group_id) {
            // Dead-actor detection: a closed mpsc sender means the actor exited
            // (typically after a log-write failure). Drop the entry and respawn.
            if !h.value().tx.is_closed() {
                return h.value().clone();
            }
            drop(h);
            self.streams_groups.remove(group_id);
        }
        let h = Arc::new(StreamsGroupActorHandle::spawn(
            group_id.into(),
            self.streams_config.clone(),
            self.offsets_log.clone(),
            self.metadata_source(),
            self.clone(),
        ));
        let inserted = self
            .streams_groups
            .entry(group_id.into())
            .or_insert(h)
            .value()
            .clone();
        if let Some(seed) = self.cached_streams_seed(group_id) {
            let _ = inserted.tx.try_send(StreamsGroupActorMessage::Seed(seed));
        }
        inserted
    }

    #[must_use]
    pub fn find_streams(&self, group_id: &str) -> Option<Arc<StreamsGroupActorHandle>> {
        self.streams_groups.get(group_id).map(|e| e.value().clone())
    }

    /// KIP-1071 cold upgrade: convert a drained classic `group_id` to a
    /// streams group in place.
    ///
    /// The method tombstones the classic k2 `GroupMetadata` and forces the
    /// type lock to `Streams`. The committed offsets survive untouched. The
    /// classic actor stays in the `groups` map, so `OffsetFetch` requests can
    /// still read back the committed offset state.
    ///
    /// The method returns `NotClassic` for a non-classic group, and the caller
    /// then serves it as normal. It returns `Converted` after a successful
    /// flip. It returns `RejectLiveMembers` when live classic members remain,
    /// because Kafka does not support an online streams migration.
    pub(crate) async fn try_convert_classic_to_streams(
        self: &Arc<Self>,
        group_id: &str,
        now_ms: i64,
    ) -> Result<streams::migration::ConvertOutcome, crate::error::BrokerError> {
        use streams::migration::{ConvertOutcome, classic_group_metadata_tombstone_batch};

        if self.group_type(group_id) != Some(GroupType::Classic) {
            return Ok(ConvertOutcome::NotClassic);
        }

        // Inspect the live classic actor (if any) for remaining members.
        if let Some(handle) = self.find(group_id) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if handle
                .tx
                .send(GroupActorMessage::ClassicInspect { reply: tx })
                .await
                .is_ok()
                && let Ok(view) = rx.await
                && !view.members.is_empty()
            {
                return Ok(ConvertOutcome::RejectLiveMembers);
            }
        }

        // Drained classic group → convert. Tombstone the classic k2 GroupMetadata
        // to clear any persisted classic metadata (defensive + matching the
        // KIP-848 upgrade flip; a no-op on replay when none was persisted). Flip
        // the type lock to Streams; the classic actor (if any) stays in
        // `self.groups` so its committed_offsets remain accessible to
        // `OffsetFetch` without a full replay cycle.
        let batch = classic_group_metadata_tombstone_batch(group_id, now_ms);
        self.offsets_log.append(group_id, batch).await?;
        self.mark_streams_after_upgrade(group_id);
        Ok(ConvertOutcome::Converted)
    }

    /// KIP-1071 cold downgrade: convert a drained streams `group_id` to a
    /// classic group in place.
    ///
    /// The method tombstones the streams records k15–21, forces the type lock
    /// to `Classic`, and drops the streams actor. The committed offsets, k0
    /// and k1, and the offset-home `groups` entry survive.
    ///
    /// The method returns `NotStreams` for a non-streams group, and the caller
    /// then serves the classic `JoinGroup` as normal. It returns `Converted`
    /// after a successful flip. It returns `RejectLiveMembers` when the
    /// streams group still has live members, because Kafka does not support an
    /// online streams migration. It is the mirror of
    /// [`Self::try_convert_classic_to_streams`].
    pub(crate) async fn try_convert_streams_to_classic(
        self: &Arc<Self>,
        group_id: &str,
        now_ms: i64,
    ) -> Result<streams::migration::DowngradeOutcome, crate::error::BrokerError> {
        use streams::{
            actor::StreamsGroupActorMessage,
            migration::{DowngradeOutcome, streams_records_tombstone_batch},
        };

        if self.group_type(group_id) != Some(GroupType::Streams) {
            return Ok(DowngradeOutcome::NotStreams);
        }

        // Reject if the streams actor (if any) still has live members; a drained
        // group falls through to convert. Mirrors slice 1's `ClassicInspect` check.
        if let Some(handle) = self.find_streams(group_id) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if handle
                .tx
                .send(StreamsGroupActorMessage::Describe { reply: tx })
                .await
                .is_ok()
                && let Ok(view) = rx.await
                && !view.members.is_empty()
            {
                return Ok(DowngradeOutcome::RejectLiveMembers);
            }
        }

        // Drained streams group → convert. Tombstone the group-level streams keys
        // (k15/k17/k18/k19), flip the lock to Classic, drop the streams actor. A
        // drained group's per-member records (k16/k20/k21) were already tombstoned
        // when those members left/expired, so no member ids are needed here. The
        // offset-home `groups` entry stays.
        let batch = streams_records_tombstone_batch(group_id, &[], now_ms);
        self.offsets_log.append(group_id, batch).await?;
        self.mark_classic_after_streams_downgrade(group_id);
        self.streams_groups.remove(group_id);
        Ok(DowngradeOutcome::Converted)
    }

    /// Snapshot the ids of every live streams group, per KIP-1071.
    ///
    /// The method is the counterpart of
    /// [`share_group_ids`](Self::share_group_ids). `ListGroups` uses it to
    /// emit `group_type="streams"` entries without a per-group `Describe` hop.
    #[must_use]
    pub fn streams_group_ids(&self) -> Vec<String> {
        self.streams_groups
            .iter()
            .map(|e| e.key().clone())
            .collect()
    }

    /// Ids of every live next-gen KIP-848 consumer group actor.
    ///
    /// The method is the counterpart of
    /// [`share_group_ids`](Self::share_group_ids). `ListGroups` uses it to
    /// emit `group_type="consumer"` entries without an actor round-trip.
    ///
    /// Note: this method returns all group ids from the shared `groups` map,
    /// classic groups included. The `emitted` dedup set in the `ListGroups`
    /// handler prevents a double wire emission. A classic group therefore goes
    /// out once as `group_type="classic"` and not again here.
    pub fn consumer_group_ids(&self) -> Vec<String> {
        self.groups.iter().map(|e| e.key().clone()).collect()
    }

    /// Spawn a classic actor seeded with a fully-replayed `Group` at
    /// bootstrap.
    pub fn seed_classic(self: &Arc<Self>, group_id: &str, group: Box<CoordinatorGroup>) {
        let handle = self.get_or_create_classic(group_id);
        let _ = handle.tx.try_send(GroupActorMessage::ClassicSeed(group));
    }

    /// Snapshot every **live-classic** group for the wire `ListGroups` pass
    /// that emits `group_type="classic"`.
    ///
    /// The method walks ALL handles and selects on the group's LIVE kind, not
    /// on the spawn-time `handle.kind` hint. A KIP-848 live migration can make
    /// the two differ. The `ClassicInspect` arm replies for a classic-kind
    /// group only, so a consumer group or an upgraded group drops its reply
    /// sender and this method skips it.
    ///
    /// This keeps `list_groups` the only producer of the `classic` rows. The
    /// `ListGroups` handler emits the consumer-kind groups separately through
    /// [`consumer_group_ids`](Self::consumer_group_ids) with the tag
    /// `group_type="consumer"`, so it does NOT count them twice or mislabel
    /// them. A *downgraded* group whose handle still reads `Consumer` still
    /// appears here, because its live kind is `Classic`.
    pub async fn list_groups(&self) -> Vec<GroupSnapshot> {
        let handles: Vec<Arc<GroupActorHandle>> =
            self.groups.iter().map(|e| e.value().clone()).collect();
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            let (tx, rx) = oneshot::channel();
            // `ClassicInspect` replies only for a classic-kind group; a
            // consumer-kind group never sends, so `rx.await` errors and we skip.
            if h.tx
                .send(GroupActorMessage::ClassicInspect { reply: tx })
                .await
                .is_ok()
                && let Ok(view) = rx.await
            {
                out.push(view.snapshot());
            }
        }
        out
    }

    /// Snapshot a single group, classic OR consumer or migrated, and return
    /// `None` when the group is unknown.
    ///
    /// The method inspects the LIVE group through [`InspectAny`] and does not
    /// gate on the spawn-time `handle.kind`. An upgraded consumer group
    /// therefore still reports.
    ///
    /// [`InspectAny`]: GroupActorMessage::InspectAny
    pub async fn describe_group(&self, group_id: &str) -> Option<GroupSnapshot> {
        let handle = self.find(group_id)?;
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::InspectAny { reply: tx })
            .await
            .ok()?;
        rx.await.ok()
    }

    /// Drop a **classic** group from the registry.
    ///
    /// The actor atomically verifies that a classic group is empty and appends
    /// its durable k2 tombstone before removing it from the registry. The
    /// method returns `NonEmpty` when the group still has live members. It
    /// returns `NotFound` when the group is unknown or is a consumer group.
    /// # Errors
    /// Returns an error when the group is not deletable or the tombstone cannot
    /// be appended.
    pub async fn delete_group(&self, group_id: &str) -> Result<(), DeleteGroupError> {
        // KIP-1071: a Streams-locked group is deleted through the streams path —
        // never fall through to the classic path, which would remove the offset-home
        // `groups` entry out from under a live streams group.
        if self.group_type(group_id) == Some(GroupType::Streams) {
            return self.delete_streams_group(group_id).await;
        }
        let handle = self.find(group_id).ok_or(DeleteGroupError::NotFound)?;
        // The actor serializes this check with Join/Leave so a concurrent join
        // cannot slip between the empty check and the tombstone append.
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicDelete { reply: tx })
            .await
            .map_err(|_| DeleteGroupError::NotFound)?;
        rx.await.map_err(|_| DeleteGroupError::NotFound)??;
        self.groups.remove(group_id);
        self.group_types.remove(group_id);
        Ok(())
    }

    /// Delete a **streams** group, per KIP-1071.
    ///
    /// The method returns `NonEmpty` when the streams actor still has live
    /// members. It returns `NotFound` when no streams actor exists for the id.
    /// In every other case it tombstones the group's records k15–21, drops the
    /// streams actor, and removes the offset-home `groups` entry. It returns
    /// `Internal` when the tombstone append fails.
    async fn delete_streams_group(&self, group_id: &str) -> Result<(), DeleteGroupError> {
        // A Streams-locked id with no live streams actor reports NotFound — the
        // safe failure mode (never silently drop an offset home). In practice a
        // live streams group always has an actor (respawned by finalize_bootstrap
        // on replay), so this only guards a genuinely-absent group.
        let handle = self
            .find_streams(group_id)
            .ok_or(DeleteGroupError::NotFound)?;
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(streams::actor::StreamsGroupActorMessage::Describe { reply: tx })
            .await
            .map_err(|_| DeleteGroupError::NotFound)?;
        let view = rx.await.map_err(|_| DeleteGroupError::NotFound)?;
        if !view.members.is_empty() {
            return Err(DeleteGroupError::NonEmpty);
        }
        // Drained group: per-member records (k16/k20/k21) were already tombstoned
        // on member leave/expiry, so only the group-level keys remain.
        let batch = streams::migration::streams_records_tombstone_batch(
            group_id,
            &[],
            crate::time_util::now_ms(),
        );
        self.offsets_log
            .append(group_id, batch)
            .await
            .map_err(|_| DeleteGroupError::Internal)?;
        self.streams_groups.remove(group_id);
        self.groups.remove(group_id);
        self.streams_seeds.remove(group_id);
        self.streams_seeds_cache.remove(group_id);
        Ok(())
    }

    pub async fn shutdown_all(&self) {
        let handles: Vec<Arc<GroupActorHandle>> =
            self.groups.iter().map(|e| e.value().clone()).collect();
        for h in handles {
            let (tx, rx) = oneshot::channel();
            if h.tx.send(GroupActorMessage::Shutdown(tx)).await.is_ok() {
                let _ = tokio::time::timeout(self.config.shutdown_ack_timeout, rx).await;
            }
        }
        let share_handles: Vec<Arc<ShareGroupActorHandle>> = self
            .share_groups
            .iter()
            .map(|e| e.value().clone())
            .collect();
        for h in share_handles {
            let (tx, rx) = oneshot::channel();
            if h.tx
                .send(ShareGroupActorMessage::Shutdown(tx))
                .await
                .is_ok()
            {
                let _ = tokio::time::timeout(self.config.shutdown_ack_timeout, rx).await;
            }
        }
        let streams_handles: Vec<Arc<StreamsGroupActorHandle>> = self
            .streams_groups
            .iter()
            .map(|e| e.value().clone())
            .collect();
        for h in streams_handles {
            let (tx, rx) = oneshot::channel();
            if h.tx
                .send(StreamsGroupActorMessage::Shutdown(tx))
                .await
                .is_ok()
            {
                let _ = tokio::time::timeout(self.config.shutdown_ack_timeout, rx).await;
            }
        }
    }
}

impl GroupCoordinator {
    pub fn replay_group_metadata(
        &self,
        group_id: &str,
        v: persistence_next_gen::GroupMetadataValue,
    ) {
        {
            let mut seed = self.seeds.entry(group_id.into()).or_default();
            seed.group_epoch = v.epoch;
        }
        let mut cached = self.seeds_cache.entry(group_id.into()).or_default();
        cached.group_epoch = v.epoch;
    }
    pub fn replay_member_metadata(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence_next_gen::MemberMetadataValue,
    ) {
        {
            let mut seed = self.seeds.entry(group_id.into()).or_default();
            seed.members.insert(member_id.into(), v.clone());
        }
        let mut cached = self.seeds_cache.entry(group_id.into()).or_default();
        cached.members.insert(member_id.into(), v);
    }
    pub fn replay_target_assignment_metadata(
        &self,
        group_id: &str,
        v: persistence_next_gen::TargetAssignmentMetadataValue,
    ) {
        {
            let mut seed = self.seeds.entry(group_id.into()).or_default();
            seed.target_epoch = v.assignment_epoch;
        }
        let mut cached = self.seeds_cache.entry(group_id.into()).or_default();
        cached.target_epoch = v.assignment_epoch;
    }
    pub fn replay_target_assignment_member(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence_next_gen::TargetAssignmentMemberValue,
    ) {
        {
            let mut seed = self.seeds.entry(group_id.into()).or_default();
            seed.target_per_member.insert(member_id.into(), v.clone());
        }
        let mut cached = self.seeds_cache.entry(group_id.into()).or_default();
        cached.target_per_member.insert(member_id.into(), v);
    }
    pub fn replay_current_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence_next_gen::CurrentMemberAssignmentValue,
    ) {
        {
            let mut seed = self.seeds.entry(group_id.into()).or_default();
            seed.current_per_member.insert(member_id.into(), v.clone());
        }
        let mut cached = self.seeds_cache.entry(group_id.into()).or_default();
        cached.current_per_member.insert(member_id.into(), v);
    }

    /// Apply a tombstone for a next-gen key.
    ///
    /// The method removes the matching entry from both `seeds` and
    /// `seeds_cache`. Bootstrap replay calls it to honor records with
    /// `value = None`.
    ///
    /// A `GroupMetadata` tombstone is the migration DOWNGRADE marker. It drops
    /// the whole next-gen group. Replay must REMOVE the seed from both `seeds`
    /// and `seeds_cache`, so that the group disappears from the next-gen set
    /// that `finalize` derives. A later classic k2 `GroupMetadata` record can
    /// then rebuild it as a CLASSIC group, because log order wins. A change
    /// that only zeroed the epoch would leave the group classified as next-gen
    /// and would replay it back as an empty consumer group.
    pub fn replay_next_gen_tombstone(&self, key: &persistence_next_gen::NextGenKey) {
        use persistence_next_gen::NextGenKey as K;
        if let K::GroupMetadata { group_id } = key {
            self.seeds.remove(group_id);
            self.seeds_cache.remove(group_id);
            return;
        }
        let group_id = match key {
            K::GroupMetadata { group_id }
            | K::MemberMetadata { group_id, .. }
            | K::TargetAssignmentMetadata { group_id }
            | K::TargetAssignmentMember { group_id, .. }
            | K::CurrentMemberAssignment { group_id, .. } => group_id.as_str(),
        };
        let scrub = |seed: &mut GroupSeed| match key {
            // Unreachable: the `GroupMetadata` tombstone removes the whole seed
            // and returns above. Kept only for match exhaustiveness.
            K::GroupMetadata { .. } => {
                seed.group_epoch = 0;
            }
            K::MemberMetadata { member_id, .. } => {
                seed.members.remove(member_id);
            }
            K::TargetAssignmentMetadata { .. } => {
                seed.target_epoch = 0;
            }
            K::TargetAssignmentMember { member_id, .. } => {
                seed.target_per_member.remove(member_id);
            }
            K::CurrentMemberAssignment { member_id, .. } => {
                seed.current_per_member.remove(member_id);
            }
        };
        {
            if let Some(mut s) = self.seeds.get_mut(group_id) {
                scrub(s.value_mut());
            }
        }
        if let Some(mut s) = self.seeds_cache.get_mut(group_id) {
            scrub(s.value_mut());
        }
    }

    // ── KIP-932 share-group replay ───────────────────────────────────────

    pub fn replay_share_group_metadata(
        &self,
        group_id: &str,
        v: share::persistence::ShareGroupMetadataValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.group_epoch = v.epoch;
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.group_epoch = v.epoch;
    }
    pub fn replay_share_member_metadata(
        &self,
        group_id: &str,
        member_id: &str,
        v: share::persistence::ShareGroupMemberMetadataValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.members.insert(member_id.into(), v.clone());
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.members.insert(member_id.into(), v);
    }
    pub fn replay_share_target_assignment_metadata(
        &self,
        group_id: &str,
        v: share::persistence::ShareGroupTargetAssignmentMetadataValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.target_epoch = v.assignment_epoch;
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.target_epoch = v.assignment_epoch;
    }
    pub fn replay_share_target_assignment_member(
        &self,
        group_id: &str,
        member_id: &str,
        v: share::persistence::ShareGroupTargetAssignmentMemberValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.target_per_member.insert(member_id.into(), v.clone());
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.target_per_member.insert(member_id.into(), v);
    }
    pub fn replay_share_current_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        v: share::persistence::ShareGroupCurrentMemberAssignmentValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.current_per_member.insert(member_id.into(), v.clone());
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.current_per_member.insert(member_id.into(), v);
    }

    /// Replay a KIP-932 `ShareGroupStatePartitionMetadata` record, key v14.
    ///
    /// The method records which `(topic_id, partition)` share-states the group
    /// has initialized. The lifecycle hook can then skip a re-initialization
    /// after a restart.
    pub fn replay_share_state_partition_metadata(
        &self,
        group_id: &str,
        v: share::persistence::ShareGroupStatePartitionMetadataValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.state_partition_metadata = v.clone();
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.state_partition_metadata = v;
    }

    /// Read the cached `ShareGroupStatePartitionMetadata` for `group_id`.
    ///
    /// The value records which `(topic_id, partition)` share-states the group
    /// has initialized. The method returns `None` for an unknown group. It
    /// drives the admin offset RPCs Describe/Alter/Delete `ShareGroupOffsets`.
    /// Those RPCs list the initialized partitions when the request omits an
    /// explicit list.
    #[must_use]
    pub fn share_state_partition_metadata(
        &self,
        group_id: &str,
    ) -> Option<share::persistence::ShareGroupStatePartitionMetadataValue> {
        self.share_seeds_cache
            .get(group_id)
            .map(|e| e.value().state_partition_metadata.clone())
    }

    /// Apply a tombstone for a share-group key.
    ///
    /// The method removes the matching entry from both `share_seeds` and
    /// `share_seeds_cache`.
    pub fn replay_share_tombstone(&self, key: &share::persistence::ShareGroupKey) {
        use share::persistence::ShareGroupKey as K;
        let group_id = match key {
            K::GroupMetadata { group_id }
            | K::MemberMetadata { group_id, .. }
            | K::TargetAssignmentMetadata { group_id }
            | K::TargetAssignmentMember { group_id, .. }
            | K::CurrentMemberAssignment { group_id, .. }
            | K::StatePartitionMetadata { group_id } => group_id.as_str(),
        };
        let scrub = |seed: &mut ShareGroupSeed| match key {
            K::GroupMetadata { .. } => seed.group_epoch = 0,
            K::MemberMetadata { member_id, .. } => {
                seed.members.remove(member_id);
            }
            K::TargetAssignmentMetadata { .. } => seed.target_epoch = 0,
            K::TargetAssignmentMember { member_id, .. } => {
                seed.target_per_member.remove(member_id);
            }
            K::CurrentMemberAssignment { member_id, .. } => {
                seed.current_per_member.remove(member_id);
            }
            K::StatePartitionMetadata { .. } => {
                seed.state_partition_metadata =
                    share::persistence::ShareGroupStatePartitionMetadataValue::default();
            }
        };
        {
            if let Some(mut s) = self.share_seeds.get_mut(group_id) {
                scrub(s.value_mut());
            }
        }
        if let Some(mut s) = self.share_seeds_cache.get_mut(group_id) {
            scrub(s.value_mut());
        }
    }

    // ── KIP-1071 streams-group replay ─────────────────────────────────────

    pub fn replay_streams_group_metadata(&self, group_id: &str, epoch: i32) {
        {
            let mut seed = self.streams_seeds.entry(group_id.into()).or_default();
            seed.group_epoch = epoch;
        }
        let mut cached = self.streams_seeds_cache.entry(group_id.into()).or_default();
        cached.group_epoch = epoch;
    }
    pub fn replay_streams_member_metadata(
        &self,
        group_id: &str,
        member_id: &str,
        v: streams::persistence::StreamsGroupMemberMetadataValue,
    ) {
        {
            let mut seed = self.streams_seeds.entry(group_id.into()).or_default();
            seed.members.insert(member_id.into(), v.clone());
        }
        let mut cached = self.streams_seeds_cache.entry(group_id.into()).or_default();
        cached.members.insert(member_id.into(), v);
    }
    pub fn replay_streams_topology(
        &self,
        group_id: &str,
        v: streams::persistence::StreamsGroupTopologyValue,
    ) {
        {
            let mut seed = self.streams_seeds.entry(group_id.into()).or_default();
            seed.topology = Some(v.clone());
        }
        let mut cached = self.streams_seeds_cache.entry(group_id.into()).or_default();
        cached.topology = Some(v);
    }
    pub fn replay_streams_partition_metadata(
        &self,
        group_id: &str,
        v: streams::persistence::StreamsGroupPartitionMetadataValue,
    ) {
        {
            let mut seed = self.streams_seeds.entry(group_id.into()).or_default();
            seed.partition_metadata = Some(v.clone());
        }
        let mut cached = self.streams_seeds_cache.entry(group_id.into()).or_default();
        cached.partition_metadata = Some(v);
    }
    pub fn replay_streams_target_assignment_metadata(&self, group_id: &str, assignment_epoch: i32) {
        {
            let mut seed = self.streams_seeds.entry(group_id.into()).or_default();
            seed.assignment_epoch = assignment_epoch;
        }
        let mut cached = self.streams_seeds_cache.entry(group_id.into()).or_default();
        cached.assignment_epoch = assignment_epoch;
    }
    pub fn replay_streams_target_assignment_member(
        &self,
        group_id: &str,
        member_id: &str,
        v: streams::persistence::StreamsGroupTargetAssignmentMemberValue,
    ) {
        {
            let mut seed = self.streams_seeds.entry(group_id.into()).or_default();
            seed.target_per_member.insert(member_id.into(), v.clone());
        }
        let mut cached = self.streams_seeds_cache.entry(group_id.into()).or_default();
        cached.target_per_member.insert(member_id.into(), v);
    }
    pub fn replay_streams_current_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        v: streams::persistence::StreamsGroupCurrentMemberAssignmentValue,
    ) {
        {
            let mut seed = self.streams_seeds.entry(group_id.into()).or_default();
            seed.current_per_member.insert(member_id.into(), v.clone());
        }
        let mut cached = self.streams_seeds_cache.entry(group_id.into()).or_default();
        cached.current_per_member.insert(member_id.into(), v);
    }

    /// Apply a tombstone for a streams-group key.
    ///
    /// The method removes the matching entry from both `streams_seeds` and
    /// `streams_seeds_cache`.
    ///
    /// A `GroupMetadata` k15 tombstone is the load-bearing downgrade tombstone
    /// of KIP-1071. It removes the whole seed, so `finalize_bootstrap` does
    /// not respawn the group as streams. It also removes the `Streams` type
    /// lock, so a classic `GroupMetadata` k2 write that comes later can lock
    /// the group again as `Classic`.
    pub fn replay_streams_tombstone(&self, key: &streams::persistence::StreamsGroupKey) {
        use streams::persistence::StreamsGroupKey as K;
        let group_id = match key {
            K::GroupMetadata { group_id }
            | K::MemberMetadata { group_id, .. }
            | K::Topology { group_id }
            | K::PartitionMetadata { group_id }
            | K::TargetAssignmentMetadata { group_id }
            | K::TargetAssignmentMember { group_id, .. }
            | K::CurrentMemberAssignment { group_id, .. } => group_id.as_str(),
        };
        // k15 GroupMetadata tombstone: purge the whole seed so finalize_bootstrap
        // does not respawn this group as streams; also drop the Streams type lock
        // so a later classic join can re-lock it as Classic.
        if matches!(key, K::GroupMetadata { .. }) {
            self.streams_seeds.remove(group_id);
            self.streams_seeds_cache.remove(group_id);
            self.group_types.remove(group_id);
            return;
        }
        let scrub = |seed: &mut StreamsGroupSeed| match key {
            K::GroupMetadata { .. } => unreachable!("handled above"),
            K::MemberMetadata { member_id, .. } => {
                seed.members.remove(member_id);
            }
            K::Topology { .. } => seed.topology = None,
            K::PartitionMetadata { .. } => seed.partition_metadata = None,
            K::TargetAssignmentMetadata { .. } => seed.assignment_epoch = 0,
            K::TargetAssignmentMember { member_id, .. } => {
                seed.target_per_member.remove(member_id);
            }
            K::CurrentMemberAssignment { member_id, .. } => {
                seed.current_per_member.remove(member_id);
            }
        };
        {
            if let Some(mut s) = self.streams_seeds.get_mut(group_id) {
                scrub(s.value_mut());
            }
        }
        if let Some(mut s) = self.streams_seeds_cache.get_mut(group_id) {
            scrub(s.value_mut());
        }
    }

    pub fn finalize_bootstrap(self: &Arc<Self>) {
        let group_ids: Vec<String> = self.seeds.iter().map(|e| e.key().clone()).collect();
        for gid in group_ids {
            if let Some((_, seed)) = self.seeds.remove(&gid) {
                let handle = self.get_or_create_consumer(&gid);
                let _ = handle.tx.try_send(actor::GroupActorMessage::Seed(seed));
            }
        }
        let share_ids: Vec<String> = self.share_seeds.iter().map(|e| e.key().clone()).collect();
        for gid in share_ids {
            if let Some((_, seed)) = self.share_seeds.remove(&gid) {
                let handle = self.get_or_create_share(&gid);
                let _ = handle.tx.try_send(ShareGroupActorMessage::Seed(seed));
            }
        }
        let streams_ids: Vec<String> = self.streams_seeds.iter().map(|e| e.key().clone()).collect();
        for gid in streams_ids {
            if let Some((_, seed)) = self.streams_seeds.remove(&gid) {
                let handle = self.get_or_create_streams(&gid);
                let _ = handle.tx.try_send(StreamsGroupActorMessage::Seed(seed));
            }
        }
    }
}

/// `MetadataProvider` backed by `crabka_raft::ControllerHandle::current_image()`.
pub struct ImageMetadataProvider {
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
}

impl std::fmt::Debug for ImageMetadataProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImageMetadataProvider")
            .finish_non_exhaustive()
    }
}

impl MetadataProvider for ImageMetadataProvider {
    fn snapshot(&self) -> reconciler::ReconcileInput {
        use crabka_protocol::primitives::uuid::Uuid as ProtoUuid;
        let image = self.controller.current_image();
        let mut topic_id_by_name = std::collections::HashMap::new();
        let mut partitions_per_topic = std::collections::HashMap::new();
        let mut partition_racks: std::collections::HashMap<(ProtoUuid, i32), Vec<String>> =
            std::collections::HashMap::new();
        for topic in image.topics() {
            let proto_id = ProtoUuid(*topic.topic_id.as_bytes());
            topic_id_by_name.insert(topic.name.clone(), proto_id);
            partitions_per_topic.insert(proto_id, topic.partitions);
            // Collect the set of racks the partition's
            // replicas are on, so the rack-aware UniformAssignor can
            // prefer rack-collocated subscribers. Partitions whose
            // replicas have no rack info don't get an entry — the
            // assignor then falls back to its non-rack-aware path.
            for pr in image.partitions_of(&topic.name) {
                let mut racks: Vec<String> = pr
                    .replicas
                    .iter()
                    .filter_map(|&node_id| image.broker(node_id).and_then(|b| b.rack.clone()))
                    .collect();
                racks.sort();
                racks.dedup();
                if !racks.is_empty() {
                    partition_racks.insert((proto_id, pr.partition), racks);
                }
            }
        }
        reconciler::ReconcileInput {
            topic_id_by_name,
            partitions_per_topic,
            partition_racks,
        }
    }
}

/// Hydration seed that the bootstrap replayer passes into a freshly-spawned
/// [`actor::GroupActorHandle`].
///
/// All fields come directly from records decoded out of `__consumer_offsets`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct GroupSeed {
    pub group_epoch: i32,
    pub target_epoch: i32,
    pub members: std::collections::HashMap<String, persistence_next_gen::MemberMetadataValue>,
    pub target_per_member:
        std::collections::HashMap<String, persistence_next_gen::TargetAssignmentMemberValue>,
    pub current_per_member:
        std::collections::HashMap<String, persistence_next_gen::CurrentMemberAssignmentValue>,
}

/// Hydration seed for a [`share::actor::ShareGroupActorHandle`].
///
/// All fields come from share-group records decoded out of
/// `__consumer_offsets`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ShareGroupSeed {
    pub group_epoch: i32,
    pub target_epoch: i32,
    pub members:
        std::collections::HashMap<String, share::persistence::ShareGroupMemberMetadataValue>,
    pub target_per_member: std::collections::HashMap<
        String,
        share::persistence::ShareGroupTargetAssignmentMemberValue,
    >,
    pub current_per_member: std::collections::HashMap<
        String,
        share::persistence::ShareGroupCurrentMemberAssignmentValue,
    >,
    /// KIP-932 `ShareGroupStatePartitionMetadata`, key v14. It holds the
    /// `(topic_id, partition)` share-states this group has already
    /// initialized, and the topic ids whose share-state the broker deletes.
    /// The lifecycle hook can then skip a re-initialization of those
    /// partitions on restart.
    pub state_partition_metadata: share::persistence::ShareGroupStatePartitionMetadataValue,
}

/// Hydration seed for a [`streams::actor::StreamsGroupActorHandle`], per
/// KIP-1071.
///
/// All fields come from streams-group records decoded out of
/// `__consumer_offsets`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct StreamsGroupSeed {
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub topology: Option<streams::persistence::StreamsGroupTopologyValue>,
    pub partition_metadata: Option<streams::persistence::StreamsGroupPartitionMetadataValue>,
    pub members:
        std::collections::HashMap<String, streams::persistence::StreamsGroupMemberMetadataValue>,
    pub target_per_member: std::collections::HashMap<
        String,
        streams::persistence::StreamsGroupTargetAssignmentMemberValue,
    >,
    pub current_per_member: std::collections::HashMap<
        String,
        streams::persistence::StreamsGroupCurrentMemberAssignmentValue,
    >,
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    /// Yield-poll until `cond` holds.
    ///
    /// A bounded hang-guard makes a real stall fail the test in a
    /// deterministic way, and the loop does not spin forever.
    async fn await_until(what: &str, mut cond: impl FnMut() -> bool) {
        for _ in 0..200_000 {
            if cond() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition never held: {what}");
    }

    #[test]
    fn group_type_has_share_variant() {
        // KIP-932: a third locked group type alongside Classic and NextGen.
        let t = GroupType::Share;
        check!(t == GroupType::Share);
        check!(t != GroupType::Classic);
        check!(t != GroupType::NextGen);
    }

    fn make_coord() -> Arc<GroupCoordinator> {
        make_coord_with_log().0
    }

    fn make_coord_with_log() -> (
        Arc<GroupCoordinator>,
        Arc<crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog>,
    ) {
        use crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog;
        let metadata: Arc<dyn MetadataProvider> = Arc::new(ImageMetadatalessProvider);
        let offsets_log = Arc::new(InMemoryOffsetsLog::default());
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig::default(),
            ShareGroupConfig::default(),
            metadata,
            offsets_log.clone(),
            StreamsGroupConfig::default(),
        ));
        (coord, offsets_log)
    }

    #[tokio::test]
    async fn actor_mailboxes_use_component_configuration() {
        use crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog;

        let metadata: Arc<dyn MetadataProvider> = Arc::new(ImageMetadatalessProvider);
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig {
                actor_mailbox_capacity: 3,
                ..NextGenConfig::default()
            },
            ShareGroupConfig {
                actor_mailbox_capacity: 5,
                ..ShareGroupConfig::default()
            },
            metadata,
            Arc::new(InMemoryOffsetsLog::default()),
            StreamsGroupConfig {
                actor_mailbox_capacity: 7,
                ..StreamsGroupConfig::default()
            },
        ));

        assert!(coord.get_or_create_classic("classic").tx.max_capacity() == 3);
        assert!(coord.get_or_create_share("share").tx.max_capacity() == 5);
        assert!(coord.get_or_create_streams("streams").tx.max_capacity() == 7);
    }

    #[derive(Debug)]
    struct ImageMetadatalessProvider;
    impl MetadataProvider for ImageMetadatalessProvider {
        fn snapshot(&self) -> reconciler::ReconcileInput {
            reconciler::ReconcileInput::default()
        }
    }

    #[derive(Debug)]
    struct FixedMetadataSource {
        image: Arc<crabka_metadata::MetadataImage>,
        leader_tx: tokio::sync::watch::Sender<Option<crabka_raft::NodeId>>,
    }

    impl FixedMetadataSource {
        fn new(image: crabka_metadata::MetadataImage) -> Self {
            let (leader_tx, _) = tokio::sync::watch::channel(Some(crabka_raft::NodeId(1)));
            Self {
                image: Arc::new(image),
                leader_tx,
            }
        }
    }

    fn unsupported() -> crabka_raft::RaftError {
        crabka_raft::RaftError::Unsupported("fixed metadata source")
    }

    #[async_trait::async_trait]
    impl crate::metadata_source::MetadataSource for FixedMetadataSource {
        fn current_image(&self) -> Arc<crabka_metadata::MetadataImage> {
            self.image.clone()
        }

        fn watch_image(&self) -> tokio::sync::watch::Receiver<Arc<crabka_metadata::MetadataImage>> {
            let (_, rx) = tokio::sync::watch::channel(self.image.clone());
            rx
        }

        fn watch_leader(&self) -> tokio::sync::watch::Receiver<Option<crabka_raft::NodeId>> {
            self.leader_tx.subscribe()
        }

        fn quorum_state(&self) -> crabka_raft::QuorumState {
            crabka_raft::QuorumState {
                current_term: 0,
                last_applied_index: 0,
                current_leader: *self.leader_tx.borrow(),
                voters: Vec::new(),
                voter_nodes: std::collections::BTreeMap::new(),
                per_voter_matched_index: std::collections::BTreeMap::new(),
            }
        }

        async fn submit_change(
            &self,
            _records: Vec<crabka_metadata::MetadataRecord>,
        ) -> Result<crabka_raft::SubmitChangeResult, crabka_raft::RaftError> {
            Ok(crabka_raft::SubmitChangeResult::default())
        }

        async fn change_membership(
            &self,
            _new_voters: std::collections::BTreeSet<crabka_raft::NodeId>,
        ) -> Result<(), crabka_raft::RaftError> {
            Err(unsupported())
        }

        async fn add_learner(
            &self,
            _node_id: crabka_raft::NodeId,
            _node: crabka_raft::Node,
        ) -> Result<(), crabka_raft::RaftError> {
            Err(unsupported())
        }

        fn controller_bound_addr(&self) -> std::net::SocketAddr {
            std::net::SocketAddr::from(([0, 0, 0, 0], 0))
        }

        fn read_snapshot_range(
            &self,
            _position: i64,
            _max_bytes: i32,
        ) -> crabka_raft::SnapshotRange {
            crabka_raft::SnapshotRange::NoSnapshot
        }

        async fn trigger_snapshot(&self) -> Result<(), crabka_raft::RaftError> {
            Err(unsupported())
        }

        async fn add_voter(
            &self,
            _req: crabka_raft::AddVoter,
        ) -> Result<crabka_raft::ReconfigOutcome, crabka_raft::RaftError> {
            Err(unsupported())
        }

        async fn remove_voter(
            &self,
            _req: crabka_raft::RemoveVoter,
        ) -> Result<crabka_raft::ReconfigOutcome, crabka_raft::RaftError> {
            Err(unsupported())
        }

        async fn update_voter(
            &self,
            _req: crabka_raft::UpdateVoter,
        ) -> Result<crabka_raft::ReconfigOutcome, crabka_raft::RaftError> {
            Err(unsupported())
        }

        async fn cancel(&self) {}
    }

    fn fixed_source(
        image: crabka_metadata::MetadataImage,
    ) -> Arc<dyn crate::metadata_source::MetadataSource> {
        Arc::new(FixedMetadataSource::new(image))
    }

    fn make_share_persister(
        source: Arc<dyn crate::metadata_source::MetadataSource>,
    ) -> Arc<crate::share_coordinator::persister_client::SharePersister> {
        let share_coordinator = Arc::new(
            crate::share_coordinator::coordinator::ShareCoordinator::new(
                crabka_metadata::NodeId(1),
                Arc::new(crate::partition_registry::PartitionRegistry::new()),
                crate::share_coordinator::config::ShareCoordinatorConfig::default(),
            ),
        );
        Arc::new(
            crate::share_coordinator::persister_client::SharePersister::new(
                crabka_metadata::NodeId(1),
                share_coordinator,
                source,
                Arc::new(crate::network::client::InterBrokerClient::new(None, None)),
                crabka_security::ListenerProtocol::Plaintext,
                "PLAINTEXT".into(),
            ),
        )
    }

    fn proto_uuid(byte: u8) -> crabka_protocol::primitives::uuid::Uuid {
        crabka_protocol::primitives::uuid::Uuid([byte; 16])
    }

    fn real_uuid(byte: u8) -> uuid::Uuid {
        uuid::Uuid::from_bytes([byte; 16])
    }

    fn next_member(client_id: &str) -> persistence_next_gen::MemberMetadataValue {
        persistence_next_gen::MemberMetadataValue {
            instance_id: Some(format!("{client_id}-instance")),
            rack_id: Some("rack-a".into()),
            client_id: client_id.into(),
            client_host: "host".into(),
            subscribed_topic_names: vec!["topic-a".into()],
            subscribed_topic_regex: Some("topic-.*".into()),
            server_assignor: Some("range".into()),
            rebalance_timeout_ms: 45_000,
            classic: None,
        }
    }

    fn next_current(epoch: i32) -> persistence_next_gen::CurrentMemberAssignmentValue {
        persistence_next_gen::CurrentMemberAssignmentValue {
            member_epoch: epoch,
            previous_member_epoch: epoch - 1,
            state: persistence_next_gen::MemberAssignmentState::Stable,
            assigned_partitions: vec![persistence_next_gen::AssignedTopicPartitions {
                topic_id: proto_uuid(1),
                partitions: vec![0, 1],
            }],
            partitions_pending_revocation: vec![],
        }
    }

    fn share_member(client_id: &str) -> share::persistence::ShareGroupMemberMetadataValue {
        share::persistence::ShareGroupMemberMetadataValue {
            rack_id: Some("rack-b".into()),
            client_id: client_id.into(),
            client_host: "host".into(),
            subscribed_topic_names: vec!["share-topic".into()],
        }
    }

    fn streams_member(client_id: &str) -> streams::persistence::StreamsGroupMemberMetadataValue {
        streams::persistence::StreamsGroupMemberMetadataValue {
            instance_id: Some(format!("{client_id}-instance")),
            rack_id: Some("rack-c".into()),
            client_id: client_id.into(),
            client_host: "host".into(),
            process_id: "process".into(),
            user_endpoint: Some(streams::persistence::StreamsEndpoint {
                host: "localhost".into(),
                port: 8080,
            }),
            client_tags: vec![("app".into(), "streams".into())],
            rebalance_timeout_ms: 30_000,
            topology_epoch: 4,
        }
    }

    #[test]
    fn mark_share_locks_group_type() {
        let coord = make_coord();
        coord.mark_share("sg");
        assert!(coord.group_type("sg") == Some(GroupType::Share));
        // First mark wins: a later mark_classic must not override.
        coord.mark_classic("sg");
        assert!(coord.group_type("sg") == Some(GroupType::Share));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_or_create_group_returns_the_one_actor_regardless_of_kind() {
        // KIP-848 live migration: BOTH RPC families route to the one actor.
        // The kind argument only decides the spawn kind for a brand-new group;
        // a later request of the other kind returns the SAME actor (the kind
        // lock now lives in the actor's message arms, not in this registry).
        let coord = make_coord();
        let a = coord.get_or_create_group("g", GroupKindTag::Classic);
        let b = coord.get_or_create_group("g", GroupKindTag::Consumer);
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_or_create_share_is_idempotent() {
        let coord = make_coord();
        let a = coord.get_or_create_share("sg");
        let b = coord.get_or_create_share("sg");
        assert!(Arc::ptr_eq(&a, &b));
        assert!(coord.find_share("sg").is_some());
    }

    #[test]
    fn mark_streams_after_upgrade_forces_streams_over_classic() {
        let c = make_coord();
        c.mark_classic("g");
        assert!(c.group_type("g") == Some(GroupType::Classic));
        // or_insert mark_streams must NOT override an existing Classic lock:
        c.mark_streams("g");
        assert!(c.group_type("g") == Some(GroupType::Classic));
        // The forced upgrade variant MUST override it:
        c.mark_streams_after_upgrade("g");
        assert!(c.group_type("g") == Some(GroupType::Streams));
    }

    #[test]
    fn mark_classic_after_streams_downgrade_forces_classic_over_streams() {
        let c = make_coord();
        c.mark_streams("g");
        assert_eq!(c.group_type("g"), Some(GroupType::Streams));
        // mark_classic is first-mark-wins, so it must NOT override an existing lock:
        c.mark_classic("g");
        assert_eq!(c.group_type("g"), Some(GroupType::Streams));
        // The forced downgrade variant MUST override it:
        c.mark_classic_after_streams_downgrade("g");
        assert_eq!(c.group_type("g"), Some(GroupType::Classic));
    }

    #[test]
    fn share_state_partition_metadata_none_then_some() {
        let coord = make_coord();
        // Unknown group → None.
        assert!(coord.share_state_partition_metadata("sg").is_none());

        let tid = uuid::Uuid::from_u128(1);
        let v = share::persistence::ShareGroupStatePartitionMetadataValue {
            initialized: vec![(tid, vec![0, 1])],
            deleting: vec![],
        };
        coord.replay_share_state_partition_metadata("sg", v.clone());
        // Some after a replay, with the same contents.
        assert!(coord.share_state_partition_metadata("sg") == Some(v));
    }

    #[test]
    fn debug_wrappers_write_type_names() {
        let source = fixed_source(crabka_metadata::MetadataImage::new(real_uuid(1)));
        assert!(
            format!("{:?}", MetadataSourceHandle(source.clone())).contains("MetadataSourceHandle")
        );
        assert!(
            format!("{:?}", ImageMetadataProvider { controller: source })
                .contains("ImageMetadataProvider")
        );
    }

    #[test]
    fn once_lock_getters_return_installed_first_values() {
        let coord = make_coord();
        assert!(coord.metadata_source().is_none());
        assert!(coord.share_persister().is_none());

        let first_source = fixed_source(crabka_metadata::MetadataImage::new(real_uuid(1)));
        let second_source = fixed_source(crabka_metadata::MetadataImage::new(real_uuid(2)));
        coord.set_metadata_source(first_source.clone());
        coord.set_metadata_source(second_source);
        let got_source = coord.metadata_source().unwrap();
        assert!(Arc::ptr_eq(&got_source, &first_source));

        let first_persister = make_share_persister(first_source.clone());
        let second_persister = make_share_persister(first_source);
        coord.set_share_persister(first_persister.clone());
        coord.set_share_persister(second_persister);
        let got_persister = coord.share_persister().unwrap();
        assert!(Arc::ptr_eq(got_persister, &first_persister));
    }

    #[test]
    fn cache_updates_and_forced_type_transitions_are_observable() {
        let coord = make_coord();
        check!(coord.cached_seed("g") == None);
        check!(coord.cached_share_seed("sg") == None);
        check!(coord.cached_streams_seed("st") == None);

        coord.update_cached_seed("g", |seed| {
            seed.group_epoch = 7;
            seed.target_epoch = 8;
        });
        let cached = coord.cached_seed("g").unwrap();
        assert!(cached.group_epoch == 7);
        assert!(cached.target_epoch == 8);

        coord.seeds.insert(
            "g".into(),
            GroupSeed {
                group_epoch: 99,
                ..GroupSeed::default()
            },
        );
        coord.mark_next_gen("g");
        assert!(coord.group_type("g") == Some(GroupType::NextGen));
        coord.mark_classic_after_downgrade("g");
        check!(coord.group_type("g") == Some(GroupType::Classic));
        check!(coord.seeds.get("g").is_none());
        check!(coord.cached_seed("g") == None);

        coord.update_share_cache(
            "sg",
            ShareGroupSeed {
                group_epoch: 17,
                target_epoch: 18,
                ..ShareGroupSeed::default()
            },
        );
        let share_cached = coord.cached_share_seed("sg").unwrap();
        assert!(share_cached.group_epoch == 17);
        assert!(share_cached.target_epoch == 18);

        coord.update_streams_cache(
            "st",
            StreamsGroupSeed {
                group_epoch: 27,
                assignment_epoch: 28,
                ..StreamsGroupSeed::default()
            },
        );
        let streams_cached = coord.cached_streams_seed("st").unwrap();
        assert!(streams_cached.group_epoch == 27);
        assert!(streams_cached.assignment_epoch == 28);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn share_and_streams_registries_report_live_ids() {
        let coord = make_coord();
        let share_a = coord.get_or_create_share("share-a");
        let share_b = coord.get_or_create_share("share-b");
        check!(Arc::ptr_eq(&share_a, &coord.get_or_create_share("share-a")));
        check!(coord.find_share("share-b").is_some());
        check!(!Arc::ptr_eq(&share_a, &share_b));

        let streams_a = coord.get_or_create_streams("streams-a");
        assert!(Arc::ptr_eq(
            &streams_a,
            &coord.get_or_create_streams("streams-a")
        ));
        assert!(Arc::ptr_eq(
            &streams_a,
            &coord.find_streams("streams-a").unwrap()
        ));

        let mut share_ids = coord.share_group_ids();
        share_ids.sort();
        assert!(share_ids == vec!["share-a".to_string(), "share-b".to_string()]);

        let mut streams_ids = coord.streams_group_ids();
        streams_ids.sort();
        assert!(streams_ids == vec!["streams-a".to_string()]);

        coord.shutdown_all().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn conversion_paths_update_type_locks_and_report_missing_streams() {
        let (coord, offsets_log) = make_coord_with_log();
        assert!(
            coord
                .try_convert_classic_to_streams("fresh", 100)
                .await
                .unwrap()
                == streams::migration::ConvertOutcome::NotClassic
        );

        coord.mark_classic("g");
        check!(
            coord
                .try_convert_classic_to_streams("g", 101)
                .await
                .unwrap()
                == streams::migration::ConvertOutcome::Converted
        );
        check!(coord.group_type("g") == Some(GroupType::Streams));
        check!(offsets_log.appended.lock().await.len() == 1);

        check!(
            coord
                .try_convert_streams_to_classic("fresh", 102)
                .await
                .unwrap()
                == streams::migration::DowngradeOutcome::NotStreams
        );
        check!(
            coord
                .try_convert_streams_to_classic("g", 103)
                .await
                .unwrap()
                == streams::migration::DowngradeOutcome::Converted
        );
        check!(coord.group_type("g") == Some(GroupType::Classic));
        check!(offsets_log.appended.lock().await.len() == 2);

        coord.mark_streams("missing-streams-actor");
        assert!(
            coord.delete_group("missing-streams-actor").await == Err(DeleteGroupError::NotFound)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_all_closes_all_group_actors() {
        let coord = make_coord();
        let group = coord.get_or_create_classic("classic");
        let share = coord.get_or_create_share("share");
        let streams = coord.get_or_create_streams("streams");

        coord.shutdown_all().await;

        // The ack can arrive a scheduler tick before the actor task exits
        // and drops its receiver — poll instead of racing it.
        await_until("all group actor channels closed", || {
            group.tx.is_closed() && share.tx.is_closed() && streams.tx.is_closed()
        })
        .await;
        assert!(group.tx.is_closed());
        assert!(share.tx.is_closed());
        assert!(streams.tx.is_closed());
    }

    #[test]
    fn next_gen_replay_populates_seed_and_cache() {
        let coord = make_coord();
        let member = next_member("member-a");
        let target = persistence_next_gen::TargetAssignmentMemberValue {
            topic_partitions: vec![persistence_next_gen::AssignedTopicPartitions {
                topic_id: proto_uuid(3),
                partitions: vec![1, 2],
            }],
        };
        let current = next_current(5);

        coord.replay_group_metadata("g", persistence_next_gen::GroupMetadataValue { epoch: 11 });
        coord.replay_member_metadata("g", "member-a", member.clone());
        coord.replay_target_assignment_metadata(
            "g",
            persistence_next_gen::TargetAssignmentMetadataValue {
                assignment_epoch: 12,
            },
        );
        coord.replay_target_assignment_member("g", "member-a", target.clone());
        coord.replay_current_member_assignment("g", "member-a", current.clone());

        let expected = GroupSeed {
            group_epoch: 11,
            target_epoch: 12,
            members: std::collections::HashMap::from([("member-a".to_string(), member)]),
            target_per_member: std::collections::HashMap::from([("member-a".to_string(), target)]),
            current_per_member: std::collections::HashMap::from([(
                "member-a".to_string(),
                current,
            )]),
        };
        assert!(*coord.seeds.get("g").unwrap() == expected);
        assert!(coord.cached_seed("g") == Some(expected));
    }

    #[test]
    fn share_replay_populates_seed_and_cache() {
        let coord = make_coord();
        let member = share_member("share-member");
        let target = share::persistence::ShareGroupTargetAssignmentMemberValue {
            topic_partitions: vec![(proto_uuid(4), vec![0, 3])],
        };
        let current = share::persistence::ShareGroupCurrentMemberAssignmentValue {
            member_epoch: 6,
            assigned_partitions: vec![(proto_uuid(4), vec![1])],
        };

        coord.replay_share_group_metadata(
            "sg",
            share::persistence::ShareGroupMetadataValue { epoch: 21 },
        );
        coord.replay_share_member_metadata("sg", "share-member", member.clone());
        coord.replay_share_target_assignment_metadata(
            "sg",
            share::persistence::ShareGroupTargetAssignmentMetadataValue {
                assignment_epoch: 22,
            },
        );
        coord.replay_share_target_assignment_member("sg", "share-member", target.clone());
        coord.replay_share_current_member_assignment("sg", "share-member", current.clone());

        let expected = ShareGroupSeed {
            group_epoch: 21,
            target_epoch: 22,
            members: std::collections::HashMap::from([("share-member".to_string(), member)]),
            target_per_member: std::collections::HashMap::from([(
                "share-member".to_string(),
                target,
            )]),
            current_per_member: std::collections::HashMap::from([(
                "share-member".to_string(),
                current,
            )]),
            state_partition_metadata: share::persistence::ShareGroupStatePartitionMetadataValue {
                initialized: vec![],
                deleting: vec![],
            },
        };
        assert!(*coord.share_seeds.get("sg").unwrap() == expected);
        assert!(coord.cached_share_seed("sg") == Some(expected));
    }

    #[test]
    fn streams_replay_populates_seed_and_cache() {
        let coord = make_coord();
        let member = streams_member("streams-member");
        let topology = streams::persistence::StreamsGroupTopologyValue {
            epoch: 31,
            subtopologies: vec![streams::persistence::StoredSubtopology {
                subtopology_id: "subtopology-a".into(),
                source_topics: vec!["input".into()],
                source_topic_regex: vec!["input-.*".into()],
                repartition_sink_topics: vec!["sink".into()],
                state_changelog_topics: vec![streams::persistence::StoredTopicInfo {
                    name: "store-changelog".into(),
                    partitions: 2,
                    replication_factor: 1,
                    topic_configs: vec![("cleanup.policy".into(), "compact".into())],
                }],
                repartition_source_topics: vec![],
                copartition_groups: vec![],
            }],
        };
        let partition_metadata = streams::persistence::StreamsGroupPartitionMetadataValue {
            topics: vec![streams::persistence::StreamsTopicMeta {
                topic_name: "input".into(),
                topic_id: real_uuid(5),
                num_partitions: 2,
            }],
        };
        let mut active = std::collections::BTreeMap::new();
        active.insert("subtopology-a".into(), vec![0, 1]);
        let target = streams::persistence::StreamsGroupTargetAssignmentMemberValue {
            active: active.clone(),
            ..Default::default()
        };
        let current = streams::persistence::StreamsGroupCurrentMemberAssignmentValue {
            member_epoch: 7,
            previous_member_epoch: 6,
            state: 1,
            active,
            ..Default::default()
        };

        coord.replay_streams_group_metadata("st", 30);
        coord.replay_streams_member_metadata("st", "streams-member", member.clone());
        coord.replay_streams_topology("st", topology.clone());
        coord.replay_streams_partition_metadata("st", partition_metadata.clone());
        coord.replay_streams_target_assignment_metadata("st", 32);
        coord.replay_streams_target_assignment_member("st", "streams-member", target.clone());
        coord.replay_streams_current_member_assignment("st", "streams-member", current.clone());

        let expected = StreamsGroupSeed {
            group_epoch: 30,
            assignment_epoch: 32,
            topology: Some(topology),
            partition_metadata: Some(partition_metadata),
            members: std::collections::HashMap::from([("streams-member".to_string(), member)]),
            target_per_member: std::collections::HashMap::from([(
                "streams-member".to_string(),
                target,
            )]),
            current_per_member: std::collections::HashMap::from([(
                "streams-member".to_string(),
                current,
            )]),
        };
        assert!(*coord.streams_seeds.get("st").unwrap() == expected);
        assert!(coord.cached_streams_seed("st") == Some(expected));
    }

    #[test]
    fn image_metadata_provider_snapshot_projects_topics_partitions_and_racks() {
        let mut image = crabka_metadata::MetadataImage::new(real_uuid(9));
        let topic_id = real_uuid(8);
        image.apply(&crabka_metadata::MetadataRecord::V1Topic(
            crabka_metadata::TopicRecord {
                name: "input".into(),
                topic_id,
                partitions: 3,
                replication_factor: 2,
            },
        ));
        for (node_id, rack) in [
            (1, Some("rack-a".to_string())),
            (2, Some("rack-b".to_string())),
            (3, None),
        ] {
            image.apply(&crabka_metadata::MetadataRecord::V1BrokerRegistration(
                crabka_metadata::BrokerRegistrationRecord {
                    node_id: crabka_metadata::NodeId(node_id),
                    broker_epoch: i64::try_from(node_id).unwrap(),
                    incarnation_id: real_uuid(u8::try_from(node_id).unwrap()),
                    host: format!("broker-{node_id}"),
                    port: 9092,
                    rack,
                    endpoints: vec![],
                },
            ));
        }
        image.apply(&crabka_metadata::MetadataRecord::V1Partition(
            crabka_metadata::PartitionRecord {
                topic: "input".into(),
                partition: 0,
                leader: crabka_metadata::NodeId(1),
                replicas: vec![crabka_metadata::NodeId(1), crabka_metadata::NodeId(2)],
                isr: vec![crabka_metadata::NodeId(1), crabka_metadata::NodeId(2)],
                directories: vec![real_uuid(1), real_uuid(2)],
                ..Default::default()
            },
        ));
        image.apply(&crabka_metadata::MetadataRecord::V1Partition(
            crabka_metadata::PartitionRecord {
                topic: "input".into(),
                partition: 1,
                leader: crabka_metadata::NodeId(3),
                replicas: vec![crabka_metadata::NodeId(3)],
                isr: vec![crabka_metadata::NodeId(3)],
                directories: vec![real_uuid(3)],
                ..Default::default()
            },
        ));

        let provider = ImageMetadataProvider {
            controller: fixed_source(image),
        };
        let snapshot = provider.snapshot();
        let proto_topic_id = crabka_protocol::primitives::uuid::Uuid(*topic_id.as_bytes());

        check!(snapshot.topic_id_by_name.get("input") == Some(&proto_topic_id));
        check!(snapshot.partitions_per_topic.get(&proto_topic_id) == Some(&2));
        check!(
            snapshot.partition_racks.get(&(proto_topic_id, 0))
                == Some(&vec!["rack-a".to_string(), "rack-b".to_string()])
        );
        check!(snapshot.partition_racks.get(&(proto_topic_id, 1)) == None);
    }
}
