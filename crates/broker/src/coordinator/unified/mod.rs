//! Unified group-coordinator subsystem (KIP-848 64d-B). Shared infra and
//! persistence for both the classic and next-gen group protocols.
//!
//! [`GroupCoordinator`] is the single owner of the next-gen consumer-group
//! machinery: it spawns per-group actors, tracks each group's locked type,
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

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::oneshot;

use actor::{GroupActorHandle, GroupActorMessage, GroupKindTag, MetadataProvider};
use config::NextGenConfig;
use group::Group;
use offsets_log::OffsetsLog;
use share::actor::{ShareGroupActorHandle, ShareGroupActorMessage};
use share::config::ShareGroupConfig;
use streams::actor::{StreamsGroupActorHandle, StreamsGroupActorMessage};
use streams::config::StreamsGroupConfig;

use crate::coordinator::{DeleteGroupError, GroupSnapshot};

/// Locked protocol identity for a `group_id`. Classic/next-gen actors enforce
/// their lock via the actor's [`GroupKindTag`]; share groups (KIP-932) live in
/// a separate `share_groups` registry and record their lock here so that the
/// classic/next-gen and share namespaces can't collide on the same id.
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
    /// First record persisted per `group_id` locks its type for life (the
    /// classic↔next-gen↔share namespace guard).
    pub group_types: Arc<DashMap<String, GroupType>>,
    /// Bootstrap-time accumulator for next-gen state; drained by
    /// `finalize_bootstrap`.
    pub seeds: Arc<DashMap<String, GroupSeed>>,
    /// Bootstrap-time share-group accumulator; drained by `finalize_bootstrap`.
    pub share_seeds: Arc<DashMap<String, ShareGroupSeed>>,
    /// Last-known-good next-gen state per group, populated alongside every
    /// successful actor write. Used to seed a fresh actor when the
    /// previous instance crashed after a log-write failure.
    pub seeds_cache: Arc<DashMap<String, GroupSeed>>,
    /// Last-known-good share-group state, the share-group analogue of
    /// `seeds_cache`.
    pub share_seeds_cache: Arc<DashMap<String, ShareGroupSeed>>,
    /// KIP-932 group-coordinator → share-state-persister bridge. Set once in
    /// `Broker::start` after both the `ShareCoordinator` and this coordinator
    /// exist. Per-group share actors read it (via [`Self::share_persister`]) to
    /// drive Initialize/Delete lifecycle calls after reconcile. `None` in the
    /// pure-coordinator unit tests, where the lifecycle hook is a no-op.
    pub(crate) share_persister:
        std::sync::OnceLock<Arc<crate::share_coordinator::persister_client::SharePersister>>,

    // ── KIP-1071 streams groups ──────────────────────────────────────────
    pub streams_config: Arc<StreamsGroupConfig>,
    /// Per-`group_id` streams-group actor handles (KIP-1071).
    pub streams_groups: Arc<DashMap<String, Arc<StreamsGroupActorHandle>>>,
    /// Bootstrap-time streams-group accumulator; drained by `finalize_bootstrap`.
    pub streams_seeds: Arc<DashMap<String, StreamsGroupSeed>>,
    /// Last-known-good streams-group state, the streams analogue of
    /// `seeds_cache`.
    pub streams_seeds_cache: Arc<DashMap<String, StreamsGroupSeed>>,
    /// KIP-1071 metadata authority. Set once in `Broker::start`. Per-group
    /// streams actors read it (via [`Self::metadata_source`]) for the full
    /// `MetadataImage` (topology resolution + internal-topic creation). `None`
    /// in the pure-coordinator unit tests, where reconcile no-ops to `NotReady`.
    pub(crate) metadata_source: std::sync::OnceLock<MetadataSourceHandle>,
}

/// `Debug`-able wrapper around an `Arc<dyn MetadataSource>` so it can live in
/// the `#[derive(Debug)]` [`GroupCoordinator`]. The trait object itself is not
/// `Debug`; this prints an opaque placeholder.
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

    /// Install the KIP-932 share-state persister bridge. Called once in
    /// `Broker::start`. A second call is silently ignored (the `OnceLock`
    /// keeps the first value), which keeps construction order-independent.
    pub(crate) fn set_share_persister(
        &self,
        persister: Arc<crate::share_coordinator::persister_client::SharePersister>,
    ) {
        let _ = self.share_persister.set(persister);
    }

    /// The installed share-state persister, if any. `None` in unit tests that
    /// construct a bare `GroupCoordinator`; the lifecycle hook then no-ops.
    #[must_use]
    pub(crate) fn share_persister(
        &self,
    ) -> Option<&Arc<crate::share_coordinator::persister_client::SharePersister>> {
        self.share_persister.get()
    }

    /// Install the KIP-1071 metadata source. Called once in `Broker::start`. A
    /// second call is silently ignored (the `OnceLock` keeps the first value).
    pub(crate) fn set_metadata_source(&self, src: Arc<dyn crate::metadata_source::MetadataSource>) {
        let _ = self.metadata_source.set(MetadataSourceHandle(src));
    }

    /// The installed metadata source, if any. `None` in unit tests that
    /// construct a bare `GroupCoordinator`; streams reconcile then no-ops to
    /// `NotReady`.
    #[must_use]
    pub(crate) fn metadata_source(
        &self,
    ) -> Option<Arc<dyn crate::metadata_source::MetadataSource>> {
        self.metadata_source.get().map(|h| h.0.clone())
    }

    /// Replace the cached seed for `group_id` with `seed`. Called by the
    /// actor after every successful `OffsetsLog::append`.
    pub fn update_cache(&self, group_id: &str, seed: GroupSeed) {
        self.seeds_cache.insert(group_id.into(), seed);
    }

    /// Fetch the most recently cached seed for `group_id`, if any.
    #[must_use]
    pub fn cached_seed(&self, group_id: &str) -> Option<GroupSeed> {
        self.seeds_cache.get(group_id).map(|e| e.value().clone())
    }

    /// The locked protocol type for `group_id`, if recorded. Share groups
    /// (KIP-932) record their lock here via [`mark_share`](Self::mark_share);
    /// classic/next-gen actors additionally enforce their lock through the
    /// actor [`GroupKindTag`].
    #[must_use]
    pub fn group_type(&self, group_id: &str) -> Option<GroupType> {
        self.group_types.get(group_id).map(|e| *e.value())
    }

    pub fn mark_classic(&self, group_id: &str) {
        self.group_types
            .entry(group_id.into())
            .or_insert(GroupType::Classic);
    }

    /// After an in-place downgrade (KIP-848), drop the consumer seed so a
    /// respawn does not re-hydrate the group as next-gen, and record it as
    /// classic. Unlike [`mark_classic`] (first-mark-wins via `or_insert`), this
    /// FORCES the type to `Classic` — a downgrade must override any prior
    /// `NextGen` lock the group carried while it was a consumer group.
    pub fn mark_classic_after_downgrade(&self, group_id: &str) {
        self.seeds.remove(group_id);
        self.seeds_cache.remove(group_id);
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

    /// Replace the cached share-group seed for `group_id`. Called by the
    /// share actor after every successful `OffsetsLog::append`.
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

    /// Replace the cached streams-group seed for `group_id`. Called by the
    /// streams actor after every successful `OffsetsLog::append`.
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

    /// Get the one actor for `group_id`, spawning it with `initial_kind` if
    /// absent.
    ///
    /// The kind argument only decides the spawn kind for a brand-new group.
    /// Both families route to one actor; the actor rejects the family it does
    /// not currently serve. (A later slice lets the actor flip kind in place,
    /// so a group is no longer pinned to its spawn kind.) Keeps the dead-actor
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

    /// Get-or-create a classic-protocol actor. Spawns a classic actor for a
    /// brand-new id; for an existing id returns the one actor regardless of its
    /// kind (the actor serves or rejects per its live kind).
    #[must_use]
    pub fn get_or_create_classic(self: &Arc<Self>, group_id: &str) -> Arc<GroupActorHandle> {
        self.get_or_create_group(group_id, GroupKindTag::Classic)
    }

    /// Get-or-create a next-gen consumer-protocol actor. Spawns a consumer actor
    /// for a brand-new id; for an existing id returns the one actor regardless
    /// of its kind (the actor serves or rejects per its live kind).
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

    /// Snapshot the ids of every live share group (KIP-932). Sync and cheap —
    /// just the registry keys, no actor round-trip — so `ListGroups` (`api_key`
    /// 16) can include share groups alongside classic ones without the
    /// per-group `Describe` mpsc hop.
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

    /// Snapshot the ids of every live streams group (KIP-1071). Mirrors
    /// [`share_group_ids`](Self::share_group_ids); used by `ListGroups` to emit
    /// `group_type="streams"` entries without a per-group `Describe` hop.
    #[must_use]
    pub fn streams_group_ids(&self) -> Vec<String> {
        self.streams_groups
            .iter()
            .map(|e| e.key().clone())
            .collect()
    }

    /// Ids of every live next-gen (KIP-848) consumer group actor. Mirrors
    /// [`share_group_ids`](Self::share_group_ids); used by `ListGroups` to emit
    /// `group_type="consumer"` entries without an actor round-trip.
    ///
    /// Note: this returns all group ids from the shared `groups` map (classic
    /// included). The `ListGroups` handler's `emitted` dedup set prevents a
    /// double wire emission, so a classic group is emitted once as
    /// `group_type="classic"` and not repeated here.
    pub fn consumer_group_ids(&self) -> Vec<String> {
        self.groups.iter().map(|e| e.key().clone()).collect()
    }

    /// Spawn a classic actor seeded with a fully-replayed `Group` (bootstrap).
    pub fn seed_classic(self: &Arc<Self>, group_id: &str, group: Box<Group>) {
        let handle = self.get_or_create_classic(group_id);
        let _ = handle.tx.try_send(GroupActorMessage::ClassicSeed(group));
    }

    /// Snapshot every **live-classic** group for the wire `ListGroups`
    /// (`group_type="classic"`) pass. Iterates ALL handles and discriminates on
    /// the group's LIVE kind, not the spawn-time `handle.kind` hint (KIP-848 live
    /// migration can make them differ): `ClassicInspect`'s arm replies only for a
    /// classic-kind group, so a consumer/upgraded group drops its reply sender
    /// and is skipped here. This keeps `list_groups` the sole producer of the
    /// `classic` rows; consumer-kind groups are surfaced separately by the
    /// `ListGroups` handler via [`consumer_group_ids`](Self::consumer_group_ids)
    /// tagged `group_type="consumer"`, so they are NOT double-counted or
    /// mislabeled. A *downgraded* group whose handle still reads `Consumer`
    /// nonetheless appears here (the live kind is `Classic`).
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

    /// Snapshot a single group (classic OR consumer/migrated), or `None` if
    /// unknown. Inspects the LIVE group via [`InspectAny`] rather than gating on
    /// the spawn-time `handle.kind`, so an upgraded consumer group still reports.
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

    /// Drop a **classic** group from the registry. `NonEmpty` if it still has
    /// live members; `NotFound` if unknown / consumer.
    pub async fn delete_group(&self, group_id: &str) -> Result<(), DeleteGroupError> {
        let handle = self.find(group_id).ok_or(DeleteGroupError::NotFound)?;
        // LIVE kind: a group that downgraded in place is now classic and must be
        // deletable; an upgraded group is consumer and keeps the existing
        // NotFound-for-non-classic behavior. The spawn-time `handle.kind` is
        // stale across a KIP-848 flip.
        if handle.live_kind() != GroupKindTag::Classic {
            return Err(DeleteGroupError::NotFound);
        }
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicInspect { reply: tx })
            .await
            .map_err(|_| DeleteGroupError::NotFound)?;
        let view = rx.await.map_err(|_| DeleteGroupError::NotFound)?;
        if !view.members.is_empty() {
            return Err(DeleteGroupError::NonEmpty);
        }
        self.groups.remove(group_id);
        Ok(())
    }

    pub async fn shutdown_all(&self) {
        let handles: Vec<Arc<GroupActorHandle>> =
            self.groups.iter().map(|e| e.value().clone()).collect();
        for h in handles {
            let (tx, rx) = oneshot::channel();
            if h.tx.send(GroupActorMessage::Shutdown(tx)).await.is_ok() {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), rx).await;
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
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), rx).await;
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

    /// Apply a tombstone for a next-gen key. Removes the corresponding
    /// entry from both `seeds` and `seeds_cache`. Used by bootstrap replay
    /// to honor records with `value = None`.
    ///
    /// A `GroupMetadata` tombstone is the migration DOWNGRADE marker: it drops
    /// the entire next-gen group. Replay must REMOVE the seed (from both
    /// `seeds` and `seeds_cache`) so the group disappears from the next-gen set
    /// `finalize` derives — letting a later classic k2 `GroupMetadata` record
    /// reconstruct it as a CLASSIC group (log order wins). Merely zeroing the
    /// epoch would leave the group classified next-gen and replay it back as an
    /// empty consumer group.
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

    /// Replay a KIP-932 `ShareGroupStatePartitionMetadata` (key v14) record,
    /// recording which `(topic_id, partition)` share-states the group has
    /// initialized so the lifecycle hook can skip re-initialization after a
    /// restart.
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

    /// Read the cached `ShareGroupStatePartitionMetadata` for `group_id`,
    /// recording which `(topic_id, partition)` share-states the group has
    /// initialized. Returns `None` for an unknown group. Drives the admin
    /// offset RPCs (Describe/Alter/Delete `ShareGroupOffsets`), which enumerate
    /// initialized partitions when the request omits an explicit list.
    #[must_use]
    pub fn share_state_partition_metadata(
        &self,
        group_id: &str,
    ) -> Option<share::persistence::ShareGroupStatePartitionMetadataValue> {
        self.share_seeds_cache
            .get(group_id)
            .map(|e| e.value().state_partition_metadata.clone())
    }

    /// Apply a tombstone for a share-group key. Removes the corresponding
    /// entry from both `share_seeds` and `share_seeds_cache`.
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

    /// Apply a tombstone for a streams-group key. Removes the corresponding
    /// entry from both `streams_seeds` and `streams_seeds_cache`.
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
        let scrub = |seed: &mut StreamsGroupSeed| match key {
            K::GroupMetadata { .. } => seed.group_epoch = 0,
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

/// Hydration seed passed from the bootstrap replayer into a freshly-spawned
/// [`actor::GroupActorHandle`]. All fields come directly from records
/// decoded out of `__consumer_offsets`.
#[derive(Debug, Default, Clone)]
pub struct GroupSeed {
    pub group_epoch: i32,
    pub target_epoch: i32,
    pub members: std::collections::HashMap<String, persistence_next_gen::MemberMetadataValue>,
    pub target_per_member:
        std::collections::HashMap<String, persistence_next_gen::TargetAssignmentMemberValue>,
    pub current_per_member:
        std::collections::HashMap<String, persistence_next_gen::CurrentMemberAssignmentValue>,
}

/// Hydration seed for a [`share::actor::ShareGroupActorHandle`]. All fields
/// come from share-group records decoded out of `__consumer_offsets`.
#[derive(Debug, Default, Clone)]
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
    /// KIP-932 `ShareGroupStatePartitionMetadata` (key v14): which
    /// `(topic_id, partition)` share-states this group has already
    /// initialized, plus topic ids whose share-state is being deleted.
    /// Lets the lifecycle hook skip re-initializing partitions on restart.
    pub state_partition_metadata: share::persistence::ShareGroupStatePartitionMetadataValue,
}

/// Hydration seed for a [`streams::actor::StreamsGroupActorHandle`] (KIP-1071).
/// All fields come from streams-group records decoded out of
/// `__consumer_offsets`.
#[derive(Debug, Default, Clone)]
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
    use super::*;
    use assert2::assert;

    #[test]
    fn group_type_has_share_variant() {
        // KIP-932: a third locked group type alongside Classic and NextGen.
        let t = GroupType::Share;
        assert!(t == GroupType::Share);
        assert!(t != GroupType::Classic);
        assert!(t != GroupType::NextGen);
    }

    fn make_coord() -> Arc<GroupCoordinator> {
        use crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog;
        let metadata: Arc<dyn MetadataProvider> = Arc::new(ImageMetadatalessProvider);
        Arc::new(GroupCoordinator::new(
            NextGenConfig::default(),
            ShareGroupConfig::default(),
            metadata,
            Arc::new(InMemoryOffsetsLog::default()),
            StreamsGroupConfig::default(),
        ))
    }

    #[derive(Debug)]
    struct ImageMetadatalessProvider;
    impl MetadataProvider for ImageMetadatalessProvider {
        fn snapshot(&self) -> reconciler::ReconcileInput {
            reconciler::ReconcileInput::default()
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
        // KIP-848 64d live migration: BOTH RPC families route to the one actor.
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
}
