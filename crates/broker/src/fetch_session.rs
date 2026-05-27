//! KIP-227 incremental-fetch-session cache.
//!
//! A `FetchSession` lets a Kafka consumer or replicator send the broker
//! its subscription set once and then issue tiny "delta" fetch requests
//! that only carry the partitions whose desired state has changed
//! (new offset, new max-bytes) plus a `forgotten_topics_data` list of
//! partitions to drop. The broker responds with only the partitions
//! whose state has changed since the previous response. For a
//! caught-up consumer with hundreds of partitions, this collapses a
//! continuous stream of identical-looking fetches into near-zero wire
//! traffic until something changes.
//!
//! ## Wire-level state machine
//!
//! Every `FetchRequest` carries `session_id: i32` and `session_epoch: i32`.
//! Four classes of request fall out:
//!
//! | `session_id` | `session_epoch` | Meaning                                |
//! |--------------|-----------------|----------------------------------------|
//! | 0            | -1 (FINAL)      | Sessionless full fetch (no caching).   |
//! | 0            | 0 (INITIAL)     | Open a new session.                    |
//! | N>0          | E (== expected) | Incremental fetch on existing session. |
//! | N>0          | -1 (FINAL)      | Close the existing session.            |
//!
//! Mismatched epochs return `INVALID_FETCH_SESSION_EPOCH` at the top
//! level of the response; unknown ids return `FETCH_SESSION_ID_NOT_FOUND`.
//!
//! ## Cache & eviction
//!
//! Sessions are held in a single bounded map keyed by allocated id;
//! capacity is `BrokerConfig::max_incremental_fetch_session_cache_slots`.
//! When full, allocation evicts the LRU **non-privileged** session;
//! privileged (follower-fetch, `replica_id >= 0`) sessions are only
//! evicted by other privileged sessions. If no eligible victim exists
//! (cache full of privileged sessions and the caller is non-privileged),
//! `try_allocate` returns `INVALID_SESSION_ID` and the caller falls back
//! to a sessionless response — matching Apache Kafka's behavior.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::time::Instant;

use crabka_protocol::owned::fetch_request::FetchRequest;
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

use crate::codes;

/// Wire sentinel: "no session". A request with `session_id == 0` and
/// `session_epoch == -1` is a sessionless full fetch; a response with
/// `session_id == 0` tells the client that no session was allocated.
pub const INVALID_SESSION_ID: i32 = 0;

/// Wire sentinel: "open a new session". A request with `session_id == 0`
/// and `session_epoch == 0` asks the broker to allocate a new session.
pub const INITIAL_EPOCH: i32 = 0;

/// Wire sentinel: "no session" / "close session". On a request with
/// `session_id == 0`, `FINAL_EPOCH` means a sessionless full fetch. On
/// a request with `session_id != 0`, it means close the named session.
pub const FINAL_EPOCH: i32 = -1;

/// Compute the epoch the broker expects on the next request after a
/// successful incremental fetch. Wraps from `i32::MAX` back to `1`,
/// skipping the two reserved sentinels (`0` = INITIAL, `-1` = FINAL).
#[must_use]
pub fn next_epoch(prev: i32) -> i32 {
    let n = prev.wrapping_add(1);
    if n <= 0 { 1 } else { n }
}

/// (`topic_name`, `topic_id`, partition) — both name and id are kept because
/// Fetch v ≤ 12 sends only the name, v ≥ 13 sends only the id, and the
/// cache must resolve regardless of which version the client uses.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FetchSessionKey {
    pub topic_name: String,
    pub topic_id: WireUuid,
    pub partition: i32,
}

/// Per-partition cached state. The first block (`fetch_offset` etc.) tracks
/// what the client wants on the next read. The `last_*` block tracks what
/// we sent in the previous response — used to decide whether the next
/// response should include this partition (KIP-227 omits a partition
/// when nothing has changed since the previous response).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CachedPartitionState {
    pub fetch_offset: i64,
    pub last_fetched_epoch: i32,
    pub current_leader_epoch: i32,
    pub max_bytes: i32,
    pub log_start_offset: i64,
    pub last_high_watermark: i64,
    pub last_last_stable_offset: i64,
    pub last_log_start_offset: i64,
    pub last_preferred_read_replica: i32,
    pub last_aborted_txns_hash: u64,
    pub last_error_code: i16,
}

pub struct FetchSession {
    pub id: i32,
    /// The epoch the *next* incremental request must carry. Initialized
    /// to `1` on allocation and bumped after each successful incremental
    /// fetch.
    pub next_epoch: i32,
    pub privileged: bool,
    pub creator_principal: String,
    pub partitions: HashMap<FetchSessionKey, CachedPartitionState>,
    pub last_used: Instant,
}

/// Outcome of `FetchSessionCache::classify`. The handler dispatches on
/// this before doing any reads.
#[derive(Debug)]
pub enum SessionDecision {
    /// `(session_id=0, epoch=-1)` — serve from `req.topics`, no caching,
    /// response `session_id = 0`.
    Sessionless,
    /// `(session_id=0, epoch=0)` — serve from `req.topics`, then ask the
    /// cache to allocate a new session for the result. The allocation may
    /// refuse if the cache is full of privileged sessions; in that case
    /// the response `session_id` is `0` and the client falls back to
    /// sessionless fetches next time.
    NewSession,
    /// `(session_id!=0, epoch>=0)` matching the cached epoch — serve from
    /// the cached subscription set (with `req.topics` merged in as
    /// updates/new entries and `forgotten_topics_data` removed). Response
    /// only contains partitions whose state has changed.
    Incremental {
        session_id: i32,
        /// Already-incremented; goes nowhere on the wire (response has no
        /// epoch field) — but the cache uses it as the *next* expected
        /// epoch for the following request.
        new_epoch: i32,
        partitions: Vec<(FetchSessionKey, CachedPartitionState)>,
    },
    /// `(session_id!=0, epoch=-1)` — serve from `req.topics` like a
    /// sessionless fetch, then drop the cached session.
    Close { session_id: i32 },
    /// Protocol violation — emit an empty response with this top-level
    /// `error_code` and `session_id = 0`.
    Error { code: i16 },
}

struct Inner {
    sessions: HashMap<i32, FetchSession>,
}

pub struct FetchSessionCache {
    inner: Mutex<Inner>,
    next_id: AtomicI32,
    max_slots: usize,
    evictions: AtomicU64,
}

impl FetchSessionCache {
    #[must_use]
    pub fn new(max_slots: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                sessions: HashMap::new(),
            }),
            // Id allocation starts at 1 — id 0 is reserved as the
            // INVALID_SESSION_ID sentinel.
            next_id: AtomicI32::new(1),
            max_slots,
            evictions: AtomicU64::new(0),
        }
    }

    /// Number of live sessions in the cache. Cheap snapshot.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().expect("poisoned").sessions.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Sum of `session.partitions.len()` across every live session. Used
    /// by the metrics sampler.
    #[must_use]
    pub fn total_partitions_cached(&self) -> usize {
        self.inner
            .lock()
            .expect("poisoned")
            .sessions
            .values()
            .map(|s| s.partitions.len())
            .sum()
    }

    /// Cumulative count of eviction events since `new()`. One increment
    /// per session displaced by an allocation; does *not* count refused
    /// allocations (those don't displace anything).
    #[must_use]
    pub fn evictions_total(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Inspect the request and decide which of the four branches the
    /// fetch handler should take. On `Incremental`, atomically:
    /// - validates the epoch,
    /// - removes `forgotten_topics_data` from the cached partition set,
    /// - merges `req.topics` into the cached set (updates `fetch_offset` etc.
    ///   on existing entries; adds new entries verbatim),
    /// - bumps `next_epoch`,
    /// - and returns the full effective partition set for the handler to
    ///   read.
    ///
    /// The handler must call `finalize_incremental` after assembling the
    /// response so the "last_*" comparison fields stay in sync with what
    /// was actually sent.
    pub fn classify(&self, req: &FetchRequest) -> SessionDecision {
        let sid = req.session_id;
        let epoch = req.session_epoch;

        if sid == INVALID_SESSION_ID {
            return match epoch {
                FINAL_EPOCH => SessionDecision::Sessionless,
                INITIAL_EPOCH => SessionDecision::NewSession,
                _ => SessionDecision::Error {
                    code: codes::INVALID_FETCH_SESSION_EPOCH,
                },
            };
        }

        let mut guard = self.inner.lock().expect("poisoned");

        if epoch == FINAL_EPOCH {
            if !guard.sessions.contains_key(&sid) {
                return SessionDecision::Error {
                    code: codes::FETCH_SESSION_ID_NOT_FOUND,
                };
            }
            return SessionDecision::Close { session_id: sid };
        }

        let Some(session) = guard.sessions.get_mut(&sid) else {
            return SessionDecision::Error {
                code: codes::FETCH_SESSION_ID_NOT_FOUND,
            };
        };

        if epoch != session.next_epoch {
            return SessionDecision::Error {
                code: codes::INVALID_FETCH_SESSION_EPOCH,
            };
        }

        session.last_used = Instant::now();

        // Drop forgotten partitions. A `ForgottenTopic` matches a cached
        // key by either topic_name (v ≤ 12) or topic_id (v ≥ 13).
        for ft in &req.forgotten_topics_data {
            session.partitions.retain(|k, _| {
                let topic_match = (!ft.topic.is_empty() && k.topic_name == ft.topic)
                    || (ft.topic_id != WireUuid::ZERO && k.topic_id == ft.topic_id);
                if !topic_match {
                    return true;
                }
                !ft.partitions.contains(&k.partition)
            });
        }

        // Merge request topics — updates existing entries' desired
        // offset/max_bytes, adds new entries with default `last_*`.
        //
        // The cache key carries both `topic_name` and `topic_id`. After
        // `try_allocate` (via the handler's resolution) both fields are
        // populated. The wire request, however, only carries one of them:
        // Fetch v ≤ 12 sends the name and leaves `topic_id` zero; v ≥ 13
        // sends the id and leaves `topic` empty. A naive `entry((name, id, p))`
        // lookup would treat the partial-key request as a *new* partition and
        // shadow the cached entry with a default-state copy (max_bytes=0),
        // which then breaks subsequent reads. Find the cached key by either
        // half-identity first; only fall back to a brand-new key when the
        // partition truly isn't in the cache.
        for t in &req.topics {
            for fp in &t.partitions {
                let existing_key = session
                    .partitions
                    .keys()
                    .find(|k| {
                        k.partition == fp.partition
                            && ((!t.topic.is_empty() && k.topic_name == t.topic)
                                || (t.topic_id != WireUuid::ZERO && k.topic_id == t.topic_id))
                    })
                    .cloned();
                let key = existing_key.unwrap_or_else(|| FetchSessionKey {
                    topic_name: t.topic.clone(),
                    topic_id: t.topic_id,
                    partition: fp.partition,
                });
                let entry = session.partitions.entry(key).or_default();
                entry.fetch_offset = fp.fetch_offset;
                entry.max_bytes = fp.partition_max_bytes;
                entry.current_leader_epoch = fp.current_leader_epoch;
                entry.last_fetched_epoch = fp.last_fetched_epoch;
                entry.log_start_offset = fp.log_start_offset;
            }
        }

        let new_epoch = next_epoch(session.next_epoch);
        session.next_epoch = new_epoch;

        let partitions: Vec<(FetchSessionKey, CachedPartitionState)> = session
            .partitions
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        SessionDecision::Incremental {
            session_id: sid,
            new_epoch,
            partitions,
        }
    }

    /// Allocate a fresh session for a `NewSession` decision. `partitions`
    /// must capture both the desired state (`fetch_offset`, `max_bytes`, ...)
    /// and the response-side `last_*` values for what was just sent — the
    /// next incremental fetch will compare new response state to these.
    ///
    /// Returns the assigned id, or `INVALID_SESSION_ID` (0) if the cache
    /// is full and no eligible victim could be evicted. On a refused
    /// allocation the caller emits `response.session_id = 0` and the
    /// client transparently falls back to sessionless full fetches.
    pub fn try_allocate(
        &self,
        privileged: bool,
        creator_principal: String,
        partitions: Vec<(FetchSessionKey, CachedPartitionState)>,
    ) -> i32 {
        if self.max_slots == 0 {
            return INVALID_SESSION_ID;
        }
        let mut guard = self.inner.lock().expect("poisoned");

        if guard.sessions.len() >= self.max_slots {
            // Pick a victim: LRU non-privileged session if one exists,
            // otherwise (only when the caller is itself privileged) the
            // LRU session of any kind. Non-privileged callers cannot
            // evict privileged sessions — they fall back to sessionless.
            let victim: Option<i32> = guard
                .sessions
                .iter()
                .filter(|(_, s)| if privileged { true } else { !s.privileged })
                .min_by_key(|(_, s)| s.last_used)
                .map(|(id, _)| *id);
            match victim {
                Some(id) => {
                    guard.sessions.remove(&id);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                }
                None => return INVALID_SESSION_ID,
            }
        }

        // Allocate a fresh id. AtomicI32::fetch_add wraps, so we skip
        // 0 (sentinel) and any negative (would round-trip on the wire
        // as a "negative session id" the client rejects) and any
        // id that's already taken (extremely rare — happens only after
        // 2^31 allocations of overlap).
        let id = loop {
            let candidate = self.next_id.fetch_add(1, Ordering::Relaxed);
            if candidate <= 0 {
                // Wrapped past i32::MAX or hit zero. Reset to 1 and
                // try again; the next iteration will fetch_add to 2
                // and store 3.
                self.next_id.store(1, Ordering::Relaxed);
                continue;
            }
            if !guard.sessions.contains_key(&candidate) {
                break candidate;
            }
        };

        let partitions: HashMap<FetchSessionKey, CachedPartitionState> =
            partitions.into_iter().collect();
        let session = FetchSession {
            id,
            // Client's first incremental request after a new-session
            // allocation must carry epoch=1.
            next_epoch: 1,
            privileged,
            creator_principal,
            partitions,
            last_used: Instant::now(),
        };
        guard.sessions.insert(id, session);
        id
    }

    /// Update the `last_*` fields on cached partitions to reflect what
    /// the handler emitted in the just-finished response. Only the
    /// partitions actually included in the response need updating —
    /// filtered-out partitions already match the cache (that's why they
    /// were filtered).
    pub fn finalize_incremental(
        &self,
        session_id: i32,
        sent: &[(FetchSessionKey, CachedPartitionState)],
    ) {
        let mut guard = self.inner.lock().expect("poisoned");
        let Some(session) = guard.sessions.get_mut(&session_id) else {
            return;
        };
        for (k, s) in sent {
            if let Some(state) = session.partitions.get_mut(k) {
                state.last_high_watermark = s.last_high_watermark;
                state.last_last_stable_offset = s.last_last_stable_offset;
                state.last_log_start_offset = s.last_log_start_offset;
                state.last_preferred_read_replica = s.last_preferred_read_replica;
                state.last_aborted_txns_hash = s.last_aborted_txns_hash;
                state.last_error_code = s.last_error_code;
            }
        }
    }

    /// Drop the session. Called when the request is `Close` (existing
    /// session, epoch=-1) or after the handler decides to forcibly
    /// invalidate the session.
    pub fn close(&self, session_id: i32) {
        let mut guard = self.inner.lock().expect("poisoned");
        guard.sessions.remove(&session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_protocol::owned::fetch_request::{FetchPartition, FetchTopic, ForgottenTopic};

    fn req(
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopic>,
        forgotten: Vec<ForgottenTopic>,
    ) -> FetchRequest {
        FetchRequest {
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data: forgotten,
            ..Default::default()
        }
    }

    fn topic(name: &str, partitions: &[i32]) -> FetchTopic {
        FetchTopic {
            topic: name.to_string(),
            topic_id: WireUuid::ZERO,
            partitions: partitions
                .iter()
                .map(|&p| FetchPartition {
                    partition: p,
                    fetch_offset: 0,
                    partition_max_bytes: 1024,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn next_epoch_wraps_skipping_sentinels() {
        assert_eq!(next_epoch(0), 1);
        assert_eq!(next_epoch(1), 2);
        assert_eq!(next_epoch(i32::MAX), 1);
        assert_eq!(next_epoch(-1), 1);
    }

    #[test]
    fn sessionless_request_is_classified_correctly() {
        let cache = FetchSessionCache::new(10);
        let r = req(0, FINAL_EPOCH, vec![], vec![]);
        assert!(matches!(cache.classify(&r), SessionDecision::Sessionless));
    }

    #[test]
    fn new_session_request_is_classified_correctly() {
        let cache = FetchSessionCache::new(10);
        let r = req(0, INITIAL_EPOCH, vec![topic("t", &[0])], vec![]);
        assert!(matches!(cache.classify(&r), SessionDecision::NewSession));
    }

    #[test]
    fn allocate_returns_nonzero_monotonic_ids() {
        let cache = FetchSessionCache::new(10);
        let a = cache.try_allocate(false, "alice".into(), vec![]);
        let b = cache.try_allocate(false, "alice".into(), vec![]);
        assert!(a > 0);
        assert!(b > 0);
        assert_ne!(a, b);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn allocate_skips_zero_on_wrap() {
        let cache = FetchSessionCache::new(10);
        // Force the next id to be 0 — the loop should skip and start from 1.
        cache.next_id.store(0, Ordering::Relaxed);
        let id = cache.try_allocate(false, "alice".into(), vec![]);
        assert!(id > 0);
    }

    #[test]
    fn allocate_returns_zero_when_max_slots_zero() {
        let cache = FetchSessionCache::new(0);
        let id = cache.try_allocate(false, "alice".into(), vec![]);
        assert_eq!(id, INVALID_SESSION_ID);
    }

    #[test]
    fn unknown_session_id_returns_not_found() {
        let cache = FetchSessionCache::new(10);
        let r = req(12345, 1, vec![], vec![]);
        match cache.classify(&r) {
            SessionDecision::Error { code } => {
                assert_eq!(code, codes::FETCH_SESSION_ID_NOT_FOUND);
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn stale_epoch_returns_invalid_epoch() {
        let cache = FetchSessionCache::new(10);
        let id = cache.try_allocate(false, "alice".into(), vec![]);
        // Session's expected next_epoch is 1; send epoch=99.
        let r = req(id, 99, vec![], vec![]);
        match cache.classify(&r) {
            SessionDecision::Error { code } => {
                assert_eq!(code, codes::INVALID_FETCH_SESSION_EPOCH);
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn close_request_returns_close_then_handler_drops() {
        let cache = FetchSessionCache::new(10);
        let id = cache.try_allocate(false, "alice".into(), vec![]);
        let r = req(id, FINAL_EPOCH, vec![], vec![]);
        match cache.classify(&r) {
            SessionDecision::Close { session_id } => assert_eq!(session_id, id),
            other => panic!("expected Close, got {other:?}"),
        }
        cache.close(id);
        assert_eq!(cache.len(), 0);
        // Subsequent classify with the same id is now NOT_FOUND.
        let r2 = req(id, 1, vec![], vec![]);
        match cache.classify(&r2) {
            SessionDecision::Error { code } => {
                assert_eq!(code, codes::FETCH_SESSION_ID_NOT_FOUND);
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn invalid_session_id_zero_with_stray_epoch_is_error() {
        let cache = FetchSessionCache::new(10);
        let r = req(0, 5, vec![], vec![]);
        match cache.classify(&r) {
            SessionDecision::Error { code } => {
                assert_eq!(code, codes::INVALID_FETCH_SESSION_EPOCH);
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn incremental_merges_request_topics_and_bumps_epoch() {
        let cache = FetchSessionCache::new(10);
        let initial = vec![(
            FetchSessionKey {
                topic_name: "t".into(),
                topic_id: WireUuid::ZERO,
                partition: 0,
            },
            CachedPartitionState {
                fetch_offset: 100,
                max_bytes: 1024,
                ..Default::default()
            },
        )];
        let id = cache.try_allocate(false, "alice".into(), initial);

        // Incremental that updates partition 0's fetch_offset and adds partition 1.
        let r = req(id, 1, vec![topic("t", &[0, 1])], vec![]);
        match cache.classify(&r) {
            SessionDecision::Incremental {
                session_id,
                new_epoch,
                partitions,
            } => {
                assert_eq!(session_id, id);
                assert_eq!(new_epoch, 2);
                assert_eq!(partitions.len(), 2);
            }
            other => panic!("expected Incremental, got {other:?}"),
        }

        // Re-sending with the old epoch fails — broker advanced to 2.
        let r2 = req(id, 1, vec![], vec![]);
        match cache.classify(&r2) {
            SessionDecision::Error { code } => {
                assert_eq!(code, codes::INVALID_FETCH_SESSION_EPOCH);
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn incremental_merge_matches_cached_key_by_topic_id_only() {
        // Reproduces the broker-jvm-acceptance regression: a v ≥ 13 client
        // opens a session, the broker resolves and caches `(name, id, p)`;
        // then the client sends an incremental that only carries `topic_id`
        // (empty `topic`). The merge must update the cached entry — not
        // insert a duplicate with default `max_bytes`, which would silently
        // drop bytes from the subsequent read.
        let cache = FetchSessionCache::new(10);
        let tid = WireUuid([7u8; 16]);
        let cached_key = FetchSessionKey {
            topic_name: "t".into(),
            topic_id: tid,
            partition: 0,
        };
        let id = cache.try_allocate(
            false,
            "alice".into(),
            vec![(
                cached_key.clone(),
                CachedPartitionState {
                    fetch_offset: 5,
                    max_bytes: 1024,
                    ..Default::default()
                },
            )],
        );

        // v ≥ 13 incremental: topic_id set, topic_name empty, new fetch_offset.
        let r = req(
            id,
            1,
            vec![FetchTopic {
                topic: String::new(),
                topic_id: tid,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 42,
                    partition_max_bytes: 2048,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![],
        );
        let SessionDecision::Incremental { partitions, .. } = cache.classify(&r) else {
            panic!("expected Incremental");
        };
        assert_eq!(partitions.len(), 1, "no duplicate entry created");
        let (k, s) = &partitions[0];
        assert_eq!(k.topic_name, "t", "cached name preserved");
        assert_eq!(k.topic_id, tid);
        assert_eq!(s.fetch_offset, 42, "fetch_offset updated");
        assert_eq!(s.max_bytes, 2048, "max_bytes updated");
    }

    #[test]
    fn incremental_merge_matches_cached_key_by_topic_name_only() {
        // Mirror case for v ≤ 12 clients: cache has (name, id, p) after
        // server-side resolution; request carries name only, id ZERO.
        let cache = FetchSessionCache::new(10);
        let tid = WireUuid([9u8; 16]);
        let cached_key = FetchSessionKey {
            topic_name: "t".into(),
            topic_id: tid,
            partition: 0,
        };
        let id = cache.try_allocate(
            false,
            "alice".into(),
            vec![(
                cached_key.clone(),
                CachedPartitionState {
                    fetch_offset: 5,
                    max_bytes: 1024,
                    ..Default::default()
                },
            )],
        );

        let r = req(
            id,
            1,
            vec![FetchTopic {
                topic: "t".into(),
                topic_id: WireUuid::ZERO,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 99,
                    partition_max_bytes: 4096,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![],
        );
        let SessionDecision::Incremental { partitions, .. } = cache.classify(&r) else {
            panic!("expected Incremental");
        };
        assert_eq!(partitions.len(), 1);
        let (_, s) = &partitions[0];
        assert_eq!(s.fetch_offset, 99);
        assert_eq!(s.max_bytes, 4096);
    }

    #[test]
    fn forgotten_topics_drop_partitions_from_cache() {
        let cache = FetchSessionCache::new(10);
        let initial = vec![
            (
                FetchSessionKey {
                    topic_name: "t".into(),
                    topic_id: WireUuid::ZERO,
                    partition: 0,
                },
                CachedPartitionState::default(),
            ),
            (
                FetchSessionKey {
                    topic_name: "t".into(),
                    topic_id: WireUuid::ZERO,
                    partition: 1,
                },
                CachedPartitionState::default(),
            ),
            (
                FetchSessionKey {
                    topic_name: "t".into(),
                    topic_id: WireUuid::ZERO,
                    partition: 2,
                },
                CachedPartitionState::default(),
            ),
        ];
        let id = cache.try_allocate(false, "alice".into(), initial);

        let forgotten = vec![ForgottenTopic {
            topic: "t".into(),
            topic_id: WireUuid::ZERO,
            partitions: vec![1],
            ..Default::default()
        }];
        let r = req(id, 1, vec![], forgotten);
        match cache.classify(&r) {
            SessionDecision::Incremental { partitions, .. } => {
                assert_eq!(partitions.len(), 2);
                let parts: Vec<i32> = partitions.iter().map(|(k, _)| k.partition).collect();
                assert!(parts.contains(&0));
                assert!(parts.contains(&2));
                assert!(!parts.contains(&1));
            }
            other => panic!("expected Incremental, got {other:?}"),
        }
    }

    #[test]
    fn lru_eviction_drops_oldest_non_privileged() {
        let cache = FetchSessionCache::new(2);
        let a = cache.try_allocate(false, "a".into(), vec![]);
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = cache.try_allocate(false, "b".into(), vec![]);
        std::thread::sleep(std::time::Duration::from_millis(2));
        let c = cache.try_allocate(false, "c".into(), vec![]);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.evictions_total(), 1);
        // `a` (oldest) was evicted; `b` and `c` remain.
        let g = cache.inner.lock().unwrap();
        assert!(!g.sessions.contains_key(&a));
        assert!(g.sessions.contains_key(&b));
        assert!(g.sessions.contains_key(&c));
    }

    #[test]
    fn non_privileged_cannot_evict_privileged() {
        let cache = FetchSessionCache::new(1);
        let p = cache.try_allocate(true, "follower".into(), vec![]);
        assert!(p > 0);
        // Cache full, only session is privileged. Consumer alloc refused.
        let c = cache.try_allocate(false, "consumer".into(), vec![]);
        assert_eq!(c, INVALID_SESSION_ID);
        assert_eq!(cache.evictions_total(), 0);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn privileged_can_evict_privileged() {
        let cache = FetchSessionCache::new(1);
        let p1 = cache.try_allocate(true, "f1".into(), vec![]);
        std::thread::sleep(std::time::Duration::from_millis(2));
        let p2 = cache.try_allocate(true, "f2".into(), vec![]);
        assert!(p2 > 0);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.evictions_total(), 1);
        let g = cache.inner.lock().unwrap();
        assert!(!g.sessions.contains_key(&p1));
        assert!(g.sessions.contains_key(&p2));
    }

    #[test]
    fn finalize_incremental_updates_last_state() {
        let cache = FetchSessionCache::new(10);
        let key = FetchSessionKey {
            topic_name: "t".into(),
            topic_id: WireUuid::ZERO,
            partition: 0,
        };
        let id = cache.try_allocate(
            false,
            "a".into(),
            vec![(key.clone(), CachedPartitionState::default())],
        );
        let sent = vec![(
            key.clone(),
            CachedPartitionState {
                last_high_watermark: 42,
                last_log_start_offset: 7,
                ..Default::default()
            },
        )];
        cache.finalize_incremental(id, &sent);
        let g = cache.inner.lock().unwrap();
        let s = g.sessions.get(&id).unwrap().partitions.get(&key).unwrap();
        assert_eq!(s.last_high_watermark, 42);
        assert_eq!(s.last_log_start_offset, 7);
    }

    #[test]
    fn total_partitions_cached_sums_across_sessions() {
        let cache = FetchSessionCache::new(10);
        let mk = |p| {
            (
                FetchSessionKey {
                    topic_name: "t".into(),
                    topic_id: WireUuid::ZERO,
                    partition: p,
                },
                CachedPartitionState::default(),
            )
        };
        cache.try_allocate(false, "a".into(), vec![mk(0), mk(1)]);
        cache.try_allocate(false, "b".into(), vec![mk(2), mk(3), mk(4)]);
        assert_eq!(cache.total_partitions_cached(), 5);
    }
}
