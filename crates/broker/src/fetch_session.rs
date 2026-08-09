//! KIP-227 incremental-fetch-session cache.
//!
//! A `FetchSession` lets a Kafka consumer or replicator send the broker its
//! subscription set once. After that it sends small "delta" fetch requests.
//! Each delta carries only the partitions whose desired state has changed (new
//! offset, new max-bytes), plus a `forgotten_topics_data` list of partitions
//! to drop. The broker answers with only the partitions whose state has
//! changed since the previous response.
//!
//! For a caught-up consumer with hundreds of partitions, this reduces a
//! continuous stream of identical fetches to almost no wire traffic until
//! something changes.
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
//! A mismatched epoch returns `INVALID_FETCH_SESSION_EPOCH` at the top level
//! of the response. An unknown id returns `FETCH_SESSION_ID_NOT_FOUND`.
//!
//! ## Cache & eviction
//!
//! The cache holds sessions in one bounded map, keyed by allocated id. Its
//! capacity is `BrokerConfig::max_incremental_fetch_session_cache_slots`.
//! When the map is full, an allocation evicts the LRU **non-privileged**
//! session. Only another privileged session evicts a privileged session, which
//! is a follower fetch with `replica_id >= 0`.
//!
//! When there is no eligible victim, `try_allocate` returns
//! `INVALID_SESSION_ID` and the caller falls back to a sessionless response.
//! This matches Apache Kafka. That case arises when the cache is full of
//! privileged sessions and the caller is non-privileged.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering},
    },
};

use crabka_protocol::{
    owned::fetch_request::{FetchRequest, FetchTopic, ForgottenTopic},
    primitives::uuid::Uuid as WireUuid,
};
use qubit_clock::{NanoClock, NanoMonotonicClock};

use crate::codes;

/// A KIP-227 fetch session id as carried on the wire
/// (`FetchRequest.session_id`). `0` ([`INVALID_SESSION_ID`]) means "no
/// session". Valid ids are strictly positive.
pub type FetchSessionId = i32;

/// A KIP-227 fetch session epoch as carried on the wire
/// (`FetchRequest.session_epoch`). `0` ([`INITIAL_EPOCH`]) opens a session and
/// `-1` ([`FINAL_EPOCH`]) closes one. Valid incremental epochs are strictly
/// positive.
pub type FetchSessionEpoch = i32;

/// Wire sentinel: "no session". A request with `session_id == 0` and
/// `session_epoch == -1` is a sessionless full fetch. A response with
/// `session_id == 0` tells the client that the broker allocated no session.
pub const INVALID_SESSION_ID: FetchSessionId = 0;

/// Wire sentinel: "open a new session". A request with `session_id == 0`
/// and `session_epoch == 0` asks the broker to allocate a new session.
pub const INITIAL_EPOCH: FetchSessionEpoch = 0;

/// Wire sentinel for "no session" and for "close session". On a request with
/// `session_id == 0`, `FINAL_EPOCH` means a sessionless full fetch. On a
/// request with `session_id != 0`, it means close the named session.
pub const FINAL_EPOCH: FetchSessionEpoch = -1;

/// The first id the allocator gives out. Ids count up from here. `0` is
/// reserved as [`INVALID_SESSION_ID`], and negative ids never go on the wire
/// because clients reject them.
const FIRST_SESSION_ID: FetchSessionId = 1;

/// Computes the epoch the broker expects on the next request after a
/// successful incremental fetch. The value wraps from `i32::MAX` back to `1`
/// and skips the two reserved sentinels: `0` for INITIAL and `-1` for FINAL.
#[must_use]
pub fn next_epoch(prev: FetchSessionEpoch) -> FetchSessionEpoch {
    let n = prev.wrapping_add(1);
    if n <= 0 { 1 } else { n }
}

fn session_id_is_reserved(candidate: FetchSessionId) -> bool {
    candidate <= 0
}

/// (`topic_name`, `topic_id`, partition). The cache keeps both the name and
/// the id, because Fetch v 12 and below sends only the name and v 13 and above
/// sends only the id. The cache must resolve the key for either version.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FetchSessionKey {
    pub topic_name: String,
    pub topic_id: WireUuid,
    pub partition: i32,
}

/// Per-partition cached state. The first block (`fetch_offset` and the fields
/// after it) records what the client wants on the next read. The `last_*`
/// block records what the broker sent in the previous response. The broker
/// compares the two blocks to decide whether the next response includes this
/// partition, because KIP-227 omits a partition when nothing has changed since
/// the previous response.
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
    pub id: FetchSessionId,
    /// The epoch the *next* incremental request must carry. The cache sets it
    /// to `1` on allocation and raises it after each successful incremental
    /// fetch.
    pub next_epoch: FetchSessionEpoch,
    pub privileged: bool,
    pub creator_principal: String,
    pub partitions: HashMap<FetchSessionKey, CachedPartitionState>,
    /// Monotonic epoch-nanosecond timestamp of the last use of this session,
    /// read from the cache's injected [`NanoClock`]. The order of these values
    /// selects the LRU eviction victim. Only their relative order matters.
    pub last_used_nanos: i128,
}

/// Outcome of `FetchSessionCache::classify`. The handler dispatches on this
/// value before it does any read.
#[derive(Debug)]
pub enum SessionDecision {
    /// `(session_id=0, epoch=-1)`: serve from `req.topics`, with no caching,
    /// and a response `session_id = 0`.
    Sessionless,
    /// `(session_id=0, epoch=0)`: serve from `req.topics`, then ask the cache
    /// to allocate a new session for the result. The cache can refuse the
    /// allocation when it is full of privileged sessions. The response
    /// `session_id` is then `0`, and the client falls back to sessionless
    /// fetches next time.
    NewSession,
    /// `(session_id!=0, epoch>=0)` that matches the cached epoch: serve from
    /// the cached subscription set. The cache merges `req.topics` in as
    /// updates and new entries, and removes `forgotten_topics_data`. The
    /// response holds only the partitions whose state has changed.
    Incremental {
        session_id: FetchSessionId,
        /// This value is already incremented. It goes nowhere on the wire,
        /// because the response has no epoch field. The cache uses it as the
        /// *next* expected epoch for the following request.
        new_epoch: FetchSessionEpoch,
        partitions: Vec<(FetchSessionKey, CachedPartitionState)>,
    },
    /// `(session_id!=0, epoch=-1)`: serve from `req.topics` like a sessionless
    /// fetch, then drop the cached session.
    Close { session_id: FetchSessionId },
    /// Protocol violation. Emit an empty response with this top-level
    /// `error_code` and `session_id = 0`.
    Error { code: i16 },
}

struct Inner {
    sessions: HashMap<FetchSessionId, FetchSession>,
}

pub struct FetchSessionCache {
    inner: Mutex<Inner>,
    next_id: AtomicI32,
    max_slots: usize,
    evictions: AtomicU64,
    /// Live session count. The cache maintains it under `inner`'s lock on
    /// every insert, evict, and close. `len()` exposes it lock-free, so the
    /// metrics gauge refresh on the hot fetch path never touches the cache
    /// mutex.
    num_sessions: AtomicUsize,
    /// Sum of `session.partitions.len()` across every live session. The cache
    /// keeps it in step as it adds partitions on merge and allocate, and drops
    /// them on forget, evict, and close. `total_partitions_cached()` reads it
    /// lock-free.
    num_partitions: AtomicUsize,
    /// Monotonic time source that the cache stamps onto
    /// `FetchSession::last_used_nanos` for LRU eviction. It is injectable, so
    /// tests drive the eviction order with a [`qubit_clock::MockClock`]
    /// instead of `thread::sleep`.
    clock: Arc<dyn NanoClock>,
}

/// Pure core of the incremental-fetch session update (KIP-227). It drops
/// forgotten partitions, then merges the requested topics into a session's
/// partition map.
///
/// - **forget**: a `ForgottenTopic` matches a cached key by either
///   `topic_name` (Fetch v 12 and below) **or** `topic_id` (v 13 and above),
///   plus the partition.
/// - **merge**: for each requested partition, this function finds a cached key
///   by either identity half plus the partition, and updates its desired state
///   in place. It inserts a new default-state key only when the partition is
///   truly not cached. That rule stops a partial-identity copy from shadowing
///   a fully resolved key.
///
/// `fetch_session_model` exercises the asymmetry between the OR-match forget
/// and the either-half-match merge exhaustively.
pub(crate) fn apply_incremental(
    partitions: &mut HashMap<FetchSessionKey, CachedPartitionState>,
    forgotten: &[ForgottenTopic],
    topics: &[FetchTopic],
) {
    for ft in forgotten {
        partitions.retain(|k, _| {
            let topic_match = (!ft.topic.is_empty() && k.topic_name == ft.topic)
                || (ft.topic_id != WireUuid::ZERO && k.topic_id == ft.topic_id);
            if !topic_match {
                return true;
            }
            !ft.partitions.contains(&k.partition)
        });
    }

    for t in topics {
        for fp in &t.partitions {
            let existing_key = partitions
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
            let entry = partitions.entry(key).or_default();
            entry.fetch_offset = fp.fetch_offset;
            entry.max_bytes = fp.partition_max_bytes;
            entry.current_leader_epoch = fp.current_leader_epoch;
            entry.last_fetched_epoch = fp.last_fetched_epoch;
            entry.log_start_offset = fp.log_start_offset;
        }
    }
}

impl FetchSessionCache {
    #[must_use]
    pub fn new(max_slots: usize) -> Self {
        Self::with_clock(max_slots, Arc::new(NanoMonotonicClock::new()))
    }

    /// Constructs a cache with a caller-supplied monotonic [`NanoClock`].
    ///
    /// Production uses [`FetchSessionCache::new`], which supplies a
    /// [`NanoMonotonicClock`]. Tests pass a [`qubit_clock::MockClock`], so
    /// that successive allocations get distinct, deterministic
    /// `last_used_nanos` without a sleep between them.
    #[must_use]
    pub fn with_clock(max_slots: usize, clock: Arc<dyn NanoClock>) -> Self {
        Self {
            inner: Mutex::new(Inner {
                sessions: HashMap::new(),
            }),
            // Id allocation starts at FIRST_SESSION_ID — id 0 is reserved
            // as the INVALID_SESSION_ID sentinel.
            next_id: AtomicI32::new(FIRST_SESSION_ID),
            max_slots,
            evictions: AtomicU64::new(0),
            num_sessions: AtomicUsize::new(0),
            num_partitions: AtomicUsize::new(0),
            clock,
        }
    }

    /// Number of live sessions in the cache. This is a lock-free read of an
    /// atomic counter and does not touch the cache mutex.
    #[must_use]
    pub fn len(&self) -> usize {
        self.num_sessions.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Sum of `session.partitions.len()` across every live session. The
    /// metrics sampler reads it. This is a lock-free read of an atomic counter
    /// and does not touch the cache mutex or scan the session map.
    #[must_use]
    pub fn total_partitions_cached(&self) -> usize {
        self.num_partitions.load(Ordering::Relaxed)
    }

    /// Cumulative count of eviction events since `new()`. There is one
    /// increment for each session that an allocation displaces. It does *not*
    /// count refused allocations, because those displace nothing.
    #[must_use]
    pub fn evictions_total(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Inspects the request and decides which of the four branches the fetch
    /// handler should take. On `Incremental` it does the following atomically:
    /// - validates the epoch,
    /// - removes `forgotten_topics_data` from the cached partition set,
    /// - merges `req.topics` into the cached set (updates `fetch_offset` etc.
    ///   on existing entries; adds new entries verbatim),
    /// - bumps `next_epoch`,
    /// - and returns the full effective partition set for the handler to
    ///   read.
    ///
    /// The handler must call `finalize_incremental` after it assembles the
    /// response, so that the `last_*` comparison fields stay in step with what
    /// the broker sent.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
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

        session.last_used_nanos = self.clock.nanos();

        // The forget + merge below add and drop partitions; snapshot the
        // count now so we can fold the net delta into `num_partitions`
        // (which backs the lock-free `total_partitions_cached()` gauge).
        let partitions_before = session.partitions.len();

        // Drop forgotten partitions then merge request topics (KIP-227). The
        // forget/merge logic — and the half-identity matching that prevents a
        // partial-identity request from shadowing a fully-resolved cached key —
        // lives in `apply_incremental`, verified by `fetch_session_model`.
        apply_incremental(
            &mut session.partitions,
            &req.forgotten_topics_data,
            &req.topics,
        );

        let partitions_after = session.partitions.len();
        if partitions_after >= partitions_before {
            self.num_partitions
                .fetch_add(partitions_after - partitions_before, Ordering::Relaxed);
        } else {
            self.num_partitions
                .fetch_sub(partitions_before - partitions_after, Ordering::Relaxed);
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

    /// Allocates a fresh session for a `NewSession` decision. `partitions`
    /// must carry both the desired state (`fetch_offset`, `max_bytes`, and the
    /// rest) and the response-side `last_*` values for what the broker just
    /// sent. The next incremental fetch compares the new response state to
    /// those values.
    ///
    /// Returns the assigned id, or `INVALID_SESSION_ID` (0) when the cache is
    /// full and it can evict no eligible victim. On a refused allocation the
    /// caller emits `response.session_id = 0`, and the client falls back to
    /// sessionless full fetches without further signalling.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn try_allocate(
        &self,
        privileged: bool,
        creator_principal: String,
        partitions: Vec<(FetchSessionKey, CachedPartitionState)>,
    ) -> FetchSessionId {
        if self.max_slots == 0 {
            return INVALID_SESSION_ID;
        }
        let mut guard = self.inner.lock().expect("poisoned");

        if guard.sessions.len() >= self.max_slots {
            // Pick a victim: LRU non-privileged session if one exists,
            // otherwise (only when the caller is itself privileged) the
            // LRU session of any kind. Non-privileged callers cannot
            // evict privileged sessions — they fall back to sessionless.
            let victim: Option<FetchSessionId> = guard
                .sessions
                .iter()
                .filter(|(_, s)| if privileged { true } else { !s.privileged })
                .min_by_key(|(_, s)| s.last_used_nanos)
                .map(|(id, _)| *id);
            match victim {
                Some(id) => {
                    let evicted = guard.sessions.remove(&id).expect("victim present");
                    self.num_sessions.fetch_sub(1, Ordering::Relaxed);
                    self.num_partitions
                        .fetch_sub(evicted.partitions.len(), Ordering::Relaxed);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                }
                None => return INVALID_SESSION_ID,
            }
        }

        // Allocate a fresh id. AtomicI32::fetch_add wraps, so we skip
        // 0 (sentinel) and any negative (would round-trip on the wire
        // as a "negative session id" the client rejects) and any
        // id that's already taken (extremely rare — happens only after
        // 2^31 allocations of overlap). The loop is bounded by the number of
        // live ids it could collide with plus the reserved wrap value and reset.
        let mut id = None;
        for _ in 0..guard.sessions.len().saturating_add(3) {
            let candidate = self.next_id.fetch_add(1, Ordering::Relaxed);
            if session_id_is_reserved(candidate) {
                // Wrapped past i32::MAX or hit zero. Reset to the first
                // allocatable id and try again; the next iteration will
                // fetch_add to 2 and store 3.
                self.next_id.store(FIRST_SESSION_ID, Ordering::Relaxed);
                continue;
            }
            if !guard.sessions.contains_key(&candidate) {
                id = Some(candidate);
                break;
            }
        }
        let Some(id) = id else {
            return INVALID_SESSION_ID;
        };

        let partitions: HashMap<FetchSessionKey, CachedPartitionState> =
            partitions.into_iter().collect();
        let session = FetchSession {
            id,
            // Client's first incremental request after a new-session
            // allocation must carry the epoch after INITIAL (i.e. 1).
            next_epoch: next_epoch(INITIAL_EPOCH),
            privileged,
            creator_principal,
            partitions,
            last_used_nanos: self.clock.nanos(),
        };
        let added_partitions = session.partitions.len();
        guard.sessions.insert(id, session);
        self.num_sessions.fetch_add(1, Ordering::Relaxed);
        self.num_partitions
            .fetch_add(added_partitions, Ordering::Relaxed);
        id
    }

    /// Updates the `last_*` fields on cached partitions to match what the
    /// handler emitted in the response that just finished. Only the partitions
    /// that the response included need an update. A filtered-out partition
    /// already matches the cache, which is why the broker filtered it.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn finalize_incremental(
        &self,
        session_id: FetchSessionId,
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

    /// Drops the session. The handler calls it when the request is `Close`,
    /// which is an existing session with epoch -1, or after the handler
    /// decides to invalidate the session by force.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn close(&self, session_id: FetchSessionId) {
        let mut guard = self.inner.lock().expect("poisoned");
        if let Some(session) = guard.sessions.remove(&session_id) {
            self.num_sessions.fetch_sub(1, Ordering::Relaxed);
            self.num_partitions
                .fetch_sub(session.partitions.len(), Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
#[path = "fetch_session_model.rs"]
mod fetch_session_model;

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_protocol::owned::fetch_request::{FetchPartition, FetchTopic, ForgottenTopic};
    use qubit_clock::MockTime;

    use super::*;

    /// Builds a cache whose LRU clock is a mock timeline anchored at the Unix
    /// epoch. It returns the [`MockTime`] handle, so a test can put successive
    /// allocations on distinct `last_used_nanos`. The test advances logical
    /// time with `mock.advance(..)` instead of a sleep between allocations.
    fn mock_cache(max_slots: usize) -> (FetchSessionCache, MockTime) {
        let mock = MockTime::unix_epoch();
        let cache = FetchSessionCache::with_clock(max_slots, Arc::new(mock.clock()));
        (cache, mock)
    }

    /// A one-nanosecond tick: the smallest advance that still gives the next
    /// allocation a strictly greater `last_used_nanos` than the previous one.
    const TICK: std::time::Duration = std::time::Duration::from_nanos(1);

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
        let cases = [(0, 1), (1, 2), (i32::MAX, 1), (-1, 1)];
        for (epoch, want) in cases {
            assert!(next_epoch(epoch) == want, "epoch {epoch}");
        }
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
        // Id allocation starts at 1 and increments monotonically.
        check!(a == 1);
        check!(b == 2);
        check!(cache.len() == 2);
    }

    #[test]
    fn is_empty_tracks_session_lifecycle() {
        let cache = FetchSessionCache::new(10);
        assert!(cache.is_empty());

        let id = cache.try_allocate(false, "alice".into(), vec![]);
        assert!(!cache.is_empty());

        cache.close(id);
        assert!(cache.is_empty());
    }

    #[test]
    fn session_id_reserved_predicate_matches_wire_sentinels() {
        let cases = [(INVALID_SESSION_ID, true), (FINAL_EPOCH, true), (1, false)];
        for (session_id, want) in cases {
            assert!(
                session_id_is_reserved(session_id) == want,
                "session_id {session_id}"
            );
        }
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
    fn allocate_skips_existing_session_id_collision() {
        let cache = FetchSessionCache::new(10);
        let first = cache.try_allocate(false, "alice".into(), vec![]);

        cache.next_id.store(first, Ordering::Relaxed);
        let second = cache.try_allocate(false, "bob".into(), vec![]);

        assert!(second == first + 1);
        assert!(cache.len() == 2);
    }

    #[test]
    fn allocate_returns_zero_when_max_slots_zero() {
        let cache = FetchSessionCache::new(0);
        let id = cache.try_allocate(false, "alice".into(), vec![]);
        assert!(id == INVALID_SESSION_ID);
    }

    #[test]
    fn unknown_session_id_returns_not_found() {
        let cache = FetchSessionCache::new(10);
        let r = req(12345, 1, vec![], vec![]);
        match cache.classify(&r) {
            SessionDecision::Error { code } => {
                assert!(code == codes::FETCH_SESSION_ID_NOT_FOUND);
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
                assert!(code == codes::INVALID_FETCH_SESSION_EPOCH);
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
            SessionDecision::Close { session_id } => assert!(session_id == id),
            other => panic!("expected Close, got {other:?}"),
        }
        cache.close(id);
        assert!(cache.len() == 0);
        // Subsequent classify with the same id is now NOT_FOUND.
        let r2 = req(id, 1, vec![], vec![]);
        match cache.classify(&r2) {
            SessionDecision::Error { code } => {
                assert!(code == codes::FETCH_SESSION_ID_NOT_FOUND);
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
                assert!(code == codes::INVALID_FETCH_SESSION_EPOCH);
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
                check!(session_id == id);
                check!(new_epoch == 2);
                check!(partitions.len() == 2);
            }
            other => panic!("expected Incremental, got {other:?}"),
        }

        // Re-sending with the old epoch fails — broker advanced to 2.
        let r2 = req(id, 1, vec![], vec![]);
        match cache.classify(&r2) {
            SessionDecision::Error { code } => {
                assert!(code == codes::INVALID_FETCH_SESSION_EPOCH);
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
        // No duplicate entry created; the cached (fully-resolved) key is
        // preserved and its desired state updated in place.
        let expected = vec![(
            FetchSessionKey {
                topic_name: "t".into(),
                topic_id: tid,
                partition: 0,
            },
            CachedPartitionState {
                fetch_offset: 42,
                last_fetched_epoch: -1,
                current_leader_epoch: -1,
                max_bytes: 2048,
                log_start_offset: -1,
                last_high_watermark: 0,
                last_last_stable_offset: 0,
                last_log_start_offset: 0,
                last_preferred_read_replica: 0,
                last_aborted_txns_hash: 0,
                last_error_code: 0,
            },
        )];
        assert!(partitions == expected);
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
        let expected = vec![(
            FetchSessionKey {
                topic_name: "t".into(),
                topic_id: tid,
                partition: 0,
            },
            CachedPartitionState {
                fetch_offset: 99,
                last_fetched_epoch: -1,
                current_leader_epoch: -1,
                max_bytes: 4096,
                log_start_offset: -1,
                last_high_watermark: 0,
                last_last_stable_offset: 0,
                last_log_start_offset: 0,
                last_preferred_read_replica: 0,
                last_aborted_txns_hash: 0,
                last_error_code: 0,
            },
        )];
        assert!(partitions == expected);
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
                let mut parts: Vec<i32> = partitions.iter().map(|(k, _)| k.partition).collect();
                parts.sort_unstable();
                assert!(parts == vec![0, 2]);
            }
            other => panic!("expected Incremental, got {other:?}"),
        }
    }

    #[test]
    fn forgotten_topic_name_drops_only_matching_topic_partition() {
        let mut partitions = HashMap::from([
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
                    topic_name: "u".into(),
                    topic_id: WireUuid::ZERO,
                    partition: 0,
                },
                CachedPartitionState::default(),
            ),
        ]);
        let forgotten = vec![ForgottenTopic {
            topic: "t".into(),
            topic_id: WireUuid::ZERO,
            partitions: vec![0],
            ..Default::default()
        }];

        apply_incremental(&mut partitions, &forgotten, &[]);

        assert!(!partitions.contains_key(&FetchSessionKey {
            topic_name: "t".into(),
            topic_id: WireUuid::ZERO,
            partition: 0,
        }));
        assert!(partitions.contains_key(&FetchSessionKey {
            topic_name: "u".into(),
            topic_id: WireUuid::ZERO,
            partition: 0,
        }));
    }

    #[test]
    fn forgotten_topic_id_drops_only_matching_topic_partition() {
        let tid = WireUuid([1u8; 16]);
        let other_tid = WireUuid([2u8; 16]);
        let mut partitions = HashMap::from([
            (
                FetchSessionKey {
                    topic_name: "t".into(),
                    topic_id: tid,
                    partition: 0,
                },
                CachedPartitionState::default(),
            ),
            (
                FetchSessionKey {
                    topic_name: "u".into(),
                    topic_id: other_tid,
                    partition: 0,
                },
                CachedPartitionState::default(),
            ),
        ]);
        let forgotten = vec![ForgottenTopic {
            topic: String::new(),
            topic_id: tid,
            partitions: vec![0],
            ..Default::default()
        }];

        apply_incremental(&mut partitions, &forgotten, &[]);

        assert!(!partitions.contains_key(&FetchSessionKey {
            topic_name: "t".into(),
            topic_id: tid,
            partition: 0,
        }));
        assert!(partitions.contains_key(&FetchSessionKey {
            topic_name: "u".into(),
            topic_id: other_tid,
            partition: 0,
        }));
    }

    #[test]
    fn lru_eviction_drops_oldest_non_privileged() {
        let (cache, mock) = mock_cache(2);
        let a = cache.try_allocate(false, "a".into(), vec![]);
        // Advance logical time so each session gets a strictly increasing
        // `last_used_nanos`, making `a` the unambiguous LRU victim — no sleep.
        mock.advance(TICK);
        let b = cache.try_allocate(false, "b".into(), vec![]);
        mock.advance(TICK);
        let c = cache.try_allocate(false, "c".into(), vec![]);
        assert!(cache.len() == 2);
        assert!(cache.evictions_total() == 1);
        // `a` (oldest) was evicted; `b` and `c` remain.
        let g = cache.inner.lock().unwrap();
        let mut ids: Vec<i32> = g.sessions.keys().copied().collect();
        ids.sort_unstable();
        assert!(!ids.contains(&a) && ids == vec![b, c]);
    }

    #[test]
    fn non_privileged_cannot_evict_privileged() {
        let cache = FetchSessionCache::new(1);
        let p = cache.try_allocate(true, "follower".into(), vec![]);
        assert!(p > 0);
        // Cache full, only session is privileged. Consumer alloc refused.
        let c = cache.try_allocate(false, "consumer".into(), vec![]);
        check!(c == INVALID_SESSION_ID);
        check!(cache.evictions_total() == 0);
        check!(cache.len() == 1);
    }

    #[test]
    fn privileged_can_evict_privileged() {
        let (cache, mock) = mock_cache(1);
        let p1 = cache.try_allocate(true, "f1".into(), vec![]);
        // Advance so `f2` is strictly newer than `f1`; `f1` is the LRU victim.
        mock.advance(TICK);
        let p2 = cache.try_allocate(true, "f2".into(), vec![]);
        // p2 gets the next monotonic id (p1 + 1) after evicting p1.
        check!(p2 == p1 + 1);
        check!(cache.len() == 1);
        check!(cache.evictions_total() == 1);
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
        assert!(s.last_high_watermark == 42);
        assert!(s.last_log_start_offset == 7);
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
        assert!(cache.total_partitions_cached() == 5);
    }

    #[test]
    fn counters_track_merge_forget_and_close() {
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
        // Two partitions on allocate.
        let id = cache.try_allocate(false, "a".into(), vec![mk(0), mk(1)]);
        assert!(cache.len() == 1);
        assert!(cache.total_partitions_cached() == 2);

        // Incremental that forgets partition 1 and adds partitions 2 and 3:
        // net partition count goes 2 -> 3.
        let forgotten = vec![ForgottenTopic {
            topic: "t".into(),
            topic_id: WireUuid::ZERO,
            partitions: vec![1],
            ..Default::default()
        }];
        let r = req(id, 1, vec![topic("t", &[0, 2, 3])], forgotten);
        assert!(matches!(
            cache.classify(&r),
            SessionDecision::Incremental { .. }
        ));
        assert!(cache.total_partitions_cached() == 3);

        // Close drops the whole session and its partitions.
        cache.close(id);
        assert!(cache.len() == 0);
        assert!(cache.total_partitions_cached() == 0);
    }

    #[test]
    fn counters_track_large_incremental_add_delta() {
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
        let id = cache.try_allocate(false, "a".into(), vec![mk(0), mk(1)]);

        let r = req(id, 1, vec![topic("t", &[0, 1, 2, 3, 4])], vec![]);
        assert!(matches!(
            cache.classify(&r),
            SessionDecision::Incremental { .. }
        ));

        assert!(cache.total_partitions_cached() == 5);
    }

    #[test]
    fn counters_track_large_incremental_forget_delta() {
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
        let id = cache.try_allocate(false, "a".into(), vec![mk(0), mk(1), mk(2), mk(3), mk(4)]);
        let forgotten = vec![ForgottenTopic {
            topic: "t".into(),
            topic_id: WireUuid::ZERO,
            partitions: vec![2, 3, 4],
            ..Default::default()
        }];

        let r = req(id, 1, vec![], forgotten);
        assert!(matches!(
            cache.classify(&r),
            SessionDecision::Incremental { .. }
        ));

        assert!(cache.total_partitions_cached() == 2);
    }

    #[test]
    fn counters_track_eviction() {
        let cache = FetchSessionCache::new(1);
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
        assert!(cache.total_partitions_cached() == 2);
        // Allocating into the full cache evicts the lone session (2 parts)
        // and inserts a fresh one (1 part).
        cache.try_allocate(false, "b".into(), vec![mk(0)]);
        assert!(cache.len() == 1);
        assert!(cache.total_partitions_cached() == 1);
    }
}

/// Large-N random fuzzing of `apply_incremental`, the KIP-227 forget and merge
/// path. It complements the exhaustive `fetch_session_model`. Random sequences
/// of incremental fetches over a small topic, id, and partition universe, with
/// random identity halves, must keep no-shadow, subscription fidelity, and
/// no-orphan-default after every step.
#[cfg(test)]
mod fuzz {
    use std::collections::HashMap;

    use crabka_protocol::{
        owned::fetch_request::{FetchPartition, FetchTopic, ForgottenTopic},
        primitives::uuid::Uuid as WireUuid,
    };
    use proptest::prelude::*;

    use super::{CachedPartitionState, FetchSessionKey, apply_incremental};

    // name index 0 = empty (id-only wire form), 1 = "A", 2 = "B".
    fn name_of(i: u8) -> String {
        ["", "A", "B"][i as usize].to_string()
    }
    // id index 0 = ZERO (name-only wire form), 1 = U, 2 = V.
    fn id_of(i: u8) -> WireUuid {
        [WireUuid::ZERO, WireUuid([1; 16]), WireUuid([2; 16])][i as usize]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]
        #[test]
        fn forget_merge_invariants(
            ops in proptest::collection::vec(
                // (forget name, forget id, forget partition,
                //  do-subscribe, sub name, sub id, sub partition, sub max_bytes)
                (0u8..3, 0u8..3, 0i32..2, any::<bool>(), 0u8..3, 0u8..3, 0i32..2, 1i32..4),
                0..200,
            )
        ) {
            let mut partitions: HashMap<FetchSessionKey, CachedPartitionState> = HashMap::new();
            for (fname, fid, fp, do_sub, sname, sid, sp, mb) in ops {
                // A forget with an all-empty identity matches nothing — skip it
                // (the wire never carries a topic with neither name nor id).
                let forgotten = if fname == 0 && fid == 0 {
                    vec![]
                } else {
                    vec![ForgottenTopic {
                        topic: name_of(fname),
                        topic_id: id_of(fid),
                        partitions: vec![fp],
                        ..Default::default()
                    }]
                };
                let subscribe = do_sub && !(sname == 0 && sid == 0);
                let topics = if subscribe {
                    vec![FetchTopic {
                        topic: name_of(sname),
                        topic_id: id_of(sid),
                        partitions: vec![FetchPartition {
                            partition: sp,
                            partition_max_bytes: mb,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }]
                } else {
                    vec![]
                };

                apply_incremental(&mut partitions, &forgotten, &topics);

                // No shadow: no two keys share a logical partition.
                let keys: Vec<_> = partitions.keys().cloned().collect();
                for (i, a) in keys.iter().enumerate() {
                    for b in &keys[i + 1..] {
                        let shadow = a.partition == b.partition
                            && ((!a.topic_name.is_empty() && a.topic_name == b.topic_name)
                                || (a.topic_id != WireUuid::ZERO && a.topic_id == b.topic_id));
                        prop_assert!(!shadow, "shadow: {a:?} vs {b:?}");
                    }
                }

                // No orphan default: every cached entry carries a subscribed
                // max_bytes (merge always sets it; we only ever request >= 1).
                prop_assert!(partitions.values().all(|v| v.max_bytes != 0));

                // Subscription fidelity: a subscribed partition is reflected with
                // the requested max_bytes by some key matching the request.
                if subscribe {
                    let name = name_of(sname);
                    let id = id_of(sid);
                    let present = partitions.iter().any(|(k, st)| {
                        k.partition == sp
                            && ((!name.is_empty() && k.topic_name == name)
                                || (id != WireUuid::ZERO && k.topic_id == id))
                            && st.max_bytes == mb
                    });
                    prop_assert!(present, "subscription not reflected");
                }
            }
        }
    }
}
