//! KIP-932 share-session cache.
//!
//! A share session tracks the incremental `ShareFetch`/`ShareAcknowledge`
//! conversation between one consumer (`(group, member)`) and the
//! share-partition leader. The `share_session_epoch` on each request drives a
//! small state machine:
//!
//! - epoch `0` opens or re-opens a session, and the stored epoch becomes `1`.
//! - epoch `-1` (`FINAL_EPOCH`) closes the session, and the cache removes the
//!   entry.
//! - any other epoch must match the stored epoch exactly. The cache then bumps
//!   the stored epoch for the next request.
//!
//! Mismatches map to Kafka's share-session error codes:
//! `INVALID_SHARE_SESSION_EPOCH` for a stale or ahead epoch,
//! `SHARE_SESSION_NOT_FOUND` for a non-zero epoch with no live session, and
//! `SHARE_SESSION_LIMIT_REACHED` when the cache is full.
//!
//! Locking discipline: each cache operation releases the mutex before it
//! returns, and this module holds nothing across an `.await`.

use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

use crate::codes;

/// Epoch value a client sends to close its share session (Kafka's
/// `ShareRequestMetadata.FINAL_EPOCH`).
const FINAL_EPOCH: i32 = -1;
/// Epoch value a client sends to open a fresh share session.
const INITIAL_EPOCH: i32 = 0;

/// One live share session. It holds the current epoch and the set of share
/// partitions that the member currently has in its session. An initial fetch
/// replaces the set; each incremental fetch adds requested partitions and
/// removes forgotten partitions.
pub(crate) type SharePartitionKey = (uuid::Uuid, i32);

#[derive(Debug)]
struct ShareSession {
    epoch: i32,
    partitions: HashSet<SharePartitionKey>,
    connection_id: String,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ShareFetchSessionUpdate {
    /// Complete partition set to fetch after applying this request.
    pub(crate) partitions: HashSet<SharePartitionKey>,
    /// Partitions whose outstanding acquisitions must be released because an
    /// old or final session was removed.
    pub(crate) released: HashSet<SharePartitionKey>,
    pub(crate) final_request: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ClosedShareSession {
    pub(crate) group: String,
    pub(crate) member: String,
    pub(crate) partitions: HashSet<SharePartitionKey>,
}

#[derive(Debug, Default)]
struct Inner {
    sessions: HashMap<(String, String), ShareSession>,
    connections: HashMap<String, (String, String)>,
}

/// Process-wide cache of live share sessions keyed by `(group, member)`.
#[derive(Debug)]
pub(crate) struct ShareSessionCache {
    inner: Mutex<Inner>,
    max: usize,
}

impl ShareSessionCache {
    /// Create a cache that holds at most `max` concurrent sessions.
    pub(crate) fn new(max: usize) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            max,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn update_fetch(
        &self,
        group: &str,
        member: &str,
        connection_id: &str,
        epoch: i32,
        requested: &HashSet<SharePartitionKey>,
        forgotten: &HashSet<SharePartitionKey>,
        has_acknowledgements: bool,
        final_has_additions: bool,
    ) -> Result<ShareFetchSessionUpdate, i16> {
        let key = (group.to_string(), member.to_string());
        let mut inner = self.inner.lock().expect("share-session mutex poisoned");

        if epoch == FINAL_EPOCH {
            if !forgotten.is_empty() || final_has_additions {
                return Err(codes::INVALID_REQUEST);
            }
            let session = remove_session(&mut inner, &key).ok_or(codes::SHARE_SESSION_NOT_FOUND)?;
            return Ok(ShareFetchSessionUpdate {
                partitions: HashSet::new(),
                released: session.partitions,
                final_request: true,
            });
        }

        if epoch == INITIAL_EPOCH {
            if has_acknowledgements || !forgotten.is_empty() {
                return Err(codes::INVALID_REQUEST);
            }
            let released = remove_session(&mut inner, &key)
                .map_or_else(HashSet::new, |session| session.partitions);
            if inner.sessions.len() >= self.max {
                return Err(codes::SHARE_SESSION_LIMIT_REACHED);
            }
            inner
                .connections
                .insert(connection_id.to_string(), key.clone());
            inner.sessions.insert(
                key,
                ShareSession {
                    epoch: 1,
                    partitions: requested.clone(),
                    connection_id: connection_id.to_string(),
                },
            );
            return Ok(ShareFetchSessionUpdate {
                partitions: requested.clone(),
                released,
                final_request: false,
            });
        }

        let session = inner
            .sessions
            .get_mut(&key)
            .ok_or(codes::SHARE_SESSION_NOT_FOUND)?;
        if session.epoch != epoch {
            return Err(codes::INVALID_SHARE_SESSION_EPOCH);
        }
        session.partitions.extend(requested.iter().copied());
        session
            .partitions
            .retain(|partition| !forgotten.contains(partition));
        session.epoch = next_epoch(session.epoch);
        Ok(ShareFetchSessionUpdate {
            partitions: session.partitions.clone(),
            released: HashSet::new(),
            final_request: false,
        })
    }

    /// Validate a `ShareAcknowledge` epoch and advance or close its session.
    pub(crate) fn update_acknowledge(
        &self,
        group: &str,
        member: &str,
        epoch: i32,
    ) -> Result<HashSet<SharePartitionKey>, i16> {
        if epoch == INITIAL_EPOCH {
            return Err(codes::INVALID_SHARE_SESSION_EPOCH);
        }
        let key = (group.to_string(), member.to_string());
        let mut inner = self.inner.lock().expect("share-session mutex poisoned");
        if epoch == FINAL_EPOCH {
            return remove_session(&mut inner, &key)
                .map(|session| session.partitions)
                .ok_or(codes::SHARE_SESSION_NOT_FOUND);
        }
        let session = inner
            .sessions
            .get_mut(&key)
            .ok_or(codes::SHARE_SESSION_NOT_FOUND)?;
        if session.epoch != epoch {
            return Err(codes::INVALID_SHARE_SESSION_EPOCH);
        }
        session.epoch = next_epoch(session.epoch);
        Ok(HashSet::new())
    }

    /// Remove the session owned by a closing client connection.
    pub(crate) fn disconnect(&self, connection_id: &str) -> Option<ClosedShareSession> {
        let mut inner = self.inner.lock().expect("share-session mutex poisoned");
        let key = inner.connections.remove(connection_id)?;
        let session = inner.sessions.get(&key)?;
        if session.connection_id != connection_id {
            return None;
        }
        let session = inner.sessions.remove(&key)?;
        Some(ClosedShareSession {
            group: key.0,
            member: key.1,
            partitions: session.partitions,
        })
    }
}

fn remove_session(inner: &mut Inner, key: &(String, String)) -> Option<ShareSession> {
    let session = inner.sessions.remove(key)?;
    if inner.connections.get(&session.connection_id) == Some(key) {
        inner.connections.remove(&session.connection_id);
    }
    Some(session)
}

fn next_epoch(epoch: i32) -> i32 {
    epoch.checked_add(1).unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn partition(id: u8, partition: i32) -> SharePartitionKey {
        (uuid::Uuid::from_bytes([id; 16]), partition)
    }

    fn set(partitions: &[SharePartitionKey]) -> HashSet<SharePartitionKey> {
        partitions.iter().copied().collect()
    }

    fn fetch(
        cache: &ShareSessionCache,
        member: &str,
        epoch: i32,
        requested: &[SharePartitionKey],
        forgotten: &[SharePartitionKey],
    ) -> Result<ShareFetchSessionUpdate, i16> {
        cache.update_fetch(
            "g",
            member,
            "connection-a",
            epoch,
            &set(requested),
            &set(forgotten),
            false,
            false,
        )
    }

    #[test]
    fn open_then_incremental_merges_forgets_and_advances() {
        let cache = ShareSessionCache::new(8);
        let p0 = partition(1, 0);
        let p1 = partition(1, 1);
        let p2 = partition(2, 0);

        let opened = fetch(&cache, "m", 0, &[p0, p1], &[]).expect("open");
        assert!(opened.partitions == set(&[p0, p1]));

        let updated = fetch(&cache, "m", 1, &[p2], &[p0]).expect("incremental");
        assert!(updated.partitions == set(&[p1, p2]));
        assert!(fetch(&cache, "m", 2, &[], &[]).is_ok());
    }

    #[test]
    fn stale_epoch_is_invalid() {
        let cache = ShareSessionCache::new(8);
        assert!(fetch(&cache, "m", 0, &[], &[]).is_ok());
        // Stored epoch is 1; sending the wrong epoch is rejected.
        assert!(fetch(&cache, "m", 5, &[], &[]) == Err(codes::INVALID_SHARE_SESSION_EPOCH));
    }

    #[test]
    fn unknown_member_non_zero_epoch_not_found() {
        let cache = ShareSessionCache::new(8);
        assert!(fetch(&cache, "ghost", 3, &[], &[]) == Err(codes::SHARE_SESSION_NOT_FOUND));
    }

    #[test]
    fn close_removes_session() {
        let cache = ShareSessionCache::new(8);
        let p = partition(1, 0);
        assert!(fetch(&cache, "m", 0, &[p], &[]).is_ok());
        let closed = fetch(&cache, "m", -1, &[], &[]).expect("close");
        assert!(closed.final_request);
        assert!(closed.partitions.is_empty());
        assert!(closed.released == set(&[p]));
        assert!(fetch(&cache, "m", 1, &[], &[]) == Err(codes::SHARE_SESSION_NOT_FOUND));
    }

    #[test]
    fn close_absent_session_is_not_found() {
        let cache = ShareSessionCache::new(8);
        assert!(fetch(&cache, "never", -1, &[], &[]) == Err(codes::SHARE_SESSION_NOT_FOUND));
    }

    #[test]
    fn initial_fetch_rejects_acknowledgements_and_forgotten_partitions() {
        let cache = ShareSessionCache::new(8);
        let p = partition(1, 0);
        assert!(
            cache.update_fetch(
                "g",
                "m",
                "connection-a",
                0,
                &set(&[p]),
                &HashSet::new(),
                true,
                false,
            ) == Err(codes::INVALID_REQUEST)
        );
        assert!(fetch(&cache, "m", 0, &[], &[p]) == Err(codes::INVALID_REQUEST));
    }

    #[test]
    fn acknowledge_zero_epoch_is_invalid_and_final_closes() {
        let cache = ShareSessionCache::new(8);
        let p = partition(1, 0);
        assert!(fetch(&cache, "m", 0, &[p], &[]).is_ok());
        assert!(cache.update_acknowledge("g", "m", 0) == Err(codes::INVALID_SHARE_SESSION_EPOCH));
        assert!(cache.update_acknowledge("g", "m", 1).is_ok());
        assert!(cache.update_acknowledge("g", "m", -1) == Ok(set(&[p])));
    }

    #[test]
    fn final_fetch_rejects_partition_additions() {
        let cache = ShareSessionCache::new(8);
        assert!(fetch(&cache, "m", 0, &[], &[]).is_ok());
        assert!(
            cache.update_fetch(
                "g",
                "m",
                "connection-a",
                -1,
                &HashSet::new(),
                &HashSet::new(),
                false,
                true,
            ) == Err(codes::INVALID_REQUEST)
        );
    }

    #[test]
    fn over_capacity_is_limit_reached() {
        let cache = ShareSessionCache::new(1);
        let p = partition(1, 0);
        assert!(fetch(&cache, "m1", 0, &[p], &[]).is_ok());
        assert!(fetch(&cache, "m2", 0, &[], &[]) == Err(codes::SHARE_SESSION_LIMIT_REACHED));
        let reopened = fetch(&cache, "m1", 0, &[], &[]).expect("reopen");
        assert!(reopened.released == set(&[p]));
    }

    #[test]
    fn disconnect_removes_only_the_connections_live_session() {
        let cache = ShareSessionCache::new(8);
        let p = partition(1, 0);
        assert!(fetch(&cache, "m", 0, &[p], &[]).is_ok());
        let closed = cache.disconnect("connection-a").expect("session");
        assert!(closed.group == "g");
        assert!(closed.member == "m");
        assert!(closed.partitions == set(&[p]));
        assert!(cache.disconnect("connection-a").is_none());
    }

    #[test]
    fn reopening_does_not_remove_another_sessions_connection_mapping() {
        let cache = ShareSessionCache::new(8);
        let old_partition = partition(1, 0);
        let current_partition = partition(2, 0);
        cache
            .update_fetch(
                "g",
                "old",
                "connection-a",
                0,
                &set(&[old_partition]),
                &HashSet::new(),
                false,
                false,
            )
            .expect("open old session");
        cache
            .update_fetch(
                "g",
                "current",
                "connection-a",
                0,
                &set(&[current_partition]),
                &HashSet::new(),
                false,
                false,
            )
            .expect("replace connection mapping");
        cache
            .update_fetch(
                "g",
                "old",
                "connection-b",
                0,
                &HashSet::new(),
                &HashSet::new(),
                false,
                false,
            )
            .expect("reopen old member on a new connection");

        let closed = cache
            .disconnect("connection-a")
            .expect("current session remains mapped");
        assert!(closed.group == "g");
        assert!(closed.member == "current");
        assert!(closed.partitions == set(&[current_partition]));
    }
}
