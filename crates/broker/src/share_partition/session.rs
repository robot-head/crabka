//! KIP-932 share-session cache.
//!
//! A share session tracks the incremental `ShareFetch`/`ShareAcknowledge`
//! conversation between one consumer (`(group, member)`) and the
//! share-partition leader. The `share_session_epoch` on each request drives a
//! small state machine:
//!
//! - epoch `0` opens (or re-opens) a session — the stored epoch becomes `1`.
//! - epoch `-1` (`FINAL_EPOCH`) closes the session — the entry is removed.
//! - any other epoch must match the stored epoch exactly; the stored epoch is
//!   then bumped for the next request.
//!
//! Mismatches map to Kafka's share-session error codes:
//! `INVALID_SHARE_SESSION_EPOCH` (stale/ahead epoch),
//! `SHARE_SESSION_NOT_FOUND` (a non-zero epoch with no live session), and
//! `SHARE_SESSION_LIMIT_REACHED` (the cache is full).
//!
//! Locking discipline: the `DashMap` guard is taken and released entirely
//! within `validate`; nothing here is held across an `.await`.

use std::collections::HashSet;

use dashmap::DashMap;

use crate::codes;

/// Epoch value a client sends to close its share session (Kafka's
/// `ShareRequestMetadata.FINAL_EPOCH`).
const FINAL_EPOCH: i32 = -1;
/// Epoch value a client sends to open a fresh share session.
const INITIAL_EPOCH: i32 = 0;

/// One live share session: the current epoch plus the set of share partitions
/// the member currently has in its session. The partition set is maintained by
/// the `ShareFetch` handler (full fetches replace it; the field is reserved for
/// incremental-fetch bookkeeping).
#[derive(Debug, Default)]
struct ShareSession {
    epoch: i32,
    #[allow(dead_code)]
    partitions: HashSet<(uuid::Uuid, i32)>,
}

/// Process-wide cache of live share sessions keyed by `(group, member)`.
#[derive(Debug)]
pub(crate) struct ShareSessionCache {
    sessions: DashMap<(String, String), ShareSession>,
    max: usize,
}

impl ShareSessionCache {
    /// Create a cache holding at most `max` concurrent sessions.
    pub(crate) fn new(max: usize) -> Self {
        Self {
            sessions: DashMap::new(),
            max,
        }
    }

    /// Validate (and advance) the share session for `(group, member)` against
    /// the request's `epoch`.
    ///
    /// On success the stored epoch is updated for the next request. See the
    /// module docs for the epoch state machine and the error codes returned.
    pub(crate) fn validate(&self, group: &str, member: &str, epoch: i32) -> Result<(), i16> {
        let key = (group.to_string(), member.to_string());

        if epoch == FINAL_EPOCH {
            // Close: drop the session (idempotent — closing an absent
            // session is a no-op, not an error).
            self.sessions.remove(&key);
            return Ok(());
        }

        if epoch == INITIAL_EPOCH {
            // Open or re-open. An existing entry is reset to epoch 1; a new one
            // is admitted only if there's room.
            if let Some(mut entry) = self.sessions.get_mut(&key) {
                entry.epoch = 1;
                entry.partitions.clear();
                return Ok(());
            }
            if self.sessions.len() >= self.max {
                return Err(codes::SHARE_SESSION_LIMIT_REACHED);
            }
            self.sessions.insert(
                key,
                ShareSession {
                    epoch: 1,
                    partitions: HashSet::new(),
                },
            );
            return Ok(());
        }

        // Incremental: the session must exist and the epoch must match exactly.
        let Some(mut entry) = self.sessions.get_mut(&key) else {
            return Err(codes::SHARE_SESSION_NOT_FOUND);
        };
        if entry.epoch != epoch {
            return Err(codes::INVALID_SHARE_SESSION_EPOCH);
        }
        // Advance for the next request, wrapping past the reserved
        // open/close sentinels (KIP-932 skips 0 and -1).
        entry.epoch = match entry.epoch.checked_add(1) {
            Some(next) if next > 0 => next,
            _ => 1,
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn open_then_incremental_advances() {
        let cache = ShareSessionCache::new(8);
        // epoch 0 opens → stored epoch becomes 1; each subsequent request
        // must carry the stored epoch, which advances by 1 on success.
        for (case, epoch) in [
            ("open", 0),
            ("first incremental", 1),
            ("second incremental", 2),
        ] {
            assert!(
                cache.validate("g", "m", epoch) == Ok(()),
                "case {case}, epoch {epoch}"
            );
        }
    }

    #[test]
    fn stale_epoch_is_invalid() {
        let cache = ShareSessionCache::new(8);
        assert!(cache.validate("g", "m", 0) == Ok(()));
        // Stored epoch is 1; sending the wrong epoch is rejected.
        assert!(cache.validate("g", "m", 5) == Err(codes::INVALID_SHARE_SESSION_EPOCH));
    }

    #[test]
    fn unknown_member_non_zero_epoch_not_found() {
        let cache = ShareSessionCache::new(8);
        assert!(cache.validate("g", "ghost", 3) == Err(codes::SHARE_SESSION_NOT_FOUND));
    }

    #[test]
    fn close_removes_session() {
        let cache = ShareSessionCache::new(8);
        // Open, close (epoch -1), then an incremental epoch finds nothing.
        for (case, epoch, want) in [
            ("open", 0, Ok(())),
            ("close", -1, Ok(())),
            (
                "incremental after close",
                1,
                Err(codes::SHARE_SESSION_NOT_FOUND),
            ),
        ] {
            assert!(
                cache.validate("g", "m", epoch) == want,
                "case {case}, epoch {epoch}"
            );
        }
    }

    #[test]
    fn close_absent_session_is_ok() {
        let cache = ShareSessionCache::new(8);
        assert!(cache.validate("g", "never", -1) == Ok(()));
    }

    #[test]
    fn over_capacity_is_limit_reached() {
        let cache = ShareSessionCache::new(1);
        // m1 takes the single slot; a second new session is rejected;
        // re-opening the existing session is still fine.
        for (case, member, want) in [
            ("first member opens", "m1", Ok(())),
            (
                "second member exceeds capacity",
                "m2",
                Err(codes::SHARE_SESSION_LIMIT_REACHED),
            ),
            ("existing member reopens", "m1", Ok(())),
        ] {
            assert!(
                cache.validate("g", member, 0) == want,
                "case {case}, member {member}"
            );
        }
    }
}
