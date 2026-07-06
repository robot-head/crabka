//! KIP-320 consumer position validation. Two responsibilities:
//!   1. Refresh per-partition leader id + leader epoch from `Metadata`, flagging
//!      a partition `awaiting_validation` when its leader epoch advances.
//!   2. For flagged partitions, issue `OffsetForLeaderEpoch` and decide (via
//!      `position::classify`) whether to resume or reset for truncation.

use std::collections::HashMap;

use crabka_ids::LeaderEpoch;

use crate::{
    consumer::Consumer,
    error::ConsumerError,
    position::{PartitionPosition, ValidationOutcome, classify},
};

fn update_leader_epoch(pos: &mut PartitionPosition, leader_epoch: LeaderEpoch) {
    if leader_epoch > pos.leader_epoch {
        pos.leader_epoch = leader_epoch;
        if should_await_validation(leader_epoch, pos.offset_epoch) {
            pos.awaiting_validation = true;
        }
    }
}

fn should_await_validation(leader_epoch: LeaderEpoch, offset_epoch: LeaderEpoch) -> bool {
    leader_epoch > offset_epoch && offset_epoch.is_known()
}

fn clear_unvalidatable_position(pos: &mut PartitionPosition) {
    if pos.awaiting_validation && !pos.offset_epoch.is_known() {
        pos.awaiting_validation = false;
    }
}

fn should_validate_position(pos: &PartitionPosition) -> bool {
    pos.awaiting_validation && pos.offset_epoch.is_known()
}

fn should_route_to_leader(leader_id: i32, knows_leader: bool) -> bool {
    leader_id >= 0 && knows_leader
}

fn response_still_current(pos: &PartitionPosition, leader_epoch: LeaderEpoch) -> bool {
    pos.leader_epoch == leader_epoch
}

fn should_skip_stale_response(pos: &PartitionPosition, leader_epoch: LeaderEpoch) -> bool {
    !response_still_current(pos, leader_epoch)
}

fn response_has_error(error_code: i16) -> bool {
    error_code != 0
}

fn mark_validation_error(pos: &mut PartitionPosition) {
    pos.leader_epoch = LeaderEpoch(-1);
    pos.awaiting_validation = true;
}

impl Consumer {
    /// Refresh leader id / leader epoch for every partition reported by
    /// `Metadata`. A partition whose metadata leader epoch is greater than the
    /// epoch we last consumed (`offset_epoch`) is flagged `awaiting_validation`.
    #[cfg_attr(test, mutants::skip)] // cargo-mutants: metadata-refresh I/O, exercised by integration tests
    #[tracing::instrument(
        name = "consumer.refresh_leader_epochs",
        level = "debug",
        skip_all,
        fields(group_id = %self.group_id),
        err
    )]
    pub(crate) async fn refresh_leader_epochs(&self) -> Result<(), ConsumerError> {
        // `refresh_metadata` (not a bare `send(MetadataRequest)`) so the main
        // client's BrokerPool learns each broker's (id → addr) mapping — this
        // is what lets `poll`/`validate` route Fetch and OffsetForLeaderEpoch
        // to the partition *leader* via `Client::broker(id)` instead of always
        // hitting the bootstrap connection.
        let md = self.client.refresh_metadata().await?;
        let mut positions = self.positions.lock().await;
        for t in &md.topics {
            let Some(name) = &t.name else { continue };
            for p in &t.partitions {
                let key = (name.clone(), p.partition_index);
                let entry = positions.entry(key).or_default();
                entry.leader_id = p.leader_id;
                // Wrap the raw wire `int32` leader epoch at the Metadata decode
                // boundary.
                update_leader_epoch(entry, LeaderEpoch(p.leader_epoch));
            }
        }
        Ok(())
    }

    /// Validate every `awaiting_validation` partition via `OffsetForLeaderEpoch`.
    /// Returns the set of partitions that truncated, mapped to the safe offset
    /// the caller must reset `next_offsets` to. Clears the validation flag for
    /// partitions confirmed consistent.
    #[cfg_attr(test, mutants::skip)] // cargo-mutants: OffsetForLeaderEpoch RPC orchestration, exercised by integration tests
    #[tracing::instrument(
        name = "consumer.validate_positions",
        level = "debug",
        skip_all,
        fields(
            group_id = %self.group_id,
            to_validate = tracing::field::Empty,
            truncated = tracing::field::Empty,
        ),
        err
    )]
    pub(crate) async fn validate_positions(
        &self,
    ) -> Result<HashMap<(String, i32), i64>, ConsumerError> {
        // Snapshot the work to do under the lock, then issue RPCs unlocked.
        // Lock order: next_offsets first, then positions — matching the
        // coordinator's established order, so we never deadlock against poll.
        // (topic, partition, offset, offset_epoch, leader_epoch, leader_id).
        let to_validate: Vec<(String, i32, i64, LeaderEpoch, LeaderEpoch, i32)> = {
            let offsets = self.next_offsets.lock().await;
            let mut positions = self.positions.lock().await;
            // Defensive: clear awaiting_validation for any partition whose
            // offset_epoch < 0 (never consumed). There is nothing to validate
            // below an offset never consumed, and leaving the flag set would
            // wedge the partition — validate_positions skips it but the fetch
            // builder also skips it, causing a permanent stall.
            for p in positions.values_mut() {
                clear_unvalidatable_position(p);
            }
            positions
                .iter()
                .filter(|(_, p)| should_validate_position(p))
                .filter_map(|((t, part), p)| {
                    let off = *offsets.get(&(t.clone(), *part))?;
                    Some((
                        t.clone(),
                        *part,
                        off,
                        p.offset_epoch,
                        p.leader_epoch,
                        p.leader_id,
                    ))
                })
                .collect()
        };

        tracing::Span::current().record("to_validate", to_validate.len());
        let mut truncated: HashMap<(String, i32), i64> = HashMap::new();
        for (topic, partition, offset, offset_epoch, leader_epoch, leader_id) in to_validate {
            // RPC issued with no lock held. KIP-320 requires the
            // OffsetForLeaderEpoch reach the partition *leader* — it is the
            // only replica with the authoritative epoch→end-offset history.
            // Route to the leader when its id is known and the pool has a
            // dialable address for it (registry populated by
            // `refresh_leader_epochs` → `refresh_metadata`); fall back to the
            // bootstrap connection otherwise (e.g. single-broker test brokers
            // advertising port 0, where bootstrap is the leader anyway).
            // Unwrap the leader epochs to raw `int32` at the client-core /
            // wire boundary (`current_leader_epoch` = `leader_epoch`,
            // `leader_epoch` arg = `offset_epoch`).
            let answer = if should_route_to_leader(leader_id, self.client.knows_broker(leader_id)) {
                self.client
                    .offset_for_leader_epoch_on(
                        leader_id,
                        &topic,
                        partition,
                        leader_epoch.get(),
                        offset_epoch.get(),
                    )
                    .await?
            } else {
                self.client
                    .offset_for_leader_epoch(
                        &topic,
                        partition,
                        leader_epoch.get(),
                        offset_epoch.get(),
                    )
                    .await?
            };

            // Re-check the partition is still assigned + epoch unchanged before
            // applying — a rebalance may have moved it.
            let mut positions = self.positions.lock().await;
            let Some(pos) = positions.get_mut(&(topic.clone(), partition)) else {
                continue;
            };
            if should_skip_stale_response(pos, leader_epoch) {
                continue; // metadata moved under us; revalidate next poll
            }

            // CRITICAL: inspect the per-partition error_code BEFORE classify.
            // On a non-zero code (FENCED_LEADER_EPOCH=74, UNKNOWN_LEADER_EPOCH=75,
            // NOT_LEADER_OR_FOLLOWER=6, UNKNOWN_TOPIC_OR_PARTITION=3, ...) the
            // broker returns end_offset = -1. Feeding that into `classify` would
            // look like truncation and wrongly reset to 0. Instead leave the
            // partition flagged for re-validation (and force a metadata refresh
            // next poll) and skip it.
            if response_has_error(answer.error_code) {
                // Reset leader_epoch so refresh_leader_epochs re-flags + the next
                // pass re-issues this RPC against fresher metadata.
                mark_validation_error(pos);
                continue;
            }

            // Wrap the leader's answer epoch (raw wire `int32`) at the
            // OffsetForLeaderEpoch decode boundary.
            match classify(
                offset,
                offset_epoch,
                LeaderEpoch(answer.leader_epoch),
                answer.end_offset,
            ) {
                ValidationOutcome::Valid { leader_epoch: le } => {
                    pos.offset_epoch = le;
                    pos.awaiting_validation = false;
                }
                ValidationOutcome::Truncated { safe_offset } => {
                    pos.awaiting_validation = false;
                    truncated.insert((topic, partition), safe_offset);
                }
            }
        }
        tracing::Span::current().record("truncated", truncated.len());
        Ok(truncated)
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn pos(offset_epoch: i32, leader_epoch: i32, awaiting_validation: bool) -> PartitionPosition {
        PartitionPosition {
            offset_epoch: LeaderEpoch(offset_epoch),
            leader_epoch: LeaderEpoch(leader_epoch),
            awaiting_validation,
            ..Default::default()
        }
    }

    #[test]
    fn leader_epoch_update_marks_only_real_advances_with_consumed_epoch() {
        let mut p = pos(2, 2, false);
        update_leader_epoch(&mut p, LeaderEpoch(2));
        assert!(p.leader_epoch == 2);
        assert!(!p.awaiting_validation);

        update_leader_epoch(&mut p, LeaderEpoch(3));
        assert!(p.leader_epoch == 3);
        assert!(p.awaiting_validation);

        let mut never_consumed = pos(-1, -1, false);
        update_leader_epoch(&mut never_consumed, LeaderEpoch(0));
        assert!(never_consumed.leader_epoch == 0);
        assert!(!never_consumed.awaiting_validation);

        let mut equal_but_consumed = pos(2, 3, false);
        update_leader_epoch(&mut equal_but_consumed, LeaderEpoch(3));
        assert!(!equal_but_consumed.awaiting_validation);

        assert!(!should_await_validation(LeaderEpoch(2), LeaderEpoch(2)));
    }

    #[test]
    fn validation_eligibility_clears_never_consumed_partitions() {
        let mut never_consumed = pos(-1, 4, true);
        clear_unvalidatable_position(&mut never_consumed);
        assert!(!never_consumed.awaiting_validation);
        assert!(!should_validate_position(&never_consumed));

        let consumed = pos(2, 4, true);
        assert!(should_validate_position(&consumed));

        let mut consumed_zero = pos(0, 4, true);
        clear_unvalidatable_position(&mut consumed_zero);
        assert!(should_validate_position(&consumed_zero));

        let already_clear = pos(2, 4, false);
        assert!(!should_validate_position(&already_clear));
    }

    #[test]
    fn validation_routing_requires_known_non_negative_leader() {
        let cases = [(3, true, true), (-1, true, false), (3, false, false)];
        for (leader_id, knows_leader, expected) in cases {
            assert!(should_route_to_leader(leader_id, knows_leader) == expected);
        }
    }

    #[test]
    fn response_epoch_must_still_match_position_epoch() {
        let p = pos(2, 7, true);
        check!(response_still_current(&p, LeaderEpoch(7)));
        check!(!response_still_current(&p, LeaderEpoch(6)));
        check!(!should_skip_stale_response(&p, LeaderEpoch(7)));
        check!(should_skip_stale_response(&p, LeaderEpoch(6)));
    }

    #[test]
    fn zero_error_code_is_success_and_nonzero_is_error() {
        assert!(!response_has_error(0));
        assert!(response_has_error(74));
    }

    #[test]
    fn validation_error_resets_epoch_and_keeps_partition_flagged() {
        let mut p = pos(2, 7, false);
        mark_validation_error(&mut p);
        assert!(p.leader_epoch == -1);
        assert!(p.awaiting_validation);
    }
}
