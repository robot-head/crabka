//! KIP-320 consumer position validation. Two responsibilities:
//!   1. Refresh per-partition leader id + leader epoch from `Metadata`, flagging
//!      a partition `awaiting_validation` when its leader epoch advances.
//!   2. For flagged partitions, issue `OffsetForLeaderEpoch` and decide (via
//!      `position::classify`) whether to resume or reset for truncation.

use std::collections::HashMap;

use crate::consumer::Consumer;
use crate::error::ConsumerError;
use crate::position::{ValidationOutcome, classify};

impl Consumer {
    /// Refresh leader id / leader epoch for every partition reported by
    /// `Metadata`. A partition whose metadata leader epoch is greater than the
    /// epoch we last consumed (`offset_epoch`) is flagged `awaiting_validation`.
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
                if p.leader_epoch > entry.leader_epoch {
                    entry.leader_epoch = p.leader_epoch;
                    if p.leader_epoch > entry.offset_epoch && entry.offset_epoch >= 0 {
                        entry.awaiting_validation = true;
                    }
                }
            }
        }
        Ok(())
    }

    /// Validate every `awaiting_validation` partition via `OffsetForLeaderEpoch`.
    /// Returns the set of partitions that truncated, mapped to the safe offset
    /// the caller must reset `next_offsets` to. Clears the validation flag for
    /// partitions confirmed consistent.
    pub(crate) async fn validate_positions(
        &self,
    ) -> Result<HashMap<(String, i32), i64>, ConsumerError> {
        // Snapshot the work to do under the lock, then issue RPCs unlocked.
        // Lock order: next_offsets first, then positions — matching the
        // coordinator's established order, so we never deadlock against poll.
        // (topic, partition, offset, offset_epoch, leader_epoch, leader_id).
        let to_validate: Vec<(String, i32, i64, i32, i32, i32)> = {
            let offsets = self.next_offsets.lock().await;
            let mut positions = self.positions.lock().await;
            // Defensive: clear awaiting_validation for any partition whose
            // offset_epoch < 0 (never consumed). There is nothing to validate
            // below an offset never consumed, and leaving the flag set would
            // wedge the partition — validate_positions skips it but the fetch
            // builder also skips it, causing a permanent stall.
            for p in positions.values_mut() {
                if p.awaiting_validation && p.offset_epoch < 0 {
                    p.awaiting_validation = false;
                }
            }
            positions
                .iter()
                .filter(|(_, p)| p.awaiting_validation && p.offset_epoch >= 0)
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
            let answer = if leader_id >= 0 && self.client.knows_broker(leader_id) {
                self.client
                    .offset_for_leader_epoch_on(
                        leader_id,
                        &topic,
                        partition,
                        leader_epoch,
                        offset_epoch,
                    )
                    .await?
            } else {
                self.client
                    .offset_for_leader_epoch(&topic, partition, leader_epoch, offset_epoch)
                    .await?
            };

            // Re-check the partition is still assigned + epoch unchanged before
            // applying — a rebalance may have moved it.
            let mut positions = self.positions.lock().await;
            let Some(pos) = positions.get_mut(&(topic.clone(), partition)) else {
                continue;
            };
            if pos.leader_epoch != leader_epoch {
                continue; // metadata moved under us; revalidate next poll
            }

            // CRITICAL: inspect the per-partition error_code BEFORE classify.
            // On a non-zero code (FENCED_LEADER_EPOCH=74, UNKNOWN_LEADER_EPOCH=75,
            // NOT_LEADER_OR_FOLLOWER=6, UNKNOWN_TOPIC_OR_PARTITION=3, ...) the
            // broker returns end_offset = -1. Feeding that into `classify` would
            // look like truncation and wrongly reset to 0. Instead leave the
            // partition flagged for re-validation (and force a metadata refresh
            // next poll) and skip it.
            if answer.error_code != 0 {
                // Reset leader_epoch so refresh_leader_epochs re-flags + the next
                // pass re-issues this RPC against fresher metadata.
                pos.leader_epoch = -1;
                pos.awaiting_validation = true;
                continue;
            }

            match classify(offset, offset_epoch, answer.leader_epoch, answer.end_offset) {
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
        Ok(truncated)
    }
}
