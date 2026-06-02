//! `Consumer::poll` — issues one `Fetch` covering every assigned partition,
//! advances next-offsets, and returns the decoded records.

use std::collections::HashMap;
use std::time::Duration;

use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
};

use crate::builder::{AutoOffsetReset, IsolationLevel};
use crate::consumer::{Consumer, ConsumerRecord};
use crate::error::ConsumerError;

impl Consumer {
    /// Returns the records from every v2 batch the broker returned per
    /// assigned partition, or an empty vec on timeout. Under
    /// `read_committed` isolation, control batches and records belonging to
    /// aborted transactions are filtered client-side using the response's
    /// `aborted_transactions` list (the broker returns verbatim bytes).
    /// Rebalances are handled transparently by the internal coordinator
    /// task, which mutates the live `assigned` snapshot in place; `poll()`
    /// simply reads it on each call.
    #[allow(clippy::too_many_lines)]
    pub async fn poll(&mut self, timeout: Duration) -> Result<Vec<ConsumerRecord>, ConsumerError> {
        // 1. Resolve any i64::MAX sentinels (auto.offset.reset=Latest) via
        //    ListOffsets(timestamp=-1).
        self.resolve_latest_sentinels().await?;

        // KIP-320: refresh leader epochs and proactively validate any position
        // whose leader epoch advanced, before fetching. Truncated partitions
        // are reset here (or surfaced for auto.offset.reset=None below).
        self.refresh_leader_epochs().await?;
        let truncated = self.validate_positions().await?;
        if !truncated.is_empty() {
            self.apply_truncation(&truncated).await?;
        }

        // 2. Build a FetchRequest covering every assigned partition.
        let assigned = self.assigned.lock().await.clone();
        if assigned.is_empty() {
            tokio::time::sleep(timeout).await;
            return Ok(Vec::new());
        }

        // (partition, fetch_offset, current_leader_epoch, last_fetched_epoch).
        // Lock order: next_offsets first, then positions (matching the
        // coordinator's order so poll can never deadlock against a rebalance).
        let mut by_topic: HashMap<String, Vec<(i32, i64, i32, i32)>> = HashMap::new();
        {
            let offsets = self.next_offsets.lock().await;
            let positions = self.positions.lock().await;
            for (t, p) in &assigned {
                // Skip partitions still awaiting validation — they must not be
                // fetched until proven consistent.
                if positions
                    .get(&(t.clone(), *p))
                    .is_some_and(|x| x.awaiting_validation)
                {
                    continue;
                }
                let next = offsets.get(&(t.clone(), *p)).copied().unwrap_or(0);
                let pos = positions.get(&(t.clone(), *p)).copied().unwrap_or_default();
                by_topic.entry(t.clone()).or_default().push((
                    *p,
                    next,
                    pos.leader_epoch,
                    pos.offset_epoch,
                ));
            }
        }

        let topic_ids = self.topic_ids.lock().await.clone();
        let topics: Vec<FetchTopic> = by_topic
            .into_iter()
            .map(|(name, plist)| {
                let topic_id = topic_ids.get(&name).copied().unwrap_or_default();
                FetchTopic {
                    topic: name,
                    topic_id,
                    partitions: plist
                        .into_iter()
                        .map(
                            |(p, off, leader_epoch, last_fetched_epoch)| FetchPartition {
                                partition: p,
                                fetch_offset: off,
                                current_leader_epoch: leader_epoch,
                                last_fetched_epoch,
                                partition_max_bytes: 1 << 20,
                                ..Default::default()
                            },
                        )
                        .collect(),
                    ..Default::default()
                }
            })
            .collect();

        let timeout_ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
        let resp = self
            .client
            .send(FetchRequest {
                max_wait_ms: timeout_ms,
                min_bytes: 1,
                max_bytes: 50 * 1024 * 1024,
                isolation_level: self.isolation_level.wire(),
                topics,
                ..Default::default()
            })
            .await?;

        // 3. Decode each partition's RecordBatches, advance next-offsets.
        //
        // The wire-level `records` field can carry multiple concatenated
        // RecordBatches; we iterate every v2 batch, emit one ConsumerRecord
        // per Record, and bump next_offsets to the highest seen offset + 1.
        // Reverse-map topic_id → name. At Fetch v ≥ 13 the response carries
        // only `topic_id`; `topic.topic` is empty.
        let id_to_name: HashMap<_, _> = topic_ids
            .iter()
            .map(|(name, id)| (*id, name.clone()))
            .collect();

        // Re-snapshot the assignment: a cooperative rebalance may have
        // revoked partitions while this Fetch was in flight. Records for
        // partitions we no longer own must be dropped — the new owner will
        // serve them from the offset we committed at revoke time. Snapshot
        // before locking `next_offsets` to keep the coordinator's
        // assigned→next_offsets lock order (avoids deadlock).
        let still_owned: std::collections::HashSet<(String, i32)> =
            self.assigned.lock().await.iter().cloned().collect();

        let mut out: Vec<ConsumerRecord> = Vec::new();
        let mut offsets = self.next_offsets.lock().await;
        for topic in &resp.responses {
            let topic_name = if topic.topic.is_empty() {
                id_to_name.get(&topic.topic_id).cloned().unwrap_or_default()
            } else {
                topic.topic.clone()
            };
            for part in &topic.partitions {
                // Drop records for partitions revoked while this Fetch was
                // in flight (cooperative rebalance transparency).
                if !still_owned.contains(&(topic_name.clone(), part.partition_index)) {
                    continue;
                }

                let key = (topic_name.clone(), part.partition_index);

                // KIP-320 in-band truncation: leader served no records and told
                // us where to truncate (diverging_epoch.end_offset >= 0).
                if part.diverging_epoch.end_offset >= 0 {
                    self.handle_truncation_in_poll(
                        &mut offsets,
                        &key,
                        part.diverging_epoch.end_offset,
                    )?;
                    continue;
                }
                // Error-first: inspect the partition error_code before decoding.
                match part.error_code {
                    0 => {}
                    1 /* OFFSET_OUT_OF_RANGE */ => {
                        // Reset per policy using the response's log_start_offset
                        // (the broker includes it in every OOR partition response).
                        // We must NOT use a hardcoded 0: if retention has moved
                        // log_start forward, re-fetching from 0 re-triggers OOR
                        // forever. Mirrors what the replicator does on OOR.
                        // No RPC needed — log_start_offset is already in `part`.
                        let fetch_offset = offsets.get(&key).copied().unwrap_or(-1);
                        let log_start = part.log_start_offset;
                        let (topic, partition) = (key.0.clone(), key.1);
                        match self.auto_offset_reset {
                            AutoOffsetReset::Earliest => {
                                // Reset to the real log start, not 0.
                                offsets.insert(key.clone(), log_start);
                            }
                            AutoOffsetReset::Latest => {
                                // Plant i64::MAX sentinel; resolved next poll
                                // by resolve_latest_sentinels via ListOffsets.
                                offsets.insert(key.clone(), i64::MAX);
                            }
                            AutoOffsetReset::None => {
                                return Err(ConsumerError::LogTruncation {
                                    topic,
                                    partition,
                                    fetch_offset,
                                    safe_offset: log_start,
                                });
                            }
                        }
                        continue;
                    }
                    74 /* FENCED_LEADER_EPOCH */
                    | 75 /* UNKNOWN_LEADER_EPOCH */
                    | 6 /* NOT_LEADER_OR_FOLLOWER */ => {
                        let mut positions = self.positions.lock().await;
                        if let Some(p) = positions.get_mut(&key) {
                            // Force refresh_leader_epochs to re-flag against
                            // fresher metadata next poll (any real epoch >= 0 > -1).
                            p.leader_epoch = -1;
                            // Only gate on validation when we have a consumed epoch
                            // to validate against. A never-consumed partition
                            // (offset_epoch < 0) has nothing to validate; flagging it
                            // would wedge it — validate_positions skips offset_epoch
                            // < 0, and the fetch builder skips awaiting_validation.
                            if p.offset_epoch >= 0 {
                                p.awaiting_validation = true;
                            }
                        }
                        continue;
                    }
                    other => {
                        return Err(ConsumerError::Server(other));
                    }
                }

                let Some(payload) = &part.records else {
                    continue;
                };
                // Legacy MessageSet payloads are skipped here; the consumer
                // only handles v2 batches.
                let Some(batches) = payload.as_v2() else {
                    continue;
                };
                // The broker returns whole record batches whose last offset is
                // >= the requested fetch_offset, even when the batch starts
                // before it (e.g. after an OFFSET_OUT_OF_RANGE reset or when
                // a single large batch straddles log_start). Kafka's JVM
                // client skips any records below the position; we do the same.
                // Capture the position now — before `next_offset_after` updates
                // it — so the filter baseline matches the actual fetch offset.
                let fetch_floor = offsets.get(&key).copied().unwrap_or(0);
                // read_committed filtering happens entirely client-side: the
                // broker returns verbatim on-disk bytes (control batches,
                // aborted records and all) plus an `aborted_transactions`
                // list. We replay Kafka's algorithm — walk batches in offset
                // order, tracking which producer_ids have an open aborted
                // transaction, and drop transactional records from those.
                let read_committed = self.isolation_level == IsolationLevel::ReadCommitted;
                // Aborted txns sorted by first_offset; consumed front-to-back
                // as batch offsets advance past each entry's start.
                let mut aborted: std::collections::VecDeque<(i64, i64)> = if read_committed {
                    let mut v: Vec<(i64, i64)> = part
                        .aborted_transactions
                        .as_deref()
                        .unwrap_or(&[])
                        .iter()
                        .map(|a| (a.first_offset, a.producer_id))
                        .collect();
                    v.sort_unstable();
                    v.into()
                } else {
                    std::collections::VecDeque::new()
                };
                // producer_ids with a currently-open aborted transaction.
                let mut aborted_pids: std::collections::HashSet<i64> =
                    std::collections::HashSet::new();
                for batch in batches {
                    // Move every aborted txn that starts at or before this
                    // batch into the active set.
                    if read_committed {
                        while let Some(&(first_offset, pid)) = aborted.front() {
                            if first_offset <= batch.base_offset {
                                aborted_pids.insert(pid);
                                aborted.pop_front();
                            } else {
                                break;
                            }
                        }
                    }
                    // Control batches (commit/abort markers) carry no user
                    // records. A control batch for a producer ends its aborted
                    // transaction; drop the batch either way.
                    if batch.attributes.is_control_batch() {
                        if read_committed {
                            aborted_pids.remove(&batch.producer_id);
                        }
                        continue;
                    }
                    // Drop transactional records belonging to an aborted txn.
                    if read_committed
                        && batch.attributes.is_transactional()
                        && aborted_pids.contains(&batch.producer_id)
                    {
                        continue;
                    }
                    for r in &batch.records {
                        let offset = batch.base_offset + i64::from(r.offset_delta);
                        // Skip records that precede the fetch floor: the broker
                        // returned a whole batch whose base_offset < our
                        // position (straddle case — see fetch_floor comment).
                        if offset < fetch_floor {
                            continue;
                        }
                        out.push(ConsumerRecord {
                            topic: topic_name.clone(),
                            partition: part.partition_index,
                            offset,
                            leader_epoch: batch.partition_leader_epoch,
                            timestamp: batch.base_timestamp + r.timestamp_delta,
                            key: r.key.clone(),
                            value: r.value.clone(),
                        });
                    }
                }
                if let Some(next) = next_offset_after(batches) {
                    offsets.insert(key.clone(), next);
                    // Advance the position's offset_epoch to the highest batch
                    // leader epoch consumed, so the next Fetch sends the correct
                    // last_fetched_epoch (KIP-320). Lock order holds: offsets is
                    // already locked, positions acquired second.
                    if let Some(last_epoch) = batches.iter().map(|b| b.partition_leader_epoch).max()
                    {
                        let mut positions = self.positions.lock().await;
                        positions.entry(key.clone()).or_default().offset_epoch = last_epoch;
                    }
                }
            }
        }
        Ok(out)
    }
}

/// The offset to fetch next after consuming `batches`: one past the highest
/// `base_offset + last_offset_delta` across all decoded batches. `None` when
/// there are no batches (offset unchanged). Used so the consumer advances past
/// control/aborted batches that emit no records, instead of re-fetching them.
fn next_offset_after(batches: &[crabka_protocol::records::RecordBatch]) -> Option<i64> {
    batches
        .iter()
        .map(|b| b.base_offset + i64::from(b.last_offset_delta) + 1)
        .max()
}

impl Consumer {
    /// Replace any `i64::MAX` sentinels in `next_offsets` (planted by
    /// `auto_offset_reset = Latest` at build time) with the real log-end
    /// offset from `ListOffsets(timestamp=-1)`.
    async fn resolve_latest_sentinels(&self) -> Result<(), ConsumerError> {
        let mut offsets = self.next_offsets.lock().await;
        let sentinels: Vec<(String, i32)> = offsets
            .iter()
            .filter(|(_, v)| **v == i64::MAX)
            .map(|(k, _)| k.clone())
            .collect();
        if sentinels.is_empty() {
            return Ok(());
        }
        let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
        for (t, p) in &sentinels {
            by_topic.entry(t.clone()).or_default().push(*p);
        }
        let topics: Vec<ListOffsetsTopic> = by_topic
            .into_iter()
            .map(|(name, partitions)| ListOffsetsTopic {
                name,
                partitions: partitions
                    .into_iter()
                    .map(|p| ListOffsetsPartition {
                        partition_index: p,
                        timestamp: -1, // LATEST
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect();
        let lo = self
            .client
            .send(ListOffsetsRequest {
                replica_id: -1,
                topics,
                ..Default::default()
            })
            .await?;
        for t in &lo.topics {
            for p in &t.partitions {
                offsets.insert((t.name.clone(), p.partition_index), p.offset);
            }
        }
        Ok(())
    }
}

impl Consumer {
    /// Apply truncations detected by the proactive validate pass to
    /// `next_offsets`, honoring `auto.offset.reset` (None → error on the first
    /// truncated partition).
    async fn apply_truncation(
        &self,
        truncated: &HashMap<(String, i32), i64>,
    ) -> Result<(), ConsumerError> {
        let mut offsets = self.next_offsets.lock().await;
        for (key, safe_offset) in truncated {
            if let AutoOffsetReset::None = self.auto_offset_reset {
                let fetch_offset = offsets.get(key).copied().unwrap_or(-1);
                return Err(ConsumerError::LogTruncation {
                    topic: key.0.clone(),
                    partition: key.1,
                    fetch_offset,
                    safe_offset: *safe_offset,
                });
            }
            offsets.insert(key.clone(), *safe_offset);
        }
        Ok(())
    }

    /// In-band `diverging_epoch` handler used inside the poll loop while the
    /// `next_offsets` guard is already held.
    fn handle_truncation_in_poll(
        &self,
        offsets: &mut HashMap<(String, i32), i64>,
        key: &(String, i32),
        safe_offset: i64,
    ) -> Result<(), ConsumerError> {
        if let AutoOffsetReset::None = self.auto_offset_reset {
            let fetch_offset = offsets.get(key).copied().unwrap_or(-1);
            return Err(ConsumerError::LogTruncation {
                topic: key.0.clone(),
                partition: key.1,
                fetch_offset,
                safe_offset,
            });
        }
        offsets.insert(key.clone(), safe_offset);
        Ok(())
    }
}

#[cfg(test)]
mod offset_advance_tests {
    use assert2::assert;
    use crabka_protocol::records::{RecordBatch, RecordsPayload};

    #[test]
    fn advance_target_uses_last_offset_delta_not_record_count() {
        // A batch spanning offsets 10..=14 (last_offset_delta = 4) but carrying
        // zero surviving records must still advance the fetch offset to 15.
        let batch = RecordBatch {
            base_offset: 10,
            last_offset_delta: 4,
            records: vec![],
            ..Default::default()
        };
        let payload = RecordsPayload::V2(vec![batch]);
        let batches = payload.as_v2().unwrap();
        assert!(super::next_offset_after(batches) == Some(15));
    }

    #[test]
    fn advance_target_none_for_empty() {
        let payload = RecordsPayload::V2(vec![]);
        assert!(super::next_offset_after(payload.as_v2().unwrap()) == None);
    }
}
