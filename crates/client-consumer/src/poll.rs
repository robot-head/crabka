//! `Consumer::poll` — issues one `Fetch` covering every assigned partition,
//! advances next-offsets, and returns the decoded records.

use std::collections::HashMap;
use std::time::Duration;

use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
};

use crate::builder::IsolationLevel;
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

        // 2. Build a FetchRequest covering every assigned partition.
        let assigned = self.assigned.lock().await.clone();
        if assigned.is_empty() {
            tokio::time::sleep(timeout).await;
            return Ok(Vec::new());
        }

        let mut by_topic: HashMap<String, Vec<(i32, i64)>> = HashMap::new();
        {
            let offsets = self.next_offsets.lock().await;
            for (t, p) in &assigned {
                let next = offsets.get(&(t.clone(), *p)).copied().unwrap_or(0);
                by_topic.entry(t.clone()).or_default().push((*p, next));
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
                        .map(|(p, off)| FetchPartition {
                            partition: p,
                            fetch_offset: off,
                            partition_max_bytes: 1 << 20,
                            ..Default::default()
                        })
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
                let Some(payload) = &part.records else {
                    continue;
                };
                // Legacy MessageSet payloads are skipped here; the consumer
                // only handles v2 batches.
                let Some(batches) = payload.as_v2() else {
                    continue;
                };
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
                        out.push(ConsumerRecord {
                            topic: topic_name.clone(),
                            partition: part.partition_index,
                            offset,
                            timestamp: batch.base_timestamp + r.timestamp_delta,
                            key: r.key.clone(),
                            value: r.value.clone(),
                        });
                    }
                }
                if let Some(next) = next_offset_after(batches) {
                    offsets.insert((topic_name.clone(), part.partition_index), next);
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

#[cfg(test)]
mod offset_advance_tests {
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
        assert_eq!(super::next_offset_after(batches), Some(15));
    }

    #[test]
    fn advance_target_none_for_empty() {
        let payload = RecordsPayload::V2(vec![]);
        assert_eq!(super::next_offset_after(payload.as_v2().unwrap()), None);
    }
}
