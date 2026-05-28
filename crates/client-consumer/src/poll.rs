//! `Consumer::poll` — issues one `Fetch` covering every assigned partition,
//! advances next-offsets, and returns the decoded records.

use std::collections::HashMap;
use std::time::Duration;

use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
};

use crate::consumer::{Consumer, ConsumerRecord};
use crate::error::ConsumerError;

impl Consumer {
    /// Returns at most one batch's worth of records per assigned partition,
    /// or an empty vec on timeout. Rebalances are handled transparently by
    /// the internal coordinator task, which mutates the live `assigned`
    /// snapshot in place; `poll()` simply reads it on each call.
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

        // 3. Decode each partition's RecordBatch, advance next-offsets.
        //
        // The wire-level `records` field can carry multiple concatenated
        // RecordBatches, but the generated FetchResponse codec decodes a
        // single RecordBatch out of the bytes — which is good enough for
        // the MVP. We emit one ConsumerRecord per Record and bump
        // next_offsets to the highest seen offset + 1.
        // Reverse-map topic_id → name. At Fetch v ≥ 13 the response carries
        // only `topic_id`; `topic.topic` is empty.
        let id_to_name: HashMap<_, _> = topic_ids
            .iter()
            .map(|(name, id)| (*id, name.clone()))
            .collect();

        let mut out: Vec<ConsumerRecord> = Vec::new();
        let mut offsets = self.next_offsets.lock().await;
        for topic in &resp.responses {
            let topic_name = if topic.topic.is_empty() {
                id_to_name.get(&topic.topic_id).cloned().unwrap_or_default()
            } else {
                topic.topic.clone()
            };
            for part in &topic.partitions {
                let Some(payload) = &part.records else {
                    continue;
                };
                // Legacy MessageSet payloads are skipped here; the consumer
                // only handles v2 batches.
                let Some(batch) = payload.as_v2() else {
                    continue;
                };
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
                    offsets.insert((topic_name.clone(), part.partition_index), offset + 1);
                }
            }
        }
        Ok(out)
    }

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
