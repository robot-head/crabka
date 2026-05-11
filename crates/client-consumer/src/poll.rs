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
    /// or an empty vec on timeout. If the heartbeat task signalled a
    /// rebalance, this returns `Err(CommitInvalid)`; the caller should drop
    /// any in-flight commits and rebuild the consumer.
    pub async fn poll(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<ConsumerRecord>, ConsumerError> {
        // 1. Drain any rebalance notices first.
        {
            let mut rebalance_rx = self.rebalance_rx.lock().await;
            if let Ok(notice) = rebalance_rx.try_recv() {
                tracing::info!(?notice, "rebalance notice received during poll");
                return Err(ConsumerError::CommitInvalid);
            }
        }

        // 2. Resolve any i64::MAX sentinels (auto.offset.reset=Latest) via
        //    ListOffsets(timestamp=-1).
        self.resolve_latest_sentinels().await?;

        // 3. Build a FetchRequest covering every assigned partition.
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

        let topics: Vec<FetchTopic> = by_topic
            .into_iter()
            .map(|(name, plist)| FetchTopic {
                topic: name,
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
            })
            .collect();

        let timeout_ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
        let resp = self
            .client
            .send(FetchRequest {
                max_wait_ms: timeout_ms,
                min_bytes: 1,
                max_bytes: 50 * 1024 * 1024,
                topics,
                ..Default::default()
            })
            .await?;

        // 4. Decode each partition's RecordBatch, advance next-offsets.
        //
        // The wire-level `records` field can carry multiple concatenated
        // RecordBatches, but the generated FetchResponse codec decodes a
        // single RecordBatch out of the bytes — which is good enough for
        // the MVP. We emit one ConsumerRecord per Record and bump
        // next_offsets to the highest seen offset + 1.
        let mut out: Vec<ConsumerRecord> = Vec::new();
        let mut offsets = self.next_offsets.lock().await;
        for topic in &resp.responses {
            for part in &topic.partitions {
                let Some(batch) = &part.records else { continue };
                for r in &batch.records {
                    let offset = batch.base_offset + i64::from(r.offset_delta);
                    out.push(ConsumerRecord {
                        topic: topic.topic.clone(),
                        partition: part.partition_index,
                        offset,
                        timestamp: batch.base_timestamp + r.timestamp_delta,
                        key: r.key.clone(),
                        value: r.value.clone(),
                    });
                    offsets.insert(
                        (topic.topic.clone(), part.partition_index),
                        offset + 1,
                    );
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
