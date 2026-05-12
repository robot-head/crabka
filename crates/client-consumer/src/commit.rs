//! `Consumer::commit_sync` and `commit_async`.

use std::collections::HashMap;

use crabka_protocol::owned::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};

use crate::consumer::Consumer;
use crate::error::ConsumerError;

impl Consumer {
    /// Commit the current next-offsets for every assigned partition.
    /// Blocks until the broker acks.
    pub async fn commit_sync(&self) -> Result<(), ConsumerError> {
        let offsets = self.next_offsets.lock().await.clone();
        if offsets.is_empty() {
            return Ok(());
        }
        let topics = build_commit_topics(offsets);

        let resp = self
            .client
            .send(OffsetCommitRequest {
                group_id: self.group_id.clone(),
                generation_id_or_member_epoch: self.generation_id,
                member_id: self.member_id.clone(),
                topics,
                ..Default::default()
            })
            .await?;

        // Surface the first non-zero error_code if any.
        for t in &resp.topics {
            for p in &t.partitions {
                if p.error_code != 0 {
                    return Err(ConsumerError::Server(p.error_code));
                }
            }
        }
        Ok(())
    }

    /// Fire-and-forget commit. Returns once the request is enqueued on the
    /// client's writer task; does NOT wait for the broker ack. Errors are
    /// logged but not returned.
    pub fn commit_async(&self) {
        let client = self.client.clone();
        let group_id = self.group_id.clone();
        let generation = self.generation_id;
        let member_id = self.member_id.clone();
        let offsets = self.next_offsets.clone();
        tokio::spawn(async move {
            let snapshot = offsets.lock().await.clone();
            if snapshot.is_empty() {
                return;
            }
            let topics = build_commit_topics(snapshot);
            let res = client
                .send(OffsetCommitRequest {
                    group_id,
                    generation_id_or_member_epoch: generation,
                    member_id,
                    topics,
                    ..Default::default()
                })
                .await;
            if let Err(e) = res {
                tracing::warn!(error = %e, "commit_async failed");
            }
        });
    }
}

fn build_commit_topics(offsets: HashMap<(String, i32), i64>) -> Vec<OffsetCommitRequestTopic> {
    let mut by_topic: HashMap<String, Vec<(i32, i64)>> = HashMap::new();
    for ((t, p), off) in offsets {
        by_topic.entry(t).or_default().push((p, off));
    }
    by_topic
        .into_iter()
        .map(|(name, parts)| OffsetCommitRequestTopic {
            name,
            partitions: parts
                .into_iter()
                .map(|(p, off)| OffsetCommitRequestPartition {
                    partition_index: p,
                    committed_offset: off,
                    committed_leader_epoch: -1,
                    committed_metadata: Some(String::new()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect()
}
