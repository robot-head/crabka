//! `Consumer::commit_sync` and `commit_async`.

use std::collections::HashMap;

use crabka_protocol::owned::offset_commit_request::OffsetCommitRequest;

use crate::consumer::Consumer;
use crate::error::ConsumerError;
use crate::offset_wire::build_commit_topics;

impl Consumer {
    /// Commit the current next-offsets for every assigned partition.
    /// Blocks until the broker acks.
    pub async fn commit_sync(&self) -> Result<(), ConsumerError> {
        let raw_offsets = self.next_offsets.lock().await.clone();
        if raw_offsets.is_empty() {
            return Ok(());
        }
        let pos = self.positions.lock().await;
        let offsets: HashMap<(String, i32), (i64, i32)> = raw_offsets
            .into_iter()
            .map(|(k, v)| {
                let epoch = pos.get(&k).map_or(-1, |p| p.offset_epoch);
                (k, (v, epoch))
            })
            .collect();
        drop(pos);
        let topic_ids = self.topic_ids.lock().await.clone();
        let topics = build_commit_topics(offsets, &topic_ids);

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
        let positions = self.positions.clone();
        let topic_ids = self.topic_ids.clone();
        tokio::spawn(async move {
            let raw_snapshot = offsets.lock().await.clone();
            if raw_snapshot.is_empty() {
                return;
            }
            let pos = positions.lock().await;
            let snapshot: HashMap<(String, i32), (i64, i32)> = raw_snapshot
                .into_iter()
                .map(|(k, v)| {
                    let epoch = pos.get(&k).map_or(-1, |p| p.offset_epoch);
                    (k, (v, epoch))
                })
                .collect();
            drop(pos);
            let topic_ids = topic_ids.lock().await.clone();
            let topics = build_commit_topics(snapshot, &topic_ids);
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
