//! `Consumer::commit_sync` and `commit_async`.

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use crabka_protocol::owned::offset_commit_request::OffsetCommitRequest;
use crabka_protocol::owned::offset_commit_response::OffsetCommitResponse;

use crate::consumer::Consumer;
use crate::coordinator::{COORDINATOR_RETRY_TIMEOUT, find_coordinator, with_coordinator_refind};
use crate::error::ConsumerError;
use crate::offset_wire::build_commit_topics;

/// First non-zero per-partition `error_code` in an `OffsetCommitResponse`, or
/// `0` if every partition committed cleanly. `with_coordinator_refind` reads
/// this to decide whether to re-discover the coordinator and retry.
fn first_commit_error(resp: &OffsetCommitResponse) -> i16 {
    for t in &resp.topics {
        for p in &t.partitions {
            if p.error_code != 0 {
                return p.error_code;
            }
        }
    }
    0
}

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

        // OffsetCommit is a coordinator RPC: route it to the coordinator broker
        // (discovered at build time, kept current by the coordinator task), and
        // re-discover on a cold/relocating-coordinator code so a coordinator
        // move is chased rather than looping NOT_COORDINATOR on the stale id.
        let resp = with_coordinator_refind(
            &self.client,
            &self.group_id,
            &self.coordinator_id,
            COORDINATOR_RETRY_TIMEOUT,
            first_commit_error,
            || {
                let group_id = self.group_id.clone();
                let member_id = self.member_id.clone();
                let topics = topics.clone();
                let client = &self.client;
                let target = self.coordinator_id.load(Ordering::Relaxed);
                async move {
                    client
                        .broker(target)
                        .send(OffsetCommitRequest {
                            group_id,
                            generation_id_or_member_epoch: self.generation_id,
                            member_id,
                            topics,
                            ..Default::default()
                        })
                        .await
                        .map_err(ConsumerError::from)
                }
            },
        )
        .await?;

        // Surface the first non-zero error_code if any.
        let code = first_commit_error(&resp);
        if code != 0 {
            return Err(ConsumerError::Server(code));
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
        let coordinator_id = self.coordinator_id.clone();
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
            // Route to the coordinator broker. If it returns a moved/cold
            // coordinator code (or the socket is gone), re-discover once and
            // retry — but don't block a background commit on the full retry
            // loop; one re-find recovers a coordinator move at-least-once.
            let make_req = |topics: Vec<_>| OffsetCommitRequest {
                group_id: group_id.clone(),
                generation_id_or_member_epoch: generation,
                member_id: member_id.clone(),
                topics,
                ..Default::default()
            };
            let target = coordinator_id.load(Ordering::Relaxed);
            let res = client.broker(target).send(make_req(topics.clone())).await;
            let moved = match &res {
                Ok(resp) => {
                    crate::coordinator::is_retriable_coordinator_code(first_commit_error(resp))
                }
                Err(crabka_client_core::ClientError::Disconnected) => true,
                Err(_) => false,
            };
            if moved {
                match find_coordinator(&client, &group_id).await {
                    Ok(id) => {
                        coordinator_id.store(id, Ordering::Relaxed);
                        if let Err(e) = client.broker(id).send(make_req(topics)).await {
                            tracing::warn!(error = %e, "commit_async retry after re-find failed");
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "commit_async coordinator re-discovery failed");
                    }
                }
            } else if let Err(e) = res {
                tracing::warn!(error = %e, "commit_async failed");
            }
        });
    }
}
