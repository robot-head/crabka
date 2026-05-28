//! Background task: consume the state topic from offset 0, track the
//! latest non-tombstone value, and flip `LoadedState::is_loaded` once
//! the consumer has seen no new records for 5 consecutive 100ms polls
//! (the "quiet period" end-of-log heuristic).

use std::sync::Arc;
use std::time::Duration;

use crabka_client_core::Client;
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::records::RecordsPayload;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::state_topic::error::StateTopicError;
use crate::state_topic::{LoadedState, STATE_KEY, serde_format};

const POLL_INTERVAL: Duration = Duration::from_millis(100);
const QUIET_POLLS_TO_DECLARE_LOADED: u32 = 5;
const MAX_BYTES_PER_FETCH: i32 = 1 << 20; // 1 MiB

/// `(absolute_offset, key_bytes, value_bytes)` — value is `None` for tombstones.
type FetchedRecord = (i64, Option<Vec<u8>>, Option<Vec<u8>>);

pub struct StateTopicLoader {
    pub client: Arc<Client>,
    pub topic: String,
    pub state: Arc<LoadedState>,
    pub shutdown: CancellationToken,
}

impl StateTopicLoader {
    pub async fn run(self) {
        info!(topic = %self.topic, "state-topic loader started");
        let mut next_offset: i64 = 0;
        let mut quiet_polls: u32 = 0;
        loop {
            tokio::select! {
                () = tokio::time::sleep(POLL_INTERVAL) => {}
                () = self.shutdown.cancelled() => {
                    info!("state-topic loader shutting down");
                    return;
                }
            }
            match self.poll_once(next_offset).await {
                Ok(records) => {
                    let saw_new = !records.is_empty();
                    for (offset, key, value) in records {
                        if key.as_deref() != Some(STATE_KEY.as_bytes()) {
                            continue; // ignore unknown keys
                        }
                        match value {
                            None => self.state.store(None),
                            Some(bytes) => match serde_format::decode(&bytes) {
                                Ok(f) => self.state.store(Some(f)),
                                Err(e) => {
                                    warn!(
                                        error = %e,
                                        offset,
                                        "state-topic record had malformed JSON; skipping"
                                    );
                                }
                            },
                        }
                        next_offset = offset + 1;
                    }
                    if saw_new {
                        quiet_polls = 0;
                    } else {
                        quiet_polls += 1;
                        if quiet_polls >= QUIET_POLLS_TO_DECLARE_LOADED
                            && !self.state.is_loaded()
                        {
                            info!("state-topic load reached steady state; marking loaded");
                            self.state.mark_loaded();
                        }
                    }
                }
                Err(e) => {
                    debug!(error = %e, "state-topic poll failed; will retry");
                    // Do NOT advance offset; do NOT count as quiet.
                }
            }
        }
    }

    async fn poll_once(
        &self,
        fetch_offset: i64,
    ) -> Result<Vec<FetchedRecord>, StateTopicError> {
        let req = FetchRequest {
            replica_id: -1,
            max_wait_ms: 0,
            min_bytes: 0,
            max_bytes: MAX_BYTES_PER_FETCH,
            topics: vec![FetchTopic {
                topic: self.topic.clone(),
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset,
                    partition_max_bytes: MAX_BYTES_PER_FETCH,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let resp = self.client.send(req).await?;
        let mut out: Vec<FetchedRecord> = Vec::new();
        for t in &resp.responses {
            for p in &t.partitions {
                if p.error_code != 0 {
                    return Err(StateTopicError::FetchErrorCode { code: p.error_code });
                }
                let Some(payload) = &p.records else { continue };
                let RecordsPayload::V2(batch) = payload else { continue };
                for r in &batch.records {
                    let off = batch.base_offset + i64::from(r.offset_delta);
                    out.push((
                        off,
                        r.key.as_ref().map(|b| b.to_vec()),
                        r.value.as_ref().map(|b| b.to_vec()),
                    ));
                }
            }
        }
        Ok(out)
    }
}
