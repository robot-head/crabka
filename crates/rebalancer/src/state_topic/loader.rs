//! Background task: consume the state topic from offset 0, track the
//! latest non-tombstone value, and flip `LoadedState::is_loaded` once
//! the consumer has seen no new records for 5 consecutive 100ms polls
//! (the "quiet period" end-of-log heuristic).

use std::sync::Arc;
use std::time::Duration;

use crabka_client_core::Client;
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::fetch_response::FetchResponse;
use crabka_protocol::records::RecordsPayload;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::state_topic::error::StateTopicError;
use crate::state_topic::{LoadedState, STATE_KEY, serde_format};

/// Kafka partition error codes treated as "topic exists but has no records
/// visible yet" — counts as a quiet poll rather than a hard error. Covers
/// transient windows between `ensure_topic` returning and the partition log
/// accepting consumer fetches (`LEADER_NOT_AVAILABLE`, `UNKNOWN_TOPIC_OR_PARTITION`
/// if the local router hasn't caught up yet).
const TRANSIENT_EMPTY_CODES: &[i16] = &[
    3, // UNKNOWN_TOPIC_OR_PARTITION — topic just created, router not yet updated
    5, // LEADER_NOT_AVAILABLE — leader election in progress
    9, // REPLICA_NOT_AVAILABLE — partition replica not yet available
];

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
                    let saw_new = apply_fetched_records(&self.state, &mut next_offset, records);
                    if saw_new {
                        quiet_polls = 0;
                    } else {
                        quiet_polls += 1;
                        if should_mark_loaded(quiet_polls, self.state.is_loaded()) {
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

    async fn poll_once(&self, fetch_offset: i64) -> Result<Vec<FetchedRecord>, StateTopicError> {
        let req = fetch_request(&self.topic, fetch_offset);
        let resp = self.client.send(req).await?;
        fetched_records_from_response(&resp)
    }
}

fn fetch_request(topic: &str, fetch_offset: i64) -> FetchRequest {
    FetchRequest {
        max_bytes: MAX_BYTES_PER_FETCH,
        topics: vec![FetchTopic {
            topic: topic.to_string(),
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset,
                partition_max_bytes: MAX_BYTES_PER_FETCH,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn should_mark_loaded(quiet_polls: u32, is_loaded: bool) -> bool {
    quiet_polls >= QUIET_POLLS_TO_DECLARE_LOADED && !is_loaded
}

fn apply_fetched_records(
    state: &LoadedState,
    next_offset: &mut i64,
    records: Vec<FetchedRecord>,
) -> bool {
    let saw_new = !records.is_empty();
    for (offset, key, value) in records {
        *next_offset = offset + 1;
        if key.as_deref() != Some(STATE_KEY.as_bytes()) {
            continue; // ignore unknown keys
        }
        match value {
            None => state.store(None),
            Some(bytes) => match serde_format::decode(&bytes) {
                Ok(f) => state.store(Some(f)),
                Err(e) => {
                    warn!(
                        error = %e,
                        offset,
                        "state-topic record had malformed JSON; skipping"
                    );
                }
            },
        }
    }
    saw_new
}

fn fetched_records_from_response(
    resp: &FetchResponse,
) -> Result<Vec<FetchedRecord>, StateTopicError> {
    let mut out: Vec<FetchedRecord> = Vec::new();
    for t in &resp.responses {
        for p in &t.partitions {
            if p.error_code != 0 {
                if TRANSIENT_EMPTY_CODES.contains(&p.error_code) {
                    // Transient: topic/partition not yet visible to this
                    // broker. Treat as empty — the caller counts it as a
                    // quiet poll.
                    debug!(
                        error_code = p.error_code,
                        "state-topic fetch: transient partition error; treating as empty"
                    );
                    continue;
                }
                return Err(StateTopicError::FetchErrorCode { code: p.error_code });
            }
            let Some(payload) = &p.records else { continue };
            let RecordsPayload::V2(batches) = payload else {
                continue;
            };
            for batch in batches {
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
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use bytes::Bytes;
    use crabka_protocol::owned::fetch_response::{
        FetchResponse, FetchableTopicResponse, PartitionData,
    };
    use crabka_protocol::records::{Record, RecordBatch, RecordsPayload};
    use std::time::Duration;

    use crate::executor::state::{InFlightFile, Phase};

    fn in_flight(id: &str) -> InFlightFile {
        InFlightFile::new(id.to_string(), Phase::Wait, 42, 50_000_000)
    }

    fn fetched(offset: i64, key: Option<&str>, value: Option<Vec<u8>>) -> FetchedRecord {
        (offset, key.map(|s| s.as_bytes().to_vec()), value)
    }

    fn fetch_response(error_code: i16, records: Option<RecordsPayload>) -> FetchResponse {
        FetchResponse {
            responses: vec![FetchableTopicResponse {
                partitions: vec![PartitionData {
                    error_code,
                    records,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn fetch_request_targets_state_topic_partition_with_consumer_limits() {
        let req = fetch_request("__crabka_state", 123);
        assert!(req.replica_id == -1);
        assert!(req.max_wait_ms == 0);
        assert!(req.min_bytes == 0);
        assert!(req.max_bytes == 1_048_576);
        assert!(req.topics.len() == 1);
        assert!(req.topics[0].topic == "__crabka_state");
        assert!(req.topics[0].partitions.len() == 1);
        assert!(req.topics[0].partitions[0].partition == 0);
        assert!(req.topics[0].partitions[0].fetch_offset == 123);
        assert!(req.topics[0].partitions[0].partition_max_bytes == 1_048_576);
    }

    #[test]
    fn should_mark_loaded_only_at_quiet_threshold_before_loaded() {
        assert!(!should_mark_loaded(4, false));
        assert!(should_mark_loaded(5, false));
        assert!(!should_mark_loaded(5, true));
    }

    #[test]
    fn applying_records_stores_state_key_and_advances_offset() {
        let state = LoadedState::new();
        let mut next_offset = 0;
        let value = serde_format::encode(&in_flight("p-1")).unwrap().to_vec();

        apply_fetched_records(
            &state,
            &mut next_offset,
            vec![fetched(4, Some(STATE_KEY), Some(value))],
        );

        assert!(next_offset == 5);
        assert!(state.current().is_some_and(|f| f.proposal_id == "p-1"));
    }

    #[test]
    fn applying_records_advances_offset_over_unknown_key_without_changing_state() {
        let state = LoadedState::new();
        let existing = in_flight("existing");
        state.store(Some(existing.clone()));
        let mut next_offset = 9;

        apply_fetched_records(
            &state,
            &mut next_offset,
            vec![fetched(9, Some("other-key"), Some(b"ignored".to_vec()))],
        );

        assert!(next_offset == 10);
        assert!(state.current().is_some_and(|f| f.proposal_id == "existing"));
    }

    #[test]
    fn applying_tombstone_clears_state_and_advances_offset() {
        let state = LoadedState::new();
        state.store(Some(in_flight("existing")));
        let mut next_offset = 0;

        apply_fetched_records(
            &state,
            &mut next_offset,
            vec![fetched(7, Some(STATE_KEY), None)],
        );

        assert!(next_offset == 8);
        assert!(state.current().is_none());
    }

    #[test]
    fn fetch_response_non_transient_error_is_returned() {
        let err = fetched_records_from_response(&fetch_response(42, None)).unwrap_err();
        assert!(matches!(err, StateTopicError::FetchErrorCode { code: 42 }));
    }

    #[test]
    fn fetch_response_extracts_absolute_offsets_from_batches() {
        let payload = RecordsPayload::V2(vec![RecordBatch {
            base_offset: 10,
            records: vec![
                Record {
                    offset_delta: 0,
                    key: Some(Bytes::from_static(STATE_KEY.as_bytes())),
                    value: Some(Bytes::from_static(b"one")),
                    ..Default::default()
                },
                Record {
                    offset_delta: 2,
                    key: Some(Bytes::from_static(STATE_KEY.as_bytes())),
                    value: Some(Bytes::from_static(b"two")),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }]);

        let records = fetched_records_from_response(&fetch_response(0, Some(payload))).unwrap();

        assert!(records.len() == 2);
        assert!(records[0].0 == 10);
        assert!(records[1].0 == 12);
    }

    #[tokio::test]
    async fn poll_once_propagates_fetch_send_errors() {
        let client = Arc::new(
            Client::builder()
                .bootstrap("127.0.0.1:1")
                .client_id("state-topic-loader-test")
                .connect_timeout(Duration::from_millis(50))
                .request_timeout(Duration::from_millis(50))
                .build()
                .await
                .expect("client build does not connect"),
        );
        let loader = StateTopicLoader {
            client,
            topic: "__crabka_state".into(),
            state: LoadedState::new(),
            shutdown: CancellationToken::new(),
        };

        assert!(loader.poll_once(0).await.is_err());
    }
}
