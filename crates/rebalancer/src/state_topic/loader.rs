//! Background task: consume the state topic from offset 0, track the
//! latest non-tombstone value, and flip `LoadedState::is_loaded` once
//! the consumer has seen no new records for 5 consecutive 100ms polls
//! (the "quiet period" end-of-log heuristic).

use std::sync::Arc;

use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
    },
    records::RecordsPayload,
};
use crabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
    mebibytes, millis,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::state_topic::{
    LoadedState, STATE_KEY,
    error::{StateTopicError, is_transient_topic_partition_code},
    serde_format,
};

const POLL_INTERVAL: Time = millis(100);
const QUIET_POLLS_TO_DECLARE_LOADED: u32 = 5;
const MAX_BYTES_PER_FETCH: ByteSize = mebibytes(1);
/// The loader drains the log as fast as the broker will answer, so it asks for
/// whatever is already there rather than parking on the broker's fetch queue.
const NO_FETCH_WAIT: Time = Time::ZERO;
const NO_MIN_BYTES: ByteSize = ByteSize::ZERO;

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
                () = tokio::time::sleep(POLL_INTERVAL.to_std()) => {}
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
        max_wait_ms: NO_FETCH_WAIT.millis_i32(),
        min_bytes: NO_MIN_BYTES.bytes_i32(),
        max_bytes: MAX_BYTES_PER_FETCH.bytes_i32(),
        topics: vec![FetchTopic {
            topic: topic.to_string(),
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset,
                partition_max_bytes: MAX_BYTES_PER_FETCH.bytes_i32(),
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
                if is_transient_topic_partition_code(p.error_code) {
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
    use bytes::Bytes;
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::{
            fetch_request::ReplicaState,
            fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
        },
        primitives::uuid::Uuid,
        records::{Record, RecordBatch, RecordsPayload},
    };

    use super::*;
    use crate::executor::state::{InFlightFile, Phase};

    /// Connect/request timeout for the deliberately-unreachable test client.
    const CLIENT_TIMEOUT: Time = millis(50);

    fn in_flight(id: &str) -> InFlightFile {
        InFlightFile::new(
            id.to_string(),
            Phase::Wait,
            42,
            crabka_units::bytes_per_sec(50_000_000),
        )
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
        assert2::assert!(
            req == FetchRequest {
                replica_id: -1,
                max_wait_ms: 0,
                min_bytes: 0,
                max_bytes: 1_048_576,
                isolation_level: 0,
                session_id: 0,
                session_epoch: -1,
                topics: vec![FetchTopic {
                    topic: "__crabka_state".into(),
                    topic_id: Uuid([0; 16]),
                    partitions: vec![FetchPartition {
                        partition: 0,
                        current_leader_epoch: -1,
                        fetch_offset: 123,
                        last_fetched_epoch: -1,
                        log_start_offset: -1,
                        partition_max_bytes: 1_048_576,
                        replica_directory_id: Uuid([0; 16]),
                        high_watermark: i64::MAX,
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                forgotten_topics_data: vec![],
                rack_id: String::new(),
                cluster_id: None,
                replica_state: ReplicaState {
                    replica_id: -1,
                    replica_epoch: -1,
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                },
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn should_mark_loaded_only_at_quiet_threshold_before_loaded() {
        for (quiet_polls, is_loaded, want) in
            [(4, false, false), (5, false, true), (5, true, false)]
        {
            assert2::assert!(should_mark_loaded(quiet_polls, is_loaded) == want);
        }
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

        assert2::assert!(next_offset == 5);
        assert2::assert!(state.current().is_some_and(|f| f.proposal_id == "p-1"));
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

        assert2::assert!(next_offset == 10);
        assert2::assert!(state.current().is_some_and(|f| f.proposal_id == "existing"));
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

        assert2::assert!(next_offset == 8);
        assert2::assert!(state.current().is_none());
    }

    #[test]
    fn fetch_response_non_transient_error_is_returned() {
        let err = fetched_records_from_response(&fetch_response(42, None)).unwrap_err();
        assert2::assert!(matches!(err, StateTopicError::FetchErrorCode { code: 42 }));
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

        assert2::assert!(
            records
                == vec![
                    (
                        10,
                        Some(STATE_KEY.as_bytes().to_vec()),
                        Some(b"one".to_vec())
                    ),
                    (
                        12,
                        Some(STATE_KEY.as_bytes().to_vec()),
                        Some(b"two".to_vec())
                    ),
                ]
        );
    }

    #[tokio::test]
    async fn poll_once_propagates_fetch_send_errors() {
        let client = Arc::new(
            Client::builder()
                .bootstrap("127.0.0.1:1")
                .client_id("state-topic-loader-test")
                .connect_timeout(CLIENT_TIMEOUT)
                .request_timeout(CLIENT_TIMEOUT)
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

        assert2::assert!(loader.poll_once(0).await.is_err());
    }
}
