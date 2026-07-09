//! Streaming Connect handlers — bidirectional `SendStream` (produce) and
//! `Subscribe` (consume). The per-handler logic lives in a `*_inner` function
//! returning a plain `Stream` (unit-testable); the public handler is a thin
//! wrapper into `ConnectResponse::new(StreamBody::new(inner))`.

use std::{
    collections::{BTreeSet, HashMap},
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
};

use axum::Extension;
use connectrpc_axum::message::{
    ConnectError, ConnectRequest, ConnectResponse, StreamBody, Streaming,
};
use crabka_authz::AuthorizationResult;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_security::Principal;
use futures_util::{Stream, StreamExt};

use crate::{
    codec::{SchemaFormat, SchemaMeta},
    consume::{ConsumeSession, PartitionAckState},
    filter::CompiledFilter,
    handlers::{anonymous_principal, authorize_resource, to_gateway_record, unknown_host},
    pb,
    state::AppState,
};

fn schema_meta_to_pb(meta: SchemaMeta) -> pb::SchemaSelector {
    pb::SchemaSelector {
        subject: meta.subject,
        id: meta.id,
        format: match meta.format {
            SchemaFormat::Avro => pb::SchemaFormat::Avro as i32,
            SchemaFormat::Json => pb::SchemaFormat::Json as i32,
            SchemaFormat::Protobuf => pb::SchemaFormat::Protobuf as i32,
        },
    }
}

fn inbound_from_decoded_record(record: crate::consume::DecodedConsumerRecord) -> pb::Inbound {
    let headers = record
        .headers
        .into_iter()
        .map(|header| pb::Header {
            key: header.key,
            value: header.value.map(|value| value.to_vec()),
        })
        .collect();

    pb::Inbound {
        topic: record.topic,
        partition: record.partition.into(),
        offset: record.offset.into(),
        key: record.key.map(|b| b.to_vec()),
        value: record.raw_value.to_vec(),
        headers,
        timestamp_ms: record.timestamp.into(),
        structured: record.json.map(|json| pb::StructuredValue {
            json: json.to_vec(),
        }),
        schema: record.schema.map(schema_meta_to_pb),
    }
}

#[derive(Debug)]
struct FilteredPollBatch {
    decisions: Vec<FilteredRecordDecision>,
}

#[derive(Debug)]
enum FilteredRecordDecision {
    Deliver(crate::consume::DecodedConsumerRecord),
    Filter(crate::consume::DecodedConsumerRecord),
}

impl FilteredPollBatch {
    #[cfg(all(test, feature = "arrow"))]
    fn delivered(&self) -> impl Iterator<Item = &crate::consume::DecodedConsumerRecord> {
        self.decisions.iter().filter_map(|decision| match decision {
            FilteredRecordDecision::Deliver(record) => Some(record),
            FilteredRecordDecision::Filter(_) => None,
        })
    }

    #[cfg(all(test, feature = "arrow"))]
    fn filtered(&self) -> impl Iterator<Item = &crate::consume::DecodedConsumerRecord> {
        self.decisions.iter().filter_map(|decision| match decision {
            FilteredRecordDecision::Deliver(_) => None,
            FilteredRecordDecision::Filter(record) => Some(record),
        })
    }
}

#[cfg(feature = "arrow")]
#[derive(Debug)]
struct ArrowIpcRecord {
    record: crate::consume::DecodedConsumerRecord,
    batches: Vec<arrow::array::RecordBatch>,
    row_count: usize,
}

#[cfg(feature = "arrow")]
#[derive(Debug, Default)]
struct ArrowIpcRecordGroup {
    schema_key: Option<String>,
    records: Vec<ArrowIpcRecord>,
}

#[cfg(feature = "arrow")]
#[derive(Debug, Default)]
struct SchemaJsonRecordGroup {
    schema_id: Option<i32>,
    records: Vec<crate::consume::DecodedConsumerRecord>,
}

#[cfg(feature = "arrow")]
impl ArrowIpcRecordGroup {
    fn accepts(&self, schema_key: &str) -> bool {
        self.schema_key
            .as_deref()
            .is_none_or(|key| key == schema_key)
    }

    fn push(&mut self, schema_key: String, record: ArrowIpcRecord) {
        if self.schema_key.is_none() {
            self.schema_key = Some(schema_key);
        }
        self.records.push(record);
    }

    fn drain_filtering_into(
        &mut self,
        filter: &CompiledFilter,
        decisions: &mut Vec<FilteredRecordDecision>,
    ) -> Result<(), crate::filter::FilterCompileError> {
        if self.records.is_empty() {
            return Ok(());
        }

        let records = std::mem::take(&mut self.records);
        self.schema_key = None;
        filter_arrow_ipc_group(filter, records, decisions)
    }
}

#[cfg(feature = "arrow")]
impl SchemaJsonRecordGroup {
    fn accepts(&self, schema_id: i32) -> bool {
        self.schema_id.is_none_or(|id| id == schema_id)
    }

    fn push(&mut self, schema_id: i32, record: crate::consume::DecodedConsumerRecord) {
        if self.schema_id.is_none() {
            self.schema_id = Some(schema_id);
        }
        self.records.push(record);
    }

    fn drain_filtering_into(
        &mut self,
        filter: &CompiledFilter,
        decisions: &mut Vec<FilteredRecordDecision>,
    ) -> Result<(), crate::filter::FilterCompileError> {
        if self.records.is_empty() {
            return Ok(());
        }

        let schema_id = self.schema_id;
        let records = std::mem::take(&mut self.records);
        self.schema_id = None;
        filter_schema_json_group(filter, schema_id, records, decisions)
    }
}

#[cfg(feature = "arrow")]
fn filter_polled_records(
    filter: &CompiledFilter,
    records: Vec<crate::consume::DecodedConsumerRecord>,
) -> Result<FilteredPollBatch, crate::filter::FilterCompileError> {
    filter_polled_records_with_arrow_batches(filter, records)
}

#[cfg(not(feature = "arrow"))]
fn filter_polled_records(
    filter: &CompiledFilter,
    records: Vec<crate::consume::DecodedConsumerRecord>,
) -> FilteredPollBatch {
    filter_polled_records_with_json_fallback(filter, records)
}

#[cfg(not(feature = "arrow"))]
fn filter_polled_records_with_json_fallback(
    filter: &CompiledFilter,
    records: Vec<crate::consume::DecodedConsumerRecord>,
) -> FilteredPollBatch {
    let mut decisions = Vec::with_capacity(records.len());

    for record in records {
        let decision = filter.evaluate_decoded_record(record.json.as_ref(), &record.value);
        if decision.should_deliver() {
            decisions.push(FilteredRecordDecision::Deliver(record));
        } else {
            decisions.push(FilteredRecordDecision::Filter(record));
        }
    }

    FilteredPollBatch { decisions }
}

#[cfg(feature = "arrow")]
fn filter_polled_records_with_arrow_batches(
    filter: &CompiledFilter,
    records: Vec<crate::consume::DecodedConsumerRecord>,
) -> Result<FilteredPollBatch, crate::filter::FilterCompileError> {
    let mut decisions = Vec::with_capacity(records.len());
    let mut arrow_group = ArrowIpcRecordGroup::default();
    let mut schema_json_group = SchemaJsonRecordGroup::default();

    for record in records {
        let Some((batches, row_count)) = decode_arrow_ipc_record(&record)? else {
            arrow_group.drain_filtering_into(filter, &mut decisions)?;
            if let Some(schema_id) = schema_json_filter_schema_id(&record) {
                if !schema_json_group.accepts(schema_id) {
                    schema_json_group.drain_filtering_into(filter, &mut decisions)?;
                }
                schema_json_group.push(schema_id, record);
            } else {
                schema_json_group.drain_filtering_into(filter, &mut decisions)?;
                filter_json_fallback_record(filter, record, &mut decisions);
            }
            continue;
        };

        schema_json_group.drain_filtering_into(filter, &mut decisions)?;
        let arrow_record = ArrowIpcRecord {
            record,
            batches,
            row_count,
        };

        let schema_key = arrow_ipc_record_schema_key(&arrow_record);
        if !arrow_group.accepts(&schema_key) {
            arrow_group.drain_filtering_into(filter, &mut decisions)?;
        }
        arrow_group.push(schema_key, arrow_record);
    }
    arrow_group.drain_filtering_into(filter, &mut decisions)?;
    schema_json_group.drain_filtering_into(filter, &mut decisions)?;

    Ok(FilteredPollBatch { decisions })
}

#[cfg(feature = "arrow")]
fn schema_json_filter_schema_id(record: &crate::consume::DecodedConsumerRecord) -> Option<i32> {
    record.json.as_ref()?;
    record.schema.as_ref().map(|schema| schema.id)
}

#[cfg(feature = "arrow")]
fn filter_schema_json_group(
    filter: &CompiledFilter,
    schema_id: Option<i32>,
    records: Vec<crate::consume::DecodedConsumerRecord>,
    decisions: &mut Vec<FilteredRecordDecision>,
) -> Result<(), crate::filter::FilterCompileError> {
    use arrow::array::Array;

    if records.is_empty() {
        return Ok(());
    }

    let rows = records
        .iter()
        .map(|record| {
            let Some(json) = record.json.as_ref() else {
                return Err(crate::filter::FilterCompileError::DataFusion(
                    "schema-registry row bridge requires decoded JSON rows".to_string(),
                ));
            };
            serde_json::from_slice::<serde_json::Value>(json).map_err(|error| {
                crate::filter::FilterCompileError::DataFusion(format!(
                    "schema-registry row bridge JSON decode failed: {error}"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let batch =
        crabka_client_streams::columnar::serde::arrow::json_rows_to_arrow_filter_batch(&rows)
            .map_err(|error| crate::filter::FilterCompileError::DataFusion(error.to_string()))?;
    let mask = filter.evaluate_arrow_batch_for_schema_id(schema_id, &batch)?;
    if mask.len() != records.len() {
        return Err(crate::filter::FilterCompileError::DataFusion(format!(
            "schema-registry row bridge produced {} filter rows for {} records",
            mask.len(),
            records.len()
        )));
    }

    for (row, record) in records.into_iter().enumerate() {
        if !mask.is_null(row) && mask.value(row) {
            decisions.push(FilteredRecordDecision::Deliver(record));
        } else {
            decisions.push(FilteredRecordDecision::Filter(record));
        }
    }

    Ok(())
}

#[cfg(feature = "arrow")]
fn decode_arrow_ipc_record(
    record: &crate::consume::DecodedConsumerRecord,
) -> Result<Option<(Vec<arrow::array::RecordBatch>, usize)>, crate::filter::FilterCompileError> {
    let Ok(reader) = arrow::ipc::reader::StreamReader::try_new(&record.value[..], None) else {
        return Ok(None);
    };

    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| crate::filter::FilterCompileError::DataFusion(error.to_string()))?;
    let row_count = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    Ok(Some((batches, row_count)))
}

#[cfg(feature = "arrow")]
fn arrow_ipc_record_schema_key(record: &ArrowIpcRecord) -> String {
    let Some(first_batch) = record.batches.first() else {
        return "empty-arrow-ipc-stream".to_string();
    };

    arrow_schema_compatibility_key(first_batch.schema().as_ref())
}

#[cfg(feature = "arrow")]
fn arrow_schema_compatibility_key(schema: &arrow::datatypes::Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|field| {
            let mut metadata = field
                .metadata()
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>();
            metadata.sort();
            format!(
                "{}:{:?}:{}:{}",
                field.name(),
                field.data_type(),
                field.is_nullable(),
                metadata.join(",")
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

#[cfg(feature = "arrow")]
fn filter_arrow_ipc_group(
    filter: &CompiledFilter,
    records: Vec<ArrowIpcRecord>,
    decisions: &mut Vec<FilteredRecordDecision>,
) -> Result<(), crate::filter::FilterCompileError> {
    if records.is_empty() {
        return Ok(());
    }

    let Some(schema) = records
        .iter()
        .flat_map(|record| record.batches.iter())
        .next()
        .map(arrow::array::RecordBatch::schema)
    else {
        decisions.extend(
            records
                .into_iter()
                .map(|record| FilteredRecordDecision::Filter(record.record)),
        );
        return Ok(());
    };

    let batches = records
        .iter()
        .flat_map(|record| record.batches.iter().cloned())
        .collect::<Vec<_>>();
    let batch = arrow::compute::concat_batches(&schema, &batches)
        .map_err(|error| crate::filter::FilterCompileError::DataFusion(error.to_string()))?;
    let mask = filter.evaluate_arrow_batch(&batch)?;
    let mut row_offset = 0;

    for record in records {
        let matching_rows = count_true_mask_values(&mask, row_offset, record.row_count);
        row_offset += record.row_count;
        if matching_rows > 0 {
            decisions.push(FilteredRecordDecision::Deliver(record.record));
        } else {
            decisions.push(FilteredRecordDecision::Filter(record.record));
        }
    }

    Ok(())
}

#[cfg(feature = "arrow")]
fn count_true_mask_values(mask: &arrow::array::BooleanArray, start: usize, len: usize) -> usize {
    use arrow::array::Array;

    (start..start + len)
        .filter(|row| !mask.is_null(*row) && mask.value(*row))
        .count()
}

#[cfg(feature = "arrow")]
fn filter_json_fallback_record(
    filter: &CompiledFilter,
    record: crate::consume::DecodedConsumerRecord,
    decisions: &mut Vec<FilteredRecordDecision>,
) {
    if filter.matches_structured_json(record.json.as_ref()) {
        decisions.push(FilteredRecordDecision::Deliver(record));
    } else {
        decisions.push(FilteredRecordDecision::Filter(record));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExplicitCommitOffset {
    topic: String,
    partition: i32,
    next_offset: i64,
}

impl ExplicitCommitOffset {
    fn insert_into(self, offsets: &mut HashMap<(String, i32), i64>) {
        offsets.insert((self.topic, self.partition), self.next_offset);
    }
}

#[derive(Debug, Default)]
struct PendingAutoCommitOffsets {
    offsets: HashMap<(String, i32), i64>,
}

impl PendingAutoCommitOffsets {
    fn record(&mut self, record: &crate::consume::DecodedConsumerRecord) {
        let partition = record.partition.into();
        let next_offset = i64::from(record.offset) + 1;
        self.offsets
            .entry((record.topic.clone(), partition))
            .and_modify(|offset| *offset = (*offset).max(next_offset))
            .or_insert(next_offset);
    }

    fn offsets_to_commit(&self) -> Option<HashMap<(String, i32), i64>> {
        if self.offsets.is_empty() {
            return None;
        }

        Some(self.offsets.clone())
    }

    fn mark_committed(&mut self, committed_offsets: &HashMap<(String, i32), i64>) {
        self.offsets.retain(|key, offset| {
            let Some(committed_offset) = committed_offsets.get(key) else {
                return true;
            };

            *offset > *committed_offset
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SubscribeAckError {
    NegativeOffset(i64),
    UnknownPartition {
        topic: String,
        partition: i32,
    },
    UndeliveredOffset {
        topic: String,
        partition: i32,
        offset: i64,
    },
    PendingOverflow {
        topic: String,
        partition: i32,
    },
}

impl std::fmt::Display for SubscribeAckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NegativeOffset(offset) => {
                write!(f, "Subscribe ack offset must be non-negative, got {offset}")
            }
            Self::UnknownPartition { topic, partition } => write!(
                f,
                "Subscribe ack references undelivered partition {topic}:{partition}"
            ),
            Self::UndeliveredOffset {
                topic,
                partition,
                offset,
            } => write!(
                f,
                "Subscribe ack references undelivered offset {topic}:{partition}:{offset}"
            ),
            Self::PendingOverflow { topic, partition } => write!(
                f,
                "Subscribe ack pending set overflow for partition {topic}:{partition}"
            ),
        }
    }
}

#[derive(Debug, Default)]
struct SubscribeAckTracker {
    partitions: HashMap<(String, i32), DeliveredPartitionAckState>,
}

#[derive(Debug)]
struct DeliveredPartitionAckState {
    acknowledgments: PartitionAckState,
    delivered_offsets: BTreeSet<i64>,
}

impl DeliveredPartitionAckState {
    fn new(first_delivered_offset: i64) -> Self {
        let mut acknowledgments = PartitionAckState::new(first_delivered_offset);
        acknowledgments.record_observed(first_delivered_offset);
        Self {
            acknowledgments,
            delivered_offsets: BTreeSet::from([first_delivered_offset]),
        }
    }

    fn record_delivery(&mut self, offset: i64) {
        self.delivered_offsets.insert(offset);
        self.acknowledgments.record_observed(offset);
    }

    fn record_ack(&mut self, offset: i64) -> Result<(), AckOffsetWasNotDelivered> {
        if !self.delivered_offsets.contains(&offset) {
            return Err(AckOffsetWasNotDelivered);
        }

        self.acknowledgments
            .record(offset)
            .map_err(|_| AckOffsetWasNotDelivered)
    }

    fn commit_value(&self) -> Option<i64> {
        self.acknowledgments.commit_value()
    }

    fn mark_committed(&mut self) {
        self.acknowledgments.mark_committed();
    }
}

struct AckOffsetWasNotDelivered;

impl SubscribeAckTracker {
    fn record_delivery(&mut self, record: &crate::consume::DecodedConsumerRecord) {
        let topic = record.topic.clone();
        let partition = record.partition.into();
        let offset = record.offset.into();
        self.partitions
            .entry((topic, partition))
            .and_modify(|state| state.record_delivery(offset))
            .or_insert_with(|| DeliveredPartitionAckState::new(offset));
    }

    fn record_filtered_delivery(
        &mut self,
        record: &crate::consume::DecodedConsumerRecord,
    ) -> Result<Option<ExplicitCommitOffset>, SubscribeAckError> {
        self.record_delivery(record);
        self.record_ack(pb::SubscribeAck {
            topic: record.topic.clone(),
            partition: record.partition.into(),
            offset: record.offset.into(),
        })
    }

    fn record_ack(
        &mut self,
        ack: pb::SubscribeAck,
    ) -> Result<Option<ExplicitCommitOffset>, SubscribeAckError> {
        if ack.offset < 0 {
            return Err(SubscribeAckError::NegativeOffset(ack.offset));
        }

        let topic = ack.topic;
        let partition = ack.partition;
        let key = (topic.clone(), partition);
        let Some(state) = self.partitions.get_mut(&key) else {
            return Err(SubscribeAckError::UnknownPartition { topic, partition });
        };

        state.record_ack(ack.offset).map_err(|_| {
            if state.delivered_offsets.contains(&ack.offset) {
                SubscribeAckError::PendingOverflow { topic, partition }
            } else {
                SubscribeAckError::UndeliveredOffset {
                    topic,
                    partition,
                    offset: ack.offset,
                }
            }
        })?;

        Ok(state
            .commit_value()
            .map(|next_offset| ExplicitCommitOffset {
                topic: key.0,
                partition: key.1,
                next_offset,
            }))
    }

    #[cfg(test)]
    fn mark_committed(&mut self, commit: &ExplicitCommitOffset) {
        let Some(state) = self
            .partitions
            .get_mut(&(commit.topic.clone(), commit.partition))
        else {
            return;
        };
        state.mark_committed();
    }

    fn mark_offsets_committed(&mut self, offsets: &HashMap<(String, i32), i64>) {
        for key in offsets.keys() {
            let Some(state) = self.partitions.get_mut(key) else {
                continue;
            };
            state.mark_committed();
        }
    }
}

/// Produce every record in each inbound `SendRequest`, emitting one `SendAck`
/// (with a per-record `RecordResult` vector) per request. Each record is gated
/// by a Write ACL on its target topic for the on-behalf-of `principal`/`host`;
/// a denied record is skipped and reported as a non-retriable `PERMISSION_DENIED`.
pub fn send_stream_inner(
    mut inbound: Streaming<pb::SendRequest>,
    state: Arc<AppState>,
    principal: Principal,
    host: SocketAddr,
) -> impl Stream<Item = Result<pb::SendAck, ConnectError>> {
    async_stream::stream! {
        while let Some(item) = inbound.next().await {
            let send_req = match item {
                Ok(r) => r,
                Err(e) => { yield Err(e); break; }
            };
            let mut results = Vec::with_capacity(send_req.records.len());
            for r in send_req.records {
                let rec = to_gateway_record(r);
                if authorize_resource(
                    &state,
                    &principal,
                    &host,
                    ResourceType::Topic,
                    &rec.topic,
                    AclOperation::Write,
                ) == AuthorizationResult::Deny
                {
                    results.push(crate::handlers::error_result(
                        &crate::error::GatewayError::Unauthorized(format!(
                            "Write Topic:{}",
                            rec.topic
                        )),
                    ));
                    continue;
                }
                let result = match state.produce.produce(rec, &principal).await {
                    Ok(o) => pb::RecordResult {
                        partition: o.partition.into(),
                        offset: o.offset.into(),
                        deduplicated: o.deduplicated,
                        error: None,
                    },
                    Err(e) => crate::handlers::error_result(&e),
                };
                results.push(result);
            }
            yield Ok(pb::SendAck { results });
        }
    }
}

/// Bidi `SendStream` Connect handler.
pub async fn send_stream(
    Extension(state): Extension<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    peer: Option<Extension<SocketAddr>>,
    req: ConnectRequest<Streaming<pb::SendRequest>>,
) -> Result<
    ConnectResponse<
        StreamBody<Pin<Box<dyn Stream<Item = Result<pb::SendAck, ConnectError>> + Send>>>,
    >,
    ConnectError,
> {
    let eff = principal.map_or_else(anonymous_principal, |Extension(p)| p);
    let host = peer.map_or_else(unknown_host, |Extension(a)| a);
    Ok(ConnectResponse::new(StreamBody::new(Box::pin(
        send_stream_inner(req.0, state, eff, host),
    ))))
}

/// Join a consumer group on the caller's behalf and stream records. The first
/// frame MUST be `Start`; subsequent `Ack` frames drive offset commits
/// (at-least-once). The subscription ends when the control stream closes or
/// errors.
///
/// Commit semantics: on `Ack`, the session commits only the acknowledged
/// contiguous offset frontier for that `topic`/`partition`. The committed value
/// is Kafka's next offset (`acked_offset + 1`). With `auto_commit`, the session
/// preserves the existing current-position commit after each non-empty poll (at
/// enqueue, slightly weaker than on-receipt). For strict at-least-once, the
/// caller should ack synchronously per received batch.
#[allow(clippy::too_many_lines)]
pub fn subscribe_inner(
    mut frames: Streaming<pb::SubscribeFrame>,
    state: Arc<AppState>,
    principal: Principal,
    host: SocketAddr,
) -> impl Stream<Item = Result<pb::Inbound, ConnectError>> {
    async_stream::stream! {
        // First frame must be Start.
        let start = match frames.next().await {
            Some(Ok(pb::SubscribeFrame { frame: Some(pb::subscribe_frame::Frame::Start(s)) })) => s,
            Some(Ok(_)) => { yield Err(ConnectError::new_invalid_argument("first Subscribe frame must be Start")); return; }
            Some(Err(e)) => { yield Err(e); return; }
            None => return,
        };

        // Authorize the subscription up-front: the consumer group (Read) and
        // every topic (Read). Subscribing requires read access to all of them —
        // any Deny ends the stream with a PERMISSION_DENIED (there is no
        // per-topic partial subscribe on the Connect surface).
        if authorize_resource(&state, &principal, &host, ResourceType::Group, &start.group_id, AclOperation::Read)
            == AuthorizationResult::Deny
        {
            yield Err(ConnectError::new_permission_denied(format!("Read Group:{}", start.group_id)));
            return;
        }
        for topic in &start.topics {
            if authorize_resource(&state, &principal, &host, ResourceType::Topic, topic, AclOperation::Read)
                == AuthorizationResult::Deny
            {
                yield Err(ConnectError::new_permission_denied(format!("Read Topic:{topic}")));
                return;
            }
        }

        let filter = match CompiledFilter::compile(&start.filter) {
            Ok(filter) => filter,
            Err(e) => { yield Err(ConnectError::new_invalid_argument(e.to_string())); return; }
        };

        let client_id = format!("{}-sub", state.config.client_id);
        let mut session = match ConsumeSession::new(&state.config.bootstrap, &start.group_id, &client_id, start.topics, state.config.broker_security.clone(), state.codec.clone()).await {
            Ok(s) => s,
            Err(e) => { yield Err(ConnectError::new_internal(e.to_string())); return; }
        };
        let auto_commit = start.auto_commit;
        let mut ack_tracker = SubscribeAckTracker::default();
        // Kafka may defer commits while the group is stabilizing and ask the
        // next call to retry. Keep the highest offset observed from real
        // non-empty polls so fully filtered batches still advance without ever
        // turning an empty poll into a new commit position.
        let mut pending_auto_commit_offsets = PendingAutoCommitOffsets::default();

        loop {
            // BORROW NOTE: do NOT call session.commit() inside a select! arm —
            // session.poll(..) holds a &mut borrow across the select. Instead set
            // flags inside the select and commit AFTER it resolves.
            let mut commit_current_position = false;
            let mut explicit_commit_offsets = HashMap::new();
            let mut stop = false;
            let mut to_emit: Vec<pb::Inbound> = Vec::new();
            tokio::select! {
                frame = frames.next() => {
                    match frame {
                        Some(Ok(pb::SubscribeFrame { frame: Some(pb::subscribe_frame::Frame::Ack(ack)) })) => {
                            if auto_commit {
                                commit_current_position = true;
                            } else {
                                match ack_tracker.record_ack(ack) {
                                    Ok(Some(commit)) => commit.insert_into(&mut explicit_commit_offsets),
                                    Ok(None) => {}
                                    Err(e) => { yield Err(ConnectError::new_invalid_argument(e.to_string())); stop = true; }
                                }
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => { yield Err(e); stop = true; }
                        None => stop = true,
                    }
                }
                batch = session.poll(std::time::Duration::from_millis(500)) => {
                    match batch {
                        Ok(records) => {
                            #[cfg(feature = "arrow")]
                            let filtered_batch = match filter_polled_records(&filter, records) {
                                Ok(batch) => batch,
                                Err(e) => {
                                    yield Err(ConnectError::new_invalid_argument(e.to_string()));
                                    stop = true;
                                    FilteredPollBatch { decisions: Vec::new() }
                                }
                            };
                            #[cfg(not(feature = "arrow"))]
                            let filtered_batch = filter_polled_records(&filter, records);
                            for decision in filtered_batch.decisions {
                                match decision {
                                    FilteredRecordDecision::Deliver(r) => {
                                        if auto_commit {
                                            pending_auto_commit_offsets.record(&r);
                                        } else {
                                            ack_tracker.record_delivery(&r);
                                        }
                                        to_emit.push(inbound_from_decoded_record(r));
                                    }
                                    FilteredRecordDecision::Filter(r) => {
                                        if auto_commit {
                                            pending_auto_commit_offsets.record(&r);
                                        } else {
                                            match ack_tracker.record_filtered_delivery(&r) {
                                                Ok(Some(commit)) => commit.insert_into(&mut explicit_commit_offsets),
                                                Ok(None) => {}
                                                Err(e) => { yield Err(ConnectError::new_invalid_argument(e.to_string())); stop = true; }
                                            }
                                            if stop { break; }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => { yield Err(ConnectError::new_internal(e.to_string())); stop = true; }
                    }
                }
            }
            for msg in to_emit {
                yield Ok(msg);
            }
            if !explicit_commit_offsets.is_empty() {
                if let Err(e) = session.commit_offsets(explicit_commit_offsets.clone()).await {
                    yield Err(ConnectError::new_internal(e.to_string()));
                    break;
                }
                ack_tracker.mark_offsets_committed(&explicit_commit_offsets);
            }
            if commit_current_position {
                if let Err(e) = session.commit().await {
                    yield Err(ConnectError::new_internal(e.to_string()));
                    break;
                }
            } else if let Some(auto_commit_offsets) = pending_auto_commit_offsets.offsets_to_commit() {
                if let Err(e) = session.commit_offsets(auto_commit_offsets.clone()).await {
                    yield Err(ConnectError::new_internal(e.to_string()));
                    break;
                }
                pending_auto_commit_offsets.mark_committed(&auto_commit_offsets);
            }
            if stop { break; }
        }
    }
}

/// Bidi `Subscribe` Connect handler.
pub async fn subscribe(
    Extension(state): Extension<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    peer: Option<Extension<SocketAddr>>,
    req: ConnectRequest<Streaming<pb::SubscribeFrame>>,
) -> Result<
    ConnectResponse<
        StreamBody<Pin<Box<dyn Stream<Item = Result<pb::Inbound, ConnectError>> + Send>>>,
    >,
    ConnectError,
> {
    let eff = principal.map_or_else(anonymous_principal, |Extension(p)| p);
    let host = peer.map_or_else(unknown_host, |Extension(a)| a);
    Ok(ConnectResponse::new(StreamBody::new(Box::pin(
        subscribe_inner(req.0, state, eff, host),
    ))))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use crabka_client_consumer::Header;

    use super::*;
    use crate::codec::{SchemaFormat, SchemaMeta};

    fn delivered_record(
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> crate::consume::DecodedConsumerRecord {
        crate::consume::DecodedConsumerRecord {
            topic: topic.to_string(),
            partition: crate::ids::PartitionIndex(partition),
            offset: crate::ids::Offset(offset),
            timestamp: crate::ids::Timestamp(0),
            key: None,
            raw_value: Bytes::new(),
            value: Bytes::new(),
            headers: Vec::new(),
            schema: None,
            json: None,
        }
    }

    #[cfg(feature = "arrow")]
    fn schema_json_record(
        offset: i64,
        schema_id: i32,
        raw_value: &'static [u8],
        json: &serde_json::Value,
    ) -> crate::consume::DecodedConsumerRecord {
        crate::consume::DecodedConsumerRecord {
            topic: "orders".to_string(),
            partition: crate::ids::PartitionIndex(0),
            offset: crate::ids::Offset(offset),
            timestamp: crate::ids::Timestamp(0),
            key: None,
            raw_value: Bytes::from_static(raw_value),
            value: Bytes::from_static(b"decoded-payload"),
            headers: Vec::new(),
            schema: Some(SchemaMeta {
                subject: "orders-value".to_string(),
                id: schema_id,
                format: SchemaFormat::Protobuf,
            }),
            json: Some(Bytes::from(json.to_string())),
        }
    }

    fn ack(topic: &str, partition: i32, offset: i64) -> pb::SubscribeAck {
        pb::SubscribeAck {
            topic: topic.to_string(),
            partition,
            offset,
        }
    }

    #[test]
    fn pending_auto_commit_offsets_clear_after_successful_commit() {
        let mut pending = PendingAutoCommitOffsets::default();
        pending.record(&delivered_record("topic", 0, 0));

        let committed_offsets = pending
            .offsets_to_commit()
            .expect("first non-empty poll creates auto-commit offsets");
        assert_eq!(
            committed_offsets,
            HashMap::from([(("topic".to_string(), 0), 1)])
        );

        pending.mark_committed(&committed_offsets);

        assert_eq!(
            pending.offsets_to_commit(),
            None,
            "a following empty poll must not repeat the successful auto-commit"
        );
    }

    #[test]
    fn pending_auto_commit_offsets_record_fully_filtered_non_empty_poll() {
        let mut pending = PendingAutoCommitOffsets::default();
        pending.record(&delivered_record("topic", 0, 0));
        pending.record(&delivered_record("topic", 0, 1));

        assert_eq!(
            pending
                .offsets_to_commit()
                .expect("fully filtered non-empty poll creates auto-commit offsets"),
            HashMap::from([(("topic".to_string(), 0), 2)])
        );
    }

    #[test]
    fn subscribe_ack_tracker_commits_ack_plus_one() {
        let mut tracker = SubscribeAckTracker::default();
        tracker.record_delivery(&delivered_record("topic", 0, 42));

        let commit = tracker
            .record_ack(ack("topic", 0, 42))
            .expect("ack accepted")
            .expect("frontier advanced");

        assert_eq!(
            commit,
            ExplicitCommitOffset {
                topic: "topic".to_string(),
                partition: 0,
                next_offset: 43,
            }
        );
    }

    #[test]
    fn subscribe_ack_tracker_out_of_order_ack_does_not_skip_gap() {
        let mut tracker = SubscribeAckTracker::default();
        tracker.record_delivery(&delivered_record("topic", 0, 0));
        tracker.record_delivery(&delivered_record("topic", 0, 1));

        let commit = tracker
            .record_ack(ack("topic", 0, 1))
            .expect("ack accepted");

        assert_eq!(commit, None);
    }

    #[test]
    fn subscribe_ack_tracker_filtered_later_offset_waits_for_delivered_gap() {
        let mut tracker = SubscribeAckTracker::default();
        tracker.record_delivery(&delivered_record("topic", 0, 0));

        let filtered_commit = tracker
            .record_filtered_delivery(&delivered_record("topic", 0, 1))
            .expect("filtered delivery accepted");
        assert_eq!(filtered_commit, None);

        let commit = tracker
            .record_ack(ack("topic", 0, 0))
            .expect("delivered ack accepted")
            .expect("frontier includes previously filtered offset");
        assert_eq!(commit.next_offset, 2);
    }

    #[test]
    fn subscribe_ack_tracker_commits_sparse_fully_filtered_poll() {
        let mut tracker = SubscribeAckTracker::default();

        let first_commit = tracker
            .record_filtered_delivery(&delivered_record("topic", 0, 0))
            .expect("first filtered offset accepted")
            .expect("first filtered offset advances");
        assert_eq!(first_commit.next_offset, 1);
        let sparse_commit = tracker
            .record_filtered_delivery(&delivered_record("topic", 0, 2))
            .expect("sparse filtered offset accepted")
            .expect("sparse observed offset skips unobserved gap");

        assert_eq!(sparse_commit.next_offset, 3);
    }

    #[test]
    fn subscribe_ack_tracker_sparse_filtered_offset_waits_for_earlier_delivered_ack() {
        let mut tracker = SubscribeAckTracker::default();
        tracker.record_delivery(&delivered_record("topic", 0, 0));

        let filtered_commit = tracker
            .record_filtered_delivery(&delivered_record("topic", 0, 2))
            .expect("sparse filtered offset accepted");
        assert_eq!(filtered_commit, None);

        let commit = tracker
            .record_ack(ack("topic", 0, 0))
            .expect("earlier delivered ack accepted")
            .expect("commit skips only unobserved offset gap");
        assert_eq!(commit.next_offset, 3);
    }

    #[test]
    fn subscribe_ack_tracker_rejects_future_ack_without_later_commit() {
        let mut tracker = SubscribeAckTracker::default();
        tracker.record_delivery(&delivered_record("topic", 0, 0));

        let future_ack = tracker
            .record_ack(ack("topic", 0, 2))
            .expect_err("future ack rejected");
        assert_eq!(
            future_ack,
            SubscribeAckError::UndeliveredOffset {
                topic: "topic".to_string(),
                partition: 0,
                offset: 2,
            }
        );

        let first_commit = tracker
            .record_ack(ack("topic", 0, 0))
            .expect("delivered ack accepted")
            .expect("frontier advanced to first delivered offset");
        assert_eq!(first_commit.next_offset, 1);
        tracker.mark_committed(&first_commit);

        tracker.record_delivery(&delivered_record("topic", 0, 1));
        let second_commit = tracker
            .record_ack(ack("topic", 0, 1))
            .expect("intervening delivered ack accepted")
            .expect("frontier advanced only to intervening offset");
        assert_eq!(second_commit.next_offset, 2);
    }

    #[test]
    fn subscribe_ack_tracker_rejects_future_ack_after_filtered_delivery() {
        let mut tracker = SubscribeAckTracker::default();
        let first_commit = tracker
            .record_filtered_delivery(&delivered_record("topic", 0, 0))
            .expect("filtered delivery accepted")
            .expect("filtered delivery advances frontier");
        assert_eq!(first_commit.next_offset, 1);
        tracker.mark_committed(&first_commit);

        let future_ack = tracker
            .record_ack(ack("topic", 0, 2))
            .expect_err("future ack rejected");
        assert_eq!(
            future_ack,
            SubscribeAckError::UndeliveredOffset {
                topic: "topic".to_string(),
                partition: 0,
                offset: 2,
            }
        );

        let second_commit = tracker
            .record_filtered_delivery(&delivered_record("topic", 0, 1))
            .expect("intervening filtered delivery accepted")
            .expect("frontier advanced only to intervening offset");
        assert_eq!(second_commit.next_offset, 2);
    }

    #[test]
    fn subscribe_ack_tracker_duplicate_ack_does_not_repeat_commit() {
        let mut tracker = SubscribeAckTracker::default();
        tracker.record_delivery(&delivered_record("topic", 0, 0));

        let commit = tracker
            .record_ack(ack("topic", 0, 0))
            .expect("ack accepted")
            .expect("frontier advanced");
        tracker.mark_committed(&commit);

        let duplicate = tracker
            .record_ack(ack("topic", 0, 0))
            .expect("duplicate ack accepted");

        assert_eq!(duplicate, None);
    }

    #[test]
    fn explicit_commit_offsets_accumulate_multiple_advanced_partitions() {
        let mut tracker = SubscribeAckTracker::default();
        let mut offsets = HashMap::new();

        tracker
            .record_filtered_delivery(&delivered_record("topic", 0, 0))
            .expect("partition 0 filtered delivery accepted")
            .expect("partition 0 frontier advanced")
            .insert_into(&mut offsets);
        tracker
            .record_filtered_delivery(&delivered_record("topic", 1, 0))
            .expect("partition 1 filtered delivery accepted")
            .expect("partition 1 frontier advanced")
            .insert_into(&mut offsets);

        assert_eq!(
            offsets,
            HashMap::from([(("topic".to_string(), 0), 1), (("topic".to_string(), 1), 1)])
        );

        tracker.mark_offsets_committed(&offsets);

        assert_eq!(
            tracker
                .record_ack(ack("topic", 0, 0))
                .expect("duplicate ack accepted"),
            None
        );
        assert_eq!(
            tracker
                .record_ack(ack("topic", 1, 0))
                .expect("duplicate ack accepted"),
            None
        );
    }

    #[test]
    fn inbound_carries_structured_json_and_schema_metadata() {
        let record = crate::consume::DecodedConsumerRecord {
            topic: "metadata".to_string(),
            partition: crate::ids::PartitionIndex(2),
            offset: crate::ids::Offset(9),
            timestamp: crate::ids::Timestamp(1234),
            key: Some(Bytes::from_static(b"k")),
            raw_value: Bytes::from_static(b"\0\0\0\0\x11\x08\x07"),
            value: Bytes::from_static(b"\x08\x07"),
            headers: Vec::new(),
            schema: Some(SchemaMeta {
                subject: "metadata-value".to_string(),
                id: 17,
                format: SchemaFormat::Protobuf,
            }),
            json: Some(Bytes::from_static(br#"{"entity_type":"NETWORK_NODE"}"#)),
        };

        let inbound = inbound_from_decoded_record(record);

        assert_eq!(inbound.topic, "metadata");
        assert_eq!(inbound.partition, 2);
        assert_eq!(inbound.offset, 9);
        assert_eq!(inbound.key.as_deref(), Some(&b"k"[..]));
        assert_eq!(inbound.value, b"\0\0\0\0\x11\x08\x07");
        assert_eq!(
            inbound.structured.expect("structured JSON").json,
            br#"{"entity_type":"NETWORK_NODE"}"#
        );
        let schema = inbound.schema.expect("schema metadata");
        assert_eq!(schema.subject, "metadata-value");
        assert_eq!(schema.id, 17);
        assert_eq!(schema.format, pb::SchemaFormat::Protobuf as i32);
    }

    #[test]
    fn inbound_preserves_header_order_duplicates_and_null_values() {
        let record = crate::consume::DecodedConsumerRecord {
            topic: "metadata".to_string(),
            partition: crate::ids::PartitionIndex(2),
            offset: crate::ids::Offset(9),
            timestamp: crate::ids::Timestamp(1234),
            key: None,
            raw_value: Bytes::from_static(b"value"),
            value: Bytes::from_static(b"value"),
            headers: vec![
                Header {
                    key: "duplicate".to_string(),
                    value: Some(Bytes::from_static(b"first")),
                },
                Header {
                    key: "null".to_string(),
                    value: None,
                },
                Header {
                    key: "duplicate".to_string(),
                    value: Some(Bytes::from_static(b"last")),
                },
            ],
            schema: None,
            json: None,
        };

        let inbound = inbound_from_decoded_record(record);

        assert_eq!(
            inbound.headers,
            vec![
                pb::Header {
                    key: "duplicate".to_string(),
                    value: Some(b"first".to_vec()),
                },
                pb::Header {
                    key: "null".to_string(),
                    value: None,
                },
                pb::Header {
                    key: "duplicate".to_string(),
                    value: Some(b"last".to_vec()),
                },
            ]
        );
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn filtered_arrow_ipc_delivery_preserves_original_record_bytes() {
        use std::sync::Arc;

        use arrow::{
            array::{Float64Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
            ipc::writer::StreamWriter,
        };

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("status", DataType::Utf8, false),
                Field::new("price", DataType::Float64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["PENDING", "PAID"])),
                Arc::new(Float64Array::from(vec![50.0, 125.0])),
            ],
        )
        .expect("record batch builds");
        let mut decoded_arrow_ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut decoded_arrow_ipc, &batch.schema())
                .expect("Arrow IPC writer builds");
            writer.write(&batch).expect("Arrow IPC batch writes");
            writer.finish().expect("Arrow IPC stream finishes");
        }
        let record = crate::consume::DecodedConsumerRecord {
            topic: "orders".to_string(),
            partition: crate::ids::PartitionIndex(0),
            offset: crate::ids::Offset(7),
            timestamp: crate::ids::Timestamp(1234),
            key: None,
            raw_value: Bytes::from_static(b"original-kafka-wire-bytes"),
            value: Bytes::from(decoded_arrow_ipc),
            headers: Vec::new(),
            schema: None,
            json: None,
        };
        let filter =
            CompiledFilter::compile("status = 'PAID' AND price > 100").expect("filter compiles");

        let filtered =
            filter_polled_records(&filter, vec![record]).expect("filter evaluates Arrow IPC batch");

        assert_eq!(filtered.filtered().count(), 0);
        let delivered = filtered
            .delivered()
            .next()
            .expect("record delivered")
            .clone();
        assert_eq!(
            inbound_from_decoded_record(delivered).value,
            b"original-kafka-wire-bytes"
        );
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn filtered_schema_registry_row_bridge_supports_nested_repeated_enum_and_raw_delivery() {
        let matching = schema_json_record(
            0,
            17,
            b"original-confluent-frame-0",
            &serde_json::json!({
                "customer": {"status": "ACTIVE"},
                "items": [{"price": 125}],
                "status": "PAID"
            }),
        );
        let non_matching = schema_json_record(
            1,
            17,
            b"original-confluent-frame-1",
            &serde_json::json!({
                "customer": {"status": "ACTIVE"},
                "items": [{"price": 25}],
                "status": "PAID"
            }),
        );
        let filter = CompiledFilter::compile(
            "customer.status = 'ACTIVE' AND items[0].price > 100 AND status = 'PAID'",
        )
        .expect("filter compiles");

        let filtered = filter_polled_records(&filter, vec![matching, non_matching])
            .expect("schema-registry row bridge filters records");

        assert_eq!(filtered.delivered().count(), 1);
        assert_eq!(filtered.filtered().count(), 1);
        assert_eq!(
            inbound_from_decoded_record(
                filtered
                    .delivered()
                    .next()
                    .expect("record delivered")
                    .clone(),
            )
            .value,
            b"original-confluent-frame-0"
        );
        assert_eq!(
            filtered.filtered().next().expect("record filtered").offset,
            crate::ids::Offset(1)
        );
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn filtered_schema_registry_row_bridge_empty_filter_matches_empty_column_rows() {
        let empty_object = schema_json_record(0, 17, b"empty-object", &serde_json::json!({}));
        let empty_arrays = schema_json_record(
            2,
            17,
            b"empty-arrays",
            &serde_json::json!({"items": [], "tags": []}),
        );
        let filter = CompiledFilter::compile("").expect("empty filter compiles");

        let filtered = filter_polled_records(&filter, vec![empty_object, empty_arrays])
            .expect("empty-column row bridge keeps row count");

        let delivered = filtered.delivered().collect::<Vec<_>>();
        assert_eq!(delivered.len(), 2);
        assert_eq!(filtered.filtered().count(), 0);
        assert_eq!(delivered[0].offset, crate::ids::Offset(0));
        assert_eq!(delivered[1].offset, crate::ids::Offset(2));
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn filtered_schema_registry_row_bridge_missing_field_filter_fails_loudly() {
        let empty_object = schema_json_record(0, 17, b"empty-object", &serde_json::json!({}));
        let filter = CompiledFilter::compile("status = 'PAID'").expect("filter compiles");

        let error = filter_polled_records(&filter, vec![empty_object])
            .expect_err("missing filter field should not silently drop records");

        assert!(matches!(
            error,
            crate::filter::FilterCompileError::DataFusion(_)
        ));
        assert!(error.to_string().contains("status"));
    }

    #[test]
    #[cfg(feature = "arrow")]
    fn filtered_schema_registry_row_bridge_recompiles_across_schema_ids() {
        let schema_v1 = schema_json_record(
            0,
            21,
            b"schema-21",
            &serde_json::json!({"status": "PAID", "total": 125}),
        );
        let schema_v2 = schema_json_record(
            1,
            22,
            b"schema-22",
            &serde_json::json!({"status": "PAID", "total": 125.5, "region": "eu"}),
        );
        let filter =
            CompiledFilter::compile("status = 'PAID' AND total > 100").expect("filter compiles");

        let filtered = filter_polled_records(&filter, vec![schema_v1, schema_v2])
            .expect("schema-id evolution recompiles filter");

        let delivered = filtered.delivered().collect::<Vec<_>>();
        assert_eq!(delivered.len(), 2);
        assert_eq!(filtered.filtered().count(), 0);
        assert_eq!(delivered[0].raw_value, Bytes::from_static(b"schema-21"));
        assert_eq!(delivered[1].raw_value, Bytes::from_static(b"schema-22"));
    }
}
