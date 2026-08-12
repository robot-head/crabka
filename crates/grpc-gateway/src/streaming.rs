//! Streaming Connect handlers: the bidirectional `SendStream` for produce and
//! `Subscribe` for consume.
//!
//! The per-handler logic lives in a `*_inner` function that returns a plain
//! `Stream`, which a unit test can drive. The public handler is a thin wrapper
//! into `ConnectResponse::new(StreamBody::new(inner))`.

use std::{net::SocketAddr, pin::Pin, sync::Arc};

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
    consume::ConsumeSession,
    filter::CompiledFilter,
    handlers::{
        anonymous_principal, authorize_resource, producer_acks_from_pb, to_gateway_record,
        unknown_host,
    },
    pb,
    state::AppState,
};

type TopicPartition = (String, i32);
type DeliveredOffsets = std::collections::HashMap<TopicPartition, std::collections::BTreeSet<i64>>;

fn validate_ack(
    delivered: &DeliveredOffsets,
    acknowledged: &std::collections::HashMap<TopicPartition, i64>,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<bool, String> {
    let key = (topic.to_string(), partition);
    if acknowledged
        .get(&key)
        .is_some_and(|previous| offset <= *previous)
    {
        return Ok(false);
    }
    if offset < 0 || !delivered.get(&key).is_some_and(|set| set.contains(&offset)) {
        return Err(format!(
            "Ack targets a record that was not delivered: {topic}-{partition}@{offset}"
        ));
    }
    Ok(true)
}

fn mark_acknowledged(
    delivered: &mut DeliveredOffsets,
    acknowledged: &mut std::collections::HashMap<TopicPartition, i64>,
    topic: &str,
    partition: i32,
    frontier: i64,
) {
    let key = (topic.to_string(), partition);
    acknowledged.insert(key.clone(), frontier);
    if let Some(offsets) = delivered.get_mut(&key) {
        offsets.retain(|delivered_offset| *delivered_offset > frontier);
        if offsets.is_empty() {
            delivered.remove(&key);
        }
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
        let decision = filter
            .evaluate_decoded_record(structured_json_or_unframed_value(&record), &record.value);
        if decision.should_deliver() {
            decisions.push(FilteredRecordDecision::Deliver(record));
        } else {
            decisions.push(FilteredRecordDecision::Filter(record));
        }
    }

    FilteredPollBatch { decisions }
}

fn structured_json_or_unframed_value(
    record: &crate::consume::DecodedConsumerRecord,
) -> Option<&bytes::Bytes> {
    record.json.as_ref().or_else(|| {
        serde_json::from_slice::<serde_json::Value>(&record.value)
            .is_ok()
            .then_some(&record.value)
    })
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
    if filter.matches_structured_json(structured_json_or_unframed_value(&record)) {
        decisions.push(FilteredRecordDecision::Deliver(record));
    } else {
        decisions.push(FilteredRecordDecision::Filter(record));
    }
}

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

/// Produce every record in each inbound `SendRequest`, and emit one `SendAck`
/// per request. Each `SendAck` carries a per-record `RecordResult` vector.
///
/// A Write ACL on the target topic gates each record for the on-behalf-of
/// `principal` and `host`. A denied record is skipped and reported as a
/// non-retriable `PERMISSION_DENIED`.
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
            let Some(acks) = producer_acks_from_pb(send_req.acks) else {
                yield Err(ConnectError::new_invalid_argument(format!(
                    "unknown acknowledgement mode {}",
                    send_req.acks
                )));
                break;
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
                let result = match state.produce.produce_with_acks(rec, &principal, acks).await {
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
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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

/// Join a consumer group on the caller's behalf and stream records.
///
/// The first frame MUST be `Start`. Each later `Ack` frame drives an offset
/// commit, which gives at-least-once. The subscription ends when the control
/// stream closes or errors.
///
/// In explicit mode, an `Ack` advances only the named partition's contiguous
/// acknowledgement frontier, and the session commits `frontier + 1`. An ack
/// above a gap remains pending and cannot skip an unacknowledged record. In
/// auto-commit mode, Ack frames are ignored and each non-empty poll commits the
/// consumer's whole current position at enqueue.
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
        let mut session = match ConsumeSession::new_with_policy(&state.config.bootstrap, &start.group_id, &client_id, start.topics, state.config.broker_security.clone(), state.codec.clone(), &state.config.runtime).await {
            Ok(s) => s,
            Err(e) => { yield Err(ConnectError::new_internal(e.to_string())); return; }
        };
        let auto_commit = start.auto_commit;
        let mut delivered_offsets = DeliveredOffsets::new();
        let mut acknowledged_offsets = std::collections::HashMap::<(String, i32), i64>::new();

        loop {
            // BORROW NOTE: do NOT call session.commit() inside a select! arm —
            // session.poll(..) holds a &mut borrow across the select. Instead set
            // flags inside the select and commit AFTER it resolves.
            let mut commit = false;
            let mut client_ack: Option<(String, i32, i64)> = None;
            let mut deliveries: Vec<(String, i32, i64)> = Vec::new();
            let mut filtered_acks: Vec<(String, i32, i64)> = Vec::new();
            let mut frame_error: Option<ConnectError> = None;
            let mut stop = false;
            let mut to_emit: Vec<pb::Inbound> = Vec::new();
            tokio::select! {
                frame = frames.next() => {
                    match frame {
                        Some(Ok(pb::SubscribeFrame { frame: Some(pb::subscribe_frame::Frame::Ack(ack)) })) => {
                            if !auto_commit {
                                match validate_ack(
                                    &delivered_offsets,
                                    &acknowledged_offsets,
                                    &ack.topic,
                                    ack.partition,
                                    ack.offset,
                                ) {
                                    Ok(true) => client_ack = Some((ack.topic, ack.partition, ack.offset)),
                                    Ok(false) => {}
                                    Err(message) => {
                                        frame_error = Some(ConnectError::new_invalid_argument(message));
                                        stop = true;
                                    }
                                }
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => { yield Err(e); stop = true; }
                        None => stop = true,
                    }
                }
                batch = session.poll(state.config.runtime.consumer_poll_timeout) => {
                    match batch {
                        Ok(records) => {
                            let polled_records = !records.is_empty();
                            #[cfg(feature = "arrow")]
                            let filtered_batch = match filter_polled_records(&filter, records) {
                                Ok(batch) => batch,
                                Err(e) => {
                                    frame_error = Some(ConnectError::new_invalid_argument(e.to_string()));
                                    stop = true;
                                    FilteredPollBatch { decisions: Vec::new() }
                                }
                            };
                            #[cfg(not(feature = "arrow"))]
                            let filtered_batch = filter_polled_records(&filter, records);
                            for decision in filtered_batch.decisions {
                                let (r, deliver) = match decision {
                                    FilteredRecordDecision::Deliver(record) => (record, true),
                                    FilteredRecordDecision::Filter(record) => (record, false),
                                };
                                if !auto_commit {
                                    deliveries.push((r.topic.clone(), r.partition.0, r.offset.0));
                                }
                                if !deliver {
                                    if !auto_commit {
                                        filtered_acks.push((r.topic, r.partition.0, r.offset.0));
                                    }
                                    continue;
                                }
                                if !auto_commit {
                                    delivered_offsets
                                        .entry((r.topic.clone(), r.partition.0))
                                        .or_default()
                                        .insert(r.offset.0);
                                }
                                to_emit.push(inbound_from_decoded_record(r));
                            }
                            if polled_records && auto_commit { commit = true; }
                        }
                        Err(e) => { yield Err(ConnectError::new_internal(e.to_string())); stop = true; }
                    }
                }
            }
            for msg in to_emit {
                yield Ok(msg);
            }
            if let Some(error) = frame_error {
                yield Err(error);
                break;
            }

            let mut explicit_commit = false;
            if !auto_commit {
                for (topic, partition, offset) in deliveries {
                    session.record_delivery(&topic, partition, offset);
                }
                for (topic, partition, offset) in filtered_acks
                    .into_iter()
                    .chain(client_ack)
                {
                    if let Err(e) = session.record_ack(&topic, partition, offset) {
                        yield Err(ConnectError::new_resource_exhausted(e.to_string()));
                        return;
                    }
                    if let Some(frontier) = session.ack_frontier(&topic, partition) {
                        mark_acknowledged(
                            &mut delivered_offsets,
                            &mut acknowledged_offsets,
                            &topic,
                            partition,
                            frontier,
                        );
                    }
                    explicit_commit = true;
                }
            }

            if commit || explicit_commit {
                let result = if auto_commit {
                    session.commit().await
                } else {
                    session.commit_acked().await
                };
                if let Err(e) = result {
                    yield Err(ConnectError::new_internal(e.to_string()));
                    break;
                }
            }
            if stop { break; }
        }
    }
}

/// Bidi `Subscribe` Connect handler.
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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

    use super::*;
    use crate::codec::{CodecError, RecordCodec, SchemaFormat, SchemaMeta};

    const FILTER_PROTO_SCHEMA: &str = r#"
        syntax = "proto3";
        message Order {
          Status status = 1;
          repeated Item items = 2;
          Customer customer = 3;
        }
        enum Status { STATUS_UNSPECIFIED = 0; PAID = 1; CANCELLED = 2; }
        message Item { int64 price = 1; }
        message Customer { string status = 1; }
    "#;

    const FILTER_AVRO_SCHEMA: &str = r#"
        {
          "type":"record","name":"Order","fields":[
            {"name":"status","type":{"type":"enum","name":"Status","symbols":["STATUS_UNSPECIFIED","PAID","CANCELLED"]}},
            {"name":"items","type":{"type":"array","items":{"type":"record","name":"Item","fields":[{"name":"price","type":"long"}]}}},
            {"name":"customer","type":{"type":"record","name":"Customer","fields":[{"name":"status","type":"string"}]}}
          ]
        }
    "#;

    const EVOLUTION_AVRO_V1: &str = r#"
        {"type":"record","name":"Order","fields":[
          {"name":"status","type":"string"},
          {"name":"total","type":"long"}
        ]}
    "#;

    const EVOLUTION_AVRO_V2: &str = r#"
        {"type":"record","name":"Order","fields":[
          {"name":"status","type":"string"},
          {"name":"total","type":"double"},
          {"name":"region","type":"string","default":"unknown"}
        ]}
    "#;

    #[derive(Debug)]
    struct FilterSchemaResolver {
        id: i32,
        schema: &'static str,
        format: SchemaFormat,
    }

    #[async_trait::async_trait]
    impl crate::schema::codec::SchemaResolver for FilterSchemaResolver {
        async fn by_id(&self, id: i32) -> Result<(String, SchemaFormat), CodecError> {
            if id != self.id {
                return Err(CodecError::Registry(format!("schema id {id} not found")));
            }
            Ok((self.schema.to_string(), self.format))
        }

        async fn latest(&self, _subject: &str) -> Result<(i32, String, SchemaFormat), CodecError> {
            Ok((self.id, self.schema.to_string(), self.format))
        }
    }

    async fn schema_registry_record(
        offset: i64,
        schema_id: i32,
        format: SchemaFormat,
        schema: &'static str,
        json: &serde_json::Value,
    ) -> crate::consume::DecodedConsumerRecord {
        let payload = crate::schema::format::serialize(format, schema, json.to_string().as_bytes())
            .expect("test schema value serializes");
        let raw_value = crate::schema::wire::encode_frame(schema_id, format, &payload);
        let codec = crate::schema::codec::SchemaRegistryCodec::with_resolver(
            Arc::new(FilterSchemaResolver {
                id: schema_id,
                schema,
                format,
            }),
            false,
        );
        let decoded = codec
            .decode("orders", raw_value.clone())
            .await
            .expect("test schema value decodes through registry codec");
        crate::consume::DecodedConsumerRecord {
            topic: "orders".to_string(),
            partition: crate::ids::PartitionIndex(0),
            offset: crate::ids::Offset(offset),
            timestamp: crate::ids::Timestamp(0),
            key: None,
            raw_value,
            value: decoded.value,
            headers: Vec::new(),
            schema: decoded.schema,
            json: decoded.json,
        }
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
            headers: vec![
                crabka_client_consumer::Header {
                    key: "ce_type".to_string(),
                    value: Some(Bytes::from_static(b"order.created")),
                },
                crabka_client_consumer::Header {
                    key: "duplicate".to_string(),
                    value: Some(Bytes::from_static(b"first")),
                },
                crabka_client_consumer::Header {
                    key: "duplicate".to_string(),
                    value: Some(Bytes::from_static(b"last")),
                },
                crabka_client_consumer::Header {
                    key: "null_value".to_string(),
                    value: None,
                },
            ],
            schema: Some(SchemaMeta {
                subject: "metadata-value".to_string(),
                id: 17,
                format: SchemaFormat::Protobuf,
            }),
            json: Some(Bytes::from_static(br#"{"entity_type":"NETWORK_NODE"}"#)),
        };

        let inbound = inbound_from_decoded_record(record);

        assert2::assert!(
            inbound
                == pb::Inbound {
                    topic: "metadata".to_string(),
                    partition: 2,
                    offset: 9,
                    key: Some(b"k".to_vec()),
                    value: b"\0\0\0\0\x11\x08\x07".to_vec(),
                    headers: vec![
                        pb::Header {
                            key: "ce_type".to_string(),
                            value: Some(b"order.created".to_vec()),
                        },
                        pb::Header {
                            key: "duplicate".to_string(),
                            value: Some(b"first".to_vec()),
                        },
                        pb::Header {
                            key: "duplicate".to_string(),
                            value: Some(b"last".to_vec()),
                        },
                        pb::Header {
                            key: "null_value".to_string(),
                            value: None,
                        },
                    ],
                    timestamp_ms: 1234,
                    structured: Some(pb::StructuredValue {
                        json: br#"{"entity_type":"NETWORK_NODE"}"#.to_vec(),
                    }),
                    schema: Some(pb::SchemaSelector {
                        subject: "metadata-value".to_string(),
                        id: 17,
                        format: pb::SchemaFormat::Protobuf as i32,
                    }),
                }
        );
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn filtered_arrow_ipc_delivery_preserves_original_record_bytes() {
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
        let delivered = filtered
            .delivered()
            .next()
            .expect("record delivered")
            .clone();

        assert2::assert!(filtered.filtered().count() == 0);
        assert2::assert!(
            inbound_from_decoded_record(delivered).value == b"original-kafka-wire-bytes"
        );
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn raw_codec_unframed_json_filters_server_side_and_opaque_bytes_drop() {
        fn raw_record(offset: i64, value: &'static [u8]) -> crate::consume::DecodedConsumerRecord {
            crate::consume::DecodedConsumerRecord {
                topic: "raw-orders".to_string(),
                partition: crate::ids::PartitionIndex(0),
                offset: crate::ids::Offset(offset),
                timestamp: crate::ids::Timestamp(0),
                key: None,
                raw_value: Bytes::from_static(value),
                value: Bytes::from_static(value),
                headers: Vec::new(),
                schema: None,
                json: None,
            }
        }

        let filter = CompiledFilter::compile("kind = 'keep'").expect("filter compiles");
        let keep_bytes = Bytes::from_static(br#"{"kind":"keep"}"#);
        let filtered = filter_polled_records(
            &filter,
            vec![
                raw_record(0, br#"{"kind":"skip"}"#),
                raw_record(1, br#"{"kind":"keep"}"#),
                raw_record(2, b"opaque-not-json"),
            ],
        )
        .expect("raw JSON fallback filters");
        let delivered = filtered
            .delivered()
            .next()
            .expect("matching raw JSON record delivered")
            .clone();
        let dropped_offsets = filtered
            .filtered()
            .map(|record| record.offset)
            .collect::<Vec<_>>();

        assert2::assert!(filtered.delivered().count() == 1);
        assert2::assert!(dropped_offsets == vec![crate::ids::Offset(0), crate::ids::Offset(2)]);
        assert2::assert!(inbound_from_decoded_record(delivered).value == keep_bytes);
    }

    #[cfg(feature = "arrow")]
    #[tokio::test]
    async fn protobuf_nested_repeated_enum_filter_is_byte_exact() {
        let matching = schema_registry_record(
            0,
            17,
            SchemaFormat::Protobuf,
            FILTER_PROTO_SCHEMA,
            &serde_json::json!({
                "customer": {"status": "ACTIVE"},
                "items": [{"price": 125}],
                "status": "PAID"
            }),
        )
        .await;
        let expected_bytes = matching.raw_value.clone();
        let non_matching = schema_registry_record(
            1,
            17,
            SchemaFormat::Protobuf,
            FILTER_PROTO_SCHEMA,
            &serde_json::json!({
                "customer": {"status": "ACTIVE"},
                "items": [{"price": 25}],
                "status": "PAID"
            }),
        )
        .await;
        let filter = CompiledFilter::compile(
            "customer.status = 'ACTIVE' AND items[0].price > 100 AND status = 'PAID'",
        )
        .expect("filter compiles");

        let filtered = filter_polled_records(&filter, vec![matching, non_matching])
            .expect("schema-registry rows filter");
        let delivered = filtered
            .delivered()
            .next()
            .expect("record delivered")
            .clone();

        assert2::assert!(filtered.delivered().count() == 1);
        assert2::assert!(filtered.filtered().count() == 1);
        assert2::assert!(inbound_from_decoded_record(delivered).value == expected_bytes);
    }

    #[cfg(feature = "arrow")]
    #[tokio::test]
    async fn avro_and_protobuf_filters_have_semantic_parity() {
        let protobuf_match = schema_registry_record(
            0,
            31,
            SchemaFormat::Protobuf,
            FILTER_PROTO_SCHEMA,
            &serde_json::json!({
                "status": "PAID",
                "items": [{"price": 125}],
                "customer": {"status": "ACTIVE"}
            }),
        )
        .await;
        let protobuf_match_bytes = protobuf_match.raw_value.clone();
        let protobuf_drop = schema_registry_record(
            1,
            31,
            SchemaFormat::Protobuf,
            FILTER_PROTO_SCHEMA,
            &serde_json::json!({
                "status": "PAID",
                "items": [{"price": 25}],
                "customer": {"status": "ACTIVE"}
            }),
        )
        .await;
        let avro_match = schema_registry_record(
            2,
            41,
            SchemaFormat::Avro,
            FILTER_AVRO_SCHEMA,
            &serde_json::json!({
                "status": "PAID",
                "items": [{"price": 125}],
                "customer": {"status": "ACTIVE"}
            }),
        )
        .await;
        let avro_match_bytes = avro_match.raw_value.clone();
        let avro_drop = schema_registry_record(
            3,
            41,
            SchemaFormat::Avro,
            FILTER_AVRO_SCHEMA,
            &serde_json::json!({
                "status": "PAID",
                "items": [{"price": 25}],
                "customer": {"status": "ACTIVE"}
            }),
        )
        .await;
        let filter = CompiledFilter::compile("status = 'PAID' AND items[0].price > 100")
            .expect("filter compiles");

        let filtered = filter_polled_records(
            &filter,
            vec![protobuf_match, protobuf_drop, avro_match, avro_drop],
        )
        .expect("both formats filter");
        let delivered = filtered
            .delivered()
            .map(|record| record.raw_value.clone())
            .collect::<Vec<_>>();

        assert2::assert!(delivered == vec![protobuf_match_bytes, avro_match_bytes]);
        assert2::assert!(filtered.filtered().count() == 2);
    }

    #[cfg(feature = "arrow")]
    #[tokio::test]
    async fn schema_evolution_recompiles_and_preserves_original_bytes() {
        let schema_v1 = schema_registry_record(
            0,
            21,
            SchemaFormat::Avro,
            EVOLUTION_AVRO_V1,
            &serde_json::json!({"status": "PAID", "total": 125}),
        )
        .await;
        let schema_v1_bytes = schema_v1.raw_value.clone();
        let schema_v2 = schema_registry_record(
            1,
            22,
            SchemaFormat::Avro,
            EVOLUTION_AVRO_V2,
            &serde_json::json!({"status": "PAID", "total": 125.5, "region": "eu"}),
        )
        .await;
        let schema_v2_bytes = schema_v2.raw_value.clone();
        let filter =
            CompiledFilter::compile("status = 'PAID' AND total > 100").expect("filter compiles");

        let filtered = filter_polled_records(&filter, vec![schema_v1, schema_v2])
            .expect("schema-id evolution recompiles filter");
        let delivered = filtered
            .delivered()
            .map(|record| record.raw_value.clone())
            .collect::<Vec<_>>();

        assert2::assert!(delivered == vec![schema_v1_bytes, schema_v2_bytes]);
        assert2::assert!(filtered.filtered().count() == 0);
    }

    #[test]
    fn subscribe_ack_requires_an_exact_delivered_offset() {
        let delivered = DeliveredOffsets::from([(
            ("topic".to_string(), 2),
            std::collections::BTreeSet::from([10, 12]),
        )]);
        let acknowledged = std::collections::HashMap::new();

        assert2::assert!(validate_ack(&delivered, &acknowledged, "topic", 2, 10) == Ok(true));
        assert2::assert!(validate_ack(&delivered, &acknowledged, "topic", 2, 11).is_err());
        assert2::assert!(validate_ack(&delivered, &acknowledged, "other", 2, 10).is_err());
        assert2::assert!(validate_ack(&delivered, &acknowledged, "topic", 2, -1).is_err());
    }

    #[test]
    fn acknowledged_frontier_preserves_out_of_order_deliveries() {
        let mut delivered = DeliveredOffsets::from([(
            ("topic".to_string(), 2),
            std::collections::BTreeSet::from([10, 11, 12]),
        )]);
        let mut acknowledged = std::collections::HashMap::new();

        mark_acknowledged(&mut delivered, &mut acknowledged, "topic", 2, 10);

        assert2::assert!(acknowledged.get(&("topic".to_string(), 2)) == Some(&10));
        assert2::assert!(
            delivered.get(&("topic".to_string(), 2))
                == Some(&std::collections::BTreeSet::from([11, 12]))
        );
        assert2::assert!(validate_ack(&delivered, &acknowledged, "topic", 2, 10) == Ok(false));
        assert2::assert!(validate_ack(&delivered, &acknowledged, "topic", 2, 11) == Ok(true));
        assert2::assert!(validate_ack(&delivered, &acknowledged, "topic", 2, 12) == Ok(true));
    }
}
