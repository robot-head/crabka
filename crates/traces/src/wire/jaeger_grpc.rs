//! Jaeger API v2 gRPC decoding.

use prost_types::{Duration, Timestamp};

use crate::{
    span::{AttrValue, KeyValue, Span},
    wire::{
        WireError,
        jaeger::{JaegerBatch, JaegerLog, JaegerProcess, JaegerRef, JaegerSpan, spans_from_batch},
    },
};

#[allow(clippy::all, clippy::pedantic, clippy::nursery)]
pub mod api_v2 {
    tonic::include_proto!("jaeger.api_v2");
}

pub fn decode_jaeger_grpc_batch(batch: api_v2::Batch) -> Result<Vec<Span>, WireError> {
    let batch_process = batch
        .process
        .as_ref()
        .map(process_from_proto)
        .unwrap_or_default();
    let mut spans = Vec::with_capacity(batch.spans.len());
    for span in batch.spans {
        let process = span
            .process
            .as_ref()
            .map_or_else(|| batch_process.clone(), process_from_proto);
        spans.extend(spans_from_batch(&JaegerBatch {
            process,
            spans: vec![span_from_proto(span)?],
        }));
    }
    Ok(spans)
}

fn process_from_proto(process: &api_v2::Process) -> JaegerProcess {
    JaegerProcess {
        service_name: process.service_name.clone(),
        tags: process.tags.iter().map(key_value_from_proto).collect(),
    }
}

fn span_from_proto(span: api_v2::Span) -> Result<JaegerSpan, WireError> {
    let (trace_id_high, trace_id_low) = trace_id_parts(&span.trace_id)?;
    let span_id = span_id_part(&span.span_id)?;
    Ok(JaegerSpan {
        trace_id_low,
        trace_id_high,
        span_id,
        parent_span_id: 0,
        operation_name: span.operation_name,
        references: span
            .references
            .iter()
            .map(ref_from_proto)
            .collect::<Result<Vec<_>, _>>()?,
        start_time_micros: timestamp_micros(span.start_time.as_ref()),
        duration_micros: duration_micros(span.duration.as_ref()),
        tags: span.tags.iter().map(key_value_from_proto).collect(),
        logs: span.logs.iter().map(log_from_proto).collect(),
    })
}

fn ref_from_proto(reference: &api_v2::SpanRef) -> Result<JaegerRef, WireError> {
    let (trace_id_high, trace_id_low) = trace_id_parts(&reference.trace_id)?;
    Ok(JaegerRef {
        ref_type: reference.ref_type,
        trace_id_low,
        trace_id_high,
        span_id: span_id_part(&reference.span_id)?,
    })
}

fn log_from_proto(log: &api_v2::Log) -> JaegerLog {
    JaegerLog {
        timestamp_micros: timestamp_micros(log.timestamp.as_ref()),
        fields: log.fields.iter().map(key_value_from_proto).collect(),
    }
}

fn key_value_from_proto(kv: &api_v2::KeyValue) -> KeyValue {
    let value_type = api_v2::ValueType::try_from(kv.v_type).unwrap_or(api_v2::ValueType::String);
    let value = match value_type {
        api_v2::ValueType::String => AttrValue::Str(kv.v_str.clone()),
        api_v2::ValueType::Bool => AttrValue::Bool(kv.v_bool),
        api_v2::ValueType::Int64 => AttrValue::Int(kv.v_int64),
        api_v2::ValueType::Float64 => AttrValue::Double(kv.v_float64),
        api_v2::ValueType::Binary => AttrValue::Bytes(kv.v_binary.clone()),
    };
    KeyValue {
        key: kv.key.clone(),
        value,
    }
}

fn trace_id_parts(bytes: &[u8]) -> Result<(i64, i64), WireError> {
    if bytes.len() != 16 {
        return Err(WireError::Decode("jaeger trace_id must be 16 bytes".into()));
    }
    let high = i64::from_be_bytes(bytes[0..8].try_into().expect("slice length checked"));
    let low = i64::from_be_bytes(bytes[8..16].try_into().expect("slice length checked"));
    Ok((high, low))
}

fn span_id_part(bytes: &[u8]) -> Result<i64, WireError> {
    if bytes.len() != 8 {
        return Err(WireError::Decode("jaeger span_id must be 8 bytes".into()));
    }
    Ok(i64::from_be_bytes(
        bytes[0..8].try_into().expect("slice length checked"),
    ))
}

fn timestamp_micros(timestamp: Option<&Timestamp>) -> i64 {
    timestamp.map_or(0, |ts| {
        ts.seconds
            .saturating_mul(1_000_000)
            .saturating_add(i64::from(ts.nanos) / 1_000)
    })
}

fn duration_micros(duration: Option<&Duration>) -> i64 {
    duration.map_or(0, |duration| {
        duration
            .seconds
            .saturating_mul(1_000_000)
            .saturating_add(i64::from(duration.nanos) / 1_000)
    })
}
