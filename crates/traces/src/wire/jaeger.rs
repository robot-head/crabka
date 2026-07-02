//! Jaeger push-door decoding.

use crate::{
    span::{AttrValue, KeyValue, LinkRecord, Span, SpanKind, StatusCode},
    wire::WireError,
};

const T_STOP: u8 = 0;
const T_BOOL_TRUE: u8 = 1;
const T_BOOL_FALSE: u8 = 2;
const T_BYTE: u8 = 3;
const T_I16: u8 = 4;
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_DOUBLE: u8 = 7;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_SET: u8 = 10;
const T_MAP: u8 = 11;
const T_STRUCT: u8 = 12;

/// Decode a Jaeger compact-Thrift HTTP `Batch` body into internal spans.
pub fn decode_jaeger_thrift(body: &[u8]) -> Result<Vec<Span>, WireError> {
    let mut input = CompactInput::new(body);
    let batch = read_batch(&mut input)?;
    Ok(spans_from_batch(&batch))
}

/// Decode a Jaeger binary-Thrift HTTP `Batch` body into internal spans.
pub fn decode_jaeger_binary_thrift(body: &[u8]) -> Result<Vec<Span>, WireError> {
    let mut input = BinaryInput::new(body);
    let batch = read_binary_batch(&mut input)?;
    Ok(spans_from_batch(&batch))
}

pub(super) fn spans_from_batch(batch: &JaegerBatch) -> Vec<Span> {
    batch
        .spans
        .iter()
        .map(|span| jaeger_span_to_internal(span, &batch.process))
        .collect()
}

#[derive(Clone, Default)]
pub(super) struct JaegerBatch {
    pub(super) process: JaegerProcess,
    pub(super) spans: Vec<JaegerSpan>,
}

#[derive(Clone, Default)]
pub(super) struct JaegerProcess {
    pub(super) service_name: String,
    pub(super) tags: Vec<KeyValue>,
}

#[derive(Clone, Default)]
pub(super) struct JaegerSpan {
    pub(super) trace_id_low: i64,
    pub(super) trace_id_high: i64,
    pub(super) span_id: i64,
    pub(super) parent_span_id: i64,
    pub(super) operation_name: String,
    pub(super) references: Vec<JaegerRef>,
    pub(super) start_time_micros: i64,
    pub(super) duration_micros: i64,
    pub(super) tags: Vec<KeyValue>,
    pub(super) logs: Vec<JaegerLog>,
}

#[derive(Clone, Default)]
pub(super) struct JaegerLog {
    pub(super) timestamp_micros: i64,
    pub(super) fields: Vec<KeyValue>,
}

#[derive(Clone, Default)]
pub(super) struct JaegerRef {
    pub(super) ref_type: i32,
    pub(super) trace_id_low: i64,
    pub(super) trace_id_high: i64,
    pub(super) span_id: i64,
}

fn read_batch(input: &mut CompactInput<'_>) -> Result<JaegerBatch, WireError> {
    let mut out = JaegerBatch::default();
    let mut last = 0;
    while let Some((field_type, field_id)) = input.read_field(&mut last)? {
        match (field_id, field_type) {
            (1, T_STRUCT) => out.process = read_process(input)?,
            (2, T_LIST) => out.spans = input.read_struct_list(read_span)?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}

fn read_process(input: &mut CompactInput<'_>) -> Result<JaegerProcess, WireError> {
    let mut out = JaegerProcess::default();
    let mut last = 0;
    while let Some((field_type, field_id)) = input.read_field(&mut last)? {
        match (field_id, field_type) {
            (1, T_BINARY) => out.service_name = input.read_string()?,
            (2, T_LIST) => out.tags = input.read_struct_list(read_key_value)?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}

fn read_span(input: &mut CompactInput<'_>) -> Result<JaegerSpan, WireError> {
    let mut out = JaegerSpan::default();
    let mut last = 0;
    while let Some((field_type, field_id)) = input.read_field(&mut last)? {
        match (field_id, field_type) {
            (1, T_I64) => out.trace_id_low = input.read_i64()?,
            (2, T_I64) => out.trace_id_high = input.read_i64()?,
            (3, T_I64) => out.span_id = input.read_i64()?,
            (4, T_I64) => out.parent_span_id = input.read_i64()?,
            (5, T_BINARY) => out.operation_name = input.read_string()?,
            (6, T_LIST) => out.references = input.read_struct_list(read_ref)?,
            (8, T_I64) => out.start_time_micros = input.read_i64()?,
            (9, T_I64) => out.duration_micros = input.read_i64()?,
            (10, T_LIST) => out.tags = input.read_struct_list(read_key_value)?,
            (11, T_LIST) => out.logs = input.read_struct_list(read_log)?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}

fn read_log(input: &mut CompactInput<'_>) -> Result<JaegerLog, WireError> {
    let mut out = JaegerLog::default();
    let mut last = 0;
    while let Some((field_type, field_id)) = input.read_field(&mut last)? {
        match (field_id, field_type) {
            (1, T_I64) => out.timestamp_micros = input.read_i64()?,
            (2, T_LIST) => out.fields = input.read_struct_list(read_key_value)?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}

fn read_ref(input: &mut CompactInput<'_>) -> Result<JaegerRef, WireError> {
    let mut out = JaegerRef::default();
    let mut last = 0;
    while let Some((field_type, field_id)) = input.read_field(&mut last)? {
        match (field_id, field_type) {
            (1, T_I32) => out.ref_type = input.read_i32()?,
            (2, T_I64) => out.trace_id_low = input.read_i64()?,
            (3, T_I64) => out.trace_id_high = input.read_i64()?,
            (4, T_I64) => out.span_id = input.read_i64()?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}

fn read_key_value(input: &mut CompactInput<'_>) -> Result<KeyValue, WireError> {
    let mut key = String::new();
    let mut value = None;
    let mut last = 0;
    while let Some((field_type, field_id)) = input.read_field(&mut last)? {
        match (field_id, field_type) {
            (1, T_BINARY) => key = input.read_string()?,
            (3, T_BINARY) => value = Some(AttrValue::Str(input.read_string()?)),
            (4, T_DOUBLE) => value = Some(AttrValue::Double(input.read_double()?)),
            (5, T_BOOL_TRUE) => value = Some(AttrValue::Bool(true)),
            (5, T_BOOL_FALSE) => value = Some(AttrValue::Bool(false)),
            (6, T_I64) => value = Some(AttrValue::Int(input.read_i64()?)),
            (7, T_BINARY) => value = Some(AttrValue::Bytes(input.read_binary()?)),
            _ => input.skip(field_type)?,
        }
    }
    Ok(KeyValue {
        key,
        value: value.unwrap_or_else(|| AttrValue::Str(String::new())),
    })
}

fn jaeger_span_to_internal(span: &JaegerSpan, process: &JaegerProcess) -> Span {
    let mut resource_attrs = process.tags.clone();
    resource_attrs.push(KeyValue {
        key: "service.name".into(),
        value: AttrValue::Str(process.service_name.clone()),
    });
    let parent_span_id = span
        .references
        .iter()
        .find(|reference| reference.ref_type == 0)
        .map(|reference| i64_bytes(reference.span_id))
        .or_else(|| (span.parent_span_id != 0).then(|| i64_bytes(span.parent_span_id)));
    let links = span
        .references
        .iter()
        .filter(|reference| reference.ref_type != 0)
        .map(|reference| LinkRecord {
            trace_id: trace_id(reference.trace_id_high, reference.trace_id_low),
            span_id: i64_bytes(reference.span_id),
            attrs: vec![KeyValue {
                key: "ref.type".into(),
                value: AttrValue::Str(ref_type_name(reference.ref_type).into()),
            }],
        })
        .collect();
    Span {
        trace_id: trace_id(span.trace_id_high, span.trace_id_low),
        span_id: i64_bytes(span.span_id),
        parent_span_id,
        name: span.operation_name.clone(),
        kind: span_kind(&span.tags),
        start_ns: span.start_time_micros.saturating_mul(1_000),
        duration_ns: span.duration_micros.saturating_mul(1_000),
        status: span_status(&span.tags),
        status_message: String::new(),
        resource_attrs,
        span_attrs: span.tags.clone(),
        events: span_logs_to_events(&span.logs),
        links,
        instrumentation_scope: String::new(),
        instrumentation_version: String::new(),
    }
}

fn span_logs_to_events(logs: &[JaegerLog]) -> Vec<crate::span::EventRecord> {
    logs.iter()
        .map(|log| {
            let name = log
                .fields
                .iter()
                .find_map(|field| {
                    if field.key != "event" {
                        return None;
                    }
                    match &field.value {
                        AttrValue::Str(value) if !value.is_empty() => Some(value.clone()),
                        _ => None,
                    }
                })
                .unwrap_or_else(|| "log".to_string());
            crate::span::EventRecord {
                time_unix_nano: log.timestamp_micros.saturating_mul(1_000),
                name,
                attrs: log.fields.clone(),
            }
        })
        .collect()
}

fn ref_type_name(ref_type: i32) -> &'static str {
    match ref_type {
        0 => "child_of",
        1 => "follows_from",
        _ => "reference",
    }
}

fn trace_id(high: i64, low: i64) -> [u8; 16] {
    let mut out = [0; 16];
    out[..8].copy_from_slice(&high.to_be_bytes());
    out[8..].copy_from_slice(&low.to_be_bytes());
    out
}

fn i64_bytes(value: i64) -> [u8; 8] {
    value.to_be_bytes()
}

fn span_kind(tags: &[KeyValue]) -> SpanKind {
    tags.iter()
        .find_map(|tag| {
            if tag.key != "span.kind" {
                return None;
            }
            match &tag.value {
                AttrValue::Str(value) => match value.as_str() {
                    "server" => Some(SpanKind::Server),
                    "client" => Some(SpanKind::Client),
                    "producer" => Some(SpanKind::Producer),
                    "consumer" => Some(SpanKind::Consumer),
                    "internal" => Some(SpanKind::Internal),
                    _ => None,
                },
                _ => None,
            }
        })
        .unwrap_or(SpanKind::Internal)
}

fn span_status(tags: &[KeyValue]) -> StatusCode {
    if tags.iter().any(|tag| {
        tag.key == "error"
            && match &tag.value {
                AttrValue::Bool(true) => true,
                AttrValue::Str(value) => value == "true",
                AttrValue::Int(_)
                | AttrValue::Double(_)
                | AttrValue::Bool(false)
                | AttrValue::Bytes(_) => false,
            }
    }) {
        StatusCode::Error
    } else {
        StatusCode::Unset
    }
}

struct CompactInput<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> CompactInput<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn read_field(&mut self, last_field_id: &mut i16) -> Result<Option<(u8, i16)>, WireError> {
        let header = self.read_u8()?;
        let field_type = header & 0x0F;
        if field_type == T_STOP {
            return Ok(None);
        }
        let delta = i16::from(header >> 4);
        let field_id = if delta == 0 {
            i16::try_from(self.read_i32()?)
                .map_err(|_| WireError::Decode("field id out of range".into()))?
        } else {
            last_field_id.saturating_add(delta)
        };
        *last_field_id = field_id;
        Ok(Some((field_type, field_id)))
    }

    fn read_struct_list<T>(
        &mut self,
        read_one: fn(&mut CompactInput<'_>) -> Result<T, WireError>,
    ) -> Result<Vec<T>, WireError> {
        let (element_type, len) = self.read_list_header()?;
        if element_type != T_STRUCT {
            return Err(WireError::Decode("expected struct list".into()));
        }
        (0..len).map(|_| read_one(self)).collect()
    }

    fn read_list_header(&mut self) -> Result<(u8, usize), WireError> {
        let header = self.read_u8()?;
        let element_type = header & 0x0F;
        let short_len = usize::from(header >> 4);
        let len = if short_len == 15 {
            usize::try_from(self.read_varint()?)
                .map_err(|_| WireError::Decode("list too large".into()))?
        } else {
            short_len
        };
        Ok((element_type, len))
    }

    fn read_map_header(&mut self) -> Result<(u8, u8, usize), WireError> {
        let len = usize::try_from(self.read_varint()?)
            .map_err(|_| WireError::Decode("map too large".into()))?;
        if len == 0 {
            return Ok((T_STOP, T_STOP, 0));
        }
        let types = self.read_u8()?;
        Ok((types >> 4, types & 0x0F, len))
    }

    fn read_string(&mut self) -> Result<String, WireError> {
        String::from_utf8(self.read_binary()?).map_err(|err| WireError::Decode(err.to_string()))
    }

    fn read_binary(&mut self) -> Result<Vec<u8>, WireError> {
        let len = usize::try_from(self.read_varint()?)
            .map_err(|_| WireError::Decode("binary too large".into()))?;
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| WireError::Decode("binary length overflow".into()))?;
        if end > self.bytes.len() {
            return Err(WireError::Decode("truncated binary".into()));
        }
        let out = self.bytes[self.pos..end].to_vec();
        self.pos = end;
        Ok(out)
    }

    fn read_i32(&mut self) -> Result<i32, WireError> {
        let value = self.read_varint()?;
        let value = u32::try_from(value)
            .map_err(|_| WireError::Decode("i32 varint out of range".into()))?;
        Ok((value >> 1).cast_signed() ^ -((value & 1).cast_signed()))
    }

    fn read_i64(&mut self) -> Result<i64, WireError> {
        let value = self.read_varint()?;
        Ok((value >> 1).cast_signed() ^ -((value & 1).cast_signed()))
    }

    fn read_double(&mut self) -> Result<f64, WireError> {
        let mut bytes = [0; 8];
        for byte in &mut bytes {
            *byte = self.read_u8()?;
        }
        Ok(f64::from_le_bytes(bytes))
    }

    fn read_varint(&mut self) -> Result<u64, WireError> {
        let mut shift = 0;
        let mut out = 0_u64;
        loop {
            let byte = self.read_u8()?;
            out |= u64::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                return Ok(out);
            }
            shift += 7;
            if shift >= 64 {
                return Err(WireError::Decode("varint too long".into()));
            }
        }
    }

    fn read_u8(&mut self) -> Result<u8, WireError> {
        let Some(byte) = self.bytes.get(self.pos).copied() else {
            return Err(WireError::Decode("unexpected end of thrift payload".into()));
        };
        self.pos += 1;
        Ok(byte)
    }

    fn skip(&mut self, field_type: u8) -> Result<(), WireError> {
        match field_type {
            T_STOP | T_BOOL_TRUE | T_BOOL_FALSE => Ok(()),
            T_BYTE => self.read_u8().map(|_| ()),
            T_I16 | T_I32 => self.read_i32().map(|_| ()),
            T_I64 => self.read_i64().map(|_| ()),
            T_DOUBLE => self.read_double().map(|_| ()),
            T_BINARY => self.read_binary().map(|_| ()),
            T_STRUCT => {
                let mut last = 0;
                while let Some((inner_type, _)) = self.read_field(&mut last)? {
                    self.skip(inner_type)?;
                }
                Ok(())
            }
            T_LIST | T_SET => {
                let (element_type, len) = self.read_list_header()?;
                for _ in 0..len {
                    self.skip(element_type)?;
                }
                Ok(())
            }
            T_MAP => {
                let (key_type, value_type, len) = self.read_map_header()?;
                for _ in 0..len {
                    self.skip(key_type)?;
                    self.skip(value_type)?;
                }
                Ok(())
            }
            other => Err(WireError::Decode(format!("unknown thrift type {other}"))),
        }
    }
}

const BT_STOP: u8 = 0;
const BT_BOOL: u8 = 2;
const BT_BYTE: u8 = 3;
const BT_DOUBLE: u8 = 4;
const BT_I16: u8 = 6;
const BT_I32: u8 = 8;
const BT_I64: u8 = 10;
const BT_BINARY: u8 = 11;
const BT_STRUCT: u8 = 12;
const BT_MAP: u8 = 13;
const BT_SET: u8 = 14;
const BT_LIST: u8 = 15;

fn read_binary_batch(input: &mut BinaryInput<'_>) -> Result<JaegerBatch, WireError> {
    let mut out = JaegerBatch::default();
    while let Some((field_type, field_id)) = input.read_field()? {
        match (field_id, field_type) {
            (1, BT_STRUCT) => out.process = read_binary_process(input)?,
            (2, BT_LIST) => out.spans = input.read_struct_list(read_binary_span)?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}

fn read_binary_process(input: &mut BinaryInput<'_>) -> Result<JaegerProcess, WireError> {
    let mut out = JaegerProcess::default();
    while let Some((field_type, field_id)) = input.read_field()? {
        match (field_id, field_type) {
            (1, BT_BINARY) => out.service_name = input.read_string()?,
            (2, BT_LIST) => out.tags = input.read_struct_list(read_binary_key_value)?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}

fn read_binary_span(input: &mut BinaryInput<'_>) -> Result<JaegerSpan, WireError> {
    let mut out = JaegerSpan::default();
    while let Some((field_type, field_id)) = input.read_field()? {
        match (field_id, field_type) {
            (1, BT_I64) => out.trace_id_low = input.read_i64()?,
            (2, BT_I64) => out.trace_id_high = input.read_i64()?,
            (3, BT_I64) => out.span_id = input.read_i64()?,
            (4, BT_I64) => out.parent_span_id = input.read_i64()?,
            (5, BT_BINARY) => out.operation_name = input.read_string()?,
            (6, BT_LIST) => out.references = input.read_struct_list(read_binary_ref)?,
            (8, BT_I64) => out.start_time_micros = input.read_i64()?,
            (9, BT_I64) => out.duration_micros = input.read_i64()?,
            (10, BT_LIST) => out.tags = input.read_struct_list(read_binary_key_value)?,
            (11, BT_LIST) => out.logs = input.read_struct_list(read_binary_log)?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}

fn read_binary_log(input: &mut BinaryInput<'_>) -> Result<JaegerLog, WireError> {
    let mut out = JaegerLog::default();
    while let Some((field_type, field_id)) = input.read_field()? {
        match (field_id, field_type) {
            (1, BT_I64) => out.timestamp_micros = input.read_i64()?,
            (2, BT_LIST) => out.fields = input.read_struct_list(read_binary_key_value)?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}

fn read_binary_ref(input: &mut BinaryInput<'_>) -> Result<JaegerRef, WireError> {
    let mut out = JaegerRef::default();
    while let Some((field_type, field_id)) = input.read_field()? {
        match (field_id, field_type) {
            (1, BT_I32) => out.ref_type = input.read_i32()?,
            (2, BT_I64) => out.trace_id_low = input.read_i64()?,
            (3, BT_I64) => out.trace_id_high = input.read_i64()?,
            (4, BT_I64) => out.span_id = input.read_i64()?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}

fn read_binary_key_value(input: &mut BinaryInput<'_>) -> Result<KeyValue, WireError> {
    let mut key = String::new();
    let mut value = None;
    while let Some((field_type, field_id)) = input.read_field()? {
        match (field_id, field_type) {
            (1, BT_BINARY) => key = input.read_string()?,
            (3, BT_BINARY) => value = Some(AttrValue::Str(input.read_string()?)),
            (4, BT_DOUBLE) => value = Some(AttrValue::Double(input.read_double()?)),
            (5, BT_BOOL) => value = Some(AttrValue::Bool(input.read_bool()?)),
            (6, BT_I64) => value = Some(AttrValue::Int(input.read_i64()?)),
            (7, BT_BINARY) => value = Some(AttrValue::Bytes(input.read_binary()?)),
            _ => input.skip(field_type)?,
        }
    }
    Ok(KeyValue {
        key,
        value: value.unwrap_or_else(|| AttrValue::Str(String::new())),
    })
}

struct BinaryInput<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> BinaryInput<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn read_field(&mut self) -> Result<Option<(u8, i16)>, WireError> {
        let field_type = self.read_u8()?;
        if field_type == BT_STOP {
            return Ok(None);
        }
        Ok(Some((field_type, self.read_i16()?)))
    }

    fn read_struct_list<T>(
        &mut self,
        read_one: fn(&mut BinaryInput<'_>) -> Result<T, WireError>,
    ) -> Result<Vec<T>, WireError> {
        let (element_type, len) = self.read_list_header()?;
        if element_type != BT_STRUCT {
            return Err(WireError::Decode("expected struct list".into()));
        }
        (0..len).map(|_| read_one(self)).collect()
    }

    fn read_list_header(&mut self) -> Result<(u8, usize), WireError> {
        let element_type = self.read_u8()?;
        let len = usize::try_from(self.read_i32()?)
            .map_err(|_| WireError::Decode("list length out of range".into()))?;
        Ok((element_type, len))
    }

    fn read_map_header(&mut self) -> Result<(u8, u8, usize), WireError> {
        let key_type = self.read_u8()?;
        let value_type = self.read_u8()?;
        let len = usize::try_from(self.read_i32()?)
            .map_err(|_| WireError::Decode("map length out of range".into()))?;
        Ok((key_type, value_type, len))
    }

    fn read_string(&mut self) -> Result<String, WireError> {
        String::from_utf8(self.read_binary()?).map_err(|err| WireError::Decode(err.to_string()))
    }

    fn read_binary(&mut self) -> Result<Vec<u8>, WireError> {
        let len = usize::try_from(self.read_i32()?)
            .map_err(|_| WireError::Decode("binary length out of range".into()))?;
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| WireError::Decode("binary length overflow".into()))?;
        if end > self.bytes.len() {
            return Err(WireError::Decode("truncated binary".into()));
        }
        let out = self.bytes[self.pos..end].to_vec();
        self.pos = end;
        Ok(out)
    }

    fn read_bool(&mut self) -> Result<bool, WireError> {
        Ok(self.read_u8()? != 0)
    }

    fn read_i16(&mut self) -> Result<i16, WireError> {
        let mut bytes = [0; 2];
        self.read_exact(&mut bytes)?;
        Ok(i16::from_be_bytes(bytes))
    }

    fn read_i32(&mut self) -> Result<i32, WireError> {
        let mut bytes = [0; 4];
        self.read_exact(&mut bytes)?;
        Ok(i32::from_be_bytes(bytes))
    }

    fn read_i64(&mut self) -> Result<i64, WireError> {
        let mut bytes = [0; 8];
        self.read_exact(&mut bytes)?;
        Ok(i64::from_be_bytes(bytes))
    }

    fn read_double(&mut self) -> Result<f64, WireError> {
        let mut bytes = [0; 8];
        self.read_exact(&mut bytes)?;
        Ok(f64::from_bits(u64::from_be_bytes(bytes)))
    }

    fn read_u8(&mut self) -> Result<u8, WireError> {
        let Some(byte) = self.bytes.get(self.pos).copied() else {
            return Err(WireError::Decode("unexpected end of thrift payload".into()));
        };
        self.pos += 1;
        Ok(byte)
    }

    fn read_exact(&mut self, out: &mut [u8]) -> Result<(), WireError> {
        let end = self
            .pos
            .checked_add(out.len())
            .ok_or_else(|| WireError::Decode("read length overflow".into()))?;
        if end > self.bytes.len() {
            return Err(WireError::Decode("unexpected end of thrift payload".into()));
        }
        out.copy_from_slice(&self.bytes[self.pos..end]);
        self.pos = end;
        Ok(())
    }

    fn skip(&mut self, field_type: u8) -> Result<(), WireError> {
        match field_type {
            BT_STOP => Ok(()),
            BT_BOOL | BT_BYTE => self.read_u8().map(|_| ()),
            BT_I16 => self.read_i16().map(|_| ()),
            BT_I32 => self.read_i32().map(|_| ()),
            BT_I64 => self.read_i64().map(|_| ()),
            BT_DOUBLE => self.read_double().map(|_| ()),
            BT_BINARY => self.read_binary().map(|_| ()),
            BT_STRUCT => {
                while let Some((inner_type, _)) = self.read_field()? {
                    self.skip(inner_type)?;
                }
                Ok(())
            }
            BT_LIST | BT_SET => {
                let (element_type, len) = self.read_list_header()?;
                for _ in 0..len {
                    self.skip(element_type)?;
                }
                Ok(())
            }
            BT_MAP => {
                let (key_type, value_type, len) = self.read_map_header()?;
                for _ in 0..len {
                    self.skip(key_type)?;
                    self.skip(value_type)?;
                }
                Ok(())
            }
            other => Err(WireError::Decode(format!("unknown thrift type {other}"))),
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub(crate) mod test_support {
    pub fn encode_sample_batch() -> Vec<u8> {
        let mut out = Vec::new();
        write_process(&mut out, 1, "checkout");
        write_span_list(&mut out, 2);
        out.push(0);
        out
    }

    fn write_process(out: &mut Vec<u8>, field_id: i16, service: &str) {
        write_field_header(out, 12, field_id, &mut 0);
        let mut last = 0;
        write_string_field(out, 1, service, &mut last);
        write_field_header(out, 9, 2, &mut last);
        write_list_header(out, 12, 1);
        write_key_value_string(out, "process.tag", "present");
        out.push(0);
    }

    fn write_span_list(out: &mut Vec<u8>, field_id: i16) {
        write_field_header(out, 9, field_id, &mut 1);
        write_list_header(out, 12, 1);
        let mut last = 0;
        write_i64_field(out, 1, 2, &mut last);
        write_i64_field(out, 2, 1, &mut last);
        write_i64_field(out, 3, 3, &mut last);
        write_i64_field(out, 4, 0, &mut last);
        write_string_field(out, 5, "GET /", &mut last);
        write_field_header(out, 9, 6, &mut last);
        write_list_header(out, 12, 2);
        write_span_ref(out, 0, 2, 1, 4);
        write_span_ref(out, 1, 5, 6, 7);
        write_i32_field(out, 7, 0, &mut last);
        write_i64_field(out, 8, 1_000, &mut last);
        write_i64_field(out, 9, 25, &mut last);
        write_field_header(out, 9, 10, &mut last);
        write_list_header(out, 12, 3);
        write_key_value_string(out, "span.kind", "server");
        write_key_value_string(out, "http.method", "GET");
        write_key_value_bool(out, "error", true);
        write_field_header(out, 9, 11, &mut last);
        write_list_header(out, 12, 1);
        write_log(out);
        out.push(0);
    }

    fn write_span_ref(out: &mut Vec<u8>, ref_type: i32, low: i64, high: i64, span_id: i64) {
        let mut last = 0;
        write_i32_field(out, 1, ref_type, &mut last);
        write_i64_field(out, 2, low, &mut last);
        write_i64_field(out, 3, high, &mut last);
        write_i64_field(out, 4, span_id, &mut last);
        out.push(0);
    }

    fn write_log(out: &mut Vec<u8>) {
        let mut last = 0;
        write_i64_field(out, 1, 1_005, &mut last);
        write_field_header(out, 9, 2, &mut last);
        write_list_header(out, 12, 2);
        write_key_value_string(out, "event", "cache.miss");
        write_key_value_string(out, "cache.key", "users");
        out.push(0);
    }

    fn write_key_value_string(out: &mut Vec<u8>, key: &str, value: &str) {
        let mut last = 0;
        write_string_field(out, 1, key, &mut last);
        write_i32_field(out, 2, 0, &mut last);
        write_string_field(out, 3, value, &mut last);
        out.push(0);
    }

    fn write_key_value_bool(out: &mut Vec<u8>, key: &str, value: bool) {
        let mut last = 0;
        write_string_field(out, 1, key, &mut last);
        write_i32_field(out, 2, 3, &mut last);
        write_bool_field(out, 5, value, &mut last);
        out.push(0);
    }

    fn write_i32_field(out: &mut Vec<u8>, id: i16, value: i32, last: &mut i16) {
        write_field_header(out, 5, id, last);
        write_varint(out, zigzag_i32(value));
    }

    fn write_i64_field(out: &mut Vec<u8>, id: i16, value: i64, last: &mut i16) {
        write_field_header(out, 6, id, last);
        write_varint(out, zigzag_i64(value));
    }

    fn write_string_field(out: &mut Vec<u8>, id: i16, value: &str, last: &mut i16) {
        write_field_header(out, 8, id, last);
        write_varint(out, u64::try_from(value.len()).unwrap());
        out.extend_from_slice(value.as_bytes());
    }

    fn write_bool_field(out: &mut Vec<u8>, id: i16, value: bool, last: &mut i16) {
        write_field_header(out, if value { 1 } else { 2 }, id, last);
    }

    fn write_field_header(out: &mut Vec<u8>, type_id: u8, id: i16, last: &mut i16) {
        let delta = id - *last;
        if (1..=15).contains(&delta) {
            out.push((u8::try_from(delta).unwrap() << 4) | type_id);
        } else {
            out.push(type_id);
            write_varint(out, zigzag_i32(i32::from(id)));
        }
        *last = id;
    }

    fn write_list_header(out: &mut Vec<u8>, element_type: u8, size: usize) {
        if size < 15 {
            out.push((u8::try_from(size).unwrap() << 4) | element_type);
        } else {
            out.push(0xF0 | element_type);
            write_varint(out, u64::try_from(size).unwrap());
        }
    }

    fn write_varint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push((value as u8) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
    }

    fn zigzag_i32(value: i32) -> u64 {
        ((value << 1) ^ (value >> 31)) as u32 as u64
    }

    fn zigzag_i64(value: i64) -> u64 {
        ((value << 1) ^ (value >> 63)) as u64
    }
}

#[cfg(test)]
#[allow(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::span::{AttrValue, EventRecord, SpanKind, StatusCode};

    #[test]
    fn decodes_jaeger_thrift_batch() {
        let spans = decode_jaeger_thrift(&encode_sample_batch()).unwrap();

        assert_eq!(
            spans,
            vec![Span {
                trace_id: [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2],
                span_id: [0, 0, 0, 0, 0, 0, 0, 3],
                parent_span_id: Some([0, 0, 0, 0, 0, 0, 0, 4]),
                name: "GET /".into(),
                kind: SpanKind::Server,
                start_ns: 1_000_000,
                duration_ns: 25_000,
                status: StatusCode::Error,
                status_message: String::new(),
                resource_attrs: vec![
                    KeyValue {
                        key: "process.tag".into(),
                        value: AttrValue::Str("present".into()),
                    },
                    KeyValue {
                        key: "service.name".into(),
                        value: AttrValue::Str("checkout".into()),
                    },
                ],
                span_attrs: vec![
                    KeyValue {
                        key: "span.kind".into(),
                        value: AttrValue::Str("server".into()),
                    },
                    KeyValue {
                        key: "http.method".into(),
                        value: AttrValue::Str("GET".into()),
                    },
                    KeyValue {
                        key: "error".into(),
                        value: AttrValue::Bool(true),
                    },
                ],
                events: vec![EventRecord {
                    time_unix_nano: 1_005_000,
                    name: "cache.miss".into(),
                    attrs: vec![
                        KeyValue {
                            key: "event".into(),
                            value: AttrValue::Str("cache.miss".into()),
                        },
                        KeyValue {
                            key: "cache.key".into(),
                            value: AttrValue::Str("users".into()),
                        },
                    ],
                }],
                links: vec![LinkRecord {
                    trace_id: [0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 5],
                    span_id: [0, 0, 0, 0, 0, 0, 0, 7],
                    attrs: vec![KeyValue {
                        key: "ref.type".into(),
                        value: AttrValue::Str("follows_from".into()),
                    }],
                }],
                instrumentation_scope: String::new(),
                instrumentation_version: String::new(),
            }]
        );
    }

    #[test]
    fn decodes_jaeger_binary_thrift_batch() {
        let spans = decode_jaeger_binary_thrift(&encode_binary_sample_batch()).unwrap();

        assert_eq!(
            spans,
            vec![Span {
                trace_id: [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2],
                span_id: [0, 0, 0, 0, 0, 0, 0, 3],
                parent_span_id: None,
                name: "GET /binary".into(),
                kind: SpanKind::Server,
                start_ns: 1_000_000,
                duration_ns: 25_000,
                status: StatusCode::Error,
                status_message: String::new(),
                resource_attrs: vec![
                    KeyValue {
                        key: "process.tag".into(),
                        value: AttrValue::Str("present".into()),
                    },
                    KeyValue {
                        key: "service.name".into(),
                        value: AttrValue::Str("checkout".into()),
                    },
                ],
                span_attrs: vec![
                    KeyValue {
                        key: "span.kind".into(),
                        value: AttrValue::Str("server".into()),
                    },
                    KeyValue {
                        key: "http.method".into(),
                        value: AttrValue::Str("GET".into()),
                    },
                    KeyValue {
                        key: "error".into(),
                        value: AttrValue::Bool(true),
                    },
                ],
                events: Vec::new(),
                links: Vec::new(),
                instrumentation_scope: String::new(),
                instrumentation_version: String::new(),
            }]
        );
    }

    #[test]
    fn compact_thrift_skips_unknown_map_fields() {
        let spans = decode_jaeger_thrift(&encode_sample_batch_with_unknown_map()).unwrap();

        assert!(spans.len() == 1);
        assert!(spans[0].name == "GET /");
    }

    #[test]
    fn binary_thrift_skips_unknown_map_fields() {
        let spans =
            decode_jaeger_binary_thrift(&encode_binary_sample_batch_with_unknown_map()).unwrap();

        assert!(spans.len() == 1);
        assert!(spans[0].name == "GET /binary");
    }

    fn encode_binary_sample_batch() -> Vec<u8> {
        const T_STOP: u8 = 0;
        const T_BOOL: u8 = 2;
        const T_I32: u8 = 8;
        const T_I64: u8 = 10;
        const T_BINARY: u8 = 11;
        const T_STRUCT: u8 = 12;
        const T_LIST: u8 = 15;

        fn field(out: &mut Vec<u8>, type_: u8, id: i16) {
            out.push(type_);
            out.extend_from_slice(&id.to_be_bytes());
        }
        fn string(out: &mut Vec<u8>, value: &str) {
            out.extend_from_slice(&i32::try_from(value.len()).unwrap().to_be_bytes());
            out.extend_from_slice(value.as_bytes());
        }
        fn string_field(out: &mut Vec<u8>, id: i16, value: &str) {
            field(out, T_BINARY, id);
            string(out, value);
        }
        fn i32_field(out: &mut Vec<u8>, id: i16, value: i32) {
            field(out, T_I32, id);
            out.extend_from_slice(&value.to_be_bytes());
        }
        fn i64_field(out: &mut Vec<u8>, id: i16, value: i64) {
            field(out, T_I64, id);
            out.extend_from_slice(&value.to_be_bytes());
        }
        fn bool_field(out: &mut Vec<u8>, id: i16, value: bool) {
            field(out, T_BOOL, id);
            out.push(u8::from(value));
        }
        fn key_value_string(out: &mut Vec<u8>, key: &str, value: &str) {
            string_field(out, 1, key);
            i32_field(out, 2, 0);
            string_field(out, 3, value);
            out.push(T_STOP);
        }
        fn key_value_bool(out: &mut Vec<u8>, key: &str, value: bool) {
            string_field(out, 1, key);
            i32_field(out, 2, 3);
            bool_field(out, 5, value);
            out.push(T_STOP);
        }

        let mut out = Vec::new();
        field(&mut out, T_STRUCT, 1);
        string_field(&mut out, 1, "checkout");
        field(&mut out, T_LIST, 2);
        out.push(T_STRUCT);
        out.extend_from_slice(&1_i32.to_be_bytes());
        key_value_string(&mut out, "process.tag", "present");
        out.push(T_STOP);

        field(&mut out, T_LIST, 2);
        out.push(T_STRUCT);
        out.extend_from_slice(&1_i32.to_be_bytes());
        i64_field(&mut out, 1, 2);
        i64_field(&mut out, 2, 1);
        i64_field(&mut out, 3, 3);
        i64_field(&mut out, 4, 0);
        string_field(&mut out, 5, "GET /binary");
        i64_field(&mut out, 8, 1_000);
        i64_field(&mut out, 9, 25);
        field(&mut out, T_LIST, 10);
        out.push(T_STRUCT);
        out.extend_from_slice(&3_i32.to_be_bytes());
        key_value_string(&mut out, "span.kind", "server");
        key_value_string(&mut out, "http.method", "GET");
        key_value_bool(&mut out, "error", true);
        out.push(T_STOP);
        out.push(T_STOP);
        out
    }

    fn encode_binary_sample_batch_with_unknown_map() -> Vec<u8> {
        const T_MAP: u8 = 13;
        const T_BINARY: u8 = 11;
        const T_I32: u8 = 8;

        let mut out = encode_binary_sample_batch();
        out.pop();
        out.push(T_MAP);
        out.extend_from_slice(&3_i16.to_be_bytes());
        out.push(T_BINARY);
        out.push(T_I32);
        out.extend_from_slice(&1_i32.to_be_bytes());
        out.extend_from_slice(&7_i32.to_be_bytes());
        out.extend_from_slice(b"ignored");
        out.extend_from_slice(&42_i32.to_be_bytes());
        out.push(0);
        out
    }

    fn encode_sample_batch() -> Vec<u8> {
        let mut out = Vec::new();
        write_process(&mut out, 1, "checkout");
        write_span_list(&mut out, 2);
        out.push(0);
        out
    }

    fn encode_sample_batch_with_unknown_map() -> Vec<u8> {
        let mut out = Vec::new();
        write_process(&mut out, 1, "checkout");
        write_span_list(&mut out, 2);
        let mut last = 2;
        write_field_header(&mut out, 11, 3, &mut last);
        write_map_header(&mut out, 8, 5, 1);
        write_varint(&mut out, 7);
        out.extend_from_slice(b"ignored");
        write_varint(&mut out, zigzag_i32(42));
        out.push(0);
        out
    }

    fn write_process(out: &mut Vec<u8>, field_id: i16, service: &str) {
        write_field_header(out, 12, field_id, &mut 0);
        let mut last = 0;
        write_string_field(out, 1, service, &mut last);
        write_field_header(out, 9, 2, &mut last);
        write_list_header(out, 12, 1);
        write_key_value_string(out, "process.tag", "present");
        out.push(0);
    }

    fn write_span_list(out: &mut Vec<u8>, field_id: i16) {
        write_field_header(out, 9, field_id, &mut 1);
        write_list_header(out, 12, 1);
        let mut last = 0;
        write_i64_field(out, 1, 2, &mut last);
        write_i64_field(out, 2, 1, &mut last);
        write_i64_field(out, 3, 3, &mut last);
        write_i64_field(out, 4, 0, &mut last);
        write_string_field(out, 5, "GET /", &mut last);
        write_field_header(out, 9, 6, &mut last);
        write_list_header(out, 12, 2);
        write_span_ref(out, 0, 2, 1, 4);
        write_span_ref(out, 1, 5, 6, 7);
        write_i32_field(out, 7, 0, &mut last);
        write_i64_field(out, 8, 1_000, &mut last);
        write_i64_field(out, 9, 25, &mut last);
        write_field_header(out, 9, 10, &mut last);
        write_list_header(out, 12, 3);
        write_key_value_string(out, "span.kind", "server");
        write_key_value_string(out, "http.method", "GET");
        write_key_value_bool(out, "error", true);
        write_field_header(out, 9, 11, &mut last);
        write_list_header(out, 12, 1);
        write_log(out);
        out.push(0);
    }

    fn write_span_ref(out: &mut Vec<u8>, ref_type: i32, low: i64, high: i64, span_id: i64) {
        let mut last = 0;
        write_i32_field(out, 1, ref_type, &mut last);
        write_i64_field(out, 2, low, &mut last);
        write_i64_field(out, 3, high, &mut last);
        write_i64_field(out, 4, span_id, &mut last);
        out.push(0);
    }

    fn write_log(out: &mut Vec<u8>) {
        let mut last = 0;
        write_i64_field(out, 1, 1_005, &mut last);
        write_field_header(out, 9, 2, &mut last);
        write_list_header(out, 12, 2);
        write_key_value_string(out, "event", "cache.miss");
        write_key_value_string(out, "cache.key", "users");
        out.push(0);
    }

    fn write_key_value_string(out: &mut Vec<u8>, key: &str, value: &str) {
        let mut last = 0;
        write_string_field(out, 1, key, &mut last);
        write_i32_field(out, 2, 0, &mut last);
        write_string_field(out, 3, value, &mut last);
        out.push(0);
    }

    fn write_key_value_bool(out: &mut Vec<u8>, key: &str, value: bool) {
        let mut last = 0;
        write_string_field(out, 1, key, &mut last);
        write_i32_field(out, 2, 3, &mut last);
        write_bool_field(out, 5, value, &mut last);
        out.push(0);
    }

    fn write_i32_field(out: &mut Vec<u8>, id: i16, value: i32, last: &mut i16) {
        write_field_header(out, 5, id, last);
        write_varint(out, zigzag_i32(value));
    }

    fn write_i64_field(out: &mut Vec<u8>, id: i16, value: i64, last: &mut i16) {
        write_field_header(out, 6, id, last);
        write_varint(out, zigzag_i64(value));
    }

    fn write_string_field(out: &mut Vec<u8>, id: i16, value: &str, last: &mut i16) {
        write_field_header(out, 8, id, last);
        write_varint(out, u64::try_from(value.len()).unwrap());
        out.extend_from_slice(value.as_bytes());
    }

    fn write_bool_field(out: &mut Vec<u8>, id: i16, value: bool, last: &mut i16) {
        write_field_header(out, if value { 1 } else { 2 }, id, last);
    }

    fn write_field_header(out: &mut Vec<u8>, type_id: u8, id: i16, last: &mut i16) {
        let delta = id - *last;
        if (1..=15).contains(&delta) {
            out.push((u8::try_from(delta).unwrap() << 4) | type_id);
        } else {
            out.push(type_id);
            write_varint(out, zigzag_i32(i32::from(id)));
        }
        *last = id;
    }

    fn write_list_header(out: &mut Vec<u8>, element_type: u8, size: usize) {
        if size < 15 {
            out.push((u8::try_from(size).unwrap() << 4) | element_type);
        } else {
            out.push(0xF0 | element_type);
            write_varint(out, u64::try_from(size).unwrap());
        }
    }

    fn write_map_header(out: &mut Vec<u8>, key_type: u8, value_type: u8, size: usize) {
        if size == 0 {
            out.push(0);
        } else {
            write_varint(out, u64::try_from(size).unwrap());
            out.push((key_type << 4) | value_type);
        }
    }

    fn write_varint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push((value as u8) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
    }

    fn zigzag_i32(value: i32) -> u64 {
        ((value << 1) ^ (value >> 31)) as u32 as u64
    }

    fn zigzag_i64(value: i64) -> u64 {
        ((value << 1) ^ (value >> 63)) as u64
    }
}
