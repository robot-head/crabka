//! Jaeger push-door decoding.

use crate::span::{AttrValue, KeyValue, Span, SpanKind, StatusCode};
use crate::wire::WireError;

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
    Ok(batch
        .spans
        .iter()
        .map(|span| jaeger_span_to_internal(span, &batch.process))
        .collect())
}

#[derive(Default)]
struct JaegerBatch {
    process: JaegerProcess,
    spans: Vec<JaegerSpan>,
}

#[derive(Default)]
struct JaegerProcess {
    service_name: String,
    tags: Vec<KeyValue>,
}

#[derive(Default)]
struct JaegerSpan {
    trace_id_low: i64,
    trace_id_high: i64,
    span_id: i64,
    parent_span_id: i64,
    operation_name: String,
    references: Vec<JaegerRef>,
    start_time_micros: i64,
    duration_micros: i64,
    tags: Vec<KeyValue>,
}

#[derive(Default)]
struct JaegerRef {
    ref_type: i32,
    trace_id_low: i64,
    trace_id_high: i64,
    span_id: i64,
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
        events: Vec::new(),
        links: Vec::new(),
        instrumentation_scope: String::new(),
        instrumentation_version: String::new(),
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
            T_MAP => Err(WireError::Decode("map skip unsupported".into())),
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
        write_list_header(out, 12, 1);
        write_span_ref(out, 0, 2, 1, 4);
        write_i32_field(out, 7, 0, &mut last);
        write_i64_field(out, 8, 1_000, &mut last);
        write_i64_field(out, 9, 25, &mut last);
        write_field_header(out, 9, 10, &mut last);
        write_list_header(out, 12, 3);
        write_key_value_string(out, "span.kind", "server");
        write_key_value_string(out, "http.method", "GET");
        write_key_value_bool(out, "error", true);
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
    use crate::span::{AttrValue, SpanKind, StatusCode};

    #[test]
    fn decodes_jaeger_thrift_batch() {
        let spans = decode_jaeger_thrift(&encode_sample_batch()).unwrap();

        assert!(spans.len() == 1);
        let span = &spans[0];
        assert!(span.trace_id == [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2]);
        assert!(span.span_id == [0, 0, 0, 0, 0, 0, 0, 3]);
        assert!(span.parent_span_id == Some([0, 0, 0, 0, 0, 0, 0, 4]));
        assert!(span.name == "GET /");
        assert!(span.kind == SpanKind::Server);
        assert!(span.start_ns == 1_000_000);
        assert!(span.duration_ns == 25_000);
        assert!(span.status == StatusCode::Error);
        assert!(
            span.resource_attrs
                .iter()
                .any(|attr| attr.key == "service.name"
                    && attr.value == AttrValue::Str("checkout".into()))
        );
        assert!(
            span.span_attrs
                .iter()
                .any(|attr| attr.key == "http.method"
                    && attr.value == AttrValue::Str("GET".into()))
        );
    }

    fn encode_sample_batch() -> Vec<u8> {
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
        write_list_header(out, 12, 1);
        write_span_ref(out, 0, 2, 1, 4);
        write_i32_field(out, 7, 0, &mut last);
        write_i64_field(out, 8, 1_000, &mut last);
        write_i64_field(out, 9, 25, &mut last);
        write_field_header(out, 9, 10, &mut last);
        write_list_header(out, 12, 3);
        write_key_value_string(out, "span.kind", "server");
        write_key_value_string(out, "http.method", "GET");
        write_key_value_bool(out, "error", true);
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
