//! Pure `CloudEvents` HTTP binding translation helpers.

// This module intentionally lands before handler integration so the translation
// core can be tested in isolation.
#![allow(dead_code)]

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use bytes::Bytes;
use serde_json::{Map, Value, json};

const CE_HTTP_PREFIX: &str = "ce-";
const CE_KAFKA_PREFIX: &str = "ce_";
const CONTENT_TYPE: &str = "content-type";
const DATA_CONTENT_TYPE: &str = "datacontenttype";
const REQUIRED_ATTRIBUTES: [&str; 4] = ["id", "source", "type", "specversion"];
const SUPPORTED_SPEC_VERSION: &[u8] = b"1.0";

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum IngressMode {
    Binary,
    Structured,
    Batch,
    NotCloudEvent,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum CeError {
    #[error("missing required CloudEvents attribute: {0}")]
    MissingAttribute(&'static str),
    #[error("CloudEvents attribute header value is not valid UTF-8: {0}")]
    NonUtf8Attribute(String),
    #[error("unsupported CloudEvents specversion (only 1.0)")]
    UnsupportedSpecVersion,
    #[error("malformed structured CloudEvents JSON")]
    MalformedJson,
    #[error("malformed structured CloudEvents data_base64")]
    MalformedBase64,
    #[error("structured CloudEvents attribute is not representable as a header: {0}")]
    NonScalarAttribute(String),
}

#[must_use]
pub(crate) fn detect_content_mode(media_type: Option<&str>, has_ce_header: bool) -> IngressMode {
    let Some(media_type) = media_type else {
        return mode_from_headers(has_ce_header);
    };

    let base_media_type = media_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    if base_media_type.starts_with("application/cloudevents-batch") {
        return IngressMode::Batch;
    }

    if base_media_type.starts_with("application/cloudevents") {
        return IngressMode::Structured;
    }

    mode_from_headers(has_ce_header)
}

pub(crate) fn http_headers_to_kafka(headers: &HeaderMap) -> Result<Vec<(String, Bytes)>, CeError> {
    let mut kafka_headers = Vec::new();

    for (name, value) in headers {
        let header_name = name.as_str();

        if let Some(attribute_name) = header_name.strip_prefix(CE_HTTP_PREFIX) {
            let attribute_value = value
                .to_str()
                .map_err(|_| CeError::NonUtf8Attribute(header_name.to_owned()))?;
            if attribute_name == DATA_CONTENT_TYPE {
                kafka_headers.push((
                    CONTENT_TYPE.to_owned(),
                    Bytes::copy_from_slice(attribute_value.as_bytes()),
                ));
                continue;
            }

            kafka_headers.push((
                format!("{CE_KAFKA_PREFIX}{attribute_name}"),
                Bytes::copy_from_slice(attribute_value.as_bytes()),
            ));
            continue;
        }

        if header_name != CONTENT_TYPE {
            continue;
        }

        let content_type = value
            .to_str()
            .map_err(|_| CeError::NonUtf8Attribute(header_name.to_owned()))?;
        kafka_headers.push((
            CONTENT_TYPE.to_owned(),
            Bytes::copy_from_slice(content_type.as_bytes()),
        ));
    }

    Ok(kafka_headers)
}

#[must_use]
pub(crate) fn kafka_headers_to_http(
    headers: &[(String, Option<Bytes>)],
) -> Vec<(HeaderName, HeaderValue)> {
    let mut http_headers = Vec::new();

    for (key, value) in headers {
        let header_value = value.as_deref().unwrap_or_default();

        if let Some(attribute_name) = key.strip_prefix(CE_KAFKA_PREFIX) {
            if attribute_name == DATA_CONTENT_TYPE {
                if let Ok(value) = HeaderValue::from_bytes(header_value) {
                    http_headers.push((HeaderName::from_static(CONTENT_TYPE), value));
                }
                continue;
            }

            if let Some(translated_header) =
                ce_attribute_to_http_header(attribute_name, header_value)
            {
                http_headers.push(translated_header);
            }
            continue;
        }

        if key != CONTENT_TYPE {
            continue;
        }

        if let Ok(value) = HeaderValue::from_bytes(header_value) {
            http_headers.push((HeaderName::from_static(CONTENT_TYPE), value));
        }
    }

    http_headers
}

pub(crate) fn validate_binary_required(headers: &[(String, Bytes)]) -> Result<(), CeError> {
    for attribute_name in REQUIRED_ATTRIBUTES {
        let header_name = format!("{CE_KAFKA_PREFIX}{attribute_name}");
        let has_required_attribute = headers
            .iter()
            .any(|(key, value)| key == &header_name && !value.is_empty());

        if !has_required_attribute {
            return Err(CeError::MissingAttribute(attribute_name));
        }
    }

    let Some((_, specversion)) = headers.iter().find(|(key, _)| key == "ce_specversion") else {
        return Err(CeError::MissingAttribute("specversion"));
    };

    if specversion.as_ref() != SUPPORTED_SPEC_VERSION {
        return Err(CeError::UnsupportedSpecVersion);
    }

    Ok(())
}

pub(crate) fn validate_structured_json(value: &serde_json::Value) -> Result<(), CeError> {
    for attribute_name in REQUIRED_ATTRIBUTES {
        let Some(attribute_value) = value
            .get(attribute_name)
            .and_then(serde_json::Value::as_str)
        else {
            return Err(CeError::MissingAttribute(attribute_name));
        };

        if attribute_value.is_empty() {
            return Err(CeError::MissingAttribute(attribute_name));
        }
    }

    if value.get("specversion").and_then(serde_json::Value::as_str) != Some("1.0") {
        return Err(CeError::UnsupportedSpecVersion);
    }

    Ok(())
}

pub(crate) fn structured_json_to_binary_record_parts(
    event: &Value,
) -> Result<(Vec<(String, Bytes)>, Bytes), CeError> {
    let event = event.as_object().ok_or(CeError::MalformedJson)?;
    let record_headers = structured_event_headers(event)?;
    let record_value = structured_event_data(event)?;

    Ok((record_headers, record_value))
}

#[must_use]
pub(crate) fn structured_from_binary(headers: &[(String, Option<Bytes>)], value: &[u8]) -> Vec<u8> {
    let mut event = serde_json::Map::new();

    for (key, header_value) in headers {
        let Some(header_value) = header_value
            .as_deref()
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
        else {
            continue;
        };

        if let Some(attribute_name) = key.strip_prefix(CE_KAFKA_PREFIX) {
            event.insert(attribute_name.to_owned(), json!(header_value));
            continue;
        }

        if key == CONTENT_TYPE {
            event.insert(DATA_CONTENT_TYPE.to_owned(), json!(header_value));
        }
    }

    match serde_json::from_slice::<serde_json::Value>(value) {
        Ok(json_data) => event.insert("data".to_owned(), json_data),
        Err(_) => event.insert(
            "data_base64".to_owned(),
            json!(BASE64_STANDARD.encode(value)),
        ),
    };

    serde_json::to_vec(&serde_json::Value::Object(event)).unwrap_or_default()
}

#[must_use]
fn mode_from_headers(has_ce_header: bool) -> IngressMode {
    if has_ce_header {
        return IngressMode::Binary;
    }

    IngressMode::NotCloudEvent
}

fn ce_attribute_to_http_header(
    attribute_name: &str,
    value: &[u8],
) -> Option<(HeaderName, HeaderValue)> {
    let header_name =
        HeaderName::from_bytes(format!("{CE_HTTP_PREFIX}{attribute_name}").as_bytes()).ok()?;
    let header_value = HeaderValue::from_bytes(value).ok()?;

    Some((header_name, header_value))
}

fn structured_event_headers(event: &Map<String, Value>) -> Result<Vec<(String, Bytes)>, CeError> {
    let mut record_headers = Vec::new();

    for (attribute_name, attribute_value) in event {
        if attribute_name == "data" || attribute_name == "data_base64" {
            continue;
        }

        if attribute_name == DATA_CONTENT_TYPE {
            record_headers.push((
                CONTENT_TYPE.to_owned(),
                Bytes::from(attribute_value_to_header_bytes(
                    attribute_name,
                    attribute_value,
                )?),
            ));
            continue;
        }

        record_headers.push((
            format!("{CE_KAFKA_PREFIX}{attribute_name}"),
            Bytes::from(attribute_value_to_header_bytes(
                attribute_name,
                attribute_value,
            )?),
        ));
    }

    Ok(record_headers)
}

fn structured_event_data(event: &Map<String, Value>) -> Result<Bytes, CeError> {
    if let Some(encoded_data) = event.get("data_base64") {
        let Some(encoded_data) = encoded_data.as_str() else {
            return Err(CeError::MalformedBase64);
        };

        return BASE64_STANDARD
            .decode(encoded_data)
            .map(Bytes::from)
            .map_err(|_| CeError::MalformedBase64);
    }

    let Some(data) = event.get("data") else {
        return Ok(Bytes::new());
    };

    serde_json::to_vec(data)
        .map(Bytes::from)
        .map_err(|_| CeError::MalformedJson)
}

fn attribute_value_to_header_bytes(
    attribute_name: &str,
    attribute_value: &Value,
) -> Result<Vec<u8>, CeError> {
    match attribute_value {
        Value::String(value) => Ok(value.as_bytes().to_vec()),
        Value::Bool(value) => Ok(value.to_string().into_bytes()),
        Value::Number(value) => Ok(value.to_string().into_bytes()),
        Value::Null | Value::Array(_) | Value::Object(_) => {
            Err(CeError::NonScalarAttribute(attribute_name.to_owned()))
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn detect_mode_is_prefix_based() {
        assert!(matches!(
            detect_content_mode(Some("application/cloudevents+json"), false),
            IngressMode::Structured
        ));
        assert!(matches!(
            detect_content_mode(Some("application/cloudevents+json; charset=UTF-8"), false),
            IngressMode::Structured
        ));
        assert!(matches!(
            detect_content_mode(Some("application/cloudevents"), false),
            IngressMode::Structured
        ));
        assert!(matches!(
            detect_content_mode(Some("APPLICATION/CLOUDEVENTS+JSON"), false),
            IngressMode::Structured
        ));
        assert!(matches!(
            detect_content_mode(Some("application/cloudevents-batch+json"), false),
            IngressMode::Batch
        ));
        assert!(matches!(
            detect_content_mode(Some("application/json"), true),
            IngressMode::Binary
        ));
        assert!(matches!(
            detect_content_mode(Some("application/json"), false),
            IngressMode::NotCloudEvent
        ));
        assert!(matches!(
            detect_content_mode(None, false),
            IngressMode::NotCloudEvent
        ));
    }

    #[test]
    fn http_to_kafka_swaps_prefix_and_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert("ce-id", "42".parse().expect("valid header value"));
        headers.insert("ce-source", "/x".parse().expect("valid header value"));
        headers.insert(
            "content-type",
            "application/avro".parse().expect("valid header value"),
        );

        let kafka_headers = http_headers_to_kafka(&headers).expect("headers translate");

        assert!(kafka_headers.contains(&("ce_id".to_owned(), Bytes::from_static(b"42"))));
        assert!(kafka_headers.contains(&("ce_source".to_owned(), Bytes::from_static(b"/x"))));
        assert!(kafka_headers.contains(&(
            "content-type".to_owned(),
            Bytes::from_static(b"application/avro")
        )));
        assert!(
            !kafka_headers
                .iter()
                .any(|(key, _)| key == "ce_datacontenttype")
        );
    }

    #[test]
    fn kafka_to_http_round_trips_prefix() {
        let headers = vec![("ce_id".to_owned(), Some(Bytes::from_static(b"42")))];

        let http_headers = kafka_headers_to_http(&headers);

        assert!(
            http_headers
                .iter()
                .any(|(name, value)| name.as_str() == "ce-id" && value.as_bytes() == b"42")
        );
    }

    #[test]
    fn datacontenttype_uses_bare_content_type_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "ce-datacontenttype",
            "application/json".parse().expect("valid header value"),
        );

        let kafka_headers = http_headers_to_kafka(&headers).expect("headers translate");

        assert!(kafka_headers.contains(&(
            "content-type".to_owned(),
            Bytes::from_static(b"application/json")
        )));
        assert!(
            !kafka_headers
                .iter()
                .any(|(key, _)| key == "ce_datacontenttype")
        );

        let http_headers = kafka_headers_to_http(&[(
            "ce_datacontenttype".to_owned(),
            Some(Bytes::from_static(b"application/json")),
        )]);

        assert!(
            http_headers
                .iter()
                .any(|(name, value)| name.as_str() == "content-type"
                    && value.as_bytes() == b"application/json")
        );
        assert!(
            !http_headers
                .iter()
                .any(|(name, _)| name.as_str() == "ce-datacontenttype")
        );
    }

    #[test]
    fn validate_binary_requires_the_four() {
        let valid_headers = vec![
            ("ce_id".to_owned(), Bytes::from_static(b"1")),
            ("ce_source".to_owned(), Bytes::from_static(b"/s")),
            ("ce_type".to_owned(), Bytes::from_static(b"t")),
            ("ce_specversion".to_owned(), Bytes::from_static(b"1.0")),
        ];
        assert!(validate_binary_required(&valid_headers).is_ok());

        let missing_headers = vec![("ce_id".to_owned(), Bytes::from_static(b"1"))];
        assert!(let Err(CeError::MissingAttribute(_)) = validate_binary_required(&missing_headers));
    }

    #[test]
    fn validate_binary_rejects_unsupported_specversion() {
        let headers = vec![
            ("ce_id".to_owned(), Bytes::from_static(b"1")),
            ("ce_source".to_owned(), Bytes::from_static(b"/s")),
            ("ce_type".to_owned(), Bytes::from_static(b"t")),
            ("ce_specversion".to_owned(), Bytes::from_static(b"0.3")),
        ];

        assert!(let Err(CeError::UnsupportedSpecVersion) = validate_binary_required(&headers));
    }

    #[test]
    fn structured_from_binary_emits_data_or_data_base64() {
        let headers = vec![
            ("ce_id".to_owned(), Some(Bytes::from_static(b"1"))),
            ("ce_source".to_owned(), Some(Bytes::from_static(b"/s"))),
            ("ce_type".to_owned(), Some(Bytes::from_static(b"t"))),
            (
                "ce_specversion".to_owned(),
                Some(Bytes::from_static(b"1.0")),
            ),
            (
                "content-type".to_owned(),
                Some(Bytes::from_static(b"application/json")),
            ),
        ];

        let json_event: serde_json::Value =
            serde_json::from_slice(&structured_from_binary(&headers, br#"{"n":7}"#))
                .expect("structured event should be valid JSON");
        assert!(json_event["id"] == "1" && json_event["datacontenttype"] == "application/json");
        assert!(json_event["data"]["n"] == 7);

        let binary_event: serde_json::Value =
            serde_json::from_slice(&structured_from_binary(&headers, &[0xff, 0x00]))
                .expect("structured event should be valid JSON");
        assert!(binary_event["data_base64"].is_string() && binary_event.get("data").is_none());
    }

    #[test]
    fn validate_structured_json_requires_the_four() {
        let valid_event = json!({
            "id": "1",
            "source": "/s",
            "type": "t",
            "specversion": "1.0"
        });
        assert!(validate_structured_json(&valid_event).is_ok());

        let invalid_event = json!({
            "id": "1",
            "source": "/s",
            "type": "t",
            "specversion": "0.3"
        });
        assert!(let Err(CeError::UnsupportedSpecVersion) = validate_structured_json(&invalid_event));
    }

    #[test]
    fn structured_json_to_binary_record_parts_extracts_headers_and_json_data() {
        let event = json!({
            "specversion": "1.0",
            "id": "evt-1",
            "source": "/tests",
            "type": "com.example.created",
            "datacontenttype": "application/json",
            "traceid": "abc123",
            "data": { "n": 7 }
        });

        let (headers, value) =
            structured_json_to_binary_record_parts(&event).expect("event normalizes");

        assert!(headers.contains(&("ce_id".to_owned(), Bytes::from_static(b"evt-1"))));
        assert!(headers.contains(&("ce_source".to_owned(), Bytes::from_static(b"/tests"))));
        assert!(headers.contains(&(
            "ce_type".to_owned(),
            Bytes::from_static(b"com.example.created")
        )));
        assert!(headers.contains(&("ce_specversion".to_owned(), Bytes::from_static(b"1.0"))));
        assert!(headers.contains(&(
            "content-type".to_owned(),
            Bytes::from_static(b"application/json")
        )));
        assert!(headers.contains(&("ce_traceid".to_owned(), Bytes::from_static(b"abc123"))));
        assert!(value == Bytes::from_static(br#"{"n":7}"#));
    }

    #[test]
    fn structured_json_to_binary_record_parts_decodes_base64_data() {
        let event = json!({
            "specversion": "1.0",
            "id": "evt-1",
            "source": "/tests",
            "type": "com.example.created",
            "data_base64": "/wA="
        });

        let (_headers, value) =
            structured_json_to_binary_record_parts(&event).expect("event normalizes");

        assert!(value == Bytes::from_static(&[0xff, 0x00]));
    }

    #[test]
    fn normalized_structured_event_can_render_as_structured_again() {
        let event = json!({
            "specversion": "1.0",
            "id": "evt-1",
            "source": "/tests",
            "type": "com.example.created",
            "datacontenttype": "application/json",
            "data": { "n": 7 }
        });

        let (headers, value) =
            structured_json_to_binary_record_parts(&event).expect("event normalizes");
        let headers = headers
            .into_iter()
            .map(|(key, value)| (key, Some(value)))
            .collect::<Vec<_>>();

        let rendered: Value = serde_json::from_slice(&structured_from_binary(&headers, &value))
            .expect("event renders as structured JSON");

        assert!(rendered["id"] == "evt-1");
        assert!(rendered["source"] == "/tests");
        assert!(rendered["type"] == "com.example.created");
        assert!(rendered["specversion"] == "1.0");
        assert!(rendered["datacontenttype"] == "application/json");
        assert!(rendered["data"]["n"] == 7);
    }
}
