//! `CloudEvents` HTTP/Kafka binding translation.
//!
//! HTTP boundaries use `ce-` context-attribute headers. Kafka records use
//! `ce_` headers, with the binding-defined exception that
//! `datacontenttype` is stored as the bare `content-type` header.

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use bytes::Bytes;
use serde_json::{Map, Value, json};

const CE_HTTP_PREFIX: &str = "ce-";
const CE_KAFKA_PREFIX: &str = "ce_";
const CONTENT_TYPE: &str = "content-type";
const DATA_CONTENT_TYPE: &str = "datacontenttype";
const REQUIRED_ATTRIBUTES: [&str; 4] = ["id", "source", "type", "specversion"];

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
    #[error("CloudEvents attribute is not valid UTF-8: {0}")]
    NonUtf8Attribute(String),
    #[error("unsupported CloudEvents specversion (only 1.0 is accepted)")]
    UnsupportedSpecVersion,
    #[error("malformed structured CloudEvents JSON")]
    MalformedJson,
    #[error("malformed structured CloudEvents data_base64")]
    MalformedBase64,
    #[error("structured CloudEvents cannot contain both data and data_base64")]
    ConflictingData,
    #[error("structured CloudEvents attribute is not scalar: {0}")]
    NonScalarAttribute(String),
    #[error("CloudEvents attribute cannot be represented as an HTTP header: {0}")]
    InvalidHttpHeader(String),
}

/// Classify an HTTP request according to the `CloudEvents` content modes.
#[must_use]
pub(crate) fn detect_content_mode(media_type: Option<&str>, has_ce_header: bool) -> IngressMode {
    if let Some(media_type) = media_type {
        let base = base_media_type(media_type);
        if base.starts_with("application/cloudevents-batch") {
            return IngressMode::Batch;
        }
        if base.starts_with("application/cloudevents") {
            return IngressMode::Structured;
        }
    }

    if has_ce_header {
        IngressMode::Binary
    } else {
        IngressMode::NotCloudEvent
    }
}

/// Return whether a media type selects `CloudEvents` structured mode.
#[must_use]
pub(crate) fn is_structured_media_type(media_type: &str) -> bool {
    matches!(
        detect_content_mode(Some(media_type), false),
        IngressMode::Structured
    )
}

/// Translate HTTP binding headers to Kafka binding headers.
pub(crate) fn http_headers_to_kafka(headers: &HeaderMap) -> Result<Vec<(String, Bytes)>, CeError> {
    let mut translated = Vec::new();

    for (name, value) in headers {
        let name = name.as_str();
        if let Some(attribute) = name.strip_prefix(CE_HTTP_PREFIX) {
            let value = value
                .to_str()
                .map_err(|_| CeError::NonUtf8Attribute(name.to_owned()))?;
            let kafka_name = if attribute == DATA_CONTENT_TYPE {
                CONTENT_TYPE.to_owned()
            } else {
                format!("{CE_KAFKA_PREFIX}{attribute}")
            };
            translated.push((kafka_name, Bytes::copy_from_slice(value.as_bytes())));
        } else if name == CONTENT_TYPE {
            let value = value
                .to_str()
                .map_err(|_| CeError::NonUtf8Attribute(name.to_owned()))?;
            translated.push((
                CONTENT_TYPE.to_owned(),
                Bytes::copy_from_slice(value.as_bytes()),
            ));
        }
    }

    Ok(translated)
}

/// Translate Kafka binding headers to HTTP binding headers.
pub(crate) fn kafka_headers_to_http(
    headers: &[(String, Option<Bytes>)],
) -> Result<Vec<(HeaderName, HeaderValue)>, CeError> {
    let mut translated = Vec::new();

    for (key, value) in headers {
        let Some(value) = value.as_deref() else {
            continue;
        };

        if let Some(attribute) = key.strip_prefix(CE_KAFKA_PREFIX) {
            let header_name = if attribute == DATA_CONTENT_TYPE {
                HeaderName::from_static(CONTENT_TYPE)
            } else {
                HeaderName::from_bytes(format!("{CE_HTTP_PREFIX}{attribute}").as_bytes())
                    .map_err(|_| CeError::InvalidHttpHeader(key.clone()))?
            };
            let header_value = HeaderValue::from_bytes(value)
                .map_err(|_| CeError::InvalidHttpHeader(key.clone()))?;
            translated.push((header_name, header_value));
        } else if key == CONTENT_TYPE {
            let header_value = HeaderValue::from_bytes(value)
                .map_err(|_| CeError::InvalidHttpHeader(key.clone()))?;
            translated.push((HeaderName::from_static(CONTENT_TYPE), header_value));
        }
    }

    Ok(translated)
}

/// Validate required binary-mode attributes and the Crabka 1.0 policy.
pub(crate) fn validate_binary_required(headers: &[(String, Bytes)]) -> Result<(), CeError> {
    for (key, value) in headers {
        if key.starts_with(CE_KAFKA_PREFIX) {
            std::str::from_utf8(value).map_err(|_| CeError::NonUtf8Attribute(key.clone()))?;
        }
    }

    for attribute in REQUIRED_ATTRIBUTES {
        let key = format!("{CE_KAFKA_PREFIX}{attribute}");
        if !headers
            .iter()
            .any(|(candidate, value)| candidate == &key && !value.is_empty())
        {
            return Err(CeError::MissingAttribute(attribute));
        }
    }

    let specversion = headers
        .iter()
        .find(|(key, _)| key == "ce_specversion")
        .map(|(_, value)| value.as_ref());
    if specversion != Some(b"1.0") {
        return Err(CeError::UnsupportedSpecVersion);
    }

    Ok(())
}

/// Validate required structured-mode attributes and the Crabka 1.0 policy.
pub(crate) fn validate_structured_json(event: &Value) -> Result<(), CeError> {
    let event = event.as_object().ok_or(CeError::MalformedJson)?;
    if event.contains_key("data") && event.contains_key("data_base64") {
        return Err(CeError::ConflictingData);
    }
    for attribute in REQUIRED_ATTRIBUTES {
        match event.get(attribute).and_then(Value::as_str) {
            Some(value) if !value.is_empty() => {}
            _ => return Err(CeError::MissingAttribute(attribute)),
        }
    }

    if event.get("specversion").and_then(Value::as_str) != Some("1.0") {
        return Err(CeError::UnsupportedSpecVersion);
    }

    Ok(())
}

/// Build a structured `CloudEvent` from a binary-mode Kafka record.
pub(crate) fn structured_from_binary(
    headers: &[(String, Option<Bytes>)],
    value: &[u8],
) -> Result<Vec<u8>, CeError> {
    let mut event = Map::new();

    for (key, header_value) in headers {
        let Some(header_value) = header_value.as_deref() else {
            continue;
        };
        let header_value = std::str::from_utf8(header_value)
            .map_err(|_| CeError::NonUtf8Attribute(key.clone()))?;

        if let Some(attribute) = key.strip_prefix(CE_KAFKA_PREFIX) {
            if attribute == DATA_CONTENT_TYPE {
                event.insert(DATA_CONTENT_TYPE.to_owned(), json!(header_value));
            } else {
                event.insert(attribute.to_owned(), json!(header_value));
            }
        } else if key == CONTENT_TYPE {
            event.insert(DATA_CONTENT_TYPE.to_owned(), json!(header_value));
        }
    }

    let json_data = event
        .get(DATA_CONTENT_TYPE)
        .and_then(Value::as_str)
        .filter(|media_type| is_json_media_type(media_type))
        .and_then(|_| serde_json::from_slice::<Value>(value).ok());
    if let Some(data) = json_data {
        event.insert("data".to_owned(), data);
    } else {
        event.insert(
            "data_base64".to_owned(),
            json!(BASE64_STANDARD.encode(value)),
        );
    }

    serde_json::to_vec(&Value::Object(event)).map_err(|_| CeError::MalformedJson)
}

/// Convert a structured `CloudEvent` to binary-mode HTTP body/header parts.
pub(crate) fn binary_from_structured(
    event: &Value,
) -> Result<(Vec<(String, Bytes)>, Bytes), CeError> {
    validate_structured_json(event)?;
    let event = event.as_object().ok_or(CeError::MalformedJson)?;
    let mut headers = Vec::new();

    for (attribute, value) in event {
        if attribute == "data" || attribute == "data_base64" {
            continue;
        }
        let value = scalar_attribute_bytes(attribute, value)?;
        let key = if attribute == DATA_CONTENT_TYPE {
            CONTENT_TYPE.to_owned()
        } else {
            format!("{CE_KAFKA_PREFIX}{attribute}")
        };
        headers.push((key, Bytes::from(value)));
    }

    let data = if let Some(encoded) = event.get("data_base64") {
        let encoded = encoded.as_str().ok_or(CeError::MalformedBase64)?;
        BASE64_STANDARD
            .decode(encoded)
            .map(Bytes::from)
            .map_err(|_| CeError::MalformedBase64)?
    } else if let Some(data) = event.get("data") {
        serde_json::to_vec(data)
            .map(Bytes::from)
            .map_err(|_| CeError::MalformedJson)?
    } else {
        Bytes::new()
    };

    Ok((headers, data))
}

fn base_media_type(media_type: &str) -> String {
    media_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

fn is_json_media_type(media_type: &str) -> bool {
    let media_type = base_media_type(media_type);
    media_type == "application/json" || media_type.ends_with("+json")
}

fn scalar_attribute_bytes(attribute: &str, value: &Value) -> Result<Vec<u8>, CeError> {
    match value {
        Value::String(value) => Ok(value.as_bytes().to_vec()),
        Value::Bool(value) => Ok(value.to_string().into_bytes()),
        Value::Number(value) => Ok(value.to_string().into_bytes()),
        Value::Null | Value::Array(_) | Value::Object(_) => {
            Err(CeError::NonScalarAttribute(attribute.to_owned()))
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn required_binary_headers() -> Vec<(String, Bytes)> {
        vec![
            ("ce_id".to_owned(), Bytes::from_static(b"event-1")),
            ("ce_source".to_owned(), Bytes::from_static(b"/tests")),
            ("ce_type".to_owned(), Bytes::from_static(b"example.created")),
            ("ce_specversion".to_owned(), Bytes::from_static(b"1.0")),
        ]
    }

    #[test]
    fn mode_detection_uses_case_insensitive_prefixes() {
        for media_type in [
            "application/cloudevents+json",
            "application/cloudevents+json; charset=UTF-8",
            "application/cloudevents",
            " APPLICATION/CLOUDEVENTS+JSON ",
        ] {
            assert!(detect_content_mode(Some(media_type), false) == IngressMode::Structured);
        }
        assert!(
            detect_content_mode(Some("application/cloudevents-batch+json"), true)
                == IngressMode::Batch
        );
        assert!(detect_content_mode(Some("application/json"), true) == IngressMode::Binary);
        assert!(detect_content_mode(None, false) == IngressMode::NotCloudEvent);
    }

    #[test]
    fn prefixes_and_datacontenttype_round_trip() {
        let mut http = HeaderMap::new();
        http.insert("ce-id", HeaderValue::from_static("event-1"));
        http.insert("ce-source", HeaderValue::from_static("/tests"));
        http.insert("ce-type", HeaderValue::from_static("example.created"));
        http.insert("ce-specversion", HeaderValue::from_static("1.0"));
        http.insert("content-type", HeaderValue::from_static("application/json"));

        let kafka = http_headers_to_kafka(&http).expect("HTTP headers translate");
        assert!(kafka.iter().any(|(key, _)| key == "ce_id"));
        assert!(kafka.iter().any(|(key, _)| key == CONTENT_TYPE));
        assert!(!kafka.iter().any(|(key, _)| key == "ce_datacontenttype"));

        let optional = kafka
            .into_iter()
            .map(|(key, value)| (key, Some(value)))
            .collect::<Vec<_>>();
        let round_trip = kafka_headers_to_http(&optional).expect("Kafka headers translate");
        assert!(
            round_trip.iter().any(|(name, value)| {
                name.as_str() == "ce-id" && value.as_bytes() == b"event-1"
            })
        );
        assert!(round_trip.iter().any(|(name, value)| {
            name.as_str() == CONTENT_TYPE && value.as_bytes() == b"application/json"
        }));
        assert!(
            !round_trip
                .iter()
                .any(|(name, _)| { name.as_str() == "ce-datacontenttype" })
        );
    }

    #[test]
    fn prefixed_datacontenttype_never_stays_prefixed() {
        let mut http = HeaderMap::new();
        http.insert(
            "ce-datacontenttype",
            HeaderValue::from_static("application/json"),
        );
        let kafka = http_headers_to_kafka(&http).expect("header translates");
        assert!(
            kafka
                == vec![(
                    CONTENT_TYPE.to_owned(),
                    Bytes::from_static(b"application/json")
                )]
        );

        let http = kafka_headers_to_http(&[(
            "ce_datacontenttype".to_owned(),
            Some(Bytes::from_static(b"application/json")),
        )])
        .expect("header translates");
        assert!(http[0].0.as_str() == CONTENT_TYPE);
    }

    #[test]
    fn non_utf8_http_attribute_is_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "ce-id",
            HeaderValue::from_bytes(&[0xff]).expect("opaque HTTP header value"),
        );

        assert!(matches!(
            http_headers_to_kafka(&headers),
            Err(CeError::NonUtf8Attribute(name)) if name == "ce-id"
        ));
    }

    #[test]
    fn required_attributes_are_nonempty_and_version_one() {
        assert!(validate_binary_required(&required_binary_headers()).is_ok());

        let mut missing = required_binary_headers();
        missing.retain(|(key, _)| key != "ce_id");
        assert!(matches!(
            validate_binary_required(&missing),
            Err(CeError::MissingAttribute("id"))
        ));

        let mut old_version = required_binary_headers();
        old_version
            .iter_mut()
            .find(|(key, _)| key == "ce_specversion")
            .expect("specversion exists")
            .1 = Bytes::from_static(b"0.3");
        assert!(matches!(
            validate_binary_required(&old_version),
            Err(CeError::UnsupportedSpecVersion)
        ));
    }

    #[test]
    fn binary_and_structured_conversion_preserves_attributes_and_data() {
        let mut headers = required_binary_headers();
        headers.push((
            CONTENT_TYPE.to_owned(),
            Bytes::from_static(b"application/json"),
        ));
        headers.push(("ce_traceid".to_owned(), Bytes::from_static(b"abc")));
        let optional = headers
            .iter()
            .map(|(key, value)| (key.clone(), Some(value.clone())))
            .collect::<Vec<_>>();

        let structured = structured_from_binary(&optional, br#"{"n":7}"#)
            .expect("binary event becomes structured");
        let event: Value = serde_json::from_slice(&structured).expect("structured JSON");
        assert!(event["id"] == "event-1");
        assert!(event["traceid"] == "abc");
        assert!(event[DATA_CONTENT_TYPE] == "application/json");
        assert!(event["data"]["n"] == 7);

        let (mut round_trip_headers, round_trip_data) =
            binary_from_structured(&event).expect("structured event becomes binary");
        headers.sort_by(|left, right| left.0.cmp(&right.0));
        round_trip_headers.sort_by(|left, right| left.0.cmp(&right.0));
        assert!(round_trip_headers == headers);
        assert!(round_trip_data == Bytes::from_static(br#"{"n":7}"#));
    }

    #[test]
    fn non_json_data_uses_data_base64() {
        let headers = required_binary_headers()
            .into_iter()
            .map(|(key, value)| (key, Some(value)))
            .collect::<Vec<_>>();
        let structured =
            structured_from_binary(&headers, &[0xff, 0]).expect("binary event becomes structured");
        let event: Value = serde_json::from_slice(&structured).expect("structured JSON");
        assert!(event.get("data").is_none());
        assert!(event["data_base64"] == "/wA=");

        let (_, data) = binary_from_structured(&event).expect("base64 event becomes binary");
        assert!(data == Bytes::from_static(&[0xff, 0]));
    }

    #[test]
    fn non_json_content_type_uses_data_base64_even_for_json_shaped_bytes() {
        let mut headers = required_binary_headers();
        headers.push((
            CONTENT_TYPE.to_owned(),
            Bytes::from_static(b"application/avro"),
        ));
        let headers = headers
            .into_iter()
            .map(|(key, value)| (key, Some(value)))
            .collect::<Vec<_>>();
        let original = br#"{ "looks": "json" }"#;

        let structured = structured_from_binary(&headers, original)
            .expect("binary event becomes structured without changing opaque data");
        let event: Value = serde_json::from_slice(&structured).expect("structured JSON");
        assert!(event.get("data").is_none());
        assert!(event["data_base64"] == "eyAibG9va3MiOiAianNvbiIgfQ==");

        let (_, round_trip) =
            binary_from_structured(&event).expect("structured event becomes binary");
        assert!(round_trip.as_ref() == original);
    }

    #[test]
    fn structured_json_suffix_content_type_uses_data() {
        let mut headers = required_binary_headers();
        headers.push((
            CONTENT_TYPE.to_owned(),
            Bytes::from_static(b"application/vnd.example+json; charset=utf-8"),
        ));
        let headers = headers
            .into_iter()
            .map(|(key, value)| (key, Some(value)))
            .collect::<Vec<_>>();

        let structured = structured_from_binary(&headers, br#"{"n":7}"#)
            .expect("JSON-suffix event becomes structured");
        let event: Value = serde_json::from_slice(&structured).expect("structured JSON");
        assert!(event["data"]["n"] == 7);
        assert!(event.get("data_base64").is_none());
    }

    #[test]
    fn structured_event_rejects_data_and_data_base64_together() {
        let event = json!({
            "id": "event-1",
            "source": "/tests",
            "type": "example.created",
            "specversion": "1.0",
            "data": {"n": 7},
            "data_base64": "eyJuIjo3fQ=="
        });

        assert!(binary_from_structured(&event) == Err(CeError::ConflictingData));
    }
}
