//! [`crate::codec::RecordCodec`] implementation backed by the Confluent Schema Registry.
//!
//! [`SchemaRegistryCodec`] wraps a [`SchemaResolver`] (in production, a
//! [`super::client::SchemaRegistryClient`]) and, on the encode path, resolves
//! the schema, serializes the structured body into the wire format, and
//! prepends the Confluent frame. On the decode path it strips the frame, looks
//! up the schema by id, and deserializes the payload to a JSON view.
//!
//! The codec depends on the registry through the small [`SchemaResolver`] trait
//! rather than the concrete client, so the encode/decode logic is unit-testable
//! against a fake resolver with no live registry. The live wiring (client →
//! HTTP) is exercised by the integration test.

use std::sync::Arc;

use bytes::Bytes;

use super::client::SchemaRegistryClient;
use crate::codec::{CodecError, Decoded, EncodeBody, SchemaFormat, SchemaMeta};

/// Schema lookups the codec needs, factored out of the concrete client so the
/// encode/decode paths can be unit-tested against a fake.
///
/// [`SchemaRegistryClient`] implements this by delegating to its caching HTTP
/// methods.
#[async_trait::async_trait]
pub trait SchemaResolver: Send + Sync + 'static {
    /// Resolve `(schema string, format)` by numeric schema id.
    async fn by_id(&self, id: i32) -> Result<(String, SchemaFormat), CodecError>;
    /// Resolve the latest `(id, schema string, format)` for `subject`.
    async fn latest(&self, subject: &str) -> Result<(i32, String, SchemaFormat), CodecError>;
}

#[async_trait::async_trait]
impl SchemaResolver for SchemaRegistryClient {
    async fn by_id(&self, id: i32) -> Result<(String, SchemaFormat), CodecError> {
        self.schema_by_id(id).await
    }

    async fn latest(&self, subject: &str) -> Result<(i32, String, SchemaFormat), CodecError> {
        SchemaRegistryClient::latest(self, subject).await
    }
}

/// A [`crate::codec::RecordCodec`] that encodes/decodes record values via a
/// Confluent Schema Registry.
///
/// Constructed once at gateway startup and shared across all connection
/// handlers via `Arc`.
pub struct SchemaRegistryCodec {
    /// The schema resolver (in production, the caching HTTP client).
    pub client: Arc<dyn SchemaResolver>,
    /// When `true`, `EncodeBody::Raw` bytes are Confluent-framed using the
    /// subject's latest registered schema id (the bytes are assumed already
    /// serialized in that format). When `false` (default) raw bytes pass
    /// through verbatim.
    pub frame_raw: bool,
}

impl SchemaRegistryCodec {
    /// Construct a codec over the concrete [`SchemaRegistryClient`].
    ///
    /// This is the constructor the gateway wiring uses; the client is boxed
    /// behind the [`SchemaResolver`] trait object.
    #[must_use]
    pub fn new(client: Arc<SchemaRegistryClient>, frame_raw: bool) -> Self {
        Self {
            client: client as Arc<dyn SchemaResolver>,
            frame_raw,
        }
    }

    /// Construct a codec over an arbitrary [`SchemaResolver`] (used by tests
    /// with a fake resolver).
    #[must_use]
    pub fn with_resolver(client: Arc<dyn SchemaResolver>, frame_raw: bool) -> Self {
        Self { client, frame_raw }
    }
}

/// `TopicNameStrategy`: the default subject for a topic's value schema.
fn subject_for(topic: &str) -> String {
    format!("{topic}-value")
}

#[async_trait::async_trait]
impl crate::codec::RecordCodec for SchemaRegistryCodec {
    async fn encode(&self, topic: &str, body: EncodeBody) -> Result<Bytes, CodecError> {
        match body {
            EncodeBody::Raw(bytes) => {
                if self.frame_raw {
                    // Bytes are assumed already serialized in the registered
                    // format; frame them against the subject's latest schema.
                    let subject = subject_for(topic);
                    let (id, _schema, fmt) = self.client.latest(&subject).await?;
                    Ok(super::wire::encode_frame(id, fmt, &bytes))
                } else {
                    // Passthrough (default).
                    Ok(bytes)
                }
            }
            EncodeBody::Structured { json, schema } => {
                let subject = schema.subject.clone().unwrap_or_else(|| subject_for(topic));

                // Resolve (id, schema string, format). Trust the REGISTRY's
                // format over the selector hint.
                let (id, schema_str, fmt) = match schema.id {
                    Some(id) => {
                        let (schema_str, fmt) = self.client.by_id(id).await?;
                        (id, schema_str, fmt)
                    }
                    None => self.client.latest(&subject).await.map_err(|e| match e {
                        // A 404 from `latest` means no schema is registered for
                        // this subject — surface a clear, actionable message.
                        CodecError::Registry(_) => CodecError::Registry(format!(
                            "no schema registered for subject {subject}"
                        )),
                        other => other,
                    })?,
                };

                let payload = super::format::serialize(fmt, &schema_str, &json)?;
                Ok(super::wire::encode_frame(id, fmt, &payload))
            }
        }
    }

    async fn decode(&self, topic: &str, value: Bytes) -> Result<Decoded, CodecError> {
        // Not Confluent-framed (empty or missing magic byte) → lenient
        // passthrough with no schema metadata.
        if value.is_empty() || value[0] != 0x00 {
            return Ok(Decoded {
                value,
                schema: None,
                json: None,
            });
        }

        let (id, rest) = super::wire::decode_frame(&value)?;
        let (schema_str, fmt) = self.client.by_id(id).await?;

        // Protobuf carries a message-index prefix after the header; strip it
        // before deserialization.
        let payload = if fmt == SchemaFormat::Protobuf {
            super::wire::strip_proto_index(&rest)?
        } else {
            rest
        };

        let json = super::format::deserialize(fmt, &schema_str, &payload)?;

        Ok(Decoded {
            value: Bytes::from(payload),
            schema: Some(SchemaMeta {
                subject: subject_for(topic),
                id,
                format: fmt,
            }),
            json: Some(json),
        })
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{RecordCodec, SchemaSelector};

    /// An Avro record schema reused across the structured encode/decode tests.
    const AVRO_SCHEMA: &str = r#"{
        "type": "record",
        "name": "R",
        "fields": [
            {"name": "id",   "type": "long"},
            {"name": "name", "type": "string"}
        ]
    }"#;

    /// A canned-schema resolver for unit tests — no live registry. Records the
    /// id under which `latest` answers, and serves a single schema for both
    /// lookup paths.
    struct FakeResolver {
        id: i32,
        schema: String,
        fmt: SchemaFormat,
    }

    #[async_trait::async_trait]
    impl SchemaResolver for FakeResolver {
        async fn by_id(&self, id: i32) -> Result<(String, SchemaFormat), CodecError> {
            if id == self.id {
                Ok((self.schema.clone(), self.fmt))
            } else {
                Err(CodecError::Registry(format!("schema id {id} not found")))
            }
        }

        async fn latest(&self, _subject: &str) -> Result<(i32, String, SchemaFormat), CodecError> {
            Ok((self.id, self.schema.clone(), self.fmt))
        }
    }

    fn avro_codec(frame_raw: bool) -> SchemaRegistryCodec {
        SchemaRegistryCodec::with_resolver(
            Arc::new(FakeResolver {
                id: 5,
                schema: AVRO_SCHEMA.to_owned(),
                fmt: SchemaFormat::Avro,
            }),
            frame_raw,
        )
    }

    #[test]
    fn subject_for_appends_value_suffix() {
        assert_eq!(subject_for("orders"), "orders-value");
    }

    #[tokio::test]
    async fn raw_passthrough_when_frame_raw_false() {
        let codec = avro_codec(false);
        let input = Bytes::from_static(b"\x09opaque-bytes");
        let out = codec
            .encode("orders", EncodeBody::Raw(input.clone()))
            .await
            .unwrap();
        assert_eq!(out, input, "frame_raw=false must pass raw bytes unchanged");
    }

    #[tokio::test]
    async fn raw_frames_when_frame_raw_true() {
        let codec = avro_codec(true);
        let input = Bytes::from_static(b"already-avro");
        let out = codec
            .encode("orders", EncodeBody::Raw(input.clone()))
            .await
            .unwrap();
        // Confluent frame: 0x00 then id=5 big-endian, then the payload verbatim
        // (Avro adds no message-index prefix).
        let expected = [&[0x00, 0x00, 0x00, 0x00, 0x05][..], input.as_ref()].concat();
        assert_eq!(out, Bytes::from(expected));
    }

    #[tokio::test]
    async fn structured_avro_encode_produces_framing() {
        let codec = avro_codec(false);
        let json = Bytes::from_static(br#"{"id": 1, "name": "a"}"#);
        let out = codec
            .encode(
                "orders",
                EncodeBody::Structured {
                    json,
                    schema: SchemaSelector {
                        subject: None,
                        id: None,
                        format: SchemaFormat::Avro,
                    },
                },
            )
            .await
            .unwrap();
        // [00][id=5 BE][avro datum…]
        assert_eq!(&out[..5], &[0x00, 0x00, 0x00, 0x00, 0x05]);
        assert!(out.len() > 5, "framed Avro payload must follow the header");
    }

    #[tokio::test]
    async fn structured_encode_then_decode_round_trips() {
        let codec = avro_codec(false);
        let json_in = br#"{"id": 1, "name": "a"}"#;
        let framed = codec
            .encode(
                "orders",
                EncodeBody::Structured {
                    json: Bytes::from_static(json_in),
                    schema: SchemaSelector {
                        subject: None,
                        id: None,
                        format: SchemaFormat::Avro,
                    },
                },
            )
            .await
            .unwrap();

        let expected_value = framed.slice(5..);
        let decoded = codec.decode("orders", framed).await.unwrap();
        let json_out = decoded
            .json
            .as_deref()
            .expect("framed value carries a JSON view");
        let expected: serde_json::Value = serde_json::from_slice(json_in).unwrap();
        let actual: serde_json::Value = serde_json::from_slice(json_out).unwrap();
        assert_eq!(
            decoded.value, expected_value,
            "complete decoded value should match"
        );
        assert_eq!(
            decoded.schema,
            Some(SchemaMeta {
                subject: "orders-value".to_string(),
                id: 5,
                format: SchemaFormat::Avro,
            }),
            "complete decoded value should match"
        );
        assert_eq!(actual, expected, "complete decoded value should match");
    }

    #[tokio::test]
    async fn structured_encode_by_pinned_id() {
        let codec = avro_codec(false);
        let json = Bytes::from_static(br#"{"id": 7, "name": "b"}"#);
        let out = codec
            .encode(
                "orders",
                EncodeBody::Structured {
                    json,
                    schema: SchemaSelector {
                        subject: None,
                        id: Some(5),
                        format: SchemaFormat::Avro,
                    },
                },
            )
            .await
            .unwrap();
        assert_eq!(&out[..5], &[0x00, 0x00, 0x00, 0x00, 0x05]);
    }

    #[tokio::test]
    async fn decode_unframed_values_pass_through() {
        for (name, value) in [
            ("non-magic", Bytes::from_static(b"\x01not-confluent-framed")),
            ("empty", Bytes::new()),
        ] {
            let codec = avro_codec(false);
            let decoded = codec.decode("orders", value.clone()).await.unwrap();
            assert_eq!(decoded.value, value, "case {name}");
            assert_eq!(decoded.schema, None, "case {name}");
            assert_eq!(decoded.json, None, "case {name}");
        }
    }
}
