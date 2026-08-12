//! Messaging module for publish, `CloudEvents` publish, and subscribe.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_util::Stream;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::{
    client::{CrabkaClient, MockMessage},
    error::CrabkaError,
    pb,
};

const SUBSCRIPTION_CHANNEL_CAPACITY: usize = 32;

/// Messaging module client.
#[derive(Debug, Clone)]
pub struct MessagingClient {
    client: CrabkaClient,
}

impl MessagingClient {
    pub(crate) fn new(client: CrabkaClient) -> Self {
        Self { client }
    }

    /// Publish one raw record.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, transport failures, or a gateway
    /// record error.
    pub async fn publish(
        &self,
        topic: impl Into<String>,
        value: impl Into<Bytes>,
        options: PublishOptions,
    ) -> Result<RecordResult, CrabkaError> {
        let topic = topic.into();
        let value = value.into();
        if self.client.is_mock() {
            return self.publish_mock(topic, value, options);
        }
        if self.client.is_unreachable() {
            return Err(CrabkaError::Transport("endpoint unreachable".into()));
        }
        if topic.is_empty() {
            return Err(CrabkaError::InvalidArgument("topic is required".into()));
        }
        let request = pb::SendRequest {
            records: vec![pb::Record {
                topic,
                key: options.key.map(Vec::from),
                body: Some(pb::record::Body::Raw(value.to_vec())),
                headers: headers_to_pb(options.headers),
                partition: options.partition,
                timestamp_ms: None,
                idempotency_key: options.idempotency_key,
                schema: None,
            }],
            acks: pb::Acks::All as i32,
        };
        let response: pb::SendResponse = self
            .client
            .inner
            .connect
            .unary("/crabka.gateway.v1.Gateway/Send", &request)
            .await?;
        response
            .results
            .into_iter()
            .next()
            .map(record_result_from_pb)
            .transpose()?
            .ok_or_else(|| CrabkaError::ServerError("gateway returned no record result".into()))
    }

    /// Publish a `CloudEvent` using the contract's binary-mode header mapping.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, transport failures, or a gateway
    /// record error.
    pub async fn publish_event(
        &self,
        topic: impl Into<String>,
        event: CloudEvent,
    ) -> Result<RecordResult, CrabkaError> {
        if event.id.is_empty() {
            return Err(CrabkaError::InvalidArgument(
                "CloudEvent id is required".into(),
            ));
        }
        let headers = event.headers();
        self.publish(
            topic,
            event.data,
            PublishOptions {
                headers,
                ..PublishOptions::default()
            },
        )
        .await
    }

    /// Subscribe to topics.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid topic or group input, an unreachable
    /// endpoint, or unavailable mock state.
    pub fn subscribe(
        &self,
        topics: &[String],
        group: impl Into<String>,
        filter: Option<&Filter>,
    ) -> Result<MessageStream, CrabkaError> {
        let group = group.into();
        let gateway_filter = filter.map(Filter::to_gateway_filter).transpose()?;
        if !self.client.is_mock() {
            return self.subscribe_live(topics, group, gateway_filter);
        }
        let messages = self
            .client
            .inner
            .mock
            .lock()
            .map_err(|_| CrabkaError::ServerError("mock state lock poisoned".into()))?
            .messages
            .clone();
        let (tx, rx) = mpsc::channel(messages.len().max(1));
        for message in messages {
            if !topics.iter().any(|topic| topic == &message.topic) {
                continue;
            }
            if !filter_matches(filter, &message.value) {
                continue;
            }
            let inbound = Inbound {
                topic: message.topic,
                partition: message.partition,
                offset: message.offset,
                value: message.value,
                headers: message.headers.into_iter().collect(),
            };
            if tx.try_send(Ok(inbound)).is_err() {
                break;
            }
        }
        Ok(MessageStream { rx })
    }

    fn publish_mock(
        &self,
        topic: String,
        value: Bytes,
        options: PublishOptions,
    ) -> Result<RecordResult, CrabkaError> {
        if topic.is_empty() {
            return Err(CrabkaError::InvalidArgument("topic is required".into()));
        }
        if topic == "__missing_topic" {
            return Err(CrabkaError::NotFound("topic not found".into()));
        }
        let mut state = self
            .client
            .inner
            .mock
            .lock()
            .expect("mock state lock not poisoned");
        let offset = state
            .messages
            .iter()
            .filter(|message| message.topic == topic)
            .count()
            .try_into()
            .expect("message count fits i64");
        state.messages.push(MockMessage {
            topic,
            partition: 0,
            offset,
            value,
            headers: options.headers,
        });
        Ok(RecordResult {
            partition: 0,
            offset,
            deduplicated: false,
        })
    }

    fn subscribe_live(
        &self,
        topics: &[String],
        group: String,
        filter: Option<String>,
    ) -> Result<MessageStream, CrabkaError> {
        if self.client.is_unreachable() {
            return Err(CrabkaError::Transport("endpoint unreachable".into()));
        }
        if topics.is_empty() {
            return Err(CrabkaError::InvalidArgument(
                "at least one topic is required".into(),
            ));
        }
        if group.is_empty() {
            return Err(CrabkaError::InvalidArgument("group is required".into()));
        }

        let request = pb::SubscribeFrame {
            frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
                group_id: group,
                topics: topics.to_vec(),
                auto_commit: true,
                filter: filter.unwrap_or_default(),
            })),
        };
        let connect = self.client.inner.connect.clone();
        let (tx, rx) = mpsc::channel(SUBSCRIPTION_CHANNEL_CAPACITY);
        tokio::spawn(async move {
            let mut inbound = match connect
                .streaming::<_, pb::Inbound>("/crabka.gateway.v1.Gateway/Subscribe", &request)
                .await
            {
                Ok(inbound) => inbound,
                Err(error) => {
                    let _ = tx.send(Err(CrabkaError::from(error))).await;
                    return;
                }
            };
            while let Some(message) = inbound.recv().await {
                let mapped = message.map(inbound_from_pb).map_err(CrabkaError::from);
                if tx.send(mapped).await.is_err() {
                    return;
                }
            }
        });
        Ok(MessageStream { rx })
    }
}

/// Publish options for raw records.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PublishOptions {
    /// Optional record key.
    pub key: Option<Bytes>,
    /// Headers in publish order. Header keys may repeat and values may be null.
    pub headers: Vec<(String, Option<Bytes>)>,
    /// Optional partition.
    pub partition: Option<i32>,
    /// Optional idempotency key.
    pub idempotency_key: Option<String>,
}

/// Publish result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordResult {
    /// Partition index.
    pub partition: i32,
    /// Record offset.
    pub offset: i64,
    /// Whether the gateway deduplicated the record.
    pub deduplicated: bool,
}

/// `CloudEvent` binary-mode payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudEvent {
    /// Event id.
    pub id: String,
    /// Event source.
    pub source: String,
    /// Event type.
    pub type_: String,
    /// Specversion, usually `1.0`.
    pub specversion: String,
    /// Optional data content type.
    pub datacontenttype: Option<String>,
    /// Raw event data.
    pub data: Bytes,
}

impl CloudEvent {
    fn headers(&self) -> Vec<(String, Option<Bytes>)> {
        let mut headers = vec![
            ("ce_id".into(), Some(Bytes::from(self.id.clone()))),
            ("ce_source".into(), Some(Bytes::from(self.source.clone()))),
            ("ce_type".into(), Some(Bytes::from(self.type_.clone()))),
            (
                "ce_specversion".into(),
                Some(Bytes::from(self.specversion.clone())),
            ),
        ];
        if let Some(datacontenttype) = &self.datacontenttype {
            headers.push((
                "content-type".into(),
                Some(Bytes::from(datacontenttype.clone())),
            ));
        }
        headers
    }
}

/// Equality filter for structured JSON values.
#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    /// JSON field path. v1 supports `$.field`.
    pub path: String,
    /// Expected scalar value.
    pub value: Value,
}

impl Filter {
    fn to_gateway_filter(&self) -> Result<String, CrabkaError> {
        let Some(field) = self.path.strip_prefix("$.") else {
            return Err(CrabkaError::InvalidArgument(
                "filter path must start with $.".into(),
            ));
        };
        if field.is_empty() || !field.split('.').all(valid_filter_segment) {
            return Err(CrabkaError::InvalidArgument(
                "filter path must contain identifier segments".into(),
            ));
        }
        Ok(format!(
            "{field} = {}",
            gateway_filter_literal(&self.value)?
        ))
    }
}

/// Delivered message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inbound {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Record offset.
    pub offset: i64,
    /// Record value.
    pub value: Bytes,
    /// Headers in deterministic order.
    pub headers: Vec<(String, Option<Bytes>)>,
}

/// Subscription stream.
#[derive(Debug)]
pub struct MessageStream {
    rx: mpsc::Receiver<Result<Inbound, CrabkaError>>,
}

impl Stream for MessageStream {
    type Item = Result<Inbound, CrabkaError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

fn headers_to_pb(headers: Vec<(String, Option<Bytes>)>) -> Vec<pb::Header> {
    headers
        .into_iter()
        .map(|(key, value)| pb::Header {
            key,
            value: value.map(Vec::from),
        })
        .collect()
}

fn inbound_from_pb(inbound: pb::Inbound) -> Inbound {
    Inbound {
        topic: inbound.topic,
        partition: inbound.partition,
        offset: inbound.offset,
        value: Bytes::from(inbound.value),
        headers: inbound
            .headers
            .into_iter()
            .map(|header| (header.key, header.value.map(Bytes::from)))
            .collect(),
    }
}

fn gateway_filter_literal(value: &Value) -> Result<String, CrabkaError> {
    match value {
        Value::String(value) => Ok(format!("'{}'", value.replace('\'', "''"))),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Null => Ok("null".into()),
        Value::Array(_) | Value::Object(_) => Err(CrabkaError::InvalidArgument(
            "filter value must be a string, number, boolean, or null".into(),
        )),
    }
}

fn valid_filter_segment(segment: &str) -> bool {
    let mut chars = segment.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

fn record_result_from_pb(result: pb::RecordResult) -> Result<RecordResult, CrabkaError> {
    if let Some(error) = result.error {
        return Err(CrabkaError::from_connect_code(
            match error.code {
                3 => "invalid_argument",
                5 => "not_found",
                7 => "unauthenticated",
                14 => "unavailable",
                _ => "internal",
            },
            error.message,
        ));
    }
    Ok(RecordResult {
        partition: result.partition,
        offset: result.offset,
        deduplicated: result.deduplicated,
    })
}

fn filter_matches(filter: Option<&Filter>, value: &Bytes) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    let Ok(json) = serde_json::from_slice::<Value>(value) else {
        return false;
    };
    let Some(field) = filter.path.strip_prefix("$.") else {
        return false;
    };
    field
        .split('.')
        .try_fold(&json, |value, segment| value.get(segment))
        == Some(&filter.value)
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt as _;

    use super::*;

    #[test]
    fn cloudevent_binary_mode_headers_are_contract_shaped() {
        let event = CloudEvent {
            id: "1".into(),
            source: "/svc".into(),
            type_: "order.created".into(),
            specversion: "1.0".into(),
            datacontenttype: Some("application/json".into()),
            data: Bytes::from_static(br#"{"n":7}"#),
        };

        let headers = event.headers();

        assert!(
            headers
                .iter()
                .any(|(k, v)| k == "ce_id" && v.as_deref() == Some(&b"1"[..]))
        );
        assert!(
            headers
                .iter()
                .any(|(k, v)| k == "ce_source" && v.as_deref() == Some(&b"/svc"[..]))
        );
        assert!(
            headers
                .iter()
                .any(|(k, v)| k == "ce_type" && v.as_deref() == Some(&b"order.created"[..]))
        );
        assert!(
            headers
                .iter()
                .any(|(k, v)| k == "ce_specversion" && v.as_deref() == Some(&b"1.0"[..]))
        );
        assert!(
            headers
                .iter()
                .any(|(k, v)| k == "content-type"
                    && v.as_deref() == Some(&b"application/json"[..]))
        );
        assert!(!headers.iter().any(|(k, _)| k == "ce_datacontenttype"));
    }

    #[test]
    fn headers_to_pb_preserves_order_duplicates_and_nulls() {
        let headers = headers_to_pb(vec![
            ("x".into(), Some(Bytes::from_static(b"first"))),
            ("x".into(), None),
            ("y".into(), Some(Bytes::from_static(b"last"))),
        ]);

        assert_eq!(
            headers,
            vec![
                pb::Header {
                    key: "x".into(),
                    value: Some(b"first".to_vec()),
                },
                pb::Header {
                    key: "x".into(),
                    value: None,
                },
                pb::Header {
                    key: "y".into(),
                    value: Some(b"last".to_vec()),
                },
            ]
        );
    }

    #[tokio::test]
    async fn mock_subscribe_filters_matching_records() {
        let client = CrabkaClient::new("mock://gateway").unwrap();
        let messaging = client.messaging();
        messaging
            .publish(
                "orders",
                Bytes::from_static(br#"{"kind":"skip"}"#),
                PublishOptions::default(),
            )
            .await
            .unwrap();
        messaging
            .publish(
                "orders",
                Bytes::from_static(br#"{"kind":"keep"}"#),
                PublishOptions::default(),
            )
            .await
            .unwrap();

        let mut stream = messaging
            .subscribe(
                &["orders".into()],
                "g",
                Some(&Filter {
                    path: "$.kind".into(),
                    value: Value::String("keep".into()),
                }),
            )
            .unwrap();
        let msg = stream.next().await.unwrap().unwrap();

        assert_eq!(msg.offset, 1);
        assert_eq!(msg.value.as_ref(), br#"{"kind":"keep"}"#);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn subscription_inbox_applies_backpressure_at_capacity() {
        let (tx, rx) = mpsc::channel(SUBSCRIPTION_CHANNEL_CAPACITY);
        let message = Inbound {
            topic: "orders".into(),
            partition: 0,
            offset: 0,
            value: Bytes::new(),
            headers: Vec::new(),
        };
        for _ in 0..SUBSCRIPTION_CHANNEL_CAPACITY {
            tx.try_send(Ok(message.clone())).expect("inbox has room");
        }
        assert2::assert!(matches!(
            tx.try_send(Ok(message.clone())),
            Err(mpsc::error::TrySendError::Full(_))
        ));

        let mut stream = MessageStream { rx };
        assert2::assert!(stream.next().await.is_some());
        tx.try_send(Ok(message))
            .expect("draining one item restores one slot");
    }

    #[test]
    fn gateway_filter_uses_sql_standard_string_literals() {
        let filter = Filter {
            path: "$.path".into(),
            value: Value::String("C:\\tmp\\O'Brien".into()),
        };

        assert_eq!(
            filter.to_gateway_filter().unwrap(),
            "path = 'C:\\tmp\\O''Brien'"
        );
    }

    #[tokio::test]
    async fn publish_event_rejects_empty_id() {
        let client = CrabkaClient::new("mock://gateway").unwrap();
        let error = client
            .messaging()
            .publish_event(
                "events",
                CloudEvent {
                    id: String::new(),
                    source: "/svc".into(),
                    type_: "created".into(),
                    specversion: "1.0".into(),
                    datacontenttype: None,
                    data: Bytes::new(),
                },
            )
            .await
            .unwrap_err();

        assert_eq!(
            error,
            CrabkaError::InvalidArgument("CloudEvent id is required".into())
        );
    }

    #[test]
    fn filter_rejects_malformed_paths_and_nonscalar_values() {
        for filter in [
            Filter {
                path: "kind".into(),
                value: Value::Bool(true),
            },
            Filter {
                path: "$.kind;drop".into(),
                value: Value::Bool(true),
            },
            Filter {
                path: "$.kind".into(),
                value: Value::Array(Vec::new()),
            },
        ] {
            assert!(filter.to_gateway_filter().is_err());
        }
    }
}
