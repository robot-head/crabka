//! Connector-framework SPI for Crabka.
//!
//! This crate defines the keystone traits that every connector — every CDC
//! source, every telemetry sink — builds on, plus the converter layer that
//! bridges typed payloads to the Kafka wire.
//!
//! - [`Source`] pulls records out of an external system one at a time
//!   ([`poll`](Source::poll)), snapshots its read position
//!   ([`checkpoint`](Source::checkpoint)), and restores it on restart
//!   ([`seek`](Source::seek)). Sources can optionally acknowledge a persisted
//!   checkpoint after sink commit with [`acknowledge`](Source::acknowledge).
//! - [`Sink`] pushes records into an external system in batches
//!   ([`put`](Sink::put)), makes them durable ([`flush`](Sink::flush)), and
//!   optionally gates writes behind a transaction for exactly-once delivery
//!   ([`begin`](Sink::begin) / [`commit`](Sink::commit) / [`abort`](Sink::abort)).
//! - [`Converter`] bridges a connector's typed payload `T` to and from wire
//!   [`bytes::Bytes`]. [`ByteIdentity`] is a byte-for-byte passthrough;
//!   [`SchemaConverter`] wraps the Confluent schema-registry serdes from
//!   `crabka-schema-serde`.
//!
//! - [`ConnectorRuntime`] is the embeddable, single-process driver that owns a
//!   `Source` + `Sink` pair and pipes records between them — polling, applying
//!   bounded backpressure, coordinating source checkpoints with sink commits
//!   through the transactional gate, and exposing pause/resume + graceful-drain
//!   lifecycle hooks. No Connect worker protocol, no REST.
//!
//! The transport traits mirror the streams runtime I/O traits
//! (`RecordFetcher` / `RecordProducer`) and the remote-storage
//! `RemoteStorageManager` SPI; the converter layer mirrors Kafka Connect's
//! `Converter`. All records carry an optional key, optional value (a `None`
//! value is a tombstone), optional Kafka destination routing, an optional
//! timestamp, and ordered headers — see [`ConnectRecord`].

pub mod config;
pub mod convert;
pub mod error;
pub mod ids;
pub mod record;
pub mod runtime;
pub mod sink;
pub mod source;

pub use config::{
    ConfigDef, ConfigError, ConfigKey, ConfigKind, ConfigResult, ConnectorConfig,
    EnvSecretResolver, FromResolvedValue, RawConfig, ResolveOptions, ResolvedConfig, SecretRef,
    SecretResolutionError, SecretResolver, SecretString,
};
pub use convert::{ByteIdentity, Converter, SchemaConverter};
#[cfg(feature = "derive")]
pub use crabka_connect_derive::ConnectorConfig;
pub use error::ConnectError;
pub use ids::{PartitionMap, PositionMap};
pub use record::{ConnectRecord, Header, OffsetMap, OffsetValue, SourceOffset};
pub use runtime::{
    CheckpointStore, ConnectorHandle, ConnectorRuntime, HasSink, HasSource,
    InMemoryCheckpointStore, NoSink, NoSource, RuntimeState,
};
#[doc(hidden)]
pub use serde_json as __serde_json;
pub use sink::Sink;
pub use source::Source;

#[cfg(test)]
mod tests {
    //! An end-to-end exercise of the SPI with in-memory fakes, proving the
    //! traits are object-safe and compose as the runtime would drive them.

    use async_trait::async_trait;
    use bytes::Bytes;

    use super::*;

    /// A finite in-memory source over `(key, value)` pairs that tracks its read
    /// position as the index into the backing vector.
    struct VecSource {
        records: Vec<(Bytes, Bytes)>,
        pos: usize,
    }

    #[async_trait]
    impl Source<Bytes, Bytes> for VecSource {
        async fn poll(&mut self) -> Result<Option<ConnectRecord<Bytes, Bytes>>, ConnectError> {
            let Some((k, v)) = self.records.get(self.pos).cloned() else {
                return Ok(None);
            };
            self.pos += 1;
            Ok(Some(ConnectRecord::new(Some(k), Some(v))))
        }

        fn checkpoint(&self) -> Option<SourceOffset> {
            if self.pos == 0 {
                return None;
            }
            let mut position = OffsetMap::new();
            let index = i64::try_from(self.pos).expect("test source position fits in i64");
            position.insert("index".into(), OffsetValue::Long(index));
            Some(SourceOffset::new(OffsetMap::new().into(), position.into()))
        }

        async fn seek(&mut self, offset: SourceOffset) -> Result<(), ConnectError> {
            match offset.position.get("index") {
                Some(OffsetValue::Long(i)) => {
                    self.pos = usize::try_from(*i)
                        .map_err(|_| ConnectError::Offset(format!("negative index {i}")))?;
                    Ok(())
                }
                _ => Err(ConnectError::Offset("missing `index` component".into())),
            }
        }
    }

    /// A transactional in-memory sink: `put` stages records, `commit` promotes
    /// the staged batch into the committed log, `abort` drops it.
    #[derive(Default)]
    struct VecSink {
        staged: Vec<ConnectRecord<Bytes, Bytes>>,
        committed: Vec<ConnectRecord<Bytes, Bytes>>,
    }

    #[async_trait]
    impl Sink<Bytes, Bytes> for VecSink {
        async fn put(
            &mut self,
            records: Vec<ConnectRecord<Bytes, Bytes>>,
        ) -> Result<(), ConnectError> {
            self.staged.extend(records);
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), ConnectError> {
            self.committed.append(&mut self.staged);
            Ok(())
        }

        fn supports_transactions(&self) -> bool {
            true
        }

        async fn abort(&mut self) -> Result<(), ConnectError> {
            self.staged.clear();
            Ok(())
        }
    }

    #[tokio::test]
    async fn source_drains_then_checkpoint_and_seek_resume() {
        use assert2::check;

        let mut src = VecSource {
            records: vec![
                (Bytes::from_static(b"k0"), Bytes::from_static(b"v0")),
                (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
            ],
            pos: 0,
        };
        check!(src.checkpoint().is_none());

        let first = src.poll().await.unwrap().unwrap();
        check!(first.key == Some(Bytes::from_static(b"k0")));
        let cp = src.checkpoint().expect("position after one poll");

        // A fresh source seeks to the checkpoint and resumes at the next record.
        let mut resumed = VecSource {
            records: src.records.clone(),
            pos: 0,
        };
        resumed.seek(cp).await.unwrap();
        let next = resumed.poll().await.unwrap().unwrap();
        check!(next.key == Some(Bytes::from_static(b"k1")));
        // Drained: further polls report caught-up, not an error.
        check!(resumed.poll().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn sink_commit_promotes_and_abort_discards() {
        use assert2::check;

        let mut sink = VecSink::default();
        check!(sink.supports_transactions());

        // A committed interval lands in the log.
        sink.begin().await.unwrap();
        sink.put(vec![ConnectRecord::new(
            None,
            Some(Bytes::from_static(b"a")),
        )])
        .await
        .unwrap();
        sink.commit().await.unwrap();
        check!(sink.committed.len() == 1);

        // An aborted interval is discarded.
        sink.begin().await.unwrap();
        sink.put(vec![ConnectRecord::new(
            None,
            Some(Bytes::from_static(b"b")),
        )])
        .await
        .unwrap();
        sink.abort().await.unwrap();
        check!((sink.committed.len(), sink.staged.is_empty()) == (1, true));
    }

    #[tokio::test]
    async fn spi_is_object_safe() {
        use assert2::check;

        // The runtime holds connectors behind trait objects; confirm both
        // traits are dyn-compatible.
        let mut src: Box<dyn Source<Bytes, Bytes>> = Box::new(VecSource {
            records: vec![(Bytes::from_static(b"k"), Bytes::from_static(b"v"))],
            pos: 0,
        });
        let mut sink: Box<dyn Sink<Bytes, Bytes>> = Box::new(VecSink::default());

        while let Some(rec) = src.poll().await.unwrap() {
            sink.put(vec![rec]).await.unwrap();
        }
        sink.flush().await.unwrap();

        let conv: &dyn Converter<Bytes> = &ByteIdentity;
        let wire = conv.serialize("t", &Bytes::from_static(b"v")).unwrap();
        check!(wire == Bytes::from_static(b"v"));
    }

    #[tokio::test]
    async fn source_seek_rejects_unresumable_offset() {
        use assert2::check;

        let mut src = VecSource {
            records: vec![],
            pos: 0,
        };
        // An offset with no `index` component names no position to resume from.
        let missing = SourceOffset::default();
        // A negative index cannot map to a vector position.
        let mut position = OffsetMap::new();
        position.insert("index".into(), OffsetValue::Long(-1));
        let negative = SourceOffset::new(OffsetMap::new().into(), position.into());
        for (name, offset) in [("missing_index", missing), ("negative_index", negative)] {
            check!(src.seek(offset).await.is_err(), "case {name}");
        }
    }

    #[tokio::test]
    async fn source_and_sink_close_are_noops() {
        use assert2::check;

        let mut src = VecSource {
            records: vec![],
            pos: 0,
        };
        let source_close = src.close().await;
        let mut sink = VecSink::default();
        let sink_close = sink.close().await;
        check!((source_close.is_ok(), sink_close.is_ok()) == (true, true));
    }
}
