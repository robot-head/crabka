//! WAL shard registry.

use std::sync::Arc;

use crabka_ids::PartitionIndex;
use dashmap::DashMap;

use super::{
    engine::WalShardEngine,
    wire::{QuorumGroup, decode_fetch, encode_fetch_response, encode_unknown_shard_fetch_response},
};

/// Per-partition WAL shard identity for Slice 6a.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ShardId {
    pub(crate) topic_id: uuid::Uuid,
    pub(crate) partition: PartitionIndex,
}

/// Registry from shard identity to its in-process WAL engine.
#[derive(Debug, Default)]
pub(crate) struct WalShardRegistry {
    engines: DashMap<ShardId, Arc<WalShardEngine>>,
}

impl WalShardRegistry {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert(&self, shard_id: ShardId, engine: Arc<WalShardEngine>) {
        self.engines.insert(shard_id, engine);
    }

    pub(crate) fn get(&self, shard_id: ShardId) -> Option<Arc<WalShardEngine>> {
        self.engines
            .get(&shard_id)
            .map(|entry| entry.value().clone())
    }

    pub(crate) fn remove(&self, shard_id: ShardId) -> Option<Arc<WalShardEngine>> {
        self.engines.remove(&shard_id).map(|(_, engine)| engine)
    }
}

/// Routes shard-addressed KIP-595 Fetch requests to the registered diskless
/// WAL engines.
#[derive(Debug, Clone)]
pub(crate) struct WalShardRouter {
    registry: Arc<WalShardRegistry>,
}

impl WalShardRouter {
    #[must_use]
    pub(crate) fn new(registry: Arc<WalShardRegistry>) -> Self {
        Self { registry }
    }
}

impl crabka_raft::RaftShardRouter for WalShardRouter {
    fn route(&self, api_key: i16, body: bytes::Bytes) -> crabka_raft::ShardRouteFuture<'_> {
        Box::pin(async move {
            if api_key != crabka_raft::kraft::transport::api_key::FETCH {
                return Ok(None);
            }
            let Some(request) = decode_fetch(&body) else {
                return Ok(None);
            };
            let QuorumGroup::DisklessWal {
                topic_id,
                partition,
            } = request.group
            else {
                return Ok(None);
            };
            let shard = ShardId {
                topic_id,
                partition,
            };
            let Some(engine) = self.registry.get(shard) else {
                return Ok(Some(encode_unknown_shard_fetch_response(request.group)));
            };
            let (hwm, records) = engine
                .serve_fetch(crabka_ids::Offset(request.fetch_offset), request.max_size)
                .map_err(|err| crabka_raft::RaftError::ChangeRejected(err.to_string()))?;
            Ok(Some(encode_fetch_response(request.group, hwm.0, records)))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Mutex};

    use bytes::Bytes;
    use crabka_ids::Offset;
    use crabka_log::{Log, LogConfig};
    use crabka_protocol::{
        Decode,
        owned::fetch_response::FetchResponse,
        records::{Record, RecordBatch},
    };
    use crabka_raft::RaftShardRouter;
    use tempfile::tempdir;

    use super::*;
    use crate::wal::quorum::{engine::WalShardEngine, wire::encode_fetch_for_group};

    #[tokio::test]
    async fn wal_shard_router_serves_registered_fetch() {
        let dir = tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).unwrap(),
        ));
        let mut batch = RecordBatch {
            records: vec![Record {
                attributes: 0,
                offset_delta: 0,
                timestamp_delta: 0,
                key: None,
                value: Some(Bytes::from_static(b"a")),
                headers: vec![],
            }],
            ..Default::default()
        };
        source
            .lock()
            .unwrap()
            .append_at(&mut batch, Offset(0))
            .unwrap();
        source.lock().unwrap().sync().unwrap();
        let engine = Arc::new(WalShardEngine::for_logs(BTreeMap::from([(
            crabka_raft::NodeId(1),
            source.clone(),
        )])));
        engine.replicate_and_sync(&source, Offset(1)).await.unwrap();

        let registry = Arc::new(WalShardRegistry::new());
        let topic_id = uuid::Uuid::from_u128(17);
        let partition = PartitionIndex(2);
        registry.insert(
            ShardId {
                topic_id,
                partition,
            },
            engine,
        );
        let router = WalShardRouter::new(registry);
        let body = encode_fetch_for_group(
            QuorumGroup::diskless_wal(topic_id, partition),
            crabka_raft::NodeId(9),
            0,
            0,
        );

        let response = router
            .route(crabka_raft::kraft::transport::api_key::FETCH, body)
            .await
            .unwrap()
            .expect("diskless WAL fetch response");
        let decoded = FetchResponse::decode(&mut response.as_ref(), 17).unwrap();
        let partition = &decoded.responses[0].partitions[0];
        assert_eq!(partition.high_watermark, 1);
        assert!(
            partition
                .records
                .as_ref()
                .is_some_and(|records| records.payload_len() > 0)
        );
    }
}
