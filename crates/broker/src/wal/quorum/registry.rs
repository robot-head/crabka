//! WAL shard registry.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crabka_ids::PartitionIndex;
use dashmap::DashMap;

use super::{
    engine::WalShardEngine,
    wire::{
        OFFSET_OUT_OF_RANGE, QuorumGroup, WalFetchRequest, decode_fetch, decode_fetch_request,
        encode_fetch_response_struct, fetch_response, unknown_shard_fetch_response,
    },
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
    placements: RwLock<HashMap<ShardId, Vec<crabka_raft::NodeId>>>,
}

impl WalShardRegistry {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert(&self, shard_id: ShardId, engine: Arc<WalShardEngine>) {
        self.engines.insert(shard_id, engine);
    }

    /// Atomically install the voter placement derived from one metadata image.
    /// Replacing the map also removes deleted topics and superseded topic IDs.
    pub(crate) fn replace_placements(
        &self,
        placements: HashMap<ShardId, Vec<crabka_raft::NodeId>>,
    ) {
        *self
            .placements
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = placements;
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn placement(&self, shard_id: ShardId) -> Option<Vec<crabka_raft::NodeId>> {
        self.placements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&shard_id)
            .cloned()
    }

    pub(crate) fn get(&self, shard_id: ShardId) -> Option<Arc<WalShardEngine>> {
        self.engines
            .get(&shard_id)
            .map(|entry| entry.value().clone())
    }

    pub(crate) fn remove(&self, shard_id: ShardId) -> Option<Arc<WalShardEngine>> {
        self.placements
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&shard_id);
        self.engines.remove(&shard_id).map(|(_, engine)| engine)
    }

    pub(crate) fn route_fetch_request(
        &self,
        request: &crabka_protocol::owned::fetch_request::FetchRequest,
    ) -> Option<Result<crabka_protocol::owned::fetch_response::FetchResponse, crate::BrokerError>>
    {
        self.route_decoded_fetch(decode_fetch_request(request)?)
    }

    fn route_decoded_fetch(
        &self,
        request: WalFetchRequest,
    ) -> Option<Result<crabka_protocol::owned::fetch_response::FetchResponse, crate::BrokerError>>
    {
        let QuorumGroup::DisklessWal {
            topic_id,
            partition,
        } = request.group
        else {
            return None;
        };
        let shard = ShardId {
            topic_id,
            partition,
        };
        let authorized = self
            .placements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&shard)
            .is_some_and(|voters| voters.contains(&request.from));
        if !authorized {
            return Some(Ok(unknown_shard_fetch_response(request.group)));
        }
        let Some(engine) = self.get(shard) else {
            return Some(Ok(unknown_shard_fetch_response(request.group)));
        };
        Some(
            engine
                .serve_fetch(crabka_ids::Offset(request.fetch_offset), request.max_size)
                .map(|fetch| {
                    let error_code = if fetch.offset_out_of_range {
                        OFFSET_OUT_OF_RANGE
                    } else {
                        0
                    };
                    fetch_response(
                        request.group,
                        fetch.high_watermark.0,
                        fetch.log_end_offset.0,
                        fetch.log_start_offset.0,
                        fetch.records,
                        error_code,
                    )
                }),
        )
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
            let Some(response) = self.registry.route_decoded_fetch(request) else {
                return Ok(None);
            };
            let response =
                response.map_err(|err| crabka_raft::RaftError::ChangeRejected(err.to_string()))?;
            Ok(Some(encode_fetch_response_struct(&response)))
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
        registry.replace_placements(HashMap::from([(
            ShardId {
                topic_id,
                partition,
            },
            vec![crabka_raft::NodeId(9)],
        )]));
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
        assert_eq!(partition.last_stable_offset, 1);
        assert_eq!(partition.log_start_offset, 0);
        assert!(
            partition
                .records
                .as_ref()
                .is_some_and(|records| records.payload_len() > 0)
        );
    }

    #[tokio::test]
    async fn wal_shard_router_reports_offset_out_of_range_with_log_bounds() {
        let dir = tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).unwrap(),
        ));
        {
            let mut log = source.lock().unwrap();
            for offset in 0..6 {
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
                log.append_at(&mut batch, Offset(offset)).unwrap();
            }
            log.sync().unwrap();
            log.trim_to_offset(Offset(5)).unwrap();
        }
        let engine = Arc::new(WalShardEngine::for_logs(BTreeMap::from([(
            crabka_raft::NodeId(1),
            source,
        )])));

        let registry = Arc::new(WalShardRegistry::new());
        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(18),
            partition: PartitionIndex(3),
        };
        registry.insert(shard, engine);
        registry.replace_placements(HashMap::from([(shard, vec![crabka_raft::NodeId(9)])]));
        let body = encode_fetch_for_group(
            QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
            crabka_raft::NodeId(9),
            0,
            4,
        );

        let router = WalShardRouter::new(registry);
        let response = router
            .route(crabka_raft::kraft::transport::api_key::FETCH, body)
            .await
            .unwrap()
            .unwrap();
        let decoded = FetchResponse::decode(&mut response.as_ref(), 17).unwrap();
        let partition = &decoded.responses[0].partitions[0];
        assert_eq!(partition.error_code, OFFSET_OUT_OF_RANGE);
        assert_eq!(partition.log_start_offset, 5);
        assert_eq!(partition.last_stable_offset, 6);
        assert!(partition.records.is_none());

        let body = encode_fetch_for_group(
            QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
            crabka_raft::NodeId(9),
            0,
            7,
        );
        let response = router
            .route(crabka_raft::kraft::transport::api_key::FETCH, body)
            .await
            .unwrap()
            .unwrap();
        let decoded = FetchResponse::decode(&mut response.as_ref(), 17).unwrap();
        let partition = &decoded.responses[0].partitions[0];
        assert_eq!(partition.error_code, OFFSET_OUT_OF_RANGE);
        assert_eq!(partition.log_start_offset, 5);
        assert_eq!(partition.last_stable_offset, 6);
        assert!(partition.records.is_none());
    }

    #[tokio::test]
    async fn wal_shard_router_rejects_a_broker_outside_the_placement() {
        let registry = Arc::new(WalShardRegistry::new());
        let topic_id = uuid::Uuid::from_u128(17);
        let partition = PartitionIndex(2);
        let shard = ShardId {
            topic_id,
            partition,
        };
        let dir = tempdir().unwrap();
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).unwrap(),
        ));
        registry.insert(
            shard,
            Arc::new(WalShardEngine::for_logs(BTreeMap::from([(
                crabka_raft::NodeId(1),
                log,
            )]))),
        );
        registry.replace_placements(HashMap::from([(shard, vec![crabka_raft::NodeId(2)])]));
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
        assert_eq!(partition.error_code, 3);
        assert!(partition.records.is_none());
    }

    #[test]
    fn replacing_placements_removes_stale_shards() {
        let registry = WalShardRegistry::new();
        let stale = ShardId {
            topic_id: uuid::Uuid::from_u128(1),
            partition: PartitionIndex(0),
        };
        let current = ShardId {
            topic_id: uuid::Uuid::from_u128(2),
            partition: PartitionIndex(0),
        };
        registry.replace_placements(HashMap::from([(stale, vec![crabka_raft::NodeId(1)])]));

        registry.replace_placements(HashMap::from([(current, vec![crabka_raft::NodeId(2)])]));

        assert!(registry.placement(stale).is_none());
        assert_eq!(
            registry.placement(current),
            Some(vec![crabka_raft::NodeId(2)])
        );
    }
}
