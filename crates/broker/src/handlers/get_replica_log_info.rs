//! `GetReplicaLogInfo` (`api_key` 93, KIP-966). Inter-broker RPC: the
//! controller asks this broker for the log-end-offset and last-written
//! leader epoch of partitions it hosts, to drive offset-aware unclean
//! recovery. Served on the inter-broker listener via the handler table.
//!
//! For each requested partition the broker hosts locally we answer with
//! the local LEO + cached leader epoch. Partitions not hosted here get
//! `REPLICA_NOT_AVAILABLE (11)` with sentinel offsets (`-1`), matching
//! the JVM behaviour for a replica the broker isn't a member of.

use std::sync::atomic::Ordering;

use bytes::Bytes;
use futures_util::future::BoxFuture;

use crabka_protocol::owned::get_replica_log_info_request::GetReplicaLogInfoRequest;
use crabka_protocol::owned::get_replica_log_info_response::{
    GetReplicaLogInfoResponse, PartitionLogInfo, TopicPartitionLogInfo,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let image = broker.controller.current_image();
    let mut topic_results: Vec<TopicPartitionLogInfo> = Vec::new();

    let mut cur: &[u8] = req_bytes;
    if let Ok(req) = GetReplicaLogInfoRequest::decode(&mut cur, version) {
        for tp in &req.topic_partitions {
            // The protocol `topic_id` is the `[u8; 16]` wire newtype; the
            // metadata image stores the external `uuid::Uuid`. Match on the
            // raw bytes (mirrors the lookup in `handlers/metadata.rs`).
            let topic_name = image
                .topics()
                .find(|t| t.topic_id.into_bytes() == tp.topic_id.0)
                .map(|t| t.name.clone());

            let mut partition_log_info = Vec::with_capacity(tp.partitions.len());
            for &p in &tp.partitions {
                let hosted = topic_name
                    .as_deref()
                    .and_then(|name| broker.partitions.get(name, p));
                partition_log_info.push(match hosted {
                    Some(part) => {
                        let epoch = part.current_leader_epoch.load(Ordering::Acquire);
                        PartitionLogInfo {
                            partition: p,
                            last_written_leader_epoch: epoch,
                            current_leader_epoch: epoch,
                            log_end_offset: part.log_end_offset(),
                            error_code: codes::NONE,
                            error_message: None,
                            ..Default::default()
                        }
                    }
                    None => PartitionLogInfo {
                        partition: p,
                        last_written_leader_epoch: -1,
                        current_leader_epoch: -1,
                        log_end_offset: -1,
                        error_code: codes::REPLICA_NOT_AVAILABLE,
                        error_message: Some("partition not hosted locally".into()),
                        ..Default::default()
                    },
                });
            }

            topic_results.push(TopicPartitionLogInfo {
                topic_id: tp.topic_id,
                partition_log_info,
                ..Default::default()
            });
        }
    }

    let resp = GetReplicaLogInfoResponse {
        broker_epoch: 0,
        topic_partition_log_info_list: topic_results,
        ..Default::default()
    };

    Box::pin(async move {
        let mut body = Vec::new();
        resp.encode(&mut body, version)
            .map_err(|e| BrokerError::Replication(format!("encode GetReplicaLogInfo: {e}")))?;
        Ok(Bytes::from(body))
    })
}
