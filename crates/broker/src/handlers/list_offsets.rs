//! `ListOffsets` (`api_key=2`). Resolves the EARLIEST / LATEST sentinels
//! using each partition's log. Any other timestamp returns -1
//! (real timestamp index lookups are out of MVP scope).

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::list_offsets_request::ListOffsetsRequest;
use crabka_protocol::owned::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

const EARLIEST: i64 = -2;
const LATEST: i64 = -1;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let partitions = broker.partitions.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = ListOffsetsRequest::decode(&mut cur, version)?;

        let mut topics_out: Vec<ListOffsetsTopicResponse> = Vec::with_capacity(req.topics.len());
        for topic in req.topics {
            let mut parts_out: Vec<ListOffsetsPartitionResponse> =
                Vec::with_capacity(topic.partitions.len());
            for part in topic.partitions {
                let idx = part.partition_index;
                let mut out = ListOffsetsPartitionResponse {
                    partition_index: idx,
                    timestamp: -1,
                    ..Default::default()
                };

                let Some(p) = partitions.get(&(topic.name.clone(), idx)) else {
                    out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                    parts_out.push(out);
                    continue;
                };

                let offset = match part.timestamp {
                    EARLIEST => {
                        let log = p.log.lock().expect("log mutex poisoned");
                        log.log_start_offset()
                    }
                    LATEST => {
                        let log = p.log.lock().expect("log mutex poisoned");
                        log.log_end_offset()
                    }
                    _ => -1,
                };

                out.error_code = codes::NONE;
                out.offset = offset;
                parts_out.push(out);
            }
            topics_out.push(ListOffsetsTopicResponse {
                name: topic.name,
                partitions: parts_out,
                ..Default::default()
            });
        }

        let resp = ListOffsetsResponse {
            throttle_time_ms: 0,
            topics: topics_out,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
