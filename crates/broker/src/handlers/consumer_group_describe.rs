//! `ConsumerGroupDescribe` (`api_key` 69) — returns one `DescribedGroup` per
//! requested `group_id`. Uses the actor's `Describe` view to render.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use tokio::sync::oneshot;

use crabka_protocol::owned::consumer_group_describe_request::ConsumerGroupDescribeRequest;
use crabka_protocol::owned::consumer_group_describe_response::{
    ConsumerGroupDescribeResponse, DescribedGroup,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::actor::{GroupActorMessage, GroupKindTag};
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let coordinator = broker.group_coordinator.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = ConsumerGroupDescribeRequest::decode(&mut cur, version)?;

        let mut described: Vec<DescribedGroup> = Vec::with_capacity(req.group_ids.len());
        let next_gen_enabled = coordinator.config.next_gen_enabled();
        for group_id in &req.group_ids {
            let mut row = DescribedGroup {
                group_id: group_id.clone(),
                error_code: codes::NONE,
                ..Default::default()
            };
            if !next_gen_enabled {
                row.error_code = codes::GROUP_ID_NOT_FOUND;
                described.push(row);
                continue;
            }
            // Only next-gen (consumer) groups are described here; a classic
            // group (or an unknown id) is GROUP_ID_NOT_FOUND.
            let handle = coordinator.find(group_id);
            let Some(handle) = handle.filter(|h| h.kind == GroupKindTag::Consumer) else {
                row.error_code = codes::GROUP_ID_NOT_FOUND;
                described.push(row);
                continue;
            };
            let (tx, rx) = oneshot::channel();
            if handle
                .tx
                .send(GroupActorMessage::Describe { reply: tx })
                .await
                .is_err()
            {
                row.error_code = codes::COORDINATOR_LOAD_IN_PROGRESS;
                described.push(row);
                continue;
            }
            if let Ok(view) = rx.await {
                row.group_state = match view.members.len() {
                    0 => "EMPTY".into(),
                    _ => "STABLE".into(),
                };
                described.push(row);
            } else {
                row.error_code = codes::UNKNOWN_SERVER_ERROR;
                described.push(row);
            }
        }
        let resp = ConsumerGroupDescribeResponse {
            groups: described,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
