//! `ConsumerGroupDescribe` (api_key 69) — returns one DescribedGroup per
//! requested group_id. Uses the actor's `Describe` view to render.

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
use crate::coordinator::next_gen::GroupType;
use crate::coordinator::next_gen::group_actor::GroupActorMessage;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let group_manager = broker.group_manager.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = ConsumerGroupDescribeRequest::decode(&mut cur, version)?;

        let mut described: Vec<DescribedGroup> = Vec::with_capacity(req.group_ids.len());
        let ng_opt = group_manager.next_gen().cloned();
        for group_id in &req.group_ids {
            let mut row = DescribedGroup {
                group_id: group_id.clone(),
                error_code: codes::NONE,
                ..Default::default()
            };
            let ng = match &ng_opt {
                Some(c) if c.config.next_gen_enabled() => c,
                _ => {
                    row.error_code = codes::GROUP_ID_NOT_FOUND;
                    described.push(row);
                    continue;
                }
            };
            if matches!(ng.group_type(group_id), Some(GroupType::Classic)) {
                row.error_code = codes::GROUP_ID_NOT_FOUND;
                described.push(row);
                continue;
            }
            let Some(handle) = ng.find(group_id) else {
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
            match rx.await {
                Ok(view) => {
                    row.group_state = match view.members.len() {
                        0 => "EMPTY".into(),
                        _ => "STABLE".into(),
                    };
                    described.push(row);
                }
                Err(_) => {
                    row.error_code = codes::UNKNOWN_SERVER_ERROR;
                    described.push(row);
                }
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
