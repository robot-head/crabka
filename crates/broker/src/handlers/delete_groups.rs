//! `DeleteGroups` (`api_key=42`). Drops empty groups from the in-memory
//! registry. Non-empty groups are rejected with `NON_EMPTY_GROUP`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::delete_groups_request::DeleteGroupsRequest;
use crabka_protocol::owned::delete_groups_response::{DeletableGroupResult, DeleteGroupsResponse};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::DeleteGroupError;
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
        let req = DeleteGroupsRequest::decode(&mut cur, version)?;

        let mut results: Vec<DeletableGroupResult> = Vec::with_capacity(req.groups_names.len());
        for gid in req.groups_names {
            let error_code = match group_manager.delete_group(&gid).await {
                Ok(()) => codes::NONE,
                Err(DeleteGroupError::NotFound) => codes::GROUP_ID_NOT_FOUND,
                Err(DeleteGroupError::NonEmpty) => codes::NON_EMPTY_GROUP,
            };
            results.push(DeletableGroupResult {
                group_id: gid,
                error_code,
                ..Default::default()
            });
        }

        let resp = DeleteGroupsResponse {
            results,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
