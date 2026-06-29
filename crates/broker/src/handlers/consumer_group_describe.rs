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
use crate::coordinator::unified::actor::GroupActorMessage;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let coordinator = broker.group_coordinator.clone();
    let image = broker.controller.current_image();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = ConsumerGroupDescribeRequest::decode(&mut cur, version)?;

        let mut described: Vec<DescribedGroup> = Vec::with_capacity(req.group_ids.len());
        let next_gen_enabled = coordinator.config.next_gen_enabled();
        for group_id in &req.group_ids {
            let mut row = ok_row(group_id);
            // KIP-848 / KIP-584: next-gen describe requires finalized
            // group.version >= 1; below that — including UNFINALIZED, which
            // means disabled — reject (consistent with the heartbeat fallback).
            if group_version_disabled(&image) {
                row.error_code = codes::UNSUPPORTED_VERSION;
                described.push(row);
                continue;
            }
            if next_gen_config_disabled(next_gen_enabled) {
                row.error_code = codes::GROUP_ID_NOT_FOUND;
                described.push(row);
                continue;
            }
            // Only next-gen (consumer) groups are described here; a classic
            // group (or an unknown id) is GROUP_ID_NOT_FOUND. The `Describe` arm
            // dispatches on the actor's LIVE `group.kind`: it replies ONLY for a
            // consumer-kind group and drops the sender otherwise, so an UPGRADED
            // group (spawned classic, now consumer in place via KIP-848) is
            // reachable while a classic group's no-reply maps to
            // GROUP_ID_NOT_FOUND — without consulting the stale spawn-time
            // `h.kind`.
            let Some(handle) = coordinator.find(group_id) else {
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
                row.group_state = group_state_for_member_count(view.members.len());
                described.push(row);
            } else {
                // No reply means the live group is classic (not describable via
                // api 69), which surfaces as GROUP_ID_NOT_FOUND — matching the
                // pre-refactor behavior for a classic group.
                row.error_code = codes::GROUP_ID_NOT_FOUND;
                described.push(row);
            }
        }
        let resp = response(described);
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

fn ok_row(group_id: &str) -> DescribedGroup {
    DescribedGroup {
        group_id: group_id.into(),
        error_code: codes::NONE,
        ..Default::default()
    }
}

fn group_version_disabled(image: &crabka_metadata::MetadataImage) -> bool {
    !crate::features::feature_enabled(
        image,
        crabka_metadata::group_version::GROUP_VERSION_FEATURE,
        1,
    )
}

fn next_gen_config_disabled(next_gen_enabled: bool) -> bool {
    !next_gen_enabled
}

fn group_state_for_member_count(members: usize) -> String {
    match members {
        0 => "EMPTY".into(),
        _ => "STABLE".into(),
    }
}

fn response(groups: Vec<DescribedGroup>) -> ConsumerGroupDescribeResponse {
    ConsumerGroupDescribeResponse {
        groups,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_metadata::{FeatureLevelRecord, MetadataImage, MetadataRecord};

    const VERSION: i16 = crabka_protocol::owned::consumer_group_describe_request::MAX_VERSION;

    fn request(group_ids: Vec<&str>) -> Bytes {
        let req = ConsumerGroupDescribeRequest {
            group_ids: group_ids.into_iter().map(Into::into).collect(),
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(req.encoded_len(VERSION));
        req.encode(&mut buf, VERSION)
            .expect("encode ConsumerGroupDescribeRequest");
        buf.freeze()
    }

    fn decode_response(bytes: Bytes) -> ConsumerGroupDescribeResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp = ConsumerGroupDescribeResponse::decode(&mut cur, VERSION)
            .expect("decode ConsumerGroupDescribeResponse");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    async fn start_broker() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let cfg = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = crate::broker::Broker::start(cfg)
            .await
            .expect("start broker");
        (handle, dir)
    }

    fn image_with_group_version(level: i16) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: crabka_metadata::group_version::GROUP_VERSION_FEATURE.into(),
            level,
        }));
        image
    }

    #[test]
    fn ok_row_preserves_requested_group_id() {
        let row = ok_row("orders");
        assert!(row.group_id == "orders");
        assert!(row.error_code == codes::NONE);
    }

    #[test]
    fn group_version_gate_distinguishes_disabled_and_enabled_images() {
        let fresh = MetadataImage::new(uuid::Uuid::nil());
        assert!(group_version_disabled(&fresh));

        let enabled = image_with_group_version(1);
        assert!(!group_version_disabled(&enabled));

        let disabled = image_with_group_version(0);
        assert!(group_version_disabled(&disabled));
    }

    #[test]
    fn next_gen_config_gate_inverts_enabled_flag() {
        assert!(!next_gen_config_disabled(true));
        assert!(next_gen_config_disabled(false));
    }

    #[test]
    fn group_state_reflects_member_count() {
        assert!(group_state_for_member_count(0) == "EMPTY");
        assert!(group_state_for_member_count(1) == "STABLE");
        assert!(group_state_for_member_count(3) == "STABLE");
    }

    #[test]
    fn response_preserves_group_rows() {
        let mut first = ok_row("a");
        first.error_code = codes::GROUP_ID_NOT_FOUND;
        let mut second = ok_row("b");
        second.error_code = codes::UNSUPPORTED_VERSION;

        let resp = response(vec![first, second]);

        assert!(resp.groups.len() == 2, "{resp:?}");
        assert!(resp.groups[0].group_id == "a", "{resp:?}");
        assert!(resp.groups[0].error_code == codes::GROUP_ID_NOT_FOUND);
        assert!(resp.groups[1].group_id == "b", "{resp:?}");
        assert!(resp.groups[1].error_code == codes::UNSUPPORTED_VERSION);
    }

    #[tokio::test]
    async fn handle_unknown_group_preserves_requested_group_id() {
        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let req = request(vec!["missing-group"]);

        let bytes = handle(&broker, VERSION, 3, &req)
            .await
            .expect("ConsumerGroupDescribe handler");
        let resp = decode_response(bytes);

        assert!(resp.groups.len() == 1, "{resp:?}");
        assert!(resp.groups[0].group_id == "missing-group", "{resp:?}");
        assert!(resp.groups[0].error_code == codes::GROUP_ID_NOT_FOUND);

        broker_handle.shutdown().await;
    }
}
