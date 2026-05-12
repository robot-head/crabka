//! `ApiVersions` (`api_key=18`). Returns the (min, max) supported version
//! range for every API key this broker handles.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::Decode;
use crabka_protocol::Encode;
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

/// Static table mirrored from each API's generated `MIN_VERSION`/`MAX_VERSION`
/// constants. Update this when adding a handler.
fn supported_apis() -> Vec<ApiVersion> {
    use crabka_protocol::owned;
    macro_rules! v {
        ($mod:ident) => {
            ApiVersion {
                api_key: owned::$mod::API_KEY,
                min_version: owned::$mod::MIN_VERSION,
                max_version: owned::$mod::MAX_VERSION,
                ..Default::default()
            }
        };
    }
    vec![
        v!(api_versions_request),
        v!(produce_request),
        v!(fetch_request),
        v!(list_offsets_request),
        v!(metadata_request),
        v!(find_coordinator_request),
        v!(join_group_request),
        v!(sync_group_request),
        v!(heartbeat_request),
        v!(leave_group_request),
        // OffsetCommit and OffsetFetch: MVP only handles the legacy
        // single-group / name-keyed shape. v8+ (OffsetFetch) and v10+
        // (OffsetCommit) switch to topic_id / per-group arrays which
        // require a topic-id index this slice doesn't wire up. Cap the
        // advertised max so clients negotiate down to a version we can
        // serve cleanly.
        ApiVersion {
            api_key: owned::offset_commit_request::API_KEY,
            min_version: owned::offset_commit_request::MIN_VERSION,
            max_version: 9,
            ..Default::default()
        },
        ApiVersion {
            api_key: owned::offset_fetch_request::API_KEY,
            min_version: owned::offset_fetch_request::MIN_VERSION,
            max_version: 7,
            ..Default::default()
        },
        v!(create_topics_request),
        v!(delete_topics_request),
        v!(init_producer_id_request),
        v!(add_partitions_to_txn_request),
        v!(add_offsets_to_txn_request),
        v!(end_txn_request),
        v!(write_txn_markers_request),
        v!(txn_offset_commit_request),
        v!(describe_configs_request),
    ]
}

pub(crate) fn handle(
    _broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let _req = ApiVersionsRequest::decode(&mut cur, version)?;

        let resp = ApiVersionsResponse {
            error_code: codes::NONE,
            api_keys: supported_apis(),
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
