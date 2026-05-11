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
        v!(create_topics_request),
        v!(delete_topics_request),
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
