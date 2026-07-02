use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use assert2::assert;
use bytes::BytesMut;
use crabka_client_admin::{AclEntryFilter, AdminClient};
use crabka_protocol::{
    Encode,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        describe_acls_request,
        describe_acls_response::DescribeAclsResponse,
    },
};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::{
    codec::{Framed, LengthDelimitedCodec},
    sync::CancellationToken,
};

fn kafka_frame(
    stream: tokio::net::TcpStream,
) -> Framed<tokio::net::TcpStream, LengthDelimitedCodec> {
    Framed::new(
        stream,
        LengthDelimitedCodec::builder()
            .length_field_offset(0)
            .length_field_length(4)
            .length_field_type::<u32>()
            .max_frame_length(100 * 1024 * 1024)
            .big_endian()
            .new_codec(),
    )
}

fn api_versions_response_v0() -> Vec<u8> {
    let response = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![
            ApiVersion {
                api_key: api_versions_request::API_KEY,
                min_version: 0,
                max_version: 3,
                ..Default::default()
            },
            ApiVersion {
                api_key: describe_acls_request::API_KEY,
                min_version: 1,
                max_version: 3,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();
    buf.to_vec()
}

fn describe_acls_response(version: i16) -> Vec<u8> {
    let mut buf = BytesMut::new();
    if version >= crabka_protocol::owned::describe_acls_response::FLEXIBLE_MIN {
        buf.extend_from_slice(&[0x00]);
    }
    DescribeAclsResponse::default()
        .encode(&mut buf, version)
        .unwrap();
    buf.to_vec()
}

struct ClosingAclBroker {
    addr: SocketAddr,
    shutdown: CancellationToken,
    describe_calls: Arc<AtomicUsize>,
}

impl ClosingAclBroker {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let first_acl_request = Arc::new(AtomicBool::new(true));
        let describe_calls = Arc::new(AtomicUsize::new(0));
        let task_shutdown = shutdown.clone();
        let task_first_acl_request = Arc::clone(&first_acl_request);
        let task_describe_calls = Arc::clone(&describe_calls);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = task_shutdown.cancelled() => break,
                    Ok((stream, _)) = listener.accept() => {
                        let conn_shutdown = task_shutdown.clone();
                        let conn_first_acl_request = Arc::clone(&task_first_acl_request);
                        let conn_describe_calls = Arc::clone(&task_describe_calls);
                        tokio::spawn(async move {
                            let mut framed = kafka_frame(stream);
                            loop {
                                tokio::select! {
                                    () = conn_shutdown.cancelled() => break,
                                    maybe_frame = framed.next() => {
                                        let Some(Ok(frame)) = maybe_frame else { break; };
                                        if frame.len() < 8 {
                                            continue;
                                        }
                                        let api_key = i16::from_be_bytes([frame[0], frame[1]]);
                                        let version = i16::from_be_bytes([frame[2], frame[3]]);
                                        let corr_id = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
                                        let body = if api_key == api_versions_request::API_KEY {
                                            Some(api_versions_response_v0())
                                        } else if api_key == describe_acls_request::API_KEY {
                                            conn_describe_calls.fetch_add(1, Ordering::SeqCst);
                                            if conn_first_acl_request.swap(false, Ordering::SeqCst) {
                                                break;
                                            }
                                            Some(describe_acls_response(version))
                                        } else {
                                            None
                                        };
                                        let Some(response_body) = body else { continue; };
                                        let mut response = BytesMut::with_capacity(4 + response_body.len());
                                        response.extend_from_slice(&corr_id.to_be_bytes());
                                        response.extend_from_slice(&response_body);
                                        if framed.send(response.freeze()).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                        });
                    }
                }
            }
        });

        Self {
            addr,
            shutdown,
            describe_calls,
        }
    }

    fn stop(self) {
        self.shutdown.cancel();
    }
}

#[tokio::test]
async fn describe_acls_reconnects_after_cached_connection_closes() {
    let mock = ClosingAclBroker::start().await;
    let mut admin = AdminClient::connect(&[mock.addr.to_string()])
        .await
        .unwrap();

    let acls = admin
        .describe_acls(&AclEntryFilter::default())
        .await
        .unwrap();

    assert!(acls.is_empty());
    assert!(mock.describe_calls.load(Ordering::SeqCst) == 2);

    mock.stop();
}
