//! In-process mock Kafka broker.
//!
//! This module helps you test `Connection` without a JVM. It is gated to
//! `#[cfg(any(test, feature = "mock"))]`.
//!
//! # Handler signature
//!
//! The handler receives `(api_key, version, correlation_id, request_body)` and
//! returns `Option<Vec<u8>>`:
//!
//! - `Some(bytes)` — `MockBroker` prepends the correlation-id and sends the
//!   frame back to the client.
//! - `None` — `MockBroker` silently drops the request. The client
//!   eventually reaches its `request_timeout`.
//!
//! `MockBroker` prepends the correlation-id header automatically. The handler
//! only needs to supply the response body, which is the part after the
//! correlation-id.

#![cfg(any(test, feature = "mock"))]

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use bytes::BytesMut;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

/// A simple in-process mock Kafka broker for unit testing.
///
/// `MockBroker` handles each accepted TCP connection in its own Tokio task.
/// All connections share the same handler closure.
pub struct MockBroker {
    /// The address the mock is listening on.
    pub addr: SocketAddr,
    shutdown: CancellationToken,
    _task: JoinHandle<()>,
}

/// Handler type: receives `(api_key, version, correlation_id, request_body)`
/// and returns `Some(response_body)` to reply or `None` to drop the request.
type Handler = Box<dyn FnMut(i16, i16, i32, &[u8]) -> Option<Vec<u8>> + Send>;

impl MockBroker {
    /// Start a mock broker listening on a random localhost port.
    ///
    /// The handler receives `(api_key, version, correlation_id, request_body)`
    /// and returns `Some(body)` to send a response or `None` to stay silent.
    /// When the handler stays silent, the client times out on that request.
    ///
    /// The `MockBroker` prepends the correlation-id to the returned body
    /// automatically. The handler only supplies the response body bytes.
    ///
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn start<F>(handler: F) -> Self
    where
        F: FnMut(i16, i16, i32, &[u8]) -> Option<Vec<u8>> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handler: Arc<Mutex<Handler>> = Arc::new(Mutex::new(Box::new(handler)));
        let shutdown = CancellationToken::new();

        let task_handler = Arc::clone(&handler);
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = task_shutdown.cancelled() => break,
                    Ok((stream, _)) = listener.accept() => {
                        let h = Arc::clone(&task_handler);
                        let sd = task_shutdown.clone();
                        tokio::spawn(async move {
                            handle_connection(stream, h, sd).await;
                        });
                    }
                }
            }
        });

        Self {
            addr,
            shutdown,
            _task: task,
        }
    }

    /// Stop the broker, cancelling all connection tasks.
    pub fn stop(self) {
        self.shutdown.cancel();
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    handler: Arc<Mutex<Handler>>,
    shutdown: CancellationToken,
) {
    use futures_util::{SinkExt, StreamExt};

    let mut framed = crate::transport::frame(stream);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            maybe_frame = framed.next() => {
                let Some(frame) = maybe_frame else { break; };
                let Ok(frame) = frame else { break; };
                // Minimum: api_key(2) + version(2) + corr_id(4) = 8 bytes.
                if frame.len() < 8 { continue; }

                // RequestHeader v1+ wire shape:
                //   api_key:i16, api_version:i16, correlation_id:i32, client_id...
                let api_key = i16::from_be_bytes([frame[0], frame[1]]);
                let version  = i16::from_be_bytes([frame[2], frame[3]]);
                let corr_id  = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
                // Everything after the first 8 bytes is passed as the request body.
                // (We skip client_id parsing — tests don't need it.)
                let body = &frame[8..];

                let response_body_opt = {
                    let mut h = handler.lock().unwrap();
                    h(api_key, version, corr_id, body)
                };

                // None => stay silent (client will time out).
                let Some(response_body) = response_body_opt else { continue; };

                // Build the response frame: corr_id(i32 BE) + body bytes.
                let mut resp = BytesMut::with_capacity(4 + response_body.len());
                resp.extend_from_slice(&corr_id.to_be_bytes());
                resp.extend_from_slice(&response_body);

                if framed.send(resp.freeze()).await.is_err() {
                    break;
                }
            }
        }
    }
}
