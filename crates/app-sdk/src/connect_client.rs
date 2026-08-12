//! Minimal Connect client pieces used by the Rust app SDK.

use bytes::{Buf as _, Bytes, BytesMut};
use futures_util::{StreamExt as _, stream};
use http_body_util::{BodyExt as _, Full, StreamBody};
use hyper::{
    Method, Request, Uri, body::Frame as BodyFrame, client::conn::http2, header::CONTENT_TYPE,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::Deserialize;
use tokio::sync::mpsc;

const STREAM_CHANNEL_CAPACITY: usize = 32;

/// Connect envelope codec.
pub mod envelope {
    use super::{Bytes, BytesMut, ConnectClientError, EndStream, TrailerBody};

    /// Message frame flag byte.
    pub const MESSAGE_FLAGS: u8 = 0x00;
    /// `EndStream` trailer frame flag byte.
    pub const END_STREAM_FLAGS: u8 = 0x02;

    /// Decoded Connect frame.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum Frame {
        /// Protobuf message bytes.
        Message(Bytes),
        /// `EndStream` trailer.
        EndStream(EndStream),
    }

    /// Encode one Connect envelope frame.
    ///
    /// # Panics
    ///
    /// Panics when `message` is larger than the Connect envelope's `u32`
    /// length field can represent.
    #[must_use]
    pub fn encode(flags: u8, message: &[u8]) -> Bytes {
        let mut framed = BytesMut::with_capacity(5 + message.len());
        framed.extend_from_slice(&[flags]);
        framed.extend_from_slice(
            &u32::try_from(message.len())
                .expect("message length fits u32")
                .to_be_bytes(),
        );
        framed.extend_from_slice(message);
        framed.freeze()
    }

    /// Decode one complete Connect envelope frame.
    ///
    /// # Errors
    ///
    /// Returns an error for incomplete, malformed, compressed, or unsupported
    /// Connect frames.
    pub fn decode_one(input: &[u8]) -> Result<Frame, ConnectClientError> {
        let Some((frame, _consumed)) = decode_next(input)? else {
            return Err(ConnectClientError::PartialFrame { needed: 1 });
        };
        Ok(frame)
    }

    /// Decode the first frame from a buffer, returning `None` when more bytes are needed.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, compressed, or unsupported Connect
    /// frames.
    pub fn decode_next(input: &[u8]) -> Result<Option<(Frame, usize)>, ConnectClientError> {
        if input.len() < 5 {
            return Ok(None);
        }
        let flags = input[0];
        let len = u32::from_be_bytes([input[1], input[2], input[3], input[4]]);
        let len = usize::try_from(len).unwrap_or(usize::MAX);
        let Some(frame_len) = 5usize.checked_add(len) else {
            return Ok(None);
        };
        if input.len() < frame_len {
            return Ok(None);
        }
        let body = Bytes::copy_from_slice(&input[5..frame_len]);
        let frame = match flags {
            MESSAGE_FLAGS => Ok(Frame::Message(body)),
            END_STREAM_FLAGS => {
                let trailer = if body.is_empty() {
                    EndStream { error: None }
                } else {
                    let parsed = serde_json::from_slice::<TrailerBody>(&body)?;
                    EndStream {
                        error: parsed.error,
                    }
                };
                Ok(Frame::EndStream(trailer))
            }
            flags if flags & 0x01 == 0x01 => Err(ConnectClientError::CompressedFrame),
            other => Err(ConnectClientError::UnknownFrameFlags(other)),
        }?;
        Ok(Some((frame, frame_len)))
    }
}

/// Connect `EndStream` frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndStream {
    /// Optional Connect error from the trailer.
    pub error: Option<ConnectTrailerError>,
}

/// Connect trailer error body.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ConnectTrailerError {
    /// Connect code string.
    pub code: String,
    /// Error message.
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TrailerBody {
    pub(crate) error: Option<ConnectTrailerError>,
}

/// Minimal h2c Connect client.
#[derive(Debug, Clone)]
pub struct ConnectClient {
    endpoint: String,
    bearer: Option<String>,
}

impl ConnectClient {
    /// Create a client for an HTTP endpoint.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, bearer: Option<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            bearer,
        }
    }

    /// Execute a unary Connect request.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint, connection, HTTP exchange, or
    /// protobuf encoding/decoding fails, or when Connect returns an error.
    pub async fn unary<Req, Resp>(
        &self,
        path: &str,
        request: &Req,
    ) -> Result<Resp, ConnectClientError>
    where
        Req: prost::Message,
        Resp: prost::Message + Default,
    {
        let uri = self.uri_for(path)?;
        let authority = authority_for(&uri)?;
        let stream = tokio::net::TcpStream::connect(&authority).await?;
        let io = TokioIo::new(stream);
        let (mut sender, connection) = http2::handshake(TokioExecutor::new(), io).await?;
        tokio::spawn(async move {
            let _ = Box::pin(connection).await;
        });

        let mut encoded = Vec::with_capacity(prost::Message::encoded_len(request));
        prost::Message::encode(request, &mut encoded)?;
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header(CONTENT_TYPE, "application/proto");
        if let Some(bearer) = &self.bearer {
            builder = builder.header("authorization", format!("Bearer {bearer}"));
        }
        let response = sender
            .send_request(builder.body(Full::new(Bytes::from(encoded)))?)
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.into_body().collect().await?.to_bytes();
            return Err(response_error(status.as_u16(), &body));
        }
        let body = response.into_body().collect().await?.to_bytes();
        Ok(<Resp as prost::Message>::decode(body)?)
    }

    /// Execute a Connect client-streaming/server-streaming request with one start message.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint, connection, HTTP exchange, or
    /// protobuf encoding fails, or when Connect rejects the stream.
    pub async fn streaming<Req, Resp>(
        &self,
        path: &str,
        request: &Req,
    ) -> Result<mpsc::Receiver<Result<Resp, ConnectClientError>>, ConnectClientError>
    where
        Req: prost::Message,
        Resp: prost::Message + Default + Send + 'static,
    {
        let uri = self.uri_for(path)?;
        let authority = authority_for(&uri)?;
        let stream = tokio::net::TcpStream::connect(&authority).await?;
        let io = TokioIo::new(stream);
        let (mut sender, connection) = http2::handshake(TokioExecutor::new(), io).await?;
        tokio::spawn(async move {
            let _ = Box::pin(connection).await;
        });
        let mut encoded = Vec::with_capacity(prost::Message::encoded_len(request));
        prost::Message::encode(request, &mut encoded)?;
        let frame = envelope::encode(envelope::MESSAGE_FLAGS, &encoded);
        let body_stream = stream::iter([Ok::<_, std::convert::Infallible>(BodyFrame::data(frame))])
            .chain(stream::pending());
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header(CONTENT_TYPE, "application/connect+proto")
            .header("connect-protocol-version", "1");
        if let Some(bearer) = &self.bearer {
            builder = builder.header("authorization", format!("Bearer {bearer}"));
        }
        let response = sender
            .send_request(builder.body(StreamBody::new(body_stream))?)
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.into_body().collect().await?.to_bytes();
            return Err(response_error(status.as_u16(), &body));
        }

        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        tokio::spawn(read_streaming_response::<Resp>(response.into_body(), tx));
        Ok(rx)
    }

    fn uri_for(&self, path: &str) -> Result<Uri, ConnectClientError> {
        let endpoint = self.endpoint.trim_end_matches('/');
        let uri = format!("{endpoint}{path}")
            .parse::<Uri>()
            .map_err(|e| ConnectClientError::InvalidEndpoint(e.to_string()))?;
        if uri.scheme_str() != Some("http") {
            return Err(ConnectClientError::InvalidEndpoint(
                "only plaintext http endpoints are supported".into(),
            ));
        }
        if uri.authority().is_none() {
            return Err(ConnectClientError::InvalidEndpoint(
                "missing authority".into(),
            ));
        }
        Ok(uri)
    }
}

fn authority_for(uri: &Uri) -> Result<String, ConnectClientError> {
    uri.authority()
        .ok_or_else(|| ConnectClientError::InvalidEndpoint("missing authority".into()))
        .map(ToString::to_string)
}

fn response_error(status: u16, body: &[u8]) -> ConnectClientError {
    if let Ok(error) = serde_json::from_slice::<ConnectTrailerError>(body) {
        return error.into();
    }
    ConnectClientError::HttpStatus {
        status,
        message: String::from_utf8_lossy(body).into_owned(),
    }
}

async fn read_streaming_response<Resp>(
    mut body: hyper::body::Incoming,
    tx: mpsc::Sender<Result<Resp, ConnectClientError>>,
) where
    Resp: prost::Message + Default,
{
    let mut buffer = BytesMut::new();
    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                let _ = tx.send(Err(error.into())).await;
                return;
            }
        };
        let Ok(data) = frame.into_data() else {
            continue;
        };
        buffer.extend_from_slice(&data);
        loop {
            let decoded = envelope::decode_next(&buffer);
            let Some((frame, consumed)) = (match decoded {
                Ok(frame) => frame,
                Err(error) => {
                    let _ = tx.send(Err(error)).await;
                    return;
                }
            }) else {
                break;
            };
            buffer.advance(consumed);
            match frame {
                envelope::Frame::Message(message) => {
                    let decoded = Resp::decode(message).map_err(ConnectClientError::from);
                    if tx.send(decoded).await.is_err() {
                        return;
                    }
                }
                envelope::Frame::EndStream(end) => {
                    if let Some(error) = end.error {
                        let _ = tx.send(Err(error.into())).await;
                    }
                    return;
                }
            }
        }
    }
    if !buffer.is_empty() {
        let _ = tx
            .send(Err(ConnectClientError::PartialFrame { needed: 1 }))
            .await;
    }
}

/// Errors returned by the minimal Connect client.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectClientError {
    /// Endpoint URL was invalid.
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    /// TCP or process I/O failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// HTTP/2 connection failed.
    #[error("hyper: {0}")]
    Hyper(#[from] hyper::Error),
    /// HTTP request construction failed.
    #[error("http: {0}")]
    Http(#[from] hyper::http::Error),
    /// Protobuf encode or decode failed.
    #[error("protobuf: {0}")]
    Prost(#[from] prost::DecodeError),
    /// Protobuf encode failed.
    #[error("protobuf encode: {0}")]
    ProstEncode(#[from] prost::EncodeError),
    /// JSON trailer body failed to decode.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// HTTP status was not successful.
    #[error("http status {status}: {message}")]
    HttpStatus {
        /// HTTP status code.
        status: u16,
        /// Response body text.
        message: String,
    },
    /// Connect trailer contained an error.
    #[error("connect {code}: {message}")]
    Connect {
        /// Connect code string.
        code: String,
        /// Connect error message.
        message: String,
    },
    /// Input did not contain a complete envelope frame.
    #[error("partial frame; need {needed} more bytes")]
    PartialFrame {
        /// Number of additional bytes needed.
        needed: usize,
    },
    /// Compressed frames are not supported by this minimal client.
    #[error("compressed connect frames are unsupported")]
    CompressedFrame,
    /// Unknown frame flags were encountered.
    #[error("unknown connect frame flags {0:#x}")]
    UnknownFrameFlags(u8),
}

impl From<ConnectTrailerError> for ConnectClientError {
    fn from(value: ConnectTrailerError) -> Self {
        Self::Connect {
            code: value.code,
            message: value.message,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, time::Duration};

    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::{Response, StatusCode, service::service_fn};
    use hyper_util::rt::{TokioExecutor, TokioIo};

    use super::{
        ConnectClient, ConnectClientError, STREAM_CHANNEL_CAPACITY,
        envelope::{self, Frame},
        response_error,
    };
    use crate::error::CrabkaError;

    #[derive(Clone, PartialEq, prost::Message)]
    struct Empty {}

    async fn serve_once(status: StatusCode, body: Bytes) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener binds");
        let addr = listener.local_addr().expect("test listener has address");
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("test client connects");
            let service = service_fn(move |_| {
                let body = body.clone();
                async move {
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(status)
                            .body(Full::new(body))
                            .expect("test response builds"),
                    )
                }
            });
            let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(socket), service)
                .await;
        });
        (format!("http://{addr}"), task)
    }

    #[test]
    fn envelope_round_trips() {
        let msg = b"payload";
        let framed = envelope::encode(0x00, msg);
        assert_eq!(framed[0], 0);
        assert_eq!(u32::from_be_bytes(framed[1..5].try_into().unwrap()), 7);
        assert2::assert!(let Frame::Message(m) = envelope::decode_one(&framed).unwrap());
        assert_eq!(m.as_ref(), msg);
    }

    #[test]
    fn end_stream_trailer_decodes_connect_error() {
        let framed = envelope::encode(0x02, br#"{"error":{"code":"not_found","message":"x"}}"#);
        assert2::assert!(let Frame::EndStream(t) = envelope::decode_one(&framed).unwrap());
        let error = t.error.unwrap();
        assert_eq!(error.code, "not_found");
        assert_eq!(error.message, "x");
    }

    #[test]
    fn non_success_connect_body_decodes_connect_error() {
        let error = response_error(
            400,
            br#"{"code":"invalid_argument","message":"bad request"}"#,
        );

        assert2::assert!(let ConnectClientError::Connect { code, message } = error);
        assert_eq!(code, "invalid_argument");
        assert_eq!(message, "bad request");
    }

    #[test]
    fn non_connect_body_preserves_http_status() {
        let error = response_error(503, b"upstream unavailable");

        assert2::assert!(let ConnectClientError::HttpStatus { status, message } = error);
        assert_eq!(status, 503);
        assert_eq!(message, "upstream unavailable");
    }

    #[tokio::test]
    async fn live_http_and_end_stream_errors_reach_the_sdk_taxonomy() {
        let (endpoint, server) = serve_once(
            StatusCode::BAD_REQUEST,
            Bytes::from_static(br#"{"code":"invalid_argument","message":"bad request"}"#),
        )
        .await;
        let error = ConnectClient::new(endpoint, None)
            .unary::<_, Empty>("/test.Service/Unary", &Empty {})
            .await
            .expect_err("non-success Connect response fails");
        server.abort();

        assert2::assert!(let CrabkaError::InvalidArgument(message) = CrabkaError::from(error));
        assert_eq!(message, "bad request");

        let trailer = envelope::encode(
            envelope::END_STREAM_FLAGS,
            br#"{"error":{"code":"not_found","message":"missing"}}"#,
        );
        let (endpoint, server) = serve_once(StatusCode::OK, trailer).await;
        let mut responses = ConnectClient::new(endpoint, None)
            .streaming::<_, Empty>("/test.Service/Streaming", &Empty {})
            .await
            .expect("HTTP streaming response opens");
        let error = tokio::time::timeout(Duration::from_secs(2), responses.recv())
            .await
            .expect("stream response arrives")
            .expect("stream emits one error")
            .expect_err("EndStream carries an error");
        server.abort();

        assert2::assert!(let CrabkaError::NotFound(message) = CrabkaError::from(error));
        assert_eq!(message, "missing");
    }

    #[tokio::test]
    async fn streaming_receiver_is_bounded_and_truncated_frames_are_errors() {
        for body in [
            Bytes::from_static(&[0x00, 0x00]),
            Bytes::from_static(&[0x00, 0x00, 0x00, 0x00, 0x04, 0x01, 0x02]),
        ] {
            let (endpoint, server) = serve_once(StatusCode::OK, body).await;
            let mut responses = ConnectClient::new(endpoint, None)
                .streaming::<_, Empty>("/test.Service/Streaming", &Empty {})
                .await
                .expect("HTTP streaming response opens");

            assert_eq!(responses.max_capacity(), STREAM_CHANNEL_CAPACITY);
            let error = tokio::time::timeout(Duration::from_secs(2), responses.recv())
                .await
                .expect("stream response arrives")
                .expect("truncated stream emits an error")
                .expect_err("truncated stream must not complete cleanly");
            assert2::assert!(matches!(error, ConnectClientError::PartialFrame { .. }));
            server.abort();
        }
    }
}
