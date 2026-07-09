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
    pub fn decode_one(input: &[u8]) -> Result<Frame, ConnectClientError> {
        let Some((frame, _consumed)) = decode_next(input)? else {
            return Err(ConnectClientError::PartialFrame { needed: 1 });
        };
        Ok(frame)
    }

    /// Decode the first frame from a buffer, returning `None` when more bytes are needed.
    pub fn decode_next(input: &[u8]) -> Result<Option<(Frame, usize)>, ConnectClientError> {
        if input.len() < 5 {
            return Ok(None);
        }
        let flags = input[0];
        let len = u32::from_be_bytes(
            input[1..5]
                .try_into()
                .expect("five-byte frame header has four-byte length"),
        );
        let len = usize::try_from(len).expect("u32 fits usize");
        if input.len() < 5 + len {
            return Ok(None);
        }
        let body = Bytes::copy_from_slice(&input[5..5 + len]);
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
        Ok(Some((frame, 5 + len)))
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
            let message = String::from_utf8_lossy(&body).into_owned();
            return Err(ConnectClientError::HttpStatus {
                status: status.as_u16(),
                message,
            });
        }
        let body = response.into_body().collect().await?.to_bytes();
        Ok(<Resp as prost::Message>::decode(body)?)
    }

    /// Execute a Connect client-streaming/server-streaming request with one start message.
    pub async fn streaming<Req, Resp>(
        &self,
        path: &str,
        request: &Req,
    ) -> Result<mpsc::UnboundedReceiver<Result<Resp, ConnectClientError>>, ConnectClientError>
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
            let message = String::from_utf8_lossy(&body).into_owned();
            return Err(ConnectClientError::HttpStatus {
                status: status.as_u16(),
                message,
            });
        }

        let (tx, rx) = mpsc::unbounded_channel();
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

async fn read_streaming_response<Resp>(
    mut body: hyper::body::Incoming,
    tx: mpsc::UnboundedSender<Result<Resp, ConnectClientError>>,
) where
    Resp: prost::Message + Default,
{
    let mut buffer = BytesMut::new();
    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                let _ = tx.send(Err(error.into()));
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
                    let _ = tx.send(Err(error));
                    return;
                }
            }) else {
                break;
            };
            buffer.advance(consumed);
            match frame {
                envelope::Frame::Message(message) => {
                    let decoded = Resp::decode(message).map_err(ConnectClientError::from);
                    if tx.send(decoded).is_err() {
                        return;
                    }
                }
                envelope::Frame::EndStream(end) => {
                    if let Some(error) = end.error {
                        let _ = tx.send(Err(error.into()));
                    }
                    return;
                }
            }
        }
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
    use super::envelope::{self, Frame};

    #[test]
    fn envelope_round_trips() {
        let msg = b"payload";
        let framed = envelope::encode(0x00, msg);
        assert!(framed[0] == 0);
        assert!(u32::from_be_bytes(framed[1..5].try_into().unwrap()) == 7);
        assert2::assert!(let Frame::Message(m) = envelope::decode_one(&framed).unwrap());
        assert!(m.as_ref() == msg);
    }

    #[test]
    fn end_stream_trailer_decodes_connect_error() {
        let framed = envelope::encode(0x02, br#"{"error":{"code":"not_found","message":"x"}}"#);
        assert2::assert!(let Frame::EndStream(t) = envelope::decode_one(&framed).unwrap());
        let error = t.error.unwrap();
        assert!(error.code == "not_found");
        assert!(error.message == "x");
    }
}
