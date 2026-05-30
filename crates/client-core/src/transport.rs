//! TCP framing wrapper. Kafka uses a 4-byte big-endian length prefix
//! followed by the frame body.

// These items are consumed by `connection.rs` (Tasks 8–10); dead_code
// triggers because connection.rs doesn't exist yet.
#![allow(dead_code)]

use bytes::BufMut;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Maximum frame size we'll accept (matches Kafka's default
/// `socket.request.max.bytes` = 100 MiB).
pub const MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

/// Build a length-delimited codec configured for Kafka's wire framing.
#[must_use]
pub fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .length_field_offset(0)
        .length_field_length(4)
        .length_field_type::<u32>()
        .max_frame_length(MAX_FRAME_BYTES)
        .big_endian()
        .new_codec()
}

/// Wrap a `TcpStream` with the Kafka length-delimited codec.
pub fn frame(stream: TcpStream) -> Framed<TcpStream, LengthDelimitedCodec> {
    Framed::new(stream, codec())
}

/// Generic wrapper: wrap any `AsyncRead + AsyncWrite` stream with the
/// Kafka length-delimited codec. Used by [`crate::Connection::from_stream`]
/// so callers can hand in a pre-authenticated stream (e.g., the output of
/// the broker's `InterBrokerClient` after TLS + SASL).
pub fn frame_generic<S>(stream: S) -> Framed<S, LengthDelimitedCodec>
where
    S: AsyncRead + AsyncWrite,
{
    Framed::new(stream, codec())
}

/// LEB128-encode `v` into `buf`.
pub(crate) fn put_uvarint<B: BufMut>(buf: &mut B, mut v: u32) {
    while (v & !0x7F) != 0 {
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u8(((v & 0x7F) as u8) | 0x80);
        v >>= 7;
    }
    #[allow(clippy::cast_possible_truncation)]
    buf.put_u8(v as u8);
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use bytes::{Bytes, BytesMut};
    use futures_util::SinkExt;
    use futures_util::StreamExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn roundtrips_a_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut framed = frame(stream);
            let frame = framed.next().await.unwrap().unwrap();
            frame.freeze()
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let mut framed = frame(client);
        framed
            .send(Bytes::from_static(b"hello kafka"))
            .await
            .unwrap();
        framed.into_inner().shutdown().await.unwrap();

        let received = server.await.unwrap();
        assert!(received.as_ref() == b"hello kafka");
    }

    #[test]
    fn put_uvarint_single_byte() {
        let mut buf = BytesMut::new();
        put_uvarint(&mut buf, 0);
        assert!(buf.as_ref() == &[0u8]);
        buf.clear();
        put_uvarint(&mut buf, 127);
        assert!(buf.as_ref() == &[0x7Fu8]);
    }

    #[test]
    fn put_uvarint_multibyte() {
        // 128 encodes to 0x80 0x01
        let mut buf = BytesMut::new();
        put_uvarint(&mut buf, 128);
        assert!(buf.as_ref() == &[0x80u8, 0x01u8]);
    }
}
