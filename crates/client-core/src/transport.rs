//! TCP framing wrapper. Kafka uses a 4-byte big-endian length prefix
//! followed by the frame body.

// These items are consumed by `connection.rs` (Tasks 8–10); dead_code
// triggers because connection.rs doesn't exist yet.
#![allow(dead_code)]

use bytes::BufMut;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
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
    use assert2::assert;
    use bytes::{Bytes, BytesMut};
    use futures_util::{SinkExt, StreamExt};
    use tokio::{
        io::AsyncWriteExt,
        net::{TcpListener, TcpStream},
    };

    use super::*;

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
        for (name, value, expected) in [
            ("zero", 0, &[0u8][..]),
            ("largest single byte", 127, &[0x7F][..]),
        ] {
            let mut buf = BytesMut::new();
            put_uvarint(&mut buf, value);
            assert!(buf.as_ref() == expected, "case {name}");
        }
    }

    #[test]
    fn put_uvarint_multibyte() {
        for (name, value, expected) in [
            ("two bytes", 128, &[0x80u8, 0x01][..]),
            ("three bytes", 16_384, &[0x80u8, 0x80, 0x01][..]),
            ("u32 max", u32::MAX, &[0xFFu8, 0xFF, 0xFF, 0xFF, 0x0F][..]),
        ] {
            let mut buf = BytesMut::new();
            put_uvarint(&mut buf, value);
            assert!(buf.as_ref() == expected, "case {name}");
        }
    }

    #[test]
    fn max_frame_bytes_matches_kafka_default() {
        assert!(MAX_FRAME_BYTES == 100 * 1024 * 1024);
    }

    #[tokio::test]
    async fn codec_accepts_frames_larger_than_tokio_util_default() {
        let (client, server) = tokio::io::duplex(10 * 1024 * 1024);
        let payload = Bytes::from(vec![0xA5; 9 * 1024 * 1024]);

        let server_task = tokio::spawn(async move {
            let mut framed = frame_generic(server);
            framed.next().await.unwrap().unwrap().len()
        });
        let mut framed = frame_generic(client);
        framed.send(payload).await.unwrap();

        assert!(server_task.await.unwrap() == 9 * 1024 * 1024);
    }
}
