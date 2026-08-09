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

use crate::ClientFrameMax;

/// Build a Kafka wire codec with a validated accepted-frame limit.
#[must_use]
pub fn codec_with_max(max: ClientFrameMax) -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .length_field_offset(0)
        .length_field_length(4)
        .length_field_type::<u32>()
        .max_frame_length(max.bytes())
        .big_endian()
        .new_codec()
}

/// Build a length-delimited codec configured for Kafka's wire framing.
#[must_use]
pub fn codec() -> LengthDelimitedCodec {
    codec_with_max(ClientFrameMax::default())
}

/// Wrap a `TcpStream` with the Kafka length-delimited codec.
pub fn frame(stream: TcpStream) -> Framed<TcpStream, LengthDelimitedCodec> {
    Framed::new(stream, codec())
}

/// Wrap any `AsyncRead + AsyncWrite` stream with the Kafka length-delimited
/// codec.
///
/// [`crate::Connection::from_stream`] calls this so callers can hand in a
/// pre-authenticated stream, for example the output of the broker's
/// `InterBrokerClient` after TLS + SASL.
pub fn frame_generic<S>(stream: S) -> Framed<S, LengthDelimitedCodec>
where
    S: AsyncRead + AsyncWrite,
{
    Framed::new(stream, codec())
}

/// LEB128-encode `v` into `buf`.
pub(crate) fn put_uvarint<B: BufMut>(buf: &mut B, mut v: u32) {
    while (v & !0x7F) != 0 {
        let byte = u8::try_from(v & 0x7F).expect("varint payload is seven bits");
        buf.put_u8(byte | 0x80);
        v >>= 7;
    }
    buf.put_u8(u8::try_from(v).expect("final varint byte is less than 128"));
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
    use tokio_util::codec::Decoder;

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
        buf.clear();
        put_uvarint(&mut buf, 16_384);
        assert!(buf.as_ref() == &[0x80u8, 0x80u8, 0x01u8]);
        buf.clear();
        put_uvarint(&mut buf, u32::MAX);
        assert!(buf.as_ref() == &[0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8, 0x0F]);
    }

    #[test]
    fn max_frame_bytes_matches_kafka_default() {
        assert!(ClientFrameMax::default().bytes() == 100 * 1024 * 1024);
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

    #[test]
    fn configured_codec_rejects_a_frame_over_its_limit() {
        let max = ClientFrameMax::try_from(crabka_units::bytes(8)).unwrap();
        let mut codec = codec_with_max(max);
        let mut input = BytesMut::from(&[0, 0, 0, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8][..]);

        let error = codec
            .decode(&mut input)
            .expect_err("nine-byte frame exceeds eight-byte max");
        assert!(error.to_string().contains("frame size too big"));
    }

    #[test]
    fn configured_codec_accepts_the_exact_limit() {
        let max = ClientFrameMax::try_from(crabka_units::bytes(8)).unwrap();
        let mut codec = codec_with_max(max);
        let mut input = BytesMut::from(&[0, 0, 0, 8, 0, 1, 2, 3, 4, 5, 6, 7][..]);

        assert!(codec.decode(&mut input).unwrap().unwrap().len() == 8);
    }
}
