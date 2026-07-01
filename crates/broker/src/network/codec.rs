//! Kafka uses a 4-byte big-endian length prefix followed by the frame body.
//! Both directions of every connection share this framing.

#![allow(dead_code)] // accept loop materializes elsewhere.

use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Default Apache Kafka `socket.request.max.bytes` is 100 MiB. Match it.
pub const MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

/// Build a [`LengthDelimitedCodec`] configured for Kafka's wire framing.
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

/// Wrap a [`TcpStream`] with the Kafka length-delimited codec.
#[must_use]
pub fn frame(stream: TcpStream) -> Framed<TcpStream, LengthDelimitedCodec> {
    Framed::new(stream, codec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use bytes::{BufMut, Bytes, BytesMut};
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::Decoder as _;

    #[tokio::test]
    async fn roundtrips_a_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut framed = frame(stream);
            framed.next().await.unwrap().unwrap().freeze()
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let mut framed = frame(client);
        framed
            .send(Bytes::from_static(b"hello broker"))
            .await
            .unwrap();
        framed.into_inner().shutdown().await.unwrap();

        let received = server.await.unwrap();
        assert!(received.as_ref() == b"hello broker");
    }

    #[test]
    fn kafka_max_frame_size_is_one_hundred_mib() {
        assert!(MAX_FRAME_BYTES == 100 * 1024 * 1024);
        assert!(MAX_FRAME_BYTES == 104_857_600);
    }

    #[test]
    fn codec_decodes_frames_larger_than_tokio_default_but_within_kafka_max() {
        let payload_len = 9 * 1024 * 1024;
        assert!(payload_len < MAX_FRAME_BYTES);

        let mut bytes = BytesMut::with_capacity(4 + payload_len);
        bytes.put_u32(u32::try_from(payload_len).expect("payload length fits u32"));
        bytes.resize(4 + payload_len, 0xA5);

        let decoded = codec().decode(&mut bytes).expect("decode").expect("frame");
        assert!((decoded.len(), decoded[0], decoded[payload_len - 1]) == (payload_len, 0xA5, 0xA5));
    }

    #[test]
    fn codec_rejects_frames_over_kafka_max() {
        let mut bytes = BytesMut::with_capacity(4);
        bytes.put_u32(u32::try_from(MAX_FRAME_BYTES + 1).expect("max frame length fits u32"));

        let err = codec().decode(&mut bytes).expect_err("oversized frame");
        assert!(
            err.to_string().contains("frame size too big"),
            "unexpected error: {err}"
        );
    }
}
