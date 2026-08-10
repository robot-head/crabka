//! Kafka uses a 4-byte big-endian length prefix followed by the frame body.
//! Both directions of every connection share this framing.

#[cfg(test)]
use tokio::net::TcpStream;
#[cfg(test)]
use tokio_util::codec::Framed;
use tokio_util::codec::LengthDelimitedCodec;

pub(crate) fn validate_frame_length(
    frame_body_len: usize,
    max_frame_bytes: usize,
) -> std::io::Result<()> {
    if frame_body_len > max_frame_bytes {
        return Err(std::io::Error::other(
            "frame exceeds configured maximum size",
        ));
    }
    Ok(())
}

/// Builds a [`LengthDelimitedCodec`] configured for Kafka's wire framing.
#[must_use]
pub fn codec(max_frame_bytes: usize) -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .length_field_offset(0)
        .length_field_length(4)
        .length_field_type::<u32>()
        .max_frame_length(max_frame_bytes)
        .big_endian()
        .new_codec()
}

/// Wraps a [`TcpStream`] with the Kafka length-delimited codec.
#[must_use]
#[cfg(test)]
pub fn frame(stream: TcpStream, max_frame_bytes: usize) -> Framed<TcpStream, LengthDelimitedCodec> {
    Framed::new(stream, codec(max_frame_bytes))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use bytes::{BufMut, Bytes, BytesMut};
    use futures_util::{SinkExt, StreamExt};
    use tokio::{
        io::AsyncWriteExt,
        net::{TcpListener, TcpStream},
    };
    use tokio_util::codec::{Decoder as _, Encoder as _};

    use super::*;

    const DEFAULT_MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

    #[tokio::test]
    async fn roundtrips_a_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut framed = frame(stream, DEFAULT_MAX_FRAME_BYTES);
            framed.next().await.unwrap().unwrap().freeze()
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let mut framed = frame(client, DEFAULT_MAX_FRAME_BYTES);
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
        assert!(DEFAULT_MAX_FRAME_BYTES == 100 * 1024 * 1024);
        assert!(DEFAULT_MAX_FRAME_BYTES == 104_857_600);
    }

    #[test]
    fn codec_decodes_frames_larger_than_tokio_default_but_within_kafka_max() {
        let payload_len = 9 * 1024 * 1024;
        assert!(payload_len < DEFAULT_MAX_FRAME_BYTES);

        let mut bytes = BytesMut::with_capacity(4 + payload_len);
        bytes.put_u32(u32::try_from(payload_len).expect("payload length fits u32"));
        bytes.resize(4 + payload_len, 0xA5);

        let decoded = codec(DEFAULT_MAX_FRAME_BYTES)
            .decode(&mut bytes)
            .expect("decode")
            .expect("frame");
        check!(decoded.len() == payload_len);
        check!(decoded[0] == 0xA5);
        check!(decoded[payload_len - 1] == 0xA5);
    }

    #[test]
    fn codec_honors_nondefault_max_frame_length() {
        let mut exact = BytesMut::with_capacity(12);
        exact.put_u32(8);
        exact.resize(12, 0xA5);
        assert!(
            codec(8)
                .decode(&mut exact)
                .expect("decode exact maximum")
                .is_some()
        );

        let mut bytes = BytesMut::with_capacity(4);
        bytes.put_u32(9);

        let err = codec(8).decode(&mut bytes).expect_err("oversized frame");
        assert!(err.to_string().contains("frame size too big"));

        let mut encoded = BytesMut::new();
        codec(8)
            .encode(Bytes::from_static(b"12345678"), &mut encoded)
            .expect("encode exact maximum");
        let err = codec(8)
            .encode(Bytes::from_static(b"123456789"), &mut encoded)
            .expect_err("oversized response");
        assert!(err.to_string().contains("frame size too big"));
    }

    #[test]
    fn codec_rejects_frames_over_kafka_max() {
        let mut bytes = BytesMut::with_capacity(4);
        bytes.put_u32(
            u32::try_from(DEFAULT_MAX_FRAME_BYTES + 1).expect("max frame length fits u32"),
        );

        let err = codec(DEFAULT_MAX_FRAME_BYTES)
            .decode(&mut bytes)
            .expect_err("oversized frame");
        assert!(
            err.to_string().contains("frame size too big"),
            "unexpected error: {err}"
        );
    }
}
