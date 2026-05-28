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
    use bytes::Bytes;
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

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
        assert_eq!(received.as_ref(), b"hello broker");
    }
}
