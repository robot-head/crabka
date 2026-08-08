use std::fmt;

use bytes::{BufMut, BytesMut};
use crabka_pgwire::messages::frontend::{self, StartupPacket};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::ActivatorError;

/// Startup prelude captured before the transparent pipe starts.
#[derive(Clone, PartialEq, Eq)]
pub struct Prelude {
    /// Database parameter, used as the tenant wake key.
    pub database: String,
    /// Exact raw `StartupMessage` bytes to replay to the backend compute.
    pub raw_startup: Vec<u8>,
}

impl fmt::Debug for Prelude {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Prelude")
            .field("database", &self.database)
            .field("raw_startup_len", &self.raw_startup.len())
            .finish()
    }
}

/// Read the frontend prelude from a TCP stream.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn peek_prelude(stream: &mut tokio::net::TcpStream) -> Result<Prelude, ActivatorError> {
    peek_prelude_from(stream).await
}

/// Read the frontend prelude from an async stream.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn peek_prelude_from<S>(stream: &mut S) -> Result<Prelude, ActivatorError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let first = read_startup_frame(stream).await?;
    match decode_startup_frame(&first)? {
        StartupPacket::SslRequest | StartupPacket::GssEncRequest => {
            stream.write_all(b"N").await?;
            let startup = read_startup_frame(stream).await?;
            prelude_from_startup(startup)
        }
        StartupPacket::Startup { params } => prelude_from_parts(first, params),
        StartupPacket::CancelRequest { .. } => Err(ActivatorError::Prelude(
            "cancel requests cannot wake suspended tenants".to_string(),
        )),
    }
}

async fn read_startup_frame<S>(stream: &mut S) -> Result<Vec<u8>, ActivatorError>
where
    S: AsyncRead + Unpin,
{
    let mut len = [0_u8; 4];
    stream.read_exact(&mut len).await?;
    let frame_len = i32::from_be_bytes(len);
    if frame_len < 8 {
        return Err(ActivatorError::Prelude(format!(
            "invalid startup packet length: {frame_len}"
        )));
    }
    let frame_len = usize::try_from(frame_len)
        .map_err(|_| ActivatorError::Prelude("negative startup packet length".to_string()))?;
    if frame_len > frontend::MAX_STARTUP_PACKET_LEN {
        return Err(ActivatorError::Prelude(format!(
            "invalid startup packet length: {frame_len}"
        )));
    }
    let mut frame = Vec::with_capacity(frame_len);
    frame.extend_from_slice(&len);
    frame.resize(frame_len, 0);
    stream.read_exact(&mut frame[4..]).await?;
    Ok(frame)
}

fn prelude_from_startup(startup: Vec<u8>) -> Result<Prelude, ActivatorError> {
    match decode_startup_frame(&startup)? {
        StartupPacket::Startup { params } => prelude_from_parts(startup, params),
        StartupPacket::SslRequest | StartupPacket::GssEncRequest => Err(ActivatorError::Prelude(
            "startup prelude repeated encryption request".to_string(),
        )),
        StartupPacket::CancelRequest { .. } => Err(ActivatorError::Prelude(
            "cancel requests cannot wake suspended tenants".to_string(),
        )),
    }
}

fn prelude_from_parts(
    raw_startup: Vec<u8>,
    params: Vec<(String, String)>,
) -> Result<Prelude, ActivatorError> {
    let Some(database) = params
        .into_iter()
        .find_map(|(key, value)| (key == "database").then_some(value))
    else {
        return Err(ActivatorError::MissingDatabase);
    };
    Ok(Prelude {
        database,
        raw_startup,
    })
}

fn decode_startup_frame(frame: &[u8]) -> Result<StartupPacket, ActivatorError> {
    let mut buf = BytesMut::with_capacity(frame.len());
    buf.put_slice(frame);
    frontend::decode_startup(&mut buf)
        .map_err(|error| ActivatorError::Prelude(error.to_string()))?
        .ok_or_else(|| ActivatorError::Prelude("incomplete startup packet".to_string()))
}
