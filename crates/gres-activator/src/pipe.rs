use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::ActivatorError;

/// Connect the backend, replay the held startup, and pipe bytes bidirectionally.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn pipe_startup_and_session(
    mut frontend: TcpStream,
    backend_endpoint: &str,
    raw_startup: &[u8],
) -> Result<(), ActivatorError> {
    let mut backend = TcpStream::connect(backend_endpoint).await?;
    backend.write_all(raw_startup).await?;
    let _bytes = tokio::io::copy_bidirectional(&mut frontend, &mut backend).await?;
    Ok(())
}
