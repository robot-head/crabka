//! TLS connector shared by live conformance and driver gates.

use std::path::Path;

use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Client, NoTls};

/// Build a hostname-verifying `PostgreSQL` TLS connector from a PEM root CA.
///
/// # Errors
///
/// Returns an error when the CA cannot be read or parsed, or the native TLS
/// connector cannot be constructed.
pub fn connector_from_root_ca(
    root_ca: &Path,
) -> Result<MakeTlsConnector, Box<dyn std::error::Error + Send + Sync>> {
    let pem = std::fs::read(root_ca)?;
    let certificate = native_tls::Certificate::from_pem(&pem)?;
    let mut builder = native_tls::TlsConnector::builder();
    builder.add_root_certificate(certificate);
    Ok(MakeTlsConnector::new(builder.build()?))
}

/// Build the connector named by `PGSSLROOTCERT`.
///
/// # Errors
///
/// Returns an error when the environment variable is absent or invalid.
pub fn connector_from_env() -> Result<MakeTlsConnector, Box<dyn std::error::Error + Send + Sync>> {
    let root_ca = std::env::var_os("PGSSLROOTCERT").ok_or("PGSSLROOTCERT is required")?;
    connector_from_root_ca(Path::new(&root_ca))
}

/// Connect with verified TLS when `PGSSLROOTCERT` is set, otherwise without TLS.
///
/// # Errors
///
/// Returns connection, certificate, or background connection errors.
pub async fn connect(
    database_url: &str,
) -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
    if std::env::var_os("PGSSLROOTCERT").is_some() {
        let (client, connection) =
            tokio_postgres::connect(database_url, connector_from_env()?).await?;
        drop(tokio::spawn(connection));
        return Ok(client);
    }
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    drop(tokio::spawn(connection));
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_ca_builds_a_verifying_connector() {
        let fixture =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../pgwire/tests/fixtures/test-ca.pem");
        connector_from_root_ca(&fixture).expect("fixture CA builds connector");
    }
}
