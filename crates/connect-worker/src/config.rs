//! Command-line and environment configuration for one connector worker.

use std::{net::SocketAddr, path::PathBuf, str::FromStr, time::Duration};

use clap::Parser;
use crabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity,
    security::{ClientSecurity, SaslCredentials, TlsConnectorConfig},
};
use crabka_connect::SecretString;
use crabka_connect_postgres::PostgresSourceConfig;
use crabka_security::{ListenerProtocol, SaslMechanism};
use crabka_units::{ByteSize, convert::ByteSizeExt as _};

/// Kafka listener protocol used by the worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerProtocol {
    /// Unencrypted Kafka protocol.
    Plaintext,
    /// Kafka over TLS.
    Ssl,
    /// SASL over an unencrypted connection.
    SaslPlaintext,
    /// SASL over TLS.
    SaslSsl,
}

impl FromStr for BrokerProtocol {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_uppercase().replace('-', "_").as_str() {
            "PLAINTEXT" => Ok(Self::Plaintext),
            "SSL" => Ok(Self::Ssl),
            "SASL_PLAINTEXT" => Ok(Self::SaslPlaintext),
            "SASL_SSL" => Ok(Self::SaslSsl),
            _ => Err(format!(
                "unsupported broker protocol {value:?}; expected PLAINTEXT, SSL, SASL_PLAINTEXT, or SASL_SSL"
            )),
        }
    }
}

impl From<BrokerProtocol> for ListenerProtocol {
    fn from(value: BrokerProtocol) -> Self {
        match value {
            BrokerProtocol::Plaintext => Self::Plaintext,
            BrokerProtocol::Ssl => Self::Ssl,
            BrokerProtocol::SaslPlaintext => Self::SaslPlaintext,
            BrokerProtocol::SaslSsl => Self::SaslSsl,
        }
    }
}

/// Supported username/password SASL mechanisms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerSaslMechanism {
    /// SASL/PLAIN.
    Plain,
    /// SCRAM-SHA-256.
    ScramSha256,
    /// SCRAM-SHA-512.
    ScramSha512,
}

impl FromStr for BrokerSaslMechanism {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_uppercase().replace('_', "-").as_str() {
            "PLAIN" => Ok(Self::Plain),
            "SCRAM-SHA-256" => Ok(Self::ScramSha256),
            "SCRAM-SHA-512" => Ok(Self::ScramSha512),
            _ => Err(format!(
                "unsupported SASL mechanism {value:?}; expected PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512"
            )),
        }
    }
}

/// Complete configuration for one managed Postgres source connector.
#[derive(Clone, Parser)]
#[command(name = "crabka-connect-worker", version, about)]
pub struct WorkerConfig {
    /// Stable connector ID and checkpoint key.
    #[arg(long, env = "CRABKA_CONNECTOR_ID", value_parser = parse_non_empty)]
    pub connector_id: String,
    /// Kafka bootstrap address.
    #[arg(long, env = "CRABKA_KAFKA_BOOTSTRAP", value_parser = parse_non_empty)]
    pub kafka_bootstrap: String,
    /// `PostgreSQL` connection URL. Formatting is always redacted.
    #[arg(long, env = "CRABKA_POSTGRES_URL", value_parser = parse_secret)]
    pub postgres_url: SecretString,
    /// `PostgreSQL` logical replication slot.
    #[arg(long, env = "CRABKA_POSTGRES_SLOT", value_parser = parse_non_empty)]
    pub postgres_slot: String,
    /// `PostgreSQL` publication.
    #[arg(
        long,
        env = "CRABKA_POSTGRES_PUBLICATION",
        default_value = "crabka_connect"
    )]
    pub postgres_publication: String,
    /// `PostgreSQL` schema containing the selected tables.
    #[arg(long, env = "CRABKA_POSTGRES_SCHEMA", default_value = "public")]
    pub postgres_schema: String,
    /// Comma-delimited table names captured from the publication.
    #[arg(
        long,
        env = "CRABKA_POSTGRES_TABLES",
        value_delimiter = ',',
        required = true
    )]
    pub postgres_tables: Vec<String>,
    /// Prefix prepended to source topics with a dot separator; empty disables it.
    #[arg(long, env = "CRABKA_TOPIC_PREFIX", default_value = "db")]
    pub topic_prefix: String,
    /// Maximum records buffered before a Kafka durability barrier.
    #[arg(long, env = "CRABKA_CONNECT_BATCH_SIZE", default_value_t = 500, value_parser = parse_positive_usize)]
    pub batch_size: usize,
    /// Maximum time between Kafka durability barriers, in milliseconds.
    #[arg(long, env = "CRABKA_CONNECT_COMMIT_INTERVAL_MS", default_value_t = 5_000, value_parser = clap::value_parser!(u64).range(1..))]
    pub commit_interval_ms: u64,
    /// Delay after `PostgreSQL` reports no new changes, in milliseconds.
    #[arg(long, env = "CRABKA_CONNECT_POLL_BACKOFF_MS", default_value_t = 100, value_parser = clap::value_parser!(u64).range(1..))]
    pub poll_backoff_ms: u64,
    /// Address serving `/live`, `/ready`, and `/metrics`.
    #[arg(
        long,
        env = "CRABKA_CONNECT_HEALTH_LISTEN",
        default_value = "0.0.0.0:8080"
    )]
    pub health_listen: SocketAddr,
    /// Capacity of each Kafka client's pending request dispatch queue.
    #[arg(long, env = "CRABKA_CLIENT_DISPATCH_QUEUE_CAPACITY", default_value_t = 64, value_parser = parse_positive_usize)]
    pub client_dispatch_queue_capacity: usize,
    /// Maximum accepted Kafka response frame size in bytes.
    #[arg(long, env = "CRABKA_CLIENT_FRAME_MAX_BYTES", default_value_t = 100 * 1024 * 1024, value_parser = clap::value_parser!(u64).range(1..))]
    pub client_frame_max_bytes: u64,
    /// Broker security protocol.
    #[arg(long, env = "CRABKA_BROKER_PROTOCOL", default_value = "PLAINTEXT")]
    pub broker_protocol: BrokerProtocol,
    /// PEM file containing broker CA certificates.
    #[arg(long, env = "CRABKA_BROKER_CA_PATH")]
    pub broker_ca_path: Option<PathBuf>,
    /// TLS SNI name used to verify the broker certificate.
    #[arg(long, env = "CRABKA_BROKER_SERVER_NAME")]
    pub broker_server_name: Option<String>,
    /// PEM client certificate chain for mTLS.
    #[arg(long, env = "CRABKA_BROKER_CERT_PATH")]
    pub broker_cert_path: Option<PathBuf>,
    /// PEM private key for mTLS.
    #[arg(long, env = "CRABKA_BROKER_KEY_PATH")]
    pub broker_key_path: Option<PathBuf>,
    /// SASL username.
    #[arg(long, env = "CRABKA_BROKER_SASL_USERNAME")]
    pub broker_sasl_username: Option<String>,
    /// SASL password. Formatting is always redacted.
    #[arg(long, env = "CRABKA_BROKER_SASL_PASSWORD", value_parser = parse_secret)]
    pub broker_sasl_password: Option<SecretString>,
    /// SASL PLAIN or SCRAM mechanism.
    #[arg(long, env = "CRABKA_BROKER_SASL_MECHANISM")]
    pub broker_sasl_mechanism: Option<BrokerSaslMechanism>,
}

impl WorkerConfig {
    /// Validate cross-field resource and security constraints.
    ///
    /// # Errors
    ///
    /// Returns an error for an oversized batch, invalid client limit, incomplete
    /// mTLS identity, or security material inconsistent with the protocol.
    pub fn validate(&self) -> Result<(), String> {
        u32::try_from(self.batch_size)
            .map_err(|_| "batch size must fit PostgreSQL's u32 poll limit".to_owned())?;
        ConnectionDispatchQueueCapacity::new(self.client_dispatch_queue_capacity)?;
        ClientFrameMax::try_from(ByteSize::from_bytes(self.client_frame_max_bytes))?;

        let protocol: ListenerProtocol = self.broker_protocol.into();
        let has_tls = self.broker_ca_path.is_some()
            || self.broker_server_name.is_some()
            || self.broker_cert_path.is_some()
            || self.broker_key_path.is_some();
        if protocol.requires_tls() {
            if self.broker_ca_path.is_none() || self.broker_server_name.is_none() {
                return Err("TLS requires broker CA path and server name".to_owned());
            }
            if self.broker_cert_path.is_some() != self.broker_key_path.is_some() {
                return Err("mTLS requires both broker cert path and key path".to_owned());
            }
        } else if has_tls {
            return Err("TLS settings require SSL or SASL_SSL broker protocol".to_owned());
        }

        let has_sasl = self.broker_sasl_username.is_some()
            || self.broker_sasl_password.is_some()
            || self.broker_sasl_mechanism.is_some();
        if protocol.requires_sasl() {
            if self.broker_sasl_username.is_none()
                || self.broker_sasl_password.is_none()
                || self.broker_sasl_mechanism.is_none()
            {
                return Err("SASL requires mechanism, username, and password".to_owned());
            }
        } else if has_sasl {
            return Err(
                "SASL settings require SASL_PLAINTEXT or SASL_SSL broker protocol".to_owned(),
            );
        }
        Ok(())
    }

    /// Build the existing typed `PostgreSQL` source configuration.
    ///
    /// # Panics
    ///
    /// Panics only if called before [`Self::validate`] with a batch size that
    /// does not fit `u32`.
    #[must_use]
    pub fn postgres_source(&self) -> PostgresSourceConfig {
        PostgresSourceConfig {
            database_url: self.postgres_url.clone(),
            slot_name: self.postgres_slot.clone(),
            publication_name: self.postgres_publication.clone(),
            schema: self.postgres_schema.clone(),
            table_names: self.postgres_tables.clone(),
            max_messages_per_poll: u32::try_from(self.batch_size)
                .expect("worker config validated before source construction"),
        }
    }

    /// Commit interval as a standard duration.
    #[must_use]
    pub const fn commit_interval(&self) -> Duration {
        Duration::from_millis(self.commit_interval_ms)
    }

    /// Empty-poll backoff as a standard duration.
    #[must_use]
    pub const fn poll_backoff(&self) -> Duration {
        Duration::from_millis(self.poll_backoff_ms)
    }

    /// Validated Kafka client security, or `None` for plaintext.
    ///
    /// # Errors
    ///
    /// Returns an error when the security fields are incomplete or inconsistent.
    pub fn client_security(&self) -> Result<Option<ClientSecurity>, String> {
        self.validate()?;
        let protocol: ListenerProtocol = self.broker_protocol.into();
        if protocol == ListenerProtocol::Plaintext {
            return Ok(None);
        }
        let tls = if protocol.requires_tls() {
            Some(TlsConnectorConfig {
                trust_roots_pem: self.broker_ca_path.clone(),
                server_name: self
                    .broker_server_name
                    .clone()
                    .ok_or_else(|| "TLS server name missing after validation".to_owned())?,
                client_identity: self
                    .broker_cert_path
                    .clone()
                    .zip(self.broker_key_path.clone()),
            })
        } else {
            None
        };
        let sasl = if protocol.requires_sasl() {
            let username = self
                .broker_sasl_username
                .clone()
                .ok_or_else(|| "SASL username missing after validation".to_owned())?;
            let password = self
                .broker_sasl_password
                .as_ref()
                .ok_or_else(|| "SASL password missing after validation".to_owned())?
                .expose_secret()
                .to_owned();
            Some(
                match self
                    .broker_sasl_mechanism
                    .ok_or_else(|| "SASL mechanism missing after validation".to_owned())?
                {
                    BrokerSaslMechanism::Plain => SaslCredentials::Plain { username, password },
                    BrokerSaslMechanism::ScramSha256 => SaslCredentials::Scram {
                        mechanism: SaslMechanism::ScramSha256,
                        username,
                        password,
                    },
                    BrokerSaslMechanism::ScramSha512 => SaslCredentials::Scram {
                        mechanism: SaslMechanism::ScramSha512,
                        username,
                        password,
                    },
                },
            )
        } else {
            None
        };
        Ok(Some(ClientSecurity {
            protocol,
            tls,
            sasl,
            sasl_host: self.broker_server_name.clone(),
        }))
    }
}

fn parse_non_empty(value: &str) -> Result<String, String> {
    if value.trim().is_empty() {
        Err("value must not be empty".to_owned())
    } else {
        Ok(value.to_owned())
    }
}

fn parse_secret(value: &str) -> Result<SecretString, String> {
    parse_non_empty(value).map(SecretString::new)
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value.parse::<usize>().map_err(|error| error.to_string())?;
    if parsed == 0 {
        Err("value must be greater than zero".to_owned())
    } else {
        Ok(parsed)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use clap::Parser as _;

    use super::*;

    fn base_args() -> Vec<&'static str> {
        vec![
            "crabka-connect-worker",
            "--connector-id=orders",
            "--kafka-bootstrap=localhost:9092",
            "--postgres-url=postgres://secret@localhost/app",
            "--postgres-slot=orders",
            "--postgres-tables=orders,customers",
        ]
    }

    #[test]
    fn parses_defaults_without_exposing_postgres_secret() {
        let config = WorkerConfig::try_parse_from(base_args()).expect("valid CLI");
        assert!(config.validate().is_ok());
        assert!(config.postgres_tables == ["orders", "customers"]);
        assert!(config.topic_prefix == "db");
        assert!(config.batch_size == 500);
        assert!(config.postgres_url.to_string() == "[REDACTED]");
    }

    #[test]
    fn builds_tls_scram_security_and_rejects_partial_mtls() {
        let mut args = base_args();
        args.extend([
            "--broker-protocol=SASL_SSL",
            "--broker-ca-path=ca.pem",
            "--broker-server-name=broker.example",
            "--broker-sasl-mechanism=SCRAM-SHA-512",
            "--broker-sasl-username=user",
            "--broker-sasl-password=secret",
        ]);
        let config = WorkerConfig::try_parse_from(args).expect("valid secure CLI");
        let security = config
            .client_security()
            .expect("valid security")
            .expect("secure protocol");
        assert!(security.protocol == ListenerProtocol::SaslSsl);
        assert!(matches!(
            security.sasl,
            Some(SaslCredentials::Scram {
                mechanism: SaslMechanism::ScramSha512,
                ..
            })
        ));

        let mut partial = base_args();
        partial.extend([
            "--broker-protocol=SSL",
            "--broker-ca-path=ca.pem",
            "--broker-server-name=broker.example",
            "--broker-cert-path=client.pem",
        ]);
        let config = WorkerConfig::try_parse_from(partial).expect("CLI shape parses");
        assert!(config.validate().is_err());
    }
}
