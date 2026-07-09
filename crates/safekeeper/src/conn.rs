//! Bounded physical-replication session state machine.
//!
//! The pinned `tokio-postgres 0.7` workspace dependency does not expose a
//! bidirectional physical-replication `CopyBoth` API. This module therefore owns
//! the small wire-protocol boundary needed by safekeeper: startup with
//! `replication=true`, simple replication commands, `CopyBothResponse`, and raw
//! `CopyData` frames.

use std::{
    fmt, io,
    io::{Read, Write},
    net::TcpStream,
    str::FromStr,
};

use thiserror::Error;

use crate::{
    Lsn,
    protocol::{CopyBothError, CopyBothMessage, StandbyStatusUpdate},
};

const SLOT_PREFIX: &str = "crabka_sk_";
const PG_IDENTIFIER_MAX_LEN: usize = 63;
const DEFAULT_POSTGRES_PORT: u16 = 5432;
const POSTGRES_PROTOCOL_VERSION_3: i32 = 196_608;
const AUTHENTICATION_OK: i32 = 0;
const AUTHENTICATION_CLEARTEXT_PASSWORD: i32 = 3;
const AUTHENTICATION_MD5_PASSWORD: i32 = 5;
const AUTHENTICATION_SASL: i32 = 10;
const MESSAGE_AUTHENTICATION: u8 = b'R';
const MESSAGE_BACKEND_KEY_DATA: u8 = b'K';
const MESSAGE_COMMAND_COMPLETE: u8 = b'C';
const MESSAGE_COPY_BOTH_RESPONSE: u8 = b'W';
const MESSAGE_COPY_DATA: u8 = b'd';
const MESSAGE_COPY_DONE: u8 = b'c';
const MESSAGE_DATA_ROW: u8 = b'D';
const MESSAGE_ERROR_RESPONSE: u8 = b'E';
const MESSAGE_PARAMETER_STATUS: u8 = b'S';
const MESSAGE_QUERY: u8 = b'Q';
const MESSAGE_READY_FOR_QUERY: u8 = b'Z';
const MESSAGE_ROW_DESCRIPTION: u8 = b'T';
const MESSAGE_NOTICE_RESPONSE: u8 = b'N';
const FIELD_SYSTEM_ID: usize = 0;
const FIELD_TIMELINE: usize = 1;
const FIELD_FLUSH_LSN: usize = 2;
const FIELD_CONSISTENT_POINT: usize = 1;

/// Result type for replication session operations.
pub type Result<T> = std::result::Result<T, ReplicationSessionError>;

/// Cluster name parsed into a safe `PostgreSQL` replication-slot suffix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationCluster(String);

impl ReplicationCluster {
    /// Parses a cluster name into the restricted identifier form used in slot names.
    pub fn parse(raw: &str) -> Result<Self> {
        if raw.is_empty() {
            return Err(ReplicationSessionError::InvalidClusterName {
                name: raw.to_owned(),
                reason: "cluster name must not be empty",
            });
        }

        if !raw.bytes().all(is_slot_suffix_byte) {
            return Err(ReplicationSessionError::InvalidClusterName {
                name: raw.to_owned(),
                reason: "cluster name must contain only ASCII letters, digits, or '_'",
            });
        }

        let slot_len = SLOT_PREFIX.len() + raw.len();
        if slot_len > PG_IDENTIFIER_MAX_LEN {
            return Err(ReplicationSessionError::SlotNameTooLong {
                name: raw.to_owned(),
                len: slot_len,
            });
        }

        Ok(Self(raw.to_owned()))
    }

    /// Returns the `PostgreSQL` physical replication slot name for this cluster.
    #[must_use]
    pub fn slot_name(&self) -> ReplicationSlotName {
        ReplicationSlotName(format!("{SLOT_PREFIX}{}", self.0))
    }
}

/// A parsed physical replication slot name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationSlotName(String);

impl ReplicationSlotName {
    /// Returns this slot name as `PostgreSQL` identifier text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ReplicationSlotName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Timeline identifier reported by the primary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimelineId(pub u32);

/// Result of `IDENTIFY_SYSTEM`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentifiedSystem {
    /// Primary system identifier.
    pub system_id: String,
    /// Current timeline for physical replication.
    pub timeline: TimelineId,
    /// Primary flush LSN returned by `IDENTIFY_SYSTEM`.
    pub flush_lsn: Lsn,
}

/// Commands the session may issue to the transport seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationCommand {
    /// `IDENTIFY_SYSTEM`.
    IdentifySystem,
    /// `CREATE_REPLICATION_SLOT <slot> PHYSICAL`.
    CreatePhysicalReplicationSlot { slot: ReplicationSlotName },
    /// `START_REPLICATION SLOT <slot> PHYSICAL <resume_lsn> TIMELINE <timeline>`.
    StartPhysicalReplication {
        slot: ReplicationSlotName,
        resume_lsn: Lsn,
        timeline: TimelineId,
    },
}

impl ReplicationCommand {
    /// Returns this command as the simple-query string sent to `PostgreSQL`.
    #[must_use]
    pub fn to_simple_query(&self) -> String {
        match self {
            Self::IdentifySystem => "IDENTIFY_SYSTEM".to_owned(),
            Self::CreatePhysicalReplicationSlot { slot } => {
                format!("CREATE_REPLICATION_SLOT {slot} PHYSICAL")
            }
            Self::StartPhysicalReplication {
                slot,
                resume_lsn,
                timeline,
            } => {
                let TimelineId(timeline) = *timeline;
                format!("START_REPLICATION SLOT {slot} PHYSICAL {resume_lsn} TIMELINE {timeline}")
            }
        }
    }
}

/// Transport outcome for a replication command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationCommandOutcome {
    /// Parsed `IDENTIFY_SYSTEM` row.
    IdentifiedSystem(IdentifiedSystem),
    /// Physical replication slot was created.
    CreatedPhysicalSlot { consistent_point: Lsn },
    /// Physical replication slot already existed; this is idempotent success.
    SlotAlreadyExists,
    /// `START_REPLICATION` entered `CopyBoth` mode on the requested timeline.
    CopyBothStarted { timeline: TimelineId },
}

/// Narrow IO seam for the physical replication session.
pub trait ReplicationTransport {
    /// Executes a replication simple-query command.
    fn execute_replication_command(
        &mut self,
        command: &ReplicationCommand,
    ) -> Result<ReplicationCommandOutcome>;

    /// Receives the next raw `CopyData` payload, or `None` after `CopyDone`/EOF.
    fn receive_copy_data(&mut self) -> Result<Option<Vec<u8>>>;

    /// Sends a raw standby `CopyData` payload.
    fn send_copy_data(&mut self, bytes: &[u8]) -> Result<()>;
}

/// Physical replication session over a narrow transport seam.
#[derive(Debug, Clone)]
pub struct ReplicationSession<T> {
    transport: T,
    slot: ReplicationSlotName,
    identified_system: Option<IdentifiedSystem>,
}

impl<T> ReplicationSession<T> {
    /// Creates a session over an already-connected transport seam.
    pub fn new(transport: T, cluster: &str) -> Result<Self> {
        let cluster = ReplicationCluster::parse(cluster)?;
        Ok(Self {
            transport,
            slot: cluster.slot_name(),
            identified_system: None,
        })
    }

    /// Consumes the session and returns the underlying transport.
    #[must_use]
    pub fn into_transport(self) -> T {
        self.transport
    }

    /// Returns the physical replication slot name this session manages.
    #[must_use]
    pub fn slot_name(&self) -> &ReplicationSlotName {
        &self.slot
    }
}

impl ReplicationSession<LivePostgresTransport> {
    /// Parses a live `PostgreSQL` connection plan and returns the explicit
    /// `CopyBoth` support error for the current workspace driver.
    pub fn connect(url: &str, cluster: &str) -> Result<Self> {
        let plan = LivePostgresConnectPlan::parse(url, cluster)?;
        LivePostgresTransport::connect(&plan).map(|transport| Self {
            slot: transport.slot.clone(),
            transport,
            identified_system: None,
        })
    }
}

impl<T> ReplicationSession<T>
where
    T: ReplicationTransport,
{
    /// Runs `IDENTIFY_SYSTEM` and records the primary timeline for later `START_REPLICATION`.
    pub fn identify(&mut self) -> Result<IdentifiedSystem> {
        let outcome = self
            .transport
            .execute_replication_command(&ReplicationCommand::IdentifySystem)?;
        let ReplicationCommandOutcome::IdentifiedSystem(system) = outcome else {
            return Err(ReplicationSessionError::UnexpectedCommandOutcome {
                command: "IDENTIFY_SYSTEM",
                outcome,
            });
        };

        self.identified_system = Some(system.clone());
        Ok(system)
    }

    /// Creates the physical slot, treating an existing slot as success.
    pub fn ensure_slot(&mut self) -> Result<()> {
        let command = ReplicationCommand::CreatePhysicalReplicationSlot {
            slot: self.slot.clone(),
        };
        let outcome = self.transport.execute_replication_command(&command)?;
        match outcome {
            ReplicationCommandOutcome::CreatedPhysicalSlot { .. }
            | ReplicationCommandOutcome::SlotAlreadyExists => Ok(()),
            other => Err(ReplicationSessionError::UnexpectedCommandOutcome {
                command: "CREATE_REPLICATION_SLOT",
                outcome: other,
            }),
        }
    }

    /// Starts `CopyBoth` physical WAL streaming at `resume_lsn`.
    pub fn start(&mut self, resume_lsn: Lsn) -> Result<()> {
        let identified_system = self
            .identified_system
            .as_ref()
            .ok_or(ReplicationSessionError::NotIdentified)?;
        let command = ReplicationCommand::StartPhysicalReplication {
            slot: self.slot.clone(),
            resume_lsn,
            timeline: identified_system.timeline,
        };
        let outcome = self.transport.execute_replication_command(&command)?;
        let ReplicationCommandOutcome::CopyBothStarted { timeline } = outcome else {
            return Err(ReplicationSessionError::UnexpectedCommandOutcome {
                command: "START_REPLICATION",
                outcome,
            });
        };

        if timeline != identified_system.timeline {
            return Err(ReplicationSessionError::TimelineSwitch {
                expected: identified_system.timeline,
                got: timeline,
            });
        }

        Ok(())
    }

    /// Receives and parses the next `CopyBoth` message.
    pub fn receive(&mut self) -> Result<Option<CopyBothMessage>> {
        let Some(bytes) = self.transport.receive_copy_data()? else {
            return Ok(None);
        };

        CopyBothMessage::parse(&bytes)
            .map(Some)
            .map_err(ReplicationSessionError::CopyBoth)
    }

    /// Sends a standby status update to the primary.
    pub fn send_status(&mut self, status: StandbyStatusUpdate) -> Result<()> {
        self.transport.send_copy_data(&status.encode())
    }
}

/// Parsed inputs for the best-available live `PostgreSQL` replication boundary.
#[derive(Debug, Clone)]
pub struct LivePostgresConnectPlan {
    url: String,
    config: tokio_postgres::Config,
    slot: ReplicationSlotName,
}

impl LivePostgresConnectPlan {
    /// Parses the `tokio-postgres` URL/config and safekeeper slot derivation.
    pub fn parse(url: &str, cluster: &str) -> Result<Self> {
        let cluster = ReplicationCluster::parse(cluster)?;
        let config = url.parse().map_err(|source: tokio_postgres::Error| {
            ReplicationSessionError::LivePostgresConfig {
                message: source.to_string(),
            }
        })?;

        Ok(Self {
            url: url.to_owned(),
            config,
            slot: cluster.slot_name(),
        })
    }

    /// Returns the parsed `tokio-postgres` connection config.
    #[must_use]
    pub fn config(&self) -> &tokio_postgres::Config {
        &self.config
    }

    /// Returns the original connection string used to derive raw transport prerequisites.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the physical replication slot this plan will manage.
    #[must_use]
    pub fn slot_name(&self) -> &ReplicationSlotName {
        &self.slot
    }
}

/// Best-available live `PostgreSQL` transport boundary for physical replication.
#[derive(Debug)]
pub struct LivePostgresTransport {
    slot: ReplicationSlotName,
    inner: PostgresReplicationTransport<TcpStream>,
}

impl LivePostgresTransport {
    /// Opens a live physical-replication connection using the safekeeper raw
    /// `CopyBoth` wire-protocol seam.
    ///
    /// This deliberately supports only the explicit prerequisites that can be
    /// implemented without a `CopyBoth` driver: TCP, `sslmode=disable`, and
    /// trust/no-password authentication. Other configurations fail before any
    /// replication command is issued.
    pub fn connect(plan: &LivePostgresConnectPlan) -> Result<Self> {
        let stream = connect_tcp_stream(plan)?;
        let inner = PostgresReplicationTransport::connect(stream, plan.config())?;
        Ok(Self {
            slot: plan.slot.clone(),
            inner,
        })
    }
}

impl ReplicationTransport for LivePostgresTransport {
    fn execute_replication_command(
        &mut self,
        command: &ReplicationCommand,
    ) -> Result<ReplicationCommandOutcome> {
        self.inner.execute_replication_command(command)
    }

    fn receive_copy_data(&mut self) -> Result<Option<Vec<u8>>> {
        self.inner.receive_copy_data()
    }

    fn send_copy_data(&mut self, bytes: &[u8]) -> Result<()> {
        self.inner.send_copy_data(bytes)
    }
}

/// Minimal blocking byte-stream seam used by the raw replication transport.
pub trait ReplicationByteStream {
    /// Reads exactly `bytes.len()` bytes or returns an IO failure.
    fn read_exact_bytes(&mut self, bytes: &mut [u8]) -> io::Result<()>;

    /// Writes the complete byte slice or returns an IO failure.
    fn write_all_bytes(&mut self, bytes: &[u8]) -> io::Result<()>;
}

impl ReplicationByteStream for TcpStream {
    fn read_exact_bytes(&mut self, bytes: &mut [u8]) -> io::Result<()> {
        self.read_exact(bytes)
    }

    fn write_all_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.write_all(bytes)
    }
}

/// Raw `PostgreSQL` physical-replication transport over an injectable byte stream.
#[derive(Debug)]
pub struct PostgresReplicationTransport<S> {
    stream: S,
}

impl<S> PostgresReplicationTransport<S>
where
    S: ReplicationByteStream,
{
    /// Performs `PostgreSQL` startup using `replication=true` and waits until the
    /// backend reports `ReadyForQuery`.
    pub fn connect(mut stream: S, config: &tokio_postgres::Config) -> Result<Self> {
        let startup = startup_message(config)?;
        stream
            .write_all_bytes(&startup)
            .map_err(|source| transport_io_error(&source))?;
        wait_for_startup_ready(&mut stream)?;
        Ok(Self { stream })
    }
}

impl<S> ReplicationTransport for PostgresReplicationTransport<S>
where
    S: ReplicationByteStream,
{
    fn execute_replication_command(
        &mut self,
        command: &ReplicationCommand,
    ) -> Result<ReplicationCommandOutcome> {
        write_query(&mut self.stream, &command.to_simple_query())?;
        read_command_outcome(&mut self.stream, command)
    }

    fn receive_copy_data(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            let frame = read_backend_message(&mut self.stream)?;
            match frame.tag {
                MESSAGE_COPY_DATA => return Ok(Some(frame.payload)),
                MESSAGE_COPY_DONE => return Ok(None),
                MESSAGE_ERROR_RESPONSE => return Err(error_response(&frame.payload)),
                MESSAGE_NOTICE_RESPONSE => {}
                other => {
                    return Err(ReplicationSessionError::Protocol {
                        message: format!(
                            "expected CopyData/CopyDone during CopyBoth, got backend message 0x{other:02X}"
                        ),
                    });
                }
            }
        }
    }

    fn send_copy_data(&mut self, bytes: &[u8]) -> Result<()> {
        write_tagged_message(&mut self.stream, MESSAGE_COPY_DATA, bytes)
    }
}

/// Errors returned by replication session orchestration.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplicationSessionError {
    /// The live raw transport requires a narrower `PostgreSQL` connection mode.
    #[error("live PostgreSQL replication prerequisite failed: {message}")]
    LivePostgresPrerequisite {
        /// Missing live transport prerequisite.
        message: String,
    },

    /// The live `tokio-postgres` connection configuration could not be parsed.
    #[error("invalid live PostgreSQL connection config: {message}")]
    LivePostgresConfig {
        /// Configuration parse error text.
        message: String,
    },

    /// Cluster name is not usable as the replication slot suffix.
    #[error("invalid replication cluster name {name:?}: {reason}")]
    InvalidClusterName {
        /// Rejected cluster name.
        name: String,
        /// Human-readable reason.
        reason: &'static str,
    },

    /// The generated slot identifier exceeds `PostgreSQL`'s identifier limit.
    #[error("replication slot name derived from {name:?} is too long: {len} bytes")]
    SlotNameTooLong {
        /// Rejected cluster name.
        name: String,
        /// Generated slot name length.
        len: usize,
    },

    /// `START_REPLICATION` was called before `IDENTIFY_SYSTEM`.
    #[error("replication session must identify the primary before START_REPLICATION")]
    NotIdentified,

    /// The transport returned a result that does not match the command.
    #[error("unexpected outcome for {command}: {outcome:?}")]
    UnexpectedCommandOutcome {
        /// Command being executed.
        command: &'static str,
        /// Unexpected transport outcome.
        outcome: ReplicationCommandOutcome,
    },

    /// The primary timeline changed between identify and streaming.
    #[error("primary timeline switched: expected {expected:?}, got {got:?}")]
    TimelineSwitch {
        /// Timeline reported by `IDENTIFY_SYSTEM`.
        expected: TimelineId,
        /// Timeline reported by `START_REPLICATION`.
        got: TimelineId,
    },

    /// A `CopyBoth` payload did not match the physical replication codec.
    #[error(transparent)]
    CopyBoth(#[from] CopyBothError),

    /// The transport seam failed.
    #[error("replication transport failed: {message}")]
    Transport {
        /// Transport error text.
        message: String,
    },

    /// The `PostgreSQL` wire protocol returned an unexpected frame.
    #[error("replication protocol failed: {message}")]
    Protocol {
        /// Protocol error text.
        message: String,
    },
}

fn is_slot_suffix_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn connect_tcp_stream(plan: &LivePostgresConnectPlan) -> Result<TcpStream> {
    require_ssl_disabled(plan.config())?;
    let (host, port) = tcp_endpoint(plan.url())?;
    TcpStream::connect((host.as_str(), port)).map_err(|source| ReplicationSessionError::Transport {
        message: format!("failed to connect to {host}:{port}: {source}"),
    })
}

fn require_ssl_disabled(config: &tokio_postgres::Config) -> Result<()> {
    if format!("{:?}", config.get_ssl_mode()) == "Disable" {
        return Ok(());
    }

    Err(ReplicationSessionError::LivePostgresPrerequisite {
        message: "raw CopyBoth transport currently requires sslmode=disable".to_owned(),
    })
}

fn tcp_endpoint(raw_url: &str) -> Result<(String, u16)> {
    if raw_url.starts_with("postgres://") || raw_url.starts_with("postgresql://") {
        return tcp_endpoint_from_url(raw_url);
    }

    tcp_endpoint_from_key_value(raw_url)
}

fn tcp_endpoint_from_url(raw_url: &str) -> Result<(String, u16)> {
    let after_scheme = raw_url
        .split_once("://")
        .map(|(_, rest)| rest)
        .ok_or_else(|| ReplicationSessionError::LivePostgresPrerequisite {
            message: "raw CopyBoth transport requires a PostgreSQL URL host".to_owned(),
        })?;
    let authority = after_scheme.split(['/', '?']).next().unwrap_or_default();
    let host_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    let first_host = host_port.split(',').next().unwrap_or_default();
    if first_host.is_empty() || first_host.starts_with("%2F") || first_host.starts_with("%2f") {
        return Err(ReplicationSessionError::LivePostgresPrerequisite {
            message: "raw CopyBoth transport requires an explicit TCP host".to_owned(),
        });
    }
    let (host, port) = split_host_port(first_host)?;
    Ok((host.to_owned(), port.unwrap_or(DEFAULT_POSTGRES_PORT)))
}

fn tcp_endpoint_from_key_value(raw_url: &str) -> Result<(String, u16)> {
    let host = key_value(raw_url, "host").ok_or_else(|| {
        ReplicationSessionError::LivePostgresPrerequisite {
            message: "raw CopyBoth transport requires an explicit TCP host".to_owned(),
        }
    })?;
    let first_host = host.split(',').next().unwrap_or_default();
    if first_host.starts_with('/') || first_host.is_empty() {
        return Err(ReplicationSessionError::LivePostgresPrerequisite {
            message: "raw CopyBoth transport does not yet support Unix sockets".to_owned(),
        });
    }
    let first_port = key_value(raw_url, "port")
        .and_then(|port| port.split(',').next())
        .filter(|port| !port.is_empty())
        .map(str::parse::<u16>)
        .transpose()
        .map_err(|source| ReplicationSessionError::LivePostgresPrerequisite {
            message: format!("raw CopyBoth transport received invalid port: {source}"),
        })?
        .unwrap_or(DEFAULT_POSTGRES_PORT);
    Ok((first_host.to_owned(), first_port))
}

fn split_host_port(host_port: &str) -> Result<(&str, Option<u16>)> {
    let Some((host, port)) = host_port.rsplit_once(':') else {
        return Ok((host_port, None));
    };
    if host.contains(']') && !host.starts_with('[') {
        return Ok((host_port, None));
    }
    let port = port.parse::<u16>().map_err(|source| {
        ReplicationSessionError::LivePostgresPrerequisite {
            message: format!("raw CopyBoth transport received invalid port: {source}"),
        }
    })?;
    Ok((host.trim_matches(['[', ']']), Some(port)))
}

fn key_value<'a>(raw: &'a str, key: &str) -> Option<&'a str> {
    raw.split_whitespace().find_map(|part| {
        let (candidate_key, candidate_value) = part.split_once('=')?;
        (candidate_key == key).then_some(candidate_value.trim_matches('\''))
    })
}

fn startup_message(config: &tokio_postgres::Config) -> Result<Vec<u8>> {
    let Some(user) = config.get_user() else {
        return Err(ReplicationSessionError::LivePostgresPrerequisite {
            message: "raw CopyBoth transport requires a configured user".to_owned(),
        });
    };

    let mut body = Vec::new();
    body.extend_from_slice(&POSTGRES_PROTOCOL_VERSION_3.to_be_bytes());
    push_startup_parameter(&mut body, "user", user);
    if let Some(application_name) = config.get_application_name() {
        push_startup_parameter(&mut body, "application_name", application_name);
    }
    push_startup_parameter(&mut body, "replication", "true");
    body.push(0);

    let total_len = checked_message_len(body.len())?;
    let mut message = Vec::with_capacity(body.len() + 4);
    message.extend_from_slice(&total_len.to_be_bytes());
    message.extend_from_slice(&body);
    Ok(message)
}

fn push_startup_parameter(bytes: &mut Vec<u8>, key: &str, value: &str) {
    bytes.extend_from_slice(key.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(value.as_bytes());
    bytes.push(0);
}

fn wait_for_startup_ready<S>(stream: &mut S) -> Result<()>
where
    S: ReplicationByteStream,
{
    loop {
        let frame = read_backend_message(stream)?;
        match frame.tag {
            MESSAGE_AUTHENTICATION => handle_authentication(&frame.payload)?,
            MESSAGE_PARAMETER_STATUS | MESSAGE_BACKEND_KEY_DATA | MESSAGE_NOTICE_RESPONSE => {}
            MESSAGE_READY_FOR_QUERY => return Ok(()),
            MESSAGE_ERROR_RESPONSE => return Err(error_response(&frame.payload)),
            other => {
                return Err(ReplicationSessionError::Protocol {
                    message: format!("unexpected startup backend message 0x{other:02X}"),
                });
            }
        }
    }
}

fn handle_authentication(payload: &[u8]) -> Result<()> {
    let auth_code = read_i32_payload(payload, "authentication code")?;
    if auth_code == AUTHENTICATION_OK {
        return Ok(());
    }

    let method = match auth_code {
        AUTHENTICATION_CLEARTEXT_PASSWORD => "cleartext password",
        AUTHENTICATION_MD5_PASSWORD => "MD5 password",
        AUTHENTICATION_SASL => "SASL/SCRAM password",
        _ => "unknown authentication",
    };
    Err(ReplicationSessionError::LivePostgresPrerequisite {
        message: format!(
            "raw CopyBoth transport currently supports only trust authentication; server requested {method}"
        ),
    })
}

fn write_query<S>(stream: &mut S, query: &str) -> Result<()>
where
    S: ReplicationByteStream,
{
    let mut payload = Vec::with_capacity(query.len() + 1);
    payload.extend_from_slice(query.as_bytes());
    payload.push(0);
    write_tagged_message(stream, MESSAGE_QUERY, &payload)
}

fn write_tagged_message<S>(stream: &mut S, tag: u8, payload: &[u8]) -> Result<()>
where
    S: ReplicationByteStream,
{
    let total_len = checked_message_len(payload.len())?;
    let mut message = Vec::with_capacity(payload.len() + 5);
    message.push(tag);
    message.extend_from_slice(&total_len.to_be_bytes());
    message.extend_from_slice(payload);
    stream
        .write_all_bytes(&message)
        .map_err(|source| transport_io_error(&source))
}

fn read_command_outcome<S>(
    stream: &mut S,
    command: &ReplicationCommand,
) -> Result<ReplicationCommandOutcome>
where
    S: ReplicationByteStream,
{
    let mut rows = Vec::new();
    loop {
        let frame = read_backend_message(stream)?;
        match frame.tag {
            MESSAGE_ROW_DESCRIPTION | MESSAGE_COMMAND_COMPLETE | MESSAGE_NOTICE_RESPONSE => {}
            MESSAGE_DATA_ROW => rows.push(parse_data_row(&frame.payload)?),
            MESSAGE_COPY_BOTH_RESPONSE => return parse_copy_both_started(&frame.payload, command),
            MESSAGE_READY_FOR_QUERY => return parse_ready_command_outcome(command, &rows),
            MESSAGE_ERROR_RESPONSE => return Err(error_response(&frame.payload)),
            other => {
                return Err(ReplicationSessionError::Protocol {
                    message: format!("unexpected query backend message 0x{other:02X}"),
                });
            }
        }
    }
}

fn parse_ready_command_outcome(
    command: &ReplicationCommand,
    rows: &[Vec<Option<String>>],
) -> Result<ReplicationCommandOutcome> {
    match command {
        ReplicationCommand::IdentifySystem => parse_identify_system(rows),
        ReplicationCommand::CreatePhysicalReplicationSlot { .. } => parse_create_slot(rows),
        ReplicationCommand::StartPhysicalReplication { .. } => {
            Err(ReplicationSessionError::Protocol {
                message: "START_REPLICATION reached ReadyForQuery before CopyBothResponse"
                    .to_owned(),
            })
        }
    }
}

fn parse_identify_system(rows: &[Vec<Option<String>>]) -> Result<ReplicationCommandOutcome> {
    let row = single_row(rows, "IDENTIFY_SYSTEM")?;
    let system_id = required_text_field(row, FIELD_SYSTEM_ID, "system_id")?.to_owned();
    let timeline = required_text_field(row, FIELD_TIMELINE, "timeline")?
        .parse::<u32>()
        .map_err(|source| ReplicationSessionError::Protocol {
            message: format!("invalid IDENTIFY_SYSTEM timeline: {source}"),
        })?;
    let flush_lsn =
        Lsn::from_str(required_text_field(row, FIELD_FLUSH_LSN, "xlogpos")?).map_err(|source| {
            ReplicationSessionError::Protocol {
                message: format!("invalid IDENTIFY_SYSTEM flush LSN: {source}"),
            }
        })?;

    Ok(ReplicationCommandOutcome::IdentifiedSystem(
        IdentifiedSystem {
            system_id,
            timeline: TimelineId(timeline),
            flush_lsn,
        },
    ))
}

fn parse_create_slot(rows: &[Vec<Option<String>>]) -> Result<ReplicationCommandOutcome> {
    let row = single_row(rows, "CREATE_REPLICATION_SLOT")?;
    let consistent_point = Lsn::from_str(required_text_field(
        row,
        FIELD_CONSISTENT_POINT,
        "consistent_point",
    )?)
    .map_err(|source| ReplicationSessionError::Protocol {
        message: format!("invalid CREATE_REPLICATION_SLOT consistent point: {source}"),
    })?;

    Ok(ReplicationCommandOutcome::CreatedPhysicalSlot { consistent_point })
}

fn parse_copy_both_started(
    payload: &[u8],
    command: &ReplicationCommand,
) -> Result<ReplicationCommandOutcome> {
    let ReplicationCommand::StartPhysicalReplication { timeline, .. } = command else {
        return Err(ReplicationSessionError::Protocol {
            message: "CopyBothResponse received for non-streaming replication command".to_owned(),
        });
    };
    if payload.len() < 3 {
        return Err(ReplicationSessionError::Protocol {
            message: format!("CopyBothResponse is truncated: got {} bytes", payload.len()),
        });
    }

    Ok(ReplicationCommandOutcome::CopyBothStarted {
        timeline: *timeline,
    })
}

fn single_row<'a>(rows: &'a [Vec<Option<String>>], command: &str) -> Result<&'a [Option<String>]> {
    let [row] = rows else {
        return Err(ReplicationSessionError::Protocol {
            message: format!("{command} expected one data row, got {}", rows.len()),
        });
    };
    Ok(row)
}

fn required_text_field<'a>(
    row: &'a [Option<String>],
    index: usize,
    field: &str,
) -> Result<&'a str> {
    row.get(index)
        .and_then(Option::as_deref)
        .ok_or_else(|| ReplicationSessionError::Protocol {
            message: format!("missing text field {field}"),
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BackendMessage {
    tag: u8,
    payload: Vec<u8>,
}

fn read_backend_message<S>(stream: &mut S) -> Result<BackendMessage>
where
    S: ReplicationByteStream,
{
    let mut tag = [0_u8; 1];
    stream
        .read_exact_bytes(&mut tag)
        .map_err(|source| transport_io_error(&source))?;
    let mut len = [0_u8; 4];
    stream
        .read_exact_bytes(&mut len)
        .map_err(|source| transport_io_error(&source))?;
    let len = i32::from_be_bytes(len);
    if len < 4 {
        return Err(ReplicationSessionError::Protocol {
            message: format!("backend message 0x{:02X} has invalid length {len}", tag[0]),
        });
    }

    let payload_len =
        usize::try_from(len - 4).map_err(|source| ReplicationSessionError::Protocol {
            message: format!("backend message length does not fit usize: {source}"),
        })?;
    let mut payload = vec![0_u8; payload_len];
    stream
        .read_exact_bytes(&mut payload)
        .map_err(|source| transport_io_error(&source))?;
    Ok(BackendMessage {
        tag: tag[0],
        payload,
    })
}

fn parse_data_row(payload: &[u8]) -> Result<Vec<Option<String>>> {
    if payload.len() < 2 {
        return Err(ReplicationSessionError::Protocol {
            message: "DataRow missing column count".to_owned(),
        });
    }
    let field_count = usize::from(u16::from_be_bytes([payload[0], payload[1]]));
    let mut fields = Vec::with_capacity(field_count);
    let mut offset = 2;
    for _ in 0..field_count {
        let len = read_i32_at(payload, offset, "DataRow field length")?;
        offset += 4;
        if len == -1 {
            fields.push(None);
            continue;
        }
        if len < 0 {
            return Err(ReplicationSessionError::Protocol {
                message: format!("DataRow field has invalid length {len}"),
            });
        }
        let field_len =
            usize::try_from(len).map_err(|source| ReplicationSessionError::Protocol {
                message: format!("DataRow field length does not fit usize: {source}"),
            })?;
        let end =
            offset
                .checked_add(field_len)
                .ok_or_else(|| ReplicationSessionError::Protocol {
                    message: "DataRow field length overflow".to_owned(),
                })?;
        let raw = payload
            .get(offset..end)
            .ok_or_else(|| ReplicationSessionError::Protocol {
                message: "DataRow field is truncated".to_owned(),
            })?;
        let value =
            std::str::from_utf8(raw).map_err(|source| ReplicationSessionError::Protocol {
                message: format!("DataRow field is not UTF-8: {source}"),
            })?;
        fields.push(Some(value.to_owned()));
        offset = end;
    }
    if offset == payload.len() {
        return Ok(fields);
    }

    Err(ReplicationSessionError::Protocol {
        message: format!("DataRow has {} trailing bytes", payload.len() - offset),
    })
}

fn error_response(payload: &[u8]) -> ReplicationSessionError {
    ReplicationSessionError::Transport {
        message: format!(
            "PostgreSQL error response: {}",
            parse_error_message(payload)
        ),
    }
}

fn parse_error_message(payload: &[u8]) -> String {
    let mut offset = 0;
    let mut severity = None;
    let mut message = None;
    while offset < payload.len() {
        let field_type = payload[offset];
        offset += 1;
        if field_type == 0 {
            break;
        }
        let Some(end) = payload[offset..].iter().position(|byte| *byte == 0) else {
            break;
        };
        let value = String::from_utf8_lossy(&payload[offset..offset + end]).into_owned();
        match field_type {
            b'S' => severity = Some(value),
            b'M' => message = Some(value),
            _ => {}
        }
        offset += end + 1;
    }
    match (severity, message) {
        (Some(severity), Some(message)) => format!("{severity}: {message}"),
        (None, Some(message)) => message,
        _ => "unparseable error response".to_owned(),
    }
}

fn checked_message_len(payload_len: usize) -> Result<i32> {
    let total_len =
        payload_len
            .checked_add(4)
            .ok_or_else(|| ReplicationSessionError::Protocol {
                message: "frontend message length overflow".to_owned(),
            })?;
    i32::try_from(total_len).map_err(|source| ReplicationSessionError::Protocol {
        message: format!("frontend message length exceeds PostgreSQL i32 limit: {source}"),
    })
}

fn read_i32_payload(payload: &[u8], field: &str) -> Result<i32> {
    read_i32_at(payload, 0, field)
}

fn read_i32_at(payload: &[u8], offset: usize, field: &str) -> Result<i32> {
    let bytes =
        payload
            .get(offset..offset + 4)
            .ok_or_else(|| ReplicationSessionError::Protocol {
                message: format!("{field} is truncated"),
            })?;
    Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn transport_io_error(source: &io::Error) -> ReplicationSessionError {
    ReplicationSessionError::Transport {
        message: source.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;

    use super::*;
    use crate::protocol::{PrimaryKeepalive, XLogData};

    #[derive(Debug, Default)]
    struct ScriptedTransport {
        outcomes: Vec<ReplicationCommandOutcome>,
        commands: Vec<ReplicationCommand>,
        inbound_copy_data: Vec<Vec<u8>>,
        outbound_copy_data: Vec<Vec<u8>>,
    }

    impl ScriptedTransport {
        fn with_outcomes(outcomes: Vec<ReplicationCommandOutcome>) -> Self {
            Self {
                outcomes: outcomes.into_iter().rev().collect(),
                ..Self::default()
            }
        }
    }

    #[derive(Debug, Default)]
    struct MemoryByteStream {
        read: Vec<u8>,
        written: Vec<u8>,
        offset: usize,
    }

    impl MemoryByteStream {
        fn with_backend_messages(messages: Vec<(u8, Vec<u8>)>) -> Self {
            let mut read = Vec::new();
            for (tag, payload) in messages {
                push_backend_message(&mut read, tag, &payload);
            }
            Self {
                read,
                ..Self::default()
            }
        }
    }

    impl ReplicationByteStream for MemoryByteStream {
        fn read_exact_bytes(&mut self, bytes: &mut [u8]) -> io::Result<()> {
            let end = self.offset + bytes.len();
            if end > self.read.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "test EOF"));
            }
            bytes.copy_from_slice(&self.read[self.offset..end]);
            self.offset = end;
            Ok(())
        }

        fn write_all_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
            self.written.extend_from_slice(bytes);
            Ok(())
        }
    }

    fn push_backend_message(bytes: &mut Vec<u8>, tag: u8, payload: &[u8]) {
        bytes.push(tag);
        let len = i32::try_from(payload.len() + 4).expect("test message length fits i32");
        bytes.extend_from_slice(&len.to_be_bytes());
        bytes.extend_from_slice(payload);
    }

    fn authentication_ok() -> (u8, Vec<u8>) {
        (
            MESSAGE_AUTHENTICATION,
            AUTHENTICATION_OK.to_be_bytes().to_vec(),
        )
    }

    fn ready_for_query() -> (u8, Vec<u8>) {
        (MESSAGE_READY_FOR_QUERY, vec![b'I'])
    }

    fn copy_both_response() -> (u8, Vec<u8>) {
        (MESSAGE_COPY_BOTH_RESPONSE, vec![0, 0, 0])
    }

    fn data_row(fields: &[&str]) -> (u8, Vec<u8>) {
        let mut payload = Vec::new();
        let field_count = u16::try_from(fields.len()).expect("test field count fits u16");
        payload.extend_from_slice(&field_count.to_be_bytes());
        for field in fields {
            let len = i32::try_from(field.len()).expect("test field length fits i32");
            payload.extend_from_slice(&len.to_be_bytes());
            payload.extend_from_slice(field.as_bytes());
        }
        (MESSAGE_DATA_ROW, payload)
    }

    fn test_config() -> tokio_postgres::Config {
        "host=localhost user=replicator sslmode=disable application_name=crabka-test"
            .parse()
            .expect("test config parses")
    }

    impl ReplicationTransport for ScriptedTransport {
        fn execute_replication_command(
            &mut self,
            command: &ReplicationCommand,
        ) -> Result<ReplicationCommandOutcome> {
            self.commands.push(command.clone());
            self.outcomes
                .pop()
                .ok_or(ReplicationSessionError::Transport {
                    message: "script exhausted".to_owned(),
                })
        }

        fn receive_copy_data(&mut self) -> Result<Option<Vec<u8>>> {
            if self.inbound_copy_data.is_empty() {
                return Ok(None);
            }

            Ok(Some(self.inbound_copy_data.remove(0)))
        }

        fn send_copy_data(&mut self, bytes: &[u8]) -> Result<()> {
            self.outbound_copy_data.push(bytes.to_vec());
            Ok(())
        }
    }

    #[test]
    fn cluster_name_parses_to_bounded_slot_identifier() {
        let cluster = ReplicationCluster::parse("cluster_7");
        assert!(let Ok(cluster) = cluster);

        assert!(cluster.slot_name().as_str() == "crabka_sk_cluster_7");
        assert!(let Err(ReplicationSessionError::InvalidClusterName { .. }) = ReplicationCluster::parse("bad-name"));
    }

    #[test]
    fn commands_render_postgres_replication_queries() {
        let slot = ReplicationCluster::parse("alpha")
            .expect("valid test cluster")
            .slot_name();

        let command = ReplicationCommand::StartPhysicalReplication {
            slot,
            resume_lsn: Lsn(0x16_B374_D848),
            timeline: TimelineId(3),
        };

        assert!(
            command.to_simple_query()
                == "START_REPLICATION SLOT crabka_sk_alpha PHYSICAL 16/B374D848 TIMELINE 3"
        );
    }

    #[test]
    fn ensure_slot_treats_existing_slot_as_success() {
        let transport = ScriptedTransport::with_outcomes(vec![
            ReplicationCommandOutcome::SlotAlreadyExists,
            ReplicationCommandOutcome::SlotAlreadyExists,
        ]);
        let mut session = ReplicationSession::new(transport, "alpha").expect("valid test session");

        assert!(session.ensure_slot() == Ok(()));
        assert!(session.ensure_slot() == Ok(()));

        let transport = session.into_transport();
        assert!(transport.commands.len() == 2);
        assert!(
            transport.commands[0].to_simple_query()
                == "CREATE_REPLICATION_SLOT crabka_sk_alpha PHYSICAL"
        );
    }

    #[test]
    fn start_requires_identify_and_halts_on_timeline_switch() {
        let transport = ScriptedTransport::with_outcomes(vec![
            ReplicationCommandOutcome::IdentifiedSystem(IdentifiedSystem {
                system_id: "sys".to_owned(),
                timeline: TimelineId(7),
                flush_lsn: Lsn(10),
            }),
            ReplicationCommandOutcome::CopyBothStarted {
                timeline: TimelineId(8),
            },
        ]);
        let mut session = ReplicationSession::new(transport, "alpha").expect("valid test session");

        assert!(session.identify().is_ok());

        assert!(
            session.start(Lsn(10))
                == Err(ReplicationSessionError::TimelineSwitch {
                    expected: TimelineId(7),
                    got: TimelineId(8)
                })
        );
    }

    #[test]
    fn copyboth_receive_and_status_send_use_protocol_codec() {
        let mut transport = ScriptedTransport::default();
        transport.inbound_copy_data.push(
            CopyBothMessage::XLogData(XLogData {
                wal_start: Lsn(100),
                wal_end: Lsn(103),
                send_time: 1,
                data: Bytes::from_static(b"wal"),
            })
            .encode(),
        );
        transport.inbound_copy_data.push(
            CopyBothMessage::PrimaryKeepalive(PrimaryKeepalive {
                wal_end: Lsn(103),
                send_time: 2,
                reply_requested: true,
            })
            .encode(),
        );
        let mut session = ReplicationSession::new(transport, "alpha").expect("valid test session");

        assert!(matches!(
            session.receive(),
            Ok(Some(CopyBothMessage::XLogData(_)))
        ));
        assert!(matches!(
            session.receive(),
            Ok(Some(CopyBothMessage::PrimaryKeepalive(PrimaryKeepalive {
                reply_requested: true,
                ..
            })))
        ));

        let status = StandbyStatusUpdate {
            write_lsn: Lsn(103),
            flush_lsn: Lsn(103),
            apply_lsn: Lsn(103),
            client_time: 3,
            reply_requested: false,
        };
        assert!(session.send_status(status) == Ok(()));

        let transport = session.into_transport();
        assert!(transport.outbound_copy_data == vec![status.encode()]);
    }

    #[test]
    fn raw_transport_startup_sends_replication_parameter_and_reads_copyboth() {
        let stream = MemoryByteStream::with_backend_messages(vec![
            authentication_ok(),
            ready_for_query(),
            data_row(&["sys", "7", "0/10", "db"]),
            ready_for_query(),
            data_row(&["crabka_sk_alpha", "0/10", "", ""]),
            ready_for_query(),
            copy_both_response(),
            (
                MESSAGE_COPY_DATA,
                PrimaryKeepalive {
                    wal_end: Lsn(16),
                    send_time: 99,
                    reply_requested: true,
                }
                .encode(),
            ),
        ]);
        let mut transport = PostgresReplicationTransport::connect(stream, &test_config())
            .expect("startup handshake succeeds");

        assert!(
            transport.execute_replication_command(&ReplicationCommand::IdentifySystem)
                == Ok(ReplicationCommandOutcome::IdentifiedSystem(
                    IdentifiedSystem {
                        system_id: "sys".to_owned(),
                        timeline: TimelineId(7),
                        flush_lsn: Lsn(16),
                    }
                ))
        );
        assert!(matches!(
            transport.execute_replication_command(
                &ReplicationCommand::CreatePhysicalReplicationSlot {
                    slot: ReplicationCluster::parse("alpha")
                        .expect("valid cluster")
                        .slot_name(),
                }
            ),
            Ok(ReplicationCommandOutcome::CreatedPhysicalSlot {
                consistent_point: Lsn(16)
            })
        ));
        assert!(
            transport.execute_replication_command(&ReplicationCommand::StartPhysicalReplication {
                slot: ReplicationCluster::parse("alpha")
                    .expect("valid cluster")
                    .slot_name(),
                resume_lsn: Lsn(16),
                timeline: TimelineId(7),
            }) == Ok(ReplicationCommandOutcome::CopyBothStarted {
                timeline: TimelineId(7)
            })
        );
        assert!(
            transport.receive_copy_data()
                == Ok(Some(
                    PrimaryKeepalive {
                        wal_end: Lsn(16),
                        send_time: 99,
                        reply_requested: true,
                    }
                    .encode()
                ))
        );

        let stream = transport.stream;
        let written = String::from_utf8_lossy(&stream.written);
        assert!(written.contains("user\0replicator\0"));
        assert!(written.contains("replication\0true\0"));
        assert!(written.contains("IDENTIFY_SYSTEM\0"));
        assert!(written.contains("CREATE_REPLICATION_SLOT crabka_sk_alpha PHYSICAL\0"));
        assert!(
            written.contains("START_REPLICATION SLOT crabka_sk_alpha PHYSICAL 0/10 TIMELINE 7\0")
        );
    }

    #[test]
    fn raw_transport_rejects_password_auth_before_queries() {
        let stream = MemoryByteStream::with_backend_messages(vec![(
            MESSAGE_AUTHENTICATION,
            AUTHENTICATION_MD5_PASSWORD.to_be_bytes().to_vec(),
        )]);

        let result = PostgresReplicationTransport::connect(stream, &test_config());

        assert!(matches!(
            result,
            Err(ReplicationSessionError::LivePostgresPrerequisite { message })
                if message.contains("trust authentication")
        ));
    }
}
