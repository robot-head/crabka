//! Capture-backed `PostgreSQL` driver startup and pooler replay fixtures.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
};

use serde::Deserialize;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub const SCHEMA_VERSION: u32 = 1;
const ALLOWED_STARTUP_KEYS: &[&str] = &[
    "DateStyle",
    "TimeZone",
    "application_name",
    "client_encoding",
    "extra_float_digits",
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    pub schema_version: u32,
    pub captured_on: String,
    pub recapture_command: String,
    pub pgdog: PgDogPin,
    pub drivers: Vec<DriverCapture>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgDogPin {
    pub version: String,
    pub image: String,
    pub commit: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverCapture {
    pub driver: String,
    pub version: String,
    pub lock_source: String,
    pub capture_target: String,
    pub startup_parameters: BTreeMap<String, String>,
    pub pgdog_backend_set_batches: Vec<String>,
}

#[derive(Debug, Error)]
pub enum FixtureError {
    #[error("invalid driver-golden JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("driver-golden invariant failed: {0}")]
    Invariant(String),
}

pub fn parse_and_validate(
    text: &str,
    cargo_lock: &str,
    python_requirements: &str,
) -> Result<Fixture, FixtureError> {
    let fixture: Fixture = serde_json::from_str(text)?;
    validate_fixture(&fixture, text, cargo_lock, python_requirements)?;
    Ok(fixture)
}

pub fn validate(
    text: &str,
    cargo_lock: &str,
    python_requirements: &str,
) -> Result<(), FixtureError> {
    parse_and_validate(text, cargo_lock, python_requirements).map(drop)
}

fn validate_fixture(
    fixture: &Fixture,
    source: &str,
    cargo_lock: &str,
    python_requirements: &str,
) -> Result<(), FixtureError> {
    validate_provenance(fixture)?;
    validate_driver_captures(fixture)?;
    validate_dependency_pins(cargo_lock, python_requirements)?;
    validate_payload_safety(source)
}

fn validate_provenance(fixture: &Fixture) -> Result<(), FixtureError> {
    require(
        fixture.schema_version == SCHEMA_VERSION,
        "schema_version must be 1",
    )?;
    require(
        is_iso_date(&fixture.captured_on),
        "captured_on must be YYYY-MM-DD",
    )?;
    require(
        fixture.recapture_command == "python3 tools/capture-gres-driver-goldens.py --write",
        "recapture_command is not deterministic",
    )?;
    require(
        fixture.pgdog.version == "0.1.6",
        "PgDog version must be 0.1.6",
    )?;
    require(
        fixture.pgdog.commit == "c99282e",
        "PgDog commit must be c99282e",
    )?;
    require(
        fixture.pgdog.image.contains("0.1.6") && !fixture.pgdog.image.contains("latest"),
        "PgDog image must carry the pinned version",
    )?;

    Ok(())
}

fn validate_driver_captures(fixture: &Fixture) -> Result<(), FixtureError> {
    let expected = BTreeMap::from([
        ("tokio-postgres", "0.7.18"),
        ("sqlx", "0.9.0"),
        ("psycopg", "3.2.9"),
    ]);
    let mut seen = BTreeSet::new();
    for capture in &fixture.drivers {
        let Some(version) = expected.get(capture.driver.as_str()) else {
            return Err(invariant(format!("unexpected driver {}", capture.driver)));
        };
        require(
            capture.version == *version,
            "driver version does not match the pin",
        )?;
        require(
            seen.insert(capture.driver.as_str()),
            "duplicate driver capture",
        )?;
        require(
            capture.capture_target
                == "postgres:18 via payload-safe TCP recorder; PgDog 0.1.6 backend via the same recorder",
            "capture target must name both observed paths",
        )?;
        require(
            !capture.lock_source.is_empty(),
            "lock_source must be populated",
        )?;
        if capture.driver == "psycopg" {
            require(
                capture
                    .lock_source
                    .ends_with("2fbb46fcd17bc81f993f28c47f1ebea38d66ae97cc2dbc3cad73b37cefbff700"),
                "psycopg fixture checksum differs from requirements",
            )?;
        }
        for key in capture.startup_parameters.keys() {
            require(
                ALLOWED_STARTUP_KEYS.contains(&key.as_str()),
                "startup key is not allowlisted",
            )?;
        }
        for batch in &capture.pgdog_backend_set_batches {
            let normalized = batch.trim_start().to_ascii_uppercase();
            require(
                normalized.starts_with("SET ") || normalized.starts_with("SET\n"),
                "backend SQL batch must contain only SET statements",
            )?;
            require(!batch.contains('\0'), "backend SQL batch contains a NUL")?;
        }
    }
    require(
        seen.len() == expected.len(),
        "one exact capture per pinned driver is required",
    )?;

    Ok(())
}

fn validate_dependency_pins(
    cargo_lock: &str,
    python_requirements: &str,
) -> Result<(), FixtureError> {
    for (name, version) in [("tokio-postgres", "0.7.18"), ("sqlx", "0.9.0")] {
        let package = format!("name = \"{name}\"\nversion = \"{version}\"");
        require(
            cargo_lock.contains(&package),
            "Rust driver pin differs from Cargo.lock",
        )?;
    }
    require(
        python_requirements
            .lines()
            .any(|line| line.starts_with("psycopg==3.2.9 ")),
        "psycopg pin differs from requirements-driver-smoke.txt",
    )?;

    Ok(())
}

fn validate_payload_safety(source: &str) -> Result<(), FixtureError> {
    let lowered = source.to_ascii_lowercase();
    for forbidden in [
        "password",
        "database",
        "username",
        "postgres://",
        "postgresql://",
        "alice",
        "bob-secret",
    ] {
        require(
            !lowered.contains(forbidden),
            "fixture contains forbidden identity or secret material",
        )?;
    }
    Ok(())
}

fn require(condition: bool, message: &'static str) -> Result<(), FixtureError> {
    condition.then_some(()).ok_or_else(|| invariant(message))
}

fn invariant(message: impl Into<String>) -> FixtureError {
    FixtureError::Invariant(message.into())
}

fn is_iso_date(value: &str) -> bool {
    value.len() == 10
        && value.bytes().enumerate().all(|(index, byte)| match index {
            4 | 7 => byte == b'-',
            _ => byte.is_ascii_digit(),
        })
}

pub fn startup_packet(
    user: &str,
    dbname: &str,
    parameters: &BTreeMap<String, String>,
) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    body.extend_from_slice(&196_608_u32.to_be_bytes());
    append_startup_field(&mut body, "user", user)?;
    append_startup_field(&mut body, "database", dbname)?;
    for (name, value) in parameters {
        append_startup_field(&mut body, name, value)?;
    }
    body.push(0);
    let length = u32::try_from(body.len() + 4)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "startup packet is too large"))?;
    let mut packet = Vec::with_capacity(length as usize);
    packet.extend_from_slice(&length.to_be_bytes());
    packet.extend_from_slice(&body);
    Ok(packet)
}

fn append_startup_field(out: &mut Vec<u8>, name: &str, value: &str) -> io::Result<()> {
    if name.as_bytes().contains(&0) || value.as_bytes().contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "startup field contains a NUL",
        ));
    }
    out.extend_from_slice(name.as_bytes());
    out.push(0);
    out.extend_from_slice(value.as_bytes());
    out.push(0);
    Ok(())
}

pub async fn replay_startup(
    host: &str,
    port: u16,
    user: &str,
    dbname: &str,
    parameters: &BTreeMap<String, String>,
) -> io::Result<()> {
    let mut stream = TcpStream::connect((host, port)).await?;
    stream
        .write_all(&startup_packet(user, dbname, parameters)?)
        .await?;
    loop {
        let kind = stream.read_u8().await?;
        let length = stream.read_u32().await?;
        if !(4..=16 * 1024 * 1024).contains(&length) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid backend message length",
            ));
        }
        let mut payload = vec![0; (length - 4) as usize];
        stream.read_exact(&mut payload).await?;
        match kind {
            b'R' if payload == 0_u32.to_be_bytes() => {}
            b'R' => {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "driver-golden replay target requires authentication; use a trust-auth Gres endpoint",
                ));
            }
            b'E' => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Gres rejected captured startup parameters: {}",
                        error_message(&payload)
                    ),
                ));
            }
            b'Z' => return Ok(()),
            _ => {}
        }
    }
}

fn error_message(payload: &[u8]) -> String {
    payload
        .split(|byte| *byte == 0)
        .find_map(|field| field.strip_prefix(b"M"))
        .and_then(|message| std::str::from_utf8(message).ok())
        .unwrap_or("unknown startup error")
        .to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::parse_and_validate;

    #[test]
    fn checked_fixture_matches_pinned_dependencies_and_safe_schema() {
        let fixture = include_str!("../fixtures/driver-connect-v1.json");
        super::validate(
            fixture,
            include_str!("../../../Cargo.lock"),
            include_str!("../requirements-driver-smoke.txt"),
        )
        .expect("checked driver fixture must remain valid");
    }

    #[test]
    fn unsafe_or_drifted_fixture_is_rejected() {
        let safe = include_str!("../fixtures/driver-connect-v1.json");
        let unsafe_fixture = safe.replace("client_encoding", "password");
        assert!(
            parse_and_validate(
                &unsafe_fixture,
                include_str!("../../../Cargo.lock"),
                include_str!("../requirements-driver-smoke.txt")
            )
            .is_err()
        );
        let drifted = safe.replace("0.7.18", "0.7.17");
        assert!(
            parse_and_validate(
                &drifted,
                include_str!("../../../Cargo.lock"),
                include_str!("../requirements-driver-smoke.txt")
            )
            .is_err()
        );
    }

    #[test]
    fn replay_startup_packet_contains_identity_and_exact_captured_parameters() {
        let parameters = BTreeMap::from([("client_encoding".to_string(), "UTF8".to_string())]);
        let packet = super::startup_packet("fixture_user", "fixture_db", &parameters).unwrap();
        assert_eq!(
            u32::from_be_bytes(packet[..4].try_into().unwrap()) as usize,
            packet.len()
        );
        assert!(
            packet
                .windows(b"client_encoding\0UTF8\0".len())
                .any(|window| window == b"client_encoding\0UTF8\0")
        );
    }
}
