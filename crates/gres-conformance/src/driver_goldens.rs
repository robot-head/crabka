//! Capture-backed `PostgreSQL` driver startup and pooler replay fixtures.

use std::{collections::BTreeMap, io};

use crabka_units::{ByteSize, convert::ByteSizeExt as _, mebibytes};
use serde::{
    Deserialize, Deserializer,
    de::{MapAccess, Visitor},
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub const SCHEMA_VERSION: u32 = 2;
const PGDOG_IMAGE: &str = "ghcr.io/pgdogdev/pgdog@sha256:5d21fa668d091ae6ce30e5cb1536c7bcaba1f96b0d492227b1a46852d1f3ab2c";
const PGDOG_IMAGE_ID: &str =
    "sha256:5d21fa668d091ae6ce30e5cb1536c7bcaba1f96b0d492227b1a46852d1f3ab2c";
const PGDOG_REVISION: &str = "c99282e9001f66194b03b108ba2a66ad7a27a75d";
const POSTGRES_IMAGE: &str =
    "postgres@sha256:22c89fe0d0f507606260237fd55e51f6137f58b2d5bcf6152242b96d9fe8f9a4";
const POSTGRES_IMAGE_ID: &str =
    "sha256:22c89fe0d0f507606260237fd55e51f6137f58b2d5bcf6152242b96d9fe8f9a4";

/// Largest backend message [`replay_startup`] will buffer.
///
/// The wire protocol sets no ceiling of its own, so this is the replayer's own
/// budget: a desynchronized or hostile endpoint must not be able to make the
/// harness allocate an arbitrary payload.
const MAX_BACKEND_MESSAGE: ByteSize = mebibytes(16);

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    pub schema_version: u32,
    pub captured_on: String,
    pub recapture_command: String,
    pub postgres: PostgresPin,
    pub pgdog: PgDogPin,
    pub drivers: Vec<DriverCapture>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgDogPin {
    pub version: String,
    pub image: String,
    pub image_id: String,
    pub revision: String,
    pub source: String,
    pub oci_version: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresPin {
    pub version: String,
    pub image: String,
    pub image_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverCapture {
    pub driver: String,
    pub version: String,
    pub lock_source: String,
    pub capture_target: String,
    #[serde(deserialize_with = "deserialize_unique_startup_parameters")]
    pub startup_parameters: BTreeMap<String, String>,
    #[serde(deserialize_with = "deserialize_unique_startup_parameters")]
    pub pgdog_backend_startup_parameters: BTreeMap<String, String>,
    pub pgdog_backend_set_batches: Vec<String>,
}

#[derive(Debug, Error)]
pub enum FixtureError {
    #[error("invalid driver-golden JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("driver-golden invariant failed: {0}")]
    Invariant(String),
}

fn deserialize_unique_startup_parameters<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct UniqueMapVisitor;

    impl<'de> Visitor<'de> for UniqueMapVisitor {
        type Value = BTreeMap<String, String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a startup parameter object with unique keys")
        }

        fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut parameters = BTreeMap::new();
            while let Some((key, value)) = access.next_entry::<String, String>()? {
                if parameters.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate startup parameter key"));
                }
            }
            Ok(parameters)
        }
    }

    deserializer.deserialize_map(UniqueMapVisitor)
}

/// Parse a driver fixture and validate its provenance and dependency pins.
///
/// # Errors
///
/// Returns [`FixtureError`] when JSON decoding or fixture validation fails.
pub fn parse_and_validate(
    text: &str,
    cargo_lock: &str,
    python_requirements: &str,
) -> Result<Fixture, FixtureError> {
    let fixture: Fixture = serde_json::from_str(text)?;
    validate_fixture(&fixture, text, cargo_lock, python_requirements)?;
    Ok(fixture)
}

/// Validate a serialized driver fixture.
///
/// # Errors
///
/// Returns [`FixtureError`] when JSON decoding or fixture validation fails.
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
    validate_dependency_pins(fixture, cargo_lock, python_requirements)?;
    validate_payload_safety(source)
}

fn validate_provenance(fixture: &Fixture) -> Result<(), FixtureError> {
    require(
        fixture.schema_version == SCHEMA_VERSION,
        "schema_version must be 2",
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
        fixture.pgdog.image == PGDOG_IMAGE,
        "PgDog digest reference drifted",
    )?;
    require(
        fixture.pgdog.image_id == PGDOG_IMAGE_ID,
        "PgDog inspected image id drifted",
    )?;
    require(
        fixture.pgdog.revision == PGDOG_REVISION,
        "PgDog OCI revision drifted",
    )?;
    require(
        fixture.pgdog.source == "https://github.com/pgdogdev/pgdog",
        "PgDog OCI source drifted",
    )?;
    require(
        fixture.pgdog.oci_version == "v0.1.6",
        "PgDog OCI version drifted",
    )?;
    require(
        fixture.postgres.version == "18",
        "PostgreSQL version must be 18",
    )?;
    require(
        fixture.postgres.image == POSTGRES_IMAGE,
        "PostgreSQL digest reference drifted",
    )?;
    require(
        fixture.postgres.image_id == POSTGRES_IMAGE_ID,
        "PostgreSQL inspected image id drifted",
    )?;

    Ok(())
}

fn validate_driver_captures(fixture: &Fixture) -> Result<(), FixtureError> {
    let expected_order = ["tokio-postgres", "sqlx", "psycopg"];
    require(
        fixture
            .drivers
            .iter()
            .map(|capture| capture.driver.as_str())
            .eq(expected_order),
        "driver captures are missing, duplicated, or reordered",
    )?;
    let backend_startup = BTreeMap::from([
        ("application_name".to_string(), "PgDog".to_string()),
        ("client_encoding".to_string(), "utf-8".to_string()),
    ]);
    for capture in &fixture.drivers {
        let (version, startup, batches): (&str, BTreeMap<String, String>, &[&str]) =
            match capture.driver.as_str() {
                "tokio-postgres" => (
                    "0.7.18",
                    BTreeMap::from([("client_encoding".to_string(), "UTF8".to_string())]),
                    &[],
                ),
                "sqlx" => (
                    "0.9.0",
                    BTreeMap::from([
                        ("DateStyle".to_string(), "ISO, MDY".to_string()),
                        ("TimeZone".to_string(), "UTC".to_string()),
                        ("client_encoding".to_string(), "UTF8".to_string()),
                        ("extra_float_digits".to_string(), "2".to_string()),
                    ]),
                    &[
                        "SET \"datestyle\" TO 'ISO, MDY'",
                        "SET \"extra_float_digits\" TO '2'",
                        "SET \"timezone\" TO 'UTC'",
                    ],
                ),
                "psycopg" => ("3.2.9", BTreeMap::new(), &[]),
                other => return Err(invariant(format!("unexpected driver {other}"))),
            };
        require(
            capture.version == version,
            "driver version does not match the pin",
        )?;
        require(
            capture.capture_target
                == "pinned PostgreSQL 18 and PgDog 0.1.6 via payload-safe TCP recorder",
            "capture target must name both observed paths",
        )?;
        require(
            capture.startup_parameters == startup,
            "direct startup capture drifted",
        )?;
        validate_startup_parameters(&capture.startup_parameters)?;
        require(
            capture.pgdog_backend_startup_parameters == backend_startup,
            "PgDog backend startup capture drifted",
        )?;
        validate_startup_parameters(&capture.pgdog_backend_startup_parameters)?;
        require(
            capture
                .pgdog_backend_set_batches
                .iter()
                .map(String::as_str)
                .eq(batches.iter().copied()),
            "PgDog backend SET capture drifted",
        )?;
        for batch in &capture.pgdog_backend_set_batches {
            validate_safe_set_batch(batch)?;
        }
    }
    Ok(())
}

fn validate_startup_parameters(parameters: &BTreeMap<String, String>) -> Result<(), FixtureError> {
    for (key, value) in parameters {
        let allowed = matches!(
            (key.as_str(), value.as_str()),
            ("DateStyle", "ISO, MDY")
                | ("TimeZone", "UTC")
                | ("application_name", "PgDog")
                | ("client_encoding", "UTF8" | "utf-8")
                | ("extra_float_digits", "2")
        );
        require(
            allowed,
            "startup parameter or value is outside the exact allowlist",
        )?;
    }
    Ok(())
}

fn validate_safe_set_batch(batch: &str) -> Result<(), FixtureError> {
    require(
        matches!(
            batch,
            "SET \"datestyle\" TO 'ISO, MDY'"
                | "SET \"extra_float_digits\" TO '2'"
                | "SET \"timezone\" TO 'UTC'"
        ),
        "backend SQL batch is outside the exact GUC/value grammar",
    )
}

fn validate_dependency_pins(
    fixture: &Fixture,
    cargo_lock: &str,
    python_requirements: &str,
) -> Result<(), FixtureError> {
    for (name, version) in [("tokio-postgres", "0.7.18"), ("sqlx", "0.9.0")] {
        let package = format!("name = \"{name}\"\nversion = \"{version}\"");
        require(
            cargo_lock.contains(&package),
            "Rust driver pin differs from Cargo.lock",
        )?;
        let checksum = package_checksum(cargo_lock, &package).ok_or_else(|| {
            invariant(format!(
                "Cargo.lock package {name} {version} has no checksum"
            ))
        })?;
        let expected_source = format!("Cargo.lock registry checksum {checksum}");
        let capture = fixture
            .drivers
            .iter()
            .find(|capture| capture.driver == name)
            .ok_or_else(|| invariant(format!("missing {name} capture")))?;
        require(
            capture.lock_source == expected_source,
            "Rust fixture checksum differs from Cargo.lock",
        )?;
    }
    require(
        fixture
            .drivers
            .iter()
            .find(|capture| capture.driver == "psycopg")
            .map(|capture| capture.lock_source.as_str())
            == psycopg_lock_source(python_requirements).as_deref(),
        "psycopg pin differs from requirements-driver-smoke.txt",
    )?;

    Ok(())
}

fn package_checksum<'a>(cargo_lock: &'a str, package: &str) -> Option<&'a str> {
    let block = cargo_lock
        .get(cargo_lock.find(package)?..)?
        .split("[[package]]")
        .next()?;
    let value = block.get(block.find("checksum = \"")? + "checksum = \"".len()..)?;
    value.get(..value.find('"')?)
}

fn psycopg_lock_source(requirements: &str) -> Option<String> {
    let mut matches = requirements
        .lines()
        .filter(|line| line.starts_with("psycopg==3.2.9 "));
    let line = matches.next()?;
    if matches.next().is_some() {
        return None;
    }
    let mut fields = line.split_whitespace();
    if fields.next()? != "psycopg==3.2.9" {
        return None;
    }
    let hash = fields.next()?.strip_prefix("--hash=sha256:")?;
    if fields.next().is_some()
        || hash.len() != 64
        || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    Some(format!("requirements-driver-smoke.txt sha256:{hash}"))
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

/// Encode a `PostgreSQL` startup packet.
///
/// # Errors
///
/// Returns an error when a field contains NUL or the packet exceeds the
/// protocol length limit.
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

/// Replay a startup packet against a live PostgreSQL-compatible endpoint.
///
/// # Errors
///
/// Returns an I/O error for connection failures, malformed backend messages,
/// or an error response from the endpoint.
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
        // The floor of four is the length field counting itself — protocol
        // layout rather than a budget — so it stays a raw integer.
        if !(4..=MAX_BACKEND_MESSAGE.bytes_u64()).contains(&u64::from(length)) {
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
    use std::{collections::BTreeMap, io};

    use assert2::check;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::{parse_and_validate, replay_startup};

    /// The ceiling written out, so the test pins the budget rather than
    /// restating whichever constant the code happens to hold.
    const CEILING_BYTES: usize = 16 * 1024 * 1024;

    /// A backend message header: kind byte then the big-endian length, which
    /// counts itself.
    fn header(kind: u8, length: u32) -> Vec<u8> {
        let mut out = vec![kind];
        out.extend_from_slice(&length.to_be_bytes());
        out
    }

    /// A complete backend message with `payload_len` zero bytes after the header.
    fn frame(kind: u8, payload_len: usize) -> Vec<u8> {
        let length = u32::try_from(payload_len + 4).expect("frame length fits an int32");
        let mut out = header(kind, length);
        out.resize(out.len() + payload_len, 0);
        out
    }

    /// Serve one connection that swallows the startup packet and replies with
    /// `frames`, yielding the loopback port to point the replayer at.
    async fn serve_frames(frames: Vec<Vec<u8>>) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback backend");
        let port = listener.local_addr().expect("listener address").port();
        drop(tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept replay client");
            let mut startup = [0_u8; 1024];
            let _ = stream.read(&mut startup).await;
            for frame in frames {
                if stream.write_all(&frame).await.is_err() {
                    return;
                }
            }
        }));
        port
    }

    async fn replay_against(port: u16) -> io::Result<()> {
        replay_startup(
            "127.0.0.1",
            port,
            "fixture_user",
            "fixture_db",
            &BTreeMap::new(),
        )
        .await
    }

    #[tokio::test]
    async fn backend_message_at_the_size_ceiling_is_buffered() {
        let port = serve_frames(vec![frame(b'S', CEILING_BYTES - 4), frame(b'Z', 1)]).await;

        check!(replay_against(port).await.is_ok());
    }

    #[tokio::test]
    async fn backend_message_one_byte_past_the_ceiling_is_refused() {
        let over = u32::try_from(CEILING_BYTES + 1).expect("ceiling plus one fits an int32");
        let port = serve_frames(vec![header(b'S', over)]).await;

        let error = replay_against(port)
            .await
            .expect_err("an oversized backend message must not be buffered");

        check!(error.kind() == io::ErrorKind::InvalidData);
        check!(error.to_string() == "invalid backend message length");
    }

    fn assert_rejected(text: &str) {
        assert!(
            parse_and_validate(
                text,
                include_str!("../../../Cargo.lock"),
                include_str!("../requirements-driver-smoke.txt")
            )
            .is_err()
        );
    }

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
        assert_rejected(&unsafe_fixture);
        let duplicate_startup = safe.replacen(
            "\"client_encoding\": \"UTF8\"",
            "\"client_encoding\": \"UTF8\", \"client_encoding\": \"UTF8\"",
            1,
        );
        assert_rejected(&duplicate_startup);
        let unknown_guc = safe.replace(
            "SET \\\"timezone\\\" TO 'UTC'",
            "SET \\\"search_path\\\" TO 'private'",
        );
        assert_rejected(&unknown_guc);
        let set_role = safe.replace("SET \\\"timezone\\\" TO 'UTC'", "SET ROLE private");
        assert_rejected(&set_role);
        let comment_trick = safe.replace(
            "SET \\\"timezone\\\" TO 'UTC'",
            "SET \\\"timezone\\\" TO 'UTC' /* private */",
        );
        assert_rejected(&comment_trick);
        let drifted = safe.replace("0.7.18", "0.7.17");
        assert_rejected(&drifted);
        let false_provenance = safe.replace(
            "a528f7d280f6d5b9cd149635c8705b0dd049754bc67d81d31fa25169a93809d3",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        assert_rejected(&false_provenance);
        let mixed_batch = safe.replace(
            "SET \\\"datestyle\\\" TO 'ISO, MDY'",
            "SET \\\"datestyle\\\" TO 'ISO, MDY'; SELECT 1",
        );
        assert_rejected(&mixed_batch);
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

    #[test]
    fn exact_capture_evidence_cannot_be_deleted_reordered_or_mutated() {
        let source = include_str!("../fixtures/driver-connect-v1.json");
        let original: serde_json::Value = serde_json::from_str(source).unwrap();
        let mut mutations = Vec::new();

        let mut deleted = original.clone();
        deleted["drivers"][1]["pgdog_backend_set_batches"]
            .as_array_mut()
            .unwrap()
            .pop();
        mutations.push(deleted);

        let mut reordered = original.clone();
        reordered["drivers"][1]["pgdog_backend_set_batches"]
            .as_array_mut()
            .unwrap()
            .swap(0, 2);
        mutations.push(reordered);

        let mut emptied = original.clone();
        emptied["drivers"][0]["startup_parameters"] = serde_json::json!({});
        mutations.push(emptied);

        let mut backend_deleted = original.clone();
        backend_deleted["drivers"][2]["pgdog_backend_startup_parameters"] = serde_json::json!({});
        mutations.push(backend_deleted);

        let mut private_batch = original.clone();
        private_batch["drivers"][1]["pgdog_backend_set_batches"][2] =
            serde_json::json!("SET \"timezone\" TO 'private-token'");
        mutations.push(private_batch);

        let mut pgdog_digest = original.clone();
        pgdog_digest["pgdog"]["image"] = serde_json::json!("ghcr.io/pgdogdev/pgdog:0.1.6");
        mutations.push(pgdog_digest);

        let mut postgres_digest = original;
        postgres_digest["postgres"]["image"] = serde_json::json!("postgres:18");
        mutations.push(postgres_digest);

        for mutation in mutations {
            let text = serde_json::to_string(&mutation).unwrap();
            assert_rejected(&text);
        }
    }
}
