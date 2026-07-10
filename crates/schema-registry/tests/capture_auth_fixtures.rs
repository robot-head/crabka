// Attributes FIRST (above the `//!` module docs): on windows `cfg(false)` would
// otherwise strip the trailing `#![allow(clippy::pedantic)]` while the crate-root
// `//!` docs are still linted ⇒ `doc_markdown` fires windows-only. Attributes-first
// prevents that; code-like identifiers in the docs are also backticked.

#![allow(clippy::pedantic)]

//! Golden HTTP-Basic-auth capture harness for Crabka Schema Registry slice 6.
//!
//! Boots a real `mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0` container with
//! `authentication.method=BASIC` against an in-process Crabka broker (same
//! networking as `capture_admin_fixtures.rs` / `capture_references_fixtures.rs`:
//! the broker binds `0.0.0.0:9092` and advertises `host.docker.internal:9092`,
//! while the host connects directly on `127.0.0.1:9092`), then drives the
//! Basic-auth `401` lifecycle against cp's REST API. cp's Jetty
//! `PropertyFileLoginModule` is wired via a JAAS file + a property password
//! file written to a host tempdir and mounted at `/etc/sr`.
//!
//! One fixture is produced:
//!
//!   * `tests/fixtures/auth/basic.json` — for each of three credential cases
//!     (no `Authorization`, `alice:wrongpw`, `alice:pw`) the HTTP status, the
//!     exact `WWW-Authenticate` response header (the realm cp emits!), and the
//!     parsed-or-raw body that cp returns for `GET /subjects`. This is the cp
//!     oracle for the `401` + `WWW-Authenticate` calibration of slice 6.
//!
//! ```text
//! cargo test -p crabka-schema-registry --test capture_auth_fixtures -- --ignored --nocapture
//! ```
//!
//! Re-running this test regenerates the fixture file verbatim.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use crabka_broker::{Broker, BrokerConfig};

/// The broker binds host port 9092 and cp-schema-registry reaches it via
/// `host.docker.internal:9092` (container network) while the host connects
/// directly on `127.0.0.1:9092`.
const LISTEN: &str = "0.0.0.0:9092";
const CONTROLLER_LISTEN: &str = "0.0.0.0:9093";
const ADVERTISED: &str = "host.docker.internal:9092";

const SR_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0";

/// The JAAS entry name. cp's `authentication.realm` must equal this so the
/// `BasicAuthenticator` resolves the `PropertyFileLoginModule` entry.
const JAAS_REALM: &str = "SchemaRegistry-Props";

/// Captured credentials (mirror `password.properties`: `alice: pw,admin`).
const AUTH_USER: &str = "alice";
const AUTH_PASS: &str = "pw";

// ── fixture paths ─────────────────────────────────────────────────────────────

fn auth_fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("auth")
}

fn write_auth_fixture(name: &str, body: &str) {
    let dir = auth_fixtures_dir();
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("create dir {}: {e}", dir.display()));
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap_or_else(|e| panic!("write fixture {}: {e}", path.display()));
    eprintln!("CAPTURE wrote {} ({} bytes)", path.display(), body.len());
}

// ── broker ────────────────────────────────────────────────────────────────────

async fn start_host_broker() -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=info,info")),
        )
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().expect("tempdir");
    let listen_addr: SocketAddr = LISTEN.parse().expect("static addr");
    let controller_addr: SocketAddr = CONTROLLER_LISTEN.parse().expect("static addr");
    let config = BrokerConfig {
        broker_id: 1,
        listen_addr,
        advertised_listener: ADVERTISED.into(),
        log_dir: dir.path().to_path_buf(),
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(crabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval_ms: 3_000,
        heartbeat_timeout_ms: 9_000,
        replica_lag_time_max_ms: 30_000,
        controller_election_timeout: Duration::from_secs(5),
        controller_heartbeat_interval: Duration::from_millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start broker");
    eprintln!("CAPTURE broker started listen={LISTEN} advertised={ADVERTISED}");
    (handle, dir)
}

// ── JAAS / password file plumbing ───────────────────────────────────────────────

/// Write `jaas.conf` + `password.properties` into a fresh tempdir, returning the
/// dir (kept alive by the caller for the container's lifetime). The dir is
/// bind-mounted at `/etc/sr` and referenced by `SCHEMA_REGISTRY_OPTS`.
///
/// `jaas.conf` declares one `SchemaRegistry-Props` entry backed by Jetty's
/// `org.eclipse.jetty.jaas.spi.PropertyFileLoginModule` (the LoginModule cp
/// 7.4.0 ships for `authentication.method=BASIC`). Its `file=` points at the
/// mounted `password.properties`, whose Jetty format is
/// `username: password[,role...]` — here `alice: pw,admin` (role `admin` matches
/// `SCHEMA_REGISTRY_AUTHENTICATION_ROLES`).
fn write_auth_config_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("auth config tempdir");
    let jaas = format!(
        "{JAAS_REALM} {{\n    \
         org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required\n    \
         file=\"/etc/sr/password.properties\"\n    \
         debug=\"true\";\n}};\n"
    );
    std::fs::write(dir.path().join("jaas.conf"), jaas).expect("write jaas.conf");
    // Jetty property format: `username: password[,role...]`.
    std::fs::write(
        dir.path().join("password.properties"),
        format!("{AUTH_USER}: {AUTH_PASS},admin\n"),
    )
    .expect("write password.properties");
    // World-readable so the in-container `appuser` can read the mount.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        for f in ["jaas.conf", "password.properties"] {
            let p = dir.path().join(f);
            let mut perm = std::fs::metadata(&p).expect("stat").permissions();
            perm.set_mode(0o644);
            std::fs::set_permissions(&p, perm).expect("chmod");
        }
    }
    eprintln!("CAPTURE auth config dir {}", dir.path().display());
    dir
}

// ── docker helpers ────────────────────────────────────────────────────────────

fn docker_pull(image: &str) {
    eprintln!("CAPTURE docker pull {image} (large; may take minutes)...");
    // cp images can be flaky to pull ("bytes remaining on stream"); retry a few times.
    let mut last = String::new();
    for attempt in 1..=3 {
        let out = Command::new("docker")
            .args(["pull", image])
            .output()
            .expect("spawn docker pull");
        if out.status.success() {
            return;
        }
        last = format!(
            "stdout={}, stderr={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
        eprintln!("CAPTURE docker pull attempt {attempt} failed; retrying: {last}");
        std::thread::sleep(Duration::from_secs(3));
    }
    panic!("docker pull {image} failed after 3 attempts: {last}");
}

/// Run cp with `authentication.method=BASIC`, mounting the JAAS/password tempdir
/// at `/etc/sr` and pointing the JVM at the JAAS file via `SCHEMA_REGISTRY_OPTS`.
fn docker_run_schema_registry(auth_dir: &Path) -> String {
    let mount = format!("{}:/etc/sr", auth_dir.display());
    let out = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "-p",
            "0:8081",
            "-v",
            &mount,
            "-e",
            "SCHEMA_REGISTRY_HOST_NAME=localhost",
            "-e",
            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://host.docker.internal:9092",
            "-e",
            "SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081",
            // ── BASIC auth ──
            "-e",
            "SCHEMA_REGISTRY_AUTHENTICATION_METHOD=BASIC",
            "-e",
            "SCHEMA_REGISTRY_AUTHENTICATION_ROLES=admin",
            // The JAAS entry NAME the BasicAuthenticator resolves.
            "-e",
            "SCHEMA_REGISTRY_AUTHENTICATION_REALM=SchemaRegistry-Props",
            "-e",
            "SCHEMA_REGISTRY_OPTS=-Djava.security.auth.login.config=/etc/sr/jaas.conf",
            SR_IMAGE,
        ])
        .output()
        .expect("spawn docker run schema-registry");
    assert2::assert!(out.status.success());
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert2::assert!(!id.is_empty());
    eprintln!("CAPTURE schema-registry container id={id}");
    id
}

fn docker_mapped_port(id: &str) -> u16 {
    let out = Command::new("docker")
        .args(["port", id, "8081"])
        .output()
        .expect("spawn docker port");
    assert2::assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    let port = text
        .lines()
        .filter_map(|l| l.rsplit(':').next())
        .find_map(|p| p.trim().parse::<u16>().ok())
        .unwrap_or_else(|| panic!("could not parse mapped 8081 port from: {text:?}"));
    eprintln!("CAPTURE schema-registry mapped 8081 -> host {port}");
    port
}

fn docker_logs(id: &str) -> String {
    let out = Command::new("docker")
        .args(["logs", id])
        .output()
        .expect("spawn docker logs");
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    )
}

fn docker_rm_f(id: &str) {
    let _ = Command::new("docker").args(["rm", "-f", id]).output();
    eprintln!("CAPTURE removed container {id}");
}

struct ContainerGuard {
    id: String,
}
impl Drop for ContainerGuard {
    fn drop(&mut self) {
        docker_rm_f(&self.id);
    }
}

// ── REST helpers ──────────────────────────────────────────────────────────────

/// Wait until cp answers `GET /subjects` with `200`. Because BASIC auth is on,
/// the readiness probe MUST present a valid credential (`alice:pw`); a
/// credential-less probe would loop on `401` forever.
async fn wait_for_registry(http: &reqwest::Client, base: &str, container_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    let url = format!("{base}/subjects");
    let mut last: Option<String> = None;
    while Instant::now() < deadline {
        match http
            .get(&url)
            .basic_auth(AUTH_USER, Some(AUTH_PASS))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                eprintln!("CAPTURE schema-registry READY ({})", resp.status());
                return;
            }
            Ok(resp) => last = Some(format!("status {}", resp.status())),
            Err(e) => last = Some(format!("err {e}")),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let logs = docker_logs(container_id);
    panic!(
        "schema-registry never became ready within 120s (last: {last:?}).\ncontainer logs:\n{logs}"
    );
}

/// Issue `GET /subjects` with the given optional `(user, pass)` credential and
/// capture the verdict: HTTP status, the `WWW-Authenticate` response header (the
/// realm cp emits, if any), and the parsed-or-raw body — one entry for
/// `basic.json`.
async fn capture_case(
    http: &reqwest::Client,
    base: &str,
    label: &str,
    creds: Option<(&str, &str)>,
) -> serde_json::Value {
    let url = format!("{base}/subjects");
    let rb = http.get(&url);
    let rb = match creds {
        Some((u, p)) => rb.basic_auth(u, Some(p)),
        None => rb,
    };
    let resp = rb.send().await.unwrap_or_else(|e| panic!("GET {url}: {e}"));
    let status = resp.status().as_u16();
    let www = resp
        .headers()
        .get(reqwest::header::WWW_AUTHENTICATE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let text = resp.text().await.unwrap_or_default();
    let parsed =
        serde_json::from_str::<serde_json::Value>(&text).unwrap_or(serde_json::Value::String(text));
    eprintln!("CAPTURE {label}: GET /subjects -> {status} www={www:?} body={parsed}");
    serde_json::json!({
        "case": label,
        "credentials": creds.map(|(u, _)| u),
        "status": status,
        "www_authenticate": www,
        "body": parsed,
    })
}

// ── the test ──────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; captures cp-schema-registry Basic-auth 401 shape"]
async fn capture_basic_auth() {
    docker_pull(SR_IMAGE);

    let (broker, _dir) = start_host_broker().await;

    // JAAS + password file mounted at /etc/sr; kept alive for the container.
    let auth_dir = write_auth_config_dir();

    let container_id = docker_run_schema_registry(auth_dir.path());
    let _guard = ContainerGuard {
        id: container_id.clone(),
    };

    let port = docker_mapped_port(&container_id);
    let base = format!("http://127.0.0.1:{port}");

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build reqwest client");

    // Readiness probe authenticates (BASIC is on).
    wait_for_registry(&http, &base, &container_id).await;

    // Three credential cases against `GET /subjects`:
    //   (a) no Authorization      → expect 401 + WWW-Authenticate (the realm!)
    //   (b) alice:wrongpw         → expect 401
    //   (c) alice:pw              → expect 200
    let mut cases: Vec<serde_json::Value> = Vec::new();
    cases.push(capture_case(&http, &base, "no_credentials", None).await);
    cases.push(capture_case(&http, &base, "wrong_password", Some((AUTH_USER, "wrongpw"))).await);
    cases.push(
        capture_case(
            &http,
            &base,
            "good_credentials",
            Some((AUTH_USER, AUTH_PASS)),
        )
        .await,
    );

    write_auth_fixture("basic.json", &serde_json::to_string_pretty(&cases).unwrap());

    broker.shutdown().await;
    eprintln!("CAPTURE done — basic.json has {} cases", cases.len());
}
