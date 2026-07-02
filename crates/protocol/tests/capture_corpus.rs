//! Docker-gated, `#[ignore]` corpus generator. Boots `mirror.gcr.io/apache/kafka:4.3.0`,
//! routes real JVM-client traffic through an in-process `kafka-tap`, captures
//! one frame per `(api_key, version, direction)`, then synthesizes the
//! remainder via the JVM oracle. Run manually:
//!   `cargo test -p crabka-protocol --test capture_corpus -- --ignored --nocapture`
mod support;
use support::driver;
use support::oracle;

use std::collections::BTreeMap;
use std::process::Command;
use std::sync::{Arc, Mutex};

use crabka_kafka_tap::frame::CapturedFrame;
use crabka_kafka_tap::{Recorder, spawn};

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/generated/differential_table.rs"
));

/// Captured message bodies keyed by `(api_key, version, is_request)`.
type CaptureMap = Arc<Mutex<BTreeMap<(i16, i16, bool), Vec<u8>>>>;

const IMAGE: &str = "mirror.gcr.io/apache/kafka:4.3.0";
const CONTAINER: &str = "crabka-corpus-capture";
const BROKER_HOST_PORT: u16 = 19092;
const TAP_PORT: u16 = 19091;

fn docker_rm_f() {
    let _ = Command::new("docker")
        .args(["rm", "-f", CONTAINER])
        .output();
}

#[allow(clippy::too_many_lines)]
fn docker_run_broker() {
    docker_rm_f();
    let advertised =
        format!("PLAINTEXT://localhost:9092,EXTERNAL://host.docker.internal:{TAP_PORT}");
    let out = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            CONTAINER,
            "--add-host",
            "host.docker.internal:host-gateway",
            "-p",
            &format!("{BROKER_HOST_PORT}:{BROKER_HOST_PORT}"),
            "-e",
            "KAFKA_NODE_ID=1",
            "-e",
            "KAFKA_PROCESS_ROLES=broker,controller",
            "-e",
            &format!(
                "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:{BROKER_HOST_PORT},CONTROLLER://0.0.0.0:9093"
            ),
            "-e",
            &format!("KAFKA_ADVERTISED_LISTENERS={advertised}"),
            "-e",
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e",
            "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
            "-e",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT",
            "-e",
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "-e",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            "-e",
            "CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
            IMAGE,
        ])
        .output()
        .expect("docker run");
    assert!(
        out.status.success(),
        "docker run failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn tap_upstream() -> String {
    format!("127.0.0.1:{BROKER_HOST_PORT}")
}

fn docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .output()
        .is_ok_and(|o| o.status.success())
}

fn wait_ready() {
    for _ in 0..60 {
        let ok = Command::new("docker")
            .args([
                "exec",
                CONTAINER,
                "/opt/kafka/bin/kafka-topics.sh",
                "--list",
                "--bootstrap-server",
                "localhost:9092",
            ])
            .output()
            .is_ok_and(|o| o.status.success());
        if ok {
            return;
        }
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
    panic!("broker not ready");
}

fn corpus_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus")
}

fn hex_encode(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        use std::fmt::Write;
        let _ = write!(s, "{x:02x}");
    }
    s
}

/// Map `(api_key, direction)` to the message name via the included `CASES` table.
fn name_for(api_key: i16, is_request: bool) -> Option<&'static str> {
    CASES
        .iter()
        .find(|c| {
            c.api_key == api_key
                && matches!(
                    (c.kind, is_request),
                    (Kind::Request, true) | (Kind::Response, false)
                )
        })
        .map(|c| c.name)
}

/// Mirror `name_conv::module_name`: '_' before interior uppercase, lowercased.
fn to_snake(name: &str) -> String {
    let mut out = String::new();
    for (i, ch) in name.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if i != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

fn write_entry(
    api_key: i16,
    version: i16,
    is_request: bool,
    message_body: &[u8],
    synthetic: bool,
    desc: &str,
) {
    let dir = corpus_dir();
    let name = name_for(api_key, is_request)
        .unwrap_or_else(|| panic!("no CASES name for api_key {api_key}"));
    let dirn = if is_request { "request" } else { "response" };
    let stem = format!("{}_{dirn}_v{version}_001", to_snake(name));
    std::fs::write(dir.join(format!("{stem}.hex")), hex_encode(message_body)).unwrap();
    let toml = format!(
        "api_key = {api_key}\nversion = {version}\ndirection = \"{dirn}\"\nsource_kafka_version = \"4.3.0\"\nsynthetic = {synthetic}\ndescription = \"{desc}\"\n"
    );
    std::fs::write(dir.join(format!("{stem}.toml")), toml).unwrap();
}

#[test]
#[ignore = "requires docker + mirror.gcr.io/apache/kafka:4.3.0"]
#[allow(clippy::too_many_lines)]
fn capture_and_generate_corpus() {
    if !docker_available() {
        eprintln!("docker unavailable; skipping");
        return;
    }
    let check_only = std::env::var("CORPUS_CHECK_ONLY").is_ok();
    docker_run_broker();
    wait_ready();

    let captured: CaptureMap = Arc::new(Mutex::new(BTreeMap::new()));
    let rec: Recorder = {
        let captured = captured.clone();
        Arc::new(move |f: CapturedFrame| {
            captured
                .lock()
                .unwrap()
                .entry((f.api_key, f.version, f.is_request))
                .or_insert(f.body);
        })
    };
    let addr = spawn(("127.0.0.1", TAP_PORT), &tap_upstream(), rec).unwrap();
    eprintln!("tap on {addr} -> {}", tap_upstream());

    driver::run(CONTAINER);
    std::thread::sleep(std::time::Duration::from_secs(2));

    let pairs = captured.lock().unwrap();
    eprintln!(
        "captured {} distinct (api_key,version,dir) pairs",
        pairs.len()
    );

    // Clear any previously generated corpus so a re-run is deterministic.
    // Skipped in check-only mode, which must be fully read-only.
    if !check_only {
        for e in std::fs::read_dir(corpus_dir()).unwrap() {
            let p = e.unwrap().path();
            if matches!(p.extension().and_then(|s| s.to_str()), Some("hex" | "toml")) {
                let _ = std::fs::remove_file(p);
            }
        }
    }

    // Post-process captured frames: strip header -> message body, synthetic=false.
    let mut covered: std::collections::BTreeSet<(i16, i16, bool)> =
        std::collections::BTreeSet::new();
    for (&(api_key, version, is_request), frame) in pairs.iter() {
        let Some(name) = name_for(api_key, is_request) else {
            continue;
        };
        let body = strip_frame_header(name, version, is_request, frame);
        let re = roundtrip(name, version, &body);
        if re != body {
            eprintln!(
                "WARN captured {name} v{version} req={is_request} does not round-trip; skipping"
            );
            continue;
        }
        if check_only {
            let dirn = if is_request { "request" } else { "response" };
            let stem = format!("{}_{dirn}_v{version}_001", to_snake(name));
            let committed = std::fs::read_to_string(corpus_dir().join(format!("{stem}.hex")))
                .unwrap_or_default();
            let committed: String = committed.chars().filter(|c| !c.is_whitespace()).collect();
            assert!(
                committed == hex_encode(&body),
                "DRIFT: {name} v{version} {dirn} differs from committed corpus"
            );
            covered.insert((api_key, version, is_request));
            continue;
        }
        write_entry(
            api_key,
            version,
            is_request,
            &body,
            false,
            &format!(
                "{name} v{version} captured from mirror.gcr.io/apache/kafka:4.3.0 client traffic"
            ),
        );
        covered.insert((api_key, version, is_request));
    }
    eprintln!("wrote {} captured entries", covered.len());

    // Synthesis pass: fill every uncovered CASES Request/Response pair via oracle.
    // Skipped in check-only mode so a drift run never needs the JVM oracle and
    // stays fully read-only.
    if !check_only {
        let mut o = oracle::shared();
        let mut synth = 0usize;
        for c in CASES {
            let is_request = match c.kind {
                Kind::Request => true,
                Kind::Response => false,
                Kind::RequestHeader | Kind::ResponseHeader => continue,
            };
            if covered.contains(&(c.api_key, c.version, is_request)) {
                continue;
            }
            let jval = default_json_for(c.name, c.version);
            let body = o.encode(c.api_key, c.version, is_request, &jval);
            let re = roundtrip(c.name, c.version, &body);
            assert!(
                re == body,
                "synthetic {} v{} does not round-trip",
                c.name,
                c.version
            );
            write_entry(
                c.api_key,
                c.version,
                is_request,
                &body,
                true,
                &format!(
                    "{} v{} oracle-synthesized (not realistically client-emitted)",
                    c.name, c.version
                ),
            );
            synth += 1;
        }
        eprintln!(
            "wrote {synth} synthetic entries; total {} pairs",
            covered.len() + synth
        );
    }

    docker_rm_f();
}
