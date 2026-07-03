use assert2::assert;
use std::process::Command;

fn broker_bin() -> std::path::PathBuf {
    let exe = std::env::var_os("CARGO_BIN_EXE_crabka-broker")
        .expect("cargo provides CARGO_BIN_EXE_<bin> in test env");
    std::path::PathBuf::from(exe)
}

/// Format a fresh standalone log directory via `crabka format`. KIP-853
/// requires every node be formatted (it seeds `meta.properties.json` + the
/// singleton `VotersRecord`) before `crabka-broker` will boot; an unformatted
/// dir is treated as operator error and aborts startup.
///
/// `crabka` lives in the `crabka-cli` package, so its `CARGO_BIN_EXE_*` isn't
/// exported to this crate's test env — shell out via `env!("CARGO")` like
/// `bootstrap_consumption.rs` does. The `crabka-cli` dev-dep keeps it in the
/// compile graph so this is a cache hit, not a rebuild.
fn run_crabka_format(log_dir: &std::path::Path, node_id: u32, controller_listener: &str) {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let out = Command::new(cargo)
        .args([
            "run",
            "--quiet",
            "-p",
            "crabka-cli",
            "--bin",
            "crabka",
            "--",
            "format",
            "--log-dir",
            log_dir.to_str().unwrap(),
            "--standalone",
            "--node-id",
            &node_id.to_string(),
            "--controller-listener",
            controller_listener,
        ])
        .output()
        .expect("spawn crabka format");
    assert!(
        out.status.success(),
        "crabka format failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

#[test]
fn help_mentions_cluster_id_and_advertised_listener() {
    let out = Command::new(broker_bin()).arg("--help").output().unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let help = String::from_utf8(out.stdout).unwrap();
    assert!(
        help.contains("--cluster-id"),
        "help missing --cluster-id:\n{help}"
    );
    assert!(
        help.contains("--advertised-listener"),
        "help missing --advertised-listener:\n{help}"
    );
}

#[test]
fn version_returns_zero() {
    let out = Command::new(broker_bin())
        .arg("--version")
        .output()
        .unwrap();
    assert!(out.status.success());
}

/// Boot `crabka-broker` with `--config-file` pointing at a
/// minimal TOML and assert the process binds the listener declared in
/// the file (port comes from the file, not from a CLI flag).
#[test]
fn boots_with_config_file_listener() {
    use std::io::Write as _;

    let tmp = tempfile::tempdir().expect("tempdir");
    let log_dir = tmp.path().join("data");

    // KIP-853: the broker refuses to boot an unformatted log dir, so seed
    // it first. `crabka format` creates the directory itself (it must be
    // empty or non-existent), so don't pre-create it.
    run_crabka_format(&log_dir, 1, "127.0.0.1:9093");

    // Pick an ephemeral port by binding briefly, then release it.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };

    let cfg_path = tmp.path().join("broker.toml");
    let mut f = std::fs::File::create(&cfg_path).unwrap();
    writeln!(
        f,
        r#"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "127.0.0.1:{port}"
advertised = "127.0.0.1:{port}"
protocol = "Plaintext"
"#
    )
    .unwrap();

    let mut child = Command::new(broker_bin())
        .arg(format!("--config-file={}", cfg_path.display()))
        .arg(format!("--log-dir={}", log_dir.display()))
        .arg("--broker-id=1")
        .arg("--metrics-listen-addr=none")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn crabka-broker");

    // Poll for the port to accept connections within 10 seconds.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut connected = false;
    while std::time::Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            connected = true;
            break;
        }
        // real-time wait (not a progress poll): polling a spawned child broker process to bind its TCP listener
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Tear down before assertions so a hang doesn't leave a stray process.
    let _ = child.kill();
    let _ = child.wait();

    assert!(connected, "broker never opened TCP listener on port {port}");
}

#[test]
fn errors_when_config_file_and_listen_addr_both_set() {
    let out = Command::new(broker_bin())
        .arg("--config-file=/tmp/nonexistent.toml")
        .arg("--listen-addr=127.0.0.1:9092")
        .output()
        .expect("spawn crabka-broker");

    assert!(!out.status.success(), "expected non-zero exit");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("config-file") && stderr.contains("listen-addr"),
        "expected clap mutual-exclusion error, got stderr:\n{stderr}"
    );
}
