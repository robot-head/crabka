use std::process::Command;

fn broker_bin() -> std::path::PathBuf {
    let exe = std::env::var_os("CARGO_BIN_EXE_crabka-broker")
        .expect("cargo provides CARGO_BIN_EXE_<bin> in test env");
    std::path::PathBuf::from(exe)
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
