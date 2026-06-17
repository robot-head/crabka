use assert2::assert;

#[test]
fn rejects_missing_config() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_crabka-replicator"))
        .arg("--config")
        .arg("definitely-nonexistent-config.yaml")
        .output()
        .expect("spawn binary");
    assert!(
        !out.status.success(),
        "binary should exit non-zero on a missing config; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
}
