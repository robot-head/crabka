#[test]
fn rejects_missing_config() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_crabka-replicator"))
        .arg("--config")
        .arg("definitely-nonexistent-config.yaml")
        .output()
        .expect("spawn binary");
    assert2::assert!(!out.status.success());
}
