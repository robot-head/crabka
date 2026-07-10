use std::process::Command;

#[test]
fn failover_gate_help_names_required_evidence() {
    let output = Command::new(env!("CARGO_BIN_EXE_crabka-bench-report"))
        .arg("--help")
        .output()
        .expect("run crabka-bench-report --help");

    assert2::assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("help output is utf8");

    assert2::assert!(stdout.contains("rate, drop, latency-spike, and topology evidence"));
}

#[test]
fn failover_gate_exits_nonzero_without_failover_evidence() {
    let dir = tempfile::tempdir().expect("temp dir");
    let out = dir.path().join("SUMMARY.md");

    let output = Command::new(env!("CARGO_BIN_EXE_crabka-bench-report"))
        .arg("--input-dir")
        .arg(dir.path())
        .arg("--out")
        .arg(&out)
        .arg("--failover-gate")
        .output()
        .expect("run crabka-bench-report --failover-gate");

    assert2::assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr is utf8");
    assert2::assert!(stderr.contains("failover gate: missing failover results"));
}
