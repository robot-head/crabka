use std::process::Command;

fn demo() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_observability-demo-app"));
    command.env_clear();
    command
}

#[test]
fn help_lists_each_schema_fetch_retry_flag_once() {
    let output = demo().arg("--help").output().expect("help");
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("UTF-8 help");
    for flag in [
        "--schema-fetch-retry-initial-backoff",
        "--schema-fetch-retry-max-backoff",
    ] {
        assert_eq!(
            help.split_whitespace()
                .filter(|token| *token == flag)
                .count(),
            1
        );
    }
}

#[test]
fn zero_schema_fetch_retry_bounds_are_rejected() {
    for flag in [
        "--schema-fetch-retry-initial-backoff",
        "--schema-fetch-retry-max-backoff",
    ] {
        let output = demo()
            .args(["--role", "produce", flag, "0ms"])
            .output()
            .expect("run demo");
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("invalid value '0ms'"));
    }
}

#[test]
fn environment_schema_fetch_retry_range_is_validated_before_external_io() {
    let output = demo()
        .args(["--role", "produce"])
        .env("CRABKA_DEMO_SCHEMA_FETCH_RETRY_INITIAL_BACKOFF", "91ms")
        .env("CRABKA_DEMO_SCHEMA_FETCH_RETRY_MAX_BACKOFF", "37ms")
        .output()
        .expect("run demo");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("91ms"));
    assert!(stderr.contains("37ms"));
    assert!(stderr.contains("must not exceed"));
}

#[test]
fn cli_schema_fetch_retry_value_overrides_environment() {
    let output = demo()
        .args([
            "--role",
            "produce",
            "--schema-fetch-retry-initial-backoff",
            "97ms",
        ])
        .env("CRABKA_DEMO_SCHEMA_FETCH_RETRY_INITIAL_BACKOFF", "37ms")
        .env("CRABKA_DEMO_SCHEMA_FETCH_RETRY_MAX_BACKOFF", "91ms")
        .output()
        .expect("run demo");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("97ms"));
    assert!(stderr.contains("91ms"));
    assert!(stderr.contains("must not exceed"));
}
