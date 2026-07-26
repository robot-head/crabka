use std::process::Command;

fn demo() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_observability-demo-app"));
    command.env_clear();
    command
}

#[test]
fn environment_is_used_and_cli_wins_before_external_io() {
    let environment = demo()
        .args(["--role", "produce"])
        .env("CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES", "37")
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(
        String::from_utf8_lossy(&environment.stderr).contains(
            "--streams-state-store-cache-max-bytes (37) is only valid with --role stream"
        )
    );

    let cli = demo()
        .args([
            "--role",
            "produce",
            "--streams-state-store-cache-max-bytes",
            "41",
        ])
        .env("CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES", "37")
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(
        String::from_utf8_lossy(&cli.stderr).contains(
            "--streams-state-store-cache-max-bytes (41) is only valid with --role stream"
        )
    );
}

#[test]
fn negative_fails_early_zero_is_parseable_and_help_lists_the_flag_once() {
    let negative = demo()
        .args(["--role", "stream"])
        .env("CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES", "-1")
        .output()
        .expect("run demo");
    assert!(!negative.status.success());
    assert!(
        String::from_utf8_lossy(&negative.stderr).contains("streams state-store cache max bytes")
    );

    let zero = demo()
        .args(["--role", "produce"])
        .env("CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES", "0")
        .output()
        .expect("run demo");
    assert!(!zero.status.success());
    assert!(
        String::from_utf8_lossy(&zero.stderr)
            .contains("--streams-state-store-cache-max-bytes (0) is only valid with --role stream")
    );

    let help = demo().arg("--help").output().expect("help");
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).expect("UTF-8 help");
    assert_eq!(
        help.split_whitespace()
            .filter(|token| *token == "--streams-state-store-cache-max-bytes")
            .count(),
        1
    );
}
