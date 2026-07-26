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
        .env("CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS", "37000")
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(
        String::from_utf8_lossy(&environment.stderr)
            .contains("--streams-rebalance-timeout-ms (37000 ms) is only valid with --role stream")
    );

    let cli = demo()
        .args([
            "--role",
            "produce",
            "--streams-rebalance-timeout-ms",
            "41000",
        ])
        .env("CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS", "37000")
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(
        String::from_utf8_lossy(&cli.stderr)
            .contains("--streams-rebalance-timeout-ms (41000 ms) is only valid with --role stream")
    );
}

#[test]
fn invalid_values_fail_early_and_help_lists_the_flag_once() {
    let zero = demo()
        .args(["--role", "stream"])
        .env("CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS", "0")
        .output()
        .expect("run demo");
    assert!(!zero.status.success());
    assert!(String::from_utf8_lossy(&zero.stderr).contains("invalid value '0'"));

    let overflow = demo()
        .args(["--role", "stream"])
        .env("CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS", "2147483648")
        .output()
        .expect("run demo");
    assert!(!overflow.status.success());
    assert!(String::from_utf8_lossy(&overflow.stderr).contains("streams rebalance timeout"));

    let help = demo().arg("--help").output().expect("help");
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).expect("UTF-8 help");
    assert_eq!(
        help.split_whitespace()
            .filter(|token| *token == "--streams-rebalance-timeout-ms")
            .count(),
        1
    );
}
