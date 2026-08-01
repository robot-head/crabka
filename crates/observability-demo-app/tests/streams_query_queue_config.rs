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
        .env("CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY", "37")
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(String::from_utf8_lossy(&environment.stderr).contains(
        "--streams-interactive-query-queue-capacity (37) is only valid with --role stream"
    ));

    let cli = demo()
        .args([
            "--role",
            "produce",
            "--streams-interactive-query-queue-capacity",
            "41",
        ])
        .env("CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY", "37")
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(String::from_utf8_lossy(&cli.stderr).contains(
        "--streams-interactive-query-queue-capacity (41) is only valid with --role stream"
    ));
}

#[test]
fn zero_fails_early_and_help_lists_the_flag_once() {
    let zero = demo()
        .args(["--role", "stream"])
        .env("CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY", "0")
        .output()
        .expect("run demo");
    assert!(!zero.status.success());
    assert!(String::from_utf8_lossy(&zero.stderr).contains("invalid value '0'"));

    let help = demo().arg("--help").output().expect("help");
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).expect("UTF-8 help");
    assert_eq!(
        help.split_whitespace()
            .filter(|token| *token == "--streams-interactive-query-queue-capacity")
            .count(),
        1
    );
}
