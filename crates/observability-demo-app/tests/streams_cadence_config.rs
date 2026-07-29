use std::process::Command;

fn demo() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_observability-demo-app"));
    command
        .env_remove("CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT")
        .env_remove("CRABKA_DEMO_STREAMS_POLL_INTERVAL")
        .env_remove("CRABKA_DEMO_STREAMS_COMMIT_INTERVAL");
    command
}

#[test]
fn environment_is_used_and_cli_wins_before_external_io() {
    let environment = demo()
        .args(["--role", "produce"])
        .env("CRABKA_DEMO_STREAMS_POLL_INTERVAL", "37ms")
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(
        String::from_utf8_lossy(&environment.stderr)
            .contains("--streams-poll-interval (37ms) is only valid with --role stream")
    );

    let cli = demo()
        .args(["--role", "produce", "--streams-poll-interval", "41ms"])
        .env("CRABKA_DEMO_STREAMS_POLL_INTERVAL", "37ms")
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(
        String::from_utf8_lossy(&cli.stderr)
            .contains("--streams-poll-interval (41ms) is only valid with --role stream")
    );

    let commit = demo()
        .args(["--role", "consume"])
        .env("CRABKA_DEMO_STREAMS_COMMIT_INTERVAL", "43ms")
        .output()
        .expect("run demo");
    assert!(!commit.status.success());
    assert!(
        String::from_utf8_lossy(&commit.stderr)
            .contains("--streams-commit-interval (43ms) is only valid with --role stream")
    );
}

#[test]
fn zero_values_are_rejected_and_help_lists_each_flag_once() {
    for (flag, environment) in [
        (
            "--streams-poll-interval",
            "CRABKA_DEMO_STREAMS_POLL_INTERVAL",
        ),
        (
            "--streams-commit-interval",
            "CRABKA_DEMO_STREAMS_COMMIT_INTERVAL",
        ),
    ] {
        let zero = demo()
            .args(["--role", "stream"])
            .env(environment, "0")
            .output()
            .expect("run demo");
        assert!(!zero.status.success());
        assert!(String::from_utf8_lossy(&zero.stderr).contains("invalid value '0'"));

        let help = demo().arg("--help").output().expect("help");
        assert!(help.status.success());
        let help = String::from_utf8(help.stdout).expect("UTF-8 help");
        assert_eq!(
            help.split_whitespace()
                .filter(|token| *token == flag)
                .count(),
            1
        );
    }
}
