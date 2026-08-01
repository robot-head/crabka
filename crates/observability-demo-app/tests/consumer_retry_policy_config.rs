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
        .env("CRABKA_DEMO_CONSUMER_STARTUP_DEADLINE", "37s")
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(
        String::from_utf8_lossy(&environment.stderr)
            .contains("--consumer-startup-deadline (37s) is only valid with --role consume")
    );

    let cli = demo()
        .args(["--role", "stream", "--consumer-startup-deadline", "41s"])
        .env("CRABKA_DEMO_CONSUMER_STARTUP_DEADLINE", "37s")
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(
        String::from_utf8_lossy(&cli.stderr)
            .contains("--consumer-startup-deadline (41s) is only valid with --role consume")
    );
}

#[test]
fn invalid_values_and_ordering_fail_before_external_io() {
    let zero = demo()
        .args(["--role", "consume", "--consumer-startup-deadline", "0ms"])
        .output()
        .expect("run demo");
    assert!(!zero.status.success());
    assert!(String::from_utf8_lossy(&zero.stderr).contains("invalid value '0ms'"));

    let ordering = demo()
        .args([
            "--role",
            "consume",
            "--consumer-startup-attempt-timeout",
            "2s",
            "--consumer-startup-deadline",
            "1s",
        ])
        .output()
        .expect("run demo");
    assert!(!ordering.status.success());
    assert!(
        String::from_utf8_lossy(&ordering.stderr)
            .contains("consumer startup attempt timeout must not exceed startup deadline")
    );
}
