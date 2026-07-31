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
        .env("CRABKA_DEMO_CONSUMER_SESSION_TIMEOUT", "47s")
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(
        String::from_utf8_lossy(&environment.stderr)
            .contains("--consumer-session-timeout (47s) is only valid with --role consume")
    );

    let cli = demo()
        .args(["--role", "stream", "--consumer-session-timeout", "48s"])
        .env("CRABKA_DEMO_CONSUMER_SESSION_TIMEOUT", "47s")
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(
        String::from_utf8_lossy(&cli.stderr)
            .contains("--consumer-session-timeout (48s) is only valid with --role consume")
    );
}

#[test]
fn zero_fails_before_external_io() {
    let zero = demo()
        .args(["--role", "consume", "--consumer-session-timeout", "0ms"])
        .output()
        .expect("run demo");
    assert!(!zero.status.success());
    assert!(String::from_utf8_lossy(&zero.stderr).contains("invalid value '0ms'"));
}
