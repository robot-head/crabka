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
        .env("CRABKA_DEMO_CONSUMER_FETCH_MIN", "3B")
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(
        String::from_utf8_lossy(&environment.stderr)
            .contains("--consumer-fetch-min (3B) is only valid with --role consume")
    );

    let cli = demo()
        .args(["--role", "stream", "--consumer-fetch-min", "5B"])
        .env("CRABKA_DEMO_CONSUMER_FETCH_MIN", "3B")
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(
        String::from_utf8_lossy(&cli.stderr)
            .contains("--consumer-fetch-min (5B) is only valid with --role consume")
    );
}

#[test]
fn invalid_values_and_ordering_fail_before_external_io() {
    let zero = demo()
        .args(["--role", "consume", "--consumer-fetch-min", "0B"])
        .output()
        .expect("run demo");
    assert!(!zero.status.success());
    assert!(String::from_utf8_lossy(&zero.stderr).contains("invalid value '0B'"));

    let ordering = demo()
        .args([
            "--role",
            "consume",
            "--consumer-fetch-min",
            "2B",
            "--consumer-fetch-max",
            "1B",
        ])
        .output()
        .expect("run demo");
    assert!(!ordering.status.success());
    assert!(
        String::from_utf8_lossy(&ordering.stderr)
            .contains("consumer fetch min must not exceed consumer fetch max")
    );
}
