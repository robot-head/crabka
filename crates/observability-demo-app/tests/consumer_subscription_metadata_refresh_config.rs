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
        .env(
            "CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL",
            "37ms",
        )
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(String::from_utf8_lossy(&environment.stderr).contains(
        "--consumer-subscription-metadata-refresh-interval (37ms) is only valid with --role consume"
    ));

    let cli = demo()
        .args([
            "--role",
            "stream",
            "--consumer-subscription-metadata-refresh-interval",
            "41ms",
        ])
        .env(
            "CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL",
            "37ms",
        )
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(String::from_utf8_lossy(&cli.stderr).contains(
        "--consumer-subscription-metadata-refresh-interval (41ms) is only valid with --role consume"
    ));
}

#[test]
fn zero_fails_early_and_help_lists_the_flag_once() {
    let zero = demo()
        .args(["--role", "consume"])
        .env(
            "CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL",
            "0ms",
        )
        .output()
        .expect("run demo");
    assert!(!zero.status.success());
    assert!(String::from_utf8_lossy(&zero.stderr).contains("invalid value '0ms'"));

    let help = demo().arg("--help").output().expect("help");
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).expect("UTF-8 help");
    assert_eq!(
        help.split_whitespace()
            .filter(|token| { *token == "--consumer-subscription-metadata-refresh-interval" })
            .count(),
        1
    );
}
