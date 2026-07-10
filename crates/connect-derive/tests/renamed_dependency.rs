use std::{fs, process::Command};

#[test]
fn derive_works_when_crabka_connect_dependency_is_renamed() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(std::path::Path::parent)
        .expect("derive crate lives under crates/connect-derive");
    let crate_dir = workspace_root.join("target/tests/renamed-connect-derive");
    if crate_dir.exists() {
        fs::remove_dir_all(&crate_dir).expect("remove stale renamed dependency test crate");
    }
    fs::create_dir_all(crate_dir.join("src")).expect("create renamed dependency test crate");

    fs::write(
        crate_dir.join("Cargo.toml"),
        format!(
            r#"[package]
name = "renamed-connect-derive"
version = "0.0.0"
edition = "2024"

[workspace]

[dependencies]
renamed-connect = {{ package = "crabka-connect", path = "{}" }}
"#,
            // Backslashes in Windows paths are parsed as escape sequences in
            // basic TOML strings; forward slashes work on every platform.
            workspace_root
                .join("crates/connect")
                .display()
                .to_string()
                .replace('\\', "/")
        ),
    )
    .expect("write renamed dependency manifest");

    fs::write(
        crate_dir.join("src/main.rs"),
        r"use renamed_connect::{ConnectorConfig, SecretString};

#[derive(ConnectorConfig)]
struct RenamedConfig {
    database_url: String,
    #[config(secret)]
    password: SecretString,
}

fn main() {
    let _ = RenamedConfig::config_def();
}
",
    )
    .expect("write renamed dependency source");

    let output = Command::new("cargo")
        .arg("check")
        .arg("--quiet")
        .current_dir(&crate_dir)
        .output()
        .expect("run cargo check for renamed dependency test crate");

    assert2::assert!(output.status.success());
}
