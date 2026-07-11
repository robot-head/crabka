use std::{path::PathBuf, process::Command};

#[test]
fn cli_uses_the_supplied_schema_and_output_paths() {
    let root = std::env::temp_dir().join(format!(
        "crabka-protocol-codegen-cli-{}",
        std::process::id()
    ));
    let generated = root.join("protocol").join("generated").join("kafka_3_6_2");
    if root.exists() {
        std::fs::remove_dir_all(&root).unwrap();
    }
    std::fs::create_dir_all(&generated).unwrap();
    let schemas = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("protocol")
        .join("schemas")
        .join("versions")
        .join("kafka_3_6_2");

    let output = Command::new(env!("CARGO_BIN_EXE_crabka-protocol-codegen"))
        .arg("--namespace")
        .arg("kafka_3_6_2")
        .arg(&schemas)
        .arg(&generated)
        .output()
        .unwrap();

    assert2::assert!(output.status.success());
    assert2::assert!(generated.join("FetchRequest.owned.rs").is_file());
    assert2::assert!(
        root.join("protocol")
            .join("src")
            .join("kafka_3_6_2")
            .join("owned")
            .join("fetch_request.rs")
            .is_file()
    );

    std::fs::remove_dir_all(root).unwrap();
}
