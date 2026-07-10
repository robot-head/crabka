use std::{fs, path::PathBuf};

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    println!("cargo:rerun-if-changed=schemas/VERSION");
    println!("cargo:rerun-if-changed=generated");

    let schemas_version =
        fs::read_to_string(root.join("schemas/VERSION")).expect("schemas/VERSION must exist");
    let sha = schemas_version
        .lines()
        .find_map(|l| l.strip_prefix("sha: "))
        .expect("schemas/VERSION must contain a `sha:` line");

    let one = fs::read_to_string(root.join("generated/ApiVersionsRequest.owned.rs"))
        .expect("generated/ApiVersionsRequest.owned.rs must exist; run tools/regenerate.sh");
    assert2::assert!(one.contains(sha));

    let ns_version = fs::read_to_string(root.join("schemas/versions/kafka_3_6_2/VERSION"))
        .expect("schemas/versions/kafka_3_6_2/VERSION must exist");
    let ns_sha = ns_version
        .lines()
        .find_map(|l| l.strip_prefix("sha: "))
        .expect("schemas/versions/kafka_3_6_2/VERSION must contain a `sha:` line");
    let ns_one = fs::read_to_string(root.join("generated/kafka_3_6_2/ProduceRequest.owned.rs"))
        .expect(
            "generated/kafka_3_6_2/ProduceRequest.owned.rs must exist; run tools/regenerate.sh",
        );
    assert2::assert!(ns_one.contains(ns_sha));
    println!("cargo:rerun-if-changed=schemas/versions/kafka_3_6_2/VERSION");
}
