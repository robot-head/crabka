use std::fs;
use std::path::PathBuf;

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
    assert!(
        one.contains(sha),
        "generated/ApiVersionsRequest.owned.rs was produced against a different schemas SHA \
         ({sha}). Run tools/regenerate.sh and commit the updated files."
    );
}
