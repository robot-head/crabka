use std::path::PathBuf;

use crabka_protocol_codegen::{emit, ir};

fn schemas_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("protocol")
        .join("schemas")
}

#[test]
fn every_vendored_schema_emits_clean() {
    let specs = ir::load_dir(&schemas_dir()).expect("schemas load");
    let mut failures = Vec::new();
    for spec in &specs {
        if spec.valid_versions.is_empty() {
            // Schemas with validVersions: "none" are deprecated/removed; skip.
            continue;
        }
        if let Err(e) = emit::owned_quote::emit(spec, "test") {
            failures.push(format!("owned::{}: {e}", spec.name));
        }
        if let Err(e) = emit::borrowed_quote::emit(spec, "test", None) {
            failures.push(format!("borrowed::{}: {e}", spec.name));
        }
    }
    assert2::assert!(failures.is_empty());
}
