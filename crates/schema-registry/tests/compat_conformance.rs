//! No-Docker conformance gate: drive the compatibility engine directly against
//! the golden cp-schema-registry verdicts in `tests/fixtures/compat/*_matrix.json`
//! (21 Avro cases, 88 Protobuf cases, 92 JSON cases, all captured from real
//! cp 7.4.0). cp is the authority; this gate fails if our engine diverges from a
//! single verdict.

use std::path::Path;

use crabka_schema_registry::{compat, format::SchemaType, store::StoreState};

#[derive(serde::Deserialize)]
#[allow(clippy::struct_field_names)]
struct Case {
    case: String,
    level: String,
    writer: String,
    reader: String,
    is_compatible: bool,
}

/// Load a golden matrix fixture (`avro_matrix.json` / `protobuf_matrix.json`).
fn load_matrix(path: &Path) -> Vec<Case> {
    serde_json::from_slice(
        &std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display())),
    )
    .expect("valid matrix")
}

/// Drive `ty` cases from `file` through the engine and assert each verdict
/// matches cp (modulo any documented `known_divergences`).
fn assert_matrix_matches_cp(
    path: &Path,
    ty: SchemaType,
    known_divergences: &std::collections::HashMap<(&str, &str), bool>,
) {
    let mut mismatches = Vec::new();
    for c in load_matrix(path) {
        let mut snap = StoreState::default();
        snap.set_subject_compat("s", c.level.clone());
        snap.register("s", ty, &c.writer, &[], None)
            .expect("writer registers");
        let got = compat::check_against_version(&snap, "s", ty, &c.reader, &[], None)
            .expect("verdict")
            .is_compatible;
        let expected = *known_divergences
            .get(&(c.case.as_str(), c.level.as_str()))
            .unwrap_or(&c.is_compatible);
        if got != expected {
            mismatches.push(format!(
                "{}/{}: ours={got} cp={} (expected {expected})",
                c.case, c.level, c.is_compatible
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "engine diverges from cp on:\n{}",
        mismatches.join("\n")
    );
}

#[allow(clippy::unnecessary_wraps)]
fn engine_matches_cp_verdicts(path: &Path) -> datatest_stable::Result<()> {
    let ty = match path.file_name().and_then(|name| name.to_str()) {
        Some("avro_matrix.json") => SchemaType::Avro,
        Some("protobuf_matrix.json") => SchemaType::Protobuf,
        Some("json_matrix.json") => SchemaType::Json,
        other => panic!("unexpected compatibility matrix {other:?}"),
    };
    let known_divergences: std::collections::HashMap<(&str, &str), bool> =
        std::collections::HashMap::from([]);
    assert_matrix_matches_cp(path, ty, &known_divergences);
    Ok(())
}

datatest_stable::harness! {
    { test = engine_matches_cp_verdicts, root = "tests/fixtures/compat", pattern = r".*_matrix\.json$" },
}
