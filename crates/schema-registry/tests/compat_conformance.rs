#![cfg(not(target_os = "windows"))]

//! No-Docker conformance gate: drive the compatibility engine directly against
//! the golden cp-schema-registry verdicts in `tests/fixtures/compat/*_matrix.json`
//! (21 Avro cases, 88 Protobuf cases, 92 JSON cases, all captured from real
//! cp 7.4.0). cp is the authority; this gate fails if our engine diverges from a
//! single verdict.

use crabka_schema_registry::compat;
use crabka_schema_registry::format::SchemaType;
use crabka_schema_registry::store::StoreState;

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
fn load_matrix(file: &str) -> Vec<Case> {
    let p = format!(
        "{}/tests/fixtures/compat/{file}",
        env!("CARGO_MANIFEST_DIR")
    );
    serde_json::from_slice(&std::fs::read(&p).unwrap_or_else(|e| panic!("read {p}: {e}")))
        .expect("valid matrix")
}

/// Drive `ty` cases from `file` through the engine and assert each verdict
/// matches cp (modulo any documented `known_divergences`).
fn assert_matrix_matches_cp(
    file: &str,
    ty: SchemaType,
    known_divergences: &std::collections::HashMap<(&str, &str), bool>,
) {
    let mut mismatches = Vec::new();
    for c in load_matrix(file) {
        let mut snap = StoreState::default();
        snap.set_subject_compat("s", c.level.clone());
        snap.register("s", ty, &c.writer).expect("writer registers");
        let got = compat::check_against_version(&snap, "s", ty, &c.reader, None)
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

#[test]
fn engine_matches_cp_verdicts() {
    // (case, level) pairs where apache-avro's verdict is KNOWN to diverge from
    // cp-schema-registry, with the reason. Populate ONLY if a divergence is real.
    // Format: ("case", "LEVEL") -> our_expected_bool.
    let known_divergences: std::collections::HashMap<(&str, &str), bool> =
        std::collections::HashMap::from([]);
    assert_matrix_matches_cp("avro_matrix.json", SchemaType::Avro, &known_divergences);
}

#[test]
fn engine_matches_cp_protobuf_verdicts() {
    // (case, level) pairs where our Protobuf engine is KNOWN to diverge from
    // cp-schema-registry, with the reason documented in
    // `tests/fixtures/compat/README.md`. Empty == we match cp on all 88 cases.
    let known_divergences: std::collections::HashMap<(&str, &str), bool> =
        std::collections::HashMap::from([]);
    assert_matrix_matches_cp(
        "protobuf_matrix.json",
        SchemaType::Protobuf,
        &known_divergences,
    );
}

#[test]
fn engine_matches_cp_json_verdicts() {
    // (case, level) pairs where our JSON Schema engine is KNOWN to diverge from
    // cp-schema-registry, with the reason documented in
    // `tests/fixtures/compat/README.md`. Empty == we match cp on all 92 cases.
    let known_divergences: std::collections::HashMap<(&str, &str), bool> =
        std::collections::HashMap::from([]);
    assert_matrix_matches_cp("json_matrix.json", SchemaType::Json, &known_divergences);
}
