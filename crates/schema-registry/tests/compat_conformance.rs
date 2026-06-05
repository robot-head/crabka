#![cfg(not(target_os = "windows"))]

//! No-Docker conformance gate: drive the compatibility engine directly against
//! the 21 golden cp-schema-registry verdicts in
//! `tests/fixtures/compat/avro_matrix.json`.

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

fn matrix() -> Vec<Case> {
    let p = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/compat/avro_matrix.json"
    );
    serde_json::from_slice(&std::fs::read(p).expect("avro_matrix.json")).expect("valid matrix")
}

#[test]
fn engine_matches_cp_verdicts() {
    // (case, level) pairs where apache-avro's verdict is KNOWN to diverge from
    // cp-schema-registry, with the reason. Populate ONLY if a divergence is real.
    // Format: ("case", "LEVEL") -> our_expected_bool.
    let known_divergences: std::collections::HashMap<(&str, &str), bool> =
        std::collections::HashMap::from([]);

    let mut mismatches = Vec::new();
    for c in matrix() {
        let mut snap = StoreState::default();
        snap.set_subject_compat("s", c.level.clone());
        snap.register("s", SchemaType::Avro, &c.writer)
            .expect("writer registers");
        let got = compat::check_against_version(&snap, "s", SchemaType::Avro, &c.reader, None)
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
