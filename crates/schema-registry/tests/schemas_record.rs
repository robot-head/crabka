use crabka_schema_registry::{
    format::SchemaType,
    ids::{SchemaId, SchemaVersion},
    kafkastore::record::{SchemaKey, SchemaRecord, SchemaValue, encode_schema},
};

fn fixture(name: &str) -> serde_json::Value {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_slice(&std::fs::read(&path).unwrap_or_else(|_| panic!("fixture {path}")))
        .expect("valid json")
}

/// Find the fixture record (0..=4) whose key is a SCHEMA key for `subject`.
fn schema_fixture_for(subject: &str) -> (String, String) {
    for i in 0..=4 {
        let rec = fixture(&format!("schemas_record_{i}.json"));
        let Some(key_str) = rec["key"].as_str() else {
            continue;
        };
        let key: serde_json::Value = serde_json::from_str(key_str).unwrap();
        if key["keytype"] == "SCHEMA" && key["subject"] == subject {
            return (
                key_str.to_string(),
                rec["value"].as_str().unwrap().to_string(),
            );
        }
    }
    panic!("no SCHEMA fixture for {subject}");
}

#[test]
fn schema_key_serialises_byte_exact() {
    // The KEY drives compaction; it must be byte-identical to Confluent's.
    let (key_str, _) = schema_fixture_for("av-value");
    let ours = String::from_utf8(
        serde_json::to_vec(&SchemaKey::new("av-value", SchemaVersion(1))).unwrap(),
    )
    .unwrap();
    assert_eq!(ours, key_str);
}

#[test]
fn avro_value_omits_schema_type_and_references() {
    let (_, val_str) = schema_fixture_for("av-value");
    let v: serde_json::Value = serde_json::from_str(&val_str).unwrap();
    assert_eq!(
        v.get("schemaType"),
        None,
        "AVRO value omits optional metadata"
    );
    assert_eq!(
        v.get("references"),
        None,
        "AVRO value omits optional metadata"
    );
    // Our encode produces a structurally-equal value (field order may differ).
    let (_, our_val) = encode_schema(
        "av-value",
        SchemaVersion(1),
        SchemaId(i32::try_from(v["id"].as_i64().unwrap()).unwrap()),
        SchemaType::Avro,
        v["schema"].as_str().unwrap(),
        &[],
    );
    let ours: serde_json::Value = serde_json::from_slice(&our_val).unwrap();
    assert_eq!(ours, v, "structural value match");
}

#[test]
fn protobuf_value_has_schema_type() {
    let (_, val_str) = schema_fixture_for("pb-value");
    let v: serde_json::Value = serde_json::from_str(&val_str).unwrap();
    assert_eq!(v["schemaType"], "PROTOBUF");
    let (_, our_val) = encode_schema(
        "pb-value",
        SchemaVersion(i32::try_from(v["version"].as_i64().unwrap()).unwrap()),
        SchemaId(i32::try_from(v["id"].as_i64().unwrap()).unwrap()),
        SchemaType::Protobuf,
        v["schema"].as_str().unwrap(),
        &[],
    );
    let ours: serde_json::Value = serde_json::from_slice(&our_val).unwrap();
    assert_eq!(
        ours, v,
        "structural value match (incl. schemaType:PROTOBUF)"
    );
}

#[test]
fn decode_handles_noop_and_schema_and_tombstone() {
    // NOOP fixture (record 0 or 1): value is null.
    let noop = fixture("schemas_record_0.json");
    let noop_key = noop["key"].as_str().unwrap().as_bytes();
    assert!(matches!(
        SchemaRecord::decode(noop_key, None),
        SchemaRecord::Noop
    ));
    // SCHEMA decode round-trip.
    let (k, val) = schema_fixture_for("av-value");
    match SchemaRecord::decode(k.as_bytes(), Some(val.as_bytes())) {
        SchemaRecord::Schema(key, value) => {
            assert_eq!(key, SchemaKey::new("av-value", SchemaVersion(1)));
            assert_eq!(
                value,
                SchemaValue {
                    subject: "av-value".into(),
                    version: SchemaVersion(1),
                    id: SchemaId(1),
                    schema_type: None,
                    message_type: None,
                    references: vec![],
                    schema:
                        r#"{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}"#
                            .into(),
                    deleted: false,
                }
            );
        }
        other => panic!("expected Schema, got {other:?}"),
    }
    // Unknown keytype -> Unknown (never panics).
    assert!(matches!(
        SchemaRecord::decode(br#"{"keytype":"WAT","magic":9}"#, None),
        SchemaRecord::Unknown
    ));
}

#[test]
fn schema_value_round_trips() {
    let v = SchemaValue {
        subject: "av-value".into(),
        version: SchemaVersion(1),
        id: SchemaId(1),
        schema_type: None,
        message_type: None,
        references: vec![],
        schema: "{\"type\":\"int\"}".into(),
        deleted: false,
    };
    let s = serde_json::to_string(&v).unwrap();
    assert_eq!(serde_json::from_str::<SchemaValue>(&s).unwrap(), v);
}
