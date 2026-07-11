//! `_schemas` topic record types. Keys drive log compaction and must serialise
//! byte-exactly (field order fixed); values are parsed structurally (Confluent
//! does not pin value field order). Pinned against tests/fixtures/.

use serde::{Deserialize, Serialize};

use crate::{
    format::SchemaType,
    ids::{SchemaId, SchemaVersion},
};

/// Key for a `SCHEMA` record.
///
/// Field order is fixed so that `serde_json::to_string` produces the exact
/// bytes Confluent uses as the compaction key:
/// `{"keytype":"SCHEMA","subject":...,"version":...,"magic":1}`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaKey {
    /// Always `"SCHEMA"`.
    pub keytype: String,
    pub subject: String,
    pub version: SchemaVersion,
    /// Always `1` for `SCHEMA` keys; `0` for `NOOP`/`CONFIG`/`MODE` keys.
    pub magic: u8,
}

impl SchemaKey {
    /// Construct a `SCHEMA` key with `magic = 1`.
    #[must_use]
    pub fn new(subject: impl Into<String>, version: SchemaVersion) -> Self {
        Self {
            keytype: "SCHEMA".into(),
            subject: subject.into(),
            version,
            magic: 1,
        }
    }
}

/// Value for a `SCHEMA` record.
///
/// `schemaType` is omitted for Avro (Confluent convention). `references` is
/// omitted when empty. Field order is not pinned — parse structurally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaValue {
    pub subject: String,
    pub version: SchemaVersion,
    pub id: SchemaId,
    #[serde(
        rename = "schemaType",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub schema_type: Option<String>,
    #[serde(
        rename = "messageType",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub message_type: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<SchemaReference>,
    pub schema: String,
    #[serde(default)]
    pub deleted: bool,
}

/// A schema reference embedded in a [`SchemaValue`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: SchemaVersion,
}

/// Key for a `CONFIG` record.
///
/// `subject = None` denotes the global compatibility config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigKey {
    pub keytype: String,
    pub subject: Option<String>,
    pub magic: u8,
}

/// Value for a `CONFIG` record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigValue {
    #[serde(rename = "compatibilityLevel")]
    pub compatibility_level: String,
}

/// Key for a `MODE` record. `subject = None` is the global mode. Field order is
/// fixed to match Confluent's compaction key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModeKey {
    pub keytype: String,
    pub subject: Option<String>,
    pub magic: u8,
}

/// Value for a `MODE` record: `{"mode":"READWRITE"|"READONLY"|"IMPORT"}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModeValue {
    pub mode: String,
}

/// Key for a `DELETE_SUBJECT` record (soft subject-delete marker).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteSubjectKey {
    pub keytype: String,
    pub subject: String,
    pub magic: u8,
}

/// Value for a `DELETE_SUBJECT` record: the subject + the version up to which it
/// is soft-deleted (the latest version at delete time).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteSubjectValue {
    pub subject: String,
    pub version: SchemaVersion,
}

/// A decoded `_schemas` record.
///
/// Unknown key-types and tombstones decode to a non-panicking variant — the
/// reader replays a real registry topic that carries `NOOP` (and possibly
/// `CONFIG`/`MODE`) records.
#[derive(Debug, Clone)]
pub enum SchemaRecord {
    Schema(SchemaKey, SchemaValue),
    /// A `CONFIG` record. `None` value = a CONFIG tombstone (clears the per-subject override).
    Config(ConfigKey, Option<ConfigValue>),
    /// A `MODE` record. `None` value = a MODE tombstone (clears the override).
    Mode(ModeKey, Option<ModeValue>),
    /// A soft subject-delete marker.
    DeleteSubject(DeleteSubjectKey, DeleteSubjectValue),
    /// A `SCHEMA` key with a null value = permanent version delete.
    Tombstone(SchemaKey),
    Noop,
    Unknown,
}

impl SchemaRecord {
    /// Decode a raw `_schemas` `(key, value)` pair. `value = None` is a
    /// tombstone. Never panics; unparseable / unknown key-types become
    /// [`SchemaRecord::Unknown`].
    #[must_use]
    pub fn decode(key: &[u8], value: Option<&[u8]>) -> Self {
        let Ok(kv) = serde_json::from_slice::<serde_json::Value>(key) else {
            return Self::Unknown;
        };
        match kv.get("keytype").and_then(|v| v.as_str()) {
            Some("SCHEMA") => match serde_json::from_slice::<SchemaKey>(key) {
                Ok(k) => match value {
                    Some(v) => match serde_json::from_slice::<SchemaValue>(v) {
                        Ok(val) => Self::Schema(k, val),
                        Err(_) => Self::Unknown,
                    },
                    None => Self::Tombstone(k), // null value = permanent version delete
                },
                Err(_) => Self::Unknown,
            },
            Some("CONFIG") => match serde_json::from_slice::<ConfigKey>(key) {
                Ok(k) => match value.and_then(|v| serde_json::from_slice::<ConfigValue>(v).ok()) {
                    Some(val) => Self::Config(k, Some(val)),
                    None => Self::Config(k, None), // null value = clear config override
                },
                Err(_) => Self::Unknown,
            },
            Some("MODE") => match serde_json::from_slice::<ModeKey>(key) {
                Ok(k) => match value.and_then(|v| serde_json::from_slice::<ModeValue>(v).ok()) {
                    Some(val) => Self::Mode(k, Some(val)),
                    None => Self::Mode(k, None), // null value = clear mode override
                },
                Err(_) => Self::Unknown,
            },
            Some("DELETE_SUBJECT") => match (
                serde_json::from_slice::<DeleteSubjectKey>(key),
                value.and_then(|v| serde_json::from_slice::<DeleteSubjectValue>(v).ok()),
            ) {
                (Ok(k), Some(val)) => Self::DeleteSubject(k, val),
                // a DELETE_SUBJECT tombstone: the versions are removed by their
                // own SCHEMA tombstones, so this marker is a no-op on replay.
                _ => Self::Noop,
            },
            Some("NOOP" | "CLEAR_SUBJECTS" | "CLEAR_SUBJECT") => Self::Noop,
            _ => Self::Unknown,
        }
    }
}

/// Build the `(key, value)` bytes for a `CONFIG` record. `subject = None` is the
/// global config.
///
/// NOTE: not fixture-validated (cp-schema-registry writes no `CONFIG` record by
/// default); shape follows the Confluent docs and is read back by our own reader,
/// so it round-trips internally.
///
/// # Panics
///
/// Panics only if `serde_json` fails to serialise a plain struct — i.e. never in
/// practice.
#[must_use]
pub fn encode_config(subject: Option<&str>, level: &str) -> (Vec<u8>, Vec<u8>) {
    let key = ConfigKey {
        keytype: "CONFIG".to_string(),
        subject: subject.map(str::to_string),
        magic: 0,
    };
    let value = ConfigValue {
        compatibility_level: level.to_string(),
    };
    (
        serde_json::to_vec(&key).expect("config key serialises"),
        serde_json::to_vec(&value).expect("config value serialises"),
    )
}

#[derive(Clone, Copy)]
struct SchemaRecordParts<'a> {
    subject: &'a str,
    version: SchemaVersion,
    id: SchemaId,
    ty: SchemaType,
    schema: &'a str,
    references: &'a [SchemaReference],
    message_type: Option<&'a str>,
    deleted: bool,
}

fn schema_kv(record: SchemaRecordParts<'_>) -> (Vec<u8>, Vec<u8>) {
    let key = SchemaKey::new(record.subject, record.version);
    let value = SchemaValue {
        subject: record.subject.to_string(),
        version: record.version,
        id: record.id,
        schema_type: record.ty.wire_name().map(str::to_string),
        message_type: record.message_type.map(str::to_string),
        references: record.references.to_vec(),
        schema: record.schema.to_string(),
        deleted: record.deleted,
    };
    (
        serde_json::to_vec(&key).expect("key serialises"),
        serde_json::to_vec(&value).expect("value serialises"),
    )
}

/// Build the byte-exact key + structurally-stable value for a `SCHEMA` record.
#[must_use]
pub fn encode_schema(
    subject: &str,
    version: SchemaVersion,
    id: SchemaId,
    ty: SchemaType,
    schema: &str,
    references: &[SchemaReference],
) -> (Vec<u8>, Vec<u8>) {
    schema_kv(SchemaRecordParts {
        subject,
        version,
        id,
        ty,
        schema,
        references,
        message_type: None,
        deleted: false,
    })
}

/// Build a `SCHEMA` record carrying optional Crabka protobuf message binding
/// metadata. `message_type = None` preserves the Confluent-compatible value
/// shape.
#[must_use]
pub fn encode_schema_with_message_type(
    subject: &str,
    version: SchemaVersion,
    id: SchemaId,
    ty: SchemaType,
    schema: &str,
    references: &[SchemaReference],
    message_type: Option<&str>,
) -> (Vec<u8>, Vec<u8>) {
    schema_kv(SchemaRecordParts {
        subject,
        version,
        id,
        ty,
        schema,
        references,
        message_type,
        deleted: false,
    })
}

/// Build a soft-delete `SCHEMA` record: identical key/value to the original but
/// with `deleted = true` (cp re-emits the full value with the flag flipped).
#[must_use]
pub fn encode_schema_deleted(
    subject: &str,
    version: SchemaVersion,
    id: SchemaId,
    ty: SchemaType,
    schema: &str,
    references: &[SchemaReference],
) -> (Vec<u8>, Vec<u8>) {
    schema_kv(SchemaRecordParts {
        subject,
        version,
        id,
        ty,
        schema,
        references,
        message_type: None,
        deleted: true,
    })
}

/// Build a soft-delete `SCHEMA` record while preserving optional message
/// binding metadata.
#[must_use]
pub fn encode_schema_deleted_with_message_type(
    subject: &str,
    version: SchemaVersion,
    id: SchemaId,
    ty: SchemaType,
    schema: &str,
    references: &[SchemaReference],
    message_type: Option<&str>,
) -> (Vec<u8>, Vec<u8>) {
    schema_kv(SchemaRecordParts {
        subject,
        version,
        id,
        ty,
        schema,
        references,
        message_type,
        deleted: true,
    })
}

/// Build the `SCHEMA` key bytes for a permanent-delete tombstone (value is null,
/// produced via [`crate::kafkastore::writer::SchemaWriter::produce_tombstone`]).
#[must_use]
pub fn encode_tombstone(subject: &str, version: SchemaVersion) -> Vec<u8> {
    serde_json::to_vec(&SchemaKey::new(subject, version)).expect("schema key serialises")
}

/// Build a `MODE` record's (key, value). `subject = None` is the global mode.
#[must_use]
pub fn encode_mode(subject: Option<&str>, mode: &str) -> (Vec<u8>, Vec<u8>) {
    let key = ModeKey {
        keytype: "MODE".to_string(),
        subject: subject.map(str::to_string),
        magic: 0,
    };
    let value = ModeValue {
        mode: mode.to_string(),
    };
    (
        serde_json::to_vec(&key).expect("mode key serialises"),
        serde_json::to_vec(&value).expect("mode value serialises"),
    )
}

/// Build the `MODE` key bytes for a mode-clear tombstone (value is null).
#[must_use]
pub fn mode_key(subject: Option<&str>) -> Vec<u8> {
    let key = ModeKey {
        keytype: "MODE".to_string(),
        subject: subject.map(str::to_string),
        magic: 0,
    };
    serde_json::to_vec(&key).expect("mode key serialises")
}

/// Serialise just the CONFIG key for a subject (or global when `subject` is
/// `None`). Used to produce a tombstone that removes per-subject overrides.
pub fn config_key(subject: Option<&str>) -> Vec<u8> {
    let key = ConfigKey {
        keytype: "CONFIG".to_string(),
        subject: subject.map(str::to_string),
        magic: 0,
    };
    serde_json::to_vec(&key).expect("config key serialises")
}

/// Build a `DELETE_SUBJECT` record's (key, value).
#[must_use]
pub fn encode_delete_subject(subject: &str, version: SchemaVersion) -> (Vec<u8>, Vec<u8>) {
    let key = DeleteSubjectKey {
        keytype: "DELETE_SUBJECT".to_string(),
        subject: subject.to_string(),
        magic: 0,
    };
    let value = DeleteSubjectValue {
        subject: subject.to_string(),
        version,
    };
    (
        serde_json::to_vec(&key).expect("delete-subject key serialises"),
        serde_json::to_vec(&value).expect("delete-subject value serialises"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test helpers so literals read as `sid(7)` / `sv(2)` instead of
    /// `SchemaId(7)` / `SchemaVersion(2)` at every call site.
    fn sid(n: i32) -> SchemaId {
        SchemaId(n)
    }
    fn sv(n: i32) -> SchemaVersion {
        SchemaVersion(n)
    }

    #[test]
    fn encode_config_round_trips_via_decode() {
        let (k, v) = encode_config(None, "FULL");
        match SchemaRecord::decode(&k, Some(&v)) {
            SchemaRecord::Config(key, Some(val)) => {
                assert2::assert!(key.subject == None);
                assert2::assert!(val.compatibility_level == "FULL".to_string());
            }
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn encode_mode_round_trips() {
        let (k, v) = encode_mode(Some("s"), "READONLY");
        match SchemaRecord::decode(&k, Some(&v)) {
            SchemaRecord::Mode(key, Some(val)) => {
                assert2::assert!(key.subject == Some("s".to_string()));
                assert2::assert!(val.mode == "READONLY".to_string());
            }
            other => panic!("expected Mode, got {other:?}"),
        }
        let (gk, _gv) = encode_mode(None, "IMPORT");
        assert2::assert!(&gk == br#"{"keytype":"MODE","subject":null,"magic":0}"#);
    }

    #[test]
    fn mode_tombstone_decodes_to_clear() {
        let k = mode_key(Some("s"));
        match SchemaRecord::decode(&k, None) {
            SchemaRecord::Mode(key, None) => assert2::assert!(key.subject.as_deref() == Some("s")),
            other => panic!("expected Mode-clear, got {other:?}"),
        }
    }

    #[test]
    fn encode_delete_subject_round_trips() {
        let (k, v) = encode_delete_subject("s", sv(3));
        assert2::assert!(&k == br#"{"keytype":"DELETE_SUBJECT","subject":"s","magic":0}"#);
        match SchemaRecord::decode(&k, Some(&v)) {
            SchemaRecord::DeleteSubject(key, val) => {
                assert2::assert!(key.subject.as_str() == "s");
                assert2::assert!(val.subject.as_str() == "s");
                assert2::assert!(val.version == sv(3));
            }
            other => panic!("expected DeleteSubject, got {other:?}"),
        }
    }

    #[test]
    fn schema_null_value_decodes_to_tombstone() {
        let key = encode_tombstone("s", sv(2));
        assert2::assert!(&key == br#"{"keytype":"SCHEMA","subject":"s","version":2,"magic":1}"#);
        match SchemaRecord::decode(&key, None) {
            SchemaRecord::Tombstone(k) => {
                assert2::assert!(k.subject.as_str() == "s");
                assert2::assert!(k.version == sv(2));
            }
            other => panic!("expected Tombstone, got {other:?}"),
        }
    }

    #[test]
    fn encode_schema_deleted_sets_flag() {
        let (_k, v) = encode_schema_deleted(
            "s",
            sv(1),
            sid(7),
            SchemaType::Avro,
            "{\"type\":\"int\"}",
            &[],
        );
        let val: SchemaValue = serde_json::from_slice(&v).unwrap();
        assert2::assert!(val.deleted);
        assert2::assert!(val.id == sid(7));
    }

    #[test]
    fn encode_schema_round_trips_references() {
        let refs = vec![SchemaReference {
            name: "n".into(),
            subject: "b".into(),
            version: sv(1),
        }];
        let (k, v) = encode_schema("s", sv(1), sid(1), SchemaType::Avro, "{}", &refs);
        match SchemaRecord::decode(&k, Some(&v)) {
            SchemaRecord::Schema(_, val) => assert2::assert!(val.references == refs),
            other => panic!("expected Schema, got {other:?}"),
        }
    }

    #[test]
    fn encode_schema_round_trips_message_type_when_present() {
        let (k, v) = encode_schema_with_message_type(
            "pb-value",
            sv(1),
            sid(7),
            SchemaType::Protobuf,
            "syntax = \"proto3\"; message Order {}",
            &[],
            Some("demo.Order"),
        );
        match SchemaRecord::decode(&k, Some(&v)) {
            SchemaRecord::Schema(_, val) => {
                assert2::assert!(val.message_type.as_deref() == Some("demo.Order"));
            }
            other => panic!("expected Schema, got {other:?}"),
        }
        let raw: serde_json::Value = serde_json::from_slice(&v).unwrap();
        assert2::assert!(raw["messageType"] == "demo.Order");
    }

    /// The `_schemas` SCHEMA value `references` byte-shape must match cp 7.4.0
    /// exactly (pinned against tests/fixtures/references/records.json): the
    /// `references` array sits after `id`/`schemaType` and before `schema`, each
    /// ref is `{name,subject,version}`, and it is omitted entirely when empty.
    #[test]
    fn references_value_shape_matches_cp_capture() {
        for (_name, subject, id, schema_type, references, expected) in [
            (
                "avro_referrer",
                "av_order",
                sid(2),
                SchemaType::Avro,
                vec![SchemaReference {
                    name: "Money".into(),
                    subject: "av_money".into(),
                    version: sv(1),
                }],
                r#"{"subject":"av_order","version":1,"id":2,"references":[{"name":"Money","subject":"av_money","version":1}],"schema":"S","deleted":false}"#,
            ),
            (
                "protobuf_referrer",
                "pb_order",
                sid(4),
                SchemaType::Protobuf,
                vec![SchemaReference {
                    name: "money.proto".into(),
                    subject: "pb_money".into(),
                    version: sv(1),
                }],
                r#"{"subject":"pb_order","version":1,"id":4,"schemaType":"PROTOBUF","references":[{"name":"money.proto","subject":"pb_money","version":1}],"schema":"S","deleted":false}"#,
            ),
            (
                "empty_references_omitted",
                "av_money",
                sid(1),
                SchemaType::Avro,
                vec![],
                r#"{"subject":"av_money","version":1,"id":1,"schema":"S","deleted":false}"#,
            ),
        ] {
            let (_, value) = encode_schema(subject, sv(1), id, schema_type, "S", &references);
            assert2::assert!(String::from_utf8(value).unwrap() == expected);
        }
    }

    #[test]
    fn clear_subjects_and_delete_subject_tombstone_are_noop() {
        let (dk, _dv) = encode_delete_subject("s", sv(1));
        for (_name, key, value) in [
            (
                "clear_subjects",
                br#"{"keytype":"CLEAR_SUBJECTS","subject":"s","magic":0}"#.as_slice(),
                None,
            ),
            (
                "clear_subject",
                br#"{"keytype":"CLEAR_SUBJECT","subject":"i","magic":0}"#.as_slice(),
                Some(br#"{"subject":"i"}"#.as_slice()),
            ),
            ("delete_subject_tombstone", dk.as_slice(), None),
        ] {
            assert2::assert!(matches!(
                SchemaRecord::decode(key, value),
                SchemaRecord::Noop
            ));
        }
    }

    /// The `_schemas` keys we emit must match cp-schema-registry 7.4.0 byte-for-byte
    /// (the compaction keys); confirmed against `tests/fixtures/admin/records.json`.
    #[test]
    fn encoders_match_cp_captured_keys() {
        for (_name, actual, expected) in [
            (
                "schema",
                encode_schema("t", sv(1), sid(1), SchemaType::Avro, "{}", &[]).0,
                br#"{"keytype":"SCHEMA","subject":"t","version":1,"magic":1}"#.as_slice(),
            ),
            (
                "delete_subject",
                encode_delete_subject("d", sv(1)).0,
                br#"{"keytype":"DELETE_SUBJECT","subject":"d","magic":0}"#.as_slice(),
            ),
            (
                "mode",
                encode_mode(Some("r"), "READONLY").0,
                br#"{"keytype":"MODE","subject":"r","magic":0}"#.as_slice(),
            ),
            (
                "tombstone",
                encode_tombstone("t", sv(1)),
                br#"{"keytype":"SCHEMA","subject":"t","version":1,"magic":1}"#.as_slice(),
            ),
        ] {
            assert2::assert!(actual == expected);
        }
        // soft-delete value: cp's SCHEMA value field order with `deleted:true`.
        let (_k, v) = encode_schema_deleted("t", sv(1), sid(1), SchemaType::Avro, "{}", &[]);
        assert2::assert!(
            &v == br#"{"subject":"t","version":1,"id":1,"schema":"{}","deleted":true}"#
        );
    }

    #[test]
    fn config_key_subject_cases() {
        for (_name, subject, expected_subject) in [
            (
                "subject",
                Some("my-subject"),
                serde_json::json!("my-subject"),
            ),
            ("global", None, serde_json::Value::Null),
        ] {
            let key = config_key(subject);
            let value: serde_json::Value = serde_json::from_slice(&key).unwrap();
            assert2::assert!(
                value
                    == serde_json::json!({
                        "keytype": "CONFIG",
                        "subject": expected_subject,
                        "magic": 0,
                    })
            );
        }
    }
}
