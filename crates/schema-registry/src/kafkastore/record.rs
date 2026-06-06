//! `_schemas` topic record types. Keys drive log compaction and must serialise
//! byte-exactly (field order fixed); values are parsed structurally (Confluent
//! does not pin value field order). Pinned against tests/fixtures/.

use serde::{Deserialize, Serialize};

use crate::format::SchemaType;

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
    pub version: i32,
    /// Always `1` for `SCHEMA` keys; `0` for `NOOP`/`CONFIG`/`MODE` keys.
    pub magic: u8,
}

impl SchemaKey {
    /// Construct a `SCHEMA` key with `magic = 1`.
    #[must_use]
    pub fn new(subject: impl Into<String>, version: i32) -> Self {
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
    pub version: i32,
    pub id: i32,
    #[serde(
        rename = "schemaType",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub schema_type: Option<String>,
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
    pub version: i32,
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
/// fixed to match Confluent's compaction key (seeded; confirmed in Task 4).
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
    pub version: i32,
}

/// A decoded `_schemas` record.
///
/// Unknown key-types and tombstones decode to a non-panicking variant — the
/// reader replays a real registry topic that carries `NOOP` (and possibly
/// `CONFIG`/`MODE`) records.
#[derive(Debug, Clone)]
pub enum SchemaRecord {
    Schema(SchemaKey, SchemaValue),
    Config(ConfigKey, ConfigValue),
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
            Some("CONFIG") => match (
                serde_json::from_slice::<ConfigKey>(key),
                value.and_then(|v| serde_json::from_slice::<ConfigValue>(v).ok()),
            ) {
                (Ok(k), Some(val)) => Self::Config(k, val),
                _ => Self::Unknown,
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
            Some("NOOP" | "CLEAR_SUBJECTS") => Self::Noop,
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

fn schema_kv(
    subject: &str,
    version: i32,
    id: i32,
    ty: SchemaType,
    schema: &str,
    deleted: bool,
) -> (Vec<u8>, Vec<u8>) {
    let key = SchemaKey::new(subject, version);
    let value = SchemaValue {
        subject: subject.to_string(),
        version,
        id,
        schema_type: ty.wire_name().map(str::to_string),
        references: Vec::new(),
        schema: schema.to_string(),
        deleted,
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
    version: i32,
    id: i32,
    ty: SchemaType,
    schema: &str,
) -> (Vec<u8>, Vec<u8>) {
    schema_kv(subject, version, id, ty, schema, false)
}

/// Build a soft-delete `SCHEMA` record: identical key/value to the original but
/// with `deleted = true` (cp re-emits the full value with the flag flipped).
#[must_use]
pub fn encode_schema_deleted(
    subject: &str,
    version: i32,
    id: i32,
    ty: SchemaType,
    schema: &str,
) -> (Vec<u8>, Vec<u8>) {
    schema_kv(subject, version, id, ty, schema, true)
}

/// Build the `SCHEMA` key bytes for a permanent-delete tombstone (value is null,
/// produced via [`crate::kafkastore::writer::SchemaWriter::produce_tombstone`]).
#[must_use]
pub fn encode_tombstone(subject: &str, version: i32) -> Vec<u8> {
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

/// Build a `DELETE_SUBJECT` record's (key, value).
#[must_use]
pub fn encode_delete_subject(subject: &str, version: i32) -> (Vec<u8>, Vec<u8>) {
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

    #[test]
    fn encode_config_round_trips_via_decode() {
        let (k, v) = encode_config(None, "FULL");
        match SchemaRecord::decode(&k, Some(&v)) {
            SchemaRecord::Config(key, val) => {
                assert!(key.subject.is_none());
                assert_eq!(val.compatibility_level, "FULL");
            }
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn encode_mode_round_trips() {
        let (k, v) = encode_mode(Some("s"), "READONLY");
        match SchemaRecord::decode(&k, Some(&v)) {
            SchemaRecord::Mode(key, Some(val)) => {
                assert_eq!(key.subject.as_deref(), Some("s"));
                assert_eq!(val.mode, "READONLY");
            }
            other => panic!("expected Mode, got {other:?}"),
        }
        let (gk, _gv) = encode_mode(None, "IMPORT");
        assert_eq!(&gk, br#"{"keytype":"MODE","subject":null,"magic":0}"#);
    }

    #[test]
    fn mode_tombstone_decodes_to_clear() {
        let k = mode_key(Some("s"));
        match SchemaRecord::decode(&k, None) {
            SchemaRecord::Mode(key, None) => assert_eq!(key.subject.as_deref(), Some("s")),
            other => panic!("expected Mode-clear, got {other:?}"),
        }
    }

    #[test]
    fn encode_delete_subject_round_trips() {
        let (k, v) = encode_delete_subject("s", 3);
        assert_eq!(
            &k,
            br#"{"keytype":"DELETE_SUBJECT","subject":"s","magic":0}"#
        );
        match SchemaRecord::decode(&k, Some(&v)) {
            SchemaRecord::DeleteSubject(key, val) => {
                assert_eq!(key.subject, "s");
                assert_eq!((val.subject.as_str(), val.version), ("s", 3));
            }
            other => panic!("expected DeleteSubject, got {other:?}"),
        }
    }

    #[test]
    fn schema_null_value_decodes_to_tombstone() {
        let key = encode_tombstone("s", 2);
        assert_eq!(
            &key,
            br#"{"keytype":"SCHEMA","subject":"s","version":2,"magic":1}"#
        );
        match SchemaRecord::decode(&key, None) {
            SchemaRecord::Tombstone(k) => assert_eq!((k.subject.as_str(), k.version), ("s", 2)),
            other => panic!("expected Tombstone, got {other:?}"),
        }
    }

    #[test]
    fn encode_schema_deleted_sets_flag() {
        let (_k, v) = encode_schema_deleted("s", 1, 7, SchemaType::Avro, "{\"type\":\"int\"}");
        let val: SchemaValue = serde_json::from_slice(&v).unwrap();
        assert!(val.deleted);
        assert_eq!(val.id, 7);
    }

    #[test]
    fn clear_subjects_and_delete_subject_tombstone_are_noop() {
        let cs = br#"{"keytype":"CLEAR_SUBJECTS","subject":"s","magic":0}"#;
        assert!(matches!(SchemaRecord::decode(cs, None), SchemaRecord::Noop));
        let (dk, _dv) = encode_delete_subject("s", 1);
        assert!(matches!(
            SchemaRecord::decode(&dk, None),
            SchemaRecord::Noop
        ));
    }
}
