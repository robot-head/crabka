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

/// A decoded `_schemas` record.
///
/// Unknown key-types and tombstones decode to a non-panicking variant — the
/// reader replays a real registry topic that carries `NOOP` (and possibly
/// `CONFIG`/`MODE`) records.
#[derive(Debug, Clone)]
pub enum SchemaRecord {
    Schema(SchemaKey, SchemaValue),
    Config(ConfigKey, ConfigValue),
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
            Some("SCHEMA") => {
                match (
                    serde_json::from_slice::<SchemaKey>(key),
                    value.and_then(|v| serde_json::from_slice::<SchemaValue>(v).ok()),
                ) {
                    (Ok(k), Some(val)) => Self::Schema(k, val),
                    _ => Self::Unknown,
                }
            }
            Some("CONFIG") => {
                match (
                    serde_json::from_slice::<ConfigKey>(key),
                    value.and_then(|v| serde_json::from_slice::<ConfigValue>(v).ok()),
                ) {
                    (Ok(k), Some(val)) => Self::Config(k, val),
                    _ => Self::Unknown,
                }
            }
            Some("NOOP") => Self::Noop,
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

/// Build the byte-exact key and structurally-stable value for a `SCHEMA`
/// record.
///
/// The key serialises byte-identically to Confluent's compaction key. The
/// value omits `schemaType` for [`SchemaType::Avro`] and omits `references`
/// when empty.
///
/// # Panics
///
/// Panics only if `serde_json` fails to serialise a plain struct — i.e. never
/// in practice.
#[must_use]
pub fn encode_schema(
    subject: &str,
    version: i32,
    id: i32,
    ty: SchemaType,
    schema: &str,
) -> (Vec<u8>, Vec<u8>) {
    let key = SchemaKey::new(subject, version);
    let value = SchemaValue {
        subject: subject.to_string(),
        version,
        id,
        schema_type: ty.wire_name().map(str::to_string),
        references: Vec::new(),
        schema: schema.to_string(),
        deleted: false,
    };
    (
        serde_json::to_vec(&key).expect("key serialises"),
        serde_json::to_vec(&value).expect("value serialises"),
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
}
