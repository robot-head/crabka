use serde_json::{Map, Value};

/// Incoming connector configuration as a JSON object.
pub type RawConfig = Map<String, Value>;

/// ConfigDef-style connector configuration schema.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ConfigDef {
    _private: (),
}

/// One connector configuration field definition.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConfigKey {
    _private: (),
}

/// Supported logical connector configuration kinds.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ConfigKind {
    String,
    Bool,
    Integer,
    Float,
    DurationMillis,
    StringList,
    Json,
    Secret,
}
