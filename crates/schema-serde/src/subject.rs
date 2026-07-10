//! Subject naming. Confluent's default `TopicNameStrategy` maps a topic +
//! key/value role to `<topic>-key` / `<topic>-value`.

/// Whether a serde handles the record key or value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Key,
    Value,
}

/// The schema format, used to set the registry `schemaType` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaKind {
    Avro,
    Protobuf,
    Json,
}

impl SchemaKind {
    /// Registry `schemaType` wire value (`None` ⇒ omitted ⇒ AVRO default).
    #[must_use]
    pub fn wire_name(self) -> Option<&'static str> {
        match self {
            Self::Avro => None,
            Self::Protobuf => Some("PROTOBUF"),
            Self::Json => Some("JSON"),
        }
    }
}

/// Maps `(topic, role)` to a registry subject. The seam exists so
/// Record/TopicRecord strategies can be added later; only `TopicNameStrategy`
/// ships now.
pub trait SubjectStrategy: Send + Sync + 'static {
    fn subject(&self, topic: &str, role: Role) -> String;
}

/// Confluent default: `<topic>-key` / `<topic>-value`.
#[derive(Debug, Clone, Copy, Default)]
pub struct TopicNameStrategy;

impl SubjectStrategy for TopicNameStrategy {
    fn subject(&self, topic: &str, role: Role) -> String {
        match role {
            Role::Key => format!("{topic}-key"),
            Role::Value => format!("{topic}-value"),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn topic_name_strategy() {
        let s = TopicNameStrategy;
        for (name, role, expected) in [
            ("value", Role::Value, "orders-value"),
            ("key", Role::Key, "orders-key"),
        ] {
            check!(s.subject("orders", role) == expected, "case {name}");
        }
    }

    #[test]
    fn schema_kind_wire_names() {
        for (name, kind, expected) in [
            ("avro", SchemaKind::Avro, None),
            ("protobuf", SchemaKind::Protobuf, Some("PROTOBUF")),
            ("json", SchemaKind::Json, Some("JSON")),
        ] {
            check!(kind.wire_name() == expected, "case {name}");
        }
    }
}
