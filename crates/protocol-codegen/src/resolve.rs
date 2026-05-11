//! Classify PascalCase type references in a `MessageSpec` as nested,
//! common, or unknown. Used by the emitter to compute the Rust type path.

use std::collections::HashMap;

use crate::ir::{FieldSpec, MessageSpec};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StructKind {
    /// Inline-defined under a field in the same message; emitted as a
    /// sibling type in the same file.
    Nested,
    /// Top-level `commonStructs` entry on the parent spec; emitted into
    /// the shared `common/` module.
    Common,
}

#[derive(Debug, Clone)]
pub struct Resolution {
    pub kind: StructKind,
    /// The path to use in generated code (owned flavor without `<'a>`).
    pub rust_path: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("unresolved type reference `{type_name}` in message `{message}`")]
    Unknown { message: String, type_name: String },
}

/// Build a resolution map for one message. Maps each PascalCase type name
/// referenced anywhere in the field tree to its kind + Rust path.
pub fn resolve_message(spec: &MessageSpec) -> Result<HashMap<String, Resolution>, ResolveError> {
    let mut map = HashMap::new();

    // Common structs first — they win if there's a name collision with a nested
    // (in practice this doesn't happen but we don't need to enforce it).
    for cs in &spec.common_structs {
        map.insert(
            cs.name.clone(),
            Resolution {
                kind: StructKind::Common,
                rust_path: format!("super::common::{}", cs.name),
            },
        );
    }

    // Walk fields to find inline-defined nested structs (those with `fields:`).
    fn walk(fields: &[FieldSpec], map: &mut HashMap<String, Resolution>) {
        for f in fields {
            if !f.fields.is_empty() {
                let type_name = base_type(&f.field_type);
                map.insert(
                    type_name.to_string(),
                    Resolution {
                        kind: StructKind::Nested,
                        rust_path: type_name.to_string(),
                    },
                );
                walk(&f.fields, map);
            }
        }
    }
    walk(&spec.fields, &mut map);

    // Walk fields again to verify every struct-typed reference resolves.
    fn check<'a>(
        fields: &'a [FieldSpec],
        map: &HashMap<String, Resolution>,
        message: &str,
    ) -> Result<(), ResolveError> {
        for f in fields {
            let base = base_type(&f.field_type);
            if is_struct_type(base) && !map.contains_key(base) {
                return Err(ResolveError::Unknown {
                    message: message.to_string(),
                    type_name: base.to_string(),
                });
            }
            check(&f.fields, map, message)?;
        }
        Ok(())
    }
    check(&spec.fields, &map, &spec.name)?;

    Ok(map)
}

fn base_type(t: &str) -> &str {
    t.strip_prefix("[]").unwrap_or(t)
}

fn is_struct_type(t: &str) -> bool {
    t.chars().next().map_or(false, char::is_uppercase)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir;
    use std::path::PathBuf;

    fn load(name: &str) -> MessageSpec {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("protocol")
            .join("schemas");
        let specs = ir::load_dir(&dir).unwrap();
        specs.into_iter().find(|s| s.name == name).unwrap()
    }

    #[test]
    fn api_versions_request_has_no_nested_structs() {
        let spec = load("ApiVersionsRequest");
        let map = resolve_message(&spec).unwrap();
        assert!(map.is_empty(), "found unexpected struct refs: {map:?}");
    }

    #[test]
    fn metadata_request_resolves_topics() {
        let spec = load("MetadataRequest");
        let map = resolve_message(&spec).unwrap();
        // MetadataRequest declares a nested MetadataRequestTopic struct.
        assert!(
            map.contains_key("MetadataRequestTopic"),
            "did not resolve MetadataRequestTopic: {map:?}"
        );
    }
}
