//! Classify `PascalCase` type references in a `MessageSpec` as nested,
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
    /// Whether the borrowed-flavor type for this struct carries a `'a` lifetime
    /// parameter (true for common structs with string/bytes/records fields, and
    /// for nested structs whose fields recursively need a lifetime).
    pub needs_lifetime: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("unresolved type reference `{type_name}` in message `{message}`")]
    Unknown { message: String, type_name: String },
}

fn base_type(t: &str) -> &str {
    t.strip_prefix("[]").unwrap_or(t)
}

fn is_struct_type(t: &str) -> bool {
    t.chars().next().is_some_and(char::is_uppercase)
}

/// Returns true if any field (recursively) carries a borrowed lifetime in the
/// generated Rust type — i.e., `string`, `bytes`, `records`, or a nested struct
/// whose own fields recursively need a lifetime.
///
/// For common-struct references (`PascalCase` type where `f.fields.is_empty()`), the
/// caller must consult the resolution map being built; those are handled separately.
fn fields_need_lifetime(fields: &[FieldSpec]) -> bool {
    fields.iter().any(|f| {
        let base = base_type(&f.field_type);
        matches!(base, "string" | "bytes" | "records")
            || (!f.fields.is_empty() && fields_need_lifetime(&f.fields))
    })
}

/// Walk fields to find inline-defined nested structs (those with `fields:`).
fn walk(fields: &[FieldSpec], map: &mut HashMap<String, Resolution>) {
    for f in fields {
        if !f.fields.is_empty() {
            let type_name = base_type(&f.field_type);
            let nl = fields_need_lifetime(&f.fields);
            map.insert(
                type_name.to_string(),
                Resolution {
                    kind: StructKind::Nested,
                    rust_path: type_name.to_string(),
                    needs_lifetime: nl,
                },
            );
            walk(&f.fields, map);
        }
    }
}

/// Walk fields again to verify every struct-typed reference resolves.
fn check(
    fields: &[FieldSpec],
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

/// Compute whether a common struct needs a `<'a>` lifetime, considering that
/// its fields may themselves reference other common structs (by name, with empty
/// `f.fields`). We do a simple fixpoint over the set of common struct names.
fn common_struct_needs_lifetime(
    cs_fields: &[FieldSpec],
    common_names_needing_lifetime: &std::collections::HashSet<String>,
) -> bool {
    cs_fields.iter().any(|f| {
        let base = base_type(&f.field_type);
        matches!(base, "string" | "bytes" | "records")
            || (!f.fields.is_empty()
                && common_struct_needs_lifetime(&f.fields, common_names_needing_lifetime))
            || (is_struct_type(base)
                && f.fields.is_empty()
                && common_names_needing_lifetime.contains(base))
    })
}

/// Build a resolution map for one message. Maps each `PascalCase` type name
/// referenced anywhere in the field tree to its kind + Rust path.
pub fn resolve_message(spec: &MessageSpec) -> Result<HashMap<String, Resolution>, ResolveError> {
    let mut map = HashMap::new();

    // ── Pass 1: compute which common structs need '<'a>' via fixpoint ─────────
    // Start with those that directly have string/bytes/records fields, then
    // expand transitively (a common struct that references a lifetime-bearing
    // common struct also needs '<'a>').
    let mut cs_needing_lt: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut changed = true;
    while changed {
        changed = false;
        for cs in &spec.common_structs {
            if cs_needing_lt.contains(&cs.name) {
                continue; // already known
            }
            if common_struct_needs_lifetime(&cs.fields, &cs_needing_lt) {
                cs_needing_lt.insert(cs.name.clone());
                changed = true;
            }
        }
    }

    // ── Pass 2: insert common struct resolutions with correct needs_lifetime ──
    // Common structs first — they win if there's a name collision with a nested
    // (in practice this doesn't happen but we don't need to enforce it).
    //
    // `commonStructs` are MESSAGE-LOCAL in Kafka schemas: two different messages
    // may each declare a struct of the same name with different fields. We scope
    // every common struct under its owning message so identically-named structs
    // never collide. The wrapper module nests as
    // `src/{flavor}/common/<message_snake>/<struct_snake>.rs`, so the path from a
    // message file is `super::common::<message_snake>::<struct_snake>::TypeName`.
    let message_snake = crate::name_conv::module_name(&spec.name);
    for cs in &spec.common_structs {
        let snake = crate::name_conv::module_name(&cs.name);
        map.insert(
            cs.name.clone(),
            Resolution {
                kind: StructKind::Common,
                rust_path: format!("super::common::{message_snake}::{snake}::{}", cs.name),
                needs_lifetime: cs_needing_lt.contains(&cs.name),
            },
        );
    }

    walk(&spec.fields, &mut map);
    check(&spec.fields, &map, &spec.name)?;

    Ok(map)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use assert2::assert;

    use super::*;
    use crate::ir;

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
