pub mod api_key_enum_quote;
pub mod borrowed;
pub mod borrowed_quote;
pub mod common;
pub mod default_json;
pub mod differential_table;
pub mod mod_rs;
pub mod owned;
pub mod owned_quote;
pub mod protocol_request;
pub mod wrappers;
pub use crate::emit::owned::EmitError;

/// The output of a single emitter run for one `MessageSpec`.
///
/// `primary` is the body of the main generated `.rs` file.
/// `commons` contains one entry per top-level `commonStruct` in the schema;
/// each entry is `(struct_name, file_body)`. For the current curated set,
/// `commons` is always empty (`DescribeGroups` uses inline nested structs, not
/// top-level commonStructs). The field is included so future schemas with real
/// commonStructs can be wired up without changing the API again.
pub struct EmittedMessage {
    pub primary: String,
    pub commons: Vec<(String, String)>,
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use assert2::check;

    use super::*;
    use crate::{ir, name_conv, validate};

    fn schemas_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("protocol")
            .join("schemas")
    }

    /// Drive the entire emit pipeline over a real schema directory, exercising
    /// every emitter (owned, borrowed, wrappers, `default_json` /
    /// `protocol_request` reached via `owned::emit`, common structs, `mod.rs`,
    /// `ApiKey` enum, and the differential dispatch table) the same way
    /// `main::run` does — but in the library's own test target so the work
    /// counts toward `--lib` coverage.
    fn emit_all(dir: &Path, namespace: Option<&str>) {
        let specs = ir::load_dir(dir).unwrap();
        validate::validate(&specs).unwrap();
        let sha = "0000000000000000000000000000000000000000";

        let active: Vec<&ir::MessageSpec> = specs
            .iter()
            .filter(|s| !s.valid_versions.is_empty())
            .collect();
        assert2::assert!(!active.is_empty());

        for s in &active {
            let owned = owned_quote::emit(s, sha).unwrap();
            assert2::assert!(
                owned.primary.contains("MIN_VERSION") || owned.primary.contains("struct")
            );
            let borrowed = borrowed_quote::emit(s, sha, namespace).unwrap();
            assert2::assert!(!borrowed.primary.is_empty());
            for (_, body) in owned.commons.iter().chain(borrowed.commons.iter()) {
                assert2::assert!(!body.is_empty());
            }
            if wrappers::should_emit_wrapper(s) {
                let w_owned = wrappers::emit(s, wrappers::Flavor::Owned, sha, namespace);
                let w_borrowed = wrappers::emit(s, wrappers::Flavor::Borrowed, sha, namespace);
                check!(w_owned.contains("mod tests"));
                check!(w_borrowed.contains("mod tests"));
                check!(w_owned.contains("case {case}, version {v}"));
                check!(w_borrowed.contains("case {case}, version {v}"));
                check!(!w_owned.contains("_case , msg"));
                check!(!w_borrowed.contains("_case , msg"));
                let has_fetch_plan = w_owned.contains("fetch_response_plan.rs");
                let expects_fetch_plan = namespace.is_none() && s.name == "FetchResponse";
                check!(has_fetch_plan == expects_fetch_plan);
                check!(!w_borrowed.contains("fetch_response_plan.rs"));
                check!(!name_conv::module_name(&s.name).is_empty());
            }
        }

        for flavor in [wrappers::Flavor::Owned, wrappers::Flavor::Borrowed] {
            for has_common in [false, true] {
                let m = mod_rs::emit(&active, flavor, sha, has_common);
                assert2::assert!(m.contains("pub mod"));
            }
        }

        if namespace.is_none() {
            assert2::assert!(api_key_enum_quote::emit(&specs, sha).contains("ApiKey"));
            assert2::assert!(!differential_table::emit(&specs, sha).is_empty());
        }
        assert2::assert!(common::banner(sha).contains(sha));
    }

    #[test]
    fn emit_all_top_level_schemas() {
        emit_all(&schemas_dir(), None);
    }

    #[test]
    fn emit_all_namespaced_schemas() {
        let dir = schemas_dir().join("versions").join("kafka_3_6_2");
        emit_all(&dir, Some("kafka_3_6_2"));
    }
}
