//! Convert Kafka schema identifiers (camelCase, `PascalCase`) into idiomatic
//! Rust identifiers (`snake_case` for fields/modules, `PascalCase` for types),
//! with reserved-keyword escape and acronym handling.

/// `errorCode` -> `error_code`, `apiKeys` -> `api_keys`,
/// `ZkMigrationReady` -> `zk_migration_ready`,
/// `type` -> `type_` (reserved keyword).
#[must_use]
pub fn field_name(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        if b.is_ascii_uppercase() {
            let is_first = i == 0;
            let prev_upper = i > 0 && bytes[i - 1].is_ascii_uppercase();
            let next_lower = i + 1 < bytes.len() && bytes[i + 1].is_ascii_lowercase();
            if !is_first && (!prev_upper || next_lower) {
                out.push('_');
            }
            out.push(b.to_ascii_lowercase() as char);
        } else {
            out.push(b as char);
        }
    }
    if is_reserved_keyword(&out) {
        out.push('_');
    }
    out
}

/// `ApiVersionsRequest` -> `api_versions_request` (used for module file names).
#[must_use]
pub fn module_name(s: &str) -> String {
    field_name(s)
}

/// `ApiVersionsRequest` -> `ApiVersionsRequest` (type name, unchanged).
/// Provided for symmetry; trivial today but a single place to change if rules evolve.
#[must_use]
pub fn type_name(s: &str) -> String {
    s.to_string()
}

fn is_reserved_keyword(s: &str) -> bool {
    matches!(
        s,
        "as" | "break"
            | "const"
            | "continue"
            | "crate"
            | "else"
            | "enum"
            | "extern"
            | "false"
            | "fn"
            | "for"
            | "if"
            | "impl"
            | "in"
            | "let"
            | "loop"
            | "match"
            | "mod"
            | "move"
            | "mut"
            | "pub"
            | "ref"
            | "return"
            | "self"
            | "Self"
            | "static"
            | "struct"
            | "super"
            | "trait"
            | "true"
            | "type"
            | "unsafe"
            | "use"
            | "where"
            | "while"
            | "async"
            | "await"
            | "dyn"
            | "abstract"
            | "become"
            | "box"
            | "do"
            | "final"
            | "macro"
            | "override"
            | "priv"
            | "typeof"
            | "unsized"
            | "virtual"
            | "yield"
            | "try"
            | "union"
            | "gen"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn camel_to_snake() {
        for (input, want) in [
            ("errorCode", "error_code"),
            ("apiKeys", "api_keys"),
            ("aclEntries", "acl_entries"),
            ("zkMigrationReady", "zk_migration_ready"),
        ] {
            assert!(field_name(input) == want);
        }
    }

    #[test]
    fn pascal_to_snake() {
        assert!(field_name("ZkMigrationReady") == "zk_migration_ready");
        assert!(field_name("ApiVersionsRequest") == "api_versions_request");
    }

    #[test]
    fn acronym_runs_stay_together() {
        // KafkaClusterID -> kafka_cluster_id (acronym ID at the end)
        assert!(field_name("KafkaClusterID") == "kafka_cluster_id");
        // HTTPSEndpoint -> https_endpoint (acronym followed by Title)
        assert!(field_name("HTTPSEndpoint") == "https_endpoint");
    }

    #[test]
    fn reserved_keywords_get_underscore() {
        for (input, want) in [("type", "type_"), ("Match", "match_"), ("loop", "loop_")] {
            assert!(field_name(input) == want);
        }
    }

    #[test]
    fn module_name_uses_snake_case() {
        assert!(module_name("ApiVersionsRequest") == "api_versions_request");
        assert!(module_name("OffsetCommitResponse") == "offset_commit_response");
    }
}
