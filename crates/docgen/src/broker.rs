//! Generate the three broker reference pages: server config (`FileConfig` JSON
//! schema), topic configs (whitelist), and the protocol API catalog.

use crate::schema_md::render_field_table;
use std::fmt::Write;

/// Server-config reference, rendered from the `FileConfig` JSON schema.
#[must_use]
pub fn server_config_md() -> String {
    let schema = schemars::schema_for!(crabka_broker::file_config::FileConfig);
    let value = serde_json::to_value(&schema).expect("FileConfig schema serializes");
    let mut out = String::from(
        "Crabka brokers are configured from a TOML file. The fields below are \
         the top-level keys and nested tables of that file.\n\n",
    );
    out.push_str(&render_field_table(&value));
    out
}

/// Topic-config reference (the `AlterConfigs` whitelist).
#[must_use]
pub fn topic_configs_md() -> String {
    let mut out = String::from(
        "These dynamic per-topic configs are accepted by `AlterConfigs` / \
         `IncrementalAlterConfigs`. Unknown keys are rejected with `INVALID_CONFIG`.\n\n\
         | Key | Type | Default | KIP | Description |\n\
         |-----|------|---------|-----|-------------|\n",
    );
    for d in crabka_broker::topic_config_docs() {
        let default = d.default.map(|x| format!("`{x}`")).unwrap_or_default();
        let kip = d.kip.unwrap_or("—");
        let _ = writeln!(
            out,
            "| `{}` | {} | {} | {} | {} |",
            d.key, d.value_type, default, kip, d.description
        );
    }
    out
}

/// Protocol API catalog: every Kafka API key the broker advertises.
#[must_use]
pub fn protocol_apis_md() -> String {
    use crabka_protocol::api_key::ApiKey;
    let mut out = String::from(
        "The Kafka protocol APIs this broker advertises in its `ApiVersions` \
         (key 18) response, with the supported version range for each.\n\n\
         | API Key | Name | Min Version | Max Version |\n\
         |---------|------|-------------|-------------|\n",
    );
    let mut apis = crabka_broker::api_catalog::supported_apis();
    apis.sort_by_key(|a| a.api_key);
    for a in apis {
        let name: &'static str = ApiKey::from_i16(a.api_key).map_or("?", <&'static str>::from);
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} |",
            a.api_key, name, a.min_version, a.max_version
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn server_config_page_has_field_table() {
        assert!(server_config_md().contains("| Field | Type | Required | Default | Description |"));
    }
    #[test]
    fn topic_configs_page_lists_retention_ms() {
        let md = topic_configs_md();
        assert!(md.contains("`retention.ms`"));
        assert!(md.contains("| Key | Type | Default | KIP | Description |"));
    }
    #[test]
    fn protocol_apis_page_lists_named_apis() {
        let md = protocol_apis_md();
        assert!(md.contains("| API Key | Name | Min Version | Max Version |"));
        assert!(md.contains("| 18 | ApiVersions |"));
        assert!(!md.contains("| ? |"));
    }
}
