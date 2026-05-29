//! Render a JSON-Schema-shaped value (`OpenAPI` v3 / schemars output) to a
//! markdown field table. Shared by the CRD and broker-config generators.

use serde_json::Value;
use std::fmt::Write;

/// Render the `properties` of an object schema as a markdown table with one
/// row per (possibly nested) field. Columns: Field, Type, Required, Default,
/// Description. Nested object properties recurse with a dotted path.
#[must_use]
pub fn render_field_table(schema: &Value) -> String {
    let mut rows = String::new();
    collect_rows(schema, "", &mut rows);
    let mut out = String::from(
        "| Field | Type | Required | Default | Description |\n\
         |-------|------|----------|---------|-------------|\n",
    );
    out.push_str(&rows);
    out
}

fn collect_rows(schema: &Value, prefix: &str, out: &mut String) {
    let required: Vec<&str> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let Some(props) = schema.get("properties").and_then(Value::as_object) else {
        return;
    };
    for (name, field) in props {
        let path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        let ty = type_label(field);
        let req = if required.contains(&name.as_str()) {
            "yes"
        } else {
            "no"
        };
        let default = field.get("default").map(render_default).unwrap_or_default();
        let desc = field
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("")
            .replace('\n', " ");
        let _ = writeln!(out, "| `{path}` | {ty} | {req} | {default} | {desc} |");
        if field.get("type").and_then(Value::as_str) == Some("object")
            && field.get("properties").is_some()
        {
            collect_rows(field, &path, out);
        }
    }
}

fn type_label(field: &Value) -> String {
    match field.get("type").and_then(Value::as_str) {
        Some("array") => format!(
            "array<{}>",
            field.get("items").map_or_else(|| "any".into(), type_label)
        ),
        Some(t) => t.to_string(),
        None => "object".to_string(),
    }
}

fn render_default(v: &Value) -> String {
    match v {
        Value::String(s) => format!("`{s}`"),
        other => format!("`{other}`"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn renders_nested_object_with_types_and_defaults() {
        let schema = json!({
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": { "type": "string", "description": "The thing's name." },
                "replicas": { "type": "integer", "default": 3, "description": "How many." },
                "nested": { "type": "object", "properties": {
                    "enabled": { "type": "boolean", "description": "Toggle." } } }
            }
        });
        let md = render_field_table(&schema);
        assert!(md.contains("| `name` | string | yes |"));
        assert!(md.contains("The thing's name."));
        assert!(md.contains("| `replicas` | integer | no | `3` |"));
        assert!(md.contains("`nested.enabled`"));
    }

    #[test]
    fn renders_array_item_type() {
        let schema = json!({ "type": "object", "properties": {
            "ports": { "type": "array", "items": { "type": "integer" }, "description": "Listener ports." } } });
        let md = render_field_table(&schema);
        assert!(md.contains("`ports`"));
        assert!(md.contains("array<integer>"));
    }
}
