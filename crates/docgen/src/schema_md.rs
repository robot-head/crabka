//! Render a JSON-Schema-shaped value (`OpenAPI` v3 / schemars output) to a
//! markdown field table. Shared by the CRD and broker-config generators.

use serde_json::Value;
use std::fmt::Write;

/// Render the `properties` of an object schema as a markdown table with one
/// row per (possibly nested) field. Columns: Field, Type, Required, Default,
/// Description. Nested object properties recurse with a dotted path.
///
/// The argument doubles as the root schema for resolving `$ref` pointers
/// (schemars emits `$ref` into a top-level `$defs`/`definitions` table for
/// nested structs); kube-generated CRD schemas are fully inlined and carry no
/// refs, so they render identically.
#[must_use]
pub fn render_field_table(schema: &Value) -> String {
    let mut rows = String::new();
    collect_rows(schema, schema, "", 0, &mut rows);
    let mut out = String::from(
        "| Field | Type | Required | Default | Description |\n\
         |-------|------|----------|---------|-------------|\n",
    );
    out.push_str(&rows);
    out
}

/// Resolve a JSON-pointer `$ref` like `#/$defs/Foo` against `root`. Returns
/// `None` for external or unresolvable refs.
fn resolve_ref<'a>(root: &'a Value, reference: &str) -> Option<&'a Value> {
    let pointer = reference.strip_prefix('#')?;
    root.pointer(pointer)
}

/// Resolve a field schema to its effective underlying schema. Follows a bare
/// `$ref` into `$defs`/`definitions` and unwraps schemars' `Option<T>`-style
/// wrappers — an `anyOf`/`allOf`/`oneOf` whose branches are `[T, null]`
/// collapses to `T` (recursively). Returns a borrow of the effective schema
/// `Value` (the field itself when no `$ref`/wrapper applies).
fn effective_schema<'a>(root: &'a Value, field: &'a Value) -> &'a Value {
    if let Some(reference) = field.get("$ref").and_then(Value::as_str)
        && let Some(target) = resolve_ref(root, reference)
    {
        return target;
    }
    for key in ["anyOf", "allOf", "oneOf"] {
        if let Some(arr) = field.get(key).and_then(Value::as_array) {
            // Pick the first non-"null" branch (schemars' Option<T> pattern).
            for branch in arr {
                let is_null = branch.get("type").and_then(Value::as_str) == Some("null");
                if !is_null {
                    return effective_schema(root, branch);
                }
            }
        }
    }
    field
}

/// Maximum nesting depth for `$ref`/object recursion. Bounds work on
/// self-referential or mutually-recursive schemas so rendering can't overflow
/// the stack; deeper fields still emit their own row but aren't expanded.
const MAX_DEPTH: usize = 12;

fn collect_rows(root: &Value, schema: &Value, prefix: &str, depth: usize, out: &mut String) {
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
        let resolved = effective_schema(root, field);
        let ty = type_label(root, resolved);
        let req = if required.contains(&name.as_str()) {
            "yes"
        } else {
            "no"
        };
        // The default lives on the original field, not the resolved target.
        let default = field
            .get("default")
            .or_else(|| resolved.get("default"))
            .map(render_default)
            .unwrap_or_default();
        let desc = field
            .get("description")
            .or_else(|| resolved.get("description"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .replace('\n', " ")
            // Escape pipes so a description can't break the table's columns.
            .replace('|', "\\|");
        let _ = writeln!(out, "| `{path}` | {ty} | {req} | {default} | {desc} |");
        // Recurse into nested objects (inlined or resolved via $ref), but stop
        // at MAX_DEPTH so cyclic/self-referential schemas can't overflow.
        if resolved.get("properties").is_some() && depth < MAX_DEPTH {
            collect_rows(root, resolved, &path, depth + 1, out);
        }
    }
}

/// First non-"null" entry of a `type` that may be a string or an array
/// (schemars emits `["string", "null"]` for `Option<T>`).
fn primitive_type(field: &Value) -> Option<String> {
    match field.get("type") {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(Value::as_str)
            .find(|t| *t != "null")
            .map(ToString::to_string),
        _ => None,
    }
}

fn type_label(root: &Value, field: &Value) -> String {
    // Follow Option<T>/$ref wrappers before inspecting the type.
    let field = effective_schema(root, field);
    match primitive_type(field).as_deref() {
        Some("array") => {
            let item = field
                .get("items")
                .map_or_else(|| "any".to_string(), |it| type_label(root, it));
            format!("array<{item}>")
        }
        Some(t) => t.to_string(),
        None => "object".to_string(),
    }
}

fn render_default(v: &Value) -> String {
    // Escape pipes so a default containing `|` can't break the table columns,
    // even though the value is wrapped in a code span.
    match v {
        Value::String(s) => format!("`{}`", s.replace('|', "\\|")),
        other => format!("`{}`", other.to_string().replace('|', "\\|")),
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

    /// Mimic schemars 1.x output: `Option<T>` types are arrays `["T","null"]`,
    /// nested structs are `$ref` into `$defs` (directly or wrapped in `anyOf`
    /// with a `null` branch), and `Vec<Struct>` arrays reference items by ref.
    #[test]
    fn resolves_refs_and_nullable_type_arrays() {
        let schema = json!({
            "type": "object",
            "$defs": {
                "Tls": {
                    "type": "object",
                    "required": ["cert"],
                    "properties": {
                        "cert": { "type": "string", "description": "Cert path." }
                    }
                }
            },
            "properties": {
                "broker_id": { "type": ["integer", "null"], "format": "int32" },
                "rack": { "type": ["string", "null"], "default": null, "description": "Rack id." },
                "tls": {
                    "anyOf": [ { "$ref": "#/$defs/Tls" }, { "type": "null" } ],
                    "description": "TLS material."
                },
                "listeners": { "type": "array", "items": { "$ref": "#/$defs/Tls" } }
            }
        });
        let md = render_field_table(&schema);
        // type-array collapses to its non-null member
        assert!(md.contains("| `broker_id` | integer | no |"), "{md}");
        assert!(md.contains("| `rack` | string | no |"), "{md}");
        // anyOf [$ref, null] resolves to the referenced object and recurses
        assert!(md.contains("| `tls` | object | no |"), "{md}");
        assert!(md.contains("| `tls.cert` | string | yes |"), "{md}");
        // array of $ref renders as an object-element array
        assert!(md.contains("| `listeners` | array<object> | no |"), "{md}");
    }

    /// A self-referential `$def` (a property that `$ref`s back to its own def)
    /// must not recurse forever / overflow the stack. The cap bounds the work;
    /// the top-level field row is still emitted.
    #[test]
    fn caps_recursion_on_cyclic_ref() {
        let schema = json!({
            "type": "object",
            "$defs": {
                "Node": {
                    "type": "object",
                    "properties": {
                        // self-reference back into its own def
                        "child": { "$ref": "#/$defs/Node" },
                        "label": { "type": "string", "description": "Node label." }
                    }
                }
            },
            "properties": {
                "root": { "$ref": "#/$defs/Node" }
            }
        });
        // Returns (doesn't hang/overflow) and emits the top-level field row.
        let md = render_field_table(&schema);
        assert!(md.contains("| `root` | object | no |"), "{md}");
        // It recurses some, but the cap keeps the path length bounded.
        let deepest = md
            .lines()
            .filter_map(|l| l.split('`').nth(1))
            .map(|path| path.matches('.').count())
            .max()
            .unwrap_or(0);
        assert!(deepest <= MAX_DEPTH, "path nesting {deepest} exceeded cap");
    }

    #[test]
    fn escapes_pipe_in_description() {
        let schema = json!({
            "type": "object",
            "properties": {
                "mode": {
                    "type": "string",
                    "description": "one of: a | b | c"
                }
            }
        });
        let md = render_field_table(&schema);
        let row = md.lines().find(|l| l.contains("`mode`")).expect("mode row");
        // Pipes in the description are escaped...
        assert!(row.contains("a \\| b \\| c"), "{row}");
        // ...so the row keeps exactly the 5 columns (6 unescaped delimiters).
        let unescaped_bars = row
            .match_indices('|')
            .filter(|(i, _)| *i == 0 || row.as_bytes()[i - 1] != b'\\')
            .count();
        assert_eq!(unescaped_bars, 6, "{row}");
    }
}
