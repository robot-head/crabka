//! Render a JSON-Schema-shaped value to a markdown field table.
//!
//! The input is `OpenAPI` v3 output or schemars output. The CRD generator and
//! the broker-config generator share this module.

use std::fmt::Write;

use serde_json::Value;

/// Render the `properties` of an object schema as a markdown table.
///
/// The table has one row per field, and a field can be nested. The columns are
/// Field, Type, Required, Default, and Description. Nested object properties
/// recurse with a dotted path.
///
/// The argument is also the root schema that resolves `$ref` pointers. schemars
/// emits `$ref` into a top-level `$defs` or `definitions` table for nested
/// structs. Kube-generated CRD schemas are fully inlined and carry no refs, so
/// they render identically.
#[must_use]
pub fn render_field_table(schema: &Value) -> String {
    let mut rows = String::new();
    collect_rows(schema, schema, "", 0, &mut rows);
    let mut out = field_table_header();
    out.push_str(&rows);
    out
}

/// Render the root schema's top-level properties as separate, captioned
/// subsections instead of one flat table.
///
/// These subsections are easier to scan than the large dense table that
/// `render_field_table` produces.
///
/// Each top-level property that resolves to an object, that is to a schema with
/// `properties`, gets its own `## <key>` heading, its description as a blurb,
/// and a focused field table. The rows of that table are relative to the
/// subtree, so the function removes the leading `<key>.` prefix. All the other
/// top-level properties go under one leading `## General` section, as one table
/// keyed by the bare property name. These are the scalars, the arrays, and the
/// objects without `properties`. A `---` horizontal rule separates the
/// sections.
///
/// This function shares all the type, default, description, and escaping logic
/// with `render_field_table` through `effective_schema`, `type_label`,
/// `render_default`, and `collect_rows`. `render_field_table` keeps its flat
/// layout, so the CRD pages and the other pages keep that layout too.
#[must_use]
pub fn render_sectioned_field_table(schema: &Value) -> String {
    let required: Vec<&str> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let Some(props) = schema.get("properties").and_then(Value::as_object) else {
        return String::new();
    };

    // Partition top-level properties: object-with-`properties` ones become
    // their own section; everything else (scalars, arrays, refless objects)
    // lands in the shared "General" table.
    let mut general: Vec<(&String, &Value)> = Vec::new();
    let mut sections: Vec<(&String, &Value, &Value)> = Vec::new();
    for (name, field) in props {
        let resolved = effective_schema(schema, field);
        if resolved.get("properties").is_some() {
            sections.push((name, field, resolved));
        } else {
            general.push((name, field));
        }
    }

    let mut parts: Vec<String> = Vec::new();

    if !general.is_empty() {
        let mut body = String::from("## General\n\n");
        body.push_str(&field_table_header());
        for (name, field) in general {
            let resolved = effective_schema(schema, field);
            let req = required.contains(&name.as_str());
            write_field_row(schema, name, field, resolved, req, &mut body);
        }
        parts.push(body);
    }

    for (name, field, resolved) in sections {
        let mut body = format!("## {name}\n\n");
        if let Some(desc) = field
            .get("description")
            .or_else(|| resolved.get("description"))
            .and_then(Value::as_str)
        {
            // Blurbs are prose, not table cells, so no pipe escaping needed;
            // collapse newlines for a single tidy paragraph.
            body.push_str(&desc.replace('\n', " "));
            body.push_str("\n\n");
        }
        body.push_str(&field_table_header());
        // Reuse the dotted-path row logic scoped to this subtree, with an empty
        // prefix so rows read relative to the section (no leading `<key>.`).
        collect_rows(schema, resolved, "", 0, &mut body);
        parts.push(body);
    }

    parts.join("\n\n---\n\n")
}

/// The shared 5-column field-table header.
///
/// `render_field_table` holds the same header as a literal. This copy for the
/// sectioned variant stays in sync with that literal.
fn field_table_header() -> String {
    String::from(
        "| Field | Type | Required | Default | Description |\n\
         |-------|------|----------|---------|-------------|\n",
    )
}

/// Emit a single field row for `name`.
///
/// The "General" section uses this function, and it renders scalar and array
/// top-level props as plain one-row entries. The function mirrors the per-field
/// formatting in `collect_rows`, so the type, the default, the description, and
/// the escaping stay identical.
fn write_field_row(
    root: &Value,
    name: &str,
    field: &Value,
    resolved: &Value,
    required: bool,
    out: &mut String,
) {
    let ty = type_label(root, resolved);
    let req = if required { "yes" } else { "no" };
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
        .replace('|', "\\|");
    let _ = writeln!(out, "| `{name}` | {ty} | {req} | {default} | {desc} |");
}

/// Resolve a JSON-pointer `$ref` like `#/$defs/Foo` against `root`.
///
/// The function returns `None` for external or unresolvable refs.
fn resolve_ref<'a>(root: &'a Value, reference: &str) -> Option<&'a Value> {
    let pointer = reference.strip_prefix('#')?;
    root.pointer(pointer)
}

/// Resolve a field schema to its effective underlying schema.
///
/// The function follows a bare `$ref` into `$defs` or `definitions`, and it
/// unwraps schemars' `Option<T>`-style wrappers. An `anyOf`, `allOf`, or
/// `oneOf` whose branches are `[T, null]` collapses to `T`, and the function
/// applies this rule recursively. It returns a borrow of the effective schema
/// `Value`. That value is the field itself when no `$ref` and no wrapper
/// applies.
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

/// Maximum nesting depth for `$ref` and object recursion.
///
/// This bound limits the work on self-referential or mutually recursive
/// schemas, so the rendering cannot overflow the stack. Deeper fields still
/// emit their own row, but the renderer does not expand them.
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

/// First non-"null" entry of a `type` that can be a string or an array.
///
/// schemars emits `["string", "null"]` for `Option<T>`.
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
    use assert2::check;
    use serde_json::json;

    use super::*;

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
        for needle in [
            "| `name` | string | yes |",
            "The thing's name.",
            "| `replicas` | integer | no | `3` |",
            "`nested.enabled`",
        ] {
            assert2::assert!(md.contains(needle));
        }
    }

    #[test]
    fn renders_array_item_type() {
        let schema = json!({ "type": "object", "properties": {
            "ports": { "type": "array", "items": { "type": "integer" }, "description": "Listener ports." } } });
        let md = render_field_table(&schema);
        assert2::assert!(md.contains("`ports`"));
        assert2::assert!(md.contains("array<integer>"));
    }

    /// Mimic schemars 1.x output. `Option<T>` types are arrays `["T","null"]`.
    /// Nested structs are a `$ref` into `$defs`, either directly or wrapped in
    /// `anyOf` with a `null` branch. `Vec<Struct>` arrays reference items by
    /// ref.
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
        for needle in [
            // type-array collapses to its non-null member
            "| `broker_id` | integer | no |",
            "| `rack` | string | no |",
            // anyOf [$ref, null] resolves to the referenced object and recurses
            "| `tls` | object | no |",
            "| `tls.cert` | string | yes |",
            // array of $ref renders as an object-element array
            "| `listeners` | array<object> | no |",
        ] {
            assert2::assert!(md.contains(needle));
        }
    }

    /// A self-referential `$def` is a property that `$ref`s back to its own
    /// def. It must not recurse forever and it must not overflow the stack. The
    /// cap limits the work, and the renderer still emits the top-level field
    /// row.
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
        assert2::assert!(md.contains("| `root` | object | no |"));
        // It recurses some, but the cap keeps the path length bounded.
        let deepest = md
            .lines()
            .filter_map(|l| l.split('`').nth(1))
            .map(|path| path.matches('.').count())
            .max()
            .unwrap_or(0);
        assert2::assert!(deepest <= MAX_DEPTH);
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
        assert2::assert!(row.contains("a \\| b \\| c"));
        // ...so the row keeps exactly the 5 columns (6 unescaped delimiters).
        let unescaped_bars = row
            .match_indices('|')
            .filter(|(i, _)| *i == 0 || row.as_bytes()[i - 1] != b'\\')
            .count();
        assert2::assert!(unescaped_bars == 6);
    }

    #[test]
    fn sectioned_table_emits_headings_and_separators() {
        let schema = json!({
            "type": "object",
            "required": ["broker_id"],
            "$defs": {
                "Tls": {
                    "type": "object",
                    "description": "TLS material for the listener.",
                    "required": ["cert"],
                    "properties": {
                        "cert": { "type": "string", "description": "Cert path." },
                        "key": { "type": "string", "description": "Key path." }
                    }
                }
            },
            "properties": {
                "broker_id": { "type": "integer", "description": "This broker's id." },
                "log_dir": { "type": ["string", "null"], "description": "Data directory." },
                "tls": {
                    "anyOf": [ { "$ref": "#/$defs/Tls" }, { "type": "null" } ],
                    "description": "Server TLS config."
                }
            }
        });
        let md = render_sectioned_field_table(&schema);
        // Scalars are grouped under a single General section...
        check!(md.contains("## General"), "{md}");
        check!(md.contains("| `broker_id` | integer | yes |"), "{md}");
        check!(md.contains("| `log_dir` | string | no |"), "{md}");
        // ...objects get their own captioned section with the field's blurb...
        check!(md.contains("## tls"), "{md}");
        check!(md.contains("Server TLS config."), "{md}");
        // ...whose rows are relative to the subtree (no leading `tls.` prefix).
        check!(md.contains("| `cert` | string | yes |"), "{md}");
        check!(!md.contains("`tls.cert`"), "{md}");
        // Sections are separated by a horizontal rule.
        check!(md.contains("\n---\n"), "{md}");
    }

    #[test]
    fn sectioned_table_escapes_pipes_in_section_rows() {
        let schema = json!({
            "type": "object",
            "$defs": {
                "Inner": {
                    "type": "object",
                    "properties": {
                        "mode": { "type": "string", "description": "one of: a | b | c" }
                    }
                }
            },
            "properties": {
                "section": { "$ref": "#/$defs/Inner" }
            }
        });
        let md = render_sectioned_field_table(&schema);
        let row = md.lines().find(|l| l.contains("`mode`")).expect("mode row");
        assert2::assert!(row.contains("a \\| b \\| c"));
        let unescaped_bars = row
            .match_indices('|')
            .filter(|(i, _)| *i == 0 || row.as_bytes()[i - 1] != b'\\')
            .count();
        assert2::assert!(unescaped_bars == 6);
    }
}
