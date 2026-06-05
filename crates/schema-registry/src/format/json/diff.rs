//! Structural diff between two JSON Schema documents (`serde_json::Value`),
//! mirroring Confluent's json.diff. Classified by `compat.rs`. No direction
//! logic — the engine swaps (reader, writer) per level.

use std::collections::BTreeSet;

use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    TypeNarrowed,
    TypeExtended,
    TypeChanged,
    PropertyAddedToOpenContentModel,
    PropertyRemovedFromOpenContentModel,
    PropertyAddedToClosedContentModel,
    PropertyRemovedFromClosedContentModel,
    RequiredAttributeAdded,
    RequiredAttributeRemoved,
    AdditionalPropertiesRemoved,
    AdditionalPropertiesAdded,
}

#[derive(Debug, Clone)]
pub struct Difference {
    pub kind: Kind,
    pub path: String,
}

fn d(kind: Kind, path: &str) -> Difference {
    Difference {
        kind,
        path: path.to_string(),
    }
}

#[must_use]
pub fn compare(original: &Value, update: &Value) -> Vec<Difference> {
    let mut out = Vec::new();
    compare_schema("#", original, update, &mut out);
    out
}

fn compare_schema(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    compare_type(path, orig, upd, out);
    compare_properties(path, orig, upd, out);
    compare_required(path, orig, upd, out);
    compare_additional_properties(path, orig, upd, out);
}

fn types_of(schema: &Value) -> BTreeSet<String> {
    match schema.get("type") {
        Some(Value::String(s)) => BTreeSet::from([s.clone()]),
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        _ => BTreeSet::new(),
    }
}

fn compare_type(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    let (ot, ut) = (types_of(orig), types_of(upd));
    if ot == ut {
        return;
    }
    if ot.is_empty() && !ut.is_empty() {
        out.push(d(Kind::TypeNarrowed, path));
    } else if ut.is_empty() && !ot.is_empty() {
        out.push(d(Kind::TypeExtended, path));
    } else if ut.is_subset(&ot) {
        out.push(d(Kind::TypeNarrowed, path));
    } else if ot.is_subset(&ut) {
        out.push(d(Kind::TypeExtended, path));
    } else {
        out.push(d(Kind::TypeChanged, path));
    }
}

fn is_closed(schema: &Value) -> bool {
    matches!(schema.get("additionalProperties"), Some(Value::Bool(false)))
}

fn props(schema: &Value) -> Option<&serde_json::Map<String, Value>> {
    schema.get("properties").and_then(Value::as_object)
}

fn required_set(schema: &Value) -> BTreeSet<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

fn compare_properties(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    let closed = is_closed(upd) || is_closed(orig);
    let empty = serde_json::Map::new();
    let op = props(orig).unwrap_or(&empty);
    let up = props(upd).unwrap_or(&empty);
    for name in op.keys() {
        if !up.contains_key(name) {
            let kind = if closed {
                Kind::PropertyRemovedFromClosedContentModel
            } else {
                Kind::PropertyRemovedFromOpenContentModel
            };
            out.push(d(kind, &format!("{path}/properties/{name}")));
        }
    }
    for (name, uschema) in up {
        match op.get(name) {
            None => {
                let kind = if closed {
                    Kind::PropertyAddedToClosedContentModel
                } else {
                    Kind::PropertyAddedToOpenContentModel
                };
                out.push(d(kind, &format!("{path}/properties/{name}")));
            }
            Some(oschema) => {
                compare_schema(&format!("{path}/properties/{name}"), oschema, uschema, out);
            }
        }
    }
}

fn compare_required(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    let (orq, urq) = (required_set(orig), required_set(upd));
    for name in urq.difference(&orq) {
        out.push(d(
            Kind::RequiredAttributeAdded,
            &format!("{path}/required/{name}"),
        ));
    }
    for name in orq.difference(&urq) {
        out.push(d(
            Kind::RequiredAttributeRemoved,
            &format!("{path}/required/{name}"),
        ));
    }
}

fn compare_additional_properties(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    let oa = orig.get("additionalProperties");
    let ua = upd.get("additionalProperties");
    let o_false = matches!(oa, Some(Value::Bool(false)));
    let u_false = matches!(ua, Some(Value::Bool(false)));
    if o_false && !u_false {
        out.push(d(Kind::AdditionalPropertiesAdded, path));
    } else if !o_false && u_false {
        out.push(d(Kind::AdditionalPropertiesRemoved, path));
    }
}
