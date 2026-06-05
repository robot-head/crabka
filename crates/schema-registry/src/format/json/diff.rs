//! Structural diff between two JSON Schema documents (`serde_json::Value`),
//! mirroring Confluent's json.diff. Classified by `compat.rs`. No direction
//! logic — the engine swaps (reader, writer) per level.

use std::collections::{BTreeSet, HashSet};

use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    // --- Type ---
    TypeNarrowed,
    TypeExtended,
    TypeChanged,
    // --- Properties ---
    PropertyAddedToOpenContentModel,
    PropertyRemovedFromOpenContentModel,
    PropertyAddedToClosedContentModel,
    PropertyRemovedFromClosedContentModel,
    // --- Required ---
    RequiredAttributeAdded,
    RequiredAttributeRemoved,
    // --- AdditionalProperties ---
    AdditionalPropertiesRemoved,
    AdditionalPropertiesAdded,
    // --- Enum / const ---
    EnumArrayNarrowed,
    EnumArrayExtended,
    EnumArrayChanged,
    // --- Numeric bounds ---
    MaximumAdded,
    MaximumRemoved,
    MaximumDecreased,
    MaximumIncreased,
    MinimumAdded,
    MinimumRemoved,
    MinimumDecreased,
    MinimumIncreased,
    ExclusiveMaximumAdded,
    ExclusiveMaximumRemoved,
    ExclusiveMaximumDecreased,
    ExclusiveMaximumIncreased,
    ExclusiveMinimumAdded,
    ExclusiveMinimumRemoved,
    ExclusiveMinimumDecreased,
    ExclusiveMinimumIncreased,
    MultipleOfAdded,
    MultipleOfRemoved,
    MultipleOfChanged,
    // --- String ---
    MaxLengthAdded,
    MaxLengthRemoved,
    MaxLengthDecreased,
    MaxLengthIncreased,
    MinLengthAdded,
    MinLengthRemoved,
    MinLengthDecreased,
    MinLengthIncreased,
    PatternAdded,
    PatternRemoved,
    PatternChanged,
    // --- Array ---
    MaxItemsAdded,
    MaxItemsRemoved,
    MaxItemsDecreased,
    MaxItemsIncreased,
    MinItemsAdded,
    MinItemsRemoved,
    MinItemsDecreased,
    MinItemsIncreased,
    AdditionalItemsRemoved,
    AdditionalItemsAdded,
    // --- Object size ---
    MaxPropertiesAdded,
    MaxPropertiesRemoved,
    MaxPropertiesDecreased,
    MaxPropertiesIncreased,
    MinPropertiesAdded,
    MinPropertiesRemoved,
    MinPropertiesDecreased,
    MinPropertiesIncreased,
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

/// Context for $ref resolution — carries the document roots and a cycle-guard
/// set of `(orig_ptr, upd_ptr)` pairs already visited.
#[allow(dead_code)] // fields used in $ref resolution (added in Task 4)
struct Ctx<'a> {
    orig_root: &'a Value,
    upd_root: &'a Value,
    visited: HashSet<(String, String)>,
}

impl<'a> Ctx<'a> {
    fn new(orig_root: &'a Value, upd_root: &'a Value) -> Self {
        Ctx {
            orig_root,
            upd_root,
            visited: HashSet::new(),
        }
    }
}

#[must_use]
pub fn compare(original: &Value, update: &Value) -> Vec<Difference> {
    let mut out = Vec::new();
    let mut ctx = Ctx::new(original, update);
    compare_schema("#", original, update, &mut ctx, &mut out);
    out
}

fn compare_schema(
    path: &str,
    orig: &Value,
    upd: &Value,
    ctx: &mut Ctx<'_>,
    out: &mut Vec<Difference>,
) {
    compare_type(path, orig, upd, out);
    compare_enum(path, orig, upd, out);
    compare_properties(path, orig, upd, ctx, out);
    compare_required(path, orig, upd, out);
    compare_additional_properties(path, orig, upd, out);
    compare_numeric(path, orig, upd, out);
    compare_string_constraints(path, orig, upd, out);
    compare_array_constraints(path, orig, upd, ctx, out);
    compare_object_size(path, orig, upd, out);
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

fn compare_properties(
    path: &str,
    orig: &Value,
    upd: &Value,
    ctx: &mut Ctx<'_>,
    out: &mut Vec<Difference>,
) {
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
                compare_schema(
                    &format!("{path}/properties/{name}"),
                    oschema,
                    uschema,
                    ctx,
                    out,
                );
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

// ---------------------------------------------------------------------------
// Enum / const
// ---------------------------------------------------------------------------

fn canonical_value(v: &Value) -> String {
    crate::format::json::canonicalize(v)
}

fn compare_enum(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    let oe = enum_set(orig);
    let ue = enum_set(upd);
    if oe == ue {
        return;
    }
    match (oe, ue) {
        (Some(os), Some(us)) => {
            if us.is_subset(&os) {
                out.push(d(Kind::EnumArrayNarrowed, path));
            } else if os.is_subset(&us) {
                out.push(d(Kind::EnumArrayExtended, path));
            } else {
                out.push(d(Kind::EnumArrayChanged, path));
            }
        }
        (None, Some(_)) => out.push(d(Kind::EnumArrayNarrowed, path)),
        (Some(_), None) => out.push(d(Kind::EnumArrayExtended, path)),
        (None, None) => {}
    }
}

fn enum_set(schema: &Value) -> Option<BTreeSet<String>> {
    // support both `enum` array and `const` (treated as single-element enum)
    if let Some(arr) = schema.get("enum").and_then(Value::as_array) {
        Some(arr.iter().map(canonical_value).collect())
    } else {
        schema
            .get("const")
            .map(|c| BTreeSet::from([canonical_value(c)]))
    }
}

// ---------------------------------------------------------------------------
// Numeric bounds
// ---------------------------------------------------------------------------

fn num(s: &Value, k: &str) -> Option<f64> {
    s.get(k).and_then(Value::as_f64)
}

fn compare_numeric(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    compare_bound(
        path,
        orig,
        upd,
        out,
        "maximum",
        Kind::MaximumAdded,
        Kind::MaximumRemoved,
        Kind::MaximumDecreased,
        Kind::MaximumIncreased,
        true, // max: decreased = tighter
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "minimum",
        Kind::MinimumAdded,
        Kind::MinimumRemoved,
        Kind::MinimumDecreased,
        Kind::MinimumIncreased,
        false, // min: increased = tighter (Decreased = looser)
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "exclusiveMaximum",
        Kind::ExclusiveMaximumAdded,
        Kind::ExclusiveMaximumRemoved,
        Kind::ExclusiveMaximumDecreased,
        Kind::ExclusiveMaximumIncreased,
        true,
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "exclusiveMinimum",
        Kind::ExclusiveMinimumAdded,
        Kind::ExclusiveMinimumRemoved,
        Kind::ExclusiveMinimumDecreased,
        Kind::ExclusiveMinimumIncreased,
        false,
    );
    // multipleOf: added or changed = tighter
    match (num(orig, "multipleOf"), num(upd, "multipleOf")) {
        (None, Some(_)) => out.push(d(Kind::MultipleOfAdded, path)),
        (Some(_), None) => out.push(d(Kind::MultipleOfRemoved, path)),
        (Some(o), Some(u)) if (o - u).abs() > f64::EPSILON => {
            out.push(d(Kind::MultipleOfChanged, path));
        }
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn compare_bound(
    path: &str,
    orig: &Value,
    upd: &Value,
    out: &mut Vec<Difference>,
    key: &str,
    kind_added: Kind,
    kind_removed: Kind,
    kind_decreased: Kind,
    kind_increased: Kind,
    max_style: bool, // true = max (decreased=tighter), false = min (increased=tighter)
) {
    match (num(orig, key), num(upd, key)) {
        (None, Some(_)) => out.push(d(kind_added, path)),
        (Some(_), None) => out.push(d(kind_removed, path)),
        (Some(o), Some(u)) => {
            if (o - u).abs() > f64::EPSILON {
                if u < o {
                    if max_style {
                        out.push(d(kind_decreased, path)); // max decreased = tighter
                    } else {
                        out.push(d(kind_decreased, path)); // min decreased = looser
                    }
                } else if max_style {
                    out.push(d(kind_increased, path)); // max increased = looser
                } else {
                    out.push(d(kind_increased, path)); // min increased = tighter
                }
            }
        }
        (None, None) => {}
    }
}

// ---------------------------------------------------------------------------
// String constraints
// ---------------------------------------------------------------------------

fn compare_string_constraints(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    compare_bound(
        path,
        orig,
        upd,
        out,
        "maxLength",
        Kind::MaxLengthAdded,
        Kind::MaxLengthRemoved,
        Kind::MaxLengthDecreased,
        Kind::MaxLengthIncreased,
        true,
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "minLength",
        Kind::MinLengthAdded,
        Kind::MinLengthRemoved,
        Kind::MinLengthDecreased,
        Kind::MinLengthIncreased,
        false,
    );
    // pattern
    let op = orig.get("pattern").and_then(Value::as_str);
    let up = upd.get("pattern").and_then(Value::as_str);
    match (op, up) {
        (None, Some(_)) => out.push(d(Kind::PatternAdded, path)),
        (Some(_), None) => out.push(d(Kind::PatternRemoved, path)),
        (Some(o), Some(u)) if o != u => out.push(d(Kind::PatternChanged, path)),
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Array constraints
// ---------------------------------------------------------------------------

fn compare_array_constraints(
    path: &str,
    orig: &Value,
    upd: &Value,
    ctx: &mut Ctx<'_>,
    out: &mut Vec<Difference>,
) {
    // items: if both are object schemas, recurse
    let oi = orig.get("items");
    let ui = upd.get("items");
    if let (Some(oi), Some(ui)) = (oi, ui)
        && oi.is_object()
        && ui.is_object()
    {
        compare_schema(&format!("{path}/items"), oi, ui, ctx, out);
    }

    compare_bound(
        path,
        orig,
        upd,
        out,
        "maxItems",
        Kind::MaxItemsAdded,
        Kind::MaxItemsRemoved,
        Kind::MaxItemsDecreased,
        Kind::MaxItemsIncreased,
        true,
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "minItems",
        Kind::MinItemsAdded,
        Kind::MinItemsRemoved,
        Kind::MinItemsDecreased,
        Kind::MinItemsIncreased,
        false,
    );

    // additionalItems: false in update = tighter
    let oa = orig.get("additionalItems");
    let ua = upd.get("additionalItems");
    let o_false = matches!(oa, Some(Value::Bool(false)));
    let u_false = matches!(ua, Some(Value::Bool(false)));
    if !o_false && u_false {
        out.push(d(Kind::AdditionalItemsRemoved, path));
    } else if o_false && !u_false {
        out.push(d(Kind::AdditionalItemsAdded, path));
    }
}

// ---------------------------------------------------------------------------
// Object size
// ---------------------------------------------------------------------------

fn compare_object_size(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    compare_bound(
        path,
        orig,
        upd,
        out,
        "maxProperties",
        Kind::MaxPropertiesAdded,
        Kind::MaxPropertiesRemoved,
        Kind::MaxPropertiesDecreased,
        Kind::MaxPropertiesIncreased,
        true,
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "minProperties",
        Kind::MinPropertiesAdded,
        Kind::MinPropertiesRemoved,
        Kind::MinPropertiesDecreased,
        Kind::MinPropertiesIncreased,
        false,
    );
}
