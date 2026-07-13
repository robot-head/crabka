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
    // --- Combinators ---
    CombinedTypeChanged,
    ProductTypeExtended,
    ProductTypeNarrowed,
    SumTypeExtended,
    SumTypeNarrowed,
    #[allow(dead_code)] // retained for wider not-schema diagnostics
    NotTypeExtended,
    NotTypeNarrowed,
    CombinedTypeSubschemasChanged,
    // --- $ref / dependencies / conditionals ---
    DependencyAdded,
    DependencyRemoved,
    ConditionalChanged,
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

/// A side's registry references as `(name, document)` pairs. `name` is the
/// `$ref` target string a referring schema uses to point at that document.
type RefMap = [(String, Value)];

/// Context for $ref resolution — carries each side's document root and registry
/// ref-map, plus a cycle-guard set of `(orig_ptr, upd_ptr)` pairs already
/// visited.
struct Ctx<'a> {
    orig_root: &'a Value,
    upd_root: &'a Value,
    orig_refs: &'a RefMap,
    upd_refs: &'a RefMap,
    visited: HashSet<(String, String)>,
}

impl<'a> Ctx<'a> {
    fn new(
        orig_root: &'a Value,
        upd_root: &'a Value,
        orig_refs: &'a RefMap,
        upd_refs: &'a RefMap,
    ) -> Self {
        Ctx {
            orig_root,
            upd_root,
            orig_refs,
            upd_refs,
            visited: HashSet::new(),
        }
    }
}

/// Diff two JSON Schema documents. Each side carries a registry ref-map so a
/// `$ref` whose target is not an intra-document `#/...` pointer can resolve
/// against a registered reference's document. With empty ref-maps an unmatched
/// non-`#` `$ref` stays permissive.
#[must_use]
pub fn compare_with_refs(
    original: &Value,
    update: &Value,
    original_refs: &RefMap,
    update_refs: &RefMap,
) -> Vec<Difference> {
    let mut out = Vec::new();
    let mut ctx = Ctx::new(original, update, original_refs, update_refs);
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
    compare_combinators(path, orig, upd, ctx, out);
    compare_refs(path, orig, upd, ctx, out);
    compare_dependencies(path, orig, upd, out);
    compare_conditionals(path, orig, upd, ctx, out);
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
    // NOTE: only the boolean open/closed transition is classified; a
    // schema-valued additionalProperties narrowing is treated permissively.
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
        (
            Kind::MaximumAdded,
            Kind::MaximumRemoved,
            Kind::MaximumDecreased,
            Kind::MaximumIncreased,
        ),
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "minimum",
        (
            Kind::MinimumAdded,
            Kind::MinimumRemoved,
            Kind::MinimumDecreased,
            Kind::MinimumIncreased,
        ),
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "exclusiveMaximum",
        (
            Kind::ExclusiveMaximumAdded,
            Kind::ExclusiveMaximumRemoved,
            Kind::ExclusiveMaximumDecreased,
            Kind::ExclusiveMaximumIncreased,
        ),
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "exclusiveMinimum",
        (
            Kind::ExclusiveMinimumAdded,
            Kind::ExclusiveMinimumRemoved,
            Kind::ExclusiveMinimumDecreased,
            Kind::ExclusiveMinimumIncreased,
        ),
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

fn compare_bound(
    path: &str,
    orig: &Value,
    upd: &Value,
    out: &mut Vec<Difference>,
    key: &str,
    kinds: (Kind, Kind, Kind, Kind),
) {
    let (kind_added, kind_removed, kind_decreased, kind_increased) = kinds;
    match (num(orig, key), num(upd, key)) {
        (None, Some(_)) => out.push(d(kind_added, path)),
        (Some(_), None) => out.push(d(kind_removed, path)),
        (Some(o), Some(u)) => {
            if (o - u).abs() > f64::EPSILON {
                if u < o {
                    out.push(d(kind_decreased, path));
                } else {
                    out.push(d(kind_increased, path));
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
        (
            Kind::MaxLengthAdded,
            Kind::MaxLengthRemoved,
            Kind::MaxLengthDecreased,
            Kind::MaxLengthIncreased,
        ),
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "minLength",
        (
            Kind::MinLengthAdded,
            Kind::MinLengthRemoved,
            Kind::MinLengthDecreased,
            Kind::MinLengthIncreased,
        ),
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
        (
            Kind::MaxItemsAdded,
            Kind::MaxItemsRemoved,
            Kind::MaxItemsDecreased,
            Kind::MaxItemsIncreased,
        ),
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "minItems",
        (
            Kind::MinItemsAdded,
            Kind::MinItemsRemoved,
            Kind::MinItemsDecreased,
            Kind::MinItemsIncreased,
        ),
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
        (
            Kind::MaxPropertiesAdded,
            Kind::MaxPropertiesRemoved,
            Kind::MaxPropertiesDecreased,
            Kind::MaxPropertiesIncreased,
        ),
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "minProperties",
        (
            Kind::MinPropertiesAdded,
            Kind::MinPropertiesRemoved,
            Kind::MinPropertiesDecreased,
            Kind::MinPropertiesIncreased,
        ),
    );
}

// ---------------------------------------------------------------------------
// Combinators: allOf / anyOf / oneOf / not
// ---------------------------------------------------------------------------

fn combinator_keyword(schema: &Value) -> Option<&str> {
    ["allOf", "anyOf", "oneOf", "not"]
        .iter()
        .copied()
        .find(|kw| schema.get(kw).is_some())
}

fn canonicalize_subschemas(arr: &[Value]) -> BTreeSet<String> {
    arr.iter().map(canonical_value).collect()
}

fn compare_combinators(
    path: &str,
    orig: &Value,
    upd: &Value,
    ctx: &mut Ctx<'_>,
    out: &mut Vec<Difference>,
) {
    let ok = combinator_keyword(orig);
    let uk = combinator_keyword(upd);

    match (ok, uk) {
        (None, None) => {}
        (Some(ok), Some(uk)) if ok == uk => {
            // same keyword — compare subschemas
            let cpath = format!("{path}/{ok}");
            match ok {
                "not" => {
                    let on = orig.get("not").unwrap();
                    let un = upd.get("not").unwrap();
                    // recurse for structural diff; report change if canonical differs
                    let oc = canonical_value(on);
                    let uc = canonical_value(un);
                    if oc != uc {
                        // Conservative: any change to a `not` subschema is classified incompatible (NotTypeNarrowed); the cp matrix only exercises not-added (CombinedTypeChanged), so the directional split is unexercised.
                        out.push(d(Kind::NotTypeNarrowed, &cpath));
                        compare_schema(&format!("{path}/not"), on, un, ctx, out);
                    }
                }
                "allOf" => {
                    let os = orig
                        .get("allOf")
                        .and_then(Value::as_array)
                        .map_or(&[] as &[Value], Vec::as_slice);
                    let us = upd
                        .get("allOf")
                        .and_then(Value::as_array)
                        .map_or(&[] as &[Value], Vec::as_slice);
                    let oss = canonicalize_subschemas(os);
                    let uss = canonicalize_subschemas(us);
                    if oss != uss {
                        if uss.is_superset(&oss) {
                            // allOf: more constraints = NARROWER
                            out.push(d(Kind::ProductTypeNarrowed, &cpath));
                        } else if oss.is_superset(&uss) {
                            out.push(d(Kind::ProductTypeExtended, &cpath));
                        } else {
                            out.push(d(Kind::CombinedTypeSubschemasChanged, &cpath));
                        }
                    }
                }
                "anyOf" | "oneOf" => {
                    let os = orig
                        .get(ok)
                        .and_then(Value::as_array)
                        .map_or(&[] as &[Value], Vec::as_slice);
                    let us = upd
                        .get(uk)
                        .and_then(Value::as_array)
                        .map_or(&[] as &[Value], Vec::as_slice);
                    let oss = canonicalize_subschemas(os);
                    let uss = canonicalize_subschemas(us);
                    if oss != uss {
                        if uss.is_superset(&oss) {
                            // anyOf/oneOf: more alternatives = WIDER
                            out.push(d(Kind::SumTypeExtended, &cpath));
                        } else if oss.is_superset(&uss) {
                            out.push(d(Kind::SumTypeNarrowed, &cpath));
                        } else {
                            out.push(d(Kind::CombinedTypeSubschemasChanged, &cpath));
                        }
                    }
                }
                _ => {}
            }
        }
        // Different keywords or one absent → incompatible structural change
        (Some(_) | None, Some(_)) | (Some(_), None) => {
            out.push(d(Kind::CombinedTypeChanged, path));
        }
    }
}

// ---------------------------------------------------------------------------
// $ref resolution
// ---------------------------------------------------------------------------

/// Resolve a `$ref`. An intra-document `#/...` pointer resolves against `root`
/// (unchanged). A non-`#` ref resolves against `refs` if its string matches a
/// registered reference's `name`; otherwise it stays permissive (`None`). Both
/// branches borrow for the same lifetime, so the result is a plain `&Value`.
fn resolve_ref<'a>(schema: &Value, root: &'a Value, refs: &'a RefMap) -> Option<&'a Value> {
    let ref_str = schema.get("$ref").and_then(Value::as_str)?;
    if let Some(ptr) = ref_str.strip_prefix('#') {
        return if ptr.is_empty() {
            Some(root)
        } else {
            root.pointer(ptr)
        };
    }
    // Non-local ref: resolve against the registry ref-map by name, else permissive.
    refs.iter().find(|(n, _)| n == ref_str).map(|(_, v)| v)
}

fn compare_refs(
    path: &str,
    orig: &Value,
    upd: &Value,
    ctx: &mut Ctx<'_>,
    out: &mut Vec<Difference>,
) {
    let o_ref = orig.get("$ref").and_then(Value::as_str).map(String::from);
    let u_ref = upd.get("$ref").and_then(Value::as_str).map(String::from);

    if let (None, None) = (&o_ref, &u_ref) {
        return;
    }

    // Build cycle-guard key from the two ref strings (or a sentinel for absent)
    let key = (
        o_ref.clone().unwrap_or_default(),
        u_ref.clone().unwrap_or_default(),
    );
    if ctx.visited.contains(&key) {
        return; // already walking this pair — cycle, stop
    }
    ctx.visited.insert(key.clone());

    // Resolve each side against its own root + ref-map; an unmatched non-local
    // ref leaves that side permissive (None). Don't cross the streams.
    let o_resolved = o_ref
        .as_deref()
        .and_then(|_| resolve_ref(orig, ctx.orig_root, ctx.orig_refs));
    let u_resolved = u_ref
        .as_deref()
        .and_then(|_| resolve_ref(upd, ctx.upd_root, ctx.upd_refs));

    match (o_resolved, u_resolved) {
        (Some(ores), Some(ures)) => {
            // Both resolve — diff the targets; clone to avoid borrow issues
            let ores = ores.clone();
            let ures = ures.clone();
            compare_schema(&format!("{path}/$ref"), &ores, &ures, ctx, out);
        }
        (Some(ores), None) => {
            // orig had a $ref, update doesn't — diff resolved orig vs update directly
            let ores = ores.clone();
            compare_schema(path, &ores, upd, ctx, out);
        }
        (None, Some(ures)) => {
            // update has a $ref, orig doesn't — diff orig vs resolved update
            let ures = ures.clone();
            compare_schema(path, orig, &ures, ctx, out);
        }
        (None, None) => {
            // neither resolved (remote refs or unresolvable) — treat permissively
        }
    }

    ctx.visited.remove(&key);
}

// ---------------------------------------------------------------------------
// Dependencies
// ---------------------------------------------------------------------------

fn dep_keys(schema: &Value) -> Option<BTreeSet<String>> {
    // Check `dependencies` (draft-07) or `dependentRequired`/`dependentSchemas` (draft 2019-09+)
    for kw in &["dependencies", "dependentRequired", "dependentSchemas"] {
        if let Some(obj) = schema.get(kw).and_then(Value::as_object) {
            return Some(obj.keys().cloned().collect());
        }
    }
    None
}

fn compare_dependencies(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    let ok = dep_keys(orig);
    let uk = dep_keys(upd);
    match (ok, uk) {
        (None, None) => {}
        (None, Some(uk)) => {
            for k in &uk {
                out.push(d(
                    Kind::DependencyAdded,
                    &format!("{path}/dependencies/{k}"),
                ));
            }
        }
        (Some(ok), None) => {
            for k in &ok {
                out.push(d(
                    Kind::DependencyRemoved,
                    &format!("{path}/dependencies/{k}"),
                ));
            }
        }
        (Some(ok), Some(uk)) => {
            for k in uk.difference(&ok) {
                out.push(d(
                    Kind::DependencyAdded,
                    &format!("{path}/dependencies/{k}"),
                ));
            }
            for k in ok.difference(&uk) {
                out.push(d(
                    Kind::DependencyRemoved,
                    &format!("{path}/dependencies/{k}"),
                ));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Conditionals: if / then / else
// ---------------------------------------------------------------------------

fn compare_conditionals(
    path: &str,
    orig: &Value,
    upd: &Value,
    ctx: &mut Ctx<'_>,
    out: &mut Vec<Difference>,
) {
    // Check if any of the conditional keywords are present in either schema
    let has_cond_orig =
        orig.get("if").is_some() || orig.get("then").is_some() || orig.get("else").is_some();
    let has_cond_upd =
        upd.get("if").is_some() || upd.get("then").is_some() || upd.get("else").is_some();

    if !has_cond_orig && !has_cond_upd {
        return;
    }

    // If both sides have the same structure, recurse into the branches; otherwise flag changed.
    for kw in &["if", "then", "else"] {
        let ov = orig.get(kw);
        let uv = upd.get(kw);
        match (ov, uv) {
            (Some(ov), Some(uv)) => {
                let oc = canonical_value(ov);
                let uc = canonical_value(uv);
                if oc != uc {
                    out.push(d(Kind::ConditionalChanged, &format!("{path}/{kw}")));
                    // Also recurse to surface detailed diffs inside the branch
                    let ov = ov.clone();
                    let uv = uv.clone();
                    compare_schema(&format!("{path}/{kw}"), &ov, &uv, ctx, out);
                }
            }
            (None, Some(_)) | (Some(_), None) => {
                out.push(d(Kind::ConditionalChanged, &format!("{path}/{kw}")));
            }
            (None, None) => {}
        }
    }
}
