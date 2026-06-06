# Crabka Schema Registry — Slice 2c (JSON Schema compatibility, full parity) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fill `format::json::check(reader, writer)` with the full Confluent JSON Schema compatibility rule set — a structural diff over two parsed schema documents (`serde_json::Value`), each difference classified backward-compatible-or-not — calibrated to match `cp-schema-registry 7.4.0` exactly.

**Architecture:** `check(reader, writer)` parses both schemas, computes `diff::compare(original=writer, update=reader) -> Vec<Difference{kind, path}>`, and returns `Err` if any `kind` is not backward-compatible. The engine supplies direction by argument order (BACKWARD=`check(new, old)`, FORWARD=`check(old, new)`), so the rules carry **no** per-direction logic. The classification table is **seeded then calibrated against a cp golden matrix — cp wins.** Identical to slice 2b (which matched cp 88/88) but over JSON `Value`s.

**Tech Stack:** Rust 2024, `serde_json` (already a dep), the slice-2 compat engine/seam (unchanged). Tests: per-rule `check()` unit tests, golden cp JSON matrix (`testcontainers` + `cp-schema-registry:7.4.0`, `#[ignore]` Docker), no-Docker `compat_conformance`.

---

## Design reference

Spec: `docs/superpowers/specs/2026-06-05-crabka-schema-registry-slice-2c-json-schema-compat-design.md`. Read it.

### Verified existing signatures
```rust
// existing format/json.rs (becomes json/mod.rs):
//   pub struct JsonSchema(serde_json::Value);              // private field .0
//   pub fn parse(schema: &str) -> Result<JsonSchema, SrError>   // serde_json + must be object|boolean
//   pub fn check(_reader, _writer) -> Result<(), Vec<String>>   // CURRENTLY permissive Ok(()) — replace
//   impl ParsedSchema for JsonSchema { fn canonical_form() }     // key-sorted canonicalize
//   #[cfg(test)] has `check_is_permissive_for_now` (will break — UPDATE/remove it), parses_object_and_dedups_key_order, rejects_non_json
// engine: format::check(SchemaType::Json, reader, writer) already routes to json::check.
//   compat::check_against_version(.., SchemaType::Json, ..) used by tests/conformance.
```

### Branch / commit / gate discipline (executors read this)
- Worktree: `/Users/mattstone/git/crabka/.claude/worktrees/musing-cartwright-7af144`. Branch: `claude/schema-registry-slice-2c` (assert NOT main). Always `git -C <worktree>`. Do NOT push (controller handles push/PR; stacks on 2b PR #397).
- Commits: `git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`; end body with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- **Per change before commit:** `cargo clippy -p crabka-schema-registry --all-targets -- -D warnings` + `cargo fmt -p crabka-schema-registry`. `git add` only the task's files.

---

## File structure
```
crates/schema-registry/src/format/json/        # was json.rs (git mv)
  mod.rs    # existing parse + canonical_form + JsonSchema + a `value()` accessor; `check` delegates
  diff.rs   # Difference{kind,path}, Kind enum, compare(original,update), content-model + $ref helpers
  compat.rs # Kind::is_backward_compatible() table + messages
crates/schema-registry/tests/
  compat_conformance.rs        # extend: also iterate json_matrix.json
  capture_json_fixtures.rs     # NEW #[ignore] Docker capture -> json_matrix.json
  integration.rs               # + a JSON-Schema enforcement test
  fixtures/compat/json_matrix.json   # NEW golden verdicts
```

## Execution batches (sequential)
- **Task 1** — module split + diff scaffold + type + properties(open/closed) + required + additionalProperties.
- **Task 2** — enum + numeric + string + array + object-size constraints.
- **Task 3** — combinators (allOf/anyOf/oneOf/not).
- **Task 4** — `$ref` (intra-document, cycle-guarded) + dependencies + if/then/else.
- **Task 5** — capture the cp JSON matrix (Docker).
- **Task 6** — conformance calibration (cp authority) + enforcement integration test.

---

## Task 1: module split + diff scaffold + type/properties/required/additionalProperties

**Files:** `git mv` `src/format/json.rs` → `src/format/json/mod.rs`; create `src/format/json/diff.rs`, `src/format/json/compat.rs`.

- [ ] **Step 1: move the file.**
```bash
WT=/Users/mattstone/git/crabka/.claude/worktrees/musing-cartwright-7af144
git -C "$WT" mv crates/schema-registry/src/format/json.rs crates/schema-registry/src/format/json/mod.rs
```

- [ ] **Step 2: in `mod.rs`** add `mod compat; mod diff;`, a `value()` accessor, and delegate `check`:
```rust
impl JsonSchema {
    pub(crate) fn value(&self) -> &serde_json::Value { &self.0 }
}

/// Confluent JSON Schema compatibility: can a reader using `reader` read data
/// written with `writer`? Diffs (original = writer, update = reader); rejects if
/// any difference is backward-incompatible.
pub fn check(reader: &str, writer: &str) -> Result<(), Vec<String>> {
    let reader_s = parse(reader).map_err(|e| vec![format!("reader: {e}")])?;
    let writer_s = parse(writer).map_err(|e| vec![format!("writer: {e}")])?;
    let diffs = diff::compare(writer_s.value(), reader_s.value());
    let incompatible: Vec<&diff::Difference> =
        diffs.iter().filter(|d| !compat::is_backward_compatible(&d.kind)).collect();
    if incompatible.is_empty() { Ok(()) } else { Err(compat::messages(&incompatible)) }
}
```
DELETE the now-stale `check_is_permissive_for_now` test.

- [ ] **Step 3: failing tests** (add to `mod.rs` `#[cfg(test)] mod tests`):
```rust
    #[test] fn add_optional_property_open_model_is_compatible() {
        // open content model (no additionalProperties:false); adding an optional property.
        let w = r#"{"type":"object","properties":{"a":{"type":"integer"}}}"#;
        let r = r#"{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"string"}}}"#;
        assert!(check(r, w).is_ok());
    }
    #[test] fn add_required_property_closed_model_is_incompatible() {
        let w = r#"{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"}}}"#;
        let r = r#"{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"},"b":{"type":"string"}},"required":["b"]}"#;
        assert!(check(r, w).is_err());
    }
    #[test] fn type_narrowed_is_incompatible() {
        // ["string","null"] -> ["string"] : the reader rejects nulls the writer could produce.
        let w = r#"{"type":["string","null"]}"#;
        let r = r#"{"type":"string"}"#;
        assert!(check(r, w).is_err());
    }
    #[test] fn required_added_is_incompatible() {
        let w = r#"{"type":"object","properties":{"a":{"type":"integer"}}}"#;
        let r = r#"{"type":"object","properties":{"a":{"type":"integer"}},"required":["a"]}"#;
        assert!(check(r, w).is_err());
    }
```
Run `cargo test -p crabka-schema-registry --lib format::json` → fails (diff/compat missing).

- [ ] **Step 4: implement `diff.rs`** (framework + batch-1 detection):
```rust
//! Structural diff between two JSON Schema documents (serde_json::Value),
//! mirroring Confluent's json.diff. Classified by `compat.rs`. No direction
//! logic — the engine swaps (reader, writer) per level.

use std::collections::BTreeSet;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    TypeNarrowed, TypeExtended, TypeChanged,
    PropertyAddedToOpenContentModel, PropertyRemovedFromOpenContentModel,
    PropertyAddedToClosedContentModel, PropertyRemovedFromClosedContentModel,
    RequiredAttributeAdded, RequiredAttributeRemoved,
    AdditionalPropertiesRemoved, AdditionalPropertiesAdded,
    // batches 2-4 extend this enum.
}

#[derive(Debug, Clone)]
pub struct Difference { pub kind: Kind, pub path: String }

fn d(kind: Kind, path: &str) -> Difference { Difference { kind, path: path.to_string() } }

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
    // batches 2-4 add: enum/const, numeric, string, array, object-size,
    // combinators, $ref, dependencies, conditionals.
}

/// The declared `type`s as a set; empty == "any type".
fn types_of(schema: &Value) -> BTreeSet<String> {
    match schema.get("type") {
        Some(Value::String(s)) => BTreeSet::from([s.clone()]),
        Some(Value::Array(a)) => a.iter().filter_map(|v| v.as_str().map(String::from)).collect(),
        _ => BTreeSet::new(),
    }
}

fn compare_type(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    let (ot, ut) = (types_of(orig), types_of(upd));
    if ot == ut { return; }
    // empty set = any; an update narrowing from any to specific is a narrow.
    if ot.is_empty() && !ut.is_empty() { out.push(d(Kind::TypeNarrowed, path)); }
    else if ut.is_empty() && !ot.is_empty() { out.push(d(Kind::TypeExtended, path)); }
    else if ut.is_subset(&ot) { out.push(d(Kind::TypeNarrowed, path)); }
    else if ot.is_subset(&ut) { out.push(d(Kind::TypeExtended, path)); }
    else { out.push(d(Kind::TypeChanged, path)); }
}

/// A closed content model forbids extra properties (`additionalProperties:false`).
fn is_closed(schema: &Value) -> bool {
    matches!(schema.get("additionalProperties"), Some(Value::Bool(false)))
}

fn props(schema: &Value) -> Option<&serde_json::Map<String, Value>> {
    schema.get("properties").and_then(Value::as_object)
}

fn required_set(schema: &Value) -> BTreeSet<String> {
    schema.get("required").and_then(Value::as_array)
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default()
}

fn compare_properties(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    let (op, up) = (props(orig), props(upd));
    let closed = is_closed(upd) || is_closed(orig);
    let empty = serde_json::Map::new();
    let (op, up) = (op.unwrap_or(&empty), up.unwrap_or(&empty));
    for name in op.keys() {
        if !up.contains_key(name) {
            out.push(d(if closed { Kind::PropertyRemovedFromClosedContentModel } else { Kind::PropertyRemovedFromOpenContentModel }, &format!("{path}/properties/{name}")));
        }
    }
    for (name, uschema) in up {
        match op.get(name) {
            None => out.push(d(if closed { Kind::PropertyAddedToClosedContentModel } else { Kind::PropertyAddedToOpenContentModel }, &format!("{path}/properties/{name}"))),
            Some(oschema) => compare_schema(&format!("{path}/properties/{name}"), oschema, uschema, out),
        }
    }
}

fn compare_required(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    let (orq, urq) = (required_set(orig), required_set(upd));
    for name in urq.difference(&orq) { out.push(d(Kind::RequiredAttributeAdded, &format!("{path}/required/{name}"))); }
    for name in orq.difference(&urq) { out.push(d(Kind::RequiredAttributeRemoved, &format!("{path}/required/{name}"))); }
}

fn compare_additional_properties(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    let (oa, ua) = (orig.get("additionalProperties"), upd.get("additionalProperties"));
    match (oa, ua) {
        // false (closed) -> not-false (open): additionalProperties allowed now.
        (Some(Value::Bool(false)), o) if !matches!(o, Some(Value::Bool(false))) => out.push(d(Kind::AdditionalPropertiesAdded, path)),
        (o, Some(Value::Bool(false))) if !matches!(o, Some(Value::Bool(false))) => out.push(d(Kind::AdditionalPropertiesRemoved, path)),
        _ => {}
    }
}
```

- [ ] **Step 5: implement `compat.rs`** (seed table; Task 6 calibrates):
```rust
//! Backward-compatibility classification for JSON Schema differences. SEED from
//! Confluent's behavior; the cp golden matrix (compat_conformance) is the
//! authority and re-tunes this table.

use super::diff::{Difference, Kind};

#[must_use]
pub fn is_backward_compatible(kind: &Kind) -> bool {
    match kind {
        // additive / widening = compatible; narrowing / new constraints = not.
        Kind::TypeExtended => true,
        Kind::TypeNarrowed | Kind::TypeChanged => false,
        Kind::PropertyAddedToOpenContentModel => false,
        Kind::PropertyRemovedFromOpenContentModel => true,
        Kind::PropertyAddedToClosedContentModel => true,
        Kind::PropertyRemovedFromClosedContentModel => false,
        Kind::RequiredAttributeAdded => false,
        Kind::RequiredAttributeRemoved => true,
        Kind::AdditionalPropertiesAdded => true,
        Kind::AdditionalPropertiesRemoved => false,
    }
}

#[must_use]
pub fn messages(diffs: &[&Difference]) -> Vec<String> {
    diffs.iter().map(|d| format!("{:?} at {}", d.kind, d.path)).collect()
}
```

- [ ] **Step 6: run** `cargo test -p crabka-schema-registry --lib format::json` → 4 batch-1 tests + the retained parses/rejects tests pass. `cargo build` clean.
- [ ] **Step 7: clippy + fmt + commit** the whole `crates/schema-registry/src/format/json/` dir, message:
`schema-registry: json schema diff framework + type/properties/required rules`

> SEED verdicts are best-effort; Task 6 calibrates them against cp. If a batch-1 unit test's expected verdict later conflicts with cp, cp wins — update both.

---

## Task 2: enum + numeric + string + array + object-size constraints

**Files:** `diff.rs`, `compat.rs`, `mod.rs` (tests).

- [ ] **Step 1: unit tests** (add to `mod.rs` tests):
```rust
    fn s(body: &str) -> String { format!("{{{body}}}") }
    #[test] fn enum_extended_vs_narrowed() {
        let narrow = r#"{"enum":["a"]}"#; let wide = r#"{"enum":["a","b"]}"#;
        let _ = check(wide, narrow); // verdict cp-calibrated; must not panic
        let _ = check(narrow, wide);
    }
    #[test] fn maximum_lowered_is_incompatible() {
        let w = r#"{"type":"integer","maximum":100}"#; let r = r#"{"type":"integer","maximum":10}"#;
        assert!(check(r, w).is_err(), "tightening maximum rejects values the writer allowed");
    }
    #[test] fn min_length_added_is_incompatible() {
        let w = r#"{"type":"string"}"#; let r = r#"{"type":"string","minLength":3}"#;
        assert!(check(r, w).is_err());
    }
    #[test] fn max_items_raised_is_compatible() {
        let w = r#"{"type":"array","maxItems":3}"#; let r = r#"{"type":"array","maxItems":9}"#;
        let _ = check(r, w); // cp-calibrated
    }
```

- [ ] **Step 2: extend `diff.rs`** `compare_schema` with constraint comparisons + `Kind` variants:
  - **enum/const:** `EnumArrayExtended` (update superset), `EnumArrayNarrowed` (update subset), `EnumArrayChanged` (neither). Compare the `enum` arrays as sets of canonicalized values.
  - **numeric:** for each of `maximum`/`exclusiveMaximum`/`minimum`/`exclusiveMinimum`/`multipleOf`: added/removed/tightened/loosened. Tightened = a new or stricter bound (max lowered, min raised, multipleOf added/changed). Kinds: `MaximumAdded/Removed/Increased/Decreased`, `MinimumAdded/Removed/Increased/Decreased`, `ExclusiveMaximum*`, `ExclusiveMinimum*`, `MultipleOfAdded/Removed/Expanded/Reduced`.
  - **string:** `MaxLengthAdded/Removed/Increased/Decreased`, `MinLengthAdded/Removed/Increased/Decreased`, `PatternAdded/Removed/Changed`.
  - **array:** `items` schema change (recurse via `compare_schema` on the `items` subschema if both are schemas; tuple `items` arrays compared element-wise), `MaxItemsAdded/Removed/Increased/Decreased`, `MinItemsAdded/Removed/Increased/Decreased`, `additionalItems` add/remove.
  - **object size:** `MaxPropertiesAdded/Removed/Increased/Decreased`, `MinProperties*`.
  Add a numeric helper `fn num(schema, key) -> Option<f64>`.

- [ ] **Step 3: classify** (seed) in `compat.rs`: a tightening (added bound, max decreased, min increased, minLength/minItems/minProperties increased or added, maxLength/maxItems/maxProperties decreased or added, pattern added/changed, multipleOf added/changed, enum narrowed) => **false**; a loosening (removed bound, bound relaxed, enum extended) => **true**. Add an arm for EVERY new Kind (non-exhaustive match won't compile).

- [ ] **Step 4: run** `cargo test -p crabka-schema-registry --lib format::json` → pass. **Step 5: clippy + fmt + commit**, message `schema-registry: json schema enum/numeric/string/array constraint rules`.

---

## Task 3: combinators (allOf/anyOf/oneOf/not)

**Files:** `diff.rs`, `compat.rs`, `mod.rs` (tests).

- [ ] **Step 1: unit tests:**
```rust
    #[test] fn anyof_subschema_added_does_not_panic() {
        let w = r#"{"anyOf":[{"type":"string"}]}"#;
        let r = r#"{"anyOf":[{"type":"string"},{"type":"integer"}]}"#;
        let _ = check(r, w); // cp-calibrated (sum-type widening)
    }
    #[test] fn allof_subschema_added_does_not_panic() {
        let w = r#"{"allOf":[{"type":"object"}]}"#;
        let r = r#"{"allOf":[{"type":"object"},{"required":["a"]}]}"#;
        let _ = check(r, w); // product-type narrowing
    }
```

- [ ] **Step 2: extend `diff.rs`:** add `Kind::{CombinedTypeChanged, ProductTypeExtended, ProductTypeNarrowed, SumTypeExtended, SumTypeNarrowed, CombinedTypeSubschemasChanged, NotTypeExtended, NotTypeNarrowed}`. In `compare_schema`, detect which combinator keyword each side uses (`allOf`/`anyOf`/`oneOf`/`not`); if the combinator keyword changed → `CombinedTypeChanged`. For the same combinator: compare the subschema **arrays** as sets by structural (canonicalized) equality — subschemas added/removed → `Product*`/`Sum*` extended/narrowed (`allOf` = product: more subschemas = narrower; `anyOf`/`oneOf` = sum: more subschemas = wider). For `not`: recurse / compare the negated subschema.

- [ ] **Step 3: classify** (seed): `ProductTypeNarrowed => false, ProductTypeExtended => true, SumTypeExtended => true, SumTypeNarrowed => false, CombinedTypeChanged => false, CombinedTypeSubschemasChanged => false, NotTypeNarrowed => false, NotTypeExtended => true`.

- [ ] **Step 4: run** → pass. **Step 5: clippy + fmt + commit**, message `schema-registry: json schema combinator (allOf/anyOf/oneOf/not) rules`.

---

## Task 4: $ref + dependencies + conditionals

**Files:** `diff.rs`, `compat.rs`, `mod.rs` (tests).

- [ ] **Step 1: unit tests:**
```rust
    #[test] fn ref_resolves_and_diffs_target() {
        let w = r#"{"$ref":"#/$defs/T","$defs":{"T":{"type":"integer"}}}"#;
        let r = r#"{"$ref":"#/$defs/T","$defs":{"T":{"type":"string"}}}"#;
        assert!(check(r, w).is_err(), "resolved target type changed");
    }
    #[test] fn recursive_ref_terminates() {
        let s = r#"{"$ref":"#/$defs/N","$defs":{"N":{"type":"object","properties":{"next":{"$ref":"#/$defs/N"}}}}}"#;
        assert!(check(s, s).is_ok(), "self-referential schema must terminate and be compatible with itself");
    }
    #[test] fn dependencies_and_conditionals_do_not_panic() {
        let _ = check(r#"{"if":{"required":["a"]},"then":{"required":["b"]}}"#, r#"{"type":"object"}"#);
    }
```

- [ ] **Step 2: extend `diff.rs`:** add a `$ref` resolver `fn resolve<'a>(root: &'a Value, schema: &'a Value, seen: &mut BTreeSet<String>) -> &'a Value` that, when `schema` has a `$ref` string starting `#/`, follows the intra-document JSON Pointer against the owning root document; a **visited-set** keyed on the pointer string prevents infinite recursion (on a repeat, stop descending and treat as equal). Thread the two root documents through `compare` (store them at the top of `compare`; pass to `compare_schema`). Resolve both sides before comparing. Cross-document/remote `$ref` (not starting `#`) → treat permissively (no difference; documented). Add `Kind::{DependencyAdded, DependencyRemoved, ConditionalChanged}` and compare `dependencies`/`dependentRequired`/`dependentSchemas` (added/removed) + `if`/`then`/`else` (recurse).

- [ ] **Step 3: classify** (seed): `DependencyAdded => false, DependencyRemoved => true, ConditionalChanged => false`.

- [ ] **Step 4: run** → pass (esp. `recursive_ref_terminates` must not stack-overflow). **Step 5: clippy + fmt + commit**, message `schema-registry: json schema $ref resolution + dependencies + conditionals`.

---

## Task 5: capture the cp JSON Schema golden matrix (Docker)

**Files:** Create `crates/schema-registry/tests/capture_json_fixtures.rs`; output `tests/fixtures/compat/json_matrix.json`.

- [ ] **Step 1: write the `#[ignore]` harness** modeled on `tests/capture_protobuf_fixtures.rs` (READ it — copy the broker + `cp-schema-registry:7.4.0` setup verbatim). For each `(case, writer, reader)` below × each level `[BACKWARD, FORWARD, FULL]`: `PUT /config/{case}-{level}` the level; `POST /subjects/{case}-{level}/versions` `{"schema": writer, "schemaType":"JSON"}`; `POST /compatibility/subjects/{case}-{level}/versions/latest` `{"schema": reader, "schemaType":"JSON"}` → record `is_compatible`. Write to `tests/fixtures/compat/json_matrix.json` (array of `{case, level, writer, reader, is_compatible}`). Build bodies with serde_json so the schema string is escaped.

Cases (each writer/reader a JSON Schema string; ≈50 pairs — generate the obvious reverses where noted):
```
add_prop_open          W:{type:object,properties:{a:int}}                    R:{...,b:string}
add_prop_closed        W:{type:object,additionalProperties:false,props:{a}}  R:{...,b}
remove_prop_open       (reverse of add_prop_open)
remove_prop_closed     (reverse of add_prop_closed)
required_added         W:{props:{a}}                                         R:{props:{a},required:[a]}
required_removed       (reverse)
addl_props_false_to_true   W:{additionalProperties:false}                    R:{additionalProperties:true}
addl_props_true_to_false   (reverse)
type_widen             W:{type:string}                                       R:{type:[string,null]}
type_narrow            (reverse)
type_changed           W:{type:string}                                       R:{type:integer}
enum_extended          W:{enum:[a]}                                          R:{enum:[a,b]}
enum_narrowed          (reverse)
maximum_added          W:{type:integer}                                      R:{type:integer,maximum:10}
maximum_removed        (reverse)
maximum_increased      W:{type:integer,maximum:10}                           R:{type:integer,maximum:100}
maximum_decreased      (reverse)
minimum_added/.../...  (analogous to maximum)
exclusive_max_added    W:{type:integer}                                      R:{type:integer,exclusiveMaximum:10}
multiple_of_added      W:{type:integer}                                      R:{type:integer,multipleOf:5}
min_length_added       W:{type:string}                                       R:{type:string,minLength:3}
max_length_added       W:{type:string}                                       R:{type:string,maxLength:9}
pattern_added          W:{type:string}                                       R:{type:string,pattern:"^x"}
min_items_added        W:{type:array}                                        R:{type:array,minItems:1}
max_items_added        W:{type:array}                                        R:{type:array,maxItems:5}
items_type_change      W:{type:array,items:{type:integer}}                   R:{type:array,items:{type:string}}
min_properties_added   W:{type:object}                                       R:{type:object,minProperties:1}
anyof_subschema_added  W:{anyOf:[{type:string}]}                             R:{anyOf:[{type:string},{type:integer}]}
anyof_subschema_removed (reverse)
allof_subschema_added  W:{allOf:[{type:object}]}                             R:{allOf:[{type:object},{required:[a]}]}
oneof_subschema_added  W:{oneOf:[{type:string}]}                             R:{oneOf:[{type:string},{type:integer}]}
not_added              W:{type:object}                                       R:{not:{type:string}}
ref_target_type_change W:{$ref:#/$defs/T,$defs:{T:{type:integer}}}           R:{$ref:#/$defs/T,$defs:{T:{type:string}}}
dependency_added       W:{type:object}                                       R:{type:object,dependentRequired:{a:[b]}}
if_then_added          W:{type:object}                                       R:{type:object,if:{required:[a]},then:{required:[b]}}
```
(That's ~35 cases incl. reverses → expand the numeric/min families to reach ~50. If cp rejects a schema at registration, log + skip + report.)

- [ ] **Step 2: run** (Docker): `cargo test -p crabka-schema-registry --test capture_json_fixtures -- --ignored --nocapture`. Confirm `json_matrix.json` is written (~140 entries / ~47×3). 
- [ ] **Step 3: inspect** + report ~15 representative verdicts (esp. add_prop_open & add_prop_closed all levels, required_added, type_widen/type_narrow, maximum_added/removed, enum_extended/narrowed, anyof_subschema_added, allof_subschema_added) — cp ground truth for Task 6.
- [ ] **Step 4: clippy + fmt + commit** (`capture_json_fixtures.rs` + `json_matrix.json` [+ Cargo.toml stanza]), message `schema-registry: golden JSON Schema compatibility verdicts from cp-schema-registry 7.4.0`.

> If Docker unavailable, STOP and report — the controller runs the capture. Do NOT fabricate verdicts.

---

## Task 6: conformance calibration + enforcement

**Files:** `tests/compat_conformance.rs`, `tests/integration.rs`; re-tune `src/format/json/{compat,diff}.rs` until the matrix passes.

- [ ] **Step 1: extend `compat_conformance.rs`** with `engine_matches_cp_json_verdicts`, mirroring the Avro/Protobuf tests but loading `json_matrix.json` and using `SchemaType::Json`. Empty `known_divergences` to start.
```rust
#[test]
fn engine_matches_cp_json_verdicts() {
    let known_divergences: std::collections::HashMap<(&str, &str), bool> = std::collections::HashMap::from([]);
    let mut mismatches = Vec::new();
    for c in load_matrix("json_matrix.json") {
        let mut snap = crabka_schema_registry::store::StoreState::default();
        snap.set_subject_compat("s", c.level.clone());
        snap.register("s", crabka_schema_registry::format::SchemaType::Json, &c.writer).expect("writer registers");
        let got = crabka_schema_registry::compat::check_against_version(&snap, "s", crabka_schema_registry::format::SchemaType::Json, &c.reader, None).unwrap().is_compatible;
        let expected = *known_divergences.get(&(c.case.as_str(), c.level.as_str())).unwrap_or(&c.is_compatible);
        if got != expected { mismatches.push(format!("{}/{}: ours={got} cp={}", c.case, c.level, c.is_compatible)); }
    }
    assert!(mismatches.is_empty(), "json engine diverges from cp on:\n{}", mismatches.join("\n"));
}
```
(Reuse the existing `Case`/`load_matrix` helper used by the protobuf conformance test — all three matrix files share the shape.)

- [ ] **Step 2: CALIBRATE.** Run `cargo test -p crabka-schema-registry --test compat_conformance -- --nocapture`. For each mismatch, fix `compat.rs` (table) or `diff.rs` (if a difference isn't detected). Re-run until ALL json cases pass (Avro 21 + Protobuf 88 must stay green). Update any `mod.rs` unit tests cp contradicts (with a one-line note). Genuine cp quirks → `known_divergences` + `tests/fixtures/compat/README.md`, but PREFER matching. **Report every table/diff change + the final divergence list.**

- [ ] **Step 3: JSON enforcement integration test** in `tests/integration.rs`:
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_schema_compat_enforced_on_register() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    let v1 = r#"{"schemaType":"JSON","schema":"{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"integer\"}}}"}"#;
    assert_eq!(app.clone().oneshot(req_post("/subjects/js/versions", v1)).await.unwrap().status(), StatusCode::OK);
    // add a required property -> incompatible under default BACKWARD
    let bad = r#"{"schemaType":"JSON","schema":"{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"integer\"}},\"required\":[\"a\"]}"}"#;
    let r = app.clone().oneshot(req_post("/subjects/js/versions", bad)).await.unwrap();
    assert_eq!(r.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(r).await["error_code"], 409);
    // add an optional property (open model) -> compatible
    let good = r#"{"schemaType":"JSON","schema":"{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"integer\"},\"b\":{\"type\":\"string\"}}}"}"#;
    assert_eq!(app.clone().oneshot(req_post("/subjects/js/versions", good)).await.unwrap().status(), StatusCode::OK);
    cancel.cancel();
    broker.shutdown().await;
}
```
(If cp's calibration makes "add a required property" compatible/incompatible differently than assumed, adjust the bad/good schemas so the test asserts a real 409 + a real 200 consistent with the calibrated engine.)

- [ ] **Step 4: run** `cargo test -p crabka-schema-registry --test compat_conformance --test integration --lib format::json -- --nocapture` → all green. clippy + fmt.
- [ ] **Step 5: commit** (`compat_conformance.rs`, `integration.rs`, `src/format/json/{compat,diff,mod}.rs`, `tests/fixtures/compat/README.md` if written), message `schema-registry: json schema compatibility conformance (cp-calibrated) + enforcement`.

---

## Self-review (completed by plan author)

**Spec coverage:** diff-based `check` reusing engine direction → Task 1; full catalog → Tasks 1–4 (type/properties/required/additionalProperties; enum/numeric/string/array/object-size; combinators; $ref/dependencies/conditionals); open/closed content model → Task 1 `is_closed`/`compare_properties`; cp matrix + calibration → Tasks 5–6; per-rule unit tests → Tasks 1–4; enforcement → Task 6; module split → Task 1; out-of-scope (remote $ref permissive, non-draft-07, all-versions endpoint) → absent.

**Placeholder scan:** seed classification verdicts are explicitly seed-then-calibrate (Task 6 authority) — the spec's design, not unfilled placeholders. Unit tests with `let _ = check(...)` (enum/combinator/conditional) assert "produces a verdict without panic" because the verdict is cp-calibrated in Task 6; discriminating assertions live in the matrix.

**Type consistency:** `diff::{Difference, Kind, compare}`, `compat::{is_backward_compatible, messages}`, `JsonSchema::value()`, `format::check(Json, reader, writer)`, `compat::check_against_version(.., SchemaType::Json, ..)`, and helpers `types_of`/`is_closed`/`props`/`required_set`/`num`/`resolve` are referenced consistently. `Kind` grows additively across Tasks 1–4; `is_backward_compatible`'s exhaustive match forces an arm per variant.
