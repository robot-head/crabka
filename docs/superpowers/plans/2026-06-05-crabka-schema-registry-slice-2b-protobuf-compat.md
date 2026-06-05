# Crabka Schema Registry — Slice 2b (Protobuf compatibility, full parity) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fill `format::protobuf::check(reader, writer)` with the full Confluent Protobuf compatibility rule set — a structural diff over two `FileDescriptorProto`s, each difference classified backward-compatible-or-not — so incompatible Protobuf evolutions are rejected (409) and `/compatibility` returns true verdicts, calibrated to match `cp-schema-registry 7.4.0` exactly.

**Architecture:** `check(reader, writer)` parses both `.proto`, computes `diff::compare(original=writer, update=reader) -> Vec<Difference{kind, path}>`, and returns `Err(messages)` if any `kind` is not backward-compatible. The engine already supplies direction by argument order (BACKWARD = `check(new, old)`, FORWARD = `check(old, new)`), so the rules carry **no** per-direction logic. The classification table is **seeded from Confluent's behavior, then calibrated against a golden cp matrix — cp wins.**

**Tech Stack:** Rust 2024, `protox-parse` + `prost_reflect::prost_types` (already deps), the slice-2 compat engine/seam (unchanged). Tests: per-rule `check()` unit tests, golden cp Protobuf matrix (`testcontainers` + `cp-schema-registry:7.4.0`, `#[ignore]` Docker), no-Docker `compat_conformance`.

---

## Design reference

Spec: `docs/superpowers/specs/2026-06-05-crabka-schema-registry-slice-2b-protobuf-compat-design.md`. Read it.

### Verified existing + upstream signatures

```rust
// existing format/protobuf.rs (becomes protobuf/mod.rs):
//   pub struct ProtobufSchema { descriptor: FileDescriptorProto, normalised: String }   // fields private
//   pub fn parse(schema: &str) -> Result<ProtobufSchema, SrError>     // via protox_parse::parse("schema.proto", schema)
//   pub fn normalize(fdp: &FileDescriptorProto) -> String            // pretty-printer
//   impl ProtobufSchema { pub fn normalized_form(&self) -> &str }
//   impl ParsedSchema for ProtobufSchema { fn canonical_form(&self) -> String }
//   pub fn check(_reader: &str, _writer: &str) -> Result<(), Vec<String>>   // CURRENTLY permissive Ok(()) — replace
// imports already present: prost_reflect::prost::Message; prost_reflect::prost_types::field_descriptor_proto::{Label, Type as FieldType}; prost_reflect::prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto}

// prost_types descriptor model (all via prost_reflect::prost_types):
//   FileDescriptorProto { package: Option<String>, message_type: Vec<DescriptorProto>, enum_type: Vec<EnumDescriptorProto>, syntax: Option<String>, .. }
//   DescriptorProto { name: Option<String>, field: Vec<FieldDescriptorProto>, nested_type: Vec<DescriptorProto>, enum_type: Vec<EnumDescriptorProto>, oneof_decl: Vec<OneofDescriptorProto>, options: Option<MessageOptions>, reserved_range: Vec<descriptor_proto::ReservedRange>, reserved_name: Vec<String> }
//   descriptor_proto::ReservedRange { start: Option<i32>, end: Option<i32> }   // [start, end)
//   FieldDescriptorProto { name: Option<String>, number: Option<i32>, type_name: Option<String>, oneof_index: Option<i32>, proto3_optional: Option<bool>, .. }
//     accessors: field.label() -> Label, field.r#type() -> FieldType
//   field_descriptor_proto::Label { Optional=1, Required=2, Repeated=3 }
//   field_descriptor_proto::Type { Double, Float, Int64, Uint64, Int32, Fixed64, Fixed32, Bool, String, Group, Message, Bytes, Uint32, Enum, Sfixed32, Sfixed64, Sint32, Sint64 }
//   EnumDescriptorProto { name: Option<String>, value: Vec<EnumValueDescriptorProto>, .. }
//   EnumValueDescriptorProto { name: Option<String>, number: Option<i32> }
//   OneofDescriptorProto { name: Option<String> }
//   MessageOptions { map_entry: Option<bool>, .. }   // map_entry == Some(true) for synthetic map<> entries
// IMPORTANT proto3 wrinkle: a `optional` proto3 field gets a SYNTHETIC oneof (proto3_optional == Some(true), oneof_index set). Treat such fields as SINGULAR, NOT oneof members.

// slice-2 seam (unchanged): format::check(SchemaType::Protobuf, reader, writer) already routes to protobuf::check.
// engine calls check(new, old) for BACKWARD and check(old, new) for FORWARD.
```

### Commit & gate discipline (executors read this)

- Worktree: `/Users/mattstone/git/crabka/.claude/worktrees/musing-cartwright-7af144`. Branch: the slice-2b branch the controller is on (assert NOT `main`). Always `git -C <worktree>`. Do NOT push (controller handles push/PR; slice 2b stacks on slice-2 PR #395).
- Commits: `git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`; end body with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- **Per change before commit:** `cargo clippy -p crabka-schema-registry --all-targets -- -D warnings` + `cargo fmt -p crabka-schema-registry`. `git add` only the task's files.

---

## File structure

```
crates/schema-registry/src/format/protobuf/        # was protobuf.rs (git mv)
  mod.rs    # existing parse + normalize + ProtobufSchema + canonical_form; `check` delegates to diff+compat
  diff.rs   # Difference{kind:Kind, path:String}, Kind enum, compare(original,update) -> Vec<Difference>
  compat.rs # Kind::is_backward_compatible() table + messages_from(diffs)
crates/schema-registry/tests/
  compat_conformance.rs        # extend: also iterate protobuf_matrix.json
  capture_compat_fixtures.rs   # extend OR new capture_protobuf_fixtures.rs (#[ignore] Docker) -> protobuf_matrix.json
  integration.rs               # + a Protobuf enforcement test
  fixtures/compat/protobuf_matrix.json   # NEW golden verdicts
```

No other modules change (`format/mod.rs`'s `pub mod protobuf;` and the `SchemaType::Protobuf => protobuf::check(...)` arm resolve to the directory unchanged).

---

## Execution batches (run tasks in order; sequential)

- **Task 1** — module split + diff framework + field/scalar/kind/label rules.
- **Task 2** — oneof rules.
- **Task 3** — reserved + map rules.
- **Task 4** — enum + message + nested + package rules.
- **Task 5** — capture the cp Protobuf matrix (Docker).
- **Task 6** — conformance calibration (cp authority) + enforcement integration test.

---

## Task 1: module split + diff framework + field/scalar/kind/label rules

**Files:** `git mv` `src/format/protobuf.rs` → `src/format/protobuf/mod.rs`; create `src/format/protobuf/diff.rs`, `src/format/protobuf/compat.rs`.

- [ ] **Step 1: Move the file.**
```bash
WT=/Users/mattstone/git/crabka/.claude/worktrees/musing-cartwright-7af144
git -C "$WT" mv crates/schema-registry/src/format/protobuf.rs crates/schema-registry/src/format/protobuf/mod.rs
```

- [ ] **Step 2: In `mod.rs`,** add submodule declarations near the top (after the doc comment / imports) and rewrite `check` to delegate:
```rust
mod compat;
mod diff;

// ... keep existing parse / normalize / ProtobufSchema / canonical_form unchanged ...

/// Confluent Protobuf compatibility: can a reader using `reader` read data
/// written with `writer`? Computes the structural diff (original = writer,
/// update = reader) and rejects if any difference is backward-incompatible.
pub fn check(reader: &str, writer: &str) -> Result<(), Vec<String>> {
    let reader_d = parse(reader).map_err(|e| vec![format!("reader: {e}")])?;
    let writer_d = parse(writer).map_err(|e| vec![format!("writer: {e}")])?;
    let diffs = diff::compare(writer_d.descriptor(), reader_d.descriptor());
    let incompatible: Vec<&diff::Difference> =
        diffs.iter().filter(|d| !compat::is_backward_compatible(&d.kind)).collect();
    if incompatible.is_empty() {
        Ok(())
    } else {
        Err(compat::messages(&incompatible))
    }
}
```
Add a `pub(crate) fn descriptor(&self) -> &FileDescriptorProto { &self.descriptor }` accessor to `impl ProtobufSchema` (the diff needs the descriptor; keep the field private).

- [ ] **Step 3: Write `diff.rs`** — the model + the file/message/field walk for batch-1 kinds. Write these failing tests first (they exercise the whole `check` pipeline, so put them in `mod.rs`'s `#[cfg(test)]` or in `diff.rs`; use `super::check`):
```rust
#[cfg(test)]
mod tests {
    use super::check;
    // helper: a proto3 message U with the given body lines
    fn p(body: &str) -> String { format!("syntax = \"proto3\"; message U {{ {body} }}") }

    #[test] fn field_added_is_backward_compatible() {
        assert!(check(&p("int32 id = 1; int32 x = 2;"), &p("int32 id = 1;")).is_ok());
    }
    #[test] fn field_removed_is_backward_compatible() {
        // proto3: removing a field is compatible (reader ignores / default)
        assert!(check(&p("int32 id = 1;"), &p("int32 id = 1; int32 x = 2;")).is_ok());
    }
    #[test] fn scalar_change_within_group_ok_across_group_bad() {
        // int32 -> int64 : same varint group -> compatible
        assert!(check(&p("int64 id = 1;"), &p("int32 id = 1;")).is_ok());
        // int32 -> string : different group -> incompatible
        assert!(check(&p("string id = 1;"), &p("int32 id = 1;")).is_err());
    }
    #[test] fn label_change_is_incompatible() {
        assert!(check(&p("repeated int32 id = 1;"), &p("int32 id = 1;")).is_err());
    }
    #[test] fn kind_change_scalar_to_message_is_incompatible() {
        let w = "syntax = \"proto3\"; message U { int32 id = 1; }";
        let r = "syntax = \"proto3\"; message M {} message U { M id = 1; }";
        assert!(check(r, w).is_err());
    }
}
```
Then implement `diff.rs`:
```rust
//! Structural diff between two FileDescriptorProto, mirroring Confluent's
//! SchemaDiff. Each Difference is classified by `compat.rs`. No direction logic
//! here — the engine calls `check` with (reader, writer) swapped per level.

use std::collections::BTreeMap;

use prost_reflect::prost_types::field_descriptor_proto::{Label, Type as FieldType};
use prost_reflect::prost_types::{DescriptorProto, EnumDescriptorProto, FileDescriptorProto};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    // batch 1
    FieldAdded,
    FieldRemoved,
    FieldScalarKindChanged { compatible_group: bool },
    FieldKindChanged,            // scalar<->message<->enum<->map<->group
    FieldNamedTypeChanged,       // message/enum identity changed
    FieldLabelChanged,
    // batch 2 (oneof) — added in Task 2
    // batch 3 (reserved/map) — added in Task 3
    // batch 4 (enum/message/package) — added in Task 4
    MessageRemoved,
    MessageAdded,
}

#[derive(Debug, Clone)]
pub struct Difference {
    pub kind: Kind,
    pub path: String,
}

/// Compare `original` (writer) to `update` (reader); collect differences.
#[must_use]
pub fn compare(original: &FileDescriptorProto, update: &FileDescriptorProto) -> Vec<Difference> {
    let mut out = Vec::new();
    // package, syntax: handled in Task 4.
    compare_messages("", &original.message_type, &update.message_type, &mut out);
    // enums at file level: Task 4.
    out
}

fn compare_messages(prefix: &str, orig: &[DescriptorProto], upd: &[DescriptorProto], out: &mut Vec<Difference>) {
    let orig_by: BTreeMap<&str, &DescriptorProto> = orig.iter().filter_map(|m| Some((m.name(), m))).collect();
    let upd_by: BTreeMap<&str, &DescriptorProto> = upd.iter().filter_map(|m| Some((m.name(), m))).collect();
    for (name, om) in &orig_by {
        let path = join(prefix, name);
        match upd_by.get(name) {
            None => out.push(Difference { kind: Kind::MessageRemoved, path }),
            Some(um) => compare_message(&path, om, um, out),
        }
    }
    for (name, _) in &upd_by {
        if !orig_by.contains_key(name) {
            out.push(Difference { kind: Kind::MessageAdded, path: join(prefix, name) });
        }
    }
}

fn compare_message(path: &str, orig: &DescriptorProto, upd: &DescriptorProto, out: &mut Vec<Difference>) {
    // Fields matched by NUMBER (the wire identity).
    let orig_f: BTreeMap<i32, &_> = orig.field.iter().map(|f| (f.number(), f)).collect();
    let upd_f: BTreeMap<i32, &_> = upd.field.iter().map(|f| (f.number(), f)).collect();
    for (num, of) in &orig_f {
        let fpath = format!("{path}.#{num}");
        match upd_f.get(num) {
            None => out.push(Difference { kind: Kind::FieldRemoved, path: fpath }),
            Some(uf) => compare_field(&fpath, of, uf, out),
        }
    }
    for (num, _) in &upd_f {
        if !orig_f.contains_key(num) {
            out.push(Difference { kind: Kind::FieldAdded, path: format!("{path}.#{num}") });
        }
    }
    // nested_type recursion + reserved + oneof: later tasks (call compare_messages on nested in Task 4).
}

fn compare_field(path: &str, of: &prost_reflect::prost_types::FieldDescriptorProto,
                 uf: &prost_reflect::prost_types::FieldDescriptorProto, out: &mut Vec<Difference>) {
    if of.label() != uf.label() {
        out.push(Difference { kind: Kind::FieldLabelChanged, path: path.to_string() });
    }
    let (ot, ut) = (of.r#type(), uf.r#type());
    if ot != ut {
        // kind change vs scalar-kind change
        if is_scalar(ot) && is_scalar(ut) {
            out.push(Difference { kind: Kind::FieldScalarKindChanged { compatible_group: same_group(ot, ut) }, path: path.to_string() });
        } else {
            out.push(Difference { kind: Kind::FieldKindChanged, path: path.to_string() });
        }
    } else if matches!(ot, FieldType::Message | FieldType::Enum) && of.type_name != uf.type_name {
        out.push(Difference { kind: Kind::FieldNamedTypeChanged, path: path.to_string() });
    }
}

fn is_scalar(t: FieldType) -> bool {
    !matches!(t, FieldType::Message | FieldType::Enum | FieldType::Group)
}

/// Confluent's wire-compatible scalar groups.
fn same_group(a: FieldType, b: FieldType) -> bool {
    use FieldType::{Bool, Bytes, Fixed32, Fixed64, Int32, Int64, Sfixed32, Sfixed64, Sint32, Sint64, String as Str, Uint32, Uint64};
    fn g(t: FieldType) -> u8 {
        match t {
            Int32 | Int64 | Uint32 | Uint64 | Bool => 1,
            Sint32 | Sint64 => 2,
            Str | Bytes => 3,
            Fixed32 | Sfixed32 => 4,
            Fixed64 | Sfixed64 => 5,
            FieldType::Float => 6,
            FieldType::Double => 7,
            _ => 0,
        }
    }
    let (ga, gb) = (g(a), g(b));
    ga != 0 && ga == gb
}

fn join(prefix: &str, name: &str) -> String {
    if prefix.is_empty() { name.to_string() } else { format!("{prefix}.{name}") }
}
```
(Use `of.label()` / `of.r#type()` accessors — they exist on the prost type. `m.name()` / `f.number()` accessors return the unwrapped value-or-default.)

- [ ] **Step 4: Write `compat.rs`** — seed classification + messages:
```rust
//! Backward-compatibility classification for Protobuf differences. SEED values
//! from Confluent's behavior; the cp golden matrix (compat_conformance) is the
//! authority and re-tunes this table.

use super::diff::{Difference, Kind};

/// Is this difference backward-compatible (reader can still read writer's data)?
#[must_use]
pub fn is_backward_compatible(kind: &Kind) -> bool {
    match kind {
        Kind::FieldAdded | Kind::FieldRemoved | Kind::MessageAdded | Kind::MessageRemoved => true,
        Kind::FieldScalarKindChanged { compatible_group } => *compatible_group,
        Kind::FieldKindChanged | Kind::FieldNamedTypeChanged | Kind::FieldLabelChanged => false,
    }
}

#[must_use]
pub fn messages(diffs: &[&Difference]) -> Vec<String> {
    diffs.iter().map(|d| format!("{:?} at {}", d.kind, d.path)).collect()
}
```

- [ ] **Step 5: Run** `cargo test -p crabka-schema-registry --lib format::protobuf` → the 5 batch-1 tests pass. `cargo build` clean.
- [ ] **Step 6: clippy + fmt + commit** (`src/format/protobuf/` whole dir), message:
`schema-registry: protobuf diff framework + field/scalar/kind/label rules`

> NOTE: the seed verdicts above (e.g. `FieldRemoved` = compatible, `MessageRemoved` = compatible) are best-effort; Task 6 calibrates them against cp. If a batch-1 unit test's expected verdict later conflicts with cp, cp wins — update both the table and the unit test, with a one-line note.

---

## Task 2: oneof rules

**Files:** Modify `diff.rs`, `compat.rs`.

- [ ] **Step 1: Add unit tests** (in `mod.rs`/`diff.rs` tests):
```rust
    #[test] fn moving_field_into_oneof_compatibility() {
        // v1: two singular fields; v2: same two numbers inside a oneof.
        let w = "syntax = \"proto3\"; message U { int32 a = 1; int32 b = 2; }";
        let r = "syntax = \"proto3\"; message U { oneof x { int32 a = 1; int32 b = 2; } }";
        // record whatever the engine says; Task 6 calibrates against cp. Assert it does NOT panic and returns a verdict:
        let _ = check(r, w);
    }
    #[test] fn proto3_optional_is_not_a_oneof_change() {
        // adding `optional` (synthetic oneof) must NOT be flagged as a oneof move.
        let w = "syntax = \"proto3\"; message U { int32 a = 1; }";
        let r = "syntax = \"proto3\"; message U { optional int32 a = 1; }";
        assert!(check(r, w).is_ok(), "proto3 optional is not a oneof migration");
    }
```

- [ ] **Step 2: Add `Kind` variants** to `diff.rs`: `OneofFieldMovedIn`, `OneofFieldMovedOut`, `OneofAdded`, `OneofRemoved`. Extend `compare_field` (and `compare_message`) to detect oneof membership by comparing each field's **real** oneof membership. A field is a real oneof member iff `f.oneof_index.is_some() && f.proto3_optional != Some(true)`. For a field present in both with changed real-oneof-membership: emit `OneofFieldMovedIn`/`OneofFieldMovedOut`. Compare `orig.oneof_decl` vs `upd.oneof_decl` (by name) for `OneofAdded`/`OneofRemoved`. Add a helper `fn real_oneof(f) -> Option<usize>` returning the oneof index only when not proto3_optional.

- [ ] **Step 3: Classify** in `compat.rs` (seed): `OneofFieldMovedIn => true`, `OneofFieldMovedOut => false`, `OneofAdded => true`, `OneofRemoved => false` (seed — Task 6 calibrates).

- [ ] **Step 4: Run** `cargo test -p crabka-schema-registry --lib format::protobuf` → pass; `cargo build` clean.
- [ ] **Step 5: clippy + fmt + commit**, message `schema-registry: protobuf oneof compatibility rules`.

---

## Task 3: reserved + map rules

**Files:** Modify `diff.rs`, `compat.rs`.

- [ ] **Step 1: Add unit tests:**
```rust
    #[test] fn reserving_a_number_is_compatible() {
        let w = "syntax = \"proto3\"; message U { int32 id = 1; }";
        let r = "syntax = \"proto3\"; message U { reserved 2; int32 id = 1; }";
        assert!(check(r, w).is_ok());
    }
    #[test] fn map_value_type_change_across_group_is_incompatible() {
        let w = "syntax = \"proto3\"; message U { map<string, int32> m = 1; }";
        let r = "syntax = \"proto3\"; message U { map<string, string> m = 1; }";
        assert!(check(r, w).is_err());
    }
    #[test] fn identical_map_is_compatible() {
        let s = "syntax = \"proto3\"; message U { map<string, int32> m = 1; }";
        assert!(check(s, s).is_ok());
    }
```

- [ ] **Step 2: Map handling** in `diff.rs`: a `map<K,V>` field is a `repeated` field whose `type` is `Message` and whose `type_name` resolves to a synthetic nested message with `options.map_entry == Some(true)` (fields #1 key, #2 value). When comparing two fields that are both maps (resolve their entry messages via the message's `nested_type` by `type_name`), compare the key (#1) and value (#2) field types using the same scalar/kind logic instead of treating it as `FieldNamedTypeChanged`. Add a helper `fn map_entry<'a>(msg: &'a DescriptorProto, type_name: &str) -> Option<&'a DescriptorProto>` that finds the nested type with `map_entry`. (Recursion: skip synthetic map-entry messages when recursing `nested_type` so they aren't double-compared as ordinary messages.)

- [ ] **Step 3: Reserved handling** in `diff.rs`: add `Kind::ReservedNumberAdded`, `Kind::ReservedNameAdded`. Compare `orig.reserved_range`/`reserved_name` to `upd`'s; a newly-reserved number/name is its own difference. (Reusing a previously-live number as reserved while removing the field is the common "safe removal" idiom — ensure that combination classifies compatible.)

- [ ] **Step 4: Classify** (seed): `ReservedNumberAdded => true`, `ReservedNameAdded => true`. Map differences reuse the scalar/kind kinds on the entry's key/value, so no new map Kind is needed (the value-type change surfaces as `FieldScalarKindChanged`/`FieldKindChanged` on the entry).

- [ ] **Step 5: Run** tests → pass; build clean.
- [ ] **Step 6: clippy + fmt + commit**, message `schema-registry: protobuf reserved + map compatibility rules`.

---

## Task 4: enum + message + nested + package rules

**Files:** Modify `diff.rs`, `compat.rs`.

- [ ] **Step 1: Add unit tests:**
```rust
    #[test] fn enum_const_added_compatible() {
        let w = "syntax = \"proto3\"; enum E { A = 0; } message U { E e = 1; }";
        let r = "syntax = \"proto3\"; enum E { A = 0; B = 1; } message U { E e = 1; }";
        assert!(check(r, w).is_ok());
    }
    #[test] fn nested_message_field_change_detected() {
        let w = "syntax = \"proto3\"; message U { message N { int32 a = 1; } N n = 1; }";
        let r = "syntax = \"proto3\"; message U { message N { string a = 1; } N n = 1; }";
        assert!(check(r, w).is_err(), "nested int32->string is across-group");
    }
    #[test] fn package_change_detected() {
        let w = "syntax = \"proto3\"; package a; message U { int32 id = 1; }";
        let r = "syntax = \"proto3\"; package b; message U { int32 id = 1; }";
        let _ = check(r, w); // verdict calibrated in Task 6; assert no panic + a Difference is produced
    }
```

- [ ] **Step 2:** In `diff.rs`: (a) recurse `nested_type` inside `compare_message` (call `compare_messages(path, &orig.nested_type, &upd.nested_type, out)`, skipping map-entry synthetic messages from Task 3); (b) compare file-level + nested `enum_type` (match enums by name; for each, match values by **number** → `EnumConstAdded`/`EnumConstRemoved`; whole enum add/remove → `EnumAdded`/`EnumRemoved`); (c) compare `original.package` vs `update.package` → `PackageChanged`. Add the `Kind` variants `EnumConstAdded`, `EnumConstRemoved`, `EnumAdded`, `EnumRemoved`, `PackageChanged`.

- [ ] **Step 3: Classify** (seed): `EnumConstAdded => true`, `EnumConstRemoved => true`, `EnumAdded => true`, `EnumRemoved => true`, `PackageChanged => false`.

- [ ] **Step 4: Run** tests → pass; build clean.
- [ ] **Step 5: clippy + fmt + commit**, message `schema-registry: protobuf enum/message/nested/package compatibility rules`.

---

## Task 5: capture the cp Protobuf golden matrix (Docker)

**Files:** Create `crates/schema-registry/tests/capture_protobuf_fixtures.rs`; output `tests/fixtures/compat/protobuf_matrix.json`.

- [ ] **Step 1: Write the `#[ignore]` harness** modeled on `tests/capture_compat_fixtures.rs` (READ it — copy the broker + `cp-schema-registry:7.4.0` setup verbatim). For each `(case, writer_proto, reader_proto)` below, under each level in `[BACKWARD, FORWARD, FULL]`: `PUT /config/{case}-{level}` the level; `POST /subjects/{case}-{level}/versions` `{"schema": writer, "schemaType":"PROTOBUF"}`; `POST /compatibility/subjects/{case}-{level}/versions/latest` `{"schema": reader, "schemaType":"PROTOBUF"}` → record `is_compatible`. Write all to `tests/fixtures/compat/protobuf_matrix.json` (array of `{case, level, writer, reader, is_compatible}`). Build request bodies with serde_json so the `.proto` string is correctly escaped.

Cases (each `writer`/`reader` a full `syntax="proto3";` source; ≈35 pairs):
```
field_added            W: message U{int32 id=1;}                 R: message U{int32 id=1; int32 x=2;}
field_removed          W: message U{int32 id=1; int32 x=2;}      R: message U{int32 id=1;}
scalar_int_widen       W: message U{int32 id=1;}                 R: message U{int64 id=1;}
scalar_int_to_string   W: message U{int32 id=1;}                 R: message U{string id=1;}
scalar_sint_group      W: message U{sint32 id=1;}                R: message U{sint64 id=1;}
scalar_string_bytes    W: message U{string id=1;}               R: message U{bytes id=1;}
scalar_fixed32_group   W: message U{fixed32 id=1;}               R: message U{sfixed32 id=1;}
scalar_int_to_sint     W: message U{int32 id=1;}                 R: message U{sint32 id=1;}     (different group)
kind_scalar_to_msg     W: message U{int32 id=1;}                 R: message M{} message U{M id=1;}
kind_scalar_to_enum    W: message U{int32 id=1;}                 R: enum E{A=0;} message U{E id=1;}
named_type_changed     W: message A{} message B{} message U{A f=1;}   R: message A{} message B{} message U{B f=1;}
label_singular_repeat  W: message U{int32 id=1;}                 R: message U{repeated int32 id=1;}
oneof_move_in          W: message U{int32 a=1; int32 b=2;}       R: message U{oneof x{int32 a=1; int32 b=2;}}
oneof_move_out         (reverse of above)
oneof_added            W: message U{int32 a=1;}                  R: message U{oneof x{int32 a=1;} }  (note: same number into a new oneof)
proto3_optional        W: message U{int32 a=1;}                  R: message U{optional int32 a=1;}
reserved_number        W: message U{int32 id=1;}                 R: message U{reserved 2; int32 id=1;}
reserved_name          W: message U{int32 id=1;}                 R: message U{reserved "old"; int32 id=1;}
map_identical          W: message U{map<string,int32> m=1;}      R: (same)
map_value_widen        W: message U{map<string,int32> m=1;}      R: message U{map<string,int64> m=1;}
map_value_to_string    W: message U{map<string,int32> m=1;}      R: message U{map<string,string> m=1;}
scalar_to_map          W: message U{int32 m=1;}                  R: message U{map<string,int32> m=1;}
enum_const_added       W: enum E{A=0;} message U{E e=1;}         R: enum E{A=0;B=1;} message U{E e=1;}
enum_const_removed     (reverse)
enum_added             W: message U{int32 id=1;}                 R: enum E{A=0;} message U{int32 id=1;}
enum_removed           (reverse)
message_added          W: message U{int32 id=1;}                 R: message U{int32 id=1;} message V{int32 a=1;}
message_removed        (reverse)
nested_scalar_change   W: message U{message N{int32 a=1;} N n=1;}  R: message U{message N{string a=1;} N n=1;}
package_change         W: package a; message U{int32 id=1;}      R: package b; message U{int32 id=1;}
```
(Generate the obvious reverses; aim for ~35 case-pairs total.)

- [ ] **Step 2: Run** (Docker): `cargo test -p crabka-schema-registry --test capture_protobuf_fixtures -- --ignored --nocapture`. Confirm `protobuf_matrix.json` has ~105 entries (35×3). Add `[[test]] name = "capture_protobuf_fixtures"` to `Cargo.toml` if cargo doesn't auto-discover it (it does; the explicit stanza is optional but harmless).
- [ ] **Step 3: Inspect** and report a dozen representative verdicts (esp. field_removed/BACKWARD, oneof_move_in/all levels, scalar_int_to_sint/BACKWARD, package_change/all) — these reveal cp's ground truth for calibration.
- [ ] **Step 4: clippy + fmt + commit** (`capture_protobuf_fixtures.rs` + `protobuf_matrix.json` [+ Cargo.toml]), message `schema-registry: golden Protobuf compatibility verdicts from cp-schema-registry 7.4.0`.

> If Docker is unavailable, STOP and report — the controller runs the capture. Do NOT fabricate verdicts.

---

## Task 6: conformance calibration + enforcement

**Files:** Modify `tests/compat_conformance.rs`, `tests/integration.rs`; re-tune `src/format/protobuf/compat.rs` (and `diff.rs` if a difference isn't detected) until the matrix passes.

- [ ] **Step 1: Extend `compat_conformance.rs`** to also load `protobuf_matrix.json` and assert the engine matches, mirroring the Avro `engine_matches_cp_verdicts` test (build a `StoreState`, `set_subject_compat`, `register` the writer as Protobuf, `compat::check_against_version` the reader, compare `is_compatible`). Reuse the same `known_divergences` allowlist pattern (start empty).
```rust
#[test]
fn engine_matches_cp_protobuf_verdicts() {
    let cases: Vec<Case> = load("protobuf_matrix.json");
    let known_divergences: std::collections::HashMap<(&str, &str), bool> = std::collections::HashMap::from([]);
    let mut mismatches = Vec::new();
    for c in cases {
        let mut snap = StoreState::default();
        snap.set_subject_compat("s", c.level.clone());
        snap.register("s", SchemaType::Protobuf, &c.writer).expect("writer registers");
        let got = compat::check_against_version(&snap, "s", SchemaType::Protobuf, &c.reader, None).unwrap().is_compatible;
        let expected = *known_divergences.get(&(c.case.as_str(), c.level.as_str())).unwrap_or(&c.is_compatible);
        if got != expected { mismatches.push(format!("{}/{}: ours={got} cp={}", c.case, c.level, c.is_compatible)); }
    }
    assert!(mismatches.is_empty(), "protobuf engine diverges from cp on:\n{}", mismatches.join("\n"));
}
```

- [ ] **Step 2: Run** `cargo test -p crabka-schema-registry --test compat_conformance -- --nocapture`. **This is the calibration gate.** For every mismatch, fix `compat.rs`'s `is_backward_compatible` table (or `diff.rs` if a difference isn't being detected at all) until all pass. If a verdict is a genuine cp quirk we choose not to match, add it to `known_divergences` with a reason in `tests/fixtures/compat/README.md`. Re-run until green. **Report every table change you made and the final divergence list.**

- [ ] **Step 3: Add a Protobuf enforcement integration test** to `tests/integration.rs` (reuse `boot_registry`, `req_post`, `req_put`, `body_json`):
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protobuf_compat_enforced_on_register() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    let v1 = r#"{"schemaType":"PROTOBUF","schema":"syntax = \"proto3\"; message U { int32 id = 1; }"}"#;
    assert_eq!(app.clone().oneshot(req_post("/subjects/pb/versions", v1)).await.unwrap().status(), StatusCode::OK);
    // across-group scalar change -> incompatible under default BACKWARD
    let bad = r#"{"schemaType":"PROTOBUF","schema":"syntax = \"proto3\"; message U { string id = 1; }"}"#;
    let r = app.clone().oneshot(req_post("/subjects/pb/versions", bad)).await.unwrap();
    assert_eq!(r.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(r).await["error_code"], 409);
    // add a field -> compatible
    let good = r#"{"schemaType":"PROTOBUF","schema":"syntax = \"proto3\"; message U { int32 id = 1; int32 x = 2; }"}"#;
    assert_eq!(app.clone().oneshot(req_post("/subjects/pb/versions", good)).await.unwrap().status(), StatusCode::OK);
    cancel.cancel();
    broker.shutdown().await;
}
```

- [ ] **Step 4: Run** `cargo test -p crabka-schema-registry --test integration --test compat_conformance -- --nocapture` → all pass. (CI's `schema-registry-integration` job already runs `--test compat_conformance` and `--test integration`; no workflow change needed.)
- [ ] **Step 5: clippy + fmt + commit** (`compat_conformance.rs`, `integration.rs`, `src/format/protobuf/compat.rs`/`diff.rs` re-tunes [+ README.md]), message `schema-registry: protobuf compatibility conformance (cp-calibrated) + enforcement`.

---

## Self-review (completed by plan author)

**Spec coverage:** diff-based `check` reusing engine direction → Task 1; full rule catalog → Tasks 1–4 (field/scalar/kind/label, oneof, reserved/map, enum/message/nested/package); fields-by-number → Task 1 `compare_message`; module split → Task 1; cp matrix + calibration (cp authority) → Tasks 5–6; per-rule unit tests → Tasks 1–4; enforcement integration → Task 6; out-of-scope (groups/extensions/custom-options permissive; JSON Schema; all-versions endpoint) → absent.

**Placeholder scan:** the seed classification verdicts are explicitly labeled seed-then-calibrate (Task 6 is the authority) — this is the spec's design, not an unfilled placeholder. Unit tests with `let _ = check(...)` (oneof_move/package) deliberately assert "produces a verdict without panic" because the *verdict* is cp-calibrated in Task 6; the discriminating assertions live in the matrix.

**Type consistency:** `diff::{Difference, Kind, compare}`, `compat::{is_backward_compatible, messages}`, `ProtobufSchema::descriptor()`, `format::check(Protobuf, reader, writer)`, `compat::check_against_version(.., SchemaType::Protobuf, ..)`, and the `same_group`/`is_scalar`/`real_oneof`/`map_entry` helpers are referenced consistently. `Kind` grows additively across Tasks 1–4; `is_backward_compatible` must get an arm for every variant (a non-exhaustive match is a clippy/compile error, which enforces this).
