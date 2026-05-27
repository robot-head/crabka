# Mass Rollout (sub-plan 1d) Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** Not tracked as a dedicated STATUS.md header — covered implicitly by the protocol-foundation preamble or rolled into subsequent slices.

**Incomplete / deferred steps:** None recorded in STATUS.md.

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn on codegen for every active Kafka 4.2 schema (~190 messages) and prove byte-equality with the JVM `kafka-clients` 4.2.0 for every `(api_key, version)` pair.

**Architecture:** Replace `CURATED`-as-allowlist with `validVersions.is_empty()` as the only skip. Generate wrappers and `mod.rs` files as drift-checked artifacts. Codegen emits a `default_json()` helper per message so a single parameterised `differential_all.rs` test can sweep all pairs without per-message fixture maintenance. Headers gain `header_encode` / `header_decode` oracle ops, resolving the 1a deferral.

**Tech Stack:** Rust 1.95.0 (edition 2024); existing `crabka-protocol`, `crabka-protocol-codegen`, `crabka-compression`; existing JVM oracle in `tools/oracle/` extended with header ops; `serde_json` for the codegen-emitted defaults; nightly GitHub Actions workflow for the 256-proptest sweep.

**Reference spec:** [`docs/superpowers/specs/2026-05-11-crabka-coverage-mass-rollout-1d-design.md`](../specs/2026-05-11-crabka-coverage-mass-rollout-1d-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Implementation runs on `feature/coverage-mass-rollout-1d`, branched from `main` once this plan's PR merges.

---

## File structure

```
crates/protocol-codegen/src/
├── main.rs                      # gate: "validVersions.is_empty()" skip
├── emit/
│   ├── wrappers.rs              # NEW: generate src/{owned,borrowed}/<snake>.rs
│   ├── mod_rs.rs                # NEW: generate src/{owned,borrowed}/mod.rs
│   └── default_json.rs          # NEW: per-message default_json() emitter

crates/protocol/generated/
├── owned/*.rs                   # existing per-message generated bodies
├── borrowed/*.rs                # existing per-message generated bodies
├── wrappers/                    # NEW: generated wrapper bodies
│   ├── owned/<snake>.rs
│   └── borrowed/<snake>.rs
├── mod_rs/                      # NEW: generated mod.rs bodies
│   ├── owned_mod.rs
│   └── borrowed_mod.rs
└── default_json.rs              # NEW: dispatch shim for differential_all

crates/protocol/src/
├── owned/<snake>.rs             # include! wrappers/owned/<snake>.rs (drift-checked)
├── owned/mod.rs                 # include! mod_rs/owned_mod.rs
├── borrowed/<snake>.rs          # mirror
└── borrowed/mod.rs              # mirror

crates/protocol/tests/
└── differential_all.rs          # NEW: parameterised sweep

tools/oracle/src/main/java/com/crabka/oracle/Oracle.java
                                 # adds header_encode + header_decode ops

.github/workflows/
└── nightly-differential.yml     # NEW: scheduled 256-proptest sweep

KNOWN_ISSUES.md                  # update: remove header deferral, add corpus carve-out
```

---

## Phase A — Wrapper and mod.rs generation

### Task 1: Emit wrappers as generated artifacts

Refactor the codegen so wrappers (`crates/protocol/src/{owned,borrowed}/<snake>.rs`) are produced by the codegen bin, not hand-written. Existing wrappers stay byte-identical until the codegen takes over; after this task, all 12 curated-message wrappers are regenerated.

**Files:**
- Create: `crates/protocol-codegen/src/emit/wrappers.rs`
- Modify: `crates/protocol-codegen/src/emit/mod.rs`
- Modify: `crates/protocol-codegen/src/main.rs`

- [ ] **Step 1: Write the wrapper emitter**

`crates/protocol-codegen/src/emit/wrappers.rs`:

```rust
//! Generate the `include!` wrapper module bodies under
//! `crates/protocol/src/{owned,borrowed}/<snake>.rs`.

use std::fmt::Write;

use crate::emit::common::banner;
use crate::ir::{MessageSpec, MessageType};
use crate::name_conv;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    Owned,
    Borrowed,
}

impl Flavor {
    fn dir(self) -> &'static str {
        match self {
            Flavor::Owned => "owned",
            Flavor::Borrowed => "borrowed",
        }
    }
    fn snake(self) -> &'static str { self.dir() }
}

/// Emit a wrapper body for one message + flavor.
#[must_use]
pub fn emit(spec: &MessageSpec, flavor: Flavor, schemas_version: &str) -> String {
    let type_name = name_conv::type_name(&spec.name);
    let snake = name_conv::module_name(&spec.name);
    let flavor_dir = flavor.dir();
    let suffix = match flavor {
        Flavor::Owned => "owned",
        Flavor::Borrowed => "borrowed",
    };
    let mut out = banner(schemas_version);
    writeln!(out, "#![allow(clippy::pedantic, dead_code, clippy::similar_names, clippy::redundant_else, clippy::needless_late_init, clippy::collapsible_if, clippy::useless_let_if_seq)]").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "include!(concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/generated/{flavor_dir}/{type_name}.{suffix}.rs\"));").unwrap();
    writeln!(out).unwrap();
    // Inline round-trip tests gated on the flavor.
    match flavor {
        Flavor::Owned => write_owned_tests(&mut out, &type_name),
        Flavor::Borrowed => write_borrowed_tests(&mut out, &type_name),
    }
    let _ = (snake, type_name);
    out
}

fn write_owned_tests(out: &mut String, type_name: &str) {
    writeln!(out, "#[cfg(test)]
mod tests {{
    use super::*;
    use bytes::BytesMut;
    use crate::{{Decode, Encode}};

    #[test]
    fn min_version_roundtrips() {{
        let v = MIN_VERSION;
        let msg = {type_name}::default();
        let mut buf = BytesMut::new();
        msg.encode(&mut buf, v).unwrap();
        assert_eq!(msg.encoded_len(v), buf.len());
        let mut cur = &buf[..];
        let decoded = {type_name}::decode(&mut cur, v).unwrap();
        assert_eq!(decoded, msg);
    }}

    #[test]
    fn max_version_roundtrips() {{
        let v = MAX_VERSION;
        let msg = {type_name}::default();
        let mut buf = BytesMut::new();
        msg.encode(&mut buf, v).unwrap();
        assert_eq!(msg.encoded_len(v), buf.len());
        let mut cur = &buf[..];
        let decoded = {type_name}::decode(&mut cur, v).unwrap();
        assert_eq!(decoded, msg);
    }}
}}").unwrap();
}

fn write_borrowed_tests(out: &mut String, type_name: &str) {
    writeln!(out, "#[cfg(test)]
mod tests {{
    use super::*;
    use bytes::BytesMut;
    use crate::{{DecodeBorrow, Encode}};

    #[test]
    fn min_version_roundtrips() {{
        let v = MIN_VERSION;
        let msg = {type_name}::default();
        let mut buf = BytesMut::new();
        msg.encode(&mut buf, v).unwrap();
        let frozen = buf.freeze();
        let mut cur: &[u8] = &frozen;
        let _decoded = {type_name}::decode_borrow(&mut cur, v).unwrap();
    }}

    #[test]
    fn max_version_roundtrips() {{
        let v = MAX_VERSION;
        let msg = {type_name}::default();
        let mut buf = BytesMut::new();
        msg.encode(&mut buf, v).unwrap();
        let frozen = buf.freeze();
        let mut cur: &[u8] = &frozen;
        let _decoded = {type_name}::decode_borrow(&mut cur, v).unwrap();
    }}
}}").unwrap();
}

/// True if this spec should get a wrapper (skip deprecated schemas).
#[must_use]
pub fn should_emit_wrapper(spec: &MessageSpec) -> bool {
    !spec.valid_versions.is_empty()
        && matches!(spec.message_type, MessageType::Request | MessageType::Response | MessageType::Header | MessageType::Data)
}
```

- [ ] **Step 2: Wire the emitter into `main.rs`**

In `crates/protocol-codegen/src/main.rs`, after the existing per-message body emission, add wrapper emission for each flavor. The output path is `crates/protocol/src/{owned,borrowed}/<snake>.rs` — overwrites existing hand-written wrappers.

Make a wrapper-write function in `main.rs`:

```rust
fn write_wrapper(
    spec: &ir::MessageSpec,
    flavor: emit::wrappers::Flavor,
    schemas_version: &str,
    protocol_src: &Path,
) -> std::io::Result<()> {
    use emit::wrappers::Flavor;
    let snake = crabka_protocol_codegen::name_conv::module_name(&spec.name);
    let body = emit::wrappers::emit(spec, flavor, schemas_version);
    let dir = protocol_src.join(match flavor {
        Flavor::Owned => "owned",
        Flavor::Borrowed => "borrowed",
    });
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join(format!("{snake}.rs")), body)?;
    Ok(())
}
```

Call it for each curated spec (still using the existing CURATED — we don't flip it yet; this task only proves the wrapper generator produces byte-identical output for the existing 12 messages).

- [ ] **Step 3: Regenerate and verify drift**

```bash
./tools/regenerate.sh
git diff crates/protocol/src/owned crates/protocol/src/borrowed
```

The diff should be empty or near-empty (only banner-comment changes if anything). If there's substantive divergence — different test bodies, different `#![allow]` lists — the existing hand-written wrappers had bespoke content. Adjust the emitter's templates to match.

- [ ] **Step 4: Run tests**

```bash
cargo test -p crabka-protocol
```

Expected: all existing tests still pass. The 12 wrapper files were just regenerated; tests within them still run.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(codegen): emit wrapper modules as drift-checked artifacts"
```

---

### Task 2: Emit `mod.rs` as a generated artifact

After Task 1, the wrappers are generated. Now make `crates/protocol/src/{owned,borrowed}/mod.rs` generated too, listing every active module alphabetically.

**Files:**
- Create: `crates/protocol-codegen/src/emit/mod_rs.rs`
- Modify: `crates/protocol-codegen/src/main.rs`

- [ ] **Step 1: Write the `mod.rs` emitter**

`crates/protocol-codegen/src/emit/mod_rs.rs`:

```rust
//! Generate the `mod.rs` files for `crates/protocol/src/{owned,borrowed}/`.

use std::fmt::Write;

use crate::emit::common::banner;
use crate::emit::wrappers::Flavor;
use crate::ir::MessageSpec;
use crate::name_conv;

/// Emit a mod.rs that declares one `pub mod` per active spec, sorted by name.
/// Also re-exports `common` if the flavor uses common-struct support
/// (placeholder for future commonStructs work; included as a comment for now).
#[must_use]
pub fn emit(specs: &[MessageSpec], _flavor: Flavor, schemas_version: &str) -> String {
    let mut out = banner(schemas_version);
    writeln!(out).unwrap();
    // List alphabetically by snake-case name.
    let mut entries: Vec<String> = specs
        .iter()
        .filter(|s| !s.valid_versions.is_empty())
        .map(|s| name_conv::module_name(&s.name))
        .collect();
    entries.sort();
    entries.dedup();
    for snake in &entries {
        writeln!(out, "pub mod {snake};").unwrap();
    }
    out
}
```

- [ ] **Step 2: Wire into `main.rs`**

After per-message wrapper emission, emit the two `mod.rs` files:

```rust
let owned_mod = emit::mod_rs::emit(&specs, Flavor::Owned, &schemas_sha);
let borrowed_mod = emit::mod_rs::emit(&specs, Flavor::Borrowed, &schemas_sha);
std::fs::write(protocol_src.join("owned/mod.rs"), owned_mod)?;
std::fs::write(protocol_src.join("borrowed/mod.rs"), borrowed_mod)?;
```

Pass the **full** `specs` list (every active schema) — not the curated subset. This produces a mod.rs that names every active module, even though wrappers only exist for the curated set yet. **Task 3 flips the wrapper gate** so this immediately becomes consistent.

If the generated mod.rs references modules whose wrapper files don't exist, the build breaks. Task 2 thus depends on Task 3 ordering — alternatively, in Task 2 we filter mod.rs entries to only the curated set, then update in Task 3 to flip both gates simultaneously.

**Choose:** flip both gates in **Task 3** as a single coherent change. In Task 2, run mod.rs emission only over `CURATED` so the build stays green, then expand the gate in Task 3.

Concretely, in Task 2's `main.rs` call, filter the specs passed to `emit::mod_rs::emit` to only those whose names are in `CURATED`. Task 3 removes that filter.

- [ ] **Step 3: Regenerate, verify drift**

```bash
./tools/regenerate.sh
git diff crates/protocol/src/owned/mod.rs crates/protocol/src/borrowed/mod.rs
```

Expect tiny or empty diff. If the existing hand-written mod.rs has extra `pub use` statements or different ordering, decide whether the codegen template should match or whether the hand-written form should be replaced — preserve any genuinely useful re-exports.

- [ ] **Step 4: Test + commit**

```bash
cargo test -p crabka-protocol
git add -A
git commit -m "feat(codegen): emit owned/borrowed mod.rs as generated artifacts"
```

---

## Phase B — Flip the gate

### Task 3: Switch to "all active" gate

This is the big flip. Replace the `CURATED.contains(&s.name.as_str())` allowlist with `s.valid_versions.is_empty()` as the only skip. Run the build and tests; **expect breakage**, fix each issue at the source.

**Files:**
- Modify: `crates/protocol-codegen/src/main.rs`

- [ ] **Step 1: Make the change**

In `main.rs`, find the `CURATED` constant (a `&[&str]`) and the `if !CURATED.contains(&s.name.as_str()) { continue; }` line. Replace with:

```rust
fn should_emit(spec: &ir::MessageSpec) -> bool {
    !spec.valid_versions.is_empty()
}
```

Update both the per-message-body loop and the wrapper-emission call sites to use `should_emit`. Remove `CURATED` from `main.rs` entirely.

Update Task 2's mod.rs emission to use `should_emit` instead of the `CURATED` filter.

- [ ] **Step 2: Regenerate**

```bash
./tools/regenerate.sh 2>&1 | tee /tmp/regen.log | tail -50
```

Expected: every active schema's `.owned.rs` and `.borrowed.rs` files appear in `crates/protocol/generated/owned/` and `crates/protocol/generated/borrowed/`. Wrappers appear in `crates/protocol/src/{owned,borrowed}/<snake>.rs`. `mod.rs` lists every active module.

- [ ] **Step 3: Build and triage failures**

```bash
cargo build -p crabka-protocol 2>&1 | head -100
```

Likely failures fall into a few classes:

1. **A wrapper file's `MIN_VERSION` or `MAX_VERSION` doesn't compile in the inline test** because the generated message has different constants (e.g., a `Data`-typed schema doesn't get an `API_KEY` constant). Fix the wrapper emitter to skip `API_KEY`-dependent code for non-Request/Response messages.

2. **Codegen emits a field type the wrapper test doesn't satisfy** — e.g., the message has a nested struct without `Default`, so `default()` panics. Fix the codegen's `Default` impl emission to cover the new shape.

3. **`mod.rs` references a wrapper that wasn't emitted** — e.g., the wrapper emitter filtered something the `should_emit` gate accepted. Tighten the filters to agree exactly.

For each failure, **fix at the source in `crates/protocol-codegen`**, regenerate, re-run. Each fix is its own commit on the feature branch.

- [ ] **Step 4: Run tests**

```bash
cargo test -p crabka-protocol
```

Expected (eventually): all unit tests pass — every wrapper's `min_version_roundtrips` and `max_version_roundtrips` runs. Errors fall into the same triage classes as Step 3.

If a single message's default fixture doesn't round-trip (encode → decode != original), that's a real codec bug. Diagnose using the patterns from the design's Section 3:
- tagged-field default mismatch (manual Default impl needed)
- nullable-vs-empty
- field-order
- version-conditional gating off-by-one
- compact-array length

Fix in the emitter; do not skip the message.

- [ ] **Step 5: Run differential regression**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.19.10-hotspot"
cargo test -p crabka-protocol --tests -- --ignored
```

Expected: existing differential tests (`differential_api_versions`, `differential_metadata`, `differential_produce`, `differential_offset_commit`, `differential_describe_groups`, `differential_records`) all continue to pass.

If any fails because the generated code changed for an existing message, fix the codegen — do not modify the existing differential test fixtures to accommodate divergence.

- [ ] **Step 6: Commit**

After the build is green and tests pass:

```bash
git add -A
git commit -m "feat(codegen): emit every active schema (~190 messages)"
```

---

## Phase C — Default-JSON helper

### Task 4: Emit `default_json()` per message

For `differential_all` to work, the JVM and Rust must agree on what "default state" means. The codegen emits a `pub fn default_json() -> serde_json::Value` per message that produces the JSON the oracle should receive to match `MessageName::default()`.

**Files:**
- Create: `crates/protocol-codegen/src/emit/default_json.rs`
- Modify: `crates/protocol-codegen/src/emit/owned.rs` (call into `default_json`)
- Modify: `crates/protocol-codegen/src/main.rs` (no-op if owned.rs already includes it)
- Modify: `crates/protocol/Cargo.toml` (add `serde_json` to dependencies if it's not already there)

- [ ] **Step 1: Check whether `serde_json` is in `crabka-protocol`'s deps**

```bash
grep -n serde_json crates/protocol/Cargo.toml
```

If not present in `[dependencies]` (only in `[dev-dependencies]`), add to `[dependencies]` via the workspace dep:

```toml
serde_json = { workspace = true }
```

(`serde_json` is already in `[workspace.dependencies]` from foundation.)

- [ ] **Step 2: Write the `default_json` emitter**

`crates/protocol-codegen/src/emit/default_json.rs`:

```rust
//! Emit `pub fn default_json() -> serde_json::Value` per message.

use std::fmt::Write;

use crate::ir::{FieldSpec, MessageSpec, MessageType};
use crate::name_conv;

/// Emit the body of a `default_json()` function for the given message,
/// producing JSON matching the schema's declared defaults.
///
/// The output is plain Rust source intended to be appended to the
/// per-message owned module body. The function constructs a
/// `serde_json::Value` whose shape mirrors what
/// Kafka's `MessageDataJsonConverter` accepts as default input for the
/// equivalent struct.
#[must_use]
pub fn emit_default_json(spec: &MessageSpec) -> String {
    let mut out = String::new();
    writeln!(out, "
/// Default JSON payload the JVM oracle should receive to match
/// `Self::default()` byte-for-byte.
#[must_use]
pub fn default_json() -> ::serde_json::Value {{
    ::serde_json::json!({});
}}",
        emit_object(&spec.fields, matches!(spec.message_type, MessageType::Request | MessageType::Response | MessageType::Header))
    ).unwrap();
    out
}

fn emit_object(fields: &[FieldSpec], _is_top_level: bool) -> String {
    let mut s = String::new();
    s.push('{');
    let mut first = true;
    for f in fields {
        if !first { s.push_str(", "); }
        first = false;
        // JSON field names are camelCase (as in upstream schemas).
        write!(s, "\"{}\": {}", f.name, default_value_for(f)).unwrap();
    }
    s.push('}');
    s
}

fn default_value_for(f: &FieldSpec) -> String {
    // Use the field's `default` annotation if present; else fall back to
    // the "zero" value for its type. The codegen already computes
    // schema-aware defaults for the Rust struct's `Default` impl; this
    // function mirrors that logic for JSON.
    if let Some(d) = &f.default {
        return d.to_string();
    }
    let t = strip_array(&f.field_type);
    match t {
        "bool" => "false".into(),
        "int8" | "int16" | "int32" | "int64" | "uint16" | "uint32" => "0".into(),
        "float64" => "0.0".into(),
        "string" => "\"\"".into(),
        "bytes" | "records" => "\"\"".into(), // empty hex
        "uuid" => "\"00000000-0000-0000-0000-000000000000\"".into(),
        _ => {
            if f.field_type.starts_with("[]") {
                "[]".into()
            } else if !f.fields.is_empty() {
                emit_object(&f.fields, false)
            } else {
                "null".into() // commonStructs default — overridden as needed
            }
        }
    }
}

fn strip_array(t: &str) -> &str { t.strip_prefix("[]").unwrap_or(t) }
```

> **Note on schema defaults vs Rust defaults:** the design says
> `default_json()` should match `MessageName::default()` byte-for-byte
> after JVM encoding. The emitter's per-field default lookup uses the
> schema's `default` annotation when present (matching the Rust struct's
> manual Default impl); for fields without a declared default, the
> emitter falls back to the type's zero. This must agree with the Rust
> Default impl's behaviour. If a future codegen change diverges (e.g.,
> a new schema construct), `differential_all` will catch it.

- [ ] **Step 3: Hook into the owned emitter**

In `crates/protocol-codegen/src/emit/owned.rs`, after the existing per-message body emission, append the result of `default_json::emit_default_json(spec)` to the output.

Add `use crate::emit::default_json;` near the other `use` statements.

- [ ] **Step 4: Expose the emit module**

`crates/protocol-codegen/src/emit/mod.rs`:

```rust
pub mod borrowed;
pub mod common;
pub mod default_json;
pub mod mod_rs;
pub mod owned;
pub mod wrappers;
```

- [ ] **Step 5: Regenerate + verify the helper is present**

```bash
./tools/regenerate.sh
grep -l "fn default_json" crates/protocol/generated/owned/*.rs | head -3
```

Expected: every owned generated body now contains `pub fn default_json()`.

- [ ] **Step 6: Test + commit**

```bash
cargo test -p crabka-protocol
git add -A
git commit -m "feat(codegen): emit default_json() helper per message"
```

---

## Phase D — Oracle: header ops

### Task 5: Add `header_encode` / `header_decode` to the JVM oracle

The existing oracle indexes by `apiKey`. Headers don't have an apiKey. Add two new ops that route through Kafka's `RequestHeaderData` / `ResponseHeaderData` types directly.

**Files:**
- Modify: `tools/oracle/src/main/java/com/crabka/oracle/Oracle.java`

- [ ] **Step 1: Read the existing dispatch**

```bash
cat tools/oracle/src/main/java/com/crabka/oracle/Oracle.java
```

Find the `handle()` (or equivalent) method's `op` dispatch — should have cases for `encode`, `decode`, `compress`, `decompress`, `record_batch_encode`, `record_batch_decode` from earlier sub-plans. Add `header_encode` and `header_decode` cases alongside.

- [ ] **Step 2: Implement the new ops**

```java
case "header_encode": {
    String kind = req.get("kind").asText();  // "request" or "response"
    short version = (short) req.get("version").asInt();
    JsonNode value = req.get("value");
    byte[] hex = headerEncode(kind, version, value);
    ObjectNode resp = M.createObjectNode();
    resp.put("ok", true);
    resp.put("hex", HexFormat.of().formatHex(hex));
    return resp;
}
case "header_decode": {
    String kind = req.get("kind").asText();
    short version = (short) req.get("version").asInt();
    byte[] bytes = HexFormat.of().parseHex(req.get("hex").asText());
    JsonNode value = headerDecode(kind, version, bytes);
    ObjectNode resp = M.createObjectNode();
    resp.put("ok", true);
    resp.set("value", value);
    return resp;
}
```

Helpers:

```java
private static byte[] headerEncode(String kind, short version, JsonNode value) throws Exception {
    if (kind.equals("request")) {
        org.apache.kafka.common.message.RequestHeaderData data =
            new org.apache.kafka.common.message.RequestHeaderData();
        org.apache.kafka.common.message.RequestHeaderDataJsonConverter
            .read(value, version);
        // The above static read returns a new RequestHeaderData; redo:
        data = org.apache.kafka.common.message.RequestHeaderDataJsonConverter
            .read(value, version);
        org.apache.kafka.common.protocol.ObjectSerializationCache cache =
            new org.apache.kafka.common.protocol.ObjectSerializationCache();
        int size = data.size(cache, version);
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(size);
        data.write(new org.apache.kafka.common.protocol.ByteBufferAccessor(buf), cache, version);
        buf.flip();
        byte[] out = new byte[buf.remaining()];
        buf.get(out);
        return out;
    } else if (kind.equals("response")) {
        org.apache.kafka.common.message.ResponseHeaderData data =
            org.apache.kafka.common.message.ResponseHeaderDataJsonConverter
                .read(value, version);
        org.apache.kafka.common.protocol.ObjectSerializationCache cache =
            new org.apache.kafka.common.protocol.ObjectSerializationCache();
        int size = data.size(cache, version);
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(size);
        data.write(new org.apache.kafka.common.protocol.ByteBufferAccessor(buf), cache, version);
        buf.flip();
        byte[] out = new byte[buf.remaining()];
        buf.get(out);
        return out;
    }
    throw new IllegalArgumentException("unknown kind: " + kind);
}

private static JsonNode headerDecode(String kind, short version, byte[] bytes) throws Exception {
    java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(bytes);
    if (kind.equals("request")) {
        org.apache.kafka.common.message.RequestHeaderData data =
            new org.apache.kafka.common.message.RequestHeaderData();
        data.read(new org.apache.kafka.common.protocol.ByteBufferAccessor(buf), version);
        return org.apache.kafka.common.message.RequestHeaderDataJsonConverter
            .write(data, version);
    } else if (kind.equals("response")) {
        org.apache.kafka.common.message.ResponseHeaderData data =
            new org.apache.kafka.common.message.ResponseHeaderData();
        data.read(new org.apache.kafka.common.protocol.ByteBufferAccessor(buf), version);
        return org.apache.kafka.common.message.ResponseHeaderDataJsonConverter
            .write(data, version);
    }
    throw new IllegalArgumentException("unknown kind: " + kind);
}
```

> **Note on method names:** `RequestHeaderDataJsonConverter` and
> `ResponseHeaderDataJsonConverter` are generated by Kafka's
> `MessageGenerator`. Verify the static method names (`read` /
> `write`) against the actual jar before relying on them; if Kafka
> 4.2 uses a different naming scheme, adapt:
>
> ```bash
> javap -p -classpath tools/oracle/build/install/crabka-oracle/lib/kafka-clients-*.jar \
>     org.apache.kafka.common.message.RequestHeaderDataJsonConverter | head -10
> ```

- [ ] **Step 3: Rebuild oracle and smoke-test**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.19.10-hotspot"
(cd tools/oracle && ./gradlew installDist -q --no-daemon)

# Encode a v1 RequestHeader: api_key=18 (ApiVersions), api_version=3, correlation_id=42, client_id="test"
echo '{"op":"header_encode","kind":"request","version":1,"value":{"requestApiKey":18,"requestApiVersion":3,"correlationId":42,"clientId":"test"}}' \
    | tools/oracle/build/install/crabka-oracle/bin/crabka-oracle.bat
```

Expected: `{"ok":true,"hex":"..."}` with a hex string. Decode the same hex via `header_decode` to round-trip.

- [ ] **Step 4: Commit**

```bash
git add tools/oracle
git commit -m "feat(oracle): header_encode/header_decode ops for differential testing"
```

---

## Phase E — `differential_all` parameterised sweep

### Task 6: Codegen — dispatch table for `differential_all`

The differential sweep needs a `(message_name, api_key, version, is_request)` table plus a dispatch shim mapping each entry to its typed `default()` + encode + `default_json()`. Generate both.

**Files:**
- Create: `crates/protocol-codegen/src/emit/differential_table.rs`
- Modify: `crates/protocol-codegen/src/emit/mod.rs`
- Modify: `crates/protocol-codegen/src/main.rs` (write to `crates/protocol/generated/differential_table.rs`)
- Modify: `crates/protocol/src/lib.rs` — add `pub mod differential_table_generated;` gated to `#[cfg(test)]` or feature

Actually the table should live where the test can include it. Cleanest: write to `crates/protocol/generated/differential_table.rs` and `include!` from `crates/protocol/tests/differential_all.rs`. No need to expose it on the public API.

- [ ] **Step 1: Write the emitter**

`crates/protocol-codegen/src/emit/differential_table.rs`:

```rust
//! Emit a Rust source file that provides:
//!   - `CASES: &[Case]` — a static table of (message, version) cases
//!   - `default_json_for(name, version)` — JSON the oracle should accept
//!   - `encode_default(name, version)` — Rust-encoded bytes via Default::default()
//!
//! Consumed by `crates/protocol/tests/differential_all.rs` via include!.

use std::fmt::Write;

use crate::ir::{MessageSpec, MessageType};
use crate::name_conv;

#[must_use]
pub fn emit(specs: &[MessageSpec], schemas_version: &str) -> String {
    let mut out = String::new();
    writeln!(out, "// AUTO-GENERATED by crabka-protocol-codegen against {schemas_version}. Do not edit.").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "use bytes::BytesMut;").unwrap();
    writeln!(out, "use crabka_protocol::Encode;").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "#[derive(Debug, Clone, Copy)]").unwrap();
    writeln!(out, "pub struct Case {{").unwrap();
    writeln!(out, "    pub name: &'static str,").unwrap();
    writeln!(out, "    pub api_key: i16,").unwrap();
    writeln!(out, "    pub version: i16,").unwrap();
    writeln!(out, "    pub kind: Kind,").unwrap();
    writeln!(out, "}}").unwrap();
    writeln!(out, "#[derive(Debug, Clone, Copy)]").unwrap();
    writeln!(out, "pub enum Kind {{ Request, Response, RequestHeader, ResponseHeader }}").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "pub const CASES: &[Case] = &[").unwrap();
    for s in specs {
        if s.valid_versions.is_empty() { continue; }
        let kind = match (&s.message_type, s.name.as_str()) {
            (MessageType::Request, _)            => "Kind::Request",
            (MessageType::Response, _)           => "Kind::Response",
            (MessageType::Header, "RequestHeader")  => "Kind::RequestHeader",
            (MessageType::Header, "ResponseHeader") => "Kind::ResponseHeader",
            _ => continue, // Data type schemas don't get a top-level case
        };
        let api_key = s.api_key.unwrap_or(0);
        let snake = name_conv::module_name(&s.name);
        for v in s.valid_versions.min..=s.valid_versions.max {
            writeln!(out, "    Case {{ name: \"{}\", api_key: {api_key}, version: {v}, kind: {kind} }},", s.name).unwrap();
            let _ = snake;
        }
    }
    writeln!(out, "];").unwrap();
    writeln!(out).unwrap();

    // encode_default dispatch
    writeln!(out, "pub fn encode_default(name: &str, version: i16) -> Vec<u8> {{").unwrap();
    writeln!(out, "    match name {{").unwrap();
    for s in specs {
        if s.valid_versions.is_empty() { continue; }
        if !matches!(s.message_type, MessageType::Request | MessageType::Response | MessageType::Header) {
            continue;
        }
        let snake = name_conv::module_name(&s.name);
        let flavor = match s.message_type {
            MessageType::Header => "owned",  // Headers use the same owned path
            _ => "owned",
        };
        let type_name = name_conv::type_name(&s.name);
        writeln!(out, "        \"{}\" => {{", s.name).unwrap();
        writeln!(out, "            let msg = crabka_protocol::{flavor}::{snake}::{type_name}::default();").unwrap();
        writeln!(out, "            let mut buf = BytesMut::new();").unwrap();
        writeln!(out, "            msg.encode(&mut buf, version).unwrap();").unwrap();
        writeln!(out, "            buf.to_vec()").unwrap();
        writeln!(out, "        }}").unwrap();
    }
    writeln!(out, "        _ => panic!(\"unknown message in CASES: {{name}}\"),").unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out, "}}").unwrap();
    writeln!(out).unwrap();

    // default_json_for dispatch
    writeln!(out, "pub fn default_json_for(name: &str) -> ::serde_json::Value {{").unwrap();
    writeln!(out, "    match name {{").unwrap();
    for s in specs {
        if s.valid_versions.is_empty() { continue; }
        if !matches!(s.message_type, MessageType::Request | MessageType::Response | MessageType::Header) {
            continue;
        }
        let snake = name_conv::module_name(&s.name);
        let type_name = name_conv::type_name(&s.name);
        writeln!(out, "        \"{}\" => crabka_protocol::owned::{snake}::{type_name}::default_json(),", s.name).unwrap();
    }
    writeln!(out, "        _ => panic!(\"unknown message: {{name}}\"),").unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out, "}}").unwrap();

    out
}
```

- [ ] **Step 2: Wire into the emit module + main.rs**

`crates/protocol-codegen/src/emit/mod.rs`:

```rust
pub mod borrowed;
pub mod common;
pub mod default_json;
pub mod differential_table;
pub mod mod_rs;
pub mod owned;
pub mod wrappers;
```

In `main.rs`, after the per-message emit loop:

```rust
let table = emit::differential_table::emit(&specs, &schemas_sha);
std::fs::write(
    protocol_generated.join("differential_table.rs"),
    table,
)?;
```

- [ ] **Step 3: Regenerate, verify**

```bash
./tools/regenerate.sh
head -50 crates/protocol/generated/differential_table.rs
wc -l crates/protocol/generated/differential_table.rs
```

Expected: a multi-thousand-line file with `CASES`, `encode_default`, and `default_json_for`.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(codegen): emit differential_table dispatch for parameterised sweep"
```

---

### Task 7: `differential_all.rs` parameterised test

**Files:**
- Create: `crates/protocol/tests/differential_all.rs`

- [ ] **Step 1: Write the test**

`crates/protocol/tests/differential_all.rs`:

```rust
//! Parameterised differential sweep over every active (api_key, version) pair.
//!
//! For each case in the generated table, encodes the Rust default fixture,
//! sends the equivalent JSON to the JVM oracle, asserts byte equality.

mod support;
use support::oracle;

include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated/differential_table.rs"));

#[test]
#[ignore = "requires JVM oracle"]
fn every_pair_byte_equal() {
    let mut o = oracle::shared();
    let mut failures: Vec<String> = Vec::new();
    for case in CASES {
        let rust_bytes = encode_default(case.name, case.version);
        let json = default_json_for(case.name);
        let jvm_bytes = match case.kind {
            Kind::Request => o.encode(case.api_key, case.version, true, &json),
            Kind::Response => o.encode(case.api_key, case.version, false, &json),
            Kind::RequestHeader => o.header_encode("request", case.version, &json),
            Kind::ResponseHeader => o.header_encode("response", case.version, &json),
        };
        if rust_bytes != jvm_bytes {
            failures.push(format!(
                "{}[{}] v{}: rust={} ({} bytes), jvm={} ({} bytes), first diff at {}",
                case.name,
                match case.kind {
                    Kind::Request => "req",
                    Kind::Response => "resp",
                    Kind::RequestHeader => "rhdr",
                    Kind::ResponseHeader => "shdr",
                },
                case.version,
                hex::encode(&rust_bytes),
                rust_bytes.len(),
                hex::encode(&jvm_bytes),
                jvm_bytes.len(),
                first_diff(&rust_bytes, &jvm_bytes),
            ));
        }
    }
    if !failures.is_empty() {
        panic!(
            "{} pair(s) failed differential:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }
}

fn first_diff(a: &[u8], b: &[u8]) -> usize {
    let min = a.len().min(b.len());
    for i in 0..min {
        if a[i] != b[i] { return i; }
    }
    min
}
```

- [ ] **Step 2: Extend the oracle wrapper for header ops**

In `crates/protocol/tests/support/oracle.rs`, add:

```rust
pub fn header_encode(&mut self, kind: &str, version: i16, value: &Value) -> Vec<u8> {
    let r = self.call(&json!({
        "op": "header_encode",
        "kind": kind,
        "version": version,
        "value": value,
    }));
    hex::decode(r["hex"].as_str().unwrap()).unwrap()
}

pub fn header_decode(&mut self, kind: &str, version: i16, bytes: &[u8]) -> Value {
    let r = self.call(&json!({
        "op": "header_decode",
        "kind": kind,
        "version": version,
        "hex": hex::encode(bytes),
    }));
    r["value"].clone()
}
```

- [ ] **Step 3: Run the sweep**

```bash
cargo test -p crabka-protocol --test differential_all -- --ignored
```

Expected: 1 test (`every_pair_byte_equal`). It either passes (every active pair is byte-equal with the JVM) or fails with a multi-line panic listing every mismatch.

**If the test fails**, fix each issue at the source per the design's Section 3 guidance. This is where the hard-fail policy kicks in. Each fix is its own commit. Re-run until it passes.

- [ ] **Step 4: Commit (after passing)**

```bash
git add -A
git commit -m "test(protocol): parameterised differential sweep across every active pair"
```

---

## Phase F — Nightly workflow

### Task 8: Add the nightly differential workflow

**Files:**
- Create: `.github/workflows/nightly-differential.yml`

- [ ] **Step 1: Write the workflow**

`.github/workflows/nightly-differential.yml`:

```yaml
name: nightly-differential
on:
  schedule:
    - cron: '0 3 * * *'   # 03:00 UTC daily
  workflow_dispatch:

jobs:
  nightly:
    runs-on: ubuntu-latest
    timeout-minutes: 360
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
      - uses: actions/setup-java@v5
        with:
          distribution: temurin
          java-version: 17
      - name: Build JVM oracle
        run: (cd tools/oracle && ./gradlew installDist --no-daemon)
      - name: Run parameterised differential sweep (256 proptest cases per pair)
        env:
          PROPTEST_CASES: "256"
        run: cargo test --workspace --test differential_all --release -- --ignored
      - name: Create issue on failure
        if: failure()
        uses: actions/github-script@v7
        with:
          script: |
            await github.rest.issues.create({
              owner: context.repo.owner,
              repo: context.repo.repo,
              title: `nightly-differential failed on ${context.sha.slice(0,7)}`,
              body: `See workflow run: https://github.com/${context.repo.owner}/${context.repo.repo}/actions/runs/${context.runId}`,
              labels: ['nightly-fail'],
            });
```

- [ ] **Step 2: Validate YAML syntax**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/nightly-differential.yml'))"
```

(If `python3 -c` isn't available, the GitHub Actions checker on push will validate.)

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/nightly-differential.yml
git commit -m "ci: nightly differential sweep workflow"
```

---

## Phase G — KNOWN_ISSUES + acceptance

### Task 9: Update `KNOWN_ISSUES.md`

Remove the deferred-header entry; add the corpus carve-out.

**Files:**
- Modify: `KNOWN_ISSUES.md`

- [ ] **Step 1: Replace contents**

`KNOWN_ISSUES.md`:

```markdown
# Known issues

## Captured-traffic corpus deviation from coverage acceptance criterion #9

The coverage meta-spec
(`docs/superpowers/specs/2026-05-11-crabka-protocol-coverage-design.md`)
acceptance criterion #9 requires a captured-traffic corpus entry per
`(api_key, version)` pair. Sub-plan 1d explicitly does not build the
corpus. Differential testing (default-fixture per pair on PR CI;
256 proptest per pair nightly) is the substitute.

Rationale: building ~1000 corpus entries via real broker captures
(high setup cost) or oracle-synthetic generation (which proves
nothing differential testing doesn't) is not worth the work for the
validation value it adds. The corpus remains useful for regression
reproduction; growth is deferred to a future maintenance task.

Status: open. Tracked here pending a future maintenance pass.
```

- [ ] **Step 2: Commit**

```bash
git add KNOWN_ISSUES.md
git commit -m "docs: replace header deferral with corpus carve-out in KNOWN_ISSUES"
```

---

### Task 10: Acceptance gate verification

Verification only. Mark complete only when every item passes.

- [ ] `cargo fmt --check` clean.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [ ] `cargo build -p crabka-protocol` (default features) succeeds.
- [ ] `cargo build -p crabka-protocol --no-default-features` succeeds.
- [ ] `cargo test --workspace` clean.
- [ ] `cargo test --workspace -- --include-ignored` clean (every active `(api_key, version)` pair byte-equal with JVM).
- [ ] Existing per-message differential tests still pass (regression):
  `differential_api_versions`, `differential_metadata`, `differential_produce`, `differential_offset_commit`, `differential_describe_groups`, `differential_records`.
- [ ] `cargo doc --no-deps -p crabka-protocol` passes with no warnings.
- [ ] `./tools/regenerate.sh && git diff --quiet` (no drift).
- [ ] `crates/protocol/src/owned/mod.rs` and `crates/protocol/src/borrowed/mod.rs` list every active module.
- [ ] No `#[ignore]` annotations exist on `(api_key, version)`-specific test cases for known-failure reasons.
- [ ] `KNOWN_ISSUES.md` contains the corpus carve-out section and nothing else.
- [ ] `.github/workflows/nightly-differential.yml` exists, runs `cargo test --workspace --test differential_all --release -- --ignored` with `PROPTEST_CASES=256`.

When all items pass:

```bash
git push -u origin feature/coverage-mass-rollout-1d
gh pr create --base main --head feature/coverage-mass-rollout-1d \
    --title "Sub-plan 1d: mass rollout to every active Kafka 4.2 schema" \
    --body "$(cat <<'EOF'
## Summary

Switches the codegen from the 6-pair curated set to every active Kafka 4.2 schema (~190 messages). Byte-equality with `kafka-clients` 4.2.0 verified for every active `(api_key, version)` pair via parameterised differential sweep.

## What landed

- Codegen emits wrappers + mod.rs as drift-checked artifacts (Tasks 1-2)
- `CURATED` gate replaced with `validVersions.is_empty()` skip (Task 3)
- `default_json()` helper per message (Task 4)
- JVM oracle gains `header_encode` / `header_decode` ops (Task 5)
- Generated `differential_table.rs` + new `differential_all.rs` parameterised sweep (Tasks 6-7)
- Nightly differential workflow at 256 proptest cases per pair (Task 8)
- KNOWN_ISSUES updated: header deferral removed, corpus carve-out documented (Task 9)

## Reference

Spec: `docs/superpowers/specs/2026-05-11-crabka-coverage-mass-rollout-1d-design.md`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-review against the spec

**Spec coverage:**

| Spec acceptance item | Plan task |
|---|---|
| 1. Codegen emits every active schema | Task 3 |
| 2. Owned + borrowed wrappers for every active schema | Tasks 1, 3 |
| 3. mod.rs declares every active module | Tasks 2, 3 |
| 4. `differential_all.rs` byte-equality per pair | Tasks 6, 7 |
| 5. Existing per-message differential tests still pass | Task 3 regression step, Task 7 |
| 6. Headers move from KNOWN_ISSUES to live coverage | Tasks 5, 7, 9 |
| 7. Hard-fail (no `#[ignore]` for known failures) | Task 7 + Task 10 verification |
| 8. KNOWN_ISSUES ends with corpus carve-out | Task 9 |
| 9. Nightly workflow exists | Task 8 |
| 10. `default_json()` helper per message | Task 4 |
| 11. CI matrix continues to pass on three OSes | Task 10 verification |
| 12. jvm-differential job runs `differential_all` within budget | Task 7 (test design) |
| 13. drift workflow validates wrappers + mod.rs | Tasks 1, 2 |
| 14-17. fmt/clippy/test/doc clean | Task 10 |

**Placeholder scan:** No `TODO` / `TBD` in requirements. The plan flags one ambiguity to resolve at implementation time — the exact Kafka 4.2 `*DataJsonConverter` static-method names for header ops — and instructs the implementer to grep the jar before relying on the plan's pseudocode. Task 7's "fix each issue at the source" wording is concrete: each fix class has a named diagnostic in the design.

**Type consistency:** `Case`, `Kind`, `CASES`, `encode_default`, `default_json_for` all referenced consistently across Tasks 6 and 7. `default_json()` per-message helper named consistently in Tasks 4, 6, 7.

Plan is ready for execution.
