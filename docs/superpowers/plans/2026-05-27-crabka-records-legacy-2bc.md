# Records-legacy 2b+2c (legacy Produce/Fetch wire support) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Accept Produce v0–2 and Fetch v0–3 from legacy clients. Up-convert v0/v1 `MessageSet` payloads to v2 `RecordBatch` on the produce path; down-convert v2 batches to v0/v1 `MessageSet` on the fetch response path. Storage stays v2-only.

**Architecture:** Vendor Kafka 3.6.2 schemas for Produce/Fetch into `schemas/versions/kafka_3_6_2/`. Codegen gets a `--namespace` flag that emits into a parallel module tree (`protocol::kafka_3_6_2::{owned,borrowed}`). Handlers in `crates/broker/src/handlers/{produce,fetch}.rs` peek the version, decode through the legacy module for the low range, adapt to the canonical type via hand-written `From` impls in `crates/protocol/src/legacy_compat.rs`, and bridge records via `crabka-records-legacy::{legacy_to_v2, v2_to_legacy}`.

**Tech Stack:** Rust 1.95, existing `crabka-protocol-codegen`, `crabka-records-legacy` (v0/v1 codec — already merged), `crabka-compression` (zstd→snappy).

**Spec:** `docs/superpowers/specs/2026-05-27-crabka-records-legacy-2bc-design.md`

**Branch:** Create a new branch off `main` named `legacy-records-2bc`.

---

## Pre-flight: branch + worktree

- [ ] **Step 1: Create the branch on main**

```bash
git checkout main && git pull --ff-only
git checkout -b legacy-records-2bc
```

- [ ] **Step 2: Verify clean tree, on the new branch**

```bash
git status
git rev-parse --abbrev-ref HEAD
```

Expected: `nothing to commit, working tree clean`, branch `legacy-records-2bc`.

---

## Task 1: Vendor Kafka 3.6.2 schemas

**Files:**
- Create: `crates/protocol/schemas/versions/kafka_3_6_2/README.md`
- Create: `crates/protocol/schemas/versions/kafka_3_6_2/VERSION`
- Create: `crates/protocol/schemas/versions/kafka_3_6_2/ProduceRequest.json`
- Create: `crates/protocol/schemas/versions/kafka_3_6_2/ProduceResponse.json`
- Create: `crates/protocol/schemas/versions/kafka_3_6_2/FetchRequest.json`
- Create: `crates/protocol/schemas/versions/kafka_3_6_2/FetchResponse.json`

- [ ] **Step 1: Determine the upstream 3.6.2 SHA**

```bash
git ls-remote https://github.com/apache/kafka.git refs/tags/3.6.2 | awk '{print $1}'
```

Capture the SHA; you'll write it into `VERSION` in step 3.

- [ ] **Step 2: Fetch each schema verbatim from kafka.git@3.6.2**

For each of the four file names below, run:

```bash
mkdir -p crates/protocol/schemas/versions/kafka_3_6_2
for f in ProduceRequest.json ProduceResponse.json FetchRequest.json FetchResponse.json; do
  curl -sSfL "https://raw.githubusercontent.com/apache/kafka/3.6.2/clients/src/main/resources/common/message/$f" \
    -o "crates/protocol/schemas/versions/kafka_3_6_2/$f"
done
```

Expected: four files written, each non-empty, each starting with the Apache license header and containing `validVersions`.

- [ ] **Step 3: Write the VERSION manifest**

`crates/protocol/schemas/versions/kafka_3_6_2/VERSION`:

```
ref: 3.6.2
sha: <SHA-from-step-1>
synced_at: 2026-05-27T00:00:00Z
```

Substitute the SHA from step 1; keep the date set to the day of execution (UTC, ISO 8601).

- [ ] **Step 4: Write the README**

`crates/protocol/schemas/versions/kafka_3_6_2/README.md`:

```markdown
# kafka_3_6_2 schemas

Vendored verbatim from [apache/kafka@3.6.2](https://github.com/apache/kafka/tree/3.6.2/clients/src/main/resources/common/message).

These schemas declare the pre-Kafka-4.0 version ranges for Produce
(v0–9) and Fetch (v0–15). The crabka codegen emits this directory
into the `kafka_3_6_2` namespace so the broker can decode the
legacy-exclusive ranges (Produce v0–2, Fetch v0–3) that the
top-level 4.0 schemas no longer declare.

Do not hand-edit. To re-sync against a different upstream tag,
update `VERSION` and re-fetch with the commands in the plan
`2026-05-27-crabka-records-legacy-2bc.md`.
```

- [ ] **Step 5: Verify file contents**

```bash
ls -la crates/protocol/schemas/versions/kafka_3_6_2/
head -3 crates/protocol/schemas/versions/kafka_3_6_2/ProduceRequest.json
grep -E '"validVersions"' crates/protocol/schemas/versions/kafka_3_6_2/*.json
```

Expected: 6 files; license header on the JSON; `validVersions` ranges starting at `0`.

- [ ] **Step 6: Commit**

```bash
git add crates/protocol/schemas/versions/kafka_3_6_2/
git commit -m "vendor: Kafka 3.6.2 Produce/Fetch schemas under schemas/versions/kafka_3_6_2"
```

---

## Task 2: Add `--namespace` flag to the codegen

**Files:**
- Modify: `crates/protocol-codegen/src/main.rs:42-235` (CLI + run signature + path derivations)
- Modify: `crates/protocol-codegen/src/emit/wrappers.rs:80-95` (include! path)

The codegen today takes positional args `<schemas> <out>`. We add an optional `--namespace <name>` that, when set:
- Treats `out` as the namespace's generated dir (e.g. `crates/protocol/generated/kafka_3_6_2/`).
- Writes wrappers under `protocol_src/<namespace>/{owned,borrowed}/`.
- Emits `include!` paths of the form `/generated/<namespace>/<Type>.<flavor>.rs`.
- Writes the namespace's `mod.rs` files (`protocol_src/<namespace>/mod.rs` + the two flavor mods).

When `--namespace` is absent, behavior is unchanged.

- [ ] **Step 1: Pick up the namespace from argv**

In `crates/protocol-codegen/src/main.rs`, replace the existing arg parse with a small flag parser. Locate the existing `fn main()` and the `RunError::SchemaShaMissing` definition; add:

```rust
fn parse_args() -> (PathBuf, PathBuf, Option<String>) {
    let mut positional: Vec<String> = Vec::new();
    let mut namespace: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        if a == "--namespace" {
            namespace = Some(args.next().expect("--namespace requires a value"));
        } else {
            positional.push(a);
        }
    }
    assert_eq!(positional.len(), 2,
        "usage: codegen [--namespace NAME] <schemas> <out>");
    (PathBuf::from(&positional[0]), PathBuf::from(&positional[1]), namespace)
}
```

And rewrite the existing entry to call it:

```rust
fn main() -> Result<(), RunError> {
    let (schemas, out, namespace) = parse_args();
    let count = run(&schemas, &out, namespace.as_deref())?;
    eprintln!("Emitted {count} message specs.");
    Ok(())
}
```

- [ ] **Step 2: Thread `namespace` through `run`**

Change `fn run(schemas: &Path, out: &Path) -> Result<usize, RunError>` to `fn run(schemas: &Path, out: &Path, namespace: Option<&str>) -> Result<usize, RunError>`. Where `protocol_src_from_out(out)` is called, replace with the namespace-aware variant:

```rust
let protocol_src = match namespace {
    None => protocol_src_from_out(out),
    Some(ns) => out
        .parent().expect("out must have a parent")        // generated/
        .parent().expect("out parent must have a parent") // crates/protocol/
        .join("src")
        .join(ns),
};
```

When `namespace = Some("kafka_3_6_2")` and `out = .../crates/protocol/generated/kafka_3_6_2`, this resolves to `.../crates/protocol/src/kafka_3_6_2`. When `namespace = None`, behavior is unchanged.

- [ ] **Step 3: Thread `namespace` to wrapper emission**

`emit::wrappers::emit` and `write_wrapper`/`write_common_wrapper` currently bake the `include!` path as `"/generated/{type_name}.{suffix}.rs"`. Extend the function to accept the namespace and emit `"/generated/{namespace}/{type_name}.{suffix}.rs"` when set.

In `crates/protocol-codegen/src/emit/wrappers.rs`, change:

```rust
pub fn emit(spec: &ir::MessageSpec, flavor: Flavor, schemas_version: &str) -> String {
```

to:

```rust
pub fn emit(
    spec: &ir::MessageSpec,
    flavor: Flavor,
    schemas_version: &str,
    namespace: Option<&str>,
) -> String {
```

and update the body that constructs the `include!` line. Where you currently have something like:

```rust
"include!(concat!(\n    env!(\"CARGO_MANIFEST_DIR\"),\n    \"/generated/{type_name}.{suffix}.rs\"\n));"
```

use:

```rust
let path_prefix = match namespace {
    None => String::new(),
    Some(ns) => format!("{ns}/"),
};
format!(
    "include!(concat!(\n    env!(\"CARGO_MANIFEST_DIR\"),\n    \"/generated/{path_prefix}{type_name}.{suffix}.rs\"\n));"
)
```

Update both call sites in `main.rs` to pass the namespace through (`write_wrapper`, `write_common_wrapper`).

- [ ] **Step 4: Namespace the protocol-side `mod.rs`**

Today the codegen writes `protocol_src/owned/mod.rs` and `protocol_src/borrowed/mod.rs`. With a namespace these go to `protocol_src/owned/mod.rs` where `protocol_src` now points at `crates/protocol/src/kafka_3_6_2`. So the existing write logic at `main.rs:233-234` already does the right thing — the mod files land at `crates/protocol/src/kafka_3_6_2/owned/mod.rs`.

Additionally, write a namespace-level `mod.rs` that declares the two flavor mods:

```rust
if let Some(_ns) = namespace {
    let body = "pub mod owned;\npub mod borrowed;\n";
    std::fs::write(protocol_src.join("mod.rs"), body)?;
}
```

Add this near the end of `run`, alongside the existing `owned/mod.rs` / `borrowed/mod.rs` writes.

- [ ] **Step 5: Verify the codegen still works with no `--namespace`**

```bash
cargo run -p crabka-protocol-codegen -- crates/protocol/schemas crates/protocol/generated
git diff crates/protocol/generated crates/protocol/src/owned crates/protocol/src/borrowed
```

Expected: no diff (regenerating against the same schemas produces byte-identical output).

- [ ] **Step 6: Commit**

```bash
git add crates/protocol-codegen/src/main.rs crates/protocol-codegen/src/emit/wrappers.rs
git commit -m "codegen: --namespace flag for per-version emit"
```

---

## Task 3: Regenerate `kafka_3_6_2` module and wire it into `protocol/src/lib.rs`

**Files:**
- Modify: `tools/regenerate.sh` (add a second codegen invocation)
- Create: `crates/protocol/generated/kafka_3_6_2/*.{owned,borrowed}.rs` (codegen-emitted)
- Create: `crates/protocol/src/kafka_3_6_2/{owned,borrowed}/*.rs` (codegen-emitted wrappers)
- Create: `crates/protocol/src/kafka_3_6_2/mod.rs` (codegen-emitted namespace mod)
- Modify: `crates/protocol/src/lib.rs` (add `pub mod kafka_3_6_2;`)
- Modify: `crates/protocol/build.rs` (extend SHA check to cover the namespace too)

- [ ] **Step 1: Extend `tools/regenerate.sh` to emit the namespace**

Replace the contents of `tools/regenerate.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

cargo run -p crabka-protocol-codegen -- \
    crates/protocol/schemas \
    crates/protocol/generated

cargo run -p crabka-protocol-codegen -- \
    --namespace kafka_3_6_2 \
    crates/protocol/schemas/versions/kafka_3_6_2 \
    crates/protocol/generated/kafka_3_6_2

echo "Regenerated. Review the diff with: git diff crates/protocol/generated crates/protocol/src"
```

Keep the file executable (it already is).

- [ ] **Step 2: Run the regenerator**

```bash
./tools/regenerate.sh
```

Expected: prints "Emitted N message specs." twice (once for the top-level set, once for kafka_3_6_2). The kafka_3_6_2 count should be 4 (Produce req/resp, Fetch req/resp).

- [ ] **Step 3: Add the namespace to `protocol/src/lib.rs`**

Locate the existing `pub mod owned;` / `pub mod borrowed;` declarations in `crates/protocol/src/lib.rs` and add immediately after:

```rust
pub mod kafka_3_6_2;
```

- [ ] **Step 4: Extend `crates/protocol/build.rs` to validate the namespace's SHA**

Append to `crates/protocol/build.rs` after the existing top-level SHA check:

```rust
let ns_version = fs::read_to_string(
    root.join("schemas/versions/kafka_3_6_2/VERSION"),
).expect("schemas/versions/kafka_3_6_2/VERSION must exist");
let ns_sha = ns_version
    .lines()
    .find_map(|l| l.strip_prefix("sha: "))
    .expect("schemas/versions/kafka_3_6_2/VERSION must contain a `sha:` line");
let ns_one = fs::read_to_string(
    root.join("generated/kafka_3_6_2/ProduceRequest.owned.rs"),
).expect("generated/kafka_3_6_2/ProduceRequest.owned.rs must exist; run tools/regenerate.sh");
assert!(
    ns_one.contains(ns_sha),
    "generated/kafka_3_6_2/ProduceRequest.owned.rs was produced against a different SHA \
     ({ns_sha}). Run tools/regenerate.sh and commit the updated files."
);
println!("cargo:rerun-if-changed=schemas/versions/kafka_3_6_2/VERSION");
```

- [ ] **Step 5: Build and verify the new namespace is usable**

```bash
cargo build -p crabka-protocol
```

Expected: clean build. If unresolved-import errors appear from the new mod tree, re-run `./tools/regenerate.sh` and inspect the diff under `crates/protocol/src/kafka_3_6_2/`.

- [ ] **Step 6: Sanity-import the new types in a throwaway test, then delete**

```bash
cat > /tmp/sanity.rs <<'EOF'
fn _force_compile() {
    let _: crabka_protocol::kafka_3_6_2::owned::produce_request::ProduceRequest = Default::default();
    let _: crabka_protocol::kafka_3_6_2::owned::fetch_request::FetchRequest = Default::default();
}
EOF
```

Add this content to `crates/protocol/src/sanity_check.rs`, declare `#[cfg(test)] mod sanity_check;` in `lib.rs`, run `cargo build -p crabka-protocol --tests`, then revert both the file and the lib.rs change (this step is purely a compile probe).

```bash
rm crates/protocol/src/sanity_check.rs
# Remove the `#[cfg(test)] mod sanity_check;` line you added to lib.rs.
cargo build -p crabka-protocol --tests
```

Expected: still clean.

- [ ] **Step 7: Commit**

```bash
git add tools/regenerate.sh crates/protocol/build.rs crates/protocol/src/lib.rs \
    crates/protocol/src/kafka_3_6_2 crates/protocol/generated/kafka_3_6_2
git commit -m "protocol: emit kafka_3_6_2 namespace from vendored 3.6.2 schemas"
```

---

## Task 4: Type bridges in `legacy_compat.rs`

**Files:**
- Create: `crates/protocol/src/legacy_compat.rs`
- Modify: `crates/protocol/src/lib.rs` (declare `pub mod legacy_compat;`)
- Test: `crates/protocol/src/legacy_compat.rs` (`#[cfg(test)] mod tests` inline)

Hand-written `From` impls: legacy request → canonical, canonical response → legacy. Direction is asymmetric on purpose: requests arrive in the wire's flavor and the handler operates on the canonical type; responses are built canonical and serialized in the requester's flavor.

- [ ] **Step 1: Read the legacy and canonical struct shapes**

Read each pair end-to-end so the From impls map every field accurately:

```bash
ls crates/protocol/generated/kafka_3_6_2/
grep -n "pub struct ProduceRequest\|pub struct ProduceResponse" \
    crates/protocol/generated/ProduceRequest.owned.rs \
    crates/protocol/generated/ProduceResponse.owned.rs \
    crates/protocol/generated/kafka_3_6_2/ProduceRequest.owned.rs \
    crates/protocol/generated/kafka_3_6_2/ProduceResponse.owned.rs
grep -n "pub struct FetchRequest\|pub struct FetchResponse" \
    crates/protocol/generated/FetchRequest.owned.rs \
    crates/protocol/generated/FetchResponse.owned.rs \
    crates/protocol/generated/kafka_3_6_2/FetchRequest.owned.rs \
    crates/protocol/generated/kafka_3_6_2/FetchResponse.owned.rs
```

Take note of every field on the canonical side that is *not* present on the legacy side — those need explicit defaults. Likely defaults: `TransactionalId = None`, `topic_id = Uuid::nil()`, any v3+/v4+ field gets its struct `Default::default()`.

- [ ] **Step 2: Write the failing tests first**

`crates/protocol/src/legacy_compat.rs`:

```rust
//! Adapters between the canonical Produce/Fetch types and the
//! `kafka_3_6_2`-namespaced flavors emitted from the vendored 3.6.2
//! schemas. Used by the wire-router branches for Produce v0–2 and
//! Fetch v0–3.

use crate::kafka_3_6_2;
use crate::owned::{
    fetch_request::FetchRequest,
    fetch_response::FetchResponse,
    produce_request::ProduceRequest,
    produce_response::ProduceResponse,
};

mod produce;
mod fetch;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn produce_request_legacy_to_canonical_defaults_transactional_id_to_none() {
        let legacy = kafka_3_6_2::owned::produce_request::ProduceRequest {
            acks: 1,
            timeout_ms: 30_000,
            topic_data: vec![],
            ..Default::default()
        };
        let canonical: ProduceRequest = legacy.into();
        assert_eq!(canonical.acks, 1);
        assert_eq!(canonical.timeout_ms, 30_000);
        assert!(canonical.transactional_id.is_none());
    }

    #[test]
    fn produce_response_canonical_to_legacy_drops_modern_only_fields() {
        let canonical = ProduceResponse {
            throttle_time_ms: 17,
            responses: vec![],
            ..Default::default()
        };
        let legacy: kafka_3_6_2::owned::produce_response::ProduceResponse =
            canonical.into();
        assert_eq!(legacy.throttle_time_ms, 17);
    }

    #[test]
    fn fetch_request_legacy_to_canonical_defaults_cluster_id_and_topic_id() {
        let legacy = kafka_3_6_2::owned::fetch_request::FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            ..Default::default()
        };
        let canonical: FetchRequest = legacy.into();
        assert_eq!(canonical.max_wait_ms, 500);
        assert_eq!(canonical.min_bytes, 1);
        assert!(canonical.cluster_id.is_none());
    }

    #[test]
    fn fetch_response_canonical_to_legacy_preserves_top_level_fields() {
        let canonical = FetchResponse {
            throttle_time_ms: 42,
            error_code: 0,
            session_id: 0,
            responses: vec![],
            ..Default::default()
        };
        let legacy: kafka_3_6_2::owned::fetch_response::FetchResponse =
            canonical.into();
        assert_eq!(legacy.throttle_time_ms, 42);
    }
}
```

Add `pub mod legacy_compat;` to `crates/protocol/src/lib.rs`.

- [ ] **Step 3: Run the tests; verify they fail to compile**

```bash
cargo test -p crabka-protocol --lib legacy_compat 2>&1 | tail -20
```

Expected: `error[E0432]: unresolved import` or `error[E0277]` because the From impls don't exist yet.

- [ ] **Step 4: Write the four `From` impls**

Create `crates/protocol/src/legacy_compat/produce.rs` (mapping the field set you enumerated in Step 1; the snippets below give the structural skeleton — fill the nested `topic_data` and `partition_data` mappings by walking the field lists you read in Step 1):

```rust
use crate::kafka_3_6_2;
use crate::owned::produce_request::ProduceRequest;
use crate::owned::produce_response::ProduceResponse;

impl From<kafka_3_6_2::owned::produce_request::ProduceRequest> for ProduceRequest {
    fn from(legacy: kafka_3_6_2::owned::produce_request::ProduceRequest) -> Self {
        Self {
            transactional_id: None,
            acks: legacy.acks,
            timeout_ms: legacy.timeout_ms,
            topic_data: legacy.topic_data.into_iter().map(Into::into).collect(),
            ..Default::default()
        }
    }
}

impl From<kafka_3_6_2::owned::produce_request::TopicProduceData>
    for crate::owned::produce_request::TopicProduceData
{
    fn from(l: kafka_3_6_2::owned::produce_request::TopicProduceData) -> Self {
        Self {
            name: l.name,
            partition_data: l.partition_data.into_iter().map(Into::into).collect(),
            ..Default::default()  // topic_id (v13+) defaults to Uuid::nil()
        }
    }
}

impl From<kafka_3_6_2::owned::produce_request::PartitionProduceData>
    for crate::owned::produce_request::PartitionProduceData
{
    fn from(l: kafka_3_6_2::owned::produce_request::PartitionProduceData) -> Self {
        Self {
            index: l.index,
            records: l.records,    // RecordsPayload type matches across namespaces
            ..Default::default()
        }
    }
}

impl From<ProduceResponse>
    for kafka_3_6_2::owned::produce_response::ProduceResponse
{
    fn from(c: ProduceResponse) -> Self {
        Self {
            responses: c.responses.into_iter().map(Into::into).collect(),
            throttle_time_ms: c.throttle_time_ms,
            ..Default::default()
        }
    }
}

impl From<crate::owned::produce_response::TopicProduceResponse>
    for kafka_3_6_2::owned::produce_response::TopicProduceResponse
{
    fn from(c: crate::owned::produce_response::TopicProduceResponse) -> Self {
        Self {
            name: c.name,
            partition_responses: c.partition_responses.into_iter().map(Into::into).collect(),
            ..Default::default()
        }
    }
}

impl From<crate::owned::produce_response::PartitionProduceResponse>
    for kafka_3_6_2::owned::produce_response::PartitionProduceResponse
{
    fn from(c: crate::owned::produce_response::PartitionProduceResponse) -> Self {
        Self {
            index: c.index,
            error_code: c.error_code,
            base_offset: c.base_offset,
            log_append_time_ms: c.log_append_time_ms,
            log_start_offset: c.log_start_offset,
            ..Default::default()
        }
    }
}
```

Create `crates/protocol/src/legacy_compat/fetch.rs` with the symmetric Fetch impls. Use the same field-walk approach (Step 1 output). Top-level `From<kafka_3_6_2::FetchRequest> for FetchRequest`, plus the nested `Topic`/`Partition` types. Top-level `From<FetchResponse> for kafka_3_6_2::FetchResponse`, plus the nested `Topic`/`Partition` types.

If the field walk reveals a v4+ field on the canonical Partition response that has no v0-3 equivalent (e.g., diverging-epoch metadata), default it; the legacy response variant doesn't carry it.

Switch the inline `mod produce; mod fetch;` in `legacy_compat.rs` from inline modules to file-backed (already declared above).

- [ ] **Step 5: Run the tests; verify they pass**

```bash
cargo test -p crabka-protocol --lib legacy_compat 2>&1 | tail -10
```

Expected: `test result: ok. 4 passed; …`.

- [ ] **Step 6: Clippy on the new module**

```bash
cargo clippy -p crabka-protocol --lib --tests -- -D warnings
```

Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add crates/protocol/src/legacy_compat.rs crates/protocol/src/legacy_compat \
    crates/protocol/src/lib.rs
git commit -m "protocol: hand-written type bridges between kafka_3_6_2 and canonical Produce/Fetch"
```

---

## Task 5: Produce handler — legacy decode + up-conversion

**Files:**
- Modify: `crates/broker/src/handlers/produce.rs:35-80` (version-dispatch decode at handler entry)
- Modify: `crates/broker/src/handlers/produce.rs:275-285` (replace `RecordsPayload::Legacy => INVALID_REQUEST` arm)
- Modify: `crates/broker/src/handlers/produce.rs` (response encode for legacy versions)
- Test: `crates/broker/tests/legacy_produce.rs` (new integration test)

- [ ] **Step 1: Write the failing integration test (hand-crafted v0 Produce)**

Create `crates/broker/tests/legacy_produce.rs`:

```rust
//! End-to-end: a hand-crafted Produce v0 request goes through up-conversion
//! and lands on disk as a v2 RecordBatch. Fetching back via v4 should
//! return what we sent.

mod common;
use common::Client;

#[tokio::test]
async fn produce_v0_upconverts_and_is_readable_via_fetch_v4() {
    let client = common::start_broker_and_client().await;
    let topic = common::ensure_topic(&client, "legacy_produce_v0", 1).await;

    // Hand-craft a v0 MessageSet with a single message: key="k", value="v".
    let messageset_bytes = common::build_v0_messageset(&[("k", "v")]);

    // Send Produce v0. The wire frame is: api_key=0, api_version=0, …
    let resp = client.send_produce_v0(&topic, 0, messageset_bytes).await;
    assert_eq!(resp.error_code(0, 0), 0, "produce error: {:?}", resp);

    // Fetch v4 back; expect a v2 batch with the same key/value.
    let fetched = client.fetch_v4(&topic, 0, 0).await;
    let batches = fetched.batches_for(&topic, 0);
    assert_eq!(batches.len(), 1);
    let records: Vec<(Vec<u8>, Vec<u8>)> = batches[0].records.iter()
        .map(|r| (r.key.clone().unwrap_or_default().to_vec(),
                  r.value.clone().unwrap_or_default().to_vec()))
        .collect();
    assert_eq!(records, vec![(b"k".to_vec(), b"v".to_vec())]);
}
```

You will likely need to add `common::send_produce_v0`, `common::build_v0_messageset`, and `common::fetch_v4` to whatever harness `crates/broker/tests/` uses today. Inspect a sibling test (e.g., `crates/broker/tests/integration.rs`) to see what's available and extend `crates/broker/tests/common/` (create if missing) accordingly. Keep the harness extensions small and focused — wire bytes through the same TCP socket the existing tests use.

- [ ] **Step 2: Run the test; verify it fails for the right reason**

```bash
cargo test -p crabka-broker --test legacy_produce 2>&1 | tail -20
```

Expected: either a compile failure on the missing helpers (acceptable — implement them) OR a runtime failure where the broker returns `INVALID_REQUEST` because the legacy path isn't wired yet.

- [ ] **Step 3: Add version-dispatch at the Produce handler entry**

Replace the existing `let req = ProduceRequest::decode(&mut cur, version)?;` at `crates/broker/src/handlers/produce.rs:47` with:

```rust
let req: ProduceRequest = if (0..3).contains(&version) {
    crabka_protocol::kafka_3_6_2::owned::produce_request::ProduceRequest::decode(&mut cur, version)?
        .into()
} else {
    ProduceRequest::decode(&mut cur, version)?
};
```

`Into` here uses the `legacy_compat` adapter from Task 4. Add the `use crabka_protocol::legacy_compat as _;` import at the top of the file if needed to bring the impl into scope (`From` impls in scope make `.into()` work without an explicit name).

- [ ] **Step 4: Replace the `INVALID_REQUEST` arm with up-conversion**

At `crates/broker/src/handlers/produce.rs:275-281`, replace the existing match:

```rust
let mut batch = match payload {
    RecordsPayload::V2(rb) => rb,
    RecordsPayload::Legacy(_) => {
        out.error_code = codes::INVALID_REQUEST;
        return Ok(out);
    }
};
```

with:

```rust
let mut batch = match payload {
    RecordsPayload::V2(rb) => rb,
    RecordsPayload::Legacy(bytes) => match crabka_records_legacy::legacy_to_v2(&bytes) {
        Ok(rb) => rb,
        Err(e) => {
            tracing::warn!(error = %e, "legacy_to_v2 failed");
            out.error_code = codes::CORRUPT_MESSAGE;
            return Ok(out);
        }
    },
};
```

Confirm that `codes::CORRUPT_MESSAGE` exists — grep `crates/broker/src/codes.rs`; if the constant isn't there, add it as `pub const CORRUPT_MESSAGE: i16 = 2;` (Kafka error code 2). Also remove the stale comment block at `produce.rs:8` that says up-conversion is in a follow-up slice.

- [ ] **Step 5: Encode the response in the legacy flavor when the request was legacy**

At the response-encode site in `produce.rs`, where today the canonical `ProduceResponse` is encoded directly, branch:

```rust
if (0..3).contains(&version) {
    let legacy_resp: crabka_protocol::kafka_3_6_2::owned::produce_response::ProduceResponse =
        response.into();
    legacy_resp.encode(&mut out_buf, version)?;
} else {
    response.encode(&mut out_buf, version)?;
}
```

Use the `From<ProduceResponse> for kafka_3_6_2::ProduceResponse` impl from Task 4.

- [ ] **Step 6: Run the integration test**

```bash
cargo test -p crabka-broker --test legacy_produce 2>&1 | tail -20
```

Expected: pass.

- [ ] **Step 7: Run the broker suite to catch regressions**

```bash
cargo test -p crabka-broker --tests 2>&1 | tail -20
```

Expected: no new failures.

- [ ] **Step 8: Clippy**

```bash
cargo clippy -p crabka-broker --tests -- -D warnings
```

Expected: clean.

- [ ] **Step 9: Commit**

```bash
git add crates/broker/src/handlers/produce.rs crates/broker/tests/legacy_produce.rs \
    crates/broker/tests/common
git commit -m "broker: accept Produce v0-2 via kafka_3_6_2 decoder + up-convert legacy records"
```

---

## Task 6: Fetch handler — legacy decode + down-conversion + zstd→snappy

**Files:**
- Modify: `crates/broker/src/handlers/fetch.rs:69-100` (version-dispatch decode)
- Modify: `crates/broker/src/handlers/fetch.rs` (response assembly: down-convert per batch when version < 4)
- Modify: `crates/broker/src/handlers/fetch.rs` (response encode in legacy flavor)
- Create: `crates/broker/src/handlers/fetch_downconvert.rs` (the `down_convert_for_fetch` helper)
- Test: `crates/broker/tests/legacy_fetch.rs` (new)

- [ ] **Step 1: Write the failing integration test (hand-crafted v3 Fetch)**

Create `crates/broker/tests/legacy_fetch.rs`:

```rust
//! End-to-end: produce a v2 batch via the modern Produce path, then
//! Fetch v3 and expect a v0/v1 MessageSet on the wire that decodes
//! back to the same records. Includes a zstd-compressed batch case
//! that must come back as snappy.

mod common;
use common::Client;

#[tokio::test]
async fn fetch_v3_downconverts_v2_batch_to_v0_messageset() {
    let client = common::start_broker_and_client().await;
    let topic = common::ensure_topic(&client, "legacy_fetch_v3", 1).await;

    client.produce_v13(&topic, 0, &[("k1", "v1"), ("k2", "v2")]).await;

    let resp = client.fetch_v3(&topic, 0, 0).await;
    let bytes = resp.records_for(&topic, 0);
    let parsed = crabka_records_legacy::decode_message_set(&bytes).unwrap();
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = parsed.into_iter()
        .map(|r| (r.key.unwrap_or_default(), r.value.unwrap_or_default()))
        .collect();
    assert_eq!(pairs, vec![
        (b"k1".to_vec(), b"v1".to_vec()),
        (b"k2".to_vec(), b"v2".to_vec()),
    ]);
}

#[tokio::test]
async fn fetch_v3_recompresses_zstd_as_snappy() {
    let client = common::start_broker_and_client().await;
    let topic = common::ensure_topic(&client, "legacy_fetch_zstd", 1).await;

    client.produce_v13_compressed(&topic, 0, &[("a", "b"); 50],
        crabka_compression::Codec::Zstd).await;

    let resp = client.fetch_v3(&topic, 0, 0).await;
    let bytes = resp.records_for(&topic, 0);
    let codec = crabka_records_legacy::probe_compression(&bytes).unwrap();
    assert_eq!(codec, crabka_compression::Codec::Snappy);
}

#[tokio::test]
async fn fetch_v3_drops_control_records() {
    let client = common::start_broker_and_client().await;
    let topic = common::ensure_topic(&client, "legacy_fetch_ctrl", 1).await;

    client.write_v2_batch_with_control_record(&topic, 0,
        &[("real_key", "real_value")]).await;

    let resp = client.fetch_v3(&topic, 0, 0).await;
    let bytes = resp.records_for(&topic, 0);
    let parsed = crabka_records_legacy::decode_message_set(&bytes).unwrap();
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = parsed.into_iter()
        .map(|r| (r.key.unwrap_or_default(), r.value.unwrap_or_default()))
        .collect();
    // Only the non-control record survives.
    assert_eq!(pairs, vec![(b"real_key".to_vec(), b"real_value".to_vec())]);
}
```

The `probe_compression` helper may not exist in `records-legacy` today; add it as a thin function that inspects the magic+attributes bytes of the first message in a MessageSet. If easier, replace this assertion with a structural check (e.g., the outer wrapper-message attributes carry the snappy codec id `2`).

Extend `crates/broker/tests/common/` with `produce_v13`, `produce_v13_compressed`, `fetch_v3`, `write_v2_batch_with_control_record` as needed.

- [ ] **Step 2: Run; verify it fails**

```bash
cargo test -p crabka-broker --test legacy_fetch 2>&1 | tail -20
```

Expected: compile/runtime failures (decoder/version-dispatch not wired).

- [ ] **Step 3: Implement `down_convert_for_fetch`**

Create `crates/broker/src/handlers/fetch_downconvert.rs`:

```rust
//! Helpers for down-converting v2 `RecordBatch`es to v0/v1 `MessageSet`
//! bytes when the requester is on Fetch v<4. Control batches (txn
//! markers) are dropped entirely; zstd-compressed batches are
//! re-compressed as snappy (v0/v1 doesn't support zstd).

use crabka_compression::CompressionType;
use crabka_protocol::records::owned::RecordBatch;
use crabka_protocol::records::RecordsPayload;
use crabka_records_legacy::{Magic, v2_to_legacy};

use crate::codes;

/// Build the records-field payload for a single batch.
///
/// Returns `Ok(None)` when the entire batch is dropped (control batch on
/// the legacy path). Returns `Err(error_code)` for hard down-conversion
/// failures the caller should surface as a per-partition error.
pub(crate) fn down_convert_for_fetch(
    batch: &RecordBatch,
    request_version: i16,
) -> Result<Option<RecordsPayload>, i16> {
    if request_version >= 4 {
        return Ok(Some(RecordsPayload::V2(batch.clone())));
    }
    // Drop control batches entirely on the legacy path. Legacy clients
    // have no concept of control records (txn markers, etc.).
    if batch.attributes.is_control_batch() {
        return Ok(None);
    }
    let working = if batch.attributes.compression() == CompressionType::Zstd {
        let mut clone = batch.clone();
        clone.attributes = clone.attributes.with_compression(CompressionType::Snappy);
        clone
    } else {
        batch.clone()
    };
    // Fetch v0-1 → MessageSet magic 0; Fetch v2-3 → magic 1 (KIP-32 timestamps).
    let target = if request_version >= 2 { Magic::V1 } else { Magic::V0 };
    let bytes = v2_to_legacy(&working, target).map_err(|e| {
        tracing::warn!(error = %e, "v2_to_legacy failed during fetch down-conversion");
        codes::CORRUPT_MESSAGE
    })?;
    Ok(Some(RecordsPayload::Legacy(bytes)))
}
```

Add `pub(crate) mod fetch_downconvert;` to `crates/broker/src/handlers/mod.rs`.

Note on compression: `RecordBatch` keeps records uncompressed in memory after decode; setting the `attributes.compression()` to `Snappy` is enough — the next `encode` round-trips through snappy via `crabka_compression::compress`. No explicit decompress-then-recompress call needed.

- [ ] **Step 4: Wire `down_convert_for_fetch` into the Fetch response assembly**

In `crates/broker/src/handlers/fetch.rs`, locate the response-building loop that converts each read batch into a `RecordsPayload` for the response. For each batch, call:

```rust
match crate::handlers::fetch_downconvert::down_convert_for_fetch(&batch, version) {
    Ok(Some(payload)) => { /* push payload as-is for this batch */ }
    Ok(None) => { /* control batch dropped on legacy path; skip */ }
    Err(error_code) => {
        partition_response.error_code = error_code;
        // proceed without records for this partition
    }
}
```

If the existing loop collects per-batch `RecordsPayload`s and concatenates them into a single per-partition payload, accumulate the `Some(_)` payloads and concat their `Legacy` bytes (or wrap a single V2 payload, depending on shape). Read the surrounding 30–40 lines before editing to match the existing accumulation pattern.

- [ ] **Step 5: Version-dispatch the request decode**

At `crates/broker/src/handlers/fetch.rs:79`, replace:

```rust
let req = FetchRequest::decode(&mut cur, version)?;
```

with:

```rust
let req: FetchRequest = if (0..4).contains(&version) {
    crabka_protocol::kafka_3_6_2::owned::fetch_request::FetchRequest::decode(&mut cur, version)?
        .into()
} else {
    FetchRequest::decode(&mut cur, version)?
};
```

- [ ] **Step 6: Encode the response in the legacy flavor when the request was legacy**

Same pattern as Task 5 Step 5 but for Fetch:

```rust
if (0..4).contains(&version) {
    let legacy_resp: crabka_protocol::kafka_3_6_2::owned::fetch_response::FetchResponse =
        response.into();
    legacy_resp.encode(&mut out_buf, version)?;
} else {
    response.encode(&mut out_buf, version)?;
}
```

- [ ] **Step 7: Run the new integration tests**

```bash
cargo test -p crabka-broker --test legacy_fetch 2>&1 | tail -20
```

Expected: three tests pass.

- [ ] **Step 8: Run the full broker test suite**

```bash
cargo test -p crabka-broker --tests 2>&1 | tail -20
```

Expected: no new failures.

- [ ] **Step 9: Clippy**

```bash
cargo clippy -p crabka-broker --tests -- -D warnings
```

Expected: clean.

- [ ] **Step 10: Commit**

```bash
git add crates/broker/src/handlers/fetch.rs crates/broker/src/handlers/fetch_downconvert.rs \
    crates/broker/src/handlers/mod.rs crates/broker/tests/legacy_fetch.rs \
    crates/broker/tests/common
git commit -m "broker: accept Fetch v0-3 + down-convert v2 batches (zstd->snappy, drop control records)"
```

---

## Task 7: ApiVersions advertises the widened range

**Files:**
- Modify: `crates/broker/src/handlers/api_versions.rs:18-90` (broaden Produce/Fetch entries)

ApiVersions currently advertises `min_version = owned::produce_request::MIN_VERSION` (= 3) and `owned::fetch_request::MIN_VERSION` (= 4). After this slice the broker also serves v0–2 and v0–3 respectively. Override the min in the table.

- [ ] **Step 1: Write the failing test**

Add a test to `crates/broker/src/handlers/api_versions.rs` (or its companion test file if one exists):

```rust
#[test]
fn api_versions_advertises_legacy_produce_and_fetch_min() {
    let table = supported_apis();
    let produce = table.iter().find(|v| v.api_key == 0).expect("produce");
    let fetch = table.iter().find(|v| v.api_key == 1).expect("fetch");
    assert_eq!(produce.min_version, 0,
        "Produce min must be 0 to advertise the legacy v0-2 support");
    assert_eq!(fetch.min_version, 0,
        "Fetch min must be 0 to advertise the legacy v0-3 support");
}
```

Run:

```bash
cargo test -p crabka-broker --lib api_versions_advertises_legacy 2>&1 | tail -10
```

Expected: fail with `assertion left == right failed: 3 != 0` (Produce) or `4 != 0` (Fetch).

- [ ] **Step 2: Override the min in `supported_apis()`**

Locate the two `v!(produce_request)` / `v!(fetch_request)` entries in `crates/broker/src/handlers/api_versions.rs`. Replace each with an explicit entry:

```rust
ApiVersion {
    api_key: owned::produce_request::API_KEY,
    min_version: crabka_protocol::kafka_3_6_2::owned::produce_request::MIN_VERSION,
    max_version: owned::produce_request::MAX_VERSION,
    ..Default::default()
},
ApiVersion {
    api_key: owned::fetch_request::API_KEY,
    min_version: crabka_protocol::kafka_3_6_2::owned::fetch_request::MIN_VERSION,
    max_version: owned::fetch_request::MAX_VERSION,
    ..Default::default()
},
```

Pulling `min_version` from the legacy module means it tracks whatever the vendored schemas declare (currently 0). If we ever re-vendor, the advertisement stays correct without manual edits.

- [ ] **Step 3: Run the test**

```bash
cargo test -p crabka-broker --lib api_versions_advertises_legacy 2>&1 | tail -10
```

Expected: pass.

- [ ] **Step 4: Run the broker tests for ApiVersions interactions**

```bash
cargo test -p crabka-broker --tests api_versions 2>&1 | tail -10
```

Expected: any existing JVM-acceptance or wire tests that snapshot the ApiVersions response need to be updated to reflect the new mins. Update fixtures if the test reports a snapshot mismatch.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/api_versions.rs
git commit -m "broker: ApiVersions advertises Produce/Fetch min=0 now that legacy versions are served"
```

---

## Task 8: Codegen snapshot fixtures for `kafka_3_6_2`

**Files:**
- Modify: `crates/protocol-codegen/tests/snapshot.rs:1-90` (extend CURATED + helper that knows about namespaced schemas)
- Create: `crates/protocol-codegen/tests/snapshots/kafka_3_6_2/{Produce,Fetch}{Request,Response}.{owned,borrowed}.rs`

- [ ] **Step 1: Extend the snapshot test to cover the kafka_3_6_2 dir**

Add to `crates/protocol-codegen/tests/snapshot.rs` (alongside the existing `curated_owned_snapshots` and `curated_borrowed_snapshots`):

```rust
const CURATED_KAFKA_3_6_2: &[&str] = &[
    "ProduceRequest",
    "ProduceResponse",
    "FetchRequest",
    "FetchResponse",
];

fn ns_schemas_dir(ns: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent().unwrap()
        .join("protocol")
        .join("schemas")
        .join("versions")
        .join(ns)
}

fn ns_snap_dir(ns: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/snapshots")
        .join(ns)
}

#[test]
fn curated_owned_snapshots_kafka_3_6_2() {
    let specs = ir::load_dir(&ns_schemas_dir("kafka_3_6_2")).unwrap();
    for name in CURATED_KAFKA_3_6_2 {
        let spec = specs.iter().find(|s| s.name == *name).unwrap();
        let em = emit::owned::emit(spec, "test").unwrap();
        let base = ns_snap_dir("kafka_3_6_2");
        check(&base.join(format!("{name}.owned.rs")), &em.primary);
        for (cs_name, body) in &em.commons {
            check(&base.join(format!("common/{cs_name}.owned.rs")), body);
        }
    }
}

#[test]
fn curated_borrowed_snapshots_kafka_3_6_2() {
    let specs = ir::load_dir(&ns_schemas_dir("kafka_3_6_2")).unwrap();
    for name in CURATED_KAFKA_3_6_2 {
        let spec = specs.iter().find(|s| s.name == *name).unwrap();
        let em = emit::borrowed::emit(spec, "test").unwrap();
        let base = ns_snap_dir("kafka_3_6_2");
        check(&base.join(format!("{name}.borrowed.rs")), &em.primary);
        for (cs_name, body) in &em.commons {
            check(&base.join(format!("common/{cs_name}.borrowed.rs")), body);
        }
    }
}
```

- [ ] **Step 2: Generate the snapshot fixtures**

```bash
UPDATE_SNAPSHOTS=1 cargo test -p crabka-protocol-codegen --test snapshot \
    curated_owned_snapshots_kafka_3_6_2 curated_borrowed_snapshots_kafka_3_6_2
```

Expected: writes the new snapshot files under `crates/protocol-codegen/tests/snapshots/kafka_3_6_2/`.

- [ ] **Step 3: Rerun without UPDATE_SNAPSHOTS to confirm they match**

```bash
cargo test -p crabka-protocol-codegen --test snapshot 2>&1 | tail -10
```

Expected: all snapshot tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol-codegen/tests/snapshot.rs crates/protocol-codegen/tests/snapshots/kafka_3_6_2
git commit -m "test(codegen): snapshot fixtures for kafka_3_6_2 namespace"
```

---

## Task 9: Differential tests against the JVM oracle

**Files:**
- Create: `crates/protocol-codegen/tests/differential_produce_legacy.rs`
- Create: `crates/protocol-codegen/tests/differential_fetch_legacy.rs`

- [ ] **Step 1: Skim the existing differential test pattern**

```bash
ls crates/protocol-codegen/tests/differential_*.rs
head -80 crates/protocol-codegen/tests/differential_api_versions.rs
```

Note the harness (it ignores tests when the JVM oracle isn't available — `ignored, requires JVM oracle`). New tests follow the same gating.

- [ ] **Step 2: Add Produce v0/v1/v2 byte-equal tests**

`crates/protocol-codegen/tests/differential_produce_legacy.rs`:

```rust
//! Byte-equal differential tests: our kafka_3_6_2 Produce decoder/encoder
//! against the JVM oracle for v0, v1, v2.

mod common;
use common::jvm_oracle;

#[test]
fn produce_request_v0_byte_equal() {
    let Some(oracle) = jvm_oracle() else {
        eprintln!("ignored, requires JVM oracle");
        return;
    };
    let our_bytes = common::build_produce_v0();
    let jvm_bytes = oracle.encode_produce_request_v0();
    assert_eq!(our_bytes, jvm_bytes);
}

#[test]
fn produce_request_v1_byte_equal() { /* analogous, v1 */ }

#[test]
fn produce_request_v2_byte_equal() { /* analogous, v2 */ }
```

Mirror the call shape and helper module used by the existing `differential_*.rs` files; copy the `common.rs` setup if needed.

- [ ] **Step 3: Add Fetch v0/v1/v2/v3 byte-equal tests**

`crates/protocol-codegen/tests/differential_fetch_legacy.rs` — four tests, same pattern.

- [ ] **Step 4: Run locally (will skip if no oracle)**

```bash
cargo test -p crabka-protocol-codegen --test differential_produce_legacy 2>&1 | tail -10
cargo test -p crabka-protocol-codegen --test differential_fetch_legacy 2>&1 | tail -10
```

Expected locally: tests print `ignored, requires JVM oracle` (CI runs them against the real oracle).

- [ ] **Step 5: Commit**

```bash
git add crates/protocol-codegen/tests/differential_produce_legacy.rs \
    crates/protocol-codegen/tests/differential_fetch_legacy.rs
git commit -m "test(codegen): JVM-differential tests for legacy Produce v0-2 and Fetch v0-3"
```

---

## Execution batches (for parallel subagent dispatch)

Per CLAUDE.md, dispatch tasks in parallel batches where file sets don't overlap.

- **Batch A** (parallel): Task 1, Task 2 — Task 1 touches only `schemas/versions/kafka_3_6_2/`; Task 2 touches only `protocol-codegen/src/`. Zero overlap.
- **Batch B** (sequential after A): Task 3 — needs both the vendored schemas (Task 1) and the `--namespace` flag (Task 2) to run.
- **Batch C** (parallel after B): Task 4 (legacy_compat), Task 7 (api_versions), Task 8 (snapshot fixtures), Task 9 (differential tests). Each touches a disjoint file set.
- **Batch D** (sequential after C): Task 5 (Produce handler), then Task 6 (Fetch handler). Both extend `crates/broker/tests/common/`, so they conflict in test-harness shared code — run them sequentially.

---

## Final verification

- [ ] **Step 1: Full workspace build**

```bash
cargo build --workspace
```

Expected: clean.

- [ ] **Step 2: Full workspace test (--lib + integration)**

```bash
cargo test --workspace --lib
cargo test --workspace --tests
```

Expected: no regressions; the new tests added in Tasks 4, 5, 6, 7, 8, 9 all pass (Task 9 differential tests skip locally without the JVM oracle).

- [ ] **Step 3: Clippy across the touched crates**

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: clean.

- [ ] **Step 4: Fmt check**

```bash
cargo fmt --check
```

Expected: clean.

- [ ] **Step 5: Open PR**

```bash
git push -u origin legacy-records-2bc
gh pr create --title "Slice 2b+2c: legacy Produce v0-2 / Fetch v0-3 wire support" --body "$(cat <<'EOF'
## Summary

Combined slice **2b + 2c** of the v0/v1 down-conversion roadmap (slice
2a — `RecordsPayload` — shipped in #214).

- Vendors Kafka 3.6.2 Produce/Fetch schemas under
  `schemas/versions/kafka_3_6_2/`.
- Codegen learns a `--namespace` flag and emits the kafka_3_6_2
  module tree at `crates/protocol/{generated,src}/kafka_3_6_2/`.
- Hand-written `From` adapters in `crates/protocol/src/legacy_compat.rs`
  bridge the two flavors at the handler boundary.
- Produce handler decodes v0–2 via kafka_3_6_2 and up-converts
  `RecordsPayload::Legacy` (was `INVALID_REQUEST`).
- Fetch handler decodes v0–3 via kafka_3_6_2 and down-converts v2
  batches per request via the new `down_convert_for_fetch` helper.
  Control records are dropped; zstd batches are re-compressed as snappy.
- ApiVersions advertises the widened min for Produce/Fetch.

## What's deferred to slice 2d

JVM acceptance with `kafka-console-producer/consumer --producer-property
message.format=v1` against the broker.

## Test plan

- [x] cargo build --workspace
- [x] cargo test --workspace
- [x] cargo clippy --workspace --all-targets -- -D warnings
- [x] cargo fmt --check
- [ ] CI differential tests (Produce v0–2 / Fetch v0–3 against JVM oracle)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL printed; checks start running.
