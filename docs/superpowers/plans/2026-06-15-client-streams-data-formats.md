# client-streams data-formats guide + tested format pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Document `crabka-client-streams`' data formats with a getting-started guide and one worked, self-asserting JSON→Protobuf→Arrow→Polars→summary-Protobuf pipeline, plus an automated harness that builds/runs every documented example and guarantees the published docs contain exactly the tested code.

**Architecture:** The worked pipeline is a single self-contained Cargo example (`examples/format_pipeline.rs`) that boots an in-process broker **and** an in-process Schema Registry (real HTTP port), drives each stage with explicit produce/consume + the real serdes (deterministic — no streams-group timing), runs the columnar Polars stage via `BuiltColumnarTopology::run_batch`, and `assert!`s the result. Running it **is** the test. Smaller per-format examples compile-check each serde. The website guide embeds anchored regions from these example files via a new `crabka-docgen snippets` pass; a script + CI job build/run the examples and fail on doc drift.

**Tech Stack:** Rust (edition 2024), tokio, `crabka-client-streams` (`polars`/`arrow` features), `crabka-schema-serde` (JSON/Protobuf), `crabka-schema-registry` (in-process), `crabka-broker` (`test-helpers`), `crabka-client-{admin,producer,consumer}`, prost/prost-reflect (committed bindings), polars + arrow-rs, clap (docgen), bash + GitHub Actions.

**Design decision (recorded):** The approved spec described Stage A "via the StreamsApp DSL." For a *deterministic, self-asserting* test we instead drive the row stages (A, B, D) with explicit produce/consume against the live broker + registry, and showcase the idiomatic high-level DSL in a separate compile-checked example (`examples/format_dsl.rs`). This honors the spec's chosen realism ("real produce/fetch path", "the example is the test") and the spec's explicit license to use consume→produce bridges, while still documenting the DSL.

---

## File structure

Create:
- `crates/client-streams/examples/proto/orders.proto` — `OrderProto`, `OrderSummary` (package `demo`).
- `crates/client-streams/examples/gen/orders.rs` — committed prost + prost-reflect bindings.
- `crates/client-streams/examples/gen/file_descriptor_set.bin` — committed descriptor set.
- `crates/client-streams/examples/gen/regenerate.sh` — regeneration recipe.
- `crates/client-streams/examples/format_pipeline.rs` — the worked, self-asserting pipeline (defines `ArrowBlobCodec` inline).
- `crates/client-streams/examples/format_json.rs` — `JsonSerde` round-trip (no broker).
- `crates/client-streams/examples/format_arrow.rs` — `ArrowIpcSerde` round-trip (no broker).
- `crates/client-streams/examples/format_dsl.rs` — idiomatic `StreamsApp` DSL (compile-checked).
- `crates/docgen/src/snippets.rs` — markdown snippet-sync.
- `website/content/guide/streams.md` — getting-started + formats + worked pipeline.
- `tools/test-doc-examples.sh` — local/CI runner.

Modify:
- `crates/client-streams/Cargo.toml` — `[[example]]` entries + dev-deps.
- `crates/docgen/src/lib.rs`, `crates/docgen/src/main.rs` — wire in `snippets`.
- `.github/workflows/ci.yml` — `changes` filter output + `doc-examples` job.

**Batches** (non-overlapping file sets; dispatch each batch's tasks in parallel):
- **Batch 1:** Task 1 (proto/gen), Task 2 (Cargo.toml), Task 3 (docgen snippets).
- **Batch 2:** Task 4 (small format examples), Task 5 (format_pipeline).
- **Batch 3:** Task 6 (guide page).
- **Batch 4:** Task 7 (snippet sync — depends on 3,4,5,6).
- **Batch 5:** Task 8 (script), Task 9 (CI).

---

## Task 1: Protobuf schema + committed bindings

**Files:**
- Create: `crates/client-streams/examples/proto/orders.proto`
- Create: `crates/client-streams/examples/gen/regenerate.sh`
- Create: `crates/client-streams/examples/gen/orders.rs` (generated)
- Create: `crates/client-streams/examples/gen/file_descriptor_set.bin` (generated)

Mirrors the existing `examples/proto/order.proto` + `examples/gen/` pattern (no `build.rs`).

- [ ] **Step 1: Write the proto**

`crates/client-streams/examples/proto/orders.proto`:

```proto
syntax = "proto3";
package demo;

// Canonical order, produced by the JSON->proto stage.
message OrderProto {
  string order_id = 1;
  string user = 2;
  int64  amount_cents = 3;
  string currency = 4;
  int64  ts_ms = 5;
}

// Per-user rollup, produced by the Polars->proto stage.
message OrderSummary {
  string user = 1;
  int64  total_cents = 2;
  int64  order_count = 3;
}
```

- [ ] **Step 2: Write the regeneration recipe**

`crates/client-streams/examples/gen/regenerate.sh` (mirror the header of the existing `order.proto` recipe; this is the runnable procedure):

```bash
#!/usr/bin/env bash
# Regenerate the committed protobuf bindings from examples/proto/orders.proto.
# The crate has NO build.rs, so bindings (orders.rs + file_descriptor_set.bin)
# are generated once and committed. Requires protox (pure-Rust, no protoc).
#
#   cargo new --bin /tmp/gen-orders && cd /tmp/gen-orders
#   cargo add protox prost-build prost prost-reflect
#   cat > src/main.rs <<'EOF'
#   fn main() {
#       let repo = std::env::var("REPO").unwrap();
#       let proto = format!("{repo}/crates/client-streams/examples/proto/orders.proto");
#       let dir   = format!("{repo}/crates/client-streams/examples/proto");
#       let fds = protox::compile([proto], [dir]).unwrap();
#       std::fs::write("file_descriptor_set.bin", protox::prost::Message::encode_to_vec(&fds)).unwrap();
#       let pool = protox::prost_reflect::DescriptorPool::from_file_descriptor_set(fds.clone()).unwrap();
#       let mut cfg = prost_build::Config::new();
#       cfg.skip_protoc_run().out_dir(".");
#       for m in pool.all_messages() {
#           let f = m.full_name();
#           cfg.type_attribute(f, "#[derive(::prost_reflect::ReflectMessage)]")
#              .type_attribute(f, format!("#[prost_reflect(message_name = \"{f}\")]"))
#              .type_attribute(f, "#[prost_reflect(file_descriptor_set_bytes = \"crate::FILE_DESCRIPTOR_SET_BYTES\")]");
#       }
#       cfg.compile_fds(fds).unwrap();
#   }
#   EOF
#   REPO=<repo-root> cargo run   # produces demo.rs + file_descriptor_set.bin
#
# Then copy demo.rs -> orders.rs (prepend the @generated header) and
# file_descriptor_set.bin into this directory.
echo "See the comments in this script for the one-off regeneration recipe."
```

- [ ] **Step 3: Generate the bindings**

Run the recipe from Step 2 (set `REPO` to this repo's absolute root). It emits `demo.rs` and `file_descriptor_set.bin` in `/tmp/gen-orders`. Copy them in:

```bash
cp /tmp/gen-orders/file_descriptor_set.bin crates/client-streams/examples/gen/file_descriptor_set.bin
cp /tmp/gen-orders/demo.rs crates/client-streams/examples/gen/orders.rs
```

Then prepend this header to `crates/client-streams/examples/gen/orders.rs` (matching the existing `order.rs` header):

```rust
// @generated by protox from examples/proto/orders.proto — DO NOT EDIT.
// Regenerate: see crates/client-streams/examples/gen/regenerate.sh
```

Expected: `orders.rs` contains `pub struct OrderProto { pub order_id: ..., pub user: ..., pub amount_cents: i64, pub currency: ..., pub ts_ms: i64 }` and `pub struct OrderSummary { pub user: ..., pub total_cents: i64, pub order_count: i64 }`, each with `#[derive(::prost_reflect::ReflectMessage)]` and `#[prost_reflect(file_descriptor_set_bytes = "crate::FILE_DESCRIPTOR_SET_BYTES")]`.

- [ ] **Step 4: Verify the descriptor parses**

Quick sanity check (no crate build yet):

```bash
ls -l crates/client-streams/examples/gen/file_descriptor_set.bin
grep -c 'struct OrderProto\|struct OrderSummary' crates/client-streams/examples/gen/orders.rs
```

Expected: the `.bin` is non-empty; grep prints `2`.

- [ ] **Step 5: Commit**

```bash
chmod +x crates/client-streams/examples/gen/regenerate.sh
git add crates/client-streams/examples/proto/orders.proto crates/client-streams/examples/gen/
git commit -m "feat(client-streams): protobuf bindings for the format-pipeline example"
```

---

## Task 2: Cargo.toml — example entries + dev-deps

**Files:**
- Modify: `crates/client-streams/Cargo.toml`

- [ ] **Step 1: Add the dev-dependency for the in-process registry**

In `[dev-dependencies]` (the others — `crabka-broker` w/ `test-helpers`, `prost`, `prost-reflect`, `schemars`, `serde`, `serde_json`, `tempfile`, `tokio` — already exist), add:

```toml
crabka-schema-registry = { version = "0.3.6", path = "../schema-registry" }
crabka-client-admin = { version = "0.3.6", path = "../client-admin" }
```

(`crabka-client-{core,producer,consumer}`, `crabka-schema-serde`, `tokio-util` are already normal deps; `polars`, `polars-arrow`, `arrow` are optional deps enabled by the `polars`/`arrow` features.)

- [ ] **Step 2: Add the `[[example]]` entries**

Append after the existing `[[example]]` blocks:

```toml
[[example]]
name = "format_json"

[[example]]
name = "format_arrow"
required-features = ["arrow"]

[[example]]
name = "format_dsl"

[[example]]
name = "format_pipeline"
required-features = ["polars", "arrow"]
```

- [ ] **Step 3: Verify the manifest parses**

Run: `cargo metadata --no-deps --format-version 1 -q >/dev/null`
Expected: exits 0 (no examples exist yet, but the manifest is valid).

- [ ] **Step 4: Commit**

```bash
git add crates/client-streams/Cargo.toml
git commit -m "feat(client-streams): declare format examples + in-process SR dev-deps"
```

---

## Task 3: docgen `snippets` markdown-sync pass

**Files:**
- Create: `crates/docgen/src/snippets.rs`
- Modify: `crates/docgen/src/lib.rs`
- Modify: `crates/docgen/src/main.rs`
- Test: inline `#[cfg(test)]` in `crates/docgen/src/snippets.rs`

The pass scans markdown for `<!-- snippet: <relpath>#<anchor> -->` … `<!-- /snippet -->` blocks and rewrites the content between the markers with a fenced ```rust block holding the lines between `// docs:begin <anchor>` / `// docs:end <anchor>` in `crates/<relpath>` (markers stripped, common leading indentation trimmed). Idempotent.

- [ ] **Step 1: Write the failing test**

Create `crates/docgen/src/snippets.rs`:

```rust
//! Sync fenced code blocks in website markdown from anchored regions of source
//! files, so published docs contain exactly the tested example code.

use std::path::Path;

/// Extract the lines between `// docs:begin <anchor>` and `// docs:end <anchor>`
/// in `source`, stripping the markers and trimming common leading indentation.
///
/// # Errors
/// Returns an error string if either marker is missing.
pub fn extract(source: &str, anchor: &str) -> Result<String, String> {
    let begin = format!("docs:begin {anchor}");
    let end = format!("docs:end {anchor}");
    let mut lines = Vec::new();
    let mut inside = false;
    for line in source.lines() {
        if line.contains(&begin) {
            inside = true;
            continue;
        }
        if line.contains(&end) {
            if !inside {
                return Err(format!("anchor {anchor}: end before begin"));
            }
            let indent = lines
                .iter()
                .filter(|l: &&String| !l.trim().is_empty())
                .map(|l| l.len() - l.trim_start().len())
                .min()
                .unwrap_or(0);
            let body: Vec<String> = lines
                .iter()
                .map(|l: &String| if l.len() >= indent { l[indent..].to_string() } else { l.clone() })
                .collect();
            return Ok(body.join("\n"));
        }
        if inside {
            lines.push(line.to_string());
        }
    }
    Err(format!("anchor {anchor}: markers not found in source"))
}

/// Rewrite every `<!-- snippet: <relpath>#<anchor> --> ... <!-- /snippet -->`
/// block in `markdown` with the current code from `crates_dir/<relpath>`.
/// Returns the new markdown. Idempotent.
///
/// # Errors
/// Returns an error if a referenced source file or anchor cannot be read.
pub fn sync_markdown(markdown: &str, crates_dir: &Path) -> Result<String, String> {
    let mut out = String::new();
    let mut rest = markdown;
    while let Some(start) = rest.find("<!-- snippet:") {
        let (head, after) = rest.split_at(start);
        out.push_str(head);
        let close = after.find("-->").ok_or("unterminated snippet directive")?;
        let directive = &after[..close];
        let spec = directive
            .trim_start_matches("<!-- snippet:")
            .trim();
        let (relpath, anchor) = spec
            .split_once('#')
            .ok_or_else(|| format!("snippet directive missing '#': {spec}"))?;
        let end_marker = "<!-- /snippet -->";
        let body_start = &after[close + 3..];
        let body_end = body_start
            .find(end_marker)
            .ok_or("missing <!-- /snippet -->")?;
        let source = std::fs::read_to_string(crates_dir.join(relpath.trim()))
            .map_err(|e| format!("read {relpath}: {e}"))?;
        let code = extract(&source, anchor.trim())?;
        out.push_str(directive);
        out.push_str("-->\n");
        out.push_str("```rust\n");
        out.push_str(&code);
        out.push_str("\n```\n");
        out.push_str(end_marker);
        rest = &body_start[body_end + end_marker.len()..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_trims_indent_and_markers() {
        let src = "fn main() {\n    // docs:begin foo\n    let x = 1;\n    // docs:end foo\n}\n";
        assert_eq!(extract(src, "foo").unwrap(), "let x = 1;");
    }

    #[test]
    fn sync_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("c/examples")).unwrap();
        std::fs::write(
            dir.path().join("c/examples/e.rs"),
            "// docs:begin a\nlet y = 2;\n// docs:end a\n",
        )
        .unwrap();
        let md = "intro\n<!-- snippet: c/examples/e.rs#a -->\nOLD\n<!-- /snippet -->\nend\n";
        let once = sync_markdown(md, dir.path()).unwrap();
        let twice = sync_markdown(&once, dir.path()).unwrap();
        assert_eq!(once, twice);
        assert!(once.contains("```rust\nlet y = 2;\n```"));
        assert!(!once.contains("OLD"));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p crabka-docgen snippets:: 2>&1 | tail -5`
Expected: FAIL — `snippets` module not declared in `lib.rs` (compile error / unresolved module).

- [ ] **Step 3: Wire the module and a public driver into `lib.rs`**

In `crates/docgen/src/lib.rs`, add `pub mod snippets;` next to the existing `pub mod` lines, and add a driver that walks `website/content`:

```rust
/// Rewrite snippet blocks in every `.md` under `content_dir`, pulling code from
/// source files under `crates_dir`. Returns the number of files changed.
///
/// # Errors
/// Returns an error if a directory walk, file read/write, or snippet sync fails.
pub fn sync_snippets(content_dir: &std::path::Path, crates_dir: &std::path::Path) -> anyhow::Result<usize> {
    use std::fs;
    let mut changed = 0;
    let mut stack = vec![content_dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in fs::read_dir(&d)? {
            let path = entry?.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "md") {
                let before = fs::read_to_string(&path)?;
                let after = snippets::sync_markdown(&before, crates_dir)
                    .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
                if after != before {
                    fs::write(&path, after)?;
                    changed += 1;
                }
            }
        }
    }
    Ok(changed)
}
```

Ensure `tempfile` is a dev-dependency of `crates/docgen` (add `tempfile = { workspace = true }` under `[dev-dependencies]` if absent).

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p crabka-docgen snippets:: 2>&1 | tail -5`
Expected: PASS (`extract_trims_indent_and_markers`, `sync_is_idempotent`).

- [ ] **Step 5: Add the `Snippets` subcommand**

In `crates/docgen/src/main.rs`, add a variant to the `Command` enum:

```rust
    /// Sync fenced code blocks in website markdown from anchored source regions.
    Snippets {
        /// Website content dir to scan (default: website/content).
        #[arg(long, default_value = "website/content")]
        content: std::path::PathBuf,
        /// Crates dir snippet paths are relative to (default: crates).
        #[arg(long, default_value = "crates")]
        crates: std::path::PathBuf,
    },
```

And a match arm in `main`:

```rust
        Command::Snippets { content, crates } => {
            let n = crabka_docgen::sync_snippets(&content, &crates)?;
            eprintln!("synced snippets in {n} file(s)");
            Ok(())
        }
```

- [ ] **Step 6: Verify the CLI builds**

Run: `cargo run -p crabka-docgen -- snippets --help`
Expected: prints usage for the `snippets` subcommand (exit 0).

- [ ] **Step 7: Commit**

```bash
git add crates/docgen/
git commit -m "feat(docgen): snippets pass to sync markdown from anchored source regions"
```

---

## Task 4: Small per-format examples

**Files:**
- Create: `crates/client-streams/examples/format_json.rs`
- Create: `crates/client-streams/examples/format_arrow.rs`
- Create: `crates/client-streams/examples/format_dsl.rs`

These compile-check (and where broker-free, self-check) each serde and the DSL.

- [ ] **Step 1: Write `format_json.rs` (no broker; round-trips via a seeded cache)**

```rust
//! JsonSerde round-trip: a typed value <-> Confluent JSON-Schema wire bytes.
//! Run: cargo run -p crabka-client-streams --example format_json
use crabka_client_streams::SchemaSerde;
use crabka_client_streams::processor::serde::Serde;
use crabka_schema_serde::RegistryClient;
use crabka_schema_serde::cache::{CacheConfig, SchemaCache};
use crabka_schema_serde::format::json::JsonSerde;
use serde::{Deserialize, Serialize};

// docs:begin json-type
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
struct OrderEvent {
    order_id: String,
    user: String,
    amount: f64,
    currency: String,
    ts_ms: i64,
}
// docs:end json-type

fn main() {
    // docs:begin json-roundtrip
    let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
    cache.seed_subject_id("orders.json-value", 1);
    let serde = SchemaSerde::new(JsonSerde::<OrderEvent>::value(&cache, false));

    let event = OrderEvent {
        order_id: "o-1".into(),
        user: "alice".into(),
        amount: 5.0,
        currency: "USD".into(),
        ts_ms: 1,
    };
    let bytes = serde.serialize("orders.json", &event);
    let back: OrderEvent = serde.deserialize("orders.json", &bytes).unwrap();
    // docs:end json-roundtrip
    assert_eq!(back, event);
    println!("format_json: OK ({} bytes)", bytes.len());
}
```

If `SchemaCache` lacks a `seed_subject_id`/seed helper usable without a live registry for JSON, mirror the exact seeding idiom from `crates/client-streams/tests/schema_serde_bridge.rs` (which seeds `seed_subject_id` + `seed_writer_schema`). Use whatever that test does verbatim.

- [ ] **Step 2: Write `format_arrow.rs` (no broker)**

```rust
//! ArrowIpcSerde round-trip for an arrow-rs RecordBatch.
//! Run: cargo run -p crabka-client-streams --example format_arrow --features arrow
use crabka_client_streams::columnar::serde::arrow::ArrowIpcSerde;
use crabka_client_streams::processor::serde::Serde;
use std::sync::Arc;

use ::arrow::array::{Int64Array, StringArray};
use ::arrow::datatypes::{DataType, Field, Schema};
use ::arrow::record_batch::RecordBatch;

fn main() {
    // docs:begin arrow-roundtrip
    let schema = Arc::new(Schema::new(vec![
        Field::new("user", DataType::Utf8, false),
        Field::new("amount_cents", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob"])),
            Arc::new(Int64Array::from(vec![850_i64, 900])),
        ],
    )
    .unwrap();
    let bytes = ArrowIpcSerde.serialize("orders.arrow", &batch);
    let back = ArrowIpcSerde.deserialize("orders.arrow", &bytes).unwrap();
    // docs:end arrow-roundtrip
    assert_eq!(back.num_rows(), 2);
    assert_eq!(back, batch);
    println!("format_arrow: OK ({} bytes)", bytes.len());
}
```

- [ ] **Step 3: Write `format_dsl.rs` (idiomatic high-level DSL; compile-checked)**

This shows the `StreamsApp` DSL form documented in the getting-started section. It must compile; it is not run against a live group in CI (no `main` execution path that blocks).

```rust
//! Idiomatic high-level Streams DSL over schema serdes (compile-checked).
//! Reads JSON `OrderEvent`s, normalizes to a Protobuf `OrderProto`, and writes
//! them out. Requires an external broker + registry to actually run; CI only
//! builds it. Run: cargo run -p crabka-client-streams --example format_dsl
use crabka_client_streams::{DefaultSerde, SchemaSerde, StreamsApp};
use crabka_schema_serde::format::json::JsonSerde;
use crabka_schema_serde::format::protobuf::ProtobufSerde;
use serde::{Deserialize, Serialize};

pub const FILE_DESCRIPTOR_SET_BYTES: &[u8] = include_bytes!("gen/file_descriptor_set.bin");
mod orders {
    include!("gen/orders.rs");
}
use orders::OrderProto;

#[derive(Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct OrderEvent {
    order_id: String,
    user: String,
    amount: f64,
    currency: String,
    ts_ms: i64,
}

// docs:begin dsl-defaultserde
impl DefaultSerde for OrderEvent {
    type Serde = SchemaSerde<OrderEvent, JsonSerde<OrderEvent>>;
}
impl DefaultSerde for OrderProto {
    type Serde = SchemaSerde<OrderProto, ProtobufSerde<OrderProto>>;
}
// docs:end dsl-defaultserde

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // docs:begin dsl-topology
    let app = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("orders-formats")
        .schema_registry("http://127.0.0.1:8081")
        .build();

    let topology = app.streams_builder();
    topology
        .stream::<String, OrderEvent>(["orders.json"])
        .map_values(|e: &OrderEvent| OrderProto {
            order_id: e.order_id.clone(),
            user: e.user.clone(),
            amount_cents: (e.amount * 100.0).round() as i64,
            currency: e.currency.to_uppercase(),
            ts_ms: e.ts_ms,
        })
        .to("orders.proto");
    // docs:end dsl-topology

    if std::env::var("RUN_DSL").is_err() {
        // Build-only by default so CI can compile this without a live broker.
        let _ = topology.build("orders-formats")?;
        println!("format_dsl: built (set RUN_DSL=1 with a live broker to run)");
        return Ok(());
    }
    let mut streams = app.run(topology).await?;
    streams.close().await?;
    Ok(())
}
```

If `DefaultSerde` for `OrderEvent`'s `JsonSerde` requires a `Default` registry (it does — `DefaultSerde::Serde: Default`), keep the build-only path guarded by `RUN_DSL` and ensure `topology.build(..)` does not require a live registry (it does not — building only validates the graph). If `streams_builder()` returns a value that must be consumed by `build`, take `topology` by value at `build` as shown.

- [ ] **Step 4: Build and run the broker-free examples**

Run:
```bash
cargo run -p crabka-client-streams --example format_json
cargo run -p crabka-client-streams --example format_arrow --features arrow
cargo build -p crabka-client-streams --example format_dsl
cargo run -p crabka-client-streams --example format_dsl
```
Expected: `format_json: OK ...`, `format_arrow: OK ...`, and `format_dsl: built ...`. Fix any serde-accessor/type mismatches against the real signatures before proceeding.

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/examples/format_json.rs crates/client-streams/examples/format_arrow.rs crates/client-streams/examples/format_dsl.rs
git commit -m "feat(client-streams): per-format examples (json, arrow, dsl)"
```

---

## Task 5: The worked pipeline example (self-asserting)

**Files:**
- Create: `crates/client-streams/examples/format_pipeline.rs`

Boots an in-process broker + in-process Schema Registry, runs JSON→proto→arrow→Polars→summary-proto with explicit produce/consume, and asserts the per-user rollup. Running it is the test.

- [ ] **Step 1: Write the example**

`crates/client-streams/examples/format_pipeline.rs`:

```rust
//! End-to-end multi-format Streams pipeline, self-contained and self-asserting:
//! JSON -> Protobuf -> Arrow -> columnar Polars -> summary Protobuf, against an
//! in-process broker + in-process Schema Registry (no external services).
//!
//! Run: cargo run -p crabka-client-streams --example format_pipeline --features polars,arrow
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_client_streams::SchemaSerde;
use crabka_client_streams::columnar::serde::arrow::ArrowIpcSerde;
use crabka_client_streams::columnar::serde::polars::PolarsIpcSerde;
use crabka_client_streams::columnar::topology::ColumnarTopology;
use crabka_client_streams::columnar::topology::codec::{
    BatchCodec, BatchError, BlobCodec, ConsumedRecord, ProduceRecord,
};
use crabka_client_streams::columnar::topology::operator::BuiltinOp;
use crabka_client_streams::processor::serde::Serde;
use crabka_schema_registry::config::{RegistryConfig, SecurityConfig};
use crabka_schema_registry::kafkastore::KafkaStore;
use crabka_schema_registry::rest::{self, AppState};
use crabka_schema_serde::RegistryClient;
use crabka_schema_serde::cache::{CacheConfig, SchemaCache};
use crabka_schema_serde::format::json::JsonSerde;
use crabka_schema_serde::format::protobuf::ProtobufSerde;
use polars::prelude::*;
use tokio_util::sync::CancellationToken;

use ::arrow::array::{Int64Array, StringArray};
use ::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use ::arrow::record_batch::RecordBatch;

// docs:begin types
/// Raw order, ingested as JSON (JSON-Schema serde).
#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
struct OrderEvent {
    order_id: String,
    user: String,
    amount: f64,
    currency: String,
    ts_ms: i64,
}
// docs:end types

// Protobuf messages generated from examples/proto/orders.proto.
pub const FILE_DESCRIPTOR_SET_BYTES: &[u8] = include_bytes!("gen/file_descriptor_set.bin");
mod orders {
    include!("gen/orders.rs");
}
use orders::{OrderProto, OrderSummary};

// docs:begin arrow-codec
/// Source codec: each Kafka record value is an Arrow-IPC `RecordBatch`; decode
/// them into one Polars `DataFrame` the columnar engine can process. Bridges
/// arrow-rs -> polars explicitly (different Arrow memory libraries).
struct ArrowBlobCodec;

impl BatchCodec for ArrowBlobCodec {
    fn decode(&self, records: &[ConsumedRecord]) -> Result<DataFrame, BatchError> {
        let mut users: Vec<String> = Vec::new();
        let mut cents: Vec<i64> = Vec::new();
        for (i, rec) in records.iter().enumerate() {
            let batch = ArrowIpcSerde
                .deserialize("", &rec.value)
                .map_err(|e| BatchError(format!("arrow decode rec {i}: {e}")))?;
            let user_col = batch
                .column_by_name("user")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| BatchError("missing user column".into()))?;
            let cent_col = batch
                .column_by_name("amount_cents")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| BatchError("missing amount_cents column".into()))?;
            for row in 0..batch.num_rows() {
                users.push(user_col.value(row).to_string());
                cents.push(cent_col.value(row));
            }
        }
        df!("user" => users, "amount_cents" => cents).map_err(|e| BatchError(e.to_string()))
    }

    fn encode(&self, _df: &DataFrame) -> Result<Vec<ProduceRecord>, BatchError> {
        Err(BatchError("ArrowBlobCodec is source-only".into()))
    }
}
// docs:end arrow-codec

struct Boot {
    _broker: BrokerHandle,
    bootstrap: String,
    registry_url: String,
    cancel: CancellationToken,
    _dir: tempfile::TempDir,
}

// docs:begin setup
async fn boot() -> Boot {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();

    // In-process Schema Registry over a real HTTP port.
    let cancel = CancellationToken::new();
    let cfg = RegistryConfig {
        bootstrap: bootstrap.clone(),
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: 1,
        client_id: "format-pipeline-sr".into(),
        advertised_url: "http://127.0.0.1:0".into(),
        group_id: "schema-registry".into(),
        leader_eligibility: true,
        security: SecurityConfig::default(),
    };
    let store = KafkaStore::start(&cfg, cancel.clone()).await.expect("sr start");
    let app = rest::router(AppState { store });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind sr");
    let sr_addr = listener.local_addr().expect("sr addr");
    let serve_cancel = cancel.clone();
    tokio::spawn(async move {
        let _ = rest::serve::serve_http(listener, app, serve_cancel).await;
    });

    Boot {
        _broker: broker,
        bootstrap,
        registry_url: format!("http://{sr_addr}"),
        cancel,
        _dir: dir,
    }
}
// docs:end setup

async fn send_record(producer: &Producer, topic: &str, value: Bytes) {
    producer
        .send(ProducerRecord { topic: topic.into(), value: Some(value), ..Default::default() })
        .await
        .await
        .expect("send recv")
        .expect("send ack");
}

/// Poll a fresh consumer group until `want` records arrive (bounded retries).
async fn drain(bootstrap: &str, topic: &str, group: &str, want: usize) -> Vec<Bytes> {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id(group)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([topic.to_string()])
        .build()
        .await
        .expect("consumer build");
    let mut out = Vec::new();
    for _ in 0..60 {
        if out.len() >= want {
            break;
        }
        let recs = consumer.poll(Duration::from_millis(500)).await.expect("poll");
        for r in recs {
            if let Some(v) = r.value {
                out.push(v);
            }
        }
    }
    assert!(out.len() >= want, "drain {topic}: got {} want {want}", out.len());
    out
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let boot = boot().await;
    let bootstrap = boot.bootstrap.clone();

    let mut admin = AdminClient::connect(&[bootstrap.clone()]).await.expect("admin");
    for t in ["orders.json", "orders.proto", "orders.arrow", "orders.summary"] {
        admin
            .create_topics(
                &[CreateTopicSpec { name: t.into(), partitions: 1, replicas: 1, configs: BTreeMap::new() }],
                5_000,
            )
            .await
            .expect("create topic");
    }

    let cache = SchemaCache::new(RegistryClient::new(&boot.registry_url), CacheConfig::default());
    let json_serde = SchemaSerde::new(JsonSerde::<OrderEvent>::value(&cache, false));
    let proto_serde = SchemaSerde::new(ProtobufSerde::<OrderProto>::value(&cache));
    let summary_serde = SchemaSerde::new(ProtobufSerde::<OrderSummary>::value(&cache));

    let producer = Producer::builder().bootstrap(&bootstrap).acks(Acks::All).build().await.expect("producer");

    // Seed orders.json (alice: 5.00 + 3.50; bob: 9.00).
    let events = vec![
        OrderEvent { order_id: "o1".into(), user: "alice".into(), amount: 5.00, currency: "usd".into(), ts_ms: 1 },
        OrderEvent { order_id: "o2".into(), user: "alice".into(), amount: 3.50, currency: "usd".into(), ts_ms: 2 },
        OrderEvent { order_id: "o3".into(), user: "bob".into(), amount: 9.00, currency: "usd".into(), ts_ms: 3 },
    ];
    for e in &events {
        let bytes = json_serde.serialize("orders.json", e);
        send_record(&producer, "orders.json", bytes).await;
    }
    producer.flush().await.expect("flush json");

    // docs:begin stage-a-json-proto
    // Stage A — JSON -> Protobuf: deserialize JSON, normalize, emit OrderProto.
    for v in drain(&bootstrap, "orders.json", "stage-a", events.len()).await {
        let ev: OrderEvent = json_serde.deserialize("orders.json", &v).expect("json decode");
        let proto = OrderProto {
            order_id: ev.order_id,
            user: ev.user,
            amount_cents: (ev.amount * 100.0).round() as i64,
            currency: ev.currency.to_uppercase(),
            ts_ms: ev.ts_ms,
        };
        let bytes = proto_serde.serialize("orders.proto", &proto);
        send_record(&producer, "orders.proto", bytes).await;
    }
    producer.flush().await.expect("flush proto");
    // docs:end stage-a-json-proto

    // docs:begin stage-b-proto-arrow
    // Stage B — Protobuf -> Arrow: collect rows into one arrow-rs RecordBatch.
    let mut users = Vec::new();
    let mut cents = Vec::new();
    for v in drain(&bootstrap, "orders.proto", "stage-b", events.len()).await {
        let p: OrderProto = proto_serde.deserialize("orders.proto", &v).expect("proto decode");
        users.push(p.user);
        cents.push(p.amount_cents);
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("user", ArrowDataType::Utf8, false),
        Field::new("amount_cents", ArrowDataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(users)), Arc::new(Int64Array::from(cents))],
    )
    .expect("record batch");
    send_record(&producer, "orders.arrow", ArrowIpcSerde.serialize("orders.arrow", &batch)).await;
    producer.flush().await.expect("flush arrow");
    // docs:end stage-b-proto-arrow

    // docs:begin stage-c-arrow-polars
    // Stage C — Arrow -> Polars: group-by-user sum + count in the columnar engine.
    let consumed: Vec<ConsumedRecord> = drain(&bootstrap, "orders.arrow", "stage-c", 1)
        .await
        .into_iter()
        .enumerate()
        .map(|(i, v)| ConsumedRecord { key: None, value: v, timestamp: 0, partition: 0, offset: i as i64 })
        .collect();

    let mut topo = ColumnarTopology::new();
    let src = topo.add_source("src", ["orders.arrow"], ArrowBlobCodec);
    let agg = topo.add_operator(
        "agg",
        BuiltinOp::GroupByAgg {
            keys: vec![col("user")],
            aggs: vec![
                col("amount_cents").sum().alias("total_cents"),
                col("amount_cents").count().alias("order_count"),
            ],
        },
        src,
    );
    topo.add_sink("out", "orders.summary.df", BlobCodec::default(), agg);
    let built = topo.build().expect("build columnar");
    let produced = built.run_batch("orders.arrow", &consumed).expect("run_batch");
    // docs:end stage-c-arrow-polars

    // docs:begin stage-d-polars-proto
    // Stage D — Polars -> Protobuf: each aggregated row becomes an OrderSummary.
    for (_topic, rec) in produced {
        let df = PolarsIpcSerde.deserialize("orders.summary.df", &rec.value).expect("polars decode");
        let user_col = df.column("user").expect("user");
        let total_col = df.column("total_cents").expect("total_cents");
        let count_col = df
            .column("order_count")
            .expect("order_count")
            .cast(&DataType::Int64)
            .expect("cast count");
        for i in 0..df.height() {
            let summary = OrderSummary {
                user: extract_str(user_col, i),
                total_cents: extract_i64(total_col, i),
                order_count: extract_i64(&count_col, i),
            };
            let bytes = summary_serde.serialize("orders.summary", &summary);
            send_record(&producer, "orders.summary", bytes).await;
        }
    }
    producer.flush().await.expect("flush summary");
    // docs:end stage-d-polars-proto

    // docs:begin assert
    // Verify the per-user rollup off the wire.
    let mut by_user = BTreeMap::new();
    for v in drain(&bootstrap, "orders.summary", "verify", 2).await {
        let s: OrderSummary = summary_serde.deserialize("orders.summary", &v).expect("summary decode");
        by_user.insert(s.user.clone(), s);
    }
    let alice = by_user.get("alice").expect("alice summary");
    assert_eq!(alice.total_cents, 850, "alice total_cents");
    assert_eq!(alice.order_count, 2, "alice order_count");
    let bob = by_user.get("bob").expect("bob summary");
    assert_eq!(bob.total_cents, 900, "bob total_cents");
    assert_eq!(bob.order_count, 1, "bob order_count");
    // docs:end assert

    boot.cancel.cancel();
    println!("format_pipeline: OK");
}
```

The two helpers `extract_str`/`extract_i64` isolate the one polars-version-sensitive idiom (reading a value out of a `Column`/`Series`). Implement them by copying the exact column-access idiom already used in `crates/client-streams/src/columnar/topology/row_bridge.rs` (e.g. `.str()` / `.i64()` ChunkedArray accessors with `.get(i)`):

```rust
fn extract_str(col: &polars::prelude::Column, i: usize) -> String {
    col.str().expect("utf8 column").get(i).unwrap_or("").to_string()
}
fn extract_i64(col: &polars::prelude::Column, i: usize) -> i64 {
    col.i64().expect("i64 column").get(i).unwrap_or(0)
}
```

If the pinned polars exposes `&Series` rather than `&Column` from `DataFrame::column`, change the parameter type to `&polars::prelude::Series` to match `row_bridge.rs`. Verify against that file before building.

- [ ] **Step 2: Build the example**

Run: `cargo build -p crabka-client-streams --example format_pipeline --features polars,arrow 2>&1 | tail -30`
Expected: compiles. Resolve any signature mismatches against the real APIs (producer `send` double-await, `Column` vs `Series`, `BuiltinOp::GroupByAgg`, `run_batch`) — these are the known churn points; fix in place.

- [ ] **Step 3: Run the example (this is the test)**

Run: `cargo run -p crabka-client-streams --example format_pipeline --features polars,arrow 2>&1 | tail -20`
Expected: ends with `format_pipeline: OK` and exit code 0. If it hangs in `drain`, increase the retry bound or confirm topic creation succeeded; if an assert fires, the printed left/right shows which stage is wrong.

- [ ] **Step 4: Commit**

```bash
git add crates/client-streams/examples/format_pipeline.rs
git commit -m "feat(client-streams): self-asserting JSON->proto->arrow->polars->proto pipeline example"
```

---

## Task 6: The getting-started + data-formats guide page

**Files:**
- Create: `website/content/guide/streams.md`

- [ ] **Step 1: Write the page with snippet directives**

`website/content/guide/streams.md` (the `<!-- snippet: ... -->` blocks are populated by Task 7; leave a single placeholder line between each pair):

```markdown
+++
title = "Streams & Data Formats"
weight = 35
template = "docs/page.html"
+++

`crabka-client-streams` is the KIP-1071 Streams client: it joins a Streams
rebalance group, runs a processing topology, and reads/writes Kafka topics
through pluggable **serdes**. It offers two processing models:

- **Row model** — the Processor API and the high-level DSL (`StreamsApp` /
  `streams_builder`), one record at a time, with `TopologyTestDriver` for
  broker-free tests.
- **Columnar model** — a `ColumnarTopology` whose edges are Polars
  `DataFrame`s, for vectorized aggregation, with `ColumnarTestDriver` for
  broker-free tests.

## Data formats

| Serde / codec | Rust type | Cargo feature | Use it for |
|---|---|---|---|
| `StringSerde` / `I64Serde` / `BytesSerde` | `String` / `i64` / `Bytes` | (built-in) | primitive keys/values |
| `SchemaSerde<T, JsonSerde<T>>` | any `serde` + `schemars::JsonSchema` | (built-in) | Confluent JSON Schema |
| `SchemaSerde<T, ProtobufSerde<T>>` | a prost `Message` | (built-in) | Confluent Protobuf (dynamic via `prost-reflect`) |
| `SchemaSerde<T, AvroSerde<T>>` | `apache_avro::AvroSchema` | (built-in) | Confluent Avro |
| `PolarsIpcSerde` | `polars::DataFrame` | `polars` | columnar values (Arrow IPC) |
| `ArrowIpcSerde` | `arrow::RecordBatch` | `arrow` | arrow-rs interchange |
| `ColumnarSerde<T>` | `columnar::Columnar` | `columnar` | zero-copy native columnar |

Schema serdes resolve schema IDs against a Confluent-compatible registry
(`crabka-schema-registry`); the columnar serdes are self-describing Arrow IPC.

## Getting started

Add the client and pick the columnar features you need:

```toml
[dependencies]
crabka-client-streams = { version = "0.3.6", features = ["polars", "arrow"] }
```

Round-tripping a typed value through a schema serde:

<!-- snippet: client-streams/examples/format_json.rs#json-roundtrip -->
placeholder
<!-- /snippet -->

The idiomatic high-level DSL wires types in via `DefaultSerde`:

<!-- snippet: client-streams/examples/format_dsl.rs#dsl-defaultserde -->
placeholder
<!-- /snippet -->

<!-- snippet: client-streams/examples/format_dsl.rs#dsl-topology -->
placeholder
<!-- /snippet -->

## Worked pipeline: JSON → Protobuf → Arrow → Polars → summary Protobuf

This pipeline ingests order events as JSON, normalizes them to a Protobuf
canonical form, batches them into Arrow, aggregates per user with the Polars
columnar engine, and emits a Protobuf summary — one format at each topic hop:

```text
orders.json   --JSON Schema-->  Stage A  --Protobuf-->  orders.proto
orders.proto  --Protobuf----->  Stage B  --Arrow IPC--> orders.arrow
orders.arrow  --Arrow IPC----->  Stage C (Polars group-by)
(agg rows)    --------------->  Stage D  --Protobuf-->  orders.summary
```

The full source is `crates/client-streams/examples/format_pipeline.rs`; it boots
an in-process broker and Schema Registry and asserts the result, so it runs in
CI as a test.

The shared event type and the Arrow→Polars bridge codec:

<!-- snippet: client-streams/examples/format_pipeline.rs#types -->
placeholder
<!-- /snippet -->

<!-- snippet: client-streams/examples/format_pipeline.rs#arrow-codec -->
placeholder
<!-- /snippet -->

**Stage A — JSON → Protobuf**

<!-- snippet: client-streams/examples/format_pipeline.rs#stage-a-json-proto -->
placeholder
<!-- /snippet -->

**Stage B — Protobuf → Arrow**

<!-- snippet: client-streams/examples/format_pipeline.rs#stage-b-proto-arrow -->
placeholder
<!-- /snippet -->

**Stage C — Arrow → Polars (columnar group-by)**

<!-- snippet: client-streams/examples/format_pipeline.rs#stage-c-arrow-polars -->
placeholder
<!-- /snippet -->

**Stage D — Polars → summary Protobuf**

<!-- snippet: client-streams/examples/format_pipeline.rs#stage-d-polars-proto -->
placeholder
<!-- /snippet -->

**Verifying the rollup**

<!-- snippet: client-streams/examples/format_pipeline.rs#assert -->
placeholder
<!-- /snippet -->
```

- [ ] **Step 2: Verify it is valid Zola front matter**

Run: `head -6 website/content/guide/streams.md`
Expected: the `+++ … +++` TOML front matter block with `title`, `weight = 35`, `template`.

- [ ] **Step 3: Commit**

```bash
git add website/content/guide/streams.md
git commit -m "docs(guide): streams data-formats getting-started page (snippet placeholders)"
```

---

## Task 7: Sync the snippets into the guide

**Files:**
- Modify: `website/content/guide/streams.md` (generated content)

- [ ] **Step 1: Run the snippet sync**

Run: `cargo run -p crabka-docgen -- snippets`
Expected: prints `synced snippets in 1 file(s)` (or more if other pages gained directives).

- [ ] **Step 2: Verify the placeholders were replaced with real code**

Run: `grep -c 'placeholder' website/content/guide/streams.md; grep -c '```rust' website/content/guide/streams.md`
Expected: `0` placeholders; one ```rust fence per snippet directive (9).

- [ ] **Step 3: Confirm idempotence (drift guard sanity)**

Run: `cargo run -p crabka-docgen -- snippets && git diff --quiet -- website/content && echo CLEAN`
Expected: prints `CLEAN` (a second run changes nothing).

- [ ] **Step 4: Commit**

```bash
git add website/content/guide/streams.md
git commit -m "docs(guide): embed tested streams pipeline snippets"
```

---

## Task 8: Local runner script

**Files:**
- Create: `tools/test-doc-examples.sh`

- [ ] **Step 1: Write the script**

`tools/test-doc-examples.sh`:

```bash
#!/usr/bin/env bash
# Build and run every documented client-streams example, then verify the website
# snippets are in sync with their source (doc-drift guard).
set -euo pipefail

echo "==> building all client-streams examples"
cargo build -p crabka-client-streams --examples --features polars,arrow

echo "==> running self-asserting examples"
cargo run -p crabka-client-streams --example format_json
cargo run -p crabka-client-streams --example format_arrow --features arrow
cargo run -p crabka-client-streams --example format_dsl
cargo run -p crabka-client-streams --example format_pipeline --features polars,arrow

echo "==> checking documentation snippets are in sync"
cargo run -p crabka-docgen -- snippets
if ! git diff --quiet -- website/content; then
  echo "ERROR: website snippets are stale. Run: cargo run -p crabka-docgen -- snippets" >&2
  git --no-pager diff -- website/content >&2
  exit 1
fi

echo "==> doc examples OK"
```

- [ ] **Step 2: Make it executable and run it**

Run:
```bash
chmod +x tools/test-doc-examples.sh
./tools/test-doc-examples.sh
```
Expected: ends with `==> doc examples OK` and exit 0.

- [ ] **Step 3: Commit**

```bash
git add tools/test-doc-examples.sh
git commit -m "test(client-streams): runner for documented examples + snippet drift guard"
```

---

## Task 9: CI job

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add a `doc_examples` path-filter output**

In the `changes` job's `outputs:` map, add:

```yaml
      doc_examples: ${{ steps.filter.outputs.doc_examples }}
```

And under `dorny/paths-filter` `with.filters: |`, add a filter (mirrors `client_streams` plus docgen/website/tools):

```yaml
            doc_examples:
              - 'crates/client-streams/**'
              - 'crates/schema-serde/**'
              - 'crates/schema-registry/**'
              - 'crates/client-core/**'
              - 'crates/client-admin/**'
              - 'crates/client-producer/**'
              - 'crates/client-consumer/**'
              - 'crates/broker/**'
              - 'crates/docgen/**'
              - 'website/content/**'
              - 'tools/test-doc-examples.sh'
              - 'Cargo.toml'
              - 'Cargo.lock'
              - '.github/workflows/ci.yml'
```

- [ ] **Step 2: Add the `doc-examples` job**

Add a new job (mirrors `client-streams-integration`'s runner/toolchain/cache):

```yaml
  doc-examples:
    needs: changes
    if: ${{ needs.changes.outputs.doc_examples == 'true' }}
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v6
      - uses: ./.github/actions/free-disk-space
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: "1.96.0"
      - uses: Swatinem/rust-cache@v2
        with:
          key: doc-examples
      - name: Build & run documented examples + snippet drift guard
        run: ./tools/test-doc-examples.sh
```

- [ ] **Step 3: Validate the workflow YAML**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('YAML OK')"`
Expected: `YAML OK`.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: build/run documented client-streams examples + doc-drift guard"
```

---

## Self-review

**Spec coverage:**
- Doc section on client-streams + formats → Task 6 (page) + Task 7 (embedded snippets).
- Getting-started guide → Task 6 "Getting started" section + `format_json`/`format_dsl` snippets.
- JSON→proto→arrow→polars→summary-proto example → Task 1 (proto) + Task 5 (pipeline).
- Automated run+test of all doc examples → Task 5 (self-asserting), Task 8 (script), Task 9 (CI).
- Docs embed tested code (drift-guarded) → Task 3 (`snippets`), Task 7 (sync), Task 8/9 (`git diff` guard).
- Live broker + live Schema Registry → Task 5 `boot()`.

**Placeholder scan:** The only literal `placeholder` strings are the guide's pre-sync filler lines, which Task 7 replaces and Step 2 asserts go to zero. No `TBD`/`TODO`/"add error handling" in any task.

**Type consistency:** `OrderProto`/`OrderSummary` field names (`order_id`, `user`, `amount_cents`, `currency`, `ts_ms`; `total_cents`, `order_count`) are identical across the proto (Task 1), the pipeline stages, the columnar agg aliases, and the asserts. `ArrowBlobCodec`, `BatchError(String)`, `ConsumedRecord`/`ProduceRecord` fields, `run_batch`, `BuiltinOp::GroupByAgg { keys, aggs }`, and the SR boot signatures all match the verified source APIs. Snippet anchors used in Task 6 (`json-roundtrip`, `dsl-defaultserde`, `dsl-topology`, `types`, `arrow-codec`, `stage-a-json-proto`, `stage-b-proto-arrow`, `stage-c-arrow-polars`, `stage-d-polars-proto`, `assert`) each exist in the example files (Tasks 4–5).

**Known churn points flagged for the implementer:** producer `send` double-await; polars `Column` vs `Series` accessor (mirror `row_bridge.rs`); JSON cache seeding for the broker-free example (mirror `schema_serde_bridge.rs`). Each task step says to verify against the named in-repo file.
