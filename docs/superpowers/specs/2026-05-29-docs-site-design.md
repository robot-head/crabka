# Crabka Documentation Site — Design

**Date:** 2026-05-29
**Status:** Approved (pending spec review)

## Goal

Publish a documentation site for Crabka on GitHub Pages, built with
[Zola](https://github.com/getzola/zola). The site carries hand-written
narrative docs plus **auto-generated API references** for the **Operator**
(CRD field reference) and the **Broker** (server config, topic configs,
Kafka protocol API table), plus published Rust crate docs (rustdoc).

The defining constraint: generated reference content is produced **from
source at build time** so it never drifts from the code. Nothing generated
is committed to the repo.

## Decisions (locked)

- **Broker reference covers:** broker server config + Kafka topic configs +
  protocol API table + rustdoc.
- **Generation:** auto-generated from source (not hand-written).
- **Deploy:** GitHub Actions → `actions/deploy-pages` on push to `main`.
- **Theme:** [AdiDoks](https://github.com/aaranxu/adidoks), vendored under
  `website/themes/adidoks` (copied in, not a submodule — keeps CI checkout
  simple and pins the theme).
- **Site root:** new top-level `website/` directory (kept separate from
  `docs/`, which holds branding PNGs and superpowers specs).
- **Pages URL:** `https://robot-head.github.io/crabka` (project page → Zola
  `base_url` includes the `/crabka` path prefix).

## Architecture

```
website/
  config.toml                      # base_url, theme=adidoks, search index
  content/
    _index.md                      # landing page
    guide/
      _index.md
      introduction.md              # what Crabka is (sourced from README)
      quickstart.md                # run a broker, run the operator
    reference/
      _index.md
      operator/
        _index.md                  # GENERATED index
        kafka.md                   # GENERATED  (one page per CRD)
        kafkanodepool.md           # GENERATED
        kafkatopic.md              # GENERATED
        kafkauser.md               # GENERATED
        kafkarebalance.md          # GENERATED
      broker/
        _index.md
        server-config.md           # GENERATED (FileConfig schema)
        topic-configs.md           # GENERATED (config_keys whitelist)
        protocol-apis.md           # GENERATED (supported_apis table)
        rust-api.md                # hand-written stub linking to /api/rust/
  static/
    images/                        # branding copied from docs/*.png at build
    api/rust/                       # rustdoc HTML copied in at build (CI)
  themes/adidoks/                  # vendored theme
```

Hand-written pages (`_index.md`, `guide/*`, the `reference/*/_index.md`
section intros, `rust-api.md`) are committed. The five operator pages and
the three broker reference pages are **generated into `content/reference/`
at build time and git-ignored**.

## Components

### 1. `crabka-docgen` tool

A workspace member at `crates/docgen` (a CLI, mirroring the existing
`operator gen-crds` pattern). One subcommand per generated artifact, plus an
`all` that writes the full `content/reference/` tree.

```
crabka-docgen all --out website/content/reference
```

It depends on `crabka-operator` and `crabka-broker` so it can pull the
in-process source-of-truth data structures (no shelling out, no parsing Rust
text). Responsibilities:

- **`operator`** — Read the five CRD schemas. Source: the CRD
  `CustomResourceDefinition` values the operator already builds for
  `gen-crds` (reuse the same builders rather than re-parsing
  `deploy/crds/*.yaml`, so there is a single source of truth). For each CRD,
  walk the OpenAPI v3 `spec`/`status` schema and emit markdown field tables
  (path, type, required, default, description).
- **`broker-config`** — Derive a JSON Schema from `FileConfig` via
  `schemars` and render it with the **same schema→markdown renderer** used
  for CRDs (both are OpenAPI/JSON-Schema shaped). Doc-comments on
  `FileConfig` fields become field descriptions.
- **`topic-configs`** — Read a `pub` doc-table exposed from
  `broker::config_keys` and emit a table (key, type, default, KIP,
  description).
- **`protocol-apis`** — Read a `pub` accessor exposing the broker's
  advertised API set (`supported_apis()`), join with `ApiKey` names, and
  emit a table (api key id, name, min version, max version).

A single shared `schema_to_markdown` module handles the CRD and broker-config
rendering so field-table formatting is identical across both.

### 2. Source-side changes (to make data machine-readable)

These are the only edits outside `website/` and `crates/docgen`:

- **`crates/broker/src/file_config.rs`** — add `#[derive(schemars::JsonSchema)]`
  to `FileConfig` and its nested config structs. (`schemars` 1.2 is already a
  workspace dep; the operator already uses it.) No behavior change.
- **`crates/broker/src/config_keys.rs`** — expose a
  `pub const TOPIC_CONFIG_DOCS: &[TopicConfigDoc]` (or `pub fn`) carrying
  `{ key, value_type, default, kip, description }` for the whitelisted keys,
  built from the existing consts/doc-comments. Re-export from the crate root.
- **`crates/broker/src/handlers/api_versions.rs`** — make the advertised-API
  table reachable from outside the handler (e.g. `pub fn advertised_apis()
  -> Vec<ApiVersion>` at the crate root delegating to the existing
  `supported_apis()`), and ensure `ApiKey` → display-name mapping is `pub`.

No serde/wire/raft formats change. Per CLAUDE.md (greenfield, no compat
shims) these are straightforward additive `pub` exposures.

### 3. Zola site

- `config.toml`: `base_url = "https://robot-head.github.io/crabka"`,
  `theme = "adidoks"`, `build_search_index = true`, generate sitemap.
- AdiDoks vendored under `website/themes/adidoks`.
- Landing page and guide pages hand-written; guide is intentionally small
  (introduction + quickstart) — narrative docs can grow later, out of scope
  here.

### 4. GitHub Actions workflow (`.github/workflows/docs.yml`)

Trigger: `push` to `main` (paths: `website/**`, the generating crates,
`crates/docgen/**`, the workflow itself) + `workflow_dispatch`. Also build
(without deploy) on PRs touching those paths, to catch breakage.

Steps:
1. Checkout.
2. Rust toolchain (pinned via `rust-toolchain.toml`).
3. `cargo run -p crabka-docgen -- all --out website/content/reference`.
4. `cargo doc --no-deps --workspace` → copy `target/doc` to
   `website/static/api/rust/`.
5. Copy `docs/*.png` branding into `website/static/images/`.
6. Install Zola (`taiki-e/install-action` or the official action) and run
   `zola build` in `website/` (Zola checks internal link integrity).
7. On `main` only: `actions/upload-pages-artifact` + `actions/deploy-pages`
   (with the `pages: write`, `id-token: write` permissions and a `github-pages`
   environment).

A one-time manual step (documented in the spec, not automated): enable
Pages → "GitHub Actions" source in repo settings.

## Data flow

```
deploy/crds builders ─┐
FileConfig (schemars) ─┤
config_keys table     ├─► crabka-docgen ─► website/content/reference/*.md ─┐
api_versions table   ─┘                                                     │
                                                                            ├─► zola build ─► public/ ─► deploy-pages
cargo doc ─► target/doc ─► website/static/api/rust/ ───────────────────────┤
docs/*.png ─► website/static/images/ ──────────────────────────────────────┘
hand-written content/ (committed) ──────────────────────────────────────────┘
```

## Error handling

- **docgen** fails hard (non-zero exit) on any missing/unexpected schema
  shape — a CRD field type it can't render, an empty API table, etc. CI
  surfaces this rather than publishing a half-empty reference.
- **zola build** fails on broken internal links (Zola default), catching
  generated-page links that point nowhere.
- PR builds run steps 1–6 (no deploy) so reference-generation breakage is
  caught before merge.

## Testing

- `crates/docgen` unit tests: the `schema_to_markdown` renderer against a
  small fixture schema (nested object, array, enum, default) → asserts the
  expected markdown table; a smoke test that `all` produces non-empty output
  for each of the 8 generated pages.
- The protocol-apis generator asserts the emitted table is non-empty and
  every row has a known `ApiKey` name (guards the silent-empty failure mode).
- CI is the integration test: `zola build` succeeding with all generated
  pages present and links intact.

## Local preview

```
cargo run -p crabka-docgen -- all --out website/content/reference
cargo doc --no-deps --workspace && cp -r target/doc website/static/api/rust
cd website && zola serve
```

A short `website/README.md` documents this.

## Out of scope (YAGNI)

- Versioned docs (multiple Crabka versions). Single "latest" site.
- Custom domain / DNS.
- Search beyond Zola's built-in index.
- Broad narrative documentation (architecture deep-dives, per-KIP guides) —
  the guide ships with just introduction + quickstart; more can follow.
- A drift-check CI job that diffs generated output against committed copies
  (we don't commit generated output, so there's nothing to diff).
