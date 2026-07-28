# Admin UI Mutation Body Limit Design

## Goal

Replace the admin UI's fixed one-mebibyte authenticated mutation JSON body
limit with a validated runtime setting while preserving the existing default,
authentication order, and HTTP error behavior.

## Scope

This slice changes only the mutation JSON body limit owned by the standalone
`crabka-admin-ui` binary.

It does not migrate the admin UI's existing environment-only settings to
command-line arguments, add an operator deployment, or add a CRD field. No
operator or checked-in Kubernetes deployment currently owns this binary.

## Public Configuration

The compiled default remains exactly 1,048,576 bytes.

The binary accepts:

- `--mutation-json-body-limit-bytes`
- `CRABKA_ADMIN_UI_MUTATION_JSON_BODY_LIMIT_BYTES`

The command-line value wins when both sources are present. When neither is
present, the compiled default is used.

Zero, non-integer values, negative values, and values larger than the target
platform's `usize` range fail during argument parsing, before the listener is
bound or broker I/O begins.

## Validated Type

Add a public `MutationJsonBodyLimitBytes` newtype. Its constructor and
`FromStr` implementation validate with `refined_type::rule::GreaterUsize<0>`.
The type exposes the validated `usize` only at the Axum body-read boundary.

`AdminUiConfig` stores this newtype rather than a raw integer. Its default uses
the existing one-mebibyte value.

## Input Resolution

Add a small Clap parser for this new runtime input only. The parser uses
Clap's existing `long` plus `env` support and the typed default, which gives
the required command-line-over-environment precedence without a second custom
precedence layer.

The binary parses this input before calling the existing
`AdminUiConfig::from_env()`, then places the resolved typed value into the
configuration. Existing admin UI configuration behavior remains unchanged.

## Runtime Flow

The exact value flow is:

```text
CLI / environment / typed default
  -> MutationJsonBodyLimitBytes
  -> AdminUiConfig::mutation_json_body_limit_bytes
  -> AppState::cfg
  -> parse_authenticated_json_request
  -> axum::body::to_bytes
```

Authentication remains before body buffering and JSON decoding. An
authenticated request above the configured limit still returns HTTP 413 with
`request body too large`; malformed JSON within the limit still returns HTTP
400.

## Tests

Test-first coverage will prove:

1. the typed default is exactly 1,048,576 bytes;
2. zero and malformed values are rejected before runtime construction;
3. the environment value is accepted and a command-line value overrides it;
4. a configured small limit reaches every authenticated mutation route and
   produces HTTP 413 before deserialization or mutation execution;
5. the existing authentication-before-decoding behavior remains unchanged.

The crate's all-target tests, strict Clippy, nightly formatting, and diff
hygiene remain required completion gates. `Cargo.lock` may change only to add
the already-locked `refined_type` package to `crabka-admin-ui`'s direct
dependency list; package versions and transitive dependencies must not change.

## Audit Closure

After implementation, update `docs/configuration-audit.md` with the exact
scanner and focused-search counts, the complete value flow, verification
evidence, and the next real unresolved admin UI owner. Constants that are
protocol identifiers, permission bits, static navigation data, or test inputs
remain fixed rather than becoming configuration.
