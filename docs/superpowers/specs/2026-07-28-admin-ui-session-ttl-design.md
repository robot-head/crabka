# Admin UI Session TTL Design

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

## Goal

Replace the admin UI's fixed eight-hour session lifetime with a validated
runtime setting while preserving the existing default and session-expiry
behavior.

## Scope

This slice changes only the session TTL owned by the standalone
`crabka-admin-ui` binary.

It does not change `SessionStore`'s public `Duration` API, alter cookie
behavior, migrate unrelated admin UI settings, add an operator deployment, or
add a CRD field. No operator or checked-in Kubernetes deployment currently
owns this binary.

## Public Configuration

The compiled default remains exactly 28,800 seconds.

The binary accepts:

- `--session-ttl-seconds`
- `CRABKA_ADMIN_UI_SESSION_TTL_SECONDS`

The command-line value wins when both sources are present. When neither is
present, the compiled default is used.

Zero, malformed, negative, and platform-unrepresentable values fail during
argument parsing, before the listener is bound or broker I/O begins.

## Validated Type

Add a public `SessionTtlSeconds` newtype. Its constructor first validates the
raw `u64` with `refined_type::rule::GreaterU64<0>`, then proves that
`Instant::now().checked_add(Duration::from_secs(value))` succeeds on the
current platform.

This second check prevents accepting a nominally positive TTL that
`SessionStore` would have to treat as immediately expired after `Instant`
overflow. `AdminUiConfig` stores `SessionTtlSeconds` instead of a raw `u64`.
The type exposes a `Duration` only when constructing `SessionStore`.

The lower-level `SessionStore` remains defensive for callers that pass
`Duration::ZERO` or `Duration::MAX`; its existing tests and behavior are not
changed.

## Input Resolution

Add the field to the existing `AdminUiRuntimeArgs` Clap parser. Clap's `long`
plus `env` support and the typed default provide command-line-over-environment
precedence without custom resolution code.

The binary parses the runtime arguments before calling
`AdminUiConfig::from_env()`, then places both resolved typed settings into the
configuration. Existing admin UI configuration behavior remains unchanged.

## Runtime Flow

The exact value flow is:

```text
CLI / environment / typed default
  -> SessionTtlSeconds
  -> AdminUiConfig::session_ttl
  -> AppState::new
  -> SessionStore::new
  -> SessionRecord::expires_at
```

Session creation, lookup, expiration, logout, credentials storage, and cookie
handling remain unchanged.

## Tests

Test-first coverage will prove:

1. the typed default is exactly 28,800 seconds;
2. one second is accepted;
3. zero, malformed, negative, and `u64::MAX` values are rejected;
4. an environment value is accepted and a command-line value overrides it;
5. `AppState::new` passes the configured typed TTL to `SessionStore`;
6. the lower-level zero-duration immediate-expiry and oversized-duration
   no-panic tests remain unchanged and pass.

The crate's all-target tests, strict Clippy, nightly formatting, single-help-
entry check, and diff hygiene remain required completion gates. `Cargo.lock`
must remain unchanged because `crabka-admin-ui` already directly depends on
the workspace-pinned `refined_type`.

## Audit Closure

After implementation, update `docs/configuration-audit.md` with exact scanner
and focused-search counts, the complete value flow, verification evidence,
and the next real unresolved admin UI owner. The three fixed 30,000-millisecond
broker-admin request timeouts remain the adjacent pending policy.
