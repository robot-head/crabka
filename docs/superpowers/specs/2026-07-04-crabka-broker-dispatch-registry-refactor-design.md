# Crabka Broker Dispatch Registry Refactor Design

**Date:** 2026-07-04
**Status:** Approved design; implementation plan to follow
**Scope:** Broker core request dispatch clarity and LOC reduction

## Goal

Refactor the broker request-dispatch path to reduce repeated code and make API routing easier to reason about, while preserving Kafka wire-protocol behavior byte-for-byte.

The immediate target is `crates/broker/src/network/dispatch.rs`, which currently mixes the per-connection loop, SASL session state, TLS/kTLS handling, request parsing, API routing, context construction, response-header encoding, request quota patching, and roughly sixty near-identical `handle_*_frame` wrapper functions. The result is correct but hard to extend: adding or changing an API can require updates in the inline match, `HandlerTable`, the flexible-body table, comments documenting intercepted APIs, and route-specific tests.

The refactor should make one registry the source of truth for ordinary broker API routing, so the connection loop keeps connection concerns and the registry owns API dispatch metadata.

## Scope

In scope:

- Replace the split routing model in broker core with a dispatch registry keyed by `ApiKey`.
- Remove repeated wrapper logic for parsing headers, building request contexts, calling handlers, and prepending response headers.
- Allow private handler signatures to change where this materially reduces duplication.
- Preserve Kafka request/response shapes, response-header versions, flexible-version decisions, error codes, and connection-close behavior.
- Keep special-case paths explicit where they are genuinely stateful or I/O-specific.

Out of scope:

- No Kafka protocol schema changes and no edits to generated protocol code.
- No new broker API behavior, authorization policy changes, or request quota semantics changes.
- No broad cleanup of startup, config loading, operator code, generated protocol files, or unrelated handler business logic.
- No compatibility shims for old internal interfaces; Crabka is greenfield and undeployed.

## Architecture Overview

The refactor introduces a single broker dispatch registry that describes how each supported API is executed. The connection loop parses a frame once into a request descriptor, applies connection-level gates, asks the registry for the matching entry, then executes that entry through a small number of shared helper paths.

The registry should distinguish handler families rather than generating one bespoke wrapper per API:

- Plain handlers need only `&Broker`, request version, correlation id, and body bytes.
- Context handlers additionally need a `RequestContext` built from the connection principal, peer address, client id, listener name, and sendfile capability.
- Telemetry handlers need a `TelemetryContext` built from client id, peer address, and the KIP-511 client software fields captured from `ApiVersions`.
- Fetch is special because it may return a `WriteOp` plan and write directly to the stream instead of returning one contiguous `Bytes` body.
- SASL remains outside the registry because it mutates `ConnectionAuth` and has close-after-response behavior tied to the connection state machine.

The connection loop remains responsible for framing, TLS/kTLS convergence, SASL session expiry, auth gating, request spans, quota sleeps, and actual writes. The registry and shared dispatch helpers own request-body dispatch and response-body wrapping.

## Key Design Decisions

### One Registry Owns Routing Metadata

The current code has two routing surfaces: `handlers::HandlerTable` for a small set of plain handlers and a large inline match in `network::dispatch` for context-aware handlers. That split creates drift risk because a reader must check several places to understand whether an API is supported, intercepted, flexible, request-throttled, or expected to receive a connection context.

The new registry should store one entry per supported non-SASL API. Each entry should carry the `ApiKey`, handler family, and generated flexible-version boundary used for request-header parsing and response-header encoding. This keeps API metadata co-located with routing and removes the standalone `handler_body_flexible` match from `dispatch.rs`.

The registry does not need to be dynamic. A static or eagerly built table is enough; the broker does not enable or disable APIs at runtime today. The goal is clarity and deduplication, not a plugin system.

### Parse Once Per Frame

The dispatch loop currently peeks and parses the same frame multiple times: once for tracing, again for auth gating, again in each wrapper, and again for quota patching. The refactor should parse the fixed request header once into a `ParsedRequest` value containing the raw `api_key`, typed `ApiKey` if known, `api_version`, `correlation_id`, `body`, `body_flexible`, and `client_id`.

That parsed value becomes the input to tracing, the pre-auth gate, `ApiVersions` client-info capture, registry lookup, handler execution, response encoding, and request quota patching. Keeping the parsed request borrowed from the original frame avoids extra body allocation and preserves the Produce zero-copy body-slice path.

### Keep Connection-Stateful Paths Explicit

SASL should not be folded into the ordinary registry. `SaslHandshake` and `SaslAuthenticate` mutate `ConnectionAuth`, can enter re-authentication, record authentication metrics, and may require the connection to close immediately after sending a typed response. Keeping this path explicit preserves the state-machine boundary.

Fetch should also stay visibly special at the write boundary. It shares normal request parsing and context construction, but the handler can return a `WriteOp` plan for zero-copy/vectored writes, and the loop must flush the codec before writing the plan directly. Hiding that behind the same `Bytes` path would obscure the data-plane optimization.

ApiVersions is ordinary from a handler-routing perspective, but response-header encoding must continue to honor Kafka's exception: even when the body is flexible, ApiVersions uses response header v0.

### Preserve Existing Request Quota Semantics

The current fallback `HandlerTable` path applies KIP-124 `request_percentage` accounting, while many inline-intercepted admin/data APIs self-account or are exempt by current implementation policy. The registry refactor must not accidentally start throttling previously exempt APIs or double-charge Produce/Fetch.

The registry should therefore carry enough metadata for the loop to decide whether to apply the existing post-handler request quota patch/sleep. This can be a simple execution-policy flag. It is an internal behavior-preservation detail, not a new public API.

### Prefer Small Helper Paths Over Macros That Hide Behavior

Some duplication can be removed with macros, but the durable shape should be normal Rust functions and small enums where possible. The important abstraction is not text generation; it is that each handler family has one dispatch helper whose behavior can be tested directly.

Macros are acceptable only for the registry declaration if they make the table more readable than repetitive function-pointer boilerplate. They should not hide parse, auth, quota, or response-header semantics.

## Data Flow

For each accepted frame, the connection loop should follow this flow:

1. Freeze the frame into `Bytes` when needed so Produce can retain zero-copy body slices.
2. Parse the request header into `ParsedRequest` using the registry's flexible-version metadata when the key is known, and the existing non-flexible fallback for unknown keys.
3. Build the request span from the parsed fields when request tracing is enabled.
4. Apply the SASL pre-auth gate using the parsed API key.
5. Let the SASL path handle handshake/authenticate frames and mutate `ConnectionAuth` when applicable.
6. Capture KIP-511 client software name/version from valid `ApiVersions` v3+ requests before running the regular handler.
7. Look up the dispatch entry and execute it through the matching shared helper.
8. Apply request-quota accounting only when the entry's policy says the existing fallback behavior applies.
9. Write either the encoded `Bytes` response or the Fetch `WriteOp` plan.

Unknown or unregistered keys continue to synthesize the existing two-byte `UNSUPPORTED_VERSION` response body and then pass through normal response-header encoding.

## Error Handling

Connection semantics stay unchanged. Header parse errors, decode errors, handler errors, send errors, and fetch-plan write errors close the connection after logging. Unsupported API keys return the existing synthetic `UNSUPPORTED_VERSION` response. SASL authentication failures write the typed SASL response and close only when the current SASL logic requires it. Pre-auth gate failures still close the connection without trying to synthesize a generic typed Kafka response.

The new internal invariant is that every API Crabka expects to serve has exactly one registry entry, except the SASL pair. Tests should fail if a known plain/context/telemetry/fetch API is missing or registered under the wrong family.

## Kafka Compatibility

Kafka compatibility is the primary constraint. This refactor must preserve:

- Request header v1/v2 selection from body flexibility.
- Response header v0/v1 selection, including the ApiVersions exception.
- All generated request/response body encodings.
- Error codes and typed error bodies produced by handlers.
- SASL pre-auth allowlist and KIP-368 re-auth close behavior.
- KIP-511 client software capture from ApiVersions v3+.
- KIP-219 throttle patching where the existing path applies it.
- Fetch zero-copy/sendfile behavior and fallback inline encoding for legacy versions.

Any ambiguity should be resolved by matching the current behavior first, then Kafka behavior if the current code is found to diverge.

## Testing

Tests should exercise behavior, not source text. Existing dispatch tests should remain or be adapted to the new helpers:

- Header parsing accepts v1/v2 shapes and rejects truncated frames.
- Response encoding uses ApiVersions header v0 and other flexible APIs header v1.
- Flexible-boundary tests verify the registry metadata against representative generated `FLEXIBLE_MIN` constants.
- Throttle patch tests verify only APIs with a leading `ThrottleTimeMs` field are patched.
- Live socket routing tests verify representative context APIs reach their handlers instead of falling through to `UNSUPPORTED_VERSION`.

New tests should cover:

- Registry entries resolve to the expected family for one plain API, one context API, one telemetry API, and Fetch.
- `RequestContext` construction preserves principal, peer address, client id, listener name, and sendfile flag.
- `TelemetryContext` construction preserves client id, peer, and captured KIP-511 software fields.
- Unknown keys still return the synthetic unsupported response.
- SASL frames still bypass ordinary registry execution and preserve close-after-response behavior.

Focused verification should start with dispatch tests and representative broker integration tests, then run `cargo test -p crabka-broker`. Formatting and clippy remain required before claiming implementation completion.

## Risks

The main risk is subtle behavior drift from moving routing into a shared helper path. Response-header versioning, request quota accounting, and Fetch writes are the highest-risk details because they are currently embedded in the connection loop and per-API wrappers. The implementation plan should split these into small, separately tested steps rather than replacing the whole dispatch path at once.

The second risk is over-abstracting handler signatures. A unified interface is useful only if it removes duplication without hiding why SASL, Fetch, and telemetry are different. If a handler family has genuinely unique inputs, the registry should represent that explicitly rather than force everything into one overly broad context type.

## Success Criteria

The refactor is successful when `dispatch.rs` no longer contains the long run of near-identical `handle_*_frame` wrappers, ordinary API routing is visible from one registry, and tests prove representative plain, context, telemetry, fetch, SASL, and unknown-key paths still behave as before.

The practical LOC goal is a substantial net reduction in broker dispatch plumbing, not a style-only rearrangement. Any touched code should become easier to extend for the next Kafka API without adding another bespoke frame wrapper.
