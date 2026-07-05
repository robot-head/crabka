# Crabka Observability Role Ops Router Design

## Summary

`crates/observability/src/lib.rs` repeats the same operational HTTP route
plumbing for the distributor, querier, and compactor roles. This refactor
collapses the shared role status surface into one role-aware router helper while
preserving every existing route, response status, response body, and handler
semantic.

The goal is total line-count reduction in maintained code. The refactor should
delete duplicated role-specific wrapper handlers and repeated route registration;
it should not split modules for style-only reasons and should not touch compaction
or query execution logic.

## Context

The observability service is role-selectable (`distributor`, `querier`,
`compactor`) and intentionally emulates Loki-compatible operational endpoints so
Grafana and Loki tooling work unchanged. Today each role router registers a copy
of common operational routes:

- `/ready`
- `/log_level`
- `/metrics`
- `/config`
- `/services`
- `/memberlist`
- `/ring`
- `/loki/api/v1/status/buildinfo`
- role-specific ring aliases such as `/distributor/ring` and `/compactor/ring`

The underlying behavior is mostly static or role-name driven, but the code has
separate tiny handlers such as `querier_config`, `distributor_config`,
`compactor_config`, `querier_services`, `distributor_services`,
`compactor_services`, `querier_metrics`, `distributor_metrics`,
`compactor_metrics`, and role ring wrappers.

## Goals

- Reduce maintained LOC in `crates/observability/src/lib.rs` by consolidating the
  repeated operational router/status plumbing.
- Preserve all existing public HTTP routes for distributor, querier, and
  compactor.
- Preserve existing response statuses, bodies, and content types for operational
  endpoints.
- Keep role business routes owned by the role routers.
- Add behavior tests for the shared operational surface.

## Non-Goals

- Do not change Loki/OTLP ingest behavior.
- Do not change LogQL query behavior.
- Do not change compaction, delete request materialization, object-store IO, or
  hot-tail polling.
- Do not perform a broad module split unless required by the router refactor.
- Do not introduce macros for route generation.

## Approach

Use a small internal role metadata type and one shared route installer.

Conceptually:

```rust
struct RoleOps {
    target: Role,
    ring_component: &'static str,
    role_ring_path: Option<&'static str>,
}

fn with_role_ops_routes<S>(router: Router<S>, ops: RoleOps) -> Router<S>;
```

The exact type shape can vary during implementation, but the design requires the
metadata to be static and independent of distributor, querier, or compactor state.
The shared operational routes should not need access to `DistributorState`,
`QuerierState`, or `CompactorDeleteState`.

### Shared Routes

The helper registers the common role operations routes once:

- `/ready` -> existing ready response.
- `/log_level` -> existing GET/POST behavior.
- `/metrics` -> role-aware `status_metrics` response.
- `/config` -> existing config response logic.
- `/services` -> existing static services response.
- `/memberlist` -> existing memberlist compatibility response.
- `/ring` -> role-aware ring response.
- `/loki/api/v1/status/buildinfo` -> existing build-info response.
- Optional role ring alias -> same role-aware ring response.

The distributor router passes distributor metadata and then registers only its
business routes: ingest, OTLP, flush, prepare-shutdown, shutdown, and format
query.

The querier router passes querier metadata and then registers only its query,
rules, labels, series, index, tail, and extra scheduler/ruler alias routes.

The compactor router passes compactor metadata and then registers only delete
request and compactor-specific routes.

### Handler Consolidation

The role-specific wrappers should be replaced by role-aware helpers. The new
handlers can either capture static role metadata through Axum state layering or
use a small dedicated operations state merged with the role state. The
implementation should pick the least invasive option that works cleanly with the
existing `Router<S>` state types.

The behavior to preserve includes:

- `/config?mode=diff` returns HTTP 500, `text/plain; charset=utf-8`, and
  `unsupported type <nil>\n`.
- `/config?mode=defaults` returns HTTP 200, `application/yaml; charset=utf-8`,
  and the existing defaults YAML body.
- `/config` returns the existing base YAML body.
- Invalid `POST /log_level` requests return the existing JSON failure shape.
- `/services` returns the current compatibility service list.
- Unknown routes continue using Axum's default 404 behavior.

## Alternatives Considered

### 1. Shared Role Operations Router

This is the recommended approach. It removes repeated route registration and tiny
role wrappers while keeping behavior explicit and easy to test.

Trade-off: the helper must fit Axum's router state model carefully, but the
result is straightforward Rust and easy to review.

### 2. Route Table Macro

A macro could declare common routes for each role and expand repeated route
chains.

Trade-off: it may save a few additional lines but hides router construction and
makes failures harder to debug. This code does not need macro-level abstraction.

### 3. Broad Module Split

Moving distributor, querier, compactor, delete requests, and operational endpoints
into separate modules would improve navigation.

Trade-off: most LOC would move rather than disappear, producing a large diff with
less direct line-count reduction. It remains a later cleanup option, not this
refactor's focus.

## Testing

Tests should exercise behavior, not source structure.

Add or update observability HTTP tests to verify:

- Distributor, querier, and compactor expose `/ready`, `/log_level`, `/metrics`,
  `/config`, `/services`, `/memberlist`, `/ring`, and
  `/loki/api/v1/status/buildinfo`.
- `/distributor/ring` and `/compactor/ring` remain available.
- Existing querier scheduler and ruler ring aliases remain available.
- Representative status/config responses remain unchanged for `/config`,
  `/config?mode=diff`, `/config?mode=defaults`, `/services`, and invalid
  `POST /log_level`.
- Existing ingest, query, and delete request tests pass unchanged.

Verification command:

```sh
cargo test -p crabka-observability
```

## Risks

- Axum router state composition may make a generic route installer awkward. If so,
  prefer a small explicit helper per state type over introducing a macro.
- Operational endpoints are compatibility surfaces for Grafana/Loki tooling, so
  response bodies and content types must not drift.
- Sharing `/ring` behavior must preserve the role component names currently used
  in the HTML pages.

## Acceptance Criteria

- `crates/observability/src/lib.rs` has fewer maintained lines after the refactor.
- Role-specific operational wrapper handlers are removed or substantially reduced.
- All existing role operational routes still respond with equivalent behavior.
- `cargo test -p crabka-observability` passes.
