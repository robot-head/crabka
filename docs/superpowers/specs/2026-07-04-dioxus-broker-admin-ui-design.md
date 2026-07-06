# Dioxus Broker Admin UI Design

## Summary

Add a new standalone Dioxus fullstack web application for administering one configured Crabka broker cluster. The first version targets full admin-console workflows for topics/configs, consumer groups, ACLs, SCRAM users, quotas, and log dirs.

The UI uses broker-backed login with SASL/SCRAM-SHA-512 credentials. The server authenticates by connecting to the broker as the submitted principal, derives UI capabilities from broker ACLs, and calls `crabka-client-admin` for all admin operations. Broker authorization remains the source of truth.

## Goals

- Provide a browser-based operations console for one Crabka cluster per UI instance.
- Ship as a new standalone `crabka-admin-ui` workspace binary.
- Use Dioxus fullstack/server-rendered app structure in one binary.
- Authenticate UI users against the broker with SASL/SCRAM-SHA-512.
- Derive visible and enabled UI actions from broker ACLs for the logged-in principal.
- Support the first admin slice: topics/configs, consumer groups, ACLs, SCRAM users, quotas, and log dirs.
- Preserve exact broker error information so operators can diagnose Kafka-level failures.
- Add high-value browser E2E coverage with `playwright-rs`.

## Non-Goals

- Multi-cluster administration.
- Mounting the UI into the broker or `crabka-grpc-gateway` HTTP server.
- A separate public REST API for non-Dioxus clients.
- OIDC/OAuth, reverse-proxy auth, mTLS-only UI auth, or static UI user files.
- Support for SASL/PLAIN or SCRAM-SHA-256 in the first version.
- Metrics dashboards and broker health views in the first implemented slice.

## Architecture

Create a new workspace crate and binary named `crabka-admin-ui`.

The binary owns the HTTP server, renders the Dioxus fullstack app, manages UI login sessions, and calls `crabka-client-admin` from server functions. It is configured for exactly one cluster through local config: display name, bootstrap broker addresses, listener/security options, HTTP bind address, and session settings.

The app does not introduce a separate public JSON API in the first slice. Server functions are the boundary between browser UI and server-side admin logic. This keeps the first version close to existing `crabka-client-admin` capabilities and avoids duplicating a client API contract before there is another consumer.

Login uses broker-backed SASL/SCRAM-SHA-512 credentials. On login, the server attempts an admin-client connection with the submitted username and password, verifies the connection with a lightweight admin call, derives initial capabilities from ACLs, and creates a server-side session. The browser receives only session identity, not broker credentials.

The primary UI shape is an operations-sidebar console: Overview, Topics, Groups, ACLs, Users, Quotas, and Log Dirs. Sections use dense tables, search/filter controls, detail drawers, and confirmation/action modals for mutations.

## Components

- `src/main.rs`: starts the standalone server, loads config, and mounts the Dioxus fullstack app.
- `src/config.rs`: defines HTTP bind address, broker bootstrap addresses, cluster display name, SCRAM-SHA-512 settings, any TLS options already supported by the admin client, and session settings.
- `src/auth.rs`: handles login/logout, server-side sessions, and conversion from login credentials into secured admin-client connection settings.
- `src/admin.rs`: thin adapter over `crabka-client-admin`. It exposes UI-facing async functions for topics/configs, groups, ACLs, SCRAM users, quotas, and log dirs.
- `src/permissions.rs`: derives visible and disabled UI capabilities from broker ACLs for the logged-in principal. Broker authorization remains authoritative.
- `src/views/*`: Dioxus routes and components by section. Shared table, drawer, modal, form, and error-summary components should stay minimal until reuse is real.

Each unit should have one clear responsibility and communicate through explicit DTOs or domain structs. The admin adapter should keep Kafka errors structured rather than flattening them into display strings.

## Data Flow

Startup loads one cluster configuration containing display name, bootstrap brokers, listener/security settings, HTTP bind settings, and session settings.

Login flow:

1. User submits username and password.
2. Server builds SCRAM-SHA-512 client security settings.
3. Server connects through `crabka-client-admin`.
4. Server verifies the connection with a lightweight admin call.
5. Server describes ACLs for the principal and derives initial capabilities.
6. Server creates a server-side session.

Read flow:

1. Dioxus route loads call server functions.
2. Server functions validate the session.
3. Server functions create or borrow an admin client for that session.
4. The admin adapter calls `crabka-client-admin`.
5. Results are mapped into UI DTOs and rendered as tables or detail views.

Mutation flow:

1. Forms post through server functions.
2. The server revalidates the session and intended capability.
3. The admin adapter calls the broker admin RPC.
4. Per-resource Kafka outcomes are returned to the UI.
5. The UI shows mixed success/failure states inline and refreshes affected data.

Capability flow:

1. The UI hides or disables sections/actions based on ACL-derived permissions.
2. Broker authorization remains the source of truth for every admin call.
3. If the broker rejects a request, the UI shows the broker error and refreshes capabilities when appropriate.

## First-Slice UI Surface

Topics and configs:

- List topics with partition count, replication factor, topic id, and error state.
- Create topics with name, partitions, replicas, and optional configs.
- Delete topics with confirmation.
- Expand partitions.
- Describe and incrementally alter dynamic topic config overrides.

Consumer groups:

- List groups.
- Show committed offsets for a selected group.

ACLs:

- List ACLs with filtering by resource, principal, host, operation, permission, and pattern type where supported by the admin client.
- Create ACLs.
- Delete ACLs with confirmation.

SCRAM users:

- Upsert SCRAM-SHA-512 credentials.
- Delete SCRAM-SHA-512 credentials.
- Show per-user operation outcomes.

Quotas:

- Describe user quotas.
- Alter user quotas with validate-only support where exposed by the admin client.

Log dirs:

- Describe log dirs for the connected broker.
- Show directories, topics, partitions, sizes, offset lag, and future-log status.
- Support `AlterReplicaLogDirs` only if the UI can clearly communicate that it is per-broker and not a cluster-wide reassignment workflow.

## Error Handling

Kafka/admin errors stay visible and structured. Each operation distinguishes transport/session failures from broker-returned Kafka errors. Broker errors display the Kafka error name, code, affected resource, and message when available.

Session failures redirect to login with a concise reason: expired session, invalid credentials, or broker connection failure. Authorization failures are displayed as broker-denied actions and trigger capability refresh for that session.

Mutating operations return per-item outcomes when Kafka does, especially topic create/delete, config changes, ACL changes, quota changes, and SCRAM user updates. The UI supports mixed success/failure states rather than presenting batches as atomic.

Dangerous actions use confirmation modals with resource names in the confirmation copy. Destructive operations are disabled while pending and results are shown in the relevant table or detail drawer.

## Testing

Behavior tests cover the adapter, server-function boundaries, and browser-level flows.

Unit tests cover DTO/error mapping, permission derivation from ACL entries, validation for topic/config forms, and session-state transitions.

Server-side integration tests use existing broker test helpers where practical to verify login, topic CRUD/config changes, group listing, ACL visibility/action gating, SCRAM user mutation, quota mutation, and log-dir reads. Tests assert behavior through the admin UI server-function seam.

End-to-end tests use `playwright-rs` against a running `crabka-admin-ui` and test broker. The first E2E suite should cover login, sidebar navigation, topic create/config edit/delete, unauthorized-action hiding for an ACL-limited user, and structured error display for a broker-denied mutation. These tests should be few and high-value, not a duplicate of every server-side test.

Dioxus UI/component tests stay focused on route guards, disabled/hidden actions based on capabilities, form validation, confirmation modal behavior, and display of structured per-resource outcomes.

Verification for implementation should include targeted crate tests first, then workspace formatting and clippy checks as needed.

## Open Decisions Deferred To Planning

- Exact Dioxus crate features and server integration details.
- Exact session store implementation.
- Whether admin clients are pooled, cached per session, or created per request.
- Whether `AlterReplicaLogDirs` is included immediately or gated behind a clear per-broker warning.
- Exact `playwright-rs` harness layout and browser installation strategy for local and CI runs.
