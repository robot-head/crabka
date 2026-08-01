# GSSAPI Clock-Skew Policy Implementation Plan

**Goal:** Expose the incoming Kerberos AP-REQ clock-skew tolerance through the
existing security, broker TOML, and Kafka listener CRD boundaries.

**Architecture:** Carry one UOM `Time` value from the GSSAPI listener CRD into
the broker `[gssapi]` TOML block, resolve the compatible five-minute default in
broker configuration, and lower it to `std::time::Duration` only at SSPI.

**Tech Stack:** Rust, crabka-units, serde, schemars, kube CRDs, sspi.

### Task 1: Security boundary

- [x] Add failing tests for default and explicit acceptor clock skew.
- [x] Add `max_time_skew: Time` to `GssapiConfig`.
- [x] Pass the dimensioned value into `SspiAcceptor` and SSPI.
- [x] Run security tests and strict all-target Clippy; commit.

### Task 2: Broker TOML boundary

- [x] Add failing omitted-default and explicit-UOM file-config tests.
- [x] Add optional `max_time_skew` to `[gssapi]` and resolve omission to `5m`.
- [x] Update broker GSSAPI callers and fixtures without changing behavior.
- [x] Run focused broker tests and strict all-target Clippy; commit.

### Task 3: Kafka listener CRD and operator rendering

- [x] Add failing CRD schema/serde and rendered-TOML tests.
- [x] Add optional `maxTimeSkew` to `ListenerAuthenticationGssapi`.
- [x] Render it into the existing broker-global `[gssapi]` block.
- [x] Run operator tests and strict all-target Clippy; commit.

### Task 4: Closure

- [x] Run affected all-target tests.
- [x] Run workspace all-target check and strict warnings-as-errors Clippy.
- [x] Run nightly formatting and `git diff --check`.
- [x] Update `docs/configuration-audit.md` with the surface and evidence.
- [x] Commit closure documents; leave the broader repository goal active.
