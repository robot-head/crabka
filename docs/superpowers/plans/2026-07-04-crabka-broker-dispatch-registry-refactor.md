# Crabka Broker Dispatch Registry Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace broker request dispatch's split `HandlerTable` plus inline-intercept routing with a single registry-backed dispatch path that removes repeated frame wrappers without changing Kafka wire behavior.

**Architecture:** The refactor adds focused request/context/registry helpers, migrates handler families into registry adapters, then collapses `network::dispatch` onto parsed-request execution. SASL remains connection-stateful and Fetch remains write-plan-special; ordinary API routing moves to `handlers::registry`.

**Tech Stack:** Rust 2024, Tokio, `bytes::Bytes`, `futures_util::future::BoxFuture`, generated `crabka_protocol::owned::*` codecs, existing broker tests with `assert2`.

## Global Constraints

- Preserve Apache Kafka wire-protocol byte exactness: request/response shapes, response-header versions, field order, error codes, and flexible-version behavior must not change.
- Do not edit generated protocol code under `crates/protocol/generated` or `crates/protocol/src/owned` / `borrowed` generated files.
- Keep SASL handshake/authenticate outside ordinary registry execution because those frames mutate `ConnectionAuth` and can close the connection after a typed response.
- Keep Fetch visibly special at the write boundary because it may return a `Vec<crate::network::fetch_writer::WriteOp>` and bypass `Framed::send`.
- Preserve existing KIP-124 request-quota behavior: do not start request-percentage throttling APIs that are currently inline-exempt, and do not double-charge Produce or Fetch.
- Use behavior tests only; do not assert against source text.
- Crabka is greenfield and undeployed; do not add compatibility shims for old internal handler interfaces.
- Run `cargo +nightly fmt` before final verification, matching the workspace formatting policy.
- Run at least `cargo test -p crabka-broker` before claiming the refactor complete.

---

## File Structure

- Create `crates/broker/src/network/request.rs`: borrowed request-header parsing, `ParsedRequest<'a>`, `peek_api_key`, and `peek_client_id` behavior currently embedded in `dispatch.rs`.
- Modify `crates/broker/src/network/mod.rs`: declare `pub(crate) mod request;`.
- Modify `crates/broker/src/handlers/context.rs`: add constructors for `RequestContext` and `TelemetryContext` so dispatch code stops repeating struct literals.
- Create `crates/broker/src/handlers/registry.rs`: registry types, handler-family adapters, flexible-version metadata, request-quota policy, and tests for registered API families.
- Modify `crates/broker/src/handlers/mod.rs`: re-export registry types and remove the old `HandlerTable` once the registry is wired.
- Modify `crates/broker/src/broker.rs`: store `DispatchRegistry` instead of `HandlerTable`.
- Modify `crates/broker/src/network/dispatch.rs`: consume `ParsedRequest`, route through `DispatchRegistry`, keep SASL/Fetch write concerns in the connection loop, and delete repeated `handle_*_frame` wrappers as their handler families move.
- Modify typed-request handler files only where a signature change removes more wrapper code than it adds. The preferred implementation is registry adapters, so most handler business files should remain unchanged.

## Execution Batches

Most tasks touch `network/dispatch.rs` or `handlers/registry.rs`, so they must run sequentially. Task 1 can run independently from Task 2 only if the implementer splits Task 2 to avoid `handlers/mod.rs`; otherwise execute tasks in order.

---

### Task 1: Context Constructors

**Files:**
- Modify: `crates/broker/src/handlers/context.rs`

**Interfaces:**
- Consumes: existing `RequestContext<'a>` and `TelemetryContext<'a>` structs.
- Produces: `RequestContext::new(...) -> RequestContext<'a>` and `TelemetryContext::new(...) -> TelemetryContext<'a>`.

- [ ] **Step 1: Write failing constructor tests**

Append this test module to `crates/broker/src/handlers/context.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_security::{AuthMethod, Principal};

    use super::*;

    fn principal() -> Principal {
        Principal {
            name: "alice".to_string(),
            auth_method: AuthMethod::SaslPlain,
            groups: vec!["operators".to_string()],
        }
    }

    #[test]
    fn request_context_new_preserves_connection_fields() {
        let principal = principal();
        let peer = SocketAddr::from(([127, 0, 0, 1], 9092));

        let ctx = RequestContext::new(&principal, &peer, "client-a", true, "SASL_SSL");

        assert!(ctx.principal.name == "alice");
        assert!(ctx.peer == &peer);
        assert!(ctx.client_id == "client-a");
        assert!(ctx.sendfile_capable);
        assert!(ctx.connection_listener_name == "SASL_SSL");
    }

    #[test]
    fn telemetry_context_new_preserves_client_identity_fields() {
        let peer = SocketAddr::from(([127, 0, 0, 1], 9092));

        let ctx = TelemetryContext::new(&peer, "client-a", "crabka-test", "1.2.3");

        assert!(ctx.peer == &peer);
        assert!(ctx.client_id == "client-a");
        assert!(ctx.software_name == "crabka-test");
        assert!(ctx.software_version == "1.2.3");
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p crabka-broker handlers::context::tests --lib`

Expected: FAIL with errors that `RequestContext::new` and `TelemetryContext::new` are not found.

- [ ] **Step 3: Add constructors**

Add these impl blocks above the test module in `crates/broker/src/handlers/context.rs`:

```rust
impl<'a> RequestContext<'a> {
    pub(crate) fn new(
        principal: &'a Principal,
        peer: &'a SocketAddr,
        client_id: &'a str,
        sendfile_capable: bool,
        connection_listener_name: &'a str,
    ) -> Self {
        Self {
            principal,
            peer,
            client_id,
            sendfile_capable,
            connection_listener_name,
        }
    }
}

impl<'a> TelemetryContext<'a> {
    pub(crate) fn new(
        peer: &'a SocketAddr,
        client_id: &'a str,
        software_name: &'a str,
        software_version: &'a str,
    ) -> Self {
        Self {
            client_id,
            peer,
            software_name,
            software_version,
        }
    }
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `cargo test -p crabka-broker handlers::context::tests --lib`

Expected: PASS for both context constructor tests.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/context.rs
git commit -m "refactor(broker): add request context constructors"
```

---

### Task 2: Borrowed Parsed Request Module

**Files:**
- Create: `crates/broker/src/network/request.rs`
- Modify: `crates/broker/src/network/mod.rs`

**Interfaces:**
- Consumes: `crate::handlers::{ApiKeyCode, ApiVersion, CorrelationId}` and `BrokerError`.
- Produces: `ParsedRequest<'a>`, `parse_request`, `peek_api_key`, `peek_client_id`.

- [ ] **Step 1: Add failing request parser tests**

Create `crates/broker/src/network/request.rs` with the module shell and tests:

```rust
//! Borrowed Kafka request-header parsing for the broker dispatch loop.

use bytes::{Buf, BytesMut};

use crate::{
    error::BrokerError,
    handlers::{ApiKeyCode, ApiVersion, CorrelationId},
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct ParsedRequest<'a> {
    pub api_key: ApiKeyCode,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,
    pub body: &'a [u8],
    pub body_flexible: bool,
    pub client_id: Option<&'a str>,
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use bytes::BufMut;

    use super::*;

    fn request_frame(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<&[u8]>,
        tagged: Option<u8>,
        body: &[u8],
    ) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_i16(api_key);
        buf.put_i16(api_version);
        buf.put_i32(correlation_id);
        match client_id {
            Some(id) => {
                buf.put_i16(i16::try_from(id.len()).expect("client id length"));
                buf.put_slice(id);
            }
            None => buf.put_i16(-1),
        }
        if let Some(tagged) = tagged {
            buf.put_u8(tagged);
        }
        buf.put_slice(body);
        buf
    }

    #[test]
    fn parse_request_non_flexible_header() {
        let frame = request_frame(3, 8, 42, Some(b"client-a"), None, b"body");

        let parsed = parse_request(&frame, |_, _| false).expect("parse request");

        check!(parsed.api_key == 3);
        check!(parsed.api_version == 8);
        check!(parsed.correlation_id == 42);
        check!(parsed.client_id == Some("client-a"));
        check!(!parsed.body_flexible);
        check!(parsed.body == b"body".as_slice());
    }

    #[test]
    fn parse_request_flexible_header_consumes_tagged_fields_byte() {
        let frame = request_frame(18, 3, 7, Some(b"client-a"), Some(0), b"body");

        let parsed = parse_request(&frame, |key, version| key == 18 && version >= 3)
            .expect("parse request");

        check!(parsed.api_key == 18);
        check!(parsed.api_version == 3);
        check!(parsed.correlation_id == 7);
        check!(parsed.client_id == Some("client-a"));
        check!(parsed.body_flexible);
        check!(parsed.body == b"body".as_slice());
    }

    #[test]
    fn parse_request_rejects_truncated_headers() {
        let mut missing_client_id_len = BytesMut::new();
        missing_client_id_len.put_i16(3);
        missing_client_id_len.put_i16(8);
        missing_client_id_len.put_i32(42);

        let truncated_client_id = request_frame(3, 8, 42, Some(b"client"), None, b"");
        let flexible_without_tag = request_frame(18, 3, 42, Some(b"client"), None, b"");

        let cases = [
            ("missing fixed header", vec![0_u8; 7]),
            ("missing client id length", missing_client_id_len.to_vec()),
            (
                "truncated client id",
                truncated_client_id[..truncated_client_id.len() - 1].to_vec(),
            ),
            ("flexible missing tagged byte", flexible_without_tag.to_vec()),
        ];

        for (case, frame) in cases {
            assert!(
                parse_request(&frame, |key, version| key == 18 && version >= 3).is_err(),
                "{case}"
            );
        }
    }

    #[test]
    fn peek_helpers_match_existing_dispatch_behavior() {
        let present = request_frame(3, 8, 42, Some(b"client-a"), None, b"body");
        let null = request_frame(3, 8, 42, None, None, b"body");
        let empty = request_frame(3, 8, 42, Some(b""), None, b"body");
        let invalid = request_frame(3, 8, 42, Some(&[0xff, 0xfe]), None, b"body");

        assert!(peek_api_key(&present).expect("api key") == 3);
        assert!(peek_client_id(&present) == Some("client-a"));
        assert!(peek_client_id(&null).is_none());
        assert!(peek_client_id(&empty).is_none());
        assert!(peek_client_id(&invalid).is_none());
    }
}
```

Add this line to `crates/broker/src/network/mod.rs`:

```rust
pub(crate) mod request;
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p crabka-broker network::request::tests --lib`

Expected: FAIL with missing functions `parse_request`, `peek_api_key`, and `peek_client_id`.

- [ ] **Step 3: Implement borrowed parser**

Add these functions above the test module in `crates/broker/src/network/request.rs`:

```rust
pub(crate) fn parse_request<F>(
    frame: &[u8],
    flexible_for: F,
) -> Result<ParsedRequest<'_>, BrokerError>
where
    F: Fn(ApiKeyCode, ApiVersion) -> bool,
{
    if frame.len() < 8 {
        return Err(protocol_invalid("request frame < 8 bytes"));
    }

    let mut cur = frame;
    let api_key = cur.get_i16();
    let api_version = cur.get_i16();
    let correlation_id = cur.get_i32();
    let body_flexible = flexible_for(api_key, api_version);

    if cur.remaining() < 2 {
        return Err(protocol_invalid("request frame: missing client_id length"));
    }
    let cid_len = cur.get_i16();
    let client_id = if cid_len > 0 {
        let n = usize::try_from(cid_len).expect("positive i16 fits usize");
        if cur.remaining() < n {
            return Err(protocol_invalid("request frame: client_id length > available"));
        }
        let raw = &cur[..n];
        cur.advance(n);
        std::str::from_utf8(raw).ok()
    } else {
        None
    };

    if body_flexible {
        if cur.remaining() < 1 {
            return Err(protocol_invalid("request frame: missing header tagged-fields byte"));
        }
        let tagged = cur.get_u8();
        if tagged != 0 {
            tracing::debug!(api_key, api_version, "non-empty header tagged fields ignored");
        }
    }

    Ok(ParsedRequest {
        api_key,
        api_version,
        correlation_id,
        body: cur,
        body_flexible,
        client_id,
    })
}

pub(crate) fn peek_api_key(frame: &[u8]) -> Result<ApiKeyCode, BrokerError> {
    if frame.len() < 2 {
        return Err(protocol_invalid("request frame < 2 bytes"));
    }
    Ok(i16::from_be_bytes([frame[0], frame[1]]))
}

pub(crate) fn peek_client_id(frame: &[u8]) -> Option<&str> {
    if frame.len() < 10 {
        return None;
    }
    let cid_len = i16::from_be_bytes([frame[8], frame[9]]);
    if cid_len <= 0 {
        return None;
    }
    let n = usize::try_from(cid_len).ok()?;
    let start = 10_usize;
    let end = start.checked_add(n)?;
    if frame.len() < end {
        return None;
    }
    std::str::from_utf8(&frame[start..end]).ok()
}

fn protocol_invalid(message: &'static str) -> BrokerError {
    BrokerError::Protocol(crabka_protocol::ProtocolError::InvalidValue(message))
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `cargo test -p crabka-broker network::request::tests --lib`

Expected: PASS for all request parser tests.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/network/mod.rs crates/broker/src/network/request.rs
git commit -m "refactor(broker): add borrowed request parser"
```

---

### Task 3: Plain Handler Registry

**Files:**
- Create: `crates/broker/src/handlers/registry.rs`
- Modify: `crates/broker/src/handlers/mod.rs`
- Modify: `crates/broker/src/broker.rs`
- Modify: `crates/broker/src/network/dispatch.rs`

**Interfaces:**
- Consumes: existing plain four-argument handler functions.
- Produces: `DispatchRegistry`, `DispatchEntry`, `DispatchKind::Plain`, `RequestQuotaPolicy`, `build_registry()`.

- [ ] **Step 1: Write failing registry tests**

Create `crates/broker/src/handlers/registry.rs` with this shell and tests:

```rust
//! Broker API dispatch registry.

use bytes::Bytes;
use crabka_protocol::api_key::ApiKey;
use futures_util::future::BoxFuture;

use crate::{
    broker::Broker,
    error::BrokerError,
    handlers::{ApiKeyCode, ApiVersion, CorrelationId},
};

pub(crate) type PlainHandler = fn(
    &Broker,
    ApiVersion,
    CorrelationId,
    &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestQuotaPolicy {
    ApplyFallbackAccounting,
    InlineExempt,
    SelfAccounted,
}

#[derive(Clone, Copy)]
pub(crate) enum DispatchKind {
    Plain(PlainHandler),
}

#[derive(Clone, Copy)]
pub(crate) struct DispatchEntry {
    api_key: ApiKey,
    flexible_min: ApiVersion,
    quota_policy: RequestQuotaPolicy,
    kind: DispatchKind,
}

#[derive(Default)]
pub(crate) struct DispatchRegistry {
    table: std::collections::HashMap<ApiKeyCode, DispatchEntry>,
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::handlers;

    #[test]
    fn registry_registers_plain_handlers() {
        let registry = build_registry();

        let api_versions = registry.get(ApiKey::ApiVersions as i16).expect("ApiVersions");
        assert!(api_versions.is_plain());
        assert!(api_versions.quota_policy() == RequestQuotaPolicy::ApplyFallbackAccounting);
        assert!(api_versions.body_flexible(3));
        assert!(!api_versions.body_flexible(2));

        for key in [25, 27, 59, 69, 73, 83, 84, 85, 86, 87, 89] {
            let entry = registry.get(key).unwrap_or_else(|| panic!("registered api_key {key}"));
            assert!(entry.is_plain(), "api_key {key}");
        }
    }

    #[test]
    fn registry_reports_missing_keys() {
        let registry = build_registry();

        assert!(registry.get(9999).is_none());
    }

    #[test]
    fn plain_handler_pointer_matches_existing_api_versions_handler() {
        let registry = build_registry();
        let handler = registry
            .get_plain(ApiKey::ApiVersions as i16)
            .expect("plain ApiVersions handler");

        assert!(std::ptr::fn_addr_eq(
            handler,
            handlers::api_versions::handle as PlainHandler
        ));
    }
}
```

In `crates/broker/src/handlers/mod.rs`, add:

```rust
pub(crate) mod registry;
pub(crate) use registry::{
    DispatchEntry, DispatchKind, DispatchRegistry, PlainHandler, RequestQuotaPolicy,
};
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p crabka-broker handlers::registry::tests --lib`

Expected: FAIL with missing `build_registry`, `DispatchRegistry::get`, `get_plain`, and `DispatchEntry` methods.

- [ ] **Step 3: Implement registry for plain handlers**

Add this implementation to `crates/broker/src/handlers/registry.rs` above the tests:

```rust
impl DispatchEntry {
    pub(crate) fn plain(api_key: ApiKey, flexible_min: ApiVersion, handler: PlainHandler) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::ApplyFallbackAccounting,
            kind: DispatchKind::Plain(handler),
        }
    }

    pub(crate) fn api_key(self) -> ApiKey {
        self.api_key
    }

    pub(crate) fn kind(self) -> DispatchKind {
        self.kind
    }

    pub(crate) fn quota_policy(self) -> RequestQuotaPolicy {
        self.quota_policy
    }

    pub(crate) fn body_flexible(self, version: ApiVersion) -> bool {
        self.flexible_min != i16::MAX && version >= self.flexible_min
    }

    pub(crate) fn is_plain(self) -> bool {
        matches!(self.kind, DispatchKind::Plain(_))
    }
}

impl DispatchRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register(&mut self, entry: DispatchEntry) -> bool {
        self.table.insert(entry.api_key as i16, entry).is_none()
    }

    pub(crate) fn get(&self, api_key: ApiKeyCode) -> Option<DispatchEntry> {
        self.table.get(&api_key).copied()
    }

    pub(crate) fn get_plain(&self, api_key: ApiKeyCode) -> Option<PlainHandler> {
        match self.get(api_key)?.kind {
            DispatchKind::Plain(handler) => Some(handler),
        }
    }
}

macro_rules! register_plain {
    ($registry:ident, $api:ident, $request:ident, $handler:ident) => {{
        $registry.register(DispatchEntry::plain(
            ApiKey::$api,
            crabka_protocol::owned::$request::FLEXIBLE_MIN,
            crate::handlers::$handler::handle,
        ));
    }};
}

pub(crate) fn build_registry() -> DispatchRegistry {
    let mut registry = DispatchRegistry::new();

    register_plain!(registry, ApiVersions, api_versions_request, api_versions);
    registry.register(DispatchEntry::plain(
        ApiKey::AddOffsetsToTxn,
        crabka_protocol::owned::add_offsets_to_txn_request::FLEXIBLE_MIN,
        crate::txn::handlers::add_offset_commits_to_txn::handle,
    ));
    registry.register(DispatchEntry::plain(
        ApiKey::WriteTxnMarkers,
        crabka_protocol::owned::write_txn_markers_request::FLEXIBLE_MIN,
        crate::txn::handlers::write_txn_markers::handle,
    ));
    register_plain!(registry, FetchSnapshot, fetch_snapshot_request, fetch_snapshot);
    register_plain!(registry, ConsumerGroupDescribe, consumer_group_describe_request, consumer_group_describe);
    register_plain!(registry, AssignReplicasToDirs, assign_replicas_to_dirs_request, assign_replicas_to_dirs);
    registry.register(DispatchEntry::plain(
        ApiKey::InitializeShareGroupState,
        crabka_protocol::owned::initialize_share_group_state_request::FLEXIBLE_MIN,
        crate::share_coordinator::handlers::initialize::handle,
    ));
    registry.register(DispatchEntry::plain(
        ApiKey::ReadShareGroupState,
        crabka_protocol::owned::read_share_group_state_request::FLEXIBLE_MIN,
        crate::share_coordinator::handlers::read::handle,
    ));
    registry.register(DispatchEntry::plain(
        ApiKey::WriteShareGroupState,
        crabka_protocol::owned::write_share_group_state_request::FLEXIBLE_MIN,
        crate::share_coordinator::handlers::write::handle,
    ));
    registry.register(DispatchEntry::plain(
        ApiKey::DeleteShareGroupState,
        crabka_protocol::owned::delete_share_group_state_request::FLEXIBLE_MIN,
        crate::share_coordinator::handlers::delete::handle,
    ));
    registry.register(DispatchEntry::plain(
        ApiKey::ReadShareGroupStateSummary,
        crabka_protocol::owned::read_share_group_state_summary_request::FLEXIBLE_MIN,
        crate::share_coordinator::handlers::read_summary::handle,
    ));
    register_plain!(registry, StreamsGroupDescribe, streams_group_describe_request, streams_group_describe);

    registry
}
```

- [ ] **Step 4: Wire Broker and dispatch to the new registry for plain handlers**

In `crates/broker/src/broker.rs`, change the import:

```rust
use crate::{
    config::BrokerConfig,
    error::BrokerError,
    handlers::DispatchRegistry,
    log_dir,
    partition::{Partition, WriterMessage},
    partition_registry::PartitionRegistry,
};
```

Change the `Broker` field and accessor:

```rust
handlers: DispatchRegistry,
```

```rust
pub(crate) fn handlers(&self) -> &DispatchRegistry {
    &self.handlers
}
```

Change startup handler construction:

```rust
let handlers = crate::handlers::registry::build_registry();
```

In `crates/broker/src/network/dispatch.rs`, change the plain handler lookup in `dispatch_one` to use `get_plain`:

```rust
let handler = broker
    .handlers()
    .get_plain(api_key)
    .ok_or(BrokerError::UnsupportedApi {
        api_key,
        version: api_version,
    });
```

- [ ] **Step 5: Remove old HandlerTable tests and keep plain registry tests**

In `crates/broker/src/handlers/mod.rs`, delete the old `HandlerFn` alias, `HandlerTable` type, `build_table`, and the `#[cfg(test)] mod tests` block that tests `HandlerTable`. Keep `ApiKeyCode`, `ApiVersion`, `ErrorCode`, and `CorrelationId` type aliases.

- [ ] **Step 6: Run tests to verify pass**

Run: `cargo test -p crabka-broker handlers::registry::tests --lib`

Expected: PASS.

Run: `cargo test -p crabka-broker network::dispatch::tests::encode_response_apiversions_uses_v0_header --lib`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/broker/src/handlers/mod.rs crates/broker/src/handlers/registry.rs crates/broker/src/broker.rs crates/broker/src/network/dispatch.rs
git commit -m "refactor(broker): introduce dispatch registry"
```

---

### Task 4: Registry Adapters for Raw Context, Produce, and Telemetry Handlers

**Files:**
- Modify: `crates/broker/src/handlers/registry.rs`
- Modify: `crates/broker/src/network/dispatch.rs`

**Interfaces:**
- Consumes: `RequestContext::new`, `TelemetryContext::new`, existing async handlers with raw request bytes.
- Produces: `DispatchKind::Context`, `DispatchKind::Produce`, `DispatchKind::Telemetry`, and a registry execution helper that handles these families before the old inline match.

- [ ] **Step 1: Add failing registry family tests**

Add these tests to `crates/broker/src/handlers/registry.rs`:

```rust
#[test]
fn registry_registers_raw_context_handlers() {
    let registry = build_registry();

    for key in [0, 3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 19, 20, 21, 22, 23, 24, 26, 28, 32, 33, 35, 37, 42, 44, 47, 55, 56, 60, 61, 63, 64, 65, 66, 68, 74, 75, 76, 77, 78, 79, 80, 81, 82, 88, 90, 91, 92, 93] {
        let entry = registry.get(key).unwrap_or_else(|| panic!("registered api_key {key}"));
        assert!(
            matches!(entry.kind(), DispatchKind::Context(_) | DispatchKind::Produce(_)),
            "api_key {key}"
        );
    }
}

#[test]
fn registry_registers_telemetry_handlers() {
    let registry = build_registry();

    for key in [71, 72] {
        let entry = registry.get(key).unwrap_or_else(|| panic!("registered api_key {key}"));
        assert!(matches!(entry.kind(), DispatchKind::Telemetry(_)), "api_key {key}");
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p crabka-broker handlers::registry::tests --lib`

Expected: FAIL because context, produce, and telemetry variants are not defined or registered.

- [ ] **Step 3: Add handler family types and adapters**

In `crates/broker/src/handlers/registry.rs`, add imports:

```rust
use crate::handlers::{RequestContext, TelemetryContext};
```

Add type aliases:

```rust
pub(crate) type ContextHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;

pub(crate) type ProduceHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    Bytes,
    &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;

pub(crate) type TelemetryHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    &'a TelemetryContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;
```

Extend `DispatchKind`:

```rust
#[derive(Clone, Copy)]
pub(crate) enum DispatchKind {
    Plain(PlainHandler),
    Context(ContextHandler),
    Produce(ProduceHandler),
    Telemetry(TelemetryHandler),
}
```

Add constructors:

```rust
impl DispatchEntry {
    pub(crate) fn context(
        api_key: ApiKey,
        flexible_min: ApiVersion,
        handler: ContextHandler,
    ) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::Context(handler),
        }
    }

    pub(crate) fn produce(flexible_min: ApiVersion, handler: ProduceHandler) -> Self {
        Self {
            api_key: ApiKey::Produce,
            flexible_min,
            quota_policy: RequestQuotaPolicy::SelfAccounted,
            kind: DispatchKind::Produce(handler),
        }
    }

    pub(crate) fn telemetry(
        api_key: ApiKey,
        flexible_min: ApiVersion,
        handler: TelemetryHandler,
    ) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::Telemetry(handler),
        }
    }
}
```

Update `DispatchRegistry::get_plain` so it remains exhaustive after the new variants are added:

```rust
pub(crate) fn get_plain(&self, api_key: ApiKeyCode) -> Option<PlainHandler> {
    match self.get(api_key)?.kind {
        DispatchKind::Plain(handler) => Some(handler),
        _ => None,
    }
}
```

Add adapter macros and concrete adapters:

```rust
macro_rules! context_adapter {
    ($adapter:ident, $handler:expr) => {
        fn $adapter<'a>(
            broker: &'a Broker,
            version: ApiVersion,
            correlation_id: CorrelationId,
            body: &'a [u8],
            ctx: &'a RequestContext<'a>,
        ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
            Box::pin(($handler)(broker, version, correlation_id, body, ctx))
        }
    };
}

macro_rules! telemetry_adapter {
    ($adapter:ident, $handler:expr) => {
        fn $adapter<'a>(
            broker: &'a Broker,
            version: ApiVersion,
            correlation_id: CorrelationId,
            body: &'a [u8],
            ctx: &'a TelemetryContext<'a>,
        ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
            Box::pin(($handler)(broker, version, correlation_id, body, ctx))
        }
    };
}

fn produce_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    correlation_id: CorrelationId,
    body: &'a [u8],
    body_bytes: Bytes,
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(crate::handlers::produce::handle(
        broker,
        version,
        correlation_id,
        body,
        body_bytes,
        ctx,
    ))
}

context_adapter!(metadata_adapter, crate::handlers::metadata::handle);
context_adapter!(create_topics_adapter, crate::handlers::create_topics::handle);
context_adapter!(delete_topics_adapter, crate::handlers::delete_topics::handle);
context_adapter!(alter_configs_adapter, crate::handlers::alter_configs::handle);
context_adapter!(incremental_alter_configs_adapter, crate::handlers::incremental_alter_configs::handle);
context_adapter!(delete_records_adapter, crate::handlers::delete_records::handle);
context_adapter!(create_partitions_adapter, crate::handlers::create_partitions::handle);
context_adapter!(describe_groups_adapter, crate::handlers::describe_groups::handle);
context_adapter!(list_groups_adapter, crate::handlers::list_groups::handle);
context_adapter!(share_group_describe_adapter, crate::handlers::share_group_describe::handle);
context_adapter!(share_fetch_adapter, crate::handlers::share_fetch::handle);
context_adapter!(share_acknowledge_adapter, crate::handlers::share_acknowledge::handle);
context_adapter!(describe_share_group_offsets_adapter, crate::handlers::describe_share_group_offsets::handle);
context_adapter!(alter_share_group_offsets_adapter, crate::handlers::alter_share_group_offsets::handle);
context_adapter!(delete_share_group_offsets_adapter, crate::handlers::delete_share_group_offsets::handle);
context_adapter!(delete_groups_adapter, crate::handlers::delete_groups::handle);
context_adapter!(join_group_adapter, crate::handlers::join_group::handle);
context_adapter!(offset_commit_adapter, crate::handlers::offset_commit::handle);
context_adapter!(offset_fetch_adapter, crate::handlers::offset_fetch::handle);
context_adapter!(offset_delete_adapter, crate::handlers::offset_delete::handle);
context_adapter!(describe_cluster_adapter, crate::handlers::describe_cluster::handle);
context_adapter!(describe_producers_adapter, crate::handlers::describe_producers::handle);
context_adapter!(describe_transactions_adapter, crate::handlers::describe_transactions::handle);
context_adapter!(list_transactions_adapter, crate::handlers::list_transactions::handle);
context_adapter!(unregister_broker_adapter, crate::handlers::unregister_broker::handle);
context_adapter!(describe_topic_partitions_adapter, crate::handlers::describe_topic_partitions::handle);
context_adapter!(list_config_resources_adapter, crate::handlers::list_config_resources::handle);
context_adapter!(describe_quorum_adapter, crate::handlers::describe_quorum::handle);
context_adapter!(add_raft_voter_adapter, crate::handlers::add_raft_voter::handle);
context_adapter!(remove_raft_voter_adapter, crate::handlers::remove_raft_voter::handle);
context_adapter!(update_raft_voter_adapter, crate::handlers::update_raft_voter::handle);
context_adapter!(alter_partition_adapter, crate::handlers::alter_partition::handle);
context_adapter!(broker_heartbeat_adapter, crate::handlers::broker_heartbeat::handle);
context_adapter!(get_replica_log_info_adapter, crate::handlers::get_replica_log_info::handle);
context_adapter!(heartbeat_adapter, crate::handlers::heartbeat::handle);
context_adapter!(sync_group_adapter, crate::handlers::sync_group::handle);
context_adapter!(leave_group_adapter, crate::handlers::leave_group::handle);
context_adapter!(consumer_group_heartbeat_adapter, crate::handlers::consumer_group_heartbeat::handle);
context_adapter!(share_group_heartbeat_adapter, crate::handlers::share_group_heartbeat::handle);
context_adapter!(streams_group_heartbeat_adapter, crate::handlers::streams_group_heartbeat::handle);
context_adapter!(find_coordinator_adapter, crate::handlers::find_coordinator::handle);
context_adapter!(list_offsets_adapter, crate::handlers::list_offsets::handle);
context_adapter!(offset_for_leader_epoch_adapter, crate::handlers::offset_for_leader_epoch::handle);
context_adapter!(describe_configs_adapter, crate::handlers::describe_configs::handle);
context_adapter!(describe_log_dirs_adapter, crate::handlers::describe_log_dirs::handle);
context_adapter!(init_producer_id_adapter, crate::handlers::init_producer_id::handle);
context_adapter!(add_partitions_to_txn_adapter, crate::txn::handlers::add_partitions_to_txn::handle);
context_adapter!(end_txn_adapter, crate::txn::handlers::end_txn::handle);
context_adapter!(txn_offset_commit_adapter, crate::txn::handlers::txn_offset_commit::handle);

telemetry_adapter!(get_telemetry_subscriptions_adapter, crate::handlers::get_telemetry_subscriptions::handle);
telemetry_adapter!(push_telemetry_adapter, crate::handlers::push_telemetry::handle);
```

Add these registrations to `build_registry()` after the plain registrations from Task 3:

```rust
registry.register(DispatchEntry::produce(
    crabka_protocol::owned::produce_request::FLEXIBLE_MIN,
    produce_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::Metadata,
    crabka_protocol::owned::metadata_request::FLEXIBLE_MIN,
    metadata_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::CreateTopics,
    crabka_protocol::owned::create_topics_request::FLEXIBLE_MIN,
    create_topics_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::DeleteTopics,
    crabka_protocol::owned::delete_topics_request::FLEXIBLE_MIN,
    delete_topics_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::AlterConfigs,
    crabka_protocol::owned::alter_configs_request::FLEXIBLE_MIN,
    alter_configs_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::IncrementalAlterConfigs,
    crabka_protocol::owned::incremental_alter_configs_request::FLEXIBLE_MIN,
    incremental_alter_configs_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::DeleteRecords,
    crabka_protocol::owned::delete_records_request::FLEXIBLE_MIN,
    delete_records_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::CreatePartitions,
    crabka_protocol::owned::create_partitions_request::FLEXIBLE_MIN,
    create_partitions_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::DescribeGroups,
    crabka_protocol::owned::describe_groups_request::FLEXIBLE_MIN,
    describe_groups_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::ListGroups,
    crabka_protocol::owned::list_groups_request::FLEXIBLE_MIN,
    list_groups_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::ShareGroupDescribe,
    crabka_protocol::owned::share_group_describe_request::FLEXIBLE_MIN,
    share_group_describe_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::ShareFetch,
    crabka_protocol::owned::share_fetch_request::FLEXIBLE_MIN,
    share_fetch_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::ShareAcknowledge,
    crabka_protocol::owned::share_acknowledge_request::FLEXIBLE_MIN,
    share_acknowledge_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::DescribeShareGroupOffsets,
    crabka_protocol::owned::describe_share_group_offsets_request::FLEXIBLE_MIN,
    describe_share_group_offsets_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::AlterShareGroupOffsets,
    crabka_protocol::owned::alter_share_group_offsets_request::FLEXIBLE_MIN,
    alter_share_group_offsets_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::DeleteShareGroupOffsets,
    crabka_protocol::owned::delete_share_group_offsets_request::FLEXIBLE_MIN,
    delete_share_group_offsets_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::DeleteGroups,
    crabka_protocol::owned::delete_groups_request::FLEXIBLE_MIN,
    delete_groups_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::JoinGroup,
    crabka_protocol::owned::join_group_request::FLEXIBLE_MIN,
    join_group_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::OffsetCommit,
    crabka_protocol::owned::offset_commit_request::FLEXIBLE_MIN,
    offset_commit_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::OffsetFetch,
    crabka_protocol::owned::offset_fetch_request::FLEXIBLE_MIN,
    offset_fetch_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::OffsetDelete,
    crabka_protocol::owned::offset_delete_request::FLEXIBLE_MIN,
    offset_delete_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::DescribeCluster,
    crabka_protocol::owned::describe_cluster_request::FLEXIBLE_MIN,
    describe_cluster_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::DescribeProducers,
    crabka_protocol::owned::describe_producers_request::FLEXIBLE_MIN,
    describe_producers_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::DescribeTransactions,
    crabka_protocol::owned::describe_transactions_request::FLEXIBLE_MIN,
    describe_transactions_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::ListTransactions,
    crabka_protocol::owned::list_transactions_request::FLEXIBLE_MIN,
    list_transactions_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::UnregisterBroker,
    crabka_protocol::owned::unregister_broker_request::FLEXIBLE_MIN,
    unregister_broker_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::DescribeTopicPartitions,
    crabka_protocol::owned::describe_topic_partitions_request::FLEXIBLE_MIN,
    describe_topic_partitions_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::ListConfigResources,
    crabka_protocol::owned::list_config_resources_request::FLEXIBLE_MIN,
    list_config_resources_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::DescribeQuorum,
    crabka_protocol::owned::describe_quorum_request::FLEXIBLE_MIN,
    describe_quorum_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::AddRaftVoter,
    crabka_protocol::owned::add_raft_voter_request::FLEXIBLE_MIN,
    add_raft_voter_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::RemoveRaftVoter,
    crabka_protocol::owned::remove_raft_voter_request::FLEXIBLE_MIN,
    remove_raft_voter_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::UpdateRaftVoter,
    crabka_protocol::owned::update_raft_voter_request::FLEXIBLE_MIN,
    update_raft_voter_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::AlterPartition,
    crabka_protocol::owned::alter_partition_request::FLEXIBLE_MIN,
    alter_partition_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::BrokerHeartbeat,
    crabka_protocol::owned::broker_heartbeat_request::FLEXIBLE_MIN,
    broker_heartbeat_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::GetReplicaLogInfo,
    crabka_protocol::owned::get_replica_log_info_request::FLEXIBLE_MIN,
    get_replica_log_info_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::Heartbeat,
    crabka_protocol::owned::heartbeat_request::FLEXIBLE_MIN,
    heartbeat_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::SyncGroup,
    crabka_protocol::owned::sync_group_request::FLEXIBLE_MIN,
    sync_group_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::LeaveGroup,
    crabka_protocol::owned::leave_group_request::FLEXIBLE_MIN,
    leave_group_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::ConsumerGroupHeartbeat,
    crabka_protocol::owned::consumer_group_heartbeat_request::FLEXIBLE_MIN,
    consumer_group_heartbeat_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::ShareGroupHeartbeat,
    crabka_protocol::owned::share_group_heartbeat_request::FLEXIBLE_MIN,
    share_group_heartbeat_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::StreamsGroupHeartbeat,
    crabka_protocol::owned::streams_group_heartbeat_request::FLEXIBLE_MIN,
    streams_group_heartbeat_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::FindCoordinator,
    crabka_protocol::owned::find_coordinator_request::FLEXIBLE_MIN,
    find_coordinator_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::ListOffsets,
    crabka_protocol::owned::list_offsets_request::FLEXIBLE_MIN,
    list_offsets_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::OffsetForLeaderEpoch,
    crabka_protocol::owned::offset_for_leader_epoch_request::FLEXIBLE_MIN,
    offset_for_leader_epoch_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::DescribeConfigs,
    crabka_protocol::owned::describe_configs_request::FLEXIBLE_MIN,
    describe_configs_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::DescribeLogDirs,
    crabka_protocol::owned::describe_log_dirs_request::FLEXIBLE_MIN,
    describe_log_dirs_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::InitProducerId,
    crabka_protocol::owned::init_producer_id_request::FLEXIBLE_MIN,
    init_producer_id_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::AddPartitionsToTxn,
    crabka_protocol::owned::add_partitions_to_txn_request::FLEXIBLE_MIN,
    add_partitions_to_txn_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::EndTxn,
    crabka_protocol::owned::end_txn_request::FLEXIBLE_MIN,
    end_txn_adapter,
));
registry.register(DispatchEntry::context(
    ApiKey::TxnOffsetCommit,
    crabka_protocol::owned::txn_offset_commit_request::FLEXIBLE_MIN,
    txn_offset_commit_adapter,
));
registry.register(DispatchEntry::telemetry(
    ApiKey::GetTelemetrySubscriptions,
    crabka_protocol::owned::get_telemetry_subscriptions_request::FLEXIBLE_MIN,
    get_telemetry_subscriptions_adapter,
));
registry.register(DispatchEntry::telemetry(
    ApiKey::PushTelemetry,
    crabka_protocol::owned::push_telemetry_request::FLEXIBLE_MIN,
    push_telemetry_adapter,
));
```

Do not register Kafka Fetch (`ApiKey::Fetch`), SASL, decoded-request handlers, or auth-gated handlers in this task.

- [ ] **Step 4: Add dispatch helper for registered byte responses**

In `crates/broker/src/network/dispatch.rs`, add this helper near `dispatch_one`:

```rust
async fn dispatch_registered_bytes(
    broker: &Broker,
    entry: crate::handlers::DispatchEntry,
    parsed: &crate::network::request::ParsedRequest<'_>,
    frame: &Bytes,
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
    listener_name: &str,
    client_software_name: &str,
    client_software_version: &str,
) -> Option<Result<Bytes, BrokerError>> {
    match entry.kind() {
        crate::handlers::DispatchKind::Context(handler) => {
            let ctx = crate::handlers::RequestContext::new(
                principal_or_anonymous(auth),
                peer,
                parsed.client_id.unwrap_or(""),
                false,
                listener_name,
            );
            Some(handler(
                broker,
                parsed.api_version,
                parsed.correlation_id,
                parsed.body,
                &ctx,
            )
            .await
            .map(|body| encode_response(parsed.api_key, parsed.correlation_id, parsed.body_flexible, &body)))
        }
        crate::handlers::DispatchKind::Produce(handler) => {
            let ctx = crate::handlers::RequestContext::new(
                principal_or_anonymous(auth),
                peer,
                parsed.client_id.unwrap_or(""),
                false,
                "",
            );
            let body_offset = frame.len() - parsed.body.len();
            let body_bytes = frame.slice(body_offset..);
            Some(handler(
                broker,
                parsed.api_version,
                parsed.correlation_id,
                parsed.body,
                body_bytes,
                &ctx,
            )
            .await
            .map(|body| encode_response(parsed.api_key, parsed.correlation_id, parsed.body_flexible, &body)))
        }
        crate::handlers::DispatchKind::Telemetry(handler) => {
            let ctx = crate::handlers::TelemetryContext::new(
                peer,
                parsed.client_id.unwrap_or(""),
                client_software_name,
                client_software_version,
            );
            Some(handler(
                broker,
                parsed.api_version,
                parsed.correlation_id,
                parsed.body,
                &ctx,
            )
            .await
            .map(|body| encode_response(parsed.api_key, parsed.correlation_id, parsed.body_flexible, &body)))
        }
        crate::handlers::DispatchKind::Plain(_) => None,
    }
}
```

Before the existing inline `match peek_api_key(&frame).ok().and_then(ApiKey::from_i16)` in `serve_connection_stream`, add a temporary parsed request using the existing flexible function:

```rust
let parsed = match crate::network::request::parse_request(&frame, handler_body_flexible) {
    Ok(parsed) => parsed,
    Err(e) => {
        tracing::warn!(error = %e, "request parse error, closing");
        break;
    }
};

if let Some(entry) = broker.handlers().get(parsed.api_key)
    && !matches!(entry.kind(), crate::handlers::DispatchKind::Plain(_))
    && let Some(result) = dispatch_registered_bytes(
        &broker,
        entry,
        &parsed,
        &frame,
        &auth,
        &peer,
        &spec.name,
        &client_software_name,
        &client_software_version,
    )
    .instrument(req_span.clone())
    .await
{
    match result {
        Ok(bytes) => {
            if let Err(e) = framed.send(bytes).await {
                tracing::warn!(error = %e, "framed.send error during registry dispatch, closing");
                break;
            }
            continue;
        }
        Err(e) => {
            tracing::warn!(error = %e, "registry dispatch error, closing connection");
            break;
        }
    }
}
```

This helper intentionally runs before the old inline match and leaves the old wrappers in place for keys not migrated in this task.

- [ ] **Step 5: Run representative routing tests**

Run: `cargo test -p crabka-broker handlers::registry::tests --lib`

Expected: PASS.

Run: `cargo test -p crabka-broker network::dispatch::tests::raft_voter_dispatch_arms_route_to_real_handlers --lib`

Expected: PASS. The test still drives a socket; the name can be updated in the next cleanup task.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/handlers/registry.rs crates/broker/src/network/dispatch.rs
git commit -m "refactor(broker): route raw-context handlers through registry"
```

---

### Task 5: Registry Adapters for Decoded Request and Auth-Gated Handlers

**Files:**
- Modify: `crates/broker/src/handlers/registry.rs`
- Modify: `crates/broker/src/network/dispatch.rs`

**Interfaces:**
- Consumes: generated request `Decode`, generated response `Encode`, existing decoded-request handlers, existing auth-gated handlers.
- Produces: `DispatchKind::DecodedContext`, `DispatchKind::EncodedContext`, `DispatchKind::Auth`, and removal of decoded/auth wrapper execution from the inline match.

- [ ] **Step 1: Add failing tests for decoded and auth families**

Add these tests to `crates/broker/src/handlers/registry.rs`:

```rust
#[test]
fn registry_registers_decoded_context_handlers() {
    let registry = build_registry();

    for key in [29, 30, 31, 43, 45, 46, 48, 49, 50, 51, 57] {
        let entry = registry.get(key).unwrap_or_else(|| panic!("registered api_key {key}"));
        assert!(
            matches!(
                entry.kind(),
                DispatchKind::DecodedContext(_) | DispatchKind::EncodedContext(_)
            ),
            "api_key {key}"
        );
    }
}

#[test]
fn registry_registers_auth_handlers() {
    let registry = build_registry();

    for key in [34, 38, 39, 40, 41] {
        let entry = registry.get(key).unwrap_or_else(|| panic!("registered api_key {key}"));
        assert!(matches!(entry.kind(), DispatchKind::Auth(_)), "api_key {key}");
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p crabka-broker handlers::registry::tests --lib`

Expected: FAIL because decoded and auth variants are not defined or registered.

- [ ] **Step 3: Add decoded and auth handler types**

Add this type alias and enum variants to `crates/broker/src/handlers/registry.rs`:

```rust
pub(crate) type AuthHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    &'a crate::network::auth::ConnectionAuth,
    &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;
```

Extend `DispatchKind`:

```rust
#[derive(Clone, Copy)]
pub(crate) enum DispatchKind {
    Plain(PlainHandler),
    Context(ContextHandler),
    Produce(ProduceHandler),
    Telemetry(TelemetryHandler),
    DecodedContext(ContextHandler),
    EncodedContext(ContextHandler),
    Auth(AuthHandler),
}
```

Add constructors:

```rust
impl DispatchEntry {
    pub(crate) fn decoded_context(
        api_key: ApiKey,
        flexible_min: ApiVersion,
        handler: ContextHandler,
    ) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::DecodedContext(handler),
        }
    }

    pub(crate) fn encoded_context(
        api_key: ApiKey,
        flexible_min: ApiVersion,
        handler: ContextHandler,
    ) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::EncodedContext(handler),
        }
    }

    pub(crate) fn auth(api_key: ApiKey, flexible_min: ApiVersion, handler: AuthHandler) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::Auth(handler),
        }
    }
}
```

- [ ] **Step 4: Add concrete adapters for decoded-context handlers**

Add adapter functions to `crates/broker/src/handlers/registry.rs`. These adapters keep typed decode close to the registry and avoid one full wrapper function per API in `network::dispatch`:

```rust
fn describe_acls_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;
        let mut cur = body;
        let req = crabka_protocol::owned::describe_acls_request::DescribeAclsRequest::decode(
            &mut cur,
            version,
        )?;
        crate::handlers::describe_acls::handle(broker, req, ctx, version).await
    })
}

fn create_acls_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;
        let mut cur = body;
        let req = crabka_protocol::owned::create_acls_request::CreateAclsRequest::decode(
            &mut cur,
            version,
        )?;
        crate::handlers::create_acls::handle(broker, req, ctx, version).await
    })
}

fn delete_acls_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;
        let mut cur = body;
        let req = crabka_protocol::owned::delete_acls_request::DeleteAclsRequest::decode(
            &mut cur,
            version,
        )?;
        crate::handlers::delete_acls::handle(broker, req, ctx, version).await
    })
}
```

Add the remaining decoded-context adapters in the same file:

```rust
fn elect_leaders_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;
        let mut cur = body;
        let req = crabka_protocol::owned::elect_leaders_request::ElectLeadersRequest::decode(
            &mut cur,
            version,
        )?;
        crate::handlers::elect_leaders::handle(broker, req, ctx, version).await
    })
}

fn alter_partition_reassignments_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;
        let mut cur = body;
        let req = crabka_protocol::owned::alter_partition_reassignments_request::AlterPartitionReassignmentsRequest::decode(
            &mut cur,
            version,
        )?;
        crate::handlers::alter_partition_reassignments::handle(broker, req, ctx, version).await
    })
}

fn list_partition_reassignments_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;
        let mut cur = body;
        let req = crabka_protocol::owned::list_partition_reassignments_request::ListPartitionReassignmentsRequest::decode(
            &mut cur,
            version,
        )?;
        crate::handlers::list_partition_reassignments::handle(broker, req, ctx, version).await
    })
}

fn describe_client_quotas_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;
        let mut cur = body;
        let req = crabka_protocol::owned::describe_client_quotas_request::DescribeClientQuotasRequest::decode(
            &mut cur,
            version,
        )?;
        crate::handlers::describe_client_quotas::handle(broker, req, ctx, version).await
    })
}

fn alter_client_quotas_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;
        let mut cur = body;
        let req = crabka_protocol::owned::alter_client_quotas_request::AlterClientQuotasRequest::decode(
            &mut cur,
            version,
        )?;
        crate::handlers::alter_client_quotas::handle(broker, req, ctx, version).await
    })
}

fn describe_user_scram_credentials_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use crabka_protocol::Decode;
        let mut cur = body;
        let req = crabka_protocol::owned::describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest::decode(
            &mut cur,
            version,
        )?;
        crate::handlers::describe_user_scram_credentials::handle(broker, req, ctx, version).await
    })
}
```

For typed-response context handlers, encode the response inside the adapter:

```rust
fn alter_user_scram_credentials_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use bytes::BytesMut;
        use crabka_protocol::{Decode, Encode};

        let mut cur = body;
        let req = crabka_protocol::owned::alter_user_scram_credentials_request::AlterUserScramCredentialsRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::alter_user_scram_credentials::handle(broker, req, ctx).await;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

fn update_features_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use bytes::BytesMut;
        use crabka_protocol::{Decode, Encode};

        let mut cur = body;
        let req = crabka_protocol::owned::update_features_request::UpdateFeaturesRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::update_features::handle(broker, req, version, ctx).await;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

- [ ] **Step 5: Add auth-gated adapters**

Add these auth adapters to `crates/broker/src/handlers/registry.rs`:

```rust
fn alter_replica_log_dirs_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use std::collections::BTreeMap;

        use bytes::BytesMut;
        use crabka_protocol::{
            Decode, Encode,
            owned::{
                alter_replica_log_dirs_request::AlterReplicaLogDirsRequest,
                alter_replica_log_dirs_response::{
                    AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
                    AlterReplicaLogDirsResponse,
                },
            },
        };

        let anonymous;
        let principal = match auth.principal() {
            Some(principal) => principal,
            None => {
                anonymous = crabka_security::Principal {
                    name: "ANONYMOUS".to_string(),
                    auth_method: crabka_security::AuthMethod::Anonymous,
                    groups: vec![],
                };
                &anonymous
            }
        };

        let image = broker.controller.current_image();
        let authorized = broker.config.authorizer.authorize(
            &*image,
            &crate::authorizer::AuthorizationRequest {
                principal,
                host: peer,
                resource_type: crabka_metadata::ResourceType::Cluster,
                resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                operation: crabka_metadata::AclOperation::Alter,
            },
        ) == crate::authorizer::AuthorizationResult::Allow;

        if !authorized {
            let mut cur = body;
            let req = AlterReplicaLogDirsRequest::decode(&mut cur, version)?;
            let mut by_topic: BTreeMap<String, Vec<AlterReplicaLogDirPartitionResult>> =
                BTreeMap::new();
            for dir in req.dirs {
                for topic in dir.topics {
                    for partition_index in topic.partitions {
                        by_topic.entry(topic.name.clone()).or_default().push(
                            AlterReplicaLogDirPartitionResult {
                                partition_index,
                                error_code: crate::codes::CLUSTER_AUTHORIZATION_FAILED,
                                ..Default::default()
                            },
                        );
                    }
                }
            }
            let results = by_topic
                .into_iter()
                .map(|(topic_name, partitions)| AlterReplicaLogDirTopicResult {
                    topic_name,
                    partitions,
                    ..Default::default()
                })
                .collect();
            let resp = AlterReplicaLogDirsResponse {
                throttle_time_ms: 0,
                results,
                ..Default::default()
            };
            let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
            resp.encode(&mut buf, version)?;
            return Ok(buf.freeze());
        }

        crate::handlers::alter_replica_log_dirs::handle(broker, version, correlation_id, body)
            .await
    })
}

fn create_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    _peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use bytes::BytesMut;
        use crabka_protocol::{Decode, Encode};

        let mut cur = body;
        let req = crabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::create_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            broker.config.delegation_token_max_lifetime_ms,
            broker.config.delegation_token_default_renew_period_ms,
            &*broker.controller,
            &broker.config.super_users,
        )
        .await;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

Add the remaining delegation-token auth adapters in the same file:

```rust
fn renew_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    _peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use bytes::BytesMut;
        use crabka_protocol::{Decode, Encode};

        let mut cur = body;
        let req = crabka_protocol::owned::renew_delegation_token_request::RenewDelegationTokenRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::renew_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            broker.config.delegation_token_default_renew_period_ms,
            &*broker.controller,
            &broker.config.super_users,
        )
        .await;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

fn expire_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    _peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use bytes::BytesMut;
        use crabka_protocol::{Decode, Encode};

        let mut cur = body;
        let req = crabka_protocol::owned::expire_delegation_token_request::ExpireDelegationTokenRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::expire_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            &*broker.controller,
            &broker.config.super_users,
        )
        .await;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

fn describe_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use bytes::BytesMut;
        use crabka_protocol::{Decode, Encode};

        let mut cur = body;
        let req = crabka_protocol::owned::describe_delegation_token_request::DescribeDelegationTokenRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::describe_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            &*broker.controller,
            peer,
            broker.config.authorizer.as_ref(),
        )
        .await;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

Each delegation-token adapter must encode the typed response with `BytesMut::with_capacity(resp.encoded_len(version))` and `resp.encode(&mut buf, version)?` before returning `Ok(buf.freeze())`.

- [ ] **Step 6: Register decoded and auth adapters**

Add `registry.register(...)` calls to `build_registry()` for these APIs:

```rust
registry.register(DispatchEntry::decoded_context(ApiKey::DescribeAcls, crabka_protocol::owned::describe_acls_request::FLEXIBLE_MIN, describe_acls_adapter));
registry.register(DispatchEntry::decoded_context(ApiKey::CreateAcls, crabka_protocol::owned::create_acls_request::FLEXIBLE_MIN, create_acls_adapter));
registry.register(DispatchEntry::decoded_context(ApiKey::DeleteAcls, crabka_protocol::owned::delete_acls_request::FLEXIBLE_MIN, delete_acls_adapter));
registry.register(DispatchEntry::decoded_context(ApiKey::ElectLeaders, crabka_protocol::owned::elect_leaders_request::FLEXIBLE_MIN, elect_leaders_adapter));
registry.register(DispatchEntry::decoded_context(ApiKey::AlterPartitionReassignments, crabka_protocol::owned::alter_partition_reassignments_request::FLEXIBLE_MIN, alter_partition_reassignments_adapter));
registry.register(DispatchEntry::decoded_context(ApiKey::ListPartitionReassignments, crabka_protocol::owned::list_partition_reassignments_request::FLEXIBLE_MIN, list_partition_reassignments_adapter));
registry.register(DispatchEntry::decoded_context(ApiKey::DescribeClientQuotas, crabka_protocol::owned::describe_client_quotas_request::FLEXIBLE_MIN, describe_client_quotas_adapter));
registry.register(DispatchEntry::decoded_context(ApiKey::AlterClientQuotas, crabka_protocol::owned::alter_client_quotas_request::FLEXIBLE_MIN, alter_client_quotas_adapter));
registry.register(DispatchEntry::decoded_context(ApiKey::DescribeUserScramCredentials, crabka_protocol::owned::describe_user_scram_credentials_request::FLEXIBLE_MIN, describe_user_scram_credentials_adapter));
registry.register(DispatchEntry::encoded_context(ApiKey::AlterUserScramCredentials, crabka_protocol::owned::alter_user_scram_credentials_request::FLEXIBLE_MIN, alter_user_scram_credentials_adapter));
registry.register(DispatchEntry::encoded_context(ApiKey::UpdateFeatures, crabka_protocol::owned::update_features_request::FLEXIBLE_MIN, update_features_adapter));
registry.register(DispatchEntry::auth(ApiKey::AlterReplicaLogDirs, crabka_protocol::owned::alter_replica_log_dirs_request::FLEXIBLE_MIN, alter_replica_log_dirs_adapter));
registry.register(DispatchEntry::auth(ApiKey::CreateDelegationToken, crabka_protocol::owned::create_delegation_token_request::FLEXIBLE_MIN, create_delegation_token_adapter));
registry.register(DispatchEntry::auth(ApiKey::RenewDelegationToken, crabka_protocol::owned::renew_delegation_token_request::FLEXIBLE_MIN, renew_delegation_token_adapter));
registry.register(DispatchEntry::auth(ApiKey::ExpireDelegationToken, crabka_protocol::owned::expire_delegation_token_request::FLEXIBLE_MIN, expire_delegation_token_adapter));
registry.register(DispatchEntry::auth(ApiKey::DescribeDelegationToken, crabka_protocol::owned::describe_delegation_token_request::FLEXIBLE_MIN, describe_delegation_token_adapter));
```

- [ ] **Step 7: Extend dispatch helper to execute decoded and auth entries**

In `dispatch_registered_bytes`, add these arms:

```rust
crate::handlers::DispatchKind::DecodedContext(handler)
| crate::handlers::DispatchKind::EncodedContext(handler) => {
    let ctx = crate::handlers::RequestContext::new(
        principal_or_anonymous(auth),
        peer,
        parsed.client_id.unwrap_or(""),
        false,
        listener_name,
    );
    Some(handler(
        broker,
        parsed.api_version,
        parsed.correlation_id,
        parsed.body,
        &ctx,
    )
    .await
    .map(|body| encode_response(parsed.api_key, parsed.correlation_id, parsed.body_flexible, &body)))
}
crate::handlers::DispatchKind::Auth(handler) => Some(handler(
    broker,
    parsed.api_version,
    parsed.correlation_id,
    parsed.body,
    auth,
    peer,
)
.await
.map(|body| encode_response(parsed.api_key, parsed.correlation_id, parsed.body_flexible, &body))),
```

- [ ] **Step 8: Run tests and remove migrated old-match arms**

Run: `cargo test -p crabka-broker handlers::registry::tests --lib`

Expected: PASS.

Run: `cargo test -p crabka-broker network::dispatch::tests::raft_voter_dispatch_arms_route_to_real_handlers --lib`

Expected: PASS.

After the tests pass, delete the old inline match arms and `handle_*_frame` functions for the decoded and auth-gated APIs migrated in this task, including `handle_alter_replica_log_dirs_frame`. Keep Fetch and SASL functions. Keep `dispatch_one` for plain fallback until Task 6.

Run the same two commands again.

Expected: PASS after deletion.

- [ ] **Step 9: Commit**

```bash
git add crates/broker/src/handlers/registry.rs crates/broker/src/network/dispatch.rs
git commit -m "refactor(broker): route decoded and auth handlers through registry"
```

---

### Task 6: Final Registry Dispatch Collapse

**Files:**
- Modify: `crates/broker/src/handlers/registry.rs`
- Modify: `crates/broker/src/network/dispatch.rs`
- Modify: `crates/broker/src/network/request.rs`
- Modify: `crates/broker/src/broker.rs`

**Interfaces:**
- Consumes: all registry handler families.
- Produces: one registry-backed dispatch path, no ordinary per-API frame wrappers, no standalone `handler_body_flexible` table in `dispatch.rs`.

- [ ] **Step 1: Add flexible metadata tests to registry**

Move the representative cases from `network::dispatch::tests::handler_body_flexible_matches_selected_schema_boundaries` into `handlers::registry::tests`:

```rust
#[test]
fn registry_body_flexible_matches_selected_schema_boundaries() {
    use crabka_protocol::owned;

    let registry = build_registry();
    let cases = [
        (0, owned::produce_request::FLEXIBLE_MIN - 1, false),
        (0, owned::produce_request::FLEXIBLE_MIN, true),
        (1, owned::fetch_request::FLEXIBLE_MIN - 1, false),
        (1, owned::fetch_request::FLEXIBLE_MIN, true),
        (36, owned::sasl_authenticate_request::FLEXIBLE_MIN - 1, false),
        (36, owned::sasl_authenticate_request::FLEXIBLE_MIN, true),
        (17, i16::MAX, false),
        (999, 0, false),
    ];

    for (api_key, version, want) in cases {
        assert!(
            registry.body_flexible(api_key, version) == want,
            "api_key {api_key} version {version}"
        );
    }
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test -p crabka-broker handlers::registry::tests::registry_body_flexible_matches_selected_schema_boundaries --lib`

Expected: FAIL because `DispatchRegistry::body_flexible` does not exist and Fetch/SASL metadata is not registered.

- [ ] **Step 3: Add metadata-only entries for Fetch and SASL**

Add `DispatchKind::Fetch` and `DispatchKind::SaslMetadata` variants:

```rust
#[derive(Clone, Copy)]
pub(crate) enum DispatchKind {
    Plain(PlainHandler),
    Context(ContextHandler),
    Produce(ProduceHandler),
    Telemetry(TelemetryHandler),
    DecodedContext(ContextHandler),
    EncodedContext(ContextHandler),
    Auth(AuthHandler),
    Fetch,
    SaslMetadata,
}
```

Add constructors and body-flexible lookup:

```rust
impl DispatchEntry {
    pub(crate) fn fetch(flexible_min: ApiVersion) -> Self {
        Self {
            api_key: ApiKey::Fetch,
            flexible_min,
            quota_policy: RequestQuotaPolicy::SelfAccounted,
            kind: DispatchKind::Fetch,
        }
    }

    pub(crate) fn sasl_metadata(api_key: ApiKey, flexible_min: ApiVersion) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::SaslMetadata,
        }
    }
}

impl DispatchRegistry {
    pub(crate) fn body_flexible(&self, api_key: ApiKeyCode, version: ApiVersion) -> bool {
        self.get(api_key)
            .is_some_and(|entry| entry.body_flexible(version))
    }
}
```

Register Fetch and the SASL pair:

```rust
registry.register(DispatchEntry::fetch(
    crabka_protocol::owned::fetch_request::FLEXIBLE_MIN,
));
registry.register(DispatchEntry::sasl_metadata(
    ApiKey::SaslHandshake,
    i16::MAX,
));
registry.register(DispatchEntry::sasl_metadata(
    ApiKey::SaslAuthenticate,
    crabka_protocol::owned::sasl_authenticate_request::FLEXIBLE_MIN,
));
```

The `DispatchEntry::body_flexible` method must keep `flexible_min != i16::MAX` in the condition so `SaslHandshake` remains non-flexible even at `i16::MAX`.

Update the existing `dispatch_registered_bytes` match so it stays exhaustive after adding these metadata-only variants:

```rust
crate::handlers::DispatchKind::Fetch | crate::handlers::DispatchKind::SaslMetadata => None,
```

- [ ] **Step 4: Use `ParsedRequest` in the connection loop**

In `serve_connection_stream`, replace calls to local `parse_request_header`, `peek_api_key`, and `peek_client_id` with `crate::network::request::parse_request`, `crate::network::request::peek_api_key`, and `parsed.client_id.unwrap_or("")`.

The parse call should use the registry metadata:

```rust
let parsed = match crate::network::request::parse_request(&frame, |api_key, version| {
    broker.handlers().body_flexible(api_key, version)
}) {
    Ok(parsed) => parsed,
    Err(e) => {
        tracing::warn!(error = %e, "request parse error, closing");
        break;
    }
};
```

For request tracing, use `parsed.api_key`, `parsed.api_version`, `parsed.correlation_id`, `parsed.client_id`, and `peer` directly.

For KIP-511 capture, use `parsed.api_key == API_VERSIONS_KEY`, `parsed.api_version >= 3`, and `parsed.body`.

For quota patching, use `parsed.api_key`, `parsed.api_version`, and `parsed.client_id.unwrap_or("")`.

- [ ] **Step 5: Replace `dispatch_one` with registry execution for all non-SASL entries**

Add a helper that returns bytes for every non-Fetch, non-SASL metadata entry:

```rust
async fn dispatch_registry_response(
    broker: &Broker,
    entry: crate::handlers::DispatchEntry,
    parsed: &crate::network::request::ParsedRequest<'_>,
    frame: &Bytes,
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
    listener_name: &str,
    client_software_name: &str,
    client_software_version: &str,
) -> Result<Option<Bytes>, BrokerError> {
    match dispatch_registered_bytes(
        broker,
        entry,
        parsed,
        frame,
        auth,
        peer,
        listener_name,
        client_software_name,
        client_software_version,
    )
    .await
    {
        Some(result) => result.map(Some),
        None => match entry.kind() {
            crate::handlers::DispatchKind::Plain(handler) => {
                let body = handler(
                    broker,
                    parsed.api_version,
                    parsed.correlation_id,
                    parsed.body,
                )
                .await?;
                Ok(Some(encode_response(
                    parsed.api_key,
                    parsed.correlation_id,
                    parsed.body_flexible,
                    &body,
                )))
            }
            crate::handlers::DispatchKind::Fetch | crate::handlers::DispatchKind::SaslMetadata => Ok(None),
            _ => Ok(None),
        },
    }
}
```

Replace the old fallback `dispatch_one` call with registry lookup:

```rust
let Some(entry) = broker.handlers().get(parsed.api_key) else {
    tracing::warn!(api_key = parsed.api_key, api_version = parsed.api_version, "unsupported api, returning error");
    broker.metrics.record_unsupported_api_request(parsed.api_key);
    let mut buf = BytesMut::with_capacity(2);
    buf.put_i16(codes::UNSUPPORTED_VERSION);
    let mut response_bytes = encode_response(
        parsed.api_key,
        parsed.correlation_id,
        parsed.body_flexible,
        &buf.freeze(),
    );
    response_bytes = maybe_apply_request_quota(
        broker,
        response_bytes,
        &parsed,
        &auth,
        std::time::Instant::now(),
    )
    .await;
    if let Err(e) = framed.send(response_bytes).await {
        tracing::warn!(error = %e, "framed.send error, closing");
        break;
    }
    continue;
};
```

Extract the current post-handler request quota code into:

```rust
async fn maybe_apply_request_quota(
    broker: &Broker,
    mut response_bytes: Bytes,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &crate::network::auth::ConnectionAuth,
    started: std::time::Instant,
) -> Bytes {
    let elapsed_micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    let self_accounts = matches!(
        ApiKey::from_i16(parsed.api_key),
        Some(ApiKey::Produce | ApiKey::Fetch)
    );
    if !self_accounts && let Some(principal) = auth.principal() {
        let image = broker.controller.current_image();
        let delay = crate::quota::consume_request_quota(
            &image,
            &broker.quota_buckets,
            &principal.name,
            parsed.client_id.unwrap_or(""),
            elapsed_micros,
        );
        if delay > std::time::Duration::ZERO {
            if throttle_is_leading_field(parsed.api_key, parsed.api_version) {
                let delay_ms = i32::try_from(delay.as_millis()).unwrap_or(i32::MAX);
                response_bytes = patch_leading_throttle(
                    response_bytes,
                    parsed.api_key,
                    parsed.api_version,
                    delay_ms,
                );
            }
            tokio::time::sleep(delay).await;
        }
    }
    response_bytes
}
```

Only call `maybe_apply_request_quota` when `entry.quota_policy() == RequestQuotaPolicy::ApplyFallbackAccounting`. This preserves inline exemptions and self-accounting.

- [ ] **Step 6: Keep Fetch write-plan path explicit**

Replace the old Fetch match arm with a direct check after registry lookup:

```rust
if matches!(entry.kind(), crate::handlers::DispatchKind::Fetch) {
    let sendfile_capable = crate::network::fetch_writer::SendfileSink::is_sendfile_capable(
        framed.get_ref(),
    );
    match handle_fetch_frame_from_parsed(&broker, &parsed, &auth, &peer, sendfile_capable)
        .instrument(req_span.clone())
        .await
    {
        Ok(ops) => {
            if let Err(e) = SinkExt::<Bytes>::flush(&mut framed).await {
                tracing::warn!(error = %e, "framed.flush error before fetch plan, closing");
                break;
            }
            if let Err(e) = crate::network::fetch_writer::write_fetch_plan(framed.get_mut(), ops).await {
                tracing::warn!(error = %e, "fetch plan write error, closing");
                break;
            }
            continue;
        }
        Err(e) => {
            tracing::warn!(error = %e, "Fetch dispatch error, closing connection");
            break;
        }
    }
}
```

Change `handle_fetch_frame` to accept `ParsedRequest` instead of reparsing the frame:

```rust
async fn handle_fetch_frame_from_parsed(
    broker: &Broker,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
    sendfile_capable: bool,
) -> Result<Vec<crate::network::fetch_writer::WriteOp>, BrokerError> {
    use crate::network::fetch_writer::{WriteOp, build_fetch_plan};

    let principal = principal_or_anonymous(auth);
    let ctx = crate::handlers::RequestContext::new(
        principal,
        peer,
        parsed.client_id.unwrap_or(""),
        sendfile_capable && parsed.api_version >= 4,
        "",
    );
    let (resp, version) = crate::handlers::fetch::handle(
        broker,
        parsed.api_version,
        parsed.correlation_id,
        parsed.body,
        &ctx,
    )
    .await?;

    if version < 4 {
        let body_bytes = crate::handlers::fetch::encode_fetch_response(resp, version)?;
        let framed = encode_response(
            parsed.api_key,
            parsed.correlation_id,
            parsed.body_flexible,
            &body_bytes,
        );
        let mut framed_with_len = BytesMut::with_capacity(4 + framed.len());
        framed_with_len.put_u32(u32::try_from(framed.len()).map_err(|_| {
            BrokerError::Io(std::io::Error::other("fetch response exceeds max frame size"))
        })?);
        framed_with_len.put_slice(&framed);
        return Ok(vec![WriteOp::Inline(framed_with_len.freeze())]);
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    {
        if sendfile_capable && parsed.api_version >= 4 {
            return build_fetch_plan(
                &resp,
                version,
                parsed.correlation_id,
                parsed.body_flexible,
                crate::network::fetch_writer::resolve_records_sendfile,
            );
        }
    }

    build_fetch_plan(
        &resp,
        version,
        parsed.correlation_id,
        parsed.body_flexible,
        crate::network::fetch_writer::resolve_records_inline,
    )
}
```

- [ ] **Step 7: Delete old duplication**

Delete from `crates/broker/src/network/dispatch.rs` after all tests in this task pass once:

- The old inline `match peek_api_key(&frame).ok().and_then(ApiKey::from_i16)` block.
- The old `dispatch_one` function.
- The local `parse_request_header`, `peek_api_key`, and `peek_client_id` functions.
- The old `handler_body_flexible` function and its dispatch-local test.
- Every `handle_*_frame` wrapper except the SASL functions and the new `handle_fetch_frame_from_parsed` helper.

Do not delete `encode_response`, `throttle_is_leading_field`, `patch_leading_throttle`, `InFlightGuard`, or `ActiveConnectionGuard`.

- [ ] **Step 8: Rename behavior tests that mention inline arms**

In `network::dispatch::tests`, rename `raft_voter_dispatch_arms_route_to_real_handlers` to `raft_voter_registry_routes_to_real_handlers`. Keep the socket-driven test body and assertions. The test should still prove `AddRaftVoter`, `RemoveRaftVoter`, and `UpdateRaftVoter` reach real handlers and do not fall through to `UNSUPPORTED_VERSION`.

- [ ] **Step 9: Run focused tests**

Run: `cargo test -p crabka-broker handlers::registry::tests --lib`

Expected: PASS.

Run: `cargo test -p crabka-broker network::request::tests --lib`

Expected: PASS.

Run: `cargo test -p crabka-broker network::dispatch::tests --lib`

Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add crates/broker/src/handlers/registry.rs crates/broker/src/network/dispatch.rs crates/broker/src/network/request.rs crates/broker/src/broker.rs
git commit -m "refactor(broker): collapse dispatch onto registry"
```

---

### Task 7: Full Verification and Documentation Touch-Up

**Files:**
- Modify only files already changed by Tasks 1-6 if formatting, imports, or comments need cleanup.

**Interfaces:**
- Consumes: complete registry dispatch implementation.
- Produces: formatted, tested, clippy-clean refactor ready for review.

- [ ] **Step 1: Run broker tests**

Run: `cargo test -p crabka-broker`

Expected: PASS.

- [ ] **Step 2: Run focused protocol-adjacent tests if broker tests expose no failures**

Run: `cargo test -p crabka-broker network::dispatch::tests --lib`

Expected: PASS.

Run: `cargo test -p crabka-broker handlers::registry::tests --lib`

Expected: PASS.

- [ ] **Step 3: Format**

Run: `cargo +nightly fmt --all -- --check`

Expected: PASS.

If it fails, run `cargo +nightly fmt --all`, inspect the diff, then rerun `cargo +nightly fmt --all -- --check` and expect PASS.

- [ ] **Step 4: Clippy**

Run: `cargo clippy -p crabka-broker --all-targets -- -D warnings`

Expected: PASS.

- [ ] **Step 5: Inspect LOC and diff for reviewability**

Run: `git diff --stat`

Expected: `crates/broker/src/network/dispatch.rs` has a net line reduction, `crates/broker/src/handlers/registry.rs` is the main new routing table, and no generated protocol files changed.

Run: `git diff -- crates/broker/src/network/dispatch.rs crates/broker/src/handlers/registry.rs crates/broker/src/network/request.rs crates/broker/src/handlers/context.rs crates/broker/src/broker.rs`

Expected: Diff shows behavior-preserving dispatch movement, not handler business-logic changes.

- [ ] **Step 6: Commit verification cleanup**

```bash
git add crates/broker/src/network/dispatch.rs crates/broker/src/handlers/registry.rs crates/broker/src/network/request.rs crates/broker/src/handlers/context.rs crates/broker/src/broker.rs crates/broker/src/network/mod.rs crates/broker/src/handlers/mod.rs
git commit -m "chore(broker): verify dispatch registry refactor"
```

If there are no changes after verification, skip this commit and record the passing commands in the final implementation summary.
