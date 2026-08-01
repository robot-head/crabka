# Client Core Connection Resource Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace client-core's dispatch-capacity, accepted-frame, isolated-fetch-minimum, and SASL-client-id constants with validated typed policy while preserving existing defaults.

**Architecture:** Add three small validated value types at their existing ownership boundaries, store connection-wide values in `ConnectionOptions`, and lower UOM values only at Tokio/Kafka protocol calls. Reuse the configured connection client id for SASL, and update existing `IsolatedFetch` constructors to select the typed one-byte default; deployment-specific propagation remains in separate plans after this generic phase is stable.

**Tech Stack:** Rust, `refined_type`, `crabka-units`, Tokio, `tokio-util`, Bon builders, Cargo tests.

## Global Constraints

- Preserve defaults exactly: dispatch queue `64`, accepted frame maximum `100MiB`, and isolated-fetch minimum `1B`.
- Keep the accepted-frame security ceiling fixed at `100MiB`; reject larger requested values.
- Use `refined_type` for positive newtype invariants.
- Dimensioned inputs use `crabka_units::ByteSize`; reject non-finite and fractional-byte values.
- `FetchMinBytes` must fit Kafka's positive signed `i32` `min_bytes` field.
- SASL reuses `ConnectionOptions.client_id`; do not add a SASL-specific setting.
- Libraries do not read environment variables.
- Preserve the four unrelated untracked plans dated `2026-07-28`.
- Run Cargo with `TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.

---

## File Structure

- `crates/client-core/src/connection.rs`: owns dispatch-capacity and frame-limit types, defaults, `ConnectionOptions`, and application to live connections.
- `crates/client-core/src/transport.rs`: converts typed frame policy into Tokio length-delimited codecs.
- `crates/client-core/src/sasl.rs`: reuses the configured client id and bounds SASL frame allocation.
- `crates/client-core/src/fetch.rs`: owns `FetchMinBytes`, its default, and application to `IsolatedFetch`.
- `crates/client-core/src/client.rs`: validates raw builder inputs and constructs typed `ConnectionOptions`.
- `crates/client-core/src/lib.rs`: exports the new public types and defaults.
- Existing production `IsolatedFetch` call sites: explicitly select the typed default until owner propagation plans add overrides.
- `docs/configuration-audit.md`: records closure of the four client-core scanner rows.

---

### Task 1: Add Validated Connection Policy Types

**Files:**
- Modify: `crates/client-core/src/connection.rs`
- Modify: `crates/client-core/src/lib.rs`

**Interfaces:**
- Produces: `ConnectionDispatchQueueCapacity::new(usize) -> Result<Self, String>`
- Produces: `ConnectionDispatchQueueCapacity::get(self) -> usize`
- Produces: `ClientFrameMax::try_from(ByteSize) -> Result<Self, String>`
- Produces: `ClientFrameMax::bytes(self) -> usize`
- Produces: `ClientFrameMax::size(self) -> ByteSize`
- Produces: `DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY: usize`
- Produces: `DEFAULT_CLIENT_FRAME_MAX: ByteSize`
- Produces: fixed `MAX_CLIENT_FRAME_BYTES: ByteSize`

- [ ] **Step 1: Write failing scalar-policy tests**

Add tests beside the existing `ClientDnsTimeout` tests:

```rust
#[test]
fn connection_resource_defaults_preserve_existing_values() {
    assert!(
        ConnectionDispatchQueueCapacity::default().get()
            == DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY
    );
    assert!(ConnectionDispatchQueueCapacity::default().get() == 64);
    assert!(ClientFrameMax::default().size() == mebibytes(100));
    assert!(MAX_CLIENT_FRAME_BYTES == mebibytes(100));
}

#[test]
fn connection_resource_policy_validates_boundaries() {
    assert!(ConnectionDispatchQueueCapacity::new(0).is_err());
    assert!(ConnectionDispatchQueueCapacity::new(7).unwrap().get() == 7);

    assert!(ClientFrameMax::try_from(bytes(0)).is_err());
    assert!(ClientFrameMax::try_from(ByteSize::from_bytes_f64(1.5)).is_err());
    assert!(ClientFrameMax::try_from(mebibytes(100) + bytes(1)).is_err());
    assert!(ClientFrameMax::try_from(kibibytes(32)).unwrap().size() == kibibytes(32));
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core --lib connection::tests::connection_resource --locked
```

Expected: compilation fails because the new constants and types do not exist.

- [ ] **Step 3: Implement the minimum validated types**

In `connection.rs`, reuse `GreaterUsize::<0>` and store integers so the types
remain `Copy + Eq`:

```rust
pub const DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY: usize = 64;
pub const MAX_CLIENT_FRAME_BYTES: ByteSize = mebibytes(100);
pub const DEFAULT_CLIENT_FRAME_MAX: ByteSize = MAX_CLIENT_FRAME_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionDispatchQueueCapacity(usize);

impl ConnectionDispatchQueueCapacity {
    pub fn new(value: usize) -> Result<Self, String> {
        GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("client dispatch queue capacity: {error}"))
    }

    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

impl Default for ConnectionDispatchQueueCapacity {
    fn default() -> Self {
        Self::new(DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY)
            .expect("default client dispatch queue capacity is valid")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientFrameMax(usize);
```

Implement `TryFrom<ByteSize>` with one explicit check:

```rust
let bytes = value.bytes_f64();
if !bytes.is_finite()
    || bytes.fract() != 0.0
    || !(1.0..=MAX_CLIENT_FRAME_BYTES.bytes_f64()).contains(&bytes)
{
    return Err("client frame max must be a positive whole-byte value no greater than 100MiB".into());
}
let bytes = usize::try_from(value.bytes_u64())
    .map_err(|_| "client frame max does not fit usize".to_owned())?;
Ok(Self(bytes))
```

Add `bytes`, `size`, and `Default`. Export both types and all three constants
from `lib.rs`.

- [ ] **Step 4: Run the focused tests**

Run the command from Step 2.

Expected: both tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/client-core/src/connection.rs crates/client-core/src/lib.rs
git commit -m "feat(client): validate connection resource policy"
```

---

### Task 2: Apply the Configured Frame Limit to Normal Transport

**Files:**
- Modify: `crates/client-core/src/transport.rs`
- Modify: `crates/client-core/src/connection.rs`

**Interfaces:**
- Consumes: `ClientFrameMax`
- Produces: `transport::codec_with_max(ClientFrameMax) -> LengthDelimitedCodec`
- Preserves: `transport::codec()` as a default compatibility wrapper

- [ ] **Step 1: Write failing exact-limit transport tests**

Keep the existing roundtrip tests and add:

```rust
#[tokio::test]
async fn configured_codec_rejects_a_frame_over_its_limit() {
    let max = ClientFrameMax::try_from(bytes(8)).unwrap();
    let mut codec = codec_with_max(max);
    let mut input = BytesMut::from(&[0, 0, 0, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8][..]);

    let error = codec.decode(&mut input).expect_err("nine-byte frame exceeds eight-byte max");
    assert!(error.to_string().contains("frame size too big"));
}

#[test]
fn configured_codec_accepts_the_exact_limit() {
    let max = ClientFrameMax::try_from(bytes(8)).unwrap();
    let mut codec = codec_with_max(max);
    let mut input = BytesMut::from(&[0, 0, 0, 8, 0, 1, 2, 3, 4, 5, 6, 7][..]);

    assert!(codec.decode(&mut input).unwrap().unwrap().len() == 8);
}
```

- [ ] **Step 2: Run the tests to verify failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core --lib transport::tests::configured_codec --locked
```

Expected: compilation fails because `codec_with_max` does not exist.

- [ ] **Step 3: Add the typed codec constructor**

Replace the literal-backed constructor with:

```rust
pub fn codec_with_max(max: ClientFrameMax) -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .length_field_offset(0)
        .length_field_length(4)
        .length_field_type::<u32>()
        .max_frame_length(max.bytes())
        .big_endian()
        .new_codec()
}

pub fn codec() -> LengthDelimitedCodec {
    codec_with_max(ClientFrameMax::default())
}
```

Remove `MAX_FRAME_BYTES`. Update its default-pinning test to use
`ClientFrameMax::default()`.

- [ ] **Step 4: Thread the typed limit into both I/O halves**

Change `spawn_io_tasks` to accept `frame_max: ClientFrameMax`. Construct both
`FramedRead` and `FramedWrite` with `codec_with_max(frame_max)`. Pass
`options.frame_max` from `Connection::from_stream`.

- [ ] **Step 5: Run transport and connection tests**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core --lib transport --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core --lib connection --locked
```

Expected: both commands pass.

- [ ] **Step 6: Commit**

```bash
git add crates/client-core/src/transport.rs crates/client-core/src/connection.rs
git commit -m "feat(client): bound configured frames"
```

---

### Task 3: Store Connection Policy in Options and the Client Builder

**Files:**
- Modify: `crates/client-core/src/connection.rs`
- Modify: `crates/client-core/src/client.rs`
- Modify: all `ConnectionOptions` literals reported by `rg -n 'ConnectionOptions \\{' crates --glob '*.rs'`

**Interfaces:**
- Consumes: the two Task 1 policy types
- Produces: `ConnectionOptions.dispatch_queue_capacity`
- Produces: `ConnectionOptions.frame_max`
- Produces: Bon builder inputs `dispatch_queue_capacity: usize` and `frame_max: ByteSize`

- [ ] **Step 1: Write failing builder tests**

Add in `client.rs`:

```rust
#[tokio::test]
async fn invalid_connection_resource_policy_fails_before_resolution() {
    let queue_error = Client::builder()
        .bootstrap("invalid.invalid:9092")
        .dispatch_queue_capacity(0)
        .build()
        .await
        .expect_err("zero queue capacity");
    assert!(queue_error.to_string().contains("dispatch queue capacity"));

    let frame_error = Client::builder()
        .bootstrap("invalid.invalid:9092")
        .frame_max(mebibytes(100) + bytes(1))
        .build()
        .await
        .expect_err("frame limit above fixed ceiling");
    assert!(frame_error.to_string().contains("client frame max"));
}
```

- [ ] **Step 2: Run the test to verify failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core --lib client::tests::invalid_connection_resource_policy_fails_before_resolution --locked
```

Expected: compilation fails because the builder setters do not exist.

- [ ] **Step 3: Extend `ConnectionOptions` and its default**

Add:

```rust
pub dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
pub frame_max: ClientFrameMax,
```

and initialize both with `Default::default()`.

- [ ] **Step 4: Validate builder inputs once**

Add to `Client::start`:

```rust
#[builder(default = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY)]
dispatch_queue_capacity: usize,
#[builder(default = DEFAULT_CLIENT_FRAME_MAX)]
frame_max: ByteSize,
```

Construct the two validated types before bootstrap resolution and store them in
`ConnectionOptions`. Map errors to `ClientError::InvalidConfig`.

- [ ] **Step 5: Replace the dispatch literal**

In `Connection::from_stream`, replace:

```rust
mpsc::channel::<DispatchItem>(64)
```

with:

```rust
mpsc::channel::<DispatchItem>(options.dispatch_queue_capacity.get())
```

- [ ] **Step 6: Make every existing options literal preserve defaults**

For each compile-reported `ConnectionOptions` literal, add:

```rust
dispatch_queue_capacity: Default::default(),
frame_max: Default::default(),
```

Do not add deployment inputs in this phase. These explicit defaults are
compatibility updates only.

- [ ] **Step 7: Run the client-core test and workspace check**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo check --workspace --all-targets --locked
```

Expected: both commands pass.

- [ ] **Step 8: Commit**

```bash
git add crates
git commit -m "feat(client): carry connection resource policy"
```

---

### Task 4: Reuse Client Policy During SASL

**Files:**
- Modify: `crates/client-core/src/sasl.rs`
- Modify: `crates/client-core/src/connection.rs`

**Interfaces:**
- Consumes: `&ConnectionOptions.client_id`
- Consumes: `ConnectionOptions.frame_max`
- Produces: `outbound_sasl(stream, credentials, server_name, client_id, frame_max)`

- [ ] **Step 1: Replace the fixed-id assertion with a configured-id test**

Change the existing request-header helper to accept `expected_client_id: &str`.
In the PLAIN handshake test, call:

```rust
outbound_sasl(
    &mut client,
    &creds,
    "localhost",
    "configured-sasl-client",
    ClientFrameMax::default(),
)
```

and assert both handshake and authenticate request headers contain
`"configured-sasl-client"`.

- [ ] **Step 2: Add a pre-allocation response-limit test**

Call `round_trip` over `tokio::io::duplex`, have the peer announce a response
length of nine bytes without sending a payload, and use an eight-byte
`ClientFrameMax`. Assert the call returns promptly with:

```rust
assert!(error.to_string().contains("announced 9"));
assert!(error.to_string().contains("maximum 8"));
```

This distinguishes pre-allocation rejection from a blocked `read_exact`.

- [ ] **Step 3: Add an outbound request-limit test**

Call `round_trip` with a body that makes the complete encoded request nine
bytes and an eight-byte `ClientFrameMax`. Assert the peer receives no bytes and
the error reports the encoded size and configured maximum. This proves the
same policy covers SASL writes, not only response allocation.

- [ ] **Step 4: Run the tests to verify failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core --lib sasl::tests --locked
```

Expected: configured-id assertions fail or the new signatures do not compile,
and oversized request/response frames are not rejected.

- [ ] **Step 5: Thread two borrowed/copy policy values through SASL helpers**

Delete `OUTBOUND_CLIENT_ID`. Add `client_id: &str` and
`frame_max: ClientFrameMax` to `outbound_sasl`, `round_trip`, and the private
mechanism helpers that call `round_trip`. Pass the same values through each
round without storing a second policy object.

Encode `client_id` in the request header. After `read_u32`, reject before
allocation:

```rust
if frame.len() > frame_max.bytes() {
    return Err(OutboundSaslError::Codec(format!(
        "SASL request encoded {} bytes, maximum {}",
        frame.len(),
        frame_max.bytes()
    )));
}

let resp_len = usize::try_from(stream.read_u32().await?)
    .map_err(|_| OutboundSaslError::Codec("SASL response length does not fit usize".into()))?;
if resp_len > frame_max.bytes() {
    return Err(OutboundSaslError::Codec(format!(
        "SASL response announced {resp_len} bytes, maximum {}",
        frame_max.bytes()
    )));
}
let mut resp = vec![0u8; resp_len];
```

The fixed ceiling is already guaranteed by `ClientFrameMax`.

- [ ] **Step 6: Pass connection policy into SASL**

In `Connection::connect_secured`, call:

```rust
crate::sasl::outbound_sasl(
    &mut *stream,
    creds,
    server_name,
    &options.client_id,
    options.frame_max,
)
```

- [ ] **Step 7: Run SASL and connection tests**

Run the command from Step 4, then:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core --lib connection --locked
```

Expected: both pass.

- [ ] **Step 8: Commit**

```bash
git add crates/client-core/src/sasl.rs crates/client-core/src/connection.rs
git commit -m "fix(client): reuse policy in SASL"
```

---

### Task 5: Add the Typed Isolated-Fetch Minimum

**Files:**
- Modify: `crates/client-core/src/fetch.rs`
- Modify: `crates/client-core/src/lib.rs`
- Modify: `crates/client-streams/src/runtime/io_broker.rs`
- Modify: `crates/gres-control/src/registry.rs`
- Modify: `crates/gres-fdw/src/source.rs`
- Modify: `crates/gres-substrate/src/recovery.rs`
- Modify: `crates/gres/src/lib.rs`
- Modify: existing tests that construct `IsolatedFetch`

**Interfaces:**
- Produces: `FetchMinBytes::try_from(ByteSize) -> Result<Self, String>`
- Produces: `FetchMinBytes::bytes(self) -> i32`
- Produces: `FetchMinBytes::size(self) -> ByteSize`
- Produces: `DEFAULT_FETCH_MIN: ByteSize = bytes(1)`
- Extends: `IsolatedFetch.fetch_min: FetchMinBytes`

- [ ] **Step 1: Write failing validation and request tests**

Add:

```rust
#[test]
fn fetch_min_validates_protocol_boundaries() {
    assert!(FetchMinBytes::default().size() == bytes(1));
    assert!(FetchMinBytes::try_from(bytes(0)).is_err());
    assert!(FetchMinBytes::try_from(ByteSize::from_bytes_f64(1.5)).is_err());
    assert!(FetchMinBytes::try_from(ByteSize::from_bytes_i64(i64::from(i32::MAX) + 1)).is_err());
    assert!(FetchMinBytes::try_from(bytes(17)).unwrap().bytes() == 17);
}

#[test]
fn isolated_fetch_uses_configured_minimum() {
    let request = build_fetch_request(IsolatedFetch {
        fetch_min: FetchMinBytes::try_from(bytes(17)).unwrap(),
        topic: "orders",
        topic_id: WireUuid([7; 16]),
        partition: 3,
        fetch_offset: 123,
        max_wait: millis(250),
        max: kibibytes(96),
        partition_max: kibibytes(64),
        isolation_level: 1,
    });
    assert!(request.min_bytes == 17);
}
```

- [ ] **Step 2: Run the tests to verify failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core --lib fetch::tests --locked
```

Expected: compilation fails because `FetchMinBytes` and `fetch_min` do not
exist.

- [ ] **Step 3: Implement the type by reusing the existing consumer pattern**

Use `GreaterI32::<0>`, the same finite/fractional/range check used by
`ConsumerFetchMaxBytes`, and these accessors:

```rust
pub const fn bytes(self) -> i32 { self.0 }
pub fn size(self) -> ByteSize { ByteSize::from_bytes_i64(i64::from(self.0)) }
```

Implement `Default` from `DEFAULT_FETCH_MIN`. Export the type and constant.

- [ ] **Step 4: Apply the typed value to request construction**

Add `pub fetch_min: FetchMinBytes` to `IsolatedFetch` and replace
`min_bytes: 1` with `min_bytes: fetch.fetch_min.bytes()`.

- [ ] **Step 5: Preserve behavior at every existing constructor**

Use:

```rust
fetch_min: FetchMinBytes::default(),
```

in every current `IsolatedFetch` literal found by:

```bash
rg -n 'IsolatedFetch \\{' crates --glob '*.rs'
```

Do not add CLI, environment, CRD, or higher-level policy fields in this phase.

- [ ] **Step 6: Run affected-package tests**

```bash
for package in \
  crabka-client-core crabka-client-streams crabka-gres-control \
  crabka-gres-fdw crabka-gres-substrate crabka-gres
do
  TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
    cargo test -p \"$package\" --all-targets --locked || exit 1
done
```

Expected: all commands pass.

- [ ] **Step 7: Commit**

```bash
git add crates
git commit -m "feat(client): type isolated fetch minimum"
```

---

### Task 6: Verify and Record the Generic Phase

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes: all Task 1-5 behavior
- Produces: an audit record distinguishing generic closure from deployment propagation

- [ ] **Step 1: Scan the four original constants**

```bash
rg -n \
  'mpsc::channel::<DispatchItem>\\(64\\)|min_bytes: 1|MAX_FRAME_BYTES|OUTBOUND_CLIENT_ID' \
  crates/client-core/src
```

Expected: no production matches. Test fixtures may use literal boundary
inputs only when the literal is the subject of the test.

- [ ] **Step 2: Run the complete generic verification gate**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo check --workspace --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo +nightly fmt --all
git diff --check
```

Expected: every command exits zero and formatting produces no unexpected
changes.

- [ ] **Step 3: Append the audit evidence**

Record in `docs/configuration-audit.md`:

- the three validated types and exact defaults;
- the fixed `100MiB` security ceiling;
- normal and SASL frame enforcement;
- reuse of the configured SASL client id;
- typed `IsolatedFetch` minimum with compatibility defaults at current owners;
- the exact commands and results from Step 2; and
- that library propagation, deployment CLI/environment, and Kafka/Gres CRD
  exposure remain open and will be planned as separate independently testable
  phases.

- [ ] **Step 4: Check and commit the audit**

```bash
git diff --check
git add docs/configuration-audit.md
git commit -m "docs(config): record client resource policy"
```

Expected: the commit contains only the audit update.

---

## Follow-On Plans Required by the Approved Design

After this plan passes, write separate plans for:

1. producer/admin/streams/Gres library propagation, including secondary
   clients;
2. standalone binary CLI/environment ownership; and
3. Kafka/Gres CRD validation and operator rendering.

Splitting here avoids mixing generic wire-safety changes with dozens of
deployment surfaces and gives each boundary its own runnable review gate.
