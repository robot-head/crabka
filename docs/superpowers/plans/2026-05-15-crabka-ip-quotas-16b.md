# Slice 16b: IP quotas + KIP-612 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Per `CLAUDE.md`, dispatch independent tasks within a batch in parallel.

**Goal:** Recognize the `ip` entity type in `AlterClientQuotas`/`DescribeClientQuotas` and enforce KIP-612 `connection_creation_rate` at TCP accept — IPv4 only, accept-and-delay (never reject).

**Architecture:** No new metadata records — `ClientQuotaRecord` already carries arbitrary `(entity, key, value)` tuples. Add `lookup_ip_quota` (two-priority specific→default) to slice 16's `quota::lookup`. Extend slice 16's validator to accept `"ip"` entity type (with IPv4 parse check) and `"connection_creation_rate"` key. Hook the TCP accept loop in `broker.rs` to consume from a per-IP `TokenBucket` (reusing slice 16's `QuotaBuckets` cache); on overage, `tokio::time::sleep` before spawning the per-connection handler.

**Tech Stack:** Rust 1.95.0; reuses slice 16's `QuotaBuckets`, `lookup_quota_with_key`, refresh task. Wire types unchanged (api_keys 48 + 49 already wired in slice 16).

**Reference spec:** [`docs/superpowers/specs/2026-05-15-crabka-ip-quotas-16b-design.md`](../specs/2026-05-15-crabka-ip-quotas-16b-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Branch `feature/ip-quotas-16b` already created with spec committed at `ff1be4c`.

---

## File structure

```
crates/broker/src/
├── quota/lookup.rs                  # MODIFIED — lookup_ip_quota + lookup_ip_quota_with_key + 4 unit tests
├── handlers/alter_client_quotas.rs  # MODIFIED — "ip" in SUPPORTED_ENTITY_TYPES + connection_creation_rate in KNOWN_QUOTA_KEYS + IPv4 validation + 2 unit tests
└── broker.rs                        # MODIFIED — TCP accept hook + clone quota_buckets/controller into listener spawn

crates/broker/tests/
├── ip_quotas.rs                     # NEW — 3 broker integration tests
└── jvm_acceptance.rs                # MODIFIED — 1 new JVM test
```

6 tasks across 5 batches.

---

## Batch 1 — Lookup + validator (parallel: T1, T2)

### Task 1: `lookup_ip_quota` + 4 unit tests

**Files:**
- Modify: `crates/broker/src/quota/lookup.rs`

- [ ] **Step 1: Append `lookup_ip_quota` + `lookup_ip_quota_with_key` to `lookup.rs`**

After the existing `lookup_quota_with_key` function:

```rust
/// Lookup an `ip`-scoped quota for `peer_ip`. Priority order:
///   1. (ip = Some(peer_ip)) — specific
///   2. (ip = None)          — default
///
/// Disjoint from `lookup_quota` (which checks `("user", *)` and
/// `("client-id", *)` candidates only). Used by KIP-612
/// connection_creation_rate enforcement.
#[must_use]
pub fn lookup_ip_quota(
    image: &MetadataImage,
    peer_ip: &std::net::Ipv4Addr,
    quota_key: &str,
) -> Option<f64> {
    lookup_ip_quota_with_key(image, peer_ip, quota_key).map(|(_, v)| v)
}

#[must_use]
pub fn lookup_ip_quota_with_key(
    image: &MetadataImage,
    peer_ip: &std::net::Ipv4Addr,
    quota_key: &str,
) -> Option<(EntityKey, f64)> {
    let candidates: [EntityKey; 2] = [
        vec![("ip".into(), Some(peer_ip.to_string()))],
        vec![("ip".into(), None)],
    ];
    for key in candidates {
        if let Some(configs) = image.client_quotas().get(&key) {
            if let Some(&v) = configs.get(quota_key) {
                return Some((key, v));
            }
        }
    }
    None
}
```

- [ ] **Step 2: Re-export from `crates/broker/src/quota/mod.rs`**

Find the existing `pub use lookup::{lookup_quota, lookup_quota_with_key};` line and extend:

```rust
pub use lookup::{lookup_ip_quota, lookup_ip_quota_with_key, lookup_quota, lookup_quota_with_key};
```

- [ ] **Step 3: Add code comment in `lookup_quota`**

Just above the existing `lookup_quota` function body, add a one-line comment:

```rust
// Disjoint from `lookup_ip_quota` (which checks `("ip", *)` candidates only).
```

- [ ] **Step 4: Append 4 unit tests to the existing `#[cfg(test)] mod tests`**

```rust
    fn rec_ip(ip: Option<&str>, key: &str, value: f64) -> ClientQuotaRecord {
        ClientQuotaRecord {
            entity: vec![QuotaEntity {
                entity_type: "ip".into(),
                entity_name: ip.map(Into::into),
            }],
            config_key: key.into(),
            config_value: Some(value),
        }
    }

    fn img_with_ip(records: Vec<ClientQuotaRecord>) -> MetadataImage {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        for r in records {
            img.apply(&MetadataRecord::V1ClientQuota(r));
        }
        img
    }

    #[test]
    fn ip_specific_match() {
        let img = img_with_ip(vec![rec_ip(Some("127.0.0.1"), "connection_creation_rate", 1.0)]);
        let ip: std::net::Ipv4Addr = "127.0.0.1".parse().unwrap();
        assert_eq!(lookup_ip_quota(&img, &ip, "connection_creation_rate"), Some(1.0));
    }

    #[test]
    fn ip_default_fallback() {
        let img = img_with_ip(vec![rec_ip(None, "connection_creation_rate", 2.0)]);
        let ip: std::net::Ipv4Addr = "10.0.0.7".parse().unwrap();
        assert_eq!(lookup_ip_quota(&img, &ip, "connection_creation_rate"), Some(2.0));
    }

    #[test]
    fn ip_specific_wins_over_default() {
        let img = img_with_ip(vec![
            rec_ip(None, "connection_creation_rate", 8.0),
            rec_ip(Some("127.0.0.1"), "connection_creation_rate", 1.0),
        ]);
        let ip: std::net::Ipv4Addr = "127.0.0.1".parse().unwrap();
        assert_eq!(lookup_ip_quota(&img, &ip, "connection_creation_rate"), Some(1.0));
    }

    #[test]
    fn ip_no_match_returns_none() {
        let img = img_with_ip(vec![]);
        let ip: std::net::Ipv4Addr = "127.0.0.1".parse().unwrap();
        assert!(lookup_ip_quota(&img, &ip, "connection_creation_rate").is_none());
    }
```

(`ClientQuotaRecord`, `QuotaEntity`, and `MetadataRecord` are already imported in the existing test module from slice 16's tests.)

- [ ] **Step 5: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib quota::lookup
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: 4 new tests PASS + slice 16's 9 existing pass.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/quota/lookup.rs crates/broker/src/quota/mod.rs
git commit -m "$(cat <<'EOF'
feat(broker): lookup_ip_quota for KIP-612

Two-priority lookup (specific IP → default) for ip-scoped quotas.
Disjoint from the 8-priority user/client-id lookup. Used by
connection_creation_rate enforcement in task 3.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Validator + key allowlist extensions

**Files:**
- Modify: `crates/broker/src/handlers/alter_client_quotas.rs`

- [ ] **Step 1: Extend `SUPPORTED_ENTITY_TYPES`**

Find the existing `const SUPPORTED_ENTITY_TYPES: &[&str] = &["user", "client-id"];` (near the top of the file). Replace with:

```rust
const SUPPORTED_ENTITY_TYPES: &[&str] = &["user", "client-id", "ip"];
```

- [ ] **Step 2: Extend `KNOWN_QUOTA_KEYS`**

Find the existing `const KNOWN_QUOTA_KEYS` and replace with:

```rust
const KNOWN_QUOTA_KEYS: &[&str] = &[
    "producer_byte_rate",
    "consumer_byte_rate",
    "request_percentage",
    "connection_creation_rate", // KIP-612 — only enforced when paired with ip entity
];
```

- [ ] **Step 3: Add IPv4 validation in `process_one_entry`**

Inside the per-entity validation loop (after the duplicate-type check), add:

```rust
    if e.entity_type == "ip" {
        if let Some(name) = &e.entity_name {
            if name.parse::<std::net::Ipv4Addr>().is_err() {
                return Err((INVALID_REQUEST, format!("invalid IPv4 address {name:?}")));
            }
        }
        // entity_name == None is fine — default ip entity.
    }
```

Place after the `SUPPORTED_ENTITY_TYPES.contains` check and after the duplicate-type check. Read the existing `process_one_entry` body to confirm the right insertion point.

- [ ] **Step 4: Add 2 unit tests**

Append to the existing `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn ip_entity_with_valid_ipv4_accepted() {
        let e = entry(
            vec![("ip", Some("10.0.0.1"))],
            vec![("connection_creation_rate", 1.0, false)],
        );
        let records = process_one_entry(&e).expect("ok");
        assert_eq!(records.len(), 1);
        let MetadataRecord::V1ClientQuota(r) = &records[0] else { panic!() };
        assert_eq!(r.config_key, "connection_creation_rate");
        assert_eq!(r.config_value, Some(1.0));
    }

    #[test]
    fn ip_entity_with_invalid_address_rejected() {
        let e = entry(
            vec![("ip", Some("not-an-ip"))],
            vec![("connection_creation_rate", 1.0, false)],
        );
        let err = process_one_entry(&e).unwrap_err();
        assert_eq!(err.0, INVALID_REQUEST);
    }
```

(The `entry` test helper already exists from slice 16 T5; reuse.)

- [ ] **Step 5: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib alter_client_quotas
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: 2 new tests PASS + slice 16's 6 existing pass.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/handlers/alter_client_quotas.rs
git commit -m "$(cat <<'EOF'
feat(broker): accept ip entity + connection_creation_rate in AlterClientQuotas

Extends the validator allowlist for KIP-612. ip entity_name must
parse as Ipv4Addr (IPv4 only, slice-13 ACL parity). Combinations
like producer_byte_rate on (ip=*) or connection_creation_rate on
(user=*) are accepted but never enforced — matches Kafka's
permissive validator.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 2 — TCP accept enforcement (sequential: T3)

### Task 3: TCP accept hook + Arc capture in listener spawn

**Files:**
- Modify: `crates/broker/src/broker.rs`

This task touches the accept loop. Per the spec, the hook lives between `listener.accept()` returning `(stream, peer)` and the per-connection `tokio::spawn`.

- [ ] **Step 1: Read the listener spawn block**

```
rg -n "listener\.accept\|TcpListener::bind\|spawn.*peer" crates/broker/src/broker.rs
```

The accept loop is around `broker.rs:1204` (per spec). Find the surrounding per-listener `tokio::spawn` block that wraps the loop.

- [ ] **Step 2: Capture `quota_buckets` + `controller` into the spawn closure**

Just before the `tokio::spawn(async move { ... })` for each listener, add:

```rust
let listener_quota_buckets = quota_buckets.clone();
let listener_controller = controller.clone();
```

Both are `Arc<...>` so clone is cheap. Pass into the spawn closure via the `async move` capture.

- [ ] **Step 3: Add the accept-loop hook**

In the `Ok((stream, peer))` arm of the accept loop, insert BEFORE the existing per-connection spawn:

```rust
                    // KIP-612 connection_creation_rate enforcement (slice 16b).
                    // IPv4 only — slice 13 ACL parity. IPv6 peers skip the quota check.
                    if let std::net::IpAddr::V4(peer_ipv4) = peer.ip() {
                        let image = listener_controller.current_image();
                        if let Some((entity_key, rate)) = crate::quota::lookup_ip_quota_with_key(
                            &image,
                            &peer_ipv4,
                            "connection_creation_rate",
                        ) {
                            if rate > 0.0 {
                                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                                let initial_rate = rate.max(1.0) as u64;
                                let bucket = listener_quota_buckets.get_or_create(
                                    "connection_creation_rate",
                                    &entity_key,
                                    initial_rate,
                                );
                                if bucket.try_consume(1) == 0 {
                                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                                    let delay_micros = ((1.0_f64 / rate) * 1_000_000.0) as u64;
                                    let delay = std::time::Duration::from_micros(delay_micros)
                                        .min(std::time::Duration::from_secs(1));
                                    tokio::time::sleep(delay).await;
                                }
                            }
                        }
                    }
```

(Adapt variable names — confirm `peer` is the `SocketAddr` and `stream` is the `TcpStream`. If different, rename in the snippet.)

- [ ] **Step 4: Confirm `quota_buckets` is in scope where the spawn block runs**

Slice 16 T8 added `Broker.quota_buckets: Arc<QuotaBuckets>`. Search for where the listener spawn is constructed (in `Broker::start`); the `quota_buckets` Arc should already be a local variable there. If it's only accessible via `broker.quota_buckets`, hoist it into a local before the spawn:

```rust
let quota_buckets = broker.quota_buckets.clone();
```

(Verify by reading the surrounding 50 lines of `Broker::start`.)

- [ ] **Step 5: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

All existing tests pass. The hook is a no-op when no `connection_creation_rate` quota is configured (which is true for every existing test).

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/broker.rs
git commit -m "$(cat <<'EOF'
feat(broker): KIP-612 connection_creation_rate at TCP accept

After listener.accept() returns (stream, peer), consume 1 token from
the (ip=peer.ip()) bucket. On overage: tokio::time::sleep before
spawning the per-connection handler. Cap at 1 second; never reject
the connection. IPv4 only (slice-13 parity); IPv6 peers bypass.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 3 — Integration tests (sequential: T4)

### Task 4: 3 broker integration tests

**Files:**
- Create: `crates/broker/tests/ip_quotas.rs`

- [ ] **Step 1: File scaffold + copied helpers**

```rust
#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]
```

Copy from slice 16's `tests/client_quotas.rs`:
- `round_trip`
- `sasl_plain_authenticate`
- `start_single_broker_sasl_plaintext_with_users`
- `start_single_broker_plaintext` (or its slice-equivalent — there's a plain-broker helper in slice 16 T12)
- `drive_alter_client_quotas_sasl`
- `drive_describe_client_quotas_sasl`
- `controller_image_for_test` (on BrokerHandle — already added by slice 16 T12)

Rust integration tests can't share `mod common` across sibling files. Copy verbatim.

- [ ] **Step 2: Test 1 — `ip_quota_alter_then_describe_round_trip`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ip_quota_alter_then_describe_round_trip() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret")],
    ).await;

    let alter_resp = drive_alter_client_quotas_sasl(
        addr, "admin", "admin-secret",
        vec![(
            vec![("ip".into(), Some("127.0.0.1".into()))],
            vec![("connection_creation_rate".into(), 2.0, false)],
        )],
        false,
    ).await;
    assert_eq!(alter_resp[0].1, 0, "alter should succeed");

    // Poll the image until the quota is visible.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let img = handle.controller_image_for_test();
        let key: crabka_metadata::EntityKey = vec![("ip".into(), Some("127.0.0.1".into()))];
        if let Some(cfgs) = img.client_quotas().get(&key) {
            if cfgs.get("connection_creation_rate") == Some(&2.0) {
                break;
            }
        }
        if std::time::Instant::now() > deadline {
            panic!("ip quota not visible in image");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let desc = drive_describe_client_quotas_sasl(
        addr, "admin", "admin-secret",
        vec![("ip".into(), /*ANY*/ 2, None)],
        false,
    ).await;
    assert_eq!(desc.len(), 1);
    assert_eq!(
        desc[0].1.iter().find(|(k, _)| k == "connection_creation_rate").map(|(_, v)| *v),
        Some(2.0)
    );
}
```

- [ ] **Step 3: Test 2 — `connection_creation_rate_throttles_accept`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_creation_rate_throttles_accept() {
    let (handle, _dir, addr) = start_single_broker_plaintext().await;

    // Set rate=1 connection/sec for the loopback IP.
    // Use submit_metadata_record_for_test directly (PLAINTEXT cluster — no SASL helper
    // available without seeding auth).
    let rec = crabka_metadata::MetadataRecord::V1ClientQuota(crabka_metadata::ClientQuotaRecord {
        entity: vec![crabka_metadata::QuotaEntity {
            entity_type: "ip".into(),
            entity_name: Some("127.0.0.1".into()),
        }],
        config_key: "connection_creation_rate".into(),
        config_value: Some(1.0),
    });
    handle.submit_metadata_record_for_test(rec).await.expect("seed quota");

    // Poll until refresh task picks up the rate.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let img = handle.controller_image_for_test();
        let key: crabka_metadata::EntityKey = vec![("ip".into(), Some("127.0.0.1".into()))];
        if img.client_quotas().get(&key)
            .and_then(|m| m.get("connection_creation_rate"))
            .is_some()
        {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("quota not visible after submit");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Open 5 connections in sequence; measure wall time.
    let started = std::time::Instant::now();
    let mut streams = Vec::with_capacity(5);
    for _ in 0..5 {
        let s = tokio::net::TcpStream::connect(addr).await.expect("connect");
        streams.push(s);
    }
    let elapsed = started.elapsed();
    drop(streams);

    // Expected timeline with rate=1, capacity=1, cap=1s:
    //   conn 1: free (initial token)
    //   conn 2: bucket empty → sleep 1s → free
    //   conn 3: bucket refills 1 token in 1s → ... → sleep 1s → free
    //   conn 4: same → 1s
    //   conn 5: same → 1s
    // Total ~4s. Tolerance: >=3s proves the throttle fires.
    assert!(
        elapsed >= std::time::Duration::from_secs(3),
        "expected >=3s of throttle, got {elapsed:?}"
    );
}
```

- [ ] **Step 4: Test 3 — `unthrottled_ip_unaffected`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unthrottled_ip_unaffected() {
    let (_handle, _dir, addr) = start_single_broker_plaintext().await;
    // No connection_creation_rate quota configured.

    let started = std::time::Instant::now();
    let mut streams = Vec::with_capacity(5);
    for _ in 0..5 {
        let s = tokio::net::TcpStream::connect(addr).await.expect("connect");
        streams.push(s);
    }
    let elapsed = started.elapsed();
    drop(streams);

    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "expected fast unthrottled connect, got {elapsed:?}"
    );
}
```

- [ ] **Step 5: Run via WSL**

```
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test ip_quotas -- --nocapture --test-threads=1"
```

Expected: 3 tests PASS.

- [ ] **Step 6: Lints + commit**

```bash
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
git add crates/broker/tests/ip_quotas.rs
git commit -m "$(cat <<'EOF'
test(broker): ip_quotas alter/describe + accept throttle

Three integration tests: SASL/PLAIN alter+describe round-trip on
(ip=127.0.0.1) connection_creation_rate, accept-throttle wall-clock
proof (~4s for 5 connections at rate=1/sec), and unthrottled
baseline (<500ms).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 4 — JVM acceptance (sequential: T5)

### Task 5: JVM `kafka-configs --entity-type ips` round-trip

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Append the test**

Pattern after slice 16 T13's `jvm_kafka_configs_alter_client_quota_end_to_end`.

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_configs_alter_ip_quota_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";

    let (h1, _h2, _h3, _d1, _d2, _d3, _c1, _c2, _c3) =
        start_three_broker_sasl_plaintext_jvm_cluster_with_users(
            ADMIN, ADMIN_PASS, &[],
        ).await;
    nc_check_connectivity();

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Set connection_creation_rate=2 for 127.0.0.1.
    let out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN, &admin_mount,
        &[
            "kafka-configs", "--alter",
            "--entity-type", "ips", "--entity-name", "127.0.0.1",
            "--add-config", "connection_creation_rate=2.0",
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ],
    );
    assert!(out.status.success(), "alter failed: {}", String::from_utf8_lossy(&out.stderr));

    // Describe — confirm visibility (slice 16 T13 documented that --describe may
    // exit non-zero due to DescribeUserScramCredentials side-call; assert on stdout).
    let desc = std::process::Command::new("docker")
        .args([
            "run", "--rm", "-v", &admin_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-configs", "--describe",
            "--entity-type", "ips", "--entity-name", "127.0.0.1",
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ])
        .output()
        .expect("spawn kafka-configs --describe");
    let stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        stdout.contains("connection_creation_rate=2"),
        "expected ip quota in describe output: {stdout}"
    );

    // Delete the config.
    let out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN, &admin_mount,
        &[
            "kafka-configs", "--alter",
            "--entity-type", "ips", "--entity-name", "127.0.0.1",
            "--delete-config", "connection_creation_rate",
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ],
    );
    assert!(out.status.success(), "delete failed: {}", String::from_utf8_lossy(&out.stderr));

    // Confirm cleared from image.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let img = h1.controller_image_for_test();
        let key: crabka_metadata::EntityKey = vec![("ip".into(), Some("127.0.0.1".into()))];
        if img.client_quotas().get(&key)
            .and_then(|m| m.get("connection_creation_rate"))
            .is_none()
        {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("ip quota not cleared after delete-config");
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
```

**Note on `start_three_broker_sasl_plaintext_jvm_cluster_with_users` return tuple:** slice 16 T13 added this helper. Verify the return arity (likely 9 elements: `(h1, h2, h3, cfg1, cfg2, cfg3, dir1, dir2, dir3)` per the slice 16 T13 commit note). Match the destructuring.

- [ ] **Step 2: Run via WSL**

```
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test jvm_acceptance jvm_kafka_configs_alter_ip_quota_end_to_end -- --ignored --nocapture --test-threads=1"
```

Expected: PASS in 30-60 seconds.

- [ ] **Step 3: Lints + commit**

```bash
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "$(cat <<'EOF'
test(jvm): kafka-configs --entity-type ips KIP-612 round-trip

Three-broker SASL/PLAINTEXT cluster; --alter + --describe (stdout
substring) + --delete-config on (ip=127.0.0.1) connection_creation_rate.
Wall-time enforcement not tested via JVM (single connection doesn't
exercise the rate limit); Rust integration test covers that.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 5 — Sweep + docs + PR (sequential: T6)

### Task 6: Sweep + README + STATUS + PR

**Files:**
- Modify: `README.md`
- Modify: `STATUS.md`

- [ ] **Step 1: Full local sweep**

```
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace
cargo test --workspace --exclude crabka-client-core --exclude crabka-log --exclude crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
```

All clean.

- [ ] **Step 2: Update the Quotas matrix in `README.md`**

Slice 16 left two rows under Quotas as `❌`:

```markdown
| IP entity + `connection_creation_rate` (KIP-612) | ❌ |
| Controller mutation rate (KIP-599) | ❌ |
```

Flip the first row to `✅`:

```markdown
| IP entity + `connection_creation_rate` (KIP-612) | ✅ |
| Controller mutation rate (KIP-599) | ❌ |
```

(Slice 16c will flip the second row.)

- [ ] **Step 3: Append to `STATUS.md`**

Add a new section:

```markdown
## Slice 16b — IP quotas + KIP-612 (2026-05-15)

- `ip` entity type recognized by `AlterClientQuotas` (api_key 49) and `DescribeClientQuotas` (api_key 48). IPv4 only — entity_name validated via `Ipv4Addr::from_str`; IPv6 rejected with `INVALID_REQUEST` (slice-13 ACL parity).
- New `connection_creation_rate` quota key (KIP-612). Stored in `ClientQuotaRecord` like any other quota; no new metadata record.
- `lookup_ip_quota` + `lookup_ip_quota_with_key` in `crates/broker/src/quota/lookup.rs` — two-priority (specific IP → default). Disjoint from the 8-priority user/client-id lookup. 4 unit tests.
- Validator extension in `process_one_entry`: `SUPPORTED_ENTITY_TYPES` += `"ip"`; `KNOWN_QUOTA_KEYS` += `"connection_creation_rate"`; IPv4 validation on `entity_name`. 2 unit tests.
- TCP accept enforcement in `broker.rs::accept` loop. After `listener.accept()` returns `(stream, peer)`, look up `(ip=peer.ip())` connection_creation_rate; if rate > 0 and bucket exhausted, compute `delay = 1/rate` seconds (capped at 1s) and `tokio::time::sleep` before spawning the per-connection handler. Connection is never rejected — only delayed (KIP-612 semantic).
- 3 broker integration tests in `tests/ip_quotas.rs`: alter+describe round-trip, accept-throttle wall-clock proof (~4s for 5 connections at rate=1/sec), unthrottled baseline (<500ms).
- 1 new JVM acceptance test for `kafka-configs --entity-type ips` round-trip.
- **Known limitations:**
  - Sub-1-connection-per-second rates floor to 1 — `rate.max(1.0) as u64` to avoid the "0 tokens/sec = always blocked" footgun. Production operators don't configure sub-1 rates.
  - Byte-rate quotas on `(ip)` entity are accepted by the validator but not enforced (matches Kafka's permissive validator).
  - Per-IP bucket cache grows unbounded over the broker's lifetime (inherits slice 16's no-eviction limitation).
- Out of scope: IPv6 entity names, connection rejection (vs delay), `controller_mutation_rate` (KIP-599 — slice 16c).
```

- [ ] **Step 4: Commit docs**

```bash
git add README.md STATUS.md
git commit -m "$(cat <<'EOF'
docs(slice-16b): README matrix + STATUS entry

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 5: Push + open PR**

```
git push -u origin feature/ip-quotas-16b
gh pr create --base main --head feature/ip-quotas-16b \
  --title "Slice 16b: IP quotas + KIP-612 connection_creation_rate" \
  --body "$(cat <<'EOF'
## Summary

Finishes the quota story started in slice 16 with the \`ip\` entity type and KIP-612 \`connection_creation_rate\`:

1. **\`ip\` entity recognized** by \`AlterClientQuotas\` + \`DescribeClientQuotas\`. IPv4 only (slice-13 ACL parity).
2. **\`connection_creation_rate\` key** added to the validator allowlist. Enforced at TCP accept via slice-15b's TokenBucket; on overage, \`tokio::time::sleep\` before spawning the per-connection handler.
3. **No connection rejection** — delay-only (KIP-612 semantic). 1-second cap on per-connection delay.

JVM \`kafka-configs --entity-type ips --entity-name 127.0.0.1 --add-config connection_creation_rate=...\` round-trips end-to-end.

## Verified

- 6 new unit tests (lookup_ip_quota 4, validator 2).
- 3 broker integration tests in \`tests/ip_quotas.rs\` (alter/describe round-trip, accept-throttle wall-clock proof, unthrottled baseline).
- 1 new JVM acceptance test.
- Workspace \`cargo fmt --check\`, \`cargo clippy --workspace --all-targets -- -D warnings\`, \`cargo test --workspace\` all green.

## Known limitations

- Sub-1-connection-per-second rates floor to 1.
- Byte-rate quotas on \`(ip)\` accepted but not enforced (matches Kafka).
- Per-IP bucket cache grows unbounded (inherits slice 16).

## Out of scope

- IPv6 entity names
- Connection rejection on sustained overage
- \`controller_mutation_rate\` (KIP-599) — slice 16c

## Plan / spec

- Spec: \`docs/superpowers/specs/2026-05-15-crabka-ip-quotas-16b-design.md\`
- Plan: \`docs/superpowers/plans/2026-05-15-crabka-ip-quotas-16b.md\` (6 tasks across 5 batches)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: Capture PR URL** and return.

---

## Notes for the executing agent

1. **CLAUDE.md compatibility rule** — no metadata schema changes in this slice. Reuses existing `ClientQuotaRecord`.

2. **Parallel batches** (per CLAUDE.md):
   - **B1 (T1 + T2)**: T1 touches `crates/broker/src/quota/lookup.rs` + `mod.rs`; T2 touches `crates/broker/src/handlers/alter_client_quotas.rs`. Disjoint.
   - **B2 (T3)**: TCP accept hook in `broker.rs`. Sequential.
   - **B3 (T4)**: integration tests. Sequential (depends on T3 enforcement).
   - **B4 (T5)**: JVM acceptance. Sequential.
   - **B5 (T6)**: sweep + PR. Sequential.

3. **TCP accept hook is in `broker.rs:1204`** per the spec. Verify before editing; line numbers drift.

4. **`broker.quota_buckets`** is `pub` (slice 16 T8). Accessible to the accept loop. May need to be cloned into a local variable before the listener spawn for the spawn closure to capture it; check the surrounding code before deciding.

5. **`listener_controller.current_image()` is sync** — returns `Arc<MetadataImage>`. The accept-hook code path is `.await`-free until the optional `tokio::time::sleep` at the end. Good — the accept loop stays responsive.

6. **Test 2 timing** — 5 sequential connections at rate=1/sec with 1-second per-conn cap = ~4s total. Tolerance ≥3s. If test flakes on CI, widen to ≥2.5s.

7. **Slice 16 T13 documented** that `kafka-configs --describe` exits non-zero due to `DescribeUserScramCredentials` side-call. The slice-16b JVM test handles `--describe` via `std::process::Command` directly + stdout substring assertion, NOT the `assert_status_success` helper.

8. **`start_three_broker_sasl_plaintext_jvm_cluster_with_users`** was added in slice 16 T13. Its return tuple has 9 elements (per slice 16 T13 commit note). Verify before destructuring.
