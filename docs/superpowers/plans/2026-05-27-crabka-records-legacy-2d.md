# Records-legacy 2d (JVM acceptance for legacy clients) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add four end-to-end tests that drive a real Apache Kafka 0.10.0 console-producer/consumer (inside `cp-kafka:3.1.2`) against the Rust broker, validating the v0/v1 down-conversion plan landed in #214 and #226.

**Architecture:** All tests live in `crates/broker/tests/jvm_acceptance.rs` and follow the existing `docker_run_kafka_tool_with_image(image, args)` + `start_host_broker()` pattern. The broker runs on the host at `0.0.0.0:9092`, advertises `host.docker.internal:9092`, and is reachable from containers via `--add-host=host.docker.internal:host-gateway`. Topic creation uses the modern `cp-kafka:6.1.1` AdminClient (Kafka 0.10.0 predated AdminClient; using the new tools for setup avoids the legacy `--zookeeper` flag).

**Tech Stack:** Rust 1.95, `tokio::test(flavor = "multi_thread")`, Docker (Linux/macOS — Windows is `#![cfg(not(target_os = "windows"))]`-gated in this file), `confluentinc/cp-kafka:3.1.2` (Kafka 0.10.0), reuses `cp-kafka:6.1.1` (Kafka 2.6).

**Spec:** `docs/superpowers/specs/2026-05-27-crabka-records-legacy-2d-design.md`

**Branch:** Create a new branch off `main` named `legacy-records-2d`.

---

## Pre-flight: branch + verify image

- [ ] **Step 1: Create the branch on main**

```bash
git checkout main && git pull --ff-only
git checkout -b legacy-records-2d
```

- [ ] **Step 2: Verify the legacy image is pullable**

Run once locally so the implementer fails fast if Confluent has garbage-collected the tag:

```bash
docker pull confluentinc/cp-kafka:3.1.2 2>&1 | tail -3
```

Expected: a digest line, no `manifest unknown` error. If the pull fails, fall back to `wurstmeister/kafka:0.10.0.1` (Risk Register item in the spec). Adjust `KAFKA_IMAGE_LEGACY` in Task 1 accordingly and note the substitution in the test docstring.

- [ ] **Step 3: Confirm clean tree, on the new branch**

```bash
git status
git rev-parse --abbrev-ref HEAD
```

Expected: `nothing to commit, working tree clean`, branch `legacy-records-2d`.

---

## Task 1: Add the `KAFKA_IMAGE_LEGACY` constant

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs` (add one constant near `KAFKA_IMAGE_TXN` at line ~54)

- [ ] **Step 1: Add the constant**

Read `crates/broker/tests/jvm_acceptance.rs` around line 43–55. Locate the existing `KAFKA_IMAGE_TXN` constant; add immediately after:

```rust
/// Kafka 0.10.0 console tools, used by the slice-2d legacy-client
/// acceptance tests (`jvm_legacy_010_*`). The 0.10.0-era producer
/// emits v1 `MessageSet` (KIP-32 timestamps) by default; the consumer
/// negotiates Fetch v0–3. This exercises the broker's
/// `kafka_3_6_2`-namespace handlers and the up/down-conversion paths
/// landed in slices 2b+2c (#226).
const KAFKA_IMAGE_LEGACY: &str = "confluentinc/cp-kafka:3.1.2";
```

- [ ] **Step 2: Verify the file still builds**

```bash
cargo build -p crabka-broker --tests 2>&1 | tail -3
```

Expected: clean build (the constant is unused so far; Rust allows unused module-level `const` items without a `#[allow]`).

- [ ] **Step 3: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/broker/tests/jvm_acceptance.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "test(broker): KAFKA_IMAGE_LEGACY constant for slice 2d acceptance tests"
```

---

## Task 2: `jvm_legacy_010_round_trip` — pure legacy

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs` (append a new `#[tokio::test]` at the end of the file, before any closing items)

- [ ] **Step 1: Write the test**

Append at the end of `crates/broker/tests/jvm_acceptance.rs`:

```rust
/// Slice 2d test 1: pure-legacy round-trip.
///
/// A Kafka 0.10.0 console-producer sends 3 records via Produce v0–2
/// (v1 `MessageSet` records). A Kafka 0.10.0 console-consumer reads
/// them back via Fetch v0–3. Exercises both up-conversion (Produce
/// handler) and down-conversion (Fetch handler) end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_legacy_010_round_trip() {
    const TOPIC: &str = "legacy-010-round-trip";

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    // 1. Create the topic via the modern AdminClient. The 0.10.0-era
    //    kafka-topics tool used --zookeeper, not --bootstrap-server,
    //    so we can't drive it from a 3.1.2 image without standing up
    //    Zookeeper. Use 6.1.1's AdminClient for setup.
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);

    // 2. Produce 3 records via the 0.10.0 console-producer.
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_LEGACY,
            "kafka-console-producer",
            "--broker-list",
            BOOTSTRAP,
            "--topic",
            TOPIC,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn legacy producer");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"alpha\nbravo\ncharlie\n")
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait legacy producer");
    assert!(
        producer_out.status.success(),
        "legacy producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // 3. Consume them back via the 0.10.0 console-consumer.
    //    0.10.0 added `--new-consumer` + `--bootstrap-server`; the
    //    old `--zookeeper` mode is unusable without ZK. Use the new
    //    consumer with --partition 0 to bypass group coordination.
    let consumer_out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_LEGACY,
        &[
            "kafka-console-consumer",
            "--new-consumer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            "3",
            "--timeout-ms",
            "10000",
        ],
    );
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for needle in ["alpha", "bravo", "charlie"] {
        assert!(s.contains(needle),
            "legacy consumer didn't emit {needle}: stdout={s:?}");
    }

    broker.shutdown().await;
}
```

- [ ] **Step 2: Run the test**

```bash
cargo test -p crabka-broker --test jvm_acceptance jvm_legacy_010_round_trip -- --ignored --nocapture 2>&1 | tail -30
```

Expected: pass. If the producer flag is rejected (`unrecognized option: --broker-list`), try `--bootstrap-server` instead; 0.10.0's producer accepted `--broker-list` and the alternative was added in 0.10.1. Update the test if needed.

If the consumer fails with `unrecognized option: --new-consumer`, that flag may have been default in 0.10.0; drop it and retain `--bootstrap-server`. Re-run.

If neither the producer nor consumer can connect to `host.docker.internal:9092`, the broker logs (printed via `tracing` to test stderr) should show an `accepted` log — confirm visually before debugging further.

- [ ] **Step 3: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/broker/tests/jvm_acceptance.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "test(broker): jvm_legacy_010_round_trip — pure-legacy v0/v1 round-trip"
```

---

## Task 3: `jvm_legacy_010_produce_modern_consume` — up-conversion correctness

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs` (append after Task 2's test)

- [ ] **Step 1: Write the test**

Append at the end of `crates/broker/tests/jvm_acceptance.rs`:

```rust
/// Slice 2d test 2: legacy producer, modern consumer.
///
/// A Kafka 0.10.0 console-producer sends 3 records; a Kafka 2.6
/// console-consumer reads them back via Fetch v11+. Validates that
/// what the up-conversion writes to the log is a well-formed v2
/// `RecordBatch` that a modern client can decode — not just bytes a
/// Crabka broker accepts on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_legacy_010_produce_modern_consume() {
    const TOPIC: &str = "legacy-010-produce-modern-consume";

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);

    // Produce via legacy.
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_LEGACY,
            "kafka-console-producer",
            "--broker-list",
            BOOTSTRAP,
            "--topic",
            TOPIC,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn legacy producer");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"alpha\nbravo\ncharlie\n")
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait legacy producer");
    assert!(
        producer_out.status.success(),
        "legacy producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // Consume via modern (cp-kafka:6.1.1, uses Fetch v11+).
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        BOOTSTRAP,
        "--topic",
        TOPIC,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        "3",
        "--timeout-ms",
        "10000",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for needle in ["alpha", "bravo", "charlie"] {
        assert!(s.contains(needle),
            "modern consumer didn't emit {needle}: stdout={s:?}");
    }

    broker.shutdown().await;
}
```

- [ ] **Step 2: Run the test**

```bash
cargo test -p crabka-broker --test jvm_acceptance jvm_legacy_010_produce_modern_consume -- --ignored --nocapture 2>&1 | tail -30
```

Expected: pass. If it fails on the modern consumer side, the up-conversion is producing v2 batches the modern reader can't parse — inspect the broker's tracing logs for `legacy_to_v2` warnings, and check `crates/protocol/src/records/owned.rs::RecordBatch::encode` against what the modern client expects (CRC, attributes byte, etc.).

- [ ] **Step 3: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/broker/tests/jvm_acceptance.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "test(broker): jvm_legacy_010_produce_modern_consume — up-conversion correctness"
```

---

## Task 4: `jvm_modern_produce_legacy_010_consume` — down-conversion correctness

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs` (append after Task 3's test)

- [ ] **Step 1: Write the test**

Append at the end of `crates/broker/tests/jvm_acceptance.rs`:

```rust
/// Slice 2d test 3: modern producer, legacy consumer.
///
/// A Kafka 2.6 console-producer sends 3 records via Produce v9. A
/// Kafka 0.10.0 console-consumer reads them via Fetch v0–3. Validates
/// that the bytes `down_convert_for_fetch` emits are parseable as a
/// v0/v1 `MessageSet` by a real Kafka 0.10.0 client — the load-bearing
/// concern for down-conversion correctness.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_modern_produce_legacy_010_consume() {
    const TOPIC: &str = "modern-produce-legacy-010-consume";

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);

    // Produce via modern (cp-kafka:6.1.1, Produce v9).
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn modern producer");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"alpha\nbravo\ncharlie\n")
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait modern producer");
    assert!(
        producer_out.status.success(),
        "modern producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // Consume via legacy (cp-kafka:3.1.2, Fetch v0-3).
    let consumer_out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_LEGACY,
        &[
            "kafka-console-consumer",
            "--new-consumer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            "3",
            "--timeout-ms",
            "10000",
        ],
    );
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for needle in ["alpha", "bravo", "charlie"] {
        assert!(s.contains(needle),
            "legacy consumer didn't emit {needle}: stdout={s:?}");
    }

    broker.shutdown().await;
}
```

- [ ] **Step 2: Run the test**

```bash
cargo test -p crabka-broker --test jvm_acceptance jvm_modern_produce_legacy_010_consume -- --ignored --nocapture 2>&1 | tail -30
```

Expected: pass. This is the test most likely to find a real wire-format bug. If the legacy consumer reads zero records or errors out on parsing, inspect:
1. `crates/broker/src/handlers/fetch_downconvert.rs::down_convert_for_fetch` — does it emit a valid v1 MessageSet given a v2 batch?
2. `crates/records-legacy/src/bridge.rs::v2_to_legacy` — same question one layer down.
3. Broker tracing logs for any `v2_to_legacy failed` warnings.

- [ ] **Step 3: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/broker/tests/jvm_acceptance.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "test(broker): jvm_modern_produce_legacy_010_consume — down-conversion correctness"
```

---

## Task 5: `jvm_legacy_010_compressed_round_trip` — compression path

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs` (append after Task 4's test)

- [ ] **Step 1: Write the test**

Append at the end of `crates/broker/tests/jvm_acceptance.rs`:

```rust
/// Slice 2d test 4: gzip-compressed legacy round-trip.
///
/// A Kafka 0.10.0 console-producer with `compression.type=gzip`
/// sends ~50 records as a single outer-wrapped gzip `MessageSet`
/// (the v0/v1 way of representing compressed batches). A Kafka 2.6
/// console-consumer reads them back. Validates the gzip path
/// through `legacy_to_v2` (decompress legacy → re-emit as a v2
/// `RecordBatch` with the same compression marker).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_legacy_010_compressed_round_trip() {
    const TOPIC: &str = "legacy-010-compressed-round-trip";

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);

    // 50 newline-separated records to give gzip something to compress.
    let mut input = String::new();
    for i in 0..50 {
        input.push_str(&format!("record-{i:03}\n"));
    }

    // Produce via legacy with gzip.
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_LEGACY,
            "kafka-console-producer",
            "--broker-list",
            BOOTSTRAP,
            "--topic",
            TOPIC,
            "--producer-property",
            "compression.type=gzip",
            "--producer-property",
            "batch.size=131072",  // 128 KiB — enough to batch all 50 records together
            "--producer-property",
            "linger.ms=100",      // give the producer time to batch
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn legacy producer");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(input.as_bytes())
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait legacy producer");
    assert!(
        producer_out.status.success(),
        "legacy gzip producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // Consume all 50 via modern.
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        BOOTSTRAP,
        "--topic",
        TOPIC,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        "50",
        "--timeout-ms",
        "15000",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for i in 0..50 {
        let needle = format!("record-{i:03}");
        assert!(s.contains(&needle),
            "modern consumer didn't emit {needle} after legacy gzip produce");
    }

    broker.shutdown().await;
}
```

- [ ] **Step 2: Run the test**

```bash
cargo test -p crabka-broker --test jvm_acceptance jvm_legacy_010_compressed_round_trip -- --ignored --nocapture 2>&1 | tail -30
```

Expected: pass. If decompression fails on the broker side, inspect `crates/records-legacy/src/bridge.rs::legacy_to_v2` — specifically how it handles a wrapper-message whose attributes indicate gzip and whose value is the inner uncompressed `MessageSet`.

- [ ] **Step 3: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/broker/tests/jvm_acceptance.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "test(broker): jvm_legacy_010_compressed_round_trip — gzip up-conversion path"
```

---

## Execution batches (for parallel subagent dispatch)

All five tasks touch the same file (`crates/broker/tests/jvm_acceptance.rs`), so they **cannot** run in parallel — they must be sequential to avoid edit conflicts:

- **Batch A**: Task 1
- **Batch B**: Task 2
- **Batch C**: Task 3
- **Batch D**: Task 4
- **Batch E**: Task 5

Each task is small (one test ~80 lines), so sequential execution is still fast.

---

## Final verification

- [ ] **Step 1: Run all four new tests together**

```bash
cargo test -p crabka-broker --test jvm_acceptance \
    jvm_legacy_010 jvm_modern_produce_legacy_010 \
    -- --ignored --nocapture --test-threads=1 2>&1 | tail -30
```

Expected: all four pass. `--test-threads=1` matches what CI does for the broker-jvm-acceptance job (port 9092 is exclusive).

- [ ] **Step 2: Run the full jvm_acceptance suite to catch regressions**

```bash
cargo test -p crabka-broker --test jvm_acceptance -- --ignored --nocapture --test-threads=1 2>&1 | tail -10
```

Expected: no new failures versus the pre-2d baseline.

- [ ] **Step 3: Clippy + fmt**

```bash
cargo clippy -p crabka-broker --tests -- -D warnings 2>&1 | tail -5
cargo fmt --check 2>&1 | tail -5
```

Expected: clean.

- [ ] **Step 4: Open PR**

```bash
git push -u origin legacy-records-2d
gh pr create --title "Slice 2d: JVM acceptance for legacy v0/v1 Kafka clients" --body "$(cat <<'EOF'
## Summary

Slice **2d** of the v0/v1 down-conversion roadmap. Adds four JVM
acceptance tests in `crates/broker/tests/jvm_acceptance.rs` that
drive a real Apache Kafka 0.10.0 console-producer/consumer (inside
`cp-kafka:3.1.2`) against the Rust broker. Closes out the plan
started in #214 and continued in #226.

- `jvm_legacy_010_round_trip` — pure-legacy round-trip.
- `jvm_legacy_010_produce_modern_consume` — up-conversion correctness.
- `jvm_modern_produce_legacy_010_consume` — down-conversion correctness.
- `jvm_legacy_010_compressed_round_trip` — gzip up-conversion path.

Spec: `docs/superpowers/specs/2026-05-27-crabka-records-legacy-2d-design.md`
Plan: `docs/superpowers/plans/2026-05-27-crabka-records-legacy-2d.md`

## Test plan

- [x] `cargo test -p crabka-broker --test jvm_acceptance jvm_legacy_010 jvm_modern_produce_legacy_010 -- --ignored --nocapture --test-threads=1` (local)
- [x] full `jvm_acceptance` suite (local, --test-threads=1)
- [x] `cargo clippy -p crabka-broker --tests -- -D warnings`
- [x] `cargo fmt --check`
- [ ] CI `broker-jvm-acceptance` job (pulls cp-kafka:3.1.2 on first run, ~400 MB)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL printed; the broker-jvm-acceptance job will pull the new image on first run.
