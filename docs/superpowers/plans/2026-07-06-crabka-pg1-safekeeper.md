# PG-1: The safekeeper — physical WAL ingest — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A new standalone crate `crabka-safekeeper` that streams physical WAL from a stock Postgres 17 primary (`START_REPLICATION … PHYSICAL`, physical slot, standby feedback) into an internal Crabka topic `__pg_wal.<cluster>` as contiguity-guarded `PGW1`-framed records produced with `acks=all` — gated by consuming the stored stream back through `crabka-postgres-wal`'s decoder (CRC-valid, LSN-contiguous across every chunk and restart boundary).

**Architecture:** Zero broker changes — the safekeeper is an ordinary Kafka-wire client (`client-producer`/`client-consumer`/`client-admin`). Replication connection via `tokio-postgres` if the workspace version supports the `replication=true` startup parameter (verify-first), else a minimal in-crate CopyBoth session over `postgres-protocol` primitives. `flushed_lsn` = highest **acked** end-LSN (tier-qualified). Restart resumes from the topic tail.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `tokio`, `tokio-postgres 0.7`/`postgres-protocol`, `crabka-client-{producer,consumer,admin}`, `bytes`, `thiserror`, `testcontainers` + `testcontainers-modules` (`postgres` feature) for integration, `assert2`/`nextest`, `cargo +nightly fmt`, `clippy::pedantic`.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-pg1-safekeeper-design.md`](../specs/2026-07-06-crabka-pg1-safekeeper-design.md).

**PREREQUISITES (unlanded):** **PG-2** (`crabka-postgres-wal`) as a **dev-dependency** for the decode gate only. The runtime path needs nothing unbuilt (durability tier inherited from the topic; upgrades with diskless slices 1/6a, no code change here).

---

## Invariants

1. **Contiguity:** every produced record's `start_lsn` equals the previous record's `end_lsn` — checked before produce, re-verified across restart; violation halts.
2. **Feedback truth:** `flushed_lsn` advances only on producer acks, and its meaning is tier-qualified (documented: dev-grade until diskless slice 1).
3. **Chunk alignment:** records split only at `XLogData` message boundaries (never mid-message; WAL-record boundaries are irrelevant — PG-2 reassembles).
4. **Stock primary only:** plain replication protocol; no compute patching; single timeline (halt on switch).
5. **The decode gate:** the stored stream must decode cleanly through `WalStreamDecoder` before the slice ships.
6. **New-crate hygiene:** `publish = false` + private release-plz entry; every task ends green before its commit.

## Scope boundary

- **In scope:** frame codec + chunker; replication-protocol messages; the connection (verify-first + fallback); the ingest loop with ensure-topic, acks tracking, feedback; tail-read resume; the containerized integration + decode gate.
- **Deferred:** LSN→offset random-access index (the live pageserver-ingest slice); WAL trim; timeline switches/HA; SCRAM if fixtures can use password/trust; multi-cluster management.

---

## File Structure

- **`crates/safekeeper/`** (new crate `crabka-safekeeper`):
  - `Cargo.toml` (`publish = false`), `src/lib.rs`
  - `src/frame.rs` — the `PGW1` record frame + chunker
  - `src/protocol.rs` — `XLogData`/keepalive parse, standby-status-update encode
  - `src/conn.rs` — the replication session (tokio-postgres or the minimal fallback)
  - `src/ingest.rs` — the loop: conn → chunker → producer → feedback; resume
  - `tests/integration.rs` — containerized PG 17 end-to-end + the decode gate
- **`release-plz.toml`** — private entry.

**Batching:** Task 1 (`frame.rs`) ∥ Task 2 (`protocol.rs`) — disjoint, pure. Task 3 (`conn.rs`) after 2. Task 4 (`ingest.rs`) after 1+3. Task 5 (resume) extends `ingest.rs` after 4. Task 6 (gate) last before the final gate.

---

## Task 1 (∥ Task 2): Frame codec + chunker

**Files:**
- Create: `crates/safekeeper/{Cargo.toml, src/lib.rs, src/frame.rs}`; Modify: `release-plz.toml`

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn frame_round_trips() {
        let f = WalFrame { start_lsn: Lsn(0x16_0000_0010), bytes: Bytes::from_static(b"wal") };
        let enc = f.encode();
        assert!(&enc[..4] == b"PGW1");
        let d = WalFrame::decode(&enc).unwrap();
        assert!(d.start_lsn == f.start_lsn && d.bytes == f.bytes && d.end_lsn() == Lsn(0x16_0000_0013));
    }

    #[test]
    fn chunker_aligns_to_xlogdata_and_respects_target() {
        let mut c = Chunker::new(Lsn(100), 512 * 1024);
        c.push_xlogdata(Lsn(100), vec![0u8; 300 * 1024]).unwrap();
        c.push_xlogdata(Lsn(100 + 300 * 1024), vec![0u8; 300 * 1024]).unwrap(); // over target -> first flushes alone
        let out = c.drain_ready();
        assert!(out.len() == 1 && out[0].bytes.len() == 300 * 1024); // never split mid-XLogData
    }

    #[test]
    fn contiguity_guard_rejects_gap_and_overlap() {
        let mut c = Chunker::new(Lsn(100), 1 << 20);
        c.push_xlogdata(Lsn(100), vec![0u8; 10]).unwrap();
        let_assert!(Err(SafekeeperError::LsnGap { expected, got }) = c.push_xlogdata(Lsn(200), vec![0u8; 1]));
        assert!(expected == Lsn(110) && got == Lsn(200));
        let_assert!(Err(SafekeeperError::LsnGap { .. }) = c.push_xlogdata(Lsn(105), vec![0u8; 1])); // overlap
    }
```

- [ ] **Step 2: Implement**

`WalFrame { start_lsn, bytes }` (`encode`: `b"PGW1" | start_lsn u64 LE | bytes`; `end_lsn = start + len`); `Chunker` accumulates XLogData payloads (contiguity-checked), emitting frames at the size target on message boundaries. `Lsn` re-used from `crabka-postgres-wal`? — **no**: that's a dev-dependency only; define a local `Lsn(u64)` newtype (tiny, avoids a runtime dep on the decoder crate) with a `From` conversion in tests. `Cargo.toml` `publish = false`; release-plz private entry.

- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-safekeeper --lib frame` → PASS; `./tools/check-publish-allowlist.sh` → 0.

```bash
git add crates/safekeeper release-plz.toml
git commit -m "feat(safekeeper): PGW1 frame codec + contiguity-guarded chunker"
```

---

## Task 2 (∥ Task 1): Replication-protocol messages

**Files:**
- Create: `crates/safekeeper/src/protocol.rs`

- [ ] **Step 1: Write the failing tests** — against the documented byte layouts (all big-endian, per the PG streaming-replication protocol):

```rust
    #[test]
    fn parses_xlogdata() {
        // 'w' | wal_start u64 | wal_end u64 | send_time i64 | bytes...
        let mut m = vec![b'w']; m.extend(100u64.to_be_bytes()); m.extend(200u64.to_be_bytes());
        m.extend(0i64.to_be_bytes()); m.extend(b"walwal");
        let_assert!(CopyBothMsg::XLogData { wal_start, data } = parse_copy_msg(&m).unwrap());
        assert!(wal_start == Lsn(100) && data.as_ref() == b"walwal");
    }

    #[test]
    fn parses_keepalive_reply_flag() {
        // 'k' | wal_end u64 | send_time i64 | reply u8
        let mut m = vec![b'k']; m.extend(300u64.to_be_bytes()); m.extend(0i64.to_be_bytes()); m.push(1);
        let_assert!(CopyBothMsg::Keepalive { wal_end, reply_requested: true } = parse_copy_msg(&m).unwrap());
        assert!(wal_end == Lsn(300));
    }

    #[test]
    fn encodes_standby_status_update() {
        // 'r' | written u64 | flushed u64 | applied u64 | send_time i64 | reply u8
        let b = StandbyStatus { written: Lsn(10), flushed: Lsn(8), applied: Lsn(8), reply: false }.encode(0);
        assert!(b[0] == b'r' && b.len() == 1 + 8 * 3 + 8 + 1);
        assert!(u64::from_be_bytes(b[9..17].try_into().unwrap()) == 8); // flushed field
    }
```

- [ ] **Step 2: Implement** (parse/encode exactly; unknown tag → `SafekeeperError::UnexpectedCopyMessage(tag)`).

- [ ] **Step 3: Verify + commit**

```bash
git add crates/safekeeper/src/protocol.rs crates/safekeeper/src/lib.rs
git commit -m "feat(safekeeper): CopyBoth message codecs (XLogData, keepalive, status update)"
```

---

## Task 3: The replication session (verify-first)

**Files:**
- Create: `crates/safekeeper/src/conn.rs`

- [ ] **Step 1: VERIFY** whether workspace `tokio-postgres 0.7` exposes the `replication=true` startup parameter (look for a `replication` option on `Config` / a `replication_mode` API in the pinned version's docs). Record the finding in a module comment.
- [ ] **Step 2: Implement accordingly.**
  - **If supported:** `Config` with `replication=true` + `copy_both_simple("START_REPLICATION SLOT crabka_sk_<cluster> PHYSICAL X/Y TIMELINE n")`; `IDENTIFY_SYSTEM` and `CREATE_REPLICATION_SLOT … PHYSICAL` (idempotent: tolerate "already exists") as simple_query calls on the same connection.
  - **Fallback:** a minimal session in-crate: TCP + `postgres-protocol`'s startup/auth (password/trust for fixtures), then the simple-query + CopyBoth framing (`CopyBothResponse`, `CopyData` wrapping Task 2's submessages, `CopyDone`). Scope strictly to what the safekeeper needs.
  - Either way expose: `ReplicationSession::connect(url, cluster) -> Self`, `identify() -> (sysid, timeline, flush_lsn)`, `ensure_slot()`, `start(resume: Lsn) -> impl Stream<Item = CopyBothMsg>`, `send_status(StandbyStatus)`. A timeline value different from `identify()`'s → `SafekeeperError::TimelineSwitch` (halt).
- [ ] **Step 3: Smoke test** (first containerized test, `testcontainers-modules` `postgres` at tag `17`, `wal_level=replica`, `max_wal_senders>0`): connect, identify, ensure slot twice (idempotent), start streaming, receive at least one `XLogData` after an insert. Commit.

```bash
git add crates/safekeeper/src/conn.rs crates/safekeeper/Cargo.toml crates/safekeeper/tests
git commit -m "feat(safekeeper): physical replication session (slot, START_REPLICATION, CopyBoth)"
```

---

## Task 4: The ingest loop

**Files:**
- Create: `crates/safekeeper/src/ingest.rs`

- [ ] **Step 1: Write the failing integration test** (containerized PG + in-process `Broker::start` — the `producer_integration.rs` boot pattern): run the safekeeper against both; insert rows on PG; assert `__pg_wal.<cluster>` exists (ensure-topic ran), contains ≥1 `PGW1` frame, frames are LSN-contiguous, and a keepalive with `reply_requested` gets a status update whose `flushed` equals the last **acked** frame's end-LSN (not the last *sent*).

- [ ] **Step 2: Implement**

```rust
pub struct Safekeeper { session: ReplicationSession, producer: Producer, chunker: Chunker,
                        flushed: Lsn, written: Lsn, topic: String }
// loop: select! over session stream + feedback ticker:
//   XLogData { wal_start, data } -> chunker.push_xlogdata; for frame in chunker.drain_ready():
//       producer.send(topic, frame.encode(), acks=All).await -> on ack: flushed = frame.end_lsn()
//   Keepalive { reply_requested: true, .. } | tick -> session.send_status(StandbyStatus {
//       written, flushed, applied: flushed, reply: false })
// ensure_topic(admin, "__pg_wal.<cluster>", partitions=1) before starting (CreateTopicSpec pattern).
```

Written = last enqueued end-LSN; flushed advances only in the ack completion. Produce errors: retry with backoff; a permanently failed produce halts (never skip — contiguity).

- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-safekeeper --test integration ingest` → PASS.

```bash
git add crates/safekeeper/src/ingest.rs crates/safekeeper/tests
git commit -m "feat(safekeeper): ingest loop with acks-gated feedback"
```

---

## Task 5: Restart / resume

**Files:**
- Modify: `crates/safekeeper/src/ingest.rs`; `tests/integration.rs`

- [ ] **Step 1: Write the failing test** — run ingest; stop the safekeeper mid-stream (drop it); write more rows; start a new safekeeper instance; assert the topic's frame sequence is **gap-free and overlap-free across the restart seam** (scan all frames, verify `start == prev.end` throughout).
- [ ] **Step 2: Implement** `resume_lsn(consumer) -> Option<Lsn>`: read the topic tail (last record), decode the `PGW1` frame, return `end_lsn`; `START_REPLICATION` from it (the slot guarantees the primary still has that WAL). First frame after resume is contiguity-checked against the tail like any other.
- [ ] **Step 3: Verify + commit**

```bash
git add crates/safekeeper/src crates/safekeeper/tests
git commit -m "feat(safekeeper): tail-read resume with cross-restart contiguity"
```

---

## Task 6: The decode gate (PG-2 as oracle)

**Files:**
- Modify: `crates/safekeeper/tests/integration.rs` (+ dev-dep `crabka-postgres-wal`)

- [ ] **Step 1: Write the gate test** — after the Task 4/5 runs (including the restart seam): consume **all** of `__pg_wal.<cluster>`, decode every `PGW1` frame, feed the byte runs in order into `crabka_postgres_wal::WalStreamDecoder` (`feed(start_lsn, bytes)`), and poll to exhaustion: every record CRC-valid, LSNs monotone, zero framing errors — across every chunk boundary and the restart seam. Cross-check the record count is > 0 and the last decoded LSN ≥ the last produced frame's start.
- [ ] **Step 2: Run to verify it passes** — a failure here is a safekeeper framing/contiguity bug or a PG-2 decoder bug; the two crates arbitrate each other. Commit.

```bash
git add crates/safekeeper/tests crates/safekeeper/Cargo.toml
git commit -m "test(safekeeper): stored-stream decode gate via crabka-postgres-wal"
```

---

## Task 7: Final gate

- [ ] **Step 1:** `cargo +nightly fmt --check` — no diff.
- [ ] **Step 2:** `cargo clippy -p crabka-safekeeper --all-targets -- -D warnings` — no warnings.
- [ ] **Step 3:** `cargo nextest run -p crabka-safekeeper` — PASS (units always; container tests under the integration profile/job like the other testcontainers crates).
- [ ] **Step 4:** `./tools/check-publish-allowlist.sh` — exit 0. Commit any formatting.

---

## Self-Review

**1. Spec coverage:** frame + XLogData-aligned chunker + contiguity guard (Task 1); protocol codecs (Task 2); the verify-first session with the named fallback + slot idempotence + timeline halt (Task 3); the ingest loop with ensure-topic and acks-gated, tier-qualified feedback (Task 4); tail-read resume with cross-restart contiguity (Task 5); the PG-2 decode gate (Task 6); hygiene (Tasks 1, 7). Deferred set (LSN index, trim, HA/timelines, SCRAM) untouched — Scope boundary. ✅

**2. Placeholder scan:** byte layouts are spelled in tests; the one genuine unknown (tokio-postgres replication support) is an explicit VERIFY step with a scoped fallback, not a hand-wave; the ingest loop's control flow is given concretely. No `TBD`.

**3. Type consistency:** `WalFrame`/`Chunker`/`Lsn` (Task 1) flow through `ingest` (Task 4) and the resume path (Task 5); `CopyBothMsg`/`StandbyStatus` (Task 2) are `conn.rs`'s stream items (Task 3) consumed in Task 4; `SafekeeperError::{LsnGap, TimelineSwitch, UnexpectedCopyMessage}` named consistently; the gate uses PG-2's real `feed`/`poll_record` seam.

**4. Invariant check:** contiguity (Tasks 1, 4, 5 + the gate); acks-gated feedback (Task 4 test asserts flushed ≠ sent); alignment (Task 1); stock-primary/single-timeline (Task 3); the decode gate (Task 6); allowlist (Tasks 1, 7). Each task green before commit.

**5. Prerequisites flagged:** PG-2 as dev-dep only (header); zero unbuilt runtime dependencies — the durability tier inherits from the topic and upgrades with the diskless slices, no code change here. Batching: (1 ∥ 2) → 3 → 4 → 5 → 6 → 7.
