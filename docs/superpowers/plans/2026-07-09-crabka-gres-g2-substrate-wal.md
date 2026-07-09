# Chapter Gres G-2: Substrate WAL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A gres tenant's durable truth moves from local fjall to a per-tenant Crabka topic (`__gres_wal.<tenant>`), making the compute disposable: kill it at any instant and a successor replays the topic and serves, with zombie computes fenced broker-side.

**Architecture:** New internal crate `crabka-gres-substrate` implements the engine's existing `Committer`/`Linearizer` seams (`SqlEngine::replicated`): a single WAL-writer task owns a transactional producer and group-commits framed `GRW1` records (one per `Committer` batch) inside one Kafka transaction per group; recovery fences predecessors via `init_transactions()`, replays the topic at READ_COMMITTED with max-merge/write-once apply rules, reseeds counters, then serves. One targeted engine fix routes FDW-object DDL through the seam (a verified donor bypass).

**Tech Stack:** `crabka-client-producer` (transactional), `crabka-client-core` (`fetch_partition_with_isolation`, raw `ListOffsets`), `crabka-client-admin` (`create_topics`), tokio mpsc/oneshot, the vendored `crabka-pgexec`/`crabka-pgkv`/`crabka-pgmvcc` crates from G-1.

## Global Constraints

- **Prerequisite:** the G-1 vendoring plan ([2026-07-09-crabka-gres-g1-vendor.md](2026-07-09-crabka-gres-g1-vendor.md)) has landed — this plan edits crates G-1 creates. Verify signatures quoted here against the tree at execution time; they were verified against donor `crabgresql@93f3d17`, which G-1 vendors unchanged.
- **Spec:** [docs/superpowers/specs/2026-07-09-crabka-gres-g2-substrate-wal-design.md](../specs/2026-07-09-crabka-gres-g2-substrate-wal-design.md).
- **Load-bearing seam signatures (verified):**
  - `crabka_pgexec::Committer` — `async fn commit(&self, ops: Vec<WriteOp>) -> Result<(), ExecError>` (async-trait).
  - `crabka_pgexec::Linearizer` — `async fn ensure_readable(&self) -> Result<(), ExecError>`.
  - `crabka_pgexec::SqlEngine::replicated(catalog_kv: Arc<dyn Kv>, sm_kv: Arc<dyn Kv>, committer: Arc<dyn Committer>, linearizer: Arc<dyn Linearizer>) -> Result<Self, ExecError>`; single-node passes the same `Arc` twice; call `engine.reseed_counters()` after replay.
  - `crabka_pgkv::WriteOp` — `Put { key: Vec<u8>, value: Vec<u8> } | Delete { key: Vec<u8> }` (serde-derivable but we hand-frame).
  - `ExecError::{NotLeader, Unavailable}` variants exist (the cluster's committer mapped to them).
  - Producer: `Producer::builder().bootstrap(..).transactional_id(..).acks(Acks::All).build().await`; `init_transactions()`, `begin_transaction() -> Transaction<'_>`, `Transaction::commit()/abort()`, `send(ProducerRecord) -> oneshot::Receiver<Result<RecordMetadata, ProducerError>>`, `ProducerError::FencedProducer`.
  - Fetch: `crabka_client_core::fetch_partition_with_isolation(&conn, topic, topic_uuid, partition, offset, max_wait_ms, max_bytes, isolation: i8)`; isolation `1` = READ_COMMITTED.
- **Merge rules (must match the donor's cluster semantics):** counter keys (`next_xid`, `/0/seq/*`) max-merge on 8-byte BE u64; clog keys write-once with first terminal decision winning (`crabka_pgmvcc::clog::is_terminal`); all else LWW. Replay must also fold within a batch (pending-map), exactly like the donor's `durable.rs`.
- **Record sizing (scaling-review amendment):** the writer chunks a batch across multiple `GRW1` records when its encoded size exceeds `max_record_bytes` (default 1 MiB); the enclosing Kafka transaction makes the chunk group atomic to READ_COMMITTED replay. Never reject a statement for write-set size.
- **Local-store persistence (scaling-review amendment):** the substrate read model and replay use a no-fsync cache mode (`FjallKv` with `PersistMode::Buffer`/periodic); the store is disposable, so `SyncAll`-per-batch would be pure waste inside commit latency and recovery. The durable local mode's fsync contract is unchanged.
- **No behavior changes** to local mode; the conformance baseline gates the substrate mode at identical parity.
- **New tests:** `assert2`, condition-driven bounded waits (never settle-sleeps), nextest.
- **Lints/format/commits:** workspace pedantic `-D warnings`; `cargo +nightly fmt`; conventional commits ending with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- **Version placeholder:** `0.3.9` in path-dep `version =` fields means "current workspace version at execution time".

---

## Batch 1 — foundations (run Tasks 1 and 2 in parallel; disjoint file sets)

### Task 1: `crabka-gres-substrate` crate — GRW1 framing + merge-rule apply (+ pgkv prefix helpers)

**Files:**
- Create: `crates/gres-substrate/Cargo.toml`, `src/lib.rs`, `src/error.rs`, `src/frame.rs`, `src/apply.rs`, `README.md`
- Modify: `crates/pgkv/src/key.rs` (only if the prefix helpers below are missing), `release-plz.toml`, `.cargo/mutants.toml` is NOT touched (new G-2 code IS mutation-tested)

**Interfaces:**
- Consumes: `crabka_pgkv::{Kv, WriteOp, KvError, key}`, `crabka_pgmvcc::clog`.
- Produces: `WalFrame { journal_seq: u64, ops: Vec<WriteOp> }` with `encode() -> Vec<u8>` / `decode(&[u8]) -> Result<WalFrame, SubstrateError>`; `apply_frame(kv: &dyn Kv, ops: &[WriteOp]) -> Result<(), KvError>`; `SubstrateError` (thiserror). Tasks 3–5 consume all three.

- [ ] **Step 1: Crate scaffold.** `crates/gres-substrate/Cargo.toml`:

```toml
[package]
name = "crabka-gres-substrate"
publish = false
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Substrate-backed durability for Crabka Gres tenant computes: WAL journaling to a per-tenant topic, fencing, and replay"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-gres-substrate"
readme = "README.md"
keywords = ["postgres", "wal", "durability", "crabka", "gres"]
categories = ["database-implementations"]

[lints]
workspace = true

[dependencies]
crabka-pgexec = { version = "0.3.9", path = "../pgexec" }
crabka-pgkv = { version = "0.3.9", path = "../pgkv" }
crabka-pgmvcc = { version = "0.3.9", path = "../pgmvcc" }
crabka-client-core = { version = "0.3.9", path = "../client-core" }
crabka-client-producer = { version = "0.3.9", path = "../client-producer" }
crabka-client-admin = { version = "0.3.9", path = "../client-admin" }
crabka-protocol = { version = "0.3.9", path = "../protocol" }
async-trait = { workspace = true }
bytes = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "net", "io-util", "time", "sync"] }
tracing = { workspace = true }

[dev-dependencies]
assert2 = { workspace = true }
proptest = { workspace = true }
crabka-broker = { version = "0.3.9", path = "../broker" }
tempfile = { workspace = true }
tokio = { workspace = true, features = ["full"] }
```

`src/lib.rs` starts as:

```rust
//! Substrate-backed durability for Crabka Gres tenant computes.
//!
//! Implements the engine's [`crabka_pgexec::Committer`] and
//! [`crabka_pgexec::Linearizer`] seams over a per-tenant WAL topic
//! (`__gres_wal.<tenant>`): a single writer task group-commits framed batches
//! inside Kafka transactions (the broker's coordinator-checked producer epoch
//! is the zombie fence), and recovery replays the topic before serving.
//!
//! # Key Types
//! - [`WalFrame`] — the `GRW1` record framing.
//! - [`apply_frame`] — replay application with the engine's merge rules.

pub mod apply;
pub mod error;
pub mod frame;

pub use apply::apply_frame;
pub use error::SubstrateError;
pub use frame::WalFrame;
```

Also add to `release-plz.toml`'s internal group:

```toml
[[package]]
name = "crabka-gres-substrate"
publish = false
release = false
```

- [ ] **Step 2: Error enum** — `src/error.rs`:

```rust
//! Error type for the substrate durability layer.

/// Errors from WAL framing, journaling, and recovery.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SubstrateError {
    #[error("malformed GRW1 frame: {0}")]
    Frame(String),
    #[error("journal sequence gap: expected {expected}, found {found} at offset {offset}")]
    SequenceGap { expected: u64, found: u64, offset: i64 },
    #[error("fenced: a newer compute generation owns this tenant")]
    Fenced,
    #[error("WAL topic unavailable: {0}")]
    Unavailable(String),
    #[error(transparent)]
    Kv(#[from] crabka_pgkv::KvError),
}
```

(Adapt the `KvError` path/name to the vendored crate's actual error export.)

- [ ] **Step 3: Write the failing frame tests** — in `src/frame.rs`, module skeleton with tests first:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::WriteOp;
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn round_trips_a_mixed_batch() {
        let frame = WalFrame {
            journal_seq: 42,
            ops: vec![
                WriteOp::Put { key: b"k1".to_vec(), value: b"v1".to_vec() },
                WriteOp::Delete { key: b"k2".to_vec() },
            ],
        };
        let decoded = WalFrame::decode(&frame.encode()).expect("decode");
        assert!(decoded == frame);
    }

    #[test]
    fn rejects_unknown_version() {
        let mut bytes = WalFrame { journal_seq: 0, ops: vec![] }.encode();
        bytes[0] = 99;
        assert!(WalFrame::decode(&bytes).is_err());
    }

    #[test]
    fn rejects_truncated_frame() {
        let bytes = WalFrame {
            journal_seq: 7,
            ops: vec![WriteOp::Put { key: b"k".to_vec(), value: b"v".to_vec() }],
        }
        .encode();
        assert!(WalFrame::decode(&bytes[..bytes.len() - 1]).is_err());
    }

    proptest! {
        #[test]
        fn prop_round_trip(seq in any::<u64>(), ops in proptest::collection::vec(op_strategy(), 0..32)) {
            let frame = WalFrame { journal_seq: seq, ops };
            prop_assert_eq!(WalFrame::decode(&frame.encode()).expect("decode"), frame);
        }
    }

    fn op_strategy() -> impl Strategy<Value = WriteOp> {
        prop_oneof![
            (proptest::collection::vec(any::<u8>(), 0..64), proptest::collection::vec(any::<u8>(), 0..256))
                .prop_map(|(key, value)| WriteOp::Put { key, value }),
            proptest::collection::vec(any::<u8>(), 0..64).prop_map(|key| WriteOp::Delete { key }),
        ]
    }
}
```

(`WalFrame` needs `#[derive(Debug, Clone, PartialEq, Eq)]`.)

- [ ] **Step 4: Run to verify failure** — `cargo nextest run -p crabka-gres-substrate` → FAIL: `WalFrame` undefined.

- [ ] **Step 5: Implement the framing** — `src/frame.rs` body:

```rust
//! `GRW1` record framing: one frame per `Committer` batch.
//!
//! Layout (all integers big-endian):
//! `[version: u8][journal_seq: u64][op_count: u32]` then per op
//! `[tag: u8]` (0 = Put, 1 = Delete) `[klen: u32][key][vlen: u32][value]`
//! (`vlen`/`value` present only for Put). Wire lengths are bounds-checked
//! against the remaining buffer before any allocation.

use crabka_pgkv::WriteOp;

use crate::error::SubstrateError;

/// Current (only) frame version.
pub const GRW1_VERSION: u8 = 1;

/// One journaled `Committer` batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFrame {
    /// Monotone per-generation sequence; a replay tripwire, not a protocol.
    pub journal_seq: u64,
    /// The batch, in engine order.
    pub ops: Vec<WriteOp>,
}

impl WalFrame {
    /// Serialize to the `GRW1` byte layout.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        out.push(GRW1_VERSION);
        out.extend_from_slice(&self.journal_seq.to_be_bytes());
        out.extend_from_slice(&u32::try_from(self.ops.len()).expect("op count fits u32").to_be_bytes());
        for op in &self.ops {
            match op {
                WriteOp::Put { key, value } => {
                    out.push(0);
                    push_chunk(&mut out, key);
                    push_chunk(&mut out, value);
                }
                WriteOp::Delete { key } => {
                    out.push(1);
                    push_chunk(&mut out, key);
                }
            }
        }
        out
    }

    /// Parse a `GRW1` frame; every length is validated before use.
    pub fn decode(bytes: &[u8]) -> Result<Self, SubstrateError> {
        let mut r = Reader { bytes, at: 0 };
        let version = r.u8()?;
        if version != GRW1_VERSION {
            return Err(SubstrateError::Frame(format!("unknown version {version}")));
        }
        let journal_seq = r.u64()?;
        let op_count = r.u32()?;
        let mut ops = Vec::new();
        for _ in 0..op_count {
            let tag = r.u8()?;
            let key = r.chunk()?.to_vec();
            let op = match tag {
                0 => WriteOp::Put { key, value: r.chunk()?.to_vec() },
                1 => WriteOp::Delete { key },
                other => return Err(SubstrateError::Frame(format!("unknown op tag {other}"))),
            };
            ops.push(op);
        }
        if r.at != bytes.len() {
            return Err(SubstrateError::Frame("trailing bytes".into()));
        }
        Ok(Self { journal_seq, ops })
    }

    fn encoded_len(&self) -> usize {
        13 + self
            .ops
            .iter()
            .map(|op| match op {
                WriteOp::Put { key, value } => 9 + key.len() + value.len(),
                WriteOp::Delete { key } => 5 + key.len(),
            })
            .sum::<usize>()
    }
}

fn push_chunk(out: &mut Vec<u8>, chunk: &[u8]) {
    out.extend_from_slice(&u32::try_from(chunk.len()).expect("chunk fits u32").to_be_bytes());
    out.extend_from_slice(chunk);
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], SubstrateError> {
        let end = self
            .at
            .checked_add(n)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| SubstrateError::Frame("truncated frame".into()))?;
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, SubstrateError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, SubstrateError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().expect("4 bytes")))
    }

    fn u64(&mut self) -> Result<u64, SubstrateError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().expect("8 bytes")))
    }

    fn chunk(&mut self) -> Result<&'a [u8], SubstrateError> {
        let len = self.u32()?;
        self.take(usize::try_from(len).expect("u32 fits usize"))
    }
}
```

- [ ] **Step 6: Run frame tests** — `cargo nextest run -p crabka-gres-substrate` → PASS (unit + proptest).

- [ ] **Step 7: pgkv cache-mode constructor.** Add `FjallKv::open_cache(path) -> Result<Self, KvError>` beside `open`: identical except mutations skip the per-op `sync()` (`PersistMode::SyncAll`) tail — construct with a mode flag consulted by `put`/`delete`/`write_batch`, plus a periodic/explicit `persist_async` escape hatch if fjall's API makes it one line. Preserve the donor's DO-NOT-REFACTOR fsync comment on the durable path and extend it: the cache mode exists for the Gres substrate read model, where the topic is the truth. TDD: a test asserting `open_cache` round-trips data within a process (durability across reopen is explicitly NOT contracted — document that in the constructor's rustdoc).

- [ ] **Step 7b: pgkv prefix helpers.** Check `crates/pgkv/src/key.rs` for public `clog_prefix()` and a seq prefix helper (`seq_prefix()` or equivalent). The donor's cluster crate matched clog keys by `clog_prefix()` and seq keys via an `is_seq_key` built on the seq key shape. If either helper is missing or `pub(crate)`, add/publicize (with rustdoc + a unit test asserting `seq_key(t)` starts with `seq_prefix()` and `clog_key(x)` starts with `clog_prefix()`), e.g.:

```rust
/// Prefix of every per-table sequence-allocator key (`/0/seq/<table>`).
#[must_use]
pub fn seq_prefix() -> Vec<u8> { /* mirror seq_key's construction, minus the table id */ }
```

(Mirror the exact byte construction used by `seq_key` — copy its path segments, not a guess.)

- [ ] **Step 8: Write the failing apply tests** — `src/apply.rs` tests, ported from the donor's `durable.rs` cases:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::{key, Kv, MemKv, WriteOp};
    use crabka_pgmvcc::clog;

    use super::*;

    fn u64_be(v: u64) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }

    #[test]
    fn counter_keys_max_merge_across_frames() {
        let kv = MemKv::default();
        apply_frame(&kv, &[WriteOp::Put { key: key::next_xid_key(), value: u64_be(7) }]).expect("apply");
        apply_frame(&kv, &[WriteOp::Put { key: key::next_xid_key(), value: u64_be(6) }]).expect("apply");
        assert!(kv.get(&key::next_xid_key()).expect("get") == Some(u64_be(7)));
    }

    #[test]
    fn counter_keys_fold_within_one_frame() {
        let kv = MemKv::default();
        apply_frame(
            &kv,
            &[
                WriteOp::Put { key: key::next_xid_key(), value: u64_be(9) },
                WriteOp::Put { key: key::next_xid_key(), value: u64_be(8) },
            ],
        )
        .expect("apply");
        assert!(kv.get(&key::next_xid_key()).expect("get") == Some(u64_be(9)));
    }

    #[test]
    fn clog_first_terminal_decision_wins() {
        let kv = MemKv::default();
        apply_frame(&kv, &[clog::put_op(11, clog::XidStatus::Aborted)]).expect("apply");
        apply_frame(&kv, &[clog::put_op(11, clog::XidStatus::Committed)]).expect("apply");
        let stored = kv.get(&key::clog_key(11)).expect("get").expect("present");
        assert!(clog::is_terminal(&stored));
        assert!(stored[0] == 2, "aborted (first decision) must win");
    }

    #[test]
    fn plain_keys_are_last_writer_wins() {
        let kv = MemKv::default();
        apply_frame(&kv, &[WriteOp::Put { key: b"a".to_vec(), value: b"1".to_vec() }]).expect("apply");
        apply_frame(&kv, &[WriteOp::Put { key: b"a".to_vec(), value: b"2".to_vec() }]).expect("apply");
        apply_frame(&kv, &[WriteOp::Delete { key: b"a".to_vec() }]).expect("apply");
        assert!(kv.get(b"a").expect("get").is_none());
    }
}
```

(Adjust `clog::put_op`/`XidStatus`/`is_terminal` paths and the aborted status byte (`2`) to the vendored `crabka-pgmvcc` exports — the donor defines 1 = Committed, 2 = Aborted.)

- [ ] **Step 9: Run to verify failure** — `cargo nextest run -p crabka-gres-substrate apply` → FAIL: `apply_frame` undefined.

- [ ] **Step 10: Implement `apply_frame`** — `src/apply.rs`:

```rust
//! Replay application with the engine's merge semantics.
//!
//! A strictly-ordered single-writer journal still needs two non-LWW rules
//! (mirrored from the donor's replicated state machine): counter keys
//! max-merge because sessions fold counter ops at allocation time, so journal
//! order can carry non-monotone values; clog keys are write-once with the
//! first terminal decision winning, because an abort race can journal two
//! decisions for one xid.

use std::collections::HashMap;

use crabka_pgkv::{key, Kv, KvError, WriteOp};
use crabka_pgmvcc::clog;

/// True for the `next_xid` counter and any per-table sequence key.
fn is_counter_key(k: &[u8]) -> bool {
    k == key::next_xid_key().as_slice() || k.starts_with(&key::seq_prefix())
}

/// True for any clog (`pg_xact`) status key.
fn is_clog_key(k: &[u8]) -> bool {
    k.starts_with(&key::clog_prefix())
}

/// Apply one journaled batch to `kv` with max-merge counters and
/// write-once clog, folding duplicates within the batch.
pub fn apply_frame(kv: &dyn Kv, ops: &[WriteOp]) -> Result<(), KvError> {
    let mut counters: HashMap<Vec<u8>, u64> = HashMap::new();
    let mut decided: HashMap<Vec<u8>, ()> = HashMap::new();
    let mut adjusted = Vec::with_capacity(ops.len());
    for op in ops {
        match op {
            WriteOp::Put { key: k, value } if is_counter_key(k) => {
                let incoming = u64_be(value);
                let current = match counters.get(k) {
                    Some(v) => *v,
                    None => kv.get(k)?.as_deref().map_or(0, u64_be),
                };
                let merged = incoming.max(current);
                counters.insert(k.clone(), merged);
                adjusted.push(WriteOp::Put { key: k.clone(), value: merged.to_be_bytes().to_vec() });
            }
            WriteOp::Put { key: k, value } if is_clog_key(k) => {
                let already_terminal = decided.contains_key(k)
                    || kv.get(k)?.as_deref().is_some_and(clog::is_terminal);
                if already_terminal {
                    continue;
                }
                if clog::is_terminal(value) {
                    decided.insert(k.clone(), ());
                }
                adjusted.push(op.clone());
            }
            other => adjusted.push(other.clone()),
        }
    }
    kv.write_batch(&adjusted)
}

/// Decode an 8-byte big-endian counter value; shorter/absent decodes as 0.
fn u64_be(bytes: &[u8]) -> u64 {
    let mut buf = [0_u8; 8];
    let n = bytes.len().min(8);
    buf[8 - n..].copy_from_slice(&bytes[bytes.len() - n..]);
    u64::from_be_bytes(buf)
}
```

(Match `u64_be` lenience to the donor's `u64_be` in the cluster store — check its exact handling of short values and mirror it.)

- [ ] **Step 11: Run, lint, format, README, commit**

```bash
cargo nextest run -p crabka-gres-substrate
cargo clippy -p crabka-gres-substrate -p crabka-pgkv --all-targets -- -D warnings
cargo +nightly fmt -p crabka-gres-substrate -p crabka-pgkv
```

Write `crates/gres-substrate/README.md` (internal template from the G-1 plan) with one-liner "Substrate-backed durability for Crabka Gres tenant computes: WAL journaling to a per-tenant topic, fencing, and replay." and an Overview paragraph naming the three pieces (framing, writer, recovery) and the G-2 spec link. Commit:

```bash
git add crates/gres-substrate crates/pgkv release-plz.toml
git commit -m "feat(gres): gres-substrate crate with GRW1 framing and merge-rule apply

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

### Task 2: Route FDW-object DDL through the Committer seam

Fixes the verified donor bypass: `CREATE/DROP FOREIGN DATA WRAPPER / SERVER / USER MAPPING / FOREIGN TABLE` currently write the kv store directly from the catalog, skipping replication in every mode.

**Files:**
- Modify: `crates/pgcatalog/src/lib.rs` (ops-returning variants), `crates/pgexec/src/exec.rs` (FDW DDL arms return ops), `crates/pgexec/tests/` (new test file `fdw_ddl_seam.rs`)

**Interfaces:**
- Consumes: existing `create_fdw/create_server/create_user_mapping/create_foreign_table` + drop functions in `crabka-pgcatalog`, and the executor's DDL path (`run_ddl` commits whatever ops `execute_ddl` returns).
- Produces: `crabka_pgcatalog::{create_fdw_ops, create_server_ops, create_user_mapping_ops, create_foreign_table_ops, drop_fdw_ops, drop_server_ops, drop_user_mapping_ops, drop_foreign_table_ops}` — same validation and encoding as the direct functions, returning `Vec<WriteOp>` instead of writing. Task 4's replay makes these durable on the substrate.

- [ ] **Step 1: Write the failing seam test** — `crates/pgexec/tests/fdw_ddl_seam.rs`. Use the same in-process pattern as the executor's other integration tests (spawn a pgwire server over a `SqlEngine`), but construct the engine via `SqlEngine::replicated` with a counting committer so the test observes what flows through the seam:

```rust
//! FDW-object DDL must flow through the Committer seam (not write kv directly).

use std::sync::{Arc, Mutex};

use assert2::assert;
use crabka_pgexec::{Committer, ExecError, LocalLinearizer, SqlEngine};
use crabka_pgkv::{Kv, MemKv, WriteOp};

/// Applies batches to the store and records every batch it sees.
struct RecordingCommitter {
    kv: Arc<dyn Kv>,
    batches: Mutex<Vec<Vec<WriteOp>>>,
}

#[async_trait::async_trait]
impl Committer for RecordingCommitter {
    async fn commit(&self, ops: Vec<WriteOp>) -> Result<(), ExecError> {
        self.kv.write_batch(&ops).map_err(|_| ExecError::Unavailable)?;
        self.batches.lock().expect("lock").push(ops);
        Ok(())
    }
}

#[tokio::test]
async fn foreign_object_ddl_flows_through_the_committer() {
    let store: Arc<dyn Kv> = Arc::new(MemKv::default());
    let committer = Arc::new(RecordingCommitter { kv: store.clone(), batches: Mutex::new(Vec::new()) });
    let engine = SqlEngine::replicated(store.clone(), store, committer.clone(), Arc::new(LocalLinearizer))
        .expect("engine");

    // Drive the FDW DDL through a session exactly as pgwire would.
    // (Use the same session-driving helper the neighboring executor tests use —
    //  e.g. spawn crabka_pgwire::server::serve on a loopback listener and run
    //  the statements via tokio_postgres, mirroring tests/end_to_end.rs.)
    run_sql(&engine, "CREATE FOREIGN DATA WRAPPER kafka_fdw").await;
    run_sql(&engine, "CREATE SERVER s1 FOREIGN DATA WRAPPER kafka_fdw").await;

    let batches = committer.batches.lock().expect("lock");
    let fdw_batches = batches.iter().filter(|b| !b.is_empty()).count();
    assert!(fdw_batches >= 2, "each FDW DDL statement must commit a non-empty batch through the seam");
}
```

(Fill `run_sql` with the exact helper idiom used by the vendored executor tests — copy from `crates/pgexec/tests/end_to_end.rs` at execution time; exact FDW DDL syntax should mirror what `crates/gres-fdw` tests use, including any `IMPORT FOREIGN SCHEMA` prerequisites. If `ExecError::Unavailable` carries fields, adapt the construction.)

- [ ] **Step 2: Run to verify failure** — `cargo nextest run -p crabka-pgexec --test fdw_ddl_seam` → FAIL: the FDW DDL arms return empty ops today (the batches are empty).

- [ ] **Step 3: Add ops-returning catalog functions.** In `crates/pgcatalog/src/lib.rs`, for each of the eight foreign-object CRUD functions: extract the existing body's validation + encoding into a `*_ops(...) -> Result<Vec<WriteOp>, CatalogError>` sibling (same name + `_ops` suffix, mirroring the existing `create_table_ops`/`drop_table_ops` shape), and reimplement the original direct function as `let ops = Self::*_ops(...)?; self.kv.write_batch(&ops)?;` so non-executor callers keep working unchanged. No behavior change; identical bytes written.

- [ ] **Step 4: Route the executor's FDW DDL arms through the seam.** In `crates/pgexec/src/exec.rs`, the FDW DDL match arms (marked by the donor comment "writes its single small batch directly … return an EMPTY ops vec") switch from calling the direct catalog functions to calling the `*_ops` variants and **returning** those ops, so `run_ddl`'s existing `committer.commit(ops)` call makes them durable. Delete the now-stale comment; update it to state the ops flow through the seam.

- [ ] **Step 5: Run the seam test + full engine suites**

```bash
cargo nextest run -p crabka-pgexec --test fdw_ddl_seam
cargo nextest run -p crabka-pgexec -p crabka-pgcatalog -p crabka-gres-fdw
```

Expected: seam test PASSES; every pre-existing executor/catalog/fdw test stays green (the fdw roundtrip test exercises the full `CREATE SERVER`/`IMPORT FOREIGN SCHEMA` path and proves behavior is unchanged).

- [ ] **Step 6: Lint, format, commit**

```bash
cargo clippy -p crabka-pgexec -p crabka-pgcatalog --all-targets -- -D warnings
cargo +nightly fmt -p crabka-pgexec -p crabka-pgcatalog
git add crates/pgexec crates/pgcatalog
git commit -m "fix(pgexec): route FDW-object DDL through the Committer seam

Foreign-object catalog CRUD previously wrote the kv store directly, bypassing
replication in every mode (a donor wart). Catalog gains *_ops variants; the
executor's FDW DDL arms return those ops for run_ddl to commit like any other
DDL.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Batch 2 — the substrate runtime (serial; needs Task 1)

### Task 3: WAL writer task, `SubstrateCommitter`, `SubstrateLinearizer`, topic-ensure, recovery

**Files:**
- Create: `crates/gres-substrate/src/writer.rs`, `src/committer.rs`, `src/topic.rs`, `src/recover.rs`
- Modify: `crates/gres-substrate/src/lib.rs` (module list + re-exports), `src/error.rs` (if new variants needed)

**Interfaces:**
- Consumes: Task 1's `WalFrame`/`apply_frame`; producer/client APIs per Global Constraints.
- Produces (consumed by Task 4's binary wiring and Task 5's tests):
  - `topic::wal_topic(tenant: &str) -> String` (= `__gres_wal.<tenant>`) and `topic::ensure_wal_topic(admin: &mut AdminClient, tenant: &str, replicas: i32) -> Result<(), SubstrateError>` (1 partition, `cleanup.policy=delete`, `retention.ms=-1`, tolerate `TOPIC_ALREADY_EXISTS`).
  - `recover::recover(bootstrap: &str, tenant: &str, store: Arc<dyn Kv>) -> Result<Recovered, SubstrateError>` where `Recovered { producer: Producer, next_journal_seq: u64 }` — fence → stable end → replay → return.
  - `writer::spawn_wal_writer(producer: Producer, topic: String, store: Arc<dyn Kv>, next_journal_seq: u64) -> WalHandle` where `WalHandle { tx: mpsc::Sender<WalRequest>, fenced: Arc<AtomicBool> }`.
  - `committer::SubstrateCommitter::new(handle: WalHandle) -> Self` (implements `Committer`); `committer::SubstrateLinearizer::new(fenced: Arc<AtomicBool>) -> Self` (implements `Linearizer`).

- [ ] **Step 1: Topic ensure** — `src/topic.rs` (the `__remote_log_metadata` idiom): build `CreateTopicSpec { name: wal_topic(tenant), partitions: 1, replicas, configs: BTreeMap::from([("cleanup.policy".into(), "delete".into()), ("retention.ms".into(), "-1".into())]) }`, call `admin.create_topics(&[spec], timeout_ms)`, treat outcome error code 36 (`TOPIC_ALREADY_EXISTS`) as success. Unit-testable only against a broker — covered by Task 5's harness; this task's gate is compile + clippy.

- [ ] **Step 2: The writer task** — `src/writer.rs`. Core loop (complete logic; adapt names to the producer's actual API at execution time — signatures are in Global Constraints):

```rust
//! The single writer task that owns the tenant's WAL producer.
//!
//! One task per compute owns the transactional producer (one open Kafka
//! transaction at a time), so concurrent sessions' batches group-commit:
//! drain the queue, produce every batch in one transaction, EndTxn, apply to
//! the local store in order, ack every waiter. Queue order is journal order.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crabka_client_producer::{Producer, ProducerError, ProducerRecord};
use crabka_pgkv::{Kv, WriteOp};
use tokio::sync::{mpsc, oneshot};

use crate::apply::apply_frame;
use crate::error::SubstrateError;
use crate::frame::WalFrame;

/// One enqueued `Committer` batch awaiting durability.
pub struct WalRequest {
    pub ops: Vec<WriteOp>,
    pub ack: oneshot::Sender<Result<(), SubstrateError>>,
}

/// Handle the committer uses to reach the writer.
#[derive(Clone)]
pub struct WalHandle {
    pub tx: mpsc::Sender<WalRequest>,
    pub fenced: Arc<AtomicBool>,
}

/// Spawn the writer; it runs until the channel closes or the producer fences.
pub fn spawn_wal_writer(
    producer: Producer,
    topic: String,
    store: Arc<dyn Kv>,
    next_journal_seq: u64,
) -> WalHandle {
    let (tx, rx) = mpsc::channel(1024);
    let fenced = Arc::new(AtomicBool::new(false));
    tokio::spawn(run(producer, topic, store, next_journal_seq, rx, Arc::clone(&fenced)));
    WalHandle { tx, fenced }
}

async fn run(
    producer: Producer,
    topic: String,
    store: Arc<dyn Kv>,
    mut next_seq: u64,
    mut rx: mpsc::Receiver<WalRequest>,
    fenced: Arc<AtomicBool>,
) {
    while let Some(first) = rx.recv().await {
        // Group: the head request plus everything already queued.
        let mut group = vec![first];
        while let Ok(req) = rx.try_recv() {
            group.push(req);
        }
        match commit_group(&producer, &topic, &store, &mut next_seq, &group).await {
            Ok(()) => {
                for req in group {
                    let _ = req.ack.send(Ok(()));
                }
            }
            Err(err) => {
                let is_fence = matches!(err, SubstrateError::Fenced);
                for req in group {
                    let _ = req.ack.send(Err(clone_err(&err)));
                }
                if is_fence {
                    fenced.store(true, Ordering::SeqCst);
                    tracing::error!("gres WAL writer fenced; a newer generation owns this tenant");
                    return; // channel drops; all future commits fail fast
                }
            }
        }
    }
}

async fn commit_group(
    producer: &Producer,
    topic: &str,
    store: &Arc<dyn Kv>,
    next_seq: &mut u64,
    group: &[WalRequest],
) -> Result<(), SubstrateError> {
    let txn = producer.begin_transaction().await.map_err(map_producer_err)?;
    let mut acks = Vec::new();
    let base_seq = *next_seq;
    let mut seq = base_seq;
    for req in group {
        // Scaling-review amendment: chunk oversized batches across records at
        // max_record_bytes; the enclosing Kafka txn keeps the chunk group atomic
        // to READ_COMMITTED replay. chunk_ops splits req.ops greedily so each
        // frame's encoded size stays under the cap (a single op larger than the
        // cap gets its own frame — the broker cap is the real limit there).
        for chunk in chunk_ops(&req.ops, MAX_RECORD_BYTES) {
            let frame = WalFrame { journal_seq: seq, ops: chunk };
            seq += 1;
            let record = ProducerRecord {
                topic: topic.to_string(),
                partition: Some(0),
                value: Some(frame.encode().into()),
                ..Default::default()
            };
            acks.push(producer.send(record).await);
        }
    }
    for ack in acks {
        ack.await
            .map_err(|_| SubstrateError::Unavailable("producer dropped ack".into()))?
            .map_err(map_producer_err)?;
    }
    txn.commit().await.map_err(|e| map_end_txn_err(e))?;
    // Durable: now apply to the local read model, in order, then advance.
    for req in group {
        store.apply_or_panic(&req.ops); // see note below
    }
    *next_seq = seq;
    Ok(())
}

/// Greedy split of a batch into chunks whose encoded frames stay under `cap`
/// (a single op larger than `cap` gets its own frame). Pure; proptest that the
/// concatenation of chunks equals the input and every multi-op chunk is under cap.
fn chunk_ops(ops: &[WriteOp], cap: usize) -> Vec<Vec<WriteOp>> { /* sizes via WalFrame::encoded_len parts */ todo_impl!() }
```

(`todo_impl!()` is plan shorthand — implement the greedy accumulator inline; ~15 lines. `MAX_RECORD_BYTES: usize = 1 << 20` as a module constant, overridable via writer config.)

**Two adaptations the implementer makes concretely (not placeholders — the decision is made, only names vary):**
1. `store.apply_or_panic` is pseudocode for: `apply_frame(store.as_ref(), &req.ops).expect("local apply after durable commit")` — a local-store failure after durable EndTxn is unrecoverable state divergence; crash loudly (the successor replays cleanly).
2. `clone_err`/`map_producer_err`/`map_end_txn_err`: `SubstrateError` needs `Clone` on the variants used here (or wrap in `Arc<SubstrateError>` for acks); map `ProducerError::FencedProducer` → `SubstrateError::Fenced`, everything else → `SubstrateError::Unavailable(err.to_string())`. `Transaction::commit()` returns `EndTransactionError` — extract the inner producer error for the same mapping (check `crates/client-producer/src/transactional.rs` for its shape).

- [ ] **Step 3: The seams** — `src/committer.rs`:

```rust
//! The engine-facing seam implementations.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crabka_pgexec::{Committer, ExecError, Linearizer};
use crabka_pgkv::WriteOp;
use tokio::sync::oneshot;

use crate::error::SubstrateError;
use crate::writer::{WalHandle, WalRequest};

/// Journals every engine batch through the WAL writer; returns once durable
/// and locally applied (the [`Committer`] contract).
pub struct SubstrateCommitter {
    handle: WalHandle,
}

impl SubstrateCommitter {
    #[must_use]
    pub fn new(handle: WalHandle) -> Self {
        Self { handle }
    }
}

#[async_trait::async_trait]
impl Committer for SubstrateCommitter {
    async fn commit(&self, ops: Vec<WriteOp>) -> Result<(), ExecError> {
        let (ack, done) = oneshot::channel();
        self.handle
            .tx
            .send(WalRequest { ops, ack })
            .await
            .map_err(|_| fence_or_unavailable(&self.handle))?;
        match done.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(SubstrateError::Fenced)) => Err(ExecError::NotLeader),
            Ok(Err(_)) | Err(_) => Err(fence_or_unavailable(&self.handle)),
        }
    }
}

fn fence_or_unavailable(handle: &WalHandle) -> ExecError {
    if handle.fenced.load(Ordering::SeqCst) {
        ExecError::NotLeader
    } else {
        ExecError::Unavailable
    }
}

/// Refuses reads once this compute has been fenced.
pub struct SubstrateLinearizer {
    fenced: Arc<AtomicBool>,
}

impl SubstrateLinearizer {
    #[must_use]
    pub fn new(fenced: Arc<AtomicBool>) -> Self {
        Self { fenced }
    }
}

#[async_trait::async_trait]
impl Linearizer for SubstrateLinearizer {
    async fn ensure_readable(&self) -> Result<(), ExecError> {
        if self.fenced.load(Ordering::SeqCst) {
            return Err(ExecError::NotLeader);
        }
        Ok(())
    }
}
```

(If `ExecError::{NotLeader, Unavailable}` carry payloads in the vendored crate, construct them the way `crates/cluster`'s committer did in the donor — the mapping intent is fixed.)

- [ ] **Step 4: Recovery** — `src/recover.rs`. Order is load-bearing (fence FIRST):

```rust
//! Fence-then-replay recovery: after `init_transactions()` bumps the epoch,
//! no predecessor can add committed records, so the stable end read next is
//! final; replay applies every committed frame with the merge rules and
//! verifies journal_seq continuity, refusing to serve on any gap.
```

Logic (complete; client-call shapes per Global Constraints and the `crates/gres-fdw/src/source.rs` precedents for metadata/fetch). **Replay terminates at the compute's own barrier record — do NOT use ListOffsets as the target (Crabka's handler ignores `isolation_level` and returns LEO; amended per the G-3 design):**
1. `ensure_wal_topic(...)`.
2. Build the transactional producer (`transactional_id = format!("__gres.{tenant}")`, `acks(Acks::All)`), `producer.init_transactions().await` — the fence.
3. **Produce the barrier:** peek the last committed frame's `journal_seq` is unknown until replay, so the barrier carries `journal_seq = BARRIER_SEQ` (`u64::MAX`, reserved — `WalFrame` with `ops: vec![]`); produce it in its own Kafka transaction (`begin_transaction` → `send` → `commit`). Record nothing else in the txn. (Reserve `u64::MAX` in `frame.rs` with a doc comment and a unit test that ordinary writers never reach it.)
4. Resolve topic id via `AdminClient::metadata`; open a `client_core::Connection` to the partition leader (the fdw's `source.rs` shows the exact connect + metadata idiom).
5. Replay loop: from `offset = 0`, `fetch_partition_with_isolation(&conn, topic, topic_id, 0, offset, max_wait_ms, max_bytes, 1)`; for each record: `WalFrame::decode`; if `journal_seq == BARRIER_SEQ`: this is a barrier — if it is OURS (track: count barriers produced by this recovery = 1; ours is the first barrier encountered *after* the fence, i.e. simply the first barrier whose offset is ≥ the offset our own produce ack reported — capture `RecordMetadata.offset` from step 3) then replay is complete, break; a FOREIGN barrier (an older generation's) is skipped and replay continues. Otherwise: assert `journal_seq == expected` else return `SubstrateError::SequenceGap { .. }`; `apply_frame(store, &frame.ops)?`; `expected += 1`. Advance `offset = last.offset + 1`. Empty fetch before our barrier's known offset retries (bounded attempts, then `Unavailable`).
6. Return `Recovered { producer, next_journal_seq: expected }`. (`expected` starts at 0 and counts only non-barrier frames; barriers never consume engine sequence numbers, so cross-generation continuity asserts stay exact.)

- [ ] **Step 5: Wire the lib** — add `pub mod committer; pub mod recover; pub mod topic; pub mod writer;` + re-export `SubstrateCommitter`, `SubstrateLinearizer`, `spawn_wal_writer`, `recover::{recover, Recovered}`, `topic::{ensure_wal_topic, wal_topic}` from `lib.rs`; extend the crate rustdoc's Key Types list.

- [ ] **Step 6: Compile, lint, unit-test, commit**

```bash
cargo nextest run -p crabka-gres-substrate
cargo clippy -p crabka-gres-substrate --all-targets -- -D warnings
cargo +nightly fmt -p crabka-gres-substrate
git add crates/gres-substrate
git commit -m "feat(gres): substrate WAL writer, committer/linearizer seams, fence-then-replay recovery

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

(Broker-backed behavior is exercised in Task 5's integration suite; this task's gate is the unit layer + compile.)

---

## Batch 3 — binary wiring (serial)

### Task 4: `crabka-gres --substrate` mode

**Files:**
- Modify: `crates/gres/Cargo.toml` (add `crabka-gres-substrate = { path = "../gres-substrate" }` dependency), `crates/gres/src/main.rs`, `crates/gres/README.md`

**Interfaces:**
- Consumes: Task 3's `recover`, `spawn_wal_writer`, `SubstrateCommitter`, `SubstrateLinearizer`; `SqlEngine::replicated`; `engine.reseed_counters()`.
- Produces: CLI flags `--substrate-bootstrap <ADDR>` and `--tenant <NAME>` (both required together, conflicting with `--data-dir`); `--cache-dir <PATH>` (optional; fjall read model on ephemeral disk, else `MemKv`).

- [ ] **Step 1: Extend clap.** Add to the serve args:

```rust
    /// Substrate mode: Crabka bootstrap address for the tenant WAL topic.
    #[arg(long, requires = "tenant", conflicts_with = "data_dir")]
    substrate_bootstrap: Option<String>,
    /// Substrate mode: tenant name (owns __gres_wal.<tenant>).
    #[arg(long, requires = "substrate_bootstrap")]
    tenant: Option<String>,
    /// Substrate mode: local read-model cache directory (default: in-memory).
    #[arg(long, requires = "substrate_bootstrap")]
    cache_dir: Option<std::path::PathBuf>,
```

- [ ] **Step 2: Engine construction branch.** In `run_serve`, when `substrate_bootstrap` is set:

```rust
let store: Arc<dyn Kv> = match &args.cache_dir {
    // Cache mode (Task 1 Step 7): the topic is the truth; never fsync the read model.
    Some(dir) => Arc::new(FjallKv::open_cache(dir).map_err(|e| std::io::Error::other(format!("cache dir: {e:?}")))?),
    None => Arc::new(MemKv::default()),
};
let tenant = args.tenant.as_deref().expect("clap requires tenant");
let recovered = crabka_gres_substrate::recover(bootstrap, tenant, Arc::clone(&store))
    .await
    .map_err(|e| std::io::Error::other(format!("substrate recovery: {e}")))?;
let handle = crabka_gres_substrate::spawn_wal_writer(
    recovered.producer,
    crabka_gres_substrate::wal_topic(tenant),
    Arc::clone(&store),
    recovered.next_journal_seq,
);
let mut engine = SqlEngine::replicated(
    Arc::clone(&store),
    store,
    Arc::new(crabka_gres_substrate::SubstrateCommitter::new(handle.clone())),
    Arc::new(crabka_gres_substrate::SubstrateLinearizer::new(Arc::clone(&handle.fenced))),
)
.map_err(|e| std::io::Error::other(format!("engine: {e:?}")))?;
engine.reseed_counters();
```

(Then the existing FDW wiring + `Arc::new(engine)` + `serve_tls` path continues unchanged. Adapt import paths and the exact `FjallKv::open`/`MemKv` constructors to `crabka-pgkv`'s exports; `reseed_counters`'s exact receiver/return per the vendored `lib.rs:173`.)

- [ ] **Step 3: Manual smoke** (requires a running broker; use the quick-start from the root README to format + start a standalone broker, then):

```bash
cargo run -p crabka-gres -- --listen 127.0.0.1:54398 --substrate-bootstrap 127.0.0.1:9092 --tenant smoke &
psql "host=127.0.0.1 port=54398 user=crab dbname=crab" -c "CREATE TABLE t (id int4); INSERT INTO t VALUES (1); SELECT * FROM t;"
```

Expected: `1`. Kill and restart the same command; `SELECT * FROM t` still returns `1` with no `--cache-dir` (proof the topic, not the store, is the truth).

- [ ] **Step 4: Update `crates/gres/README.md`** — extend the Overview + Quick Start with the substrate mode flags and the disposability property (one short paragraph + one command block mirroring Step 3).

- [ ] **Step 5: Lint, format, commit**

```bash
cargo clippy -p crabka-gres --all-targets -- -D warnings
cargo +nightly fmt -p crabka-gres
git add crates/gres
git commit -m "feat(gres): --substrate mode wires the engine to the tenant WAL topic

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Batch 4 — proof (serial)

### Task 5: Disposability + fencing integration suite

**Files:**
- Create: `crates/gres-substrate/tests/harness/mod.rs` (single-node in-process broker helper — lift the minimal broker-start portion of `crates/gres-fdw/tests/harness/mod.rs`), `crates/gres-substrate/tests/disposability.rs`, `crates/gres-substrate/tests/fencing.rs`
- Modify: `.config/nextest.toml` (test group capping `crabka-gres-substrate` integration tests, mirroring the gres-fdw group)

**Interfaces:**
- Consumes: everything from Tasks 1–4; `crabka-broker`'s `Broker::start(BrokerConfig::for_tests(..))` in-process harness idiom.
- Produces: the G-2 gate evidence.

- [ ] **Step 1: Harness.** `tests/harness/mod.rs`: start one in-process broker on an ephemeral port (copy the broker-start + `create_topic` helpers from the gres-fdw harness; no schema registry needed), plus a helper `substrate_engine(tenant, store) -> (SqlEngine, WalHandle)` that runs `recover` + `spawn_wal_writer` + `SqlEngine::replicated` + `reseed_counters` exactly as the binary does (shared shape keeps the test honest).

- [ ] **Step 2: Disposability test** — `tests/disposability.rs`:

```rust
//! Kill a compute at any instant; a successor reproduces exactly the acked state.

#[tokio::test(flavor = "multi_thread")]
async fn acked_state_survives_compute_loss_and_unacked_state_does_not() {
    // 1. Boot broker; engine A for tenant "t"; drive via pgwire+tokio_postgres:
    //    - committed txn:   CREATE TABLE t1(id int4); INSERT 1..=10; COMMIT
    //    - in-flight txn:   BEGIN; INSERT INTO t1 VALUES (99);   -- never committed
    // 2. Drop engine A (no shutdown; simulates kill -9 of the compute).
    // 3. Fresh store; engine B recovers the same tenant.
    // 4. assert!(SELECT count(*) FROM t1 == 10)  — acked rows all present
    //    assert!(no row 99)                       — unacked txn vanished
    //    fresh INSERT works and commits (counters reseeded, no xid/rowid reuse errors)
}
```

Write it fully (the executor's own `tests/transactions.rs`/`durability.rs` show the pgwire+tokio-postgres driving idiom to copy); every wait is a bounded condition loop (connect-retry), never a settle-sleep.

- [ ] **Step 3: Fencing test** — `tests/fencing.rs`:

```rust
//! A stale compute cannot commit after a successor starts, and the journal
//! contains no stale-generation record after the fence point.

#[tokio::test(flavor = "multi_thread")]
async fn stale_compute_is_fenced_and_journal_has_no_interleaving() {
    // 1. Engine A serving tenant "t"; commit one row (journal_seq 0..).
    // 2. Engine B recovers the same tenant (fences A) and commits one row.
    // 3. A attempts another commit: assert the SQL statement/COMMIT errors
    //    (ExecError::NotLeader surfaces as a pgwire error), and A's fenced flag is set.
    // 4. Read the whole topic READ_UNCOMMITTED via fetch_partition and decode frames:
    //    assert every committed frame after B's first frame has B's producer epoch
    //    (no A-record post-fence), and journal_seq is continuous per generation.
    // 5. Engine C recovers: sees A's acked row + B's acked row, nothing else.
}
```

Write it fully using the harness. Additionally: (a) an **oversized-batch case** in the disposability suite — one statement writing a row large enough to force chunking (> `MAX_RECORD_BYTES`); kill; recover; the row survives intact (pins chunk-group atomicity through replay); (b) a **coordinator≠leader fencing variant** — the single-broker harness co-locates the transaction coordinator with the partition leader, which is exactly the configuration where the produce-path epoch check fires; the corrected G-2 spec notes fencing falls back to `EndTxn` when they differ, so add a multi-broker variant using the in-process multi-node fixture from `crates/integration-tests` (if standing that fixture up here is disproportionate, land the single-broker suite and file the multi-broker variant as an explicit TODO test referencing the spec's fencing-locality paragraph — do not silently skip it).

- [ ] **Step 4: nextest group.** In `.config/nextest.toml` `[test-groups]` add `gres-substrate = { max-threads = 2 }` (broker-heavy, same rationale as `gres-fdw`) plus the matching `[[profile.default.overrides]]` block with `filter = 'package(crabka-gres-substrate) & kind(test)'`.

- [ ] **Step 5: Run everything**

```bash
cargo nextest run -p crabka-gres-substrate
cargo clippy -p crabka-gres-substrate --all-targets -- -D warnings
cargo +nightly fmt -p crabka-gres-substrate
```

Expected: unit + both integration suites green, no `#[ignore]`, no sleeps-as-waits.

- [ ] **Step 6: Commit**

```bash
git add crates/gres-substrate .config/nextest.toml
git commit -m "test(gres): disposability and fencing gates for the substrate WAL

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Batch 5 — CI (serial)

### Task 6: CI legs — substrate conformance + integration coverage

**Files:**
- Modify: `.github/workflows/ci.yml` (extend `gres-conformance` with a substrate leg; add `crabka-gres-substrate` to `gres-integration`'s package list; extend the `changes.gres` filter with `crates/gres-substrate/**`)

**Interfaces:**
- Consumes: the G-1 CI jobs; the parity baseline at `crates/gres-conformance/baseline.json` (unchanged — the substrate must match it exactly).
- Produces: the CI-enforced G-2 gate.

- [ ] **Step 1: Filter + integration job.** Add `- "crates/gres-substrate/**"` to the `gres` filter list; add `-p crabka-gres-substrate` to the `gres-integration` job's `cargo llvm-cov nextest` package list.

- [ ] **Step 2: Substrate conformance leg.** Append to the `gres-conformance` job, after the existing baseline harness step (a second subject; same oracle service is NOT reusable across corpus runs — start a second oracle container or, simpler, a second database on the same service: the corpus creates tables, so use `dbname=postgres2` after `createdb`):

```yaml
      - name: Create a fresh oracle database for the substrate leg
        run: psql "host=127.0.0.1 port=54320 user=postgres dbname=postgres" -c "CREATE DATABASE oracle2"
      - name: Start a standalone broker
        run: |
          cargo build --locked -p crabka-cli -p crabka-broker
          export CRABKA_CLUSTER_ID=00000000-0000-0000-0000-000000000001
          ./target/debug/crabka format --log-dir /tmp/gres-ci-data --cluster-id "$CRABKA_CLUSTER_ID" --standalone --node-id 1 --controller-listener 127.0.0.1:9093
          ./target/debug/crabka-broker --log-dir /tmp/gres-ci-data --cluster-id "$CRABKA_CLUSTER_ID" --broker-id 1 --listen-addr 127.0.0.1:9092 &
      - name: Conformance against the substrate-backed engine
        run: |
          ./target/debug/crabka-gres --listen 127.0.0.1:54334 --substrate-bootstrap 127.0.0.1:9092 --tenant conformance &
          for _ in $(seq 60); do
            if psql "host=127.0.0.1 port=54334 user=crab dbname=crab sslmode=prefer" -tAc 'SELECT 1' >/dev/null 2>&1; then break; fi
            sleep 0.5
          done
          ./target/debug/crabka-gres-conformance \
            --oracle-url "host=127.0.0.1 port=54320 user=postgres dbname=oracle2" \
            --subject-url "host=127.0.0.1 port=54334 user=crab dbname=crab" \
            --corpus crates/gres-conformance/corpus \
            --baseline crates/gres-conformance/baseline.json \
            --out parity-substrate.json --summary parity-substrate.md
      - name: Publish substrate parity summary
        if: ${{ !cancelled() }}
        run: cat parity-substrate.md >> "$GITHUB_STEP_SUMMARY"
```

Add `parity-substrate.json` / `parity-substrate.md` to the artifact upload paths. (Verify the format/broker CLI flags against the root README quick-start at execution time; readiness waits are bounded condition loops.)

- [ ] **Step 3: Validate + commit**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml')); print('yaml ok')"
git add .github/workflows/ci.yml
git commit -m "ci: gres substrate conformance leg and coverage for gres-substrate

The conformance corpus now also runs against crabka-gres --substrate on a
standalone broker and must match the same parity baseline — the G-2 gate that
the durability seam changes nothing observable.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Completion checklist (maps to the G-2 gate)

- Deterministic kill/respawn with zero acked-transaction loss and no unacked resurrection (Task 5).
- A fenced stale compute provably cannot commit; the journal shows no post-fence interleaving (Task 5).
- Conformance parity on `--substrate` equals the recorded baseline in CI (Task 6).
- FDW DDL is seam-durable (Task 2 + replayed in Task 5's suites).
- GRW1 proptest round-trips; merge-rule apply pinned by donor-ported cases (Task 1).
- Deferred by design: checkpoints/truncation (G-3), the Stateright fence/replay/checkpoint model (G-3 gate), async statement acks (evidence-gated optimization), diskless-tier WAL topics (cross-track dependency).
