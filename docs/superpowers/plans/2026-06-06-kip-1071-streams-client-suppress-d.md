# Suppress Slice D — fault tolerance (JVM-exact changelog + restore) + `maxBytes` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the suppress buffer durable — a registered state store that writes a **JVM-byte-exact** changelog and restores from it — and add `maxBytes`. The final suppress slice.

**Architecture:** The processor's owned buffer becomes a registered byte-oriented `SuppressBytesStore`; the existing `StreamTask::restore` handles restore for free. The changelog value matches the JVM `InMemoryTimeOrderedKeyValueChangeBuffer` (`BufferValue` + `ProcessorRecordContext`), pinned by an empirical byte-vector capture. Serdes reach the store via a store-factory thunk on the KTable handle.

**Tech Stack:** Rust, `async-trait`, `bytes`; reuses #3 `StateStore`/changelog/restore, 4d store patterns, slice A–C suppress.

**Branch / worktree:** `streams-suppress-d` (stacked on `streams-suppress-c` / PR #409) in `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`. Spec: `docs/superpowers/specs/2026-06-06-kip-1071-streams-client-suppress-d-design.md`.

**Git discipline:** all git via `git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl …`; assert branch `== streams-suppress-d` before each commit; commit `-c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`; no push.

---

## File Structure

**New files:**
- `src/store/suppress_bufval.rs` — JVM-exact `BufferValue`/`ProcessorRecordContext` changelog codec.
- `src/store/suppress_store.rs` — `SuppressStore` trait + `SuppressBytesStore` (registered, byte-oriented).
- `tests/jvm-capture/src/main/java/crabka/capture/BufferValueCapture.java` — byte-vector capture.
- `tests/testdata/suppress_bufval/*.hex` — captured `BufferValue` byte vectors.
- `tests/testdata/golden/dsl/suppress_until_window_closes_logged.topology.json` — golden #14.

**Modified files:**
- `src/store/mod.rs`, `src/store/registry.rs` (`get_suppress`), `src/processor/api.rs` (`get_suppress_store`).
- `src/dsl/processors/suppress.rs` — owned buffer → store access via `ctx`; `maxBytes`.
- `src/dsl/suppress.rs` — `BufferConfig::max_bytes`/`with_max_bytes`; `Suppressed.logging` + toggles.
- `src/dsl/ktable.rs` — `suppress_store_factory` field + thunk wiring + `suppress` registers the store / panics.
- `src/dsl/windowed_kgrouped.rs`, `src/dsl/session_windowed_kgrouped.rs`, `src/dsl/builder.rs` — set the serde thunk.
- `src/topology/{node,wire,builder}.rs` — `ChangelogKind::Suppress` + `add_suppress_store`.
- `tests/jvm-capture/.../Capture.java` + `run.sh` — fixture #14.
- `tests/dsl_golden_frame.rs`, `tests/dsl_execution.rs`, `src/lib.rs`.

## Execution batches (mostly sequential; T1 is independent)

- **T1:** `BufferValue` codec + byte-vector capture (capture-FIRST). Independent.
- **T2:** `SuppressBytesStore` + registry/accessor — needs T1.
- **T3:** processor refactor (owned buffer → `ctx` store) + `maxBytes` branch + test migration — needs T2.
- **T4:** wire (`add_suppress_store` + `ChangelogKind::Suppress`) + `Suppressed.logging`/`BufferConfig::max_bytes` + serde-thunk on the KTable handle + set in aggregations/`builder.table` + `suppress` registers/panics — needs T3.
- **T5 (Phase C):** Capture.java #14 + Docker capture (controller) + golden #14 + restart-restore + maxBytes execution tests + docs + final verify — needs T4.

---

## Task 1: JVM-exact `BufferValue` codec + byte-vector capture (capture FIRST)

**Files:**
- Create: `src/store/suppress_bufval.rs`, `tests/jvm-capture/src/main/java/crabka/capture/BufferValueCapture.java`, `tests/testdata/suppress_bufval/*.hex`
- Modify: `src/store/mod.rs`

> **Capture-first**: the exact byte layout (sentinels, field order) is JVM ground truth — write the Java capture, run it, read the bytes, THEN write the Rust codec to match. Do NOT assume the spec's byte layout is exact until the capture confirms it.

- [ ] **Step 1: Write the Java capture.** Create `BufferValueCapture.java` (a standalone `main`): for each case, construct a Kafka `org.apache.kafka.streams.state.internals.BufferValue` (and `org.apache.kafka.streams.processor.internals.ProcessorRecordContext`) with KNOWN inputs, call its `serialize(int)` (reflection if needed — these are internal classes), and write the resulting bytes as hex to `tests/testdata/suppress_bufval/<case>.hex`. Cases (each records the exact inputs in a comment):
  - `wc_first.hex`: context `(topic="in", partition=0, offset=0, timestamp=10, no headers)`, `prior=null`, `old=null`, `new=<i64 count 1 BE serialized>`, `bufferTime=10`.
  - `wc_update.hex`: same context (timestamp=12), `prior=<count 1>`, `old=<count 1>`, `new=<count 2>`, `bufferTime=12`. (Exercises the `old == prior` `-2` sentinel.)
  - `tombstone.hex`: `new=null` (a deletion), `bufferTime=20`.
  Include a `BufferValueCapture` run target in `run.sh` (a new mode, e.g. `./run.sh --bufval`, that compiles + runs this class against the kafka-streams jar, no broker).

- [ ] **Step 2: Run the capture (CONTROLLER).** `cd tests/jvm-capture && ./run.sh --bufval` → writes the `.hex` fixtures. Read them; record the observed field order + sentinel values.

- [ ] **Step 3: Register the module + write the codec** to match the fixtures. In `src/store/mod.rs` add `pub(crate) mod suppress_bufval;`. Create `src/store/suppress_bufval.rs`:

```rust
//! JVM-exact `InMemoryTimeOrderedKeyValueChangeBuffer` changelog codec — matches
//! `BufferValue.serialize()` + `ProcessorRecordContext.serialize()` byte-for-byte
//! (pinned by the captured byte vectors in tests/testdata/suppress_bufval).
//! Changelog KEY = the record key bytes; VALUE = BufferValue ‖ bufferTime:8BE.
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// The buffered record's context (the `ProcessorRecordContext`). Crabka streams
/// records are header-less → `header_count = 0`.
#[derive(Clone, Debug)]
pub(crate) struct SuppressRecordCtx {
    pub timestamp: i64,
    pub offset: i64,
    pub partition: i32,
    pub topic: String,
}

const NULL: i32 = -1;
const OLD_EQ_PRIOR: i32 = -2;

fn put_ctx(b: &mut BytesMut, c: &SuppressRecordCtx) {
    b.put_i64(c.timestamp);
    b.put_i64(c.offset);
    b.put_i32(c.partition);
    let t = c.topic.as_bytes();
    b.put_i32(i32::try_from(t.len()).expect("topic len"));
    b.extend_from_slice(t);
    b.put_i32(0); // header count (Crabka streams records are header-less)
}

fn put_value(b: &mut BytesMut, v: Option<&[u8]>) {
    match v {
        None => b.put_i32(NULL),
        Some(bytes) => {
            b.put_i32(i32::try_from(bytes.len()).expect("value len"));
            b.extend_from_slice(bytes);
        }
    }
}

/// `BufferValue.serialize() ‖ bufferTime:8BE`. (TUNE the exact field order + the
/// old/prior sentinel logic to the captured fixtures.)
pub(crate) fn serialize_buffer_change(
    ctx: &SuppressRecordCtx,
    prior: Option<&[u8]>,
    old: Option<&[u8]>,
    new: Option<&[u8]>,
    buffer_time: i64,
) -> Bytes {
    let mut b = BytesMut::new();
    put_ctx(&mut b, ctx);
    put_value(&mut b, prior);
    // old: -1 null, -2 "old == prior", else len+bytes
    match old {
        None => b.put_i32(NULL),
        Some(o) if prior == Some(o) => b.put_i32(OLD_EQ_PRIOR),
        Some(o) => {
            b.put_i32(i32::try_from(o.len()).expect("old len"));
            b.extend_from_slice(o);
        }
    }
    put_value(&mut b, new);
    b.put_i64(buffer_time);
    b.freeze()
}

/// Restore needs `new` + `buffer_time` (and the ctx). Returns `(ctx, old, new,
/// buffer_time)`; `prior` is parsed but discarded.
pub(crate) fn deserialize_buffer_change(
    mut data: &[u8],
) -> (SuppressRecordCtx, Option<Bytes>, Option<Bytes>, i64) {
    let timestamp = data.get_i64();
    let offset = data.get_i64();
    let partition = data.get_i32();
    let tlen = data.get_i32();
    let topic = if tlen < 0 { String::new() } else {
        let s = String::from_utf8(data[..tlen as usize].to_vec()).expect("topic utf8");
        data.advance(tlen as usize);
        s
    };
    let hcount = data.get_i32();
    for _ in 0..hcount { /* skip headers — none expected */ }
    let ctx = SuppressRecordCtx { timestamp, offset, partition, topic };
    let read_opt = |d: &mut &[u8]| -> Option<Bytes> {
        let n = d.get_i32();
        if n < 0 { None } else { let v = Bytes::copy_from_slice(&d[..n as usize]); d.advance(n as usize); Some(v) }
    };
    let prior = read_opt(&mut data);
    // old: -1 null, -2 old==prior, else bytes
    let old = {
        let n = data.get_i32();
        match n {
            NULL => None,
            OLD_EQ_PRIOR => prior.clone(),
            n => { let v = Bytes::copy_from_slice(&data[..n as usize]); data.advance(n as usize); Some(v) }
        }
    };
    let new = read_opt(&mut data);
    let buffer_time = data.get_i64();
    (ctx, old, new, buffer_time)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_fixture(name: &str) -> Vec<u8> {
        let path = format!("tests/testdata/suppress_bufval/{name}.hex");
        let s = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        // hex with optional whitespace/newlines
        let clean: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
        (0..clean.len()).step_by(2).map(|i| u8::from_str_radix(&clean[i..i + 2], 16).unwrap()).collect()
    }

    #[test]
    fn matches_jvm_first_value() {
        let ctx = SuppressRecordCtx { timestamp: 10, offset: 0, partition: 0, topic: "in".into() };
        let new = 1i64.to_be_bytes(); // MATCH the capture's value serialization
        let got = serialize_buffer_change(&ctx, None, None, Some(&new), 10);
        assert_eq!(got.as_ref(), hex_fixture("wc_first").as_slice());
    }
    // + matches_jvm_update (old==prior sentinel), matches_jvm_tombstone, + a round-trip
    // (serialize → deserialize → new/buffer_time recovered).
}
```

> **Implementer note:** the codec above is the spec's best-guess layout. **The fixtures are ground truth** — if `matches_jvm_*` fails, adjust the field order / sentinel logic in `serialize_buffer_change`/`put_ctx` to match the captured bytes exactly, and document any deviation from the spec. The value serialization of the `new`/`old`/`prior` (e.g. how the JVM serializes the count `1`) must match what the Java capture used (use the same serde — `Serdes.Long().serializer().serialize(...)` for an `i64`, which is 8-byte BE).

- [ ] **Step 4: Run + verify.** `cargo test -p crabka-client-streams --lib suppress_bufval` → byte-vector tests + round-trip PASS. `cargo clippy -p crabka-client-streams --lib -- -D warnings` + `cargo fmt`.

- [ ] **Step 5: Commit.**

```bash
git -C <worktree> add crates/client-streams/src/store/suppress_bufval.rs crates/client-streams/src/store/mod.rs crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/BufferValueCapture.java crates/client-streams/tests/jvm-capture/run.sh crates/client-streams/tests/testdata/suppress_bufval/
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-store): JVM-exact suppress BufferValue changelog codec + byte-vector capture"
```

---

## Task 2: `SuppressBytesStore`

**Files:**
- Create: `src/store/suppress_store.rs`
- Modify: `src/store/mod.rs`, `src/store/registry.rs`, `src/processor/api.rs`

Mirror `src/store/window.rs` (`WindowStore<K,V>` trait + `WindowBytesStore<K,V>` **typed wrapper** holding `key_serde`/`value_serde` over a byte map — the `StateStore` impl, `in_memory` ctor, `take_changelog`/`apply_changelog`, registry `get_window<K,V>`, `get_window_store::<K,V>` accessor). The suppress store is the **typed** analogue (holds serdes so the processor accesses it typed, no serdes in the processor), and its changelog uses T1's `BufferValue` codec.

- [ ] **Step 1: `SuppressStore<K,V>` trait + `SuppressBytesStore<K,V>` (TYPED)** (`src/store/suppress_store.rs`):
  - `#[async_trait] trait SuppressStore<K: Send+Sync, V: Send>: StateStore`: `async fn put(&mut self, key: K, buffer_time: i64, change: Change<V>, ctx: SuppressRecordCtx)`, `async fn evict_while(&mut self, threshold: i64) -> Vec<(K, Change<V>, i64)>`, `async fn evict_oldest(&mut self) -> Option<(K, Change<V>, i64)>`, `fn len(&self) -> usize`, `fn byte_size(&self) -> usize`.
  - `SuppressBytesStore<K, V>`: holds `name`, `changelog_topic`, `logging: bool`, `key_serde: Box<dyn Serde<K>>`, `value_serde: Box<dyn Serde<V>>`, and a byte buffer (`entries: BTreeMap<(i64,u64), Entry>` where `Entry { key_bytes: Bytes, new_bytes: Option<Bytes>, old_bytes: Option<Bytes>, prior_bytes: Option<Bytes>, record_ts: i64, ctx: SuppressRecordCtx }`, `index: HashMap<Bytes,(i64,u64)>`, `seq: u64`, `byte_size: usize`) + `changelog: Vec<(Bytes, Option<Bytes>)>`. (Mirror `WindowBytesStore<K,V>`'s typed-wrapper shape.)
  - `put(key, buffer_time, change, ctx)`: `kb = key_serde.serialize(&key)`; `new_bytes = change.new.map(|v| value_serde.serialize(&v))`; `old_bytes = change.old.map(..)`; `prior_bytes` = the existing entry's `new_bytes` (the value previously buffered for this key) or `None`. Replace-by-key (drop old slot; `byte_size -= kb.len + old_new_len`); insert; `byte_size += kb.len + new_bytes.len`. If `logging`, push `(kb, Some(serialize_buffer_change(&ctx, prior_bytes, old_bytes, new_bytes, buffer_time)))`.
  - `evict_while`/`evict_oldest`: pop front while `buffer_time <= threshold` (one for oldest); `byte_size -=`; if `logging`, push `(kb, None)` tombstone; deserialize `key_bytes`→K + `new_bytes`/`old_bytes`→`Change<V>`; return `(K, Change<V>, record_ts)`.
  - `StateStore`: `take_changelog` drains; `apply_changelog(key, value)`: `Some(v)` → `deserialize_buffer_change(&v)` → re-insert (regenerate `seq`; store `new`/`old`/ctx/buffer_time); `None` → remove by key. `set_logging` toggles the flag. `in_memory(name, key_serde, value_serde, changelog_topic)` ctor + `new(...)` like `WindowBytesStore`.
  - Unit tests (typed, like `WindowBytesStore`'s): put/evict_while/evict_oldest/len/byte_size; take_changelog (put→Some, evict→None); apply_changelog rebuild round-trip.

- [ ] **Step 2: registry + accessor (TYPED, mirroring `get_window`).** `StoreRegistry::get_suppress<K: Send+Sync+'static, V: Send+'static>(name) -> Option<&mut dyn SuppressStore<K,V>>` (downcast to `SuppressBytesStore<K,V>`). `ProcessorContext::get_suppress_store::<K2,V2>(name) -> Option<&mut dyn SuppressStore<K2,V2>>` (mirror `get_window_store::<K2,V2>`). Register `pub mod suppress_store;` in `store/mod.rs`.

- [ ] **Step 3: test + commit.** `cargo test -p crabka-client-streams suppress_store` + clippy/fmt. Commit `feat(streams-store): SuppressBytesStore (byte-oriented, time-ordered, changelog/restore)`.

---

## Task 3: processor refactor (owned buffer → registered store) + `maxBytes`

**Files:**
- Modify: `src/dsl/processors/suppress.rs`

- [ ] **Step 1: Refactor the struct/process** to use the typed registered store (T2's `SuppressBytesStore<K,V>`, accessed via `ctx.get_suppress_store::<K,V>(name)` — no serdes in the processor, exactly like `window_aggregate.rs` uses `get_window_store::<K,V>`). Drop the owned `buffer` field; add `store_name: String` + `max_bytes: Option<usize>`. `new(store_name, wait_ms, buffer_time, max_records, max_bytes, emit_early)`. `process`:
  - `let key = r.key.expect(..);` compute `bt = (buffer_time)(&key, ts)`; advance `observed_stream_time`; build `SuppressRecordCtx` from `ctx.record_context()` (topic/partition/offset/timestamp).
  - `{ let store = ctx.get_suppress_store::<K, V>(&self.store_name).expect(..); store.put(key, bt, r.value /*Change<V>*/, rec_ctx).await; }` (scoped borrow; the store serializes internally).
  - `{ let store = ...; store.evict_while(observed_stream_time - wait_ms).await }` → for each `(k, change, rts)` `ctx.forward(Record::new(Some(k), change, rts))` (store borrow dropped before forward).
  - Overflow: read `len()`/`byte_size()` from the store; if `max_records` or `max_bytes` exceeded → `emit_early` ⇒ `while over { let (k,ch,rts) = store.evict_oldest().await...; forward }`, else ⇒ `assert!(within caps, "...shutDownWhenFull")`. Each store access in its own scoped borrow; `forward` outside the borrow.
- [ ] **Step 2: Migrate the processor tests** — register a `SuppressBytesStore<Windowed<String>, i64>` (window-close) / `<String, i64>` (time-limit) in the test `StoreRegistry` (with `StringSerde`-style serdes + a `Windowed` serde), and have the processor reference it by name. The `window_close_proc`/time-limit constructions take a `store_name`; the test seeds the store. (Mirror `window_aggregate.rs` tests, which seed a `WindowBytesStore`.)
- [ ] **Step 3: test + commit.** `cargo test -p crabka-client-streams suppress` (lib) + clippy/fmt. Commit `refactor(streams-dsl): suppress processor uses registered SuppressBytesStore + maxBytes`.

---

## Task 4: wire (`add_suppress_store` + `ChangelogKind::Suppress`) + `Suppressed.logging` + `maxBytes` + serde thunk

**Files:**
- Modify: `src/topology/{node,wire,builder}.rs`, `src/dsl/suppress.rs`, `src/dsl/ktable.rs`, `src/dsl/windowed_kgrouped.rs`, `src/dsl/session_windowed_kgrouped.rs`, `src/dsl/builder.rs`

- [ ] **Step 1: `ChangelogKind::Suppress` + `add_suppress_store`.** In `node.rs` add `Suppress { retention_ms: i64 }` (config captured in T5 — initially mirror the windowed/compact config; tune to golden #14) + `add_suppress_store` on `NodeRegistry`. In `wire.rs` `state_changelog_topics` add the `Suppress` arm (`suppress_changelog_topic_configs`, tuned to the capture). In `builder.rs` `Topology::add_suppress_store::<K,V,KS,VS>(name, ks, vs, logging, processors)` — builds the `SuppressBytesStore<K,V>` factory; registers the NodeRegistry changelog store entry **only when `logging`** (so a logging-off store adds no wire topic). Mirror `add_window_store`.
- [ ] **Step 2: `BufferConfig::max_bytes` + `Suppressed.logging`** (`src/dsl/suppress.rs`): add `max_bytes: Option<usize>` to `BufferConfig` + `max_bytes(n)` (eager static) / `with_max_bytes(n)` + `byte_cap()` getter. Add `logging: bool` to `Suppressed<K>` (default `true` in both constructors) + `with_logging_disabled()`/`with_logging_enabled()`.
- [ ] **Step 3: serde-thunk on the KTable handle** (`src/dsl/ktable.rs`): add `suppress_store_factory: Option<SuppressStoreFactory>` (type alias `Box<dyn Fn(&str, &mut LowerState, /*processor*/&str)>` — match the lowering thunk's signature) + a `with_suppress_factory` setter + getter. `suppress` (in the lowering thunk): if `suppressed.logging`, take the factory; if `None` → panic with the spec's message; else call it (registers the store with the real serdes) — passing the store name. If `!logging`, register an in-memory suppress store (logging off) via the factory too (the factory registers the store; `add_suppress_store`'s `logging` param controls the changelog). Pass `max_bytes`/`max_records`/`emit_early`/`buffer_time`/`wait_ms` to the processor.
- [ ] **Step 4: set the thunk in producers.** In `windowed_kgrouped.rs`/`session_windowed_kgrouped.rs`, when building the result `KTable<Windowed<K>, VA>`, attach a `suppress_store_factory` that captures `TimeWindowedSerde::new(ks, size)` / `SessionWindowedSerde::new(ks)` + `vs` and calls `state.topology.add_suppress_store::<Windowed<K>, VA, _, _>(name, win_serde, vs, logging, [proc])`. In `builder.rs` `table(...)`, attach one capturing `ks`/`vs`. In `ktable.rs` `filter` (keeps V): propagate the parent's factory; `map_values` (changes V): leave it `None`.
- [ ] **Step 5: build + test.** `cargo build` + `cargo test -p crabka-client-streams --lib` + `cargo test --test dsl_golden_frame` (**13 passed**, byte-identical — logging-off path adds no topic) + `cargo test --test dsl_execution suppress` (existing suppress exec tests pass against the registered store) + clippy/fmt. Commit `feat(streams-dsl): suppress changelog wire + logging toggle + maxBytes + serde thunk`.

---

## Task 5: golden #14 + restart-restore + maxBytes execution tests + docs (Phase C — controller runs Docker)

**Files:**
- Modify: `Capture.java`, `run.sh`, `tests/dsl_golden_frame.rs`, `tests/dsl_execution.rs`, `src/lib.rs`
- Create: `tests/testdata/golden/dsl/suppress_until_window_closes_logged.topology.json`

- [ ] **Step 1: Capture.java #14** `suppressUntilWindowClosesLogged()` = the #13 app WITHOUT `.withLoggingDisabled()`. Register it (#14), bump run.sh/comment counts to 14.
- [ ] **Step 2: Docker capture (CONTROLLER).** `./run.sh --gradle` → `suppress_until_window_closes_logged.topology.json`. Confirm the suppress changelog topic name + config; tune `ChangelogKind::Suppress`'s `suppress_changelog_topic_configs` + the `add_suppress_store` naming to byte-match. Only the new fixture changes; the 13 prior stay byte-identical.
- [ ] **Step 3: golden test** `suppress_until_window_closes_logged_matches_jvm` (logging ON: `…suppress(Suppressed::until_window_closes(BufferConfig::unbounded()))` — default logging on). **Update the slice-A `suppress_until_window_closes_matches_jvm` test to `.with_logging_disabled()`** so fixture #13 stays byte-identical (it now matters: default logging is on).
- [ ] **Step 4: execution tests** (`dsl_execution.rs`): (a) **restart-restore** — build a windowed-count+suppress topology, pipe records to buffer windows, `take_changelog` from the suppress store + restore into a fresh driver/store, assert the buffered windows still emit on close (mirror `task.rs::stateful_task_produces_changelog_and_restores` adapted to the suppress store); (b) **maxBytes** — `until_window_closes(unbounded().with_max_bytes(small))` overflow panics; `until_time_limit(.., max_bytes(small) eager)` emit-early.
- [ ] **Step 5: docs + final verify.** lib.rs prose (logging/changelog/restore + maxBytes). Run `cargo test -p crabka-client-streams` + `--doc` + `cargo clippy --all-targets -D warnings` + `cargo fmt --check`. Expected: all green; `dsl_golden_frame` **14 passed** (13 prior byte-identical + logged). Commit `test(streams-dsl): suppress logged golden (#14) + restart-restore + maxBytes + docs`.

---

## Done criteria

- Suppress buffer is a registered `SuppressBytesStore`; with logging on it writes a **JVM-byte-exact** changelog (validated by the byte-vector capture) and restores via `StreamTask::restore`; logging off → no changelog topic (slice-A golden #13 byte-identical).
- `BufferConfig::max_bytes`/`with_max_bytes` enforced (shutDown / emitEarly).
- golden #14 (logging on) byte-matches JVM 4.1; **13 prior goldens byte-identical**; restart-restore + maxBytes execution tests pass.
- Full suite + doctests + clippy `--all-targets` + fmt green. **Completes the suppress program + the windowing arc.**
