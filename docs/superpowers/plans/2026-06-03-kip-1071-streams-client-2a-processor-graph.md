# KIP-1071 Streams Client #2a — Processor API + Erased Graph + TopologyTestDriver — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the merged `crabka-client-streams` topology builder a typed `Processor<KIn,VIn,KOut,VOut>` API backed by a `dyn Any`-erased runtime graph (driver-loop forwarding), with build-time wiring-type diagnostics, and a synchronous broker-free `TopologyTestDriver` to test topologies end-to-end.

**Architecture:** Records flow through the graph erased as `Box<dyn Any + Send>`; typed `Processor` adapters downcast on input and box on `forward`; serdes (de)serialize only at source/sink boundaries. Forwarding is non-recursive: `ProcessorContext::forward` appends `(child_idx, ErasedRecord)` to a buffer that a driver loop drains, so no `&mut` aliasing across nodes. `build()` validates every parent→child edge by `TypeId` and emits an `rustc`-style diagnostic on mismatch.

**Tech Stack:** Rust 2024, `std::any` (TypeId/Any/type_name), `bytes`, `thiserror`. No new dependencies. Builds on the merged #1 crate (`topology/{builder,node,grouping,wire}.rs`, `membership/`).

**Spec:** `docs/superpowers/specs/2026-06-03-kip-1071-streams-client-2-execution-design.md` (§3, §4, §6). This plan is **Phase 2a**; the broker-backed runtime (§5) is Phase 2b, a separate follow-on plan.

**Branch:** `claude/streams-2-execution` (off latest `main`, which has #373 + #377).

---

## Scope of 2a (and what's deferred to 2b)

**In 2a:** `Serde`, `Record`, the erased graph + driver loop, `Processor`/`ProcessorContext`/`ProcessorSupplier` traits, the three erased node adapters, build-time `TypeId` validation + diagnostics, the typed builder evolution, and the `TopologyTestDriver`. All deterministic, no network.

**Deferred to 2b:** `StreamTask`/`StreamThread`/`KafkaStreams`, fetch/produce/commit, rebalance. (No `runtime/` module in 2a.)

## File structure (2a)

```
crates/client-streams/src/
  lib.rs                    MODIFY: add `pub mod processor;`, `mod test_driver;`, re-exports
  processor/
    mod.rs                  NEW: module glue + re-exports
    serde.rs                NEW: Serde<T> trait, SerdeError, BytesSerde/StringSerde/I64Serde
    record.rs               NEW: Record<K,V>, RecordContext
    erased.rs               NEW: ErasedRecord, OutputRecord, Dispatch, ProcessorError
    api.rs                  NEW: Processor, ProcessorSupplier, ProcessorContext<KOut,VOut>
    node.rs                 NEW: ErasedNode trait + Source/Processor/Sink adapters + factories (TypeId-carrying)
    graph.rs                NEW: Graph (nodes/children, driver loop, output collector), build from factories
  test_driver.rs            NEW: TopologyTestDriver, TestInputTopic<K,V>, TestOutputTopic<K,V>
  topology/
    builder.rs              MODIFY: typed add_source/add_processor/add_sink; store factories; TypeMismatch; build graph
    node.rs                 MODIFY: NodeKind unchanged structurally; builder holds a parallel factory map
  tests/integration.rs      MODIFY: migrate #1 membership test to typed add_source/add_sink (BytesSerde)
  tests/golden_frame.rs     MODIFY: migrate to typed builder calls (wire output unchanged)
```

Note: `topology/{grouping,wire}.rs` and `membership/*` are **unchanged**. The wire `Topology` is identical after migration, so `golden_frame` still passes.

## Reference signatures (verbatim — current crate)

- Current builder: `Topology { reg: NodeRegistry, error: Option<TopologyError> }`; `add_source<S,I,T>(name, topics)`, `add_processor<S,I,T>(name, predecessors)`, `add_sink<S,U,I,T>(name, topic, predecessors)`, `add_state_store`, `add_repartition_topic`, `build<S>(application_id) -> Result<BuiltTopology, TopologyError>`.
- `BuiltTopology { wire: WireTopology, source_topics: BTreeMap<String,Vec<String>>, application_id: String }` with `to_wire()`, `source_topics_for(id)`, `application_id()`.
- `NodeRegistry` (in `topology/node.rs`): `pub(crate)` with `add_source(&str, Vec<String>)`, `add_processor(&str, Vec<String>)`, `add_sink(&str, String, Vec<String>)`, `add_store(&str, Vec<String>)`, `repartition_topics: HashSet<String>`, `validate_predecessors()`, `nodes: Vec<Node>`, `index: HashMap<String,usize>`. `NodeKind::{Source{topics}, Processor{predecessors}, Sink{topic,predecessors}}`.

---

## Task 1: Serde trait + built-in serdes

**Files:**
- Create: `crates/client-streams/src/processor/serde.rs`
- Create: `crates/client-streams/src/processor/mod.rs`
- Modify: `crates/client-streams/src/lib.rs`

- [ ] **Step 1: Scaffold the module**

`crates/client-streams/src/processor/mod.rs`:
```rust
//! Typed Processor API + the `dyn Any`-erased execution graph (sub-project #2).

pub mod serde;

pub use serde::{BytesSerde, I64Serde, Serde, SerdeError, StringSerde};
```

In `crates/client-streams/src/lib.rs`, add after the existing `pub mod` lines:
```rust
pub mod processor;
```
and to the re-export block add:
```rust
pub use processor::{BytesSerde, I64Serde, Serde, SerdeError, StringSerde};
```

- [ ] **Step 2: Write the failing test**

Append to `serde.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn string_serde_round_trips() {
        let s = StringSerde;
        let b = s.serialize(&"héllo".to_string());
        check!(s.deserialize(&b).unwrap() == "héllo");
    }

    #[test]
    fn i64_serde_is_big_endian_8_bytes() {
        let s = I64Serde;
        let b = s.serialize(&1i64);
        check!(b.as_ref() == [0, 0, 0, 0, 0, 0, 0, 1]);
        check!(s.deserialize(&b).unwrap() == 1);
        check!(s.deserialize(&[0, 1]).is_err()); // wrong length
    }

    #[test]
    fn bytes_serde_is_identity() {
        let s = BytesSerde;
        let b = s.serialize(&bytes::Bytes::from_static(b"xy"));
        check!(s.deserialize(&b).unwrap() == bytes::Bytes::from_static(b"xy"));
    }
}
```

- [ ] **Step 3: Run → FAIL**

Run: `cargo test -p crabka-client-streams --lib processor::serde`
Expected: FAIL — types not defined.

- [ ] **Step 4: Implement**

`serde.rs` (above the test module):
```rust
//! `Serde<T>`: typed (de)serialization at source/sink boundaries.

use bytes::Bytes;

/// Failure to deserialize bytes into `T`.
#[derive(Debug, thiserror::Error)]
#[error("deserialization error: {0}")]
pub struct SerdeError(pub String);

/// Serialize a `T` to bytes and back. Used by source nodes (deserialize) and
/// sink/repartition nodes (serialize).
pub trait Serde<T>: Send + Sync + 'static {
    fn serialize(&self, value: &T) -> Bytes;
    fn deserialize(&self, bytes: &[u8]) -> Result<T, SerdeError>;
}

/// Identity serde for raw `Bytes`.
#[derive(Debug, Clone, Copy, Default)]
pub struct BytesSerde;
impl Serde<Bytes> for BytesSerde {
    fn serialize(&self, value: &Bytes) -> Bytes {
        value.clone()
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<Bytes, SerdeError> {
        Ok(Bytes::copy_from_slice(bytes))
    }
}

/// UTF-8 `String` serde.
#[derive(Debug, Clone, Copy, Default)]
pub struct StringSerde;
impl Serde<String> for StringSerde {
    fn serialize(&self, value: &String) -> Bytes {
        Bytes::copy_from_slice(value.as_bytes())
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<String, SerdeError> {
        String::from_utf8(bytes.to_vec()).map_err(|e| SerdeError(e.to_string()))
    }
}

/// Big-endian 8-byte `i64` serde (matches the JVM `Serdes.Long()`).
#[derive(Debug, Clone, Copy, Default)]
pub struct I64Serde;
impl Serde<i64> for I64Serde {
    fn serialize(&self, value: &i64) -> Bytes {
        Bytes::copy_from_slice(&value.to_be_bytes())
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<i64, SerdeError> {
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| SerdeError(format!("expected 8 bytes, got {}", bytes.len())))?;
        Ok(i64::from_be_bytes(arr))
    }
}
```

- [ ] **Step 5: Run → PASS + clippy + commit**

Run: `cargo test -p crabka-client-streams --lib processor::serde` → PASS (3).
Run: `cargo clippy -p crabka-client-streams --all-targets -- -D warnings` → clean.
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe add crates/client-streams/src/processor crates/client-streams/src/lib.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): Serde trait + built-in serdes"
```

---

## Task 2: Record + RecordContext

**Files:**
- Create: `crates/client-streams/src/processor/record.rs`
- Modify: `crates/client-streams/src/processor/mod.rs`

- [ ] **Step 1: Implement (no separate test — trivial value types, exercised by later tasks)**

`record.rs`:
```rust
//! `Record<K,V>` flowing through the processor graph + `RecordContext`.

/// A key/value record with a timestamp. `key` is optional (Kafka allows null
/// keys); `value` is required (tombstones — null values — are a stateful/#3
/// concern and out of scope here).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<K, V> {
    pub key: Option<K>,
    pub value: V,
    pub timestamp: i64,
}

impl<K, V> Record<K, V> {
    pub fn new(key: Option<K>, value: V, timestamp: i64) -> Self {
        Self { key, value, timestamp }
    }
}

/// Metadata about the source record currently being processed (JVM
/// `RecordContext`). Exposed via [`ProcessorContext::record_context`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordContext {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
}
```

Add to `processor/mod.rs`:
```rust
pub mod record;
pub use record::{Record, RecordContext};
```
and add to `lib.rs` re-exports: `pub use processor::{Record, RecordContext};`

- [ ] **Step 2: Verify build + clippy + commit**

Run: `cargo build -p crabka-client-streams` → compiles.
Run: `cargo clippy -p crabka-client-streams --all-targets -- -D warnings` → clean.
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe add crates/client-streams/src/processor/record.rs crates/client-streams/src/processor/mod.rs crates/client-streams/src/lib.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): Record + RecordContext types"
```

---

## Task 3: Erased record + dispatch + ProcessorError

**Files:**
- Create: `crates/client-streams/src/processor/erased.rs`
- Modify: `crates/client-streams/src/processor/mod.rs`

- [ ] **Step 1: Write the failing test**

Append to `erased.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn erase_then_downcast_roundtrips_value_and_key() {
        let er = ErasedRecord::new(Some(Box::new(7i32)), Box::new("v".to_string()), 1);
        let key = er.key.unwrap().downcast::<i32>().unwrap();
        let val = er.value.downcast::<String>().unwrap();
        check!(*key == 7);
        check!(*val == "v");
    }
}
```

- [ ] **Step 2: Run → FAIL** — `cargo test -p crabka-client-streams --lib processor::erased`

- [ ] **Step 3: Implement**

`erased.rs` (above tests):
```rust
//! Type-erased records + the per-dispatch context the graph driver hands to
//! each node. Records flow erased (`Box<dyn Any + Send>`) between nodes; only
//! source/sink boundaries (de)serialize.

use std::any::Any;
use std::collections::VecDeque;

use bytes::Bytes;

use super::record::RecordContext;

/// An error raised while a node processes a record (e.g. an internal downcast
/// mismatch — unreachable in practice because `build()` validates wiring).
#[derive(Debug, thiserror::Error)]
pub enum ProcessorError {
    #[error("type mismatch in node `{node}`: expected {expected}, found a different type")]
    Downcast { node: String, expected: &'static str },
    #[error("serialization error in sink `{node}`: {message}")]
    Serde { node: String, message: String },
}

/// A record with erased key/value, as it flows between nodes.
pub(crate) struct ErasedRecord {
    pub key: Option<Box<dyn Any + Send>>,
    pub value: Box<dyn Any + Send>,
    pub timestamp: i64,
}

impl ErasedRecord {
    pub fn new(key: Option<Box<dyn Any + Send>>, value: Box<dyn Any + Send>, timestamp: i64) -> Self {
        Self { key, value, timestamp }
    }
}

/// A record emitted by a sink node, collected by the driver (test driver: into
/// an output queue; runtime: into the producer).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutputRecord {
    pub topic: String,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub timestamp: i64,
}

/// What the driver lends to a node for the duration of one `process` call:
/// the forward buffer (for source/processor children), this node's child
/// indices, the sink output collector, and the source-record context.
pub(crate) struct Dispatch<'a> {
    pub buffer: &'a mut VecDeque<(usize, ErasedRecord)>,
    pub children: &'a [usize],
    pub output: &'a mut Vec<OutputRecord>,
    pub record_ctx: &'a RecordContext,
}
```

Add to `processor/mod.rs`:
```rust
pub mod erased;
pub use erased::ProcessorError;
```
and `lib.rs` re-export: `pub use processor::ProcessorError;`

- [ ] **Step 4: Run → PASS + clippy + commit**
Run: `cargo test -p crabka-client-streams --lib processor::erased` → PASS.
Run clippy. Commit:
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe add crates/client-streams/src/processor/erased.rs crates/client-streams/src/processor/mod.rs crates/client-streams/src/lib.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): erased record + dispatch context"
```

---

## Task 4: Processor / ProcessorSupplier / ProcessorContext

**Files:**
- Create: `crates/client-streams/src/processor/api.rs`
- Modify: `crates/client-streams/src/processor/mod.rs`, `lib.rs`

- [ ] **Step 1: Write the failing test** (a processor that upper-cases values + forwards)

Append to `api.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::erased::{Dispatch, ErasedRecord};
    use crate::processor::record::{Record, RecordContext};
    use assert2::check;
    use std::collections::VecDeque;

    struct Upper;
    impl Processor<String, String, String, String> for Upper {
        fn process(&mut self, ctx: &mut ProcessorContext<String, String>, r: Record<String, String>) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }

    #[test]
    fn forward_pushes_erased_record_to_each_child() {
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext { topic: "t".into(), partition: 0, offset: 0, timestamp: 5 };
        let children = [3usize, 4usize];
        let mut dispatch = Dispatch { buffer: &mut buffer, children: &children, output: &mut output, record_ctx: &rc };
        let mut ctx = ProcessorContext::<String, String>::new(&mut dispatch);
        Upper.process(&mut ctx, Record::new(Some("k".into()), "hi".into(), 5));
        check!(buffer.len() == 2); // one per child
        let (child, rec) = buffer.pop_front().unwrap();
        check!(child == 3);
        check!(*rec.value.downcast::<String>().unwrap() == "HI");
    }
}
```

- [ ] **Step 2: Run → FAIL** — `cargo test -p crabka-client-streams --lib processor::api`

- [ ] **Step 3: Implement**

`api.rs` (above tests):
```rust
//! The typed Processor API: `Processor`, `ProcessorSupplier`, and the
//! `ProcessorContext` users call `forward` on.

use std::any::Any;
use std::marker::PhantomData;

use super::erased::{Dispatch, ErasedRecord};
use super::record::{Record, RecordContext};

/// A stateless record processor. One instance is created per task via
/// [`ProcessorSupplier::get`]. Mirrors `org.apache.kafka.streams.processor.api.Processor`.
pub trait Processor<KIn, VIn, KOut, VOut>: Send + 'static {
    /// Called once before the first record (override to capture config).
    fn init(&mut self, _ctx: &mut ProcessorContext<KOut, VOut>) {}
    /// Process one record; call `ctx.forward(..)` to emit to child nodes.
    fn process(&mut self, ctx: &mut ProcessorContext<KOut, VOut>, record: Record<KIn, VIn>);
    /// Called once at task shutdown.
    fn close(&mut self) {}
}

/// Factory for [`Processor`] instances (one per task → per-task isolation).
pub trait ProcessorSupplier<KIn, VIn, KOut, VOut>: Send + Sync + 'static {
    fn get(&self) -> Box<dyn Processor<KIn, VIn, KOut, VOut>>;
}

// Blanket impl so a closure `|| Box::new(MyProc)` is a supplier.
impl<F, KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> for F
where
    F: Fn() -> Box<dyn Processor<KIn, VIn, KOut, VOut>> + Send + Sync + 'static,
{
    fn get(&self) -> Box<dyn Processor<KIn, VIn, KOut, VOut>> {
        self()
    }
}

/// Handed to [`Processor::process`]. `forward` boxes the record and queues it
/// for each child node (the driver drains the queue). `KOut`/`VOut` must be
/// `Clone` so a record can fan out to multiple children.
pub struct ProcessorContext<'a, 'd, KOut, VOut> {
    dispatch: &'a mut Dispatch<'d>,
    _pd: PhantomData<fn(KOut, VOut)>,
}

impl<'a, 'd, KOut, VOut> ProcessorContext<'a, 'd, KOut, VOut>
where
    KOut: Any + Send + Clone,
    VOut: Any + Send + Clone,
{
    pub(crate) fn new(dispatch: &'a mut Dispatch<'d>) -> Self {
        Self { dispatch, _pd: PhantomData }
    }

    /// Forward a record to all child nodes.
    pub fn forward(&mut self, record: Record<KOut, VOut>) {
        let children = self.dispatch.children;
        for (i, &child) in children.iter().enumerate() {
            // Clone for all but the last child; move into the last.
            let rec = if i + 1 == children.len() {
                erase(&record, true)
            } else {
                erase(&record, false)
            };
            self.dispatch.buffer.push_back((child, rec));
        }
    }

    /// Metadata of the source record currently being processed.
    #[must_use]
    pub fn record_context(&self) -> &RecordContext {
        self.dispatch.record_ctx
    }
}

fn erase<KOut, VOut>(record: &Record<KOut, VOut>, _last: bool) -> ErasedRecord
where
    KOut: Any + Send + Clone,
    VOut: Any + Send + Clone,
{
    // Clone-per-child keeps fan-out safe; single-child is the common case.
    let key: Option<Box<dyn Any + Send>> =
        record.key.clone().map(|k| Box::new(k) as Box<dyn Any + Send>);
    let value: Box<dyn Any + Send> = Box::new(record.value.clone());
    ErasedRecord::new(key, value, record.timestamp)
}
```

Note for implementer: the `'a, 'd` two-lifetime context is needed because `Dispatch<'d>` itself borrows. If the borrow checker complains, simplify to a single lifetime (`ProcessorContext<'a, KOut, VOut>` holding `&'a mut Dispatch<'a>`); the test will tell you which compiles. Keep the public `forward`/`record_context` surface identical.

Add to `processor/mod.rs`:
```rust
pub mod api;
pub use api::{Processor, ProcessorContext, ProcessorSupplier};
```
and `lib.rs`: `pub use processor::{Processor, ProcessorContext, ProcessorSupplier};`

- [ ] **Step 4: Run → PASS + clippy + commit**
Run: `cargo test -p crabka-client-streams --lib processor::api` → PASS.
Clippy. Commit:
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe add crates/client-streams/src/processor/api.rs crates/client-streams/src/processor/mod.rs crates/client-streams/src/lib.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): typed Processor API + ProcessorContext"
```

---

## Task 5: Erased node adapters + factories

**Files:**
- Create: `crates/client-streams/src/processor/node.rs`
- Modify: `crates/client-streams/src/processor/mod.rs`

This is the type-erasure boundary: each adapter carries the `TypeId`+`type_name` of the `(K,V)` it consumes/produces (for build-time validation), and downcasts/serializes as appropriate.

- [ ] **Step 1: Write the failing test** (a processor node adapter downcasts, runs, forwards)

Append to `node.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::api::{Processor, ProcessorContext};
    use crate::processor::erased::{Dispatch, ErasedRecord};
    use crate::processor::record::{Record, RecordContext};
    use crate::processor::serde::StringSerde;
    use assert2::check;
    use std::any::TypeId;
    use std::collections::VecDeque;

    struct Upper;
    impl Processor<String, String, String, String> for Upper {
        fn process(&mut self, ctx: &mut ProcessorContext<String, String>, r: Record<String, String>) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }

    #[test]
    fn processor_node_downcasts_runs_forwards() {
        let mut node = ProcessorNode::new("upcase".into(), || Box::new(Upper));
        check!(node.input_kv() == (TypeId::of::<String>(), TypeId::of::<String>()));
        check!(node.output_kv() == Some((TypeId::of::<String>(), TypeId::of::<String>())));

        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext { topic: "t".into(), partition: 0, offset: 0, timestamp: 1 };
        let children = [9usize];
        let mut d = Dispatch { buffer: &mut buffer, children: &children, output: &mut output, record_ctx: &rc };
        let rec = ErasedRecord::new(Some(Box::new("k".to_string())), Box::new("hi".to_string()), 1);
        node.process(&mut d, rec).unwrap();
        let (_c, out) = buffer.pop_front().unwrap();
        check!(*out.value.downcast::<String>().unwrap() == "HI");
    }

    #[test]
    fn sink_node_serializes_to_output() {
        let mut node = SinkNode::new("out".into(), "out-topic".into(), StringSerde, StringSerde);
        let mut buffer = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext { topic: "t".into(), partition: 0, offset: 0, timestamp: 1 };
        let mut d = Dispatch { buffer: &mut buffer, children: &[], output: &mut output, record_ctx: &rc };
        let rec = ErasedRecord::new(Some(Box::new("k".to_string())), Box::new("V".to_string()), 1);
        node.process(&mut d, rec).unwrap();
        check!(output.len() == 1);
        check!(output[0].topic == "out-topic");
        check!(output[0].value.as_ref().unwrap().as_ref() == b"V");
    }
}
```

- [ ] **Step 2: Run → FAIL** — `cargo test -p crabka-client-streams --lib processor::node`

- [ ] **Step 3: Implement**

`node.rs` (above tests):
```rust
//! Erased node adapters: Source (deserialize→box), Processor (downcast→run→box),
//! Sink (downcast→serialize→emit). Each carries the `TypeId`+`type_name` of the
//! `(K,V)` it consumes/produces, for build-time wiring validation.

use std::any::{Any, TypeId, type_name};

use bytes::Bytes;

use super::api::{Processor, ProcessorContext, ProcessorSupplier};
use super::erased::{Dispatch, ErasedRecord, OutputRecord, ProcessorError};
use super::record::Record;
use super::serde::Serde;

/// A node in the instantiated graph. Object-safe so heterogeneous nodes live in
/// one `Vec`.
pub(crate) trait ErasedNode: Send {
    fn name(&self) -> &str;
    fn process(&mut self, d: &mut Dispatch<'_>, rec: ErasedRecord) -> Result<(), ProcessorError>;
    /// `(TypeId<K>, TypeId<V>)` this node consumes.
    fn input_kv(&self) -> (TypeId, TypeId);
    /// `(TypeId<K>, TypeId<V>)` this node forwards, or `None` for a sink.
    fn output_kv(&self) -> Option<(TypeId, TypeId)>;
    /// `(type_name<K>, type_name<V>)` consumed/produced — for diagnostics.
    fn input_names(&self) -> (&'static str, &'static str);
    fn output_names(&self) -> Option<(&'static str, &'static str)>;
}

// ---- Processor node ----
pub(crate) struct ProcessorNode<KIn, VIn, KOut, VOut> {
    name: String,
    inner: Box<dyn Processor<KIn, VIn, KOut, VOut>>,
}
impl<KIn, VIn, KOut, VOut> ProcessorNode<KIn, VIn, KOut, VOut>
where
    KIn: Any + Send,
    VIn: Any + Send,
    KOut: Any + Send + Clone,
    VOut: Any + Send + Clone,
{
    pub fn new<S: ProcessorSupplier<KIn, VIn, KOut, VOut>>(name: String, supplier: S) -> Self {
        Self { name, inner: supplier.get() }
    }
}
impl<KIn, VIn, KOut, VOut> ErasedNode for ProcessorNode<KIn, VIn, KOut, VOut>
where
    KIn: Any + Send,
    VIn: Any + Send,
    KOut: Any + Send + Clone,
    VOut: Any + Send + Clone,
{
    fn name(&self) -> &str { &self.name }
    fn input_kv(&self) -> (TypeId, TypeId) { (TypeId::of::<KIn>(), TypeId::of::<VIn>()) }
    fn output_kv(&self) -> Option<(TypeId, TypeId)> { Some((TypeId::of::<KOut>(), TypeId::of::<VOut>())) }
    fn input_names(&self) -> (&'static str, &'static str) { (type_name::<KIn>(), type_name::<VIn>()) }
    fn output_names(&self) -> Option<(&'static str, &'static str)> { Some((type_name::<KOut>(), type_name::<VOut>())) }
    fn process(&mut self, d: &mut Dispatch<'_>, rec: ErasedRecord) -> Result<(), ProcessorError> {
        let value = rec.value.downcast::<VIn>()
            .map_err(|_| ProcessorError::Downcast { node: self.name.clone(), expected: type_name::<VIn>() })?;
        let key = match rec.key {
            Some(k) => Some(*k.downcast::<KIn>()
                .map_err(|_| ProcessorError::Downcast { node: self.name.clone(), expected: type_name::<KIn>() })?),
            None => None,
        };
        let record = Record::new(key, *value, rec.timestamp);
        let mut ctx = ProcessorContext::<KOut, VOut>::new(d);
        self.inner.process(&mut ctx, record);
        Ok(())
    }
}

// ---- Sink node ----
pub(crate) struct SinkNode<K, V, KS, VS> {
    name: String,
    topic: String,
    key_serde: KS,
    value_serde: VS,
    _pd: std::marker::PhantomData<fn(K, V)>,
}
impl<K, V, KS, VS> SinkNode<K, V, KS, VS>
where
    K: Any + Send, V: Any + Send, KS: Serde<K>, VS: Serde<V>,
{
    pub fn new(name: String, topic: String, key_serde: KS, value_serde: VS) -> Self {
        Self { name, topic, key_serde, value_serde, _pd: std::marker::PhantomData }
    }
}
impl<K, V, KS, VS> ErasedNode for SinkNode<K, V, KS, VS>
where
    K: Any + Send, V: Any + Send, KS: Serde<K>, VS: Serde<V>,
{
    fn name(&self) -> &str { &self.name }
    fn input_kv(&self) -> (TypeId, TypeId) { (TypeId::of::<K>(), TypeId::of::<V>()) }
    fn output_kv(&self) -> Option<(TypeId, TypeId)> { None }
    fn input_names(&self) -> (&'static str, &'static str) { (type_name::<K>(), type_name::<V>()) }
    fn output_names(&self) -> Option<(&'static str, &'static str)> { None }
    fn process(&mut self, d: &mut Dispatch<'_>, rec: ErasedRecord) -> Result<(), ProcessorError> {
        let value = rec.value.downcast::<V>()
            .map_err(|_| ProcessorError::Downcast { node: self.name.clone(), expected: type_name::<V>() })?;
        let key = match rec.key {
            Some(k) => Some(*k.downcast::<K>()
                .map_err(|_| ProcessorError::Downcast { node: self.name.clone(), expected: type_name::<K>() })?),
            None => None,
        };
        let key_bytes: Option<Bytes> = key.as_ref().map(|k| self.key_serde.serialize(k));
        let value_bytes = Some(self.value_serde.serialize(&value));
        d.output.push(OutputRecord { topic: self.topic.clone(), key: key_bytes, value: value_bytes, timestamp: rec.timestamp });
        Ok(())
    }
}

// ---- Source node ----  (deserializes raw bytes → typed → erased; driven by the graph entry point)
pub(crate) struct SourceNode<K, V, KS, VS> {
    name: String,
    key_serde: KS,
    value_serde: VS,
    _pd: std::marker::PhantomData<fn(K, V)>,
}
impl<K, V, KS, VS> SourceNode<K, V, KS, VS>
where
    K: Any + Send + Clone, V: Any + Send + Clone, KS: Serde<K>, VS: Serde<V>,
{
    pub fn new(name: String, key_serde: KS, value_serde: VS) -> Self {
        Self { name, key_serde, value_serde, _pd: std::marker::PhantomData }
    }
    /// Deserialize raw bytes into an erased record (the graph pushes this to the
    /// source's children). Returns an erased record + the output `(K,V)` typeids.
    pub fn deserialize(&self, key: Option<&[u8]>, value: &[u8], timestamp: i64) -> Result<ErasedRecord, ProcessorError> {
        let k: Option<Box<dyn Any + Send>> = match key {
            Some(b) => Some(Box::new(self.key_serde.deserialize(b)
                .map_err(|e| ProcessorError::Serde { node: self.name.clone(), message: e.to_string() })?) as Box<dyn Any + Send>),
            None => None,
        };
        let v = self.value_serde.deserialize(value)
            .map_err(|e| ProcessorError::Serde { node: self.name.clone(), message: e.to_string() })?;
        Ok(ErasedRecord::new(k, Box::new(v) as Box<dyn Any + Send>, timestamp))
    }
    pub fn output_kv(&self) -> (TypeId, TypeId) { (TypeId::of::<K>(), TypeId::of::<V>()) }
    pub fn output_names(&self) -> (&'static str, &'static str) { (type_name::<K>(), type_name::<V>()) }
}
```

Note for implementer: source nodes are entered by the driver (Task 6) by calling `deserialize`, then pushing to children — they don't implement `ErasedNode::process` (they're never a *child* target). If it's cleaner to make `SourceNode` implement `ErasedNode` with a no-op `process` and a separate `deserialize`, do so; the graph (Task 6) only calls `deserialize` on sources and `process` on processor/sink nodes.

Add to `processor/mod.rs`: `pub(crate) mod node;`

- [ ] **Step 4: Run → PASS + clippy + commit**
Run: `cargo test -p crabka-client-streams --lib processor::node` → PASS (2).
Clippy. Commit:
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe add crates/client-streams/src/processor/node.rs crates/client-streams/src/processor/mod.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): erased source/processor/sink node adapters"
```

---

## Task 6: Graph + driver loop

**Files:**
- Create: `crates/client-streams/src/processor/graph.rs`
- Modify: `crates/client-streams/src/processor/mod.rs`

- [ ] **Step 1: Write the failing test** (build a source→upper→sink graph by hand, pipe a record, read output)

Append to `graph.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::api::{Processor, ProcessorContext};
    use crate::processor::node::{ProcessorNode, SinkNode, SourceNode};
    use crate::processor::record::Record;
    use crate::processor::serde::StringSerde;
    use assert2::check;

    struct Upper;
    impl Processor<String, String, String, String> for Upper {
        fn process(&mut self, ctx: &mut ProcessorContext<String, String>, r: Record<String, String>) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }

    #[test]
    fn drives_source_processor_sink() {
        // node 0 = source "src", node 1 = processor "up", node 2 = sink "out"
        let source = SourceNode::new("src".into(), StringSerde, StringSerde);
        let up = Box::new(ProcessorNode::new("up".into(), || Box::new(Upper))) as Box<dyn ErasedNode>;
        let sink = Box::new(SinkNode::new("out".into(), "out-topic".into(), StringSerde, StringSerde)) as Box<dyn ErasedNode>;
        let mut graph = Graph {
            nodes: vec![up, sink],          // index 0=up, 1=sink (non-source nodes)
            children: vec![vec![1], vec![]], // up -> sink ; sink -> none
            sources: vec![GraphSource { topic: "in".into(), node: source_into_erased(source), children: vec![0] }],
            output: Vec::new(),
        };
        graph.pipe("in", Some(b"k"), b"hi", 7).unwrap();
        let out = graph.take_output();
        check!(out.len() == 1);
        check!(out[0].topic == "out-topic");
        check!(out[0].value.as_ref().unwrap().as_ref() == b"HI");
    }
}
```

The test references helpers (`GraphSource`, `source_into_erased`) — adapt the exact shape to your implementation; the assertion (source→upper→sink produces "HI" on "out-topic") is the contract.

- [ ] **Step 2: Run → FAIL** — `cargo test -p crabka-client-streams --lib processor::graph`

- [ ] **Step 3: Implement**

`graph.rs` (above tests). The graph holds non-source nodes in a `Vec` indexed by position, a parallel `children` adjacency list, the sources (keyed by topic), and the output collector. `pipe` deserializes at the matching source, seeds the buffer with the source's children, then drains:
```rust
//! The instantiated, runnable processor graph for one subtopology + partition.
//! Non-recursive driver loop: `forward` appends to a buffer the driver drains.

use std::collections::VecDeque;

use super::erased::{Dispatch, ErasedRecord, OutputRecord, ProcessorError};
use super::node::ErasedNode;
use super::record::RecordContext;

/// A source: which external/repartition topic it reads, its deserializing
/// adapter, and the node indices it feeds.
pub(crate) struct GraphSource {
    pub topic: String,
    /// Deserialize `(key,value,ts)` → an erased record. Boxed closure so the
    /// graph stays non-generic. (Built from a `SourceNode<K,V,..>` in Task 8.)
    pub deserialize: Box<dyn Fn(Option<&[u8]>, &[u8], i64) -> Result<ErasedRecord, ProcessorError> + Send>,
    pub children: Vec<usize>,
}

pub(crate) struct Graph {
    pub nodes: Vec<Box<dyn ErasedNode>>,
    pub children: Vec<Vec<usize>>,
    pub sources: Vec<GraphSource>,
    pub output: Vec<OutputRecord>,
}

impl Graph {
    /// Feed one record arriving on `topic`; runs the graph to completion,
    /// appending sink outputs to `self.output`.
    pub fn pipe(&mut self, topic: &str, key: Option<&[u8]>, value: &[u8], timestamp: i64) -> Result<(), ProcessorError> {
        // Find the source(s) for this topic.
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let rc = RecordContext { topic: topic.to_string(), partition: 0, offset: 0, timestamp };
        let mut seeded = false;
        for src in &self.sources {
            if src.topic == topic {
                let rec = (src.deserialize)(key, value, timestamp)?;
                // fan the source record to its children (clone via re-deserialize not needed for single source;
                // for multiple children, re-run deserialize to avoid cloning Box<dyn Any>).
                for (i, &child) in src.children.iter().enumerate() {
                    let r = if i + 1 == src.children.len() { rec_take(&rec, key, value, timestamp, src)? } else { (src.deserialize)(key, value, timestamp)? };
                    buffer.push_back((child, r));
                }
                seeded = true;
                break;
            }
        }
        if !seeded { return Ok(()); } // no source for this topic — ignore

        while let Some((idx, rec)) = buffer.pop_front() {
            let children = std::mem::take(&mut self.children[idx]);
            let mut d = Dispatch { buffer: &mut buffer, children: &children, output: &mut self.output, record_ctx: &rc };
            let res = self.nodes[idx].process(&mut d, rec);
            self.children[idx] = children; // restore
            res?;
        }
        Ok(())
    }

    pub fn take_output(&mut self) -> Vec<OutputRecord> {
        std::mem::take(&mut self.output)
    }
}
```

Note for implementer: cloning `Box<dyn Any>` is impossible, so to fan a *source* record to multiple children, re-run `deserialize` per child (sources hold the raw bytes). For *processor* fan-out, the typed `ProcessorContext::forward` already clones the typed value per child (Task 4). The `rec_take`/re-deserialize detail above is illustrative — implement multi-child source fan-out by calling `(src.deserialize)(...)` once per child (simplest correct approach); drop the `rec_take` helper. The `std::mem::take(&mut self.children[idx])` dance avoids borrowing `self.children` and `self.nodes` simultaneously — or store `children` in a separate `Vec` you can split-borrow; pick what compiles cleanly.

Add to `processor/mod.rs`: `pub(crate) mod graph;`

- [ ] **Step 4: Run → PASS + clippy + commit**
Run: `cargo test -p crabka-client-streams --lib processor::graph` → PASS.
Clippy. Commit:
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe add crates/client-streams/src/processor/graph.rs crates/client-streams/src/processor/mod.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): graph driver loop"
```

---

## Task 7: Builder evolution — typed add_* + factories + build-time TypeId validation + diagnostics

**Files:**
- Modify: `crates/client-streams/src/topology/builder.rs`
- Modify: `crates/client-streams/src/topology/node.rs` (if needed for factory storage)
- Modify: `crates/client-streams/src/topology/mod.rs`, `lib.rs`
- Migrate: `crates/client-streams/tests/golden_frame.rs`, `crates/client-streams/tests/integration.rs`, and the `#[cfg(test)]` builder tests

This is the largest task: it threads typed serdes/suppliers into the existing structural builder, stores erased **factories** (so the runtime can instantiate a `Graph`), validates wiring types at `build()`, and migrates #1's call sites. The structural side (`reg: NodeRegistry`, grouping, wire) is unchanged.

- [ ] **Step 1: Write the failing tests**

Replace the existing `#[cfg(test)] mod tests` in `builder.rs` with:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::api::{Processor, ProcessorContext};
    use crate::processor::record::Record;
    use crate::processor::serde::{I64Serde, StringSerde};
    use assert2::check;

    struct Upper;
    impl Processor<String, String, String, String> for Upper {
        fn process(&mut self, ctx: &mut ProcessorContext<String, String>, r: Record<String, String>) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }

    fn upper_topology() -> Topology {
        let mut t = Topology::new();
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_processor("up", || Box::new(Upper), ["src"]);
        t.add_sink("out", "out-topic", ["up"], StringSerde, StringSerde);
        t
    }

    #[test]
    fn build_single_source_sink_wire_unchanged() {
        let built = upper_topology().build("app").unwrap();
        let wire = built.to_wire();
        check!(wire.epoch == 0);
        check!(wire.subtopologies[0].subtopology_id == "0");
        check!(wire.subtopologies[0].source_topics == vec!["in".to_string()]);
        check!(built.source_topics_for("0") == ["in".to_string()]);
    }

    #[test]
    fn type_mismatch_is_reported_at_build() {
        // sink expects <String,i64> but its parent forwards <String,String>
        let mut t = Topology::new();
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_processor("up", || Box::new(Upper), ["src"]);          // produces Record<String,String>
        t.add_sink("out", "out-topic", ["up"], StringSerde, I64Serde); // expects Record<String,i64>
        let err = t.build("app").unwrap_err();
        let msg = err.to_string();
        check!(msg.contains("wiring type error"));
        check!(msg.contains("`out`"));
        check!(msg.contains("`up`"));
    }

    #[test]
    fn unknown_predecessor_still_rejected() {
        let mut t = Topology::new();
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_sink("out", "o", ["nope"], StringSerde, StringSerde);
        check!(t.build("app").is_err());
    }
}
```

- [ ] **Step 2: Run → FAIL** — `cargo test -p crabka-client-streams --lib topology::builder` (signatures changed → compile error first)

- [ ] **Step 3: Implement**

In `builder.rs`:
1. Add a `TypeMismatch` variant to `TopologyError`:
```rust
    #[error(
        "topology wiring type error: {child_kind} `{child}` expects `{expects}`,\n  \
         but its parent {parent_kind} `{parent}` forwards `{produces}`\n  \
         = help: a node's output type (KOut, VOut) must match every child's input type (KIn, VIn)\n  \
         = note: checked at build() because the Processor API wires nodes by name;\n          \
         use the typed DSL (sub-project #4) for compile-time wiring safety"
    )]
    TypeMismatch {
        parent: String, parent_kind: &'static str, produces: String,
        child: String, child_kind: &'static str, expects: String,
    },
```
(Use `String` for `produces`/`expects` holding the `Record<K,V>` rendering, e.g. `format!("Record<{}, {}>", kname, vname)`.)

2. Store an **erased factory** per node name alongside the structural registry. Add to `Topology`:
```rust
    factories: std::collections::HashMap<String, NodeFactory>,
```
where `NodeFactory` (define in `processor/graph.rs` or a new `processor/factory.rs`) is an enum/struct capturing what's needed to (a) report input/output `(TypeId, type_name)` for validation, and (b) instantiate an `ErasedNode`/`GraphSource`:
```rust
pub(crate) struct NodeFactory {
    pub kind: FactoryKind,
    pub input_kv: Option<(TypeId, TypeId)>,            // None for source (source has no input)
    pub output_kv: Option<(TypeId, TypeId)>,           // None for sink
    pub input_names: Option<(&'static str, &'static str)>,
    pub output_names: Option<(&'static str, &'static str)>,
    pub make_node: Box<dyn Fn() -> Box<dyn ErasedNode> + Send + Sync>,   // for processor/sink
    pub make_source: Option<Box<dyn Fn() -> GraphSource + Send + Sync>>, // for source (children filled at instantiate)
}
pub(crate) enum FactoryKind { Source, Processor, Sink }
```
The typed `add_*` methods build the appropriate `NodeFactory` (capturing serdes/supplier by move into the closures) AND call the existing structural `self.reg.add_source/add_processor/add_sink` (unchanged — feeds grouping/wire).

3. Typed builder methods (replace the untyped ones):
```rust
pub fn add_source<K, V, KS, VS>(&mut self, name: impl Into<String>, topics: impl IntoIterator<Item = impl Into<String>>, key_serde: KS, value_serde: VS) -> &mut Self
where K: Any + Send + Clone, V: Any + Send + Clone, KS: Serde<K> + Clone, VS: Serde<V> + Clone { /* reg.add_source + factory(Source, output_kv=(K,V)) */ self }

pub fn add_processor<KIn, VIn, KOut, VOut, S>(&mut self, name: impl Into<String>, supplier: S, predecessors: impl IntoIterator<Item = impl Into<String>>) -> &mut Self
where KIn: Any+Send, VIn: Any+Send, KOut: Any+Send+Clone, VOut: Any+Send+Clone, S: ProcessorSupplier<KIn,VIn,KOut,VOut> + Clone { /* reg.add_processor + factory(Processor, input=(KIn,VIn), output=(KOut,VOut)) */ self }

pub fn add_sink<K, V, KS, VS>(&mut self, name: impl Into<String>, topic: impl Into<String>, predecessors: impl IntoIterator<Item = impl Into<String>>, key_serde: KS, value_serde: VS) -> &mut Self
where K: Any+Send, V: Any+Send, KS: Serde<K> + Clone, VS: Serde<V> + Clone { /* reg.add_sink + factory(Sink, input=(K,V)) */ self }
```
The serdes/suppliers must be `Clone` so each task instantiation gets its own (the `make_node`/`make_source` closures clone them). (Built-in serdes derive `Clone`; require user suppliers/serdes to be `Clone`.)

4. In `build()`, after the existing structural steps, add **edge validation**: for each node, for each of its children (resolved via `self.reg` predecessors → reverse to children, or iterate predecessor edges), compare `parent.output_kv` to `child.input_kv`. On mismatch, return `TypeError::TypeMismatch { … }` using the stored `*_names`. Then stash the factories into `BuiltTopology` for runtime instantiation:
```rust
pub struct BuiltTopology {
    wire: WireTopology,
    source_topics: BTreeMap<String, Vec<String>>,
    application_id: String,
    pub(crate) factories: HashMap<String, NodeFactory>,   // NEW
    pub(crate) edges: Vec<(String, String)>,              // NEW: (parent_name, child_name) for graph wiring
    pub(crate) subtopology_nodes: BTreeMap<String, Vec<String>>, // NEW: subtopology_id -> node names
}
```
`BuiltTopology` is no longer `Clone` (closures aren't `Clone`) — drop the `#[derive(Clone)]`. Verify no caller relies on `BuiltTopology: Clone` (membership takes it by value via `Arc` in #1 — check `membership/client.rs`; it wraps in `Arc`, so non-Clone is fine).

5. Add `BuiltTopology::instantiate_subtopology(&self, subtopology_id, partition) -> Graph` (used by the test driver + 2b): build the `Vec<Box<dyn ErasedNode>>` + `children` adjacency + `sources` for that subtopology from the factories + edges. (Define here; the test driver Task 9 consumes it.)

- [ ] **Step 4: Migrate #1's call sites + tests**

- `crates/client-streams/tests/golden_frame.rs`: change `topo.add_source("src", ["streams-input"])` → `topo.add_source("src", ["streams-input"], BytesSerde, BytesSerde)` and `topo.add_sink("snk", "streams-output", ["src"])` → `topo.add_sink("snk", "streams-output", ["src"], BytesSerde, BytesSerde)`. Wire assertions unchanged → still passes.
- `crates/client-streams/tests/integration.rs`: same migration for both `member_joins_*` and `missing_source_*` topologies (use `BytesSerde`; they don't process records).
- Any other `add_source`/`add_sink`/`add_processor` call site in the crate (grep): migrate to typed.

- [ ] **Step 5: Run → PASS (incl. golden frame) + clippy + commit**

Run: `cargo test -p crabka-client-streams --lib topology` → PASS.
Run: `cargo test -p crabka-client-streams --test golden_frame` → PASS (wire unchanged).
Run: `cargo clippy -p crabka-client-streams --all-targets -- -D warnings` → clean.
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe add crates/client-streams/src/topology crates/client-streams/src/processor crates/client-streams/tests crates/client-streams/src/lib.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): typed builder + factories + build-time wiring diagnostics"
```

---

## Task 8: TopologyTestDriver

**Files:**
- Create: `crates/client-streams/src/test_driver.rs`
- Modify: `crates/client-streams/src/lib.rs`

- [ ] **Step 1: Write the failing tests**

`test_driver.rs` test module:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::api::{Processor, ProcessorContext};
    use crate::processor::record::Record;
    use crate::processor::serde::StringSerde;
    use crate::topology::Topology;
    use assert2::check;

    struct Upper;
    impl Processor<String, String, String, String> for Upper {
        fn process(&mut self, ctx: &mut ProcessorContext<String, String>, r: Record<String, String>) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }

    struct DropEmpty; // filter: forward only non-empty values
    impl Processor<String, String, String, String> for DropEmpty {
        fn process(&mut self, ctx: &mut ProcessorContext<String, String>, r: Record<String, String>) {
            if !r.value.is_empty() { ctx.forward(r); }
        }
    }

    fn built() -> crate::topology::BuiltTopology {
        let mut t = Topology::new();
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_processor("up", || Box::new(Upper), ["src"]);
        t.add_processor("flt", || Box::new(DropEmpty), ["up"]);
        t.add_sink("out", "out", ["flt"], StringSerde, StringSerde);
        t.build("app").unwrap()
    }

    #[test]
    fn pipes_input_through_to_output() {
        let mut d = TopologyTestDriver::new(built());
        let mut input = d.input_topic::<String, String>("in");
        let mut output = d.output_topic::<String, String>("out");
        input.pipe(Some("k".into()), "hello".into(), 0);
        check!(output.read() == Some((Some("k".to_string()), "HELLO".to_string())));
        input.pipe(Some("k2".into()), String::new(), 1);   // filtered
        check!(output.read().is_none());
    }
}
```

The `input_topic`/`output_topic` borrow design may need `&mut driver` per call or a handle that borrows the driver — implement whichever compiles; the contract is `pipe(key, value, ts)` and `read() -> Option<(Option<K>, V)>`.

- [ ] **Step 2: Run → FAIL** — `cargo test -p crabka-client-streams --lib test_driver`

- [ ] **Step 3: Implement**

`test_driver.rs`:
```rust
//! Synchronous, broker-free driver for testing topologies (JVM
//! `TopologyTestDriver` analog). Pipe input records, read output records.

use std::collections::HashMap;
use std::marker::PhantomData;

use crate::processor::erased::OutputRecord;
use crate::processor::graph::Graph;
use crate::processor::serde::Serde;
use crate::topology::BuiltTopology;

pub struct TopologyTestDriver {
    /// One instantiated graph per subtopology (partition 0).
    graphs: Vec<Graph>,
    /// topic -> graph index that sources it (external + repartition).
    source_index: HashMap<String, usize>,
    /// Captured outputs per topic (FIFO).
    output: HashMap<String, std::collections::VecDeque<OutputRecord>>,
    /// Serdes by topic, to type input/output handles (captured from the topology).
    built: BuiltTopology,
}

impl TopologyTestDriver {
    #[must_use]
    pub fn new(built: BuiltTopology) -> Self {
        // Instantiate each subtopology's graph at partition 0; index sources by topic.
        // ... build graphs + source_index from built.instantiate_subtopology(...) ...
        # /* see note */ unimplemented!()
    }

    /// Pipe one already-serialized record and drain outputs, looping any
    /// repartition-topic output back into the matching source.
    fn pipe_bytes(&mut self, topic: &str, key: Option<&[u8]>, value: &[u8], ts: i64) {
        let mut queue = vec![(topic.to_string(), key.map(<[u8]>::to_vec), value.to_vec(), ts)];
        while let Some((t, k, v, ts)) = queue.pop() {
            let Some(&gi) = self.source_index.get(&t) else { continue };
            let _ = self.graphs[gi].pipe(&t, k.as_deref(), &v, ts); // ProcessorError surfaced in real impl
            for out in self.graphs[gi].take_output() {
                if self.source_index.contains_key(&out.topic) {
                    // internal repartition topic feeding another subtopology — loop back
                    queue.push((out.topic.clone(), out.key.as_ref().map(|b| b.to_vec()), out.value.clone().unwrap_or_default().to_vec(), out.timestamp));
                } else {
                    self.output.entry(out.topic.clone()).or_default().push_back(out);
                }
            }
        }
    }

    pub fn input_topic<K, V>(&mut self, topic: &str) -> TestInputTopic<'_, K, V> { /* capture topic's source serdes */ unimplemented!() }
    pub fn output_topic<K, V>(&mut self, topic: &str) -> TestOutputTopic<'_, K, V> { unimplemented!() }
}

pub struct TestInputTopic<'a, K, V> { driver: &'a mut TopologyTestDriver, topic: String, key_serde: /* Arc<dyn Serde<K>> */ ..., value_serde: ..., _pd: PhantomData<(K,V)> }
impl<K, V> TestInputTopic<'_, K, V> {
    pub fn pipe(&mut self, key: Option<K>, value: V, timestamp: i64) {
        let kb = key.as_ref().map(|k| self.key_serde.serialize(k));
        let vb = self.value_serde.serialize(&value);
        self.driver.pipe_bytes(&self.topic, kb.as_deref(), &vb, timestamp);
    }
}
pub struct TestOutputTopic<'a, K, V> { driver: &'a mut TopologyTestDriver, topic: String, key_serde: ..., value_serde: ..., _pd: PhantomData<(K,V)> }
impl<K, V> TestOutputTopic<'_, K, V> {
    pub fn read(&mut self) -> Option<(Option<K>, V)> {
        let out = self.driver.output.get_mut(&self.topic)?.pop_front()?;
        let k = out.key.map(|b| self.key_serde.deserialize(&b).expect("deser key"));
        let v = self.value_serde.deserialize(&out.value.unwrap_or_default()).expect("deser val");
        Some((k, v))
    }
}
```

Note for implementer: the `unimplemented!()`/`...` placeholders above are **structure illustrations, not deliverable code** — fill them in. The two hard parts: (1) instantiating graphs from `BuiltTopology` (use the `instantiate_subtopology` added in Task 7), and (2) typing the input/output handles. For (2), the simplest approach that compiles: have the caller pass the serdes explicitly — `input_topic::<K,V>(topic, key_serde, value_serde)` — rather than recovering them type-erased from the topology (recovering a `Serde<K>` from an erased factory needs the K type, which the turbofish supplies but the stored serde is erased). Passing serdes at `input_topic`/`output_topic` is the pragmatic, JVM-aligned choice (`createInputTopic(topic, keySerde, valueSerde)`); update the Task-8 test to pass `StringSerde, StringSerde` to `input_topic`/`output_topic` accordingly. Resolve this in the implementation and keep the `pipe`/`read` contract.

Add to `lib.rs`:
```rust
mod test_driver;
pub use test_driver::{TestInputTopic, TestOutputTopic, TopologyTestDriver};
```

- [ ] **Step 4: Run → PASS + clippy + commit**

Run: `cargo test -p crabka-client-streams --lib test_driver` → PASS.
Add a multi-child (branch) test and a re-key+repartition test (two subtopologies, sink to an internal repartition topic that's also a source — assert the looped-through output appears on the final sink). Run them.
Clippy. Commit:
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe add crates/client-streams/src/test_driver.rs crates/client-streams/src/lib.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): TopologyTestDriver"
```

---

## Task 9: Docs example + final verification

**Files:**
- Modify: `crates/client-streams/src/lib.rs`

- [ ] **Step 1: Add a doc example** to the crate docs (a map/filter topology tested with the `TopologyTestDriver`):
```rust
//! ## Processor API (sub-project #2)
//!
//! ```
//! use crabka_client_streams::{Processor, ProcessorContext, Record, StringSerde, Topology, TopologyTestDriver};
//!
//! struct Upper;
//! impl Processor<String, String, String, String> for Upper {
//!     fn process(&mut self, ctx: &mut ProcessorContext<String, String>, r: Record<String, String>) {
//!         ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
//!     }
//! }
//!
//! let mut topo = Topology::new();
//! topo.add_source("src", ["in"], StringSerde, StringSerde);
//! topo.add_processor("up", || Box::new(Upper), ["src"]);
//! topo.add_sink("out", "out", ["up"], StringSerde, StringSerde);
//! let built = topo.build("my-app").unwrap();
//!
//! let mut driver = TopologyTestDriver::new(built);
//! let mut input = driver.input_topic::<String, String>("in", StringSerde, StringSerde);
//! let mut output = driver.output_topic::<String, String>("out", StringSerde, StringSerde);
//! input.pipe(Some("k".into()), "hello".into(), 0);
//! assert_eq!(output.read(), Some((Some("k".to_string()), "HELLO".to_string())));
//! ```
```
(Match the final `input_topic`/`output_topic` signatures from Task 8.)

- [ ] **Step 2: Full verification**

Run: `cargo test -p crabka-client-streams` → ALL pass (unit incl. processor/test_driver + the existing membership integration + golden_frame + doctests).
Run: `cargo fmt -p crabka-client-streams -- --check` → clean (else `cargo fmt -p crabka-client-streams` and include).
Run: `cargo clippy -p crabka-client-streams --all-targets -- -D warnings` → clean.
Run: `cargo build --workspace` → ok.

- [ ] **Step 3: Commit**
```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe add crates/client-streams/src/lib.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/sweet-faraday-835ffe -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "docs(streams-client): Processor API quick-start + 2a verification"
```

---

## Self-review

**Spec coverage (2a portion):**
- §3.1 traits (Serde/Record/Processor/ProcessorSupplier/ProcessorContext) → Tasks 1, 2, 4. ✓
- §3.2 erased records + driver-loop forwarding + 3 node adapters → Tasks 3, 5, 6. ✓
- §3.3 build-time TypeId validation + `rustc`-style `TypeMismatch` diagnostic → Task 7. ✓
- §4 builder evolution to typed `add_*`; wire `Topology` unchanged; #1 tests migrated → Task 7 (+ golden-frame migration). ✓
- §6 `TopologyTestDriver` + typed input/output topics + repartition loopback → Task 8. ✓
- §7 gate 1 (TopologyTestDriver unit tests) + gate 2 (erased-graph unit tests) → Tasks 5,6,8. ✓ (Gate 3 broker integration + gate 4 are 2b.)
- §9 doc example + clippy/fmt/golden-frame → Task 9. ✓

**Deferred to 2b (not in this plan):** runtime/{task,thread,app}.rs, fetch/produce/commit, rebalance, the in-process-broker `KafkaStreams` integration test, the at-least-once/rebalance gate. The `client-streams-integration` codecov flag already covers `--tests`; 2b adds the integration test under it.

**Placeholder note:** Tasks 6 and 8 contain explicitly-labelled *structure illustrations* (the `unimplemented!()`/`...` in the test-driver, the `rec_take` fan-out note) with precise instructions on how to complete them and why the straightforward path was left to the implementer (borrow-checker-dependent shapes + the serde-recovery decision). These are flagged, not silent — each says exactly what to build. The recommended resolution (pass serdes to `input_topic`/`output_topic`) is stated.

**Type consistency:** `Record<K,V>` (Task 2), `ErasedRecord`/`Dispatch`/`OutputRecord`/`ProcessorError` (Task 3), `ProcessorContext::{new,forward,record_context}` (Task 4), `ErasedNode::{name,process,input_kv,output_kv,*_names}` (Task 5), `Graph::{pipe,take_output}` + `GraphSource` (Task 6), `NodeFactory`/`BuiltTopology::instantiate_subtopology` (Task 7), `TopologyTestDriver`/`TestInputTopic`/`TestOutputTopic` (Task 8) are referenced consistently across tasks. `Clone` bounds on forwarded `KOut,VOut` (Task 4) propagate to processor adapters (Task 5) and the builder's `add_processor` (Task 7). ✓

**Known risk:** the erased graph's borrow-checker shape (two-lifetime `ProcessorContext`, `mem::take` on `children`, source multi-child fan-out by re-deserialize) may need iteration against the compiler; the per-task TDD loop catches it. The contracts (assertions) are exact; the internal plumbing is the implementer's to make compile.
