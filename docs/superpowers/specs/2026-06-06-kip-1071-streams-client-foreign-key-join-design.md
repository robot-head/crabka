# KIP-1071 Streams Client — Foreign-Key KTable Join (KIP-213)

**Date:** 2026-06-06
**Status:** Design approved, pending spec review
**Scope:** Many-to-one `KTable<K,VA>` ↔ `KTable<KO,VB>` join (inner + left), the
single largest remaining DSL parity gap.
**Builds on:** #4 DSL, 4c-i `Change`, 4c-ii/4c-iii KTable joins (copartition +
`connect_processor_store` + dual-processor patterns), 4d-ii `ValueAndTimestamp`
codec + byte store. Branch `streams-fk-join` off `origin/main` (which has IQ #432).
The open EOS PR #429 is orthogonal (runtime vs DSL/store) → trivial reconcile.

---

## 1. Goal & non-goals

### Goal

Foreign-key (many-to-one) join in the DSL. Many rows of the **left** table (`this`,
primary key `K`, value `VA`) reference **one** row of the **right** table (`other`,
primary key `KO`, value `VB`) via a foreign key `KO = fk(VA)` extracted from the
left value:

```rust
impl<K, VA> KTable<K, VA> {
    /// Inner FK join: emits `VR` only when the referenced right row exists.
    pub fn join_on_foreign_key<KO, VB, VR, FKE, J, KOS>(
        &self,
        other: &KTable<KO, VB>,
        fk_extractor: FKE,   // Fn(&VA) -> KO          + Clone + Send + Sync + 'static
        joiner: J,           // Fn(&VA, &VB) -> VR      + Clone + Send + Sync + 'static
        fk_serde: KOS,       // Serde<KO>               (subscription topic key + CombinedKey FK part)
    ) -> KTable<K, VR>
    where KO: Any + Send + Clone, VB: Any + Send + Clone, VR: Any + Send + Clone;
    // (No `Hash` bound on KO — partitioning + CombinedKey range use the serialized
    //  fk bytes, not Rust hashing.)

    /// Left FK join: emits `VR` whenever the **left** row exists; right optional.
    pub fn left_join_on_foreign_key<KO, VB, VR, FKE, J, KOS>(
        &self,
        other: &KTable<KO, VB>,
        fk_extractor: FKE,   // Fn(&VA) -> KO
        joiner: J,           // Fn(&VA, Option<&VB>) -> VR
        fk_serde: KOS,       // Serde<KO>
    ) -> KTable<K, VR>;
}
```

- A change on **either** side recomputes the affected join result(s) and emits a
  `Change<VR>` to the result KTable.
- A row that drops out (FK now references a missing right row under inner, or the
  left row is deleted) emits a **tombstone** (`Change{new: None}`) via 4c-i `Change`.
- Both input tables must be **materialized** (have a store + source topic): the
  resolver reads `A.get(K)`; the subscription-join reads `B.get(KO)`.
- Byte-exact vs JVM Kafka Streams 4.1 — captured goldens for the topology
  description, **both** repartition topics, the subscription changelog, and the
  result topic.

### Method naming rationale

Rust cannot overload the existing equi-`join` (the right table has a *different*
key type `KO` plus an extractor `FKE`, so the signatures are incompatible). The
JVM method is `KTable.join(other, foreignKeyExtractor, joiner, …)`; we name the
distinct Rust methods `join_on_foreign_key` / `left_join_on_foreign_key`
(descriptive, matches the KIP-213 terminology). The wire topology is unaffected by
the Rust method name.

### Non-goals (deferred)

- **`outer` FK join** — KIP-213 defines none. No outer variant.
- **Materialized result** (a result store/changelog) — the result KTable is a
  value-getter, consistent with the existing KTable–KTable join (4c-iii).
- **`TableJoined` partitioner / custom partitioner** — the subscription and
  response topics use the default key-hash partitioner.
- **Self-join** (`a.join_on_foreign_key(a, …)`), **versioned-table** temporal FK
  semantics (KIP-923), and a **non-materialized** input table.
- **Bloom-filter / hashing version negotiation** beyond the single wrapper version
  Streams 4.1 emits (greenfield — we match exactly one version, no multi-version
  replay support).

---

## 2. Topology — the FK-join subgraph

The JVM splits the join across the two tables' subtopologies, bridged by **two
internal repartition topics**. ASCII (left table A keyed by `K`/`VA`, right table B
keyed by `KO`/`VB`):

```
LEFT subtopology  (co-partitioned by K — where table A lives)
  A.Change<VA> ─► SubscriptionSend ─► SINK ─► [subscription-registration topic]   (partitioned by KO)
                                                            │
RIGHT subtopology (co-partitioned by KO — where table B lives)                     ▼
  [subscription-registration] ─► SOURCE ─► SubscriptionReceive ─► (SUBSCRIPTION STORE: CombinedKey<KO,K>)
                                                    └─► SubscriptionJoin ──┐
  B.Change<VB> ─► ForeignTableJoin (range-scan store by KO prefix) ────────┤
                                                                           ▼
                                                 SINK ─► [subscription-response topic]  (partitioned by K)
                                                                           │
LEFT subtopology again (co-partitioned by K)                               ▼
  [subscription-response] ─► SOURCE ─► SubscriptionResolver ─► result KTable<K,VR>
```

### Two driving paths

**Left-driven** (A updated, FK changed, or A deleted). `SubscriptionSend` extracts
`KO_new = fk(newVA)` and `KO_old = fk(oldVA)`, and emits one or two
`SubscriptionWrapper` records keyed by foreign key:

| Transition | Emit to `KO_new` | Emit to `KO_old` (if `KO_old != KO_new` or A deleted) |
|---|---|---|
| A created/updated (newVA present) | `{instruction, hash(newVA), pk=K}` | unsubscribe `{delete-instruction, hash=null, pk=K}` |
| A deleted (newVA = None) | — | `{delete-instruction, hash=null, pk=K}` |

The exact `instruction` differs for inner vs left (see §3). `SubscriptionReceive`
upserts (or deletes) the wrapper into the subscription store keyed by
`CombinedKey{KO, K}`; `SubscriptionJoin` reads `VB = B.get(KO)` and emits a
`SubscriptionResponseWrapper` keyed back by `K`.

**Right-driven** (B updated/deleted for `KO`). `ForeignTableJoin` range-scans the
subscription store by the `KO` **prefix** and re-emits a `SubscriptionResponseWrapper`
keyed by `K` for **every** subscribed `K` (each carrying the stored subscription's
hash and the new `VB`).

### Staleness check — what makes FK join correct

The two repartition hops are asynchronous: between a subscription being sent and
its response returning, the left value may have changed. Each
`SubscriptionResponseWrapper` carries the **hash of the left value at send time**.
`SubscriptionResolver` re-reads the **current** `VA = A.get(K)`, re-hashes it, and:

- **hash mismatch** ⇒ the left row changed in flight ⇒ **drop** the response (a
  newer subscription is already in flight and will produce the authoritative
  result);
- **hash match** ⇒ apply the joiner. Inner: emit `Change{new: Some(joiner(VA, vb))}`
  when `vb` present, else tombstone. Left: emit `joiner(VA, vb_or_none)`.

This is the canonical KIP-213 design; matching it byte-for-byte is the point of the
capture-first goldens.

---

## 3. Codecs (byte-exact, capture-confirmed)

All four formats are **app-internal** (carried on internal repartition topics + the
subscription changelog, never the Kafka client wire protocol). Per the approved
"JVM-exact" decision we match them exactly and validate via capture. The capture
task (§6, Batch 3) is the **oracle** for every byte marked "confirm"; we do **not**
hand-author a golden.

### 3.1 `CombinedKey<KO, K>` (`dsl/processors/fk/combined_key.rs`)

Serialized layout (JVM `CombinedKeySchema`):

```
[ foreignKeyLen : 4 bytes BE ] [ foreignKeyBytes ] [ primaryKeyBytes ]
```

- **Range prefix** for "all primary keys subscribed to `KO`" = `[fkLen:4BE][fkBytes]`
  — a strict byte prefix, scanned via the existing `ByteKeyValueStore::range(lo, hi)`
  using the `hi = prefix ++ 0xFF…`/successor trick already used by IQ range and the
  window store.
- Decode splits at `4 + fkLen`.

### 3.2 `SubscriptionWrapper` (`dsl/processors/fk/subscription.rs`)

```
[ version : 1 ] [ instruction : 1 ] [ hash : 16 bytes OR absent ] [ primaryKeyBytes … ]
```

- `version`: the single byte Streams 4.1 emits (confirm exact value via capture; the
  JVM `SubscriptionWrapper.Instruction`/version constants).
- `instruction` enum ordinal (confirm ordinals via capture):
  - `PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE` — inner, A present.
  - `PROPAGATE_NULL_IF_NO_FK_VALUE_AVAILABLE` — left, A present.
  - `DELETE_KEY_AND_PROPAGATE` — left, A deleted / FK changed (emit a tombstone).
  - `DELETE_KEY_NO_PROPAGATE` — inner, A deleted / FK changed (no downstream row).
- `hash`: 16-byte Murmur3-128 of the serialized left value, present iff the wrapper
  carries a live subscription (absent on delete instructions).
- The presence/absence of a trailing `primaryPartition : 4BE` field is **confirmed
  by capture** — if Streams 4.1 writes it, we include it; if not, we omit it. (No
  back-compat: we match exactly one observed layout.)

### 3.3 `SubscriptionResponseWrapper` (`dsl/processors/fk/subscription.rs`)

```
[ version : 1 ] [ hash : 16 bytes OR absent ] [ foreignValueBytes … OR null-marker ]
```

- `hash`: the originating subscription's left-value hash (echoed back for the
  staleness check). Null only on the paths the JVM leaves null (confirm).
- `foreignValue`: the serialized right value `VB`, or a null marker when the right
  row is absent (left join / inner miss). The presence of a `primaryPartition` field
  is **confirmed by capture** (same as §3.2).

### 3.4 Murmur3-128 (`dsl/processors/fk/murmur3.rs`)

JVM `org.apache.kafka.streams.state.internals.Murmur3.hash128(byte[])` with seed 0
(the 128-bit x64 variant). Hand-rolled (~60 lines), returning a `[u8; 16]`.
Validated against hashes captured from the JVM harness (Batch 3) and a couple of
hand-checked vectors. This is the only "from scratch" cryptographic-style routine;
it is small, deterministic, and fully test-covered.

### 3.5 Subscription store value

The subscription store holds `ValueAndTimestamp<SubscriptionWrapper>` — reuse the
existing `wrap_value(record_ts, &subscription_bytes)` / `unwrap_value` codec from
4d-ii (`store/window_schema.rs`), so the changelog value framing matches the JVM
(`recordTs:8BE ‖ subscriptionWrapperBytes`).

---

## 4. State store & topology wiring

### 4.1 `SubscriptionBytesStore` (`store/fk_subscription.rs`)

A typed store over the existing `ByteKeyValueStore` byte backend (the same pattern
as `WindowBytesStore`/`SessionBytesStore`):

- **key**: `CombinedKey<KO, K>` bytes; **value**: `ValueAndTimestamp<SubscriptionWrapper>`.
- `put(combined_key, wrapper, record_ts)`, `delete(combined_key)`,
  `range_by_foreign(ko_bytes) -> Vec<(K-bytes, SubscriptionWrapper)>` (prefix scan
  via §3.1).
- Implements `StateStore` (name, flush, changelog, `apply_changelog`, `clear`,
  `as_any_mut`, `set_logging`). Changelog-backed, **compact** cleanup policy
  (a keyed subscription store, not windowed — no retention/delete).
- Registered via a new `TopologyBuilder::add_fk_subscription_store::<K, KO, …>` that
  captures the `K` + `KO` serdes (for `CombinedKey`) and the subscription-wrapper
  codec, and connects to the receive + foreign-table-join processors.

### 4.2 Repartition topics

Two internal repartition topics declared via the existing
`TopologyBuilder::add_repartition_topic` + sink/source pairs:

- **subscription-registration** — key `KO` (`fk_serde`), value `SubscriptionWrapper`,
  partitioned by `KO`. Created by the left subtopology's `SubscriptionSend` sink and
  consumed by the right subtopology's source.
- **subscription-response** — key `K` (left table's key serde), value
  `SubscriptionResponseWrapper`, partitioned by `K`. Created by the right
  subtopology's sink and consumed by the left subtopology's resolver source.

Topic names follow the JVM convention
`<applicationId>-<nodePrefix>-subscription-registration-topic` /
`…-subscription-response-topic`; the exact `nodePrefix` strings are **confirmed by
capture** (Batch 3) and recorded as `names.rs` constants.

### 4.3 Node-name prefixes (`dsl/names.rs`)

New constants for the FK-join nodes (exact JVM strings confirmed by capture):
`SubscriptionSend`, `SubscriptionReceive`, `SubscriptionJoin`, `ForeignTableJoin`,
`SubscriptionResolver`, the two `KTABLE-SINK-`/`KTABLE-SOURCE-` repartition nodes,
and the subscription store name prefix. Placeholders during Batch 2, pinned to the
captured values in Batch 3 (this is exactly how 4d-ii/4d-iii pinned window/join
node names).

---

## 5. Processors (`dsl/processors/fk/*.rs`)

Five erased processors, all on `Change`-typed edges where the JVM is `Change`-typed:

1. **`SubscriptionSendProcessor`** `Processor<K, Change<VA>, KO, SubscriptionWrapper>` —
   computes `KO_new`/`KO_old`, hashes the live left value (Murmur3-128 of the
   serialized `VA`), selects the instruction (inner vs left), forwards one/two
   wrappers keyed by `KO`. Carries `fk_extractor`, `VA` serde (for the hash),
   `is_left` flag.
2. **`SubscriptionReceiveProcessor`** `Processor<KO, SubscriptionWrapper, CombinedKey, …>` —
   on a wrapper for `KO` + `pk`, upsert/delete `CombinedKey{KO,pk}` in the
   subscription store; forward the stored wrapper downstream to `SubscriptionJoin`.
3. **`SubscriptionJoinProcessor`** — reads `VB = B.get(KO)`, builds a
   `SubscriptionResponseWrapper{hash, foreignValue}` keyed by `pk`, applying the
   instruction's propagate/delete semantics; forwards keyed by `K`.
4. **`ForeignTableJoinProcessor`** `Processor<KO, Change<VB>, K, SubscriptionResponseWrapper>` —
   on a `B` change for `KO`, range-scan the subscription store by the `KO` prefix
   (§3.1); for each subscribed `pk`, emit a response wrapper with the **stored**
   subscription hash + the new `VB`.
5. **`SubscriptionResolverJoinProcessor`** `Processor<K, SubscriptionResponseWrapper, K, Change<VR>>` —
   read `VA = A.get(K)`; re-hash; **drop on hash mismatch** (stale); on match apply
   the joiner (inner: `Some(joiner(va, vb))` when `vb` present else tombstone; left:
   `joiner(va, vb_or_none)`) and forward `Change<VR>` to the result KTable.

A **unified inner/left rule** (like the 4c-iii `JoinKind { a_required, b_required }`
result rule) parameterizes the instruction selection in (1) and the null handling in
(3)/(5); inner vs left differ only there.

The joiner is stored in outer form `Fn(&VA, Option<&VB>) -> VR` (inner wraps with
`expect` on `Some`), matching the 4c-iii joiner-wrapping convention.

---

## 6. DSL lowering & decomposition

### 6.1 Lowering (`dsl/ktable.rs`, `dsl/graph.rs`, `dsl/lower.rs`, `dsl/names.rs`)

`join_on_foreign_key`/`left_join_on_foreign_key` record the logical nodes + lowering
thunks (same pattern as the equi-join `join_impl`):

- Require both inputs materialized:
  `a_store = self.store_name().expect("FK join: left table must be materialized")`,
  `b_store = other.store_name().expect("FK join: right table must be materialized")`.
- Capture the serdes needed at lowering: `K` + `VA` from `self`, `KO` from
  `fk_serde`, `VB` from `other` (the FK-join op extends the existing serde-capture
  KTable carries for `suppress`, or takes them explicitly — final choice recorded in
  the plan; default is "K/VA/VB captured from the tables, KO explicit").
- Build the subgraph: the five processors, the subscription store, the two
  repartition sink/source pairs (with the right key serdes), connecting each
  processor to the stores it reads (`connect_processor_store` from 4c-ii) so
  grouping pulls the left subtopology (A + send + resolver), the right subtopology
  (B + receive + subscription-join + foreign-table-join), and the repartition
  boundaries into the JVM-matching subtopology layout.
- The result KTable's underlying node is the `SubscriptionResolver` (value-getter,
  unmaterialized).

### 6.2 Three internal batches (one PR, `streams-fk-join`)

Per the CLAUDE.md execution rule, tasks within a batch whose file sets don't overlap
run as parallel subagents.

**Batch 1 — codecs + store** (no DSL; fully parallel):
- T1 `murmur3.rs` — Murmur3-128 + unit vectors.
- T2 `combined_key.rs` — `CombinedKey` encode/decode + prefix + round-trip tests.
- T3 `subscription.rs` — `SubscriptionWrapper` + `Instruction` enum + codec tests.
- T4 `subscription.rs` — `SubscriptionResponseWrapper` + codec tests (same file as
  T3 ⇒ T3 then T4 sequential within the batch, or one task covering both wrappers).
- T5 `store/fk_subscription.rs` — `SubscriptionBytesStore` (`StateStore` +
  prefix-range) + contract test (in-memory + Turso backends).

**Batch 2 — processors + wiring**:
- T6 `dsl/processors/fk/*.rs` — the five processors + the unified inner/left rule +
  per-processor unit tests (driven against the byte store directly).
- T7 `topology/builder.rs` — `add_fk_subscription_store` + repartition sink/source
  helpers + subtopology grouping test.
- T8 `dsl/names.rs` + `dsl/lower.rs` + `dsl/graph.rs` — node prefixes (placeholder),
  lowering assembly, subtopology-shape assertion.

**Batch 3 — DSL + capture + goldens + e2e + docs**:
- T9 `dsl/ktable.rs` — `join_on_foreign_key` / `left_join_on_foreign_key` ops +
  `TopologyTestDriver` execution tests (inner + left, incl. FK change, right-side
  update re-emit, left delete tombstone, **staleness drop**).
- T10 `tests/jvm-capture/.../ForeignKeyJoinBehavior.java` + `run.sh --fkjoin` mode +
  capture fixtures; **pin** the node-name prefixes, topic names, wrapper version
  bytes, instruction ordinals, and `primaryPartition` presence from the capture.
- T11 goldens **#15 (inner)** + **#16 (left)**: assert topology description, the
  subscription-registration + subscription-response repartition topic bytes, the
  subscription changelog, and the result topic — all byte-exact vs the capture.
- T12 in-process broker e2e (FK join over a real broker, restart-restore of the
  subscription store) + docs (`lib.rs` `## Foreign-key joins` section) + final
  `cargo fmt --check`, `cargo clippy --workspace --all-targets -D warnings`, full
  `client-streams` suite.

---

## 7. Error handling & edge cases

- **Unmaterialized input** ⇒ `expect` panic at build time with a clear message
  (consistent with the equi-join).
- **FK change** (left value's foreign key changes) ⇒ unsubscribe from `KO_old`
  (delete instruction) + subscribe to `KO_new`; the old `KO`'s subscription is
  removed from the store so a later `KO_old` right-side change no longer re-emits.
- **Left delete** ⇒ delete-and-propagate (left) or delete-no-propagate (inner);
  resolver emits a tombstone for the left form.
- **Stale response** ⇒ dropped at the resolver via the hash mismatch (the
  correctness keystone; explicitly tested in T9).
- **Right row absent** ⇒ inner emits a tombstone / no row; left emits
  `joiner(VA, None)`.
- **Restart** ⇒ the subscription store restores from its compact changelog
  (clean-slate-then-replay, like every other store); both input tables restore from
  their own changelogs. T12 covers this.

---

## 8. Testing strategy

- **Codec unit tests** (Batch 1): round-trip + exact-bytes for `CombinedKey`,
  both wrappers, and Murmur3 vectors.
- **Processor unit tests** (Batch 2): each processor driven directly against an
  in-memory subscription/byte store — subscribe, right-update range re-emit,
  unsubscribe, stale-drop.
- **DSL execution tests** (Batch 3, `TopologyTestDriver`): inner + left end-to-end,
  including FK change, right-side update, left delete tombstone, and the staleness
  drop.
- **JVM goldens** #15/#16 (Batch 3): byte-exact topology + both repartition topics +
  changelog + result vs a real Docker JVM Streams 4.1 capture. **Capture-first** —
  no fixture is hand-authored.
- **Broker e2e** (Batch 3): FK join over an in-process broker + subscription-store
  restart-restore.

---

## 9. Files

**New:**
- `crates/client-streams/src/dsl/processors/fk/mod.rs`
- `crates/client-streams/src/dsl/processors/fk/murmur3.rs`
- `crates/client-streams/src/dsl/processors/fk/combined_key.rs`
- `crates/client-streams/src/dsl/processors/fk/subscription.rs` (both wrappers + enum)
- `crates/client-streams/src/dsl/processors/fk/processors.rs` (the five processors)
- `crates/client-streams/src/store/fk_subscription.rs` (`SubscriptionBytesStore`)
- `crates/client-streams/tests/fk_join_golden.rs`
- `crates/client-streams/tests/fk_join_broker.rs`
- `crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/ForeignKeyJoinBehavior.java`
- `crates/client-streams/tests/testdata/fk_join/{inner,left}.json` (captured)

**Modified:**
- `crates/client-streams/src/dsl/ktable.rs` (the two FK-join ops + lowering)
- `crates/client-streams/src/dsl/names.rs` (FK node prefixes + topic suffixes)
- `crates/client-streams/src/dsl/lower.rs`, `dsl/graph.rs` (subgraph assembly)
- `crates/client-streams/src/dsl/processors/mod.rs` (mod fk)
- `crates/client-streams/src/store/mod.rs` (mod fk_subscription)
- `crates/client-streams/src/topology/builder.rs` (`add_fk_subscription_store` +
  repartition helpers)
- `crates/client-streams/src/lib.rs` (re-exports + `## Foreign-key joins` doc)
- `crates/client-streams/tests/jvm-capture/run.sh` (`--fkjoin` mode)

---

## 10. Open items deliberately resolved by capture (not in this doc)

These are intentionally **not** specified as literal bytes here; the Batch-3 capture
is the oracle and pins them (the same discipline as 4d-ii/4d-iii/suppress-D):

1. `SubscriptionWrapper` / `SubscriptionResponseWrapper` exact `version` byte(s).
2. `Instruction` enum ordinals.
3. Presence + position of any `primaryPartition` field in either wrapper.
4. The exact node-name prefixes + repartition topic-name segments.
5. Murmur3 endianness of the emitted 16-byte hash (x64 produces two longs; confirm
   byte order from the capture).
