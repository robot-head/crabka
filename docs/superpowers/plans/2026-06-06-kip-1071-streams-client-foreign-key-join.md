# Foreign-Key KTable Join (KIP-213) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the many-to-one foreign-key `KTable<K,VA>`↔`KTable<KO,VB>` join (inner + left) to the Crabka Kafka-Streams DSL, byte-exact vs JVM Kafka Streams 4.1.

**Architecture:** A faithful mirror of the JVM FK-join subgraph — five processors (subscription **send** on the left; **receive** / **subscription-join** / **foreign-table-join** on the right; **resolve** back on the left), two internal repartition topics (subscription-registration keyed by `KO`, subscription-response keyed back by `K`), and one changelog-backed subscription store keyed by `CombinedKey<KO,K>`. A Murmur3-128 hash of the left value, carried through the response, gives the staleness check that makes the async two-hop join correct. All internal byte formats (`CombinedKey`, `SubscriptionWrapper`, `SubscriptionResponseWrapper`, the hash) are byte-exact, pinned and validated by a **gating JVM capture** (`behavior.json` + wire goldens).

**Tech Stack:** Rust (`crabka-client-streams`), `async-trait`, `bytes`, `tokio`; existing DSL/store/processor infrastructure (`ByteKeyValueStore`, `StateStore`, `Processor`/`ProcessorContext`, `LowerState`, `TopologyTestDriver`); JVM capture harness (Docker + Kafka Streams 4.1, `tests/jvm-capture/`).

**Branch:** `streams-fk-join` off `origin/main` (already created). Commit with
`git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`. Work
ONLY in the worktree `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`.

---

## Dependency shape & batching

FK join is an inherent **dependency chain**: capture → codecs → store → processors →
lowering/DSL → goldens. Most tasks edit overlapping or downstream files, so the
executor dispatches them **sequentially**. The one genuine parallel pair is at the
end (T7 wire/byte goldens ∥ T8 broker-e2e+docs — disjoint test files).

| Batch | Tasks | Parallel? |
|---|---|---|
| 1 — capture + codecs + store | T1 (gating capture) → T2 → T3 → T4 | sequential (chain) |
| 2 — processors | T5 | single task |
| 3 — DSL + goldens + e2e | T6 → (T7 ∥ T8) | T7 ∥ T8 only |

**Capture-first rule (non-negotiable):** T1 runs the JVM and commits real fixtures.
Do **not** hand-author any `*.json` fixture or hard-code a "guessed" wrapper byte.
Every byte marked "PINNED BY T1" is read from the committed capture; the codec/
processor tests assert against the committed fixture, so they fail until the
implementation matches the JVM exactly.

---

## File structure

**New files:**
- `crates/client-streams/src/dsl/processors/fk/mod.rs` — FK submodule root + re-exports.
- `crates/client-streams/src/dsl/processors/fk/murmur3.rs` — Murmur3-128 x64.
- `crates/client-streams/src/dsl/processors/fk/combined_key.rs` — `CombinedKey` codec.
- `crates/client-streams/src/dsl/processors/fk/subscription.rs` — `SubscriptionWrapper`, `SubscriptionResponseWrapper`, `Instruction`, codecs.
- `crates/client-streams/src/dsl/processors/fk/processors.rs` — the five processors + the unified inner/left rule.
- `crates/client-streams/src/store/fk_subscription.rs` — `SubscriptionBytesStore` (typed store over the byte backend).
- `crates/client-streams/tests/fk_join_golden.rs` — wire-topology + byte-parity goldens.
- `crates/client-streams/tests/fk_join_broker.rs` — in-process broker e2e.
- `crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/ForeignKeyJoinBehavior.java` — byte-behavior capture.
- `crates/client-streams/tests/testdata/fk_join/behavior.json` — captured byte/semantic oracle (committed by T1).
- `crates/client-streams/tests/testdata/golden/dsl/fk_join_inner.topology.json`, `fk_join_left.topology.json` — captured wire goldens (committed by T1).

**Modified files:**
- `dsl/processors/mod.rs` — `pub(crate) mod fk;`.
- `dsl/ktable.rs` — `join_on_foreign_key` / `left_join_on_foreign_key` + lowering.
- `dsl/names.rs` — FK node-name prefixes + topic suffixes (PINNED BY T1).
- `store/mod.rs` — `pub(crate) mod fk_subscription;`.
- `store/registry.rs` — `get_fk_subscription` downcast.
- `processor/api.rs` — `ProcessorContext::get_fk_subscription_store`.
- `topology/builder.rs` — `add_fk_subscription_store`.
- `lib.rs` — re-exports + `## Foreign-key joins` doc section.
- `tests/jvm-capture/run.sh` — `--fkjoin` mode.
- `tests/jvm-capture/src/main/java/crabka/capture/Capture.java` — `fkJoinInner()` / `fkJoinLeft()` topology builders.

---

## Task 1 (GATING): JVM capture — wire goldens + byte behavior

**Why first:** the exact wrapper version byte, `Instruction` ordinals, presence/position
of a `primaryPartition` field, the Murmur3 output byte-order, the FK node-name
prefixes, and the repartition/changelog topic names are all empirically defined by
JVM Streams 4.1. This task pins them into committed fixtures that every later task
asserts against. **Requires Docker.**

**Files:**
- Modify: `crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/Capture.java`
- Create: `crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/ForeignKeyJoinBehavior.java`
- Modify: `crates/client-streams/tests/jvm-capture/run.sh`
- Output (committed): `crates/client-streams/tests/testdata/golden/dsl/fk_join_inner.topology.json`, `fk_join_left.topology.json`, `crates/client-streams/tests/testdata/fk_join/behavior.json`

- [ ] **Step 1: Add the two FK wire topologies to `Capture.java`**

Add two builders next to `ktableKtableJoin()` and register them in the same
`write(...)` dispatch list the other 17 use (so they emit
`fk_join_inner.topology.json` / `fk_join_left.topology.json`):

```java
/**
 * fk_join_inner: table("a", sa).join(table("b", sb), fkExtractor, joiner) — KIP-213
 * many-to-one foreign-key join. Left value "VA" references right PK via fkExtractor.
 * Produces: left subtopology (a-source, subscription-send, response-source, resolve),
 * right subtopology (b-source, subscription-source, receive, subscription-join,
 * foreign-table-join), two repartition topics (subscription-registration keyed by KO,
 * subscription-response keyed by K), and the subscription store changelog.
 */
static Topology fkJoinInner() {
    StreamsBuilder b = new StreamsBuilder();
    KTable<String, String> a = b.table("a",
        Consumed.with(Serdes.String(), Serdes.String()),
        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("sa"));
    KTable<String, String> bt = b.table("b",
        Consumed.with(Serdes.String(), Serdes.String()),
        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("sb"));
    a.join(bt,
            (String va) -> va,                       // foreign-key extractor: VA -> KO
            (String va, String vb) -> va + vb,        // joiner
            Materialized.with(Serdes.String(), Serdes.String()))
        .toStream()
        .to("out", Produced.with(Serdes.String(), Serdes.String()));
    return b.build(optimizedProps());
}

/** fk_join_left: identical but `leftJoin` (right optional → joiner sees null vb). */
static Topology fkJoinLeft() {
    StreamsBuilder b = new StreamsBuilder();
    KTable<String, String> a = b.table("a",
        Consumed.with(Serdes.String(), Serdes.String()),
        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("sa"));
    KTable<String, String> bt = b.table("b",
        Consumed.with(Serdes.String(), Serdes.String()),
        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("sb"));
    a.leftJoin(bt,
            (String va) -> va,
            (String va, String vb) -> va + (vb == null ? "_" : vb),
            Materialized.with(Serdes.String(), Serdes.String()))
        .toStream()
        .to("out", Produced.with(Serdes.String(), Serdes.String()));
    return b.build(optimizedProps());
}
```

Register them so `--gradle`/`--javac` writes the two `.topology.json` files (follow
the exact registration list pattern already in `Capture.java` — add
`write(outDir, "fk_join_inner", fkJoinInner());` and
`write(outDir, "fk_join_left", fkJoinLeft());`).

- [ ] **Step 2: Write `ForeignKeyJoinBehavior.java` (byte + semantic oracle)**

Mirror the `--bufval` reflective-serialization approach (dump exact bytes for known
inputs) AND the `--iq`/`--punctuation` behavior approach (drive a
`TopologyTestDriver` and record input→output sequences). The program writes ONE
`behavior.json` containing:

1. **`combined_key`** examples: for `(fk, pk)` pairs, the hex of
   `CombinedKeySchema.toBytes(fk, pk)` and the hex of its prefix
   (`prefixBytes(fk)`), via the internal
   `org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKeySchema`.
2. **`murmur3`** examples: for known byte inputs, the 16-byte hex of
   `org.apache.kafka.streams.state.internals.Murmur3.hash128(input)` serialized the
   same way the FK code stores it (capture the exact byte order).
3. **`subscription_wrapper`** examples: for each `Instruction` and a known
   `(hash, primaryKey)`, the hex of `SubscriptionWrapperSerde.serializer().serialize(...)`.
   Also record `Instruction.values()` with their ordinals/byte and the wrapper
   `VERSION` constant (read the static field reflectively).
4. **`subscription_response_wrapper`** examples: for `(hash, foreignValue)` and the
   null-foreign-value case, the hex of `SubscriptionResponseWrapperSerde.serialize(...)`.
5. **`inner_sequence`** / **`left_sequence`**: drive a `TopologyTestDriver` over the
   `fkJoinInner()` / `fkJoinLeft()` topologies with this exact record script and
   record `(topic,key,value,timestamp)` for every output record on `out`:
   ```
   a:(k1,"A")          // left arrives, no right yet  → inner: nothing; left: "A_"
   b:(A,"X")           // right arrives for fk "A"     → inner: "AX";    left: "AX"
   a:(k1,"A2")         // left value changes (same fk) → re-emit "A2X" (or left "A2X")
   a:(k2,"A")          // second left row, same fk "A" → "AX" for k2
   b:(A,"Y")           // right update → re-emit for k1 AND k2 (range scan)
   a:(k1,"B")          // left fk changes A->B (B absent) → inner: tombstone k1; left: "B_"
   a:(k1,null)         // left delete → tombstone k1
   ```
   Use `Serdes.String()` everywhere; `b.build()` (NOT optimized) for the TTD run so
   store names are stable. This sequence exercises: first-match, left-value-change
   re-emit, multi-subscriber range re-emit on right update, FK change, and delete.
   It is the **behavioral oracle** for T5's processor semantics.
6. **`store_names`** / **`topic_names`** / **`node_names`**: from
   `fkJoinInner().describe().toString()` and the internal topology, record the FK
   processor node-name prefixes, the subscription store name, and the
   subscription-registration / subscription-response repartition topic names + the
   subscription changelog topic name. These PIN `dsl/names.rs` (T6).

Write JSON with the same hand-rolled style as `BufferValueCapture.java` /
`InteractiveQueryBehavior.java` (look at those two for the exact JSON-emit helpers).

- [ ] **Step 3: Add the `--fkjoin` mode to `run.sh`**

Clone the `--iq` case verbatim (it has the right jar set: kafka-streams +
streams-test-utils + clients + rocksdb + slf4j), changing only the output dir +
class:

```bash
  --fkjoin)
    # Pin the JVM FK-join (KIP-213) internal byte formats + behavior:
    # CombinedKey / SubscriptionWrapper / SubscriptionResponseWrapper / Murmur3 hex,
    # plus inner/left TopologyTestDriver input->output sequences, into
    # testdata/fk_join/behavior.json (the codec + processor oracle).
    TESTS_DIR="$(cd "$HERE/.." && pwd)"
    docker run --rm \
      -v "$TESTS_DIR":/tests -w /tests/jvm-capture \
      "$JDK_IMAGE" bash -c '
        set -euo pipefail
        M=https://repo1.maven.org/maven2
        J=/tmp/j; mkdir -p "$J"
        get() { f=$(basename "$2"); [ -f "$J/$f" ] || curl -sSfL "$M/$1/$2" -o "$J/$f"; }
        get org/apache/kafka/kafka-streams/'"$KAFKA_VERSION"' kafka-streams-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-streams-test-utils/'"$KAFKA_VERSION"' kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-clients/'"$KAFKA_VERSION"' kafka-clients-'"$KAFKA_VERSION"'.jar
        get org/slf4j/slf4j-api/1.7.36 slf4j-api-1.7.36.jar
        get org/rocksdb/rocksdbjni/'"$ROCKSDB_VERSION"' rocksdbjni-'"$ROCKSDB_VERSION"'.jar
        CP="$J/kafka-streams-'"$KAFKA_VERSION"'.jar:$J/kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar:$J/kafka-clients-'"$KAFKA_VERSION"'.jar:$J/rocksdbjni-'"$ROCKSDB_VERSION"'.jar"
        RT="$CP:$J/slf4j-api-1.7.36.jar"
        mkdir -p /tmp/build /tests/testdata/fk_join
        javac -cp "$CP" -d /tmp/build src/main/java/crabka/capture/ForeignKeyJoinBehavior.java
        java -cp "/tmp/build:$RT" crabka.capture.ForeignKeyJoinBehavior /tests/testdata/fk_join
      '
    ;;
```

Also add `--fkjoin` to the usage line.

- [ ] **Step 4: Run the captures**

```bash
cd /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl/crates/client-streams/tests/jvm-capture
./run.sh --javac      # writes fk_join_inner.topology.json + fk_join_left.topology.json
./run.sh --fkjoin     # writes ../testdata/fk_join/behavior.json
```

Expected: three new fixture files exist and are non-empty. Inspect `behavior.json`
and record (in the commit message) the pinned scalars: `SubscriptionWrapper.VERSION`,
each `Instruction` ordinal, whether a `primaryPartition` int is present in either
wrapper, the Murmur3 16-byte order, and the FK node/topic/store names.

- [ ] **Step 5: Commit**

```bash
cd /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl
git add crates/client-streams/tests/jvm-capture crates/client-streams/tests/testdata/golden/dsl/fk_join_inner.topology.json crates/client-streams/tests/testdata/golden/dsl/fk_join_left.topology.json crates/client-streams/tests/testdata/fk_join/behavior.json
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(streams): capture JVM FK-join (KIP-213) wire goldens + byte behavior

Pinned: SubscriptionWrapper.VERSION=<v>, Instruction ordinals=<...>,
primaryPartition present=<yes/no>, Murmur3 order=<...>, node/topic/store names.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Murmur3-128 + CombinedKey codecs

**Files:**
- Create: `crates/client-streams/src/dsl/processors/fk/mod.rs`
- Create: `crates/client-streams/src/dsl/processors/fk/murmur3.rs`
- Create: `crates/client-streams/src/dsl/processors/fk/combined_key.rs`
- Modify: `crates/client-streams/src/dsl/processors/mod.rs` (add `pub(crate) mod fk;`)
- Test: inline `#[cfg(test)]` in each file, plus a fixture-driven test that loads `behavior.json`.

- [ ] **Step 1: Create the `fk` module root**

`dsl/processors/fk/mod.rs`:

```rust
//! KIP-213 foreign-key join internals: byte codecs (`CombinedKey`,
//! `SubscriptionWrapper`, `SubscriptionResponseWrapper`, Murmur3-128) + the five
//! join processors. All byte formats are JVM-exact (pinned by the `--fkjoin`
//! capture in `tests/testdata/fk_join/behavior.json`).
pub(crate) mod combined_key;
pub(crate) mod murmur3;
pub(crate) mod subscription;
// `processors` is added in Task 5.
```

Add `pub(crate) mod fk;` to `dsl/processors/mod.rs` (place it in alphabetical order
with the other `mod` lines).

> NOTE: declaring `pub(crate) mod subscription;` here while `subscription.rs` does
> not yet exist will not compile. Create an empty `subscription.rs` with a doc
> comment now (`//! FK subscription wrappers (Task 3).`) so the crate compiles after
> Task 2; Task 3 fills it in. This keeps every task green.

- [ ] **Step 2: Write the Murmur3 test (against the capture)**

`dsl/processors/fk/murmur3.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // Hand-checked MurmurHash3 x64 128-bit vectors (seed 0). The JVM
    // `Murmur3.hash128` is the x64 128-bit variant; these confirm the algorithm
    // independent of the capture.
    #[test]
    fn empty_input() {
        // x64_128("", seed=0) = 0x00000000000000000000000000000000
        assert_eq!(hash128(b""), [0u8; 16]);
    }

    #[test]
    fn matches_jvm_capture() {
        // behavior.json `murmur3` entries: { "input_hex": "...", "hash_hex": "<32 hex>" }
        let v: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string("tests/testdata/fk_join/behavior.json").unwrap(),
        )
        .unwrap();
        for e in v["murmur3"].as_array().unwrap() {
            let input = hex_to_bytes(e["input_hex"].as_str().unwrap());
            let want = hex_to_bytes(e["hash_hex"].as_str().unwrap());
            assert_eq!(hash128(&input).as_slice(), want.as_slice(),
                "murmur3 mismatch for input {}", e["input_hex"]);
        }
    }

    fn hex_to_bytes(s: &str) -> Vec<u8> {
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }
}
```

- [ ] **Step 3: Run the test (fails — `hash128` undefined)**

Run: `cargo test -p crabka-client-streams murmur3 -- --nocapture`
Expected: FAIL (cannot find function `hash128`).

- [ ] **Step 4: Implement Murmur3-128 x64**

Prepend to `murmur3.rs` (canonical MurmurHash3 x64 128-bit; the JVM
`Murmur3.hash128` uses seed 0 and returns `long[2]`; **the 16-byte serialization
order is PINNED BY T1** — set `to_bytes` to match `behavior.json`, almost certainly
each long big-endian, h1 then h2):

```rust
//! MurmurHash3 x64 128-bit (seed 0) — JVM `org.apache.kafka.streams.state.internals.Murmur3.hash128`.

const C1: u64 = 0x87c3_7b91_1142_53d5;
const C2: u64 = 0x4cf5_ad43_2745_937f;

/// 128-bit MurmurHash3 (x64 variant, seed 0) of `data`, serialized to 16 bytes in
/// the JVM FK-join order (PINNED BY T1: each 64-bit half big-endian, h1 then h2).
#[must_use]
pub(crate) fn hash128(data: &[u8]) -> [u8; 16] {
    let (h1, h2) = hash128_longs(data, 0);
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&h1.to_be_bytes());
    out[8..].copy_from_slice(&h2.to_be_bytes());
    out
}

fn hash128_longs(data: &[u8], seed: u32) -> (u64, u64) {
    let mut h1 = u64::from(seed);
    let mut h2 = u64::from(seed);
    let nblocks = data.len() / 16;
    for i in 0..nblocks {
        let base = i * 16;
        let mut k1 = u64::from_le_bytes(data[base..base + 8].try_into().unwrap());
        let mut k2 = u64::from_le_bytes(data[base + 8..base + 16].try_into().unwrap());
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);
        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;
        h2 = h2.rotate_left(31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x3849_5ab5);
    }
    // tail
    let tail = &data[nblocks * 16..];
    let mut k1: u64 = 0;
    let mut k2: u64 = 0;
    let len = tail.len();
    if len > 8 {
        for j in (8..len).rev() {
            k2 ^= u64::from(tail[j]) << ((j - 8) * 8);
        }
        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;
    }
    if len > 0 {
        for j in (0..len.min(8)).rev() {
            k1 ^= u64::from(tail[j]) << (j * 8);
        }
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }
    // finalization
    h1 ^= data.len() as u64;
    h2 ^= data.len() as u64;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    (h1, h2)
}

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}
```

- [ ] **Step 5: Run the Murmur3 test (passes)**

Run: `cargo test -p crabka-client-streams murmur3 -- --nocapture`
Expected: PASS. If `matches_jvm_capture` fails, the 16-byte order in `to_bytes` is
wrong — flip to little-endian or swap h1/h2 to match `behavior.json` (do NOT change
the fixture).

- [ ] **Step 6: Write the CombinedKey test**

`dsl/processors/fk/combined_key.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn round_trip_and_prefix() {
        let fk = b"foreign";
        let pk = b"primary";
        let k = combined_key(fk, pk);
        // layout: [fkLen:4BE][fk][pk]
        assert_eq!(&k[..4], &(fk.len() as u32).to_be_bytes());
        assert_eq!(&k[4..4 + fk.len()], fk);
        assert_eq!(&k[4 + fk.len()..], pk);
        assert_eq!(foreign_prefix(fk).as_ref(), &k[..4 + fk.len()]);
        let (gfk, gpk) = split_combined_key(&k);
        assert_eq!(gfk, fk);
        assert_eq!(gpk, pk);
    }

    #[test]
    fn matches_jvm_capture() {
        let v: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string("tests/testdata/fk_join/behavior.json").unwrap(),
        ).unwrap();
        for e in v["combined_key"].as_array().unwrap() {
            let fk = e["fk"].as_str().unwrap().as_bytes();
            let pk = e["pk"].as_str().unwrap().as_bytes();
            let want = hex(e["bytes_hex"].as_str().unwrap());
            assert_eq!(combined_key(fk, pk), Bytes::from(want));
            let want_prefix = hex(e["prefix_hex"].as_str().unwrap());
            assert_eq!(foreign_prefix(fk), Bytes::from(want_prefix));
        }
    }
    fn hex(s: &str) -> Vec<u8> {
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }
}
```

- [ ] **Step 7: Run it (fails), then implement CombinedKey**

Run: `cargo test -p crabka-client-streams combined_key` → FAIL (undefined fns).

Prepend to `combined_key.rs`:

```rust
//! `CombinedKey<KO,K>` byte codec (JVM `CombinedKeySchema`).
//! Layout: `[ foreignKeyLen : 4 bytes BE ] [ foreignKeyBytes ] [ primaryKeyBytes ]`.
//! The range prefix `[fkLen:4BE][fk]` selects every primary key subscribed to a
//! foreign key — scanned via the byte store's half-open `range`.
use bytes::{BufMut, Bytes, BytesMut};

/// Encode `(foreignKeyBytes, primaryKeyBytes)` → combined-key bytes.
#[must_use]
pub(crate) fn combined_key(fk: &[u8], pk: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(4 + fk.len() + pk.len());
    b.put_u32(u32::try_from(fk.len()).expect("fk len fits u32"));
    b.extend_from_slice(fk);
    b.extend_from_slice(pk);
    b.freeze()
}

/// The range-scan prefix for "all primary keys subscribed to `fk`": `[fkLen:4BE][fk]`.
#[must_use]
pub(crate) fn foreign_prefix(fk: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(4 + fk.len());
    b.put_u32(u32::try_from(fk.len()).expect("fk len fits u32"));
    b.extend_from_slice(fk);
    b.freeze()
}

/// Exclusive upper bound for a prefix scan: the prefix with `0x00` appended is the
/// least key strictly greater than every key with this prefix that the half-open
/// `range(lo, hi)` must include — so use the successor of the LAST in-prefix key.
/// We instead bump the prefix to its byte-successor (drop trailing 0xFF, +1) which,
/// because the next field is a fixed-length count, never collides. Simpler: append
/// `0xFF * 0` — see `range_upper`.
#[must_use]
pub(crate) fn split_combined_key(k: &[u8]) -> (&[u8], &[u8]) {
    let fk_len = u32::from_be_bytes(k[..4].try_into().expect("4 bytes")) as usize;
    (&k[4..4 + fk_len], &k[4 + fk_len..])
}

/// Half-open upper bound covering every combined key with foreign prefix `fk`:
/// the byte-successor of `foreign_prefix(fk)` (increment the last non-0xFF byte).
/// The prefix is `[len:4BE][fk]`; since the pk bytes follow with no separator, the
/// successor of the prefix is the correct exclusive bound.
#[must_use]
pub(crate) fn range_upper(fk: &[u8]) -> Bytes {
    let mut p = foreign_prefix(fk).to_vec();
    // increment to the lexicographic successor (strip trailing 0xFF, bump last byte)
    while let Some(last) = p.last().copied() {
        if last == 0xFF {
            p.pop();
        } else {
            *p.last_mut().unwrap() = last + 1;
            break;
        }
    }
    Bytes::from(p)
}
```

> The successor trick mirrors the inclusive-range successor used by IQ KV range and
> the window store; reuse the exact form already in the codebase if one exists
> (grep `range_upper`/`successor` in `store/`), otherwise the above is correct.

- [ ] **Step 8: Run all Task-2 tests (pass), clippy, commit**

```bash
cargo test -p crabka-client-streams murmur3 combined_key
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
cargo fmt -p crabka-client-streams
git add crates/client-streams/src/dsl/processors/fk crates/client-streams/src/dsl/processors/mod.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams): FK-join Murmur3-128 + CombinedKey codecs (KIP-213)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: SubscriptionWrapper + SubscriptionResponseWrapper codecs

**Files:**
- Modify: `crates/client-streams/src/dsl/processors/fk/subscription.rs` (created empty in T2)
- Test: inline `#[cfg(test)]` + fixture-driven.

- [ ] **Step 1: Write the wrapper tests (against the capture)**

In `subscription.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn instruction_ordinals_match_capture() {
        let v = behavior();
        for e in v["instruction_ordinals"].as_array().unwrap() {
            let name = e["name"].as_str().unwrap();
            let byte = u8::try_from(e["byte"].as_u64().unwrap()).unwrap();
            assert_eq!(Instruction::from_byte(byte).unwrap().name(), name);
            assert_eq!(Instruction::from_byte(byte).unwrap().to_byte(), byte);
        }
    }

    #[test]
    fn subscription_wrapper_matches_capture() {
        let v = behavior();
        for e in v["subscription_wrapper"].as_array().unwrap() {
            let instr = Instruction::from_byte(
                u8::try_from(e["instruction_byte"].as_u64().unwrap()).unwrap()).unwrap();
            let hash = e["hash_hex"].as_str().map(hex);
            let pk = e["pk"].as_str().unwrap().as_bytes();
            let w = SubscriptionWrapper { instruction: instr, hash: hash.clone(), primary_key: Bytes::copy_from_slice(pk) };
            assert_eq!(w.serialize(), Bytes::from(hex(e["bytes_hex"].as_str().unwrap())),
                "subscription wrapper bytes mismatch: {e}");
            assert_eq!(SubscriptionWrapper::deserialize(&w.serialize()), w);
        }
    }

    #[test]
    fn response_wrapper_matches_capture() {
        let v = behavior();
        for e in v["subscription_response_wrapper"].as_array().unwrap() {
            let hash = e["hash_hex"].as_str().map(hex);
            let fv = e["foreign_value_hex"].as_str().map(|s| Bytes::from(hex(s)));
            let w = SubscriptionResponseWrapper { hash: hash.clone(), foreign_value: fv.clone() };
            assert_eq!(w.serialize(), Bytes::from(hex(e["bytes_hex"].as_str().unwrap())),
                "response wrapper bytes mismatch: {e}");
            assert_eq!(SubscriptionResponseWrapper::deserialize(&w.serialize()), w);
        }
    }

    fn behavior() -> serde_json::Value {
        serde_json::from_str(&std::fs::read_to_string("tests/testdata/fk_join/behavior.json").unwrap()).unwrap()
    }
    fn hex(s: &str) -> Vec<u8> {
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }
}
```

- [ ] **Step 2: Run it (fails), then implement the wrappers**

Run: `cargo test -p crabka-client-streams subscription` → FAIL (undefined types).

Prepend to `subscription.rs` (constants/layout PINNED BY T1 — set `VERSION`,
the `Instruction` byte mapping, and the optional `primaryPartition` field to match
`behavior.json`; the structure below is the JVM `SubscriptionWrapper` /
`SubscriptionResponseWrapper` shape):

```rust
//! KIP-213 subscription + response wrappers (JVM
//! `org.apache.kafka.streams.kstream.internals.foreignkeyjoin`).
//!
//! `SubscriptionWrapper`  : `version(1) ‖ instruction(1) ‖ [hash:16|absent] ‖ pk…`
//! `SubscriptionResponse` : `version(1) ‖ [hash:16|absent] ‖ [foreignValue…|null]`
//! All scalars (VERSION, instruction bytes, hash-presence marker, any
//! primaryPartition int) are PINNED by the `--fkjoin` capture.
use bytes::{BufMut, Bytes, BytesMut};

/// Wrapper format version (PINNED BY T1 — the JVM `SubscriptionWrapper` VERSION).
const VERSION: u8 = 1;
const HASH_LEN: usize = 16;

/// What the right side must do with a subscription (JVM
/// `SubscriptionWrapper.Instruction`). Byte values PINNED BY T1.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Instruction {
    /// Inner, fk value present-or-not: emit a result only if the foreign value exists.
    PropagateOnlyIfFkValAvailable,
    /// Left: emit a result even if the foreign value is missing (null vb).
    PropagateNullIfNoFkValAvailable,
    /// Left delete / fk change on a left row: delete the subscription AND emit a tombstone.
    DeleteKeyAndPropagate,
    /// Inner delete / fk change: delete the subscription, emit nothing.
    DeleteKeyNoPropagate,
}

impl Instruction {
    pub(crate) fn to_byte(self) -> u8 {
        // PINNED BY T1 — match behavior.json `instruction_ordinals`.
        match self {
            Instruction::DeleteKeyNoPropagate => 0,
            Instruction::PropagateNullIfNoFkValAvailable => 1,
            Instruction::PropagateOnlyIfFkValAvailable => 2,
            Instruction::DeleteKeyAndPropagate => 3,
        }
    }
    pub(crate) fn from_byte(b: u8) -> Option<Self> {
        Some(match b {
            0 => Instruction::DeleteKeyNoPropagate,
            1 => Instruction::PropagateNullIfNoFkValAvailable,
            2 => Instruction::PropagateOnlyIfFkValAvailable,
            3 => Instruction::DeleteKeyAndPropagate,
            _ => return None,
        })
    }
    pub(crate) fn name(self) -> &'static str {
        match self {
            Instruction::DeleteKeyNoPropagate => "DELETE_KEY_NO_PROPAGATE",
            Instruction::PropagateNullIfNoFkValAvailable => "PROPAGATE_NULL_IF_NO_FK_VALUE_AVAILABLE",
            Instruction::PropagateOnlyIfFkValAvailable => "PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE",
            Instruction::DeleteKeyAndPropagate => "DELETE_KEY_AND_PROPAGATE",
        }
    }
    pub(crate) fn is_propagate(self) -> bool {
        matches!(self, Instruction::PropagateOnlyIfFkValAvailable | Instruction::PropagateNullIfNoFkValAvailable)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SubscriptionWrapper {
    pub instruction: Instruction,
    /// Murmur3-128 of the serialized left value; `None` on delete instructions.
    pub hash: Option<Vec<u8>>,
    pub primary_key: Bytes,
}

impl SubscriptionWrapper {
    pub(crate) fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u8(VERSION);
        b.put_u8(self.instruction.to_byte());
        // hash-presence marker (PINNED BY T1: JVM writes a length byte / sentinel;
        // confirm exact encoding from behavior.json). Common form: 1 byte present-flag.
        match &self.hash {
            Some(h) => { b.put_u8(1); debug_assert_eq!(h.len(), HASH_LEN); b.extend_from_slice(h); }
            None => { b.put_u8(0); }
        }
        b.extend_from_slice(&self.primary_key);
        b.freeze()
    }
    pub(crate) fn deserialize(bytes: &[u8]) -> Self {
        let _version = bytes[0];
        let instruction = Instruction::from_byte(bytes[1]).expect("valid instruction");
        let present = bytes[2];
        let (hash, rest) = if present == 1 {
            (Some(bytes[3..3 + HASH_LEN].to_vec()), &bytes[3 + HASH_LEN..])
        } else {
            (None, &bytes[3..])
        };
        Self { instruction, hash, primary_key: Bytes::copy_from_slice(rest) }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SubscriptionResponseWrapper {
    /// Echoed left-value hash for the staleness check; `None` on delete-driven responses.
    pub hash: Option<Vec<u8>>,
    /// Serialized foreign (right) value, or `None` (inner miss / left null / tombstone).
    pub foreign_value: Option<Bytes>,
}

impl SubscriptionResponseWrapper {
    pub(crate) fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u8(VERSION);
        match &self.hash {
            Some(h) => { b.put_u8(1); debug_assert_eq!(h.len(), HASH_LEN); b.extend_from_slice(h); }
            None => { b.put_u8(0); }
        }
        match &self.foreign_value {
            Some(fv) => { b.put_u8(1); b.extend_from_slice(fv); }
            None => { b.put_u8(0); }
        }
        b.freeze()
    }
    pub(crate) fn deserialize(bytes: &[u8]) -> Self {
        let _version = bytes[0];
        let mut i = 1;
        let hash = if bytes[i] == 1 { i += 1; let h = bytes[i..i + HASH_LEN].to_vec(); i += HASH_LEN; Some(h) } else { i += 1; None };
        let foreign_value = if bytes[i] == 1 { i += 1; Some(Bytes::copy_from_slice(&bytes[i..])) } else { None };
        Self { hash, foreign_value }
    }
}
```

> **PINNING NOTE:** the JVM's exact hash-presence + foreign-value-presence encoding
> (a flag byte vs. a length-prefix vs. relying on remaining length) is determined by
> `behavior.json`. If the fixture shows a different framing (e.g. no present-flag and
> the hash is always 16 bytes for propagate / 0 bytes for delete), adjust
> `serialize`/`deserialize` until `*_matches_capture` passes. Do NOT edit the fixture.

- [ ] **Step 3: Run, clippy, fmt, commit**

```bash
cargo test -p crabka-client-streams subscription
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
cargo fmt -p crabka-client-streams
git add crates/client-streams/src/dsl/processors/fk/subscription.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams): FK-join subscription + response wrapper codecs (KIP-213)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: SubscriptionBytesStore + registry + context + builder

**Files:**
- Create: `crates/client-streams/src/store/fk_subscription.rs`
- Modify: `crates/client-streams/src/store/mod.rs` (`pub(crate) mod fk_subscription;`)
- Modify: `crates/client-streams/src/store/registry.rs` (`get_fk_subscription`)
- Modify: `crates/client-streams/src/processor/api.rs` (`ProcessorContext::get_fk_subscription_store`)
- Modify: `crates/client-streams/src/topology/builder.rs` (`add_fk_subscription_store`)

The store mirrors `SessionBytesStore` exactly (typed store over `ByteKeyValueStore`,
changelog-backed, compact). It holds `CombinedKey` bytes → `ValueAndTimestamp<SubscriptionWrapper>`,
and exposes a prefix range by foreign key.

- [ ] **Step 1: Write the store contract test**

`store/fk_subscription.rs` (test module):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::processors::fk::subscription::{Instruction, SubscriptionWrapper};
    use bytes::Bytes;

    fn wrapper(pk: &str) -> SubscriptionWrapper {
        SubscriptionWrapper {
            instruction: Instruction::PropagateOnlyIfFkValAvailable,
            hash: Some(vec![7u8; 16]),
            primary_key: Bytes::copy_from_slice(pk.as_bytes()),
        }
    }

    #[tokio::test]
    async fn put_get_range_by_foreign_and_changelog() {
        let mut s = SubscriptionBytesStore::in_memory("sub".into(), "app-sub-changelog".into());
        s.put(b"FK1", b"pk1", &wrapper("pk1"), 10).await;
        s.put(b"FK1", b"pk2", &wrapper("pk2"), 11).await;
        s.put(b"FK2", b"pk9", &wrapper("pk9"), 12).await;
        // exact get
        assert_eq!(s.get(b"FK1", b"pk1").await.unwrap().primary_key, Bytes::from_static(b"pk1"));
        // range by foreign key prefix → only FK1's two subscribers, in pk order
        let subs = s.range_by_foreign(b"FK1").await;
        let pks: Vec<&[u8]> = subs.iter().map(|(_ck_pk, w)| w.primary_key.as_ref()).collect();
        assert_eq!(pks, vec![b"pk1".as_ref(), b"pk2".as_ref()]);
        // delete
        assert!(s.delete(b"FK1", b"pk1").await.is_some());
        assert_eq!(s.range_by_foreign(b"FK1").await.len(), 1);
        // changelog drained both puts + the delete (3 entries; delete = tombstone)
        assert_eq!(s.take_changelog().len(), 4);
    }
}
```

- [ ] **Step 2: Run it (fails), implement the store**

Run: `cargo test -p crabka-client-streams fk_subscription` → FAIL.

Prepend to `store/fk_subscription.rs` (structure copied from `SessionBytesStore`,
keyed by `CombinedKey`, value `ValueAndTimestamp<SubscriptionWrapper>`):

```rust
//! KIP-213 subscription store: `CombinedKey<KO,K>` → `ValueAndTimestamp<SubscriptionWrapper>`,
//! over the pluggable byte backend. Prefix-range by foreign key drives the
//! right-table-change re-emit. Changelog-backed (compact); restore = replay.
use std::any::Any;

use async_trait::async_trait;
use bytes::Bytes;

use crate::dsl::processors::fk::combined_key::{combined_key, foreign_prefix, range_upper, split_combined_key};
use crate::dsl::processors::fk::subscription::SubscriptionWrapper;
use crate::store::api::StateStore;
use crate::store::byte::{ByteKeyValueStore, InMemoryBytes};
use crate::store::window_schema::{unwrap_value, wrap_value};

pub(crate) struct SubscriptionBytesStore {
    name: String,
    changelog_topic: String,
    backend: Box<dyn ByteKeyValueStore>,
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
}

impl SubscriptionBytesStore {
    pub(crate) fn new(name: String, backend: Box<dyn ByteKeyValueStore>, changelog_topic: String) -> Self {
        Self { name, changelog_topic, backend, changelog: Vec::new(), logging: true }
    }
    #[cfg(test)]
    pub(crate) fn in_memory(name: String, changelog_topic: String) -> Self {
        Self::new(name, Box::new(InMemoryBytes::default()), changelog_topic)
    }

    pub(crate) async fn put(&mut self, fk: &[u8], pk: &[u8], w: &SubscriptionWrapper, record_ts: i64) {
        let key = combined_key(fk, pk);
        let val = wrap_value(record_ts, &w.serialize());
        self.backend.put(key.clone(), val.clone()).await;
        if self.logging {
            self.changelog.push((key, Some(val)));
        }
    }
    pub(crate) async fn get(&self, fk: &[u8], pk: &[u8]) -> Option<SubscriptionWrapper> {
        let raw = self.backend.get(&combined_key(fk, pk)).await?;
        let (_ts, w) = unwrap_value(&raw);
        Some(SubscriptionWrapper::deserialize(w))
    }
    pub(crate) async fn delete(&mut self, fk: &[u8], pk: &[u8]) -> Option<SubscriptionWrapper> {
        let key = combined_key(fk, pk);
        let prev = self.backend.delete(&key).await.map(|raw| {
            let (_ts, w) = unwrap_value(&raw);
            SubscriptionWrapper::deserialize(w)
        });
        if self.logging {
            self.changelog.push((key, None));
        }
        prev
    }
    /// Every `(primaryKeyBytes, wrapper)` subscribed to `fk`, in stored key order.
    pub(crate) async fn range_by_foreign(&self, fk: &[u8]) -> Vec<(Bytes, SubscriptionWrapper)> {
        let lo = foreign_prefix(fk);
        let hi = range_upper(fk);
        let mut out = Vec::new();
        for (k, raw) in self.backend.range(&lo, &hi).await {
            let (gfk, gpk) = split_combined_key(&k);
            if gfk != fk {
                continue;
            }
            let (_ts, w) = unwrap_value(&raw);
            out.push((Bytes::copy_from_slice(gpk), SubscriptionWrapper::deserialize(w)));
        }
        out
    }
}

#[async_trait]
impl StateStore for SubscriptionBytesStore {
    fn name(&self) -> &str { &self.name }
    async fn flush(&mut self) {}
    fn close(&mut self) {}
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
    fn changelog_topic(&self) -> &str { &self.changelog_topic }
    fn take_changelog(&mut self) -> Vec<(Bytes, Option<Bytes>)> { std::mem::take(&mut self.changelog) }
    async fn apply_changelog(&mut self, key: Bytes, value: Option<Bytes>) {
        match value {
            Some(v) => self.backend.put(key, v).await,
            None => { self.backend.delete(&key).await; }
        }
    }
    fn set_logging(&mut self, on: bool) { self.logging = on; }
    async fn clear(&mut self) { self.backend.clear().await; self.changelog.clear(); }
}
```

> If `StateStore` on this branch (off main, no EOS) has no `clear` method, drop the
> `clear` override (check `store/api.rs` — EOS added `clear`; this branch is off
> `origin/main` which has IQ but may NOT have EOS's `clear`). Mirror whatever
> `SessionBytesStore` implements on THIS branch exactly.

- [ ] **Step 3: Wire the module + registry downcast + context accessor**

`store/mod.rs`: add `pub(crate) mod fk_subscription;`.

`store/registry.rs` — add next to `get_session`:

```rust
    /// Mutable access to the FK subscription store (untyped K/V — it stores
    /// `CombinedKey` bytes directly). `None` if absent or not a subscription store.
    pub(crate) fn get_fk_subscription(
        &mut self,
        name: &str,
    ) -> Option<&mut crate::store::fk_subscription::SubscriptionBytesStore> {
        let store = self.stores.get_mut(name)?;
        store
            .as_any_mut()
            .downcast_mut::<crate::store::fk_subscription::SubscriptionBytesStore>()
    }
```

`processor/api.rs` — add next to `get_window_store`:

```rust
    /// Access the connected FK subscription store. `None` if absent.
    pub fn get_fk_subscription_store(
        &mut self,
        name: &str,
    ) -> Option<&mut crate::store::fk_subscription::SubscriptionBytesStore> {
        self.dispatch.stores.get_fk_subscription(name)
    }
```

- [ ] **Step 4: `add_fk_subscription_store` on the builder**

`topology/builder.rs` — mirror `add_session_store`'s registration (compact changelog,
no retention math; the subscription store is a plain keyed store). The store factory
constructs a `SubscriptionBytesStore` (no K/V generics — it stores `CombinedKey`
bytes):

```rust
pub fn add_fk_subscription_store(
    &mut self,
    name: impl Into<String>,
    processors: impl IntoIterator<Item = impl Into<String>>,
) -> &mut Self {
    let name: String = name.into();
    let procs: Vec<String> = processors.into_iter().map(Into::into).collect();
    // Plain compact changelog (like add_state_store), NOT windowed retention.
    self.reg.add_store(&name, procs, None);
    self.store_factories.insert(
        name.clone(),
        (
            None,
            Box::new(
                move |store_name: &str,
                      changelog: String,
                      backend: Box<dyn crate::store::byte::ByteKeyValueStore>| {
                    Box::new(crate::store::fk_subscription::SubscriptionBytesStore::new(
                        store_name.to_string(),
                        backend,
                        changelog,
                    )) as Box<dyn crate::store::api::StateStore>
                },
            ),
        ),
    );
    self
}
```

- [ ] **Step 5: Run, clippy, fmt, commit**

```bash
cargo test -p crabka-client-streams fk_subscription
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
cargo fmt -p crabka-client-streams
git add crates/client-streams/src/store crates/client-streams/src/processor/api.rs crates/client-streams/src/topology/builder.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams): FK-join SubscriptionBytesStore + registry/context/builder wiring

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: The five FK-join processors

**Files:**
- Create: `crates/client-streams/src/dsl/processors/fk/processors.rs`
- Modify: `crates/client-streams/src/dsl/processors/fk/mod.rs` (`pub(crate) mod processors;`)

All five mirror the existing join-processor pattern (read store in a `match` to drop
the borrow before `forward`). The behavioral oracle for every edge case is
`behavior.json`'s `inner_sequence` / `left_sequence`; the per-processor unit tests
below assert the building blocks, and the full end-to-end behavior is verified in T7.

Types: left key `K`, left value `VA`, right key `KO`, right value `VB`, result `VR`.
The left side is keyed by `K`; the subscription topic is keyed by `KO`; the response
topic is keyed by `K`.

- [ ] **Step 1: Write processor unit tests**

`fk/processors.rs` test module — drive each processor against an in-memory subscription
store + a `Vec` capture of forwarded records. (Use the existing test harness for
processors if one exists — grep `dsl/processors/` tests for the in-memory
`ProcessorContext` pattern; otherwise test `SubscriptionSendProcessor`'s
instruction/hatch selection as a pure function `plan_send(...)` extracted for
testability.) Minimum coverage:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::processors::fk::subscription::Instruction;

    // SubscriptionSend: first left arrival (no old) under inner → one wrapper to fk(newVA)
    // with PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE + a hash, pk = key.
    #[test]
    fn send_first_arrival_inner() {
        let plan = plan_send(/*is_left=*/false, /*old=*/None, /*new=*/Some(b"A".to_vec()), b"k1");
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].fk, b"A");
        assert_eq!(plan[0].wrapper.instruction, Instruction::PropagateOnlyIfFkValAvailable);
        assert!(plan[0].wrapper.hash.is_some());
        assert_eq!(plan[0].wrapper.primary_key.as_ref(), b"k1");
    }

    // FK change A->B under inner → propagate to B (new) + DELETE_KEY_NO_PROPAGATE to A (old).
    #[test]
    fn send_fk_change_inner() {
        let plan = plan_send(false, Some(b"A".to_vec()), Some(b"B".to_vec()), b"k1");
        assert_eq!(plan.len(), 2);
        assert_eq!(plan[0].fk, b"B");
        assert_eq!(plan[0].wrapper.instruction, Instruction::PropagateOnlyIfFkValAvailable);
        assert_eq!(plan[1].fk, b"A");
        assert_eq!(plan[1].wrapper.instruction, Instruction::DeleteKeyNoPropagate);
    }

    // Left delete under left-join → DELETE_KEY_AND_PROPAGATE to old fk (tombstone path).
    #[test]
    fn send_delete_left() {
        let plan = plan_send(true, Some(b"A".to_vec()), None, b"k1");
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].fk, b"A");
        assert_eq!(plan[0].wrapper.instruction, Instruction::DeleteKeyAndPropagate);
        assert!(plan[0].wrapper.hash.is_none());
    }
}
```

> Cross-check the expected instruction for every transition against
> `behavior.json`'s sequences before locking the asserts — if the JVM emits a
> different instruction on FK-change-old-side, match it (capture wins).

- [ ] **Step 2: Run (fails), implement the processors**

Run: `cargo test -p crabka-client-streams fk::processors` → FAIL.

Implement `fk/processors.rs`. Sketch of the five (full bodies follow the
`KTableKTableJoinThisProcessor` store-access pattern verbatim — `match
ctx.get_*_store(..)` then drop borrow before `forward`):

```rust
//! The five KIP-213 FK-join processors. Left key K, left value VA, right key KO,
//! right value VB, result VR. Serdes are captured so the processors can serialize
//! the left value (for the hash) and the foreign value (into the response).
use std::marker::PhantomData;
use async_trait::async_trait;

use crate::dsl::processors::change::Change;
use crate::dsl::processors::fk::murmur3::hash128;
use crate::dsl::processors::fk::subscription::{Instruction, SubscriptionResponseWrapper, SubscriptionWrapper};
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;
use crate::processor::serde::Serde;
use bytes::Bytes;

/// One planned subscription emission (foreign key + wrapper).
pub(crate) struct SendPlan {
    pub fk: Vec<u8>,
    pub wrapper: SubscriptionWrapper,
}

/// Pure decision function (unit-tested): given is_left + old/new serialized left
/// values + the primary key, which subscription wrappers to emit (1 or 2).
pub(crate) fn plan_send(is_left: bool, old: Option<Vec<u8>>, new: Option<Vec<u8>>, pk: &[u8]) -> Vec<SendPlan> {
    let propagate = if is_left { Instruction::PropagateNullIfNoFkValAvailable } else { Instruction::PropagateOnlyIfFkValAvailable };
    let mut out = Vec::new();
    match (&old, &new) {
        (_, Some(nv)) => {
            // NOTE: fk == the serialized left value in these tests (extractor = identity);
            // in the processor the extractor runs on the typed value before serialization.
            let new_fk = nv.clone();
            let hash = hash128(nv).to_vec();
            out.push(SendPlan { fk: new_fk.clone(), wrapper: SubscriptionWrapper { instruction: propagate, hash: Some(hash), primary_key: Bytes::copy_from_slice(pk) } });
            if let Some(ov) = &old {
                let old_fk = ov.clone();
                if old_fk != new_fk {
                    out.push(SendPlan { fk: old_fk, wrapper: SubscriptionWrapper { instruction: Instruction::DeleteKeyNoPropagate, hash: None, primary_key: Bytes::copy_from_slice(pk) } });
                }
            }
        }
        (Some(ov), None) => {
            let del = if is_left { Instruction::DeleteKeyAndPropagate } else { Instruction::DeleteKeyNoPropagate };
            out.push(SendPlan { fk: ov.clone(), wrapper: SubscriptionWrapper { instruction: del, hash: None, primary_key: Bytes::copy_from_slice(pk) } });
        }
        (None, None) => {}
    }
    out
}
```

The five processor structs (full `process` bodies mirror the gathered
`KTableKTableJoinThisProcessor`/`KStreamKTableJoinProcessor` patterns):

1. **`SubscriptionSendProcessor<K, VA, KO>`** `Processor<K, Change<VA>, KO, SubscriptionWrapper>` —
   carries `fk_extractor: Fn(&VA)->KO`, `va_serde`, `ko_serde`, `is_left`. In
   `process`: serialize old/new left values, build `plan_send`, then for each plan
   `ctx.forward(Record::new(Some(ko_deser(plan.fk)), plan.wrapper, ts))`. (Key type
   out is `KO`.)
2. **`SubscriptionReceiveProcessor<KO>`** `Processor<KO, SubscriptionWrapper, Bytes, SubscriptionWrapper>` —
   carries `store_name`, `ko_serde`. In `process`: `fk = ko_serde.serialize(key)`;
   on a `Propagate*` instruction `store.put(fk, wrapper.primary_key, &wrapper, ts)`;
   on a `DeleteKey*` instruction `store.delete(fk, wrapper.primary_key)`; then forward
   the wrapper downstream (so `SubscriptionJoin` can read B) keyed by the combined key.
3. **`SubscriptionJoinProcessor<KO, VB, K>`** `Processor<KO/combined, SubscriptionWrapper, K, SubscriptionResponseWrapper>` —
   carries `b_store` (right table store name), `vb_serde`. In `process`:
   `vb = b_store.get(fk)`; build the response per the instruction:
   - `DeleteKeyNoPropagate` → forward nothing.
   - `DeleteKeyAndPropagate` → forward `Response{hash:None, foreign_value:None}` (tombstone) keyed by pk.
   - `PropagateOnlyIfFkValAvailable` → if `vb.is_some()` forward `Response{hash, Some(serialize(vb))}`, else nothing (inner miss).
   - `PropagateNullIfNoFkValAvailable` → forward `Response{hash, vb.map(serialize)}` (left; null vb allowed).
4. **`ForeignTableJoinProcessor<KO, VB, K>`** `Processor<KO, Change<VB>, K, SubscriptionResponseWrapper>` —
   carries `store_name` (subscription), `vb_serde`. On a B `Change` for `KO`:
   `fk = ko_serde.serialize(key)`; `subs = store.range_by_foreign(fk)`; for each
   `(pk, stored)`: forward `Response{hash: stored.hash, foreign_value: new_vb.map(serialize)}`
   keyed by `pk` (using the stored wrapper's instruction to decide inner-skip vs
   left-null — same branch logic as #3). A B delete (`new = None`) emits responses
   with `foreign_value: None`.
5. **`SubscriptionResolverProcessor<K, VA, VB, VR>`** `Processor<K, SubscriptionResponseWrapper, K, Change<VR>>` —
   carries `a_store` (left table store), `va_serde`, `vb_serde`, `joiner: Fn(&VA, Option<&VB>)->VR`,
   `is_left`. On a response for `K`: `va = a_store.get(K)`; **staleness check**:
   if `va` present, `hash128(serialize(va)) == response.hash` must hold, else DROP
   (a newer subscription is in flight). On match: if `response.foreign_value` decode
   to `Some(vb)` → `Change::update(old, joiner(&va, Some(&vb)))`; if `None` →
   inner: `Change::tombstone(old)`, left: `Change::update(old, joiner(&va, None))`.
   `old` is the result-getter's previous value; since the result KTable is
   unmaterialized, forward `Change{old:None, new}` (the merge downstream dedups).
   Forward keyed by `K`.

Each `process` uses the `match ctx.get_*_store(name){ Some(s)=>..., None=>... }`
borrow-drop pattern (see the gathered `KTableKTableJoinThisProcessor`). Add
`pub(crate) mod processors;` to `fk/mod.rs`.

- [ ] **Step 3: Run unit tests, clippy, fmt, commit**

```bash
cargo test -p crabka-client-streams fk::processors
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
cargo fmt -p crabka-client-streams
git add crates/client-streams/src/dsl/processors/fk
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams): the five KIP-213 FK-join processors

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: DSL ops + lowering + names + execution tests

**Files:**
- Modify: `crates/client-streams/src/dsl/names.rs` (FK prefixes — PINNED BY T1)
- Modify: `crates/client-streams/src/dsl/ktable.rs` (`join_on_foreign_key` / `left_join_on_foreign_key` + lowering)
- Modify: `crates/client-streams/src/lib.rs` (re-exports if needed)
- Test: `dsl/ktable.rs` `#[cfg(test)]` execution tests via `TopologyTestDriver`.

- [ ] **Step 1: Add the FK node-name prefixes (PINNED BY T1)**

`dsl/names.rs` — add the prefixes captured in `behavior.json` `node_names` /
`store_names` / `topic_names`. Example (replace with the actual captured strings):

```rust
/// KIP-213 FK-join node-name prefixes + the subscription store name prefix +
/// repartition topic name segments (PINNED by the --fkjoin capture, behavior.json).
pub(crate) const FK_SUBSCRIPTION_SEND: &str = "KTABLE-SUBSCRIPTION-REGISTRATION-PROCESSOR-";
pub(crate) const FK_SUBSCRIPTION_RECEIVE: &str = "KTABLE-SUBSCRIPTION-RECEIVE-";
pub(crate) const FK_SUBSCRIPTION_JOIN: &str = "KTABLE-SUBSCRIPTION-JOIN-";
pub(crate) const FK_FOREIGN_JOIN: &str = "KTABLE-FK-JOIN-";
pub(crate) const FK_RESPONSE_RESOLVER: &str = "KTABLE-SUBSCRIPTION-RESPONSE-RESOLVER-";
pub(crate) const FK_SUBSCRIPTION_STORE: &str = "KTABLE-FK-JOIN-SUBSCRIPTION-STATE-STORE-";
pub(crate) const FK_REGISTRATION_TOPIC: &str = "-subscription-registration-topic";
pub(crate) const FK_RESPONSE_TOPIC: &str = "-subscription-response-topic";
pub(crate) const FK_SINK: &str = "KTABLE-SINK-";
pub(crate) const FK_SOURCE: &str = "KTABLE-SOURCE-";
```

- [ ] **Step 2: Write the execution tests (drive the inner/left sequences)**

`dsl/ktable.rs` test module — replicate `behavior.json`'s `inner_sequence` /
`left_sequence` via `TopologyTestDriver`:

```rust
#[tokio::test(flavor = "current_thread")]
async fn fk_inner_join_executes() {
    use crate::processor::serde::StringSerde;
    let b = StreamsBuilder::new();
    let a = b.table::<String, String, _, _>("a", Consumed::with(StringSerde, StringSerde), Materialized::with(StringSerde, StringSerde).as_store("sa"));
    let bt = b.table::<String, String, _, _>("b", Consumed::with(StringSerde, StringSerde), Materialized::with(StringSerde, StringSerde).as_store("sb"));
    a.join_on_foreign_key(&bt, |va: &String| va.clone(), |va: &String, vb: &String| format!("{va}{vb}"), StringSerde)
        .to_stream()
        .to("out", Produced::with(StringSerde, StringSerde));
    drop(a); drop(bt);
    let built = b.build("app").unwrap();
    let mut d = TopologyTestDriver::new(&built).unwrap();
    let sk = || Consumed::with(StringSerde, StringSerde);
    let pk = || Produced::with(StringSerde, StringSerde);

    d.pipe_input("a", sk(), Some("k1".into()), "A".into(), 0);        // no right yet
    assert_eq!(d.read_output("out", pk()), None);
    d.pipe_input("b", sk(), Some("A".into()), "X".into(), 1);          // right arrives
    assert_eq!(d.read_output("out", pk()), Some((Some("k1".into()), "AX".into())));
    d.pipe_input("a", sk(), Some("k2".into()), "A".into(), 2);          // 2nd subscriber
    assert_eq!(d.read_output("out", pk()), Some((Some("k2".into()), "AX".into())));
    d.pipe_input("b", sk(), Some("A".into()), "Y".into(), 3);           // right update → both
    let mut got = vec![d.read_output("out", pk()), d.read_output("out", pk())];
    got.sort();
    assert_eq!(got, vec![Some((Some("k1".into()), "AY".into())), Some((Some("k2".into()), "AY".into()))]);
}

// fk_left_join_executes: leftJoin variant — first left arrival emits "A_" immediately
// (right absent → joiner sees None); rest mirrors the capture's left_sequence.
```

Cross-check expected outputs against `behavior.json`'s sequences exactly.

- [ ] **Step 3: Run (fails), implement the DSL ops + lowering**

Run: `cargo test -p crabka-client-streams fk_inner_join_executes` → FAIL (no method).

Add to `dsl/ktable.rs` (mirror `join_impl`'s multi-node lowering — mint the five
processor names + the two sink/source pairs + the subscription store name; add the
graph nodes; attach lowering thunks that call `add_processor` / `add_sink` /
`add_repartition_topic` / `add_source` / `add_fk_subscription_store` /
`connect_processor_store`; declare the `[a_src, b_src]` copartition group). The two
public methods wrap the joiner to outer form and set `is_left`:

```rust
impl<K, VA> KTable<K, VA>
where
    K: Any + Send + Sync + Clone,
    VA: Any + Send + Clone,
{
    /// Inner foreign-key join (KIP-213). `fk_extractor` maps the left value to the
    /// right table's key; `joiner` combines matched rows. `fk_serde` serializes the
    /// foreign key for the subscription topic + `CombinedKey`. Both tables must be
    /// materialized; the result is an unmaterialized `KTable`.
    pub fn join_on_foreign_key<KO, VB, VR, FKE, J, KOS>(
        &self, other: &KTable<KO, VB>, fk_extractor: FKE, joiner: J, fk_serde: KOS,
    ) -> KTable<K, VR>
    where
        KO: Any + Send + Clone, VB: Any + Send + Clone, VR: Any + Send + Clone,
        FKE: Fn(&VA) -> KO + Clone + Send + Sync + 'static,
        J: Fn(&VA, &VB) -> VR + Clone + Send + Sync + 'static,
        KOS: Serde<KO> + Clone,
    {
        let jf = move |a: &VA, b: Option<&VB>| joiner(a, b.expect("inner fk join: b present"));
        self.fk_join_impl(other, fk_extractor, jf, fk_serde, /*is_left=*/false)
    }

    /// Left foreign-key join (KIP-213): emits whenever the left row exists; the
    /// joiner receives `None` for the right value on a miss.
    pub fn left_join_on_foreign_key<KO, VB, VR, FKE, J, KOS>(
        &self, other: &KTable<KO, VB>, fk_extractor: FKE, joiner: J, fk_serde: KOS,
    ) -> KTable<K, VR>
    where
        KO: Any + Send + Clone, VB: Any + Send + Clone, VR: Any + Send + Clone,
        FKE: Fn(&VA) -> KO + Clone + Send + Sync + 'static,
        J: Fn(&VA, Option<&VB>) -> VR + Clone + Send + Sync + 'static,
        KOS: Serde<KO> + Clone,
    {
        self.fk_join_impl(other, fk_extractor, joiner, fk_serde, /*is_left=*/true)
    }
}
```

`fk_join_impl` (the lowering) follows `join_impl`'s structure exactly: borrow the
builder, mint names via `g.new_processor_name(names::FK_*)` (in the JVM order pinned
by T1 so the counter indices match the wire golden), add graph nodes with
`g.graph.add(...)`, and attach a thunk per node. The thunks:
- **send** node (fed by `self.node`): `add_processor(send_name, SubscriptionSendProcessor{...}, [a_parent])`.
- **registration sink+source**: in the receive thunk, `add_sink` to
  `<app>-<base>-subscription-registration-topic`, `add_repartition_topic(...)`,
  `add_source` reading it; then `add_processor(receive_name, SubscriptionReceiveProcessor{...})`
  + `add_fk_subscription_store(sub_store, [receive_name, foreign_join_name])` +
  `connect_processor_store`.
- **subscription-join** node: `add_processor(...)` connected to `b_store`.
- **foreign-table-join** node (fed by `other.node`): `add_processor(...)` connected
  to the subscription store + `b_store`.
- **response sink+source**: `add_sink` to `<app>-<base>-subscription-response-topic`,
  `add_repartition_topic`, `add_source`; then `add_processor(resolver_name,
  SubscriptionResolverProcessor{...})` connected to `a_store`.
- declare `add_copartition_group([a_src, b_src])` where applicable.
- the returned `KTable` node is the resolver's lowered node (`store_name=None`,
  `source_topic=None`).

Use `Materialized` for `other`/`self` requirements: `a_store =
self.store_name().expect("FK join: left table must be materialized")`, `b_store =
other.store_name().expect("FK join: right table must be materialized")`.

- [ ] **Step 4: Run execution tests (pass)**

Run: `cargo test -p crabka-client-streams fk_inner_join_executes fk_left_join_executes`
Expected: PASS. Debug against `behavior.json` if outputs differ.

- [ ] **Step 5: clippy, fmt, commit**

```bash
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
cargo fmt -p crabka-client-streams
git add crates/client-streams/src/dsl crates/client-streams/src/lib.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams): FK-join DSL ops + lowering (join_on_foreign_key / left_join_on_foreign_key)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: Wire-topology + byte-parity goldens  (∥ Task 8)

**Files:**
- Create: `crates/client-streams/tests/fk_join_golden.rs`

- [ ] **Step 1: Wire-topology goldens**

Mirror `ktable_ktable_join_matches_jvm` (from `tests/dsl_golden_frame.rs`) for both
FK fixtures. Build the Rust FK topology, `build_optimized("app").to_wire()`, and
`assert_matches_fixture(&wire, "fk_join_inner")` / `"fk_join_left"` against the
committed `testdata/golden/dsl/fk_join_*.topology.json`:

```rust
#[test]
fn fk_join_inner_matches_jvm() {
    use crabka_client_streams::{Materialized, StringSerde};
    let b = StreamsBuilder::new();
    let a = b.table::<String, String, _, _>("a", Consumed::with(StringSerde, StringSerde), Materialized::with(StringSerde, StringSerde).as_store("sa"));
    let bt = b.table::<String, String, _, _>("b", Consumed::with(StringSerde, StringSerde), Materialized::with(StringSerde, StringSerde).as_store("sb"));
    a.join_on_foreign_key(&bt, |va: &String| va.clone(), |va: &String, vb: &String| format!("{va}{vb}"), StringSerde)
        .to_stream()
        .to("out", Produced::with(StringSerde, StringSerde));
    drop(a); drop(bt);
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "fk_join_inner");
}
// fk_join_left_matches_jvm: same with left_join_on_foreign_key + the "_"-on-null joiner.
```

(Copy `assert_matches_fixture` from `dsl_golden_frame.rs`.)

- [ ] **Step 2: Byte-parity test for the wrappers via the exec path**

Assert the Rust processors emit the exact JVM wrapper bytes by re-running the codec
checks at the integration boundary: for one record from `inner_sequence`, build the
expected `SubscriptionWrapper`/`SubscriptionResponseWrapper` via the Rust codecs and
assert equality with `behavior.json` (the unit tests in T2/T3 already cover the
codecs; this test guards the end-to-end serialize path used by the processors). Keep
it a thin assertion that the public re-exports + the captured fixture agree.

- [ ] **Step 3: Run, fmt, commit**

```bash
cargo test -p crabka-client-streams --test fk_join_golden
cargo fmt -p crabka-client-streams
git add crates/client-streams/tests/fk_join_golden.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(streams): FK-join wire-topology + byte-parity goldens vs JVM

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 8: Broker e2e + docs + final verification  (∥ Task 7)

**Files:**
- Create: `crates/client-streams/tests/fk_join_broker.rs`
- Modify: `crates/client-streams/src/lib.rs` (`## Foreign-key joins` doc section)

- [ ] **Step 1: In-process broker e2e**

Mirror the existing DSL broker integration test (grep `tests/` for the in-process
broker harness used by `iq_broker.rs` / the DSL broker test). Build an FK-join app,
start the in-process broker, produce to `a` + `b`, consume `out`, assert the joined
results, and assert subscription-store **restart-restore** (restart the app, verify
the join still resolves from the restored subscription changelog).

- [ ] **Step 2: Docs section**

Add to `lib.rs` a `## Foreign-key joins` section under the existing DSL docs:
the two methods, the materialization requirement, the unmaterialized result, and a
short example. Keep it parallel to the existing `## Interactive Queries` section
style.

- [ ] **Step 3: Final verification (whole crate)**

```bash
cargo test -p crabka-client-streams
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
cargo clippy -p crabka-broker --all-targets -- -D warnings   # FK touches no broker code, but verify
cargo fmt --check
cargo test -p crabka-client-streams --doc
```

Expected: all green; new tests (codecs, store, processors, exec, goldens, broker
e2e, doctests) pass; clippy + fmt clean.

- [ ] **Step 4: Commit**

```bash
git add crates/client-streams/tests/fk_join_broker.rs crates/client-streams/src/lib.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(streams): FK-join broker e2e + restart-restore + docs

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-review checklist (run before execution)

1. **Spec coverage:** §1 surface → T6; §2 topology → T6 lowering; §3 codecs → T2/T3
   (+ Murmur3 T2); §4 store/wiring → T4; §5 processors → T5; §6 lowering/decomp →
   T6; §7 edge cases → T5 + T6 exec tests + T8 restore; §8 testing → T1 capture +
   T2/T3/T4/T5 unit + T6 exec + T7 golden + T8 e2e; §9 files → all tasks; §10
   capture-pinned items → T1. ✔ All covered.
2. **Type consistency:** `SubscriptionWrapper{instruction, hash, primary_key}`,
   `SubscriptionResponseWrapper{hash, foreign_value}`, `Instruction` variants,
   `combined_key`/`foreign_prefix`/`range_upper`/`split_combined_key`, `hash128`,
   `SubscriptionBytesStore::{new,put,get,delete,range_by_foreign}`,
   `get_fk_subscription`/`get_fk_subscription_store`/`add_fk_subscription_store`,
   `join_on_foreign_key`/`left_join_on_foreign_key`/`fk_join_impl` — names used
   consistently across T2–T8. ✔
3. **Capture-pinned placeholders are intentional:** every "PINNED BY T1" scalar is
   produced by the gating capture and asserted via committed fixtures — not a
   hand-wave. The structural code is complete.
