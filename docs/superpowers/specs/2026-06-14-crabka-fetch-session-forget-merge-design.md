# Fetch-Session Forget+Merge Composition Model (KIP-227) — Design

**Date:** 2026-06-14
**Status:** Approved (design); spec under review
**Workstream:** A (wrap-real stateright model of a stateful core) + proptest. **Capstone** — the last
candidate in the model-checking program's backlog.

## Goal

Verify the KIP-227 incremental-fetch session-cache `forget`+`merge` **composition** under topic-rename /
identity-churn sequences — the one surface a fresh survey found still unmodeled. Drive the real
forget+merge logic over sequences of incremental fetches and assert no **shadow** cached entry (two keys
for one logical partition) and subscription fidelity. Honest discovery odds: moderate — the merge-side
half-identity shadow is already fixed+tested; this targets the *composed* forget+merge path that no test
covers. A GREEN result cleanly closes the area; a RED is a real cache-corruption bug.

## Background

`FetchSessionCache::classify` (`crates/broker/src/fetch_session.rs`) mutates a session's
`partitions: HashMap<FetchSessionKey, CachedPartitionState>` on each incremental fetch:

- **forget** (lines 271-280): `retain`, dropping a cached key when a `ForgottenTopic` matches by
  name-**OR**-id (`(!ft.topic.is_empty() && k.topic_name==ft.topic) || (ft.topic_id!=ZERO && k.topic_id==ft.topic_id)`)
  and `ft.partitions.contains(k.partition)`.
- **merge** (295-318): for each requested `(topic, partition)`, find an existing key by either-half
  identity (`(!t.topic.is_empty() && k.topic_name==t.topic) || (t.topic_id!=ZERO && k.topic_id==t.topic_id)`)
  + partition; update its state in place; **else insert a brand-new default-state key**
  (`FetchSessionKey { topic_name: t.topic, topic_id: t.topic_id, partition }`).

`FetchSessionKey { topic_name: String, topic_id: WireUuid, partition: i32 }` (Hash/Eq/Clone);
`CachedPartitionState` (Default/Clone/Eq; default `max_bytes=0`). A newly-allocated session has keys with
**both** identity halves resolved; incremental fetches carry only one half (Fetch v≤12 = name,
v≥13 = id). The asymmetry — forget OR-matches, merge finds-by-either-half then *inserts* on miss — is
where a forget-then-merge cycle under identity churn could create a shadow (the merge-only shadow is
already fixed: tests at lines 655 / 711).

## Refactor (small, behavior-preserving)

Extract the forget+merge block into a pure free fn taking the real request slices:

```rust
pub(crate) fn apply_incremental(
    partitions: &mut HashMap<FetchSessionKey, CachedPartitionState>,
    forgotten: &[ForgottenTopic],   // owned::fetch_request::ForgottenTopic
    topics: &[FetchTopic],          // owned::fetch_request::FetchTopic (name/id + per-partition fetch params)
);
```

`classify` calls it between the `partitions_before`/`partitions_after` snapshots (the `num_partitions`
gauge bookkeeping stays in `classify`). Behavior-preserving — gated by the existing fetch-session unit +
integration tests. (Exact request-type field names confirmed at implementation time.)

## Stateright model (`fetch_session_model.rs`, `#[cfg(test)]` descendant)

The cache is **stateful** across fetches, so a real state machine fits.

- **State:** the cached partition map projected to a hashable, sorted `Vec<(FetchSessionKey,
  max_bytes)>` (max_bytes distinguishes a real entry from a default-state shadow; other
  `CachedPartitionState` fields are irrelevant to the no-shadow invariant). Seeded from a
  **fully-resolved** session (keys carry both name+id), mirroring post-allocation reality.
- **Actions:** `IncrementalFetch { forgotten: Vec<(IdHalf, partition)>, subscribed: Vec<(IdHalf,
  partition, max_bytes)> }` over a tiny universe — topics `{A,B}` with ids `{U,V}` (a rename = same id,
  different name), `IdHalf ∈ { NameOnly, IdOnly, Both }`, partitions `{0,1}`, max_bytes `{1,2}`. Each
  action builds real `ForgottenTopic`/`FetchTopic` values and drives the real `apply_incremental`.
- **Safety asserts (`always` / per-transition):**
  - **no_shadow** (HEADLINE): no two distinct keys in the map share a logical partition — i.e. for all
    key pairs, NOT (`same partition` AND (`both names non-empty & equal` OR `both ids non-zero & equal`)).
  - **subscription_fidelity:** after an `IncrementalFetch`, a partition named in `forgotten` (and not
    re-subscribed) is absent; a subscribed partition is present, and its `max_bytes` equals the latest
    requested value (no stale/default override of a live subscription).
  - **no_orphan_default:** no key has default state (`max_bytes==0`) unless the client actually requested
    `max_bytes==0` (a merge-created entry always carries the request's value).
  - Non-vacuity (`sometimes`): a rename cycle (forget by name, re-subscribe by id with same id) occurs;
    an entry is updated in place; an entry is dropped; the map reaches ≥2 partitions.
- **Bounds (watchdog-guarded):** small (topics {A,B}, ids {U,V}, partitions {0,1}, short fetch
  sequences). Two configs (basic / wider sequence depth), exhaustive under the host memory watchdog;
  bound on unique-state count with a high truncation target if the generated count runs high (the
  compaction-model technique).

## proptest fuzz

Large-N random sequences of `IncrementalFetch` ops (random forgotten + subscribed sets over the small
topic/id/partition universe, random identity halves) driving `apply_incremental` against a reference
cache, asserting the same invariants (no-shadow, subscription fidelity, no-orphan-default) after every
step.

## RED handling

If a shadow-creating sequence is found, assess reachability via real Fetch RPCs (can a client drive
forget-by-name + subscribe-by-id for a renamed topic in one session?). If real → fix `apply_incremental`
(e.g. canonicalize identity on merge, or forget+merge must agree on matching) and re-verify GREEN
(RED→GREEN, recording the counterexample); if an unrealistic action → constrain the model + document.

## Out of scope (YAGNI)

- Session allocation / eviction / epoch validation (the surrounding `classify` machinery); the
  `num_partitions` gauge. The model targets the forget+merge partition-map mutation only.
- The fetch read path itself (modeled in #529).

## Verification discipline

- stateright watchdog-guarded (3 GB / 150 s; `[[feedback_bound_model_checkers]]`); proptest bounded.
  `cargo +nightly fmt` per-crate; `cargo clippy --all-targets -- -D warnings` clean.

## Success criteria

1. `apply_incremental` extracted; existing fetch-session unit + integration tests pass unchanged.
2. The model proves no-shadow + subscription-fidelity + no-orphan-default exhaustively (or produces a
   concrete counterexample handled RED→GREEN); witnesses satisfied; clean under the watchdog.
3. The proptest passes at large N.
4. fmt + clippy clean; broader broker suite unaffected. After this slice, the program is complete.
