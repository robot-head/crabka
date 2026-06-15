# Data-Path Composition Model — Design

**Date:** 2026-06-14
**Status:** Approved (design); spec under review
**Workstream:** A (formal verification) — the first **compositional / end-to-end** model, going beyond the 14
isolated per-slice bounded models.

## Goal

Verify the broker's canonical data-path guarantee **end-to-end** by composing already-verified cores at their
seams: a record produced with `acks=all` is **never lost across clean leader changes** and **every consumer
read is consistent** (read-your-writes, no dirty reads, no read regression); and under **unclean** leader
election, any loss is **exactly characterized** (only via unclean election, only on un-replicated offsets).

The 14 existing models each verify a core *in isolation*, assuming its neighbors behave. This model targets
the **seams** between them — `HWM ↔ truncation ↔ failover ↔ visibility` — which is where cross-component
interaction bugs hide and where the program currently has zero coverage. Honest discovery odds:
**moderate-to-good** (unlike the saturated per-slice space, this composition is genuinely unexplored).

## Scope

**In:** the topic-partition data path on a tiny cluster — produce → leader-follower replicate → commit (HWM) →
clean & unclean failover (with leader-epoch truncation) → consumer fetch (visibility).

**Out (deliberate):**
- **Metadata / raft consensus** — separately verified (`kraft_model`). The model takes leader/ISR decisions as
  driven actions, as if handed down by the controller.
- **Idempotent-producer dedup (`check_pure`)** — a v1 trim. Dedup is about *not duplicating*, largely
  orthogonal to the *not losing / read-consistency* property at the failover seam, and the weakest of the five
  seams while adding `(producer_id, epoch, seq)` state. The v1 spine is `HWM ↔ truncation ↔ failover ↔
  visibility`; dedup is a possible v2 extension.
- Multiple partitions / topics; `acks=0/1` (the property is the `acks=all` guarantee); transactions/EOS.

## Construction: wrap-real cores at the seams

A single new `stateright` model `crates/broker/src/data_path_model.rs` (`#[cfg(test)]` module in
`crabka-broker`, which can reach the broker-crate `pub(crate)` cores). It **drives the real pure cores** where
they are the seam under test, and abstracts producer + log as small vectors:

| Seam | Real core driven | Location |
|------|------------------|----------|
| commit (when a record is durable) | `ReplicaState::recompute_hw_for_leader_append`, `install_isr`, `update_follower_leo` | `crates/broker/src/replica_state.rs` |
| truncation (divergence on epoch change) | `epoch_and_offset_for_entries`, `end_offset_for_epoch` | `crates/log/src/leader_epoch_checkpoint.rs` |
| failover (winner selection) | `failover_one` (clean), `select_best_replica` (unclean) | `crates/broker/src/leader_election.rs`, `unclean_recovery.rs` |
| visibility (what a consumer sees) | `compute_visibility_window` | `crates/broker/src/handlers/fetch.rs` |

**Production change (small, expected):** the log truncation core is `pub(crate)` in `crabka-log`; widen
`epoch_and_offset_for_entries` + `end_offset_for_epoch` to `pub` so the broker-crate model can drive them
(greenfield — no compat concern). No logic change. The broker-side cores are already `pub(crate)` and reachable
from a same-crate test module.

**Adapter layer (the main effort + risk):** the four cores take different input types (`ReplicaState`, the
epoch-entries `Vec`, `ReplicaLogInfo`, the visibility-inputs struct). The model holds the composed cluster
state and projects it into each core's input on demand, folding results back. This glue is where most
implementation work and bug-risk lives.

## State & actions

**State (hashable projection):**
- per-broker `log[b]: Vec<epoch>` — offset is the index; the record "value" ≡ offset (minimizes state; sufficient
  for no-loss / divergence / truncation; corruption shows as an epoch mismatch at an offset).
- per-broker `leo[b]` (= `log[b].len()`).
- cluster `leader`, `leader_epoch`, `isr: Set<broker>`, `live: Set<broker>`, leader-authoritative `hwm`.
- ghost (bounded, for invariants): `committed_max` + the epoch of each offset ever `≤ hwm` (the durability
  obligation); per-consumer `observed[c]` (read-prefix offset) and the epoch it saw at each read offset.

**Actions:**
- `Produce` — leader appends `(leader_epoch)` to `log[leader]`.
- `Replicate(follower)` — follower fetches from leader: the **real truncation core** resolves any divergence
  point, the follower truncates and appends the next matching entry; bump `leo[follower]`.
- `AdvanceHwm` — **real `recompute_hw_for_leader_append`** over ISR members' LEOs.
- `ShrinkIsr`/`ExpandIsr(follower)` — **real `install_isr`** + the caught-up admission rule.
- `Die`/`Revive(broker)` — liveness.
- `Elect` — leader unavailable → new leader: **clean** config picks from `isr ∩ live` via **real `failover_one`**;
  **unclean** config picks from `live` via **real `select_best_replica`**. Bumps `leader_epoch`.
- `ConsumerFetch(c)` — **real `compute_visibility_window`**; advance `observed[c]`.

## Invariants

Per-transition `next_state` asserts + `Property::always`:
- **No-dirty-read:** a consumer never observes an offset `≥` the leader's HWM.
- **Read-monotonicity (the crux):** `observed[c]` never regresses, and an offset a consumer already read is
  never later rewritten with a different epoch underneath it.
- **No-committed-loss (clean):** an offset ever `≤ hwm` stays present with the same epoch in every future
  leader's log.
- **HWM-monotonic** except across an unclean election.
- **Loss-is-flagged-and-bounded (unclean):** any committed loss occurs *only* via an unclean election and
  *only* on offsets not replicated to the elected leader.
- Sanity (from the cores): `leader ∈ isr` under clean operation; `isr ⊆ live`.

**Non-vacuity witnesses (`sometimes`):** a clean failover occurs; an unclean failover causes a flagged loss; a
consumer reads then a failover happens; truncation actually removes a suffix; HWM reaches full replication.

## Configs

- **`data_clean`** — clean elections only (`isr ∩ live`). Asserts the strong properties: no-committed-loss +
  read-monotonicity MUST hold.
- **`data_unclean`** — allow unclean elections. Asserts the weaker *characterization*: loss only via unclean +
  bounded to un-replicated offsets; read-monotonicity may break only across an unclean election.

## Tractability (the central risk)

This is the largest model in the program; state explosion + OOM is the primary risk
(`[[feedback_bound_model_checkers]]` — it OOM'd the machine once). Controls:
- 3 brokers, 1 partition, `leader_epoch ≤ 3`, `log-len ≤ 3–4`, 1–2 consumers.
- Ghost state bounded (committed prefix + observed prefix, not a growing history).
- Keep monotonic generators (epoch, offsets) out of the fingerprint where they don't gate transitions (the
  recurring DPM-A1 / classic-group lesson).
- Bound exhaustiveness on **unique** states with a high `target_state_count` truncation backstop.
- **Mandatory host memory watchdog (3 GB / 150 s)** on every run.
- Fallback if it explodes: 2 followers / shorter logs / a single consumer / collapse the `data_unclean` config
  to fewer brokers.

## Incremental build

1. **Spine** — produce → replicate → HWM → fetch, single leader, no failover. Verify no-dirty-read +
   read-monotonicity + HWM-monotonic.
2. **Clean failover** — add `Elect` from ISR; verify no-committed-loss.
3. **Unclean failover + truncation** — add out-of-ISR election + the real truncation core; verify the
   loss-characterization.
4. **Scale-up + witnesses + finalize** — empirical bound scale-up under the watchdog; non-vacuity; nightly fmt.

## RED handling

A counterexample at a seam is the **goal** (the entire point of composition). If found, determine whether it is
a real cross-component bug vs. a model-faithfulness gap (an adapter mis-projecting a core's input). If real →
fix production RED→GREEN, recording the counterexample; if a faithfulness gap → fix the adapter and document.

## Verification discipline

- `stateright` wrap-real; watchdog-guarded runs (mandatory). `cargo +nightly fmt` per-crate; `cargo clippy
  --all-targets -- -D warnings` clean.
- Production change limited to widening two `crabka-log` fns to `pub` (no logic change); any further change only
  if a real seam bug is found.

## Success criteria

1. The composed model drives all four real cores at their seams over a tiny cluster, both configs.
2. `data_clean` proves no-committed-loss + read-monotonicity exhaustively (or finds + fixes a real seam bug
   RED→GREEN); `data_unclean` proves the loss-characterization. Witnesses satisfied; clean under the watchdog.
3. fmt + clippy clean; existing broker/log suites unaffected.
4. This establishes the compositional verification layer; further end-to-end paths (control, consumer-group,
   transaction) become follow-on slices if desired.
