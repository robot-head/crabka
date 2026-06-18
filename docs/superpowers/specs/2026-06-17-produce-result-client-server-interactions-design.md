# Produce Result Client/Server Interaction Model - Design

**Date:** 2026-06-17
**Status:** Approved direction; implementation pending
**Workstream:** Formal failover model extension

## Goal

Extend the client/server failover model so it represents the produce exchange
more explicitly. The current model proves that a prepared batch can survive
clean failover and that retry does not duplicate committed data, but it
compresses several client-visible server responses into broad send/retry states.

This addendum models the response states a producer actually has to reason
about during failover:

- `NotLeader`: the request reached a live broker that is not the current leader.
- `TimedOutUnknown`: the client does not know whether the broker appended the
  batch.
- `AppendedUnacked`: the leader accepted the batch, but it is not yet committed.
- `Acked`: the batch reached HWM and the client has a committed offset.

The objective is to prove that these responses compose safely with metadata
refresh, reroute, and clean leader election: the client either converges to the
acknowledged batch or fails within the bounded retry budget, without appending a
duplicate or advancing sequence state early.

## Scope

**In:**
- One producer, one topic-partition, one prepared batch, and three brokers.
- Cached leader metadata, metadata refresh, and wrong-leader responses.
- Unknown produce outcome after timeout where the batch may or may not have
  been appended before the response was lost.
- Retry after `NotLeader`, retry after unknown outcome, and retry after clean
  failover.
- Existing idempotent producer-state check via `check_pure`.
- Existing clean-election/HWM prefix constraints.

**Out:**
- Multiple in-flight batches and batching reorder.
- Transaction coordinator or producer fencing by a second producer.
- Full wall-clock backoff timing. The model keeps bounded abstract retry
  counters rather than real time.
- Unclean leader election.

## Model Shape

Extend `crates/broker/src/client_server_failover_model.rs` rather than creating
a third independent model. The existing model already has the right safety
context: logs, HWM, cached leader, live brokers, producer entry projection, and
non-vacuity witnesses.

Add a small client/server response layer:

- `ProduceResult::NotLeader`
- `ProduceResult::TimedOutUnknown`
- `ProduceResult::AppendedUnacked`
- `ProduceResult::Acked`

Track it in state as the latest client-visible result, plus compact counters for
send attempts and metadata refreshes. The counters should be bounded and part of
`within_boundary`.

## Data Flow

`ClientSend` starts the first request when the batch is empty. `ClientRetry`
resends the same prepared batch after `NotLeader`, `TimedOutUnknown`,
`AppendedUnacked`, or failover refresh. Both actions route through
`cached_leader`.

If the cached leader is dead or not the actual leader, the result becomes
`NotLeader`, `refresh_needed` is set, and the producer sequence must not
advance.

If the cached leader is the live leader, the model chooses among faithful server
outcomes:

- append and return `AppendedUnacked`;
- append and lose the response, recorded as `TimedOutUnknown`;
- reject as duplicate and return the original offset once the accepted batch is
  known and present on the leader;
- ack only after HWM reaches the accepted batch.

`RefreshMetadata` updates `cached_leader` to the current clean leader and clears
`refresh_needed`. Clean `ElectClean` continues to require the HWM prefix.

## Properties

Keep the existing safety properties and add:

- **Unknown outcome is idempotent:** retry after `TimedOutUnknown` can return
  the original offset but cannot append a second copy of the same
  `(producer_epoch, base_sequence)`.
- **NotLeader does not mutate sequence:** wrong-leader responses force refresh
  or retry without incrementing `next_sequence`.
- **Unacked append is not acknowledged early:** `AppendedUnacked` cannot become
  `Acked` until HWM advances and the leader contains the accepted batch.
- **Bounded interaction churn:** send attempts and metadata refreshes remain
  below small caps while the model reaches `Acked` or `Failed`.
- **Clean failover preserves unknown outcomes:** if the client times out after
  an append and then fails over, retry on the new clean leader resolves as the
  same accepted batch, not a new append.

Add non-vacuity witnesses for:

- `NotLeader` response;
- `TimedOutUnknown` response;
- retry after unknown outcome and failover;
- `AppendedUnacked` followed by HWM and `Acked`;
- duplicate response after unknown outcome.

## Tractability

The model must stay small:

- keep one log slot and one prepared batch;
- cap send attempts and metadata refreshes;
- keep response state as a compact enum, not a log of responses;
- prefer witnesses over long histories;
- raise `MAX_DEPTH` and `MAX_STATES` only after measuring the state count.

If the state space grows too quickly, prioritize the unknown-outcome and
wrong-leader response paths before adding more replication interleavings.

## Verification

Run the broker model with `--nocapture` and require it to stay under its depth
and state caps. Rerun the existing client model and sender regression because
the PR still claims fast client failover. Finish with formatting, clippy, and a
whole-diff check.

Expected commands:

```bash
TMPDIR=/home/matt/.codex/worktrees/bdee/crabka/target/tmp CARGO_BUILD_JOBS=1 cargo test --locked -p crabka-broker --lib client_server_failover_preserves_acked_batch -- --nocapture
TMPDIR=/home/matt/.codex/worktrees/bdee/crabka/target/tmp CARGO_BUILD_JOBS=1 cargo test --locked -p crabka-client-producer --lib client_failover_recovers_or_fails_boundedly -- --nocapture
TMPDIR=/home/matt/.codex/worktrees/bdee/crabka/target/tmp CARGO_BUILD_JOBS=1 cargo test --locked -p crabka-client-producer --lib dead_leader_failover_refreshes_and_reroutes_before_timeout_churn -- --nocapture
cargo fmt --check
TMPDIR=/home/matt/.codex/worktrees/bdee/crabka/target/tmp CARGO_BUILD_JOBS=1 cargo clippy --locked -p crabka-client-producer -p crabka-broker --all-targets -- -D warnings
git diff --check
```

## Success Criteria

1. The broker model includes explicit produce-result responses.
2. Unknown-outcome retry and wrong-leader retry are both reachable witnesses.
3. Ack still requires HWM and a leader that contains the accepted batch.
4. Retry after timeout/failover preserves producer identity and offset.
5. The extended checker remains bounded and completes reliably in local runs.
