# Raft controller-quorum membership change — Design Spec

## Goal

Expose a manual `change_membership` API on the controller so operators
(and tests) can add/remove openraft voters at runtime. Close the
remaining 3 slice-10b-deferred tests using this API + a multi-broker
bootstrap fix.

## Background

The slice-10b follow-up unblocked 4 of the 6 rust integration tests
and 1 of the 3 deferred JVM acceptance tests by exposing openraft
timings on `BrokerConfig`. The remaining 3 tests all share the same
shape: a 3-broker cluster has one broker killed (or restarted), and
either the surviving openraft engines spam `AppendEntries` to the dead
voter indefinitely, or the JVM producer's bootstrap server points at
the just-killed broker so it can't reconnect.

Apache Kafka's KRaft handles this with **manual** operator-driven
voter-set changes via `kafka-metadata-quorum.sh add/remove-controller`:
the controller quorum is static until an operator explicitly mutates
it. Surviving voters tolerate a dead peer for as long as the operator
takes to act. KIP-853 (auto-membership) is a future direction; we
explicitly match the current Kafka behavior here.

## Architecture

Two independent changes, each in its own commit.

### 1. Expose `change_membership` on the controller

`crabka_raft::ControllerHandle` gains a `change_membership` method that
forwards to openraft's `Raft::change_membership(members, retain=false)`.
Same shape on `BrokerHandle` as a wrapper, so tests and production
callers don't need to reach into the raft crate.

```rust
// crates/raft/src/controller.rs
impl ControllerHandle {
    pub async fn change_membership(
        &self,
        new_voters: BTreeSet<NodeId>,
    ) -> Result<(), RaftError>;
}

// crates/broker/src/broker.rs
impl BrokerHandle {
    pub async fn change_membership(
        &self,
        new_voters: BTreeSet<NodeId>,
    ) -> Result<(), BrokerError>;
}
```

`new_voters` is the desired voter set (not a delta) — matches openraft's
`ChangeMembers::ReplaceAllVoters` semantics. `retain=false` means voters
not in the new set are fully removed (not demoted to learners). Caller
must be the openraft leader; otherwise openraft returns
`ForwardToLeader`.

For adding back a previously-removed voter, the existing static-init
path doesn't apply (the cluster is already initialized). We use
openraft's `add_learner` + `change_membership` two-step:

1. `add_learner(node_id, addr, blocking=true)` — replicates the log to
   the new node as a non-voting learner.
2. `change_membership({old_voters} ∪ {new_voter})` — promotes the
   learner to a voter.

So we also expose `BrokerHandle::add_learner(node_id, addr)` and the
caller composes them.

### 2. Fix the 3 deferred tests

**`leader_election::isr_expand_on_catchup`** — needs a true
remove/re-add cycle.

```rust
// Before kill: change_membership(voters - {3}) on a surviving broker.
// Survivor's openraft commits the joint config, stops spamming target=3.
// Kill broker 3.
// Reboot broker 3 with fresh BrokerConfig at same voter map.
// On a survivor: add_learner(3, addr).
// On a survivor: change_membership(voters + {3}).
// Assert: partition's ISR converges to {1,2,3} within 10s.
```

The reborn broker 3 starts with empty raft log; `add_learner` runs the
catch-up replication path. After promotion via `change_membership`,
broker 3 votes in elections and counts toward quorum again.

**`jvm_acceptance::acks_all_survives_leader_crash`** — the test's
`--bootstrap-server` arg points at the just-killed broker.
Switch to comma-separated all-3-bootstraps so the JVM producer can
find a survivor. No membership change needed; raft cluster is
2-of-3 quorum after the kill and continues to serve.

**`jvm_acceptance::three_node_replication_byte_compare`** — investigate
during implementation. If it's the same bootstrap issue, the same fix
applies. If it's something else, escalate.

## Components

```
crates/raft/src/
├── controller.rs                    # MODIFIED — change_membership + add_learner public methods
└── error.rs                         # MAYBE MODIFIED — surface openraft's ChangeMembershipError variants

crates/broker/src/
└── broker.rs                        # MODIFIED — change_membership + add_learner on BrokerHandle

crates/broker/tests/
├── leader_election.rs               # MODIFIED — un-#[ignore] isr_expand_on_catchup + use new API
└── jvm_acceptance.rs                # MODIFIED — un-skip 2 tests; multi-bootstrap producer args

.github/workflows/ci.yml             # MODIFIED — drop --skip flags
```

## Error handling

`change_membership` can fail with:
- `ForwardToLeader` — caller hit a non-leader broker. Test helpers iterate
  the cluster looking for the leader; production callers retry via
  metadata refresh (same pattern as `submit_change`).
- `EmptyMembership` — caller passed an empty `new_voters`. Surface as
  `BrokerError::InvalidArgument` (or equivalent) — never quietly accept.
- `LearnerIsLagging` — `add_learner(blocking=true)` blocks until the
  learner catches up; if it doesn't within the call's timeout (we'll
  use 30s), surface as an error.

## Testing

Existing-coverage assumption: the 3 deferred tests are the acceptance
criteria. We don't add new unit tests for `change_membership` itself
because openraft's own test suite covers correctness; we add only
crabka-level integration coverage via the un-ignored tests.

Acceptance:
- `cargo test -p crabka-broker --test leader_election` — 4/4 pass on
  Linux (currently 3/4)
- `cargo test -p crabka-broker --test jvm_acceptance --ignored` — 9/9
  pass on Linux with Docker (currently 7/9)
- `cargo test --workspace` — all green on ubuntu/macos/windows
- CI workflow has no remaining `--skip` flags in `broker-jvm-acceptance`

## Out of scope

- Admin RPC (the `kafka-metadata-quorum.sh` server-side equivalent). A
  manual API for tests/ops is sufficient until we have a use case.
- Auto-remove on broker death. Matches Kafka KRaft behavior; covered by
  manual API only.
- KIP-853 (dynamic raft membership). Future direction.

## YAGNI decisions

- No `set_learners_only` helper — callers compose `add_learner` and
  `change_membership` themselves.
- No batch membership-change API — callers can call `change_membership`
  multiple times for staged changes.
- No metrics / observability for membership changes beyond the openraft
  default tracing.
