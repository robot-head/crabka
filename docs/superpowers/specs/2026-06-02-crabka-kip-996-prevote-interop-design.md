# KIP-996 Pre-Vote — finish & correct (JVM-faithful response matching)

## Status

Design — approved for implementation planning.

## Background

KIP-996 adds a **pre-vote** round to KRaft elections: a voter that loses contact
with the leader first broadcasts a *non-binding* vote request at its **current**
epoch, and only bumps its epoch and starts a real (binding) election once a
majority grants the pre-vote. This stops a partitioned or rejoining voter from
forcing disruptive elections and term inflation: a voter that cannot win will
not perturb a healthy leader.

Contrary to the README KIP matrix (which marks KIP-996 ❌), the KIP-595 real-wire
work (#352) already implemented almost all of pre-vote:

| Layer | State |
|-------|-------|
| Pure state machine ([`core.rs`](../../../crates/raft/src/kraft/core.rs)) | ✅ `Role::Prospective`, pre-vote-before-epoch-bump, non-binding grants gated on `leader_id.is_none()`, `promote_to_candidate` defers the epoch bump until pre-vote majority, full unit tests |
| Events / actions ([`event.rs`](../../../crates/raft/src/kraft/event.rs), [`action.rs`](../../../crates/raft/src/kraft/action.rs)) | ✅ `pre_vote` flag threaded through |
| Async engine ([`controller.rs`](../../../crates/raft/src/kraft/controller.rs)) | ✅ drives `Prospective` → `broadcast_vote(epoch, pre_vote)` |
| **Request wire** ([`VoteRequest.json`](../../../crates/protocol/schemas/VoteRequest.json)) | ✅ Crabka sends `VoteRequest` **v2** with the real KIP-996 `PreVote` field |
| **Response wire** ([`transport.rs`](../../../crates/raft/src/kraft/transport.rs)) | ⚠️ **interop defect** — see below |

## The defect

Kafka's `VoteResponse` (v2) deliberately has **no** `PreVote` field. A candidate
knows which round a response belongs to from its **own state**: it is in
`Prospective` (pre-vote) or `Candidate` (real vote). Crabka instead smuggles the
flag back to itself in a private tagged field, `PRE_VOTE_ECHO_TAG` (`0x6b76`), on
the response.

That works Crabka↔Crabka but breaks in a mixed quorum. When a **JVM voter grants
a Crabka candidate's pre-vote**, the JVM does not echo the private tag, so
[`decode_vote`](../../../crates/raft/src/kraft/transport.rs) defaults
`pre_vote = false`. The core's `handle_vote_response` then takes the
`(Role::Candidate, false)` arm instead of `(Role::Prospective, true)`, finds no
match, and **silently drops the grant**. A Crabka-led election therefore cannot
gather pre-votes from JVM voters and stalls.

The interop target is confirmed as `mirror.gcr.io/apache/kafka:4.0.0`
([`server.rs`](../../../crates/raft/src/server.rs)), which implements
KIP-996, so this is a live bug, not a theoretical one. Slice 6's mixed-quorum
acceptance test
([`jvm_static_quorum_spike.rs`](../../../crates/broker/tests/jvm_static_quorum_spike.rs))
never exercised it: the JVM there joins as a **follower of an already-elected
Crabka leader**, so no pre-vote response ever crosses the JVM↔Crabka boundary. A
*contested* cross-impl election was an explicit Slice-6 follow-up.

## Goal

1. Make Crabka's pre-vote interoperate with a real KIP-996 JVM voter by matching
   vote responses the way Kafka does — from the candidate's own role + epoch —
   and deleting the private echo tag.
2. Prove it with a contested mixed JVM+Crabka election test (the previously
   broken direction).
3. Correct the KIP matrix.

## Non-goals (documented follow-ups, not pre-vote defects)

- The **reverse direction**: a JVM-*led* quorum with Crabka following a JVM
  leader. (Slice-6 follow-up; broader than pre-vote.)
- Any broader disruptive-server / check-quorum hardening beyond pre-vote.
- KIP-853 dynamic voters in the mixed quorum.

## Design

### Chosen approach: infer the round from role + epoch; delete the tag

Kafka's `VoteResponse` omits a PreVote field precisely because the candidate's
state already disambiguates. Crabka adopts the same model: the `pre_vote` flag is
removed from the **response** path entirely (wire, event, and core handler). The
**request** path is unchanged — `Action::SendVoteRequest { pre_vote }` is still
driven by role, and a responding voter still reads the request's `PreVote` field
to choose a binding vs non-binding grant.

Rejected alternatives:

- *Keep `pre_vote` on the event, populate it from the engine's current role at
  loop-processing time.* Removes the bug but keeps a redundant field that must
  stay in sync with role — extra drift surface, no benefit.
- *Keep the tag, fall back to role when it is absent.* A backwards-compatibility
  shim, forbidden for greenfield by `CLAUDE.md`.

### Why role + epoch is unambiguous

`handle_vote_response` matches on the candidate's current role, gated by
`epoch == self.state.leader_epoch`:

- `Prospective` at epoch `E` sends a pre-vote at `E`; a granting voter echoes its
  own (unbumped) epoch `E`. Match → tally pre-vote.
- On pre-vote majority the candidate promotes to `Candidate`, bumps to `E+1`, and
  sends a real vote at `E+1`; grants come back tagged `E+1`. Match → tally real
  vote.
- A **late** pre-vote grant (epoch `E`) arriving after promotion (now
  `leader_epoch == E+1`) fails the `epoch == leader_epoch` guard and is correctly
  ignored — a stale pre-vote must not count toward the real election.
- Real votes are only ever sent once `Candidate`, so there is no
  `(Prospective, real-vote)` case to confuse.

This is identical disambiguation to today, minus the wire flag.

### Code changes

| File | Change |
|------|--------|
| [`transport.rs`](../../../crates/raft/src/kraft/transport.rs) | Delete `PRE_VOTE_ECHO_TAG`; remove `pre_vote` from `PeerResponse::Vote`; `encode` emits a clean `VoteResponse` v2 with no unknown tagged field; `decode_vote` stops reading the tag. Rewrite the module doc comment that documents the hack. |
| [`event.rs`](../../../crates/raft/src/kraft/event.rs) | Drop `pre_vote` from `Event::ReceiveVoteResponse`. |
| [`core.rs`](../../../crates/raft/src/kraft/core.rs) | `handle_vote_response(from, epoch, vote_granted, now)` — drop the `pre_vote` param; match `(Role::Prospective, epoch==leader_epoch)` → pre-vote tally, `(Role::Candidate, epoch==leader_epoch)` → real-vote tally; higher-epoch-rejection fence unchanged. Update the `on_event` dispatch arm and affected unit tests. |
| [`controller.rs`](../../../crates/raft/src/kraft/controller.rs) | `response_to_event` builds `ReceiveVoteResponse` without `pre_vote`; fix the two unit-test event injections. |

The `Action::SendVoteRequest { pre_vote }` request path and the responder's
request-side `PreVote` handling are intentionally untouched.

### Test: contested mixed JVM+Crabka election

Add an `#[ignore]`, Docker-gated test to the existing harness in
[`jvm_static_quorum_spike.rs`](../../../crates/broker/tests/jvm_static_quorum_spike.rs)
(2 Crabka voters + 1 `mirror.gcr.io/apache/kafka:4.0.0` voter, static 3-voter set):

1. Boot the quorum; a Crabka node wins the initial election and the JVM follows
   (existing behaviour).
2. **Kill the Crabka leader.** The surviving Crabka voter is now 1 of 3 and
   *requires the JVM voter's pre-vote grant* to reach the 2/3 majority — exactly
   the previously-broken direction.
3. Assert: a new Crabka leader emerges at a strictly higher epoch, and a
   post-failover metadata record commits and becomes visible on the JVM
   controller (high-watermark advances past it).

Before the fix this hangs (the JVM's pre-vote grant is dropped, majority never
reached); after the fix the quorum recovers. The test doubles as the empirical
confirmation that Kafka 4.0.0 grants pre-votes on the wire. Use generous polling
deadlines to tolerate slow JVM container boot, mirroring the sibling tests; keep
it out of the default `cargo test` lane (JVM/Docker CI lane only).

The existing Crabka↔Crabka sim coverage in
[`core.rs`](../../../crates/raft/src/kraft/core.rs) and
[`kraft_engine_sim.rs`](../../../crates/raft/tests/kraft_engine_sim.rs) continues
to guard the pure pre-vote logic with no Docker dependency.

### Docs / matrix

- Flip KIP-996 ❌→✅ in the [README](../../../README.md) KIP matrix.
- Add a CHANGELOG entry in
  [`crates/raft/CHANGELOG.md`](../../../crates/raft/CHANGELOG.md).

## Risks & open questions

- **JVM pre-vote behaviour under partition.** After the Crabka leader is killed,
  the JVM voter's fetch to the dead leader must time out so it, too, is "not
  following a live leader" and will grant pre-votes. The JVM may also itself
  become prospective and race. The test asserts *cluster recovery* (some new
  Crabka leader at a higher epoch), not a specific winner, so it is robust to the
  race. If the JVM consistently wins, that is the reverse direction (a non-goal);
  the test topology gives the surviving Crabka voter the timing edge by killing
  only the Crabka leader, leaving a live Crabka voter to drive the pre-vote.
- **Determinism.** Dockerized JVM election timing is inherently loose; rely on
  polling with generous deadlines rather than fixed sleeps, as the existing
  `#[ignore]` JVM tests do.

## Acceptance criteria

- `PRE_VOTE_ECHO_TAG` and every read/write of it are gone; `VoteResponse` v2
  encodes with no unknown tagged field.
- `pre_vote` no longer appears on `Event::ReceiveVoteResponse`,
  `PeerResponse::Vote`, or `handle_vote_response`'s signature.
- All existing Crabka↔Crabka pre-vote unit/sim tests pass unchanged in behaviour.
- The new contested mixed-quorum test passes with the fix and (verified once)
  hangs/fails without it.
- `cargo fmt --check` and `cargo clippy --workspace --all-targets -D warnings`
  are clean.
- README KIP-996 row reads ✅; CHANGELOG updated.
