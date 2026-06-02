# KIP-996 Pre-Vote Interop Fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Crabka's KIP-996 pre-vote interoperate with a real KIP-996 JVM voter by matching vote responses the way Kafka does — from the candidate's own role + epoch — deleting the non-interoperable private `PRE_VOTE_ECHO_TAG`.

**Architecture:** Pre-vote is already implemented end to end (state machine, engine, `VoteRequest` v2 `PreVote` field). The only defect is that the candidate learns a response's round from a private tagged field Crabka echoes to itself on `VoteResponse`; a JVM voter never echoes it, so a JVM pre-vote grant is dropped and a Crabka-led election stalls. The fix removes `pre_vote` from the entire *response* path (the `ReplyVote` action, the `ReceiveVoteResponse` event, the `PeerResponse::Vote` wire body, and the echo tag) and has the core infer the round from its own `Prospective`/`Candidate` role gated by epoch — exactly why Kafka's `VoteResponse` has no PreVote field. The *request* path (which carries the real on-wire `PreVote`) is untouched.

**Tech Stack:** Rust 2024, `crabka-raft` (hand-rolled KRaft state machine + async engine), `crabka-protocol` (generated Kafka wire codecs), `crabka-broker` (integration tests), Docker + `apache/kafka:4.0.0` for the JVM interop lane.

**Spec:** `docs/superpowers/specs/2026-06-02-crabka-kip-996-prevote-interop-design.md`

---

## File map

| File | Responsibility | Change |
|------|----------------|--------|
| `crates/raft/src/kraft/action.rs` | Core output actions | Drop `pre_vote` from `Action::ReplyVote` |
| `crates/raft/src/kraft/event.rs` | Core input events | Drop `pre_vote` from `Event::ReceiveVoteResponse` |
| `crates/raft/src/kraft/core.rs` | Pure state machine | Role-based `handle_vote_response`; drop `pre_vote` from `ReplyVote` constructions; update + add tests |
| `crates/raft/src/kraft/transport.rs` | KIP-595 wire codec | Delete `PRE_VOTE_ECHO_TAG`; drop `pre_vote` from `PeerResponse::Vote`; clean encode/decode; rewrite module doc; update round-trip test |
| `crates/raft/src/kraft/controller.rs` | Async engine | `run_inbound_reply`, `response_to_event`, `elect_leader_with_helper` + 2 test injections |
| `crates/raft/tests/sim_harness/mod.rs` | Deterministic multi-node bus | Drop `pre_vote` from the `ReplyVote → ReceiveVoteResponse` translation (this makes the existing sim election tests the regression guard) |
| `crates/broker/tests/jvm_static_quorum_spike.rs` | JVM interop acceptance | Add a second `#[ignore]` contested-election test |
| `README.md` | KIP matrix | KIP-996 ❌ → ✅ |
| `crates/raft/CHANGELOG.md` | Changelog | Add Unreleased entry |

Tasks 1 is a single compile-coupled refactor (removing a struct field touches every match site at once, so the crate cannot build half-done). Task 2 and Task 3 are independent and could run in parallel after Task 1.

---

## Task 1: Role-based vote-response matching

**Files:**
- Modify: `crates/raft/src/kraft/action.rs`
- Modify: `crates/raft/src/kraft/event.rs`
- Modify: `crates/raft/src/kraft/core.rs`
- Modify: `crates/raft/src/kraft/transport.rs`
- Modify: `crates/raft/src/kraft/controller.rs`
- Modify: `crates/raft/tests/sim_harness/mod.rs`

### Edits (the crate will not compile until all are applied)

- [ ] **Step 1: `action.rs` — drop `pre_vote` from `ReplyVote`**

Replace:

```rust
    /// Reply to a Vote request.
    ReplyVote {
        to: NodeId,
        epoch: LeaderEpoch,
        granted: bool,
        pre_vote: bool,
    },
```

with:

```rust
    /// Reply to a Vote request. (Kafka's `VoteResponse` carries no pre-vote
    /// flag — the candidate matches the reply to its round by its own role, so
    /// the responder does not echo `pre_vote`.)
    ReplyVote {
        to: NodeId,
        epoch: LeaderEpoch,
        granted: bool,
    },
```

- [ ] **Step 2: `event.rs` — drop `pre_vote` from `ReceiveVoteResponse`**

Replace:

```rust
    /// A peer answered our Vote.
    ReceiveVoteResponse {
        from: NodeId,
        epoch: LeaderEpoch,
        vote_granted: bool,
        pre_vote: bool,
    },
```

with:

```rust
    /// A peer answered our Vote. The round (pre-vote vs real vote) is NOT on the
    /// wire — the candidate infers it from its own `Prospective`/`Candidate`
    /// role + epoch (KIP-996; mirrors Kafka's field-less `VoteResponse`).
    ReceiveVoteResponse {
        from: NodeId,
        epoch: LeaderEpoch,
        vote_granted: bool,
    },
```

(Leave `Event::ReceiveVoteRequest.pre_vote` — the *request* path — unchanged.)

- [ ] **Step 3: `core.rs` — `on_event` dispatch arm**

Replace:

```rust
            Event::ReceiveVoteResponse {
                from,
                epoch,
                vote_granted,
                pre_vote,
            } => self.handle_vote_response(log, from, epoch, vote_granted, pre_vote, now),
```

with:

```rust
            Event::ReceiveVoteResponse {
                from,
                epoch,
                vote_granted,
            } => self.handle_vote_response(log, from, epoch, vote_granted, now),
```

- [ ] **Step 4: `core.rs` — rewrite `handle_vote_response` to match on role**

Replace the whole method:

```rust
    fn handle_vote_response(
        &mut self,
        log: &dyn LogView,
        from: NodeId,
        epoch: LeaderEpoch,
        vote_granted: bool,
        pre_vote: bool,
        now: SimInstant,
    ) -> Vec<Action> {
        // A higher-epoch rejection fences us: step down to that epoch.
        if !vote_granted && epoch > self.state.leader_epoch {
            let mut actions = Vec::new();
            self.transition_to_unattached(epoch, now, &mut actions);
            return actions;
        }
        if !vote_granted {
            return Vec::new();
        }
        match (&mut self.role, pre_vote) {
            (Role::Prospective { granted, .. }, true) if epoch == self.state.leader_epoch => {
                granted.insert(from);
                if self.tally_prevote_reached_majority() {
                    self.promote_to_candidate(log, now)
                } else {
                    Vec::new()
                }
            }
            (Role::Candidate { granted, .. }, false) if epoch == self.state.leader_epoch => {
                granted.insert(from);
                if self.tally_candidate_reached_majority() {
                    self.promote_to_leader(log)
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        }
    }
```

with:

```rust
    fn handle_vote_response(
        &mut self,
        log: &dyn LogView,
        from: NodeId,
        epoch: LeaderEpoch,
        vote_granted: bool,
        now: SimInstant,
    ) -> Vec<Action> {
        // A higher-epoch rejection fences us: step down to that epoch.
        if !vote_granted && epoch > self.state.leader_epoch {
            let mut actions = Vec::new();
            self.transition_to_unattached(epoch, now, &mut actions);
            return actions;
        }
        if !vote_granted {
            return Vec::new();
        }
        // Match the grant to our round by our OWN role + epoch — exactly as
        // Kafka does (its `VoteResponse` carries no pre-vote flag). `Prospective`
        // ⇒ this is a pre-vote grant; `Candidate` ⇒ a real-vote grant. The epoch
        // guard drops a stale grant from a superseded round (e.g. a late pre-vote
        // grant at epoch E arriving after we bumped to E+1 and became Candidate).
        match &mut self.role {
            Role::Prospective { granted, .. } if epoch == self.state.leader_epoch => {
                granted.insert(from);
                if self.tally_prevote_reached_majority() {
                    self.promote_to_candidate(log, now)
                } else {
                    Vec::new()
                }
            }
            Role::Candidate { granted, .. } if epoch == self.state.leader_epoch => {
                granted.insert(from);
                if self.tally_candidate_reached_majority() {
                    self.promote_to_leader(log)
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        }
    }
```

- [ ] **Step 5: `core.rs` — drop `pre_vote` from the two `ReplyVote` constructions in `handle_vote_request`**

There are **two** `Action::ReplyVote { … pre_vote }` pushes in `handle_vote_request`, with different `granted` values. Fix both.

(a) The early fenced reply (candidate epoch below ours). Replace:

```rust
            actions.push(Action::ReplyVote {
                to: from,
                epoch: self.state.leader_epoch,
                granted: false,
                pre_vote,
            });
```

with:

```rust
            actions.push(Action::ReplyVote {
                to: from,
                epoch: self.state.leader_epoch,
                granted: false,
            });
```

(b) The final reply at the bottom of the method. Replace:

```rust
        actions.push(Action::ReplyVote {
            to: from,
            epoch: self.state.leader_epoch,
            granted,
            pre_vote,
        });
```

with:

```rust
        actions.push(Action::ReplyVote {
            to: from,
            epoch: self.state.leader_epoch,
            granted,
        });
```

Keep every other use of the `pre_vote` parameter inside `handle_vote_request` (it still decides the binding vs non-binding grant) untouched.

- [ ] **Step 6: `transport.rs` — delete the echo tag, drop `pre_vote` from `PeerResponse::Vote`, clean encode/decode**

(a) Rewrite the module doc paragraph. Replace:

```rust
/// `pre_vote` has no field in the JVM `VoteResponse`, so the responder echoes it
/// back in an internal tagged field ([`PRE_VOTE_ECHO_TAG`]) that a JVM peer
/// harmlessly ignores; the candidate reads it to match the response to its
/// pre-vote vs vote round (keeping the loop's `ReceiveVoteResponse` handling
/// unchanged).
```

with:

```rust
/// Kafka's `VoteResponse` carries no pre-vote field: a candidate matches a reply
/// to its round from its own `Prospective`/`Candidate` role, so Crabka encodes a
/// byte-faithful `VoteResponse` and the core infers the round itself (KIP-996).
```

(b) Delete the const:

```rust
    /// Internal tagged-field tag carrying the `pre_vote` echo on a `VoteResponse`
    /// (a single byte: 1 = pre-vote round, 0 = real vote). Picked well above any
    /// JVM-assigned tag so a real Kafka voter ignores it as unknown.
    const PRE_VOTE_ECHO_TAG: u32 = 0x6b76; // "kv"
```

(c) `PeerResponse::Vote` — drop `pre_vote`:

```rust
        Vote {
            epoch: LeaderEpoch,
            granted: bool,
        },
```

(d) In `PeerResponse::encode`, replace the whole `PeerResponse::Vote { … }` arm:

```rust
                PeerResponse::Vote {
                    epoch,
                    granted,
                    pre_vote,
                } => {
                    let mut resp = VoteResponse {
                        error_code: 0,
                        topics: vec![vote_resp::TopicData {
                            topic_name: METADATA_TOPIC.to_string(),
                            partitions: vec![vote_resp::PartitionData {
                                partition_index: METADATA_PARTITION,
                                error_code: 0,
                                leader_id: -1,
                                leader_epoch: epoch_to_wire(*epoch),
                                vote_granted: *granted,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    // Echo pre_vote in an internal tagged field so the candidate
                    // can match the response to its round (the JVM schema has no
                    // pre_vote response field; an unknown tag is ignored there).
                    resp.unknown_tagged_fields = UnknownTaggedFields(vec![UnknownTaggedField {
                        tag: PRE_VOTE_ECHO_TAG,
                        bytes: Bytes::from_static(if *pre_vote { &[1u8] } else { &[0u8] }),
                    }]);
                    encode_body(&resp, VOTE_VERSION)
                }
```

with:

```rust
                PeerResponse::Vote { epoch, granted } => {
                    let resp = VoteResponse {
                        error_code: 0,
                        topics: vec![vote_resp::TopicData {
                            topic_name: METADATA_TOPIC.to_string(),
                            partitions: vec![vote_resp::PartitionData {
                                partition_index: METADATA_PARTITION,
                                error_code: 0,
                                leader_id: -1,
                                leader_epoch: epoch_to_wire(*epoch),
                                vote_granted: *granted,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    encode_body(&resp, VOTE_VERSION)
                }
```

(e) Rewrite `decode_vote`:

```rust
        /// Decode a Vote response body (api 52).
        #[must_use]
        pub fn decode_vote(buf: &[u8]) -> Option<Self> {
            let mut cur = buf;
            let resp = VoteResponse::decode(&mut cur, VOTE_VERSION).ok()?;
            let p = resp.topics.first()?.partitions.first()?;
            let pre_vote = resp
                .unknown_tagged_fields
                .0
                .iter()
                .find(|f| f.tag == PRE_VOTE_ECHO_TAG)
                .is_some_and(|f| f.bytes.first().copied() == Some(1));
            Some(PeerResponse::Vote {
                epoch: epoch_from_wire(p.leader_epoch),
                granted: p.vote_granted,
                pre_vote,
            })
        }
```

to:

```rust
        /// Decode a Vote response body (api 52). The round (pre-vote vs real) is
        /// not on the wire — the engine infers it from the candidate's role.
        #[must_use]
        pub fn decode_vote(buf: &[u8]) -> Option<Self> {
            let mut cur = buf;
            let resp = VoteResponse::decode(&mut cur, VOTE_VERSION).ok()?;
            let p = resp.topics.first()?.partitions.first()?;
            Some(PeerResponse::Vote {
                epoch: epoch_from_wire(p.leader_epoch),
                granted: p.vote_granted,
            })
        }
```

(f) If `UnknownTaggedField` / `UnknownTaggedFields` are now unused in this module, remove them from the `use crabka_protocol::tagged_fields::{…}` import to avoid an `unused_imports` warning. (Check after building — other arms may still use them; if so, leave the import.)

- [ ] **Step 7: `transport.rs` — update the round-trip test**

Replace:

```rust
        #[test]
        fn vote_response_round_trips_with_pre_vote_echo() {
            for pre_vote in [false, true] {
                let resp = PeerResponse::Vote {
                    epoch: 3,
                    granted: true,
                    pre_vote,
                };
                assert!(PeerResponse::decode_vote(&resp.encode()) == Some(resp));
            }
        }
```

with:

```rust
        #[test]
        fn vote_response_round_trips() {
            let resp = PeerResponse::Vote {
                epoch: 3,
                granted: true,
            };
            assert!(PeerResponse::decode_vote(&resp.encode()) == Some(resp));
        }

        #[test]
        fn vote_response_decodes_without_any_tagged_field() {
            // A JVM `VoteResponse` carries no Crabka echo tag; decode must still
            // succeed (regression guard for the removed `PRE_VOTE_ECHO_TAG`).
            let encoded = PeerResponse::Vote {
                epoch: 7,
                granted: true,
            }
            .encode();
            let decoded = PeerResponse::decode_vote(&encoded).unwrap();
            assert!(decoded == PeerResponse::Vote { epoch: 7, granted: true });
        }
```

- [ ] **Step 8: `controller.rs` — `run_inbound_reply`**

Replace the default-response init:

```rust
        let mut resp = wire::PeerResponse::Vote {
            epoch: self.core.quorum_state().leader_epoch,
            granted: false,
            pre_vote: false,
        };
```

with:

```rust
        let mut resp = wire::PeerResponse::Vote {
            epoch: self.core.quorum_state().leader_epoch,
            granted: false,
        };
```

and replace the `ReplyVote` mapping:

```rust
            if let Action::ReplyVote {
                epoch,
                granted,
                pre_vote,
                ..
            } = action
            {
                resp = wire::PeerResponse::Vote {
                    epoch,
                    granted,
                    pre_vote,
                };
            } else {
```

with:

```rust
            if let Action::ReplyVote { epoch, granted, .. } = action {
                resp = wire::PeerResponse::Vote { epoch, granted };
            } else {
```

- [ ] **Step 9: `controller.rs` — `response_to_event`**

Replace:

```rust
        self::api_key::VOTE => match wire::PeerResponse::decode_vote(body)? {
            wire::PeerResponse::Vote {
                epoch,
                granted,
                pre_vote,
            } => Some(Event::ReceiveVoteResponse {
                from: peer,
                epoch,
                vote_granted: granted,
                pre_vote,
            }),
            _ => None,
        },
```

with:

```rust
        self::api_key::VOTE => match wire::PeerResponse::decode_vote(body)? {
            wire::PeerResponse::Vote { epoch, granted } => Some(Event::ReceiveVoteResponse {
                from: peer,
                epoch,
                vote_granted: granted,
            }),
            _ => None,
        },
```

- [ ] **Step 10: `controller.rs` — fix the two test injections in `elect_leader_with_helper`**

Remove the `pre_vote: true,` line from the first `ReceiveVoteResponse` injection and the `pre_vote: false,` line from the second, so they read:

```rust
        ctrl.inject_event(Event::ReceiveVoteResponse {
            from: helper,
            epoch: 0,
            vote_granted: true,
        })
        .await
        .unwrap();
        // Candidate round runs at the bumped epoch 1.
        ctrl.inject_event(Event::ReceiveVoteResponse {
            from: helper,
            epoch: 1,
            vote_granted: true,
        })
        .await
        .unwrap();
```

- [ ] **Step 11: `sim_harness/mod.rs` — drop `pre_vote` from the `ReplyVote → ReceiveVoteResponse` translation**

Replace:

```rust
            Action::ReplyVote {
                to,
                epoch,
                granted,
                pre_vote,
            } => {
                self.send(
                    id,
                    to,
                    Event::ReceiveVoteResponse {
                        from: id,
                        epoch,
                        vote_granted: granted,
                        pre_vote,
                    },
                );
            }
```

with:

```rust
            Action::ReplyVote { to, epoch, granted } => {
                self.send(
                    id,
                    to,
                    Event::ReceiveVoteResponse {
                        from: id,
                        epoch,
                        vote_granted: granted,
                    },
                );
            }
```

Leave `broadcast_vote_request` and `Action::SendVoteRequest { epoch, pre_vote }` untouched (request path). This change means the harness now delivers vote responses with **no** pre-vote signal — so the existing sim election tests exercise role-based matching end to end.

- [ ] **Step 12: `core.rs` — drop `pre_vote` from existing `ReceiveVoteResponse` lines in unit tests**

In `crates/raft/src/kraft/core.rs` there are several `Event::ReceiveVoteResponse { … pre_vote: true/false }` in `#[cfg(test)] mod tests` (in `prevote_majority_promotes_to_candidate_and_bumps_epoch`, `real_majority_promotes_to_leader_and_appends_leader_change`, `leader_advances_hwm_at_majority_fetch_offset`, `leader_holds_hwm_for_prior_epoch_entries_until_current_epoch_committed`, and `leader_detects_divergence_and_returns_truncate`). Remove the `pre_vote: …,` line from **every** `ReceiveVoteResponse` literal in the test module. The surrounding assertions (role/epoch transitions) stay — they now prove role-based matching.

- [ ] **Step 13: Build the whole crate**

Run: `cargo build -p crabka-raft --all-targets`
Expected: compiles with no errors. (If `unused_imports` fires on `UnknownTaggedField`/`UnknownTaggedFields`, apply Step 6(f).)

- [ ] **Step 14: Run the raft test suite (regression guard)**

Run: `cargo test -p crabka-raft`
Expected: PASS — in particular the existing `kraft::core` pre-vote tests, the `kraft::controller` engine tests (`elect_leader_with_helper`), and the `sim_harness`-driven multi-node election tests in `crates/raft/tests/kraft_sim.rs` / `kraft_engine_sim.rs`. These now run with no on-wire pre-vote signal, so passing proves the candidate counts grants by role alone.

- [ ] **Step 15: Add a focused core test naming the JVM-interop semantics**

In `crates/raft/src/kraft/core.rs`, inside `#[cfg(test)] mod tests`, add:

```rust
    #[test]
    fn prospective_counts_grant_with_no_wire_prevote_signal() {
        // A JVM voter's `VoteResponse` carries no pre-vote flag. The candidate
        // must still count the grant as a PRE-VOTE because it is Prospective —
        // this is the KIP-996 interop fix (was dropped by the old echo-tag path).
        let mut m = machine(1, &[1, 2, 3]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        m.on_event(Event::ElectionTimeout, &log, SimInstant(2000)); // → Prospective, epoch 0
        assert!(matches!(m.role(), Role::Prospective { .. }));
        let actions = m.on_event(
            Event::ReceiveVoteResponse {
                from: 2,
                epoch: 0,
                vote_granted: true,
            },
            &log,
            SimInstant(2001),
        );
        // Pre-vote majority (self + 2) → promote to Candidate and bump the epoch.
        assert!(matches!(m.role(), Role::Candidate { .. }));
        assert!(m.quorum_state().leader_epoch == 1);
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::SendVoteRequest {
                pre_vote: false,
                epoch: 1
            }
        )));
    }

    #[test]
    fn stale_prevote_grant_ignored_after_promotion() {
        // A late pre-vote grant at the old epoch must not be miscounted toward
        // the real election once we have promoted to Candidate at epoch+1.
        let mut m = machine(1, &[1, 2, 3]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
        m.on_event(
            Event::ReceiveVoteResponse {
                from: 2,
                epoch: 0,
                vote_granted: true,
            },
            &log,
            SimInstant(2001),
        ); // → Candidate @ epoch 1
        assert!(matches!(m.role(), Role::Candidate { .. }));
        // A duplicate/late pre-vote grant still tagged epoch 0 arrives.
        let actions = m.on_event(
            Event::ReceiveVoteResponse {
                from: 3,
                epoch: 0,
                vote_granted: true,
            },
            &log,
            SimInstant(2002),
        );
        // Epoch guard (0 != 1) drops it: we stay Candidate, do NOT become leader.
        assert!(matches!(m.role(), Role::Candidate { .. }));
        assert!(!m.role().is_leader());
        assert!(actions.is_empty());
    }
```

- [ ] **Step 16: Run the new tests**

Run: `cargo test -p crabka-raft prospective_counts_grant_with_no_wire_prevote_signal stale_prevote_grant_ignored_after_promotion`
Expected: PASS (2 tests).

- [ ] **Step 17: Format + lint**

Run: `cargo fmt -p crabka-raft` then `cargo clippy -p crabka-raft --all-targets -- -D warnings`
Expected: no diffs from fmt beyond your edits; clippy clean.

- [ ] **Step 18: Commit**

```bash
git add crates/raft/src/kraft/action.rs crates/raft/src/kraft/event.rs \
        crates/raft/src/kraft/core.rs crates/raft/src/kraft/transport.rs \
        crates/raft/src/kraft/controller.rs crates/raft/tests/sim_harness/mod.rs
git commit -m "fix(raft): KIP-996 — match vote responses by role, drop PRE_VOTE_ECHO_TAG

Kafka's VoteResponse has no pre-vote field; a candidate infers the round
from its own Prospective/Candidate role + epoch. The private echo tag was
non-interoperable: a JVM voter never echoes it, so its pre-vote grant was
dropped and a Crabka-led election stalled in a mixed quorum. Remove pre_vote
from the entire response path (ReplyVote action, ReceiveVoteResponse event,
PeerResponse::Vote wire body) and have the core decide by role.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Contested mixed JVM+Crabka election test

**Files:**
- Modify: `crates/broker/tests/jvm_static_quorum_spike.rs` (add a second `#[ignore]` test reusing the file's helpers)

**Why this proves the fix:** after the Crabka leader is killed, only 1 Crabka voter + the JVM voter remain of 3, so majority (2) is unreachable unless the JVM grants the surviving Crabka candidate's **pre-vote** and its real vote. Before the fix the JVM's pre-vote grant is dropped and the survivor is stuck in `Prospective` forever — the test times out. After the fix the quorum recovers. The JVM is configured to release the dead leader quickly (`fetch.timeout.ms=1000`) but be slow to launch its own candidacy (`election.timeout.ms=8000`), and the Crabka survivor uses a short election timeout, so the surviving Crabka node reliably wins (avoiding the unfinished Crabka-follows-JVM-leader direction).

- [ ] **Step 1: Add the test function**

Append to `crates/broker/tests/jvm_static_quorum_spike.rs` (it already has `KAFKA_IMAGE`, `kafka_cluster_id_string`, `crabka_controller_config`, `docker_rm`, and `mod support`):

```rust
const CONTESTED_CONTAINER: &str = "crabka-kip996-contested";

/// KIP-996 CONTESTED-ELECTION ACCEPTANCE TEST (Docker-gated, `#[ignore]`).
///
/// 2 Crabka voters (ids 1,2) + 1 `apache/kafka:4.0.0` voter (id 3) form a static
/// 3-voter quorum. After the Crabka leader is killed, only 1 Crabka voter + the
/// JVM voter survive, so the surviving Crabka candidate can only reach majority
/// if the JVM grants its PRE-VOTE and real vote. This is the path the old
/// `PRE_VOTE_ECHO_TAG` shortcut broke (a JVM pre-vote grant was dropped). The JVM
/// is tuned to release the dead leader fast but self-nominate slowly so the
/// surviving Crabka node wins; recovery to a new Crabka leader at a higher epoch
/// is the proof.
///
/// Run:
/// ```text
/// cargo test -p crabka-broker --test jvm_static_quorum_spike \
///   contested_election_crabka_counts_jvm_prevote -- --ignored --nocapture
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + a published controller port"]
#[allow(clippy::too_many_lines)]
async fn contested_election_crabka_counts_jvm_prevote() {
    support::init_tracing();
    docker_rm(CONTESTED_CONTAINER);

    let cluster_id = Uuid::from_u128(0x4b69_7039_3936_4350_7245_566f_7445_7374);
    let cid_str = kafka_cluster_id_string(cluster_id);

    let (client_addrs, controller_addrs) = support::bind_and_drop_ports(3).await;
    let p1 = controller_addrs[0].port();
    let p2 = controller_addrs[1].port();
    let p3 = controller_addrs[2].port();
    let crabka_ctrl_1: SocketAddr = format!("0.0.0.0:{p1}").parse().unwrap();
    let crabka_ctrl_2: SocketAddr = format!("0.0.0.0:{p2}").parse().unwrap();
    let crabka_voters: Vec<(u64, SocketAddr)> = vec![
        (1, format!("127.0.0.1:{p1}").parse().unwrap()),
        (2, format!("127.0.0.1:{p2}").parse().unwrap()),
        (3, format!("127.0.0.1:{p3}").parse().unwrap()),
    ];

    // Short Crabka election timeout so the survivor re-elects promptly.
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let mut cfg1 = crabka_controller_config(
        0,
        client_addrs[0],
        crabka_ctrl_1,
        &crabka_voters,
        cluster_id,
        dir1.path(),
    );
    let mut cfg2 = crabka_controller_config(
        1,
        client_addrs[1],
        crabka_ctrl_2,
        &crabka_voters,
        cluster_id,
        dir2.path(),
    );
    cfg1.controller_election_timeout = Duration::from_millis(300);
    cfg2.controller_election_timeout = Duration::from_millis(300);

    let (c1, c2): (BrokerHandle, BrokerHandle) = {
        let s1 = tokio::spawn(Broker::start(cfg1));
        let s2 = tokio::spawn(Broker::start(cfg2));
        (
            s1.await.unwrap().expect("crabka voter 1 start"),
            s2.await.unwrap().expect("crabka voter 2 start"),
        )
    };

    // JVM voter id 3: release the dead leader fast, self-nominate slowly.
    let props = format!(
        "process.roles=controller\n\
         node.id=3\n\
         controller.quorum.voters=1@host.docker.internal:{p1},2@host.docker.internal:{p2},3@localhost:{p3}\n\
         controller.listener.names=CONTROLLER\n\
         listeners=CONTROLLER://0.0.0.0:{p3}\n\
         listener.security.protocol.map=CONTROLLER:PLAINTEXT\n\
         controller.quorum.fetch.timeout.ms=1000\n\
         controller.quorum.election.timeout.ms=8000\n\
         log.dirs=/tmp/kraft-controller-logs\n"
    );
    let propdir = TempDir::new().unwrap();
    let proppath = propdir.path().join("controller.properties");
    std::fs::write(&proppath, props).unwrap();
    let entry = format!(
        "/opt/kafka/bin/kafka-storage.sh format -t {cid_str} --config /tmp/c.properties --ignore-formatted && \
         exec /opt/kafka/bin/kafka-server-start.sh /tmp/c.properties"
    );
    let status = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            CONTESTED_CONTAINER,
            "--add-host=host.docker.internal:host-gateway",
            "-p",
            &format!("{p3}:{p3}"),
            "-v",
            &format!("{}:/tmp/c.properties", proppath.display()),
            "--entrypoint",
            "bash",
            KAFKA_IMAGE,
            "-c",
            &entry,
        ])
        .status()
        .expect("docker run JVM controller");
    assert!(status.success(), "docker run failed");

    // ── Phase 1: a Crabka node leads and the JVM joins as a follower. ───────
    let deadline = std::time::Instant::now() + Duration::from_secs(50);
    let mut leader0: Option<u64> = None;
    while std::time::Instant::now() < deadline {
        let l1 = c1.controller_leader_id().await;
        let l2 = c2.controller_leader_id().await;
        if l1.is_some() && l1 == l2 && matches!(l1, Some(1) | Some(2)) {
            leader0 = l1;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let leader0 = leader0.expect("Crabka 2/3 majority did not elect a leader in {1,2}");
    let epoch0 = c1.controller_quorum_state_for_test().current_term;
    eprintln!("phase 1: Crabka leader={leader0} epoch={epoch0}");

    // ── Phase 2: kill the Crabka leader; the survivor needs the JVM's grants. ─
    let (killed, survivor, survivor_id) = if leader0 == 1 {
        (c1, c2, 2u64)
    } else {
        (c2, c1, 1u64)
    };
    killed.shutdown().await;
    eprintln!("phase 2: killed Crabka leader {leader0}; survivor is {survivor_id}");

    // ── Phase 3: the surviving Crabka voter must win a new election. ─────────
    let recover_deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut recovered = false;
    while std::time::Instant::now() < recover_deadline {
        let qs = survivor.controller_quorum_state_for_test();
        if qs.current_leader == Some(survivor_id) && qs.current_term > epoch0 {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let final_qs = survivor.controller_quorum_state_for_test();
    eprintln!(
        "phase 3: survivor view leader={:?} epoch={} (was {epoch0})",
        final_qs.current_leader, final_qs.current_term
    );

    // Capture JVM logs for diagnosis regardless of outcome.
    let logs = Command::new("docker")
        .args(["logs", CONTESTED_CONTAINER])
        .output()
        .expect("docker logs");
    let log_text = format!(
        "{}{}",
        String::from_utf8_lossy(&logs.stdout),
        String::from_utf8_lossy(&logs.stderr)
    );
    let _ = std::fs::write("/tmp/jvm_contested.log", &log_text);
    let jvm_fatal_fault = log_text.contains("Encountered fatal fault");

    docker_rm(CONTESTED_CONTAINER);
    survivor.shutdown().await;

    assert!(
        recovered,
        "surviving Crabka voter {survivor_id} did not win a new election at a \
         higher epoch after the leader died — the JVM's pre-vote grant was not \
         counted (KIP-996 interop regression). survivor view: leader={:?} epoch={} (was {epoch0})",
        final_qs.current_leader, final_qs.current_term
    );
    assert!(
        !jvm_fatal_fault,
        "JVM controller fatal-faulted during the contested election; see /tmp/jvm_contested.log"
    );
}
```

- [ ] **Step 2: Type-check the test (compile only — do not require Docker)**

Run: `cargo test -p crabka-broker --test jvm_static_quorum_spike --no-run`
Expected: compiles. (`current_leader` / `current_term` are fields on `crabka_raft::QuorumState`; `controller_quorum_state_for_test()` returns it.)

- [ ] **Step 3: Run it if Docker is available (the empirical confirmation)**

Run: `cargo test -p crabka-broker --test jvm_static_quorum_spike contested_election_crabka_counts_jvm_prevote -- --ignored --nocapture`
Expected: PASS (recovery within ~60s). If Docker is unavailable in this environment, note that and rely on the compile check; the test runs in the JVM/Docker CI lane. (Optional sanity check: temporarily `git stash` Task 1, confirm this test times out/fails, then restore — proving it guards the regression.)

- [ ] **Step 4: Format + commit**

```bash
cargo fmt -p crabka-broker
git add crates/broker/tests/jvm_static_quorum_spike.rs
git commit -m "test(raft): contested mixed JVM+Crabka election guards KIP-996 pre-vote interop

After the Crabka leader is killed, the surviving Crabka voter can only reach
majority by counting the JVM voter's pre-vote + real-vote grants — the path the
removed echo-tag shortcut broke. Docker-gated #[ignore]; JVM tuned to release the
dead leader fast and self-nominate slowly so the Crabka survivor wins.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Docs — KIP matrix + changelog

**Files:**
- Modify: `README.md`
- Modify: `crates/raft/CHANGELOG.md`

- [ ] **Step 1: Flip the KIP-996 matrix row**

In `README.md`, replace:

```markdown
| [KIP-996](https://cwiki.apache.org/confluence/display/KAFKA/KIP-996) | Pre-vote | ❌ |
```

with:

```markdown
| [KIP-996](https://cwiki.apache.org/confluence/display/KAFKA/KIP-996) | Pre-vote | ✅ |
```

(This is the row under **Replication & availability**. Leave the unrelated KIP-966 `DescribeTopicPartitions` row alone.)

- [ ] **Step 2: Add a CHANGELOG entry**

In `crates/raft/CHANGELOG.md`, replace the `## [Unreleased]` line:

```markdown
## [Unreleased]
```

with:

```markdown
## [Unreleased]

### <!-- 1 -->🐛 Bug Fixes

- KIP-996 pre-vote now interoperates with a real KIP-996 JVM voter: vote
  responses are matched to their round by the candidate's own role + epoch
  (as Kafka does), replacing a private `VoteResponse` tagged-field echo that a
  JVM peer never sends. A JVM voter's pre-vote grant is now counted, so a
  Crabka-led election no longer stalls in a mixed JVM+Crabka quorum.
```

- [ ] **Step 3: Commit**

```bash
git add README.md crates/raft/CHANGELOG.md
git commit -m "docs: KIP-996 pre-vote ✅ (interop fix) + changelog

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Final verification

- [ ] **Workspace build + lint (the CI gates):**

```bash
cargo build --workspace --all-targets
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p crabka-raft
cargo test -p crabka-broker --test jvm_static_quorum_spike --no-run
```

Expected: all green. (`cargo fmt --check` and `clippy --workspace --all-targets -D warnings` are the CI gates — see the project memory on running fmt before push and clippy with `--all-targets`.)

- [ ] **Grep for stragglers:** `rg -n "PRE_VOTE_ECHO_TAG|pre_vote" crates/raft/src/kraft/transport.rs` should return nothing (the *response* path is fully clean); `rg -n "pre_vote" crates/raft/src/kraft/` should show `pre_vote` only on the **request** path (`SendVoteRequest`, `ReceiveVoteRequest`, `PeerRequest::Vote`, and `handle_vote_request`'s grant logic / `broadcast_vote`).
