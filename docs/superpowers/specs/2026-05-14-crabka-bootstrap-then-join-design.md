# Deterministic raft cluster bootstrap — Design Spec

## Goal

Eliminate the cold-boot split-vote that's been forcing `start_n_node_with_retry`
and `--skip` flags on multi-broker tests. Switch from simultaneous
`raft.initialize(full_voter_set)` on every broker to a **bootstrap-then-join**
pattern: one broker initializes as a singleton voter, the rest skip
initialize and wait to be added by the bootstrap broker via `add_learner` +
`change_membership` (the API shipped in PR #75). No concurrent elections,
no split votes, deterministic 3-broker boot.

## Background

openraft 0.9 doesn't have pre-vote (KIP-595's equivalent), so its election
randomization is the only defense against split-vote on cold boot. The
randomization picks a per-engine election timeout *once at startup*
(`config.new_rand_election_timeout::<RT>()` in `engine_config.rs:46`) and
re-uses that fixed value for every subsequent election round. When 3 brokers
spawn concurrently with the same election_timeout range, they:

1. All call `raft.initialize(full_voter_set)` locally — each engine now
   thinks it's in a 3-voter cluster.
2. Each engine's once-randomized election_timeout fires.
3. Each becomes Candidate and votes for itself in term T.
4. Each rejects the others' RequestVote (already voted in T).
5. Retry with term T+1 — but the engine still has the SAME timeout.
   The brokers keep firing at the same cadence, splitting votes again.

We've observed this lasting >2 minutes on ubuntu-latest. The current
workaround — `start_n_node_with_retry` — restarts the cluster on a fresh
TempDir + ephemeral ports so the random seed changes. Works for rust tests;
not feasible for the JVM-acceptance helpers which use Docker.

## Architecture

Replace simultaneous static initialization with a **two-phase** boot:

1. **Phase 1 — Bootstrap**: one broker (designated by the caller) starts as
   a single-voter cluster. Its raft engine initializes with
   `members = {(self.node_id, self.controller_addr)}`. There's no other
   voter to compete with, so this broker becomes leader immediately on its
   first election timeout (~few hundred ms with current 500ms `election_timeout_min`).

2. **Phase 2 — Join**: remaining brokers boot their raft engines without
   calling `initialize()`. Their engines sit in Learner state, listening
   for `AppendEntries` on the controller listener. They don't try to elect.
   `Broker::start` still blocks on `watch_leader` — they'll see a leader
   only after the bootstrap broker adds them.

3. **Phase 3 — Grow**: the test harness (or operator script in production)
   calls `bootstrap_broker.add_learner(node, addr)` for each joining broker,
   then `bootstrap_broker.change_membership({all_voters})` once. The
   bootstrap broker replicates the log to each new follower; after promotion
   to voter, the new brokers see a leader via the standard raft heartbeat
   path and `Broker::start` unblocks on them.

## API changes

Add a `BootstrapMode` enum to `crabka_raft::ControllerConfig`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapMode {
    /// This broker initializes a fresh raft cluster as the only voter.
    /// Use for the *first* broker on cluster cold boot. The caller (test
    /// harness or operator) is responsible for `add_learner` +
    /// `change_membership` to bring up the remaining voters.
    Bootstrap,

    /// This broker boots its raft engine but does NOT call `initialize`.
    /// Waits passively for the bootstrap broker to add it as a learner
    /// (and subsequently promote it to voter) before `watch_leader` fires.
    /// Use for the *non-first* brokers on cluster cold boot.
    Join,

    /// Restart: a previously-formatted broker rejoining an existing
    /// cluster. The raft log on disk already encodes membership; we don't
    /// call `initialize` and openraft's persisted vote/log carries us back
    /// into the existing cluster. This is the only mode that handles
    /// non-empty on-disk state correctly.
    Rejoin,
}

pub struct ControllerConfig {
    // ... existing fields ...
    pub bootstrap_mode: BootstrapMode,
}
```

No default. Callers explicitly pick the mode they need; this is a one-time
operational decision per broker per cluster, and silent defaults would
silently re-introduce the simultaneous-init failure.

Mirror onto `crabka_broker::BrokerConfig` with the same enum re-exported
from `crabka_broker`. Both `Default` and `for_tests` set
`BootstrapMode::Bootstrap` — that's the right answer for single-broker
setups (the singleton-voter path is a no-op for a 1-voter "cluster") and
for the first broker in a multi-broker cold boot. Callers building a
multi-broker cluster construct subsequent brokers with `Join` explicitly.

## Behavior in `Controller::start`

The existing "if log is empty, initialize" block gets replaced by a match
on `bootstrap_mode`:

```rust
if log_store.last_log_id().await.is_none() {
    match config.bootstrap_mode {
        BootstrapMode::Bootstrap => {
            // Initialize as a singleton voter; we become leader on the
            // first election timeout with no contention.
            let self_node = openraft::BasicNode {
                addr: config.controller_listen_addr.to_string(),
            };
            let members: BTreeMap<NodeId, Node> =
                [(config.node_id, self_node)].into_iter().collect();
            raft.initialize(members).await.map_err(|e| {
                RaftError::Openraft(format!("bootstrap initialize: {e:?}"))
            })?;
        }
        BootstrapMode::Join => {
            // Don't initialize. We'll receive AppendEntries from the
            // bootstrap broker once it calls add_learner.
        }
        BootstrapMode::Rejoin => {
            // Log is empty but caller claims this is a restart — abort.
            return Err(RaftError::Startup(
                "Rejoin mode requires non-empty raft log; \
                 use Bootstrap or Join for fresh state".into(),
            ));
        }
    }
} else if matches!(config.bootstrap_mode, BootstrapMode::Bootstrap) {
    return Err(RaftError::Startup(
        "Bootstrap mode requires empty raft log; \
         existing log indicates an already-initialized broker. Use Rejoin.".into(),
    ));
}
```

The bootstrap broker initializes immediately. Join brokers leave their
engine alone — it'll receive AE later. Rejoin brokers also skip initialize
because the existing log carries the membership.

## Test plumbing

`tests/support/mod.rs::start_n_node` is rewritten:

```rust
pub async fn start_n_node(n: u64) -> Result<Vec<(BrokerHandle, BrokerConfig, TempDir)>, BrokerError> {
    let (client_addrs, controller_addrs) = bind_and_drop_ports(n_usize).await;

    // Phase 1: bootstrap broker 1 as a singleton voter. Becomes leader trivially.
    let cfg1 = broker_config(0, &client_addrs, &controller_addrs, BootstrapMode::Bootstrap);
    let broker1 = Broker::start(cfg1.clone()).await?;

    // Phase 2: start brokers 2..n in Join mode (their Broker::start will block
    // waiting for leader; spawn so we can drive add_learner concurrently).
    let mut join_handles = Vec::with_capacity(n_usize - 1);
    let mut metas = Vec::with_capacity(n_usize);
    metas.push((TempDir::new()?, cfg1));
    for i in 1..n_usize {
        let cfg = broker_config(i, &client_addrs, &controller_addrs, BootstrapMode::Join);
        let cfg_clone = cfg.clone();
        join_handles.push(tokio::spawn(async move { Broker::start(cfg_clone).await }));
        metas.push((TempDir::new()?, cfg));
    }

    // Phase 3: bootstrap broker adds each join broker as learner, then
    // promotes them all to voters in one change_membership call.
    for i in 1..n_usize {
        broker1.add_learner(u64::try_from(i + 1)?, controller_addrs[i]).await?;
    }
    let voters: BTreeSet<NodeId> = (1..=u64::try_from(n_usize)?).collect();
    broker1.change_membership(voters).await?;

    // Join brokers' Broker::start now sees leader and returns.
    let mut out = vec![(broker1, metas[0].1.clone(), metas.remove(0).0)];
    for (jh, (dir, cfg)) in join_handles.into_iter().zip(metas) {
        out.push((jh.await??, cfg, dir));
    }
    Ok(out)
}
```

`start_n_node_with_retry` becomes a thin pass-through to `start_n_node`
(retry is no longer needed but the symbol stays so existing test files
don't churn).

`tests/jvm_acceptance.rs` migrates its inline 3-broker boot helpers to use
the same pattern. The 4 multi-broker JVM tests currently `--skip`-ped in CI
get un-skipped.

## Matches Kafka how

KRaft uses simultaneous static-init + pre-vote (KIP-595) to avoid the
disruption-on-rejoin variant of split-vote. We don't have pre-vote so we
take a different path: explicit bootstrap orchestration. The
*operational semantics* match — all membership changes are operator/test
driven via the equivalent of `kafka-metadata-quorum.sh add-controller`
(our `change_membership`). The difference is at format/cold-boot:

| Kafka KRaft | Crabka (this slice) |
|---|---|
| `kafka-storage format` writes membership to log | `BootstrapMode::Bootstrap` initializes log on first start |
| All brokers boot with full static voter set | Bootstrap broker boots alone; Join brokers wait |
| Pre-vote dampens spurious elections | No pre-vote; deterministic boot path avoids contention |
| Membership mutated by `kafka-metadata-quorum.sh` | Membership mutated by `BrokerHandle::{add_learner, change_membership}` |

When openraft adds pre-vote (or we adopt a fork that has it), we can collapse
`Bootstrap` + `Join` back into a single static-init mode without breaking
the operational API.

## Components

```
crates/raft/src/
├── config.rs                    # MODIFIED — BootstrapMode enum + ControllerConfig field
├── controller.rs                # MODIFIED — replace `if log empty: initialize` with mode-match
└── error.rs                     # MODIFIED — add RaftError::Startup variant

crates/broker/src/
├── config.rs                    # MODIFIED — BootstrapMode re-export + BrokerConfig field; Default panics
└── broker.rs                    # MODIFIED — pass bootstrap_mode through to ControllerConfig

crates/broker/tests/
├── support/mod.rs               # MODIFIED — start_n_node uses bootstrap-then-join
└── jvm_acceptance.rs            # MODIFIED — 4 multi-broker tests use bootstrap-then-join

.github/workflows/ci.yml         # MODIFIED — drop the 4 --skip flags
```

`Default for BrokerConfig` sets `BootstrapMode::Bootstrap` — correct for
the binary entry point (`crates/broker/src/bin/broker.rs`) and for any
single-broker test. Multi-broker test/integration callers explicitly set
`Join` on the non-first brokers.

## Test plan

1. `cargo test -p crabka-broker` — all green on Linux + macOS + Windows.
2. `cargo test -p crabka-broker --test jvm_acceptance --ignored` — all 9
   tests pass (the 4 previously-skipped multi-broker tests run).
3. New unit test: `BrokerConfig::default()` sets `bootstrap_mode == Bootstrap`.
4. New unit test: `Controller::start` returns `Startup` error if `Bootstrap`
   mode given a non-empty raft log.
5. Local boot timing: a 3-broker cluster reaches "first leader elected"
   within ~600ms (vs. the 5-30s observed today on the simultaneous path).

## Out of scope

- Pre-vote in openraft. Tracked separately if we ever fork.
- Auto-bootstrap orchestration (i.e., broker 1 auto-adds the other voters
  on startup). Operator/test continues to call `change_membership`
  explicitly. Matches KRaft's static-then-mutate model.
- Reducing the Bootstrap broker's election timeout. Singleton-voter
  election is trivially fast (~election_timeout_min); no tuning needed.
- Cluster-wide rolling restarts. `Rejoin` mode handles single-broker
  restart; the slice doesn't address simultaneous-rolling-restart
  consistency. (Same as Kafka — operators stagger restarts.)
