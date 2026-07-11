Status: DONE

Commits: 3571d691..940e78d4

Implemented an in-process broker plus real r0/r1 crabka-gres child harness with stable cache directories, generated mTLS identity, condition-driven readiness, kill, respawn, and transparent network partition controls. Ported real-process coverage into all seven named Task 7 binaries. Added concurrent bank histories, participant/range0/cascade kills, drain recovery, partition/heal, and a Stateright LinearizabilityTester over real list-append transactions. Fixed strict-serializability write skew with the intended range-0 explicit-transaction gate and fixed remote participant release incorrectly attempting a GTM decision on rN. Added a two-thread nextest group and dated evidence.

Verification:

- `cargo test -p crabka-gres-ranges --lib`: 105 passed.
- `CRABKA_GRES_TEST_BINARY=target/debug/crabka-gres cargo nextest run -p crabka-gres-ranges --test multiprocess --test jepsen_bank --test participant_kill_bank --test range0_cascade_kill_bank --test range0_leader_kill_drain --test crossrange_2pc_nemesis --test jepsen_elle`: 17 passed, 0 skipped.
- `git diff --check`: passed.

Known scope: G8/G9 sharded timestamp explicit transactions and later distributed maturity gates are outside G7 Task 7 and remain active work.

## Independent-review remediation: range-0 explicit transaction lease

Implemented a range-0-owned lease protocol for ordinary distributed explicit
transactions. `BEGIN` through any registry-backed compute gateway resolves r0,
waits for the current owner to release or expire, and receives an r0-allocated
owner token. Ordinary statements and `COMMIT` renew and validate that token;
expired owners fail with SQLSTATE `40001`. `COMMIT`, `ROLLBACK`, error cleanup,
and session drop release ownership, with a two-second server deadline as the
bounded disconnect/idle fallback. Stale renew and release requests cannot alter
the current owner. The old process-local mutex remains only as the in-process or
mock-forwarder fallback and is no longer the distributed correctness mechanism.

TDD/evidence:

- `cargo test -p crabka-gres-ranges range_zero_explicit_gate_serializes_expires_and_fences_stale_owners --no-fail-fast`: passed; covers acquire, conflict waiting, expiry, unique replacement token, stale release, renew, and release.
- `cargo test -p crabka-gres-ranges --lib --no-fail-fast`: 106 passed, 0 failed.
- `cargo build -p crabka-gres --bin crabka-gres`: passed.
- `CRABKA_GRES_TEST_BINARY=target/debug/crabka-gres cargo test -p crabka-gres-ranges --test multiprocess range_zero_lease_serializes_explicit_transactions_across_compute_gateways_and_expires --no-fail-fast`: passed using separate r0 and direct-r1 SQL gateways; the second `BEGIN` waits for r0 release and an abandoned owner is replaced after the bounded lease.
- `git diff --check`: passed.

The shared session lifecycle for completed G9 timestamp transactions is
unchanged; timestamp transactions acquire the same outer explicit-session lease
but retain their existing timestamp decision and participant protocol.
