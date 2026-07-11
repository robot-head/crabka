# G7 multi-range live evidence — 2026-07-11

Cluster: `kind-crabka-g7-clean`. Tenant: `tenant-g7c`. The operator created
separate `tenant-g7c-gres` (r0) and `tenant-g7c-gres-r1` deployments and an
operator-owned `kubernetes.io/tls` range identity Secret.

## Live SQL and protocol

- A range-spanning timestamp statement, `INSERT INTO t50 VALUES (1), (11)`,
  completed as `INSERT 0 2` after timestamp participants were changed to wait
  for the range-0 follower barrier before primary-identity validation.
- A remote-only write, `INSERT INTO t50 VALUES (12)`, was visible through the r0
  gateway.
- PostgreSQL extended protocol was exercised with `pgbench -M extended`; one
  transaction completed with zero failures and remote row `15` was visible.
- Connecting directly to the r1-only SQL gateway and selecting rows owned by
  both r0 and r1 returned `1, 12, 15, 17, 18`, proving follower-backed routing
  and remote-r0 execution from a process that does not host r0.
- A provisional buffered explicit-transaction implementation produced keys `9`
  and `3002` in Kind, but independent review rejected it because it lacked
  read-your-writes and merged SQL text unsafely. That implementation was
  removed; durable explicit sharded transactions remain an open G7 gate.
- PostgreSQL extended Parse/Bind routing was exercised with a real bound shard
  parameter through `pgbench -M extended`; the transaction completed with zero
  failures and row `3003` was immediately visible.

Raw results are under `target/g7-kind-clean-artifacts/multirange/`, notably
`sql-scatter-after-barrier.txt`, `pgbench-extended-literal.txt`,
`sql-extended-result.txt`, `sql-direct-r1-remote-r0-after-fix.txt`, and
`pgbench-parameterized-shard-final.txt`, and
`sql-parameterized-shard-final.txt`.

## Recovery

- Deleting the r1 pod and waiting for its deployment restored rows `1, 12, 15`.
- Deleting the r0 pod and waiting for its deployment again restored rows
  `1, 12, 15`.
- A new cross-range write completed after both replacements, demonstrating that
  the recovered r0 TSO and r1 participant remained writable.
- A sparse cross-range write `(6), (1000)` was visible immediately and remained
  visible after independent r1 and r0 replacements, proving atomic publication
  of participant scan terminals with timestamp resolution.

Pod snapshots and query output are in `pods-before-kill.txt`,
`pods-after-r1-kill.txt`, `pods-after-r0-kill.txt`, `sql-after-r1-kill.txt`, and
`sql-after-r0-kill.txt`.

## Transport boundary

- Plaintext produced a server-side TLS `InvalidContentType` rejection.
- TLS without a client certificate produced `peer sent no certificates`.
- A certificate signed by the tenant CA but carrying `CN=outside-tenant-range`
  completed cryptographic negotiation and was then rejected by application
  authorization as `range transport peer is not authorized for tenant
  tenant-g7c`.
- The operator-issued certificate negotiated TLS 1.3 and verified successfully.
- Unit tests prove plaintext and authenticated non-allowlisted connections never
  invoke the range service (`service.calls == 0`).

See `tls-server-rejections.log`, `tls-plaintext.txt`,
`tls-no-client-cert.txt`, `tls-wrong-principal.txt`, and
`tls-allowed-principal.txt`.

## Verification

```text
cargo test -p crabka-pgexec primary_prewrite_waits_for_range0_replica_barrier --lib
  1 passed
cargo test -p crabka-gres-ranges --lib
  105 passed
cargo test -p crabka-gres-ranges --test multirange --test crossrange_2pc
  23 + 21 passed before the rejected explicit-transaction prototype was removed
cargo check -p crabka-gres -p crabka-gres-ranges --all-targets
  passed
cargo fmt --all -- --check
  passed
```

The live fixes include `29aba497` (barrier primary reads), `f5edf25b`
(follower-backed rN routing), `79084e7c` (atomic sparse scan terminals),
and `efcfa68e` (typed deferred shard routing). Commits `4ec1e9f9`/`9ca47d2f`
were reviewed as insufficient and their behavior has been removed pending a
real timestamp-transaction session with read-your-writes semantics.
