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
- Explicit multi-statement transactions containing sharded writes currently
  fail closed with `sharded table writes inside explicit transactions are not
  supported`. Parameterized shard keys likewise fail closed because routing
  cannot infer their owner during Parse.

Raw results are under `target/g7-kind-clean-artifacts/multirange/`, notably
`sql-scatter-after-barrier.txt`, `pgbench-extended-literal.txt`,
`sql-extended-result.txt`, `sql-direct-r1-remote-r0-after-fix.txt`, and
`sql-explicit-txn.txt`.

## Recovery

- Deleting the r1 pod and waiting for its deployment restored rows `1, 12, 15`.
- Deleting the r0 pod and waiting for its deployment again restored rows
  `1, 12, 15`.
- A new cross-range write completed after both replacements, demonstrating that
  the recovered r0 TSO and r1 participant remained writable.

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
cargo check -p crabka-gres -p crabka-gres-ranges --all-targets
  passed
cargo fmt --all -- --check
  passed
```

The live fixes are `29aba497` (barrier primary reads) and `f5edf25b`
(follower-backed rN routing).
