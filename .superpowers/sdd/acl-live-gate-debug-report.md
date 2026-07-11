# ACL live gate debug report

## Root cause

The broker authorization decision was correct. A retained successful reproduction showed the
Kafka 4.0 console consumer receiving `TOPIC_AUTHORIZATION_FAILED` for `__gres_tenants`, while
the reported failing run had an empty redirected log and a nonzero command status. The old gate
therefore depended on a Docker-launched cold JVM completing startup and printing its decoded
Metadata error inside the outer 20-second bound. A container/JVM failure or timeout occurs before
Kafka protocol output exists, so the empty log could not prove authorization.

The repaired gate removes that boundary. A hidden `crabka gres probe-topic-read` command uses
Crabka's native SCRAM client and sends a named Kafka Metadata request. The assertion passes only
for process status 1 plus the exact named-topic result
`topic __gres_tenants metadata: UNKNOWN (29)`. Authentication failures, omitted/missing topics,
transport errors, timeouts, success, another topic's error, and generic authorization text fail.
The password is read from a mode-0600 file and is never logged.

## RED / GREEN evidence

- RED: `bash scripts/tests/gres-e2e-topic-probe.sh` failed because the script still contained
  `kafka-console-consumer.sh` and did not invoke the native probe.
- RED refinement: the contract rejected an implementation that accepted code 29 for a different
  topic; before tightening, it failed with `classifier accepted authorization denial for the wrong topic`.
- GREEN: `bash scripts/tests/gres-e2e-topic-probe.sh` passes the exact-topic/code-29 contract and
  negative cases for empty output, timeout, wrong topic, generic denial text, and success.
- GREEN: `cargo test -p crabka-cli` passed 46 tests (44 unit, 2 integration).
- GREEN: `bash -n scripts/gres-e2e.sh` and `cargo fmt --all -- --check` passed (stable rustfmt emits
  existing warnings for nightly-only configuration keys).

## Live evidence

`CRABKA_GRES_SKIP_BUILD=1 CRABKA_GRES_E2E_KEEP_ARTIFACTS=1 ./scripts/gres-e2e.sh`
passed all three ACL assertions. The retained global-registry artifact contains exactly:

`crabka gres: topic __gres_tenants metadata: UNKNOWN (29)`

The E2E continued beyond the repaired ACL gate and then failed independently because Python
`psycopg` is not installed for the later PgDog driver smoke tests. Artifacts remain under
`target/gres-e2e-artifacts`.
