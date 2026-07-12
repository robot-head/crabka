# G-8 corpus-through-sharding evidence

Date: 2026-07-12

## Scope

- The existing primary PostgreSQL oracle corpus and `baseline.json` are unchanged.
- Only subject `CREATE TABLE` setup statements are parser-validated and rewritten with `SHARDED`.
- A live standalone substrate uses one tenant with two ranges (`0,0:2`).
- Extended-case setup transformation is unit-tested but is not part of the live primary-corpus gate.

## Verification

The following commands passed:

```text
cargo test -p crabka-gres-conformance --lib sharded_
cargo test -p crabka-gres-conformance
python3 scripts/tests/gres_sharded_conformance_ci.py
bash -n scripts/gres-sharded-conformance.sh
CRABKA_GRES_SHARDED_CONFORMANCE_MODE=live \
  CRABKA_GRES_SHARDED_ORACLE_URL='host=127.0.0.1 port=5432 user=gres_ci dbname=postgres password=gres_ci' \
  CRABKA_GRES_SHARDED_CONFORMANCE_ARTIFACT_DIR=target/g8-live-artifacts \
  scripts/gres-sharded-conformance.sh
```

The live corpus report recorded 688 cases, with 616 matches and the remaining
72 outcomes accepted by the existing baseline. The runtime ownership artifact
recorded 15 committed timestamp-primary operations for range 0 and 7 for range
1. Both counts come from the live Gres process log and are mandatory for the
gate to pass.

CI runs the same script in live mode with PostgreSQL 18 and uploads all gate
artifacts. The static CI-contract test includes negative mutations for the live
invocation, mode, `continue-on-error`, SHARDED flag, baseline path, and each
runtime primary-owner assertion.
