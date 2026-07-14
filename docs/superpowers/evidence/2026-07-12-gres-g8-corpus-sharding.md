# G-8 corpus-through-sharding evidence

Date: 2026-07-12

## Scope

- The existing primary PostgreSQL oracle corpus and ordinary-table `baseline.json`
  are unchanged; `sharded-baseline.json` independently ratchets this surface.
- Only subject `CREATE TABLE` setup statements are parser-validated and rewritten with `SHARDED`.
- A live standalone substrate uses one tenant with two row-boundary ranges
  (`0,0:250`), placing early and late corpus writes on both physical owners.
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

The PostgreSQL 18 live corpus report recorded 688 cases, with 662 matches
and the remaining 26 outcomes accepted by the dedicated sharded baseline. The
runtime ownership artifact
records committed timestamp-primary operations by physical user table ID and
primary range. The observed corpus included user-table commits on both range 0
and range 1 (24 distinct physical user table IDs in total). No individual table
was observed with both primaries, and the report does not claim that every table
spans both ranges. Catalog-only (`table_id = 0`) evidence is excluded and cannot
satisfy either expected range.

CI runs the same script in live mode with PostgreSQL 18 and uploads all gate
artifacts. The static CI-contract test includes negative mutations for the live
invocation, mode, `continue-on-error`, SHARDED flag, baseline path, evidence
parser invocation, and the catalog-table exclusion. Its behavioral fixture
proves that a catalog-only range-0 event fails the gate.
