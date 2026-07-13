# GRES G8 Split crash-anywhere matrix evidence

Date: 2026-07-13

Commit: `5d969b8bbdd9ad2280f1b7a1f221770a2b98981b`

## Contract

The definitive matrix regenerated all evidence from an empty family directory at the same commit. It ran the real `crabka gres split` CLI through the production operator, authenticated range-control transport, real GRES child processes, continuous fsynced SQL workload, child replacement, recovery, publication, predecessor retirement, and post-restart verification.

The schema-v2 validator independently reconstructs the expected result from raw durable state. For every case it checks the reopened payload-event ledger; ACK, recovered-ACK, pre-kill, post-restart, and post-publication streams; six complete physical scans over tables 50 and 51 on r0/r2/r3; SQL/oracle equality; exact successor marker partitions and digest; ordered authenticated control requests and responses; journal-derived receipt expectations; delete attempts; terminal operation evidence; layout and retirement versions; process-group cleanup; and cross-case identity uniqueness.

## Definitive matrix

Values are `observed / enforced bound` in milliseconds. Every case completed with target layout `[0, 2, 3]`, exactly one recovered ambiguous acknowledgement or more, no predecessor topic, both successor topics, the unrelated sentinel topic intact, and the workload/source process groups reaped.

| Family | Kill point | Maximum ACK gap | Operation time | Acknowledged rows | Recovered ACKs |
| --- | --- | ---: | ---: | ---: | ---: |
| `publication` | `layout_published_after_journal_cas` | 11,097 / 15,000 | 70,851 / 240,000 | 233 | 1 |
| `publication` | `tenant_cas_before_journal_cas` | 11,817 / 15,000 | 73,460 / 240,000 | 216 | 1 |
| `retirement_resume` | `completed_after_journal_cas` | 11,817 / 15,000 | 74,318 / 240,000 | 207 | 1 |
| `retirement_resume` | `delete_success_before_sidecar_cas` | 11,333 / 15,000 | 78,369 / 240,000 | 238 | 1 |
| `retirement_resume` | `parked_after_sidecar_cas` | 11,729 / 15,000 | 72,300 / 240,000 | 222 | 1 |
| `retirement_resume` | `resuming_after_journal_cas` | 11,735 / 15,000 | 73,460 / 240,000 | 214 | 1 |
| `retirement_resume` | `retire_receipt_before_journal_cas` | 11,448 / 15,000 | 79,795 / 240,000 | 232 | 1 |
| `retirement_resume` | `retiring_before_delete` | 11,444 / 15,000 | 70,595 / 240,000 | 229 | 1 |
| `source_restore` | `activated_after_journal_cas` | 11,534 / 25,000 | 74,668 / 240,000 | 212 | 1 |
| `source_restore` | `checkpoint_receipt_before_journal_cas` | 12,246 / 25,000 | 75,406 / 240,000 | 202 | 1 |
| `source_restore` | `checkpointed_after_journal_cas` | 11,316 / 25,000 | 69,717 / 240,000 | 201 | 1 |
| `source_restore` | `initiated_before_running_cas` | 12,949 / 25,000 | 73,349 / 240,000 | 195 | 1 |
| `source_restore` | `marker_claim_receipt_before_journal_cas` | 20,591 / 25,000 | 79,356 / 240,000 | 187 | 2 |
| `source_restore` | `pause_receipt_before_journal_cas` | 18,705 / 25,000 | 76,922 / 240,000 | 198 | 1 |
| `source_restore` | `paused_before_stage` | 16,046 / 25,000 | 72,538 / 240,000 | 196 | 2 |
| `source_restore` | `prologue_receipt_before_journal_cas` | 11,362 / 25,000 | 81,145 / 240,000 | 210 | 4 |
| `source_restore` | `restored_after_journal_cas` | 18,905 / 25,000 | 77,806 / 240,000 | 184 | 1 |
| `source_restore` | `stage_receipt_before_journal_cas` | 19,935 / 25,000 | 77,498 / 240,000 | 193 | 1 |
| `source_restore` | `staged_after_journal_cas` | 16,832 / 25,000 | 72,811 / 240,000 | 196 | 1 |

The largest ACK gap was 20,591 ms, leaving 4,409 ms below the source-family ceiling. The largest publication/retirement gap was 11,817 ms, leaving 3,183 ms below its ceiling. The largest operation time was 81,145 ms, leaving 158,855 ms below the per-case timeout. All 19 tenant IDs, operation IDs, and evidence IDs were pairwise unique.

## Reproducible gates

The complete clean matrix ran from 00:07 through 00:35 UTC and exited zero:

```text
scripts/tests/gres-topology-process-split-matrix-ci.sh
```

It produced 19 successful exact live-test results and 19 JSON artifacts. Fresh independent validation after the wrapper also exited zero:

```text
python3 scripts/tests/validate-gres-split-crash-evidence.py --validate-family source_restore "$PWD/target/g8-split-crash/source_restore"
python3 scripts/tests/validate-gres-split-crash-evidence.py --validate-family publication "$PWD/target/g8-split-crash/publication"
python3 scripts/tests/validate-gres-split-crash-evidence.py --validate-family retirement_resume "$PWD/target/g8-split-crash/retirement_resume"
python3 scripts/tests/validate-gres-split-crash-evidence.py --validate-matrix "$PWD/target/g8-split-crash"
```

The regression gates at the same commit were:

```text
cargo test --locked -p crabka-gres-ranges --test split_model -- --nocapture
# 5 passed, including broken-model counterexample teeth

scripts/tests/gres-topology-process-nemesis-ci.sh
# foundation plus four source-phase SIGKILL variants passed; JSON audit passed

cargo test --locked -p crabka-gres-ranges transport::tests --lib
# 13 passed

cargo test --locked -p crabka-operator controller::gres_split_operation --lib
# 10 passed

cargo check --locked -p crabka-operator
# exited zero

cargo test --locked -p crabka-gres --test topology_process_split_crash
# 23 passed

cargo fmt --all --check
git diff --check
# both exited zero
```
