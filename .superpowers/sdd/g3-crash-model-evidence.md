# G-3 crash/model closure evidence

Date: 2026-07-11 UTC

Scope: Chapter Gres G-3 only. This evidence does not claim G-4 or later gates.

## Production crash boundaries

`checkpoint-test-hooks` is a non-default compile-time feature. The failpoint
field, callback type, boundary enum, and test-only constructor method are absent
from default builds. With the feature enabled, the real `CheckpointService`
reports `BeforeParts`, every `PartsUploaded`, `ManifestWritten`, `Truncated`, and
`Pruned` boundary. Returning true aborts the real orchestration at that boundary.

`checkpoint_crashes` drives acknowledged WAL groups into the serving KV, creates
a baseline through `CheckpointService`, aborts a later service attempt at every
boundary, and restores a fresh KV from the surviving object closure plus the
retained WAL. It also removes the newest manifest after real service truncation
and proves `TornTruncation`, and lets an older service complete after a successor
manifest without displacing the successor.

Command:

```text
cargo test -p crabka-gres-substrate --features checkpoint-test-hooks
```

Result: 105 unit, 10 checkpoint-crash, 2 checkpoint-model, 3 live checkpoint
runtime, 1 coordinator-leader, 4 G-2 acceptance, 4 split runtime, and 6 live
writer-fault tests passed. The live runtime suite boots the in-process broker,
writes checkpoint objects, executes `DeleteRecords`, verifies the retained low
watermark, and restores checkpoint plus tail.

## Snapshot, horizon, and fetch origins

`CheckpointSnapshotSource::capture` shares the exact semaphore used by
`SubstrateCommitter`. It captures WAL metadata and an online `KvSnapshot` while
no group is in flight, then releases the gate before object upload. The racing
test proves the acknowledged pre-request group is present and the queued later
group is absent from the captured snapshot while subsequently reaching live KV.
All Gres force/final/successor checkpoint entry points use this capture path.

The xid visibility property now ranges committed, aborted, and in-progress
xmins/xmaxs, frozen xmin, snapshot `xip` membership, and reads at or above the
garbage horizon. The companion timestamp property ranges intent, aborted,
committed, and deleted states. Unit cases pin clog pruning and resolved intent
behavior.

The recovery reader records the offset passed to `committed_from`. Same
generation recovery observed `covered_offset + 1`; older-checkpoint/fresh-WAL
generation recovery observed offset 0 and journal sequence reset. The live
broker runtime additionally succeeds after `DeleteRecords`, so an accidental
fetch from zero in the same-generation case cannot pass by silently.

## Bounded model

The Stateright state has three computes, per-compute epoch/applied prefix and
serving/recovering/crashed/refused plus checkpoint phase, per-generation WAL,
manifest state, and log start. Explicit actions cover append, checkpoint steps,
zombie checkpoint steps, prune, crash, successor fencing, park/recreate with
offset reset, manifest loss, and recovery steps. Properties cover serving-fold
equivalence, existence of a safe recovery path absent injected loss,
manifest/log-start ordering, and refusal instead of corrupt serving after loss.

Correct model result: 2,497 unique states, 6,736 generated states, maximum depth
18. A model that removes manifest-before-truncate ordering produces a
`no_torn_truncation_without_manifest_loss` counterexample.

## Static and compatibility gates

All commands completed successfully:

```text
cargo check -p crabka-gres-substrate -p crabka-pgkv -p crabka-client-admin -p crabka-gres --all-targets --all-features
cargo clippy -p crabka-gres-substrate -p crabka-pgkv -p crabka-client-admin -p crabka-gres --all-targets --all-features -- -D warnings
cargo +nightly fmt --all -- --check
cargo test -p crabka-pgkv                         # 60 passed
cargo test -p crabka-client-admin                 # passed
cargo test -p crabka-gres --lib                   # 41 passed
python3 scripts/tests/gres_f0_runtime_gates.py     # PASS
python3 -m unittest tools/tests/test_gres_wire_recorder.py tools/tests/test_capture_gres_driver_goldens.py  # 13 passed
./tools/check-pg-compat-matrix.sh --self-test      # PASS
./tools/check-pg-compat-matrix.sh                  # PASS
git diff --check HEAD~3..HEAD                      # clean
```
