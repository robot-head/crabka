# G-3 crash/model closure evidence

Date: 2026-07-11 UTC

Scope: Chapter Gres G-3 only. This evidence does not claim G-4 or later gates.

## Production crash boundaries

`checkpoint-test-hooks` is a non-default compile-time feature. The failpoint
field, callback type, boundary enum, and test-only constructor method are absent
from default builds. With the feature enabled, the real `CheckpointService`
reports `BeforeParts`, every `PartsUploaded`, `ManifestWritten`, `Truncated`, and
`Pruned` boundary. Returning true aborts the real orchestration at that boundary.

`checkpoint_crashes` drives the production-wired `checkpoint_from_source` path.
Its live matrix starts a fresh broker for each boundary, commits transactional
WAL, uploads through the captured service, executes real Admin `DeleteRecords`
when the boundary permits it, and performs fresh live recovery. Each case has a
15-second outer deadline. It also removes the newest manifest after real service
truncation and proves `TornTruncation`. The zombie race blocks an old generation
after a part upload, fences it, completes a successor checkpoint, then releases
the old upload: the post-upload lease check returns `Fenced` before old
`DeleteRecords`, and successor recovery wins.

Command:

```text
cargo test -p crabka-gres-substrate --features checkpoint-test-hooks
```

Result: 106 unit, 12 checkpoint-crash, 2 checkpoint-model, 3 live checkpoint
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
The replicated engine installs a horizon provider that combines active
ProcArray xmin with the recovery scan watermark and first nonterminal clog
entry. A production-wiring test observes a nonzero horizon; a prepared marker
test proves it pins the horizon. The source also holds the writer lease used for
the post-upload/pre-truncate zombie check.

The whole-history xid property builds randomized multi-key/multi-version stores
with committed, aborted, and in-progress xmins/xmaxs, frozen xmin, and clog
entries; rewrites the entire store; and compares visible rows for every key over
every bounded snapshot xmin/xmax at or above the horizon with randomized `xip`.
The companion timestamp property ranges intent, aborted, committed, and deleted
states. Unit cases pin clog pruning and resolved intent behavior.

The recovery reader records the offset passed to `committed_from`. Same
generation recovery observed `covered_offset + 1`; older-checkpoint/fresh-WAL
generation recovery observed offset 0 and journal sequence reset. The live
fresh-generation test restores a generation-0 checkpoint covering offset 7,
then recovers generation 1 from a newly created broker topic whose required
record is physically at offset 0. Same-generation live recovery runs after
`DeleteRecords`, so an accidental fetch from zero there cannot pass silently.

## Bounded model

The Stateright state has three computes, per-compute epoch/applied prefix and
serving/recovering/crashed/refused plus checkpoint phase, per-generation WAL,
manifest state, and log start. Explicit actions cover append, checkpoint steps,
zombie checkpoint steps, prune, crash, successor fencing, park/recreate with
offset reset, manifest loss, and recovery steps. Properties cover serving-fold
equivalence, deterministic state-local safe recovery classification absent
injected loss, manifest/log-start ordering, no serving under unresolved torn
loss, and fresh-generation replay origin zero. Park/recreate atomically fences
serving/recovering computes, bumps generation/epoch, resets log start, and
creates the successor in recovering phase.

Correct model result: 3,343 unique states, 8,117 generated states, maximum depth
18. A model that removes manifest-before-truncate ordering produces a
`no_torn_truncation_without_manifest_loss` counterexample.

## Static and compatibility gates

All commands completed successfully:

```text
cargo check -p crabka-gres-substrate -p crabka-pgexec -p crabka-pgkv -p crabka-client-admin -p crabka-gres --all-targets --all-features
cargo clippy -p crabka-gres-substrate -p crabka-pgexec -p crabka-pgkv -p crabka-client-admin -p crabka-gres --all-targets --all-features -- -D warnings
cargo +nightly fmt --all -- --check
cargo test -p crabka-pgkv                         # 60 passed
cargo test -p crabka-client-admin                 # passed
cargo test -p crabka-gres --lib                   # 42 passed
python3 scripts/tests/gres_f0_runtime_gates.py     # PASS
python3 -m unittest tools/tests/test_gres_wire_recorder.py tools/tests/test_capture_gres_driver_goldens.py  # 13 passed
./tools/check-pg-compat-matrix.sh --self-test      # PASS
./tools/check-pg-compat-matrix.sh                  # PASS
git diff --check HEAD~3..HEAD                      # clean
```
