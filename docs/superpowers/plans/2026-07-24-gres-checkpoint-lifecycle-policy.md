# Gres Checkpoint and Lifecycle Policy

**Goal:** Make periodic checkpointing actually run, then expose its operational policy and Gres tenant lifecycle pacing through validated CLI/environment and fleet CRD settings without masking tenant checkpoint thresholds.

**Architecture:** Threshold ownership stays unchanged: explicit compute CLI/environment values override the registry record; `Gres.defaults` then `GresTenant.overrides` produce that record; compiled defaults apply only after record hydration. Fleet/process checkpoint sizing and cadence live under `Gres.spec.compute`. The substrate service serializes periodic and manual checkpoints in one loop. Retained manifests remain replayable, and a range-control operation pins its exact forced checkpoint and WAL boundary until resume or retirement so periodic pruning cannot invalidate an in-flight transfer. Operator lifecycle cadence is fleet-owned but never rendered into the compute container.

**Constraints:** Never render checkpoint frame/byte flags from the operator. Keep partition 0, offset math, manifest-last durability, generation fencing, retention safety, object/topic layout, and Kafka protocol codes fixed. Use `refined_type` for new validated newtypes. Add no artificial relation such as part size versus threshold. Run Cargo with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.

## Task 1: Make checkpoint polling real

**Files:**

- Modify: `crates/gres-substrate/src/checkpoint/service.rs`
- Modify: `crates/gres-substrate/src/checkpoint/runtime.rs`
- Modify only required constructor/test call sites in `crates/gres-substrate`
- Modify: `crates/gres/src/lib.rs`
- Modify: `crates/gres/src/live_range_control.rs`
- Modify only required range-transfer trait/test call sites

- [x] Add paused-time RED tests proving no immediate tick, below-threshold no-op, above-threshold checkpoint, and retry after one tick failure.
- [x] Require an explicit positive poll interval in `CheckpointConfig`; remove its hardcoded one-second assignment.
- [x] Add a production spawn path that receives the existing `CheckpointSnapshotSource` and selects between a delayed interval tick and commands.
- [x] On a tick, snapshot/checkpoint only after a threshold crosses; log errors and retry on later ticks.
- [x] Serialize periodic and manual attempts in the same service loop; preserve command-only spawn behavior for crash-test seams that provide snapshots explicitly.
- [x] Calculate the ordinary WAL prune horizon from the oldest retained manifest, so every retained checkpoint remains replayable.
- [x] Add an operation-scoped pin that atomically protects a forced manifest, its objects, and its WAL replay boundary from later periodic checkpoints; release it on resume or predecessor retirement and preserve startup reconciliation safety.
- [x] Add RED tests for retained-checkpoint replayability, more-than-retention periodic checkpoints during an active transfer, and pin release.
- [x] Route both single-range and multi-range Gres runtimes through the periodic spawn path.
- [x] Run full Gres-substrate and Gres tests plus strict gates; commit only Task 1 files.

## Task 2: Expose standalone checkpoint and suspend inputs

**Files:**

- Modify: `crates/gres-control/src/` for shared refined validated scalar types
- Modify: `crates/gres/src/lib.rs`
- Modify required Gres test constructors

- [x] Keep `--checkpoint-frames` and `--checkpoint-bytes` optional with no Clap defaults; add `CRABKA_GRES_CHECKPOINT_FRAMES` / `CRABKA_GRES_CHECKPOINT_BYTES`.
- [x] Preserve precedence: explicit CLI/environment, tenant record, compiled 10,000 frames / 67,108,864 bytes.
- [x] Add environment bindings to `--checkpoint-part-bytes` and `--checkpoint-retain`.
- [x] Add exact positive flags/environment/defaults:
  - `--checkpoint-delete-records-timeout-ms` / `CRABKA_GRES_CHECKPOINT_DELETE_RECORDS_TIMEOUT_MS` / 30000
  - `--checkpoint-poll-interval-ms` / `CRABKA_GRES_CHECKPOINT_POLL_INTERVAL_MS` / 1000
  - `--idle-suspend-poll-interval-ms` / `CRABKA_GRES_IDLE_SUSPEND_POLL_INTERVAL_MS` / 1000
- [x] Validate part bytes as at least 8, retention as positive, delete timeout within positive `i32`, and durations as positive using shared `refined_type`-backed types.
- [x] Thread the values to checkpoint construction, WAL pruning, and the idle-suspend loop; remove the five Gres hardcoded constants.
- [x] Do not let always-present runtime defaults independently enable checkpointing or mask a tenant record.
- [x] Add parser default/environment/CLI precedence, boundary rejection, consumer, and threshold-precedence tests.
- [x] Run full Gres-control, Gres-substrate, and Gres tests plus strict gates; commit only Task 2 files.

## Task 3: Expose fleet compute checkpoint/lifecycle policy

**Files:**

- Modify: `crates/operator/src/crd/gres.rs`
- Modify: `crates/operator/src/controller/gres_tenant.rs`
- Modify required constructor-only fallout
- Regenerate: `deploy/crds/crabka.io_greses.yaml`

- [x] Extend `Gres.spec.compute` with optional validated fields/defaults:
  - `checkpointPartBytes`: 67108864, minimum 8
  - `checkpointRetain`: 2, minimum 1
  - `checkpointDeleteRecordsTimeoutMs`: 30000, range 1 through `i32::MAX`
  - `checkpointPollIntervalMs`: 1000, minimum 1
  - `idleSuspendPollIntervalMs`: 1000, minimum 1
  - `lifecycleRequeueMs`: 5000, minimum 1
- [x] Materialize threshold precedence as tenant override, fleet tenant default, compiled threshold fallback in the `TenantRecord`.
- [x] Validate the effective fleet policy before Kafka/resource/Deployment writes.
- [x] Render exactly the five compute runtime flags and never frame/byte threshold flags.
- [x] Replace every tenant lifecycle five-second requeue site with the validated fleet value.
- [x] Add CRD schema/roundtrip/boundary tests, threshold precedence matrix, deployment flag tests, invalid-before-I/O tests, and all lifecycle branch tests.
- [x] Run full operator and Gres tests, strict gates, and exact all-nine CRD generation; commit only Task 3 files.

## Task 4: Independent review and audit closure

- [x] Review each task independently; remediate findings with fresh implementers and re-review.
- [x] Classify all checkpoint/lifecycle scanner candidates and prove every new knob has one owner and a live consumer.
- [x] Verify full focused package suites, strict gates, exact CRDs, and scanner closure.
- [x] Record the next remaining Gres runtime owner.
