# Runtime configuration Task 5 report

Status: DONE

Implemented `Kafka.spec.brokerTuning` as 117 typed optional camel-case fields,
matching the complete broker `RuntimeFileConfig` field set. This includes the
consumed Share and Streams settings and excludes the unconsumed staged Streams
enable/max-groups/max-size controls. Scalar constraints use `refined_type`
directly in operator production validation; cross-field checks resolve absent
values against broker defaults. Invalid values set
`KafkaConfigValid=False`, reason `KafkaConfigInvalid`, with the camel-case CRD
path in the message and leave the existing broker ConfigMap untouched.

The operator renders a deterministic numeric `[runtime]` TOML section in field
declaration order and omits it for absent or empty tuning. The generated Kafka
CRD contains all 117 properties and their schema minimum/maximum/minLength
constraints. `crabka-broker` remains a dev-dependency only.

KafkaSpec constructor fallout was updated explicitly in:

- `crates/operator/src/controller/common.rs`
- `crates/operator/src/controller/grpc_gateway.rs`
- `crates/operator/src/controller/kafka.rs`
- `crates/operator/src/controller/kafka_node_pool.rs`
- `crates/operator/src/controller/listeners.rs`
- `crates/operator/src/controller/metrics.rs`
- `crates/operator/src/controller/network_policy.rs`
- `crates/operator/src/controller/topic.rs`
- `crates/operator/src/crd/kafka.rs`
- `crates/operator/tests/reconcile_ca.rs`
- `crates/operator/tests/reconcile_ca_rotation.rs`
- `crates/operator/tests/reconcile_inter_broker_mtls.rs`
- `crates/operator/tests/reconcile_kafka.rs`
- `crates/operator/tests/reconcile_kafka_authorization.rs`
- `crates/operator/tests/reconcile_listener_auth.rs`
- `crates/operator/tests/reconcile_listener_gssapi.rs`
- `crates/operator/tests/reconcile_listener_ingress.rs`
- `crates/operator/tests/reconcile_listener_oauth.rs`
- `crates/operator/tests/reconcile_oauth_introspection.rs`
- `crates/operator/tests/reconcile_oauth_trust.rs`

Verification:

- strict RED: missing `BrokerTuning` and `KafkaSpec.broker_tuning`
- focused broker-tuning reconciliation/validation tests: 5 passed
- `cargo test -p crabka-operator --lib crd`: 157 passed
- `cargo test -p crabka-operator --test reconcile_kafka`: 28 passed
- `cargo check -p crabka-operator --all-targets`: passed
- strict all-target/all-feature Clippy with `-D warnings`: passed
- operator rustfmt check and `git diff --check`: passed
- generated CRD/runtime field-set comparison: 117 equals 117, no differences

Concerns: none.

## Review fix: broker tuning rolls pods

Fix commit: `fb66e9e4`, following `5c513cc0`.

RED:

- `cargo test -p crabka-operator --lib combined_hash_tracks_nonempty_broker_tuning_only`
- failed because nonempty tuning and absent tuning both hashed to
  `e3b0c44298fc1c14`.

GREEN:

- the focused regression passed;
- `controller::common::config_hash_tests`: 11 passed;
- `reconcile_kafka`: 28 passed;
- strict operator all-target/all-feature Clippy with `-D warnings`: passed;
- operator rustfmt check and `git diff --check`: passed.

`combined_config_hash` now includes the deterministic
`BrokerTuning::render_runtime_toml()` bytes. Absent and
`Some(BrokerTuning::default())` still take the existing empty-hash collapse;
any rendered tuning value changes the desired pool hash and rolls broker pods.

---

# G9 Task 5 report

Status: DONE

## Final completion after clean rebase (2026-07-13)

The prior blockers are resolved on the rebased branch. Production SQL now preserves hash bucket
identity through writes, timestamp metadata, transport, recovery, filtered restore, marker
inheritance, registry activation, and cursor termination. The external process harness runs a real
populated hash split at bucket 8 and emits strict schema-v3 evidence for every crash point.

The final debugging pass also repaired merge-era and restart regressions exposed only by the broad
serial suite:

- stale predecessor timestamp descriptors are rehomed through the active map while active
  participant identities remain authoritative;
- hash scan terminals use logical rowids instead of the sparse bucket-prefixed physical integer;
- rN-only computes start without advertising structural control that cannot durably receipt via a
  local r0 engine;
- already-materialized successor folds carry a replay seed into canonical writer activation;
- irreversible prologue recovery is proved from durable topology-activation receipts before older
  control steps are skipped;
- the raw-KV test runtime supplies an explicit snapshot-derived identity table mapping;
- nullable hash shard keys receive a deterministic bucket;
- distributed explicit transactions continue to acquire the single r0-hosted lease; and
- exact authenticated control-step bindings are persisted by the crash driver for replay.

Fresh final verification:

- authoritative hash crash matrix: 19/19 cases passed (11 source restore, 2 publication,
  6 retirement/resume);
- `validate-gres-split-crash-evidence.py --self-test`: passed;
- schema-v3 full-matrix validation under `target/g9-hash-split-crash`: passed;
- `crabka-pgexec`: 350 library tests and all integration targets passed serially;
- `crabka-gres-substrate`: 131 library tests and all integration targets passed serially;
- `crabka-gres`: 87 library tests, 21 runtime tests, 12 topology nemesis tests, and 27 topology
  crash-contract tests passed serially;
- `crabka-gres-ranges`: 179 library tests and all integration targets passed, with the one
  initially stale explicit-gate fixture corrected and its real multiprocess lease/expiry test
  rerun green;
- `cargo check --workspace --all-targets`: passed (warnings only);
- `git diff --check`: passed.

No commit, staging operation, or push was performed. The protected
`crates/gres-ranges/src/control.rs` remains byte-identical at SHA-256
`2c0431bcf5edc5f54e5b8d9e1abd0be031ecf1d834e98169600ea1547395ce05`.

## Resume after timestamp wire prerequisite (`809e29a8`)

Strict RED converted the production live transfer regression from the prior degenerate whole-table
move into a real midpoint split at `(logical table 10, bucket 8, rowid 0)`. SQL inserts the pinned
`int4` corpus `0..15`; its big-endian FNV-1a hashes form a bijection over buckets `0..15`. The first
production restore failed with `checkpoint invalid: malformed timestamp intent metadata key`.
A focused substrate test reproduced the same failure for a bucket-zero intent sidecar and a
bucket-15 prewrite reservation.

Root cause: `CheckpointFilter::timestamp_metadata_key` accepted only the legacy bucketless sidecar
tails and always reconstructed an ordinary `(table,rowid)` `RangeKey`. Its TXD2 descriptor paths
also ignored `TimestampTxnOperation.bucket`. Commit `524dae75` adds strict legacy-or-hash sidecar
decoding (including a validated tag and bucket zero), uses the bucket in descriptor selection and
rewriting, and updates stale substrate test literals that did not compile with the mandatory field.

GREEN live evidence after the fix:

- predecessor primary-version fold equals the disjoint successor union;
- left and right contain exactly eight `HashPrimaryVersion` keys each;
- their physical bucket sets are exactly `0..7` and `8..15`, with no ordinary primary key and no
  cross-bucket leakage;
- a fresh post-publication SQL session returns exactly `0..15`.

Fresh command results on `524dae75`:

- `cargo test -p crabka-gres-substrate --lib --no-fail-fast`: 124 passed;
- focused production midpoint runtime: 1 passed;
- parser/catalog/pgkv/pgmvcc library suites: 309 passed;
- `cargo test -p crabka-gres-control --lib --no-fail-fast`: 65 passed;
- split model: 5 passed; split nemesis: 4 passed;
- topology process split crash binary: 23 passed, preserving all 19 kill-point contract tests;
- changed-file rustfmt and `git diff --check`: clean;
- the pre-existing `crates/gres-ranges/src/control.rs` working-tree bytes remained exactly unchanged
  (verified against the pre-work SHA-256).

Status remains `BLOCKED`, not `DONE`: the authoritative externally enabled G8 process harness is
still hard-coded to two ordinary `SHARDED` tables and a rowid boundary. This slice did not
parameterize its live workload, remote committed/aborted timestamp prewrites, CLI invocation,
physical-fold oracle, or schema-v2 evidence validator for a hash table and bucket-8 boundary, and
therefore did not run the requested source-restore/publication/retirement live hash kill points.
The in-process midpoint is real production data movement, but it is not a substitute for that
multiprocess crash gate.

## Landed-seam audit

Base `aa18718bb7f8493a3070edb882254e609c6e7f44` already contained the broad Task 5 vertical slice in the earlier bulk commit `d9489c823`: parser AST and `SHARDED BY HASH`; catalog `HashSharding`; bucket-leading `pgkv`/`pgmvcc` keys; `RangeKey { table_id, bucket, rowid }`; equality/scatter routing; layout co-location validation; registry `HashPlacement`; and the G9 Task 4 co-partitioned join execution arm. Those seams were re-read and tested rather than treated as completion.

The audit found two concrete fail-open/drift gaps, fixed in `2e723943`:

- sharding catalog payloads were not independently versioned, accepted trailing bytes, and accepted empty column/group names;
- range routing duplicated the physical FNV-1a implementation instead of using `pgkv` as the single bucket authority, and validated specs accepted empty names.

`crates/gres-ranges/src/control.rs` remained un-staged and unmodified by this task.

## RED/GREEN evidence

RED:

- `hash_spec_rejects_empty_column_and_group_names` failed because `HashShardSpec::new` accepted malformed names.
- `hash_sharding_decode_rejects_trailing_bytes_and_empty_names` failed first on accepted trailing bytes.

GREEN:

- constructors/decoders now fail closed for empty names, invalid versions, invalid tags/counts and trailing bytes;
- range routing delegates bucket calculation to `crabka_pgkv::key::hash_bucket`;
- the cross-layer corpus pins routing and physical encoding to identical results.

## Hash algorithm and corpus

Algorithm: 64-bit FNV-1a, offset basis `0xcbf29ce484222325`, prime `0x100000001b3`, byte-at-a-time XOR then wrapping multiply. A validated power-of-two bucket count uses `hash & (bucket_count - 1)`.

Pinned 16-bucket corpus (bytes -> bucket): empty -> 5; `a` -> 12; `alpha` -> 11; `alice` -> 7; `[00 ff 01]` -> 3.

## Commits

- `2e723943 feat(gres): hash sharding as bucket intervals`
- this report is committed separately.

## Tests and results

- parser/catalog/pgkv/pgmvcc library suites: 308 passed.
- registry hash validation: 1 passed.
- gres-ranges hash-focused library tests: 5 passed.
- hash-sharded multirange integration: 4 passed.
- co-partitioned join selection/execution focus: 1 passed.
- unchanged G8 `split_model`: 5 passed.
- unchanged G8 `split_nemesis`: 4 passed.
- `git diff --check` for changed feature files: clean.
- `cargo fmt --all -- --check`: blocked by the pre-existing unstaged formatting delta in `crates/gres-ranges/src/control.rs`; the changed feature files are rustfmt-clean.

## Brief-clause review

- Exact hash syntax parsing, power-of-two rejection, AST/catalog roundtrip, bucket-leading pgkv/pgmvcc encoding, equality routing, scatter routing, registry/CRD metadata, layout co-location validation, and the co-partitioned join arm were present on the base and exercised by their existing tests.
- This change adds independent catalog versioning, strict decode completion, validated non-empty names, one authoritative physical/routing hash implementation, and an exact deterministic corpus.
- The unchanged split model and split nemesis prove the generic interval machinery remains green.

## Divergences and concerns

- No new balancer-free operation-sequence property test was added. The landed map constructor validates corresponding group buckets after each reconstructed layout and therefore rejects a violating transition, but the repository still lacks the requested dedicated generated sequence model that performs coordinated group split/move operations.
- The existing G8 split/nemesis tests use ordinary `SHARDED` tables; they were run unchanged, but no practical multiprocess crash gate was run with a `SHARDED BY HASH` workload in this task.
- The parser supports multiple hash columns and the optional existing `COLOCATED WITH group` suffix in addition to the brief's singular spelling. The exact singular spelling is accepted and invalid bucket counts are rejected, but a standalone SQL deparser does not exist in `pgparser`, so no parser-level deparse API was added.
- Because these gaps are explicit Task 5 gates, they preclude an unqualified completion status.

## Continuation audit and blocker

The binding design is singular: `docs/superpowers/specs/2026-07-09-crabka-gres-g9-distributed-maturity-design.md:73` specifies `SHARDED BY HASH (col) BUCKETS n`, and its test gate at line 109 requires hash-spec tables through the whole G8 crash/nemesis suite.

That gate cannot honestly be parameterized onto the current deployed split bridge. The bridge rejects every sharded or hash-sharded table in `crates/gres-ranges/src/tenant.rs:1048-1050` and again at `1170-1172`. The latter physical path also rejects any non-empty predecessor table at `1177-1182`; its checkpoint is explicitly named `local-empty-table-no-data-migration` at line 2100. Consequently there is no real hash-table filtered checkpoint/restore/catch-up/cutover implementation for a process crash/nemesis test to exercise. A test-only parameter would either fail at validation or mock the missing physical path, contrary to the requirement.

Completing the requested gate therefore requires first implementing the missing G8 deployed data-migration bridge for populated sharded tables (filtered checkpoint/restore, tail catch-up, cutover, restart recovery), then enabling hash bucket boundaries and coordinated co-location moves. This is architectural prerequisite work, not a Task 5 test parameterization seam. The repository's own audit records the same blocker in `.superpowers/sdd/audit-g7-g9.md:311-315` and recommends completing the physical operator path before the G9 gates.

Because the parent explicitly disallowed a concerns status and requires a practical non-mock process crash shard, the final status is `BLOCKED` on this absent physical G8 prerequisite. The earlier fast-model and in-process results remain valid but do not satisfy the missing system gate.

## Resume after populated-transfer prerequisite

Commits `2e656607`/`4c958aaa` removed the populated-table transfer rejection and proved a whole-table hash move. The singular binding grammar was then enforced test-first in `042ab98f`: multi-column syntax now fails with SQLSTATE `42601`, while the canonical singular AST golden remains green. Commit `472dfff2` adds a generated 0-64 action coordinated split/move placement model over two group tables and four buckets, checks the invariant after every action, and includes an uncoordinated-move teeth variant.

Fresh inspection found the remaining live-system blocker below the repaired transfer layer: production registry `RangeBoundary` still contains only `(table_id, rowid)` (`crates/gres-control/src/record.rs:824-830`), the CRD key has the same two fields (`crates/operator/src/crd/gres_tenant.rs:67-76`), the CLI constructs split boundaries only from table/rowid (`crates/cli/src/gres.rs:465-510`), and operator conversion necessarily drops any bucket (`crates/operator/src/controller/gres_tenant.rs:1115-1123`). Therefore the production external orchestrator cannot express a midpoint hash-bucket boundary `(table_id, bucket, rowid)` at all. The prerequisite regression is necessarily the degenerate whole-table move described in its report.

This prevents the requested real populated midpoint bucket split and its external crash/nemesis parameterization without first versioning and migrating registry layout records, CRD schema/status, CLI arguments, operator conversions, split-operation sealing/digests, and compatibility tests to carry the bucket component. Running the external suite with `--rowid` would exercise a different lexicographic boundary and would be weakened evidence. Status remains `BLOCKED` on this production interval-schema prerequisite.

## Resume after bucket-aware control-plane prerequisite

Commits `1f222ac8`/`9fe54501` completed the bucket-aware registry/CRD/CLI/operator seam. A strict RED changed the live populated hash regression from a degenerate table move to a bucket-8 midpoint and inserted a pinned 16-value corpus spanning both halves. The predecessor inspection found **zero** `HashPrimaryVersion` keys, and the right successor received zero rows instead of eight.

This exposes the remaining data-plane blocker: SQL execution does not use the hash-key constructors. `crates/pgexec/src/exec.rs:1039-1045` writes every INSERT with ordinary `version_key_xid`; timestamp paths likewise use ordinary `version_key_ts`. A repository-wide search finds no `hash_version_key_xid` or `hash_version_key_ts` call in either `pgexec` or `gres-ranges`; those constructors are referenced only by isolated key/checkpoint/transfer tests. The previous whole-table test's `primary_versions` helper accepted both ordinary and hash key classes, so it did not prove that SQL-created hash tables had bucket-prefixed physical rows.

Therefore a production bucket midpoint correctly has no real bucket-prefixed SQL rows to partition, and the requested external hash crash/nemesis parameterization would be false evidence. Completion requires wiring catalog hash metadata and typed hash values through xid and timestamp INSERT/UPDATE/DELETE/read/recovery paths, including intents and marker keys, before the production split suite can exercise exact bucket folds. The failing experimental edit was removed; `control.rs` remains the sole worktree delta. Status remains `BLOCKED` on this missing hash physical-write/read vertical slice.

## Resume after hash SQL data-path commits

The strict midpoint test could not reach execution on HEAD `42ee28d4`: compiling `crabka-gres-ranges` fails with eight `E0063` errors because the new mandatory `bucket` field on `TimestampTxnOperation`/`TimestampWrite` was not propagated through forwarding and tenant construction (`forward.rs:305,662,776,861,1808,3547`; `tenant.rs:4239,4315`). The transport schema also still omits bucket from both `WireTimestampWrite` and `WireTimestampOperation` (`transport.rs:534-539,588-593`), so merely adding `bucket: None` to compile would silently erase hash identity across remote prewrite/recovery—the exact restart/intent invariant this task must prove.

Accordingly the claimed physical prerequisite is not a compiling cross-crate vertical slice, and no honest external process gate can be built or run on this HEAD. Completion first requires versioning and propagating bucket identity through wire prewrite/resolve/recover operations and every conversion, with bucket-zero roundtrip tests, then rerunning the midpoint RED. The experimental runtime edit was removed and `control.rs` remains the sole unstaged delta. Status is `BLOCKED` on the incomplete prerequisite.

## Recovery-availability diagnostic after rebase (2026-07-13)

Status: BLOCKED; no commit created.

### Root cause and evidence

The focused hash `InitiatedBeforeRunningCas` run reached journal `Completed` but measured a
33,849 ms acknowledgement gap against the unchanged 25,000 ms bound. The gap ran from
05:50:12.442780 to 05:50:46.293157; activation completed at approximately 05:50:43.832.
Changing SQL INSERT/recovery timeouts did not change this interval.

Read-only tracing found that activation synchronously calls
`recover_durable_timestamp_transactions`. It scans every historical primary descriptor and
replays every participant. Committed participant identity sidecars under `meta/ts_intent/` were
never deleted, so each hash workload commit remained permanently discoverable as outstanding
recovery work. This explains why the descriptor-heavy hash workload accumulates activation work.

The correction is deliberately narrow:

- every terminal participant resolution deletes its identity sidecar in the same committer batch
  as the row state, recovered global-index operations, and scan-terminal operations;
- pending descriptors remain recovery work;
- terminal hosted participants are replayed only while a matching durable identity sidecar exists;
- remote participants remain conservative and are always replayed because local discovery cannot
  prove their settlement.

Absence of the identity is therefore a safe completion proof: physical row resolution,
global-index settlement, scan-terminal advancement, and identity deletion share one atomic commit.
An immediate idempotent replay remains accepted through `write_is_resolved_to`.

### RED/GREEN

RED command:

`cargo test -p crabka-pgexec --test transactions committed_descriptor_recovery_resolves_put_delete_and_global_index_intents -- --exact --nocapture`

Result before the fix: 1 failed, 37 filtered; the new assertion failed because
`durable_timestamp_intent_identities()` was non-empty after committed recovery.

GREEN result after the fix: 1 passed, 0 failed, 37 filtered, finished in 0.02 s. The test also
performs a second idempotent recovery after the sidecar is absent and rechecks row/delete/global
index visibility.

A complementary `crabka-gres-ranges` unit test pins the selection rule for settled terminal,
outstanding terminal, and pending descriptors. Its crate build is blocked before reaching the test
by unrelated current-HEAD compiler errors:

- `crates/metadata/src/image.rs:1161`: mismatched closing delimiter;
- `crates/client-core/src/fetch.rs`: missing `IsolatedFetch` and stale call shape;
- `crates/log/src/log.rs:323`: `Log` has no `log_start_override` field.

Because those errors also prevent rebuilding the focused process test, the warm live hash case and
schema-v3 evidence validator were not run. The 25 s availability bound and 240 s wrapper deadline
were not changed.

### Intentional files

- `crates/pgexec/src/timestamp_txn.rs`
- `crates/pgexec/tests/transactions.rs`
- `crates/gres-ranges/src/tenant.rs`

The three files were rustfmt-formatted directly and `git diff --check` is clean. The protected
`crates/gres-ranges/src/control.rs` remains SHA-256
`2c0431bcf5edc5f54e5b8d9e1abd0be031ecf1d834e98169600ea1547395ce05` with numstat 14/7.
`Cargo.lock` regeneration from the rebase's manifest/lock mismatch was restored and is not part of
this change.

### Concern

The local atomicity and idempotence regression is green, but the recovery-selection test and the
required live availability/evidence gates cannot be claimed until the unrelated rebase compiler
breakage is repaired. Per instruction, no commit was made.
