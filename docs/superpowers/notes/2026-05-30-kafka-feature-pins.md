# Kafka 4.0 feature-flag verification (group.version / transaction.version / metadata.version)

**Date:** 2026-05-30
**Image used:** `mirror.gcr.io/apache/kafka:4.0.0` (the task's first-choice `mirror.gcr.io/confluentinc/cp-kafka:7.9.0` was not cached and not pulled; `mirror.gcr.io/apache/kafka:4.0.0` is the upstream Apache release of the same Kafka 4.0.0 build, so feature levels / enums are identical to cp-kafka 4.0).
  - JRE: Eclipse Temurin 21.0.6+7. Kafka jars dated 2025-03-14, version 4.0.0.
**Method:** Formatted scratch KRaft clusters with `kafka-storage.sh`, ran a single-node broker, queried `kafka-features.sh describe`, produced a real transaction and dumped `__transaction_state`, and decompiled feature-version / `Errors` class bytecode pulled straight out of the image jars. Web source (apache/kafka @ tag `4.0.0`) used only to corroborate values already observed empirically.

All values below were empirically confirmed in the image unless explicitly noted.

---

## group.version (KIP-848)

### 1. Supported integer level range — CONFIRMED: `0..=1`
- Live broker `kafka-features.sh describe`: `group.version  SupportedMinVersion: 0  SupportedMaxVersion: 1`.
- Formatter rejects level 2: `kafka-storage format --feature group.version=2` →
  `java.lang.IllegalArgumentException: No feature:group.version with feature level 2`.
- Levels 0 and 1 both format successfully.

### 2. Default at `--release-version 4.0` — CONFIRMED: `1`
- `kafka-storage format -c ... --release-version 4.0` writes `bootstrap.checkpoint`.
  `kafka-dump-log --cluster-metadata-decoder` of that file shows:
  `{"type":"FEATURE_LEVEL_RECORD","data":{"name":"group.version","featureLevel":1}}`.
- Live broker (`describe`): `group.version ... FinalizedVersionLevel: 1`.

### 3. metadata.version threshold for group.version=1 default — CONFIRMED: metadata.version 4.0-IV0 = level 22
- Bootstrap-checkpoint sweep over `--release-version` (reading the emitted FEATURE_LEVEL_RECORDs):
  | release-version | metadata.version level | group.version default |
  |---|---|---|
  | 3.9-IV0 | 21 | (absent ⇒ 0) |
  | **4.0-IV0** | **22** | **1** |
  | 4.0-IV1 | 23 | 1 |
  | 4.0-IV2 | 24 | 1 |
  | 4.0-IV3 | 25 | 1 |
- This matches the enum's `bootstrapMetadataVersion`: `GroupVersion.GV_1` carries `MetadataVersion.IBP_4_0_IV0`. Verified in the image bytecode (`GroupVersion.class` constant pool references `IBP_4_0_IV0` and `MINIMUM_VERSION`) and in upstream source `server-common/.../GroupVersion.java` @4.0.0.
- IMPORTANT NUANCE: `GV_1.dependencies()` is an **empty map** (no hard metadata.version dependency). The formatter happily writes `group.version=1` even with `--release-version 3.3-IV3` (verified). So "metadata.version 22" is the *bootstrap MV at which 1 becomes the auto-selected default*, not a level below which 1 is forbidden. The repo's KIP-1022 phrasing ("dependency") should be read as the bootstrap threshold, not a validation floor.

---

## transaction.version (KIP-890)

### 4. Supported integer level range — CONFIRMED: `0..=2`
- Live broker `describe`: `transaction.version  SupportedMinVersion: 0  SupportedMaxVersion: 2`.
- Formatter rejects level 3: `--feature transaction.version=3` →
  `IllegalArgumentException: No feature:transaction.version with feature level 3`.
- Levels 0, 1, 2 all format successfully.

### 5. Default at `--release-version 4.0` — CONFIRMED: `2`
- Bootstrap checkpoint at `--release-version 4.0`:
  `{"name":"transaction.version","featureLevel":2}`.
- Live broker (`describe`): `transaction.version ... FinalizedVersionLevel: 2`.

### 6. metadata.version that TV_1 and TV_2 depend on — CONFIRMED
- Per-constant `bootstrapMetadataVersion` (from image `TransactionVersion.class` bytecode, which references `MINIMUM_VERSION` and `IBP_4_0_IV2`; corroborated by upstream `TransactionVersion.java` @4.0.0):
  - `TV_0` (level 0): bootstrap MV = `MetadataVersion.MINIMUM_VERSION` (= 3.3-IV3, level 7), deps = empty
  - `TV_1` (level 1): bootstrap MV = `IBP_4_0_IV2` (= 4.0-IV2, **level 24**), deps = empty
  - `TV_2` (level 2): bootstrap MV = `IBP_4_0_IV2` (= 4.0-IV2, **level 24**), deps = empty
- Empirical release-version sweep of the bootstrap checkpoint confirms transaction.version first appears as a default at 4.0-IV2, jumping straight to **2** (TV_1 is *never* a standalone release default because both TV_1 and TV_2 bootstrap at the same MV and the formatter picks the highest default ≤ MV):
  | release-version | metadata.version level | transaction.version default |
  |---|---|---|
  | 4.0-IV0 | 22 | (absent ⇒ 0) |
  | 4.0-IV1 | 23 | (absent ⇒ 0) |
  | **4.0-IV2** | **24** | **2** |
  | 4.0-IV3 | 25 | 2 |
- Same nuance as group.version: `dependencies()` is empty for all three; the formatter accepts `transaction.version=1` or `=2` even at metadata.version 3.3-IV3 (verified). So 4.0-IV2 is the bootstrap MV, not a hard floor.

### 7. On-disk flexible record format at TV_1 — CONFIRMED (schema + real captured bytes)

**Schema** — extracted verbatim from the image jar
`kafka-transaction-coordinator-4.0.0.jar : common/message/TransactionLogValue.json`:

- `validVersions: "0-1"`, `flexibleVersions: "1+"` → **version 1 is the first flexible version** (KIP-915 note in the schema: bumping the version no longer keeps the record backward compatible; only add/remove tagged fields).
- Version header on disk is a plain **int16** (`0x0001`), not a varint.
- Fields (in wire order for v1):

  | field | type | versions | tag |
  |---|---|---|---|
  | ProducerId | int64 | 0+ | — |
  | PreviousProducerId | int64 | taggedVersions 1+ | tag 0, default -1 |
  | NextProducerId | int64 | taggedVersions 1+ | tag 1, default -1 |
  | ProducerEpoch | int16 | 0+ | — |
  | TransactionTimeoutMs | int32 | 0+ | — |
  | TransactionStatus | int8 | 0+ | — |
  | TransactionPartitions | []PartitionsSchema (nullable) | 0+ | — |
  | &nbsp;&nbsp;PartitionsSchema.Topic | string | 0+ | — |
  | &nbsp;&nbsp;PartitionsSchema.PartitionIds | []int32 | 0+ | — |
  | TransactionLastUpdateTimestampMs | int64 | 0+ | — |
  | TransactionStartTimestampMs | int64 | 0+ | — |
  | ClientTransactionVersion | int16 | taggedVersions 1+ | tag 2, default 0 |

  At v1 (flexible) the three tagged fields (PreviousProducerId/0, NextProducerId/1, ClientTransactionVersion/2) live in the tagged-field section; strings/arrays use compact (length+1) encoding.

`TransactionLogKey.json` (key schema): `validVersions: "0"`, `flexibleVersions: "none"`, single field `TransactionalId: string` (non-compact, v0).

**Real captured record (TV_1 broker).** Formatted with `--release-version 4.0 --feature transaction.version=1`, confirmed `FinalizedVersionLevel: 1`, produced a committed transaction (`kafka-producer-perf-test --transactional-id my-txn-id --transaction-duration-ms 300`), then `kafka-dump-log --transaction-log-decoder` on `__transaction_state-11`. Decoded state machine (all written by the TV_1 coordinator):
```
Empty -> Ongoing(partitions=[txtest-0]) -> PrepareCommit -> CompleteCommit -> Ongoing -> PrepareCommit -> CompleteCommit
key: transaction_metadata::transactionalId=my-txn-id
```

Raw value bytes of the **Ongoing** record (valueSize 48), copied out of the segment with `dd ... | xxd`:
```
00 01                            version = 1
00 00 00 00 00 00 00 00          ProducerId = 0            (int64)
00 00                            ProducerEpoch = 0         (int16)
00 00 ea 60                      TransactionTimeoutMs = 60000 (int32)
01                               TransactionStatus = 1 (Ongoing) (int8)
02                               TransactionPartitions: compact-array len 2-1 = 1 entry
  07 74 78 74 65 73 74           Topic: compact-string len 7-1=6 = "txtest"
  02                             PartitionIds: compact-array len 2-1 = 1
    00 00 00 00                  partition 0              (int32)
  00                             PartitionsSchema tagged-field count = 0
00 00 01 9e 7b 4b 36 7a          TransactionLastUpdateTimestampMs (int64)
00 00 01 9e 7b 4b 36 7a          TransactionStartTimestampMs      (int64)
00                               top-level tagged-field count = 0
```
Contiguous hex: `0001 0000000000000000 0000 0000ea60 01 02 07 7478746573 74 02 00000000 00 000001 9e7b4b367a 000001 9e7b4b367a 00`
(2+8+2+4+1+14+8+8+1 = 48 bytes ✓).

Notes on the captured bytes:
- The version header is `00 01` = int16 1 ⇒ this is the v1 (flexible) record, as expected at TV_1.
- All three tagged fields are absent here ⇒ the top-level tagged-field count is `0x00`. PreviousProducerId/NextProducerId/ClientTransactionVersion are only emitted when non-default, so a simple Ongoing record at TV_1 carries an empty tagged section. (At TV_2 / epoch-bump scenarios these tagged fields populate.)
- `transactionLogValueVersion()` in the enum returns `(short)(featureLevel >= 1 ? 1 : 0)` — so TV_1 and TV_2 both write value version 1; only TV_0 writes version 0. Confirmed by the on-disk `00 01` header.

### 8. AddPartitionsToTxn verify-only error for a partition not in the txn — CONFIRMED: `TRANSACTION_ABORTABLE` = **120**
- Error-code numbers decoded directly from the image's `kafka-clients-4.0.0.jar : org/apache/kafka/common/protocol/Errors.class` `<clinit>` bytecode (each enum entry pushes `<arrayIndex>, <code>, <message>`):
  - `INVALID_TXN_STATE` → code **48** (array index 49)
  - `TRANSACTION_ABORTABLE` → code **120** (array index 121)
  - (sanity-checked: `NONE` → code 0, index 1)
- Which one AddPartitionsToTxn returns: upstream `TransactionCoordinator.scala` @4.0.0, verify-only path:
  ```scala
  partitions.map { part =>
    if (txnMetadata.topicPartitions.contains(part)) (part, Errors.NONE)
    else (part, Errors.TRANSACTION_ABORTABLE)
  }
  ```
  ⇒ a partition NOT part of the txn yields **TRANSACTION_ABORTABLE (120)**.
- Note: AddPartitionsToTxn is at API version 0..5 in this build (`usable: 5`); verifyOnly is the KIP-890 path. The error-code numbers are empirically from the image jar; the branch selection is from upstream source (a hand-crafted raw verify-only request was not sent because the container ships a JRE only — no python/javac — and the broker port was not host-routable on macOS; the code path is unambiguous in source and the numeric code is image-verified).

---

## Cross-check: repo metadata_version.rs vs cp-kafka 4.0 MetadataVersion — MATCHES (one note)

Repo table at `crates/metadata/src/metadata_version.rs`: levels 7..25, MIN=7 (3.3-IV3), MAX=25 (4.0-IV3).

Image `MetadataVersion` enum constants (from `kafka-server-common-4.0.0.jar : MetadataVersion.class` bytecode):
```
IBP_3_3_IV3 IBP_3_4_IV0 IBP_3_5_IV0 IBP_3_5_IV1 IBP_3_5_IV2
IBP_3_6_IV0 IBP_3_6_IV1 IBP_3_6_IV2
IBP_3_7_IV0 IBP_3_7_IV1 IBP_3_7_IV2 IBP_3_7_IV3 IBP_3_7_IV4
IBP_3_8_IV0 IBP_3_9_IV0
IBP_4_0_IV0 IBP_4_0_IV1 IBP_4_0_IV2 IBP_4_0_IV3
IBP_4_1_IV0
```

- Names 3.3-IV3 .. 4.0-IV3 match the repo's `ivn` strings exactly, in the same order.
- Level↔name anchors empirically confirmed via bootstrap-checkpoint feature records and `kafka-features describe`:
  - 7 = 3.3-IV3 (broker SupportedMinVersion), 15 = 3.7-IV0, 20 = 3.8-IV0, 21 = 3.9-IV0,
    22 = 4.0-IV0, 24 = 4.0-IV2, 25 = 4.0-IV3 (broker SupportedMaxVersion + format default).
- **The only discrepancy is intentional-and-correct:** the 4.0.0 enum *also* defines `IBP_4_1_IV0` (= level 26), which the repo omits. This is NOT production in 4.0.0:
  - `kafka-storage format --release-version 4.1-IV0` → `metadata.version 4.1-IV0 is not yet stable.`
  - `kafka-storage format --feature metadata.version=26` → same "not yet stable" rejection.
  - Live broker advertises `SupportedMaxVersion: 4.0-IV3`.
  So the repo correctly pins MAX at 25; level 26 is an unstable/dev-only enum entry. No fix needed.

**Verdict:** No mismatch in the 7..25 range. Repo table is byte-for-byte consistent with cp/apache-kafka 4.0.0 for every level it advertises.

---

## Summary table

| # | Fact | Expected | Result | Confirmed how |
|---|---|---|---|---|
| 1 | group.version range | 0..=1 | **0..=1** | broker describe + formatter reject @2 |
| 2 | group.version default @4.0 | 1 | **1** | bootstrap checkpoint + describe |
| 3 | group.version=1 bootstrap MV | 4.0-IV0 (22) | **22 (4.0-IV0)** | release-version sweep + bytecode (deps empty) |
| 4 | transaction.version range | 0..=2 | **0..=2** | broker describe + formatter reject @3 |
| 5 | transaction.version default @4.0 | 2 | **2** | bootstrap checkpoint + describe |
| 6 | TV_1 / TV_2 bootstrap MV | — | **both 4.0-IV2 (24)** | bytecode + source + release-version sweep (deps empty) |
| 7 | TV_1 on-disk format | TransactionLogValue v1 flexible | **v1, int16 header, tagged 0/1/2; real bytes captured** | jar schema + dumped __transaction_state record |
| 8 | AddPartitionsToTxn verifyOnly, partition not in txn | TRANSACTION_ABORTABLE or INVALID_TXN_STATE | **TRANSACTION_ABORTABLE = 120** | Errors.class bytecode (code) + TransactionCoordinator.scala (branch) |
| X | metadata_version.rs vs image | match | **match (7..25); image also has unstable 4.1-IV0=26, correctly omitted)** | enum bytecode + format/describe |
