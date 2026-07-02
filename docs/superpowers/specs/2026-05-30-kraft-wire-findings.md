# KRaft wire findings (mirror.gcr.io/apache/kafka:4.0.0) — Slice 0 capture

Date: 2026-05-30
Source: pure-JVM KRaft cluster (`mirror.gcr.io/apache/kafka:4.0.0`): a combined controller+broker
leader (`jvm-kraft`, node 1) + a broker-only observer (`jvm-obs`, node 2) on a shared
docker network. tcpdump sidecar in the controller's net namespace captured port 9093;
`kafka-dump-log`/`kafka-metadata-shell` decoded the on-disk artifacts. Raw request
headers parsed from TCP payload (tshark's bundled Kafka dissector does not understand
the 4.0 flexible protocol — it mis-decodes; raw byte parsing was authoritative).

## Negotiated versions (observer → controller, on the CONTROLLER listener :9093)

Parsed from raw TCP payloads (api_key i16, api_version i16 after the 4-byte length):

| api_key | name | version | count | relevance to decode-only spike |
|---------|------|---------|-------|-------------------------------|
| 18 | ApiVersions | **4** | 2 | REQUIRED — observer negotiates first |
| 1  | Fetch | **17** | 39 | REQUIRED — pull-based metadata replication |
| 62 | BrokerRegistration | 4 | 1 | NOT needed (broker-mgmt, not Raft) |
| 63 | BrokerHeartbeat | 1 | 21 | NOT needed (broker-mgmt, not Raft) |

- **No `FetchSnapshot` (59) was sent** — the log is tiny and un-snapshotted, so the
  observer fetched from offset 0 over plain `Fetch`. Decode-only needs only
  **ApiVersions v4 + Fetch v17**. (BrokerRegistration/Heartbeat go unanswered by the
  spike controller; that is fine — the observer still Fetches + decodes the log.)
- Fetch v17 is **flexible** (Fetch FLEXIBLE_MIN = v12). RequestHeader is **v2**
  (flexible). ResponseHeader for Fetch is **v1** (flexible, empty tagged-fields byte).
- ApiVersions response header is the Kafka special-case: **v0, NO tagged-fields byte**,
  regardless of request flexibility.

### Captured first Fetch (api 1 v17) request, raw

```
0000009e 0001 0011 00000000 000d 726166742d636c69656e742d32 00 ...
len=158  key=1 ver=17 corr=0  clientIdLen=13 "raft-client-2"  hdr-tagged=0x00 ...
```

Notable: the request carries `cluster_id` as a Fetch tagged field — the bytes
`455a686c765a615f53527937384e524475566d345177` = `EZhlvZa_SRy78NRDuVm4Qw` (the
cluster id) appear in the body. The RaftClient's `client_id` is `raft-client-<nodeId>`.
The per-request `replica_state.replica_id` = the observer's node id (2).

## `__cluster_metadata`

- **topic id (UUID): `00000000-0000-0000-0000-000000000001`** — on disk as base64url
  `AAAAAAAAAAAAAAAAAAAAAQ` in `partition.metadata` (`version: 0`). This is the
  well-known KRaft metadata topic id (`new Uuid(0L, 1L)`), fixed across clusters.
- partition: 0. Topic name on the wire is empty (v13+ uses topic_id).

## `quorum-state` file (`__cluster_metadata-0/quorum-state`)

```json
{"clusterId":"","leaderId":1,"leaderEpoch":1,"votedId":-1,"appliedOffset":0,
 "currentVoters":[{"voterId":1}],"data_version":0}
```

- **Leader epoch = 1** for the freshly-formatted single-voter cluster.
- `leader-epoch-checkpoint` = ASCII `0\n1\n1 0\n` → version 0, 1 entry, epoch 1 @ offset 0.
- So the spike leader is **leader_id=1, leader_epoch=1**, and the log's
  `partitionLeaderEpoch` is **1** from offset 0.

## Bootstrap checkpoint (`bootstrap.checkpoint`) — what `kafka-storage format` writes

Decoded batches (all magic 2):

| batch offsets | isControl | contents |
|---------------|-----------|----------|
| 0 | true | `SnapshotHeader {"version":0,"lastContainedLogTimestamp":0}` |
| 1–3 | false | 3× `FEATURE_LEVEL_RECORD`: `metadata.version`=**25**, `group.version`=1, `transaction.version`=2 |
| 4 | true | `SnapshotFooter {"version":0}` |

`metadata.version` featureLevel **25** is the kafka:4.0.0 default.

## Actual `__cluster_metadata-0/00000000000000000000.log` (the live log the observer Fetches)

Decoded record sequence the observer pulls from offset 0 (all `partitionLeaderEpoch=1`):

| offset | batch isControl | record |
|--------|-----------------|--------|
| 0 | true | `LEADER_CHANGE` control record (controlType 2) |
| 1 | false | `BEGIN_TRANSACTION_RECORD {"name":"Bootstrap records"}` |
| 2 | false | `FEATURE_LEVEL_RECORD metadata.version=25` |
| 3 | false | `FEATURE_LEVEL_RECORD group.version=1` |
| 4 | false | `FEATURE_LEVEL_RECORD transaction.version=2` |
| 5 | false | `END_TRANSACTION_RECORD {}` |
| 6 | false | `REGISTER_CONTROLLER_RECORD` (v0): controllerId 1, endpoints, supported features |
| 7 | false | `REGISTER_BROKER_RECORD` (v3): brokerId 1, fenced, logDirs, features |
| 8 | false | `BROKER_REGISTRATION_CHANGE_RECORD` (v0) |
| 9+ | false | periodic `NO_OP_RECORD` (v0), ~one per 500ms |

Note: offsets 1–5 are the bootstrap feature records **wrapped in a transaction**
(BEGIN/END_TRANSACTION). The control record at offset 0 (`LEADER_CHANGE`) carries the
voter set and establishes leader epoch 1.

### Minimal log the spike must serve (decode-only)

For decode-only success the observer must Fetch from offset 0 and decode without error.
The smallest faithful log is **the LEADER_CHANGE control batch at offset 0 + the
bootstrap feature records**. Serving the exact JVM bootstrap (offsets 0–5) is the safe
choice; controller/broker registrations (6–8) are not required for the observer to
decode and advance. Match `partitionLeaderEpoch=1`, magic 2, CRC-32C per batch.

## Implications for slices 1–3

- **Slice 2 (RPC codecs):** must implement Fetch **v17** (flexible) request decode +
  response encode with the KRaft tagged fields (`current_leader`, `diverging_epoch`,
  `snapshot_id`), and ApiVersions **v4**. ResponseHeader v1 for Fetch, v0 for ApiVersions.
- **Slice 1 (records):** the real control-record formats needed at minimum are
  `LEADER_CHANGE` (control), `FEATURE_LEVEL_RECORD`, `BEGIN/END_TRANSACTION_RECORD`,
  `REGISTER_CONTROLLER_RECORD` (v0), `REGISTER_BROKER_RECORD` (v3),
  `BROKER_REGISTRATION_CHANGE_RECORD` (v0), `NO_OP_RECORD` (v0). The bootstrap
  checkpoint format is SnapshotHeader + feature records + SnapshotFooter (KIP-630).
  Metadata topic id is the fixed `00000000-0000-0000-0000-000000000001`.
- **Slice 3 (state machine):** the leader advertises itself via the Fetch response's
  `current_leader{leader_id, leader_epoch}`; HWM is the log end offset; the leader epoch
  begins at 1 for a fresh single-voter cluster; the `quorum-state` file shape is recorded
  above. Observers are non-voters that pull via Fetch; the broker-mgmt channel
  (BrokerRegistration/Heartbeat) is a separate concern (slice ≥3 / broker liveness).

## Spike constants (paste into `crates/raft/src/kraft_spike.rs`)

```rust
const FETCH_REQ_VERSION: i16 = 17;          // observer's Fetch version
const APIVERSIONS_REQ_VERSION: i16 = 4;     // observer's ApiVersions version
const CLUSTER_METADATA_TOPIC_ID: [u8; 16] =
    [0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,1];     // 00000000-0000-0000-0000-000000000001
const METADATA_VERSION_LEVEL: i16 = 25;     // kafka:4.0.0 default metadata.version
const SPIKE_LEADER_ID: i32 = 1;
const SPIKE_LEADER_EPOCH: i32 = 1;          // fresh single-voter cluster
```

`required_api_keys()` seed: advertise `(18, 0, 4)` and `(1, 4, 17)`. If the observer
refuses to Fetch, widen from the iteration loop (e.g. add FetchSnapshot `(59, 0, 1)`),
recording each addition here.

## Surprises / notes

- tshark 4.x bundled Kafka dissector mis-decodes mirror.gcr.io/apache/kafka:4.0.0 flexible requests
  (reported Fetch as "Unknown api 63"); parse raw payload bytes instead.
- The combined leader node never sends Fetch over the wire (it's the leader; appends
  locally). You need a separate observer node to capture the Raft Fetch.
- The node must resolve its own advertised/voter hostname — run with
  `--hostname <name>` matching `controller.quorum.voters`, or it crash-loops on
  `UnknownHostException`.
- Data dir for `mirror.gcr.io/apache/kafka:4.0.0` is `/tmp/kafka-logs`.

## Spike result (validated 2026-05-30)

Decode-only success confirmed against a live `mirror.gcr.io/apache/kafka:4.0.0` broker observer
(`crates/broker/tests/kraft_spike_jvm.rs`, Docker-gated). The Crabka controller
served real `ApiVersions` v4 + `Fetch` v17 and replayed the captured 284-byte
bootstrap log (offsets 0–5) embedded via `include_bytes!`. The JVM observer's own
logs showed:

- `Attempting durable transition to FollowerState(epoch=1, leader=1, leaderEndpoints=…:9093)`
  — accepted the Crabka controller as the Raft leader at epoch 1.
- `High watermark set to … offset=6 … for epoch 1` — accepted the served hwm.
- `[MetadataLoader] finished catching up to the current high water mark of 6` —
  decoded every served record.
- `Publishing initial metadata at offset OffsetAndEpoch(offset=5, epoch=1) with
  metadata.version Optional[4.0-IV3]` — parsed the `FEATURE_LEVEL_RECORD` and built
  a `MetadataImage` at metadata.version level 25.
- **Zero** `CorruptRecordException` / `InvalidRecordException` / metadata-log decode
  faults.

The only JVM-side ERRORs were `BROKER_REGISTRATION` `UnsupportedVersionException` —
the deliberately out-of-scope broker-management path (the observer tries to register
over a separate RPC the spike does not serve). Not a metadata-log decode error.

**Conclusion:** the three hardest unknowns are de-risked empirically — byte-exact
`Fetch`-for-Raft framing (request v17 decode + response with `current_leader`/HWM
tagged fields), the KRaft `Fetch`/`ApiVersions` handshake, and KRaft record/batch
framing (validated by replaying the JVM's own bytes). Slices 1–3 can proceed against
the concrete facts above.

## Disposition

The spike code is **throwaway** and feature-gated (`kraft-spike`, off by default):
- `crates/raft/src/kraft_spike.rs` + `crates/raft/src/kraft_spike_metadata_log.bin`
- the `#[cfg(feature = "kraft-spike")]` interception block + stub gating in
  `crates/raft/src/server.rs`
- `crates/broker/tests/kraft_spike_jvm.rs`
- the `kraft-spike` feature in `crates/raft/Cargo.toml` and `crates/broker/Cargo.toml`

It must NOT be mistaken for production code: it serves a hand-captured static log, has
no state machine, election, writes, registration, or multi-voter support. Delete it
(or keep purely as a wire reference) once slice 3 lands the real KRaft consensus layer.
The default build is unaffected — the openraft controller path is unchanged when the
feature is off (verified: `cargo build -p crabka-raft` + 30 raft unit tests green).
