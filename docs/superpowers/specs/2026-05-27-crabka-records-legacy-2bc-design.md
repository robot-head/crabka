# Slice 2b+2c — Legacy Produce/Fetch wire support

**Status:** design
**Date:** 2026-05-27
**Roadmap:** combined 2b (schema backfill) + 2c (handler dispatch) of the v0/v1
down-conversion plan kicked off by PR #214 (slice 2a, `RecordsPayload`).
**Out of scope:** JVM acceptance tests via `kafka-console-producer/consumer`
(slice 2d, separate PR).

## Goal

Accept Produce v0–2 and Fetch v0–3 from legacy clients. Up-convert their
v0/v1 `MessageSet` payloads to the canonical v2 `RecordBatch` on the produce
path; down-convert v2 batches back to v0/v1 `MessageSet` on the fetch
response path when the requesting client is on a pre-v2 fetch version.
Storage stays v2 only.

## Non-goals

- No on-disk legacy storage. The log layer never sees a legacy MessageSet.
- No down-conversion result cache. Inline only; revisit if benchmarks
  show a leader-CPU hit on real workloads.
- No JVM-oracle acceptance tests in this slice.
- No advertised range changes beyond lowering `MIN_VERSION` (no new
  features behind a flag, no compatibility shims — see CLAUDE.md).

## Architecture

### Schema layout

```
crates/protocol/schemas/
  *.json                                # unchanged: Kafka-4.0 canonical
  versions/
    kafka_3_6_2/
      README.md                         # source: kafka.git tag 3.6.2
      ProduceRequest.json               # vendored verbatim from 3.6.2
      ProduceResponse.json
      FetchRequest.json
      FetchResponse.json
```

Only the four schemas that declare wider version ranges in 3.6.2 are
vendored. Each file is taken verbatim from `kafka.git@3.6.2`, so its
`validVersions` is whatever 3.6.2 declared (overlap with the modern
decoder is fine — the wire router only routes the *legacy-exclusive*
range Produce v0–2 / Fetch v0–3 to the legacy decoder; overlap
versions go to the modern decoder). Everything else stays in the
existing top-level layout. The `README.md` records the upstream tag
so future drift checks can re-source.

### Codegen

- `crates/protocol-codegen/src/ir.rs`: extend the schema loader to walk
  `schemas/versions/*/` in addition to `schemas/*.json`. Each version
  directory loads through the existing `ir::load_dir` and becomes a
  separate `(directory_name, Vec<MessageSpec>)` pair.
- `crates/protocol-codegen/src/emit/{owned,borrowed}.rs`: no semantic
  changes. The driver loops over version directories and writes each
  one's output under `crates/protocol/generated/{dir}/`.
- `crates/protocol-codegen/tests/snapshot.rs`: extend `CURATED` to
  include the `kafka_3_6_2` schemas. Adds four new snapshot files.
- The `drift` workflow re-runs codegen for both the top-level and
  versioned schemas and diffs the full `crates/protocol/generated/`
  tree. No workflow yaml changes needed; the dir walk subsumes it.

### Type bridge

`crates/protocol/src/legacy_compat.rs` (new) contains four hand-written
adapter impls:

```rust
impl From<kafka_3_6_2::ProduceRequest>     for ProduceRequest     { … }
impl From<ProduceResponse>                  for kafka_3_6_2::ProduceResponse { … }
impl From<kafka_3_6_2::FetchRequest>       for FetchRequest       { … }
impl From<FetchResponse>                    for kafka_3_6_2::FetchResponse   { … }
```

Direction is asymmetric on purpose: requests adapt legacy → canonical
(client sends old; handler operates on new), responses adapt canonical →
legacy (handler builds new; wire writes old). Fields present only on the
modern side default sensibly (e.g. `TransactionalId = None`,
`topic_id = Uuid::nil()`).

### Wire router

`crates/broker/src/wire.rs`: existing per-API decode-and-dispatch picks
which decoder to use:

```rust
let req = match (api_key, version) {
    (ApiKey::Produce, 0..=2) => {
        kafka_3_6_2::ProduceRequest::decode(buf, version)?.into()
    }
    (ApiKey::Produce, _) => ProduceRequest::decode(buf, version)?,
    (ApiKey::Fetch, 0..=3) => {
        kafka_3_6_2::FetchRequest::decode(buf, version)?.into()
    }
    (ApiKey::Fetch, _) => FetchRequest::decode(buf, version)?,
    …
};
```

`ApiVersionsResponse` lowers `MIN_VERSION` to 0 for both APIs. The
response-encode side mirrors: if request version is in the legacy
range, encode through the legacy response type.

### Up-conversion (Produce path)

`crates/broker/src/handlers/produce.rs`: the current
`RecordsPayload::Legacy(_) => INVALID_REQUEST` arm becomes:

```rust
RecordsPayload::Legacy(bytes) => {
    let batch = crabka_records_legacy::legacy_to_v2(&bytes)?;
    // producer_id, producer_epoch, base_sequence default to -1;
    // is_transactional = false. The bridge sets these.
    // Fall through to the existing v2 storage path with `batch`.
}
```

The match arm is dispatch-by-bytes, not by request version: a v3+
client may still send a legacy-format payload inside the request, and
this arm handles both that and the v0-2 path the wire router routed
through us. `legacy_to_v2` already exists in `crabka-records-legacy`.

### Down-conversion (Fetch path)

`crates/broker/src/handlers/fetch.rs`: new helper applied per batch
after the log read, before response serialization:

```rust
fn down_convert_for_fetch(batch: &RecordBatch, request_version: i16)
    -> Result<RecordsPayload, FetchError>
{
    if request_version >= 4 {
        return Ok(RecordsPayload::V2(batch.clone()));
    }
    let working = if batch.compression() == Compression::Zstd {
        recompress_zstd_as_snappy(batch)?
    } else {
        batch.clone()
    };
    // Drop control records; all other records flow through.
    let bytes = crabka_records_legacy::v2_to_legacy(&working,
        /* drop_control_records */ true)?;
    Ok(RecordsPayload::Legacy(bytes))
}
```

`v2_to_legacy` is already in `crabka-records-legacy`. Its current
signature is verified against the crate before plan execution; if it
does not already filter control records, this slice adds a
`drop_control_records: bool` parameter (call sites in the broker pass
`true`; existing call sites elsewhere, if any, pass `false`).

`recompress_zstd_as_snappy` reuses `crabka-compression` codecs: decompress
to the inner v2 record stream, re-emit as a new `RecordBatch` with
snappy compression set. The records themselves don't change.

### Error handling

| Failure                                     | Response                          |
|---------------------------------------------|------------------------------------|
| `legacy_to_v2` returns parse error          | `CORRUPT_MESSAGE` per partition    |
| `v2_to_legacy` returns error                | log + close connection (server bug)|
| Zstd decompress fails during down-conv      | `CORRUPT_MESSAGE` per partition    |
| Wrong-decoder for version (impossible)      | unreachable; wire router enforces  |
| Snappy re-compress fails                    | log + close connection (server bug)|

## Testing

- **Unit (`crates/protocol/src/legacy_compat.rs`)**: round-trip tests
  for each of the four adapter impls, including the modern-only fields
  that get defaulted.
- **Codegen snapshot**: add `kafka_3_6_2/{ProduceRequest,ProduceResponse,
  FetchRequest,FetchResponse}.{owned,borrowed}.rs` under
  `crates/protocol-codegen/tests/snapshots/`. Driven by extending
  `CURATED` with the new spec set.
- **Broker integration (`crates/broker/tests/legacy_protocol.rs`, new)**:
  - Produce v0 with a hand-crafted v0 `MessageSet`: assert the partition
    response succeeds, then read back via Fetch v4 and assert the v2
    `RecordBatch` matches what we'd expect from up-conversion.
  - Symmetric down-conversion: produce a v2 batch via Produce v13, then
    Fetch v3 and assert the wire bytes parse as a v0 or v1 `MessageSet`
    that matches the original records.
  - Zstd → snappy: produce a zstd-compressed batch via Produce v13,
    Fetch v3, assert the response carries snappy.
  - Control record filtering: write a v2 batch with a control record
    (txn abort marker), Fetch v3, assert the control record is absent
    in the legacy bytes.
- **Differential (extending `crates/protocol-codegen/tests/differential_*.rs`)**:
  - `differential_produce.rs` already byte-equals against JVM oracle for
    Produce v3+; add v0/v1/v2 cases.
  - `differential_fetch.rs` similarly for Fetch v0–v3.

## Implementation order (informal)

1. Vendor the four `kafka_3_6_2/` schemas; commit verbatim.
2. Extend codegen schema loader + emit driver to walk version dirs;
   regenerate `crates/protocol/generated/kafka_3_6_2/`; commit
   generated files plus snapshot fixtures.
3. Add `legacy_compat.rs` with the four `From` impls; unit-test.
4. Wire router: route v0–2 / v0–3 to legacy decoder, adapt to
   canonical, dispatch as before; lower `MIN_VERSION` in ApiVersions.
5. Produce handler: replace `INVALID_REQUEST` arm with `legacy_to_v2`.
6. Fetch handler: add `down_convert_for_fetch` helper, call from the
   response assembly. Extend `v2_to_legacy` with the
   `drop_control_records` knob if needed.
7. Integration tests; differential tests; clippy clean.

Roughly 7 ordered steps. Some can parallelize (1+2 are independent of
3+4+5+6; 5 and 6 are independent of each other once 4 lands).
