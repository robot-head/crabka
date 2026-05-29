# Crabka tiered storage 48m — Read-committed filtering on remote fetch (design)

**Date:** 2026-05-29
**Status:** Slice design. Closes a 48d follow-up. Part of the KIP-405
umbrella
(`docs/superpowers/specs/2026-05-25-crabka-tiered-storage-roadmap-design.md`).
First of the "finish KIP-405" slices (48m–48r).

## Goal

Make a `READ_COMMITTED` consumer that fetches from the remote tier behave
exactly as one fetching from the local log: it must receive the
`aborted_transactions` list so it can drop records belonging to aborted
transactions client-side.

## The actual gap

The local path does **not** filter batch bytes server-side. `do_read`
returns the verbatim record bytes (including aborted and control batches)
and, for `read_committed && !is_follower_fetch`, attaches an
`aborted_transactions` list computed from the segment transaction index:

```rust
// crates/broker/src/handlers/fetch.rs:934
let aborted = if read_committed && !is_follower_fetch {
    log.aborted_in_range(fetch_offset, effective_lso)
        .into_iter()
        .map(|e| AbortedTransaction {
            producer_id: e.producer_id,
            first_offset: e.start_offset,
            ..Default::default()
        })
        .collect()
} else {
    Vec::new()
};
```

This matches Apache Kafka: the consumer drops aborted records using the
list. The remote path (`try_remote_read`, `fetch.rs:980`) returns the
batch but **never populates `out.aborted_transactions`**, so a strict
read-committed consumer reading a remote segment can surface records from
a transaction that was aborted before the segment was sealed.

The transaction index is already copied to the remote tier — it is part
of `LogSegmentData.transaction_index` (an `Option<PathBuf>`,
`crates/remote-storage/src/storage_manager.rs:61`) and written by the
copy path (`crates/broker/src/remote_log_manager.rs:593`). 48m only adds
the read-side consumption of it.

## Approach

The transaction index has a fixed 24-byte big-endian entry layout,
defined in `crates/log/src/txn_index.rs:31`:

```
start_offset: i64 BE
last_offset:  i64 BE
producer_id:  i64 BE
```

This is the same layout `RemoteReader` already mirrors for the offset and
time indexes (`parse_offset_index`, `parse_time_index` in
`remote_reader.rs`). Add a third pure parser next to them:

```rust
/// Parse Kafka's transaction-index format (24 bytes / entry:
/// start_offset i64 BE, last_offset i64 BE, producer_id i64 BE).
pub(crate) fn parse_txn_index(bytes: &[u8]) -> Vec<AbortedTxnEntry>;

pub(crate) struct AbortedTxnEntry {
    pub start_offset: i64,
    pub last_offset: i64,
    pub producer_id: i64,
}
```

Add a `RemoteReader` method that, for the finished segment covering
`offset`, fetches its transaction index and returns the aborted
transactions overlapping the returned batch's offset range:

```rust
/// Aborted transactions overlapping `[from_offset, to_offset]` in the
/// finished remote segment covering `from_offset`. Returns an empty Vec
/// when the segment has no transaction index (NotFound) or no overlaps.
pub(crate) async fn aborted_transactions(
    &self,
    tp: &TopicIdPartition,
    leader_epoch: i32,
    from_offset: i64,
    to_offset: i64,
) -> Result<Vec<AbortedTxnEntry>, RemoteStorageError>;
```

`fetch_index(IndexType::Transaction)` returning a `NotFound`-class error
is treated as "no aborts" (the index is optional), yielding an empty
list. Overlap test mirrors `TxnIndex::aborted_in_range`: an entry
overlaps when `start_offset <= to_offset && last_offset >= from_offset`.

In `try_remote_read`, once a batch is located, compute `to_offset` as the
batch's last offset and, when `p.read_committed && !p.is_follower_fetch`,
populate the response:

```rust
let aborts = reader
    .aborted_transactions(&tp, leader_epoch, p.fetch_offset, batch_last_offset)
    .await
    .unwrap_or_default();
p.out.aborted_transactions = Some(
    aborts.into_iter().map(|e| AbortedTransaction {
        producer_id: e.producer_id,
        first_offset: e.start_offset,
        ..Default::default()
    }).collect(),
);
```

`Some(empty)` (not `None`) is the correct read-committed signal, matching
the local path and Apache Kafka. `last_stable_offset` / `high_watermark`
stay at the local view `do_read` already wrote — those are partition-wide
pointers above every remote offset, so the remote tier doesn't move them;
the abort list is what the consumer needs.

## Files

- `crates/broker/src/remote_reader.rs` — `parse_txn_index` +
  `AbortedTxnEntry` + the `aborted_transactions` method (and its
  `fetch_index_blocking(IndexType::Transaction)` wiring, which already
  exists generically).
- `crates/broker/src/handlers/fetch.rs` — `try_remote_read` reads
  `p.read_committed` / `p.is_follower_fetch` (both already on
  `PendingRead`) and sets `p.out.aborted_transactions`.

No `crates/log` changes; no config; no wire-format changes.

## Testing

- Unit: `parse_txn_index` round-trips known 24-byte entries; truncated
  trailing bytes ignored (mirrors the offset/time parsers).
- Unit: overlap filter includes/excludes entries at range boundaries.
- Integration (`LocalTieredStorage` + `InmemoryRemoteLogMetadataManager`,
  mirroring the existing `populated_reader` harness in `remote_reader.rs`
  tests): a copied segment carrying a `.txnindex` with one aborted txn →
  read-committed remote fetch returns that abort; read-uncommitted →
  `aborted_transactions` is `None`; a segment with no `.txnindex` →
  `Some(empty)`.

## Non-goals

- No server-side batch filtering (neither path does it; the consumer
  filters).
- No change to control-batch handling — remote bytes already include
  control batches verbatim from the copy path.

## Dependencies & sequencing

Independent. Runs in the first parallel batch alongside 48n (disjoint
files).

## Acceptance gates

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p crabka-broker`
- `cargo test --workspace` (no regressions)
- KIP-98/KIP-405 read-committed semantics preserved; no CRD drift.
