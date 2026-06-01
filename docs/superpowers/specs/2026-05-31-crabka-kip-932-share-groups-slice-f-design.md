# KIP-932 Share Groups — Slice F (GA Parity + Robustness) Design

**Date:** 2026-05-31
**Status:** Approved (F1–F6; multi-broker items deferred). Builds on broker slices A–E.
**KIP:** KIP-932 + KIP-1222 (RENEW) + KIP-1226 (lag).

## Goal

Close GA parity and the accumulated C/D robustness gaps for share groups:
RENEW acknowledgement, `read_committed` isolation, lag persistence/restore, plus
durability-retry, fragmented-window reads, and delete-metadata rewrite. (Cross-broker
leader-cache invalidation + multi-broker persister response parsing are explicitly deferred —
they require a real multi-broker share cluster to validate.)

## Items

### F1 — RENEW acknowledgement (KIP-1222)
RENEW is a **request flag** (`is_renew_ack: bool` on `ShareFetchRequest`/`ShareAcknowledgeRequest`),
NOT a 5th ack-type code. When set, the request's `acknowledgement_batches` are *renewals*: reset
the acquisition-lock deadline for the covered Acquired-by-member offsets, keeping them `Acquired`
(no delivery-count change, no SPSO advance). Only meaningful in explicit mode.
- **Broker** (`share_partition/state.rs`): add `AcquisitionState::renew(member, first, last, now, lock_dur) -> Result<(), i16>` — same ownership guard as `acknowledge` (`range_acquired_by` → else `INVALID_RECORD_STATE`), but only resets `lock_deadline = Some(now + lock_dur)` on the Acquired range; marks `dirty`; does NOT touch state/delivery_count/SPSO.
- **Handlers** (`share_fetch.rs`, `share_acknowledge.rs`): if `req.is_renew_ack`, route each `(first,last,types)` to `st.renew(...)` instead of `apply_one_ack`. (Renew ignores the per-offset ack types — the batch ranges identify which acquired records to renew.)
- **Client** (`client-consumer/src/share/`): add `ShareConsumer::renew(&record) -> Result<(), …>` (explicit mode) that sends a standalone `ShareAcknowledgeRequest { is_renew_ack: true, … }` covering the record's range. No new `ShareAckType` variant (renew is a flag).

### F2 — `read_committed` isolation
No per-request isolation field exists; `read_committed` is a **group config**.
- `ShareGroupConfig` (`share/config.rs`): add `isolation_level: ShareIsolationLevel { ReadUncommitted (default), ReadCommitted }`.
- Share-partition leader (`share_fetch.rs`): when read_committed, the materialize/read upper bound becomes `effective_upper = lso.min(hwm)` instead of `hwm` (so records from open transactions are never surfaced), reusing `Partition::lso()`. Pass `effective_upper` to `materialize` (it already early-returns when `end_offset >= bound`) and as the read `limit`.
- **Aborted records:** within the committed range, aborted-transaction records must not be delivered. Reuse the Fetch path's `Log::aborted_in_range(start, lso)` (called inside the existing `read_acquired_bytes` `spawn_blocking`, where `part.log` is already locked) to identify aborted offset ranges, and **archive** those offsets in `AcquisitionState` (mark `Archived` so they're skipped and SPSO can advance past them) when materializing under read_committed. (If precise abort-range→offset archival proves complex, the LSO clamp alone delivers the core "no uncommitted records" guarantee; implement the clamp first, then aborted-record archival as the refinement — document whichever is shipped.)

### F3 — lag persistence/restore (KIP-1226)
`AcquisitionState::load_from` currently hardcodes `delivery_complete_count = 0`, losing the
persisted lag metric across leader (re)load.
- Add a `delivery_complete_count: i32` parameter to `load_from`; set the field from it.
- `manager.get_or_load`: the persister `read_state` result (`SharePartitionState`) already carries `delivery_complete_count` — thread it into `load_from`.
- Confirms DescribeShareGroupOffsets `Lag` (best-effort `hwm − SPSO`) + the persisted `delivery_complete_count` survive restart.

### F4 — durability retry
`SharePartitionLeaderManager::persist_if_dirty` clears `st.dirty = false` **unconditionally**,
even when `write_state` fails — so a failed persist is silently dropped until the next mutation.
Fix: clear `dirty` only on `Ok`; on `Err`, leave `dirty` set so the lock-sweeper retries. (Closes
the Slice-C "Accept ack'd to client but SPSO advance non-durable" gap.)

### F5 — fragmented-window read
`share_fetch.rs` reads one contiguous `[min_first, max_last+1)` blob across all acquired ranges,
so disjoint ranges (after poison-pill archival between Available runs) surface gap bytes in
`records`. Fix: read each acquired range's bytes separately (N `read_raw` calls) and concatenate
**only acquired bytes** into the single `RecordsPayload::Raw` (verbatim v2 batches concatenate
cleanly). `acquired_records` already lists the authoritative ranges.

### F6 — delete-metadata rewrite
`DeleteShareGroupOffsets` deletes persister state + invalidates the leader cache but does NOT
rewrite the group's v14 `ShareGroupStatePartitionMetadata` (the topic stays in `initialized`,
reading back as `-1`). The authoritative `state.initialized` lives in the share actor.
- Add `ShareGroupActorMessage::DropTopicMetadata { topic_id, reply: oneshot::Sender<()> }`; the arm does `state.initialized.retain(|(tid,_)| *tid != topic_id)` then `flush_pending(PendingShareRecords { state_partition_metadata: Some(state_partition_metadata_from(state)), .. })`.
- `delete_share_group_offsets.rs`: after the delete loop succeeds, `ng.find_share(&gid)` → send `DropTopicMetadata { topic_id }` per deleted topic, await. Best-effort (log on failure).

## Non-goals (Slice F)
- Cross-broker leader-cache invalidation; multi-broker `SharePersister::send_to_leader` response parsing (deferred — need a multi-broker share cluster).
- Per-record read_committed precision beyond LSO-clamp + best-effort aborted archival (refine later if needed).

## Testing
- **Unit (`state.rs`):** `renew` resets the lock + keeps Acquired (no SPSO advance / no delivery bump); `renew` on a non-Acquired range → INVALID_RECORD_STATE; `load_from` restores `delivery_complete_count`.
- **Integration (`tests/share_consume.rs` / `share_admin_offsets.rs`):**
  - F1: explicit consumer renews a record before the lock expires → it is NOT redelivered after the original lock duration (proves the lock was extended); without renew it IS redelivered.
  - F2: produce a committed batch + an open/aborted transaction; a read_committed share group only delivers the committed records (the LSO clamp / aborted records excluded).
  - F3: consume+ack, restart, DescribeShareGroupOffsets reflects the restored lag / `delivery_complete_count`.
  - F4: (manager unit or integration) a persist failure leaves `dirty` set so the next sweep retries.
  - F5: after a partial release creating a gap, a ShareFetch's `records` contains only acquired offsets (decode the payload; no gap offsets).
  - F6: DeleteShareGroupOffsets then DescribeShareGroupOffsets (describe-all) no longer lists the deleted topic; survives restart (v14 rewritten).
- **JVM (optional, docker):** a read_committed JVM share consumer, or RENEW via the JVM client, if the tooling supports it — best-effort.

## Acceptance gate
1. fmt clean. 2. `clippy --workspace --all-targets -- -D warnings` clean. 3. `cargo test --workspace` green. 4. no drift. 5. All new F unit + integration tests green.

## Decomposition (sequential batches)
- **F-state:** `state.rs` (`renew`, `load_from` dcc param) + `manager.rs` (`persist_if_dirty` fix F4, `get_or_load` thread dcc F3) + `ShareGroupConfig.isolation_level`.
- **F-fetch:** `share_fetch.rs`/`share_acknowledge.rs` — renew routing (F1), read_committed LSO-clamp + aborted archival (F2), per-range concat (F5).
- **F-lifecycle+client:** F6 (actor `DropTopicMetadata` + delete handler) + F1 client renew.
- **F-tests:** the integration suite.
