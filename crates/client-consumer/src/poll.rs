//! `Consumer::poll` — issues one `Fetch` covering every assigned partition,
//! advances next-offsets, and returns the decoded records.

use std::{collections::HashMap, time::Duration};

use crabka_ids::LeaderEpoch;
use crabka_protocol::owned::{
    fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
};

use crate::{
    builder::{AutoOffsetReset, IsolationLevel},
    consumer::{Consumer, ConsumerRecord, Header},
    error::ConsumerError,
};

/// Synthetic leader id meaning "leader unknown → use the bootstrap connection".
/// Matches `BrokerPool`'s bootstrap slot so a fallback Fetch is sent via
/// `Client::send` rather than `Client::broker(id)`.
const BOOTSTRAP_LEADER: i32 = -1;
const UNKNOWN_FETCH_OFFSET: i64 = -1;
const UNKNOWN_LEADER_ID: i32 = -1;
pub(crate) const DEFAULT_FETCH_PARTITION_MAX_BYTES: i32 = 1 << 20;
pub(crate) const DEFAULT_FETCH_MAX_BYTES: i32 = 50 * 1024 * 1024;

/// One fetchable partition's request fields:
/// `(partition, fetch_offset, current_leader_epoch, last_fetched_epoch)`.
type FetchSpec = (i32, i64, LeaderEpoch, LeaderEpoch);

/// Partitions to fetch, grouped first by leader id, then by topic.
type FetchByLeader = HashMap<i32, HashMap<String, Vec<FetchSpec>>>;

fn fetch_leader_id(leader_id: i32, knows_leader: bool) -> i32 {
    if leader_id >= 0 && knows_leader {
        leader_id
    } else {
        BOOTSTRAP_LEADER
    }
}

fn should_use_bootstrap_leader(leader: i32) -> bool {
    leader == BOOTSTRAP_LEADER
}

fn fetch_offset_or_unknown(offsets: &HashMap<(String, i32), i64>, key: &(String, i32)) -> i64 {
    offsets.get(key).copied().unwrap_or(UNKNOWN_FETCH_OFFSET)
}

fn is_read_committed(isolation_level: IsolationLevel) -> bool {
    isolation_level == IsolationLevel::ReadCommitted
}

fn is_transient_transport_error(e: &crabka_client_core::ClientError) -> bool {
    matches!(
        e,
        crabka_client_core::ClientError::Connect { .. }
            | crabka_client_core::ClientError::Disconnected
            | crabka_client_core::ClientError::Timeout(_)
            | crabka_client_core::ClientError::Io(_)
    )
}

fn is_transient_poll_error(e: &ConsumerError) -> bool {
    matches!(e, ConsumerError::Client(client) if is_transient_transport_error(client))
}

fn aborted_txn_started(first_offset: i64, batch_base_offset: i64) -> bool {
    first_offset <= batch_base_offset
}

fn should_drop_aborted_batch(
    read_committed: bool,
    is_transactional: bool,
    producer_is_aborted: bool,
) -> bool {
    read_committed && is_transactional && producer_is_aborted
}

fn record_offset(base_offset: i64, offset_delta: i32) -> i64 {
    base_offset + i64::from(offset_delta)
}

fn record_timestamp(base_timestamp: i64, timestamp_delta: i64) -> i64 {
    base_timestamp + timestamp_delta
}

fn build_fetch_topic(
    name: String,
    topic_id: crabka_protocol::primitives::uuid::Uuid,
    partitions: Vec<FetchSpec>,
    partition_max_bytes: i32,
) -> FetchTopic {
    FetchTopic {
        topic: name,
        topic_id,
        partitions: partitions
            .into_iter()
            .map(
                |(p, off, leader_epoch, last_fetched_epoch)| FetchPartition {
                    partition: p,
                    fetch_offset: off,
                    // Unwrap the leader epochs to raw wire `int32` at the
                    // FetchRequest encode boundary.
                    current_leader_epoch: leader_epoch.get(),
                    last_fetched_epoch: last_fetched_epoch.get(),
                    partition_max_bytes,
                    ..Default::default()
                },
            )
            .collect(),
        ..Default::default()
    }
}

fn build_fetch_request(
    timeout_ms: i32,
    isolation_level: IsolationLevel,
    max_bytes: i32,
    topics: Vec<FetchTopic>,
) -> FetchRequest {
    FetchRequest {
        max_wait_ms: timeout_ms,
        min_bytes: 1,
        max_bytes,
        isolation_level: isolation_level.wire(),
        topics,
        ..Default::default()
    }
}

fn build_latest_offsets_request(by_topic: HashMap<String, Vec<i32>>) -> ListOffsetsRequest {
    let topics: Vec<ListOffsetsTopic> = by_topic
        .into_iter()
        .map(|(name, partitions)| ListOffsetsTopic {
            name,
            partitions: partitions
                .into_iter()
                .map(|p| ListOffsetsPartition {
                    partition_index: p,
                    timestamp: -1, // LATEST
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    ListOffsetsRequest {
        replica_id: -1,
        topics,
        ..Default::default()
    }
}

impl Consumer {
    /// Returns the records from every v2 batch the broker returned per
    /// assigned partition, or an empty vec on timeout. Under
    /// `read_committed` isolation, control batches and records belonging to
    /// aborted transactions are filtered client-side using the response's
    /// `aborted_transactions` list (the broker returns verbatim bytes).
    /// Rebalances are handled transparently by the internal coordinator
    /// task, which mutates the live `assigned` snapshot in place; `poll()`
    /// simply reads it on each call.
    #[allow(clippy::too_many_lines)]
    #[tracing::instrument(
        name = "consumer.poll",
        level = "debug",
        skip_all,
        fields(
            group_id = %self.group_id,
            timeout_ms = timeout.as_millis(),
            assigned_partitions = tracing::field::Empty,
            leaders = tracing::field::Empty,
            records = tracing::field::Empty,
        ),
        err
    )]
    pub async fn poll(&mut self, timeout: Duration) -> Result<Vec<ConsumerRecord>, ConsumerError> {
        // 0. Materialise any pending `seek` whose partition is now assigned,
        //    BEFORE the fetch is built. This must run after the coordinator's
        //    post-assignment prime (which it does — the prime happens off the
        //    poll path) so a sought position wins over the prime, and before the
        //    FetchRequest below so no record below the sought offset is ever
        //    fetched (and none above it skipped). See `seek.rs`.
        self.apply_pending_seeks().await;

        // 1. Resolve any i64::MAX sentinels (auto.offset.reset=Latest) via
        //    ListOffsets(timestamp=-1).
        if let Err(e) = self.resolve_latest_sentinels().await {
            if is_transient_poll_error(&e) {
                self.client.reconnect_bootstrap().await;
                return Ok(Vec::new());
            }
            return Err(e);
        }

        // KIP-320: refresh leader epochs and proactively validate any position
        // whose leader epoch advanced, before fetching. Truncated partitions
        // are reset here (or surfaced for auto.offset.reset=None below).
        if let Err(e) = self.refresh_leader_epochs().await {
            if is_transient_poll_error(&e) {
                self.client.reconnect_bootstrap().await;
                return Ok(Vec::new());
            }
            return Err(e);
        }
        let truncated = match self.validate_positions().await {
            Ok(truncated) => truncated,
            Err(e) if is_transient_poll_error(&e) => {
                self.client.reconnect_bootstrap().await;
                return Ok(Vec::new());
            }
            Err(e) => return Err(e),
        };
        if !truncated.is_empty() {
            self.apply_truncation(&truncated).await?;
        }

        // 2. Build a FetchRequest covering every assigned partition.
        let assigned = self.assigned.lock().await.clone();
        tracing::Span::current().record("assigned_partitions", assigned.len());
        if assigned.is_empty() {
            tokio::time::sleep(timeout).await;
            return Ok(Vec::new());
        }

        // Group the fetchable partitions by their leader id so each FetchRequest
        // reaches the broker that actually hosts the partition. On a
        // multi-broker cluster the bootstrap connection is rarely the leader of
        // every partition, and a Fetch sent to a non-leader returns
        // NOT_LEADER_OR_FOLLOWER instead of records. The per-partition leader
        // lives in the `positions` sidecar (populated by `refresh_leader_epochs`
        // from Metadata, whose `refresh_metadata` also teaches the pool each
        // broker's address so `Client::broker(id)` can connect).
        //
        // A partition whose leader is unknown (or whose advertised address is
        // unusable) falls back to the bootstrap connection (synthetic id `-1`)
        // for this round. The `refresh_leader_epochs` pass at the top of every
        // poll already re-pulls Metadata, so the next poll re-targets it once
        // the leader is learnable — no extra refresh needed here.
        //
        // (partition, fetch_offset, current_leader_epoch, last_fetched_epoch).
        // Lock order: next_offsets first, then positions (matching the
        // coordinator's order so poll can never deadlock against a rebalance).
        // Both guards are dropped before any per-leader Fetch is issued — the
        // sends are await points and we must never hold a Mutex guard across an
        // `.await`.
        let mut by_leader: FetchByLeader = HashMap::new();
        {
            let offsets = self.next_offsets.lock().await;
            let positions = self.positions.lock().await;
            for (t, p) in &assigned {
                // Skip partitions still awaiting validation — they must not be
                // fetched until proven consistent.
                if positions
                    .get(&(t.clone(), *p))
                    .is_some_and(|x| x.awaiting_validation)
                {
                    continue;
                }
                let next = offsets.get(&(t.clone(), *p)).copied().unwrap_or(0);
                let pos = positions.get(&(t.clone(), *p)).copied().unwrap_or_default();
                // Route to the leader when its id is known AND the pool has a
                // dialable address for it; otherwise fall back to the bootstrap
                // connection. `knows_broker` is a synchronous registry lookup
                // (no await), so it's safe to call while the offsets/positions
                // guards are held. A leader whose advertised address is unusable
                // (e.g. port 0 from an in-process test broker) is treated as
                // unknown — the bootstrap broker is the leader in that
                // single-broker case anyway.
                let leader =
                    fetch_leader_id(pos.leader_id, self.client.knows_broker(pos.leader_id));
                by_leader
                    .entry(leader)
                    .or_default()
                    .entry(t.clone())
                    .or_default()
                    .push((*p, next, pos.leader_epoch, pos.offset_epoch));
            }
        }

        tracing::Span::current().record("leaders", by_leader.len());
        let topic_ids = self.topic_ids.lock().await.clone();
        let timeout_ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);

        // Issue one Fetch per leader. All guards are released; we collect every
        // response before re-locking to process them. Sent sequentially so a
        // single parked leader can't starve the others' deadlines beyond the
        // per-request timeout (and to keep the borrow on `self.client` simple).
        let mut responses = Vec::with_capacity(by_leader.len());
        for (leader, by_topic) in by_leader {
            let topics: Vec<FetchTopic> = by_topic
                .into_iter()
                .map(|(name, plist)| {
                    let topic_id = topic_ids.get(&name).copied().unwrap_or_default();
                    build_fetch_topic(name, topic_id, plist, self.fetch_partition_max_bytes)
                })
                .collect();
            let req = build_fetch_request(
                timeout_ms,
                self.isolation_level,
                self.fetch_max_bytes,
                topics,
            );
            let resp = if should_use_bootstrap_leader(leader) {
                match self.client.send(req).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        if is_transient_transport_error(&e) {
                            self.client.reconnect_bootstrap().await;
                            continue;
                        }
                        return Err(e.into());
                    }
                }
            } else {
                match self.client.broker(leader).send(req).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        if is_transient_transport_error(&e) {
                            self.client.evict_broker(leader);
                            continue;
                        }
                        return Err(e.into());
                    }
                }
            };
            responses.push(resp);
        }

        // 3. Decode each partition's RecordBatches, advance next-offsets.
        //
        // The wire-level `records` field can carry multiple concatenated
        // RecordBatches; we iterate every v2 batch, emit one ConsumerRecord
        // per Record, and bump next_offsets to the highest seen offset + 1.
        // Reverse-map topic_id → name. At Fetch v ≥ 13 the response carries
        // only `topic_id`; `topic.topic` is empty.
        let id_to_name: HashMap<_, _> = topic_ids
            .iter()
            .map(|(name, id)| (*id, name.clone()))
            .collect();

        // Re-snapshot the assignment: a cooperative rebalance may have
        // revoked partitions while this Fetch was in flight. Records for
        // partitions we no longer own must be dropped — the new owner will
        // serve them from the offset we committed at revoke time. Snapshot
        // before locking `next_offsets` to keep the coordinator's
        // assigned→next_offsets lock order (avoids deadlock).
        let still_owned: std::collections::HashSet<(String, i32)> =
            self.assigned.lock().await.iter().cloned().collect();

        let mut out: Vec<ConsumerRecord> = Vec::new();
        // Set when a NOT_LEADER_OR_FOLLOWER response carried no current_leader
        // hint: we refresh metadata after the processing loop (we can't `.await`
        // while the `offsets`/`positions` guards are held) so the next poll
        // re-targets the new leader.
        let mut refresh_after_processing = false;
        let mut offsets = self.next_offsets.lock().await;
        // Process every per-leader response with the identical per-partition
        // logic (error-first, offset advance, fetch_floor, read_committed). The
        // partition key is unique across leaders, so the order responses are
        // drained in doesn't matter.
        for topic in responses.iter().flat_map(|resp| &resp.responses) {
            let topic_name = if topic.topic.is_empty() {
                id_to_name.get(&topic.topic_id).cloned().unwrap_or_default()
            } else {
                topic.topic.clone()
            };
            for part in &topic.partitions {
                // Drop records for partitions revoked while this Fetch was
                // in flight (cooperative rebalance transparency).
                if !still_owned.contains(&(topic_name.clone(), part.partition_index)) {
                    continue;
                }

                let key = (topic_name.clone(), part.partition_index);

                // KIP-320 in-band truncation: leader served no records and told
                // us where to truncate (diverging_epoch.end_offset >= 0).
                if part.diverging_epoch.end_offset >= 0 {
                    self.handle_truncation_in_poll(
                        &mut offsets,
                        &key,
                        part.diverging_epoch.end_offset,
                    )?;
                    continue;
                }
                // Error-first: inspect the partition error_code before decoding.
                match part.error_code {
                    0 => {}
                    1 /* OFFSET_OUT_OF_RANGE */ => {
                        // Reset per policy using the response's log_start_offset
                        // (the broker includes it in every OOR partition response).
                        // We must NOT use a hardcoded 0: if retention has moved
                        // log_start forward, re-fetching from 0 re-triggers OOR
                        // forever. Mirrors what the replicator does on OOR.
                        // No RPC needed — log_start_offset is already in `part`.
                        let fetch_offset = fetch_offset_or_unknown(&offsets, &key);
                        let log_start = part.log_start_offset;
                        let (topic, partition) = (key.0.clone(), key.1);
                        match self.auto_offset_reset {
                            AutoOffsetReset::Earliest => {
                                // Reset to the real log start, not 0.
                                offsets.insert(key.clone(), log_start);
                            }
                            AutoOffsetReset::Latest => {
                                // Plant i64::MAX sentinel; resolved next poll
                                // by resolve_latest_sentinels via ListOffsets.
                                offsets.insert(key.clone(), i64::MAX);
                            }
                            AutoOffsetReset::None => {
                                return Err(ConsumerError::LogTruncation {
                                    topic,
                                    partition,
                                    fetch_offset,
                                    safe_offset: log_start,
                                });
                            }
                        }
                        continue;
                    }
                    6 /* NOT_LEADER_OR_FOLLOWER */ => {
                        // A routing miss, NOT a truncation: we sent the Fetch to
                        // a broker that no longer leads this partition (e.g. a
                        // leadership change since the last metadata refresh).
                        // Re-target the leader so the next poll routes correctly;
                        // do NOT set awaiting_validation (nothing diverged).
                        let mut positions = self.positions.lock().await;
                        if part.current_leader.leader_id >= 0 {
                            // The broker handed us the new leader inline (KIP-320
                            // current_leader hint). Adopt it immediately.
                            let p = positions.entry(key.clone()).or_default();
                            p.leader_id = part.current_leader.leader_id;
                            // Wrap the KIP-320 current-leader hint (raw wire
                            // `int32`) at the Fetch-response decode boundary.
                            p.leader_epoch = LeaderEpoch(part.current_leader.leader_epoch);
                        } else {
                            // No hint: force a metadata refresh after this loop
                            // so the next poll learns the new leader. Reset the
                            // stale leader id so the bootstrap fallback (and a
                            // re-flag, if metadata advances the epoch) kicks in.
                            if let Some(p) = positions.get_mut(&key) {
                                p.leader_id = UNKNOWN_LEADER_ID;
                            }
                            drop(positions);
                            refresh_after_processing = true;
                        }
                        continue;
                    }
                    74 /* FENCED_LEADER_EPOCH */
                    | 75 /* UNKNOWN_LEADER_EPOCH */ => {
                        let mut positions = self.positions.lock().await;
                        if let Some(p) = positions.get_mut(&key) {
                            // Force refresh_leader_epochs to re-flag against
                            // fresher metadata next poll (any real epoch >= 0 > -1).
                            p.leader_epoch = LeaderEpoch(UNKNOWN_LEADER_ID);
                            // Only gate on validation when we have a consumed epoch
                            // to validate against. A never-consumed partition
                            // (offset_epoch < 0) has nothing to validate; flagging it
                            // would wedge it — validate_positions skips offset_epoch
                            // < 0, and the fetch builder skips awaiting_validation.
                            if p.offset_epoch.is_known() {
                                p.awaiting_validation = true;
                            }
                        }
                        continue;
                    }
                    other => {
                        return Err(ConsumerError::Server(other));
                    }
                }

                let Some(payload) = &part.records else {
                    continue;
                };
                // Legacy MessageSet payloads are skipped here; the consumer
                // only handles v2 batches.
                let Some(batches) = payload.as_v2() else {
                    continue;
                };
                // The broker returns whole record batches whose last offset is
                // >= the requested fetch_offset, even when the batch starts
                // before it (e.g. after an OFFSET_OUT_OF_RANGE reset or when
                // a single large batch straddles log_start). Kafka's JVM
                // client skips any records below the position; we do the same.
                // Capture the position now — before `next_offset_after` updates
                // it — so the filter baseline matches the actual fetch offset.
                let fetch_floor = offsets.get(&key).copied().unwrap_or(0);
                // read_committed filtering happens entirely client-side: the
                // broker returns verbatim on-disk bytes (control batches,
                // aborted records and all) plus an `aborted_transactions`
                // list. We replay Kafka's algorithm — walk batches in offset
                // order, tracking which producer_ids have an open aborted
                // transaction, and drop transactional records from those.
                let read_committed = is_read_committed(self.isolation_level);
                // Aborted txns sorted by first_offset; consumed front-to-back
                // as batch offsets advance past each entry's start.
                let mut aborted: std::collections::VecDeque<(i64, i64)> = if read_committed {
                    let mut v: Vec<(i64, i64)> = part
                        .aborted_transactions
                        .as_deref()
                        .unwrap_or(&[])
                        .iter()
                        .map(|a| (a.first_offset, a.producer_id))
                        .collect();
                    v.sort_unstable();
                    v.into()
                } else {
                    std::collections::VecDeque::new()
                };
                // producer_ids with a currently-open aborted transaction.
                let mut aborted_pids: std::collections::HashSet<i64> =
                    std::collections::HashSet::new();
                for batch in batches {
                    // Move every aborted txn that starts at or before this
                    // batch into the active set.
                    if read_committed {
                        while let Some(&(first_offset, pid)) = aborted.front() {
                            if aborted_txn_started(first_offset, batch.base_offset) {
                                aborted_pids.insert(pid);
                                aborted.pop_front();
                            } else {
                                break;
                            }
                        }
                    }
                    // Control batches (commit/abort markers) carry no user
                    // records. A control batch for a producer ends its aborted
                    // transaction; drop the batch either way.
                    if batch.attributes.is_control_batch() {
                        if read_committed {
                            aborted_pids.remove(&batch.producer_id);
                        }
                        continue;
                    }
                    // Drop transactional records belonging to an aborted txn.
                    if should_drop_aborted_batch(
                        read_committed,
                        batch.attributes.is_transactional(),
                        aborted_pids.contains(&batch.producer_id),
                    ) {
                        continue;
                    }
                    for r in &batch.records {
                        let offset = record_offset(batch.base_offset, r.offset_delta);
                        // Skip records that precede the fetch floor: the broker
                        // returned a whole batch whose base_offset < our
                        // position (straddle case — see fetch_floor comment).
                        if offset < fetch_floor {
                            continue;
                        }
                        out.push(ConsumerRecord {
                            topic: topic_name.clone(),
                            partition: part.partition_index,
                            offset,
                            leader_epoch: batch.partition_leader_epoch,
                            timestamp: record_timestamp(batch.base_timestamp, r.timestamp_delta),
                            key: r.key.clone(),
                            value: r.value.clone(),
                            headers: r
                                .headers
                                .iter()
                                .map(|h| Header {
                                    key: h.key.clone(),
                                    value: h.value.clone(),
                                })
                                .collect(),
                        });
                    }
                }
                if let Some(next) = next_offset_after(batches) {
                    offsets.insert(key.clone(), next);
                    // Advance the position's offset_epoch to the highest batch
                    // leader epoch consumed, so the next Fetch sends the correct
                    // last_fetched_epoch (KIP-320). Lock order holds: offsets is
                    // already locked, positions acquired second.
                    if let Some(last_epoch) = batches.iter().map(|b| b.partition_leader_epoch).max()
                    {
                        let mut positions = self.positions.lock().await;
                        // Wrap the batch's raw wire `partition_leader_epoch`
                        // (`int32`) at the RecordBatch decode boundary.
                        positions.entry(key.clone()).or_default().offset_epoch =
                            LeaderEpoch(last_epoch);
                    }
                }
            }
        }
        // Drop the offsets guard before any `.await`: refreshing metadata is an
        // RPC, and we must never hold a Mutex guard across an await point.
        drop(offsets);
        tracing::Span::current().record("records", out.len());
        if refresh_after_processing {
            // Best-effort: a NOT_LEADER_OR_FOLLOWER without a current_leader
            // hint means our cached leader is stale; learn the new one so the
            // next poll routes correctly. A failure is non-fatal — the next
            // refresh_leader_epochs pass retries.
            let _ = self.client.refresh_metadata().await;
        }
        Ok(out)
    }
}

/// The offset to fetch next after consuming `batches`: one past the highest
/// `base_offset + last_offset_delta` across all decoded batches. `None` when
/// there are no batches (offset unchanged). Used so the consumer advances past
/// control/aborted batches that emit no records, instead of re-fetching them.
fn next_offset_after(batches: &[crabka_protocol::records::RecordBatch]) -> Option<i64> {
    batches
        .iter()
        .map(|b| b.base_offset + i64::from(b.last_offset_delta) + 1)
        .max()
}

impl Consumer {
    /// Replace any `i64::MAX` sentinels in `next_offsets` (planted by
    /// `auto_offset_reset = Latest` at build time) with the real log-end
    /// offset from `ListOffsets(timestamp=-1)`.
    #[tracing::instrument(
        name = "consumer.resolve_latest_sentinels",
        level = "debug",
        skip_all,
        fields(group_id = %self.group_id, sentinels = tracing::field::Empty),
        err
    )]
    async fn resolve_latest_sentinels(&self) -> Result<(), ConsumerError> {
        let mut offsets = self.next_offsets.lock().await;
        let sentinels: Vec<(String, i32)> = offsets
            .iter()
            .filter(|(_, v)| **v == i64::MAX)
            .map(|(k, _)| k.clone())
            .collect();
        if sentinels.is_empty() {
            return Ok(());
        }
        tracing::Span::current().record("sentinels", sentinels.len());
        let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
        for (t, p) in &sentinels {
            by_topic.entry(t.clone()).or_default().push(*p);
        }
        let lo = self
            .client
            .send(build_latest_offsets_request(by_topic))
            .await?;
        for t in &lo.topics {
            for p in &t.partitions {
                offsets.insert((t.name.clone(), p.partition_index), p.offset);
            }
        }
        Ok(())
    }
}

impl Consumer {
    /// Apply truncations detected by the proactive validate pass to
    /// `next_offsets`, honoring `auto.offset.reset` (None → error on the first
    /// truncated partition).
    #[tracing::instrument(
        name = "consumer.apply_truncation",
        level = "debug",
        skip_all,
        fields(group_id = %self.group_id, truncated = truncated.len()),
        err
    )]
    async fn apply_truncation(
        &self,
        truncated: &HashMap<(String, i32), i64>,
    ) -> Result<(), ConsumerError> {
        let mut offsets = self.next_offsets.lock().await;
        for (key, safe_offset) in truncated {
            if let AutoOffsetReset::None = self.auto_offset_reset {
                let fetch_offset = fetch_offset_or_unknown(&offsets, key);
                return Err(ConsumerError::LogTruncation {
                    topic: key.0.clone(),
                    partition: key.1,
                    fetch_offset,
                    safe_offset: *safe_offset,
                });
            }
            offsets.insert(key.clone(), *safe_offset);
        }
        Ok(())
    }

    /// In-band `diverging_epoch` handler used inside the poll loop while the
    /// `next_offsets` guard is already held.
    fn handle_truncation_in_poll(
        &self,
        offsets: &mut HashMap<(String, i32), i64>,
        key: &(String, i32),
        safe_offset: i64,
    ) -> Result<(), ConsumerError> {
        if let AutoOffsetReset::None = self.auto_offset_reset {
            let fetch_offset = fetch_offset_or_unknown(offsets, key);
            return Err(ConsumerError::LogTruncation {
                topic: key.0.clone(),
                partition: key.1,
                fetch_offset,
                safe_offset,
            });
        }
        offsets.insert(key.clone(), safe_offset);
        Ok(())
    }
}

#[cfg(test)]
mod offset_advance_tests {
    use std::collections::HashMap;

    use assert2::{assert, check};
    use crabka_protocol::{
        owned::fetch_request::ReplicaState,
        primitives::uuid::Uuid as WireUuid,
        records::{RecordBatch, RecordsPayload},
        tagged_fields::UnknownTaggedFields,
    };

    use super::*;

    fn id(n: u8) -> WireUuid {
        let mut b = [0u8; 16];
        b[15] = n;
        WireUuid(b)
    }

    #[test]
    fn fetch_leader_id_uses_known_non_negative_leader_or_bootstrap() {
        check!(BOOTSTRAP_LEADER == -1);
        check!(UNKNOWN_LEADER_ID == -1);
        check!(UNKNOWN_FETCH_OFFSET == -1);
        check!(fetch_leader_id(3, true) == 3);
        check!(fetch_leader_id(-1, true) == BOOTSTRAP_LEADER);
        check!(fetch_leader_id(3, false) == BOOTSTRAP_LEADER);
        check!(should_use_bootstrap_leader(BOOTSTRAP_LEADER));
        check!(!should_use_bootstrap_leader(3));
    }

    #[test]
    fn poll_helpers_preserve_sentinel_filter_and_record_math_boundaries() {
        let key = ("topic-a".to_string(), 2);
        let mut offsets = HashMap::new();

        assert!(fetch_offset_or_unknown(&offsets, &key) == UNKNOWN_FETCH_OFFSET);
        offsets.insert(key.clone(), 42);
        check!(fetch_offset_or_unknown(&offsets, &key) == 42);

        check!(is_read_committed(IsolationLevel::ReadCommitted));
        check!(!is_read_committed(IsolationLevel::ReadUncommitted));
        check!(aborted_txn_started(10, 10));
        check!(aborted_txn_started(10, 11));
        check!(!aborted_txn_started(11, 10));
        check!(should_drop_aborted_batch(true, true, true));
        check!(!should_drop_aborted_batch(false, true, true));
        check!(!should_drop_aborted_batch(true, false, true));
        check!(!should_drop_aborted_batch(true, true, false));
        check!(record_offset(100, 7) == 107);
        check!(record_timestamp(1000, 33) == 1033);
    }

    #[test]
    fn transient_transport_error_classification_is_narrow() {
        use std::{io, time::Duration};

        use crabka_client_core::ClientError;

        check!(is_transient_transport_error(&ClientError::Disconnected));
        check!(is_transient_transport_error(&ClientError::Timeout(
            Duration::from_millis(10)
        )));
        check!(is_transient_transport_error(&ClientError::Io(
            io::Error::new(io::ErrorKind::ConnectionReset, "reset")
        )));
        check!(is_transient_transport_error(&ClientError::Connect {
            addr: "127.0.0.1:9092".parse().unwrap(),
            source: io::Error::new(io::ErrorKind::ConnectionRefused, "refused"),
        }));
        check!(is_transient_poll_error(&ConsumerError::Client(
            ClientError::Connect {
                addr: "127.0.0.1:9092".parse().unwrap(),
                source: io::Error::new(io::ErrorKind::ConnectionRefused, "refused"),
            }
        )));
        check!(!is_transient_transport_error(&ClientError::Server {
            error_code: 6
        }));
        check!(!is_transient_poll_error(&ConsumerError::Server(6)));
        check!(!is_transient_transport_error(
            &ClientError::IncompatibleVersion {
                api_key: 1,
                broker_min: 0,
                broker_max: 1,
                client_min: 2,
                client_max: 3,
            }
        ));
    }

    #[test]
    fn build_fetch_request_preserves_topic_partition_and_limits() {
        let topic = build_fetch_topic(
            "topic-a".into(),
            id(7),
            vec![(2, 42, LeaderEpoch(5), LeaderEpoch(4))],
            128 * 1024,
        );
        assert!(
            topic
                == FetchTopic {
                    topic: "topic-a".into(),
                    topic_id: id(7),
                    partitions: vec![FetchPartition {
                        partition: 2,
                        current_leader_epoch: 5,
                        fetch_offset: 42,
                        last_fetched_epoch: 4,
                        log_start_offset: -1,
                        partition_max_bytes: 128 * 1024,
                        replica_directory_id: WireUuid::default(),
                        high_watermark: i64::MAX,
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                }
        );

        let req = build_fetch_request(
            123,
            IsolationLevel::ReadCommitted,
            2 * 1024 * 1024,
            vec![topic.clone()],
        );
        assert!(
            req == FetchRequest {
                replica_id: -1,
                max_wait_ms: 123,
                min_bytes: 1,
                max_bytes: 2 * 1024 * 1024,
                isolation_level: 1, // read_committed wire value
                session_id: 0,
                session_epoch: -1,
                topics: vec![topic],
                forgotten_topics_data: Vec::new(),
                rack_id: String::new(),
                cluster_id: None,
                replica_state: ReplicaState {
                    replica_id: -1,
                    replica_epoch: -1,
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }
        );
    }

    #[test]
    fn latest_offsets_request_uses_latest_timestamp_and_replica_sentinel() {
        let mut by_topic = HashMap::new();
        by_topic.insert("topic-a".to_string(), vec![3]);
        let req = build_latest_offsets_request(by_topic);

        assert!(
            req == ListOffsetsRequest {
                replica_id: -1,
                isolation_level: 0,
                topics: vec![ListOffsetsTopic {
                    name: "topic-a".into(),
                    partitions: vec![ListOffsetsPartition {
                        partition_index: 3,
                        current_leader_epoch: -1,
                        timestamp: -1,
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                }],
                timeout_ms: 0,
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }
        );
    }

    #[test]
    fn advance_target_uses_last_offset_delta_not_record_count() {
        // A batch spanning offsets 10..=14 (last_offset_delta = 4) but carrying
        // zero surviving records must still advance the fetch offset to 15.
        let batch = RecordBatch {
            base_offset: 10,
            last_offset_delta: 4,
            records: vec![],
            ..Default::default()
        };
        let payload = RecordsPayload::V2(vec![batch]);
        let batches = payload.as_v2().unwrap();
        assert!(super::next_offset_after(batches) == Some(15));
    }

    #[test]
    fn advance_target_none_for_empty() {
        let payload = RecordsPayload::V2(vec![]);
        assert!(super::next_offset_after(payload.as_v2().unwrap()) == None);
    }
}
