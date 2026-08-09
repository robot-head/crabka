//! KIP-213 foreign-key join: the five processors.
//!
//! This module is ported from Apache Kafka 4.1
//! (`org.apache.kafka.streams.kstream.internals.foreignkeyjoin`). The byte
//! formats are JVM-exact, and `tests/testdata/fk_join/behavior.json` pins them.
//! The retraction and tombstone logic is ported verbatim from the 4.1 source and
//! validated against the `inner_sequence` and `left_sequence` execution oracle.
//!
//! ## Pipeline
//!
//! Left table `a` (`KTable<K,VA>`) → `SubscriptionSend`, keyed by the foreign
//! key `KO` → registration repartition topic → `SubscriptionReceive`, which
//! writes the subscription store → `SubscriptionJoin`, which reads `sb`, the
//! right table → response repartition topic. Independently, right table `b`
//! (`KTable<KO,VB>`) → `ForeignTableJoin`, which prefix-scans the subscription
//! store and re-emits for every subscribed primary key on a right-side change →
//! response repartition topic. The response topic → `SubscriptionResolver`,
//! which reads `sa`, staleness-checks the hash, and applies the joiner →
//! `OUTPUT`, the result `KTable<K,VR>`.
//!
//! ## fk extractor and the hash
//! The foreign key is `fk_extractor(&VA)`. The subscription wrapper carries
//! `hash = murmur3(va_serde.serialize(newVA))`. The hash is `None` when the new
//! value is null. The resolver re-hashes the *current* `sa` value and drops the
//! response if it disagrees with the wrapper's hash. This discards results that a
//! rapid foreign-key change on the same primary key made stale.

use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    dsl::processors::{
        change::Change,
        fk::{
            murmur3::hash128,
            subscription::{Instruction, SubscriptionResponseWrapper, SubscriptionWrapper},
        },
    },
    processor::{
        api::{Processor, ProcessorContext},
        record::Record,
        serde::Serde,
    },
};

/// Variance-neutral marker for multi-param processor structs.
type Marker<T> = PhantomData<fn() -> T>;

// ── 1. SubscriptionSend ──────────────────────────────────────────────────────

/// `SubscriptionSendProcessorSupplier`: emit subscription wrappers per left
/// change.
///
/// On each left-table `Change<VA>`, this processor emits one or more
/// subscription wrappers keyed by the foreign key `KO`. The instruction, and
/// whether the processor propagates a delete, follows the JVM
/// `defaultJoinInstructions` or `leftJoinInstructions` exactly.
pub(crate) struct SubscriptionSendProcessor<K, VA, KO, FKE> {
    pub fk_extractor: FKE,
    pub va_serde: Box<dyn Serde<VA>>,
    pub ko_serde: Box<dyn Serde<KO>>,
    pub k_serde: Box<dyn Serde<K>>,
    pub is_left: bool,
    pub _pd: Marker<(K, VA, KO)>,
}

impl<K, VA, KO, FKE> SubscriptionSendProcessor<K, VA, KO, FKE>
where
    K: Send + 'static,
    VA: Send + 'static,
    KO: Send + Clone + 'static,
    FKE: Fn(&VA) -> KO + Send + 'static,
{
    /// Build and forward one subscription wrapper keyed by `fk`. `hash` is the
    /// cached hash of the new left value, and it is `None` on a delete.
    fn forward(
        &self,
        ctx: &mut ProcessorContext<'_, '_, KO, SubscriptionWrapper>,
        key: &K,
        fk: KO,
        instruction: Instruction,
        hash: Option<Vec<u8>>,
        timestamp: i64,
    ) {
        // FK-internal byte computations (repartition key / hash / FK compare):
        // topic is irrelevant, only consistency matters.
        let primary_key = self.k_serde.serialize("", key);
        let primary_partition = ctx.record_context().partition;
        ctx.forward(Record::new(
            Some(fk),
            SubscriptionWrapper {
                instruction,
                hash,
                primary_key,
                primary_partition,
            },
            timestamp,
        ));
    }
}

#[async_trait]
impl<K, VA, KO, FKE> Processor<K, Change<VA>, KO, SubscriptionWrapper>
    for SubscriptionSendProcessor<K, VA, KO, FKE>
where
    K: Send + Sync + 'static,
    VA: Send + Sync + 'static,
    KO: Send + Sync + Clone + 'static,
    FKE: Fn(&VA) -> KO + Send + Sync + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, KO, SubscriptionWrapper>,
        r: Record<K, Change<VA>>,
    ) {
        let key = r.key.expect("FK subscription-send requires a non-null key");
        let ts = r.timestamp;
        // Murmur3-128 hash of the serialized NEW value (None when newValue is null).
        let hash = r
            .value
            .new
            .as_ref()
            .map(|v| hash128(&self.va_serde.serialize("", v)).to_vec());
        let old_fk = r.value.old.as_ref().map(|v| (self.fk_extractor)(v));
        let new_fk = r.value.new.as_ref().map(|v| (self.fk_extractor)(v));
        let had_old = r.value.old.is_some();

        if self.is_left {
            self.left_join_instructions(ctx, &key, had_old, (old_fk, new_fk), hash.as_deref(), ts);
        } else {
            self.default_join_instructions(
                ctx,
                &key,
                had_old,
                (old_fk, new_fk),
                hash.as_deref(),
                ts,
            );
        }
    }
}

impl<K, VA, KO, FKE> SubscriptionSendProcessor<K, VA, KO, FKE>
where
    K: Send + 'static,
    VA: Send + 'static,
    KO: Send + Clone + 'static,
    FKE: Fn(&VA) -> KO + Send + 'static,
{
    /// True if and only if `serialize(a) != serialize(b)`. The JVM compares
    /// foreign keys by their serialized bytes and not by `Eq`. Here `a` and `b`
    /// are both `Some`.
    fn fk_differs(&self, a: &KO, b: &KO) -> bool {
        self.ko_serde.serialize("", a) != self.ko_serde.serialize("", b)
    }

    /// JVM `leftJoinInstructions`.
    // mirrors the JVM signature + the cached hash
    fn left_join_instructions(
        &self,
        ctx: &mut ProcessorContext<'_, '_, KO, SubscriptionWrapper>,
        key: &K,
        had_old: bool,
        foreign_keys: (Option<KO>, Option<KO>),
        hash: Option<&[u8]>,
        ts: i64,
    ) {
        use Instruction::{DeleteKeyNoPropagate, PropagateNullIfNoFkValAvailable};

        let (old_fk, new_fk) = foreign_keys;
        if had_old {
            // Delete the OLD key's subscription when the FK changed (or vanished).
            if let Some(ofk) = old_fk
                && new_fk.as_ref().is_none_or(|nfk| self.fk_differs(nfk, &ofk))
            {
                self.forward(
                    ctx,
                    key,
                    ofk,
                    DeleteKeyNoPropagate,
                    hash.map(<[u8]>::to_vec),
                    ts,
                );
            }
            if let Some(nfk) = new_fk {
                self.forward(
                    ctx,
                    key,
                    nfk,
                    PropagateNullIfNoFkValAvailable,
                    hash.map(<[u8]>::to_vec),
                    ts,
                );
            }
        } else if let Some(nfk) = new_fk {
            self.forward(
                ctx,
                key,
                nfk,
                PropagateNullIfNoFkValAvailable,
                hash.map(<[u8]>::to_vec),
                ts,
            );
        }
    }

    /// JVM `defaultJoinInstructions`, the inner form.
    // mirrors the JVM signature + the cached hash
    fn default_join_instructions(
        &self,
        ctx: &mut ProcessorContext<'_, '_, KO, SubscriptionWrapper>,
        key: &K,
        had_old: bool,
        foreign_keys: (Option<KO>, Option<KO>),
        hash: Option<&[u8]>,
        ts: i64,
    ) {
        use Instruction::{
            DeleteKeyAndPropagate, DeleteKeyNoPropagate, PropagateNullIfNoFkValAvailable,
            PropagateOnlyIfFkValAvailable,
        };

        let (old_fk, new_fk) = foreign_keys;
        let h = || hash.map(<[u8]>::to_vec);
        if !had_old {
            if let Some(nfk) = new_fk {
                self.forward(ctx, key, nfk, PropagateOnlyIfFkValAvailable, h(), ts);
            }
            return;
        }
        match (old_fk, new_fk) {
            (None, None) => { /* both FKs null → skip */ }
            (Some(ofk), None) => {
                self.forward(ctx, key, ofk, DeleteKeyAndPropagate, h(), ts);
            }
            (Some(ofk), Some(nfk)) if self.fk_differs(&ofk, &nfk) => {
                // Different FK: delete from the old key's store, propagate null under
                // the new key (unset the previous output).
                self.forward(ctx, key, ofk, DeleteKeyNoPropagate, h(), ts);
                self.forward(ctx, key, nfk, PropagateNullIfNoFkValAvailable, h(), ts);
            }
            // First arrival of this FK (old null) or unchanged FK: propagate-only.
            (_, Some(nfk)) => {
                self.forward(ctx, key, nfk, PropagateOnlyIfFkValAvailable, h(), ts);
            }
        }
    }
}

// ── 2. SubscriptionReceive ───────────────────────────────────────────────────

/// `SubscriptionReceiveProcessorSupplier`: update the subscription store.
///
/// This processor writes to or deletes from the subscription store, keyed by
/// `combined_key(fk, pk)`. It then forwards the wrapper downstream keyed by
/// `KO`, so `SubscriptionJoin` can read the right table.
pub(crate) struct SubscriptionReceiveProcessor<KO> {
    pub store_name: String,
    pub ko_serde: Box<dyn Serde<KO>>,
    pub _pd: Marker<KO>,
}

#[async_trait]
impl<KO> Processor<KO, SubscriptionWrapper, KO, SubscriptionWrapper>
    for SubscriptionReceiveProcessor<KO>
where
    KO: Send + Sync + Clone + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, KO, SubscriptionWrapper>,
        r: Record<KO, SubscriptionWrapper>,
    ) {
        let fk = r
            .key
            .expect("FK subscription-receive requires a non-null key");
        let fk_bytes = self.ko_serde.serialize(&self.store_name, &fk);
        let w = r.value;
        let pk = w.primary_key.clone();
        let is_delete = matches!(
            w.instruction,
            Instruction::DeleteKeyAndPropagate | Instruction::DeleteKeyNoPropagate
        );
        {
            let store = ctx
                .get_fk_subscription_store(&self.store_name)
                .expect("FK subscription store not found");
            if is_delete {
                store.delete(&fk_bytes, &pk).await;
            } else {
                store.put(&fk_bytes, &pk, &w, r.timestamp).await;
            }
        }
        ctx.forward(Record::new(Some(fk), w, r.timestamp));
    }
}

// ── 3. SubscriptionJoin ──────────────────────────────────────────────────────

/// `SubscriptionJoinProcessorSupplier`: read the foreign value and respond.
///
/// This processor reads the foreign value `VB` from the right table store `sb`.
/// It builds a `SubscriptionResponseWrapper` that follows the wrapper's
/// instruction, then forwards it keyed by the primary key `K`.
pub(crate) struct SubscriptionJoinProcessor<KO, K, VB> {
    pub b_store: String,
    pub k_serde: Box<dyn Serde<K>>,
    pub vb_serde: Box<dyn Serde<VB>>,
    pub _pd: Marker<(KO, K, VB)>,
}

#[async_trait]
impl<KO, K, VB> Processor<KO, SubscriptionWrapper, K, SubscriptionResponseWrapper>
    for SubscriptionJoinProcessor<KO, K, VB>
where
    KO: Send + Sync + Clone + 'static,
    K: Send + Sync + Clone + 'static,
    VB: Send + Sync + Clone + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, K, SubscriptionResponseWrapper>,
        r: Record<KO, SubscriptionWrapper>,
    ) {
        let fk = r.key.expect("FK subscription-join requires a non-null key");
        let w = r.value;
        // Read the current foreign value VB from the right-table store.
        let fk_val: Option<VB> = match ctx.get_state_store::<KO, VB>(&self.b_store) {
            Some(s) => s.get(&fk).await,
            None => None,
        };
        let fk_val_bytes = fk_val
            .as_ref()
            .map(|v| self.vb_serde.serialize(&self.b_store, v));
        let pk = self.k_serde.deserialize(&self.b_store, &w.primary_key).ok();

        let response = match w.instruction {
            Instruction::DeleteKeyAndPropagate => Some(SubscriptionResponseWrapper {
                hash: w.hash.clone(),
                foreign_value: None,
            }),
            Instruction::PropagateNullIfNoFkValAvailable => Some(SubscriptionResponseWrapper {
                hash: w.hash.clone(),
                foreign_value: fk_val_bytes,
            }),
            Instruction::PropagateOnlyIfFkValAvailable => {
                fk_val_bytes.map(|fv| SubscriptionResponseWrapper {
                    hash: w.hash.clone(),
                    foreign_value: Some(fv),
                })
            }
            Instruction::DeleteKeyNoPropagate => None,
        };
        if let Some(resp) = response {
            ctx.forward(Record::new(pk, resp, r.timestamp));
        }
    }
}

// ── 4. ForeignTableJoin ──────────────────────────────────────────────────────

/// `ForeignTableJoinProcessorSupplier`: re-emit on a right-table change.
///
/// On a right-table `Change<VB>` for `KO`, this processor prefix-scans the
/// subscription store for every primary key subscribed to that foreign key. It
/// then re-emits a response keyed by each primary key `K`. The response carries
/// the new VB value, or null on a right tombstone.
pub(crate) struct ForeignTableJoinProcessor<KO, K, VB> {
    pub store_name: String,
    pub ko_serde: Box<dyn Serde<KO>>,
    pub k_serde: Box<dyn Serde<K>>,
    pub vb_serde: Box<dyn Serde<VB>>,
    pub _pd: Marker<(KO, K, VB)>,
}

#[async_trait]
impl<KO, K, VB> Processor<KO, Change<VB>, K, SubscriptionResponseWrapper>
    for ForeignTableJoinProcessor<KO, K, VB>
where
    KO: Send + Sync + Clone + 'static,
    K: Send + Sync + Clone + 'static,
    VB: Send + Sync + Clone + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, K, SubscriptionResponseWrapper>,
        r: Record<KO, Change<VB>>,
    ) {
        let fk = r
            .key
            .expect("FK foreign-table-join requires a non-null key");
        let fk_bytes = self.ko_serde.serialize(&self.store_name, &fk);
        // The current right value (None on a tombstone).
        let new_vb: Option<Bytes> = r
            .value
            .new
            .as_ref()
            .map(|v| self.vb_serde.serialize(&self.store_name, v));
        // Prefix-scan every subscriber of this foreign key.
        let subs: Vec<(Bytes, SubscriptionWrapper)> = {
            let store = ctx
                .get_fk_subscription_store(&self.store_name)
                .expect("FK subscription store not found");
            store.range_by_foreign(&fk_bytes).await
        };
        for (pk_bytes, w) in subs {
            let Ok(pk) = self.k_serde.deserialize(&self.store_name, &pk_bytes) else {
                continue;
            };
            ctx.forward(Record::new(
                Some(pk),
                SubscriptionResponseWrapper {
                    hash: w.hash.clone(),
                    foreign_value: new_vb.clone(),
                },
                r.timestamp,
            ));
        }
    }
}

// ── 5. SubscriptionResolver ──────────────────────────────────────────────────

/// `ResponseJoinProcessorSupplier`: resolve a response against the left value.
///
/// This processor reads the current left value `VA` from `sa`. It drops the
/// response when its hash disagrees with the current value's hash, because the
/// response is then stale. Otherwise it applies the joiner and forwards a
/// `Change<VR>`. It forwards a tombstone when the foreign value is null under
/// INNER, and under LEFT when the left value is also gone.
pub(crate) struct SubscriptionResolverProcessor<K, VA, VB, VR, J> {
    pub a_store: String,
    pub va_serde: Box<dyn Serde<VA>>,
    pub vb_serde: Box<dyn Serde<VB>>,
    pub joiner: J,
    pub is_left: bool,
    pub _pd: Marker<(K, VA, VB, VR)>,
}

#[async_trait]
impl<K, VA, VB, VR, J> Processor<K, SubscriptionResponseWrapper, K, Change<VR>>
    for SubscriptionResolverProcessor<K, VA, VB, VR, J>
where
    K: Send + Sync + Clone + 'static,
    VA: Send + Sync + Clone + 'static,
    VB: Send + Sync + Clone + 'static,
    VR: Send + Sync + Clone + 'static,
    J: Fn(&VA, Option<&VB>) -> VR + Send + Sync + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, K, Change<VR>>,
        r: Record<K, SubscriptionResponseWrapper>,
    ) {
        let key = r.key.expect("FK resolver requires a non-null key");
        let resp = r.value;
        // Current left value from `sa`.
        let current_va: Option<VA> = match ctx.get_state_store::<K, VA>(&self.a_store) {
            Some(s) => s.get(&key).await,
            None => None,
        };
        let current_hash: Option<Vec<u8>> = current_va
            .as_ref()
            .map(|v| hash128(&self.va_serde.serialize("", v)).to_vec());

        // Staleness check: drop if the message hash != the current value's hash.
        if resp.hash != current_hash {
            return;
        }

        let foreign_vb: Option<VB> = resp
            .foreign_value
            .as_ref()
            .and_then(|b| self.vb_serde.deserialize(&self.a_store, b).ok());

        let result: Option<VR> =
            if resp.foreign_value.is_none() && (!self.is_left || current_va.is_none()) {
                None // tombstone
            } else {
                // joiner(currentVA, foreignVB); currentVA is Some here for INNER (it
                // matched the hash, and a null hash only matches a null current value
                // — in which case the foreign value is also null → handled above).
                current_va
                    .as_ref()
                    .map(|va| (self.joiner)(va, foreign_vb.as_ref()))
            };

        // old is None: the FK-join output node is a non-materialized KTableSource,
        // so it never has a prior value to report (matches the JVM, which feeds the
        // resolver's raw value into a store-less KTableSource).
        ctx.forward(Record::new(
            Some(key),
            Change {
                old: None,
                new: result,
            },
            r.timestamp,
        ));
    }
}

// ── 6. FK-join OUTPUT ────────────────────────────────────────────────────────

/// `KTABLE-FK-JOIN-OUTPUT-`: the result `KTable<K, VR>` node.
///
/// The JVM uses a store-less `KTableSource`. Here the node is a passthrough that
/// forwards the resolver's `Change<VR>` unchanged, because the resolver already
/// produced the correct value or tombstone. A downstream `to_stream` or
/// materialization therefore sees a normal table change-stream.
pub(crate) struct FkJoinOutputProcessor<K, VR> {
    pub _pd: Marker<(K, VR)>,
}

#[async_trait]
impl<K, VR> Processor<K, Change<VR>, K, Change<VR>> for FkJoinOutputProcessor<K, VR>
where
    K: Send + Sync + Clone + 'static,
    VR: Send + Sync + Clone + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, K, Change<VR>>,
        r: Record<K, Change<VR>>,
    ) {
        ctx.forward(r);
    }
}

// ── Test-only change collector ───────────────────────────────────────────────

/// Shared buffer of collected `(key, new-value)` pairs. A tombstone becomes
/// `None`.
#[cfg(test)]
pub(crate) type ChangeBuffer<K, V> = std::sync::Arc<std::sync::Mutex<Vec<(Option<K>, Option<V>)>>>;

/// Test-only terminal processor that records the change-stream.
///
/// It records each `Change<V>`'s key and **new** value into a shared buffer in
/// arrival order, and it forwards nothing. A tombstone becomes `None`. It backs
/// [`KTable::collect_changes`], which the FK-join exec tests use to observe a
/// table's full change-stream, `None` tombstones included.
///
/// [`KTable::collect_changes`]: crate::dsl::ktable::KTable::collect_changes
#[cfg(test)]
pub(crate) struct ChangeCollectorProcessor<K, V> {
    pub buf: ChangeBuffer<K, V>,
    pub _pd: Marker<(K, V)>,
}

#[cfg(test)]
#[async_trait]
impl<K, V> Processor<K, Change<V>, K, V> for ChangeCollectorProcessor<K, V>
where
    K: Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
    async fn process(
        &mut self,
        _ctx: &mut ProcessorContext<'_, '_, K, V>,
        r: Record<K, Change<V>>,
    ) {
        self.buf.lock().unwrap().push((r.key, r.value.new));
    }
}
