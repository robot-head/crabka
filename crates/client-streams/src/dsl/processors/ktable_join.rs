//! KTable–KTable join processors and the `result` rule.
//!
//! Two processors work in tandem: `KTableKTableJoinThisProcessor` is placed on
//! the "this" (left) `KTable` side, and `KTableKTableJoinOtherProcessor` on the
//! "other" (right) side. Each reads the *other* side's current value from a
//! connected state store and applies the join.
//!
//! The `result` function encodes whether a `(a, b)` pair produces a join row
//! (`Some(VR)`) or a tombstone (`None`) for the three join kinds:
//!
//! - **inner**: both `a` and `b` must be present.
//! - **left**: `a` must be present; `b` is optional.
//! - **outer**: at least one of `a` or `b` must be present.

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::dsl::processors::change::Change;
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

/// Variance-neutral marker for multi-param processor structs.
type Marker<T> = PhantomData<fn() -> T>;

/// Which sides are required for the join row to exist.
#[derive(Clone, Copy)]
pub(crate) struct JoinKind {
    pub a_required: bool,
    pub b_required: bool,
}

impl JoinKind {
    pub fn inner() -> Self {
        Self {
            a_required: true,
            b_required: true,
        }
    }
    pub fn left() -> Self {
        Self {
            a_required: true,
            b_required: false,
        }
    }
    pub fn outer() -> Self {
        Self {
            a_required: false,
            b_required: false,
        }
    }
}

/// Returns `Some(VR)` iff the join row exists for `(a, b)` under `kind`; else
/// `None` (tombstone).
///
/// Rules:
/// - If `kind.a_required` and `a` is absent → `None`.
/// - If `kind.b_required` and `b` is absent → `None`.
/// - If both `a` and `b` are absent → `None` (outer still needs at least one).
pub(crate) fn result<VA, VB, VR, F>(
    kind: JoinKind,
    joiner: &F,
    a: Option<&VA>,
    b: Option<&VB>,
) -> Option<VR>
where
    F: Fn(Option<&VA>, Option<&VB>) -> VR,
{
    let present = (a.is_some() || !kind.a_required)
        && (b.is_some() || !kind.b_required)
        && (a.is_some() || b.is_some());
    if present { Some(joiner(a, b)) } else { None }
}

/// Processor placed on the **"this"** (left/`VA`) side of a KTable–KTable join.
///
/// Reads `b_cur` from the connected `other_store` (keyed by `K`, holding `VB`)
/// and forwards a `Change<VR>` for every input `Change<VA>`.
pub(crate) struct KTableKTableJoinThisProcessor<K, VA, VB, VR, F> {
    pub other_store: String,
    pub joiner: F,
    pub kind: JoinKind,
    pub _pd: Marker<(K, VA, VB, VR)>,
}

#[async_trait]
impl<K, VA, VB, VR, F> Processor<K, Change<VA>, K, Change<VR>>
    for KTableKTableJoinThisProcessor<K, VA, VB, VR, F>
where
    K: std::any::Any + Send + Sync + Clone,
    VA: Send + 'static,
    VB: Send + 'static,
    VR: std::any::Any + Send + Clone,
    F: Fn(Option<&VA>, Option<&VB>) -> VR + Send + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, K, Change<VR>>,
        r: Record<K, Change<VA>>,
    ) {
        let key = r.key.expect("join key");
        // `and_then(|s| s.get(..))` can't `.await`; do the lookup in a `match`,
        // dropping the store borrow before `ctx.forward`.
        let b_cur = match ctx.get_state_store::<K, VB>(&self.other_store) {
            Some(s) => s.get(&key).await,
            None => None,
        };
        let old = result(
            self.kind,
            &self.joiner,
            r.value.old.as_ref(),
            b_cur.as_ref(),
        );
        let new = result(
            self.kind,
            &self.joiner,
            r.value.new.as_ref(),
            b_cur.as_ref(),
        );
        if old.is_some() || new.is_some() {
            ctx.forward(Record::new(Some(key), Change { old, new }, r.timestamp));
        }
    }
}

/// Processor placed on the **"other"** (right/`VB`) side of a KTable–KTable join.
///
/// Reads `a_cur` from the connected `other_store` (keyed by `K`, holding `VA`)
/// and forwards a `Change<VR>` for every input `Change<VB>`.
pub(crate) struct KTableKTableJoinOtherProcessor<K, VA, VB, VR, F> {
    pub other_store: String,
    pub joiner: F,
    pub kind: JoinKind,
    pub _pd: Marker<(K, VA, VB, VR)>,
}

#[async_trait]
impl<K, VA, VB, VR, F> Processor<K, Change<VB>, K, Change<VR>>
    for KTableKTableJoinOtherProcessor<K, VA, VB, VR, F>
where
    K: std::any::Any + Send + Sync + Clone,
    VA: Send + 'static,
    VB: Send + 'static,
    VR: std::any::Any + Send + Clone,
    F: Fn(Option<&VA>, Option<&VB>) -> VR + Send + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, K, Change<VR>>,
        r: Record<K, Change<VB>>,
    ) {
        let key = r.key.expect("join key");
        // `and_then(|s| s.get(..))` can't `.await`; do the lookup in a `match`,
        // dropping the store borrow before `ctx.forward`.
        let a_cur = match ctx.get_state_store::<K, VA>(&self.other_store) {
            Some(s) => s.get(&key).await,
            None => None,
        };
        let old = result(
            self.kind,
            &self.joiner,
            a_cur.as_ref(),
            r.value.old.as_ref(),
        );
        let new = result(
            self.kind,
            &self.joiner,
            a_cur.as_ref(),
            r.value.new.as_ref(),
        );
        if old.is_some() || new.is_some() {
            ctx.forward(Record::new(Some(key), Change { old, new }, r.timestamp));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::marker::PhantomData;

    use assert2::check;

    use super::*;
    use crate::dsl::processors::change::Change;
    use crate::processor::api::ProcessorContext;
    use crate::processor::erased::{Dispatch, ErasedRecord};
    use crate::processor::record::RecordContext;
    use crate::processor::serde::StringSerde;
    use crate::store::api::KeyValueStore;
    use crate::store::kv::KeyValueBytesStore;
    use crate::store::registry::StoreRegistry;

    // ── helpers ──────────────────────────────────────────────────────────────

    type StrJoiner = fn(Option<&String>, Option<&String>) -> String;
    type TestJoinProc = KTableKTableJoinThisProcessor<String, String, String, String, StrJoiner>;

    async fn make_stores_with_b(b_val: Option<(&str, &str)>) -> StoreRegistry {
        let mut stores = StoreRegistry::default();
        let mut store = KeyValueBytesStore::<String, String>::in_memory(
            "b".into(),
            Box::new(StringSerde),
            Box::new(StringSerde),
            "b-cl".into(),
        );
        if let Some((k, v)) = b_val {
            store.put(k.to_string(), v.to_string()).await;
        }
        stores.insert(Box::new(store));
        stores
    }

    fn concat_joiner(a: Option<&String>, b: Option<&String>) -> String {
        format!(
            "{}{}",
            a.cloned().unwrap_or_default(),
            b.cloned().unwrap_or_default()
        )
    }

    fn make_proc() -> TestJoinProc {
        KTableKTableJoinThisProcessor {
            other_store: "b".into(),
            joiner: concat_joiner as StrJoiner,
            kind: JoinKind::inner(),
            _pd: PhantomData,
        }
    }

    fn rc() -> RecordContext {
        RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        }
    }

    // ── processor tests ───────────────────────────────────────────────────────

    /// Inner join update: store `b["k"] = "B"`, process an insert of `"A"`.
    /// Should forward `Change { old: None, new: Some("AB") }`.
    #[tokio::test]
    async fn inner_join_update_forwards_ab() {
        let mut stores = make_stores_with_b(Some(("k", "B"))).await;
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();

        let mut proc = make_proc();

        {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
                globals: &globals,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<String>>::new(&mut dispatch);
            proc.process(
                &mut ctx,
                Record::new(Some("k".into()), Change::update(None, "A".into()), 0),
            )
            .await;
        }

        let (_, rec) = buffer.pop_front().expect("expected forwarded record");
        let change = rec.value.downcast::<Change<String>>().unwrap();
        check!(change.old.is_none());
        check!(change.new == Some("AB".to_string()));
        check!(buffer.is_empty());
    }

    /// Inner join tombstone: store `b["k"] = "B"`, process a delete of `"A"`.
    /// Should forward `Change { old: Some("AB"), new: None }`.
    #[tokio::test]
    async fn inner_join_tombstone_forwards_old_ab_new_none() {
        let mut stores = make_stores_with_b(Some(("k", "B"))).await;
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();

        let mut proc = make_proc();

        {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
                globals: &globals,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<String>>::new(&mut dispatch);
            proc.process(
                &mut ctx,
                Record::new(Some("k".into()), Change::tombstone(Some("A".into())), 1),
            )
            .await;
        }

        let (_, rec) = buffer.pop_front().expect("expected forwarded record");
        let change = rec.value.downcast::<Change<String>>().unwrap();
        check!(change.old == Some("AB".to_string()));
        check!(change.new.is_none());
        check!(buffer.is_empty());
    }

    /// Inner join with empty store (no `"k"`): process an insert of `"A"`.
    /// Should NOT forward — inner requires both sides present.
    #[tokio::test]
    async fn inner_join_empty_store_no_forward() {
        let mut stores = make_stores_with_b(None).await;
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();

        let mut proc = make_proc();

        {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
                globals: &globals,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<String>>::new(&mut dispatch);
            proc.process(
                &mut ctx,
                Record::new(Some("k".into()), Change::update(None, "A".into()), 0),
            )
            .await;
        }

        check!(
            buffer.is_empty(),
            "inner join with absent b must not forward"
        );
    }

    // ── result() rule tests ────────────────────────────────────────────────────

    fn joiner(a: Option<&String>, b: Option<&String>) -> String {
        format!(
            "{}{}",
            a.cloned().unwrap_or_default(),
            b.cloned().unwrap_or_default()
        )
    }

    fn a() -> String {
        "A".into()
    }
    fn b() -> String {
        "B".into()
    }

    #[test]
    fn result_inner() {
        let k = JoinKind::inner();
        // both present → Some
        check!(result(k, &joiner, Some(&a()), Some(&b())) == Some("AB".into()));
        // a absent → None
        check!(result(k, &joiner, None, Some(&b())).is_none());
        // b absent → None
        check!(result(k, &joiner, Some(&a()), None).is_none());
        // both absent → None
        check!(result(k, &joiner, None::<&String>, None::<&String>).is_none());
    }

    #[test]
    fn result_left() {
        let k = JoinKind::left();
        // both present → Some("AB")
        check!(result(k, &joiner, Some(&a()), Some(&b())) == Some("AB".into()));
        // a present, b absent → Some("A")
        check!(result(k, &joiner, Some(&a()), None::<&String>) == Some("A".into()));
        // a absent, b present → None (a required)
        check!(result(k, &joiner, None::<&String>, Some(&b())).is_none());
        // both absent → None
        check!(result(k, &joiner, None::<&String>, None::<&String>).is_none());
    }

    #[test]
    fn result_outer() {
        let k = JoinKind::outer();
        // both present → Some("AB")
        check!(result(k, &joiner, Some(&a()), Some(&b())) == Some("AB".into()));
        // only a → Some("A")
        check!(result(k, &joiner, Some(&a()), None::<&String>) == Some("A".into()));
        // only b → Some("B")
        check!(result(k, &joiner, None::<&String>, Some(&b())) == Some("B".into()));
        // both absent → None
        check!(result(k, &joiner, None::<&String>, None::<&String>).is_none());
    }
}
