//! Stateless `Processor` impls backing the `KStream` DSL ops.
//!
//! Each struct captures the user closure and implements [`Processor`] with the
//! input and output KV types of its op. The DSL lowering thunk constructs one
//! struct inside a `ProcessorSupplier` closure,
//! `move || Proc { f: f.clone(), .. }`. The structs themselves therefore need no
//! `Clone`, and only the captured closure needs it.
//!
//! The bounds match what the Processor-API needs. Every *output* key and value
//! type is `Any + Send + Clone`, so [`ProcessorContext::forward`] can box it and
//! fan it out. Every captured closure is `Fn(..) + Send + Sync + 'static`, so the
//! enclosing `move ||` supplier satisfies `ProcessorSupplier: Send + Sync`.

use std::{any::Any, marker::PhantomData};

use async_trait::async_trait;

use crate::processor::{
    api::{Processor, ProcessorContext},
    record::Record,
};

/// Variance-neutral marker that "uses" the otherwise-unconstrained type params of
/// a processor struct. It is always `Send + Sync` and free of contravariance.
/// It is factored out, so the multi-param markers stay under clippy's
/// `type_complexity` threshold.
type Marker<T> = PhantomData<fn() -> T>;

/// `map_values`: rewrite each value, key unchanged. `Processor<K, V, K, V2>`.
pub(crate) struct MapValuesProcessor<V, V2, F> {
    pub f: F,
    pub _pd: std::marker::PhantomData<fn(V) -> V2>,
}
#[async_trait]
impl<K, V, V2, F> Processor<K, V, K, V2> for MapValuesProcessor<V, V2, F>
where
    K: Any + Send + Clone,
    V: Send + 'static,
    V2: Any + Send + Clone,
    F: Fn(&V) -> V2 + Send + Sync + 'static,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V2>, r: Record<K, V>) {
        ctx.forward(Record::new(r.key, (self.f)(&r.value), r.timestamp));
    }
}

/// `filter` / `filter_not`: forward when `predicate(k, v) != negate`.
/// `Processor<K, V, K, V>`. A null key reaches the predicate as the type's
/// `Default`, so the predicate signature stays `Fn(&K, &V)`. The JVM passes a
/// nullable key. This crate has no key to lend, so it synthesizes one, and a
/// filter rarely keys on identity.
pub(crate) struct FilterProcessor<K, V, P> {
    pub predicate: P,
    pub negate: bool,
    pub _pd: std::marker::PhantomData<fn(K, V)>,
}
#[async_trait]
impl<K, V, P> Processor<K, V, K, V> for FilterProcessor<K, V, P>
where
    K: Any + Send + Clone + Default,
    V: Any + Send + Clone,
    P: Fn(&K, &V) -> bool + Send + Sync + 'static,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V>, r: Record<K, V>) {
        let key = r.key.clone().unwrap_or_default();
        if (self.predicate)(&key, &r.value) != self.negate {
            ctx.forward(r);
        }
    }
}

/// `map`: rewrite both key and value. `Processor<K, V, K2, V2>`.
pub(crate) struct MapProcessor<K, V, K2, V2, F> {
    pub f: F,
    pub _pd: Marker<(K, V, K2, V2)>,
}
#[async_trait]
impl<K, V, K2, V2, F> Processor<K, V, K2, V2> for MapProcessor<K, V, K2, V2, F>
where
    K: Default + Send + 'static,
    V: Send + 'static,
    K2: Any + Send + Clone,
    V2: Any + Send + Clone,
    F: Fn(&K, &V) -> (K2, V2) + Send + Sync + 'static,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K2, V2>, r: Record<K, V>) {
        let key = r.key.unwrap_or_default();
        let (k2, v2) = (self.f)(&key, &r.value);
        ctx.forward(Record::new(Some(k2), v2, r.timestamp));
    }
}

/// `select_key`: rewrite the key, value unchanged. `Processor<K, V, K2, V>`.
pub(crate) struct SelectKeyProcessor<K, V, K2, F> {
    pub f: F,
    pub _pd: std::marker::PhantomData<fn(K, V) -> K2>,
}
#[async_trait]
impl<K, V, K2, F> Processor<K, V, K2, V> for SelectKeyProcessor<K, V, K2, F>
where
    K: Default + Send + 'static,
    V: Any + Send + Clone,
    K2: Any + Send + Clone,
    F: Fn(&K, &V) -> K2 + Send + Sync + 'static,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K2, V>, r: Record<K, V>) {
        let key = r.key.unwrap_or_default();
        let k2 = (self.f)(&key, &r.value);
        ctx.forward(Record::new(Some(k2), r.value, r.timestamp));
    }
}

/// `flat_map`: one record into zero or more `(K2, V2)`.
/// `Processor<K, V, K2, V2>`.
pub(crate) struct FlatMapProcessor<K, V, K2, V2, IT, F> {
    pub f: F,
    pub _pd: Marker<(K, V, K2, V2, IT)>,
}
#[async_trait]
impl<K, V, K2, V2, IT, F> Processor<K, V, K2, V2> for FlatMapProcessor<K, V, K2, V2, IT, F>
where
    K: Default + Send + 'static,
    V: Send + 'static,
    K2: Any + Send + Clone,
    V2: Any + Send + Clone,
    IT: IntoIterator<Item = (K2, V2)> + 'static,
    F: Fn(&K, &V) -> IT + Send + Sync + 'static,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K2, V2>, r: Record<K, V>) {
        let key = r.key.unwrap_or_default();
        for (k2, v2) in (self.f)(&key, &r.value) {
            ctx.forward(Record::new(Some(k2), v2, r.timestamp));
        }
    }
}

/// `flat_map_values`: one record into zero or more `V2`, key unchanged.
/// `Processor<K, V, K, V2>`.
pub(crate) struct FlatMapValuesProcessor<V, V2, IT, F> {
    pub f: F,
    pub _pd: std::marker::PhantomData<fn(V) -> (V2, IT)>,
}
#[async_trait]
impl<K, V, V2, IT, F> Processor<K, V, K, V2> for FlatMapValuesProcessor<V, V2, IT, F>
where
    K: Any + Send + Clone,
    V: Send + 'static,
    V2: Any + Send + Clone,
    IT: IntoIterator<Item = V2> + 'static,
    F: Fn(&V) -> IT + Send + Sync + 'static,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V2>, r: Record<K, V>) {
        for v2 in (self.f)(&r.value) {
            ctx.forward(Record::new(r.key.clone(), v2, r.timestamp));
        }
    }
}

/// `peek`: side-effect on each record, then forward unchanged.
/// `Processor<K, V, K, V>`.
pub(crate) struct PeekProcessor<K, V, F> {
    pub f: F,
    pub _pd: std::marker::PhantomData<fn(K, V)>,
}
#[async_trait]
impl<K, V, F> Processor<K, V, K, V> for PeekProcessor<K, V, F>
where
    K: Any + Send + Clone + Default,
    V: Any + Send + Clone,
    F: Fn(&K, &V) + Send + Sync + 'static,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V>, r: Record<K, V>) {
        let key = r.key.clone().unwrap_or_default();
        (self.f)(&key, &r.value);
        ctx.forward(r);
    }
}

/// `foreach`: a terminal side-effect that never forwards. It is typed
/// `Processor<K, V, K, V>`, because reuse of the input KV as the unused output
/// avoids a unit-typed handle. The node has no children, so it forwards nothing
/// in any case.
pub(crate) struct ForeachProcessor<K, V, F> {
    pub f: F,
    pub _pd: std::marker::PhantomData<fn(K, V)>,
}
#[async_trait]
impl<K, V, F> Processor<K, V, K, V> for ForeachProcessor<K, V, F>
where
    K: Any + Send + Clone + Default,
    V: Any + Send + Clone,
    F: Fn(&K, &V) + Send + Sync + 'static,
{
    async fn process(&mut self, _ctx: &mut ProcessorContext<'_, '_, K, V>, r: Record<K, V>) {
        let key = r.key.unwrap_or_default();
        (self.f)(&key, &r.value);
    }
}

/// `merge`: forward each record unchanged. Attached with both parents.
/// `Processor<K, V, K, V>`.
pub(crate) struct MergeProcessor<K, V> {
    pub _pd: std::marker::PhantomData<fn(K, V)>,
}
#[async_trait]
impl<K, V> Processor<K, V, K, V> for MergeProcessor<K, V>
where
    K: Any + Send + Clone,
    V: Any + Send + Clone,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V>, r: Record<K, V>) {
        ctx.forward(r);
    }
}
