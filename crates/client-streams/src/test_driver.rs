//! Synchronous, broker-free driver for testing topologies.
//!
//! This driver is the analog of the JVM `TopologyTestDriver`. Pipe a typed
//! input record, then read typed output. The driver loops a record back into
//! the graph when the record goes to an internal repartition topic that is also
//! a source.

use std::collections::{HashMap, HashSet, VecDeque};

use crabka_units::prelude::*;

use crate::{
    processor::{
        erased::{OutputRecord, ProcessorError},
        graph::Graph,
        serde::{Consumed, Produced, Serde, SerdeAssociate},
    },
    topology::BuiltTopology,
};

/// A pending record entry in the repartition loop-back queue.
type PendingRecord = (String, Option<Vec<u8>>, Vec<u8>, i64);

/// Synchronous test driver.
///
/// This driver builds a runnable processor graph from a [`BuiltTopology`]. It
/// exposes `pipe_input` and `read_output`, so a test can exercise processing
/// logic without a real broker.
pub struct TopologyTestDriver {
    graph: Graph,
    source_topics: HashSet<String>,
    output: HashMap<String, VecDeque<OutputRecord>>,
    /// Mock wall clock in ms. `advance_wall_clock_time` advances it, and the
    /// driver passes it to `Graph::punctuate_wall_clock`. It starts at `0`, the
    /// same as the JVM `TopologyTestDriver` mock time at construction.
    mock_wall_ms: i64,
    /// Changelog records the stores produced, collected in order as
    /// `(changelog_topic, key, value, record_ts)`. The driver has no broker, so
    /// it keeps the drained changelog here instead of discarding it. A test can
    /// then assert byte-exact changelog records with `drain_changelog`.
    changelog_captured: Vec<(String, bytes::Bytes, Option<bytes::Bytes>, Option<i64>)>,
}

impl TopologyTestDriver {
    /// Instantiates the topology's graph for testing. This method returns an
    /// error if the topology is invalid. It propagates `instantiate`'s error.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn new(built: &BuiltTopology) -> Result<Self, ProcessorError> {
        let source_topics: HashSet<String> = built.list_source_topics().into_iter().collect();
        let backend = crate::store::backend::StoreBackend::InMemory;
        // The JVM `TopologyTestDriver` defaults `statestore.cache.max.bytes` to 0
        // (caching disabled), so stores emit on every update and goldens stay
        // deterministic. Match that here.
        let mut graph = pollster::block_on(built.instantiate(&backend, "app", ByteSize::ZERO))?;
        // The TopologyTestDriver builds the SHARED, fully-replicated global stores
        // into a `GlobalStateManager` and lends it to the graph's dispatch — the
        // same shared manager the real app runtime uses (the global consumer that
        // populates it lives in the app wiring; `BuiltTopology::instantiate`
        // deliberately omits global stores from the per-task registry, since a
        // global store is fully replicated, not task-partitioned). Tests populate
        // it directly via `pipe_global`, and stream-globaltable joins read it.
        graph.globals = pollster::block_on(crate::runtime::global::GlobalStateManager::build(
            built.global_store_factories(),
            built.global_store_topics(),
            &backend,
            "app",
        ));
        pollster::block_on(graph.init_processors())?;
        Ok(Self {
            graph,
            source_topics,
            output: HashMap::new(),
            mock_wall_ms: 0,
            changelog_captured: Vec::new(),
        })
    }

    /// Serializes and pipes one record on `topic`, then loops repartition
    /// outputs back.
    ///
    /// `consumed` is the same `Consumed::with(key_serde, value_serde)` pair the
    /// source for `topic` reads with.
    pub fn pipe_input<KS, VS>(
        &mut self,
        topic: &str,
        consumed: impl Into<Consumed<KS, VS>>,
        key: Option<KS::Target>,
        value: VS::Target,
        timestamp: i64,
    ) where
        KS: SerdeAssociate + Serde<KS::Target>,
        VS: SerdeAssociate + Serde<VS::Target>,
    {
        let consumed = consumed.into();
        let kb = key.as_ref().map(|k| consumed.key_serde.serialize(topic, k));
        let vb = consumed.value_serde.serialize(topic, &value);
        drop(key);
        drop(value);
        self.pipe_bytes(topic, kb.as_deref(), &vb, timestamp);
    }

    fn pipe_bytes(&mut self, topic: &str, key: Option<&[u8]>, value: &[u8], timestamp: i64) {
        let mut queue: VecDeque<PendingRecord> = VecDeque::from([(
            topic.to_string(),
            key.map(<[u8]>::to_vec),
            value.to_vec(),
            timestamp,
        )]);
        while let Some((t, k, v, ts)) = queue.pop_front() {
            // run the graph for this topic; ignore unknown topics
            let _ = pollster::block_on(self.graph.pipe(&t, k.as_deref(), &v, ts));
            self.route_outputs(&mut queue);
            // Stream-time advanced by this record → fire stream-time punctuators; their
            // forwarded records route like any output (and may loop back to a source).
            let _ = pollster::block_on(self.graph.punctuate_stream_time(self.graph.stream_time));
            self.route_outputs(&mut queue);
        }
    }

    /// Advances the mock wall clock by `by` and fires wall-clock punctuators.
    ///
    /// Each punctuator fires at most once, with the new clock as its value.
    /// This mirrors the JVM `TopologyTestDriver.advanceWallClockTime`.
    pub fn advance_wall_clock_time(&mut self, by: std::time::Duration) {
        self.mock_wall_ms += i64::try_from(by.as_millis()).unwrap_or(i64::MAX);
        let mut queue: VecDeque<PendingRecord> = VecDeque::new();
        let _ = pollster::block_on(self.graph.punctuate_wall_clock(self.mock_wall_ms));
        self.route_outputs(&mut queue);
        // Drain any loopback the punctuators produced (and fire stream-time for those).
        while let Some((t, k, v, ts)) = queue.pop_front() {
            let _ = pollster::block_on(self.graph.pipe(&t, k.as_deref(), &v, ts));
            self.route_outputs(&mut queue);
            let _ = pollster::block_on(self.graph.punctuate_stream_time(self.graph.stream_time));
            self.route_outputs(&mut queue);
        }
    }

    /// Drains the graph's output buffer.
    ///
    /// Outputs to a source topic go back on the queue as a repartition
    /// loop-back. All other outputs append to `self.output`. This method also
    /// drains the changelog buffers and keeps them in `changelog_captured`. The
    /// test driver has no broker, so restore does nothing with fresh stores,
    /// but a test can assert the changelog bytes with `drain_changelog`. Both
    /// the record-processing phase and the stream-time-punctuation phase of
    /// `pipe_bytes` call this method.
    fn route_outputs(&mut self, queue: &mut VecDeque<PendingRecord>) {
        for out in self.graph.take_output() {
            if self.source_topics.contains(&out.topic) {
                // internal repartition topic feeding another subtopology → loop back
                let vv = out.value.clone().unwrap_or_default().to_vec();
                queue.push_back((
                    out.topic.clone(),
                    out.key.as_ref().map(|b| b.to_vec()),
                    vv,
                    out.timestamp,
                ));
            } else {
                self.output
                    .entry(out.topic.clone())
                    .or_default()
                    .push_back(out);
            }
        }
        // Drain changelog buffers so they don't grow unbounded; the test driver
        // has no broker, so instead of re-producing them we RETAIN them for
        // byte-exact changelog assertions (`drain_changelog`). No reuse-source
        // suppression — the driver never re-produces, so no write-back loop.
        self.changelog_captured.extend(
            self.graph
                .drain_changelogs(&std::collections::HashSet::new()),
        );
    }

    /// Inspects a state store's contents after piping.
    ///
    /// This method mirrors the JVM `TopologyTestDriver.getKeyValueStore`. It is
    /// test-only. Do not use it for interactive queries.
    pub fn get_key_value_store<K: Send + Sync + 'static, V: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::api::KeyValueStore<K, V>> {
        self.graph.stores.get_kv::<K, V>(name)
    }

    /// Test-only synchronous store read that calls `block_on` on the async
    /// store.
    ///
    /// The store API is async. This method drives one typed `get` to
    /// completion, so sync `#[test]` assertions can inspect materialized state.
    pub fn store_get<K: Send + Sync + 'static, V: Send + 'static>(
        &mut self,
        store: &str,
        key: &K,
    ) -> Option<V> {
        let s = self.graph.stores.get_kv::<K, V>(store)?;
        pollster::block_on(s.get(key))
    }

    /// Latest live value of a key in a versioned store. This is a test helper.
    pub fn store_get_versioned<K: Send + Sync + 'static, V: Send + 'static>(
        &mut self,
        store_name: &str,
        key: &K,
    ) -> Option<V> {
        let store = self.graph.stores.get_versioned::<K, V>(store_name)?;
        pollster::block_on(store.get(key)).map(|r| r.value)
    }

    /// Drains the changelog records the stores have produced so far. This is a
    /// test helper.
    ///
    /// The records come back as `(changelog_topic, key, value, record_ts)` in
    /// production order. A test can then assert byte-exact changelog records,
    /// for example the KIP-889 versioned changelog, which holds bare value
    /// bytes and the version timestamp in the record-timestamp field.
    pub fn drain_changelog(
        &mut self,
    ) -> Vec<(String, bytes::Bytes, Option<bytes::Bytes>, Option<i64>)> {
        std::mem::take(&mut self.changelog_captured)
    }

    /// Populates a GLOBAL store directly. This method is test-only.
    ///
    /// In a real app the global consumer reads all partitions of the source
    /// topic and applies them to the fully-replicated global store. The test
    /// driver instead injects values straight into the shared global store
    /// manager, so a stream-globaltable join can look them up.
    pub fn pipe_global<K, V>(&mut self, store_name: &str, key: K, value: V)
    where
        K: Send + Sync + 'static,
        V: Send + 'static,
    {
        pollster::block_on(self.graph.globals.put(store_name, key, value));
    }

    // ── Interactive-query reads ─────────────────────────────────────────────
    //
    // These read the driver's local stores through the *byte* IQ layer
    // (`StoreRegistry::iq_get` → `&dyn IqQueryable`), then deserialize — the same
    // path the supervisor serves `KafkaStreams::*_store` queries from. They
    // exercise the full DSL → store → IQ-byte-read round-trip, so a Rust IQ test
    // can assert read semantics without standing up the async supervisor.

    /// Interactive-query KV read: the value for `key`, else `None`. The result
    /// is `None` if the store is absent or is not a key-value store.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn iq_kv_get<K: 'static, V: 'static>(
        &self,
        store: &str,
        key: &K,
        ks: &dyn Serde<K>,
        vs: &dyn Serde<V>,
    ) -> Option<V> {
        let q = self.graph.stores.iq_get(store)?;
        let kb = ks.serialize(store, key);
        let vb = q.iq_kv_get(&kb).await?;
        Some(vs.deserialize(store, &vb).expect("iq deserialize"))
    }

    /// Interactive-query KV range read over the inclusive `[lo, hi]` key span,
    /// in store (memcmp) order. The result is empty if the store is absent or
    /// is not a KV store.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn iq_kv_range<K: 'static, V: 'static>(
        &self,
        store: &str,
        lo: &K,
        hi: &K,
        ks: &dyn Serde<K>,
        vs: &dyn Serde<V>,
    ) -> Vec<(K, V)> {
        let Some(q) = self.graph.stores.iq_get(store) else {
            return Vec::new();
        };
        let lob = ks.serialize(store, lo);
        let hib = ks.serialize(store, hi);
        q.iq_kv_range(&lob, &hib)
            .await
            .into_iter()
            .map(|(k, v)| {
                (
                    ks.deserialize(store, &k).expect("iq deserialize"),
                    vs.deserialize(store, &v).expect("iq deserialize"),
                )
            })
            .collect()
    }

    /// Interactive-query read of every entry in a KV store, in store order.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn iq_kv_all<K: 'static, V: 'static>(
        &self,
        store: &str,
        ks: &dyn Serde<K>,
        vs: &dyn Serde<V>,
    ) -> Vec<(K, V)> {
        let Some(q) = self.graph.stores.iq_get(store) else {
            return Vec::new();
        };
        q.iq_kv_all()
            .await
            .into_iter()
            .map(|(k, v)| {
                (
                    ks.deserialize(store, &k).expect("iq deserialize"),
                    vs.deserialize(store, &v).expect("iq deserialize"),
                )
            })
            .collect()
    }

    /// Interactive-query approximate entry count for a KV store. The result is
    /// `0` if the store is absent.
    pub async fn iq_kv_count(&self, store: &str) -> u64 {
        match self.graph.stores.iq_get(store) {
            Some(q) => q.iq_kv_approx_count().await,
            None => 0,
        }
    }

    /// Interactive-query window read: `(windowStart, value)` for every window
    /// of `key` whose start is in the inclusive `[from, to]` span, ascending by
    /// window start. The result is empty if the store is absent or is not a
    /// window store.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn iq_window_fetch<K: 'static, V: 'static>(
        &self,
        store: &str,
        key: &K,
        from: i64,
        to: i64,
        ks: &dyn Serde<K>,
        vs: &dyn Serde<V>,
    ) -> Vec<(i64, V)> {
        let Some(q) = self.graph.stores.iq_get(store) else {
            return Vec::new();
        };
        let kb = ks.serialize(store, key);
        q.iq_window_fetch(&kb, from, to)
            .await
            .into_iter()
            .map(|(start, v)| (start, vs.deserialize(store, &v).expect("iq deserialize")))
            .collect()
    }

    /// Interactive-query single-window read: value of the window of `key` that
    /// starts exactly at `window_start`, else `None`.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn iq_window_fetch_single<K: 'static, V: 'static>(
        &self,
        store: &str,
        key: &K,
        window_start: i64,
        ks: &dyn Serde<K>,
        vs: &dyn Serde<V>,
    ) -> Option<V> {
        let q = self.graph.stores.iq_get(store)?;
        let kb = ks.serialize(store, key);
        let vb = q.iq_window_fetch_single(&kb, window_start).await?;
        Some(vs.deserialize(store, &vb).expect("iq deserialize"))
    }

    /// Runs an `IQv2` query against the single test graph, partition 0.
    ///
    /// `Position` is empty, because the driver does not track source offsets.
    /// Use unit tests on `Position::dominates` for bound behavior.
    pub async fn query<Q: crate::runtime::iqv2::Query>(
        &self,
        req: crate::runtime::iqv2::StateQuery<Q>,
    ) -> crate::runtime::iqv2::StateQueryResult<Q::Result> {
        use std::collections::BTreeMap;

        use crate::runtime::iqv2::{
            request::Position,
            result::{FailureReason, QueryResult, StateQueryResult},
        };

        let store_name = req.store.clone();
        let kind = req.query.store_kind();
        let Some(store) = self.graph.stores.iq_get(&store_name) else {
            // The driver has full single-graph knowledge, so an unknown store
            // name is genuinely `DoesNotExist`. (The distributed runtime cannot
            // distinguish absent-from-topology vs not-on-this-partition, so
            // `serve_iq2` returns an empty partition map instead — see §9.)
            let mut m = BTreeMap::new();
            m.insert(
                0,
                QueryResult::Failure {
                    reason: FailureReason::DoesNotExist,
                    message: format!("no state store named {store_name:?}"),
                },
            );
            return StateQueryResult::new(m);
        };
        if store.kind() != kind {
            // Mirror `serve_iq2`: a name that resolves to a different store kind
            // is reported `NotPresent`, not `DoesNotExist`.
            let mut m = BTreeMap::new();
            m.insert(
                0,
                QueryResult::Failure {
                    reason: FailureReason::NotPresent,
                    message: "wrong store kind".into(),
                },
            );
            return StateQueryResult::new(m);
        }
        let lowered = req.query.lower();
        let qr = match store.iq2_execute(&lowered).await {
            Ok(boxed) => match boxed.downcast::<Q::Result>() {
                Ok(r) => QueryResult::Success {
                    result: *r,
                    position: Position::default(),
                },
                Err(_) => QueryResult::Failure {
                    reason: FailureReason::StoreException,
                    message: "IQv2 result type mismatch".into(),
                },
            },
            Err(crate::store::iq::Iq2Failure::UnknownQueryType) => QueryResult::Failure {
                reason: FailureReason::UnknownQueryType,
                message: "unknown query type".into(),
            },
            Err(crate::store::iq::Iq2Failure::KeyTypeMismatch) => QueryResult::Failure {
                reason: FailureReason::StoreException,
                message: "key type mismatch".into(),
            },
        };
        let mut m = BTreeMap::new();
        m.insert(0, qr);
        StateQueryResult::new(m)
    }

    /// Interactive-query session read: `((start, end), value)` for every
    /// session of `key`, in store order. The result is empty if the store is
    /// absent or is not a session store.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn iq_session_fetch<K: 'static, V: 'static>(
        &self,
        store: &str,
        key: &K,
        ks: &dyn Serde<K>,
        vs: &dyn Serde<V>,
    ) -> Vec<((i64, i64), V)> {
        let Some(q) = self.graph.stores.iq_get(store) else {
            return Vec::new();
        };
        let kb = ks.serialize(store, key);
        q.iq_session_fetch_key(&kb)
            .await
            .into_iter()
            .map(|(win, v)| (win, vs.deserialize(store, &v).expect("iq deserialize")))
            .collect()
    }

    /// Pops and deserializes the next output record for `topic`.
    ///
    /// `produced` is the same `Produced::with(key_serde, value_serde)` pair the
    /// sink writing `topic` produced with.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn read_output<KS, VS>(
        &mut self,
        topic: &str,
        produced: impl Into<Produced<KS, VS>>,
    ) -> Option<(Option<KS::Target>, VS::Target)>
    where
        KS: SerdeAssociate + Serde<KS::Target>,
        VS: SerdeAssociate + Serde<VS::Target>,
    {
        let produced = produced.into();
        let out = self.output.get_mut(topic)?.pop_front()?;
        let key = out.key.map(|b| {
            produced
                .key_serde
                .deserialize(topic, &b)
                .expect("test: deserialize output key")
        });
        let value = produced
            .value_serde
            .deserialize(topic, &out.value.unwrap_or_default())
            .expect("test: deserialize output value");
        Some((key, value))
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use async_trait::async_trait;

    use super::*;
    use crate::{
        processor::{
            api::{Processor, ProcessorContext},
            record::Record,
            serde::StringSerde,
        },
        topology::{NodeHandle, Topology},
    };

    struct Upper;
    #[async_trait]
    impl Processor<String, String, String, String> for Upper {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, String>,
            r: Record<String, String>,
        ) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }
    struct DropEmpty;
    #[async_trait]
    impl Processor<String, String, String, String> for DropEmpty {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, String>,
            r: Record<String, String>,
        ) {
            if !r.value.is_empty() {
                ctx.forward(r);
            }
        }
    }
    struct Identity;
    #[async_trait]
    impl Processor<String, String, String, String> for Identity {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, String>,
            r: Record<String, String>,
        ) {
            ctx.forward(r);
        }
    }

    fn map_filter() -> crate::topology::BuiltTopology {
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let up = t.add_processor("up", || Upper, [&src]);
        let flt = t.add_processor("flt", || DropEmpty, [&up]);
        t.add_sink("out", "out", [&flt]);
        t.build("app").unwrap()
    }

    #[test]
    fn test_driver_disables_cache() {
        // The JVM `TopologyTestDriver` default is `statestore.cache.max.bytes = 0`,
        // so the driver builds its graph with caching disabled (emit-on-update).
        let built = map_filter();
        let d = TopologyTestDriver::new(&built).unwrap();
        check!(d.graph.cache_max_bytes() == ByteSize::ZERO);
    }

    #[test]
    fn map_filter_through() {
        let built = map_filter();
        let mut d = TopologyTestDriver::new(&built).unwrap();
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some("k".to_string()),
            "hello".to_string(),
            0,
        );
        check!(
            d.read_output("out", Produced::with(StringSerde, StringSerde))
                == Some((Some("k".to_string()), "HELLO".to_string()))
        );
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some("k2".to_string()),
            String::new(),
            1,
        );
        check!(
            d.read_output("out", Produced::with(StringSerde, StringSerde))
                == None::<(Option<String>, String)>
        );
    }

    #[test]
    fn repartition_loops_through() {
        // src(in) -> identity -> sink(rp, internal repartition) ; src(rp) -> up -> sink(out)
        let mut t = Topology::new();
        t.add_repartition_topic("rp");
        let s1: NodeHandle<String, String> = t.add_source("s1", ["in"]);
        let id = t.add_processor("id", || Identity, [&s1]);
        t.add_sink("to_rp", "rp", [&id]);
        let s2: NodeHandle<String, String> = t.add_source("s2", ["rp"]);
        let up = t.add_processor("up", || Upper, [&s2]);
        t.add_sink("out", "out", [&up]);
        let built = t.build("app").unwrap();

        let mut d = TopologyTestDriver::new(&built).unwrap();
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            None,
            "hi".to_string(),
            0,
        );
        check!(
            d.read_output("out", Produced::with(StringSerde, StringSerde))
                == Some((None, "HI".to_string()))
        );
    }

    #[test]
    fn branch_to_two_sinks() {
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let up = t.add_processor("up", || Upper, [&src]);
        t.add_sink("a", "out-a", [&up]);
        t.add_sink("b", "out-b", [&up]);
        let built = t.build("app").unwrap();
        let mut d = TopologyTestDriver::new(&built).unwrap();
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            None,
            "x".to_string(),
            0,
        );
        check!(
            d.read_output("out-a", Produced::with(StringSerde, StringSerde))
                == Some((None, "X".to_string()))
        );
        check!(
            d.read_output("out-b", Produced::with(StringSerde, StringSerde))
                == Some((None, "X".to_string()))
        );
    }

    #[test]
    fn stateful_count_and_store_inspection() {
        use crate::processor::serde::I64Serde;
        struct Counter;
        #[async_trait]
        impl Processor<String, String, String, i64> for Counter {
            async fn process(
                &mut self,
                ctx: &mut ProcessorContext<'_, '_, String, i64>,
                r: Record<String, String>,
            ) {
                let n = {
                    let s = ctx.get_state_store::<String, i64>("counts").unwrap();
                    let n = s.get(&r.value).await.unwrap_or(0) + 1;
                    s.put(r.value.clone(), n).await;
                    n
                };
                ctx.forward(Record::new(Some(r.value), n, r.timestamp));
            }
        }
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let c = t.add_processor("c", || Counter, [&src]);
        t.add_state_store("counts", StringSerde, I64Serde, [c.name()]);
        t.add_sink("out", "out", [&c]);
        let mut d = TopologyTestDriver::new(&t.build("app").unwrap()).unwrap();
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            None,
            "a".to_string(),
            0,
        );
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            None,
            "a".to_string(),
            1,
        );
        check!(
            d.read_output("out", Produced::with(StringSerde, I64Serde))
                == Some((Some("a".to_string()), 1))
        );
        check!(
            d.read_output("out", Produced::with(StringSerde, I64Serde))
                == Some((Some("a".to_string()), 2))
        );
        check!(d.store_get::<String, i64>("counts", &"a".to_string()) == Some(2));
    }

    #[tokio::test]
    async fn iqv2_query_kv_via_driver() {
        use crate::{
            processor::serde::{Consumed, StringSerde},
            runtime::iqv2::{KeyQuery, StateQueryRequest},
        };

        let b = crate::StreamsBuilder::new();
        b.stream::<String, String>(["in"])
            .group_by_key()
            .count("counts");
        let built = b.build("app").unwrap();
        let mut d = TopologyTestDriver::new(&built).unwrap();
        for v in ["a", "a", "b"] {
            d.pipe_input(
                "in",
                Consumed::with(StringSerde, StringSerde),
                Some(v.to_string()),
                v.to_string(),
                0,
            );
        }

        let res = d
            .query(
                StateQueryRequest::in_store("counts")
                    .with_query(KeyQuery::<String, i64>::with_key("a".into())),
            )
            .await;
        let only = res.only_partition_result().unwrap();
        assert_eq!(only.result(), Some(&Some(2)));
    }
}
