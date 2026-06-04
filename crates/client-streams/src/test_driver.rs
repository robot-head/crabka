//! Synchronous, broker-free driver for testing topologies (JVM
//! `TopologyTestDriver` analog). Pipe a typed input record, read typed output.
//! Records produced to an internal topic that is also a source (repartition)
//! are looped back into the graph.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::processor::erased::{OutputRecord, ProcessorError};
use crate::processor::graph::Graph;
use crate::processor::serde::{Consumed, Produced, Serde};
use crate::topology::BuiltTopology;

/// A pending record entry in the repartition loop-back queue.
type PendingRecord = (String, Option<Vec<u8>>, Vec<u8>, i64);

/// Synchronous test driver: builds a runnable [`Graph`] from a [`BuiltTopology`]
/// and exposes `pipe_input` / `read_output` for exercising processing logic
/// without a real broker.
pub struct TopologyTestDriver {
    graph: Graph,
    source_topics: HashSet<String>,
    output: HashMap<String, VecDeque<OutputRecord>>,
}

impl TopologyTestDriver {
    /// Instantiate the topology's graph for testing. Errors if the topology is
    /// invalid (propagates `instantiate`'s error).
    pub fn new(built: &BuiltTopology) -> Result<Self, ProcessorError> {
        let source_topics: HashSet<String> = built.list_source_topics().into_iter().collect();
        let mut graph = built.instantiate()?;
        graph.init_processors()?;
        Ok(Self {
            graph,
            source_topics,
            output: HashMap::new(),
        })
    }

    /// Serialize + pipe one record on `topic`; loops repartition outputs back.
    ///
    /// `consumed` is the same `Consumed::with(key_serde, value_serde)` pair the
    /// source for `topic` reads with.
    #[allow(clippy::needless_pass_by_value)] // owned K/V is the natural API
    pub fn pipe_input<K, V, KS: Serde<K>, VS: Serde<V>>(
        &mut self,
        topic: &str,
        consumed: Consumed<KS, VS>,
        key: Option<K>,
        value: V,
        timestamp: i64,
    ) {
        let kb = key.as_ref().map(|k| consumed.key_serde.serialize(k));
        let vb = consumed.value_serde.serialize(&value);
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
            let _ = self.graph.pipe(&t, k.as_deref(), &v, ts);
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
            // has no broker, so we discard them (restore is a no-op with fresh stores).
            let _ = self.graph.drain_changelogs();
        }
    }

    /// Inspect a state store's contents after piping (mirrors the JVM
    /// `TopologyTestDriver.getKeyValueStore`). Test-only; not for interactive queries.
    pub fn get_key_value_store<K: 'static, V: 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::api::KeyValueStore<K, V>> {
        self.graph.stores.get_kv::<K, V>(name)
    }

    /// Pop + deserialize the next output record for `topic`.
    ///
    /// `produced` is the same `Produced::with(key_serde, value_serde)` pair the
    /// sink writing `topic` produced with.
    #[allow(clippy::needless_pass_by_value)] // Produced holds Copy serdes; by-value reads cleanly
    pub fn read_output<K, V, KS: Serde<K>, VS: Serde<V>>(
        &mut self,
        topic: &str,
        produced: Produced<KS, VS>,
    ) -> Option<(Option<K>, V)> {
        let out = self.output.get_mut(topic)?.pop_front()?;
        let key = out.key.map(|b| {
            produced
                .key_serde
                .deserialize(&b)
                .expect("test: deserialize output key")
        });
        let value = produced
            .value_serde
            .deserialize(&out.value.unwrap_or_default())
            .expect("test: deserialize output value");
        Some((key, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::api::{Processor, ProcessorContext};
    use crate::processor::record::Record;
    use crate::processor::serde::StringSerde;
    use crate::topology::Topology;
    use assert2::check;

    struct Upper;
    impl Processor<String, String, String, String> for Upper {
        fn process(
            &mut self,
            ctx: &mut ProcessorContext<String, String>,
            r: Record<String, String>,
        ) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }
    struct DropEmpty;
    impl Processor<String, String, String, String> for DropEmpty {
        fn process(
            &mut self,
            ctx: &mut ProcessorContext<String, String>,
            r: Record<String, String>,
        ) {
            if !r.value.is_empty() {
                ctx.forward(r);
            }
        }
    }
    struct Identity;
    impl Processor<String, String, String, String> for Identity {
        fn process(
            &mut self,
            ctx: &mut ProcessorContext<String, String>,
            r: Record<String, String>,
        ) {
            ctx.forward(r);
        }
    }

    fn map_filter() -> crate::topology::BuiltTopology {
        let mut t = Topology::new();
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        let up = t.add_processor("up", || Upper, [&src]);
        let flt = t.add_processor("flt", || DropEmpty, [&up]);
        t.add_sink(
            "out",
            "out",
            [&flt],
            Produced::with(StringSerde, StringSerde),
        );
        t.build("app").unwrap()
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
        let s1 = t.add_source("s1", ["in"], Consumed::with(StringSerde, StringSerde));
        let id = t.add_processor("id", || Identity, [&s1]);
        t.add_sink(
            "to_rp",
            "rp",
            [&id],
            Produced::with(StringSerde, StringSerde),
        );
        let s2 = t.add_source("s2", ["rp"], Consumed::with(StringSerde, StringSerde));
        let up = t.add_processor("up", || Upper, [&s2]);
        t.add_sink(
            "out",
            "out",
            [&up],
            Produced::with(StringSerde, StringSerde),
        );
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
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        let up = t.add_processor("up", || Upper, [&src]);
        t.add_sink(
            "a",
            "out-a",
            [&up],
            Produced::with(StringSerde, StringSerde),
        );
        t.add_sink(
            "b",
            "out-b",
            [&up],
            Produced::with(StringSerde, StringSerde),
        );
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
        impl Processor<String, String, String, i64> for Counter {
            fn process(
                &mut self,
                ctx: &mut ProcessorContext<String, i64>,
                r: Record<String, String>,
            ) {
                let s = ctx.get_state_store::<String, i64>("counts").unwrap();
                let n = s.get(&r.value).unwrap_or(0) + 1;
                s.put(r.value.clone(), n);
                ctx.forward(Record::new(Some(r.value), n, r.timestamp));
            }
        }
        let mut t = Topology::new();
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        let c = t.add_processor("c", || Counter, [&src]);
        t.add_state_store("counts", StringSerde, I64Serde, [c.name()]);
        t.add_sink("out", "out", [&c], Produced::with(StringSerde, I64Serde));
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
        let store = d.get_key_value_store::<String, i64>("counts").unwrap();
        check!(store.get(&"a".to_string()) == Some(2));
    }
}
