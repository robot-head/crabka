//! Orders-analytics demo: a deterministic order generator + the Streams topology
//! shape. The real proto/registry/broker run lives in `main.rs`.

use crabka_client_streams::{DefaultSerde, SchemaSerde};
use crabka_schema_serde::format::protobuf::ProtobufSerde;

pub const FILE_DESCRIPTOR_SET_BYTES: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));

mod order {
    include!(concat!(env!("OUT_DIR"), "/demo.rs"));
}
pub use order::Order;

// Proto Order resolves against the process default registry.
// Placed here (not main.rs) so `Order` is local to the crate that defines it,
// satisfying the orphan rule.
impl DefaultSerde for Order {
    type Serde = SchemaSerde<Order, ProtobufSerde<Order>>;
}

/// The category keys the generator cycles through.
pub const CATEGORIES: &[&str] = &["books", "electronics", "grocery", "toys", "garden"];

/// Deterministic order for index `i` (no RNG — varied but reproducible).
#[must_use]
pub fn order_at(i: u64) -> Order {
    let category = CATEGORIES[usize::try_from(i % CATEGORIES.len() as u64).unwrap_or(0)];
    // A few anomalous (zero-amount) orders to drive warn logs / error spans.
    let amount = if i.is_multiple_of(17) {
        0.0
    } else {
        // value is bounded < 200, so the u64->f64 cast is exact.
        #[allow(clippy::cast_precision_loss)]
        let dollars = (i % 200) as f64;
        dollars + 0.99
    };
    Order {
        order_id: format!("o-{i:010}"),
        category: category.to_string(),
        amount,
        currency: "USD".to_string(),
        ts_ms: 0, // stamped at send time in main.rs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, I64Serde, StringSerde, TopologyTestDriver};

    #[test]
    fn order_at_is_deterministic_and_cycles_categories() {
        assert_eq!(order_at(0).category, "books");
        assert_eq!(order_at(1).category, "electronics");
        assert_eq!(order_at(5).category, "books");
        assert_eq!(order_at(0).order_id, "o-0000000000");
        assert!(
            order_at(17).amount.abs() < f64::EPSILON,
            "every 17th order is anomalous (zero amount)"
        );
    }

    #[test]
    fn count_topology_aggregates_by_category() {
        // Validate the group_by_key -> count -> to_stream -> to chain (the same
        // structure main.rs uses with proto serdes) using registry-free StringSerde.
        let b = StreamsBuilder::new();
        b.stream::<String, String>(["orders"])
            .group_by_key()
            .count("orders-by-category-store")
            .to_stream()
            .to("order-counts");
        let built = b.build("orders-analytics-test").expect("build topology");
        let mut driver = TopologyTestDriver::new(&built).expect("driver");

        for (k, v) in [("books", "a"), ("books", "b"), ("toys", "c")] {
            driver.pipe_input(
                "orders",
                Consumed::with(StringSerde, StringSerde),
                Some(k.to_string()),
                v.to_string(),
                0,
            );
        }
        // read_output pops ONE deserialized record per call:
        //   fn read_output<KS, VS>(&mut self, topic, produced: impl Into<Produced<KS,VS>>)
        //       -> Option<(Option<KS::Target>, VS::Target)>
        // Type params are inferred from the `produced` arg — pass the serdes, not turbofish.
        let mut books_count: i64 = 0;
        while let Some((key, value)) = driver.read_output("order-counts", (StringSerde, I64Serde)) {
            if key.as_deref() == Some("books") {
                books_count = value; // keep the latest emitted count for "books"
            }
        }
        assert_eq!(books_count, 2, "two 'books' orders → count 2");
    }
}
