//! Orders-analytics demo: a deterministic order generator + the Streams topology
//! shape, plus the pure business rules the traced consumer applies. The real
//! proto/registry/broker run lives in `main.rs`.

use crabka_client_streams::{DefaultSerde, SchemaSerde};
use crabka_schema_serde::format::protobuf::ProtobufSerde;

pub mod metrics;

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
/// Regions orders originate from. Cycles slower than category so each category
/// spans regions (richer cross-product for span attributes / metric labels).
pub const REGIONS: &[&str] = &["us-east", "us-west", "eu-west", "ap-south"];
/// Fulfillment center serving each region (aligned index-for-index with
/// [`REGIONS`]).
pub const WAREHOUSES: &[&str] = &["wh-atl", "wh-sjc", "wh-fra", "wh-blr"];
/// Payment methods (drives the fraud heuristic and payment-method labels).
pub const PAYMENT_METHODS: &[&str] = &["card", "paypal", "wire", "crypto"];
/// Customer tiers.
pub const CUSTOMER_TIERS: &[&str] = &["free", "pro", "enterprise"];

/// Deterministic order for index `i` (no RNG — varied but reproducible). Every
/// field is varied at a different period so the pipeline emits a broad spread of
/// span attributes and metric label combinations.
#[must_use]
pub fn order_at(i: u64) -> Order {
    let idx = |len: usize, div: u64| usize::try_from((i / div) % len as u64).unwrap_or(0);
    let category = CATEGORIES[idx(CATEGORIES.len(), 1)];
    let region_i = idx(REGIONS.len(), 2);
    let region = REGIONS[region_i];
    let warehouse = WAREHOUSES[region_i];
    let payment_method = PAYMENT_METHODS[idx(PAYMENT_METHODS.len(), 3)];
    let customer_tier = CUSTOMER_TIERS[idx(CUSTOMER_TIERS.len(), 7)];
    let quantity = i32::try_from(1 + i % 5).unwrap_or(1);
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
        region: region.to_string(),
        payment_method: payment_method.to_string(),
        quantity,
        customer_tier: customer_tier.to_string(),
        warehouse: warehouse.to_string(),
    }
}

/// A seeded anomaly (zero-amount) order. Drives the produce-side warn log and
/// the consumer's `anomalous` processing outcome.
#[must_use]
pub fn is_anomalous(order: &Order) -> bool {
    order.amount.abs() < f64::EPSILON
}

/// Deterministic demo "fraud" heuristic that drives the fraud-check span outcome
/// and the `fraud_rejected` processing metric: high-value crypto orders are
/// flagged. Pure, so the trace/metric outcome is reproducible and unit-testable.
#[must_use]
pub fn is_suspicious(order: &Order) -> bool {
    order.payment_method == "crypto" && order.amount > 150.0
}

/// The terminal outcome the traced consumer assigns to an order. Also the value
/// of the `outcome` metric label and the `demo.order.outcome` span attribute.
#[must_use]
pub fn classify_outcome(order: &Order) -> &'static str {
    if is_anomalous(order) {
        "anomalous"
    } else if is_suspicious(order) {
        "fraud_rejected"
    } else {
        "fulfilled"
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
    fn order_at_populates_rich_fields_with_aligned_warehouse() {
        let o = order_at(0);
        assert_eq!(o.region, "us-east");
        assert_eq!(o.warehouse, "wh-atl", "warehouse serves the order's region");
        assert_eq!(o.payment_method, "card");
        assert_eq!(o.customer_tier, "free");
        assert!((1..=5).contains(&o.quantity));

        // The warehouse index tracks the region index for every order.
        for i in [1_u64, 2, 3, 7, 42, 199] {
            let o = order_at(i);
            let region_i = REGIONS.iter().position(|r| *r == o.region).unwrap();
            assert_eq!(WAREHOUSES[region_i], o.warehouse);
        }
    }

    #[test]
    fn outcome_classification_covers_the_three_paths() {
        // Anomalous (zero amount) wins over everything.
        assert_eq!(classify_outcome(&order_at(17)), "anomalous");

        // A high-value crypto order is flagged as fraud.
        let mut fraud = order_at(1);
        fraud.payment_method = "crypto".to_string();
        fraud.amount = 199.99;
        assert!(is_suspicious(&fraud));
        assert_eq!(classify_outcome(&fraud), "fraud_rejected");

        // A normal card order is fulfilled.
        let mut ok = order_at(1);
        ok.payment_method = "card".to_string();
        ok.amount = 42.0;
        assert!(!is_suspicious(&ok));
        assert_eq!(classify_outcome(&ok), "fulfilled");
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
