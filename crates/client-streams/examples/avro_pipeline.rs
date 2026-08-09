// `#[tracing::instrument]` on the DSL runtime deepens this example's `main`
// future past the default type-layout depth limit; raise it.
#![recursion_limit = "512"]
//! Applied Avro Streams pipeline over **compound types**, in an e-commerce
//! scenario. It uses `StreamsApp` and the default-serde DSL. It needs a running
//! broker at `127.0.0.1:9092` and a Confluent-compatible registry at
//! `http://127.0.0.1:8081`.
//!
//! The pipeline reads Avro `Order` records from `orders` and runs them through a
//! custom Processor-API node. It keeps the paid orders, projects each one into an
//! Avro `OrderSummary` with a line-item count and a discounted total, and writes
//! them to `order-summaries`. Every value that crosses a topic boundary is a
//! nested Avro record, built from structs, a `Vec`, an `Option`, and an enum. The
//! pipeline registers and resolves each one against the schema registry.
//!
//! Run: `cargo run -p crabka-client-streams --example avro_pipeline`

use apache_avro::AvroSchema;
use crabka_client_streams::{DefaultSerde, Record, SchemaSerde, StreamsApp, impl_processor};
use crabka_schema_serde::format::avro::AvroSerde;
use serde::{Deserialize, Serialize};

/// A customer order, the inbound Avro value. It holds a nested record, an array,
/// an optional, and an enum, which is the shape that real payloads have.
#[derive(Clone, Debug, Serialize, Deserialize, AvroSchema)]
pub struct Order {
    pub order_id: String,
    pub customer_id: String,
    pub status: OrderStatus,
    pub lines: Vec<LineItem>,
    pub coupon: Option<Coupon>,
    pub placed_at_ms: i64,
}

/// One line of an order (a nested Avro record inside the `lines` array).
#[derive(Clone, Debug, Serialize, Deserialize, AvroSchema)]
pub struct LineItem {
    pub sku: String,
    pub quantity: i32,
    pub unit_price_cents: i64,
}

/// An applied discount (a nested optional Avro record).
#[derive(Clone, Debug, Serialize, Deserialize, AvroSchema)]
pub struct Coupon {
    pub code: String,
    pub percent_off: i32,
}

/// Order lifecycle state, a C-style Avro `enum`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, AvroSchema)]
pub enum OrderStatus {
    Placed,
    Paid,
    Shipped,
    Cancelled,
}

/// The projected, billable summary, the outbound Avro value.
#[derive(Clone, Debug, Serialize, Deserialize, AvroSchema)]
pub struct OrderSummary {
    pub order_id: String,
    pub customer_id: String,
    pub item_count: i64,
    pub total_cents: i64,
}

// Declare each Avro type's default serde so the plain `stream`/`to` DSL works
// with no per-call serde wiring (values resolve against the process registry).
impl DefaultSerde for Order {
    type Serde = SchemaSerde<Order, AvroSerde<Order>>;
}
impl DefaultSerde for OrderSummary {
    type Serde = SchemaSerde<OrderSummary, AvroSerde<OrderSummary>>;
}

fn summarize(order: &Order) -> OrderSummary {
    let gross: i64 = order
        .lines
        .iter()
        .map(|l| i64::from(l.quantity) * l.unit_price_cents)
        .sum();
    let total_cents = match &order.coupon {
        Some(c) => gross - gross * i64::from(c.percent_off) / 100,
        None => gross,
    };
    OrderSummary {
        order_id: order.order_id.clone(),
        customer_id: order.customer_id.clone(),
        item_count: i64::try_from(order.lines.len()).unwrap_or(i64::MAX),
        total_cents,
    }
}

struct PaidOrderSummarizer;
impl_processor! {
    impl PaidOrderSummarizer: (String, Order) -> (String, OrderSummary) {
        async fn process(&mut self, ctx, r) {
            if r.value.status != OrderStatus::Paid {
                return;
            }
            let summary = summarize(&r.value);
            ctx.forward(Record::new(
                Some(summary.customer_id.clone()),
                summary,
                r.timestamp,
            ));
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("orders-summary")
        .schema_registry("http://127.0.0.1:8081")
        .build();

    let topology = app.streams_builder();
    topology
        .stream::<String, Order>(["orders"])
        .process(|| PaidOrderSummarizer, std::iter::empty::<String>())
        .to("order-summaries");

    let streams = app.run(topology).await?;
    streams.close().await?;
    Ok(())
}
