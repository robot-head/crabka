// `#[tracing::instrument]` on the DSL runtime deepens this example's `main`
// future past the default type-layout depth limit; raise it.
#![recursion_limit = "512"]
//! Applied Avro Streams pipeline over **rich compound types** (a realistic
//! e-commerce scenario), using `StreamsApp` + the default-serde DSL. Requires a
//! running broker (`127.0.0.1:9092`) and a Confluent-compatible registry
//! (`http://127.0.0.1:8081`).
//!
//! Pipeline: read Avro `Order` records from `orders`, run them through a custom
//! Processor-API node, keep the paid ones, project each into an Avro
//! `OrderSummary` (line-item count + discounted total), and write them to
//! `order-summaries`. Every value crossing a topic boundary is a nested Avro
//! record (structs, a `Vec`, an `Option`, and an enum) registered and resolved
//! against the schema registry.
//!
//! Run: `cargo run -p crabka-client-streams --example avro_pipeline`

use apache_avro::AvroSchema;
use crabka_client_streams::{DefaultSerde, Record, SchemaSerde, StreamsApp, impl_processor};
use crabka_schema_serde::format::avro::AvroSerde;
use serde::{Deserialize, Serialize};

/// A customer order — the inbound Avro value. Nested record + array + optional +
/// enum, i.e. the shape real payloads actually have.
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

/// Order lifecycle state — a C-style Avro `enum`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, AvroSchema)]
pub enum OrderStatus {
    Placed,
    Paid,
    Shipped,
    Cancelled,
}

/// The projected, billable summary — the outbound Avro value.
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
