//! Profiling load generator: drive sustained produce (and optional fetch)
//! traffic against a running `profile_server`. Prints achieved throughput.
//!
//!   cargo run --release --example loadgen -p crabka-integration-tests
//!
//! Env:
//!   LOAD_BOOTSTRAP   broker addr (default 127.0.0.1:9092)
//!   LOAD_TOPIC       topic name (default loadgen)
//!   LOAD_PARTITIONS  partition count (default 8)
//!   LOAD_PRODUCERS   concurrent producer tasks (default 4)
//!   LOAD_VALUE_BYTES record value size (default 128)
//!   LOAD_SECONDS     run duration seconds (default 20)
//!   LOAD_INFLIGHT    max in-flight sends per producer (default 1000)
//!   LOAD_ACKS        0 | 1 | all (default 1)
//!   LOAD_CONSUME     1 to also run a consumer group (default 0)

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::Client;
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};

fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let bootstrap: String =
        std::env::var("LOAD_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic: String = std::env::var("LOAD_TOPIC").unwrap_or_else(|_| "loadgen".into());
    let partitions: i32 = env("LOAD_PARTITIONS", 8);
    let producers: usize = env("LOAD_PRODUCERS", 4);
    let value_bytes: usize = env("LOAD_VALUE_BYTES", 128);
    let seconds: u64 = env("LOAD_SECONDS", 20);
    let inflight: usize = env("LOAD_INFLIGHT", 1000);
    let acks_s: String = std::env::var("LOAD_ACKS").unwrap_or_else(|_| "1".into());
    let consume: bool = env::<u8>("LOAD_CONSUME", 0) == 1;

    let acks = match acks_s.as_str() {
        "0" => Acks::Zero,
        "all" => Acks::All,
        _ => Acks::One,
    };

    // Create the topic (idempotent: ignore "already exists").
    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .client_id("loadgen-admin")
        .build()
        .await
        .expect("admin client");
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.clone(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 10_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    let ec = resp.topics[0].error_code;
    // 36 = TOPIC_ALREADY_EXISTS
    assert2::assert!(ec == 0 || ec == 36);

    let value = Bytes::from(vec![0xABu8; value_bytes]);
    let sent = Arc::new(AtomicU64::new(0));
    let acked = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let start = Instant::now();
    let mut handles = Vec::new();
    for p in 0..producers {
        let bootstrap = bootstrap.clone();
        let topic = topic.clone();
        let value = value.clone();
        let sent = sent.clone();
        let acked = acked.clone();
        let stop = stop.clone();
        handles.push(tokio::spawn(async move {
            let producer = Producer::builder()
                .bootstrap(bootstrap)
                .client_id(format!("loadgen-{p}"))
                .enable_idempotence(false)
                .acks(acks)
                .linger(Duration::from_millis(5))
                .build()
                .await
                .expect("producer build");

            let mut window: std::collections::VecDeque<_> = std::collections::VecDeque::new();
            while !stop.load(Ordering::Relaxed) {
                while window.len() < inflight {
                    let f = producer
                        .send(ProducerRecord {
                            topic: topic.clone(),
                            value: Some(value.clone()),
                            ..Default::default()
                        })
                        .await;
                    sent.fetch_add(1, Ordering::Relaxed);
                    window.push_back(f);
                }
                if let Some(f) = window.pop_front()
                    && f.await.is_ok()
                {
                    acked.fetch_add(1, Ordering::Relaxed);
                }
            }
            let _ = producer.flush().await;
            for f in window {
                if f.await.is_ok() {
                    acked.fetch_add(1, Ordering::Relaxed);
                }
            }
            producer.close().await.ok();
        }));
    }

    if consume {
        let bootstrap = bootstrap.clone();
        let topic = topic.clone();
        let stop = stop.clone();
        handles.push(tokio::spawn(async move {
            let mut consumer = Consumer::builder()
                .bootstrap(&bootstrap)
                .client_id("loadgen-consumer")
                .group_id("loadgen-grp")
                .session_timeout(crabka_units::secs(30))
                .rebalance_timeout(crabka_units::secs(5))
                .heartbeat_interval(crabka_units::secs(1))
                .auto_offset_reset(AutoOffsetReset::Earliest)
                .subscribe([topic])
                .build()
                .await
                .expect("consumer build");
            while !stop.load(Ordering::Relaxed) {
                let _ = consumer.poll(Duration::from_millis(200)).await;
            }
            consumer.close().await.ok();
        }));
    }

    tokio::time::sleep(Duration::from_secs(seconds)).await;
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let a = acked.load(Ordering::Relaxed);
    let s = sent.load(Ordering::Relaxed);
    let mb = (a as f64 * value_bytes as f64) / 1e6;
    println!(
        "loadgen: acks={acks_s} producers={producers} value={value_bytes}B parts={partitions} \
         | sent={s} acked={a} | {:.0} msg/s | {:.1} MB/s over {elapsed:.1}s",
        a as f64 / elapsed,
        mb / elapsed,
    );
}
