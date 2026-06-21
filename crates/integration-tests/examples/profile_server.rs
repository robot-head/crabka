//! Profiling helper: boot a single-node in-process Crabka broker on a fixed
//! port and run until killed. Pair with the `loadgen` example and attach
//! `perf record -p <pid>` to capture a clean broker-only CPU profile.
//!
//!   cargo run --release --example profile_server -p crabka-integration-tests
//!
//! Env:
//!   PROFILE_LISTEN   bind/advertise addr (default 127.0.0.1:9092)
//!   PROFILE_DATA_DIR data dir (default /tmp/crabka-profile-data, wiped on start)

use std::path::PathBuf;

use crabka_broker::{Broker, BrokerConfig};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let listen = std::env::var("PROFILE_LISTEN").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let data_dir =
        std::env::var("PROFILE_DATA_DIR").unwrap_or_else(|_| "/tmp/crabka-profile-data".into());
    let data_dir = PathBuf::from(data_dir);
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).expect("create data dir");

    let mut cfg = BrokerConfig::for_tests(data_dir);
    cfg.listen_addr = listen.parse().expect("PROFILE_LISTEN must be host:port");
    cfg.advertised_listener = listen.clone();

    let broker = Broker::start(cfg).await.expect("broker start");
    let addr = broker.listen_addr().to_string();
    println!("PROFILE_SERVER pid={} listen={addr}", std::process::id());

    tokio::signal::ctrl_c().await.expect("ctrl_c");
    broker.shutdown().await;
}
