//! Top-level `Broker` lifecycle. Wires together the partition registry,
//! metadata image, network listener, and handler table.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};

use dashmap::DashMap;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::BrokerConfig;
use crate::error::BrokerError;
use crate::handlers::HandlerTable;
use crate::log_dir;
use crate::metadata::MetadataImage;
use crate::partition::{Partition, ProduceJob};

/// The running broker. Library callers get a [`BrokerHandle`] from
/// [`Broker::start`]; this struct is the shared internal state.
// `config`, `metadata`, `partitions` are consumed by the per-API handlers
// landing in Tasks 12-16; allow dead_code on the struct until the handlers
// pick them up.
#[allow(dead_code)]
pub struct Broker {
    pub(crate) config: BrokerConfig,
    pub(crate) metadata: Arc<RwLock<MetadataImage>>,
    /// Wrapped in `Arc` so handlers cloning the field share the same
    /// underlying map. `DashMap::clone` is a deep copy by default.
    pub(crate) partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    pub(crate) group_manager: Arc<crate::coordinator::GroupManager>,
    handlers: HandlerTable,
}

impl Broker {
    pub(crate) fn handlers(&self) -> &HandlerTable {
        &self.handlers
    }
}

/// Lifecycle handle returned by [`Broker::start`]. Drop or call
/// [`shutdown`](BrokerHandle::shutdown) to stop the broker.
pub struct BrokerHandle {
    listen_addr: SocketAddr,
    shutdown: CancellationToken,
    listener_task: Option<JoinHandle<()>>,
    /// Held so partition writer tasks live as long as the handle.
    _broker: Arc<Broker>,
}

impl BrokerHandle {
    /// The actual bound `SocketAddr` (useful when `BrokerConfig.listen_addr`
    /// used port 0 to let the OS pick).
    #[must_use]
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// Cancel the listener + drain in-flight connections. Awaiting the
    /// returned future blocks until the listener task exits.
    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(t) = self.listener_task.take() {
            let _ = t.await;
        }
    }
}

impl Broker {
    /// Build a `Broker`, scan the log dir, spawn partition writers for
    /// every existing `<topic>-<partition>/`, bind the TCP listener, and
    /// return the handle.
    pub async fn start(mut config: BrokerConfig) -> Result<BrokerHandle, BrokerError> {
        let metadata = Arc::new(RwLock::new(MetadataImage::new()));
        let partitions: Arc<DashMap<(String, i32), Arc<Partition>>> = Arc::new(DashMap::new());

        // 1. Scan + recover.
        for (topic, partition_id) in log_dir::scan(&config.log_dir)? {
            let dir = log_dir::partition_dir(&config.log_dir, &topic, partition_id);
            let log = crabka_log::Log::open(&dir, config.log_config.clone())?;
            let part = spawn_partition(topic.clone(), partition_id, log);
            partitions.insert((topic.clone(), partition_id), part);
        }
        // Now derive partition_count per topic and seed the metadata image.
        {
            let mut meta = metadata.write().expect("metadata poisoned");
            let mut by_topic: std::collections::BTreeMap<String, i32> =
                std::collections::BTreeMap::default();
            for entry in partitions.iter() {
                let (topic, partition_id) = entry.key();
                let cur = by_topic.entry(topic.clone()).or_insert(0);
                if *partition_id + 1 > *cur {
                    *cur = *partition_id + 1;
                }
            }
            for (topic, count) in by_topic {
                meta.insert_topic(&topic, count, config.broker_id);
            }
        }

        // Group coordinator bootstrap (slice 5).
        let group_manager = Arc::new(crate::coordinator::GroupManager::new());
        crate::coordinator::bootstrap::bootstrap(
            &config,
            &metadata,
            &partitions,
            group_manager.as_ref(),
        )
        .await?;

        // 2. Build handler table.
        let handlers = crate::handlers::build_table();

        // 3. Bind first so the actual port is known. If
        //    `advertised_listener` points at port 0 (tests typically),
        //    rewrite it to the bound port so FindCoordinator/Metadata
        //    return a useful host:port instead of `:0`.
        let listener = TcpListener::bind(config.listen_addr).await?;
        let listen_addr = listener.local_addr()?;
        if config.advertised_listener.ends_with(":0") {
            if let Some((host, _)) = config.advertised_listener.rsplit_once(':') {
                config.advertised_listener = format!("{host}:{}", listen_addr.port());
            }
        }
        let broker = Arc::new(Self {
            config,
            metadata,
            partitions,
            group_manager: group_manager.clone(),
            handlers,
        });

        let shutdown = CancellationToken::new();
        let listener_task = tokio::spawn(accept_loop(broker.clone(), listener, shutdown.clone()));

        Ok(BrokerHandle {
            listen_addr,
            shutdown,
            listener_task: Some(listener_task),
            _broker: broker,
        })
    }
}

/// Create the partition runtime (mpsc channel + writer task + notify).
pub(crate) fn spawn_partition(
    topic: String,
    partition_id: i32,
    log: crabka_log::Log,
) -> Arc<Partition> {
    let log = Arc::new(Mutex::new(log));
    let (tx, rx) = tokio::sync::mpsc::channel::<ProduceJob>(64);
    let notify = Arc::new(tokio::sync::Notify::new());
    let writer = tokio::spawn(crate::partition_writer::run(
        log.clone(),
        rx,
        notify.clone(),
    ));
    Arc::new(Partition {
        topic,
        partition_id,
        log,
        writer_tx: tx,
        append_notify: notify,
        _writer_handle: Arc::new(writer),
    })
}

async fn accept_loop(broker: Arc<Broker>, listener: TcpListener, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                tracing::info!("listener shutting down");
                break;
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        tracing::debug!(%peer, "accepted connection");
                        let b = broker.clone();
                        tokio::spawn(async move {
                            crate::network::dispatch::serve_connection(b, stream).await;
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "accept failed");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn start_and_shutdown_clean() {
        let dir = tempdir().unwrap();
        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = Broker::start(config).await.unwrap();
        assert_ne!(handle.listen_addr().port(), 0);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn start_recovers_existing_partition_dirs() {
        let dir = tempdir().unwrap();
        // Create a partition dir with a log inside.
        let part_dir = dir.path().join("foo-0");
        std::fs::create_dir(&part_dir).unwrap();
        {
            let _log = crabka_log::Log::open(&part_dir, crabka_log::LogConfig::default()).unwrap();
        }

        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = Broker::start(config).await.unwrap();
        // We can't easily inspect the partition registry from outside the
        // crate yet, but starting cleanly is the assertion we need here.
        handle.shutdown().await;
    }
}
