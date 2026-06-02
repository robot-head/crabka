//! `BrokerPool`: a `DashMap<broker_id, Arc<Connection>>` with lazy
//! connect on first use.

use std::net::SocketAddr;
use std::sync::Arc;

use dashmap::DashMap;

use crate::connection::{Connection, ConnectionOptions};
use crate::error::ClientError;

/// Information about a single Kafka broker, as reported by a `MetadataResponse`.
#[derive(Debug, Clone)]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

/// Pool of `Arc<Connection>` keyed by broker id. Connections are opened lazily
/// on first use and cached thereafter.
pub struct BrokerPool {
    by_id: DashMap<i32, Arc<Connection>>,
    by_addr: DashMap<i32, SocketAddr>,
    bootstrap: Vec<SocketAddr>,
    options: ConnectionOptions,
}

impl BrokerPool {
    /// Create a new pool with the given bootstrap addresses and connection options.
    #[must_use]
    pub fn new(bootstrap: Vec<SocketAddr>, options: ConnectionOptions) -> Self {
        Self {
            by_id: DashMap::new(),
            by_addr: DashMap::new(),
            bootstrap,
            options,
        }
    }

    /// Get-or-connect to a specific broker id. The pool must have already
    /// learned the (id, address) mapping via [`refresh_brokers`].
    ///
    /// [`refresh_brokers`]: BrokerPool::refresh_brokers
    pub async fn get(&self, broker_id: i32) -> Result<Arc<Connection>, ClientError> {
        if let Some(entry) = self.by_id.get(&broker_id) {
            return Ok(entry.clone());
        }
        let addr = self
            .by_addr
            .get(&broker_id)
            .map(|e| *e)
            .ok_or(ClientError::Disconnected)?;
        let conn = Arc::new(Connection::connect_with_options(addr, self.options.clone()).await?);
        self.by_id.insert(broker_id, conn.clone());
        Ok(conn)
    }

    /// Get-or-connect to the first reachable bootstrap address. The bootstrap
    /// connection is cached under the synthetic broker id `-1`.
    pub async fn bootstrap_connection(&self) -> Result<Arc<Connection>, ClientError> {
        const BOOTSTRAP_ID: i32 = -1;
        if let Some(entry) = self.by_id.get(&BOOTSTRAP_ID) {
            return Ok(entry.clone());
        }
        let mut last_err: Option<ClientError> = None;
        for addr in &self.bootstrap {
            match Connection::connect_with_options(*addr, self.options.clone()).await {
                Ok(c) => {
                    let arc = Arc::new(c);
                    self.by_id.insert(BOOTSTRAP_ID, arc.clone());
                    return Ok(arc);
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or(ClientError::Disconnected))
    }

    /// Update the (id, addr) address registry from a list of brokers, typically
    /// sourced from a `MetadataResponse`. Does not open any new connections.
    ///
    /// Brokers advertising port `0` are skipped: that is not a dialable address
    /// (it shows up for in-process test brokers whose advertised port never got
    /// rewritten to the real bound port). Leaving such an entry out means
    /// [`get`](BrokerPool::get) reports `Disconnected` for that id, letting a
    /// caller fall back to the bootstrap connection rather than attempting a
    /// doomed `host:0` connect.
    pub fn refresh_brokers(&self, brokers: &[BrokerInfo]) {
        for b in brokers {
            if b.port == 0 {
                continue;
            }
            let addr_str = format!("{}:{}", b.host, b.port);
            if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                self.by_addr.insert(b.id, addr);
            }
        }
    }

    /// Whether the (id → addr) registry knows a dialable address for this
    /// broker id (i.e. [`refresh_brokers`](BrokerPool::refresh_brokers) learned
    /// it and the port was not `0`). A caller can use this to decide between
    /// routing to a specific broker and falling back to the bootstrap
    /// connection, without a speculative connect.
    #[must_use]
    pub fn knows_broker(&self, broker_id: i32) -> bool {
        self.by_addr.contains_key(&broker_id)
    }

    /// Close every open connection in the pool. Consumes the pool.
    pub fn close_all(self) {
        let conns: Vec<_> = self.by_id.iter().map(|e| e.value().clone()).collect();
        drop(self.by_id);
        // Drop each Arc; when the last reference goes away the background tasks
        // shut down naturally via the CancellationToken in ConnectionInner.
        drop(conns);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn refresh_inserts_addresses() {
        let pool = BrokerPool::new(vec![], ConnectionOptions::default());
        pool.refresh_brokers(&[
            BrokerInfo {
                id: 1,
                host: "127.0.0.1".into(),
                port: 9092,
                rack: None,
            },
            BrokerInfo {
                id: 2,
                host: "127.0.0.1".into(),
                port: 9093,
                rack: None,
            },
        ]);
        assert!(pool.by_addr.contains_key(&1));
        assert!(pool.by_addr.contains_key(&2));
        assert!(*pool.by_addr.get(&1).unwrap() == "127.0.0.1:9092".parse().unwrap());
        assert!(*pool.by_addr.get(&2).unwrap() == "127.0.0.1:9093".parse().unwrap());
    }
}
