//! `BrokerPool`: a `DashMap<broker_id, Arc<Connection>>` with lazy
//! connect on first use.

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

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

/// The single live-IO dependency [`BrokerPool`] needs: dial an address and
/// return an opened connection. Abstracting it behind a trait (mirroring
/// `connect-postgres`'s `PgCatalog` seam) makes the pool's caching, fallback
/// iteration, and eviction *logic* killable without a socket: a `mockall` mock
/// connector hands back a stand-in connection type, so [`BrokerPool::get`],
/// [`BrokerPool::bootstrap_connection`], [`BrokerPool::evict`],
/// [`BrokerPool::evict_bootstrap`], and [`BrokerPool::close_all`] are all
/// exercised under the crate's default feature set. The only un-mockable part —
/// the actual TCP dial + API-versions handshake — stays in [`TcpConnector`].
///
/// The trait has an associated `Conn` type rather than returning a fixed handle
/// so tests can substitute a cheap stand-in; `mockall` does not cleanly mock a
/// generic-associated-type trait, so the test connector is hand-written
/// (`CountingConnector`) instead of `automock`-generated.
#[async_trait::async_trait]
pub trait BrokerConnector: Send + Sync {
    /// Connection handle this connector produces. `Connection` in production; a
    /// cheap stand-in in tests so caching/fallback decisions are observable
    /// without opening a real socket.
    type Conn: Send + Sync;

    /// Dial `addr` and return a ready connection, or a transport error.
    async fn dial(&self, addr: SocketAddr) -> Result<Self::Conn, ClientError>;
}

/// Production [`BrokerConnector`]: opens a real [`Connection`] honouring the
/// pool's TLS/SASL policy. This thin adapter is the only un-mockable part of the
/// pool (the live TCP dial + API-versions handshake).
#[derive(Debug)]
pub struct TcpConnector {
    options: ConnectionOptions,
}

#[async_trait::async_trait]
impl BrokerConnector for TcpConnector {
    type Conn = Connection;

    #[tracing::instrument(level = "debug", skip_all, fields(addr = %addr), err)]
    async fn dial(&self, addr: SocketAddr) -> Result<Connection, ClientError> {
        Connection::connect_with_options(addr, self.options.clone()).await
    }
}

/// Pool of `Arc<Connection>` keyed by broker id. Connections are opened lazily
/// on first use and cached thereafter.
///
/// Generic over the [`BrokerConnector`] seam so the caching/fallback/eviction
/// logic is unit-testable against a mock connector; the default `C` is the live
/// [`TcpConnector`], so the public type and every downstream use stay
/// `BrokerPool` (no type argument needed).
pub struct BrokerPool<C: BrokerConnector = TcpConnector> {
    by_id: DashMap<i32, Arc<C::Conn>>,
    by_addr: DashMap<i32, SocketAddr>,
    bootstrap: RwLock<Vec<SocketAddr>>,
    connector: C,
}

/// Synthetic broker id under which the shared bootstrap connection is cached.
/// Never a real Kafka node id (those are `>= 0`).
const BOOTSTRAP_ID: i32 = -1;

impl BrokerPool<TcpConnector> {
    /// Create a new pool with the given bootstrap addresses and connection options.
    #[must_use]
    pub fn new(bootstrap: Vec<SocketAddr>, options: ConnectionOptions) -> Self {
        BrokerPool::with_connector(bootstrap, TcpConnector { options })
    }
}

impl<C: BrokerConnector> BrokerPool<C> {
    /// Build a pool over an explicit [`BrokerConnector`]. Used by [`new`] for the
    /// live connector and by tests for a mock connector.
    ///
    /// [`new`]: BrokerPool::new
    fn with_connector(bootstrap: Vec<SocketAddr>, connector: C) -> Self {
        Self {
            by_id: DashMap::new(),
            by_addr: DashMap::new(),
            bootstrap: RwLock::new(bootstrap),
            connector,
        }
    }

    /// Get-or-connect to a specific broker id. The pool must have already
    /// learned the (id, address) mapping via [`refresh_brokers`].
    ///
    /// [`refresh_brokers`]: BrokerPool::refresh_brokers
    #[tracing::instrument(level = "debug", skip_all, fields(broker_id), err)]
    pub async fn get(&self, broker_id: i32) -> Result<Arc<C::Conn>, ClientError> {
        if let Some(entry) = self.by_id.get(&broker_id) {
            return Ok(entry.clone());
        }
        let addr = self
            .by_addr
            .get(&broker_id)
            .map(|e| *e)
            .ok_or(ClientError::Disconnected)?;
        let conn = Arc::new(self.connector.dial(addr).await?);
        self.by_id.insert(broker_id, conn.clone());
        Ok(conn)
    }

    /// Drop the cached connection to `broker_id` (if any) so the next
    /// [`get`](BrokerPool::get) reconnects. Used after a send fails: a bounced
    /// or failed-over broker must not be retried over its dead, cached socket.
    /// The `(id → addr)` mapping is left intact so the reconnect targets the
    /// broker's current advertised address.
    pub fn evict(&self, broker_id: i32) {
        self.by_id.remove(&broker_id);
    }

    /// Drop the cached bootstrap connection so the next
    /// [`bootstrap_connection`](BrokerPool::bootstrap_connection) re-iterates the
    /// bootstrap addresses and reconnects to a live broker. Required because the
    /// bootstrap connection is keyed by the synthetic id `-1`, which no real
    /// broker id matches — so [`evict`](BrokerPool::evict) can never reach it.
    /// Call this after a bootstrap send fails: the broker backing the bootstrap
    /// connection may have been killed (e.g. it was the failed-over partition
    /// leader), and the dead socket must not be reused for metadata refreshes.
    pub fn evict_bootstrap(&self) {
        self.by_id.remove(&BOOTSTRAP_ID);
    }

    /// Replace the bootstrap address list and drop the cached bootstrap
    /// connection so the next bootstrap send dials the fresh addresses.
    pub fn replace_bootstrap(&self, bootstrap: Vec<SocketAddr>) {
        match self.bootstrap.write() {
            Ok(mut guard) => *guard = bootstrap,
            Err(poisoned) => *poisoned.into_inner() = bootstrap,
        }
        self.evict_bootstrap();
    }

    /// Get-or-connect to the first reachable bootstrap address. The bootstrap
    /// connection is cached under the synthetic broker id `-1`.
    #[tracing::instrument(level = "debug", skip_all, err)]
    pub async fn bootstrap_connection(&self) -> Result<Arc<C::Conn>, ClientError> {
        if let Some(entry) = self.by_id.get(&BOOTSTRAP_ID) {
            return Ok(entry.clone());
        }
        let bootstrap = match self.bootstrap.read() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        let mut last_err: Option<ClientError> = None;
        for addr in bootstrap {
            match self.connector.dial(addr).await {
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
    #[tracing::instrument(level = "debug", skip_all, fields(brokers = brokers.len()))]
    pub async fn refresh_brokers(&self, brokers: &[BrokerInfo]) {
        for b in brokers {
            let Ok(port) = u16::try_from(b.port) else {
                continue;
            };
            if port == 0 {
                continue;
            }
            // Resolve the advertised host to a dialable address. Brokers
            // commonly advertise a DNS name (e.g. a Kubernetes pod FQDN), and a
            // bare `parse::<SocketAddr>()` only accepts a literal IP — so a
            // hostname-advertised broker would never enter the registry,
            // leaving `knows_broker` false and routing every produce/fetch to
            // the bootstrap connection. On a multi-broker cluster that means a
            // partition whose leader isn't the bootstrap broker gets a
            // permanent `NOT_LEADER_OR_FOLLOWER`. `lookup_host` resolves both
            // DNS names and literal IPs.
            if let Ok(mut addrs) = tokio::net::lookup_host((b.host.as_str(), port)).await
                && let Some(addr) = addrs.next()
            {
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
    // cargo-mutants: teardown; no observable return to assert against
    #[cfg_attr(test, mutants::skip)]
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
    use assert2::{assert, check};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn refresh_inserts_addresses() {
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
        ])
        .await;
        assert!(pool.by_addr.contains_key(&1));
        assert!(pool.by_addr.contains_key(&2));
        check!(*pool.by_addr.get(&1).unwrap() == "127.0.0.1:9092".parse().unwrap());
        check!(*pool.by_addr.get(&2).unwrap() == "127.0.0.1:9093".parse().unwrap());
    }

    #[tokio::test]
    async fn refresh_resolves_hostnames() {
        // Regression: a broker advertising a DNS name (not a literal IP) must
        // still enter the registry. `localhost` resolves offline.
        let pool = BrokerPool::new(vec![], ConnectionOptions::default());
        pool.refresh_brokers(&[BrokerInfo {
            id: 7,
            host: "localhost".into(),
            port: 9092,
            rack: None,
        }])
        .await;
        assert!(pool.knows_broker(7));
    }

    #[tokio::test]
    async fn refresh_skips_undialable_ports() {
        let pool = BrokerPool::new(vec![], ConnectionOptions::default());
        pool.refresh_brokers(&[
            BrokerInfo {
                id: 1,
                host: "127.0.0.1".into(),
                port: 0,
                rack: None,
            },
            BrokerInfo {
                id: 2,
                host: "127.0.0.1".into(),
                port: -1,
                rack: None,
            },
        ])
        .await;

        assert!(!pool.knows_broker(1));
        assert!(!pool.knows_broker(2));
    }

    // ── socket-free caching / fallback / eviction via a counting connector ────
    //
    // These drive the pool's connection-lifecycle logic without a broker. A
    // `CountingConnector` hands back a cheap stand-in `Conn` and records how
    // many dials it performed and against which addresses, so caching, the
    // bootstrap fallback iteration, and the two eviction paths are all killable
    // under the crate's default feature set.

    /// Stand-in connection: an opaque marker carrying the address it was dialed
    /// against so tests can prove which bootstrap address won.
    #[derive(Debug)]
    struct StubConn {
        addr: SocketAddr,
    }

    struct CountingConnector {
        dials: Arc<AtomicUsize>,
        /// Addresses that should fail to dial (simulating a dead broker).
        fail: Vec<SocketAddr>,
    }

    #[async_trait::async_trait]
    impl BrokerConnector for CountingConnector {
        type Conn = StubConn;

        async fn dial(&self, addr: SocketAddr) -> Result<StubConn, ClientError> {
            self.dials.fetch_add(1, Ordering::Relaxed);
            if self.fail.contains(&addr) {
                return Err(ClientError::Disconnected);
            }
            Ok(StubConn { addr })
        }
    }

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    #[tokio::test]
    async fn get_dials_once_then_serves_from_cache() {
        let dials = Arc::new(AtomicUsize::new(0));
        let pool = BrokerPool::with_connector(
            vec![],
            CountingConnector {
                dials: dials.clone(),
                fail: vec![],
            },
        );
        // Unknown id: no address learned → Disconnected, no dial attempted.
        assert!(matches!(pool.get(5).await, Err(ClientError::Disconnected)));
        assert!(dials.load(Ordering::Relaxed) == 0);

        pool.by_addr.insert(5, addr(9092));
        let first = pool.get(5).await.unwrap();
        assert!(first.addr == addr(9092));
        // Second get is served from cache: still a single dial.
        let second = pool.get(5).await.unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert!(dials.load(Ordering::Relaxed) == 1);
    }

    #[tokio::test]
    async fn evict_forces_reconnect_only_for_that_id() {
        let dials = Arc::new(AtomicUsize::new(0));
        let pool = BrokerPool::with_connector(
            vec![],
            CountingConnector {
                dials: dials.clone(),
                fail: vec![],
            },
        );
        pool.by_addr.insert(1, addr(9092));
        pool.by_addr.insert(2, addr(9093));
        let _ = pool.get(1).await.unwrap();
        let _ = pool.get(2).await.unwrap();
        assert!(dials.load(Ordering::Relaxed) == 2);

        // Evicting id 1 drops only its cached connection.
        pool.evict(1);
        assert!(!pool.by_id.contains_key(&1));
        assert!(pool.by_id.contains_key(&2));

        // id 1 re-dials; id 2 is still cached.
        let _ = pool.get(1).await.unwrap();
        let _ = pool.get(2).await.unwrap();
        assert!(dials.load(Ordering::Relaxed) == 3);
    }

    #[tokio::test]
    async fn bootstrap_connection_caches_and_skips_dead_addresses() {
        let dials = Arc::new(AtomicUsize::new(0));
        // First bootstrap address is dead; the second must win.
        let pool = BrokerPool::with_connector(
            vec![addr(1111), addr(2222)],
            CountingConnector {
                dials: dials.clone(),
                fail: vec![addr(1111)],
            },
        );
        let boot = pool.bootstrap_connection().await.unwrap();
        assert!(boot.addr == addr(2222));
        // Two dials: the dead first address, then the live second.
        assert!(dials.load(Ordering::Relaxed) == 2);

        // Cached under the synthetic bootstrap id; a second call does not redial.
        let again = pool.bootstrap_connection().await.unwrap();
        check!(Arc::ptr_eq(&boot, &again));
        check!(dials.load(Ordering::Relaxed) == 2);
        check!(pool.by_id.contains_key(&BOOTSTRAP_ID));
    }

    #[tokio::test]
    async fn bootstrap_id_does_not_collide_with_real_broker_ids() {
        // The bootstrap connection is keyed under a synthetic id that no real
        // broker id (>= 0) can equal, so `evict(0)` must not disturb it.
        let dials = Arc::new(AtomicUsize::new(0));
        let pool = BrokerPool::with_connector(
            vec![addr(2222)],
            CountingConnector {
                dials: dials.clone(),
                fail: vec![],
            },
        );
        let _ = pool.bootstrap_connection().await.unwrap();
        assert!(pool.by_id.contains_key(&BOOTSTRAP_ID));
        // BOOTSTRAP_ID must be negative; a real broker id is never negative.
        assert!(BOOTSTRAP_ID < 0);

        // Evicting any real id leaves the bootstrap connection intact.
        pool.evict(0);
        pool.evict(1);
        assert!(pool.by_id.contains_key(&BOOTSTRAP_ID));
    }

    #[tokio::test]
    async fn evict_bootstrap_drops_only_the_bootstrap_connection() {
        let dials = Arc::new(AtomicUsize::new(0));
        let pool = BrokerPool::with_connector(
            vec![addr(2222)],
            CountingConnector {
                dials: dials.clone(),
                fail: vec![],
            },
        );
        pool.by_addr.insert(3, addr(9092));
        let _ = pool.get(3).await.unwrap();
        let _ = pool.bootstrap_connection().await.unwrap();
        assert!(pool.by_id.contains_key(&BOOTSTRAP_ID));
        assert!(pool.by_id.contains_key(&3));

        pool.evict_bootstrap();
        // Only the bootstrap entry is gone; the real broker stays cached.
        assert!(!pool.by_id.contains_key(&BOOTSTRAP_ID));
        assert!(pool.by_id.contains_key(&3));

        // The next bootstrap_connection redials.
        let before = dials.load(Ordering::Relaxed);
        let _ = pool.bootstrap_connection().await.unwrap();
        assert!(dials.load(Ordering::Relaxed) == before + 1);
    }

    #[tokio::test]
    async fn replace_bootstrap_addresses_forces_redial_to_new_address() {
        let dials = Arc::new(AtomicUsize::new(0));
        let pool = BrokerPool::with_connector(
            vec![addr(1111)],
            CountingConnector {
                dials: dials.clone(),
                fail: vec![],
            },
        );

        let first = pool.bootstrap_connection().await.unwrap();
        assert!(first.addr == addr(1111));
        assert!(dials.load(Ordering::Relaxed) == 1);

        pool.replace_bootstrap(vec![addr(2222)]);

        let second = pool.bootstrap_connection().await.unwrap();
        assert!(second.addr == addr(2222));
        assert!(dials.load(Ordering::Relaxed) == 2);
    }

    #[tokio::test]
    async fn close_all_releases_every_cached_connection() {
        let dials = Arc::new(AtomicUsize::new(0));
        let pool = BrokerPool::with_connector(
            vec![addr(2222)],
            CountingConnector {
                dials: dials.clone(),
                fail: vec![],
            },
        );
        pool.by_addr.insert(1, addr(9092));
        let held = pool.get(1).await.unwrap();
        let _ = pool.bootstrap_connection().await.unwrap();
        // Two strong refs to broker 1's conn: the pool's and `held`.
        assert!(Arc::strong_count(&held) == 2);

        pool.close_all();
        // The pool dropped its references; only `held` remains.
        assert!(Arc::strong_count(&held) == 1);
    }
}
