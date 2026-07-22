//! Chaos TCP proxy: every cluster endpoint sits behind one of these.
//!
//! The proxy is a byte pipe (TLS and pgwire pass through untouched) with
//! dynamically reconfigurable fault behavior:
//!
//! - **Partition** — [`PartitionStyle::Blackhole`] pauses pumping: no bytes
//!   are read from either side, so kernel buffers fill and peers stall
//!   exactly as they would on a real partition; live connections survive a
//!   heal. [`PartitionStyle::Reset`] closes live connections and refuses new
//!   ones. New connections during a blackhole are accepted but not connected
//!   to the backend until the partition heals.
//! - **Latency** — each chunk read is stamped with a delivery time of
//!   `read_time + base + uniform(0..=jitter)` and forwarded no earlier than
//!   that, preserving order and pipelining (a busy stream is delayed, not
//!   serialized). Applied independently in each direction, so a
//!   request/response pair sees roughly twice the configured delay.
//! - **Throttle** — a per-direction token bucket caps forwarded bytes per
//!   second.
//!
//! All controls take effect on live connections without reconnecting.

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use rand::RngExt as _;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
    time::Instant,
};

use crate::scenario::PartitionStyle;

/// Largest chunk read from a socket in one pass. Equal to
/// [`MIN_BURST_BYTES`], so a single chunk always fits the token bucket.
const CHUNK_LEN: usize = 64 * 1024;

/// Smallest token-bucket capacity in bytes, regardless of how low the
/// configured rate is.
const MIN_BURST_BYTES: u64 = 64 * 1024;

/// Depth of the per-direction delay queue, in chunks. Deep enough that a
/// busy stream under high configured latency keeps pipelining rather than
/// serializing on the delay.
const DELAY_QUEUE_DEPTH: usize = 256;

/// One-way delay applied to a proxied link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatencySpec {
    /// Base one-way delay in milliseconds.
    pub ms: u64,
    /// Uniform jitter in milliseconds added on top of the base.
    pub jitter_ms: u64,
}

/// A chaos TCP proxy listening on an OS-assigned localhost port.
///
/// Dropping the proxy aborts its accept loop and every live connection.
#[derive(Debug)]
pub struct ChaosProxy {
    addr: SocketAddr,
    backend: watch::Sender<SocketAddr>,
    latency: watch::Sender<Option<LatencySpec>>,
    throttle: watch::Sender<Option<u64>>,
    commands: mpsc::Sender<PartitionCommand>,
    task: JoinHandle<()>,
}

impl ChaosProxy {
    /// Spawns a proxy forwarding to `backend`.
    ///
    /// # Errors
    ///
    /// Returns an error if the listener cannot bind.
    pub async fn spawn(backend: SocketAddr) -> std::io::Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr = listener.local_addr()?;
        let (backend_tx, backend_rx) = watch::channel(backend);
        let (latency_tx, latency_rx) = watch::channel(None);
        let (throttle_tx, throttle_rx) = watch::channel(None);
        let (commands, command_rx) = mpsc::channel(8);
        let task = tokio::spawn(run(
            listener,
            command_rx,
            backend_rx,
            latency_rx,
            throttle_rx,
        ));
        Ok(Self {
            addr,
            backend: backend_tx,
            latency: latency_tx,
            throttle: throttle_tx,
            commands,
            task,
        })
    }

    /// The address clients should dial.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Repoints the proxy at a new backend (used after a node restart).
    /// Existing connections keep their old backend; new ones use the new
    /// address.
    pub fn set_backend(&self, backend: SocketAddr) {
        self.backend.send_replace(backend);
    }

    /// Applies or clears a partition. `Some(style)` cuts the link;
    /// `None` heals it. Completes once the state change has taken effect.
    pub async fn set_partitioned(&self, style: Option<PartitionStyle>) {
        let (acked, ack) = oneshot::channel();
        if self
            .commands
            .send(PartitionCommand { style, acked })
            .await
            .is_err()
        {
            return;
        }
        let _ = ack.await;
    }

    /// Applies or clears one-way delay on both directions.
    pub fn set_latency(&self, latency: Option<LatencySpec>) {
        self.latency.send_replace(latency);
    }

    /// Applies or clears a per-direction bandwidth cap in bytes per second.
    pub fn set_throttle_bytes_per_sec(&self, limit: Option<u64>) {
        self.throttle.send_replace(limit);
    }
}

impl Drop for ChaosProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A partition state change plus the ack the control loop fires once the
/// change has taken effect.
#[derive(Debug)]
struct PartitionCommand {
    style: Option<PartitionStyle>,
    acked: oneshot::Sender<()>,
}

/// Accept-and-control loop: owns the listener, the partition state, and the
/// set of live connection tasks.
async fn run(
    listener: TcpListener,
    mut commands: mpsc::Receiver<PartitionCommand>,
    backend: watch::Receiver<SocketAddr>,
    latency: watch::Receiver<Option<LatencySpec>>,
    throttle: watch::Receiver<Option<u64>>,
) {
    let (partition, _) = watch::channel(None);
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((client, _)) = accepted else {
                    return;
                };
                if *partition.borrow() == Some(PartitionStyle::Reset) {
                    // Accept-then-close: the peer sees an immediate
                    // disconnect, as from an administratively-down endpoint.
                    drop(client);
                } else {
                    connections.spawn(handle_connection(
                        client,
                        partition.subscribe(),
                        backend.clone(),
                        latency.clone(),
                        throttle.clone(),
                    ));
                }
            }
            command = commands.recv() => {
                let Some(PartitionCommand { style, acked }) = command else {
                    return;
                };
                partition.send_replace(style);
                if style == Some(PartitionStyle::Reset) {
                    connections.abort_all();
                    while connections.join_next().await.is_some() {}
                }
                let _ = acked.send(());
            }
            _ = connections.join_next(), if !connections.is_empty() => {}
        }
    }
}

/// Serves one accepted client connection: waits out any blackhole in
/// progress, dials the backend, then pumps bytes both ways until either side
/// closes.
async fn handle_connection(
    client: TcpStream,
    mut partition: watch::Receiver<Option<PartitionStyle>>,
    backend: watch::Receiver<SocketAddr>,
    latency: watch::Receiver<Option<LatencySpec>>,
    throttle: watch::Receiver<Option<u64>>,
) {
    // A connection accepted mid-blackhole is left dangling (accepted, no
    // backend) until the heal, exactly like a SYN that squeezed through just
    // before the partition started.
    if !wait_until_pumping(&mut partition).await {
        return;
    }
    let backend_addr = *backend.borrow();
    let Ok(server) = TcpStream::connect(backend_addr).await else {
        return;
    };
    let _ = client.set_nodelay(true);
    let _ = server.set_nodelay(true);
    let (client_read, client_write) = client.into_split();
    let (server_read, server_write) = server.into_split();
    tokio::join!(
        pump(
            client_read,
            server_write,
            partition.clone(),
            latency.clone(),
            throttle.clone(),
        ),
        pump(server_read, client_write, partition, latency, throttle),
    );
}

/// Waits while the link is blackholed. Returns `false` when the proxy has
/// shut down (partition channel closed) and pumping should stop.
async fn wait_until_pumping(partition: &mut watch::Receiver<Option<PartitionStyle>>) -> bool {
    loop {
        if *partition.borrow_and_update() != Some(PartitionStyle::Blackhole) {
            return true;
        }
        if partition.changed().await.is_err() {
            return false;
        }
    }
}

/// Pumps one direction of a proxied connection through a delay queue: a
/// read side stamps, throttles, and enqueues chunks; a write side delivers
/// each chunk once its deadline passes.
async fn pump(
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    partition: watch::Receiver<Option<PartitionStyle>>,
    latency: watch::Receiver<Option<LatencySpec>>,
    throttle: watch::Receiver<Option<u64>>,
) {
    let (queue_tx, queue_rx) = mpsc::channel(DELAY_QUEUE_DEPTH);
    tokio::join!(
        read_side(reader, queue_tx, partition, latency, throttle),
        write_side(queue_rx, writer),
    );
}

/// Read half of a pump: gates on the blackhole state before every read, then
/// stamps each chunk with its delivery deadline, charges the token bucket,
/// and enqueues it for the write side.
async fn read_side(
    mut reader: OwnedReadHalf,
    queue: mpsc::Sender<(Vec<u8>, Instant)>,
    mut partition: watch::Receiver<Option<PartitionStyle>>,
    latency: watch::Receiver<Option<LatencySpec>>,
    throttle: watch::Receiver<Option<u64>>,
) {
    let mut bucket = TokenBucket::new();
    let mut buf = vec![0_u8; CHUNK_LEN];
    loop {
        if !wait_until_pumping(&mut partition).await {
            return;
        }
        // `biased` polls the partition watch first, so once a blackhole is
        // acknowledged no further chunk can win the race against it; a
        // pending read is dropped without consuming bytes (kernel buffers
        // keep them for after the heal).
        let outcome = tokio::select! {
            biased;
            changed = partition.changed() => {
                if changed.is_err() {
                    return;
                }
                continue;
            }
            outcome = reader.read(&mut buf) => outcome,
        };
        let len = match outcome {
            Ok(0) | Err(_) => return,
            Ok(len) => len,
        };
        let deliver_at = delivery_instant(Instant::now(), *latency.borrow());
        bucket
            .acquire(&throttle, u64::try_from(len).unwrap_or(u64::MAX))
            .await;
        if queue.send((buf[..len].to_vec(), deliver_at)).await.is_err() {
            return;
        }
    }
}

/// Write half of a pump: delivers queued chunks in order, each no earlier
/// than its deadline, then propagates the half-close once the read side is
/// done.
async fn write_side(mut queue: mpsc::Receiver<(Vec<u8>, Instant)>, mut writer: OwnedWriteHalf) {
    while let Some((chunk, deliver_at)) = queue.recv().await {
        tokio::time::sleep_until(deliver_at).await;
        if writer.write_all(&chunk).await.is_err() {
            return;
        }
    }
    let _ = writer.shutdown().await;
}

/// Delivery deadline for a chunk read at `read_at` under `latency`.
fn delivery_instant(read_at: Instant, latency: Option<LatencySpec>) -> Instant {
    let Some(LatencySpec { ms, jitter_ms }) = latency else {
        return read_at;
    };
    let jitter = rand::rng().random_range(0..=jitter_ms);
    read_at + Duration::from_millis(ms.saturating_add(jitter))
}

/// Token bucket pacing one pump direction. Consulted per chunk against the
/// live throttle setting; refills continuously at the configured rate up to
/// a burst of one tenth of the rate (at least [`MIN_BURST_BYTES`]).
struct TokenBucket {
    tokens: u64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new() -> Self {
        Self {
            tokens: 0,
            last_refill: Instant::now(),
        }
    }

    /// Waits until `needed` bytes of budget are available under the current
    /// throttle setting and consumes them. Returns immediately when the
    /// throttle is off.
    async fn acquire(&mut self, throttle: &watch::Receiver<Option<u64>>, needed: u64) {
        loop {
            let Some(limit) = *throttle.borrow() else {
                return;
            };
            let limit = limit.max(1);
            let burst = (limit / 10).max(MIN_BURST_BYTES);
            // A chunk never exceeds `CHUNK_LEN`, which equals the minimum
            // burst, but clamp anyway so an oversized request cannot spin
            // against a bucket it could never fill.
            let needed = needed.min(burst);
            let now = Instant::now();
            let elapsed = now.duration_since(self.last_refill);
            let earned = u128::from(limit).saturating_mul(elapsed.as_micros()) / 1_000_000;
            self.tokens = self
                .tokens
                .saturating_add(u64::try_from(earned).unwrap_or(u64::MAX))
                .min(burst);
            self.last_refill = now;
            if self.tokens >= needed {
                self.tokens -= needed;
                return;
            }
            let deficit = u128::from(needed - self.tokens);
            let wait_micros = deficit
                .saturating_mul(1_000_000)
                .div_ceil(u128::from(limit));
            let wait = Duration::from_micros(u64::try_from(wait_micros).unwrap_or(u64::MAX));
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use tokio::time::timeout;

    use super::*;

    async fn spawn_echo(map: fn(u8) -> u8) -> SocketAddr {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind echo listener");
        let addr = listener.local_addr().expect("echo listener address");
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let _ = socket.set_nodelay(true);
                    let mut buf = vec![0_u8; CHUNK_LEN];
                    loop {
                        match socket.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(len) => {
                                for byte in &mut buf[..len] {
                                    *byte = map(*byte);
                                }
                                if socket.write_all(&buf[..len]).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                });
            }
        });
        addr
    }

    async fn connect(proxy: &ChaosProxy) -> TcpStream {
        let stream = TcpStream::connect(proxy.addr())
            .await
            .expect("connect to proxy");
        stream.set_nodelay(true).expect("set nodelay");
        stream
    }

    async fn read_exactly(stream: &mut TcpStream, len: usize, wait: Duration) -> Vec<u8> {
        let mut buf = vec![0_u8; len];
        timeout(wait, stream.read_exact(&mut buf))
            .await
            .expect("read timed out")
            .expect("read failed");
        buf
    }

    async fn echo_round_trip(stream: &mut TcpStream, payload: &[u8]) {
        stream.write_all(payload).await.expect("write payload");
        let echoed = read_exactly(stream, payload.len(), Duration::from_secs(5)).await;
        assert!(echoed == payload);
    }

    #[tokio::test]
    async fn forwards_bytes_unchanged() {
        let echo = spawn_echo(std::convert::identity).await;
        let proxy = ChaosProxy::spawn(echo).await.expect("spawn proxy");
        let mut conn = connect(&proxy).await;
        let payload: Vec<u8> = (0..=255_u8).cycle().take(4096).collect();
        echo_round_trip(&mut conn, &payload).await;
    }

    #[tokio::test]
    async fn latency_delays_round_trips() {
        let echo = spawn_echo(std::convert::identity).await;
        let proxy = ChaosProxy::spawn(echo).await.expect("spawn proxy");
        proxy.set_latency(Some(LatencySpec {
            ms: 50,
            jitter_ms: 0,
        }));
        let mut conn = connect(&proxy).await;

        let start = Instant::now();
        echo_round_trip(&mut conn, b"ping").await;
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(100));
        assert!(elapsed < Duration::from_millis(700));

        // Retuning latency applies to the live connection; jitter keeps the
        // round trip within base..=base+jitter per leg.
        proxy.set_latency(Some(LatencySpec {
            ms: 10,
            jitter_ms: 20,
        }));
        let start = Instant::now();
        echo_round_trip(&mut conn, b"pong").await;
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(20));
        assert!(elapsed < Duration::from_millis(700));
    }

    #[tokio::test]
    async fn latency_preserves_pipelining() {
        let echo = spawn_echo(std::convert::identity).await;
        let proxy = ChaosProxy::spawn(echo).await.expect("spawn proxy");
        proxy.set_latency(Some(LatencySpec {
            ms: 50,
            jitter_ms: 0,
        }));
        let mut conn = connect(&proxy).await;
        let chunk = vec![0x5a_u8; 1024];

        // Twenty dependent round trips pay the full delay every time.
        let sequential_start = Instant::now();
        for _ in 0..20 {
            echo_round_trip(&mut conn, &chunk).await;
        }
        let sequential = sequential_start.elapsed();

        // Twenty chunks written back-to-back pipeline through the delay.
        let burst_start = Instant::now();
        conn.write_all(&vec![0x5a_u8; 20 * 1024])
            .await
            .expect("write burst");
        read_exactly(&mut conn, 20 * 1024, Duration::from_secs(5)).await;
        let burst = burst_start.elapsed();

        assert!(sequential >= Duration::from_secs(2));
        assert!(burst >= Duration::from_millis(100));
        assert!(burst < sequential / 4);
    }

    #[tokio::test]
    async fn throttle_caps_bandwidth() {
        let echo = spawn_echo(std::convert::identity).await;
        let proxy = ChaosProxy::spawn(echo).await.expect("spawn proxy");
        proxy.set_throttle_bytes_per_sec(Some(64 * 1024));
        let mut conn = connect(&proxy).await;

        let payload = vec![0x42_u8; 192 * 1024];
        let start = Instant::now();
        let (mut reader, mut writer) = conn.split();
        let write = async move {
            writer.write_all(&payload).await.expect("write payload");
        };
        let read = async {
            let mut buf = vec![0_u8; 192 * 1024];
            timeout(Duration::from_secs(15), reader.read_exact(&mut buf))
                .await
                .expect("throttled echo timed out")
                .expect("read echoed payload");
        };
        tokio::join!(write, read);
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(1900));
        assert!(elapsed < Duration::from_secs(20));
    }

    #[tokio::test]
    async fn blackhole_stalls_then_heals_in_place() {
        let echo = spawn_echo(std::convert::identity).await;
        let proxy = ChaosProxy::spawn(echo).await.expect("spawn proxy");
        let mut conn = connect(&proxy).await;
        echo_round_trip(&mut conn, b"before").await;

        proxy.set_partitioned(Some(PartitionStyle::Blackhole)).await;
        conn.write_all(b"stalled")
            .await
            .expect("write during blackhole");
        let mut buf = [0_u8; 7];
        let stalled = timeout(Duration::from_millis(400), conn.read(&mut buf)).await;
        assert!(stalled.is_err(), "bytes must stall during a blackhole");

        // A connection opened during the blackhole is accepted but sees no
        // backend until the heal.
        let mut late = connect(&proxy).await;
        late.write_all(b"later")
            .await
            .expect("write on late connection");
        let late_read = timeout(Duration::from_millis(300), late.read(&mut buf)).await;
        assert!(late_read.is_err(), "late connection must stall too");

        proxy.set_partitioned(None).await;
        // The stalled bytes arrive on the SAME connection, which keeps
        // working afterwards.
        let echoed = read_exactly(&mut conn, 7, Duration::from_secs(5)).await;
        assert!(echoed == b"stalled");
        echo_round_trip(&mut conn, b"after").await;
        let late_echoed = read_exactly(&mut late, 5, Duration::from_secs(5)).await;
        assert!(late_echoed == b"later");
    }

    #[tokio::test]
    async fn reset_closes_live_and_new_connections_until_heal() {
        let echo = spawn_echo(std::convert::identity).await;
        let proxy = ChaosProxy::spawn(echo).await.expect("spawn proxy");
        let mut conn = connect(&proxy).await;
        echo_round_trip(&mut conn, b"before").await;

        proxy.set_partitioned(Some(PartitionStyle::Reset)).await;
        let mut buf = [0_u8; 16];
        let live = timeout(Duration::from_secs(2), conn.read(&mut buf))
            .await
            .expect("live connection must be closed promptly");
        assert!(let (Ok(0) | Err(_)) = live);

        // A new connection is accepted, then immediately closed.
        let mut refused = TcpStream::connect(proxy.addr())
            .await
            .expect("connect during reset");
        let refused_read = timeout(Duration::from_secs(2), refused.read(&mut buf))
            .await
            .expect("new connection must be closed promptly");
        assert!(let (Ok(0) | Err(_)) = refused_read);

        proxy.set_partitioned(None).await;
        let mut healed = connect(&proxy).await;
        echo_round_trip(&mut healed, b"after").await;
    }

    #[tokio::test]
    async fn set_backend_affects_only_new_connections() {
        let original = spawn_echo(std::convert::identity).await;
        let replacement = spawn_echo(|byte| byte.wrapping_add(1)).await;
        let proxy = ChaosProxy::spawn(original).await.expect("spawn proxy");
        let mut old_conn = connect(&proxy).await;
        echo_round_trip(&mut old_conn, b"abc").await;

        proxy.set_backend(replacement);
        // The live connection keeps its original backend.
        echo_round_trip(&mut old_conn, b"def").await;
        // A new connection dials the replacement.
        let mut new_conn = connect(&proxy).await;
        new_conn.write_all(b"abc").await.expect("write");
        let echoed = read_exactly(&mut new_conn, 3, Duration::from_secs(5)).await;
        assert!(echoed == b"bcd");
    }
}
