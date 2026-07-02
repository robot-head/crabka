//! Broker-only metadata observer (Component B).
//!
//! A broker-only `KRaft` node is not an openraft voter — it keeps its
//! `MetadataImage` current by *fetching* the committed `__cluster_metadata`
//! log from the controller quorum over `API_KEY_METADATA_FETCH`, decoding
//! each record batch through the `crabka_metadata` Kafka-record bridge, and
//! applying records exactly as the controller state machine would.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crabka_metadata::{MetadataImage, from_kraft_value};
use crabka_protocol::records::RecordBatch;
use crabka_raft::{NodeId, OutboundDialer};

/// Static configuration for the observer.
#[derive(Clone)]
pub struct ObserverConfig {
    /// Controller-listener voter map `(id, "<host>:<port>")` from
    /// `controller_quorum_voters`. The host is carried verbatim; the dialer
    /// re-resolves it per connect so a rejoining peer's new pod IP is reached.
    pub voters: Vec<(NodeId, String)>,
    /// Outbound dialer (same TLS/SASL path as the raft transport).
    pub dialer: Arc<dyn OutboundDialer>,
    /// `client_id` for the dial handshake.
    pub client_id: String,
    /// Cluster UUID for the initial empty image.
    pub cluster_id: uuid::Uuid,
    /// Soft cap per fetch.
    pub max_bytes: u32,
    /// Idle poll interval once caught up to the high watermark.
    pub poll_interval: Duration,
}

/// Handle to a running observer. Holds the image watch and the background
/// fetch task.
pub struct MetadataObserver {
    image: watch::Sender<Arc<MetadataImage>>,
    leader: watch::Sender<Option<NodeId>>,
    shutdown: CancellationToken,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl MetadataObserver {
    /// Start the observer loop. The image watch begins at an empty image
    /// for `cluster_id`; callers subscribe via [`Self::watch_image`].
    #[must_use]
    pub fn start(config: ObserverConfig) -> Arc<Self> {
        let (image_tx, _) = watch::channel(Arc::new(MetadataImage::new(config.cluster_id)));
        let (leader_tx, _) = watch::channel(None);
        let shutdown = CancellationToken::new();
        let observer = Arc::new(Self {
            image: image_tx,
            leader: leader_tx,
            shutdown: shutdown.clone(),
            task: tokio::sync::Mutex::new(None),
        });
        let task = tokio::spawn(run_loop(config, observer.clone(), shutdown));
        if let Ok(mut guard) = observer.task.try_lock() {
            *guard = Some(task);
        }
        observer
    }

    #[must_use]
    pub fn current_image(&self) -> Arc<MetadataImage> {
        self.image.borrow().clone()
    }

    #[must_use]
    pub fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.image.subscribe()
    }

    #[must_use]
    pub fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.leader.subscribe()
    }

    /// Stop the fetch loop and drain the task.
    pub async fn cancel(&self) {
        self.shutdown.cancel();
        if let Some(h) = self.task.lock().await.take() {
            let _ = h.await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn task_drained_for_test(&self) -> bool {
        self.task.lock().await.is_none()
    }
}

/// One iteration: fetch from `addr` at `fetch_offset`, decode + apply, and
/// return the new fetch offset (or `None` on a transport error so the
/// caller fails over).
async fn fetch_once(
    config: &ObserverConfig,
    addr: &str,
    target: NodeId,
    fetch_offset: u64,
    image_tx: &watch::Sender<Arc<MetadataImage>>,
) -> Option<u64> {
    let req = crabka_raft::CrabkaMetadataFetchRequest {
        fetch_offset: i64::try_from(fetch_offset).unwrap_or(i64::MAX),
        max_bytes: i32::try_from(config.max_bytes).unwrap_or(i32::MAX),
    };
    let mut body = Vec::with_capacity(12);
    req.encode_v0(&mut body);

    let opts = crabka_client_core::ConnectionOptions {
        client_id: config.client_id.clone(),
        ..crabka_client_core::ConnectionOptions::default()
    };
    let conn = match config.dialer.dial(target, addr, opts).await {
        Ok(c) => c,
        Err(e) => {
            debug!(%addr, error = %e, "observer dial failed");
            return None;
        }
    };
    let resp_body = match conn
        .raw_request(
            crabka_raft::API_KEY_METADATA_FETCH,
            0,
            bytes::Bytes::from(body),
        )
        .await
    {
        Ok(b) => b,
        Err(e) => {
            debug!(%addr, error = %e, "observer fetch request failed");
            conn.close();
            return None;
        }
    };
    conn.close();

    let mut cur: &[u8] = &resp_body;
    let resp = match crabka_raft::CrabkaMetadataFetchResponse::decode_v0(&mut cur) {
        Ok(r) => r,
        Err(e) => {
            warn!(%addr, error = %e, "observer response decode failed");
            return None;
        }
    };
    if resp.error_code != 0 {
        return None;
    }

    Some(apply_fetch_records(fetch_offset, &resp.records, image_tx))
}

fn apply_fetch_records(
    fetch_offset: u64,
    records: &[u8],
    image_tx: &watch::Sender<Arc<MetadataImage>>,
) -> u64 {
    // No new records: the controller had nothing past `fetch_offset`. Skip the
    // expensive full-image clone entirely.
    if records.is_empty() {
        return fetch_offset;
    }

    let mut next: MetadataImage = (**image_tx.borrow()).clone();
    let mut new_offset = fetch_offset;
    let mut buf: &[u8] = records;
    while !buf.is_empty() {
        let batch = match RecordBatch::decode(&mut buf) {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "observer batch decode failed");
                break;
            }
        };
        let index = u64::try_from(batch.base_offset.max(0)).unwrap_or(0);
        // The LeaderChange control batch carries no metadata records.
        if batch.attributes.is_control_batch() {
            new_offset = index + 1;
            continue;
        }
        for r in &batch.records {
            let Some(value) = r.value.as_ref() else {
                continue;
            };
            match from_kraft_value(value, &next) {
                Ok(rec) => {
                    if let Err(e) = next.validate(&rec) {
                        warn!(error = %e, "observer skipped record failing validation");
                        continue;
                    }
                    next.apply(&rec);
                }
                Err(e) => warn!(error = %e, "observer failed to decode record"),
            }
        }
        new_offset = index + 1;
    }
    if new_offset != fetch_offset {
        let _ = image_tx.send_replace(Arc::new(next));
    }
    new_offset.max(fetch_offset)
}

/// Round-robin pick into a non-empty voter list: index `idx` wrapped by the
/// list length. Pulled out of the serve-loop so the wrap-around is unit-tested
/// (a `/`-for-`%` slip would stop the observer rotating to the next voter when
/// the current one is unreachable, stranding it on a dead voter).
fn voter_at(voters: &[(NodeId, String)], idx: usize) -> &(NodeId, String) {
    &voters[idx % voters.len()]
}

async fn run_loop(
    config: ObserverConfig,
    observer: Arc<MetadataObserver>,
    shutdown: CancellationToken,
) {
    let mut fetch_offset: u64 = 0;
    let mut target_idx: usize = 0;
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        if config.voters.is_empty() {
            tokio::time::sleep(config.poll_interval).await;
            continue;
        }
        let (target, addr) = voter_at(&config.voters, target_idx).clone();
        let result = tokio::select! {
            () = shutdown.cancelled() => return,
            r = fetch_once(&config, &addr, target, fetch_offset, &observer.image) => r,
        };
        if let Some(new_offset) = result {
            let _ = observer.leader.send_replace(Some(target));
            if new_offset == fetch_offset {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(config.poll_interval) => {}
                }
            } else {
                fetch_offset = new_offset;
            }
        } else {
            target_idx = target_idx.wrapping_add(1);
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(config.poll_interval) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use bytes::{Bytes, BytesMut};
    use crabka_metadata::{MetadataRecord, TopicRecord, to_kraft_values};
    use crabka_protocol::Encode;
    use crabka_protocol::owned::api_versions_request;
    use crabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};
    use crabka_protocol::records::header::Attributes;
    use crabka_protocol::records::{Record, RecordBatch};
    use crabka_raft::{BootstrapMode, Controller, ControllerConfig};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;
    use uuid::Uuid;

    #[derive(Clone)]
    struct RecordingDialer {
        client_ids: Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl OutboundDialer for RecordingDialer {
        async fn dial(
            &self,
            target: NodeId,
            addr: &str,
            options: crabka_client_core::ConnectionOptions,
        ) -> Result<crabka_client_core::Connection, crabka_client_core::ClientError> {
            self.client_ids
                .lock()
                .unwrap()
                .push(options.client_id.clone());
            crabka_raft::PlaintextDialer
                .dial(target, addr, options)
                .await
        }
    }

    #[derive(Clone)]
    struct CountingDialer {
        dial_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl OutboundDialer for CountingDialer {
        async fn dial(
            &self,
            target: NodeId,
            addr: &str,
            options: crabka_client_core::ConnectionOptions,
        ) -> Result<crabka_client_core::Connection, crabka_client_core::ClientError> {
            self.dial_count.fetch_add(1, Ordering::SeqCst);
            crabka_raft::PlaintextDialer
                .dial(target, addr, options)
                .await
        }
    }

    fn api_versions_response_v0() -> Vec<u8> {
        let resp = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![ApiVersion {
                api_key: api_versions_request::API_KEY,
                min_version: 0,
                max_version: 3,
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    fn metadata_fetch_response_body(records: Bytes) -> Vec<u8> {
        let mut out = vec![0u8]; // flexible ResponseHeader v1 tagged-fields
        crabka_raft::CrabkaMetadataFetchResponse {
            error_code: 0,
            leader_hint: 1,
            log_start_offset: 0,
            high_watermark: 0,
            records,
        }
        .encode_v0(&mut out)
        .unwrap();
        out
    }

    fn topic_record(name: &str) -> MetadataRecord {
        MetadataRecord::V1Topic(TopicRecord {
            name: name.into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        })
    }

    fn metadata_batch(base_offset: i64, rec: &MetadataRecord) -> RecordBatch {
        let values = to_kraft_values(rec, &MetadataImage::new(Uuid::nil())).expect("to kraft");
        let records: Vec<Record> = values
            .into_iter()
            .enumerate()
            .map(|(idx, value)| Record {
                offset_delta: i32::try_from(idx).unwrap(),
                value: Some(value),
                ..Default::default()
            })
            .collect();
        RecordBatch {
            base_offset,
            last_offset_delta: i32::try_from(records.len().saturating_sub(1)).unwrap(),
            records,
            ..Default::default()
        }
    }

    fn control_batch(base_offset: i64) -> RecordBatch {
        RecordBatch {
            base_offset,
            attributes: Attributes::default().with_control(true),
            last_offset_delta: 0,
            records: vec![Record {
                offset_delta: 0,
                value: Some(Bytes::from_static(b"leader-change")),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn encode_batches(batches: &[RecordBatch]) -> Bytes {
        let mut out = Vec::new();
        for batch in batches {
            batch.encode(&mut out).expect("encode batch");
        }
        Bytes::from(out)
    }

    fn image_channel(cluster_id: Uuid) -> watch::Sender<Arc<MetadataImage>> {
        let (tx, _) = watch::channel(Arc::new(MetadataImage::new(cluster_id)));
        tx
    }

    /// Per-fetch soft byte cap used by every observer fixture (1 MiB).
    const TEST_MAX_FETCH_BYTES: u32 = 1_048_576;

    #[test]
    fn voter_at_wraps_round_robin_by_modulo() {
        let voters = vec![
            (1u64, "a:9093".to_string()),
            (2u64, "b:9093".to_string()),
            (3u64, "c:9093".to_string()),
        ];
        // In-range picks each distinct voter. `idx / len` (the `%`→`/` mutant)
        // would collapse 1 and 2 to index 0 ("a"), so distinguishing 0/1/2 here
        // proves the modulo, not integer division, indexes the list.
        // Wrap-around: index 3 must rotate back to the first voter (3 % 3 == 0);
        // `3 / 3 == 1` would return the second voter instead.
        let cases = [(0usize, 1u64), (1, 2), (2, 3), (3, 1), (4, 2)];
        for (idx, expected_id) in cases {
            assert!(voter_at(&voters, idx).0 == expected_id, "idx {idx}");
        }
    }

    #[test]
    fn apply_fetch_records_advances_past_control_batch() {
        let image_tx = image_channel(Uuid::new_v4());
        let records = encode_batches(&[control_batch(6)]);

        let new_offset = apply_fetch_records(6, &records, &image_tx);

        assert!(new_offset == 7);
    }

    #[test]
    fn apply_fetch_records_advances_data_batch_offset_and_publishes() {
        let image_tx = image_channel(Uuid::new_v4());
        let records = encode_batches(&[metadata_batch(4, &topic_record("offset-topic"))]);

        let new_offset = apply_fetch_records(4, &records, &image_tx);

        assert!(new_offset == 5);
        assert!(image_tx.borrow().topic("offset-topic").is_some());
    }

    #[tokio::test]
    async fn cancel_drains_background_task() {
        let observer = MetadataObserver::start(ObserverConfig {
            voters: vec![],
            dialer: Arc::new(crabka_raft::PlaintextDialer),
            client_id: "cancel-test".into(),
            cluster_id: Uuid::nil(),
            max_bytes: TEST_MAX_FETCH_BYTES,
            poll_interval: Duration::from_mins(1),
        });

        observer.cancel().await;

        assert!(observer.task.lock().await.is_none());
    }

    #[tokio::test]
    async fn run_loop_sleeps_after_empty_fetch() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let fetches_for_mock = fetches.clone();
        let mock =
            crabka_client_core::MockBroker::start(move |api_key, _version, _corr_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(api_versions_response_v0());
                }
                if api_key == crabka_raft::API_KEY_METADATA_FETCH {
                    fetches_for_mock.fetch_add(1, Ordering::SeqCst);
                    return Some(metadata_fetch_response_body(Bytes::new()));
                }
                None
            })
            .await;
        let dial_count = Arc::new(AtomicUsize::new(0));
        let observer = MetadataObserver::start(ObserverConfig {
            voters: vec![(1, mock.addr.to_string())],
            dialer: Arc::new(CountingDialer {
                dial_count: dial_count.clone(),
            }),
            client_id: "sleep-test".into(),
            cluster_id: Uuid::nil(),
            max_bytes: TEST_MAX_FETCH_BYTES,
            poll_interval: Duration::from_millis(250),
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            while fetches.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("observer should issue the first fetch");
        let after_first_fetch = fetches.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(fetches.load(Ordering::SeqCst) == after_first_fetch);
        assert!(dial_count.load(Ordering::SeqCst) == after_first_fetch);

        observer.cancel().await;
        mock.stop();
    }

    #[tokio::test]
    async fn observer_replicates_committed_topic() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        let mut leader_rx = ctrl.watch_leader();
        while leader_rx.borrow().is_none() {
            leader_rx.changed().await.unwrap();
        }
        let ctrl_addr = ctrl.controller_bound_addr();
        ctrl.submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
            name: "observed".into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        })])
        .await
        .expect("submit");
        let client_ids = Arc::new(std::sync::Mutex::new(Vec::new()));

        let observer = MetadataObserver::start(ObserverConfig {
            voters: vec![(1, ctrl_addr.to_string())],
            dialer: Arc::new(RecordingDialer {
                client_ids: client_ids.clone(),
            }),
            client_id: "test-observer".into(),
            cluster_id: Uuid::nil(),
            max_bytes: TEST_MAX_FETCH_BYTES,
            poll_interval: Duration::from_millis(50),
        });

        let mut img_rx = observer.watch_image();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if img_rx.borrow().topic("observed").is_some() {
                break;
            }
            assert!(
                tokio::time::Instant::now() <= deadline,
                "observer did not replicate topic within 5s"
            );
            let _ = tokio::time::timeout(Duration::from_millis(200), img_rx.changed()).await;
        }

        assert!(observer.current_image().topic("observed").is_some());
        assert!(
            client_ids
                .lock()
                .unwrap()
                .iter()
                .any(|id| id == "test-observer")
        );

        observer.cancel().await;
        ctrl.shutdown().await;
    }
}
