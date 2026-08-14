//! Diskless WAL Slice 6d composite shipping gate.
//!
//! The ignored test below is run explicitly in CI. It uses three real brokers,
//! an RF=1 diskless topic, two concurrent public producers, a direct Rust
//! partition fetch, and the JVM console consumer. The two non-replica brokers
//! are WAL voters only until one is explicitly promoted after the sole classic
//! owner crashes. Every fault has a discriminating witness; a no-op fails.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use assert2::assert;
use bytes::Bytes;
use crabka_broker::{
    BootstrapMode, Broker, BrokerConfig, BrokerHandle, NodeId, RemoteStorageBackend, RlmmKind,
};
use crabka_client_core::{Client, IsolatedFetch};
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_metadata::MetadataRecord;
use crabka_protocol::owned::create_topics_request::{
    CreatableTopic, CreatableTopicConfig, CreateTopicsRequest,
};
use stateright::semantics::{ConsistencyTester, LinearizabilityTester, SequentialSpec};
use tempfile::TempDir;

const TOPIC: &str = "diskless-jepsen";
const APPENDERS: u64 = 2;
const RECORDS_PER_APPENDER: u64 = 4;
const JVM_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.4.0";
/// The deadline for a `docker` invocation that only reads local daemon state.
const DOCKER_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
/// The deadline for the `docker run` of the JVM console consumer.
///
/// The deadline covers the container start and the consumer's own
/// `--timeout-ms`. It also covers a pull of the roughly 380 MB `JVM_IMAGE`,
/// because a developer who runs this test outside CI has no preload step.
const JVM_CONSUME_TIMEOUT: Duration = Duration::from_mins(5);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct AppendOp(Vec<u8>);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct AppendRet(i64);

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
struct KafkaLogSpec {
    values: Vec<Vec<u8>>,
}

impl SequentialSpec for KafkaLogSpec {
    type Op = AppendOp;
    type Ret = AppendRet;

    fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
        let offset = i64::try_from(self.values.len()).expect("test history fits i64");
        self.values.push(op.0.clone());
        AppendRet(offset)
    }
}

#[derive(Debug)]
struct AckedRecord {
    client: u64,
    value: Vec<u8>,
    partition: i32,
    offset: i64,
    invoke_order: u64,
    return_order: u64,
}

#[derive(Debug)]
enum HistoryEvent<'a> {
    Invoke(&'a AckedRecord),
    Return(&'a AckedRecord),
}

#[derive(Debug, Default)]
struct FaultWitness {
    put_failures: u64,
    lost_wal_node: Option<NodeId>,
    old_controller: Option<NodeId>,
    new_controller: Option<NodeId>,
    old_partition_leader: Option<NodeId>,
    new_partition_leader: Option<NodeId>,
    object_retry_succeeded: bool,
    rust_ledger_checked: bool,
    jvm_differential_checked: bool,
}

struct TestNode {
    handle: Option<BrokerHandle>,
    config: BrokerConfig,
    _data_dir: TempDir,
}

impl TestNode {
    fn handle(&self) -> &BrokerHandle {
        self.handle.as_ref().expect("broker is live")
    }

    fn host_bootstrap(&self) -> String {
        format!("127.0.0.1:{}", self.config.listen_addr.port())
    }

    fn docker_bootstrap(&self) -> String {
        format!("host.docker.internal:{}", self.config.listen_addr.port())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "Slice 6d shipping gate; requires Docker and runs explicitly in CI"]
async fn three_broker_fault_schedule_preserves_the_acked_ledger() {
    let object_dir = TempDir::new().expect("shared object dir");
    let put_blocker = object_dir.path().join("diskless-wal");
    std::fs::write(&put_blocker, b"force object PUT to fail")
        .expect("install deterministic PUT blocker");

    let gateway = docker_bridge_gateway().await;
    let mut cluster = start_three_brokers(object_dir.path(), gateway).await;
    for node in &cluster {
        node.handle().wait_until_brokers_registered(3).await;
    }

    create_diskless_topic(&cluster[0].host_bootstrap()).await;
    for node in &cluster {
        node.handle()
            .wait_for_image(|image| {
                image.partition(TOPIC, 0).is_some_and(|partition| {
                    partition.replicas.len() == 1 && partition.isr.len() == 1
                })
            })
            .await;
        node.handle().wait_until_diskless_flusher_ready().await;
    }

    // Make one exact broker both the accepting partition leader and the
    // controller leader. Killing it therefore exercises the requested
    // accepting-broker fault and a sequencer/controller-authority handoff in
    // the same bounded schedule, while two of three voters remain live.
    let old_controller = converged_controller_leader(&cluster).await;
    let all_nodes = [0, 1, 2];
    force_partition_owner(&cluster, &all_nodes, 0, old_controller).await;
    let victim = cluster
        .iter()
        .position(|node| node.config.node_id == old_controller)
        .expect("controller leader is a broker");
    wait_for_wal_runtime(&cluster[victim], old_controller).await;
    let survivors: Vec<usize> = (0..cluster.len())
        .filter(|index| *index != victim)
        .collect();
    let owner_record = cluster[victim]
        .handle()
        .partition_record_for_test(TOPIC, 0)
        .expect("sole classic owner record");
    assert!(owner_record.replicas == vec![old_controller]);
    assert!(
        survivors.iter().all(|index| !owner_record
            .replicas
            .contains(&cluster[*index].config.node_id)),
        "a WAL survivor unexpectedly remained a classic replica"
    );
    for &index in &survivors {
        wait_until_path_absent(&cluster[index].config.log_dir.join(format!("{TOPIC}-0"))).await;
    }

    let put_failures_before = cluster[victim]
        .handle()
        .diskless_put_failure_count_for_test();
    let clock = Arc::new(AtomicU64::new(0));
    let (left, right) = tokio::join!(
        produce_appender(cluster[victim].host_bootstrap(), 0, Arc::clone(&clock)),
        produce_appender(cluster[victim].host_bootstrap(), 1, Arc::clone(&clock)),
    );
    let mut ledger = left;
    ledger.extend(right);
    ledger.sort_unstable_by_key(|record| record.offset);
    assert_acked_ledger(&ledger);
    assert_linearizable_history(&ledger);

    wait_for_put_failure(&cluster[victim], put_failures_before).await;
    assert!(
        put_blocker.is_file(),
        "PUT failure witness disappeared before WAL-loss fault"
    );
    assert!(
        !object_dir.path().join("diskless-wal").is_dir(),
        "blocked PUT unexpectedly created its object namespace"
    );

    let victim_handle = cluster[victim].handle();
    victim_handle
        .wait_until_local_log_end_offset(
            TOPIC,
            0,
            i64::try_from(ledger.len()).expect("ledger length fits i64"),
        )
        .await;
    let durable_end = i64::try_from(ledger.len()).expect("ledger length fits i64");
    for &index in &survivors {
        let range = wait_for_follower_checkpoint(
            &cluster[index].config.log_dir,
            cluster[index].config.node_id,
            durable_end,
        )
        .await;
        assert!(range == (0, durable_end));
    }

    let victim_wal = cluster[victim].config.log_dir.join(format!("{TOPIC}-0"));
    let wal_files_before_loss = recursive_file_count(&victim_wal);
    assert!(
        wal_files_before_loss > 0,
        "victim canonical log was empty; node-loss injection would be a no-op"
    );

    cluster[victim]
        .handle
        .take()
        .expect("victim live")
        .crash_for_test()
        .await;
    std::fs::remove_dir_all(&victim_wal).expect("erase the exact victim canonical log");
    assert!(
        !victim_wal.exists(),
        "victim canonical log still exists after node-loss injection"
    );

    let new_controller =
        wait_for_new_controller(cluster[survivors[0]].handle(), old_controller).await;
    for &index in &survivors[1..] {
        let observed = wait_for_new_controller(cluster[index].handle(), old_controller).await;
        assert!(
            observed == new_controller,
            "survivors did not converge on one controller: {new_controller} vs {observed}"
        );
    }
    let new_partition_leader = new_controller;
    let new_partition_leader_index = survivors
        .iter()
        .copied()
        .find(|index| cluster[*index].config.node_id == new_partition_leader)
        .expect("replacement partition leader is a live broker");
    force_partition_owner(
        &cluster,
        &survivors,
        new_partition_leader_index,
        new_partition_leader,
    )
    .await;
    cluster[new_partition_leader_index]
        .handle()
        .wait_until_local_partition_leader(TOPIC, 0, new_partition_leader)
        .await;
    cluster[new_partition_leader_index]
        .handle()
        .wait_until_local_log_end_offset(
            TOPIC,
            0,
            i64::try_from(ledger.len()).expect("ledger length fits i64"),
        )
        .await;
    cluster[new_partition_leader_index]
        .handle()
        .wait_until_high_watermark(
            TOPIC,
            0,
            i64::try_from(ledger.len()).expect("ledger length fits i64"),
        )
        .await;

    // The object tier is still unavailable here. Successful readback therefore
    // proves that the acknowledged tail survived on the remaining WAL quorum,
    // rather than being rescued by a completed object flush.
    assert!(put_blocker.is_file());
    let observed = consume_ledger(&cluster[survivors[0]].host_bootstrap(), ledger.len()).await;
    let expected = ledger_values(&ledger);
    assert!(
        observed == expected,
        "Rust consumer lost or reordered acknowledged records: expected={expected:?} observed={observed:?}"
    );

    // The failed pre-crash attempt belonged to the victim's flusher. Before
    // unblocking the store, require another failure after handoff so the
    // replacement leader proves that its own retry loop is live and owns the
    // durable tail.
    let put_failures_after_crash = cluster[new_partition_leader_index]
        .handle()
        .diskless_put_failure_count_for_test();
    eprintln!(
        "[diskless_replacement_flusher_state] node={new_partition_leader} state={:?} failures={put_failures_after_crash}",
        cluster[new_partition_leader_index]
            .handle()
            .diskless_flush_state_for_test(TOPIC, 0)
            .await
    );
    let put_failures = wait_for_put_failure(
        &cluster[new_partition_leader_index],
        put_failures_after_crash,
    )
    .await;

    // Let the failed PUT retry only after the no-acked-loss assertion. A real
    // `.ckwl` object is the recovery witness; merely removing the blocker is
    // insufficient.
    std::fs::remove_file(&put_blocker).expect("remove PUT blocker");
    std::fs::create_dir(object_dir.path().join("diskless-wal"))
        .expect("create object namespace after fault");
    wait_for_wal_object(object_dir.path(), new_partition_leader).await;

    let jvm_stdout =
        jvm_consume_exact(&cluster[survivors[0]].docker_bootstrap(), ledger.len()).await;
    let expected_stdout = expected
        .iter()
        .flat_map(|(_, value)| value.iter().copied().chain(std::iter::once(b'\n')))
        .collect::<Vec<_>>();
    assert!(
        jvm_stdout == expected_stdout,
        "JVM byte differential mismatch: expected={:?} observed={:?}",
        String::from_utf8_lossy(&expected_stdout),
        String::from_utf8_lossy(&jvm_stdout),
    );

    let witness = FaultWitness {
        put_failures,
        lost_wal_node: Some(old_controller),
        old_controller: Some(old_controller),
        new_controller: Some(new_controller),
        old_partition_leader: Some(old_controller),
        new_partition_leader: Some(new_partition_leader),
        object_retry_succeeded: true,
        rust_ledger_checked: true,
        jvm_differential_checked: true,
    };
    assert_complete_witness(&witness, put_failures_after_crash);
    eprintln!("[diskless_slice6d_witness] {witness:?}");

    for node in cluster {
        if let Some(handle) = node.handle {
            handle.shutdown().await;
        }
    }
}

async fn start_three_brokers(object_dir: &Path, gateway: IpAddr) -> Vec<TestNode> {
    let mut client_listeners = Vec::with_capacity(3);
    let mut controller_listeners = Vec::with_capacity(3);
    for _ in 0..3 {
        client_listeners.push(
            tokio::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0))
                .await
                .expect("bind client listener"),
        );
        controller_listeners.push(
            tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
                .await
                .expect("bind controller listener"),
        );
    }
    let client_addrs = client_listeners
        .iter()
        .map(|listener| listener.local_addr().expect("client local addr"))
        .collect::<Vec<_>>();
    let controller_addrs = controller_listeners
        .iter()
        .map(|listener| listener.local_addr().expect("controller local addr"))
        .collect::<Vec<_>>();
    let voters = controller_addrs
        .iter()
        .enumerate()
        .map(|(index, address)| {
            (
                NodeId(u64::try_from(index + 1).expect("node id")),
                address.to_string(),
            )
        })
        .collect::<Vec<_>>();

    let mut starts = Vec::with_capacity(3);
    let mut metadata = Vec::with_capacity(3);
    for (index, (client_listener, controller_listener)) in client_listeners
        .into_iter()
        .zip(controller_listeners)
        .enumerate()
    {
        let data_dir = TempDir::new().expect("broker data dir");
        let mut config = BrokerConfig::for_tests(data_dir.path().to_path_buf());
        config.broker_id = i32::try_from(index + 1).expect("broker id");
        config.node_id = NodeId(u64::try_from(index + 1).expect("node id"));
        config.directory_id = uuid::Uuid::from_u128(u128::from(config.node_id.0));
        config.listen_addr = client_addrs[index];
        config.advertised_listener =
            SocketAddr::new(gateway, client_addrs[index].port()).to_string();
        config.controller_listen_addr = controller_addrs[index];
        config.controller_quorum_voters.clone_from(&voters);
        config.bootstrap_mode = BootstrapMode::Bootstrap;
        config.auto_join = false;
        config.bootstrap_servers.clear();
        config.rack = Some(format!("rack-{index}"));
        config.default_min_insync_replicas = 1;
        config.audit_enabled = false;
        config.diskless_wal_local_replica_count = 3;
        config.diskless_wal_flush_interval = crabka_units::millis(100);
        config.diskless_wal_index_projection_timeout = crabka_units::secs(10);
        config.diskless_wal_trim_safety_lag = 1_024;
        config.heartbeat_interval = crabka_units::millis(250);
        config.heartbeat_timeout = crabka_units::secs(2);
        config.liveness_tick_interval = crabka_units::millis(100);
        config.remote_storage_backend = Some(RemoteStorageBackend::Local {
            dir: object_dir.to_path_buf(),
        });
        config.remote_log_metadata = RlmmKind::InMemory;

        let start_config = config.clone();
        starts.push(tokio::spawn(async move {
            Broker::start_with_listeners(
                start_config,
                Some(controller_listener),
                Some(client_listener),
            )
            .await
        }));
        metadata.push((config, data_dir));
    }

    let mut cluster = Vec::with_capacity(3);
    for (start, (config, data_dir)) in starts.into_iter().zip(metadata) {
        let handle = start
            .await
            .expect("broker start task")
            .expect("three-broker start");
        cluster.push(TestNode {
            handle: Some(handle),
            config,
            _data_dir: data_dir,
        });
    }
    cluster
}

async fn create_diskless_topic(bootstrap: &str) {
    let client = Client::builder()
        .bootstrap(bootstrap)
        .client_id("diskless-slice6d-admin")
        .build()
        .await
        .expect("admin client");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.into(),
                num_partitions: 1,
                replication_factor: 1,
                configs: vec![CreatableTopicConfig {
                    name: "crabka.diskless".into(),
                    value: Some("true".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            timeout_ms: 10_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        response.topics[0].error_code == 0,
        "create diskless RF=1 topic failed: {response:?}"
    );
}

async fn converged_controller_leader(cluster: &[TestNode]) -> NodeId {
    let leader = cluster[0].handle().wait_until_controller_leader().await;
    for node in &cluster[1..] {
        let observed = node.handle().wait_until_controller_leader().await;
        assert!(
            observed == leader,
            "controller leader did not converge: {leader} vs {observed}"
        );
    }
    leader
}

async fn force_partition_owner(
    cluster: &[TestNode],
    live: &[usize],
    submitter: usize,
    leader: NodeId,
) {
    let current = cluster[submitter]
        .handle()
        .partition_record_for_test(TOPIC, 0)
        .expect("diskless partition record");
    if current.leader != leader || current.replicas != [leader] {
        let mut forced = current;
        forced.leader = leader;
        forced.replicas = vec![leader];
        forced.isr = vec![leader];
        forced.adding_replicas.clear();
        forced.removing_replicas.clear();
        forced.directories = vec![uuid::Uuid::nil()];
        forced.leader_epoch = crabka_metadata::LeaderEpoch(forced.leader_epoch.0 + 1);
        forced.partition_epoch += 1;
        cluster[submitter]
            .handle()
            .submit_metadata_record_for_test(MetadataRecord::V1Partition(forced))
            .await
            .expect("assign the sole classic replica");
    }
    for &index in live {
        cluster[index]
            .handle()
            .wait_for_image(|image| {
                image.partition(TOPIC, 0).is_some_and(|partition| {
                    partition.leader == leader
                        && partition.replicas == [leader]
                        && partition.isr == [leader]
                })
            })
            .await;
    }
}

async fn produce_appender(
    bootstrap: String,
    appender: u64,
    clock: Arc<AtomicU64>,
) -> Vec<AckedRecord> {
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .client_id(format!("diskless-slice6d-appender-{appender}"))
        .enable_idempotence(true)
        .acks(Acks::All)
        .linger(Duration::from_millis(2))
        .build()
        .await
        .expect("producer build");

    let mut records = Vec::with_capacity(RECORDS_PER_APPENDER as usize);
    for sequence in 0..RECORDS_PER_APPENDER {
        let client = appender * RECORDS_PER_APPENDER + sequence + 1;
        let value = format!("appender-{appender}-record-{sequence}").into_bytes();
        let invoke_order = clock.fetch_add(1, Ordering::SeqCst);
        let metadata = producer
            .send(ProducerRecord {
                topic: TOPIC.into(),
                partition: Some(0),
                value: Some(Bytes::copy_from_slice(&value)),
                ..Default::default()
            })
            .await
            .await
            .expect("producer response channel")
            .expect("acks=all record");
        let return_order = clock.fetch_add(1, Ordering::SeqCst);
        records.push(AckedRecord {
            client,
            value,
            partition: metadata.partition,
            offset: metadata.offset,
            invoke_order,
            return_order,
        });
    }
    producer.flush().await.expect("producer flush");
    producer.close().await.expect("producer close");
    records
}

async fn wait_for_wal_runtime(node: &TestNode, leader: NodeId) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if node
                .handle()
                .diskless_wal_ready_for_test(TOPIC, 0, leader, 3)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("diskless WAL runtime placement did not become ready");
}

fn assert_acked_ledger(ledger: &[AckedRecord]) {
    assert!(
        ledger.len()
            == usize::try_from(APPENDERS * RECORDS_PER_APPENDER).expect("record count fits usize")
    );
    for (expected_offset, record) in ledger.iter().enumerate() {
        assert!(record.partition == 0);
        assert!(
            record.offset == i64::try_from(expected_offset).expect("offset fits i64"),
            "acked ledger is not gap-free: {ledger:?}"
        );
    }
    let unique_values = ledger
        .iter()
        .map(|record| record.value.as_slice())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        unique_values.len() == ledger.len(),
        "duplicate ledger values"
    );
}

fn assert_linearizable_history(ledger: &[AckedRecord]) {
    let mut events = ledger
        .iter()
        .flat_map(|record| {
            [
                (record.invoke_order, HistoryEvent::Invoke(record)),
                (record.return_order, HistoryEvent::Return(record)),
            ]
        })
        .collect::<Vec<_>>();
    events.sort_unstable_by_key(|(order, _)| *order);

    let mut checker = LinearizabilityTester::new(KafkaLogSpec::default());
    for (_, event) in events {
        match event {
            HistoryEvent::Invoke(record) => {
                checker
                    .on_invoke(record.client, AppendOp(record.value.clone()))
                    .expect("one in-flight operation per client");
            }
            HistoryEvent::Return(record) => {
                checker
                    .on_return(record.client, AppendRet(record.offset))
                    .expect("return matches invoke");
            }
        }
    }
    assert!(
        checker.serialized_history().is_some(),
        "acked producer history is not linearizable"
    );
}

async fn wait_for_put_failure(node: &TestNode, before: u64) -> u64 {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let current = node.handle().diskless_put_failure_count_for_test();
            if current > before {
                return current;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("real diskless object PUT failure was never observed")
}

async fn wait_for_follower_checkpoint(
    data_dir: &Path,
    node_id: NodeId,
    expected_end: i64,
) -> (i64, i64) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let root = data_dir.join("__diskless_wal_quorum");
            if let Ok(entries) = std::fs::read_dir(&root) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    let name = entry.file_name();
                    let checkpoint = path
                        .join(format!("voter-{}", node_id.0))
                        .join("wal-durable-offset.checkpoint");
                    if name.to_string_lossy().starts_with(TOPIC)
                        && let Ok(value) = std::fs::read_to_string(checkpoint)
                    {
                        let offsets = value
                            .split_ascii_whitespace()
                            .filter_map(|value| value.parse::<i64>().ok())
                            .collect::<Vec<_>>();
                        if offsets.as_slice() == [0, expected_end] {
                            return (offsets[0], offsets[1]);
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("non-replica WAL voter did not checkpoint the acknowledged prefix")
}

fn recursive_file_count(path: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(path) else {
        return usize::from(path.is_file());
    };
    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                recursive_file_count(&path)
            } else {
                usize::from(path.is_file())
            }
        })
        .sum()
}

async fn wait_until_path_absent(path: &Path) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while path.exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("stale classic partition remained at {}", path.display()));
}

async fn wait_for_new_controller(handle: &BrokerHandle, old: NodeId) -> NodeId {
    let mut leaders = handle.watch_leader_for_test();
    tokio::time::timeout(
        Duration::from_secs(30),
        leaders
            .wait_for(|leader| leader.is_some_and(|leader| leader != old && leader != NodeId(0))),
    )
    .await
    .expect("controller did not hand off after leader loss")
    .expect("controller leader watch closed")
    .to_owned()
    .expect("predicate requires a leader")
}

fn ledger_values(ledger: &[AckedRecord]) -> Vec<(i64, Vec<u8>)> {
    ledger
        .iter()
        .map(|record| (record.offset, record.value.clone()))
        .collect()
}

async fn consume_ledger(bootstrap: &str, expected: usize) -> Vec<(i64, Vec<u8>)> {
    // Fetch the one known partition directly. The durability checker must not
    // depend on a classic consumer-group coordinator, which is an unrelated
    // subsystem and may be hosted by the broker the nemesis just killed.
    let client = Client::builder()
        .bootstrap(bootstrap)
        .client_id("diskless-slice6d-consumer")
        .build()
        .await
        .expect("direct fetch client");
    let metadata = client.refresh_metadata().await.expect("fetch metadata");
    let topic = metadata
        .topics
        .iter()
        .find(|topic| topic.name.as_deref() == Some(TOPIC))
        .expect("diskless topic metadata");
    let partition = topic
        .partitions
        .iter()
        .find(|partition| partition.partition_index == 0)
        .expect("diskless partition metadata");
    let broker_id = partition.leader_id;
    let topic_id = topic.topic_id;

    let deadline = Instant::now() + Duration::from_secs(45);
    let mut observed = Vec::new();
    let mut offset = 0;
    while observed.len() < expected && Instant::now() < deadline {
        let records = client
            .fetch_partition_with_isolation_on(
                broker_id,
                IsolatedFetch {
                    topic: TOPIC,
                    topic_id,
                    partition: 0,
                    fetch_offset: offset,
                    max_wait: crabka_units::millis(500),
                    max: crabka_units::mebibytes(4),
                    partition_max: crabka_units::mebibytes(4),
                    fetch_min: crabka_client_core::FetchMinBytes::default(),
                    isolation_level: 0,
                },
            )
            .await
            .expect("direct partition fetch");
        for record in records {
            offset = record.offset + 1;
            observed.push((
                record.offset,
                record.value.map_or_else(Vec::new, |value| value.to_vec()),
            ));
        }
    }
    client.close();
    observed
}

async fn wait_for_wal_object(object_dir: &Path, leader: NodeId) {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let namespace = object_dir.join("diskless-wal").join(leader.0.to_string());
            let has_wal_object = std::fs::read_dir(namespace).is_ok_and(|entries| {
                entries.flatten().any(|entry| {
                    entry.path().is_file()
                        && entry
                            .path()
                            .extension()
                            .is_some_and(|extension| extension == "ckwl")
                })
            });
            if has_wal_object {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("diskless WAL object PUT did not recover after blocker removal");
}

async fn docker_bridge_gateway() -> IpAddr {
    let output = docker_output(
        &[
            "network",
            "inspect",
            "bridge",
            "--format",
            "{{(index .IPAM.Config 0).Gateway}}",
        ],
        DOCKER_QUERY_TIMEOUT,
        "docker network inspect",
    )
    .await;
    assert!(
        output.status.success(),
        "docker bridge inspection failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("docker gateway is UTF-8")
        .trim()
        .parse()
        .expect("docker bridge gateway is an IP address")
}

/// Runs `docker` under a deadline and kills the child when the deadline passes.
///
/// `std::process::Command::output` blocks with no deadline of its own, so a
/// stalled image pull holds the test open until the CI job wall stops it. The
/// tokio child gives the wait an async deadline, and `kill_on_drop` stops the
/// `docker` client when the timeout drops the future.
async fn docker_output(args: &[&str], deadline: Duration, what: &str) -> std::process::Output {
    let child = tokio::process::Command::new("docker")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap_or_else(|err| panic!("spawn {what}: {err}"));
    tokio::time::timeout(deadline, child.wait_with_output())
        .await
        .unwrap_or_else(|_| panic!("{what} did not finish within {deadline:?}"))
        .unwrap_or_else(|err| panic!("run {what}: {err}"))
}

async fn jvm_consume_exact(bootstrap: &str, expected: usize) -> Vec<u8> {
    let max_messages = expected.to_string();
    let output = docker_output(
        &[
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            JVM_IMAGE,
            "kafka-console-consumer",
            "--bootstrap-server",
            bootstrap,
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            &max_messages,
            "--timeout-ms",
            "30000",
        ],
        JVM_CONSUME_TIMEOUT,
        "JVM console consumer",
    )
    .await;
    assert!(
        output.status.success(),
        "JVM consumer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    output.stdout
}

fn assert_complete_witness(witness: &FaultWitness, put_failures_before: u64) {
    assert!(witness.put_failures > put_failures_before);
    assert!(witness.lost_wal_node.is_some());
    assert!(witness.old_controller.is_some());
    assert!(witness.new_controller.is_some());
    assert!(witness.old_controller != witness.new_controller);
    assert!(witness.old_partition_leader.is_some());
    assert!(witness.new_partition_leader.is_some());
    assert!(witness.old_partition_leader != witness.new_partition_leader);
    assert!(witness.lost_wal_node == witness.old_partition_leader);
    assert!(witness.object_retry_succeeded);
    assert!(witness.rust_ledger_checked);
    assert!(witness.jvm_differential_checked);
}
