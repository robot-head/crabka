mod harness;

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use assert2::assert;
use harness::{TwoComputeHarness, process::ProcessHarness};

#[tokio::test]
async fn combined_r0_r1_source_starts_with_local_activation_receipt_authority() {
    let system = ProcessHarness::start_all_on_zero("tenant-combined-receipt-authority").await;
    system
        .sql(0)
        .await
        .simple_query("SELECT 1")
        .await
        .expect("combined source SQL ready");
    assert_ne!(system.endpoints()[0].1, system.endpoints()[1].1);
    system.shutdown().await;
}

#[tokio::test]
async fn concurrent_process_harnesses_publish_distinct_ports_and_shutdown_cleanly() {
    let (first, second) = tokio::join!(
        ProcessHarness::start("tenant-concurrent-harness-a"),
        ProcessHarness::start("tenant-concurrent-harness-b"),
    );
    let first_endpoints = first.endpoints();
    let second_endpoints = second.endpoints();
    assert!(
        first_endpoints
            .iter()
            .all(|endpoint| endpoint.0 != 0 && endpoint.1 != 0)
    );
    assert!(
        second_endpoints
            .iter()
            .all(|endpoint| endpoint.0 != 0 && endpoint.1 != 0)
    );
    for first in first_endpoints {
        for second in second_endpoints {
            assert_ne!(first.0, second.0);
            assert_ne!(first.1, second.1);
        }
    }
    first
        .sql(0)
        .await
        .simple_query("SELECT 1")
        .await
        .expect("first ready");
    second
        .sql(1)
        .await
        .simple_query("SELECT 1")
        .await
        .expect("second ready");
    tokio::join!(first.shutdown(), second.shutdown());
}

#[tokio::test]
async fn explicit_transactions_run_concurrently_across_compute_gateways() {
    let computes = ProcessHarness::start("tenant-real-concurrent-explicit").await;
    let r0 = computes.sql(0).await;
    let r1 = computes.sql(1).await;

    r0.simple_query("BEGIN").await.expect("r0 begin");
    // The removed range-0 explicit-transaction lease serialized every
    // BEGIN..COMMIT in the cluster through one token; a second gateway's
    // transaction must now run to completion while the first stays open.
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        r1.simple_query("BEGIN").await?;
        r1.simple_query("COMMIT").await
    })
    .await
    .expect("explicit transactions must not serialize across gateways")
    .expect("r1 transaction");
    r0.simple_query("COMMIT").await.expect("r0 commit");
    computes.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_range_computes_accept_forwarded_dml_on_hosted_ranges() {
    let computes = TwoComputeHarness::start("tenant_multiprocess").await;
    // Each CREATE TABLE is issued once, through the non-r0 right compute: it
    // forwards to the left-hosted range-0 owner and barriers cluster-wide.
    computes.create_table("CREATE TABLE t150 (id int4)").await;
    computes.create_table("CREATE TABLE t250 (id int4)").await;

    // Unsharded tables with locally-hosted writes: t150 lives on the left
    // compute's r1, t250 on the right compute's r2.
    computes.forwarded_insert(150, 10).await;
    computes.forwarded_insert(250, 20).await;

    assert!(computes.count_rows(150).await == 1);
    assert!(computes.count_rows(250).await == 1);
    // Cross-compute visibility: each side reads the row the other side hosts.
    assert!(computes.count_rows_via_peer(150).await == 1);
    assert!(computes.count_rows_via_peer(250).await == 1);
}

#[tokio::test]
async fn real_range_process_recovers_durable_forwarded_rows_after_kill() {
    let mut computes = ProcessHarness::start("tenant-process-recovery").await;
    computes.create_table("CREATE TABLE t150 (id int4)").await;
    let gateway = computes.sql(0).await;
    gateway
        .simple_query("INSERT INTO t150 VALUES (7)")
        .await
        .expect("forward insert to r1");

    let old_pid = computes.pid(1);
    computes.kill_and_restart(1).await;
    assert_ne!(computes.pid(1), old_pid, "r1 must be a new OS process");

    let recovered_gateway = computes.sql(0).await;
    let rows = recovered_gateway
        .query("SELECT id FROM t150", &[])
        .await
        .expect("read recovered remote-owned row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 7);
    computes.shutdown().await;
}

/// Deadline for a notification that must cross the range-0 log. That path is an
/// append to a real broker, then the follower's 100 ms poll inside another OS
/// process.
const CROSS_NODE_DELIVERY: Duration = Duration::from_secs(10);

/// How long a "nothing arrived" check waits.
///
/// A negative assertion can only false-pass, and it can never flake, so the wait
/// stays short. Each such assertion here is paired with a positive assertion on
/// a second listener, which proves the path was live at the same time.
const QUIET: Duration = Duration::from_millis(500);

type Notifications = tokio::sync::mpsc::UnboundedReceiver<tokio_postgres::Notification>;

/// The next notification as `(channel, payload, process_id)`. This is the whole
/// message in one value, so a case compares it in one assertion.
async fn next_notification(delivered: &mut Notifications) -> (String, String, i32) {
    let notification = tokio::time::timeout(CROSS_NODE_DELIVERY, delivered.recv())
        .await
        .expect("a notification within the cross-node deadline")
        .expect("the connection driver is still running");
    (
        notification.channel().to_owned(),
        notification.payload().to_owned(),
        notification.process_id(),
    )
}

async fn no_notification(delivered: &mut Notifications) {
    let idle = tokio::time::timeout(QUIET, delivered.recv()).await;
    assert!(idle.is_err(), "unexpected notification: {idle:?}");
}

/// A `NOTIFY` issued on the node that hosts range 0 reaches a listener on the
/// node that hosts no range 0, which is the only place the follower tail runs.
/// The notification arrives with the *originating* backend's pid, taken off that
/// connection's own `BackendKeyData`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn range_zero_notify_reaches_a_listener_on_the_node_without_range_zero() {
    let computes = ProcessHarness::start("tenant-cross-notify").await;

    let (listener, mut delivered) = computes.sql_with_notifications(1).await;
    listener
        .simple_query("LISTEN cross_chan")
        .await
        .expect("listen on the node without range 0");
    // A second listener on the same channel: it turns every "the first client
    // heard nothing" below into a bounded fact rather than a race with the
    // follower poll, because the bus fans out to both or to neither.
    let (witness, mut witnessed) = computes.sql_with_notifications(1).await;
    witness
        .simple_query("LISTEN cross_chan")
        .await
        .expect("second listener on the same channel");
    let (bystander, mut ignored) = computes.sql_with_notifications(1).await;
    bystander
        .simple_query("LISTEN other_chan")
        .await
        .expect("listener on a different channel");

    // Two connections on r0, notifying from the second: the pid on the wire
    // then identifies the originating *connection*, not merely the node.
    let quiet_r0_connection = computes.raw_sql(0).await;
    let mut notifier = computes.raw_sql(0).await;
    assert!(notifier.pid() != 0, "r0 must announce a real backend pid");
    assert!(notifier.pid() != quiet_r0_connection.pid());
    notifier.simple_query("NOTIFY cross_chan, 'payload'").await;

    let expected = (
        "cross_chan".to_owned(),
        "payload".to_owned(),
        notifier.pid(),
    );
    assert!(next_notification(&mut delivered).await == expected);
    assert!(next_notification(&mut witnessed).await == expected);
    // Exactly one delivery, and no bleed onto the other channel.
    no_notification(&mut delivered).await;
    no_notification(&mut ignored).await;

    // UNLISTEN takes effect across the hop: the witness still hears the next
    // notification, the unlistened connection does not.
    listener
        .simple_query("UNLISTEN cross_chan")
        .await
        .expect("unlisten on the node without range 0");
    notifier
        .simple_query("NOTIFY cross_chan, 'after unlisten'")
        .await;
    assert!(
        next_notification(&mut witnessed).await
            == (
                "cross_chan".to_owned(),
                "after unlisten".to_owned(),
                notifier.pid(),
            )
    );
    no_notification(&mut delivered).await;
    no_notification(&mut ignored).await;

    // The reverse hop: NOTIFY on the rN node is forwarded to the node owning
    // the log, and the originating pid survives the forwarding.
    let (r0_listener, mut reversed) = computes.sql_with_notifications(0).await;
    r0_listener
        .simple_query("LISTEN back_chan")
        .await
        .expect("listen on the range-0 node");
    let quiet_r1_connection = computes.raw_sql(1).await;
    let mut r1_notifier = computes.raw_sql(1).await;
    assert!(
        r1_notifier.pid() != 0,
        "r1 must announce a real backend pid"
    );
    assert!(r1_notifier.pid() != quiet_r1_connection.pid());
    r1_notifier
        .simple_query("NOTIFY back_chan, 'reverse'")
        .await;
    assert!(
        next_notification(&mut reversed).await
            == (
                "back_chan".to_owned(),
                "reverse".to_owned(),
                r1_notifier.pid(),
            )
    );

    computes.shutdown().await;
}

/// The property the whole design rests on. A notify record rides the range-0
/// log and is fanned out from memory, but it never lands in any key-value store.
///
/// Range 0's checkpoints snapshot its entire KV, and the WAL topic is retained
/// forever. One stored record would therefore stay in the catalog store forever,
/// in every checkpoint taken afterwards, and restored onto every follower. The
/// test reads both nodes' stores off disk after the delivery of the
/// notifications, because a checkpoint can only carry what the KV holds. It then
/// restarts the nodes to show that the log replays no notification back out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivered_notify_records_reach_neither_node_key_value_store() {
    let mut computes = ProcessHarness::start("tenant-notify-never-persists").await;

    let (listener, mut delivered) = computes.sql_with_notifications(1).await;
    listener
        .simple_query("LISTEN persist_chan")
        .await
        .expect("listen on the node without range 0");
    let mut notifier = computes.raw_sql(0).await;
    notifier.simple_query("NOTIFY persist_chan, 'first'").await;
    notifier.simple_query("NOTIFY persist_chan, 'second'").await;
    // Delivery first: without it the store assertions below would be vacuous,
    // since a record that never existed also never persisted.
    assert!(next_notification(&mut delivered).await.1 == "first");
    assert!(next_notification(&mut delivered).await.1 == "second");

    // A catalog write after the notifications, so the frames carrying them are
    // behind an ordinary applied catalog frame on both nodes by the time the
    // stores are read.
    computes
        .create_table("CREATE TABLE after_notify (id int4)")
        .await;

    // Fjall holds a directory lock, so both stores are read with their owning
    // process stopped.
    computes.kill(1).await;
    computes.kill(0).await;

    let coordinator_store = computes.cache_dir(0).join("r0");
    let follower_store = follower_store_dir(&computes.cache_dir(1));
    for store in [&coordinator_store, &follower_store] {
        let (total, notify) = scan_store(store);
        assert!(total > 0, "{} holds no keys at all", store.display());
        assert!(
            notify == 0,
            "{} holds {notify} keys under the notify prefix",
            store.display()
        );
    }

    // Restart, and prove the follower replays no notification out of what it
    // restores from. The fresh NOTIFY afterwards is what makes that silence
    // meaningful rather than a dead connection.
    computes.restart(0).await;
    computes.restart(1).await;
    let (revived, mut redelivered) = computes.sql_with_notifications(1).await;
    revived
        .simple_query("LISTEN persist_chan")
        .await
        .expect("listen after restart");
    no_notification(&mut redelivered).await;

    let mut revived_notifier = computes.raw_sql(0).await;
    revived_notifier
        .simple_query("NOTIFY persist_chan, 'after restart'")
        .await;
    assert!(
        next_notification(&mut redelivered).await
            == (
                "persist_chan".to_owned(),
                "after restart".to_owned(),
                revived_notifier.pid(),
            )
    );

    computes.shutdown().await;
}

/// The live range-0 follower cache under a node's `--cache-dir`.
///
/// The follower opens a new cache generation whenever it has to rebuild from a
/// checkpoint, and it sweeps the older generations. The directory name therefore
/// carries the generation and is not fixed.
fn follower_store_dir(cache_dir: &Path) -> PathBuf {
    let mut generations = std::fs::read_dir(cache_dir)
        .unwrap_or_else(|error| panic!("list {}: {error}", cache_dir.display()))
        .flatten()
        .filter_map(|entry| {
            entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_prefix("r0-follower-")?.parse::<u64>().ok())
        })
        .collect::<Vec<_>>();
    generations.sort_unstable();
    let newest = generations
        .last()
        .unwrap_or_else(|| panic!("no range-0 follower cache under {}", cache_dir.display()));
    cache_dir.join(format!("r0-follower-{newest}"))
}

/// `(total keys, keys under the notify prefix)` in a stopped node's store.
fn scan_store(path: &Path) -> (usize, usize) {
    use crabka_pgkv::Kv as _;

    assert!(path.is_dir(), "no store at {}", path.display());
    let store = crabka_pgkv::FjallKv::open_cache(path)
        .unwrap_or_else(|error| panic!("open {}: {error:?}", path.display()));
    let total = store.scan_prefix(&[]).expect("scan the whole store").len();
    let notify = store
        .scan_prefix(&crabka_pgkv::key::notify_prefix())
        .expect("scan the notify prefix")
        .len();
    (total, notify)
}

/// The point of `--checkpoint-frames`. A live node must trim its own WAL with no
/// request from anybody. The harness starts r0 with
/// `--checkpoint-store local --checkpoint-frames 1`, so a committed DDL must
/// produce a durable checkpoint manifest under the checkpoint root on its own.
#[tokio::test]
async fn a_live_node_writes_a_checkpoint_manifest_once_the_frame_threshold_is_crossed() {
    let system =
        ProcessHarness::start_with_checkpoint_frames("tenant-threshold-checkpoint", 1).await;
    let root = system.checkpoint_root();
    assert!(checkpoint_manifests(&root).is_empty());

    // Issue the DDL on range 0 itself: nobody asks it to checkpoint, and its own
    // background threshold poll is the only thing that can produce a manifest.
    system
        .sql(0)
        .await
        .simple_query("CREATE TABLE threshold_checkpoint (id int primary key)")
        .await
        .expect("create table on range 0");

    let manifests = wait_for_checkpoint_manifest(&root).await;
    assert!(!manifests.is_empty());
    assert!(
        manifests
            .iter()
            .all(|manifest| manifest.starts_with(&root) && manifest.ends_with("MANIFEST"))
    );

    system.shutdown().await;
}

/// Range 0 prunes its own WAL behind every checkpoint, and the node that does
/// not host range 0 follows that WAL through a local replica. A trim that lands
/// while the follower is between polls takes away the frames the follower still
/// needs. No later fetch can return them. Without a rebuild from the newest
/// checkpoint, the follower never applies again, and every statement on that
/// node stalls behind its range-0 read barrier.
///
/// `--checkpoint-frames 1` makes range 0 checkpoint and prune on every commit,
/// so each iteration below is another chance for a trim to land in that window.
/// The node without range 0 must keep serving through all of them.
#[tokio::test]
async fn the_node_without_range_zero_keeps_serving_while_range_zero_trims_its_wal() {
    let system = ProcessHarness::start_with_checkpoint_frames("tenant-follower-wal-trim", 1).await;

    for table in 0..12 {
        system
            .create_table(&format!("CREATE TABLE trim_{table} (id int4)"))
            .await;
        let client = system.sql(1).await;
        client
            .simple_query(&format!("INSERT INTO trim_{table} VALUES ({table})"))
            .await
            .unwrap_or_else(|error| panic!("insert after trim {table}: {error}"));
        let rows = client
            .query(&format!("SELECT id FROM trim_{table}"), &[])
            .await
            .unwrap_or_else(|error| panic!("select after trim {table}: {error}"));
        assert!(rows.len() == 1);
        assert!(rows[0].get::<_, i32>(0) == table);
    }

    system.shutdown().await;
}

/// Poll the checkpoint root until the background threshold poll writes a
/// manifest. The poll is bounded, so a regression fails instead of hangs.
async fn wait_for_checkpoint_manifest(root: &Path) -> Vec<PathBuf> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let manifests = checkpoint_manifests(root);
        if !manifests.is_empty() {
            return manifests;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no checkpoint manifest appeared under {}",
            root.display(),
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Every durable checkpoint manifest below `root`, in no particular order.
fn checkpoint_manifests(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.file_name().is_some_and(|name| name == "MANIFEST") {
                found.push(path);
            }
        }
    }
    found
}
