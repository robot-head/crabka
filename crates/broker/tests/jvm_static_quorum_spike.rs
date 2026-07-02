//! KIP-595 Slice 6 ACCEPTANCE TEST (Docker-gated, `#[ignore]`) — one
//! `mirror.gcr.io/apache/kafka:4.0.0` controller plus two Crabka controllers form a single
//! STATIC (`controller.quorum.voters`, kraft.version=0) metadata quorum that
//! elects a cross-impl leader AND replicates committed metadata: the JVM joins
//! as a follower of the Crabka leader, never fatal-faults, catches its
//! high-watermark up to the leader's, and builds a `FeaturesImage` carrying
//! `metadata.version=25` from the Crabka-committed log. This is the program's
//! end goal for the leader→follower direction (the static-quorum spike of
//! Slice 5 grew into this acceptance test; KIP-853 dynamic voters proved
//! unnecessary). Not a default-CI gate (needs Docker + a published controller
//! port); run it explicitly.
//!
//! Run:
//! ```text
//! cargo test -p crabka-broker --test jvm_static_quorum_spike -- --ignored --nocapture
//! ```
//!
//! ## Topology
//!
//! - Crabka voters id 1, 2: in-process, real TCP controller listeners bound to
//!   `0.0.0.0:p1` / `0.0.0.0:p2` on the host. They hold the 2/3 majority and
//!   elect among themselves immediately.
//! - JVM voter id 3: `mirror.gcr.io/apache/kafka:4.0.0`, `process.roles=controller`, in a
//!   container publishing `-p p3:p3`, dialing the Crabka voters at
//!   `host.docker.internal:p1` / `:p2`.
//! - Shared cluster id: a `uuid::Uuid` whose 16 bytes are the same bytes the JVM
//!   sees as the base64-url-no-pad `--cluster-id` string.

use std::net::SocketAddr;
use std::process::Command;
use std::time::Duration;

use assert2::check;
use base64::Engine as _;
use tempfile::TempDir;
use uuid::Uuid;

use crabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle};

mod support;

const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.0.0";
const CONTAINER: &str = "crabka-kip595-slice5-spike";

/// Kafka encodes a 16-byte UUID as URL-safe base64 with no padding. The JVM
/// `--cluster-id` string and Crabka's `uuid::Uuid` must wrap the *same* 16
/// bytes or the two sides reject each other on cluster-id mismatch.
fn kafka_cluster_id_string(id: Uuid) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(id.as_bytes())
}

/// Build a Crabka controller `BrokerConfig` for voter `i` (0-indexed; id = i+1)
/// in the shared static 3-voter set, with the shared cluster id.
fn crabka_controller_config(
    i: usize,
    own_client_addr: SocketAddr,
    own_controller_addr: SocketAddr,
    voters: &[(u64, SocketAddr)],
    cluster_id: Uuid,
    log_dir: &std::path::Path,
) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.broker_id = i32::try_from(i + 1).unwrap();
    cfg.node_id = u64::try_from(i + 1).unwrap();
    cfg.listen_addr = own_client_addr;
    cfg.advertised_listener = own_client_addr.to_string();
    cfg.controller_listen_addr = own_controller_addr;
    cfg.directory_id = Uuid::from_u128(u128::from(cfg.node_id));
    cfg.bootstrap_mode = BootstrapMode::Bootstrap;
    cfg.controller_quorum_voters = voters.iter().map(|(id, a)| (*id, a.to_string())).collect();
    cfg.auto_join = false;
    cfg.bootstrap_servers = vec![];
    cfg.cluster_id = Some(cluster_id);
    // metadata.version/group.version/transaction.version are seeded into the
    // bootstrap log automatically (KIP-584 `bootstrap_feature_records`, fired
    // when the static voter set is derived), so the JVM controller can build
    // its FeaturesImage.
    cfg
}

fn docker_rm(name: &str) {
    let _ = Command::new("docker").args(["rm", "-f", name]).output();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + a published controller port (throwaway spike)"]
#[allow(clippy::too_many_lines)]
async fn static_mixed_jvm_crabka_quorum() {
    support::init_tracing();
    docker_rm(CONTAINER);

    // ── shared cluster id ──────────────────────────────────────────────────
    let cluster_id = Uuid::from_u128(0x4d6b_5533_4f45_5642_4e54_6377_4e54_4a45);
    let cid_str = kafka_cluster_id_string(cluster_id);
    eprintln!("shared cluster_id uuid={cluster_id} kafka_str={cid_str}");

    // ── pre-bind 3 controller ports on the host ────────────────────────────
    let (client_addrs, controller_addrs) = support::bind_and_drop_ports(3).await;
    let p1 = controller_addrs[0].port();
    let p2 = controller_addrs[1].port();
    let p3 = controller_addrs[2].port();

    // Crabka voters bind 0.0.0.0 so the JVM container can reach them through
    // host.docker.internal. The pre-bound addrs are 127.0.0.1:<p>; rewrite to
    // 0.0.0.0:<p> for the bind, but keep 127.0.0.1 in the voter set Crabka uses
    // to dial *its own* peers (loopback is reachable in-process).
    let crabka_ctrl_1: SocketAddr = format!("0.0.0.0:{p1}").parse().unwrap();
    let crabka_ctrl_2: SocketAddr = format!("0.0.0.0:{p2}").parse().unwrap();

    // Voter set as seen FROM the Crabka side: dial peers on loopback; the JVM
    // (id 3) is reachable at its published host port.
    let crabka_voters: Vec<(u64, SocketAddr)> = vec![
        (1, format!("127.0.0.1:{p1}").parse().unwrap()),
        (2, format!("127.0.0.1:{p2}").parse().unwrap()),
        (3, format!("127.0.0.1:{p3}").parse().unwrap()),
    ];

    // ── start the 2 Crabka controllers ─────────────────────────────────────
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let cfg1 = crabka_controller_config(
        0,
        client_addrs[0],
        crabka_ctrl_1,
        &crabka_voters,
        cluster_id,
        dir1.path(),
    );
    let cfg2 = crabka_controller_config(
        1,
        client_addrs[1],
        crabka_ctrl_2,
        &crabka_voters,
        cluster_id,
        dir2.path(),
    );
    let (c1, c2): (BrokerHandle, BrokerHandle) = {
        let s1 = tokio::spawn(Broker::start(cfg1));
        let s2 = tokio::spawn(Broker::start(cfg2));
        (
            s1.await.unwrap().expect("crabka voter 1 start"),
            s2.await.unwrap().expect("crabka voter 2 start"),
        )
    };
    eprintln!("both Crabka controllers started (2/3 majority should self-elect)");

    // ── format + start the JVM controller (id 3) ───────────────────────────
    // The JVM's controller.quorum.voters lists addresses reachable FROM the
    // container: the Crabka voters at host.docker.internal, itself on localhost.
    let props = format!(
        "process.roles=controller\n\
         node.id=3\n\
         controller.quorum.voters=1@host.docker.internal:{p1},2@host.docker.internal:{p2},3@localhost:{p3}\n\
         controller.listener.names=CONTROLLER\n\
         listeners=CONTROLLER://0.0.0.0:{p3}\n\
         listener.security.protocol.map=CONTROLLER:PLAINTEXT\n\
         log.dirs=/tmp/kraft-controller-logs\n"
    );
    let propdir = TempDir::new().unwrap();
    let proppath = propdir.path().join("controller.properties");
    std::fs::write(&proppath, props).unwrap();

    let entry = format!(
        "/opt/kafka/bin/kafka-storage.sh format -t {cid_str} --config /tmp/c.properties --ignore-formatted && \
         exec /opt/kafka/bin/kafka-server-start.sh /tmp/c.properties"
    );
    let status = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            CONTAINER,
            "--add-host=host.docker.internal:host-gateway",
            "-p",
            &format!("{p3}:{p3}"),
            "-v",
            &format!("{}:/tmp/c.properties", proppath.display()),
            "--entrypoint",
            "bash",
            KAFKA_IMAGE,
            "-c",
            &entry,
        ])
        .status()
        .expect("docker run JVM controller");
    assert!(status.success(), "docker run failed");
    eprintln!("JVM controller (id 3) container started");

    // ── observe for ~40s ────────────────────────────────────────────────────
    // Success criterion 1: a single leader emerges across all three voters and
    // the two Crabka nodes agree on it. Success criterion 2: a follower's image
    // reflects the leader's committed records.
    let deadline = std::time::Instant::now() + Duration::from_secs(50);
    let mut elected = false;
    let mut last_l1 = None;
    let mut last_l2 = None;
    let mut tick = 0u32;
    while std::time::Instant::now() < deadline {
        let l1 = c1.controller_leader_id().await;
        let l2 = c2.controller_leader_id().await;
        last_l1 = l1;
        last_l2 = l2;
        if l1.is_some() && l1 == l2 {
            elected = true;
        }
        // Crabka-side telemetry every ~2s: leader epoch, HWM, and per-voter
        // matched index (does the JVM voter id=3 show up as fetching?).
        if tick.is_multiple_of(4) {
            let qs = c1.controller_quorum_state_for_test();
            eprintln!(
                "[t={}s] crabka n1 view: leader={:?} epoch={} hwm={} matched={:?}",
                tick / 2,
                qs.current_leader,
                qs.current_term,
                qs.last_applied_index,
                qs.per_voter_matched_index,
            );
        }
        tick += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Capture JVM logs regardless of outcome — they ARE the finding.
    let logs = Command::new("docker")
        .args(["logs", CONTAINER])
        .output()
        .expect("docker logs");
    let log_text = format!(
        "{}{}",
        String::from_utf8_lossy(&logs.stdout),
        String::from_utf8_lossy(&logs.stderr)
    );
    let _ = std::fs::write("/tmp/jvm_spike.log", &log_text);
    eprintln!("==== JVM controller logs (tail) ====");
    for line in log_text
        .lines()
        .rev()
        .take(40)
        .collect::<Vec<_>>()
        .iter()
        .rev()
    {
        eprintln!("{line}");
    }

    // Crabka-side observations.
    eprintln!(
        "Crabka leader view: node1={last_l1:?} node2={last_l2:?}  \
         voter_count(n1)={} voter_count(n2)={}",
        c1.voter_count_for_test(),
        c2.voter_count_for_test(),
    );

    // Did the JVM successfully join the quorum cross-impl? Success looks like
    // the JVM transitioning to Follower of the Crabka leader (or, less likely,
    // winning leadership itself). The dominant *failure* signal is the JVM
    // declaring `UNSUPPORTED_VERSION` ("The node does not support VOTE") because
    // Crabka's controller-listener ApiVersions handshake advertises no APIs —
    // so the JVM's NetworkClient refuses to even send Vote/Fetch on the wire.
    let jvm_joined = log_text.contains("Completed transition to FollowerState")
        || log_text.contains("Completed transition to LeaderState");
    let jvm_unsupported_version =
        log_text.contains("does not support VOTE") || log_text.contains("UNSUPPORTED_VERSION");
    let jvm_fatal_fault = log_text.contains("Encountered fatal fault");
    // The done bar: the JVM follower replicated the Crabka leader's committed
    // log and built its FeaturesImage from it — proving cross-impl metadata
    // replication, not just election.
    let jvm_replicated = log_text.contains("finished catching up to the current high water mark")
        && log_text.contains("metadata.version=25");
    eprintln!(
        "JVM cross-impl: joined={jvm_joined} unsupported={jvm_unsupported_version} \
         fatal_fault={jvm_fatal_fault} replicated={jvm_replicated}"
    );

    docker_rm(CONTAINER);
    c1.shutdown().await;
    c2.shutdown().await;

    // The two Crabka voters MUST elect among themselves regardless of the JVM.
    check!(
        elected,
        "Crabka 2/3 majority failed to elect a stable shared leader \
         (n1={last_l1:?} n2={last_l2:?})"
    );

    // The acceptance bar (Slice 6): the JVM controller joins the Crabka-led
    // static quorum as a follower, never fatal-faults, and replicates the
    // leader's committed metadata (HWM catch-up + a FeaturesImage carrying
    // metadata.version=25).
    check!(
        jvm_joined && !jvm_unsupported_version,
        "JVM did not join cross-impl: joined={jvm_joined}, unsupported={jvm_unsupported_version}"
    );
    check!(
        !jvm_fatal_fault,
        "JVM raft thread fatal-faulted (a wire/record inconsistency); see logs"
    );
    check!(
        jvm_replicated,
        "JVM did not replicate the Crabka leader's committed metadata (no HWM catch-up / \
         metadata.version not loaded); see JVM logs"
    );
}

const CONTESTED_CONTAINER: &str = "crabka-kip996-contested";

/// KIP-996 CONTESTED-ELECTION ACCEPTANCE TEST (Docker-gated, `#[ignore]`).
///
/// 2 Crabka voters (ids 1,2) + 1 `mirror.gcr.io/apache/kafka:4.0.0` voter (id 3) form a static
/// 3-voter quorum. After the Crabka leader is killed, only 1 Crabka voter + the
/// JVM voter survive, so the surviving Crabka candidate can only reach majority
/// if the JVM grants its PRE-VOTE and real vote. This is the path the old
/// `PRE_VOTE_ECHO_TAG` shortcut broke (a JVM pre-vote grant was dropped). The JVM
/// is tuned to release the dead leader fast but self-nominate slowly so the
/// surviving Crabka node wins; recovery to a new Crabka leader at a higher epoch
/// is the proof.
///
/// Run:
/// ```text
/// cargo test -p crabka-broker --test jvm_static_quorum_spike \
///   contested_election_crabka_counts_jvm_prevote -- --ignored --nocapture
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + a published controller port"]
#[allow(clippy::too_many_lines)]
async fn contested_election_crabka_counts_jvm_prevote() {
    support::init_tracing();
    docker_rm(CONTESTED_CONTAINER);

    let cluster_id = Uuid::from_u128(0x4b69_7039_3936_4350_7245_566f_7445_7374);
    let cid_str = kafka_cluster_id_string(cluster_id);

    let (client_addrs, controller_addrs) = support::bind_and_drop_ports(3).await;
    let p1 = controller_addrs[0].port();
    let p2 = controller_addrs[1].port();
    let p3 = controller_addrs[2].port();
    let crabka_ctrl_1: SocketAddr = format!("0.0.0.0:{p1}").parse().unwrap();
    let crabka_ctrl_2: SocketAddr = format!("0.0.0.0:{p2}").parse().unwrap();
    let crabka_voters: Vec<(u64, SocketAddr)> = vec![
        (1, format!("127.0.0.1:{p1}").parse().unwrap()),
        (2, format!("127.0.0.1:{p2}").parse().unwrap()),
        (3, format!("127.0.0.1:{p3}").parse().unwrap()),
    ];

    // Slow Crabka pre-vote retries (2s) so they sit well above the JVM's 300ms
    // fetch-timeout — giving the JVM a quiet window between pre-votes to time out
    // the dead leader and promote itself to Prospective (then grant the survivor).
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let mut cfg1 = crabka_controller_config(
        0,
        client_addrs[0],
        crabka_ctrl_1,
        &crabka_voters,
        cluster_id,
        dir1.path(),
    );
    let mut cfg2 = crabka_controller_config(
        1,
        client_addrs[1],
        crabka_ctrl_2,
        &crabka_voters,
        cluster_id,
        dir2.path(),
    );
    cfg1.controller_election_timeout = Duration::from_secs(2);
    cfg2.controller_election_timeout = Duration::from_secs(2);

    let (c1, c2): (BrokerHandle, BrokerHandle) = {
        let s1 = tokio::spawn(Broker::start(cfg1));
        let s2 = tokio::spawn(Broker::start(cfg2));
        (
            s1.await.unwrap().expect("crabka voter 1 start"),
            s2.await.unwrap().expect("crabka voter 2 start"),
        )
    };

    // JVM voter id 3: release the dead leader fast, self-nominate slowly.
    let props = format!(
        "process.roles=controller\n\
         node.id=3\n\
         controller.quorum.voters=1@host.docker.internal:{p1},2@host.docker.internal:{p2},3@localhost:{p3}\n\
         controller.listener.names=CONTROLLER\n\
         listeners=CONTROLLER://0.0.0.0:{p3}\n\
         listener.security.protocol.map=CONTROLLER:PLAINTEXT\n\
         controller.quorum.fetch.timeout.ms=300\n\
         controller.quorum.election.timeout.ms=10000\n\
         log.dirs=/tmp/kraft-controller-logs\n"
    );
    let propdir = TempDir::new().unwrap();
    let proppath = propdir.path().join("controller.properties");
    std::fs::write(&proppath, props).unwrap();
    let entry = format!(
        "/opt/kafka/bin/kafka-storage.sh format -t {cid_str} --config /tmp/c.properties --ignore-formatted && \
         exec /opt/kafka/bin/kafka-server-start.sh /tmp/c.properties"
    );
    let status = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            CONTESTED_CONTAINER,
            "--add-host=host.docker.internal:host-gateway",
            "-p",
            &format!("{p3}:{p3}"),
            "-v",
            &format!("{}:/tmp/c.properties", proppath.display()),
            "--entrypoint",
            "bash",
            KAFKA_IMAGE,
            "-c",
            &entry,
        ])
        .status()
        .expect("docker run JVM controller");
    assert!(status.success(), "docker run failed");

    // ── Phase 1: a Crabka node leads and the JVM joins as a follower. ───────
    let deadline = std::time::Instant::now() + Duration::from_secs(50);
    let mut leader0: Option<u64> = None;
    while std::time::Instant::now() < deadline {
        let l1 = c1.controller_leader_id().await;
        let l2 = c2.controller_leader_id().await;
        if l1.is_some() && l1 == l2 && matches!(l1, Some(1 | 2)) {
            leader0 = l1;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let leader0 = leader0.expect("Crabka 2/3 majority did not elect a leader in {1,2}");
    let epoch0 = c1.controller_quorum_state_for_test().current_term;
    eprintln!("phase 1: Crabka leader={leader0} epoch={epoch0}");

    // ── Phase 1b: WAIT for the JVM voter to actually join AND catch up. ──────
    // The two Crabka nodes agree on a leader in ~1-2s, but the JVM container
    // takes ~20-40s to boot and replicate. If we kill the leader before the JVM
    // is a functional, caught-up voter, the lone survivor (1 of 3) has no
    // reachable majority and stays stuck forever. So gate the kill on the JVM
    // log showing BOTH a role transition (Follower/Leader) AND high-water-mark
    // catch-up — the same join signals the sibling `static_mixed_jvm_crabka_quorum`
    // test relies on. Generous deadline to tolerate a slow JVM boot.
    let join_deadline = std::time::Instant::now() + Duration::from_secs(70);
    let mut jvm_joined = false;
    let mut last_jvm_log = String::new();
    while std::time::Instant::now() < join_deadline {
        let logs = Command::new("docker")
            .args(["logs", CONTESTED_CONTAINER])
            .output()
            .expect("docker logs");
        last_jvm_log = format!(
            "{}{}",
            String::from_utf8_lossy(&logs.stdout),
            String::from_utf8_lossy(&logs.stderr)
        );
        let transitioned = last_jvm_log.contains("Completed transition to FollowerState")
            || last_jvm_log.contains("Completed transition to LeaderState");
        let caught_up =
            last_jvm_log.contains("finished catching up to the current high water mark");
        if transitioned && caught_up {
            jvm_joined = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    if !jvm_joined {
        eprintln!("==== JVM controller logs (tail) — JVM NEVER JOINED ====");
        for line in last_jvm_log
            .lines()
            .rev()
            .take(40)
            .collect::<Vec<_>>()
            .iter()
            .rev()
        {
            eprintln!("{line}");
        }
        let _ = std::fs::write("/tmp/jvm_contested.log", &last_jvm_log);
        docker_rm(CONTESTED_CONTAINER);
        // Best-effort cleanup of the in-process brokers; the process is dying.
        c1.shutdown().await;
        c2.shutdown().await;
        panic!(
            "JVM voter id 3 did not join the quorum (no Follower/Leader transition + HWM \
             catch-up) within 70s — this is a pre-existing JVM-join problem, not the \
             KIP-996 pre-vote fix. See /tmp/jvm_contested.log."
        );
    }
    eprintln!("phase 1b: JVM voter joined and caught up to HWM — safe to kill the leader");

    // ── Phase 1c: let the JVM settle into a STEADY live-fetch relationship. ──
    // The Phase 1b gate trips the instant the JVM logs both "transition to
    // FollowerState" and "finished catching up to the current high water mark"
    // — but the JVM catches up from the *bootstrap snapshot* within tens of
    // milliseconds of booting, long before it has completed a single live Fetch
    // round-trip to the leader. Killing at that instant leaves the JVM with no
    // recent successful fetch, so its FollowerState fetch-timeout clock has no
    // live baseline and (with the leader endpoint in NetworkClient connection-
    // backoff) KRaft 4.0 never promotes it to Prospective — it stays
    // Follower(leader=1) and rejects every pre-vote for the whole window.
    //
    // Sleeping here lets the JVM run several live Fetch cycles before the kill.
    // NOTE: doing so surfaced a SEPARATE, deeper Crabka blocker — the JVM
    // replicates past the bootstrap snapshot and fatal-faults applying a
    // DUPLICATE `__consumer_offsets` TopicRecord with a mismatched topic id
    // ("Found duplicate TopicRecord for __consumer_offsets with a different ID
    // than before"). That duplicate comes from both Crabka voters racing the
    // read-then-write topic-bootstrap in coordinator/bootstrap.rs, each
    // submitting a TopicRecord with its own fresh Uuid::new_v4(). Until that
    // bootstrap is made idempotent on topic id, a JVM follower that replicates
    // far enough will crash and can never grant the survivor's pre-vote.
    tokio::time::sleep(Duration::from_secs(6)).await;
    eprintln!("phase 1c: JVM has had 6s of steady fetching — killing the leader now");

    // ── Phase 2: kill the Crabka leader; the survivor needs the JVM's grants. ─
    let (killed, survivor, survivor_id) = if leader0 == 1 {
        (c1, c2, 2u64)
    } else {
        (c2, c1, 1u64)
    };
    killed.shutdown().await;
    eprintln!("phase 2: killed Crabka leader {leader0}; survivor is {survivor_id}");

    // ── Phase 3: the surviving Crabka voter must win a new election. ─────────
    // Trace the survivor's quorum state every ~2s so a stuck recovery is legible:
    // does `current_term` climb past epoch0 (the survivor IS promoting in some
    // rounds) or is it truly pinned at the old epoch (no majority reachable)?
    let recover_deadline = std::time::Instant::now() + Duration::from_mins(1);
    let mut recovered = false;
    let mut tick = 0u32;
    while std::time::Instant::now() < recover_deadline {
        let qs = survivor.controller_quorum_state_for_test();
        if tick.is_multiple_of(4) {
            eprintln!(
                "[recovery t={}s] survivor {survivor_id} view: leader={:?} term={} (was {epoch0})",
                tick / 2,
                qs.current_leader,
                qs.current_term,
            );
        }
        if qs.current_leader == Some(survivor_id) && qs.current_term > epoch0 {
            recovered = true;
            break;
        }
        tick += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let final_qs = survivor.controller_quorum_state_for_test();
    eprintln!(
        "phase 3: survivor view leader={:?} epoch={} (was {epoch0})",
        final_qs.current_leader, final_qs.current_term
    );

    // Capture JVM logs for diagnosis regardless of outcome.
    let logs = Command::new("docker")
        .args(["logs", CONTESTED_CONTAINER])
        .output()
        .expect("docker logs");
    let log_text = format!(
        "{}{}",
        String::from_utf8_lossy(&logs.stdout),
        String::from_utf8_lossy(&logs.stderr)
    );
    let _ = std::fs::write("/tmp/jvm_contested.log", &log_text);
    let jvm_fatal_fault = log_text.contains("Encountered fatal fault");

    // Dump the JVM log tail to stderr (pass or fail) — it shows whether the JVM
    // granted/rejected the survivor's preVote/Vote, and whether it tried to
    // become candidate/leader itself.
    eprintln!("==== JVM controller logs (tail) — contested election ====");
    for line in log_text
        .lines()
        .rev()
        .take(40)
        .collect::<Vec<_>>()
        .iter()
        .rev()
    {
        eprintln!("{line}");
    }

    docker_rm(CONTESTED_CONTAINER);
    survivor.shutdown().await;

    assert!(
        recovered,
        "surviving Crabka voter {survivor_id} did not win a new election at a \
         higher epoch after the leader died — the JVM's pre-vote grant was not \
         counted (KIP-996 interop regression). survivor view: leader={:?} epoch={} (was {epoch0})",
        final_qs.current_leader, final_qs.current_term
    );
    assert!(
        !jvm_fatal_fault,
        "JVM controller fatal-faulted during the contested election; see /tmp/jvm_contested.log"
    );
}
