mod harness;

use crabka_gres_ranges::RangeId;
use harness::{FaultEvent, SystemHarness, TableAccount, process::ProcessHarness};

#[tokio::test]
async fn range0_writer_kill_drain_is_fence_plus_prologue_before_serving() {
    let mut system = SystemHarness::start("tenant_range0_drain");
    system.initialize_bank(100).await;

    system.kill_writer(RangeId::new(0));
    assert!(
        !system
            .transfer(TableAccount::LEFT, TableAccount::RIGHT, 13)
            .await
    );
    system.fence_and_recover(RangeId::new(0));
    assert!(
        !system
            .transfer(TableAccount::LEFT, TableAccount::RIGHT, 13)
            .await
    );

    assert_eq!(system.bank_total().await, 200);
    assert_eq!(
        system.fault_log(),
        &[
            FaultEvent::WriterKilled(RangeId::new(0)),
            FaultEvent::FenceAndPrologue(RangeId::new(0)),
        ]
    );
}

#[tokio::test]
async fn real_range0_kill_fences_old_session_and_recovers_before_serving() {
    let mut system = ProcessHarness::start("tenant-real-range0-drain").await;
    system
        .create_table(
            "CREATE TABLE bank50 (id int4, balance int4); \
             CREATE TABLE bank150 (id int4, balance int4)",
        )
        .await;
    let old_session = system.sql(0).await;
    old_session
        .simple_query(
            "INSERT INTO bank50 VALUES (1, 100); \
             INSERT INTO bank150 VALUES (1, 100)",
        )
        .await
        .expect("seed bank");
    let old_r0 = system.pid(0);
    let participant = system.pid(1);

    system.kill(0).await;
    assert!(system.try_sql(0).await.is_none());
    assert!(old_session.simple_query("SELECT 1").await.is_err());
    assert_eq!(
        system.pid(1),
        participant,
        "participant stays live during drain"
    );

    system.restart(0).await;
    assert_ne!(system.pid(0), old_r0, "r0 must be a recovered OS process");
    assert_eq!(system.pid(1), participant);

    let client = system.sql(0).await;
    client
        .simple_query("BEGIN")
        .await
        .expect("begin after prologue");
    client
        .simple_query("UPDATE bank50 SET balance = 87 WHERE id = 1")
        .await
        .expect("debit after prologue");
    client
        .simple_query("UPDATE bank150 SET balance = 113 WHERE id = 1")
        .await
        .expect("credit after prologue");
    client
        .simple_query("COMMIT")
        .await
        .expect("commit after prologue");

    let left: i32 = client
        .query_one("SELECT balance FROM bank50 WHERE id = 1", &[])
        .await
        .expect("read left")
        .get(0);
    let right: i32 = client
        .query_one("SELECT balance FROM bank150 WHERE id = 1", &[])
        .await
        .expect("read right")
        .get(0);
    assert_eq!((left, right, left + right), (87, 113, 200));
}

#[tokio::test]
async fn real_range0_readiness_waits_for_in_doubt_recovery_prologue() {
    let mut system = ProcessHarness::start_with_commit_fault(
        "tenant-real-range0-in-doubt-drain",
        "before_decision_after_prepare",
    )
    .await;
    system
        .create_table(
            "CREATE TABLE bank50 (id int4, balance int4); CREATE TABLE bank150 (id int4, balance int4)",
        )
        .await;
    let client = system.sql(0).await;
    client
        .simple_query("INSERT INTO bank50 VALUES (1, 100); INSERT INTO bank150 VALUES (1, 100)")
        .await
        .unwrap();
    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query("UPDATE bank50 SET balance = 90 WHERE id = 1")
        .await
        .unwrap();
    client
        .simple_query("UPDATE bank150 SET balance = 110 WHERE id = 1")
        .await
        .unwrap();
    let error = client
        .simple_query("COMMIT")
        .await
        .expect_err("prepared phase barrier");
    assert!(
        error
            .as_db_error()
            .is_some_and(|error| error.message().contains("before global decision"))
    );
    let participant = system.pid(1);
    system.kill(0).await;
    system.clear_commit_fault();
    system.restart(0).await;
    assert_eq!(
        system.pid(1),
        participant,
        "prepared participant remains live"
    );

    // restart returns only after the child's recovery-complete readiness event.
    // These reads and the following transaction prove the prepared locks were
    // settled before SQL serving was published.
    let recovered = system.sql(0).await;
    let left: i32 = recovered
        .query_one("SELECT balance FROM bank50 WHERE id = 1", &[])
        .await
        .unwrap()
        .get(0);
    let right: i32 = recovered
        .query_one("SELECT balance FROM bank150 WHERE id = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!((left, right, left + right), (100, 100, 200));
    recovered.simple_query("BEGIN").await.unwrap();
    recovered
        .simple_query("UPDATE bank50 SET balance = 99 WHERE id = 1")
        .await
        .unwrap();
    recovered
        .simple_query("UPDATE bank150 SET balance = 101 WHERE id = 1")
        .await
        .unwrap();
    recovered.simple_query("COMMIT").await.unwrap();
    system.shutdown().await;
}

/// After a restart of the range-0 host, timestamp grants must resume strictly
/// before the assembled SQL topology serves. The warming transport activates the
/// oracle as soon as range 0 itself recovers, and SQL stays gated behind the
/// full prologue.
#[tokio::test]
async fn real_range0_restart_serves_timestamp_grants_before_sql_topology() {
    use std::time::Duration;

    use assert2::assert;

    let mut system = ProcessHarness::start_all_on_zero("tenant-real-early-tso").await;
    let seed = system.sql(0).await;
    seed.simple_query("CREATE TABLE early_tso (id int4, balance int4)")
        .await
        .expect("create table");
    for chunk in 0_i32..4 {
        let values = (0_i32..50)
            .map(|offset| format!("({}, 1)", chunk * 50 + offset))
            .collect::<Vec<_>>()
            .join(", ");
        seed.simple_query(&format!("INSERT INTO early_tso VALUES {values}"))
            .await
            .expect("seed rows");
    }
    drop(seed);

    system.kill(0).await;
    // Spawn the replacement without waiting for its recovery-complete
    // readiness event, so the warming window is observable. The stable
    // proxies only re-point at readiness, so probe the address the child
    // binds early and logs (its log file was truncated by the respawn).
    system.restart_spawned(0, "r0,r1");
    let client = system.operator_control_client();
    let deadline = std::time::Instant::now() + Duration::from_mins(1);
    let warming_endpoint = loop {
        assert!(
            std::time::Instant::now() < deadline,
            "warming range transport never bound: {}",
            system.log(0)
        );
        if let Some(endpoint) = range_listen_endpoint(&system.log(0)) {
            break endpoint;
        }
        tokio::time::sleep(Duration::from_millis(3)).await;
    };

    // Probe SQL before grants inside each iteration: a strict ordering of
    // the first-success indexes proves grants served during a window in
    // which the SQL topology still refused.
    let mut iteration = 0_u64;
    let mut first_grant = None;
    let first_sql = loop {
        assert!(
            std::time::Instant::now() < deadline,
            "assembled SQL topology never served: {}",
            system.log(0)
        );
        if range0_sql_probe_ok(&client, &warming_endpoint).await {
            break iteration;
        }
        if first_grant.is_none() && range0_grant_probe_ok(&client, &warming_endpoint).await {
            first_grant = Some(iteration);
        }
        iteration += 1;
        tokio::time::sleep(Duration::from_millis(3)).await;
    };
    let first_grant = first_grant.expect("grants must serve before the assembled SQL topology");
    assert!(first_grant < first_sql);

    system.finish_restart(0).await;

    // Full serving still arrives with the seeded rows intact.
    let recovered = system.sql(0).await;
    let count: i64 = recovered
        .query_one("SELECT count(*) FROM early_tso", &[])
        .await
        .expect("count seeded rows")
        .get(0);
    assert!(count == 200);
    system.shutdown().await;
}

/// Extract the early-bound range transport address from a child's log.
///
/// The child logs through a piped `tracing` writer that keeps the ANSI color
/// codes. This function therefore scans for the loopback address digits, and
/// does not split on a `field=` boundary that can carry escape sequences.
fn range_listen_endpoint(log: &str) -> Option<String> {
    log.lines()
        .filter(|line| line.contains("range compute listening"))
        .find_map(|line| {
            let start = line.rfind("127.0.0.1:")?;
            let port = line[start + "127.0.0.1:".len()..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>();
            (!port.is_empty()).then(|| format!("127.0.0.1:{port}"))
        })
}

async fn range0_sql_probe_ok(client: &crabka_gres_ranges::FramedTcpClient, endpoint: &str) -> bool {
    matches!(
        client
            .call(
                endpoint,
                &crabka_gres_ranges::RangeRequest::Sql {
                    range_id: RangeId::new(0),
                    sql: "SELECT 1".to_string(),
                },
            )
            .await,
        Ok(crabka_gres_ranges::RangeResponse::Sql { .. }
            | crabka_gres_ranges::RangeResponse::SqlResults { .. })
    )
}

async fn range0_grant_probe_ok(
    client: &crabka_gres_ranges::FramedTcpClient,
    endpoint: &str,
) -> bool {
    matches!(
        client
            .call(
                endpoint,
                &crabka_gres_ranges::RangeRequest::Tso(crabka_gres_ranges::TsoReq::Grant {
                    count: 1,
                }),
            )
            .await,
        Ok(crabka_gres_ranges::RangeResponse::Tso(
            crabka_gres_ranges::TsoResp::Granted { .. }
        ))
    )
}
