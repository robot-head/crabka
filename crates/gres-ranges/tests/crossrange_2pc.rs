use crabka_gres_ranges::{
    GatewayCommitFault, MultiRangeTenant, MultiRangeTenantConfig, TenantName, TransactionDecision,
};
use crabka_pgexec::{SqlEngine, TimestampTransactionId, TimestampWrite};
use crabka_pgkv::Kv;
use crabka_pgwire::engine::{BoundParam, Engine, QueryResult, Session, TxStatus};

fn tenant_config(name: &str) -> MultiRangeTenantConfig {
    MultiRangeTenantConfig::from_boundaries(TenantName::parse(name).expect("tenant"), "0,100,200")
        .expect("config")
}

#[tokio::test]
async fn cross_range_write_transaction_commits_all_participants() {
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_2pc").expect("tenant"),
        "0,100,200",
    )
    .expect("config");
    let (gateway, handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4); CREATE TABLE t250 (id int4)")
        .await
        .expect("create");
    session.simple_query("BEGIN").await.expect("begin");
    session
        .simple_query("INSERT INTO t150 VALUES (1)")
        .await
        .expect("insert first range");
    session
        .simple_query("INSERT INTO t250 VALUES (2)")
        .await
        .expect("insert second range");
    session.simple_query("COMMIT").await.expect("commit");

    assert_eq!(select_ids(&mut session, "SELECT id FROM t150").await, [1]);
    assert_eq!(select_ids(&mut session, "SELECT id FROM t250").await, [2]);

    let records = handles.coordinator().records().await;
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].decision,
        Some(crabka_gres_ranges::TransactionDecision::Commit)
    );
}

#[tokio::test]
async fn cross_range_write_transaction_rollback_hides_all_participants() {
    let (gateway, _handles) =
        MultiRangeTenant::start(tenant_config("tenant_2pc_rollback")).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4); CREATE TABLE t250 (id int4)")
        .await
        .expect("create");
    session.simple_query("BEGIN").await.expect("begin");
    session
        .simple_query("INSERT INTO t150 VALUES (1)")
        .await
        .expect("insert first range");
    session
        .simple_query("INSERT INTO t250 VALUES (2)")
        .await
        .expect("insert second range");

    session.simple_query("ROLLBACK").await.expect("rollback");

    assert!(
        select_ids(&mut session, "SELECT id FROM t150")
            .await
            .is_empty()
    );
    assert!(
        select_ids(&mut session, "SELECT id FROM t250")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn simple_abort_cleanup_clears_cross_range_failed_gateway_state() {
    for (index, cleanup_statement) in cleanup_statements().iter().enumerate() {
        let (gateway, _handles) = MultiRangeTenant::start(tenant_config(&format!(
            "tenant_simple_{index}_{}",
            cleanup_tenant_suffix(cleanup_statement)
        )))
        .expect("tenant");
        let mut session = gateway.connect();

        create_failed_cross_range_transaction(&mut session).await;
        session
            .simple_query(cleanup_statement)
            .await
            .expect("cleanup");
        session.simple_query("BEGIN").await.expect("begin again");
        session.simple_query("ROLLBACK").await.expect("rollback");
    }
}

#[tokio::test]
async fn extended_cleanup_clears_cross_range_failed_gateway_state() {
    for (index, cleanup_statement) in ["ROLLBACK", "ROLLBACK;", "ABORT"].iter().enumerate() {
        let (gateway, _handles) = MultiRangeTenant::start(tenant_config(&format!(
            "tenant_extended_{index}_{}",
            cleanup_tenant_suffix(cleanup_statement)
        )))
        .expect("tenant");
        let mut session = gateway.connect();

        create_failed_cross_range_transaction(&mut session).await;
        session
            .extended_query_v2(cleanup_statement, &[])
            .await
            .expect("extended cleanup");
        session.simple_query("BEGIN").await.expect("begin again");
        session.simple_query("ROLLBACK").await.expect("rollback");
    }
}

#[tokio::test]
async fn extended_insert_in_explicit_transaction_commits_with_gateway_commit() {
    let (gateway, _handles) =
        MultiRangeTenant::start(tenant_config("tenant_extended_commit")).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4)")
        .await
        .expect("create");
    session.simple_query("BEGIN").await.expect("begin");
    session
        .extended_query_v2("INSERT INTO t150 VALUES ($1)", &[text_param("7")])
        .await
        .expect("extended insert");
    session
        .extended_query_v2("COMMIT", &[])
        .await
        .expect("extended commit");

    assert_eq!(select_ids(&mut session, "SELECT id FROM t150").await, [7]);
    assert_eq!(session.tx_status(), TxStatus::Idle);
}

#[tokio::test]
async fn extended_cross_range_writes_commit_with_gateway_commit() {
    let (gateway, _handles) =
        MultiRangeTenant::start(tenant_config("tenant_extended_cross_range_commit"))
            .expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4); CREATE TABLE t250 (id int4)")
        .await
        .expect("create");
    session.simple_query("BEGIN").await.expect("begin");
    session
        .extended_query_v2("INSERT INTO t150 VALUES ($1)", &[text_param("7")])
        .await
        .expect("extended insert first range");
    session
        .extended_query_v2("INSERT INTO t250 VALUES ($1)", &[text_param("8")])
        .await
        .expect("extended insert second range");
    session
        .extended_query_v2("COMMIT", &[])
        .await
        .expect("extended commit");

    assert_eq!(select_ids(&mut session, "SELECT id FROM t150").await, [7]);
    assert_eq!(select_ids(&mut session, "SELECT id FROM t250").await, [8]);
    assert_eq!(session.tx_status(), TxStatus::Idle);
}

#[tokio::test]
async fn extended_valid_commit_forms_commit_open_gateway_transaction() {
    for (index, commit_statement) in ["COMMIT", "COMMIT;"].iter().enumerate() {
        let (gateway, _handles) = MultiRangeTenant::start(tenant_config(&format!(
            "tenant_extended_commit_form_{index}"
        )))
        .expect("tenant");
        let mut session = gateway.connect();
        let row_id = i32::try_from(index).expect("index fits i32") + 20;

        session
            .simple_query("CREATE TABLE t150 (id int4)")
            .await
            .expect("create");
        session.simple_query("BEGIN").await.expect("begin");
        session
            .extended_query_v2(
                "INSERT INTO t150 VALUES ($1)",
                &[text_param(&row_id.to_string())],
            )
            .await
            .expect("extended insert");
        session
            .extended_query_v2(commit_statement, &[])
            .await
            .expect("extended commit");

        assert_eq!(
            select_ids(&mut session, "SELECT id FROM t150").await,
            [row_id]
        );
        assert_eq!(session.tx_status(), TxStatus::Idle);
    }
}

#[tokio::test]
async fn extended_invalid_commit_like_statements_do_not_commit_open_gateway_transaction() {
    for (index, invalid_commit_statement) in ["COMMITMENT", "COMMIT garbage"].iter().enumerate() {
        let (gateway, _handles) = MultiRangeTenant::start(tenant_config(&format!(
            "tenant_extended_invalid_commit_{index}"
        )))
        .expect("tenant");
        let mut session = gateway.connect();
        let row_id = i32::try_from(index).expect("index fits i32") + 30;

        session
            .simple_query("CREATE TABLE t150 (id int4)")
            .await
            .expect("create");
        session.simple_query("BEGIN").await.expect("begin");
        session
            .extended_query_v2(
                "INSERT INTO t150 VALUES ($1)",
                &[text_param(&row_id.to_string())],
            )
            .await
            .expect("extended insert");
        session
            .extended_query_v2(invalid_commit_statement, &[])
            .await
            .expect_err("invalid commit-like statement rejected");
        session
            .extended_query_v2("ROLLBACK", &[])
            .await
            .expect("extended rollback");

        assert!(
            select_ids(&mut session, "SELECT id FROM t150")
                .await
                .is_empty()
        );
        assert_eq!(session.tx_status(), TxStatus::Idle);
    }
}

#[tokio::test]
async fn extended_insert_in_explicit_transaction_rolls_back_with_gateway_cleanup() {
    for (index, cleanup_statement) in ["ROLLBACK", "ABORT"].iter().enumerate() {
        let (gateway, _handles) =
            MultiRangeTenant::start(tenant_config(&format!("tenant_extended_rollback_{index}")))
                .expect("tenant");
        let mut session = gateway.connect();

        session
            .simple_query("CREATE TABLE t150 (id int4)")
            .await
            .expect("create");
        session.simple_query("BEGIN").await.expect("begin");
        session
            .extended_query_v2("INSERT INTO t150 VALUES ($1)", &[text_param("11")])
            .await
            .expect("extended insert");
        session
            .extended_query_v2(cleanup_statement, &[])
            .await
            .expect("extended cleanup");

        assert!(
            select_ids(&mut session, "SELECT id FROM t150")
                .await
                .is_empty()
        );
        assert_eq!(session.tx_status(), TxStatus::Idle);
    }
}

#[tokio::test]
async fn extended_routed_error_marks_failed_until_cleanup_clears() {
    let (gateway, _handles) =
        MultiRangeTenant::start(tenant_config("tenant_extended_error_failed")).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4)")
        .await
        .expect("create");
    session.simple_query("BEGIN").await.expect("begin");
    session
        .extended_query_v2("INSERT INTO t150 VALUES ($1)", &[text_param("13")])
        .await
        .expect("extended insert");
    let error = session
        .extended_query_v2("INSERT INTO t150 VALUES ($1)", &[text_param("not-an-int")])
        .await
        .expect_err("bad extended insert marks failed");
    assert_ne!(error.code, "25P02");

    let error = session
        .extended_query_v2("SELECT id FROM t150", &[])
        .await
        .expect_err("failed transaction rejects extended statement");
    assert_eq!(error.code, "25P02");

    let error = session
        .extended_query_v2("COMMIT", &[])
        .await
        .expect_err("failed transaction rejects extended commit");
    assert_eq!(error.code, "25P02");

    let error = session
        .extended_query_v2("SELECT id FROM t150", &[])
        .await
        .expect_err("failed transaction remains failed after extended commit");
    assert_eq!(error.code, "25P02");

    session
        .extended_query_v2("ROLLBACK", &[])
        .await
        .expect("extended cleanup");
    assert!(
        select_ids(&mut session, "SELECT id FROM t150")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn extended_invalid_commit_like_statements_do_not_clear_failed_gateway_state() {
    let (gateway, _handles) =
        MultiRangeTenant::start(tenant_config("tenant_failed_invalid_commit")).expect("tenant");
    let mut session = gateway.connect();

    create_failed_cross_range_transaction(&mut session).await;
    for invalid_commit_statement in ["COMMITMENT", "COMMIT garbage"] {
        let error = session
            .extended_query_v2(invalid_commit_statement, &[])
            .await
            .expect_err("invalid commit-like statement rejected while failed");
        assert_eq!(error.code, "25P02");
    }

    let error = session
        .extended_query_v2("SELECT id FROM t150", &[])
        .await
        .expect_err("failed transaction remains failed");
    assert_eq!(error.code, "25P02");

    session
        .extended_query_v2("ROLLBACK", &[])
        .await
        .expect("extended rollback");
    assert!(
        select_ids(&mut session, "SELECT id FROM t150")
            .await
            .is_empty()
    );
    assert_eq!(session.tx_status(), TxStatus::Idle);
}

#[tokio::test]
async fn invalid_rollback_prefix_does_not_clear_failed_gateway_state() {
    let (gateway, _handles) =
        MultiRangeTenant::start(tenant_config("tenant_invalid_prefix")).expect("tenant");
    let mut session = gateway.connect();

    create_failed_cross_range_transaction(&mut session).await;

    for invalid_cleanup in ["rollback_bogus", "abortive"] {
        let error = session
            .simple_query(invalid_cleanup)
            .await
            .expect_err("invalid prefix rejected while failed");
        assert_eq!(error.code, "25P02");
    }
    let error = session
        .extended_query_v2("SELECT id FROM t150", &[])
        .await
        .expect_err("extended statement remains failed");
    assert_eq!(error.code, "25P02");

    session.simple_query("ROLLBACK").await.expect("rollback");
    session.simple_query("BEGIN").await.expect("begin again");
    session
        .simple_query("ROLLBACK")
        .await
        .expect("rollback again");
}

#[tokio::test]
async fn rollback_after_statement_error_cleans_touched_participants() {
    let (gateway, _handles) =
        MultiRangeTenant::start(tenant_config("tenant_failed_participants")).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4)")
        .await
        .expect("create");
    session.simple_query("BEGIN").await.expect("begin");
    session
        .simple_query("INSERT INTO t150 VALUES (1)")
        .await
        .expect("insert");
    let error = session
        .simple_query("INSERT INTO t150 VALUES ('not-an-int')")
        .await
        .expect_err("bad insert marks gateway failed");
    assert_ne!(error.code, "25P02");

    for rejected_statement in [
        "SELECT id FROM t150",
        "CREATE TABLE t151 (id int4)",
        "COMMIT",
    ] {
        let error = session
            .simple_query(rejected_statement)
            .await
            .expect_err("failed transaction rejects statement");
        assert_eq!(error.code, "25P02");
    }

    session.simple_query("ROLLBACK").await.expect("rollback");
    let results = session
        .simple_query("SELECT id FROM t150")
        .await
        .expect("select after cleanup");
    let [QueryResult::Rows { rows, .. }] = results.as_slice() else {
        panic!("expected rows")
    };
    assert!(rows.is_empty());

    session.simple_query("BEGIN").await.expect("begin cleanly");
    session
        .simple_query("INSERT INTO t150 VALUES (2)")
        .await
        .expect("insert after cleanup");
    session.simple_query("COMMIT").await.expect("commit");
}

#[tokio::test]
async fn commit_failure_after_prepare_keeps_gateway_transaction_cleanable() {
    let config = tenant_config("tenant_commit_prepare_failure")
        .with_commit_fault_for_testing(GatewayCommitFault::BeforeDecisionAfterPrepare);
    let (gateway, handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4); CREATE TABLE t250 (id int4)")
        .await
        .expect("create");
    session.simple_query("BEGIN").await.expect("begin");
    session
        .simple_query("INSERT INTO t150 VALUES (1)")
        .await
        .expect("insert first range");
    session
        .simple_query("INSERT INTO t250 VALUES (2)")
        .await
        .expect("insert second range");

    let error = session
        .simple_query("COMMIT")
        .await
        .expect_err("injected commit failure");
    assert_eq!(error.code, "XX000");
    let error = session
        .simple_query("SELECT id FROM t150")
        .await
        .expect_err("failed commit keeps transaction failed");
    assert_eq!(error.code, "25P02");

    session.simple_query("ROLLBACK").await.expect("rollback");

    assert!(
        select_ids(&mut session, "SELECT id FROM t150")
            .await
            .is_empty()
    );
    assert!(
        select_ids(&mut session, "SELECT id FROM t250")
            .await
            .is_empty()
    );
    assert_eq!(session.tx_status(), TxStatus::Idle);
    let records = handles.coordinator().records().await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].decision, Some(TransactionDecision::Abort));
}

#[tokio::test]
async fn rollback_after_commit_decision_failure_uses_known_commit_recovery_decision() {
    let config = tenant_config("tenant_known_commit_recovery")
        .with_commit_fault_for_testing(GatewayCommitFault::BeforeReleaseAfterCommitDecision);
    let (gateway, handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4); CREATE TABLE t250 (id int4)")
        .await
        .expect("create");
    session.simple_query("BEGIN").await.expect("begin");
    session
        .simple_query("INSERT INTO t150 VALUES (30)")
        .await
        .expect("insert first range");
    session
        .simple_query("INSERT INTO t250 VALUES (40)")
        .await
        .expect("insert second range");

    let error = session
        .simple_query("COMMIT")
        .await
        .expect_err("injected commit failure leaves committed decision");
    assert_eq!(error.code, "XX000");
    let records = handles.coordinator().records().await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].decision, Some(TransactionDecision::Commit));

    session
        .simple_query("ROLLBACK")
        .await
        .expect("rollback releases participants by effective commit decision");

    assert_eq!(select_ids(&mut session, "SELECT id FROM t150").await, [30]);
    assert_eq!(select_ids(&mut session, "SELECT id FROM t250").await, [40]);
    assert_eq!(session.tx_status(), TxStatus::Idle);
    let records = handles.coordinator().records().await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].decision, Some(TransactionDecision::Commit));
}

#[tokio::test]
async fn rollback_after_unknown_decision_recovery_respects_existing_commit_decision() {
    let config = tenant_config("tenant_unknown_decision_recovery").with_commit_fault_for_testing(
        GatewayCommitFault::AfterCommitDecisionWithoutRecoveryMetadata,
    );
    let (gateway, handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4); CREATE TABLE t250 (id int4)")
        .await
        .expect("create");
    session.simple_query("BEGIN").await.expect("begin");
    session
        .simple_query("INSERT INTO t150 VALUES (50)")
        .await
        .expect("insert first range");
    session
        .simple_query("INSERT INTO t250 VALUES (60)")
        .await
        .expect("insert second range");

    let error = session
        .simple_query("COMMIT")
        .await
        .expect_err("injected commit failure leaves committed decision without recovery metadata");
    assert_eq!(error.code, "XX000");
    let records = handles.coordinator().records().await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].decision, Some(TransactionDecision::Commit));

    session
        .simple_query("ROLLBACK")
        .await
        .expect("rollback attempts abort cleanup and follows effective commit decision");

    assert_eq!(select_ids(&mut session, "SELECT id FROM t150").await, [50]);
    assert_eq!(select_ids(&mut session, "SELECT id FROM t250").await, [60]);
    assert_eq!(session.tx_status(), TxStatus::Idle);
    let records = handles.coordinator().records().await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].decision, Some(TransactionDecision::Commit));
}

#[tokio::test]
async fn commit_failure_after_decision_keeps_release_retryable() {
    let config = tenant_config("tenant_commit_release_failure")
        .with_commit_fault_for_testing(GatewayCommitFault::BeforeReleaseAfterCommitDecision);
    let (gateway, handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4); CREATE TABLE t250 (id int4)")
        .await
        .expect("create");
    session.simple_query("BEGIN").await.expect("begin");
    session
        .simple_query("INSERT INTO t150 VALUES (10)")
        .await
        .expect("insert first range");
    session
        .simple_query("INSERT INTO t250 VALUES (20)")
        .await
        .expect("insert second range");

    session
        .simple_query("COMMIT")
        .await
        .expect_err("injected release failure");
    session
        .simple_query("ROLLBACK")
        .await
        .expect("rollback retries committed release cleanup");

    assert_eq!(select_ids(&mut session, "SELECT id FROM t150").await, [10]);
    assert_eq!(select_ids(&mut session, "SELECT id FROM t250").await, [20]);
    assert_eq!(session.tx_status(), TxStatus::Idle);
    let records = handles.coordinator().records().await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].decision, Some(TransactionDecision::Commit));
}

#[tokio::test]
async fn autocommit_dml_errors_do_not_poison_gateway_transaction_state() {
    let (gateway, _handles) =
        MultiRangeTenant::start(tenant_config("tenant_autocommit_error")).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t150 (id int4)")
        .await
        .expect("create");
    let error = session
        .simple_query("INSERT INTO t150 VALUES ('not-an-int')")
        .await
        .expect_err("autocommit insert fails");
    assert_ne!(error.code, "25P02");

    session
        .simple_query("BEGIN")
        .await
        .expect("begin after error");
    session
        .extended_query_v2("INSERT INTO t150 VALUES ($1)", &[text_param("3")])
        .await
        .expect("extended insert after autocommit error");
    session.simple_query("COMMIT").await.expect("commit");
}

#[tokio::test]
async fn literal_row_key_scatter_insert_commits_two_ranges_atomically() {
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_timestamp_scatter").expect("tenant"),
        "0,50:0,50:10",
    )
    .expect("config");
    let (gateway, _handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t50 (id int4) SHARDED")
        .await
        .expect("create");
    session
        .simple_query("INSERT INTO t50 VALUES (1), (11)")
        .await
        .expect("atomic scatter insert");

    assert_eq!(
        select_ids(&mut session, "SELECT id FROM t50 ORDER BY id").await,
        [1, 11]
    );
}

#[tokio::test]
async fn restart_physically_resolves_committed_timestamp_scatter_before_conflicting_write() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_timestamp_recovery").expect("tenant"),
        "0,50:0,50:10",
    )
    .expect("config")
    .with_data_dir(data_dir.path().to_path_buf())
    .with_commit_fault_for_testing(GatewayCommitFault::AfterTimestampCommitDecision);
    let (gateway, handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t50 (id int4) SHARDED")
        .await
        .expect("create");
    session
        .simple_query("INSERT INTO t50 VALUES (1), (11)")
        .await
        .expect_err("crash after the durable commit decision");
    drop(session);
    drop(gateway);
    drop(handles);

    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_timestamp_recovery").expect("tenant"),
        "0,50:0,50:10",
    )
    .expect("config")
    .with_data_dir(data_dir.path().to_path_buf());
    let (gateway, _handles) = MultiRangeTenant::start(config).expect("recovered tenant");
    let mut session = gateway.connect();
    assert_eq!(
        select_ids(&mut session, "SELECT id FROM t50 ORDER BY id").await,
        [1, 11]
    );
    session
        .simple_query("INSERT INTO t50 VALUES (1)")
        .await
        .expect("conflicting write succeeds after committed intents are resolved");
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the durable recovery timeline is clearest as one end-to-end behavior test"
)]
async fn restart_durably_aborts_matching_global_index_intents_without_touching_other_timestamps() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_timestamp_abort_recovery").expect("tenant"),
        "0,50:0,50:10",
    )
    .expect("config")
    .with_data_dir(data_dir.path().to_path_buf());
    let (gateway, handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();
    session
        .simple_query("CREATE TABLE t50 (id int4) SHARDED")
        .await
        .expect("create");
    session
        .simple_query("CREATE GLOBAL INDEX t50_id_idx ON t50 (id)")
        .await
        .expect("create global index");
    drop(session);
    drop(gateway);
    drop(handles);

    let coordinator = SqlEngine::open(data_dir.path().join("r0")).expect("open coordinator");
    let mut participant = SqlEngine::open(data_dir.path().join("r1")).expect("open participant");
    participant.set_catalog_kv(coordinator.kv_handle());
    let table =
        crabka_pgcatalog::get_table(coordinator.kv_handle().as_ref(), "t50").expect("table");
    let index_id = crabka_pgcatalog::list_table_indexes(coordinator.kv_handle().as_ref(), "t50")
        .expect("indexes")
        .into_iter()
        .find(|index| index.name == "t50_id_idx")
        .expect("global index")
        .id;
    let start_ts = TimestampTransactionId::new(10).expect("start timestamp");
    let identity = crabka_pgexec::TimestampTxnIdentity {
        start_ts,
        global_xid: 9,
        primary_range: 0,
    };
    let matching_write = TimestampWrite {
        table_id: table.id,
        rowid: 11,
        row: vec![crabka_pgtypes::Datum::Int4(11)],
        delete: false,
        global_index_intents: vec![crabka_pgexec::timestamp_txn::GlobalIndexIntent {
            index_id,
            indexed_values: vec![crabka_pgtypes::Datum::Int4(11)],
            base_table_id: table.id,
            base_rowid: 11,
            unique: false,
            delete: false,
        }],
    };
    coordinator
        .begin_timestamp_transaction(&crabka_pgexec::TimestampTxnDescriptor::begun(
            start_ts,
            identity.global_xid,
            vec![1],
        ))
        .await
        .expect("write pending descriptor");
    participant
        .timestamp_txn_participant(1)
        .prewrite_with_primary(identity, std::slice::from_ref(&matching_write))
        .await
        .expect("prewrite matching timestamp intent");

    let other_start_ts =
        TimestampTransactionId::new(start_ts.get() + 1_000).expect("distinct timestamp");
    let other_write = TimestampWrite {
        table_id: table.id,
        rowid: 99,
        row: vec![crabka_pgtypes::Datum::Int4(99)],
        delete: false,
        global_index_intents: vec![crabka_pgexec::timestamp_txn::GlobalIndexIntent {
            index_id,
            indexed_values: vec![crabka_pgtypes::Datum::Int4(99)],
            base_table_id: table.id,
            base_rowid: 99,
            unique: false,
            delete: false,
        }],
    };
    participant
        .timestamp_txn_participant(1)
        .prewrite(other_start_ts, std::slice::from_ref(&other_write))
        .await
        .expect("prewrite other timestamp intent");
    assert_global_index_intent_timestamps(
        participant.kv_handle().as_ref(),
        &[start_ts.get(), other_start_ts.get()],
    );
    drop(coordinator);
    drop(participant);

    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_timestamp_abort_recovery").expect("tenant"),
        "0,50:0,50:10",
    )
    .expect("config")
    .with_data_dir(data_dir.path().to_path_buf());
    let (gateway, handles) = MultiRangeTenant::start(config).expect("first recovery");
    drop(gateway);
    drop(handles);

    let participant =
        SqlEngine::open(data_dir.path().join("r1")).expect("open settled participant");
    assert_global_index_intent_timestamps(
        participant.kv_handle().as_ref(),
        &[other_start_ts.get()],
    );
    assert_matching_timestamp_state(
        participant.kv_handle().as_ref(),
        start_ts.get(),
        crabka_pgmvcc::version::TsVersionState::Aborted,
    );
    assert_no_timestamp_sidecars(participant.kv_handle().as_ref(), start_ts.get());
    drop(participant);

    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_timestamp_abort_recovery").expect("tenant"),
        "0,50:0,50:10",
    )
    .expect("config")
    .with_data_dir(data_dir.path().to_path_buf());
    let (gateway, handles) = MultiRangeTenant::start(config).expect("repeat recovery");
    drop(gateway);
    drop(handles);

    let participant =
        SqlEngine::open(data_dir.path().join("r1")).expect("reopen settled participant");
    assert_global_index_intent_timestamps(
        participant.kv_handle().as_ref(),
        &[other_start_ts.get()],
    );
    assert_matching_timestamp_state(
        participant.kv_handle().as_ref(),
        start_ts.get(),
        crabka_pgmvcc::version::TsVersionState::Aborted,
    );
    assert_no_timestamp_sidecars(participant.kv_handle().as_ref(), start_ts.get());
}

fn assert_global_index_intent_timestamps(kv: &dyn Kv, expected: &[u64]) {
    let mut timestamps = kv
        .scan_prefix(b"\0\0\0\0index/ts_intent/")
        .expect("scan global-index intents")
        .into_iter()
        .map(|(_key, value)| {
            u64::from_be_bytes(
                value[..8]
                    .try_into()
                    .expect("global-index intent timestamp"),
            )
        })
        .collect::<Vec<_>>();
    timestamps.sort_unstable();
    assert_eq!(timestamps, expected);
}

fn assert_matching_timestamp_state(
    kv: &dyn Kv,
    start_ts: u64,
    expected_state: crabka_pgmvcc::version::TsVersionState,
) {
    assert!(
        kv.scan_range(
            &crabka_pgkv::key::table_prefix(crabka_pgkv::key::SYSTEM_TABLE_ID + 1),
            &[0xFF; 5],
        )
        .expect("scan timestamp tuples")
        .into_iter()
        .filter_map(|(_key, value)| crabka_pgmvcc::version::decode_ts_tuple(&value).ok())
        .any(|tuple| tuple.start_ts == start_ts && tuple.state == expected_state)
    );
}

fn assert_no_timestamp_sidecars(kv: &dyn Kv, start_ts: u64) {
    let timestamp = start_ts.to_be_bytes();
    assert!(
        kv.scan_prefix(b"\0\0\0\0meta/ts_prewrite/")
            .expect("scan prewrite reservations")
            .into_iter()
            .all(|(_key, value)| value.as_slice() != timestamp)
    );
    assert!(
        kv.scan_prefix(b"\0\0\0\0meta/ts_intent/")
            .expect("scan timestamp identities")
            .into_iter()
            .all(|(key, _value)| !key.ends_with(&timestamp))
    );
}

fn text_param(value: &str) -> BoundParam {
    BoundParam {
        type_oid: None,
        format: 0,
        value: Some(value.as_bytes().to_vec().into()),
    }
}

fn cleanup_statements() -> &'static [&'static str] {
    &[
        "ROLLBACK",
        "ROLLBACK WORK",
        "ROLLBACK TRANSACTION",
        "ABORT",
        "ABORT WORK",
        "ABORT TRANSACTION",
        " rollback ; ",
        " abort work ; ",
    ]
}

fn cleanup_tenant_suffix(cleanup_statement: &str) -> String {
    cleanup_statement
        .split_ascii_whitespace()
        .map(|token| token.trim_matches(';'))
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>()
        .join("_")
        .to_ascii_lowercase()
}

async fn create_failed_cross_range_transaction(
    session: &mut crabka_gres_ranges::tenant::GatewaySession,
) {
    session
        .simple_query("CREATE TABLE t150 (id int4); CREATE TABLE t250 (id int4)")
        .await
        .expect("create");
    session.simple_query("BEGIN").await.expect("begin");
    session
        .simple_query("INSERT INTO t150 VALUES (1)")
        .await
        .expect("insert first range");
    let error = session
        .simple_query("INSERT INTO t150 VALUES ('not-an-int')")
        .await
        .expect_err("participant error marks transaction failed");
    assert_ne!(error.code, "25P02");
}

async fn select_ids(
    session: &mut crabka_gres_ranges::tenant::GatewaySession,
    sql: &str,
) -> Vec<i32> {
    let results = session.simple_query(sql).await.expect(sql);
    let [QueryResult::Rows { rows, .. }] = results.as_slice() else {
        panic!("expected rows")
    };
    rows.iter()
        .map(|row| {
            let cell = row[0].as_ref().expect("cell");
            std::str::from_utf8(&cell.text)
                .expect("utf8")
                .parse::<i32>()
                .expect("i32")
        })
        .collect()
}
mod support;

use support::ExtendedQueryV2 as _;
