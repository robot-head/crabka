#![allow(clippy::missing_panics_doc, clippy::too_many_lines)]

#[path = "../../gres-ranges/tests/harness/process.rs"]
mod process;

use std::{path::PathBuf, sync::Arc, time::Instant};

use async_trait::async_trait;
use crabka_gres_control::{Registry, SplitOperationPhase, SplitOperationRecord, TenantName};
use crabka_operator::{
    context::{GresControlHandle, GresControlLike, GresControlWriteError},
    controller::{
        gres_split_operation::{
            MtlsRangeMutationClient, RangeMutationClient, reconcile_activated_cutover,
            reconcile_one_rpc_phase, verify_target_topology_ready,
        },
        gres_tenant::reconcile_one_retiring_range_wal,
    },
};
use process::ProcessHarness;
use tokio::sync::Mutex;

struct BrokerControl {
    registry: Mutex<Registry>,
}

#[async_trait]
impl GresControlLike for BrokerControl {
    async fn get_tenant(
        &self,
        tenant: &TenantName,
    ) -> Result<Option<crabka_gres_control::TenantRecord>, GresControlWriteError> {
        Ok(self.registry.lock().await.get(tenant.as_str()).await?)
    }

    async fn replace_tenant_if_version(
        &self,
        record: &crabka_gres_control::TenantRecord,
        expected: Option<u64>,
    ) -> Result<crabka_gres_control::TenantRecord, GresControlWriteError> {
        Ok(self
            .registry
            .lock()
            .await
            .replace_if_version(record, expected)
            .await?)
    }

    async fn delete_tenant(&self, tenant: &TenantName) -> Result<(), GresControlWriteError> {
        self.registry.lock().await.delete(tenant.as_str()).await?;
        Ok(())
    }

    async fn validate_final_checkpoint_manifest(
        &self,
        _record: &crabka_gres_control::TenantRecord,
    ) -> Result<(), GresControlWriteError> {
        Ok(())
    }

    async fn compare_and_swap_split_operation(
        &self,
        expected: u64,
        operation: &SplitOperationRecord,
    ) -> Result<SplitOperationRecord, GresControlWriteError> {
        Ok(self
            .registry
            .lock()
            .await
            .compare_and_swap_split_operation(Some(expected), operation)
            .await?)
    }
}

fn cli_binary() -> PathBuf {
    std::env::var_os("CRABKA_G8_CLI_BIN").map_or_else(
        || {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join("target/debug/crabka")
        },
        PathBuf::from,
    )
}

async fn initiate_move_with_cli(system: &ProcessHarness, operation_id: &str) {
    let endpoint = format!("localhost:{}", system.endpoints()[1].1);
    let output = tokio::process::Command::new(cli_binary())
        .args([
            "gres",
            "move",
            "--bootstrap",
            system.bootstrap(),
            "--tenant",
            system.tenant(),
            "--source-range-id",
            "1",
            "--table",
            "50",
            "--operation-id",
            operation_id,
            "--replacement-range-id",
            "2",
            "--replacement-endpoint",
            &endpoint,
            "--replacement-wal-generation",
            "1",
        ])
        .output()
        .await
        .expect("run actual crabka CLI");
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn load_operation(bootstrap: &str, tenant: &str, operation_id: &str) -> SplitOperationRecord {
    let mut registry = Registry::connect(bootstrap).await.expect("registry");
    registry
        .load_split_operation(tenant, operation_id)
        .await
        .expect("load operation")
        .expect("journaled operation")
}

async fn drive_operation(
    system: &ProcessHarness,
    operation_id: &str,
    max_operation_duration: std::time::Duration,
) -> SplitOperationRecord {
    let tenant = TenantName::try_from(system.tenant()).expect("tenant");
    let mut registry = Registry::connect(system.bootstrap())
        .await
        .expect("registry");
    registry.ensure_topic(1).await.expect("registry topic");
    let control: GresControlHandle = Arc::new(BrokerControl {
        registry: Mutex::new(registry),
    });
    let mutation_client = MtlsRangeMutationClient::new(system.operator_control_client());
    let started = Instant::now();
    loop {
        let current = load_operation(system.bootstrap(), system.tenant(), operation_id).await;
        match current.phase {
            SplitOperationPhase::Activated => {
                let readiness_deadline = Instant::now() + std::time::Duration::from_secs(5);
                loop {
                    match verify_target_topology_ready(&mutation_client, &current).await {
                        Ok(()) => break,
                        Err(error) if Instant::now() < readiness_deadline => {
                            let mut debug_registry = Registry::connect(system.bootstrap())
                                .await
                                .expect("debug registry");
                            let debug_tenant = debug_registry
                                .get(system.tenant())
                                .await
                                .expect("debug tenant")
                                .expect("debug tenant present");
                            let debug_operation = debug_registry
                                .load_split_operation(system.tenant(), operation_id)
                                .await
                                .expect("debug operation")
                                .expect("debug operation present");
                            eprintln!(
                                "target readiness retry: {error}; tenant_version={} phase={:?} source_version={} current_match={} target_match={}",
                                debug_tenant.record_version,
                                debug_operation.phase,
                                debug_operation
                                    .plan
                                    .as_ref()
                                    .expect("plan")
                                    .source_record_version,
                                debug_tenant.ranges
                                    == debug_operation.plan.as_ref().expect("plan").current_layout,
                                debug_tenant.ranges
                                    == debug_operation.plan.as_ref().expect("plan").target_layout,
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                        Err(error) => panic!("target readiness: {error}"),
                    }
                }
                reconcile_activated_cutover(&control, &current)
                    .await
                    .expect("atomic cutover");
            }
            SplitOperationPhase::Retiring => {
                let mut admin =
                    crabka_client_admin::AdminClient::connect(&[system.bootstrap().to_owned()])
                        .await
                        .expect("admin");
                assert!(
                    reconcile_one_retiring_range_wal(&control, &mut admin, &tenant)
                        .await
                        .expect("WAL retirement")
                );
                reconcile_one_rpc_phase(&control, &mutation_client, &current)
                    .await
                    .expect("retire predecessor RPC");
            }
            SplitOperationPhase::Completed => {
                assert!(
                    started.elapsed() < max_operation_duration,
                    "operation exceeded duration bound"
                );
                return current;
            }
            _ => {
                reconcile_one_rpc_phase(&control, &mutation_client, &current)
                    .await
                    .unwrap_or_else(|error| {
                        panic!(
                            "one production reconcile: {error}; source log: {}",
                            system.log(0)
                        )
                    });
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_process_move_cli_operator_and_wal_retirement() {
    if std::env::var_os("CRABKA_G8_PROCESS_NEMESIS").is_none() {
        return;
    }
    assert!(cli_binary().is_file(), "dedicated CI must build crabka CLI");
    let system = ProcessHarness::start_all_on_zero("tenant-g8-live-move").await;
    let mut ddl = String::new();
    for table in 1..50 {
        ddl.push_str(&format!("CREATE TABLE filler_{table} (id int4);"));
    }
    ddl.push_str("CREATE TABLE live_ledger (seq int4) SHARDED");
    let source = system.sql(0).await;
    source.simple_query(&ddl).await.expect("create live ledger");
    for seq in 0..32_i32 {
        source
            .execute("INSERT INTO live_ledger VALUES ($1)", &[&seq])
            .await
            .expect("acknowledged source write");
    }
    initiate_move_with_cli(&system, "g8-live-move").await;
    let operation_started = Instant::now();
    let completed =
        drive_operation(&system, "g8-live-move", std::time::Duration::from_secs(30)).await;
    assert_eq!(completed.phase, SplitOperationPhase::Completed);
    let target = system.sql(0).await;
    let count: i64 = target
        .query_one("SELECT count(*) FROM live_ledger", &[])
        .await
        .expect("target ledger")
        .get(0);
    assert_eq!(count, 32, "exact acknowledged ledger, no resurrection/loss");
    if let Some(path) = std::env::var_os("CRABKA_G8_NEMESIS_EVIDENCE") {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create evidence directory");
        }
        let evidence = serde_json::json!({
            "operation": "move",
            "completed": true,
            "acknowledged_rows": 32,
            "target_rows": count,
            "predecessor_wal_retired": true,
            "operation_elapsed_ms": operation_started.elapsed().as_millis(),
        });
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&evidence).expect("serialize evidence"),
        )
        .expect("write evidence");
    }
    system.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_child_keeps_range_control_receipt_runtime_alive_while_serving() {
    if std::env::var_os("CRABKA_G8_PROCESS_NEMESIS").is_none() {
        return;
    }
    let system = ProcessHarness::start_all_on_zero("tenant-g8-receipt-lifetime").await;
    let client = MtlsRangeMutationClient::new(system.operator_control_client());
    let response = client
        .mutate(
            &system.range_endpoint(0),
            crabka_gres_ranges::RangeControlReq {
                tenant: system.tenant().into(),
                range_id: crabka_gres_ranges::RangeId::COORDINATOR,
                generation: 0,
                operation_id: "missing-operation".into(),
                operation: crabka_gres_ranges::RangeControlOperation::Status,
            },
        )
        .await
        .expect("authenticated control response");
    assert!(
        !matches!(
            response,
            crabka_gres_ranges::RangeControlResp::Rejected { ref code, .. }
                if code == "receipt_store"
        ),
        "serve path must retain the transfer backing durable receipts"
    );
    system.shutdown().await;
}
