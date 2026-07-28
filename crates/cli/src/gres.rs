//! `crabka gres` subcommands.

use std::{
    io::Read as _,
    num::NonZeroU16,
    path::{Path, PathBuf},
};

use clap::{ArgGroup, Args, Subcommand, ValueEnum};
use crabka_client_admin::{
    AclEntry, AclOperation, AdminClient, PatternType, PermissionType, ResourceType, ScramUpsertion,
};
use crabka_client_core::security::{ClientSecurity, SaslCredentials};
use crabka_gres_balancer::{
    BalanceOperation, BalancerConfig, ExecutionPolicy, ExecutionReport, Planner, TenantMetrics,
    UnsupportedExecutor, execute_plan,
};
use crabka_gres_control::{
    HashPlacement, PgdogConnectAttempts, PgdogGeneral, PgdogPoolerMode, PgdogRenderInput,
    PgdogUser, PositiveI32, PositiveMillis, RangeBoundary, RangeLayoutEntry, RangeLayoutSplit,
    RangeLifecycle, Registry, RegistryPolicy, RegistryReplicationFactor, SplitOperationPlan,
    SplitOperationRecord, SqlUser, TenantEndpoint, TenantId, TenantName, TenantRecord, TenantState,
    render_pgdog_toml, render_users_toml, tenant_config_topic,
};
use crabka_security::{ListenerProtocol, SaslMechanism, scram::PgScramVerifier};
use crabka_units::{Time, convert::TimeExt as _};
use serde::Serialize;

/// A validated positive millisecond count as a time extent.
///
/// [`PositiveMillis`] is `crabka-gres-control`'s parse-level validator over the
/// raw `u64` a CLI flag carries; this is the seam where it becomes a quantity.
/// [`TimeExt::from_millis`] takes an `i64`, so a value past `i64::MAX`
/// milliseconds saturates rather than wrapping negative.
fn positive_millis(value: PositiveMillis) -> Time {
    Time::from_millis(i64::try_from(value.into_value()).unwrap_or(i64::MAX))
}

const EXIT_OK: i32 = 0;
const EXIT_ERROR: i32 = 1;
const DEFAULT_WAL_REPLICATION: i32 = 1;
const DEFAULT_SCRAM_ITERATIONS: u32 = 4096;
const DEFAULT_BACKEND_PORT: u16 = 5432;

#[derive(Args, Debug)]
pub struct GresArgs {
    #[command(flatten)]
    registry: RegistryOptions,
    #[command(subcommand)]
    command: GresCommand,
}

#[derive(Args, Debug)]
struct RegistryOptions {
    #[arg(
        long = "registry-replication-factor",
        env = "CRABKA_GRES_REGISTRY_REPLICATION_FACTOR",
        default_value = "1"
    )]
    replication_factor: RegistryReplicationFactor,
    #[arg(
        long = "registry-topic-create-timeout-ms",
        env = "CRABKA_GRES_REGISTRY_TOPIC_CREATE_TIMEOUT_MS",
        default_value = "15000"
    )]
    topic_create_timeout_ms: PositiveI32,
    #[arg(
        long = "registry-reader-retry-backoff-ms",
        env = "CRABKA_GRES_REGISTRY_READER_RETRY_BACKOFF_MS",
        default_value = "250"
    )]
    reader_retry_backoff_ms: PositiveMillis,
    #[arg(
        long = "registry-fetch-max-wait-ms",
        env = "CRABKA_GRES_REGISTRY_FETCH_MAX_WAIT_MS",
        default_value = "500"
    )]
    fetch_max_wait_ms: PositiveI32,
    #[arg(
        long = "registry-fetch-partition-max-bytes",
        env = "CRABKA_GRES_REGISTRY_FETCH_PARTITION_MAX_BYTES",
        default_value = "1048576"
    )]
    fetch_partition_max_bytes: PositiveI32,
    #[arg(
        long = "registry-producer-dns-timeout-ms",
        env = "CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT_MS"
    )]
    producer_dns_timeout_ms: Option<PositiveMillis>,
    #[arg(
        long = "registry-reader-admin-dns-timeout-ms",
        env = "CRABKA_GRES_REGISTRY_READER_ADMIN_DNS_TIMEOUT_MS"
    )]
    reader_admin_dns_timeout_ms: Option<PositiveMillis>,
}

impl RegistryOptions {
    fn policy(&self) -> RegistryPolicy {
        let producer_dns_timeout_ms = self.producer_dns_timeout_ms.map_or_else(
            || {
                RegistryPolicy::default()
                    .producer_dns_timeout()
                    .milliseconds()
            },
            PositiveMillis::into_value,
        );
        let reader_admin_dns_timeout_ms = self.reader_admin_dns_timeout_ms.map_or_else(
            || {
                RegistryPolicy::default()
                    .reader_admin_dns_timeout()
                    .milliseconds()
            },
            PositiveMillis::into_value,
        );

        RegistryPolicy::new(
            self.replication_factor.into_value(),
            self.topic_create_timeout_ms.into_value(),
            self.reader_retry_backoff_ms.into_value(),
            self.fetch_max_wait_ms.into_value(),
            self.fetch_partition_max_bytes.into_value(),
        )
        .expect("validated registry options")
        .with_producer_dns_timeout_ms(producer_dns_timeout_ms)
        .expect("validated registry producer DNS timeout")
        .with_reader_admin_dns_timeout_ms(reader_admin_dns_timeout_ms)
        .expect("validated registry reader/admin DNS timeout")
    }
}

#[derive(Subcommand, Debug)]
enum GresCommand {
    /// Create or replace a Gres tenant registry record.
    CreateTenant(CreateTenantArgs),
    /// Describe one Gres tenant registry record.
    Describe(TenantNameArgs),
    /// List Gres tenant registry records.
    List(BootstrapArgs),
    /// Mark one Gres tenant suspended.
    Suspend(TenantNameArgs),
    /// Mark one Gres tenant active.
    Resume(TenantNameArgs),
    /// Initiate a sealed, journaled two-successor range split.
    Split(SplitRangeArgs),
    /// Initiate a sealed, journaled one-for-one range move.
    Move(MoveRangeArgs),
    /// Delete one Gres tenant registry record.
    Delete(TenantNameArgs),
    /// Render `PgDog` configuration files from the tenant registry.
    RenderPgdog(RenderPgdogArgs),
    /// Plan Gres range balancing from a JSON metrics snapshot without writes.
    BalanceDryRun(BalanceDryRunArgs),
    /// Validate or apply a Gres range balancing plan from a JSON metrics snapshot.
    BalanceApply(BalanceApplyArgs),
    /// Probe named-topic metadata using SCRAM credentials.
    #[command(hide = true)]
    ProbeTopicRead(ProbeTopicReadArgs),
}

#[derive(Args, Debug)]
struct ProbeTopicReadArgs {
    #[arg(long)]
    bootstrap: String,
    #[arg(long)]
    topic: String,
    #[arg(long)]
    username: String,
    #[arg(long)]
    password_file: PathBuf,
}

#[derive(Args, Debug)]
#[command(group(
    ArgGroup::new("password_source")
        .required(true)
        .args(["password_file", "password_stdin"])
))]
struct CreateTenantArgs {
    /// Kafka bootstrap address used for the Gres registry.
    #[arg(long)]
    bootstrap: String,
    /// Tenant name and route identity.
    #[arg(long)]
    name: String,
    /// `PostgreSQL` login role for the tenant.
    #[arg(long = "user")]
    sql_user: String,
    /// Read the `PostgreSQL` password from this file.
    #[arg(long)]
    password_file: Option<PathBuf>,
    /// Read the `PostgreSQL` password from stdin.
    #[arg(long)]
    password_stdin: bool,
    /// WAL topic replication factor for this tenant.
    #[arg(long, default_value_t = DEFAULT_WAL_REPLICATION)]
    wal_replication: i32,
    /// Optional object-store prefix for tenant checkpoints.
    #[arg(long)]
    bucket_prefix: Option<String>,
    /// Optional frame threshold for checkpointing.
    #[arg(long)]
    checkpoint_frames: Option<u64>,
    /// Optional byte threshold for checkpointing.
    #[arg(long)]
    checkpoint_bytes: Option<u64>,
    /// Idle seconds before automatic suspension. Zero means never.
    #[arg(long)]
    idle_seconds: Option<u64>,
    /// Comma-separated table, table:rowid, or table:bucket:rowid boundaries.
    #[arg(long)]
    ranges: Option<String>,
    /// Hash placement as `TABLE:COLUMN[,COLUMN...]:BUCKETS[:COLOCATION_GROUP]`.
    #[arg(long = "hash-placement", value_parser = parse_hash_placement)]
    hash_placements: Vec<HashPlacement>,
}

#[derive(Args, Debug)]
struct TenantNameArgs {
    /// Kafka bootstrap address used for the Gres registry.
    #[arg(long)]
    bootstrap: String,
    /// Tenant name.
    #[arg(long)]
    name: String,
}

#[derive(Args, Debug)]
struct BootstrapArgs {
    /// Kafka bootstrap address used for the Gres registry.
    #[arg(long)]
    bootstrap: String,
}

#[derive(Args, Debug)]
struct RenderPgdogArgs {
    /// Kafka bootstrap address used for the Gres registry.
    #[arg(long, env = "CRABKA_GRES_PGDOG_BOOTSTRAP")]
    bootstrap: String,
    /// Directory that will receive pgdog.toml and users.toml.
    #[arg(long, env = "CRABKA_GRES_PGDOG_OUT_DIR")]
    out_dir: PathBuf,
    /// Suspended-tenant activator route as host:port.
    #[arg(
        long,
        env = "CRABKA_GRES_PGDOG_ACTIVATOR",
        value_parser = parse_activator
    )]
    activator: Option<(String, u16)>,
    /// Client-facing `PgDog` listen port.
    #[arg(long, env = "CRABKA_GRES_PGDOG_LISTEN_PORT", default_value = "6432")]
    listen_port: NonZeroU16,
    /// Client-facing TLS certificate path as visible inside the `PgDog` runtime.
    #[arg(
        long,
        env = "CRABKA_GRES_PGDOG_TLS_CERTIFICATE",
        requires = "tls_private_key"
    )]
    tls_certificate: Option<PathBuf>,
    /// Client-facing TLS private-key path as visible inside the `PgDog` runtime.
    #[arg(
        long,
        env = "CRABKA_GRES_PGDOG_TLS_PRIVATE_KEY",
        requires = "tls_certificate"
    )]
    tls_private_key: Option<PathBuf>,
    /// Client CA path as visible inside the `PgDog` runtime.
    #[arg(
        long,
        env = "CRABKA_GRES_PGDOG_TLS_CLIENT_CA_CERTIFICATE",
        requires_all = ["tls_certificate", "tls_private_key"]
    )]
    tls_client_ca_certificate: Option<PathBuf>,
    /// Fleet-wide backend connection pooling mode.
    #[arg(
        long,
        env = "CRABKA_GRES_PGDOG_POOLER_MODE",
        default_value = "transaction",
        value_parser = parse_pgdog_pooler_mode
    )]
    pooler_mode: PgdogPoolerMode,
    /// Number of backend connection attempts.
    #[arg(long, env = "CRABKA_GRES_PGDOG_CONNECT_ATTEMPTS", default_value = "3")]
    connect_attempts: PgdogConnectAttempts,
    /// Maximum acceptable tenant wake latency in milliseconds.
    #[arg(
        long,
        env = "CRABKA_GRES_PGDOG_COLD_START_CEILING_MS",
        default_value = "30000"
    )]
    cold_start_ceiling_ms: PositiveMillis,
    /// Normal pooled-server idle timeout in milliseconds.
    #[arg(
        long,
        env = "CRABKA_GRES_PGDOG_IDLE_TIMEOUT_MS",
        default_value = "60000"
    )]
    idle_timeout_ms: PositiveMillis,
    /// Pooled-server idle timeout when at least one tenant may suspend.
    #[arg(
        long,
        env = "CRABKA_GRES_PGDOG_SUSPENSION_IDLE_TIMEOUT_MS",
        default_value = "1000"
    )]
    suspension_idle_timeout_ms: PositiveMillis,
    /// Maximum pooled backend connection lifetime in milliseconds.
    #[arg(
        long,
        env = "CRABKA_GRES_PGDOG_SERVER_LIFETIME_MS",
        default_value = "300000"
    )]
    server_lifetime_ms: PositiveMillis,
}

#[derive(Args, Debug)]
struct SplitRangeArgs {
    /// Kafka bootstrap address used for the Gres registry.
    #[arg(long)]
    bootstrap: String,
    /// Tenant name.
    tenant: String,
    /// Table identifier that starts the successor range.
    table: u64,
    /// Row identifier at the split point.
    rowid: u64,
    /// Hash bucket at the split point; required exactly for hash-sharded tables.
    #[arg(long)]
    bucket: Option<u32>,
    /// Durable idempotency key for this split.
    #[arg(long)]
    operation_id: String,
    /// Left successor range id. Range zero may remain the left successor.
    #[arg(long)]
    left_range_id: Option<u32>,
    /// Left successor compute endpoint.
    #[arg(long)]
    left_endpoint: Option<String>,
    /// New successor range id. Defaults to max(existing range id) + 1.
    #[arg(long)]
    successor_range_id: Option<u32>,
    /// Successor compute endpoint. Defaults to the operator service naming convention.
    #[arg(long)]
    successor_endpoint: Option<String>,
    /// Successor WAL generation. Defaults to the source range generation.
    #[arg(long)]
    successor_wal_generation: Option<u64>,
}

#[derive(Args, Debug)]
struct MoveRangeArgs {
    #[arg(long)]
    bootstrap: String,
    /// Tenant name.
    #[arg(long)]
    tenant: String,
    /// Existing range to replace.
    #[arg(long)]
    source_range_id: u32,
    /// Catalog table identity used to seal the routing epoch.
    #[arg(long)]
    table: u64,
    /// Durable idempotency key.
    #[arg(long)]
    operation_id: String,
    /// Fresh replacement range id.
    #[arg(long)]
    replacement_range_id: u32,
    /// Fresh replacement compute endpoint.
    #[arg(long)]
    replacement_endpoint: Option<String>,
    /// Fresh replacement WAL generation. Defaults to predecessor + 1.
    #[arg(long)]
    replacement_wal_generation: Option<u64>,
}

#[derive(Args, Debug)]
struct BalanceDryRunArgs {
    /// JSON metrics snapshot path. Use `-` to read from stdin.
    #[arg(long)]
    metrics_file: PathBuf,
}

#[derive(Args, Debug)]
struct BalanceApplyArgs {
    /// JSON metrics snapshot path. Use `-` to read from stdin.
    #[arg(long)]
    metrics_file: PathBuf,
    /// Execution adapter to use. `validate` reports unsupported physical-operation hooks.
    #[arg(long, value_enum, default_value = "validate")]
    execute_mode: BalanceExecuteMode,
    /// Continue after a live operation fails or is unsupported.
    #[arg(long)]
    continue_on_failure: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum BalanceExecuteMode {
    /// Keep legacy dry-run behavior: plan only, no executor hooks.
    DryRun,
    /// Run through the executor seam without mutating registry state.
    Validate,
}

#[derive(Debug, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct BalanceDryRunInput {
    #[serde(default)]
    config: BalancerConfig,
    tenants: Vec<TenantMetrics>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct BalanceDryRunOutput {
    dry_run: bool,
    goals_applied: Vec<String>,
    operations: Vec<BalanceOperation>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct BalanceApplyOutput {
    dry_run: bool,
    execute_mode: &'static str,
    execution_policy: &'static str,
    goals_applied: Vec<String>,
    report: ExecutionReport,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct RedactedTenantRecord {
    record_version: u64,
    id: String,
    name: String,
    state: TenantState,
    sql_user: String,
    scram_verifier: &'static str,
    wal_replication: i32,
    bucket_prefix: Option<String>,
    checkpoint_frames: Option<u64>,
    checkpoint_bytes: Option<u64>,
    idle_seconds: Option<u64>,
    ranges: Vec<RangeLayoutEntry>,
}

pub async fn run(args: GresArgs) -> i32 {
    match run_inner(args).await {
        Ok(()) => EXIT_OK,
        Err(error) => {
            eprintln!("crabka gres: {error}");
            EXIT_ERROR
        }
    }
}

async fn run_inner(args: GresArgs) -> Result<(), String> {
    let policy = args.registry.policy();
    match args.command {
        GresCommand::CreateTenant(args) => create_tenant(args, &policy).await,
        GresCommand::Describe(args) => describe_tenant(args, &policy).await,
        GresCommand::List(args) => list_tenants(args, &policy).await,
        GresCommand::Suspend(args) => {
            change_tenant_state(args, TenantState::Suspended, &policy).await
        }
        GresCommand::Resume(args) => change_tenant_state(args, TenantState::Active, &policy).await,
        GresCommand::Split(args) => split_range(args, &policy).await,
        GresCommand::Move(args) => move_range(args, &policy).await,
        GresCommand::Delete(args) => delete_tenant(args, &policy).await,
        GresCommand::RenderPgdog(args) => render_pgdog(args, &policy).await,
        GresCommand::BalanceDryRun(args) => balance_dry_run(&args),
        GresCommand::BalanceApply(args) => balance_apply(&args),
        GresCommand::ProbeTopicRead(args) => probe_topic_read(&args).await,
    }
}

async fn probe_topic_read(args: &ProbeTopicReadArgs) -> Result<(), String> {
    let password = std::fs::read_to_string(&args.password_file)
        .map_err(|error| format!("read Kafka password file: {error}"))?;
    let password = password.trim_end_matches(['\r', '\n']);
    if password.is_empty() {
        return Err("Kafka password file is empty".to_string());
    }
    let bootstrap_addrs = args
        .bootstrap
        .split(',')
        .map(str::trim)
        .filter(|address| !address.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let security = ClientSecurity {
        protocol: ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: Some(SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha512,
            username: args.username.clone(),
            password: password.to_string(),
        }),
        sasl_host: None,
    };
    let mut admin = AdminClient::connect_secured(&bootstrap_addrs, Some(security))
        .await
        .map_err(|error| format!("Kafka probe connect: {error}"))?;
    let metadata = admin
        .metadata(&[args.topic.as_str()])
        .await
        .map_err(|error| format!("Kafka probe metadata: {error}"))?;
    let topic = metadata
        .topics
        .into_iter()
        .find(|topic| topic.name == args.topic)
        .ok_or_else(|| format!("Kafka probe response omitted topic {}", args.topic))?;
    if let Some(error) = topic.error {
        return Err(format!(
            "topic {} metadata: {} ({})",
            args.topic, error.name, error.code
        ));
    }
    println!("topic {} is readable", args.topic);
    Ok(())
}

fn balance_dry_run(args: &BalanceDryRunArgs) -> Result<(), String> {
    let input = read_balance_input(&args.metrics_file)?;
    let output = plan_balance_dry_run(&input);
    print_json(&output)
}

fn balance_apply(args: &BalanceApplyArgs) -> Result<(), String> {
    let input = read_balance_input(&args.metrics_file)?;
    let policy = execution_policy(args.continue_on_failure);
    let output = match args.execute_mode {
        BalanceExecuteMode::DryRun | BalanceExecuteMode::Validate => {
            plan_balance_apply(&input, args.execute_mode, policy)
        }
    };
    print_json(&output)?;
    if output.report.has_terminal_error() {
        return Err(format!(
            "{} mode found unsupported or failed balance operations; policy is {}",
            output.execute_mode, output.execution_policy
        ));
    }
    Ok(())
}

fn read_balance_input(path: &Path) -> Result<BalanceDryRunInput, String> {
    let json = if path == Path::new("-") {
        let mut input = String::new();
        std::io::stdin()
            .read_to_string(&mut input)
            .map_err(|e| format!("read metrics stdin: {e}"))?;
        input
    } else {
        std::fs::read_to_string(path).map_err(|e| format!("read metrics file: {e}"))?
    };
    serde_json::from_str(&json).map_err(|e| format!("parse metrics JSON: {e}"))
}

fn plan_balance_dry_run(input: &BalanceDryRunInput) -> BalanceDryRunOutput {
    let planner = Planner::from_config(&input.config);
    let output = planner.plan(&input.tenants, &input.config.context);
    BalanceDryRunOutput {
        dry_run: true,
        goals_applied: output.goals_applied,
        operations: output.plan.operations,
    }
}

fn plan_balance_apply(
    input: &BalanceDryRunInput,
    mode: BalanceExecuteMode,
    policy: ExecutionPolicy,
) -> BalanceApplyOutput {
    let planner = Planner::from_config(&input.config);
    let output = planner.plan(&input.tenants, &input.config.context);
    let report = match mode {
        BalanceExecuteMode::DryRun => {
            crabka_gres_balancer::DryRunExecutor::default().execute(&output.plan)
        }
        BalanceExecuteMode::Validate => {
            let mut executor = UnsupportedExecutor;
            execute_plan(&mut executor, &output.plan, policy)
        }
    };
    balance_apply_output(mode, policy, output.goals_applied, report)
}

fn balance_apply_output(
    mode: BalanceExecuteMode,
    policy: ExecutionPolicy,
    goals_applied: Vec<String>,
    report: ExecutionReport,
) -> BalanceApplyOutput {
    BalanceApplyOutput {
        dry_run: report.dry_run,
        execute_mode: mode.as_str(),
        execution_policy: execution_policy_name(policy),
        goals_applied,
        report,
    }
}

impl BalanceExecuteMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::DryRun => "dry-run",
            Self::Validate => "validate",
        }
    }
}

const fn execution_policy(continue_on_failure: bool) -> ExecutionPolicy {
    if continue_on_failure {
        return ExecutionPolicy::BestEffort;
    }
    ExecutionPolicy::StopOnFailure
}

const fn execution_policy_name(policy: ExecutionPolicy) -> &'static str {
    match policy {
        ExecutionPolicy::StopOnFailure => "stop-on-failure",
        ExecutionPolicy::BestEffort => "best-effort",
    }
}

async fn split_range(args: SplitRangeArgs, policy: &RegistryPolicy) -> Result<(), String> {
    let tenant_name = TenantName::try_from(args.tenant.as_str()).map_err(|e| e.to_string())?;
    let mut registry = connect_registry(&args.bootstrap, policy).await?;
    let record = registry
        .get(tenant_name.as_str())
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("tenant {tenant_name} not found"))?;
    let boundary = split_boundary(&record, args.table, args.bucket, args.rowid)?;
    let source = source_range_for_key(&record, boundary)?;
    let first_free = next_range_id(&record);
    let left_range_id =
        args.left_range_id
            .unwrap_or(if source.range_id == 0 { 0 } else { first_free });
    let successor_range_id = args
        .successor_range_id
        .unwrap_or_else(|| first_free.max(left_range_id.saturating_add(1)));
    let left_endpoint = args.left_endpoint.unwrap_or_else(|| {
        if left_range_id == source.range_id {
            source.endpoint.clone()
        } else {
            range_endpoint(&tenant_name, left_range_id)
        }
    });
    let successor_endpoint = args
        .successor_endpoint
        .unwrap_or_else(|| range_endpoint(&tenant_name, successor_range_id));
    let successor_wal_generation = args
        .successor_wal_generation
        .unwrap_or_else(|| source.wal_generation.saturating_add(1));
    let successor = |range_id, end_key, endpoint: String| RangeLayoutEntry {
        range_id,
        end_key,
        endpoint,
        wal_generation: successor_wal_generation,
        lifecycle: RangeLifecycle::Serving,
        retirement: None,
    };
    let split = RangeLayoutSplit {
        source_range_id: source.range_id,
        predecessor_generation: source.wal_generation,
        left: successor(left_range_id, Some(boundary), left_endpoint),
        right: successor(successor_range_id, source.end_key, successor_endpoint),
    };
    let target = record
        .clone()
        .split_range_layout(split.clone())
        .map_err(|e| e.to_string())?;
    let operation =
        SplitOperationRecord::new(tenant_name.clone(), args.operation_id.clone(), split)
            .map_err(|e| e.to_string())?
            .with_plan(SplitOperationPlan {
                source_record_version: record.record_version,
                source_map_epoch: record.record_version,
                routing_table_id: args.table,
                current_layout: record.ranges.clone(),
                target_layout: target.ranges,
            })
            .map_err(|e| e.to_string())?;
    registry
        .begin_split_operation(&operation)
        .await
        .map_err(|e| e.to_string())?;
    println!(
        "initiated split {} for tenant {tenant_name} range {} at table {} bucket {:?} rowid {} into ranges {} and {}",
        args.operation_id,
        source.range_id,
        args.table,
        args.bucket,
        args.rowid,
        left_range_id,
        successor_range_id
    );
    Ok(())
}

fn split_boundary(
    record: &TenantRecord,
    table_id: u64,
    bucket: Option<u32>,
    rowid: u64,
) -> Result<RangeBoundary, String> {
    let placement = record
        .hash_placements
        .iter()
        .find(|placement| placement.table_id == table_id);
    match (placement, bucket) {
        (Some(placement), Some(bucket)) if bucket < placement.bucket_count => {
            Ok(RangeBoundary::hash(table_id, bucket, rowid))
        }
        (Some(_), Some(_)) => Err("--bucket must be less than the table hash bucket count".into()),
        (Some(_), None) => Err("--bucket is required for a hash-sharded table split".into()),
        (None, Some(_)) => Err("--bucket is only valid for a hash-sharded table split".into()),
        (None, None) => Ok(RangeBoundary::new(table_id, rowid)),
    }
}

async fn move_range(args: MoveRangeArgs, policy: &RegistryPolicy) -> Result<(), String> {
    let tenant_name = TenantName::try_from(args.tenant.as_str()).map_err(|e| e.to_string())?;
    let mut registry = connect_registry(&args.bootstrap, policy).await?;
    let record = registry
        .get(tenant_name.as_str())
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("tenant {tenant_name} not found"))?;
    let operation = build_move_operation(&tenant_name, &record, &args)?;
    let replacement = &operation.move_intent().expect("sealed move").replacement;
    registry
        .begin_split_operation(&operation)
        .await
        .map_err(|e| e.to_string())?;
    println!(
        "initiated move {} for tenant {tenant_name} range {} generation {} to range {} generation {}",
        args.operation_id,
        operation.source_range_id(),
        operation.predecessor_generation(),
        replacement.range_id,
        replacement.wal_generation,
    );
    Ok(())
}

fn build_move_operation(
    tenant_name: &TenantName,
    record: &TenantRecord,
    args: &MoveRangeArgs,
) -> Result<SplitOperationRecord, String> {
    let source_index = record
        .ranges
        .iter()
        .position(|range| range.range_id == args.source_range_id)
        .ok_or_else(|| {
            format!(
                "range {} is not in tenant {tenant_name}",
                args.source_range_id
            )
        })?;
    let source = &record.ranges[source_index];
    if args.replacement_range_id == source.range_id
        || record
            .ranges
            .iter()
            .any(|range| range.range_id == args.replacement_range_id)
    {
        return Err("replacement range id must be fresh and differ from the predecessor".into());
    }
    let replacement = RangeLayoutEntry {
        range_id: args.replacement_range_id,
        end_key: source.end_key,
        endpoint: args
            .replacement_endpoint
            .clone()
            .unwrap_or_else(|| range_endpoint(tenant_name, args.replacement_range_id)),
        wal_generation: args
            .replacement_wal_generation
            .unwrap_or_else(|| source.wal_generation.saturating_add(1)),
        lifecycle: RangeLifecycle::Serving,
        retirement: None,
    };
    let mut target_layout = record.ranges.clone();
    target_layout[source_index] = replacement.clone();
    SplitOperationRecord::new_move(
        tenant_name.clone(),
        args.operation_id.clone(),
        source.range_id,
        source.wal_generation,
        replacement,
    )
    .map_err(|e| e.to_string())?
    .with_plan(SplitOperationPlan {
        source_record_version: record.record_version,
        source_map_epoch: record.record_version,
        routing_table_id: args.table,
        current_layout: record.ranges.clone(),
        target_layout,
    })
    .map_err(|e| e.to_string())
}

async fn create_tenant(args: CreateTenantArgs, policy: &RegistryPolicy) -> Result<(), String> {
    let password = read_password(&args)?;
    let mut registry = connect_registry(&args.bootstrap, policy).await?;
    let current = registry.get(&args.name).await.map_err(|e| e.to_string())?;
    let expected_record_version = current.as_ref().map(|record| record.record_version);
    let record_version = next_record_version(expected_record_version)?;
    let record = build_create_tenant_record(&args, &password, record_version)?;
    registry
        .replace_if_version(&record, expected_record_version)
        .await
        .map_err(|e| e.to_string())?;
    let record = registry
        .get(&args.name)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("tenant {} disappeared after replacement", args.name))?;
    registry
        .upsert_tenant_config(&record, args.wal_replication)
        .await
        .map_err(|e| e.to_string())?;
    provision_tenant_kafka_access(&args.bootstrap, &record.name, &password).await?;
    println!("created tenant {}", record.name);
    Ok(())
}

async fn provision_tenant_kafka_access(
    bootstrap: &str,
    tenant: &TenantName,
    password: &str,
) -> Result<(), String> {
    let bootstrap_addrs = bootstrap
        .split(',')
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if bootstrap_addrs.is_empty() {
        return Err("bootstrap address list must not be empty".to_string());
    }
    let username = tenant_kafka_username(tenant);
    let mut admin = AdminClient::connect(&bootstrap_addrs)
        .await
        .map_err(|e| format!("tenant Kafka admin connect: {e}"))?;
    admin
        .alter_user_scram_credentials_sha512(
            &[ScramUpsertion {
                username: username.clone(),
                password: password.to_string(),
                iterations: i32::try_from(DEFAULT_SCRAM_ITERATIONS)
                    .map_err(|_| "default SCRAM iterations exceed i32".to_string())?,
            }],
            &[],
        )
        .await
        .map_err(|e| format!("tenant Kafka SCRAM provision: {e}"))?;
    admin
        .create_acls(&tenant_acls(&format!("User:{username}"), tenant))
        .await
        .map_err(|e| format!("tenant Kafka ACL provision: {e}"))?;
    Ok(())
}

fn tenant_acls(principal: &str, tenant: &TenantName) -> Vec<AclEntry> {
    let mut acls = Vec::new();
    for (resource_name, pattern_type) in [
        (format!("__gres_wal.{tenant}"), PatternType::Prefixed),
        (tenant_config_topic(tenant), PatternType::Literal),
    ] {
        for operation in [
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Create,
            AclOperation::Delete,
            AclOperation::Describe,
        ] {
            acls.push(AclEntry {
                resource_type: ResourceType::Topic,
                resource_name: resource_name.clone(),
                pattern_type,
                principal: principal.to_string(),
                host: "*".to_string(),
                operation,
                permission_type: PermissionType::Allow,
            });
        }
    }
    for operation in [AclOperation::Write, AclOperation::Describe] {
        acls.push(AclEntry {
            resource_type: ResourceType::TransactionalId,
            resource_name: format!("__gres.{tenant}"),
            pattern_type: PatternType::Prefixed,
            principal: principal.to_string(),
            host: "*".to_string(),
            operation,
            permission_type: PermissionType::Allow,
        });
    }
    for operation in [AclOperation::Create, AclOperation::IdempotentWrite] {
        acls.push(AclEntry {
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster".to_string(),
            pattern_type: PatternType::Literal,
            principal: principal.to_string(),
            host: "*".to_string(),
            operation,
            permission_type: PermissionType::Allow,
        });
    }
    acls
}

fn tenant_kafka_username(tenant: &TenantName) -> String {
    format!("gres-{tenant}")
}

async fn describe_tenant(args: TenantNameArgs, policy: &RegistryPolicy) -> Result<(), String> {
    let mut registry = connect_registry(&args.bootstrap, policy).await?;
    let tenant = registry
        .get(&args.name)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("tenant {} not found", args.name))?;
    print_json(&redact_tenant(&tenant))
}

async fn list_tenants(args: BootstrapArgs, policy: &RegistryPolicy) -> Result<(), String> {
    let mut registry = connect_registry(&args.bootstrap, policy).await?;
    let tenants = registry.list().await.map_err(|e| e.to_string())?;
    let redacted = tenants.iter().map(redact_tenant).collect::<Vec<_>>();
    print_json(&redacted)
}

async fn change_tenant_state(
    args: TenantNameArgs,
    state: TenantState,
    policy: &RegistryPolicy,
) -> Result<(), String> {
    let mut registry = connect_registry(&args.bootstrap, policy).await?;
    match state {
        TenantState::Suspended => registry.mark_suspended(&args.name).await,
        TenantState::Active => {
            registry
                .mark_active(
                    &args.name,
                    format!("{}.gres.svc:{DEFAULT_BACKEND_PORT}", args.name),
                )
                .await
        }
        TenantState::Parking => {
            return Err(
                "tenant parking is managed by the controller while old WAL topics are deleted"
                    .to_string(),
            );
        }
        TenantState::ResumeRequested => {
            return Err(
                "tenant resume requests are created by the activator after it receives traffic"
                    .to_string(),
            );
        }
    }
    .map_err(|e| e.to_string())?;
    println!("updated tenant {}", args.name);
    Ok(())
}

async fn delete_tenant(args: TenantNameArgs, policy: &RegistryPolicy) -> Result<(), String> {
    let mut registry = connect_registry(&args.bootstrap, policy).await?;
    registry
        .delete(&args.name)
        .await
        .map_err(|e| e.to_string())?;
    println!("deleted tenant {}", args.name);
    Ok(())
}

async fn render_pgdog(args: RenderPgdogArgs, policy: &RegistryPolicy) -> Result<(), String> {
    let mut registry = connect_registry(&args.bootstrap, policy).await?;
    let tenants = registry.list().await.map_err(|e| e.to_string())?;
    render_pgdog_files(&tenants, &args)
}

async fn connect_registry(bootstrap: &str, policy: &RegistryPolicy) -> Result<Registry, String> {
    let mut registry = Registry::connect_with_policy(bootstrap, policy.clone())
        .await
        .map_err(|e| e.to_string())?;
    registry.ensure_topic().await.map_err(|e| e.to_string())?;
    Ok(registry)
}

fn build_create_tenant_record(
    args: &CreateTenantArgs,
    password: &str,
    record_version: u64,
) -> Result<TenantRecord, String> {
    let name = TenantName::try_from(args.name.as_str()).map_err(|e| e.to_string())?;
    let id = TenantId::try_from(args.name.as_str()).map_err(|e| e.to_string())?;
    let sql_user = SqlUser::try_from(args.sql_user.as_str()).map_err(|e| e.to_string())?;
    let verifier = PgScramVerifier::generate(password, DEFAULT_SCRAM_ITERATIONS)
        .map_err(|e| e.to_string())?
        .to_string();
    let ranges = parse_range_layout(&name, args.ranges.as_deref())?;
    let mut record = TenantRecord::new(
        record_version,
        id,
        name.clone(),
        TenantState::Active,
        sql_user,
        verifier,
        args.wal_replication,
    )
    .map_err(|e| e.to_string())?;
    record.bucket_prefix.clone_from(&args.bucket_prefix);
    record.checkpoint_frames = args.checkpoint_frames;
    record.checkpoint_bytes = args.checkpoint_bytes;
    record.idle_seconds = args.idle_seconds;
    record.ranges = ranges;
    record.hash_placements.clone_from(&args.hash_placements);
    record.ensure_valid().map_err(|e| e.to_string())?;
    Ok(record)
}

fn parse_range_layout(
    tenant: &TenantName,
    ranges: Option<&str>,
) -> Result<Vec<RangeLayoutEntry>, String> {
    let Some(ranges) = ranges else {
        return Ok(Vec::new());
    };
    let boundaries = ranges
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(parse_range_boundary)
        .collect::<Result<Vec<_>, _>>()?;
    if boundaries.is_empty() {
        return Err("--ranges must contain at least one boundary".to_string());
    }
    if boundaries[0] != RangeBoundary::table_start(0)
        || boundaries
            .windows(2)
            .any(|pair| range_boundary_key(pair[0]) >= range_boundary_key(pair[1]))
    {
        return Err("--ranges boundaries must be strictly increasing and start at 0".to_string());
    }
    boundaries
        .iter()
        .enumerate()
        .map(|(index, _boundary)| {
            let range_id = u32::try_from(index).map_err(|_| "too many ranges".to_string())?;
            Ok(RangeLayoutEntry {
                range_id,
                end_key: boundaries.get(index + 1).copied(),
                endpoint: range_endpoint(tenant, range_id),
                wal_generation: 0,
                lifecycle: RangeLifecycle::default(),
                retirement: None,
            })
        })
        .collect()
}

fn parse_range_boundary(token: &str) -> Result<RangeBoundary, String> {
    let invalid =
        |error: &dyn std::fmt::Display| format!("invalid --ranges boundary {token:?}: {error}");
    let parts = token.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        [table] => table
            .parse::<u64>()
            .map(RangeBoundary::table_start)
            .map_err(|error| invalid(&error)),
        [table, rowid] => Ok(RangeBoundary::new(
            table.parse::<u64>().map_err(|error| invalid(&error))?,
            rowid.parse::<u64>().map_err(|error| invalid(&error))?,
        )),
        [table, bucket, rowid] => Ok(RangeBoundary::hash(
            table.parse::<u64>().map_err(|error| invalid(&error))?,
            bucket.parse::<u32>().map_err(|error| invalid(&error))?,
            rowid.parse::<u64>().map_err(|error| invalid(&error))?,
        )),
        _ => Err(format!(
            "invalid --ranges boundary {token:?}: expected table, table:rowid, or table:bucket:rowid"
        )),
    }
}

fn range_boundary_key(boundary: RangeBoundary) -> (u64, u32, u64) {
    (
        boundary.table_id,
        boundary.bucket.unwrap_or(0),
        boundary.rowid,
    )
}

fn parse_hash_placement(value: &str) -> Result<HashPlacement, String> {
    let parts = value.split(':').collect::<Vec<_>>();
    let [table_id, hash_columns, bucket_count, co_location_group @ ..] = parts.as_slice() else {
        return Err(
            "hash placement must be TABLE:COLUMN[,COLUMN...]:BUCKETS[:COLOCATION_GROUP]"
                .to_string(),
        );
    };
    if co_location_group.len() > 1 {
        return Err(
            "hash placement must be TABLE:COLUMN[,COLUMN...]:BUCKETS[:COLOCATION_GROUP]"
                .to_string(),
        );
    }
    let hash_columns = hash_columns
        .split(',')
        .map(str::trim)
        .map(str::to_string)
        .collect::<Vec<_>>();
    Ok(HashPlacement {
        table_id: table_id
            .parse::<u64>()
            .map_err(|error| format!("invalid hash-placement table ID: {error}"))?,
        hash_columns,
        bucket_count: bucket_count
            .parse::<u32>()
            .map_err(|error| format!("invalid hash-placement bucket count: {error}"))?,
        co_location_group: co_location_group.first().map(|group| (*group).to_string()),
    })
}

fn range_endpoint(tenant: &TenantName, range_id: u32) -> String {
    format!("{tenant}-gres-r{range_id}.gres.svc:{DEFAULT_BACKEND_PORT}")
}

fn source_range_for_key(
    record: &TenantRecord,
    key: RangeBoundary,
) -> Result<&RangeLayoutEntry, String> {
    let mut previous_end = RangeBoundary::table_start(0);
    for range in &record.ranges {
        if key >= previous_end && range.end_key.is_none_or(|end| key < end) {
            return Ok(range);
        }
        if let Some(end) = range.end_key {
            previous_end = end;
        }
    }
    Err(format!(
        "key ({}, {}) is not covered by tenant {}",
        key.table_id, key.rowid, record.name
    ))
}

fn next_range_id(record: &TenantRecord) -> u32 {
    record
        .ranges
        .iter()
        .map(|range| range.range_id)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}

fn next_record_version(current: Option<u64>) -> Result<u64, String> {
    current.map_or(Ok(1), |version| {
        version
            .checked_add(1)
            .ok_or_else(|| "record version overflowed".to_string())
    })
}

fn read_password(args: &CreateTenantArgs) -> Result<String, String> {
    let mut password = if let Some(path) = &args.password_file {
        std::fs::read_to_string(path).map_err(|e| format!("read password file: {e}"))?
    } else if args.password_stdin {
        let mut input = String::new();
        std::io::stdin()
            .read_to_string(&mut input)
            .map_err(|e| format!("read password stdin: {e}"))?;
        input
    } else {
        return Err("password source is required".to_string());
    };
    trim_single_trailing_newline(&mut password);
    if password.is_empty() {
        return Err("password must not be empty".to_string());
    }
    Ok(password)
}

fn render_pgdog_files(records: &[TenantRecord], args: &RenderPgdogArgs) -> Result<(), String> {
    std::fs::create_dir_all(&args.out_dir).map_err(|e| format!("create output directory: {e}"))?;
    let endpoints = records.iter().map(tenant_endpoint).collect::<Vec<_>>();
    let users = records
        .iter()
        .map(|record| PgdogUser {
            name: record.sql_user.as_str().to_string(),
            database: record.name.as_str().to_string(),
            password: None,
        })
        .collect();
    let idle_timeout_ms = if records.iter().any(|record| {
        record.idle_seconds.is_some_and(|seconds| seconds > 0)
            && (record.state == TenantState::Active || args.activator.is_some())
    }) {
        args.suspension_idle_timeout_ms
    } else {
        args.idle_timeout_ms
    };
    let input = PgdogRenderInput {
        tenants: &endpoints,
        activator: args.activator.clone(),
        general: PgdogGeneral {
            listen_port: args.listen_port.get(),
            tls_cert_path: args
                .tls_certificate
                .as_deref()
                .map(|path| path.to_string_lossy().into_owned()),
            tls_key_path: args
                .tls_private_key
                .as_deref()
                .map(|path| path.to_string_lossy().into_owned()),
            tls_client_ca_path: args
                .tls_client_ca_certificate
                .as_deref()
                .map(|path| path.to_string_lossy().into_owned()),
            passthrough_auth: true,
            pooler_mode: args.pooler_mode,
            cold_start_ceiling: positive_millis(args.cold_start_ceiling_ms),
            connect_attempts: args.connect_attempts,
            timeouts: None,
            idle_timeout: positive_millis(idle_timeout_ms),
            server_lifetime: positive_millis(args.server_lifetime_ms),
            users,
        },
    };
    let pgdog = render_pgdog_toml(&input).map_err(|e| e.to_string())?;
    let users = render_users_toml(&input).map_err(|e| e.to_string())?;
    std::fs::write(args.out_dir.join("pgdog.toml"), pgdog)
        .map_err(|e| format!("write pgdog.toml: {e}"))?;
    std::fs::write(args.out_dir.join("users.toml"), users)
        .map_err(|e| format!("write users.toml: {e}"))?;
    Ok(())
}

fn tenant_endpoint(record: &TenantRecord) -> TenantEndpoint {
    TenantEndpoint {
        name: record.name.as_str().to_string(),
        backend_host: format!("{}.gres.svc", record.name.as_str()),
        backend_port: DEFAULT_BACKEND_PORT,
        state: record.state,
        pooler_mode: None,
    }
}

fn redact_tenant(record: &TenantRecord) -> RedactedTenantRecord {
    RedactedTenantRecord {
        record_version: record.record_version,
        id: record.id.as_str().to_string(),
        name: record.name.as_str().to_string(),
        state: record.state,
        sql_user: record.sql_user.as_str().to_string(),
        scram_verifier: "<redacted>",
        wal_replication: record.wal_replication,
        bucket_prefix: record.bucket_prefix.clone(),
        checkpoint_frames: record.checkpoint_frames,
        checkpoint_bytes: record.checkpoint_bytes,
        idle_seconds: record.idle_seconds,
        ranges: record.ranges.clone(),
    }
}

fn print_json(value: &impl Serialize) -> Result<(), String> {
    let json = serde_json::to_string_pretty(value).map_err(|e| e.to_string())?;
    println!("{json}");
    Ok(())
}

fn trim_single_trailing_newline(value: &mut String) {
    if value.ends_with("\r\n") {
        value.truncate(value.len() - 2);
        return;
    }
    if value.ends_with('\n') {
        value.pop();
    }
}

fn parse_activator(value: &str) -> Result<(String, u16), String> {
    let (host, port) = value
        .rsplit_once(':')
        .ok_or("activator must be host:port")?;
    if host.is_empty() {
        return Err("activator host must not be empty".to_string());
    }
    let port = port
        .parse::<u16>()
        .map_err(|e| format!("activator port: {e}"))?;
    if port == 0 {
        return Err("activator port must be greater than zero".to_string());
    }
    Ok((host.to_string(), port))
}

fn parse_pgdog_pooler_mode(value: &str) -> Result<PgdogPoolerMode, String> {
    match value {
        "transaction" => Ok(PgdogPoolerMode::Transaction),
        "session" => Ok(PgdogPoolerMode::Session),
        _ => Err("pooler mode must be transaction or session".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use clap::Parser as _;

    use super::*;

    #[derive(clap::Parser)]
    struct TestCli {
        #[command(flatten)]
        gres: GresArgs,
    }

    #[test]
    fn registry_policy_options_use_exact_defaults_and_validation() {
        let defaults =
            TestCli::try_parse_from(["test", "list", "--bootstrap=broker:9092"]).expect("defaults");
        assert!(defaults.gres.registry.policy() == crabka_gres_control::RegistryPolicy::default());
        for option in [
            "--registry-replication-factor=0",
            "--registry-replication-factor=32768",
            "--registry-topic-create-timeout-ms=0",
            "--registry-reader-retry-backoff-ms=0",
            "--registry-fetch-max-wait-ms=0",
            "--registry-fetch-partition-max-bytes=0",
            "--registry-producer-dns-timeout-ms=0",
            "--registry-reader-admin-dns-timeout-ms=0",
        ] {
            assert!(
                TestCli::try_parse_from(["test", option, "list", "--bootstrap=broker:9092",])
                    .is_err()
            );
        }
    }

    #[test]
    fn registry_policy_options_read_environment_and_prefer_cli() {
        const CHILD: &str = "CRABKA_TEST_CLI_REGISTRY_ENV_CHILD";
        let vars = [
            ("CRABKA_GRES_REGISTRY_REPLICATION_FACTOR", "2"),
            ("CRABKA_GRES_REGISTRY_TOPIC_CREATE_TIMEOUT_MS", "15001"),
            ("CRABKA_GRES_REGISTRY_READER_RETRY_BACKOFF_MS", "251"),
            ("CRABKA_GRES_REGISTRY_FETCH_MAX_WAIT_MS", "501"),
            ("CRABKA_GRES_REGISTRY_FETCH_PARTITION_MAX_BYTES", "1048577"),
            ("CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT_MS", "37"),
            ("CRABKA_GRES_REGISTRY_READER_ADMIN_DNS_TIMEOUT_MS", "37"),
        ];
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().expect("test exe"))
                .args([
                    "--exact",
                    "gres::tests::registry_policy_options_read_environment_and_prefer_cli",
                ])
                .env(CHILD, "1")
                .envs(vars)
                .status()
                .expect("child test");
            assert!(status.success());
            return;
        }
        let environment =
            TestCli::try_parse_from(["test", "list", "--bootstrap=broker:9092"]).expect("env");
        let environment_policy = RegistryPolicy::new(2, 15_001, 251, 501, 1_048_577)
            .expect("policy")
            .with_producer_dns_timeout_ms(37)
            .expect("environment DNS timeout")
            .with_reader_admin_dns_timeout_ms(37)
            .expect("environment reader/admin DNS timeout");
        assert!(environment.gres.registry.policy() == environment_policy);
        let cli = TestCli::try_parse_from([
            "test",
            "--registry-replication-factor=3",
            "--registry-topic-create-timeout-ms=15002",
            "--registry-reader-retry-backoff-ms=252",
            "--registry-fetch-max-wait-ms=502",
            "--registry-fetch-partition-max-bytes=1048578",
            "--registry-producer-dns-timeout-ms=47",
            "--registry-reader-admin-dns-timeout-ms=47",
            "list",
            "--bootstrap=broker:9092",
        ])
        .expect("CLI over environment");
        let cli_policy = RegistryPolicy::new(3, 15_002, 252, 502, 1_048_578)
            .expect("policy")
            .with_producer_dns_timeout_ms(47)
            .expect("CLI DNS timeout")
            .with_reader_admin_dns_timeout_ms(47)
            .expect("CLI reader/admin DNS timeout");
        assert!(cli.gres.registry.policy() == cli_policy);
    }

    #[test]
    fn render_pgdog_options_use_exact_defaults() {
        let args = render_pgdog_test_args([
            "test",
            "render-pgdog",
            "--bootstrap=broker:9092",
            "--out-dir=/tmp/pgdog",
        ]);

        assert!(args.bootstrap == "broker:9092");
        assert!(args.out_dir == PathBuf::from("/tmp/pgdog"));
        assert!(args.activator.is_none());
        assert!(args.listen_port.get() == 6_432);
        assert!(args.tls_certificate.is_none());
        assert!(args.tls_private_key.is_none());
        assert!(args.tls_client_ca_certificate.is_none());
        assert!(args.pooler_mode == PgdogPoolerMode::Transaction);
        assert!(args.connect_attempts.into_value() == 3);
        assert!(args.cold_start_ceiling_ms.into_value() == 30_000);
        assert!(args.idle_timeout_ms.into_value() == 60_000);
        assert!(args.suspension_idle_timeout_ms.into_value() == 1_000);
        assert!(args.server_lifetime_ms.into_value() == 300_000);
    }

    #[test]
    fn render_pgdog_options_accept_exact_custom_surface() {
        let parsed = TestCli::try_parse_from([
            "test",
            "render-pgdog",
            "--bootstrap=cli:9092",
            "--out-dir=/tmp/cli-pgdog",
            "--activator=activator:7444",
            "--listen-port=6543",
            "--tls-certificate=/tls/cert.pem",
            "--tls-private-key=/tls/key.pem",
            "--tls-client-ca-certificate=/tls/ca.pem",
            "--pooler-mode=session",
            "--connect-attempts=4",
            "--cold-start-ceiling-ms=10001",
            "--idle-timeout-ms=60001",
            "--suspension-idle-timeout-ms=1001",
            "--server-lifetime-ms=300001",
        ])
        .expect("custom render-pgdog options");

        let GresCommand::RenderPgdog(args) = parsed.gres.command else {
            panic!("expected render-pgdog command");
        };
        assert!(args.bootstrap == "cli:9092");
        assert!(args.out_dir == PathBuf::from("/tmp/cli-pgdog"));
        assert!(args.activator == Some(("activator".to_string(), 7_444)));
        assert!(args.listen_port.get() == 6_543);
        assert!(args.tls_certificate == Some(PathBuf::from("/tls/cert.pem")));
        assert!(args.tls_private_key == Some(PathBuf::from("/tls/key.pem")));
        assert!(args.tls_client_ca_certificate == Some(PathBuf::from("/tls/ca.pem")));
        assert!(args.pooler_mode == PgdogPoolerMode::Session);
        assert!(args.connect_attempts.into_value() == 4);
        assert!(args.cold_start_ceiling_ms.into_value() == 10_001);
        assert!(args.idle_timeout_ms.into_value() == 60_001);
        assert!(args.suspension_idle_timeout_ms.into_value() == 1_001);
        assert!(args.server_lifetime_ms.into_value() == 300_001);
    }

    #[test]
    fn render_pgdog_options_reject_invalid_values_and_tls_relationships() {
        for options in [
            &["--listen-port=0"][..],
            &["--listen-port=65536"],
            &["--connect-attempts=0"],
            &["--connect-attempts=65536"],
            &["--cold-start-ceiling-ms=0"],
            &["--idle-timeout-ms=0"],
            &["--suspension-idle-timeout-ms=0"],
            &["--server-lifetime-ms=0"],
            &["--cold-start-ceiling-ms=18446744073709551616"],
            &["--pooler-mode=statement"],
            &["--tls-certificate=/tls/cert.pem"],
            &["--tls-private-key=/tls/key.pem"],
            &["--tls-client-ca-certificate=/tls/ca.pem"],
        ] {
            let args = [
                &[
                    "test",
                    "render-pgdog",
                    "--bootstrap=broker:9092",
                    "--out-dir=/tmp/pgdog",
                ][..],
                options,
            ]
            .concat();
            assert!(TestCli::try_parse_from(args).is_err());
        }
    }

    #[test]
    fn render_pgdog_options_read_environment_and_prefer_cli() {
        const CHILD: &str = "CRABKA_TEST_CLI_PGDOG_ENV_CHILD";
        let vars = [
            ("CRABKA_GRES_PGDOG_BOOTSTRAP", "env:9092"),
            ("CRABKA_GRES_PGDOG_OUT_DIR", "/tmp/env-pgdog"),
            ("CRABKA_GRES_PGDOG_ACTIVATOR", "env-activator:7443"),
            ("CRABKA_GRES_PGDOG_LISTEN_PORT", "6542"),
            ("CRABKA_GRES_PGDOG_TLS_CERTIFICATE", "/env/cert.pem"),
            ("CRABKA_GRES_PGDOG_TLS_PRIVATE_KEY", "/env/key.pem"),
            ("CRABKA_GRES_PGDOG_TLS_CLIENT_CA_CERTIFICATE", "/env/ca.pem"),
            ("CRABKA_GRES_PGDOG_POOLER_MODE", "session"),
            ("CRABKA_GRES_PGDOG_CONNECT_ATTEMPTS", "5"),
            ("CRABKA_GRES_PGDOG_COLD_START_CEILING_MS", "30005"),
            ("CRABKA_GRES_PGDOG_IDLE_TIMEOUT_MS", "60005"),
            ("CRABKA_GRES_PGDOG_SUSPENSION_IDLE_TIMEOUT_MS", "1005"),
            ("CRABKA_GRES_PGDOG_SERVER_LIFETIME_MS", "300005"),
        ];
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().expect("test exe"))
                .args([
                    "--exact",
                    "gres::tests::render_pgdog_options_read_environment_and_prefer_cli",
                ])
                .env(CHILD, "1")
                .envs(vars)
                .status()
                .expect("child test");
            assert!(status.success());
            return;
        }

        let environment = TestCli::try_parse_from(["test", "render-pgdog"]).expect("environment");
        let GresCommand::RenderPgdog(environment) = environment.gres.command else {
            panic!("expected render-pgdog command");
        };
        assert!(environment.bootstrap == "env:9092");
        assert!(environment.out_dir == PathBuf::from("/tmp/env-pgdog"));
        assert!(environment.activator == Some(("env-activator".to_string(), 7_443)));
        assert!(environment.listen_port.get() == 6_542);
        assert!(environment.tls_certificate == Some(PathBuf::from("/env/cert.pem")));
        assert!(environment.tls_private_key == Some(PathBuf::from("/env/key.pem")));
        assert!(environment.tls_client_ca_certificate == Some(PathBuf::from("/env/ca.pem")));
        assert!(environment.pooler_mode == PgdogPoolerMode::Session);
        assert!(environment.connect_attempts.into_value() == 5);
        assert!(environment.cold_start_ceiling_ms.into_value() == 30_005);
        assert!(environment.idle_timeout_ms.into_value() == 60_005);
        assert!(environment.suspension_idle_timeout_ms.into_value() == 1_005);
        assert!(environment.server_lifetime_ms.into_value() == 300_005);

        let cli = TestCli::try_parse_from([
            "test",
            "render-pgdog",
            "--bootstrap=cli:9092",
            "--out-dir=/tmp/cli-pgdog",
            "--listen-port=6543",
            "--connect-attempts=6",
        ])
        .expect("CLI over environment");
        let GresCommand::RenderPgdog(cli) = cli.gres.command else {
            panic!("expected render-pgdog command");
        };
        assert!(cli.bootstrap == "cli:9092");
        assert!(cli.out_dir == PathBuf::from("/tmp/cli-pgdog"));
        assert!(cli.listen_port.get() == 6_543);
        assert!(cli.connect_attempts.into_value() == 6);
    }

    fn fixture_password() -> String {
        std::process::id().to_string()
    }

    const BALANCE_SNAPSHOT_ENABLED: &str = r#"{
        "config": {
            "goals": { "disabledGoals": [] },
            "context": {
                "sizeCeilingBytes": 1000,
                "mergeFloorBytes": 100,
                "splitStrideRows": 100,
                "loadSkewHysteresisPct": 25,
                "maxRangesPerCompute": null,
                "maxOperations": 32,
                "cooldownEpochs": 2,
                "currentEpoch": 10,
                "cooldowns": []
            }
        },
        "tenants": [{
            "tenantName": "tenant-a",
            "computes": [{ "computeId": "c1" }],
            "tables": [{
                "tableId": 10,
                "tableName": "orders",
                "isSharded": true,
                "autoShardDisabled": false,
                "convertStoreBytesThreshold": 10000,
                "convertCommitRateThreshold": 10000
            }],
            "ranges": [{
                "rangeId": 1,
                "tableId": 10,
                "startRowid": 0,
                "endRowid": 1000,
                "computeId": "c1",
                "storeBytes": 2500,
                "checkpointBytes": 0,
                "commitRate": 0,
                "scanBytes": 0,
                "isSharded": true,
                "coLocationGroup": null,
                "coLocationBucket": null,
                "isIndexRange": false
            }]
        }]
    }"#;

    const BALANCE_SNAPSHOT_DISABLED: &str = r#"{
        "config": {
            "goals": { "disabledGoals": ["range_size"] },
            "context": {
                "sizeCeilingBytes": 1000,
                "mergeFloorBytes": 100,
                "splitStrideRows": 100,
                "loadSkewHysteresisPct": 25,
                "maxRangesPerCompute": null,
                "maxOperations": 32,
                "cooldownEpochs": 2,
                "currentEpoch": 10,
                "cooldowns": []
            }
        },
        "tenants": [{
            "tenantName": "tenant-a",
            "computes": [{ "computeId": "c1" }],
            "tables": [{
                "tableId": 10,
                "tableName": "orders",
                "isSharded": true,
                "autoShardDisabled": false,
                "convertStoreBytesThreshold": 10000,
                "convertCommitRateThreshold": 10000
            }],
            "ranges": [{
                "rangeId": 1,
                "tableId": 10,
                "startRowid": 0,
                "endRowid": 1000,
                "computeId": "c1",
                "storeBytes": 2500,
                "checkpointBytes": 0,
                "commitRate": 0,
                "scanBytes": 0,
                "isSharded": true,
                "coLocationGroup": null,
                "coLocationBucket": null,
                "isIndexRange": false
            }]
        }]
    }"#;

    #[test]
    fn build_create_tenant_record_hashes_password_as_pg_scram_verifier() {
        let args = CreateTenantArgs {
            bootstrap: "127.0.0.1:9092".to_string(),
            name: "tenant-a".to_string(),
            sql_user: "alice".to_string(),
            password_file: None,
            password_stdin: true,
            wal_replication: 3,
            bucket_prefix: Some("prefix".to_string()),
            checkpoint_frames: Some(10),
            checkpoint_bytes: Some(20),
            idle_seconds: Some(30),
            ranges: Some("0,100,200".to_string()),
            hash_placements: Vec::new(),
        };

        let password = fixture_password();
        let record = build_create_tenant_record(&args, &password, 7).expect("valid record");

        check!(record.record_version == 7);
        check!(record.name.as_str() == "tenant-a");
        check!(record.sql_user.as_str() == "alice");
        check!(record.wal_replication == 3);
        check!(record.bucket_prefix.as_deref() == Some("prefix"));
        check!(record.ranges.len() == 3);
        check!(record.ranges[0].end_key == Some(RangeBoundary::table_start(100)));
        check!(record.ranges[2].endpoint == "tenant-a-gres-r2.gres.svc:5432");
        assert!(PgScramVerifier::parse(&record.scram_verifier).is_ok());
        assert!(!record.scram_verifier.contains(&password));
    }

    #[test]
    fn create_tenant_range_layout_parses_supported_boundary_shapes() {
        let tenant = TenantName::try_from("tenant-a").expect("tenant name");
        let cases = [
            ("0", vec![None]),
            ("0,7:50", vec![Some(RangeBoundary::new(7, 50)), None]),
            ("0,7:3:50", vec![Some(RangeBoundary::hash(7, 3, 50)), None]),
            (
                "0,7,8:3:50",
                vec![
                    Some(RangeBoundary::table_start(7)),
                    Some(RangeBoundary::hash(8, 3, 50)),
                    None,
                ],
            ),
        ];

        for (ranges, expected_end_keys) in cases {
            let layout = parse_range_layout(&tenant, Some(ranges)).expect("valid range layout");
            let actual_end_keys = layout.iter().map(|entry| entry.end_key).collect::<Vec<_>>();

            assert_eq!(actual_end_keys, expected_end_keys, "ranges={ranges}");
        }
    }

    #[test]
    fn create_tenant_hash_placement_parser_is_table_driven() {
        let cases = [
            (
                "1:id:4",
                HashPlacement {
                    table_id: 1,
                    hash_columns: vec!["id".to_string()],
                    bucket_count: 4,
                    co_location_group: None,
                },
            ),
            (
                "7:tenant_id,order_id:16:orders",
                HashPlacement {
                    table_id: 7,
                    hash_columns: vec!["tenant_id".to_string(), "order_id".to_string()],
                    bucket_count: 16,
                    co_location_group: Some("orders".to_string()),
                },
            ),
        ];

        for (input, expected) in cases {
            assert_eq!(parse_hash_placement(input), Ok(expected), "input={input}");
        }
    }

    #[test]
    fn create_tenant_range_snapshot_uses_its_assigned_initial_version() {
        assert!(next_record_version(None) == Ok(1));
        assert!(next_record_version(Some(7)) == Ok(8));
        assert!(next_record_version(Some(u64::MAX)).is_err());
    }

    #[test]
    fn redacted_tenant_output_never_contains_verifier_material() {
        let record = test_record("tenant-a", TenantState::Active);

        let json = serde_json::to_string(&redact_tenant(&record)).expect("redaction serializes");

        assert!(json.contains("<redacted>"));
        assert!(!json.contains("stored"));
        assert!(!json.contains("server"));
    }

    #[test]
    fn source_range_lookup_accepts_nonzero_rowid_boundaries() {
        let mut record = test_record("tenant-a", TenantState::Active);
        record.ranges = vec![
            RangeLayoutEntry {
                range_id: 0,
                end_key: Some(RangeBoundary::new(10, 50)),
                endpoint: "tenant-a-gres-r0.gres.svc:5432".into(),
                wal_generation: 1,
                lifecycle: RangeLifecycle::default(),
                retirement: None,
            },
            RangeLayoutEntry {
                range_id: 1,
                end_key: None,
                endpoint: "tenant-a-gres-r1.gres.svc:5432".into(),
                wal_generation: 1,
                lifecycle: RangeLifecycle::default(),
                retirement: None,
            },
        ];

        let left = source_range_for_key(&record, RangeBoundary::new(10, 49)).unwrap();
        let right = source_range_for_key(&record, RangeBoundary::new(10, 50)).unwrap();

        assert!(left.range_id == 0);
        assert!(right.range_id == 1);
    }

    #[test]
    fn split_boundary_requires_exact_hash_bucket_contract() {
        let mut record = test_record("tenant-a", TenantState::Active);
        record.hash_placements = vec![crabka_gres_control::HashPlacement {
            table_id: 7,
            hash_columns: vec!["id".into()],
            bucket_count: 8,
            co_location_group: None,
        }];

        assert!(split_boundary(&record, 7, Some(3), 9) == Ok(RangeBoundary::hash(7, 3, 9)));
        assert!(split_boundary(&record, 7, None, 9).is_err());
        assert!(split_boundary(&record, 7, Some(8), 9).is_err());
        assert!(split_boundary(&record, 9, Some(0), 9).is_err());
        assert!(split_boundary(&record, 9, None, 9) == Ok(RangeBoundary::new(9, 9)));
    }

    #[test]
    fn move_builder_seals_one_replacement_without_mutating_tenant_layout() {
        let mut record = test_record("tenant-a", TenantState::Active);
        record.ranges = vec![RangeLayoutEntry {
            range_id: 0,
            end_key: None,
            endpoint: "source:7443".into(),
            wal_generation: 4,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        }];
        let original = record.clone();
        let args = MoveRangeArgs {
            bootstrap: "unused:9092".into(),
            tenant: "tenant-a".into(),
            source_range_id: record.ranges[0].range_id,
            table: 7,
            operation_id: "move-1".into(),
            replacement_range_id: 99,
            replacement_endpoint: Some("replacement:7443".into()),
            replacement_wal_generation: Some(record.ranges[0].wal_generation + 1),
        };

        let operation = build_move_operation(&record.name, &record, &args).unwrap();

        assert!(record == original);
        assert!(operation.plan.as_ref().unwrap().current_layout == original.ranges);
        assert!(operation.plan.as_ref().unwrap().target_layout.len() == original.ranges.len());
        assert!(operation.move_intent().unwrap().replacement.range_id == 99);
        assert!(operation.split_intent().is_none());
    }

    #[test]
    fn render_pgdog_writes_pgdog_and_users_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let records = vec![test_record("tenant-a", TenantState::Active)];
        let out_dir = format!("--out-dir={}", dir.path().display());
        let args =
            render_pgdog_test_args(["test", "render-pgdog", "--bootstrap=broker:9092", &out_dir]);

        render_pgdog_files(&records, &args).expect("render succeeds");

        let pgdog = std::fs::read_to_string(dir.path().join("pgdog.toml")).expect("pgdog file");
        let users = std::fs::read_to_string(dir.path().join("users.toml")).expect("users file");
        assert!(
            pgdog
                == concat!(
                    "[general]\n",
                    "port = 6432\n",
                    "min_pool_size = 0\n",
                    "pooler_mode = \"transaction\"\n",
                    "passthrough_auth = \"enabled\"\n",
                    "connect_timeout = 10000\n",
                    "connect_attempts = 3\n",
                    "checkout_timeout = 30000\n",
                    "idle_timeout = 60000\n",
                    "server_lifetime = 300000\n",
                    "idle_healthcheck_delay = 3155760000000\n",
                    "tls_client_required = false\n",
                    "\n",
                    "[[databases]]\n",
                    "name = \"tenant-a\"\n",
                    "host = \"tenant-a.gres.svc\"\n",
                    "port = 5432\n",
                    "pooler_mode = \"transaction\"\n",
                )
        );
        assert!(
            users
                == concat!(
                    "[[users]]\n",
                    "name = \"alice\"\n",
                    "database = \"tenant-a\"\n",
                )
        );
    }

    #[test]
    fn render_pgdog_writes_exact_custom_configuration() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut record = test_record("tenant-a", TenantState::Active);
        record.idle_seconds = Some(30);
        let out_dir = format!("--out-dir={}", dir.path().display());
        let args = render_pgdog_test_args([
            "test",
            "render-pgdog",
            "--bootstrap=broker:9092",
            &out_dir,
            "--activator=activator:7444",
            "--listen-port=6543",
            "--tls-certificate=/tls/cert.pem",
            "--tls-private-key=/tls/key.pem",
            "--tls-client-ca-certificate=/tls/ca.pem",
            "--pooler-mode=session",
            "--connect-attempts=4",
            "--cold-start-ceiling-ms=10001",
            "--idle-timeout-ms=60001",
            "--suspension-idle-timeout-ms=1001",
            "--server-lifetime-ms=300001",
        ]);

        render_pgdog_files(&[record], &args).expect("render succeeds");

        let pgdog = std::fs::read_to_string(dir.path().join("pgdog.toml")).expect("pgdog file");
        assert!(
            pgdog
                == concat!(
                    "[general]\n",
                    "port = 6543\n",
                    "min_pool_size = 0\n",
                    "pooler_mode = \"session\"\n",
                    "passthrough_auth = \"enabled\"\n",
                    "connect_timeout = 2501\n",
                    "connect_attempts = 4\n",
                    "checkout_timeout = 10001\n",
                    "idle_timeout = 1001\n",
                    "server_lifetime = 300001\n",
                    "idle_healthcheck_delay = 3155760000000\n",
                    "tls_certificate = \"/tls/cert.pem\"\n",
                    "tls_private_key = \"/tls/key.pem\"\n",
                    "tls_client_ca_certificate = \"/tls/ca.pem\"\n",
                    "tls_client_required = true\n",
                    "\n",
                    "[[databases]]\n",
                    "name = \"tenant-a\"\n",
                    "host = \"tenant-a.gres.svc\"\n",
                    "port = 5432\n",
                    "pooler_mode = \"session\"\n",
                )
        );
    }

    #[test]
    fn render_pgdog_does_not_use_suspension_timeout_for_zero_idle_seconds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut record = test_record("tenant-a", TenantState::Active);
        record.idle_seconds = Some(0);
        let out_dir = format!("--out-dir={}", dir.path().display());
        let args = render_pgdog_test_args([
            "test",
            "render-pgdog",
            "--bootstrap=broker:9092",
            &out_dir,
            "--idle-timeout-ms=60001",
            "--suspension-idle-timeout-ms=1001",
        ]);

        render_pgdog_files(&[record], &args).expect("render succeeds");

        let pgdog = std::fs::read_to_string(dir.path().join("pgdog.toml")).expect("pgdog file");
        assert!(pgdog.contains("idle_timeout = 60001\n"));
    }

    #[test]
    fn render_pgdog_uses_only_rendered_tenants_for_suspension_timeout() {
        let normal_dir = tempfile::tempdir().expect("normal tempdir");
        let suspension_dir = tempfile::tempdir().expect("suspension tempdir");
        let mut record = test_record("tenant-a", TenantState::Suspended);
        record.idle_seconds = Some(30);
        let out_dir = format!("--out-dir={}", normal_dir.path().display());
        let mut args = render_pgdog_test_args([
            "test",
            "render-pgdog",
            "--bootstrap=broker:9092",
            &out_dir,
            "--idle-timeout-ms=60001",
            "--suspension-idle-timeout-ms=1001",
        ]);

        render_pgdog_files(std::slice::from_ref(&record), &args).expect("normal render succeeds");
        let pgdog =
            std::fs::read_to_string(normal_dir.path().join("pgdog.toml")).expect("normal pgdog");
        assert!(pgdog.contains("idle_timeout = 60001\n"));

        args.activator = Some(("activator".to_string(), 7_444));
        args.out_dir = suspension_dir.path().to_path_buf();
        render_pgdog_files(&[record], &args).expect("suspension render succeeds");
        let pgdog = std::fs::read_to_string(suspension_dir.path().join("pgdog.toml"))
            .expect("suspension pgdog");
        assert!(pgdog.contains("idle_timeout = 1001\n"));
    }

    fn render_pgdog_test_args<const N: usize>(args: [&str; N]) -> RenderPgdogArgs {
        let parsed = TestCli::try_parse_from(args).expect("render-pgdog args");
        let GresCommand::RenderPgdog(args) = parsed.gres.command else {
            panic!("expected render-pgdog command");
        };
        args
    }

    #[test]
    fn trim_password_removes_one_terminal_line_ending_only() {
        let mut password = "secret\n\n".to_string();

        trim_single_trailing_newline(&mut password);

        assert!(password == "secret\n");
    }

    #[test]
    fn gres_balance_dry_run_outputs_planned_operations_without_writes() {
        let input: BalanceDryRunInput = serde_json::from_str(BALANCE_SNAPSHOT_ENABLED).unwrap();

        let output = plan_balance_dry_run(&input);

        assert!(output.dry_run);
        assert!(
            output.operations
                == vec![BalanceOperation::Split {
                    tenant_name: "tenant-a".to_string(),
                    table_id: 10,
                    source_range_id: 1,
                    split_at_rowid: 500,
                }]
        );
    }

    #[test]
    fn gres_balance_dry_run_disabled_goal_suppresses_operations() {
        let input: BalanceDryRunInput = serde_json::from_str(BALANCE_SNAPSHOT_DISABLED).unwrap();

        let output = plan_balance_dry_run(&input);

        assert!(output.operations.is_empty());
        assert!(!output.goals_applied.contains(&"range_size".to_string()));
    }

    #[test]
    fn gres_balance_apply_dry_run_mode_keeps_planner_only_status() {
        let input: BalanceDryRunInput = serde_json::from_str(BALANCE_SNAPSHOT_ENABLED).unwrap();

        let output = plan_balance_apply(
            &input,
            BalanceExecuteMode::DryRun,
            ExecutionPolicy::StopOnFailure,
        );

        assert!(output.dry_run);
        assert!(output.execute_mode == "dry-run");
        assert!(output.execution_policy == "stop-on-failure");
        assert!(output.report.operations == plan_balance_dry_run(&input).operations);
        assert!(
            output
                .report
                .operation_results
                .iter()
                .all(|result| result.status == crabka_gres_balancer::OperationStatus::Planned)
        );
    }

    #[test]
    fn gres_balance_apply_validate_mode_reports_unsupported_hooks_as_json_status() {
        let input: BalanceDryRunInput = serde_json::from_str(BALANCE_SNAPSHOT_ENABLED).unwrap();

        let output = plan_balance_apply(
            &input,
            BalanceExecuteMode::Validate,
            ExecutionPolicy::BestEffort,
        );
        let json = serde_json::to_value(&output).expect("serialize output");

        assert!(output.dry_run);
        assert!(output.report.has_terminal_error());
        assert!(json["executeMode"] == "validate");
        assert!(json["executionPolicy"] == "best-effort");
        assert!(json["report"]["operationResults"][0]["status"] == "unsupported");
        assert!(
            json["report"]["operationResults"][0]["message"]
                .as_str()
                .is_some_and(|message| message.contains("split"))
        );
    }

    fn test_record(name: &str, state: TenantState) -> TenantRecord {
        TenantRecord::new(
            1,
            TenantId::try_from(name).expect("id"),
            TenantName::try_from(name).expect("name"),
            state,
            SqlUser::try_from("alice").expect("sql user"),
            "SCRAM-SHA-256$4096:salt$stored:server".to_string(),
            3,
        )
        .expect("record")
    }
}
