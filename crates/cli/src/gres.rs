//! `crabka gres` subcommands.

use std::{
    io::Read as _,
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
    PgdogGeneral, PgdogRenderInput, PgdogUser, RangeBoundary, RangeLayoutEntry, RangeLayoutSplit,
    RangeLifecycle, Registry, SplitOperationPlan, SplitOperationRecord, SqlUser, TenantEndpoint,
    TenantId, TenantName, TenantRecord, TenantState, render_pgdog_toml, render_users_toml,
    tenant_config_topic,
};
use crabka_security::{ListenerProtocol, SaslMechanism, scram::PgScramVerifier};
use serde::Serialize;

const EXIT_OK: i32 = 0;
const EXIT_ERROR: i32 = 1;
const DEFAULT_WAL_REPLICATION: i32 = 1;
const DEFAULT_SCRAM_ITERATIONS: u32 = 4096;
const DEFAULT_BACKEND_PORT: u16 = 5432;

#[derive(Args, Debug)]
pub struct GresArgs {
    #[command(subcommand)]
    command: GresCommand,
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
    /// Unsafely edit one tenant range layout for offline development only.
    Split(SplitRangeArgs),
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
    /// Comma-separated table-start range boundaries, for example 0,100,200.
    #[arg(long)]
    ranges: Option<String>,
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
    #[arg(long)]
    bootstrap: String,
    /// Directory that will receive pgdog.toml and users.toml.
    #[arg(long)]
    out_dir: PathBuf,
    /// Suspended-tenant activator route as host:port.
    #[arg(long, value_parser = parse_activator)]
    activator: Option<(String, u16)>,
    /// Client-facing PgDog listen port.
    #[arg(long, default_value_t = 6432)]
    listen_port: u16,
    /// Client-facing TLS certificate path as visible inside the PgDog runtime.
    #[arg(long, requires = "tls_private_key")]
    tls_certificate: Option<PathBuf>,
    /// Client-facing TLS private-key path as visible inside the PgDog runtime.
    #[arg(long, requires = "tls_certificate")]
    tls_private_key: Option<PathBuf>,
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
    match args.command {
        GresCommand::CreateTenant(args) => create_tenant(args).await,
        GresCommand::Describe(args) => describe_tenant(args).await,
        GresCommand::List(args) => list_tenants(args).await,
        GresCommand::Suspend(args) => change_tenant_state(args, TenantState::Suspended).await,
        GresCommand::Resume(args) => change_tenant_state(args, TenantState::Active).await,
        GresCommand::Split(args) => split_range(args).await,
        GresCommand::Delete(args) => delete_tenant(args).await,
        GresCommand::RenderPgdog(args) => render_pgdog(args).await,
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

async fn split_range(args: SplitRangeArgs) -> Result<(), String> {
    let tenant_name = TenantName::try_from(args.tenant.as_str()).map_err(|e| e.to_string())?;
    let mut registry = connect_registry(&args.bootstrap).await?;
    let record = registry
        .get(tenant_name.as_str())
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("tenant {tenant_name} not found"))?;
    let source = source_range_for_key(&record, RangeBoundary::new(args.table, args.rowid))?;
    let first_free = next_range_id(&record);
    let left_range_id = args
        .left_range_id
        .unwrap_or_else(|| if source.range_id == 0 { 0 } else { first_free });
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
        left: successor(
            left_range_id,
            Some(RangeBoundary::new(args.table, args.rowid)),
            left_endpoint,
        ),
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
        "initiated split {} for tenant {tenant_name} range {} at table {} rowid {} into ranges {} and {}",
        args.operation_id,
        source.range_id,
        args.table,
        args.rowid,
        left_range_id,
        successor_range_id
    );
    Ok(())
}

async fn create_tenant(args: CreateTenantArgs) -> Result<(), String> {
    let password = read_password(&args)?;
    let mut registry = connect_registry(&args.bootstrap).await?;
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

async fn describe_tenant(args: TenantNameArgs) -> Result<(), String> {
    let mut registry = connect_registry(&args.bootstrap).await?;
    let tenant = registry
        .get(&args.name)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("tenant {} not found", args.name))?;
    print_json(&redact_tenant(&tenant))
}

async fn list_tenants(args: BootstrapArgs) -> Result<(), String> {
    let mut registry = connect_registry(&args.bootstrap).await?;
    let tenants = registry.list().await.map_err(|e| e.to_string())?;
    let redacted = tenants.iter().map(redact_tenant).collect::<Vec<_>>();
    print_json(&redacted)
}

async fn change_tenant_state(args: TenantNameArgs, state: TenantState) -> Result<(), String> {
    let mut registry = connect_registry(&args.bootstrap).await?;
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

async fn delete_tenant(args: TenantNameArgs) -> Result<(), String> {
    let mut registry = connect_registry(&args.bootstrap).await?;
    registry
        .delete(&args.name)
        .await
        .map_err(|e| e.to_string())?;
    println!("deleted tenant {}", args.name);
    Ok(())
}

async fn render_pgdog(args: RenderPgdogArgs) -> Result<(), String> {
    let mut registry = connect_registry(&args.bootstrap).await?;
    let tenants = registry.list().await.map_err(|e| e.to_string())?;
    render_pgdog_files(
        &tenants,
        args.activator,
        &args.out_dir,
        args.listen_port,
        args.tls_certificate.as_deref(),
        args.tls_private_key.as_deref(),
    )
}

async fn connect_registry(bootstrap: &str) -> Result<Registry, String> {
    let mut registry = Registry::connect(bootstrap)
        .await
        .map_err(|e| e.to_string())?;
    registry
        .ensure_topic(DEFAULT_WAL_REPLICATION)
        .await
        .map_err(|e| e.to_string())?;
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
        .map(|token| {
            let (table, rowid) = token.split_once(':').map_or((token, "0"), |parts| parts);
            let table = table
                .parse::<u64>()
                .map_err(|error| format!("invalid --ranges boundary {token:?}: {error}"))?;
            let rowid = rowid
                .parse::<u64>()
                .map_err(|error| format!("invalid --ranges boundary {token:?}: {error}"))?;
            Ok::<RangeBoundary, String>(RangeBoundary::new(table, rowid))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if boundaries.is_empty() {
        return Err("--ranges must contain at least one boundary".to_string());
    }
    if boundaries[0] != RangeBoundary::table_start(0)
        || boundaries
            .windows(2)
            .any(|pair| (pair[0].table_id, pair[0].rowid) >= (pair[1].table_id, pair[1].rowid))
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
                lifecycle: Default::default(),
                retirement: None,
            })
        })
        .collect()
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

fn render_pgdog_files(
    records: &[TenantRecord],
    activator: Option<(String, u16)>,
    out_dir: &Path,
    listen_port: u16,
    tls_certificate: Option<&Path>,
    tls_private_key: Option<&Path>,
) -> Result<(), String> {
    std::fs::create_dir_all(out_dir).map_err(|e| format!("create output directory: {e}"))?;
    let endpoints = records.iter().map(tenant_endpoint).collect::<Vec<_>>();
    let users = records
        .iter()
        .map(|record| PgdogUser {
            name: record.sql_user.as_str().to_string(),
            database: record.name.as_str().to_string(),
            password: None,
        })
        .collect();
    let input = PgdogRenderInput {
        tenants: &endpoints,
        activator,
        general: PgdogGeneral {
            listen_port,
            tls_cert_path: tls_certificate.map(|path| path.to_string_lossy().into_owned()),
            tls_key_path: tls_private_key.map(|path| path.to_string_lossy().into_owned()),
            users,
            ..PgdogGeneral::default()
        },
    };
    let pgdog = render_pgdog_toml(&input).map_err(|e| e.to_string())?;
    let users = render_users_toml(&input).map_err(|e| e.to_string())?;
    std::fs::write(out_dir.join("pgdog.toml"), pgdog)
        .map_err(|e| format!("write pgdog.toml: {e}"))?;
    std::fs::write(out_dir.join("users.toml"), users)
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

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

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
        };

        let record = build_create_tenant_record(&args, "hunter2", 7).expect("valid record");

        check!(record.record_version == 7);
        check!(record.name.as_str() == "tenant-a");
        check!(record.sql_user.as_str() == "alice");
        check!(record.wal_replication == 3);
        check!(record.bucket_prefix.as_deref() == Some("prefix"));
        check!(record.ranges.len() == 3);
        check!(record.ranges[0].end_key == Some(RangeBoundary::table_start(100)));
        check!(record.ranges[2].endpoint == "tenant-a-gres-r2.gres.svc:5432");
        assert!(PgScramVerifier::parse(&record.scram_verifier).is_ok());
        assert!(!record.scram_verifier.contains("hunter2"));
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
                lifecycle: Default::default(),
                retirement: None,
            },
            RangeLayoutEntry {
                range_id: 1,
                end_key: None,
                endpoint: "tenant-a-gres-r1.gres.svc:5432".into(),
                wal_generation: 1,
                lifecycle: Default::default(),
                retirement: None,
            },
        ];

        let left = source_range_for_key(&record, RangeBoundary::new(10, 49)).unwrap();
        let right = source_range_for_key(&record, RangeBoundary::new(10, 50)).unwrap();

        assert!(left.range_id == 0);
        assert!(right.range_id == 1);
    }

    #[test]
    fn render_pgdog_writes_pgdog_and_users_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let records = vec![test_record("tenant-a", TenantState::Active)];

        render_pgdog_files(&records, None, dir.path(), 6432, None, None).expect("render succeeds");

        let pgdog = std::fs::read_to_string(dir.path().join("pgdog.toml")).expect("pgdog file");
        let users = std::fs::read_to_string(dir.path().join("users.toml")).expect("users file");
        assert!(
            pgdog
                == concat!(
                    "[general]\n",
                    "port = 6432\n",
                    "pooler_mode = \"transaction\"\n",
                    "passthrough_auth = \"enabled\"\n",
                    "connect_timeout = 10000\n",
                    "connect_attempts = 3\n",
                    "checkout_timeout = 30000\n",
                    "idle_timeout = 60000\n",
                    "server_lifetime = 300000\n",
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
