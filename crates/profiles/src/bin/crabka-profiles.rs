#![allow(
    clippy::default_trait_access,
    clippy::needless_raw_string_hashes,
    clippy::too_many_lines
)]

#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use crabka_blockstore::ProfileIndex;
use crabka_client_producer::Producer;
use crabka_pprof::UnionProfileStore;
use crabka_profiles::blockbuilder::BlockBuilderConfig;
use crabka_profiles::cold_store::ColdProfileStore;
use crabka_profiles::compactor::{DownsamplePolicy, compact_once_with_policy};
use crabka_profiles::distributor::{DistributorState, KafkaSink, serve};
use crabka_profiles::hot_store::{WalTailProfileStore, run_wal_tail};
use crabka_profiles::ingest::{RelabelConfig, TenantLimitConfig};
use crabka_profiles::limits::OverridesProvider;
use crabka_profiles::metrics::ServiceMetrics;
use crabka_profiles::query::{QuerierState, serve as serve_querier};
use crabka_profiles::query_frontend::FrontendConfig;
use crabka_telemetry::OtlpConfig;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    target: Target,
    #[arg(long, default_value = "127.0.0.1:4040")]
    listen: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:9092")]
    bootstrap: String,
    #[arg(long, default_value = "file://./.crabka-profiles-blocks")]
    object_store_url: String,
    #[arg(long, default_value_t = 15 * 60 * 1000)]
    query_frontend_shard_ms: i64,
    #[arg(long)]
    tenant_limits_config: Option<std::path::PathBuf>,
    #[arg(long)]
    profiles_limits_overrides_config: Option<std::path::PathBuf>,
    #[arg(long, default_value = "crabka-profiles-query-wal-tail")]
    query_wal_tail_group_id: String,
    #[arg(long, default_value_t = 8)]
    compactor_max_blocks_per_job: usize,
    #[arg(long)]
    compactor_downsample_resolution_ns: Option<i64>,
    /// debuginfod base URLs (comma-separated) to fetch DWARF for unsymbolized
    /// native frames. Empty by default: the symbolizer makes NO outbound
    /// requests unless an operator explicitly opts in by supplying URLs.
    #[arg(long = "debuginfod-url", value_delimiter = ',')]
    debuginfod_urls: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum Target {
    Distributor,
    BlockBuilder,
    Querier,
    QueryFrontend,
    Compactor,
    Symbolizer,
}

struct ConfiguredObjectStore {
    store: std::sync::Arc<dyn ObjectStore>,
    prefix: ObjectPath,
}

impl ConfiguredObjectStore {
    fn object_key(&self, key: &str) -> String {
        let prefix = self.prefix.as_ref().trim_matches('/');
        let key = key.trim_start_matches('/');
        if prefix.is_empty() {
            key.to_string()
        } else {
            format!("{prefix}/{key}")
        }
    }
}

fn build_object_store(
    url: &str,
) -> Result<ConfiguredObjectStore, Box<dyn std::error::Error + Send + Sync>> {
    let parsed = url::Url::parse(url)?;
    let (store, prefix) = object_store::parse_url_opts(&parsed, std::env::vars())?;
    Ok(ConfiguredObjectStore {
        store: std::sync::Arc::from(store),
        prefix,
    })
}

/// Periodically reload the profile block index from object storage and swap it
/// into the cold store. The block-builder writes new blocks continuously; without
/// this the querier only ever sees the index snapshot it loaded at boot, so blocks
/// created afterwards are invisible (manifests as recent profiles — especially
/// sparse ones like memory that age out of the hot tier — returning empty). Mirrors
/// the traces querier's `TraceIndex` refresh loop.
fn spawn_profile_index_refresh(
    cold: Arc<ColdProfileStore>,
    store: Arc<dyn ObjectStore>,
    index_key: String,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(15));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            if let Ok(index) = ProfileIndex::load(&store, &index_key).await {
                cold.replace_index(Arc::new(index));
            }
        }
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _telemetry = crabka_telemetry::init(
        OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "crabka-profiles",
            env!("CARGO_PKG_VERSION"),
            "crabka-profiles",
        ),
        "crabka_profiles=info,info",
        "info",
        "crabka-profiles",
    )?;
    let metrics = ServiceMetrics::new();
    crabka_telemetry::profiling::serve_admin_from_env_with(
        "0.0.0.0:9404",
        crabka_profiles::metrics::metrics_router(metrics.registry.clone()),
    )
    .await?;

    let cli = Cli::parse();
    match cli.target {
        Target::Distributor => {
            let limits = load_tenant_limits_config(cli.tenant_limits_config.as_deref())?;
            let profile_overrides = load_profiles_limits_overrides_config(
                cli.profiles_limits_overrides_config.as_deref(),
            )?;
            let producer = Producer::builder()
                .bootstrap(&cli.bootstrap)
                .build()
                .await?;
            let state = Arc::new(DistributorState {
                sink: Arc::new(KafkaSink::new(Arc::new(producer))),
                limits,
                profile_overrides,
                active_series: Default::default(),
                ingestion_buckets: Default::default(),
                relabel: Vec::<RelabelConfig>::new(),
                max_decompressed: 1 << 24,
                metrics: metrics.clone(),
            });
            let shutdown = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            let bound = serve(cli.listen, state, shutdown).await?;
            tracing::info!(%bound, "profiles distributor listening");
            let _ = tokio::signal::ctrl_c().await;
        }
        Target::BlockBuilder => {
            let configured = build_object_store(&cli.object_store_url)
                .map_err(|e| format!("object store: {e}"))?;
            crabka_profiles::blockbuilder::run_with_config(BlockBuilderConfig::new(
                cli.bootstrap,
                configured.store,
            ))
            .await?;
        }
        Target::Querier => {
            let overrides = load_profiles_limits_overrides_config(
                cli.profiles_limits_overrides_config.as_deref(),
            )?;
            let configured = build_object_store(&cli.object_store_url)
                .map_err(|e| format!("object store: {e}"))?;
            let index_key = configured.object_key("index/profiles.json");
            let index = ProfileIndex::load(&configured.store, &index_key)
                .await
                .unwrap_or_else(|_| ProfileIndex::new());
            let refresh_store = Arc::clone(&configured.store);
            let cold = Arc::new(ColdProfileStore::new_with_debuginfod_urls(
                configured.store,
                Arc::new(index),
                cli.debuginfod_urls.clone(),
            )?);
            spawn_profile_index_refresh(Arc::clone(&cold), refresh_store, index_key.clone());
            let hot = WalTailProfileStore::new();
            spawn_wal_tail(&cli, hot.clone());
            let union = Arc::new(UnionProfileStore::new(Arc::new(hot), cold));
            let state = Arc::new(
                QuerierState::new_with_overrides(union, overrides).with_metrics(metrics.clone()),
            );
            let shutdown = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            let bound = serve_querier(cli.listen, state, shutdown).await?;
            tracing::info!(%bound, "profiles querier listening");
            let _ = tokio::signal::ctrl_c().await;
        }
        Target::QueryFrontend => {
            let overrides = load_profiles_limits_overrides_config(
                cli.profiles_limits_overrides_config.as_deref(),
            )?;
            let configured = build_object_store(&cli.object_store_url)
                .map_err(|e| format!("object store: {e}"))?;
            let index_key = configured.object_key("index/profiles.json");
            let index = ProfileIndex::load(&configured.store, &index_key)
                .await
                .unwrap_or_else(|_| ProfileIndex::new());
            let refresh_store = Arc::clone(&configured.store);
            let cold = Arc::new(ColdProfileStore::new_with_debuginfod_urls(
                configured.store,
                Arc::new(index),
                cli.debuginfod_urls.clone(),
            )?);
            spawn_profile_index_refresh(Arc::clone(&cold), refresh_store, index_key.clone());
            let hot = WalTailProfileStore::new();
            spawn_wal_tail(&cli, hot.clone());
            let union = Arc::new(UnionProfileStore::new(Arc::new(hot), cold));
            let state = Arc::new(
                QuerierState::new_frontend_with_overrides(
                    union,
                    FrontendConfig {
                        shard_width_ms: cli.query_frontend_shard_ms,
                    },
                    overrides,
                )
                .with_metrics(metrics.clone()),
            );
            let shutdown = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            let bound = serve_querier(cli.listen, state, shutdown).await?;
            tracing::info!(
                %bound,
                shard_width_ms = cli.query_frontend_shard_ms,
                "profiles query-frontend listening"
            );
            let _ = tokio::signal::ctrl_c().await;
        }
        Target::Symbolizer => {
            crabka_profiles::symbolizer::run(cli.debuginfod_urls).await?;
        }
        Target::Compactor => {
            let configured = build_object_store(&cli.object_store_url)
                .map_err(|e| format!("object store: {e}"))?;
            let index_key = configured.object_key("index/profiles.json");
            let mut index = ProfileIndex::load(&configured.store, &index_key)
                .await
                .unwrap_or_else(|_| ProfileIndex::new());
            let downsample = cli
                .compactor_downsample_resolution_ns
                .map(|resolution_ns| DownsamplePolicy { resolution_ns });
            let metas = compact_once_with_policy(
                &configured.store,
                &mut index,
                cli.compactor_max_blocks_per_job,
                downsample,
            )
            .await?;
            index.save(&configured.store, &index_key).await?;
            tracing::info!(
                compacted_blocks = metas.len(),
                downsample_resolution_ns = ?cli.compactor_downsample_resolution_ns,
                "profiles compactor finished one pass"
            );
        }
    }

    Ok(())
}

fn load_tenant_limits_config(
    path: Option<&Path>,
) -> Result<TenantLimitConfig, Box<dyn std::error::Error>> {
    let Some(path) = path else {
        return Ok(TenantLimitConfig::default());
    };
    let bytes = std::fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn load_profiles_limits_overrides_config(
    path: Option<&Path>,
) -> Result<OverridesProvider, Box<dyn std::error::Error>> {
    let Some(path) = path else {
        return Ok(OverridesProvider::new(Default::default()));
    };
    let text = std::fs::read_to_string(path)?;
    Ok(OverridesProvider::from_yaml(&text)?)
}

fn spawn_wal_tail(cli: &Cli, hot: WalTailProfileStore) {
    let bootstrap = cli.bootstrap.clone();
    let group_id = cli.query_wal_tail_group_id.clone();
    tokio::spawn(async move {
        if let Err(err) = run_wal_tail(hot, bootstrap, group_id, Duration::from_millis(500)).await {
            tracing::warn!(%err, "profiles hot WAL-tail stopped");
        }
    });
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_distributor_target() {
        let cli = Cli::try_parse_from(["crabka-profiles", "--target", "distributor"]).unwrap();

        assert!(matches!(cli.target, Target::Distributor));
    }

    #[test]
    fn parses_block_builder_target() {
        let cli = Cli::try_parse_from(["crabka-profiles", "--target", "block-builder"]).unwrap();

        assert!(matches!(cli.target, Target::BlockBuilder));
    }

    #[test]
    fn parses_querier_target() {
        let cli = Cli::try_parse_from(["crabka-profiles", "--target", "querier"]).unwrap();

        assert!(matches!(cli.target, Target::Querier));
    }

    #[test]
    fn parses_query_frontend_target_and_shard_width() {
        let cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "query-frontend",
            "--query-frontend-shard-ms",
            "30000",
        ])
        .unwrap();

        assert!(matches!(cli.target, Target::QueryFrontend));
        assert!(cli.query_frontend_shard_ms == 30_000);
    }

    #[test]
    fn parses_query_wal_tail_group_id() {
        let cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "querier",
            "--query-wal-tail-group-id",
            "profiles-tail-a",
        ])
        .unwrap();

        assert!(cli.query_wal_tail_group_id == "profiles-tail-a");
    }

    #[test]
    fn parses_profiles_limits_overrides_config() {
        let cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "query-frontend",
            "--profiles-limits-overrides-config",
            "overrides.yaml",
        ])
        .unwrap();

        assert!(
            cli.profiles_limits_overrides_config.as_deref() == Some(Path::new("overrides.yaml"))
        );
    }

    #[test]
    fn parses_distributor_profiles_limits_overrides_config() {
        let cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "distributor",
            "--profiles-limits-overrides-config",
            "overrides.yaml",
        ])
        .unwrap();

        assert!(
            cli.profiles_limits_overrides_config.as_deref() == Some(Path::new("overrides.yaml"))
        );
    }

    #[test]
    fn parses_compactor_max_blocks_per_job() {
        let cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "compactor",
            "--compactor-max-blocks-per-job",
            "3",
        ])
        .unwrap();

        assert!(matches!(cli.target, Target::Compactor));
        assert!(cli.compactor_max_blocks_per_job == 3);
    }

    #[test]
    fn parses_compactor_downsample_resolution() {
        let cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "compactor",
            "--compactor-downsample-resolution-ns",
            "60000000000",
        ])
        .unwrap();

        assert!(cli.compactor_downsample_resolution_ns == Some(60_000_000_000));
    }

    #[test]
    fn debuginfod_urls_default_is_empty() {
        // Security default: no outbound debuginfod egress unless the operator
        // explicitly opts in. The list must be empty when the flag is absent.
        let cli = Cli::try_parse_from(["crabka-profiles", "--target", "querier"]).unwrap();

        assert!(cli.debuginfod_urls.is_empty());
    }

    #[test]
    fn parses_debuginfod_urls() {
        let cli = Cli::try_parse_from([
            "crabka-profiles",
            "--target",
            "querier",
            "--debuginfod-url",
            "http://one.example,http://two.example",
        ])
        .unwrap();

        assert!(
            cli.debuginfod_urls
                == vec![
                    "http://one.example".to_string(),
                    "http://two.example".to_string()
                ]
        );
    }

    #[test]
    fn loads_tenant_limits_config_from_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("limits.json");
        std::fs::write(
            &path,
            r#"{
              "default": {
                "max_label_names_per_series": 10,
                "max_label_value_len": 100,
                "session_id_buckets": 32
              },
              "tenants": {
                "tenant-a": {
                  "max_label_names_per_series": 2,
                  "max_label_value_len": 3,
                  "session_id_buckets": 4
                }
              }
            }"#,
        )
        .unwrap();

        let config = load_tenant_limits_config(Some(&path)).unwrap();

        assert!(config.default.max_label_names_per_series == 10);
        assert!(config.for_tenant("tenant-a").max_label_value_len == 3);
    }

    #[test]
    fn loads_profiles_limits_overrides_config_from_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("overrides.yaml");
        std::fs::write(
            &path,
            r#"
overrides:
  tenant-a:
    max_query_length_secs: 30
    max_flamegraph_nodes_max: 512
"#,
        )
        .unwrap();

        let overrides = load_profiles_limits_overrides_config(Some(&path)).unwrap();

        assert!(overrides.for_tenant("tenant-a").max_query_length_secs == 30);
        assert!(overrides.for_tenant("tenant-a").max_flamegraph_nodes_max == 512);
        // An unlisted tenant inherits the process default query-length cap.
        assert!(
            overrides.for_tenant("tenant-b").max_query_length_secs
                == crabka_profiles::limits::DEFAULT_MAX_QUERY_LENGTH_SECS
        );
    }

    #[test]
    fn rejects_unknown_target() {
        assert!(Cli::try_parse_from(["crabka-profiles", "--target", "bogus"]).is_err());
    }
}
