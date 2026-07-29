use std::path::{Path, PathBuf};

use assert2::check;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate lives under repo_root/crates/<name>")
        .to_path_buf()
}

fn alloy_config() -> String {
    std::fs::read_to_string(repo_root().join("demo/observability/alloy/config.alloy"))
        .expect("read Alloy config")
}

fn docker_compose() -> String {
    std::fs::read_to_string(repo_root().join("demo/observability/docker-compose.yml"))
        .expect("read observability compose file")
}

fn compose_service_block<'a>(compose: &'a str, service: &str) -> &'a str {
    let marker = format!("  {service}:");
    let start = compose.find(&marker).expect("compose service exists");
    let rest = &compose[start..];
    let mut offset = 0_usize;
    for (index, line) in rest.split_inclusive('\n').enumerate() {
        if index > 0
            && line.starts_with("  ")
            && !line.starts_with("    ")
            && !line.trim_start().starts_with('#')
        {
            return &rest[..offset];
        }
        offset += line.len();
    }
    rest
}

fn demo_melange_config() -> String {
    std::fs::read_to_string(repo_root().join("packaging/melange/crabka-demo.yaml"))
        .expect("read demo melange config")
}

fn demo_apko_config() -> String {
    std::fs::read_to_string(repo_root().join("packaging/apko/crabka-demo.yaml"))
        .expect("read demo apko config")
}

fn demo_publish_workflow() -> String {
    std::fs::read_to_string(repo_root().join(".github/workflows/publish-demo-image.yml"))
        .expect("read demo image publish workflow")
}

fn dashboard_provider_config() -> String {
    std::fs::read_to_string(
        repo_root().join("demo/observability/grafana/provisioning/dashboards/dashboards.yaml"),
    )
    .expect("read Grafana dashboard provisioning config")
}

fn grafana_datasource_config() -> String {
    std::fs::read_to_string(
        repo_root().join("demo/observability/grafana/provisioning/datasources/crabka.yaml"),
    )
    .expect("read Grafana datasource provisioning config")
}

fn grafana_alerting_config() -> String {
    std::fs::read_to_string(
        repo_root().join("demo/observability/grafana/provisioning/alerting/crabka-alerts.yaml"),
    )
    .expect("read Grafana alerting provisioning config")
}

fn rustfs_bootstrap_script() -> String {
    std::fs::read_to_string(repo_root().join("demo/observability/rustfs/bootstrap.sh"))
        .expect("read RustFS bootstrap script")
}

fn dashboard(name: &str) -> String {
    std::fs::read_to_string(repo_root().join(format!(
        "demo/observability/grafana/provisioning/dashboards/{name}"
    )))
    .expect("read Grafana dashboard")
}

fn datasource_block<'a>(config: &'a str, uid: &str) -> &'a str {
    let start = config.find(uid).expect("datasource UID exists");
    let rest = &config[start..];
    let end = rest
        .find("\n  - name:")
        .map_or(rest.len(), |offset| offset + 1);
    &rest[..end]
}

fn balanced_block_after_marker<'a>(input: &'a str, marker: &str) -> &'a str {
    let start = input.find(marker).expect("block marker exists");
    let rest = &input[start..];
    let mut depth = 0_usize;
    let mut end = None;
    for (idx, ch) in rest.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(idx + ch.len_utf8());
                    break;
                }
            }
            _ => {}
        }
    }
    &rest[..end.expect("pyroscope scrape block is balanced")]
}

fn scrape_block<'a>(config: &'a str, name: &str) -> &'a str {
    let marker = format!("pyroscope.scrape \"{name}\" {{");
    balanced_block_after_marker(config, &marker)
}

fn profile_memory_block(input: &str) -> &str {
    let start = input.find("profile.memory").expect("profile.memory exists");
    let rest = &input[start..];
    let open = rest.find('{').expect("profile.memory block opens");
    balanced_block_after_marker(rest, &rest[..=open])
}

fn profile_process_cpu_block(input: &str) -> &str {
    let start = input
        .find("profile.process_cpu")
        .expect("profile.process_cpu exists");
    let rest = &input[start..];
    let open = rest.find('{').expect("profile.process_cpu block opens");
    balanced_block_after_marker(rest, &rest[..=open])
}

fn normalize_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn docker_log_tailing_is_scoped_to_the_demo_compose_project() {
    let config = alloy_config();
    let relabel = balanced_block_after_marker(&config, "discovery.relabel \"containers\" {");
    for (needle, _why) in [
        (
            "__meta_docker_container_label_com_docker_compose_project",
            "Docker log discovery should inspect the Compose project label before forwarding targets",
        ),
        (
            "action        = \"keep\"",
            "Docker log discovery should drop non-demo containers instead of tailing every Docker container",
        ),
        (
            "regex         = \"crabka-observability-.*\"",
            "Docker log discovery should only tail Crabka observability Compose projects",
        ),
    ] {
        assert2::assert!(relabel.contains(needle));
    }
}

#[test]
fn crabka_worker_targets_also_collect_memory_profiles() {
    let config = alloy_config();
    let scrape = scrape_block(&config, "crabka_workers");
    let process_cpu = normalize_whitespace(profile_process_cpu_block(scrape));
    assert2::assert!(process_cpu.contains("enabled = true"));
    let memory = profile_memory_block(scrape);
    assert2::assert!(memory.contains("enabled = true"));
    assert2::assert!(memory.contains("path    = \"/debug/pprof/heap\""));
}

#[test]
fn jemalloc_heap_profiling_uses_bounded_always_on_sampling() {
    let compose = docker_compose();
    assert2::assert!(compose.contains("MALLOC_CONF: \"prof:true,prof_active:true,lg_prof_sample:20,background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000\""));
    assert2::assert!(compose.contains("lg_prof_sample:20"));
    for option in [
        "background_thread:true",
        "dirty_decay_ms:1000",
        "muzzy_decay_ms:1000",
    ] {
        assert2::assert!(compose.contains(option));
    }
    assert2::assert!(
        !compose
            .lines()
            .any(|line| line.trim_start().starts_with("MALLOC_CONF:")
                && line.contains("lg_prof_sample:0"))
    );
}

#[test]
fn metrics_compactor_bounds_cold_block_retention_for_demo() {
    let compose = docker_compose();
    let block = compose_service_block(&compose, "metrics-compactor");
    assert2::assert!(
        block
            .contains("--compactor-retention-ms=${CRABKA_METRICS_COMPACTOR_RETENTION_MS:-3600000}")
    );
    assert2::assert!(block.contains(
        "--compactor-retention-sweep-ms=${CRABKA_METRICS_COMPACTOR_RETENTION_SWEEP_MS:-30000}"
    ));
}

#[test]
fn trace_and_profile_snapshot_policy_is_overrideable_per_signal() {
    let compose = docker_compose();
    for (service, signal) in [
        ("traces-block-builder", "TRACES"),
        ("profiles-block-builder", "PROFILES"),
    ] {
        let block = compose_service_block(&compose, service);
        assert2::assert!(block.contains(&format!(
            "CRABKA_{signal}_INDEX_SNAPSHOT_MAX_BYTES: \"${{CRABKA_{signal}_INDEX_SNAPSHOT_MAX_BYTES:-268435456}}\""
        )));
        assert2::assert!(block.contains(&format!(
            "CRABKA_{signal}_INDEX_SNAPSHOT_RETAIN: \"${{CRABKA_{signal}_INDEX_SNAPSHOT_RETAIN:-8}}\""
        )));
    }

    for (service, signal) in [
        ("traces-querier", "TRACES"),
        ("profiles-querier", "PROFILES"),
    ] {
        let block = compose_service_block(&compose, service);
        assert2::assert!(block.contains(&format!(
            "CRABKA_{signal}_INDEX_SNAPSHOT_MAX_BYTES: \"${{CRABKA_{signal}_INDEX_SNAPSHOT_MAX_BYTES:-268435456}}\""
        )));
        assert2::assert!(!block.contains(&format!("CRABKA_{signal}_INDEX_SNAPSHOT_RETAIN:")));
    }
}

#[test]
fn trace_and_profile_wal_fetch_limits_are_overrideable_per_signal() {
    let compose = docker_compose();
    for (service, signal) in [
        ("traces-block-builder", "TRACES"),
        ("profiles-block-builder", "PROFILES"),
    ] {
        let block = compose_service_block(&compose, service);
        assert2::assert!(block.contains(&format!(
            "CRABKA_{signal}_WAL_FETCH_MAX_BYTES: \"${{CRABKA_{signal}_WAL_FETCH_MAX_BYTES:-2097152}}\""
        )));
        assert2::assert!(block.contains(&format!(
            "CRABKA_{signal}_WAL_FETCH_PARTITION_MAX_BYTES: \"${{CRABKA_{signal}_WAL_FETCH_PARTITION_MAX_BYTES:-262144}}\""
        )));
    }
}

#[test]
fn traces_querier_parquet_read_cap_is_overrideable() {
    let compose = docker_compose();
    let block = compose_service_block(&compose, "traces-querier");
    assert2::assert!(block.contains(
        "CRABKA_TRACES_BLOCK_READ_MAX_BYTES: \"${CRABKA_TRACES_BLOCK_READ_MAX_BYTES:-1073741824}\""
    ));
}

#[test]
fn traces_querier_scan_concat_cap_is_overrideable() {
    let compose = docker_compose();
    let block = compose_service_block(&compose, "traces-querier");
    assert2::assert!(block.contains(
        "CRABKA_TRACES_SCAN_CONCAT_MAX_BYTES: \"${CRABKA_TRACES_SCAN_CONCAT_MAX_BYTES:-1500000000}\""
    ));
}

#[test]
fn otlp_heartbeat_traces_use_per_component_service_names() {
    let compose = docker_compose();
    assert2::assert!(compose.contains("CRABKA_OTLP_HEARTBEAT_INTERVAL_SECS: \"15\""));
    for service in [
        "broker",
        "schema-registry",
        "metrics-distributor",
        "metrics-compactor",
        "metrics-querier",
        "traces-distributor",
        "traces-block-builder",
        "traces-querier",
        "logs-distributor",
        "logs-compactor",
        "logs-querier",
        "profiles-distributor",
        "profiles-block-builder",
        "profiles-querier",
        "demo-produce",
        "demo-stream",
        "demo-consume",
    ] {
        let block = compose_service_block(&compose, service);
        assert2::assert!(block.contains(&format!("OTEL_SERVICE_NAME: {service}")));
    }
}

#[test]
fn demo_app_profiles_cover_all_roles_without_heap_scrape() {
    let config = alloy_config();
    let scrape = scrape_block(&config, "demo_apps");
    for role in ["demo-produce", "demo-stream"] {
        assert2::assert!(scrape.contains(&format!("service_name = \"{role}\"")));
    }
    let process_cpu = normalize_whitespace(profile_process_cpu_block(scrape));
    assert2::assert!(process_cpu.contains("enabled = true"));
    let memory = profile_memory_block(scrape);
    assert2::assert!(memory.contains("enabled = false"));

    let consume_scrape = scrape_block(&config, "demo_consume_cpu");
    assert2::assert!(consume_scrape.contains("service_name = \"demo-consume\""));
    let consume_cpu = normalize_whitespace(profile_process_cpu_block(consume_scrape));
    assert2::assert!(consume_cpu.contains("enabled = true"));
    let consume_memory = profile_memory_block(consume_scrape);
    assert2::assert!(consume_memory.contains("enabled = false"));
}

#[test]
fn cpu_profiles_use_bounded_sampling_windows() {
    let config = alloy_config();
    for (scrape_name, expected_duration) in [
        ("crabka_services", "5s"),
        ("demo_apps", "5s"),
        ("demo_consume_cpu", "20s"),
        ("trace_cpu", "15s"),
        ("trace_block_builder_cpu", "30s"),
        ("crabka_workers", "3s"),
    ] {
        let scrape = scrape_block(&config, scrape_name);
        let process_cpu = normalize_whitespace(profile_process_cpu_block(scrape));
        check!(
            process_cpu.contains("enabled = true"),
            "{scrape_name} should keep CPU profiling enabled"
        );
        check!(
            scrape.contains(&format!(
                "delta_profiling_duration = \"{expected_duration}\""
            )),
            "{scrape_name} should keep CPU profiling overhead low enough for the demo stack"
        );
        check!(
            !process_cpu.contains("delta_profiling_duration"),
            "{scrape_name} should set the CPU duration on pyroscope.scrape, not profile.process_cpu"
        );
    }
}

#[test]
fn idle_profile_scrapes_are_lower_frequency() {
    let config = alloy_config();
    let consume_scrape = scrape_block(&config, "demo_consume_cpu");
    assert2::assert!(consume_scrape.contains("scrape_interval          = \"120s\""));
    assert2::assert!(consume_scrape.contains("scrape_timeout           = \"30s\""));

    let trace_scrape = scrape_block(&config, "trace_block_builder_cpu");
    assert2::assert!(trace_scrape.contains("scrape_interval          = \"60s\""));
}

#[test]
fn demo_app_image_does_not_enable_conflicting_heap_allocator() {
    let melange = demo_melange_config();
    let demo_app_build = melange
        .split("cargo build --release \\\n        -p crabka-cli")
        .nth(1)
        .and_then(|rest| rest.split("mkdir -p dist").next())
        .expect("demo app cargo build block exists");
    check!(
        melange.contains("-p observability-demo-app"),
        "demo image package should still build the demo app"
    );
    check!(
        !demo_app_build.contains("--features heap-profiling"),
        "demo app depends on turso, which already defines a global allocator"
    );
    check!(
        melange.contains("--features heap-profiling"),
        "Crabka service binaries should still expose jemalloc heap profiling"
    );
}

#[test]
fn demo_runtime_image_does_not_ship_full_dwarf_debug_sections() {
    let melange = demo_melange_config();
    assert2::assert!(!melange.contains("CARGO_PROFILE_RELEASE_DEBUG=true"));
    assert2::assert!(melange.contains("strip --strip-debug"));
}

#[test]
fn demo_image_is_built_with_apko_and_melange() {
    let compose = docker_compose();
    let melange = demo_melange_config();
    let apko = demo_apko_config();
    let workflow = demo_publish_workflow();

    assert2::assert!(!repo_root().join("demo/observability/Dockerfile").exists());
    assert2::assert!(melange.contains("package:\n  name: crabka-demo"));
    for bin in [
        "crabka-broker",
        "crabka",
        "crabka-metrics",
        "crabka-metrics-service",
        "crabka-traces",
        "crabka-observability",
        "crabka-profiles",
        "crabka-schema-registry",
        "observability-demo-app",
    ] {
        assert2::assert!(melange.contains(bin));
    }
    check!(
        apko.contains("- crabka-demo"),
        "apko image should install the local crabka-demo package"
    );
    check!(
        apko.contains("- curl"),
        "demo image should keep curl for compose healthchecks"
    );
    check!(
        workflow.contains("melange build packaging/melange/crabka-demo.yaml"),
        "publish-demo-image should build the demo APK with melange"
    );
    check!(
        workflow.contains("apko publish packaging/apko/crabka-demo.yaml"),
        "publish-demo-image should publish the demo OCI image with apko"
    );
    check!(
        !workflow.contains("docker/build-push-action"),
        "publish-demo-image should not use the Dockerfile build action"
    );
    check!(
        compose.contains("image: ghcr.io/robot-head/crabka-demo:latest"),
        "all demo Crabka services should pull the GHCR image by default"
    );
    check!(
        !compose.contains("image: crabka-demo:latest"),
        "compose should not require a short local crabka-demo tag for broker-format"
    );
}

#[test]
fn rustfs_bootstrap_verifies_obsolete_log_manifest_cleanup() {
    let bootstrap = rustfs_bootstrap_script();
    for (needle, _why) in [
        (
            "obsolete_logs_manifest_key=\"logs/tenant=demo/index/logs/manifest.json\"",
            "RustFS bootstrap should target the obsolete full logs manifest left by older demo revisions",
        ),
        (
            "obsolete_logs_shard_catalog_key=\"logs/tenant=demo/index/logs/shards/manifest.json\"",
            "RustFS bootstrap should also target the obsolete logs shard catalog left by older demo revisions",
        ),
        (
            "for attempt in 1 2 3 4 5; do",
            "RustFS bootstrap should retry obsolete manifest cleanup because RustFS can be busy during setup",
        ),
        (
            "s3api delete-object",
            "RustFS bootstrap should use the S3 API delete operation for obsolete manifest cleanup",
        ),
        (
            "s3api wait object-not-exists",
            "RustFS bootstrap should verify the obsolete manifest is gone after delete",
        ),
        (
            "failed to remove obsolete $label",
            "RustFS bootstrap should fail loudly when obsolete index cleanup fails",
        ),
    ] {
        assert2::assert!(bootstrap.contains(needle));
    }
    for label in ["\"logs full manifest\"", "\"logs shard catalog\""] {
        assert2::assert!(bootstrap.contains(label));
    }
    assert2::assert!(bootstrap.contains("exit 1"));
    assert2::assert!(!bootstrap.contains("manifest.json >/dev/null 2>&1 || true"));
}

#[test]
fn grafana_dashboard_provider_uses_stable_folder_uid() {
    let provider = dashboard_provider_config();
    assert2::assert!(provider.contains("folder: Crabka"));
    assert2::assert!(provider.contains("folderUid: crabka"));
}

#[test]
fn loki_datasource_does_not_enable_datasource_managed_alert_rules() {
    let config = grafana_datasource_config();
    let loki = datasource_block(&config, "uid: crabka-loki");
    assert2::assert!(loki.contains("type: loki"));
    assert2::assert!(loki.contains("manageAlerts: false"));
}

#[test]
fn recent_traces_dashboard_panel_renders_traceql_search_results() {
    let dashboard = dashboard("crabka-self.json");
    assert2::assert!(
        dashboard.contains("\"id\": 3, \"type\": \"table\", \"title\": \"Recent traces\"")
    );
    assert2::assert!(dashboard.contains("\"queryType\": \"traceql\""));
}

#[test]
fn self_dashboard_surfaces_service_heap_profiles() {
    let dashboard = dashboard("crabka-self.json");
    assert2::assert!(dashboard.contains("memory:inuse_space:bytes:space:bytes"));
    for service in [
        "broker",
        "metrics-distributor",
        "logs-distributor",
        "traces-distributor",
        "profiles-distributor",
    ] {
        assert2::assert!(dashboard.contains(service));
    }
}

#[test]
fn compose_and_alloy_collect_container_resource_metrics() {
    let compose = docker_compose();
    check!(
        compose.contains("cadvisor:"),
        "the observability stack should include cAdvisor for container CPU/RSS metrics"
    );
    check!(
        compose.contains("ghcr.io/google/cadvisor:"),
        "cAdvisor should use the upstream GHCR container image"
    );
    check!(
        compose.contains("\"/var/run/docker.sock:/var/run/docker.sock:ro\""),
        "cAdvisor should read Docker container metadata through the socket"
    );
    check!(
        compose.contains("--disable_metrics=app,cpuLoad,disk,oom_event,percpu,perf_event,pressure"),
        "cAdvisor should keep network and diskIO enabled for runtime I/O dashboards while skipping unused metric families"
    );
    check!(
        !compose.contains("diskIO,network"),
        "cAdvisor must not disable the network and diskIO families used to explain object-store pressure"
    );

    let config = alloy_config();
    let scrape = balanced_block_after_marker(&config, "prometheus.scrape \"containers\" {");
    assert2::assert!(scrape.contains("__address__ = \"cadvisor:8080\""));
    let scrape = normalize_whitespace(scrape);
    assert2::assert!(
        scrape.contains("forward_to = [prometheus.relabel.container_resources.receiver]")
    );
    let relabel =
        balanced_block_after_marker(&config, "prometheus.relabel \"container_resources\" {");
    let relabel = normalize_whitespace(relabel);
    assert2::assert!(relabel.contains("forward_to = [prometheus.remote_write.crabka.receiver]"));
    for metric in [
        "container_memory_rss",
        "container_memory_cache",
        "container_network_receive_bytes_total",
        "container_network_transmit_bytes_total",
        "container_fs_reads_bytes_total",
        "container_fs_writes_bytes_total",
    ] {
        assert2::assert!(relabel.contains(metric));
    }
    assert2::assert!(relabel.contains("device") && relabel.contains("interface"));
}

#[test]
fn runtime_resources_dashboard_surfaces_stack_cpu_and_memory() {
    let dashboard = dashboard("crabka-runtime.json");
    for (needle, _why) in [
        (
            "\"uid\": \"crabka-runtime\"",
            "runtime resource dashboard should have a stable UID",
        ),
        (
            "container_cpu_usage_seconds_total",
            "runtime dashboard should chart container CPU",
        ),
        (
            "container_memory_working_set_bytes",
            "runtime dashboard should chart container working-set memory",
        ),
        (
            "container_memory_rss",
            "runtime dashboard should break down querier memory into RSS and cache",
        ),
        (
            "container_memory_cache",
            "runtime dashboard should break down querier memory into RSS and cache",
        ),
        (
            "Querier memory breakdown",
            "runtime dashboard should include a querier memory breakdown panel",
        ),
        (
            "container_label_com_docker_compose_service=~\\\".*-querier\\\"",
            "querier memory breakdown should focus on querier services",
        ),
        (
            "container_label_com_docker_compose_project",
            "runtime dashboard should filter on the Docker Compose project label",
        ),
        (
            "crabka-observability-.*",
            "runtime dashboard should scope resource panels to Crabka observability compose projects",
        ),
        (
            "broker-format|rustfs-permissions|rustfs-setup|topic-setup",
            "runtime resource panels should exclude one-shot setup containers from steady-state resource rankings",
        ),
    ] {
        assert2::assert!(dashboard.contains(needle));
    }
    for service in [
        "rustfs",
        "grafana",
        "alloy",
        "metrics-querier",
        "traces-querier",
        "logs-querier",
        "profiles-querier",
    ] {
        assert2::assert!(dashboard.contains(service));
    }
}

#[test]
fn runtime_resources_dashboard_surfaces_stack_io_hotspots() {
    let dashboard = dashboard("crabka-runtime.json");
    for title in [
        "Network I/O by service",
        "Filesystem I/O by service",
        "Top network I/O users",
        "Object-store path I/O",
    ] {
        assert2::assert!(dashboard.contains(title));
    }
    for metric in [
        "container_network_receive_bytes_total",
        "container_network_transmit_bytes_total",
        "container_fs_reads_bytes_total",
        "container_fs_writes_bytes_total",
    ] {
        assert2::assert!(dashboard.contains(metric));
    }
    assert2::assert!(dashboard.contains("rustfs|.*-querier|.*-compactor|.*-block-builder"));
}

#[test]
fn rustfs_dashboard_surfaces_object_store_health_and_io() {
    let dashboard = dashboard("crabka-rustfs.json");
    assert2::assert!(dashboard.contains("\"uid\": \"crabka-rustfs\""));
    assert2::assert!(dashboard.contains("\"title\": \"Crabka - RustFS Object Store\""));
    for title in [
        "Container memory working set",
        "Memory limit ratio",
        "CPU usage",
        "RustFS network I/O",
        "Filesystem I/O",
        "RustFS uptime",
        "Restarts (1h)",
        "RustFS object metric bytes",
        "Raw drive used",
        "Drive/object metric delta",
        "Storage growth and S3 operations",
        "S3 operation rate by bucket",
        "Bucket objects and versions",
        "Background storage work",
        "Recent RustFS warnings and errors",
        "Object-store client retry logs",
    ] {
        assert2::assert!(dashboard.contains(title));
    }
    for metric in [
        "container_memory_working_set_bytes",
        "container_spec_memory_limit_bytes",
        "container_cpu_usage_seconds_total",
        "container_network_receive_bytes_total",
        "container_network_transmit_bytes_total",
        "container_fs_reads_bytes_total",
        "container_fs_writes_bytes_total",
        "container_start_time_seconds",
        "rustfs_s3_operations_total",
        "rustfs_cluster_capacity_used_bytes",
        "rustfs_cluster_usage_buckets_total_bytes",
        "rustfs_cluster_usage_buckets_objects_count",
        "rustfs_cluster_usage_buckets_versions_count",
        "rustfs_cluster_usage_buckets_object_version_count_distribution",
        "rustfs_page_cache_reclaim_duration_seconds_count",
        "rustfs_capacity_update_duration_seconds_count",
        "rustfs_capacity_scan_disk_duration_seconds_count",
        "rustfs_lock_acquire_total",
    ] {
        assert2::assert!(dashboard.contains(metric));
    }
    check!(
        dashboard.contains("container_label_com_docker_compose_service=\\\"rustfs\\\""),
        "RustFS dashboard should scope resource panels to the RustFS service"
    );
    check!(
        dashboard.contains("{service_name=\\\"rustfs\\\"}"),
        "RustFS dashboard should include RustFS logs"
    );
    check!(
        dashboard.contains("object_store::client::retry"),
        "RustFS dashboard should surface S3/object-store retry chatter from Crabka clients"
    );
    check!(
        !dashboard.contains(
            "max(rustfs_cluster_capacity_used_bytes) or max(rustfs_cluster_usage_objects_total_bytes)"
        ),
        "object usage panels must not prefer raw drive capacity over RustFS object metrics"
    );
    check!(
        dashboard.contains("clamp_min(((max(rustfs_cluster_capacity_used_bytes)"),
        "RustFS dashboard should make raw-drive to object-metric deltas visible"
    );
}

#[test]
fn runtime_resources_dashboard_surfaces_container_restarts() {
    let dashboard = dashboard("crabka-runtime.json");
    for (needle, _why) in [
        (
            "Shortest container uptime",
            "runtime dashboard should make recently recreated containers obvious",
        ),
        (
            "Container start changes (1h)",
            "runtime dashboard should show container start-time changes over the last hour",
        ),
        (
            "container_start_time_seconds",
            "runtime dashboard should use cAdvisor start-time metrics for restart detection",
        ),
        (
            "changes(max by (container_label_com_docker_compose_service) (container_start_time_seconds",
            "runtime dashboard should count start-time changes by Compose service",
        ),
    ] {
        assert2::assert!(dashboard.contains(needle));
    }
}

#[test]
fn alerts_surface_recent_observability_container_restarts() {
    let alerts = grafana_alerting_config();
    for (needle, _why) in [
        (
            "uid: crabka-obs-container-restarted",
            "Grafana alerts should include a stable UID for observability container restarts",
        ),
        (
            "title: Observability container restarted recently",
            "Grafana alerts should name the restart condition clearly",
        ),
        (
            "container_start_time_seconds",
            "restart alert should be driven by cAdvisor container start times",
        ),
        (
            "changes(max by (container_label_com_docker_compose_service) (container_start_time_seconds",
            "restart alert should detect recent start-time changes by Compose service",
        ),
    ] {
        assert2::assert!(alerts.contains(needle));
    }
    for service in [
        "alloy",
        "cadvisor",
        "grafana",
        "metrics-querier",
        "logs-querier",
        "traces-querier",
        "profiles-querier",
    ] {
        assert2::assert!(alerts.contains(service));
    }
}

#[test]
fn streams_dns_timeout_is_configurable_only_on_the_stream_role() {
    let compose = docker_compose();
    let stream = compose_service_block(&compose, "demo-stream");
    assert2::assert!(stream.contains(
        "CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS: \"${CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS:-10000}\""
    ));
    for service in ["demo-produce", "demo-consume"] {
        assert2::assert!(
            !compose_service_block(&compose, service)
                .contains("CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS")
        );
    }
}

#[test]
fn streams_runtime_policy_is_configurable_only_on_the_stream_role() {
    let compose = docker_compose();
    let stream = compose_service_block(&compose, "demo-stream");
    assert2::assert!(stream.contains(
        "CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS: \"${CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS:-200}\""
    ));
    assert2::assert!(stream.contains(
        "CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS: \"${CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS:-5000}\""
    ));
    assert2::assert!(stream.contains(
        "CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS: \"${CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS:-30000}\""
    ));
    assert2::assert!(stream.contains(
        "CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS: \"${CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS:-5000}\""
    ));
    assert2::assert!(stream.contains(
        "CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS: \"${CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS:-200}\""
    ));
    assert2::assert!(stream.contains(
        "CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY: \"${CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY:-64}\""
    ));
    assert2::assert!(stream.contains(
        "CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES: \"${CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES:-10485760}\""
    ));
    for service in ["demo-produce", "demo-consume"] {
        let service = compose_service_block(&compose, service);
        assert2::assert!(!service.contains("CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS"));
        assert2::assert!(!service.contains("CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS"));
        assert2::assert!(!service.contains("CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS"));
        assert2::assert!(!service.contains("CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS"));
        assert2::assert!(!service.contains("CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS"));
        assert2::assert!(!service.contains("CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY"));
        assert2::assert!(!service.contains("CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES"));
    }
}

#[test]
fn consumer_leave_timeout_is_configurable_only_on_the_consume_role() {
    let compose = docker_compose();
    let consume = compose_service_block(&compose, "demo-consume");
    assert2::assert!(consume.contains(
        "CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS: \"${CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS:-5000}\""
    ));
    for service in ["demo-produce", "demo-stream"] {
        assert2::assert!(
            !compose_service_block(&compose, service)
                .contains("CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS")
        );
    }
}

#[test]
fn consumer_metadata_refresh_is_configurable_only_on_the_consume_role() {
    let compose = docker_compose();
    let consume = compose_service_block(&compose, "demo-consume");
    assert2::assert!(consume.contains(
        "CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS: \"${CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS:-5000}\""
    ));
    for service in ["demo-produce", "demo-stream"] {
        assert2::assert!(
            !compose_service_block(&compose, service)
                .contains("CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS")
        );
    }
}
