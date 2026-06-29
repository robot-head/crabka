use std::path::{Path, PathBuf};

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
    assert!(
        relabel.contains("__meta_docker_container_label_com_docker_compose_project"),
        "Docker log discovery should inspect the Compose project label before forwarding targets"
    );
    assert!(
        relabel.contains("action        = \"keep\""),
        "Docker log discovery should drop non-demo containers instead of tailing every Docker container"
    );
    assert!(
        relabel.contains("regex         = \"crabka-observability-demo\""),
        "Docker log discovery should only tail the demo Compose project"
    );
}

#[test]
fn crabka_worker_targets_also_collect_memory_profiles() {
    let config = alloy_config();
    let scrape = scrape_block(&config, "crabka_workers");
    let process_cpu = normalize_whitespace(profile_process_cpu_block(scrape));
    assert!(
        process_cpu.contains("enabled = true"),
        "worker roles should keep CPU profiling enabled"
    );
    let memory = profile_memory_block(scrape);
    assert!(
        memory.contains("enabled = true"),
        "worker roles should collect memory profiles too"
    );
    assert!(
        memory.contains("path    = \"/debug/pprof/heap\""),
        "worker roles should scrape the jemalloc pprof heap endpoint"
    );
}

#[test]
fn jemalloc_heap_profiling_uses_bounded_always_on_sampling() {
    let compose = docker_compose();
    assert!(
        compose.contains("MALLOC_CONF: \"prof:true,prof_active:true,lg_prof_sample:25,background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000\""),
        "heap profiling should stay active so memory profiles include long-lived allocations"
    );
    assert!(
        compose.contains("lg_prof_sample:25"),
        "always-on heap profiling should use coarse sampling to bound jemalloc backtrace overhead without empty profiles on small queriers"
    );
    for option in [
        "background_thread:true",
        "dirty_decay_ms:1000",
        "muzzy_decay_ms:1000",
    ] {
        assert!(
            compose.contains(option),
            "jemalloc should promptly release heap-profile/query allocations: missing {option}"
        );
    }
    assert!(
        !compose
            .lines()
            .any(|line| line.trim_start().starts_with("MALLOC_CONF:")
                && line.contains("lg_prof_sample:0")),
        "lg_prof_sample:0 backtraces every allocation and is too expensive for the demo stack"
    );
}

#[test]
fn demo_app_profiles_cover_all_roles_without_heap_scrape() {
    let config = alloy_config();
    let scrape = scrape_block(&config, "demo_apps");
    for role in ["demo-produce", "demo-stream", "demo-consume"] {
        assert!(
            scrape.contains(&format!("service_name = \"{role}\"")),
            "demo app profile scrape should include {role}"
        );
    }
    let process_cpu = normalize_whitespace(profile_process_cpu_block(scrape));
    assert!(
        process_cpu.contains("enabled = true"),
        "demo app roles should keep CPU profiling enabled"
    );
    let memory = profile_memory_block(scrape);
    assert!(
        memory.contains("enabled = false"),
        "demo app roles should not scrape /debug/pprof/heap until their allocator supports it"
    );
}

#[test]
fn cpu_profiles_use_bounded_sampling_windows() {
    let config = alloy_config();
    for (scrape_name, expected_duration) in [
        ("crabka_services", "5s"),
        ("demo_apps", "5s"),
        ("crabka_workers", "3s"),
    ] {
        let scrape = scrape_block(&config, scrape_name);
        let process_cpu = normalize_whitespace(profile_process_cpu_block(scrape));
        assert!(
            process_cpu.contains("enabled = true"),
            "{scrape_name} should keep CPU profiling enabled"
        );
        assert!(
            scrape.contains(&format!(
                "delta_profiling_duration = \"{expected_duration}\""
            )),
            "{scrape_name} should keep CPU profiling overhead low enough for the demo stack"
        );
        assert!(
            !process_cpu.contains("delta_profiling_duration"),
            "{scrape_name} should set the CPU duration on pyroscope.scrape, not profile.process_cpu"
        );
    }
}

#[test]
fn demo_app_image_does_not_enable_conflicting_heap_allocator() {
    let melange = demo_melange_config();
    let demo_app_build = melange
        .split("cargo build --release \\\n        -p crabka-cli")
        .nth(1)
        .and_then(|rest| rest.split("mkdir -p dist").next())
        .expect("demo app cargo build block exists");
    assert!(
        melange.contains("-p observability-demo-app"),
        "demo image package should still build the demo app"
    );
    assert!(
        !demo_app_build.contains("--features heap-profiling"),
        "demo app depends on turso, which already defines a global allocator"
    );
    assert!(
        melange.contains("--features heap-profiling"),
        "Crabka service binaries should still expose jemalloc heap profiling"
    );
}

#[test]
fn demo_runtime_image_does_not_ship_full_dwarf_debug_sections() {
    let melange = demo_melange_config();
    assert!(
        !melange.contains("CARGO_PROFILE_RELEASE_DEBUG=true"),
        "full DWARF release debuginfo makes the runtime binaries multi-GiB and inflates querier RSS"
    );
    assert!(
        melange.contains("strip --strip-debug"),
        "runtime binaries should remove DWARF debug sections while keeping symbols for readable profiles"
    );
}

#[test]
fn demo_image_is_built_with_apko_and_melange() {
    let compose = docker_compose();
    let melange = demo_melange_config();
    let apko = demo_apko_config();
    let workflow = demo_publish_workflow();

    assert!(
        !repo_root().join("demo/observability/Dockerfile").exists(),
        "the observability demo image should not keep a Dockerfile build path"
    );
    assert!(
        melange.contains("package:\n  name: crabka-demo"),
        "melange should produce a dedicated crabka-demo APK package"
    );
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
        assert!(
            melange.contains(bin),
            "demo package should install binary {bin}"
        );
    }
    assert!(
        apko.contains("- crabka-demo"),
        "apko image should install the local crabka-demo package"
    );
    assert!(
        apko.contains("- curl"),
        "demo image should keep curl for compose healthchecks"
    );
    assert!(
        workflow.contains("melange build packaging/melange/crabka-demo.yaml"),
        "publish-demo-image should build the demo APK with melange"
    );
    assert!(
        workflow.contains("apko publish packaging/apko/crabka-demo.yaml"),
        "publish-demo-image should publish the demo OCI image with apko"
    );
    assert!(
        !workflow.contains("docker/build-push-action"),
        "publish-demo-image should not use the Dockerfile build action"
    );
    assert!(
        compose.contains("image: ghcr.io/robot-head/crabka-demo:latest"),
        "all demo Crabka services should pull the GHCR image by default"
    );
    assert!(
        !compose.contains("image: crabka-demo:latest"),
        "compose should not require a short local crabka-demo tag for broker-format"
    );
}

#[test]
fn rustfs_bootstrap_verifies_obsolete_log_manifest_cleanup() {
    let bootstrap = rustfs_bootstrap_script();
    assert!(
        bootstrap
            .contains("obsolete_logs_manifest_key=\"logs/tenant=demo/index/logs/manifest.json\""),
        "RustFS bootstrap should target the obsolete full logs manifest left by older demo revisions"
    );
    assert!(
        bootstrap.contains("for attempt in 1 2 3 4 5; do"),
        "RustFS bootstrap should retry obsolete manifest cleanup because RustFS can be busy during setup"
    );
    assert!(
        bootstrap.contains("s3api delete-object"),
        "RustFS bootstrap should use the S3 API delete operation for obsolete manifest cleanup"
    );
    assert!(
        bootstrap.contains("s3api wait object-not-exists"),
        "RustFS bootstrap should verify the obsolete manifest is gone after delete"
    );
    assert!(
        bootstrap.contains("failed to remove obsolete logs full manifest"),
        "RustFS bootstrap should fail loudly when an obsolete manifest survives cleanup"
    );
    assert!(
        !bootstrap.contains("manifest.json >/dev/null 2>&1 || true"),
        "obsolete manifest cleanup must not be silently ignored"
    );
}

#[test]
fn grafana_dashboard_provider_uses_stable_folder_uid() {
    let provider = dashboard_provider_config();
    assert!(
        provider.contains("folder: Crabka"),
        "dashboard provisioning should keep the user-visible Crabka folder name"
    );
    assert!(
        provider.contains("folderUid: crabka"),
        "dashboard provisioning should pin a stable folder UID so folder URLs survive Grafana DB recreation"
    );
}

#[test]
fn loki_datasource_does_not_enable_datasource_managed_alert_rules() {
    let config = grafana_datasource_config();
    let loki = datasource_block(&config, "uid: crabka-loki");
    assert!(
        loki.contains("type: loki"),
        "the Crabka logs datasource should remain a Loki datasource"
    );
    assert!(
        loki.contains("manageAlerts: false"),
        "the demo provisions Grafana-managed alert rules, so Grafana should not probe Loki ruler APIs for datasource-managed rules"
    );
}

#[test]
fn recent_traces_dashboard_panel_renders_traceql_search_results() {
    let dashboard = dashboard("crabka-self.json");
    assert!(
        dashboard.contains("\"id\": 3, \"type\": \"table\", \"title\": \"Recent traces\""),
        "TraceQL search results are table-shaped; the dashboard should use Grafana's table panel"
    );
    assert!(
        dashboard.contains("\"queryType\": \"traceql\""),
        "recent traces panel should keep querying Tempo with TraceQL"
    );
}

#[test]
fn self_dashboard_surfaces_querier_heap_profiles() {
    let dashboard = dashboard("crabka-self.json");
    assert!(
        dashboard.contains("memory:inuse_space:bytes:space:bytes"),
        "self-observability dashboard should expose jemalloc heap profiles"
    );
    for service in [
        "metrics-querier",
        "logs-querier",
        "traces-querier",
        "profiles-querier",
    ] {
        assert!(
            dashboard.contains(&format!("service_name=\\\"{service}\\\"")),
            "self-observability dashboard should include a heap profile panel for {service}"
        );
    }
}

#[test]
fn compose_and_alloy_collect_container_resource_metrics() {
    let compose = docker_compose();
    assert!(
        compose.contains("cadvisor:"),
        "the observability stack should include cAdvisor for container CPU/RSS metrics"
    );
    assert!(
        compose.contains("ghcr.io/google/cadvisor:"),
        "cAdvisor should use the upstream GHCR container image"
    );
    assert!(
        compose.contains("\"/var/run/docker.sock:/var/run/docker.sock:ro\""),
        "cAdvisor should read Docker container metadata through the socket"
    );
    assert!(
        compose.contains("--disable_metrics=app,cpuLoad,disk,oom_event,percpu,perf_event,pressure"),
        "cAdvisor should keep network and diskIO enabled for runtime I/O dashboards while skipping unused metric families"
    );
    assert!(
        !compose.contains("diskIO,network"),
        "cAdvisor must not disable the network and diskIO families used to explain object-store pressure"
    );

    let config = alloy_config();
    let scrape = balanced_block_after_marker(&config, "prometheus.scrape \"containers\" {");
    assert!(
        scrape.contains("__address__ = \"cadvisor:8080\""),
        "Alloy should scrape cAdvisor inside the compose network"
    );
    let scrape = normalize_whitespace(scrape);
    assert!(
        scrape.contains("forward_to = [prometheus.relabel.container_resources.receiver]"),
        "container metrics should pass through the resource relabel stage"
    );
    let relabel =
        balanced_block_after_marker(&config, "prometheus.relabel \"container_resources\" {");
    let relabel = normalize_whitespace(relabel);
    assert!(
        relabel.contains("forward_to = [prometheus.remote_write.crabka.receiver]"),
        "container metrics should be written into Crabka metrics"
    );
    for metric in [
        "container_network_receive_bytes_total",
        "container_network_transmit_bytes_total",
        "container_fs_reads_bytes_total",
        "container_fs_writes_bytes_total",
    ] {
        assert!(
            relabel.contains(metric),
            "container relabeling should keep {metric} for runtime I/O dashboards"
        );
    }
    assert!(
        relabel.contains("device") && relabel.contains("interface"),
        "container relabeling should keep device/interface labels so cAdvisor I/O series remain distinct before dashboard aggregation"
    );
}

#[test]
fn runtime_resources_dashboard_surfaces_stack_cpu_and_memory() {
    let dashboard = dashboard("crabka-runtime.json");
    assert!(
        dashboard.contains("\"uid\": \"crabka-runtime\""),
        "runtime resource dashboard should have a stable UID"
    );
    assert!(
        dashboard.contains("container_cpu_usage_seconds_total"),
        "runtime dashboard should chart container CPU"
    );
    assert!(
        dashboard.contains("container_memory_working_set_bytes"),
        "runtime dashboard should chart container working-set memory"
    );
    assert!(
        dashboard.contains("container_label_com_docker_compose_project"),
        "runtime dashboard should filter on the Docker Compose project label"
    );
    assert!(
        dashboard.contains("crabka-observability-demo"),
        "runtime dashboard should scope resource panels to this compose project"
    );
    assert!(
        dashboard.contains("broker-format|rustfs-permissions|rustfs-setup|topic-setup"),
        "runtime resource panels should exclude one-shot setup containers from steady-state resource rankings"
    );
    for service in [
        "rustfs",
        "grafana",
        "alloy",
        "metrics-querier",
        "traces-querier",
        "logs-querier",
        "profiles-querier",
    ] {
        assert!(
            dashboard.contains(service),
            "runtime dashboard should make {service} visible"
        );
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
        assert!(
            dashboard.contains(title),
            "runtime dashboard should include panel {title}"
        );
    }
    for metric in [
        "container_network_receive_bytes_total",
        "container_network_transmit_bytes_total",
        "container_fs_reads_bytes_total",
        "container_fs_writes_bytes_total",
    ] {
        assert!(
            dashboard.contains(metric),
            "runtime dashboard should query {metric}"
        );
    }
    assert!(
        dashboard.contains("rustfs|.*-querier|.*-compactor|.*-block-builder"),
        "runtime dashboard should isolate the object-store-facing services"
    );
}

#[test]
fn rustfs_dashboard_surfaces_object_store_health_and_io() {
    let dashboard = dashboard("crabka-rustfs.json");
    assert!(
        dashboard.contains("\"uid\": \"crabka-rustfs\""),
        "RustFS dashboard should have a stable UID"
    );
    assert!(
        dashboard.contains("\"title\": \"Crabka - RustFS Object Store\""),
        "RustFS dashboard should have a clear object-store title"
    );
    for title in [
        "Container memory working set",
        "Memory limit ratio",
        "CPU usage",
        "RustFS network I/O",
        "Filesystem I/O",
        "RustFS uptime",
        "Restarts (1h)",
        "Storage growth and S3 operations",
        "S3 operation rate by bucket",
        "Bucket objects and versions",
        "Background storage work",
        "Recent RustFS warnings and errors",
        "Object-store client retry logs",
    ] {
        assert!(
            dashboard.contains(title),
            "RustFS dashboard should include panel {title}"
        );
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
        "rustfs_cluster_usage_buckets_objects_count",
        "rustfs_cluster_usage_buckets_versions_count",
        "rustfs_cluster_usage_buckets_object_version_count_distribution",
        "rustfs_page_cache_reclaim_duration_seconds_count",
        "rustfs_capacity_update_duration_seconds_count",
        "rustfs_capacity_scan_disk_duration_seconds_count",
        "rustfs_lock_acquire_total",
    ] {
        assert!(
            dashboard.contains(metric),
            "RustFS dashboard should query metric {metric}"
        );
    }
    assert!(
        dashboard.contains("container_label_com_docker_compose_service=\\\"rustfs\\\""),
        "RustFS dashboard should scope resource panels to the RustFS service"
    );
    assert!(
        dashboard.contains("{service_name=\\\"rustfs\\\"}"),
        "RustFS dashboard should include RustFS logs"
    );
    assert!(
        dashboard.contains("object_store::client::retry"),
        "RustFS dashboard should surface S3/object-store retry chatter from Crabka clients"
    );
}

#[test]
fn runtime_resources_dashboard_surfaces_container_restarts() {
    let dashboard = dashboard("crabka-runtime.json");
    assert!(
        dashboard.contains("Shortest container uptime"),
        "runtime dashboard should make recently recreated containers obvious"
    );
    assert!(
        dashboard.contains("Container start changes (1h)"),
        "runtime dashboard should show container start-time changes over the last hour"
    );
    assert!(
        dashboard.contains("container_start_time_seconds"),
        "runtime dashboard should use cAdvisor start-time metrics for restart detection"
    );
    assert!(
        dashboard.contains(
            "changes(max by (container_label_com_docker_compose_service) (container_start_time_seconds"
        ),
        "runtime dashboard should count start-time changes by Compose service"
    );
}

#[test]
fn alerts_surface_recent_observability_container_restarts() {
    let alerts = grafana_alerting_config();
    assert!(
        alerts.contains("uid: crabka-obs-container-restarted"),
        "Grafana alerts should include a stable UID for observability container restarts"
    );
    assert!(
        alerts.contains("title: Observability container restarted recently"),
        "Grafana alerts should name the restart condition clearly"
    );
    assert!(
        alerts.contains("container_start_time_seconds"),
        "restart alert should be driven by cAdvisor container start times"
    );
    assert!(
        alerts.contains(
            "changes(max by (container_label_com_docker_compose_service) (container_start_time_seconds"
        ),
        "restart alert should detect recent start-time changes by Compose service"
    );
    for service in [
        "alloy",
        "cadvisor",
        "grafana",
        "metrics-querier",
        "logs-querier",
        "traces-querier",
        "profiles-querier",
    ] {
        assert!(
            alerts.contains(service),
            "restart alert should watch {service}"
        );
    }
}
