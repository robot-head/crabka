use assert2::check;
use crabka_sdk_conformance::{
    Harness, HarnessConfig, HarnessSubstrate,
    vectors::{ContractVersion, load_vectors},
};

const QUEUE_V1_1_VECTOR_IDS: &[&str] = &[
    "queue_v1_1_ack_error_shape",
    "queue_v1_1_ack_shape",
    "queue_v1_1_acquire_error_shape",
    "queue_v1_1_acquire_shape",
    "queue_v1_1_lock_duration_error_shape",
    "queue_v1_1_renew_shape",
];

const TOTAL_VECTOR_COUNT: usize = 15;
const V1_0_APPLICABLE_VECTOR_COUNT: usize = 9;
const V1_1_APPLICABLE_VECTOR_COUNT: usize = 14;
const LIVE_COMPATIBLE_VECTOR_COUNT: usize = 9;

static ADAPTER_HARNESS_TEST_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> =
    std::sync::OnceLock::new();

async fn adapter_harness_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    ADAPTER_HARNESS_TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

fn conformance_bin() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_conformance"))
}

fn vectors_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("vectors/v1")
}

fn jvm_queue_cross_consumer_script() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../grpc-gateway/tests/scripts/jvm_queue_cross_consumer.sh")
}

fn workspace_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("sdk-conformance lives under the workspace crates directory")
        .to_path_buf()
}

fn rust_conformance_adapter_bin() -> std::path::PathBuf {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = std::process::Command::new(cargo)
        .current_dir(workspace_root())
        .args([
            "build",
            "-p",
            "crabka-app-sdk",
            "--features",
            "conformance-adapter",
            "--bin",
            "conformance_adapter",
            "--message-format=json-render-diagnostics",
        ])
        .output()
        .expect("cargo builds the Rust conformance adapter");
    assert!(
        output.status.success(),
        "cargo build for Rust conformance adapter failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .find_map(conformance_adapter_executable)
        .unwrap_or_else(|| {
            panic!(
                "cargo did not report a conformance_adapter executable\nstdout:\n{}\nstderr:\n{}",
                stdout,
                String::from_utf8_lossy(&output.stderr)
            )
        })
}

fn conformance_adapter_executable(cargo_message: &str) -> Option<std::path::PathBuf> {
    let message = serde_json::from_str::<serde_json::Value>(cargo_message).ok()?;
    if message.get("reason")?.as_str()? != "compiler-artifact" {
        return None;
    }
    if message.pointer("/target/name")?.as_str()? != "conformance_adapter" {
        return None;
    }

    message
        .get("executable")?
        .as_str()
        .map(std::path::PathBuf::from)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mock_adapter_passes_contract_vectors() {
    let _guard = adapter_harness_test_guard().await;
    let harness = Harness::new(HarnessConfig {
        adapter: conformance_bin(),
        adapter_args: vec!["--mock-adapter".into()],
        vectors_dir: vectors_dir(),
        filter: None,
        endpoint: "mock://gateway".into(),
        substrate: HarnessSubstrate::External,
        live_compatible_only: false,
    });

    let summary = harness.run().await.unwrap();

    check!(summary.failed.is_empty());
    check!(summary.passed == V1_0_APPLICABLE_VECTOR_COUNT);
    check!(summary.skipped.len() == QUEUE_V1_1_VECTOR_IDS.len());
    check!(summary.passed + summary.skipped.len() == TOTAL_VECTOR_COUNT);
    check!(summary.skipped.iter().any(|skipped| {
        skipped.vector_id == "queue_v1_1_acquire_shape"
            && skipped.reason == "requires contract 1.1; adapter declares 1.0"
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mock_adapter_v1_1_passes_queue_vectors_without_stub_queue() {
    let _guard = adapter_harness_test_guard().await;
    let harness = Harness::new(HarnessConfig {
        adapter: conformance_bin(),
        adapter_args: vec![
            "--mock-adapter".into(),
            "--mock-contract-minor".into(),
            "1".into(),
        ],
        vectors_dir: vectors_dir(),
        filter: None,
        endpoint: "mock://gateway".into(),
        substrate: HarnessSubstrate::External,
        live_compatible_only: false,
    });

    let summary = harness.run().await.unwrap();

    check!(summary.failed.is_empty());
    check!(summary.passed == V1_1_APPLICABLE_VECTOR_COUNT);
    check!(summary.skipped.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mock_adapter_v1_1_passes_each_queue_vector() {
    let _guard = adapter_harness_test_guard().await;
    for vector_id in QUEUE_V1_1_VECTOR_IDS {
        let harness = Harness::new(HarnessConfig {
            adapter: conformance_bin(),
            adapter_args: vec![
                "--mock-adapter".into(),
                "--mock-contract-minor".into(),
                "1".into(),
            ],
            vectors_dir: vectors_dir(),
            filter: Some((*vector_id).into()),
            endpoint: "mock://gateway".into(),
            substrate: HarnessSubstrate::External,
            live_compatible_only: false,
        });

        let summary = harness.run().await.unwrap();

        check!(summary.failed.is_empty());
        check!(summary.passed == 1);
        check!(summary.skipped.is_empty());
    }
}

#[test]
fn queue_v1_1_vectors_are_versioned_and_loaded_by_mock_contract() {
    let vectors = load_vectors(vectors_dir()).unwrap();
    check!(vectors.len() == TOTAL_VECTOR_COUNT);
    let queue_vectors = vectors
        .iter()
        .filter(|vector| QUEUE_V1_1_VECTOR_IDS.contains(&vector.id.as_str()))
        .collect::<Vec<_>>();

    check!(queue_vectors.len() == QUEUE_V1_1_VECTOR_IDS.len());
    for vector in queue_vectors {
        check!(vector.since == ContractVersion::new(1, 1));
        check!(vector.until.is_none());
        check!(ContractVersion::new(1, 1).satisfies(vector));
        check!(!ContractVersion::new(1, 0).satisfies(vector));
    }
}

#[test]
fn jvm_queue_cross_consumer_script_dry_run_is_concrete() {
    let output = std::process::Command::new("/bin/sh")
        .arg(jvm_queue_cross_consumer_script())
        .args([
            "--gateway-endpoint",
            "http://127.0.0.1:18080",
            "--bootstrap-server",
            "host.docker.internal:9092",
            "--topic",
            "queue-jvm-cross-consumer",
            "--group",
            "queue-jvm-cross-consumer-group",
            "--dry-run",
        ])
        .output()
        .unwrap();

    check!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    check!(stdout.contains("kafka-console-producer.sh"));
    check!(stdout.contains("QueueAcquire"));
    check!(stdout.contains("QueueAcknowledge"));
    check!(stdout.contains("kafka-console-share-consumer.sh"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_compatible_mode_runs_queue_v1_1_vectors_when_adapter_supports_them() {
    let _guard = adapter_harness_test_guard().await;
    let harness = Harness::new(HarnessConfig {
        adapter: conformance_bin(),
        adapter_args: vec![
            "--mock-adapter".into(),
            "--mock-contract-minor".into(),
            "1".into(),
        ],
        vectors_dir: vectors_dir(),
        filter: None,
        endpoint: "mock://gateway".into(),
        substrate: HarnessSubstrate::External,
        live_compatible_only: true,
    });

    let summary = harness.run().await.unwrap();

    check!(summary.failed.is_empty());
    check!(summary.passed == LIVE_COMPATIBLE_VECTOR_COUNT);
    check!(summary.skipped.len() == 5);
    check!(summary.passed + summary.skipped.len() == V1_1_APPLICABLE_VECTOR_COUNT);
    for vector_id in QUEUE_V1_1_VECTOR_IDS {
        check!(
            summary
                .skipped
                .iter()
                .all(|skipped| skipped.vector_id != *vector_id)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mismatch_reports_vector_id_and_step() {
    let _guard = adapter_harness_test_guard().await;
    let harness = Harness::new(HarnessConfig {
        adapter: conformance_bin(),
        adapter_args: vec![
            "--mock-adapter".into(),
            "--mock-fault".into(),
            "wrong-publish".into(),
        ],
        vectors_dir: vectors_dir(),
        filter: Some("messaging_roundtrip".into()),
        endpoint: "mock://gateway".into(),
        substrate: HarnessSubstrate::External,
        live_compatible_only: false,
    });

    let summary = harness.run().await.unwrap();

    check!(summary.failed.len() == 1);
    check!(summary.failed[0].vector_id == "messaging_roundtrip");
    check!(summary.failed[0].step == "publish raw record");
    check!(summary.skipped.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_adapter_live_substrate_passes_supported_live_vectors() {
    let _guard = adapter_harness_test_guard().await;
    let harness = Harness::new(HarnessConfig {
        adapter: rust_conformance_adapter_bin(),
        adapter_args: vec![],
        vectors_dir: vectors_dir(),
        filter: None,
        endpoint: "mock://gateway".into(),
        substrate: HarnessSubstrate::Live,
        live_compatible_only: true,
    });

    let summary = harness.run().await.unwrap();

    check!(summary.failed.is_empty());
    check!(summary.passed == LIVE_COMPATIBLE_VECTOR_COUNT);
    check!(summary.skipped.len() == 5);
    check!(summary.passed + summary.skipped.len() == V1_1_APPLICABLE_VECTOR_COUNT);
    check!(
        summary
            .skipped
            .iter()
            .all(|skipped| !QUEUE_V1_1_VECTOR_IDS.contains(&skipped.vector_id.as_str()))
    );
    check!(
        summary
            .skipped
            .iter()
            .any(|skipped| skipped.vector_id == "error_mapping")
    );
}
