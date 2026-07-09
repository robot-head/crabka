use assert2::assert;
use bytes::Bytes;
use crabka_page_store::{PAGE_SIZE, PageKey};
use crabka_pageserver::{
    BasebackupMetadataRequest, GetPageRequest, InMemoryTimelineStore, PageService, PostgresRedo,
    SyntheticRedoCodec, TimelineKey,
};
use crabka_postgres_wal::Lsn;

const PG17_FIXTURE_MANIFEST: &str = include_str!("../../postgres-wal/tests/fixtures/manifest.toml");
const REQUIRED_PG6_FORK_ORACLES: &[&str] = &[
    "fork/manifest.toml",
    "fork/parent/wal",
    "fork/child/wal",
    "fork/parent/standby-oracle",
    "fork/child/standby-oracle",
];
const REQUIRED_REAL_PG17_ORACLES: &[&str] = &[
    "oracle.waldump",
    "standby/relations.tsv",
    "standby/relations",
    "standby/slru/pg_xact",
    "standby/slru/pg_multixact",
];

fn timeline(branch: &str, timeline: &str) -> TimelineKey {
    TimelineKey::parse(branch, "tenant", timeline).expect("timeline identifiers are valid")
}

fn key(block_number: u32) -> PageKey {
    PageKey::new(1663, 5, 16_384, 0, block_number)
}

fn page(fill: u8) -> Bytes {
    Bytes::from(vec![fill; PAGE_SIZE])
}

fn service() -> PageService<PostgresRedo<SyntheticRedoCodec>> {
    PageService::new(
        InMemoryTimelineStore::new(),
        PostgresRedo::new(SyntheticRedoCodec),
    )
}

#[tokio::test]
async fn synthetic_fork_gate_preserves_ancestor_and_diverges_after_branch() {
    let service = service();
    let parent = timeline("main", "parent");
    let child = timeline("child", "child");
    let inherited_key = key(0);
    let diverged_key = key(1);
    let parent_base = page(1);
    let parent_diverged = page(2);
    let child_diverged = page(3);
    {
        let mut store = service.store_mut().await;
        store.create_timeline(&parent);
        store
            .put_image(&parent, inherited_key, Lsn(10), parent_base.clone())
            .await
            .expect("parent inherited page writes");
        store
            .put_image(&parent, diverged_key, Lsn(20), parent_diverged.clone())
            .await
            .expect("parent pre-branch page writes");
        store
            .create_branch(&child, &parent, Lsn(20))
            .expect("branch creates at fork LSN");
        store
            .put_image(&parent, diverged_key, Lsn(30), page(4))
            .await
            .expect("parent side diverges");
        store
            .put_image(&child, diverged_key, Lsn(30), child_diverged.clone())
            .await
            .expect("child side diverges");
    }

    let child_below_fork = service
        .get_page(GetPageRequest {
            timeline: child.clone(),
            key: diverged_key,
            lsn: Lsn(20),
        })
        .await
        .expect("child inherits below fork")
        .page;
    let child_after_fork = service
        .get_page(GetPageRequest {
            timeline: child.clone(),
            key: diverged_key,
            lsn: Lsn(30),
        })
        .await
        .expect("child reconstructs after fork")
        .page;
    let backup = service
        .basebackup_metadata(BasebackupMetadataRequest {
            timeline: child,
            lsn: Lsn(30),
        })
        .await
        .expect("basebackup at branch builds");

    assert!(child_below_fork == parent_diverged);
    assert!(child_after_fork == child_diverged);
    assert!(
        backup
            .tar
            .windows(PAGE_SIZE)
            .any(|window| window == parent_base)
    );
    assert!(
        backup
            .tar
            .windows(PAGE_SIZE)
            .any(|window| window == child_diverged)
    );
}

#[test]
fn pg17_manifest_declares_promote_and_diverge_fork_oracle_shape() {
    assert!(
        manifest_value("pg6_fork_capture")
            == Some("required-external-promote-and-diverge-workflow")
    );
    assert!(manifest_value("pg_major") == Some("17"));
    match manifest_value("corpus").expect("manifest declares a corpus") {
        "real-pg17-standby-oracle" => assert_real_pg17_manifest_shape(),
        corpus if corpus.contains("synthetic") => assert_synthetic_manifest_shape(),
        corpus => panic!("unexpected pg17 fixture corpus {corpus}"),
    }
}

#[test]
#[ignore = "requires CRABKA_FORKED_WAL_FIXTURE_DIR captured from a promoted PostgreSQL 17 standby"]
fn external_pg17_forked_wal_divergence_gate_is_explicit() {
    let Some(fixture_dir) = std::env::var_os("CRABKA_FORKED_WAL_FIXTURE_DIR") else {
        return;
    };
    let fixture_root = std::path::PathBuf::from(fixture_dir);

    for required_oracle in REQUIRED_PG6_FORK_ORACLES {
        let oracle_path = fixture_root.join(required_oracle);
        assert!(
            oracle_path.exists(),
            "missing required external PG17 fork fixture oracle: {}",
            oracle_path.display()
        );
    }
}

fn manifest_value(key: &str) -> Option<&str> {
    PG17_FIXTURE_MANIFEST.lines().find_map(|line| {
        let (line_key, line_value) = line.split_once(" = ")?;
        if line_key != key {
            return None;
        }

        Some(line_value.trim_matches('"'))
    })
}

fn assert_manifest_array_contains_all(key: &str, expected_values: &[&str]) {
    let actual_values = manifest_array(key).expect("manifest array key is present");

    for expected_value in expected_values {
        assert!(actual_values.contains(expected_value));
    }
}

fn assert_real_pg17_manifest_shape() {
    assert!(manifest_value("provenance") == Some("real-postgresql-17"));
    assert_manifest_array_contains_all("required_oracles", REQUIRED_REAL_PG17_ORACLES);
    assert_manifest_array_contains_all("required_fork_oracles", REQUIRED_PG6_FORK_ORACLES);
    assert!(manifest_array("missing_plan_oracles").is_none());
}

fn assert_synthetic_manifest_shape() {
    assert_manifest_array_contains_all("missing_plan_oracles", REQUIRED_PG6_FORK_ORACLES);
}

fn manifest_array(key: &str) -> Option<Vec<&str>> {
    PG17_FIXTURE_MANIFEST.lines().find_map(|line| {
        let (line_key, line_value) = line.split_once(" = ")?;
        if line_key != key {
            return None;
        }

        parse_toml_string_array(line_value)
    })
}

fn parse_toml_string_array(value: &str) -> Option<Vec<&str>> {
    let inner = value.trim().strip_prefix('[')?.strip_suffix(']')?;
    if inner.trim().is_empty() {
        return Some(Vec::new());
    }

    inner
        .split(',')
        .map(|entry| entry.trim().strip_prefix('"')?.strip_suffix('"'))
        .collect()
}
