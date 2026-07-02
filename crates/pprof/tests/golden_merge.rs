use std::sync::Arc;

use assert2::{assert, check};
use crabka_pprof::{
    EngineOpts, FlameEngine, FlameGraph, FunctionRec, InMemoryProfileStore, Level, LineRec,
    LocationRec, ProfileType,
};

const TENANT: &str = "tenant-a";
const CPU_PROFILE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const MEMORY_PROFILE: &str = "memory:alloc_space:bytes:space:bytes";

struct Fixture {
    engine: FlameEngine<InMemoryProfileStore>,
}

fn fixture() -> Fixture {
    let mut store = InMemoryProfileStore::new();
    let (alloc_stack, other_stack) = {
        let db = store.symbols_mut();
        let main = intern_location(db, "main", &["main"]);
        let work = intern_location(db, "work", &["work"]);
        let alloc_inline = intern_location(db, "alloc_inline", &["alloc", "inline_helper"]);
        let other = intern_location(db, "other", &["other"]);
        (
            db.intern_stacktrace(0, &[alloc_inline, work, main]),
            db.intern_stacktrace(0, &[other, main]),
        )
    };

    push(&mut store, CPU_PROFILE, "api", alloc_stack, 10, 100);
    push(&mut store, CPU_PROFILE, "api", alloc_stack, 7, 110);
    push(&mut store, CPU_PROFILE, "worker", other_stack, 4, 120);
    push(&mut store, MEMORY_PROFILE, "api", alloc_stack, 99, 130);

    Fixture {
        engine: FlameEngine::new(Arc::new(store), EngineOpts::default()),
    }
}

fn intern_location(db: &mut crabka_pprof::SymbolDb, file: &str, function_names: &[&str]) -> u32 {
    let lines = function_names
        .iter()
        .enumerate()
        .map(|(idx, name)| {
            let name_ref = db.intern_string(name);
            let filename_ref = db.intern_string(&format!("{file}.go"));
            let function_id = db.intern_function(FunctionRec {
                name: name_ref,
                system_name: name_ref,
                filename: filename_ref,
                start_line: i64::try_from(idx + 1).expect("test index fits i64"),
            });
            LineRec {
                function_id,
                line: i32::try_from(idx + 1).expect("test index fits i32"),
            }
        })
        .collect();
    db.intern_location(LocationRec {
        address: 0,
        mapping_id: 0,
        lines,
    })
}

fn push(
    store: &mut InMemoryProfileStore,
    profile_type: &str,
    service: &str,
    stacktrace_id: u32,
    value: i64,
    timestamp_ms: i64,
) {
    store.push_sample(
        TENANT,
        profile_type,
        vec![("service".to_string(), service.to_string())],
        0,
        stacktrace_id,
        value,
        timestamp_ms,
    );
}

async fn merge(label_selector: &str, max_nodes: i64) -> FlameGraph {
    fixture()
        .engine
        .select_merge_stacktraces(TENANT, CPU_PROFILE, label_selector, 0, 1_000, max_nodes)
        .await
        .unwrap()
}

#[tokio::test]
async fn full_merge_pins_four_int_levels_and_fold_before_symbolize() {
    let fg = merge("{}", 2_048).await;

    assert!(
        fg == FlameGraph {
            names: ["total", "main", "work", "inline_helper", "alloc", "other"]
                .map(String::from)
                .to_vec(),
            levels: vec![
                Level {
                    values: vec![0, 21, 0, 0],
                },
                Level {
                    values: vec![0, 21, 0, 1],
                },
                Level {
                    values: vec![0, 4, 4, 5, 0, 17, 0, 2],
                },
                Level {
                    values: vec![4, 17, 0, 3],
                },
                Level {
                    values: vec![4, 17, 17, 4],
                },
            ],
            total: 21,
            max_self: 17,
        }
    );
}

#[tokio::test]
async fn label_selector_filters_series_before_merge() {
    let fg = merge(r#"{service="api"}"#, 2_048).await;

    check!(fg.total == 17);
    check!(fg.names == vec!["total", "main", "work", "inline_helper", "alloc"]);
    check!(fg.levels[2].values == vec![0, 17, 0, 2]);
    check!(!fg.names.iter().any(|name| name == "other"));
}

#[tokio::test]
async fn max_nodes_truncates_to_synthetic_other_and_conserves_total() {
    let fg = merge("{}", 3).await;

    check!(fg.names == vec!["total", "main", "other", "work"]);
    check!(fg.total == 21);
    check!(fg.max_self == 17);
    check!(fg.levels[0].values == vec![0, 21, 0, 0]);
    check!(fg.levels[1].values == vec![0, 21, 0, 1]);
    check!(fg.levels[2].values == vec![4, 17, 0, 3, -4, 4, 4, 2]);
    check!(fg.levels[3].values == vec![4, 17, 17, 2]);
}

#[test]
fn profile_type_round_trips_fixture_type() {
    let profile_type = ProfileType::parse(CPU_PROFILE).unwrap();

    assert!(profile_type.to_string() == CPU_PROFILE);
}
