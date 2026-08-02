//! Every checked-in benchmark scenario loads through the driver's own loader.
//!
//! `bench/scenarios/*.yaml` is an operator-written corpus that lives outside
//! this crate, so nothing linked it to the `Scenario` type: when the dimensioned
//! fields grew units, all twelve files silently stopped parsing and no test
//! noticed. This harness closes that gap by running each file through the same
//! `serde_yaml` path `main.rs` uses, one nextest process per scenario, so a
//! broken file names itself.

use std::path::Path;

use assert2::{assert, check};
use crabka_bench_driver::scenario::{LoadMode, Scenario};
use crabka_units::prelude::*;

fn corpus_root() -> String {
    let root = std::env::var_os("TEST_SRCDIR")
        .map(|root| std::path::PathBuf::from(root).join("_main/bench/scenarios"))
        .unwrap_or_else(|| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/scenarios")
        });
    std::fs::canonicalize(root.join("high-partition-latency.yaml"))
        .unwrap()
        .parent()
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

/// Loads one scenario file and checks it describes a runnable benchmark.
fn scenario_file(path: &Path) -> datatest_stable::Result<()> {
    let yaml = std::fs::read_to_string(path)?;
    let scenario: Scenario = serde_yaml::from_str(&yaml)
        .map_err(|error| format!("{} does not load: {error}", path.display()))?;

    check!(!scenario.name.is_empty());
    // A zero-length measurement window or a zero-byte record would run but
    // measure nothing, so these are load-bearing rather than decorative.
    check!(scenario.duration > Time::ZERO);
    check!(scenario.msg_size > ByteSize::ZERO);
    check!(scenario.batch_size > ByteSize::ZERO);
    check!(scenario.partitions > 0);
    check!(scenario.producers > 0);

    if let LoadMode::FixedRate { rate } = scenario.mode {
        check!(rate > Frequency::ZERO);
    }

    // A kill scheduled at or after the end of the run would never fire.
    if let Some(failover) = &scenario.failover {
        check!(failover.kill_after < scenario.duration + scenario.warmup);
    }

    // The quantities survive a round trip through the operator-facing encoding,
    // so a scenario echoed into a report reads back as the same benchmark.
    let reencoded = serde_yaml::to_string(&scenario)?;
    let reparsed: Scenario = serde_yaml::from_str(&reencoded)?;
    assert!(reparsed == scenario);

    Ok(())
}

datatest_stable::harness! {
    { test = scenario_file, root = corpus_root(), pattern = r".*\.yaml$" },
}
