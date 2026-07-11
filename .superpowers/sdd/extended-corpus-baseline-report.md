# Extended corpus baseline discovery report

## Status

Fixed the F-0 discovery failure by reserving `baseline.json` as extended-corpus
metadata while retaining recursive discovery and strict parsing for every other
JSON file. The 6/6 baseline and corpus cases are unchanged.

## TDD evidence

- RED: `cargo test -p crabka-gres-conformance loads_extended_cases_without_parsing_baseline_metadata -- --nocapture` failed with `invalid type: map, expected a sequence`.
- GREEN: the same focused test passed after excluding files named exactly
  `baseline.json` from extended case discovery.
- Guard: `rejects_malformed_non_baseline_extended_case_file` passes and proves
  arbitrary malformed JSON is not silently ignored.

## Verification

- `cargo test -p crabka-gres-conformance`: 22 passed.
- `cargo check -p crabka-gres-conformance --all-targets`: passed.
- `cargo clippy -p crabka-gres-conformance --all-targets -- -D warnings`: passed.
- `cargo +nightly fmt --all -- --check`: passed.
- `git diff --check`: passed.
- Rebuilt the skip-build E2E binaries with
  `cargo build --locked -p crabka-cli -p crabka-broker -p crabka-gres -p crabka-gres-conformance`.

## Live E2E

Ran the complete gate with `target/gres-driver-venv/bin` first in `PATH`,
`CRABKA_GRES_SKIP_BUILD=1`, and `CRABKA_GRES_E2E_KEEP_ARTIFACTS=1`.

Discovery succeeded and all six extended cases executed. The live gate then
failed downstream at 4/6 extended parity versus the unchanged 6/6 baseline:
`where_int4_parameter` and `insert_parameterized_values_returning` returned
subject SQLSTATE `42P01` while the oracle succeeded. Artifacts are retained in
`target/gres-e2e-artifacts/`. This semantic failure is outside the bounded
baseline-discovery fix.

## Self-review

The exclusion is limited to the exact reserved basename `baseline.json`, at any
recursive level. Other JSON files remain discoverable and parse failures remain
visible. No corpus or baseline data changed.

## Review follow-up

Wrapped extended-case file reads and JSON deserialization in typed errors that
include the offending `path.display()` while retaining the original error as
the source. Strict TDD reproduced the missing-path assertion before the change;
the strengthened malformed-file test now asserts both the serde diagnostic and
the complete filename/path. The focused test and the full 22-test conformance
suite pass, along with check, clippy, nightly format, and diff checks.
