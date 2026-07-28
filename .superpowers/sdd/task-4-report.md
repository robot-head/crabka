## Task 4 report

Status: DONE

### Coverage audit

- Authoritative broker runtime rows: 103
- Rows present on direct CLI/`CRABKA_*` surface: 103
- Rows present in `[runtime]` TOML and merge path: 103
- Missing rows: 0
- Added consumed component-owned Share/Streams inputs without exposing staged
  Streams `enable`, `max_groups`, or `max_size`.

### RED

- `cargo test -p crabka-broker --bin crabka-broker runtime_policy_cli_rejects_invalid_and_accepts_valid_values -- --nocapture`
  - Failed because `--cleaner-interval-ms=1` was unknown.
- `cargo test -p crabka-broker runtime_file_config_ -- --nocapture`
  - Representative `[runtime]` values remained defaults.
  - Zero `cleaner_interval_ms` was silently ignored.
- `cargo test -p crabka-broker existing_file_inputs_reject_invalid_refined_values -- --nocapture`
  - Zero legacy top-level heartbeat/delegation values were accepted.

### GREEN

- `cargo test -p crabka-broker --bin crabka-broker runtime_policy_cli`
  - 2 passed, 0 failed, including a real `CRABKA_CLEANER_INTERVAL_MS` override.
- `cargo test -p crabka-broker runtime_file_config_`
  - 3 passed, 0 failed.
- `cargo test -p crabka-broker existing_file_inputs_reject_invalid_refined_values`
  - 1 passed, 0 failed.
- `cargo test -p crabka-broker file_config`
  - 86 passed, 0 failed.

### Focused gates

- `cargo clippy -p crabka-broker --lib --bin crabka-broker --all-features -- -D warnings`
  - Passed.
- `cargo +nightly fmt --all -- --check`
  - Passed.
- `git diff --check -- crates/broker/src/bin/broker.rs crates/broker/src/file_config.rs`
  - Passed.

### Concerns

- Task 1 has no refined positive `u32`/`i16` or nonnegative scalar type.
  Those few CLI shapes use Clap's native inclusive range parser; compatible
  millisecond, signed integer, count, byte-size, and percentage inputs use the
  Task 1 refined types.

## Review fix: explicit-source precedence and ordinary application methods

### RED

- `cargo test -p crabka-broker --bin crabka-broker explicit_ -- --nocapture`
  - Failed to compile with `E0599`: `RuntimeArgs` had no `apply_to` overlay
    boundary.
  - Both new behavior tests were blocked at that missing boundary:
    `explicit_cli_default_runtime_values_override_file` and
    `explicit_env_default_runtime_values_override_file`.
  - The tests explicitly supply the documented defaults (`30000` cleaner
    interval and `20000` controlled-shutdown timeout) over different TOML
    values (`7000` and `9000`).

### GREEN

- `cargo test -p crabka-broker --bin crabka-broker explicit_ -- --nocapture`
  - 2 passed, 0 failed.
- `cargo test -p crabka-broker --bin crabka-broker runtime_policy_cli -- --nocapture`
  - 2 passed, 0 failed.
- `cargo test -p crabka-broker runtime_file_config_ -- --nocapture`
  - 3 passed, 0 failed.
- `cargo test -p crabka-broker file_config -- --nocapture`
  - Passed.

### Refactor and precedence

- All 103 direct runtime inputs now use `Option` presence semantics; Clap
  defaults no longer erase whether CLI or `CRABKA_*` explicitly supplied a
  value.
- Resolution order is `BrokerConfig::default()` → TOML → explicit CLI/env →
  `BrokerConfig::validate()`.
- Controlled-shutdown timeout follows the same source order.
- Deleted `build_broker_config!` and `apply_runtime_fields!`.
- Ordinary Rust methods now split base construction, direct-input conversion,
  and file application by logical runtime component. Small setter macros only
  remove repeated scalar validation assignments.

### Final gates

- `cargo check -p crabka-broker --bin crabka-broker`
  - Passed.
- `cargo clippy -p crabka-broker --lib --bin crabka-broker --all-features -- -D warnings`
  - First run found `clippy::assigning_clones`; fixed with `clone_from`.
  - Rerun passed.
- `cargo +nightly fmt --all -- --check`
  - Passed.
- `git diff --check -- crates/broker/src/bin/broker.rs crates/broker/src/file_config.rs`
  - Passed.
- Broker Runtime Field Table audit:
  - 103 authoritative rows.
  - 103 direct CLI/environment inputs.
  - 103 `[runtime]` TOML fields and application paths.
  - 0 missing rows.
