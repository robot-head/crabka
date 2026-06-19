# Task 3 Report: Header-name Constants

## Fix: header-name constants

### Commands and Results

1. **Tests**: `cargo test -p crabka-audit --lib`
   - Result: **17 passed; 0 failed**
   - All chain-header tests and related audit tests pass

2. **Formatting**: `cargo +nightly fmt -p crabka-audit`
   - Result: **Clean** (no errors, exports reordered alphabetically)

3. **Clippy**: `cargo clippy -p crabka-audit --tests -- -D warnings`
   - Result: **Clean** (no warnings)

4. **Commit**: `git commit -m "refactor(audit): name chain-header keys as shared constants"`
   - SHA: `b7b46e03`
   - Subject: `refactor(audit): name chain-header keys as shared constants`
   - Changes:
     - Added `HEADER_SEQ` and `HEADER_PREV_HASH` constants to `sink.rs`
     - Updated `push_chain_headers` to use constants instead of string literals
     - Re-exported constants from `lib.rs`
