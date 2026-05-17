//! Execute-path state machine. `Executor` runs one `Execution` at a time
//! against the cluster via `crabka_client_core::Client`.
//!
//! Slice 43b adds the full state machine (`ApplyThrottle` → `Submit` →
//! `Wait` → `ClearThrottle`) and on-disk persistence with restart resume.
//! The file is intentionally split across `phases`, `state`, and
//! `throttle` so each piece is independently testable.

pub mod throttle;
