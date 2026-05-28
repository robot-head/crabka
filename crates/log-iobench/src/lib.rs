//! Bench-only support crate.
//!
//! This crate exists only to host `benches/mmap_read.rs`, which needs the
//! `unsafe` `memmap2::Mmap::map` call that the workspace-wide
//! `unsafe_code = "forbid"` policy disallows in the main crates. It opts out
//! of the workspace lint set (see `Cargo.toml`) so the mmap path can be
//! measured without weakening the policy elsewhere.
