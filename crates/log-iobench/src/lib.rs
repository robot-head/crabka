//! Bench-only support crate.
//!
//! This crate hosts `benches/mmap_read.rs` only. That bench needs the `unsafe`
//! `memmap2::Mmap::map` call, and the workspace-wide `unsafe_code = "forbid"`
//! policy disallows that call in the main crates. This crate opts out of the
//! workspace lint set, as `Cargo.toml` shows, so the bench can measure the mmap
//! path without a weaker policy elsewhere.
