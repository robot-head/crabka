//! Build-time code generator for the `crabka-protocol` crate.
//!
//! The generator reads the vendored Apache Kafka JSON message schemas,
//! validates the subset Crabka supports, resolves nested and common structs,
//! and emits the owned and borrowed Rust protocol modules.
//! `crates/protocol/build.rs` uses the binary wrapper. The library API is
//! useful for tests and for one-off schema audits.
//!
//! ## Loading and validating schemas
//!
//! ```no_run
//! use std::path::Path;
//!
//! use crabka_protocol_codegen::{ir, validate};
//!
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let specs = ir::load_dir(Path::new("crates/protocol/schemas"))?;
//! validate::validate(&specs)?;
//! println!("loaded {} protocol schemas", specs.len());
//! # Ok(())
//! # }
//! ```
//!
//! ## Resolving generated type paths
//!
//! ```no_run
//! use std::path::Path;
//!
//! use crabka_protocol_codegen::{ir, resolve};
//!
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let specs = ir::load_dir(Path::new("crates/protocol/schemas"))?;
//! let metadata = specs.iter().find(|s| s.name == "MetadataRequest").unwrap();
//! let resolution = resolve::resolve_message(metadata)?;
//! println!("{} referenced struct types", resolution.len());
//! # Ok(())
//! # }
//! ```
pub mod emit;
pub mod fmt;
pub mod ir;
pub mod name_conv;
pub mod resolve;
pub mod type_map;
pub mod validate;
