//! Transaction subsystem for the Crabka broker.
//!
//! See the design at
//! `docs/superpowers/specs/2026-05-12-crabka-transactions-design.md`.

pub(crate) mod bootstrap;
pub(crate) mod coordinator;
pub(crate) mod handlers;
pub(crate) mod marker;
pub(crate) mod partitioner;
pub(crate) mod state;
