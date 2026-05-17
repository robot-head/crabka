//! `crabka-rebalancer` — Cruise-Control-equivalent partition rebalancer.
//!
//! Slice 43a ships the advisor surface: connects to a cluster as an
//! admin client, snapshots state, exposes a Connect-RPC service for
//! `GetState` / `CreateProposal` / `DryRunProposal` (and a stub
//! `ExecuteProposal`).  Slice 43b wires execute.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    tracing::info!("crabka-rebalancer slice-43a scaffold — no service wired yet");
    Ok(())
}
