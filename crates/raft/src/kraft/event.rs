//! Inputs to the consensus state machine. Minimal scaffold; full event set
//! lands in Task 2.

/// Inputs to the consensus state machine. Expanded in Task 2.
#[derive(Debug, Clone)]
pub enum Event {
    /// The election timer fired.
    ElectionTimeout,
}
