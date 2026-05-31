//! The replica's volatile role within the current epoch and its per-role state.
//! Minimal scaffold; full role set lands in Task 2.

/// The replica's volatile role within the current epoch. Expanded in Task 2.
#[derive(Debug, Clone, Default)]
pub enum Role {
    #[default]
    Unattached,
}
