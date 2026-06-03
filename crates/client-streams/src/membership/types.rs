//! Membership types: stub placeholders for later implementation.

/// A topic-partition pair.
pub struct TopicPartition;

/// A single task assignment (subtopology + partition).
pub struct TaskAssignment;

/// The full assignment for a member (active, standby, warmup tasks).
pub struct StreamsAssignment;

/// Current membership status of the streams client.
pub enum StreamsStatus {
    /// Connecting to or waiting for the group coordinator.
    Joining,
    /// Stable group membership with a valid assignment.
    Stable,
    /// The client is shutting down.
    Closed,
}

/// Events delivered to the application from the streams membership loop.
pub enum StreamsEvent {
    /// A new assignment has been computed and is ready to use.
    Assigned(StreamsAssignment),
    /// The assignment has been revoked (rebalance in progress).
    Revoked,
    /// The membership loop has shut down.
    Closed,
}
