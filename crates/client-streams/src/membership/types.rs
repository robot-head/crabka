//! Public value types surfaced by [`StreamsMembership`](super::StreamsMembership).

/// A concrete topic-partition a task consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

/// One assigned task and the source topic-partitions it processes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskAssignment {
    pub subtopology_id: String,
    pub partitions: Vec<i32>,
    pub source_topic_partitions: Vec<TopicPartition>,
}

/// The active tasks, the standby tasks, and the warmup tasks assigned to this
/// member.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamsAssignment {
    pub active: Vec<TaskAssignment>,
    pub standby: Vec<TaskAssignment>,
    pub warmup: Vec<TaskAssignment>,
}

/// A tracker to share task restored and end offsets with the coordinator loop.
#[derive(Debug, Clone, Default)]
pub struct TaskOffsetTracker {
    pub task_offsets: std::collections::HashMap<(String, i32), i64>,
    pub task_end_offsets: std::collections::HashMap<(String, i32), i64>,
}

/// A non-ready status that the coordinator reports, using the KIP-1071 status
/// codes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamsStatus {
    StaleTopology(String),
    MissingSourceTopics(String),
    IncorrectlyPartitionedTopics(String),
    MissingInternalTopics(String),
    ShutdownApplication,
    AssignmentDelayed(String),
    Unknown(i8, String),
}

/// An event emitted by the membership heartbeat loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamsEvent {
    /// A new assignment was adopted.
    Assigned(StreamsAssignment),
    /// The group is not ready, for example because a source topic or an
    /// internal topic is missing.
    NotReady(Vec<StreamsStatus>),
    /// The broker fenced this member and the client rejoined automatically. A
    /// fresh assignment follows.
    Fenced,
}
