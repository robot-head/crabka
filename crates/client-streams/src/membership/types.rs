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

/// The active/standby/warmup tasks assigned to this member.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamsAssignment {
    pub active: Vec<TaskAssignment>,
    pub standby: Vec<TaskAssignment>,
    pub warmup: Vec<TaskAssignment>,
}

/// A non-ready status reported by the coordinator (KIP-1071 status codes).
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
    /// The group is not ready (e.g. missing source/internal topics).
    NotReady(Vec<StreamsStatus>),
    /// We were fenced and auto-rejoined; a fresh assignment will follow.
    Fenced,
}
