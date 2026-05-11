//! In-memory metadata image. No persistence — `Broker::start` reconstructs
//! the image from the `<log_dir>/<topic>-<partition>/` directory layout
//! at startup.

// Some fields / methods (e.g. `TopicMeta::topic_id`, `MetadataImage::topics`)
// are only consumed by handlers landing in the same batch as the Metadata
// handler. Keep this allow until those handlers exist.
#![allow(dead_code)]

use std::collections::HashMap;

use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct TopicMeta {
    pub topic_id: Uuid,
    pub partitions: Vec<PartitionMeta>,
}

#[derive(Debug, Clone)]
pub struct PartitionMeta {
    pub partition_id: i32,
    pub leader_broker_id: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

#[derive(Debug, Default)]
pub struct MetadataImage {
    topics: HashMap<String, TopicMeta>,
}

impl MetadataImage {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a new topic. Returns `false` if the topic already exists
    /// (caller should map to `TOPIC_ALREADY_EXISTS` = 36).
    pub fn insert_topic(
        &mut self,
        name: impl Into<String>,
        partition_count: i32,
        broker_id: i32,
    ) -> bool {
        let name = name.into();
        if self.topics.contains_key(&name) {
            return false;
        }
        let partitions = (0..partition_count)
            .map(|i| PartitionMeta {
                partition_id: i,
                leader_broker_id: broker_id,
                replicas: vec![broker_id],
                isr: vec![broker_id],
            })
            .collect();
        self.topics.insert(
            name,
            TopicMeta {
                topic_id: Uuid::new_v4(),
                partitions,
            },
        );
        true
    }

    /// Remove a topic. Returns `true` if it existed.
    pub fn remove_topic(&mut self, name: &str) -> bool {
        self.topics.remove(name).is_some()
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&TopicMeta> {
        self.topics.get(name)
    }

    #[must_use]
    pub fn topic_names(&self) -> Vec<String> {
        self.topics.keys().cloned().collect()
    }

    pub fn topics(&self) -> impl Iterator<Item = (&str, &TopicMeta)> + '_ {
        self.topics.iter().map(|(k, v)| (k.as_str(), v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_then_get() {
        let mut m = MetadataImage::new();
        assert!(m.insert_topic("foo", 3, 1));
        let t = m.get("foo").expect("foo present");
        assert_eq!(t.partitions.len(), 3);
        assert_eq!(t.partitions[0].leader_broker_id, 1);
        assert_eq!(t.partitions[0].replicas, vec![1]);
        assert_eq!(t.partitions[0].isr, vec![1]);
    }

    #[test]
    fn duplicate_insert_returns_false() {
        let mut m = MetadataImage::new();
        assert!(m.insert_topic("foo", 1, 1));
        assert!(!m.insert_topic("foo", 2, 1));
    }

    #[test]
    fn remove_then_missing() {
        let mut m = MetadataImage::new();
        m.insert_topic("foo", 1, 1);
        assert!(m.remove_topic("foo"));
        assert!(!m.remove_topic("foo"));
        assert!(m.get("foo").is_none());
    }
}
