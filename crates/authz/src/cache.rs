//! Gateway-side ACL snapshot: a flat `Vec<AclEntry>` (from `describe_acls`)
//! that implements [`AclSource`] with EXACTLY the broker's matching semantics.

use crabka_metadata::{AclEntry, PatternType, ResourceType};

use crate::AclSource;

/// Immutable ACL snapshot; rebuilt wholesale on each refresh.
#[derive(Debug, Clone, Default)]
pub struct AclCache {
    entries: Vec<AclEntry>,
}

impl AclCache {
    #[must_use]
    pub fn new(entries: Vec<AclEntry>) -> Self {
        Self { entries }
    }
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl AclSource for AclCache {
    fn matching_acls<'a>(
        &'a self,
        rt: ResourceType,
        name: &'a str,
    ) -> Box<dyn Iterator<Item = &'a AclEntry> + 'a> {
        // MUST mirror MetadataImage::matching_acls: same resource_type, and
        // (LITERAL == name) || (LITERAL == "*") || (PREFIXED && name.starts_with(resource_name)).
        Box::new(self.entries.iter().filter(move |e| {
            e.resource_type == rt
                && match e.pattern_type {
                    PatternType::Literal => e.resource_name == name || e.resource_name == "*",
                    PatternType::Prefixed => name.starts_with(e.resource_name.as_str()),
                }
        }))
    }
}

#[cfg(test)]
mod tests {

    use crabka_metadata::{
        AclOperation, MetadataImage, MetadataRecord, PermissionType, ResourceType,
    };
    use uuid::Uuid;

    use super::*;

    fn entry(rt: ResourceType, pattern: PatternType, name: &str, op: AclOperation) -> AclEntry {
        AclEntry {
            resource_type: rt,
            resource_name: name.into(),
            pattern_type: pattern,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: op,
            permission_type: PermissionType::Allow,
        }
    }

    /// A stable, comparable identity for an `AclEntry` so the two sources'
    /// `matching_acls` outputs can be compared as sets regardless of order.
    /// (The ACL enums derive `Debug` but not `Ord`, so we key on the debug
    /// rendering of the identifying fields — a `String`, which is `Ord`.)
    fn key(e: &AclEntry) -> String {
        format!(
            "{:?}|{:?}|{}|{:?}",
            e.resource_type, e.pattern_type, e.resource_name, e.operation
        )
    }

    fn sorted_keys<'a>(it: Box<dyn Iterator<Item = &'a AclEntry> + 'a>) -> Vec<String> {
        let mut v: Vec<_> = it.map(key).collect();
        v.sort();
        v
    }

    /// Cross-validation guard: the same `AclEntry` set, built into BOTH a
    /// `MetadataImage` (via `apply`) and an `AclCache`, must yield the SAME
    /// matching set for every probe. This protects against drift between the
    /// broker's image matching and the gateway cache's reimplementation.
    #[test]
    fn cache_matches_image_for_every_probe() {
        // A representative ACL set: literal exact, the literal "*" wildcard,
        // a prefixed entry, an unrelated topic, and a different resource type.
        let entries = vec![
            entry(
                ResourceType::Topic,
                PatternType::Literal,
                "foo",
                AclOperation::Read,
            ),
            entry(
                ResourceType::Topic,
                PatternType::Literal,
                "*",
                AclOperation::Write,
            ),
            entry(
                ResourceType::Topic,
                PatternType::Prefixed,
                "team-",
                AclOperation::Read,
            ),
            entry(
                ResourceType::Topic,
                PatternType::Literal,
                "bar",
                AclOperation::Read,
            ),
            entry(
                ResourceType::Group,
                PatternType::Literal,
                "cg-1",
                AclOperation::Read,
            ),
            entry(
                ResourceType::Group,
                PatternType::Prefixed,
                "app-",
                AclOperation::Read,
            ),
        ];

        // Build the same set into a MetadataImage (broker side) ...
        let mut image = MetadataImage::new(Uuid::nil());
        for e in &entries {
            image.apply(&MetadataRecord::V1AccessControlEntry(e.clone()));
        }
        // ... and into an AclCache (gateway side).
        let cache = AclCache::new(entries.clone());

        // Probes covering every matching code path.
        let probes: &[(ResourceType, &str)] = &[
            (ResourceType::Topic, "foo"),      // literal exact hit (+ "*" wildcard)
            (ResourceType::Topic, "*"),        // querying the wildcard resource itself
            (ResourceType::Topic, "team-foo"), // prefixed hit (+ "*" wildcard)
            (ResourceType::Topic, "team-"),    // prefix boundary (starts_with itself)
            (ResourceType::Topic, "nomatch"),  // only the "*" wildcard matches
            (ResourceType::Group, "cg-1"),     // literal hit, no wildcard for Group
            (ResourceType::Group, "app-svc"),  // prefixed hit on Group
            (ResourceType::Group, "other"),    // prefixed miss, no wildcard → empty
            (ResourceType::Cluster, "kafka-cluster"), // wrong type → empty in both
        ];

        for &(rt, name) in probes {
            let from_image = sorted_keys(AclSource::matching_acls(&image, rt, name));
            let from_cache = sorted_keys(AclSource::matching_acls(&cache, rt, name));
            assert2::assert!(from_image == from_cache);
        }
    }

    #[test]
    fn len_and_is_empty_track_entries() {
        let empty = AclCache::default();
        assert2::assert!(empty.is_empty());
        assert2::assert!(empty.len() == 0);

        let cache = AclCache::new(vec![entry(
            ResourceType::Topic,
            PatternType::Literal,
            "foo",
            AclOperation::Read,
        )]);
        assert2::assert!(!cache.is_empty());
        assert2::assert!(cache.len() == 1);
    }
}
