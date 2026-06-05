//! In-memory authoritative registry state, rebuilt by replaying `_schemas`.
//! Pure data structure: no I/O. The `KafkaStore` wraps it behind a lock and the
//! write-serialisation gate (see kafkastore/mod.rs). Cloneable so the write path
//! can decide id/version on a throwaway copy (the reader is the sole mutator of
//! the live instance).

use std::collections::BTreeMap;

use crate::error::SrError;
use crate::format::{self, SchemaType};
use crate::kafkastore::record::{SchemaKey, SchemaValue};

/// Result of a registration: the global id and the per-subject version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Registered {
    pub id: i32,
    pub version: i32,
}

#[derive(Debug, Clone)]
struct VersionEntry {
    version: i32,
    id: i32,
}

#[derive(Debug, Default, Clone)]
pub struct StoreState {
    subjects: BTreeMap<String, Vec<VersionEntry>>,
    by_id: BTreeMap<i32, (SchemaType, String)>,
    by_canonical: BTreeMap<String, i32>,
    global_compat: Option<String>,
    subject_compat: BTreeMap<String, String>,
    max_id: i32,
}

impl StoreState {
    /// Decide id/version for a registration AND apply it locally. Validates the
    /// schema (NONE compat still rejects unparseable schemas -> `InvalidSchema`).
    /// `id` is global (keyed by canonical form); `version` is per-subject.
    ///
    /// The `schema` string is assumed to be already in normalised storage form
    /// (see `format::normalized_storage_form`); the caller (e.g. `KafkaStore`)
    /// is responsible for normalising before passing it in.
    pub fn register(
        &mut self,
        subject: &str,
        ty: SchemaType,
        schema: &str,
    ) -> Result<Registered, SrError> {
        let canonical = format::parse(ty, schema)?.canonical_form();
        // Idempotent within subject?
        if let Some(existing) = self.find_under_subject_canonical(subject, &canonical) {
            return Ok(existing);
        }
        // Global id: reuse if canonical seen anywhere, else next.
        let id = if let Some(&id) = self.by_canonical.get(&canonical) {
            id
        } else {
            let id = self.max_id + 1;
            self.max_id = id;
            self.by_canonical.insert(canonical, id);
            self.by_id.insert(id, (ty, schema.to_string()));
            id
        };
        let next_version = self
            .subjects
            .get(subject)
            .map_or(1, |v| i32::try_from(v.len()).unwrap_or(i32::MAX) + 1);
        self.subjects
            .entry(subject.to_string())
            .or_default()
            .push(VersionEntry {
                version: next_version,
                id,
            });
        Ok(Registered {
            id,
            version: next_version,
        })
    }

    /// Fold a decoded SCHEMA record into state (reader replay). Idempotent.
    pub fn apply_schema(&mut self, _key: &SchemaKey, value: &SchemaValue) {
        if value.deleted {
            return;
        }
        let ty = SchemaType::from_wire(value.schema_type.as_deref());
        self.max_id = self.max_id.max(value.id);
        self.by_id
            .entry(value.id)
            .or_insert_with(|| (ty, value.schema.clone()));
        if let Ok(p) = format::parse(ty, &value.schema) {
            self.by_canonical
                .entry(p.canonical_form())
                .or_insert(value.id);
        }
        let entry = self.subjects.entry(value.subject.clone()).or_default();
        if !entry.iter().any(|v| v.version == value.version) {
            entry.push(VersionEntry {
                version: value.version,
                id: value.id,
            });
            entry.sort_by_key(|v| v.version);
        }
    }

    fn find_under_subject_canonical(&self, subject: &str, canonical: &str) -> Option<Registered> {
        let id = *self.by_canonical.get(canonical)?;
        let entry = self.subjects.get(subject)?.iter().find(|v| v.id == id)?;
        Some(Registered {
            id,
            version: entry.version,
        })
    }

    pub fn set_global_compat(&mut self, level: String) {
        self.global_compat = Some(level);
    }

    pub fn set_subject_compat(&mut self, subject: &str, level: String) {
        self.subject_compat.insert(subject.to_string(), level);
    }

    #[must_use]
    pub fn global_compat(&self) -> &str {
        self.global_compat.as_deref().unwrap_or("BACKWARD")
    }

    #[must_use]
    pub fn subject_compat(&self, subject: &str) -> Option<&str> {
        self.subject_compat.get(subject).map(String::as_str)
    }

    #[must_use]
    pub fn subjects(&self) -> Vec<String> {
        self.subjects.keys().cloned().collect()
    }

    #[must_use]
    pub fn versions(&self, subject: &str) -> Option<Vec<i32>> {
        self.subjects
            .get(subject)
            .map(|vs| vs.iter().map(|v| v.version).collect())
    }

    /// Returns `(id, version, schemaType, schema)` for a subject+version.
    /// `version = None` resolves to the latest version.
    /// The second element is the resolved concrete version number.
    #[must_use]
    pub fn version(
        &self,
        subject: &str,
        version: Option<i32>,
    ) -> Option<(i32, i32, SchemaType, String)> {
        let vs = self.subjects.get(subject)?;
        let entry = match version {
            Some(v) => vs.iter().find(|e| e.version == v)?,
            None => vs.last()?,
        };
        let (ty, schema) = self.by_id.get(&entry.id)?;
        Some((entry.id, entry.version, *ty, schema.clone()))
    }

    #[must_use]
    pub fn schema_by_id(&self, id: i32) -> Option<(SchemaType, String)> {
        self.by_id.get(&id).cloned()
    }

    /// A subject's versions as ordered `(type, schema)` pairs (ascending
    /// version). Empty if the subject is unknown. Used by the compatibility
    /// engine for transitive checks.
    #[must_use]
    pub fn versions_schemas(&self, subject: &str) -> Vec<(SchemaType, String)> {
        let Some(entries) = self.subjects.get(subject) else {
            return Vec::new();
        };
        entries
            .iter()
            .filter_map(|e| self.by_id.get(&e.id).cloned())
            .collect()
    }

    /// Lookup an already-registered schema under a subject (POST /subjects/{s}).
    #[must_use]
    pub fn find_under_subject(
        &self,
        subject: &str,
        ty: SchemaType,
        schema: &str,
    ) -> Option<Registered> {
        let canonical = format::parse(ty, schema).ok()?.canonical_form();
        self.find_under_subject_canonical(subject, &canonical)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::SchemaType;

    fn av(n: &str) -> String {
        format!("{{\"type\":\"record\",\"name\":\"{n}\",\"fields\":[]}}")
    }

    #[test]
    fn first_registration_gets_id_1_version_1() {
        let mut s = StoreState::default();
        let r = s.register("av-value", SchemaType::Avro, &av("A")).unwrap();
        assert_eq!((r.id, r.version), (1, 1));
    }

    #[test]
    fn identical_under_same_subject_is_idempotent() {
        let mut s = StoreState::default();
        let r1 = s.register("av-value", SchemaType::Avro, &av("A")).unwrap();
        let r2 = s.register("av-value", SchemaType::Avro, &av("A")).unwrap();
        assert_eq!(r1, r2);
        assert_eq!(s.versions("av-value").unwrap(), vec![1]);
    }

    #[test]
    fn same_schema_new_subject_reuses_global_id_fresh_version() {
        let mut s = StoreState::default();
        let r1 = s.register("av-value", SchemaType::Avro, &av("A")).unwrap();
        let r2 = s
            .register("other-value", SchemaType::Avro, &av("A"))
            .unwrap();
        assert_eq!(r1.id, r2.id);
        assert_eq!(r2.version, 1);
    }

    #[test]
    fn different_schema_increments_id_and_version() {
        let mut s = StoreState::default();
        let r1 = s.register("av-value", SchemaType::Avro, &av("A")).unwrap();
        let r2 = s.register("av-value", SchemaType::Avro, &av("B")).unwrap();
        assert_eq!(r2.id, r1.id + 1);
        assert_eq!(r2.version, 2);
        assert_eq!(s.versions("av-value").unwrap(), vec![1, 2]);
    }

    #[test]
    fn invalid_schema_rejected_even_under_none() {
        let mut s = StoreState::default();
        assert!(
            s.register("av-value", SchemaType::Avro, "{not avro}")
                .is_err()
        );
    }

    #[test]
    fn apply_schema_is_idempotent() {
        use crate::kafkastore::record::{SchemaKey, SchemaValue};
        let mut s = StoreState::default();
        let v = SchemaValue {
            subject: "av-value".into(),
            version: 1,
            id: 1,
            schema_type: None,
            references: vec![],
            schema: av("A"),
            deleted: false,
        };
        let k = SchemaKey::new("av-value", 1);
        s.apply_schema(&k, &v);
        s.apply_schema(&k, &v); // second apply is a no-op
        assert_eq!(s.versions("av-value").unwrap(), vec![1]);
        assert_eq!(s.schema_by_id(1).unwrap().1, av("A"));
        // a fresh register of the same schema is now idempotent against replayed state
        let r = s.register("av-value", SchemaType::Avro, &av("A")).unwrap();
        assert_eq!((r.id, r.version), (1, 1));
    }

    #[test]
    fn versions_schemas_returns_ordered_pairs() {
        let mut s = StoreState::default();
        s.register("av-value", SchemaType::Avro, &av("A")).unwrap();
        s.register("av-value", SchemaType::Avro, &av("B")).unwrap();
        let vs = s.versions_schemas("av-value");
        assert_eq!(vs.len(), 2);
        assert!(matches!(vs[0].0, SchemaType::Avro));
        assert!(vs[0].1.contains("\"A\""));
        assert!(vs[1].1.contains("\"B\""));
        assert!(s.versions_schemas("missing").is_empty());
    }

    #[test]
    fn compat_defaults_backward_and_is_settable() {
        let mut s = StoreState::default();
        assert_eq!(s.global_compat(), "BACKWARD");
        s.set_global_compat("FULL".into());
        assert_eq!(s.global_compat(), "FULL");
        assert_eq!(s.subject_compat("x"), None);
        s.set_subject_compat("x", "NONE".into());
        assert_eq!(s.subject_compat("x"), Some("NONE"));
    }
}
