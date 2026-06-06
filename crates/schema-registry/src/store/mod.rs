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
    deleted: bool,
}

#[derive(Debug, Default, Clone)]
pub struct StoreState {
    subjects: BTreeMap<String, Vec<VersionEntry>>,
    by_id: BTreeMap<i32, (SchemaType, String)>,
    by_canonical: BTreeMap<String, i32>,
    global_compat: Option<String>,
    subject_compat: BTreeMap<String, String>,
    global_mode: Option<String>,
    subject_mode: BTreeMap<String, String>,
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
        let canonical = format::parse(ty, schema, &[])?.canonical_form();
        // Idempotent within subject?
        if let Some(existing) = self.find_under_subject_canonical(subject, &canonical, true) {
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
                deleted: false,
            });
        Ok(Registered {
            id,
            version: next_version,
        })
    }

    /// Fold a decoded SCHEMA record into state (reader replay). Idempotent.
    /// Sets the version's `deleted` flag to the record's — so the same code path
    /// inserts (deleted=false), soft-deletes (deleted=true on an existing
    /// version), and resurrects (deleted=false on a soft-deleted version).
    pub fn apply_schema(&mut self, _key: &SchemaKey, value: &SchemaValue) {
        let ty = SchemaType::from_wire(value.schema_type.as_deref());
        self.max_id = self.max_id.max(value.id);
        self.by_id
            .entry(value.id)
            .or_insert_with(|| (ty, value.schema.clone()));
        if let Ok(p) = format::parse(ty, &value.schema, &[]) {
            self.by_canonical
                .entry(p.canonical_form())
                .or_insert(value.id);
        }
        let entry = self.subjects.entry(value.subject.clone()).or_default();
        if let Some(e) = entry.iter_mut().find(|v| v.version == value.version) {
            e.deleted = value.deleted;
        } else {
            entry.push(VersionEntry {
                version: value.version,
                id: value.id,
                deleted: value.deleted,
            });
            entry.sort_by_key(|v| v.version);
        }
    }

    fn find_under_subject_canonical(
        &self,
        subject: &str,
        canonical: &str,
        include_deleted: bool,
    ) -> Option<Registered> {
        let id = *self.by_canonical.get(canonical)?;
        let entry = self
            .subjects
            .get(subject)?
            .iter()
            .find(|v| v.id == id && (include_deleted || !v.deleted))?;
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
    pub fn subjects(&self, include_deleted: bool) -> Vec<String> {
        self.subjects
            .iter()
            .filter(|(_, vs)| vs.iter().any(|v| include_deleted || !v.deleted))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Live (or, with `include_deleted`, all) version numbers. `None` when the
    /// subject has no qualifying versions (→ 404).
    #[must_use]
    pub fn versions(&self, subject: &str, include_deleted: bool) -> Option<Vec<i32>> {
        let vs = self.subjects.get(subject)?;
        let out: Vec<i32> = vs
            .iter()
            .filter(|v| include_deleted || !v.deleted)
            .map(|v| v.version)
            .collect();
        if out.is_empty() { None } else { Some(out) }
    }

    /// `(id, version, schemaType, schema)` for a subject+version. `version=None`
    /// resolves to the latest qualifying version.
    #[must_use]
    pub fn version(
        &self,
        subject: &str,
        version: Option<i32>,
        include_deleted: bool,
    ) -> Option<(i32, i32, SchemaType, String)> {
        let vs = self.subjects.get(subject)?;
        let entry = match version {
            Some(v) => vs
                .iter()
                .find(|e| e.version == v && (include_deleted || !e.deleted))?,
            None => vs.iter().rfind(|e| include_deleted || !e.deleted)?,
        };
        let (ty, schema) = self.by_id.get(&entry.id)?;
        Some((entry.id, entry.version, *ty, schema.clone()))
    }

    /// Schema bytes for a global id. Returns `None` unless some qualifying
    /// version references the id (so a permanently-deleted id is gone, and a
    /// soft-deleted-only id is hidden without `include_deleted`).
    #[must_use]
    pub fn schema_by_id(&self, id: i32, include_deleted: bool) -> Option<(SchemaType, String)> {
        let (ty, schema) = self.by_id.get(&id)?;
        let referenced = self
            .subjects
            .values()
            .flatten()
            .any(|v| v.id == id && (include_deleted || !v.deleted));
        if referenced {
            Some((*ty, schema.clone()))
        } else {
            None
        }
    }

    /// `(subject, version)` pairs referencing a global id (GET /schemas/ids/{id}/versions).
    #[must_use]
    pub fn schema_id_subject_versions(&self, id: i32, include_deleted: bool) -> Vec<(String, i32)> {
        let mut out = Vec::new();
        for (subject, vs) in &self.subjects {
            for v in vs {
                if v.id == id && (include_deleted || !v.deleted) {
                    out.push((subject.clone(), v.version));
                }
            }
        }
        out
    }

    /// Every `(subject, version, id, schemaType, schema)` (GET /schemas), sorted
    /// by subject then version (matches cp's `/schemas` ordering).
    #[must_use]
    pub fn all_schemas(
        &self,
        include_deleted: bool,
    ) -> Vec<(String, i32, i32, SchemaType, String)> {
        let mut out = Vec::new();
        for (subject, vs) in &self.subjects {
            for v in vs {
                if (include_deleted || !v.deleted)
                    && let Some((ty, schema)) = self.by_id.get(&v.id)
                {
                    out.push((subject.clone(), v.version, v.id, *ty, schema.clone()));
                }
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        out
    }

    /// A subject's LIVE versions as ordered `(type, schema)` pairs (ascending).
    /// Soft-deleted versions are excluded (compat ignores them).
    #[must_use]
    pub fn versions_schemas(&self, subject: &str) -> Vec<(SchemaType, String)> {
        let Some(entries) = self.subjects.get(subject) else {
            return Vec::new();
        };
        entries
            .iter()
            .filter(|e| !e.deleted)
            .filter_map(|e| self.by_id.get(&e.id).cloned())
            .collect()
    }

    /// Flag every version of a subject deleted; returns the version numbers.
    pub fn soft_delete_subject(&mut self, subject: &str) -> Option<Vec<i32>> {
        let vs = self.subjects.get_mut(subject)?;
        if vs.is_empty() {
            return None;
        }
        let versions = vs.iter().map(|v| v.version).collect();
        for v in vs.iter_mut() {
            v.deleted = true;
        }
        Some(versions)
    }

    /// Remove a single version (permanent). Drops the subject if it becomes
    /// empty. Returns `None` if nothing was removed (idempotent replay).
    pub fn permanent_delete_version(&mut self, subject: &str, version: i32) -> Option<i32> {
        let vs = self.subjects.get_mut(subject)?;
        let before = vs.len();
        vs.retain(|v| v.version != version);
        if vs.len() == before {
            return None;
        }
        if vs.is_empty() {
            self.subjects.remove(subject);
        }
        Some(version)
    }

    pub fn set_global_mode(&mut self, mode: String) {
        self.global_mode = Some(mode);
    }
    pub fn set_subject_mode(&mut self, subject: &str, mode: String) {
        self.subject_mode.insert(subject.to_string(), mode);
    }
    pub fn clear_subject_mode(&mut self, subject: &str) {
        self.subject_mode.remove(subject);
    }
    pub fn clear_global_mode(&mut self) {
        self.global_mode = None;
    }

    #[must_use]
    pub fn global_mode(&self) -> &str {
        self.global_mode.as_deref().unwrap_or("READWRITE")
    }
    #[must_use]
    pub fn subject_mode(&self, subject: &str) -> Option<&str> {
        self.subject_mode.get(subject).map(String::as_str)
    }
    /// Subject override else global else `READWRITE`.
    #[must_use]
    pub fn effective_mode(&self, subject: &str) -> &str {
        self.subject_mode
            .get(subject)
            .map_or_else(|| self.global_mode(), String::as_str)
    }

    /// Lookup an already-registered schema under a subject (POST /subjects/{s}).
    #[must_use]
    pub fn find_under_subject(
        &self,
        subject: &str,
        ty: SchemaType,
        schema: &str,
        include_deleted: bool,
    ) -> Option<Registered> {
        let canonical = format::parse(ty, schema, &[]).ok()?.canonical_form();
        self.find_under_subject_canonical(subject, &canonical, include_deleted)
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
        assert_eq!(s.versions("av-value", false).unwrap(), vec![1]);
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
        assert_eq!(s.versions("av-value", false).unwrap(), vec![1, 2]);
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
        assert_eq!(s.versions("av-value", false).unwrap(), vec![1]);
        assert_eq!(s.schema_by_id(1, false).unwrap().1, av("A"));
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

    #[test]
    fn soft_delete_hides_then_deleted_shows_then_permanent_removes() {
        let mut s = StoreState::default();
        s.register("av", SchemaType::Avro, &av("A")).unwrap();
        s.register("av", SchemaType::Avro, &av("B")).unwrap();
        apply_deleted(&mut s, "av", 1, 1, &av("A"));
        assert_eq!(s.versions("av", false).unwrap(), vec![2]);
        assert_eq!(s.versions("av", true).unwrap(), vec![1, 2]);
        assert!(s.version("av", Some(1), false).is_none());
        assert!(s.version("av", Some(1), true).is_some());
        assert_eq!(s.permanent_delete_version("av", 1), Some(1));
        assert!(s.version("av", Some(1), true).is_none());
        assert_eq!(s.versions("av", true).unwrap(), vec![2]);
        // idempotent replay: deleting a missing subject/version is a no-op
        assert_eq!(s.permanent_delete_version("nope", 9), None);
        assert_eq!(s.permanent_delete_version("av", 99), None);
    }

    #[test]
    fn version_none_resolves_latest_skipping_deleted() {
        let mut s = StoreState::default();
        s.register("av", SchemaType::Avro, &av("A")).unwrap(); // id1 / v1
        s.register("av", SchemaType::Avro, &av("B")).unwrap(); // id2 / v2
        apply_deleted(&mut s, "av", 2, 2, &av("B"));
        // latest LIVE version is v1; latest incl. deleted is v2
        assert_eq!(s.version("av", None, false).unwrap().1, 1);
        assert_eq!(s.version("av", None, true).unwrap().1, 2);
    }

    #[test]
    fn soft_delete_subject_flags_all_versions() {
        let mut s = StoreState::default();
        s.register("av", SchemaType::Avro, &av("A")).unwrap();
        s.register("av", SchemaType::Avro, &av("B")).unwrap();
        assert_eq!(s.soft_delete_subject("av"), Some(vec![1, 2]));
        assert!(s.versions("av", false).is_none());
        assert_eq!(s.subjects(false), Vec::<String>::new());
        assert_eq!(s.subjects(true), vec!["av".to_string()]);
    }

    #[test]
    fn resurrect_on_reregister_clears_deleted() {
        let mut s = StoreState::default();
        s.register("av", SchemaType::Avro, &av("A")).unwrap();
        apply_deleted(&mut s, "av", 1, 1, &av("A"));
        assert!(s.versions("av", false).is_none());
        apply_live(&mut s, "av", 1, 1, &av("A"));
        assert_eq!(s.versions("av", false).unwrap(), vec![1]);
    }

    #[test]
    fn schema_by_id_respects_reference_liveness() {
        let mut s = StoreState::default();
        s.register("av", SchemaType::Avro, &av("A")).unwrap();
        assert!(s.schema_by_id(1, false).is_some());
        apply_deleted(&mut s, "av", 1, 1, &av("A"));
        assert!(s.schema_by_id(1, false).is_none());
        assert!(s.schema_by_id(1, true).is_some());
        s.permanent_delete_version("av", 1);
        assert!(s.schema_by_id(1, true).is_none());
    }

    #[test]
    fn schema_id_subject_versions_and_all_schemas() {
        let mut s = StoreState::default();
        s.register("a", SchemaType::Avro, &av("A")).unwrap();
        s.register("b", SchemaType::Avro, &av("A")).unwrap();
        let mut sv = s.schema_id_subject_versions(1, false);
        sv.sort();
        assert_eq!(sv, vec![("a".to_string(), 1), ("b".to_string(), 1)]);
        assert_eq!(s.all_schemas(false).len(), 2);
    }

    #[test]
    fn modes_default_and_resolve() {
        let mut s = StoreState::default();
        assert_eq!(s.global_mode(), "READWRITE");
        assert_eq!(s.effective_mode("x"), "READWRITE");
        s.set_global_mode("READONLY".into());
        assert_eq!(s.effective_mode("x"), "READONLY");
        s.set_subject_mode("x", "IMPORT".into());
        assert_eq!(s.effective_mode("x"), "IMPORT");
        s.clear_subject_mode("x");
        assert_eq!(s.effective_mode("x"), "READONLY");
    }

    fn apply_live(s: &mut StoreState, subject: &str, version: i32, id: i32, schema: &str) {
        apply_with_flag(s, subject, version, id, schema, false);
    }
    fn apply_deleted(s: &mut StoreState, subject: &str, version: i32, id: i32, schema: &str) {
        apply_with_flag(s, subject, version, id, schema, true);
    }
    fn apply_with_flag(
        s: &mut StoreState,
        subject: &str,
        version: i32,
        id: i32,
        schema: &str,
        deleted: bool,
    ) {
        use crate::kafkastore::record::{SchemaKey, SchemaValue};
        let v = SchemaValue {
            subject: subject.into(),
            version,
            id,
            schema_type: None,
            references: vec![],
            schema: schema.into(),
            deleted,
        };
        s.apply_schema(&SchemaKey::new(subject, version), &v);
    }
}
