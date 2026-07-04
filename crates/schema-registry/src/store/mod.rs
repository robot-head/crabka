//! In-memory authoritative registry state, rebuilt by replaying `_schemas`.
//! Pure data structure: no I/O. The `KafkaStore` wraps it behind a lock and the
//! write-serialisation gate (see kafkastore/mod.rs). Cloneable so the write path
//! can decide id/version on a throwaway copy (the reader is the sole mutator of
//! the live instance).

use std::collections::BTreeMap;

use crate::{
    error::SrError,
    format::{self, SchemaType},
    ids::{SchemaId, SchemaVersion},
    kafkastore::record::{SchemaKey, SchemaReference, SchemaValue},
};

/// Result of a registration: the global id and the per-subject version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Registered {
    pub id: SchemaId,
    pub version: SchemaVersion,
}

/// A registered schema's stored form: type + text + its references (references
/// are part of the id identity, so they live with the schema in `by_id`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredSchema {
    pub ty: SchemaType,
    pub schema: String,
    pub references: Vec<crate::kafkastore::record::SchemaReference>,
    pub message_type: Option<String>,
}

/// A single subject-version's stored schema (the domain view returned by
/// [`StoreState::version`]). Named to avoid colliding with the
/// [`SchemaVersion`](crate::ids::SchemaVersion) version-number newtype.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedSchema {
    pub id: SchemaId,
    pub version: SchemaVersion,
    pub ty: SchemaType,
    pub schema: String,
    pub references: Vec<SchemaReference>,
    pub message_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedSchema {
    pub subject: String,
    pub version: SchemaVersion,
    pub id: SchemaId,
    pub ty: SchemaType,
    pub schema: String,
    pub references: Vec<SchemaReference>,
    pub message_type: Option<String>,
}

#[derive(Debug, Clone)]
struct VersionEntry {
    version: SchemaVersion,
    id: SchemaId,
    deleted: bool,
}

#[derive(Debug, Default, Clone)]
pub struct StoreState {
    subjects: BTreeMap<String, Vec<VersionEntry>>,
    by_id: BTreeMap<SchemaId, RegisteredSchema>,
    by_canonical: BTreeMap<String, SchemaId>,
    global_compat: Option<String>,
    subject_compat: BTreeMap<String, String>,
    global_mode: Option<String>,
    subject_mode: BTreeMap<String, String>,
    max_id: SchemaId,
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
        references: &[SchemaReference],
        message_type: Option<&str>,
    ) -> Result<Registered, SrError> {
        let resolved = self.resolve_closure(references)?;
        let canonical = format::parse(ty, schema, &resolved)?.canonical_form();
        let key = Self::dedup_key(&canonical, references, message_type);
        // Idempotent within subject?
        if let Some(existing) = self.find_under_subject_canonical(subject, &key, true) {
            return Ok(existing);
        }
        // Global id: reuse if (canonical+refs) seen anywhere, else next.
        let id = if let Some(&id) = self.by_canonical.get(&key) {
            id
        } else {
            let id = SchemaId(self.max_id.0 + 1);
            self.max_id = id;
            self.by_canonical.insert(key, id);
            self.by_id.insert(
                id,
                RegisteredSchema {
                    ty,
                    schema: schema.to_string(),
                    references: references.to_vec(),
                    message_type: message_type.map(str::to_string),
                },
            );
            id
        };
        let next_version = SchemaVersion(
            self.subjects
                .get(subject)
                .map_or(1, |v| i32::try_from(v.len()).unwrap_or(i32::MAX) + 1),
        );
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

    /// The id-dedup key: canonical form joined with a stable fingerprint of the
    /// references (so identical text with different refs gets a distinct id).
    fn dedup_key(
        canonical: &str,
        references: &[SchemaReference],
        message_type: Option<&str>,
    ) -> String {
        if references.is_empty() && message_type.is_none() {
            return canonical.to_string();
        }
        let mut refs: Vec<String> = references
            .iter()
            .map(|r| format!("{}\u{1}{}\u{1}{}", r.name, r.subject, r.version))
            .collect();
        refs.sort();
        format!(
            "{canonical}\u{0}{}\u{0}{}",
            refs.join("\u{2}"),
            message_type.unwrap_or("")
        )
    }

    /// Resolve a reference list into its transitive closure of
    /// `ResolvedReference`s (depth-first, dependencies emitted before the
    /// referring schema, dedup-by-name keeping first, cycle-guarded by
    /// `(subject, version)`). `ReferenceNotFound` if any
    /// referenced `(subject, version)` is absent.
    pub fn resolve_closure(
        &self,
        references: &[SchemaReference],
    ) -> Result<Vec<crate::format::ResolvedReference>, SrError> {
        let mut out = Vec::new();
        let mut seen_names = std::collections::BTreeSet::new();
        let mut visited = std::collections::BTreeSet::new();
        self.resolve_into(references, &mut out, &mut seen_names, &mut visited)?;
        Ok(out)
    }

    fn resolve_into(
        &self,
        references: &[SchemaReference],
        out: &mut Vec<crate::format::ResolvedReference>,
        seen_names: &mut std::collections::BTreeSet<String>,
        visited: &mut std::collections::BTreeSet<(String, SchemaVersion)>,
    ) -> Result<(), SrError> {
        for r in references {
            let key = (r.subject.clone(), r.version);
            if !visited.insert(key) {
                continue;
            }
            let id = self.id_of(&r.subject, r.version).ok_or_else(|| {
                SrError::ReferenceNotFound(format!("{}:{}:{}", r.name, r.subject, r.version))
            })?;
            let reg = self
                .by_id
                .get(&id)
                .ok_or_else(|| {
                    SrError::ReferenceNotFound(format!("{}:{}:{}", r.name, r.subject, r.version))
                })?
                .clone();
            self.resolve_into(&reg.references, out, seen_names, visited)?;
            if seen_names.insert(r.name.clone()) {
                out.push(crate::format::ResolvedReference {
                    name: r.name.clone(),
                    ty: reg.ty,
                    schema: reg.schema,
                });
            }
        }
        Ok(())
    }

    /// The id of a concrete `(subject, version)`, or `None` (considers deleted
    /// versions — a reference can name a soft-deleted version's content).
    fn id_of(&self, subject: &str, version: SchemaVersion) -> Option<SchemaId> {
        self.subjects
            .get(subject)?
            .iter()
            .find(|v| v.version == version)
            .map(|v| v.id)
    }

    /// Ids of (qualifying) schemas whose references include `(subject, version)`.
    #[must_use]
    pub fn referenced_by(
        &self,
        subject: &str,
        version: SchemaVersion,
        include_deleted: bool,
    ) -> Vec<SchemaId> {
        let mut ids = Vec::new();
        for vs in self.subjects.values() {
            for entry in vs {
                if entry.deleted && !include_deleted {
                    continue;
                }
                if let Some(reg) = self.by_id.get(&entry.id)
                    && reg
                        .references
                        .iter()
                        .any(|r| r.subject == subject && r.version == version)
                    && !ids.contains(&entry.id)
                {
                    ids.push(entry.id);
                }
            }
        }
        ids.sort_unstable();
        ids
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
            .or_insert_with(|| RegisteredSchema {
                ty,
                schema: value.schema.clone(),
                references: value.references.clone(),
                message_type: value.message_type.clone(),
            });
        if let Ok(resolved) = self.resolve_closure(&value.references)
            && let Ok(p) = format::parse(ty, &value.schema, &resolved)
        {
            let key = Self::dedup_key(
                &p.canonical_form(),
                &value.references,
                value.message_type.as_deref(),
            );
            self.by_canonical.entry(key).or_insert(value.id);
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

    pub fn clear_subject_compat(&mut self, subject: &str) {
        self.subject_compat.remove(subject);
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
    pub fn versions(&self, subject: &str, include_deleted: bool) -> Option<Vec<SchemaVersion>> {
        let vs = self.subjects.get(subject)?;
        let out: Vec<SchemaVersion> = vs
            .iter()
            .filter(|v| include_deleted || !v.deleted)
            .map(|v| v.version)
            .collect();
        if out.is_empty() { None } else { Some(out) }
    }

    /// Stored schema details for a subject+version.
    /// `version=None` resolves to the latest qualifying version.
    #[must_use]
    pub fn version(
        &self,
        subject: &str,
        version: Option<SchemaVersion>,
        include_deleted: bool,
    ) -> Option<VersionedSchema> {
        let vs = self.subjects.get(subject)?;
        let entry = match version {
            Some(v) => vs
                .iter()
                .find(|e| e.version == v && (include_deleted || !e.deleted))?,
            None => vs.iter().rfind(|e| include_deleted || !e.deleted)?,
        };
        let reg = self.by_id.get(&entry.id)?;
        Some(VersionedSchema {
            id: entry.id,
            version: entry.version,
            ty: reg.ty,
            schema: reg.schema.clone(),
            references: reg.references.clone(),
            message_type: reg.message_type.clone(),
        })
    }

    /// Schema bytes for a global id. Returns `None` unless some qualifying
    /// version references the id (so a permanently-deleted id is gone, and a
    /// soft-deleted-only id is hidden without `include_deleted`).
    #[must_use]
    pub fn schema_by_id(
        &self,
        id: SchemaId,
        include_deleted: bool,
    ) -> Option<(SchemaType, String, Vec<SchemaReference>, Option<String>)> {
        let reg = self.by_id.get(&id)?;
        let referenced = self
            .subjects
            .values()
            .flatten()
            .any(|v| v.id == id && (include_deleted || !v.deleted));
        if referenced {
            Some((
                reg.ty,
                reg.schema.clone(),
                reg.references.clone(),
                reg.message_type.clone(),
            ))
        } else {
            None
        }
    }

    /// `(subject, version)` pairs referencing a global id (GET /schemas/ids/{id}/versions).
    #[must_use]
    pub fn schema_id_subject_versions(
        &self,
        id: SchemaId,
        include_deleted: bool,
    ) -> Vec<(String, SchemaVersion)> {
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

    /// Every schema visible to GET /schemas, sorted by subject then version
    /// (matches cp's `/schemas` ordering).
    #[must_use]
    pub fn all_schemas(&self, include_deleted: bool) -> Vec<ListedSchema> {
        let mut out = Vec::new();
        for (subject, vs) in &self.subjects {
            for v in vs {
                if (include_deleted || !v.deleted)
                    && let Some(reg) = self.by_id.get(&v.id)
                {
                    out.push(ListedSchema {
                        subject: subject.clone(),
                        version: v.version,
                        id: v.id,
                        ty: reg.ty,
                        schema: reg.schema.clone(),
                        references: reg.references.clone(),
                        message_type: reg.message_type.clone(),
                    });
                }
            }
        }
        out.sort_by(|a, b| a.subject.cmp(&b.subject).then(a.version.cmp(&b.version)));
        out
    }

    /// A subject's LIVE versions as ordered `(type, schema, references)` tuples
    /// (ascending). Soft-deleted versions are excluded (compat ignores them).
    #[must_use]
    pub fn versions_schemas(
        &self,
        subject: &str,
    ) -> Vec<(SchemaType, String, Vec<SchemaReference>)> {
        let Some(entries) = self.subjects.get(subject) else {
            return Vec::new();
        };
        entries
            .iter()
            .filter(|e| !e.deleted)
            .filter_map(|e| {
                self.by_id
                    .get(&e.id)
                    .map(|reg| (reg.ty, reg.schema.clone(), reg.references.clone()))
            })
            .collect()
    }

    /// Flag every version of a subject deleted; returns the version numbers.
    pub fn soft_delete_subject(&mut self, subject: &str) -> Option<Vec<SchemaVersion>> {
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
    pub fn permanent_delete_version(
        &mut self,
        subject: &str,
        version: SchemaVersion,
    ) -> Option<SchemaVersion> {
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
        references: &[SchemaReference],
        message_type: Option<&str>,
        include_deleted: bool,
    ) -> Option<Registered> {
        let resolved = self.resolve_closure(references).ok()?;
        let canonical = format::parse(ty, schema, &resolved).ok()?.canonical_form();
        self.find_under_subject_canonical(
            subject,
            &Self::dedup_key(&canonical, references, message_type),
            include_deleted,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{format::SchemaType, kafkastore::record::SchemaReference};

    fn av(n: &str) -> String {
        format!("{{\"type\":\"record\",\"name\":\"{n}\",\"fields\":[]}}")
    }

    /// Wrap a raw `i32` version so test literals read as `sv(1)`.
    fn sv(n: i32) -> SchemaVersion {
        SchemaVersion(n)
    }
    /// Wrap a raw `i32` id so test literals read as `sid(1)`.
    fn sid(n: i32) -> SchemaId {
        SchemaId(n)
    }

    fn sref(name: &str, subject: &str, version: i32) -> SchemaReference {
        SchemaReference {
            name: name.into(),
            subject: subject.into(),
            version: sv(version),
        }
    }

    #[test]
    fn same_schema_different_refs_gets_distinct_id() {
        let mut s = StoreState::default();
        s.register("base", SchemaType::Avro, &av("Base"), &[], None)
            .unwrap();
        let r1 = s
            .register("a", SchemaType::Avro, &av("A"), &[], None)
            .unwrap();
        let r2 = s
            .register(
                "b",
                SchemaType::Avro,
                &av("A"),
                &[sref("base", "base", 1)],
                None,
            )
            .unwrap();
        assert_ne!(r1.id, r2.id, "refs are part of id identity");
    }

    #[test]
    fn same_schema_same_refs_is_idempotent() {
        let mut s = StoreState::default();
        s.register("base", SchemaType::Avro, &av("Base"), &[], None)
            .unwrap();
        let r1 = s
            .register(
                "d",
                SchemaType::Avro,
                &av("D"),
                &[sref("base", "base", 1)],
                None,
            )
            .unwrap();
        let r2 = s
            .register(
                "d",
                SchemaType::Avro,
                &av("D"),
                &[sref("base", "base", 1)],
                None,
            )
            .unwrap();
        assert_eq!(
            r1, r2,
            "re-register with identical text + refs is idempotent"
        );
    }

    #[test]
    fn resolve_closure_is_transitive_and_cycle_guarded() {
        let mut s = StoreState::default();
        s.register("base", SchemaType::Avro, &av("Base"), &[], None)
            .unwrap();
        s.register(
            "mid",
            SchemaType::Avro,
            &av("Mid"),
            &[sref("base", "base", 1)],
            None,
        )
        .unwrap();
        let closure = s.resolve_closure(&[sref("mid", "mid", 1)]).unwrap();
        let names: Vec<&str> = closure.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"mid") && names.contains(&"base"));
        assert!(s.resolve_closure(&[sref("x", "nope", 1)]).is_err());
    }

    #[test]
    fn referenced_by_lists_referrers() {
        let mut s = StoreState::default();
        s.register("base", SchemaType::Avro, &av("Base"), &[], None)
            .unwrap();
        let r = s
            .register(
                "dep",
                SchemaType::Avro,
                &av("Dep"),
                &[sref("base", "base", 1)],
                None,
            )
            .unwrap();
        assert_eq!(s.referenced_by("base", sv(1), false), vec![r.id]);
        assert!(s.referenced_by("base", sv(99), false).is_empty());
    }

    #[test]
    fn first_registration_gets_id_1_version_1() {
        let mut s = StoreState::default();
        let r = s
            .register("av-value", SchemaType::Avro, &av("A"), &[], None)
            .unwrap();
        assert_eq!((r.id, r.version), (sid(1), sv(1)));
    }

    #[test]
    fn identical_under_same_subject_is_idempotent() {
        let mut s = StoreState::default();
        let r1 = s
            .register("av-value", SchemaType::Avro, &av("A"), &[], None)
            .unwrap();
        let r2 = s
            .register("av-value", SchemaType::Avro, &av("A"), &[], None)
            .unwrap();
        assert_eq!(r1, r2);
        assert_eq!(s.versions("av-value", false).unwrap(), vec![sv(1)]);
    }

    #[test]
    fn same_schema_new_subject_reuses_global_id_fresh_version() {
        let mut s = StoreState::default();
        let r1 = s
            .register("av-value", SchemaType::Avro, &av("A"), &[], None)
            .unwrap();
        let r2 = s
            .register("other-value", SchemaType::Avro, &av("A"), &[], None)
            .unwrap();
        assert_eq!(r1.id, r2.id);
        assert_eq!(r2.version, sv(1));
    }

    #[test]
    fn different_schema_increments_id_and_version() {
        let mut s = StoreState::default();
        let r1 = s
            .register("av-value", SchemaType::Avro, &av("A"), &[], None)
            .unwrap();
        let r2 = s
            .register("av-value", SchemaType::Avro, &av("B"), &[], None)
            .unwrap();
        assert_eq!(r2.id, SchemaId(r1.id.0 + 1));
        assert_eq!(r2.version, sv(2));
        assert_eq!(s.versions("av-value", false).unwrap(), vec![sv(1), sv(2)]);
    }

    #[test]
    fn invalid_schema_rejected_even_under_none() {
        let mut s = StoreState::default();
        assert!(
            s.register("av-value", SchemaType::Avro, "{not avro}", &[], None)
                .is_err()
        );
    }

    #[test]
    fn apply_schema_is_idempotent() {
        use crate::kafkastore::record::{SchemaKey, SchemaValue};
        let mut s = StoreState::default();
        let v = SchemaValue {
            subject: "av-value".into(),
            version: sv(1),
            id: sid(1),
            schema_type: None,
            message_type: None,
            references: vec![],
            schema: av("A"),
            deleted: false,
        };
        let k = SchemaKey::new("av-value", sv(1));
        s.apply_schema(&k, &v);
        s.apply_schema(&k, &v); // second apply is a no-op
        assert_eq!(s.versions("av-value", false).unwrap(), vec![sv(1)]);
        assert_eq!(s.schema_by_id(sid(1), false).unwrap().1, av("A"));
        // a fresh register of the same schema is now idempotent against replayed state
        let r = s
            .register("av-value", SchemaType::Avro, &av("A"), &[], None)
            .unwrap();
        assert_eq!((r.id, r.version), (sid(1), sv(1)));
    }

    #[test]
    fn versions_schemas_returns_ordered_pairs() {
        let mut s = StoreState::default();
        s.register("av-value", SchemaType::Avro, &av("A"), &[], None)
            .unwrap();
        s.register("av-value", SchemaType::Avro, &av("B"), &[], None)
            .unwrap();
        let vs = s.versions_schemas("av-value");
        assert_eq!(
            vs,
            vec![
                (SchemaType::Avro, av("A"), vec![]),
                (SchemaType::Avro, av("B"), vec![]),
            ]
        );
        assert_eq!(s.versions_schemas("missing"), vec![]);
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
        s.register("av", SchemaType::Avro, &av("A"), &[], None)
            .unwrap();
        s.register("av", SchemaType::Avro, &av("B"), &[], None)
            .unwrap();
        apply_deleted(&mut s, "av", 1, 1, &av("A"));
        assert_eq!(s.versions("av", false).unwrap(), vec![sv(2)]);
        assert_eq!(s.versions("av", true).unwrap(), vec![sv(1), sv(2)]);
        assert!(s.version("av", Some(sv(1)), false).is_none());
        assert!(s.version("av", Some(sv(1)), true).is_some());
        assert_eq!(s.permanent_delete_version("av", sv(1)), Some(sv(1)));
        assert!(s.version("av", Some(sv(1)), true).is_none());
        assert_eq!(s.versions("av", true).unwrap(), vec![sv(2)]);
        // idempotent replay: deleting a missing subject/version is a no-op
        assert_eq!(s.permanent_delete_version("nope", sv(9)), None);
        assert_eq!(s.permanent_delete_version("av", sv(99)), None);
    }

    #[test]
    fn version_none_resolves_latest_skipping_deleted() {
        let mut s = StoreState::default();
        s.register("av", SchemaType::Avro, &av("A"), &[], None)
            .unwrap(); // id1 / v1
        s.register("av", SchemaType::Avro, &av("B"), &[], None)
            .unwrap(); // id2 / v2
        apply_deleted(&mut s, "av", 2, 2, &av("B"));
        // latest LIVE version is v1; latest incl. deleted is v2
        assert_eq!(s.version("av", None, false).unwrap().version, sv(1));
        assert_eq!(s.version("av", None, true).unwrap().version, sv(2));
    }

    #[test]
    fn soft_delete_subject_flags_all_versions() {
        let mut s = StoreState::default();
        s.register("av", SchemaType::Avro, &av("A"), &[], None)
            .unwrap();
        s.register("av", SchemaType::Avro, &av("B"), &[], None)
            .unwrap();
        assert_eq!(s.soft_delete_subject("av"), Some(vec![sv(1), sv(2)]));
        assert!(s.versions("av", false).is_none());
        assert_eq!(s.subjects(false), Vec::<String>::new());
        assert_eq!(s.subjects(true), vec!["av".to_string()]);
    }

    #[test]
    fn resurrect_on_reregister_clears_deleted() {
        let mut s = StoreState::default();
        s.register("av", SchemaType::Avro, &av("A"), &[], None)
            .unwrap();
        apply_deleted(&mut s, "av", 1, 1, &av("A"));
        assert!(s.versions("av", false).is_none());
        apply_live(&mut s, "av", 1, 1, &av("A"));
        assert_eq!(s.versions("av", false).unwrap(), vec![sv(1)]);
    }

    #[test]
    fn schema_by_id_respects_reference_liveness() {
        let mut s = StoreState::default();
        s.register("av", SchemaType::Avro, &av("A"), &[], None)
            .unwrap();
        assert!(s.schema_by_id(sid(1), false).is_some());
        apply_deleted(&mut s, "av", 1, 1, &av("A"));
        assert!(s.schema_by_id(sid(1), false).is_none());
        assert!(s.schema_by_id(sid(1), true).is_some());
        s.permanent_delete_version("av", sv(1));
        assert!(s.schema_by_id(sid(1), true).is_none());
    }

    #[test]
    fn schema_id_subject_versions_and_all_schemas() {
        let mut s = StoreState::default();
        s.register("a", SchemaType::Avro, &av("A"), &[], None)
            .unwrap();
        s.register("b", SchemaType::Avro, &av("A"), &[], None)
            .unwrap();
        let mut pairs = s.schema_id_subject_versions(sid(1), false);
        pairs.sort();
        assert_eq!(
            pairs,
            vec![("a".to_string(), sv(1)), ("b".to_string(), sv(1))]
        );
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
            version: sv(version),
            id: sid(id),
            schema_type: None,
            message_type: None,
            references: vec![],
            schema: schema.into(),
            deleted,
        };
        s.apply_schema(&SchemaKey::new(subject, sv(version)), &v);
    }
}
