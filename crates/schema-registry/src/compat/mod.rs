//! Compatibility engine.
//!
//! It resolves the effective level, picks the version set, and runs the
//! per-format directional check. It is format-agnostic and delegates to
//! `format::check`. It knows nothing about Avro, Protobuf, or JSON internals.

use crate::{
    error::SrError,
    format::{self, SchemaType},
    ids::SchemaVersion,
    store::StoreState,
};

/// Confluent compatibility levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityLevel {
    None,
    Backward,
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
}

/// One directional check.
#[derive(Debug, Clone, Copy)]
enum Direction {
    /// reader = candidate (new), writer = existing (old).
    NewReadsOld,
    /// reader = existing (old), writer = candidate (new).
    OldReadsNew,
}

/// Result of a `/compatibility` query.
#[derive(Debug, Clone)]
pub struct Verdict {
    pub is_compatible: bool,
    pub messages: Vec<String>,
}

impl CompatibilityLevel {
    /// Parse a stored level string. An unknown string falls back to the global
    /// default `BACKWARD`. The `/config` layer validates input, so an unknown
    /// string is not expected.
    #[must_use]
    pub fn parse(s: &str) -> Self {
        Self::try_parse(s).unwrap_or(Self::Backward)
    }

    /// Parse a compatibility level without applying a fallback.
    #[must_use]
    pub fn try_parse(s: &str) -> Option<Self> {
        Some(match s {
            "NONE" => Self::None,
            "BACKWARD" => Self::Backward,
            "BACKWARD_TRANSITIVE" => Self::BackwardTransitive,
            "FORWARD" => Self::Forward,
            "FORWARD_TRANSITIVE" => Self::ForwardTransitive,
            "FULL" => Self::Full,
            "FULL_TRANSITIVE" => Self::FullTransitive,
            _ => return None,
        })
    }

    #[must_use]
    pub fn is_transitive(self) -> bool {
        matches!(
            self,
            Self::BackwardTransitive | Self::ForwardTransitive | Self::FullTransitive
        )
    }

    fn directions(self) -> &'static [Direction] {
        match self {
            Self::None => &[],
            Self::Backward | Self::BackwardTransitive => &[Direction::NewReadsOld],
            Self::Forward | Self::ForwardTransitive => &[Direction::OldReadsNew],
            Self::Full | Self::FullTransitive => &[Direction::NewReadsOld, Direction::OldReadsNew],
        }
    }
}

/// Effective level for a subject. It is the per-subject `/config` override when
/// set, and the global level otherwise.
fn effective_level(snap: &StoreState, subject: &str) -> CompatibilityLevel {
    let s = snap
        .subject_compat(subject)
        .map_or_else(|| snap.global_compat().to_string(), str::to_string);
    CompatibilityLevel::parse(&s)
}

/// Run `candidate` against one existing version's schema in the given
/// directions and collect the failure messages.
fn check_pair(
    ty: SchemaType,
    candidate: &str,
    candidate_refs: &[crate::format::ResolvedReference],
    existing: &str,
    existing_refs: &[crate::format::ResolvedReference],
    dirs: &[Direction],
    out: &mut Vec<String>,
) {
    for dir in dirs {
        let (reader, writer, reader_refs, writer_refs) = match dir {
            Direction::NewReadsOld => (candidate, existing, candidate_refs, existing_refs),
            Direction::OldReadsNew => (existing, candidate, existing_refs, candidate_refs),
        };
        if let Err(msgs) = format::check(ty, reader, writer, reader_refs, writer_refs) {
            out.extend(msgs);
        }
    }
}

/// Enforcement on register. It returns `Ok(())` when the candidate is
/// compatible, when the level is `NONE`, or when this is the first version. It
/// returns `Err(SrError::Incompatible)` in every other case. `candidate` must
/// be normalised.
#[tracing::instrument(
    level = "debug",
    name = "compat.check_registration",
    skip_all,
    fields(subject = %subject, schema_type = ?ty, level = tracing::field::Empty, transitive = tracing::field::Empty),
    err
)]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub fn check_registration(
    snap: &StoreState,
    subject: &str,
    ty: SchemaType,
    candidate: &str,
    candidate_refs: &[crate::format::ResolvedReference],
) -> Result<(), SrError> {
    let level = effective_level(snap, subject);
    let span = tracing::Span::current();
    span.record("level", tracing::field::debug(level));
    span.record("transitive", level.is_transitive());
    let dirs = level.directions();
    if dirs.is_empty() {
        return Ok(());
    }
    let versions = snap.versions_schemas(subject);
    if versions.is_empty() {
        return Ok(());
    }
    let targets: &[(
        SchemaType,
        String,
        Vec<crate::kafkastore::record::SchemaReference>,
    )] = if level.is_transitive() {
        &versions
    } else {
        &versions[versions.len() - 1..]
    };
    let mut msgs = Vec::new();
    for (_vty, vschema, v_refs) in targets {
        // An already-stored version was valid at register time, so its closure
        // resolves; tolerate failure defensively (empty refs).
        let existing_resolved = snap.resolve_closure(v_refs).unwrap_or_default();
        check_pair(
            ty,
            candidate,
            candidate_refs,
            vschema,
            &existing_resolved,
            dirs,
            &mut msgs,
        );
    }
    if msgs.is_empty() {
        Ok(())
    } else {
        Err(SrError::Incompatible(msgs))
    }
}

/// `/compatibility` query. It returns the verdict of `candidate` against
/// version `version` under the subject's effective level, where `None` means
/// the latest version. It returns `Err` for an unknown subject or version.
#[tracing::instrument(
    level = "debug",
    name = "compat.check_against_version",
    skip_all,
    fields(subject = %subject, schema_type = ?ty, level = tracing::field::Empty, is_compatible = tracing::field::Empty),
    err
)]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub fn check_against_version(
    snap: &StoreState,
    subject: &str,
    ty: SchemaType,
    candidate: &str,
    candidate_refs: &[crate::format::ResolvedReference],
    version: Option<SchemaVersion>,
) -> Result<Verdict, SrError> {
    if snap.versions(subject, false).is_none() {
        return Err(SrError::SubjectNotFound(subject.to_string()));
    }
    let existing = snap
        .version(subject, version, false)
        .ok_or(SrError::VersionNotFound)?;
    let level = effective_level(snap, subject);
    tracing::Span::current().record("level", tracing::field::debug(level));
    let dirs = level.directions();
    if dirs.is_empty() {
        tracing::Span::current().record("is_compatible", true);
        return Ok(Verdict {
            is_compatible: true,
            messages: Vec::new(),
        });
    }
    // The stored version's closure resolves (valid at register time); tolerate
    // failure defensively.
    let existing_resolved = snap
        .resolve_closure(&existing.references)
        .unwrap_or_default();
    let mut msgs = Vec::new();
    check_pair(
        ty,
        candidate,
        candidate_refs,
        &existing.schema,
        &existing_resolved,
        dirs,
        &mut msgs,
    );
    tracing::Span::current().record("is_compatible", msgs.is_empty());
    Ok(Verdict {
        is_compatible: msgs.is_empty(),
        messages: msgs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{format::SchemaType, store::StoreState};

    fn av(fields: &str) -> String {
        format!("{{\"type\":\"record\",\"name\":\"U\",\"fields\":[{fields}]}}")
    }
    const ID: &str = "{\"name\":\"id\",\"type\":\"int\"}";

    #[test]
    fn level_parse_and_props() {
        for (_name, input, expected) in [
            (
                "backward",
                "BACKWARD",
                (CompatibilityLevel::Backward, false, 1),
            ),
            (
                "full_transitive",
                "FULL_TRANSITIVE",
                (CompatibilityLevel::FullTransitive, true, 2),
            ),
            ("none", "NONE", (CompatibilityLevel::None, false, 0)),
        ] {
            let level = CompatibilityLevel::parse(input);
            let (expected_level, expected_transitive, expected_direction_count) = expected;
            assert2::assert!(level == expected_level);
            assert2::assert!(level.is_transitive() == expected_transitive);
            assert2::assert!(level.directions().len() == expected_direction_count);
        }
    }

    #[test]
    fn first_version_and_none_always_ok() {
        let snap = StoreState::default();
        assert2::assert!(check_registration(&snap, "s", SchemaType::Avro, &av(ID), &[]).is_ok());
    }

    #[test]
    fn backward_rejects_added_required_field() {
        let mut snap = StoreState::default();
        snap.set_subject_compat("s", "BACKWARD".into());
        snap.register("s", SchemaType::Avro, &av(ID), &[], None)
            .unwrap();
        for (_name, candidate, compatible) in [
            (
                "required_field",
                av(&format!("{ID},{{\"name\":\"x\",\"type\":\"int\"}}")),
                false,
            ),
            (
                "defaulted_field",
                av(&format!(
                    "{ID},{{\"name\":\"x\",\"type\":\"int\",\"default\":0}}"
                )),
                true,
            ),
        ] {
            assert2::assert!(
                check_registration(&snap, "s", SchemaType::Avro, &candidate, &[]).is_ok()
                    == compatible
            );
        }
    }

    #[test]
    fn none_level_bypasses() {
        let mut snap = StoreState::default();
        snap.set_subject_compat("s", "NONE".into());
        snap.register("s", SchemaType::Avro, &av(ID), &[], None)
            .unwrap();
        let bad = av(&format!("{ID},{{\"name\":\"x\",\"type\":\"int\"}}"));
        assert2::assert!(check_registration(&snap, "s", SchemaType::Avro, &bad, &[]).is_ok());
    }

    #[test]
    fn check_against_version_verdict() {
        let mut snap = StoreState::default();
        snap.set_subject_compat("s", "BACKWARD".into());
        snap.register("s", SchemaType::Avro, &av(ID), &[], None)
            .unwrap();
        let bad = av(&format!("{ID},{{\"name\":\"x\",\"type\":\"int\"}}"));
        let good = av(&format!(
            "{ID},{{\"name\":\"x\",\"type\":\"int\",\"default\":0}}"
        ));
        for (_name, subject, candidate, expected) in [
            ("incompatible", "s", bad.as_str(), Some((false, false))),
            ("compatible", "s", good.as_str(), Some((true, true))),
            ("missing_subject", "nope", good.as_str(), None),
        ] {
            let actual =
                check_against_version(&snap, subject, SchemaType::Avro, candidate, &[], None)
                    .ok()
                    .map(|verdict| (verdict.is_compatible, verdict.messages.is_empty()));
            assert2::assert!(actual == expected);
        }
    }
}
