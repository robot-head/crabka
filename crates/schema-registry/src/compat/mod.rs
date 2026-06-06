//! Compatibility engine: resolve the effective level, pick the version set, and
//! run the per-format directional check. Format-agnostic (delegates to
//! `format::check`); knows nothing about Avro/Protobuf/JSON internals.

use crate::error::SrError;
use crate::format::{self, SchemaType};
use crate::store::StoreState;

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
    /// Parse a stored level string; unknown strings fall back to the global
    /// default `BACKWARD` (the `/config` layer validates input, so unknowns
    /// are not expected).
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s {
            "NONE" => Self::None,
            "FORWARD" => Self::Forward,
            "FORWARD_TRANSITIVE" => Self::ForwardTransitive,
            "FULL" => Self::Full,
            "FULL_TRANSITIVE" => Self::FullTransitive,
            "BACKWARD_TRANSITIVE" => Self::BackwardTransitive,
            _ => Self::Backward,
        }
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

/// Effective level for a subject: per-subject `/config` override if set, else global.
fn effective_level(snap: &StoreState, subject: &str) -> CompatibilityLevel {
    let s = snap
        .subject_compat(subject)
        .map_or_else(|| snap.global_compat().to_string(), str::to_string);
    CompatibilityLevel::parse(&s)
}

/// Run `candidate` against one existing version's schema in the given
/// directions, collecting failure messages.
fn check_pair(
    ty: SchemaType,
    candidate: &str,
    existing: &str,
    dirs: &[Direction],
    out: &mut Vec<String>,
) {
    for dir in dirs {
        let (reader, writer) = match dir {
            Direction::NewReadsOld => (candidate, existing),
            Direction::OldReadsNew => (existing, candidate),
        };
        if let Err(msgs) = format::check(ty, reader, writer) {
            out.extend(msgs);
        }
    }
}

/// Enforcement on register: `Ok(())` if compatible / `NONE` / first version;
/// `Err(SrError::Incompatible)` otherwise. `candidate` must be normalised.
pub fn check_registration(
    snap: &StoreState,
    subject: &str,
    ty: SchemaType,
    candidate: &str,
) -> Result<(), SrError> {
    let level = effective_level(snap, subject);
    let dirs = level.directions();
    if dirs.is_empty() {
        return Ok(());
    }
    let versions = snap.versions_schemas(subject);
    if versions.is_empty() {
        return Ok(());
    }
    let targets: &[(SchemaType, String)] = if level.is_transitive() {
        &versions
    } else {
        &versions[versions.len() - 1..]
    };
    let mut msgs = Vec::new();
    for (_vty, vschema) in targets {
        check_pair(ty, candidate, vschema, dirs, &mut msgs);
    }
    if msgs.is_empty() {
        Ok(())
    } else {
        Err(SrError::Incompatible(msgs))
    }
}

/// `/compatibility` query: verdict of `candidate` against version `version`
/// (`None` = latest) under the subject's effective level. `Err` for unknown
/// subject/version.
pub fn check_against_version(
    snap: &StoreState,
    subject: &str,
    ty: SchemaType,
    candidate: &str,
    version: Option<i32>,
) -> Result<Verdict, SrError> {
    if snap.versions(subject, false).is_none() {
        return Err(SrError::SubjectNotFound(subject.to_string()));
    }
    let (_, _, _vty, vschema) = snap
        .version(subject, version, false)
        .ok_or(SrError::VersionNotFound)?;
    let level = effective_level(snap, subject);
    let dirs = level.directions();
    if dirs.is_empty() {
        return Ok(Verdict {
            is_compatible: true,
            messages: Vec::new(),
        });
    }
    let mut msgs = Vec::new();
    check_pair(ty, candidate, &vschema, dirs, &mut msgs);
    Ok(Verdict {
        is_compatible: msgs.is_empty(),
        messages: msgs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::SchemaType;
    use crate::store::StoreState;

    fn av(fields: &str) -> String {
        format!("{{\"type\":\"record\",\"name\":\"U\",\"fields\":[{fields}]}}")
    }
    const ID: &str = "{\"name\":\"id\",\"type\":\"int\"}";

    #[test]
    fn level_parse_and_props() {
        assert_eq!(
            CompatibilityLevel::parse("BACKWARD"),
            CompatibilityLevel::Backward
        );
        assert_eq!(
            CompatibilityLevel::parse("FULL_TRANSITIVE"),
            CompatibilityLevel::FullTransitive
        );
        assert!(CompatibilityLevel::FullTransitive.is_transitive());
        assert!(!CompatibilityLevel::Backward.is_transitive());
        assert!(CompatibilityLevel::None.directions().is_empty());
    }

    #[test]
    fn first_version_and_none_always_ok() {
        let snap = StoreState::default();
        assert!(check_registration(&snap, "s", SchemaType::Avro, &av(ID)).is_ok());
    }

    #[test]
    fn backward_rejects_added_required_field() {
        let mut snap = StoreState::default();
        snap.set_subject_compat("s", "BACKWARD".into());
        snap.register("s", SchemaType::Avro, &av(ID)).unwrap();
        let bad = av(&format!("{ID},{{\"name\":\"x\",\"type\":\"int\"}}"));
        assert!(matches!(
            check_registration(&snap, "s", SchemaType::Avro, &bad),
            Err(crate::error::SrError::Incompatible(_))
        ));
        let good = av(&format!(
            "{ID},{{\"name\":\"x\",\"type\":\"int\",\"default\":0}}"
        ));
        assert!(check_registration(&snap, "s", SchemaType::Avro, &good).is_ok());
    }

    #[test]
    fn none_level_bypasses() {
        let mut snap = StoreState::default();
        snap.set_subject_compat("s", "NONE".into());
        snap.register("s", SchemaType::Avro, &av(ID)).unwrap();
        let bad = av(&format!("{ID},{{\"name\":\"x\",\"type\":\"int\"}}"));
        assert!(check_registration(&snap, "s", SchemaType::Avro, &bad).is_ok());
    }

    #[test]
    fn check_against_version_verdict() {
        let mut snap = StoreState::default();
        snap.set_subject_compat("s", "BACKWARD".into());
        snap.register("s", SchemaType::Avro, &av(ID)).unwrap();
        let bad = av(&format!("{ID},{{\"name\":\"x\",\"type\":\"int\"}}"));
        let v = check_against_version(&snap, "s", SchemaType::Avro, &bad, None).unwrap();
        assert!(!v.is_compatible);
        assert!(!v.messages.is_empty());
        let good = av(&format!(
            "{ID},{{\"name\":\"x\",\"type\":\"int\",\"default\":0}}"
        ));
        assert!(
            check_against_version(&snap, "s", SchemaType::Avro, &good, None)
                .unwrap()
                .is_compatible
        );
        assert!(check_against_version(&snap, "nope", SchemaType::Avro, &good, None).is_err());
    }
}
