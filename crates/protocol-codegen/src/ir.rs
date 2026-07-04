use std::{fs, path::Path};

use serde::Deserialize;
use thiserror::Error;

/// One parsed Kafka message schema.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    #[serde(default)]
    pub api_key: Option<i16>,
    pub valid_versions: VersionRange,
    #[serde(default)]
    pub flexible_versions: FlexibleVersions,
    #[serde(default)]
    pub fields: Vec<FieldSpec>,
    #[serde(default)]
    pub common_structs: Vec<CommonStruct>,
    /// crabka-internal RPC with no upstream Apache Kafka equivalent (e.g.
    /// the KIP-966 `GetReplicaLogInfo` controller↔broker RPC). Such messages
    /// are excluded from the JVM differential sweep — the oracle has no
    /// matching message class and crabka owns their wire shape outright.
    #[serde(default)]
    pub internal: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MessageType {
    Request,
    Response,
    Header,
    Data,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    pub versions: VersionRange,
    #[serde(default)]
    pub nullable_versions: Option<VersionRange>,
    #[serde(default)]
    pub tagged_versions: Option<VersionRange>,
    #[serde(default)]
    pub tag: Option<u32>,
    #[serde(default)]
    pub fields: Vec<FieldSpec>,
    #[serde(default)]
    pub default: Option<serde_json::Value>,
    #[serde(default = "default_entity_type")]
    pub entity_type: String,
    #[serde(default)]
    pub map_key: bool,
    #[serde(default)]
    pub about: String,
    /// Per-field flexible-version override. `Some(FlexibleVersions::None)` means this
    /// field always uses legacy (non-compact) encoding even in a flexible-version message.
    /// `None` means "inherit from the message-level `flexible_versions`" (the common case).
    #[serde(default)]
    pub flexible_versions: Option<FlexibleVersions>,
}

fn default_entity_type() -> String {
    String::new()
}

#[derive(Debug, Deserialize)]
pub struct CommonStruct {
    pub name: String,
    pub versions: VersionRange,
    pub fields: Vec<FieldSpec>,
}

/// `"0+"`, `"3+"`, `"0-2"`, `"none"`, `"4"` etc.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VersionRange {
    pub min: i16,
    pub max: i16, // inclusive; i16::MAX represents `+` (open-ended)
}

impl<'de> Deserialize<'de> for VersionRange {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        // "none" means the version range is empty (e.g. removed messages).
        if s == "none" {
            return Ok(VersionRange {
                min: i16::MAX,
                max: i16::MIN,
            });
        }
        parse_version_range(&s).map_err(serde::de::Error::custom)
    }
}

fn parse_version_range(s: &str) -> Result<VersionRange, String> {
    if let Some(rest) = s.strip_suffix('+') {
        let min: i16 = rest
            .parse()
            .map_err(|e| format!("bad version `{s}`: {e}"))?;
        return Ok(VersionRange { min, max: i16::MAX });
    }
    if let Some((lo, hi)) = s.split_once('-') {
        let min: i16 = lo.parse().map_err(|e| format!("bad version `{s}`: {e}"))?;
        let max: i16 = hi.parse().map_err(|e| format!("bad version `{s}`: {e}"))?;
        return Ok(VersionRange { min, max });
    }
    let single: i16 = s.parse().map_err(|e| format!("bad version `{s}`: {e}"))?;
    Ok(VersionRange {
        min: single,
        max: single,
    })
}

impl VersionRange {
    #[must_use]
    pub fn contains(&self, v: i16) -> bool {
        v >= self.min && v <= self.max
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.min > self.max
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum FlexibleVersions {
    #[default]
    None,
    Range(VersionRange),
}

impl<'de> Deserialize<'de> for FlexibleVersions {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        if s == "none" {
            return Ok(FlexibleVersions::None);
        }
        Ok(FlexibleVersions::Range(
            parse_version_range(&s).map_err(serde::de::Error::custom)?,
        ))
    }
}

impl FlexibleVersions {
    #[must_use]
    pub fn is_flexible(&self, v: i16) -> bool {
        match self {
            FlexibleVersions::None => false,
            FlexibleVersions::Range(r) => r.contains(v),
        }
    }
}

#[derive(Debug, Error)]
pub enum IrError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse {file}: {source}")]
    Parse {
        file: String,
        #[source]
        source: serde_json::Error,
    },
}

/// Read every `*.json` file in `dir`, strip `//` comments, parse as `MessageSpec`.
pub fn load_dir(dir: &Path) -> Result<Vec<MessageSpec>, IrError> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let raw = fs::read_to_string(&path)?;
        let stripped = strip_line_comments(&raw);
        let spec: MessageSpec = serde_json::from_str(&stripped).map_err(|e| IrError::Parse {
            file: path.display().to_string(),
            source: e,
        })?;
        out.push(spec);
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// Strip JavaScript-style `//` line comments, ignoring `//` that appears inside
/// a double-quoted JSON string (so string values like URLs survive intact).
fn strip_line_comments(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    for line in src.lines() {
        let bytes = line.as_bytes();
        let mut in_str = false;
        let mut escaped = false;
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            if in_str {
                if escaped {
                    escaped = false;
                } else if b == b'\\' {
                    escaped = true;
                } else if b == b'"' {
                    in_str = false;
                }
            } else if b == b'"' {
                in_str = true;
            } else if b == b'/' && bytes.get(i + 1) == Some(&b'/') {
                break;
            }
            i += 1;
        }
        out.push_str(&line[..i]);
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn version_range_parsing() {
        for (input, want) in [
            (
                "0+",
                VersionRange {
                    min: 0,
                    max: i16::MAX,
                },
            ),
            (
                "3+",
                VersionRange {
                    min: 3,
                    max: i16::MAX,
                },
            ),
            ("0-2", VersionRange { min: 0, max: 2 }),
            ("4", VersionRange { min: 4, max: 4 }),
        ] {
            assert!(parse_version_range(input).unwrap() == want);
        }
        assert!(parse_version_range("none").is_err()); // handled at call site
    }

    #[test]
    fn comment_strip() {
        let src = "{\n// hi\n  \"x\": 1 // trailing\n}";
        let out = strip_line_comments(src);
        assert!(out == "{\n\n  \"x\": 1 \n}\n");
    }

    #[test]
    fn comment_strip_preserves_double_slash_in_string() {
        let src = "{ \"default\": \"http://example.com\" } // tail";
        let out = strip_line_comments(src);
        assert!(out.contains("http://example.com"));
        assert!(!out.contains("tail"));
    }
}
