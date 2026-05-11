use serde::Deserialize;
use std::fs;
use std::path::Path;
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

/// Strip JavaScript-style `//` line comments. Naive but adequate for these schemas:
/// quoted strings in the schemas do not contain `//`.
fn strip_line_comments(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    for line in src.lines() {
        if let Some(idx) = line.find("//") {
            out.push_str(&line[..idx]);
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_range_parsing() {
        assert_eq!(
            parse_version_range("0+").unwrap(),
            VersionRange {
                min: 0,
                max: i16::MAX
            }
        );
        assert_eq!(
            parse_version_range("3+").unwrap(),
            VersionRange {
                min: 3,
                max: i16::MAX
            }
        );
        assert_eq!(
            parse_version_range("0-2").unwrap(),
            VersionRange { min: 0, max: 2 }
        );
        assert_eq!(
            parse_version_range("4").unwrap(),
            VersionRange { min: 4, max: 4 }
        );
        assert!(parse_version_range("none").is_err()); // handled at call site
    }

    #[test]
    fn comment_strip() {
        let src = "{\n// hi\n  \"x\": 1 // trailing\n}";
        let out = strip_line_comments(src);
        assert!(!out.contains("hi"));
        assert!(!out.contains("trailing"));
        assert!(out.contains("\"x\": 1"));
    }
}
