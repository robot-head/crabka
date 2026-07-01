//! Bulk import endpoint for compiled protobuf descriptor sets.

use std::collections::{BTreeMap, BTreeSet};

use axum::body::Bytes;
use axum::extract::State;
use axum::response::Response;
use prost_reflect::prost::Message;
use prost_reflect::prost_types::{FileDescriptorProto, FileDescriptorSet};

use crate::error::SrError;
use crate::format::{self, SchemaType};
use crate::kafkastore::RegisterSchema;
use crate::kafkastore::record::SchemaReference;
use crate::rest::{AppState, response::ok_json};

/// POST /schemas/import
///
/// Body is a binary-encoded [`FileDescriptorSet`]. Each file is registered as a
/// PROTOBUF schema under subject `<file.name>`. Imports become Schema Registry
/// references using Confluent's default reference subject policy:
/// `{ name: import_path, subject: import_path, version: dependency_version }`.
#[tracing::instrument(
    level = "info",
    name = "sr.import_descriptor_set",
    skip_all,
    fields(body_len = body.len(), files = tracing::field::Empty),
    err
)]
pub async fn file_descriptor_set(
    State(st): State<AppState>,
    body: Bytes,
) -> Result<Response, SrError> {
    let set = FileDescriptorSet::decode(body)
        .map_err(|e| SrError::InvalidSchema(format!("FileDescriptorSet: {e}")))?;
    let import = DescriptorSetImport::new(set)?;
    tracing::Span::current().record("files", import.order.len());
    let mut registered = BTreeMap::<String, i32>::new();
    let mut rows = Vec::with_capacity(import.order.len());

    for name in &import.order {
        let file = import
            .files
            .get(name)
            .expect("topological order contains known file");
        let references = references_for(file, &registered, &st)?;
        let schema = format::protobuf::normalize(file);
        let reg = st
            .store
            .register(RegisterSchema {
                subject: name,
                ty: SchemaType::Protobuf,
                schema: &schema,
                references: &references,
                message_type: None,
                import_id: None,
                import_version: None,
            })
            .await?;
        registered.insert(name.clone(), reg.version);
        rows.push(serde_json::json!({
            "subject": name,
            "id": reg.id,
            "version": reg.version,
        }));
    }

    Ok(ok_json(&rows))
}

#[derive(Debug)]
struct DescriptorSetImport {
    files: BTreeMap<String, FileDescriptorProto>,
    order: Vec<String>,
}

impl DescriptorSetImport {
    fn new(set: FileDescriptorSet) -> Result<Self, SrError> {
        let mut files = BTreeMap::new();
        for file in set.file {
            let name = file.name.clone().filter(|n| !n.is_empty()).ok_or_else(|| {
                SrError::InvalidSchema("FileDescriptorSet file missing name".into())
            })?;
            if files.insert(name.clone(), file).is_some() {
                return Err(SrError::InvalidSchema(format!(
                    "FileDescriptorSet contains duplicate file {name}"
                )));
            }
        }
        let mut order = Vec::with_capacity(files.len());
        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        for name in files.keys() {
            visit(name, &files, &mut visiting, &mut visited, &mut order)?;
        }
        Ok(Self { files, order })
    }
}

fn visit(
    name: &str,
    files: &BTreeMap<String, FileDescriptorProto>,
    visiting: &mut BTreeSet<String>,
    visited: &mut BTreeSet<String>,
    order: &mut Vec<String>,
) -> Result<(), SrError> {
    if visited.contains(name) {
        return Ok(());
    }
    if !visiting.insert(name.to_string()) {
        return Err(SrError::InvalidSchema(format!(
            "FileDescriptorSet import cycle at {name}"
        )));
    }
    let file = files
        .get(name)
        .expect("visit is only called for known files");
    for dep in &file.dependency {
        if files.contains_key(dep) {
            visit(dep, files, visiting, visited, order)?;
        }
    }
    visiting.remove(name);
    visited.insert(name.to_string());
    order.push(name.to_string());
    Ok(())
}

fn references_for(
    file: &FileDescriptorProto,
    registered: &BTreeMap<String, i32>,
    st: &AppState,
) -> Result<Vec<SchemaReference>, SrError> {
    file.dependency
        .iter()
        .map(|dep| {
            let version = match registered.get(dep).copied() {
                Some(version) => version,
                None => st
                    .store
                    .store
                    .read()
                    .version(dep, None, false)
                    .map(|found| found.version)
                    .ok_or_else(|| SrError::ReferenceNotFound(dep.clone()))?,
            };
            Ok(SchemaReference {
                name: dep.clone(),
                subject: dep.clone(),
                version,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(name: &str, deps: &[&str]) -> FileDescriptorProto {
        FileDescriptorProto {
            name: Some(name.into()),
            dependency: deps.iter().map(|d| (*d).into()).collect(),
            syntax: Some("proto3".into()),
            ..Default::default()
        }
    }

    #[test]
    fn descriptor_set_is_sorted_by_internal_dependencies() {
        let import = DescriptorSetImport::new(FileDescriptorSet {
            file: vec![file("b.proto", &["a.proto"]), file("a.proto", &[])],
        })
        .unwrap();
        assert_eq!(import.order, vec!["a.proto", "b.proto"]);
    }

    #[test]
    fn duplicate_file_names_are_rejected() {
        let err = DescriptorSetImport::new(FileDescriptorSet {
            file: vec![file("a.proto", &[]), file("a.proto", &[])],
        })
        .unwrap_err();
        assert!(err.to_string().contains("duplicate file"));
    }
}
