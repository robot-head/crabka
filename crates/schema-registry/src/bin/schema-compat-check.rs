//! Local protobuf `FileDescriptorSet` backward-compatibility checker.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use clap::Parser;
use crabka_schema_registry::format::{self, ResolvedReference, SchemaType};
use prost_reflect::{
    prost::Message,
    prost_types::{FileDescriptorProto, FileDescriptorSet},
};

#[derive(Debug, Parser)]
#[command(
    name = "crabka-schema-compat-check",
    version,
    about = "Fail if a new protobuf FileDescriptorSet is backward-incompatible with an old one"
)]
struct Args {
    /// Baseline binary `FileDescriptorSet`.
    #[arg(long)]
    old: PathBuf,
    /// Candidate binary `FileDescriptorSet`.
    #[arg(long)]
    new: PathBuf,
}

#[derive(Debug)]
struct DescriptorSet {
    files: BTreeMap<String, FileDescriptorProto>,
    schemas: BTreeMap<String, String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let old = read_set(&args.old)?;
    let new = read_set(&args.new)?;
    let errors = check_backward(&old, &new);
    if errors.is_empty() {
        println!("compatible");
        return Ok(());
    }
    for error in &errors {
        eprintln!("{error}");
    }
    anyhow::bail!("{} incompatible protobuf schema(s)", errors.len())
}

fn read_set(path: &PathBuf) -> anyhow::Result<DescriptorSet> {
    let bytes = std::fs::read(path)
        .map_err(|e| anyhow::anyhow!("read descriptor set {}: {e}", path.display()))?;
    let set = FileDescriptorSet::decode(bytes.as_slice())
        .map_err(|e| anyhow::anyhow!("decode descriptor set {}: {e}", path.display()))?;
    let mut files = BTreeMap::new();
    let mut schemas = BTreeMap::new();
    for file in set.file {
        let name = file.name.clone().filter(|n| !n.is_empty()).ok_or_else(|| {
            anyhow::anyhow!(
                "descriptor set {} contains file without name",
                path.display()
            )
        })?;
        if files.insert(name.clone(), file.clone()).is_some() {
            anyhow::bail!(
                "descriptor set {} contains duplicate file {name}",
                path.display()
            );
        }
        schemas.insert(name, format::protobuf::normalize(&file));
    }
    Ok(DescriptorSet { files, schemas })
}

fn check_backward(old: &DescriptorSet, new: &DescriptorSet) -> Vec<String> {
    let mut errors = Vec::new();
    for name in old.files.keys() {
        let Some(new_schema) = new.schemas.get(name) else {
            errors.push(format!("{name}: file removed"));
            continue;
        };
        let old_schema = old
            .schemas
            .get(name)
            .expect("old file has normalized schema");
        let old_refs = match resolved_closure(name, old) {
            Ok(refs) => refs,
            Err(e) => {
                errors.push(format!("{name}: old descriptor references: {e}"));
                continue;
            }
        };
        let new_refs = match resolved_closure(name, new) {
            Ok(refs) => refs,
            Err(e) => {
                errors.push(format!("{name}: new descriptor references: {e}"));
                continue;
            }
        };
        if let Err(reasons) = format::check(
            SchemaType::Protobuf,
            new_schema,
            old_schema,
            &new_refs,
            &old_refs,
        ) {
            errors.push(format!("{name}: {}", reasons.join("; ")));
        }
    }
    errors
}

fn resolved_closure(name: &str, set: &DescriptorSet) -> anyhow::Result<Vec<ResolvedReference>> {
    let mut seen = BTreeSet::new();
    let mut refs = Vec::new();
    collect_refs(name, set, &mut seen, &mut refs)?;
    Ok(refs)
}

fn collect_refs(
    name: &str,
    set: &DescriptorSet,
    seen: &mut BTreeSet<String>,
    refs: &mut Vec<ResolvedReference>,
) -> anyhow::Result<()> {
    let file = set
        .files
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("missing file {name}"))?;
    for dep in &file.dependency {
        if !seen.insert(dep.clone()) {
            continue;
        }
        collect_refs(dep, set, seen, refs)?;
        let schema = set
            .schemas
            .get(dep)
            .ok_or_else(|| anyhow::anyhow!("missing dependency {dep}"))?
            .clone();
        refs.push(ResolvedReference {
            name: dep.clone(),
            ty: SchemaType::Protobuf,
            schema,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use prost_reflect::prost_types::{
        DescriptorProto, FieldDescriptorProto,
        field_descriptor_proto::{Label as FieldLabel, Type as FieldType},
    };

    use super::*;

    fn set(file: FileDescriptorProto) -> DescriptorSet {
        let name = file.name.clone().unwrap();
        DescriptorSet {
            schemas: BTreeMap::from([(name.clone(), format::protobuf::normalize(&file))]),
            files: BTreeMap::from([(name, file)]),
        }
    }

    fn user_file(field_type: FieldType) -> FileDescriptorProto {
        FileDescriptorProto {
            name: Some("user.proto".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("User".into()),
                field: vec![FieldDescriptorProto {
                    name: Some("id".into()),
                    number: Some(1),
                    label: Some(FieldLabel::Optional as i32),
                    r#type: Some(field_type as i32),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn scalar_wire_group_change_is_compatible() {
        let old = set(user_file(FieldType::Int32));
        let new = set(user_file(FieldType::Int64));
        assert!(check_backward(&old, &new).is_empty());
    }

    #[test]
    fn string_replacing_int32_is_incompatible() {
        let old = set(user_file(FieldType::Int32));
        let new = set(user_file(FieldType::String));
        let errors = check_backward(&old, &new);
        assert_eq!(
            (
                errors.len(),
                errors
                    .first()
                    .is_some_and(|error| error.contains("user.proto")),
            ),
            (1, true)
        );
    }
}
