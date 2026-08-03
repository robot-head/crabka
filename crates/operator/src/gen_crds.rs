use std::{fs, path::Path};

use crate::crd::{
    Gres, GresTenant, Kafka, KafkaGrpcGateway, KafkaNodePool, KafkaRebalance, KafkaTopic,
    KafkaUser, SchemaRegistry,
};

/// Write every CRD this operator owns into `out_dir` as
/// `<group>_<plural>.yaml`. Existing files are overwritten.
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub fn write_all(out_dir: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(out_dir)?;
    write_one::<Kafka>(out_dir)?;
    write_one::<KafkaNodePool>(out_dir)?;
    write_one::<KafkaTopic>(out_dir)?;
    write_one::<KafkaUser>(out_dir)?;
    write_one::<KafkaRebalance>(out_dir)?;
    write_one::<KafkaGrpcGateway>(out_dir)?;
    write_one::<SchemaRegistry>(out_dir)?;
    write_one::<Gres>(out_dir)?;
    write_one::<GresTenant>(out_dir)?;
    Ok(())
}

fn write_one<K>(out_dir: &Path) -> anyhow::Result<()>
where
    K: kube::Resource<DynamicType = ()> + kube::CustomResourceExt,
{
    let mut crd = K::crd();
    for version in &mut crd.spec.versions {
        if let Some(schema) = version
            .schema
            .as_mut()
            .and_then(|validation| validation.open_api_v3_schema.as_mut())
        {
            let mut value = serde_json::to_value(&*schema)?;
            normalize_numeric_formats(&mut value);
            *schema = serde_json::from_value(value)?;
        }
    }
    let group = &crd.spec.group;
    let plural = &crd.spec.names.plural;
    let file = out_dir.join(format!("{group}_{plural}.yaml"));
    let yaml = serde_yaml::to_string(&crd)?;
    fs::write(&file, yaml)?;
    eprintln!("wrote {}", file.display());
    Ok(())
}

/// Kubernetes ignores numeric `format` annotations and warns about them when a
/// CRD is applied. Replace each format with any constraints needed beyond
/// Kubernetes' native i64/f64 domains so admission still enforces the Rust
/// type's range.
fn normalize_numeric_formats(schema: &mut serde_json::Value) {
    match schema {
        serde_json::Value::Object(fields) => {
            let numeric_format = fields
                .get("format")
                .and_then(serde_json::Value::as_str)
                .and_then(|format| match format {
                    // Kubernetes already models `number` as f64 and `integer`
                    // as signed i64. Only narrower and unsigned Rust types
                    // need extra bounds after their ignored format is removed.
                    "double" => Some(("number", None, None)),
                    "int16" => Some(("integer", Some(-32_768.0), Some(32_767.0))),
                    "int32" => Some(("integer", Some(-2_147_483_648.0), Some(2_147_483_647.0))),
                    "int64" => Some(("integer", None, None)),
                    "uint" | "uint64" => Some(("integer", Some(0.0), None)),
                    "uint16" => Some(("integer", Some(0.0), Some(65_535.0))),
                    "uint32" => Some(("integer", Some(0.0), Some(4_294_967_295.0))),
                    _ => None,
                });
            if let Some((expected_type, minimum, maximum)) = numeric_format
                && fields.get("type").and_then(serde_json::Value::as_str) == Some(expected_type)
            {
                if let Some(minimum) = minimum
                    && fields.get("minimum").is_none_or(serde_json::Value::is_null)
                {
                    fields.insert("minimum".to_owned(), serde_json::json!(minimum));
                }
                if let Some(maximum) = maximum
                    && fields.get("maximum").is_none_or(serde_json::Value::is_null)
                {
                    fields.insert("maximum".to_owned(), serde_json::json!(maximum));
                }
                fields.remove("format");
            }

            // Only descend through JSON Schema child locations. Values under
            // `default`, `enum`, or `example` are user data and may themselves
            // legitimately contain keys named `type` and `format`.
            for child_name in [
                "additionalItems",
                "additionalProperties",
                "allOf",
                "anyOf",
                "items",
                "not",
                "oneOf",
            ] {
                if let Some(child) = fields.get_mut(child_name) {
                    normalize_numeric_formats(child);
                }
            }
            for map_name in [
                "definitions",
                "dependencies",
                "patternProperties",
                "properties",
            ] {
                if let Some(serde_json::Value::Object(children)) = fields.get_mut(map_name) {
                    children.values_mut().for_each(normalize_numeric_formats);
                }
            }
        }
        serde_json::Value::Array(values) => values.iter_mut().for_each(normalize_numeric_formats),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn writes_kafka_pool_topic_and_user_crd_files() {
        let dir = tempdir().unwrap();
        write_all(dir.path()).unwrap();
        for (file, plural, short_name) in [
            ("crabka.io_kafkas.yaml", "plural: kafkas", None),
            (
                "crabka.io_kafkanodepools.yaml",
                "plural: kafkanodepools",
                Some("- knp"),
            ),
            (
                "crabka.io_kafkatopics.yaml",
                "plural: kafkatopics",
                Some("- kt"),
            ),
            (
                "crabka.io_kafkausers.yaml",
                "plural: kafkausers",
                Some("- ku"),
            ),
            (
                "crabka.io_kafkarebalances.yaml",
                "plural: kafkarebalances",
                Some("- kr"),
            ),
            (
                "crabka.io_kafkagrpcgateways.yaml",
                "plural: kafkagrpcgateways",
                Some("- kgg"),
            ),
            (
                "crabka.io_schemaregistries.yaml",
                "plural: schemaregistries",
                Some("- sr"),
            ),
            ("crabka.io_greses.yaml", "plural: greses", Some("- gg")),
            (
                "crabka.io_grestenants.yaml",
                "plural: grestenants",
                Some("- gt"),
            ),
        ] {
            let path = dir.path().join(file);
            assert!(path.exists(), "case {file:?}");
            let yaml = std::fs::read_to_string(&path).unwrap();
            assert!(yaml.contains(plural), "case {file:?}");
            assert!(
                !yaml.lines().any(|line| matches!(
                    line.trim(),
                    "format: double"
                        | "format: int16"
                        | "format: int32"
                        | "format: int64"
                        | "format: uint"
                        | "format: uint16"
                        | "format: uint32"
                        | "format: uint64"
                )),
                "case {file:?}"
            );
            if file == "crabka.io_kafkagrpcgateways.yaml" {
                let document: serde_json::Value = serde_yaml::from_str(&yaml).unwrap();
                let ready_replicas = "/spec/versions/0/schema/openAPIV3Schema/properties/status/properties/readyReplicas";
                assert!(
                    document.pointer(&format!("{ready_replicas}/minimum"))
                        == Some(&json!(-2_147_483_648.0))
                );
                assert!(
                    document.pointer(&format!("{ready_replicas}/maximum"))
                        == Some(&json!(2_147_483_647.0))
                );
            }
            if let Some(short) = short_name {
                assert!(yaml.contains(short), "case {file:?}");
            }
        }
    }

    #[test]
    fn normalizes_numeric_formats_without_weakening_ranges() {
        for (format, schema_type, minimum, maximum) in [
            ("double", "number", None, None),
            ("int16", "integer", Some(-32_768.0), Some(32_767.0)),
            (
                "int32",
                "integer",
                Some(-2_147_483_648.0),
                Some(2_147_483_647.0),
            ),
            ("int64", "integer", None, None),
            ("uint", "integer", Some(0.0), None),
            ("uint16", "integer", Some(0.0), Some(65_535.0)),
            ("uint32", "integer", Some(0.0), Some(4_294_967_295.0)),
            ("uint64", "integer", Some(0.0), None),
        ] {
            let mut schema = json!({ "type": schema_type, "format": format });
            normalize_numeric_formats(&mut schema);
            assert!(schema.get("format").is_none(), "format {format:?}");
            assert!(
                schema.get("minimum").and_then(serde_json::Value::as_f64) == minimum,
                "format {format:?}"
            );
            assert!(
                schema.get("maximum").and_then(serde_json::Value::as_f64) == maximum,
                "format {format:?}"
            );
        }

        let mut nested = json!({
            "allOf": [{
                "properties": {
                    "bounded": {
                        "type": "integer",
                        "format": "int32",
                        "minimum": 1.0,
                        "maximum": 7.0
                    }
                }
            }],
            "default": { "type": "integer", "format": "int32" }
        });
        normalize_numeric_formats(&mut nested);
        let bounded = &nested["allOf"][0]["properties"]["bounded"];
        assert!(bounded.get("format").is_none());
        assert!(bounded["minimum"] == 1.0);
        assert!(bounded["maximum"] == 7.0);
        assert!(nested["default"]["format"] == "int32");

        let mut timestamp = json!({ "type": "string", "format": "date-time" });
        normalize_numeric_formats(&mut timestamp);
        assert!(timestamp["format"] == "date-time");
    }
}
