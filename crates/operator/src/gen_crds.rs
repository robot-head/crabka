use std::{fs, path::Path};

use crate::crd::{
    Gres, GresTenant, Kafka, KafkaConnector, KafkaGrpcGateway, KafkaNodePool, KafkaRebalance,
    KafkaTopic, KafkaUser, SchemaRegistry,
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
    write_one::<KafkaConnector>(out_dir)?;
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
    let crd = K::crd();
    let group = &crd.spec.group;
    let plural = &crd.spec.names.plural;
    let file = out_dir.join(format!("{group}_{plural}.yaml"));
    let yaml = serde_yaml::to_string(&crd)?;
    fs::write(&file, yaml)?;
    eprintln!("wrote {}", file.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
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
                "crabka.io_kafkaconnectors.yaml",
                "plural: kafkaconnectors",
                Some("- kc"),
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
            if let Some(short) = short_name {
                assert!(yaml.contains(short), "case {file:?}");
            }
        }
    }
}
