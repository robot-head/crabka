use std::fs;
use std::path::Path;

use crate::crd::{Kafka, KafkaNodePool, KafkaRebalance, KafkaTopic, KafkaUser, SchemaRegistry};

/// Write every CRD this operator owns into `out_dir` as
/// `<group>_<plural>.yaml`. Existing files are overwritten.
pub fn write_all(out_dir: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(out_dir)?;
    write_one::<Kafka>(out_dir)?;
    write_one::<KafkaNodePool>(out_dir)?;
    write_one::<KafkaTopic>(out_dir)?;
    write_one::<KafkaUser>(out_dir)?;
    write_one::<KafkaRebalance>(out_dir)?;
    write_one::<SchemaRegistry>(out_dir)?;
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
    use super::*;
    use assert2::assert;
    use tempfile::tempdir;

    #[test]
    fn writes_kafka_pool_topic_and_user_crd_files() {
        let dir = tempdir().unwrap();
        write_all(dir.path()).unwrap();
        let kf = dir.path().join("crabka.io_kafkas.yaml");
        let pf = dir.path().join("crabka.io_kafkanodepools.yaml");
        let tf = dir.path().join("crabka.io_kafkatopics.yaml");
        let uf = dir.path().join("crabka.io_kafkausers.yaml");
        let rf = dir.path().join("crabka.io_kafkarebalances.yaml");
        assert!(kf.exists());
        assert!(pf.exists());
        assert!(tf.exists());
        assert!(uf.exists());
        assert!(rf.exists());
        let kafka = std::fs::read_to_string(&kf).unwrap();
        assert!(kafka.contains("plural: kafkas"));
        let pool = std::fs::read_to_string(&pf).unwrap();
        assert!(pool.contains("plural: kafkanodepools"));
        assert!(pool.contains("- knp"));
        let topic = std::fs::read_to_string(&tf).unwrap();
        assert!(topic.contains("plural: kafkatopics"));
        assert!(topic.contains("- kt"));
        let user = std::fs::read_to_string(&uf).unwrap();
        assert!(user.contains("plural: kafkausers"));
        assert!(user.contains("- ku"));
        let rebalance = std::fs::read_to_string(&rf).unwrap();
        assert!(rebalance.contains("plural: kafkarebalances"));
        assert!(rebalance.contains("- kr"));
        let sf = dir.path().join("crabka.io_schemaregistries.yaml");
        assert!(sf.exists());
        let sr = std::fs::read_to_string(&sf).unwrap();
        assert!(sr.contains("plural: schemaregistries"));
        assert!(sr.contains("- sr"));
    }
}
