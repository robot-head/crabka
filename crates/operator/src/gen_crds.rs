use std::fs;
use std::path::Path;

use crate::crd::{Kafka, KafkaNodePool};

/// Write every CRD this operator owns into `out_dir` as
/// `<group>_<plural>.yaml`. Existing files are overwritten.
pub fn write_all(out_dir: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(out_dir)?;
    write_one::<Kafka>(out_dir)?;
    write_one::<KafkaNodePool>(out_dir)?;
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
    use tempfile::tempdir;

    #[test]
    fn writes_kafka_and_pool_crd_files() {
        let dir = tempdir().unwrap();
        write_all(dir.path()).unwrap();
        let kf = dir.path().join("crabka.io_kafkas.yaml");
        let pf = dir.path().join("crabka.io_kafkanodepools.yaml");
        assert!(kf.exists());
        assert!(pf.exists());
        let kafka = std::fs::read_to_string(&kf).unwrap();
        assert!(kafka.contains("plural: kafkas"));
        let pool = std::fs::read_to_string(&pf).unwrap();
        assert!(pool.contains("plural: kafkanodepools"));
        assert!(pool.contains("- knp"));
    }
}
