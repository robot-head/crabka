use std::fs;
use std::path::Path;

use crate::crd::Kafka;

/// Write every CRD this operator owns into `out_dir` as
/// `<group>_<plural>.yaml`. Existing files are overwritten.
pub fn write_all(out_dir: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(out_dir)?;
    write_one::<Kafka>(out_dir)?;
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
    fn writes_kafka_crd_file() {
        let dir = tempdir().unwrap();
        write_all(dir.path()).unwrap();
        let f = dir.path().join("crabka.io_kafkas.yaml");
        assert!(f.exists());
        let content = std::fs::read_to_string(&f).unwrap();
        assert!(content.contains("kind: CustomResourceDefinition"));
        assert!(content.contains("group: crabka.io"));
        assert!(content.contains("plural: kafkas"));
    }
}
