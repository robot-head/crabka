//! Pure-logic helper: sum the regular-file sizes inside a partition
//! directory. Returns 0 for a missing directory (treated as "not yet
//! materialized", not an error) and propagates IO errors for other
//! failure modes.

use std::{fs, io, path::Path};

pub fn sum_partition_dir(path: &Path) -> Result<u64, io::Error> {
    let entries = match fs::read_dir(path) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    let mut total: u64 = 0;
    for entry in entries {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_file() {
            total = total.saturating_add(meta.len());
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use assert2::assert;

    use super::*;

    #[test]
    fn empty_dir_returns_zero() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(sum_partition_dir(tmp.path()).unwrap() == 0);
    }

    #[test]
    fn missing_dir_returns_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("nope");
        assert!(sum_partition_dir(&missing).unwrap() == 0);
    }

    fn write_file(path: &Path, bytes: &[u8]) {
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(bytes).unwrap();
        // Drop closes the handle so Windows updates the directory metadata
        // before `sum_partition_dir` walks it.
    }

    #[test]
    fn sums_regular_files() {
        let tmp = tempfile::tempdir().unwrap();
        write_file(&tmp.path().join("00000000000000000000.log"), &[0u8; 1024]);
        write_file(&tmp.path().join("00000000000000000000.index"), &[0u8; 128]);
        write_file(&tmp.path().join("leader-epoch-checkpoint"), &[0u8; 32]);
        assert!(sum_partition_dir(tmp.path()).unwrap() == 1024 + 128 + 32);
    }

    #[test]
    fn ignores_subdirectories() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir(tmp.path().join("subdir")).unwrap();
        write_file(&tmp.path().join("subdir/inner.log"), &[0u8; 999]);
        write_file(&tmp.path().join("top.log"), &[0u8; 100]);
        assert!(sum_partition_dir(tmp.path()).unwrap() == 100);
    }
}
