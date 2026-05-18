//! Pure-logic helper: sum the regular-file sizes inside a partition
//! directory. Returns 0 for a missing directory (treated as "not yet
//! materialized", not an error) and propagates IO errors for other
//! failure modes.

use std::fs;
use std::io;
use std::path::Path;

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
    use super::*;
    use std::io::Write;

    #[test]
    fn empty_dir_returns_zero() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(sum_partition_dir(tmp.path()).unwrap(), 0);
    }

    #[test]
    fn missing_dir_returns_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("nope");
        assert_eq!(sum_partition_dir(&missing).unwrap(), 0);
    }

    #[test]
    fn sums_regular_files() {
        let tmp = tempfile::tempdir().unwrap();
        let mut f1 = std::fs::File::create(tmp.path().join("00000000000000000000.log")).unwrap();
        f1.write_all(&[0u8; 1024]).unwrap();
        let mut f2 = std::fs::File::create(tmp.path().join("00000000000000000000.index")).unwrap();
        f2.write_all(&[0u8; 128]).unwrap();
        let mut f3 = std::fs::File::create(tmp.path().join("leader-epoch-checkpoint")).unwrap();
        f3.write_all(&[0u8; 32]).unwrap();
        assert_eq!(sum_partition_dir(tmp.path()).unwrap(), 1024 + 128 + 32);
    }

    #[test]
    fn ignores_subdirectories() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir(tmp.path().join("subdir")).unwrap();
        let mut f = std::fs::File::create(tmp.path().join("subdir/inner.log")).unwrap();
        f.write_all(&[0u8; 999]).unwrap();
        let mut top = std::fs::File::create(tmp.path().join("top.log")).unwrap();
        top.write_all(&[0u8; 100]).unwrap();
        // Only `top.log` counted; the subdir is not recursed.
        assert_eq!(sum_partition_dir(tmp.path()).unwrap(), 100);
    }
}
