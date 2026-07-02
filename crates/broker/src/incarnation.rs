//! Broker incarnation ID persistence.
//!
//! Each broker process invocation is identified by a UUID (`incarnation_id`).
//! The UUID is generated once on first boot, written to `{log_dir}/incarnation_id`
//! as a lowercase hex string, and reloaded on every subsequent start.

use std::{
    io::{Read, Write},
    path::Path,
};

use uuid::Uuid;

const FILE_NAME: &str = "incarnation_id";

/// Load the persisted incarnation ID from `{log_dir}/incarnation_id`, or
/// generate a fresh [`Uuid::new_v4`], persist it, and return it.
///
/// Any I/O error on the read path is treated as "file not present" and a new
/// UUID is generated. Failure to persist is logged as a warning; the ephemeral
/// UUID is returned regardless (next restart will get a different UUID).
pub fn load_or_generate(log_dir: &Path) -> Uuid {
    let path = log_dir.join(FILE_NAME);
    if let Ok(mut f) = std::fs::File::open(&path) {
        let mut s = String::new();
        if f.read_to_string(&mut s).is_ok()
            && let Ok(id) = s.trim().parse::<Uuid>()
        {
            return id;
        }
    }
    let id = Uuid::new_v4();
    match std::fs::File::create(&path) {
        Ok(mut f) => {
            if let Err(e) = f.write_all(id.to_string().as_bytes()) {
                tracing::warn!(path = %path.display(), error = %e, "failed to persist incarnation_id");
            }
        }
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "failed to create incarnation_id file");
        }
    }
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_uuid_when_file_absent() {
        let dir = tempfile::tempdir().unwrap();
        let id = load_or_generate(dir.path());
        assert!(!id.is_nil(), "generated UUID must not be nil");
    }

    #[test]
    fn persists_and_reloads_uuid() {
        let dir = tempfile::tempdir().unwrap();
        let id1 = load_or_generate(dir.path());
        let id2 = load_or_generate(dir.path());
        assert_eq!(id1, id2, "UUID must be stable across reloads");
    }
}
