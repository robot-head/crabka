//! Slice 12b. Read `bootstrap.records.bin` (produced by
//! `crabka format --add-scram`) on broker first start.
//!
//! File framing (matches `crates/cli/src/format.rs`):
//!   [`u32_le` length][serde_wincode-encoded MetadataRecord]
//! Repeated until EOF.

use std::path::Path;

use crabka_metadata::MetadataRecord;
use serde_wincode::SerdeCompat;
use wincode::Deserialize;

use crate::error::BrokerError;

pub fn load_bootstrap_records(log_dir: &Path) -> Result<Vec<MetadataRecord>, BrokerError> {
    let path = log_dir.join("bootstrap.records.bin");
    if !path.exists() {
        return Ok(vec![]);
    }
    let bytes = std::fs::read(&path).map_err(|e| BrokerError::BootstrapFile {
        path: path.clone(),
        source: Box::new(e),
    })?;
    let mut out = Vec::new();
    let mut cur = &bytes[..];
    while !cur.is_empty() {
        if cur.len() < 4 {
            return Err(BrokerError::BootstrapFile {
                path: path.clone(),
                source: "truncated length prefix".into(),
            });
        }
        let len = u32::from_le_bytes([cur[0], cur[1], cur[2], cur[3]]) as usize;
        cur = &cur[4..];
        if cur.len() < len {
            return Err(BrokerError::BootstrapFile {
                path: path.clone(),
                source: "truncated record body".into(),
            });
        }
        let rec = <SerdeCompat<MetadataRecord>>::deserialize(&cur[..len]).map_err(|e| {
            BrokerError::BootstrapFile {
                path: path.clone(),
                source: Box::new(std::io::Error::other(format!("decode: {e}"))),
            }
        })?;
        out.push(rec);
        cur = &cur[len..];
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::ScramCredentialRecord;
    use crabka_security::SaslMechanism;
    use serde_wincode::SerdeCompat;
    use wincode::Serialize;

    fn write_frame(out: &mut Vec<u8>, rec: &MetadataRecord) {
        let bytes = <SerdeCompat<MetadataRecord>>::serialize(rec).unwrap();
        out.extend_from_slice(
            &u32::try_from(bytes.len())
                .expect("record too large for u32")
                .to_le_bytes(),
        );
        out.extend_from_slice(&bytes);
    }

    #[test]
    fn returns_empty_when_absent() {
        let dir = tempfile::tempdir().unwrap();
        let got = load_bootstrap_records(dir.path()).unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn decodes_v1_scram_credential() {
        let dir = tempfile::tempdir().unwrap();
        let rec = MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: SaslMechanism::ScramSha512,
            salt: vec![1; 16],
            stored_key: vec![2; 64],
            server_key: vec![3; 64],
            iterations: 4096,
        });
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &rec);
        std::fs::write(dir.path().join("bootstrap.records.bin"), &bytes).unwrap();
        let got = load_bootstrap_records(dir.path()).unwrap();
        assert_eq!(got.len(), 1);
        match &got[0] {
            MetadataRecord::V1ScramCredential(r) => assert_eq!(r.user, "alice"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn refuses_truncated_length_prefix() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("bootstrap.records.bin"), [0u8, 0u8, 0u8]).unwrap();
        let err = load_bootstrap_records(dir.path()).unwrap_err();
        assert!(matches!(err, BrokerError::BootstrapFile { .. }));
    }

    #[test]
    fn refuses_truncated_record_body() {
        let dir = tempfile::tempdir().unwrap();
        // Length prefix says 100 bytes follow; only write 4.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&100u32.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 4]);
        std::fs::write(dir.path().join("bootstrap.records.bin"), &bytes).unwrap();
        assert!(matches!(
            load_bootstrap_records(dir.path()),
            Err(BrokerError::BootstrapFile { .. })
        ));
    }

    #[test]
    fn refuses_undecodable_record() {
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = Vec::new();
        // Length prefix=8, body=random bytes that aren't valid bincode for MetadataRecord.
        bytes.extend_from_slice(&8u32.to_le_bytes());
        bytes.extend_from_slice(&[0xFFu8; 8]);
        std::fs::write(dir.path().join("bootstrap.records.bin"), &bytes).unwrap();
        assert!(matches!(
            load_bootstrap_records(dir.path()),
            Err(BrokerError::BootstrapFile { .. })
        ));
    }
}
