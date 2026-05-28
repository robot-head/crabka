//! `crabka format` subcommand.
//!
//! Writes bootstrap metadata for a fresh broker:
//! - a randomly-generated (or operator-supplied) cluster id
//! - any seed SCRAM credentials supplied via `--add-scram`
//!
//! ## Bootstrap output format
//!
//! Rather than writing a real raft-log snapshot directly (the bootstrap
//! state is currently tangled with the live openraft engine's
//! `initialize` call), this command writes a placeholder manifest listing
//! the records the broker should pre-load. The output is:
//!
//! - `<log_dir>/bootstrap.json` — a human-readable manifest with the
//!   cluster id and a base64'd `serde_wincode` blob per metadata record.
//! - `<log_dir>/bootstrap.records.bin` — the same records concatenated
//!   as length-prefixed `serde_wincode<SerdeCompat<MetadataRecord>>`
//!   payloads, so the broker can stream them without touching JSON.

use std::path::PathBuf;

use clap::Args;
use crabka_metadata::{AclEntry, MetadataRecord, ScramCredentialRecord};
use crabka_security::SaslMechanism;
use crabka_security::scram::hash_scram_password_with_salt;
use ring::rand::{SecureRandom, SystemRandom};
use serde::Serialize;
use serde_wincode::SerdeCompat;
use uuid::Uuid;
use wincode::Serialize as _;

/// Exit codes:
/// - 0: success
/// - 2: iterations < 4096
/// - 3: `log_dir` non-empty
/// - 4: bootstrap write failure
const EXIT_OK: i32 = 0;
const EXIT_LOW_ITERATIONS: i32 = 2;
const EXIT_DIRTY_LOG_DIR: i32 = 3;
const EXIT_BOOTSTRAP_FAIL: i32 = 4;

/// SCRAM iteration floor — matches Kafka's recommended minimum and the
/// `AlterUserScramCredentials` handler's pre-check.
const MIN_SCRAM_ITERATIONS: u32 = 4096;

#[derive(Args, Debug)]
pub struct FormatArgs {
    /// Directory to format. Must be empty or non-existent.
    #[arg(long)]
    log_dir: PathBuf,
    /// Cluster id. Generated if not provided.
    #[arg(long)]
    cluster_id: Option<Uuid>,
    /// Seed a SCRAM credential. May be repeated.
    /// Format: `SCRAM-SHA-256=[name=<u>,password=<p>,iterations=<n>]`
    /// or `SCRAM-SHA-512=[name=<u>,password=<p>,iterations=<n>]`
    /// (iterations defaults to 4096 when omitted)
    #[arg(long, value_parser = parse_scram_spec)]
    add_scram: Vec<ScramSpec>,
    /// Seed an ACL entry. May be repeated.
    /// Format: `principal=User:<name>,host=<ip|*>,operation=<Op>,permission=<Allow|Deny>,resource=<Type>:<Name>[:<Pattern>]`
    /// Pattern defaults to `Literal`.
    #[arg(long, value_parser = parse_acl_spec)]
    add_acl: Vec<AclEntry>,
}

#[derive(Debug, Clone)]
pub struct ScramSpec {
    mechanism: SaslMechanism,
    name: String,
    password: String,
    iterations: u32,
}

fn parse_scram_spec(s: &str) -> Result<ScramSpec, String> {
    let s = s.trim();
    let (mechanism, body) = if let Some(rest) = s.strip_prefix("SCRAM-SHA-512=[") {
        (SaslMechanism::ScramSha512, rest)
    } else if let Some(rest) = s.strip_prefix("SCRAM-SHA-256=[") {
        (SaslMechanism::ScramSha256, rest)
    } else {
        return Err("must start with SCRAM-SHA-256=[ or SCRAM-SHA-512=[".into());
    };
    let body = body.strip_suffix(']').ok_or("must end with ]")?;
    let mut name = None;
    let mut password = None;
    let mut iterations: u32 = 4096;
    for attr in body.split(',') {
        let (k, v) = attr
            .split_once('=')
            .ok_or_else(|| format!("malformed attr: {attr}"))?;
        match k.trim() {
            "name" => name = Some(v.trim().to_string()),
            "password" => password = Some(v.trim().to_string()),
            "iterations" => {
                iterations = v.trim().parse().map_err(|e| format!("iterations: {e}"))?;
            }
            other => return Err(format!("unknown attr: {other}")),
        }
    }
    Ok(ScramSpec {
        mechanism,
        name: name.ok_or("missing name")?,
        password: password.ok_or("missing password")?,
        iterations,
    })
}

fn parse_acl_spec(spec: &str) -> Result<AclEntry, String> {
    use crabka_metadata::{AclOperation, PatternType, PermissionType, ResourceType};

    let mut principal = None;
    let mut host = None;
    let mut operation = None;
    let mut permission = None;
    let mut resource_type = None;
    let mut resource_name = None;
    let mut pattern_type = PatternType::Literal;

    for kv in spec.split(',') {
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| format!("malformed pair: {kv}"))?;
        match k {
            "principal" => principal = Some(v.to_string()),
            "host" => host = Some(v.to_string()),
            "operation" => {
                operation = Some(match v {
                    "All" => AclOperation::All,
                    "Read" => AclOperation::Read,
                    "Write" => AclOperation::Write,
                    "Create" => AclOperation::Create,
                    "Delete" => AclOperation::Delete,
                    "Alter" => AclOperation::Alter,
                    "Describe" => AclOperation::Describe,
                    "ClusterAction" => AclOperation::ClusterAction,
                    "DescribeConfigs" => AclOperation::DescribeConfigs,
                    "AlterConfigs" => AclOperation::AlterConfigs,
                    "IdempotentWrite" => AclOperation::IdempotentWrite,
                    other => return Err(format!("unknown operation: {other}")),
                });
            }
            "permission" => {
                permission = Some(match v {
                    "Allow" => PermissionType::Allow,
                    "Deny" => PermissionType::Deny,
                    other => return Err(format!("unknown permission: {other}")),
                });
            }
            "resource" => {
                let mut parts = v.splitn(3, ':');
                let rt = parts.next().ok_or("missing resource type")?;
                let rn = parts.next().ok_or("missing resource name")?;
                if let Some(pt) = parts.next() {
                    pattern_type = match pt {
                        "Literal" => PatternType::Literal,
                        "Prefixed" => PatternType::Prefixed,
                        other => return Err(format!("unknown pattern: {other}")),
                    };
                }
                resource_type = Some(match rt {
                    "Topic" => ResourceType::Topic,
                    "Group" => ResourceType::Group,
                    "Cluster" => ResourceType::Cluster,
                    "TransactionalId" => ResourceType::TransactionalId,
                    other => return Err(format!("unknown resource type: {other}")),
                });
                resource_name = Some(rn.to_string());
            }
            other => return Err(format!("unknown key: {other}")),
        }
    }

    Ok(AclEntry {
        resource_type: resource_type.ok_or("resource required")?,
        resource_name: resource_name.ok_or("resource_name required")?,
        pattern_type,
        principal: principal.ok_or("principal required")?,
        host: host.ok_or("host required")?,
        operation: operation.ok_or("operation required")?,
        permission_type: permission.ok_or("permission required")?,
    })
}

/// Human-readable manifest written to `<log_dir>/bootstrap.json`.
#[derive(Debug, Serialize)]
struct BootstrapManifest {
    /// Schema version of this bootstrap manifest. Bumped if the layout
    /// changes; the broker's future consumer will reject unknown values.
    schema: u32,
    cluster_id: Uuid,
    record_count: usize,
    /// Base64-encoded `SerdeCompat<MetadataRecord>` payloads, one per
    /// seed record. Mirrors the contents of `bootstrap.records.bin` so
    /// operators can inspect the file without a hex editor.
    records_b64: Vec<String>,
}

// `async` matches the entry point in `main.rs`; the body is sync today
// (purely fs + crypto) but a real raft-log bootstrap would await tokio I/O.
#[allow(clippy::unused_async)]
pub async fn run(args: FormatArgs) -> i32 {
    // Refuse to overwrite a non-empty directory. We treat "exists with
    // any entry" as non-empty; an empty dir or missing path is OK.
    if args.log_dir.exists() {
        match std::fs::read_dir(&args.log_dir) {
            Ok(mut it) => {
                if it.next().is_some() {
                    eprintln!(
                        "crabka format: refusing to overwrite non-empty log_dir {}",
                        args.log_dir.display(),
                    );
                    return EXIT_DIRTY_LOG_DIR;
                }
            }
            Err(e) => {
                eprintln!(
                    "crabka format: cannot read log_dir {}: {e}",
                    args.log_dir.display(),
                );
                return EXIT_BOOTSTRAP_FAIL;
            }
        }
    }

    if let Err(e) = std::fs::create_dir_all(&args.log_dir) {
        eprintln!(
            "crabka format: cannot create log_dir {}: {e}",
            args.log_dir.display(),
        );
        return EXIT_BOOTSTRAP_FAIL;
    }

    let cluster_id = args.cluster_id.unwrap_or_else(Uuid::new_v4);

    // Build the seed records. Each `--add-scram` is hashed *here* (CLI
    // side) using `hash_scram_password_with_salt` from `crabka-security`
    // so the on-disk record carries the stretched keys, never the plain
    // password.
    let mut records: Vec<MetadataRecord> = Vec::with_capacity(args.add_scram.len());
    for spec in &args.add_scram {
        if spec.iterations < MIN_SCRAM_ITERATIONS {
            eprintln!(
                "crabka format: iterations must be >= {MIN_SCRAM_ITERATIONS}, got {} for user {}",
                spec.iterations, spec.name,
            );
            return EXIT_LOW_ITERATIONS;
        }
        let mut salt = vec![0u8; 16];
        if let Err(e) = SystemRandom::new().fill(&mut salt) {
            eprintln!("crabka format: rng failure: {e}");
            return EXIT_BOOTSTRAP_FAIL;
        }
        let cred = hash_scram_password_with_salt(
            spec.password.as_bytes(),
            spec.mechanism,
            spec.iterations,
            salt,
        );
        records.push(MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: spec.name.clone(),
            mechanism: spec.mechanism,
            salt: cred.salt,
            stored_key: cred.stored_key,
            server_key: cred.server_key,
            iterations: cred.iterations,
        }));
    }

    for acl in args.add_acl {
        records.push(MetadataRecord::V1AccessControlEntry(acl));
    }

    if let Err(e) = write_bootstrap_files(&args.log_dir, cluster_id, &records) {
        eprintln!("crabka format: bootstrap failed: {e}");
        return EXIT_BOOTSTRAP_FAIL;
    }

    println!(
        "Formatted {} with cluster-id {} ({} seed record(s))",
        args.log_dir.display(),
        cluster_id,
        records.len(),
    );
    EXIT_OK
}

/// Serialize the manifest + records to disk under `log_dir`. Returns the
/// first I/O or encoding error encountered.
fn write_bootstrap_files(
    log_dir: &std::path::Path,
    cluster_id: Uuid,
    records: &[MetadataRecord],
) -> Result<(), String> {
    // 1. Per-record `SerdeCompat<MetadataRecord>` payloads.
    let mut record_blobs: Vec<Vec<u8>> = Vec::with_capacity(records.len());
    for rec in records {
        let bytes = <SerdeCompat<MetadataRecord>>::serialize(rec)
            .map_err(|e| format!("serialize record: {e}"))?;
        record_blobs.push(bytes);
    }

    // 2. Binary stream: length-prefixed (u32 LE) blobs, concatenated.
    let mut bin = Vec::new();
    for blob in &record_blobs {
        let len: u32 = u32::try_from(blob.len())
            .map_err(|_| format!("record too large: {} bytes", blob.len()))?;
        bin.extend_from_slice(&len.to_le_bytes());
        bin.extend_from_slice(blob);
    }
    std::fs::write(log_dir.join("bootstrap.records.bin"), &bin)
        .map_err(|e| format!("write bootstrap.records.bin: {e}"))?;

    // 3. Manifest JSON (cluster id + base64 mirrors of each blob).
    let records_b64: Vec<String> = record_blobs.iter().map(|b| base64_encode(b)).collect();
    let manifest = BootstrapManifest {
        schema: 1,
        cluster_id,
        record_count: records.len(),
        records_b64,
    };
    let json =
        serde_json::to_string_pretty(&manifest).map_err(|e| format!("serialize manifest: {e}"))?;
    std::fs::write(log_dir.join("bootstrap.json"), json)
        .map_err(|e| format!("write bootstrap.json: {e}"))?;

    Ok(())
}

/// Tiny self-contained base64 encoder (standard alphabet, padded). We
/// don't pull in the `base64` crate just for the manifest mirror — the
/// records are only base64'd for human readability; the authoritative
/// copy lives in `bootstrap.records.bin`.
fn base64_encode(input: &[u8]) -> String {
    const ALPHA: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    let mut iter = input.chunks_exact(3);
    for chunk in iter.by_ref() {
        let n = (u32::from(chunk[0]) << 16) | (u32::from(chunk[1]) << 8) | u32::from(chunk[2]);
        out.push(ALPHA[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHA[((n >> 12) & 0x3f) as usize] as char);
        out.push(ALPHA[((n >> 6) & 0x3f) as usize] as char);
        out.push(ALPHA[(n & 0x3f) as usize] as char);
    }
    let rem = iter.remainder();
    match rem.len() {
        0 => {}
        1 => {
            let n = u32::from(rem[0]) << 16;
            out.push(ALPHA[((n >> 18) & 0x3f) as usize] as char);
            out.push(ALPHA[((n >> 12) & 0x3f) as usize] as char);
            out.push('=');
            out.push('=');
        }
        2 => {
            let n = (u32::from(rem[0]) << 16) | (u32::from(rem[1]) << 8);
            out.push(ALPHA[((n >> 18) & 0x3f) as usize] as char);
            out.push(ALPHA[((n >> 12) & 0x3f) as usize] as char);
            out.push(ALPHA[((n >> 6) & 0x3f) as usize] as char);
            out.push('=');
        }
        _ => unreachable!("chunks_exact(3) remainder is 0..3"),
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_scram_spec_happy_path() {
        let spec = parse_scram_spec("SCRAM-SHA-512=[name=alice,password=hunter2,iterations=8192]")
            .unwrap();
        assert_eq!(spec.name, "alice");
        assert_eq!(spec.password, "hunter2");
        assert_eq!(spec.iterations, 8192);
        assert_eq!(spec.mechanism, SaslMechanism::ScramSha512);
    }

    #[test]
    fn parse_scram_spec_iterations_default() {
        let spec = parse_scram_spec("SCRAM-SHA-512=[name=bob,password=p]").unwrap();
        assert_eq!(spec.iterations, 4096);
    }

    #[test]
    fn parse_scram_spec_sha256_prefix() {
        let spec = parse_scram_spec("SCRAM-SHA-256=[name=alice,password=hunter2,iterations=8192]")
            .unwrap();
        assert_eq!(spec.name, "alice");
        assert_eq!(spec.mechanism, SaslMechanism::ScramSha256);
    }

    #[test]
    fn parse_scram_spec_rejects_missing_prefix() {
        assert!(parse_scram_spec("PLAIN=[name=a,password=b]").is_err());
    }

    #[test]
    fn parse_scram_spec_rejects_missing_name() {
        assert!(parse_scram_spec("SCRAM-SHA-512=[password=p,iterations=4096]").is_err());
    }

    #[test]
    fn parse_scram_spec_rejects_unknown_attr() {
        assert!(parse_scram_spec("SCRAM-SHA-512=[name=a,password=b,foo=bar]").is_err());
    }

    #[test]
    fn parse_acl_spec_minimal() {
        let s = "principal=User:admin,host=*,operation=All,permission=Allow,resource=Cluster:kafka-cluster";
        let entry = parse_acl_spec(s).unwrap();
        assert_eq!(entry.resource_type, crabka_metadata::ResourceType::Cluster);
        assert_eq!(entry.resource_name, "kafka-cluster");
        assert_eq!(entry.pattern_type, crabka_metadata::PatternType::Literal);
        assert_eq!(entry.principal, "User:admin");
        assert_eq!(entry.operation, crabka_metadata::AclOperation::All);
        assert_eq!(
            entry.permission_type,
            crabka_metadata::PermissionType::Allow
        );
    }

    #[test]
    fn parse_acl_spec_with_prefixed_pattern() {
        let s = "principal=User:alice,host=*,operation=Read,permission=Allow,resource=Topic:team-:Prefixed";
        let entry = parse_acl_spec(s).unwrap();
        assert_eq!(entry.pattern_type, crabka_metadata::PatternType::Prefixed);
        assert_eq!(entry.resource_name, "team-");
    }

    #[test]
    fn parse_acl_spec_unknown_key_errors() {
        let s = "principal=User:admin,host=*,bogus=x";
        assert!(parse_acl_spec(s).is_err());
    }

    #[test]
    fn base64_encode_known_vectors() {
        // RFC 4648 §10
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }
}
