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
use crabka_metadata::{
    AclEntry, FeatureLevelRecord, KRaftVersionRange, KRaftVersionRecord, MetadataRecord,
    ScramCredentialRecord, Voter, VoterEndpoint, VoterSet, VotersRecord,
};
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
    /// Bootstrap `metadata.version` (KIP-778), e.g. `4.0` or `4.0-IV3`.
    /// Defaults to the broker's maximum supported level when omitted.
    #[arg(long)]
    release_version: Option<String>,
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
    /// This node's raft id. Required with `--standalone` (KIP-853 needs
    /// to know which voter this node *is* when seeding the singleton set).
    #[arg(long)]
    node_id: Option<crabka_metadata::NodeId>,
    /// Format this node as the sole initial controller voter.
    #[arg(long, conflicts_with = "initial_controllers")]
    standalone: bool,
    /// Explicit initial controllers: `id@host:port:dir-uuid`, comma-separated.
    #[arg(long, value_delimiter = ',')]
    initial_controllers: Vec<String>,
    /// This node's controller listener (`host:port`) — written into the
    /// `VotersRecord` when `--standalone`.
    #[arg(long)]
    controller_listener: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ScramSpec {
    mechanism: SaslMechanism,
    name: String,
    password: String,
    iterations: u32,
}

/// Map a release string to a supported `metadata.version` feature level,
/// erroring if it is unknown or outside `[MIN, MAX]`.
fn resolve_release_level(s: &str) -> Result<i16, String> {
    let mv = crabka_metadata::metadata_version::from_version_string(s)
        .ok_or_else(|| format!("unknown metadata.version {s:?}"))?;
    let level = mv.feature_level();
    if !crabka_metadata::metadata_version::is_supported_level(level) {
        return Err(format!(
            "metadata.version {s:?} (level {level}) is outside the supported range"
        ));
    }
    Ok(level)
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

/// Parse one `--initial-controllers` entry: `id@host:port:dir-uuid`.
///
/// The directory uuid is the trailing colon-delimited field, so we split
/// it off the right first, then peel `host:port` off the remainder.
fn parse_initial_controller(spec: &str) -> Result<Voter, String> {
    let (id_part, rest) = spec.split_once('@').ok_or("missing '@'")?;
    let id: crabka_metadata::NodeId = id_part.parse().map_err(|_| "bad id")?;
    let (host_port, dir_part) = rest.rsplit_once(':').ok_or("missing directory uuid")?;
    let dir: Uuid = dir_part.parse().map_err(|_| "bad directory uuid")?;
    let (host, port) = host_port.rsplit_once(':').ok_or("missing host:port")?;
    let port: u16 = port.parse().map_err(|_| "bad port")?;
    Ok(Voter {
        id,
        directory_id: dir,
        endpoints: vec![VoterEndpoint {
            name: "CONTROLLER".into(),
            host: host.to_string(),
            port,
        }],
        kraft_version: KRaftVersionRange::default(),
    })
}

/// Derive the initial controller voter set from the format args.
///
/// - `--standalone`: a singleton set holding just this node (requires
///   `--node-id` + `--controller-listener`).
/// - `--initial-controllers`: the explicitly-listed voters.
/// - neither: an empty set — this node is a joiner that relies on auto-join
///   to enter an already-bootstrapped cluster.
fn build_initial_voters(args: &FormatArgs, directory_id: Uuid) -> Result<VoterSet, String> {
    if args.standalone {
        let id = args.node_id.ok_or("--standalone requires --node-id")?;
        let listener = args
            .controller_listener
            .as_deref()
            .ok_or("--standalone requires --controller-listener")?;
        let (host, port) = listener
            .rsplit_once(':')
            .ok_or("--controller-listener must be host:port")?;
        let port: u16 = port.parse().map_err(|_| "bad --controller-listener port")?;
        Ok(VoterSet::from_voters([Voter {
            id,
            directory_id,
            endpoints: vec![VoterEndpoint {
                name: "CONTROLLER".into(),
                host: host.to_string(),
                port,
            }],
            kraft_version: KRaftVersionRange::default(),
        }]))
    } else if !args.initial_controllers.is_empty() {
        let voters: Result<Vec<_>, _> = args
            .initial_controllers
            .iter()
            .map(|s| parse_initial_controller(s))
            .collect();
        Ok(VoterSet::from_voters(voters?))
    } else {
        Ok(VoterSet::default())
    }
}

/// Persist `meta.properties.json` — the broker recovers `directory_id`
/// from it on every boot (KIP-853 voter identity).
fn write_meta_properties(
    log_dir: &std::path::Path,
    cluster_id: Uuid,
    directory_id: Uuid,
) -> Result<(), String> {
    let meta = serde_json::json!({
        "cluster_id": cluster_id.to_string(),
        "directory_id": directory_id.to_string(),
        "version": 1,
    });
    let bytes = serde_json::to_vec_pretty(&meta)
        .map_err(|e| format!("serialize meta.properties.json: {e}"))?;
    std::fs::write(log_dir.join("meta.properties.json"), bytes)
        .map_err(|e| format!("write meta.properties.json: {e}"))
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
#[allow(clippy::unused_async, clippy::too_many_lines)]
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

    // KIP-853: generate + persist this replica's stable directory id. The
    // broker reads it back from `meta.properties.json` on every boot; it is
    // the identity component of every `Voter` this node ever appears as.
    let directory_id = Uuid::new_v4();
    if let Err(e) = write_meta_properties(&args.log_dir, cluster_id, directory_id) {
        eprintln!("crabka format: {e}");
        return EXIT_BOOTSTRAP_FAIL;
    }

    // KIP-853 dynamic-voter seed records. These lead the bootstrap record
    // stream (before SCRAM/ACL) so the controller's first committed batch
    // establishes the kraft.version + initial membership.
    let initial_voters = match build_initial_voters(&args, directory_id) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("crabka format: {e}");
            return EXIT_BOOTSTRAP_FAIL;
        }
    };
    let mut records: Vec<MetadataRecord> = Vec::new();
    records.push(MetadataRecord::V1KRaftVersion(KRaftVersionRecord {
        kraft_version: 1,
    }));
    if !initial_voters.is_empty() {
        records.push(MetadataRecord::V1Voters(VotersRecord {
            voters: initial_voters,
        }));
    }

    // KIP-778 bootstrap: every formatted cluster finalizes a real
    // metadata.version so the image never sits at MetadataVersion.UNKNOWN.
    let release = args
        .release_version
        .as_deref()
        .map(resolve_release_level)
        .transpose();
    let release_level = match release {
        Ok(Some(level)) => level,
        Ok(None) => crabka_metadata::metadata_version::METADATA_VERSION_MAX,
        Err(e) => {
            eprintln!("crabka format: {e}");
            return EXIT_BOOTSTRAP_FAIL;
        }
    };
    records.push(MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: crabka_metadata::metadata_version::METADATA_VERSION_FEATURE.to_string(),
        level: release_level,
    }));

    // Build the seed records. Each `--add-scram` is hashed *here* (CLI
    // side) using `hash_scram_password_with_salt` from `crabka-security`
    // so the on-disk record carries the stretched keys, never the plain
    // password.
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
    fn release_version_maps_to_feature_level() {
        assert_eq!(resolve_release_level("4.0").unwrap(), 25);
        assert_eq!(resolve_release_level("3.7-IV4").unwrap(), 19);
        assert!(resolve_release_level("2.8").is_err()); // below MIN / unknown
        assert!(resolve_release_level("9.9-IV0").is_err()); // unknown
    }

    #[test]
    fn default_release_level_is_max() {
        assert_eq!(
            resolve_release_level("4.0").unwrap(),
            crabka_metadata::metadata_version::METADATA_VERSION_MAX
        );
    }

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
    fn parses_initial_controller_spec() {
        let v =
            parse_initial_controller("3@host:9093:00000000-0000-0000-0000-000000000003").unwrap();
        assert_eq!(v.id, 3);
        assert_eq!(v.endpoints[0].name, "CONTROLLER");
        assert_eq!(v.endpoints[0].host, "host");
        assert_eq!(v.endpoints[0].port, 9093);
        assert_eq!(v.directory_id, Uuid::from_u128(3));
    }

    #[test]
    fn rejects_initial_controller_without_at() {
        assert!(parse_initial_controller("3:host:9093:uuid").is_err());
    }

    #[test]
    fn rejects_initial_controller_bad_uuid() {
        assert!(parse_initial_controller("3@host:9093:not-a-uuid").is_err());
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
