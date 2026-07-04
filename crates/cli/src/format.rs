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

use std::{collections::BTreeMap, path::PathBuf};

use clap::Args;
use crabka_metadata::{
    AclEntry, KRaftVersionRange, KRaftVersionRecord, MetadataRecord, ScramCredentialRecord, Voter,
    VoterEndpoint, VoterSet, VotersRecord,
};
use crabka_security::{SaslMechanism, scram::hash_scram_password_with_salt};
use ring::rand::{SecureRandom, SystemRandom};
use serde::Serialize;
use serde_wincode::SerdeCompat;
use uuid::Uuid;
use wincode::Serialize as _;

use crate::ids::{ClusterId, DirectoryId};

/// Exit codes:
/// - 0: success
/// - 2: iterations < 4096
/// - 3: `log_dir` non-empty
/// - 4: bootstrap write failure
const EXIT_OK: i32 = 0;
const EXIT_LOW_ITERATIONS: i32 = 2;
const EXIT_DIRTY_LOG_DIR: i32 = 3;
const EXIT_BOOTSTRAP_FAIL: i32 = 4;
const EXIT_INVALID_FEATURE: i32 = 5;

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
    /// Set an individual feature's finalized level at format time (KIP-1022),
    /// e.g. `--feature transaction.version=2`. May be repeated. Combines with
    /// `--release-version` (which sets the base release) for every feature
    /// except `metadata.version`, where the two conflict.
    #[arg(long = "feature", value_parser = parse_feature_spec)]
    feature: Vec<(String, i16)>,
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
    #[arg(long, value_parser = parse_node_id)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Parse a node id: a bare `u64` wrapped in the `NodeId` newtype.
fn parse_node_id(s: &str) -> Result<crabka_metadata::NodeId, String> {
    let id: u64 = s.trim().parse().map_err(|e| format!("node id: {e}"))?;
    Ok(crabka_metadata::NodeId(id))
}

/// Parse one `--feature NAME=LEVEL` spec into `(name, level)`.
fn parse_feature_spec(s: &str) -> Result<(String, i16), String> {
    let (name, level) = s
        .split_once('=')
        .ok_or("--feature must be NAME=LEVEL, e.g. transaction.version=2")?;
    let name = name.trim();
    if name.is_empty() {
        return Err("feature name must not be empty".into());
    }
    let level: i16 = level
        .trim()
        .parse()
        .map_err(|e| format!("feature level: {e}"))?;
    Ok((name.to_string(), level))
}

/// Resolve `crabka format`'s KIP-1022 feature flags into the bootstrap
/// `metadata.version` level and the per-feature override map, applying the
/// validation `kafka-storage format` performs:
///
/// - every `--feature` names a registered feature, finalized in its supported
///   range (else reject);
/// - `--feature metadata.version=X` conflicts with `--release-version`;
/// - `bootstrap_mv` = `--feature metadata.version` if set, else
///   `--release-version`, else the newest supported level (latest stable);
/// - the fully-resolved feature set satisfies every KIP-1022 dependency.
fn resolve_format_features(
    release_version: Option<&str>,
    features: &[(String, i16)],
) -> Result<(i16, BTreeMap<String, i16>), String> {
    use crabka_metadata::metadata_version::{METADATA_VERSION_FEATURE, METADATA_VERSION_MAX};

    let mut overrides: BTreeMap<String, i16> = BTreeMap::new();
    let mut feature_mv: Option<i16> = None;

    for (name, level) in features {
        let Some(feat) = crabka_metadata::feature(name) else {
            let mut known: Vec<&str> = crabka_metadata::feature_registry()
                .iter()
                .map(|f| f.name())
                .collect();
            known.sort_unstable();
            return Err(format!(
                "Unsupported feature: {name}. Supported features are: {}",
                known.join(", ")
            ));
        };
        let (min, max) = feat.supported_range();
        if *level < min || *level > max {
            return Err(format!(
                "feature {name}={level} is outside the supported range {min}..={max}"
            ));
        }
        if name == METADATA_VERSION_FEATURE {
            if release_version.is_some() {
                return Err(
                    "Use --release-version instead of --feature metadata.version=X to avoid ambiguity.".into(),
                );
            }
            feature_mv = Some(*level);
        }
        if overrides.insert(name.clone(), *level).is_some() {
            return Err(format!("feature {name} specified more than once"));
        }
    }

    let bootstrap_mv = if let Some(mv) = feature_mv {
        mv
    } else if let Some(rv) = release_version {
        resolve_release_level(rv)?
    } else {
        METADATA_VERSION_MAX
    };

    // KIP-1022 dependency validation over the fully-resolved feature set
    // (every registered feature at its override-or-default level).
    let resolved: BTreeMap<String, i16> = crabka_metadata::feature_registry()
        .iter()
        .map(|f| {
            let level = overrides
                .get(f.name())
                .copied()
                .unwrap_or_else(|| f.default_level(bootstrap_mv));
            (f.name().to_string(), level)
        })
        .collect();
    crabka_metadata::validate_feature_dependencies(&resolved)?;

    Ok((bootstrap_mv, overrides))
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
                    "TwoPhaseCommit" => AclOperation::TwoPhaseCommit,
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
    let id = crabka_metadata::NodeId(id_part.parse::<u64>().map_err(|_| "bad id")?);
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
fn build_initial_voters(args: &FormatArgs, directory_id: DirectoryId) -> Result<VoterSet, String> {
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
            // `Voter.directory_id` is a raw `Uuid` (owned by `crabka_voters`);
            // unwrap the newtype at this crate boundary.
            directory_id: directory_id.into(),
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
#[tracing::instrument(
    level = "debug",
    name = "cli.write_meta_properties",
    skip_all,
    fields(log_dir = %log_dir.display(), %cluster_id, %directory_id),
    err
)]
fn write_meta_properties(
    log_dir: &std::path::Path,
    cluster_id: ClusterId,
    directory_id: DirectoryId,
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
    // `ClusterId` is `#[serde(transparent)]`, so this serializes as the bare
    // UUID string exactly as the previous `Uuid` field did.
    cluster_id: ClusterId,
    record_count: usize,
    /// Base64-encoded `SerdeCompat<MetadataRecord>` payloads, one per
    /// seed record. Mirrors the contents of `bootstrap.records.bin` so
    /// operators can inspect the file without a hex editor.
    records_b64: Vec<String>,
}

// `async` matches the entry point in `main.rs`; the body is sync today
// (purely fs + crypto) but a real raft-log bootstrap would await tokio I/O.
// The body yields an `i32` (not a future), so `#[instrument]` is safe here
// w.r.t. `clippy::async_yields_async`.
#[tracing::instrument(
    level = "info",
    name = "cli.format",
    skip_all,
    fields(log_dir = %args.log_dir.display(), standalone = args.standalone)
)]
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

    let cluster_id = ClusterId(args.cluster_id.unwrap_or_else(Uuid::new_v4));

    // KIP-853: generate + persist this replica's stable directory id. The
    // broker reads it back from `meta.properties.json` on every boot; it is
    // the identity component of every `Voter` this node ever appears as.
    let directory_id = DirectoryId(Uuid::new_v4());
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

    // KIP-584 / KIP-778 / KIP-1022 bootstrap: finalize each registered feature
    // at its `--feature` override, else its per-release default for the
    // resolved bootstrap metadata.version (`--feature metadata.version` >
    // `--release-version` > latest stable). A 4.0 format thus seeds
    // metadata.version, group.version, etc. at their 4.0 defaults so a fresh
    // cluster engages each feature with no manual step; a level-0 feature is
    // omitted (absent = disabled), matching `kafka-storage format`.
    let (bootstrap_mv, feature_overrides) =
        match resolve_format_features(args.release_version.as_deref(), &args.feature) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("crabka format: {e}");
                return EXIT_INVALID_FEATURE;
            }
        };
    records.extend(crabka_metadata::bootstrap_feature_records_with_overrides(
        bootstrap_mv,
        &feature_overrides,
    ));

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
#[tracing::instrument(
    level = "debug",
    name = "cli.write_bootstrap_files",
    skip_all,
    fields(log_dir = %log_dir.display(), record_count = records.len()),
    err
)]
fn write_bootstrap_files(
    log_dir: &std::path::Path,
    cluster_id: ClusterId,
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
    use assert2::assert;

    use super::*;

    #[test]
    fn release_version_maps_to_feature_level() {
        for (input, want) in [
            ("4.0", Some(25)),
            ("3.7-IV4", Some(19)),
            ("2.8", None),     // below MIN / unknown
            ("9.9-IV0", None), // unknown
        ] {
            assert!(resolve_release_level(input).ok() == want);
        }
    }

    #[test]
    fn bootstrap_seeds_every_nonzero_feature_at_release_default() {
        let bootstrap_mv = crabka_metadata::metadata_version::from_version_string("4.0")
            .unwrap()
            .feature_level();
        // Exercises the exact helper `run()` uses, so it tracks the registry
        // as features are added in later tasks. Features whose release default
        // is 0 are omitted (KIP-1022: level 0 = absent = disabled), matching
        // `kafka-storage format`.
        let records = crabka_metadata::bootstrap_feature_records(bootstrap_mv);
        for feat in crabka_metadata::feature_registry() {
            let found = records.iter().find_map(|r| match r {
                MetadataRecord::V1FeatureLevel(f) if f.name == feat.name() => Some(f.level),
                _ => None,
            });
            let expected = feat.default_level(bootstrap_mv);
            if expected > 0 {
                assert!(
                    found == Some(expected),
                    "feature {} not seeded at its release default",
                    feat.name()
                );
            } else {
                assert!(
                    found.is_none(),
                    "feature {} defaults to 0 and must not be seeded",
                    feat.name()
                );
            }
        }
    }

    // The no-flag default path (`Ok(None) => METADATA_VERSION_MAX` in `run()`)
    // is covered end-to-end by `format_smoke.rs`, which formats without
    // `--release-version` and asserts the FeatureLevel record is present.
    #[test]
    fn max_version_string_resolves_to_max() {
        assert!(
            resolve_release_level("4.0").unwrap()
                == crabka_metadata::metadata_version::METADATA_VERSION_MAX
        );
    }

    #[test]
    fn parse_feature_spec_happy_path() {
        assert!(parse_feature_spec("group.version=1").unwrap() == ("group.version".to_string(), 1));
        assert!(
            parse_feature_spec("metadata.version=20").unwrap()
                == ("metadata.version".to_string(), 20)
        );
    }

    #[test]
    fn parse_feature_spec_error_branches() {
        for bad in [
            "noequals",          // missing '='
            "group.version=abc", // non-integer level
            "group.version=",    // empty level
            "=1",                // empty name
        ] {
            assert!(
                parse_feature_spec(bad).is_err(),
                "expected error for {bad:?}"
            );
        }
    }

    #[test]
    fn resolve_features_defaults_bootstrap_mv_to_max() {
        // No --release-version, no metadata.version override → bootstrap at MAX;
        // an explicit non-metadata feature becomes an override.
        let (mv, ov) =
            resolve_format_features(None, &[("group.version".into(), 1)]).expect("resolve");
        assert!(mv == crabka_metadata::metadata_version::METADATA_VERSION_MAX);
        assert!(ov.get("group.version") == Some(&1));
    }

    #[test]
    fn resolve_features_metadata_version_feature_sets_bootstrap_mv() {
        let (mv, ov) =
            resolve_format_features(None, &[("metadata.version".into(), 20)]).expect("resolve");
        assert!(mv == 20);
        assert!(ov.get("metadata.version") == Some(&20));
    }

    #[test]
    fn resolve_features_release_version_sets_bootstrap_mv() {
        let (mv, ov) = resolve_format_features(Some("4.0-IV0"), &[]).expect("resolve");
        assert!(mv == 22);
        assert!(ov.is_empty());
    }

    #[test]
    fn resolve_features_release_and_feature_combine() {
        // --release-version sets the base; a non-metadata --feature overrides it.
        let (mv, ov) =
            resolve_format_features(Some("4.0-IV0"), &[("transaction.version".into(), 2)])
                .expect("resolve");
        assert!(mv == 22);
        assert!(ov.get("transaction.version") == Some(&2));
    }

    #[test]
    fn resolve_features_rejects_release_plus_metadata_version_feature() {
        // Ambiguity: both --release-version and --feature metadata.version set MV.
        let err = resolve_format_features(Some("4.0-IV0"), &[("metadata.version".into(), 24)])
            .unwrap_err();
        assert!(err.contains("metadata.version"), "{err}");
    }

    #[test]
    fn resolve_features_rejects_unknown_feature() {
        let err = resolve_format_features(None, &[("bogus.version".into(), 1)]).unwrap_err();
        assert!(err.contains("Unsupported feature"), "{err}");
        assert!(err.contains("bogus.version"), "{err}");
    }

    #[test]
    fn resolve_features_rejects_out_of_range_level() {
        for (name, level) in [
            ("group.version", 5),     // group.version supports 0..=1
            ("metadata.version", 99), // metadata.version supports 7..=25
            ("metadata.version", 1),
        ] {
            assert!(
                resolve_format_features(None, &[(name.into(), level)]).is_err(),
                "expected error for {name}={level}"
            );
        }
    }

    #[test]
    fn resolve_features_rejects_bad_release_string() {
        assert!(resolve_format_features(Some("2.8"), &[]).is_err());
    }

    #[test]
    fn parse_scram_spec_happy_path() {
        let spec = parse_scram_spec("SCRAM-SHA-512=[name=alice,password=hunter2,iterations=8192]")
            .unwrap();
        assert!(
            spec == ScramSpec {
                mechanism: SaslMechanism::ScramSha512,
                name: "alice".to_string(),
                password: "hunter2".to_string(),
                iterations: 8192,
            }
        );
    }

    #[test]
    fn parse_scram_spec_iterations_default() {
        let spec = parse_scram_spec("SCRAM-SHA-512=[name=bob,password=p]").unwrap();
        assert!(spec.iterations == 4096);
    }

    #[test]
    fn parse_scram_spec_sha256_prefix() {
        let spec = parse_scram_spec("SCRAM-SHA-256=[name=alice,password=hunter2,iterations=8192]")
            .unwrap();
        assert!(spec.name == "alice");
        assert!(spec.mechanism == SaslMechanism::ScramSha256);
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
        assert!(
            entry
                == AclEntry {
                    resource_type: crabka_metadata::ResourceType::Cluster,
                    resource_name: "kafka-cluster".to_string(),
                    pattern_type: crabka_metadata::PatternType::Literal,
                    principal: "User:admin".to_string(),
                    host: "*".to_string(),
                    operation: crabka_metadata::AclOperation::All,
                    permission_type: crabka_metadata::PermissionType::Allow,
                }
        );
    }

    #[test]
    fn parse_acl_spec_with_prefixed_pattern() {
        let s = "principal=User:alice,host=*,operation=Read,permission=Allow,resource=Topic:team-:Prefixed";
        let entry = parse_acl_spec(s).unwrap();
        assert!(entry.pattern_type == crabka_metadata::PatternType::Prefixed);
        assert!(entry.resource_name == "team-");
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
        assert!(
            v == Voter {
                id: crabka_metadata::NodeId(3),
                directory_id: Uuid::from_u128(3),
                endpoints: vec![VoterEndpoint {
                    name: "CONTROLLER".to_string(),
                    host: "host".to_string(),
                    port: 9093,
                }],
                kraft_version: KRaftVersionRange { min: 0, max: 1 },
            }
        );
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
    fn parse_acl_spec_all_operations() {
        use crabka_metadata::AclOperation;
        for (s, op) in [
            ("All", AclOperation::All),
            ("Read", AclOperation::Read),
            ("Write", AclOperation::Write),
            ("Create", AclOperation::Create),
            ("Delete", AclOperation::Delete),
            ("Alter", AclOperation::Alter),
            ("Describe", AclOperation::Describe),
            ("ClusterAction", AclOperation::ClusterAction),
            ("DescribeConfigs", AclOperation::DescribeConfigs),
            ("AlterConfigs", AclOperation::AlterConfigs),
            ("IdempotentWrite", AclOperation::IdempotentWrite),
            ("TwoPhaseCommit", AclOperation::TwoPhaseCommit),
        ] {
            let spec =
                format!("principal=User:u,host=*,operation={s},permission=Allow,resource=Topic:t");
            assert!(parse_acl_spec(&spec).unwrap().operation == op);
        }
    }

    #[test]
    fn parse_acl_spec_all_resource_types_and_deny() {
        use crabka_metadata::{PermissionType, ResourceType};
        for (s, rt) in [
            ("Topic", ResourceType::Topic),
            ("Group", ResourceType::Group),
            ("Cluster", ResourceType::Cluster),
            ("TransactionalId", ResourceType::TransactionalId),
        ] {
            let spec =
                format!("principal=User:u,host=*,operation=All,permission=Deny,resource={s}:n");
            let entry = parse_acl_spec(&spec).unwrap();
            assert!(entry.resource_type == rt);
            assert!(entry.permission_type == PermissionType::Deny);
        }
    }

    #[test]
    fn parse_acl_spec_error_branches() {
        for bad in [
            "principal=User:u,host=*,operation=Bogus,permission=Allow,resource=Topic:t",
            "principal=User:u,host=*,operation=All,permission=Maybe,resource=Topic:t",
            "principal=User:u,host=*,operation=All,permission=Allow,resource=Topic:t:Weird",
            "principal=User:u,host=*,operation=All,permission=Allow,resource=Bogus:t",
            "principal=User:u,host=*,operation=All,permission=Allow,resource=Topic",
            "malformedpair",
            "host=*,operation=All,permission=Allow,resource=Topic:t",
            "principal=User:u,operation=All,permission=Allow,resource=Topic:t",
            "principal=User:u,host=*,permission=Allow,resource=Topic:t",
            "principal=User:u,host=*,operation=All,resource=Topic:t",
            "principal=User:u,host=*,operation=All,permission=Allow",
        ] {
            assert!(parse_acl_spec(bad).is_err(), "expected error for {bad:?}");
        }
    }

    #[test]
    fn parse_scram_spec_error_branches() {
        for bad in [
            "SCRAM-SHA-512=[name=a,password=b", // missing closing ]
            "SCRAM-SHA-512=[name=a,password=b,iterations=xx]", // bad iterations
            "SCRAM-SHA-512=[name=a,badattr]",   // malformed attr (no '=')
            "SCRAM-SHA-512=[name=a,iterations=4096]", // missing password
        ] {
            assert!(parse_scram_spec(bad).is_err(), "expected error for {bad:?}");
        }
    }

    #[test]
    fn parse_initial_controller_error_branches() {
        for bad in [
            "notanum@host:9093:00000000-0000-0000-0000-000000000003", // bad id
            "3@host9093",                                             // missing directory uuid
            "3@host:notaport:00000000-0000-0000-0000-000000000003",   // bad port
            "3@hostonly:00000000-0000-0000-0000-000000000003",        // missing host:port
        ] {
            assert!(
                parse_initial_controller(bad).is_err(),
                "expected error for {bad:?}"
            );
        }
    }

    #[test]
    fn base64_encode_known_vectors() {
        // RFC 4648 §10
        for (input, want) in [
            (b"".as_slice(), ""),
            (b"f".as_slice(), "Zg=="),
            (b"fo".as_slice(), "Zm8="),
            (b"foo".as_slice(), "Zm9v"),
            (b"foob".as_slice(), "Zm9vYg=="),
            (b"fooba".as_slice(), "Zm9vYmE="),
            (b"foobar".as_slice(), "Zm9vYmFy"),
        ] {
            assert!(base64_encode(input) == want);
        }
    }
}
