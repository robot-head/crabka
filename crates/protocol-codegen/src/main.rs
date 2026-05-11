use std::path::{Path, PathBuf};
use std::process::ExitCode;

use crabka_protocol_codegen::{emit, ir, name_conv, validate};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(schemas) = args.next() else {
        return usage();
    };
    let Some(out) = args.next() else {
        return usage();
    };
    match run(&PathBuf::from(schemas), &PathBuf::from(out)) {
        Ok(n) => {
            eprintln!("Generated {n} files");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn usage() -> ExitCode {
    eprintln!("usage: crabka-protocol-codegen <schemas-dir> <out-dir>");
    ExitCode::from(2)
}

#[derive(Debug, thiserror::Error)]
enum RunError {
    #[error(transparent)]
    Ir(#[from] ir::IrError),
    #[error(transparent)]
    Validate(#[from] validate::ValidateError),
    #[error(transparent)]
    Emit(#[from] emit::EmitError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("schemas/VERSION must contain a `sha:` line")]
    MissingSha,
}

fn read_schemas_sha(schemas: &std::path::Path) -> Result<String, RunError> {
    let version_text = std::fs::read_to_string(schemas.join("VERSION"))?;
    let sha = version_text
        .lines()
        .find_map(|l| l.strip_prefix("sha: "))
        .ok_or(RunError::MissingSha)?
        .to_owned();
    Ok(sha)
}

const CURATED: &[&str] = &[
    "ApiVersionsRequest",
    "ApiVersionsResponse",
    "MetadataRequest",
    "MetadataResponse",
    "ProduceRequest",
    "ProduceResponse",
    "OffsetCommitRequest",
    "OffsetCommitResponse",
    "RequestHeader",
    "ResponseHeader",
    "DescribeGroupsRequest",
    "DescribeGroupsResponse",
];

/// Derive the `crates/protocol/src` directory from the generated-output dir.
/// Convention: generated output is `crates/protocol/generated`; src is the sibling `src`.
fn protocol_src_from_out(out: &Path) -> PathBuf {
    out.parent().unwrap_or(out).join("src")
}

fn write_wrapper(
    spec: &ir::MessageSpec,
    flavor: emit::wrappers::Flavor,
    schemas_version: &str,
    protocol_src: &Path,
) -> std::io::Result<()> {
    use emit::wrappers::Flavor;
    let snake = name_conv::module_name(&spec.name);
    let body = emit::wrappers::emit(spec, flavor, schemas_version);
    let dir = protocol_src.join(match flavor {
        Flavor::Owned => "owned",
        Flavor::Borrowed => "borrowed",
    });
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join(format!("{snake}.rs")), body)?;
    Ok(())
}

fn run(schemas: &std::path::Path, out: &std::path::Path) -> Result<usize, RunError> {
    let schemas_sha = read_schemas_sha(schemas)?;
    let specs = ir::load_dir(schemas)?;
    validate::validate(&specs)?;
    std::fs::create_dir_all(out)?;
    // Common-struct output directory (owned flavor).
    let common_owned_dir = out.join("common").join("owned");
    let common_borrowed_dir = out.join("common").join("borrowed");

    // Derive the protocol/src directory so we can write wrappers and mod.rs.
    let protocol_src = protocol_src_from_out(out);

    let mut count = 0;
    for s in &specs {
        if !CURATED.contains(&s.name.as_str()) {
            continue;
        }
        let owned_em = emit::owned::emit(s, &schemas_sha)?;
        let borrowed_em = emit::borrowed::emit(s, &schemas_sha)?;
        std::fs::write(out.join(format!("{}.owned.rs", s.name)), &owned_em.primary)?;
        std::fs::write(
            out.join(format!("{}.borrowed.rs", s.name)),
            &borrowed_em.primary,
        )?;
        count += 2;
        // Write common-struct files.
        if !owned_em.commons.is_empty() {
            std::fs::create_dir_all(&common_owned_dir)?;
        }
        for (cs_name, body) in &owned_em.commons {
            std::fs::write(common_owned_dir.join(format!("{cs_name}.owned.rs")), body)?;
            count += 1;
        }
        if !borrowed_em.commons.is_empty() {
            std::fs::create_dir_all(&common_borrowed_dir)?;
        }
        for (cs_name, body) in &borrowed_em.commons {
            std::fs::write(
                common_borrowed_dir.join(format!("{cs_name}.borrowed.rs")),
                body,
            )?;
            count += 1;
        }

        // Emit wrapper files — overwrite the hand-written wrappers.
        if emit::wrappers::should_emit_wrapper(s) {
            write_wrapper(
                s,
                emit::wrappers::Flavor::Owned,
                &schemas_sha,
                &protocol_src,
            )?;
            write_wrapper(
                s,
                emit::wrappers::Flavor::Borrowed,
                &schemas_sha,
                &protocol_src,
            )?;
            count += 2;
        }
    }

    // Emit owned/mod.rs and borrowed/mod.rs for the curated set.
    let curated_specs: Vec<&ir::MessageSpec> = specs
        .iter()
        .filter(|s| CURATED.contains(&s.name.as_str()))
        .collect();
    let owned_mod = emit::mod_rs::emit(&curated_specs, emit::wrappers::Flavor::Owned, &schemas_sha);
    let borrowed_mod = emit::mod_rs::emit(
        &curated_specs,
        emit::wrappers::Flavor::Borrowed,
        &schemas_sha,
    );
    std::fs::write(protocol_src.join("owned").join("mod.rs"), owned_mod)?;
    std::fs::write(protocol_src.join("borrowed").join("mod.rs"), borrowed_mod)?;
    count += 2;

    // Always emit the ApiKey enum regardless of CURATED — it reflects ALL schemas.
    let api_key_src = emit::api_key_enum::emit(&specs, &schemas_sha);
    std::fs::write(out.join("api_key.rs"), &api_key_src)?;
    count += 1;
    Ok(count)
}
