use std::path::PathBuf;
use std::process::ExitCode;

use crabka_protocol_codegen::{emit, ir, validate};

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

fn run(schemas: &std::path::Path, out: &std::path::Path) -> Result<usize, RunError> {
    let schemas_sha = read_schemas_sha(schemas)?;
    let specs = ir::load_dir(schemas)?;
    validate::validate(&specs)?;
    std::fs::create_dir_all(out)?;
    // Common-struct output directory (owned flavor).
    let common_owned_dir = out.join("common").join("owned");
    let common_borrowed_dir = out.join("common").join("borrowed");
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
    }
    // Always emit the ApiKey enum regardless of CURATED — it reflects ALL schemas.
    let api_key_src = emit::api_key_enum::emit(&specs, &schemas_sha);
    std::fs::write(out.join("api_key.rs"), &api_key_src)?;
    count += 1;
    Ok(count)
}
