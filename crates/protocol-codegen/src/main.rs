use std::path::PathBuf;
use std::process::ExitCode;

use crabka_protocol_codegen::{emit_borrowed, emit_owned, ir, validate};

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
    Emit(#[from] emit_owned::EmitError),
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

fn run(schemas: &std::path::Path, out: &std::path::Path) -> Result<usize, RunError> {
    let schemas_sha = read_schemas_sha(schemas)?;
    let specs = ir::load_dir(schemas)?;
    validate::validate(&specs)?;
    std::fs::create_dir_all(out)?;
    let mut count = 0;
    for s in &specs {
        if s.name != "ApiVersionsRequest" {
            continue;
        }
        let owned_body = emit_owned::emit(s, &schemas_sha)?;
        let borrowed_body = emit_borrowed::emit(s, &schemas_sha)?;
        std::fs::write(out.join(format!("{}.owned.rs", s.name)), owned_body)?;
        std::fs::write(out.join(format!("{}.borrowed.rs", s.name)), borrowed_body)?;
        count += 2;
    }
    Ok(count)
}
