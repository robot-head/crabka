use std::path::PathBuf;
use std::process::ExitCode;

use crabka_protocol_codegen::{emit_owned, ir, validate};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(schemas) = args.next() else {
        eprintln!("usage: crabka-protocol-codegen <schemas-dir> <out-dir>");
        return ExitCode::from(2);
    };
    let Some(out) = args.next() else {
        eprintln!("usage: crabka-protocol-codegen <schemas-dir> <out-dir>");
        return ExitCode::from(2);
    };
    match run(&PathBuf::from(schemas), &PathBuf::from(out)) {
        Ok(n) => {
            eprintln!("Generated code for {n} messages");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
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
}

fn run(schemas: &std::path::Path, out: &std::path::Path) -> Result<usize, RunError> {
    let specs = ir::load_dir(schemas)?;
    validate::validate(&specs)?;
    std::fs::create_dir_all(out)?;
    let mut count = 0;
    for s in &specs {
        // Today: only ApiVersionsRequest is supported by the owned emitter.
        if s.name != "ApiVersionsRequest" {
            continue;
        }
        let body = emit_owned::emit(s)?;
        let file = out.join(format!("{}.owned.rs", s.name));
        std::fs::write(&file, body)?;
        count += 1;
    }
    Ok(count)
}
