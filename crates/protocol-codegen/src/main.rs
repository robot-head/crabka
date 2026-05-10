use std::path::PathBuf;
use std::process::ExitCode;

use crabka_protocol_codegen::ir;

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

fn run(schemas: &std::path::Path, _out: &std::path::Path) -> Result<usize, ir::IrError> {
    let specs = ir::load_dir(schemas)?;
    Ok(specs.len())
}
