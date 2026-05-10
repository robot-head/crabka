use std::path::PathBuf;
use std::process::ExitCode;

mod ir;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let schemas_dir = match args.next() {
        Some(s) => PathBuf::from(s),
        None => {
            eprintln!("usage: crabka-protocol-codegen <schemas-dir> <out-dir>");
            return ExitCode::from(2);
        }
    };
    let out_dir = match args.next() {
        Some(s) => PathBuf::from(s),
        None => {
            eprintln!("usage: crabka-protocol-codegen <schemas-dir> <out-dir>");
            return ExitCode::from(2);
        }
    };

    match run(&schemas_dir, &out_dir) {
        Ok(n) => {
            eprintln!("Generated code for {n} messages into {}", out_dir.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(_schemas: &std::path::Path, _out: &std::path::Path) -> Result<usize, ir::IrError> {
    Ok(0) // filled in later
}
