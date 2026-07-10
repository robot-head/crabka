//! Emit the SQL commands accepted by the Gres parser for compatibility tooling.

use std::io::{self, Write as _};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let report = crabka_gres_conformance::parser_command_report()?;
    let stdout = io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer(&mut output, &report)?;
    writeln!(output)?;
    Ok(())
}
