fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/jaeger/api_v2/collector.proto";
    let fds = protox::compile([proto], ["proto/jaeger/api_v2"])?;
    tonic_prost_build::compile_fds(fds)?;
    normalize_generated_code()?;
    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed=proto/jaeger/api_v2/model.proto");
    Ok(())
}

fn normalize_generated_code() -> Result<(), Box<dyn std::error::Error>> {
    let generated = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("jaeger.api_v2.rs");
    let source = std::fs::read_to_string(&generated)?;
    let mut normalized = String::with_capacity(source.len());
    for line in source.lines() {
        if line.trim_start().starts_with("///") {
            continue;
        }
        let indent_len = line.len() - line.trim_start().len();
        let trimmed = line.trim_start();
        if trimmed.starts_with("pub fn as_str_name") || trimmed.starts_with("pub fn from_str_name")
        {
            normalized.push_str(&line[..indent_len]);
            normalized.push_str("#[must_use]\n");
        }
        if trimmed.starts_with("pub async fn ") || trimmed.starts_with("pub fn ") {
            normalized.push_str(&line[..indent_len]);
            normalized.push_str("///\n");
            normalized.push_str(&line[..indent_len]);
            normalized.push_str("/// # Errors\n");
            normalized.push_str(&line[..indent_len]);
            normalized.push_str("/// Returns an error if the RPC operation cannot be completed.\n");
        }
        normalized.push_str(line);
        normalized.push('\n');
    }
    let normalized = normalized
        .replace(
            "accept_compression_encodings: Default::default()",
            "accept_compression_encodings: EnabledCompressionEncodings::default()",
        )
        .replace(
            "send_compression_encodings: Default::default()",
            "send_compression_encodings: EnabledCompressionEncodings::default()",
        );
    std::fs::write(generated, normalized)?;
    Ok(())
}
