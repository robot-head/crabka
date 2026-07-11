//! Generates Connect-RPC server stubs + prost message types from the vendored
//! `push.v1`, `querier.v1`, and OTLP `profiles/v1development` protos.
//!
//! Drives codegen through a vendored `protoc` binary (`protoc-bin-vendored`) so
//! the build is hermetic — no system `protoc` and no network fetch. The Connect
//! generator (connectrpc-axum-build) always invokes a `protoc` binary, so the
//! vendored one is supplied via `prost-build`'s `protoc_executable`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/google/v1/profile.proto",
        "proto/push/v1/push.proto",
        "proto/querier/v1/querier.proto",
        "proto/settings/v1/settings.proto",
        "proto/opentelemetry/proto/collector/profiles/v1development/profiles_service.proto",
    ];
    let includes = ["proto"];
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    connectrpc_axum_build::compile_protos(&protos, &includes)
        .with_prost_config(move |config| {
            config.protoc_executable(protoc_path.clone());
        })
        .compile()?;
    normalize_generated_code()?;
    for path in [
        "proto/types/v1/types.proto",
        "proto/google/v1/profile.proto",
        "proto/push/v1/push.proto",
        "proto/querier/v1/querier.proto",
        "proto/settings/v1/settings.proto",
        "proto/opentelemetry/proto/common/v1/common.proto",
        "proto/opentelemetry/proto/resource/v1/resource.proto",
        "proto/opentelemetry/proto/profiles/v1development/profiles.proto",
        "proto/opentelemetry/proto/collector/profiles/v1development/profiles_service.proto",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
    Ok(())
}

fn normalize_generated_code() -> Result<(), Box<dyn std::error::Error>> {
    const GENERATED_FILES: &[&str] = &[
        "google.v1.rs",
        "opentelemetry.proto.collector.profiles.v1development.rs",
        "opentelemetry.proto.common.v1.rs",
        "opentelemetry.proto.profiles.v1development.rs",
        "opentelemetry.proto.resource.v1.rs",
        "push.v1.rs",
        "querier.v1.rs",
        "settings.v1.rs",
        "types.v1.rs",
    ];
    const BUILDERS: &[&str] = &[
        "ProfilesServiceBuilder",
        "PusherServiceBuilder",
        "QuerierServiceBuilder",
        "SettingsServiceBuilder",
    ];

    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    for filename in GENERATED_FILES {
        let path = out_dir.join(filename);
        let source = std::fs::read_to_string(&path)?;
        let mut normalized = String::with_capacity(source.len());

        for line in source.lines() {
            if line.trim_start().starts_with("///") {
                continue;
            }

            let indent_len = line.len() - line.trim_start().len();
            if line.trim_start().starts_with("pub fn ") {
                normalized.push_str(&line[..indent_len]);
                normalized.push_str("#[must_use]\n");
            }

            let mut line = line.to_owned();
            for builder in BUILDERS {
                line = line.replace(
                    &format!("pub struct {builder}"),
                    &format!("# [must_use] pub struct {builder}"),
                );
            }
            line = line.replace("&FIELDS)", "FIELDS)");
            line = line.replace(
                "write!(formatter, \"expected one of: {:?}\", FIELDS)",
                "write!(formatter, \"expected one of: {FIELDS:?}\")",
            );
            line = line.replace("[`build()`]", "`build()`");
            normalized.push_str(&line);
            normalized.push('\n');
        }

        if *filename == "google.v1.rs" {
            normalized = compact_profile_deserializer(&normalized)?;
        }
        std::fs::write(path, normalized)?;
    }
    Ok(())
}

fn compact_profile_deserializer(source: &str) -> Result<String, Box<dyn std::error::Error>> {
    const START: &str = "impl<'de> serde::Deserialize<'de> for Profile {";
    let start = source
        .find(START)
        .ok_or("generated Profile deserializer is missing")?;
    let mut depth = 0_usize;
    let mut end = None;
    for (offset, byte) in source[start..].bytes().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(start + offset + 1);
                    break;
                }
            }
            _ => {}
        }
    }
    let end = end.ok_or("generated Profile deserializer is unbalanced")?;
    let compact: String = source[start..end]
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    Ok(format!("{}{}{}", &source[..start], compact, &source[end..]))
}
