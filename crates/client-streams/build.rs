//! Build script. Under the `schema-serde` feature, compiles the protobuf
//! example's `.proto` into `OUT_DIR` via protox (pure Rust, no `protoc`):
//! prost structs that derive `prost_reflect::ReflectMessage` with the file
//! descriptor set embedded via `crate::FILE_DESCRIPTOR_SET_BYTES`. A no-op for
//! normal (non-feature) builds.

fn main() {
    #[cfg(feature = "schema-serde")]
    {
        use protox::prost::Message as _;
        use std::path::PathBuf;

        println!("cargo:rerun-if-changed=examples/proto/order.proto");

        let fds = protox::compile(["examples/proto/order.proto"], ["examples/proto"])
            .expect("protox compile examples/proto/order.proto");

        let mut config = prost_build::Config::new();
        config.skip_protoc_run();
        for file in &fds.file {
            let pkg = file.package();
            for msg in &file.message_type {
                let full = if pkg.is_empty() {
                    msg.name().to_string()
                } else {
                    format!("{pkg}.{}", msg.name())
                };
                config
                    .type_attribute(&full, "#[derive(::prost_reflect::ReflectMessage)]")
                    .type_attribute(&full, format!("#[prost_reflect(message_name = \"{full}\")]"))
                    .type_attribute(
                        &full,
                        "#[prost_reflect(file_descriptor_set_bytes = \"crate::FILE_DESCRIPTOR_SET_BYTES\")]",
                    );
            }
        }

        let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR set"));
        std::fs::write(out_dir.join("file_descriptor_set.bin"), fds.encode_to_vec())
            .expect("write file_descriptor_set.bin");
        config
            .compile_fds(fds)
            .expect("generate prost types from descriptor set");
    }
}
