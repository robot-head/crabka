use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=proto/order.proto");
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());

    // protox compiles to a FileDescriptorSet without needing protoc installed.
    let fds = protox::compile(["proto/order.proto"], ["proto"]).expect("protox compile");

    let fds_path = out_dir.join("file_descriptor_set.bin");
    std::fs::write(&fds_path, protox::prost::Message::encode_to_vec(&fds)).expect("write fds");

    let pool = prost_reflect::DescriptorPool::from_file_descriptor_set(fds.clone())
        .expect("descriptor pool");

    let mut cfg = prost_build::Config::new();
    cfg.out_dir(&out_dir);
    for message in pool.all_messages() {
        let full = message.full_name().to_string();
        cfg.type_attribute(&full, "#[derive(::prost_reflect::ReflectMessage)]")
            .type_attribute(&full, format!("#[prost_reflect(message_name = \"{full}\")]"))
            .type_attribute(
                &full,
                "#[prost_reflect(file_descriptor_set_bytes = \"crate::FILE_DESCRIPTOR_SET_BYTES\")]",
            );
    }
    cfg.compile_fds(fds).expect("prost compile");
}
