#!/usr/bin/env bash
# Regenerate the committed protobuf example bindings from examples/proto/order.proto.
#
# The crate intentionally has NO build.rs (so non-example builds stay lean), so the
# prost + prost-reflect bindings for the protobuf example are generated once and
# committed here (order.rs + file_descriptor_set.bin). Re-run this if order.proto
# changes. Requires protox (pure-Rust, no protoc).
#
# One-off generator (run from the repo root):
#
#   cargo new --bin /tmp/gen-order && cd /tmp/gen-order
#   cargo add protox prost-build prost prost-reflect
#   cat > src/main.rs <<'EOF'
#   fn main() {
#       let proto = "<REPO>/crates/client-streams/examples/proto/order.proto";
#       let dir = "<REPO>/crates/client-streams/examples/proto";
#       let fds = protox::compile([proto], [dir]).unwrap();
#       std::fs::write("file_descriptor_set.bin", protox::prost::Message::encode_to_vec(&fds)).unwrap();
#       let pool = protox::prost_reflect::DescriptorPool::from_file_descriptor_set(fds.clone()).unwrap();
#       let mut cfg = prost_build::Config::new();
#       cfg.skip_protoc_run().out_dir(".");
#       for m in pool.all_messages() {
#           let f = m.full_name();
#           cfg.type_attribute(f, "#[derive(::prost_reflect::ReflectMessage)]")
#              .type_attribute(f, format!("#[prost_reflect(message_name = \"{f}\")]"))
#              .type_attribute(f, "#[prost_reflect(file_descriptor_set_bytes = \"crate::FILE_DESCRIPTOR_SET_BYTES\")]");
#       }
#       cfg.compile_fds(fds).unwrap();
#   }
#   EOF
#   cargo run   # produces demo.rs + file_descriptor_set.bin
#
# Then copy demo.rs -> order.rs (keep this header) and file_descriptor_set.bin here.
echo "See the comments in this script for the one-off regeneration recipe."
