#!/usr/bin/env bash
# Regenerate the committed protobuf example bindings from examples/proto/*.proto.
#
# The crate intentionally has NO build.rs (so non-example builds stay lean), so the
# prost + prost-reflect bindings for the protobuf examples are generated once and
# committed here (order.rs, orders.rs + file_descriptor_set.bin). The descriptor set
# is SHARED: every example exposes the same `crate::FILE_DESCRIPTOR_SET_BYTES`, so
# the committed file_descriptor_set.bin must contain *all* example protos (order.proto
# AND orders.proto). Re-run this if any of them change. Requires protox (pure-Rust, no
# protoc).
#
# One-off generator (run from the repo root):
#
#   cargo new --bin /tmp/gen-orders && cd /tmp/gen-orders
#   cargo add protox prost-build prost prost-reflect
#   cat > src/main.rs <<'EOF'
#   fn main() {
#       let base = "<REPO>/crates/client-streams/examples/proto";
#       let fds = protox::compile(
#           [format!("{base}/order.proto"), format!("{base}/orders.proto")],
#           [base],
#       ).unwrap();
#       std::fs::write("file_descriptor_set.bin", protox::prost::Message::encode_to_vec(&fds)).unwrap();
#       let pool = protox::prost_reflect::DescriptorPool::from_file_descriptor_set(fds.clone()).unwrap();
#       let mut cfg = prost_build::Config::new();
#       cfg.skip_protoc_run().out_dir(".");
#       for m in pool.all_messages() {
#           let f = m.full_name().to_string();
#           cfg.type_attribute(&f, "#[derive(::prost_reflect::ReflectMessage)]")
#              .type_attribute(&f, format!("#[prost_reflect(message_name = \"{f}\")]"))
#              .type_attribute(&f, "#[prost_reflect(file_descriptor_set_bytes = \"crate::FILE_DESCRIPTOR_SET_BYTES\")]");
#       }
#       cfg.compile_fds(fds).unwrap();
#   }
#   EOF
#   cargo run   # produces demo.rs + file_descriptor_set.bin
#
# demo.rs holds every message; split the OrderProto/OrderSummary structs into
# orders.rs and the Order struct into order.rs (keep each file's protox header),
# then copy file_descriptor_set.bin here.
echo "See the comments in this script for the one-off regeneration recipe."
