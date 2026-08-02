# Crabka container images

Production images are Bazel targets in [`BUILD.bazel`](BUILD.bazel). Rust
binaries come directly from `rules_rs`; `rules_apko` constructs a locked Wolfi
base and `rules_oci` creates the `linux/amd64` + `linux/arm64` indexes. There is
no nested Cargo, Melange, Dockerfile, or QEMU build.

```sh
aspect build //packaging:broker_image --bazel-flag=--config=ci
aspect run //packaging:broker_image_load --bazel-flag=--config=ci
```

The release workflows invoke the generated `*_push` targets. Published service
images go to both `robothead/<image>` and `ghcr.io/robot-head/<image>`; the demo
image goes to GHCR. GitHub Actions adds keyless SLSA provenance and SPDX SBOM
attestations after each push.

The separate `creusot-toolchain` recipe remains a single-architecture CI tool
image. It is not part of the user-facing runtime image family.
