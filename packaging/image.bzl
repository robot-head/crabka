load("@rules_oci//oci:defs.bzl", "oci_image", "oci_image_index", "oci_load", "oci_push")
load("@rules_pkg//pkg:mappings.bzl", "pkg_files")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

def runtime_image(name, binaries, entrypoint, repository, cmd = []):
    """Builds a two-platform OCI image from Bazel Rust binaries."""
    files = name + "_files"
    layer = name + "_layer"
    image = name + "_platform_image"

    pkg_files(
        name = files,
        srcs = binaries.keys(),
        prefix = "/usr/bin",
        renames = binaries,
    )

    pkg_tar(
        name = layer,
        srcs = [":" + files],
        mode = "0755",
    )

    oci_image(
        name = image,
        base = "//packaging/apko:runtime_base",
        cmd = cmd,
        entrypoint = [entrypoint],
        tars = [":" + layer],
    )

    oci_image_index(
        name = name,
        images = [":" + image],
        platforms = [
            "//platforms:linux_amd64",
            "//platforms:linux_arm64",
        ],
    )

    oci_push(
        name = name + "_push",
        image = ":" + name,
    )

    oci_load(
        name = name + "_load",
        image = ":" + image,
        repo_tags = [repository + ":e2e"],
    )

    native.filegroup(
        name = name + "_tar",
        srcs = [":" + name + "_load"],
        output_group = "tarball",
    )
