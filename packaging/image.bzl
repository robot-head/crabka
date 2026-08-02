load("@rules_oci//oci:defs.bzl", "oci_image", "oci_image_index", "oci_load", "oci_push")
load("@rules_pkg//pkg:mappings.bzl", "pkg_files")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

def _linux_transition_impl(_settings, attr):
    return {"//command_line_option:platforms": str(attr.platform)}

_linux_transition = transition(
    implementation = _linux_transition_impl,
    inputs = [],
    outputs = ["//command_line_option:platforms"],
)

def _transitioned_image_impl(ctx):
    return DefaultInfo(files = depset(ctx.files.image))

_transitioned_image = rule(
    implementation = _transitioned_image_impl,
    attrs = {
        "image": attr.label(cfg = _linux_transition),
        "platform": attr.label(),
        "_allowlist_function_transition": attr.label(
            default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
        ),
    },
)

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
        entrypoint = [entrypoint] if type(entrypoint) == "string" else entrypoint,
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

    for arch, platform in {
        "amd64": "//platforms:linux_amd64",
        "arm64": "//platforms:linux_arm64",
    }.items():
        _transitioned_image(
            name = name + "_load_" + arch,
            image = ":" + image,
            platform = platform,
        )

    native.alias(
        name = name + "_load_image",
        actual = select({
            "@platforms//cpu:arm64": ":" + name + "_load_arm64",
            "//conditions:default": ":" + name + "_load_amd64",
        }),
    )

    oci_load(
        name = name + "_load",
        image = ":" + name + "_load_image",
        repo_tags = [repository + ":e2e"],
    )

    native.filegroup(
        name = name + "_tar",
        srcs = [":" + name + "_load"],
        output_group = "tarball",
    )
