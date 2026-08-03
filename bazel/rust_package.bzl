"""Minimal rules_rs targets for a Cargo workspace package."""

load("@crates//:data.bzl", "DEP_DATA")
load("@crates//:defs.bzl", "all_crate_deps")
load("@rules_rs//rs:cargo_build_script.bzl", "cargo_build_script")
load("@rules_rs//rs:rust_binary.bzl", "rust_binary")
load("@rules_rs//rs:rust_library.bzl", "rust_library")
load("@rules_rs//rs:rust_proc_macro.bzl", "rust_proc_macro")
load("@rules_rs//rs:rust_test.bzl", "rust_test")
load("@rules_rust//rust:defs.bzl", "rust_doc")

def _aliases(*kinds):
    metadata = DEP_DATA[native.package_name()]
    deps = []
    for kind in kinds:
        deps.extend(metadata[kind])
        for platform_deps in metadata[kind + "_by_platform"].values():
            deps.extend(platform_deps)
    return {dep: alias for dep, alias in metadata["aliases"].items() if dep in deps}

def normal_aliases():
    """Aliases restricted to normal dependencies."""
    return _aliases("deps")

def cargo_features():
    """The feature set Cargo resolves for this workspace package."""
    return DEP_DATA[native.package_name()]["crate_features"]

def rust_feature_binary(name, crate_root, crate_label = None, features = [], deps = []):
    """Declares an opt-in feature variant of a Cargo binary."""
    metadata = DEP_DATA[native.package_name()]
    srcs = native.glob(["src/**/*.rs"])
    data = native.glob(["**/*"], exclude = ["**/*.rs", "BUILD.bazel"], allow_empty = True)
    rust_binary(
        name = name,
        aliases = normal_aliases(),
        compile_data = data,
        crate_features = metadata["crate_features"] + features,
        crate_root = crate_root,
        data = data,
        deps = all_crate_deps(normal = True) + deps + ([crate_label] if crate_label else []),
        edition = "2024",
        srcs = srcs,
        visibility = ["//visibility:public"],
    )

def rust_package_tests(name, crate_label = None, rustc_env = {}, compile_data = [], harnessless = [], test_binaries = {}):
    """Declares a package's top-level integration tests for a hand-written library target."""
    metadata = DEP_DATA[native.package_name()]
    srcs = native.glob(["src/**/*.rs"])
    data = depset(native.glob(["**/*"], exclude = ["**/*.rs", "BUILD.bazel"], allow_empty = True) + compile_data).to_list()
    for test in native.glob(["tests/*.rs"], allow_empty = True):
        test_name = test.removeprefix("tests/").removesuffix(".rs")
        test_rustc_env = dict(rustc_env)
        test_rustc_env.update({"CARGO_BIN_EXE_" + binary: "$(rootpath " + target + ")" for binary, target in test_binaries.items()})
        rust_test(
            name = test_name,
            aliases = _aliases("deps", "dev_deps"),
            crate_root = test,
            compile_data = data,
            crate_features = metadata["crate_features"],
            data = data + test_binaries.values(),
            deps = all_crate_deps(normal = True, normal_dev = True) + ([crate_label] if crate_label else []),
            edition = "2024",
            env = {"CARGO_MANIFEST_DIR": native.package_name()},
            use_libtest_harness = test_name not in harnessless,
            rustc_env = test_rustc_env,
            srcs = srcs + native.glob(["tests/**/*.rs"]),
        )

def rust_package_benches(crate_label = None, rustc_env = {}, compile_data = []):
    """Declares Cargo-style harnessless benchmark binaries."""
    metadata = DEP_DATA[native.package_name()]
    srcs = native.glob(["src/**/*.rs"])
    data = depset(native.glob(["**/*"], exclude = ["**/*.rs", "BUILD.bazel"], allow_empty = True) + compile_data).to_list()
    for bench in native.glob(["benches/*.rs"], allow_empty = True):
        bench_name = bench.removeprefix("benches/").removesuffix(".rs")
        rust_binary(
            name = "bench_" + bench_name,
            aliases = _aliases("deps", "dev_deps"),
            crate_root = bench,
            compile_data = data,
            crate_features = metadata["crate_features"],
            data = data,
            deps = all_crate_deps(normal = True, normal_dev = True) + ([crate_label] if crate_label else []),
            edition = "2024",
            rustc_env = rustc_env,
            srcs = srcs + native.glob(["benches/**/*.rs"]),
        )

def rust_package_examples(crate_label = None, rustc_env = {}, compile_data = [], features = [], extra_deps = []):
    """Declares Cargo-style example binaries."""
    metadata = DEP_DATA[native.package_name()]
    srcs = native.glob(["src/**/*.rs"])
    data = depset(native.glob(["**/*"], exclude = ["**/*.rs", "BUILD.bazel"], allow_empty = True) + compile_data).to_list()
    for example in native.glob(["examples/*.rs"], allow_empty = True):
        example_name = example.removeprefix("examples/").removesuffix(".rs")
        rust_binary(
            name = "example_" + example_name,
            aliases = _aliases("deps", "dev_deps"),
            crate_root = example,
            compile_data = data,
            crate_features = metadata["crate_features"] + features,
            data = data,
            deps = all_crate_deps(normal = True, normal_dev = True) + extra_deps + ([crate_label] if crate_label else []),
            edition = "2024",
            rustc_env = rustc_env,
            srcs = srcs + native.glob(["examples/**/*.rs"]),
        )

def rust_package(
        name,
        cargo_name = None,
        crate_name = None,
        crate_root = None,
        build_script = False,
        protoc = False,
        proc_macro = False,
        rustc_env = {},
        compile_data = [],
        test_compile_data = [],
        test_env = {},
        test_features = {},
        lib_test_rustc_flags = [],
        lib_test_size = "medium",
        test_tags = {},
        test_binaries = {},
        harnessless = [],
        features = [],
        extra_deps = [],
        examples = False):
    """Declares the library, binaries, and tests described by Cargo metadata."""
    metadata = DEP_DATA[native.package_name()]
    srcs = native.glob(["src/**/*.rs"])
    data = depset(native.glob(["**/*"], exclude = ["**/*.rs", "BUILD.bazel"], allow_empty = True) + compile_data).to_list()
    deps = all_crate_deps(normal = True) + extra_deps

    if build_script:
        cargo_build_script(
            name = "_build_script",
            aliases = _aliases("build_deps"),
            build_script_env = {"PROTOC": "$(execpath @crates//:protox__protox)"} if protoc else {},
            crate_root = "build.rs",
            compile_data = native.glob(["**"], exclude = ["BUILD.bazel"]),
            data = native.glob(["**"], exclude = ["BUILD.bazel"]),
            deps = all_crate_deps(build = True),
            edition = "2024",
            link_deps = deps,
            pkg_name = cargo_name or name,
            srcs = srcs + ["build.rs"],
            tools = ["@crates//:protox__protox"] if protoc else [],
        )
        deps = deps + [":_build_script"]

    if crate_name:
        rule = rust_proc_macro if proc_macro else rust_library
        rule(
            name = name,
            aliases = normal_aliases(),
            compile_data = data,
            crate_features = metadata["crate_features"] + features,
            crate_name = crate_name,
            crate_root = crate_root,
            deps = deps,
            edition = "2024",
            rustc_env = rustc_env,
            srcs = srcs,
            visibility = ["//visibility:public"],
        )

        lib_test_rustc_env = dict(rustc_env)
        lib_test_rustc_env["CARGO_MANIFEST_DIR"] = native.package_name()
        runtime_env = {"CARGO_MANIFEST_DIR": native.package_name()}
        runtime_env.update(test_env)
        rust_test(
            name = name + "_lib_test",
            crate = ":" + name,
            data = data + test_compile_data,
            deps = all_crate_deps(normal_dev = True),
            env = runtime_env,
            rustc_env = lib_test_rustc_env,
            rustc_flags = lib_test_rustc_flags,
            size = lib_test_size,
        )

        rust_doc(
            name = name + "_doc",
            crate = ":" + name,
        )

    for binary, root in metadata["binaries"].items():
        rust_binary(
            name = binary + "__bin",
            aliases = normal_aliases(),
            compile_data = data,
            crate_features = metadata["crate_features"] + features,
            crate_root = root,
            deps = deps + ([":" + name] if crate_name else []),
            edition = "2024",
            rustc_env = rustc_env,
            srcs = srcs,
            visibility = ["//visibility:public"],
        )

    rust_package_benches(crate_label = ":" + name if crate_name else None, rustc_env = rustc_env, compile_data = compile_data)
    if examples:
        rust_package_examples(
            crate_label = ":" + name if crate_name else None,
            rustc_env = rustc_env,
            compile_data = compile_data,
            features = features,
            extra_deps = extra_deps,
        )

    for test in native.glob(["tests/*.rs"], allow_empty = True):
        test_name = test.removeprefix("tests/").removesuffix(".rs")
        features = test_features.get(test_name, [])
        test_library = ":" + name
        if features:
            test_library = ":" + name + "_" + test_name + "_lib"
            rust_library(
                name = name + "_" + test_name + "_lib",
                aliases = normal_aliases(),
                compile_data = data,
                crate_features = metadata["crate_features"] + features,
                crate_name = crate_name,
                crate_root = crate_root,
                deps = deps,
                edition = "2024",
                rustc_env = rustc_env,
                srcs = srcs,
                testonly = True,
            )
        binary_data = [":" + binary + "__bin" for binary in metadata["binaries"]] + test_binaries.values()
        test_rustc_env = dict(rustc_env)
        # Tests run from the workspace's runfiles root. Keep this relative so
        # it does not capture a compilation sandbox that disappears at runtime.
        test_rustc_env["CARGO_MANIFEST_DIR"] = native.package_name()
        test_rustc_env.update({"CARGO_BIN_EXE_" + binary: "$(rootpath :" + binary + "__bin)" for binary in metadata["binaries"]})
        test_rustc_env.update({"CARGO_BIN_EXE_" + binary: "$(rootpath " + target + ")" for binary, target in test_binaries.items()})
        runtime_env = {"CARGO_MANIFEST_DIR": native.package_name()}
        runtime_env.update(test_env)
        rust_test(
            name = test_name,
            aliases = _aliases("deps", "dev_deps"),
            crate_root = test,
            compile_data = data + test_compile_data,
            crate_features = metadata["crate_features"] + features,
            deps = all_crate_deps(normal = True, normal_dev = True) + ([":_build_script"] if build_script else []) + ([test_library] if crate_name else []),
            data = data + test_compile_data + binary_data,
            edition = "2024",
            env = runtime_env,
            rustc_env = test_rustc_env,
            srcs = srcs + native.glob(["tests/**/*.rs"]),
            tags = test_tags.get(test_name, []),
            use_libtest_harness = test_name not in harnessless,
        )
