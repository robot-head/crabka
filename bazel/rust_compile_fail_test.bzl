"""Hermetic compile-fail coverage for exported Rust procedural macros."""

load("@rules_rust//rust:defs.bzl", "rust_common")

def _rust_compile_fail_test_impl(ctx):
    crate = ctx.attr.proc_macro[rust_common.crate_info]
    rustc_files = [file for file in ctx.attr._rustc[DefaultInfo].files.to_list() if file.basename == "rustc"]
    if len(rustc_files) != 1:
        fail("expected one rustc executable, got %s" % rustc_files)

    runner = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(
        output = runner,
        is_executable = True,
        content = """#!/usr/bin/env bash
set -euo pipefail

runfile() {
  case "$1" in
    ../*) printf '%%s/%%s\\n' "$TEST_SRCDIR" "${1#../}" ;;
    *) printf '%%s/%%s/%%s\\n' "$TEST_SRCDIR" "$TEST_WORKSPACE" "$1" ;;
  esac
}

rustc="$(runfile %s)"
sysroot="${rustc%%/bin/rustc}"
source="$(runfile %s)"
proc_macro="$(runfile %s)"
stderr="$TEST_TMPDIR/stderr"

if "$rustc" --sysroot="$sysroot" --edition=2024 --crate-type=lib --crate-name=compile_fail_case \
    "$source" --extern "crabka_connect_derive=$proc_macro" \
    --error-format=human --color=never --out-dir "$TEST_TMPDIR" \
    2>"$stderr"; then
  echo "expected rustc to reject $source" >&2
  exit 1
fi

expected=%s
span=%s
diagnostics="$(<"$stderr")"
if [[ "$diagnostics" != *"$expected"* || "$diagnostics" != *"$span"* ]]; then
  printf '%%s\\n' "$diagnostics" >&2
  exit 1
fi
""" % (
            repr(rustc_files[0].short_path),
            repr(ctx.file.src.short_path),
            repr(crate.output.short_path),
            repr(ctx.attr.expected),
            repr(ctx.attr.span),
        ),
    )

    runfiles = ctx.runfiles(
        files = [ctx.file.src, crate.output, rustc_files[0]],
        transitive_files = ctx.attr._rust_std[DefaultInfo].files,
    )
    runfiles = runfiles.merge(ctx.attr.proc_macro[DefaultInfo].default_runfiles)
    runfiles = runfiles.merge(ctx.attr._rustc[DefaultInfo].default_runfiles)
    return [DefaultInfo(executable = runner, runfiles = runfiles)]

rust_compile_fail_test = rule(
    implementation = _rust_compile_fail_test_impl,
    attrs = {
        "expected": attr.string(mandatory = True),
        "proc_macro": attr.label(mandatory = True),
        "span": attr.string(mandatory = True),
        "src": attr.label(allow_single_file = [".rs"], mandatory = True),
        "_rustc": attr.label(default = Label("@rules_rust//rust/toolchain:current_rustc_files")),
        "_rust_std": attr.label(default = Label("@rules_rust//rust/toolchain:current_rust_stdlib_files")),
    },
    test = True,
)
