"""Bazel-visible Creusot proof replay without stable-Rust compilation."""

load("@rules_rust//rust:rust_common.bzl", "CrateInfo")

def _shell_quote(value):
    return "'" + value.replace("'", "'\"'\"'") + "'"

def _creusot_replay_test_impl(ctx):
    launcher = ctx.actions.declare_file(ctx.label.name)
    replay = " && ".join([
        "cargo creusot --replay -p %s -- --locked" % _shell_quote(package)
        for package in ctx.attr.packages
    ])
    container_command = " && ".join([
        "mkdir -p /tmp/cargo-home",
        "cp -R /opt/creusot/home/.cargo/registry /opt/creusot/home/.cargo/git /tmp/cargo-home/",
        replay,
    ])
    ctx.actions.write(
        output = launcher,
        is_executable = True,
        content = """#!/usr/bin/env bash
set -euo pipefail

source_root="${CRABKA_SOURCE_ROOT:-}"
if [[ -z "$source_root" || ! -d "$source_root" ]]; then
  echo "error: CRABKA_SOURCE_ROOT must name the checked-out workspace" >&2
  exit 1
fi

workspace_copy="$(mktemp -d "${TEST_TMPDIR:-/tmp}/crabka-creusot.XXXXXX")"
cidfile="$workspace_copy.cid"
cleanup() {
  if [[ -s "$cidfile" ]]; then
    docker rm --force "$(cat "$cidfile")" >/dev/null 2>&1 || true
  fi
  rm -f -- "$cidfile"
  rm -rf -- "$workspace_copy"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
git -C "$source_root" ls-files --cached --others --exclude-standard -z | \
  tar --create --file=- --directory "$source_root" \
    --exclude='.why3find/**' --exclude='target/**' \
    --null --files-from=- | \
  tar --extract --file=- --directory "$workspace_copy"

pin="$(tr -d '[:space:]' < "$workspace_copy/.creusot-version")"
if [[ -z "$pin" ]]; then
  echo "error: .creusot-version is empty" >&2
  exit 1
fi

image="ghcr.io/robot-head/crabka-creusot:$pin"
docker pull "$image"
docker run --rm \
  --cidfile "$cidfile" \
  --user "$(id -u):$(id -g)" \
  --env CARGO_HOME=/tmp/cargo-home \
  --env CARGO_TARGET_DIR=/tmp/creusot-target \
  --volume "$workspace_copy:/workspace" \
  --workdir /workspace \
  "$image" \
  %s
""" % _shell_quote(container_command),
    )

    source_inputs = depset(
        direct = ctx.files.config + ctx.files.proofs,
        transitive = [crate[CrateInfo].srcs for crate in ctx.attr.crates],
    )
    return [DefaultInfo(
        executable = launcher,
        runfiles = ctx.runfiles(transitive_files = source_inputs),
    )]

creusot_replay_test = rule(
    implementation = _creusot_replay_test_impl,
    test = True,
    attrs = {
        "config": attr.label_list(allow_files = True),
        "crates": attr.label_list(providers = [CrateInfo]),
        "packages": attr.string_list(mandatory = True),
        "proofs": attr.label_list(allow_files = True),
    },
)
