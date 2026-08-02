"""Bazel test rules for host-orchestrated integration suites."""

def _host_integration_test_impl(ctx):
    script = ctx.file.script
    loader_executables = []
    runfiles = ctx.runfiles(files = [script] + ctx.files.data)
    for loader in ctx.attr.image_loaders:
        info = loader[DefaultInfo]
        executable = info.files_to_run.executable
        if executable == None:
            fail("image loader %s is not executable" % loader.label)
        loader_executables.append(executable)
        runfiles = runfiles.merge(info.default_runfiles)

    command_checks = "\n".join([
        "command -v %s >/dev/null || { echo 'FAIL: %s is required' >&2; exit 1; }" % (command, command)
        for command in ctx.attr.required_commands
    ])
    image_loads = "\n".join([
        '"${workspace}/%s"' % executable.short_path
        for executable in loader_executables
    ])
    ctx.actions.write(
        output = ctx.outputs.executable,
        content = """#!/usr/bin/env bash
set -euo pipefail
workspace="${TEST_SRCDIR}/${TEST_WORKSPACE}"
cd "${workspace}"
%s
%s
export CRABKA_GRES_IMAGES_LOADED=1
export CRABKA_GRES_KIND_ARTIFACT_DIR="${TEST_UNDECLARED_OUTPUTS_DIR}"
exec "${workspace}/%s" "$@"
""" % (command_checks, image_loads, script.short_path),
        is_executable = True,
    )
    return [DefaultInfo(executable = ctx.outputs.executable, runfiles = runfiles)]

host_integration_test = rule(
    implementation = _host_integration_test_impl,
    attrs = {
        "data": attr.label_list(allow_files = True),
        "image_loaders": attr.label_list(cfg = "target"),
        "required_commands": attr.string_list(),
        "script": attr.label(allow_single_file = True, mandatory = True),
    },
    test = True,
)
