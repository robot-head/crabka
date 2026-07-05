#!/usr/bin/env bash
# Run the pinned Crabka Creusot toolchain image.
#
# Usage examples:
#   ./tools/creusot.sh
#   ./tools/creusot.sh "cargo creusot -p crabka-verified"
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
WORKSPACE="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"
PIN="$(tr -d '[:space:]' < "${WORKSPACE}/.creusot-version")"
IMAGE="ghcr.io/robot-head/crabka-creusot:${PIN}"
TARGET_VOLUME="crabka-creusot-target"

if [ -z "${PIN}" ]; then
  echo "error: .creusot-version is empty" >&2
  exit 1
fi

if [ "$#" -eq 0 ]; then
  COMMAND="bash"
else
  COMMAND="$*"
fi

DOCKER_TTY_ARGS=()
if [ -t 0 ] && [ -t 1 ]; then
  DOCKER_TTY_ARGS=(-it)
fi

select_docker_command() {
  if command -v docker >/dev/null 2>&1; then
    printf '%s\n' docker
    return
  fi

  if command -v docker.exe >/dev/null 2>&1; then
    printf '%s\n' docker.exe
    return
  fi

  echo "error: neither docker nor docker.exe was found on PATH" >&2
  exit 1
}

running_under_wsl() {
  [ -n "${WSL_DISTRO_NAME:-}" ] && return 0
  grep -qi microsoft /proc/sys/kernel/osrelease 2>/dev/null
}

docker_command_is_windows_executable() {
  case "${DOCKER_COMMAND##*/}" in
    docker.exe|Docker.exe|DOCKER.EXE) return 0 ;;
    *) return 1 ;;
  esac
}

docker_expects_windows_paths() {
  if running_under_wsl && docker_command_is_windows_executable; then
    return 0
  fi

  local docker_os
  docker_os="$("${DOCKER_COMMAND}" version --format '{{.Client.Os}} {{.Server.Os}}' 2>/dev/null || true)"
  docker_os="${docker_os//$'\r'/}"

  case "${docker_os}" in
    *windows*) return 0 ;;
    *) return 1 ;;
  esac
}

workspace_mount_source() {
  local workspace_path="$1"
  local posix_workspace_path
  local windows_workspace_path

  posix_workspace_path="$(cd -- "${workspace_path}" && pwd -P)"

  if windows_workspace_path="$(cd -- "${workspace_path}" && pwd -W 2>/dev/null)"; then
    printf '%s\n' "${windows_workspace_path}"
    return
  fi

  if running_under_wsl && docker_expects_windows_paths && command -v wslpath >/dev/null 2>&1; then
    wslpath -w "${posix_workspace_path}"
    return
  fi

  if command -v cygpath >/dev/null 2>&1; then
    cygpath -aw "${posix_workspace_path}"
    return
  fi

  printf '%s\n' "${posix_workspace_path}"
}

DOCKER_COMMAND="$(select_docker_command)"
WORKSPACE_MOUNT="$(workspace_mount_source "${WORKSPACE}")"

"${DOCKER_COMMAND}" volume create "${TARGET_VOLUME}" >/dev/null

MSYS_NO_PATHCONV=1 "${DOCKER_COMMAND}" run --rm "${DOCKER_TTY_ARGS[@]}" \
  --mount "type=bind,source=${WORKSPACE_MOUNT},target=/workspace" \
  --mount "type=volume,source=${TARGET_VOLUME},target=/cargo-target" \
  --workdir /workspace \
  --env CARGO_TARGET_DIR=/cargo-target \
  "${IMAGE}" \
  "${COMMAND}"
