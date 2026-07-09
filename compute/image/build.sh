#!/usr/bin/env bash
set -euo pipefail

readonly PG_VERSION="${PG_VERSION:-17.5}"
readonly PG_TARBALL="postgresql-${PG_VERSION}.tar.bz2"
readonly PG_URL="${PG_URL:-https://ftp.postgresql.org/pub/source/v${PG_VERSION}/${PG_TARBALL}}"
readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/compute/.build}"
readonly SRC_DIR="${BUILD_DIR}/postgresql-${PG_VERSION}"
readonly INSTALL_DIR="${INSTALL_DIR:-${BUILD_DIR}/pg-install}"
readonly IMAGE_TAG="${IMAGE_TAG:-crabka/compute-pg17:local}"

require_command() {
    local command_name="$1"

    if command -v "${command_name}" >/dev/null 2>&1; then
        return 0
    fi

    printf 'required command not found: %s\n' "${command_name}" >&2
    return 1
}

fetch_postgres_source() {
    mkdir -p "${BUILD_DIR}"
    if [ -d "${SRC_DIR}" ]; then
        return 0
    fi

    require_command curl
    require_command tar
    curl --fail --location --output "${BUILD_DIR}/${PG_TARBALL}" "${PG_URL}"
    tar -xjf "${BUILD_DIR}/${PG_TARBALL}" -C "${BUILD_DIR}"
}

apply_crabka_patches() {
    require_command git
    git -C "${SRC_DIR}" apply --check "${ROOT_DIR}/compute/patches/pg17/0001-smgr-hook.patch"
    git -C "${SRC_DIR}" apply "${ROOT_DIR}/compute/patches/pg17/0001-smgr-hook.patch"
}

build_postgres() {
    require_command make
    (cd "${SRC_DIR}" && ./configure --prefix="${INSTALL_DIR}")
    make -C "${SRC_DIR}" -j"${JOBS:-$(nproc)}"
    make -C "${SRC_DIR}" install
}

build_compute_client() {
    require_command cargo
    cargo build -p crabka-compute-client --release
}

build_extension() {
    make -C "${ROOT_DIR}/compute/extension" PG_CONFIG="${INSTALL_DIR}/bin/pg_config"
    make -C "${ROOT_DIR}/compute/extension" PG_CONFIG="${INSTALL_DIR}/bin/pg_config" install
}

assemble_image() {
    require_command docker
    local image_context="${BUILD_DIR}/image-context"

    rm -rf "${image_context}"
    mkdir -p "${image_context}"
    cp -a "${INSTALL_DIR}" "${image_context}/pg-install"
    cp "${ROOT_DIR}/compute/image/Dockerfile" "${image_context}/Dockerfile"
    cp "${ROOT_DIR}/compute/image/entrypoint.sh" "${image_context}/entrypoint.sh"

    docker build \
        --tag "${IMAGE_TAG}" \
        "${image_context}"
}

main() {
    fetch_postgres_source
    apply_crabka_patches
    build_postgres
    build_compute_client
    build_extension
    assemble_image
}

main "$@"
