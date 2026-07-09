#!/usr/bin/env bash
set -euo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly PATCH_FILE="${ROOT_DIR}/compute/patches/pg17/0001-smgr-hook.patch"
readonly EXTENSION_C="${ROOT_DIR}/compute/extension/crabka.c"
readonly HEADER="${ROOT_DIR}/compute/include/crabka_compute_client.h"
readonly BUILD_SCRIPT="${ROOT_DIR}/compute/image/build.sh"
readonly ENTRYPOINT="${ROOT_DIR}/compute/image/entrypoint.sh"

readonly SMGR_FIELDS=(
    smgr_init
    smgr_shutdown
    smgr_open
    smgr_close
    smgr_create
    smgr_exists
    smgr_unlink
    smgr_extend
    smgr_zeroextend
    smgr_prefetch
    smgr_readv
    smgr_writev
    smgr_writeback
    smgr_nblocks
    smgr_truncate
    smgr_immedsync
    smgr_registersync
)

require_file() {
    local path="$1"

    if [ -f "${path}" ]; then
        return 0
    fi

    printf 'missing required artifact: %s\n' "${path}" >&2
    return 1
}

require_text() {
    local path="$1"
    local text="$2"

    if grep -F --quiet -- "${text}" "${path}"; then
        return 0
    fi

    printf 'artifact %s does not contain required text: %s\n' "${path}" "${text}" >&2
    return 1
}

check_required_files() {
    require_file "${PATCH_FILE}"
    require_file "${HEADER}"
    require_file "${ROOT_DIR}/compute/extension/Makefile"
    require_file "${EXTENSION_C}"
    require_file "${BUILD_SCRIPT}"
    require_file "${ENTRYPOINT}"
    require_file "${ROOT_DIR}/compute/image/Dockerfile"
}

check_patch_shape() {
    require_text "${PATCH_FILE}" "smgr_hook_type"
    require_text "${PATCH_FILE}" "smgr_hook"
    require_text "${PATCH_FILE}" "smgr_hook_methods"
    require_text "${PATCH_FILE}" "smgr_hook_methods->smgr_readv"
    require_text "${PATCH_FILE}" "smgr_hook_methods->smgr_writev"

    for field in "${SMGR_FIELDS[@]}"; do
        require_text "${PATCH_FILE}" "(*${field})"
    done

    git apply --stat --summary "${PATCH_FILE}" >/dev/null

    if [ -z "${PG17_SOURCE_DIR:-}" ]; then
        printf 'SKIP: PG17_SOURCE_DIR is unset; not running git apply --check for smgr patch\n' >&2
        return 0
    fi
    if [ ! -d "${PG17_SOURCE_DIR}" ]; then
        printf 'PG17_SOURCE_DIR does not name a directory: %s\n' "${PG17_SOURCE_DIR}" >&2
        return 1
    fi

    git -C "${PG17_SOURCE_DIR}" apply --check "${PATCH_FILE}"
}

check_extension_shape() {
    require_text "${EXTENSION_C}" "_PG_init"
    require_text "${EXTENSION_C}" "crabka.pageserver_endpoint"
    require_text "${EXTENSION_C}" "ck_connect"
    require_text "${EXTENSION_C}" "ck_get_page"
    require_text "${EXTENSION_C}" "CrabkaComputePageFetchRequest"
    require_text "${EXTENSION_C}" "reln->smgr_rlocator.locator"
    require_text "${EXTENSION_C}" "smgr_hook = crabka_select_smgr"

    for field in "${SMGR_FIELDS[@]}"; do
        require_text "${EXTENSION_C}" ".${field} ="
    done

    if grep -F --quiet -- ".smgr_read =" "${EXTENSION_C}"; then
        printf 'artifact %s uses obsolete f_smgr field: .smgr_read\n' "${EXTENSION_C}" >&2
        return 1
    fi
    if grep -F --quiet -- ".smgr_write =" "${EXTENSION_C}"; then
        printf 'artifact %s uses obsolete f_smgr field: .smgr_write\n' "${EXTENSION_C}" >&2
        return 1
    fi
}

check_header_transport_gate() {
    require_text "${HEADER}" "typedef struct CrabkaComputeClient CrabkaComputeClient"
    require_text "${HEADER}" "ck_connect"
    require_text "${HEADER}" "ck_get_page"
    require_text "${HEADER}" "CRABKA_COMPUTE_PAGE_SIZE"

    local probe
    probe="$(mktemp "${TMPDIR:-/tmp}/crabka-compute-header-probe.XXXXXX.c")"
    cat >"${probe}" <<'EOF'
#include <stddef.h>
#include <stdint.h>
#include "crabka_compute_client.h"

_Static_assert(CRABKA_COMPUTE_RESULT_OK == 0, "ok result code");
_Static_assert(CRABKA_COMPUTE_RESULT_INTERNAL_ERROR == -2, "internal error result code");
_Static_assert(CRABKA_COMPUTE_STATUS_INVALID_ARGUMENT == 1U, "invalid argument status");
_Static_assert(CRABKA_COMPUTE_PAGE_SIZE == 8192U, "page size");

int main(void) {
    CrabkaComputeClient *client = NULL;
    CrabkaComputePageFetchRequest request = {0};
    uint8_t page[CRABKA_COMPUTE_PAGE_SIZE] = {0};
    (void)ck_connect("http://127.0.0.1:9898", &client);
    (void)ck_get_page(client, &request, page, sizeof(page));
    ck_disconnect(client);
    (void)ck_last_error_message();
    return 0;
}
EOF
    cc -std=c11 -fsyntax-only -I"$(dirname "${HEADER}")" "${probe}"
    rm -f "${probe}"
}

check_shell_syntax() {
    bash -n "${BASH_SOURCE[0]}"
    bash -n "${BUILD_SCRIPT}"
    bash -n "${ENTRYPOINT}"
}

main() {
    check_required_files
    check_patch_shape
    check_extension_shape
    check_header_transport_gate
    check_shell_syntax
}

main "$@"
