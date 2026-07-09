#!/usr/bin/env bash
set -euo pipefail

if [ -z "${PGDATA:-}" ]; then
    printf 'PGDATA must be set\n' >&2
    exit 1
fi

if [ ! -s "${PGDATA}/PG_VERSION" ]; then
    if [ -z "${CRABKA_BASEBACKUP_COMMAND:-}" ]; then
        printf 'PGDATA is empty and CRABKA_BASEBACKUP_COMMAND is not set; live boot remains externally gated\n' >&2
        exit 1
    fi

    mkdir -p "${PGDATA}"
    sh -c "${CRABKA_BASEBACKUP_COMMAND}"
fi

exec "$@"
