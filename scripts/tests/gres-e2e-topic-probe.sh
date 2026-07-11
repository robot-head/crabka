#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

run_classifier() {
    local status="$1"
    local output="$2"
    CRABKA_GRES_E2E_TEST_CLASSIFY_STATUS="$status" \
        CRABKA_GRES_E2E_TEST_CLASSIFY_OUTPUT="$output" \
        scripts/gres-e2e.sh
}

run_classifier 1 'TopicAuthorizationException: Not authorized to access topics: [__gres_tenants]' \
    | grep -Fx 'denied'

if run_classifier 0 ''; then
    echo 'classifier accepted a successful fetch' >&2
    exit 1
fi

if run_classifier 1 'TimeoutException: Timeout expired while fetching records'; then
    echo 'classifier accepted an empty/timeout result as authorization denial' >&2
    exit 1
fi

echo 'PASS: named-topic probe only accepts explicit authorization denial'
