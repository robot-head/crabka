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
run_classifier 0 'TOPIC_AUTHORIZATION_FAILED: TopicAuthorizationException: Not authorized to access topics: [__gres_tenants]' \
    | grep -Fx 'denied'

if run_classifier 0 ''; then
    echo 'classifier accepted a successful fetch' >&2
    exit 1
fi

if run_classifier 1 'TimeoutException: Timeout expired while fetching records'; then
    echo 'classifier accepted an empty/timeout result as authorization denial' >&2
    exit 1
fi

grep -Fq 'timeout 10s docker info' scripts/gres-e2e.sh
grep -Fq 'timeout 120s docker pull "$KAFKA_IMAGE"' scripts/gres-e2e.sh
grep -Fq 'chmod 600 "$client_properties"' scripts/gres-e2e.sh
grep -Fq -- '--user "$(id -u):$(id -g)"' scripts/gres-e2e.sh

echo 'PASS: named-topic denial classification including zero-exit auth errors, Docker bounds, and credential mount ownership'
