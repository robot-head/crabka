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

run_classifier 1 'crabka gres: topic __gres_tenants metadata: UNKNOWN (29)' \
    | grep -Fx 'denied'

if run_classifier 1 'crabka gres: topic another-topic metadata: UNKNOWN (29)'; then
    echo 'classifier accepted authorization denial for the wrong topic' >&2
    exit 1
fi

if run_classifier 1 'TopicAuthorizationException: Not authorized'; then
    echo 'classifier accepted an unscoped authorization string' >&2
    exit 1
fi

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
grep -Fq 'crabka gres probe-topic-read' scripts/gres-e2e.sh
grep -Fq -- '--password-file "$client_properties"' scripts/gres-e2e.sh
if grep -Fq 'kafka-console-consumer.sh' scripts/gres-e2e.sh; then
    echo 'global registry ACL proof still depends on the JVM console consumer' >&2
    exit 1
fi

echo 'PASS: named-topic denial classification and native bounded protocol probe contract'
