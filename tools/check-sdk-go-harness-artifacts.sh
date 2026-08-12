#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APKO_CONFIG="${ROOT}/packaging/apko/crabka-gateway.yaml"
COMPOSE_CONFIG="${ROOT}/sdks/go/testdata/docker-compose.yml"
GO_INTEGRATION_TEST="${ROOT}/sdks/go/integration_smoke_test.go"
MELANGE_CONFIG="${ROOT}/packaging/melange/crabka.yaml"
PUBLISH_IMAGES_WORKFLOW="${ROOT}/.github/workflows/publish-images.yml"
SDK_GO_WORKFLOW="${ROOT}/.github/workflows/sdk-go.yml"

require_file() {
  local path="$1"
  if [[ -f "${path}" ]]; then
    return 0
  fi
  printf 'missing required harness artifact: %s\n' "${path}" >&2
  return 1
}

require_line() {
  local path="$1"
  local pattern="$2"
  if grep -Eq -- "${pattern}" "${path}"; then
    return 0
  fi
  printf 'artifact %s does not match required shape: %s\n' "${path}" "${pattern}" >&2
  return 1
}

require_command_or_note() {
  local command_name="$1"
  local reason="$2"
  if command -v "${command_name}" >/dev/null 2>&1; then
    return 0
  fi
  printf 'note: %s is not installed; relying on static %s checks\n' "${command_name}" "${reason}" >&2
  return 1
}

require_file "${APKO_CONFIG}"
require_file "${COMPOSE_CONFIG}"
require_file "${GO_INTEGRATION_TEST}"
require_file "${MELANGE_CONFIG}"
require_file "${PUBLISH_IMAGES_WORKFLOW}"
require_file "${SDK_GO_WORKFLOW}"

require_line "${APKO_CONFIG}" '^    - crabka-grpc-gateway$'
require_line "${APKO_CONFIG}" '^  command: /usr/bin/crabka-grpc-gateway$'
require_line "${APKO_CONFIG}" '^  - x86_64$'
require_line "${APKO_CONFIG}" '^  - aarch64$'

require_line "${MELANGE_CONFIG}" '-p crabka-grpc-gateway'
require_line "${MELANGE_CONFIG}" '--bin crabka-grpc-gateway'
require_line "${MELANGE_CONFIG}" '^  - name: crabka-grpc-gateway$'
require_line "${MELANGE_CONFIG}" 'install -D -m 0755 dist/crabka-grpc-gateway .*/usr/bin/crabka-grpc-gateway'

require_line "${PUBLISH_IMAGES_WORKFLOW}" '^          - crabka-gateway$'
require_line "${PUBLISH_IMAGES_WORKFLOW}" 'packaging/apko/\$\{IMAGE\}\.yaml'

require_line "${COMPOSE_CONFIG}" '^  broker:$'
require_line "${COMPOSE_CONFIG}" '^  gateway:$'
require_line "${COMPOSE_CONFIG}" 'image: \$\{CRABKA_BROKER_IMAGE:-ghcr\.io/robot-head/crabka-broker:edge\}'
require_line "${COMPOSE_CONFIG}" 'image: \$\{CRABKA_GATEWAY_IMAGE:-ghcr\.io/robot-head/crabka-gateway:edge\}'
require_line "${COMPOSE_CONFIG}" 'condition: service_healthy'
require_line "${COMPOSE_CONFIG}" 'CRABKA_BOOTSTRAP_SERVERS: broker:9092'
require_line "${COMPOSE_CONFIG}" 'CRABKA_GATEWAY_LISTEN_ADDR: 0\.0\.0\.0:9500'
require_line "${COMPOSE_CONFIG}" 'CRABKA_GATEWAY_ADVERTISED_ADDR: gateway:9500'
require_line "${COMPOSE_CONFIG}" '"\$\{CRABKA_BROKER_PORT:-9092\}:9092"'
require_line "${COMPOSE_CONFIG}" '"\$\{CRABKA_GATEWAY_PORT:-9500\}:9500"'
require_line "${COMPOSE_CONFIG}" 'nc -z 127\.0\.0\.1 9092'
require_line "${COMPOSE_CONFIG}" 'nc -z 127\.0\.0\.1 9500'

require_line "${GO_INTEGRATION_TEST}" '^//go:build integration$'
require_line "${GO_INTEGRATION_TEST}" 'CRABKA_GO_INTEGRATION'
require_line "${GO_INTEGRATION_TEST}" 'CRABKA_GATEWAY_ENDPOINT'
require_line "${GO_INTEGRATION_TEST}" 'checkGatewayHealthOverSDKTransport'

require_line "${SDK_GO_WORKFLOW}" 'CRABKA_GO_INTEGRATION=1'
require_line "${SDK_GO_WORKFLOW}" 'CRABKA_GATEWAY_ENDPOINT="http://127\.0\.0\.1:\$\{CRABKA_GATEWAY_PORT:-9500\}"'
require_line "${SDK_GO_WORKFLOW}" 'go test -tags integration ./\.\.\.'

if require_command_or_note apko 'apko config shape'; then
  apko show-config "${APKO_CONFIG}" >/dev/null
fi

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  docker compose -f "${COMPOSE_CONFIG}" config --quiet
else
  printf 'note: docker compose is not installed; static compose checks passed, live harness remains external\n' >&2
fi

if command -v ruby >/dev/null 2>&1; then
  ruby -e 'require "yaml"; YAML.load_file(ARGV.fetch(0))' "${SDK_GO_WORKFLOW}" >/dev/null
else
  printf 'note: ruby is not installed; skipping sdk-go.yml YAML parse check\n' >&2
fi

printf 'Go SDK harness artifacts are present and coherent for default CI checks\n'
