#!/usr/bin/env bash
set -euo pipefail

gateway_url="${CRABKA_GATEWAY_URL:-http://127.0.0.1:9500}"
topic="${CRABKA_CE_TOPIC:-cloudevents-demo}"
capture_port="${CRABKA_CE_CAPTURE_PORT:-18080}"
capture_dir="$(mktemp -d "${TMPDIR:-/tmp}/crabka-ce-smoke.XXXXXX")"
capture_file="${capture_dir}/requests.jsonl"

cleanup() {
  if [[ -n "${capture_pid:-}" ]]; then
    kill "${capture_pid}" 2>/dev/null || true
  fi
  rm -rf "${capture_dir}"
}
trap cleanup EXIT

require_command() {
  local command_name="$1"
  if command -v "${command_name}" >/dev/null 2>&1; then
    return 0
  fi
  printf 'missing required command: %s\n' "${command_name}" >&2
  exit 127
}

wait_for_capture_server() {
  for _ in {1..50}; do
    if curl -fsS "http://127.0.0.1:${capture_port}/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  printf 'capture server did not start on 127.0.0.1:%s\n' "${capture_port}" >&2
  return 1
}

post_binary_cloudevent() {
  curl -fsS -X POST "${gateway_url}/v1/produce/${topic}" \
    -H 'ce-id: demo-binary-1' \
    -H 'ce-source: /demo/cloudevents/smoke.sh' \
    -H 'ce-type: com.example.crabka.binary' \
    -H 'ce-specversion: 1.0' \
    -H 'content-type: application/json' \
    --data '{"mode":"binary","ok":true}' >/dev/null
}

post_structured_cloudevent() {
  curl -fsS -X POST "${gateway_url}/v1/webhooks/events" \
    -H 'content-type: application/cloudevents+json' \
    --data '{"specversion":"1.0","id":"demo-structured-1","source":"/demo/cloudevents/smoke.sh","type":"com.example.crabka.structured","datacontenttype":"application/json","data":{"mode":"structured","ok":true}}' >/dev/null
}

wait_for_egress() {
  python3 - "$capture_file" <<'PY'
import json
import pathlib
import sys
import time

capture_path = pathlib.Path(sys.argv[1])
deadline = time.monotonic() + 20

while time.monotonic() < deadline:
    requests = []
    if capture_path.exists():
        for line in capture_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                requests.append(json.loads(line))

    saw_binary = any(
        request["headers"].get("ce-id") == "demo-binary-1"
        and request["headers"].get("ce-type") == "com.example.crabka.binary"
        and request["body"] == {"mode": "binary", "ok": True}
        for request in requests
    )
    saw_structured = any(
        request["headers"].get("content-type", "").startswith("application/cloudevents+json")
        and request["body"].get("id") == "demo-structured-1"
        and request["body"].get("data") == {"mode": "structured", "ok": True}
        for request in requests
        if isinstance(request.get("body"), dict)
    )

    if saw_binary and saw_structured:
        print(f"captured {len(requests)} webhook deliveries")
        sys.exit(0)

    time.sleep(0.2)

print("timed out waiting for CloudEvents binary and structured egress", file=sys.stderr)
if capture_path.exists():
    print(capture_path.read_text(encoding="utf-8"), file=sys.stderr)
sys.exit(1)
PY
}

require_command curl
require_command python3

python3 - "${capture_port}" "${capture_file}" <<'PY' &
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import pathlib
import sys

port = int(sys.argv[1])
capture_path = pathlib.Path(sys.argv[2])

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/healthz":
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(204)
        self.end_headers()

    def do_POST(self):
        length = int(self.headers.get("content-length", "0"))
        raw_body = self.rfile.read(length)
        try:
            body = json.loads(raw_body.decode("utf-8"))
        except Exception:
            body = raw_body.decode("utf-8", errors="replace")
        request = {
            "path": self.path,
            "headers": {key.lower(): value for key, value in self.headers.items()},
            "body": body,
        }
        with capture_path.open("a", encoding="utf-8") as output:
            output.write(json.dumps(request, sort_keys=True) + "\n")
        self.send_response(204)
        self.end_headers()

    def log_message(self, format, *args):
        return

ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
PY
capture_pid="$!"

wait_for_capture_server
post_binary_cloudevent
post_structured_cloudevent
wait_for_egress

printf 'CloudEvents smoke passed through %s topic %s\n' "${gateway_url}" "${topic}"
