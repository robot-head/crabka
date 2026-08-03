#!/usr/bin/env python3
"""Structural contract for the F-0 CI and PgDog runtime gates."""

from __future__ import annotations

import ast
import re
import shlex
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def source(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def uncommented_shell(path: str) -> str:
    lines = []
    for line in source(path).splitlines():
        if not line.lstrip().startswith("#"):
            lines.append(line)
    return "\n".join(lines)


def rust_without_comments(path: str) -> str:
    text = source(path)
    output: list[str] = []
    index = 0
    in_string = False
    while index < len(text):
        if not in_string and text.startswith("//", index):
            newline = text.find("\n", index)
            index = len(text) if newline < 0 else newline
        elif not in_string and text.startswith("/*", index):
            end = text.find("*/", index + 2)
            assert end >= 0, "unterminated Rust block comment"
            index = end + 2
        else:
            char = text[index]
            output.append(char)
            if char == '"' and (index == 0 or text[index - 1] != "\\"):
                in_string = not in_string
            index += 1
    return "".join(output)


def workflow_step(name: str) -> str:
    workflow = source(".github/workflows/ci.yml")
    match = re.search(
        rf"^\s+- name: {re.escape(name)}\n(?P<body>(?:\s{{8,}}.*\n)+)",
        workflow,
        re.MULTILINE,
    )
    assert match, f"missing CI step: {name}"
    return match.group("body")


def normalized_commands(text: str) -> str:
    return re.sub(r"\\\n\s*", " ", text)


e2e = normalized_commands(uncommented_shell("scripts/gres-e2e.sh"))
conformance = next(
    line for line in e2e.splitlines() if line.startswith("./target/debug/crabka-gres-conformance ")
)
for option, value in {
    "--extended-corpus": "crates/gres-conformance/corpus-extended",
    "--extended-baseline": "crates/gres-conformance/corpus-extended/baseline.json",
    "--extended-out": '"${ARTIFACT_DIR}/extended-parity-pgdog.json"',
    "--extended-summary": '"${ARTIFACT_DIR}/extended-parity-pgdog.md"',
}.items():
    assert f"{option} {value}" in conformance, f"PgDog corpus command missing {option}"

assert re.search(
    r'^DATABASE_URL=.* timeout 30s ./target/debug/crabka-gres-driver-smoke ',
    e2e,
    re.MULTILINE,
), "Rust driver smoke must have a command-level timeout"
assert re.search(r'^DATABASE_URL=.* timeout 30s python3 - <<\'PY\'', e2e, re.MULTILINE), (
    "Python driver smoke must have a command-level timeout"
)
assert e2e.count("connect_timeout=5") >= 2, "both driver URLs need connect_timeout"

rust = rust_without_comments("crates/gres-conformance/src/bin/driver_smoke.rs")
assert "enum Driver" in rust and "value_enum" in rust, (
    "Rust smoke must independently select tokio-postgres or sqlx for protocol diagnosis"
)
for function, query, transaction, values in (
    ("tokio_postgres_smoke", 'query_one("SELECT $1::int4"', "client.transaction()", "[41_i32, 42_i32]"),
    ("sqlx_smoke", 'query_scalar("SELECT $1::int4")', "connection.begin()", "[51_i32, 52_i32]"),
):
    body = re.search(rf"async fn {function}\b(?P<body>.*?\n}})", rust, re.DOTALL)
    assert body, f"missing Rust driver function {function}"
    code = body.group("body")
    assert query in code and transaction in code and values in code
    assert ".commit().await?" in code and "actual != expected" in code
assert ".bind(expected)" in rust, "sqlx must bind a real parameter"

python_block = re.search(r"<<'PY'.*?\n(?P<code>import os\n.*)\nPY", e2e, re.DOTALL)
assert python_block, "missing executable psycopg heredoc"
tree = ast.parse(python_block.group("code"))
calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
execute = next(
    node
    for node in calls
    if isinstance(node.func, ast.Attribute) and node.func.attr == "execute"
)
assert isinstance(execute.args[0], ast.Constant) and "%s::int4" in execute.args[0].value
assert isinstance(execute.args[1], ast.Tuple), "psycopg execute must receive parameter tuple"
assert any(
    isinstance(node.func, ast.Attribute) and node.func.attr == "transaction" for node in calls
), "psycopg must use explicit transactions"
assert any(
    isinstance(node, ast.For)
    and isinstance(node.iter, ast.Tuple)
    and len(node.iter.elts) == 2
    for node in ast.walk(tree)
), "psycopg must reuse its connection across two transactions"

golden = source("crates/gres-control/tests/golden/pgdog.toml")
assert re.search(r'^pooler_mode = "transaction"$', golden, re.MULTILINE)

install = workflow_step("Install pinned Python PostgreSQL driver")
assert "pip install --require-hashes --no-deps -r crates/gres-conformance/requirements-driver-smoke.txt" in install
requirements = source("crates/gres-conformance/requirements-driver-smoke.txt")
active_requirements = [line for line in requirements.splitlines() if line and not line.startswith("#")]
assert active_requirements and all("==" in line and "--hash=sha256:" in line for line in active_requirements)

contract_step = workflow_step("F-0 runtime wiring contract")
assert "python3 scripts/tests/gres_f0_runtime_gates.py" in contract_step
capture_contract = workflow_step("Driver capture safety contract")
assert "python3 -m unittest tools/tests/test_gres_wire_recorder.py tools/tests/test_capture_gres_driver_goldens.py" in capture_contract
front_door = workflow_step("Front-door PgDog e2e gate")
assert "--skip-pgdog" not in front_door and "CRABKA_GRES_E2E_KEEP_ARTIFACTS=1" in front_door
driver_goldens = workflow_step("Captured driver startup replay")
assert "timeout 30s aspect gres --task:name=gres-driver-goldens --suite driver-goldens" in driver_goldens

workflow = source(".github/workflows/ci.yml")
gres_filter = re.search(
    r"^\s{12}gres:\n(?P<body>(?:^\s{14,}.*\n)+)",
    workflow,
    re.MULTILINE,
)
assert gres_filter, "missing Gres changed-files filter"
for path in (
    "tools/capture-gres-driver-goldens.py",
    "tools/gres-wire-recorder.py",
    "tools/tests/test_gres_wire_recorder.py",
    "tools/tests/test_capture_gres_driver_goldens.py",
    "scripts/gres-driver-goldens-gate.sh",
    "scripts/tests/gres_f0_runtime_gates.py",
):
    assert f"- '{path}'" in gres_filter.group("body"), f"Gres filter missing {path}"

for step_name, artifact in (
    ("Conformance harness against the parity baseline", "extended-parity-standalone"),
    ("Conformance against the substrate-backed engine", "extended-parity-substrate"),
):
    leg = normalized_commands(workflow_step(step_name))
    assert "--extended-corpus crates/gres-conformance/corpus-extended" in leg
    assert "--extended-baseline crates/gres-conformance/corpus-extended/baseline.json" in leg
    assert f"--extended-out {artifact}.json" in leg
    assert f"--extended-summary {artifact}.md" in leg
substrate_leg = " ".join(
    normalized_commands(workflow_step("Conformance against the substrate-backed engine")).split()
)
for fragment in (
    "./target/debug/crabka gres create-tenant --bootstrap 127.0.0.1:9092 --name conformance --user crab --password-stdin",
    "./target/debug/crabka-gres --listen 127.0.0.1:54334 --substrate-bootstrap 127.0.0.1:9092 --tenant conformance --auth trust",
):
    assert fragment in substrate_leg, f"substrate conformance leg missing {fragment}"
summary = workflow_step("Publish extended parity summaries")
assert "cat extended-parity-standalone.md extended-parity-substrate.md" in summary
upload = workflow_step("Upload parity report")
assert all(name in upload for name in ("extended-parity-standalone.json", "extended-parity-substrate.json"))
pgdog_upload = workflow_step("Upload Gres front-door e2e artifacts")
assert "!cancelled()" in pgdog_upload and "hashFiles('target/gres-e2e-artifacts/**')" in pgdog_upload
assert "extended-parity-pgdog.json" in pgdog_upload and "extended-parity-pgdog.md" in pgdog_upload
coldstart_upload = workflow_step("Upload Gres cold-start artifacts")
assert "!cancelled()" in coldstart_upload and "hashFiles('target/gres-coldstart-artifacts/**')" in coldstart_upload

print("PASS: structurally validated F-0 runtime and CI gates")
