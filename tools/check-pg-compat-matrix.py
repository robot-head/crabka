#!/usr/bin/env python3
"""Validate the Chapter Gres PostgreSQL compatibility matrix.

The check is intentionally small and deterministic: parse the markdown matrix,
query the parser-command helper, and ensure every accepted command is answered
by a resolved row. Resolved rows are either implemented/mapped or a clear
SQLSTATE refusal, which prevents parsed-but-rejected commands from being
documented as executable. It also rejects undecided or malformed dispositions so
the matrix remains a useful planning artifact between waves.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIX = ROOT / "docs" / "PG_COMPAT_MATRIX.md"
PARSER_COMMAND_REPORT_FORMAT_VERSION = 1
DEFAULT_PARSER_COMMAND = [
    "cargo",
    "run",
    "--quiet",
    "--locked",
    "-p",
    "crabka-gres-conformance",
    "--bin",
    "crabka-gres-parser-commands",
]

VALID_DISPOSITION = re.compile(
    r"^(Implemented|Wave-assigned\([^)]+\)|Mapped\([^)]+\)|"
    r"Error-with-notice\([0-9A-Z]{5}\)|Non-goal\([^)]+\))$"
)
RESOLVED_PARSER_PREFIXES = ("Mapped(", "Error-with-notice(")

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def accepted_commands() -> set[str]:
    result = subprocess.run(
        DEFAULT_PARSER_COMMAND,
        cwd=ROOT,
        check=False,
        capture_output=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        raise ValueError(
            "parser command helper failed"
            + (f": {stderr}" if stderr else f" with exit code {result.returncode}")
        )

    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise ValueError(f"parser command helper emitted invalid JSON: {error}") from error
    if not isinstance(report, dict):
        raise ValueError("parser command helper report must be a JSON object")
    if report.get("format_version") != PARSER_COMMAND_REPORT_FORMAT_VERSION:
        raise ValueError(
            "parser command helper report has unsupported format_version: "
            f"{report.get('format_version')!r}"
        )

    commands = report.get("commands")
    if not isinstance(commands, list) or not all(isinstance(command, str) for command in commands):
        raise ValueError("parser command helper report commands must be a JSON string array")
    if commands != sorted(commands) or len(commands) != len(set(commands)):
        raise ValueError("parser command helper report commands must be sorted and unique")
    if not commands:
        raise ValueError("parser command helper report commands must not be empty")
    return set(commands)


def matrix_rows(matrix_path: Path) -> dict[str, str]:
    rows: dict[str, str] = {}
    for line_number, line in enumerate(matrix_path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.startswith("|") or line.startswith("|---"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 3 or cells[0] == "Item":
            continue
        item, disposition = cells[0], cells[1]
        if item in rows:
            raise ValueError(f"{matrix_path}:{line_number}: duplicate item row: {item}")
        if "UNDECIDED" in disposition:
            raise ValueError(f"{matrix_path}:{line_number}: undecided disposition for {item}")
        if VALID_DISPOSITION.match(disposition) is None:
            raise ValueError(f"{matrix_path}:{line_number}: invalid disposition for {item}: {disposition}")
        rows[item] = disposition
    if not rows:
        raise ValueError(f"{matrix_path}: no matrix rows found")
    return rows


def is_resolved_parser_disposition(disposition: str) -> bool:
    return disposition == "Implemented" or disposition.startswith(RESOLVED_PARSER_PREFIXES)


def validate(matrix_path: Path, parser_commands: set[str]) -> list[str]:
    rows = matrix_rows(matrix_path)
    resolved_parser_rows = {
        item
        for item, disposition in rows.items()
        if is_resolved_parser_disposition(disposition)
    }

    errors: list[str] = []
    missing_rows = sorted(parser_commands - rows.keys())
    if missing_rows:
        errors.append("parser accepts command(s) with no matrix row: " + ", ".join(missing_rows))

    stale_rows = sorted(parser_commands - resolved_parser_rows)
    if stale_rows:
        errors.append(
            "parser accepts command(s) without a resolved Implemented/Mapped/Error-with-notice matrix row: "
            + ", ".join(stale_rows)
        )
    return errors


def self_test_errors(matrix: str, parser_commands: set[str]) -> list[str]:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "PG_COMPAT_MATRIX.md"
        path.write_text(matrix, encoding="utf-8")
        return validate(path, parser_commands)


def run_self_test(parser_commands: set[str]) -> None:
    required_commands = {"BEGIN", "END", "ROLLBACK", "START TRANSACTION"}
    missing_commands = sorted(required_commands - parser_commands)
    if missing_commands:
        raise AssertionError(
            "self-test expected parser command helper commands: " + ", ".join(missing_commands)
        )

    incomplete_matrix = """| Item | Disposition | Notes |\n|---|---|---|\n| BEGIN | Implemented | ok |\n"""
    incomplete_errors = self_test_errors(incomplete_matrix, parser_commands)
    if not incomplete_errors:
        raise AssertionError("self-test expected the deliberately incomplete matrix to fail")

    malformed_matrix = """| Item | Disposition | Notes |\n|---|---|---|\n| BEGIN | UNDECIDED | bad |\n"""
    try:
        self_test_errors(malformed_matrix, parser_commands)
    except ValueError as error:
        if "undecided disposition" in str(error):
            return
        raise AssertionError(f"self-test expected undecided-disposition failure, got: {error}") from error

    raise AssertionError("self-test expected the deliberately malformed matrix to fail")


def main() -> int:
    args = parse_args()
    try:
        parser_commands = accepted_commands()
        if args.self_test:
            run_self_test(parser_commands)
        errors = validate(args.matrix, parser_commands)
    except (OSError, ValueError, AssertionError) as error:
        print(f"pg compat matrix check FAILED: {error}", file=sys.stderr)
        return 1

    if errors:
        print("pg compat matrix check FAILED:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print(f"pg compat matrix check passed: {args.matrix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
