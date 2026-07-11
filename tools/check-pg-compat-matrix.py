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
DEFAULT_INVENTORY = ROOT / "docs" / "pg18-command-inventory.json"
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


def matrix_command_rows(matrix_path: Path) -> dict[str, str]:
    rows: dict[str, str] = {}
    in_command_table = False
    for line_number, line in enumerate(matrix_path.read_text(encoding="utf-8").splitlines(), 1):
        if line == "## PG18 command rows":
            in_command_table = True
            continue
        if line == "## Major language-feature rows":
            break
        if not in_command_table or not line.startswith("|") or line.startswith("|---"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 3 or cells[0] == "Item":
            continue
        item, disposition = cells[0], cells[1]
        if item in rows:
            raise ValueError(f"{matrix_path}:{line_number}: duplicate command row: {item}")
        rows[item] = disposition
    if not rows:
        raise ValueError(f"{matrix_path}: no command rows found")
    return rows


def load_inventory(path: Path) -> set[str]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"{path}: invalid JSON: {error}") from error
    if not isinstance(document, dict) or document.get("format_version") != 1:
        raise ValueError(f"{path}: unsupported inventory format")
    if document.get("postgresql_major") != 18:
        raise ValueError(f"{path}: inventory must be pinned to PostgreSQL 18")
    source = document.get("source")
    if not isinstance(source, str) or "/docs/18/" not in source:
        raise ValueError(f"{path}: inventory source must identify PostgreSQL 18 documentation")
    commands = document.get("commands")
    if not isinstance(commands, list) or not all(isinstance(command, str) for command in commands):
        raise ValueError(f"{path}: commands must be a JSON string array")
    if len(commands) != 190:
        raise ValueError(f"{path}: expected exactly 190 commands, found {len(commands)}")
    if len(set(commands)) != len(commands):
        raise ValueError(f"{path}: duplicate command names are forbidden")
    if commands != sorted(commands):
        raise ValueError(f"{path}: commands must be sorted")
    return set(commands)


def validate_inventory_rows(rows: dict[str, str], inventory: set[str]) -> list[str]:
    errors: list[str] = []
    missing = sorted(inventory - rows.keys())
    if missing:
        errors.append("matrix is missing authoritative command row(s): " + ", ".join(missing))
    extra = sorted(rows.keys() - inventory)
    if extra:
        errors.append("matrix command row(s) not in authoritative PG18 inventory: " + ", ".join(extra))
    return errors


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
    inventory = load_inventory(DEFAULT_INVENTORY)
    if len(inventory) != 190:
        raise AssertionError("self-test expected the pinned inventory to contain exactly 190 commands")

    missing_inventory = set(inventory)
    missing_inventory.remove("ABORT")
    inventory_errors = validate_inventory_rows(
        {command: "Wave-assigned(test)" for command in missing_inventory}, inventory
    )
    if not any("missing authoritative command" in error for error in inventory_errors):
        raise AssertionError("self-test expected missing-inventory-row direction to fail")

    extra_rows = {command: "Wave-assigned(test)" for command in inventory}
    extra_rows["RENAMED ABORT"] = "Wave-assigned(test)"
    inventory_errors = validate_inventory_rows(extra_rows, inventory)
    if not any("not in authoritative" in error for error in inventory_errors):
        raise AssertionError("self-test expected extra/renamed-row direction to fail")

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
        inventory = load_inventory(DEFAULT_INVENTORY)
        errors = validate_inventory_rows(matrix_command_rows(args.matrix), inventory)
        errors.extend(validate(args.matrix, parser_commands))
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
