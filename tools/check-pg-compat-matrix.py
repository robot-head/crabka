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
import hashlib
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIX = ROOT / "docs" / "PG_COMPAT_MATRIX.md"
DEFAULT_INVENTORY = ROOT / "docs" / "pg18-command-inventory.json"
PG18_SOURCE_SHA256 = "4240987b5fddaa5ab5ffa2562551cb1325f2e5527b552c3bbe5be7ca6fd42fc7"
PG18_SOURCE_ALIASES = {
    "ALTER OPCLASS": "ALTER OPERATOR CLASS", "ALTER OPFAMILY": "ALTER OPERATOR FAMILY",
    "ALTER TSCONFIG": "ALTER TEXT SEARCH CONFIGURATION", "ALTER TSDICTIONARY": "ALTER TEXT SEARCH DICTIONARY",
    "ALTER TSPARSER": "ALTER TEXT SEARCH PARSER", "ALTER TSTEMPLATE": "ALTER TEXT SEARCH TEMPLATE",
    "CREATE OPCLASS": "CREATE OPERATOR CLASS", "CREATE OPFAMILY": "CREATE OPERATOR FAMILY",
    "CREATE TSCONFIG": "CREATE TEXT SEARCH CONFIGURATION", "CREATE TSDICTIONARY": "CREATE TEXT SEARCH DICTIONARY",
    "CREATE TSPARSER": "CREATE TEXT SEARCH PARSER", "CREATE TSTEMPLATE": "CREATE TEXT SEARCH TEMPLATE",
    "DROP OPCLASS": "DROP OPERATOR CLASS", "DROP OPFAMILY": "DROP OPERATOR FAMILY",
    "DROP TSCONFIG": "DROP TEXT SEARCH CONFIGURATION", "DROP TSDICTIONARY": "DROP TEXT SEARCH DICTIONARY",
    "DROP TSPARSER": "DROP TEXT SEARCH PARSER", "DROP TSTEMPLATE": "DROP TEXT SEARCH TEMPLATE",
    "ROLLBACK TO": "ROLLBACK TO SAVEPOINT", "SET SESSION AUTH": "SET SESSION AUTHORIZATION",
}
PG18_SYNTAX_SNAPSHOT_HASHES = {
    "pg18-alter_table-REL_18_0.sgml": "dc44b2b50476dff8ed0e7f79d425e6b404f3b0860a91f18f536490f912c02dbe",
    "pg18-create_table-REL_18_0.sgml": "8f281d48523129f41a81d6c6e1fdc4d6de7637cf31f36f5c63940fd2d1b51972",
    "pg18-select-REL_18_0.sgml": "300d0d5eb2bc5b7a1ef69f528c2a673c11819bb4dc975f9a7f82dff7fe2c560d",
}
PG18_SYNTAX_PATTERNS = (
    ("pg18-alter_table-REL_18_0.sgml", "ALTER TABLE", r"(?m)^\s*(ATTACH PARTITION)\s"),
    ("pg18-alter_table-REL_18_0.sgml", "ALTER TABLE", r"(?m)^\s*(DETACH PARTITION)\s"),
    ("pg18-alter_table-REL_18_0.sgml", "ALTER TABLE", r"(?m)^\s*(ENABLE ROW LEVEL SECURITY)\s*$"),
    ("pg18-create_table-REL_18_0.sgml", "CREATE TABLE", r"(?m)^\s*\[\s*(INHERITS)\s*\("),
    ("pg18-create_table-REL_18_0.sgml", "CREATE TABLE", r"(?m)^\s*\[\s*(PARTITION BY)\s"),
    ("pg18-create_table-REL_18_0.sgml", "CREATE TABLE", r"(?m)^\s*(PARTITION OF)\s"),
    ("pg18-select-REL_18_0.sgml", "", r"(?m)^\s*(TABLE)\s+\[\s*ONLY\s*\]"),
)
PARSER_COMMAND_REPORT_FORMAT_VERSION = 2
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
DEFAULT_RUNTIME_COMMAND = [
    "cargo", "test", "--quiet", "--locked", "-p", "crabka-gres-conformance",
    "--test", "compatibility_behavior",
]

VALID_DISPOSITION = re.compile(
    r"^(Implemented|Wave-assigned\([^)]+\)|Mapped\([^)]+\)|"
    r"Error-with-notice\([0-9A-Z]{5}\)|Non-goal\([^)]+\))$"
)
RESOLVED_PARSER_PREFIXES = ("Mapped(", "Error-with-notice(", "Non-goal(")

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def parser_behavior_report() -> tuple[set[str], list[dict[str, object]], list[dict[str, object]]]:
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
    probes = report.get("probes")
    if not isinstance(probes, list) or not all(isinstance(probe, dict) for probe in probes):
        raise ValueError("parser command helper report probes must be a JSON object array")
    features = report.get("features")
    if not isinstance(features, list) or not all(isinstance(probe, dict) for probe in features):
        raise ValueError("parser command helper report features must be a JSON object array")
    return set(commands), probes, features


def validate_runtime_behavior() -> None:
    result = subprocess.run(
        DEFAULT_RUNTIME_COMMAND,
        cwd=ROOT,
        check=False,
        capture_output=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise ValueError(
            "session behavior manifest failed"
            + (f": {detail}" if detail else f" with exit code {result.returncode}")
        )


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


def matrix_feature_rows(matrix_path: Path) -> dict[str, str]:
    rows: dict[str, str] = {}
    in_features = False
    for line_number, line in enumerate(matrix_path.read_text(encoding="utf-8").splitlines(), 1):
        if line == "## Major language-feature rows":
            in_features = True
            continue
        if not in_features or not line.startswith("|") or line.startswith("|---"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 3 or cells[0] == "Item":
            continue
        if cells[0] in rows:
            raise ValueError(f"{matrix_path}:{line_number}: duplicate feature row: {cells[0]}")
        rows[cells[0]] = cells[1]
    if not rows:
        raise ValueError(f"{matrix_path}: no major language-feature rows found")
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
    if not isinstance(source, str) or "REL_18_0" not in source:
        raise ValueError(f"{path}: inventory source must identify immutable PostgreSQL REL_18_0")
    snapshot_name = document.get("source_snapshot")
    if not isinstance(snapshot_name, str) or Path(snapshot_name).name != snapshot_name:
        raise ValueError(f"{path}: source_snapshot must be a sibling filename")
    snapshot_path = path.parent / snapshot_name
    snapshot = snapshot_path.read_bytes()
    digest = hashlib.sha256(snapshot).hexdigest()
    if document.get("source_sha256") != PG18_SOURCE_SHA256 or digest != PG18_SOURCE_SHA256:
        raise ValueError(f"{path}: PostgreSQL REL_18_0 source snapshot SHA-256 mismatch")
    syntax_metadata = document.get("syntax_snapshots")
    if not isinstance(syntax_metadata, list):
        raise ValueError(f"{path}: syntax_snapshots must be an array")
    syntax_sources: dict[str, str] = {}
    metadata_hashes: dict[str, str] = {}
    for entry in syntax_metadata:
        if not isinstance(entry, dict) or not isinstance(entry.get("file"), str):
            raise ValueError(f"{path}: invalid syntax snapshot metadata")
        filename = entry["file"]
        if Path(filename).name != filename or not isinstance(entry.get("sha256"), str):
            raise ValueError(f"{path}: invalid syntax snapshot filename/hash")
        metadata_hashes[filename] = entry["sha256"]
        content = (path.parent / filename).read_bytes()
        if hashlib.sha256(content).hexdigest() != PG18_SYNTAX_SNAPSHOT_HASHES.get(filename):
            raise ValueError(f"{path}: syntax snapshot SHA-256 mismatch for {filename}")
        syntax_sources[filename] = content.decode("utf-8")
    if metadata_hashes != PG18_SYNTAX_SNAPSHOT_HASHES:
        raise ValueError(f"{path}: syntax snapshot metadata does not match pinned REL_18_0 artifacts")
    commands = document.get("commands")
    if not isinstance(commands, list) or not all(isinstance(command, str) for command in commands):
        raise ValueError(f"{path}: commands must be a JSON string array")
    if len(commands) != 190:
        raise ValueError(f"{path}: expected exactly 190 commands, found {len(commands)}")
    if len(set(commands)) != len(commands):
        raise ValueError(f"{path}: duplicate command names are forbidden")
    if commands != sorted(commands):
        raise ValueError(f"{path}: commands must be sorted")
    derived = derive_pg18_commands(snapshot.decode("utf-8"), syntax_sources)
    if set(commands) != derived:
        missing = sorted(derived - set(commands))
        extra = sorted(set(commands) - derived)
        raise ValueError(
            f"{path}: inventory does not match derived REL_18_0 titles; missing={missing}, extra={extra}"
        )
    return set(commands)


def derive_pg18_commands(
    snapshot: str,
    syntax_sources: dict[str, str],
    *,
    aliases: dict[str, str] = PG18_SOURCE_ALIASES,
    syntax_patterns: tuple[tuple[str, str, str], ...] = PG18_SYNTAX_PATTERNS,
) -> set[str]:
    try:
        sql_section = snapshot.split("<!-- SQL commands -->", 1)[1].split(
            "<!-- applications and utilities -->", 1
        )[0]
    except IndexError as error:
        raise ValueError("PostgreSQL source snapshot lacks SQL command boundaries") from error
    filenames = re.findall(r'^<!ENTITY\s+\S+\s+SYSTEM\s+"([^"]+)\.sgml">$', sql_section, re.MULTILINE)
    if len(filenames) != 183:
        raise ValueError(f"PostgreSQL REL_18_0 snapshot expected 183 SQL entities, found {len(filenames)}")
    commands = {
        aliases.get(filename.replace("_", " ").upper(), filename.replace("_", " ").upper())
        for filename in filenames
    }
    for source_name, base_command, pattern in syntax_patterns:
        source = syntax_sources.get(source_name)
        if source is None:
            raise ValueError(f"missing PostgreSQL syntax source artifact: {source_name}")
        match = re.search(pattern, source)
        if match is None:
            raise ValueError(f"PostgreSQL syntax source {source_name} lacks required synopsis {pattern}")
        commands.add(" ".join(part for part in (base_command, match.group(1)) if part))
    if len(commands) != 190:
        raise ValueError(f"derived PostgreSQL 18 command inventory has {len(commands)} titles, expected 190")
    return commands


def validate_inventory_rows(rows: dict[str, str], inventory: set[str]) -> list[str]:
    errors: list[str] = []
    missing = sorted(inventory - rows.keys())
    if missing:
        errors.append("matrix is missing authoritative command row(s): " + ", ".join(missing))
    extra = sorted(rows.keys() - inventory)
    if extra:
        errors.append("matrix command row(s) not in authoritative PG18 inventory: " + ", ".join(extra))
    return errors


def validate_behavior_probes(
    command_rows: dict[str, str], probes: list[dict[str, object]]
) -> list[str]:
    errors: list[str] = []
    probe_by_command: dict[str, dict[str, object]] = {}
    for probe in probes:
        command = probe.get("command")
        if not isinstance(command, str) or not command:
            errors.append("behavior probe has missing/invalid command")
            continue
        if command in probe_by_command:
            errors.append(f"duplicate behavior probe: {command}")
            continue
        probe_by_command[command] = probe
        for field in ("sql", "parser_shape", "behavior"):
            if not isinstance(probe.get(field), str) or not probe[field]:
                errors.append(f"behavior probe {command} has missing/invalid {field}")

    resolved = {
        command
        for command, disposition in command_rows.items()
        if is_resolved_parser_disposition(disposition)
    }
    missing = sorted(resolved - probe_by_command.keys())
    if missing:
        errors.append("resolved row(s) lack behavior probe: " + ", ".join(missing))
    extra = sorted(probe_by_command.keys() - command_rows.keys())
    if extra:
        errors.append("probe(s) lack matrix row: " + ", ".join(extra))

    for command in sorted(resolved & probe_by_command.keys()):
        disposition = command_rows[command]
        probe = probe_by_command[command]
        behavior = probe.get("behavior")
        expects_refusal = disposition.startswith(("Error-with-notice(", "Non-goal("))
        expected_behavior = "refuse" if expects_refusal else "session-execute"
        if behavior != expected_behavior:
            errors.append(
                f"disposition/behavior mismatch for {command}: {disposition} vs {behavior!r}"
            )
            continue
        if expects_refusal:
            sqlstate = probe.get("sqlstate")
            message = probe.get("message_fragment")
            if disposition.startswith("Error-with-notice("):
                expected_sqlstate = disposition.removeprefix("Error-with-notice(").removesuffix(")")
            else:
                expected_sqlstate = "0A000"
            if sqlstate != expected_sqlstate:
                errors.append(
                    f"refusal SQLSTATE mismatch for {command}: expected {expected_sqlstate}, got {sqlstate!r}"
                )
            if not isinstance(message, str) or not message:
                errors.append(f"refusal probe {command} lacks stable message_fragment")

    accepted_wave = sorted(
        command
        for command in probe_by_command.keys() & command_rows.keys()
        if command_rows[command].startswith("Wave-assigned(")
        and probe_by_command[command].get("behavior") != "refuse"
    )
    if accepted_wave:
        errors.append(
            "parser-accepted wave-assigned command(s) lack intentional refusal: "
            + ", ".join(accepted_wave)
        )
    return errors


def validate_feature_probes(
    feature_rows: dict[str, str], probes: list[dict[str, object]]
) -> list[str]:
    errors: list[str] = []
    by_item: dict[str, dict[str, object]] = {}
    for probe in probes:
        item = probe.get("item")
        if not isinstance(item, str) or not item:
            errors.append("major feature probe has missing/invalid item")
            continue
        if item in by_item:
            errors.append(f"duplicate major feature probe: {item}")
            continue
        by_item[item] = probe
        if not isinstance(probe.get("sql"), str) or not probe["sql"]:
            errors.append(f"major feature probe {item} lacks representative SQL")

    missing = sorted(feature_rows.keys() - by_item.keys())
    if missing:
        errors.append("major feature row(s) lack probe: " + ", ".join(missing))
    orphan = sorted(by_item.keys() - feature_rows.keys())
    if orphan:
        errors.append("major feature probe(s) lack row: " + ", ".join(orphan))

    for item in sorted(feature_rows.keys() & by_item.keys()):
        disposition = feature_rows[item]
        probe = by_item[item]
        behavior = probe.get("behavior")
        if disposition == "Implemented" or disposition.startswith("Mapped("):
            if behavior not in ("session-execute", "extended-execute"):
                errors.append(f"major feature disposition/behavior mismatch for {item}")
        elif disposition.startswith("Error-with-notice("):
            expected = disposition.removeprefix("Error-with-notice(").removesuffix(")")
            if behavior != "session-refuse" or probe.get("sqlstate") != expected:
                errors.append(f"major feature disposition/behavior mismatch for {item}")
        elif disposition.startswith("Wave-assigned("):
            if behavior not in (
                "parser-reject-pending", "session-refuse", "session-execute", "extended-execute"
            ):
                errors.append(f"major feature wave probe has invalid behavior for {item}")
        else:
            errors.append(f"major feature unsupported disposition for {item}: {disposition}")
        if behavior == "session-refuse" and (
            not isinstance(probe.get("sqlstate"), str)
            or not isinstance(probe.get("message_fragment"), str)
        ):
            errors.append(f"major feature refusal lacks exact contract for {item}")
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
    with tempfile.TemporaryDirectory() as directory:
        directory_path = Path(directory)
        snapshot_name = "pg18-allfiles-REL_18_0.sgml"
        (directory_path / snapshot_name).write_bytes(
            (DEFAULT_INVENTORY.parent / snapshot_name).read_bytes()
        )
        syntax_metadata = [
            {"file": filename, "sha256": digest}
            for filename, digest in PG18_SYNTAX_SNAPSHOT_HASHES.items()
        ]
        for filename in PG18_SYNTAX_SNAPSHOT_HASHES:
            (directory_path / filename).write_bytes(
                (DEFAULT_INVENTORY.parent / filename).read_bytes()
            )
        base = sorted(inventory)
        mutations = {
            "missing": base[:-1],
            "extra": sorted(base + ["FAKE EXTRA COMMAND"]),
            "rename": sorted(["ABORT RENAMED" if command == "ABORT" else command for command in base]),
            "fake": sorted(["FAKE COMMAND" if command == "ABORT" else command for command in base]),
            "duplicate": base[:-1] + [base[-2]],
            "count": base[:188],
        }
        for label, mutated_commands in mutations.items():
            mutation_path = directory_path / f"inventory-{label}.json"
            mutation_path.write_text(
                json.dumps({
                    "format_version": 1,
                    "postgresql_major": 18,
                    "source": "https://github.com/postgres/postgres/blob/REL_18_0/doc/src/sgml/ref/allfiles.sgml",
                    "source_snapshot": snapshot_name,
                    "source_sha256": PG18_SOURCE_SHA256,
                    "syntax_snapshots": syntax_metadata,
                    "commands": mutated_commands,
                }),
                encoding="utf-8",
            )
            try:
                load_inventory(mutation_path)
            except ValueError:
                pass
            else:
                raise AssertionError(f"self-test expected {label}-inventory direction to fail")

    allfiles_source = (DEFAULT_INVENTORY.parent / "pg18-allfiles-REL_18_0.sgml").read_text(
        encoding="utf-8"
    )
    syntax_sources = {
        filename: (DEFAULT_INVENTORY.parent / filename).read_text(encoding="utf-8")
        for filename in PG18_SYNTAX_SNAPSHOT_HASHES
    }
    extraction_mutations = {
        "missing expansion source": lambda: derive_pg18_commands(
            allfiles_source,
            {name: source for name, source in syntax_sources.items() if not name.startswith("pg18-select")},
        ),
        "missing expansion mapping": lambda: derive_pg18_commands(
            allfiles_source, syntax_sources, syntax_patterns=PG18_SYNTAX_PATTERNS[:-1]
        ),
        "fake expansion mapping": lambda: derive_pg18_commands(
            allfiles_source,
            syntax_sources,
            aliases={**PG18_SOURCE_ALIASES, "ALTER OPCLASS": "FAKE OPERATOR CLASS"},
        ),
        "extra expansion mapping": lambda: derive_pg18_commands(
            allfiles_source,
            syntax_sources,
            syntax_patterns=PG18_SYNTAX_PATTERNS
            + (("pg18-select-REL_18_0.sgml", "FAKE", r"(?m)^\s*(TABLE)\s+\[\s*ONLY\s*\]"),),
        ),
    }
    for label, mutation in extraction_mutations.items():
        try:
            mutated = mutation()
        except ValueError:
            continue
        if mutated == inventory:
            raise AssertionError(f"self-test expected {label} direction to fail")

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

    execute_probe = {
        "command": "BEGIN", "sql": "BEGIN", "parser_shape": "Begin",
        "behavior": "session-execute",
    }
    refusal_probe = {
        "command": "CREATE DATABASE", "sql": "CREATE DATABASE other",
        "parser_shape": "CompatibilityRefusal", "behavior": "refuse",
        "sqlstate": "0A000", "message_fragment": "database lifecycle",
    }
    behavior_rows = {"BEGIN": "Implemented", "CREATE DATABASE": "Error-with-notice(0A000)"}
    if validate_behavior_probes(behavior_rows, [execute_probe, refusal_probe]):
        raise AssertionError("self-test expected a matching bidirectional manifest to pass")
    directions = [
        ([execute_probe], "resolved row(s) lack behavior probe"),
        ([execute_probe, refusal_probe, {**execute_probe, "command": "EXTRA"}], "probe(s) lack matrix row"),
        ([{**execute_probe, "behavior": "refuse", "sqlstate": "0A000", "message_fragment": "x"}, refusal_probe], "disposition/behavior mismatch"),
    ]
    for probes, fragment in directions:
        if not any(fragment in error for error in validate_behavior_probes(behavior_rows, probes)):
            raise AssertionError(f"self-test expected failure direction: {fragment}")
    wave_errors = validate_behavior_probes(
        {"BEGIN": "Wave-assigned(F-test)"}, [execute_probe]
    )
    if not any("lack intentional refusal" in error for error in wave_errors):
        raise AssertionError("self-test expected parser-accepted wave-assigned direction to fail")

    feature_rows = {"Feature A": "Implemented", "Feature B": "Wave-assigned(T)"}
    feature_probes = [
        {"item": "Feature A", "sql": "SELECT 1", "behavior": "session-execute", "setup": []},
        {"item": "Feature B", "sql": "SELECT bad", "behavior": "parser-reject-pending", "setup": []},
    ]
    if validate_feature_probes(feature_rows, feature_probes):
        raise AssertionError("self-test expected matching major feature manifest to pass")
    feature_directions = [
        (feature_probes[:1], "major feature row(s) lack probe"),
        (feature_probes + [{"item": "Orphan", "sql": "SELECT 1", "behavior": "session-execute"}], "major feature probe(s) lack row"),
        ([{**feature_probes[0], "behavior": "parser-reject-pending"}, feature_probes[1]], "major feature disposition/behavior mismatch"),
    ]
    for probes, fragment in feature_directions:
        if not any(fragment in error for error in validate_feature_probes(feature_rows, probes)):
            raise AssertionError(f"self-test expected feature failure direction: {fragment}")

    injected_identity_matrix = """| Item | Disposition | Notes |\n|---|---|---|\n| BEGIN | Implemented | ok |\n"""
    injected_errors = self_test_errors(
        injected_identity_matrix, {"BEGIN", "FAKE BEGIN ALIAS"}
    )
    if not any("FAKE BEGIN ALIAS" in error for error in injected_errors):
        raise AssertionError(
            "self-test expected an accepted existing-shape alias absent from the manifest to fail"
        )

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
        parser_commands, behavior_probes, feature_probes = parser_behavior_report()
        if args.self_test:
            run_self_test(parser_commands)
        inventory = load_inventory(DEFAULT_INVENTORY)
        errors = validate_inventory_rows(matrix_command_rows(args.matrix), inventory)
        errors.extend(validate(args.matrix, parser_commands))
        errors.extend(validate_behavior_probes(matrix_command_rows(args.matrix), behavior_probes))
        errors.extend(validate_feature_probes(matrix_feature_rows(args.matrix), feature_probes))
        validate_runtime_behavior()
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
