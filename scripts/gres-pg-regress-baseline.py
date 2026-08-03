#!/usr/bin/env python3
"""Build and enforce the monotone Gres pg_regress failure baseline."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import sys
import tempfile
from typing import Any


SCHEMA_VERSION = 1
TAP_RESULT = re.compile(
    r"^(not )?ok\s+(\d+)\s+(?:[+-]\s+)?([A-Za-z0-9_][A-Za-z0-9_.-]*)(?:\s+.*)?$"
)
SHA256 = re.compile(r"^[0-9a-f]{64}$")


class BaselineError(ValueError):
    """An invalid pg_regress result or baseline."""


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise BaselineError(f"cannot read {path}: {error}") from error


def parse_schedule(path: Path) -> list[str]:
    tests: list[str] = []
    for line_number, raw_line in enumerate(read_text(path).splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if not line.startswith("test:"):
            raise BaselineError(f"{path}:{line_number}: unsupported schedule line: {line}")
        names = line.removeprefix("test:").split()
        if not names:
            raise BaselineError(f"{path}:{line_number}: empty test group")
        tests.extend(names)
    if not tests:
        raise BaselineError(f"{path}: schedule contains no tests")
    duplicates = sorted(name for name in set(tests) if tests.count(name) > 1)
    if duplicates:
        raise BaselineError(f"{path}: duplicate scheduled tests: {', '.join(duplicates)}")
    return tests


def parse_tap(path: Path, schedule: list[str]) -> dict[str, bool]:
    results: list[tuple[int, str, bool]] = []
    for line_number, raw_line in enumerate(read_text(path).splitlines(), 1):
        line = raw_line.strip()
        match = TAP_RESULT.match(line)
        if match:
            results.append((int(match.group(2)), match.group(3), match.group(1) is None))
        elif line.startswith("ok ") or line.startswith("not ok "):
            raise BaselineError(f"{path}:{line_number}: malformed TAP result: {line}")

    numbers = [number for number, _, _ in results]
    expected_numbers = list(range(1, len(results) + 1))
    if numbers != expected_numbers:
        raise BaselineError(f"{path}: TAP result numbers are not consecutive from 1")
    names = [name for _, name, _ in results]
    if names != schedule:
        missing = [name for name in schedule if name not in set(names)]
        extra = [name for name in names if name not in set(schedule)]
        raise BaselineError(
            f"{path}: TAP membership/order differs from schedule"
            f"; missing={missing}; extra={extra}"
        )
    return {name: passed for _, name, passed in results}


def replacement_pairs(source_root: Path | None, build_root: Path | None) -> list[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for root, token in ((source_root, "<SOURCE_ROOT>"), (build_root, "<BUILD_ROOT>")):
        if root is None:
            continue
        raw = str(root).rstrip("/")
        resolved = str(root.resolve()).rstrip("/")
        if raw:
            pairs.add((raw, token))
        if resolved:
            pairs.add((resolved, token))
    return sorted(pairs, key=lambda pair: len(pair[0]), reverse=True)


def split_diff_sections(path: Path, failed: set[str]) -> dict[str, list[str]]:
    if not path.exists():
        if failed:
            raise BaselineError(f"{path}: diff file is missing for failing tests")
        return {}
    sections: list[list[str]] = []
    current: list[str] | None = None
    for line in read_text(path).splitlines():
        if line.startswith("diff -U3 "):
            if current is not None:
                sections.append(current)
            current = [line]
        elif current is not None:
            current.append(line)
        elif line.strip():
            raise BaselineError(f"{path}: content before first diff section")
    if current is not None:
        sections.append(current)

    by_test: dict[str, list[str]] = {}
    for section in sections:
        try:
            command = shlex.split(section[0])
        except ValueError as error:
            raise BaselineError(f"{path}: malformed diff command: {section[0]}") from error
        if len(command) != 4 or command[:2] != ["diff", "-U3"]:
            raise BaselineError(f"{path}: unsupported diff command: {section[0]}")
        result_basename = Path(command[3]).name
        if not result_basename.endswith(".out"):
            raise BaselineError(f"{path}: result path is not an .out file: {command[3]}")
        test = result_basename.removesuffix(".out")
        if test in by_test:
            raise BaselineError(f"{path}: duplicate diff section for {test}")
        by_test[test] = section

    if set(by_test) != failed:
        missing = sorted(failed - set(by_test))
        extra = sorted(set(by_test) - failed)
        raise BaselineError(f"{path}: diff membership differs from TAP failures; missing={missing}; extra={extra}")
    return by_test


def canonical_failure(
    section: list[str], replacements: list[tuple[str, str]]
) -> dict[str, int | str]:
    command = shlex.split(section[0])
    expected_basename = Path(command[2]).name
    result_basename = Path(command[3]).name
    canonical = [f"diff -U3 expected/{expected_basename} results/{result_basename}"]
    in_hunk = False
    hunks = 0
    changed_lines = 0

    for line in section[1:]:
        if not in_hunk and line.startswith("--- "):
            line = f"--- expected/{expected_basename}"
        elif not in_hunk and line.startswith("+++ "):
            line = f"+++ results/{result_basename}"
        else:
            for root, token in replacements:
                line = line.replace(root, token)
        if line.startswith("@@ "):
            hunks += 1
            in_hunk = True
        elif in_hunk and (line.startswith("+") or line.startswith("-")):
            changed_lines += 1
        canonical.append(line)

    if hunks == 0:
        raise BaselineError(f"diff for {result_basename} contains no unified-diff hunks")
    canonical_diff = "\n".join(canonical) + "\n"
    return {
        "expected": expected_basename,
        "hunks": hunks,
        "changed_lines": changed_lines,
        "sha256": hashlib.sha256(canonical_diff.encode()).hexdigest(),
    }


def generate_actual(args: argparse.Namespace) -> dict[str, Any]:
    schedule = parse_schedule(args.schedule)
    tap = parse_tap(args.tap, schedule)
    failed = {name for name, passed in tap.items() if not passed}
    sections = split_diff_sections(args.diff, failed)
    replacements = replacement_pairs(args.source_root, args.build_root)
    failures = {
        name: canonical_failure(sections[name], replacements)
        for name in schedule
        if name in failed
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "postgres_tag": args.postgres_tag,
        "schedule_sha256": hashlib.sha256(args.schedule.read_bytes()).hexdigest(),
        "scheduled_tests": len(schedule),
        "schedule": schedule,
        "passed": len(schedule) - len(failures),
        "total": len(schedule),
        "failures": failures,
    }


def validate_document(document: Any, label: str) -> dict[str, Any]:
    if not isinstance(document, dict) or document.get("schema_version") != SCHEMA_VERSION:
        raise BaselineError(f"{label}: unsupported or missing schema_version")
    schedule = document.get("schedule")
    failures = document.get("failures")
    if not isinstance(document.get("postgres_tag"), str) or not document["postgres_tag"]:
        raise BaselineError(f"{label}: postgres_tag must be a non-empty string")
    if not isinstance(document.get("schedule_sha256"), str) or not SHA256.fullmatch(
        document["schedule_sha256"]
    ):
        raise BaselineError(f"{label}: invalid schedule SHA-256")
    if (
        not isinstance(schedule, list)
        or not all(isinstance(name, str) and name for name in schedule)
        or len(schedule) != len(set(schedule))
    ):
        raise BaselineError(f"{label}: schedule must be a unique string list")
    if not isinstance(failures, dict) or not set(failures).issubset(schedule):
        raise BaselineError(f"{label}: failures must be keyed by scheduled test")
    for test, failure in failures.items():
        if not isinstance(failure, dict) or set(failure) != {
            "expected",
            "hunks",
            "changed_lines",
            "sha256",
        }:
            raise BaselineError(f"{label}: malformed failure entry for {test}")
        if not isinstance(failure["expected"], str) or not failure["expected"].endswith(".out"):
            raise BaselineError(f"{label}: invalid expected basename for {test}")
        if not isinstance(failure["hunks"], int) or failure["hunks"] < 1:
            raise BaselineError(f"{label}: invalid hunk count for {test}")
        if not isinstance(failure["changed_lines"], int) or failure["changed_lines"] < 1:
            raise BaselineError(f"{label}: invalid changed-line count for {test}")
        if not isinstance(failure["sha256"], str) or not SHA256.fullmatch(failure["sha256"]):
            raise BaselineError(f"{label}: invalid SHA-256 for {test}")
    total = document.get("total")
    passed = document.get("passed")
    if (
        document.get("scheduled_tests") != len(schedule)
        or total != len(schedule)
        or passed != total - len(failures)
    ):
        raise BaselineError(f"{label}: passed/total does not match schedule and failures")
    return document


def read_document(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(read_text(path))
    except json.JSONDecodeError as error:
        raise BaselineError(f"{path}: invalid JSON: {error}") from error
    return validate_document(document, str(path))


def compare(baseline: dict[str, Any], actual: dict[str, Any]) -> dict[str, list[str]]:
    classifications = {
        "new": [],
        "removed": [],
        "worsened": [],
        "same-count-different": [],
        "improved": [],
        "exact": [],
    }
    baseline_failures = baseline["failures"]
    actual_failures = actual["failures"]
    for test in baseline["schedule"]:
        before = baseline_failures.get(test)
        after = actual_failures.get(test)
        if before is None and after is None:
            continue
        if before is None:
            classifications["new"].append(test)
        elif after is None:
            classifications["removed"].append(test)
        elif after == before:
            classifications["exact"].append(test)
        elif after["changed_lines"] > before["changed_lines"]:
            classifications["worsened"].append(test)
        elif after["changed_lines"] < before["changed_lines"]:
            classifications["improved"].append(test)
        else:
            classifications["same-count-different"].append(test)
    return classifications


def write_document(path: Path | None, document: dict[str, Any]) -> None:
    contents = json.dumps(document, indent=2, sort_keys=True) + "\n"
    if path is None or str(path) == "-":
        sys.stdout.write(contents)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(contents)
        os.chmod(temporary, 0o644)
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def print_comparison(classifications: dict[str, list[str]]) -> None:
    for classification, tests in classifications.items():
        if tests:
            detail = f"{len(tests)} tests" if classification == "exact" else ", ".join(tests)
            print(f"{classification}: {detail}", file=sys.stderr)


def write_summary(
    path: Path | None,
    actual: dict[str, Any],
    classifications: dict[str, list[str]] | None,
) -> None:
    if path is None:
        return
    failures = actual["failures"]
    lines = [
        "## Gres upstream pg_regress",
        "",
        f"- PostgreSQL: `{actual['postgres_tag']}`",
        f"- Passed: **{actual['passed']} / {actual['total']}**",
        f"- Remaining failing tests: **{len(failures)}**",
        f"- Remaining changed lines: **{sum(item['changed_lines'] for item in failures.values())}**",
        f"- Remaining diff hunks: **{sum(item['hunks'] for item in failures.values())}**",
    ]
    if classifications is not None:
        lines.extend(
            f"- Baseline {name}: **{len(tests)}**"
            for name, tests in classifications.items()
        )
    lines.extend(
        [
            "",
            f"<details><summary>{len(failures)} failing-test baselines</summary>",
            "",
            "| Test | Changed lines | Hunks |",
            "| --- | ---: | ---: |",
        ]
    )
    lines.extend(
        f"| `{test}` | {failure['changed_lines']} | {failure['hunks']} |"
        for test, failure in failures.items()
    )
    lines.extend(["", "</details>"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def add_result_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--postgres-tag", required=True)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument(
        "--tap",
        required=True,
        type=Path,
        help="pg_regress TAP stream (the runner's retained command.log)",
    )
    parser.add_argument("--diff", required=True, type=Path, help="pg_regress regression.diffs")
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--build-root", type=Path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser("generate", help="emit the canonical actual result")
    add_result_arguments(generate)
    generate.add_argument("--output", type=Path, default=Path("-"))
    for name in ("check", "seed", "update"):
        command = commands.add_parser(name)
        add_result_arguments(command)
        command.add_argument("--baseline", required=True, type=Path)
        command.add_argument("--actual-output", type=Path)
        command.add_argument("--summary-output", type=Path)
    return parser


def run(args: argparse.Namespace) -> int:
    actual = validate_document(generate_actual(args), "actual result")
    if args.command == "generate":
        write_document(args.output, actual)
        return 0
    if args.actual_output is not None:
        write_document(args.actual_output, actual)
    if args.command == "seed":
        if args.baseline.exists():
            raise BaselineError(f"refusing to overwrite existing baseline: {args.baseline}")
        write_document(args.baseline, actual)
        write_summary(args.summary_output, actual, None)
        return 0

    if not args.baseline.exists():
        raise BaselineError(f"baseline does not exist: {args.baseline}")
    baseline = read_document(args.baseline)
    if (
        baseline["postgres_tag"] != actual["postgres_tag"]
        or baseline["schedule_sha256"] != actual["schedule_sha256"]
        or baseline["schedule"] != actual["schedule"]
    ):
        raise BaselineError("scheduled tests differ from the baseline")
    classifications = compare(baseline, actual)
    print_comparison(classifications)
    write_summary(args.summary_output, actual, classifications)
    changed = classifications["improved"] or classifications["removed"]
    unsafe = (
        classifications["new"]
        or classifications["worsened"]
        or classifications["same-count-different"]
    )
    if args.command == "check":
        return 1 if unsafe or changed else 0
    if unsafe:
        raise BaselineError("baseline update would grow or replace the mismatch surface")
    if not changed:
        raise BaselineError("baseline update has no improved or removed failures")
    write_document(args.baseline, actual)
    return 0


def main() -> int:
    try:
        return run(build_parser().parse_args())
    except BaselineError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
