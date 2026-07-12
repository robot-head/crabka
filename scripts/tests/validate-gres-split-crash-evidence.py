#!/usr/bin/env python3
"""Strict validator for the exhaustive Gres Split crash evidence matrix."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import Any

EXPECTED = {
    "source_restore": [
        "initiated_before_running_cas",
        "checkpoint_receipt_before_journal_cas",
        "checkpointed_after_journal_cas",
        "pause_receipt_before_journal_cas",
        "paused_before_stage",
        "stage_receipt_before_journal_cas",
        "staged_after_journal_cas",
        "marker_claim_receipt_before_journal_cas",
        "restored_after_journal_cas",
        "prologue_receipt_before_journal_cas",
        "activated_after_journal_cas",
    ],
    "publication": [
        "tenant_cas_before_journal_cas",
        "layout_published_after_journal_cas",
    ],
    "retirement_resume": [
        "retiring_before_delete",
        "delete_success_before_sidecar_cas",
        "parked_after_sidecar_cas",
        "retire_receipt_before_journal_cas",
        "resuming_after_journal_cas",
        "completed_after_journal_cas",
    ],
}

INT_FIELDS = {
    "schema_version",
    "acknowledged_rows",
    "recovered_acknowledgements",
    "max_ack_gap_ms",
    "max_ack_gap_bound_ms",
    "operation_elapsed_ms",
    "operation_bound_ms",
    "marker_count",
    "left_marker_count",
    "right_marker_count",
    "delete_count",
    "old_pid",
    "new_pid",
    "kill_ms",
    "restart_ms",
    "publication_ms",
    "left_wal_generation",
    "right_wal_generation",
}
STRING_FIELDS = {
    "family",
    "case",
    "tenant_id",
    "operation_id",
    "sentinel_topic",
    "left_endpoint",
    "right_endpoint",
}
BOOL_FIELDS = {
    "post_publication_r2_ack",
    "post_publication_r3_ack",
    "predecessor_topic_absent",
    "sentinel_topic_present",
    "workload_process_reaped",
}
LIST_FIELDS = {"topology_topics"}
EXACT_KEYS = INT_FIELDS | STRING_FIELDS | BOOL_FIELDS | LIST_FIELDS
FAMILY_GAP_BOUND = {
    "source_restore": 25_000,
    "publication": 12_000,
    "retirement_resume": 12_000,
}


class ValidationError(ValueError):
    pass


def fail(message: str) -> None:
    raise ValidationError(message)


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"{path}: unreadable JSON: {error}")
    if not isinstance(value, dict):
        fail(f"{path}: root must be an object")
    return value


def validate_record(record: dict[str, Any], family: str, case: str, source: str) -> None:
    if set(record) != EXACT_KEYS:
        fail(f"{source}: schema keys differ: missing={sorted(EXACT_KEYS - set(record))} extra={sorted(set(record) - EXACT_KEYS)}")
    for key in INT_FIELDS:
        if isinstance(record[key], bool) or not isinstance(record[key], int):
            fail(f"{source}: {key} must be an integer")
    for key in STRING_FIELDS:
        if not isinstance(record[key], str) or not record[key]:
            fail(f"{source}: {key} must be a nonempty string")
    for key in BOOL_FIELDS:
        if type(record[key]) is not bool:
            fail(f"{source}: {key} must be a boolean")
    if not isinstance(record["topology_topics"], list) or not all(
        isinstance(topic, str) and topic for topic in record["topology_topics"]
    ):
        fail(f"{source}: topology_topics must be a list of nonempty strings")
    if record["schema_version"] != 1:
        fail(f"{source}: unsupported schema version")
    if record["family"] != family or record["case"] != case:
        fail(f"{source}: family/case mismatch")
    if case not in EXPECTED[family]:
        fail(f"{source}: unexpected case")
    if record["acknowledged_rows"] <= 0 or record["recovered_acknowledgements"] < 1:
        fail(f"{source}: payload acknowledgement counts are invalid")
    bound = FAMILY_GAP_BOUND[family]
    if record["max_ack_gap_bound_ms"] != bound or not 0 < record["max_ack_gap_ms"] <= bound:
        fail(f"{source}: ACK gap/bound is invalid")
    if record["operation_bound_ms"] != 240_000 or not 0 < record["operation_elapsed_ms"] < 240_000:
        fail(f"{source}: operation duration/bound is invalid")
    if record["marker_count"] != record["left_marker_count"] + record["right_marker_count"]:
        fail(f"{source}: marker partition arithmetic differs from union")
    if (record["marker_count"], record["left_marker_count"], record["right_marker_count"]) != (1, 0, 1):
        fail(f"{source}: marker partition must be exactly right-only")
    if record["delete_count"] != 1:
        fail(f"{source}: predecessor deletion count must be one")
    if record["old_pid"] <= 0 or record["new_pid"] <= 0 or record["old_pid"] == record["new_pid"]:
        fail(f"{source}: process identities are invalid")
    if not (0 < record["kill_ms"] <= record["restart_ms"]):
        fail(f"{source}: kill/restart ordering is invalid")
    if record["publication_ms"] <= 0:
        fail(f"{source}: publication timestamp is invalid")
    if not all(record[key] for key in BOOL_FIELDS):
        fail(f"{source}: a terminal invariant is false")
    if record["left_endpoint"] == record["right_endpoint"]:
        fail(f"{source}: successor endpoints must differ")
    if (record["left_wal_generation"], record["right_wal_generation"]) != (1, 1):
        fail(f"{source}: successor generations must both be one")
    sentinel = record["sentinel_topic"]
    if not sentinel.startswith("g8-sentinel-"):
        fail(f"{source}: sentinel topic name is invalid")
    tenant = record["tenant_id"]
    expected_topics = {
        f"__gres_wal.{tenant}.r0",
        f"__gres_wal.{tenant}.r2.g0000000001",
        f"__gres_wal.{tenant}.r3.g0000000001",
        sentinel,
    }
    topics = record["topology_topics"]
    if topics != sorted(expected_topics):
        fail(f"{source}: topology topic set is not exact")


def validate_file(family: str, case: str, path: Path) -> dict[str, Any]:
    require_family(family)
    record = load_json(path)
    validate_record(record, family, case, str(path))
    return record


def validate_family(family: str, directory: Path) -> list[dict[str, Any]]:
    require_family(family)
    if not directory.is_dir():
        fail(f"{directory}: evidence directory is absent")
    actual = {path.name for path in directory.glob("*.json")}
    expected = {f"{case}.json" for case in EXPECTED[family]}
    if actual != expected:
        fail(f"{directory}: evidence file set differs: missing={sorted(expected - actual)} extra={sorted(actual - expected)}")
    records = [validate_file(family, case, directory / f"{case}.json") for case in EXPECTED[family]]
    tenants = {record["tenant_id"] for record in records}
    operations = {record["operation_id"] for record in records}
    if len(tenants) != len(records) or len(operations) != len(records):
        fail(f"{directory}: tenant and operation identities must each be unique")
    return records


def require_family(family: str) -> None:
    if family not in EXPECTED:
        fail(f"unknown family {family!r}")


def synthetic_record(family: str, case: str, index: int) -> dict[str, Any]:
    tenant = f"tg8synthetic-{family}-{index}"
    sentinel = f"g8-sentinel-{family}-{index}"
    return {
        "schema_version": 1,
        "family": family,
        "case": case,
        "tenant_id": tenant,
        "operation_id": f"g8-operation-{family}-{index}",
        "acknowledged_rows": 32,
        "recovered_acknowledgements": 1,
        "max_ack_gap_ms": 1,
        "max_ack_gap_bound_ms": FAMILY_GAP_BOUND[family],
        "operation_elapsed_ms": 2,
        "operation_bound_ms": 240_000,
        "marker_count": 1,
        "left_marker_count": 0,
        "right_marker_count": 1,
        "delete_count": 1,
        "old_pid": index * 2 + 1,
        "new_pid": index * 2 + 2,
        "kill_ms": 1,
        "restart_ms": 2,
        "publication_ms": 3,
        "post_publication_r2_ack": True,
        "post_publication_r3_ack": True,
        "predecessor_topic_absent": True,
        "sentinel_topic": sentinel,
        "sentinel_topic_present": True,
        "left_endpoint": f"127.0.0.1:{10_000 + index * 2}",
        "right_endpoint": f"127.0.0.1:{10_001 + index * 2}",
        "left_wal_generation": 1,
        "right_wal_generation": 1,
        "topology_topics": [
            f"__gres_wal.{tenant}.r0",
            f"__gres_wal.{tenant}.r2.g0000000001",
            f"__gres_wal.{tenant}.r3.g0000000001",
            sentinel,
        ],
        "workload_process_reaped": True,
    }


def expect_failure(action: Any, label: str) -> None:
    try:
        action()
    except ValidationError:
        return
    fail(f"self-test negative unexpectedly passed: {label}")


def run_self_tests() -> None:
    with tempfile.TemporaryDirectory(prefix="g8-validator-") as raw:
        root = Path(raw)
        for family, cases in EXPECTED.items():
            directory = root / family
            directory.mkdir()
            for index, case in enumerate(cases):
                (directory / f"{case}.json").write_text(
                    json.dumps(synthetic_record(family, case, index)), encoding="utf-8"
                )
            validate_family(family, directory)

        family = "publication"
        case = EXPECTED[family][0]
        valid = synthetic_record(family, case, 50)
        sample = root / "sample.json"
        sample.write_text(json.dumps(valid), encoding="utf-8")
        expect_failure(lambda: validate_file(family, case, root / "absent.json"), "empty input")
        for label, mutate in [
            ("incomplete JSON", lambda value: value.pop("delete_count")),
            ("wrong family", lambda value: value.__setitem__("family", "source_restore")),
            ("wrong case", lambda value: value.__setitem__("case", EXPECTED[family][1])),
            ("false invariant", lambda value: value.__setitem__("workload_process_reaped", False)),
            ("wrong count", lambda value: value.__setitem__("delete_count", 2)),
            ("wrong bound", lambda value: value.__setitem__("max_ack_gap_bound_ms", 25_000)),
            ("malformed marker partition", lambda value: value.__setitem__("left_marker_count", 1)),
        ]:
            changed = dict(valid)
            mutate(changed)
            sample.write_text(json.dumps(changed), encoding="utf-8")
            expect_failure(lambda: validate_file(family, case, sample), label)

        directory = root / family
        first, second = EXPECTED[family]
        duplicate = load_json(directory / f"{first}.json")
        other = load_json(directory / f"{second}.json")
        other["tenant_id"] = duplicate["tenant_id"]
        other["operation_id"] = duplicate["operation_id"]
        directory.joinpath(f"{second}.json").write_text(json.dumps(other), encoding="utf-8")
        expect_failure(lambda: validate_family(family, directory), "duplicate identity")
        directory.joinpath(f"{second}.json").unlink()
        expect_failure(lambda: validate_family(family, directory), "missing expected case")
        directory.joinpath("extra.json").write_text(json.dumps(valid), encoding="utf-8")
        expect_failure(lambda: validate_family(family, directory), "extra case")


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--validate-family", nargs=2, metavar=("FAMILY", "DIRECTORY"))
    mode.add_argument("--validate-file", nargs=3, metavar=("FAMILY", "CASE", "FILE"))
    mode.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    try:
        if args.self_test:
            run_self_tests()
        elif args.validate_family:
            family, directory = args.validate_family
            validate_family(family, Path(directory))
        else:
            family, case, path = args.validate_file
            validate_file(family, case, Path(path))
    except ValidationError as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
