#!/usr/bin/env python3
"""Validate and summarize user-table primary ownership from a live Gres log."""

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path


ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")
#: `table_ids` reaches the log as the `Debug` rendering of a `BTreeSet<u32>`
#: (`"{1, 2}"`), so pull the ids back out of that string.
TABLE_ID = re.compile(r"\d+")


def commit_record(line: str) -> dict | None:
    """The decoded `timestamp_primary_committed` record on `line`, if any.

    gres emits structured JSON (crabka_logfmt, installed by
    `crabka_telemetry::init`). The previous `primary_range=N` spelling this
    parsed only ever existed in tracing_subscriber's plain-text output.
    """
    if "timestamp_primary_committed" not in line:
        return None
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(record, dict):
        return None
    if record.get("message") != "timestamp_primary_committed":
        return None
    if "primary_range" not in record or "table_ids" not in record:
        raise ValueError(
            "timestamp_primary_committed record omitted primary_range/table_ids"
        )
    return record


def summarize_lines(lines: list[str], expected_ranges: set[int] = {0, 1}) -> dict:
    counts: Counter[tuple[int, int]] = Counter()
    ranges_by_table: dict[int, set[int]] = defaultdict(set)
    for line in lines:
        line = ANSI_ESCAPE.sub("", line)
        record = commit_record(line)
        if record is None:
            continue
        primary_range = int(record["primary_range"])
        table_ids = {
            int(value) for value in TABLE_ID.findall(str(record["table_ids"]))
        }
        for table_id in table_ids:
            if table_id == 0:
                continue
            counts[(table_id, primary_range)] += 1
            ranges_by_table[table_id].add(primary_range)

    observed_ranges = {primary_range for _, primary_range in counts}
    missing = expected_ranges - observed_ranges
    if missing:
        raise ValueError(
            "missing user-table timestamp primary commits for ranges "
            + ", ".join(map(str, sorted(missing)))
        )

    return {
        "user_table_primary_counts": {
            str(table_id): {
                str(primary_range): counts[(table_id, primary_range)]
                for primary_range in sorted(ranges_by_table[table_id])
            }
            for table_id in sorted(ranges_by_table)
        },
        "user_tables_spanning_primaries": [
            table_id
            for table_id, ranges in sorted(ranges_by_table.items())
            if expected_ranges <= ranges
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("log", type=Path)
    parser.add_argument("artifact", type=Path)
    args = parser.parse_args()
    evidence = summarize_lines(args.log.read_text(encoding="utf-8").splitlines())
    args.artifact.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
