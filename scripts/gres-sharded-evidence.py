#!/usr/bin/env python3
"""Validate and summarize user-table primary ownership from a live Gres log."""

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path


COMMIT = re.compile(
    r"timestamp_primary_committed primary_range=(\d+).*table_ids=\{([^}]*)\}"
)


def summarize_lines(lines: list[str], expected_ranges: set[int] = {0, 1}) -> dict:
    counts: Counter[tuple[int, int]] = Counter()
    ranges_by_table: dict[int, set[int]] = defaultdict(set)
    for line in lines:
        match = COMMIT.search(line)
        if not match:
            continue
        primary_range = int(match.group(1))
        table_ids = {
            int(value.strip())
            for value in match.group(2).split(",")
            if value.strip()
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
