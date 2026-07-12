#!/usr/bin/env python3
"""Validate observed timestamp-primary counts for the uniform scaling workload."""

import json
import pathlib
import sys


def main() -> None:
    if len(sys.argv) != 6:
        raise SystemExit(
            "usage: check-gres-primary-distribution.py ARTIFACT RANGES SESSIONS TXNS WARMUPS"
        )
    artifact = pathlib.Path(sys.argv[1])
    range_count, sessions, txns, warmups = map(int, sys.argv[2:])
    payload = json.loads(artifact.read_text())
    observed = payload.get("primary_range_distribution")
    if not isinstance(observed, dict):
        raise SystemExit("primary_range_distribution is missing")
    expected_ids = {str(range_id) for range_id in range(range_count)}
    if set(observed) != expected_ids:
        raise SystemExit(f"observed primary ranges {set(observed)} != {expected_ids}")
    expected_per_range = sessions * (txns + warmups)
    mismatches = {
        range_id: count
        for range_id, count in observed.items()
        if not isinstance(count, int) or count != expected_per_range
    }
    if mismatches:
        raise SystemExit(
            f"non-uniform primary counts {mismatches}; expected {expected_per_range} per range"
        )
    observed_total = payload.get("observed_primary_transactions")
    expected_total = range_count * expected_per_range
    if observed_total != expected_total:
        raise SystemExit(
            f"observed total {observed_total} != expected {expected_total}"
        )


if __name__ == "__main__":
    main()
