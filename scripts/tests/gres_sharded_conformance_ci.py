#!/usr/bin/env python3
"""Static anti-rot checks for the live G-8 corpus-through-sharding gate."""

from pathlib import Path


workflow = Path(".github/workflows/ci.yml").read_text()
script = Path("scripts/gres-sharded-conformance.sh").read_text()
evidence = Path("scripts/gres-sharded-evidence.py").read_text()


def validate(
    workflow_text: str = workflow,
    script_text: str = script,
    evidence_text: str = evidence,
) -> None:
    start = workflow_text.index("  gres-sharded-conformance:\n")
    end = workflow_text.index("\n  helm-lint:", start)
    job = workflow_text[start:end]
    required_job = [
        "services:",
        "postgres:",
        "postgres:18",
        "CRABKA_GRES_SHARDED_CONFORMANCE_MODE: live",
        "aspect gres --task:name=gres-sharded-conformance --suite sharded-conformance",
        "if: ${{ !cancelled() }}",
        "target/gres-sharded-conformance-artifacts",
    ]
    for token in required_job:
        assert token in job, f"missing live sharded conformance job token: {token}"
    assert "continue-on-error" not in job, "live sharded conformance must gate CI"

    required_script = [
        "cargo build --locked",
        "crabka-gres-conformance",
        "--subject-sharded-ddl",
        "--baseline crates/gres-conformance/sharded-baseline.json",
        "if ! ./target/debug/crabka-gres-conformance",
        "--ranges 0,0:250",
        "scripts/gres-sharded-evidence.py",
        '"mode": "live"',
        '"range_count": 2',
    ]
    for token in required_script:
        assert token in script_text, f"missing live sharded conformance script token: {token}"

    namespace = {"__name__": "gres_sharded_evidence_test"}
    exec(compile(evidence_text, "gres-sharded-evidence.py", "exec"), namespace)
    summarize = namespace["summarize_lines"]
    valid = summarize([
        "\x1b[2m2026-07-14T00:00:00Z\x1b[0m "
        "timestamp_primary_committed "
        "\x1b[3mprimary_range\x1b[0m\x1b[2m=\x1b[0m0 "
        "\x1b[3mstart_ts\x1b[0m\x1b[2m=\x1b[0m1 "
        "\x1b[3mtable_ids\x1b[0m\x1b[2m=\x1b[0m{0, 42}",
        "timestamp_primary_committed primary_range=1 start_ts=2 table_ids={42}",
    ])
    assert valid == {
        "user_table_primary_counts": {"42": {"0": 1, "1": 1}},
        "user_tables_spanning_primaries": [42],
    }
    try:
        summarize([
            "timestamp_primary_committed primary_range=0 start_ts=1 table_ids={0}",
            "timestamp_primary_committed primary_range=1 start_ts=2 table_ids={42}",
        ])
    except ValueError:
        pass
    else:
        raise AssertionError("catalog-only range-0 evidence unexpectedly passed")


validate()

mutations = [
    (
        "live invocation",
        {
            "workflow_text": workflow.replace(
                "aspect gres --task:name=gres-sharded-conformance --suite sharded-conformance",
                "aspect gres --task:name=gres-sharded-conformance --suite not-live",
                1,
            )
        },
    ),
    (
        "live mode",
        {
            "workflow_text": workflow.replace(
                "CRABKA_GRES_SHARDED_CONFORMANCE_MODE: live",
                "CRABKA_GRES_SHARDED_CONFORMANCE_MODE: static",
                1,
            )
        },
    ),
    (
        "required gate",
        {
            "workflow_text": workflow.replace(
                "  gres-sharded-conformance:\n",
                "  gres-sharded-conformance:\n    continue-on-error: true\n",
                1,
            )
        },
    ),
    (
        "sharded DDL",
        {
            "script_text": script.replace(
                "--subject-sharded-ddl", "--plain-subject-ddl", 1
            )
        },
    ),
    (
        "sharded baseline",
        {
            "script_text": script.replace(
                "--baseline crates/gres-conformance/sharded-baseline.json",
                "--baseline /tmp/fake.json",
                1,
            )
        },
    ),
    (
        "parity failure propagation",
        {
            "script_text": script.replace(
                "if ! ./target/debug/crabka-gres-conformance",
                "./target/debug/crabka-gres-conformance",
                1,
            )
        },
    ),
    (
        "two owner layout",
        {"script_text": script.replace("--ranges 0,0:250", "--ranges 0,0:2")},
    ),
    (
        "evidence parser",
        {
            "script_text": script.replace(
                "scripts/gres-sharded-evidence.py", "scripts/fake-evidence.py", 1
            )
        },
    ),
    (
        "catalog exclusion",
        {
            "evidence_text": evidence.replace(
                "if table_id == 0:", "if table_id < 0:", 1
            )
        },
    ),
    (
        "ANSI normalization",
        {
            "evidence_text": evidence.replace(
                'line = ANSI_ESCAPE.sub("", line)', "line = line", 1
            )
        },
    ),
]
for name, mutation in mutations:
    try:
        validate(**mutation)
    except (AssertionError, ValueError):
        pass
    else:
        raise AssertionError(f"negative live-gate mutation {name!r} unexpectedly passed")

print("PASS: live G-8 corpus-through-sharding CI contract and negative mutations")
