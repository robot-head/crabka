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
        "./scripts/gres-sharded-conformance.sh",
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
        "--baseline crates/gres-conformance/baseline.json",
        "--ranges 0,0:2",
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
        "timestamp_primary_committed primary_range=0 start_ts=1 table_ids={0, 42}",
        "timestamp_primary_committed primary_range=1 start_ts=2 table_ids={42}",
    ])
    assert valid["user_tables_spanning_primaries"] == [42]
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
    {"workflow_text": workflow.replace("./scripts/gres-sharded-conformance.sh", "./scripts/not-live.sh", 1)},
    {"workflow_text": workflow.replace("CRABKA_GRES_SHARDED_CONFORMANCE_MODE: live", "CRABKA_GRES_SHARDED_CONFORMANCE_MODE: static", 1)},
    {"workflow_text": workflow.replace("  gres-sharded-conformance:\n", "  gres-sharded-conformance:\n    continue-on-error: true\n", 1)},
    {"script_text": script.replace("--subject-sharded-ddl", "--plain-subject-ddl", 1)},
    {"script_text": script.replace("--baseline crates/gres-conformance/baseline.json", "--baseline /tmp/fake.json", 1)},
    {"script_text": script.replace("scripts/gres-sharded-evidence.py", "scripts/fake-evidence.py", 1)},
    {"evidence_text": evidence.replace("if table_id == 0:", "if table_id < 0:", 1)},
]
for index, mutation in enumerate(mutations):
    try:
        validate(**mutation)
    except (AssertionError, ValueError):
        pass
    else:
        raise AssertionError(f"negative live-gate mutation {index} unexpectedly passed")

print("PASS: live G-8 corpus-through-sharding CI contract and negative mutations")
