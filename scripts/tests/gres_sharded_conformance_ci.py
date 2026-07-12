#!/usr/bin/env python3
"""Static anti-rot checks for the live G-8 corpus-through-sharding gate."""

from pathlib import Path


workflow = Path(".github/workflows/ci.yml").read_text()
script = Path("scripts/gres-sharded-conformance.sh").read_text()


def validate(workflow_text: str = workflow, script_text: str = script) -> None:
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
        "timestamp_primary_committed",
        'primary_range=0',
        'primary_range=1',
        '"mode": "live"',
        '"range_count": 2',
    ]
    for token in required_script:
        assert token in script_text, f"missing live sharded conformance script token: {token}"


validate()

mutations = [
    workflow.replace("./scripts/gres-sharded-conformance.sh", "./scripts/not-live.sh", 1),
    workflow.replace("CRABKA_GRES_SHARDED_CONFORMANCE_MODE: live", "CRABKA_GRES_SHARDED_CONFORMANCE_MODE: static", 1),
    workflow.replace("  gres-sharded-conformance:\n", "  gres-sharded-conformance:\n    continue-on-error: true\n", 1),
    script.replace("--subject-sharded-ddl", "--plain-subject-ddl", 1),
    script.replace("--baseline crates/gres-conformance/baseline.json", "--baseline /tmp/fake.json", 1),
    script.replace('primary_range=1', 'configured_range=1', 1),
]
for index, mutated in enumerate(mutations):
    try:
        if index < 3:
            validate(workflow_text=mutated)
        else:
            validate(script_text=mutated)
    except (AssertionError, ValueError):
        pass
    else:
        raise AssertionError(f"negative live-gate mutation {index} unexpectedly passed")

print("PASS: live G-8 corpus-through-sharding CI contract and negative mutations")
