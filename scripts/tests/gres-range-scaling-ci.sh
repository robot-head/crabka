#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

python3 - <<'PY'
from pathlib import Path

workflow = Path('.github/workflows/ci.yml').read_text()
script = Path('scripts/gres-range-scaling.sh').read_text()

def job_block(source):
    marker = '  gres-range-scaling:\n'
    start = source.index(marker)
    lines = source[start:].splitlines()
    kept = [lines[0]]
    for line in lines[1:]:
        if line.startswith('  ') and not line.startswith('    ') and line.strip():
            break
        kept.append(line)
    return '\n'.join(kept)

def validate(source, benchmark=script):
    job = job_block(source)
    required_job = [
        'runs-on: ubuntu-latest', 'timeout-minutes: 30',
        'dtolnay/rust-toolchain@stable', 'Swatinem/rust-cache@v2',
        'postgresql-client', 'bash scripts/tests/gres-range-scaling-ci.sh',
        'CRABKA_GRES_RANGE_SCALING_MODE=fast',
        './scripts/gres-range-scaling.sh', 'artifact["mode"] == "live"',
        'artifact["passed"]["overall"] is True', 'actions/upload-artifact@v7',
    ]
    for needle in required_job:
        assert needle in job, f'missing live scaling job contract: {needle!r}'
    forbidden = ['continue-on-error:', '|| true', 'if: false', 'if: ${{ false }}']
    for needle in forbidden:
        assert needle not in job, f'non-gating scaling job token: {needle}'
    invocation_lines = [line.strip() for line in job.splitlines() if './scripts/gres-range-scaling.sh' in line and not line.lstrip().startswith('#')]
    assert invocation_lines == ['./scripts/gres-range-scaling.sh'], invocation_lines
    dependencies = [
        "'crates/broker/**'", "'crates/client-admin/**'", "'crates/client-core/**'",
        "'crates/client-producer/**'", "'crates/client-consumer/**'", "'crates/protocol/**'",
        "'crates/security/**'", "'crates/gres-control/**'", "'Cargo.toml'", "'Cargo.lock'",
        "'rust-toolchain.toml'", "'scripts/gres-range-scaling.sh'",
        "'scripts/tests/gres-range-scaling-ci.sh'", "'.github/workflows/ci.yml'",
    ]
    gres_filter = source[source.index('            gres:'):source.index('\n  rust:', source.index('            gres:'))]
    for needle in dependencies:
        assert needle in gres_filter, f'missing direct dependency path: {needle}'
    required_script = [
        'cargo build --locked', 'MEASURE_BEGIN', 'warmup_txns_per_session',
        'aggregate["trials"] = samples', 'statistics.median',
        'decision_ceiling_passed = all(point["within_expected_envelope"] for point in decision_points)',
        'expected_min_tps <= measured_tps <= expected_max_tps',
        'sharded_range_boundaries()', 'boundaries+=("0:$((index * 1000000))")',
        'primary_range_distribution', 'runtime timestamp_primary_committed observations cover all expected ranges',
        'timestamp_primary_committed', 'observed_primary_transactions',
        'ansi_escape.sub("", raw_line)',
        'check-gres-primary-distribution.py',
    ]
    for needle in required_script:
        assert needle in benchmark, f'missing benchmark/gate contract: {needle}'

validate(workflow)

mutations = [
    workflow.replace('./scripts/gres-range-scaling.sh', './scripts/not-the-scaling-script.sh', 1),
    workflow.replace('artifact["mode"] == "live"', 'artifact["mode"] == "dry-run"', 1),
    workflow.replace('  gres-range-scaling:\n', '  gres-range-scaling:\n    continue-on-error: ${{ true }}\n', 1),
    workflow.replace('CRABKA_GRES_RANGE_SCALING_MODE=fast', '# fast mode removed', 1),
]
for index, mutated in enumerate(mutations):
    try:
        validate(mutated)
    except AssertionError:
        pass
    else:
        raise AssertionError(f'negative workflow mutation {index} unexpectedly passed')

upper_mutation = script.replace('expected_min_tps <= measured_tps <= expected_max_tps', 'expected_min_tps <= measured_tps')
try:
    validate(workflow, upper_mutation)
except AssertionError:
    pass
else:
    raise AssertionError('upper envelope mutation unexpectedly passed')

boundary_mutation = script.replace('boundaries+=("0:$((index * 1000000))")', 'boundaries+=("$((index * 1000000))")')
try:
    validate(workflow, boundary_mutation)
except AssertionError:
    pass
else:
    raise AssertionError('table-id sharded-boundary mutation unexpectedly passed')

fabricated_distribution = script.replace(
    'timestamp_primary_committed',
    'sessions_per_range * txns_per_session',
)
try:
    validate(workflow, fabricated_distribution)
except AssertionError:
    pass
else:
    raise AssertionError('fabricated primary distribution unexpectedly passed')

ansi_mutation = script.replace('ansi_escape.sub("", raw_line)', 'raw_line')
try:
    validate(workflow, ansi_mutation)
except AssertionError:
    pass
else:
    raise AssertionError('ANSI-sensitive primary distribution parser unexpectedly passed')
PY

skewed_artifact="$(mktemp)"
trap 'rm -f "$skewed_artifact"' EXIT
cat >"$skewed_artifact" <<'JSON'
{"primary_range_distribution":{"0":437,"1":1,"2":1,"3":1},"observed_primary_transactions":440}
JSON
if python3 scripts/check-gres-primary-distribution.py "$skewed_artifact" 4 2 50 5 >/dev/null 2>&1; then
    echo 'FAIL: skewed observed primary artifact unexpectedly passed' >&2
    exit 1
fi

echo 'PASS: live Gres scaling CI contract and negative mutations'
