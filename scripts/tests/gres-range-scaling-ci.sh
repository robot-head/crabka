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
        'sharded_range_boundaries()', 'boundaries+=("1:${index}:0")',
        'readonly SHARDED_TABLE_NAME="s1"',
        'SHARDED BY HASH (id) BUCKETS ${range_count}',
        '--hash-placement "1:id:${hash_buckets}"',
        'sharded_id_for_range()', '"$range_count" "$range_index"',
        'primary_range_distribution', 'runtime timestamp_primary_committed observations cover all expected ranges',
        'timestamp_primary_committed', 'observed_primary_transactions',
        'ansi_escape.sub("", raw_line)',
        'check-gres-primary-distribution.py',
    ]
    for needle in required_script:
        assert needle in benchmark, f'missing benchmark/gate contract: {needle}'
    fast_marker = 'elif [ "${MODE_REQUEST}" = "fast" ] || [ "${CRABKA_GRES_RANGE_SCALING_FAST:-0}" = "1" ]; then'
    fast_start = benchmark.index(fast_marker)
    fast_end = benchmark.index('\nelse\n', fast_start)
    fast_block = benchmark[fast_start:fast_end]
    assert 'SESSIONS_PER_RANGE="${CRABKA_GRES_RANGE_SCALING_SESSIONS_PER_RANGE:-1}"' in fast_block, \
        'fast mode must use one persistent session per range so the live curve measures range scaling'
    sharded_result = 'result-sharded-${range_count}-trial-${trial}.json'
    sharded_start = benchmark.index(sharded_result)
    sharded_end = benchmark.index('\nPY\n', sharded_start)
    sharded_parser = benchmark[sharded_start:sharded_end]
    assert '\nimport re\n' in sharded_parser, 'sharded result parser must import re'

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

sharded_start = script.index('result-sharded-${range_count}-trial-${trial}.json')
missing_re = script[:sharded_start] + script[sharded_start:].replace('import re\n', '', 1)
fast_marker = 'elif [ "${MODE_REQUEST}" = "fast" ] || [ "${CRABKA_GRES_RANGE_SCALING_FAST:-0}" = "1" ]; then'
fast_start = script.index(fast_marker)
fast_end = script.index('\nelse\n', fast_start)
fast_block = script[fast_start:fast_end]
two_fast_sessions = (
    script[:fast_start]
    + fast_block.replace('SESSIONS_PER_RANGE="${CRABKA_GRES_RANGE_SCALING_SESSIONS_PER_RANGE:-1}"',
                         'SESSIONS_PER_RANGE="${CRABKA_GRES_RANGE_SCALING_SESSIONS_PER_RANGE:-2}"')
    + script[fast_end:]
)
benchmark_mutations = [
    ('upper envelope', script.replace('expected_min_tps <= measured_tps <= expected_max_tps', 'expected_min_tps <= measured_tps')),
    ('hash bucket boundary', script.replace('boundaries+=("1:${index}:0")', 'boundaries+=("1:$((index * 1000000))")')),
    ('logical table ID', script.replace('readonly SHARDED_TABLE_NAME="s1"', 'readonly SHARDED_TABLE_NAME="s0"')),
    ('hash-sharded DDL', script.replace('SHARDED BY HASH (id) BUCKETS ${range_count}', 'SHARDED')),
    ('registry hash placement', script.replace('--hash-placement "1:id:${hash_buckets}"', '')),
    ('bucket-targeted worker', script.replace('"$range_count" "$range_index"', '"$range_count"')),
    ('fabricated primary distribution', script.replace('timestamp_primary_committed', 'sessions_per_range * txns_per_session')),
    ('ANSI-sensitive primary parser', script.replace('ansi_escape.sub("", raw_line)', 'raw_line')),
    ('missing re import', missing_re),
    ('two fast sessions per range', two_fast_sessions),
]
for label, mutated in benchmark_mutations:
    try:
        validate(workflow, mutated)
    except AssertionError:
        pass
    else:
        raise AssertionError(f'{label} mutation unexpectedly passed')
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
