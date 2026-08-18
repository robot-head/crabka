# pg_regress gap triage, 2026-08-18

This directory holds the evidence behind
[`docs/superpowers/plans/2026-08-18-gres-pg-regress-close-the-rest.md`](../../plans/2026-08-18-gres-pg-regress-close-the-rest.md).
Every brief in that programme is written from these ledgers, not from the plan summary.

## Source

- CI run 32047096161 on `main` at commit `14c87bfe0`, artifact `gres-pg-regress`
  id 9295866181: 56 / 231 files exact, 175 failing, 110,197 canonical changed lines,
  4,903 hunks. `regression.diffs.xz` is that run's `gres-serial/regression.diffs`.
- The oracle outputs (`self-check-serial/results/*.out`) and the Gres outputs are in the
  artifact and are not copied here.

## Files

| file | content |
|---|---|
| `roots.json` | 16 cluster ledgers, 799 roots: id, what PostgreSQL does, what Gres does, evidence, attributable lines (whole-block rule), files affected, fix locations (path + symbol), size, dependencies, oracle facts. Per-file first failing statement and planner-only estimate. |
| `verdicts.json` | 37 adversarial verifications of the largest roots: confirmed/refuted, corrected fix locations, recounts, hidden prerequisites. |
| `clusters/*.md` | Each analyst's markdown summary, with "Brief corrections". |
| `verify/*.md` | Each verifier's notes. |
| `explain-census.txt`, `explain-census.json` | EXPLAIN census over the failing files: node types, annotations, options, GUCs, statistics dependence, per-file counts. |
| `executor-architecture.md`, `.json` | How a SELECT runs today, operator inventory, index read path, memory policy, planner insertion point, phased proposal. |
| `synthesis.md`, `synthesis.json` | Ledger summary, workstreams with file sets, batches, planner programme, open questions. |
| `file-stats.json`, `classify.py` | Per-file changed lines and the rough line classifier used to size the EXPLAIN bucket. |

## Counting rule

A changed line is any in-hunk line that starts with `+` or `-`, except the two file
headers `^(\+\+\+|---) /`. Attribute the whole change block to one root: a one-line Gres
error that replaced a 30-row result costs 31 lines. Numbers are ±30 %.

## Reproduce the split diffs

```bash
xz -dk regression.diffs.xz
python3 - <<'EOF'
import re
cur=None; out=None
for line in open('regression.diffs'):
    m=re.match(r'^diff -U3 .*expected/([\w.]+)\.out ', line)
    if m:
        if out: out.close()
        cur=m.group(1); out=open(f'diffs-{cur}.diff','w')
    if out: out.write(line)
if out: out.close()
EOF
```
