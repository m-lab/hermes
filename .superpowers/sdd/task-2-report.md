# Task 2 Report: Port v2 Correlation Tomography; Drop v1 Backend

## What Was Done

### Step 1: Copied v2 module and path-local SQL
- `cp 2026-06-hermes-pipeline/src/hermes/pipeline/correlation_tomography.py` → work repo (1278 lines, v2 greedy set-cover)
- `cp 2026-06-hermes-pipeline/src/hermes/sql/queries/correlation_tomography_unexplained_hops.sql` → `src/hermes/sql/queries/06_correlation_tomography_unexplained_hops_union.sql`
- `sed` relabeled all three `load_query` filenames to the public-repo naming convention (`06_correlation_tomography_*_union.sql`)

### Step 2: Grep/correctness verification
```
grep -nE '"correlation_tomography_(prepare|all_edges|unexplained_hops)\.sql"' ... || echo "OK: all relabeled"
# → OK: all relabeled

grep -n "correlation_hyperedges_tomography_v2" ...
# → 102:OUTPUT_TABLE = f"{PROJECT_ID}.hermes_union.correlation_hyperedges_tomography_v2"
```
Both checks pass.

### Step 3: Simplified tomography.py
Replaced the two-backend dispatch (python/bigquery) with a single `run_tomography` that only accepts `backend="python"`, raises `ValueError` for anything else, and imports/calls `correlation_tomography.run_correlation_tomography` directly. Removed `_run_python`/`_run_bigquery` helpers.
```
grep -n "bigquery\|forward_included" src/hermes/pipeline/tomography.py
# → (no output) — clean
```

### Step 4: Deleted v1 SQL file
`git rm src/hermes/sql/queries/06_correlation_tomography_bigquery_union.sql`

### Step 5: Updated dispatch test
Replaced the two old test functions (`test_python_backend_calls_hybrid`, `test_bigquery_backend_calls_sql`) with the brief's exact two tests:
- `test_python_backend_calls_v2` — monkeypatches `run_correlation_tomography` at the module level
- `test_unknown_backend_raises` — confirms `backend="bigquery"` now raises `ValueError`

### Step 6: Import verification
```
.venv/bin/python -c "import hermes.pipeline.correlation_tomography"
# → OK (no output, clean import)
```

### Ruff/Mypy Fixes (minimal, logic-preserving)

**Ruff B023** — `active_ids_anom` / `active_ids_non` are loop variables captured in a lambda inside the greedy set-cover loop. Fixed by materializing them to `_anom`/`_non` before the lambda and passing as default args.

**Ruff E702** — 5 semicolons on single lines (style only). Fixed by `ruff format src/hermes/pipeline/correlation_tomography.py`.

**mypy `[attr-defined]`** (line 42) — `from google.cloud import bigquery_storage` is a dynamic try/except import; added `# type: ignore[attr-defined]`.

**mypy `[return-value]`** (line 880/1148) — `run_mixed_granularity_cover` annotation said `-> list[dict]` but the function returns `(culprits, entity_stats)` (a tuple). Fixed annotation to `-> tuple[list[dict], list[dict]]`.

**mypy `[return-value]`** (line 950) — early-return `return []` (for empty-day case) is now `return [], []` to match the corrected tuple return type.

### Golden data re-blessed
The old `expected_setcover_2026-06-01-sample.json` was blessed against the v1 module (124 culprits). After porting v2, `python tests/golden/bless.py` produced 66 culprits / 250 total. The JSON was updated accordingly — this is intentional: the v2 algorithm is more conservative (alpha=0.01 + odds ratio gate).

## Test Results
```
23 passed, 1 deselected (bq tests skipped)  — all green
ruff check src tests  — All checks passed!
ruff format --check src tests  — 43 files already formatted
mypy  — Success: no issues found in 32 source files
```

## Files Changed
| File | Action |
|------|--------|
| `src/hermes/pipeline/correlation_tomography.py` | Replaced with v2 module + relabeled SQL names + minimal ruff/mypy fixes |
| `src/hermes/pipeline/tomography.py` | Simplified to python-only backend; removed v1 helpers |
| `src/hermes/sql/queries/06_correlation_tomography_bigquery_union.sql` | Deleted (git rm) |
| `src/hermes/sql/queries/06_correlation_tomography_unexplained_hops_union.sql` | Added (path-local attribution SQL) |
| `tests/test_tomography_dispatch.py` | Replaced with brief's two tests |
| `tests/golden/python/expected_setcover_2026-06-01-sample.json` | Re-blessed with v2 output (66 culprits) |

## Commit
`fe7a51b  feat: port v2 correlation tomography (single-track); drop v1 SQL backend`

## Fix pass

**Review findings addressed (2026-06-28):**

### 1. README stale references (`src/hermes/sql/queries/README.md`)
- Removed "BigQuery-only backend" / "two interchangeable backends" / `--tomography-backend` language from Step 06 description.
- Updated Step 06 to describe the single Python hybrid backend plus the path-local `unexplained_hops` step.
- Replaced `06_correlation_tomography_bigquery_union.sql` line in the SQL file-reference block with `06_correlation_tomography_unexplained_hops_union.sql`.
- Changed output table name from `correlation_hyperedges_tomography` to `correlation_hyperedges_tomography_v2` in the Output tables row (line ~182) and the Step 06 header (line ~164).

### 2. Docstrings in `src/hermes/pipeline/correlation_tomography.py`
- Updated three occurrences of unsuffixed SQL names to use the correct `_union` filenames:
  - `correlation_tomography_prepare.sql` → `06_correlation_tomography_prepare_union.sql` (lines ~197 and ~1221)
  - `correlation_tomography_all_edges.sql` → `06_correlation_tomography_all_edges_union.sql` (line ~610)

### Test / lint results
```
.venv/bin/python -m pytest tests/test_tomography_dispatch.py tests/test_golden_python.py -v
→ 3 passed in ~4s

.venv/bin/ruff check src tests
→ All checks passed!

.venv/bin/mypy src tests
→ Found 2 errors in 1 file (tests/golden/bless.py:29-30 — pre-existing, unrelated to these changes)
```

### Commit
`fix: update README + docstrings for single-track v2 correlation (review)`

## Concerns
One non-obvious side effect: the golden data change from 124 → 66 culprits means the v2 algorithm's set-cover terminates earlier on this fixture. This is expected behavior (v2 uses Fisher's exact + odds ratio gate), not a regression. The re-bless is correct.

No logic was changed in the ported module — only: (a) SQL filename relabeling, (b) the three mypy/ruff fixes described above, all of which are annotation corrections or style, none touching algorithmic paths.
