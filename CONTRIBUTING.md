# Contributing to HERMES

Thanks for your interest in improving HERMES! This guide covers the local setup
and the checks that run in CI.

## Development setup

Requires Python **3.11+**.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"      # installs runtime deps + ruff, mypy, pytest, pre-commit
pre-commit install           # run linters automatically on commit
```

## Checks (these gate every pull request)

CI (`.github/workflows/ci.yml`) runs exactly these; run them locally before pushing:

```bash
ruff check src tests         # lint
ruff format --check src tests  # formatting (drop --check to auto-format)
mypy                         # type checking
pytest tests/ -m "not bq"    # unit tests (BigQuery-backed tests are opt-in)
```

Tests marked `bq` hit live BigQuery and are **excluded by default** (they need
GCP credentials). Run them deliberately with `pytest -m bq`.

### SQL

BigQuery SQL lives in `src/hermes/sql/`. It is parameterized with `${DAY}`-style
placeholders. `.github/workflows/sql-lint.yml` checks that every query still
**parses** in the BigQuery dialect (via sqlfluff's placeholder templater); style
is not enforced. Validate locally with:

```bash
pip install sqlfluff
sqlfluff lint src/hermes/sql   # PRS/TMP entries are real errors; the rest is advisory
```

## Pull requests

- Branch off `main`; use a descriptive branch name (e.g. `fix/...`, `feat/...`,
  `chore/...`).
- Keep PRs focused and include a clear description of the change and its intent.
- Make sure the checks above are green.
- Never commit secrets (see [SECURITY.md](SECURITY.md)); use a git-ignored
  `.env` for `CLOUDFLARE_API_TOKEN` and ADC for BigQuery.

By contributing you agree to abide by our
[Code of Conduct](CODE_OF_CONDUCT.md).
