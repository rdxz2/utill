# Repository Instructions

This repository is a Python utility library that wraps multiple services (for example BigQuery, PostgreSQL, Metabase, and Google Drive) and also exposes a CLI.

Key locations:

- `src/utill/`: core library modules
- `src/utill/cmd/`: CLI command definitions and handlers
- `src/utill/templates/`: config templates copied to `~/.utill/`

## Repository Overview

`utill` is a Python utility library that provides reusable helpers and service wrappers for common data workflows.

Primary integrations include:

- BigQuery / Google Cloud Storage
- PostgreSQL
- Metabase
- Google Drive

The repository also exposes a CLI (`utill`) for common operational tasks.

## Project Structure

- `src/utill/`: Core library modules
- `src/utill/cmd/`: CLI command definitions and handlers
- `src/utill/templates/`: Template config files copied to `~/.utill/`
- `pyproject.toml`: Packaging, dependencies, and lint settings
- `.github/workflows/build-and-publish.yaml`: Release workflow (publish on `v*` tags)

Notable modules:

- Service wrappers: `bq.py`, `my_pg.py`, `my_mb.py`, `my_gcs.py`, `my_gdrive.py`
- General helpers: `my_csv.py`, `my_datetime.py`, `my_dict.py`, `my_encryption.py`, `my_file.py`, `my_json.py`, `my_queue.py`, `my_string.py`, `my_style.py`
- CLI entrypoint: `src/utill/cmd/utill.py`

## Local Development Setup

### Option A: `uv` (recommended)

```sh
uv sync --all-extras --group dev
```

### Option B: `venv` + `pip`

```sh
python -m venv .venv
source .venv/bin/activate
pip install -e .
pip install -e ".[google-cloud,postgresql,pdf]"
pip install ruff
```

## Configuration Files

Runtime configuration is stored under `~/.utill/`:

- `~/.utill/env`
- `~/.utill/pg.json`
- `~/.utill/mb.json`

Initialize these with:

```sh
utill conf init google-cloud
utill conf init postgresql
utill conf init metabase
```

## Code Style and Conventions

- Target Python: `>=3.10`
- Use type hints for public functions and methods
- Keep imports sorted (Ruff `I` rules are enabled)
- Keep command wiring in `src/utill/cmd/utill.py`
- Put command behavior in `src/utill/cmd/_*.py`
- Validate inputs early and raise clear exceptions (`ValueError` where appropriate)

Ruff lint settings are defined in `pyproject.toml` (`F` and `I` checks).

## Adding New Features

When adding a new service integration:

1. Add a wrapper module in `src/utill/`.
2. Add or update CLI commands in `src/utill/cmd/utill.py` and matching handler(s) in `src/utill/cmd/_*.py`.
3. Add optional dependencies in `pyproject.toml` under `[project.optional-dependencies]` if needed.
4. Add template config(s) in `src/utill/templates/` if the service needs user config.
5. Update `README.md` examples for any new public API or command.

## Validation Before Opening a PR

Run lint checks:

```sh
uv run ruff check .
```

You can auto-fix import/order issues with:

```sh
uv run ruff check . --fix
```

There is currently no dedicated test suite in CI, so include manual verification notes in your PR (for example: commands run, environments used, and observed output).

# Agent Skills

- For lazy-loaded reusable module objects (for example `bq` and `gcs`), follow the implementation rules in `.github/skills/lazy-loading/SKILL.md`.

## Commit and PR Guidelines

- Prefer clear commit prefixes such as `feat:`, `fix:`, `chore:`, `docs:`
- Keep commit messages short and specific
- Keep PRs focused on one logical change
- Document breaking changes and migration steps in the PR description

## Security and Secrets

- Never commit credentials, API keys, or real connection profiles
- Keep local config files and generated artifacts out of version control
- Use placeholder/sample values in templates and docs

## Release Notes

Publishing is automated via GitHub Actions when a tag matching `v*` is pushed.
