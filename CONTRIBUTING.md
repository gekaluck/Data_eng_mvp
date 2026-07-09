# Contributing & Repo Conventions

A short map of how this repo is organized and how to work in it, so any session — human
or agent — has the context it needs. This is a personal learning/portfolio project;
conventions favor clarity over ceremony.

## Golden rule for the AI-agent layer

The existing Bronze/Silver/Gold pipeline is **stable and off-limits** to the new work.
When building the [`ai_agent/`](ai_agent) module, do **not** change DAGs, PySpark
transforms, or dbt models. The agent layer extends the platform through read-only access
and configuration only. Design authority is [`docs/new_ARCHITECTURE.md`](docs/new_ARCHITECTURE.md).

## Where things live

| Path         | What                                                          |
|--------------|--------------------------------------------------------------|
| `dags/`      | Airflow DAGs — orchestrator, leaf DAGs, backfills            |
| `spark/`     | PySpark transforms and helpers                               |
| `dbt/`       | dbt project (SQL Gold path + data tests)                     |
| `config/`    | Trino catalog/service config                                 |
| `tests/`     | pytest suite (DAG integrity, schemas, transforms)           |
| `scripts/`   | PowerShell helpers to run the stack                          |
| `docs/`      | Architecture, decisions, runbook, milestones                |
| `ai_agent/`  | AI-agent layer skeleton (Phase A, not yet implemented)      |

## Branching & commits

- **Never commit to `main`.** Branch per feature: `feat/<short-name>` or `chore/<short-name>`.
- Commit incrementally with clear, scoped messages; keep unrelated changes in separate commits.
- When a feature is done, open a PR with a short summary.
- Before starting new work, confirm the previous PR is merged.

## Documentation after action

After a milestone, update the relevant docs — especially
[`docs/architecture.md`](docs/architecture.md) and [`docs/decisions.md`](docs/decisions.md).
The decision log is **append-only**: record reversals as a new superseding entry
(e.g., `D023 supersedes D020`) rather than rewriting history.

## Secrets

Real credentials live only in `.env` (gitignored). Only `.env.example` is committed, with
`REPLACE_WITH_...` placeholders. Never commit real keys, tokens, or passwords; config and
notebooks reference secrets via `${ENV:...}` / `os.environ[...]`, never inline literals.

## Running the stack & checks

```powershell
.\scripts\stack.ps1 up            # start (creates .env from template on first run)
.\scripts\stack.ps1 status        # container status + service URLs
.\scripts\stack.ps1 down          # stop (add -Volumes to wipe local data)

make test                         # full pytest suite inside the scheduler container
make test-dag                     # DAG-integrity tests only
```

dbt runs against Trino; see [`dbt/README.md`](dbt/README.md). CI
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs static checks only
(Python error-lint + `compileall`, and `dbt parse`) — it never starts the stack.

## Style

- Prefer explicit, readable code over cleverness; comment where it aids a learner.
- Match the conventions of the surrounding code.
- Keep the project runnable locally and local-first unless a change explicitly says otherwise.
