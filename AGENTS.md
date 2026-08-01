# AGENTS.md

**The operating rules for this repository live in [`CLAUDE.md`](CLAUDE.md). Read that file
before doing anything else — it is the single source of truth, and this file exists only so
that agents which look for `AGENTS.md` are pointed at it.**

Deliberately not duplicated here: keeping two copies of the rules means keeping two copies
in sync, and the one that drifts is always the one nobody is looking at.

After `CLAUDE.md`, read [`docs/README.md`](docs/README.md) — the documentation map. It says
which document is authoritative for what, and which ones are historical records that
describe a system that no longer exists.

---

## The four that are expensive to get wrong

Everything below is also in `CLAUDE.md`. It is restated here, and only this much, because
these are the rules whose violation is either irreversible or costs real money.

1. **Never commit to `main`.** Branch per unit of work: `feat/<short-name>` or
   `chore/<short-name>`. Open a PR when it's done.
2. **Never commit secrets.** Real credentials live only in `.env`, which is gitignored. Only
   `.env.example` is committed, with placeholders.
3. **CoinCap calls cost real money.** The free tier is metered by data volume, roughly
   500 credits/month, and one careless backfill has already consumed a whole month's worth
   (see incident I2). Do not call the API, and do not trigger
   `bronze_coincap_history_backfill`, without the human explicitly agreeing to the spend.
4. **Ask before assuming.** This is a learning project; the simplest correct solution wins.
   If requirements, scope, or constraints are unclear, ask rather than guess.

## Before you open a PR

`CLAUDE.md` §4 has a table of which document to update and what triggers each. Work through
it — updating docs at PR time is deliberate, because the reasoning and the exact symptom are
still in your head then and cannot be reconstructed from the diff later.

The short version: `architecture.md` when the shape of the system changed, `decisions.md`
when a choice was made, `incidents.md` when something broke or you found something already
broken, `evolution.md` for the narrative, `runbook.md` when there's a new way to operate or
debug something.

## Running things

Tests run inside Docker, and on Windows via PowerShell — Git Bash mangles container paths:

```powershell
make test          # full pytest suite in the scheduler container
make test-dag      # DAG-integrity tests only
```

`docker compose restart <svc>` reuses the stale container definition; use
`docker compose up -d --force-recreate <svc>` after a branch switch (incident I8).
