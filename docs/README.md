# Documentation Map

Start here. This file says **which document is authoritative on what**, and tells the story
of how the project got to its current shape. Several docs in this folder are deliberately
kept as historical records — reading them as descriptions of the current system will
mislead you.

---

## Which doc answers which question

| Question | Doc | Notes |
|----------|-----|-------|
| How do I run this? | [`../README.md`](../README.md) | Quick start, stack overview, roadmap |
| How is it built, and why that shape? | [`architecture.md`](architecture.md) | Current design. Authoritative |
| Why was X decided? | [`decisions.md`](decisions.md) | Dated, append-only. Read the newest entry on a topic; earlier ones may be superseded |
| What broke, and what did it teach us? | [`incidents.md`](incidents.md) | I1–I19, plus the hardening items they produced |
| How did the project get here? | [`evolution.md`](evolution.md) | The narrative spine, and the draft for a future write-up |
| How do I operate it / debug a symptom? | [`runbook.md`](runbook.md) | First checks by symptom |
| How does the daily cloud capture work? | [`autonomous-daily-capture.md`](autonomous-daily-capture.md) | GitHub Actions → S3 |
| How does Superset serve the data? | [`superset.md`](superset.md) | Serving profile, bootstrap |
| How do I browse the lakehouse tables? | [`table_browser.md`](table_browser.md) | Jupyter/Trino exploration |
| What is the AI-agent layer going to be? | [`ai-agent-architecture.md`](ai-agent-architecture.md) | **Design authority** for the next phase |
| What must be done before that phase starts? | [`pre-ai-readiness.md`](pre-ai-readiness.md) | Punch list against the running system, checked 2026-07-31 |
| How should an agent work in this repo? | [`../CLAUDE.md`](../CLAUDE.md) | Operating rules — branching, scope, questions. [`../AGENTS.md`](../AGENTS.md) points here for non-Claude agents |

**Historical, not current** — everything under [`historical/`](historical/) describes the
system as it *was*, kept for the reasoning rather than the facts:
[`milestones.md`](historical/milestones.md), [`m1-setup.md`](historical/m1-setup.md),
[`m2-bronze.md`](historical/m2-bronze.md), [`m3-silver.md`](historical/m3-silver.md).

The folder is the signal. A banner at the top of a file only helps someone who opens it and
reads from the top — but most encounters with a doc are a grep hit or a file listing, where
only the path is visible. `docs/historical/m2-bronze.md` carries the warning into every one
of those. The banners are still there as well.

### Precedence when two docs disagree

`decisions.md` (newest entry wins) → `architecture.md` → `runbook.md` → everything else.
The M-docs never win; they describe the past. If you find a contradiction that this
ordering doesn't settle, that's a bug in the docs — fix it rather than guessing.

---

## How the project evolved

The short version below is for orientation. The full narrative — with the causation between
decisions, the Spark-vs-dbt thread, and the beliefs that turned out to be wrong — is in
[`evolution.md`](evolution.md).

**This project is a lakehouse whose design was driven almost entirely by things going
wrong.** Nearly every structural choice is a response to a specific failure. Reading
`incidents.md` alongside `decisions.md` is the fastest way to understand *why* the system
looks like it does.

### Phase 1 — Build the layers (2026-02 → 2026-04)

M0–M3, documented in the M-docs. Docker Compose with Airflow and MinIO, a Bronze DAG
fetching CoinCap `/assets` daily into Parquet, then a PySpark Silver layer on Iceberg with
entity modelling (coins vs price snapshots). Gold followed, in two implementations —
PySpark and dbt — kept side by side deliberately, to compare the two ways of expressing the
same transformations.

At this point the pipeline worked and the data was quietly terrible.

### Phase 2 — Discover the constraints (2026-07-11)

Two discoveries in one day reshaped the strategy:

- **CoinCap's free tier is credit-metered by data volume, not call count.** A single 5-day
  history backfill consumed ~498 of 500 monthly credits (I2). You cannot buy history back.
- **Gold produced zero rows whenever the prior day was missing** (I3), because it computed
  a day-over-day `LAG` and then filtered out null predecessors.

Together these forced **D024 (build forward, don't backfill deep)** and **D025 (Gold
tolerates coverage gaps)**. If history is unaffordable, missing days are permanent — so the
transforms must tolerate a sparse series instead of demanding a dense one.

### Phase 3 — Stop depending on the laptop (2026-07-28 → 07-29)

The coverage audit found a ~3-month hole: data only accrued on days the machine happened to
be running (I1). Since history can't be repurchased, *missing a day is unrecoverable* — which
made the one unreliable component the one that most needed to be reliable.

**D026/D027** split capture from processing. A GitHub Actions cron makes the single daily
call and writes to S3; local Airflow syncs whatever landed and processes exactly those
dates. Capture is cheap and must not miss; processing is expensive and can catch up. They
have opposite requirements, so only the cheap half left the laptop.

This phase also produced the trap that keeps recurring: `/assets` is a **live** endpoint with
no date parameter, so a run's date label and its data can disagree (I6, I10, I17).

### Phase 4 — Make failures loud (2026-07-30)

An 8-day Gold outage (I9) went unnoticed because two Gold DAGs were paused: triggering a
paused DAG queues a run the scheduler never starts, and the orchestrator waited on it
forever while Bronze and Silver stayed green. Nothing asserted that the layers agreed.

**D029** states the principle: *silence is a failure mode*. Waits are bounded, coverage is
asserted by tests, a quiet upstream is distinguished from a dead one, and metered operations
don't retry. The same pass repaired two fabricated flat-market days (I11, I17) — dates where
one API response had been stored under two labels, producing an exact 0.00% change for
every coin at once.

### What's next

The platform is stable. The next track is the **AI-agent layer** —
[`ai-agent-architecture.md`](ai-agent-architecture.md) is the design authority, scaffolded
under [`../ai_agent/`](../ai_agent). It reads the Gold layer only, through an allow-list,
which is why Gold's column-level descriptions in `dbt/models/gold/schema.yml` matter more
than they look: they are the semantic layer an agent reasons over.

Hardening is finished: H5 landed with D033, so Bronze now records when each snapshot was
fetched and the open-items table in [`incidents.md`](incidents.md) is empty. What remains
before agent code starts is configuration and two decisions, listed in
[`pre-ai-readiness.md`](pre-ai-readiness.md) — most notably that the pipeline is currently a
day behind, because GitHub's cron drift outgrew the orchestrator's one-hour buffer.

---

## Things that will surprise you about this data

Worth knowing before writing a query or an analysis, and doubly so before pointing an LLM
at it:

- **Coverage is sparse, permanently and on purpose.** 107 distinct dates spanning ~198
  calendar days, with an 87-day gap (2026-04-08 → 07-03) and a 4-day one in March. Any "over
  the last N days" question is answering over a series with holes. The gaps will not be
  filled — see [D032](decisions.md) for why, and don't propose backfilling them.
- **`price_change_pct` is null by design** on the first day after a gap (D025). It is not
  missing data; it means the previous day genuinely isn't there.
- **Repaired days are lower fidelity.** Dates rebuilt from the history endpoints have null
  `vwap_24hr` and `change_percent_24hr`, and carry a wider coin universe (25 vs 20), because
  those endpoints return less than `/assets` does (D024).
- **Bronze is not immutable** for 07-19 and 07-22..07-28. Silver holds the better values
  there; do not rebuild Silver from Bronze for that window (D028).
