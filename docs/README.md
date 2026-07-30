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
| Why was X decided? | [`decisions.md`](decisions.md) | D001–D030, dated, append-only |
| What broke, and what did it teach us? | [`incidents.md`](incidents.md) | I1–I18, plus remaining hardening items |
| How do I operate it / debug a symptom? | [`runbook.md`](runbook.md) | First checks by symptom |
| How does the daily cloud capture work? | [`autonomous-daily-capture.md`](autonomous-daily-capture.md) | GitHub Actions → S3 |
| How does Superset serve the data? | [`superset.md`](superset.md) | Serving profile, bootstrap |
| How do I browse the lakehouse tables? | [`table_browser.md`](table_browser.md) | Jupyter/Trino exploration |
| What is the AI-agent layer going to be? | [`ai-agent-architecture.md`](ai-agent-architecture.md) | **Design authority** for the next phase |
| How should an agent work in this repo? | [`../CLAUDE.md`](../CLAUDE.md) | Operating rules — branching, scope, questions |

**Historical, not current** — kept for the reasoning, banner-marked at the top of each:
[`milestones.md`](milestones.md), [`m1-setup.md`](m1-setup.md),
[`m2-bronze.md`](m2-bronze.md), [`m3-silver.md`](m3-silver.md).

### Precedence when two docs disagree

`decisions.md` (newest entry wins) → `architecture.md` → `runbook.md` → everything else.
The M-docs never win; they describe the past. If you find a contradiction that this
ordering doesn't settle, that's a bug in the docs — fix it rather than guessing.

---

## How the project evolved

The short version: **this project is a lakehouse whose design was driven almost entirely by
things going wrong.** Nearly every structural choice is a response to a specific failure.
Reading `incidents.md` alongside `decisions.md` is the fastest way to understand *why* the
system looks like it does.

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

One hardening item is still open — **H5**, recording the fetch timestamp in Bronze, tracked
at the bottom of [`incidents.md`](incidents.md).

---

## Things that will surprise you about this data

Worth knowing before writing a query or an analysis, and doubly so before pointing an LLM
at it:

- **Coverage is sparse.** 107 distinct dates spanning ~198 calendar days. Any "over the last
  N days" question is answering over a series with holes.
- **`price_change_pct` is null by design** on the first day after a gap (D025). It is not
  missing data; it means the previous day genuinely isn't there.
- **Repaired days are lower fidelity.** Dates rebuilt from the history endpoints have null
  `vwap_24hr` and `change_percent_24hr`, and carry a wider coin universe (25 vs 20), because
  those endpoints return less than `/assets` does (D024).
- **Bronze is not immutable** for 07-19 and 07-22..07-28. Silver holds the better values
  there; do not rebuild Silver from Bronze for that window (D028).
