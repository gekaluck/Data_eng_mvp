# How This Project Evolved

**Purpose**: this is the narrative spine — the story of how the system got its current
shape, written to be read start to finish. It exists because the other docs deliberately
destroy narrative. `decisions.md` is an ADR log: atomic, present-tense, self-contained
entries that are excellent reference and unreadable as a story. `incidents.md` is
per-incident. Neither records *causation between* entries, and neither keeps the things
that turned out to be wrong — good documentation hygiene actively deletes those.

This file is also the raw material for a future write-up, so it keeps the false starts,
the beliefs that were later overturned, and the order in which things were understood.
It links to `Dxxx` decisions and `Ixx` incidents rather than restating them.

**Maintenance**: append at the end of a phase of work, or whenever you change your mind
about something. Write it in your own voice; this is the one doc where that is the point.

---

## The one-sentence version

A local crypto lakehouse whose architecture was driven almost entirely by things going
wrong — a billing model reshaped the data model, a three-month gap in the commit log
became a three-month gap in the data, and the worst failure was the one where every
dashboard stayed green.

---

## Act 1 — Choosing everything on paper (2026-02-06)

Twelve decisions were recorded on the first day, before there was any code:
[D001–D012](decisions.md). CoinCap as the source, MinIO for storage, Airflow via the
official Compose file, local PySpark for compute, Iceberg as the table format, entity
modelling in Silver and dimensional in Gold.

Most of them survived. The two that didn't are the interesting ones, and both failed for
the same reason: they were chosen for *simplicity in isolation* and broke when something
else in the system needed them.

Bronze landed on 02-12 with Pydantic contracts at ingestion ([D015](decisions.md)) and
Parquet rather than JSON ([D014](decisions.md)). Silver followed on 02-17, which required a
custom Dockerfile just to get Java 17 under PySpark ([D019](decisions.md)) — the first sign
that "local PySpark" is a bigger commitment than it looks in a diagram.

### The catalog, decided three times

This is the cleanest thread in the whole project:

| When | Decision | What forced it |
|------|----------|----------------|
| 02-06 | [D003](decisions.md) — JDBC catalog on SQLite | Chosen on paper as "the simplest catalog" |
| 02-17 | [D020](decisions.md) — Hadoop catalog, supersedes D003 | SQLite was awkward across Docker containers |
| 04-01 | [D023](decisions.md) — JDBC catalog on Postgres, supersedes D020 | **Trino cannot read a Hadoop catalog** |

Eleven days from "simplest possible choice" to the first reversal. The pattern only becomes
visible in hindsight: **each catalog choice was correct until a new consumer arrived.**
SQLite was fine until there were multiple containers. Hadoop was fine until there was a
second query engine. The catalog looked like a storage detail and was actually an
integration point — the one component every engine has to agree on.

The lesson worth writing up: when you pick the simplest option, ask *simplest for whom*.
D003 was simplest for the writer and hostile to every future reader.

---

## Act 2 — The second engine, and the choice with no ADR (2026-04-01)

Spark Gold landed 03-24. A week later, dbt and Trino arrived together in a single commit,
and the project acquired **two Gold implementations of the same transformations** — one in
the PySpark DataFrame API, one in dbt SQL — deliberately kept side by side to compare.

Here is the thing worth noticing, and it is why this file exists:

> **The most consequential architectural decision in the project has no ADR.** There is no
> entry for "adopt dbt", none for "adopt Trino". They appear in `decisions.md` only as a
> *consequence* — [D023](decisions.md) exists because Trino couldn't read the catalog the
> project already had.

The decision log faithfully recorded the second-order effect and missed the cause. That is
not a discipline failure so much as a property of ADR logs: you write an entry when you
feel a choice being made, and adding a tool that "obviously" belongs doesn't feel like one.

The two-implementation setup also had a cost that took months to surface. A comparison is
only worth something if both sides stay in step, and they drifted twice:

- [I15](incidents.md) — range support was added to Spark Gold and not to dbt Gold, so a
  seven-day catch-up would have built seven days in one and one day in the other.
- [I16](incidents.md) — gap tolerance ([D025](decisions.md)) was applied to Spark Gold and
  not to dbt Gold, so they disagreed on exactly the dates the comparison was most
  interesting.

Both were invisible while each table looked fine on its own. **The comparison you build to
learn from becomes a thing you have to maintain, and it fails silently.**

---

## Act 3 — Three months of nothing (2026-04-02 → 2026-07-08)

The commit log has a hole here. So does the data.

That is not a coincidence — it is the same hole. The honest version: I built the skeleton of
the project, my priorities moved to other things, and I came back to it three months later.
Nothing failed. The only thing fetching CoinCap was a local Airflow DAG, so data accrued
only on days the laptop happened to be on — and for three months, it wasn't
([I1](incidents.md): an 87-day gap, 2026-04-08 → 07-03, plus a 4-day one in March).

For a write-up this is the strongest single image the project has: *put the git history and
the data coverage chart side by side and they are the same picture.* A side project's
coverage is a record of the author's attention, and nothing in the architecture said so
until it was too late to fix.

**The gap is staying.** It was tempting to buy the history back and present a smooth line,
and that turned out to be the wrong instinct for three separate reasons — recorded as
[D032](decisions.md). The short version: it would cost roughly 17 months of free quota, the
history endpoints return only 2 of the 5 measure columns so the "filled" region would be
mostly null anyway, and a sparse dataset is a *better* test bed than a dense one for the
agent layer being built next, whose whole design is about refusing to answer when the data
can't support it. Filling the gap would have deleted the most interesting thing to test
against, and paid for the privilege.

---

## Act 4 — A billing model reshapes the data model (2026-07-11)

One day, three incidents, two decisions, and the strategy of the project changes.

1. [I2](incidents.md) — a single five-day, ~20-coin history backfill consumed **~498 of 500**
   monthly credits. CoinCap's free tier bills by data volume, not call count.
2. [I3](incidents.md) — Gold produced **zero rows** for any date whose predecessor was
   missing, because it computed a day-over-day `LAG` and then filtered out null
   predecessors.
3. [I4](incidents.md) — Bronze retried quota-exhaustion `429`s five times with 60s backoffs,
   burning more of a quota that was already gone, because a per-minute limit and a monthly
   limit look identical from the response code.

The chain runs like this, and it is the core argument of any article about this project:

> **You cannot buy history back** (I2) → **so a missed day is permanent, not inconvenient**
> (which is what made I1 serious rather than annoying) → **so the data will be permanently
> sparse** → **so every transform must tolerate gaps instead of assuming density** (D025).

[D024](decisions.md) made building forward the strategy: the daily snapshot is the way
history accrues, and deep backfill is a rare, deliberate, human-approved operation.
[D025](decisions.md) made Gold keep rows with null change columns rather than dropping
them.

A pricing page changed the shape of the transforms. That is not how anyone draws the
architecture diagram.

---

## Act 5 — Getting the capture off the laptop (2026-07-12 → 07-29)

If a missed day is unrecoverable, the component that must never miss is the *cheapest* one:
a single API call. [D026](decisions.md) moved it to a GitHub Actions cron writing to S3;
[D027](decisions.md) made that the only scheduled call in the system, with local Airflow
syncing whatever landed and processing exactly those dates.

The insight worth keeping: **capture and processing have opposite requirements.** Capture
is cheap, must not miss, and needs to run whether or not anyone is around. Processing is
expensive, can catch up, and wants to be local for iteration speed. They had been welded
together because they started in the same DAG. Only the must-not-miss half needed to leave.

This era generated a cluster of unglamorous, very real incidents — the kind that don't make
architecture diagrams but consume entire evenings: an IAM policy whose ARN didn't match the
bucket ([I5](incidents.md)); a write-only key used for a read ([I7](incidents.md)); Trino
breaking because a branch switch removed a bind-mounted config file from under a running
container ([I8](incidents.md)); the orchestrator scheduled 30 minutes *before* the capture
it depended on, adding 24h of lag to every run ([I14](incidents.md)).

And one trap that keeps paying dividends in problems: **`/assets` is a live endpoint with
no date parameter.** A run's date label and its contents can disagree — the `--date` flag
chooses the object key, not the data ([I6](incidents.md)). That property is the seed of the
worst data corruption in the project.

---

## Act 6 — The failure where everything stayed green (2026-07-22 → 07-31)

Two Gold DAGs were paused, probably during dashboard work. Triggering a paused DAG doesn't
fail — it creates a run that sits in `queued` forever, and
`TriggerDagRunOperator(wait_for_completion=True)` polls it with no timeout. Bronze and
Silver kept producing fresh data daily. Thirty orphaned queued runs accumulated.

**It took eight days to notice, and it was noticed by a human looking at a chart**
([I9](incidents.md)).

Then, looking closely at that chart, a date where every coin showed exactly 0.00% change
([I11](incidents.md)). Twenty assets never all close exactly flat. The cause was the live-endpoint
trap from I6, now realised at scale: catch-up runs had written *one* API response under
*two* date labels, so two adjacent dates held identical prices and the day-over-day change
was mathematically zero for every coin at once ([I10](incidents.md)).

The repair pass found more than it went looking for:

- Running the duplicate signature across *all* history turned up a **second** fabricated
  flat day on 07-19, four days outside the window that had been documented as the extent of
  the damage ([I17](incidents.md)). The recorded extent had been "where we happened to look".
  Both were repaired from the history endpoints; the proof was that the two Bronze objects
  were byte-identical *and shared an S3 LastModified timestamp*.
- The repair itself failed the first time, six minutes in, on a coin CoinCap has no history
  for — after spending credits on every coin ahead of it, with retries configured to spend
  them again ([I18](incidents.md)).
- [I16](incidents.md) turned out to have been fixed in *code* weeks earlier. Only the stored
  data still diverged, because nothing rebuilds a partition when a model changes. **A
  transform fix and the data it already produced are two different things.**
- Two serving models had never been backfilled at all: they held **9 dates against
  `daily_snapshot`'s 107**, while every test stayed green ([I19](incidents.md)).
  `data_availability_daily` had been faithfully reporting it as 98 `partial` days the whole
  time — and, as that incident puts it, *a report nobody has to acknowledge is not an
  assertion.*

[D029](decisions.md) states what all of these share: **silence is a failure mode.** Waits
get timeouts, coverage gets asserted by tests rather than reported on a dashboard, a quiet
upstream is distinguished from a dead one, and operations that cost money don't retry.
[D031](decisions.md) applies the same idea to the dashboard, splitting "what the data says"
from "whether you should believe it".

---

## Things I believed that turned out to be wrong

The most perishable material in the project, and the reason this file exists: correcting a
belief usually means *deleting* it from the docs, so by the time you write anything up, the
mistakes have been tidied away.

**"Bronze is an immutable landing zone."** It says so in early architecture notes. It was
never true: Bronze was written with `replace=True` from the start, and objects for 07-19 and
07-22..07-28 were overwritten with wrong-day prices. The claim was removed from
`architecture.md` and recorded as [D028](decisions.md). *"Raw landing zone" and "mutable in
place" are incompatible, and calling something immutable does not make it so.*

**"The simplest catalog is the best catalog."** Simplicity measured at the wrong boundary
cost two migrations. See Act 1.

**"A skip is a safe default."** When the sync finds nothing new it skips, which is exactly
right when there's genuinely nothing new — and indistinguishable from the upstream being
dead. The safe-looking default hid the failure mode for months (fixed by the freshness
check in D029).

**"Two implementations give you a comparison."** They give you a comparison only while both
are maintained. Otherwise they give you something worse than one implementation: false
confidence, plus twice the surface area (I15, I16).

**"Green tests mean correct data."** I19 is the counterexample: every test passed while two
of five serving models held 8% of the dates they should have. Tests only assert what someone
thought to assert.

**"The incident is closed because the fix is merged."** I16 was fixed in code and open in
the data for weeks.

---

## Threads worth pulling for a write-up

- **Cost as an architectural force.** A free-tier billing model (volume-metered, 500/month)
  determined the ingestion strategy, the gap-tolerance of every transform, the decision to
  move capture to the cloud, and even the retry policy. Most architecture writing treats
  cost as a footnote.
- **Spark vs dbt, honestly.** Two implementations of the same Gold models, and what the
  comparison actually taught — including that maintaining it is a real cost, and that
  divergence is silent by default.
- **The three-month hole.** The git log and the coverage chart are the same picture.
- **Silence as the dominant failure mode.** Not one of the expensive incidents in this
  project was a crash. They were all things that looked fine: a paused DAG, a skip, a green
  test suite, a plausible-looking 0.00%.
- **What "immutable" means when nothing enforces it.**
- **Building for an LLM reader.** The Gold semantic layer exists because the next consumer
  of this data is an agent that cannot ask a follow-up question — and the things it would
  get confidently wrong are exactly the things a human would too.

---

## Where it stands (2026-08-07)

107 distinct dates over ~198 calendar days, all three serving models in step, both Gold
implementations agreeing on every date, and no fabricated flat days left. Hardening is
finished: H5 closed with D033, so every new Bronze snapshot records when it was fetched —
the thing that would have made I10 and I17 *detectable* instead of inferred from a price
coincidence. It arrives, as usual here, after the incidents rather than before them, and it
can only ever cover the dates written from now on.

The next act is the AI-agent layer: [`ai-agent-architecture.md`](ai-agent-architecture.md),
with the platform prerequisites listed in [`pre-ai-readiness.md`](pre-ai-readiness.md).
The first prerequisite milestone landed on 2026-08-06: the future agent now has a
read-only, resource-bounded Trino lane over an explicit five-table dbt Gold allow-list, and
the Gold DAG publishes dbt metadata after successful builds. The platform now presents the
boundary the MCP server was designed against. Later the same day, D036 crossed the next
gate: dependencies moved into an AI-only environment and CI job, and the first executable
guardrail began rejecting anything except a single, fully qualified, allow-listed Trino
`SELECT`. D037 then added the five catalog tools without adding a network surface: dbt's
published manifest/catalog now drive the allow-listed table index, docs, tests, and bounded
lineage, while the restricted Trino identity reads live Iceberg columns, snapshots, and
file statistics through fixed query shapes. The first live reconciliation caught two kinds
of drift immediately—the weekly model's physical alias was wrong in the allow-list, and two
live coverage columns lacked dbt descriptions—and both were corrected before transport.
D038 then put one typed registry behind both intended MCP frontends: stdio for a commodity
local host and stateless streamable HTTP for the owned client path. The HTTP surface stays
on loopback with DNS-rebinding checks, and the official client smoke proves that both paths
discover and invoke the same five tools, including the same structured denial. The agent
still cannot execute an analytical query, but an MCP client can now acquire trustworthy
planning context without bypassing the allow-list.

D039 made the first caller-supplied SQL cross that boundary, but stopped deliberately at
planning. `explain_query` runs the same AST and table-scope proof before wrapping the
statement in Trino's ordinary distributed `EXPLAIN`—never `EXPLAIN ANALYZE`—and caps the
returned plan before it reaches an LLM context. A missing column is now useful typed
feedback (`valid: false`) rather than a generic server failure, while access and connection
problems remain tool errors. The tempting companion, `sample_rows`, was deferred: unlike
planning it reads business data, and the design already promises an audit record and a
budget charge for every sample. Those controls should exist before that tool does.

D040 supplied those controls and then added the sample, in that order. Planning and
sampling now spend from the same request-scoped counter—three Trino attempts in `fast`, ten
in `thorough`—so exploration cannot acquire a second hidden budget. `sample_rows` never
accepts SQL; it owns a quoted `SELECT * ... LIMIT n`, refuses more than 20 rows, and writes
the attempt to a local JSONL record before returning. The audit keeps the evidence needed
to debug a loop (request, table, verdict, timing, shape, failure) without quietly becoming
a second store of business-row values. The first live parity check used all three fast
tokens on each transport and proved the fourth call fails before Trino. The remaining query
step is deliberately larger: `execute_query` still needs arbitrary-SQL row truncation,
scan/time enforcement, execution stats, and the same fail-closed audit boundary.
