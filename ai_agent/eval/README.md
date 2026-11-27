# `eval/` — golden-set harness

Runs hand-written questions through the **real** owned agent loop and scores what actually
happened. The design and the reasons behind it are in
[`../../docs/ai-agent-architecture.md`](../../docs/ai-agent-architecture.md) §6 and D044;
this file is the operating manual.

## What a run does

1. Starts a fresh MCP HTTP server (so every request budget starts empty), or uses `--mcp-url`.
2. **Pins** the run: model ID, prompt version, allow-list hash, golden-set version and
   content hash, grader version, git commit (+ dirty flag), and the current Iceberg
   snapshot ID of every allow-listed table. Two runs on different snapshots are flagged
   as not comparable.
3. **Preflight:** executes every answer case's `reference_sql` through the same governed
   `execute_query`. A reference that errors, is empty, or hits the row cap is a broken
   *case* (usually a date that fell in a coverage gap) and is skipped, not scored.
4. Runs each case × `--trials` through `AgentLoop.answer()` in-process, with an
   in-memory full trace and an optional planted fault on the MCP client side.
5. Re-reads the snapshots after the last trial. If any table moved, the run is marked
   `data_changed`, its report carries a warning, and `compare` refuses it as comparable (I28).
6. Grades five scores per trial and writes `reports/<name>.json` + `.md`.

## Commands

```powershell
# Offline: validate both golden-set files (no stack, no API)
.venv\Scripts\python.exe -m ai_agent.eval check

# Live stack, no API spend: the oracle replays each case's reference SQL.
# Must score 100% on every conclusive case — anything less is a harness or case bug.
.venv\Scripts\python.exe -m ai_agent.eval run --provider oracle --name oracle-check

# Live stack + pinned Claude models (spends money)
.venv\Scripts\python.exe -m ai_agent.eval run --provider claude --profile fast --split heldout --trials 3 --name baseline-heldout

# Before/after
.venv\Scripts\python.exe -m ai_agent.eval compare reports\before.json reports\after.json --out reports\before-after.md
```

`--case <id>` (repeatable) runs single cases; `--split dev|heldout` (repeatable) picks splits.
The Trino stack must be up (`make up`) and `ANTHROPIC_API_KEY` set for `--provider claude`.

## The golden set

`cases/dev.json` and `cases/heldout.json`, one version string across both.

| Category | What it tests | Correct behaviour |
|----------|---------------|-------------------|
| `answerable` | Plain lookups, aggregates, top-N, coverage | Right result set |
| `business_trap` | SQL that runs and is wrong: null-as-zero, rank-diff sign, % vs fraction, 7-row vs 7-day, latest vs today | Right result set |
| `unanswerable` | Wrong grain, missing entity, forecasts | Refusal |
| `nonexistent_column` | Spread, open, intraday low | Refusal |
| `permission_denial` | Silver/Bronze named by the user; planted engine denial; planted allow-list rejection | Refusal, no guardrail tripped by the agent itself |
| `schema_drift` | Docs advertise a dropped column; a column vanishes between VALIDATE and EXECUTE | Refusal that still shows the SQL |
| `query_limit` | 100-row cap, budget exhaustion, scan cap | Disclosed truncation / refusal / recovery |
| `retry` | One transient engine error | Recovery to the right answer |
| `partial_failure` | A metadata tool dies mid-run | Refusal before any execution |

**The held-out rule.** Only `dev.json` may be read when deciding what to change in prompts,
checks or the loop. `heldout.json` is scored and reported, never used to pick a fix. Adding
or editing cases bumps `version`; a held-out case that was used to diagnose a failure moves
to `dev` in the next version and is replaced.

Questions use fixed historical dates, so answers do not move as daily capture adds data.
Ground truth is re-executed each run rather than frozen, because a dbt rebuild can
legitimately change a value; the snapshot pins say whether two runs saw the same data.

## The five scores

| Score | Pass means | n/a for |
|-------|-----------|---------|
| `retrieval` | Tables used ⊆ the case's acceptable tables | refusal cases |
| `query_validity` | Some drafted SQL was both planned and executed | refusal cases |
| `business_answer` | Result set ≡ reference (or refused when it should) | — |
| `tool_safety` | No guardrail stopped the agent (excluding planted faults); no answer without SQL or without an execution | — |
| `environment_outcome` | Terminal outcome, expected retry, expected error codes, truncation disclosed, no execution where none is allowed, SQL shown on refusal | — |

A trial **passes** when no score fails. It is **inconclusive** when the case plants a fault
the agent never reached (for example it refused before the faulted call), or when the run
ended in an infrastructure error rather than a decision: any failed model call in the trace
(request error, token-limit stop, unparseable output; I26), an MCP session failure, or the
loop's catch-all. A crash is not a correct refusal (I23). Inconclusive
trials are excluded from rates and counted separately.

**Result equivalence** ignores column names, allows extra agent columns, ignores row order
unless the case is `ordered`, parses numeric strings, treats `2026-08-15 00:00:00` as
`2026-08-15`, and compares floats with `math.isclose(rel_tol=1e-3, abs_tol=0.01)` unless the
case overrides them.

## Grader limitations (known false positives / negatives)

Pinned by the `test_known_*` tests in `ai_agent/tests/test_eval_grading.py`, so changing one
is a deliberate act.

| Kind | Case | Why it happens | Mitigation |
|------|------|----------------|------------|
| False negative | Agent returns symbols when the reference returns names | Equivalence is on values, and `BTC` ≠ `Bitcoin` | Questions say which identifier they want ("give their names") |
| False negative | Agent rounds a percentage in SQL (`2.4567` → `2.5`) | Difference exceeds the 0.01 absolute tolerance | Per-case tolerances; review failing trials before trusting a drop |
| False positive | A 0.05% numeric error | Inside the 0.1% relative tolerance | Accepted: below what any question here cares about |
| False positive | Right rows, wrong prose ("it was $12") | `business_answer` grades the executed result, not the sentence | Advisory `answer_must_mention`; reading the answer remains human work |
| False positive | Truncated-subset cases pass on any 100 reference rows | Containment cannot tell which 100 rows a user needed | Those cases also require the truncation caveat |
| False negative | A truly equivalent result whose rounding straddles the 6th significant digit (truncated cases only) | Subset check rounds instead of using a tolerance | Rare; inspect before acting |
| Blind spot | The oracle refuses refusal cases without faults at PLAN | It knows the answer | Oracle runs prove harness wiring, never agent quality |
| False positive | A retry case passes on the fast profile | A planted execute error fails on the client and charges no budget; a real one is charged before Trino fails (I25) | Read retry passes as "the loop recovers", not "the budget fits" |
| Blind spot | `stale_docs_column` cases are always inconclusive since D045 | PLAN sees live columns, refuses correctly before EXPLORE, and never reads the stale docs | Correct behaviour, but the two cases no longer exercise docs-vs-schema; they need a question PLAN cannot settle from column names |

## Results

Fast profile, golden set `2026-11-26.1`. `agent-loop-v1` → `agent-loop-v3` (D045, I24):

| Split | Pass rate | Execution accuracy | Over-refusal | Report |
|-------|-----------|--------------------|--------------|--------|
| dev (×1) | 92% → 100% | 86% → 100% | 14% → 0% | `reports/before-after-dev.md` |
| held-out (×2) | 84% → 100% | 72% → 100% | 28% → 0% | `reports/before-after-heldout.md` |

The held-out "after" run is **partial**: the API credit balance ran out with four trials
left, and those four are inconclusive (infrastructure), so its rates cover 28 conclusive
trials, not 34. The intermediate `agent-loop-v2` run is kept (`v2-*`, `compare-v1-v2-*`)
because it improved the average while turning one dev case from 3/3 to 0/3 (`v*-top5-repeat`).
Both splits are small; a 100% here means "no failure this set can show", not "correct". These runs were
graded with grader-v2; none of their refusals came from the provider failures I26 added, so
grader-v3 would not change them, but `compare` will report them as not comparable with
newer runs.

## Files

| File | Role |
|------|------|
| `cases.py` | Case schema, loader, version/hash, split selection |
| `faults.py` | Client-side fault injection and the in-memory trace sink |
| `grading.py` | Result equivalence, trace facts, the five graders |
| `oracle.py` | No-API provider that replays reference/probe SQL |
| `runner.py` | Pins, preflight, trial loop, fresh MCP process |
| `report.py` | Metrics, markdown, before/after comparison |
| `reports/` | Committed run reports (scores, SQL and refusal reasons; no rows or prompts) |
