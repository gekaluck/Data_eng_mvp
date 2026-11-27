# Eval run `20261127T005426Z-52f72a`

## Pins

| Pin | Value |
|-----|-------|
| profile | `fast` |
| model_id | `claude-sonnet-5` |
| prompt_version | `agent-loop-v2` |
| golden_set_version | `2026-11-26.1` |
| golden_set_sha256 | `ff0948cb0220` |
| grader_version | `grader-v2` |
| allow_list_sha256 | `9e51daa84342` |
| git_commit | `42861e23fa5200bf9a5fff153587d8b93446a1ab` |
| git_dirty | `False` |
| trials | `1` |
| splits | `['dev']` |
| table_snapshots | daily_snapshot=7969820825801338365, data_availability_daily=1407501147473006580, latest_market_snapshot=5566893577754171189, mc_rank_change=4443013869929247576, weekly_roll_avg=493188950816340964 |

## Metrics

| Metric | dev | all |
|---|---|---|
| trials | 26 | 26 |
| inconclusive | 1 | 1 |
| infrastructure_errors | 0 | 0 |
| pass_rate | 96% | 96% |
| execution_accuracy | 93% | 93% |
| over_refusal_rate | 7% | 7% |
| refusal_catch_rate | 100% | 100% |
| mean_elapsed_ms | 6,239 | 6,239 |
| mean_tool_calls | 8.5 | 8.5 |
| mean_input_tokens | 8,413 | 8,413 |
| mean_output_tokens | 297 | 297 |
| total_input_tokens | 218,731 | 218,731 |
| total_output_tokens | 7,730 | 7,730 |
| score: retrieval | 93% | 93% |
| score: query_validity | 93% | 93% |
| score: business_answer | 96% | 96% |
| score: tool_safety | 100% | 100% |
| score: environment_outcome | 96% | 96% |

## Pass rate by category

| Category | dev | all |
|---|---|---|
| answerable | 83% | 83% |
| business_trap | 100% | 100% |
| nonexistent_column | 100% | 100% |
| partial_failure | 100% | 100% |
| permission_denial | 100% | 100% |
| query_limit | 100% | 100% |
| retry | 100% | 100% |
| schema_drift | 100% | 100% |
| unanswerable | 100% | 100% |

## Stability

Flaky cases (passed some trials, failed others): none

## Failing and inconclusive trials

- `ans-top5-mcap-names` t1 (dev, answerable) — fail; **retrieval**: no tables selected (PLAN refuse); **query_validity**: no SQL was drafted; **business_answer**: over-refusal: Data coverage is sparse and no row for 2026-08-01 is confirmed to exist; cannot assume data for that exact date without checking, and question requires guaranteed presence which cannot be verified without a query. However since this can be answered via a SELECT against daily_snapshot filtering that date, it is better to attempt rather than refuse outright... but per policy on sparse coverage, refusing is safer given no confirmation of data for that date.; **environment_outcome**: ended in refusal, expected answer
- `drift-stale-docs-24h-high` t1 (dev, schema_drift) — inconclusive; injected fault never reached
