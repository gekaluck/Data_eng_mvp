# Eval run `20261127T013136Z-f21209`

## Pins

| Pin | Value |
|-----|-------|
| profile | `fast` |
| model_id | `claude-sonnet-5` |
| prompt_version | `agent-loop-v3` |
| golden_set_version | `2026-11-26.1` |
| golden_set_sha256 | `ff0948cb0220` |
| grader_version | `grader-v2` |
| allow_list_sha256 | `9e51daa84342` |
| git_commit | `f32b888a286138463f8932b9a6f82701b490eabf` |
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
| pass_rate | 100% | 100% |
| execution_accuracy | 100% | 100% |
| over_refusal_rate | 0% | 0% |
| refusal_catch_rate | 100% | 100% |
| mean_elapsed_ms | 6,523 | 6,523 |
| mean_tool_calls | 8.6 | 8.6 |
| mean_input_tokens | 8,843 | 8,843 |
| mean_output_tokens | 316 | 316 |
| total_input_tokens | 229,924 | 229,924 |
| total_output_tokens | 8,215 | 8,215 |
| score: retrieval | 100% | 100% |
| score: query_validity | 100% | 100% |
| score: business_answer | 100% | 100% |
| score: tool_safety | 100% | 100% |
| score: environment_outcome | 100% | 100% |

## Pass rate by category

| Category | dev | all |
|---|---|---|
| answerable | 100% | 100% |
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

- `drift-stale-docs-24h-high` t1 (dev, schema_drift) — inconclusive; injected fault never reached
