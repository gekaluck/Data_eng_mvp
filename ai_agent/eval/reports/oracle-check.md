# Eval run `20261127T015739Z-9cf6d8`

## Pins

| Pin | Value |
|-----|-------|
| profile | `fast` |
| model_id | `oracle-no-api/claude-sonnet-5` |
| prompt_version | `agent-loop-v3` |
| golden_set_version | `2026-11-26.1` |
| golden_set_sha256 | `ff0948cb0220` |
| grader_version | `grader-v3` |
| allow_list_sha256 | `9e51daa84342` |
| git_commit | `1f55823ab4ed2b6397a59d7352de57cd8d58539c` |
| git_dirty | `False` |
| trials | `1` |
| splits | `['dev', 'heldout']` |
| table_snapshots | daily_snapshot=7969820825801338365, data_availability_daily=1407501147473006580, latest_market_snapshot=5566893577754171189, mc_rank_change=4443013869929247576, weekly_roll_avg=493188950816340964 |

## Metrics

| Metric | dev | heldout | all |
|---|---|---|---|
| trials | 26 | 17 | 43 |
| inconclusive | 0 | 0 | 0 |
| infrastructure_errors | 0 | 0 | 0 |
| pass_rate | 100% | 100% | 100% |
| execution_accuracy | 100% | 100% | 100% |
| over_refusal_rate | 0% | 0% | 0% |
| refusal_catch_rate | 100% | 100% | 100% |
| mean_elapsed_ms | 1,808 | 1,716 | 1,771 |
| mean_tool_calls | 8.8 | 9.1 | 8.9 |
| mean_input_tokens | 0 | 0 | 0 |
| mean_output_tokens | 0 | 0 | 0 |
| total_input_tokens | 0 | 0 | 0 |
| total_output_tokens | 0 | 0 | 0 |
| score: retrieval | 100% | 100% | 100% |
| score: query_validity | 100% | 100% | 100% |
| score: business_answer | 100% | 100% | 100% |
| score: tool_safety | 100% | 100% | 100% |
| score: environment_outcome | 100% | 100% | 100% |

## Pass rate by category

| Category | dev | heldout | all |
|---|---|---|---|
| answerable | 100% | 100% | 100% |
| business_trap | 100% | 100% | 100% |
| nonexistent_column | 100% | 100% | 100% |
| partial_failure | 100% | 100% | 100% |
| permission_denial | 100% | 100% | 100% |
| query_limit | 100% | 100% | 100% |
| retry | 100% | 100% | 100% |
| schema_drift | 100% | 100% | 100% |
| unanswerable | 100% | 100% | 100% |

## Stability

Flaky cases (passed some trials, failed others): none

## Failing and inconclusive trials

None.
