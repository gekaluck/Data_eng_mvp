# Eval run `20261127T010252Z-e26339`

## Pins

| Pin | Value |
|-----|-------|
| profile | `fast` |
| model_id | `claude-sonnet-5` |
| prompt_version | `agent-loop-v1` |
| golden_set_version | `2026-11-26.1` |
| golden_set_sha256 | `ff0948cb0220` |
| grader_version | `grader-v2` |
| allow_list_sha256 | `9e51daa84342` |
| git_commit | `fc1144f79907512da8fafc616e7b411e9f198885` |
| git_dirty | `False` |
| trials | `3` |
| splits | `['dev', 'heldout']` |
| table_snapshots | daily_snapshot=7969820825801338365, data_availability_daily=1407501147473006580, latest_market_snapshot=5566893577754171189, mc_rank_change=4443013869929247576, weekly_roll_avg=493188950816340964 |

## Metrics

| Metric | dev | all |
|---|---|---|
| trials | 3 | 3 |
| inconclusive | 0 | 0 |
| infrastructure_errors | 0 | 0 |
| pass_rate | 100% | 100% |
| execution_accuracy | 100% | 100% |
| over_refusal_rate | 0% | 0% |
| refusal_catch_rate | — | — |
| mean_elapsed_ms | 9,922 | 9,922 |
| mean_tool_calls | 6.3 | 6.3 |
| mean_input_tokens | 12,541 | 12,541 |
| mean_output_tokens | 669 | 669 |
| total_input_tokens | 37,623 | 37,623 |
| total_output_tokens | 2,006 | 2,006 |
| score: retrieval | 100% | 100% |
| score: query_validity | 100% | 100% |
| score: business_answer | 100% | 100% |
| score: tool_safety | 100% | 100% |
| score: environment_outcome | 100% | 100% |

## Pass rate by category

| Category | dev | all |
|---|---|---|
| answerable | 100% | 100% |

## Stability

Flaky cases (passed some trials, failed others): none

## Failing and inconclusive trials

None.
