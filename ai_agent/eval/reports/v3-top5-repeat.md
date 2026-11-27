# Eval run `20261127T010404Z-998aef`

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
| git_commit | `25f7d429743237c12502f0a0a90cdd96ce920787` |
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
| mean_elapsed_ms | 9,198 | 9,198 |
| mean_tool_calls | 10 | 10 |
| mean_input_tokens | 12,938 | 12,938 |
| mean_output_tokens | 490 | 490 |
| total_input_tokens | 38,814 | 38,814 |
| total_output_tokens | 1,470 | 1,470 |
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
