# Eval run `20261127T005718Z-67e68c`

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
| trials | `2` |
| splits | `['heldout']` |
| table_snapshots | daily_snapshot=7969820825801338365, data_availability_daily=1407501147473006580, latest_market_snapshot=5566893577754171189, mc_rank_change=4443013869929247576, weekly_roll_avg=493188950816340964 |

## Metrics

| Metric | heldout | all |
|---|---|---|
| trials | 34 | 34 |
| inconclusive | 2 | 2 |
| infrastructure_errors | 0 | 0 |
| pass_rate | 94% | 94% |
| execution_accuracy | 89% | 89% |
| over_refusal_rate | 11% | 11% |
| refusal_catch_rate | 100% | 100% |
| mean_elapsed_ms | 6,517 | 6,517 |
| mean_tool_calls | 8.7 | 8.7 |
| mean_input_tokens | 9,001 | 9,001 |
| mean_output_tokens | 331 | 331 |
| total_input_tokens | 306,050 | 306,050 |
| total_output_tokens | 11,255 | 11,255 |
| score: retrieval | 89% | 89% |
| score: query_validity | 89% | 89% |
| score: business_answer | 94% | 94% |
| score: tool_safety | 100% | 100% |
| score: environment_outcome | 94% | 94% |

## Pass rate by category

| Category | heldout | all |
|---|---|---|
| answerable | 100% | 100% |
| business_trap | 67% | 67% |
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

- `ho-trap-null-30d-not-zero` t1 (heldout, business_trap) — fail; **retrieval**: no tables selected (PLAN refuse); **query_validity**: no SQL was drafted; **business_answer**: over-refusal: mc_rank_change requires exact calendar date 2026-08-05 minus 30 days to exist in sparse coverage; also no data confirmed present for 2026-08-05 itself. Cannot verify without risking null-as-stability misinterpretation; also catalog has no confirmed row for that exact date.; **environment_outcome**: ended in refusal, expected answer
- `ho-drift-stale-docs-eur` t1 (heldout, schema_drift) — inconclusive; injected fault never reached
- `ho-trap-null-30d-not-zero` t2 (heldout, business_trap) — fail; **retrieval**: no tables selected (PLAN refuse); **query_validity**: no SQL was drafted; **business_answer**: over-refusal: The mc_rank_change table's mc_rank_diff_30d is null whenever the exact comparison date is missing from sparse coverage, and nulls mean missing data, not zero change. Also no data exists confirmed for 2026-08-05 specifically. Cannot reliably count coins with exactly zero change without risking conflating missing data with stability.; **environment_outcome**: ended in refusal, expected answer
- `ho-drift-stale-docs-eur` t2 (heldout, schema_drift) — inconclusive; injected fault never reached
