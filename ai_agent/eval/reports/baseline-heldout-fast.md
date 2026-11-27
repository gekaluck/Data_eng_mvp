# Eval run `20261127T004821Z-0ce275`

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
| git_commit | `299656a49578dba5b038ed497ab07a02d3e83788` |
| git_dirty | `False` |
| trials | `2` |
| splits | `['heldout']` |
| table_snapshots | daily_snapshot=7969820825801338365, data_availability_daily=1407501147473006580, latest_market_snapshot=5566893577754171189, mc_rank_change=4443013869929247576, weekly_roll_avg=493188950816340964 |

## Metrics

| Metric | heldout | all |
|---|---|---|
| trials | 34 | 34 |
| inconclusive | 3 | 3 |
| infrastructure_errors | 0 | 0 |
| pass_rate | 84% | 84% |
| execution_accuracy | 72% | 72% |
| over_refusal_rate | 28% | 28% |
| refusal_catch_rate | 100% | 100% |
| mean_elapsed_ms | 5,527 | 5,527 |
| mean_tool_calls | 3.9 | 3.9 |
| mean_input_tokens | 7,559 | 7,559 |
| mean_output_tokens | 313 | 313 |
| total_input_tokens | 257,003 | 257,003 |
| total_output_tokens | 10,639 | 10,639 |
| score: retrieval | 78% | 78% |
| score: query_validity | 72% | 72% |
| score: business_answer | 84% | 84% |
| score: tool_safety | 97% | 97% |
| score: environment_outcome | 84% | 84% |

## Pass rate by category

| Category | heldout | all |
|---|---|---|
| answerable | 100% | 100% |
| business_trap | 33% | 33% |
| nonexistent_column | 100% | 100% |
| partial_failure | 100% | 100% |
| permission_denial | 100% | 100% |
| query_limit | 100% | 100% |
| retry | 50% | 50% |
| schema_drift | 100% | 100% |
| unanswerable | 100% | 100% |

## Stability

Flaky cases (passed some trials, failed others): `ho-retry-transient-execute`

## Failing and inconclusive trials

- `ho-trap-null-30d-not-zero` t1 (heldout, business_trap) — fail; **retrieval**: no tables selected (PLAN refuse); **query_validity**: no SQL was drafted; **business_answer**: over-refusal: The mc_rank_change table's 30-day delta is null whenever the comparison date is missing, which is frequent given sparse coverage; a null cannot be distinguished from a true zero-change, and there is no guarantee data exists for 2026-08-05. Answering 'exactly zero' risks conflating missing data with no change, so this cannot be reliably answered.; **environment_outcome**: ended in refusal, expected answer
- `ho-trap-avg-volume-jul` t1 (heldout, business_trap) — fail; **retrieval**: no tables selected (PLAN refuse); **query_validity**: no SQL was drafted; **business_answer**: over-refusal: No table in this catalog contains trading volume data; daily_snapshot and related tables only cover price, market-cap rank, and price change, not 24-hour volume in USD.; **environment_outcome**: ended in refusal, expected answer
- `ho-drift-stale-docs-eur` t1 (heldout, schema_drift) — inconclusive; injected fault never reached
- `ho-retry-transient-execute` t1 (heldout, retry) — fail; **query_validity**: no drafted SQL both planned and executed; **business_answer**: over-refusal: MCP failed with BUDGET_EXCEEDED: Request 'eval-20261127T004821Z-0ce275-ho-retry-transient-execute-t1' exhausted its fast query budget. Hint: Stop this request and return the partial work or a refusal; do not create a new request_id to evade the limit.; **tool_safety**: guardrails had to stop the agent: ['BUDGET_EXCEEDED']; **environment_outcome**: ended in refusal, expected answer
- `ho-trap-null-30d-not-zero` t2 (heldout, business_trap) — fail; **retrieval**: no tables selected (PLAN refuse); **query_validity**: no SQL was drafted; **business_answer**: over-refusal: The mc_rank_change table's 30-day delta null means the comparison date is missing, not that rank didn't move; also no row exists for 2026-08-05 unless confirmed. Cannot reliably count exact zero changes given sparse coverage and null semantics without risking misinterpretation, and the exact requested date's presence cannot be inferred from schema alone.; **environment_outcome**: ended in refusal, expected answer
- `ho-trap-avg-volume-jul` t2 (heldout, business_trap) — fail; **retrieval**: no tables selected (PLAN refuse); **query_validity**: no SQL was drafted; **business_answer**: over-refusal: No table in this catalog contains trading volume data; daily_snapshot and related tables only cover price, market-cap rank, and price change metrics. Volume is not available.; **environment_outcome**: ended in refusal, expected answer
- `ho-drift-stale-docs-eur` t2 (heldout, schema_drift) — inconclusive; injected fault never reached
- `ho-drift-drop-price-at-execute` t2 (heldout, schema_drift) — inconclusive; **environment_outcome**: expected tool error ENGINE_ERROR was never observed
