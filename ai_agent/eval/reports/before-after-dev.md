# Before / after

Before `20261127T004342Z-7e6335` → after `20261127T013136Z-f21209`. Changed pins: `prompt_version`, `git_commit`.

| Metric | dev before | dev after | all before | all after |
|---|---|---|---|---|
| trials | 26 | 26 | 26 | 26 |
| inconclusive | 2 | 1 | 2 | 1 |
| infrastructure_errors | 0 | 0 | 0 | 0 |
| pass_rate | 92% | 100% | 92% | 100% |
| execution_accuracy | 86% | 100% | 86% | 100% |
| over_refusal_rate | 14% | 0% | 14% | 0% |
| refusal_catch_rate | 100% | 100% | 100% | 100% |
| score: retrieval | 93% | 100% | 93% | 100% |
| score: query_validity | 86% | 100% | 86% | 100% |
| score: business_answer | 92% | 100% | 92% | 100% |
| score: tool_safety | 96% | 100% | 96% | 100% |
| score: environment_outcome | 92% | 100% | 92% | 100% |
| mean_elapsed_ms | 5,597 | 6,523 | 5,597 | 6,523 |
| mean_input_tokens | 7,530 | 8,843 | 7,530 | 8,843 |

## Per-case pass fraction changes

| Case | Before | After |
|---|---|---|
| `drift-drop-mcap-at-execute` | — | 100% |
| `retry-transient-execute` | 0% | 100% |
| `trap-7-calendar-days-gap` | 0% | 100% |
