# Before / after

Before `20261127T004342Z-7e6335` → after `20261127T005426Z-52f72a`. Changed pins: `prompt_version`, `git_commit`.

| Metric | dev before | dev after | all before | all after |
|---|---|---|---|---|
| pass_rate | 92% | 96% | 92% | 96% |
| execution_accuracy | 86% | 93% | 86% | 93% |
| over_refusal_rate | 14% | 7% | 14% | 7% |
| refusal_catch_rate | 100% | 100% | 100% | 100% |
| score: retrieval | 93% | 93% | 93% | 93% |
| score: query_validity | 86% | 93% | 86% | 93% |
| score: business_answer | 92% | 96% | 92% | 96% |
| score: tool_safety | 96% | 100% | 96% | 100% |
| score: environment_outcome | 92% | 96% | 92% | 96% |
| mean_elapsed_ms | 5,597 | 6,239 | 5,597 | 6,239 |
| mean_input_tokens | 7,530 | 8,413 | 7,530 | 8,413 |

## Per-case pass fraction changes

| Case | Before | After |
|---|---|---|
| `ans-top5-mcap-names` | 100% | 0% |
| `drift-drop-mcap-at-execute` | — | 100% |
| `retry-transient-execute` | 0% | 100% |
| `trap-7-calendar-days-gap` | 0% | 100% |
