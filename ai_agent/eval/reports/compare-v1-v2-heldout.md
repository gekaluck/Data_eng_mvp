# Before / after

Before `20261127T004821Z-0ce275` → after `20261127T005718Z-67e68c`. Changed pins: `prompt_version`, `git_commit`.

| Metric | heldout before | heldout after | all before | all after |
|---|---|---|---|---|
| pass_rate | 84% | 94% | 84% | 94% |
| execution_accuracy | 72% | 89% | 72% | 89% |
| over_refusal_rate | 28% | 11% | 28% | 11% |
| refusal_catch_rate | 100% | 100% | 100% | 100% |
| score: retrieval | 78% | 89% | 78% | 89% |
| score: query_validity | 72% | 89% | 72% | 89% |
| score: business_answer | 84% | 94% | 84% | 94% |
| score: tool_safety | 97% | 100% | 97% | 100% |
| score: environment_outcome | 84% | 94% | 84% | 94% |
| mean_elapsed_ms | 5,527 | 6,517 | 5,527 | 6,517 |
| mean_input_tokens | 7,559 | 9,001 | 7,559 | 9,001 |

## Per-case pass fraction changes

| Case | Before | After |
|---|---|---|
| `ho-retry-transient-execute` | 50% | 100% |
| `ho-trap-avg-volume-jul` | 0% | 100% |
