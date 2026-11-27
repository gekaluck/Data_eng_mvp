# Before / after

Before `20261127T004821Z-0ce275` → after `20261127T013436Z-0cac6b`. Changed pins: `prompt_version`, `git_commit`.

> **Partial run:** after lost 4 trial(s) to infrastructure errors. Rates cover conclusive trials only.

| Metric | heldout before | heldout after | all before | all after |
|---|---|---|---|---|
| trials | 34 | 34 | 34 | 34 |
| inconclusive | 3 | 6 | 3 | 6 |
| infrastructure_errors | 0 | 4 | 0 | 4 |
| pass_rate | 84% | 100% | 84% | 100% |
| execution_accuracy | 72% | 100% | 72% | 100% |
| over_refusal_rate | 28% | 0% | 28% | 0% |
| refusal_catch_rate | 100% | 100% | 100% | 100% |
| score: retrieval | 78% | 100% | 78% | 100% |
| score: query_validity | 72% | 100% | 72% | 100% |
| score: business_answer | 84% | 100% | 84% | 100% |
| score: tool_safety | 97% | 100% | 97% | 100% |
| score: environment_outcome | 84% | 100% | 84% | 100% |
| mean_elapsed_ms | 5,527 | 6,018 | 5,527 | 6,018 |
| mean_input_tokens | 7,559 | 7,880 | 7,559 | 7,880 |

## Per-case pass fraction changes

| Case | Before | After |
|---|---|---|
| `ho-retry-transient-execute` | 50% | 100% |
| `ho-trap-avg-volume-jul` | 0% | 100% |
| `ho-trap-null-30d-not-zero` | 0% | 100% |
