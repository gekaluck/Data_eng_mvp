# CLAUDE.md — Agent Operating Rules

This file defines **how the agent must operate** while working in this repository.
It is intentionally short and procedural.

This is the single source of truth for how to work here, whichever agent is reading it —
[`AGENTS.md`](AGENTS.md) exists only to point tools that look for that filename back to this
file, so rules are written once and never drift between copies.

For *what the project is* and which document is authoritative on what, start at
[`docs/README.md`](docs/README.md) — the documentation map and evolution story. Read it
before making design claims about this repo; several docs are deliberately kept as
historical records and are not descriptions of how the system works today.

---

## 1. Clarifying Questions (MANDATORY)
Before starting any non-trivial work:
- Ask clarifying questions.
- Do NOT assume requirements, scope, tools, or constraints.
- Prefer fewer, high-impact questions.
- Restate agreed requirements before proceeding.

---

## 2. Scope & Complexity
- This is a **learning-first** project.
- Avoid overengineering and unnecessary abstractions.
- Prefer explicit, readable code over cleverness.
- If unsure, choose the simplest correct solution.

---

## 3. Branch & Git Workflow
- Never commit to `main`.
- For each new feature or milestone:
  - Create a new branch: `feat/<short-name>` or `chore/<short-name>`.
  - Commit incrementally to that branch.
- After completing a feature:
  - Prepare a PR summary.
- If review feedback arrives:
  - Continue committing to the same branch.
- Before starting new work:
  - Ask whether the previous PR is merged.

---

## 4. Documentation After Action

Before opening a PR, update the docs the change actually touches. Each has a trigger — if
the trigger fired, the entry is not optional:

| Doc | Update it when | Form |
|-----|----------------|------|
| `docs/architecture.md` | The shape of the system changed — a component, a dependency, a data flow | Edit in place; it describes the present |
| `docs/decisions.md` | A choice was made that a future reader could reasonably question | New `Dxxx` entry: decision, why, alternatives, revisit-if |
| `docs/incidents.md` | **Something broke, or you found something already broken** | New `Ixx` entry: symptom, root cause, fix, lesson |
| `docs/evolution.md` | A phase of work ended, or you changed your mind about something | Narrative paragraph, in your own voice |
| `docs/runbook.md` | A new way to operate or debug the system exists | A symptom → first-checks entry |

Rules that matter:

- **`decisions.md` and `incidents.md` are append-only.** Never rewrite or renumber an
  existing entry. Reversals become a new entry that supersedes the old one
  (e.g. `D023 supersedes D020`).
- **Never reuse a D or I number.** Check the highest existing one first — parallel PRs have
  collided before and produced two `D027`s.
- **An incident is worth recording even when you fixed it in the same session.** The log is
  the project's memory of what its failure modes actually are; a bug fixed and never written
  down teaches nothing and tends to recur.
- Keep explanations concise and practical, and prefer the specific over the general: the
  number, the date, the exact wrong value.

### Which docs are authoritative

Start at [`docs/README.md`](docs/README.md) — the documentation map. Everything under
**`docs/historical/`** describes the system as it *was*: useful for reasoning, never a
statement of how anything works today. Do not cite it as current, and do not "fix" the
contradictions in it — they are the point.

When two current docs disagree: `decisions.md` (newest entry wins) → `architecture.md` →
`runbook.md` → everything else.

---

## 5. Assumptions & Safety
- Do not introduce new services, tools, or dependencies without asking.
- Do not include secrets or credentials.
- Keep the project runnable locally.
- Prefer local-first solutions unless explicitly told otherwise.

---

## 6. Learning Focus
- Code should be understandable by a human learning the stack.
- Use comments where they improve clarity.
- Explain important tradeoffs briefly when they occur.

---

## 7. If Unsure
When in doubt:
- Ask a question.
- Do not guess.

## 8. Before Opening a PR
Work through the table in §4 and update every doc whose trigger fired. Doing this at PR
time rather than later is deliberate: the reasoning, the false starts, and the exact
symptom are all still in your head, and they are the parts that cannot be reconstructed
from the diff afterwards.

