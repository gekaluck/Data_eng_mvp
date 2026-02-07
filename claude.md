# CODE.md — Agent Operating Rules

This file defines **how the agent must operate** while working in this repository.
It is intentionally short and procedural.

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
After completing a task or milestone:
- Add a short description of:
  - what was built
  - why it was built this way
  - what to try or improve next
- Keep explanations concise and practical.

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

## 8 
When creating pr or preparing to create one when finishing a feature always check the context md files in repo to updates like architecture.md and decisions.md and other relevant ones

