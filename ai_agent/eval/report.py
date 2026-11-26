"""Turn graded trials into the §6 metrics, a markdown report, and before/after deltas."""

from collections import defaultdict
from statistics import mean
from typing import Any

from ai_agent.eval.grading import SCORE_NAMES

_PIN_KEYS_THAT_MUST_MATCH = ("golden_set_sha256", "grader_version", "allow_list_sha256", "table_snapshots")


def _rate(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 3) if denominator else None


def summarize(trials: list[dict[str, Any]]) -> dict[str, Any]:
    """Metrics for one group of trials (a split, or everything)."""
    conclusive = [t for t in trials if t["verdict"] != "inconclusive"]
    answerable = [t for t in conclusive if t["expected_outcome"] == "answer"]
    unanswerable = [t for t in conclusive if t["expected_outcome"] == "refusal"]

    scores = {}
    for name in SCORE_NAMES:
        applicable = [t for t in conclusive if t["scores"][name]["score"] != "n/a"]
        passed = sum(t["scores"][name]["score"] == "pass" for t in applicable)
        scores[name] = _rate(passed, len(applicable))

    by_category: dict[str, list[dict]] = defaultdict(list)
    for trial in conclusive:
        by_category[trial["category"]].append(trial)

    by_case: dict[str, list[str]] = defaultdict(list)
    for trial in trials:
        by_case[trial["case_id"]].append(trial["verdict"])
    stability = {
        case_id: _rate(sum(v == "pass" for v in verdicts), len([v for v in verdicts if v != "inconclusive"]))
        for case_id, verdicts in sorted(by_case.items())
    }

    return {
        "trials": len(trials),
        "inconclusive": len(trials) - len(conclusive),
        "infrastructure_errors": sum(bool(t.get("infrastructure_error")) for t in trials),
        "pass_rate": _rate(sum(t["verdict"] == "pass" for t in conclusive), len(conclusive)),
        "execution_accuracy": _rate(
            sum(t["scores"]["business_answer"]["score"] == "pass" for t in answerable), len(answerable)
        ),
        "over_refusal_rate": _rate(sum(t["outcome"] == "refusal" for t in answerable), len(answerable)),
        "refusal_catch_rate": _rate(sum(t["outcome"] == "refusal" for t in unanswerable), len(unanswerable)),
        "scores": scores,
        "by_category": {
            category: _rate(sum(t["verdict"] == "pass" for t in group), len(group))
            for category, group in sorted(by_category.items())
        },
        "case_pass_fraction": stability,
        "flaky_cases": sorted(c for c, rate in stability.items() if rate is not None and 0 < rate < 1),
        "mean_elapsed_ms": round(mean(t["elapsed_ms"] for t in trials)) if trials else None,
        "mean_tool_calls": round(mean(t["tool_calls"] for t in trials), 1) if trials else None,
        "mean_input_tokens": round(mean(t["input_tokens"] for t in trials)) if trials else None,
        "mean_output_tokens": round(mean(t["output_tokens"] for t in trials)) if trials else None,
        "total_input_tokens": sum(t["input_tokens"] for t in trials),
        "total_output_tokens": sum(t["output_tokens"] for t in trials),
    }


def summarize_run(run: dict[str, Any]) -> dict[str, Any]:
    trials = list(run["trials"])
    splits = sorted({t["split"] for t in trials})
    return {
        "all": summarize(trials),
        **{split: summarize([t for t in trials if t["split"] == split]) for split in splits},
    }


def _fmt(value: Any, *, pct: bool = True) -> str:
    """Rates (0..1 floats) render as percentages; counts and means as numbers."""
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:.0%}" if pct else f"{value:,.1f}"
    return f"{value:,}" if isinstance(value, int) else str(value)


_NUMERIC_METRICS = frozenset({"mean_tool_calls"})


def render_run(run: dict[str, Any]) -> str:
    pins = run["pins"]
    summary = summarize_run(run)
    lines = [
        f"# Eval run `{pins['run_id']}`",
        "",
        "## Pins",
        "",
        "| Pin | Value |",
        "|-----|-------|",
    ]
    for key in (
        "profile", "model_id", "prompt_version", "golden_set_version", "golden_set_sha256",
        "grader_version", "allow_list_sha256", "git_commit", "git_dirty", "trials", "splits",
    ):
        value = pins[key]
        if isinstance(value, str) and len(value) == 64:
            value = value[:12]
        lines.append(f"| {key} | `{value}` |")
    snapshots = ", ".join(f"{t.split('.')[-1]}={s}" for t, s in sorted(pins["table_snapshots"].items()))
    lines += [f"| table_snapshots | {snapshots} |", ""]

    if run["preflight_failures"]:
        lines += ["## Preflight skips (broken cases, not scored)", ""]
        lines += [f"- `{f['case_id']}`: {f['reason']}" for f in run["preflight_failures"]]
        lines.append("")

    groups = [name for name in summary if name != "all"] + ["all"]
    lines += ["## Metrics", "", "| Metric | " + " | ".join(groups) + " |", "|---" * (len(groups) + 1) + "|"]
    for metric in ("trials", "inconclusive", "infrastructure_errors", "pass_rate", "execution_accuracy", "over_refusal_rate",
                   "refusal_catch_rate", "mean_elapsed_ms", "mean_tool_calls", "mean_input_tokens",
                   "mean_output_tokens", "total_input_tokens", "total_output_tokens"):
        lines.append(f"| {metric} | " + " | ".join(_fmt(summary[g][metric], pct=metric not in _NUMERIC_METRICS) for g in groups) + " |")
    for name in SCORE_NAMES:
        lines.append(f"| score: {name} | " + " | ".join(_fmt(summary[g]["scores"][name]) for g in groups) + " |")
    lines.append("")

    lines += ["## Pass rate by category", "", "| Category | " + " | ".join(groups) + " |", "|---" * (len(groups) + 1) + "|"]
    categories = sorted({c for g in groups for c in summary[g]["by_category"]})
    for category in categories:
        lines.append(f"| {category} | " + " | ".join(_fmt(summary[g]["by_category"].get(category)) for g in groups) + " |")
    lines.append("")

    flaky = summary["all"]["flaky_cases"]
    lines += ["## Stability", "", f"Flaky cases (passed some trials, failed others): {', '.join(f'`{c}`' for c in flaky) or 'none'}", ""]

    lines += ["## Failing and inconclusive trials", ""]
    bad = [t for t in run["trials"] if t["verdict"] != "pass"]
    if not bad:
        lines.append("None.")
    for trial in bad:
        failed = [f"**{name}**: {detail['reason']}" for name, detail in trial["scores"].items() if detail["score"] == "fail"]
        lines.append(f"- `{trial['case_id']}` t{trial['trial']} ({trial['split']}, {trial['category']}) — "
                     f"{trial['verdict']}; " + (f"infrastructure error: {trial['infrastructure_error']}" if trial.get("infrastructure_error")
                        else "; ".join(failed) or "injected fault never reached"))
    lines.append("")
    return "\n".join(lines)


def comparability_problems(before: dict[str, Any], after: dict[str, Any]) -> list[str]:
    problems = []
    for key in _PIN_KEYS_THAT_MUST_MATCH:
        if before["pins"][key] != after["pins"][key]:
            problems.append(f"`{key}` differs between runs")
    if before["pins"]["profile"] != after["pins"]["profile"]:
        problems.append("profiles differ")
    return problems


def render_comparison(before: dict[str, Any], after: dict[str, Any], *, title: str = "Before / after") -> str:
    b, a = summarize_run(before), summarize_run(after)
    lines = [f"# {title}", ""]
    problems = comparability_problems(before, after)
    if problems:
        lines += ["> **Not directly comparable:** " + "; ".join(problems), ""]
    changed = [k for k in ("model_id", "prompt_version", "git_commit") if before["pins"][k] != after["pins"][k]]
    lines += [
        (f"Before `{before['pins']['run_id']}` → after `{after['pins']['run_id']}`. "
        f"Changed pins: {', '.join(f'`{k}`' for k in changed) or 'none'}."),
        "",
    ]
    groups = [g for g in ("dev", "heldout", "all") if g in b and g in a]
    lines += ["| Metric | " + " | ".join(f"{g} before | {g} after" for g in groups) + " |",
              "|---" * (2 * len(groups) + 1) + "|"]
    metrics = ["pass_rate", "execution_accuracy", "over_refusal_rate", "refusal_catch_rate"]
    for metric in metrics:
        lines.append(f"| {metric} | " + " | ".join(f"{_fmt(b[g][metric])} | {_fmt(a[g][metric])}" for g in groups) + " |")
    for name in SCORE_NAMES:
        lines.append(f"| score: {name} | " + " | ".join(
            f"{_fmt(b[g]['scores'][name])} | {_fmt(a[g]['scores'][name])}" for g in groups) + " |")
    for metric in ("mean_elapsed_ms", "mean_input_tokens"):
        lines.append(f"| {metric} | " + " | ".join(f"{_fmt(b[g][metric])} | {_fmt(a[g][metric])}" for g in groups) + " |")
    lines.append("")

    lines += ["## Per-case pass fraction changes", "", "| Case | Before | After |", "|---|---|---|"]
    before_cases, after_cases = b["all"]["case_pass_fraction"], a["all"]["case_pass_fraction"]
    rows = [
        (case, before_cases.get(case), after_cases.get(case))
        for case in sorted(set(before_cases) | set(after_cases))
        if before_cases.get(case) != after_cases.get(case)
    ]
    lines += [f"| `{c}` | {_fmt(x)} | {_fmt(y)} |" for c, x, y in rows] or ["| (no changes) | | |"]
    lines.append("")
    return "\n".join(lines)
