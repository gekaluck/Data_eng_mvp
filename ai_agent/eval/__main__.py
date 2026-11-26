"""CLI: `python -m ai_agent.eval {check,run,report,compare}`.

    check    validate both golden-set files offline (no stack, no API)
    run      run cases through the agent loop against a live MCP/Trino stack
    report   re-render a saved run's markdown
    compare  write a before/after markdown for two saved runs
"""

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from ai_agent.eval.cases import DEFAULT_CASES_DIR, SPLITS, load_golden_set

DEFAULT_REPORTS_DIR = Path("ai_agent/eval/reports")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m ai_agent.eval")
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check", help="Validate the golden set offline.")
    check.add_argument("--cases-dir", default=str(DEFAULT_CASES_DIR))

    run = sub.add_parser("run", help="Run cases against the live stack.")
    run.add_argument("--cases-dir", default=str(DEFAULT_CASES_DIR))
    run.add_argument("--profile", choices=("fast", "thorough"), default="fast")
    run.add_argument("--trials", type=int, default=1)
    run.add_argument("--split", action="append", choices=SPLITS, dest="splits")
    run.add_argument("--case", action="append", default=[], dest="case_ids")
    run.add_argument(
        "--provider",
        choices=("oracle", "claude"),
        default="oracle",
        help="oracle (default) replays reference SQL with no API spend; claude calls the pinned models.",
    )
    run.add_argument("--mcp-url", help="Use a running MCP server instead of starting a fresh one.")
    run.add_argument("--name", help="Report file stem (default: run id).")
    run.add_argument("--out-dir", default=str(DEFAULT_REPORTS_DIR))

    report = sub.add_parser("report", help="Render markdown for a saved run.")
    report.add_argument("run_json")

    compare = sub.add_parser("compare", help="Before/after markdown for two saved runs.")
    compare.add_argument("before_json")
    compare.add_argument("after_json")
    compare.add_argument("--out", required=True)
    compare.add_argument("--title", default="Before / after")
    return parser


def _check(cases_dir: str) -> int:
    golden = load_golden_set(cases_dir)
    counts: dict[tuple[str, str], int] = {}
    for case in golden.cases:
        counts[(case.split, case.category)] = counts.get((case.split, case.category), 0) + 1
    print(f"golden set {golden.version} sha256={golden.content_sha256[:12]} cases={len(golden.cases)}")
    for (split, category), count in sorted(counts.items()):
        print(f"  {split:8} {category:20} {count}")
    return 0


async def _run(args: argparse.Namespace) -> int:
    from ai_agent.agent_service.mcp_tools import McpHttpGateway
    from ai_agent.eval.oracle import OracleProvider
    from ai_agent.eval.report import render_run
    from ai_agent.eval.runner import local_mcp_server, run_cases, write_run

    if args.trials < 1:
        raise SystemExit("--trials must be at least 1")
    golden = load_golden_set(args.cases_dir)
    splits = tuple(args.splits or SPLITS)
    cases = golden.select(splits=splits, case_ids=tuple(args.case_ids))
    if args.provider == "claude":
        from ai_agent.agent_service.provider import AnthropicStructuredLlm

        provider = AnthropicStructuredLlm.from_env()
    else:
        provider = OracleProvider(golden.cases)

    async def execute(url: str):
        return await run_cases(
            golden,
            cases,
            provider=provider,
            gateway=McpHttpGateway(url),
            profile=args.profile,
            trials=args.trials,
            splits=splits,
            progress=lambda line: print(line, file=sys.stderr, flush=True),
        )

    if args.mcp_url:
        run = await execute(args.mcp_url)
    else:
        async with local_mcp_server() as url:
            run = await execute(url)

    stem = args.name or run.pins.run_id
    out_dir = Path(args.out_dir)
    json_path = write_run(run, out_dir / f"{stem}.json")
    md_path = out_dir / f"{stem}.md"
    md_path.write_text(render_run(run.model_dump(mode="json")), encoding="utf-8")
    print(f"wrote {json_path} and {md_path}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "check":
        return _check(args.cases_dir)
    if args.command == "run":
        return asyncio.run(_run(args))

    from ai_agent.eval.report import render_comparison, render_run

    if args.command == "report":
        print(render_run(json.loads(Path(args.run_json).read_text(encoding="utf-8"))))
        return 0
    before = json.loads(Path(args.before_json).read_text(encoding="utf-8"))
    after = json.loads(Path(args.after_json).read_text(encoding="utf-8"))
    Path(args.out).write_text(render_comparison(before, after, title=args.title), encoding="utf-8")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
