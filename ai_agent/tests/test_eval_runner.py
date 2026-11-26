"""Offline end-to-end eval runs: real AgentLoop, oracle provider, canned MCP tools."""

import asyncio

from ai_agent.eval.cases import GoldenCase, GoldenSet, load_golden_set
from ai_agent.eval.faults import FaultInjectingGateway
from ai_agent.eval.oracle import OracleProvider, reference_columns, reference_tables
from ai_agent.eval.report import render_comparison, render_run, summarize_run
from ai_agent.eval.runner import allow_list_sha256, run_cases
from ai_agent.tests.eval_fixtures import TABLE, CannedGateway, CannedTools, executed

PRICE_SQL = f"SELECT price_usd FROM {TABLE} WHERE coin_id = 'bitcoin'"
COUNT_SQL = f"SELECT count(*) AS coins FROM {TABLE}"


def _case(case_id, **fields):
    base = {"id": case_id, "split": "dev", "category": "answerable", "intent": "test", "expected_outcome": "answer"}
    base.update(fields)
    return GoldenCase.model_validate(base)


CASES = (
    _case("price", question="Bitcoin price?", reference_sql=PRICE_SQL, acceptable_tables=(TABLE,)),
    _case(
        "price-retry",
        split="heldout",
        category="retry",
        question="Bitcoin price, flaky engine?",
        reference_sql=PRICE_SQL,
        acceptable_tables=(TABLE,),
        expect_retry=True,
        fault={"kind": "tool_error", "tool": "execute_query", "code": "ENGINE_ERROR", "message": "worker down", "retryable": True},
    ),
    _case("refuse", category="unanswerable", question="Which exchange?", expected_outcome="refusal"),
    _case(
        "drift",
        category="schema_drift",
        question="Bitcoin price, column dropped?",
        expected_outcome="refusal",
        expect_error_codes=("ENGINE_ERROR",),
        fault={"kind": "drop_column_at_execute", "table": TABLE, "column": "price_usd"},
        probe_sql=PRICE_SQL,
    ),
    _case("broken", question="Count on a gap day?", reference_sql=COUNT_SQL, acceptable_tables=(TABLE,)),
)
GOLDEN = GoldenSet(version="2026-11-26.1", content_sha256="0" * 64, cases=CASES)


def _tools():
    return CannedTools(
        {
            PRICE_SQL: executed(("price_usd",), ((65000.5,),)),
            COUNT_SQL: executed(("coins",), ()),  # empty reference -> preflight skip
        }
    )


def _run(provider, tools, trials=1):
    return asyncio.run(
        run_cases(
            GOLDEN,
            CASES,
            provider=provider,
            gateway=CannedGateway(tools),
            profile="fast",
            trials=trials,
            splits=("dev", "heldout"),
            progress=lambda line: None,
        )
    )


def test_checked_in_golden_set_loads_and_every_question_is_unique():
    golden = load_golden_set()
    assert len(golden.cases) >= 30
    assert len({case.question for case in golden.cases}) == len(golden.cases)
    assert {case.split for case in golden.cases} == {"dev", "heldout"}
    for case in golden.cases:
        if case.reference_sql:
            assert reference_columns(case.reference_sql)
            assert set(reference_tables(case.reference_sql)) <= set(case.acceptable_tables)


def test_oracle_run_scores_every_conclusive_case_and_pins_the_run():
    run = _run(OracleProvider(CASES), _tools(), trials=2)
    assert [f.case_id for f in run.preflight_failures] == ["broken"]
    verdicts = {(t.case_id, t.trial): t.verdict for t in run.trials}
    assert set(verdicts.values()) == {"pass"}, run.trials
    assert len(run.trials) == 8
    retry = next(t for t in run.trials if t.case_id == "price-retry")
    assert retry.retries == 1 and retry.injected_faults
    pins = run.pins
    assert pins.model_id.startswith("oracle-no-api/")
    assert pins.table_snapshots == {TABLE: 42}
    assert pins.allow_list_sha256 == allow_list_sha256()
    assert pins.golden_set_version == "2026-11-26.1"


class WrongNumberTools(CannedTools):
    """Agent SQL runs fine but returns a different number than the reference did."""

    def __init__(self):
        super().__init__({PRICE_SQL: executed(("price_usd",), ((65000.5,),))})
        self.agent_calls = 0

    async def execute_query(self, sql, *, request_id, profile, max_rows=100):
        if request_id.startswith("eval-ref-"):
            return await super().execute_query(sql, request_id=request_id, profile=profile, max_rows=max_rows)
        return executed(("price_usd",), ((1.0,),))


def test_valid_sql_with_wrong_result_fails_business_answer_only():
    run = _run(OracleProvider(CASES), WrongNumberTools())
    price = next(t for t in run.trials if t.case_id == "price")
    assert price.verdict == "fail"
    assert [n for n, d in price.scores.items() if d.score == "fail"] == ["business_answer"]


def test_report_and_comparison_render():
    run = _run(OracleProvider(CASES), _tools()).model_dump(mode="json")
    summary = summarize_run(run)
    assert summary["all"]["execution_accuracy"] == 1.0
    assert summary["heldout"]["trials"] == 1
    markdown = render_run(run)
    assert "## Pins" in markdown and "broken" in markdown
    worse = _run(OracleProvider(CASES), WrongNumberTools()).model_dump(mode="json")
    comparison = render_comparison(worse, run)
    assert "| `price` |" in comparison


def test_fault_gateway_passes_through_without_a_fault():
    tools = _tools()
    gateway = FaultInjectingGateway(CannedGateway(tools), None)

    async def go():
        async with gateway.session() as session:
            return session

    assert asyncio.run(go()) is tools


def test_all_zero_reference_is_skipped_unless_the_case_allows_it():
    zero_sql = f"SELECT count(*) AS n FROM {TABLE} WHERE snapshot_date = DATE '2099-01-01'"
    tools = CannedTools({zero_sql: executed(("n",), ((0,),))})
    strict = _case("zero", question="Count far future?", reference_sql=zero_sql, acceptable_tables=(TABLE,))
    allowed = _case("zero-ok", question="Count of none?", reference_sql=zero_sql, acceptable_tables=(TABLE,), allow_degenerate_reference=True)

    from ai_agent.eval.runner import reference_results

    references, failures = asyncio.run(reference_results(CannedGateway(tools), (strict, allowed), run_id="r"))
    assert [f.case_id for f in failures] == ["zero"]
    assert set(references) == {"zero-ok"}


# Local Gold coverage when the set was written (I22). A case dated outside it measures
# the pipeline's lag, not the agent. Move the upper bound only after Gold has caught up.
LOCAL_GOLD_COVERAGE = ("2026-01-14", "2026-08-10")


def test_case_dates_stay_inside_local_gold_coverage():
    import re

    first, last = LOCAL_GOLD_COVERAGE
    for case in load_golden_set().cases:
        text = " ".join(filter(None, (case.question, case.reference_sql, case.probe_sql)))
        for found in re.findall(r"\d{4}-\d{2}-\d{2}", text):
            assert first <= found <= last, (case.id, found)
