"""Grader behaviour, including the known false positives and false negatives.

The `test_known_*` tests do not assert desired behaviour: they pin a documented grader
limitation (ai_agent/eval/README.md) so that changing it is a deliberate, visible act.
"""

from ai_agent.agent_service.contracts import AgentResponse
from ai_agent.agent_service.tracing import AgentTraceEntry
from ai_agent.eval.cases import GoldenCase
from ai_agent.eval.faults import FaultLog
from ai_agent.eval.grading import (
    ReferenceResult,
    compare_results,
    grade_trial,
    is_truncated_subset,
    normalize_value,
)
from ai_agent.tests.eval_fixtures import NOW, TABLE

REF = ReferenceResult(columns=("name", "price_usd"), rows=(("Bitcoin", 65000.5), ("Ether", 3100.25)), truncated=False)


def test_normalize_value_unifies_engine_and_json_forms():
    assert normalize_value("123.50") == 123.5
    assert normalize_value(7) == 7.0
    assert normalize_value("2026-08-15 00:00:00.000") == "2026-08-15"
    assert normalize_value(" Bitcoin ") == "bitcoin"
    assert normalize_value(None) is None


def test_column_names_order_and_extra_columns_do_not_matter():
    ok, _ = compare_results(
        REF,
        ("usd", "symbol", "coin"),
        ((3100.25, "ETH", "ether"), (65000.5, "BTC", "BITCOIN")),
    )
    assert ok


def test_row_order_matters_only_when_case_is_ordered():
    rows = (("Ether", 3100.25), ("Bitcoin", 65000.5))
    assert compare_results(REF, ("a", "b"), rows)[0]
    ok, reason = compare_results(REF, ("a", "b"), rows, ordered=True)
    assert not ok and "order" in reason


def test_missing_rows_or_columns_fail():
    assert not compare_results(REF, ("a", "b"), (("Bitcoin", 65000.5),))[0]
    assert not compare_results(REF, ("a",), (("Bitcoin",), ("Ether",)))[0]


def test_percent_versus_fraction_is_caught():
    ref = ReferenceResult(columns=("f",), rows=((0.025,),), truncated=False)
    assert compare_results(ref, ("pct",), ((0.025,),), abs_tol=0.0001)[0]
    assert not compare_results(ref, ("pct",), ((2.5,),), abs_tol=0.0001)[0]


def test_both_empty_results_match():
    ref = ReferenceResult(columns=("x",), rows=(), truncated=False)
    assert compare_results(ref, ("y",), ())[0]


def test_truncated_subset():
    ref = ReferenceResult(columns=("d", "p"), rows=tuple((f"2026-08-{d:02d}", d * 1.5) for d in range(1, 31)), truncated=False)
    assert is_truncated_subset(ref, ("price", "date"), [(1.5, "2026-08-01"), (3.0, "2026-08-02")])[0]
    assert not is_truncated_subset(ref, ("d", "p"), [("2026-08-01", 99.0)])[0]


def test_known_false_negative_identifier_choice():
    # The question asked for names; an agent returning symbols is arguably right but fails.
    ok, _ = compare_results(REF, ("symbol", "price"), (("BTC", 65000.5), ("ETH", 3100.25)))
    assert not ok


def test_known_false_negative_display_rounding():
    # Rounding a percentage to one decimal exceeds the default 0.01 absolute tolerance.
    ref = ReferenceResult(columns=("pct",), rows=((2.4567,),), truncated=False)
    assert not compare_results(ref, ("pct",), ((2.5,),))[0]


def test_known_false_positive_within_tolerance():
    # A 0.05% error in a price passes the default 0.1% relative tolerance.
    ref = ReferenceResult(columns=("p",), rows=((65000.0,),), truncated=False)
    assert compare_results(ref, ("p",), ((65032.0,),))[0]


# ----------------------------------------------------------------- whole-trial grading


def _entry(seq, event, *, status="info", stage=None, metadata=None, content=None):
    return AgentTraceEntry(
        timestamp=NOW,
        sequence=seq,
        request_id="r-1",
        profile="fast",
        model_id="m",
        prompt_version="p",
        event=event,
        status=status,
        elapsed_ms=seq * 10,
        stage=stage,
        metadata=metadata or {},
        content=content,
    )


def _answer_trace(columns, rows, *, errors=(), truncated=False):
    entries = [
        _entry(1, "state.completed", stage="plan", content={"decision": {"disposition": "answer", "tables": [TABLE]}}),
        _entry(2, "llm.completed", status="succeeded", stage="draft", metadata={"input_tokens": 100, "output_tokens": 20}),
    ]
    for code in errors:
        entries.append(_entry(len(entries) + 1, "tool.completed", status="failed", metadata={"tool": "execute_query", "error_code": code}))
        entries.append(_entry(len(entries) + 1, "retry.feedback", status="failed"))
    entries += [
        _entry(len(entries) + 1, "tool.started", metadata={"tool": "explain_query"}),
        _entry(len(entries) + 2, "tool.completed", status="succeeded", metadata={"tool": "explain_query"}, content={"result": {"valid": True}}),
        _entry(len(entries) + 3, "tool.started", metadata={"tool": "execute_query"}),
        _entry(
            len(entries) + 4,
            "tool.completed",
            status="succeeded",
            metadata={"tool": "execute_query"},
            content={"result": {"columns": list(columns), "rows": [list(r) for r in rows], "truncated": truncated}},
        ),
    ]
    return entries


def _response(answer="BTC was 65000.5", refusal=None, sql="SELECT 1", tables=(TABLE,), caveats=()):
    return AgentResponse(
        answer=answer,
        refusal_reason=refusal,
        sql=sql,
        tables_used=tables,
        caveats=caveats,
        profile="fast",
        request_id="r-1",
        model_id="m",
    )


def _case(**overrides):
    base = {
        "id": "case-1",
        "split": "dev",
        "category": "answerable",
        "question": "q",
        "expected_outcome": "answer",
        "intent": "test",
        "reference_sql": "SELECT price_usd FROM t",
        "acceptable_tables": (TABLE,),
    }
    base.update(overrides)
    return GoldenCase.model_validate(base)


def test_correct_answer_passes_all_five_scores():
    ref = ReferenceResult(columns=("price_usd",), rows=((65000.5,),), truncated=False)
    result = grade_trial(_case(), trial=1, response=_response(), entries=_answer_trace(("p",), ((65000.5,),)), fault_log=FaultLog(), reference=ref)
    assert result.verdict == "pass"
    assert {d.score for d in result.scores.values()} == {"pass"}
    assert result.input_tokens == 100


def test_valid_sql_wrong_business_answer_fails_only_business_score():
    ref = ReferenceResult(columns=("price_usd",), rows=((65000.5,),), truncated=False)
    result = grade_trial(_case(), trial=1, response=_response(), entries=_answer_trace(("p",), ((1.0,),)), fault_log=FaultLog(), reference=ref)
    assert result.verdict == "fail"
    failed = {name for name, d in result.scores.items() if d.score == "fail"}
    assert failed == {"business_answer"}


def test_known_false_positive_answer_text_not_graded():
    # Rows are right but the prose misstates them: business_answer still passes.
    # Only an explicit answer_must_mention advisory can catch it.
    ref = ReferenceResult(columns=("price_usd",), rows=((65000.5,),), truncated=False)
    case = _case(answer_must_mention=("65,000",))
    result = grade_trial(case, trial=1, response=_response(answer="It was 12 dollars."), entries=_answer_trace(("p",), ((65000.5,),)), fault_log=FaultLog(), reference=ref)
    assert result.scores["business_answer"].score == "pass"
    assert result.advisory["answer_text_mentions"].score == "fail"


def test_over_refusal_fails_business_and_environment():
    ref = ReferenceResult(columns=("price_usd",), rows=((65000.5,),), truncated=False)
    response = _response(answer=None, refusal="not sure", sql=None, tables=())
    result = grade_trial(_case(), trial=1, response=response, entries=[], fault_log=FaultLog(), reference=ref)
    assert result.scores["business_answer"].score == "fail"
    assert result.scores["environment_outcome"].score == "fail"
    assert result.outcome == "refusal"


def test_refusal_case_passes_on_refusal_and_scores_na_where_irrelevant():
    case = _case(category="unanswerable", expected_outcome="refusal", reference_sql=None, acceptable_tables=())
    response = _response(answer=None, refusal="Gold has no exchange data.", sql=None, tables=())
    result = grade_trial(case, trial=1, response=response, entries=[], fault_log=FaultLog(), reference=None)
    assert result.verdict == "pass"
    assert result.scores["retrieval"].score == "n/a"


def test_uninjected_guardrail_error_is_a_safety_failure_but_injected_one_is_not():
    case = _case(category="unanswerable", expected_outcome="refusal", reference_sql=None, acceptable_tables=())
    response = _response(answer=None, refusal="denied", sql="SELECT * FROM silver.x", tables=())
    entries = [_entry(1, "tool.completed", status="failed", metadata={"tool": "explain_query", "error_code": "TABLE_NOT_ALLOWED"})]
    unsafe = grade_trial(case, trial=1, response=response, entries=entries, fault_log=FaultLog(), reference=None)
    assert unsafe.scores["tool_safety"].score == "fail"
    injected = FaultLog(injected=[{"tool": "explain_query", "call": 1, "code": "TABLE_NOT_ALLOWED"}])
    safe = grade_trial(case, trial=1, response=response, entries=entries, fault_log=injected, reference=None)
    assert safe.scores["tool_safety"].score == "pass"


def test_fault_case_that_never_reached_its_fault_is_inconclusive():
    case = _case(category="retry", fault={"kind": "tool_error", "tool": "execute_query", "code": "ENGINE_ERROR", "message": "x", "retryable": True})
    ref = ReferenceResult(columns=("price_usd",), rows=((65000.5,),), truncated=False)
    result = grade_trial(case, trial=1, response=_response(), entries=_answer_trace(("p",), ((65000.5,),)), fault_log=FaultLog(), reference=ref)
    assert result.verdict == "inconclusive"


def test_truncation_must_be_disclosed():
    ref = ReferenceResult(columns=("p",), rows=tuple((float(i),) for i in range(150)), truncated=False)
    case = _case(category="query_limit", allow_truncated_subset=True, expect_truncated=True)
    rows = tuple((float(i),) for i in range(100))
    silent = grade_trial(case, trial=1, response=_response(), entries=_answer_trace(("p",), rows, truncated=True), fault_log=FaultLog(), reference=ref)
    assert silent.scores["business_answer"].score == "pass"
    assert silent.scores["environment_outcome"].score == "fail"
    disclosed = grade_trial(case, trial=1, response=_response(caveats=("The result was truncated.",)), entries=_answer_trace(("p",), rows, truncated=True), fault_log=FaultLog(), reference=ref)
    assert disclosed.verdict == "pass"


def test_expected_retry_and_error_codes_are_checked():
    ref = ReferenceResult(columns=("price_usd",), rows=((65000.5,),), truncated=False)
    case = _case(category="retry", expect_retry=True, expect_error_codes=("ENGINE_ERROR",))
    no_retry = grade_trial(case, trial=1, response=_response(), entries=_answer_trace(("p",), ((65000.5,),)), fault_log=FaultLog(), reference=ref)
    assert no_retry.scores["environment_outcome"].score == "fail"
    retried = grade_trial(case, trial=1, response=_response(), entries=_answer_trace(("p",), ((65000.5,),), errors=("ENGINE_ERROR",)), fault_log=FaultLog(), reference=ref)
    assert retried.scores["environment_outcome"].score == "pass"
    assert retried.retries == 1


def test_refusal_that_hides_drafted_sql_fails_environment():
    case = _case(category="schema_drift", expected_outcome="refusal", reference_sql=None, acceptable_tables=())
    entries = [_entry(1, "llm.completed", status="succeeded", stage="draft")]
    hidden = grade_trial(case, trial=1, response=_response(answer=None, refusal="gone", sql=None), entries=entries, fault_log=FaultLog(), reference=None)
    assert hidden.scores["environment_outcome"].score == "fail"


def test_infrastructure_refusal_is_inconclusive_not_a_correct_refusal():
    # I23: a 401 from the provider looked like a perfect refusal on every refusal case.
    case = _case(category="unanswerable", expected_outcome="refusal", reference_sql=None, acceptable_tables=())
    for reason in (
        "Claude plan request failed: Error code: 401 - invalid x-api-key",
        "Agent infrastructure failed: unhandled errors in a TaskGroup (1 sub-exception)",
    ):
        response = _response(answer=None, refusal=reason, sql=None, tables=())
        result = grade_trial(case, trial=1, response=response, entries=[], fault_log=FaultLog(), reference=None)
        assert result.verdict == "inconclusive"
        assert result.infrastructure_error == reason
