"""Tests for privacy levels, full-flow content, and fail-open trace writes."""

import json

import pytest

from ai_agent.agent_service.contracts import AnswerDecision
from ai_agent.agent_service.loop import AgentLoop
from ai_agent.agent_service.tracing import (
    AgentTrace,
    AgentTraceEntry,
    JsonlAgentTrace,
)
from ai_agent.tests.test_agent_loop import (
    FakeGateway,
    FakeProvider,
    FakeTools,
    draft,
    plan,
    run,
)


class MemoryTraceSink:
    level = "full"

    def __init__(self):
        self.entries = []

    def write(self, entry):
        self.entries.append(entry)


class BrokenTraceSink:
    level = "full"

    def write(self, entry):
        raise OSError("trace disk unavailable")


def entry(*, content):
    return AgentTraceEntry(
        timestamp="2026-08-10T00:00:00Z",
        sequence=1,
        request_id="request-1",
        profile="fast",
        model_id="model-1",
        prompt_version="prompt-1",
        event="test.event",
        status="succeeded",
        elapsed_ms=5,
        content=content,
    )


def test_jsonl_trace_levels_control_full_content(tmp_path):
    full_path = tmp_path / "full.jsonl"
    JsonlAgentTrace(full_path, level="full", log_level="CRITICAL").write(
        entry(content={"question": "full question", "rows": [["BTC"]]})
    )
    full = json.loads(full_path.read_text(encoding="utf-8"))
    assert full["content"]["question"] == "full question"
    assert full["content"]["rows"] == [["BTC"]]

    basic_path = tmp_path / "basic.jsonl"
    JsonlAgentTrace(basic_path, level="basic", log_level="CRITICAL").write(
        entry(content={"question": "must be removed"})
    )
    basic = json.loads(basic_path.read_text(encoding="utf-8"))
    assert basic["content"] is None

    off_path = tmp_path / "off.jsonl"
    JsonlAgentTrace(off_path, level="off", log_level="CRITICAL").write(
        entry(content={"question": "not written"})
    )
    assert not off_path.exists()


def test_trace_environment_rejects_unknown_levels(monkeypatch):
    monkeypatch.setenv("AI_TRACE_LEVEL", "verbose")
    with pytest.raises(ValueError, match="AI_TRACE_LEVEL"):
        JsonlAgentTrace.from_env()

    monkeypatch.setenv("AI_TRACE_LEVEL", "full")
    monkeypatch.setenv("AI_LOG_LEVEL", "LOUD")
    with pytest.raises(ValueError, match="AI_LOG_LEVEL"):
        JsonlAgentTrace.from_env()


def test_full_loop_trace_contains_prompts_rows_tokens_query_and_answer():
    sink = MemoryTraceSink()
    provider = FakeProvider(
        plan=[plan()],
        draft=[draft()],
        answer=[AnswerDecision(answer="BTC is available.", caveats=())],
    )

    response = run(AgentLoop(provider, FakeGateway(FakeTools()), trace_sink=sink))

    assert response.answer == "BTC is available."
    events = [event.event for event in sink.entries]
    assert events[0] == "request.started"
    assert events[-1] == "request.completed"
    assert "mcp.session.ready" in events
    assert "tool.started" in events
    assert "tool.completed" in events
    assert "llm.started" in events
    assert "llm.completed" in events
    assert "check.completed" in events
    assert [event.sequence for event in sink.entries] == list(
        range(1, len(sink.entries) + 1)
    )

    started = next(
        event
        for event in sink.entries
        if event.event == "request.started"
    )
    assert started.content["request"]["question"] == "Which symbols are available?"

    llm_started = next(
        event
        for event in sink.entries
        if event.event == "llm.started" and event.stage == "draft"
    )
    assert "INPUT_JSON" in llm_started.content["prompt"]
    assert llm_started.content["system"]

    llm_completed = next(
        event
        for event in sink.entries
        if event.event == "llm.completed" and event.stage == "draft"
    )
    assert llm_completed.metadata["input_tokens"] == 10
    assert llm_completed.metadata["output_tokens"] == 5
    assert llm_completed.content["output"]["sql"] == draft().sql

    execution = next(
        event
        for event in sink.entries
        if event.event == "tool.completed"
        and event.metadata.get("tool") == "execute_query"
    )
    assert execution.metadata["query_id"] == "query-1"
    assert execution.content["result"]["rows"] == [["BTC"]]

    terminal = sink.entries[-1]
    assert terminal.content["response"]["answer"] == "BTC is available."


def test_trace_write_failure_does_not_break_the_agent(caplog):
    provider = FakeProvider(
        plan=[plan()],
        draft=[draft()],
        answer=[AnswerDecision(answer="BTC is available.", caveats=())],
    )

    response = run(
        AgentLoop(provider, FakeGateway(FakeTools()), trace_sink=BrokenTraceSink())
    )

    assert response.answer == "BTC is available."
    assert "Agent trace write failed" in caplog.text


def test_request_trace_uses_monotonic_sequence_and_elapsed_time():
    sink = MemoryTraceSink()
    ticks = iter((10.0, 10.005, 10.010))
    trace = AgentTrace(
        sink,
        request_id="request-1",
        profile="fast",
        model_id="model-1",
        prompt_version="prompt-1",
        timer=lambda: next(ticks),
    )

    trace.event("first")
    trace.event("second")

    assert [item.sequence for item in sink.entries] == [1, 2]
    assert [item.elapsed_ms for item in sink.entries] == [5, 10]
