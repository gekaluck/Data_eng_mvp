"""Local structured tracing for the owned agent loop."""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from time import monotonic
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from ai_agent.mcp_server.budget import BudgetProfile

TraceLevel = Literal["off", "basic", "full"]
TraceStatus = Literal["started", "succeeded", "failed", "info"]
DEFAULT_TRACE_PATH = Path("ai_agent/runtime/agent-trace.jsonl")
DEFAULT_TRACE_LEVEL: TraceLevel = "full"
DEFAULT_LOG_LEVEL = "INFO"
_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
_FAILURE_LOGGER = logging.getLogger(__name__)


class AgentTraceEntry(BaseModel):
    """One ordered event in a one-shot agent request."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    timestamp: datetime
    sequence: int = Field(ge=1)
    request_id: str
    profile: BudgetProfile
    model_id: str
    prompt_version: str
    event: str
    status: TraceStatus
    elapsed_ms: int = Field(ge=0)
    duration_ms: int | None = Field(default=None, ge=0)
    stage: str | None = None
    attempt: int | None = Field(default=None, ge=1)
    metadata: dict[str, Any] = Field(default_factory=dict)
    content: dict[str, Any] | None = None


class TraceSink(Protocol):
    """Destination contract used by request-local trace contexts."""

    level: TraceLevel

    def write(self, entry: AgentTraceEntry) -> None: ...


class NullTraceSink:
    """Test/default sink that performs no writes or console logging."""

    level: TraceLevel = "off"

    def write(self, entry: AgentTraceEntry) -> None:
        return None


class JsonlAgentTrace:
    """Append full ordered traces locally and emit concise console events."""

    def __init__(
        self,
        path: str | Path = DEFAULT_TRACE_PATH,
        *,
        level: TraceLevel = DEFAULT_TRACE_LEVEL,
        log_level: str = DEFAULT_LOG_LEVEL,
    ) -> None:
        if level not in {"off", "basic", "full"}:
            raise ValueError("AI_TRACE_LEVEL must be one of: off, basic, full.")
        normalized_log_level = log_level.upper()
        if normalized_log_level not in _LOG_LEVELS:
            raise ValueError(
                "AI_LOG_LEVEL must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL."
            )
        self.path = Path(path)
        self.level = level
        self._lock = Lock()
        self._logger = logging.getLogger("ai_agent.trace")
        self._logger.setLevel(normalized_log_level)
        self._logger.propagate = False
        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stderr)
            handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s %(message)s")
            )
            self._logger.addHandler(handler)

    @classmethod
    def from_env(cls) -> "JsonlAgentTrace":
        return cls(
            os.getenv("AI_AGENT_TRACE_PATH", str(DEFAULT_TRACE_PATH)),
            level=os.getenv("AI_TRACE_LEVEL", DEFAULT_TRACE_LEVEL),
            log_level=os.getenv("AI_LOG_LEVEL", DEFAULT_LOG_LEVEL),
        )

    def write(self, entry: AgentTraceEntry) -> None:
        if self.level == "off":
            return
        payload = entry.model_dump(mode="json")
        if self.level != "full":
            payload["content"] = None
        line = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(line + "\n")
        self._logger.info(
            "agent_trace request_id=%s event=%s stage=%s attempt=%s status=%s "
            "elapsed_ms=%s duration_ms=%s",
            entry.request_id,
            entry.event,
            entry.stage or "-",
            entry.attempt or "-",
            entry.status,
            entry.elapsed_ms,
            entry.duration_ms if entry.duration_ms is not None else "-",
        )


class AgentTrace:
    """Request-local sequence/timing context with fail-open monitoring writes."""

    def __init__(
        self,
        sink: TraceSink,
        *,
        request_id: str,
        profile: BudgetProfile,
        model_id: str,
        prompt_version: str,
        timer=monotonic,
    ) -> None:
        self.sink = sink
        self.request_id = request_id
        self.profile = profile
        self.model_id = model_id
        self.prompt_version = prompt_version
        self._timer = timer
        self._started = timer()
        self._sequence = 0

    @property
    def enabled(self) -> bool:
        return self.sink.level != "off"

    def now(self) -> float:
        return self._timer()

    def event(
        self,
        event: str,
        *,
        status: TraceStatus = "info",
        stage: str | None = None,
        attempt: int | None = None,
        duration_ms: int | None = None,
        metadata: dict[str, Any] | None = None,
        content: dict[str, Any] | None = None,
    ) -> None:
        if not self.enabled:
            return
        self._sequence += 1
        entry = AgentTraceEntry(
            timestamp=datetime.now(timezone.utc),
            sequence=self._sequence,
            request_id=self.request_id,
            profile=self.profile,
            model_id=self.model_id,
            prompt_version=self.prompt_version,
            event=event,
            status=status,
            elapsed_ms=_milliseconds(self._timer() - self._started),
            duration_ms=duration_ms,
            stage=stage,
            attempt=attempt,
            metadata=_jsonable(metadata or {}),
            content=_jsonable(content) if content is not None else None,
        )
        try:
            self.sink.write(entry)
        except Exception:
            _FAILURE_LOGGER.exception(
                "Agent trace write failed for request_id=%s event=%s",
                self.request_id,
                event,
            )


def elapsed_ms(started: float, timer=monotonic) -> int:
    return _milliseconds(timer() - started)


def _milliseconds(seconds: float) -> int:
    return max(0, round(seconds * 1000))


def _jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Exception):
        return {"type": type(value).__name__, "message": str(value)}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)
