from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import time
from typing import Any, Callable, Dict, List, Optional, Protocol

from agent.compiler.models.issues import Issue
from agent.compiler.models.profile import DatasetProfile


@dataclass(frozen=True)
class Tier3ResolutionRequest:
    """
    Input contract sent to the model client to propose remediation for an unresolved issue.
    Excludes ephemeral or session-dependent IDs to maximize replay caching determinism.
    """
    issue_id: str
    issue_type: str
    issue_description: str
    dataset_name: str
    target_columns: List[str]
    dataset_schema: Dict[str, str]  # column_name -> dtype
    sample_values: List[Any] = field(default_factory=list)
    available_rules: List[Dict[str, Any]] = field(default_factory=list)
    target_engine: str = "AZURE_SQL"
    contract_version: str = "1.0"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Tier3ModelResponse:
    """
    Raw structured response and operational telemetry from the model invocation.
    Operational metadata (latency, tokens, retries) is strictly separated from the semantic proposal.
    """
    raw_content: str
    model_provider: str
    model_deployment: str
    model_version: str
    request_id: str
    latency_ms: float
    token_usage: Dict[str, int] = field(default_factory=dict)
    retry_count: int = 0
    status: str = "SUCCESS"  # SUCCESS | RETRYABLE_ERROR | FATAL_ERROR | TIMEOUT | UNAVAILABLE
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class Tier3ModelClient(Protocol):
    """Protocol for Tier 3 proposal generator clients."""

    def propose_resolution(self, request: Tier3ResolutionRequest) -> Tier3ModelResponse:
        """Invokes model provider or mock generator to propose structured remediation."""
        ...


class MockTier3ModelClient:
    """
    Configurable deterministic model client for offline verification, failure injection,
    prompt injection resistance testing, and caching validation.
    """

    def __init__(
        self,
        default_response: Optional[str] = None,
        responses_by_issue: Optional[Dict[str, str]] = None,
        latency_ms: float = 10.0,
        model_name: str = "mock-gpt-4o",
        simulate_timeout: bool = False,
        simulate_retryable_error_count: int = 0,
    ):
        self.default_response = default_response or "{}"
        self.responses_by_issue = dict(responses_by_issue or {})
        self.latency_ms = latency_ms
        self.model_name = model_name
        self.simulate_timeout = simulate_timeout
        self.simulate_retryable_error_count = simulate_retryable_error_count
        self.call_count: int = 0
        self.requests_received: List[Tier3ResolutionRequest] = []

    def propose_resolution(self, request: Tier3ResolutionRequest) -> Tier3ModelResponse:
        self.call_count += 1
        self.requests_received.append(request)

        if self.simulate_timeout:
            return Tier3ModelResponse(
                raw_content="",
                model_provider="mock",
                model_deployment=self.model_name,
                model_version="1.0",
                request_id=f"req_{self.call_count}",
                latency_ms=self.latency_ms,
                status="TIMEOUT",
                error_message="Model client connection timed out.",
            )

        if self.simulate_retryable_error_count > 0 and self.call_count <= self.simulate_retryable_error_count:
            return Tier3ModelResponse(
                raw_content="",
                model_provider="mock",
                model_deployment=self.model_name,
                model_version="1.0",
                request_id=f"req_{self.call_count}",
                latency_ms=self.latency_ms,
                retry_count=self.call_count - 1,
                status="RETRYABLE_ERROR",
                error_message="Rate limit or transient connection reset.",
            )

        content = self.responses_by_issue.get(request.issue_id, self.default_response)
        return Tier3ModelResponse(
            raw_content=content,
            model_provider="mock",
            model_deployment=self.model_name,
            model_version="1.0",
            request_id=f"req_{self.call_count}",
            latency_ms=self.latency_ms,
            token_usage={"prompt_tokens": 120, "completion_tokens": 85, "total_tokens": 205},
            retry_count=max(0, self.call_count - 1),
            status="SUCCESS",
        )


class OpenAITier3ModelClient:
    """Production LLM client for Tier 3 proposals utilizing AzureOpenAI or OpenAI."""

    def __init__(self, prompt_builder: Optional[Any] = None, timeout: float = 30.0):
        from agent.compiler.tier3.prompt_builder import Tier3PromptBuilder
        self.prompt_builder = prompt_builder or Tier3PromptBuilder()
        self.timeout = timeout

    def propose_resolution(self, request: Tier3ResolutionRequest) -> Tier3ModelResponse:
        t0 = time.time()
        from agent.model_config import load_llm_config
        cfg = load_llm_config(purpose="tier3_resolution") or load_llm_config(purpose="etl_codegen") or load_llm_config(purpose="general")

        if not cfg or not cfg.api_key:
            return Tier3ModelResponse(
                raw_content="",
                model_provider="none",
                model_deployment="none",
                model_version="none",
                request_id=f"req_{int(time.time()*1000)}",
                latency_ms=0.0,
                status="UNAVAILABLE",
                error_message="LLM provider credentials not configured for Tier 3 resolution.",
            )

        client = None
        try:
            if cfg.provider == "azure_openai" and cfg.endpoint:
                from openai import AzureOpenAI
                client = AzureOpenAI(
                    azure_endpoint=cfg.endpoint,
                    api_key=cfg.api_key,
                    api_version=cfg.api_version or "2024-02-01",
                    timeout=self.timeout,
                    max_retries=1,
                )
            elif cfg.provider == "openai":
                from openai import OpenAI
                client = OpenAI(
                    api_key=cfg.api_key,
                    timeout=self.timeout,
                    max_retries=1,
                )
        except Exception as e:
            return Tier3ModelResponse(
                raw_content="",
                model_provider=cfg.provider,
                model_deployment=cfg.model,
                model_version="1.0",
                request_id=f"req_{int(time.time()*1000)}",
                latency_ms=round((time.time() - t0) * 1000, 1),
                status="UNAVAILABLE",
                error_message=f"Failed to initialize OpenAI client: {e}",
            )

        if not client:
            return Tier3ModelResponse(
                raw_content="",
                model_provider=cfg.provider,
                model_deployment=cfg.model,
                model_version="1.0",
                request_id=f"req_{int(time.time()*1000)}",
                latency_ms=0.0,
                status="UNAVAILABLE",
                error_message="Could not initialize client with provider config.",
            )

        prompts = self.prompt_builder.build_prompt(request)
        try:
            resp = client.chat.completions.create(
                model=cfg.model,
                messages=[
                    {"role": "system", "content": prompts["system_prompt"]},
                    {"role": "user", "content": prompts["user_prompt"]},
                ],
                temperature=0.0,
                response_format={"type": "json_object"},
            )
            raw = resp.choices[0].message.content or "{}"
            usage = {}
            if getattr(resp, "usage", None):
                usage = {
                    "prompt_tokens": resp.usage.prompt_tokens,
                    "completion_tokens": resp.usage.completion_tokens,
                    "total_tokens": resp.usage.total_tokens,
                }
            latency = round((time.time() - t0) * 1000, 1)
            return Tier3ModelResponse(
                raw_content=raw,
                model_provider=cfg.provider,
                model_deployment=cfg.model,
                model_version="1.0",
                request_id=getattr(resp, "id", f"req_{int(time.time()*1000)}"),
                latency_ms=latency,
                token_usage=usage,
                status="SUCCESS",
            )
        except Exception as exc:
            import re
            latency = round((time.time() - t0) * 1000, 1)
            raw_err = str(exc)
            # Redact secrets, keys, or bearer tokens in error message
            redacted_err = re.sub(
                r"(Bearer\s+|api[-_]?key\s*[:=]\s*|key=)[A-Za-z0-9_\-\.]{8,}",
                r"\1[REDACTED]",
                raw_err,
                flags=re.IGNORECASE,
            )
            err_lower = redacted_err.lower()
            if "content_filter" in err_lower or "responsibleai" in err_lower:
                status = "CONTENT_FILTER"
            elif "timeout" in err_lower or "timed out" in err_lower:
                status = "TIMEOUT"
            elif "rate" in err_lower or "429" in err_lower:
                status = "RETRYABLE_ERROR"
            elif "connection" in err_lower or "network" in err_lower:
                status = "UNAVAILABLE"
            else:
                status = "FATAL_ERROR"

            return Tier3ModelResponse(
                raw_content="",
                model_provider=cfg.provider,
                model_deployment=cfg.model,
                model_version="1.0",
                request_id=f"req_{int(time.time()*1000)}",
                latency_ms=latency,
                status=status,
                error_message=redacted_err,
            )
