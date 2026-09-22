from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal, Optional


Provider = Literal["azure_openai", "openai"]


@dataclass(frozen=True)
class LLMConfig:
    provider: Provider
    api_key: str
    model: str
    endpoint: Optional[str] = None
    api_version: Optional[str] = None


def load_llm_config(*, purpose: str = "general") -> Optional[LLMConfig]:
    """
    Central place to configure LLM usage.

    Supported env:
    - Azure OpenAI:
        AZURE_OPENAI_ENDPOINT
        AZURE_OPENAI_API_KEY
        AZURE_OPENAI_API_VERSION (optional)
        AZURE_OPENAI_DEPLOYMENT or AZURE_OPENAI_DEPLOYMENT_NAME
    - OpenAI:
        OPENAI_API_KEY
        OPENAI_MODEL (optional)

    `purpose` allows per-purpose overrides. Set AZURE_OPENAI_CODEGEN_DEPLOYMENT
    to use a separate (faster) model for ETL code generation (e.g. gpt-4o).
    Falls back to the default deployment if not set.
    """
    az_endpoint = (os.environ.get("AZURE_OPENAI_ENDPOINT") or "").strip().rstrip("/")
    az_key = (os.environ.get("AZURE_OPENAI_API_KEY") or "").strip()

    # Allow a dedicated deployment for code generation (heavier, needs faster model)
    if purpose == "etl_codegen":
        codegen_deployment = (os.environ.get("AZURE_OPENAI_CODEGEN_DEPLOYMENT") or "").strip()
        if codegen_deployment and az_endpoint and az_key:
            az_version = (os.environ.get("AZURE_OPENAI_API_VERSION") or "").strip() or "2024-02-01"
            return LLMConfig(
                provider="azure_openai",
                api_key=az_key,
                endpoint=az_endpoint,
                api_version=az_version,
                model=codegen_deployment,
            )

    az_deployment = (
        (os.environ.get("AZURE_OPENAI_DEPLOYMENT") or os.environ.get("AZURE_OPENAI_DEPLOYMENT_NAME") or "").strip()
    )
    az_version = (os.environ.get("AZURE_OPENAI_API_VERSION") or "").strip() or "2024-02-01"
    if az_endpoint and az_key and az_deployment:
        return LLMConfig(
            provider="azure_openai",
            api_key=az_key,
            endpoint=az_endpoint,
            api_version=az_version,
            model=az_deployment,
        )

    oai_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    oai_model = (os.environ.get("OPENAI_MODEL") or "").strip() or "gpt-4o-mini"
    if oai_key:
        return LLMConfig(provider="openai", api_key=oai_key, model=oai_model)

    return None


def is_llm_enabled(*, purpose: str = "general") -> bool:
    return load_llm_config(purpose=purpose) is not None


LLM_REQUEST_TIMEOUT = int(os.getenv("LLM_REQUEST_TIMEOUT", "120"))


_MODEL_CONTEXT_WINDOWS: dict[str, int] = {
    "gpt-5.4":             1_048_576,
    "gpt-5.4-mini":        1_048_576,
    "gpt-5":               1_048_576,
    "gpt-4.1":             1_048_576,
    "gpt-4.1-mini":        1_048_576,
    "gpt-4.1-nano":        1_048_576,
    "o1":                    200_000,
    "o3":                    200_000,
    "o3-mini":               200_000,
    "gpt-4o":                128_000,
    "gpt-4o-mini":           128_000,
    "gpt-4":                   8_192,
    "gpt-4-turbo":           128_000,
    "gpt-35-turbo":           16_385,
    "gpt-4-32k":              32_768,
}

def get_context_window(model: str) -> int:
    underlying = (os.environ.get("AZURE_OPENAI_MODEL_NAME") or "").strip().lower()
    search_str = f"{model.lower()} {underlying}"
    for key, window in _MODEL_CONTEXT_WINDOWS.items():
        if key in search_str:
            return window
    return 128_000  # safe default for unknown models


def safe_chat_completion(client, **kwargs):
    """
    Executes client.chat.completions.create with adaptive parameter handling
    between older models (which require max_tokens) and newer models like
    GPT-5.4-mini, GPT-5, o1, o3, and API versions 2025+ (which require max_completion_tokens).
    """
    call_kwargs = dict(kwargs)
    api_version = (os.getenv("AZURE_OPENAI_API_VERSION") or "").strip()
    model_name = str(call_kwargs.get("model") or "").lower()

    # Pre-emptively translate max_tokens -> max_completion_tokens for models/APIs known to reject max_tokens
    prefer_max_completion = (
        "2025-" in api_version
        or any(k in model_name for k in ("gpt-5", "o1", "o3", "gpt-4.1"))
    )

    if "max_tokens" in call_kwargs and prefer_max_completion:
        call_kwargs["max_completion_tokens"] = call_kwargs.pop("max_tokens")

    try:
        return client.chat.completions.create(**call_kwargs)
    except Exception as exc:
        err_msg = str(exc)
        # Case 1: Model rejects max_tokens and requests max_completion_tokens
        if "max_tokens" in call_kwargs and ("max_completion_tokens" in err_msg or "unsupported_parameter" in err_msg):
            call_kwargs["max_completion_tokens"] = call_kwargs.pop("max_tokens")
            return client.chat.completions.create(**call_kwargs)
        # Case 2: Older model or proxy rejects max_completion_tokens and requests max_tokens
        if "max_completion_tokens" in call_kwargs and "max_tokens" in err_msg and "unsupported" in err_msg:
            call_kwargs["max_tokens"] = call_kwargs.pop("max_completion_tokens")
            return client.chat.completions.create(**call_kwargs)
        raise



