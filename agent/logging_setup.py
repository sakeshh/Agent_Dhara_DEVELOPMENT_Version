from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if hasattr(record, "request_id"):
            payload["request_id"] = getattr(record, "request_id")
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


# ---------------------------------------------------------------------------
# Issue 3 Fix: Suppress noisy great_expectations internal loggers
# Issue 4 Fix: Suppress verbose Azure SDK HTTP request/response logging
# ---------------------------------------------------------------------------
_NOISY_LOGGERS = [
    # great_expectations — hundreds of closure/datasource registration INFO lines
    "great_expectations._docs_decorators",
    "great_expectations.expectations.registry",
    "great_expectations.datasource.fluent.config",
    "great_expectations.data_context",
    # Keep great_expectations.validator.validator at WARNING so we still see
    # "N expectation(s) included" — but suppress the GX 'Could not find' noise
    "great_expectations.data_context.data_context.context_factory",
    "great_expectations.data_context.types.base",
    # Azure SDK HTTP — logs every single blob request + response header dump
    "azure.core.pipeline.policies.http_logging_policy",
    # httpx — verbose HTTP request lines for OpenAI calls (200 OK spamming)
    "httpx",
    "httpx2",
]


def configure_noisy_loggers() -> None:
    """Suppress verbose third-party library loggers to WARNING level.

    Called automatically by setup_logging(). Can also be called standalone
    in tests or scripts that import individual modules.
    """
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)


def setup_logging() -> None:
    level = (os.environ.get("LOG_LEVEL") or "INFO").upper().strip()
    logging.basicConfig(level=level)
    root = logging.getLogger()
    for h in list(root.handlers):
        h.setFormatter(JsonFormatter())
    # Apply noise suppression after basicConfig so the levels take effect
    configure_noisy_loggers()

