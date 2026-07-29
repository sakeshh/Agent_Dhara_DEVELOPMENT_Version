"""Intelligent Data Assessment Engine (Modularized Shim).

Re-exports everything from the agent.profiling package for backward compatibility.
"""
from __future__ import annotations

from agent.profiling import *

# Explicitly re-export private helper functions imported by tests/specialists/MCP
from agent.profiling.data_loaders import _sql_location_key_prefix, _load_json_to_df
from agent.profiling.format_validators import _detect_phone_formats
from agent.profiling.type_inference import _is_text_dtype
