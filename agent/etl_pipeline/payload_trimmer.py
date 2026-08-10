from dataclasses import dataclass
from typing import Literal, Any, Dict, List
import copy
import json
import logging

logger = logging.getLogger("agent.etl_pipeline.payload_trimmer")

@dataclass
class TrimConfig:
    mode: Literal["floor_check", "codegen"]
    token_budget: int
    field_priority: List[str]  # fields dropped or minimized first

_FLOOR_TRIM_CONFIG = TrimConfig(
    mode="floor_check",
    token_budget=8_000,
    field_priority=["manual_review", "domain_rules", "source_metadata", "cross_field_rules"]
)

_CODEGEN_TRIM_CONFIG = TrimConfig(
    mode="codegen",
    token_budget=80_000,
    field_priority=["manual_review", "domain_rules", "source_metadata", "cross_field_rules"]
)

def _estimate_tokens(text: str | dict) -> int:
    if isinstance(text, dict):
        try:
            text = json.dumps(text, default=str)
        except Exception:
            text = str(text)
    try:
        import tiktoken
        enc = tiktoken.get_encoding("cl100k_base")
        return len(enc.encode(text, disallowed_special=()))
    except (ImportError, Exception):
        # JSON payload: ~2 chars per token (more accurate than 4)
        # Prose: ~4 chars per token
        is_json_like = text.lstrip().startswith(("{", "["))
        chars_per_token = 2 if is_json_like else 4
        return max(1, len(text) // chars_per_token)

def trim_payload(payload: dict, config: TrimConfig) -> dict:
    """
    Unified canonical payload trimmer.
    """
    result = copy.deepcopy(payload)
    est = _estimate_tokens(result)
    
    if est <= config.token_budget:
        return result

    # Standard cleanups: remove unnecessary narration and policy blocks
    for key in ("plan_narrator_output", "policy_block", "rule_provenance"):
        result.pop(key, None)
    
    est = _estimate_tokens(result)
    if est <= config.token_budget:
        return result

    if config.mode == "floor_check":
        # Level 1 Floor Check Trimming
        sm = result.get("source_metadata") or {}
        slim_sm = {}
        for ds, meta in sm.items():
            if not isinstance(meta, dict):
                continue
            slim_sm[ds] = {
                "row_count": meta.get("row_count"),
                "columns": {
                    col: {
                        "dtype": cm.get("dtype") if isinstance(cm, dict) else None,
                        "semantic_type": cm.get("semantic_type") if isinstance(cm, dict) else None,
                        "sub_type": cm.get("sub_type") if isinstance(cm, dict) else None,
                    }
                    for col, cm in (meta.get("columns") or {}).items()
                }
            }
        result["source_metadata"] = slim_sm

        mr = result.get("manual_review") or []
        if len(mr) > 8:
            result["manual_review"] = mr[:8]
            result["manual_review_truncated"] = f"...{len(mr)-8} more items omitted to fit context"

        blocked = result.get("blocked") or []
        if len(blocked) > 5:
            result["blocked"] = blocked[:5]
            result["blocked_truncated"] = f"...{len(blocked)-5} more blocked items omitted"

        est = _estimate_tokens(result)
        if est <= config.token_budget:
            return result

        # Level 2 Floor Check Trimming
        ds = result.get("datasets") or {}
        _STEP_PRIORITIES = {
            "cast_type": 1,
            "strip_symbols": 2,
            "fill_nulls": 3,
            "zero_to_null": 4,
            "lowercase": 5,
            "uppercase": 5,
            "trim": 5,
            "sanitize_email": 6,
            "normalize_phone": 6,
            "hash_phone": 6,
            "mask_phone": 6,
            "clip_outliers": 7,
            "flag_outliers": 7,
            "clip_or_flag": 7,
            "deduplicate": 8,
        }
        def get_priority(st):
            return _STEP_PRIORITIES.get(str(st.get("action") or "").lower().strip(), 99)

        for ds_name, block in ds.items():
            if not isinstance(block, dict):
                continue
            steps = block.get("steps") or []
            if len(steps) > 25:
                sorted_steps = sorted(steps, key=get_priority)
                keep_steps = sorted_steps[:25]
                omitted_count = len(steps) - 25
                for i, st in enumerate(keep_steps):
                    st_copy = dict(st)
                    st_copy["order"] = i + 1
                    keep_steps[i] = st_copy
                block["steps"] = keep_steps
                block["omitted_steps_summary"] = f"+{omitted_count} more low-priority steps omitted — apply standard heuristics"

        dr = result.get("domain_rules") or {}
        if len(dr) > 15:
            result["domain_rules"] = {k: dr[k] for k in list(dr.keys())[:15]}
            result["domain_rules_truncated"] = f"...{len(dr)-15} more domain rules omitted"

        rel = result.get("relationships") or {}
        joins = rel.get("joins") or []
        if len(joins) > 10:
            rel_copy = dict(rel)
            rel_copy["joins"] = joins[:10]
            rel_copy["joins_truncated"] = f"...{len(joins)-10} more joins omitted"
            result["relationships"] = rel_copy

    else:
        # Codegen mode trimming
        # Step 2: slim source_metadata to column names + dtype only
        for ds, meta in (result.get("source_metadata") or {}).items():
            if not isinstance(meta, dict):
                continue
            cols_dict = meta.get("columns") or {}
            meta["columns"] = {}
            for col, cmeta in cols_dict.items():
                meta["columns"][col] = {
                    "dtype": cmeta.get("dtype") if isinstance(cmeta, dict) else None,
                    "semantic_type": cmeta.get("semantic_type") if isinstance(cmeta, dict) else None,
                }
        est = _estimate_tokens(result)
        if est <= config.token_budget:
            return result

        # Step 3: trim manual_review to top 20 items
        if len(result.get("manual_review") or []) > 20:
            result["manual_review"] = result["manual_review"][:20]
        est = _estimate_tokens(result)
        if est <= config.token_budget:
            return result

        # Step 4: Drop column details
        for ds_name, ds_data in (result.get("datasets") or {}).items():
            if isinstance(ds_data, dict):
                ds_data.pop("column_descriptions", None)
                ds_data.pop("semantic_tags", None)
                ds_data.pop("quality_summary", None)
        est = _estimate_tokens(result)
        if est <= config.token_budget:
            return result

        # Step 5: If still over budget, apply priority list removals
        for field in config.field_priority:
            if est <= config.token_budget:
                break
            if field in result:
                result.pop(field, None)
                est = _estimate_tokens(result)

    return result
