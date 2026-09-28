from __future__ import annotations

import re
from typing import Dict, List, Optional

from agent.compiler.models.actions import (
    ActionSource,
    ComparisonOperator,
    ConditionalUpdateParameters,
    PadParameters,
    PatternReplaceParameters,
    PrefixAddParameters,
    StructuredAction,
    StructuredCondition,
    SuffixAddParameters,
    ValueMapParameters,
)
from agent.compiler.models.logical import ActionType
from agent.compiler.rules.column_resolver import ColumnResolver
from agent.compiler.rules.models import DecompositionStatus, RuleDecompositionResult


class DeterministicParser:
    """
    Tier 1 Deterministic Parser.
    Conservative rule grammar matcher for common, unambiguous business rules.
    Only claims a match when confident about the semantics.
    """

    @classmethod
    def parse(
        cls,
        rule_text: str,
        dataset_name: str,
        available_columns: List[str],
    ) -> Optional[RuleDecompositionResult]:
        text = rule_text.strip()

        # Multi-action rule pattern: e.g. "Trim X, pad with zeros to 8, then prefix EMP-"
        multi_res = cls._parse_multi_action(text, dataset_name, available_columns)
        if multi_res:
            return multi_res

        # 1. Prefix: e.g. "EmployeeCode must start with 'EMP-'" or "Prefix EmployeeCode with 'EMP-'"
        m = re.search(
            r"(?:(?:ensure|make|require)?\s*\[?([a-zA-Z0-9_\s]+)\]?\s+(?:must\s+start\s+with|starts\s+with|prefix\s+with)\s+['\"]([^'\"]+)['\"])|"
            r"(?:prefix\s+\[?([a-zA-Z0-9_\s]+)\]?\s+with\s+['\"]?([a-zA-Z0-9_\-]+)['\"]?)",
            text,
            re.IGNORECASE,
        )
        if m:
            col_raw = m.group(1) or m.group(3)
            prefix_val = m.group(2) or m.group(4)
            resolved_col = ColumnResolver.resolve_column(col_raw, available_columns)
            if not resolved_col:
                return RuleDecompositionResult(
                    status=DecompositionStatus.INVALID,
                    original_rule=rule_text,
                    diagnostics=[f"Unknown target column '{col_raw}' in rule '{rule_text}'."],
                )
            action = StructuredAction(
                action_type=ActionType.PREFIX_ADD,
                dataset_name=dataset_name,
                target_columns=[resolved_col],
                parameters={"prefix": prefix_val, "only_if_missing": True},
                source=ActionSource.BUSINESS_RULE,
                extraction_method="deterministic",
                confidence=1.0,
            )
            return RuleDecompositionResult(
                status=DecompositionStatus.RESOLVED,
                actions=[action],
                extraction_method="deterministic",
                confidence=1.0,
                original_rule=rule_text,
            )

        # 2. Suffix: e.g. "EmployeeCode must end with '-IN'" or "Suffix EmployeeCode with '-IN'"
        m = re.search(
            r"(?:(?:ensure|make|require)?\s*\[?([a-zA-Z0-9_\s]+)\]?\s+(?:must\s+end\s+with|ends\s+with|suffix\s+with)\s+['\"]([^'\"]+)['\"])|"
            r"(?:suffix\s+\[?([a-zA-Z0-9_\s]+)\]?\s+with\s+['\"]?([a-zA-Z0-9_\-]+)['\"]?)",
            text,
            re.IGNORECASE,
        )
        if m:
            col_raw = m.group(1) or m.group(3)
            suffix_val = m.group(2) or m.group(4)
            resolved_col = ColumnResolver.resolve_column(col_raw, available_columns)
            if not resolved_col:
                return RuleDecompositionResult(
                    status=DecompositionStatus.INVALID,
                    original_rule=rule_text,
                    diagnostics=[f"Unknown target column '{col_raw}' in rule '{rule_text}'."],
                )
            action = StructuredAction(
                action_type=ActionType.SUFFIX_ADD,
                dataset_name=dataset_name,
                target_columns=[resolved_col],
                parameters={"suffix": suffix_val, "only_if_missing": True},
                source=ActionSource.BUSINESS_RULE,
                extraction_method="deterministic",
                confidence=1.0,
            )
            return RuleDecompositionResult(
                status=DecompositionStatus.RESOLVED,
                actions=[action],
                extraction_method="deterministic",
                confidence=1.0,
                original_rule=rule_text,
            )

        # 3. Pad: e.g. "Left pad EmployeeCode with zeros to length 8" or "Pad EmployeeCode with 0 to 8"
        m = re.search(
            r"(?:(left|right)\s+)?pad\s+\[?([a-zA-Z0-9_\s]+?)\]?\s+(?:with\s+['\"]?([0-9a-zA-Z]|zeros?)['\"]?\s+)?(?:to\s+(?:length\s+)?([0-9]+))",
            text,
            re.IGNORECASE,
        )
        if m:
            side = (m.group(1) or "left").lower()
            col_raw = m.group(2)
            fill_raw = m.group(3) or "0"
            fill_char = "0" if "zero" in fill_raw.lower() else fill_raw[:1]
            length_val = int(m.group(4))
            resolved_col = ColumnResolver.resolve_column(col_raw, available_columns)
            if not resolved_col:
                return RuleDecompositionResult(
                    status=DecompositionStatus.INVALID,
                    original_rule=rule_text,
                    diagnostics=[f"Unknown target column '{col_raw}' in rule '{rule_text}'."],
                )
            action = StructuredAction(
                action_type=ActionType.PAD,
                dataset_name=dataset_name,
                target_columns=[resolved_col],
                parameters={"length": length_val, "fill_character": fill_char, "side": side},
                source=ActionSource.BUSINESS_RULE,
                extraction_method="deterministic",
                confidence=1.0,
            )
            return RuleDecompositionResult(
                status=DecompositionStatus.RESOLVED,
                actions=[action],
                extraction_method="deterministic",
                confidence=1.0,
                original_rule=rule_text,
            )

        # 4. Pattern Replace (Literal): e.g. "Replace '-' with '/' in AccountCode"
        m = re.search(
            r"replace\s+['\"]([^'\"]+)['\"]\s+with\s+['\"]([^'\"]*)['\"]\s+in\s+\[?([a-zA-Z0-9_\s]+)\]?",
            text,
            re.IGNORECASE,
        )
        if m:
            pattern_val = m.group(1)
            repl_val = m.group(2)
            col_raw = m.group(3)
            resolved_col = ColumnResolver.resolve_column(col_raw, available_columns)
            if not resolved_col:
                return RuleDecompositionResult(
                    status=DecompositionStatus.INVALID,
                    original_rule=rule_text,
                    diagnostics=[f"Unknown target column '{col_raw}' in rule '{rule_text}'."],
                )
            action = StructuredAction(
                action_type=ActionType.PATTERN_REPLACE,
                dataset_name=dataset_name,
                target_columns=[resolved_col],
                parameters={"pattern": pattern_val, "replacement": repl_val, "regex": False},
                source=ActionSource.BUSINESS_RULE,
                extraction_method="deterministic",
                confidence=1.0,
            )
            return RuleDecompositionResult(
                status=DecompositionStatus.RESOLVED,
                actions=[action],
                extraction_method="deterministic",
                confidence=1.0,
                original_rule=rule_text,
            )

        # 5. Value Map: e.g. "Map A to Active and I to Inactive in Status"
        m_map = re.search(r"map\s+(.*?)\s+in\s+\[?([a-zA-Z0-9_\s]+)\]?$", text, re.IGNORECASE)
        if m_map:
            pairs_text = m_map.group(1)
            col_raw = m_map.group(2)
            resolved_col = ColumnResolver.resolve_column(col_raw, available_columns)
            if not resolved_col:
                return RuleDecompositionResult(
                    status=DecompositionStatus.INVALID,
                    original_rule=rule_text,
                    diagnostics=[f"Unknown target column '{col_raw}' in rule '{rule_text}'."],
                )
            # Find all "X to Y" patterns
            pair_matches = re.findall(r"['\"]?([a-zA-Z0-9_]+)['\"]?\s+to\s+['\"]?([a-zA-Z0-9_]+)['\"]?", pairs_text, re.IGNORECASE)
            if pair_matches:
                mapping_dict = {src: tgt for src, tgt in pair_matches}
                action = StructuredAction(
                    action_type=ActionType.VALUE_MAP,
                    dataset_name=dataset_name,
                    target_columns=[resolved_col],
                    parameters={"mappings": mapping_dict, "preserve_unmapped": True},
                    source=ActionSource.BUSINESS_RULE,
                    extraction_method="deterministic",
                    confidence=1.0,
                )
                return RuleDecompositionResult(
                    status=DecompositionStatus.RESOLVED,
                    actions=[action],
                    extraction_method="deterministic",
                    confidence=1.0,
                    original_rule=rule_text,
                )

        # 6. Conditional Rule: e.g. "If Department equals Contractor, set EmployeeType to External"
        m_cond = re.search(
            r"if\s+\[?([a-zA-Z0-9_\s]+)\]?\s+(?:equals?|is|=)\s+['\"]?([a-zA-Z0-9_\-]+)['\"]?\s*,?\s*set\s+\[?([a-zA-Z0-9_\s]+)\]?\s+to\s+['\"]?([a-zA-Z0-9_\-]+)['\"]?",
            text,
            re.IGNORECASE,
        )
        if m_cond:
            cond_col_raw = m_cond.group(1)
            cond_val = m_cond.group(2)
            tgt_col_raw = m_cond.group(3)
            new_val = m_cond.group(4)
            cond_col = ColumnResolver.resolve_column(cond_col_raw, available_columns)
            tgt_col = ColumnResolver.resolve_column(tgt_col_raw, available_columns)
            if not cond_col or not tgt_col:
                missing = [c for c, r in [(cond_col_raw, cond_col), (tgt_col_raw, tgt_col)] if not r]
                return RuleDecompositionResult(
                    status=DecompositionStatus.INVALID,
                    original_rule=rule_text,
                    diagnostics=[f"Unknown column(s) '{missing}' in rule '{rule_text}'."],
                )
            cond = StructuredCondition(
                column=cond_col,
                operator=ComparisonOperator.EQ,
                value=cond_val,
            )
            action = StructuredAction(
                action_type=ActionType.CONDITIONAL_UPDATE,
                dataset_name=dataset_name,
                target_columns=[tgt_col],
                parameters={"condition": cond.model_dump(), "new_value": new_val},
                source=ActionSource.BUSINESS_RULE,
                extraction_method="deterministic",
                confidence=1.0,
            )
            return RuleDecompositionResult(
                status=DecompositionStatus.RESOLVED,
                actions=[action],
                extraction_method="deterministic",
                confidence=1.0,
                original_rule=rule_text,
            )

        return None

    @classmethod
    def _parse_multi_action(
        cls, text: str, dataset_name: str, available_columns: List[str]
    ) -> Optional[RuleDecompositionResult]:
        """
        Parses composite rules: e.g.
        "Trim EmployeeCode, pad with zeros to 8, then prefix EMP-"
        """
        # Multi-action pattern detection
        trim_m = re.search(r"\btrim\s+\[?([a-zA-Z0-9_\s]+)\]?", text, re.IGNORECASE)
        pad_m = re.search(r"pad\s+(?:with\s+(?:zeros?|['\"]?[0-9a-zA-Z]['\"]?)\s+)?to\s+(?:length\s+)?([0-9]+)", text, re.IGNORECASE)
        prefix_m = re.search(r"prefix\s+(?:with\s+)?['\"]?([a-zA-Z0-9_\-]+)['\"]?", text, re.IGNORECASE)

        if trim_m and pad_m and prefix_m:
            col_raw = trim_m.group(1)
            resolved_col = ColumnResolver.resolve_column(col_raw, available_columns)
            if not resolved_col:
                return RuleDecompositionResult(
                    status=DecompositionStatus.INVALID,
                    original_rule=text,
                    diagnostics=[f"Unknown target column '{col_raw}' in multi-action rule."],
                )

            pad_len = int(pad_m.group(1))
            prefix_val = prefix_m.group(1)

            # Build 3 sequential actions with explicit dependency chain!
            step1_id = f"step_{dataset_name}_trim_0"
            act1 = StructuredAction(
                action_type=ActionType.TRIM,
                dataset_name=dataset_name,
                target_columns=[resolved_col],
                parameters={"side": "both"},
                source=ActionSource.BUSINESS_RULE,
                extraction_method="deterministic",
                metadata={"step_id": step1_id},
            )

            step2_id = f"step_{dataset_name}_pad_1"
            act2 = StructuredAction(
                action_type=ActionType.PAD,
                dataset_name=dataset_name,
                target_columns=[resolved_col],
                parameters={"length": pad_len, "fill_character": "0", "side": "left"},
                source=ActionSource.BUSINESS_RULE,
                extraction_method="deterministic",
                metadata={"step_id": step2_id, "depends_on_step_ids": [step1_id]},
            )

            step3_id = f"step_{dataset_name}_prefix_add_2"
            act3 = StructuredAction(
                action_type=ActionType.PREFIX_ADD,
                dataset_name=dataset_name,
                target_columns=[resolved_col],
                parameters={"prefix": prefix_val, "only_if_missing": True},
                source=ActionSource.BUSINESS_RULE,
                extraction_method="deterministic",
                metadata={"step_id": step3_id, "depends_on_step_ids": [step2_id]},
            )

            return RuleDecompositionResult(
                status=DecompositionStatus.RESOLVED,
                actions=[act1, act2, act3],
                extraction_method="deterministic",
                confidence=1.0,
                original_rule=text,
            )

        return None
