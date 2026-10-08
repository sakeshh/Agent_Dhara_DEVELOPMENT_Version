from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from agent.compiler.rules.normalized_rules import (
    ColumnBusinessRule,
    ConstraintType,
    CrossFieldBusinessRule,
    NormalizedBusinessRules,
    PolicyRule,
    RuleAuthority,
    RuleSourceReference,
    TransformationBusinessRule,
    ViolationDisposition,
    AUTHORITY_WEIGHTS,
)

logger = logging.getLogger(__name__)


class BusinessRuleNormalizer:
    """
    Normalizes business rules from all heterogeneous sources (YAML, requirements docs,
    UI/chat inputs, legacy domain_rules, and assessment business_rules) into the
    canonical NormalizedBusinessRules model before planning.
    """

    @classmethod
    def normalize(
        cls,
        raw_sources: Union[Dict[str, Any], List[Any]],
        default_dataset: str = "dbo.DefaultDataset",
        source_name: str = "business_rules",
    ) -> NormalizedBusinessRules:
        """
        Main entry point for business rule normalization.
        Accepts dicts, lists, or unified plan/assessment dictionaries.
        """
        never_drop_rows = False
        required_cols: List[str] = []
        column_rules: List[ColumnBusinessRule] = []
        cross_field_rules: List[CrossFieldBusinessRule] = []
        transformation_rules: List[TransformationBusinessRule] = []
        policy_rules: List[PolicyRule] = []
        source_refs: List[RuleSourceReference] = []

        if isinstance(raw_sources, list):
            for idx, item in enumerate(raw_sources):
                if isinstance(item, dict):
                    cls._parse_single_rule_dict(
                        item,
                        idx=idx,
                        default_dataset=default_dataset,
                        source_name=source_name,
                        column_rules=column_rules,
                        cross_field_rules=cross_field_rules,
                        transformation_rules=transformation_rules,
                        policy_rules=policy_rules,
                    )
            source_refs.append(RuleSourceReference(source_type=source_name))

        elif isinstance(raw_sources, dict):
            source_refs.append(RuleSourceReference(source_type=source_name))

            # 1. Global policy checks
            if raw_sources.get("never_drop_rows") is True:
                never_drop_rows = True
                policy_rules.append(
                    PolicyRule(
                        rule_id="POL-NEVER-DROP-001",
                        name="never_drop_rows",
                        value=True,
                        source=source_name,
                        description="Global governance policy prohibiting deletion of rows.",
                    )
                )

            # 2. required_columns
            raw_req = raw_sources.get("required_columns") or []
            if isinstance(raw_req, (list, tuple)):
                for c in raw_req:
                    c_clean = str(c).strip().strip("[]")
                    if c_clean and c_clean not in required_cols:
                        required_cols.append(c_clean)
                        column_rules.append(
                            ColumnBusinessRule(
                                rule_id=f"REQ-COL-{c_clean.upper()}",
                                dataset=default_dataset,
                                column=c_clean,
                                constraint_type=ConstraintType.REQUIRED_COLUMN,
                                on_violation_disposition=ViolationDisposition.REJECT,
                                authority=RuleAuthority.APPROVED_BUSINESS_RULE,
                                source=source_name,
                                description=f"Required column contract: {c_clean}",
                            )
                        )

            # 3. non_nullable constraints
            raw_non_null = raw_sources.get("non_nullable") or []
            if isinstance(raw_non_null, (list, tuple)):
                for c in raw_non_null:
                    c_clean = str(c).strip().strip("[]")
                    if c_clean:
                        # Constraint-only by default: do NOT authorize FILL_NULLS automatically!
                        column_rules.append(
                            ColumnBusinessRule(
                                rule_id=f"NN-{c_clean.upper()}",
                                dataset=default_dataset,
                                column=c_clean,
                                constraint_type=ConstraintType.NOT_NULL,
                                on_violation_disposition=ViolationDisposition.FLAG,
                                explicit_remediation_action=None,  # Pure constraint
                                authority=RuleAuthority.APPROVED_BUSINESS_RULE,
                                source=source_name,
                                description=f"Non-nullable constraint on {c_clean}",
                            )
                        )

            # 4. valid_values
            raw_valid_vals = raw_sources.get("valid_values") or {}
            if isinstance(raw_valid_vals, dict):
                for col_name, vv_spec in raw_valid_vals.items():
                    c_clean = str(col_name).strip().strip("[]")
                    if isinstance(vv_spec, dict):
                        allowed = vv_spec.get("allowed") or []
                        aliases = vv_spec.get("aliases") or {}
                        disposition_str = str(vv_spec.get("unknown_disposition") or "FLAG").upper()
                        disp = ViolationDisposition.FLAG if disposition_str != "REJECT" else ViolationDisposition.REJECT

                        # Explicit remediation action is 'map' only if aliases mapping is provided
                        explicit_action = "map" if aliases else None

                        column_rules.append(
                            ColumnBusinessRule(
                                rule_id=f"VV-{c_clean.upper()}",
                                dataset=default_dataset,
                                column=c_clean,
                                constraint_type=ConstraintType.ALLOWED_VALUES,
                                parameters={
                                    "allowed_values": list(allowed),
                                    "aliases": dict(aliases),
                                    "unknown_disposition": disp.value,
                                },
                                on_violation_disposition=disp,
                                explicit_remediation_action=explicit_action,
                                authority=RuleAuthority.APPROVED_BUSINESS_RULE,
                                source=source_name,
                                description=f"Valid values specification for {c_clean}",
                            )
                        )
                    elif isinstance(vv_spec, (list, tuple)):
                        column_rules.append(
                            ColumnBusinessRule(
                                rule_id=f"VV-{c_clean.upper()}",
                                dataset=default_dataset,
                                column=c_clean,
                                constraint_type=ConstraintType.ALLOWED_VALUES,
                                parameters={"allowed_values": list(vv_spec), "aliases": {}},
                                on_violation_disposition=ViolationDisposition.FLAG,
                                explicit_remediation_action=None,  # No aliases -> Pure constraint
                                authority=RuleAuthority.APPROVED_BUSINESS_RULE,
                                source=source_name,
                                description=f"Valid values list for {c_clean}",
                            )
                        )

            # 5. custom_assertions
            raw_assertions = raw_sources.get("custom_assertions") or []
            if isinstance(raw_assertions, (list, tuple)):
                for idx, ass in enumerate(raw_assertions):
                    if isinstance(ass, str):
                        cls._parse_assertion_string(
                            ass,
                            idx=idx,
                            dataset=default_dataset,
                            source_name=source_name,
                            column_rules=column_rules,
                            cross_field_rules=cross_field_rules,
                        )

            # 6. domain_rules (transformations / explicit domain operations)
            raw_domain_rules = raw_sources.get("domain_rules") or {}
            if isinstance(raw_domain_rules, dict):
                for rule_k, rule_v in raw_domain_rules.items():
                    cls._parse_domain_rule(
                        rule_k,
                        rule_v,
                        default_dataset=default_dataset,
                        source_name=source_name,
                        transformation_rules=transformation_rules,
                        column_rules=column_rules,
                    )

            # 7. Explicit rules list inside dict: rules: [...]
            if "rules" in raw_sources and isinstance(raw_sources["rules"], list):
                for idx, item in enumerate(raw_sources["rules"]):
                    if isinstance(item, dict):
                        cls._parse_single_rule_dict(
                            item,
                            idx=idx,
                            default_dataset=default_dataset,
                            source_name=source_name,
                            column_rules=column_rules,
                            cross_field_rules=cross_field_rules,
                            transformation_rules=transformation_rules,
                            policy_rules=policy_rules,
                        )

        # Deduplicate and sort rules by authority and priority
        deduped_column_rules = cls._deduplicate_column_rules(column_rules)

        return NormalizedBusinessRules(
            never_drop_rows=never_drop_rows,
            required_columns=tuple(required_cols),
            column_rules=tuple(deduped_column_rules),
            cross_field_rules=tuple(cross_field_rules),
            transformation_rules=tuple(transformation_rules),
            policy_rules=tuple(policy_rules),
            source_references=tuple(source_refs),
        )

    @classmethod
    def _parse_single_rule_dict(
        cls,
        item: Dict[str, Any],
        idx: int,
        default_dataset: str,
        source_name: str,
        column_rules: List[ColumnBusinessRule],
        cross_field_rules: List[CrossFieldBusinessRule],
        transformation_rules: List[TransformationBusinessRule],
        policy_rules: List[PolicyRule],
    ) -> None:
        rule_id = item.get("rule_id") or f"RULE-{idx:04d}"
        dataset = item.get("dataset") or default_dataset
        column = item.get("column")
        authority_str = str(item.get("authority") or "APPROVED_BUSINESS_RULE").upper()
        authority = getattr(RuleAuthority, authority_str, RuleAuthority.APPROVED_BUSINESS_RULE)
        enabled = item.get("enabled", True)
        priority = item.get("priority", 100)
        source = item.get("source") or source_name

        # Violation disposition
        on_viol = item.get("on_violation") or {}
        disp_str = str(on_viol.get("disposition") or "FLAG").upper()
        disp = getattr(ViolationDisposition, disp_str, ViolationDisposition.FLAG)
        explicit_action = on_viol.get("action")
        approval_req = bool(on_viol.get("approval_required", False) or item.get("approval_required", False))

        constraint_data = item.get("constraint") or {}
        ctype_str = str(constraint_data.get("type") or item.get("type") or "").upper()

        if ctype_str == "RANGE" or ("minimum" in constraint_data or "maximum" in constraint_data):
            col_name = str(column or "").strip("[]")
            column_rules.append(
                ColumnBusinessRule(
                    rule_id=rule_id,
                    dataset=dataset,
                    column=col_name,
                    constraint_type=ConstraintType.RANGE,
                    parameters={
                        "minimum": constraint_data.get("minimum"),
                        "maximum": constraint_data.get("maximum"),
                    },
                    on_violation_disposition=disp,
                    explicit_remediation_action=explicit_action,
                    authority=authority,
                    source=source,
                    priority=priority,
                    approval_required=approval_req,
                    enabled=enabled,
                    description=item.get("description") or f"Range constraint on {col_name}",
                )
            )
        elif ctype_str in ("NOT_NULL", "NON_NULLABLE"):
            col_name = str(column or "").strip("[]")
            def_v = constraint_data.get("default_value")
            if def_v is None:
                def_v = on_viol.get("fill_value")
            if def_v is None:
                def_v = item.get("default_value")

            column_rules.append(
                ColumnBusinessRule(
                    rule_id=rule_id,
                    dataset=dataset,
                    column=col_name,
                    constraint_type=ConstraintType.NOT_NULL,
                    parameters={"default_value": def_v},
                    on_violation_disposition=disp,
                    explicit_remediation_action=explicit_action,
                    authority=authority,
                    source=source,
                    priority=priority,
                    approval_required=approval_req,
                    enabled=enabled,
                    description=item.get("description") or f"Not null constraint on {col_name}",
                )
            )
        elif ctype_str in ("ALLOWED_VALUES", "VALID_VALUES"):
            col_name = str(column or "").strip("[]")
            column_rules.append(
                ColumnBusinessRule(
                    rule_id=rule_id,
                    dataset=dataset,
                    column=col_name,
                    constraint_type=ConstraintType.ALLOWED_VALUES,
                    parameters={
                        "allowed_values": constraint_data.get("allowed") or constraint_data.get("allowed_values") or [],
                        "aliases": constraint_data.get("aliases") or {},
                    },
                    on_violation_disposition=disp,
                    explicit_remediation_action=explicit_action,
                    authority=authority,
                    source=source,
                    priority=priority,
                    approval_required=approval_req,
                    enabled=enabled,
                    description=item.get("description") or f"Allowed values constraint on {col_name}",
                )
            )
        elif ctype_str == "POLICY" or str(item.get("type", "")).upper() == "POLICY":
            policy_rules.append(
                PolicyRule(
                    rule_id=rule_id,
                    name=item.get("name") or rule_id,
                    value=item.get("value", True),
                    dataset=dataset,
                    authority=authority,
                    source=source,
                    enabled=enabled,
                    description=item.get("description") or "",
                )
            )
        elif ctype_str == "TRANSFORMATION" or item.get("action_type"):
            act_type = str(item.get("action_type") or constraint_data.get("action_type") or "").lower()
            transformation_rules.append(
                TransformationBusinessRule(
                    rule_id=rule_id,
                    dataset=dataset,
                    column=str(column or "").strip("[]"),
                    action_type=act_type,
                    parameters=dict(item.get("parameters") or constraint_data.get("parameters") or {}),
                    authority=authority,
                    source=source,
                    priority=priority,
                    enabled=enabled,
                    description=item.get("description") or "",
                )
            )

    @classmethod
    def _parse_assertion_string(
        cls,
        assertion: str,
        idx: int,
        dataset: str,
        source_name: str,
        column_rules: List[ColumnBusinessRule],
        cross_field_rules: List[CrossFieldBusinessRule],
    ) -> None:
        """
        Parses custom assertion strings into typed constraints.
        E.g. 'Salary >= 1000 AND Salary <= 500000' -> ConstraintType.RANGE
        """
        ass_clean = assertion.strip()
        # Range check pattern: Col >= X AND Col <= Y
        range_match = re.search(
            r"([a-zA-Z0-9_\[\]]+)\s*>=\s*([0-9\.]+)\s+AND\s+\1\s*<=\s*([0-9\.]+)",
            ass_clean,
            re.IGNORECASE,
        )
        if range_match:
            col_name = range_match.group(1).strip("[]")
            min_val = float(range_match.group(2))
            max_val = float(range_match.group(3))
            column_rules.append(
                ColumnBusinessRule(
                    rule_id=f"ASSERT-RANGE-{idx:03d}",
                    dataset=dataset,
                    column=col_name,
                    constraint_type=ConstraintType.RANGE,
                    parameters={"minimum": min_val, "maximum": max_val},
                    on_violation_disposition=ViolationDisposition.FLAG,
                    explicit_remediation_action=None,  # Constraint only by default!
                    authority=RuleAuthority.APPROVED_BUSINESS_RULE,
                    source=source_name,
                    description=f"Range assertion: {ass_clean}",
                )
            )
            return

        # General assertion condition
        cols_found = [c.strip("[]") for c in re.findall(r"\[?([a-zA-Z_][a-zA-Z0-9_]*)\]?", ass_clean)]
        if len(cols_found) == 1:
            column_rules.append(
                ColumnBusinessRule(
                    rule_id=f"ASSERT-COL-{idx:03d}",
                    dataset=dataset,
                    column=cols_found[0],
                    constraint_type=ConstraintType.ASSERTION,
                    parameters={"expression": ass_clean},
                    on_violation_disposition=ViolationDisposition.FLAG,
                    explicit_remediation_action=None,
                    authority=RuleAuthority.APPROVED_BUSINESS_RULE,
                    source=source_name,
                    description=f"Column assertion: {ass_clean}",
                )
            )
        elif len(cols_found) > 1:
            cross_field_rules.append(
                CrossFieldBusinessRule(
                    rule_id=f"ASSERT-CROSS-{idx:03d}",
                    dataset=dataset,
                    columns=tuple(sorted(list(set(cols_found)))),
                    expression=ass_clean,
                    on_violation_disposition=ViolationDisposition.FLAG,
                    explicit_remediation_action=None,
                    authority=RuleAuthority.APPROVED_BUSINESS_RULE,
                    source=source_name,
                    description=f"Cross-field assertion: {ass_clean}",
                )
            )

    @classmethod
    def _parse_domain_rule(
        cls,
        rule_key: str,
        rule_val: Any,
        default_dataset: str,
        source_name: str,
        transformation_rules: List[TransformationBusinessRule],
        column_rules: List[ColumnBusinessRule],
    ) -> None:
        ds = default_dataset
        col = None
        if "." in rule_key:
            parts = rule_key.split(".")
            ds = parts[0]
            col = parts[1]
        else:
            col = rule_key

        rule_text = ""
        params: Dict[str, Any] = {}
        if isinstance(rule_val, dict):
            rule_text = rule_val.get("rule") or rule_val.get("expression") or rule_val.get("text") or ""
            ds = rule_val.get("dataset") or ds
            col = rule_val.get("column") or col
            params = dict(rule_val.get("parameters") or {})
        elif isinstance(rule_val, str):
            rule_text = rule_val

        col_clean = str(col or "").strip("[]")

        # Deterministic text pattern matching for explicit transformations
        # 1. Prefix: 'Prefix EmployeeCode with EMP-'
        prefix_match = re.search(r"prefix\s+(?:column\s+)?([a-zA-Z0-9_\[\]]+)?\s*with\s*['\"]?([^'\"\s]+)['\"]?", rule_text, re.IGNORECASE)
        if prefix_match:
            target_col = prefix_match.group(1).strip("[]") if prefix_match.group(1) else col_clean
            pfx = prefix_match.group(2)
            transformation_rules.append(
                TransformationBusinessRule(
                    rule_id=f"TR-PREFIX-{target_col.upper()}",
                    dataset=ds,
                    column=target_col,
                    action_type="prefix_add",
                    parameters={"prefix": pfx, "only_if_missing": True},
                    source=source_name,
                    description=f"Prefix {target_col} with {pfx}",
                )
            )
            return

        # 2. Pad: 'Pad AccountCode to 8 digits' or 'Pad X to Y with 0'
        pad_match = re.search(r"pad\s+(?:column\s+)?([a-zA-Z0-9_\[\]]+)?\s*to\s*(\d+)\s*(?:digits|chars)?(?:\s*with\s*['\"]?([^'\"\s]+)['\"]?)?", rule_text, re.IGNORECASE)
        if pad_match:
            target_col = pad_match.group(1).strip("[]") if pad_match.group(1) else col_clean
            t_len = int(pad_match.group(2))
            pad_char = pad_match.group(3) or "0"
            transformation_rules.append(
                TransformationBusinessRule(
                    rule_id=f"TR-PAD-{target_col.upper()}",
                    dataset=ds,
                    column=target_col,
                    action_type="pad",
                    parameters={"target_length": t_len, "pad_char": pad_char, "side": "left"},
                    source=source_name,
                    description=f"Pad {target_col} to {t_len} with {pad_char}",
                )
            )
            return

        # Fallback: if rule_text describes an assertion
        if "must be" in rule_text or ">" in rule_text or "<" in rule_text:
            cls._parse_assertion_string(
                rule_text,
                idx=0,
                dataset=ds,
                source_name=source_name,
                column_rules=column_rules,
                cross_field_rules=[],
            )

    @classmethod
    def _deduplicate_column_rules(
        cls,
        column_rules: List[ColumnBusinessRule],
    ) -> List[ColumnBusinessRule]:
        """
        Deduplicates column rules for the same (dataset, column, constraint_type).
        Higher authority and priority take precedence.
        """
        groups: Dict[Tuple[str, str, ConstraintType], List[ColumnBusinessRule]] = {}
        for r in column_rules:
            key = (r.dataset.lower(), r.column.lower(), r.constraint_type)
            groups.setdefault(key, []).append(r)

        deduped: List[ColumnBusinessRule] = []
        for key, r_list in groups.items():
            if len(r_list) == 1:
                deduped.append(r_list[0])
            else:
                # Sort by authority weight descending, then priority descending
                sorted_rules = sorted(
                    r_list,
                    key=lambda r: (AUTHORITY_WEIGHTS.get(r.authority, 100), r.priority),
                    reverse=True,
                )
                deduped.append(sorted_rules[0])

        return deduped
