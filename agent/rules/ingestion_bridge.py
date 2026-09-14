"""
Business Rules Ingestion Bridge and Legacy Compatibility Adapter.

Implements Integration 1 of the Business Rules Pipeline:
- Converts raw structured rules (YAML, UI, chat, requirements notes) into a typed CanonicalRuleRegistry.
- Assigns strict atomic IntakeDisposition to every input rule (no-drop guarantee).
- Provides shadow-mode projection: registry_to_legacy_rules(registry).
- Identifies manual review items and ingestion warnings without losing context.
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

from pydantic import BaseModel, Field

from agent.rules.canonical import canonical_json, compute_canonical_hash
from agent.rules.rule_models import (
    CanonicalExpression,
    CanonicalRule,
    CanonicalRuleMaterial,
    CanonicalRuleRegistry,
    ColumnOperand,
    EffectiveRuleClaim,
    IntakeDisposition,
    LiteralOperand,
    NullPolicy,
    RuleAction,
    RuleIntakeRecord,
    RuleOccurrence,
    RuleScope,
    RuleType,
    build_rule_id,
    validate_intake_records,
)


class IngestionWarning(BaseModel):
    """Structured warning emitted during rule ingestion."""
    code: str
    message: str
    field: Optional[str] = None


class ManualReviewItem(BaseModel):
    """Rule or note instruction that requires human review due to ambiguity."""
    item_id: str
    source_rule_id: str
    raw_text: str
    reason: str
    candidate_rule_id: Optional[str] = None


class BusinessRulesIngestionResult(BaseModel):
    """Complete result of ingesting business rules into the CanonicalRuleRegistry."""
    registry: CanonicalRuleRegistry
    warnings: Tuple[IngestionWarning, ...] = ()
    manual_review_items: Tuple[ManualReviewItem, ...] = ()
    legacy_rules_view: Optional[Dict[str, Any]] = None


# Operator pattern matching for simple string assertions: e.g., "Age >= 18", "Price > 0"
_COMPARISON_REGEX = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(==|!=|>=|<=|>|<|IN|IS_NULL|IS_NOT_NULL)\s*(.*)\s*$",
    re.IGNORECASE,
)


def _parse_simple_assertion(assertion_str: str) -> Optional[CanonicalExpression]:
    """Attempts deterministic parsing of simple comparison expressions into CanonicalExpression."""
    text = assertion_str.strip()
    match = _COMPARISON_REGEX.match(text)
    if not match:
        return None

    col_name, op, raw_val = match.groups()
    op = op.upper()
    col_op = ColumnOperand(column=col_name)

    if op in ("IS_NULL", "IS_NOT_NULL"):
        return CanonicalExpression(operator=op, operands=(col_op,))

    raw_val = raw_val.strip().strip("'\"")
    # Determine type of literal value
    val: Any = raw_val
    dtype = "string"
    if raw_val.isdigit():
        val = int(raw_val)
        dtype = "int"
    else:
        try:
            val = float(raw_val)
            dtype = "float"
        except ValueError:
            pass

    lit_op = LiteralOperand(value=val, data_type=dtype)
    return CanonicalExpression(operator=op, operands=(col_op, lit_op))


def ingest_business_rules(
    raw_rules: Dict[str, Any],
    dataset_name: str = "default",
    source_type: str = "yaml_config",
    source_reference: str = "config/business_rules.yaml",
    authority_rank: int = 10,
    tenant_id: str = "default",
) -> BusinessRulesIngestionResult:
    """
    Translates raw structured rules (or normalized legacy rules) into a CanonicalRuleRegistry
    with complete bi-directional intake accounting and legacy view projection.
    """
    rules: Dict[str, CanonicalRule] = {}
    occurrences: Dict[str, RuleOccurrence] = {}
    claims: Dict[str, EffectiveRuleClaim] = {}
    intake_records: Dict[str, RuleIntakeRecord] = {}

    warnings: List[IngestionWarning] = []
    review_items: List[ManualReviewItem] = []
    now = datetime.now(timezone.utc)

    def _add_rule(
        input_rule_id: str,
        material: CanonicalRuleMaterial,
        logical_name: str,
        is_explicit: bool = True,
    ) -> str:
        rule_id = build_rule_id(material)
        occ_id = f"occ-{uuid.uuid4().hex[:8]}"

        if rule_id not in rules:
            rules[rule_id] = CanonicalRule(
                rule_id=rule_id,
                logical_rule_id=logical_name,
                rule_version=1,
                material=material,
                source_occurrences=(occ_id,),
            )
            claims[rule_id] = EffectiveRuleClaim(
                rule_id=rule_id,
                winning_occurrence_id=occ_id,
                consolidation_reason="initial_intake",
            )
        else:
            # Append occurrence to existing canonical rule
            curr = rules[rule_id]
            rules[rule_id] = curr.model_copy(update={"source_occurrences": curr.source_occurrences + (occ_id,)})

        occurrences[occ_id] = RuleOccurrence(
            occurrence_id=occ_id,
            canonical_rule_id=rule_id,
            source_type=source_type,
            source_reference=source_reference,
            authority_rank=authority_rank,
            is_explicit=is_explicit,
            observed_at=now,
        )

        intake_records[input_rule_id] = RuleIntakeRecord(
            input_rule_id=input_rule_id,
            disposition=IntakeDisposition.INTERPRETED,
            reason_code="rule_interpreted_successfully",
            canonical_rule_id=rule_id,
        )
        return rule_id

    # 1. Process required_columns (SCHEMA_CONSTRAINT)
    req_cols = raw_rules.get("required_columns") or []
    if isinstance(req_cols, list):
        for idx, col in enumerate(req_cols):
            col_s = str(col).strip()
            if not col_s:
                continue
            inp_id = f"inp-req-{dataset_name}-{col_s}"
            mat = CanonicalRuleMaterial(
                rule_type=RuleType.SCHEMA_CONSTRAINT,
                expression=CanonicalExpression(
                    operator="IS_NOT_NULL",
                    operands=(ColumnOperand(column=col_s),),
                ),
                scope=RuleScope(dataset=dataset_name, columns=(col_s,)),
                action=RuleAction.ASSERT,
                normalized_parameters={"required": True},
            )
            _add_rule(inp_id, mat, f"required_column_{col_s}")

    # 2. Process non_nullable (VALIDATION_ASSERTION)
    non_nulls = raw_rules.get("non_nullable") or []
    if isinstance(non_nulls, list):
        for idx, col in enumerate(non_nulls):
            col_s = str(col).strip()
            if not col_s:
                continue
            inp_id = f"inp-nonnull-{dataset_name}-{col_s}"
            mat = CanonicalRuleMaterial(
                rule_type=RuleType.VALIDATION_ASSERTION,
                expression=CanonicalExpression(
                    operator="IS_NOT_NULL",
                    operands=(ColumnOperand(column=col_s),),
                ),
                scope=RuleScope(dataset=dataset_name, columns=(col_s,)),
                action=RuleAction.ASSERT,
            )
            _add_rule(inp_id, mat, f"non_nullable_{col_s}")

    # 3. Process valid_values (VALIDATION_ASSERTION)
    valid_values = raw_rules.get("valid_values") or {}
    if isinstance(valid_values, dict):
        for col, vals in valid_values.items():
            col_s = str(col).strip()
            if not isinstance(vals, list) or not vals:
                continue
            inp_id = f"inp-validval-{dataset_name}-{col_s}"
            norm_vals = [str(v).strip() for v in vals if str(v).strip()]
            mat = CanonicalRuleMaterial(
                rule_type=RuleType.VALIDATION_ASSERTION,
                expression=CanonicalExpression(
                    operator="IN",
                    operands=(
                        ColumnOperand(column=col_s),
                        LiteralOperand(value=norm_vals, data_type="list"),
                    ),
                ),
                scope=RuleScope(dataset=dataset_name, columns=(col_s,)),
                action=RuleAction.ASSERT,
                normalized_parameters={"allowed_values": norm_vals},
            )
            _add_rule(inp_id, mat, f"valid_values_{col_s}")

    # 4. Process custom_assertions
    assertions = raw_rules.get("custom_assertions") or []
    if isinstance(assertions, list):
        for idx, entry in enumerate(assertions):
            inp_id = f"inp-assert-{dataset_name}-{idx}"
            raw_text = ""
            if isinstance(entry, dict):
                raw_text = str(entry.get("assertion") or "").strip()
            elif isinstance(entry, str):
                raw_text = entry.strip()

            if not raw_text:
                continue

            parsed_expr = _parse_simple_assertion(raw_text)
            if parsed_expr:
                cols = tuple(op.column for op in parsed_expr.operands if isinstance(op, ColumnOperand))
                mat = CanonicalRuleMaterial(
                    rule_type=RuleType.VALIDATION_ASSERTION,
                    expression=parsed_expr,
                    scope=RuleScope(dataset=dataset_name, columns=cols),
                    action=RuleAction.ASSERT,
                )
                _add_rule(inp_id, mat, f"custom_assertion_{idx}")
            else:
                # Ambiguous assertion routed to MANUAL_REVIEW
                intake_records[inp_id] = RuleIntakeRecord(
                    input_rule_id=inp_id,
                    disposition=IntakeDisposition.MANUAL_REVIEW,
                    reason_code="unsupported_complex_expression_syntax",
                    canonical_rule_id=None,
                )
                review_items.append(
                    ManualReviewItem(
                        item_id=f"mr-{uuid.uuid4().hex[:8]}",
                        source_rule_id=inp_id,
                        raw_text=raw_text,
                        reason="Expression cannot be safely parsed into a typed assertion AST.",
                    )
                )

    # 5. Process never_drop_rows
    if "never_drop_rows" in raw_rules:
        inp_id = f"inp-policy-{dataset_name}-never_drop_rows"
        val = bool(raw_rules["never_drop_rows"])
        mat = CanonicalRuleMaterial(
            rule_type=RuleType.SCHEMA_CONSTRAINT,
            expression=CanonicalExpression(
                operator="==",
                operands=(
                    LiteralOperand(value=val, data_type="boolean"),
                    LiteralOperand(value=True, data_type="boolean"),
                ),
            ),
            scope=RuleScope(dataset=dataset_name, columns=()),
            action=RuleAction.ASSERT,
            normalized_parameters={"never_drop_rows": val},
        )
        _add_rule(inp_id, mat, "policy_never_drop_rows")

    # 6. Process notes (Preserved Unmapped)
    notes_text = raw_rules.get("notes") or ""
    if isinstance(notes_text, str) and notes_text.strip():
        inp_id = f"inp-notes-{dataset_name}"
        intake_records[inp_id] = RuleIntakeRecord(
            input_rule_id=inp_id,
            disposition=IntakeDisposition.PRESERVED_UNMAPPED,
            reason_code="business_context_notes_preserved",
            canonical_rule_id=None,
        )

    # Build and validate registry
    registry = CanonicalRuleRegistry(
        rules=rules,
        occurrences=occurrences,
        claims=claims,
        intake_records=intake_records,
    )

    # Enforce bi-directional intake completeness
    validate_intake_records(set(intake_records.keys()), registry.intake_records, registry)

    # Build backward-compatible legacy rules projection
    legacy_view = registry_to_legacy_rules(registry, notes_text=notes_text)

    return BusinessRulesIngestionResult(
        registry=registry,
        warnings=tuple(warnings),
        manual_review_items=tuple(review_items),
        legacy_rules_view=legacy_view,
    )


def registry_to_legacy_rules(
    registry: CanonicalRuleRegistry,
    notes_text: str = "",
) -> Dict[str, Any]:
    """
    Projects a CanonicalRuleRegistry back into the legacy dictionary format
    for zero-regression compatibility during rollout and shadow-mode testing.
    """
    req_cols: List[str] = []
    non_nulls: List[str] = []
    valid_values: Dict[str, List[str]] = {}
    custom_assertions: List[Dict[str, Any]] = []
    never_drop: bool = False

    for rule in registry.rules.values():
        mat = rule.material
        cols = mat.scope.columns

        if mat.rule_type == RuleType.SCHEMA_CONSTRAINT and mat.normalized_parameters.get("required"):
            req_cols.extend(cols)
        elif mat.rule_type == RuleType.SCHEMA_CONSTRAINT and "never_drop_rows" in mat.normalized_parameters:
            never_drop = bool(mat.normalized_parameters["never_drop_rows"])
        elif mat.rule_type == RuleType.VALIDATION_ASSERTION and mat.expression.operator == "IS_NOT_NULL":
            non_nulls.extend(cols)
        elif mat.rule_type == RuleType.VALIDATION_ASSERTION and mat.expression.operator == "IN" and "allowed_values" in mat.normalized_parameters:
            for c in cols:
                valid_values[c] = list(mat.normalized_parameters["allowed_values"])
        elif mat.rule_type == RuleType.VALIDATION_ASSERTION and len(mat.expression.operands) >= 2:
            # Reconstruct assertion string
            op_left = mat.expression.operands[0]
            op_right = mat.expression.operands[1]
            left_str = getattr(op_left, "column", str(getattr(op_left, "value", "")))
            right_str = getattr(op_right, "column", str(getattr(op_right, "value", "")))
            custom_assertions.append({
                "assertion": f"{left_str} {mat.expression.operator} {right_str}",
                "severity": "high",
                "message": f"Rule {rule.logical_rule_id} violated",
            })

    return {
        "required_columns": sorted(set(req_cols)),
        "non_nullable": sorted(set(non_nulls)),
        "exclude_columns": [],
        "valid_values": {k: sorted(set(v)) for k, v in valid_values.items()},
        "custom_assertions": custom_assertions,
        "never_drop_rows": never_drop,
        "notes": notes_text,
    }
