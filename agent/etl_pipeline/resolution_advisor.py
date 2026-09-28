"""
ResolutionAdvisor: Advisory engine layered on top of compiler plan validation.
Explains what legal alternatives exist for any validation failure, without duplicating
authoritative validation logic.
"""

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional
import uuid


@dataclass
class RecoveryActionOption:
    action: str
    label: str
    risk: str = "LOW"  # LOW, MEDIUM, HIGH
    recommended: bool = False
    description: str = ""
    parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProhibitedActionNotice:
    action: str
    reason: str


@dataclass
class RecoveryAdvisory:
    issue_id: str
    item_id: Optional[str]
    dataset: str
    column: Optional[str]
    code: str
    severity: str
    user_choice: Optional[str]
    title: str
    explanation: str
    recommended_action: Optional[str]
    allowed_actions: List[RecoveryActionOption]
    prohibited_actions: List[ProhibitedActionNotice]
    auto_fixable: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "issue_id": self.issue_id,
            "item_id": self.item_id,
            "dataset": self.dataset,
            "column": self.column,
            "code": self.code,
            "severity": self.severity,
            "user_choice": self.user_choice,
            "title": self.title,
            "explanation": self.explanation,
            "recommended_action": self.recommended_action,
            "allowed_actions": [asdict(a) for a in self.allowed_actions],
            "prohibited_actions": [asdict(p) for p in self.prohibited_actions],
            "auto_fixable": self.auto_fixable,
        }


class ResolutionAdvisor:
    """
    Advisory layer that consumes PlanValidationIssue instances emitted by authoritative
    validators (validate_promoted_plan, validate_etl_plan) and generates actionable,
    structured recovery options for the user interface.
    """

    def advise(
        self,
        issues: List[Any],
        *,
        plan: Optional[Dict[str, Any]] = None,
        assessment: Optional[Dict[str, Any]] = None,
        business_rules: Optional[Dict[str, Any]] = None,
    ) -> List[RecoveryAdvisory]:
        advisories: List[RecoveryAdvisory] = []
        rules = business_rules or (plan.get("business_rules") if plan else {}) or {}
        never_drop = bool(rules.get("never_drop_rows"))

        for idx, issue in enumerate(issues):
            code = getattr(issue, "code", "UNKNOWN_ERROR")
            col = getattr(issue, "column", None)
            ds = getattr(issue, "dataset_id", "default") or "default"
            attempted_act = getattr(issue, "attempted_action", None) or getattr(issue, "action", None)
            item_id = getattr(issue, "item_id", None) or f"issue_{idx + 1}"
            issue_id = f"adv_{uuid.uuid4().hex[:8]}"

            if code == "ILLEGAL_KEY_IMPUTATION":
                advisories.append(self._advise_key_imputation(issue_id, item_id, ds, col, attempted_act))
            elif code in ("UNSAFE_ROW_DELETION", "NEVER_DROP_ROWS_VIOLATION") or "never_drop_rows" in str(getattr(issue, "message", "")).lower():
                advisories.append(self._advise_never_drop_violation(issue_id, item_id, ds, col, attempted_act))
            elif code == "MISSING_REQUIRED_PARAMETER":
                advisories.append(self._advise_missing_parameter(issue_id, item_id, ds, col, attempted_act, getattr(issue, "message", "")))
            elif code == "INCOMPLETE_DEDUPLICATION_POLICY":
                advisories.append(self._advise_dedup_policy(issue_id, item_id, ds, col, attempted_act))
            elif code == "CONFLICTING_WRITE_COLUMN":
                advisories.append(self._advise_conflicting_write(issue_id, item_id, ds, col, getattr(issue, "message", "")))
            elif code in ("AMBIGUOUS_LEGACY_ACTION", "UNKNOWN_ACTION"):
                advisories.append(self._advise_legacy_action(issue_id, item_id, ds, col, attempted_act))
            elif code == "PHASE_MISMATCH":
                advisories.append(self._advise_phase_mismatch(issue_id, item_id, ds, col, attempted_act))
            else:
                advisories.append(self._advise_generic(issue_id, item_id, ds, col, code, getattr(issue, "message", ""), attempted_act))

        return advisories

    def _advise_key_imputation(self, issue_id: str, item_id: str, ds: str, col: Optional[str], attempted: Optional[str]) -> RecoveryAdvisory:
        return RecoveryAdvisory(
            issue_id=issue_id,
            item_id=item_id,
            dataset=ds,
            column=col,
            code="ILLEGAL_KEY_IMPUTATION",
            severity="BLOCKING",
            user_choice=attempted or "impute_numeric",
            title=f"Primary/Foreign Key '{col}' Cannot Be Synthetically Imputed",
            explanation=(
                f"Column '{col}' serves as a primary, foreign, or business identifier. "
                "Synthesizing artificial values (such as 0 or mean) corrupts relational integrity and unique constraints."
            ),
            recommended_action="preserve_null_and_flag",
            allowed_actions=[
                RecoveryActionOption(
                    action="preserve_null_and_flag",
                    label="Preserve NULL & Flag Row",
                    risk="LOW",
                    recommended=True,
                    description=f"Retain original null values and generate '{col}_validation_status' audit column.",
                ),
                RecoveryActionOption(
                    action="quarantine_null_rows",
                    label="Route Rows with Missing Key to Quarantine",
                    risk="MEDIUM",
                    recommended=False,
                    description=f"Direct invalid rows into '{ds}_Quarantine' table without destroying raw data.",
                ),
            ],
            prohibited_actions=[
                ProhibitedActionNotice(
                    action=attempted or "impute_numeric",
                    reason="Synthetic value imputation is strictly prohibited on key and identifier columns.",
                ),
                ProhibitedActionNotice(
                    action="fill_null_with_constant",
                    reason="Constant value replacement on key columns causes multiple records to share identical artificial keys.",
                ),
            ],
            auto_fixable=True,
        )

    def _advise_never_drop_violation(self, issue_id: str, item_id: str, ds: str, col: Optional[str], attempted: Optional[str]) -> RecoveryAdvisory:
        return RecoveryAdvisory(
            issue_id=issue_id,
            item_id=item_id,
            dataset=ds,
            column=col,
            code="UNSAFE_ROW_DELETION",
            severity="BLOCKING",
            user_choice=attempted or "drop_rows",
            title="Row Deletion Prohibited by Active Business Policy",
            explanation="The dataset business rule 'never_drop_rows' is active. Dropping rows is blocked to ensure zero silent data loss.",
            recommended_action="preserve_null_and_flag",
            allowed_actions=[
                RecoveryActionOption(
                    action="preserve_null_and_flag",
                    label="Preserve Data & Flag Violations",
                    risk="LOW",
                    recommended=True,
                    description="Keep all rows intact and emit a boolean validation flag column.",
                ),
                RecoveryActionOption(
                    action="quarantine_null_rows",
                    label="Move Non-Compliant Rows to Quarantine",
                    risk="MEDIUM",
                    recommended=False,
                    description="Preserve rows in a designated quarantine table for offline remediation.",
                ),
            ],
            prohibited_actions=[
                ProhibitedActionNotice(
                    action="drop_rows",
                    reason="Dataset invariant 'never_drop_rows' forbids destructive row filtering.",
                )
            ],
            auto_fixable=True,
        )

    def _advise_missing_parameter(self, issue_id: str, item_id: str, ds: str, col: Optional[str], attempted: Optional[str], msg: str) -> RecoveryAdvisory:
        return RecoveryAdvisory(
            issue_id=issue_id,
            item_id=item_id,
            dataset=ds,
            column=col,
            code="MISSING_REQUIRED_PARAMETER",
            severity="BLOCKING",
            user_choice=attempted,
            title=f"Action '{attempted}' Missing Mandatory Parameter",
            explanation=msg or f"Action '{attempted}' requires explicit parameters before deterministic code generation can proceed.",
            recommended_action=attempted,
            allowed_actions=[
                RecoveryActionOption(
                    action=attempted or "fill_null_with_constant",
                    label=f"Provide Missing Parameters for '{attempted}'",
                    risk="LOW",
                    recommended=True,
                    description="Supply constant value or strategy parameters.",
                ),
                RecoveryActionOption(
                    action="preserve_null_and_flag",
                    label="Switch to Safe Preserve & Flag",
                    risk="LOW",
                    recommended=False,
                    description="Safely bypass missing parameter by flagging without mutating.",
                ),
            ],
            prohibited_actions=[],
            auto_fixable=False,
        )

    def _advise_dedup_policy(self, issue_id: str, item_id: str, ds: str, col: Optional[str], attempted: Optional[str]) -> RecoveryAdvisory:
        return RecoveryAdvisory(
            issue_id=issue_id,
            item_id=item_id,
            dataset=ds,
            column=col,
            code="INCOMPLETE_DEDUPLICATION_POLICY",
            severity="BLOCKING",
            user_choice=attempted or "deduplicate",
            title="Deduplication Requires Key Columns and Survivorship Policy",
            explanation="Deduplication cannot execute without defining key columns and survivorship policy (e.g. keep_first, keep_last).",
            recommended_action="deduplicate",
            allowed_actions=[
                RecoveryActionOption(
                    action="deduplicate",
                    label="Deduplicate (Keep First)",
                    risk="MEDIUM",
                    recommended=True,
                    parameters={"survivorship_policy": "keep_first", "key_columns": [col] if col else ["id"]},
                    description="Deduplicate by keeping the earliest encountered record per key.",
                ),
                RecoveryActionOption(
                    action="preserve_null_and_flag",
                    label="Flag Duplicates Without Dropping",
                    risk="LOW",
                    recommended=False,
                    description="Add duplicate indicator flag without deleting rows.",
                ),
            ],
            prohibited_actions=[],
            auto_fixable=True,
        )

    def _advise_conflicting_write(self, issue_id: str, item_id: str, ds: str, col: Optional[str], msg: str) -> RecoveryAdvisory:
        return RecoveryAdvisory(
            issue_id=issue_id,
            item_id=item_id,
            dataset=ds,
            column=col,
            code="CONFLICTING_WRITE_COLUMN",
            severity="BLOCKING",
            user_choice=None,
            title=f"Conflicting Writes to Column '{col}'",
            explanation=msg or f"Multiple steps write to output column '{col}'. Each column may only have one active write producer.",
            recommended_action="rename_output_column",
            allowed_actions=[
                RecoveryActionOption(
                    action="rename_output_column",
                    label=f"Rename Target Column to '{col}_v2'",
                    risk="LOW",
                    recommended=True,
                    description="Avoid collision by writing secondary transformation to distinct column name.",
                ),
            ],
            prohibited_actions=[],
            auto_fixable=True,
        )

    def _advise_legacy_action(self, issue_id: str, item_id: str, ds: str, col: Optional[str], attempted: Optional[str]) -> RecoveryAdvisory:
        return RecoveryAdvisory(
            issue_id=issue_id,
            item_id=item_id,
            dataset=ds,
            column=col,
            code="AMBIGUOUS_LEGACY_ACTION",
            severity="BLOCKING",
            user_choice=attempted,
            title=f"Legacy Action '{attempted}' Requires Explicit Specification",
            explanation=f"Action '{attempted}' is an unconstrained legacy action. It must be mapped to a deterministic Phase 1 operator.",
            recommended_action="preserve_null_and_flag",
            allowed_actions=[
                RecoveryActionOption(
                    action="preserve_null_and_flag",
                    label="Preserve and Flag",
                    risk="LOW",
                    recommended=True,
                ),
                RecoveryActionOption(
                    action="quarantine_null_rows",
                    label="Quarantine Rows",
                    risk="MEDIUM",
                    recommended=False,
                ),
            ],
            prohibited_actions=[],
            auto_fixable=True,
        )

    def _advise_phase_mismatch(self, issue_id: str, item_id: str, ds: str, col: Optional[str], attempted: Optional[str]) -> RecoveryAdvisory:
        return RecoveryAdvisory(
            issue_id=issue_id,
            item_id=item_id,
            dataset=ds,
            column=col,
            code="PHASE_MISMATCH",
            severity="BLOCKING",
            user_choice=attempted,
            title=f"Action '{attempted}' Belongs to Phase 2",
            explanation="The selected action is a multi-dataset Phase 2 join/enrichment operator, but this plan is in Phase 1 cleaning mode.",
            recommended_action="preserve_null_and_flag",
            allowed_actions=[
                RecoveryActionOption(
                    action="preserve_null_and_flag",
                    label="Defer to Phase 2 & Flag in Phase 1",
                    risk="LOW",
                    recommended=True,
                ),
            ],
            prohibited_actions=[],
            auto_fixable=True,
        )

    def _advise_generic(self, issue_id: str, item_id: str, ds: str, col: Optional[str], code: str, msg: str, attempted: Optional[str]) -> RecoveryAdvisory:
        return RecoveryAdvisory(
            issue_id=issue_id,
            item_id=item_id,
            dataset=ds,
            column=col,
            code=code,
            severity="BLOCKING",
            user_choice=attempted,
            title=f"Validation Block: {code.replace('_', ' ').title()}",
            explanation=msg or f"Step validation failed on dataset '{ds}' column '{col}'.",
            recommended_action="preserve_null_and_flag",
            allowed_actions=[
                RecoveryActionOption(
                    action="preserve_null_and_flag",
                    label="Preserve Raw Values & Flag",
                    risk="LOW",
                    recommended=True,
                    description="Safely proceed with non-destructive audit flag.",
                ),
            ],
            prohibited_actions=[],
            auto_fixable=True,
        )
