"""
Unit tests for Remediation Capability Registry and Router (PR 1).
Validates:
1. Invariants of capability registry (risk, auto-fix, action allowlists).
2. Issue type canonicalization and aliases.
3. Unknown issue fallback safety (never auto-fixes, requires human review).
4. Contextual remediation routing (negative values, nulls, duplicates).
5. Dual impact boundary gates (% and row counts).
6. Action-specific impact policies (whitespace vs standard).
7. Decoupled lifecycle status initialization.
8. Stable cluster key generation.
"""
import pytest

from agent.etl_pipeline.remediation_capability_registry import (
    REMEDIATION_CAPABILITY_REGISTRY,
    UNKNOWN_CAPABILITY_FALLBACK,
    get_capability_entry,
    validate_capability_registry,
)
from agent.etl_pipeline.remediation_router import (
    REASON_CONTEXTUAL_REMEDIATION,
    REASON_IMPACT_PERCENTAGE_EXCEEDED,
    REASON_IMPACT_ROWS_EXCEEDED,
    REASON_STANDARD_LOW_RISK,
    REASON_UNKNOWN_ISSUE_TYPE,
    route_finding,
)


def test_registry_invariants():
    """Verify all registry entries satisfy safety constraints."""
    assert validate_capability_registry() is True
    for itype, cap in REMEDIATION_CAPABILITY_REGISTRY.items():
        # High/critical risk must NEVER be auto-fix eligible
        if cap.risk_level in ("high", "critical"):
            assert cap.auto_fix_eligible is False, f"{itype} cannot be auto-fix eligible"
        # Only high remediation certainty can be auto-fix eligible
        if cap.auto_fix_eligible:
            assert cap.remediation_certainty == "high", f"{itype} must have high remediation certainty"
            assert cap.row_destructive is False, f"{itype} cannot be destructive"


def test_standard_low_risk_routing_under_threshold():
    """Whitespace under threshold routes to standard remediation and is auto-fix eligible."""
    finding = {
        "issue_type": "whitespace",
        "column": "customer_name",
        "count": 500,
        "affected_percentage": 2.5,
    }
    result = route_finding(finding, dataset="Customers", total_rows=20000)

    assert result.recommendation_category == "standard_remediation"
    assert result.recommendation_status == "standard_available"
    assert result.approval_status == "not_required"
    assert result.auto_fix_eligible is True
    assert REASON_STANDARD_LOW_RISK in result.reason_codes
    assert result.cluster_key == ("Customers", "text_formatting")
    assert result.lifecycle["approval_status"] == "not_required"


def test_impact_percentage_exceeded_triggers_approval():
    """Case inconsistency exceeding the 5% threshold triggers approval required."""
    finding = {
        "issue_type": "case_inconsistency",
        "column": "status",
        "count": 600,
        "affected_percentage": 6.0,  # > 5.0% limit
    }
    result = route_finding(finding, dataset="Orders", total_rows=10000)

    assert result.recommendation_category == "standard_remediation"
    assert result.auto_fix_eligible is False
    assert result.approval_status == "pending"
    assert REASON_IMPACT_PERCENTAGE_EXCEEDED in result.reason_codes


def test_impact_rows_exceeded_triggers_approval():
    """Standard fix affecting > 10,000 rows triggers approval required even if percentage is small."""
    finding = {
        "issue_type": "case_inconsistency",
        "column": "category",
        "count": 12000,              # > 10,000 rows limit
        "affected_percentage": 1.2,  # <= 5%
    }
    result = route_finding(finding, dataset="Transactions", total_rows=1000000)

    assert result.auto_fix_eligible is False
    assert result.approval_status == "pending"
    assert REASON_IMPACT_ROWS_EXCEEDED in result.reason_codes


def test_whitespace_action_specific_higher_threshold():
    """Whitespace has a higher threshold (10% / 50k rows) defined by TRIM_IMPACT_POLICY."""
    finding = {
        "issue_type": "whitespace",
        "column": "comment",
        "count": 30000,              # > 10k, but <= 50k
        "affected_percentage": 6.0,  # > 5%, but <= 10%
    }
    result = route_finding(finding, dataset="Reviews", total_rows=500000)

    assert result.auto_fix_eligible is True
    assert result.approval_status == "not_required"


def test_unknown_issue_defaults_to_safe_review():
    """Unregistered/unknown issue types must never auto-fix and require human review."""
    finding = {
        "issue_type": "quantum_field_anomaly",
        "column": "sensor_x",
        "count": 10,
        "affected_percentage": 0.1,
    }
    result = route_finding(finding, dataset="Telemetry", total_rows=10000)

    assert result.recommendation_category == "decision_required"
    assert result.recommendation_status == "ai_options_available"
    assert result.approval_status == "pending"
    assert result.auto_fix_eligible is False
    assert REASON_UNKNOWN_ISSUE_TYPE in result.reason_codes
    assert result.canonical_issue_type == "quantum_field_anomaly"


def test_contextual_issue_routing_negative_values():
    """Negative values have deterministic detection but low remediation certainty -> decision required."""
    finding = {
        "issue_type": "negative_values",
        "column": "Price",
        "count": 45,
        "affected_percentage": 0.05,
    }
    result = route_finding(finding, dataset="Transactions", total_rows=90000)

    assert result.canonical_issue_type == "cross_field_non_negative"
    assert result.recommendation_category == "decision_required"
    assert result.auto_fix_eligible is False
    assert result.approval_status == "pending"
    assert REASON_CONTEXTUAL_REMEDIATION in result.reason_codes
    assert result.cluster_key == ("Transactions", "financial_measures")


def test_contextual_issue_routing_nulls():
    """Nulls route to decision required because remediation depends on business policy."""
    finding = {
        "issue_type": "null_values",
        "column": "middle_name",
        "count": 1500,
        "affected_percentage": 15.0,
    }
    result = route_finding(finding, dataset="Customers", total_rows=10000)

    assert result.canonical_issue_type == "nulls"
    assert result.recommendation_category == "decision_required"
    assert result.auto_fix_eligible is False
    assert result.approval_status == "pending"
    assert result.cluster_key == ("Customers", "completeness")


def test_critical_referential_integrity_issue():
    """Orphan foreign keys must never auto-fix and must require approval."""
    finding = {
        "issue_type": "orphan_foreign_keys",
        "column": "customer_id",
        "count": 12,
        "affected_percentage": 0.01,
    }
    result = route_finding(finding, dataset="Orders", total_rows=120000)

    assert result.canonical_issue_type == "orphan_foreign_key_rows"
    assert result.recommendation_category == "decision_required"
    assert result.capability.risk_level == "critical"
    assert result.auto_fix_eligible is False
    assert result.approval_status == "pending"
    assert result.cluster_key == ("Orders", "referential_integrity")


def test_duplicate_rows_destructive_requires_approval():
    """Duplicate rows detection has low remediation certainty and row destruction -> approval required."""
    finding = {
        "issue_type": "duplicate_rows",
        "column": None,
        "count": 20,
        "affected_percentage": 0.2,
    }
    result = route_finding(finding, dataset="Customers", total_rows=10000)

    assert result.canonical_issue_type == "exact_duplicate_rows"
    assert result.capability.row_destructive is True
    assert result.auto_fix_eligible is False
    assert result.approval_status == "pending"


def test_alias_normalization():
    """Verify raw profiler aliases map correctly to canonical registry items."""
    assert route_finding({"issue_type": "null_values"}).canonical_issue_type == "nulls"
    assert route_finding({"issue_type": "duplicate_rows"}).canonical_issue_type == "exact_duplicate_rows"
    assert route_finding({"issue_type": "orphan_foreign_keys"}).canonical_issue_type == "orphan_foreign_key_rows"
    assert route_finding({"issue_type": "invalid_number"}).canonical_issue_type == "invalid_numeric"


def test_decoupled_lifecycle_and_to_dict():
    """RoutingResult.to_dict() contains structured decoupled lifecycle states."""
    finding = {
        "issue_type": "whitespace",
        "column": "first_name",
        "count": 100,
        "affected_percentage": 1.0,
    }
    result = route_finding(finding, dataset="Users", total_rows=10000)
    data = result.to_dict()

    assert "lifecycle" in data
    assert data["lifecycle"]["recommendation_status"] == "standard_available"
    assert data["lifecycle"]["approval_status"] == "not_required"
    assert data["lifecycle"]["codegen_status"] == "not_started"
    assert data["lifecycle"]["validation_status"] == "not_started"
    assert data["lifecycle"]["execution_status"] == "not_started"
    assert data["auto_fix_eligible"] is True
    assert data["cluster_key"] == ["Users", "text_formatting"]
    assert "evaluated_policy" in data
    assert "observed" in data
    assert data["registry_version"] == "v1.0"
    assert "primary_reason_code" in data


def test_exact_threshold_boundary_percentage():
    """
    Validate exact inclusive boundary for percentage:
    <= 5.000% is eligible.
    > 5.000% is disqualified.
    """
    # 4.999% -> eligible
    res_under = route_finding(
        {"issue_type": "case_inconsistency", "column": "status", "count": 4999, "affected_percentage": 4.999},
        dataset="Orders",
        total_rows=100000,
    )
    assert res_under.auto_fix_eligible is True
    assert res_under.approval_status == "not_required"

    # 5.000% -> eligible (inclusive boundary)
    res_exact = route_finding(
        {"issue_type": "case_inconsistency", "column": "status", "count": 5000, "affected_percentage": 5.000},
        dataset="Orders",
        total_rows=100000,
    )
    assert res_exact.auto_fix_eligible is True
    assert res_exact.approval_status == "not_required"

    # 5.001% -> disqualified (> 5.0%)
    res_over = route_finding(
        {"issue_type": "case_inconsistency", "column": "status", "count": 5001, "affected_percentage": 5.001},
        dataset="Orders",
        total_rows=100000,
    )
    assert res_over.auto_fix_eligible is False
    assert res_over.approval_status == "pending"
    assert REASON_IMPACT_PERCENTAGE_EXCEEDED in res_over.reason_codes


def test_exact_threshold_boundary_rows():
    """
    Validate exact inclusive boundary for row count:
    <= 10,000 rows is eligible.
    > 10,000 rows is disqualified.
    """
    # 9,999 rows -> eligible
    res_under = route_finding(
        {"issue_type": "case_inconsistency", "column": "status", "count": 9999, "affected_percentage": 1.0},
        dataset="Orders",
        total_rows=1000000,
    )
    assert res_under.auto_fix_eligible is True
    assert res_under.approval_status == "not_required"

    # 10,000 rows -> eligible (inclusive boundary)
    res_exact = route_finding(
        {"issue_type": "case_inconsistency", "column": "status", "count": 10000, "affected_percentage": 1.0},
        dataset="Orders",
        total_rows=1000000,
    )
    assert res_exact.auto_fix_eligible is True
    assert res_exact.approval_status == "not_required"

    # 10,001 rows -> disqualified (> 10,000 rows)
    res_over = route_finding(
        {"issue_type": "case_inconsistency", "column": "status", "count": 10001, "affected_percentage": 1.0},
        dataset="Orders",
        total_rows=1000000,
    )
    assert res_over.auto_fix_eligible is False
    assert res_over.approval_status == "pending"
    assert REASON_IMPACT_ROWS_EXCEEDED in res_over.reason_codes


def test_routing_evidence_preservation():
    """Verify RoutingResult preserves full routing evidence for UI explanations."""
    finding = {
        "issue_type": "case_inconsistency",
        "column": "status",
        "count": 12540,
        "affected_percentage": 3.8,
    }
    result = route_finding(finding, dataset="Orders", total_rows=330000)
    data = result.to_dict()

    assert data["primary_reason_code"] == REASON_IMPACT_ROWS_EXCEEDED
    assert REASON_IMPACT_ROWS_EXCEEDED in data["reason_codes"]
    assert data["evaluated_policy"]["risk_level"] == "low"
    assert data["evaluated_policy"]["percentage_limit"] == 5.0
    assert data["evaluated_policy"]["row_limit"] == 10000
    assert data["observed"]["affected_percentage"] == 3.8
    assert data["observed"]["affected_rows"] == 12540
    assert data["registry_version"] == "v1.0"


def test_reason_code_precedence_multiple_disqualifications():
    """
    When multiple disqualifications occur (e.g. unknown issue + over percentage limit),
    all reason codes are preserved and the highest-precedence code becomes primary.
    """
    finding = {
        "issue_type": "completely_unregistered_anomaly",
        "column": "x",
        "count": 50000,
        "affected_percentage": 50.0,
    }
    result = route_finding(finding, dataset="Orders", total_rows=100000)
    data = result.to_dict()

    # Unknown issue type has precedence over percentage/row limits
    assert data["primary_reason_code"] == REASON_UNKNOWN_ISSUE_TYPE
    assert REASON_UNKNOWN_ISSUE_TYPE in data["reason_codes"]
    assert REASON_IMPACT_PERCENTAGE_EXCEEDED in data["reason_codes"]
    assert REASON_IMPACT_ROWS_EXCEEDED in data["reason_codes"]


def test_action_specific_thresholds_in_routing_evidence():
    """Verify that evaluated_policy correctly reflects action-specific policies rather than global hardcoding."""
    # 1. Whitespace (TRIM_IMPACT_POLICY: 10.0%, 50,000 rows)
    ws_result = route_finding({"issue_type": "whitespace", "column": "name", "count": 100, "affected_percentage": 1.0})
    ws_data = ws_result.to_dict()
    assert ws_data["evaluated_policy"]["percentage_limit"] == 10.0
    assert ws_data["evaluated_policy"]["row_limit"] == 50000

    # 2. Nullification for invalid email format (NULLIFY_IMPACT_POLICY: 2.0%, 5,000 rows)
    email_result = route_finding({"issue_type": "invalid_email", "column": "email", "count": 50, "affected_percentage": 0.5})
    email_data = email_result.to_dict()
    assert email_data["evaluated_policy"]["percentage_limit"] == 2.0
    assert email_data["evaluated_policy"]["row_limit"] == 5000

    # 3. Standard low risk (STANDARD_LOW_RISK_POLICY: 5.0%, 10,000 rows)
    std_result = route_finding({"issue_type": "case_inconsistency", "column": "status", "count": 200, "affected_percentage": 2.0})
    std_data = std_result.to_dict()
    assert std_data["evaluated_policy"]["percentage_limit"] == 5.0
    assert std_data["evaluated_policy"]["row_limit"] == 10000

    # 4. Contextual nulls (NEVER_AUTO_POLICY: 0.0%, 0 rows)
    null_result = route_finding({"issue_type": "null_values", "column": "phone", "count": 50, "affected_percentage": 0.5})
    null_data = null_result.to_dict()
    assert null_data["evaluated_policy"]["percentage_limit"] == 0.0
    assert null_data["evaluated_policy"]["row_limit"] == 0

    # 5. Unknown issue (NEVER_AUTO_POLICY: 0.0%, 0 rows)
    unk_result = route_finding({"issue_type": "unregistered_custom_anomaly", "column": "x", "count": 1, "affected_percentage": 0.01})
    unk_data = unk_result.to_dict()
    assert unk_data["evaluated_policy"]["percentage_limit"] == 0.0
    assert unk_data["evaluated_policy"]["row_limit"] == 0


