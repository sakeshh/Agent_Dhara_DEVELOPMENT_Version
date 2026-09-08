"""Unit and contract tests for PR 4.6 Report Wiring & Remediation Units.

Verifies:
- One report card exists per remediation unit
- Source findings consolidated under unit
- Configuration actions render configuration patch proposals without row-remediation code
- Dynamic header governance breakdown reflects Standard, AI-assisted, Catalog fallback, Config
- Fallback cards never render AI-assisted language
- Pending approval renders provisional code preview
- Decision required renders when no option is selected
"""

import pytest
from agent.service_layer import build_html_report


def test_remediation_unit_consolidation_and_provenance():
    """Consolidated card groups multiple findings on same field without duplicate cards."""
    findings = [
        {
            "id": "rec-email-missing",
            "dataset": "dbo.Customers_Raw",
            "column": "Email",
            "issue_type": "missing_values",
            "authority_source": "CATALOG_FALLBACK",
            "ai_assistance_status": "unavailable",
            "suggested_action": "nullify_clean_value",
            "severity": "high",
            "priority": 1,
            "lifecycle": {"approval_status": "pending"},
        },
        {
            "id": "rec-email-format",
            "dataset": "dbo.Customers_Raw",
            "column": "Email",
            "issue_type": "invalid_format",
            "authority_source": "CATALOG_FALLBACK",
            "ai_assistance_status": "unavailable",
            "suggested_action": "nullify_clean_value",
            "severity": "medium",
            "priority": 2,
            "lifecycle": {"approval_status": "pending"},
        },
    ]

    report_payload = {
        "datasets": {
            "dbo.Customers_Raw": {
                "row_count": 100,
                "column_count": 2,
                "columns": {"Email": {"dtype": "object", "semantic_type": "email"}},
            }
        },
        "data_quality_issues": {
            "datasets": {
                "dbo.Customers_Raw": {
                    "issues": [
                        {"type": "missing_values", "column": "Email", "severity": "high"},
                        {"type": "invalid_format", "column": "Email", "severity": "medium"},
                    ]
                }
            },
            "global_issues": {},
        },
        "dq_recommendations": {
            "findings": findings,
            "metadata": {"total_findings": 2},
        },
        "governance": {"assessment_run_id": "run-ru-01"},
    }

    html = build_html_report(report_payload)

    # Invariant: Exactly one card for Email field remediation unit
    assert html.count('<span class="pill-dataset">dbo.Customers_Raw.Email</span>') == 1
    # Both issues shown inside the single card
    assert "<code>missing_values</code>" in html
    assert "<code>invalid_format</code>" in html
    # Fallback badge rendered, no AI hallucination
    assert "Catalog option, AI ranking unavailable" in html
    assert "AI-assisted option" not in html
    # Provisional banner rendered for pending approval
    assert "Provisional code preview for the selected option." in html
    assert "This transformation is not approved or authorized for execution." in html


def test_configuration_action_card_renders_yaml_patch():
    """Metadata manifest mismatch produces configuration patch proposal, not row code."""
    config_finding = {
        "id": "rec-config-gstin",
        "dataset": "dbo.Customers",
        "column": "gstin",
        "issue_type": "metadata_manifest_mismatch",
        "authority_source": "DETERMINISTIC_REGISTRY",
        "severity": "medium",
        "priority": 2,
        "message": "Manifest field gstin is not present in dbo.Customers.",
        "lifecycle": {"approval_status": "pending"},
    }

    report_payload = {
        "datasets": {"dbo.Customers": {"row_count": 50, "column_count": 5, "columns": {}}},
        "data_quality_issues": {"datasets": {}, "global_issues": {}},
        "dq_recommendations": {
            "findings": [config_finding],
            "metadata": {"total_findings": 1},
        },
        "governance": {"assessment_run_id": "run-cfg-01"},
    }

    html = build_html_report(report_payload)

    # Invariant: Renders configuration card with patch proposal
    assert "Configuration Action" in html
    assert "dbo.Customers [Config: gstin]" in html
    assert "Proposed Configuration Patch (config/metadata_manifest.yaml)" in html
    assert "field_name: &quot;gstin&quot;" in html or 'field_name: "gstin"' in html
    assert "Schema configuration action does not mutate dataset rows." in html
    # Invariant: Suppresses PySpark / Pandas row transformations
    assert "PySpark DataFrame Remediation (Canonical Field Pipeline)" not in html


def test_dynamic_governance_header_breakdown():
    """Header shows recommendation mode and breakdown across standard, AI, catalog, and config."""
    findings = [
        {
            "id": "f1",
            "dataset": "dbo.TableA",
            "column": "Col1",
            "issue_type": "missing_values",
            "authority_source": "DETERMINISTIC_REGISTRY",
            "suggested_action": "nullify_clean_value",
        },
        {
            "id": "f2",
            "dataset": "dbo.TableB",
            "column": "Col2",
            "issue_type": "negative_values",
            "authority_source": "LLM_ADVISORY",
            "ai_assistance_status": "available",
            "suggested_action": "flag_only",
        },
        {
            "id": "f3",
            "dataset": "dbo.TableC",
            "column": "Col3",
            "issue_type": "impossible_date",
            "authority_source": "CATALOG_FALLBACK",
            "ai_assistance_status": "unavailable",
            "suggested_action": "nullify_clean_value",
        },
        {
            "id": "f4",
            "dataset": "dbo.TableD",
            "column": "Col4",
            "issue_type": "metadata_manifest_mismatch",
            "authority_source": "DETERMINISTIC_REGISTRY",
        },
    ]

    report_payload = {
        "datasets": {},
        "data_quality_issues": {"datasets": {}, "global_issues": {}},
        "dq_recommendations": {"findings": findings, "metadata": {"total_findings": 4}},
        "governance": {"assessment_run_id": "run-dyn-01"},
    }

    html = build_html_report(report_payload)

    assert "Mixed (AI-assisted + Standard Catalog)" in html
    assert "Standard: 1" in html
    assert "AI-assisted: 1" in html
    assert "Catalog fallback: 1" in html
    assert "Config: 1" in html
