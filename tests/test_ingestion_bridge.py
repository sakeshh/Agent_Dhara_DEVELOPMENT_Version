"""
Tests for agent.rules.ingestion_bridge:
Ingesting legacy dictionaries/YAML into CanonicalRuleRegistry,
bi-directional intake verification, shadow-mode compatibility projection,
and manual review routing.
"""
import pytest

from agent.rules.ingestion_bridge import ingest_business_rules, registry_to_legacy_rules
from agent.rules.rule_models import IntakeDisposition, RuleType


def test_ingest_structured_rules_success():
    raw_input = {
        "required_columns": ["CustomerID", "Email"],
        "non_nullable": ["Status", "CreatedAt"],
        "valid_values": {
            "Status": ["Active", "Inactive", "Pending"]
        },
        "custom_assertions": [
            {"assertion": "Age >= 18", "severity": "high", "message": "Adult check"},
            {"assertion": "Price > 0", "severity": "medium", "message": "Positive price"},
        ],
        "never_drop_rows": True,
        "notes": "Ensure every customer has a valid email and account status.",
    }

    res = ingest_business_rules(raw_input, dataset_name="DimCustomer")
    reg = res.registry

    # 1. Verify Intake Records
    assert len(reg.intake_records) == 9  # 2 req + 2 non-null + 1 validval + 2 asserts + 1 policy + 1 note
    assert reg.intake_records["inp-notes-DimCustomer"].disposition == IntakeDisposition.PRESERVED_UNMAPPED
    assert reg.intake_records["inp-req-DimCustomer-CustomerID"].disposition == IntakeDisposition.INTERPRETED

    # 2. Verify Canonical Rules count
    # Each interpreted rule creates 1 canonical rule (8 interpreted rules total)
    assert len(reg.rules) == 8  # 2 req + 2 non-null + 1 validval + 2 asserts + 1 policy
    assert len(reg.claims) == 8

    # 3. Verify Legacy Compatibility Projection
    legacy = res.legacy_rules_view
    assert legacy is not None
    assert set(legacy["required_columns"]) == {"CustomerID", "Email"}
    assert set(legacy["non_nullable"]) == {"Status", "CreatedAt"}
    assert legacy["valid_values"]["Status"] == ["Active", "Inactive", "Pending"]
    assert legacy["never_drop_rows"] is True
    assert legacy["notes"] == raw_input["notes"]


def test_ambiguous_assertion_routed_to_manual_review():
    raw_input = {
        "custom_assertions": [
            {"assertion": "Email.str.extract(r'([A-Z]+)').len() > 3", "severity": "low"}
        ]
    }
    res = ingest_business_rules(raw_input, dataset_name="DimCustomer")
    assert len(res.manual_review_items) == 1
    assert "cannot be safely parsed" in res.manual_review_items[0].reason

    # Intake record disposition is MANUAL_REVIEW
    intake_rec = res.registry.intake_records["inp-assert-DimCustomer-0"]
    assert intake_rec.disposition == IntakeDisposition.MANUAL_REVIEW
    assert intake_rec.canonical_rule_id is None


def test_duplicate_rule_occurrence_consolidation():
    # Ingesting same rule twice with different occurrences
    raw1 = {"required_columns": ["CustomerID"]}
    res1 = ingest_business_rules(raw1, dataset_name="DimCustomer", source_reference="yaml_defaults")
    res2 = ingest_business_rules(raw1, dataset_name="DimCustomer", source_reference="ui_override")

    rule_id_1 = list(res1.registry.rules.keys())[0]
    rule_id_2 = list(res2.registry.rules.keys())[0]
    # Same semantic rule produces identical rule_id
    assert rule_id_1 == rule_id_2


def test_business_rules_loader_ingestion_bridge():
    from agent.business_rules_loader import ingest_business_rules_for_datasets

    ui_input = {
        "required_columns": ["OrderID"],
        "valid_values": {"Status": ["Shipped", "Delivered"]},
    }
    res = ingest_business_rules_for_datasets(
        ui_or_chat_rules=ui_input,
        dataset_names=["FactOrders"],
        tenant_id="tenant-test",
    )
    assert len(res.registry.rules) >= 2
    assert "inp-req-FactOrders-OrderID" in res.registry.intake_records
    assert res.legacy_rules_view is not None
    assert "OrderID" in res.legacy_rules_view["required_columns"]
