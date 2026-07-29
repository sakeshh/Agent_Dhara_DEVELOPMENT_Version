from __future__ import annotations
import pytest
from typing import Dict, Any

from agent.etl_pipeline.rule_provenance import TaggedRule, RuleProvenance, RuleConflict
from agent.etl_pipeline.conflict_detector import detect_conflicts
from agent.etl_pipeline.business_rules import to_tagged_rules
from agent.etl_pipeline.manual_review_catalog import enrich_manual_review_item


def test_rule_provenance_enum():
    assert RuleProvenance.BUSINESS_RULE == 1
    assert RuleProvenance.SEMANTIC_LAYER == 2
    assert RuleProvenance.AUTO_DETECTED == 3
    # Business rule is highest priority (lowest enum value)
    assert RuleProvenance.BUSINESS_RULE < RuleProvenance.SEMANTIC_LAYER
    assert RuleProvenance.SEMANTIC_LAYER < RuleProvenance.AUTO_DETECTED


def test_detect_conflicts_no_conflict():
    rules = [
        TaggedRule(
            dataset="users",
            column="email",
            issue_type="invalid_email",
            action="sanitize_email",
            provenance=RuleProvenance.AUTO_DETECTED,
            source_detail="Auto email scanner"
        )
    ]
    resolved, conflicts = detect_conflicts(rules)
    assert len(resolved) == 1
    assert resolved[0].action == "sanitize_email"
    assert len(conflicts) == 0


def test_detect_conflicts_same_action():
    rules = [
        TaggedRule(
            dataset="users",
            column="email",
            issue_type="invalid_email",
            action="sanitize_email",
            provenance=RuleProvenance.AUTO_DETECTED,
            source_detail="Auto email scanner"
        ),
        TaggedRule(
            dataset="users",
            column="email",
            issue_type="invalid_email",
            action="sanitize_email",
            provenance=RuleProvenance.SEMANTIC_LAYER,
            source_detail="Semantic contract"
        )
    ]
    resolved, conflicts = detect_conflicts(rules)
    assert len(resolved) == 1
    assert resolved[0].provenance == RuleProvenance.SEMANTIC_LAYER  # Semantic > Auto
    assert resolved[0].action == "sanitize_email"
    assert len(conflicts) == 0


def test_detect_conflicts_different_actions():
    rules = [
        TaggedRule(
            dataset="users",
            column="phone",
            issue_type="invalid_phone",
            action="normalize_phone",
            provenance=RuleProvenance.AUTO_DETECTED,
            source_detail="Auto phone scanner"
        ),
        TaggedRule(
            dataset="users",
            column="phone",
            issue_type="invalid_phone",
            action="mask_phone",
            provenance=RuleProvenance.BUSINESS_RULE,
            source_detail="Business rule: mask phone numbers"
        )
    ]
    resolved, conflicts = detect_conflicts(rules)
    assert len(resolved) == 1
    # Business rule (mask_phone) wins over Auto (normalize_phone)
    assert resolved[0].provenance == RuleProvenance.BUSINESS_RULE
    assert resolved[0].action == "mask_phone"
    
    assert len(conflicts) == 1
    conflict = conflicts[0]
    assert conflict.dataset == "users"
    assert conflict.column == "phone"
    assert conflict.issue_type == "invalid_phone"
    assert conflict.auto_resolved is True
    assert conflict.resolution == "mask_phone"
    assert len(conflict.rules) == 2


def test_enrich_manual_review_item_with_conflict():
    rules = [
        TaggedRule(
            dataset="users",
            column="phone",
            issue_type="invalid_phone",
            action="normalize_phone",
            provenance=RuleProvenance.AUTO_DETECTED,
            source_detail="Auto phone scanner"
        ),
        TaggedRule(
            dataset="users",
            column="phone",
            issue_type="invalid_phone",
            action="mask_phone",
            provenance=RuleProvenance.BUSINESS_RULE,
            source_detail="Business rule: mask phone"
        )
    ]
    _, conflicts = detect_conflicts(rules)
    conflict = conflicts[0]
    
    item = {
        "dataset": "users",
        "column": "phone",
        "issue_type": "invalid_phone",
        "severity": "high",
        "message": "Rule conflict",
    }
    
    enriched = enrich_manual_review_item(item, conflict=conflict)
    assert "resolution_options" in enriched
    assert len(enriched["resolution_options"]) >= 2
    
    # Check that options have correct labels and business option is pre-selected (recommended)
    options = enriched["resolution_options"]
    
    # Business option should be recommended
    bus_opt = next(o for o in options if "Business" in o["label"])
    assert bus_opt["action"] == "mask_phone"
    assert bus_opt["recommended"] is True
    
    # Auto option should NOT be recommended
    auto_opt = next(o for o in options if "Auto" in o["label"])
    assert auto_opt["action"] == "normalize_phone"
    assert auto_opt["recommended"] is False


def test_business_rules_to_tagged_rules():
    rules_dict = {
        "non_nullable": ["users.email", "phone"],
        "never_drop_rows": True,
        "valid_values": {
            "users.status": ["active", "inactive"]
        }
    }
    
    tagged = to_tagged_rules(rules_dict, "users")
    assert len(tagged) == 3
    
    # 1. users.email -> nulls rule
    email_rule = next(t for t in tagged if t.column == "email")
    assert email_rule.issue_type == "nulls"
    assert email_rule.action == "fill_nulls_simple"  # never_drop_rows is True
    assert email_rule.provenance == RuleProvenance.BUSINESS_RULE
    
    # 2. phone -> nulls rule
    phone_rule = next(t for t in tagged if t.column == "phone")
    assert phone_rule.issue_type == "nulls"
    assert phone_rule.action == "fill_nulls_simple"
    
    # 3. users.status -> valid values
    status_rule = next(t for t in tagged if t.column == "status")
    assert status_rule.issue_type == "invalid_lookup_value"
    assert status_rule.action == "replace_values"
    assert status_rule.metadata["valid_values"] == ["active", "inactive"]
