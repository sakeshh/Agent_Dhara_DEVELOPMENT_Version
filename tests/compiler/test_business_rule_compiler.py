from __future__ import annotations

import pytest

from agent.compiler.models.actions import ActionSource, StructuredAction
from agent.compiler.models.logical import ActionType
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.models.context import CompilerContext
from agent.compiler.planners.ingestion_bridge import IngestionBridge
from agent.compiler.rules.normalized_rules import (
    ColumnBusinessRule,
    ConstraintType,
    NormalizedBusinessRules,
    RuleAuthority,
    ViolationDisposition,
)
from agent.compiler.rules.normalizer import BusinessRuleNormalizer
from agent.compiler.rules.validator import BusinessRuleValidator
from agent.compiler.rules.compiler import BusinessRuleCompiler
from agent.errors import CompilationBlockedError
from agent.semantic_context import (
    ColumnCleaningContract,
    ColumnRole,
    EntityDefinition,
    SemanticCleaningPlan,
)


@pytest.fixture
def sample_dataset_profiles() -> dict[str, DatasetProfile]:
    return {
        "dbo.EmployeeMaster": DatasetProfile(
            dataset_name="dbo.EmployeeMaster",
            columns={
                "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
                "EmployeeCode": ColumnProfile(name="EmployeeCode", dtype="nvarchar(50)"),
                "Salary": ColumnProfile(name="Salary", dtype="numeric(18,2)"),
                "CustomerStatus": ColumnProfile(name="CustomerStatus", dtype="nvarchar(20)"),
                "HireDate": ColumnProfile(name="HireDate", dtype="datetime"),
                "DepartmentID": ColumnProfile(name="DepartmentID", dtype="int"),
            },
        )
    }


@pytest.fixture
def sample_semantic_plan() -> SemanticCleaningPlan:
    contracts = {
        "EmployeeID": ColumnCleaningContract(column_name="EmployeeID", role=ColumnRole.PRIMARY_KEY),
        "EmployeeCode": ColumnCleaningContract(column_name="EmployeeCode", role=ColumnRole.IDENTIFIER),
        "Salary": ColumnCleaningContract(column_name="Salary", role=ColumnRole.METRIC),
        "CustomerStatus": ColumnCleaningContract(column_name="CustomerStatus", role=ColumnRole.CATEGORICAL),
        "HireDate": ColumnCleaningContract(column_name="HireDate", role=ColumnRole.DATE),
        "DepartmentID": ColumnCleaningContract(column_name="DepartmentID", role=ColumnRole.FOREIGN_KEY),
    }
    entity = EntityDefinition(
        name="Employee",
        source_datasets=["dbo.EmployeeMaster"],
        canonical_key="EmployeeID",
        column_contracts=contracts,
    )
    return SemanticCleaningPlan(
        entities={"Employee": entity},
    )


class TestBusinessRuleCompilerAndValidator:
    """Comprehensive test suite for NormalizedBusinessRules, BusinessRuleValidator, and BusinessRuleCompiler."""

    def test_1_business_rules_and_domain_rules_normalize_identically(self):
        """Test 1: Structured business_rules and legacy domain_rules normalize into identical canonical models."""
        input_br = {
            "business_rules": {
                "rules": [
                    {
                        "rule_id": "HR-SALARY-001",
                        "dataset": "dbo.EmployeeMaster",
                        "column": "Salary",
                        "constraint": {"type": "RANGE", "minimum": 1000, "maximum": 500000},
                        "on_violation": {"disposition": "FLAG"},
                    }
                ]
            }
        }
        input_dr = {
            "domain_rules": {
                "dbo.EmployeeMaster.Salary": {
                    "rule": "Salary >= 1000 AND Salary <= 500000",
                }
            }
        }

        norm_br = BusinessRuleNormalizer.normalize(input_br["business_rules"], default_dataset="dbo.EmployeeMaster")
        norm_dr = BusinessRuleNormalizer.normalize(input_dr, default_dataset="dbo.EmployeeMaster")

        assert len(norm_br.column_rules) == 1
        assert len(norm_dr.column_rules) == 1

        r_br = norm_br.column_rules[0]
        r_dr = norm_dr.column_rules[0]

        assert r_br.column == r_dr.column == "Salary"
        assert r_br.constraint_type == r_dr.constraint_type == ConstraintType.RANGE
        assert float(r_br.parameters["minimum"]) == float(r_dr.parameters["minimum"]) == 1000.0
        assert float(r_br.parameters["maximum"]) == float(r_dr.parameters["maximum"]) == 500000.0
        assert r_br.is_constraint_only is True
        assert r_dr.is_constraint_only is True

    def test_2_required_missing_column_blocks_compilation(self, sample_dataset_profiles):
        """Test 2: A required source column missing from the dataset profile fails-closed with REQUIRED_COLUMN_MISSING."""
        raw_rules = {
            "required_columns": ["TaxID", "EmployeeID"],
        }
        norm_rules = BusinessRuleNormalizer.normalize(raw_rules, default_dataset="dbo.EmployeeMaster")

        val_res = BusinessRuleValidator.validate(
            rules=norm_rules,
            dataset_profiles=sample_dataset_profiles,
        )

        assert val_res.is_valid is False
        assert len(val_res.blocking_issues) >= 1
        issue = val_res.blocking_issues[0]
        assert issue.code == "REQUIRED_COLUMN_MISSING"
        assert issue.column == "TaxID"
        assert "TaxID" in issue.message

    def test_3_non_null_without_fill_policy_produces_validation_flag_not_fill_nulls(self):
        """Test 3: Pure non-null constraint produces CONDITIONAL_UPDATE with FLAG, NEVER mutating FILL_NULLS."""
        raw_rules = {
            "non_nullable": ["Salary"],
        }
        norm_rules = BusinessRuleNormalizer.normalize(raw_rules, default_dataset="dbo.EmployeeMaster")
        assert len(norm_rules.column_rules) == 1
        assert norm_rules.column_rules[0].is_constraint_only is True

        actions = BusinessRuleCompiler.compile(norm_rules)
        assert len(actions) == 1
        act = actions[0]

        # Invariant: NEVER FILL_NULLS for pure constraint
        assert act.action_type == ActionType.CONDITIONAL_UPDATE
        assert act.action_type != ActionType.FILL_NULLS
        assert act.parameters["disposition"] == "FLAG"
        assert "[Salary] IS NULL" in act.parameters["condition"]
        assert act.parameters.get("new_value") is None  # Does NOT mutate

    def test_4_non_null_with_approved_fill_policy_produces_fill_nulls(self, sample_semantic_plan):
        """Test 4: Non-null constraint WITH explicit approved remediation produces FILL_NULLS."""
        raw_rules = {
            "rules": [
                {
                    "rule_id": "HR-SALARY-NN-01",
                    "dataset": "dbo.EmployeeMaster",
                    "column": "Salary",
                    "constraint": {"type": "NOT_NULL", "default_value": 0},
                    "on_violation": {"action": "fill", "disposition": "FILL"},
                }
            ]
        }
        norm_rules = BusinessRuleNormalizer.normalize(raw_rules, default_dataset="dbo.EmployeeMaster")
        rule = norm_rules.column_rules[0]
        assert rule.defines_explicit_remediation is True

        actions = BusinessRuleCompiler.compile(norm_rules, semantic_plan=sample_semantic_plan)
        assert len(actions) == 1
        act = actions[0]

        assert act.action_type == ActionType.FILL_NULLS
        assert act.parameters["fill_value"] == 0
        assert act.source == ActionSource.BUSINESS_RULE

    def test_5_valid_values_without_aliases_does_not_invent_mappings(self):
        """Test 5: valid_values without aliases produces validation/flag action, NOT invented VALUE_MAP."""
        raw_rules = {
            "valid_values": {
                "CustomerStatus": {
                    "allowed": ["ACTIVE", "INACTIVE"],
                    "unknown_disposition": "FLAG",
                }
            }
        }
        norm_rules = BusinessRuleNormalizer.normalize(raw_rules, default_dataset="dbo.EmployeeMaster")
        rule = norm_rules.column_rules[0]
        assert rule.is_constraint_only is True

        actions = BusinessRuleCompiler.compile(norm_rules)
        assert len(actions) == 1
        act = actions[0]

        # Invariant: No VALUE_MAP invented
        assert act.action_type == ActionType.CONDITIONAL_UPDATE
        assert act.action_type != ActionType.VALUE_MAP
        assert act.parameters["disposition"] == "FLAG"
        assert "'ACTIVE'" in act.parameters["condition"]
        assert "'INACTIVE'" in act.parameters["condition"]

    def test_6_valid_values_with_aliases_produces_value_map(self):
        """Test 6: valid_values with explicit aliases produces VALUE_MAP for known aliases."""
        raw_rules = {
            "valid_values": {
                "CustomerStatus": {
                    "allowed": ["ACTIVE", "INACTIVE"],
                    "aliases": {
                        "active": "ACTIVE",
                        "A": "ACTIVE",
                        "inactive": "INACTIVE",
                        "I": "INACTIVE",
                    },
                    "unknown_disposition": "FLAG",
                }
            }
        }
        norm_rules = BusinessRuleNormalizer.normalize(raw_rules, default_dataset="dbo.EmployeeMaster")
        actions = BusinessRuleCompiler.compile(norm_rules)

        assert len(actions) == 1
        act = actions[0]
        assert act.action_type == ActionType.VALUE_MAP
        assert act.parameters["mapping"]["A"] == "ACTIVE"
        assert act.parameters["mapping"]["I"] == "INACTIVE"

    def test_7_range_assertion_defaults_to_validation_not_clipping(self):
        """Test 7: Salary range constraint defaults to validation/flagging, NEVER unapproved RANGE_CLIP."""
        raw_rules = {
            "custom_assertions": [
                "Salary >= 1000 AND Salary <= 500000",
            ]
        }
        norm_rules = BusinessRuleNormalizer.normalize(raw_rules, default_dataset="dbo.EmployeeMaster")
        actions = BusinessRuleCompiler.compile(norm_rules)

        assert len(actions) == 1
        act = actions[0]
        # Invariant: Financial data MUST NOT be clipped without explicit remediation authorization
        assert act.action_type == ActionType.CONDITIONAL_UPDATE
        assert act.action_type != ActionType.RANGE_CLIP
        assert act.parameters["disposition"] == "FLAG"
        assert "[Salary] < 1000.0 OR [Salary] > 500000.0" in act.parameters["condition"]
        assert act.parameters.get("new_value") is None

    def test_8_explicit_approved_clipping_produces_range_clip(self):
        """Test 8: Rule with explicit authorized 'clip' action produces RANGE_CLIP."""
        raw_rules = {
            "rules": [
                {
                    "rule_id": "HR-SALARY-CLIP-01",
                    "dataset": "dbo.EmployeeMaster",
                    "column": "Salary",
                    "constraint": {"type": "RANGE", "minimum": 1000, "maximum": 500000},
                    "on_violation": {"action": "clip", "disposition": "CLIP", "approval_required": True},
                }
            ]
        }
        norm_rules = BusinessRuleNormalizer.normalize(raw_rules, default_dataset="dbo.EmployeeMaster")
        actions = BusinessRuleCompiler.compile(norm_rules)

        assert len(actions) == 1
        act = actions[0]
        assert act.action_type == ActionType.RANGE_CLIP
        assert act.parameters["min_value"] == 1000.0
        assert act.parameters["max_value"] == 500000.0
        assert act.requires_approval is True

    def test_9_never_drop_and_drop_invalid_conflict_blocks_compilation(self):
        """Test 9: never_drop_rows=True combined with drop_invalid_rows=True blocks compilation."""
        raw_rules = {
            "never_drop_rows": True,
            "rules": [
                {
                    "rule_id": "POL-DROP-01",
                    "type": "POLICY",
                    "name": "drop_invalid_rows",
                    "value": True,
                }
            ],
        }
        norm_rules = BusinessRuleNormalizer.normalize(raw_rules, default_dataset="dbo.EmployeeMaster")
        val_res = BusinessRuleValidator.validate(norm_rules)

        assert val_res.is_valid is False
        assert any(i.code == "BUSINESS_RULE_CONFLICT" for i in val_res.blocking_issues)

    def test_10_incompatible_range_sanity_blocks_compilation(self):
        """Test 10: Inverted range (minimum > maximum) blocks compilation with INVALID_RANGE_RULE."""
        raw_rules = {
            "rules": [
                {
                    "rule_id": "INV-RANGE-01",
                    "dataset": "dbo.EmployeeMaster",
                    "column": "Salary",
                    "constraint": {"type": "RANGE", "minimum": 500, "maximum": 100},
                }
            ]
        }
        norm_rules = BusinessRuleNormalizer.normalize(raw_rules, default_dataset="dbo.EmployeeMaster")
        val_res = BusinessRuleValidator.validate(norm_rules)

        assert val_res.is_valid is False
        assert any(i.code == "INVALID_RANGE_RULE" for i in val_res.blocking_issues)

    def test_11_rule_generated_action_fails_semantic_validation_when_inappropriate(self, sample_semantic_plan):
        """Test 11: Transformation rule violating semantic column role (e.g. prefix on METRIC) is blocked."""
        raw_rules = {
            "domain_rules": {
                "dbo.EmployeeMaster.Salary": "Prefix Salary with INR-",
            }
        }
        norm_rules = BusinessRuleNormalizer.normalize(raw_rules, default_dataset="dbo.EmployeeMaster")
        val_res = BusinessRuleValidator.validate(norm_rules, semantic_plan=sample_semantic_plan)

        assert val_res.is_valid is False
        assert any(i.code == "SEMANTIC_ROLE_VIOLATION" for i in val_res.blocking_issues)

    def test_12_higher_authority_business_rule_overrides_heuristic(self, sample_dataset_profiles):
        """Test 12: Constraint-only business rule suppresses conflicting heuristic mutations on the same column."""
        bridge = IngestionBridge(default_schema="dbo")

        # Assessment heuristic suggests RANGE_CLIP on Salary
        assess = {
            "data_quality_issues": {
                "datasets": {
                    "dbo.EmployeeMaster": {
                        "issues": [
                            {
                                "column": "Salary",
                                "issue_type": "salary_outlier",
                                "suggested_action": "range_clip",
                                "parameters": {"min_value": 0.0, "max_value": 1000000.0},
                                "severity": "medium",
                            }
                        ]
                    }
                }
            }
        }

        # Plan provides an APPROVED_BUSINESS_RULE range constraint (constraint-only)
        plan = {
            "custom_assertions": [
                "Salary >= 1000 AND Salary <= 500000",
            ]
        }

        canonical_issues, b_rules, telemetry = bridge.extract_and_normalize(
            assess=assess,
            plan=plan,
            known_datasets={"dbo.EmployeeMaster"},
            dataset_profiles=sample_dataset_profiles,
        )

        detected_issues, structured_actions = bridge.bridge_to_compiler_models(canonical_issues)

        # Invariant: Heuristic RANGE_CLIP must NOT be generated!
        # Only the CONDITIONAL_UPDATE (FLAG) from the approved business rule survives!
        action_types = [a.action_type for a in structured_actions if a.target_columns == ["Salary"]]
        assert ActionType.RANGE_CLIP not in action_types
        assert ActionType.CONDITIONAL_UPDATE in action_types

    def test_13_provenance_survives_into_structured_action(self):
        """Test 13: StructuredActions retain source rule ID and authority provenance."""
        raw_rules = {
            "rules": [
                {
                    "rule_id": "CRM-STATUS-001",
                    "dataset": "dbo.EmployeeMaster",
                    "column": "CustomerStatus",
                    "constraint": {
                        "type": "ALLOWED_VALUES",
                        "allowed": ["ACTIVE", "INACTIVE"],
                        "aliases": {"A": "ACTIVE", "I": "INACTIVE"},
                    },
                    "authority": "APPROVED_BUSINESS_RULE",
                }
            ]
        }
        norm_rules = BusinessRuleNormalizer.normalize(raw_rules, default_dataset="dbo.EmployeeMaster")
        actions = BusinessRuleCompiler.compile(norm_rules)

        assert len(actions) == 1
        act = actions[0]
        assert act.source == ActionSource.BUSINESS_RULE
        assert act.source_reference == "CRM-STATUS-001"
        assert act.metadata["source_rule_id"] == "CRM-STATUS-001"
        assert act.metadata["source_authority"] == "APPROVED_BUSINESS_RULE"

    def test_14_no_harmful_transformations_reintroduced(self, sample_dataset_profiles, sample_semantic_plan):
        """Test 14: Verifies that business rules do not reintroduce bad mutations on Salary, ManagerID, CustomerStatus."""
        bridge = IngestionBridge(default_schema="dbo")

        assess = {
            "data_quality_issues": {
                "datasets": {
                    "dbo.EmployeeMaster": {
                        "issues": [
                            {"column": "Salary", "issue_type": "missing_values", "suggested_action": "fill_nulls"},
                            {"column": "CustomerStatus", "issue_type": "inconsistent_casing", "suggested_action": "value_map"},
                        ]
                    }
                }
            }
        }
        # Pure constraints on Salary and CustomerStatus
        plan = {
            "non_nullable": ["Salary"],
            "valid_values": {
                "CustomerStatus": {
                    "allowed": ["ACTIVE", "INACTIVE"],
                    # NO aliases provided
                }
            },
        }

        canonical_issues, b_rules, telemetry = bridge.extract_and_normalize(
            assess=assess,
            plan=plan,
            known_datasets={"dbo.EmployeeMaster"},
            dataset_profiles=sample_dataset_profiles,
            semantic_plan=sample_semantic_plan,
        )

        detected_issues, structured_actions = bridge.bridge_to_compiler_models(
            canonical_issues,
            semantic_plan=sample_semantic_plan,
        )

        salary_actions = [a.action_type for a in structured_actions if "Salary" in a.target_columns]
        status_actions = [a.action_type for a in structured_actions if "CustomerStatus" in a.target_columns]

        # No empty string / unapproved fill on Salary
        assert ActionType.FILL_NULLS not in salary_actions
        # No invented value map on CustomerStatus
        assert ActionType.VALUE_MAP not in status_actions
        # Both are safely handled by CONDITIONAL_UPDATE (FLAG)
        assert ActionType.CONDITIONAL_UPDATE in salary_actions
        assert ActionType.CONDITIONAL_UPDATE in status_actions
