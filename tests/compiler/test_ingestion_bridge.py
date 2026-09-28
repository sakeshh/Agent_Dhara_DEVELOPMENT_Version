import logging
import pytest

from agent.compiler.models.actions import StructuredAction
from agent.compiler.models.logical import ActionType
from agent.compiler.planners.ingestion_bridge import IngestionBridge


def test_ingestion_bridge_controlled_precedence():
    bridge = IngestionBridge(default_schema="dbo")

    # 1. Explicit planned action has highest precedence
    act, path = bridge.resolve_action(
        planned_action="trim",
        business_rule={"action": "lowercase"},
        issue_type="null_values",
        suggested_action="parse_dates",
    )
    assert act == ActionType.TRIM
    assert path == "explicit_planned_action"

    # 2. Business rule action has second precedence
    act, path = bridge.resolve_action(
        planned_action=None,
        business_rule={"action": "lowercase"},
        issue_type="null_values",
        suggested_action="parse_dates",
    )
    assert act == ActionType.LOWERCASE
    assert path == "business_rule_action"

    # 3. Normalized issue type has third precedence
    act, path = bridge.resolve_action(
        planned_action=None,
        business_rule=None,
        issue_type="invalid_email",
        suggested_action="random_text",
    )
    assert act == ActionType.SANITIZE_EMAIL
    assert path == "normalized_issue_type"

    # 4. Suggested action has fourth precedence
    act, path = bridge.resolve_action(
        planned_action=None,
        business_rule=None,
        issue_type="unknown_anomaly",
        suggested_action="standardize_dates",
    )
    assert act == ActionType.PARSE_DATES
    assert "suggested_action" in path

    # 5. Unsupported novel rule produces None
    act, path = bridge.resolve_action(
        planned_action=None,
        business_rule=None,
        issue_type="invalid_manager_hierarchy",
        suggested_action=None,
    )
    assert act is None
    assert path == "unsupported"


def test_ingestion_bridge_harvesting_all_sources(caplog):
    bridge = IngestionBridge(default_schema="dbo")

    assess = {
        "datasets": {
            "dbo.Dhara_Employee": {
                "quality": {
                    "issues": [
                        {
                            "column": "Email",
                            "issue_type": "invalid_email",
                            "severity": "high",
                            "message": "Malformed email",
                        }
                    ]
                },
                "columns": {
                    "Phone": {
                        "issues": [
                            {
                                "issue_type": "invalid_phone",
                                "severity": "medium",
                                "message": "Phone has non-numeric characters",
                            }
                        ]
                    },
                    "Hierarchy": {
                        "issues": [
                            {
                                "issue_type": "invalid_manager_hierarchy",
                                "severity": "high",
                                "message": "Manager cycle detected",
                            }
                        ]
                    }
                }
            }
        },
        "data_quality_issues": {
            "datasets": {
                "dbo.Dhara_Employee": {
                    "issues": [
                        # Duplicate of column-level Email issue to test deduplication
                        {
                            "column": "Email",
                            "issue_type": "invalid_email",
                            "severity": "high",
                            "parameters": {"regex": "^[a-z]+@[a-z]+\\.[a-z]+$"},
                        },
                        {
                            "column": "Salary",
                            "issue_type": "negative_values",
                            "severity": "high",
                            "message": "Salary is negative",
                            "parameters": {"min_value": 0.0},
                        }
                    ]
                }
            },
            "global_issues": {
                "orphan_foreign_keys": [
                    {
                        "child_dataset": "dbo.Dhara_Employee",
                        "child_key": "DepartmentId",
                        "parent_dataset": "dbo.Dhara_Department",
                        "parent_key": "Id",
                    }
                ]
            }
        }
    }

    plan = {
        "datasets": {
            "dbo.Dhara_Employee": {
                "steps": [
                    {
                        "action": "trim",
                        "column": "FirstName",
                        "description": "Trim FirstName",
                    }
                ]
            }
        },
        "domain_rules": {
            "dbo.Dhara_Employee.Status": {
                "dataset": "dbo.Dhara_Employee",
                "column": "Status",
                "action": "uppercase",
                "enabled": True,
            }
        }
    }

    with caplog.at_level(logging.WARNING):
        canonical_issues, b_rules, telemetry = bridge.extract_and_normalize(
            assess=assess,
            plan=plan,
            known_datasets={"dbo.Dhara_Employee", "dbo.Dhara_Department"},
        )

    # Acceptance criteria verification
    assert telemetry["assessment_discovered_count"] == 6
    assert telemetry["plan_discovered_count"] == 1
    assert telemetry["duplicates_removed_count"] == 1  # Duplicate Email issue merged
    assert telemetry["normalized_issue_count"] == 6
    assert telemetry["mapped_action_count"] == 5
    assert telemetry["unsupported_issue_count"] == 1

    # Verify visible UNSUPPORTED logging
    assert any("UNSUPPORTED:" in record.message and "invalid_manager_hierarchy" in record.message for record in caplog.records)

    # Verify model conversion
    issues, structured_actions = bridge.bridge_to_compiler_models(canonical_issues)
    assert len(issues) == 6
    assert len(structured_actions) == 5  # Only mapped actions become structured actions
    assert any(sa.action_type == ActionType.VALIDATE_REFERENTIAL_INTEGRITY for sa in structured_actions)
    assert any(sa.action_type == ActionType.SANITIZE_EMAIL for sa in structured_actions)
    assert any(sa.action_type == ActionType.NORMALIZE_PHONE for sa in structured_actions)
    assert any(sa.action_type == ActionType.TRIM for sa in structured_actions)


def test_diagnostic_context_and_mapping():
    """Checkpoint 2.5: Verifies that a realistic assessment with 20+ mixed issues
    reaches the compiler with accurate telemetry and unsupported visibility.
    """
    bridge = IngestionBridge(default_schema="dbo")

    # Generate 20 assessment issues: 12 standard + 8 complex/unsupported
    dq_issues = []
    # 12 standard
    standard_types = [
        ("FirstName", "untrimmed_whitespace", "trim"),
        ("LastName", "untrimmed_whitespace", "trim"),
        ("Email", "invalid_email", "sanitize_email"),
        ("Phone", "invalid_phone", "normalize_phone"),
        ("BirthDate", "invalid_date", "parse_dates"),
        ("HireDate", "invalid_date", "parse_dates"),
        ("Salary", "negative_values", "range_clip", {"min_value": 0.0}),
        ("Bonus", "outliers", "range_clip", {"min_value": 0.0, "max_value": 100000.0}),
        ("Gender", "boolean_inconsistency", "standardize_boolean"),
        ("Id", "duplicate_pk", "deduplicate"),
        ("MiddleName", "null_values", "fill_nulls"),
        ("OldCode", "sentinel_values", "replace_sentinels"),
    ]
    for item in standard_types:
        col, itype, sug = item[0], item[1], item[2]
        params = item[3] if len(item) > 3 else {}
        dq_issues.append({"column": col, "issue_type": itype, "suggested_action": sug, "severity": "medium", "parameters": params})

    # 8 unsupported / domain-specific
    unsupported_types = [
        ("Hierarchy", "circular_management_reference"),
        ("TaxId", "complex_vat_checksum_failure"),
        ("Address", "unrecognized_geocoding_envelope"),
        ("SecurityClearance", "multi_jurisdiction_conflict"),
        ("CostCenter", "inactive_general_ledger_mapping"),
        ("UnionCode", "expired_collective_bargaining_agreement"),
        ("Schedule", "cross_timezone_shift_overlap"),
        ("AuditScore", "statistical_kurtosis_anomaly"),
    ]
    for col, itype in unsupported_types:
        dq_issues.append({"column": col, "issue_type": itype, "severity": "high"})

    assess = {
        "data_quality_issues": {
            "datasets": {
                "dbo.Dhara_Employee": {
                    "issues": dq_issues
                }
            }
        }
    }
    plan = {"datasets": {"dbo.Dhara_Employee": {"steps": []}}}

    canonical_issues, _, telemetry = bridge.extract_and_normalize(
        assess=assess,
        plan=plan,
        known_datasets={"dbo.Dhara_Employee"},
    )

    assert telemetry["assessment_discovered_count"] == 20
    assert telemetry["normalized_issue_count"] == 20
    assert telemetry["mapped_action_count"] == 12
    assert telemetry["unsupported_issue_count"] == 8
    assert len(telemetry["unsupported_issues"]) == 8

    # Ensure unsupported items are explicitly identified
    unsupported_names = [u["issue_type"] for u in telemetry["unsupported_issues"]]
    assert "circular_management_reference" in unsupported_names
    assert "complex_vat_checksum_failure" in unsupported_names


def test_row_level_deduplicate_with_pseudo_column_compiles_successfully():
    """Verify that a row-level deduplicate step marked with [Row-level] pseudo-column
    does NOT fail schema validation with ACTION_UNKNOWN_COLUMN and compiles cleanly.
    """
    from agent.compiler.compiler_pipeline import CompilerPipeline
    from agent.compiler.models import ColumnProfile, CompilerContext, DatasetProfile, ExecutionEngine

    bridge = IngestionBridge(default_schema="dbo")

    plan = {
        "datasets": {
            "dbo.Dhara_EmployeeStatusHistory": {
                "steps": [
                    {
                        "action": "deduplicate",
                        "column": "[Row-level]",
                        "params": {
                            "keys": ["EmployeeID", "StatusEventID", "ReasonCode"]
                        },
                        "order": 1,
                    }
                ]
            }
        }
    }
    assess = {"datasets": {"dbo.Dhara_EmployeeStatusHistory": {"columns": {}}}}

    canonical_issues, b_rules, telemetry = bridge.extract_and_normalize(
        assess=assess,
        plan=plan,
        known_datasets={"dbo.Dhara_EmployeeStatusHistory"},
    )
    detected_issues, structured_actions = bridge.bridge_to_compiler_models(canonical_issues)

    assert len(structured_actions) == 1
    act = structured_actions[0]
    assert act.action_type == ActionType.DEDUPLICATE
    # Target columns must be the candidate keys, not "[Row-level]"
    assert "Row-level" not in act.target_columns
    assert "[Row-level]" not in act.target_columns
    assert act.target_columns == ["EmployeeID", "StatusEventID", "ReasonCode"]

    # Active dataset profile has real database columns
    dataset_prof = DatasetProfile(
        dataset_name="dbo.Dhara_EmployeeStatusHistory",
        dataset_id="dbo.Dhara_EmployeeStatusHistory",
        columns={
            "StatusHistoryRowID": ColumnProfile(name="StatusHistoryRowID", dtype="int"),
            "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
            "StatusEventID": ColumnProfile(name="StatusEventID", dtype="int"),
            "EffectiveFrom": ColumnProfile(name="EffectiveFrom", dtype="date"),
            "ChangedAt": ColumnProfile(name="ChangedAt", dtype="date"),
            "ChangedBy": ColumnProfile(name="ChangedBy", dtype="string"),
            "ReasonCode": ColumnProfile(name="ReasonCode", dtype="string"),
        },
    )

    ctx = CompilerContext(
        session_id="test_row_level_dedup",
        target_engine=ExecutionEngine.AZURE_SQL,
        dataset_profiles={"dbo.Dhara_EmployeeStatusHistory": dataset_prof},
        detected_issues=detected_issues,
        structured_actions=structured_actions,
        business_rules=b_rules,
    )

    pipeline = CompilerPipeline()
    compiled_ctx, snapshot = pipeline.compile(ctx)

    assert compiled_ctx.code_artifact is not None
    sql_code = compiled_ctx.code_artifact.content
    assert "PARTITION BY [EmployeeID], [StatusEventID], [ReasonCode]" in sql_code
    assert "[Row-level]" not in sql_code
    assert "Row-level" not in sql_code


# ==============================================================================
# Regression Tests for Production Code Generation Guards
# ==============================================================================

def test_range_clip_without_bounds_is_not_emitted():
    """Verify that an issue mapping to RANGE_CLIP without min_value or max_value is not emitted."""
    bridge = IngestionBridge(default_schema="dbo")
    assess = {
        "data_quality_issues": {
            "datasets": {
                "dbo.Dhara_Employee": {
                    "issues": [
                        {
                            "issue_type": "range_outlier",
                            "column": "Salary",
                            "message": "Outlier in Salary without defined bounds",
                            "parameters": {},
                        }
                    ]
                }
            }
        }
    }
    plan = {"datasets": {}}

    canonical_issues, b_rules, telemetry = bridge.extract_and_normalize(
        assess=assess,
        plan=plan,
        known_datasets={"dbo.Dhara_Employee"},
    )
    detected_issues, structured_actions = bridge.bridge_to_compiler_models(canonical_issues)

    assert not any(act.action_type == ActionType.RANGE_CLIP for act in structured_actions)
    assert telemetry["unsupported_missing_range_bounds_count"] == 1


def test_column_action_with_star_target_is_not_emitted():
    """Verify that a column-bound action targeting '*' is dropped and not emitted."""
    bridge = IngestionBridge(default_schema="dbo")
    plan = {
        "datasets": {
            "dbo.Dhara_OrgReference": {
                "steps": [
                    {
                        "action": "range_clip",
                        "column": "*",
                        "params": {"min_value": 0.0},
                    }
                ]
            }
        }
    }
    assess = {"datasets": {}}

    canonical_issues, b_rules, telemetry = bridge.extract_and_normalize(
        assess=assess,
        plan=plan,
        known_datasets={"dbo.Dhara_OrgReference"},
    )
    detected_issues, structured_actions = bridge.bridge_to_compiler_models(canonical_issues)

    assert structured_actions == []
    assert telemetry["unsupported_missing_target_columns_count"] == 1


@pytest.mark.parametrize("pseudo_col", ["*", "[*]", "row-level", "[row-level]", "Row-level", "[Row-level]", "__row__", "[__row__]"])
def test_column_action_with_all_pseudo_columns_is_not_emitted(pseudo_col: str):
    """Verify that all forms of pseudo-columns are stripped and column actions are not emitted."""
    bridge = IngestionBridge(default_schema="dbo")
    plan = {
        "datasets": {
            "dbo.Dhara_OrgReference": {
                "steps": [
                    {
                        "action": "trim",
                        "column": pseudo_col,
                        "params": {"side": "both"},
                    }
                ]
            }
        }
    }
    assess = {"datasets": {}}

    canonical_issues, b_rules, telemetry = bridge.extract_and_normalize(
        assess=assess,
        plan=plan,
        known_datasets={"dbo.Dhara_OrgReference"},
    )
    detected_issues, structured_actions = bridge.bridge_to_compiler_models(canonical_issues)

    assert structured_actions == []
    assert telemetry["unsupported_missing_target_columns_count"] == 1


def test_range_clip_with_valid_lower_bound_only_accepted():
    """RANGE_CLIP with only min_value is accepted and emitted with valid target column."""
    bridge = IngestionBridge(default_schema="dbo")
    plan = {
        "datasets": {
            "dbo.Dhara_Employee": {
                "steps": [
                    {
                        "action": "range_clip",
                        "column": "Salary",
                        "params": {"min_value": 0.0},
                    }
                ]
            }
        }
    }
    assess = {"datasets": {}}

    canonical_issues, b_rules, telemetry = bridge.extract_and_normalize(
        assess=assess,
        plan=plan,
        known_datasets={"dbo.Dhara_Employee"},
    )
    detected_issues, structured_actions = bridge.bridge_to_compiler_models(canonical_issues)

    assert len(structured_actions) == 1
    act = structured_actions[0]
    assert act.action_type == ActionType.RANGE_CLIP
    assert act.target_columns == ["Salary"]
    assert act.parameters["min_value"] == 0.0
    assert "max_value" not in act.parameters


def test_range_clip_with_valid_upper_bound_only_accepted():
    """RANGE_CLIP with only max_value is accepted and emitted with valid target column."""
    bridge = IngestionBridge(default_schema="dbo")
    plan = {
        "datasets": {
            "dbo.Dhara_Employee": {
                "steps": [
                    {
                        "action": "range_clip",
                        "column": "Discount",
                        "params": {"max_value": 100.0},
                    }
                ]
            }
        }
    }
    assess = {"datasets": {}}

    canonical_issues, b_rules, telemetry = bridge.extract_and_normalize(
        assess=assess,
        plan=plan,
        known_datasets={"dbo.Dhara_Employee"},
    )
    detected_issues, structured_actions = bridge.bridge_to_compiler_models(canonical_issues)

    assert len(structured_actions) == 1
    act = structured_actions[0]
    assert act.action_type == ActionType.RANGE_CLIP
    assert act.target_columns == ["Discount"]
    assert act.parameters["max_value"] == 100.0
    assert "min_value" not in act.parameters


def test_logical_planner_boundary_strictly_blocks_naked_range_clip():
    """Verify that LogicalPlanner boundary remains strict and rejects malformed actions directly."""
    from agent.compiler.planners.logical_planner import LogicalPlanner
    from agent.compiler.models.actions import ActionSource

    planner = LogicalPlanner()
    naked_action = StructuredAction(
        action_id="naked_clip_01",
        action_type=ActionType.RANGE_CLIP,
        dataset_name="dbo.Dhara_Employee",
        target_columns=[],
        parameters={},
        source=ActionSource.CATALOG,
    )

    with pytest.raises(ValueError, match="Invalid StructuredAction cannot enter LogicalPlanner"):
        planner.action_to_logical_step(naked_action, 0, {})


def test_cast_type_without_target_type_is_not_emitted():
    """Verify that CAST_TYPE without target_type is not emitted and routed cleanly."""
    bridge = IngestionBridge(default_schema="dbo")
    plan = {
        "datasets": {
            "dbo.Dhara_Employee": {
                "steps": [
                    {
                        "action": "cast_type",
                        "column": "Salary",
                        "params": {},
                    }
                ]
            }
        }
    }
    assess = {"datasets": {}}

    canonical_issues, b_rules, telemetry = bridge.extract_and_normalize(
        assess=assess,
        plan=plan,
        known_datasets={"dbo.Dhara_Employee"},
    )
    detected_issues, structured_actions = bridge.bridge_to_compiler_models(canonical_issues)

    assert not any(act.action_type == ActionType.CAST_TYPE for act in structured_actions)
    assert telemetry["unsupported_missing_target_type_count"] == 1


def test_cast_type_with_valid_target_type_is_emitted():
    """Verify that CAST_TYPE with valid target_type is emitted properly."""
    bridge = IngestionBridge(default_schema="dbo")
    plan = {
        "datasets": {
            "dbo.Dhara_Employee": {
                "steps": [
                    {
                        "action": "cast_type",
                        "column": "Salary",
                        "params": {"target_type": "DECIMAL(18,2)"},
                    }
                ]
            }
        }
    }
    assess = {"datasets": {}}

    canonical_issues, b_rules, telemetry = bridge.extract_and_normalize(
        assess=assess,
        plan=plan,
        known_datasets={"dbo.Dhara_Employee"},
    )
    detected_issues, structured_actions = bridge.bridge_to_compiler_models(canonical_issues)

    cast_actions = [act for act in structured_actions if act.action_type == ActionType.CAST_TYPE]
    assert len(cast_actions) == 1
    assert cast_actions[0].parameters["target_type"] == "DECIMAL(18,2)"


def test_malformed_action_is_intercepted_by_universal_guard():
    """Verify that any action with invalid parameters failing Pydantic contract is dropped by universal guard."""
    from agent.compiler.planners.ingestion_bridge import CanonicalIssue

    bridge = IngestionBridge(default_schema="dbo")
    # Manually construct a canonical issue that resolved to PATTERN_REPLACE but lacks required 'pattern'
    bad_canonical = CanonicalIssue(
        issue_id="bad_pattern_01",
        dataset="dbo.Dhara_Employee",
        raw_dataset="dbo.Dhara_Employee",
        column="EmployeeName",
        issue_type="regex_cleaning",
        suggested_action="pattern_replace",
        severity="medium",
        parameters={},  # Missing required 'pattern'
        source="plan_dataset_steps",
        message="Bad pattern replace",
        resolved_action=ActionType.PATTERN_REPLACE,
        resolution_path="explicit_planned_action",
    )

    detected_issues, structured_actions = bridge.bridge_to_compiler_models([bad_canonical])

    assert structured_actions == []
    assert bad_canonical.resolved_action is None
    assert "unsupported_invalid_parameters" in bad_canonical.resolution_path



