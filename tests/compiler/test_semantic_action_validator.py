import pytest

from agent.compiler.compiler_pipeline import CompilerPipeline
from agent.compiler.models.actions import StructuredAction
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.models.validation import DiagnosticSeverity
from agent.compiler.validation.semantic_action_validator import SemanticActionValidator
from agent.errors import CompilationBlockedError
from agent.semantic_context import (
    ColumnCleaningContract,
    ColumnRole,
    EntityDefinition,
    SemanticCleaningPlan,
)


def _build_test_semantic_plan() -> SemanticCleaningPlan:
    entities = {
        "Customers": EntityDefinition(
            name="Customers",
            source_datasets=["Customers"],
            canonical_key="CustomerID",
            column_contracts={
                "CustomerID": ColumnCleaningContract(column_name="CustomerID", role=ColumnRole.PRIMARY_KEY),
                "ManagerID": ColumnCleaningContract(column_name="ManagerID", role=ColumnRole.IDENTIFIER),
                "Salary": ColumnCleaningContract(column_name="Salary", role=ColumnRole.METRIC),
                "CreditLimit": ColumnCleaningContract(column_name="CreditLimit", role=ColumnRole.METRIC),
                "CustomerStatus": ColumnCleaningContract(column_name="CustomerStatus", role=ColumnRole.CATEGORICAL),
                "Email": ColumnCleaningContract(column_name="Email", role=ColumnRole.EMAIL),
                "Phone": ColumnCleaningContract(column_name="Phone", role=ColumnRole.PHONE),
                "BirthDate": ColumnCleaningContract(column_name="BirthDate", role=ColumnRole.DATE),
            },
        )
    }
    return SemanticCleaningPlan(entities=entities)


def test_manager_id_cannot_be_parsed_as_date():
    sem_plan = _build_test_semantic_plan()
    action = StructuredAction(
        action_id="act_parse_date_mgr",
        action_type=ActionType.PARSE_DATES,
        dataset_name="Customers",
        target_columns=["ManagerID"],
        parameters={"date_format": "YYYY-MM-DD"},
    )
    issues = SemanticActionValidator.validate_action(action, semantic_plan=sem_plan)
    assert any(i.issue_code == "SEMANTIC_ROLE_VIOLATION" for i in issues)
    assert any("cannot be parsed as dates" in i.message for i in issues)


def test_salary_cannot_have_lowercase():
    sem_plan = _build_test_semantic_plan()
    action = StructuredAction(
        action_id="act_lower_salary",
        action_type=ActionType.LOWERCASE,
        dataset_name="Customers",
        target_columns=["Salary"],
        parameters={},
    )
    issues = SemanticActionValidator.validate_action(action, semantic_plan=sem_plan)
    assert any(i.issue_code == "SEMANTIC_ROLE_VIOLATION" for i in issues)
    assert any("LOWERCASE" in i.message for i in issues)


def test_credit_limit_cannot_have_standalone_trim():
    sem_plan = _build_test_semantic_plan()
    action = StructuredAction(
        action_id="act_trim_credit",
        action_type=ActionType.TRIM,
        dataset_name="Customers",
        target_columns=["CreditLimit"],
        parameters={},
    )
    issues = SemanticActionValidator.validate_action(action, semantic_plan=sem_plan)
    assert any(i.issue_code == "SEMANTIC_ROLE_VIOLATION" for i in issues)
    assert any("Numeric metrics cannot have standalone string TRIM" in i.message for i in issues)


def test_customer_status_cannot_be_dedup_partition_key():
    sem_plan = _build_test_semantic_plan()
    action = StructuredAction(
        action_id="act_dedup_status",
        action_type=ActionType.DEDUPLICATE,
        dataset_name="Customers",
        target_columns=["CustomerStatus"],
        parameters={"survivor_order": "[_DharaStagingRowId] ASC"},
    )
    issues = SemanticActionValidator.validate_action(action, semantic_plan=sem_plan)
    assert any(i.issue_code == "DEDUP_INVALID_CATEGORICAL_KEY" for i in issues)
    assert any(i.severity == DiagnosticSeverity.BLOCKING for i in issues)


def test_deduplicate_without_business_key_fails():
    sem_plan = SemanticCleaningPlan(entities={
        "RawTable": EntityDefinition(
            name="RawTable",
            source_datasets=["RawTable"],
            canonical_key=None,  # No primary key
            column_contracts={},
        )
    })
    action = StructuredAction(
        action_id="act_dedup_nokey",
        action_type=ActionType.DEDUPLICATE,
        dataset_name="RawTable",
        target_columns=[],
        parameters={},
    )
    issues = SemanticActionValidator.validate_action(action, semantic_plan=sem_plan)
    assert any(i.issue_code == "DEDUP_MISSING_BUSINESS_KEY" for i in issues)


def test_deduplicate_select_1_prohibited():
    sem_plan = _build_test_semantic_plan()
    action = StructuredAction(
        action_id="act_dedup_sel1",
        action_type=ActionType.DEDUPLICATE,
        dataset_name="Customers",
        target_columns=["CustomerID"],
        parameters={"survivor_order": "SELECT 1"},
    )
    issues = SemanticActionValidator.validate_action(action, semantic_plan=sem_plan)
    assert any(i.issue_code == "DEDUP_NON_DETERMINISTIC_SURVIVOR" for i in issues)


def test_identifier_casing_policy():
    sem_plan = _build_test_semantic_plan()
    action = StructuredAction(
        action_id="act_lower_id",
        action_type=ActionType.LOWERCASE,
        dataset_name="Customers",
        target_columns=["ManagerID"],
        parameters={},
    )
    # Default: Blocked
    issues_default = SemanticActionValidator.validate_action(action, semantic_plan=sem_plan)
    assert any(i.issue_code == "SEMANTIC_ROLE_VIOLATION" for i in issues_default)

    # With policy allowed
    ctx = CompilerContext(
        session_id="test_sess",
        target_engine=ExecutionEngine.AZURE_SQL,
        options={"identifier_normalization_policy": True},
    )
    issues_allowed = SemanticActionValidator.validate_action(action, semantic_plan=sem_plan, context=ctx)
    assert not any(i.issue_code == "SEMANTIC_ROLE_VIOLATION" for i in issues_allowed)


def test_valid_semantic_actions_pass():
    sem_plan = _build_test_semantic_plan()
    actions = [
        StructuredAction(
            action_id="act_email",
            action_type=ActionType.SANITIZE_EMAIL,
            dataset_name="Customers",
            target_columns=["Email"],
            parameters={},
        ),
        StructuredAction(
            action_id="act_phone",
            action_type=ActionType.NORMALIZE_PHONE,
            dataset_name="Customers",
            target_columns=["Phone"],
            parameters={},
        ),
        StructuredAction(
            action_id="act_date_clip",
            action_type=ActionType.RANGE_CLIP,
            dataset_name="Customers",
            target_columns=["BirthDate"],
            parameters={"min_value": "1900-01-01", "max_value": "2025-01-01"},
        ),
        StructuredAction(
            action_id="act_salary_num",
            action_type=ActionType.COERCE_NUMERIC,
            dataset_name="Customers",
            target_columns=["Salary"],
            parameters={},
        ),
        StructuredAction(
            action_id="act_dedup_valid",
            action_type=ActionType.DEDUPLICATE,
            dataset_name="Customers",
            target_columns=["CustomerID"],
            parameters={"survivor_order": "[_DharaStagingRowId] ASC"},
        ),
    ]

    for act in actions:
        issues = SemanticActionValidator.validate_action(act, semantic_plan=sem_plan)
        assert len(issues) == 0, f"Expected 0 issues for action {act.action_type.value}, got: {issues}"


def test_compiler_pipeline_blocks_semantic_role_violation():
    sem_plan = _build_test_semantic_plan()
    bad_action = StructuredAction(
        action_id="act_bad_parse",
        action_type=ActionType.PARSE_DATES,
        dataset_name="Customers",
        target_columns=["ManagerID"],
        parameters={"date_format": "YYYY-MM-DD"},
    )

    ctx = CompilerContext(
        session_id="test_blocked_session",
        target_engine=ExecutionEngine.AZURE_SQL,
        dataset_profiles={
            "Customers": DatasetProfile(
                dataset_id="Customers",
                row_count=100,
                columns={"ManagerID": ColumnProfile(name="ManagerID", dtype="string")},
            )
        },
        structured_actions=[bad_action],
        semantic_plan=sem_plan,
    )

    pipeline = CompilerPipeline()
    with pytest.raises(CompilationBlockedError) as exc_info:
        pipeline.compile(ctx)

    assert "Semantic validation blocked compilation" in str(exc_info.value)
    assert "cannot be parsed as dates" in str(exc_info.value)


def test_deduplicate_with_composite_identifier_keys_allowed():
    """Verify that columns ending with 'ID' like CustomerID and AccountManagerID are treated as keys/identifiers, not blocked as categorical-only deduplication."""
    action = StructuredAction(
        action_id="act_dedup_composite",
        action_type=ActionType.DEDUPLICATE,
        dataset_name="dbo.CustomerMaster",
        target_columns=["CustomerID", "AccountManagerID"],
        parameters={"survivor_order": "[_DharaStagingRowId] ASC"},
    )
    issues = SemanticActionValidator.validate_action(action)
    assert not any(i.issue_code == "DEDUP_INVALID_CATEGORICAL_KEY" for i in issues)

