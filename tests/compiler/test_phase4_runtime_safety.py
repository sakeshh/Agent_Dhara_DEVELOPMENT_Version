from __future__ import annotations

import pytest

from agent.compiler.models.actions import (
    ActionSource,
    ComparisonOperator,
    StructuredAction,
    StructuredCondition,
)
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.execution import ExecutionMode, FailureBehavior, TransformationPolicy
from agent.compiler.models.logical import ActionType, LogicalStep
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.physical.physical_planner import PhysicalPlanner
from agent.compiler.planners.logical_planner import LogicalPlanner
from agent.compiler.router.language_router import LanguageRouter
from agent.compiler.safety.circuit_breaker import CircuitBreaker
from agent.compiler.safety.dry_run import InMemoDryRunSimulator
from agent.compiler.safety.models import DryRunReport, SafetyStatus, StepSafetyMetrics
from agent.compiler.validators.sql_validator import SQLValidator
from agent.errors import CircuitBreakerError


class TestPhase4RuntimeSafety:
    """
    Phase 4 Test Suite: Runtime Safety & Defensive Execution.
    Validates:
      1. Conversion Error Tracking (replacing blind TRY_CAST)
      2. Circuit Breaker threshold enforcement (BLOCK, QUARANTINE_ROW, WARN, SKIP_RULE)
      3. In-memory dry run / pre-flight validation on sample data
      4. Defensive coercion (division-by-zero, date bounds, numeric parsing)
      5. Full end-to-end pipeline safety and T-SQL validation
    """

    def test_1_blind_try_cast_replaced_with_conversion_tracking_sql(self):
        """Test 1: CAST_TYPE generates defensive rejection logging rather than blind TRY_CAST."""
        step = LogicalStep(
            step_id="step_cast_1",
            dataset_name="Employees",
            action=ActionType.CAST_TYPE,
            target_columns=["Salary"],
            parameters={"target_type": "DECIMAL(18,2)"},
        )
        planner = PhysicalPlanner()
        phys = planner._map_step(step, ExecutionEngine.AZURE_SQL, 0)

        from agent.compiler.templates.engine import TemplateEngine
        engine = TemplateEngine.get_instance()
        sql = engine.render(
            f"sql/{phys.template_name}",
            **phys.template_parameters,
            step_id=phys.step_id,
            description="Test Cast",
            pipeline_id="pipe_safe_101",
        )

        # Must log failed conversions to Rejection_Log
        assert "INSERT INTO [ETL_REJECT].[Rejection_Log]" in sql
        assert "WHERE [Salary] IS NOT NULL AND TRY_CAST([Salary] AS DECIMAL(18,2)) IS NULL" in sql
        assert "UPDATE [dbo].[Employees_Staging]" in sql
        assert "TRY_CAST([Salary] AS DECIMAL(18,2))" in sql

    def test_2_circuit_breaker_sql_generation_with_max_failed_rows(self):
        """Test 2: Specifying max_failed_rows generates circuit breaker threshold checks in T-SQL."""
        step = LogicalStep(
            step_id="step_cast_2",
            dataset_name="Employees",
            action=ActionType.CAST_TYPE,
            target_columns=["EmployeeCode"],
            parameters={"target_type": "BIGINT", "max_failed_rows": 0},
        )
        planner = PhysicalPlanner()
        phys = planner._map_step(step, ExecutionEngine.AZURE_SQL, 0)

        from agent.compiler.templates.engine import TemplateEngine
        engine = TemplateEngine.get_instance()
        sql = engine.render(
            f"sql/{phys.template_name}",
            **phys.template_parameters,
            step_id=phys.step_id,
            description="Test Threshold",
            pipeline_id="pipe_safe_102",
        )

        assert "DECLARE @FailedCount_EmployeeCode INT = @@ROWCOUNT;" in sql
        assert "IF @FailedCount_EmployeeCode > 0" in sql
        assert "RAISERROR('Circuit breaker triggered: Conversion failures for column [%s] exceeded threshold (%d).', 16, 1, 'EmployeeCode', 0);" in sql

    def test_3_defensive_date_conversion_bounds(self):
        """Test 3: PARSE_DATES enforces safe range bounds derived from date type and logs failures."""
        step = LogicalStep(
            step_id="step_date_1",
            dataset_name="Orders",
            action=ActionType.PARSE_DATES,
            target_columns=["OrderDate"],
            parameters={"target_type": "DATETIME", "max_failed_rows": 5},
        )
        planner = PhysicalPlanner()
        phys = planner._map_step(step, ExecutionEngine.AZURE_SQL, 0)

        from agent.compiler.templates.engine import TemplateEngine
        engine = TemplateEngine.get_instance()
        sql = engine.render(
            f"sql/{phys.template_name}",
            **phys.template_parameters,
            step_id=phys.step_id,
            description="Test Date Bounds",
            pipeline_id="pipe_safe_103",
        )

        assert "TRY_CONVERT(DATETIME, [OrderDate]) < '1753-01-01'" in sql
        assert "TRY_CONVERT(DATETIME, [OrderDate]) > '9999-12-31'" in sql
        assert "INSERT INTO [ETL_REJECT].[Rejection_Log]" in sql
        assert "DECLARE @FailedCount_OrderDate INT = @@ROWCOUNT;" in sql
        assert "IF @FailedCount_OrderDate > 5" in sql

    def test_4_defensive_numeric_coercion_sql(self):
        """Test 4: COERCE_NUMERIC generates defensive logging and circuit breaker checks."""
        step = LogicalStep(
            step_id="step_num_1",
            dataset_name="Orders",
            action=ActionType.COERCE_NUMERIC,
            target_columns=["Amount"],
            parameters={"target_type": "DECIMAL(18,4)", "max_failed_rows": 10},
        )
        planner = PhysicalPlanner()
        phys = planner._map_step(step, ExecutionEngine.AZURE_SQL, 0)

        from agent.compiler.templates.engine import TemplateEngine
        engine = TemplateEngine.get_instance()
        sql = engine.render(
            f"sql/{phys.template_name}",
            **phys.template_parameters,
            step_id=phys.step_id,
            description="Test Numeric Coercion",
            pipeline_id="pipe_safe_104",
        )

        assert "Numeric coercion failure: unable to parse [Amount] as DECIMAL(18,4)" in sql
        assert "WHERE [Amount] IS NOT NULL AND TRY_CAST([Amount] AS DECIMAL(18,4)) IS NULL" in sql
        assert "DECLARE @FailedCount_Amount INT = @@ROWCOUNT;" in sql
        assert "IF @FailedCount_Amount > 10" in sql

    def test_5_in_memory_dry_run_clean_execution(self):
        """Test 5: Clean dry run simulation transforms data accurately and returns PASSED."""
        actions = [
            StructuredAction(
                action_type=ActionType.TRIM,
                dataset_name="Customers",
                target_columns=["Name"],
                parameters={"side": "both"},
                source=ActionSource.ASSESSMENT,
            ),
            StructuredAction(
                action_type=ActionType.PREFIX_ADD,
                dataset_name="Customers",
                target_columns=["Code"],
                parameters={"prefix": "CUST-", "only_if_missing": True},
                source=ActionSource.BUSINESS_RULE,
            ),
        ]
        sample = [
            {"Name": " Alice ", "Code": "001"},
            {"Name": "Bob  ", "Code": "CUST-002"},
        ]

        report = InMemoDryRunSimulator.simulate(actions, sample)

        assert report.status == SafetyStatus.PASSED
        assert report.is_safe_to_execute is True
        assert report.circuit_breaker_triggered is False
        assert len(report.clean_records) == 2
        assert report.clean_records[0]["Name"] == "Alice"
        assert report.clean_records[0]["Code"] == "CUST-001"
        assert report.clean_records[1]["Name"] == "Bob"
        assert report.clean_records[1]["Code"] == "CUST-002"

    def test_6_dry_run_detects_silent_conversion_failures(self):
        """Test 6: Dry run flags unparseable type conversions rather than silently nullifying."""
        action = StructuredAction(
            action_type=ActionType.CAST_TYPE,
            dataset_name="Employees",
            target_columns=["Age"],
            parameters={"target_type": "int"},
            source=ActionSource.ASSESSMENT,
        )
        sample = [
            {"Age": "25"},
            {"Age": "30"},
            {"Age": "invalid_thirty"},
            {"Age": "40"},
        ]
        policy = TransformationPolicy(
            action_type=ActionType.CAST_TYPE,
            max_failed_rows=5,
            max_failure_percentage=50.0,
            on_failure=FailureBehavior.WARN,
        )

        report = InMemoDryRunSimulator.simulate(
            [action],
            sample,
            policies={action.action_type.value: policy},
            raise_on_block=False,
        )

        assert report.total_failed_rows == 1
        metric = report.step_metrics[0]
        assert metric.failed_rows == 1
        assert metric.passed_rows == 3
        assert metric.failure_percentage == 25.0
        assert len(metric.conversion_failures) == 1
        assert metric.conversion_failures[0].original_value == "invalid_thirty"
        assert metric.conversion_failures[0].column == "Age"

    def test_7_circuit_breaker_aborts_on_exceeded_failed_rows(self):
        """Test 7: Exceeding max_failed_rows raises CircuitBreakerError under BLOCK policy."""
        action = StructuredAction(
            action_type=ActionType.CAST_TYPE,
            dataset_name="Employees",
            target_columns=["Salary"],
            parameters={"target_type": "float"},
            source=ActionSource.ASSESSMENT,
        )
        sample = [
            {"Salary": "1000.50"},
            {"Salary": "NaN_SALARY"},
            {"Salary": "N/A"},
        ]
        policy = TransformationPolicy(
            action_type=ActionType.CAST_TYPE,
            max_failed_rows=0,  # Zero tolerance
            on_failure=FailureBehavior.BLOCK,
        )

        with pytest.raises(CircuitBreakerError) as exc_info:
            InMemoDryRunSimulator.simulate(
                [action],
                sample,
                policies={action.action_type.value: policy},
                raise_on_block=True,
            )

        assert "Circuit breaker triggered" in str(exc_info.value)
        assert exc_info.value.failed_count == 2
        assert exc_info.value.threshold == 0

    def test_8_circuit_breaker_aborts_on_exceeded_failure_percentage(self):
        """Test 8: Strict 0% failure tolerance on key columns triggers circuit breaker."""
        action = StructuredAction(
            action_type=ActionType.CAST_TYPE,
            dataset_name="Orders",
            target_columns=["OrderID"],
            parameters={"target_type": "bigint"},
            source=ActionSource.ASSESSMENT,
        )
        sample = [
            {"OrderID": "1001"},
            {"OrderID": "1002"},
            {"OrderID": "CORRUPTED_ID"},
            {"OrderID": "1004"},
        ]
        policy = TransformationPolicy(
            action_type=ActionType.CAST_TYPE,
            max_failed_rows=None,
            max_failure_percentage=0.0,  # 0% tolerance
            on_failure=FailureBehavior.BLOCK,
        )

        with pytest.raises(CircuitBreakerError) as exc_info:
            InMemoDryRunSimulator.simulate(
                [action],
                sample,
                policies={action.action_type.value: policy},
                raise_on_block=True,
            )

        assert "Circuit breaker triggered" in str(exc_info.value)
        assert exc_info.value.failed_count == 1

    def test_9_circuit_breaker_quarantine_diverts_offending_rows(self):
        """Test 9: QUARANTINE_ROW diverts failed rows to quarantine while preserving clean rows."""
        action = StructuredAction(
            action_type=ActionType.COERCE_NUMERIC,
            dataset_name="Orders",
            target_columns=["Amount"],
            parameters={"target_type": "float"},
            source=ActionSource.ASSESSMENT,
        )
        sample = [
            {"OrderID": 1, "Amount": "50.00"},
            {"OrderID": 2, "Amount": "bad_amount"},
            {"OrderID": 3, "Amount": "75.50"},
        ]
        policy = TransformationPolicy(
            action_type=ActionType.COERCE_NUMERIC,
            max_failed_rows=0,
            on_failure=FailureBehavior.QUARANTINE_ROW,
        )

        report = InMemoDryRunSimulator.simulate(
            [action],
            sample,
            policies={action.action_type.value: policy},
            raise_on_block=False,
        )

        assert report.status == SafetyStatus.QUARANTINED
        assert report.is_safe_to_execute is True
        assert len(report.clean_records) == 2
        assert len(report.quarantined_records) == 1
        assert report.quarantined_records[0]["Amount"] == "bad_amount"
        assert report.clean_records[0]["Amount"] == 50.00
        assert report.clean_records[1]["Amount"] == 75.50

    def test_10_circuit_breaker_warn_mode_proceeds_with_diagnostics(self):
        """Test 10: WARN policy logs diagnostics and allows execution to proceed."""
        action = StructuredAction(
            action_type=ActionType.CAST_TYPE,
            dataset_name="Notes",
            target_columns=["Score"],
            parameters={"target_type": "int"},
            source=ActionSource.ASSESSMENT,
        )
        sample = [
            {"Score": "10"},
            {"Score": "unscored"},
        ]
        policy = TransformationPolicy(
            action_type=ActionType.CAST_TYPE,
            max_failed_rows=0,
            on_failure=FailureBehavior.WARN,
        )

        report = InMemoDryRunSimulator.simulate(
            [action],
            sample,
            policies={action.action_type.value: policy},
            raise_on_block=False,
        )

        assert report.status == SafetyStatus.WARNING
        assert report.is_safe_to_execute is True
        assert len(report.clean_records) == 2
        assert report.step_metrics[0].violations != []

    def test_11_circuit_breaker_skip_rule_reverts_offending_step(self):
        """Test 11: SKIP_RULE safely skips the offending transformation without corrupting data."""
        act1 = StructuredAction(
            action_type=ActionType.TRIM,
            dataset_name="Items",
            target_columns=["Code"],
            parameters={"side": "both"},
            source=ActionSource.ASSESSMENT,
        )
        act2 = StructuredAction(
            action_type=ActionType.CAST_TYPE,
            dataset_name="Items",
            target_columns=["Price"],
            parameters={"target_type": "float"},
            source=ActionSource.ASSESSMENT,
        )
        sample = [
            {"Code": " ITEM-A ", "Price": "invalid_price"},
        ]
        policy = TransformationPolicy(
            action_type=ActionType.CAST_TYPE,
            max_failed_rows=0,
            on_failure=FailureBehavior.SKIP_RULE,
        )

        report = InMemoDryRunSimulator.simulate(
            [act1, act2],
            sample,
            policies={"step_1_cast_type": policy, ActionType.CAST_TYPE.value: policy},
            raise_on_block=False,
        )

        assert report.step_metrics[0].status == SafetyStatus.PASSED
        assert report.step_metrics[1].status == SafetyStatus.SKIPPED
        # Price is retained as original 'invalid_price' rather than nullified
        assert report.clean_records[0]["Price"] == "invalid_price"
        # Prior step (TRIM) was preserved
        assert report.clean_records[0]["Code"] == "ITEM-A"

    def test_12_dry_run_simulates_all_level2_composable_operators(self):
        """Test 12: Dry run accurately simulates all 6 Level-2 composable operators."""
        actions = [
            StructuredAction(
                action_type=ActionType.PREFIX_ADD,
                dataset_name="D",
                target_columns=["Code"],
                parameters={"prefix": "PRE-", "only_if_missing": True},
                source=ActionSource.BUSINESS_RULE,
            ),
            StructuredAction(
                action_type=ActionType.SUFFIX_ADD,
                dataset_name="D",
                target_columns=["Code"],
                parameters={"suffix": "-SUF", "only_if_missing": True},
                source=ActionSource.BUSINESS_RULE,
            ),
            StructuredAction(
                action_type=ActionType.PAD,
                dataset_name="D",
                target_columns=["Seq"],
                parameters={"length": 5, "fill_character": "0", "side": "left"},
                source=ActionSource.BUSINESS_RULE,
            ),
            StructuredAction(
                action_type=ActionType.PATTERN_REPLACE,
                dataset_name="D",
                target_columns=["Notes"],
                parameters={"pattern": "/", "replacement": "-", "regex": False},
                source=ActionSource.BUSINESS_RULE,
            ),
            StructuredAction(
                action_type=ActionType.VALUE_MAP,
                dataset_name="D",
                target_columns=["Status"],
                parameters={"mappings": {"A": "Active", "I": "Inactive"}, "preserve_unmapped": True},
                source=ActionSource.BUSINESS_RULE,
            ),
            StructuredAction(
                action_type=ActionType.CONDITIONAL_UPDATE,
                dataset_name="D",
                target_columns=["Tier"],
                parameters={
                    "condition": {"column": "Status", "operator": "EQ", "value": "Active"},
                    "new_value": "Premium",
                },
                source=ActionSource.BUSINESS_RULE,
            ),
        ]
        sample = [
            {"Code": "100", "Seq": "42", "Notes": "2026/09/28", "Status": "A", "Tier": "Standard"},
            {"Code": "PRE-200-SUF", "Seq": "12345", "Notes": "OK", "Status": "I", "Tier": "Standard"},
        ]

        report = InMemoDryRunSimulator.simulate(actions, sample)

        assert report.status == SafetyStatus.PASSED
        r0 = report.clean_records[0]
        assert r0["Code"] == "PRE-100-SUF"
        assert r0["Seq"] == "00042"
        assert r0["Notes"] == "2026-09-28"
        assert r0["Status"] == "Active"
        assert r0["Tier"] == "Premium"

        r1 = report.clean_records[1]
        assert r1["Code"] == "PRE-200-SUF"  # Idempotent prefix & suffix
        assert r1["Seq"] == "12345"
        assert r1["Status"] == "Inactive"
        assert r1["Tier"] == "Standard"

    def test_13_dry_run_date_bounds_validation(self):
        """Test 13: Dates outside Azure SQL bounds (< 1753 or > 9999) are trapped."""
        action = StructuredAction(
            action_type=ActionType.PARSE_DATES,
            dataset_name="Events",
            target_columns=["EventDate"],
            parameters={},
            source=ActionSource.ASSESSMENT,
        )
        sample = [
            {"EventDate": "2026-09-28"},
            {"EventDate": "1650-01-01"},  # Year 1650 is out of bounds (< 1753)
            {"EventDate": "not_a_date"},
        ]
        policy = TransformationPolicy(
            action_type=ActionType.PARSE_DATES,
            max_failed_rows=5,
            on_failure=FailureBehavior.WARN,
        )

        report = InMemoDryRunSimulator.simulate(
            [action],
            sample,
            policies={action.action_type.value: policy},
            raise_on_block=False,
        )

        assert report.total_failed_rows == 2
        fail_reasons = [f.reason for f in report.step_metrics[0].conversion_failures]
        assert any("outside valid range" in r for r in fail_reasons)
        assert any("Unparseable date" in r for r in fail_reasons)

    def test_14_end_to_end_pipeline_integration_with_safety_and_tsql(self):
        """
        Test 14: Complete end-to-end integration:
          Actions -> ActionValidator -> LogicalPlanner -> InMemoDryRunSimulator -> PhysicalPlanner -> SQLCodegen -> SQLValidator
        """
        actions = [
            StructuredAction(
                action_type=ActionType.TRIM,
                dataset_name="Employees",
                target_columns=["EmployeeCode"],
                parameters={"side": "both"},
                source=ActionSource.BUSINESS_RULE,
            ),
            StructuredAction(
                action_type=ActionType.CAST_TYPE,
                dataset_name="Employees",
                target_columns=["Salary"],
                parameters={"target_type": "DECIMAL(18,2)", "max_failed_rows": 0},
                source=ActionSource.ASSESSMENT,
            ),
            StructuredAction(
                action_type=ActionType.PREFIX_ADD,
                dataset_name="Employees",
                target_columns=["EmployeeCode"],
                parameters={"prefix": "EMP-", "only_if_missing": True},
                source=ActionSource.BUSINESS_RULE,
            ),
        ]

        # 1. In-Memory Dry Run on sample batch
        sample_batch = [
            {"EmployeeCode": " 001 ", "Salary": "50000.00"},
            {"EmployeeCode": "EMP-002", "Salary": "65000.50"},
        ]
        dry_run = InMemoDryRunSimulator.simulate(actions, sample_batch)
        assert dry_run.status == SafetyStatus.PASSED
        assert dry_run.is_safe_to_execute is True

        # 2. Logical Planning
        ctx = CompilerContext(session_id="phase4_e2e_session", target_engine=ExecutionEngine.AZURE_SQL)
        ctx.structured_actions = actions
        l_planner = LogicalPlanner()
        ctx = l_planner.execute(ctx)
        assert ctx.logical_plan is not None

        # 3. Physical Planning with Defensive Safety
        p_planner = PhysicalPlanner()
        ctx = p_planner.execute(ctx)
        assert ctx.physical_plan is not None

        # 4. T-SQL Generation
        router = LanguageRouter.create_default()
        ctx = router.execute(ctx)
        assert ctx.code_artifact is not None

        sql_text = ctx.code_artifact.content

        # Verify defensive SQL elements
        assert "CREATE OR ALTER PROCEDURE" in sql_text
        assert "INSERT INTO [ETL_REJECT].[Rejection_Log]" in sql_text
        assert "IF @FailedCount_Salary > 0" in sql_text
        assert "RAISERROR" in sql_text

        # 5. SQL Validation Gate
        validator = SQLValidator()
        val_res = validator.validate(ctx.code_artifact, ctx)
        assert val_res.is_valid is True
        assert val_res.has_blocking_errors is False
