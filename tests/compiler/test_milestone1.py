from __future__ import annotations

import pytest
from agent.compiler.models import (
    ActionType,
    BusinessRule,
    ColumnProfile,
    CompilerContext,
    DataEffect,
    DatasetProfile,
    ExecutionEngine,
    Issue,
    IssueCategory,
    IssueSeverity,
)
from agent.compiler.events import CompilerEventType, EventBus
from agent.compiler.planners import IssueCompiler, LogicalPlanner, ConfidenceAgent
from agent.compiler.optimizers import OptimizationAgent
from agent.compiler.physical import PhysicalPlanner


def test_canonical_models_creation():
    col = ColumnProfile(name="email", dtype="string", null_percentage=12.5)
    ds = DatasetProfile(dataset_name="customers", row_count=1000, columns={"email": col})
    assert ds.get_column("email").null_percentage == 12.5

    rule = BusinessRule(rule_id="r1", name="never_drop_rows", never_drop_rows=True)
    assert rule.never_drop_rows is True

    context = CompilerContext(
        session_id="test_session",
        target_engine=ExecutionEngine.PYTHON,
        dataset_profiles={"customers": ds},
        business_rules=[rule],
    )
    assert context.get_dataset("customers") is not None
    assert context.target_engine == ExecutionEngine.PYTHON


def test_event_bus():
    bus = EventBus.get_instance()
    bus.clear()

    received_events = []
    bus.subscribe(CompilerEventType.LOGICAL_PLAN_CREATED, lambda e: received_events.append(e))

    event = bus.emit(
        event_type=CompilerEventType.LOGICAL_PLAN_CREATED,
        session_id="sess_123",
        payload={"plan_id": "p1"},
        source_agent="TestAgent",
    )

    assert len(received_events) == 1
    assert received_events[0].payload["plan_id"] == "p1"
    assert received_events[0].source_agent == "TestAgent"


def test_issue_compiler():
    compiler = IssueCompiler()
    raw = {
        "issue_type": "null_values",
        "severity": "high",
        "column": "customer_id",
        "row_count_affected": 42,
        "message": "Nulls in primary key",
    }
    issue = compiler.compile_raw_issue(raw, "customers", 1)
    assert issue.category == IssueCategory.MISSING_VALUE
    assert issue.severity == IssueSeverity.HIGH
    assert issue.column_name == "customer_id"

    step = compiler.issue_to_logical_step(issue, 1)
    assert step.action == ActionType.FILL_NULLS
    assert step.target_columns == ["customer_id"]


def test_logical_planner_and_confidence():
    planner = LogicalPlanner()
    context = CompilerContext(
        session_id="sess_plan",
        target_engine=ExecutionEngine.PYTHON,
        dataset_profiles={
            "customers": DatasetProfile(dataset_name="customers", row_count=500)
        },
        detected_issues=[
            Issue(
                issue_id="iss_1",
                dataset_name="customers",
                column_name="email",
                category=IssueCategory.UNTRIMMED_WHITESPACE,
                message="Untrimmed whitespace in email",
            ),
            Issue(
                issue_id="iss_2",
                dataset_name="customers",
                column_name="email",
                category=IssueCategory.FORMAT_ANOMALY,
                message="Lowercase email addresses",
            ),
            Issue(
                issue_id="iss_3",
                dataset_name="customers",
                column_name="customer_id",
                category=IssueCategory.DUPLICATE,
                message="Duplicate customer IDs",
            ),
        ],
    )

    ctx, metrics = planner.run_with_metrics(context)
    assert ctx.logical_plan is not None
    assert len(ctx.logical_plan.steps) == 3
    assert metrics.success is True
    assert metrics.execution_ms >= 0

    # Priorities must be ordered: TRIM (5) < LOWERCASE (8) < DEDUPLICATE (200)
    actions = [s.action for s in ctx.logical_plan.steps]
    assert actions == [ActionType.TRIM, ActionType.LOWERCASE, ActionType.DEDUPLICATE]

    # Verify confidence scoring
    assert ctx.logical_plan.steps[0].confidence >= 0.90
    assert ctx.logical_plan.steps[0].requires_user_approval is False


def test_optimization_agent_operator_fusion_and_dedup():
    planner = LogicalPlanner()
    optimizer = OptimizationAgent()

    # Create issues that produce TRIM followed by LOWERCASE on same column
    context = CompilerContext(
        session_id="sess_opt",
        target_engine=ExecutionEngine.PYTHON,
        dataset_profiles={"customers": DatasetProfile(dataset_name="customers", row_count=100)},
        detected_issues=[
            Issue(
                issue_id="iss_1",
                dataset_name="customers",
                column_name="email",
                category=IssueCategory.UNTRIMMED_WHITESPACE,
                message="Whitespace in email",
            ),
            Issue(
                issue_id="iss_2",
                dataset_name="customers",
                column_name="email",
                category=IssueCategory.FORMAT_ANOMALY,
                message="Lowercase email",
            ),
            Issue(
                issue_id="iss_3",
                dataset_name="customers",
                column_name="email",
                category=IssueCategory.UNTRIMMED_WHITESPACE,
                message="Duplicate whitespace check on email",
            ),
        ],
    )

    ctx = planner.execute(context)
    assert len(ctx.logical_plan.steps) == 3  # TRIM, LOWERCASE, duplicate TRIM

    ctx, metrics = optimizer.run_with_metrics(ctx)
    assert metrics.success is True

    # Redundant duplicate TRIM was eliminated, and TRIM + LOWERCASE were fused!
    # Expected: 1 fused step [TRIM, LOWERCASE]
    assert len(ctx.logical_plan.steps) == 1
    fused_step = ctx.logical_plan.steps[0]
    assert fused_step.action == ActionType.LOWERCASE
    assert fused_step.fused_operations == [ActionType.TRIM, ActionType.LOWERCASE]


def test_physical_planner():
    planner = LogicalPlanner()
    optimizer = OptimizationAgent()
    phys_planner = PhysicalPlanner()

    context = CompilerContext(
        session_id="sess_phys",
        target_engine=ExecutionEngine.PYTHON,
        dataset_profiles={"customers": DatasetProfile(dataset_name="customers", row_count=100)},
        detected_issues=[
            Issue(
                issue_id="iss_1",
                dataset_name="customers",
                column_name="email",
                category=IssueCategory.UNTRIMMED_WHITESPACE,
                message="Whitespace in email",
            ),
            Issue(
                issue_id="iss_2",
                dataset_name="customers",
                column_name="customer_id",
                category=IssueCategory.DUPLICATE,
                message="Duplicate records",
            ),
        ],
    )

    ctx = planner.execute(context)
    ctx = optimizer.execute(ctx)
    ctx, metrics = phys_planner.run_with_metrics(ctx)

    assert metrics.success is True
    assert ctx.physical_plan is not None
    assert ctx.physical_plan.target_engine == ExecutionEngine.PYTHON
    assert len(ctx.physical_plan.steps) == 2

    step0 = ctx.physical_plan.steps[0]
    assert step0.strategy_name == "str_strip"
    assert step0.template_name == "str_strip.j2"

    step1 = ctx.physical_plan.steps[1]
    assert step1.strategy_name == "drop_duplicates"
    assert step1.template_name == "deduplicate.j2"
