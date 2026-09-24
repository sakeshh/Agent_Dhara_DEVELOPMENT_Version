from __future__ import annotations

import ast
import pytest
from agent.compiler.models import (
    ActionType,
    BusinessRule,
    ColumnProfile,
    CompilerContext,
    DatasetProfile,
    ExecutionEngine,
    Issue,
    IssueCategory,
)
from agent.compiler.planners import LogicalPlanner
from agent.compiler.optimizers import OptimizationAgent
from agent.compiler.physical import PhysicalPlanner
from agent.compiler.router import LanguageRouter
from agent.compiler.templates.engine import TemplateEngine
from agent.codegen.python import PythonCodegenAgent
from agent.compiler.validators import PythonValidator


def test_template_engine_basic_render():
    engine = TemplateEngine.get_instance()
    rendered = engine.render_string("Hello {{ name }}!", name="Dhara")
    assert rendered == "Hello Dhara!"


def test_python_vertical_slice_end_to_end():
    # 1. Setup CompilerContext with simulated assessment profile & issues
    email_col = ColumnProfile(name="email", dtype="string", null_percentage=5.0)
    id_col = ColumnProfile(name="customer_id", dtype="int", is_primary_key_candidate=True)
    dataset = DatasetProfile(
        dataset_name="customers",
        row_count=1000,
        columns={"email": email_col, "customer_id": id_col},
    )

    context = CompilerContext(
        session_id="vertical_slice_sess",
        target_engine=ExecutionEngine.PYTHON,
        dataset_profiles={"customers": dataset},
        detected_issues=[
            Issue(
                issue_id="iss_1",
                dataset_name="customers",
                column_name="email",
                category=IssueCategory.UNTRIMMED_WHITESPACE,
                suggested_action="trim",
                message="Untrimmed whitespace in email",
            ),
            Issue(
                issue_id="iss_2",
                dataset_name="customers",
                column_name="email",
                category=IssueCategory.FORMAT_ANOMALY,
                suggested_action="lowercase",
                message="Mixed-case email addresses",
            ),
            Issue(
                issue_id="iss_3",
                dataset_name="customers",
                column_name="customer_id",
                category=IssueCategory.DUPLICATE,
                suggested_action="deduplicate",
                message="Duplicate customer IDs",
            ),
        ],
    )

    # 2. Stage 1: Logical Planning
    logical_planner = LogicalPlanner()
    context = logical_planner.execute(context)
    assert context.logical_plan is not None
    assert len(context.logical_plan.steps) == 3

    # 3. Stage 2: AST Optimization (fuses trim + lowercase on email)
    optimizer = OptimizationAgent()
    context = optimizer.execute(context)
    # Fused into 2 steps: 1 fused (trim + lower on email), 1 deduplicate on customer_id
    assert len(context.logical_plan.steps) == 2

    # 4. Stage 3: Physical Planning
    physical_planner = PhysicalPlanner()
    context = physical_planner.execute(context)
    assert context.physical_plan is not None
    assert len(context.physical_plan.steps) == 2

    # 5. Stage 4: Language Routing & Code Generation
    router = LanguageRouter()
    router.register(ExecutionEngine.PYTHON, PythonCodegenAgent)
    context = router.execute(context)

    assert context.code_artifact is not None
    assert context.code_artifact.language == "python"
    code = context.code_artifact.content

    # Verify generated Python code contains expected constructs
    assert "def transform_customers(df: pd.DataFrame)" in code
    assert "df_clean[\"email\"].astype(str).str.strip().str.lower()" in code
    assert "df_clean = df_clean.drop_duplicates" in code

    # 6. Stage 5: Validation Agent
    validator = PythonValidator()
    context = validator.execute(context)

    assert context.validation_result is not None
    assert context.validation_result.is_valid is True
    assert context.validation_result.error_count == 0

    # 7. Verify the code parses into a valid Python AST without syntax errors
    parsed = ast.parse(code)
    assert parsed is not None


def test_python_validator_catches_unsafe_code():
    from agent.compiler.models import CodeArtifact

    unsafe_artifact = CodeArtifact(
        artifact_id="art_unsafe",
        name="test_unsafe.py",
        language="python",
        content="import os\n\ndef run():\n    eval('2 + 2')\n",
        file_extension=".py",
    )

    context = CompilerContext(session_id="test_unsafe", code_artifact=unsafe_artifact)
    validator = PythonValidator()
    res = validator.validate(unsafe_artifact, context)

    assert res.is_valid is False
    assert res.has_security_hazards is True
    codes = [i.issue_code for i in res.issues]
    assert "PY_SEC_HAZARD" in codes
