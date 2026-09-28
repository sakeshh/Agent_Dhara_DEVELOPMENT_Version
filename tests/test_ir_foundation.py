import pytest
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.models.physical import ExecutionEngine
from agent.feature_flags import FeatureFlags, get_flags
from agent.ir import (
    IrPlan,
    IntentSpec,
    IntentComplexity,
    SourceNode,
    JoinNode,
    JoinType,
    FilterNode,
    AggregateNode,
    AggregationSpec,
    AggregationFunc,
    ProjectionNode,
    OutputNode,
    OutputMode,
    IrStep,
    LineageSpec,
    ValidationSpec,
    PhysicalHints,
    IrMetadata,
    RuleExpression,
    RuleType,
    ComparisonOperator,
    CompoundLogicalOperator,
    IrValidator,
    IrBuilder,
    IrMigrator,
    IrErrorCode,
)
from agent.schema_cache import SchemaCache, CachedDatasetSchema


def test_ir_models_serialization():
    plan = IrPlan(
        ir_version="1.0",
        intent=IntentSpec(
            type="filter_and_project",
            complexity=IntentComplexity.SIMPLE,
            confidence=0.98,
            description="Find certified employees with score > 80",
        ),
        sources=[
            SourceNode(
                table="TrainingEnrollments",
                schema_name="dbo",
                columns=["EnrollmentID", "EmployeeID", "CertificationStatus", "Score"],
            )
        ],
        logical_plan=[
            IrStep(
                step_id="step_1",
                step_type="filter",
                filter_node=FilterNode(
                    table="TrainingEnrollments",
                    column="Score",
                    operator="gt",
                    value=80,
                ),
            )
        ],
        lineage=LineageSpec(
            source_tables=["dbo.TrainingEnrollments"],
            target_tables=["dbo.CertifiedEmployees"],
        ),
        validation=ValidationSpec(strict_schema=True),
        physical_hints=PhysicalHints(partition_count=4, broadcast_tables=["DimDept"]),
    )

    d = plan.model_dump()
    assert d["ir_version"] == "1.0"
    assert d["intent"]["complexity"] == "simple"
    assert d["lineage"]["target_tables"] == ["dbo.CertifiedEmployees"]
    assert d["physical_hints"]["partition_count"] == 4

    restored = IrPlan.model_validate(d)
    assert restored.intent.confidence == 0.98
    assert restored.sources[0].columns == ["EnrollmentID", "EmployeeID", "CertificationStatus", "Score"]


def test_rule_expression_dsl_no_eval():
    unique_rule = RuleExpression(type=RuleType.UNIQUE, column="EnrollmentID")
    sql_unique = unique_rule.to_sql_condition()
    assert "EnrollmentID" in sql_unique
    assert "COUNT(*)" in sql_unique

    range_rule = RuleExpression(
        type=RuleType.RANGE,
        column="Score",
        min_value=0,
        max_value=100,
    )
    sql_range = range_rule.to_sql_condition()
    assert "[Score] >= 0 AND [Score] <= 100" in sql_range

    spark_range = range_rule.to_pyspark_condition()
    assert "col('Score') >= 0" in spark_range
    assert "col('Score') <= 100" in spark_range

    gx_range = range_rule.to_gx_expectation()
    assert gx_range["expectation_type"] == "expect_column_values_to_be_between"
    assert gx_range["kwargs"]["min_value"] == 0


def test_ir_validator_missing_table_and_column_suggestions():
    profiles = {
        "TrainingEnrollments": DatasetProfile(
            dataset_id="TrainingEnrollments",
            columns={
                "EnrollmentID": ColumnProfile(name="EnrollmentID", dtype="int"),
                "EmployeeScore": ColumnProfile(name="EmployeeScore", dtype="float"),
            },
        )
    }

    # Plan with typo in table name and column name
    invalid_plan = IrPlan(
        sources=[
            SourceNode(
                table="TrainingEnrollment",  # Missing 's'
                columns=["EmployeeScor"],    # Missing 'e'
            )
        ]
    )

    res = IrValidator.validate(invalid_plan, profiles)
    assert not res.valid
    assert len(res.errors) >= 1
    # Check suggestion
    table_err = next(e for e in res.errors if e.error_code == IrErrorCode.MISSING_TABLE)
    assert "TrainingEnrollments" in table_err.suggestions


def test_ir_builder_generates_compiler_context():
    profiles = {
        "TrainingEnrollments": DatasetProfile(
            dataset_id="TrainingEnrollments",
            columns={
                "EnrollmentID": ColumnProfile(name="EnrollmentID", dtype="int"),
                "Score": ColumnProfile(name="Score", dtype="float"),
            },
        )
    }

    plan = IrPlan(
        sources=[SourceNode(table="TrainingEnrollments", columns=["EnrollmentID", "Score"])],
        business_rules=[
            RuleExpression(type=RuleType.UNIQUE, column="EnrollmentID", target_table="TrainingEnrollments"),
            RuleExpression(type=RuleType.RANGE, column="Score", min_value=0, max_value=100, target_table="TrainingEnrollments"),
        ],
        logical_plan=[
            IrStep(
                step_id="step_1",
                step_type="filter",
                filter_node=FilterNode(
                    table="TrainingEnrollments",
                    column="Score",
                    operator="gt",
                    value=80,
                ),
            )
        ],
        physical_hints=PhysicalHints(partition_count=8),
    )

    ctx = IrBuilder.build(
        ir_plan=plan,
        session_id="test_sess",
        target_engine=ExecutionEngine.AZURE_SQL,
        existing_profiles=profiles,
    )

    assert ctx.session_id == "test_sess"
    assert ctx.target_engine == ExecutionEngine.AZURE_SQL
    assert len(ctx.business_rules) == 2
    assert ctx.business_rules[0].parameters["rule_type"] == "unique"
    assert ctx.logical_plan is not None
    assert len(ctx.logical_plan.steps) == 1
    assert ctx.options["physical_hints"]["partition_count"] == 8


def test_ir_migrator_version_upgrade():
    legacy_plan = {
        "intent": "filter_and_project",
        "sources": [{"table": "Employees", "columns": ["ID", "Name"]}],
    }
    upgraded = IrMigrator.upgrade(legacy_plan, target_version="1.0")
    assert upgraded["ir_version"] == "1.0"
    assert isinstance(upgraded["intent"], dict)
    assert upgraded["intent"]["type"] == "filter_and_project"
    assert "lineage" in upgraded
    assert "validation" in upgraded


def test_schema_cache_operations():
    cache = SchemaCache()
    prof = DatasetProfile(
        dataset_id="DimCustomer",
        columns={
            "CustomerID": ColumnProfile(name="CustomerID", dtype="int", is_primary_key_candidate=True),
            "CustomerName": ColumnProfile(name="CustomerName", dtype="nvarchar(100)"),
        },
    )

    cached = cache.put_from_dataset_profile("sess_123", prof, ttl_seconds=60)
    assert cached.dataset_name == "DimCustomer"
    assert "CustomerID" in cached.columns
    assert cached.schema_hash != ""

    fetched = cache.get("sess_123", "DimCustomer")
    assert fetched is not None
    assert fetched.dataset_name == "DimCustomer"

    prompt_snippet = cache.to_llm_schema_prompt("sess_123")
    assert "DimCustomer" in prompt_snippet
    assert "CustomerID" in prompt_snippet
    assert "[PK]" in prompt_snippet

    cache.invalidate("sess_123")
    assert cache.get("sess_123", "DimCustomer") is None


def test_feature_flag_llm_ir_planner():
    flags = FeatureFlags()
    assert hasattr(flags, "llm_ir_planner_enabled")
    assert flags.llm_ir_planner_enabled is False  # Safe default


def test_ir_to_compiler_pipeline_roundtrip():
    from agent.compiler.compiler_pipeline import CompilerPipeline

    profiles = {
        "Employees": DatasetProfile(
            dataset_id="Employees",
            columns={
                "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
                "Score": ColumnProfile(name="Score", dtype="int"),
                "Certification": ColumnProfile(name="Certification", dtype="varchar(50)"),
            },
        )
    }

    plan = IrPlan(
        sources=[SourceNode(table="Employees", columns=["EmployeeID", "Score", "Certification"])],
        business_rules=[
            RuleExpression(type=RuleType.UNIQUE, column="EmployeeID", target_table="Employees"),
        ],
        logical_plan=[
            IrStep(
                step_id="step_1",
                step_type="filter",
                filter_node=FilterNode(
                    table="Employees",
                    column="Score",
                    operator="gt",
                    value=80,
                ),
            )
        ],
    )

    validation = IrValidator.validate(plan, profiles)
    assert validation.valid is True

    compiler_ctx = IrBuilder.build(
        ir_plan=plan,
        session_id="test_pipeline_sess",
        target_engine=ExecutionEngine.AZURE_SQL,
        existing_profiles=profiles,
    )

    pipeline = CompilerPipeline()
    compiled_ctx, snapshot = pipeline.compile(compiler_ctx)

    assert compiled_ctx.code_artifact is not None
    assert compiled_ctx.code_artifact.content != ""
    assert snapshot.target_engine == "sql"


def test_ir_normalizer_casing_and_operators():
    from agent.ir import IrNormalizer

    profiles = {
        "TrainingEnrollments": DatasetProfile(
            dataset_id="TrainingEnrollments",
            columns={
                "EnrollmentID": ColumnProfile(name="EnrollmentID", dtype="int"),
                "EmployeeScore": ColumnProfile(name="EmployeeScore", dtype="float"),
            },
        )
    }

    # IrPlan with lowercase names and raw symbols
    plan = IrPlan(
        sources=[SourceNode(table="trainingenrollments", columns=["enrollmentid", "employeescore"])],
        logical_plan=[
            IrStep(
                step_id="step_1",
                step_type="filter",
                filter_node=FilterNode(
                    table="trainingenrollments",
                    column="employeescore",
                    operator=">=",
                    value=80,
                ),
            )
        ],
        business_rules=[
            RuleExpression(type=RuleType.UNIQUE, column="enrollmentid", target_table="trainingenrollments")
        ],
    )

    normalized = IrNormalizer.normalize(plan, profiles)

    assert normalized.sources[0].table == "TrainingEnrollments"
    assert normalized.sources[0].columns == ["EnrollmentID", "EmployeeScore"]
    assert normalized.logical_plan[0].filter_node.table == "TrainingEnrollments"
    assert normalized.logical_plan[0].filter_node.column == "EmployeeScore"
    assert normalized.logical_plan[0].filter_node.operator == "gte"
    assert normalized.business_rules[0].target_table == "TrainingEnrollments"
    assert normalized.business_rules[0].column == "EnrollmentID"


def test_ir_extractor_end_to_end_pipeline():
    from agent.ir import IrExtractor, IrValidator, IrNormalizer, IrBuilder
    from agent.compiler.compiler_pipeline import CompilerPipeline

    profiles = {
        "TrainingEnrollments": DatasetProfile(
            dataset_id="TrainingEnrollments",
            columns={
                "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
                "Score": ColumnProfile(name="Score", dtype="int"),
                "CertificationStatus": ColumnProfile(name="CertificationStatus", dtype="varchar(50)"),
            },
        )
    }

    user_prompt = "Find employees with Score > 80 and deduplicate"
    extracted_ir = IrExtractor.extract(user_prompt, profiles, session_id="test_extract_sess")

    # Verify extracted output is pure IR, never code
    assert isinstance(extracted_ir, IrPlan)
    assert not any(kw in str(extracted_ir.model_dump()) for kw in ["SELECT", "FROM", "df.filter", "eval("])

    # Normalize FIRST, then Validate
    normalized = IrNormalizer.normalize(extracted_ir, profiles)
    validation = IrValidator.validate(normalized, profiles)
    assert validation.valid is True

    # Build CompilerContext
    ctx = IrBuilder.build(
        ir_plan=normalized,
        session_id="test_extract_sess",
        target_engine=ExecutionEngine.AZURE_SQL,
        existing_profiles=profiles,
    )

    # Compile deterministically
    pipeline = CompilerPipeline()
    compiled_ctx, snapshot = pipeline.compile(ctx)

    assert compiled_ctx.code_artifact is not None
    assert "Score" in compiled_ctx.code_artifact.content
    assert compiled_ctx.target_engine == ExecutionEngine.AZURE_SQL


def test_ir_fixer_repairs_unambiguous_column_typo():
    from agent.ir import IrFixer, IrValidator, IrNormalizer

    profiles = {
        "TrainingEnrollments": DatasetProfile(
            dataset_id="TrainingEnrollments",
            columns={
                "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
                "EmployeeScore": ColumnProfile(name="EmployeeScore", dtype="float"),
            },
        )
    }

    # IrPlan with single unambiguous typo "EmployeScore"
    plan = IrPlan(
        sources=[SourceNode(table="TrainingEnrollments", columns=["EmployeeID", "EmployeScore"])],
        logical_plan=[
            IrStep(
                step_id="step_1",
                step_type="filter",
                filter_node=FilterNode(
                    table="TrainingEnrollments",
                    column="EmployeScore",
                    operator="gte",
                    value=80,
                ),
            )
        ],
    )

    normalized = IrNormalizer.normalize(plan, profiles)
    val_res = IrValidator.validate(normalized, profiles)
    assert val_res.valid is False

    repaired_plan, final_val, is_fixed = IrFixer.fix(normalized, val_res, profiles)

    assert is_fixed is True
    assert final_val.valid is True
    assert repaired_plan.sources[0].columns == ["EmployeeID", "EmployeeScore"]
    assert repaired_plan.logical_plan[0].filter_node.column == "EmployeeScore"


def test_ir_fixer_level_3_conflict_detection_does_not_guess():
    from agent.ir import IrFixer, IrValidator, IrNormalizer

    profiles = {
        "Employees": DatasetProfile(
            dataset_id="Employees",
            columns={
                "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
                "EmployeeName": ColumnProfile(name="EmployeeName", dtype="varchar(100)"),
                "EmployeeStatus": ColumnProfile(name="EmployeeStatus", dtype="varchar(20)"),
            },
        )
    }

    # "Employee" is equally close to EmployeeID, EmployeeName, EmployeeStatus
    plan = IrPlan(
        sources=[SourceNode(table="Employees", columns=["Employee"])],
    )

    normalized = IrNormalizer.normalize(plan, profiles)
    val_res = IrValidator.validate(normalized, profiles)
    assert val_res.valid is False

    # Fixer should NOT guess because multiple close candidates exist (Level 3 Conflict)
    repaired_plan, final_val, is_fixed = IrFixer.fix(normalized, val_res, profiles)

    assert is_fixed is False
    assert final_val.valid is False


def test_ir_fixer_repairs_table_typo():
    from agent.ir import IrFixer, IrValidator, IrNormalizer

    profiles = {
        "TrainingEnrollments": DatasetProfile(
            dataset_id="TrainingEnrollments",
            columns={
                "EnrollmentID": ColumnProfile(name="EnrollmentID", dtype="int"),
            },
        )
    }

    # Table typo: TrainingEnrollment (missing 's')
    plan = IrPlan(
        sources=[SourceNode(table="TrainingEnrollment", columns=["EnrollmentID"])],
        lineage=LineageSpec(source_tables=["TrainingEnrollment"]),
    )

    normalized = IrNormalizer.normalize(plan, profiles)
    val_res = IrValidator.validate(normalized, profiles)
    assert val_res.valid is False

    repaired_plan, final_val, is_fixed = IrFixer.fix(normalized, val_res, profiles)

    assert is_fixed is True
    assert final_val.valid is True
    assert repaired_plan.sources[0].table == "TrainingEnrollments"
    assert "TrainingEnrollments" in repaired_plan.lineage.source_tables


def test_etl_handlers_ir_flow_with_feature_flag(monkeypatch):
    import os
    monkeypatch.setenv("DHARA_LLM_IR_PLANNER", "1")
    from agent.feature_flags import get_flags
    # Re-init flags for test
    flags = get_flags()
    flags.llm_ir_planner_enabled = True

    from agent.etl_handlers import _run_compiler_codegen

    assess = {
        "datasets": {
            "TrainingEnrollments": {
                "columns": {
                    "EmployeeID": {"dtype": "int"},
                    "Score": {"dtype": "int"},
                    "CertificationStatus": {"dtype": "string"},
                },
                "row_count": 100,
            }
        },
        "issues": [],
    }

    plan = {
        "user_prompt": "Find employees with score > 85",
        "cleaning_mode": "cleanse_with_audit",
    }
    ctx = {"etl_flow": {}}

    code, success, issues, mode = _run_compiler_codegen(
        session_id="test_etl_ir",
        engine="sql",
        plan=plan,
        assess=assess,
        ctx=ctx,
    )

    assert success is True
    assert mode == "compiler"
    assert "Score" in code


def test_ir_latency_benchmark():
    import time
    from agent.ir import IrExtractor, IrNormalizer, IrValidator, IrFixer, IrBuilder
    from agent.compiler.compiler_pipeline import CompilerPipeline

    profiles = {
        "TrainingEnrollments": DatasetProfile(
            dataset_id="TrainingEnrollments",
            columns={
                "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
                "Score": ColumnProfile(name="Score", dtype="int"),
            },
        )
    }

    t0 = time.perf_counter()
    raw_ir = IrExtractor.extract("Find employees with score > 80", profiles, session_id="bench_sess")
    norm_ir = IrNormalizer.normalize(raw_ir, profiles)
    val_res = IrValidator.validate(norm_ir, profiles)
    if not val_res.valid:
        norm_ir, val_res, _ = IrFixer.fix(norm_ir, val_res, profiles)
    ctx = IrBuilder.build(norm_ir, session_id="bench_sess", target_engine=ExecutionEngine.AZURE_SQL, existing_profiles=profiles)
    pipeline = CompilerPipeline()
    compiled_ctx, _ = pipeline.compile(ctx)
    elapsed_ms = (time.perf_counter() - t0) * 1000

    assert compiled_ctx.code_artifact is not None
    # Deterministic pipeline without network LLM should easily complete under 100ms
    assert elapsed_ms < 100.0, f"Simple IR pipeline took {elapsed_ms:.2f}ms (target: < 100ms)"


def test_ir_fixer_audit_trail_and_telemetry():
    from agent.ir import IrFixer, IrNormalizer

    profiles = {
        "TrainingEnrollments": DatasetProfile(
            dataset_id="TrainingEnrollments",
            columns={
                "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
                "EmployeeScore": ColumnProfile(name="EmployeeScore", dtype="float"),
            },
        )
    }

    # Plan with typo "EmployeScore"
    plan = IrPlan(
        sources=[SourceNode(table="TrainingEnrollments", columns=["EmployeeID", "EmployeScore"])],
    )

    norm_plan = IrNormalizer.normalize(plan, profiles)
    healed_plan, final_val, is_valid = IrFixer.heal_plan(norm_plan, profiles, max_attempts=2)

    assert is_valid is True
    assert final_val.valid is True

    # Audit Trail verification
    assert len(healed_plan.metadata.repair_history) == 1
    record = healed_plan.metadata.repair_history[0]
    assert record.original == "EmployeScore"
    assert record.corrected == "EmployeeScore"
    assert record.strategy == "fuzzy_match"
    assert record.confidence > 0.8
    assert record.target_entity == "column"
    assert record.table == "TrainingEnrollments"

    # Telemetry Metrics verification
    metrics = healed_plan.metadata.repair_metrics
    assert metrics is not None
    assert metrics.repairs_applied == 1
    assert metrics.fixer_invocations == 1
    assert metrics.validation_attempts >= 2
    assert metrics.final_validation_status == "repaired"
    assert metrics.repair_duration_ms > 0


def test_ir_fixer_bounded_max_attempts_on_unresolvable_ambiguity():
    from agent.ir import IrFixer, IrNormalizer

    profiles = {
        "Employees": DatasetProfile(
            dataset_id="Employees",
            columns={
                "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
                "EmployeeName": ColumnProfile(name="EmployeeName", dtype="varchar(100)"),
                "EmployeeStatus": ColumnProfile(name="EmployeeStatus", dtype="varchar(20)"),
            },
        )
    }

    # "Employee" is ambiguous: 3 candidates equally likely
    plan = IrPlan(
        sources=[SourceNode(table="Employees", columns=["Employee"])],
    )

    norm_plan = IrNormalizer.normalize(plan, profiles)
    healed_plan, final_val, is_valid = IrFixer.heal_plan(norm_plan, profiles, max_attempts=2)

    assert is_valid is False
    assert final_val.valid is False
    metrics = healed_plan.metadata.repair_metrics
    assert metrics is not None
    assert metrics.fixer_invocations <= 2  # Hard limit strictly respected
    assert metrics.final_validation_status == "failed"
    assert metrics.ambiguous_conflicts >= 1


def test_intent_router_classification_simple_moderate_complex():
    from agent.router import IntentRouter, RouteTarget

    # 1. Simple Filter query -> Compiler direct fast path
    res_simple = IntentRouter.classify("Show employees where score > 80 and deduplicate")
    assert res_simple.route == RouteTarget.COMPILER_DIRECT
    assert res_simple.complexity.value == "simple"
    assert res_simple.latency_ms < 10.0  # Must be sub-millisecond to ~few ms

    # 2. Moderate query with join and group by -> LLM IR planner
    res_mod = IntentRouter.classify("Join employees with training enrollments and group by department")
    assert res_mod.route == RouteTarget.LLM_IR_PLANNER
    assert res_mod.complexity.value == "moderate"

    # 3. Complex query with SCD Type 2 -> Multi-agent planner
    res_complex = IntentRouter.classify("Build an incremental SCD Type 2 dimension merge pipeline")
    assert res_complex.route == RouteTarget.MULTI_AGENT_PLANNER
    assert res_complex.complexity.value == "complex"


def test_business_rule_validator_contradictions():
    from agent.ir.semantic_validator import BusinessRuleValidator

    profiles = {
        "TrainingEnrollments": DatasetProfile(
            dataset_id="TrainingEnrollments",
            columns={
                "Score": ColumnProfile(name="Score", dtype="int"),
                "CertificationStatus": ColumnProfile(name="CertificationStatus", dtype="string"),
            },
        )
    }

    # 1. Impossible numeric range contradiction: Score > 90 AND Score < 50
    plan_contradiction = IrPlan(
        sources=[SourceNode(table="TrainingEnrollments", columns=["Score"])],
        logical_plan=[
            IrStep(
                step_id="s1",
                step_type="filter",
                filter_node=FilterNode(table="TrainingEnrollments", column="Score", operator="gt", value=90),
            ),
            IrStep(
                step_id="s2",
                step_type="filter",
                filter_node=FilterNode(table="TrainingEnrollments", column="Score", operator="lt", value=50),
            ),
        ],
    )

    res = BusinessRuleValidator.validate_semantics(plan_contradiction, profiles)
    assert res.valid is False
    assert any("Contradictory filter range" in e.message for e in res.errors)

    # 2. Cross-field semantic warning: Certified status with Score < 40
    plan_cross_field = IrPlan(
        sources=[SourceNode(table="TrainingEnrollments", columns=["Score", "CertificationStatus"])],
        logical_plan=[
            IrStep(
                step_id="s1",
                step_type="filter",
                filter_node=FilterNode(table="TrainingEnrollments", column="CertificationStatus", operator="eq", value="Certified"),
            ),
            IrStep(
                step_id="s2",
                step_type="filter",
                filter_node=FilterNode(table="TrainingEnrollments", column="Score", operator="lt", value=40),
            ),
        ],
    )

    res_cf = BusinessRuleValidator.validate_semantics(plan_cross_field, profiles)
    assert len(res_cf.warnings) >= 1
    assert any("Potential semantic conflict" in w.message for w in res_cf.warnings)


def test_compiler_metrics_observability():
    from agent.compiler.compiler_pipeline import CompilerPipeline

    profiles = {
        "Employees": DatasetProfile(
            dataset_id="Employees",
            columns={"EmployeeID": ColumnProfile(name="EmployeeID", dtype="int")},
        )
    }

    plan = IrPlan(
        sources=[SourceNode(table="Employees", columns=["EmployeeID"])],
    )

    ctx = IrBuilder.build(
        ir_plan=plan,
        session_id="test_metrics_sess",
        target_engine=ExecutionEngine.AZURE_SQL,
        existing_profiles=profiles,
    )

    pipeline = CompilerPipeline()
    compiled_ctx, snapshot = pipeline.compile(ctx)

    assert compiled_ctx.metrics is not None
    assert compiled_ctx.metrics.compile_latency_ms > 0
    assert compiled_ctx.metrics.target_language == "sql"
    assert compiled_ctx.metrics.success is True


def test_rag_store_business_rules_and_contracts():
    from agent.rag import RagStore, RagEntityType

    store = RagStore()
    store.add_business_rule(
        rule_id="rule_cert_min_score",
        title="Certification Minimum Score",
        rule_text="Certified status strictly requires Score >= 60 and Attendance >= 75%",
        tags=["certification", "score", "threshold"],
    )
    store.add_data_contract(
        contract_id="contract_training",
        table_name="TrainingEnrollments",
        contract_spec={"primary_key": "EnrollmentID", "not_null": ["EmployeeID"]},
    )

    results = store.search("Certified employee score threshold", limit=2)
    assert len(results) >= 1
    top = results[0]
    assert top.item.entity_type == RagEntityType.BUSINESS_RULE
    assert "Score >= 60" in top.item.content
    assert top.score > 0.4


def test_rag_retriever_context_formatting():
    from agent.rag import RagStore, RagRetriever

    store = RagStore()
    store.add_business_rule(
        rule_id="rule_score_cutoff",
        title="Passing Grade Cutoff",
        rule_text="A passing grade requires score at least 60",
        tags=["score", "passing"],
    )

    retriever = RagRetriever(store=store)
    ctx = retriever.retrieve_context("Filter passing score", table_names=["TrainingEnrollments"])

    assert len(ctx.business_rules) >= 1
    assert "### APPLICABLE BUSINESS RULES" in ctx.formatted_prompt_snippet
    assert "score at least 60" in ctx.formatted_prompt_snippet
    assert ctx.retrieval_latency_ms < 50.0  # Fast in-memory retrieval


def test_rag_ir_pattern_memory_reuse():
    from agent.rag import get_rag_store
    from agent.ir import IrExtractor

    store = get_rag_store()

    historical_ir = IrPlan(
        ir_version="1.0",
        intent=IntentSpec(type="certified_filter", complexity=IntentComplexity.SIMPLE),
        sources=[SourceNode(table="TrainingEnrollments", columns=["EmployeeID", "Score", "CertificationStatus"])],
        logical_plan=[
            IrStep(
                step_id="step_filter",
                step_type="filter",
                filter_node=FilterNode(
                    table="TrainingEnrollments",
                    column="CertificationStatus",
                    operator="eq",
                    value="Certified",
                ),
            )
        ],
    )

    pattern_prompt = "Retrieve all certified employees from training"
    store.add_ir_pattern(
        pattern_id="pat_cert_01",
        user_prompt=pattern_prompt,
        ir_plan=historical_ir,
        target_engine="sql",
        success_score=1.0,
    )

    profiles = {
        "TrainingEnrollments": DatasetProfile(
            dataset_id="TrainingEnrollments",
            columns={
                "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
                "Score": ColumnProfile(name="Score", dtype="int"),
                "CertificationStatus": ColumnProfile(name="CertificationStatus", dtype="string"),
            },
        )
    }

    # Query matching historical pattern
    reused = IrExtractor.extract(pattern_prompt, profiles, session_id="rag_mem_sess")

    assert reused.metadata.planner_model == "rag_pattern_memory"
    assert reused.intent.type == "certified_filter"
    assert len(reused.logical_plan) == 1
    assert reused.logical_plan[0].filter_node.value == "Certified"


def test_pattern_scoring_composite_ranking():
    from agent.rag import RagStore

    store = RagStore()

    good_plan = IrPlan(
        ir_version="1.0",
        intent=IntentSpec(type="high_quality_plan"),
        sources=[SourceNode(table="Employees", columns=["ID", "Score"])],
    )
    poor_plan = IrPlan(
        ir_version="1.0",
        intent=IntentSpec(type="poor_quality_plan"),
        sources=[SourceNode(table="Employees", columns=["ID", "Score"])],
    )

    # Good pattern: 100% success rate, 5.0 rating
    store.add_ir_pattern(
        pattern_id="pat_good",
        user_prompt="Filter employees by score threshold",
        ir_plan=good_plan,
        success_score=1.0,
        execution_success=1.0,
        user_rating=5.0,
    )

    # Poor pattern: 40% success rate, 1.0 rating
    store.add_ir_pattern(
        pattern_id="pat_poor",
        user_prompt="Filter employees by score threshold exactly",
        ir_plan=poor_plan,
        success_score=0.40,
        execution_success=0.30,
        user_rating=1.0,
    )

    top_plan = store.get_top_ir_pattern("Filter employees by score threshold")
    assert top_plan is not None
    # Good plan should win due to composite scoring
    assert top_plan.intent.type == "high_quality_plan"


def test_multi_target_emitter_factory():
    from agent.ir.emitters import EmitterFactory, CompilerTarget

    profiles = {
        "TrainingEnrollments": DatasetProfile(
            dataset_id="TrainingEnrollments",
            columns={
                "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
                "Score": ColumnProfile(name="Score", dtype="int"),
            },
        )
    }

    plan = IrPlan(
        sources=[SourceNode(table="TrainingEnrollments", columns=["EmployeeID", "Score"])],
        logical_plan=[
            IrStep(
                step_id="s1",
                step_type="filter",
                filter_node=FilterNode(table="TrainingEnrollments", column="Score", operator="gt", value=80),
            )
        ],
    )

    # 1. SQL Emitter
    sql_artifact = EmitterFactory.emit(CompilerTarget.SQL, plan, profiles, session_id="emit_sql")
    assert sql_artifact.language == "sql"
    assert "Score" in sql_artifact.content

    # 2. Python Emitter
    py_artifact = EmitterFactory.emit(CompilerTarget.PYTHON, plan, profiles, session_id="emit_py")
    assert py_artifact.language == "python"
    assert "Score" in py_artifact.content

    # 3. ADF Emitter
    adf_artifact = EmitterFactory.emit(CompilerTarget.ADF, plan, profiles, session_id="emit_adf")
    assert "activity" in adf_artifact.content.lower() or "pipeline" in adf_artifact.content.lower()

    # 4. Fabric Emitter
    fabric_artifact = EmitterFactory.emit(CompilerTarget.FABRIC, plan, profiles, session_id="emit_fabric")
    assert fabric_artifact.content != ""

    # 5. Emit All from single canonical IR
    all_artifacts = EmitterFactory.emit_all(
        [CompilerTarget.SQL, CompilerTarget.PYTHON, CompilerTarget.ADF, CompilerTarget.FABRIC],
        plan,
        profiles,
        session_id="emit_all",
    )
    assert len(all_artifacts) == 4
    assert "sql" in all_artifacts
    assert "python" in all_artifacts
    assert "adf" in all_artifacts
    assert "fabric" in all_artifacts


def test_explain_plan_generator_sql_and_spark():
    from agent.compiler.execution_validation import ExplainPlanGenerator
    from agent.compiler.models.context import CompilerContext
    from agent.compiler.models.physical import ExecutionEngine
    from agent.compiler.models.profile import DatasetProfile, ColumnProfile
    from agent.compiler.models.logical import LogicalPlan, LogicalStep, ActionType

    profiles = {
        "Orders": DatasetProfile(
            dataset_id="Orders",
            row_count=100000,
            primary_key_columns=["OrderID"],
            columns={"OrderID": ColumnProfile(name="OrderID", dtype="int")},
        )
    }

    ctx_sql = CompilerContext(
        session_id="exp_sql",
        target_engine=ExecutionEngine.AZURE_SQL,
        dataset_profiles=profiles,
        logical_plan=LogicalPlan(
            plan_id="p_sql",
            steps=[
                LogicalStep(step_id="s1", dataset_name="Orders", action=ActionType.RANGE_CLIP, target_columns=["OrderID"], parameters={"expression": "OrderID > 100"}),
            ]
        ),
    )
    sql_plan = ExplainPlanGenerator.generate(ctx_sql)
    assert "SHOWPLAN_TEXT" in sql_plan
    assert "Index Seek" in sql_plan or "Scan" in sql_plan

    ctx_spark = CompilerContext(
        session_id="exp_spark",
        target_engine=ExecutionEngine.PYSPARK,
        dataset_profiles=profiles,
        logical_plan=LogicalPlan(
            plan_id="p_spark",
            steps=[
                LogicalStep(step_id="s1", dataset_name="Orders", action=ActionType.DEDUPLICATE, target_columns=["OrderID"]),
            ]
        ),
    )
    spark_plan = ExplainPlanGenerator.generate(ctx_spark)
    assert "df.explain" in spark_plan
    assert "Exchange hashpartitioning" in spark_plan


def test_cost_estimator_metrics_and_broadcast():
    from agent.compiler.execution_validation import RuntimeCostEstimator
    from agent.compiler.models.context import CompilerContext
    from agent.compiler.models.physical import ExecutionEngine
    from agent.compiler.models.profile import DatasetProfile, ColumnProfile
    from agent.compiler.models.logical import LogicalPlan, LogicalStep, ActionType

    # Small table + Large table to test broadcast recommendation
    profiles = {
        "DimLookup": DatasetProfile(dataset_id="DimLookup", row_count=1000),
        "FactSales": DatasetProfile(dataset_id="FactSales", row_count=1500000),
    }

    ctx = CompilerContext(
        session_id="cost_test",
        target_engine=ExecutionEngine.PYSPARK,
        dataset_profiles=profiles,
        logical_plan=LogicalPlan(
            plan_id="p_cost",
            steps=[
                LogicalStep(step_id="s1", dataset_name="FactSales", action=ActionType.CUSTOM_EXPRESSION, target_columns=["Key"], parameters={"right_dataset": "DimLookup", "expression": "join"}),
                LogicalStep(step_id="s2", dataset_name="FactSales", action=ActionType.DEDUPLICATE, target_columns=["Key"]),
            ]
        ),
    )
    cost = RuntimeCostEstimator.estimate(ctx)
    assert cost.estimated_cost in ("medium", "high")
    assert cost.shuffle_operations >= 1
    assert cost.broadcast_recommended is True
    assert len(cost.recommendations) > 0


def test_execution_validator_full_pipeline():
    from agent.compiler.execution_validation import ExecutionValidator
    from agent.compiler.models.context import CompilerContext
    from agent.compiler.models.artifacts import CodeArtifact
    from agent.compiler.models.physical import ExecutionEngine
    from agent.compiler.models.profile import DatasetProfile

    ctx = CompilerContext(
        session_id="exec_val_test",
        target_engine=ExecutionEngine.AZURE_SQL,
        dataset_profiles={"Users": DatasetProfile(dataset_id="Users", row_count=5000)},
        code_artifact=CodeArtifact(
            artifact_id="art_sql_1",
            name="test_script.sql",
            language="sql",
            file_extension=".sql",
            content="SELECT [UserID], [Score] FROM [Users] WHERE [Score] > 80;",
        ),
    )

    result = ExecutionValidator.validate(ctx)
    assert result.valid is True
    assert result.target_engine == "sql"
    assert result.explain_type == "simulated"
    assert result.confidence_score >= 0.9
    assert len(result.optimization_suggestions) > 0
    assert result.explain_plan != ""
    assert result.cost_estimate is not None
    assert "execution_validation" in ctx.metadata


def test_emitters_attach_execution_validation_metadata():
    from agent.ir.emitters import EmitterFactory, CompilerTarget
    from agent.ir.models import IrPlan, SourceNode, FilterNode, IrStep
    from agent.compiler.models.profile import DatasetProfile, ColumnProfile

    profiles = {
        "Employees": DatasetProfile(
            dataset_id="Employees",
            columns={"EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"), "Dept": ColumnProfile(name="Dept", dtype="varchar")},
        )
    }

    plan = IrPlan(
        sources=[SourceNode(table="Employees", columns=["EmployeeID", "Dept"])],
        logical_plan=[
            IrStep(
                step_id="s1",
                step_type="filter",
                filter_node=FilterNode(table="Employees", column="Dept", operator="eq", value="Engineering"),
            )
        ],
    )

    artifact = EmitterFactory.emit(CompilerTarget.SQL, plan, profiles, session_id="val_meta_test")
    assert "execution_validation" in artifact.metadata
    val_meta = artifact.metadata["execution_validation"]
    assert val_meta["valid"] is True
    assert "explain_plan" in val_meta
    assert "cost_estimate" in val_meta


def test_pattern_learning_engine_compilation_and_search():
    import tempfile
    import os
    from agent.learning import PatternLearningEngine
    from agent.ir.models import IrPlan, SourceNode, FilterNode, IrStep
    from agent.compiler.execution_validation.models import ExecutionValidationResult, CostEstimate

    with tempfile.TemporaryDirectory() as tmpdir:
        store_path = os.path.join(tmpdir, "patterns.json")
        engine = PatternLearningEngine(persistence_path=store_path)

        plan = IrPlan(
            sources=[SourceNode(table="Customers", columns=["CustomerID", "Active"])],
            logical_plan=[
                IrStep(
                    step_id="s1",
                    step_type="filter",
                    filter_node=FilterNode(table="Customers", column="Active", operator="eq", value=1),
                )
            ],
        )

        rec = engine.learn_from_compilation(
            session_id="sess_123",
            user_prompt="Filter all active customers",
            ir_plan=plan,
            validation_result=ExecutionValidationResult(valid=True, target_engine="sql"),
            cost_estimate=CostEstimate(estimated_cost="low", shuffle_operations=0),
            target_engine="sql",
        )

        assert rec.pattern_id.startswith("pat_")
        assert rec.prompt == "Filter all active customers"
        assert rec.ir_plan.sources[0].table == "Customers"
        # Verify NO raw generated code is stored in the record
        assert not hasattr(rec, "code") or not getattr(rec, "code", None)

        # Pattern search
        matched_plan = engine.get_best_pattern("active customers")
        assert matched_plan is not None
        assert matched_plan.sources[0].table == "Customers"


def test_pattern_learning_engine_execution_outcome_feedback_loop():
    from agent.learning import PatternLearningEngine
    from agent.ir.models import IrPlan, SourceNode

    engine = PatternLearningEngine()
    plan = IrPlan(sources=[SourceNode(table="Orders", columns=["OrderID", "Amount"])])

    rec = engine.learn_from_compilation(
        session_id="sess_orders",
        user_prompt="Get all high value orders",
        ir_plan=plan,
    )

    pid = rec.pattern_id
    initial_score = rec.rank_score

    # 1. Record successful execution
    upd = engine.record_execution_outcome(pid, execution_success=True, runtime_ms=320.5)
    assert upd is not None
    assert upd.total_executions == 1
    assert upd.successful_executions == 1
    assert upd.execution_runtime_ms == 320.5

    # 2. Record another execution that failed
    upd2 = engine.record_execution_outcome(pid, execution_success=False, runtime_ms=1200.0, error_message="Timeout")
    assert upd2.total_executions == 2
    assert upd2.successful_executions == 1
    assert upd2.execution_error == "Timeout"


def test_pattern_learning_engine_user_feedback_and_summary():
    from agent.learning import PatternLearningEngine
    from agent.ir.models import IrPlan, SourceNode

    engine = PatternLearningEngine()
    plan = IrPlan(sources=[SourceNode(table="Employees", columns=["EmpID"])])

    rec = engine.learn_from_compilation(
        session_id="sess_emp",
        user_prompt="List all employees",
        ir_plan=plan,
    )

    # Record ratings
    engine.record_user_feedback(rec.pattern_id, user_rating=5.0, feedback_comment="Perfect query", user_action="approved")
    engine.record_user_feedback(rec.pattern_id, user_rating=4.0)

    pat = engine.get_pattern(rec.pattern_id)
    assert pat.total_ratings == 2
    assert pat.average_rating == 4.5
    assert "approved" == pat.user_action

    summary = engine.get_learning_summary()
    assert summary.total_patterns_learned >= 1
    assert "Employees" in summary.top_domains


def test_pattern_learning_engine_export_and_load():
    import tempfile
    import os
    from agent.learning import PatternLearningEngine
    from agent.ir.models import IrPlan, SourceNode

    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "learned_patterns.json")
        engine1 = PatternLearningEngine()
        plan = IrPlan(sources=[SourceNode(table="Inventory", columns=["SKU", "Qty"])])

        rec = engine1.learn_from_compilation(
            session_id="sess_inv",
            user_prompt="Low inventory items",
            ir_plan=plan,
        )
        engine1.record_execution_outcome(rec.pattern_id, execution_success=True)
        engine1.export_patterns(filepath)

        # Load into separate engine instance
        engine2 = PatternLearningEngine(persistence_path=filepath)
        loaded_rec = engine2.get_pattern(rec.pattern_id)
        assert loaded_rec is not None
        assert loaded_rec.prompt == "Low inventory items"
        assert loaded_rec.successful_executions == 1


def test_pattern_stability_score():
    from agent.learning import PatternLearningEngine
    from agent.ir.models import IrPlan, SourceNode, RepairRecord

    engine = PatternLearningEngine()
    plan = IrPlan(
        sources=[SourceNode(table="Orders", columns=["OrderID"])],
        metadata={"repair_history": [RepairRecord(original="OrdID", corrected="OrderID", error_code="TYPO", strategy="fuzzy_match", confidence=0.95)]},
    )

    rec = engine.learn_from_compilation(session_id="sess_stab", user_prompt="Stable orders", ir_plan=plan)

    # Initially before execution: stability is 1.0
    assert rec.stability_score == 1.0

    # Execute 5 times, 5 successes
    for _ in range(5):
        upd = engine.record_execution_outcome(rec.pattern_id, execution_success=True, runtime_ms=50.0)

    # 100% success with 1 repair -> high stability
    assert upd.stability_score >= 0.95

    # 1 failure drops stability score
    upd_fail = engine.record_execution_outcome(rec.pattern_id, execution_success=False, error_message="Disk full")
    assert upd_fail.stability_score < 0.85


def test_human_approval_auto_approved_read_cleaning():
    from agent.governance import HumanApprovalEngine, ApprovalTier, ApprovalStatus, RiskLevel

    engine = HumanApprovalEngine()
    safe_sql = "SELECT [EmployeeID], [Department] FROM [Employees] WHERE [Score] > 80;"

    decision = engine.evaluate(session_id="sess_safe", code=safe_sql)
    assert decision.approval_tier == ApprovalTier.AUTO_APPROVED
    assert decision.risk_level == RiskLevel.LOW
    assert decision.status == ApprovalStatus.AUTO_APPROVED
    assert engine.is_execution_permitted(decision.approval_id) is True


def test_human_approval_warning_required_overwrite():
    from agent.governance import HumanApprovalEngine, ApprovalTier, ApprovalStatus, RiskLevel

    engine = HumanApprovalEngine()
    overwrite_sql = "INSERT OVERWRITE TABLE [Staging_Curated] SELECT * FROM [Raw_Staging];"

    decision = engine.evaluate(session_id="sess_warn", code=overwrite_sql)
    assert decision.approval_tier == ApprovalTier.WARNING_REQUIRED
    assert decision.risk_level == RiskLevel.MEDIUM
    assert decision.status == ApprovalStatus.PENDING
    assert engine.is_execution_permitted(decision.approval_id) is False

    # Lead architect authorizes warning
    engine.approve(decision.approval_id, approver="lead_architect", reason="Daily staging cycle approved")
    assert engine.is_execution_permitted(decision.approval_id) is True


def test_human_approval_mandatory_drop_delete_truncate():
    from agent.governance import HumanApprovalEngine, ApprovalTier, ApprovalStatus, RiskLevel

    engine = HumanApprovalEngine()
    destructive_sql = "DROP TABLE [Legacy_Customers]; TRUNCATE TABLE [Audit_Logs];"

    decision = engine.evaluate(session_id="sess_destruct", code=destructive_sql)
    assert decision.approval_tier == ApprovalTier.MANDATORY_HUMAN_APPROVAL
    assert decision.risk_level == RiskLevel.CRITICAL
    assert decision.status == ApprovalStatus.PENDING
    assert len(decision.destructive_actions) >= 2
    assert engine.is_execution_permitted(decision.approval_id) is False

    # Lead data architect approves
    approved = engine.approve(decision.approval_id, approver="chief_data_officer", reason="Approved migration")
    assert approved.status == ApprovalStatus.APPROVED
    assert engine.is_execution_permitted(decision.approval_id) is True

    # Test rejection path on a second destructive query
    decision2 = engine.evaluate(session_id="sess_delete", code="DELETE FROM [Production_Transactions];")
    rejected = engine.reject(decision2.approval_id, approver="security_officer", reason="Critical data loss prevented")
    assert rejected.status == ApprovalStatus.REJECTED
    assert engine.is_execution_permitted(decision2.approval_id) is False


def test_pattern_stability_tier_classification():
    from agent.learning import PatternLearningEngine
    from agent.learning.models import StabilityTier
    from agent.ir.models import IrPlan, SourceNode

    engine = PatternLearningEngine()
    plan = IrPlan(sources=[SourceNode(table="Metrics", columns=["ID"])])
    rec = engine.learn_from_compilation(session_id="sess_tier", user_prompt="Tier metrics", ir_plan=plan)

    # Initial state is PROVISIONAL (less than 3 runs)
    assert rec.stability_tier == StabilityTier.PROVISIONAL

    # 3 consecutive successful runs -> qualifies for GOLD tier (score >= 0.95 and runs >= 3)
    for _ in range(3):
        upd = engine.record_execution_outcome(rec.pattern_id, execution_success=True)

    assert upd.stability_tier == StabilityTier.GOLD

    # 1 failure drops tier to SILVER or BRONZE
    upd_fail = engine.record_execution_outcome(rec.pattern_id, execution_success=False)
    assert upd_fail.stability_tier in (StabilityTier.SILVER, StabilityTier.BRONZE, StabilityTier.PROVISIONAL)


def test_opentelemetry_exporter_span_creation_and_aggregation():
    from agent.observability import (
        OpenTelemetryExporter,
        RouteMetrics,
        IRMetrics,
        CompilerMetrics,
        ApprovalMetrics,
        ExecutionMetrics,
    )

    exporter = OpenTelemetryExporter()
    span = exporter.create_span(
        session_id="sess_otel_1",
        route=RouteMetrics(route_type="compiler_direct", route_latency_ms=2.1, confidence=0.99),
        ir=IRMetrics(validation_count=1, repair_count=0, repair_success=True),
        compiler=CompilerMetrics(target="sql", compile_time=0.045, artifact_size=520, success=True),
        approval=ApprovalMetrics(approval_tier="auto_approved", risk_level="low"),
        execution=ExecutionMetrics(runtime=0.15, cost_estimate="low", execution_success=True),
    )

    assert span.trace_id.startswith("trace_")
    assert span.span_id.startswith("span_")

    summary = exporter.get_summary()
    assert summary["total_compilations"] == 1
    assert summary["compilation_success_rate"] == 1.0
    assert summary["route_breakdown"]["compiler_direct"] == 1
    assert summary["target_breakdown"]["sql"] == 1
    assert summary["approval_tier_breakdown"]["auto_approved"] == 1


def test_opentelemetry_prometheus_text_export():
    from agent.observability import OpenTelemetryExporter, CompilerMetrics, RouteMetrics, ApprovalMetrics

    exporter = OpenTelemetryExporter()
    exporter.create_span(
        session_id="sess_prom",
        route=RouteMetrics(route_type="llm_ir_planner", route_latency_ms=12.5),
        compiler=CompilerMetrics(target="pyspark", compile_time=0.08, success=True),
        approval=ApprovalMetrics(approval_tier="warning_required"),
    )

    prom_text = exporter.export_prometheus()
    assert "# HELP dhara_compilations_total" in prom_text
    assert 'dhara_compilations_total{status="success"} 1' in prom_text
    assert 'dhara_route_requests_total{route="llm_ir_planner"} 1' in prom_text
    assert 'dhara_target_artifacts_total{target="pyspark"} 1' in prom_text
    assert 'dhara_approval_tiers_total{tier="warning_required"} 1' in prom_text
    assert "dhara_route_latency_ms_avg" in prom_text
    assert "dhara_compile_time_seconds_avg" in prom_text


def test_opentelemetry_otlp_json_export():
    from agent.observability import OpenTelemetryExporter, CompilerMetrics, ExecutionMetrics

    exporter = OpenTelemetryExporter()
    exporter.create_span(
        session_id="sess_otlp",
        compiler=CompilerMetrics(target="adf", compile_time=0.035, success=True),
        execution=ExecutionMetrics(cost_estimate="medium", confidence_score=0.96),
    )

    otlp_payload = exporter.export_otlp_json()
    assert "resourceSpans" in otlp_payload
    spans = otlp_payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
    assert len(spans) == 1
    assert spans[0]["name"] == "CompilerPipeline/adf"
    assert spans[0]["status"]["code"] == "STATUS_CODE_OK"




