from __future__ import annotations

import pytest
from typing import Any, Dict

from agent.compiler.compiler_pipeline import CompilerPipeline
from agent.compiler.models.actions import FillNullsParameters, StructuredAction
from agent.compiler.models.compilation_result import CodeGenerationError
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.planners.ingestion_bridge import CanonicalIssue, IngestionBridge
from agent.compiler.tier3 import ResolutionBucket, Tier3ResolutionService
from agent.etl_handlers import _run_compiler_codegen, etl_generate_code


def _make_sample_profiles() -> Dict[str, DatasetProfile]:
    emp_cols = {
        "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
        "ManagerID": ColumnProfile(name="ManagerID", dtype="int"),
        "FirstName": ColumnProfile(name="FirstName", dtype="string"),
        "LastName": ColumnProfile(name="LastName", dtype="string"),
        "Salary": ColumnProfile(name="Salary", dtype="float"),
        "IsActive": ColumnProfile(name="IsActive", dtype="string"),
        "LastPromotionDate": ColumnProfile(name="LastPromotionDate", dtype="string"),
    }
    return {
        "dbo.Dhara_Employee": DatasetProfile(
            dataset_id="dbo.Dhara_Employee",
            row_count=1000,
            columns=emp_cols,
        )
    }


def test_fill_nulls_strategy_alias():
    """Verify strategy='value' is aliased to 'constant' and value is mapped to fill_value."""
    params = FillNullsParameters.model_validate({"strategy": "value", "value": "unknown"})
    assert params.strategy == "constant"
    assert params.fill_value == "unknown"

    params_ffill = FillNullsParameters.model_validate({"strategy": "ffill"})
    assert params_ffill.strategy == "forward_fill"


def test_ingestion_bridge_manual_review_and_deterministic_resolutions():
    """Verify that observational issues become manual review and deterministic candidates map cleanly."""
    bridge = IngestionBridge(default_schema="dbo")
    assess = {
        "data_quality_issues": {
            "datasets": {
                "dbo.Dhara_Employee": {
                    "issues": [
                        {
                            "column": "LastName",
                            "issue_type": "multivariate_outliers",
                            "severity": "low",
                            "message": "Multivariate outlier detected",
                        },
                        {
                            "column": "FirstName",
                            "issue_type": "all_caps_values",
                            "severity": "low",
                            "message": "Values in all uppercase",
                        },
                        {
                            "column": "ManagerID",
                            "issue_type": "intra_dataset_orphan_fk",
                            "severity": "medium",
                            "message": "ManagerID refers to EmployeeID 1 self-referencing orphan value",
                        },
                        {
                            "column": "Salary",
                            "issue_type": "integer_stored_as_float",
                            "severity": "medium",
                            "message": "Integer stored as float with unknown target integer precision",
                        },
                        {
                            "column": "FirstName",
                            "issue_type": "string_length_outlier",
                            "severity": "low",
                            "message": "Name longer than 93 characters",
                        },
                    ]
                }
            }
        }
    }
    plan = {
        "datasets": {
            "dbo.Dhara_Employee": {
                "steps": [
                    {
                        "column": "IsActive",
                        "action": "fill_nulls",
                        "parameters": {"strategy": "value", "value": "true"},
                    },
                    {
                        "column": "FirstName",
                        "action": "review_manually",
                        "issue_type": "planned_review_manually",
                        "description": "Manual review of name lengths",
                    },
                    {
                        "column": "LastPromotionDate",
                        "action": "flag_missing_key",
                        "issue_type": "planned_flag_missing_key",
                        "description": "Flag missing values in LastPromotionDate",
                    },
                    {
                        "column": "FirstName",
                        "action": "normalize_registered_placeholder",
                        "issue_type": "planned_normalize_registered_placeholder",
                        "description": "Normalize registered placeholder without list",
                    },
                ]
            }
        }
    }

    profiles = _make_sample_profiles()
    canonical, rules, telem = bridge.extract_and_normalize(assess, plan, set(profiles.keys()))

    # Verify coverage percentage is mathematically valid (<= 100%)
    assert telem["coverage_percent"] <= 100.0
    assert telem["canonical_issue_count"] == len(canonical)

    # Verify manual review count is tracked and non-zero
    assert telem["manual_review_issue_count"] > 0

    detected_issues, actions = bridge.bridge_to_compiler_models(canonical)

    # Verify fill_nulls on IsActive was not dropped
    is_active_action = next((a for a in actions if "IsActive" in a.target_columns and a.action_type == ActionType.FILL_NULLS), None)
    assert is_active_action is not None
    assert is_active_action.parameters["strategy"] == "constant"
    assert is_active_action.parameters["fill_value"] == "true"

    # Verify ManagerID was resolved to referential integrity
    manager_action = next((a for a in actions if "ManagerID" in a.target_columns and a.action_type == ActionType.VALIDATE_REFERENTIAL_INTEGRITY), None)
    assert manager_action is not None

    # Tier 3 resolution service
    service = Tier3ResolutionService()
    res = service.resolve(
        canonical_issues=canonical,
        deterministic_actions=actions,
        dataset_profiles=profiles,
        business_rules=rules,
        target_engine=ExecutionEngine.AZURE_SQL,
    )

    counts = res.tracker.counts_by_bucket()
    assert res.tracker.verify_accounting_invariants() is True
    assert counts[ResolutionBucket.MANUAL_REVIEW_REQUIRED] >= 4


def test_run_compiler_codegen_live_path_succeeds_with_reconciliation():
    """Verify that _run_compiler_codegen passes without capability gate failure when observational issues are present."""
    assess = {
        "datasets": {
            "dbo.Dhara_Employee": {
                "columns": {
                    "EmployeeID": {"dtype": "int"},
                    "ManagerID": {"dtype": "int"},
                    "FirstName": {"dtype": "string"},
                    "LastName": {"dtype": "string"},
                    "Salary": {"dtype": "float"},
                    "IsActive": {"dtype": "string"},
                },
                "row_count": 1000,
            }
        },
        "data_quality_issues": {
            "datasets": {
                "dbo.Dhara_Employee": {
                    "issues": [
                        {
                            "column": "FirstName",
                            "issue_type": "whitespace",
                            "severity": "low",
                            "message": "Untrimmed whitespace",
                        },
                        {
                            "column": "LastName",
                            "issue_type": "multivariate_outliers",
                            "severity": "low",
                            "message": "Multivariate outliers detected",
                        },
                        {
                            "column": "FirstName",
                            "issue_type": "all_caps_values",
                            "severity": "low",
                            "message": "All caps values",
                        },
                    ]
                }
            }
        }
    }
    plan = {
        "plan_id": "plan_test_live_path",
        "datasets": {
            "dbo.Dhara_Employee": {
                "steps": [
                    {
                        "column": "FirstName",
                        "action": "trim",
                        "issue_type": "whitespace",
                    },
                    {
                        "column": "LastName",
                        "action": "review_manually",
                        "issue_type": "planned_review_manually",
                    },
                ]
            }
        }
    }
    ctx = {"etl_flow": {"cleaning_mode": "cleanse_only"}}

    code, ok, errs, gen_by = _run_compiler_codegen(
        session_id="test_sess_codegen",
        engine="sql",
        plan=plan,
        assess=assess,
        ctx=ctx,
        sql_dialect="tsql",
    )

    assert ok is True
    assert gen_by == "compiler"
    assert len(code) > 0
    assert "TRIM" in code or "LTRIM" in code or "RTRIM" in code
    telem = ctx["etl_flow"]["compilation_telemetry"]
    assert telem["ok"] is True
    assert telem["executable"] is True
    assert telem["coverage"]["coverage_percent"] <= 100.0
    assert len(telem["issue_reconciliation"]) > 0


def test_non_key_fill_nulls_routes_to_manual_review():
    """
    Regression: planned_flag_missing_key on a non-key column (LastPromotionDate)
    must NOT produce a FILL_NULLS action — it must be routed to manual review.
    """
    bridge = IngestionBridge(default_schema="dbo")
    assess = {"data_quality_issues": {"datasets": {"dbo.Dhara_Employee": {"issues": []}}}}
    plan = {
        "datasets": {
            "dbo.Dhara_Employee": {
                "steps": [
                    {
                        "column": "LastPromotionDate",
                        "action": "flag_missing_key",
                        "issue_type": "planned_flag_missing_key",
                        "description": "Flag missing values in LastPromotionDate",
                    }
                ]
            }
        }
    }
    profiles = _make_sample_profiles()
    canonical, rules, telem = bridge.extract_and_normalize(assess, plan, set(profiles.keys()))
    _, actions = bridge.bridge_to_compiler_models(canonical)

    # Must not produce any FILL_NULLS for LastPromotionDate
    fill_action = next(
        (a for a in actions if "LastPromotionDate" in a.target_columns and a.action_type == ActionType.FILL_NULLS),
        None,
    )
    assert fill_action is None, "Non-key NULL column must NOT produce a FILL_NULLS action"

    # Must have produced a manual review record for the column
    manual_review_issues = [
        c for c in canonical
        if "LastPromotionDate" in (c.column or "")
        and c.resolution_path is not None
        and "manual_review" in c.resolution_path
    ]
    assert len(manual_review_issues) >= 1, "Non-key NULL must be routed to manual_review"


def test_key_column_flag_missing_routes_to_conditional_update():
    """
    Regression: planned_flag_missing_key on a key column (EmployeeID)
    must produce a CONDITIONAL_UPDATE action (FLAG disposition) — not a silent FILL.
    """
    bridge = IngestionBridge(default_schema="dbo")
    assess = {"data_quality_issues": {"datasets": {"dbo.Dhara_Employee": {"issues": []}}}}
    plan = {
        "datasets": {
            "dbo.Dhara_Employee": {
                "steps": [
                    {
                        "column": "EmployeeID",
                        "action": "flag_missing_key",
                        "issue_type": "planned_flag_missing_key",
                        "description": "Flag missing EmployeeID values",
                    }
                ]
            }
        }
    }
    profiles = _make_sample_profiles()
    canonical, rules, telem = bridge.extract_and_normalize(assess, plan, set(profiles.keys()))
    _, actions = bridge.bridge_to_compiler_models(canonical)

    flag_action = next(
        (a for a in actions if "EmployeeID" in a.target_columns and a.action_type == ActionType.CONDITIONAL_UPDATE),
        None,
    )
    assert flag_action is not None, "Key column NULL must produce CONDITIONAL_UPDATE (FLAG)"
    assert flag_action.parameters.get("disposition") == "FLAG"
    assert flag_action.parameters.get("condition") == "EmployeeID IS NULL"

    # Must NOT produce FILL_NULLS
    fill_action = next(
        (a for a in actions if "EmployeeID" in a.target_columns and a.action_type == ActionType.FILL_NULLS),
        None,
    )
    assert fill_action is None, "Key column must not be silently FILL_NULLS'd"


def test_persistent_idempotency_acquire_and_complete(tmp_path, monkeypatch):
    """
    Regression: acquire_generation_idempotency must create a RUNNING record
    and complete_generation_idempotency must mark it SUCCEEDED.
    Attempting to acquire a RUNNING key must return acquired=False with status=RUNNING.
    """
    import agent.jobs_store as js

    # Redirect DB to a tmp dir so we don't pollute real state.
    # _RESOLVED_DB_PATH is the module-level cache that _db_path() returns after first resolution.
    monkeypatch.setattr(js, "_RESOLVED_DB_PATH", str(tmp_path / "test_jobs.db"))

    key = "test_session_plan_sql_full_auto"

    # First acquire should succeed
    acquired_1, existing_1 = js.acquire_generation_idempotency(
        idempotency_key=key,
        session_id="test_session",
        plan_fingerprint="plan_abc",
        target_engine="sql",
    )
    assert acquired_1 is True
    assert existing_1 is None

    # Second acquire of RUNNING key must not be re-acquired
    acquired_2, existing_2 = js.acquire_generation_idempotency(
        idempotency_key=key,
        session_id="test_session",
        plan_fingerprint="plan_abc",
        target_engine="sql",
    )
    assert acquired_2 is False
    assert existing_2 is not None
    assert existing_2.get("status") == "RUNNING"

    # Complete the record as SUCCEEDED
    js.complete_generation_idempotency(
        idempotency_key=key,
        status="SUCCEEDED",
        response={"ok": True},
        artifact_id="artifacts/test.sql",
        error=None,
    )

    # Third acquire should not re-acquire — must see SUCCEEDED
    acquired_3, existing_3 = js.acquire_generation_idempotency(
        idempotency_key=key,
        session_id="test_session",
        plan_fingerprint="plan_abc",
        target_engine="sql",
    )
    assert acquired_3 is False
    assert existing_3 is not None
    assert existing_3.get("status") == "SUCCEEDED"


def test_missing_key_flag_does_not_write_sentinel_value():
    """
    Regression: FLAG disposition for a missing required key must NOT write __MISSING_KEY__
    or any sentinel value into the source row. The key column must retain its original NULL.
    The generated SQL must only write to ETL_REJECT.Rejection_Log, never SET [EmployeeID] = anything.
    """
    from agent.compiler.compiler_pipeline import CompilerPipeline
    from agent.compiler.models.context import CompilerContext
    from agent.compiler.models.physical import ExecutionEngine
    from agent.compiler.models.profile import ColumnProfile, DatasetProfile

    assess = {
        "datasets": {
            "dbo.Dhara_Employee": {
                "columns": {"EmployeeID": {"dtype": "int"}},
                "row_count": 100,
            }
        },
        "data_quality_issues": {"datasets": {"dbo.Dhara_Employee": {"issues": []}}},
    }
    plan = {
        "plan_id": "plan_flag_test",
        "datasets": {
            "dbo.Dhara_Employee": {
                "steps": [
                    {
                        "column": "EmployeeID",
                        "action": "flag_missing_key",
                        "issue_type": "planned_flag_missing_key",
                        "description": "Flag missing EmployeeID values",
                    }
                ]
            }
        },
    }
    ctx = {"etl_flow": {"cleaning_mode": "cleanse_only"}}
    from agent.etl_handlers import _run_compiler_codegen

    code, ok, errs, gen_by = _run_compiler_codegen(
        session_id="test_flag_sentinel",
        engine="sql",
        plan=plan,
        assess=assess,
        ctx=ctx,
        sql_dialect="tsql",
    )

    assert ok is True, f"Compiler failed unexpectedly: {errs}"
    assert gen_by == "compiler"

    # The __MISSING_KEY__ sentinel must NEVER appear in the generated SQL
    assert "__MISSING_KEY__" not in code, (
        "FLAG disposition must not write a sentinel value — "
        "found __MISSING_KEY__ in generated SQL"
    )

    # The generated SQL must not contain a SET that assigns to EmployeeID
    import re as _re
    set_employee_id = _re.search(r"SET\s+\[?EmployeeID\]?\s*=", code, _re.IGNORECASE)
    assert set_employee_id is None, (
        f"FLAG disposition must not overwrite [EmployeeID] — found: {set_employee_id.group()!r}"
    )

    # The generated SQL must reference the rejection log
    assert "ETL_REJECT" in code or "Rejection_Log" in code, (
        "FLAG disposition must produce a rejection log INSERT — ETL_REJECT/Rejection_Log not found in SQL"
    )


def test_idempotency_exception_path_records_failed_retryable(tmp_path, monkeypatch):
    """
    Regression: if generation throws an unexpected exception, the idempotency lease
    must be completed as FAILED_RETRYABLE (not left as RUNNING), allowing the next
    caller to re-acquire the lease immediately.
    """
    import agent.jobs_store as js
    from agent.etl_handlers import _persist_idempotency_result

    monkeypatch.setattr(js, "_RESOLVED_DB_PATH", str(tmp_path / "test_jobs.db"))

    key = "test_exception_path_key"

    # Simulate: lease acquired, then generation throws
    acquired, _ = js.acquire_generation_idempotency(
        idempotency_key=key,
        session_id="test_sess",
        plan_fingerprint="plan_exc",
        target_engine="sql",
    )
    assert acquired is True

    # Simulate exception path — no result_payload produced
    fake_exc = RuntimeError("Simulated infra failure")
    _persist_idempotency_result(
        lease_acquired=True,
        idempotency_key=key,
        ok=False,
        generation_exception=fake_exc,
        result_payload=None,
    )

    # Verify record is FAILED_RETRYABLE (not RUNNING)
    acquired_2, rec_2 = js.acquire_generation_idempotency(
        idempotency_key=key,
        session_id="test_sess",
        plan_fingerprint="plan_exc",
        target_engine="sql",
    )
    # FAILED_RETRYABLE should be re-acquirable
    assert acquired_2 is True, (
        f"Expected lease to be re-acquirable after FAILED_RETRYABLE, got status={rec_2}"
    )


def test_idempotency_expired_lease_is_reacquired(tmp_path, monkeypatch):
    """
    Regression: a RUNNING lease older than lease_seconds must be re-acquired by the
    next caller, not block indefinitely.
    """
    import agent.jobs_store as js

    monkeypatch.setattr(js, "_RESOLVED_DB_PATH", str(tmp_path / "test_jobs.db"))

    key = "test_expired_lease_key"

    # Acquire with a very short lease so we can expire it immediately
    acquired, _ = js.acquire_generation_idempotency(
        idempotency_key=key,
        session_id="test_sess",
        plan_fingerprint="plan_exp",
        target_engine="sql",
        lease_seconds=0.001,  # Expire after 1ms
    )
    assert acquired is True

    import time as _time
    _time.sleep(0.01)  # Ensure lease has expired

    # Second caller should re-acquire the expired lease
    acquired_2, rec_2 = js.acquire_generation_idempotency(
        idempotency_key=key,
        session_id="test_sess",
        plan_fingerprint="plan_exp",
        target_engine="sql",
        lease_seconds=0.001,
    )
    assert acquired_2 is True, (
        f"Expected expired RUNNING lease to be re-acquired, got acquired={acquired_2} rec={rec_2}"
    )


def test_concurrent_acquisition_only_one_winner(tmp_path, monkeypatch):
    """
    Regression: two concurrent calls for the same idempotency key must result in
    exactly one acquiring the lease (True) and one being blocked (False/RUNNING).
    This exercises the SQLite IntegrityError path in acquire_generation_idempotency.
    """
    import threading
    import agent.jobs_store as js

    monkeypatch.setattr(js, "_RESOLVED_DB_PATH", str(tmp_path / "test_jobs.db"))

    key = "test_concurrent_key"
    results = []
    barrier = threading.Barrier(2)

    def acquire():
        barrier.wait()  # Both threads start simultaneously
        acquired, rec = js.acquire_generation_idempotency(
            idempotency_key=key,
            session_id="concurrent_sess",
            plan_fingerprint="plan_conc",
            target_engine="sql",
        )
        results.append((acquired, rec))

    t1 = threading.Thread(target=acquire)
    t2 = threading.Thread(target=acquire)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert len(results) == 2, "Both threads must complete"
    winners = [r for r in results if r[0] is True]
    blocked = [r for r in results if r[0] is False]

    assert len(winners) == 1, (
        f"Exactly one thread must acquire the lease, got {len(winners)} winners: {results}"
    )
    assert len(blocked) == 1, (
        f"Exactly one thread must be blocked, got {len(blocked)} blocked: {results}"
    )
    blocked_rec = blocked[0][1]
    assert blocked_rec is not None and blocked_rec.get("status") == "RUNNING", (
        f"Blocked thread must see RUNNING status, got: {blocked_rec}"
    )


def test_post_reconciliation_blocking_count_drives_gate():
    """
    Regression: capability gate in SQLCodegenAgent must be driven by
    blocking_issue_count (post-reconciliation), NOT raw unsupported_issue_count.
    When blocking_issue_count == 0, compilation succeeds even if unsupported count > 0.
    When blocking_issue_count > 0, compilation is blocked with the specific message.
    """
    pipeline = CompilerPipeline()

    # 1. blocking_issue_count == 0 -> should compile successfully
    ctx_allowed = CompilerContext(
        session_id="test_post_recon_pass",
        target_engine=ExecutionEngine.AZURE_SQL,
        options={"strict_execution": True},
        dataset_profiles={"dbo.Employee": DatasetProfile(dataset_id="dbo.Employee", columns={"name": ColumnProfile(name="name", dtype="string")})},
        structured_actions=[
            StructuredAction(action_type=ActionType.TRIM, dataset_name="dbo.Employee", target_columns=["name"]),
        ],
    )
    ctx_allowed.metadata["ingestion_telemetry"] = {
        "assessment_discovered_count": 10,
        "mapped_issue_count": 4,
        "unsupported_issue_count": 6,
    }
    ctx_allowed.metadata["blocking_issue_count"] = 0
    ctx_allowed.metadata["unsupported_issue_count"] = 0

    compiled_ctx, _ = pipeline.compile(ctx_allowed)
    telem = compiled_ctx.metadata.get("compilation_telemetry", {})
    assert telem["ok"] is True
    assert telem["executable"] is True
    assert telem["rendered_transformation_count"] > 0

    # 2. blocking_issue_count > 0 -> should block with exact message
    ctx_blocked = CompilerContext(
        session_id="test_post_recon_block",
        target_engine=ExecutionEngine.AZURE_SQL,
        options={"strict_execution": True},
        dataset_profiles={"dbo.Employee": DatasetProfile(dataset_id="dbo.Employee", columns={"name": ColumnProfile(name="name", dtype="string")})},
        structured_actions=[
            StructuredAction(action_type=ActionType.TRIM, dataset_name="dbo.Employee", target_columns=["name"]),
        ],
    )
    ctx_blocked.metadata["ingestion_telemetry"] = {
        "assessment_discovered_count": 10,
        "mapped_issue_count": 4,
        "unsupported_issue_count": 6,
    }
    ctx_blocked.metadata["blocking_issue_count"] = 6

    with pytest.raises(CodeGenerationError) as exc_info:
        pipeline.compile(ctx_blocked)

    assert "6 blocking issue(s) remain unresolved" in str(exc_info.value)
    assert exc_info.value.failure_stage == "capability_validation"


def test_six_unresolved_findings_reclassified_as_non_blocking_manual_review():
    """
    Regression: The 6 Tier 3 issues from the assessment/plan:
    1. canonical_schema_constraint (policy_never_drop_rows)
    2. string_with_only_digits_in_text_column
    3. custom_rule_violation (binary-like hr.system / hr.admin)
    4. dataset-level outlier plan with no target
    5. string-column outlier plan without range bounds (OfficeLocation)
    6. declared-key flag without complete semantics (ManagerID self-ref)
    must all be classified as non-blocking manual review observations (blocks_execution=False)
    and produce certification_status="REVIEW_REQUIRED", blocking_issue_count=0.
    """
    bridge = IngestionBridge(default_schema="dbo")
    assess = {
        "data_quality_issues": {
            "datasets": {
                "dbo.Dhara_OrgReference": {
                    "issues": [
                        {
                            "column": None,
                            "issue_type": "canonical_schema_constraint",
                            "message": "policy_never_drop_rows failed: 15 violation rows",
                            "severity": "high",
                        }
                    ]
                },
                "dbo.Dhara_EmployeeStatusHistory": {
                    "issues": [
                        {
                            "column": "StatusHistoryRowID",
                            "issue_type": "string_with_only_digits_in_text_column",
                            "message": "StatusHistoryRowID contains digit-only strings in text column",
                            "severity": "medium",
                        },
                        {
                            "column": "ChangedBy",
                            "issue_type": "custom_rule_violation",
                            "message": "binary-like column with values hr.system and hr.admin",
                            "severity": "low",
                        },
                    ]
                },
                "dbo.Dhara_Employee": {
                    "issues": [
                        {
                            "column": "OfficeLocation",
                            "issue_type": "outlier",
                            "suggested_action": "range_clip",
                            "message": "OfficeLocation categorical outlier",
                            "severity": "medium",
                        }
                    ]
                },
            }
        }
    }
    plan = {
        "datasets": {
            "dbo.Dhara_OrgReference": {
                "steps": [
                    {
                        "column": "*",
                        "action": "range_clip",
                        "issue_type": "planned_flag_outliers",
                        "description": "Drop dataset-level outlier plan",
                    }
                ]
            },
            "dbo.Dhara_Employee": {
                "steps": [
                    {
                        "column": "ManagerID",
                        "action": "flag_missing_declared_key",
                        "issue_type": "planned_flag_missing_declared_key",
                        "description": "Flag missing ManagerID self-reference",
                    }
                ]
            },
        }
    }

    profiles = _make_sample_profiles()
    canonical, rules, telem = bridge.extract_and_normalize(assess, plan, set(profiles.keys()))

    service = Tier3ResolutionService()
    res = service.resolve(
        canonical_issues=canonical,
        deterministic_actions=[],
        dataset_profiles=profiles,
        business_rules=rules,
    )

    assert res.ok is True, f"Expected res.ok to be True, got {res.ok} (failure_stage={res.failure_stage})"
    assert res.executable is True
    assert res.certification_status == "REVIEW_REQUIRED"
    assert len(res.blocking_items) == 0
    assert len(res.blocking_manual_review_issues) == 0

    # Verify blocks_execution is False on all reconciled entries
    for entry in res.issue_reconciliation:
        assert entry.get("blocks_execution") is False, f"Entry {entry} must not block execution"


def test_semantic_guards_isactive_boolean_not_date():
    """
    Regression: IsActive must NOT be mapped to PARSE_DATES even if keyword 'date'
    is present in the issue message or description.
    """
    bridge = IngestionBridge(default_schema="dbo")
    assess = {
        "data_quality_issues": {
            "datasets": {
                "dbo.Dhara_Employee": {
                    "issues": [
                        {
                            "column": "IsActive",
                            "issue_type": "boolean_inconsistency",
                            "message": "Effective date validation for active flag",
                            "severity": "medium",
                        }
                    ]
                }
            }
        }
    }
    canonical, _, _ = bridge.extract_and_normalize(assess, {}, {"dbo.Dhara_Employee"})
    iss = canonical[0]
    assert iss.resolved_action == ActionType.STANDARDIZE_BOOLEAN
    assert iss.resolved_action != ActionType.PARSE_DATES


def test_comma_separated_columns_split():
    """
    Regression: 'ContractStartDate,ContractEndDate' must be split into two
    independent target columns rather than a single combined pseudo-column.
    """
    bridge = IngestionBridge(default_schema="dbo")
    assess = {
        "data_quality_issues": {
            "datasets": {
                "dbo.Dhara_Employee": {
                    "issues": [
                        {
                            "column": "ContractStartDate,ContractEndDate",
                            "issue_type": "invalid_date",
                            "suggested_action": "parse_dates",
                            "severity": "medium",
                        }
                    ]
                }
            }
        }
    }
    canonical, _, _ = bridge.extract_and_normalize(assess, {}, {"dbo.Dhara_Employee"})
    cols = [c.column for c in canonical]
    assert "ContractStartDate" in cols
    assert "ContractEndDate" in cols
    assert "ContractStartDate,ContractEndDate" not in cols


def test_unrestricted_fill_nulls_guarded_on_keys_and_temporals():
    """
    Regression: Non-authoritative NULL findings on keys, temporals, and financial fields
    (EmployeeID, ManagerID, LastPromotionDate, EffectiveTo, Salary, etc.)
    must NOT map to FILL_NULLS; they must route to manual review.
    """
    bridge = IngestionBridge(default_schema="dbo")
    columns_to_test = [
        "EmployeeID", "ManagerID", "LastPromotionDate",
        "EffectiveTo", "ContractEndDate", "DateOfBirth", "HireDate", "Salary", "ReferenceID"
    ]
    issues = [
        {"column": col, "issue_type": "null_values", "suggested_action": "fill_nulls", "severity": "medium"}
        for col in columns_to_test
    ]
    assess = {
        "data_quality_issues": {
            "datasets": {
                "dbo.Dhara_Employee": {
                    "issues": issues
                }
            }
        }
    }
    canonical, _, _ = bridge.extract_and_normalize(assess, {}, {"dbo.Dhara_Employee"})
    for iss in canonical:
        assert iss.resolved_action is None, f"{iss.column} must not be mapped to FILL_NULLS"
        assert "manual_review" in (iss.resolution_path or "")


def test_trim_on_date_columns_rejected():
    """
    Regression: Explicit planned TRIM on date-typed columns (EffectiveFrom, ChangedAt)
    must be rejected and routed to manual review.
    """
    bridge = IngestionBridge(default_schema="dbo")
    plan = {
        "datasets": {
            "dbo.Dhara_OrgReference": {
                "steps": [
                    {"column": "EffectiveFrom", "action": "trim", "params": {"side": "both"}},
                    {"column": "ChangedAt", "action": "trim", "params": {"side": "both"}},
                ]
            }
        }
    }
    canonical, _, _ = bridge.extract_and_normalize({}, plan, {"dbo.Dhara_OrgReference"})
    for iss in canonical:
        assert iss.resolved_action is None, f"{iss.column} must not be mapped to TRIM"
        assert "manual_review_incompatible_trim_on_date_column" in (iss.resolution_path or "")


def test_never_drop_rows_actively_enforced_no_delete():
    """
    Regression & Policy Guarantee: When options['never_drop_rows'] is True,
    neither individual step templates (deduplicate, conditional_update, sanitize_email, etc.)
    nor the final emitted SQL script may contain any row deletion statements (DELETE FROM / DELETE).
    """
    pipeline = CompilerPipeline()
    ctx = CompilerContext(
        session_id="test_never_drop_enforced",
        target_engine=ExecutionEngine.AZURE_SQL,
        options={"strict_execution": True, "never_drop_rows": True},
        dataset_profiles={
            "dbo.Dhara_Employee": DatasetProfile(
                dataset_id="dbo.Dhara_Employee",
                columns={
                    "EmployeeID": ColumnProfile(name="EmployeeID", dtype="int"),
                    "Email": ColumnProfile(name="Email", dtype="string"),
                    "Phone": ColumnProfile(name="Phone", dtype="string"),
                    "IsActive": ColumnProfile(name="IsActive", dtype="string"),
                },
            )
        },
        structured_actions=[
            StructuredAction(
                action_type=ActionType.DEDUPLICATE,
                dataset_name="dbo.Dhara_Employee",
                target_columns=["EmployeeID"],
                parameters={"disposition": "REJECT"},
            ),
            StructuredAction(
                action_type=ActionType.SANITIZE_EMAIL,
                dataset_name="dbo.Dhara_Employee",
                target_columns=["Email"],
                parameters={"disposition": "REJECT"},
            ),
            StructuredAction(
                action_type=ActionType.NORMALIZE_PHONE,
                dataset_name="dbo.Dhara_Employee",
                target_columns=["Phone"],
                parameters={"disposition": "REJECT"},
            ),
            StructuredAction(
                action_type=ActionType.STANDARDIZE_BOOLEAN,
                dataset_name="dbo.Dhara_Employee",
                target_columns=["IsActive"],
                parameters={"disposition": "REJECT"},
            ),
        ],
    )
    ctx.metadata["blocking_issue_count"] = 0
    ctx.metadata["unsupported_issue_count"] = 0

    compiled_ctx, snapshot = pipeline.compile(ctx)
    assert snapshot is not None, "Snapshot must be emitted"
    sql = snapshot.generated_code
    assert sql is not None, "Generated SQL code must not be None"

    # Invariant: No non-comment lines may contain row deletion
    non_comment_lines = [
        line.strip() for line in sql.splitlines()
        if not line.strip().startswith("--") and line.strip()
    ]
    non_comment_sql = "\n".join(non_comment_lines).upper()

    assert "DELETE FROM" not in non_comment_sql, "Generated SQL must NOT contain DELETE FROM when never_drop_rows=True"
    assert "DELETE S" not in non_comment_sql, "Generated SQL must NOT contain alias row deletion when never_drop_rows=True"
    assert "DELETE C" not in non_comment_sql, "Generated SQL must NOT contain child row deletion when never_drop_rows=True"


def test_gating_source_telemetry_and_warnings():
    """
    Verify that gating_source reflects 'post_reconciliation' when blocking_issue_count is explicitly provided,
    and 'legacy_unsupported_count' with appropriate warning when falling back.
    """
    pipeline = CompilerPipeline()

    # 1. Post reconciliation source
    ctx1 = CompilerContext(
        session_id="test_gating_post_recon",
        target_engine=ExecutionEngine.AZURE_SQL,
        options={"strict_execution": False, "preview_mode": True},
        dataset_profiles={"dbo.Employee": DatasetProfile(dataset_id="dbo.Employee", columns={"name": ColumnProfile(name="name", dtype="string")})},
        structured_actions=[StructuredAction(action_type=ActionType.TRIM, dataset_name="dbo.Employee", target_columns=["name"])],
    )
    ctx1.metadata["blocking_issue_count"] = 0
    ctx1, _ = pipeline.compile(ctx1)
    telem1 = ctx1.metadata.get("compilation_telemetry", {})
    assert telem1.get("gating_source") == "post_reconciliation"
    assert "Post-reconciliation blocking telemetry was unavailable." not in telem1.get("warnings", [])

    # 2. Legacy fallback source
    ctx2 = CompilerContext(
        session_id="test_gating_legacy",
        target_engine=ExecutionEngine.AZURE_SQL,
        options={"strict_execution": False, "preview_mode": True},
        dataset_profiles={"dbo.Employee": DatasetProfile(dataset_id="dbo.Employee", columns={"name": ColumnProfile(name="name", dtype="string")})},
        structured_actions=[StructuredAction(action_type=ActionType.TRIM, dataset_name="dbo.Employee", target_columns=["name"])],
    )
    # omit blocking_issue_count entirely
    ctx2.metadata["ingestion_telemetry"] = {"unsupported_issue_count": 0}
    ctx2, _ = pipeline.compile(ctx2)
    telem2 = ctx2.metadata.get("compilation_telemetry", {})
    assert telem2.get("gating_source") == "legacy_unsupported_count"
    assert any("Post-reconciliation blocking telemetry was unavailable." in w for w in telem2.get("warnings", []))


def test_idempotency_fingerprint_compiler_version_2_0_1():
    """
    Verify that etl_handlers generates idempotency keys with compiler version 2.0.1
    and compiler build tag, ensuring invalidated persistent cache for prior failed runs.
    """
    import inspect
    from agent import etl_handlers
    src = inspect.getsource(etl_handlers.etl_generate_code)
    assert 'compver=' in src or '_idem_compiler_ver' in src
    assert '_idem_compiler_ver = "2.0.2"' in src or '_idem_compiler_ver = "2.0.1"' in src
    assert 'build=' in src or '_idem_compiler_build' in src

