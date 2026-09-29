from __future__ import annotations

import pytest
from unittest.mock import patch

from agent.compiler.models.canonical_manifest import CanonicalResolutionManifest
from agent.compiler.models.key_semantics import EvidenceAuthority, KeySemantics, classify_column_semantics
from agent.compiler.models.logical import ActionType
from agent.compiler.models.plan_readiness import (
    PlanReadinessResult,
    ReadinessState,
    TerminalResolutionStatus,
)
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.planners.ingestion_bridge import CanonicalIssue, IngestionBridge
from agent.compiler.readiness.plan_readiness_service import PlanReadinessService
from agent.compiler.tier3.resolution_service import Tier3ResolutionService
from agent.etl_handlers import (
    etl_apply_manual_resolutions,
    etl_confirm_plan,
    etl_generate_code,
)
from agent.session_store import load_session, save_session


# ─────────────────────────────────────────────────────────────────────────────
# 1. Key Semantic Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_declared_pk_is_classified_as_key():
    prof = DatasetProfile(
        dataset_id="Employee",
        primary_key_columns=["EmployeeID"],
        columns={"EmployeeID": ColumnProfile(name="EmployeeID", dtype="int")},
    )
    sem = classify_column_semantics("EmployeeID", dataset_profile=prof)
    assert sem.is_primary_key is True
    assert sem.authority == EvidenceAuthority.DECLARED_PK
    assert sem.is_authoritative_key is True


def test_reason_code_and_postal_code_not_classified_as_key_by_substring():
    prof = DatasetProfile(dataset_id="Employee", columns={})
    sem_reason = classify_column_semantics("ReasonCode", dataset_profile=prof)
    assert sem_reason.is_primary_key is False
    assert sem_reason.authority == EvidenceAuthority.NON_KEY

    sem_postal = classify_column_semantics("PostalCode", dataset_profile=prof)
    assert sem_postal.is_primary_key is False
    assert sem_postal.authority == EvidenceAuthority.NON_KEY

    sem_status = classify_column_semantics("StatusCode", dataset_profile=prof)
    assert sem_status.is_primary_key is False
    assert sem_status.authority == EvidenceAuthority.NON_KEY

    sem_paid = classify_column_semantics("PaidAmount", dataset_profile=prof)
    assert sem_paid.is_primary_key is False
    assert sem_paid.authority == EvidenceAuthority.NON_KEY


def test_employee_id_without_metadata_is_only_candidate_identifier():
    prof = DatasetProfile(dataset_id="Employee", columns={})
    sem = classify_column_semantics("EmployeeID", dataset_profile=prof)
    assert sem.is_primary_key is False
    assert sem.authority == EvidenceAuthority.POTENTIAL_IDENTIFIER
    assert sem.is_authoritative_key is False


# ─────────────────────────────────────────────────────────────────────────────
# 2. Readiness Invariants and Terminal Resolution Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_readiness_invariants_and_terminal_buckets():
    service = PlanReadinessService()
    plan = {
        "plan_id": "test_plan_readiness",
        "datasets": {"Employee": {"columns": ["EmployeeID", "Salary", "Notes"]}},
    }
    assess = {
        "datasets": {
            "Employee": {
                "columns": {
                    "EmployeeID": {"dtype": "int"},
                    "Salary": {"dtype": "decimal"},
                    "Notes": {"dtype": "string"},
                },
                "primary_key_columns": ["EmployeeID"],
            }
        },
        "issues": [
            {
                "dataset": "Employee",
                "column": "EmployeeID",
                "issue_type": "null_values",
                "severity": "HIGH",
            },
            {
                "dataset": "Employee",
                "column": "Notes",
                "issue_type": "unusual_distribution",
                "severity": "LOW",
            },
        ],
    }

    res = service.evaluate("sess_test", plan=plan, assessment=assess)
    # The sum invariant must hold strictly
    assert res.blocking_issue_count == (
        res.unresolved_blocking_count
        + res.blocking_manual_review_count
        + res.approval_pending_blocking_count
        + res.unsupported_blocking_count
    )
    assert len(res.blocking_issues) == res.blocking_issue_count


def test_approved_waiver_does_not_block():
    service = PlanReadinessService()
    issue_id = "iss_waived_001"
    plan = {
        "plan_id": "test_waiver",
        "datasets": {"Employee": {"columns": ["Notes"]}},
        "waived_issue_ids": [issue_id],
    }
    assess = {
        "datasets": {"Employee": {"columns": {"Notes": {"dtype": "string"}}}},
        "issues": [
            {
                "issue_id": issue_id,
                "dataset": "Employee",
                "column": "Notes",
                "issue_type": "high_null_percentage",
                "severity": "HIGH",
            }
        ],
    }

    res = service.evaluate("sess_waiver", plan=plan, assessment=assess)
    # Since issue is waived, it does not block confirmation
    assert res.blocking_issue_count == 0
    assert res.ready_for_confirmation is True
    assert res.canonical_manifest is not None


# ─────────────────────────────────────────────────────────────────────────────
# 3. Canonical Issue ID Lineage Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_canonical_id_stability_across_runs():
    id1 = IngestionBridge.generate_canonical_issue_id(
        dataset="dbo.Dhara_Employee",
        column="Salary",
        issue_type="null_values",
    )
    id2 = IngestionBridge.generate_canonical_issue_id(
        dataset="dbo.Dhara_Employee",
        column="Salary",
        issue_type="null_values",
    )
    assert id1 == id2
    assert id1.startswith("ci_dbo.dhara_employee_salary_")


def test_structured_actions_reference_only_canonical_ids():
    bridge = IngestionBridge(default_schema="dbo")
    assess = {
        "datasets": {
            "Employee": {
                "columns": {
                    "FirstName": {"dtype": "string"},
                }
            }
        },
        "issues": [
            {
                "dataset": "Employee",
                "column": "FirstName",
                "issue_type": "whitespace",
                "suggested_action": "trim",
            },
            # Duplicate issue that will be deduplicated
            {
                "dataset": "Employee",
                "column": "FirstName",
                "issue_type": "whitespace",
                "suggested_action": "trim",
            },
        ],
    }
    canonical_issues, rules, telemetry = bridge.extract_and_normalize(assess=assess, plan={})
    assert len(canonical_issues) == 1
    c_iss = canonical_issues[0]

    detected_issues, actions = bridge.bridge_to_compiler_models(canonical_issues)
    assert len(actions) == 1
    act = actions[0]

    # Every action source_issue_id must strictly match the canonical issue ID
    assert act.source_issue_ids == [c_iss.issue_id]
    # Telemetry alias map maps both original instances to canonical ID
    assert len(telemetry["issue_alias_map"]) >= 1


# ─────────────────────────────────────────────────────────────────────────────
# 4. Route Flow Tests: Apply Manual Resolutions, Confirm, Generate
# ─────────────────────────────────────────────────────────────────────────────

def test_apply_manual_resolutions_returns_ok_true_and_readiness():
    sid = "sess_route_test_readiness"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": {
            "datasets": {
                "ClientData": {
                    "columns": {
                        "ClientID": {"dtype": "int"},
                        "City": {"dtype": "string"},
                    },
                    "primary_key_columns": ["ClientID"],
                }
            }
        },
        "etl_flow": {
            "plan_version": 1,
            "plan": {
                "plan_version": 1,
                "datasets": {"ClientData": {"steps": []}},
                "manual_review": [
                    {
                        "id": "rev_city",
                        "dataset": "ClientData",
                        "column": "City",
                        "issue_type": "untrimmed_whitespace",
                        "status": "pending",
                        "resolution_options": [
                            {"id": "opt_trim", "action": "trim", "recommended": True}
                        ],
                    }
                ],
            },
        },
    }
    save_session(sess)

    # Apply valid resolution
    resp = etl_apply_manual_resolutions(sid, [{"item_id": "rev_city", "resolution_id": "opt_trim"}])
    assert resp["ok"] is True
    assert "ready_for_confirmation" in resp
    assert "can_confirm_plan" in resp
    assert "blocking_issue_count" in resp
    assert resp["can_generate"] is False


def test_confirm_plan_blocks_and_returns_422_when_unresolved():
    sid = "sess_confirm_block"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": {
            "datasets": {
                "ClientData": {
                    "columns": {"ClientID": {"dtype": "int"}},
                    "primary_key_columns": ["ClientID"],
                }
            },
            "issues": [
                {
                    "dataset": "ClientData",
                    "column": "ClientID",
                    "issue_type": "null_values",
                    "severity": "HIGH",
                }
            ],
        },
        "etl_flow": {
            "phase": "planned",
            "plan_version": 1,
            "plan": {
                "plan_id": "p_block",
                "plan_version": 1,
                "datasets": {"ClientData": {"steps": []}},
                "manual_review": [
                    {
                        "id": "item_block_1",
                        "dataset": "ClientData",
                        "column": "ClientID",
                        "issue_type": "null_values",
                        "status": "pending",
                    }
                ],
            },
        },
    }
    save_session(sess)

    # Calling confirm when blocking issues exist must fail with HTTP 422
    resp = etl_confirm_plan(session_id=sid)
    assert resp["ok"] is False
    assert resp["http_status"] == 422
    assert resp["error"] == "PLAN_NOT_READY_FOR_CONFIRMATION"
    assert resp["lifecycle_state"] == "MANUAL_REVIEW_REQUIRED"
    assert resp["ready_for_confirmation"] is False

    # Invariant: session must NOT be promoted to approved / CONFIRMED
    sess_after = load_session(sid)
    assert sess_after["context"]["etl_flow"].get("phase") != "approved"
    assert sess_after["context"]["etl_flow"].get("lifecycle_state") != "CONFIRMED"


def test_confirm_freezes_manifest_and_generate_consumes_frozen_manifest():
    sid = "sess_confirm_success_and_generate"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": {
            "datasets": {
                "ClientData": {
                    "columns": {
                        "ClientID": {"dtype": "int"},
                        "City": {"dtype": "string"},
                    },
                    "primary_key_columns": ["ClientID"],
                }
            },
            "issues": [
                {
                    "dataset": "ClientData",
                    "column": "City",
                    "issue_type": "whitespace",
                    "suggested_action": "trim",
                }
            ],
        },
        "etl_flow": {
            "phase": "planned",
            "plan_version": 1,
            "plan": {
                "plan_id": "p_ok",
                "plan_version": 1,
                "datasets": {
                    "ClientData": {
                        "steps": [{"action": "trim", "column": "City"}]
                    }
                },
                "manual_review": [],
            },
        },
    }
    save_session(sess)

    # 1. Confirm plan
    conf_res = etl_confirm_plan(session_id=sid)
    assert conf_res["ok"] is True
    assert conf_res["status"] == "CONFIRMED"
    assert conf_res["lifecycle_state"] == "CONFIRMED"
    assert "canonical_manifest_hash" in conf_res
    manifest_hash = conf_res["canonical_manifest_hash"]
    assert manifest_hash is not None

    # Verify manifest was frozen into session flow
    sess_confirmed = load_session(sid)
    flow = sess_confirmed["context"]["etl_flow"]
    assert flow.get("canonical_manifest") is not None
    assert flow.get("canonical_manifest_hash") == manifest_hash
    assert flow.get("confirmation", {}).get("status") == "CONFIRMED"

    # 2. Generate code consumes frozen manifest directly
    gen_res = etl_generate_code(session_id=sid, engine="sql", codegen_mode="compiler")
    assert gen_res["ok"] is True
    assert gen_res["generated_by"] == "compiler"
    assert "SELECT" in gen_res["code"]
    assert "City" in gen_res["code"]


def test_generate_rejects_tampered_manifest_hash():
    sid = "sess_manifest_tamper"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": {
            "datasets": {"ClientData": {"columns": {"City": {"dtype": "string"}}}},
            "issues": [],
        },
        "etl_flow": {
            "phase": "approved",
            "lifecycle_state": "CONFIRMED",
            "confirmation": {"status": "CONFIRMED"},
            "canonical_manifest_hash": "sha256:forged_hash_12345",
            "canonical_manifest": {
                "manifest_version": "1.0",
                "session_id": sid,
                "created_at": "2026-09-29T10:00:00Z",
                "assessment_fingerprint": "sha256:00",
                "plan_fingerprint": "sha256:00",
                "schema_fingerprint": "sha256:00",
                "rules_fingerprint": "sha256:00",
                "target_engine": "azure_sql",
                "canonical_issues": [],
                "structured_actions": [],
                "reconciliation": {"blocking_issue_count": 0},
            },
            "plan": {"datasets": {"ClientData": {"steps": []}}},
            "approved_plan": {"datasets": {"ClientData": {"steps": []}}},
        },
    }
    save_session(sess)

    gen_res = etl_generate_code(session_id=sid, engine="sql", codegen_mode="compiler")
    assert gen_res["ok"] is False
    assert any("MANIFEST_HASH_MISMATCH" in str(err) for err in gen_res.get("plan_validation_errors", []))


def test_generate_rejects_unconfirmed_plan():
    sid = "sess_unconfirmed_gen"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": {
            "datasets": {"ClientData": {"columns": {"City": {"dtype": "string"}}}},
            "issues": [],
        },
        "etl_flow": {
            "phase": "planned",
            "lifecycle_state": "MANUAL_REVIEW_REQUIRED",
            "plan": {"datasets": {"ClientData": {"steps": []}}},
        },
    }
    save_session(sess)

    gen_res = etl_generate_code(session_id=sid, engine="sql", codegen_mode="compiler")
    assert gen_res["ok"] is False
    assert gen_res["error"] == "PLAN_NOT_APPROVED"


def test_generate_rejects_schema_drift_after_confirmation():
    sid = "sess_drift_test"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": {
            "datasets": {
                "ClientData": {
                    "columns": {
                        "ClientID": {"dtype": "int"},
                        "City": {"dtype": "string"},
                        # Added new column after confirmation
                        "NewDriftCol": {"dtype": "string"},
                    },
                    "primary_key_columns": ["ClientID"],
                }
            },
            "issues": [],
        },
        "etl_flow": {
            "phase": "approved",
            "lifecycle_state": "CONFIRMED",
            "assessment_schema_signature": "sha256:original_schema_hash_before_drift",
            "confirmation": {
                "status": "CONFIRMED",
                "schema_fingerprint": "sha256:original_schema_hash_before_drift",
            },
            "plan": {"plan_id": "p_drift", "datasets": {"ClientData": {"steps": []}}},
            "approved_plan": {"plan_id": "p_drift", "datasets": {"ClientData": {"steps": []}}},
        },
    }
    save_session(sess)

    gen_res = etl_generate_code(session_id=sid, engine="sql", codegen_mode="compiler")
    assert gen_res["ok"] is False
    assert gen_res["error"] == "SCHEMA_DRIFT"


def test_apply_manual_resolutions_never_returns_ready_with_blockers():
    sid = "sess_partial_resolution"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": {
            "datasets": {
                "ClientData": {
                    "columns": {
                        "ClientID": {"dtype": "int"},
                        "City": {"dtype": "string"},
                    },
                    "primary_key_columns": ["ClientID"],
                }
            },
            "issues": [
                {
                    "dataset": "ClientData",
                    "column": "ClientID",
                    "issue_type": "null_values",
                    "severity": "HIGH",
                }
            ],
        },
        "etl_flow": {
            "plan_version": 1,
            "plan": {
                "plan_version": 1,
                "datasets": {"ClientData": {"steps": []}},
                "manual_review": [
                    {
                        "id": "rev_city",
                        "dataset": "ClientData",
                        "column": "City",
                        "issue_type": "untrimmed_whitespace",
                        "status": "pending",
                        "resolution_options": [
                            {"id": "opt_trim", "action": "trim", "recommended": True}
                        ],
                    },
                    {
                        "id": "rev_pk_null",
                        "dataset": "ClientData",
                        "column": "ClientID",
                        "issue_type": "null_values",
                        "status": "pending",
                        "severity": "HIGH",
                        "resolution_options": [
                            {"id": "opt_nullify", "action": "nullify", "recommended": False}
                        ],
                    },
                ],
            },
        },
    }
    save_session(sess)

    # Resolve only City; rev_pk_null remains pending and blocking
    resp = etl_apply_manual_resolutions(sid, [{"item_id": "rev_city", "resolution_id": "opt_trim"}])
    assert resp["ok"] is True
    assert resp["ready_for_confirmation"] is False
    assert resp["can_confirm_plan"] is False
    assert resp["can_generate"] is False
    assert resp["lifecycle_state"] == "MANUAL_REVIEW_REQUIRED"
    assert resp["blocking_issue_count"] > 0


def test_non_blocking_observational_review_policy():
    """Observational issues (like string length outliers or all-caps names) are non-blocking."""
    sid = "sess_non_blocking_policy"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": {
            "datasets": {
                "ClientData": {
                    "columns": {
                        "ClientID": {"dtype": "int"},
                        "CustomerName": {"dtype": "string"},
                    },
                    "primary_key_columns": ["ClientID"],
                }
            },
            "issues": [
                {
                    "dataset": "ClientData",
                    "column": "CustomerName",
                    "issue_type": "string_length_outlier",
                    "severity": "LOW",
                }
            ],
        },
        "etl_flow": {
            "phase": "planned",
            "plan_version": 1,
            "plan": {
                "plan_id": "p_obs",
                "plan_version": 1,
                "datasets": {"ClientData": {"steps": []}},
                "manual_review": [
                    {
                        "id": "rev_obs_1",
                        "dataset": "ClientData",
                        "column": "CustomerName",
                        "issue_type": "string_length_outlier",
                        "severity": "LOW",
                        "status": "pending",
                    }
                ],
            },
        },
    }
    save_session(sess)

    # Evaluating readiness: string_length_outlier is an observational finding, non-blocking
    service = PlanReadinessService()
    res = service.evaluate(
        sid,
        plan=sess["context"]["etl_flow"]["plan"],
        assessment=sess["context"]["last_assessment_result"],
    )
    assert res.blocking_issue_count == 0
    assert res.ready_for_confirmation is True

    # Confirmation succeeds because blocking count is 0
    conf_res = etl_confirm_plan(session_id=sid)
    assert conf_res["ok"] is True
    assert conf_res["status"] == "CONFIRMED"


def test_blocking_review_policy_mandatory_pk_unresolved():
    """Missing value in declared primary key is strictly blocking."""
    sid = "sess_blocking_pk_unresolved"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": {
            "datasets": {
                "ClientData": {
                    "columns": {"ClientID": {"dtype": "int"}},
                    "primary_key_columns": ["ClientID"],
                }
            },
            "issues": [
                {
                    "dataset": "ClientData",
                    "column": "ClientID",
                    "issue_type": "null_values",
                    "severity": "HIGH",
                }
            ],
        },
        "etl_flow": {
            "phase": "planned",
            "plan_version": 1,
            "plan": {
                "plan_id": "p_pk_block",
                "plan_version": 1,
                "datasets": {"ClientData": {"steps": []}},
                "manual_review": [
                    {
                        "id": "rev_pk_null",
                        "dataset": "ClientData",
                        "column": "ClientID",
                        "issue_type": "null_values",
                        "status": "pending",
                    }
                ],
            },
        },
    }
    save_session(sess)

    conf_res = etl_confirm_plan(session_id=sid)
    assert conf_res["ok"] is False
    assert conf_res["http_status"] == 422
    assert conf_res["error"] == "PLAN_NOT_READY_FOR_CONFIRMATION"
    assert conf_res["lifecycle_state"] == "MANUAL_REVIEW_REQUIRED"
    assert conf_res["blocking_issue_count"] > 0


def test_manifest_action_mutation_rejects_generation():
    """Mutating an action parameter in the frozen manifest invalidates hash and fails generation."""
    sid = "sess_action_mutation"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": {
            "datasets": {
                "ClientData": {
                    "columns": {"ClientID": {"dtype": "int"}, "City": {"dtype": "string"}},
                    "primary_key_columns": ["ClientID"],
                }
            },
            "issues": [],
        },
        "etl_flow": {
            "phase": "planned",
            "plan_version": 1,
            "plan": {
                "plan_id": "p_mut",
                "plan_version": 1,
                "datasets": {"ClientData": {"steps": [{"action": "trim", "column": "City"}]}},
                "manual_review": [],
            },
        },
    }
    save_session(sess)

    # 1. Confirm plan to freeze canonical manifest
    conf_res = etl_confirm_plan(session_id=sid)
    assert conf_res["ok"] is True

    # 2. Tamper with an action parameter inside the frozen manifest
    sess_confirmed = load_session(sid)
    manifest = sess_confirmed["context"]["etl_flow"]["canonical_manifest"]
    # Tamper with the action
    if manifest.get("structured_actions"):
        manifest["structured_actions"][0]["target_columns"] = ["ForgedColumn"]
    else:
        manifest["structured_actions"].append({"action_type": "DROP_COLUMN", "target_columns": ["ForgedColumn"]})
    save_session(sess_confirmed)

    # 3. Generating code must fail with hash mismatch
    gen_res = etl_generate_code(session_id=sid, engine="sql", codegen_mode="compiler")
    assert gen_res["ok"] is False
    assert gen_res["error"] == "MANIFEST_HASH_MISMATCH"


def test_generate_rejects_revoked_approval():
    """Revoking approval after confirmation invalidates generation."""
    sid = "sess_approval_revocation"
    sess = load_session(sid)
    sess["context"] = {
        "last_assessment_result": {
            "datasets": {
                "ClientData": {
                    "columns": {"ClientID": {"dtype": "int"}, "City": {"dtype": "string"}},
                    "primary_key_columns": ["ClientID"],
                }
            },
            "issues": [],
        },
        "etl_flow": {
            "phase": "planned",
            "plan_version": 1,
            "plan": {
                "plan_id": "p_app_rev",
                "plan_version": 1,
                "datasets": {"ClientData": {"steps": [{"action": "trim", "column": "City"}]}},
                "manual_review": [],
                "cleaning_approval": {"approved_by": "lead_steward", "revoked": False},
            },
        },
    }
    save_session(sess)

    # 1. Confirm plan
    conf_res = etl_confirm_plan(session_id=sid)
    assert conf_res["ok"] is True

    # 2. Revoke approval
    sess_confirmed = load_session(sid)
    sess_confirmed["context"]["etl_flow"]["cleaning_approval"] = {"approved_by": "lead_steward", "revoked": True}
    save_session(sess_confirmed)

    # 3. Generate code must reject with APPROVAL_REVOKED
    gen_res = etl_generate_code(session_id=sid, engine="sql", codegen_mode="compiler")
    assert gen_res["ok"] is False
    assert gen_res["error"] == "APPROVAL_REVOKED"


def test_high_risk_waiver_requires_steward_approval():
    """An unprivileged user role cannot waive a declared primary key null defect."""
    service = PlanReadinessService()
    issue_id = "iss_pk_high_risk_waiver"
    plan = {
        "plan_id": "p_steward_waiver_test",
        "datasets": {"Employee": {"columns": ["EmployeeID"]}},
        "approved_waivers": [
            {
                "issue_id": issue_id,
                "reason": "Test waiver",
                "approver_role": "OPERATOR",  # Unprivileged role
            }
        ],
    }
    assess = {
        "datasets": {
            "Employee": {
                "columns": {"EmployeeID": {"dtype": "int"}},
                "primary_key_columns": ["EmployeeID"],
            }
        },
        "issues": [
            {
                "issue_id": issue_id,
                "dataset": "Employee",
                "column": "EmployeeID",
                "issue_type": "null_values",
                "severity": "HIGH",
            }
        ],
    }

    res = service.evaluate("sess_waiver_role", plan=plan, assessment=assess)
    assert res.ready_for_confirmation is False
    assert res.approval_pending_blocking_count >= 1
    assert any("DATA_STEWARD" in b.reason for b in res.blocking_issues)


def test_high_risk_waiver_succeeds_with_data_steward_role():
    """A declared primary key null defect can be waived when authorized by DATA_STEWARD."""
    service = PlanReadinessService()
    issue_id = "iss_pk_high_risk_waiver_ok"
    plan = {
        "plan_id": "p_steward_waiver_ok",
        "datasets": {"Employee": {"columns": ["EmployeeID"]}},
        "approved_waivers": [
            {
                "issue_id": issue_id,
                "reason": "Legacy source rows cannot have IDs assigned",
                "approved_by": "lead_steward",
                "approver_role": "DATA_STEWARD",
                "execution_behavior": "PRESERVE_AND_FLAG",
            }
        ],
    }
    assess = {
        "datasets": {
            "Employee": {
                "columns": {"EmployeeID": {"dtype": "int"}},
                "primary_key_columns": ["EmployeeID"],
            }
        },
        "issues": [
            {
                "issue_id": issue_id,
                "dataset": "Employee",
                "column": "EmployeeID",
                "issue_type": "null_values",
                "severity": "HIGH",
            }
        ],
    }

    res = service.evaluate("sess_waiver_steward_ok", plan=plan, assessment=assess)
    assert res.ready_for_confirmation is True
    assert res.blocking_issue_count == 0



