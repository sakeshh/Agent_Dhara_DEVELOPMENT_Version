from __future__ import annotations

import datetime
import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.compiler.models.actions import ActionSource, StructuredAction
from agent.compiler.models.canonical_manifest import CanonicalResolutionManifest
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.plan_readiness import (
    BlockingIssue,
    PlanReadinessResult,
    ReadinessState,
    TerminalResolutionStatus,
)
from agent.compiler.models.profile import ColumnProfile, DatasetProfile
from agent.compiler.models.rules import BusinessRule
from agent.compiler.planners.fingerprint import compute_plan_fingerprint
from agent.compiler.planners.ingestion_bridge import CanonicalIssue, IngestionBridge
from agent.compiler.tier3.resolution_service import ResolutionBucket, Tier3ResolutionService

logger = logging.getLogger("agent.compiler.readiness")


class PlanReadinessService:
    """Authoritative service for evaluating whether an ETL plan and canonical issue
    resolutions are consistent, compliant, and ready to be confirmed and frozen for code generation.
    """

    def __init__(
        self,
        bridge: Optional[IngestionBridge] = None,
        tier3_service: Optional[Tier3ResolutionService] = None,
    ):
        self.bridge = bridge or IngestionBridge(default_schema="dbo")
        self.tier3_service = tier3_service or Tier3ResolutionService()

    @staticmethod
    def compute_schema_fingerprint(
        assessment: Optional[Dict[str, Any]],
        dataset_profiles: Optional[Dict[str, DatasetProfile]] = None,
    ) -> str:
        """Computes a stable SHA-256 fingerprint for dataset schemas."""
        parts: List[str] = []
        if dataset_profiles:
            for ds_name in sorted(dataset_profiles.keys()):
                prof = dataset_profiles[ds_name]
                parts.append(f"ds:{ds_name}")
                for col_name in sorted(prof.columns.keys()):
                    c_prof = prof.columns[col_name]
                    parts.append(f"col:{col_name}|type:{c_prof.dtype}")
        elif isinstance(assessment, dict) and "datasets" in assessment:
            datasets = assessment.get("datasets") or {}
            for ds_name in sorted(datasets.keys()):
                parts.append(f"ds:{ds_name}")
                cols = datasets[ds_name].get("columns") or {}
                for col_name in sorted(cols.keys()):
                    col_info = cols[col_name] or {}
                    dtype = str(col_info.get("dtype") or col_info.get("type") or "")
                    parts.append(f"col:{col_name}|type:{dtype}")
        sig_str = "\n".join(parts)
        return hashlib.sha256(sig_str.encode("utf-8")).hexdigest()

    def evaluate(
        self,
        session_id: str,
        plan: Dict[str, Any],
        assessment: Optional[Dict[str, Any]] = None,
        dataset_profiles: Optional[Dict[str, DatasetProfile]] = None,
        business_rules: Optional[List[BusinessRule]] = None,
        target_engine: ExecutionEngine = ExecutionEngine.AZURE_SQL,
        approval_records: Optional[Any] = None,
        policy_version: str = "tier3-safety-v1",
        compiler_version: str = "compiler-v3b",
    ) -> PlanReadinessResult:
        """Evaluates readiness of the plan and produces a canonical manifest if ready."""
        assess = assessment or {}
        profiles: Dict[str, DatasetProfile] = dict(dataset_profiles or {})

        # 1. Rehydrate DatasetProfiles if not supplied
        if not profiles:
            for ds_name, ds_info in (assess.get("datasets") or {}).items():
                if isinstance(ds_info, dict):
                    cols: Dict[str, ColumnProfile] = {}
                    for col_name, col_meta in (ds_info.get("columns") or {}).items():
                        dtype = col_meta.get("dtype") or col_meta.get("type") or "string" if isinstance(col_meta, dict) else "string"
                        cols[col_name] = ColumnProfile(name=col_name, dtype=dtype)
                    row_count = int(ds_info.get("row_count") or 0)
                    pk_cols = ds_info.get("primary_key_columns") or []
                    profiles[ds_name] = DatasetProfile(
                        dataset_id=ds_name,
                        row_count=row_count,
                        columns=cols,
                        primary_key_columns=pk_cols,
                    )
            if not profiles and isinstance(plan.get("datasets"), dict):
                for ds_name, p_ds in plan["datasets"].items():
                    cols = {c: ColumnProfile(name=c, dtype="string") for c in (p_ds.get("columns") or [])}
                    profiles[ds_name] = DatasetProfile(dataset_id=ds_name, columns=cols)

        # 2. Extract and normalize canonical issues via IngestionBridge
        known_ds = set(profiles.keys())
        canonical_issues, b_rules, telemetry = self.bridge.extract_and_normalize(
            assess=assess,
            plan=plan,
            known_datasets=known_ds,
        )
        detected_issues, deterministic_actions = self.bridge.bridge_to_compiler_models(canonical_issues)

        # 3. Resolve issues through Tier 1 / Tier 2 / Tier 3
        # 3. Resolve issues through Tier 1 / Tier 2 / Tier 3
        active_rules = business_rules or b_rules
        resolved_mr_list = plan.get("resolved_manual_review") or []
        tier3_res = self.tier3_service.resolve(
            canonical_issues=canonical_issues,
            deterministic_actions=deterministic_actions,
            dataset_profiles=profiles,
            business_rules=active_rules,
            target_engine=target_engine,
            approval_records=approval_records or plan.get("cleaning_approval"),
            resolved_manual_review=resolved_mr_list,
        )

        all_actions = [
            *deterministic_actions,
            *tier3_res.compiler_eligible_actions,
        ]

        # 4. Identify waivers and applied manual resolutions
        approved_waivers_map: Dict[str, Any] = {}
        for w in (plan.get("approved_waivers") or []):
            if isinstance(w, str):
                approved_waivers_map[w] = {"issue_id": w, "approved": True}
            elif isinstance(w, dict) and w.get("issue_id"):
                approved_waivers_map[w["issue_id"]] = w
        if isinstance(active_rules, dict):
            for w in (active_rules.get("approved_waivers") or []):
                if isinstance(w, str):
                    approved_waivers_map[w] = {"issue_id": w, "approved": True}
                elif isinstance(w, dict) and w.get("issue_id"):
                    approved_waivers_map[w["issue_id"]] = w
        for w in (plan.get("approved_waivers") or []):
            if isinstance(w, str):
                approved_waivers_map[w] = {"issue_id": w, "approved": True}
            elif isinstance(w, dict) and w.get("issue_id"):
                approved_waivers_map[w["issue_id"]] = w
        for wid in (plan.get("waived_issue_ids") or []):
            if isinstance(wid, str):
                approved_waivers_map[wid] = {"issue_id": wid, "approved": True}
            elif isinstance(wid, dict) and wid.get("issue_id"):
                approved_waivers_map[wid["issue_id"]] = wid

        resolved_mr_by_id: Dict[str, Dict[str, Any]] = {}
        resolved_mr_by_key: Dict[Tuple[str, Optional[str], str], Dict[str, Any]] = {}
        resolved_mr_by_col: Dict[Tuple[str, Optional[str]], List[Dict[str, Any]]] = {}

        for mr in resolved_mr_list:
            if not isinstance(mr, dict):
                continue
            for id_k in ("canonical_issue_id", "id", "item_id", "issue_id"):
                val = mr.get(id_k)
                if val:
                    resolved_mr_by_id[str(val)] = mr
            for sid in mr.get("source_issue_ids") or []:
                if sid:
                    resolved_mr_by_id[str(sid)] = mr

            ds_clean = self.bridge.normalize_dataset_name(mr.get("dataset") or "", known_datasets=known_ds).lower()
            col_raw = mr.get("column")
            col_clean = str(col_raw).strip("[]").lower() if col_raw and not self.bridge.is_pseudo_column(col_raw) else None
            itype_clean = str(mr.get("issue_type") or "").strip().lower()

            if ds_clean:
                resolved_mr_by_key[(ds_clean, col_clean, itype_clean)] = mr
                resolved_mr_by_col.setdefault((ds_clean, col_clean), []).append(mr)

            if mr.get("status") == "skipped" or mr.get("resolved_action") in ("skip", "waive", "skip_requirement"):
                for id_k in ("id", "item_id", "issue_id"):
                    val = mr.get(id_k)
                    if val:
                        approved_waivers_map[str(val)] = {
                            "issue_id": str(val),
                            "approved": True,
                            "approver_role": mr.get("approver_role") or "DATA_STEWARD",
                            "status": "skipped",
                        }

        # 5. Classify every canonical issue into exactly one terminal status and check blocking
        issue_alias_map = telemetry.get("issue_alias_map") or {}
        blocking_issues_list: List[BlockingIssue] = []
        warnings_list: List[Dict[str, Any]] = []

        unresolved_blocking_count = 0
        blocking_manual_review_count = 0
        approval_pending_blocking_count = 0
        unsupported_blocking_count = 0

        compiled_action_ready_count = 0
        waived_count = 0
        informational_count = 0

        # Map compiled actions by issue ID
        compiled_issue_ids: Set[str] = set()
        for act in all_actions:
            compiled_issue_ids.update(act.source_issue_ids)

        # Collect source issue IDs from plan steps
        for ds_name, p_ds in (plan.get("datasets") or {}).items():
            if isinstance(p_ds, dict):
                for st in (p_ds.get("steps") or []):
                    if isinstance(st, dict):
                        for sid in st.get("source_issue_ids") or []:
                            if sid:
                                compiled_issue_ids.add(str(sid))
                        if st.get("item_id"):
                            compiled_issue_ids.add(str(st["item_id"]))

        # Map approval pending proposals by issue ID
        approval_pending_map = {p.issue_id: p for p in tier3_res.approval_pending_proposals}

        # Map manual review items
        manual_review_map = {m["issue_id"]: m for m in tier3_res.manual_review_issues}

        # Canonical issues classification
        manifest_canonical_issues: List[Dict[str, Any]] = []

        for c_iss in canonical_issues:
            iss_id = c_iss.issue_id
            dataset_name = c_iss.dataset
            col_name = c_iss.column
            severity = c_iss.severity.upper() if c_iss.severity else "MEDIUM"
            category = c_iss.issue_type

            # Check if matching resolved manual review
            mr_match = None
            if iss_id in resolved_mr_by_id:
                mr_match = resolved_mr_by_id[iss_id]
            else:
                for mid in getattr(c_iss, "merged_issue_ids", []):
                    if str(mid) in resolved_mr_by_id:
                        mr_match = resolved_mr_by_id[str(mid)]
                        break
            if not mr_match:
                ds_l = dataset_name.lower()
                col_l = col_name.lower() if col_name else None
                it_l = category.lower()
                if (ds_l, col_l, it_l) in resolved_mr_by_key:
                    mr_match = resolved_mr_by_key[(ds_l, col_l, it_l)]
                elif (ds_l, col_l) in resolved_mr_by_col:
                    for cand in resolved_mr_by_col[(ds_l, col_l)]:
                        cand_it = str(cand.get("issue_type") or "").lower()
                        if cand_it == it_l or cand_it in it_l or it_l in cand_it:
                            mr_match = cand
                            break
                        if len(resolved_mr_by_col[(ds_l, col_l)]) == 1:
                            mr_match = cand
                            break

            # Verify issue fingerprint to prevent stale resolutions if issue evidence/parameters changed
            if mr_match and mr_match.get("issue_fingerprint"):
                exp_fingerprint = c_iss.compute_fingerprint() if hasattr(c_iss, "compute_fingerprint") else None
                if exp_fingerprint and mr_match["issue_fingerprint"] != exp_fingerprint:
                    logger.warning(
                        "[ReadinessReconciliation] Stale resolution for %s: recorded fingerprint %s != current %s",
                        iss_id,
                        mr_match.get("issue_fingerprint"),
                        exp_fingerprint,
                    )
                    mr_match = None

            # Decision type analysis for manual review resolutions
            mr_decision = str(mr_match.get("decision") or mr_match.get("resolved_action") or "").lower() if mr_match else ""
            mr_status = str(mr_match.get("status") or "").lower() if mr_match else ""
            mr_has_compiled_step = bool(
                mr_match
                and mr_status in ("resolved", "auto_resolved")
                and (
                    str(mr_match.get("id")) in compiled_issue_ids
                    or str(mr_match.get("item_id")) in compiled_issue_ids
                    or str(mr_match.get("issue_id")) in compiled_issue_ids
                    or any(str(s) in compiled_issue_ids for s in mr_match.get("source_issue_ids") or [])
                )
            )

            # Check if pending or review_later (must remain blocking MANUAL_REVIEW_REQUIRED)
            if mr_match and (mr_status == "pending" or mr_decision in ("review_later", "pending")):
                final_status = TerminalResolutionStatus.MANUAL_REVIEW_REQUIRED
                is_blocking = True
                blocking_reason = mr_match.get("message") or "Item marked for later manual review."
                blocking_manual_review_count += 1
            # Check if explicitly waived
            elif (
                iss_id in approved_waivers_map
                or any(mid in approved_waivers_map for mid in getattr(c_iss, "merged_issue_ids", []))
                or mr_status in ("skipped", "waived")
                or mr_decision in ("skip", "waive", "skip_requirement", "accept_risk")
            ):
                waiver_info = (
                    approved_waivers_map.get(iss_id)
                    or next((approved_waivers_map[mid] for mid in getattr(c_iss, "merged_issue_ids", []) if mid in approved_waivers_map), None)
                    or (mr_match if mr_match else {})
                )
                ds_prof = profiles.get(dataset_name)
                from agent.compiler.models.key_semantics import classify_column_semantics
                key_sem = classify_column_semantics(col_name, dataset_profile=ds_prof)

                requires_steward = key_sem.is_authoritative_key or severity in ("CRITICAL", "HIGH")
                approver_role = str(waiver_info.get("approver_role") or "DATA_STEWARD").upper()
                authorized_roles = {"DATA_STEWARD", "GOVERNANCE_LEAD", "SYSTEM_ADMIN", "DATA_OWNER", "HUMAN_OPERATOR"}

                if requires_steward and approver_role and approver_role not in authorized_roles:
                    final_status = TerminalResolutionStatus.APPROVAL_REQUIRED
                    is_blocking = True
                    blocking_reason = (
                        f"Waiver for high-risk {key_sem.authority.value} defect requires DATA_STEWARD or GOVERNANCE_LEAD approval "
                        f"(provided: {approver_role or 'none'})."
                    )
                    approval_pending_blocking_count += 1
                else:
                    final_status = TerminalResolutionStatus.WAIVED
                    is_blocking = False
                    blocking_reason = ""
                    waived_count += 1
            # Check if informational acknowledgement
            elif mr_match and (mr_status in ("acknowledged", "informational") or mr_decision in ("acknowledge", "informational_acknowledged")):
                final_status = TerminalResolutionStatus.INFORMATIONAL
                is_blocking = False
                blocking_reason = ""
                informational_count += 1
            # Check if compiled into action or resolved via manual review
            elif (
                iss_id in compiled_issue_ids
                or any(mid in compiled_issue_ids for mid in getattr(c_iss, "merged_issue_ids", []))
                or mr_has_compiled_step
            ):
                final_status = TerminalResolutionStatus.COMPILED_ACTION_READY
                is_blocking = False
                blocking_reason = ""
                compiled_action_ready_count += 1
            elif mr_match and mr_status in ("resolved", "auto_resolved"):
                # Invariant: Recorded resolution without an executable plan step cannot claim COMPILED_ACTION_READY
                final_status = TerminalResolutionStatus.MANUAL_REVIEW_REQUIRED
                is_blocking = True
                blocking_reason = "Manual resolution recorded, but no corresponding compiled plan step exists."
                blocking_manual_review_count += 1
            # Check if approval pending
            elif iss_id in approval_pending_map:
                final_status = TerminalResolutionStatus.APPROVAL_REQUIRED
                is_blocking = True
                blocking_reason = f"Proposed Tier 3 resolution requires steward approval before compilation."
                approval_pending_blocking_count += 1
            # Check if manual review required
            elif iss_id in manual_review_map:
                mr_item = manual_review_map[iss_id]
                if mr_item.get("is_blocking", False):
                    final_status = TerminalResolutionStatus.MANUAL_REVIEW_REQUIRED
                    is_blocking = True
                    blocking_reason = mr_item.get("reason") or "Manual review required on critical entity."
                    blocking_manual_review_count += 1
                else:
                    final_status = TerminalResolutionStatus.INFORMATIONAL
                    is_blocking = False
                    blocking_reason = ""
                    informational_count += 1
            # Check if unsupported or model unavailable
            else:
                matching_block = next((b for b in tier3_res.blocking_items if b.get("issue_id") == iss_id), None)
                if matching_block:
                    status_str = matching_block.get("status", "UNSUPPORTED")
                    if status_str == "MODEL_UNAVAILABLE":
                        final_status = TerminalResolutionStatus.MODEL_UNAVAILABLE
                        is_blocking = True
                        blocking_reason = matching_block.get("reason") or "Tier 3 model unavailable or timed out."
                        unsupported_blocking_count += 1
                    elif status_str == "TIER3_VALIDATION_FAILED":
                        final_status = TerminalResolutionStatus.INVALID
                        is_blocking = True
                        blocking_reason = matching_block.get("reason") or "Tier 3 validation failed safety checks."
                        unresolved_blocking_count += 1
                    else:
                        final_status = TerminalResolutionStatus.UNSUPPORTED
                        # High severity unsupported is blocking; low severity observational is informational
                        if severity in ("HIGH", "CRITICAL"):
                            is_blocking = True
                            blocking_reason = matching_block.get("reason") or "High-severity unsupported issue."
                            unsupported_blocking_count += 1
                        else:
                            is_blocking = False
                            blocking_reason = ""
                            informational_count += 1
                else:
                    # Non-blocking observational default
                    final_status = TerminalResolutionStatus.INFORMATIONAL
                    is_blocking = False
                    blocking_reason = ""
                    informational_count += 1

            source_ids = [iss_id] + getattr(c_iss, "merged_issue_ids", [])
            manifest_canonical_issues.append({
                "canonical_issue_id": iss_id,
                "issue_fingerprint": c_iss.compute_fingerprint() if hasattr(c_iss, "compute_fingerprint") else None,
                "dataset": dataset_name,
                "column": col_name,
                "issue_type": c_iss.issue_type,
                "severity": severity,
                "final_status": final_status.value,
                "is_blocking": is_blocking,
                "blocking_reason": blocking_reason,
                "source_issue_ids": source_ids,
            })

            if is_blocking:
                blocking_issues_list.append(
                    BlockingIssue(
                        issue_id=iss_id,
                        canonical_issue_id=iss_id,
                        dataset=dataset_name,
                        column=col_name,
                        severity=severity,
                        category=category,
                        resolution_status=final_status.value,
                        reason=blocking_reason,
                        requires_approval=(final_status == TerminalResolutionStatus.APPROVAL_REQUIRED),
                        source_issue_ids=tuple(source_ids),
                    )
                )
            elif final_status == TerminalResolutionStatus.INFORMATIONAL:
                warnings_list.append({
                    "issue_id": iss_id,
                    "dataset": dataset_name,
                    "column": col_name,
                    "issue_type": c_iss.issue_type,
                    "severity": severity,
                    "message": c_iss.message,
                })

        # 6. Strict accounting verification
        blocking_issue_count = (
            unresolved_blocking_count
            + blocking_manual_review_count
            + approval_pending_blocking_count
            + unsupported_blocking_count
        )
        assert len(blocking_issues_list) == blocking_issue_count, (
            f"Blocking issue count invariant failed: list={len(blocking_issues_list)} vs sum={blocking_issue_count}"
        )

        # 7. Readiness state derivation
        if blocking_issue_count == 0:
            readiness_state = ReadinessState.READY_TO_CONFIRM
        elif approval_pending_blocking_count > 0:
            readiness_state = ReadinessState.APPROVAL_REQUIRED
        elif blocking_manual_review_count > 0:
            readiness_state = ReadinessState.MANUAL_REVIEW_REQUIRED
        elif unsupported_blocking_count > 0:
            readiness_state = ReadinessState.UNSUPPORTED_BLOCKING
        else:
            readiness_state = ReadinessState.INVALID_PLAN

        # 8. Schema and Plan Fingerprinting
        schema_fp = self.compute_schema_fingerprint(assess, dataset_profiles=profiles)
        compiler_ctx = CompilerContext(
            session_id=session_id,
            target_engine=target_engine,
            preserve_source_schema=True,
            dataset_profiles=profiles,
            detected_issues=detected_issues,
            structured_actions=all_actions,
            business_rules=active_rules if isinstance(active_rules, list) else [],
        )
        plan_fp = compute_plan_fingerprint(compiler_ctx)
        assessment_fp = hashlib.sha256(json.dumps(assess.get("summary") or {}, sort_keys=True).encode("utf-8")).hexdigest()
        rules_fp = hashlib.sha256(json.dumps([r.rule_id for r in (b_rules or [])], sort_keys=True).encode("utf-8")).hexdigest()

        # 9. Build frozen CanonicalResolutionManifest only if ready for confirmation
        manifest_dict: Optional[Dict[str, Any]] = None
        manifest_hash: Optional[str] = None

        if blocking_issue_count == 0:
            raw_actions = [
                act.model_dump() if hasattr(act, "model_dump") else act.__dict__
                for act in all_actions
            ]
            manifest_obj = CanonicalResolutionManifest(
                manifest_version="1.0",
                session_id=session_id,
                created_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                assessment_fingerprint=f"sha256:{assessment_fp}",
                plan_fingerprint=f"sha256:{plan_fp}",
                schema_fingerprint=f"sha256:{schema_fp}",
                rules_fingerprint=f"sha256:{rules_fp}",
                policy_version=policy_version,
                compiler_version=compiler_version,
                target_engine=target_engine.value if hasattr(target_engine, "value") else str(target_engine),
                canonical_issues=manifest_canonical_issues,
                issue_alias_map=issue_alias_map,
                structured_actions=raw_actions,
                approval_references=[p.proposal_id for p in tier3_res.approval_pending_proposals],
                waiver_references=list(approved_waivers_map.keys()),
                manual_resolution_references=[m["issue_id"] for m in tier3_res.manual_review_issues],
                reconciliation={
                    "assessment_issue_count": len(assess.get("issues", [])) if assess else len(canonical_issues),
                    "canonical_issue_count": len(canonical_issues),
                    "compiled_action_ready_count": compiled_action_ready_count,
                    "waived_count": waived_count,
                    "informational_count": informational_count,
                    "blocking_issue_count": 0,
                },
            )
            manifest_hash = manifest_obj.bind_hash()
            manifest_dict = manifest_obj.model_dump()

        ready_for_confirmation = (
            blocking_issue_count == 0
            and manifest_dict is not None
            and manifest_hash is not None
        )

        return PlanReadinessResult(
            ready_for_confirmation=ready_for_confirmation,
            readiness_state=readiness_state,
            assessment_issue_count=len(assess.get("issues", [])) if assess else len(canonical_issues),
            canonical_issue_count=len(canonical_issues),
            resolved_issue_count=compiled_action_ready_count + waived_count,
            unresolved_blocking_count=unresolved_blocking_count,
            blocking_manual_review_count=blocking_manual_review_count,
            approval_pending_blocking_count=approval_pending_blocking_count,
            unsupported_blocking_count=unsupported_blocking_count,
            blocking_issue_count=blocking_issue_count,
            blocking_issues=tuple(blocking_issues_list),
            warnings=tuple(warnings_list),
            canonical_manifest=manifest_dict,
            manifest_hash=manifest_hash,
            schema_fingerprint=schema_fp,
            policy_version=policy_version,
            compiler_version=compiler_version,
        )
