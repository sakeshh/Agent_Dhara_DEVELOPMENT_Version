from __future__ import annotations

import hashlib
import json
import os
import time
import threading
from typing import Any, Dict, List, Optional, Tuple

_PLANNER_LOCK = threading.Lock()

from agent.transformation_suggester import suggest_transformations
from agent.etl_pipeline.business_rules import normalize_business_rules, column_is_excluded
from agent.etl_pipeline.classify_steps import classify_step_bucket
from agent.etl_pipeline.relationship_planner import build_relationship_plan
from agent.etl_pipeline.manual_review_catalog import enrich_manual_review_item
from agent.etl_pipeline.step_metadata import (
    build_plan_invariants,
    enrich_relationship_plan_joins,
    finalize_dataset_steps,
)
from agent.etl_pipeline.step_params import build_ri_step_params, build_step_params

# Lower number = earlier in pipeline (per column / global)
_ACTION_PRIORITY: Dict[str, int] = {
    "trim": 5,
    "lowercase": 8,
    "uppercase": 8,
    "zero_to_null": 30,
    "replace_sentinel_values": 31,
    "cast_type": 35,
    "coerce_numeric": 40,
    "parse_dates": 45,
    "sanitize_email": 50,
    "normalize_phone": 55,
    "hash_phone": 56,
    "mask_phone": 57,
    "nullify_future_dates": 48,
    "nullify_dummy_dates": 48,
    "nullify_punctuation": 32,
    "regex_replace": 60,
    "range_clip": 65,
    "clip_or_flag": 65,
    "flag_outliers": 65,
    "clip_outliers": 65,
    "cap_outliers": 65,
    "standardize_boolean": 70,
    "replace_values": 75,
    "fill_or_drop": 80,
    "fill_nulls_simple": 80,
    "drop_column": 85,
    "exclude_column": 86,
    "deduplicate": 200,
    "validate_referential_integrity_or_stage": 300,
}


def _plan_id() -> str:
    return f"plan_{int(time.time())}"


def _assessment_signature(assessment: Dict[str, Any]) -> str:
    try:
        blob = json.dumps(assessment, sort_keys=True, default=str)
    except Exception:
        blob = str(assessment)
    return hashlib.sha256(blob.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _dataset_columns(assessment: Dict[str, Any], dataset: str) -> Dict[str, Any]:
    ds = (assessment.get("datasets") or {}).get(dataset) or {}
    return ds.get("columns") or {}


def _col_stats_for_step(
    assessment: Dict[str, Any], dataset: str, column: Optional[str]
) -> Dict[str, Any]:
    if not dataset or not column:
        return {}
    ds_data = (assessment.get("datasets") or {}).get(dataset) or {}
    stats = dict((ds_data.get("columns") or {}).get(column) or {})
    total = int(ds_data.get("row_count") or 0)
    stats["row_count"] = total
    null_pct = stats.get("null_percentage")
    if null_pct is not None and total > 0:
        try:
            stats["null_count"] = int(round(float(null_pct) * total))
        except (TypeError, ValueError):
            pass
    return stats


def _build_evidence(
    suggestion: Dict[str, Any],
    col_stats: Dict[str, Any],
    action: str,
    rules: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Structured evidence from assessment DQ + column profile (no invented stats).
    """
    col = suggestion.get("column")
    issue_type = str(suggestion.get("issue_type") or "")
    message = str(suggestion.get("message") or "").strip()
    affected = suggestion.get("row_count_affected")
    sev = str(suggestion.get("severity") or "medium").lower()

    stype = col_stats.get("dtype") or col_stats.get("semantic_type") or "unknown"
    total = int(col_stats.get("row_count") or 0)
    nulls = col_stats.get("null_count")
    if nulls is None and col_stats.get("null_percentage") is not None and total > 0:
        try:
            nulls = int(round(float(col_stats["null_percentage"]) * total))
        except (TypeError, ValueError):
            nulls = 0
    nulls = int(nulls or 0)
    null_pct = round((nulls / max(total, 1)) * 100, 2) if nulls else 0.0
    if col_stats.get("null_percentage") is not None:
        try:
            null_pct = round(float(col_stats["null_percentage"]) * 100, 2)
        except (TypeError, ValueError):
            pass

    why_parts: List[str] = []
    alternatives: List[str] = []
    confidence = 0.55

    if message:
        why_parts.append(message)
        confidence += 0.12
    if affected is not None and isinstance(affected, (int, float)) and affected >= 0:
        why_parts.append(f"~{int(affected):,} rows affected per DQ scan")
        confidence += 0.15
    if issue_type:
        why_parts.append(f"issue_type={issue_type}")

    mean = col_stats.get("mean")
    median = col_stats.get("median")
    std = col_stats.get("std")
    skew = col_stats.get("skew")
    p5 = col_stats.get("p5")
    p95 = col_stats.get("p95")
    recommended_fill: Optional[str] = None

    act = (action or "").lower()
    if act in ("fill_nulls_simple", "fill_or_drop"):
        if null_pct > 0:
            why_parts.append(f"{nulls:,} nulls ({null_pct}% of {total:,} rows)")
        if skew is not None and abs(float(skew)) > 1.0 and median is not None:
            recommended_fill = "median"
            why_parts.append(
                f"skewed distribution (skew={round(float(skew), 2)}) — "
                f"fill with median ({round(float(median), 4)}) not mean"
            )
            confidence = min(confidence + 0.1, 0.92)
        elif median is not None and mean is not None:
            recommended_fill = "median" if abs(float(skew or 0)) > 0.5 else "mean"
            why_parts.append(
                f"near-normal spread — fill with {recommended_fill} "
                f"(mean={round(float(mean), 4)}, median={round(float(median), 4)})"
            )
        if null_pct < 1.0:
            alternatives.append("Drop rows — null rate is very low, minimal data loss")
        if null_pct > 20.0:
            alternatives.append(
                f"Consider dropping column — {null_pct}% missing may be unreliable"
            )
            confidence = min(confidence, 0.62)
        if col_stats.get("semantic_type"):
            why_parts.append(f"semantic_type={col_stats['semantic_type']}")

    elif act in ("cast_type", "coerce_numeric"):
        if col_stats.get("dtype_inference"):
            why_parts.append(f"inferred type hint: {col_stats['dtype_inference']}")
        alternatives.append("Use try-cast to preserve rows with conversion errors")

    elif act == "deduplicate":
        dupes = col_stats.get("duplicate_value_count")
        if dupes:
            why_parts.append(f"{int(dupes):,} duplicate values in column")
        if affected:
            why_parts.append(f"{int(affected):,} duplicate-related rows flagged")
        alternatives.append("Keep duplicates if source intentionally produces them (e.g. event logs)")

    elif act in ("flag_outliers", "clip_outliers", "cap_outliers", "clip_or_flag"):
        if p5 is not None and p95 is not None:
            why_parts.append(f"outliers outside p5–p95 range [{round(float(p5), 4)}, {round(float(p95), 4)}]")
        if std is not None and mean is not None:
            why_parts.append(f"std={round(float(std), 2)}, mean={round(float(mean), 2)}")
        if issue_type in ("numeric_outliers_iqr", "negative_values", "suspicious_zero"):
            why_parts.append("numeric outlier pattern detected in assessment")
        alternatives.append("Flag outliers instead of clipping — preserves values for audit")
        alternatives.append("Cap at p1/p99 for less aggressive trimming")

    elif act in ("sanitize_email", "normalize_phone"):
        if affected:
            why_parts.append(f"{int(affected):,} values failed format checks")

    elif act in ("trim", "lowercase", "uppercase"):
        if issue_type == "whitespace" or issue_type == "case_inconsistency":
            why_parts.append("format inconsistency detected in column values")

    elif act == "parse_dates":
        if col_stats.get("dtype_inference") == "datetime_like":
            why_parts.append("mixed or string dates inferred from profile")
        alternatives.append("Specify target date format if downstream requires strict typing")

    if rules.get("never_drop_rows") and act == "fill_nulls_simple":
        why_parts.append("'never_drop_rows' rule active — drop option removed")
        confidence = min(confidence + 0.05, 0.95)

    if sev == "high" and act not in ("deduplicate",):
        confidence = min(confidence, 0.68)
    if suggestion.get("auto_fixable"):
        confidence = min(confidence + 0.08, 0.94)
    if not why_parts:
        why_parts.append(f"Action '{action}' recommended from assessment profile")

    confidence = round(max(0.35, min(confidence, 0.95)), 2)

    out: Dict[str, Any] = {
        "null_count": nulls if nulls else None,
        "null_pct": null_pct if null_pct else None,
        "dtype": stype,
        "row_count": total if total else None,
        "issue_type": issue_type or None,
        "severity": sev,
        "why_this_action": " | ".join(why_parts),
        "alternatives": alternatives,
        "confidence": confidence,
        "rule_override": bool(rules.get("never_drop_rows") and act == "fill_nulls_simple"),
    }
    if mean is not None:
        out["mean"] = round(float(mean), 4)
    if median is not None:
        out["median"] = round(float(median), 4)
    if std is not None:
        out["std"] = round(float(std), 4)
    if skew is not None:
        out["skew"] = round(float(skew), 4)
    if recommended_fill:
        out["recommended_fill"] = recommended_fill
    return out


def _recommend_engine(
    source_context: Optional[Dict[str, Any]],
    assessment: Dict[str, Any],
) -> Dict[str, Any]:
    """Engine recommendation from source type + data scale."""
    ctx = source_context or {}
    src_type = str(ctx.get("type") or "unknown").lower()
    target_platform = str(ctx.get("target_platform") or "").lower()
    target_format = str(ctx.get("target_format") or "").lower()
    dataset_count = int(ctx.get("dataset_count") or ctx.get("source_count") or len(assessment.get("datasets") or {}) or 1)
    rels = assessment.get("relationships") or {}
    if isinstance(rels, list):
        rels_len = len(rels)
    else:
        rels_len = len(rels.get("joins", []))
    relationship_count = int(ctx.get("relationship_count") or rels_len or 0)
    size_mb = float(ctx.get("size_mb") or 0)
    row_count = int(ctx.get("row_count") or 0)

    # Fabric Lakehouse / Delta always requires PySpark
    if target_platform in ("fabric_lakehouse", "fabric", "delta_lake", "synapse") or target_format in ("delta", "parquet"):
        return {
            "engine": "pyspark",
            "scope": "plan",
            "dataset_count": dataset_count,
            "relationship_count": relationship_count,
            "target_platform": target_platform or "fabric_lakehouse",
            "reason": (
                f"Target platform is {target_platform or 'fabric_lakehouse'} with {target_format or 'delta'} format — "
                f"PySpark is the only supported engine."
            ),
            "alternatives": [],
            "warning": None,
        }

    # Multi-dataset with relationships always requires PySpark
    if dataset_count > 1 and relationship_count > 0:
        return {
            "engine": "pyspark",
            "scope": "plan",
            "dataset_count": dataset_count,
            "relationship_count": relationship_count,
            "target_platform": target_platform or "general",
            "reason": (
                f"Multi-dataset plan ({dataset_count} sources, {relationship_count} relationship(s)) — "
                f"PySpark is required for distributed join partitioning."
            ),
            "alternatives": ["Python/Pandas — only for single-dataset with no joins"],
            "warning": None,
        }

    if row_count == 0:
        for ds in (assessment.get("datasets") or {}).values():
            if isinstance(ds, dict):
                row_count = max(row_count, int(ds.get("row_count") or 0))
    if size_mb == 0 and row_count > 0:
        size_mb = round(row_count * 0.0005, 2)

    if src_type in ("sql_server", "azure_sql"):
        return {
            "engine": "sql",
            "dialect": "tsql",
            "reason": (
                f"Source is {src_type} — T-SQL runs in-database with no file export."
            ),
            "alternatives": [
                "Python via SQLAlchemy — script-based ETL",
                "ADF — if part of a larger Azure pipeline",
            ],
            "warning": None,
        }

    if src_type in ("postgres", "mysql"):
        return {
            "engine": "sql",
            "dialect": "ansi",
            "reason": f"Source is {src_type} — ANSI SQL is portable across engines.",
            "alternatives": ["Python/Pandas via SQLAlchemy"],
            "warning": None,
        }

    if src_type in ("blob_storage", "adls") or size_mb > 500 or row_count > 1_000_000:
        return {
            "engine": "pyspark",
            "dialect": None,
            "reason": (
                f"Large or cloud-backed data ({row_count:,} rows, ~{size_mb}MB) — "
                f"PySpark scales beyond single-node Pandas."
            ),
            "alternatives": [
                "ADF — existing Azure Data Factory pipelines",
                "Python — only for samples or small subsets",
            ],
            "warning": (
                "PySpark needs a cluster (Databricks, Synapse, or local Spark)."
            ),
        }

    if src_type in ("adf_pipeline", "databricks"):
        return {
            "engine": "adf",
            "dialect": None,
            "reason": "ADF-native source — Mapping Data Flow JSON fits your pipeline.",
            "alternatives": ["PySpark — notebook-based transforms"],
            "warning": "ADF JSON needs linked services configured in your factory.",
        }

    ext = str(ctx.get("extension") or ".csv").lower()
    file_notes = {
        ".xlsx": "Excel — pd.read_excel()",
        ".xls": "Excel — pd.read_excel()",
        ".json": "JSON — pd.read_json()",
        ".jsonl": "JSON lines — pd.read_json(lines=True)",
        ".parquet": "Parquet — pd.read_parquet()",
        ".csv": "CSV — pd.read_csv()",
        ".tsv": "TSV — pd.read_csv(sep='\\t')",
    }
    file_note = file_notes.get(ext, f"{ext or 'file'} detected")

    warn = None
    if size_mb >= 200:
        warn = f"At ~{size_mb}MB, Pandas may be slow — consider PySpark."

    return {
        "engine": "python",
        "dialect": None,
        "reason": (
            f"{file_note}. {row_count:,} rows (~{size_mb}MB) — "
            f"suitable for Pandas on a single machine."
        ),
        "alternatives": [
            "PySpark — if data grows past ~500MB or moves to lake storage",
            "SQL — after loading into a database",
        ],
        "warning": warn,
    }


def _steps_from_business_notes(
    rules: Dict[str, Any],
    assessment: Dict[str, Any],
) -> List[Tuple[str, str, str, str]]:
    """
    Promote explicit business-note instructions into plan steps, sentence-by-sentence.
    Explicitly ignore matches if a sentence contains a negation pattern.
    """
    import re
    notes = str(rules.get("notes") or "")
    sentences = re.split(r'\.(?=\s|$)|[\;\!\n]+', notes)
    
    out: List[Tuple[str, str, str, str]] = []
    ds_names = list((assessment.get("datasets") or {}).keys())
    
    negation_patterns = [r"don't", r"do not", r"never", r"no ", r"avoid", r"without", r"skip"]
    
    for sentence in sentences:
        sentence_clean = sentence.strip().lower()
        if not sentence_clean:
            continue
            
        # Check if this sentence contains negation
        is_negated = any(re.search(pat, sentence_clean) for pat in negation_patterns)
        if is_negated:
            continue
            
        # Check privacy targets
        targets_keywords = []
        if "phone" in sentence_clean:
            targets_keywords.append("phone")
        if "email" in sentence_clean:
            targets_keywords.append("email")
        if "ssn" in sentence_clean:
            targets_keywords.append("ssn")
        if "credit card" in sentence_clean or "creditcard" in sentence_clean or "cc " in sentence_clean or " cc" in sentence_clean:
            targets_keywords.append("credit card")
            
        if not targets_keywords:
            continue
        if not any(w in sentence_clean for w in ("hash", "mask", "privacy", "secure", "protect")):
            continue
            
        use_hash = "hash" in sentence_clean
        use_mask = "mask" in sentence_clean and not use_hash
        action = "hash_phone" if use_hash else ("mask_phone" if use_mask else "hash_phone")
        
        # Match datasets in this sentence
        targets = []
        for ds in ds_names:
            dsl = ds.lower()
            if dsl in sentence_clean or dsl.replace("_", "") in sentence_clean.replace("_", "").replace(" ", ""):
                targets.append(ds)
        if not targets:
            for ds in ds_names:
                if ".xml" in ds.lower() and "xml" in sentence_clean:
                    targets.append(ds)
        if not targets and len(ds_names) == 1:
            targets = ds_names
            
        for ds in targets:
            for col in _dataset_columns(assessment, ds).keys():
                col_lower = str(col).lower()
                matched_kw = None
                if "phone" in targets_keywords and "phone" in col_lower:
                    matched_kw = "phone"
                elif "email" in targets_keywords and "email" in col_lower:
                    matched_kw = "email"
                elif "ssn" in targets_keywords and "ssn" in col_lower:
                    matched_kw = "ssn"
                elif "credit card" in targets_keywords and ("credit" in col_lower and ("card" in col_lower or "cc" in col_lower)):
                    matched_kw = "credit card"
                    
                if matched_kw:
                    out.append(
                        (
                            ds,
                            col,
                            action,
                            f"business_rules.notes: {'hash' if use_hash else 'mask'} {matched_kw} for privacy",
                        )
                    )
    return out


def _apply_rules_to_action(
    action: str,
    column: Optional[str],
    business_rules: Dict[str, Any],
) -> Tuple[str, Optional[str]]:
    """
    Returns (action, note) where note is a human-readable override reason.
    """
    if business_rules.get("never_drop_rows") and action == "fill_or_drop":
        return "fill_nulls_simple", "never_drop_rows: using fill-only instead of drop/fill choice"
    return action, None


_SUGGESTION_CACHE: Dict[str, dict] = {}


class PlanBuilder:
    def __init__(self, assessment: Dict[str, Any], business_rules_raw: Any, **kwargs):
        self.assessment = assessment
        self.business_rules_raw = business_rules_raw
        self.kwargs = kwargs
        self.build_warnings = []

    def build(self) -> Dict[str, Any]:
        import logging
        logger = logging.getLogger("agent.etl_pipeline.planner")

        assessment = self.assessment
        business_rules_raw = self.business_rules_raw
        engine = self.kwargs.get("engine", "python")
        source_context = self.kwargs.get("source_context")
        generation_mode = self.kwargs.get("generation_mode", "full")
        dq_recommendations = self.kwargs.get("dq_recommendations")
        semantic_context = self.kwargs.get("semantic_context")

        if not isinstance(assessment, dict) or not assessment.get("datasets"):
            raise ValueError("Invalid assessment: missing datasets")

        rules = normalize_business_rules(business_rules_raw)
        exclude = set(rules.get("exclude_columns") or [])

        # Build explicit semantic schema layer
        sem_schema = {}
        from agent.etl_pipeline.semantic_classifier import classify_column_semantic, SemanticDescriptor
        from agent.etl_pipeline.semantic_llm_enricher import enrich_low_confidence_columns

        low_conf_cols = {}
        for ds_name, ds_meta in (assessment.get("datasets") or {}).items():
            if isinstance(ds_meta, dict):
                cols = ds_meta.get("columns") or {}
                for col_name, col in cols.items():
                    if isinstance(col, dict):
                        descriptor = classify_column_semantic(col_name, col)
                        key = f"{ds_name}.{col_name}"
                        sem_schema[key] = descriptor
                        if descriptor["confidence"] < 0.75:
                            low_conf_cols[key] = {
                                "col_name": col_name,
                                "col_meta": col,
                                "descriptor": descriptor
                            }

        # Layer 2: Optional LLM enrichment
        if low_conf_cols and os.getenv("ETL_PLAN_SEMANTIC_LLM", "").strip().lower() in ("1", "true", "yes"):
            try:
                enriched = enrich_low_confidence_columns(low_conf_cols)
                for key, enriched_desc in enriched.items():
                    sem_schema[key] = SemanticDescriptor(enriched_desc)
            except Exception as e:
                self.build_warnings.append(f"Semantic LLM enrichment failed: {e}")

        # Layer 3: User overrides from rules.get("semantic_overrides")
        overrides = rules.get("semantic_overrides") or {}
        for override_key, override_val in overrides.items():
            if not isinstance(override_val, dict):
                continue
            for key in list(sem_schema.keys()):
                if key == override_key or key.endswith(f".{override_key}"):
                    desc_dict = dict(sem_schema[key])
                    desc_dict.update(override_val)
                    desc_dict["inferred_by"] = "user_override"
                    desc_dict["confidence"] = 1.0
                    sem_schema[key] = SemanticDescriptor(desc_dict)

        # Layer 3b: Governance semantic_context
        sem_ctx_pkg = assessment.get("semantic_context") or {}
        for ds_name, ctx in (sem_ctx_pkg.get("by_dataset") or {}).items():
            if not isinstance(ctx, dict):
                continue
            base_conf = float(ctx.get("semantic_confidence") or 0.75)
            for col_name, term in (ctx.get("business_terms") or {}).items():
                key = f"{ds_name}.{col_name}"
                desc = {
                    "semantic_type": "string",
                    "sub_type": "",
                    "confidence": min(0.98, base_conf + 0.05),
                    "inferred_by": "governance_semantic_context",
                    "description": str(term)[:500],
                }
                if key not in sem_schema:
                    sem_schema[key] = SemanticDescriptor(desc)
                elif float(sem_schema[key].get("confidence") or 0) < 0.82:
                    sem_schema[key]["description"] = str(term)[:500]
                    sem_schema[key]["inferred_by"] = "governance_semantic_context"

        if semantic_context is None:
            semantic_context = assessment.get("semantic_context") or {}

        from agent.semantic_context import SemanticCleaningPlan
        semantic_plan = None
        if semantic_context:
            sem_model = semantic_context.get("semantic_model")
            if isinstance(sem_model, dict) and "entities" in sem_model:
                try:
                    semantic_plan = SemanticCleaningPlan(
                        entities=sem_model.get("entities") or {},
                        relationships=sem_model.get("relationships") or [],
                    )
                except Exception:
                    pass
            if semantic_plan is None and "entities" in semantic_context:
                try:
                    semantic_plan = SemanticCleaningPlan(
                        entities=semantic_context.get("entities") or {},
                        relationships=semantic_context.get("relationships") or [],
                    )
                except Exception:
                    pass

        # 4.7 Cache suggestion package
        assess_sig = _assessment_signature(assessment)
        if assess_sig in _SUGGESTION_CACHE:
            sug_pkg = _SUGGESTION_CACHE[assess_sig]
        else:
            try:
                sug_pkg = suggest_transformations(assessment, semantic_plan=semantic_plan)
                _SUGGESTION_CACHE[assess_sig] = sug_pkg
            except Exception as e:
                self.build_warnings.append(f"suggest_transformations failed: {e}")
                sug_pkg = {"suggested_transformations": [], "summary": {}}

        suggestions: List[Dict[str, Any]] = list(sug_pkg.get("suggested_transformations") or [])
        if source_context and "suggestions" in source_context:
            suggestions.extend(source_context["suggestions"])

        # Load business rules as TaggedRules
        from agent.etl_pipeline.business_rules import to_tagged_rules
        business_tagged_rules = []
        datasets_known = list(assessment.get("datasets") or {})
        for ds_name in datasets_known:
            try:
                business_tagged_rules.extend(to_tagged_rules(rules, ds_name, assessment=assessment))
            except Exception as e:
                self.build_warnings.append(f"to_tagged_rules failed for dataset {ds_name}: {e}")

        # Append business rules not in suggestions
        for r in business_tagged_rules:
            exists = False
            for s in suggestions:
                if s.get("dataset") == r.dataset and s.get("column") == r.column and s.get("suggested_action") == r.action:
                    exists = True
                    break
            if not exists:
                suggestions.append({
                    "dataset": r.dataset,
                    "column": r.column,
                    "issue_type": r.issue_type,
                    "severity": "medium",
                    "message": f"Business rule contract: {r.source_detail}",
                    "suggested_action": r.action,
                    "manual_guidance": "",
                    "row_count_affected": None,
                    "auto_fixable": True,
                })

        # Merge LLM recommendations
        if dq_recommendations:
            try:
                from agent.etl_pipeline.llm_rec_mapper import map_llm_recommendation_to_action, compute_llm_confidence
                recs_list = []
                if isinstance(dq_recommendations, dict):
                    recs_list = dq_recommendations.get("recommendations") or []
                elif isinstance(dq_recommendations, list):
                    recs_list = dq_recommendations

                def _norm(v):
                    return str(v or "").strip().lower()

                sug_map = {}
                for sug in suggestions:
                    k = (_norm(sug.get("dataset")), _norm(sug.get("column")), _norm(sug.get("issue_type")))
                    sug_map[k] = sug

                for rec in recs_list:
                    if not isinstance(rec, dict):
                        continue
                    ds = rec.get("dataset")
                    col = rec.get("column")
                    it = rec.get("issue_type")

                    mapped_action = map_llm_recommendation_to_action(rec)
                    confidence = compute_llm_confidence(rec)

                    matched_sug = None
                    key = (_norm(ds), _norm(col), _norm(it))
                    if key in sug_map:
                        matched_sug = sug_map[key]
                    else:
                        candidates = [
                            sug for sug in suggestions
                            if _norm(sug.get("dataset")) == _norm(ds) and _norm(sug.get("column")) == _norm(col)
                        ]
                        if len(candidates) == 1:
                            matched_sug = candidates[0]
                        elif len(candidates) > 1:
                            for cand in candidates:
                                cit = _norm(cand.get("issue_type"))
                                rit = _norm(it)
                                if rit in cit or cit in rit:
                                    matched_sug = cand
                                    break
                            if not matched_sug:
                                matched_sug = candidates[0]

                    if matched_sug:
                        matched_sug["llm_recommendation"] = rec
                        matched_sug["llm_confidence"] = confidence
                        if mapped_action and mapped_action != "noop":
                            matched_sug["suggested_action"] = mapped_action
                            matched_sug["auto_fixable"] = False
                            matched_sug["authority_source"] = "LLM_ADVISORY"
                    else:
                        new_sug = {
                            "dataset": ds,
                            "column": col,
                            "issue_type": it or "llm_inferred_issue",
                            "severity": rec.get("severity") or "medium",
                            "message": rec.get("why_it_matters") or rec.get("suggested_fix") or "LLM Inferred Issue",
                            "suggested_action": mapped_action or "noop",
                            "manual_guidance": rec.get("suggested_fix") or "",
                            "row_count_affected": None,
                            "auto_fixable": False,
                            "authority_source": "LLM_ADVISORY",
                            "llm_recommendation": rec,
                            "llm_confidence": confidence,
                        }
                        suggestions.append(new_sug)
                        new_key = (_norm(ds), _norm(col), _norm(it or "llm_inferred_issue"))
                        sug_map[new_key] = new_sug
            except Exception as e:
                self.build_warnings.append(f"dq_recommendations merge failed: {e}")

        # Run conflict detection on all TaggedRules
        from agent.etl_pipeline.rule_provenance import TaggedRule, RuleProvenance
        from agent.etl_pipeline.conflict_detector import detect_conflicts
        
        all_tagged_rules = []
        all_tagged_rules.extend(business_tagged_rules)
        
        if semantic_plan:
            try:
                all_tagged_rules.extend(semantic_plan.to_tagged_rules())
            except Exception:
                pass
                
        existing_rule_keys = set()
        for r in all_tagged_rules:
            existing_rule_keys.add((r.dataset, r.column, r.issue_type, r.action))
            
        for s in suggestions:
            ds = s.get("dataset") or ""
            col = s.get("column") or ""
            it = s.get("issue_type") or ""
            act = s.get("suggested_action")
            if act and act != "review_manually":
                key = (ds, col, it, act)
                if key not in existing_rule_keys:
                    all_tagged_rules.append(TaggedRule(
                        dataset=ds,
                        column=col,
                        issue_type=it,
                        action=act,
                        provenance=RuleProvenance.AUTO_DETECTED,
                        source_detail=s.get("message") or "Auto-detected DQ rule"
                    ))
                    existing_rule_keys.add(key)
                    
        # Detect conflicts and resolve them
        resolved_rules, conflicts = detect_conflicts(all_tagged_rules)
        
        resolved_map = {}
        for r in resolved_rules:
            resolved_map[(r.dataset, r.column, r.issue_type)] = r.action
            
        # 5.2 Cache evaluate_dq_gate result
        # Compute dq_gate_summary at plan build time and store it on the plan
        dq_gate_summary = {"blocking_issues": [], "passed": True}
        threshold = float(rules.get("dq_threshold", 70.0))
        if generation_mode == "full":
            from agent.etl_pipeline.dq_gate import check_dq_gate
            gate_issues = []
            gate_passed = True
            for ds_name in datasets_known:
                try:
                    gate_res = check_dq_gate(
                        assessment, ds_name, 
                        threshold=threshold, 
                        force_unlock=ds_name in rules.get("force_unlock", []), 
                        sem_schema=sem_schema
                    )
                    if not gate_res["passed"]:
                        gate_passed = False
                        gate_issues.append({
                            "dataset": ds_name,
                            "reason": f"Data quality score ({gate_res['score']}) below threshold ({threshold})",
                            "score": gate_res.get("score")
                        })
                except Exception as e:
                    self.build_warnings.append(f"check_dq_gate failed for {ds_name}: {e}")
            dq_gate_summary = {"blocking_issues": gate_issues, "passed": gate_passed}

        # Build steps
        # Compile suggestions to steps
        from agent.etl_pipeline.issue_to_step_compiler import compile_issues_to_steps
        
        # Apply resolved conflict actions back to suggestions
        for s in suggestions:
            ds = s.get("dataset") or ""
            col = s.get("column") or ""
            it = s.get("issue_type") or ""
            if (ds, col, it) in resolved_map:
                s["suggested_action"] = resolved_map[(ds, col, it)]

        datasets_steps, manual_review = compile_issues_to_steps(suggestions, rules, sem_schema)
        
        # Check for missing required columns and append to manual_review
        req_cols = rules.get("required_columns") or []
        for rc in req_cols:
            found = False
            for ds_name in datasets_known:
                cols_lower = {c.lower() for c in _dataset_columns(assessment, ds_name).keys()}
                if str(rc).lower() in cols_lower:
                    found = True
                    break
            if not found:
                manual_review.append(
                    enrich_manual_review_item({
                        "dataset": "global",
                        "column": rc,
                        "issue_type": "missing_required_column",
                        "severity": "high",
                        "message": f"Required column '{rc}' not found in any assessed dataset.",
                        "guidance": "Provide the column in the source data or remove/correct it in business rules.",
                    })
                )

        blocked = []

        step_map = {}
        for ds_name, steps in datasets_steps.items():
            for st in steps:
                col = st.get("column")
                action = st.get("action")
                it = st.get("source_issue_type")
                sev = st.get("severity") or "medium"
                row_est = st.get("estimated_affected_rows")
                pri = _ACTION_PRIORITY.get(action, 80)
                key = (ds_name, col, action, it)

                sug_dict = {
                    "dataset": ds_name,
                    "column": col,
                    "issue_type": it,
                    "severity": sev,
                    "message": st.get("message"),
                    "row_count_affected": row_est,
                }
                col_stats = _col_stats_for_step(assessment, ds_name, col)
                evidence = _build_evidence(sug_dict, col_stats, action, rules)
                if st.get("llm_recommendation"):
                    evidence["llm_recommendation"] = st["llm_recommendation"]

                params = build_step_params(
                    action,
                    column=col,
                    col_stats=col_stats,
                    evidence=evidence,
                    rules=rules,
                    issue_type=it,
                )

                if st.get("params"):
                    params.update(st["params"])

                entry = {
                    "dataset": ds_name or "_global",
                    "column": col,
                    "action": action,
                    "source_issue_type": it,
                    "severity": sev,
                    "estimated_affected_rows": row_est,
                    "priority": pri,
                    "note": st.get("note"),
                    "params": params,
                    "evidence": evidence,
                    "message": st.get("message"),
                }
                if st.get("llm_recommendation"):
                    entry["llm_recommendation"] = st["llm_recommendation"]

                prev = step_map.get(key)
                if not prev or (row_est and (prev.get("estimated_affected_rows") or 0) < (row_est or 0)):
                    step_map[key] = entry

        for ds, col, act, note in _steps_from_business_notes(rules, assessment):
            if column_is_excluded(col, exclude):
                continue
            key = (ds, col, act, "business_notes")
            if key not in step_map:
                cstats = _col_stats_for_step(assessment, ds, col)
                if ds and col:
                    col_key = f"{ds}.{col}"
                    desc = sem_schema.get(col_key)
                    if desc:
                        cstats = dict(cstats)
                        cstats["semantic_type"] = desc.get("semantic_type")
                        cstats["sub_type"] = desc.get("sub_type")
                        cstats["pii_level"] = desc.get("pii_level")
                        cstats["fill_strategy"] = desc.get("fill_strategy")
                ev = {"why_this_action": note, "confidence": 0.9}
                step_map[key] = {
                    "dataset": ds,
                    "column": col,
                    "action": act,
                    "source_issue_type": "business_notes",
                    "severity": "medium",
                    "priority": _ACTION_PRIORITY.get(act, 56),
                    "note": note,
                    "params": build_step_params(act, column=col, col_stats=cstats, evidence=ev, rules=rules),
                    "evidence": ev,
                    "message": note,
                }

        # Per-dataset ordered steps
        datasets_out: Dict[str, Any] = {}
        global_steps: List[Dict[str, Any]] = []

        for key, st in step_map.items():
            ds_name = key[0]
            if not ds_name or ds_name == "_global":
                global_steps.append(
                    {
                        "order": st["priority"],
                        "column": st.get("column"),
                        "action": st["action"],
                        "estimated_affected_rows": st.get("estimated_affected_rows"),
                        "note": st.get("note"),
                    }
                )
                continue
            datasets_out.setdefault(ds_name, []).append(st)

        rel_plan = build_relationship_plan(assessment)
        if rules.get("never_drop_rows"):
            for j in rel_plan.get("joins") or []:
                if str(j.get("join_type") or "").lower() == "inner":
                    j["join_type"] = "left"
                    j["note"] = (
                        (j.get("note") or "")
                        + " Upgraded inner→left: never_drop_rows business rule."
                    ).strip()
        for rstep in rel_plan.get("relationship_steps") or []:
            act = str(rstep.get("action") or "")
            if act == "validate_referential_integrity_or_stage":
                ds = rstep.get("dataset") or "_global"
                col = rstep.get("column")
                key = (ds, (col or "*"), act, "relationship")
                if key not in step_map:
                    ri_ev = rstep.get("evidence") or {}
                    step_map[key] = {
                        "dataset": ds,
                        "column": col,
                        "action": act,
                        "severity": "high",
                        "estimated_affected_rows": rstep.get("estimated_affected_rows"),
                        "priority": _ACTION_PRIORITY.get(act, 300),
                        "note": f"FK to {rstep.get('related_dataset')}.{rstep.get('related_column')}",
                        "params": build_ri_step_params(rstep, rules),
                        "evidence": ri_ev,
                        "message": ri_ev.get("why_this_action"),
                    }
                    datasets_out.setdefault(ds, []).append(step_map[key])

        global_steps.sort(key=lambda x: x.get("order") or 0)
        for i, st in enumerate(global_steps, start=1):
            st["order"] = i



        for ds_name, steps in datasets_out.items():
            # Deduplicate steps with identical action and column
            seen_step_keys = set()
            deduped_steps = []
            for st in steps:
                s_key = (st.get("action"), str(st.get("column") or "").lower())
                if s_key not in seen_step_keys:
                    seen_step_keys.add(s_key)
                    deduped_steps.append(st)
            steps = deduped_steps
            steps.sort(key=lambda x: (x["priority"], str(x.get("column") or "")))
            enriched_steps: List[Dict[str, Any]] = []
            for i, st in enumerate(steps, start=1):
                st["order"] = i
                col = st.get("column")
                cstats = _col_stats_for_step(assessment, ds_name, col)
                null_pct = None
                if cstats.get("null_percentage") is not None:
                    try:
                        null_pct = float(cstats["null_percentage"]) * 100.0
                    except (TypeError, ValueError):
                        pass
                res = classify_step_bucket(
                    str(st.get("action") or ""),
                    severity=str(st.get("severity") or "medium"),
                    null_percentage=null_pct,
                    never_drop_rows=bool(rules.get("never_drop_rows")),
                )
                st["bucket"] = res["bucket"]
                st["phase"] = res["phase"]
                enriched_steps.append(st)
            datasets_out[ds_name] = finalize_dataset_steps(enriched_steps, assessment, rules)

        rel_plan = enrich_relationship_plan_joins(rel_plan, rules)

        engine_rec = _recommend_engine(source_context, assessment)
        if rel_plan.get("join_count", 0) > 0 and engine_rec.get("engine") not in ("pyspark", "adf"):
            ds_count = len(datasets_known)
            if ds_count > 1:
                engine_rec = dict(engine_rec)
                engine_rec["engine"] = "pyspark"
                engine_rec["scope"] = "plan"
                engine_rec["dataset_count"] = ds_count
                engine_rec["relationship_count"] = rel_plan.get("join_count", 0)
                engine_rec["reason"] = (
                    f"Multi-dataset ({ds_count} sources, {rel_plan['join_count']} join(s) detected) — "
                    f"PySpark required for distributed join partitioning."
                )

        # Append DQ warning blocker if failed
        if not dq_gate_summary["passed"]:
            for blocking_ds in dq_gate_summary["blocking_issues"]:
                ds_name = blocking_ds["dataset"]
                score_str = f" ({blocking_ds['score']})" if blocking_ds.get("score") is not None else ""
                manual_review.append(
                    enrich_manual_review_item({
                        "dataset": ds_name,
                        "column": None,
                        "issue_type": "dq_gate_warning",
                        "severity": "high",
                        "message": f"Data quality score{score_str} is below threshold ({threshold}). Phase 2 transformations are blocked for dataset '{ds_name}'. Please resolve errors in Phase 1 (cleanse) first.",
                        "guidance": "Resolve data quality issues or lower/override the threshold in business rules.",
                    })
                )

        # Build coverage report
        def _clean(val: Any, default: str = "") -> str:
            if val is None or str(val).strip() == "":
                return default
            return str(val).strip()

        all_issues = set()
        for s in suggestions:
            ds = _clean(s.get("dataset"), "_global")
            col = _clean(s.get("column"), "*")
            it = _clean(s.get("issue_type"))
            all_issues.add((ds, col, it))

        covered_by_steps = set()
        for ds_name, steps in datasets_out.items():
            for st in steps:
                col = _clean(st.get("column"), "*")
                it = _clean(st.get("source_issue_type"))
                covered_by_steps.add((_clean(ds_name), col, it))

        in_manual = set()
        for m in manual_review:
            ds = _clean(m.get("dataset"), "_global")
            col = _clean(m.get("column"), "*")
            it = _clean(m.get("issue_type"))
            in_manual.add((ds, col, it))

        covered = covered_by_steps | in_manual
        uncovered = all_issues - covered

        tier_counts = {"standard": 0, "complex": 0, "non_fixable": 0}
        for m in manual_review:
            tier = m.get("risk_tier", "standard")
            tier_counts[tier] = tier_counts.get(tier, 0) + 1

        coverage_summary = {
            "total_issues": len(all_issues),
            "covered_by_steps": len(covered_by_steps),
            "manual_review_total": len(in_manual),
            "manual_review_by_tier": tier_counts,
            "uncovered": len(uncovered),
            "uncovered_items": [{"dataset": d, "column": c, "issue_type": i} for d, c, i in uncovered],
        }

        logger.info(
            "Plan coverage: total=%d, steps=%d, manual_total=%d, by_tier=%s, uncovered=%d",
            coverage_summary["total_issues"],
            coverage_summary["covered_by_steps"],
            coverage_summary["manual_review_total"],
            str(coverage_summary["manual_review_by_tier"]),
            coverage_summary["uncovered"],
        )

        plan = {
            "plan_version": 1,
            "plan_id": _plan_id(),
            "engine": (engine or "python").lower(),
            "generation_mode": generation_mode or "full",
            "created_at": time.time(),
            "assessment_signature": _assessment_signature(assessment),
            "business_rules": rules,
            "datasets": {k: {"steps": v} for k, v in datasets_out.items()},
            "global_steps": global_steps,
            "manual_review": manual_review,
            "blocked": blocked,
            "coverage": coverage_summary,
            "invariants": build_plan_invariants(rules),
            "suggestions_summary": sug_pkg.get("summary") or {},
            "engine_recommendation": engine_rec,
            "relationships": rel_plan,
            "semantic_schema": {k: v for k, v in sem_schema.items()},
            "dq_gate_summary": dq_gate_summary,
            "build_warnings": self.build_warnings,
        }
        # Provenance signature block
        prov_data = {
            "business_rules": rules,
            "semantic_schema": sem_schema,
            "assessment_signature": plan.get("assessment_signature")
        }
        try:
            prov_blob = json.dumps(prov_data, sort_keys=True, default=str)
        except Exception:
            prov_blob = str(prov_data)
        plan["provenance_signature"] = "sha256:" + hashlib.sha256(prov_blob.encode("utf-8", errors="ignore")).hexdigest()

        # Calculate dataset-level readiness scores scorecard
        readiness_scorecard = {}
        total_rows = 0
        weighted_score_sum = 0.0
        
        from agent.etl_pipeline.dq_gate import calculate_dataset_dq_score
        for ds_name in datasets_known:
            row_count = float((assessment.get("datasets") or {}).get(ds_name, {}).get("row_count") or 1000)
            score_res = calculate_dataset_dq_score(assessment, ds_name)
            score = round(score_res.get("score") or 0.0, 1)
            
            # Determine readiness status
            has_high_gating = False
            for mr in plan.get("manual_review", []):
                if mr.get("dataset") == ds_name and mr.get("severity") == "high":
                    has_high_gating = True
                    break
            
            if score < 70.0 or has_high_gating:
                readiness = "NEEDS_WORK"
                reason = "Required datasets include gating HIGH failures" if has_high_gating else f"DQ score ({score}) below threshold (70.0)"
            elif score < 90.0:
                readiness = "READY_WITH_WARNINGS"
                reason = "No HIGH findings, but extensive MEDIUM findings"
            else:
                readiness = "READY"
                reason = "Dataset meets quality standards"
                
            readiness_scorecard[ds_name] = {
                "score": score,
                "readiness": readiness,
                "primary_reason": reason,
                "row_count": row_count
            }
            total_rows += row_count
            weighted_score_sum += score * row_count
            
        overall_score = round(weighted_score_sum / max(total_rows, 1), 1)
        any_needs_work = any(card["readiness"] == "NEEDS_WORK" for card in readiness_scorecard.values())
        overall_readiness = "NEEDS_WORK" if any_needs_work else ("READY_WITH_WARNINGS" if any(card["readiness"] == "READY_WITH_WARNINGS" for card in readiness_scorecard.values()) else "READY")
        
        readiness_scorecard["Overall"] = {
            "score": overall_score,
            "readiness": overall_readiness,
            "primary_reason": "Required datasets include gating HIGH failures" if any_needs_work else "All datasets ready or with warnings",
            "row_count": total_rows
        }
        
        plan["readiness_scorecard"] = readiness_scorecard
        
        # Calculate projected column-level readiness certificates and Phase 2 gates
        try:
            from agent.etl_pipeline.readiness_engine import (
                build_projected_column_certificates,
                evaluate_all_operation_gates
            )
            approved_waivers = rules.get("approved_waivers") or []
            projected_certs = build_projected_column_certificates(plan, assessment)
            plan["projected_column_readiness"] = projected_certs
            
            joins_list = joins
            projected_gates = evaluate_all_operation_gates(projected_certs, joins_list, approved_waivers)
            plan["projected_phase2_gates"] = projected_gates
            
            blocked_gates = [g for g in projected_gates if g["gate_status"] == "BLOCKED"]
            plan["overall_gate_status"] = "BLOCKED" if blocked_gates else "READY"
        except Exception as e:
            self.build_warnings.append(f"Failed to calculate projected column readiness/gates: {e}")
        # 4.4 Resolution errors for auto_resolve_pending
        if rules.get("auto_resolve_pending") and plan.get("manual_review"):
            try:
                from agent.etl_pipeline.manual_review_promote import apply_manual_resolutions
                resolutions = []
                for m in plan["manual_review"]:
                    resolutions.append({
                        "item_id": m.get("id"),
                        "resolution_id": m.get("default_resolution")
                    })
                plan, _ = apply_manual_resolutions(plan, resolutions, business_rules=rules, assessment=assessment)
            except Exception as e:
                self.build_warnings.append(f"auto_resolve_pending failed: {e}")

        # Baseline steps for zero-issue columns (B1)
        for ds_name, ds_block in plan.get("datasets", {}).items():
            existing_cols = {s.get("column") for s in (ds_block.get("steps") or []) if s.get("column")}
            cols_info = _dataset_columns(assessment, ds_name)
            for col_name, col_meta in cols_info.items():
                if col_name in existing_cols:
                    continue
                dtype = str(col_meta.get("dtype") or "").lower()
                steps_list = ds_block.setdefault("steps", [])
                
                if any(x in dtype for x in ("varchar","nvarchar","char","text","string")):
                    max_order = max((s.get("order", 0) for s in steps_list), default=0)
                    steps_list.append({
                        "column": col_name,
                        "action": "trim",
                        "auto_fixable": True,
                        "params": {},
                        "evidence": {},
                        "order": max_order + 1,
                        "priority": 5,
                    })
                
                max_order = max((s.get("order", 0) for s in steps_list), default=0)
                steps_list.append({
                    "column": col_name,
                    "action": "cast_type",
                    "auto_fixable": True,
                    "params": {},
                    "evidence": {},
                    "order": max_order + 1,
                    "priority": 35,
                })

        # Consolidate and freeze step specifications (Phase 1 PlanSpec Contract)
        try:
            from agent.etl_pipeline.llm_codegen import _consolidate_and_filter_datasets
            source_metadata_filt = {}
            sem_schema_filt = plan.get("semantic_schema") or {}
            for ds_n, meta in (assessment.get("datasets") or {}).items():
                if not isinstance(meta, dict):
                    continue
                cols = meta.get("columns") or {}
                source_metadata_filt[ds_n] = {
                    "row_count": meta.get("row_count"),
                    "columns": {
                        col: {
                            "dtype": cmeta.get("dtype") or cmeta.get("inferred_type"),
                            "null_percentage": cmeta.get("null_percentage"),
                            "semantic_type": (sem_schema_filt.get(f"{ds_n}.{col}") or {}).get("semantic_type") or cmeta.get("semantic_type", "unknown"),
                            "sub_type": (sem_schema_filt.get(f"{ds_n}.{col}") or {}).get("sub_type") or cmeta.get("sub_type", "unknown"),
                            "pii_level": (sem_schema_filt.get(f"{ds_n}.{col}") or {}).get("pii_level") or "none",
                        }
                        for col, cmeta in cols.items()
                        if isinstance(cmeta, dict)
                    },
                }
            strict = bool(
                plan.get("strict_mode")
                or plan.get("settings", {}).get("strict_mode", False)
                or (plan.get("business_rules") or {}).get("strict_mode", False)
            )
            plan["datasets"] = _consolidate_and_filter_datasets(
                plan.get("datasets") or {}, source_metadata_filt, sem_schema_filt, strict_mode=strict
            )
            from agent.etl_pipeline.manual_review_promote import canonicalize_promoted_plan
            plan = canonicalize_promoted_plan(plan, assessment, rules)
        except Exception as e:
            logger.warning(f"Plan step consolidation/canonicalization during build_etl_plan skipped: {e}")

        return plan



def build_etl_plan(
    assessment: Dict[str, Any],
    business_rules_raw: Any,
    *,
    engine: str = "python",
    source_context: Optional[Dict[str, Any]] = None,
    generation_mode: Optional[str] = "full",
    dq_recommendations: Optional[Dict[str, Any]] = None,
    semantic_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build versioned ETL plan JSON from assessment + normalized business rules.
    """
    with _PLANNER_LOCK:
        builder = PlanBuilder(
            assessment, business_rules_raw,
            engine=engine,
            source_context=source_context,
            generation_mode=generation_mode,
            dq_recommendations=dq_recommendations,
            semantic_context=semantic_context
        )
        return builder.build()


def get_unacknowledged_blockers(plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Returns list of manual_review items that MUST be resolved before ETL generates.
    complex and non_fixable items with status=pending are blockers.
    """
    blockers = []
    for item in (plan.get("manual_review") or []):
        tier = item.get("risk_tier", "standard")
        status = item.get("status", "pending")
        if tier in ("complex", "non_fixable") and status == "pending":
            blockers.append({
                "id": item.get("id"),
                "dataset": item.get("dataset"),
                "column": item.get("column"),
                "issue_type": item.get("issue_type"),
                "risk_tier": tier,
                "message": item.get("message"),
            })
    return blockers
