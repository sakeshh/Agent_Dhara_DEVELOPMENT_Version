"""
test_etl_codegen_quality.py
Tests verifying ETL code generation governance rules:
- No fake ID generation (mean/median fill for business keys)
- No categorical columns cast to numeric types
- No unsafe dropDuplicates() when no business key
- Date parsing emits is_invalid_ flag columns
- Domain violation flagging for digit-strings in categorical columns
- Readiness % increases after manual review resolutions
- classify_steps correctly marks user-promoted steps as 'auto'
"""
from __future__ import annotations

import pytest
from typing import Any, Dict, List


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_plan(steps_by_ds: Dict[str, List[Dict[str, Any]]], **kwargs) -> Dict[str, Any]:
    datasets = {}
    for ds, steps in steps_by_ds.items():
        datasets[ds] = {"steps": steps}
    return {
        "plan_id": "test_plan",
        "datasets": datasets,
        "business_rules": kwargs.get("business_rules", {}),
        "manual_review": kwargs.get("manual_review", []),
        "resolved_manual_review": kwargs.get("resolved_manual_review", []),
        "connector_manifest": {},
        **{k: v for k, v in kwargs.items() if k not in ("business_rules", "manual_review", "resolved_manual_review")},
    }


# ─── Test 1: Business key columns NEVER filled with mean/median ───────────────

class TestNoFakeIDGeneration:
    def test_sale_id_not_filled_with_mean(self):
        """sale_id must never get F.lit(988.6368) or any computed fill."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "sales.xml": [
                {"order": 1, "column": "sale_id", "action": "fill_or_drop",
                 "params": {"fill_strategy": "mean", "fill_value": 988.6368}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "988.6368" not in code, "Fake ID value 988.6368 must not appear in generated code"
        assert "F.coalesce(F.col('sale_id'), F.lit(988" not in code
        # Should emit a flag column instead
        assert "is_missing_sale_id" in code, "Missing-ID flag column must be emitted"

    def test_campaign_id_not_filled_with_median(self):
        """campaign_id must never get a computed fill value."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "marketing.csv": [
                {"order": 1, "column": "campaign_id", "action": "fill_or_drop",
                 "params": {"fill_strategy": "median", "fill_value": 604.1034}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "604.1034" not in code, "Fake campaign_id value must not appear in generated code"
        assert "is_missing_campaign_id" in code

    def test_factory_id_not_filled_with_mean(self):
        """factory_id must never get a computed fill value."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "factory.json": [
                {"order": 1, "column": "factory_id", "action": "fill_or_drop",
                 "params": {"fill_strategy": "mean", "fill_value": 488.7018}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "488.7018" not in code, "Fake factory_id value must not appear in generated code"
        assert "is_missing_factory_id" in code


# ─── Test 2: Categorical columns NOT cast to long ─────────────────────────────

class TestNoCategoricalCastToNumeric:
    def test_region_not_cast_to_long(self):
        """region is a categorical column \u2014 must not be cast to long."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "sales.xml": [
                {"order": 1, "column": "region", "action": "cast_type",
                 "params": {"target_type": "long"}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert ".cast('long')" not in code or "region" not in code.split(".cast('long')")[0].split("\n")[-1], \
            "region must not be cast to long"
        assert "region_domain_flag" in code, "Domain flag must be emitted for categorical column"

    def test_channel_not_cast_to_long(self):
        """channel is a categorical column \u2014 must not be cast to long."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "marketing.csv": [
                {"order": 1, "column": "channel", "action": "cast_type",
                 "params": {"target_type": "integer"}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "channel_domain_flag" in code, "Domain flag must be emitted for channel column"

    def test_non_categorical_cast_is_allowed(self):
        """Numeric columns like 'qty' CAN be cast to long."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "factory.json": [
                {"order": 1, "column": "qty", "action": "cast_type",
                 "params": {"target_type": "long"}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "cast('long')" in code, "Non-categorical columns should be castable"


# ─── Test 3: Dedup uses row_number, not dropDuplicates ───────────────────────

class TestSafeDeduplication:
    def test_dedup_without_business_key_flags_not_drops(self):
        """When no business key is configured, dedup must flag rows, not drop them."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "sales.xml": [
                {"order": 1, "column": "[Row-level]", "action": "deduplicate", "params": {}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "dropDuplicates()" not in code, "dropDuplicates() must not appear without a business key"
        assert "_is_duplicate" in code, "Duplicate flag column must be emitted"
        assert "manual review required" in code.lower() or "NOT dropped" in code

    def test_dedup_with_business_key_uses_row_number(self):
        """With a business key, dedup uses row_number window."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan(
            {"sales.xml": [
                {"order": 1, "column": "sale_id", "action": "deduplicate", "params": {}},
            ]},
            business_keys={"sales.xml": ["sale_id"]},
        )
        code = generate_pyspark_etl(plan, {})
        assert "row_number()" in code, "Business-key dedup must use row_number()"
        assert "dropDuplicates()" not in code


# ─── Test 4: parse_dates_safe emits flag column ───────────────────────────────

class TestSafeDateParsing:
    def test_parse_dates_safe_emits_flag(self):
        """parse_dates_safe must emit is_invalid_ flag before nullifying bad dates."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "sales.xml": [
                {"order": 1, "column": "date", "action": "parse_dates_safe", "params": {}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "is_invalid_date" in code, "is_invalid_date flag column must be emitted"
        assert "00/00/0000" in code, "Sentinel date must be in the bad-value list"
        assert "to_timestamp" in code, "Timestamp parse must still happen"

    def test_plain_parse_dates_still_works(self):
        """Legacy parse_dates still works (backwards compat)."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "sales.xml": [
                {"order": 1, "column": "created_at", "action": "parse_dates", "params": {}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "to_timestamp" in code


# ─── Test 5: Domain violation flagging ───────────────────────────────────────

class TestDomainViolationFlagging:
    def test_flag_domain_violation_action(self):
        """flag_domain_violation must emit a flag column, not cast to integer."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "marketing.csv": [
                {"order": 1, "column": "region", "action": "flag_domain_violation", "params": {}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "region_domain_flag" in code
        assert ".cast('long')" not in code or "region" not in code


class TestPythonCodegenSafeActions:
    def test_parse_dates_safe_emits_invalid_flag(self):
        from agent.etl_pipeline.python_codegen import generate_python_etl
        from agent.etl_pipeline.validate_python import validate_python_implements_plan

        plan = _make_plan({
            "sales.xml": [
                {"order": 1, "column": "date", "action": "parse_dates_safe", "params": {}},
            ]
        })
        code = generate_python_etl(plan, {})
        assert "is_invalid_date" in code
        assert "pd.to_datetime" in code
        assert validate_python_implements_plan(code, plan) == []

    def test_flag_domain_violation_emits_python_flag(self):
        from agent.etl_pipeline.python_codegen import generate_python_etl

        plan = _make_plan({
            "sales.xml": [
                {"order": 1, "column": "region", "action": "flag_domain_violation", "params": {}},
            ]
        })
        code = generate_python_etl(plan, {})
        assert "region_domain_flag" in code
        assert "fullmatch" in code or "str.fullmatch" in code

    def test_fill_nulls_flag_preserves_original_values(self):
        from agent.etl_pipeline.python_codegen import generate_python_etl

        plan = _make_plan({
            "sales.xml": [
                {"order": 1, "column": "amount", "action": "fill_nulls_flag", "params": {}},
            ]
        })
        code = generate_python_etl(plan, {})
        assert "is_invalid_amount" in code
        assert "amount_numeric" in code
        assert "pd.to_numeric" in code

    def test_replace_sentinel_values_default_params_are_populated(self):
        from agent.etl_pipeline.issue_to_step_compiler import compile_issues_to_steps

        suggestions = [
            {
                "dataset": "sales.xml",
                "column": "Age",
                "issue_type": "sentinel_numeric_value",
                "suggested_action": "replace_sentinel_values",
                "params": {},
            }
        ]
        steps_map, manual = compile_issues_to_steps(suggestions, {}, {})
        step = steps_map["sales.xml"][0]
        assert step["action"] == "replace_sentinel_values"
        assert "sentinel_values" in step["params"]
        assert -999.0 in step["params"]["sentinel_values"]


# ─── Test 6: Readiness % improves after resolution ───────────────────────────────

class TestReadinessPctAfterResolution:
    def _build_plan_with_resolutions(self, n_auto: int, n_total: int, n_resolved: int) -> dict:
        steps = []
        for i in range(n_auto):
            steps.append({"order": i + 1, "column": f"col_{i}", "action": "trim",
                          "bucket": "auto", "classification": "auto"})
        for i in range(n_total - n_auto):
            steps.append({"order": n_auto + i + 1, "column": f"col_r_{i}", "action": "review_manually",
                          "bucket": "review", "classification": "review"})
        resolved = [{"id": f"r_{i}", "dataset": "ds", "column": f"col_r_{i}",
                     "issue_type": "nulls", "status": "resolved"} for i in range(n_resolved)]
        return {
            "plan_id": "t",
            "datasets": {"ds": {"steps": steps}},
            "manual_review": [],
            "resolved_manual_review": resolved,
            "blocked": [],
        }

    def test_readiness_increases_with_resolved_items(self):
        """Resolving manual review items should increase the readiness percentage."""
        # Simulate: 61 auto out of 84 total = 72% before resolution
        plan_before = self._build_plan_with_resolutions(n_auto=61, n_total=84, n_resolved=0)
        plan_after = self._build_plan_with_resolutions(n_auto=61, n_total=84, n_resolved=15)

        # Simulate the frontend pct formula
        def calc_pct(plan: dict) -> int:
            datasets = plan.get("datasets", {})
            total, auto = 0, 0
            for ds in datasets.values():
                for s in ds.get("steps", []):
                    total += 1
                    if (s.get("classification") or s.get("bucket") or "auto").lower() == "auto":
                        auto += 1
            resolved = len(plan.get("resolved_manual_review") or [])
            denom = total + resolved
            return round(((auto + resolved) / denom) * 100) if denom > 0 else 0

        pct_before = calc_pct(plan_before)
        pct_after = calc_pct(plan_after)
        assert pct_after > pct_before, (
            f"Readiness % must increase after resolving items: before={pct_before}%, after={pct_after}%"
        )
        # 61/84 = 72.6% — rounds to 72 in JS Math.round, 73 in Python round(); accept either
        assert pct_before in (72, 73), f"Baseline should be ~72-73%, got {pct_before}%"
        assert pct_after > 75, f"After 15 resolutions, should be >75%, got {pct_after}%"


# ─── Test 7: classify_steps marks user-promoted steps as auto ────────────────

class TestClassifyStepsBuckets:
    def test_user_promoted_step_gets_auto_bucket(self):
        """Steps with evidence.rule_override=True must get bucket='auto'."""
        from agent.etl_pipeline.classify_steps import tag_plan_step_buckets
        plan = {
            "datasets": {
                "sales.xml": {
                    "steps": [
                        {
                            "order": 1, "column": "amount", "action": "fill_or_drop",
                            "severity": "high",
                            "evidence": {"rule_override": True, "confidence": 0.95, "user_resolution": "fill_null"},
                            "source": "manual_review_promote",
                        }
                    ]
                }
            }
        }
        result = tag_plan_step_buckets(plan, {})
        step = result["datasets"]["sales.xml"]["steps"][0]
        assert step["bucket"] == "auto", (
            f"User-promoted step must have bucket='auto', got '{step['bucket']}'"
        )

    def test_non_fixable_resolution_gets_auto_bucket(self):
        """Steps from non_fixable_user_resolution source must get bucket='auto'."""
        from agent.etl_pipeline.classify_steps import tag_plan_step_buckets
        plan = {
            "datasets": {
                "factory.json": {
                    "steps": [
                        {
                            "order": 1, "column": "[Row-level]",
                            "action": "validate_referential_integrity_or_stage",
                            "severity": "high",
                            "source": "non_fixable_user_resolution",
                            "evidence": {},
                        }
                    ]
                }
            }
        }
        result = tag_plan_step_buckets(plan, {})
        step = result["datasets"]["factory.json"]["steps"][0]
        assert step["bucket"] == "auto", (
            f"Non-fixable resolution step must have bucket='auto', got '{step['bucket']}'"
        )

    def test_new_actions_in_auto_set(self):
        """New safe actions (parse_dates_safe, flag_domain_violation, fill_nulls_flag) must be AUTO."""
        from agent.etl_pipeline.classify_steps import classify_step_bucket
        for action in ("parse_dates_safe", "flag_domain_violation", "fill_nulls_flag", "replace_sentinel_values"):
            result = classify_step_bucket(action, severity="medium")
            assert result["bucket"] == "auto", (
                f"Action '{action}' must classify as 'auto', got '{result['bucket']}'"
            )


# ─── Test 8: issue_to_step_compiler mapping fixes ────────────────────────────

class TestIssueCompilerMappings:
    def _get_action(self, issue_type: str) -> str:
        from agent.etl_pipeline.issue_to_step_compiler import _ISSUE_TO_ACTION_MAP
        return _ISSUE_TO_ACTION_MAP.get(issue_type, "noop")

    def test_duplicate_primary_key_routes_to_manual(self):
        """duplicate_primary_key must map to review_manually, never deduplicate."""
        action = self._get_action("duplicate_primary_key")
        assert action == "review_manually", (
            f"duplicate_primary_key must map to review_manually, got '{action}'"
        )

    def test_mixed_case_routes_to_trim(self):
        """mixed_case must map to trim, not lowercase (protects master data)."""
        assert self._get_action("mixed_case") == "trim"
        assert self._get_action("inconsistent_case") == "trim"

    def test_placeholder_routes_to_nullify(self):
        """placeholder_detected must map to nullify_punctuation."""
        assert self._get_action("placeholder_detected") == "nullify_punctuation"

    def test_digit_strings_route_to_flag(self):
        """string_with_only_digits_in_text_column must map to flag_domain_violation."""
        assert self._get_action("string_with_only_digits_in_text_column") == "flag_domain_violation"

    def test_invalid_date_format_routes_to_safe_parse(self):
        """invalid_date_format must map to parse_dates_safe for audit trail."""
        assert self._get_action("invalid_date_format") == "parse_dates_safe"

    def test_duplicate_pk_in_complex_types(self):
        """duplicate_primary_key must be in _COMPLEX_ISSUE_TYPES."""
        from agent.etl_pipeline.issue_to_step_compiler import _COMPLEX_ISSUE_TYPES
        assert "duplicate_primary_key" in _COMPLEX_ISSUE_TYPES

    def test_string_with_digits_in_complex_types(self):
        """string_with_only_digits_in_text_column must be in _COMPLEX_ISSUE_TYPES."""
        from agent.etl_pipeline.issue_to_step_compiler import _COMPLEX_ISSUE_TYPES
        assert "string_with_only_digits_in_text_column" in _COMPLEX_ISSUE_TYPES


# ─── PRODUCTION SAFETY Tests (Bug 1-3 from /learn session) ───────────────────

class TestPySparkProductionSafety:
    """
    Regression tests for 3 production-breaking codegen bugs identified in /learn:
    1. JSON reader must use multiLine=true (array-format JSON files)
    2. avg() / percentile_approx() must NEVER be called on string/categorical columns
    3. percentile_approx must always have .cast('double') for string-stored numeric columns
    """

    def test_json_reader_uses_multiline(self):
        """JSON format reader must always emit .option('multiLine', 'true') to handle array JSON."""
        from agent.etl_pipeline.format_readers import read_json
        snippet = read_json("_resolve_data_path('Files/raw/data.json')")
        assert 'multiLine' in snippet or 'multiline' in snippet, (
            "JSON reader must include multiLine option -- array-format JSON files will produce _corrupt_record without it"
        )
        assert "true" in snippet.lower(), "multiLine option value must be 'true'"

    def test_no_avg_on_string_location_column(self):
        """fill_or_drop with fill_strategy=mean on string column 'location' must NOT emit F.avg(location)."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = {
            "plan_id": "test_no_avg_string",
            "datasets": {
                "factory.json": {
                    "steps": [
                        {
                            "order": 1,
                            "column": "location",
                            "action": "fill_or_drop",
                            "params": {"fill_strategy": "mean"},
                        }
                    ]
                }
            },
            "business_rules": {},
            "manual_review": [],
            "connector_manifest": {},
        }
        code = generate_pyspark_etl(plan, {})
        # avg() or F.avg() must NOT appear on the location column
        assert "avg(location)" not in code, "avg() must never be called on string column 'location'"
        assert "F.avg(F.col('location'))" not in code
        assert "F.expr(\"avg(location)\")" not in code
        # Should use fillna / coalesce with 'unknown' instead
        assert "unknown" in code, "String column fill should use 'unknown' default"

    def test_cast_double_before_percentile_approx_on_string_numeric(self):
        """
        For string-typed production_qty, the template must use fillna('unknown') --
        NOT percentile_approx (which crashes Spark on non-numeric types).
        For a numeric-typed column, percentile_approx must include .cast('double').
        """
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl

        # Case 1: string dtype -- must NOT use percentile_approx, must use fillna('unknown')
        plan_string = {
            "plan_id": "test_cast_percentile_string",
            "datasets": {
                "factory.json": {
                    "steps": [
                        {
                            "order": 1,
                            "column": "production_qty",
                            "action": "fill_or_drop",
                            "params": {"fill_strategy": "median", "dtype": "string"},
                        }
                    ]
                }
            },
            "business_rules": {},
            "manual_review": [],
            "connector_manifest": {},
        }
        code_string = generate_pyspark_etl(plan_string, {})
        assert "percentile_approx(F.col('production_qty'), 0.5)" not in code_string, (
            "percentile_approx without .cast('double') must never appear for string-typed production_qty"
        )
        assert "unknown" in code_string, (
            "String-typed column with fill_strategy=median must fallback to fillna('unknown')"
        )

        # Case 2: double dtype -- must use percentile_approx WITH .cast('double')
        plan_numeric = {
            "plan_id": "test_cast_percentile_numeric",
            "datasets": {
                "factory.json": {
                    "steps": [
                        {
                            "order": 1,
                            "column": "production_qty",
                            "action": "fill_or_drop",
                            "params": {"fill_strategy": "median", "dtype": "double"},
                        }
                    ]
                }
            },
            "business_rules": {},
            "manual_review": [],
            "connector_manifest": {},
        }
        code_numeric = generate_pyspark_etl(plan_numeric, {})
        if "percentile_approx" in code_numeric:
            assert ".cast('double')" in code_numeric, (
                "percentile_approx on numeric column must include .cast('double') safety guard"
            )

    def test_categorical_channel_numeric_fill_sanitization(self):
        """Categorical column 'channel' with fill_value=123 in params must be sanitized to 'unknown'."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = {
            "plan_id": "test_channel_fill",
            "datasets": {
                "marketing.csv": {
                    "steps": [
                        {
                            "order": 1,
                            "column": "channel",
                            "action": "fill_or_drop",
                            "params": {"fill_strategy": "value", "fill_value": 123},
                        }
                    ]
                }
            },
            "business_rules": {},
            "manual_review": [],
            "connector_manifest": {},
        }
        code = generate_pyspark_etl(plan, {})
        assert "lit(123)" not in code, "Categorical column 'channel' must not be filled with numeric literal 123"
        assert "unknown" in code, "Numeric fill value on categorical column 'channel' must be sanitized to 'unknown'"

    def test_action_reflection_audit_for_coerce_numeric_and_cast_type(self):
        """coerce_numeric, cast_type, and parse_dates on categorical/date columns must satisfy reflection audit."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        from agent.etl_pipeline.validate_python import validate_python_implements_plan
        plan = {
            "plan_id": "test_reflection",
            "datasets": {
                "marketing.csv": {
                    "steps": [
                        {"order": 1, "column": "channel", "action": "coerce_numeric"},
                        {"order": 2, "column": "channel", "action": "cast_type", "params": {"target_type": "string"}},
                        {"order": 3, "column": "start_date", "action": "parse_dates"},
                    ]
                }
            },
            "business_rules": {},
            "manual_review": [],
            "connector_manifest": {},
        }
        code = generate_pyspark_etl(plan, {})
        missing = validate_python_implements_plan(code, plan)
        assert len(missing) == 0, f"All plan actions must be reflected in generated code, but missing: {missing}"

    def test_validation_rejects_numeric_aggregation_on_categorical_location(self):
        """validate_pyspark_source must reject avg/mean/percentile_approx on categorical location column."""
        from agent.etl_pipeline.validate_pyspark import validate_pyspark_source
        plan = {
            "plan_id": "test_reject_location_avg",
            "datasets": {
                "factory.json": {
                    "steps": [
                        {"order": 1, "column": "location", "action": "fill_or_drop"}
                    ]
                }
            }
        }
        
        # 1. Invalid code: directly calling F.avg(F.col('location'))
        bad_code = """from pyspark.sql import SparkSession\nfrom pyspark.sql import functions as F\nspark = SparkSession.builder.getOrCreate()\ndef _resolve_data_path(x): return x\ndef transform_factory_json(df, all_dfs=None):\n    _mean = df.select(F.avg(F.col('location'))).first()[0]\n    return df\n"""
        ok, errs = validate_pyspark_source(bad_code, plan)
        assert not ok
        assert any("Numeric aggregation" in e and "location" in e for e in errs)

    def test_validation_requires_cast_for_percentile_approx_on_string_numeric(self):
        """validate_pyspark_source must reject percentile_approx on string numeric column if no double cast is present."""
        from agent.etl_pipeline.validate_pyspark import validate_pyspark_source
        plan = {
            "plan_id": "test_require_cast",
            "datasets": {
                "factory.json": {
                    "steps": [
                        {"order": 1, "column": "production_qty", "action": "fill_or_drop"}
                    ]
                }
            }
        }
        
        # 1. Invalid code: percentile_approx without casting production_qty to double
        bad_code = """from pyspark.sql import SparkSession\nfrom pyspark.sql import functions as F\nspark = SparkSession.builder.getOrCreate()\ndef _resolve_data_path(x): return x\ndef transform_factory_json(df, all_dfs=None):\n    _med = df.select(F.percentile_approx(F.col('production_qty'), 0.5)).first()[0]\n    return df\n"""
        ok, errs = validate_pyspark_source(bad_code, plan)
        assert not ok
        assert any("must cast to double first" in e and "production_qty" in e for e in errs)

        # 2. Valid code: percentile_approx with inline cast to double (might have formatting errors, but must not have cast error)
        good_code = """from pyspark.sql import SparkSession\nfrom pyspark.sql import functions as F\nspark = SparkSession.builder.getOrCreate()\ndef _resolve_data_path(x): return x\ndef transform_factory_json(df, all_dfs=None):\n    _med = df.select(F.percentile_approx(F.col('production_qty').cast('double'), 0.5)).first()[0]\n    return df\n"""
        ok, errs = validate_pyspark_source(good_code, plan)
        assert not any("must cast to double first" in e and "production_qty" in e for e in errs), f"Expected no cast errors but got: {errs}"

    def test_validation_rejects_rlike_on_numeric_column(self):
        """validate_pyspark_source must reject string regex rlike check on a numeric column (like budget)."""
        from agent.etl_pipeline.validate_pyspark import validate_pyspark_source
        plan = {
            "plan_id": "test_reject_rlike_numeric",
            "datasets": {
                "marketing.csv": {
                    "steps": [
                        {"order": 1, "column": "budget", "action": "nullify_non_numeric"}
                    ]
                }
            }
        }
        
        # 1. Invalid code: rlike check on budget column
        bad_code = """from pyspark.sql import SparkSession\nfrom pyspark.sql import functions as F\nspark = SparkSession.builder.getOrCreate()\ndef _resolve_data_path(x): return x\ndef transform_marketing_csv(df, all_dfs=None):\n    df = df.withColumn("budget", F.when(F.col("budget").rlike("^[\\\\W_]+$"), F.lit(None)).otherwise(F.col("budget")))\n    return df\n"""
        ok, errs = validate_pyspark_source(bad_code, plan)
        assert not ok
        assert any("must not be filtered using string rlike" in e and "budget" in e for e in errs)




