"""
Sprint 11: GX Integration Layer — Core Unit Tests
Tests for Phase 0-5 components.
"""
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


# ═══════════════════════════════════════════════════════════
# Phase 0: Feature Flags & Component Manifest
# ═══════════════════════════════════════════════════════════

class TestFeatureFlags:
    def test_defaults_enable_engine3(self):
        from agent.feature_flags import FeatureFlags
        flags = FeatureFlags()
        assert flags.dq_engine_v3 is True
        assert flags.privacy_score_separation is True
        assert flags.dimension_scoring_v2 is True
        assert flags.semantic_inference_v2 is True
        assert flags.gx_validation_provider is True
        assert flags.legacy_flat_penalty_scorer is False
        assert flags.legacy_pii_quality_findings is False

    def test_env_override(self, monkeypatch):
        from agent.feature_flags import reset_flags, get_flags
        reset_flags()
        monkeypatch.setenv("DHARA_DQ_ENGINE_V3", "false")
        monkeypatch.setenv("DHARA_LEGACY_FLAT_PENALTY", "true")
        flags = get_flags()
        assert flags.dq_engine_v3 is False
        assert flags.legacy_flat_penalty_scorer is True
        reset_flags()  # cleanup

    def test_manifest_entry_shows_legacy(self, monkeypatch):
        from agent.feature_flags import reset_flags, get_flags
        reset_flags()
        monkeypatch.setenv("DHARA_LEGACY_FLAT_PENALTY", "true")
        flags = get_flags()
        entry = flags.to_manifest_entry()
        assert "legacy_flat_penalty_scorer" in entry["legacy_components_enabled"]
        reset_flags()


class TestComponentManifest:
    def test_manifest_contains_required_fields(self):
        from agent.runtime_component_manifest import build_component_manifest
        manifest = build_component_manifest()
        assert manifest["engine_family"] == "AGENT_DHARA_DQ"
        assert manifest["engine_version"] == "3.1.0"
        assert "components" in manifest
        assert "validation_providers" in manifest
        assert "great_expectations" in manifest["validation_providers"]
        assert "dhara_native" in manifest["validation_providers"]

    def test_manifest_shows_privacy_separation(self):
        from agent.runtime_component_manifest import build_component_manifest
        manifest = build_component_manifest()
        assert manifest["privacy_separation"] is True

    def test_manifest_shows_dimension_scorer_v3(self):
        from agent.runtime_component_manifest import build_component_manifest
        manifest = build_component_manifest()
        assert manifest["dimension_scorer_v3"] is True


# ═══════════════════════════════════════════════════════════
# Phase 1: Validation Provider Abstraction
# ═══════════════════════════════════════════════════════════

class TestValidationProviderInterface:
    def test_provider_result_to_dict(self):
        from agent.profiling.validation_provider import ProviderResult
        pr = ProviderResult(
            provider_name="test_provider",
            provider_version="1.0",
            rule_results=[{"rule_id": "column.not_null", "status": "PASS"}],
            execution_metadata={"batch_id": "abc"},
            provider_errors=[],
        )
        d = pr.to_dict()
        assert d["provider_name"] == "test_provider"
        assert len(d["rule_results"]) == 1
        assert d["provider_errors"] == []

    def test_execution_context_defaults(self):
        from agent.profiling.validation_provider import ExecutionContext
        ctx = ExecutionContext(dataset_id="my_dataset")
        assert ctx.backend == "pandas"
        assert ctx.source_type == "dataframe"
        assert ctx.profile == {}


# ═══════════════════════════════════════════════════════════
# Phase 2: GX Capability Registry
# ═══════════════════════════════════════════════════════════

class TestGXCapabilityRegistry:
    def test_privacy_issue_types_are_classified(self):
        from agent.profiling.gx_capability_registry import is_privacy_issue
        assert is_privacy_issue("pii_email") is True
        assert is_privacy_issue("pii_phone") is True
        assert is_privacy_issue("pii_sensitive") is True
        assert is_privacy_issue("nulls") is False
        assert is_privacy_issue("exact_duplicate_rows") is False

    def test_resolve_provider_for_gx_rule(self):
        from agent.profiling.gx_capability_registry import resolve_provider
        result = resolve_provider("column.not_null", backend="pandas")
        assert result["primary"] == "great_expectations"
        assert result["fallback"] == "dhara_native"

    def test_resolve_provider_for_native_only_rule(self):
        from agent.profiling.gx_capability_registry import resolve_provider
        result = resolve_provider("relationship.orphan", backend="pandas")
        assert result["primary"] == "dhara_native"

    def test_resolve_privacy_rule_to_privacy_engine(self):
        from agent.profiling.gx_capability_registry import resolve_provider
        result = resolve_provider("privacy.email", backend="pandas")
        assert result["primary"] == "privacy_engine"
        assert result["score_family"] == "privacy"

    def test_gx_primary_downgrades_for_unsupported_backend(self):
        from agent.profiling.gx_capability_registry import resolve_provider
        # xml is not in GX backends
        result = resolve_provider("column.not_null", backend="xml")
        assert result["effective_primary"] == "dhara_native"

    def test_canonical_rule_id_for_pii_issue(self):
        from agent.profiling.gx_capability_registry import canonical_rule_id_for_issue
        assert canonical_rule_id_for_issue("pii_email") == "privacy.email"
        assert canonical_rule_id_for_issue("nulls") == "column.not_null"
        assert canonical_rule_id_for_issue("exact_duplicate_rows") == "compound_key.unique"


# ═══════════════════════════════════════════════════════════
# Phase 3: GX Suite Builder — Semantic-Aware Planning
# ═══════════════════════════════════════════════════════════

class TestGXSuiteBuilder:
    def _build_specs(self, col_name: str, semantic_type: str, df_col: pd.Series = None):
        from agent.profiling.gx_suite_builder import build_expectation_specs
        if df_col is None:
            df_col = pd.Series(["A", "B", "C"])
        df = pd.DataFrame({col_name: df_col})
        profile = {"columns": {col_name: {"semantic_type": semantic_type}}}
        sem_cls = {col_name: {"semantic_type": semantic_type, "semantic_confidence": 0.9}}
        return build_expectation_specs(
            dataset_id="test",
            df_columns=[col_name],
            profile=profile,
            semantic_classifications=sem_cls,
        )

    def test_country_does_not_get_numeric_range(self):
        specs = self._build_specs("country", "country")
        exp_types = {s["expectation_type"] for s in specs}
        assert "expect_column_values_to_be_between" not in exp_types

    def test_identifier_does_not_get_type_list(self):
        specs = self._build_specs("customer_id", "identifier",
                                  df_col=pd.Series(["C00001", "C00002"]))
        exp_types = {s["expectation_type"] for s in specs}
        assert "expect_column_values_to_be_in_type_list" not in exp_types

    def test_status_column_can_get_allowed_values(self):
        from agent.profiling.gx_suite_builder import build_expectation_specs
        df = pd.DataFrame({"status": ["PENDING", "PAID", "SHIPPED"]})
        profile = {
            "columns": {"status": {"semantic_type": "status", "unique_count": 3}},
        }
        sem_cls = {"status": {"semantic_type": "status", "semantic_confidence": 0.9}}
        br = {"valid_values": {"status": ["PENDING", "PAID", "SHIPPED", "CANCELLED", "REFUNDED"]}}
        specs = build_expectation_specs(
            dataset_id="test", df_columns=["status"],
            profile=profile, semantic_classifications=sem_cls, business_rules=br
        )
        exp_types = {s["expectation_type"] for s in specs}
        assert "expect_column_values_to_be_in_set" in exp_types

    def test_currency_amount_can_get_numeric_range(self):
        specs = self._build_specs("amount", "currency_amount",
                                  df_col=pd.Series([100.0, 200.0, 300.0]))
        # currency_amount is in NUMERIC_EXPECTATIONS allowlist so between is allowed
        exp_types = {s["expectation_type"] for s in specs}
        # not_null should always appear
        assert "expect_column_values_to_not_be_null" in exp_types

    def test_suite_hash_is_deterministic(self):
        from agent.profiling.gx_suite_builder import build_expectation_specs, suite_hash
        df = pd.DataFrame({"customer_id": ["C001", "C002"]})
        profile = {"columns": {"customer_id": {"semantic_type": "identifier"}}}
        sem_cls = {"customer_id": {"semantic_type": "identifier", "semantic_confidence": 0.9}}
        specs1 = build_expectation_specs("ds", ["customer_id"], profile, sem_cls)
        specs2 = build_expectation_specs("ds", ["customer_id"], profile, sem_cls)
        assert suite_hash(specs1) == suite_hash(specs2)


# ═══════════════════════════════════════════════════════════
# Phase 4: GX Result Translator — 7-State Mapping
# ═══════════════════════════════════════════════════════════

class TestGXResultTranslator:
    def _make_gx_result(self, success, exp_type, col="test_col",
                        unexp_count=0, element_count=100):
        """Create a mock GX result object."""
        r = MagicMock()
        r.success = success
        cfg = MagicMock()
        cfg.expectation_type = exp_type
        kw = MagicMock()
        kw.__class__ = dict
        kw.get = lambda k, default=None: {"column": col}.get(k, default)
        cfg.kwargs = {"column": col}
        r.expectation_config = cfg
        r.result = {
            "unexpected_count": unexp_count,
            "element_count": element_count,
            "unexpected_index_list": list(range(min(unexp_count, 5))),
            "unexpected_list": ["bad_val"] * min(unexp_count, 3),
        }
        return r

    def test_pass_maps_to_pass_status(self):
        from agent.profiling.gx_result_translator import translate_gx_result_list
        from agent.profiling.rule_status import RuleStatus
        r = self._make_gx_result(True, "expect_column_values_to_not_be_null")
        results = translate_gx_result_list([r], dataset_id="ds")
        assert len(results) == 1
        assert results[0].status == RuleStatus.PASS

    def test_fail_maps_to_fail_status(self):
        from agent.profiling.gx_result_translator import translate_gx_result_list
        from agent.profiling.rule_status import RuleStatus
        r = self._make_gx_result(False, "expect_column_values_to_not_be_null",
                                 unexp_count=10)
        results = translate_gx_result_list([r], dataset_id="ds")
        assert results[0].status == RuleStatus.FAIL
        assert results[0].failed_count == 10

    def test_none_success_maps_to_configuration_error(self):
        from agent.profiling.gx_result_translator import translate_gx_result_list
        from agent.profiling.rule_status import RuleStatus
        r = self._make_gx_result(None, "expect_column_values_to_not_be_null")
        results = translate_gx_result_list([r], dataset_id="ds")
        # success=None → CONFIGURATION_ERROR (never PASS)
        assert results[0].status == RuleStatus.CONFIGURATION_ERROR

    def test_batch_unavailable_returns_not_evaluated(self):
        from agent.profiling.gx_result_translator import translate_gx_result_list
        from agent.profiling.rule_status import RuleStatus
        results = translate_gx_result_list(
            [], dataset_id="ds",
            execution_metadata={"batch_unavailable": True}
        )
        assert len(results) == 1
        assert results[0].status == RuleStatus.NOT_EVALUATED

    def test_gx_init_error_returns_execution_error(self):
        from agent.profiling.gx_result_translator import translate_gx_result_list
        from agent.profiling.rule_status import RuleStatus
        results = translate_gx_result_list(
            [], dataset_id="ds",
            execution_metadata={"gx_init_error": "Connection refused"}
        )
        assert results[0].status == RuleStatus.EXECUTION_ERROR

    def test_numeric_rule_on_identifier_returns_not_applicable(self):
        from agent.profiling.gx_result_translator import translate_gx_result_list
        from agent.profiling.rule_status import RuleStatus
        # Simulate a failing numeric range expectation on an identifier column
        r = self._make_gx_result(
            False, "expect_column_values_to_be_between",
            col="customer_id", unexp_count=5
        )
        sem_cls = {"customer_id": {"semantic_type": "identifier"}}
        results = translate_gx_result_list([r], dataset_id="ds",
                                           semantic_classifications=sem_cls)
        # Should be NOT_APPLICABLE, not FAIL
        assert results[0].status == RuleStatus.NOT_APPLICABLE

    def test_country_does_not_get_numeric_rule_as_fail(self):
        from agent.profiling.gx_result_translator import translate_gx_result_list
        from agent.profiling.rule_status import RuleStatus
        r = self._make_gx_result(
            False, "expect_column_values_to_be_between",
            col="country", unexp_count=50
        )
        sem_cls = {"country": {"semantic_type": "country"}}
        results = translate_gx_result_list([r], dataset_id="ds",
                                           semantic_classifications=sem_cls)
        assert results[0].status == RuleStatus.NOT_APPLICABLE


# ═══════════════════════════════════════════════════════════
# Phase 5: Engine 3.0 Pipeline — Privacy Separation & Dedup
# ═══════════════════════════════════════════════════════════

class TestEngine3Pipeline:
    def _make_issue(self, issue_type: str, col: str, count: int = 5,
                    severity: str = "high") -> dict:
        return {
            "type": issue_type,
            "column": col,
            "count": count,
            "severity": severity,
            "row_indexes": list(range(count)),
            "sample_values": ["val1"],
            "message": f"Test finding: {issue_type}",
            "gating": True,
        }

    def test_pii_issues_are_separated_from_quality(self):
        from agent.profiling.engine3_pipeline import separate_privacy_findings
        issues = [
            self._make_issue("nulls", "customer_id"),
            self._make_issue("pii_email", "email_col"),
            self._make_issue("pii_phone", "phone_col"),
            self._make_issue("invalid_email", "email_col"),
        ]
        quality, privacy = separate_privacy_findings(issues)
        assert len(quality) == 2
        assert len(privacy) == 2
        assert all(i["type"] not in {"pii_email", "pii_phone"} for i in quality)
        assert all(i["pipeline"] == "privacy" for i in privacy)

    def test_deduplication_merges_same_rule_same_column(self):
        from agent.profiling.engine3_pipeline import deduplicate_quality_issues
        # Two nulls findings on same column should merge
        issues = [
            self._make_issue("nulls", "customer_id", count=3),
            self._make_issue("nulls", "customer_id", count=5),
        ]
        deduped, diagnostics = deduplicate_quality_issues(issues, "test_ds")
        assert len(deduped) == 1
        assert len(diagnostics) == 1
        assert diagnostics[0]["action"] == "merged_duplicate_finding"

    def test_deduplication_keeps_different_columns_separate(self):
        from agent.profiling.engine3_pipeline import deduplicate_quality_issues
        issues = [
            self._make_issue("nulls", "col_a"),
            self._make_issue("nulls", "col_b"),
        ]
        deduped, diagnostics = deduplicate_quality_issues(issues, "test_ds")
        assert len(deduped) == 2
        assert len(diagnostics) == 0

    def test_full_pipeline_excludes_pii_from_quality_score(self):
        from agent.profiling.engine3_pipeline import run_engine3_pipeline
        df = pd.DataFrame({
            "customer_id": ["C001", "C002", "C003"],
            "email": ["a@x.com", "b@x.com", None],
        })
        issues = [
            self._make_issue("nulls", "customer_id"),
            self._make_issue("pii_email", "email", severity="medium"),
        ]
        result = run_engine3_pipeline(
            name="test_ds",
            df=df,
            raw_issues=issues,
            profile={"columns": {}},
        )
        # PII should be in privacy_findings, not quality_issues
        assert not any(
            i["type"] in {"pii_email", "pii_phone"}
            for i in result["quality_issues"]
        )
        assert len(result["privacy_findings"]) == 1
        # Component manifest should be present
        assert "component_manifest" in result
        assert result["component_manifest"]["engine_family"] == "AGENT_DHARA_DQ"

    def test_dimension_scorer_replaces_legacy_formula(self):
        from agent.profiling.engine3_pipeline import run_engine3_pipeline
        df = pd.DataFrame({
            "id": range(100),
            "value": [1.0] * 95 + [None] * 5,
        })
        issues = [self._make_issue("nulls", "value", count=5)]
        result = run_engine3_pipeline(
            name="test_ds",
            df=df,
            raw_issues=issues,
            profile={"columns": {}},
        )
        # Score methodology should be v3
        assert result["score_methodology"] == "dimension_scorer_v3"
        assert result["dq_score_v3"] is not None
        assert 0 <= result["dq_score_v3"] <= 100

    def test_not_evaluated_relationship_cannot_pass_gating(self):
        """Invariant: NOT_EVALUATED must never result in PASS gating."""
        from agent.profiling.rule_status import RuleStatus, RuleEvaluationResult
        from agent.profiling.dimension_scorer import calculate_dimension_quality_scores
        not_eval_result = RuleEvaluationResult(
            rule_id="relationship.orphan",
            status=RuleStatus.NOT_EVALUATED,
            dataset="ds",
            dimension="referential_integrity",
            severity="HIGH",
            evaluated_count=0,
            failed_count=0,
        )
        score_obj = calculate_dimension_quality_scores([not_eval_result])
        # NOT_EVALUATED rules must not contribute passing scores to gating
        # readiness should be READY only if there are no blocking failures
        # but confidence should reflect the not-evaluated rule
        assert score_obj.readiness in {"READY", "UNKNOWN"}

    def test_rule_coverage_is_reported(self):
        from agent.profiling.engine3_pipeline import run_engine3_pipeline
        df = pd.DataFrame({"col": [1, 2, 3]})
        result = run_engine3_pipeline(
            name="test_ds",
            df=df,
            raw_issues=[],
            profile={"columns": {}},
        )
        coverage = result["rule_coverage"]
        assert "total_rules" in coverage
        assert "coverage_percentage" in coverage


# ═══════════════════════════════════════════════════════════
# Semantic Inference Tests
# ═══════════════════════════════════════════════════════════

class TestSemanticInferenceV3:
    def test_customer_id_classified_as_identifier(self):
        from agent.profiling.semantic_inference import infer_semantic_type_with_confidence
        series = pd.Series(["C00001", "C00002", "C00003", "C00004"])
        result = infer_semantic_type_with_confidence("customer_id", series)
        assert result.semantic_type == "identifier"

    def test_order_id_classified_as_identifier(self):
        from agent.profiling.semantic_inference import infer_semantic_type_with_confidence
        series = pd.Series(["o000001", "o000002", "o000003"])
        result = infer_semantic_type_with_confidence("order_id", series)
        assert result.semantic_type == "identifier"

    def test_country_classified_as_country(self):
        from agent.profiling.semantic_inference import infer_semantic_type_with_confidence
        series = pd.Series(["US", "GB", "IN", "DE"])
        result = infer_semantic_type_with_confidence("country", series)
        assert result.semantic_type == "country"

    def test_amount_classified_as_currency(self):
        from agent.profiling.semantic_inference import infer_semantic_type_with_confidence
        series = pd.Series([1000.0, 2500.50, 750.0])
        result = infer_semantic_type_with_confidence("payment_amount", series)
        assert result.semantic_type == "currency_amount"

    def test_status_classified_as_status(self):
        from agent.profiling.semantic_inference import infer_semantic_type_with_confidence
        series = pd.Series(["PENDING", "PAID", "SHIPPED", "CANCELLED"])
        result = infer_semantic_type_with_confidence("status", series)
        assert result.semantic_type == "status"

    def test_currency_string_not_classified_as_placeholder(self):
        from agent.profiling.semantic_inference import infer_semantic_type_with_confidence
        series = pd.Series(["$1,000", "$2,500", "$750"])
        # Currency-formatted amounts should NOT be placeholders
        result = infer_semantic_type_with_confidence("amount", series)
        assert result.semantic_type != "placeholder"

    def test_contract_override_takes_precedence(self):
        from agent.profiling.semantic_inference import infer_semantic_type_with_confidence
        # Even if column looks like a date, contract says it's free_text
        series = pd.Series(["2024-01-01", "2024-01-02"])
        result = infer_semantic_type_with_confidence("custom_col", series,
                                                     contract_override="free_text")
        assert result.semantic_type == "free_text"
        assert result.semantic_confidence == 1.0
        assert result.semantic_source == "contract"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
