from __future__ import annotations

from typing import Callable, Dict, List, Optional
from agent.compiler.models.actions import StructuredAction
from agent.compiler.models.profile import DatasetProfile
from agent.compiler.rules.ambiguity import AmbiguityDetector
from agent.compiler.rules.deterministic_parser import DeterministicParser
from agent.compiler.rules.llm_extractor import LLMExtractor
from agent.compiler.rules.models import DecompositionStatus, RuleDecompositionResult
from agent.compiler.validation.action_validator import ActionValidator


class RuleDecomposer:
    """
    Compiler front-end orchestrator for translating business-rule intent into validated StructuredAction objects.
    Enforces a strict 3-stage resolution pipeline:
      Stage 1: Ambiguity & Incompleteness Pre-Filter
      Stage 2: Tier 1 Deterministic Parser (Zero-LLM Fast Path)
      Stage 3: Tier 2 Structured LLM Extractor (Constrained Schema Fallback)
    """

    def __init__(
        self,
        llm_callable: Optional[Callable[[str], str]] = None,
        promoted_repository: Optional[Any] = None,
    ):
        self.deterministic_parser = DeterministicParser()
        self.llm_extractor = LLMExtractor(llm_callable=llm_callable)
        self.action_validator = ActionValidator()
        self.promoted_repository = promoted_repository

    def decompose(
        self,
        rule_text: str,
        dataset_name: str,
        dataset_profiles: Optional[Dict[str, DatasetProfile]] = None,
    ) -> RuleDecompositionResult:
        if not rule_text or not rule_text.strip():
            return RuleDecompositionResult(
                status=DecompositionStatus.INVALID,
                original_rule=rule_text,
                diagnostics=["Empty business rule text provided."],
                extraction_method="none",
            )

        # 1. Resolve available column names from profile if provided
        available_columns: List[str] = []
        if dataset_profiles and dataset_name in dataset_profiles:
            available_columns = list(dataset_profiles[dataset_name].columns.keys())

        # 2. Stage 1: Check for known ambiguity signals / missing business parameters
        ambiguity_check = AmbiguityDetector.check(rule_text)
        if ambiguity_check:
            status, diagnostics = ambiguity_check
            if status == DecompositionStatus.UNSUPPORTED and self.promoted_repository:
                self.promoted_repository.record_extension_candidate(
                    original_rule=rule_text,
                    dataset_name=dataset_name,
                )
            return RuleDecompositionResult(
                status=status,
                original_rule=rule_text,
                diagnostics=diagnostics,
                extraction_method="none",
                missing_information=diagnostics if status == DecompositionStatus.AMBIGUOUS else [],
            )

        # 2b. Check Promoted Rule Repository (Prior Governed Knowledge)
        if self.promoted_repository:
            schema_prof = dataset_profiles.get(dataset_name) if dataset_profiles else None
            promoted = self.promoted_repository.find_applicable(
                dataset_name=dataset_name,
                rule_text=rule_text,
                schema_profile=schema_prof,
            )
            if promoted:
                act = promoted.to_structured_action()
                val = self.action_validator.validate(act, dataset_profiles=dataset_profiles)
                if val.is_valid:
                    return RuleDecompositionResult(
                        status=DecompositionStatus.RESOLVED,
                        actions=[act],
                        extraction_method="promoted_rule",
                        confidence=1.0,
                        original_rule=rule_text,
                    )

        # 3. Stage 2: Tier 1 Deterministic Parser
        # Fast, conservative, 0 token cost, 0 latency.
        det_result = self.deterministic_parser.parse(
            rule_text=rule_text,
            dataset_name=dataset_name,
            available_columns=available_columns,
        )
        if det_result:
            # If deterministic parser matched, validate actions through ActionValidator
            if det_result.status == DecompositionStatus.RESOLVED and det_result.actions:
                validated_actions: List[StructuredAction] = []
                for act in det_result.actions:
                    val = self.action_validator.validate(act, dataset_profiles=dataset_profiles)
                    if not val.is_valid:
                        errors = [f"{i.issue_code}: {i.message}" for i in val.issues]
                        return RuleDecompositionResult(
                            status=DecompositionStatus.INVALID,
                            original_rule=rule_text,
                            diagnostics=errors,
                            extraction_method="deterministic",
                        )
                    validated_actions.append(act)
                det_result.actions = validated_actions
            return det_result

        # 4. Stage 3: Tier 2 LLM Structured Extractor (Only reached when Tier 1 doesn't match)
        llm_result = self.llm_extractor.extract(
            rule_text=rule_text,
            dataset_name=dataset_name,
            available_columns=available_columns,
        )

        if llm_result.status == DecompositionStatus.UNSUPPORTED and self.promoted_repository:
            self.promoted_repository.record_extension_candidate(
                original_rule=rule_text,
                dataset_name=dataset_name,
            )

        if llm_result.status == DecompositionStatus.RESOLVED and llm_result.actions:
            # Validate LLM output through ActionValidator
            for act in llm_result.actions:
                val = self.action_validator.validate(act, dataset_profiles=dataset_profiles)
                if not val.is_valid:
                    errors = [f"{i.issue_code}: {i.message}" for i in val.issues]
                    return RuleDecompositionResult(
                        status=DecompositionStatus.INVALID,
                        original_rule=rule_text,
                        diagnostics=errors,
                        extraction_method="llm",
                    )

        return llm_result
