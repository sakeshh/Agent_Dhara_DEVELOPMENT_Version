"""
Validation Provider Interface for Agent Dhara.
Defines the abstract contract that all validation backends (GX, native, future) must satisfy.
This prevents GX-specific objects from leaking into scoring, deduplication, or reporting modules.
"""
from __future__ import annotations

import abc
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("agent.validation_provider")


class ExecutionContext:
    """
    Describes the data and execution environment for a validation run.
    """
    def __init__(
        self,
        dataset_id: str,
        backend: str = "pandas",          # pandas | spark | sql
        source_type: str = "dataframe",   # dataframe | sql_table | stream
        profile: Optional[Dict[str, Any]] = None,
        thresholds: Optional[Dict[str, Any]] = None,
        business_rules: Optional[Dict[str, Any]] = None,
        semantic_classifications: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.dataset_id = dataset_id
        self.backend = backend
        self.source_type = source_type
        self.profile = profile or {}
        self.thresholds = thresholds or {}
        self.business_rules = business_rules or {}
        self.semantic_classifications = semantic_classifications or {}


class ProviderResult:
    """
    Provider-agnostic result container.  Never expose provider-specific objects beyond this.
    """
    def __init__(
        self,
        provider_name: str,
        provider_version: str,
        rule_results: List[Dict[str, Any]],
        execution_metadata: Optional[Dict[str, Any]] = None,
        provider_errors: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        self.provider_name = provider_name
        self.provider_version = provider_version
        self.rule_results = rule_results            # List of canonical RuleEvaluationResult dicts
        self.execution_metadata = execution_metadata or {}
        self.provider_errors = provider_errors or []

    def to_dict(self) -> Dict[str, Any]:
        return {
            "provider_name":      self.provider_name,
            "provider_version":   self.provider_version,
            "rule_results":       self.rule_results,
            "execution_metadata": self.execution_metadata,
            "provider_errors":    self.provider_errors,
        }


class ValidationProvider(abc.ABC):
    """
    Abstract base class for all validation providers.
    Implementations: GreatExpectationsProvider, DharaNativeProvider.
    """
    provider_name: str = "abstract"
    provider_version: str = "0.0.0"

    @abc.abstractmethod
    def supports(self, rule_spec: Dict[str, Any], context: ExecutionContext) -> bool:
        """
        Return True if this provider can execute the given rule in the given context.
        """
        ...

    @abc.abstractmethod
    def compile(
        self,
        rule_specs: List[Dict[str, Any]],
        context: ExecutionContext,
    ) -> Any:
        """
        Compile a list of canonical rule specs into a provider-specific plan.
        Returns an opaque compiled plan object understood by execute().
        """
        ...

    @abc.abstractmethod
    def execute(
        self,
        compiled_plan: Any,
        data_asset: Any,
        context: ExecutionContext,
    ) -> ProviderResult:
        """
        Execute the compiled plan against the data asset.
        MUST return a ProviderResult — never raise unhandled exceptions.
        On GX exceptions, return EXECUTION_ERROR status in provider_errors.
        """
        ...

    def health_check(self) -> Dict[str, Any]:
        """
        Optional health check.  Override in implementations that have initialization concerns.
        """
        return {"provider": self.provider_name, "status": "ok"}
