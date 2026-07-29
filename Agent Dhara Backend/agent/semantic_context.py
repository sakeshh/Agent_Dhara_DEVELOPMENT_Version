"""
Stable contracts for semantic enrichment (Pydantic v2).
Used by semantic_context_builder and downstream report/codegen consumers.
"""
from __future__ import annotations
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

# Legacy models and functions for backward compatibility
class ColumnBusinessTerm(BaseModel):
    column: str
    term: str
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)


class DatasetSemanticContextModel(BaseModel):
    """One dataset's semantic view (mirrors legacy dict shape, validated)."""

    dataset_name: str
    critical_columns: List[str] = Field(default_factory=list)
    likely_key_columns: List[str] = Field(default_factory=list)
    business_terms: Dict[str, str] = Field(default_factory=dict)
    column_importance: Dict[str, str] = Field(default_factory=dict)
    semantic_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    user_notes: str = ""
    prior_report_hints: Dict[str, Any] = Field(default_factory=dict)
    sample_row_count: int = 0
    domain_hints: Dict[str, Any] = Field(default_factory=dict)
    suggested_domain_rules: List[Dict[str, Any]] = Field(default_factory=list)


class SemanticContextPackageModel(BaseModel):
    by_dataset: Dict[str, DatasetSemanticContextModel] = Field(default_factory=dict)
    overall_semantic_confidence: float = Field(default=0.0, ge=0.0, le=1.0)


def semantic_package_from_legacy(legacy: Dict[str, Any]) -> SemanticContextPackageModel:
    """Parse output of build_all_semantic_contexts (dict form)."""
    by_raw = legacy.get("by_dataset") or {}
    out: Dict[str, DatasetSemanticContextModel] = {}
    if not isinstance(by_raw, dict):
        return SemanticContextPackageModel(
            by_dataset={},
            overall_semantic_confidence=float(legacy.get("overall_semantic_confidence") or 0.0),
        )
    for ds, raw in by_raw.items():
        if not isinstance(raw, dict):
            continue
        out[str(ds)] = DatasetSemanticContextModel(
            dataset_name=str(raw.get("dataset_name") or ds),
            critical_columns=list(raw.get("critical_columns") or []),
            likely_key_columns=list(raw.get("likely_key_columns") or []),
            business_terms=dict(raw.get("business_terms") or {}),
            column_importance=dict(raw.get("column_importance") or {}),
            semantic_confidence=float(raw.get("semantic_confidence") or 0.0),
            user_notes=str(raw.get("user_notes") or ""),
            prior_report_hints=dict(raw.get("prior_report_hints") or {}) if isinstance(raw.get("prior_report_hints"), dict) else {},
            sample_row_count=int(raw.get("sample_row_count") or 0),
            domain_hints=dict(raw.get("domain_hints") or {}) if isinstance(raw.get("domain_hints"), dict) else {},
            suggested_domain_rules=list(raw.get("suggested_domain_rules") or []),
        )
    return SemanticContextPackageModel(
        by_dataset=out,
        overall_semantic_confidence=float(legacy.get("overall_semantic_confidence") or 0.0),
    )


def attach_contract_to_semantic_payload(legacy: Dict[str, Any]) -> Dict[str, Any]:
    """Return legacy dict with validated `contract` key (model_dump)."""
    if not isinstance(legacy, dict):
        return {}
    try:
        legacy = dict(legacy)
        legacy["contract"] = semantic_package_from_legacy(legacy).model_dump()
    except Exception:
        legacy.setdefault("contract", {})
    return legacy


# --- ENHANCED SEMANTIC LAYER CONTRACTS (Component 10) ---

class ColumnRole(str, Enum):
    PRIMARY_KEY = "PRIMARY_KEY"
    FOREIGN_KEY = "FOREIGN_KEY"
    BUSINESS_KEY = "BUSINESS_KEY"
    METRIC = "METRIC"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    PHONE = "PHONE"
    EMAIL = "EMAIL"
    CATEGORICAL = "CATEGORICAL"
    FREE_TEXT = "FREE_TEXT"
    IDENTIFIER = "IDENTIFIER"
    FLAG = "FLAG"


class ColumnCleaningContract(BaseModel):
    column_name: str
    role: ColumnRole
    non_nullable: bool = False
    default_value: Optional[Any] = None
    valid_values: Optional[List[Any]] = None
    valid_range: Optional[Dict[str, Any]] = None  # e.g., {"min": 0, "max": 100}
    format_spec: Optional[str] = None
    quarantine_on_invalid: bool = False


class EntityDefinition(BaseModel):
    name: str
    source_datasets: List[str]
    canonical_key: Optional[str] = None
    dedup_strategy: Optional[str] = "keep_last"
    dedup_tiebreaker: Optional[str] = None
    subject_area: Optional[str] = None
    column_contracts: Dict[str, ColumnCleaningContract] = Field(default_factory=dict)


class RelationshipDefinition(BaseModel):
    from_entity: str
    to_entity: str
    join_keys: Dict[str, str]  # e.g. {"child_col": "parent_col"}
    relationship_type: str  # e.g. "one_to_many"
    enforce_referential_integrity: bool = False


class SemanticCleaningPlan(BaseModel):
    entities: Dict[str, EntityDefinition] = Field(default_factory=dict)
    relationships: List[RelationshipDefinition] = Field(default_factory=list)
    version: str = "1.0.0"
    schema_hash: str = ""

    def get_cleaning_contracts(self, ds_name: str) -> Dict[str, ColumnCleaningContract]:
        for entity in self.entities.values():
            if ds_name in entity.source_datasets:
                return entity.column_contracts
        return {}

    def get_dedup_key(self, ds_name: str) -> Optional[str]:
        for entity in self.entities.values():
            if ds_name in entity.source_datasets:
                return entity.canonical_key
        return None

    def to_tagged_rules(self) -> List[Any]:
        """Convert cleaning plan contracts into TaggedRules with SEMANTIC_LAYER provenance."""
        from agent.etl_pipeline.rule_provenance import TaggedRule, RuleProvenance
        rules = []
        for entity in self.entities.values():
            for ds in entity.source_datasets:
                # 1. Primary key rule if defined
                if entity.canonical_key:
                    rules.append(TaggedRule(
                        dataset=ds,
                        column=entity.canonical_key,
                        issue_type="duplicate_primary_key",
                        action="deduplicate",
                        provenance=RuleProvenance.SEMANTIC_LAYER,
                        source_detail=f"Canonical key for entity {entity.name}"
                    ))
                
                # 2. Field-level contract rules
                for col, contract in entity.column_contracts.items():
                    # Check roles
                    if contract.role == ColumnRole.PRIMARY_KEY and col != entity.canonical_key:
                        rules.append(TaggedRule(
                            dataset=ds,
                            column=col,
                            issue_type="duplicate_primary_key",
                            action="deduplicate",
                            provenance=RuleProvenance.SEMANTIC_LAYER,
                            source_detail="Semantic primary key contract"
                        ))
                    elif contract.role == ColumnRole.PHONE:
                        rules.append(TaggedRule(
                            dataset=ds,
                            column=col,
                            issue_type="mixed_phone_formats",
                            action="normalize_phone",
                            provenance=RuleProvenance.SEMANTIC_LAYER,
                            source_detail="Semantic phone role contract"
                        ))
                    elif contract.role == ColumnRole.EMAIL:
                        rules.append(TaggedRule(
                            dataset=ds,
                            column=col,
                            issue_type="invalid_email",
                            action="normalize_email",
                            provenance=RuleProvenance.SEMANTIC_LAYER,
                            source_detail="Semantic email role contract"
                        ))
                    elif contract.role in (ColumnRole.DATE, ColumnRole.TIMESTAMP):
                        rules.append(TaggedRule(
                            dataset=ds,
                            column=col,
                            issue_type="mixed_date_formats",
                            action="standardize_date",
                            provenance=RuleProvenance.SEMANTIC_LAYER,
                            source_detail="Semantic date/timestamp role contract"
                        ))

                    # Non nullable constraint
                    if contract.non_nullable:
                        action = "quarantine_null" if contract.quarantine_on_invalid else "fill_null"
                        rules.append(TaggedRule(
                            dataset=ds,
                            column=col,
                            issue_type="nulls",
                            action=action,
                            provenance=RuleProvenance.SEMANTIC_LAYER,
                            source_detail="Semantic non-nullable contract",
                            metadata={"default_value": contract.default_value}
                        ))

                    # Valid values enum constraint
                    if contract.valid_values:
                        rules.append(TaggedRule(
                            dataset=ds,
                            column=col,
                            issue_type="invalid_lookup_value",
                            action="quarantine_invalid_value",
                            provenance=RuleProvenance.SEMANTIC_LAYER,
                            source_detail="Semantic allowed values contract",
                            metadata={"valid_values": contract.valid_values}
                        ))
        return rules


class TargetModelHint(BaseModel):
    model_type: str = "flat"  # star, snowflake, flat
    fact_entities: List[str] = Field(default_factory=list)
    dimension_entities: List[str] = Field(default_factory=list)


class EnrichedSemanticModel(BaseModel):
    entities: Dict[str, EntityDefinition] = Field(default_factory=dict)
    relationships: List[RelationshipDefinition] = Field(default_factory=list)
    target_model_hint: TargetModelHint = Field(default_factory=TargetModelHint)
    by_dataset: Dict[str, DatasetSemanticContextModel] = Field(default_factory=dict)
    overall_semantic_confidence: float = Field(default=0.0, ge=0.0, le=1.0)

    def to_tagged_rules(self) -> List[Any]:
        # Outsource to SemanticCleaningPlan helper
        plan = SemanticCleaningPlan(entities=self.entities, relationships=self.relationships)
        return plan.to_tagged_rules()
