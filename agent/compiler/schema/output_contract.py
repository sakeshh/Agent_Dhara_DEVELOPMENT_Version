from __future__ import annotations

from typing import List, Literal, Optional
from pydantic import BaseModel, Field

from agent.compiler.models.cleaning import CleaningMode


class GeneratedColumn(BaseModel):
    """Metadata tracking a column created dynamically during transformation."""
    name: str
    purpose: str
    producer_step_id: str
    destination: Literal["clean", "audit", "rejected", "internal"]
    retained_in_final_output: bool = False
    inferred_dtype: Optional[str] = None


class OutputSchemaContract(BaseModel):
    """
    Universal Output Contract defining strict schema boundaries for clean,
    audit, rejected, and internal columns.
    """
    dataset_id: str
    mode: CleaningMode = CleaningMode.CLEANSE_WITH_AUDIT
    source_columns: List[str] = Field(default_factory=list)
    clean_columns: List[str] = Field(default_factory=list)
    audit_columns: List[str] = Field(default_factory=list)
    rejected_columns: List[str] = Field(default_factory=list)
    internal_columns: List[str] = Field(default_factory=list)
    generated_columns: List[GeneratedColumn] = Field(default_factory=list)
    preserve_source_schema: bool = True
    include_operational_metadata: bool = False

    def is_clean_column(self, col_name: str) -> bool:
        return col_name in self.clean_columns

    def is_audit_column(self, col_name: str) -> bool:
        return col_name in self.audit_columns

    def is_internal_column(self, col_name: str) -> bool:
        return col_name in self.internal_columns

    def validate_clean_projection(self, projection_cols: List[str]) -> tuple[bool, List[str]]:
        """Verifies that no audit or internal columns leak into clean output."""
        violations = []
        if self.mode == CleaningMode.CLEANSE_ONLY:
            for col in projection_cols:
                if col in self.audit_columns:
                    violations.append(f"Audit column '{col}' leaked into clean projection under CLEANSE_ONLY")
                if col in self.internal_columns:
                    violations.append(f"Internal scratch column '{col}' leaked into clean projection")
                if self.preserve_source_schema and col not in self.source_columns:
                    violations.append(f"Non-source column '{col}' present in clean projection with preserve_source_schema=True")
        return (len(violations) == 0, violations)
