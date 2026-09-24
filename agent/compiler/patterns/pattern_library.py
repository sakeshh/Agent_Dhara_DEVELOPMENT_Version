from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from agent.compiler.base.agent import BaseAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType, LogicalStep


@dataclass
class DomainPattern:
    pattern_name: str
    domain: str
    key_column_indicators: Set[str]
    suggested_actions: Dict[str, ActionType]
    description: str


class PatternLibrary(BaseAgent):
    """Catalog of enterprise domain transformation patterns for automated pipeline reuse."""

    PATTERNS: List[DomainPattern] = [
        DomainPattern(
            pattern_name="CustomerMaster",
            domain="CRM",
            key_column_indicators={"customer_id", "email", "phone", "first_name", "last_name"},
            suggested_actions={
                "email": ActionType.LOWERCASE,
                "customer_id": ActionType.DEDUPLICATE,
                "phone": ActionType.MASK_PHONE,
            },
            description="Enterprise Customer Master data cleaning: deduplication, email sanitization, and PII protection.",
        ),
        DomainPattern(
            pattern_name="OrderProcessing",
            domain="E-Commerce / ERP",
            key_column_indicators={"order_id", "customer_id", "order_date", "total_amount", "amount"},
            suggested_actions={
                "order_id": ActionType.DEDUPLICATE,
                "order_date": ActionType.PARSE_DATES,
                "total_amount": ActionType.RANGE_CLIP,
            },
            description="E-Commerce order stream: deduplication, date normalization, and negative amount clipping.",
        ),
        DomainPattern(
            pattern_name="InvoiceValidation",
            domain="Finance / Billing",
            key_column_indicators={"invoice_id", "invoice_date", "due_date", "amount_due", "tax_amount"},
            suggested_actions={
                "invoice_id": ActionType.DEDUPLICATE,
                "invoice_date": ActionType.PARSE_DATES,
                "due_date": ActionType.PARSE_DATES,
            },
            description="Finance invoicing: invoice uniqueness and temporal date alignment.",
        ),
        DomainPattern(
            pattern_name="EmployeeRecords",
            domain="HR",
            key_column_indicators={"employee_id", "hire_date", "salary", "ssn", "department"},
            suggested_actions={
                "employee_id": ActionType.DEDUPLICATE,
                "hire_date": ActionType.PARSE_DATES,
                "ssn": ActionType.MASK_PHONE,
            },
            description="HR Employee master: employee deduplication, hire date parsing, and sensitive identifier masking.",
        ),
    ]

    def match_pattern(self, column_names: Set[str]) -> Optional[tuple[DomainPattern, float]]:
        """Matches a set of columns against domain patterns using token intersection score."""
        cols_lower = {c.lower() for c in column_names}
        best_pattern: Optional[DomainPattern] = None
        best_score = 0.0

        for pattern in self.PATTERNS:
            overlap = cols_lower.intersection(pattern.key_column_indicators)
            score = len(overlap) / len(pattern.key_column_indicators)
            if score > best_score:
                best_score = score
                best_pattern = pattern

        if best_pattern and best_score >= 0.40:
            return best_pattern, round(best_score, 2)
        return None

    def execute(self, context: CompilerContext) -> CompilerContext:
        matched_patterns: List[Dict[str, Any]] = []

        for ds_name, ds_profile in context.dataset_profiles.items():
            cols = set(ds_profile.columns.keys())
            match = self.match_pattern(cols)
            if match:
                pattern, confidence = match
                matched_patterns.append({
                    "dataset": ds_name,
                    "pattern": pattern.pattern_name,
                    "domain": pattern.domain,
                    "confidence": confidence,
                    "description": pattern.description,
                })

                EventBus.get_instance().emit(
                    event_type=CompilerEventType.PATTERN_MATCHED,
                    session_id=context.session_id,
                    payload={"dataset": ds_name, "pattern": pattern.pattern_name, "confidence": confidence},
                    source_agent=self.__class__.__name__,
                )

        context.metadata["matched_domain_patterns"] = matched_patterns
        return context
