from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from agent.compiler.models.tier3_resolution import (
    AstNodeType,
    EvidenceAuthority,
    ResolutionDisposition,
    RiskLevel,
    Tier3ResolutionProposal,
)
from agent.compiler.tier3.validation_result import (
    Tier3ValidationResult,
    ValidationCode,
    ValidationFinding,
    ValidationSeverity,
)


class Tier3SafetyValidator:
    """
    Enforces operational safety bounds and organizational risk policies on Tier 3 proposals:
      1. Structural resource bounds (AST depth, node counts, IN-list sizes, target column counts).
      2. Destructive policy validation (REJECT / QUARANTINE / NULLIFY without authoritative evidence).
      3. Prohibited action and disposition enforcement.
    """

    MAX_AST_DEPTH: int = 6
    MAX_AST_NODES: int = 25
    MAX_IN_LIST_SIZE: int = 50
    MAX_TARGET_COLUMNS: int = 20
    MAX_REFERENCED_DATASETS: int = 3

    DESTRUCTIVE_DISPOSITIONS: Set[ResolutionDisposition] = {
        ResolutionDisposition.REJECT,
        ResolutionDisposition.QUARANTINE,
        ResolutionDisposition.NULLIFY,
    }

    AUTHORITATIVE_AUTHORITIES: Set[EvidenceAuthority] = {
        EvidenceAuthority.DECLARED_SCHEMA,
        EvidenceAuthority.APPROVED_BUSINESS_RULE,
        EvidenceAuthority.APPROVED_CATALOG,
    }

    def validate(self, proposal: Tier3ResolutionProposal) -> Tier3ValidationResult:
        findings: List[ValidationFinding] = []

        # 1. Target Column Limits
        if len(proposal.target_columns) > self.MAX_TARGET_COLUMNS:
            findings.append(
                ValidationFinding(
                    validator="SafetyValidator",
                    code=ValidationCode.EXCESSIVE_TARGET_COLUMNS,
                    severity=ValidationSeverity.ERROR,
                    message=f"Proposal targets {len(proposal.target_columns)} columns, exceeding maximum limit ({self.MAX_TARGET_COLUMNS}).",
                    proposal_id=proposal.proposal_id,
                    dataset=proposal.dataset_name,
                )
            )

        # 2. Risk & Destructive Disposition Policy
        if proposal.risk_level == RiskLevel.PROHIBITED:
            findings.append(
                ValidationFinding(
                    validator="SafetyValidator",
                    code=ValidationCode.PROHIBITED_DISPOSITION,
                    severity=ValidationSeverity.ERROR,
                    message=f"Action '{proposal.proposed_action_type}' has PROHIBITED risk level and cannot be executed.",
                    proposal_id=proposal.proposal_id,
                    dataset=proposal.dataset_name,
                )
            )

        has_authoritative = any(a in self.AUTHORITATIVE_AUTHORITIES for a in proposal.evidence_authorities)
        if proposal.disposition in self.DESTRUCTIVE_DISPOSITIONS and not has_authoritative:
            if not proposal.approval_required:
                findings.append(
                    ValidationFinding(
                        validator="SafetyValidator",
                        code=ValidationCode.UNAUTHORIZED_DESTRUCTIVE_ACTION,
                        severity=ValidationSeverity.ERROR,
                        message=f"Destructive disposition '{proposal.disposition.value}' based solely on inference requires mandatory approval.",
                        proposal_id=proposal.proposal_id,
                        dataset=proposal.dataset_name,
                    )
                )

        # 3. Condition AST Complexity & Resource Bounds
        if proposal.condition_ast:
            depth, node_count, in_sizes, datasets = self._analyze_ast(proposal.condition_ast, 1)

            if depth > self.MAX_AST_DEPTH:
                findings.append(
                    ValidationFinding(
                        validator="SafetyValidator",
                        code=ValidationCode.AST_DEPTH_EXCEEDED,
                        severity=ValidationSeverity.ERROR,
                        message=f"Condition AST depth ({depth}) exceeds safety limit ({self.MAX_AST_DEPTH}).",
                        proposal_id=proposal.proposal_id,
                        dataset=proposal.dataset_name,
                    )
                )

            if node_count > self.MAX_AST_NODES:
                findings.append(
                    ValidationFinding(
                        validator="SafetyValidator",
                        code=ValidationCode.AST_NODE_COUNT_EXCEEDED,
                        severity=ValidationSeverity.ERROR,
                        message=f"Condition AST node count ({node_count}) exceeds safety limit ({self.MAX_AST_NODES}).",
                        proposal_id=proposal.proposal_id,
                        dataset=proposal.dataset_name,
                    )
                )

            for sz, path in in_sizes:
                if sz > self.MAX_IN_LIST_SIZE:
                    findings.append(
                        ValidationFinding(
                            validator="SafetyValidator",
                            code=ValidationCode.IN_LIST_SIZE_EXCEEDED,
                            severity=ValidationSeverity.ERROR,
                            message=f"IN list size ({sz}) exceeds safety limit ({self.MAX_IN_LIST_SIZE}) at '{path}'.",
                            proposal_id=proposal.proposal_id,
                            dataset=proposal.dataset_name,
                            ast_path=path,
                        )
                    )

        is_valid = not any(f.severity == ValidationSeverity.ERROR for f in findings)
        return Tier3ValidationResult(valid=is_valid, findings=findings, normalized_proposal=proposal if is_valid else None)

    def _analyze_ast(
        self,
        node: Any,
        current_depth: int,
        path: str = "condition_ast",
    ) -> tuple[int, int, List[tuple[int, str]], Set[str]]:
        """Returns: (max_depth, total_nodes, in_list_sizes, referenced_datasets)."""
        if not isinstance(node, dict):
            return current_depth, 1, [], set()

        max_depth = current_depth
        total_nodes = 1
        in_sizes: List[tuple[int, str]] = []
        datasets: Set[str] = set()

        node_type = str(node.get("node_type", "")).upper()
        if node_type == AstNodeType.COLUMN:
            if node.get("dataset"):
                datasets.add(str(node["dataset"]).lower())

        elif node_type == AstNodeType.IN_LIST:
            vals = node.get("values") or []
            in_sizes.append((len(vals), f"{path}.values"))

        # Recurse children
        children = []
        if node_type == AstNodeType.COMPARISON:
            children = [("left", node.get("left")), ("right", node.get("right"))]
        elif node_type == AstNodeType.BOOLEAN:
            for idx, child in enumerate(node.get("operands") or []):
                children.append((f"operands[{idx}]", child))
        elif node_type in (AstNodeType.NULL_CHECK, AstNodeType.IN_LIST, AstNodeType.BETWEEN, AstNodeType.LIKE):
            children = [("column", node.get("column"))]

        for child_name, child in children:
            if isinstance(child, dict):
                c_depth, c_nodes, c_in, c_ds = self._analyze_ast(child, current_depth + 1, f"{path}.{child_name}")
                max_depth = max(max_depth, c_depth)
                total_nodes += c_nodes
                in_sizes.extend(c_in)
                datasets.update(c_ds)

        return max_depth, total_nodes, in_sizes, datasets
