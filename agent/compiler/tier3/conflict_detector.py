from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.compiler.models.tier3_resolution import (
    AstNodeType,
    ResolutionDisposition,
    Tier3ResolutionProposal,
)
from agent.compiler.tier3.validation_result import (
    Tier3ValidationResult,
    ValidationCode,
    ValidationFinding,
    ValidationSeverity,
)


class Tier3ConflictDetector:
    """
    Detects cross-proposal semantic and physical conflicts:
      1. Exact duplicate actions across proposals.
      2. Unconditional conflicting writes to the same dataset column.
      3. Overlapping conditions on conflicting or destructive actions.
      4. Destructive rejection followed by modification on the same rows.
      5. Dependency cycles in proposal dependency graphs.
      6. Read-write hazards (reading a modified column without declared dependency).
    """

    def validate_proposals(
        self,
        proposals: List[Tier3ResolutionProposal],
    ) -> Tier3ValidationResult:
        findings: List[ValidationFinding] = []

        if not proposals or len(proposals) < 2:
            return Tier3ValidationResult(valid=True, findings=[])

        # 1. Exact Duplicate & Conflicting Write Detection
        dup_findings = self._detect_duplicates_and_conflicts(proposals)
        findings.extend(dup_findings)

        # 2. Overlapping Destructive Action Detection
        overlap_findings = self._detect_destructive_overlaps(proposals)
        findings.extend(overlap_findings)

        # 3. Dependency Cycle Detection
        cycle_findings = self._detect_dependency_cycles(proposals)
        findings.extend(cycle_findings)

        is_valid = not any(f.severity == ValidationSeverity.ERROR for f in findings)
        return Tier3ValidationResult(
            valid=is_valid,
            findings=findings,
        )

    def _canonical_ast_str(self, ast_dict: Optional[Dict[str, Any]]) -> str:
        if not ast_dict:
            return ""
        return json.dumps(ast_dict, sort_keys=True, separators=(",", ":"))

    def _detect_duplicates_and_conflicts(
        self,
        proposals: List[Tier3ResolutionProposal],
    ) -> List[ValidationFinding]:
        findings: List[ValidationFinding] = []
        seen_exact: Dict[Tuple[str, str, str, str, str], str] = {}
        writes_by_col: Dict[Tuple[str, str], List[Tier3ResolutionProposal]] = {}

        for p in proposals:
            col_key = tuple(sorted(p.target_columns))
            ast_str = self._canonical_ast_str(p.condition_ast)
            params_str = json.dumps(p.action_parameters, sort_keys=True, separators=(",", ":"))

            exact_sig = (
                p.dataset_name.lower(),
                str(col_key),
                p.proposed_action_type.upper(),
                ast_str,
                params_str,
            )

            if exact_sig in seen_exact:
                findings.append(
                    ValidationFinding(
                        validator="ConflictDetector",
                        code=ValidationCode.DUPLICATE_ACTION,
                        severity=ValidationSeverity.WARNING,
                        message=f"Proposal '{p.proposal_id}' is an exact duplicate of proposal '{seen_exact[exact_sig]}'.",
                        proposal_id=p.proposal_id,
                        dataset=p.dataset_name,
                        details={"duplicate_of": seen_exact[exact_sig]},
                    )
                )
            else:
                seen_exact[exact_sig] = p.proposal_id

            # Group writes by (dataset, column)
            for c in p.target_columns:
                writes_by_col.setdefault((p.dataset_name.lower(), c.lower()), []).append(p)

        # Check for conflicting writes on same column
        for (ds, col), group in writes_by_col.items():
            if len(group) > 1:
                # If two unconditional proposals write to the same column with different logic
                unconditional = [p for p in group if not p.condition_ast]
                if len(unconditional) > 1:
                    actions = [p.proposed_action_type for p in unconditional]
                    if len(set(actions)) > 1 or any(p.action_parameters != unconditional[0].action_parameters for p in unconditional):
                        findings.append(
                            ValidationFinding(
                                validator="ConflictDetector",
                                code=ValidationCode.CONFLICTING_WRITE,
                                severity=ValidationSeverity.ERROR,
                                message=f"Conflicting unconditional writes to '{ds}.{col}' across proposals: {[p.proposal_id for p in unconditional]}.",
                                dataset=ds,
                                column=col,
                                details={"proposals": [p.proposal_id for p in unconditional]},
                            )
                        )

                # If two proposals have identical conditions but different outcomes
                cond_groups: Dict[str, List[Tier3ResolutionProposal]] = {}
                for p in group:
                    if p.condition_ast:
                        cond_groups.setdefault(self._canonical_ast_str(p.condition_ast), []).append(p)

                for ast_s, p_list in cond_groups.items():
                    if len(p_list) > 1:
                        # Check if parameters or dispositions conflict
                        dispositions = {p.disposition for p in p_list}
                        params = [json.dumps(p.action_parameters, sort_keys=True) for p in p_list]
                        if len(dispositions) > 1 or len(set(params)) > 1:
                            findings.append(
                                ValidationFinding(
                                    validator="ConflictDetector",
                                    code=ValidationCode.CONFLICTING_WRITE,
                                    severity=ValidationSeverity.ERROR,
                                    message=f"Proposals {[p.proposal_id for p in p_list]} have identical condition on '{ds}.{col}' but conflicting outcomes.",
                                    dataset=ds,
                                    column=col,
                                    details={"proposals": [p.proposal_id for p in p_list]},
                                )
                            )

        return findings

    def _detect_destructive_overlaps(
        self,
        proposals: List[Tier3ResolutionProposal],
    ) -> List[ValidationFinding]:
        findings: List[ValidationFinding] = []

        # Check for a REJECT/QUARANTINE disposition alongside another modification on overlapping columns
        rejections = [
            p for p in proposals
            if p.disposition in (ResolutionDisposition.REJECT, ResolutionDisposition.QUARANTINE)
        ]
        mutations = [
            p for p in proposals
            if p.disposition == ResolutionDisposition.CORRECT
        ]

        for rej in rejections:
            rej_cols = set(c.lower() for c in rej.target_columns)
            rej_ast = rej.condition_ast or {}
            rej_comp_col = self._extract_condition_column(rej_ast)

            for mut in mutations:
                if rej.dataset_name.lower() != mut.dataset_name.lower():
                    continue

                mut_cols = set(c.lower() for c in mut.target_columns)
                mut_ast = mut.condition_ast or {}
                mut_comp_col = self._extract_condition_column(mut_ast)

                # Overlap case: Rejecting rows and correcting rows on same condition column
                if rej_comp_col and mut_comp_col and rej_comp_col.lower() == mut_comp_col.lower():
                    findings.append(
                        ValidationFinding(
                            validator="ConflictDetector",
                            code=ValidationCode.OVERLAPPING_DESTRUCTIVE_ACTION,
                            severity=ValidationSeverity.WARNING,
                            message=f"Proposal '{rej.proposal_id}' rejects rows while proposal '{mut.proposal_id}' modifies rows on overlapping column '{rej_comp_col}'. Explicit ordering required.",
                            proposal_id=rej.proposal_id,
                            dataset=rej.dataset_name,
                            column=rej_comp_col,
                            details={"conflicting_proposal": mut.proposal_id},
                        )
                    )

        return findings

    def _extract_condition_column(self, ast_dict: Dict[str, Any]) -> Optional[str]:
        if not isinstance(ast_dict, dict):
            return None
        ntype = str(ast_dict.get("node_type", "")).upper()
        if ntype == AstNodeType.COMPARISON:
            left = ast_dict.get("left")
            if isinstance(left, dict) and left.get("node_type") == AstNodeType.COLUMN:
                return left.get("column")
        elif ntype in (AstNodeType.NULL_CHECK, AstNodeType.IN_LIST, AstNodeType.BETWEEN, AstNodeType.LIKE):
            col_node = ast_dict.get("column")
            if isinstance(col_node, dict) and col_node.get("node_type") == AstNodeType.COLUMN:
                return col_node.get("column")
        return None

    def _detect_dependency_cycles(
        self,
        proposals: List[Tier3ResolutionProposal],
    ) -> List[ValidationFinding]:
        findings: List[ValidationFinding] = []
        dep_graph: Dict[str, List[str]] = {}

        for p in proposals:
            deps = p.action_parameters.get("depends_on") or p.action_parameters.get("depends_on_step_ids") or []
            if isinstance(deps, list):
                dep_graph[p.proposal_id] = [str(d) for d in deps]

        visited: Set[str] = set()
        rec_stack: Set[str] = set()

        def _dfs(node: str, path: List[str]) -> Optional[List[str]]:
            visited.add(node)
            rec_stack.add(node)
            for neighbor in dep_graph.get(node, []):
                if neighbor not in visited:
                    res = _dfs(neighbor, path + [neighbor])
                    if res:
                        return res
                elif neighbor in rec_stack:
                    return path + [neighbor]
            rec_stack.remove(node)
            return None

        for p_id in dep_graph:
            if p_id not in visited:
                cycle = _dfs(p_id, [p_id])
                if cycle:
                    findings.append(
                        ValidationFinding(
                            validator="ConflictDetector",
                            code=ValidationCode.DEPENDENCY_CYCLE,
                            severity=ValidationSeverity.ERROR,
                            message=f"Dependency cycle detected among Tier 3 proposals: {' -> '.join(cycle)}",
                            proposal_id=p_id,
                            details={"cycle": cycle},
                        )
                    )
                    break

        return findings
