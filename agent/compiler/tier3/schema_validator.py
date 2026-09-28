from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.compiler.models.profile import DatasetProfile
from agent.compiler.models.tier3_resolution import (
    AstNodeType,
    Tier3ResolutionProposal,
)
from agent.compiler.schema.sql_identifier import parse_table_identifier
from agent.compiler.tier3.validation_result import (
    Tier3ValidationResult,
    ValidationCode,
    ValidationFinding,
    ValidationSeverity,
)


class Tier3SchemaValidator:
    """
    Validates that a Tier 3 proposal conforms to physical dataset profiles:
      1. Dataset identifier canonicalization and existence.
      2. Ambiguity detection for unqualified dataset names.
      3. Target column existence and case-normalization without fuzzy substitution.
      4. Internal compiler column protection (_DharaStagingRowId, etc.).
      5. Recursive AST ColumnRef validation against active schemas.
    """

    INTERNAL_COLUMN_PREFIXES = ("_dhara", "__")
    INTERNAL_COLUMNS = {
        "_dharastagingrowid",
        "_dhara_staging_row_id",
        "__duplicate_rank",
        "__rejection_reason",
        "__quarantine_flag",
    }

    def __init__(self, default_schema: str = "dbo"):
        self.default_schema = default_schema

    def validate(
        self,
        proposal: Tier3ResolutionProposal,
        dataset_profiles: Dict[str, DatasetProfile],
    ) -> Tier3ValidationResult:
        findings: List[ValidationFinding] = []

        if not dataset_profiles:
            findings.append(
                ValidationFinding(
                    validator="SchemaValidator",
                    code=ValidationCode.UNKNOWN_DATASET,
                    severity=ValidationSeverity.ERROR,
                    message=f"No dataset profiles loaded. Cannot validate dataset '{proposal.dataset_name}'.",
                    proposal_id=proposal.proposal_id,
                    dataset=proposal.dataset_name,
                )
            )
            return Tier3ValidationResult(valid=False, findings=findings)

        # 1. Resolve Target Dataset Name
        resolved_ds_key, ds_err = self._resolve_dataset_key(proposal.dataset_name, dataset_profiles)
        if ds_err:
            findings.append(
                ValidationFinding(
                    validator="SchemaValidator",
                    code=ds_err,
                    severity=ValidationSeverity.ERROR,
                    message=f"Dataset '{proposal.dataset_name}' could not be resolved: {ds_err.value}",
                    proposal_id=proposal.proposal_id,
                    dataset=proposal.dataset_name,
                    details={"available_datasets": list(dataset_profiles.keys())},
                )
            )
            return Tier3ValidationResult(valid=False, findings=findings)

        active_profile = dataset_profiles[resolved_ds_key]
        normalized_proposal = copy.deepcopy(proposal)
        normalized_proposal.dataset_name = resolved_ds_key

        # Build column index (canonical name -> lowercase name)
        canonical_cols = set(active_profile.columns.keys())
        col_lower_map = {c.lower(): c for c in canonical_cols}

        # 2. Validate Target Columns
        normalized_target_cols: List[str] = []
        for col_idx, raw_col in enumerate(proposal.target_columns):
            clean_col = str(raw_col).strip().strip("[]")
            clean_col_lower = clean_col.lower()

            # Protected Internal Column Check
            if self._is_internal_column(clean_col_lower):
                findings.append(
                    ValidationFinding(
                        validator="SchemaValidator",
                        code=ValidationCode.INTERNAL_COLUMN_PROTECTED,
                        severity=ValidationSeverity.ERROR,
                        message=f"Target column '{clean_col}' is an internal compiler column and cannot be targeted.",
                        proposal_id=proposal.proposal_id,
                        dataset=resolved_ds_key,
                        column=clean_col,
                        ast_path=f"target_columns[{col_idx}]",
                    )
                )
                continue

            # Column Existence Check
            if clean_col in canonical_cols:
                normalized_target_cols.append(clean_col)
            elif clean_col_lower in col_lower_map:
                # Canonicalize casing without fuzzy matching
                normalized_target_cols.append(col_lower_map[clean_col_lower])
            else:
                findings.append(
                    ValidationFinding(
                        validator="SchemaValidator",
                        code=ValidationCode.UNKNOWN_COLUMN,
                        severity=ValidationSeverity.ERROR,
                        message=f"Target column '{clean_col}' does not exist in dataset '{resolved_ds_key}'.",
                        proposal_id=proposal.proposal_id,
                        dataset=resolved_ds_key,
                        column=clean_col,
                        ast_path=f"target_columns[{col_idx}]",
                        details={"existing_columns": sorted(list(canonical_cols))},
                    )
                )

        normalized_proposal.target_columns = normalized_target_cols

        # 3. Recursively Validate Condition AST Columns
        if proposal.condition_ast:
            ast_copy = copy.deepcopy(proposal.condition_ast)
            ast_findings = self._validate_ast_nodes(
                node=ast_copy,
                current_dataset=resolved_ds_key,
                dataset_profiles=dataset_profiles,
                proposal_id=proposal.proposal_id,
                ast_path="condition_ast",
            )
            findings.extend(ast_findings)
            normalized_proposal.condition_ast = ast_copy

        is_valid = not any(f.severity == ValidationSeverity.ERROR for f in findings)
        return Tier3ValidationResult(
            valid=is_valid,
            findings=findings,
            normalized_proposal=normalized_proposal if is_valid else None,
        )

    def _resolve_dataset_key(
        self,
        raw_name: str,
        dataset_profiles: Dict[str, DatasetProfile],
    ) -> Tuple[Optional[str], Optional[ValidationCode]]:
        """Resolves raw table name into an active dataset profile key with ambiguity checking."""
        if not raw_name or not str(raw_name).strip():
            return None, ValidationCode.UNKNOWN_DATASET

        clean_name = str(raw_name).strip()
        if clean_name in dataset_profiles:
            return clean_name, None

        # Parse identifier
        try:
            ident = parse_table_identifier(clean_name, default_schema=self.default_schema)
        except Exception:
            return None, ValidationCode.UNKNOWN_DATASET

        target_base = ident.table.lower()
        target_schema = ident.schema.lower()

        matches = []
        for profile_key in dataset_profiles.keys():
            try:
                prof_ident = parse_table_identifier(profile_key, default_schema=self.default_schema)
                if prof_ident.table.lower() == target_base:
                    matches.append((profile_key, prof_ident))
            except Exception:
                if profile_key.lower().strip("[]") == clean_name.lower().strip("[]"):
                    matches.append((profile_key, None))

        is_unqualified = "." not in clean_name.strip("[]")

        if len(matches) == 1:
            return matches[0][0], None
        elif len(matches) > 1:
            if is_unqualified:
                return None, ValidationCode.AMBIGUOUS_DATASET
            # Check if schema differentiates
            exact_schema_matches = [
                m[0] for m in matches if m[1] and m[1].schema.lower() == target_schema
            ]
            if len(exact_schema_matches) == 1:
                return exact_schema_matches[0], None
            return None, ValidationCode.AMBIGUOUS_DATASET

        return None, ValidationCode.UNKNOWN_DATASET

    def _validate_ast_nodes(
        self,
        node: Any,
        current_dataset: str,
        dataset_profiles: Dict[str, DatasetProfile],
        proposal_id: str,
        ast_path: str,
    ) -> List[ValidationFinding]:
        findings: List[ValidationFinding] = []

        if not isinstance(node, dict):
            return findings

        node_type = str(node.get("node_type", "")).upper()

        if node_type == AstNodeType.COLUMN:
            raw_ds = node.get("dataset") or current_dataset
            raw_col = node.get("column")
            col_findings = self._validate_single_column_ref(
                raw_ds=raw_ds,
                raw_col=raw_col,
                dataset_profiles=dataset_profiles,
                proposal_id=proposal_id,
                ast_path=ast_path,
                node=node,
            )
            findings.extend(col_findings)

        # Recursively visit children based on AST node type
        elif node_type == AstNodeType.COMPARISON:
            findings.extend(self._validate_ast_nodes(node.get("left"), current_dataset, dataset_profiles, proposal_id, f"{ast_path}.left"))
            findings.extend(self._validate_ast_nodes(node.get("right"), current_dataset, dataset_profiles, proposal_id, f"{ast_path}.right"))
        elif node_type == AstNodeType.NULL_CHECK:
            findings.extend(self._validate_ast_nodes(node.get("column"), current_dataset, dataset_profiles, proposal_id, f"{ast_path}.column"))
        elif node_type == AstNodeType.BOOLEAN:
            for idx, operand in enumerate(node.get("operands") or []):
                findings.extend(self._validate_ast_nodes(operand, current_dataset, dataset_profiles, proposal_id, f"{ast_path}.operands[{idx}]"))
        elif node_type == AstNodeType.IN_LIST:
            findings.extend(self._validate_ast_nodes(node.get("column"), current_dataset, dataset_profiles, proposal_id, f"{ast_path}.column"))
        elif node_type == AstNodeType.BETWEEN:
            findings.extend(self._validate_ast_nodes(node.get("column"), current_dataset, dataset_profiles, proposal_id, f"{ast_path}.column"))
        elif node_type == AstNodeType.LIKE:
            findings.extend(self._validate_ast_nodes(node.get("column"), current_dataset, dataset_profiles, proposal_id, f"{ast_path}.column"))

        return findings

    def _validate_single_column_ref(
        self,
        raw_ds: Any,
        raw_col: Any,
        dataset_profiles: Dict[str, DatasetProfile],
        proposal_id: str,
        ast_path: str,
        node: Dict[str, Any],
    ) -> List[ValidationFinding]:
        findings: List[ValidationFinding] = []

        resolved_ds, ds_err = self._resolve_dataset_key(str(raw_ds or ""), dataset_profiles)
        if ds_err or not resolved_ds:
            findings.append(
                ValidationFinding(
                    validator="SchemaValidator",
                    code=ds_err or ValidationCode.UNKNOWN_DATASET,
                    severity=ValidationSeverity.ERROR,
                    message=f"Dataset reference '{raw_ds}' in AST at '{ast_path}' cannot be resolved.",
                    proposal_id=proposal_id,
                    dataset=str(raw_ds),
                    ast_path=ast_path,
                )
            )
            return findings

        node["dataset"] = resolved_ds
        prof = dataset_profiles[resolved_ds]
        canonical_cols = set(prof.columns.keys())
        col_lower_map = {c.lower(): c for c in canonical_cols}

        clean_col = str(raw_col or "").strip().strip("[]")
        clean_col_lower = clean_col.lower()

        if self._is_internal_column(clean_col_lower):
            findings.append(
                ValidationFinding(
                    validator="SchemaValidator",
                    code=ValidationCode.INTERNAL_COLUMN_PROTECTED,
                    severity=ValidationSeverity.ERROR,
                    message=f"AST column reference '{clean_col}' at '{ast_path}' targets a protected internal column.",
                    proposal_id=proposal_id,
                    dataset=resolved_ds,
                    column=clean_col,
                    ast_path=f"{ast_path}.column",
                )
            )
            return findings

        if clean_col in canonical_cols:
            node["column"] = clean_col
        elif clean_col_lower in col_lower_map:
            node["column"] = col_lower_map[clean_col_lower]
        else:
            findings.append(
                ValidationFinding(
                    validator="SchemaValidator",
                    code=ValidationCode.UNKNOWN_COLUMN,
                    severity=ValidationSeverity.ERROR,
                    message=f"AST column '{clean_col}' at '{ast_path}' does not exist in dataset '{resolved_ds}'.",
                    proposal_id=proposal_id,
                    dataset=resolved_ds,
                    column=clean_col,
                    ast_path=f"{ast_path}.column",
                    details={"existing_columns": sorted(list(canonical_cols))},
                )
            )

        return findings

    def _is_internal_column(self, col_lower: str) -> bool:
        if col_lower in self.INTERNAL_COLUMNS:
            return True
        return any(col_lower.startswith(p) for p in self.INTERNAL_COLUMN_PREFIXES)
