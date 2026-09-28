from __future__ import annotations

import re
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.compiler.models.profile import DatasetProfile
from agent.compiler.models.tier3_resolution import (
    AstNodeType,
    ComparisonOperator,
    Tier3ResolutionProposal,
)
from agent.compiler.tier3.validation_result import (
    Tier3ValidationResult,
    ValidationCode,
    ValidationFinding,
    ValidationSeverity,
)


class TypeFamily(str, Enum):
    STRING = "STRING"
    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    DATETIME = "DATETIME"
    BINARY = "BINARY"
    IDENTIFIER = "IDENTIFIER"
    UNKNOWN = "UNKNOWN"


class CastSafety(str, Enum):
    SAFE = "SAFE"
    CONDITIONAL = "CONDITIONAL"
    LOSSY = "LOSSY"
    UNSUPPORTED = "UNSUPPORTED"


class Tier3TypeValidator:
    """
    Validates datatype compatibility for Tier 3 operators and actions:
      1. Normalizes raw SQL types into semantic TypeFamilies.
      2. Validates AST operators against column and operand type affinities.
      3. Validates action-level datatype requirements (e.g. TRIM on STRING, RANGE on NUMERIC).
      4. Detects and flags lossy casts or incompatible literals.
    """

    # Mapping raw SQL data types to TypeFamily
    TYPE_FAMILY_MAP: Dict[str, TypeFamily] = {
        "varchar": TypeFamily.STRING,
        "nvarchar": TypeFamily.STRING,
        "char": TypeFamily.STRING,
        "nchar": TypeFamily.STRING,
        "text": TypeFamily.STRING,
        "ntext": TypeFamily.STRING,
        "string": TypeFamily.STRING,
        "str": TypeFamily.STRING,
        "tinyint": TypeFamily.INTEGER,
        "smallint": TypeFamily.INTEGER,
        "int": TypeFamily.INTEGER,
        "integer": TypeFamily.INTEGER,
        "bigint": TypeFamily.INTEGER,
        "decimal": TypeFamily.DECIMAL,
        "numeric": TypeFamily.DECIMAL,
        "money": TypeFamily.DECIMAL,
        "smallmoney": TypeFamily.DECIMAL,
        "float": TypeFamily.DECIMAL,
        "real": TypeFamily.DECIMAL,
        "double": TypeFamily.DECIMAL,
        "bit": TypeFamily.BOOLEAN,
        "boolean": TypeFamily.BOOLEAN,
        "bool": TypeFamily.BOOLEAN,
        "date": TypeFamily.DATE,
        "datetime": TypeFamily.DATETIME,
        "datetime2": TypeFamily.DATETIME,
        "smalldatetime": TypeFamily.DATETIME,
        "timestamp": TypeFamily.DATETIME,
        "binary": TypeFamily.BINARY,
        "varbinary": TypeFamily.BINARY,
        "image": TypeFamily.BINARY,
        "uniqueidentifier": TypeFamily.IDENTIFIER,
        "uuid": TypeFamily.IDENTIFIER,
        "guid": TypeFamily.IDENTIFIER,
    }

    # Ordering comparison operators
    ORDERING_OPS: Set[ComparisonOperator] = {
        ComparisonOperator.LT,
        ComparisonOperator.LTE,
        ComparisonOperator.GT,
        ComparisonOperator.GTE,
    }

    # Safe cast matrix: (source_family, target_family) -> CastSafety
    CAST_SAFETY_MATRIX: Dict[Tuple[TypeFamily, TypeFamily], CastSafety] = {
        (TypeFamily.INTEGER, TypeFamily.DECIMAL): CastSafety.SAFE,
        (TypeFamily.INTEGER, TypeFamily.STRING): CastSafety.SAFE,
        (TypeFamily.DECIMAL, TypeFamily.STRING): CastSafety.SAFE,
        (TypeFamily.DATE, TypeFamily.DATETIME): CastSafety.SAFE,
        (TypeFamily.DATE, TypeFamily.STRING): CastSafety.SAFE,
        (TypeFamily.DATETIME, TypeFamily.STRING): CastSafety.SAFE,
        (TypeFamily.BOOLEAN, TypeFamily.STRING): CastSafety.SAFE,
        # Lossy casts (require approval)
        (TypeFamily.DECIMAL, TypeFamily.INTEGER): CastSafety.LOSSY,
        (TypeFamily.DATETIME, TypeFamily.DATE): CastSafety.LOSSY,
        # Conditional casts (format dependent)
        (TypeFamily.STRING, TypeFamily.INTEGER): CastSafety.CONDITIONAL,
        (TypeFamily.STRING, TypeFamily.DECIMAL): CastSafety.CONDITIONAL,
        (TypeFamily.STRING, TypeFamily.DATE): CastSafety.CONDITIONAL,
        (TypeFamily.STRING, TypeFamily.DATETIME): CastSafety.CONDITIONAL,
        (TypeFamily.STRING, TypeFamily.BOOLEAN): CastSafety.CONDITIONAL,
        # Unsupported
        (TypeFamily.BINARY, TypeFamily.INTEGER): CastSafety.UNSUPPORTED,
        (TypeFamily.BINARY, TypeFamily.DATE): CastSafety.UNSUPPORTED,
    }

    def normalize_type_family(self, raw_dtype: Optional[str]) -> TypeFamily:
        if not raw_dtype:
            return TypeFamily.UNKNOWN
        clean = re.sub(r"\(.*\)", "", str(raw_dtype)).strip().lower()
        return self.TYPE_FAMILY_MAP.get(clean, TypeFamily.UNKNOWN)

    def validate(
        self,
        proposal: Tier3ResolutionProposal,
        dataset_profiles: Dict[str, DatasetProfile],
    ) -> Tier3ValidationResult:
        findings: List[ValidationFinding] = []

        profile = dataset_profiles.get(proposal.dataset_name)
        if not profile:
            return Tier3ValidationResult(valid=True, findings=[])

        # 1. Action-level Datatype Validation
        action_findings = self._validate_action_types(proposal, profile)
        findings.extend(action_findings)

        # 2. Condition AST Datatype Validation
        if proposal.condition_ast:
            ast_findings = self._validate_ast_types(
                node=proposal.condition_ast,
                dataset_profiles=dataset_profiles,
                default_dataset=proposal.dataset_name,
                proposal_id=proposal.proposal_id,
                ast_path="condition_ast",
            )
            findings.extend(ast_findings)

        is_valid = not any(f.severity == ValidationSeverity.ERROR for f in findings)
        return Tier3ValidationResult(
            valid=is_valid,
            findings=findings,
            normalized_proposal=proposal if is_valid else None,
        )

    def _validate_action_types(
        self,
        proposal: Tier3ResolutionProposal,
        profile: DatasetProfile,
    ) -> List[ValidationFinding]:
        findings: List[ValidationFinding] = []
        action_type = proposal.proposed_action_type.upper()

        for col_name in proposal.target_columns:
            col_prof = profile.get_column(col_name)
            if not col_prof:
                continue

            family = self.normalize_type_family(col_prof.dtype)

            # String operations require STRING columns
            if action_type in ("TRIM", "NORMALIZE_CASE"):
                if family not in (TypeFamily.STRING, TypeFamily.UNKNOWN):
                    findings.append(
                        ValidationFinding(
                            validator="TypeValidator",
                            code=ValidationCode.ACTION_TYPE_INCOMPATIBLE,
                            severity=ValidationSeverity.ERROR,
                            message=f"Action '{action_type}' requires string column, but '{col_name}' has type '{col_prof.dtype}' ({family.value}).",
                            proposal_id=proposal.proposal_id,
                            dataset=proposal.dataset_name,
                            column=col_name,
                        )
                    )

            # Range validation requires numeric, date, or datetime columns
            elif action_type in ("VALIDATE_RANGE", "RANGE_CLIP"):
                if family not in (TypeFamily.INTEGER, TypeFamily.DECIMAL, TypeFamily.DATE, TypeFamily.DATETIME, TypeFamily.UNKNOWN):
                    findings.append(
                        ValidationFinding(
                            validator="TypeValidator",
                            code=ValidationCode.ACTION_TYPE_INCOMPATIBLE,
                            severity=ValidationSeverity.ERROR,
                            message=f"Action '{action_type}' requires numeric/date column, but '{col_name}' has type '{col_prof.dtype}' ({family.value}).",
                            proposal_id=proposal.proposal_id,
                            dataset=proposal.dataset_name,
                            column=col_name,
                        )
                    )

            # Fill nulls value check
            elif action_type == "FILL_NULLS":
                fill_val = proposal.action_parameters.get("fill_value")
                if fill_val is not None:
                    if family in (TypeFamily.INTEGER, TypeFamily.DECIMAL):
                        if not isinstance(fill_val, (int, float)) and not str(fill_val).replace(".", "", 1).replace("-", "", 1).isdigit():
                            findings.append(
                                ValidationFinding(
                                    validator="TypeValidator",
                                    code=ValidationCode.TYPE_MISMATCH,
                                    severity=ValidationSeverity.ERROR,
                                    message=f"fill_value '{fill_val}' is not numeric for numeric column '{col_name}'.",
                                    proposal_id=proposal.proposal_id,
                                    dataset=proposal.dataset_name,
                                    column=col_name,
                                )
                            )

            # Cast type safety validation
            elif action_type == "CAST_TYPE":
                target_type = proposal.action_parameters.get("target_type")
                if target_type:
                    target_family = self.normalize_type_family(target_type)
                    safety = self.CAST_SAFETY_MATRIX.get((family, target_family), CastSafety.SAFE if family == target_family else CastSafety.CONDITIONAL)
                    if safety == CastSafety.UNSUPPORTED:
                        findings.append(
                            ValidationFinding(
                                validator="TypeValidator",
                                code=ValidationCode.TYPE_MISMATCH,
                                severity=ValidationSeverity.ERROR,
                                message=f"Cast from '{col_prof.dtype}' to '{target_type}' is unsupported.",
                                proposal_id=proposal.proposal_id,
                                dataset=proposal.dataset_name,
                                column=col_name,
                            )
                        )
                    elif safety == CastSafety.LOSSY and not proposal.approval_required:
                        findings.append(
                            ValidationFinding(
                                validator="TypeValidator",
                                code=ValidationCode.LOSSY_CONVERSION_UNAPPROVED,
                                severity=ValidationSeverity.ERROR,
                                message=f"Lossy cast from '{col_prof.dtype}' to '{target_type}' requires approval_required=True.",
                                proposal_id=proposal.proposal_id,
                                dataset=proposal.dataset_name,
                                column=col_name,
                            )
                        )

        return findings

    def _validate_ast_types(
        self,
        node: Any,
        dataset_profiles: Dict[str, DatasetProfile],
        default_dataset: str,
        proposal_id: str,
        ast_path: str,
    ) -> List[ValidationFinding]:
        findings: List[ValidationFinding] = []

        if not isinstance(node, dict):
            return findings

        node_type = str(node.get("node_type", "")).upper()

        if node_type == AstNodeType.COMPARISON:
            op_raw = str(node.get("operator", "")).upper()
            try:
                comp_op = ComparisonOperator(op_raw)
            except Exception:
                return findings

            left_family = self._resolve_node_family(node.get("left"), dataset_profiles, default_dataset)
            right_family = self._resolve_node_family(node.get("right"), dataset_profiles, default_dataset)

            # Ordering operators (LT, LTE, GT, GTE)
            if comp_op in self.ORDERING_OPS:
                numeric_or_date = {TypeFamily.INTEGER, TypeFamily.DECIMAL, TypeFamily.DATE, TypeFamily.DATETIME}
                if left_family not in numeric_or_date and left_family != TypeFamily.UNKNOWN:
                    findings.append(
                        ValidationFinding(
                            validator="TypeValidator",
                            code=ValidationCode.INVALID_OPERATOR_FOR_TYPE,
                            severity=ValidationSeverity.ERROR,
                            message=f"Ordering operator '{comp_op.value}' cannot be applied to '{left_family.value}' at '{ast_path}.left'.",
                            proposal_id=proposal_id,
                            ast_path=f"{ast_path}.operator",
                        )
                    )

            # Equality operand compatibility
            if left_family != TypeFamily.UNKNOWN and right_family != TypeFamily.UNKNOWN:
                if not self._are_families_comparable(left_family, right_family):
                    findings.append(
                        ValidationFinding(
                            validator="TypeValidator",
                            code=ValidationCode.TYPE_MISMATCH,
                            severity=ValidationSeverity.ERROR,
                            message=f"Type mismatch in comparison '{comp_op.value}': left operand is {left_family.value}, right is {right_family.value} at '{ast_path}'.",
                            proposal_id=proposal_id,
                            ast_path=ast_path,
                        )
                    )

        elif node_type == AstNodeType.LIKE:
            col_family = self._resolve_node_family(node.get("column"), dataset_profiles, default_dataset)
            if col_family not in (TypeFamily.STRING, TypeFamily.UNKNOWN):
                findings.append(
                    ValidationFinding(
                        validator="TypeValidator",
                        code=ValidationCode.INVALID_OPERATOR_FOR_TYPE,
                        severity=ValidationSeverity.ERROR,
                        message=f"LIKE operator requires STRING column, but column has type '{col_family.value}' at '{ast_path}'.",
                        proposal_id=proposal_id,
                        ast_path=ast_path,
                    )
                )

        elif node_type == AstNodeType.BETWEEN:
            col_family = self._resolve_node_family(node.get("column"), dataset_profiles, default_dataset)
            lower_fam = self._resolve_node_family(node.get("lower"), dataset_profiles, default_dataset)
            upper_fam = self._resolve_node_family(node.get("upper"), dataset_profiles, default_dataset)

            if col_family not in (TypeFamily.INTEGER, TypeFamily.DECIMAL, TypeFamily.DATE, TypeFamily.DATETIME, TypeFamily.UNKNOWN):
                findings.append(
                    ValidationFinding(
                        validator="TypeValidator",
                        code=ValidationCode.INVALID_OPERATOR_FOR_TYPE,
                        severity=ValidationSeverity.ERROR,
                        message=f"BETWEEN operator requires numeric or date column, got '{col_family.value}' at '{ast_path}'.",
                        proposal_id=proposal_id,
                        ast_path=ast_path,
                    )
                )

            # Validate lower <= upper when literal values are known constants
            low_val = (node.get("lower") or {}).get("value")
            up_val = (node.get("upper") or {}).get("value")
            if isinstance(low_val, (int, float)) and isinstance(up_val, (int, float)):
                if low_val > up_val:
                    findings.append(
                        ValidationFinding(
                            validator="TypeValidator",
                            code=ValidationCode.INVALID_RANGE_BOUNDS,
                            severity=ValidationSeverity.ERROR,
                            message=f"BETWEEN lower bound ({low_val}) cannot exceed upper bound ({up_val}) at '{ast_path}'.",
                            proposal_id=proposal_id,
                            ast_path=ast_path,
                        )
                    )

        elif node_type == AstNodeType.IN_LIST:
            col_family = self._resolve_node_family(node.get("column"), dataset_profiles, default_dataset)
            vals = node.get("values") or []
            if not vals:
                findings.append(
                    ValidationFinding(
                        validator="TypeValidator",
                        code=ValidationCode.EMPTY_VALUE_LIST,
                        severity=ValidationSeverity.ERROR,
                        message=f"IN list cannot be empty at '{ast_path}.values'.",
                        proposal_id=proposal_id,
                        ast_path=f"{ast_path}.values",
                    )
                )
            else:
                for idx, v_node in enumerate(vals):
                    v_fam = self._resolve_node_family(v_node, dataset_profiles, default_dataset)
                    if col_family != TypeFamily.UNKNOWN and v_fam != TypeFamily.UNKNOWN:
                        if not self._are_families_comparable(col_family, v_fam):
                            findings.append(
                                ValidationFinding(
                                    validator="TypeValidator",
                                    code=ValidationCode.TYPE_MISMATCH,
                                    severity=ValidationSeverity.ERROR,
                                    message=f"Literal in IN list at index {idx} has incompatible type '{v_fam.value}' for column '{col_family.value}'.",
                                    proposal_id=proposal_id,
                                    ast_path=f"{ast_path}.values[{idx}]",
                                )
                            )

        elif node_type == AstNodeType.BOOLEAN:
            for idx, child in enumerate(node.get("operands") or []):
                findings.extend(self._validate_ast_types(child, dataset_profiles, default_dataset, proposal_id, f"{ast_path}.operands[{idx}]"))

        return findings

    def _resolve_node_family(
        self,
        node: Any,
        dataset_profiles: Dict[str, DatasetProfile],
        default_dataset: str,
    ) -> TypeFamily:
        if not isinstance(node, dict):
            return TypeFamily.UNKNOWN

        ntype = str(node.get("node_type", "")).upper()
        if ntype == AstNodeType.COLUMN:
            ds = node.get("dataset") or default_dataset
            col = node.get("column")
            prof = dataset_profiles.get(ds)
            if prof and col:
                c_prof = prof.get_column(col)
                if c_prof:
                    return self.normalize_type_family(c_prof.dtype)
            return TypeFamily.UNKNOWN

        elif ntype == AstNodeType.LITERAL:
            dt = node.get("data_type")
            if dt:
                return self.normalize_type_family(dt)
            # Infer from python value
            val = node.get("value")
            if isinstance(val, bool):
                return TypeFamily.BOOLEAN
            if isinstance(val, int):
                return TypeFamily.INTEGER
            if isinstance(val, float):
                return TypeFamily.DECIMAL
            if isinstance(val, str):
                return TypeFamily.STRING

        return TypeFamily.UNKNOWN

    def _are_families_comparable(self, f1: TypeFamily, f2: TypeFamily) -> bool:
        if f1 == f2:
            return True
        # Numbers are comparable
        numeric = {TypeFamily.INTEGER, TypeFamily.DECIMAL}
        if f1 in numeric and f2 in numeric:
            return True
        # Date & Datetime are comparable
        dates = {TypeFamily.DATE, TypeFamily.DATETIME}
        if f1 in dates and f2 in dates:
            return True
        return False
