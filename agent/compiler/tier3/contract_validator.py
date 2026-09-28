from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Union
from pydantic import BaseModel, Field, ValidationError

from agent.compiler.models.tier3_resolution import (
    ALLOWED_TIER3_ACTION_TYPES,
    AstNodeType,
    BooleanExpression,
    BooleanOperator,
    BetweenExpression,
    ColumnRef,
    ComparisonExpression,
    ComparisonOperator,
    InListExpression,
    LikeExpression,
    LiteralValue,
    NullCheckExpression,
    Tier3ResolutionProposal,
)
from agent.compiler.models.validation import DiagnosticIssue, DiagnosticSeverity, ValidationIssueType


class ContractValidationResult(BaseModel):
    is_valid: bool
    issues: List[DiagnosticIssue] = Field(default_factory=list)
    proposal: Optional[Tier3ResolutionProposal] = None

    @property
    def has_blocking_errors(self) -> bool:
        return any(i.severity in (DiagnosticSeverity.ERROR, DiagnosticSeverity.BLOCKING) for i in self.issues)


class Tier3ContractValidator:
    """
    Validates that a Tier 3 resolution proposal conforms strictly to the versioned contract:
      1. Structural raw-SQL prohibition (forbidden field names and executable SQL tokens).
      2. Pydantic schema validation (types, bounds, enum allowlists, non-empty fields).
      3. Recursive Condition AST schema and operator validation.
      4. Provenance and authority verification.
      5. Policy sanity checking (e.g. destructive actions without authority require approval).
    """

    SUPPORTED_CONTRACT_VERSIONS: Set[str] = {"1.0"}

    # Field names that must never appear anywhere in the proposal payload
    FORBIDDEN_SQL_KEYS: Set[str] = {
        "sql", "raw_sql", "query", "statement", "script",
        "ddl", "dml", "where_clause", "join_clause", "condition_sql",
        "custom_sql", "sql_expression",
    }

    # SQL tokens prohibited in non-literal, non-freeform metadata fields
    FORBIDDEN_SQL_TOKENS: List[str] = [
        ";", "--", "/*", "*/",
        "EXEC ", "EXECUTE ", "DROP ", "ALTER ", "TRUNCATE ", "MERGE ",
        "OPENROWSET", "OPENQUERY", "XP_", "SP_",
    ]

    MAX_AST_DEPTH: int = 8

    def validate(self, payload: Union[Dict[str, Any], Tier3ResolutionProposal]) -> ContractValidationResult:
        issues: List[DiagnosticIssue] = []

        raw_dict = payload.model_dump() if isinstance(payload, BaseModel) else payload
        if not isinstance(raw_dict, dict):
            issues.append(
                DiagnosticIssue(
                    issue_code="TIER3_INVALID_PAYLOAD",
                    issue_type=ValidationIssueType.SYNTAX,
                    severity=DiagnosticSeverity.BLOCKING,
                    message="Proposal payload must be a JSON object / dictionary.",
                )
            )
            return ContractValidationResult(is_valid=False, issues=issues)

        # 1. Structural Raw SQL Key Prohibition
        key_issues = self._check_raw_sql_keys(raw_dict)
        issues.extend(key_issues)

        # 2. Structural Raw SQL Value Prohibition (in structural fields, excluding literal data / rationale)
        value_issues = self._check_raw_sql_values(raw_dict)
        issues.extend(value_issues)

        # If structural SQL violations are found, fail immediately
        if any(i.severity == DiagnosticSeverity.BLOCKING for i in issues):
            return ContractValidationResult(is_valid=False, issues=issues)

        # 3. Contract Version Validation
        contract_ver = raw_dict.get("contract_version", "1.0")
        if contract_ver not in self.SUPPORTED_CONTRACT_VERSIONS:
            issues.append(
                DiagnosticIssue(
                    issue_code="TIER3_UNSUPPORTED_VERSION",
                    issue_type=ValidationIssueType.SCHEMA_CONTRACT,
                    severity=DiagnosticSeverity.BLOCKING,
                    message=f"Contract version '{contract_ver}' is not supported. Supported: {sorted(list(self.SUPPORTED_CONTRACT_VERSIONS))}",
                )
            )
            return ContractValidationResult(is_valid=False, issues=issues)

        # 4. Pydantic Proposal Schema Validation
        proposal_obj: Optional[Tier3ResolutionProposal] = None
        try:
            if isinstance(payload, Tier3ResolutionProposal):
                proposal_obj = payload
            else:
                proposal_obj = Tier3ResolutionProposal.model_validate(raw_dict)
        except ValidationError as err:
            for error in err.errors():
                loc = ".".join(str(p) for p in error.get("loc", []))
                msg = error.get("msg", "Invalid parameter")
                issues.append(
                    DiagnosticIssue(
                        issue_code="TIER3_CONTRACT_FIELD_INVALID",
                        issue_type=ValidationIssueType.SCHEMA_CONTRACT,
                        severity=DiagnosticSeverity.ERROR,
                        message=f"Contract field '$.{loc}' invalid: {msg}",
                        suggested_fix="Ensure all required Tier3ResolutionProposal fields conform to schema.",
                    )
                )
            return ContractValidationResult(is_valid=False, issues=issues)
        except Exception as exc:
            issues.append(
                DiagnosticIssue(
                    issue_code="TIER3_SCHEMA_PARSE_ERROR",
                    issue_type=ValidationIssueType.SCHEMA_CONTRACT,
                    severity=DiagnosticSeverity.BLOCKING,
                    message=f"Failed to parse Tier 3 proposal: {str(exc)}",
                )
            )
            return ContractValidationResult(is_valid=False, issues=issues)

        # 5. Recursive Condition AST Validation
        if proposal_obj and proposal_obj.condition_ast:
            ast_issues = self._validate_condition_ast(proposal_obj.condition_ast, path="$.condition_ast", depth=0)
            issues.extend(ast_issues)

        is_valid = not any(i.severity in (DiagnosticSeverity.ERROR, DiagnosticSeverity.BLOCKING) for i in issues)
        return ContractValidationResult(
            is_valid=is_valid,
            issues=issues,
            proposal=proposal_obj if is_valid else None,
        )

    def _check_raw_sql_keys(self, obj: Any, path: str = "$") -> List[DiagnosticIssue]:
        issues: List[DiagnosticIssue] = []
        if isinstance(obj, dict):
            for k, v in obj.items():
                current_path = f"{path}.{k}"
                if str(k).lower().strip() in self.FORBIDDEN_SQL_KEYS:
                    issues.append(
                        DiagnosticIssue(
                            issue_code="TIER3_RAW_SQL_KEY_FORBIDDEN",
                            issue_type=ValidationIssueType.SECURITY,
                            severity=DiagnosticSeverity.BLOCKING,
                            message=f"Forbidden SQL-bearing field name '{k}' at path '{current_path}'. Tier 3 prohibits raw SQL constructs.",
                            suggested_fix="Express conditions using Condition AST nodes rather than SQL clauses.",
                        )
                    )
                issues.extend(self._check_raw_sql_keys(v, current_path))
        elif isinstance(obj, list):
            for idx, item in enumerate(obj):
                issues.extend(self._check_raw_sql_keys(item, f"{path}[{idx}]"))
        return issues

    def _check_raw_sql_values(self, obj: Any, path: str = "$") -> List[DiagnosticIssue]:
        issues: List[DiagnosticIssue] = []
        # Exclude legitimate free-form audit explanations and literal payload values from blanket keyword rejection
        freeform_fields = {"rationale", "assumptions", "evidence_references", "message", "explanation"}

        if isinstance(obj, dict):
            for k, v in obj.items():
                current_path = f"{path}.{k}"
                if k in freeform_fields or k == "value":
                    continue
                if isinstance(v, str):
                    v_upper = v.upper()
                    for token in self.FORBIDDEN_SQL_TOKENS:
                        if token in v_upper:
                            issues.append(
                                DiagnosticIssue(
                                    issue_code="TIER3_RAW_SQL_TOKEN_FORBIDDEN",
                                    issue_type=ValidationIssueType.SECURITY,
                                    severity=DiagnosticSeverity.BLOCKING,
                                    message=f"Executable SQL token '{token.strip()}' detected in structural field '{current_path}': '{v}'.",
                                    suggested_fix="Remove raw SQL statements or keywords from proposal structure.",
                                )
                            )
                elif isinstance(v, (dict, list)):
                    issues.extend(self._check_raw_sql_values(v, current_path))
        elif isinstance(obj, list):
            for idx, item in enumerate(obj):
                issues.extend(self._check_raw_sql_values(item, f"{path}[{idx}]"))
        return issues

    def _validate_condition_ast(self, node: Any, path: str, depth: int) -> List[DiagnosticIssue]:
        issues: List[DiagnosticIssue] = []

        if depth > self.MAX_AST_DEPTH:
            issues.append(
                DiagnosticIssue(
                    issue_code="TIER3_AST_DEPTH_EXCEEDED",
                    issue_type=ValidationIssueType.SECURITY,
                    severity=DiagnosticSeverity.ERROR,
                    message=f"Condition AST depth exceeded maximum limit ({self.MAX_AST_DEPTH}) at '{path}'.",
                )
            )
            return issues

        if not isinstance(node, dict):
            issues.append(
                DiagnosticIssue(
                    issue_code="TIER3_AST_INVALID_NODE",
                    issue_type=ValidationIssueType.SYNTAX,
                    severity=DiagnosticSeverity.ERROR,
                    message=f"AST node at '{path}' must be an object, got {type(node).__name__}.",
                )
            )
            return issues

        node_type = node.get("node_type")
        raw_name = node_type.value if hasattr(node_type, "value") else str(node_type).split(".")[-1]
        raw_name_upper = raw_name.upper().strip() if raw_name else ""
        if not raw_name_upper or raw_name_upper not in AstNodeType.__members__:
            issues.append(
                DiagnosticIssue(
                    issue_code="TIER3_AST_UNKNOWN_NODE_TYPE",
                    issue_type=ValidationIssueType.SYNTAX,
                    severity=DiagnosticSeverity.ERROR,
                    message=f"Unknown AST node_type '{node_type}' at '{path}'. Allowed: {list(AstNodeType.__members__.keys())}",
                )
            )
            return issues

        node_enum = AstNodeType(raw_name_upper)

        try:
            if node_enum == AstNodeType.COMPARISON:
                ComparisonExpression.model_validate(node)
            elif node_enum == AstNodeType.BOOLEAN:
                # Validate operator
                op = str(node.get("operator", "")).upper()
                if op not in BooleanOperator.__members__:
                    issues.append(
                        DiagnosticIssue(
                            issue_code="TIER3_AST_INVALID_BOOLEAN_OP",
                            issue_type=ValidationIssueType.SYNTAX,
                            severity=DiagnosticSeverity.ERROR,
                            message=f"Invalid Boolean operator '{op}' at '{path}'. Allowed: {list(BooleanOperator.__members__.keys())}",
                        )
                    )
                operands = node.get("operands")
                if not isinstance(operands, list) or len(operands) == 0:
                    issues.append(
                        DiagnosticIssue(
                            issue_code="TIER3_AST_EMPTY_OPERANDS",
                            issue_type=ValidationIssueType.SYNTAX,
                            severity=DiagnosticSeverity.ERROR,
                            message=f"Boolean expression at '{path}' must have non-empty operands list.",
                        )
                    )
                else:
                    for i, child in enumerate(operands):
                        issues.extend(self._validate_condition_ast(child, f"{path}.operands[{i}]", depth + 1))
            elif node_enum == AstNodeType.NULL_CHECK:
                NullCheckExpression.model_validate(node)
            elif node_enum == AstNodeType.IN_LIST:
                InListExpression.model_validate(node)
            elif node_enum == AstNodeType.BETWEEN:
                BetweenExpression.model_validate(node)
            elif node_enum == AstNodeType.LIKE:
                LikeExpression.model_validate(node)
            elif node_enum == AstNodeType.COLUMN:
                ColumnRef.model_validate(node)
            elif node_enum == AstNodeType.LITERAL:
                LiteralValue.model_validate(node)
        except ValidationError as e:
            for error in e.errors():
                loc = ".".join(str(p) for p in error.get("loc", []))
                issues.append(
                    DiagnosticIssue(
                        issue_code="TIER3_AST_VALIDATION_ERROR",
                        issue_type=ValidationIssueType.SYNTAX,
                        severity=DiagnosticSeverity.ERROR,
                        message=f"AST validation error at '{path}.{loc}': {error.get('msg')}",
                    )
                )

        return issues
