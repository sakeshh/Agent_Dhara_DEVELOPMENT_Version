from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set
from pydantic import BaseModel, Field, ValidationError

from agent.compiler.models.actions import ACTION_PARAMETER_REGISTRY, StructuredAction
from agent.compiler.models.logical import ActionType
from agent.compiler.models.physical import ExecutionEngine
from agent.compiler.models.profile import DatasetProfile
from agent.compiler.models.validation import (
    DiagnosticIssue,
    DiagnosticSeverity,
    ValidationIssueType,
)


class ActionValidationResult(BaseModel):
    """Result of multi-layered validation on a StructuredAction."""
    is_valid: bool
    issues: List[DiagnosticIssue] = Field(default_factory=list)

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity in (DiagnosticSeverity.ERROR, DiagnosticSeverity.BLOCKING))

    @property
    def has_blocking_errors(self) -> bool:
        return any(i.severity == DiagnosticSeverity.BLOCKING for i in self.issues)


class ActionValidator:
    """
    Five-layer validator for StructuredAction contracts before planning:
      Layer 1: Structural Validation (typed parameter schema conformance)
      Layer 2: Schema Validation (dataset and target column existence)
      Layer 3: Type Compatibility (column datatype vs operator affinity)
      Layer 4: Semantic Validation (business logic bounds, e.g. pad length > 0)
      Layer 5: Engine Capability Validation (engine support for requested action)
    """

    STRING_OPS: Set[ActionType] = {
        ActionType.PREFIX_ADD,
        ActionType.SUFFIX_ADD,
        ActionType.PATTERN_REPLACE,
        ActionType.PAD,
        ActionType.TRIM,
        ActionType.LOWERCASE,
        ActionType.UPPERCASE,
        ActionType.SANITIZE_EMAIL,
        ActionType.NORMALIZE_PHONE,
    }

    NUMERIC_OPS: Set[ActionType] = {
        ActionType.COERCE_NUMERIC,
        ActionType.RANGE_CLIP,
        ActionType.CAP_OUTLIERS,
    }

    DATE_OPS: Set[ActionType] = {
        ActionType.PARSE_DATES,
    }

    # Datatypes strictly incompatible with string manipulation without explicit casting
    INCOMPATIBLE_STRING_DTYPES: Set[str] = {
        "bool", "boolean", "bit", "binary", "varbinary", "image", "bytes"
    }

    # Supported actions per ExecutionEngine
    ENGINE_CAPABILITIES: Dict[ExecutionEngine, Set[ActionType]] = {
        ExecutionEngine.AZURE_SQL: {
            ActionType.TRIM, ActionType.LOWERCASE, ActionType.UPPERCASE,
            ActionType.ZERO_TO_NULL, ActionType.REPLACE_SENTINELS,
            ActionType.CAST_TYPE, ActionType.COERCE_NUMERIC, ActionType.PARSE_DATES,
            ActionType.SANITIZE_EMAIL, ActionType.NORMALIZE_PHONE, ActionType.MASK_PHONE,
            ActionType.RANGE_CLIP, ActionType.CAP_OUTLIERS, ActionType.STANDARDIZE_BOOLEAN,
            ActionType.FILL_NULLS, ActionType.DROP_COLUMN, ActionType.DEDUPLICATE,
            ActionType.VALIDATE_REFERENTIAL_INTEGRITY, ActionType.CUSTOM_EXPRESSION,
            ActionType.PREFIX_ADD, ActionType.SUFFIX_ADD, ActionType.PATTERN_REPLACE,
            ActionType.VALUE_MAP, ActionType.PAD, ActionType.CONDITIONAL_UPDATE,
        },
        ExecutionEngine.PYTHON: {
            ActionType.TRIM, ActionType.LOWERCASE, ActionType.UPPERCASE,
            ActionType.ZERO_TO_NULL, ActionType.REPLACE_SENTINELS,
            ActionType.CAST_TYPE, ActionType.COERCE_NUMERIC, ActionType.PARSE_DATES,
            ActionType.SANITIZE_EMAIL, ActionType.NORMALIZE_PHONE, ActionType.MASK_PHONE,
            ActionType.RANGE_CLIP, ActionType.CAP_OUTLIERS, ActionType.STANDARDIZE_BOOLEAN,
            ActionType.FILL_NULLS, ActionType.DROP_COLUMN, ActionType.DEDUPLICATE,
            ActionType.VALIDATE_REFERENTIAL_INTEGRITY, ActionType.CUSTOM_EXPRESSION,
            ActionType.PREFIX_ADD, ActionType.SUFFIX_ADD, ActionType.PATTERN_REPLACE,
            ActionType.VALUE_MAP, ActionType.PAD, ActionType.CONDITIONAL_UPDATE,
        },
        ExecutionEngine.PYSPARK: {
            ActionType.TRIM, ActionType.LOWERCASE, ActionType.UPPERCASE,
            ActionType.ZERO_TO_NULL, ActionType.CAST_TYPE, ActionType.PARSE_DATES,
            ActionType.RANGE_CLIP, ActionType.FILL_NULLS, ActionType.DEDUPLICATE,
            ActionType.VALIDATE_REFERENTIAL_INTEGRITY, ActionType.PREFIX_ADD,
            ActionType.SUFFIX_ADD, ActionType.PATTERN_REPLACE, ActionType.VALUE_MAP,
            ActionType.PAD, ActionType.CONDITIONAL_UPDATE,
        }
    }

    def validate(
        self,
        action: StructuredAction,
        dataset_profiles: Optional[Dict[str, DatasetProfile]] = None,
        target_engine: Optional[ExecutionEngine] = None,
    ) -> ActionValidationResult:
        """Executes all 5 validation layers sequentially."""
        issues: List[DiagnosticIssue] = []

        # 1. Structural Validation
        structural_issues = self.validate_structural(action)
        issues.extend(structural_issues)

        # 2. Schema Validation (if profiles available)
        if dataset_profiles:
            schema_issues = self.validate_schema(action, dataset_profiles)
            issues.extend(schema_issues)

            # 3. Type Compatibility Validation (if schema passed)
            type_issues = self.validate_type_compatibility(action, dataset_profiles)
            issues.extend(type_issues)

        # 4. Semantic Validation
        semantic_issues = self.validate_semantic(action)
        issues.extend(semantic_issues)

        # 5. Engine Capability Validation (if engine specified)
        if target_engine:
            engine_issues = self.validate_engine_capability(action, target_engine)
            issues.extend(engine_issues)

        is_valid = not any(
            i.severity in (DiagnosticSeverity.ERROR, DiagnosticSeverity.BLOCKING)
            for i in issues
        )

        return ActionValidationResult(is_valid=is_valid, issues=issues)

    def validate_structural(self, action: StructuredAction) -> List[DiagnosticIssue]:
        """Layer 1: Verifies parameters conform to the typed parameter model for this action."""
        issues: List[DiagnosticIssue] = []

        if not action.dataset_name or not action.dataset_name.strip():
            issues.append(
                DiagnosticIssue(
                    issue_code="ACTION_MISSING_DATASET",
                    issue_type=ValidationIssueType.SYNTAX,
                    severity=DiagnosticSeverity.BLOCKING,
                    message="StructuredAction must specify a non-empty dataset_name.",
                    suggested_fix="Provide a valid dataset_name in the action contract.",
                )
            )

        # Must specify target columns unless action is table-level (like DEDUPLICATE with no subset)
        if not action.target_columns and action.action_type not in (
            ActionType.DEDUPLICATE,
            ActionType.VALIDATE_REFERENTIAL_INTEGRITY,
        ):
            issues.append(
                DiagnosticIssue(
                    issue_code="ACTION_MISSING_TARGET_COLUMNS",
                    issue_type=ValidationIssueType.SYNTAX,
                    severity=DiagnosticSeverity.ERROR,
                    message=f"Action '{action.action_type.value}' requires at least one target column.",
                    suggested_fix="Specify target_columns in the action contract.",
                )
            )

        param_model = ACTION_PARAMETER_REGISTRY.get(action.action_type)
        if param_model:
            try:
                param_model.model_validate(action.parameters)
            except ValidationError as err:
                for error in err.errors():
                    loc = ".".join(str(p) for p in error.get("loc", []))
                    msg = error.get("msg", "Invalid parameter")
                    issues.append(
                        DiagnosticIssue(
                            issue_code="ACTION_PARAMETER_INVALID",
                            issue_type=ValidationIssueType.SYNTAX,
                            severity=DiagnosticSeverity.ERROR,
                            message=f"Parameter validation failed for '{action.action_type.value}': field '{loc}' {msg}",
                            suggested_fix=f"Provide valid parameters conforming to {param_model.__name__}.",
                        )
                    )
            except Exception as e:
                issues.append(
                    DiagnosticIssue(
                        issue_code="ACTION_PARAMETER_ERROR",
                        issue_type=ValidationIssueType.SYNTAX,
                        severity=DiagnosticSeverity.ERROR,
                        message=f"Error validating parameters for '{action.action_type.value}': {str(e)}",
                    )
                )

        return issues

    def validate_schema(
        self, action: StructuredAction, dataset_profiles: Dict[str, DatasetProfile]
    ) -> List[DiagnosticIssue]:
        """Layer 2: Verifies dataset and target columns exist in the active profile."""
        issues: List[DiagnosticIssue] = []

        profile = dataset_profiles.get(action.dataset_name)
        if not profile:
            issues.append(
                DiagnosticIssue(
                    issue_code="ACTION_UNKNOWN_DATASET",
                    issue_type=ValidationIssueType.SCHEMA_CONTRACT,
                    severity=DiagnosticSeverity.ERROR,
                    message=f"Dataset '{action.dataset_name}' not found in active dataset profiles.",
                    suggested_fix=f"Available datasets: {list(dataset_profiles.keys())}",
                )
            )
            return issues

        existing_cols = set(profile.columns.keys())
        for col in action.target_columns:
            if col not in existing_cols:
                issues.append(
                    DiagnosticIssue(
                        issue_code="ACTION_UNKNOWN_COLUMN",
                        issue_type=ValidationIssueType.SCHEMA_CONTRACT,
                        severity=DiagnosticSeverity.ERROR,
                        message=f"Target column '{col}' does not exist in dataset '{action.dataset_name}'.",
                        suggested_fix=f"Ensure column name is spelled correctly. Existing columns: {sorted(list(existing_cols))}",
                    )
                )

        return issues

    def validate_type_compatibility(
        self, action: StructuredAction, dataset_profiles: Dict[str, DatasetProfile]
    ) -> List[DiagnosticIssue]:
        """Layer 3: Verifies column datatypes are compatible with the transformation operator."""
        issues: List[DiagnosticIssue] = []

        profile = dataset_profiles.get(action.dataset_name)
        if not profile:
            return issues

        for col in action.target_columns:
            col_prof = profile.get_column(col)
            if not col_prof:
                continue

            raw_dtype = (col_prof.dtype or "").lower().strip()

            # String operator compatibility
            if action.action_type in self.STRING_OPS:
                if any(raw_dtype.startswith(bad) for bad in self.INCOMPATIBLE_STRING_DTYPES):
                    issues.append(
                        DiagnosticIssue(
                            issue_code="ACTION_TYPE_INCOMPATIBLE",
                            issue_type=ValidationIssueType.SCHEMA_CONTRACT,
                            severity=DiagnosticSeverity.ERROR,
                            message=(
                                f"Action '{action.action_type.value}' cannot be applied directly to column "
                                f"'{col}' with incompatible datatype '{col_prof.dtype}'. Requires explicit cast to string first."
                            ),
                            suggested_fix=f"Prepend CAST_TYPE to string on '{col}' before applying '{action.action_type.value}'.",
                        )
                    )

            # Numeric operator compatibility
            elif action.action_type in self.NUMERIC_OPS and action.action_type != ActionType.COERCE_NUMERIC:
                if raw_dtype in ("string", "nvarchar", "varchar", "text") and not col_prof.inferred_type == "numeric":
                    issues.append(
                        DiagnosticIssue(
                            issue_code="ACTION_TYPE_NUMERIC_MISMATCH",
                            issue_type=ValidationIssueType.SCHEMA_CONTRACT,
                            severity=DiagnosticSeverity.WARNING,
                            message=(
                                f"Numeric action '{action.action_type.value}' applied to string column '{col}'. "
                                f"Ensure column is cast or coerced to numeric."
                            ),
                            suggested_fix=f"Prepend CAST_TYPE or COERCE_NUMERIC on '{col}'.",
                        )
                    )

        return issues

    def validate_semantic(self, action: StructuredAction) -> List[DiagnosticIssue]:
        """Layer 4: Verifies semantic bounds and domain invariants."""
        issues: List[DiagnosticIssue] = []

        # Operator-specific semantic logic
        if action.action_type == ActionType.PAD:
            length = action.parameters.get("length")
            if isinstance(length, int) and length <= 0:
                issues.append(
                    DiagnosticIssue(
                        issue_code="ACTION_SEMANTIC_PAD_LENGTH",
                        issue_type=ValidationIssueType.SYNTAX,
                        severity=DiagnosticSeverity.ERROR,
                        message=f"PAD length must be greater than 0, got {length}.",
                        suggested_fix="Set pad length to a positive integer.",
                    )
                )

        elif action.action_type in (ActionType.PREFIX_ADD, ActionType.SUFFIX_ADD):
            key = "prefix" if action.action_type == ActionType.PREFIX_ADD else "suffix"
            val = action.parameters.get(key)
            if val is not None and str(val).strip() == "":
                issues.append(
                    DiagnosticIssue(
                        issue_code="ACTION_SEMANTIC_EMPTY_AFFIX",
                        issue_type=ValidationIssueType.SYNTAX,
                        severity=DiagnosticSeverity.ERROR,
                        message=f"{action.action_type.value.upper()} {key} cannot be empty or whitespace.",
                        suggested_fix=f"Provide a non-empty string for {key}.",
                    )
                )

        elif action.action_type == ActionType.PATTERN_REPLACE:
            pat = action.parameters.get("pattern")
            is_regex = action.parameters.get("regex", True)
            if is_regex and pat:
                try:
                    re.compile(pat)
                except re.error as e:
                    issues.append(
                        DiagnosticIssue(
                            issue_code="ACTION_SEMANTIC_INVALID_REGEX",
                            issue_type=ValidationIssueType.SYNTAX,
                            severity=DiagnosticSeverity.ERROR,
                            message=f"Invalid regular expression pattern '{pat}': {e}",
                            suggested_fix="Fix the regex pattern syntax.",
                        )
                    )

        return issues

    def validate_engine_capability(
        self, action: StructuredAction, target_engine: ExecutionEngine
    ) -> List[DiagnosticIssue]:
        """Layer 5: Verifies that the requested engine supports the operator."""
        issues: List[DiagnosticIssue] = []

        supported_ops = self.ENGINE_CAPABILITIES.get(target_engine, set())
        if action.action_type not in supported_ops:
            issues.append(
                DiagnosticIssue(
                    issue_code="ACTION_ENGINE_UNSUPPORTED",
                    issue_type=ValidationIssueType.DIALECT,
                    severity=DiagnosticSeverity.BLOCKING,
                    message=f"Execution engine '{target_engine.value}' does not support action '{action.action_type.value}'.",
                    suggested_fix=f"Choose a compatible engine or decompose into supported primitives.",
                )
            )

        return issues
