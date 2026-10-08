from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.logical import ActionType
from agent.compiler.models.validation import (
    DiagnosticIssue,
    DiagnosticSeverity,
    ValidationIssueType,
    ValidationResult,
)
from agent.compiler.rules.normalized_rules import NormalizedBusinessRules, ConstraintType
from agent.semantic_context import ColumnRole, SemanticCleaningPlan

logger = logging.getLogger(__name__)


class ArtifactSemanticValidator:
    """
    Validates complete rendered SQL artifacts (e.g. T-SQL stored procedures)
    for physical correctness, structural safety, semantic invariants, and policy compliance
    before persistence and execution.
    
    Operates on the entire combined artifact to catch system-level anomalies that
    individual template fragment validators cannot detect.
    """

    HTML_ENTITIES = ["<br>", "<br/>", "&gt;", "&lt;", "&amp;", "&quot;", "&#39;", "&nbsp;"]

    KNOWN_METRIC_COLUMNS = {
        "salary", "creditlimit", "amount", "price", "cost",
        "revenue", "balance", "total", "rate", "quantity", "count"
    }

    KNOWN_IDENTIFIER_COLUMNS = {
        "managerid", "employeeid", "departmentid", "customerid",
        "userid", "accountid", "orderid", "vendorid", "productid"
    }

    @classmethod
    def validate_artifact(
        cls,
        sql_code: str,
        context: Optional[CompilerContext] = None,
    ) -> ValidationResult:
        issues: List[DiagnosticIssue] = []

        # 1. Stored Procedure Singleton Check
        cls._validate_procedure_singleton(sql_code, issues)

        # 2. Raw SQL vs Display HTML Entity Check
        cls._validate_no_html_entities(sql_code, issues)

        # 3. CTE Scope and Statement Boundary Check
        cls._validate_cte_scope(sql_code, issues)

        # 4. Deduplication Safety Invariants
        cls._validate_deduplication_safety(sql_code, context, issues)

        # 5. Policy Compliance (never_drop_rows)
        cls._validate_policy_compliance(sql_code, context, issues)

        # 6. Type and Semantic Role Residue
        cls._validate_semantic_role_residue(sql_code, context, issues)

        # 7. Business Rule Residue & Pure Validation Invariants
        cls._validate_business_rule_residue(sql_code, context, issues)

        has_blocking = any(i.severity in (DiagnosticSeverity.BLOCKING, DiagnosticSeverity.ERROR) for i in issues)

        return ValidationResult(
            is_valid=not has_blocking,
            target_engine=getattr(getattr(context, "target_engine", None), "value", "azure_sql"),
            issues=issues,
            has_security_hazards=any(i.issue_type == ValidationIssueType.SECURITY for i in issues),
            has_performance_anti_patterns=any(i.issue_type == ValidationIssueType.PERFORMANCE for i in issues),
            invariants_satisfied=not has_blocking,
            metadata={"total_issue_count": len(issues)},
        )

    @classmethod
    def _strip_comments(cls, sql_code: str) -> str:
        """Removes single-line and multi-line comments from SQL for safe lexical analysis."""
        # Remove block comments
        no_block = re.sub(r"/\*[\s\S]*?\*/", "", sql_code)
        # Remove line comments
        no_line = re.sub(r"--[^\n]*", "", no_block)
        return no_line

    @classmethod
    def _split_statements(cls, sql_code: str) -> List[str]:
        """
        Splits clean SQL by semicolon statements while preserving statement blocks
        and ignoring semicolons inside string literals.
        """
        placeholder = "'__STR_LIT__'"
        no_strings = re.sub(r"'(?:''|[^'])*'", placeholder, sql_code)
        raw_stmts = [s.strip() for s in no_strings.split(";") if s.strip()]
        return raw_stmts

    @classmethod
    def _validate_procedure_singleton(cls, sql_code: str, issues: List[DiagnosticIssue]) -> None:
        """
        Ensures exactly one top-level stored procedure definition exists in the artifact.
        Blocks concatenated or duplicate procedure definitions.
        """
        clean_sql = cls._strip_comments(sql_code)
        proc_matches = re.findall(
            r"\bCREATE\s+(?:OR\s+ALTER\s+)?(?:PROCEDURE|PROC)\s+([\[\]\w\.]+)",
            clean_sql,
            re.IGNORECASE,
        )

        if len(proc_matches) > 1:
            issues.append(
                DiagnosticIssue(
                    issue_code="DUPLICATE_PROCEDURE_DEFINITION",
                    issue_type=ValidationIssueType.GOVERNANCE,
                    severity=DiagnosticSeverity.BLOCKING,
                    message=f"Artifact contains {len(proc_matches)} stored procedure definitions ({proc_matches}). Exactly 1 definition is permitted.",
                    suggested_fix="Merge physical plans into a single procedure generation pass.",
                )
            )
        elif len(proc_matches) == 0:
            # Only flag if artifact is intended to be a procedure
            if "BEGIN TRANSACTION" in sql_code.upper() or "@PipelineId" in sql_code:
                issues.append(
                    DiagnosticIssue(
                        issue_code="MISSING_PROCEDURE_DEFINITION",
                        issue_type=ValidationIssueType.SYNTAX,
                        severity=DiagnosticSeverity.BLOCKING,
                        message="Expected CREATE OR ALTER PROCEDURE definition was not found in the SQL artifact.",
                        suggested_fix="Ensure stored procedure header is generated.",
                    )
                )

    @classmethod
    def _validate_no_html_entities(cls, sql_code: str, issues: List[DiagnosticIssue]) -> None:
        """
        Ensures raw persisted SQL contains zero HTML entity residue (<br>, <br/>, &gt;, &lt;, &amp;, &quot;, &#39;, &nbsp;).
        The frontend escapes SQL for display; backend artifacts must retain raw SQL bytes.
        Does not flag standard SQL characters (', <, >, &) when used as valid syntax or literals.
        """
        for entity in cls.HTML_ENTITIES:
            if entity in sql_code:
                # Find line number
                lines = sql_code.splitlines()
                matching_lines = [idx + 1 for idx, l in enumerate(lines) if entity in l]
                issues.append(
                    DiagnosticIssue(
                        issue_code="HTML_ENTITY_IN_SQL_ARTIFACT",
                        issue_type=ValidationIssueType.SYNTAX,
                        severity=DiagnosticSeverity.BLOCKING,
                        message=f"Raw SQL artifact contains HTML entity '{entity}' at line(s) {matching_lines[:5]}.",
                        problematic_code=entity,
                        suggested_fix=f"Replace '{entity}' with raw SQL operator (e.g. &gt; -> >).",
                    )
                )

    @classmethod
    def _validate_cte_scope(cls, sql_code: str, issues: List[DiagnosticIssue]) -> None:
        """
        Detects invalid CTE scope reuse where a CTE is referenced across multiple statements.
        In T-SQL, a CTE only exists for the single statement immediately following it.
        If a subsequent statement declares its own CTE with the same name, that is valid.
        """
        clean_sql = cls._strip_comments(sql_code)
        stmts = cls._split_statements(clean_sql)

        all_prior_ctes: set[str] = set()
        for stmt in stmts:
            # Find all CTE declarations in this statement: (WITH or comma) <ident> AS (
            current_ctes = {
                m.strip("[]")
                for m in re.findall(r"(?:WITH|,)\s+([a-zA-Z0-9_\[\]]+)\s+AS\s*\(", stmt, re.IGNORECASE)
            }

            # Check if this statement references any previously declared CTE that is NOT in current_ctes
            for prior_cte in (all_prior_ctes - current_ctes):
                if re.search(rf"\b(?:FROM|JOIN)\s+(?:\[?[a-zA-Z0-9_]+\]?\.)?\[?{re.escape(prior_cte)}\]?", stmt, re.IGNORECASE):
                    issues.append(
                        DiagnosticIssue(
                            issue_code="CTE_SCOPE_VIOLATION",
                            issue_type=ValidationIssueType.SYNTAX,
                            severity=DiagnosticSeverity.BLOCKING,
                            message=f"CTE '{prior_cte}' is referenced across multiple statements after its terminating semicolon.",
                            suggested_fix="Materialize duplicate rows into a temporary table (#...) instead of reusing CTE.",
                        )
                    )

            all_prior_ctes.update(current_ctes)

    @classmethod
    def _validate_deduplication_safety(
        cls,
        sql_code: str,
        context: Optional[CompilerContext],
        issues: List[DiagnosticIssue],
    ) -> None:
        """
        Validates deduplication implementation for safety and determinism:
        - Blocks ORDER BY (SELECT 1)
        - Blocks un-partitioned ROW_NUMBER deduplication
        - Blocks single categorical partition keys
        """
        clean_sql = cls._strip_comments(sql_code)

        # 1. Non-deterministic survivor order
        if re.search(r"ORDER\s+BY\s*\(\s*SELECT\s+1\s*\)", clean_sql, re.IGNORECASE):
            issues.append(
                DiagnosticIssue(
                    issue_code="DEDUP_NON_DETERMINISTIC_SURVIVOR",
                    issue_type=ValidationIssueType.GOVERNANCE,
                    severity=DiagnosticSeverity.BLOCKING,
                    message="Deduplication uses non-deterministic 'ORDER BY (SELECT 1)'. Explicit survivor ordering is required.",
                    suggested_fix="Provide deterministic survivor order (e.g. ORDER BY [_DharaStagingRowId] ASC).",
                )
            )

        # 2. Missing partition key in deduplication context
        dedup_window_matches = re.findall(
            r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(\s*(ORDER\s+BY[^\)]+)\)",
            clean_sql,
            re.IGNORECASE,
        )
        for m in dedup_window_matches:
            if "dedup" in clean_sql.lower() or "duplicate" in clean_sql.lower():
                issues.append(
                    DiagnosticIssue(
                        issue_code="DEDUP_MISSING_PARTITION_KEY",
                        issue_type=ValidationIssueType.GOVERNANCE,
                        severity=DiagnosticSeverity.BLOCKING,
                        message="Deduplication ROW_NUMBER window is missing PARTITION BY clause.",
                        suggested_fix="Specify business key columns in PARTITION BY clause.",
                    )
                )

        # 3. Single categorical partition key without business key approval
        single_partition_matches = re.findall(
            r"PARTITION\s+BY\s+\[?([a-zA-Z0-9_]+)\]?\s+ORDER\s+BY",
            clean_sql,
            re.IGNORECASE,
        )
        categorical_names = {"customerstatus", "status", "gender", "isactive", "flag", "type"}
        for col_name in single_partition_matches:
            c_lower = col_name.lower()
            if c_lower in categorical_names:
                issues.append(
                    DiagnosticIssue(
                        issue_code="DEDUP_INVALID_PARTITION_SEMANTICS",
                        issue_type=ValidationIssueType.GOVERNANCE,
                        severity=DiagnosticSeverity.BLOCKING,
                        message=f"Deduplication partitioned by single categorical column '{col_name}', which would cause catastrophic data loss.",
                        suggested_fix="Partition by unique entity business key columns instead of categorical status.",
                    )
                )

    @classmethod
    def _validate_policy_compliance(
        cls,
        sql_code: str,
        context: Optional[CompilerContext],
        issues: List[DiagnosticIssue],
    ) -> None:
        """
        Validates compliance with global policies such as never_drop_rows:
        - Prohibits DELETE without durable quarantine into Rejection_Log
        - Prohibits any unquarantined deletion
        """
        never_drop = False
        if context:
            never_drop = context.options.get("never_drop_rows", False) or any(
                getattr(r, "never_drop_rows", False) or getattr(r, "name", "") == "never_drop_rows"
                for r in getattr(context, "business_rules", []) or []
            )

        clean_sql = cls._strip_comments(sql_code)
        delete_matches = re.finditer(r"\bDELETE\s+(?:FROM\s+)?([\[\]\w\.]+)", clean_sql, re.IGNORECASE)

        for m in delete_matches:
            tbl = m.group(1).strip("[]")
            if never_drop:
                # If never_drop_rows is active, no DELETE is allowed anywhere in the script
                issues.append(
                    DiagnosticIssue(
                        issue_code="SQL_INVARIANT_VIOLATION",
                        issue_type=ValidationIssueType.GOVERNANCE,
                        severity=DiagnosticSeverity.BLOCKING,
                        message=f"Active 'never_drop_rows' policy violated: executable DELETE on '{tbl}' detected in generated SQL.",
                        suggested_fix="Use FLAG disposition and retain rows instead of issuing DELETE.",
                    )
                )
            else:
                # Without never_drop_rows, deletion is only allowed if accompanied by durable quarantine
                # Check if Rejection_Log insertion occurs in clean_sql (comments stripped)
                if "[ETL_REJECT].[Rejection_Log]" not in clean_sql or "INSERT INTO" not in clean_sql:
                    issues.append(
                        DiagnosticIssue(
                            issue_code="UNQUARANTINED_DELETE_VIOLATION",
                            issue_type=ValidationIssueType.GOVERNANCE,
                            severity=DiagnosticSeverity.BLOCKING,
                            message=f"DELETE on table '{tbl}' executed without durable logging to [ETL_REJECT].[Rejection_Log].",
                            suggested_fix="Log rows to [ETL_REJECT].[Rejection_Log] prior to deleting from staging.",
                        )
                    )

    @classmethod
    def _validate_semantic_role_residue(
        cls,
        sql_code: str,
        context: Optional[CompilerContext],
        issues: List[DiagnosticIssue],
    ) -> None:
        """
        Scans rendered SQL for impossible combinations violating column semantic roles:
        - LOWER/UPPER on metric/numeric columns
        - COALESCE(numeric, '')
        - Date conversions on identifiers (e.g. TRY_CONVERT(DATE, ManagerID))
        """
        clean_sql = cls._strip_comments(sql_code)

        # Dynamically harvest column roles from semantic plan and profiles
        metric_cols = set(cls.KNOWN_METRIC_COLUMNS)
        ident_cols = set(cls.KNOWN_IDENTIFIER_COLUMNS)

        if context:
            if context.semantic_plan and hasattr(context.semantic_plan, "entities"):
                for entity in context.semantic_plan.entities.values():
                    for col_name, contract in getattr(entity, "column_contracts", {}).items():
                        c_lower = col_name.strip("[]").lower()
                        role = getattr(contract, "role", None)
                        role_val = getattr(role, "value", str(role)).upper()
                        if role_val in ("METRIC", "CURRENCY", "MEASURE", "NUMERIC"):
                            metric_cols.add(c_lower)
                        elif role_val in ("IDENTIFIER", "PRIMARY_KEY", "FOREIGN_KEY", "BUSINESS_KEY"):
                            ident_cols.add(c_lower)

            if context.dataset_profiles:
                for prof in context.dataset_profiles.values():
                    if hasattr(prof, "columns"):
                        for c_name, c_prof in prof.columns.items():
                            c_lower = c_name.strip("[]").lower()
                            dtype = (getattr(c_prof, "dtype", "") or "").lower()
                            sem_type = (getattr(c_prof, "semantic_type", "") or "").lower()
                            if any(k in dtype for k in ("int", "decimal", "numeric", "float", "money", "real")) or sem_type in ("metric", "numeric", "currency"):
                                metric_cols.add(c_lower)
                            if sem_type in ("id", "identifier", "key") or c_lower.endswith("id") or c_lower.endswith("key"):
                                ident_cols.add(c_lower)

        # 1. LOWER / UPPER on metric columns
        for metric in metric_cols:
            pattern = rf"\b(?:LOWER|UPPER)\s*\(\s*(?:\[?[a-zA-Z0-9_]+\]?\.)?\[?{re.escape(metric)}\]?\s*\)"
            if re.search(pattern, clean_sql, re.IGNORECASE):
                issues.append(
                    DiagnosticIssue(
                        issue_code="ARTIFACT_SEMANTIC_ROLE_VIOLATION",
                        issue_type=ValidationIssueType.SCHEMA_CONTRACT,
                        severity=DiagnosticSeverity.BLOCKING,
                        message=f"Semantic role violation in rendered SQL: String casing (LOWER/UPPER) applied to numeric metric column '{metric}'.",
                        suggested_fix=f"Remove text casing operation from column '{metric}'.",
                    )
                )

        # 2. Empty-string COALESCE on numeric columns (COALESCE([Salary], ''))
        for metric in metric_cols:
            pattern = rf"\b(?:COALESCE|ISNULL)\s*\(\s*(?:\[?[a-zA-Z0-9_]+\]?\.)?\[?{re.escape(metric)}\]?\s*,\s*''\s*\)"
            if re.search(pattern, clean_sql, re.IGNORECASE):
                issues.append(
                    DiagnosticIssue(
                        issue_code="ARTIFACT_SEMANTIC_ROLE_VIOLATION",
                        issue_type=ValidationIssueType.SCHEMA_CONTRACT,
                        severity=DiagnosticSeverity.BLOCKING,
                        message=f"Semantic role violation in rendered SQL: Empty-string fallback COALESCE applied to numeric metric column '{metric}'.",
                        suggested_fix=f"Use numeric default (e.g. 0) or leave as NULL with validation flag for '{metric}'.",
                    )
                )

        # 3. Date conversion on identifier columns (ManagerID, EmployeeID)
        for ident in ident_cols:
            pattern = (
                rf"(?:\b(?:TRY_CONVERT|CONVERT)\s*\(\s*DATE(?:TIME2?)?\s*,\s*(?:\[?[a-zA-Z0-9_]+\]?\.)?\[?{re.escape(ident)}\]?"
                rf"|\bCAST\s*\(\s*(?:\[?[a-zA-Z0-9_]+\]?\.)?\[?{re.escape(ident)}\]?\s*AS\s*DATE(?:TIME2?)?\b\s*\))"
            )
            if re.search(pattern, clean_sql, re.IGNORECASE):
                issues.append(
                    DiagnosticIssue(
                        issue_code="ARTIFACT_SEMANTIC_ROLE_VIOLATION",
                        issue_type=ValidationIssueType.SCHEMA_CONTRACT,
                        severity=DiagnosticSeverity.BLOCKING,
                        message=f"Semantic role violation in rendered SQL: Date parsing applied to identifier column '{ident}'.",
                        suggested_fix=f"Identifier column '{ident}' must not be parsed as a date.",
                    )
                )

    @classmethod
    def _validate_business_rule_residue(
        cls,
        sql_code: str,
        context: Optional[CompilerContext],
        issues: List[DiagnosticIssue],
    ) -> None:
        """
        Ensures rendered SQL strictly adheres to normalized business rules:
        - Pure constraints must not emit data mutations (RANGE_CLIP, FILL_NULLS, VALUE_MAP).
        - Actions with FLAG disposition must not render UPDATE ... SET statements.
        - Preserves step markers and lineage (-- ActionId, -- RuleId, -- RemediationAuthority).
        """
        if not context or not context.normalized_business_rules:
            return

        norm_rules: NormalizedBusinessRules = context.normalized_business_rules
        clean_sql = cls._strip_comments(sql_code)

        # 1. Lineage-based inspection: parse step blocks with ActionId/RemediationAuthority markers
        step_blocks = re.split(r"(?=--\s*ActionId:\s*)", sql_code)
        checked_lineage = False
        for block in step_blocks:
            if not block.strip().startswith("-- ActionId:"):
                continue
            checked_lineage = True
            act_match = re.search(r"--\s*ActionId:\s*([^\n]+)", block)
            rule_match = re.search(r"--\s*RuleId:\s*([^\n]+)", block)
            auth_match = re.search(r"--\s*RemediationAuthority:\s*([^\n]+)", block)
            action_id = act_match.group(1).strip() if act_match else ""
            rule_id = rule_match.group(1).strip() if rule_match else ""
            authority = auth_match.group(1).strip() if auth_match else ""

            clean_block = cls._strip_comments(block)
            if authority == "CONSTRAINT_ONLY":
                # Range clip check
                if re.search(r"SET\s+\[?\w+\]?\s*=\s*CASE\s+WHEN\s+\[?\w+\]?\s*<\s*[\d\.]+\s+THEN", clean_block, re.IGNORECASE):
                    issues.append(
                        DiagnosticIssue(
                            issue_code="BUSINESS_RULE_RESIDUE_VIOLATION",
                            issue_type=ValidationIssueType.GOVERNANCE,
                            severity=DiagnosticSeverity.BLOCKING,
                            message=f"Step '{action_id}' (rule '{rule_id}') has authority CONSTRAINT_ONLY but rendered mutating RANGE_CLIP.",
                            suggested_fix="Retain FLAG validation only without mutating column values.",
                        )
                    )
                # Fill nulls check
                if re.search(r"SET\s+\[?\w+\]?\s*=\s*(?:COALESCE|ISNULL)\s*\(\s*\[?\w+\]?\s*,", clean_block, re.IGNORECASE):
                    issues.append(
                        DiagnosticIssue(
                            issue_code="BUSINESS_RULE_RESIDUE_VIOLATION",
                            issue_type=ValidationIssueType.GOVERNANCE,
                            severity=DiagnosticSeverity.BLOCKING,
                            message=f"Step '{action_id}' (rule '{rule_id}') has authority CONSTRAINT_ONLY but rendered mutating FILL_NULLS.",
                            suggested_fix="Retain FLAG validation only without imputing values.",
                        )
                    )

        # 2. Fallback / Global rule inspection when lineage markers are absent
        if not checked_lineage:
            # Check pure range constraints: no RANGE_CLIP mutation
            for cr in norm_rules.column_rules:
                if cr.enabled and cr.constraint_type == ConstraintType.RANGE and cr.is_constraint_only:
                    col = cr.column
                    clip_pattern = rf"SET\s+\[?{re.escape(col)}\]?\s*=\s*CASE\s+WHEN\s+\[?{re.escape(col)}\]?\s*<\s*[\d\.]+\s+THEN"
                    if re.search(clip_pattern, clean_sql, re.IGNORECASE):
                        issues.append(
                            DiagnosticIssue(
                                issue_code="BUSINESS_RULE_RESIDUE_VIOLATION",
                                issue_type=ValidationIssueType.GOVERNANCE,
                                severity=DiagnosticSeverity.BLOCKING,
                                message=f"Pure range constraint on '{col}' was improperly compiled into mutating RANGE_CLIP.",
                                suggested_fix="Retain FLAG validation only without mutating column values.",
                            )
                        )

                # Check pure NOT_NULL constraints: no unapproved FILL_NULLS
                elif cr.enabled and cr.constraint_type == ConstraintType.NOT_NULL and cr.is_constraint_only:
                    col = cr.column
                    fill_pattern = rf"SET\s+\[?{re.escape(col)}\]?\s*=\s*(?:COALESCE|ISNULL)\s*\(\s*\[?{re.escape(col)}\]?\s*,"
                    if re.search(fill_pattern, clean_sql, re.IGNORECASE):
                        issues.append(
                            DiagnosticIssue(
                                issue_code="BUSINESS_RULE_RESIDUE_VIOLATION",
                                issue_type=ValidationIssueType.GOVERNANCE,
                                severity=DiagnosticSeverity.BLOCKING,
                                message=f"Pure NOT_NULL constraint on '{col}' was improperly compiled into mutating FILL_NULLS.",
                                suggested_fix="Retain FLAG validation only without imputing values.",
                            )
                        )
