# PR 6 Cross-Dialect Contract & Precedence Specification

## Overview
This document serves as the repository-committed architectural specification and evidence artifact for Amendment A-11 and A-12 of PR 6.

## Precedence Hierarchy
When multiple transformation dialect implementations are generated for the same canonical remediation unit, the runtime execution engine selects the engine based on deterministic precedence rules:

1. **PySpark / Distributed Engine**: Preferred for partitioned, distributed, or high-volume datasets (> 100,000 rows).
2. **T-SQL / Database Native Engine**: Preferred for transactional relational datasets residing directly within SQL Server / Azure SQL.
3. **Pandas / In-Memory Engine**: Used strictly for small isolated runs, client testing, and unit verification.

## Semantic Equivalence & Cross-Dialect Invariants
All three dialect generators must adhere to the same canonical contract for a given remediation action:
- **Null Safety**: All operations preserve nulls unless explicit replacement (`nullify_clean_value` or `normalize_sentinel`) is requested.
- **Raw Companion Preservation**: The raw column companion (`<Column>_raw`) retains exact source types and values without coercion.
- **Deterministic Type Mapping**: Output column types align strictly with the target schema.
- **Idempotency**: Executing any dialect transformation twice yields identical row counts, clean values, and quarantine membership.

## Verification
Automated contract tests are committed in:
- `tests/test_pr6_cross_dialect_precedence.py`
- `tests/test_composite_field_compilation.py`
- `tests/test_tsql_hardening.py`
