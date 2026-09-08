# PR 6 Acceptance Evidence & Governance Specification

## Traceability Summary
This document contains the committed acceptance evidence and governance records for Agent Dhara PR 6 (Runtime Dry-Run Validation, Exact Multiset Row Identity Reconciliation, and Granular Authorization).

### Amendment Mapping
- **A-01**: Granular Unit Authorization (`agent/etl_pipeline/runtime_authorizer.py`) -> `tests/test_pr6_runtime_authorizer.py::test_granular_unit_authorization_promotion_eligibility`
- **A-02**: Production Isolation Boundary (`agent/etl_pipeline/execution_orchestrator.py`) -> `tests/test_pr6_runtime_authorizer.py::test_dry_run_cannot_target_production`
- **A-03**: Multi-Dimensional Approval Invalidation (`agent/etl_pipeline/remediation_models.py`) -> `tests/test_pr6_approval_binding.py::test_multi_dimensional_invalidation`
- **A-04**: Cryptographic Integrity-Bound Record (`agent/etl_pipeline/remediation_models.py`) -> `tests/test_pr6_approval_binding.py::test_canonical_binding_hash_determinism`
- **A-05**: T-SQL Atomic Logging & Nested Failure Capture (`agent/etl_pipeline/sql_codegen.py`) -> `tests/test_tsql_hardening.py::test_failure_log_exception_preserves_original_transformation_error`
- **A-06**: Multiset Identity Reconciliation (`agent/etl_pipeline/dry_run_validator.py`) -> `tests/test_pr6_identity_reconciler.py::test_multiset_row_loss_and_overlap_detection`
- **A-07**: Type-Safe Raw Companion Preservation (`agent/etl_pipeline/dry_run_validator.py`) -> `tests/test_pr6_identity_reconciler.py::test_null_safe_raw_value_preservation`
- **A-08**: Distinct Semantics for Non-Partitioning Actions (`agent/etl_pipeline/dry_run_validator.py`) -> `tests/test_action_code_alignment.py::test_flag_only_semantics`
- **A-09**: Structured Config Patch Safety (`agent/etl_pipeline/configuration_patch.py`) -> `tests/test_pr6_config_patch.py::test_config_patch_security_guards`
- **A-10**: Multipart SQL Identifier Quoting (`agent/etl_pipeline/sql_codegen.py`) -> `tests/test_tsql_hardening.py::test_tsql_companion_columns_and_explicit_inserts`
- **A-11**: Cross-Dialect Contract Specification -> `docs/architecture/cross-dialect-validation.md`
- **A-12**: Dialect Precedence Hierarchy -> `tests/test_pr6_cross_dialect_precedence.py`
- **A-13**: Action-Specific Quarantine Thresholds (`agent/etl_pipeline/runtime_authorizer.py`) -> `tests/test_pr6_runtime_authorizer.py::test_approved_tolerance_violation_becomes_blocked_pending_reapproval`
- **A-14**: Metadata-Driven Field Criticality Guard (`agent/etl_pipeline/runtime_authorizer.py`) -> `tests/test_pr6_runtime_authorizer.py::test_metadata_driven_field_criticality_guard`
- **A-15**: Two-Tier Scoring (Clean vs Full Population) (`agent/etl_pipeline/dry_run_validator.py`) -> `tests/test_pr6_identity_reconciler.py::test_two_tier_dq_scores`
- **A-16**: Runtime & Compiler Idempotency -> `tests/test_pr6_runtime_idempotency.py::test_runtime_transformation_idempotency`

## Pilot Boundaries & Stop Conditions
- Staging-isolated execution only; zero production write permissions.
- In-scope actions: `trim_whitespace`, casing normalization on non-key descriptive fields.
- Excluded: primary/business keys, row quarantine, deduplication, financial fields.
