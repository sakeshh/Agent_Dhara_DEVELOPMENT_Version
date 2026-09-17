# Phase 3 Increment 3.3: Formal Certification & Closure Record

## Verdict: CERTIFIED AND CLOSED

- **Engineering Milestone**: Phase 3 Increment 3.3: **CERTIFIED AND CLOSED**
- **Target Engine & Runtime**: Apache PySpark 3.5.1 on Python 3.13, Windows 11
- **Authoritative Spark Master**: `local[2]` (managed by shared session fixture in `tests/cross_engine/conftest.py`; selected tests evaluate `local[1]` partition behaviors explicitly)
- **Distributed Cluster Certification**: Not covered
- **Production Milestone**: **NOT AUTHORIZED FOR PRODUCTION DEPLOYMENT** (strictly blocked by PROD-1 through PROD-5; publication scope restricted to `IN_MEMORY_REFERENCE`)
- **Clean Implementation Commit**: `9e2985c637fd07f9ba15e32363b4499f63aed338`
- **Consolidated Test Suite Execution**: **370 passed, 0 failed, 0 xfailed** across all test suites

---

## 1. Complete Test Collection & Warning Accounting

The cross-engine parity suite collection count of 99 is strictly verified by:
```powershell
python -m pytest tests/cross_engine/ --collect-only -q
```
Evidence captured at: `docs/certification/evidence/phase_3_increment_3_3/collection_cross_engine.txt`.

### Detailed Test Suite Summary & Authoritative Log Checksums

| Suite Name | Command | Results & Warnings | Duration | Tracked Evidence Path | SHA-256 Checksum |
| :--- | :--- | :--- | :---: | :--- | :--- |
| **Cross-Engine Parity** | `pytest tests/cross_engine/ -v --color=no` | **99 passed, 0 failed, 0 warnings** | 82.72s | `docs/certification/evidence/phase_3_increment_3_3/cross_engine_99.log` | `9804701c5929ceec166f768d17a05f1f687abc9c4cd11d6ea1400e60e563ff6f` |
| **Pandas Reference Baseline** | `pytest tests/test_pandas_ir_compiler*.py ... -v --color=no` | **202 passed, 0 failed, 17 warnings** | 18.95s | `docs/certification/evidence/phase_3_increment_3_3/reference_baseline_202.log` | `57507a524d61df008d5565c41fa535c7fd7611d48855b3f48075b31bc04c8398` |
| **Core Business Rules** | `pytest tests/test_canonical.py ... -v --color=no` | **69 passed, 0 failed, 17 warnings** | 16.98s | `docs/certification/evidence/phase_3_increment_3_3/business_rules_69.log` | `452bf4df07c555b3742931d9feb7ad600fe1fc0404512e759ef4ba21e3b65576` |
| **Consolidated Total** | | **370 passed, 0 failed, 0 xfailed** | **118.65s** | | |

*Warning Accounting*: The 17 warnings in the Pandas reference and business-rules suites are reviewed, pre-existing warning signatures originating from upstream dependency baselines (e.g. `ydata_profiling` / `matplotlib`). The cross-engine PySpark parity suite completes with zero warnings.

### Module-Level Accounting for Cross-Engine Suite (Sum = 99)
| Module | Collected Tests | Passing Count | Key Contract Proved |
| :--- | :---: | :---: | :--- |
| `tests/cross_engine/test_cross_engine_contracts.py` | 10 | 10 | 11-field capability key, deviation expiration, runtime profile hashing, scalar V1 & tuple V1 vector hashes |
| `tests/cross_engine/test_parity_harness_negative.py` | 8 | 8 | Negative harness sensitivity (null divergence, multiset omission, scale mismatch, tz shifts, HMAC mismatch, expired deviation) |
| `tests/cross_engine/test_parity_pyspark_scalars.py` | 13 | 13 | Scalar transformations, conditional replace, boolean standardization, numeric/date parsing, divergent statistical imputation snapshot governance, AST safety |
| `tests/cross_engine/test_parity_pyspark_routing.py` | 7 | 7 | Local execution identity cache materialization, source-stable identity parity, duplicate coordinate detection, quarantine copy-mode, dropped export, collision fixtures negative, exception-safe cache release |
| `tests/cross_engine/test_parity_pyspark_dedup.py` | 5 | 5 | Deterministic deduplication (`preserve_first`, `preserve_last`), partition-independent source-stable dedup, null-key failure policy, null-key distinct policy |
| `tests/cross_engine/test_parity_pyspark_schema.py` | 7 | 7 | Schema mutations (rename/reorder), protected column drop rejection, projection exclusion rejection, destructive step authorization envelope, pre/post hash enforcement, rename-then-drop tracking, routing identity drop rejection |
| `tests/cross_engine/test_parity_pyspark_security.py` | 6 | 6 | Masking parity (preserve last/first N, email masking, redact-all), canonical boolean/integer hashing, byte-level parity with null policies, string canonical hashing default denial, HMAC secret boundary rejection |
| `tests/cross_engine/test_parity_pyspark_validation.py` | 5 | 5 | Non-mutating validation metrics parity, unknown policy count-as-violation, unknown policy count-as-pass, severity error abort, fail-step on unknown |
| `tests/cross_engine/test_parity_pyspark_increment_32_e2e.py` | 1 | 1 | End-to-end multi-step pipeline integrating routing, quarantine, dedup, schema mutations, masking, and validation |
| `tests/cross_engine/test_parity_pyspark_joins_derivations.py` | 37 | 37 | Multi-dataset joins, cardinality guards with typed failure, StepAuthorization envelope, null policies, unmatched routing, collision renaming, derivations, extractions, aggregations, publication state machine |
| **Total** | **99** | **99** | **Reconciled: 10 + 8 + 13 + 7 + 5 + 7 + 6 + 5 + 1 + 37 = 99** |

---

## 2. Certified Work Packages & Contract Reconciliation

### Work Package 3.3A: Multi-Dataset Joins (`ActionFamily.JOIN`)
- **Supported Join Types**: `INNER`, `LEFT`, `RIGHT`, `FULL_OUTER`, `SEMI`, `ANTI`, `CROSS`.
- **Typed Cardinality Failure Contract**:
  - Cardinality guards reject invalid `ONE_TO_ONE`, `ONE_TO_MANY`, and `MANY_TO_ONE` key relationships through typed `JoinCardinalityViolationError` (`FailureCategory.JOIN_CARDINALITY`).
  - Row-expansion cap violations (`CROSS` join or ratio bounds) are rejected through `RowExpansionLimitExceededError` (`FailureCategory.EXPANSION_LIMIT`).
- **Elevated Cross-Join Authorization Envelope**:
  - `authorized_cross_join` is compiler-derived and cannot be supplied authoritatively by the caller.
  - Cross-join compilation strictly requires a valid `StepAuthorization` envelope attached to the step:
    - `auth.step_id == step.step_id`
    - `auth.expires_at >= datetime.now(timezone.utc)`
    - `auth.policy_decision_id` is present and non-empty
    - `auth.approved_by_subject_id` is present and non-empty
    - `auth.authorized_step_hash == step.semantic_hash` (cryptographic verification)
- **Null-Key Policies (Authoritative IR Enums)**:
  - `JoinNullPolicy.DROP_NULL_MATCHES` (default): Null keys never match each other.
  - `JoinNullPolicy.MATCH_NULLS_TOGETHER`: Null matches null via `eqNullSafe`.
  - `JoinNullPolicy.FAIL_STEP_ON_NULL_KEY`: Compilation/runtime fails if any null key is present in left or right key columns.
- **Collision Resolution**: Suffix resolution via pre-renaming (`_left_df` and `_right_df`), guarding against pre-existing suffixed collisions.
- **Unmatched Routing**: Routing unmatched left/right records to secondary output datasets via `UnmatchedRowDisposition.ROUTE_TO_UNMATCHED_OUTPUT`.

### Work Package 3.3B: Column Derivations & Extractions (`ActionFamily.DERIVE`, `ActionFamily.EXTRACT`)
- **Arithmetic Derivation**:
  - Division by zero governed by `DivisionByZeroPolicy`:
    - `SET_NULL`: Returns NULL on division by zero.
    - `FAIL_STEP`: Step fails execution upon encountering zero divisor.
    - `RETURN_INFINITY`: Returns IEEE signed infinity/NaN.
    - `USE_FALLBACK`: Substitutes configured fallback value.
  - Integer overflow prevention: Widens operands to `DECIMAL(38, 0)` before evaluation, applying `NumericOverflowPolicy` (`CLIP_TO_RANGE`, `FAIL_STEP`, `SET_NULL`).
- **Conditional Derivation**: Multi-case `when`/`otherwise` logic with `ConditionalUnknownPolicy`:
  - `SET_NULL`: Evaluates unknown/null condition to NULL.
  - `USE_THEN`: Unknown condition branch routes to THEN branch.
  - `USE_ELSE`: Unknown condition branch routes to ELSE branch.
  - `USE_FALLBACK`: Routes to explicit fallback value.
  - `FAIL_STEP`: Fails step if condition evaluates to NULL.
- **Coalesce & Concat Derivations**:
  - Null-safe coalesce with optional fallback literal.
  - Concat governed by `ConcatNullPolicy`:
    - `IGNORE_NULLS`: Skips null values natively via `concat_ws`.
    - `PRESERVE_AS_EMPTY` / `COALESCE_EMPTY`: Replaces nulls with empty strings prior to concatenation.
    - `SET_RESULT_NULL` / `PROPAGATE_NULL`: If any input column is null, returns NULL for the row.
- **Date Arithmetic Scope Qualification**:
  - Scoped strictly to `DateArithmeticUnit.DAY`, `DateArithmeticUnit.MONTH`, and `DateArithmeticUnit.YEAR`.
  - Untested units (`HOUR`, `MINUTE`, `SECOND`) and non-modeled calendar systems (e.g. business days) are NOT certified and remain `NOT_TESTED`.
- **Extractions**:
  - Regex extraction enforcing `MultipleMatchPolicy` (`FIRST`, `LAST`, `FAIL_STEP`).
  - Delimiter extraction with automatic regex metacharacter escaping.
  - Date part extraction normalized to canonical IR contract:
    - **Monday = 0, Tuesday = 1, Wednesday = 2, Thursday = 3, Friday = 4, Saturday = 5, Sunday = 6**.
  - JSONPath extraction with bounded depth (`max_depth=10`) and array index (`max_array_index=1000`); arbitrary recursive queries default-blocked.

### Work Package 3.3C: Multi-Dataset Aggregations (`ActionFamily.AGGREGATE`)
- **Supported Aggregations**: `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`, `COUNT_DISTINCT`, `STDDEV_SAMP`, `STDDEV_POP`, `APPROX_PERCENTILE` (median).
- **Empty Input Policy**: `EmptyInputPolicy.EMIT_EMPTY_DATASET` produces a 0-row typed DataFrame matching expected schema without failing.
- **SUM All-Null Semantics (`AggregateValueNullPolicy`)**:
  - `TREAT_AS_ZERO`: Null values in sum evaluate to `0`.
  - `PROPAGATE_NULL`: Preserves SQL aggregate behavior where null values within a group are ignored, but an all-null group produces `NULL` (`None`). It does not cause a single null to poison an entire group containing valid numeric values.
- **Statistical Operations**:
  - `STDDEV_SAMP` on a single observation strictly returns `NULL` (`None`), avoiding sample divide-by-zero errors.
  - `STDDEV_POP` on a single observation strictly returns `0.0`.
  - Approximate median registered as `MEDIAN_APPROX` using `percentile_approx(col, 0.5, 1000)` with relative error <= 0.05.
  - Exact `MEDIAN` is unsupported natively in Catalyst and remains `NOT_TESTED` (`execution_eligible = False`).
- **Group Key Null Policies**: Enforces `GroupKeyNullPolicy.FAIL_STEP` and `GroupKeyNullPolicy.DROP_NULL_GROUP`.

### Work Package 3.3D: In-Memory Publication Layer Contracts
- **Formal State Machine**: `NEW` → `STAGING` → `STAGED` → `VALIDATING` → `COMMITTED` / `ABORTED`.
- **Terminal State Idempotency & Transition Governance**:
  - `commit()` after `COMMITTED`: Idempotently returns existing publication manifest.
  - `abort()` after `ABORTED`: Idempotent no-op.
  - `abort()` after `COMMITTED`: Rejected with `PySparkPublicationStateError` (committed outputs cannot be retroactively aborted).
  - `commit()` after `ABORTED`: Rejected with `PySparkPublicationStateError` (aborted transactions cannot publish).
  - `stage()` after terminal state (`COMMITTED`, `ABORTED`): Rejected with `PySparkPublicationStateError` (adapter instances are single-transaction lifecycles).
- **Two-Phase Staging & Validation**: Output DataFrames are staged in temporary references and validated against schema and metric contracts before atomic publication.
- **Atomic Rollback on Error**: If any output dataset fails postcondition validation or late output generation, all staged outputs are aborted and zero outputs are published into the target session registry.

---

## 3. Authoritative 11-Field Capability Key Registrations

All capability registrations are evaluated and recorded with complete 11-field identities:
- **Compiler**: `PySparkIRCompiler` / `v1.0.0`
- **Engine**: `PySpark` / `3.5.1`
- **Dialect**: `None`
- **IR / Catalogue Version**: `1.0.0` / `v1.0`
- **Runtime Profile Hash**: Bound to `SparkRuntimeProfile().compute_hash()`
- **Commit SHA**: `9e2985c637fd07f9ba15e32363b4499f63aed338`
- **Status**: `CapabilityStatus.VERIFIED`
- **Reason Code**: `VERIFIED_BY_PYSPARK_PARITY_INCREMENT_33`
- **Verified Proving Test**: `tests/cross_engine/test_parity_pyspark_joins_derivations.py`

### Registered Action Subtype Families (18 Distinct Subtypes)
1. `(ActionFamily.JOIN, "JOIN_INNER")`
2. `(ActionFamily.JOIN, "JOIN_LEFT")`
3. `(ActionFamily.JOIN, "JOIN_RIGHT")`
4. `(ActionFamily.JOIN, "JOIN_FULL_OUTER")`
5. `(ActionFamily.JOIN, "JOIN_SEMI")`
6. `(ActionFamily.JOIN, "JOIN_ANTI")`
7. `(ActionFamily.JOIN, "JOIN_CROSS")`
8. `(ActionFamily.DERIVE, "DERIVE_ARITHMETIC")`
9. `(ActionFamily.DERIVE, "DERIVE_CONDITIONAL")`
10. `(ActionFamily.DERIVE, "DERIVE_COALESCE")`
11. `(ActionFamily.DERIVE, "DERIVE_CONCAT")`
12. `(ActionFamily.DERIVE, "DERIVE_DATE_ARITHMETIC")`
13. `(ActionFamily.EXTRACT, "EXTRACT_REGEX")`
14. `(ActionFamily.EXTRACT, "EXTRACT_DELIMITER")`
15. `(ActionFamily.EXTRACT, "EXTRACT_DATE_PART")`
16. `(ActionFamily.EXTRACT, "EXTRACT_JSON_PATH")`
17. `(ActionFamily.AGGREGATE, "AGGREGATE")`
18. `(ActionFamily.AGGREGATE, "MEDIAN_APPROX")` [accuracy: 1000, quantile: 0.5, relative tolerance <= 0.05]

*Unregistered / Default-Blocked*: Exact `(ActionFamily.AGGREGATE, "MEDIAN")`, unbounded JSONPath, and untested date arithmetic units remain `NOT_TESTED` with `execution_eligible = False`.

---

## 4. Authoritative Production Blocker Governance (PROD-1 through PROD-5)

This certification does not authorize durable production deployment or distributed cluster execution:
- **PROD-1 (Execution-Boundary Secret Resolution)**: KMS key derivation and HMAC secret management remain mock-scoped.
- **PROD-2 (Execution-Time Authorization Revalidation)**: Runtime distributed token freshness, multi-tenant revocation, and approval validation are deferred.
- **PROD-3 (Durable Publication Commit Protocol)**: Publication scope is strictly `IN_MEMORY_REFERENCE`. Transactional ACID table writes (e.g. Delta Lake) and object-store publication remain deferred.
- **PROD-4 (Cross-Engine Semantic Parity)**: SQL, DuckDB, ADF, and other engines remain blocked under capability registry default denial until subsequent milestone increments.
- **PROD-5 (Runtime Resource and Expansion Governance)**: Authoritative test fixture is `local[2]`; distributed cluster (YARN/K8s) resource governance, dynamic timeouts, executor memory bounds, and spill-to-disk controls are not certified.

---

## 5. Milestone Parity Scope

The milestone establishes parity for the exact Increment 3.3 capability tuples exercised by the 37 declared tests under the recorded PySpark 3.5.1 runtime profile. Untested expression subtypes, policy combinations, runtime profiles, and engine versions remain default-blocked.
