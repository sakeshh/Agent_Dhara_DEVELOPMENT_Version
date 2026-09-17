# Certification Record: Phase 3 Increment 3.4A — Azure SQL Database T-SQL Compiler & Semantic Parity (Vertical Slice 1)

## 1. Executive Certification Summary

| Certification Attribute | Authoritative Valuation |
| :--- | :--- |
| **Milestone Identifier** | **Phase 3 Increment 3.4A** |
| **Milestone Scope** | **Azure SQL Database T-SQL IR Compiler & Semantic Parity (Vertical Slice 1)** |
| **Authoritative Target Engine** | **Azure SQL Database** (`engine_name = "azure_sql_database"`) |
| **Authoritative Dialect** | **T-SQL** (`dialect = "tsql"`) |
| **Verification Interface** | **Azure SQL Database Query Editor** (Direct target-engine execution & committed evidence capture) |
| **Maturity / Milestone Status** | **CERTIFIED AND CLOSED** |
| **Compiler Name & Version** | `AzureSqlTsqlIRCompiler` v1.0.0 |
| **Clean Implementation Commit** | `6d6a706c8b11414092c3574fe00f40218283a260` |
| **Consolidated Verified Tests** | **390 passed, 0 failed, 0 xfailed** (100% pass rate) |
| **Publication Boundary** | `IN_MEMORY_REFERENCE` (Query artifact / result set only) |
| **Production Authorization** | **NOT AUTHORIZED FOR PRODUCTION DEPLOYMENT** (Governed by PROD-1 through PROD-5) |

---

## 2. Authoritative Engine Boundaries & Scope Exclusions

| Engine / System | Role in Increment 3.4A | Capability Status |
| :--- | :--- | :--- |
| **Azure SQL Database** | **Authoritative T-SQL Execution Engine** | **`VERIFIED` for Slice 1 capability tuples** |
| **Azure SQL Query Editor** | Direct Verification & Execution Interface | Interface for target-engine evidence generation |
| **DuckDB** | *Excluded from authoritative certification* | **Zero certification authority** (Not T-SQL) |
| **Microsoft Fabric Warehouse** | Future distributed lakehouse warehouse | Strictly **`NOT_TESTED`** (Separate dialect/runtime) |
| **Spark SQL** | Future distributed SQL dialect | Strictly **`NOT_TESTED`** (Separate runtime profile) |

> [!IMPORTANT]
> **No Dialect Proxying or Substitution**:
> Certification is grounded in unmodified T-SQL artifacts compiled specifically for and executed directly on Azure SQL Database. DuckDB and Microsoft Fabric Warehouse are excluded from this certification boundary.

---

## 3. Dynamic Azure SQL Runtime Profile (`AzureSqlDatabaseRuntimeProfile`)

Compilation and execution are bound to an evidence-captured runtime profile:
- **Engine Name**: `azure_sql_database`
- **Dialect**: `tsql`
- **Engine Version**: `12.0.2000.8` (Observed from `SERVERPROPERTY('ProductVersion')`)
- **Engine Edition**: `5` (Observed from `SERVERPROPERTY('EngineEdition')` — SQL Database)
- **Database Engine Version**: `Microsoft SQL Azure (RTM) - 12.0.2000.8` (Observed from `@@VERSION`)
- **Compatibility Level**: `160` (Observed from `sys.databases`)
- **Database Collation**: `SQL_Latin1_General_CP1_CI_AS` (Observed from `sys.databases.collation_name`)
- **Server Collation**: `SQL_Latin1_General_CP1_CI_AS` (Observed from `SERVERPROPERTY('Collation')`)
- **ANSI Session Invariants**: `ANSI_NULLS = ON`, `ANSI_WARNINGS = ON`, `ARITHABORT = ON`, `XACT_ABORT = ON`
- **Session Timezone**: `UTC`
- **Case Sensitivity**: `False` (Derived from database collation `_CI_`)
- **Three-Valued Logic**: `True` (SQL NULL semantics strictly enforced)
- **Decimal Overflow Mode**: `ERROR`
- **Runtime Profile Hash**: `fd120d68b286ea9ad46551202083ac5cd28197922253dc4eec072c86a43dca4e`

---

## 4. Verified Regression Suite Accounting

All 390 consolidated regression tests executed cleanly with zero failures and zero xfails:

| Suite | Component Tests | Passing | Failing | xfailed | Warnings | Tracked Artifact Log | SHA-256 Checksum |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| **Cross-Engine Parity** | PySpark (99) + Azure SQL Compiler (13) + Azure SQL Evidence (7) | **119** | 0 | 0 | 0 | `cross_engine_119.log` | `c7a31901e1ce446268bcbad5e12ae39e72c350a9059af844a33008385ed7706e` |
| **Pandas Reference** | Full 8-file transformation IR baseline | **202** | 0 | 0 | 17 | `reference_baseline_202.log` | `720c2f2fe4f41af0e011987969f1625a0bf24310470d873dfb0f540b8d7226f4` |
| **Core Business Rules** | 9-file rules, conflict, assessment manifest | **69** | 0 | 0 | 17 | `business_rules_69.log` | `77cfbb1716bd97236ed28ed6ec06c47b9377f10dc5cb37e1ed637f2e604358df` |
| **Consolidated Total** | **Consolidated authoritatively** | **390** | **0** | **0** | **34** | `checksums.sha256` | Complete manifest verified |

*Warning Qualification*: All 34 warnings originate exclusively from upstream datetime and matplotlib deprecation notices in third-party library baselines. The cross-engine suite executed with zero warnings.

---

## 5. Certified Vertical Slice 1 Capability Ledger

The following capabilities are formally certified under the 11-field capability identity:
`(family, expression_subtype, policy_profile_hash, ir_version, catalogue_version, compiler_name, compiler_version, engine_name, engine_version, dialect, runtime_profile_hash)`

| Action Family | Expression Subtype | Status | Proving Test Identifier |
| :--- | :--- | :---: | :--- |
| `ActionFamily.MAP_VALUE` | `TRIM` | `VERIFIED` | `test_azure_sql_execution_evidence.py::test_azure_sql_trim_upper_lower_parity` |
| `ActionFamily.MAP_VALUE` | `UPPER` | `VERIFIED` | `test_azure_sql_execution_evidence.py::test_azure_sql_trim_upper_lower_parity` |
| `ActionFamily.MAP_VALUE` | `LOWER` | `VERIFIED` | `test_azure_sql_execution_evidence.py::test_azure_sql_trim_upper_lower_parity` |
| `ActionFamily.CONDITIONAL_REPLACE` | `CONDITIONAL_REPLACE` | `VERIFIED` | `test_azure_sql_execution_evidence.py::test_azure_sql_conditional_replace_parity` |
| `ActionFamily.CLIP` | `CLIP` | `VERIFIED` | `test_azure_sql_execution_evidence.py::test_azure_sql_clip_and_flag_parity` |
| `ActionFamily.FLAG` | `FLAG` | `VERIFIED` | `test_azure_sql_execution_evidence.py::test_azure_sql_clip_and_flag_parity` |
| `ActionFamily.PARSE` | `PARSE_INTEGER` | `VERIFIED` | `test_azure_sql_execution_evidence.py::test_azure_sql_parse_typed_parity` |
| `ActionFamily.PARSE` | `PARSE_CALENDAR_DATE` | `VERIFIED` | `test_azure_sql_execution_evidence.py::test_azure_sql_parse_typed_parity` |
| `ActionFamily.IMPUTE` | `IMPUTE` (Apply-Only) | `VERIFIED` | `test_azure_sql_execution_evidence.py::test_azure_sql_impute_apply_only_parity` |
| `ActionFamily.VALIDATE` | `VALIDATE` (Three-Way) | `VERIFIED` | `test_azure_sql_execution_evidence.py::test_azure_sql_validate_three_way_parity` |

### Default-Blocked Capabilities (`NOT_TESTED`, `execution_eligible = False`)
All capability tuples outside Vertical Slice 1 remain strictly default-blocked:
- Multi-Dataset Joins (`INNER`, `LEFT`, `RIGHT`, `FULL`, `SEMI`, `ANTI`, `CROSS`)
- Multi-Dataset Aggregations & Window Functions
- Cryptographic Hashing, HMAC, and Masking
- Durable Lakehouse / Database Table Publication DDL/DML

---

## 6. Architectural Contracts Implemented & Verified

1. **Clean Separation of Compiler Artifacts from Certification Harness**:
   - Compiler emits pure transformation CTE queries, parameter manifests, and schema expectations.
   - Certification harness script generates isolated `#dhara_input` temporary tables, fixture inserts, mandatory session headers (`SET NOCOUNT ON; SET XACT_ABORT ON; SET ANSI_NULLS ON; SET ANSI_WARNINGS ON; SET ARITHABORT ON;`), query execution, and guaranteed cleanup (`DROP TABLE IF EXISTS #dhara_input;`).
2. **Pre-Image Preservation across CTE Stages**:
   - Deterministic sequential CTE stages (`[source_stage]`, `[step_001_...]`, `[step_002_...]`).
   - Explicit column projections at every stage (eliminating ambiguous `SELECT *` when mutating columns).
   - Shadow columns (`[__orig_<col>]`) preserve unmutated values when earlier steps mutate a column, enabling subsequent steps to reference `ValueVersion.ORIGINAL`.
3. **Three-Way Validation Accounting**:
   - Verification queries emit explicit counts preserving 3-valued boolean logic:
     `candidate_count = pass_count + violation_count + raw_unknown_count`.
4. **Apply-Only Imputation Guardrails**:
   - Static AST safety verification rejects aggregate functions (`AVG`, `SUM`, `PERCENTILE_CONT`, `APPROX_PERCENTILE`, subqueries).
   - Pre-fitted model snapshot hashes verified at compilation time.
   - Divergent fixture test verifies fixed pre-fitted value application.
5. **Granular Parse Subtypes**:
   - Explicit T-SQL casting via `TRY_CAST` (integers, floats, decimals) and `TRY_CONVERT` with ISO 8601 style code 23 (`yyyy-mm-dd`) and 126 (`datetime2`).
6. **Failure Normalization**:
   - Dialect errors categorized into distinct reason codes (`SQL_ARTIFACT_SYNTAX_ERROR`, `COLUMN_NOT_FOUND`, `SNAPSHOT_HASH_MISMATCH`, `DYNAMIC_FIT_PROHIBITED`, `UNSUPPORTED_AZURE_SQL_CAPABILITY`).

---

## 7. Production Blocker Ledger Status (PROD-1 through PROD-5)

Closure of Increment 3.4A advances the verified Azure SQL Database capability inventory under PROD-4, but does not close PROD-4 globally. All five production blockers remain active:

1. **PROD-1 (Execution-Boundary Secret Resolution)**: **ACTIVE BLOCKER**. Vault boundaries, Azure Key Vault token injection, and cryptographic key management remain mock-scoped.
2. **PROD-2 (Execution-Time Authorization Revalidation)**: **ACTIVE BLOCKER**. Entra ID token validation, distributed token freshness, and live step approval re-checking remain deferred.
3. **PROD-3 (Durable Publication Commit Protocol)**: **ACTIVE BLOCKER**. Publication boundary is strictly `IN_MEMORY_REFERENCE`. ACID transactional table commits (Delta Lake, Fabric Lakehouse, durable SQL DDL/DML) remain deferred.
4. **PROD-4 (Cross-Engine Semantic Parity)**: **ACTIVE BLOCKER (BOUNDED PROGRESS)**. Certified exclusively for declared PySpark 3.5.1 and Azure SQL T-SQL Vertical Slice 1 capability tuples. ADF, DuckDB, Fabric Warehouse, and uncertified PySpark/SQL tuples remain strictly default-blocked (`NOT_TESTED`, `execution_eligible = False`).
5. **PROD-5 (Runtime Resource and Expansion Governance)**: **ACTIVE BLOCKER**. Distributed cluster governance, SQL connection pooling, elastic DTU/vCore governance, dynamic query timeouts, and spill controls remain uncertified.

---

## 8. Tracked Evidence Manifest & Checksums

Evidence artifacts are committed and tracked under `docs/certification/evidence/phase_3_increment_3_4/`:

| Artifact Name | Description | SHA-256 Checksum |
| :--- | :--- | :--- |
| `cross_engine_119.log` | Consolidated cross-engine suite execution log (119 passing tests) | `c7a31901e1ce446268bcbad5e12ae39e72c350a9059af844a33008385ed7706e` |
| `reference_baseline_202.log` | Certified Pandas reference baseline suite log (202 passing tests) | `720c2f2fe4f41af0e011987969f1625a0bf24310470d873dfb0f540b8d7226f4` |
| `business_rules_69.log` | Core business rules and planning bridge log (69 passing tests) | `77cfbb1716bd97236ed28ed6ec06c47b9377f10dc5cb37e1ed637f2e604358df` |
| `collection_cross_engine.txt` | Complete test collection inventory (119 collected tests) | `ca12ae811bd94e9042d66eedb0ae3fc5d9fe055c9f854a70340c650c0a7cd0c3` |
| `environment.txt` | Python 3.13, PySpark 3.5.1, Pandas, Windows 11 host profile | `7e4569b83ec9ef5cd62161ab69b843ec5fea8ba95c78df0d83a0332a5663ccdf` |
| `evidence_bundles.json` | 6 complete structured evidence bundles with hashes and parity results | `9b597e1bcdb5b5973a0c0d7a97faa0ec07ceaecb090dd7e9d3fa0a482e0385cf` |
| `checksums.sha256` | Complete SHA-256 manifest of all evidence files | Manifest verified |
