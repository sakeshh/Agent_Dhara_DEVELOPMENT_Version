# Agent Dhara - Data Quality Engine Overhaul: Complete Implementation Plan

This document details the architecture, library selections, rationales, and exact code changes implemented to modernize and upgrade the Data Quality (DQ) Engine in Agent Dhara.

---

## 1. Libraries and Rationale

| Fix | Feature | Library / Tool | Rationale |
| :--- | :--- | :--- | :--- |
| **Fix 2** | Phone Validation | `phonenumbers` | Port of Google's `libphonenumber`. Validates phone numbers against real international plans (length, country codes, existence). |
| **Fix 3 & 4** | Date Inconsistency & Ordering | `python-dateutil` | Parses dates with custom format detection (`ParserInfo`). Native `pd.to_datetime` silently homogenizes dates, losing format details. |
| **Fix 5** | Near-Duplicate Detection | `rapidfuzz` | Pure C extension. 10-50x faster than `difflib.SequenceMatcher`. Safely compares thousands of rows via block grouping. |
| **Fix 6** | Multivariate Outliers | `scikit-learn` (`IsolationForest`) | Machine learning anomaly detection. Catches statistical anomalies across multiple columns simultaneously where IQR per-column fails. |

---

## 2. Comprehensive Breakdown of the 11 Fixes

### Fix 1: Primitive Semantic Type Detection
* **Why it matters**: The old semantic type detection was limited to a few simple types, failing to recognize critical fields like UUIDs, IP addresses, URLs, phone numbers without traditional naming prefixes, and boolean-like fields.
* **Implementation**: We replaced `detect_semantic_type` in `agent/intelligent_data_assessment.py` to scan a 200-row sample and match against regular expressions for UUIDs, IPv4/IPv6, URLs, and emails. It also performs check-filtering for boolean-like representations (e.g. true/false, yes/no, y/n, 1/0) and checks column name hints for phone columns.

### Fix 2: Phone Validation with `phonenumbers`
* **Why it matters**: The old regex-based check (`PHONE_RE`) was extremely weak, letting invalid/non-existent phone numbers (e.g., `+1234`, `+0000000000`) pass.
* **Implementation**: Created helper functions `_validate_phone_phonenumbers()` and `_detect_phone_formats()`. If a column is classified as a phone column, it checks validity via Google's library and flags format inconsistencies (e.g. mix of E.164 and national formats).

### Fix 3: Date Format Inconsistency with `dateutil`
* **Why it matters**: Native pandas `pd.to_datetime` silently parses and homogenizes dates, losing information about which specific formats were used (e.g., mixing `DD/MM/YYYY` and `MM/DD/YYYY`), which leads to day/month swaps.
* **Implementation**: Created `_detect_date_format()` with regex pattern-matching. We compute the distribution of date formats in a column sample and flag date format inconsistencies (if multiple formats exist) or high-severity format ambiguities (DD/MM vs MM/DD ambiguity).

### Fix 4: Cross-Column Date Ordering
* **Why it matters**: Chronological violations (e.g., `start_date > end_date`, `created_at > updated_at`) are very common real-world data quality issues that corrupt downstream analytics.
* **Implementation**: Implemented `check_cross_column_date_ordering()` to find pairs of date columns using name heuristics (e.g., `start` vs `end`, `created` vs `updated`) and flag rows where order constraints are violated.

### Fix 5: Near-Duplicate Row Detection with `rapidfuzz`
* **Why it matters**: Exact duplicate check (`df.duplicated()`) only catches byte-by-byte matches. Typos, spelling variations, extra spacing, or case differences hide duplicate records from basic deduplication.
* **Implementation**: Implemented `detect_near_duplicate_rows()`. To avoid the $O(n^2)$ computational explosion on large datasets, it uses block-based matching (grouping rows by a blocking key composed of the first 3 characters of the most informative text column) and computes token-based string similarity.

### Fix 6: Multivariate Outlier Detection with `sklearn.IsolationForest`
* **Why it matters**: Single-column IQR outliers fail to detect rows where all individual values are within normal bounds, but the combination of values is statistically highly improbable (e.g., `age=25` and `salary=500000` and `experience=1`).
* **Implementation**: Implemented `detect_multivariate_outliers()`. It scales numeric data using `RobustScaler` (which is less sensitive to extreme outliers than standard scaling) and applies `IsolationForest` to identify anomalous rows.

### Fix 7: Intra-Dataset Self-Referencing FK Check
* **Why it matters**: Parent-child relationships inside the same table (e.g., `manager_id` referencing `employee_id` in an employees table) can have orphan references where the child ID refers to a non-existent parent.
* **Implementation**: Implemented `check_intra_dataset_fk()`. It scans column names ending in `_id`, `_ref`, `_fk`, `_key` that are not the primary key, and verifies that all non-null values exist in the table's primary key column.

### Fix 8: Schema Drift Detection Across Datasets
* **Why it matters**: Columns with the same name across different datasets (e.g. `status` or `created_at`) might have different data types (string vs integer, or datetime vs string) in different sources, causing schema drift and pipeline failures.
* **Implementation**: Implemented `_compare_column_schemas()` and integrated it into `detect_global_issues()` to alert users when same-named columns across tables have type mismatch discrepancies.

### Fix 9: False-Positive Orphan FK (Non-ID Columns)
* **Why it matters**: Checking all shared columns for referential integrity causes false-positive orphan FK alerts on columns like `status`, `country`, or `category` which share names but are not actual foreign keys.
* **Implementation**: Implemented `_is_id_like_column()` with identifier name hints (`id`, `code`, `key`, `ref`, `fk`, `pk`, `uuid`, etc.) to filter and restrict orphan FK validation only to true ID/reference columns.

### Fix 10: Sampled Row Indexes: Mark as Estimated
* **Why it matters**: When datasets are extremely large, profiling runs on a sample. In such cases, reporting sample-relative indices (e.g., row 42 in the sample is not row 42 in the database) causes down-stream cleaning steps to act on the wrong rows.
* **Implementation**: Implemented `_finalize_sampled_issue()` to scale the estimated issue count back to the full dataset size, clear unreliable exact row index lists, and append a clear `[ESTIMATED]` tag to the issue message.

### Fix 11: Functional Dependency Validation
* **Why it matters**: Functional dependencies (e.g., `zip_code` must uniquely determine `city`) represent critical business constraints. Violations (e.g. same zip code mapping to two different cities) are common address/reference data entry errors.
* **Implementation**: Implemented `check_functional_dependency_violations()` which reads functional dependency rules from `dq_thresholds.yaml` and flags rows that violate them.

---

## 3. Configuration & Dependency Updates

### `requirements.txt`
Dependencies added for advanced data assessment:
```text
# Phone number validation
phonenumbers>=8.13.0

# Near-duplicate detection via token-level fuzzy matching
rapidfuzz>=3.0.0

# Multi-format date parsing
python-dateutil>=2.8.2

# Machine learning for outliers
scikit-learn>=1.0.0
```

### `config/dq_thresholds.yaml`
New rules and default thresholds structured for the new checks:
```yaml
# Functional dependency rules
functional_dependencies: []

# Near duplicate detection configuration
near_duplicate:
  enabled: true
  threshold: 0.92
  max_rows: 50000

# Multivariate outlier detection configuration
multivariate_outliers:
  enabled: true
  contamination: 0.02

# Date validation ordering and ambiguity configuration
date_checks:
  check_ordering: true
  flag_ambiguous_format: true
```
