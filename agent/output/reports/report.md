## Assessment Report of `factory.json`

> [!NOTE]
> **factory.json**: Analysis performed on 100% of rows.

### Datasets (summary)

| Dataset | Source | Rows | Cols | Issues | High | Med | Low |
|---|---|---:|---:|---:|---:|---:|---:|
| `factory.json` | Filesystem (abfss://04a585ce-f0ec-4a3a-9db2-e237711fd9e7@onelake.dfs.fabric.microsoft.com/abd8c72d-b5aa-4b58-83bd-fb97587de47e/Files/raw/factory_json.json) | 1000 | 5 | 2 | 1 | 1 | 0 |

### Columns (per dataset)


#### `factory.json`

| Column | dtype | null% | unique | semantic type | candidate_pk |
|---|---|---:|---:|---|:---:|
| `plant` | `object` | 0.0% | 4 | `categorical` | ✗ |
| `product_id` | `object` | 0.0% | 100 | `categorical` | ✗ |
| `production_id` | `object` | 0.0% | 1000 | `categorical` | ✓ |
| `sales_id` | `object` | 0.0% | 1000 | `categorical` | ✓ |
| `units_produced` | `int64` | 0.0% | 310 | `numeric_id` | ✗ |

### Top issues (per dataset)

#### `factory.json`

| Severity | Type | Column | Count | Message | Recommendation |
|:--:|---|---|---:|---|---|
| medium | `numeric_outliers_iqr` | `units_produced` | 44 | 44 IQR outlier(s) outside [-66.38, 350.62] | Investigate values beyond 1.5x IQR; check if they represent extreme cases, fraud, or data entry errors. |
| high | `numeric_outliers_zscore` | `units_produced` | 2 | 2 extreme outlier(s) beyond 4σ from mean (154.41 ± 88.59) | Investigate extreme z-score outliers (>4 std devs); likely data entry errors, test records, or fraud signals. |

### Relationships

| Dataset A | Column A | Dataset B | Column B | Cardinality | Shared keys |
|---|---|---|---|---|---:|
| _none_ |  | _none_ |  |  |  |

### Global issues


#### Cross-table row issues (orphan keys)

- (none)

#### Relationship warnings

- (none)

### LLM Cleaning Recommendations


| Priority | Dataset | Column | Severity | Suggested Fix | Risk |
|---|---|---|---|---|---|
| 1 | `factory.json` | `units_produced` | high | Investigate and potentially quarantine the extreme outlier values. | High risk of incorrect data affecting analysis and decision-making. |
| 2 | `factory.json` | `units_produced` | medium | Investigate values beyond 1.5x IQR; check if they represent extreme cases, fraud, or data entry errors. | Medium risk of misleading data affecting operational decisions. |

### What might still be missed

- Unmodelled business rules not captured in metadata manifest or cross-field rules.
- Drift in tails of distributions when only moments/null/distinct are snapshotted.
- Nested payload loss if raw JSON/XML is flattened without registry entries.
- GX expectations are sampled on very large tables (see GX logs).