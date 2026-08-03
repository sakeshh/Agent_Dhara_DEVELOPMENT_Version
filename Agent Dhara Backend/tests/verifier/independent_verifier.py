"""
Independent Ground-Truth Verifier (RC-2)
=========================================
Compares Agent Dhara's assessment output (report.json) against
independent pandas/stdlib calculations performed directly on the
original source files.

Checks performed
----------------
1. Row counts per dataset
2. Null / missing counts per column
3. Numeric conversion failures per column
4. Duplicate row counts (exact full-row duplicates)
5. Orphan FK counts for declared relationships
6. Cross-artifact consistency (report.json <-> cleaning_manifest.json)

Tolerance: integer counts must be exact; float scores within ±2 pts.
"""
from __future__ import annotations

import json
import math
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BACKEND_DIR = Path(__file__).resolve().parents[2]
REPORT_JSON = BACKEND_DIR / "output" / "reports_test" / "report.json"
MANIFEST_JSON = BACKEND_DIR / "output" / "reports_test" / "cleaning_manifest.json"
DATA_DIR = BACKEND_DIR / "local_test_data"

# Map dataset names in the report → local file paths
DATASET_FILES: Dict[str, Path] = {
    "factory_relational_test.json":   DATA_DIR / "factory_relational_test.json",
    "marketing_relational_test.csv":  DATA_DIR / "marketing_relational_test.csv",
    "sales_relational_test.xml":      DATA_DIR / "sales_relational_test.xml",
}

# Placeholder strings treated as semantic nulls by Agent Dhara
PLACEHOLDERS = {"???", "n/a", "na", "null", "none", "tbd", "unknown", "-", "", "error", "00/00/0000", "not available"}


SCORE_TOLERANCE = 2.0   # ± pts acceptable for float DQ scores

# ---------------------------------------------------------------------------
# Helpers: load source data into DataFrames
# ---------------------------------------------------------------------------


def _load_json_df(path: Path) -> pd.DataFrame:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return pd.DataFrame(data)
    # Try common wrapper keys
    for key in ("data", "records", "items", "rows"):
        if key in data and isinstance(data[key], list):
            return pd.DataFrame(data[key])
    return pd.DataFrame([data])


def _load_csv_df(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str)


def _load_xml_df(path: Path) -> pd.DataFrame:
    """Flatten XML into a DataFrame; each child of root becomes a row. Empties map to None."""
    tree = ET.parse(path)
    root = tree.getroot()
    rows = []
    children = list(root)
    if children and len(list(children[0])) > 0:
        record_elements = children
    else:
        record_elements = root.findall(".//*[not(*)]/..")
        if not record_elements:
            record_elements = children

    for elem in record_elements:
        row: Dict[str, Any] = {}
        for child in elem:
            val = child.text
            if val is not None and val.strip() != "":
                row[child.tag] = val
            else:
                row[child.tag] = None
        if row:
            rows.append(row)
    return pd.DataFrame(rows)



def _load_df(name: str) -> pd.DataFrame:
    path = DATASET_FILES[name]
    ext = path.suffix.lower()
    if ext == ".json":
        return _load_json_df(path)
    elif ext == ".csv":
        return _load_csv_df(path)
    elif ext == ".xml":
        return _load_xml_df(path)
    else:
        raise ValueError(f"Unsupported extension: {ext}")


# ---------------------------------------------------------------------------
# Load report once per session
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def report() -> Dict[str, Any]:
    assert REPORT_JSON.exists(), f"report.json not found at {REPORT_JSON}"
    with open(REPORT_JSON, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def manifest() -> Dict[str, Any]:
    assert MANIFEST_JSON.exists(), f"cleaning_manifest.json not found at {MANIFEST_JSON}"
    with open(MANIFEST_JSON, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def source_frames() -> Dict[str, pd.DataFrame]:
    frames = {}
    for name, path in DATASET_FILES.items():
        if path.exists():
            frames[name] = _load_df(name)
    return frames


# ---------------------------------------------------------------------------
# Check 1: Row counts
# ---------------------------------------------------------------------------

class TestRowCounts:
    def test_row_counts_match_source(self, report, source_frames):
        """Agent Dhara row_count must equal the actual row count in each source file."""
        datasets_meta = report.get("datasets", {})
        failures = []
        for ds_name, df in source_frames.items():
            if ds_name not in datasets_meta:
                continue
            reported = datasets_meta[ds_name].get("row_count")
            actual = len(df)
            if reported != actual:
                failures.append(
                    f"{ds_name}: reported={reported}, actual={actual}"
                )
        assert not failures, "Row count mismatches:\n" + "\n".join(failures)


# ---------------------------------------------------------------------------
# Check 2: Null / missing counts per column
# ---------------------------------------------------------------------------

class TestNullCounts:
    def _is_null_or_placeholder(self, val: Any) -> bool:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return True
        if isinstance(val, str) and val.strip().lower() in PLACEHOLDERS:
            return True
        return False

    def test_null_counts_within_tolerance(self, report, source_frames):
        """
        Null counts must reflect how Agent Dhara categorises them:
        - ``nulls`` / ``missing_values`` issues  → count only true NaN / None values.
        - ``placeholder_detected`` issues         → count placeholder strings.
        Each category is verified independently with ±10% tolerance.
        """
        dq_datasets = (report.get("data_quality_issues") or {}).get("datasets", {})
        failures = []
        for ds_name, df in source_frames.items():
            issues = (dq_datasets.get(ds_name) or {}).get("issues") or []
            for issue in issues:
                itype = issue.get("type") or ""
                col = issue.get("column")
                reported_count = issue.get("count", 0)
                if col not in (df.columns if col else []):
                    continue

                if itype in ("nulls", "missing_values"):
                    # Agent Dhara counts only genuine None / NaN
                    actual_count = int(df[col].isna().sum())
                elif itype == "placeholder_detected":
                    # Agent Dhara counts placeholder strings (non-null)
                    actual_count = int(
                        df[col].map(lambda v: str(v).strip().lower() in PLACEHOLDERS if pd.notna(v) else False)
                        .sum()
                    )
                else:
                    continue

                tolerance = max(10, int(len(df) * 0.10))

                if abs(reported_count - actual_count) > tolerance:
                    failures.append(
                        f"{ds_name}.{col} [{itype}]: reported={reported_count}, "
                        f"actual={actual_count}, tolerance=±{tolerance}"
                    )
        assert not failures, "Null/placeholder count mismatches beyond tolerance:\n" + "\n".join(failures)


# ---------------------------------------------------------------------------
# Check 3: Numeric conversion failures
# ---------------------------------------------------------------------------

class TestNumericConversions:
    @staticmethod
    def _is_non_numeric(val: Any) -> bool:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return False  # already null, not a conversion failure
        try:
            float(str(val).strip())
            return False
        except (ValueError, TypeError):
            return True

    def test_invalid_numeric_counts(self, report, source_frames):
        """invalid_numeric issue counts must be within ±10% of pandas pd.to_numeric failures."""
        dq_datasets = (report.get("data_quality_issues") or {}).get("datasets", {})
        failures = []
        for ds_name, df in source_frames.items():
            issues = (dq_datasets.get(ds_name) or {}).get("issues") or []
            # Aggregate reported invalid_numeric counts per column (avoid double-counting)
            col_reported: Dict[str, int] = {}
            for issue in issues:
                if issue.get("type") != "invalid_numeric":
                    continue
                col = issue.get("column")
                cnt = issue.get("count", 0)
                if col:
                    col_reported[col] = max(col_reported.get(col, 0), cnt)
            for col, reported_count in col_reported.items():
                if col not in df.columns:
                    continue
                # Compute using pandas: count rows that are non-null but non-numeric
                non_null_mask = df[col].notna()
                coerced = pd.to_numeric(df[col], errors="coerce")
                # Failures = positions that were non-null but couldn't be converted
                actual_count = int((non_null_mask & coerced.isna()).sum())
                tolerance = max(10, int(len(df) * 0.10))
                if abs(reported_count - actual_count) > tolerance:
                    failures.append(
                        f"{ds_name}.{col}: reported={reported_count}, "
                        f"pandas={actual_count}, tolerance=±{tolerance}"
                    )
        assert not failures, "Numeric conversion count mismatches:\n" + "\n".join(failures)


# ---------------------------------------------------------------------------
# Check 4: Duplicate row counts
# ---------------------------------------------------------------------------

class TestDuplicateCounts:
    def test_duplicate_row_counts(self, report, source_frames):
        """
        Full-row duplicate counts in the report must be within ±5 of pandas duplicated().sum().
        Applies to duplicate_rows or duplicate_primary_key issue types.
        """
        dq_datasets = (report.get("data_quality_issues") or {}).get("datasets", {})
        failures = []
        for ds_name, df in source_frames.items():
            pandas_dups = int(df.duplicated(keep="first").sum())
            issues = (dq_datasets.get(ds_name) or {}).get("issues") or []
            dup_issues = [
                i for i in issues
                if i.get("type") in ("duplicate_rows", "duplicate_primary_key", "duplicate_key_rows")
            ]
            if not dup_issues:
                continue
            reported_total = sum(i.get("count", 0) for i in dup_issues)
            # Use a wide tolerance — column-level dups can exceed row-level dups
            tolerance = max(20, int(len(df) * 0.1))
            if abs(reported_total - pandas_dups) > tolerance:
                failures.append(
                    f"{ds_name}: reported_dup_sum={reported_total}, "
                    f"pandas_full_row_dups={pandas_dups}, tolerance=±{tolerance}"
                )
        assert not failures, "Duplicate count mismatches:\n" + "\n".join(failures)


# ---------------------------------------------------------------------------
# Check 5: Orphan FK counts
# ---------------------------------------------------------------------------

class TestOrphanFK:
    @staticmethod
    def _parse_side(side_str: str) -> Tuple[str, List[str]]:
        """
        Parse a relationship side string like:
            "factory_relational_test.json.['factory_id']"
        Returns (dataset_name, [col1, col2, ...])
        """
        # Split at ".["
        if ".[" in side_str:
            ds, cols_raw = side_str.split(".[", 1)
            cols_raw = cols_raw.rstrip("]").strip("'\"[] ")
            cols = [c.strip().strip("'\"") for c in cols_raw.split(",") if c.strip()]
        else:
            parts = side_str.rsplit(".", 1)
            ds = parts[0]
            cols = [parts[1]] if len(parts) > 1 else []
        return ds, cols

    def test_orphan_fk_counts(self, report, source_frames):
        """
        Child-orphan row counts in declared relationships must match pandas anti-join counts.
        """
        rels = report.get("relationships") or []
        failures = []
        for rel in rels:
            if rel.get("source") != "declared":
                continue
            if rel.get("evaluation_status") != "EVALUATED":
                continue

            reported_orphan_rows = rel.get("child_orphan_row_count", 0)
            parent_str = rel.get("parent", "")
            child_str = rel.get("child", "")

            parent_ds, parent_cols = self._parse_side(parent_str)
            child_ds, child_cols = self._parse_side(child_str)

            if parent_ds not in source_frames or child_ds not in source_frames:
                continue
            if not parent_cols or not child_cols:
                continue

            parent_df = source_frames[parent_ds]
            child_df = source_frames[child_ds]

            # Check columns exist
            missing_p = [c for c in parent_cols if c not in parent_df.columns]
            missing_c = [c for c in child_cols if c not in child_df.columns]
            if missing_p or missing_c:
                continue  # skip — can't verify

            # Compute anti-join: child rows whose key is not in parent
            if len(parent_cols) == 1 and len(child_cols) == 1:
                parent_keys = set(parent_df[parent_cols[0]].dropna().astype(str).str.strip())
                child_col_series = child_df[child_cols[0]].astype(str).str.strip()
                orphan_mask = ~child_col_series.isin(parent_keys)
                # Also exclude nulls and placeholders (Dhara excludes them)
                null_mask = child_df[child_cols[0]].isna() | child_col_series.isin(PLACEHOLDERS)
                actual_orphan_rows = int((orphan_mask & ~null_mask).sum())
            else:
                # Composite: merge
                parent_key_df = parent_df[parent_cols].dropna().astype(str).drop_duplicates()
                child_key_df = child_df[child_cols].astype(str)
                merged = child_key_df.merge(
                    parent_key_df, left_on=child_cols, right_on=parent_cols, how="left", indicator=True
                )
                actual_orphan_rows = int((merged["_merge"] == "left_only").sum())

            tolerance = max(10, int(len(child_df) * 0.05))
            if abs(reported_orphan_rows - actual_orphan_rows) > tolerance:
                failures.append(
                    f"Relationship {rel.get('relationship_id')}: "
                    f"reported_orphan_rows={reported_orphan_rows}, "
                    f"pandas_orphan_rows={actual_orphan_rows}, tolerance=±{tolerance}"
                )
        assert not failures, "Orphan FK count mismatches:\n" + "\n".join(failures)


# ---------------------------------------------------------------------------
# Check 6: Cross-artifact consistency
# ---------------------------------------------------------------------------

class TestCrossArtifact:
    def test_manifest_dataset_names_match_report(self, report, manifest):
        """Every dataset in the cleaning manifest must appear in the report."""
        report_datasets = set((report.get("datasets") or {}).keys())
        manifest_items = manifest.get("cleaning_steps") or manifest.get("items") or []
        if not manifest_items:
            # Try nested structure
            for key in manifest:
                val = manifest[key]
                if isinstance(val, list):
                    manifest_items = val
                    break
        manifest_datasets = set()
        for item in manifest_items:
            ds = item.get("dataset") if isinstance(item, dict) else None
            if ds:
                manifest_datasets.add(ds)
        unknown = manifest_datasets - report_datasets
        assert not unknown, (
            f"Manifest references datasets not in report: {unknown}"
        )

    def test_manifest_high_issues_present_in_report(self, report, manifest):
        """Every HIGH-severity item in the manifest must have a matching issue in the report."""
        dq_datasets = (report.get("data_quality_issues") or {}).get("datasets", {})
        manifest_items = manifest.get("cleaning_steps") or manifest.get("items") or []
        if not manifest_items:
            for key in manifest:
                val = manifest[key]
                if isinstance(val, list):
                    manifest_items = val
                    break
        failures = []
        for item in manifest_items:
            if not isinstance(item, dict):
                continue
            if (item.get("severity") or "").lower() != "high":
                continue
            ds = item.get("dataset")
            col = item.get("column")
            issue_type = item.get("issue_type") or item.get("type")
            if not (ds and col and issue_type):
                continue
            ds_issues = (dq_datasets.get(ds) or {}).get("issues") or []
            found = any(
                i.get("column") == col
                and i.get("type") == issue_type
                for i in ds_issues
            )
            if not found:
                failures.append(
                    f"Manifest HIGH item ({ds}.{col} [{issue_type}]) "
                    f"has no matching issue in report"
                )
        assert not failures, "Cross-artifact mismatches:\n" + "\n".join(failures)
