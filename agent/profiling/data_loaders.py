from __future__ import annotations
import os
import json
import hashlib
import logging
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional
import pandas as pd

from agent.profiling.constants import *

logger = logging.getLogger("agent.profiling.data_loaders")

def count_file_lines(fp: str) -> int:
    try:
        with open(fp, "rb") as f:
            lines = 0
            buf_size = 1024 * 1024
            read_f = f.raw.read
            buf = read_f(buf_size)
            while buf:
                lines += buf.count(b"\n")
                buf = read_f(buf_size)
            return lines
    except Exception:
        return 0

def load_csv_sampled(fp: str, sep: str = ",", max_rows: Optional[int] = None) -> pd.DataFrame:
    if not max_rows:
        return pd.read_csv(fp, sep=sep, low_memory=False)
    
    total_lines = count_file_lines(fp)
    if total_lines <= max_rows:
        return pd.read_csv(fp, sep=sep, low_memory=False)
        
    chunk_size = 50000
    sample_rate = max_rows / max(1, total_lines)
    chunks = []
    
    try:
        for chunk in pd.read_csv(fp, sep=sep, chunksize=chunk_size, low_memory=False):
            target_n = int(round(len(chunk) * sample_rate))
            if target_n > 0:
                sampled_chunk = chunk.sample(n=min(len(chunk), target_n), random_state=42)
                chunks.append(sampled_chunk)
        if chunks:
            df = pd.concat(chunks, ignore_index=True)
            if len(df) > max_rows:
                df = df.sample(n=max_rows, random_state=42).reset_index(drop=True)
            return df
        else:
            return pd.read_csv(fp, sep=sep, nrows=max_rows)
    except Exception:
        try:
            return pd.read_csv(fp, sep=sep, nrows=max_rows)
        except Exception:
            return pd.DataFrame()

def _sql_location_key_prefix(loc: Dict[str, Any], conn: Dict[str, Any], db_index: int, multi_db: bool) -> str:
    """Prefix for dataset keys when multiple database locations are configured."""
    if not multi_db:
        return ""
    for k in ("id", "label", "name"):
        v = loc.get(k)
        if v and str(v).strip():
            s = re.sub(r"[^\w\-]+", "_", str(v).strip())[:48].strip("_")
            if s:
                return s + "__"
    db = str(conn.get("database") or conn.get("Database") or f"db{db_index}")
    srv = str(conn.get("server") or conn.get("Server") or "")
    h = hashlib.md5(f"{srv}|{db}".encode("utf-8")).hexdigest()[:8]
    tail = re.sub(r"[^\w]+", "_", db)[:24].strip("_") or "db"
    return f"{tail}_{h}__"

def load_sql_datasets(
    connection_cfg: Dict[str, Any],
    dataset_key_prefix: str = "",
    max_rows: Optional[int] = None,
    db_connectors_by_dataset: Optional[Dict[str, Tuple[Any, str]]] = None,
    only_tables: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """
    Loads all discovered tables from Azure SQL using the provided connector configuration.
    Returns a dict: { "<schema>.<table>": DataFrame, ... } or prefixed keys if dataset_key_prefix set.
    """
    if AzureSQLPythonNetConnector is None:
        print("[INFO] AzureSQLPythonNetConnector not available, skipping SQL datasets")
        return {}

    p = (dataset_key_prefix or "").strip()
    if p and not p.endswith("__"):
        p = p + "__"

    datasets: Dict[str, pd.DataFrame] = {}
    try:
        connector = AzureSQLPythonNetConnector(connection_cfg)
        tables = connector.discover_tables()

        if only_tables is not None:
            allowed_set = {t.lower() for t in only_tables}
            filtered_tables = []
            for t in tables:
                key = f"{p}{t}" if p else t
                if key.lower() in allowed_set:
                    filtered_tables.append(t)
            tables = filtered_tables

        for table in tables:
            key = f"{p}{table}" if p else table
            try:
                datasets[key] = connector.load_table(table, max_rows=max_rows)
                if db_connectors_by_dataset is not None:
                    db_connectors_by_dataset[key] = (connector, table)
            except Exception as e:
                print(f"[ERROR] Failed to load table {table}: {e}")
    except Exception as e:
        print(f"[INFO] Failed to connect to SQL database: {e}")

    return datasets

def _find_record_path(obj: Any, path: Optional[List[str]] = None, max_depth: int = 4) -> Optional[List[str]]:
    """Find nested list-of-dicts path for record_path (e.g., ['departments','employees'])."""
    if path is None:
        path = []
    if max_depth < 0:
        return None
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            return path
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            rp = _find_record_path(v, path + [k], max_depth - 1)
            if rp:
                return rp
    return None

def _json_deep_flatten(data: Any) -> pd.DataFrame:
    from pandas import json_normalize

    if isinstance(data, list):
        if not data:
            return pd.DataFrame()
        if isinstance(data[0], dict):
            return json_normalize(data, max_level=1)
        return pd.DataFrame({"value": data})

    if not isinstance(data, dict):
        return pd.DataFrame([{"value": data}])

    record_path = _find_record_path(data, max_depth=4)
    if not record_path:
        return json_normalize(data, max_level=1)

    meta_keys: List[str] = []

    def collect_scalars(d: Dict[str, Any]) -> None:
        for k, v in d.items():
            if not isinstance(v, (list, dict)):
                if k not in meta_keys:
                    meta_keys.append(k)

    parent: Any = data
    for k in record_path[:-1]:
        if isinstance(parent, dict):
            collect_scalars(parent)
            parent = parent.get(k, {})
        else:
            break

    try:
        return json_normalize(
            data,
            record_path=record_path,
            meta=meta_keys if meta_keys else None,
            errors="ignore"
        )
    except Exception:
        return json_normalize(data, max_level=1)

def _load_json_to_df(path: str, max_rows: Optional[int] = None) -> pd.DataFrame:
    if path.lower().endswith(".jsonl"):
        if max_rows is not None:
            import random
            reservoir = []
            count = 0
            rng = random.Random(42)
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if len(reservoir) < max_rows:
                        reservoir.append(line)
                    else:
                        j = rng.randint(0, count)
                        if j < max_rows:
                            reservoir[j] = line
                    count += 1
            rows = []
            for line in reservoir:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    rows.append({"value": line})
        else:
            rows = []
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        rows.append({"value": line})
        if not rows:
            return pd.DataFrame()
        return pd.json_normalize(rows, max_level=1)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return _json_deep_flatten(data)

def _xml_to_df_exploded(path: str) -> pd.DataFrame:
    root = ET.parse(path).getroot()
    nodes = list(root)
    if not nodes:
        return pd.DataFrame()

    if len(set(n.tag for n in nodes)) == 1:
        records: List[Dict[str, Any]] = []
        for node in nodes:
            base: Dict[str, Any] = {}
            containers: List[ET.Element] = []
            for child in node:
                g = list(child)
                if g:
                    containers.append(child)
                else:
                    base[child.tag] = child.text

            exploded = False
            for container in containers:
                items = list(container)
                if not items:
                    continue
                if len({c.tag for c in items}) == 1:
                    exploded = True
                    for item in items:
                        row = dict(base)
                        for sub in item:
                            row[f"{container.tag}_{sub.tag}"] = sub.text
                        records.append(row)
            if not exploded:
                records.append(base)

        return pd.DataFrame(records)

    return pd.DataFrame([{c.tag: c.text for c in node} for node in nodes])

def load_file_datasets(
    path: str,
    max_rows: Optional[int] = None,
    only_files: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """
    Reads supported files from a local folder and returns a dict: { "<file_name>": DataFrame }
    """
    data: Dict[str, pd.DataFrame] = {}

    if not os.path.isdir(path):
        print("[INFO] Filesystem path not found:", path)
        return data

    files_to_load = os.listdir(path)
    if only_files is not None:
        allowed_set = {f.lower() for f in only_files}
        files_to_load = [f for f in files_to_load if f.lower() in allowed_set]

    for file in files_to_load:
        fp = os.path.join(path, file)
        if not os.path.isfile(fp):
            continue

        try:
            low = file.lower()
            if low.endswith(".csv"):
                data[file] = load_csv_sampled(fp, sep=",", max_rows=max_rows)
            elif low.endswith(".tsv"):
                data[file] = load_csv_sampled(fp, sep="\t", max_rows=max_rows)
            elif low.endswith(".json") or low.endswith(".jsonl"):
                data[file] = _load_json_to_df(fp, max_rows=max_rows)
            elif low.endswith(".xml"):
                data[file] = _xml_to_df_exploded(fp) # XML is harder to sample early
            elif low.endswith(".parquet"):
                # Parquet can be sampled early if we use a different engine, but for now:
                data[file] = pd.read_parquet(fp).head(max_rows) if max_rows else pd.read_parquet(fp)
            elif low.endswith(".xlsx"):
                data[file] = pd.read_excel(fp, engine="openpyxl", nrows=max_rows)
            elif low.endswith(".html") or low.endswith(".htm"):
                tables = pd.read_html(fp)
                data[file] = tables[0] if tables else pd.DataFrame()
        except Exception as e:
            print(f"[ERROR] Reading {file}: {e}")

    return data

