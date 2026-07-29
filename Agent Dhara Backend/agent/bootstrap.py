# Bootstrapping, command-line parsing, and environmental validation.
from __future__ import annotations
import os
import sys
import json
import logging
import argparse
import time
from typing import Dict, Any, Tuple, List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Load local .env automatically
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

if load_dotenv is not None:
    try:
        load_dotenv(os.path.join(PROJECT_DIR, ".env"), override=False)
    except Exception:
        pass

# Force Azure CLI path to PATH environment variable if not already present
for _az_dir in [r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin", r"C:\Program Files (x86)\Microsoft SDKs\Azure\CLI2\wbin"]:
    if os.path.isdir(_az_dir) and _az_dir not in os.environ.get("PATH", ""):
        os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + _az_dir

try:
    from agent.intelligent_data_assessment import load_and_profile, load_dq_thresholds
except ImportError:
    try:
        ida_path = Path(PROJECT_DIR) / "agent" / "intelligent_data_assessment.py"
        if not ida_path.is_file():
            ida_path = Path(PROJECT_DIR) / "intelligent_data_assessment.py"
        import importlib.util
        spec = importlib.util.spec_from_file_location("ida_dyn", ida_path)
        ida = importlib.util.module_from_spec(spec)
        if spec and spec.loader:
            spec.loader.exec_module(ida)
        load_and_profile = ida.load_and_profile
        load_dq_thresholds = getattr(ida, "load_dq_thresholds", lambda p: {})
    except Exception as e:
        logger.critical("Could not import intelligent_data_assessment: %s", e)
        sys.exit(1)

try:
    import yaml
except Exception:
    yaml = None

try:
    from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector
except Exception as e:
    AzureSQLPythonNetConnector = None

try:
    from connectors.azure_blob_storage import AzureBlobStorageConnector
    AZURE_BLOB_AVAILABLE = True
except Exception as e:
    AZURE_BLOB_AVAILABLE = False

from agent.service_layer import (
    to_json_safe, split_schema_table, _quote_two_part_name_fallback,
    print_schema_info_schema, print_schema_top0, _fmt_pct,
    _endpoint_to_dataset, _result_for_datasets, _per_dataset_dir_name,
    _source_root_to_folder_name, _attach_run_metadata_and_suggestions,
    _brief_issue_line, _dtype_with_inference, build_dq_scorecard,
    build_cleaning_manifest, build_markdown_report, build_html_report,
    engine_to_pf_dq, evaluate_dq_rules_inline, get_azure_blob_connector_for_output,
    get_azure_blob_connector_for_assessment, _azure_blob_location_label,
    load_all_assessment_blob_datasets, upload_output_to_azure,
    upload_data_to_azure
)

def _doctor_env_hint() -> None:
    """
    Print a short preflight summary of env/config that commonly breaks Azure access.
    This intentionally avoids printing secret values.
    """
    keys = [
        "AZURE_STORAGE_CONNECTION_STRING",
        "AZURE_STORAGE_ACCOUNT_NAME",
        "AZURE_STORAGE_ACCOUNT_KEY",
        "AZURE_ASSESSMENT_CONTAINER",
        "AZURE_OUTPUT_CONTAINER",
        "AZURE_SQL_SERVER",
        "AZURE_SQL_DATABASE",
        "AZURE_SQL_USERNAME",
        "AZURE_SQL_PASSWORD",
    ]
    present = {k: (True if os.environ.get(k) else False) for k in keys}
    missing = [k for k, ok in present.items() if not ok]
    if missing:
        logger.warning(
            "Azure env preflight: missing %d var(s): %s. "
            "Tip: create '%s' and set vars there (or set in current shell).",
            len(missing),
            ", ".join(missing),
            os.path.join(PROJECT_DIR, ".env"),
        )
    else:
        logger.info("Azure env preflight: all required vars appear set.")


def load_config(path: str) -> Dict[str, Any]:
    """Load YAML or JSON config."""
    if not os.path.isfile(path):
        logger.critical("Config file not found: %s", path)
        sys.exit(2)

    def _expand_env(obj: Any) -> Any:
        """
        Recursively expand ${VAR} (and ${VAR:default}) in config values.
        Leaves the placeholder unchanged if VAR is unset and no default is provided.
        """
        if isinstance(obj, dict):
            return {k: _expand_env(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_expand_env(v) for v in obj]
        if not isinstance(obj, str):
            return obj
        pattern = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}")

        def _sub(m: "re.Match") -> str:
            var = m.group(1)
            default = m.group(2)
            val = os.environ.get(var)
            if val:
                return val
            if default is not None:
                return default
            return m.group(0)

        return pattern.sub(_sub, obj)

    ext = os.path.splitext(path)[1].lower()
    with open(path, "r", encoding="utf-8") as f:
        if ext == ".json":
            return _expand_env(json.load(f))
        if ext in (".yaml", ".yml"):
            if yaml is None:
                logger.critical("PyYAML required for YAML configs. pip install pyyaml")
                sys.exit(3)
            return _expand_env(yaml.safe_load(f) or {})
        logger.critical("Unsupported config format: %s", ext)
        sys.exit(4)


def get_db_connection_cfg(cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the first database connection block from locations, or None."""
    for loc in cfg.get("locations", []):
        if (loc.get("type") or "").lower() == "database":
            return loc.get("connection", {}) or {}
    return None


def build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="SQL Runner + Intelligent Data Assessment",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--sources", default="config/sources.yaml", help="Path to YAML or JSON config")
    p.add_argument("--rows", type=int, default=5, help="Preview N rows per SQL table")
    p.add_argument("--list-only", action="store_true", help="Only list SQL tables")
    p.add_argument("--schema-only", action="store_true", help="Only show SQL schema")
    p.add_argument("--with-schema", action="store_true", help="List SQL tables AND show schema")
    p.add_argument("--debug", action="store_true", help="Enable debug logging")
    p.add_argument("--stream-file", default=None, help="Path to JSON array of records to validate")
    p.add_argument("--stream-name", default="stream", help="Logical name for stream dataset")
    p.add_argument(
        "--blob-names",
        default="",
        help=(
            "Comma-separated blob filenames to assess (Azure Blob only). "
            "If set, only these blobs are loaded from the azure_blob container."
        ),
    )
    p.add_argument("--export-json", default=None, help="Write full JSON assessment (whole report)")
    p.add_argument("--export-report", default=None, help="Write Markdown report (whole report)")
    p.add_argument("--export-html", default=None, help="Write HTML report (whole report)")
    p.add_argument(
        "--reports-dir",
        default=None,
        help="Directory for overall report.* outputs (report.json/.md/.html).",
    )
    p.add_argument("--export-pf-input", default=None, help="Write Promptflow dq_profile JSON")
    p.add_argument("--export-pf-eval", default=None, help="Write PF evaluation PASS/WARN/FAIL")
    p.add_argument("--export-to-azure", action="store_true", help="Upload outputs to Azure Blob")
    p.add_argument("--azure-only", action="store_true", help="Save only to Azure (no local files)")
    p.add_argument("--azure-output-container", default="output", help="Output container name")
    p.add_argument("--skip-azure", action="store_true", help="Do not access Azure Blob (offline)")
    p.add_argument("--dq-thresholds", default=None, help="Path to dq_thresholds.yaml (default: config/dq_thresholds.yaml)")
    p.add_argument(
        "--export-manifest",
        metavar="PATH",
        default=None,
        help="Write cleaning_manifest.json to PATH "
        "(default with --reports-dir: <reports-dir>/cleaning_manifest.json unless overridden here).",
    )
    p.add_argument(
        "--llm-insights",
        action="store_true",
        help="After assessment, call Azure OpenAI for executive summary & risks (needs AZURE_OPENAI_* env).",
    )
    p.add_argument(
        "--evaluate",
        default="auto",
        choices=["auto", "interactive", "sql", "blob", "local", "stream", "all"],
        help=(
            "Which data to evaluate: sql|blob|local|stream|all, interactive menu, or auto "
            "(menu if stdin is a TTY, else all). Stream uses --stream-file or prompts for path."
        ),
    )
    return p.parse_args()




def cli_main():
    args = build_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _doctor_env_hint()

    # Legacy: --stream-file always runs stream batch and exits (no scope menu).
    if args.stream_file:
        try:
            from agent.mcp_interface import process_stream_chunk
            with open(args.stream_file, "r", encoding="utf-8") as f:
                records = json.load(f)
            if not isinstance(records, list):
                logger.error("Stream file must contain a JSON array of objects")
                sys.exit(5)
            report = process_stream_chunk(records, name=args.stream_name)
            print("=== STREAM BATCH REPORT ===")
            print(json.dumps(report, indent=2, default=to_json_safe))
            print("=== DONE ===")
            return
        except Exception as e:
            logger.exception("Could not process stream file: %s", e)
            sys.exit(6)

    from agent.evaluation_scope import (
        MODE_ALL,
        MODE_BLOB,
        MODE_INTERACTIVE,
        MODE_LOCAL,
        MODE_SQL,
        MODE_STREAM,
        default_evaluate_mode as _default_eval_mode,
        interactive_select_mode,
        location_types_for_mode,
        prompt_stream_file_path,
    )

    eval_mode = (args.evaluate or "auto").lower()
    if eval_mode == "auto":
        eval_mode = _default_eval_mode()
    if eval_mode == MODE_INTERACTIVE:
        eval_mode = interactive_select_mode()

    if eval_mode == MODE_STREAM:
        stream_path = prompt_stream_file_path()
        if not stream_path:
            logger.error("Stream mode requires a JSON array file path.")
            sys.exit(6)
        try:
            from agent.mcp_interface import process_stream_chunk
            with open(stream_path, "r", encoding="utf-8") as f:
                records = json.load(f)
            if not isinstance(records, list):
                logger.error("Stream file must contain a JSON array of objects")
                sys.exit(5)
            report = process_stream_chunk(records, name=args.stream_name)
            print("=== STREAM BATCH REPORT ===")
            print(json.dumps(report, indent=2, default=to_json_safe))
            print("=== DONE ===")
            return
        except Exception as e:
            logger.exception("Could not process stream file: %s", e)
            sys.exit(6)

    cfg_root = load_config(args.sources)
    source_cfg = cfg_root.get("source", cfg_root)
    has_azure_blob = any((loc.get("type") or "").lower() == "azure_blob" for loc in source_cfg.get("locations", []))
    has_filesystem = any((loc.get("type") or "").lower() == "filesystem" for loc in source_cfg.get("locations", []))
    db_locs = [
        loc for loc in source_cfg.get("locations", [])
        if (loc.get("type") or "").lower() == "database"
    ]
    has_sql = bool(db_locs) and AzureSQLPythonNetConnector is not None

    if eval_mode == MODE_SQL:
        if not has_sql:
            logger.critical("SQL-only evaluation requires a database location and SQL connector (pythonnet).")
            sys.exit(6)
    elif eval_mode == MODE_BLOB:
        if not has_azure_blob:
            logger.critical("Blob-only evaluation requires at least one azure_blob location in sources.yaml.")
            sys.exit(6)
        if args.skip_azure:
            logger.critical("Blob-only evaluation cannot use --skip-azure.")
            sys.exit(6)
    elif eval_mode == MODE_LOCAL:
        if not has_filesystem:
            logger.critical("Local-only evaluation requires a filesystem location in sources.yaml.")
            sys.exit(6)
    elif eval_mode == MODE_ALL:
        if not has_azure_blob and not has_filesystem and not has_sql:
            logger.critical(
                "Need at least one data source: azure_blob, filesystem, or database (with SQL connector available)"
            )
            sys.exit(6)
    else:
        logger.critical("Unknown evaluation mode: %s", eval_mode)
        sys.exit(6)

    multi_sql = len(db_locs) > 1

    if has_sql and eval_mode in (MODE_SQL, MODE_ALL):
        if args.list_only:
            for db_idx, db_loc in enumerate(db_locs):
                conn_cfg = db_loc.get("connection", {}) or {}
                lbl = (db_loc.get("id") or db_loc.get("label") or "").strip() or conn_cfg.get(
                    "database", f"database_{db_idx}"
                )
                print(f"\n=== Azure SQL [{lbl}] ===")
                print("Server   :", conn_cfg.get("server"))
                print("Database :", conn_cfg.get("database"))
                try:
                    connector = AzureSQLPythonNetConnector(conn_cfg)
                    tables = connector.discover_tables()
                except Exception as e:
                    logger.info("Table discovery failed [%s]: %s", lbl, str(e)[:200])
                    tables = []
                if not tables:
                    logger.info("No tables found for [%s]", lbl)
                else:
                    for t in tables:
                        print(" -", t if not multi_sql else f"{lbl} :: {t}")
            print("\n=== DONE (listed only) ===")
            return

        for db_idx, db_loc in enumerate(db_locs):
            conn_cfg = db_loc.get("connection", {}) or {}
            lbl = (db_loc.get("id") or db_loc.get("label") or "").strip() or conn_cfg.get(
                "database", f"database_{db_idx}"
            )
            logger.debug("DB [%s] connection keys: %s", lbl, list(conn_cfg.keys()))
            print(f"\n=== Connecting to Azure SQL [{lbl}] ===")
            print("Server   :", conn_cfg.get("server"))
            print("Database :", conn_cfg.get("database"))
            try:
                connector = AzureSQLPythonNetConnector(conn_cfg)
                tables = connector.discover_tables()
            except Exception as e:
                logger.info("Table discovery failed [%s]: %s", lbl, str(e)[:200])
                tables = []
                connector = None
            if not tables:
                logger.info("No tables found for [%s]", lbl)
            else:
                for t in tables:
                    print(" -", t if not multi_sql else f"{lbl} :: {t}")

            if args.schema_only or args.with_schema:
                print(f"\n=== Table Schemas [{lbl}] ===")
                if connector and tables:
                    for table in tables:
                        disp = table if not multi_sql else f"{lbl} / {table}"
                        print(f"\n--- {disp} ---")
                        ok = print_schema_info_schema(connector, table)
                        if not ok:
                            print_schema_top0(connector, table)

            if not args.schema_only and connector and tables:
                print(f"\n=== Table Previews [{lbl}] ===")
                for table in tables:
                    disp = table if not multi_sql else f"{lbl} / {table}"
                    print(f"\n--- {disp} ---")
                    try:
                        n = args.rows if args.rows > 0 else 5
                        df = connector.preview_table(table, n)
                        if df is None or df.empty:
                            print("[INFO] (empty table)")
                        else:
                            print(df.head(n))
                    except Exception as e:
                        logger.error("Preview failed for %s: %s", table, e)

        if args.schema_only:
            print("\n=== DONE ===")
            return
        if args.with_schema and args.rows <= 0:
            print("\n=== DONE ===")
            return
    else:
        if eval_mode in (MODE_SQL, MODE_ALL):
            print("\n[INFO] Skipping SQL Runner (no database configured)")
        else:
            print("\n[INFO] Skipping SQL (evaluation scope excludes SQL)")

    print("\n=== Running Intelligent Data Assessment ===")
    print(f"=== Scope: {eval_mode} ===")
    _run_t0 = time.perf_counter()
    from datetime import datetime as _dt_utc, timezone as _tz

    _run_started = _dt_utc.now(_tz.utc).isoformat()
    blob_data = {}
    load_blob = eval_mode in (MODE_BLOB, MODE_ALL) and not args.skip_azure
    if load_blob:
        blob_locs = [
            loc
            for loc in source_cfg.get("locations", [])
            if (loc.get("type") or "").lower() == "azure_blob"
        ]
        if blob_locs:
            only_blobs: List[str] = []
            if (args.blob_names or "").strip():
                only_blobs = [x.strip() for x in (args.blob_names or "").split(",") if x.strip()]
            blob_data = load_all_assessment_blob_datasets(source_cfg, only_blobs=only_blobs or None)
            logger.info(
                "Loaded %d datasets from %d Azure Blob container(s)",
                len(blob_data),
                len(blob_locs),
            )
            for blob_name, df in sorted(blob_data.items()):
                print(f"  - {blob_name}: {len(df)} rows x {len(df.columns)} columns")
        else:
            logger.info("No azure_blob locations in config")
    elif args.skip_azure:
        logger.info("Skipping Azure Blob (--skip-azure)")
    else:
        logger.info("Skipping Azure Blob (evaluation scope excludes blob)")

    dq_thresholds_path = args.dq_thresholds or os.environ.get("DQ_THRESHOLDS_PATH")
    if not dq_thresholds_path and os.path.isfile(os.path.join("config", "dq_thresholds.yaml")):
        dq_thresholds_path = os.path.join("config", "dq_thresholds.yaml")
    location_filter = location_types_for_mode(eval_mode)
    result = load_and_profile(
        source_cfg,
        additional_data=blob_data,
        dq_thresholds_path=dq_thresholds_path,
        return_datasets=False,
        location_types=location_filter,
    )
    _attach_run_metadata_and_suggestions(result, _run_t0, _run_started, args.sources or "")
    result.setdefault("run_metadata", {})["evaluation_scope"] = eval_mode
    logger.info(
        "Assessment finished in %.2fs | DQ rollup high/medium/low: %s",
        result.get("run_metadata", {}).get("duration_seconds", 0),
        result.get("run_metadata", {}).get("dq_issue_totals"),
    )

    if getattr(args, "llm_insights", False):
        try:
            from agent.llm_assessment_enhancer import generate_llm_insights

            result["llm_insights"] = generate_llm_insights(result)
            li = result["llm_insights"]
            if li.get("success"):
                logger.info("LLM insights generated (Azure OpenAI)")
            else:
                logger.warning("LLM insights not generated: %s", li.get("error"))
        except Exception as e:
            logger.exception("LLM insights failed: %s", e)
            result["llm_insights"] = {"success": False, "error": str(e), "parsed": None}

    def _ensure_parent(path: Optional[str]):
        if not path:
            return
        parent = os.path.dirname(path)
        if parent and not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)

    def _export(path_opt: Optional[str], content_bytes: bytes, content_str: Optional[str], blob_basename: str, desc: str):
        if not path_opt:
            return
        try:
            if args.azure_only and args.export_to_azure:
                if upload_data_to_azure(content_bytes, blob_basename, source_cfg, args.azure_output_container):
                    logger.info("Uploaded %s to Azure container '%s'", desc, args.azure_output_container)
                else:
                    logger.error("Failed to upload %s to Azure", desc)
            else:
                _ensure_parent(path_opt)
                if content_str is not None:
                    with open(path_opt, "w", encoding="utf-8") as f:
                        f.write(content_str)
                else:
                    with open(path_opt, "wb") as f:
                        f.write(content_bytes)
                logger.info("Wrote %s to: %s", desc, path_opt)
                if args.export_to_azure:
                    if upload_output_to_azure(path_opt, blob_basename, source_cfg, args.azure_output_container):
                        logger.info("Uploaded %s to Azure container '%s'", desc, args.azure_output_container)
        except Exception as e:
            logger.exception("Could not write/upload %s: %s", desc, e)

    if args.export_json:
        json_bytes = json.dumps(result, ensure_ascii=False, indent=2, default=to_json_safe).encode("utf-8")
        _export(args.export_json, json_bytes, None, os.path.basename(args.export_json), "JSON report")

    if args.export_report:
        md = build_markdown_report(result)
        md_bytes = md.encode("utf-8")
        _export(args.export_report, md_bytes, md, os.path.basename(args.export_report), "Markdown report")

    if args.export_html:
        html = build_html_report(result)
        html_bytes = html.encode("utf-8")
        _export(args.export_html, html_bytes, html, os.path.basename(args.export_html), "HTML report")

    # Consolidated reports: report.* at root + by_path/<folder>/report.* per source location
    if args.reports_dir:
        reports_dir = args.reports_dir
        os.makedirs(reports_dir, exist_ok=True)
        datasets = result.get("datasets", {})
        json_bytes = json.dumps(result, ensure_ascii=False, indent=2, default=to_json_safe).encode("utf-8")
        with open(os.path.join(reports_dir, "report.json"), "wb") as f:
            f.write(json_bytes)
        logger.info("Wrote %s", os.path.join(reports_dir, "report.json"))
        md_full = build_markdown_report(result)
        with open(os.path.join(reports_dir, "report.md"), "w", encoding="utf-8") as f:
            f.write(md_full)
        logger.info("Wrote %s", os.path.join(reports_dir, "report.md"))
        html_full = build_html_report(result)
        with open(os.path.join(reports_dir, "report.html"), "w", encoding="utf-8") as f:
            f.write(html_full)
        logger.info("Wrote %s", os.path.join(reports_dir, "report.html"))
        if args.export_manifest:
            manifest_path = args.export_manifest
        else:
            manifest_path = os.path.join(reports_dir, "cleaning_manifest.json")
        try:
            sc_m = build_dq_scorecard(result)
            mf = build_cleaning_manifest(result, result.get("transformation_suggestions") or {}, sc_m)
            _ensure_parent(manifest_path)
            with open(manifest_path, "w", encoding="utf-8") as mf_h:
                json.dump(mf, mf_h, indent=2, default=to_json_safe)
            logger.info("Cleaning manifest written: %s", manifest_path)
            print(f"✅ Cleaning manifest written: {manifest_path}")
        except Exception as e:
            logger.warning("Cleaning manifest skipped: %s", e)

    elif getattr(args, "export_manifest", None):
        try:
            sc_m = build_dq_scorecard(result)
            mf = build_cleaning_manifest(result, result.get("transformation_suggestions") or {}, sc_m)
            manifest_path = args.export_manifest
            _ensure_parent(manifest_path)
            with open(manifest_path, "w", encoding="utf-8") as mf_h:
                json.dump(mf, mf_h, indent=2, default=to_json_safe)
            logger.info("Cleaning manifest written: %s", manifest_path)
            print(f"✅ Cleaning manifest written: {manifest_path}")
        except Exception as e:
            logger.warning("Cleaning manifest failed: %s", e)

    pf_input = None
    try:
        pf_input = engine_to_pf_dq(result)
    except Exception as e:
        logger.exception("Could not build Promptflow input: %s", e)

    if args.export_pf_input and pf_input:
        pf_bytes = json.dumps(pf_input, ensure_ascii=False, indent=2, default=to_json_safe).encode("utf-8")
        _export(args.export_pf_input, pf_bytes, None, os.path.basename(args.export_pf_input), "PF input")

    if args.export_pf_eval and pf_input:
        try:
            pf_eval = evaluate_dq_rules_inline(pf_input)
            pf_eval_bytes = json.dumps(pf_eval, ensure_ascii=False, indent=2, default=to_json_safe).encode("utf-8")
            _export(args.export_pf_eval, pf_eval_bytes, None, os.path.basename(args.export_pf_eval), "PF evaluation")
        except Exception as e:
            logger.exception("Inline evaluation failed: %s", e)

    print("\n=== FULL RESULT (JSON) ===")
    print(json.dumps(result, indent=2, default=to_json_safe))
    _rm = result.get("run_metadata") or {}
    _tsn = (result.get("transformation_suggestions") or {}).get("summary") or {}
    print(
        f"\n=== DONE ===  ({_rm.get('duration_seconds')}s | "
        f"DQ high/med/low: {_rm.get('dq_issue_totals')} | "
        f"suggested fixes: {_tsn.get('total_suggestions', 0)})"
    )




if __name__ == "__main__":
    cli_main()
