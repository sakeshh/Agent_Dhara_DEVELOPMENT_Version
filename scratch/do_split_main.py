# do_split_main.py
import os
import re

main_path = r"c:\Users\ssakesh\Desktop\python\DHARA-GX\Agent Dhara Backend\main.py.bak"
server_path = r"c:\Users\ssakesh\Desktop\python\DHARA-GX\Agent Dhara Backend\agent\mcp_server.py.bak"

with open(main_path, "r", encoding="utf-8") as f:
    main_lines = f.readlines()

def get_main_lines(start_1indexed, end_1indexed):
    return "".join(main_lines[start_1indexed-1:end_1indexed])

with open(server_path, "r", encoding="utf-8") as f:
    server_lines = f.readlines()

def get_server_lines(start_1indexed, end_1indexed):
    return "".join(server_lines[start_1indexed-1:end_1indexed])

target_dir = r"c:\Users\ssakesh\Desktop\python\DHARA-GX\Agent Dhara Backend\agent"

# 1. Write service_layer.py
service_layer_content = f"""# Service layer for DHARA-GX report and metadata operations.
from __future__ import annotations
import os
import sys
import re
import time
import json
import html as html_module
import hashlib
import logging
from collections import defaultdict
from typing import Dict, Any, Tuple, List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Azure / SQL Connector imports and availability
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
        load_dq_thresholds = getattr(ida, "load_dq_thresholds", lambda p: {{}})
    except Exception as e:
        logger.critical("Could not import intelligent_data_assessment: %s", e)
        sys.exit(1)

try:
    from connectors.azure_sql_pythonnet import AzureSQLPythonNetConnector
except Exception as e:
    logger.info("SQL connector not available (pythonnet/.NET required for database): %s", e)
    AzureSQLPythonNetConnector = None

try:
    from connectors.azure_blob_storage import AzureBlobStorageConnector
    AZURE_BLOB_AVAILABLE = True
except Exception as e:
    logger.info("Azure Blob Storage connector not available: %s", e)
    AZURE_BLOB_AVAILABLE = False

{get_main_lines(121, 144)}
{get_main_lines(257, 383)}
{get_main_lines(384, 927)}
{get_main_lines(928, 1907)}
{get_main_lines(1908, 2022)}
{get_main_lines(2023, 2126)}
"""

with open(os.path.join(target_dir, "service_layer.py"), "w", encoding="utf-8") as f:
    f.write(service_layer_content)

# 2. Write bootstrap.py
bootstrap_content = f"""# Bootstrapping, command-line parsing, and environmental validation.
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
for _az_dir in [r"C:\\Program Files\\Microsoft SDKs\\Azure\\CLI2\\wbin", r"C:\\Program Files (x86)\\Microsoft SDKs\\Azure\\CLI2\\wbin"]:
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
        load_dq_thresholds = getattr(ida, "load_dq_thresholds", lambda p: {{}})
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

{get_main_lines(55, 83)}
{get_main_lines(145, 256)}

def cli_main():
{get_main_lines(2130, 2500)}

if __name__ == "__main__":
    cli_main()
"""

with open(os.path.join(target_dir, "bootstrap.py"), "w", encoding="utf-8") as f:
    f.write(bootstrap_content)

# 3. Write api_routes.py
# Collect models and endpoint decorators from mcp_server.py
api_routes_content = f"""# API routes and schemas for the MCP server.
from __future__ import annotations
import os
import json
import logging
import time
from typing import Any, Dict, List, Optional
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, Request, UploadFile, File, Form
from fastapi.responses import FileResponse, JSONResponse

from agent.logging_setup import setup_logging
from agent.security import InMemoryRateLimiter, client_ip, get_request_id, require_backend_token
from agent.jobs_store import create_job, fetch_events, fetch_job
from agent.jobs_worker import JobWorker
from agent.mcp_interface import (
    run_assessment,
    list_tables,
    process_stream_chunk,
    load_path,
    process_uploaded_file,
)
from agent.transformation_suggester import suggest_transformations
from agent.requirements_to_config import build_user_request_text, requirements_to_selected_sources

logger = logging.getLogger("mcp_server")

# Import report generation from service_layer and bootstrap
from agent.service_layer import (
    build_html_report as _build_html,
    build_markdown_report as _build_md
)
from agent.bootstrap import load_config as _load_config

router = APIRouter()

{get_server_lines(67, 187)}

def _get_config_text(body_config: str) -> str:
{get_server_lines(287, 296)}

# We bind the JobWorker and endpoint paths to the router
_worker = None

def set_worker(w):
    global _worker
    _worker = w

# Register all routes on the router
"""

# Now we extract all endpoints from server_lines starting at line 297 up to the end
endpoints_code = get_server_lines(297, 1225)
# Replace @app. to @router.
endpoints_code = endpoints_code.replace("@app.", "@router.")
# Replace references to global _worker to using router settings if needed, but it refers to module-level _worker which we set
api_routes_content += endpoints_code

with open(os.path.join(target_dir, "api_routes.py"), "w", encoding="utf-8") as f:
    f.write(api_routes_content)

# 4. Overwrite root main.py with the shim
main_shim = """#!/usr/bin/env python3
# -*- coding: utf-8 -*-
\"\"\"
main.py — Shim layer redirecting to agent.bootstrap and agent.service_layer.
\"\"\"
import os
import sys

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from agent.bootstrap import cli_main

# Re-export service layer functions for backward compatibility
from agent.service_layer import (
    to_json_safe,
    load_config,
    get_db_connection_cfg,
    split_schema_table,
    _quote_two_part_name_fallback,
    print_schema_info_schema,
    print_schema_top0,
    _fmt_pct,
    _endpoint_to_dataset,
    _result_for_datasets,
    _per_dataset_dir_name,
    _source_root_to_folder_name,
    _attach_run_metadata_and_suggestions,
    _brief_issue_line,
    _dtype_with_inference,
    build_dq_scorecard,
    build_cleaning_manifest,
    build_markdown_report,
    build_html_report,
    engine_to_pf_dq,
    _eval_completeness,
    _eval_consistency,
    _eval_validity,
    _eval_anomalies,
    _eval_semantic,
    evaluate_dq_rules_inline,
    get_azure_blob_connector_for_output,
    get_azure_blob_connector_for_assessment,
    _azure_blob_location_label,
    load_all_assessment_blob_datasets,
    upload_output_to_azure,
    upload_data_to_azure,
)

from agent.bootstrap import (
    _doctor_env_hint,
    build_args,
)

def main():
    cli_main()

if __name__ == "__main__":
    main()
"""

with open(main_path, "w", encoding="utf-8") as f:
    f.write(main_shim)

# 5. Overwrite agent/mcp_server.py with the shim
mcp_server_shim = """# -*- coding: utf-8 -*-
\"\"\"FastAPI MCP server for Intelligent Data Assessment.\"\"\"

# Force .NET Framework runtime for pythonnet (MUST happen before any clr/pythonnet usage)
try:
    import clr_loader
    import pythonnet
    pythonnet.set_runtime(clr_loader.get_netfx())
except Exception:
    pass

import os
import sys
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# Force PROJECT_DIR into sys.path
_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from agent.logging_setup import setup_logging
from agent.security import InMemoryRateLimiter, client_ip, get_request_id, require_backend_token
from agent.jobs_worker import JobWorker
from agent.api_routes import router, set_worker

setup_logging()
logger = logging.getLogger("mcp_server")

app = FastAPI(title="Intelligent Data Assessment MCP Server")

_limiter = InMemoryRateLimiter(
    max_requests=int(os.environ.get("RATE_LIMIT_PER_MINUTE", "120")),
    window_seconds=60,
)

_worker = JobWorker()
set_worker(_worker)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in (os.environ.get("CORS_ALLOW_ORIGINS") or "").split(",") if o.strip()] or [
        "http://localhost:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:3001",
    ],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "X-Backend-Token", "X-Request-Id", "X-Correlation-Id"],
)

@app.middleware("http")
async def auth_and_logging_middleware(request: Request, call_next):
    rid = get_request_id(request)
    request.state.request_id = rid
    try:
        _limiter.check(client_ip(request))
        if request.url.path not in ("/", "/healthz", "/readyz"):
            require_backend_token(request)
        response = await call_next(request)
        response.headers["X-Request-Id"] = rid
        return response
    except HTTPException as e:
        logger.warning("http_error", extra={"request_id": rid})
        return JSONResponse(status_code=e.status_code, content={"detail": e.detail, "request_id": rid})
    except Exception as e:
        logger.exception("unhandled_error", extra={"request_id": rid})
        return JSONResponse(status_code=500, content={"detail": str(e), "request_id": rid})

@app.on_event("startup")
async def startup_event():
    logger.info("FastAPI MCP Server starting...")

@app.exception_handler(Exception)
def generic_exception_handler(request: Request, exc: Exception):
    rid = getattr(request.state, "request_id", "unknown")
    logger.exception("unhandled_exception", extra={"request_id": rid})
    return JSONResponse(
        status_code=500,
        content={"detail": f"An unexpected error occurred: {exc}", "request_id": rid},
    )

app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("agent.mcp_server:app", host="127.0.0.1", port=8000, reload=True)
"""

with open(server_path, "w", encoding="utf-8") as f:
    f.write(mcp_server_shim)

print("Split completed successfully!")
