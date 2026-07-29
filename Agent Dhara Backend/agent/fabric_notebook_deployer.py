"""
fabric_notebook_deployer.py - Deploys PySpark ETL code to Microsoft Fabric Notebooks (one notebook per dataset) and triggers runs.

Uses the FabricAPIClient to create or update notebooks, trigger execution,
and poll for run status.
"""

from __future__ import annotations

import os
import re
import ast
import time
import logging
import asyncio
from typing import Dict, Any, List, Optional

from agent.fabric_api_client import FabricAPIClient
from agent.blob_fabric_registry import make_safe_shortcut_name

logger = logging.getLogger("agent.fabric_notebook_deployer")

def _clean_env_value(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        s = s[1:-1].strip()
    return s or None

def _is_matching_dataset(a: str, b: str) -> bool:
    """Check if two dataset keys match, ignoring dots/underscores/case differences."""
    if not a or not b:
        return False
    if a == b:
        return True
    a_norm = re.sub(r"[^a-z0-9]", "", str(a).lower())
    b_norm = re.sub(r"[^a-z0-9]", "", str(b).lower())
    return a_norm == b_norm


def _customize_code_for_dataset(pyspark_code: str, target_ds: str) -> str:
    """
    Wraps all dfs assignments for non-target datasets in `if False:` blocks
    using AST transformation to ensure 100% syntactic correctness.
    """
    try:
        tree = ast.parse(pyspark_code)
        
        class DatasetFilterTransformer(ast.NodeTransformer):
            def __init__(self, target: str):
                self.target = target

            def visit_Assign(self, node):
                # Check if target is dfs['some_ds'] where some_ds != target
                for target_node in node.targets:
                    if isinstance(target_node, ast.Subscript):
                        if isinstance(target_node.value, ast.Name) and target_node.value.id == "dfs":
                            slice_val = None
                            if isinstance(target_node.slice, ast.Constant):
                                slice_val = target_node.slice.value
                            elif isinstance(target_node.slice, ast.Index) and isinstance(target_node.slice.value, ast.Constant):
                                slice_val = target_node.slice.value.value
                            elif isinstance(target_node.slice, ast.String):
                                slice_val = target_node.slice.s
                                
                            if slice_val and not _is_matching_dataset(slice_val, self.target):
                                return ast.If(
                                    test=ast.Constant(value=False),
                                    body=[node],
                                    orelse=[]
                                )
                return self.generic_visit(node)
                
        transformer = DatasetFilterTransformer(target_ds)
        new_tree = transformer.visit(tree)
        ast.fix_missing_locations(new_tree)
        return ast.unparse(new_tree)
    except Exception as e:
        logger.warning(f"AST transformation failed, falling back to line-based matching: {e}")
        # Fallback to line-based if AST parsing/unparsing fails
        lines = pyspark_code.splitlines()
        result = []
        i = 0
        while i < len(lines):
            line = lines[i]
            m = re.match(r'^(\s*)dfs\[[\"\']([^\"\']+)[\"\']\]\s*=', line)
            if m and not _is_matching_dataset(m.group(2), target_ds):
                block = [line]
                indent = len(m.group(1))
                i += 1
                while i < len(lines):
                    next_line = lines[i]
                    if next_line.strip() == "" or (len(next_line) - len(next_line.lstrip())) > indent:
                        block.append(next_line)
                        i += 1
                    else:
                        break
                result.append(f"{m.group(1)}if False:  # [Disabled: {m.group(2)}]")
                for bl in block:
                    result.append("    " + bl)
            else:
                result.append(line)
                i += 1
        return "\n".join(result)


def _extract_datasets_from_code(pyspark_code: str) -> List[str]:
    """Extract DATASETS list from generated PySpark code (supports DATASETS array or transform_ function names)."""
    ds_match = re.search(r"DATASETS\s*=\s*(\[[\s\S]*?\])", pyspark_code)
    datasets: List[str] = []
    if ds_match:
        try:
            cleaned_arr = re.sub(r"#.*", "", ds_match.group(1))
            datasets = ast.literal_eval(cleaned_arr)
        except Exception as e:
            logger.warning(f"Could not parse DATASETS array from code: {e}")
            
    if not datasets:
        datasets = re.findall(r"def transform_(\w+)\s*\(", pyspark_code)
        datasets = [d for d in datasets if d not in ("data_quality_issues", "data_quality_issues_v2")]
            
    if not datasets:
        logger.warning("No datasets found in code, falling back to ['default']")
        datasets = ["default"]
        
    return datasets

def _is_uuid(s: str) -> bool:
    return bool(re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', s, re.I))

from agent.etl_pipeline.format_capabilities import required_package_for_format, get_fabric_hint

def required_packages_for_plan(plan: Optional[Dict[str, Any]]) -> List[str]:
    """Collect required Spark packages for all dataset formats in the plan."""
    if not plan:
        return []
    pkgs = []
    datasets = (plan.get("connector_manifest") or {}).get("datasets") or plan.get("datasets") or {}
    for ds_name, block in datasets.items():
        if isinstance(block, dict):
            fmt = str(block.get("format") or "").lower()
            pkg = required_package_for_format(fmt)
            if pkg and pkg not in pkgs:
                pkgs.append(pkg)
    return pkgs

def deploy_and_run_notebook(
    session_id: str,
    pyspark_code: str,
    lakehouse_id: Optional[str] = None,
    notebook_name: Optional[str] = None,
    plan: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Deploys one Microsoft Fabric Notebook per dataset in the PySpark script,
    attaches the default lakehouse, and triggers concurrent run jobs.
    """
    import html
    pyspark_code = html.unescape(pyspark_code or "")
    
    # Pre-flight package check
    needed_pkgs = required_packages_for_plan(plan)
    if needed_pkgs:
        attached = [p.strip() for p in os.getenv("FABRIC_ATTACHED_PACKAGES", "").split(",") if p.strip()]
        strict = os.getenv("FABRIC_STRICT_PACKAGE_CHECK", "1").strip() in ("1", "true", "yes")
        missing = [p for p in needed_pkgs if not any(p.lower() in a.lower() for a in attached)]
        if missing and (attached or strict):
            msg = f"Fabric Spark environment missing required package(s): {missing}. Please attach them in Fabric environment settings."
            logger.error(msg)
            return {"ok": False, "error": "MISSING_SPARK_PACKAGES", "message": msg, "missing_packages": missing}
        elif missing:
            logger.warning(f"Fabric execution requires Spark package(s): {missing}. Ensure they are attached to your Fabric Spark workspace environment.")
    workspace_id = _clean_env_value(os.getenv("FABRIC_WORKSPACE_ID"))
    lh_id = lakehouse_id or _clean_env_value(os.getenv("FABRIC_LAKEHOUSE_NAME") or os.getenv("FABRIC_LAKEHOUSE_ID"))
    
    if not workspace_id or not lh_id:
        err_msg = "Missing FABRIC_WORKSPACE_ID or FABRIC_LAKEHOUSE_NAME in environment."
        logger.error(err_msg)
        return {"ok": False, "error": "MISSING_CONFIG", "message": err_msg}

    client = FabricAPIClient()
    
    # Resolve lakehouse name to UUID if not already a UUID
    if lh_id and not _is_uuid(lh_id):
        resolved = client.resolve_lakehouse_id_by_name(workspace_id, lh_id)
        if resolved:
            logger.info(f"Resolved lakehouse name '{lh_id}' to UUID '{resolved}'")
            lh_id = resolved
        else:
            logger.warning(f"Could not resolve lakehouse name '{lh_id}' to a UUID. Proceeding with name.")
    
    datasets = _extract_datasets_from_code(pyspark_code)

    logger.info(f"Deploying {len(datasets)} dataset notebook(s) to Fabric Workspace '{workspace_id}' (Lakehouse: {lh_id})")
    
    runs = []
    for ds_name in datasets:
        # Generate safe clean name, e.g. "orders.csv" -> "orders"
        safe_name = make_safe_shortcut_name(ds_name)
        short_session = session_id[:8] if session_id else "default"
        if notebook_name:
            nb_name = f"{notebook_name}_{short_session}_{safe_name}"
        else:
            nb_name = f"dhara_{short_session}_{safe_name}"
        
        # Customize code to run ONLY this dataset
        custom_code = _customize_code_for_dataset(pyspark_code, ds_name)
        
        # Step 1a: Deploy or update notebook
        logger.info(f"Deploying notebook '{nb_name}'...")
        deploy_res = client.create_or_update_notebook(
            notebook_name=nb_name,
            pyspark_code=custom_code,
            lakehouse_id=lh_id
        )
        
        if not deploy_res.get("ok"):
            return deploy_res

        notebook_id = deploy_res["id"]
        
        # Step 1b: Trigger run
        try:
            logger.info(f"Triggering Notebook job run for notebook ID '{notebook_id}' ({nb_name})...")
            run_id = client.trigger_notebook_run(notebook_id)
            
            fabric_url = f"https://app.fabric.microsoft.com/groups/{workspace_id}/notebooks/{notebook_id}"
            if client.mock_mode:
                fabric_url = "#mock-mode"
                
            runs.append({
                "notebook_id": notebook_id,
                "notebook_name": nb_name,
                "dataset": ds_name,
                "run_id": run_id,
                "fabric_url": fabric_url
            })
        except Exception as e:
            logger.exception(f"Failed to trigger job run for notebook '{nb_name}': {e}")
            return {
                "ok": False,
                "error": "TRIGGER_RUN_FAILED",
                "message": f"Failed to trigger notebook run for '{nb_name}': {e}",
                "notebook_id": notebook_id
            }
            
    return {
        "ok": True,
        "runs": runs,
        "message": f"Successfully deployed {len(runs)} notebook(s) and triggered execution in Fabric."
    }

async def poll_multiple_notebooks_status(
    runs: List[Dict[str, Any]],
    timeout_seconds: int = 600,
    poll_interval: int = 10
) -> Dict[str, Any]:
    """
    Polls all running notebooks in parallel until they complete.
    """
    client = FabricAPIClient()
    start_time = time.time()
    pending_runs = list(runs)
    completed_runs = []
    failed_runs = []
    
    logger.info(f"Polling status for {len(pending_runs)} running notebook(s)...")
    while time.time() - start_time < timeout_seconds and pending_runs:
        for run in list(pending_runs):
            notebook_id = run["notebook_id"]
            run_id = run["run_id"]
            
            status_res = await asyncio.to_thread(client.get_run_status, notebook_id, run_id)
            status = status_res.get("status")
            
            if status == "Succeeded":
                logger.info(f"Notebook '{run['notebook_name']}' completed successfully.")
                completed_runs.append(run)
                pending_runs.remove(run)
            elif status == "Failed":
                logger.error(f"Notebook '{run['notebook_name']}' failed. Error: {status_res.get('error')}")
                run_err = run.copy()
                run_err["error"] = status_res.get("error")
                ds = run.get("dataset") or ""
                ext = ds.rsplit(".", 1)[-1] if "." in ds else ""
                run_err["hint"] = get_fabric_hint(ext)
                failed_runs.append(run_err)
                pending_runs.remove(run)
                
        if pending_runs:
            await asyncio.sleep(poll_interval)
            
    if pending_runs:
        logger.warning(f"{len(pending_runs)} notebook(s) timed out after {timeout_seconds}s.")
        for run in pending_runs:
            run_err = run.copy()
            run_err["error"] = "Execution timed out"
            failed_runs.append(run_err)
            
    if failed_runs:
        err_details = "; ".join([f"{r['notebook_name']}: {r['error']}" for r in failed_runs])
        return {
            "status": "Failed",
            "ok": False,
            "error": "NOTEBOOK_EXECUTION_FAILED",
            "message": f"One or more notebook runs failed: {err_details}",
            "failed_runs": failed_runs,
            "completed_runs": completed_runs
        }
        
    return {
        "status": "Succeeded",
        "ok": True,
        "completed_runs": completed_runs
    }
