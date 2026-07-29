"""
fabric_notebook_executor.py - Adapter for executing PySpark ETL code on Fabric Spark.

Implements the Executor base class. Deploys one notebook per dataset,
runs them, and polls until they all complete.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict

from agent.execution.execution_target import ExecutionEngine, ExecutionPlan, ExecutionResult, Executor
from agent.fabric_notebook_deployer import deploy_and_run_notebook, poll_multiple_notebooks_status
from agent.fabric_api_client import FabricAPIClient

logger = logging.getLogger("agent.execution.fabric_notebook")

class FabricNotebookExecutor(Executor):
    def engine_type(self) -> ExecutionEngine:
        return ExecutionEngine.FABRIC_PYSPARK

    def execute(self, plan: ExecutionPlan, **kwargs) -> ExecutionResult:
        session_id = kwargs.get("session_id") or plan.metadata.get("session_id") or "default"
        lakehouse_id = kwargs.get("lakehouse_id") or plan.metadata.get("lakehouse_id")
        
        t0 = time.time()
        logger.info(f"Executing Fabric PySpark ETL plan '{plan.plan_id}' (one notebook per dataset).")
        
        # 1. Deploy notebooks per dataset and trigger concurrent execution runs
        try:
            raw_plan_dict = kwargs.get("plan") or plan.metadata.get("raw_plan") or plan.metadata.get("plan")
            deploy_res = deploy_and_run_notebook(
                session_id=session_id,
                pyspark_code=plan.code,
                lakehouse_id=lakehouse_id,
                plan=raw_plan_dict,
            )
        except Exception as e:
            duration = (time.time() - t0) * 1000
            logger.exception(f"Fabric notebooks deployment/execution failed: {e}")
            return ExecutionResult(
                ok=False,
                engine=self.engine_type(),
                run_id="failed-deploy",
                duration_ms=duration,
                error=f"Fabric connection or initialization failed: {str(e)}"
            )
        
        if not deploy_res.get("ok"):
            duration = (time.time() - t0) * 1000
            return ExecutionResult(
                ok=False,
                engine=self.engine_type(),
                run_id="failed-deploy",
                duration_ms=duration,
                error=deploy_res.get("message", "Notebooks deployment failed")
            )
            
        runs = deploy_res["runs"]
        
        # 2. Poll all triggered notebooks for status
        logger.info(f"Triggered {len(runs)} notebook jobs successfully. Polling runs...")
        import asyncio
        import concurrent.futures

        def _run_polling_sync(runs_list, timeout_sec, poll_int):
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(
                    poll_multiple_notebooks_status(
                        runs=runs_list,
                        timeout_seconds=timeout_sec,
                        poll_interval=poll_int,
                    )
                )
            finally:
                loop.close()

        timeout_s = kwargs.get("timeout_s", 600)
        poll_interval_s = kwargs.get("poll_interval_s", 10)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_run_polling_sync, runs, timeout_s, poll_interval_s)
            status_res = future.result(timeout=timeout_s + 30)
        
        duration = (time.time() - t0) * 1000
        ok = status_res.get("ok", False)
        error_msg = status_res.get("message") if not ok else None
        
        # Build composite run ID from the triggered runs
        composite_run_id = ",".join([r["run_id"] for r in runs])
        
        # Primary notebook URL for rendering in the UI
        primary_notebook_url = runs[0]["fabric_url"] if runs else "#"
        
        return ExecutionResult(
            ok=ok,
            engine=self.engine_type(),
            run_id=composite_run_id,
            duration_ms=duration,
            committed=ok,
            error=error_msg,
            artifacts={
                "runs": runs,
                "fabric_url": primary_notebook_url,
                "notebook_name": runs[0]["notebook_name"] if runs else "unknown",
                "notebook_id": runs[0]["notebook_id"] if runs else "unknown"
            }
        )

    def test_connection(self, **kwargs) -> bool:
        try:
            client = FabricAPIClient()
            props = client.get_lakehouse_properties()
            return props is not None
        except Exception as e:
            logger.warning(f"Fabric connection test capability failed: {e}")
            return False
