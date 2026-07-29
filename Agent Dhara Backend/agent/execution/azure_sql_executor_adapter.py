"""
Azure SQL target executor adapter.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from agent.execution.execution_target import ExecutionEngine, ExecutionPlan, ExecutionResult, Executor
from agent.azure_sql_executor import run_transactional_sql, get_connection

logger = logging.getLogger("agent.execution.azure_sql")


class AzureSQLExecutorAdapter(Executor):
    def engine_type(self) -> ExecutionEngine:
        return ExecutionEngine.AZURE_SQL

    def execute(self, plan: ExecutionPlan, **kwargs) -> ExecutionResult:
        connection_string = kwargs.get("connection_string")
        dry_run = kwargs.get("dry_run", False)
        approved = kwargs.get("approved", False)
        timeout_s = kwargs.get("timeout_s", 120)
        run_id = kwargs.get("run_id", plan.plan_id)

        res = run_transactional_sql(
            plan.code,
            connection_string=connection_string,
            dry_run=dry_run,
            approved=approved,
            timeout_s=timeout_s,
            run_id=run_id,
        )

        return ExecutionResult(
            ok=res.get("ok", False),
            engine=self.engine_type(),
            run_id=res.get("run_id", run_id),
            rows_affected=res.get("total_rows_affected", 0),
            duration_ms=res.get("total_duration_ms", 0.0),
            committed=res.get("transaction_committed", False),
            error=res.get("rollback_reason"),
            batch_results=res.get("batch_results") or [],
            artifacts=res.get("artifacts") or {},
        )

    def test_connection(self, **kwargs) -> bool:
        connection_string = kwargs.get("connection_string")
        conn = None
        try:
            conn = get_connection(connection_string)
            return conn is not None
        except Exception as e:
            logger.warning("Azure SQL connection test failed: %s", e)
            return False
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
