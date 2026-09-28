from __future__ import annotations

from enum import Enum


class CompilerTarget(str, Enum):
    SQL = "sql"
    PYSPARK = "pyspark"
    DATABRICKS = "databricks"
    ADF = "adf"
    FABRIC = "fabric"
    SYNAPSE = "synapse"
    PYTHON = "python"
