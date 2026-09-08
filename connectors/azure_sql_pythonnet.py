"""
Azure SQL connector using pythonnet (Microsoft.Data.SqlClient or System.Data.SqlClient).
"""

import os
import sys
import re
import logging
from typing import Any, Dict, List, Optional
import pandas as pd
import csv

logger = logging.getLogger("connectors.azure_sql_pythonnet")

PYTHONNET_AVAILABLE = False
SqlConnection = None
SqlCommand = None
SQLCLIENT_PROVIDER = None

if sys.platform == "win32":
    try:
        import clr_loader
        import pythonnet
        try:
            rt = clr_loader.get_netfx()
            pythonnet.set_runtime(rt)
        except Exception:
            pass

        import clr
        clr.AddReference("System.Data")

        # Force modern TLS
        try:
            clr.AddReference("System.Net")
            from System.Net import ServicePointManager
            ServicePointManager.SecurityProtocol |= 768 | 3072 | 12288
        except Exception:
            pass

        def _try_load_microsoft_sqlclient():
            asm_dir = os.environ.get("SQLCLIENT_ASSEMBLY_DIR")
            if asm_dir and asm_dir not in sys.path:
                sys.path.append(asm_dir)
            try:
                clr.AddReference("Microsoft.Data.SqlClient")
                from Microsoft.Data.SqlClient import SqlConnection as _SC, SqlCommand as _SCmd
                return _SC, _SCmd
            except Exception:
                return None

        _ms = _try_load_microsoft_sqlclient()
        if _ms:
            SqlConnection, SqlCommand = _ms
            SQLCLIENT_PROVIDER = "Microsoft.Data.SqlClient"
        else:
            from System.Data.SqlClient import SqlConnection as _SC, SqlCommand as _SCmd  # type: ignore
            SqlConnection, SqlCommand = _SC, _SCmd
            SQLCLIENT_PROVIDER = "System.Data.SqlClient"
        PYTHONNET_AVAILABLE = True
        logger.info(f"Using SQL Client provider: {SQLCLIENT_PROVIDER}")
    except Exception as ex:
        logger.debug(f"pythonnet/.NET runtime not available on this host: {ex}")
        PYTHONNET_AVAILABLE = False
else:
    logger.debug("Linux / non-Windows host detected (%s); defaulting to pyodbc for Azure SQL connection.", sys.platform)


def _first_present(d: Dict[str, str], keys: List[str], default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


class AzureSQLPythonNetConnector:
    _pyodbc_logged: bool = False

    def __init__(self, conn_cfg: Dict[str, str]):
        self.cfg = conn_cfg
        self.use_pyodbc = not PYTHONNET_AVAILABLE
        if self.use_pyodbc and not AzureSQLPythonNetConnector._pyodbc_logged:
            logger.info("Using pyodbc for Azure SQL connection.")
            AzureSQLPythonNetConnector._pyodbc_logged = True

    def _get_sqlalchemy_engine(self):
        """
        Build a SQLAlchemy engine from the same config used by _pyodbc_connect.
        Returns None if sqlalchemy is not installed (will fall back to raw pyodbc).
        """
        try:
            from sqlalchemy import create_engine
        except ImportError:
            return None
        import pyodbc
        server = _first_present(self.cfg, ["server", "Server", "host", "hostname"]) or os.environ.get("AZURE_SQL_SERVER")
        if server and not server.endswith(".database.windows.net") and "localhost" not in server and "127.0.0.1" not in server and ":" not in server:
            server = server + ".database.windows.net"
        database = _first_present(self.cfg, ["database", "Database", "db"]) or os.environ.get("AZURE_SQL_DATABASE")
        user = (
            _first_present(self.cfg, ["user", "username", "User Id", "UserID", "uid"])
            or os.environ.get("AZURE_SQL_USERNAME")
            or os.environ.get("AZURE_SQL_USER")
        )
        password = _first_present(self.cfg, ["password", "Password", "pwd"]) or os.environ.get("AZURE_SQL_PASSWORD")
        if not server or not database:
            return None
        driver = "ODBC Driver 18 for SQL Server"
        try:
            sql_drivers = [d for d in pyodbc.drivers() if "sql server" in d.lower() or "odbc driver" in d.lower()]
            for pref in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]:
                if any(pref.lower() in d.lower() for d in sql_drivers):
                    driver = [d for d in sql_drivers if pref.lower() in d.lower()][0]
                    break
        except Exception:
            pass
        try:
            import urllib.parse
            if user and password:
                conn_str = (
                    f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
                    f"UID={user};PWD={password};Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
                )
            else:
                conn_str = (
                    f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
                    "Encrypt=yes;TrustServerCertificate=yes;Authentication=ActiveDirectoryDefault;Connection Timeout=30;"
                )
            encoded = urllib.parse.quote_plus(conn_str)
            return create_engine(f"mssql+pyodbc:///?odbc_connect={encoded}", fast_executemany=True)
        except Exception:
            return None

    def _read_sql_pyodbc(self, query: str, conn) -> pd.DataFrame:
        """
        pd.read_sql wrapper that prefers SQLAlchemy engine to avoid the DBAPI2 UserWarning.
        Falls back to raw pyodbc connection with the warning suppressed.
        """
        engine = self._get_sqlalchemy_engine()
        if engine is not None:
            try:
                with engine.connect() as sa_conn:
                    return pd.read_sql(query, sa_conn)
            except Exception:
                pass  # fall through to raw pyodbc
        # Fallback: raw pyodbc — suppress pandas DBAPI2 warning
        import warnings
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*DBAPI2.*", category=UserWarning)
            warnings.filterwarnings("ignore", message=".*SQLAlchemy.*", category=UserWarning)
            return pd.read_sql(query, conn)

    def _pyodbc_connect(self):
        import pyodbc
        server = _first_present(self.cfg, ["server", "Server", "host", "hostname"]) or os.environ.get("AZURE_SQL_SERVER")
        if server and not server.endswith(".database.windows.net") and "localhost" not in server and "127.0.0.1" not in server and ":" not in server:
            server = server + ".database.windows.net"
        database = _first_present(self.cfg, ["database", "Database", "db"]) or os.environ.get("AZURE_SQL_DATABASE")
        user = (
            _first_present(self.cfg, ["user", "username", "User Id", "UserID", "uid"])
            or os.environ.get("AZURE_SQL_USERNAME")
            or os.environ.get("AZURE_SQL_USER")
        )
        password = _first_present(self.cfg, ["password", "Password", "pwd"]) or os.environ.get("AZURE_SQL_PASSWORD")

        if not server or not database:
            raise ValueError("Missing required connection keys: 'server' and 'database'.")

        driver = "ODBC Driver 18 for SQL Server"
        try:
            drivers = pyodbc.drivers()
            sql_drivers = [d for d in drivers if "sql server" in d.lower() or "odbc driver" in d.lower()]
            if sql_drivers:
                for pref in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"]:
                    if any(pref.lower() in d.lower() for d in sql_drivers):
                        match = [d for d in sql_drivers if pref.lower() in d.lower()]
                        if match:
                            driver = match[0]
                            break
                else:
                    driver = sql_drivers[0]
        except Exception:
            pass

        parts = [
            f"Driver={{{driver}}}",
            f"Server={server}",
            f"Database={database}",
        ]
        if user and password:
            parts.append(f"Uid={user}")
            parts.append(f"Pwd={password}")
        parts.extend([
            "Encrypt=yes",
            "TrustServerCertificate=yes",
            "Connection Timeout=30",
        ])
        conn_str = ";".join(parts) + ";"
        return pyodbc.connect(conn_str, autocommit=True)

    def _connect(self):
        server = _first_present(self.cfg, ["server", "Server", "host", "hostname"]) or os.environ.get("AZURE_SQL_SERVER")
        if server and not server.endswith(".database.windows.net") and "localhost" not in server and "127.0.0.1" not in server and ":" not in server:
            server = server + ".database.windows.net"
        database = _first_present(self.cfg, ["database", "Database", "db"]) or os.environ.get("AZURE_SQL_DATABASE")
        user = (
            _first_present(self.cfg, ["user", "username", "User Id", "UserID", "uid"])
            or os.environ.get("AZURE_SQL_USERNAME")
            or os.environ.get("AZURE_SQL_USER")
        )
        password = _first_present(self.cfg, ["password", "Password", "pwd"]) or os.environ.get("AZURE_SQL_PASSWORD")

        if not server or not database:
            raise ValueError("Missing required connection keys: 'server' and 'database'.")

        parts = [
            f"Server={server};",
            f"Database={database};",
        ]
        if user and password:
            parts.append(f"User Id={user};")
            parts.append(f"Password={password};")
        parts.extend([
            "Encrypt=True;",
            "TrustServerCertificate=True;",
            "MultipleActiveResultSets=False;",
            "Connection Timeout=30;",
        ])
        conn_str = "".join(parts)
        return SqlConnection(conn_str)

    def discover_tables(self) -> List[str]:
        sql = """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        if self.use_pyodbc:
            conn = self._pyodbc_connect()
            try:
                cursor = conn.cursor()
                cursor.execute(sql)
                return [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
            finally:
                conn.close()

        conn = self._connect()
        try:
            conn.Open()
            cmd = SqlCommand(sql, conn)
            reader = cmd.ExecuteReader()
            tables: List[str] = []
            while reader.Read():
                schema = reader.GetString(0)
                table = reader.GetString(1)
                tables.append(f"{schema}.{table}")
            return tables
        finally:
            conn.Close()

    def _quote_two_part_name(self, two_part: str) -> str:
        parts = two_part.split(".", 1)
        if len(parts) == 2:
            return f"[{parts[0]}].[{parts[1]}]"
        return f"[{two_part}]"

    @staticmethod
    def _convert_clr_value(val: Any) -> Any:
        if val is None:
            return None
        cls_name = getattr(val, "__class__", type(val)).__name__
        if cls_name == "Decimal":
            try:
                f = float(str(val))
                return int(f) if f.is_integer() else f
            except Exception:
                return float(str(val))
        if cls_name == "DateTime":
            try:
                return str(val.ToString("yyyy-MM-dd HH:mm:ss"))
            except Exception:
                return str(val)
        return val

    def _read_reader_to_df(self, reader) -> pd.DataFrame:
        columns = [reader.GetName(i) for i in range(reader.FieldCount)]
        rows = []
        while reader.Read():
            row = [
                (None if reader.IsDBNull(i) else self._convert_clr_value(reader.GetValue(i)))
                for i in range(reader.FieldCount)
            ]
            rows.append(row)
        return pd.DataFrame(rows, columns=columns)

    def export_table_to_csv(
        self,
        table: str,
        output_path: str,
        *,
        include_header: bool = True,
    ) -> str:
        """
        Export a full table to CSV without storing all rows in memory.
        """
        output_path = os.path.abspath(output_path)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        if self.use_pyodbc:
            conn = self._pyodbc_connect()
            try:
                table_quoted = self._quote_two_part_name(table)
                df = self._read_sql_pyodbc(f"SELECT * FROM {table_quoted}", conn)
                df.to_csv(output_path, index=False, header=include_header)
                return output_path
            finally:
                conn.close()

        conn = self._connect()
        try:
            conn.Open()
            table_quoted = self._quote_two_part_name(table)
            cmd = SqlCommand(f"SELECT * FROM {table_quoted}", conn)
            reader = cmd.ExecuteReader()
            cols = [reader.GetName(i) for i in range(reader.FieldCount)]
            with open(output_path, "w", encoding="utf-8", newline="") as f:
                w = csv.writer(f)
                if include_header:
                    w.writerow(cols)
                while reader.Read():
                    row: List[Optional[Any]] = [
                        (None if reader.IsDBNull(i) else self._convert_clr_value(reader.GetValue(i)))
                        for i in range(reader.FieldCount)
                    ]
                    # Make sure values are CSV-writable
                    w.writerow([v if isinstance(v, (str, int, float, bool)) or v is None else str(v) for v in row])
            return output_path
        finally:
            conn.Close()

    def load_table(self, table: str, max_rows: Optional[int] = None) -> pd.DataFrame:
        if self.use_pyodbc:
            conn = self._pyodbc_connect()
            try:
                table_quoted = self._quote_two_part_name(table)
                limit = f"TOP {int(max_rows)} " if max_rows and int(max_rows) > 0 else ""
                return self._read_sql_pyodbc(f"SELECT {limit}* FROM {table_quoted}", conn)
            finally:
                conn.close()

        conn = self._connect()
        try:
            conn.Open()
            table_quoted = self._quote_two_part_name(table)
            limit = ""
            if max_rows and int(max_rows) > 0:
                limit = f"TOP {int(max_rows)} "
            cmd = SqlCommand(f"SELECT {limit}* FROM {table_quoted}", conn)
            reader = cmd.ExecuteReader()
            return self._read_reader_to_df(reader)
        finally:
            conn.Close()

    def preview_table(self, table: str, rows: int = 5) -> pd.DataFrame:
        rows = int(rows) if rows and rows > 0 else 5
        rows = max(1, min(rows, 10000))
        if self.use_pyodbc:
            conn = self._pyodbc_connect()
            try:
                table_quoted = self._quote_two_part_name(table)
                return self._read_sql_pyodbc(f"SELECT TOP {rows} * FROM {table_quoted}", conn)
            finally:
                conn.close()

        conn = self._connect()
        try:
            conn.Open()
            table_quoted = self._quote_two_part_name(table)
            cmd = SqlCommand(f"SELECT TOP {rows} * FROM {table_quoted}", conn)
            reader = cmd.ExecuteReader()
            return self._read_reader_to_df(reader)
        finally:
            conn.Close()

    def get_table_schema(self, table: str) -> List[Dict[str, Any]]:
        """
        Return column schema using INFORMATION_SCHEMA.COLUMNS.
        """
        if "." in table:
            schema, name = table.split(".", 1)
        else:
            schema, name = "dbo", table
        sql = """
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        if self.use_pyodbc:
            conn = self._pyodbc_connect()
            try:
                cursor = conn.cursor()
                cursor.execute(sql, (schema, name))
                cols = []
                for r in cursor.fetchall():
                    cols.append({
                        "name": str(r[0]),
                        "type": str(r[1]),
                        "nullable": str(r[2]),
                        "max_length": (None if r[3] is None else int(r[3])),
                        "precision": (None if r[4] is None else int(r[4])),
                        "scale": (None if r[5] is None else int(r[5])),
                    })
                return cols
            finally:
                conn.close()

        sql_dotnet = sql.replace("?", "@schema", 1)
        sql_dotnet = sql_dotnet.replace("?", "@table", 1)
        conn = self._connect()
        try:
            conn.Open()
            cmd = SqlCommand(sql_dotnet, conn)
            cmd.Parameters.AddWithValue("@schema", schema)
            cmd.Parameters.AddWithValue("@table", name)
            reader = cmd.ExecuteReader()
            cols: List[Dict[str, Any]] = []
            while reader.Read():
                cols.append(
                    {
                        "name": reader.GetString(0),
                        "type": reader.GetString(1),
                        "nullable": reader.GetString(2),
                        "max_length": (None if reader.IsDBNull(3) else int(reader.GetValue(3))),
                        "precision": (None if reader.IsDBNull(4) else int(reader.GetValue(4))),
                        "scale": (None if reader.IsDBNull(5) else int(reader.GetValue(5))),
                    }
                )
            return cols
        finally:
            conn.Close()

    def execute_select(self, sql: str, max_rows: Optional[int] = None) -> pd.DataFrame:
        """
        Execute a SELECT query (read-only). Reject non-SELECT statements.
        """
        q = (sql or "").strip()
        q = q.rstrip().rstrip(";").rstrip()
        if ";" in q:
            raise ValueError("Only a single SELECT statement is allowed.")
        q_norm = q.lower()
        if re.search(r"(?im)^[ \t]*go[ \t]*$", q):
            raise ValueError("Only a single SELECT statement is allowed.")
        if not (q_norm.startswith("select") or q_norm.startswith("with")):
            raise ValueError("Only SELECT queries are allowed.")
        banned = ("insert", "update", "delete", "merge", "drop", "alter", "create", "exec", "execute", "truncate")
        if any(re.search(rf"\b{kw}\b", q_norm) for kw in banned):
            raise ValueError("Query contains forbidden keywords.")
        if max_rows is not None:
            max_rows = int(max_rows) if max_rows and max_rows > 0 else 0
            if max_rows > 0 and " top " not in q_norm[:120]:
                q = f"SELECT TOP {max_rows} * FROM ({q}) AS q"

        if self.use_pyodbc:
            conn = self._pyodbc_connect()
            try:
                return self._read_sql_pyodbc(q, conn)
            finally:
                conn.close()

        conn = self._connect()
        try:
            conn.Open()
            cmd = SqlCommand(q, conn)
            reader = cmd.ExecuteReader()
            return self._read_reader_to_df(reader)
        finally:
            conn.Close()

    def compute_column_stats(self, table: str) -> Dict[str, Any]:
        """
        S4-02: SQL Server Pushdown — compute per-column aggregate stats directly
        on the server to avoid pulling all rows into memory.
        """
        table_quoted = self._quote_two_part_name(table)

        if "." in table:
            schema, name = table.split(".", 1)
        else:
            schema, name = "dbo", table

        if self.use_pyodbc:
            conn = self._pyodbc_connect()
            try:
                cursor = conn.cursor()
                schema_sql = """
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """
                cursor.execute(schema_sql, (schema, name))
                col_info = [{"name": str(r[0]), "data_type": str(r[1])} for r in cursor.fetchall()]
                if not col_info:
                    return {"row_count": 0, "columns": {}}

                agg_parts = ["COUNT(*) AS [__row_count__]"]
                for ci in col_info:
                    cname = ci["name"]
                    quoted = f"[{cname}]"
                    agg_parts.append(f"SUM(CASE WHEN {quoted} IS NULL THEN 1 ELSE 0 END) AS [{cname}__null_count]")
                    agg_parts.append(f"COUNT(DISTINCT {quoted}) AS [{cname}__distinct_count]")
                    agg_parts.append(f"CAST(MIN({quoted}) AS NVARCHAR(MAX)) AS [{cname}__min_value]")
                    agg_parts.append(f"CAST(MAX({quoted}) AS NVARCHAR(MAX)) AS [{cname}__max_value]")

                agg_sql = f"SELECT {', '.join(agg_parts)} FROM {table_quoted}"
                cursor.execute(agg_sql)
                row = cursor.fetchone()
                row_count = int(row[0]) if row and row[0] is not None else 0
                result: Dict[str, Any] = {"row_count": row_count, "columns": {}}
                col_names = [col[0] for col in cursor.description]
                val_map = dict(zip(col_names, row)) if row else {}

                for ci in col_info:
                    cname = ci["name"]
                    null_count = int(val_map.get(f"{cname}__null_count") or 0)
                    distinct_count = int(val_map.get(f"{cname}__distinct_count") or 0)
                    min_val = str(val_map.get(f"{cname}__min_value")) if val_map.get(f"{cname}__min_value") is not None else None
                    max_val = str(val_map.get(f"{cname}__max_value")) if val_map.get(f"{cname}__max_value") is not None else None
                    null_pct = (null_count / row_count) if row_count > 0 else 0.0
                    result["columns"][cname] = {
                        "null_count": null_count,
                        "null_percentage": round(null_pct, 6),
                        "distinct_count": distinct_count,
                        "min_value": min_val,
                        "max_value": max_val,
                        "sql_data_type": ci["data_type"],
                    }
                return result
            finally:
                conn.close()

        conn = self._connect()
        try:
            conn.Open()
            schema_sql = """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
            ORDER BY ORDINAL_POSITION
            """
            cmd = SqlCommand(schema_sql, conn)
            cmd.Parameters.AddWithValue("@schema", schema)
            cmd.Parameters.AddWithValue("@table", name)
            reader = cmd.ExecuteReader()
            col_info: List[Dict[str, str]] = []
            while reader.Read():
                col_info.append({
                    "name": reader.GetString(0),
                    "data_type": reader.GetString(1),
                })
            reader.Close()

            if not col_info:
                return {"row_count": 0, "columns": {}}

            agg_parts = ["COUNT(*) AS [__row_count__]"]
            for ci in col_info:
                cname = ci["name"]
                quoted = f"[{cname}]"
                agg_parts.append(f"SUM(CASE WHEN {quoted} IS NULL THEN 1 ELSE 0 END) AS [{cname}__null_count]")
                agg_parts.append(f"COUNT(DISTINCT {quoted}) AS [{cname}__distinct_count]")
                agg_parts.append(f"CAST(MIN({quoted}) AS NVARCHAR(MAX)) AS [{cname}__min_value]")
                agg_parts.append(f"CAST(MAX({quoted}) AS NVARCHAR(MAX)) AS [{cname}__max_value]")

            agg_sql = f"SELECT {', '.join(agg_parts)} FROM {table_quoted}"
            cmd2 = SqlCommand(agg_sql, conn)
            reader2 = cmd2.ExecuteReader()

            result: Dict[str, Any] = {"row_count": 0, "columns": {}}
            if reader2.Read():
                row_count = int(reader2.GetValue(0)) if not reader2.IsDBNull(0) else 0
                result["row_count"] = row_count

                for ci in col_info:
                    cname = ci["name"]
                    null_count_idx = reader2.GetOrdinal(f"{cname}__null_count")
                    distinct_idx = reader2.GetOrdinal(f"{cname}__distinct_count")
                    min_idx = reader2.GetOrdinal(f"{cname}__min_value")
                    max_idx = reader2.GetOrdinal(f"{cname}__max_value")

                    null_count = int(reader2.GetValue(null_count_idx)) if not reader2.IsDBNull(null_count_idx) else 0
                    distinct_count = int(reader2.GetValue(distinct_idx)) if not reader2.IsDBNull(distinct_idx) else 0
                    min_val = str(reader2.GetValue(min_idx)) if not reader2.IsDBNull(min_idx) else None
                    max_val = str(reader2.GetValue(max_idx)) if not reader2.IsDBNull(max_idx) else None

                    null_pct = (null_count / row_count) if row_count > 0 else 0.0

                    result["columns"][cname] = {
                        "null_count": null_count,
                        "null_percentage": round(null_pct, 6),
                        "distinct_count": distinct_count,
                        "min_value": min_val,
                        "max_value": max_val,
                        "sql_data_type": ci["data_type"],
                    }

            reader2.Close()
            return result
        finally:
            conn.Close()