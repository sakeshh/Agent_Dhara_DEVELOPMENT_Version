"""
Diagnostic script to check:
1. Azure SQL: Accounts, Accounts_Clean, Accounts_Transformed table states
2. etl_log entries
3. Blob storage: list blobs in agentdhararawdata and output containers
4. Fabric mirror env vars
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

print("=" * 60)
print("1. AZURE SQL DATABASE INSPECTION")
print("=" * 60)

try:
    from agent.azure_sql_executor import get_connection
    import pandas as pd
    
    conn = get_connection()
    
    # Check table existence and row counts
    tables_to_check = [
        "dbo.Accounts",
        "dbo.Accounts_Clean",
        "dbo.Accounts_Transformed",
        "dbo.etl_log",
        "dbo.etl_rejects",
    ]
    
    for tbl in tables_to_check:
        try:
            df = pd.read_sql(f"SELECT COUNT(*) AS cnt FROM [{tbl.split('.')[-1]}]", conn)
            print(f"  {tbl}: {df['cnt'].iloc[0]} rows")
        except Exception as e:
            print(f"  {tbl}: DOES NOT EXIST or ERROR - {e}")
    
    print("\n--- etl_log (last 10 entries) ---")
    try:
        df_log = pd.read_sql(
            "SELECT TOP 10 id, process_name, start_time, end_time, status, "
            "LEFT(error_message, 200) AS error_msg FROM dbo.etl_log ORDER BY id DESC", conn
        )
        for _, row in df_log.iterrows():
            print(f"  [{row['id']}] {row['process_name']} | {row['status']} | "
                  f"start={row['start_time']} | end={row['end_time']} | err={row['error_msg']}")
    except Exception as e:
        print(f"  Could not query etl_log: {e}")
    
    print("\n--- etl_rejects (last 5 entries) ---")
    try:
        df_rej = pd.read_sql(
            "SELECT TOP 5 id, process_name, table_name, "
            "LEFT(error_reason, 200) AS reason, rejected_at FROM dbo.etl_rejects ORDER BY id DESC", conn
        )
        for _, row in df_rej.iterrows():
            print(f"  [{row['id']}] {row['process_name']} | {row['table_name']} | {row['reason']}")
    except Exception as e:
        print(f"  Could not query etl_rejects: {e}")
    
    # Check sample data from Accounts_Clean
    print("\n--- Accounts_Clean sample (top 3 rows) ---")
    try:
        df_sample = pd.read_sql("SELECT TOP 3 * FROM [dbo].[Accounts_Clean]", conn)
        if df_sample.empty:
            print("  TABLE IS EMPTY!")
        else:
            print(df_sample.to_string(index=False))
    except Exception as e:
        print(f"  Could not query Accounts_Clean: {e}")
    
    conn.close()
except Exception as e:
    print(f"  Database connection failed: {e}")

print("\n" + "=" * 60)
print("2. BLOB STORAGE INSPECTION")
print("=" * 60)

try:
    from connectors.azure_blob_storage import AzureBlobStorageConnector
    
    # Check raw container
    raw_container = os.getenv("AZURE_ASSESSMENT_CONTAINER", "agentdhararawdata")
    print(f"\n--- Raw container: {raw_container} ---")
    try:
        blob_raw = AzureBlobStorageConnector({"container": raw_container})
        raw_blobs = blob_raw.list_blobs()
        for b in raw_blobs:
            print(f"  {b}")
        if not raw_blobs:
            print("  (empty)")
    except Exception as e:
        print(f"  Error listing raw blobs: {e}")
    
    # Check output container
    output_container = os.getenv("AZURE_OUTPUT_CONTAINER", "output")
    print(f"\n--- Output container: {output_container} ---")
    try:
        blob_out = AzureBlobStorageConnector({"container": output_container})
        out_blobs = blob_out.list_blobs()
        for b in out_blobs:
            print(f"  {b}")
        if not out_blobs:
            print("  (empty)")
    except Exception as e:
        print(f"  Error listing output blobs: {e}")
    
    # Check for "cleaned/" prefix in any container
    print(f"\n--- 'cleaned/' blobs in raw container ---")
    try:
        cleaned_blobs = [b for b in raw_blobs if b.startswith("cleaned/")]
        for b in cleaned_blobs:
            print(f"  {b}")
        if not cleaned_blobs:
            print("  (none found)")
    except Exception as e:
        print(f"  Error: {e}")

except Exception as e:
    print(f"  Blob connector error: {e}")

print("\n" + "=" * 60)
print("3. FABRIC MIRROR CONFIGURATION")
print("=" * 60)

mirror_enabled = os.getenv("DHARA_FABRIC_MIRROR_ENABLED")
workspace = os.getenv("FABRIC_WORKSPACE_ID") or os.getenv("FABRIC_WORKSPACE_NAME")
lakehouse = os.getenv("FABRIC_LAKEHOUSE_NAME") or os.getenv("FABRIC_LAKEHOUSE_ID")
tenant = os.getenv("FABRIC_TENANT_ID")
client_id = os.getenv("FABRIC_CLIENT_ID")
client_secret = os.getenv("FABRIC_CLIENT_SECRET")

print(f"  DHARA_FABRIC_MIRROR_ENABLED = {mirror_enabled}")
print(f"  FABRIC_WORKSPACE_ID         = {workspace}")
print(f"  FABRIC_LAKEHOUSE_NAME        = {lakehouse}")
print(f"  FABRIC_TENANT_ID             = {tenant}")
print(f"  FABRIC_CLIENT_ID             = {client_id or '(empty)'}")
print(f"  FABRIC_CLIENT_SECRET          = {'***set***' if client_secret else '(empty)'}")

if not client_id or not client_secret:
    print("\n  ⚠ WARNING: FABRIC_CLIENT_ID and FABRIC_CLIENT_SECRET are empty!")
    print("  The connector will try DefaultAzureCredential (Azure CLI 'az login').")
    print("  If 'az login' is not authenticated, Fabric mirror writes WILL FAIL.")

print("\n" + "=" * 60)
print("4. SESSION STATE (latest session)")
print("=" * 60)

try:
    from agent.session_store import load_session, list_sessions
    sessions = list_sessions(limit=3)
    if sessions:
        for s in sessions:
            sid = s.get("session_id", "unknown")
            print(f"\n  Session: {sid}")
            sess = load_session(sid)
            ctx = sess.get("context", {})
            flow = ctx.get("etl_flow", {})
            print(f"    phase: {flow.get('phase')}")
            print(f"    target_engine: {flow.get('target_engine')}")
            print(f"    validation_ok: {flow.get('validation_ok')}")
            
            exec_res = flow.get("sql_execution_result", {})
            if exec_res:
                print(f"    execution ok: {exec_res.get('ok')}")
                print(f"    stage: {exec_res.get('stage')}")
                post = exec_res.get("post_execution_summary", {})
                print(f"    committed: {post.get('transaction_committed')}")
                print(f"    rows affected: {post.get('total_rows_affected')}")
                print(f"    row_deltas: {post.get('row_deltas')}")
                if post.get("rollback_reason"):
                    print(f"    rollback_reason: {post.get('rollback_reason')}")
            
            fab_res = flow.get("fabric_mirror_result")
            if fab_res:
                print(f"    fabric_mirror ok: {fab_res.get('ok')}")
                for d in (fab_res.get("details") or []):
                    print(f"      - table={d.get('table', d.get('source_table', 'unknown'))}, "
                          f"ok={d.get('ok')}, error={d.get('error', 'none')}, msg={d.get('message', '')[:100]}")
            else:
                print(f"    fabric_mirror_result: NOT PRESENT")
    else:
        print("  No sessions found")
except Exception as e:
    print(f"  Error loading sessions: {e}")

print("\n" + "=" * 60)
print("DIAGNOSIS COMPLETE")
print("=" * 60)
