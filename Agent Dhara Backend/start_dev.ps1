# start_dev.ps1
# Run this instead of uvicorn directly.
# --reload-dir agent ensures uvicorn only watches the source code directory,
# NOT the output/ directory where generated ETL files are written.
# Without this, every ETL code generation triggers a full server reload storm.
python -m uvicorn agent.mcp_server:app --host 127.0.0.1 --port 8000 --reload --reload-dir agent
