# ---------------------------------------------------------------------------
# Exclude the output/ directory from Uvicorn's watchfiles reloader so that
# writing generated ETL code files does not trigger a server restart mid-request.
# This monkeypatch runs in the parent (supervisor) process because Uvicorn
# imports the application module before starting the WatchFilesReload loop.
# ---------------------------------------------------------------------------
try:
    import watchfiles as _wf  # noqa: E402
    import os as _os

    _OrigDefault = _wf.filters.DefaultFilter
    _OrigPython = _wf.filters.PythonFilter

    class _NoOutputDefault(_OrigDefault):
        def __call__(self, change, path):
            _norm = _os.path.normpath(path).replace("\\", "/")
            if "output" in _norm.split("/"):
                return False
            return super().__call__(change, path)

    class _NoOutputPython(_OrigPython):
        def __call__(self, change, path):
            _norm = _os.path.normpath(path).replace("\\", "/")
            if "output" in _norm.split("/"):
                return False
            return super().__call__(change, path)

    _wf.filters.DefaultFilter = _NoOutputDefault
    _wf.DefaultFilter = _NoOutputDefault
    _wf.filters.PythonFilter = _NoOutputPython
    _wf.PythonFilter = _NoOutputPython
except Exception:
    pass
# ---------------------------------------------------------------------------

from agent.extraction_agent import ExtractionAgent
from agent.master_agent import MasterAgent

__all__ = ["MasterAgent", "ExtractionAgent"]
