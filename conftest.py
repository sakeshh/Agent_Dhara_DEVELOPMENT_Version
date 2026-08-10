"""Pytest conftest: ensure project root is on sys.path for test imports."""
import sys
import os

# Add project root to sys.path so `agent.*` and `tests.*` modules are importable
sys.path.insert(0, os.path.dirname(__file__))
