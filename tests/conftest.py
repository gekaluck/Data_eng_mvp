"""Pytest configuration for DAG and Spark test modules."""

import sys
from pathlib import Path


repo_root = Path(__file__).resolve().parent.parent
dags_dir = repo_root / "dags"

for path in (repo_root, dags_dir):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
