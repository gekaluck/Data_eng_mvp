"""Pytest configuration — adds dags/ to sys.path so schemas are importable."""

import sys
from pathlib import Path

# Allow imports like `from schemas.coincap import ...` in tests,
# mirroring how Airflow resolves imports inside the container.
dags_dir = str(Path(__file__).resolve().parent.parent / "dags")
if dags_dir not in sys.path:
    sys.path.insert(0, dags_dir)
