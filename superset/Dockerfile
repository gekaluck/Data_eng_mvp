# Superset is pinned so exported dashboard assets remain compatible across machines.
FROM apache/superset:6.0.0

USER root

# The lean upstream image intentionally omits database drivers. Install only the
# Postgres metadata driver and the Trino analytics driver required by this stack.
RUN . /app/.venv/bin/activate \
    && uv pip install --no-cache-dir psycopg2-binary trino

COPY --chown=superset:superset superset_config.py /app/pythonpath/superset_config.py

ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

USER superset
