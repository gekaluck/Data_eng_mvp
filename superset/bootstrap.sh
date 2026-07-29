#!/usr/bin/env bash
set -euo pipefail

: "${SUPERSET_ADMIN_USERNAME:?SUPERSET_ADMIN_USERNAME is required}"
: "${SUPERSET_ADMIN_PASSWORD:?SUPERSET_ADMIN_PASSWORD is required}"
: "${SUPERSET_ADMIN_FIRSTNAME:?SUPERSET_ADMIN_FIRSTNAME is required}"
: "${SUPERSET_ADMIN_LASTNAME:?SUPERSET_ADMIN_LASTNAME is required}"
: "${SUPERSET_ADMIN_EMAIL:?SUPERSET_ADMIN_EMAIL is required}"

superset db upgrade

if ! superset fab list-users | grep -Fq "${SUPERSET_ADMIN_USERNAME}"; then
    superset fab create-admin \
        --username "${SUPERSET_ADMIN_USERNAME}" \
        --password "${SUPERSET_ADMIN_PASSWORD}" \
        --firstname "${SUPERSET_ADMIN_FIRSTNAME}" \
        --lastname "${SUPERSET_ADMIN_LASTNAME}" \
        --email "${SUPERSET_ADMIN_EMAIL}"
fi

superset init

python /app/project-superset/bootstrap_assets.py

echo "Superset metadata, admin user, datasets, charts, and dashboard are ready."
