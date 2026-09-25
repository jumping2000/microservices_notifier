#!/bin/sh
set -e
exec /app/.venv/bin/uvicorn app.main:create_app --factory --host 0.0.0.0 --port 8000
