FROM python:3.14-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY shared/ ./shared/
COPY gateway/ ./gateway/
RUN uv sync --frozen --no-dev --no-editable --package gateway

FROM python:3.14-slim AS runtime
RUN adduser --disabled-password appuser && apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder --chown=appuser /app/.venv /app/.venv
COPY --chown=appuser gateway/ /app/
RUN chmod +x /app/entrypoint.sh
USER appuser
EXPOSE 8000
ENTRYPOINT ["/app/entrypoint.sh"]
