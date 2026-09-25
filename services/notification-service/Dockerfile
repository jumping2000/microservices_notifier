FROM python:3.14-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY shared/ ./shared/
# The root workspace names gateway explicitly; uv needs its manifest to resolve.
COPY gateway/pyproject.toml ./gateway/pyproject.toml
COPY services/notification-service/ ./services/notification-service/
RUN uv sync --frozen --no-dev --no-editable --package notification-service

FROM python:3.14-slim AS runtime
RUN adduser --disabled-password appuser && apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder --chown=appuser /app/.venv /app/.venv
COPY --chown=appuser services/notification-service/ /app/
RUN chmod +x /app/entrypoint.sh
USER appuser
EXPOSE 8000
ENTRYPOINT ["/app/entrypoint.sh"]
