# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

ENV PIP_NO_CACHE_DIR=1

WORKDIR /build

COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --prefix=/install -r requirements.txt


# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY . /app

RUN chmod +x /app/scripts/entrypoint.sh /app/scripts/run_layer.sh

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
CMD ["__default__"]

HEALTHCHECK --interval=1m --timeout=10s --start-period=20s --retries=3 \
  CMD python -m services.catalog_batch.status --layer recipes || exit 1
