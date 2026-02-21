FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN python -m venv /app/.venv \
 && . /app/.venv/bin/activate \
 && pip install --upgrade pip \
 && pip install -r /app/requirements.txt

COPY . /app

RUN chmod +x /app/scripts/entrypoint.sh /app/scripts/run_layer.sh

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
CMD ["__default__"]

# Optional healthcheck; reads state only, no DB calls.
HEALTHCHECK --interval=1m --timeout=10s --start-period=20s --retries=3 \
  CMD python -m services.catalog_batch.status --layer recipes || exit 1
