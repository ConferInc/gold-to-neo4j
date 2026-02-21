#!/usr/bin/env bash
set -euo pipefail

if [ -f ".env" ]; then
  # shellcheck disable=SC2046
  export $(grep -v '^#' .env | xargs)
fi

layer="${1:-all}"
exec python -m services.catalog_batch.run --layer "$layer"
