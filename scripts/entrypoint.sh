#!/usr/bin/env bash
set -euo pipefail

required_vars=(
  SUPABASE_URL
  SUPABASE_KEY
  NEO4J_URI
  NEO4J_USER
  NEO4J_PASSWORD
)

missing=()
for var in "${required_vars[@]}"; do
  if [ -z "${!var:-}" ]; then
    missing+=("$var")
  fi
done

if [ "${#missing[@]}" -ne 0 ]; then
  echo "Missing required env vars: ${missing[*]}" >&2
  exit 1
fi

if [ "$#" -eq 0 ] || [ "$1" = "__default__" ]; then
  layer="${LAYER:-all}"
  exec python -m services.catalog_batch.run --layer "$layer"
else
  exec "$@"
fi
