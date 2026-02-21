**Run Locally**
- `source .env && python -m services.catalog_batch.run --layer recipes`
- `scripts/run_layer.sh recipes` (loads `.env` if present)

**Run in Container**
- Build: `docker build -t gold-neo4j-ingest:latest .`
- Recipes layer: `docker run --rm --env-file .env -e LAYER=recipes -v $(pwd)/state:/app/state gold-neo4j-ingest:latest`
- All layers: `docker run --rm --env-file .env -e LAYER=all -v $(pwd)/state:/app/state gold-neo4j-ingest:latest`
- Notes: mount `state/` (and `state/run_summaries/`) to persist checkpoints and summaries between runs.

**Locking & Concurrency**
- Per-layer lock files are written to `state/locks/<layer>.lock`; parallel runs of the same layer will fail fast.

**Run Summaries & Checkpoints**
- Summaries: `state/run_summaries/<layer>.jsonl`
- Checkpoints: `state/<layer>_state.json`

**Env Vars**
- Required: `SUPABASE_URL`, `SUPABASE_KEY`, `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`
- Optional: `NEO4J_DATABASE` (defaults `neo4j`), `LAYER` (container default `all`)
