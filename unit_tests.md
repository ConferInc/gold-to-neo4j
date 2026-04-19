# Unit Test Plan: Gold-to-Neo4j

## Test Framework

- **pytest** as test runner
- **unittest.mock** / `pytest-mock` for mocking Supabase/Neo4j clients and HTTP calls
- **httpx.MockTransport** or `respx` for mocking HTTP in `agent_client.py` and `semantic_embeddings.py`
- **fastapi.testclient.TestClient** for agent gateway route tests
- **pytest-env** for controlling env vars like `NEO4J_URI`, `LITELLM_BASE_URL`, etc.

---

## Priority Order

1. `shared/upsert.py` — data integrity, pure functions, easy to test
2. `shared/retry.py` — foundational, pure logic
3. `shared/schema_validation.py` — scoring functions are pure; critical to data quality
4. `services/agent_gateway/tools.py` — all pure functions
5. `services/agent_gateway/graphs.py` — validate/fallback logic (mock LLM calls)
6. `shared/semantic_embeddings.py` — text building pure; mock API calls
7. `shared/agent_client.py` — mock HTTP
8. `services/catalog_batch/run.py` — mock filesystem and layer mains
9. `services/customer_realtime/handlers.py` — validation logic
10. FastAPI routes (`service.py`) — use TestClient

---

## `shared/retry.py` — `run_with_retry()`

| Test | What it verifies |
|---|---|
| `test_succeeds_on_first_attempt` | Returns value immediately, no retry |
| `test_retries_on_transient_error` | Retries up to `attempts`, returns on 2nd success |
| `test_raises_after_max_attempts` | Exhausts retries and re-raises last exception |
| `test_non_retryable_predicate_aborts_immediately` | When `non_retryable(exc)` is True, stops without retrying |
| `test_backoff_delay_applied` | Verifies `time.sleep` called with `backoff_seconds` between retries |

---

## `shared/neo4j_client.py` — `Neo4jClient`

| Test | What it verifies |
|---|---|
| `test_is_auth_error_true` | Auth exception strings match expected error codes |
| `test_is_auth_error_false` | Unrelated exceptions return False |
| `test_is_non_retryable_write_error_true` | Schema/constraint error strings match |
| `test_is_non_retryable_write_error_false` | Transient errors return False |
| `test_from_env_reads_env_vars` | `from_env()` reads `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`, `NEO4J_DATABASE` |
| `test_query_returns_list_of_dicts` | Mocked driver, verify records are converted to dicts |
| `test_execute_many_builds_unwind_cypher` | Verify `UNWIND` statement constructed and called |
| `test_count_nodes_extracts_count` | Verify label query and returned integer |
| `test_fetch_sample_ids_returns_list` | Verify limit applied and IDs returned |

---

## `shared/supabase_client.py`

| Test | What it verifies |
|---|---|
| `test_fetch_all_paginates_correctly` | Multiple pages of 1000 rows are concatenated |
| `test_fetch_all_applies_filters` | Filter key=value pairs passed to query |
| `test_fetch_all_incremental_with_updated_since` | `updated_at_column >= updated_since` filter applied |
| `test_fetch_pending_events_respects_retry_schedule` | `next_retry_at <= now` filter applied |
| `test_mark_event_processed_updates_status` | Correct `status="processed"` update called |
| `test_mark_event_failed_sets_error_fields` | `error_code`, `error_message`, `status="failed"` |
| `test_mark_event_retry_sets_next_retry_at` | `next_retry_at` and `retry_count` persisted |
| `test_mark_event_needs_review` | `status="needs_review"` update called |
| `test_count_rows_returns_int` | Extracts integer from count response |

---

## `shared/schema_validation.py`

| Test | What it verifies |
|---|---|
| `test_tokenize_splits_on_underscores_and_caps` | `ingredient_id` → `['ingredient', 'id']` |
| `test_name_similarity_exact_match` | Score of 1.0 for identical names |
| `test_name_similarity_partial_match` | Score between 0–1 for similar names |
| `test_token_overlap_full_overlap` | Returns 1.0 when all tokens match |
| `test_suffix_bonus_id_column` | Extra score when both expected and candidate end with `_id` |
| `test_default_name_score_weighted_sum` | Confirms 0.7*sim + 0.2*overlap + bonus formula |
| `test_type_match_exact` | Returns 1.0 for `text` vs `text` |
| `test_type_match_compatible` | Returns 0.7 for `varchar` vs `text` |
| `test_type_match_incompatible` | Returns 0.0 for `integer` vs `boolean` |
| `test_value_shape_match_integer` | Numeric-looking values match integer type |
| `test_value_shape_match_uuid` | UUID-format strings match uuid type |
| `test_normalize_rows_renames_keys` | Alias map `{old: new}` applied in-place to all rows |
| `test_validate_agent_aliases_rejects_unknown_columns` | Aliases pointing to nonexistent columns are dropped |
| `test_validate_agent_aliases_rejects_already_mapped` | Aliases duplicating existing mappings are dropped |

---

## `shared/upsert.py`

| Test | What it verifies |
|---|---|
| `test_apply_filters_keeps_matching_rows` | `key=value` condition correctly filters rows |
| `test_apply_filters_removes_non_matching_rows` | Rows not matching any filter are excluded |
| `test_apply_filters_empty_filters_returns_all` | No filters → all rows returned |
| `test_build_node_rows_skips_null_primary_key` | Rows where `key_field` is None are excluded |
| `test_build_node_rows_selects_columns` | Only configured columns appear in output |
| `test_build_node_rows_includes_extra_fields` | `include_extra_fields=True` includes all columns |
| `test_build_rel_rows_extracts_from_to_keys` | Returns list of `{from_id, to_id}` dicts |
| `test_build_rel_rows_skips_null_keys` | Rows with null `from_id` or `to_id` skipped |
| `test_upsert_from_config_calls_execute_many_for_nodes` | `execute_many` called with MERGE Cypher for each table |
| `test_upsert_from_config_calls_execute_many_for_relationships` | MERGE relationship Cypher called for each rel config |

---

## `shared/semantic_embeddings.py`

| Test | What it verifies |
|---|---|
| `test_stringify_value_handles_list` | List joined with separator |
| `test_stringify_value_handles_none` | None → empty string |
| `test_build_text_from_node_concatenates_properties` | Properties joined with separator in order |
| `test_build_text_from_node_skips_missing_properties` | Missing keys do not raise, just skipped |
| `test_normalize_label_name_removes_special_chars` | `User:Profile` → `UserProfile` |
| `test_load_embedding_config_reads_yaml` | Returns dict from YAML file |
| `test_get_semantic_rules_extracts_label_rules` | Returns `label_text_rules` dict from config |
| `test_embed_via_http_routes_to_litellm_proxy` | When `LITELLM_BASE_URL` set, uses HTTP client |
| `test_embed_texts_python_falls_back_to_litellm_library` | Without base URL, uses LiteLLM library |
| `test_write_semantic_embeddings_batches_by_env_size` | `EMBEDDING_BATCH_SIZE=2`, 6 texts → 3 API calls |
| `test_write_semantic_embeddings_retries_on_429` | Rate limit error triggers exponential backoff |
| `test_write_semantic_embeddings_gives_up_after_max_retries` | Exceeds `EMBEDDING_MAX_RETRIES`, raises |
| `test_write_semantic_embeddings_writes_vectors_to_neo4j` | UNWIND Cypher with embeddings called |

---

## `shared/embedding_validation.py`

| Test | What it verifies |
|---|---|
| `test_normalize_strips_non_alphanumeric` | `"User-Profile"` → `"userprofile"` |
| `test_similarity_exact_match` | Returns 1.0 |
| `test_resolve_names_exact_match` | `"Recipe"` resolved from `["Recipe"]` |
| `test_resolve_names_normalized_match` | `"recipe"` matches `"Recipe"` via normalization |
| `test_resolve_names_similarity_threshold` | Name above 0.88 threshold resolved, below is missing |
| `test_candidate_hints_returns_top_k` | Returns top-K similar names with scores |
| `test_numeric_property_true_for_float` | `["Float"]` → True |
| `test_numeric_property_false_for_string` | `["String"]` → False |

---

## `shared/agent_client.py`

| Test | What it verifies |
|---|---|
| `test_call_agent_posts_to_correct_url` | HTTP POST sent to `{AGENT_BASE_URL}/agent` |
| `test_call_agent_returns_json_on_success` | 200 response body parsed as dict |
| `test_call_agent_returns_none_on_http_error` | Non-200 / network error → returns None |
| `test_resolve_schema_drift_sets_task_field` | Payload includes `task="schema_drift_resolver"` |
| `test_triage_failure_sets_task_field` | Payload includes `task="failure_triage"` |
| `test_propose_reconciliation_sets_task_field` | Payload includes `task="reconciliation_backfill"` |
| `test_resolve_embedding_config_sets_task_field` | Payload includes `task="embedding_config_resolver"` |

---

## `services/catalog_batch/run.py`

| Test | What it verifies |
|---|---|
| `test_acquire_lock_creates_file` | Lock file created at `state/locks/{layer}.lock` |
| `test_acquire_lock_raises_if_already_locked` | Second acquire raises an error |
| `test_release_lock_deletes_file` | Lock file removed after release |
| `test_ensure_state_file_creates_if_missing` | New state JSON file created with empty structure |
| `test_run_layer_returns_success_metrics` | `status="success"`, `duration_ms` positive float |
| `test_run_layer_returns_failed_on_exception` | Exception sets `status="failed"`, `error` populated |
| `test_main_runs_all_layers_in_order` | `--layer all` calls each of 4 layers in `LAYER_ORDER` |
| `test_main_returns_1_on_any_layer_failure` | One failing layer → exit code 1 |

---

## `services/catalog_batch/status.py`

| Test | What it verifies |
|---|---|
| `test_layer_status_missing_state_file` | `exists=False` when no state file |
| `test_layer_status_with_state_file` | Returns `last_checkpoint` and `tables` from JSON |
| `test_main_json_flag_outputs_json` | `--json` flag produces valid JSON to stdout |

---

## `services/*/service.py` (ingredients / recipes / products / customers)

| Test | What it verifies |
|---|---|
| `test_load_config_parses_yaml` | Returns dict from layer YAML |
| `test_load_state_returns_empty_dict_if_missing` | Missing state file → `{}` |
| `test_save_state_writes_json` | State dict serialized to JSON at path |
| `test_max_checkpoint_returns_latest_timestamp` | Returns max ISO timestamp across tables |
| `test_max_checkpoint_returns_none_if_empty` | Empty state → None |

---

## `services/customer_realtime/handlers.py`

| Test | What it verifies |
|---|---|
| `test_validate_payload_raises_if_not_dict` | Non-dict payload → `EventValidationError` |
| `test_validate_payload_raises_if_id_field_is_null` | `customer_id=None` → `EventValidationError` |
| `test_validate_payload_passes_with_valid_ids` | Valid payload → no exception |
| `test_handle_event_extracts_event_type` | `event.event_type` used to route handler |
| `test_handle_event_marks_processed_on_success` | `mark_event_processed` called after success |
| `test_handle_event_raises_validation_error_on_bad_payload` | Bad payload re-raises `EventValidationError` |

---

## `services/customer_realtime/service.py`

| Test | What it verifies |
|---|---|
| `test_is_non_retryable_error_true_for_validation_error` | `EventValidationError` → True |
| `test_is_non_retryable_error_true_for_auth_error` | Auth error → True |
| `test_is_non_retryable_error_false_for_network_error` | Transient error → False |

---

## `services/agent_gateway/tools.py`

| Test | What it verifies |
|---|---|
| `test_score_similarity_exact` | Returns 1.0 |
| `test_score_similarity_partial` | Returns value in (0, 1) |
| `test_top_name_candidates_returns_k_results` | Returns exactly k candidates sorted by score |
| `test_top_name_candidates_handles_fewer_than_k` | Less than k available → returns all |
| `test_normalize_type_lowercases_and_strips` | `" TEXT "` → `"text"` |
| `test_type_compatible_synonyms` | `varchar` compatible with `text` |
| `test_type_compatible_incompatible` | `integer` not compatible with `boolean` |
| `test_classify_error_basic_timeout` | Timeout keyword → `"retryable"` |
| `test_classify_error_basic_constraint` | Constraint keyword → `"poison"` |
| `test_compute_drift_ratio_zero` | Equal counts → 0.0 |
| `test_compute_drift_ratio_nonzero` | Source=100, target=80 → 0.2 |
| `test_compute_drift_ratio_zero_source` | Source=0 → 0.0 (no division by zero) |

---

## `services/agent_gateway/graphs.py`

| Test | What it verifies |
|---|---|
| `test_schema_candidates_builds_hints_dict` | Returns dict of `{missing_col: [(candidate, score)...]}` |
| `test_schema_validate_extracts_aliases_from_llm_output` | Valid JSON string → dict of aliases |
| `test_schema_validate_handles_invalid_llm_output` | Non-JSON output → empty aliases, no crash |
| `test_triage_validate_falls_back_to_basic_classification` | Invalid LLM output → `classify_error_basic()` used |
| `test_triage_validate_sets_default_retry_seconds` | `retry_in_seconds` defaults to valid integer |
| `test_reconcile_check_needs_action_above_threshold` | Drift ratio >= threshold → `needs_action=True` |
| `test_reconcile_check_no_action_below_threshold` | Drift ratio < threshold → `needs_action=False` |
| `test_reconcile_validate_defaults_to_observe` | No action needed → `action="observe"` |
| `test_embed_config_candidates_builds_hints` | Returns label and relationship hint dicts |
| `test_embed_config_validate_extracts_aliases` | Valid JSON → `label_aliases` and `relationship_aliases` |
| `test_embed_config_validate_handles_invalid_output` | Non-JSON → empty aliases, no crash |

---

## `services/agent_gateway/service.py`

| Test | What it verifies |
|---|---|
| `test_health_endpoint_returns_ok` | `GET /health` → `{"status": "ok"}` |
| `test_agent_endpoint_routes_schema_drift` | `task="schema_drift_resolver"` invokes schema drift graph |
| `test_agent_endpoint_routes_failure_triage` | `task="failure_triage"` invokes triage graph |
| `test_agent_endpoint_routes_reconciliation` | `task="reconciliation_backfill"` invokes reconciliation graph |
| `test_agent_endpoint_routes_embedding_config` | `task="embedding_config_resolver"` invokes embedding config graph |
| `test_agent_endpoint_returns_422_on_invalid_request` | Missing required fields → 422 |

---

## `services/reconciliation/service.py`

| Test | What it verifies |
|---|---|
| `test_hash_values_deterministic` | Same input → same SHA256 hash |
| `test_hash_values_different_for_different_input` | Different inputs → different hashes |
| `test_hash_values_pipe_separated` | Order of values matters (not a set hash) |
| `test_append_plan_writes_jsonl` | Record appended to `reconcile_plans.jsonl` |
| `test_load_state_returns_empty_on_missing_file` | Missing file → `{}` |
