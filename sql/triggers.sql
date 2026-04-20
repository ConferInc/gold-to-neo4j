-- ═══════════════════════════════════════════════════════════════
-- Supabase triggers for gold.outbox_events
-- Generic trigger function using TG_ARGV for PK resolution.
--
-- ⚠️  REFERENCE ONLY — Do NOT execute until:
--     1. gold.outbox_events table exists (see outbox_table.sql)
--     2. The orchestrator container is running on Coolify
--     3. The realtime worker is confirmed starting in logs
-- ═══════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────
-- Shared trigger function (TG_ARGV-based PK resolution)
--
-- Usage:
--   Default (table has 'id' column):
--     EXECUTE FUNCTION gold.fn_outbox_insert();
--
--   Composite PK (no 'id' column):
--     EXECUTE FUNCTION gold.fn_outbox_insert('col_a', 'col_b');
--     → row_id = "val_a:val_b"
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION gold.fn_outbox_insert()
RETURNS trigger AS $$
DECLARE
    _row_id     text;
    _payload    jsonb;
    _event_type text;
BEGIN
    -- Build payload and event type
    IF TG_OP = 'DELETE' THEN
        _payload    := to_jsonb(OLD);
        _event_type := TG_TABLE_NAME || '.delete';
    ELSE
        _payload    := to_jsonb(NEW);
        _event_type := TG_TABLE_NAME || '.' || lower(TG_OP);
    END IF;

    -- Build row_id from TG_ARGV (PK columns) or default to 'id'
    IF TG_NARGS > 0 THEN
        _row_id := '';
        FOR i IN 0..TG_NARGS-1 LOOP
            IF i > 0 THEN _row_id := _row_id || ':'; END IF;
            _row_id := _row_id || COALESCE(_payload->>TG_ARGV[i], 'null');
        END LOOP;
    ELSE
        _row_id := COALESCE(_payload->>'id', 'unknown');
    END IF;

    INSERT INTO gold.outbox_events (event_type, table_name, row_id, payload)
    VALUES (_event_type, TG_TABLE_NAME, _row_id, _payload);

    IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END;
$$ LANGUAGE plpgsql;


-- ═══════════════════════════════════════════════════════════════
-- Node table triggers (23 tables)
-- ═══════════════════════════════════════════════════════════════

CREATE TRIGGER trg_outbox_b2c_customers
    AFTER INSERT OR UPDATE OR DELETE ON gold.b2c_customers
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_b2c_health_profiles
    AFTER INSERT OR UPDATE OR DELETE ON gold.b2c_customer_health_profiles
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_b2c_health_conditions
    AFTER INSERT OR UPDATE OR DELETE ON gold.b2c_customer_health_conditions
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_allergens
    AFTER INSERT OR UPDATE OR DELETE ON gold.allergens
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_dietary_preferences
    AFTER INSERT OR UPDATE OR DELETE ON gold.dietary_preferences
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_health_conditions
    AFTER INSERT OR UPDATE OR DELETE ON gold.health_conditions
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_households
    AFTER INSERT OR UPDATE OR DELETE ON gold.households
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_meal_logs
    AFTER INSERT OR UPDATE OR DELETE ON gold.meal_logs
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_meal_log_items
    AFTER INSERT OR UPDATE OR DELETE ON gold.meal_log_items
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_scan_history
    AFTER INSERT OR UPDATE OR DELETE ON gold.scan_history
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_meal_plans
    AFTER INSERT OR UPDATE OR DELETE ON gold.meal_plans
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_meal_plan_items
    AFTER INSERT OR UPDATE OR DELETE ON gold.meal_plan_items
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_household_budgets
    AFTER INSERT OR UPDATE OR DELETE ON gold.household_budgets
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_meal_log_streaks
    AFTER INSERT OR UPDATE OR DELETE ON gold.meal_log_streaks
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_b2b_customers
    AFTER INSERT OR UPDATE OR DELETE ON gold.b2b_customers
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_b2b_health_profiles
    AFTER INSERT OR UPDATE OR DELETE ON gold.b2b_customer_health_profiles
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

-- NEW: 7 node tables added for full outbox coverage

CREATE TRIGGER trg_outbox_b2c_settings
    AFTER INSERT OR UPDATE OR DELETE ON gold.b2c_customer_settings
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_weight_history
    AFTER INSERT OR UPDATE OR DELETE ON gold.b2c_customer_weight_history
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_recipe_ratings
    AFTER INSERT OR UPDATE OR DELETE ON gold.recipe_ratings
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_shopping_lists
    AFTER INSERT OR UPDATE OR DELETE ON gold.shopping_lists
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_shopping_list_items
    AFTER INSERT OR UPDATE OR DELETE ON gold.shopping_list_items
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_household_preferences
    AFTER INSERT OR UPDATE OR DELETE ON gold.household_preferences
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_chat_sessions
    AFTER INSERT OR UPDATE OR DELETE ON gold.chat_sessions
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();


-- ═══════════════════════════════════════════════════════════════
-- Join table triggers (4 tables — for relationship edges)
-- ═══════════════════════════════════════════════════════════════

CREATE TRIGGER trg_outbox_b2c_customer_allergens
    AFTER INSERT OR DELETE ON gold.b2c_customer_allergens
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_b2c_customer_dietary_preferences
    AFTER INSERT OR DELETE ON gold.b2c_customer_dietary_preferences
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_customer_product_interactions
    AFTER INSERT OR UPDATE OR DELETE ON gold.customer_product_interactions
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

-- NEW: 4 join tables added for full outbox coverage

CREATE TRIGGER trg_outbox_cuisine_prefs
    AFTER INSERT OR DELETE ON gold.b2c_customer_cuisine_preferences
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert('b2c_customer_id', 'cuisine_id');

CREATE TRIGGER trg_outbox_b2b_allergens
    AFTER INSERT OR DELETE ON gold.b2b_customer_allergens
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_b2b_dietary_prefs
    AFTER INSERT OR DELETE ON gold.b2b_customer_dietary_preferences
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();

CREATE TRIGGER trg_outbox_b2b_health_conditions
    AFTER INSERT OR DELETE ON gold.b2b_customer_health_conditions
    FOR EACH ROW EXECUTE FUNCTION gold.fn_outbox_insert();
