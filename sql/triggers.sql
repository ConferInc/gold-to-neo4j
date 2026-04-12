-- ═══════════════════════════════════════════════════════════════
-- Supabase triggers for gold.outbox_events
-- Shared trigger function + per-table triggers.
--
-- ⚠️  REFERENCE ONLY — Do NOT execute until:
--     1. All pipeline code changes are deployed (Phases 0-3)
--     2. The orchestrator container is running on Coolify
--     3. The realtime worker is confirmed starting in logs
--     See: implementation_plan.md → Deferred Phase
-- ═══════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────
-- Shared trigger function
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION gold.fn_outbox_insert()
RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO gold.outbox_events (event_type, table_name, row_id, payload)
        VALUES (
            TG_TABLE_NAME || '.delete',
            TG_TABLE_NAME,
            OLD.id::text,
            to_jsonb(OLD)
        );
        RETURN OLD;
    ELSE
        INSERT INTO gold.outbox_events (event_type, table_name, row_id, payload)
        VALUES (
            TG_TABLE_NAME || '.' || lower(TG_OP),
            TG_TABLE_NAME,
            NEW.id::text,
            to_jsonb(NEW)
        );
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- ═══════════════════════════════════════════════════════════════
-- Node table triggers (16 tables)
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


-- ═══════════════════════════════════════════════════════════════
-- Join table triggers (3 tables — for relationship edges)
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
