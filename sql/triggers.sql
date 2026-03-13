-- Customer interaction & profile outbox triggers
--
-- When customer behavioral or profile data changes in the gold schema, these
-- triggers write an event to the outbox_events table.  The customer_realtime
-- worker polls that table and pushes the changes to Neo4j in near-real-time.
--
-- Tables covered:
--   gold.customer_product_interactions  — viewed/rated/saved/rejected/whitelisted/blacklisted/tried
--   gold.b2c_customer_allergens         — customer ↔ allergen links (safety-critical)
--   gold.b2c_customer_dietary_preferences — customer ↔ diet links (safety-critical)
--   gold.b2c_customer_health_conditions — customer health conditions (safety-critical)
--   gold.b2c_customer_health_profiles   — customer health profile (safety-critical)
--
-- The outbox_events table must exist before running this script.
-- See sql/outbox_table.sql.

-- ---------------------------------------------------------------------------
-- 1. Enqueue function
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION gold.enqueue_outbox_event()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  _event_type text;
  _row_id     text;
  _payload    jsonb;
BEGIN
  -- TG_ARGV[0] carries the logical event type passed from each trigger definition
  -- (e.g. 'customer_interaction', 'customer_allergen', etc.)
  _event_type := TG_ARGV[0];

  -- Resolve row identity and payload for INSERT/UPDATE vs DELETE
  IF TG_OP = 'DELETE' THEN
    _row_id  := OLD.id::text;
    _payload := jsonb_build_object(
      'op',    'DELETE',
      'table', TG_TABLE_NAME,
      'old',   to_jsonb(OLD)
    );
  ELSE
    _row_id  := NEW.id::text;
    _payload := jsonb_build_object(
      'op',    TG_OP,
      'table', TG_TABLE_NAME,
      'new',   to_jsonb(NEW),
      'old',   CASE WHEN TG_OP = 'UPDATE' THEN to_jsonb(OLD) ELSE NULL END
    );
  END IF;

  INSERT INTO outbox_events (
    id,
    event_type,
    table_name,
    row_id,
    payload,
    status,
    retry_count,
    created_at
  ) VALUES (
    gen_random_uuid(),
    _event_type,
    TG_TABLE_NAME,
    _row_id,
    _payload,
    'pending',
    0,
    now()
  );

  RETURN COALESCE(NEW, OLD);
EXCEPTION WHEN OTHERS THEN
  -- Never let an outbox write failure abort the originating transaction.
  RAISE WARNING 'enqueue_outbox_event: failed to enqueue event for %.% (op=%): %',
    TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP, SQLERRM;
  RETURN COALESCE(NEW, OLD);
END;
$$;


-- ---------------------------------------------------------------------------
-- 2. Triggers — one per table, passing a logical event type as argument
-- ---------------------------------------------------------------------------

-- customer_product_interactions: all interaction types (viewed, rated, saved, etc.)
-- INSERT only — interactions are immutable; no updates or deletes in the domain model.
DROP TRIGGER IF EXISTS trg_outbox_customer_interaction ON gold.customer_product_interactions;
CREATE TRIGGER trg_outbox_customer_interaction
  AFTER INSERT ON gold.customer_product_interactions
  FOR EACH ROW EXECUTE FUNCTION gold.enqueue_outbox_event('customer_interaction');

-- b2c_customer_allergens: safety-critical — fire on INSERT and DELETE
DROP TRIGGER IF EXISTS trg_outbox_customer_allergen ON gold.b2c_customer_allergens;
CREATE TRIGGER trg_outbox_customer_allergen
  AFTER INSERT OR DELETE ON gold.b2c_customer_allergens
  FOR EACH ROW EXECUTE FUNCTION gold.enqueue_outbox_event('customer_allergen');

-- b2c_customer_dietary_preferences: safety-critical — INSERT and DELETE
DROP TRIGGER IF EXISTS trg_outbox_customer_diet ON gold.b2c_customer_dietary_preferences;
CREATE TRIGGER trg_outbox_customer_diet
  AFTER INSERT OR DELETE ON gold.b2c_customer_dietary_preferences
  FOR EACH ROW EXECUTE FUNCTION gold.enqueue_outbox_event('customer_dietary_preference');

-- b2c_customer_health_conditions: safety-critical — full lifecycle
DROP TRIGGER IF EXISTS trg_outbox_customer_condition ON gold.b2c_customer_health_conditions;
CREATE TRIGGER trg_outbox_customer_condition
  AFTER INSERT OR UPDATE OR DELETE ON gold.b2c_customer_health_conditions
  FOR EACH ROW EXECUTE FUNCTION gold.enqueue_outbox_event('customer_health_condition');

-- b2c_customer_health_profiles: safety-critical — full lifecycle
DROP TRIGGER IF EXISTS trg_outbox_customer_profile ON gold.b2c_customer_health_profiles;
CREATE TRIGGER trg_outbox_customer_profile
  AFTER INSERT OR UPDATE OR DELETE ON gold.b2c_customer_health_profiles
  FOR EACH ROW EXECUTE FUNCTION gold.enqueue_outbox_event('customer_health_profile');
