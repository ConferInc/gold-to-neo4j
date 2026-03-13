-- Gold → Neo4j real-time webhook triggers
--
-- Uses pg_net (net.http_post) to POST a Supabase-format webhook payload to the
-- orchestrator's /webhooks/supabase endpoint whenever any gold catalog table row
-- is inserted, updated, or deleted.
--
-- Prerequisites:
--   1. pg_net extension enabled:  CREATE EXTENSION IF NOT EXISTS pg_net;
--   2. Set per-session GUCs (or in postgresql.conf / ALTER DATABASE):
--        ALTER DATABASE <db> SET app.orchestrator_url = 'https://orchestrator.yourdomain.com';
--        ALTER DATABASE <db> SET app.webhook_secret   = 'your-secret-value-here';
--
-- Layer routing (mirrors _TABLE_TO_LAYER in agent_gateway/service.py):
--   recipes, cuisines                                                  → recipes
--   ingredients                                                        → ingredients
--   products, product_categories, vendors, certifications             → products
--   b2c_customers, b2b_customers, households, allergens,
--   dietary_preferences, health_conditions                            → customers

-- ---------------------------------------------------------------------------
-- 1. Shared trigger function
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION gold.notify_orchestrator()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  _url        text;
  _secret     text;
  _payload    jsonb;
  _headers    jsonb;
  _record     jsonb;
  _old_record jsonb;
BEGIN
  -- Read configuration from GUCs so credentials stay out of source code.
  _url    := current_setting('app.orchestrator_url', true);
  _secret := current_setting('app.webhook_secret',   true);

  IF _url IS NULL OR _url = '' THEN
    RAISE WARNING 'notify_orchestrator: app.orchestrator_url is not set, skipping';
    RETURN COALESCE(NEW, OLD);
  END IF;

  -- Build record payloads; DELETE carries no NEW row.
  _record     := CASE WHEN NEW IS NOT NULL THEN to_jsonb(NEW) ELSE NULL END;
  _old_record := CASE WHEN OLD IS NOT NULL THEN to_jsonb(OLD) ELSE NULL END;

  _payload := jsonb_build_object(
    'type',       TG_OP,
    'schema',     TG_TABLE_SCHEMA,
    'table',      TG_TABLE_NAME,
    'record',     _record,
    'old_record', _old_record
  );

  _headers := jsonb_build_object(
    'Content-Type',    'application/json',
    'x-webhook-secret', COALESCE(_secret, '')
  );

  -- Fire-and-forget HTTP POST; errors are logged but do not abort the transaction.
  PERFORM net.http_post(
    url     := _url || '/webhooks/supabase',
    body    := _payload,
    headers := _headers
  );

  RETURN COALESCE(NEW, OLD);
EXCEPTION WHEN OTHERS THEN
  RAISE WARNING 'notify_orchestrator: HTTP dispatch failed for %.%: %',
    TG_TABLE_SCHEMA, TG_TABLE_NAME, SQLERRM;
  RETURN COALESCE(NEW, OLD);
END;
$$;


-- ---------------------------------------------------------------------------
-- 2. Helper macro — drop-and-recreate a trigger on a gold table
--    (idempotent; safe to re-run migrations)
-- ---------------------------------------------------------------------------

-- Webhook 1 of 13: gold.recipes  →  recipes layer
DROP TRIGGER IF EXISTS trg_notify_orchestrator ON gold.recipes;
CREATE TRIGGER trg_notify_orchestrator
  AFTER INSERT OR UPDATE OR DELETE ON gold.recipes
  FOR EACH ROW EXECUTE FUNCTION gold.notify_orchestrator();

-- Webhook 2 of 13: gold.cuisines  →  recipes layer
DROP TRIGGER IF EXISTS trg_notify_orchestrator ON gold.cuisines;
CREATE TRIGGER trg_notify_orchestrator
  AFTER INSERT OR UPDATE OR DELETE ON gold.cuisines
  FOR EACH ROW EXECUTE FUNCTION gold.notify_orchestrator();

-- Webhook 3 of 13: gold.ingredients  →  ingredients layer
DROP TRIGGER IF EXISTS trg_notify_orchestrator ON gold.ingredients;
CREATE TRIGGER trg_notify_orchestrator
  AFTER INSERT OR UPDATE OR DELETE ON gold.ingredients
  FOR EACH ROW EXECUTE FUNCTION gold.notify_orchestrator();

-- Webhook 4 of 13: gold.products  →  products layer
DROP TRIGGER IF EXISTS trg_notify_orchestrator ON gold.products;
CREATE TRIGGER trg_notify_orchestrator
  AFTER INSERT OR UPDATE OR DELETE ON gold.products
  FOR EACH ROW EXECUTE FUNCTION gold.notify_orchestrator();

-- Webhook 5 of 13: gold.product_categories  →  products layer
DROP TRIGGER IF EXISTS trg_notify_orchestrator ON gold.product_categories;
CREATE TRIGGER trg_notify_orchestrator
  AFTER INSERT OR UPDATE OR DELETE ON gold.product_categories
  FOR EACH ROW EXECUTE FUNCTION gold.notify_orchestrator();

-- Webhook 6 of 13: gold.vendors  →  products layer
DROP TRIGGER IF EXISTS trg_notify_orchestrator ON gold.vendors;
CREATE TRIGGER trg_notify_orchestrator
  AFTER INSERT OR UPDATE OR DELETE ON gold.vendors
  FOR EACH ROW EXECUTE FUNCTION gold.notify_orchestrator();

-- Webhook 7 of 13: gold.certifications  →  products layer
DROP TRIGGER IF EXISTS trg_notify_orchestrator ON gold.certifications;
CREATE TRIGGER trg_notify_orchestrator
  AFTER INSERT OR UPDATE OR DELETE ON gold.certifications
  FOR EACH ROW EXECUTE FUNCTION gold.notify_orchestrator();

-- Webhook 8 of 13: gold.b2c_customers  →  customers layer
DROP TRIGGER IF EXISTS trg_notify_orchestrator ON gold.b2c_customers;
CREATE TRIGGER trg_notify_orchestrator
  AFTER INSERT OR UPDATE OR DELETE ON gold.b2c_customers
  FOR EACH ROW EXECUTE FUNCTION gold.notify_orchestrator();

-- Webhook 9 of 13: gold.b2b_customers  →  customers layer
DROP TRIGGER IF EXISTS trg_notify_orchestrator ON gold.b2b_customers;
CREATE TRIGGER trg_notify_orchestrator
  AFTER INSERT OR UPDATE OR DELETE ON gold.b2b_customers
  FOR EACH ROW EXECUTE FUNCTION gold.notify_orchestrator();

-- Webhook 10 of 13: gold.households  →  customers layer
DROP TRIGGER IF EXISTS trg_notify_orchestrator ON gold.households;
CREATE TRIGGER trg_notify_orchestrator
  AFTER INSERT OR UPDATE OR DELETE ON gold.households
  FOR EACH ROW EXECUTE FUNCTION gold.notify_orchestrator();

-- Webhook 11 of 13: gold.allergens  →  customers layer
DROP TRIGGER IF EXISTS trg_notify_orchestrator ON gold.allergens;
CREATE TRIGGER trg_notify_orchestrator
  AFTER INSERT OR UPDATE OR DELETE ON gold.allergens
  FOR EACH ROW EXECUTE FUNCTION gold.notify_orchestrator();

-- Webhook 12 of 13: gold.dietary_preferences  →  customers layer
DROP TRIGGER IF EXISTS trg_notify_orchestrator ON gold.dietary_preferences;
CREATE TRIGGER trg_notify_orchestrator
  AFTER INSERT OR UPDATE OR DELETE ON gold.dietary_preferences
  FOR EACH ROW EXECUTE FUNCTION gold.notify_orchestrator();

-- Webhook 13 of 13: gold.health_conditions  →  customers layer
DROP TRIGGER IF EXISTS trg_notify_orchestrator ON gold.health_conditions;
CREATE TRIGGER trg_notify_orchestrator
  AFTER INSERT OR UPDATE OR DELETE ON gold.health_conditions
  FOR EACH ROW EXECUTE FUNCTION gold.notify_orchestrator();
