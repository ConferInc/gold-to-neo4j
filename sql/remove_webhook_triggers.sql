-- ═══════════════════════════════════════════════════════════════
-- Phase 0: Remove webhook triggers + B2B outbox triggers
--
-- Run this in Supabase SQL Editor BEFORE deploying code changes.
--
-- What this does:
--   1. Drops 15 fn_webhook_catalog triggers (per-row HTTP POSTs)
--   2. Drops the fn_webhook_catalog function itself
--   3. Drops 5 B2B outbox triggers (moved to batch sync lane)
--
-- Safe to re-run — all statements use IF EXISTS.
-- ═══════════════════════════════════════════════════════════════


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  STEP 1: Drop 15 Webhook Triggers (Catalog Tables)          ║
-- ║  These cause per-row HTTP POST storms during bulk writes.    ║
-- ║  The hourly batch sync replaces this functionality.          ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- Recipes (3 triggers)
DROP TRIGGER IF EXISTS trg_webhook_recipes                    ON gold.recipes;
DROP TRIGGER IF EXISTS trg_webhook_recipe_ingredients         ON gold.recipe_ingredients;
DROP TRIGGER IF EXISTS trg_webhook_recipe_nutrition_profiles   ON gold.recipe_nutrition_profiles;

-- Products (6 triggers)
DROP TRIGGER IF EXISTS trg_webhook_products                   ON gold.products;
DROP TRIGGER IF EXISTS trg_webhook_product_ingredients        ON gold.product_ingredients;
DROP TRIGGER IF EXISTS trg_webhook_product_allergens          ON gold.product_allergens;
DROP TRIGGER IF EXISTS trg_webhook_product_categories         ON gold.product_categories;
DROP TRIGGER IF EXISTS trg_webhook_product_certifications     ON gold.product_certifications;
DROP TRIGGER IF EXISTS trg_webhook_product_dietary_preferences ON gold.product_dietary_preferences;

-- Ingredients (2 triggers)
DROP TRIGGER IF EXISTS trg_webhook_ingredients                ON gold.ingredients;
DROP TRIGGER IF EXISTS trg_webhook_ingredient_allergens       ON gold.ingredient_allergens;

-- Nutrition (3 triggers)
DROP TRIGGER IF EXISTS trg_webhook_nutrition_facts            ON gold.nutrition_facts;
DROP TRIGGER IF EXISTS trg_webhook_nutrition_categories       ON gold.nutrition_categories;
DROP TRIGGER IF EXISTS trg_webhook_nutrition_definitions      ON gold.nutrition_definitions;

-- Reference (1 trigger)
DROP TRIGGER IF EXISTS trg_webhook_cuisines                   ON gold.cuisines;

-- Drop the webhook function itself (no longer needed)
DROP FUNCTION IF EXISTS gold.fn_webhook_catalog();


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  STEP 2: Drop 5 B2B Outbox Triggers                         ║
-- ║  B2B data is pipeline-managed, NOT user-interactive.         ║
-- ║  The hourly batch sync (customers layer) handles B2B.        ║
-- ╚═══════════════════════════════════════════════════════════════╝

DROP TRIGGER IF EXISTS trg_outbox_b2b_customers          ON gold.b2b_customers;
DROP TRIGGER IF EXISTS trg_outbox_b2b_health_profiles    ON gold.b2b_customer_health_profiles;
DROP TRIGGER IF EXISTS trg_outbox_b2b_allergens          ON gold.b2b_customer_allergens;
DROP TRIGGER IF EXISTS trg_outbox_b2b_dietary_prefs      ON gold.b2b_customer_dietary_preferences;
DROP TRIGGER IF EXISTS trg_outbox_b2b_health_conditions  ON gold.b2b_customer_health_conditions;


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  VERIFICATION (run after execution)                          ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- Should return 0 rows (no webhook triggers remaining)
SELECT trigger_name, event_object_table
FROM information_schema.triggers
WHERE trigger_schema = 'gold' AND trigger_name LIKE 'trg_webhook_%'
ORDER BY trigger_name;

-- Should return 0 rows (no B2B outbox triggers remaining)
SELECT trigger_name, event_object_table
FROM information_schema.triggers
WHERE trigger_schema = 'gold' AND trigger_name LIKE 'trg_outbox_b2b%'
ORDER BY trigger_name;

-- Should return ~25 rows (remaining B2C outbox triggers)
SELECT trigger_name, event_object_table,
       string_agg(event_manipulation, ', ' ORDER BY event_manipulation) AS events
FROM information_schema.triggers
WHERE trigger_schema = 'gold' AND trigger_name LIKE 'trg_outbox_%'
GROUP BY trigger_name, event_object_table
ORDER BY event_object_table;
