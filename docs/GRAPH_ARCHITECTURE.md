# Nutrition Knowledge Graph — Architecture Document

**Project:** Gold-to-Neo4j  
**Graph Name:** `customer_recipe_graph`  
**Schema Source:** Gold layer (Supabase)  
**Last updated:** Based on config files (Feb 2026)

---

## 1. Overview

The Nutrition Knowledge Graph is a Neo4j property graph that models:

- **Catalog data** — Recipes, Ingredients, Products, and their nutrition taxonomy
- **Customer data** — User profiles, households, health conditions, dietary preferences, allergens
- **Activity data** — Meal logs, scans, meal plans, and interactions with recipes/products

Data is synced incrementally from a Gold schema (Supabase) via config-driven pipelines: `recipes`, `ingredients`, `products`, and `customers`. Each pipeline writes nodes and relationships to Neo4j, then semantic and structural embeddings are computed for AI features (recommendations, similarity search).

---

## 2. Node Labels

### 2.1 Recipes Layer

| Label   | Source Table | Primary Key | Description |
|---------|--------------|-------------|-------------|
| Recipe  | recipes      | id          | Recipe with title, description, instructions, difficulty, servings |
| Cuisine | cuisines     | id          | Cuisine type (code, name, region); recipes link to one cuisine |

**Key properties (Recipe):** `title`, `description`, `instructions`, `difficulty`, `meal_type`, `prep_time_minutes`, `cook_time_minutes`, `servings`, `image_url`, `cuisine_id`

### 2.2 Ingredients Layer

| Label              | Source Table       | Primary Key | Description |
|--------------------|--------------------|-------------|-------------|
| Ingredient         | ingredients        | id          | Food ingredient (name, category, nutrition values) |
| NutritionValue     | nutrition_facts    | id          | Amount of a nutrient for an ingredient; entity_type=ingredient |
| NutrientDefinition | nutrition_definitions | id       | Nutrient taxonomy (nutrient_code, nutrient_name, category) |
| NutritionCategory  | nutrition_categories | id        | Category hierarchy for nutrients (display_name, description) |

**Key properties (Ingredient):** `name`, `category`  
**Key properties (NutritionValue):** `entity_type`, `entity_id`, `nutrient_id`, `amount`, `unit`

### 2.3 Products Layer

| Label  | Source Table | Primary Key | Description |
|--------|--------------|-------------|-------------|
| Product | products    | id          | Packaged food product (name, brand, barcode, nutrition) |

**Key properties:** `name`, `brand`, `status`, `barcode`, `category_id`, nutrition fields

### 2.4 Customers Layer

| Label                       | Source Table                    | Primary Key | Description |
|-----------------------------|----------------------------------|-------------|-------------|
| B2C_Customer                | b2c_customers                   | id          | End-user profile (email, full_name, gender, household_id) |
| Household                   | households                      | id          | Household (name, type, total_members, location) |
| B2C_Customer_Health_Profiles | b2c_customer_health_profiles   | id          | Health profile (activity_level, health_goal, targets, disliked_ingredients) |
| B2C_Customer_Health_Conditions | b2c_customer_health_conditions | id       | Health condition per customer (severity, diagnosis_date) |
| Allergens                   | allergens                       | id          | Allergen lookup (code, name, category, description) |
| Dietary_Preferences         | dietary_preferences             | id          | Diet type lookup (code, name, category, description) |
| HouseholdBudget             | household_budgets               | id          | Budget per household (period, budget_type, amount) |
| MealLogStreak               | meal_log_streaks                | id          | Streak tracker per customer (current_streak, longest_streak) |

**Safety-critical:** `B2C_Customer_Health_Conditions`, `B2C_Customer_Health_Profiles`, `Allergens`, `Dietary_Preferences`

### 2.5 Interaction Layer (Activity)

| Label      | Source Table     | Primary Key | Description |
|------------|------------------|-------------|-------------|
| MealLog    | meal_logs        | id          | Daily meal log header (log_date, totals, streak_count) |
| MealLogItem| meal_log_items   | id          | Individual food item in a meal (recipe/product link, servings) |
| ScanEvent  | scan_history     | id          | Barcode scan (barcode, barcode_format, scan_source) |
| MealPlan   | meal_plans       | id          | AI-generated meal plan (start_date, meals_per_day, budget) |
| MealPlanItem | meal_plan_items | id        | Slot in a meal plan (recipe, meal_date, meal_type) |

---

## 3. Relationships

### 3.1 Customer → Profile & Lookups

| Type           | Source → Target                        | Description |
|----------------|----------------------------------------|-------------|
| HAS_PROFILE    | B2C_Customer → B2C_Customer_Health_Profiles | One profile per customer |
| HAS_CONDITION  | B2C_Customer → B2C_Customer_Health_Conditions | 0–N conditions per customer |
| IS_ALLERGIC    | B2C_Customer → Allergens               | Customer's allergens (via b2c_customer_allergens) |
| FOLLOWS_DIET   | B2C_Customer → Dietary_Preferences     | Customer's diet preferences (via b2c_customer_dietary_preferences) |
| HAS_HOUSEHOLD  | B2C_Customer → Household               | Customer belongs to household |

### 3.2 Customer → Catalog Interactions

| Type      | Source → Target | Description |
|-----------|-----------------|-------------|
| VIEWED    | B2C_Customer → Recipe / Product | Viewed recipe/product |
| SAVED     | B2C_Customer → Recipe / Product | Saved recipe/product |
| RATED     | B2C_Customer → Recipe / Product | Rated recipe/product |
| REJECTED  | B2C_Customer → Recipe / Product | Rejected recipe/product |
| WHITELISTED | B2C_Customer → Recipe / Product | Whitelisted recipe/product |
| BLACKLISTED | B2C_Customer → Recipe / Product | Blacklisted recipe/product |
| TRIED    | B2C_Customer → Recipe / Product | Tried recipe/product |

*All interaction types are derived from `customer_product_interactions`.*

### 3.3 Recipes & Ingredients

| Type             | Source → Target | Description |
|------------------|-----------------|-------------|
| BELONGS_TO_CUSINE| Recipe → Cuisine | Recipe's cuisine type |
| USES_INGREDIENT  | Recipe → Ingredient | Recipe uses ingredient (via recipe_ingredients) |

### 3.4 Ingredients & Nutrition

| Type          | Source → Target        | Description |
|---------------|------------------------|-------------|
| HAS_NUTRITION | Ingredient → NutritionValue | Ingredient's nutrient amounts |
| OF_NUTRIENT   | NutritionValue → NutrientDefinition | Links value to nutrient definition |
| PARENT_OF     | NutritionCategory → NutritionCategory | Category hierarchy (self-join) |

### 3.5 Activity / Consumption

| Type         | Source → Target | Description |
|--------------|-----------------|-------------|
| CONTAINS_ITEM| MealLog → MealLogItem | Meal log contains food items |
| CONTAINS_ITEM| MealPlan → MealPlanItem | Meal plan contains planned items |
| OF_RECIPE    | MealLogItem → Recipe | Logged item is a recipe |
| PLANS_RECIPE | MealPlanItem → Recipe | Planned item is a recipe |

*MealLogItem and MealPlanItem can also link to Product (product_id) for logged/planned products.*

### 3.6 Household

| Type       | Source → Target    | Description |
|------------|--------------------|-------------|
| HAS_BUDGET | Household → HouseholdBudget | Household has budget(s) |

---

## 4. Embeddings

### 4.1 Semantic Embeddings (text-embedding-3-small, 1536 dim)

Generated in Python via LiteLLM; stored in `semanticEmbedding`.  
Text is built from concatenated properties per label:

| Label                       | Text Properties |
|-----------------------------|------------------|
| Recipe                      | title, description, difficulty, instructions |
| Ingredient                  | name, category |
| Product                     | name, brand, status |
| B2C_Customer                | email, full_name, gender |
| Cuisine                     | code, name |
| Household                   | account_status, household_name, household_type |
| Allergens                   | category, code, description, name |
| Dietary_Preferences         | category, code, description, name |
| B2C_Customer_Health_Profiles | activity_level, disliked_ingredients, health_goal |
| B2C_Customer_Health_Conditions | email, full_name, gender |
| NutritionValue              | entity_type, nutrient_code |
| NutrientDefinition          | category, nutrient_code, nutrient_name, subcategory |
| NutritionCategory           | category_name, description, subcategory_name, display_name, icon_name |
| MealLogItem                 | meal_type |
| ScanEvent                   | barcode_format, scan_source |
| MealPlan                    | meals_per_day |
| MealPlanItem                | meal_type, status |
| HouseholdBudget             | period, budget_type |
| MealLog, MealLogStreak      | (no text — no embedding) |

### 4.2 Structural Embeddings (GraphSAGE, 128 dim)

Generated by Neo4j GDS; stored in `graphSageEmbedding`.  
Uses graph structure and per-label numeric features:

| Label                       | Feature Properties |
|-----------------------------|--------------------|
| Recipe                      | servings |
| Ingredient                  | calcium_mg, iron_mg, magnesium_mg, potassium_mg, sodium_mg, vitamin_d_mcg, protein_g, total_fat_g, total_carbs_g |
| Household                   | total_members |
| B2C_Customer_Health_Profiles | bmi, weight_kg, height_cm |
| NutritionValue              | amount |
| NutrientDefinition          | rank |
| NutritionCategory           | hierarchy_level, sort_order |
| Others                      | dummyFeature (fallback) |

### 4.3 Vector Indexes

- **Semantic:** 21 labels use `semanticEmbedding` (1536 dim) for similarity search.
- **Structural:** Same 21 labels use `graphSageEmbedding` (128 dim) for structure-based recommendations.

---

## 5. Graph Projection (GraphSAGE)

- **Name:** `customer_recipe_graph`
- **Relationship types:** HAS_PROFILE, HAS_CONDITION, IS_ALLERGIC, FOLLOWS_DIET, VIEWED, SAVED, BELONGS_TO_CUSINE, HAS_NUTRITION, HAS_HOUSEHOLD, CONTAINS_ITEM, HAS_BUDGET
- **Model:** `b2c_customer_model`
- **Write property:** `graphSageEmbedding`

---

## 6. Pipeline Layers

| Layer      | Config File      | Tables Synced |
|------------|------------------|---------------|
| recipes    | recipes.yaml     | recipes, cuisines, recipe_ingredients |
| ingredients| ingredients.yaml | ingredients, nutrition_facts, nutrition_definitions, nutrition_categories |
| products   | products.yaml    | products |
| customers  | customers.yaml   | b2c_customers, households, health profiles/conditions, allergens, dietary_preferences, meal_logs, meal_log_items, scan_history, meal_plans, meal_plan_items, household_budgets, meal_log_streaks |

Sync is incremental; each layer maintains its own `*_state.json` for cursor-based pagination.

---

## 7. High-Level Diagram (Conceptual)

```
                    ┌─────────────┐
                    │ B2C_Customer│
                    └──────┬──────┘
         HAS_PROFILE │     │ HAS_CONDITION    │ HAS_HOUSEHOLD
         IS_ALLERGIC │     │ FOLLOWS_DIET     │ VIEWED / SAVED / RATED / ...
         ────────────┼─────┼──────────────────┼────────────────────────────
         ▼           ▼     ▼                  ▼
   ┌──────────┐ ┌──────────────┐       ┌──────────┐  ┌─────────┐
   │ Allergens│ │ Health_*     │       │ Household│  │ Recipe  │
   │ Diet_*   │ │ Profiles /   │       │ Budget   │  │ Product │
   └──────────┘ │ Conditions   │       └──────────┘  └────┬────┘
                └──────────────┘                          │
                                                          │ USES_INGREDIENT
                                                          │ BELONGS_TO_CUSINE
                                              ┌───────────┼───────────┐
                                              ▼           ▼           ▼
                                        ┌──────────┐ ┌─────────┐ ┌──────────────┐
                                        │Ingredient│ │ Cuisine │ │ Nutrition*   │
                                        └────┬─────┘ └─────────┘ └──────────────┘
                                             │ HAS_NUTRITION
                                             ▼
                                        ┌─────────────┐
                                        │NutritionValue→NutrientDefinition
                                        └─────────────┘

   Activity: B2C_Customer → MealLog → MealLogItem → Recipe/Product
             B2C_Customer → MealPlan → MealPlanItem → Recipe
             B2C_Customer → ScanEvent → Product
```

---

## 8. References

- Config: `config/recipes.yaml`, `config/ingredients.yaml`, `config/products.yaml`, `config/customers.yaml`
- Embeddings: `config/embedding_config.yaml`
- Cost estimate: `scripts/one_time_insertion_cost_summary.py`, `docs/ONE_TIME_INSERTION_COST_SUMMARY.md`
