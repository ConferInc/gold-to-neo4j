# B2B Layer Architecture — Neo4j Graph Schema

**Project:** Gold-to-Neo4j  
**Graph Name:** `customer_recipe_graph`  
**Layer:** B2B (Business-to-Business)  
**Last updated:** Mar 2026

---

## 1. Overview

The B2B layer extends the Nutrition Knowledge Graph with business-centric entities and relationships. It models:

- **B2B Customers** — Vendor-linked customers with health profiles, conditions, allergens, and dietary preferences
- **Product domain** — Products, vendors, brands, categories, certifications, ingredients
- **Customer–Product interactions** — Purchases, views, ratings, rejections
- **Product–Product links** — Substitutes, similarity, frequent co-purchases, global mapping
- **Nutrition values** — Product-level nutrition linked to nutrient definitions

---

## 2. Node Labels (B2B Layer)

### 2.1 B2B Customer Domain

| Label             | Description                                      |
|-------------------|--------------------------------------------------|
| B2BCustomer       | Business customer; linked to a vendor; has profile, conditions, preferences |
| B2BHealthProfile  | Health profile for a B2B customer (activity, goals, targets) |
| HealthCondition   | Health condition (e.g., diabetes, hypertension)  |
| Allergen          | Allergen lookup (code, name, category)           |
| DietaryPreference | Diet type lookup (code, name, category)          |
| Vendor            | Vendor/organization that owns B2B customers      |

### 2.2 Product Domain

| Label                 | Description                                      |
|-----------------------|--------------------------------------------------|
| Product               | Packaged food product (name, brand, barcode)     |
| Vendor                | Vendor that sells products                       |
| Ingredient            | Food ingredient used in products                 |
| Category              | Product category                                 |
| Certification         | Product certification (e.g., organic, gluten-free) |
| Brand                 | Brand/manufacturer of the product                |

### 2.3 Product Nutrition Domain

| Label                 | Description                                      |
|-----------------------|--------------------------------------------------|
| ProductNutritionValue | Nutrition value for a product (amount, unit)     |
| NutrientDefinition    | Nutrient taxonomy (nutrient_code, nutrient_name) |

---

## 3. Relationships

### 3.1 B2B Customer → Profile & Lookups

| #  | Type           | From → To                 | Description                          |
|----|----------------|---------------------------|--------------------------------------|
| 2  | HAS_PROFILE    | B2BCustomer → B2BHealthProfile | Customer has one health profile  |
| 4  | HAS_CONDITION  | B2BCustomer → HealthCondition  | Customer has health condition(s) |
| 6  | ALLERGIC_TO    | B2BCustomer → Allergen         | Customer is allergic to allergen |
| 8  | FOLLOWS_DIET   | B2BCustomer → DietaryPreference | Customer follows diet type     |

### 3.2 B2B Customer → Product Interactions

| #  | Type      | From → To      | Description                    |
|----|-----------|----------------|--------------------------------|
| 11 | PURCHASED | B2BCustomer → Product | Customer purchased product |
| 13 | VIEWED    | B2BCustomer → Product | Customer viewed product   |
| 15 | RATED     | B2BCustomer → Product | Customer rated product   |
| 18 | REJECTED  | B2BCustomer → Product | Customer rejected product |

### 3.3 B2B Customer → Vendor & Self

| #  | Type                   | From → To           | Description                           |
|----|------------------------|---------------------|---------------------------------------|
| 22 | BELONGS_TO_VENDOR      | B2BCustomer → Vendor | Customer belongs to vendor/organization |
| 24 | SHARES_PREFERENCES_WITH| B2BCustomer → B2BCustomer | Customer shares preferences with another B2B customer |

### 3.4 Product Domain Relationships

| #  | Type                  | From → To       | Description                        |
|----|-----------------------|-----------------|------------------------------------|
| 1  | SOLD_BY               | Product → Vendor | Product is sold by vendor       |
| 2  | CONTAINS_INGREDIENT   | Product → Ingredient | Product contains ingredient   |
| 3  | BELONGS_TO_CATEGORY   | Product → Category | Product belongs to category    |
| 4  | HAS_CERTIFICATION     | Product → Certification | Product has certification   |
| 5  | MANUFACTURED_BY       | Product → Brand | Product is manufactured by brand  |

### 3.5 Product–Product Relationships

| #  | Type                  | From → To   | Description                         |
|----|-----------------------|-------------|-------------------------------------|
| 6  | SUBSTITUTE_FOR        | Product → Product | Product substitutes for another  |
| 7  | SIMILAR_TO            | Product → Product | Product is similar to another   |
| 11 | MAPPED_TO_GLOBAL      | Product → Product | Product maps to global product  |
| 12 | FREQUENTLY_BOUGHT_WITH| Product → Product | Product frequently bought together |

### 3.6 Product Nutrition Relationships

| #  | Type                  | From → To                      | Description                         |
|----|-----------------------|--------------------------------|-------------------------------------|
| 1  | HAS_NUTRITION_VALUE   | Product → ProductNutritionValue | Product has nutrition value(s)  |
| 2  | OF_NUTRIENT           | ProductNutritionValue → NutrientDefinition | Value is for nutrient |

*Note: `HAS_NUTRITION_VALUE` typically has properties: `created_at`, `updated_at`*

---

## 4. Relationship Summary

| Category               | Count | Relationship Types                                                                 |
|------------------------|-------|-------------------------------------------------------------------------------------|
| B2B Customer → Profile | 4     | HAS_PROFILE, HAS_CONDITION, ALLERGIC_TO, FOLLOWS_DIET                               |
| B2B Customer → Product | 4     | PURCHASED, VIEWED, RATED, REJECTED                                                  |
| B2B Customer → Vendor  | 1     | BELONGS_TO_VENDOR                                                                   |
| B2B Customer → Self    | 1     | SHARES_PREFERENCES_WITH                                                             |
| Product Domain         | 5     | SOLD_BY, CONTAINS_INGREDIENT, BELONGS_TO_CATEGORY, HAS_CERTIFICATION, MANUFACTURED_BY |
| Product–Product        | 4     | SUBSTITUTE_FOR, SIMILAR_TO, MAPPED_TO_GLOBAL, FREQUENTLY_BOUGHT_WITH                |
| Product Nutrition      | 2     | HAS_NUTRITION_VALUE, OF_NUTRIENT                                                    |
| **Total**              | **21**| —                                                                                   |

---

## 5. Node Summary

| Category         | Nodes                                                                 |
|------------------|-----------------------------------------------------------------------|
| B2B Customer     | B2BCustomer, B2BHealthProfile, HealthCondition, Allergen, DietaryPreference, Vendor |
| Product Domain   | Product, Vendor, Ingredient, Category, Certification, Brand           |
| Nutrition        | ProductNutritionValue, NutrientDefinition                             |

*Note: Vendor appears in both B2B Customer and Product domain.*

---

## 6. High-Level Diagram (Conceptual)

```
                         ┌─────────────┐
                         │ B2BCustomer │
                         └──────┬──────┘
    ┌───────────────────────────┼─────────────────────────────────────┐
    │                           │                                     │
    ▼                           ▼                                     ▼
┌──────────────┐   ┌─────────────────────────┐              ┌─────────────────┐
│ B2BHealth    │   │ HealthCondition         │              │ Vendor          │
│ Profile      │   │ Allergen                │              │ (BELONGS_TO)    │
└──────────────┘   │ DietaryPreference       │              └────────┬────────┘
                   └─────────────────────────┘                       │
    │                                                                │
    │ PURCHASED / VIEWED / RATED / REJECTED                          │ SOLD_BY
    └──────────────────────────┬────────────────────────────────────┼──────┐
                               ▼                                    ▼      │
                        ┌──────────┐                              ┌───────┴──┐
                        │ Product  │◄─────────────────────────────┤ Vendor  │
                        └────┬─────┘                              └─────────┘
                             │
        ┌────────────────────┼────────────────────┬─────────────────────────┐
        ▼                    ▼                    ▼                         ▼
┌───────────────┐   ┌──────────────┐   ┌──────────────┐   ┌─────────────────────┐
│ Ingredient    │   │ Category     │   │ Certification│   │ Brand               │
│ CONTAINS_*    │   │ BELONGS_TO   │   │ HAS_CERT*    │   │ MANUFACTURED_BY     │
└───────────────┘   └──────────────┘   └──────────────┘   └─────────────────────┘
        │
        │ Product–Product: SUBSTITUTE_FOR, SIMILAR_TO, MAPPED_TO_GLOBAL, FREQUENTLY_BOUGHT_WITH
        ▼
┌──────────────────────┐     OF_NUTRIENT      ┌─────────────────────┐
│ ProductNutritionValue│─────────────────────►│ NutrientDefinition  │
└──────────────────────┘                      └─────────────────────┘

SHARES_PREFERENCES_WITH: B2BCustomer ──────► B2BCustomer
```

---

## 7. References

- Config: `config/customers.yaml`, `config/products.yaml`
- Main graph architecture: `docs/GRAPH_ARCHITECTURE.md`
