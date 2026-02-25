# Knowledge Graph Relationships Status


## Needs Data: Product Relationships

Product domain relationships requiring data/tables to implement:

| # | Relationship | From → To |
|---|--------------|-----------|
| 8 | SOLD_BY | Product → Vendor |
| 10 | BELONGS_TO_CATEGORY | Product → Category |
| 11 | HAS_CERTIFICATION | Product → Certification |
| 12 | MANUFACTURED_BY | Product → Brand |
| 13 | SUBSTITUTE_FOR | Product → Product |
| 14 | SIMILAR_TO | Product → Product |
| 16 | AVAILABLE_IN_SEASON | Product → Season |
| 17 | AVAILABLE_IN_REGION | Product → Region |
| 18 | MAPPED_TO_GLOBAL | Product → Product |
| 19 | FREQUENTLY_BOUGHT_WITH | Product → Product |
| 20 | POPULAR_IN_SEGMENT | Product → B2CCustomer |

---

## Needs Data: Recipe → Product

| # | Relationship | From → To |
|---|--------------|-----------|
| 29 | USES_PRODUCT | Recipe → Product |

---

## Dietary Preferences & Health Conditions Missing Relationships

| # | Relationship | From → To |
|---|--------------|-----------|
| 30 | SUITABLE_FOR_DIET | Recipe → DietaryPreference |
| 31 | SUITABLE_FOR_CONDITION | Recipe → HealthCondition |
| 34 | RESTRICTS | HealthCondition → Ingredient |
| 35 | RECOMMENDS | HealthCondition → Ingredient |
| 36 | FORBIDS_PRODUCT | HealthCondition → Product |
| 39 | CROSS_REACTIVE_WITH | Allergen → Allergen |
| 21 | CONTAINS_ALLERGEN | Ingredient → Allergen |
| 40 | FOUND_IN_FAMILY | Allergen → Ingredient |
| 41 | FORBIDS | DietaryPreference → Ingredient |
| 42 | ALLOWS | DietaryPreference → Ingredient |
| 43 | REQUIRES | DietaryPreference → Ingredient |

---

## Ingredient Relationships Missing

| # | Relationship | From → To |
|---|--------------|-----------|
| 23 | SUBSTITUTE_FOR | Ingredient → Ingredient |
| 24 | DERIVED_FROM | Ingredient → Ingredient |
| 25 | SYNONYM_OF | Ingredient → Ingredient |
| 27 | PART_OF_FAMILY | Ingredient → Ingredient |
| 28 | INTERACTS_WITH_DRUG | Ingredient → Compound |
