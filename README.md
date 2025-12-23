# Product Recommendation for Online Shop  
**Databases — Relational vs Graph (PostgreSQL & Azure Cosmos DB Gremlin)**

> Comparative study of SQL and Graph databases for product recommendations in an e-commerce scenario.

---

## 1. Project Overview

Modern e-commerce platforms (Amazon, Zalando, …) rely heavily on recommendation systems to:

- increase conversion rate,
- grow the average basket value,
- keep users engaged with relevant products.

Typical features:

- **“Similar products”**
- **“Customers who bought this also bought”**
- **User-based recommendations** (users with similar behaviour)

All these features are naturally expressed as **relationships** between users, products, categories and interactions – i.e. a **graph-shaped problem**.  
In this project we implement and **compare**:

1. A **relational baseline** in **PostgreSQL** (classic e-commerce schema).
2. A **graph model** in **Azure Cosmos DB (Gremlin API)**.

We generate synthetic data, run the *same logical queries* on both models, and benchmark:

- **build time** (data loading),
- **query latency** for the main recommendation scenarios,
- **scalability** as data volume increases. :contentReference[oaicite:0]{index=0}

---

## 2. Features

- Synthetic data generator for:
  - Products, Users, Categories, Brands, Tags
  - User–Product interactions: `VIEWED`, `BOUGHT`, `LIKED`
  - Product similarity edges: `SIMILAR_TO`, `BOUGHT_TOGETHER`
  - User similarity edges: `SIMILAR_USER`
- **Two storage models**:
  - PostgreSQL (relational)
  - Azure Cosmos DB Gremlin (graph)
- **Four core recommendation queries** implemented in:
  - **SQL** (PostgreSQL)
  - **Gremlin** (Cosmos DB)
- **Benchmark harness**:
  - automatic reset + rebuild per test run,
  - configurable volumes (N products),
  - timing of build and queries,
  - result aggregation into tables / plots.

---

## 3. Tech Stack

- **Python** 3.x – orchestration, data generation, benchmarks.
- **PostgreSQL** – relational database (accessed via `psycopg2` or similar).
- **Azure Cosmos DB (Gremlin API)** – graph database.
- **gremlin-python** – Gremlin client for Cosmos DB.
- **pandas** – results aggregation.
- **matplotlib / networkx** (optional) – visualisation of graph traversals.
- **Jupyter Notebook** – experiment scripts and documentation.

---

## 4. Data Model

### 4.1 Shared Domain

Entities (same logical domain for SQL and Graph):

- `Product`
- `User`
- `Category`
- `Brand`
- `Tag`

Relations:

- `Product` → `Category` (**IN_CATEGORY**)
- `Product` → `Brand` (**HAS_BRAND**)
- `Product` → `Tag` (**HAS_TAG**)
- `Category` → `Category` (**PARENT_OF**, category hierarchy)
- `User` → `Product` interactions: **VIEWED / BOUGHT / LIKED**
- `Product` ↔ `Product` similarity: **SIMILAR_TO(score)**, **BOUGHT_TOGETHER(support)**
- `User` ↔ `User` similarity: **SIMILAR_USER(score)** :contentReference[oaicite:1]{index=1}

### 4.2 Relational (PostgreSQL) Schema

Core tables (simplified):

- `brands`
- `categories`
- `tags`
- `users`
- `products`
- `product_tags` (many-to-many)
- `user_interactions` (VIEWED / BOUGHT / LIKED)
- `product_similarity`
- `product_bought_together`
- `user_similarity`

**Indexing strategy**

- Composite primary keys often include a `run_pk` (benchmark run identifier) to isolate runs:
  - e.g. `(run_pk, product_id)` as PK.
- Foreign keys enforce integrity:
  - `products.brand_id → brands.id`,
  - `products.category_id → categories.id`,
  - `user_interactions.user_id → users.id`, etc.
- Additional indices for performance, e.g.:
  - `product_similarity(run_pk, src_product_id, score DESC)`  
    for “top similar products”.

### 4.3 Graph (Cosmos DB Gremlin) Schema

**Vertex labels**

- `product`, `user`, `category`, `brand`, `tag`

**Edge labels**

- Structure:
  - `IN_CATEGORY`, `HAS_BRAND`, `HAS_TAG`, `PARENT_OF`
- Behaviour:
  - `VIEWED`, `BOUGHT`, `LIKED`
- Similarity:
  - `SIMILAR_TO` (property: `score`)
  - `BOUGHT_TOGETHER` (property: `support`)
  - `SIMILAR_USER` (property: `score`)

Each vertex stores:

- `id` – unique id used in Gremlin traversals,
- `pk` – partition key (we reuse the run identifier as partition key). :contentReference[oaicite:2]{index=2}

---

## 5. Main Recommendation Queries

The **same four scenarios** are implemented in SQL and Gremlin.

1. **Similar products by category**
   - *Idea*: products that share the same category.
2. **Similar products by similarity edges**
   - *Idea*: use the `SIMILAR_TO` relation ordered by `score`.
3. **“Customers also bought”**
   - *Idea*: from a given product, go to users who bought it, then to other products they bought.
4. **User-based recommendations**
   - *Idea*: from a user, go to their bought products, to other users who bought them, then to other products.

On PostgreSQL this translates into **self-joins and join tables**; on Cosmos DB it becomes **Gremlin traversals of depth 2–3** starting from a vertex. :contentReference[oaicite:3]{index=3}

---
