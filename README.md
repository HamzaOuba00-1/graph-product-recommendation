# Product Recommendation System  
**Relational vs Graph Databases (PostgreSQL & Azure Cosmos DB Gremlin)**

> Comparative study of relational and graph databases for building recommendation systems in an e-commerce context.

---

## Overview

Modern e-commerce platforms rely on recommendation systems to improve user engagement, increase conversion rates, and personalize product discovery.  
Many recommendation features—such as *similar products*, *customers also bought*, and *user-based recommendations*—are inherently **relationship-driven**.

This project compares two database approaches for these workloads:

- **PostgreSQL** using a traditional relational schema  
- **Azure Cosmos DB (Gremlin API)** using a graph-based model  

Both systems store the same logical data and execute identical recommendation queries to evaluate **performance**, **scalability**, and **model expressiveness**.

---

## Features

- Synthetic data generation:
  - Users, Products, Categories, Brands, Tags
  - User–product interactions (`VIEWED`, `BOUGHT`, `LIKED`)
  - Product and user similarity relationships
- Dual persistence layer:
  - Relational database (PostgreSQL)
  - Graph database (Cosmos DB Gremlin)
- Four recommendation scenarios implemented in:
  - SQL
  - Gremlin
- Automated benchmarking:
  - Database reset per run
  - Configurable data sizes
  - Query execution time measurement
  - Aggregated results and analysis

---

## Tech Stack

- **Python 3** – orchestration, data generation, benchmarks  
- **PostgreSQL** – relational database  
- **Azure Cosmos DB (Gremlin API)** – graph database  
- **gremlin-python** – Gremlin client  
- **pandas** – result aggregation  
- **Jupyter Notebook** – experiments and documentation  

---

## Data Model

### Domain Entities

- Product  
- User  
- Category  
- Brand  
- Tag  

### Relationships

- Product → Category / Brand / Tag  
- Category → Category (hierarchy)  
- User → Product (interactions)  
- Product ↔ Product (similarity, co-purchase)  
- User ↔ User (behavioral similarity)

---

## Recommendation Queries

The same four scenarios are implemented in both databases:

1. Similar products by category  
2. Similar products using similarity scores  
3. “Customers also bought”  
4. User-based recommendations  

In PostgreSQL, these queries rely on multiple joins and self-joins.  
In Cosmos DB, they are expressed as multi-hop Gremlin traversals.

---

## Benchmark Methodology

For increasing data volumes (500 / 1000 / 2000 products):

1. Reset the database  
2. Regenerate synthetic data  
3. Execute each recommendation query  
4. Measure:
   - Data loading time  
   - Query execution time  
   - Number of results  

All metrics are collected using Python timers and stored in structured tables.

---

## Results Summary

### PostgreSQL (Relational)

- Excellent performance for simple, attribute-based queries  
- Significant performance degradation for deep, user-based recommendations  
- Query time increases rapidly due to join complexity  

### Cosmos DB Gremlin (Graph)

- Slight overhead for simple lookups  
- Stable and predictable performance for multi-hop traversals  
- Query execution time remains nearly constant as data volume grows  

---

## Conclusion

Relational databases are well-suited for transactional workloads and simple queries but struggle with deep relationship traversal.  
Graph databases naturally model recommendation logic and scale efficiently for relationship-heavy queries.

**Best practice:** use a **hybrid architecture**—SQL for transactions and reporting, graph databases for recommendations.



## References

- Azure Cosmos DB Gremlin API (Microsoft Docs):  
  https://learn.microsoft.com/azure/cosmos-db/graph/

- PostgreSQL Documentation:  
  https://www.postgresql.org/docs/

- Graph vs Relational Databases (Neo4j):  
  https://neo4j.com/developer/graph-database/

---

**Author:** Hamza Ouba  
**Project:** Graph-based Product Recommendation System
