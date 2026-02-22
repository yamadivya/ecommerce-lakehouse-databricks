# 🛒 E-Commerce Lakehouse Pipeline on Azure Databricks

![Azure Databricks](https://img.shields.io/badge/Azure%20Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=for-the-badge&logo=delta&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-1B3A4B?style=for-the-badge&logo=databricks&logoColor=white)

## Project Overview

An end-to-end production-grade **E-Commerce Lakehouse Pipeline** built on 
**Azure Databricks** using the **Medallion Architecture** (Bronze → Silver → Gold).

This pipeline ingests, transforms, and aggregates **550,688 real records** 
from the [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) 
into business-ready Delta Lake tables governed by Unity Catalog.

---

## Architecture
```
CSV Files (Olist Dataset - 550,688 records)
                  ↓
    ┌─────────────────────────┐
    │      BRONZE LAYER       │
    │   7 Raw Delta Tables    │
    │   STRING everything     │
    │   + metadata columns    │
    └─────────────────────────┘
                  ↓
    ┌─────────────────────────┐
    │      SILVER LAYER       │
    │   7 Clean Delta Tables  │
    │   Proper types + NOTNULL│
    │   DLT Expectations      │
    └─────────────────────────┘
                  ↓
    ┌─────────────────────────┐
    │       GOLD LAYER        │
    │  6 Aggregated Tables    │
    │  Business-ready metrics │
    │  Dashboard-ready        │
    └─────────────────────────┘
                  
```

---

## Dataset

| File | Records | Description |
|------|---------|-------------|
| olist_orders_dataset.csv | 99,441 | Customer orders |
| olist_order_items_dataset.csv | 112,650 | Order line items |
| olist_customers_dataset.csv | 99,441 | Customer profiles |
| olist_products_dataset.csv | 32,951 | Product catalog |
| olist_order_payments_dataset.csv | 103,886 | Payment transactions |
| olist_order_reviews_dataset.csv | 99,224 | Customer reviews |
| olist_sellers_dataset.csv | 3,095 | Seller profiles |
| **Total** | **550,688** | |

---

## Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| Azure Databricks | Premium | Cloud data platform |
| Apache Spark | 3.5.2 | Distributed processing |
| Databricks Runtime | 16.4 LTS + Photon | Optimized runtime |
| Delta Lake | 3.2+ | ACID table format |
| Delta Live Tables | Latest | Declarative ETL + quality |
| Databricks Workflows | Latest | Pipeline orchestration |

---

## Project Structure
```
ecommerce-lakehouse-databricks/
├── notebooks/
│   ├── 01_environment_setup_verification.py
│   ├── 02_data_modeling.py
│   ├── 03_bronze_ingestion.py
│   ├── 04_silver_transformation.py
│   ├── 05_gold_aggregations.py
│   ├── 06_pipeline_validation.py
│   ├── 07_delta_live_tables.py
│   ├── 08_delta_optimization.py
├── docs/
│   ├── setup_guide.md
│   └── data_dictionary.md
├── screenshots/
├── requirements.txt
└── README.md
```

---

## Pipeline Steps

| Step | Notebook | Description |
|------|---------|-------------|
| 1 | 01_environment_setup | Workspace, Unity Catalog, Cluster setup |
| 2 | 02_data_modeling | Bronze, Silver, Gold schema definitions |
| 3 | 03_bronze_ingestion | Raw CSV ingestion to Delta tables |
| 4 | 04_silver_transformation | Clean, cast, deduplicate data |
| 5 | 05_gold_aggregations | Business metrics & aggregations |
| 6 | 06_pipeline_validation | Automated data quality checks |
| 7 | 07_delta_live_tables | DLT pipeline with expectations |
| 8 | 08_delta_optimization | OPTIMIZE, Z-ORDER, VACUUM |


---

## Gold Layer — Business Metrics

| Table | Records | Business Question Answered |
|-------|---------|--------------------------|
| daily_revenue | ~700 | How much revenue per day? |
| customer_ltv | ~96,000 | Who are our most valuable customers? |
| product_performance | ~32,000 | Which products sell the most? |
| category_performance | ~71 | Which categories make the most money? |
| seller_performance | ~3,095 | Which sellers perform best? |
| payment_analysis | 4 | How do customers prefer to pay? |

---





## Setup Guide

### Prerequisites
```
✅ Azure subscription
✅ Azure Databricks Premium workspace (Hybrid)
✅ Unity Catalog enabled
✅ Kaggle account (free dataset download)
```

### Quick Start

**1. Clone this repo**
```bash
git clone https://github.com/yamadivya/ecommerce-lakehouse-databricks.git
```

**2. Download Olist dataset from Kaggle**
```
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
```

**3. Upload CSV files to Databricks Volume**
```
/Volumes/ecommerce/bronze/landing_zone/
```

**4. Create Unity Catalog structure**
```sql
CREATE CATALOG IF NOT EXISTS ecommerce;
CREATE SCHEMA IF NOT EXISTS ecommerce.bronze;
CREATE SCHEMA IF NOT EXISTS ecommerce.silver;
CREATE SCHEMA IF NOT EXISTS ecommerce.gold;
```

**5. Run notebooks in order**
```
01 → 02 → 03 → 04 → 05 → 06 → 07 → 08 → 09
```

See [docs/setup_guide.md](docs/setup_guide.md) for detailed instructions.

---

## 📸 Screenshots

### Catalog Structure
*(Add screenshot here)*

### DLT Pipeline Graph
*(Add screenshot here)*

### Workflow Run
*(Add screenshot here)*

---

## 🌟 Key Features

- **Real Dataset** — 550,688 records from a real Brazilian e-commerce company
- **Production Architecture** — Medallion pattern used in enterprise data teams
- **Data Quality** — DLT Expectations with warn/drop/fail rules
- **Governance** — Unity Catalog RBAC with PII tagging
- **Automated** — Databricks Workflows scheduled daily
- **Optimized** — OPTIMIZE + Z-ORDER + VACUUM + Auto Compaction
- **Time Travel** — Full Delta Lake history on all tables

---

## 👩‍💻 Author

**Divya Manoj**

Built with ❤️ using Azure Databricks, Delta Lake & Unity Catalog

Dataset: [Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## 📄 License

MIT License — feel free to use and modify!
```

---

After pasting:
```
Scroll down
→ Commit message: "Add professional README"
→ Click "Commit changes"
→ Commit message: "Initial commit: E-Commerce Lakehouse Pipeline"
→ Click "Commit changes"
