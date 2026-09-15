# Front-Runner SA Company ETL Pipeline

## 📂 Project Directory Structure

The repository follows a production-grade modular framework tailored for automated Databricks asset deployments, isolated localized testing, and a strict separation of concerns:

```text
Front-Runner-SACompany-ETL-Pipeline/
│
├── .github/workflows/               # Automated CI/CD pipelines (GitHub Actions)
├── bundle/resources/                # Databricks Asset Bundle (DAB) resource definitions
├── jobs/                            # Databricks workflow scheduling definitions
├── monitoring/                      # Data quality dashboards and pipeline monitoring scripts
│
├── pipelines/                       # Core PySpark data engineering pipeline modules
│   │
│   ├── ingestion_pipeline/          # BRONZE LAYER: Raw ingestion framework
│   │   ├── tests/                   # Localized unit tests for ingestion logic
│   │   ├── transformations/         # Schema definitions and data casting modules
│   │   └── utilities/               # DBFS loaders, file listeners, and metadata logs
│   │
│   └── transformation_pipeline/     # SILVER & GOLD LAYER: Analytical logic framework
│       ├── tests/                   # Localized unit tests for transformation functions
│       ├── transformations/         # ZAR currency calculations, JSON CDC flattening logic
│       └── utilities/               # Delta optimization helpers and table vacuum scripts
│
├── setup/setup/                     # Cluster provisioning and initialization scripts
│
├── tests/                           # Centralized Automated Testing Suites (Root Level)
│   ├── integration/                 # End-to-end multi-table validation and join pipeline tests
│   └── smoke/                       # Cluster availability and raw data file drop checks
│
├── README.md                        # Project documentation and architectural blueprints
├── data_generator.py                # Relational and CDC mock data generator engine
└── requirements.txt                 # Required Python libraries and dependencies
```

## Data Warehouse ER Diagram


```text
                  ┌──────────────────────┐
                  │    dim_customers     │
                  ├──────────────────────┤
                  │ PK │ customer_id     │ (UUID from customer_cdc)
                  │    │ email           │
                  │    │ first_name      │
                  │    │ last_name       │
                  │    │ loyalty_tier    │
                  │    │ country         │
                  └──────────────────────┘
                             │
                             │ 1
                             │
                             │ ∞
                             ▼
┌──────────────────┐   ∞   ┌──────────────────────┐
│  dim_products    │ ─────►│      fact_orders     │
├──────────────────┤       ├──────────────────────┤
│ PK │ product_id  │       │ PK │ order_item_id   │ 
│    │ sku         │       │ FK │ order_id        │ 
│    │ name        │       │ FK │ customer_id     │ 
│    │ category    │ 1     │ FK │ product_id      │ 
│    │ weight_kg   │ ─────►│    │ timestamp       │
│    │ sales_channel   │    │ quantity        │
│    │ price_usd   │       │    │ unit_price      │
└──────────────────┘       │    │ currency        │
         │                 │    │ total_zar       │ 
         │ 1               └──────────────────────┘
         │                             ▲
         ▼                             │ 1
         ∞                             │
┌──────────────────┐                   │ 1 
│  fact_inventory  │                   ▼
├──────────────────┤       ┌──────────────────────┐
│ PK │ snapshot_id │       │   fact_clickstream   │
│    │ snapshot_date│       ├──────────────────────┤
│ FK │ product_id  │       │ PK │ event_id        │ 
│    │ warehouse_id│       │    │ session_id      │
│    │ qty_on_hand │       │ FK │ customer_id     │ 
│    │ qty_reserved│       │    │ event_type      │ 
│    │ qty_avail   │       │    │ event_timestamp │
└──────────────────┘       │ FK │ product_id      │  
                           │ FK │ order_id        │ 
                           │    │ device_type     │
                           │    │ referrer        │
                           └──────────────────────┘
```

