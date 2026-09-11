# End-to-End E-commerce Data Pipeline ⭐

**Tech:** Azure Data Factory + ADLS Gen2 + Databricks/PySpark + Azure SQL + Power BI

```
                CSV / JSON
                    │
                    ▼
             Azure Data Factory
                    │
                    ▼
                ADLS Gen2
                    │
                    ▼
             ┌─────────────┐
             │ Databricks  │
             │   PySpark   │
             └──────┬──────┘
                    │
              Transformation
                    │
                    ▼
                Azure SQL
                    │
                    ▼
                Power BI
```

A full medallion-architecture pipeline: ADF orchestrates, Databricks/PySpark
does bronze → silver → gold transformation and calculates revenue, gold
tables land in Azure SQL as a proper star schema, and Power BI reads from
there for the dashboard. This repo holds the code and infra-as-code for
all five layers — deploy the ADF/Databricks/Azure SQL pieces into your own
subscription; the PySpark logic itself is fully testable locally.

## Project structure

```
ecommerce_data_pipeline/
├── data/                              # sample raw files (land these in ADLS)
│   ├── customers.csv
│   ├── products.csv
│   └── orders.json                    # JSON on purpose - shows multi-format ingestion
├── adf/                                # orchestration layer (ADF Git-export format)
│   ├── linkedService/
│   │   ├── LS_ADLS_Gen2.json
│   │   └── LS_AzureDatabricks.json
│   ├── dataset/
│   │   └── DS_ADLS_RawFolder.json
│   ├── pipeline/
│   │   └── PL_Ecommerce_Orchestration.json    # GetMetadata check -> Databricks Notebook activity
│   └── trigger/
│       └── TR_NewFile_In_ADLS.json            # event-based: fires when a file lands in ADLS
├── databricks/                         # transformation layer
│   ├── src/
│   │   └── etl_pipeline.py            # standalone script - runs locally, tested end-to-end
│   └── notebooks/
│       └── etl_pipeline_databricks.py # same logic, Databricks notebook format
├── sql/
│   └── create_gold_tables.sql         # star schema DDL for Azure SQL
├── powerbi/
│   └── dax_measures.md                # KPI + chart build guide
├── requirements.txt
└── README.md
```

## Step 1 — ADF: orchestration only

`PL_Ecommerce_Orchestration` does two things, deliberately kept thin:
1. **`Check_Raw_Files_Exist`** — a `GetMetadata` activity confirming the
   raw files actually landed in ADLS before wasting a Databricks cluster
   spin-up on nothing.
2. **`Run_ETL_Notebook`** — a `DatabricksNotebook` activity that runs
   `etl_pipeline_databricks` and passes it the ADLS container path as a
   parameter.

It's triggered by `TR_NewFile_In_ADLS`, a **Blob Events Trigger** — the
pipeline fires automatically when a new file is created under
`raw/` in ADLS, rather than on a fixed schedule. That's closer to how a
real e-commerce ingestion pipeline behaves (upstream systems drop files
whenever they're ready, not on a clock).

ADF intentionally does **no** transformation logic itself — that's the
point of the architecture. All cleaning lives in Databricks.

## Step 2 — Databricks/PySpark: bronze → silver

`databricks/src/etl_pipeline.py` (tested and runs end-to-end locally):

- **Bronze**: lands `customers.csv`, `products.csv`, `orders.json` as-is
  with an explicit schema — no logic, just captured raw.
- **Silver**:
  - **Remove duplicates** — `dropDuplicates` on each entity's natural key.
  - **Handle nulls** — missing `region` becomes `"Unknown"`, missing
    product `price` is backfilled with the category average, missing
    order `quantity` defaults to 1, missing order-line `unit_price` is
    backfilled from the product catalog price during the join.
  - **Fix data types** — string columns from CSV/JSON get cast to proper
    date/numeric types.
  - **Validate records** — drops rows with an unparseable `order_date` or
    non-positive `quantity` (catches a bad date string and a
    negative/zero-quantity row in the sample data).
  - **Join datasets** — orders joined to customers and products, with an
    `"Unknown Customer"` / `"Unknown Product"` fallback for orphan foreign
    keys (the sample data includes a customer ID and a product ID that
    don't exist in the dimension files, on purpose) instead of silently
    dropping those rows.
  - **Calculate revenue**:
    ```python
    silver_sales = joined.withColumn(
        "revenue", F.round(F.col("quantity") * F.col("unit_price"), 2)
    )
    ```

## Step 3 — Gold: business-ready star schema

- **`dim_customer`** — `customer_key`, `customer_id`, `customer_name`,
  `email`, `region`, `signup_date`
- **`dim_product`** — `product_key`, `product_id`, `product_name`,
  `category`, `price`
- **`dim_date`** — `date_key` (`yyyyMMdd`), `order_date`, `year`, `month`,
  `month_name`, `quarter`, `day_of_week`
- **`fact_sales`** — `order_id`, `customer_key`, `product_key`,
  `date_key`, `order_date`, `quantity`, `unit_price`, `revenue`

Written as both Parquet and Delta locally (Delta write is wrapped in a
try/except so the script still completes if `delta-spark` isn't
installed — same pattern as Basic Project 1).

## Step 4 — Azure SQL: the serving layer

`sql/create_gold_tables.sql` creates the same four tables with proper
primary/foreign keys in Azure SQL. The Databricks script's
`write_to_azure_sql()` function loads each gold DataFrame via JDBC
(`spark.write.jdbc(...)`) — wrapped in a try/except locally since there's
no real Azure SQL server to connect to outside a deployed environment, but
the call is real and will work as-is on Databricks with valid
`AZURE_SQL_JDBC_URL` / credentials (use a Databricks secret scope, never
hardcode the password).

## Step 5 — Power BI: E-commerce Sales Dashboard

See `powerbi/dax_measures.md` for the full build guide. KPIs: **Total
Revenue**, **Total Orders**, **Total Customers**, **Average Order Value**.
Charts: **Monthly Revenue**, **Sales by Product**, **Sales by Category**,
**Sales by Region**, **Top 10 Customers** — all built on the star schema
so every visual cross-filters correctly.

## Run the PySpark logic locally

```bash
pip install -r requirements.txt
cd ecommerce_data_pipeline
python3 databricks/src/etl_pipeline.py
```

Reads from `data/` (relative paths), writes bronze/silver/gold to
`output/`. On the sample data: 32 raw order records go in, 28 valid
`fact_sales` rows come out (1 exact duplicate removed, 1 negative-quantity
row filtered, 1 zero-quantity row filtered, 1 unparseable date filtered).

## Deploy the Azure pieces

1. **ADLS Gen2**: container `ecommerce-pipeline`, folder `raw/` — upload
   `data/customers.csv`, `data/products.csv`, `data/orders.json`.
2. **Azure SQL**: run `sql/create_gold_tables.sql`; enable "Allow Azure
   services and resources to access this server" in the SQL server's
   networking settings.
3. **Databricks**: import `databricks/notebooks/etl_pipeline_databricks.py`
   as a notebook (or connect the workspace to this repo via Repos), attach
   a cluster, and add a secret scope named `ecommerce-pipeline` with
   `sql-user` / `sql-password` keys.
4. **ADF**: connect ADF Studio's Git integration to this repo (picks up
   everything under `adf/` automatically), or recreate manually — Linked
   Services first, then the Dataset, then the Pipeline, then the Trigger.
   Fill in the real storage account / Databricks workspace URL / access
   token in place of the placeholders (use Key Vault, never commit real
   secrets).
5. **Power BI**: connect Power BI Desktop to the Azure SQL database and
   follow `powerbi/dax_measures.md`.

## Why this is a strong resume project

It touches every layer a real data-engineering JD usually asks for in one
place: **orchestration** (ADF — Linked Service, Dataset, Pipeline, Copy/
GetMetadata/Notebook activities, event-based Trigger), **distributed
processing** (Databricks/PySpark — schema enforcement, null handling,
deduplication, validation, joins, aggregation), **data modeling** (a real
star schema with surrogate keys and a conformed date dimension), **RDBMS/
Azure SQL** (DDL, foreign keys, JDBC writes), and **BI** (Power BI, DAX
measures, a multi-visual dashboard). That combination — not any single
tool — is what the phrase "end-to-end pipeline" is supposed to mean.
