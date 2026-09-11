"""
End-to-End E-commerce Data Pipeline - Databricks / PySpark
=============================================================
Tech: ADF (orchestration) + ADLS Gen2 (storage) + Databricks/PySpark
      (transformation) + Azure SQL (serving) + Power BI (dashboard)

This script implements the Databricks half of the architecture:

    ADLS (raw CSV/JSON)
        -> Bronze  (raw, as-is, just landed)
        -> Silver  (cleaned, validated, joined, revenue calculated)
        -> Gold    (star schema: fact_sales, dim_customer, dim_product, dim_date)
        -> Azure SQL (final load for Power BI)

Run locally:
    spark-submit databricks/src/etl_pipeline.py

Run on Databricks:
    Import databricks/notebooks/etl_pipeline_databricks.py as a notebook,
    or call this script's functions from a Databricks Job. ADF invokes this
    logic via a Databricks Notebook Activity (see adf/pipeline/).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# ----------------------------------------------------------------------
# 0. CONFIG - swap these for /dbfs/... or abfss://... paths on Databricks
# ----------------------------------------------------------------------
RAW_CUSTOMERS_PATH = "data/customers.csv"
RAW_PRODUCTS_PATH = "data/products.csv"
RAW_ORDERS_PATH = "data/orders.json"

BRONZE_PATH = "output/bronze"
SILVER_PATH = "output/silver"
GOLD_PATH = "output/gold"

# Azure SQL connection - fill in for a real run; left as placeholders here.
AZURE_SQL_JDBC_URL = (
    "jdbc:sqlserver://<your-sql-server-name>.database.windows.net:1433;"
    "database=<your-database-name>;encrypt=true;trustServerCertificate=false;"
)
AZURE_SQL_PROPERTIES = {
    "user": "<your-sql-admin-user>",
    "password": "<use a secret scope / Key Vault-backed secret - never hardcode>",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

spark = (
    SparkSession.builder
    .appName("EcommerceDataPipeline")
    # ANSI mode makes to_date() raise on unparseable strings instead of
    # returning null. We WANT null here - that's how a bad "not-a-date"
    # value gets caught by the validation filter below - so disable it.
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()
)

# ========================================================================
# BRONZE LAYER - land the raw files as-is, no transformations
# ========================================================================

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("region", StringType(), True),
    StructField("signup_date", StringType(), True),
])

products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", StringType(), True),
])

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
])

bronze_customers = (
    spark.read.option("header", True).schema(customers_schema).csv(RAW_CUSTOMERS_PATH)
)
bronze_products = (
    spark.read.option("header", True).schema(products_schema).csv(RAW_PRODUCTS_PATH)
)
bronze_orders = (
    spark.read.option("multiLine", True).schema(orders_schema).json(RAW_ORDERS_PATH)
)

print(f"Bronze counts -> customers: {bronze_customers.count()}, "
      f"products: {bronze_products.count()}, orders: {bronze_orders.count()}")

bronze_customers.write.mode("overwrite").parquet(f"{BRONZE_PATH}/customers")
bronze_products.write.mode("overwrite").parquet(f"{BRONZE_PATH}/products")
bronze_orders.write.mode("overwrite").parquet(f"{BRONZE_PATH}/orders")

# ========================================================================
# SILVER LAYER - clean, validate, fix types, deduplicate, join
# ========================================================================

# --- Customers: dedupe, fix types, fill missing region ---
silver_customers = (
    bronze_customers
    .dropDuplicates(["customer_id"])
    .withColumn("signup_date", F.to_date("signup_date", "yyyy-MM-dd"))
    .withColumn("region", F.when(F.col("region").isNull() | (F.col("region") == ""),
                                  F.lit("Unknown")).otherwise(F.col("region")))
    .dropna(subset=["customer_id", "customer_name"])
)

# --- Products: dedupe, fix types, fill missing price with category average ---
products_typed = (
    bronze_products
    .dropDuplicates(["product_id"])
    .withColumn("price", F.col("price").cast(DoubleType()))
    .dropna(subset=["product_id", "product_name"])
)
category_avg_price = (
    products_typed.groupBy("category")
    .agg(F.avg("price").alias("category_avg_price"))
)
silver_products = (
    products_typed.join(category_avg_price, on="category", how="left")
    .withColumn("price", F.coalesce(F.col("price"), F.col("category_avg_price")))
    .drop("category_avg_price")
)

# --- Orders: dedupe, fix types, validate ---
silver_orders = (
    bronze_orders
    .dropDuplicates(["order_id"])
    .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
    .withColumn("quantity", F.coalesce(F.col("quantity"), F.lit(1)))  # assume 1 if missing
)

# Validate: order_date must parse, quantity must be positive.
# unit_price may still be null here - it gets backfilled from the product's
# price in the join step below, then re-validated.
silver_orders = silver_orders.filter(
    F.col("order_date").isNotNull() & (F.col("quantity") > 0)
)

# --- Join orders with customers/products, with an "Unknown" fallback for
#     orphan foreign keys instead of silently dropping the row ---
joined = (
    silver_orders
    .join(silver_customers.select("customer_id", "customer_name", "region"),
          on="customer_id", how="left")
    .join(silver_products.select("product_id", "product_name", "category", "price")
          .withColumnRenamed("price", "catalog_price"),
          on="product_id", how="left")
    .withColumn("customer_name", F.coalesce(F.col("customer_name"), F.lit("Unknown Customer")))
    .withColumn("region", F.coalesce(F.col("region"), F.lit("Unknown")))
    .withColumn("product_name", F.coalesce(F.col("product_name"), F.lit("Unknown Product")))
    .withColumn("category", F.coalesce(F.col("category"), F.lit("Unknown")))
    # backfill a missing order-line price with the product's current catalog price
    .withColumn("unit_price", F.coalesce(F.col("unit_price"), F.col("catalog_price")))
)

# Final validity check now that unit_price has been backfilled - drop any
# row that still has no usable price (e.g. both the order line AND the
# product catalog were missing a price).
joined = joined.dropna(subset=["unit_price"])

# --- Calculate revenue ---
silver_sales = joined.withColumn(
    "revenue", F.round(F.col("quantity") * F.col("unit_price"), 2)
)

print("Silver sales sample:")
silver_sales.show(10)

silver_customers.write.mode("overwrite").parquet(f"{SILVER_PATH}/customers")
silver_products.write.mode("overwrite").parquet(f"{SILVER_PATH}/products")
silver_sales.write.mode("overwrite").parquet(f"{SILVER_PATH}/sales")

# ========================================================================
# GOLD LAYER - business-ready star schema
# ========================================================================

# --- dim_customer ---
dim_customer = (
    silver_customers
    .select("customer_id", "customer_name", "email", "region", "signup_date")
    .withColumn("customer_key", F.row_number().over(Window.orderBy("customer_id")))
)

# --- dim_product ---
dim_product = (
    silver_products
    .select("product_id", "product_name", "category", "price")
    .withColumn("product_key", F.row_number().over(Window.orderBy("product_id")))
)

# --- dim_date --- (one row per distinct order date, plus standard calendar attributes)
dim_date = (
    silver_sales.select("order_date").distinct()
    .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast(IntegerType()))
    .withColumn("year", F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .withColumn("month_name", F.date_format("order_date", "MMM"))
    .withColumn("quarter", F.quarter("order_date"))
    .withColumn("day_of_week", F.date_format("order_date", "EEEE"))
    .orderBy("order_date")
)

# --- fact_sales --- (grain: one row per order)
fact_sales = (
    silver_sales
    .join(dim_customer.select("customer_id", "customer_key"), on="customer_id", how="left")
    .join(dim_product.select("product_id", "product_key"), on="product_id", how="left")
    .join(dim_date.select("order_date", "date_key"), on="order_date", how="left")
    .select(
        "order_id",
        "customer_key",
        "product_key",
        "date_key",
        "order_date",
        "quantity",
        "unit_price",
        "revenue",
    )
)

print("fact_sales sample:")
fact_sales.show(10)
print(f"Gold counts -> fact_sales: {fact_sales.count()}, "
      f"dim_customer: {dim_customer.count()}, dim_product: {dim_product.count()}, "
      f"dim_date: {dim_date.count()}")

dim_customer.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_customer")
dim_product.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_product")
dim_date.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_date")
fact_sales.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_sales")

# Delta versions (Databricks-native; falls back gracefully if delta-spark
# isn't installed in this environment, same pattern as the Basic Project 1 script)
try:
    for name, df in [("dim_customer", dim_customer), ("dim_product", dim_product),
                      ("dim_date", dim_date), ("fact_sales", fact_sales)]:
        df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}_delta/{name}")
    print("Gold tables also written as Delta.")
except Exception as e:
    print("Delta write skipped (delta-spark not available in this environment).")
    print(f"Reason: {e}")

# ========================================================================
# LOAD TO AZURE SQL - final step Power BI connects to
# ========================================================================

def write_to_azure_sql(df, table_name, mode="overwrite"):
    """Writes a gold DataFrame to Azure SQL via JDBC.

    On Databricks, the SQL Server JDBC driver is available out of the box.
    Locally this needs the mssql-jdbc jar on the classpath, and real
    AZURE_SQL_JDBC_URL / AZURE_SQL_PROPERTIES values - so this is wrapped
    in a try/except to keep local runs of this script green.
    """
    try:
        (
            df.write
            .jdbc(url=AZURE_SQL_JDBC_URL, table=table_name, mode=mode,
                  properties=AZURE_SQL_PROPERTIES)
        )
        print(f"Wrote {table_name} to Azure SQL.")
    except Exception as e:
        print(f"Skipped Azure SQL write for {table_name} "
              f"(expected outside a configured Azure environment). Reason: {e}")


write_to_azure_sql(dim_customer, "dim_customer")
write_to_azure_sql(dim_product, "dim_product")
write_to_azure_sql(dim_date, "dim_date")
write_to_azure_sql(fact_sales, "fact_sales")

spark.stop()
