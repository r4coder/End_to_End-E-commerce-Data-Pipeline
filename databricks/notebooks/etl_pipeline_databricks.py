# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End E-commerce Data Pipeline
# MAGIC **Tech:** ADF (orchestration) + ADLS Gen2 + Databricks/PySpark + Azure SQL + Power BI
# MAGIC
# MAGIC ADLS (raw CSV/JSON) → **Bronze** → **Silver** (clean/validate/join/revenue) →
# MAGIC **Gold** (star schema) → Azure SQL → Power BI
# MAGIC
# MAGIC This notebook mirrors `databricks/src/etl_pipeline.py`. ADF's Databricks
# MAGIC Notebook Activity runs this notebook as the transformation step of the
# MAGIC orchestration pipeline (see `adf/pipeline/PL_Ecommerce_Orchestration.json`).
# MAGIC Upload `data/customers.csv`, `data/products.csv`, `data/orders.json` to
# MAGIC ADLS and update the paths in the config cell below.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark.conf.set("spark.sql.ansi.enabled", "false")  # allow to_date() to return null instead of raising

# COMMAND ----------

# MAGIC %md ### 0. Config - point these at ADLS (mounted or abfss:// paths) and Azure SQL

# COMMAND ----------

dbutils.widgets.text("adls_container", "abfss://sales-pipeline@<storage-account>.dfs.core.windows.net")
ADLS_ROOT = dbutils.widgets.get("adls_container")

RAW_CUSTOMERS_PATH = f"{ADLS_ROOT}/raw/customers.csv"
RAW_PRODUCTS_PATH = f"{ADLS_ROOT}/raw/products.csv"
RAW_ORDERS_PATH = f"{ADLS_ROOT}/raw/orders.json"

BRONZE_PATH = f"{ADLS_ROOT}/bronze"
SILVER_PATH = f"{ADLS_ROOT}/silver"
GOLD_PATH = f"{ADLS_ROOT}/gold"

AZURE_SQL_JDBC_URL = (
    "jdbc:sqlserver://<your-sql-server-name>.database.windows.net:1433;"
    "database=<your-database-name>;encrypt=true;trustServerCertificate=false;"
)
AZURE_SQL_PROPERTIES = {
    "user": dbutils.secrets.get(scope="ecommerce-pipeline", key="sql-user"),
    "password": dbutils.secrets.get(scope="ecommerce-pipeline", key="sql-password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# COMMAND ----------

# MAGIC %md ### 1. Bronze - land raw files as-is

# COMMAND ----------

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

bronze_customers = spark.read.option("header", True).schema(customers_schema).csv(RAW_CUSTOMERS_PATH)
bronze_products = spark.read.option("header", True).schema(products_schema).csv(RAW_PRODUCTS_PATH)
bronze_orders = spark.read.option("multiLine", True).schema(orders_schema).json(RAW_ORDERS_PATH)

display(bronze_orders)

# COMMAND ----------

bronze_customers.write.mode("overwrite").format("delta").save(f"{BRONZE_PATH}/customers")
bronze_products.write.mode("overwrite").format("delta").save(f"{BRONZE_PATH}/products")
bronze_orders.write.mode("overwrite").format("delta").save(f"{BRONZE_PATH}/orders")

# COMMAND ----------

# MAGIC %md ### 2. Silver - dedupe, fix types, validate, join, calculate revenue

# COMMAND ----------

silver_customers = (
    bronze_customers
    .dropDuplicates(["customer_id"])
    .withColumn("signup_date", F.to_date("signup_date", "yyyy-MM-dd"))
    .withColumn("region", F.when(F.col("region").isNull() | (F.col("region") == ""),
                                  F.lit("Unknown")).otherwise(F.col("region")))
    .dropna(subset=["customer_id", "customer_name"])
)

products_typed = (
    bronze_products
    .dropDuplicates(["product_id"])
    .withColumn("price", F.col("price").cast(DoubleType()))
    .dropna(subset=["product_id", "product_name"])
)
category_avg_price = products_typed.groupBy("category").agg(F.avg("price").alias("category_avg_price"))
silver_products = (
    products_typed.join(category_avg_price, on="category", how="left")
    .withColumn("price", F.coalesce(F.col("price"), F.col("category_avg_price")))
    .drop("category_avg_price")
)

silver_orders = (
    bronze_orders
    .dropDuplicates(["order_id"])
    .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
    .withColumn("quantity", F.coalesce(F.col("quantity"), F.lit(1)))
    .filter(F.col("order_date").isNotNull() & (F.col("quantity") > 0))
)

# COMMAND ----------

joined = (
    silver_orders
    .join(silver_customers.select("customer_id", "customer_name", "region"), on="customer_id", how="left")
    .join(silver_products.select("product_id", "product_name", "category", "price")
          .withColumnRenamed("price", "catalog_price"), on="product_id", how="left")
    .withColumn("customer_name", F.coalesce(F.col("customer_name"), F.lit("Unknown Customer")))
    .withColumn("region", F.coalesce(F.col("region"), F.lit("Unknown")))
    .withColumn("product_name", F.coalesce(F.col("product_name"), F.lit("Unknown Product")))
    .withColumn("category", F.coalesce(F.col("category"), F.lit("Unknown")))
    .withColumn("unit_price", F.coalesce(F.col("unit_price"), F.col("catalog_price")))
)
joined = joined.dropna(subset=["unit_price"])

# quantity * price
silver_sales = joined.withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
display(silver_sales)

# COMMAND ----------

silver_customers.write.mode("overwrite").format("delta").save(f"{SILVER_PATH}/customers")
silver_products.write.mode("overwrite").format("delta").save(f"{SILVER_PATH}/products")
silver_sales.write.mode("overwrite").format("delta").save(f"{SILVER_PATH}/sales")

# COMMAND ----------

# MAGIC %md ### 3. Gold - star schema: fact_sales, dim_customer, dim_product, dim_date

# COMMAND ----------

dim_customer = (
    silver_customers.select("customer_id", "customer_name", "email", "region", "signup_date")
    .withColumn("customer_key", F.row_number().over(Window.orderBy("customer_id")))
)
dim_product = (
    silver_products.select("product_id", "product_name", "category", "price")
    .withColumn("product_key", F.row_number().over(Window.orderBy("product_id")))
)
dim_date = (
    silver_sales.select("order_date").distinct()
    .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast(IntegerType()))
    .withColumn("year", F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .withColumn("month_name", F.date_format("order_date", "MMM"))
    .withColumn("quarter", F.quarter("order_date"))
    .withColumn("day_of_week", F.date_format("order_date", "EEEE"))
)
fact_sales = (
    silver_sales
    .join(dim_customer.select("customer_id", "customer_key"), on="customer_id", how="left")
    .join(dim_product.select("product_id", "product_key"), on="product_id", how="left")
    .join(dim_date.select("order_date", "date_key"), on="order_date", how="left")
    .select("order_id", "customer_key", "product_key", "date_key", "order_date",
            "quantity", "unit_price", "revenue")
)
display(fact_sales)

# COMMAND ----------

dim_customer.write.mode("overwrite").format("delta").save(f"{GOLD_PATH}/dim_customer")
dim_product.write.mode("overwrite").format("delta").save(f"{GOLD_PATH}/dim_product")
dim_date.write.mode("overwrite").format("delta").save(f"{GOLD_PATH}/dim_date")
fact_sales.write.mode("overwrite").format("delta").save(f"{GOLD_PATH}/fact_sales")

# Optional: register as tables in the metastore for easy SQL access / Genie
for name in ["dim_customer", "dim_product", "dim_date", "fact_sales"]:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS ecommerce_gold.{name}
        USING DELTA
        LOCATION '{GOLD_PATH}/{name}'
    """)

# COMMAND ----------

# MAGIC %md ### 4. Load to Azure SQL - the layer Power BI connects to

# COMMAND ----------

for name, df in [("dim_customer", dim_customer), ("dim_product", dim_product),
                  ("dim_date", dim_date), ("fact_sales", fact_sales)]:
    (
        df.write
        .jdbc(url=AZURE_SQL_JDBC_URL, table=name, mode="overwrite", properties=AZURE_SQL_PROPERTIES)
    )
    print(f"Loaded {name} into Azure SQL.")

# COMMAND ----------

# MAGIC %md
# MAGIC Pipeline complete. Point Power BI at the Azure SQL database and build the
# MAGIC dashboard described in `powerbi/dax_measures.md`.
