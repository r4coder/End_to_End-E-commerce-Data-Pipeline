/* ============================================================
   End-to-End E-commerce Data Pipeline
   Gold layer star schema in Azure SQL Database.

   Databricks loads these tables via JDBC (see
   databricks/src/etl_pipeline.py / notebooks/etl_pipeline_databricks.py).
   Run this script once, up front, so the tables exist with the right
   shape and keys before the first pipeline run.
   ============================================================ */

IF OBJECT_ID('dbo.fact_sales', 'U') IS NOT NULL DROP TABLE dbo.fact_sales;
IF OBJECT_ID('dbo.dim_customer', 'U') IS NOT NULL DROP TABLE dbo.dim_customer;
IF OBJECT_ID('dbo.dim_product', 'U') IS NOT NULL DROP TABLE dbo.dim_product;
IF OBJECT_ID('dbo.dim_date', 'U') IS NOT NULL DROP TABLE dbo.dim_date;
GO

CREATE TABLE dbo.dim_customer (
    customer_key    INT             NOT NULL PRIMARY KEY,
    customer_id     VARCHAR(10)     NOT NULL,
    customer_name   NVARCHAR(100)   NOT NULL,
    email           NVARCHAR(150)   NULL,
    region          NVARCHAR(50)    NOT NULL,
    signup_date     DATE            NULL
);
GO

CREATE TABLE dbo.dim_product (
    product_key     INT             NOT NULL PRIMARY KEY,
    product_id      VARCHAR(10)     NOT NULL,
    product_name    NVARCHAR(100)   NOT NULL,
    category        NVARCHAR(50)    NOT NULL,
    price           DECIMAL(10,2)   NOT NULL
);
GO

CREATE TABLE dbo.dim_date (
    date_key        INT             NOT NULL PRIMARY KEY,   -- yyyyMMdd
    order_date      DATE            NOT NULL,
    year            INT             NOT NULL,
    month           INT             NOT NULL,
    month_name      VARCHAR(10)     NOT NULL,
    quarter         INT             NOT NULL,
    day_of_week     VARCHAR(10)     NOT NULL
);
GO

CREATE TABLE dbo.fact_sales (
    order_id        INT             NOT NULL PRIMARY KEY,
    customer_key    INT             NOT NULL,
    product_key     INT             NOT NULL,
    date_key        INT             NOT NULL,
    order_date      DATE            NOT NULL,
    quantity        INT             NOT NULL,
    unit_price      DECIMAL(10,2)   NOT NULL,
    revenue         DECIMAL(12,2)   NOT NULL,
    CONSTRAINT FK_fact_sales_customer FOREIGN KEY (customer_key) REFERENCES dbo.dim_customer(customer_key),
    CONSTRAINT FK_fact_sales_product  FOREIGN KEY (product_key)  REFERENCES dbo.dim_product(product_key),
    CONSTRAINT FK_fact_sales_date     FOREIGN KEY (date_key)     REFERENCES dbo.dim_date(date_key)
);
GO

/* ------------------------------------------------------------
   KPI / chart queries backing the Power BI dashboard
   (Power BI will express these as DAX measures instead - see
   powerbi/dax_measures.md - but these are handy for sanity checks
   straight in Azure SQL.)
   ------------------------------------------------------------ */

-- Total Revenue / Total Orders / Total Customers / Average Order Value
-- SELECT
--     SUM(revenue)                              AS total_revenue,
--     COUNT(DISTINCT order_id)                  AS total_orders,
--     COUNT(DISTINCT customer_key)               AS total_customers,
--     SUM(revenue) / COUNT(DISTINCT order_id)   AS avg_order_value
-- FROM dbo.fact_sales;

-- Monthly Revenue
-- SELECT d.year, d.month, d.month_name, SUM(f.revenue) AS revenue
-- FROM dbo.fact_sales f JOIN dbo.dim_date d ON f.date_key = d.date_key
-- GROUP BY d.year, d.month, d.month_name
-- ORDER BY d.year, d.month;

-- Sales by Product / Category
-- SELECT p.product_name, p.category, SUM(f.revenue) AS revenue
-- FROM dbo.fact_sales f JOIN dbo.dim_product p ON f.product_key = p.product_key
-- GROUP BY p.product_name, p.category
-- ORDER BY revenue DESC;

-- Sales by Region
-- SELECT c.region, SUM(f.revenue) AS revenue
-- FROM dbo.fact_sales f JOIN dbo.dim_customer c ON f.customer_key = c.customer_key
-- GROUP BY c.region
-- ORDER BY revenue DESC;

-- Top 10 Customers
-- SELECT TOP 10 c.customer_name, SUM(f.revenue) AS revenue
-- FROM dbo.fact_sales f JOIN dbo.dim_customer c ON f.customer_key = c.customer_key
-- GROUP BY c.customer_name
-- ORDER BY revenue DESC;
