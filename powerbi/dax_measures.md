# Power BI — E-commerce Sales Dashboard

Connect Power BI Desktop to Azure SQL (`Get Data → Azure → Azure SQL
Database`) and import `dbo.dim_customer`, `dbo.dim_product`, `dbo.dim_date`,
and `dbo.fact_sales` — the gold star schema Databricks loaded.

## 1. Model relationships

Power BI should auto-detect these from the foreign keys; verify in
**Model view** that all four are **one-to-many, single direction, from
dimension to fact**:

| From | To |
|---|---|
| `dim_customer[customer_key]` (1) | `fact_sales[customer_key]` (many) |
| `dim_product[product_key]` (1)  | `fact_sales[product_key]` (many) |
| `dim_date[date_key]` (1)        | `fact_sales[date_key]` (many) |

This is a textbook star schema — one fact table, three dimensions, all
joins radiating outward from the fact. Everything below filters correctly
through these relationships automatically.

## 2. Measures

Add these to `fact_sales` (or a dedicated `_Measures` table):

```dax
Total Revenue =
SUM ( fact_sales[revenue] )

Total Orders =
DISTINCTCOUNT ( fact_sales[order_id] )

Total Customers =
DISTINCTCOUNT ( fact_sales[customer_key] )

Average Order Value =
DIVIDE ( [Total Revenue], [Total Orders] )
```

Sort `dim_date[month_name]` by a numeric month column (add
`month_sort = dim_date[year] * 100 + dim_date[month]` if you want
multi-year data to sort correctly instead of alphabetically).

## 3. Layout

### KPI cards (top row)
| Card | Measure |
|---|---|
| Total Revenue | `[Total Revenue]` |
| Total Orders | `[Total Orders]` |
| Total Customers | `[Total Customers]` |
| Average Order Value | `[Average Order Value]` |

### Charts

| Chart | Type | Axis | Value |
|---|---|---|---|
| **Monthly Revenue** | Line chart | `dim_date[month_name]` (sorted by `month_sort`) | `[Total Revenue]` |
| **Sales by Product** | Bar chart | `dim_product[product_name]` | `[Total Revenue]` |
| **Sales by Category** | Donut / bar chart | `dim_product[category]` | `[Total Revenue]` |
| **Sales by Region** | Bar chart or map | `dim_customer[region]` | `[Total Revenue]` |
| **Top 10 Customers** | Bar chart, Top N filter = 10 by `[Total Revenue]` | `dim_customer[customer_name]` | `[Total Revenue]` |

For **Top 10 Customers**: add the visual-level filter on
`customer_name`, set filter type to **Top N**, count = 10, "By value" =
`[Total Revenue]`.

### Suggested page layout
- Row 1: the four KPI cards, full width.
- Row 2: Monthly Revenue (wide line chart, left) + Sales by Category (donut, right).
- Row 3: Sales by Product (bar) + Sales by Region (bar or map) + Top 10 Customers (bar).
- Optional slicers: `dim_date[year]`/`month_name`, `dim_customer[region]`,
  `dim_product[category]` — placed as a filter panel on the left so the
  whole page cross-filters together.

## 4. Refresh

- **Manual**: Home → Refresh after the ADF pipeline / Databricks job runs.
- **Scheduled**: once published to the Power BI Service, set Scheduled
  Refresh shortly after the ADF trigger typically fires. Since this
  project's trigger is event-based (`TR_NewFile_In_ADLS`), a fixed refresh
  window won't perfectly match — consider a Power Automate flow or a
  Power BI REST API call at the end of the Databricks notebook to kick off
  an on-demand dataset refresh instead of a purely time-based schedule.
- Requires **"Allow Azure services and resources to access this server"**
  enabled on the Azure SQL server (same as Project 2) so the Power BI
  Service can reach it without a gateway.
