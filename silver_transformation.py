# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Star Schema Transformation
# MAGIC This notebook transforms raw bronze tables into a dimensional model (star schema) in the `dev_silver` catalog.
# MAGIC
# MAGIC **Source:** `dev_bronze.raw` (orders, people, returns)
# MAGIC
# MAGIC **Target:** `dev_silver.star_schema`
# MAGIC
# MAGIC | Table | Type | Description |
# MAGIC |-------|------|-------------|
# MAGIC | dim_customer | Dimension | Customer attributes and segments |
# MAGIC | dim_product | Dimension | Product hierarchy (category, sub-category) |
# MAGIC | dim_geography | Dimension | Location hierarchy (city, state, region, country) |
# MAGIC | dim_ship_mode | Dimension | Shipping method details |
# MAGIC | dim_date | Dimension | Calendar date attributes |
# MAGIC | dim_regional_manager | Dimension | Regional manager assignments |
# MAGIC | fact_sales | Fact | Sales transaction metrics |
# MAGIC | fact_returns | Fact | Returned order events |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Silver Catalog and Schema

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS dev_silver")
spark.sql("USE CATALOG dev_silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS star_schema")
spark.sql("USE SCHEMA star_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Bronze Tables

# COMMAND ----------

df_orders = spark.table("dev_bronze.raw.orders")
df_people = spark.table("dev_bronze.raw.people")
df_returns = spark.table("dev_bronze.raw.returns")

print(f"Orders: {df_orders.count()} rows")
print(f"People: {df_people.count()} rows")
print(f"Returns: {df_returns.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Dimension Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_customer

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_dim_customer = (
    df_orders
    .select(
        F.col("customer_id"),
        F.col("customer_name"),
        F.col("segment")
    )
    .distinct()
    .withColumn("customer_key", F.monotonically_increasing_id() + 1)
    .select("customer_key", "customer_id", "customer_name", "segment")
)

df_dim_customer.write.mode("overwrite").saveAsTable("dev_silver.star_schema.dim_customer")
print(f"dim_customer: {df_dim_customer.count()} rows")
df_dim_customer.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_product

# COMMAND ----------

df_dim_product = (
    df_orders
    .select(
        F.col("product_id"),
        F.col("category"),
        F.col("sub_category"),
        F.col("product_name")
    )
    .distinct()
    .withColumn("product_key", F.monotonically_increasing_id() + 1)
    .select("product_key", "product_id", "category", "sub_category", "product_name")
)

df_dim_product.write.mode("overwrite").saveAsTable("dev_silver.star_schema.dim_product")
print(f"dim_product: {df_dim_product.count()} rows")
df_dim_product.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_geography

# COMMAND ----------

df_dim_geography = (
    df_orders
    .select(
        F.col("`country/region`").alias("country"),
        F.col("`state/province`").alias("state"),
        F.col("city"),
        F.col("postal_code"),
        F.col("region")
    )
    .distinct()
    .withColumn("geography_key", F.monotonically_increasing_id() + 1)
    .select("geography_key", "country", "state", "city", "postal_code", "region")
)

df_dim_geography.write.mode("overwrite").saveAsTable("dev_silver.star_schema.dim_geography")
print(f"dim_geography: {df_dim_geography.count()} rows")
df_dim_geography.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_ship_mode

# COMMAND ----------

df_dim_ship_mode = (
    df_orders
    .select(F.col("ship_mode"))
    .distinct()
    .withColumn("ship_mode_key", F.monotonically_increasing_id() + 1)
    .withColumn("ship_mode_description",
        F.when(F.col("ship_mode") == "First Class", "Priority shipping (1-2 business days)")
         .when(F.col("ship_mode") == "Second Class", "Standard priority (2-4 business days)")
         .when(F.col("ship_mode") == "Standard Class", "Regular shipping (4-7 business days)")
         .when(F.col("ship_mode") == "Same Day", "Same day delivery")
         .otherwise("Unknown")
    )
    .select("ship_mode_key", "ship_mode", "ship_mode_description")
)

df_dim_ship_mode.write.mode("overwrite").saveAsTable("dev_silver.star_schema.dim_ship_mode")
print(f"dim_ship_mode: {df_dim_ship_mode.count()} rows")
df_dim_ship_mode.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_date

# COMMAND ----------

from pyspark.sql.types import DateType

all_dates = (
    df_orders.select(F.col("order_date").cast(DateType()).alias("date"))
    .union(df_orders.select(F.col("ship_date").cast(DateType()).alias("date")))
    .distinct()
)

df_dim_date = (
    all_dates
    .withColumn("date_key", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_of_week", F.dayofweek("date"))
    .withColumn("day_name", F.date_format("date", "EEEE"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("is_weekend", F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False))
    .select("date_key", "date", "year", "quarter", "month", "month_name", "day", "day_of_week", "day_name", "week_of_year", "is_weekend")
    .orderBy("date")
)

df_dim_date.write.mode("overwrite").saveAsTable("dev_silver.star_schema.dim_date")
print(f"dim_date: {df_dim_date.count()} rows")
df_dim_date.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_regional_manager

# COMMAND ----------

df_dim_regional_manager = (
    df_people
    .withColumnRenamed("regional_manager", "manager_name")
    .withColumn("manager_key", F.monotonically_increasing_id() + 1)
    .select("manager_key", "manager_name", "region")
)

df_dim_regional_manager.write.mode("overwrite").saveAsTable("dev_silver.star_schema.dim_regional_manager")
print(f"dim_regional_manager: {df_dim_regional_manager.count()} rows")
df_dim_regional_manager.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Fact Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_sales

# COMMAND ----------

df_dim_customer_lookup = spark.table("dev_silver.star_schema.dim_customer").select("customer_key", "customer_id")
df_dim_product_lookup = spark.table("dev_silver.star_schema.dim_product").select("product_key", "product_id")
df_dim_geography_lookup = spark.table("dev_silver.star_schema.dim_geography").select("geography_key", "country", "state", "city", "postal_code")
df_dim_ship_mode_lookup = spark.table("dev_silver.star_schema.dim_ship_mode").select("ship_mode_key", "ship_mode")

df_fact_sales = (
    df_orders
    .withColumn("order_date_key", F.date_format(F.col("order_date").cast(DateType()), "yyyyMMdd").cast("int"))
    .withColumn("ship_date_key", F.date_format(F.col("ship_date").cast(DateType()), "yyyyMMdd").cast("int"))
    .join(df_dim_customer_lookup, on="customer_id", how="left")
    .join(df_dim_product_lookup, on="product_id", how="left")
    .join(
        df_dim_geography_lookup,
        on=[
            df_orders["`country/region`"] == df_dim_geography_lookup["country"],
            df_orders["`state/province`"] == df_dim_geography_lookup["state"],
            df_orders["city"] == df_dim_geography_lookup["city"],
            df_orders["postal_code"] == df_dim_geography_lookup["postal_code"]
        ],
        how="left"
    )
    .join(df_dim_ship_mode_lookup, on="ship_mode", how="left")
    .withColumn("sales_key", F.monotonically_increasing_id() + 1)
    .withColumn("revenue", F.col("sales"))
    .withColumn("cost", F.col("sales") - F.col("profit"))
    .withColumn("profit_margin", F.when(F.col("sales") != 0, F.col("profit") / F.col("sales")).otherwise(0))
    .select(
        "sales_key",
        "order_id",
        "order_date_key",
        "ship_date_key",
        "customer_key",
        "product_key",
        "geography_key",
        "ship_mode_key",
        "revenue",
        "quantity",
        "discount",
        "profit",
        "cost",
        "profit_margin"
    )
)

df_fact_sales.write.mode("overwrite").saveAsTable("dev_silver.star_schema.fact_sales")
print(f"fact_sales: {df_fact_sales.count()} rows")
df_fact_sales.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_returns

# COMMAND ----------

df_fact_returns = (
    df_returns
    .filter(F.col("returned") == "Yes")
    .join(
        df_orders.select("order_id", "customer_id", "product_id", "order_date", "region").distinct(),
        on="order_id",
        how="left"
    )
    .join(df_dim_customer_lookup, on="customer_id", how="left")
    .join(df_dim_product_lookup, on="product_id", how="left")
    .withColumn("return_key", F.monotonically_increasing_id() + 1)
    .withColumn("order_date_key", F.date_format(F.col("order_date").cast(DateType()), "yyyyMMdd").cast("int"))
    .select(
        "return_key",
        "order_id",
        "order_date_key",
        "customer_key",
        "product_key"
    )
)

df_fact_returns.write.mode("overwrite").saveAsTable("dev_silver.star_schema.fact_returns")
print(f"fact_returns: {df_fact_returns.count()} rows")
df_fact_returns.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Silver Layer Tables

# COMMAND ----------

print("=" * 70)
print("DEV_SILVER.STAR_SCHEMA - Table Summary")
print("=" * 70)

tables = spark.sql("SHOW TABLES IN dev_silver.star_schema").collect()
for t in tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM dev_silver.star_schema.{t.tableName}").collect()[0].cnt
    table_type = "DIMENSION" if t.tableName.startswith("dim_") else "FACT"
    print(f"  [{table_type}] {t.tableName}: {count:,} rows")

print("=" * 70)
print(f"Total tables: {len(tables)}")
