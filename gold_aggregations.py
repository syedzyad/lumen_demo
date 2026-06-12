# New Aggregation: Discount Impact Analysis by Segment
# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Business Aggregations for Reporting
# MAGIC This notebook creates curated, business-ready aggregation tables in `dev_gold` catalog
# MAGIC by reading from the silver star schema layer.
# MAGIC
# MAGIC **Source:** `dev_silver.star_schema` (dim + fact tables)
# MAGIC
# MAGIC **Target:** `dev_gold.reporting`
# MAGIC
# MAGIC | Table | Purpose |
# MAGIC |-------|---------|
# MAGIC | agg_monthly_sales | Monthly revenue, profit, and order KPIs |
# MAGIC | agg_customer_lifetime_value | Customer LTV, frequency, and segmentation |
# MAGIC | agg_product_performance | Product-level sales and return metrics |
# MAGIC | agg_regional_summary | Regional performance with manager attribution |
# MAGIC | agg_category_trends | Quarterly category-level trends |
# MAGIC | agg_shipping_analysis | Shipping mode efficiency and cost metrics |
# MAGIC | agg_state_performance | State-level sales performance and profitability |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Gold Catalog and Schema

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS dev_gold")
spark.sql("USE CATALOG dev_gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS reporting")
spark.sql("USE SCHEMA reporting")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Silver Layer Tables

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

fact_sales = spark.table("dev_silver.star_schema.fact_sales")
fact_returns = spark.table("dev_silver.star_schema.fact_returns")
dim_customer = spark.table("dev_silver.star_schema.dim_customer")
dim_product = spark.table("dev_silver.star_schema.dim_product")
dim_geography = spark.table("dev_silver.star_schema.dim_geography")
dim_date = spark.table("dev_silver.star_schema.dim_date")
dim_ship_mode = spark.table("dev_silver.star_schema.dim_ship_mode")
dim_regional_manager = spark.table("dev_silver.star_schema.dim_regional_manager")

print("Silver layer tables loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Gold Aggregation Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### agg_monthly_sales
# MAGIC Monthly KPIs: total revenue, profit, orders, avg order value, discount impact

# COMMAND ----------

agg_monthly_sales = (
    fact_sales
    .join(dim_date, fact_sales.order_date_key == dim_date.date_key, "left")
    .groupBy("year", "month", "month_name")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("profit").alias("total_profit"),
        F.sum("cost").alias("total_cost"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_units_sold"),
        F.avg("revenue").alias("avg_line_item_value"),
        F.avg("discount").alias("avg_discount"),
        F.avg("profit_margin").alias("avg_profit_margin"),
        F.sum(F.col("revenue") * F.col("discount")).alias("total_discount_amount")
    )
    .withColumn("revenue_per_order", F.col("total_revenue") / F.col("total_orders"))
    .withColumn("profit_per_order", F.col("total_profit") / F.col("total_orders"))
    .orderBy("year", "month")
)

agg_monthly_sales.write.mode("overwrite").saveAsTable("dev_gold.reporting.agg_monthly_sales")
print(f"agg_monthly_sales: {agg_monthly_sales.count()} rows")
agg_monthly_sales.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### agg_customer_lifetime_value
# MAGIC Customer-level metrics: total spend, order frequency, avg order value, segment, return rate

# COMMAND ----------

customer_sales = (
    fact_sales
    .join(dim_customer, "customer_key", "left")
    .join(dim_date, fact_sales.order_date_key == dim_date.date_key, "left")
    .groupBy("customer_key", "customer_id", "customer_name", "segment")
    .agg(
        F.sum("revenue").alias("lifetime_revenue"),
        F.sum("profit").alias("lifetime_profit"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_units_purchased"),
        F.avg("revenue").alias("avg_order_value"),
        F.min("date").alias("first_order_date"),
        F.max("date").alias("last_order_date"),
        F.avg("discount").alias("avg_discount_received")
    )
)

customer_returns = (
    fact_returns
    .groupBy("customer_key")
    .agg(F.countDistinct("order_id").alias("total_returns"))
)

agg_customer_ltv = (
    customer_sales
    .join(customer_returns, "customer_key", "left")
    .withColumn("total_returns", F.coalesce(F.col("total_returns"), F.lit(0)))
    .withColumn("return_rate", F.col("total_returns") / F.col("total_orders"))
    .withColumn("customer_tenure_days",
        F.datediff(F.col("last_order_date"), F.col("first_order_date"))
    )
    .withColumn("ltv_tier",
        F.when(F.col("lifetime_revenue") >= 5000, "Platinum")
         .when(F.col("lifetime_revenue") >= 2000, "Gold")
         .when(F.col("lifetime_revenue") >= 500, "Silver")
         .otherwise("Bronze")
    )
    .orderBy(F.desc("lifetime_revenue"))
)

agg_customer_ltv.write.mode("overwrite").saveAsTable("dev_gold.reporting.agg_customer_lifetime_value")
print(f"agg_customer_lifetime_value: {agg_customer_ltv.count()} rows")
agg_customer_ltv.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### agg_product_performance
# MAGIC Product-level metrics: revenue, profit margin, units sold, return count, ranking

# COMMAND ----------

product_sales = (
    fact_sales
    .join(dim_product, "product_key", "left")
    .groupBy("product_key", "product_id", "product_name", "category", "sub_category")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("profit").alias("total_profit"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("order_id").alias("total_orders"),
        F.avg("profit_margin").alias("avg_profit_margin"),
        F.avg("discount").alias("avg_discount")
    )
)

product_returns = (
    fact_returns
    .groupBy("product_key")
    .agg(F.count("return_key").alias("total_returns"))
)

agg_product_performance = (
    product_sales
    .join(product_returns, "product_key", "left")
    .withColumn("total_returns", F.coalesce(F.col("total_returns"), F.lit(0)))
    .withColumn("return_rate",
        F.when(F.col("total_orders") > 0, F.col("total_returns") / F.col("total_orders"))
         .otherwise(0)
    )
    .withColumn("revenue_rank",
        F.rank().over(Window.orderBy(F.desc("total_revenue")))
    )
    .withColumn("profit_rank",
        F.rank().over(Window.orderBy(F.desc("total_profit")))
    )
    .orderBy(F.desc("total_revenue"))
)

agg_product_performance.write.mode("overwrite").saveAsTable("dev_gold.reporting.agg_product_performance")
print(f"agg_product_performance: {agg_product_performance.count()} rows")
agg_product_performance.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### agg_regional_summary
# MAGIC Regional performance with manager attribution, customer count, and profitability

# COMMAND ----------

agg_regional_summary = (
    fact_sales
    .join(dim_geography, "geography_key", "left")
    .join(dim_regional_manager, dim_geography.region == dim_regional_manager.region, "left")
    .groupBy(
        dim_geography.region,
        "manager_name"
    )
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("profit").alias("total_profit"),
        F.sum("cost").alias("total_cost"),
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_key").alias("total_customers"),
        F.sum("quantity").alias("total_units_sold"),
        F.avg("profit_margin").alias("avg_profit_margin"),
        F.avg("discount").alias("avg_discount")
    )
    .withColumn("revenue_per_customer", F.col("total_revenue") / F.col("total_customers"))
    .withColumn("profit_per_customer", F.col("total_profit") / F.col("total_customers"))
    .orderBy(F.desc("total_revenue"))
)

agg_regional_summary.write.mode("overwrite").saveAsTable("dev_gold.reporting.agg_regional_summary")
print(f"agg_regional_summary: {agg_regional_summary.count()} rows")
agg_regional_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### agg_category_trends
# MAGIC Quarterly trends by category and sub-category with YoY growth calculation

# COMMAND ----------

category_quarterly = (
    fact_sales
    .join(dim_product, "product_key", "left")
    .join(dim_date, fact_sales.order_date_key == dim_date.date_key, "left")
    .groupBy("year", "quarter", "category", "sub_category")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("profit").alias("total_profit"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("order_id").alias("total_orders"),
        F.avg("profit_margin").alias("avg_profit_margin")
    )
)

window_yoy = Window.partitionBy("quarter", "category", "sub_category").orderBy("year")

agg_category_trends = (
    category_quarterly
    .withColumn("prev_year_revenue", F.lag("total_revenue").over(window_yoy))
    .withColumn("yoy_revenue_growth",
        F.when(F.col("prev_year_revenue").isNotNull(),
            (F.col("total_revenue") - F.col("prev_year_revenue")) / F.col("prev_year_revenue") * 100
        ).otherwise(None)
    )
    .withColumn("prev_year_profit", F.lag("total_profit").over(window_yoy))
    .withColumn("yoy_profit_growth",
        F.when(F.col("prev_year_profit").isNotNull(),
            (F.col("total_profit") - F.col("prev_year_profit")) / F.col("prev_year_profit") * 100
        ).otherwise(None)
    )
    .orderBy("year", "quarter", "category", "sub_category")
)

agg_category_trends.write.mode("overwrite").saveAsTable("dev_gold.reporting.agg_category_trends")
print(f"agg_category_trends: {agg_category_trends.count()} rows")
agg_category_trends.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### agg_shipping_analysis
# MAGIC Shipping mode efficiency: delivery time, revenue contribution, cost analysis

# COMMAND ----------

agg_shipping_analysis = (
    fact_sales
    .join(dim_ship_mode, "ship_mode_key", "left")
    .join(
        dim_date.withColumnRenamed("date_key", "order_dk").withColumnRenamed("date", "order_dt"),
        fact_sales.order_date_key == F.col("order_dk"),
        "left"
    )
    .join(
        dim_date.select(
            F.col("date_key").alias("ship_dk"),
            F.col("date").alias("ship_dt")
        ),
        fact_sales.ship_date_key == F.col("ship_dk"),
        "left"
    )
    .withColumn("delivery_days", F.datediff(F.col("ship_dt"), F.col("order_dt")))
    .groupBy("ship_mode", "ship_mode_description")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("profit").alias("total_profit"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_units_shipped"),
        F.avg("delivery_days").alias("avg_delivery_days"),
        F.min("delivery_days").alias("min_delivery_days"),
        F.max("delivery_days").alias("max_delivery_days"),
        F.avg("profit_margin").alias("avg_profit_margin")
    )
    .withColumn("revenue_share_pct",
        F.col("total_revenue") / F.sum("total_revenue").over(Window.partitionBy()) * 100
    )
    .withColumn("order_share_pct",
        F.col("total_orders") / F.sum("total_orders").over(Window.partitionBy()) * 100
    )
    .orderBy(F.desc("total_revenue"))
)

agg_shipping_analysis.write.mode("overwrite").saveAsTable("dev_gold.reporting.agg_shipping_analysis")
print(f"agg_shipping_analysis: {agg_shipping_analysis.count()} rows")
agg_shipping_analysis.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### agg_state_performance
# MAGIC State-level sales performance: revenue, profit, customer density, avg order size

# COMMAND ----------

agg_state_performance = (
    fact_sales
    .join(dim_geography, "geography_key", "left")
    .join(dim_date, fact_sales.order_date_key == dim_date.date_key, "left")
    .groupBy("country", "state", "region")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("profit").alias("total_profit"),
        F.sum("cost").alias("total_cost"),
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_key").alias("total_customers"),
        F.sum("quantity").alias("total_units_sold"),
        F.avg("profit_margin").alias("avg_profit_margin"),
        F.avg("discount").alias("avg_discount"),
        F.min("date").alias("first_order_date"),
        F.max("date").alias("last_order_date")
    )
    .withColumn("revenue_per_customer", F.col("total_revenue") / F.col("total_customers"))
    .withColumn("orders_per_customer", F.col("total_orders") / F.col("total_customers"))
    .withColumn("avg_order_value", F.col("total_revenue") / F.col("total_orders"))
    .withColumn("profit_to_cost_ratio",
        F.when(F.col("total_cost") > 0, F.col("total_profit") / F.col("total_cost"))
         .otherwise(0)
    )
    .withColumn("state_revenue_rank",
        F.rank().over(Window.orderBy(F.desc("total_revenue")))
    )
    .orderBy(F.desc("total_revenue"))
)

agg_state_performance.write.mode("overwrite").saveAsTable("dev_gold.reporting.agg_state_performance")
print(f"agg_state_performance: {agg_state_performance.count()} rows")
agg_state_performance.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Gold Layer Tables

# COMMAND ----------

print("=" * 70)
print("DEV_GOLD.REPORTING - Business Aggregation Summary")
print("=" * 70)

tables = spark.sql("SHOW TABLES IN dev_gold.reporting").collect()
for t in tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM dev_gold.reporting.{t.tableName}").collect()[0].cnt
    print(f"  {t.tableName}: {count:,} rows")

print("=" * 70)
print(f"Total reporting tables: {len(tables)}")
