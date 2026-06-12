# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC This notebook reads source data from the Unity Catalog volume and ingests all sheets into the `dev_bronze` catalog as raw tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Install Required Libraries

# COMMAND ----------

# %pip install xlrd openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create the Bronze Catalog and Schema

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS dev_bronze MANAGED LOCATION 's3://databricks-workspace-stack-9a61f-bucket/unity-catalog/6483314808213883/zyad_demo'")
spark.sql("USE CATALOG dev_bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS raw")
spark.sql("USE SCHEMA raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Discover Source Files and Import Libraries

# COMMAND ----------

import os
import pandas as pd
from pyspark.sql import DataFrame

source_path = "/Volumes/catalog_demo/dev/source_dataset"

files = os.listdir(source_path)
print(f"Files found in source volume: {files}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Define Ingestion Functions

# COMMAND ----------

def clean_table_name(name: str) -> str:
    return name.strip().lower().replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "")

def ingest_excel_file(file_path: str):
    xl = pd.ExcelFile(file_path)
    sheet_names = xl.sheet_names
    print(f"Found {len(sheet_names)} sheets in {os.path.basename(file_path)}: {sheet_names}")

    for sheet_name in sheet_names:
        print(f"\nProcessing sheet: '{sheet_name}'")
        pdf = xl.parse(sheet_name)

        if pdf.empty:
            print(f"  Skipping empty sheet: '{sheet_name}'")
            continue

        pdf.columns = [clean_table_name(str(col)) for col in pdf.columns]
        spark_df = spark.createDataFrame(pdf)

        table_name = clean_table_name(sheet_name)
        full_table_name = f"dev_bronze.raw.{table_name}"

        spark_df.write.mode("overwrite").saveAsTable(full_table_name)
        print(f"  Written table: {full_table_name} ({spark_df.count()} rows, {len(spark_df.columns)} columns)")

def ingest_csv_file(file_path: str):
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    table_name = clean_table_name(file_name)
    full_table_name = f"dev_bronze.raw.{table_name}"

    spark_df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    spark_df.write.mode("overwrite").saveAsTable(full_table_name)
    print(f"Written table: {full_table_name} ({spark_df.count()} rows, {len(spark_df.columns)} columns)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Process All Files

# COMMAND ----------

for file_name in files:
    file_path = os.path.join(source_path, file_name)
    print(f"\n{'='*60}")
    print(f"Processing file: {file_name}")
    print(f"{'='*60}")

    if file_name.endswith((".xlsx", ".xls")):
        ingest_excel_file(file_path)
    elif file_name.endswith(".csv"):
        ingest_csv_file(file_path)
    else:
        print(f"  Skipping unsupported file type: {file_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Ingested Tables

# COMMAND ----------

tables = spark.sql("SHOW TABLES IN dev_bronze.raw").collect()
print(f"\nTotal tables created in dev_bronze.raw: {len(tables)}\n")
for t in tables:
    row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM dev_bronze.raw.{t.tableName}").collect()[0].cnt
    print(f"  - {t.tableName}: {row_count} rows")
