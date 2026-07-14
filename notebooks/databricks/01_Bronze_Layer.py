# Databricks notebook source
# Databricks notebook source

# ==========================================
# EchoChain - Bronze Layer
# Week 1 - Data Ingestion
# ==========================================

# COMMAND ----------

# EchoChain - Bronze Layer
# Week 1: Raw Data Ingestion using PySpark

base_path = "/Volumes/workspace/default/echochain_raw"

ebay_path = f"{base_path}/Ebay_dataset.csv"
laptop_path = f"{base_path}/laptop_dataset.csv"
warranty_path = f"{base_path}/warranty_dataset.csv"

print("EchoChain raw data paths configured successfully.")

# COMMAND ----------

# Load raw CSV datasets using PySpark

ebay_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ebay_path)
)

laptop_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(laptop_path)
)

warranty_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(warranty_path)
)

print("All three datasets loaded successfully.")

# COMMAND ----------

print("eBay records:", ebay_df.count())
print("Laptop records:", laptop_df.count())
print("Warranty records:", warranty_df.count())

# COMMAND ----------

print("=== EBAY DATASET SCHEMA ===")
ebay_df.printSchema()

print("=== LAPTOP DATASET SCHEMA ===")
laptop_df.printSchema()

print("=== WARRANTY DATASET SCHEMA ===")
warranty_df.printSchema()

# COMMAND ----------

display(ebay_df.limit(5))
display(laptop_df.limit(5))
display(warranty_df.limit(5))

# COMMAND ----------

import re

def clean_column_names(df):
    for old_col in df.columns:
        new_col = re.sub(r'[^a-zA-Z0-9_]', '_', old_col)
        new_col = re.sub(r'_+', '_', new_col)
        new_col = new_col.strip('_').lower()
        df = df.withColumnRenamed(old_col, new_col)
    return df

ebay_df = clean_column_names(ebay_df)
laptop_df = clean_column_names(laptop_df)
warranty_df = clean_column_names(warranty_df)

print("Column names standardized successfully.")

# COMMAND ----------

# Save raw DataFrames as Bronze Delta tables

ebay_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_ebay")

laptop_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_laptop")

warranty_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_warranty")

print("Bronze Delta tables created successfully.")

# COMMAND ----------

# Verify Bronze Delta tables

spark.sql("SHOW TABLES IN workspace.default").show(truncate=False)

# COMMAND ----------

print(
    "Bronze eBay records:",
    spark.table("workspace.default.bronze_ebay").count()
)

print(
    "Bronze Laptop records:",
    spark.table("workspace.default.bronze_laptop").count()
)

print(
    "Bronze Warranty records:",
    spark.table("workspace.default.bronze_warranty").count()
)