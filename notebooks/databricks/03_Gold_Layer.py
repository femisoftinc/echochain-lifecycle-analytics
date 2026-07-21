# Databricks notebook source
# ===========================================
# EchoChain - Gold Layer
# Week 3 - Read Silver Delta Tables
# ===========================================

silver_ebay_df = spark.table("workspace.default.silver_ebay")

silver_laptop_df = spark.table("workspace.default.silver_laptop")

silver_warranty_df = spark.table("workspace.default.silver_warranty")

print("Silver tables loaded successfully!")

# COMMAND ----------

from pyspark.sql.functions import avg, count

gold_brand_summary = (
    silver_ebay_df
    .groupBy("brand")
    .agg(
        count("*").alias("total_products"),
        avg("price").alias("average_price")
    )
    .orderBy("total_products", ascending=False)
)

display(gold_brand_summary)

# COMMAND ----------

from pyspark.sql.functions import col

valid_brands = [
    "dell",
    "hp",
    "lenovo",
    "acer",
    "asus",
    "apple",
    "microsoft",
    "samsung",
    "lg",
    "other"
]

gold_brand_summary = (
    gold_brand_summary
    .filter(col("brand").isin(valid_brands))
)

display(gold_brand_summary)

# COMMAND ----------

from pyspark.sql.functions import count

gold_warranty_summary = (
    silver_laptop_df
    .groupBy("warranty")
    .agg(
        count("*").alias("total_laptops")
    )
    .orderBy("warranty")
)

display(gold_warranty_summary)

# COMMAND ----------

from pyspark.sql.functions import when, col, count

gold_price_summary = (
    silver_ebay_df
    .withColumn(
        "price_category",
        when(col("price") < 200, "Budget")
        .when((col("price") >= 200) & (col("price") < 500), "Mid-Range")
        .otherwise("Premium")
    )
    .groupBy("price_category")
    .agg(
        count("*").alias("total_products")
    )
    .orderBy("price_category")
)

display(gold_price_summary)

# COMMAND ----------

# ==========================================
# Week 3 - Save Gold Delta Tables
# ==========================================

gold_brand_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_brand_summary")

gold_warranty_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_warranty_summary")

gold_price_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_price_summary")

print("Gold Delta tables created successfully!")

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.default").show(truncate=False)

# COMMAND ----------

print(
    "Gold Brand Summary:",
    spark.table("workspace.default.gold_brand_summary").count()
)

print(
    "Gold Warranty Summary:",
    spark.table("workspace.default.gold_warranty_summary").count()
)

print(
    "Gold Price Summary:",
    spark.table("workspace.default.gold_price_summary").count()
)