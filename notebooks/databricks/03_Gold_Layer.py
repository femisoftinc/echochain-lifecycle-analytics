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

# COMMAND ----------

from pyspark.sql.functions import avg, count, round

gold_brand_resale = (
    silver_ebay_df
    .groupBy("brand")
    .agg(
        round(avg("price"), 2).alias("average_resale_price"),
        count("*").alias("total_listings")
    )
    .orderBy("average_resale_price", ascending=False)
)

display(gold_brand_resale)

# COMMAND ----------

from pyspark.sql.functions import avg, count, round, col

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

gold_brand_resale = (
    silver_ebay_df
    .filter(col("brand").isin(valid_brands))
    .groupBy("brand")
    .agg(
        round(avg("price"), 2).alias("average_resale_price"),
        count("*").alias("total_listings")
    )
    .orderBy("average_resale_price", ascending=False)
)

display(gold_brand_resale)

# COMMAND ----------

gold_brand_resale.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_brand_resale")

# COMMAND ----------

from pyspark.sql.functions import avg, count, round

gold_warranty_analysis = (
    silver_laptop_df
    .groupBy("warranty")
    .agg(
        count("*").alias("total_products"),
        round(avg("price"), 2).alias("average_price")
    )
    .orderBy("average_price", ascending=False)
)

display(gold_warranty_analysis)

# COMMAND ----------

from pyspark.sql.functions import count, avg, round

gold_executive_kpi = (
    silver_ebay_df
    .agg(
        count("*").alias("total_products"),
        round(avg("price"), 2).alias("average_resale_price"),
        count("brand").alias("total_brand_records")
    )
)

display(gold_executive_kpi)

# COMMAND ----------

from pyspark.sql.functions import count, round, col
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum

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

gold_brand_market_share = (
    silver_ebay_df
    .filter(col("brand").isin(valid_brands))
    .groupBy("brand")
    .agg(
        count("*").alias("total_products")
    )
)

window = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

gold_brand_market_share = (
    gold_brand_market_share
    .withColumn(
        "market_share_percent",
        round(
            col("total_products") * 100 / spark_sum("total_products").over(window),
            2
        )
    )
    .orderBy(col("market_share_percent").desc())
)

display(gold_brand_market_share)

# COMMAND ----------

gold_brand_market_share.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_brand_market_share")

print("Gold Brand Market Share table created successfully!")

# COMMAND ----------

from pyspark.sql.functions import countDistinct, avg, max, min, round

gold_dashboard_metrics = (
    silver_ebay_df
    .filter(col("brand").isin(valid_brands))
    .agg(
        count("*").alias("total_products"),
        countDistinct("brand").alias("total_brands"),
        round(avg("price"), 2).alias("average_price"),
        round(max("price"), 2).alias("highest_price"),
        round(min("price"), 2).alias("lowest_price")
    )
)gold_dashboard_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_dashboard_metrics")

print("Gold Dashboard Metrics table created successfully!")

display(gold_dashboard_metrics)

# COMMAND ----------

gold_dashboard_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_dashboard_metrics")

print("Gold Dashboard Metrics table created successfully!")