# Databricks notebook source
# ==========================================
# Week 3 - Fuzzy Matching
# ==========================================

silver_ebay_df = spark.table("workspace.default.silver_ebay")
silver_laptop_df = spark.table("workspace.default.silver_laptop")

print("Tables loaded successfully!")

# COMMAND ----------

silver_ebay_df.printSchema()

# COMMAND ----------

silver_laptop_df.printSchema()

# COMMAND ----------

display(
    silver_ebay_df.limit(10)
)

# COMMAND ----------

display(
    silver_laptop_df.limit(10)
)

# COMMAND ----------

print(silver_ebay_df.columns)

# COMMAND ----------

from pyspark.sql.functions import lower, trim

ebay_match = (
    silver_ebay_df
    .select(
        "brand",
        "processor",
        "ram_size",
        "storage_type",
        "screen_size_inch",
        "os",
        "price"
    )
)

laptop_match = (
    silver_laptop_df
    .select(
        "brand",
        "processor",
        "ram",
        "storage_type",
        "screen_size",
        "os",
        "price",
        "name"
    )
)

# COMMAND ----------

print(silver_laptop_df.columns)

# COMMAND ----------

from pyspark.sql.functions import lower, trim

ebay_clean = (
    silver_ebay_df
    .withColumn("brand", lower(trim("brand")))
)

laptop_clean = (
    silver_laptop_df
    .withColumn("brand", lower(trim("brand")))
)

print("Brand standardized successfully!")

# COMMAND ----------

brand_match = (
    ebay_clean.alias("e")
    .join(
        laptop_clean.alias("l"),
        on="brand",
        how="inner"
    )
)

display(
    brand_match.select(
        "brand",
        "e.processor",
        "l.processor",
        "e.price",
        "l.price",
        "l.name"
    ).limit(20)
)

# COMMAND ----------

from pyspark.sql.functions import when, lower, col

matched_df = (
    brand_match
    .withColumn(
        "processor_match",
        when(
            lower(col("e.processor")).contains("i3") &
            lower(col("l.processor")).contains("i3"),
            1
        )
        .when(
            lower(col("e.processor")).contains("i5") &
            lower(col("l.processor")).contains("i5"),
            1
        )
        .when(
            lower(col("e.processor")).contains("i7") &
            lower(col("l.processor")).contains("i7"),
            1
        )
        .when(
            lower(col("e.processor")).contains("ryzen") &
            lower(col("l.processor")).contains("ryzen"),
            1
        )
        .otherwise(0)
    )
)

display(
    matched_df.select(
        "brand",
        "e.processor",
        "l.processor",
        "processor_match",
        "l.name"
    ).limit(20)
)

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, col, when

matched_df = (
    matched_df
    .withColumn(
        "ebay_ram",
        regexp_extract(col("e.ram_size"), r"(\d+)", 1)
    )
    .withColumn(
        "laptop_ram",
        regexp_extract(col("l.ram"), r"(\d+)", 1)
    )
    .withColumn(
        "ram_match",
        when(col("ebay_ram") == col("laptop_ram"), 1).otherwise(0)
    )
)

display(
    matched_df.select(
        "brand",
        "ebay_ram",
        "laptop_ram",
        "ram_match",
        "l.name"
    ).limit(20)
)

# COMMAND ----------

from pyspark.sql.functions import lower, trim, col, when

matched_df = (
    matched_df
    .withColumn(
        "os_match",
        when(
            lower(trim(col("e.os"))) == lower(trim(col("l.os"))),
            1
        ).otherwise(0)
    )
)

display(
    matched_df.select(
        "brand",
        "e.os",
        "l.os",
        "os_match",
        "l.name"
    ).limit(20)
)

# COMMAND ----------

from pyspark.sql.functions import lit

matched_df = (
    matched_df
    .withColumn("brand_match", lit(1))
    .withColumn(
        "match_score",
        col("brand_match") +
        col("processor_match") +
        col("ram_match") +
        col("os_match")
    )
)

display(
    matched_df.select(
        "brand",
        "l.name",
        "processor_match",
        "ram_match",
        "os_match",
        "match_score"
    ).orderBy(col("match_score").desc()).limit(20)
)

# COMMAND ----------

from pyspark.sql.functions import round, col

matched_df = (
    matched_df
    .withColumn(
        "depreciation_percent",
        round(
            (
                (col("l.price") - col("e.price"))
                / col("l.price")
            ) * 100,
            2
        )
    )
)

display(
    matched_df.select(
        "brand",
        "l.name",
        "e.price",
        "l.price",
        "depreciation_percent"
    ).limit(20)
)

# COMMAND ----------

from pyspark.sql.functions import round, col

matched_df = (
    matched_df
    .withColumn(
        "circularity_score",
        round(
            (
                col("brand_match") * 10 +
                col("processor_match") * 30 +
                col("ram_match") * 20 +
                col("os_match") * 20 +
                (100 - col("depreciation_percent")) * 0.2
            ),
            2
        )
    )
)

display(
    matched_df.select(
        "brand",
        "l.name",
        "match_score",
        "depreciation_percent",
        "circularity_score"
    ).orderBy(col("circularity_score").desc())
)

# COMMAND ----------

matched_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_circularity_score")

print("Gold Circularity Score table created successfully!")

# COMMAND ----------

from pyspark.sql.functions import col

gold_circularity_score = matched_df.select(
    col("brand"),
    col("l.name").alias("laptop_name"),
    col("e.price").alias("ebay_price"),
    col("l.price").alias("original_price"),
    col("processor_match"),
    col("ram_match"),
    col("os_match"),
    col("match_score"),
    col("depreciation_percent"),
    col("circularity_score")
)

display(gold_circularity_score)

# COMMAND ----------

gold_circularity_score.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_circularity_score")

print("Gold Circularity Score table created successfully!")