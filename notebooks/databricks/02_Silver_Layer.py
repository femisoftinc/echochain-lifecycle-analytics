# Databricks notebook source
# ==========================================================
# EchoChain - Silver Layer
# Week 2 - Reading Bronze Delta Tables
# ==========================================================

bronze_ebay_df = spark.table("workspace.default.bronze_ebay")

bronze_laptop_df = spark.table("workspace.default.bronze_laptop")

bronze_warranty_df = spark.table("workspace.default.bronze_warranty")

print("Bronze tables loaded successfully!")

# COMMAND ----------

# ======================================
# Explore Bronze Tables
# ======================================

print("========== eBay ==========")
print("Rows:", bronze_ebay_df.count())
display(bronze_ebay_df.limit(5))
bronze_ebay_df.printSchema()

print("========== Laptop ==========")
print("Rows:", bronze_laptop_df.count())
display(bronze_laptop_df.limit(5))
bronze_laptop_df.printSchema()

print("========== Warranty ==========")
print("Rows:", bronze_warranty_df.count())
display(bronze_warranty_df.limit(5))
bronze_warranty_df.printSchema()

# COMMAND ----------

# ======================================
# Week 2 - Data Quality Assessment
# ======================================

from pyspark.sql.functions import col, count, when

def data_quality_report(df, name):

    print("=" * 60)
    print(f"Dataset : {name}")
    print("=" * 60)

    print(f"Rows    : {df.count()}")
    print(f"Columns : {len(df.columns)}")

    print("\nNull Values")

    display(
        df.select([
            count(
                when(col(c).isNull(), c)
            ).alias(c)
            for c in df.columns
        ])
    )

data_quality_report(bronze_ebay_df, "eBay")
data_quality_report(bronze_laptop_df, "Laptop")
data_quality_report(bronze_warranty_df, "Warranty")

# COMMAND ----------

# ======================================
# Week 2 - Remove Unwanted Columns
# ======================================

# Laptop dataset
bronze_laptop_df = bronze_laptop_df.drop("c0", "unnamed_0")

# Warranty dataset
bronze_warranty_df = bronze_warranty_df.drop("c0", "unnamed_0")

print("Unwanted columns removed successfully!")

# COMMAND ----------

print("Laptop Columns:")
print(bronze_laptop_df.columns)

print("\nWarranty Columns:")
print(bronze_warranty_df.columns)

# COMMAND ----------

# ======================================
# Week 2 - Check Duplicate Records
# ======================================

print("===== Duplicate Check =====")

print(
    "eBay:",
    bronze_ebay_df.count() - bronze_ebay_df.dropDuplicates().count()
)

print(
    "Laptop:",
    bronze_laptop_df.count() - bronze_laptop_df.dropDuplicates().count()
)

print(
    "Warranty:",
    bronze_warranty_df.count() - bronze_warranty_df.dropDuplicates().count()
)

# COMMAND ----------

# ======================================
# Week 2 - Remove Duplicate Records
# ======================================

bronze_ebay_df = bronze_ebay_df.dropDuplicates()

bronze_laptop_df = bronze_laptop_df.dropDuplicates()

bronze_warranty_df = bronze_warranty_df.dropDuplicates()

print("Duplicate records removed successfully!")

# COMMAND ----------

print("eBay:", bronze_ebay_df.count())
print("Laptop:", bronze_laptop_df.count())
print("Warranty:", bronze_warranty_df.count())

# COMMAND ----------

# ======================================
# Week 2 - Remove Null Records
# ======================================

ebay_before = bronze_ebay_df.count()
laptop_before = bronze_laptop_df.count()
warranty_before = bronze_warranty_df.count()

bronze_ebay_df = bronze_ebay_df.dropna()

bronze_laptop_df = bronze_laptop_df.dropna()

bronze_warranty_df = bronze_warranty_df.dropna()

print("Null records removed successfully!")

print()

print("eBay:")
print("Before:", ebay_before)
print("After :", bronze_ebay_df.count())

print()

print("Laptop:")
print("Before:", laptop_before)
print("After :", bronze_laptop_df.count())

print()

print("Warranty:")
print("Before:", warranty_before)
print("After :", bronze_warranty_df.count())

# COMMAND ----------

# ======================================
# Week 2 - Standardize Data Types
# ======================================

from pyspark.sql.functions import col

# eBay
bronze_ebay_df = bronze_ebay_df.withColumn(
    "price",
    col("price").cast("double")
)

# Laptop
bronze_laptop_df = bronze_laptop_df.withColumn(
    "price",
    col("price").cast("double")
)

bronze_laptop_df = bronze_laptop_df.withColumn(
    "spec_rating",
    col("spec_rating").cast("double")
)

# Warranty
bronze_warranty_df = bronze_warranty_df.withColumn(
    "price",
    col("price").cast("double")
)

bronze_warranty_df = bronze_warranty_df.withColumn(
    "spec_rating",
    col("spec_rating").cast("double")
)

print("Data types standardized successfully!")

# COMMAND ----------

print("===== eBay =====")
bronze_ebay_df.printSchema()

print("\n===== Laptop =====")
bronze_laptop_df.printSchema()

print("\n===== Warranty =====")
bronze_warranty_df.printSchema()

# COMMAND ----------

# ======================================
# Week 2 - Save Silver Delta Tables
# ======================================

bronze_ebay_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_ebay")

bronze_laptop_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_laptop")

bronze_warranty_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_warranty")

print("Silver Delta tables created successfully!")

# COMMAND ----------

bronze_ebay_df.printSchema()

# COMMAND ----------

original_ebay = spark.table("workspace.default.bronze_ebay")

display(original_ebay.select("price").limit(30))

# COMMAND ----------

original_ebay.printSchema()

# COMMAND ----------

from pyspark.sql.functions import expr

original_ebay = spark.table("workspace.default.bronze_ebay")

display(
    original_ebay.filter(
        expr("try_cast(price AS DOUBLE) IS NULL AND price IS NOT NULL")
    )
)

# COMMAND ----------

from pyspark.sql.functions import expr

bad_rows = original_ebay.filter(
    expr("try_cast(price AS DOUBLE) IS NULL AND price IS NOT NULL")
)

print("Bad rows:", bad_rows.count())

# COMMAND ----------

from pyspark.sql.functions import expr

bronze_ebay_df = original_ebay.filter(
    expr("try_cast(price AS DOUBLE) IS NOT NULL OR price IS NULL")
)

print("Rows after cleaning:", bronze_ebay_df.count())

# COMMAND ----------

from pyspark.sql.functions import col

bronze_ebay_df = bronze_ebay_df.withColumn(
    "price",
    col("price").cast("double")
)

# COMMAND ----------

bronze_ebay_df.printSchema()

# COMMAND ----------

display(bronze_ebay_df.select("brand", "price").limit(10))

# COMMAND ----------

# ======================================
# Week 2 - Save Silver Delta Tables
# ======================================

bronze_ebay_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_ebay")

bronze_laptop_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_laptop")

bronze_warranty_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_warranty")

print("Silver Delta tables created successfully!")

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.default").show()

# COMMAND ----------

print(
    "Silver eBay:",
    spark.table("workspace.default.silver_ebay").count()
)

print(
    "Silver Laptop:",
    spark.table("workspace.default.silver_laptop").count()
)

print(
    "Silver Warranty:",
    spark.table("workspace.default.silver_warranty").count()
)

# COMMAND ----------

# ==========================================
# Week 2 - Final Data Quality Validation
# ==========================================

from pyspark.sql.functions import min, max

print("===== eBay =====")
bronze_ebay_df.select(
    min("price").alias("Minimum Price"),
    max("price").alias("Maximum Price")
).show()

print("===== Laptop =====")
bronze_laptop_df.count()

print("===== Warranty =====")
bronze_warranty_df.count()

# COMMAND ----------

print("===== Silver eBay =====")
spark.table("workspace.default.silver_ebay").printSchema()

print("===== Silver Laptop =====")
spark.table("workspace.default.silver_laptop").printSchema()

print("===== Silver Warranty =====")
spark.table("workspace.default.silver_warranty").printSchema()

# COMMAND ----------

display(
    spark.table("workspace.default.silver_ebay").limit(5)
)

display(
    spark.table("workspace.default.silver_laptop").limit(5)
)

display(
    spark.table("workspace.default.silver_warranty").limit(5)
)