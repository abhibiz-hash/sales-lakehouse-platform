# Databricks notebook source


# COMMAND ----------

bronze_path = "abfss://bronze@stmodule05saleslakedev.dfs.core.windows.net/dev/sales_data"

silver_path = "abfss://silver@stmodule05saleslakedev.dfs.core.windows.net/dev/sales_data"

# COMMAND ----------

df_bronze = spark.read.format("delta").load(bronze_path)

display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC Data Validation Checks

# COMMAND ----------

#Null Check
null_count = df_bronze.filter(
    col("order_id").isNull()
).count()

print(f"Null order_id records: {null_count}")

# COMMAND ----------

# Duplicate Check
duplicate_count = df_bronze.groupBy("order_id") \
    .count() \
    .filter("count > 1") \
    .count()

print(f"Duplicate order_ids: {duplicate_count}")

# COMMAND ----------

# Invalid Amount Check
invalid_amount_count = df_bronze.filter(
    col("amount") <= 0
).count()

print(f"Invalid amount records: {invalid_amount_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC Cleaning

# COMMAND ----------

df_silver = df_bronze.dropna(
    subset=["order_id", "customer_name", "amount"]
)

# COMMAND ----------

df_silver = df_silver.dropDuplicates(["order_id"])

# COMMAND ----------

from pyspark.sql.functions import col

df_silver = df_silver.withColumn(
    "amount",
    col("amount").cast("integer")
)

# COMMAND ----------

df_silver = df_silver.filter(col("amount") > 0)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

# COMMAND ----------

window_spec = Window.partitionBy("order_id") \
                    .orderBy(desc("ingestion_timestamp"))

# COMMAND ----------

df_silver = df_bronze.withColumn(
    "row_num",
    row_number().over(window_spec)
).filter(
    "row_num = 1"
).drop(
    "row_num"
)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if DeltaTable.isDeltaTable(spark, silver_path):

    delta_table = DeltaTable.forPath(spark, silver_path)

    (
        delta_table.alias("target")
        .merge(
            df_silver.alias("source"),
            "target.order_id = source.order_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .save(silver_path)

# COMMAND ----------

df_check = spark.read.format("delta").load(silver_path)

display(df_check)

# COMMAND ----------

# MAGIC %md
# MAGIC View Delta History (time travel demo)

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, silver_path)

display(
    delta_table.history()
)

# COMMAND ----------

#Reading current version
df_current = spark.read.format("delta").load(silver_path)

display(df_current)

# COMMAND ----------

# Reading old version
df_old = spark.read \
    .format("delta") \
    .option("versionAsOf", 0) \
    .load(silver_path)

display(df_old)

# COMMAND ----------

# Timestamp based time travel
#.option("timestampAsOf", "2026-05-15")

# COMMAND ----------

