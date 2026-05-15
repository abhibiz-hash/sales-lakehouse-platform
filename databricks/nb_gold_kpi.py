# Databricks notebook source

# COMMAND ----------

silver_path = "abfss://silver@stmodule05saleslakedev.dfs.core.windows.net/dev/sales_data"

gold_path = "abfss://gold@stmodule05saleslakedev.dfs.core.windows.net/dev/daily_sales_kpi"

# COMMAND ----------

df_silver = spark.read.format("delta").load(silver_path)

display(df_silver)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_gold = df_silver.groupBy("sales_date").agg(
    count("order_id").alias("total_orders"),
    sum("amount").alias("total_sales"),
    avg("amount").alias("avg_sales"),
    max("amount").alias("max_sale"),
    min("amount").alias("min_sale")
).orderBy("sales_date")


# COMMAND ----------

display(df_gold)

# COMMAND ----------

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("sales_date") \
    .save(gold_path)

# COMMAND ----------

df_check = spark.read.format("delta").load(gold_path)

display(df_check)