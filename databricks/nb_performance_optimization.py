# Databricks notebook source


# COMMAND ----------

gold_path = "abfss://gold@stmodule05saleslakedev.dfs.core.windows.net/dev/daily_sales_kpi"

# COMMAND ----------

df_gold = spark.read.format("delta").load(gold_path)

display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC measure query performance before optimization.

# COMMAND ----------

import time

start_time = time.time()

df_test = spark.read.format("delta").load(gold_path)

df_test.filter("sales_date = '2026-05-14'").count()

end_time = time.time()

print(f"Runtime BEFORE optimization: {end_time - start_time} seconds")

# COMMAND ----------

df_test.filter("sales_date = '2026-05-14'").explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC Cache

# COMMAND ----------

df_gold.cache()

# COMMAND ----------

df_gold.count()

# COMMAND ----------

# MAGIC %md
# MAGIC time after cache

# COMMAND ----------

start_time = time.time()

df_gold.filter("sales_date = '2026-05-14'").count()

end_time = time.time()

print(f"Runtime AFTER cache: {end_time - start_time} seconds")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`abfss://gold@stmodule05saleslakedev.dfs.core.windows.net/dev/daily_sales_kpi`
# MAGIC ZORDER BY (total_sales)

# COMMAND ----------

start_time = time.time()

df_optimized = spark.read.format("delta").load(gold_path)

df_optimized.filter("sales_date = '2026-05-14'").count()

end_time = time.time()

print(f"Runtime AFTER optimization: {end_time - start_time} seconds")