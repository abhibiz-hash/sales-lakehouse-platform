# Databricks notebook source


# COMMAND ----------

dbutils.widgets.text("file_name", "")
dbutils.widgets.text("environment", "dev")

# COMMAND ----------

file_name = dbutils.widgets.get("file_name")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

input_path = f"abfss://raw@stmodule05saleslakedev.dfs.core.windows.net/{file_name}"

output_path = f"abfss://bronze@stmodule05saleslakedev.dfs.core.windows.net/{environment}/sales_data"

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

sales_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("sales_date", StringType(), True),
    StructField("discount", IntegerType(), True)
])

# COMMAND ----------

print("===== Bronze Ingestion Started =====")
print(f"Environment: {environment}")
print(f"Processing file: {file_name}")
print(f"Input path: {input_path}")

df = spark.read \
    .option("header", True) \
    .schema(sales_schema) \
    .csv(input_path)

print(f"Total records read: {df.count()}")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df = df.withColumn("ingestion_timestamp", current_timestamp())

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(output_path)

print("Bronze Delta write completed successfully")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC check delta table in bronze layer

# COMMAND ----------

df_check = spark.read.format("delta").load(output_path)

display(df_check)