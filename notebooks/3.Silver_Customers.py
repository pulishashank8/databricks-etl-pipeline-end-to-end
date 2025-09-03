# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Reading

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@mydatabricksetestorage.dfs.core.windows.net/customers")


# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df = df.withColumn("domain",split(col("email"),"@")[1])
df.limit(10).display()

# COMMAND ----------

df.groupBy("domain").agg(count("customer_id").alias("totalcustomers")).sort("totalcustomers", ascending=False).display()


# COMMAND ----------

df_gmail = df.filter(col('domain')== "gmail.com")
df_gmail.display()


# COMMAND ----------

df = df.withColumn("fullname",concat(col("first_name"),lit(" "),col("last_name")))
df.drop("first_name","last_name")
df.limit(10).display()

# COMMAND ----------

df = df.withColumn("fullname",concat(col("first_name"),lit(" "),col("last_name")))
df = df.drop("first_name","last_name")
df.limit(10).display()

# COMMAND ----------

df.write.mode("overwrite").format("delta").save("abfss://silver@mydatabricksetestorage.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.customers
# MAGIC using delta
# MAGIC location "abfss://silver@mydatabricksetestorage.dfs.core.windows.net/customers"

# COMMAND ----------

