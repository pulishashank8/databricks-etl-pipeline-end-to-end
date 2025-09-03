# Databricks notebook source
df = spark.read.table("databricks_cata.bronze.regions")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@mydatabricksetestorage.dfs.core.windows.net/regions")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.regions
# MAGIC using delta
# MAGIC location "abfss://silver@mydatabricksetestorage.dfs.core.windows.net/regions"

# COMMAND ----------

