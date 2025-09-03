# Databricks notebook source
# MAGIC %md
# MAGIC #Data Reading

# COMMAND ----------

df = spark.read.format("parquet")\
       .load("abfss://bronze@mydatabricksetestorage.dfs.core.windows.net/orders")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df_renamed = df.withColumnRenamed("_rescued_data", "rescued_data")

# COMMAND ----------

df_renamed.limit(10).display()

# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df_drop =  df_renamed.drop("rescued_data")

# COMMAND ----------

df_drop.limit(10).display()

# COMMAND ----------


df_time=df_drop.withColumn("order_date",to_timestamp(col("order_date")))

# COMMAND ----------

df_time.limit(10).display()  

# COMMAND ----------

df = df_time.withColumn("year",year(col("order_date")))


# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

df1 = df.withColumn("rank_flag", rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

# COMMAND ----------

df1.limit(10).display()

# COMMAND ----------

df2 = df1.withColumn("denserank_flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

# COMMAND ----------

df2.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Writing

# COMMAND ----------

df.limit(10).display()


# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@mydatabricksetestorage.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.orders
# MAGIC using delta
# MAGIC location "abfss://silver@mydatabricksetestorage.dfs.core.windows.net/orders"

# COMMAND ----------

