# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading
# MAGIC

# COMMAND ----------

df = spark.read.format("parquet")\
      .load("abfss://bronze@mydatabricksetestorage.dfs.core.windows.net/products")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()
 

# COMMAND ----------

# MAGIC %md
# MAGIC #Functions

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function databricks_cata.bronze.price_disc(p_price double)
# MAGIC returns double
# MAGIC language sql
# MAGIC return p_price * 0.90
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id,price,round(databricks_cata.bronze.price_disc(price),2) as price_disc
# MAGIC from products

# COMMAND ----------

df =df.withColumn("discount_price",round(expr("databricks_cata.bronze.price_disc(price)"),2))
df.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function databricks_cata.bronze.upper_brand(p_brand string)
# MAGIC returns string
# MAGIC language python
# MAGIC as 
# MAGIC $$ 
# MAGIC  return p_brand.upper()
# MAGIC  $$
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id,brand,databricks_cata.bronze.upper_brand(brand) as brand_upper
# MAGIC from products

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@mydatabricksetestorage.dfs.core.windows.net/products")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.products
# MAGIC using delta
# MAGIC location "abfss://silver@mydatabricksetestorage.dfs.core.windows.net/products"

# COMMAND ----------

