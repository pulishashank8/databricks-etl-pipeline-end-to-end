# Databricks notebook source
# MAGIC %md
# MAGIC #DLT Pipeline

# COMMAND ----------

# Read the source table as a streaming DataFrame
from pyspark.sql.functions import col

df_stage = spark.readStream.table("databricks_cata.silver.products")


# COMMAND ----------

# Apply filtering logic as expectations
df_validated = df_stage.filter(
    col("product_id").isNotNull() & col("product_name").isNotNull()
)


# COMMAND ----------

from delta.tables import DeltaTable

def scd_type_2_merge(df_new, target_path, key_col):
    if DeltaTable.isDeltaTable(spark, target_path):
        delta_target = DeltaTable.forPath(spark, target_path)
        delta_target.alias("target") \
            .merge(df_new.alias("source"), f"target.{key_col} = source.{key_col}") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        df_new.write.format("delta").mode("overwrite").save(target_path)


# COMMAND ----------

# Define your path to save the final DimProducts table
query = df_validated.writeStream \
    .foreachBatch(lambda df, epoch_id: scd_type_2_merge(df, target_path, "product_id")) \
    .outputMode("update") \
    .trigger(once=True) \
    .option("checkpointLocation", "/tmp/checkpoints/DimProducts") \
    .start()



# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS databricks_cata.gold.DimProducts
    USING DELTA
    LOCATION '{target_path}'
""")
