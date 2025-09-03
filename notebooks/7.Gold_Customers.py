# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------


init_load_flag = int(dbutils.widgets.get("init_load_flag"))


# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading From Source

# COMMAND ----------


df = spark.sql("select * from databricks_cata.silver.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dropping duplicates

# COMMAND ----------

 df = df.dropDuplicates(subset=['customer_id'])
df.limit(10).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #Dividing New vs Old Records

# COMMAND ----------

if init_load_flag == 0:
    df_old = spark.sql(''' select DimcustomerKey, customer_id, create_date, update_date from databricks_cata.gold.Dimcustomer''')
else:
    

  df_old = spark.sql(''' select 0 DimcustomerKey, 0 customer_id, 0 create_date, 0 update_date from databricks_cata.silver.customers where 1=0''')



# COMMAND ----------

df_old.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming columns of old_df
# MAGIC

# COMMAND ----------

df_old = df_old.withColumnRenamed("DimcustomerKey", "old_DimcustomerKey")\
      .withColumnRenamed("customer_id", "old_customer_id")\
      .withColumnRenamed("create_date", "old_create_date")\
      .withColumnRenamed("update_date", "old_update_date")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying join with old records

# COMMAND ----------

df_join = df.join(df_old, df['customer_id'] == df_old['old_customer_id'], "left")


# COMMAND ----------

df_join.limit(10).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Seperating New vs Old Records

# COMMAND ----------

df_new = df_join.filter(df_join['old_DimcustomerKey'].isNull())


# COMMAND ----------

df_old = df_join.filter(df_join['old_DimcustomerKey'].isNotNull())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparing df_old

# COMMAND ----------

# Dropping coloumns which are not required
df_old = df_old.drop('old_customer_id','old_update_date')
#Renaming old_Dimcustomerkey to Dimcustomerkey
df_old = df_old.withColumnRenamed("old_DimcustomerKey", "DimCustomerKey")
#Renaming old_create_date to create_date
df_old = df_old.withColumnRenamed("old_create_date", "create_date")
df_old = df_old.withColumn("create_date", to_timestamp(col("create_date")))
#Recreating update_date to current time
df_old = df_old.withColumn("update_date", current_timestamp())


# COMMAND ----------

df_old.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparing df_new

# COMMAND ----------

#Dropping all coloumns which are not required
df_new = df_new.drop("old_DimCustomerKey","old_customer_id","old_update_date","old_create_date")
#Recreating "update_date","current_date with current time"
df_new = df_new.withColumn("update_date", current_timestamp())
df_new = df_new.withColumn("create_date", current_timestamp())


# COMMAND ----------

df_new.limit(10).display()



# COMMAND ----------

# MAGIC %md
# MAGIC ## surogate key - from 1

# COMMAND ----------

df_new = df_new.withColumn("DimcustomerKey", monotonically_increasing_id()+lit(1))

# COMMAND ----------

df_new.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding max surrogate key

# COMMAND ----------

if init_load_flag == 1:
    max_surrogate_key = 0
else:
   df_maxsur = spark.sql("select max(DimCustomerKey) as max_surrogate_key from databricks_cata.gold.DimCustomer")
#converting df_maxsur to max_surrogate_key
   max_surrogate_key = df_maxsur.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", lit(max_surrogate_key)+col("DimcustomerKey"))


# COMMAND ----------

# MAGIC %md
# MAGIC ##union od df_old and df_new

# COMMAND ----------

df_final = df_new.unionByName(df_old)


# COMMAND ----------

df_final.limit(10).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #SCD TYPE -1

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------


from delta.tables import DeltaTable

if spark.catalog.tableExists("databricks_cata.gold.DimCustomer"):
    dlt_obj = DeltaTable.forName(spark, "databricks_cata.gold.DimCustomer")
    
    dlt_obj.alias("target").merge(
        df_final.alias("source"),
        "target.DimCustomerKey = source.DimCustomerKey"
    ).whenMatchedUpdateAll()\
     .whenNotMatchedInsertAll()\
     .execute()

else:
    df_final.write.mode("overwrite")\
        .saveAsTable("databricks_cata.gold.DimCustomer")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.gold.dimcustomer limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if already exists
# MAGIC SHOW EXTERNAL LOCATIONS;
# MAGIC
# MAGIC -- Use existing one (example)
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.gold.DimCustomer
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@mydatabricksetestorage.dfs.core.windows.net/DimCustomer';
# MAGIC

# COMMAND ----------

