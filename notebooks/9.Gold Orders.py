# Databricks notebook source

df = spark.sql("select * from databricks_cata.silver.orders")
df.limit(10).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.gold.dimproducts

# COMMAND ----------

df_dimcust = spark.sql("select DimCustomerKey,customer_id as dim_customer_id from databricks_cata.gold.dimcustomer")
df_dimprod = spark.sql("select product_ID AS DimProductKey,product_id as dim_product_id from databricks_cata.gold.dimproducts")

# COMMAND ----------

df_fact = df.join(df_dimcust, df['customer_id'] == df_dimcust['dim_customer_id']).join(df_dimprod, df['product_id'] == df_dimprod['dim_product_id'], how='left')
df_fact_new = df_fact.drop('dim_customer_id','dim_product_id','customer_id','product_id')

# COMMAND ----------

df_fact_new.limit(10).display()

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_cata.gold.FactOrders"):
    dlt_obj = DeltaTable.forName(spark, "databricks_cata.gold.FactOrders")
    dlt_obj.alias("trg").merge(df_fact_new.alias("src"),"trg.order_id = src.order_id AND trg.DimCustomerKey = src.DimCustomerKey AND trg.DimProductKey = src.DimProductKey")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
else:
    df_fact_new.write.format("delta")\
        .option("path","abfss://gold@mydatabricksetestorage.dfs.core.windows.net/FactOrders")\
    .saveAsTable("databricks_cata.gold.FactOrders")
    

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.gold.factorders limit(10)

# COMMAND ----------

