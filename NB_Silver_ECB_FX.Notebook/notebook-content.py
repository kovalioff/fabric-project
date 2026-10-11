# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3163ca5c-40aa-452d-bd53-305c73cfe98f",
# META       "default_lakehouse_name": "lakehouse",
# META       "default_lakehouse_workspace_id": "1023cf7f-ee85-4dc6-9f17-84f7d7ad4175",
# META       "known_lakehouses": [
# META         {
# META           "id": "3163ca5c-40aa-452d-bd53-305c73cfe98f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE silver.silver_ecb_fx AS
# MAGIC SELECT DISTINCT
# MAGIC CAST(TIME_PERIOD AS DATE) AS date_key,
# MAGIC CAST(OBS_VALUE AS DOUBLE) AS usd_to_eur
# MAGIC FROM dbo.ECB_FX
# MAGIC WHERE TIME_PERIOD IS NOT NULL 
# MAGIC AND OBS_VALUE IS NOT NULL;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
